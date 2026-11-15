//! Vtable-free execution `LinkOps` (`WsLink`, `HttpsLink`, `FixLink`), `GuardedValue` atomic
//! spinlock token swap, per-connection `LockedConnection` (`REQ`/`RESP` spinlock with short-write
//! self-retirement and `eventfd` wakeup), and multi-socket `[local_ip, count]` `UrlConnections`.
#![forbid(unsafe_code)]

use crate::{
    Connection, DriveReport, Error, IoBudget, Result,
    dialer::{DialBackoff, DialBaton, DialSpec, DialState, Dialer},
    fix, http,
    json_scan::parse_u64_ascii,
    latency_tracer::{LatencyTracer, LockDomain, LockId, ThreadTag, TimedSpinLock},
    order_router::{
        MAX_PUBLISHED_ROUTES, PublishedRoutes, RouteHealth, RouterConfig, SampleKind, SampleMask,
        SendSlot,
    },
    ws,
};
use mio::{Registry, Token, Waker};
use std::{
    collections::VecDeque,
    net::IpAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU16, Ordering},
    },
    time::Instant,
};
use tickwire_venue::OrderRoute as VenueOrderRoute;

pub const MAX_WS_INFLIGHT: usize = 16;
pub const MAX_EARLY_WS_FRAMES: usize = 64;
pub const FIX_SENT_RING_LEN: usize = 64;
pub const MAX_URL_CONNECTIONS: usize = MAX_PUBLISHED_ROUTES;

/// Wire protocol family of an execution connection pool (`UrlConnections`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LinkKind {
    Ws,
    Https,
    Fix,
}

impl LinkKind {
    #[inline]
    pub const fn default_send_slot(self) -> SendSlot {
        match self {
            Self::Ws | Self::Fix => SendSlot::Shared,
            Self::Https => SendSlot::Exclusive,
        }
    }
}

/// Compact 32-byte correlation metadata attached to an in-flight outbound order request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RequestRef {
    pub order_key: u64,
    pub client_seq: u64,
    pub sent_tsc: u64,
    pub sent_mono_ns: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplyIdSlot {
    pub active: bool,
    pub reply_id: u64,
    pub request: RequestRef,
}

/// Multi-step WebSocket private authentication state (`loginChallenge`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WsLoginState {
    Authenticated,
    AwaitingChallenge,
    AwaitingAuthAck,
    Failed,
}

/// Optional multi-step WebSocket authentication descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WsLoginChallenge {
    None,
    /// Send a static or pre-signed login frame immediately after HTTP `101` upgrade and wait for
    /// an authentication acknowledgement frame.
    ImmediateLoginFrame(Vec<u8>),
    /// Wait for the venue's initial challenge frame before sending the signed challenge response.
    ServerChallenge,
}

/// Multiplexed WebSocket execution link (`SendSlot::Shared`, up to `16` concurrent in-flight
/// requests matched by `reply_id`, `loginChallenge` state machine, and FIFO `early_frames` replay).
pub struct WsLink {
    conn: Connection,
    decoder: ws::Decoder,
    login_state: WsLoginState,
    early_frames: VecDeque<ws::Message>,
    inflight: [ReplyIdSlot; MAX_WS_INFLIGHT],
    inflight_count: u16,
    pending_ping_mono_ns: Option<u64>,
}

impl std::fmt::Debug for WsLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsLink")
            .field("login_state", &self.login_state)
            .field("inflight_count", &self.inflight_count)
            .field("early_frames", &self.early_frames.len())
            .field("ready", &self.is_order_ready())
            .finish_non_exhaustive()
    }
}

impl WsLink {
    pub fn new(conn: Connection, max_frame: usize, max_message: usize) -> Result<Self> {
        Self::with_login(conn, max_frame, max_message, WsLoginState::Authenticated)
    }

    pub fn with_login(
        conn: Connection,
        max_frame: usize,
        max_message: usize,
        login_state: WsLoginState,
    ) -> Result<Self> {
        Ok(Self {
            conn,
            decoder: ws::Decoder::new(max_frame, max_message)?,
            login_state,
            early_frames: VecDeque::new(),
            inflight: [ReplyIdSlot::default(); MAX_WS_INFLIGHT],
            inflight_count: 0,
            pending_ping_mono_ns: None,
        })
    }

    #[inline]
    pub const fn connection(&self) -> &Connection {
        &self.conn
    }

    #[inline]
    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }

    #[inline]
    pub fn decoder_mut(&mut self) -> &mut ws::Decoder {
        &mut self.decoder
    }

    #[inline]
    pub const fn login_state(&self) -> WsLoginState {
        self.login_state
    }

    #[inline]
    pub const fn inflight_count(&self) -> u16 {
        self.inflight_count
    }

    #[inline]
    pub fn is_order_ready(&self) -> bool {
        self.conn.is_ready()
            && self.login_state == WsLoginState::Authenticated
            && usize::from(self.inflight_count) < MAX_WS_INFLIGHT
    }

    #[inline]
    pub fn on_login_challenge_sent(&mut self) {
        if self.login_state == WsLoginState::AwaitingChallenge {
            self.login_state = WsLoginState::AwaitingAuthAck;
        }
    }

    #[inline]
    pub fn complete_login(&mut self) {
        if self.login_state != WsLoginState::Failed {
            self.login_state = WsLoginState::Authenticated;
        }
    }

    #[inline]
    pub fn fail_login(&mut self) {
        self.login_state = WsLoginState::Failed;
        self.conn.retire();
    }

    /// Buffers a non-auth frame received during `AwaitingChallenge` / `AwaitingAuthAck` (or
    /// coalesced right after HTTP `101`) so it can be replayed in exact FIFO order once
    /// authentication completes.
    pub fn push_early_frame(&mut self, msg: ws::Message) -> Result<()> {
        if self.early_frames.len() >= MAX_EARLY_WS_FRAMES {
            return Err(Error::Bounds);
        }
        self.early_frames.push_back(msg);
        Ok(())
    }

    #[inline]
    pub fn early_frame_count(&self) -> usize {
        self.early_frames.len()
    }

    #[inline]
    pub fn pop_early_frame(&mut self) -> Option<ws::Message> {
        if self.login_state != WsLoginState::Authenticated {
            return None;
        }
        self.early_frames.pop_front()
    }

    pub fn drain_early_frames(&mut self, mut on_msg: impl FnMut(ws::Message)) -> usize {
        if self.login_state != WsLoginState::Authenticated {
            return 0;
        }
        let mut count = 0usize;
        while let Some(msg) = self.early_frames.pop_front() {
            count += 1;
            on_msg(msg);
        }
        count
    }

    /// Reserves one of the `16` concurrent `reply_id` slots and writes `frame` atomically.
    pub fn send_order_frame(
        &mut self,
        frame: &[u8],
        reply_id: u64,
        request: RequestRef,
    ) -> Result<()> {
        if self.login_state != WsLoginState::Authenticated {
            return Err(Error::Busy);
        }
        if usize::from(self.inflight_count) >= MAX_WS_INFLIGHT {
            return Err(Error::Busy);
        }
        if self
            .inflight
            .iter()
            .any(|s| s.active && s.reply_id == reply_id)
        {
            return Err(Error::Busy);
        }
        let free_idx = self
            .inflight
            .iter()
            .position(|s| !s.active)
            .ok_or(Error::Busy)?;
        self.conn.send_frame_nonblocking(frame)?;
        self.inflight[free_idx] = ReplyIdSlot {
            active: true,
            reply_id,
            request,
        };
        self.inflight_count += 1;
        Ok(())
    }

    /// Matches and releases an in-flight `reply_id` slot in `O(16)` stack scan (zero allocation).
    #[inline]
    pub fn match_reply_id(&mut self, reply_id: u64) -> Option<RequestRef> {
        for slot in &mut self.inflight {
            if slot.active && slot.reply_id == reply_id {
                let req = slot.request;
                slot.active = false;
                self.inflight_count = self.inflight_count.saturating_sub(1);
                return Some(req);
            }
        }
        None
    }

    #[inline]
    pub fn note_ping_sent(&mut self, now_mono_ns: u64) {
        if self.pending_ping_mono_ns.is_none() {
            self.pending_ping_mono_ns = Some(now_mono_ns);
        }
    }

    #[inline]
    pub fn note_pong_received(&mut self, now_mono_ns: u64) -> Option<u32> {
        let sent = self.pending_ping_mono_ns.take()?;
        let rtt_ns = now_mono_ns.saturating_sub(sent);
        Some((rtt_ns / 1_000).min(u64::from(u32::MAX)) as u32)
    }
}

/// Exclusive HTTP/1.1 keep-alive execution link (`SendSlot::Exclusive`, `1` in-flight request).
pub struct HttpsLink {
    conn: Connection,
    decoder: http::ResponseDecoder,
    inflight: Option<RequestRef>,
}

impl std::fmt::Debug for HttpsLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpsLink")
            .field("inflight", &self.inflight.is_some())
            .field("ready", &self.is_order_ready())
            .finish_non_exhaustive()
    }
}

impl HttpsLink {
    pub fn new(conn: Connection, max_receive_bytes: usize, max_headers: usize) -> Result<Self> {
        Ok(Self {
            conn,
            decoder: http::ResponseDecoder::new(max_receive_bytes, max_headers)?,
            inflight: None,
        })
    }

    #[inline]
    pub const fn connection(&self) -> &Connection {
        &self.conn
    }

    #[inline]
    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }

    #[inline]
    pub fn decoder_mut(&mut self) -> &mut http::ResponseDecoder {
        &mut self.decoder
    }

    #[inline]
    pub const fn inflight(&self) -> Option<RequestRef> {
        self.inflight
    }

    #[inline]
    pub fn is_order_ready(&self) -> bool {
        self.conn.is_ready() && self.inflight.is_none()
    }

    pub fn send_order_frame(&mut self, frame: &[u8], request: RequestRef) -> Result<()> {
        if self.inflight.is_some() {
            return Err(Error::Busy);
        }
        self.conn.send_frame_nonblocking(frame)?;
        self.inflight = Some(request);
        Ok(())
    }

    #[inline]
    pub fn complete_inflight(&mut self) -> Option<RequestRef> {
        self.inflight.take()
    }
}

#[derive(Clone, Debug)]
pub struct FixSessionConfig {
    pub begin_string: String,
    pub sender_comp_id: String,
    pub target_comp_id: String,
    pub heart_bt_int_sec: u32,
    pub max_frame_bytes: usize,
}

impl Default for FixSessionConfig {
    fn default() -> Self {
        Self {
            begin_string: "FIX.4.4".to_owned(),
            sender_comp_id: "TICKWIRE".to_owned(),
            target_comp_id: "VENUE".to_owned(),
            heart_bt_int_sec: 30,
            max_frame_bytes: 65_536,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FixSessionState {
    AwaitingLogonAck,
    Active,
    LogoutSent,
    Retired,
}

#[derive(Clone, Copy, Debug, Default)]
struct FixSentSlot {
    seq_num: u32,
    request: RequestRef,
}

/// Decoded inbound FIX session or application event borrowing tag slices directly from the frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FixInboundAction<'a> {
    LogonAccepted {
        seq_num: u32,
    },
    Heartbeat {
        seq_num: u32,
        test_req_id: Option<&'a str>,
        rtt_us: Option<u32>,
    },
    PeerTestRequest {
        seq_num: u32,
        test_req_id: &'a str,
    },
    SequenceGap {
        expected: u32,
        received: u32,
    },
    SessionReject {
        seq_num: u32,
        ref_seq_num: Option<u32>,
        request_ref: Option<RequestRef>,
        session_reject_reason: Option<u32>,
        text: Option<&'a str>,
    },
    Logout {
        seq_num: u32,
        text: Option<&'a str>,
    },
    Application {
        msg_type: &'a str,
        seq_num: u32,
        cl_ord_id: Option<&'a str>,
        orig_cl_ord_id: Option<&'a str>,
        order_id: Option<&'a str>,
        exec_id: Option<&'a str>,
        ord_status: Option<&'a str>,
        exec_type: Option<&'a str>,
        text: Option<&'a str>,
    },
}

/// Lightweight FIX session state machine (`Logon`, `Heartbeat`, `TestRequest`, sequence-gap
/// `ResendRequest`, and `64`-slot `RefSeqNum -> RequestRef` `Reject` correlation).
pub struct FixLink {
    conn: Connection,
    decoder: fix::Decoder,
    config: FixSessionConfig,
    state: FixSessionState,
    next_out_seq: u32,
    expected_in_seq: u32,
    sent_ring: [FixSentSlot; FIX_SENT_RING_LEN],
    inflight_count: u16,
    last_tx_mono_ns: u64,
    last_rx_mono_ns: u64,
    pending_test_req_mono_ns: Option<u64>,
}

impl std::fmt::Debug for FixLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FixLink")
            .field("state", &self.state)
            .field("next_out_seq", &self.next_out_seq)
            .field("expected_in_seq", &self.expected_in_seq)
            .field("inflight_count", &self.inflight_count)
            .finish_non_exhaustive()
    }
}

impl FixLink {
    pub fn new(
        conn: Connection,
        config: FixSessionConfig,
        initial_state: FixSessionState,
    ) -> Result<Self> {
        let decoder = fix::Decoder::new(config.max_frame_bytes)?;
        Ok(Self {
            conn,
            decoder,
            config,
            state: initial_state,
            next_out_seq: 1,
            expected_in_seq: 1,
            sent_ring: [FixSentSlot::default(); FIX_SENT_RING_LEN],
            inflight_count: 0,
            last_tx_mono_ns: 0,
            last_rx_mono_ns: 0,
            pending_test_req_mono_ns: None,
        })
    }

    #[inline]
    pub const fn connection(&self) -> &Connection {
        &self.conn
    }

    #[inline]
    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }

    #[inline]
    pub fn decoder_mut(&mut self) -> &mut fix::Decoder {
        &mut self.decoder
    }

    #[inline]
    pub const fn state(&self) -> FixSessionState {
        self.state
    }

    #[inline]
    pub const fn next_out_seq(&self) -> u32 {
        self.next_out_seq
    }

    #[inline]
    pub const fn expected_in_seq(&self) -> u32 {
        self.expected_in_seq
    }

    #[inline]
    pub const fn inflight_count(&self) -> u16 {
        self.inflight_count
    }

    #[inline]
    pub fn is_order_ready(&self) -> bool {
        self.conn.is_ready()
            && self.state == FixSessionState::Active
            && usize::from(self.inflight_count) < MAX_WS_INFLIGHT
    }

    /// Allocates the next outbound `MsgSeqNum (34)` and records `request` in the 64-slot ring.
    #[inline]
    pub fn record_outbound_seq(&mut self, seq_num: u32, request: RequestRef) {
        let idx = (seq_num as usize) & (FIX_SENT_RING_LEN - 1);
        self.sent_ring[idx] = FixSentSlot { seq_num, request };
        if seq_num >= self.next_out_seq {
            self.next_out_seq = seq_num.saturating_add(1);
        }
    }

    #[inline]
    pub fn alloc_next_out_seq(&mut self) -> u32 {
        let seq = self.next_out_seq;
        self.next_out_seq = self.next_out_seq.saturating_add(1);
        seq
    }

    #[inline]
    pub fn lookup_sent_seq(&self, ref_seq_num: u32) -> Option<RequestRef> {
        if ref_seq_num == 0 {
            return None;
        }
        let slot = self.sent_ring[(ref_seq_num as usize) & (FIX_SENT_RING_LEN - 1)];
        (slot.seq_num == ref_seq_num).then_some(slot.request)
    }

    #[inline]
    pub fn release_inflight(&mut self) {
        self.inflight_count = self.inflight_count.saturating_sub(1);
    }

    pub fn send_order_frame(
        &mut self,
        frame: &[u8],
        seq_num: u32,
        request: RequestRef,
    ) -> Result<()> {
        if self.state != FixSessionState::Active {
            return Err(Error::Busy);
        }
        if usize::from(self.inflight_count) >= MAX_WS_INFLIGHT {
            return Err(Error::Busy);
        }
        self.conn.send_frame_nonblocking(frame)?;
        self.record_outbound_seq(seq_num, request);
        self.inflight_count += 1;
        self.last_tx_mono_ns = request.sent_mono_ns;
        Ok(())
    }

    fn encode_admin_frame(&mut self, msg_type: &str, extra_tags: &str) -> Result<Vec<u8>> {
        let seq = self.alloc_next_out_seq();
        let body = format!(
            "35={msg_type}\x0149={}\x0156={}\x0134={seq}\x01{extra_tags}",
            self.config.sender_comp_id, self.config.target_comp_id
        );
        fix::encode(
            &self.config.begin_string,
            body.as_bytes(),
            self.config.max_frame_bytes,
        )
    }

    pub fn encode_logon(&mut self, sending_time: &str, extra_auth_tags: &str) -> Result<Vec<u8>> {
        let extra = format!(
            "52={sending_time}\x0198=0\x01108={}\x01{extra_auth_tags}",
            self.config.heart_bt_int_sec
        );
        self.encode_admin_frame("A", &extra)
    }

    pub fn encode_heartbeat(
        &mut self,
        sending_time: &str,
        test_req_id: Option<&str>,
    ) -> Result<Vec<u8>> {
        let extra = match test_req_id {
            Some(id) => format!("52={sending_time}\x01112={id}\x01"),
            None => format!("52={sending_time}\x01"),
        };
        self.encode_admin_frame("0", &extra)
    }

    pub fn encode_test_request(
        &mut self,
        sending_time: &str,
        test_req_id: &str,
        now_mono_ns: u64,
    ) -> Result<Vec<u8>> {
        self.pending_test_req_mono_ns = Some(now_mono_ns);
        let extra = format!("52={sending_time}\x01112={test_req_id}\x01");
        self.encode_admin_frame("1", &extra)
    }

    pub fn encode_resend_request(
        &mut self,
        sending_time: &str,
        begin_seq_no: u32,
        end_seq_no: u32,
    ) -> Result<Vec<u8>> {
        let extra = format!("52={sending_time}\x017={begin_seq_no}\x0116={end_seq_no}\x01");
        self.encode_admin_frame("2", &extra)
    }

    #[inline]
    pub fn heart_bt_interval_ns(&self) -> u64 {
        u64::from(self.config.heart_bt_int_sec).saturating_mul(1_000_000_000)
    }

    #[inline]
    pub fn needs_heartbeat(&self, now_mono_ns: u64) -> bool {
        self.state == FixSessionState::Active
            && self.last_tx_mono_ns > 0
            && now_mono_ns.saturating_sub(self.last_tx_mono_ns) >= self.heart_bt_interval_ns()
    }

    #[inline]
    pub fn needs_test_request(&self, now_mono_ns: u64) -> bool {
        let hb_ns = self.heart_bt_interval_ns();
        self.state == FixSessionState::Active
            && self.pending_test_req_mono_ns.is_none()
            && self.last_rx_mono_ns > 0
            && hb_ns > 0
            && now_mono_ns.saturating_sub(self.last_rx_mono_ns) >= (hb_ns * 3) / 2
    }

    /// Zero-allocation single-pass tag scanner over a validated FIX frame (`8=...\x0110=...\x01`).
    pub fn on_inbound_frame<'a>(
        &mut self,
        frame: &'a [u8],
        now_mono_ns: u64,
    ) -> Result<FixInboundAction<'a>> {
        self.last_rx_mono_ns = now_mono_ns;
        let mut msg_type: Option<&'a str> = None;
        let mut seq_num: Option<u32> = None;
        let mut ref_seq_num: Option<u32> = None;
        let mut test_req_id: Option<&'a str> = None;
        let mut cl_ord_id: Option<&'a str> = None;
        let mut orig_cl_ord_id: Option<&'a str> = None;
        let mut order_id: Option<&'a str> = None;
        let mut exec_id: Option<&'a str> = None;
        let mut ord_status: Option<&'a str> = None;
        let mut exec_type: Option<&'a str> = None;
        let mut text: Option<&'a str> = None;
        let mut session_reject_reason: Option<u32> = None;

        for field in frame.split(|&b| b == 1) {
            if field.is_empty() {
                continue;
            }
            let Some(eq_pos) = field.iter().position(|&b| b == b'=') else {
                return Err(Error::Protocol("malformed FIX tag=value"));
            };
            let tag_bytes = &field[..eq_pos];
            let val_bytes = &field[eq_pos + 1..];
            let Some(tag) = parse_u64_ascii(tag_bytes) else {
                continue;
            };
            let val_str = core::str::from_utf8(val_bytes)
                .map_err(|_| Error::Protocol("non-utf8 FIX value"))?;
            match tag {
                35 => msg_type = Some(val_str),
                34 => {
                    seq_num = parse_u64_ascii(val_bytes).and_then(|v| u32::try_from(v).ok());
                }
                45 => {
                    ref_seq_num = parse_u64_ascii(val_bytes).and_then(|v| u32::try_from(v).ok());
                }
                112 => test_req_id = Some(val_str),
                11 => cl_ord_id = Some(val_str),
                41 => orig_cl_ord_id = Some(val_str),
                37 => order_id = Some(val_str),
                17 => exec_id = Some(val_str),
                39 => ord_status = Some(val_str),
                150 => exec_type = Some(val_str),
                58 => text = Some(val_str),
                373 => {
                    session_reject_reason =
                        parse_u64_ascii(val_bytes).and_then(|v| u32::try_from(v).ok());
                }
                _ => {}
            }
        }

        let mtype = msg_type.ok_or(Error::Protocol("missing FIX MsgType (35)"))?;
        let seq = seq_num.ok_or(Error::Protocol("missing FIX MsgSeqNum (34)"))?;

        if seq > self.expected_in_seq && mtype != "4" && mtype != "5" {
            return Ok(FixInboundAction::SequenceGap {
                expected: self.expected_in_seq,
                received: seq,
            });
        }
        if seq >= self.expected_in_seq {
            self.expected_in_seq = seq.saturating_add(1);
        }

        match mtype {
            "A" => {
                self.state = FixSessionState::Active;
                Ok(FixInboundAction::LogonAccepted { seq_num: seq })
            }
            "0" => {
                let rtt_us = if test_req_id.is_some() {
                    self.pending_test_req_mono_ns.take().map(|sent| {
                        (now_mono_ns.saturating_sub(sent) / 1_000).min(u64::from(u32::MAX)) as u32
                    })
                } else {
                    None
                };
                Ok(FixInboundAction::Heartbeat {
                    seq_num: seq,
                    test_req_id,
                    rtt_us,
                })
            }
            "1" => Ok(FixInboundAction::PeerTestRequest {
                seq_num: seq,
                test_req_id: test_req_id.unwrap_or(""),
            }),
            "3" => {
                let req_ref = ref_seq_num.and_then(|r| self.lookup_sent_seq(r));
                if req_ref.is_some() {
                    self.release_inflight();
                }
                Ok(FixInboundAction::SessionReject {
                    seq_num: seq,
                    ref_seq_num,
                    request_ref: req_ref,
                    session_reject_reason,
                    text,
                })
            }
            "5" => {
                self.state = FixSessionState::Retired;
                Ok(FixInboundAction::Logout { seq_num: seq, text })
            }
            other => Ok(FixInboundAction::Application {
                msg_type: other,
                seq_num: seq,
                cl_ord_id,
                orig_cl_ord_id,
                order_id,
                exec_id,
                ord_status,
                exec_type,
                text,
            }),
        }
    }
}

/// Unified vtable-free execution connection enum (`WsLink`, `HttpsLink`, `FixLink`).
///
/// Stored inline inside cold-allocated per-connection slots so hot `REQ`/`RESP` frame dispatch
/// avoids heap indirection and virtual function tables.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum LinkOps {
    Ws(WsLink),
    Https(HttpsLink),
    Fix(FixLink),
}

impl LinkOps {
    #[inline]
    pub const fn kind(&self) -> LinkKind {
        match self {
            Self::Ws(_) => LinkKind::Ws,
            Self::Https(_) => LinkKind::Https,
            Self::Fix(_) => LinkKind::Fix,
        }
    }

    #[inline]
    pub const fn send_slot(&self) -> SendSlot {
        self.kind().default_send_slot()
    }

    #[inline]
    pub const fn connection(&self) -> &Connection {
        match self {
            Self::Ws(w) => w.connection(),
            Self::Https(h) => h.connection(),
            Self::Fix(fx) => fx.connection(),
        }
    }

    #[inline]
    pub fn connection_mut(&mut self) -> &mut Connection {
        match self {
            Self::Ws(w) => w.connection_mut(),
            Self::Https(h) => h.connection_mut(),
            Self::Fix(fx) => fx.connection_mut(),
        }
    }

    #[inline]
    pub fn is_retired(&self) -> bool {
        self.connection().is_retired()
    }

    #[inline]
    pub fn is_order_ready(&self) -> bool {
        match self {
            Self::Ws(w) => w.is_order_ready(),
            Self::Https(h) => h.is_order_ready(),
            Self::Fix(fx) => fx.is_order_ready(),
        }
    }

    #[inline]
    pub const fn inflight_count(&self) -> u16 {
        match self {
            Self::Ws(w) => w.inflight_count(),
            Self::Https(h) => {
                if h.inflight().is_some() {
                    1
                } else {
                    0
                }
            }
            Self::Fix(fx) => fx.inflight_count(),
        }
    }

    pub fn register(&mut self, registry: &Registry, token: Token) -> Result<()> {
        self.connection_mut().register(registry, token)
    }

    pub fn reregister(&mut self, registry: &Registry, token: Token) -> Result<()> {
        self.connection_mut().reregister(registry, token)
    }

    pub fn deregister(&mut self, registry: &Registry) -> Result<()> {
        self.connection_mut().deregister(registry)
    }

    pub fn send_order_frame(
        &mut self,
        frame: &[u8],
        reply_or_seq_id: u64,
        request: RequestRef,
    ) -> Result<()> {
        match self {
            Self::Ws(w) => w.send_order_frame(frame, reply_or_seq_id, request),
            Self::Https(h) => h.send_order_frame(frame, request),
            Self::Fix(fx) => {
                let seq = u32::try_from(reply_or_seq_id).map_err(|_| Error::Bounds)?;
                fx.send_order_frame(frame, seq, request)
            }
        }
    }

    pub fn send_raw_frame(&mut self, frame: &[u8]) -> Result<()> {
        self.connection_mut().send_frame_nonblocking(frame)
    }
}

/// Spinlock-guarded token holder (`listenKey` / `urlToken`) shared between cold `AsyncRestQuerier`
/// / `Dialer` writers and session readers without heap deallocation inside the spinlock.
#[derive(Debug)]
pub struct GuardedValue<T> {
    lock: TimedSpinLock<(T, u64)>,
}

impl<T> GuardedValue<T> {
    pub fn new(initial: T, index: u32, tracer: Option<Arc<LatencyTracer>>) -> Self {
        Self {
            lock: TimedSpinLock::with_tracer(
                (initial, 1),
                LockId::new(LockDomain::GuardedValue, index),
                tracer,
                None,
            ),
        }
    }

    /// Atomically replaces the guarded value, bumping its monotonic generation and dropping the
    /// displaced value **outside** the spinlock.
    pub fn set(&self, writer: ThreadTag, next: T) -> u64 {
        let mut guard = self.lock.lock(writer);
        let next_gen = guard.1.wrapping_add(1).max(1);
        let prev = core::mem::replace(&mut guard.0, next);
        guard.1 = next_gen;
        drop(guard);
        drop(prev);
        next_gen
    }

    /// Inspects the guarded value and its generation under the userspace spinlock.
    #[inline]
    pub fn read_with<R>(&self, reader: ThreadTag, f: impl FnOnce(&T, u64) -> R) -> R {
        let guard = self.lock.lock(reader);
        f(&guard.0, guard.1)
    }

    #[inline]
    pub fn generation(&self, reader: ThreadTag) -> u64 {
        self.lock.lock(reader).1
    }
}

impl<T: Clone> GuardedValue<T> {
    /// Convenience clone accessor for cold background threads (`Dialer` / `AsyncRestQuerier`).
    pub fn get_cloned(&self, reader: ThreadTag) -> (T, u64) {
        let guard = self.lock.lock(reader);
        (guard.0.clone(), guard.1)
    }
}

/// Result of a nonblocking `LockedConnection` write on `REQ` or `RESP`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendOutcome {
    Sent,
    Busy,
    Disconnected,
    Bounds,
    /// Socket or TLS short/partial write retired the connection immediately and woke `RESP` via `eventfd`.
    ShortWriteRetired,
}

struct LockedConnInner {
    link: Option<LinkOps>,
    close_pending: bool,
}

/// Fine-grained per-connection spinlock synchronizing `REQ::send_frame` and `RESP` socket driving,
/// heartbeats, and lifecycle transitions without ever overlapping any `Order` spinlock.
pub struct LockedConnection {
    conn_id: u16,
    inner: TimedSpinLock<LockedConnInner>,
    ready_flag: AtomicBool,
    close_pending_flag: AtomicBool,
    inflight_atomic: AtomicU16,
    resp_waker: Option<Arc<Waker>>,
}

impl std::fmt::Debug for LockedConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockedConnection")
            .field("conn_id", &self.conn_id)
            .field("ready", &self.is_ready_relaxed())
            .field("close_pending", &self.is_close_pending())
            .field("inflight", &self.inflight_relaxed())
            .finish_non_exhaustive()
    }
}

impl LockedConnection {
    pub fn new(
        conn_id: u16,
        resp_waker: Option<Arc<Waker>>,
        tracer: Option<Arc<LatencyTracer>>,
    ) -> Self {
        Self {
            conn_id,
            inner: TimedSpinLock::with_tracer(
                LockedConnInner {
                    link: None,
                    close_pending: false,
                },
                LockId::new(LockDomain::Connection, u32::from(conn_id)),
                tracer,
                None,
            ),
            ready_flag: AtomicBool::new(false),
            close_pending_flag: AtomicBool::new(false),
            inflight_atomic: AtomicU16::new(0),
            resp_waker,
        }
    }

    #[inline]
    pub const fn conn_id(&self) -> u16 {
        self.conn_id
    }

    #[inline]
    pub fn is_ready_relaxed(&self) -> bool {
        self.ready_flag.load(Ordering::Acquire) && !self.close_pending_flag.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_close_pending(&self) -> bool {
        self.close_pending_flag.load(Ordering::Acquire)
    }

    #[inline]
    pub fn inflight_relaxed(&self) -> u16 {
        self.inflight_atomic.load(Ordering::Acquire)
    }

    #[inline]
    fn sync_flags_locked(&self, inner: &LockedConnInner) {
        match inner.link.as_ref() {
            Some(link) if !inner.close_pending && !link.is_retired() => {
                self.inflight_atomic
                    .store(link.inflight_count(), Ordering::Release);
                self.ready_flag
                    .store(link.is_order_ready(), Ordering::Release);
                self.close_pending_flag.store(false, Ordering::Release);
            }
            Some(link) => {
                self.inflight_atomic
                    .store(link.inflight_count(), Ordering::Release);
                self.ready_flag.store(false, Ordering::Release);
                self.close_pending_flag
                    .store(inner.close_pending || link.is_retired(), Ordering::Release);
            }
            None => {
                self.inflight_atomic.store(0, Ordering::Release);
                self.ready_flag.store(false, Ordering::Release);
                self.close_pending_flag
                    .store(inner.close_pending, Ordering::Release);
            }
        }
    }

    #[inline]
    fn wake_resp(&self) {
        if let Some(w) = self.resp_waker.as_ref() {
            let _ = w.wake();
        }
    }

    /// Installs a freshly dialed [`LinkOps`] on `RESP`, returning any previous link so socket
    /// cleanup occurs outside the spinlock.
    pub fn install(&self, link: LinkOps) -> Option<LinkOps> {
        let mut guard = self.inner.lock(ThreadTag::RESP);
        guard.close_pending = false;
        let prev = guard.link.replace(link);
        self.sync_flags_locked(&guard);
        drop(guard);
        prev
    }

    /// Takes ownership of the underlying [`LinkOps`] on `RESP` (e.g., for teardown or inspection).
    pub fn take(&self) -> Option<LinkOps> {
        let mut guard = self.inner.lock(ThreadTag::RESP);
        let taken = guard.link.take();
        self.sync_flags_locked(&guard);
        drop(guard);
        taken
    }

    /// Requests asynchronous teardown on `RESP` and triggers `eventfd` wakeup outside the spinlock.
    pub fn request_close(&self, caller: ThreadTag) {
        let mut guard = self.inner.lock(caller);
        guard.close_pending = true;
        self.sync_flags_locked(&guard);
        drop(guard);
        self.wake_resp();
    }

    /// Completes connection teardown on `RESP`, returning the retired [`LinkOps`] to be dropped
    /// outside the spinlock.
    pub fn close_connection(&self) -> Option<LinkOps> {
        let mut guard = self.inner.lock(ThreadTag::RESP);
        guard.close_pending = false;
        let retired = guard.link.take();
        self.sync_flags_locked(&guard);
        drop(guard);
        retired
    }

    /// Sends one pre-encoded outbound order frame from `REQ` with zero heap allocation, zero
    /// logging, and zero protocol parsing under the spinlock.
    ///
    /// Must NEVER be called while holding an `Order` spinlock. Upon any TCP/TLS partial or short
    /// write, immediately retires the connection, latches `close_pending`, releases the spinlock,
    /// and wakes `RESP` via `mio::Waker` (`eventfd`).
    pub fn send_frame(
        &self,
        frame: &[u8],
        reply_or_seq_id: u64,
        request: RequestRef,
    ) -> SendOutcome {
        let mut guard = self.inner.lock(ThreadTag::REQ);
        if guard.close_pending {
            return SendOutcome::Disconnected;
        }
        let Some(link) = guard.link.as_mut() else {
            return SendOutcome::Disconnected;
        };
        let res = link.send_order_frame(frame, reply_or_seq_id, request);
        let retired_now = link.is_retired();
        if retired_now {
            guard.close_pending = true;
        }
        self.sync_flags_locked(&guard);
        drop(guard);

        match res {
            Ok(()) => SendOutcome::Sent,
            Err(_) if retired_now => {
                self.wake_resp();
                SendOutcome::ShortWriteRetired
            }
            Err(Error::Busy) => SendOutcome::Busy,
            Err(Error::Bounds) => SendOutcome::Bounds,
            Err(_) => SendOutcome::Disconnected,
        }
    }

    /// Sends a control frame (`pong` or FIX `Heartbeat` / `TestRequest`) from `RESP` under the
    /// spinlock with identical short-write self-retirement protection.
    pub fn send_control_frame(&self, frame: &[u8]) -> SendOutcome {
        let mut guard = self.inner.lock(ThreadTag::RESP);
        if guard.close_pending {
            return SendOutcome::Disconnected;
        }
        let Some(link) = guard.link.as_mut() else {
            return SendOutcome::Disconnected;
        };
        let res = link.send_raw_frame(frame);
        let retired_now = link.is_retired();
        if retired_now {
            guard.close_pending = true;
        }
        self.sync_flags_locked(&guard);
        drop(guard);

        match res {
            Ok(()) => SendOutcome::Sent,
            Err(_) if retired_now => {
                self.wake_resp();
                SendOutcome::ShortWriteRetired
            }
            Err(Error::Busy) => SendOutcome::Busy,
            Err(Error::Bounds) => SendOutcome::Bounds,
            Err(_) => SendOutcome::Disconnected,
        }
    }

    /// Drives nonblocking socket/TLS reads on `RESP` and copies raw plaintext bytes into `out` so
    /// all framing and JSON/FIX parsing execute **outside** the spinlock.
    pub fn poll_raw_rx(
        &self,
        now: Instant,
        budget: IoBudget,
        out: &mut [u8],
    ) -> Result<(DriveReport, usize)> {
        let mut guard = self.inner.lock(ThreadTag::RESP);
        let Some(link) = guard.link.as_mut() else {
            return Err(Error::Closed);
        };
        let report = match link.connection_mut().drive(now, budget) {
            Ok(r) => r,
            Err(e) => {
                guard.close_pending = true;
                self.sync_flags_locked(&guard);
                return Err(e);
            }
        };
        let read_n = link.connection_mut().read(out);
        if report.peer_closed || link.is_retired() {
            guard.close_pending = true;
        }
        self.sync_flags_locked(&guard);
        Ok((report, read_n))
    }

    pub fn match_ws_reply(&self, reply_id: u64) -> Option<RequestRef> {
        let mut guard = self.inner.lock(ThreadTag::RESP);
        let res = match guard.link.as_mut() {
            Some(LinkOps::Ws(ws_link)) => ws_link.match_reply_id(reply_id),
            _ => None,
        };
        self.sync_flags_locked(&guard);
        res
    }

    pub fn complete_https_reply(&self) -> Option<RequestRef> {
        let mut guard = self.inner.lock(ThreadTag::RESP);
        let res = match guard.link.as_mut() {
            Some(LinkOps::Https(https_link)) => https_link.complete_inflight(),
            _ => None,
        };
        self.sync_flags_locked(&guard);
        res
    }

    pub fn complete_ws_login(&self) -> bool {
        let mut guard = self.inner.lock(ThreadTag::RESP);
        let ok = if let Some(LinkOps::Ws(ws_link)) = guard.link.as_mut() {
            ws_link.complete_login();
            ws_link.login_state() == WsLoginState::Authenticated
        } else {
            false
        };
        self.sync_flags_locked(&guard);
        ok
    }
}

/// `[local_ip, count]` source-binding descriptor for [`UrlConnections`] pools.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UrlBindPair {
    pub local_ip: Option<IpAddr>,
    pub nic: Option<String>,
    pub count: u16,
    pub colo: bool,
}

impl UrlBindPair {
    pub fn new(local_ip: Option<IpAddr>, count: u16) -> Self {
        Self {
            local_ip,
            nic: None,
            count,
            colo: false,
        }
    }

    pub fn with_nic(mut self, nic: impl Into<String>) -> Self {
        self.nic = Some(nic.into());
        self
    }

    pub fn with_colo(mut self, colo: bool) -> Self {
        self.colo = colo;
        self
    }
}

struct UrlSlotEntry {
    conn_id: u16,
    local_ip: Option<IpAddr>,
    nic: Option<String>,
    colo: bool,
    locked: Arc<LockedConnection>,
    baton: Option<Arc<DialBaton>>,
    backoff: DialBackoff,
    reconnect_failures: u32,
    epoch: u64,
    established: bool,
    health: RouteHealth,
}

#[inline]
fn ipv4_u32(ip: Option<IpAddr>) -> u32 {
    match ip {
        Some(IpAddr::V4(v4)) => u32::from_be_bytes(v4.octets()),
        Some(IpAddr::V6(v6)) => {
            let b = v6.octets();
            u32::from_be_bytes([b[12], b[13], b[14], b[15]])
        }
        None => 0,
    }
}

/// Multi-connection pool opening `sum(count)` persistent WebSocket, HTTPS, or FIX connections to
/// a single market order URL (`[local_ip, count]`), tagged with `account_ref` and `product_mask`
/// (`0` shared UTA pool for OKX/Bybit; per-market bitmask for Binance Spot/UM/CM/Options).
pub struct UrlConnections {
    account_ref: u8,
    kind: LinkKind,
    product_mask: u16,
    config: RouterConfig,
    sample_mask: SampleMask,
    slots: Vec<UrlSlotEntry>,
    published: Arc<PublishedRoutes>,
}

impl std::fmt::Debug for UrlConnections {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UrlConnections")
            .field("account_ref", &self.account_ref)
            .field("kind", &self.kind)
            .field("product_mask", &self.product_mask)
            .field("slot_count", &self.slots.len())
            .finish_non_exhaustive()
    }
}

impl UrlConnections {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        account_ref: u8,
        base_conn_id: u16,
        kind: LinkKind,
        product_mask: u16,
        binds: &[UrlBindPair],
        config: RouterConfig,
        sample_mask: SampleMask,
        resp_waker: Option<Arc<Waker>>,
        tracer: Option<Arc<LatencyTracer>>,
        published: Arc<PublishedRoutes>,
    ) -> Result<Self> {
        config.validate()?;
        if binds.is_empty() {
            return Err(Error::Bounds);
        }
        let total: usize = binds.iter().map(|b| usize::from(b.count)).sum();
        if total == 0 || total > MAX_URL_CONNECTIONS {
            return Err(Error::Bounds);
        }

        let mut slots = Vec::with_capacity(total);
        let mut next_id = base_conn_id;
        for bind in binds {
            if bind.count == 0 {
                return Err(Error::Bounds);
            }
            for _ in 0..bind.count {
                if next_id == VenueOrderRoute::NO_CONNECTION {
                    return Err(Error::Bounds);
                }
                let locked = Arc::new(LockedConnection::new(
                    next_id,
                    resp_waker.clone(),
                    tracer.clone(),
                ));
                slots.push(UrlSlotEntry {
                    conn_id: next_id,
                    local_ip: if bind.nic.is_some() {
                        None
                    } else {
                        bind.local_ip
                    },
                    nic: bind.nic.clone(),
                    colo: bind.colo,
                    locked,
                    baton: None,
                    backoff: DialBackoff::default(),
                    reconnect_failures: 0,
                    epoch: 0,
                    established: false,
                    health: RouteHealth::new(config.ema_min_samples),
                });
                next_id = next_id.checked_add(1).ok_or(Error::Bounds)?;
            }
        }

        let pool = Self {
            account_ref,
            kind,
            product_mask,
            config,
            sample_mask,
            slots,
            published,
        };
        pool.republish()?;
        Ok(pool)
    }

    #[inline]
    pub const fn account_ref(&self) -> u8 {
        self.account_ref
    }

    #[inline]
    pub const fn kind(&self) -> LinkKind {
        self.kind
    }

    #[inline]
    pub const fn product_mask(&self) -> u16 {
        self.product_mask
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    #[inline]
    pub fn published(&self) -> &Arc<PublishedRoutes> {
        &self.published
    }

    pub fn connection_handle(&self, conn_id: u16) -> Option<&Arc<LockedConnection>> {
        self.slots
            .iter()
            .find(|s| s.conn_id == conn_id)
            .map(|s| &s.locked)
    }

    pub fn slot_local_ip(&self, conn_id: u16) -> Option<IpAddr> {
        self.slots
            .iter()
            .find(|s| s.conn_id == conn_id)
            .and_then(|s| s.local_ip)
    }

    pub fn slot_nic(&self, conn_id: u16) -> Option<&str> {
        self.slots
            .iter()
            .find(|s| s.conn_id == conn_id)
            .and_then(|s| s.nic.as_deref())
    }

    pub fn slot_epoch(&self, conn_id: u16) -> Option<u64> {
        self.slots
            .iter()
            .find(|s| s.conn_id == conn_id)
            .map(|s| s.epoch)
    }

    /// Registers every slot in this pool with a background [`Dialer`], applying each slot's
    /// `local_ip` and `nic` (`SO_BINDTODEVICE` taking precedence over `local_ip`).
    pub fn register_with_dialer(
        &mut self,
        dialer: &Dialer,
        slot_id_base: u64,
        template: &DialSpec,
    ) -> Result<()> {
        for (offset, slot) in self.slots.iter_mut().enumerate() {
            let mut spec = template.clone();
            spec.local_ip = slot.local_ip;
            spec.nic = slot.nic.clone();
            let baton = dialer.add_slot(slot_id_base + offset as u64, spec)?;
            slot.backoff = template.backoff;
            slot.baton = Some(baton);
        }
        Ok(())
    }

    /// Installs a live [`LinkOps`] into `conn_id` on `RESP` and publishes an updated route snapshot.
    pub fn install_link(&mut self, conn_id: u16, link: LinkOps) -> Result<u64> {
        if link.kind() != self.kind {
            return Err(Error::Protocol("LinkOps kind mismatch for UrlConnections"));
        }
        let slot = self
            .slots
            .iter_mut()
            .find(|s| s.conn_id == conn_id)
            .ok_or(Error::Bounds)?;
        if let Some(ip) = link.connection().local_ip() {
            slot.local_ip = Some(ip);
        }
        let prev = slot.locked.install(link);
        drop(prev);
        slot.epoch = slot.epoch.wrapping_add(1).max(1);
        slot.established = true;
        slot.reconnect_failures = 0;
        slot.health.reset();
        slot.health.ema_min_samples = self.config.ema_min_samples;
        let epoch = slot.epoch;
        self.republish()?;
        Ok(epoch)
    }

    /// Harvests any `DialState::Ready` links from `DialBaton`s and triggers `want()` on idle
    /// disconnected slots.
    pub fn poll_dials(
        &mut self,
        now_mono_ns: u64,
        mut upgrade_link: impl FnMut(u16, crate::dialer::DialedLink) -> Result<LinkOps>,
    ) -> usize {
        let mut installed = 0usize;
        let mut changed = false;
        for slot in &mut self.slots {
            if slot.established && !slot.locked.is_close_pending() {
                continue;
            }
            if slot.locked.is_close_pending() {
                let retired = slot.locked.close_connection();
                drop(retired);
                slot.established = false;
                slot.epoch = slot.epoch.wrapping_add(1).max(1);
                slot.health.reset();
                changed = true;
            }
            let Some(baton) = slot.baton.as_ref() else {
                continue;
            };
            match baton.state() {
                DialState::Ready => {
                    if let Some(dialed) = baton.take_ready() {
                        if let Ok(link_ops) = upgrade_link(slot.conn_id, dialed) {
                            if let Some(ip) = link_ops.connection().local_ip() {
                                slot.local_ip = Some(ip);
                            }
                            let prev = slot.locked.install(link_ops);
                            drop(prev);
                            slot.established = true;
                            slot.reconnect_failures = 0;
                            slot.epoch = slot.epoch.wrapping_add(1).max(1);
                            slot.health.reset();
                            slot.health.ema_min_samples = self.config.ema_min_samples;
                            installed += 1;
                            changed = true;
                        } else {
                            let delay = slot.backoff.delay_ns(slot.reconnect_failures);
                            slot.reconnect_failures = slot.reconnect_failures.saturating_add(1);
                            let _ = baton.want(now_mono_ns.saturating_add(delay));
                        }
                    }
                }
                DialState::Idle => {
                    let _ = baton.want(now_mono_ns);
                }
                DialState::Wanted | DialState::Dialing | DialState::Stopped => {}
            }
        }
        if changed {
            let _ = self.republish();
        }
        installed
    }

    /// Reaps any slots retired by `REQ` short-write or peer EOF and schedules reconnect backoff.
    pub fn reap_close_pending(&mut self, now_mono_ns: u64) -> usize {
        let mut reaped = 0usize;
        for slot in &mut self.slots {
            if slot.locked.is_close_pending() {
                let retired = slot.locked.close_connection();
                drop(retired);
                slot.established = false;
                slot.epoch = slot.epoch.wrapping_add(1).max(1);
                slot.health.reset();
                if let Some(baton) = slot.baton.as_ref() {
                    let delay = slot.backoff.delay_ns(slot.reconnect_failures);
                    slot.reconnect_failures = slot.reconnect_failures.saturating_add(1);
                    if baton.state() == DialState::Ready {
                        baton.reset();
                    }
                    let _ = baton.want(now_mono_ns.saturating_add(delay));
                }
                reaped += 1;
            }
        }
        if reaped > 0 {
            let _ = self.republish();
        }
        reaped
    }

    pub fn note_sample(
        &mut self,
        conn_id: u16,
        epoch: u64,
        kind: SampleKind,
        sample_us: u32,
    ) -> Result<bool> {
        if !self.sample_mask.contains(kind) {
            return Ok(false);
        }
        let cfg = self.config;
        let slot = self
            .slots
            .iter_mut()
            .find(|s| s.conn_id == conn_id)
            .ok_or(Error::Bounds)?;
        if !slot.established || slot.epoch != epoch {
            return Ok(false);
        }
        slot.health.note_sample(sample_us, cfg);
        self.republish()?;
        Ok(true)
    }

    /// Publishes all slots in this pool to [`PublishedRoutes`] via lock-free double-buffering.
    pub fn republish(&self) -> Result<u32> {
        let mut buf = [VenueOrderRoute::default(); MAX_PUBLISHED_ROUTES];
        for (idx, slot) in self.slots.iter().enumerate() {
            buf[idx] = VenueOrderRoute {
                connection_id: slot.conn_id,
                account_ref: self.account_ref,
                ready: slot.established && slot.locked.is_ready_relaxed(),
                colo: slot.colo,
                product_mask: self.product_mask,
                local_ip: ipv4_u32(slot.local_ip),
                ema_score_us: slot.health.raw_score(),
            };
        }
        self.published.publish_routes(&buf[..self.slots.len()])
    }
}
