//! Stateless Direct Order Entry Protocol (`DOP`) logon, sequence-floor adoption, account
//! session routing (`MktSession`), heartbeat maintenance, and clean logout over `SOCK_SEQPACKET`.
#![forbid(unsafe_code)]

use std::path::Path;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tickwire_control::encode_frame;
use tickwire_control_sys::{Connection, ControlListener, PrivateDirectory};
use tickwire_ipc::{
    ALL_STRATS, ControlEnvelope, ControlFrame, DOP_PROTOCOL_FEATURE_MASK, DopEchoBody,
    DopHeartBeatBody, DopLoginBody, DopLogoutBody, MktSession, OrsSessionsBody, Wire, dop_opcode,
};
use tickwire_venue::{Product, RefusalReason, TerminalReason};

use crate::{Error, Result};

const SEQUENCE_LIMIT: u64 = 1 << 63;

/// Default ORS stale-order threshold (`23 µs` = `23_000 ns`) enforced against `IntentBody::tick_mono_ns`.
pub const DEFAULT_MAX_ORDER_AGE_NS: u64 = 23_000;

/// Static DOP client configuration for one strategy worker (`strat_id` in `0..=254`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DopConfig {
    pub strat_id: u8,
    pub client_id: u32,
    pub local_instance: [u8; 16],
    pub peer_instance: [u8; 16],
    pub control_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_peer_uid: Option<u32>,
    #[serde(default = "default_max_order_age_ns")]
    pub max_order_age_ns: u64,
}

const fn default_max_order_age_ns() -> u64 {
    DEFAULT_MAX_ORDER_AGE_NS
}

impl DopConfig {
    pub fn validate(&self) -> Result<()> {
        if self.strat_id == ALL_STRATS
            || self.local_instance == [0; 16]
            || self.peer_instance == [0; 16]
            || self.local_instance == self.peer_instance
            || self.control_epoch == 0
            || self.control_epoch >= SEQUENCE_LIMIT
            || self.max_order_age_ns == 0
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}

/// Control events observed on a live [`DopSession`] after initial logon completes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DopControlEvent {
    HeartBeat(DopHeartBeatBody),
    Echo(DopEchoBody),
    SessionsUpdated(Vec<MktSession>),
    PeerLogout {
        reason: TerminalReason,
        cancel_on_logout: bool,
    },
}

/// Active stateless `DOP` control session bound to a single ORS venue process.
pub struct DopSession {
    connection: Connection,
    config: DopConfig,
    echo: DopEchoBody,
    authorized_sessions: Vec<MktSession>,
    all_sessions: Vec<MktSession>,
    next_order_seqnum: u64,
    next_msg_seqnum: u64,
    hb_interval_ns: u64,
    last_hb_sent_mono_ns: u64,
    last_hb_recv_mono_ns: u64,
    ready_to_trade: bool,
    logged_out: bool,
    next_request_nonce: u64,
}

impl DopSession {
    /// Connect to an ORS Unix `SOCK_SEQPACKET` control socket and complete `DopLogin` ->
    /// `DopEcho` + `OrsSessions`.
    pub fn connect_and_login(
        socket_path: &Path,
        config: DopConfig,
        requested_next_source_seqnum: u64,
        now_mono_ns: u64,
        timeout: Duration,
    ) -> Result<Self> {
        let deadline = Instant::now().checked_add(timeout).ok_or(Error::Deadline)?;
        let connection = Connection::connect(socket_path, deadline)?;
        Self::login_over_connection(
            connection,
            config,
            requested_next_source_seqnum,
            now_mono_ns,
            deadline,
        )
    }

    /// Perform the stateless `DOP` logon handshake over an established `SOCK_SEQPACKET` connection:
    /// sends `DopLogin` (`96`), validates `DopEcho` (`98`) and `OrsSessions` (`99`), adopts
    /// `next_order_seqnum = max(1, echo.next_source_order_seqnum)`, and resets `next_msg_seqnum = 1`.
    pub fn login_over_connection(
        mut connection: Connection,
        config: DopConfig,
        requested_next_source_seqnum: u64,
        now_mono_ns: u64,
        deadline: Instant,
    ) -> Result<Self> {
        config.validate()?;
        if requested_next_source_seqnum >= SEQUENCE_LIMIT {
            return Err(Error::Sequence);
        }
        let creds = connection.peer_credentials();
        if creds.pid == 0 {
            return Err(Error::Identity);
        }
        if let Some(expected_uid) = config.expected_peer_uid
            && creds.uid != expected_uid
        {
            return Err(Error::Identity);
        }

        let login_req_id = request_id_from_counter(config.strat_id, 1);
        let login_body = DopLoginBody {
            next_source_seqnum: requested_next_source_seqnum,
            client_id: config.client_id,
            strat_id: config.strat_id,
            _pad_13: [0; 19],
        };
        let mut raw_login_body = [0u8; DopLoginBody::SIZE];
        login_body.encode(&mut raw_login_body)?;
        let login_frame = encode_frame(
            ControlEnvelope {
                opcode: dop_opcode::DOP_LOGIN,
                flags: 1,
                request_id: login_req_id,
                control_epoch: config.control_epoch,
                sender_instance: config.local_instance,
                target_instance: config.peer_instance,
                ..Default::default()
            },
            &raw_login_body,
        )?;
        connection.send(&login_frame, &[], deadline)?;

        let echo_msg = connection.recv(deadline)?;
        if !echo_msg.fds().is_empty() {
            return Err(Error::Identity);
        }
        let echo_frame =
            ControlFrame::decode_negotiated(echo_msg.bytes(), DOP_PROTOCOL_FEATURE_MASK)?;
        verify_incoming_envelope(&echo_frame.envelope, &config, Some(login_req_id))?;
        if echo_frame.envelope.opcode != dop_opcode::DOP_ECHO {
            return Err(Error::Control(tickwire_control::Error::Protocol));
        }
        let echo = DopEchoBody::decode(echo_frame.body)?;
        if echo.strat_id != config.strat_id {
            return Err(Error::Identity);
        }
        if echo_frame.envelope.flags == 18
            || echo_frame.envelope.status != 0
            || echo.refusal_reason != 0
        {
            let reason =
                RefusalReason::from_code(echo.refusal_reason).unwrap_or(TerminalReason::Unknown);
            return Err(Error::DopRefused {
                reason,
                status: echo_frame.envelope.status,
            });
        }

        let sessions_msg = connection.recv(deadline)?;
        if !sessions_msg.fds().is_empty() {
            return Err(Error::Identity);
        }
        let sessions_frame =
            ControlFrame::decode_negotiated(sessions_msg.bytes(), DOP_PROTOCOL_FEATURE_MASK)?;
        verify_incoming_envelope(&sessions_frame.envelope, &config, None)?;
        if sessions_frame.envelope.opcode != dop_opcode::ORS_SESSIONS {
            return Err(Error::Control(tickwire_control::Error::Protocol));
        }
        let ors_sessions = OrsSessionsBody::decode(sessions_frame.body)?;
        if ors_sessions.strat_id != config.strat_id || ors_sessions.session_count > 32 {
            return Err(Error::Identity);
        }
        let all_sessions =
            ors_sessions.sessions[..usize::from(ors_sessions.session_count)].to_vec();
        let authorized_sessions = extract_authorized_sessions(config.strat_id, &all_sessions)?;

        let next_order_seqnum = echo.next_source_order_seqnum.max(1);
        if next_order_seqnum >= SEQUENCE_LIMIT {
            return Err(Error::Sequence);
        }
        let hb_interval_ns = u64::from(echo.hb_interval_ms).saturating_mul(1_000_000);

        Ok(Self {
            connection,
            config,
            ready_to_trade: echo.ready_to_trade == 1,
            echo,
            authorized_sessions,
            all_sessions,
            next_order_seqnum,
            next_msg_seqnum: 1,
            hb_interval_ns,
            last_hb_sent_mono_ns: now_mono_ns,
            last_hb_recv_mono_ns: now_mono_ns,
            logged_out: false,
            next_request_nonce: 2,
        })
    }

    #[inline]
    pub fn config(&self) -> &DopConfig {
        &self.config
    }

    #[inline]
    pub fn strat_id(&self) -> u8 {
        self.config.strat_id
    }

    #[inline]
    pub fn echo(&self) -> &DopEchoBody {
        &self.echo
    }

    #[inline]
    pub fn ready_to_trade(&self) -> bool {
        self.ready_to_trade && !self.logged_out
    }

    #[inline]
    pub fn is_logged_out(&self) -> bool {
        self.logged_out
    }

    #[inline]
    pub fn hb_interval_ms(&self) -> u32 {
        self.echo.hb_interval_ms
    }

    #[inline]
    pub fn max_order_age_ns(&self) -> u64 {
        self.config.max_order_age_ns
    }

    #[inline]
    pub fn next_order_seqnum(&self) -> u64 {
        self.next_order_seqnum
    }

    #[inline]
    pub fn next_msg_seqnum(&self) -> u64 {
        self.next_msg_seqnum
    }

    /// All `MktSession` entries reported by ORS in the latest `OrsSessions` snapshot.
    #[inline]
    pub fn all_sessions(&self) -> &[MktSession] {
        &self.all_sessions
    }

    /// Authorized `MktSession` entries (`authorized == 1`, `account_ref in 1..=255`) for this strategy.
    #[inline]
    pub fn authorized_sessions(&self) -> &[MktSession] {
        &self.authorized_sessions
    }

    /// Resolve an outbound `account_ref`: `0` selects the strategy's first authorized account
    /// (`1..=255`), while `1..=255` verifies the strategy is authorized on that `account_ref`.
    pub fn resolve_account_ref(&self, account_ref: u8) -> Result<u8> {
        if account_ref == 0 {
            self.authorized_sessions
                .first()
                .map(|s| s.account_ref)
                .ok_or(Error::UnauthorizedAccount { account_ref: 0 })
        } else if self
            .authorized_sessions
            .iter()
            .any(|s| s.account_ref == account_ref)
        {
            Ok(account_ref)
        } else {
            Err(Error::UnauthorizedAccount { account_ref })
        }
    }

    /// Inspect and return the first ready authorized `MktSession` matching `account_ref` and `product`.
    pub fn resolve_ready_session(
        &self,
        account_ref: u8,
        product: Option<Product>,
    ) -> Result<&MktSession> {
        let resolved_ref = self.resolve_account_ref(account_ref)?;
        self.authorized_sessions
            .iter()
            .find(|s| {
                s.account_ref == resolved_ref
                    && s.ready == 1
                    && match product {
                        None => true,
                        Some(_) if s.product_mask == 0 => true,
                        Some(p) => (s.product_mask & p.mask_bit()) != 0,
                    }
            })
            .ok_or(Error::NotReady)
    }

    /// Claim the next monotonic `msg_seqnum` (`>= 1` since DOP logon).
    pub fn claim_msg_seqnum(&mut self) -> Result<u64> {
        if self.logged_out {
            return Err(Error::NotReady);
        }
        let seq = self.next_msg_seqnum;
        let next = seq
            .checked_add(1)
            .filter(|&v| v < SEQUENCE_LIMIT)
            .ok_or(Error::Sequence)?;
        self.next_msg_seqnum = next;
        Ok(seq)
    }

    /// Claim the next monotonic `order_seqnum` (`>= max(1, DopEcho.next_source_order_seqnum)`).
    pub fn claim_order_seqnum(&mut self) -> Result<u64> {
        if self.logged_out {
            return Err(Error::NotReady);
        }
        let seq = self.next_order_seqnum;
        let next = seq
            .checked_add(1)
            .filter(|&v| v < SEQUENCE_LIMIT)
            .ok_or(Error::Sequence)?;
        self.next_order_seqnum = next;
        Ok(seq)
    }

    /// Synchronize `next_order_seqnum` and `next_msg_seqnum` after an attached `IntentWriter` publishes.
    pub fn observe_published_sequences(&mut self, next_msg_seqnum: u64, next_order_seqnum: u64) {
        if next_msg_seqnum > self.next_msg_seqnum {
            self.next_msg_seqnum = next_msg_seqnum;
        }
        if next_order_seqnum > self.next_order_seqnum {
            self.next_order_seqnum = next_order_seqnum;
        }
    }

    /// Emit a `DopHeartBeat` (`opcode = 100`) if `hb_interval_ms` has elapsed since the previous send.
    pub fn maybe_send_heartbeat(&mut self, now_mono_ns: u64, deadline: Instant) -> Result<bool> {
        if self.logged_out || self.hb_interval_ns == 0 {
            return Ok(false);
        }
        if now_mono_ns.saturating_sub(self.last_hb_sent_mono_ns) < self.hb_interval_ns {
            return Ok(false);
        }
        self.send_heartbeat(now_mono_ns, deadline)?;
        Ok(true)
    }

    /// Unconditionally send one `DopHeartBeat` (`opcode = 100`) control frame to ORS.
    pub fn send_heartbeat(&mut self, now_mono_ns: u64, deadline: Instant) -> Result<()> {
        if self.logged_out {
            return Err(Error::NotReady);
        }
        let req_id = self.next_req_id()?;
        let body = DopHeartBeatBody {
            mono_ns: now_mono_ns,
            next_order_seqnum: self.next_order_seqnum,
            strat_id: self.config.strat_id,
            ready_to_trade: u8::from(self.ready_to_trade),
            _pad_18: [0; 14],
        };
        let mut raw = [0u8; DopHeartBeatBody::SIZE];
        body.encode(&mut raw)?;
        let frame = encode_frame(
            ControlEnvelope {
                opcode: dop_opcode::DOP_HEART_BEAT,
                flags: 32,
                request_id: req_id,
                control_epoch: self.config.control_epoch,
                sender_instance: self.config.local_instance,
                target_instance: self.config.peer_instance,
                ..Default::default()
            },
            &raw,
        )?;
        self.connection.send(&frame, &[], deadline)?;
        self.last_hb_sent_mono_ns = now_mono_ns;
        Ok(())
    }

    /// Poll one non-blocking incoming DOP control frame (`DopHeartBeat`, `DopEcho`, `OrsSessions`, `DopLogout`).
    pub fn try_recv(&mut self, now_mono_ns: u64) -> Result<Option<DopControlEvent>> {
        let Some(msg) = self.connection.try_recv()? else {
            return Ok(None);
        };
        if !msg.fds().is_empty() {
            return Err(Error::Identity);
        }
        let frame = ControlFrame::decode_negotiated(msg.bytes(), DOP_PROTOCOL_FEATURE_MASK)?;
        verify_incoming_envelope(&frame.envelope, &self.config, None)?;
        match frame.envelope.opcode {
            dop_opcode::DOP_HEART_BEAT => {
                let hb = DopHeartBeatBody::decode(frame.body)?;
                if hb.strat_id != self.config.strat_id {
                    return Err(Error::Identity);
                }
                self.ready_to_trade = hb.ready_to_trade == 1;
                if hb.next_order_seqnum > self.next_order_seqnum {
                    self.next_order_seqnum = hb.next_order_seqnum;
                }
                self.last_hb_recv_mono_ns = now_mono_ns;
                Ok(Some(DopControlEvent::HeartBeat(hb)))
            }
            dop_opcode::DOP_ECHO => {
                let echo = DopEchoBody::decode(frame.body)?;
                if echo.strat_id != self.config.strat_id {
                    return Err(Error::Identity);
                }
                self.ready_to_trade = echo.ready_to_trade == 1 && frame.envelope.status == 0;
                if echo.next_source_order_seqnum > self.next_order_seqnum {
                    self.next_order_seqnum = echo.next_source_order_seqnum;
                }
                self.echo = echo.clone();
                Ok(Some(DopControlEvent::Echo(echo)))
            }
            dop_opcode::ORS_SESSIONS => {
                let ors = OrsSessionsBody::decode(frame.body)?;
                if ors.strat_id != self.config.strat_id || ors.session_count > 32 {
                    return Err(Error::Identity);
                }
                let all_sessions = ors.sessions[..usize::from(ors.session_count)].to_vec();
                let authorized = extract_authorized_sessions(self.config.strat_id, &all_sessions)?;
                self.all_sessions = all_sessions.clone();
                self.authorized_sessions = authorized;
                Ok(Some(DopControlEvent::SessionsUpdated(all_sessions)))
            }
            dop_opcode::DOP_LOGOUT => {
                let logout = DopLogoutBody::decode(frame.body)?;
                if logout.strat_id != self.config.strat_id {
                    return Err(Error::Identity);
                }
                self.logged_out = true;
                self.ready_to_trade = false;
                let reason =
                    TerminalReason::from_code(logout.reason).unwrap_or(TerminalReason::AdminAction);
                Ok(Some(DopControlEvent::PeerLogout {
                    reason,
                    cancel_on_logout: logout.cancel_on_logout == 1,
                }))
            }
            _ => Err(Error::Control(tickwire_control::Error::Protocol)),
        }
    }

    /// Send a clean `DopLogout` (`opcode = 97`) to ORS (`cancel_on_logout` optionally canceling open orders).
    pub fn logout(
        &mut self,
        cancel_on_logout: bool,
        reason: TerminalReason,
        deadline: Instant,
    ) -> Result<()> {
        if self.logged_out {
            return Ok(());
        }
        let req_id = self.next_req_id()?;
        let body = DopLogoutBody {
            reason: reason.code(),
            strat_id: self.config.strat_id,
            cancel_on_logout: u8::from(cancel_on_logout),
            _pad_4: [0; 12],
        };
        let mut raw = [0u8; DopLogoutBody::SIZE];
        body.encode(&mut raw)?;
        let frame = encode_frame(
            ControlEnvelope {
                opcode: dop_opcode::DOP_LOGOUT,
                flags: 1,
                request_id: req_id,
                control_epoch: self.config.control_epoch,
                sender_instance: self.config.local_instance,
                target_instance: self.config.peer_instance,
                ..Default::default()
            },
            &raw,
        )?;
        self.connection.send(&frame, &[], deadline)?;
        self.logged_out = true;
        self.ready_to_trade = false;
        Ok(())
    }

    #[inline]
    pub fn last_hb_recv_mono_ns(&self) -> u64 {
        self.last_hb_recv_mono_ns
    }

    #[inline]
    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }

    fn next_req_id(&mut self) -> Result<[u8; 16]> {
        let counter = self.next_request_nonce;
        self.next_request_nonce = counter.checked_add(1).ok_or(Error::Sequence)?;
        Ok(request_id_from_counter(self.config.strat_id, counter))
    }
}

fn request_id_from_counter(strat_id: u8, counter: u64) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = b'D';
    id[1] = b'O';
    id[2] = b'P';
    id[3] = strat_id;
    id[8..16].copy_from_slice(&counter.max(1).to_le_bytes());
    id
}

fn verify_incoming_envelope(
    env: &ControlEnvelope,
    config: &DopConfig,
    expected_request_id: Option<[u8; 16]>,
) -> Result<()> {
    if env.sender_instance != config.peer_instance
        || env.target_instance != config.local_instance
        || env.control_epoch != config.control_epoch
        || env.request_id == [0; 16]
    {
        return Err(Error::Identity);
    }
    if let Some(req_id) = expected_request_id
        && env.request_id != req_id
    {
        return Err(Error::Control(tickwire_control::Error::Protocol));
    }
    Ok(())
}

fn extract_authorized_sessions(strat_id: u8, all: &[MktSession]) -> Result<Vec<MktSession>> {
    let mut authorized = Vec::new();
    for row in all {
        if row.strat_id == strat_id && row.authorized == 1 {
            if row.account_ref == 0 {
                return Err(Error::Identity);
            }
            if !authorized
                .iter()
                .any(|existing: &MktSession| existing.account_ref == row.account_ref)
            {
                authorized.push(row.clone());
            }
        }
    }
    Ok(authorized)
}

/// Create a local authenticated `AF_UNIX` `SOCK_SEQPACKET` connection pair inside a `0700`
/// [`PrivateDirectory`] exclusively via `tickwire-control-sys`.
pub fn local_seqpacket_pair(deadline: Instant) -> Result<(Connection, Connection)> {
    let dir = PrivateDirectory::create_in(Path::new("/tmp"), "tw-seqpkt")?;
    let sock_path = dir.path().join("pair.sock");
    let listener = ControlListener::bind(&sock_path)?;
    let client = Connection::connect(&sock_path, deadline)?;
    let server = listener.accept(deadline)?;
    Ok((server, client))
}

/// Accept a `DopLogin` (`96`) on an ORS-side `SOCK_SEQPACKET` peer and reply with `DopEcho` (`98`)
/// (plus `OrsSessions` (`99`) when accepted), enabling deterministic end-to-end tests over [`local_seqpacket_pair`].
#[allow(clippy::too_many_arguments)]
pub fn accept_dop_login(
    server: &mut Connection,
    ors_instance: [u8; 16],
    expected_strategy_instance: [u8; 16],
    control_epoch: u64,
    echo: &DopEchoBody,
    sessions: &[MktSession],
    server_mono_ns: u64,
    refusal_status: u16,
    deadline: Instant,
) -> Result<DopLoginBody> {
    if sessions.len() > 32 {
        return Err(Error::Bounds);
    }
    let msg = server.recv(deadline)?;
    if !msg.fds().is_empty() {
        return Err(Error::Identity);
    }
    let frame = ControlFrame::decode_negotiated(msg.bytes(), DOP_PROTOCOL_FEATURE_MASK)?;
    if frame.envelope.opcode != dop_opcode::DOP_LOGIN
        || frame.envelope.sender_instance != expected_strategy_instance
        || frame.envelope.target_instance != ors_instance
        || frame.envelope.control_epoch != control_epoch
    {
        return Err(Error::Identity);
    }
    let login = DopLoginBody::decode(frame.body)?;

    let mut raw_echo = [0u8; DopEchoBody::SIZE];
    echo.encode(&mut raw_echo)?;
    let (echo_flags, status) = if refusal_status != 0 || echo.refusal_reason != 0 {
        (18u32, refusal_status.max(1))
    } else {
        (10u32, 0u16)
    };
    let echo_frame = encode_frame(
        ControlEnvelope {
            opcode: dop_opcode::DOP_ECHO,
            flags: echo_flags,
            status,
            request_id: frame.envelope.request_id,
            control_epoch,
            sender_instance: ors_instance,
            target_instance: expected_strategy_instance,
            ..Default::default()
        },
        &raw_echo,
    )?;
    server.send(&echo_frame, &[], deadline)?;

    if status == 0 && echo.refusal_reason == 0 {
        let mut sessions_arr: [MktSession; 32] = core::array::from_fn(|_| MktSession::default());
        for (dst, src) in sessions_arr.iter_mut().zip(sessions.iter()) {
            *dst = src.clone();
        }
        let ors_body = OrsSessionsBody {
            server_mono_ns,
            session_count: sessions.len() as u16,
            strat_id: login.strat_id,
            _pad_11: [0; 5],
            sessions: sessions_arr,
        };
        let mut raw_sessions = [0u8; OrsSessionsBody::SIZE];
        ors_body.encode(&mut raw_sessions)?;
        let sessions_frame = encode_frame(
            ControlEnvelope {
                opcode: dop_opcode::ORS_SESSIONS,
                flags: 10,
                status: 0,
                request_id: request_id_from_counter(login.strat_id, 100),
                control_epoch,
                sender_instance: ors_instance,
                target_instance: expected_strategy_instance,
                ..Default::default()
            },
            &raw_sessions,
        )?;
        server.send(&sessions_frame, &[], deadline)?;
    }

    Ok(login)
}
