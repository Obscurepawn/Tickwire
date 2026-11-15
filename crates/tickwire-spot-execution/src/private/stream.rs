//! Stateless single-threaded `RESP` private WebSocket state machine (`UtaPrivateStream`) for
//! Bybit V5 (`/v5/private`) and OKX V5 (`/ws/v5/private`) Unified Trading Accounts.
//!
//! Operates strictly on the `RESP` thread with zero `Mutex`, zero shared index state, and zero
//! local persistence, driving authentication (`op: "auth"` / `op: "login"`), multi-step UTA
//! topic subscription (`["order","execution","execution.fast","wallet"]` on Bybit; step 0
//! `orders` `instType: "ANY"` + step 1 `balance_and_position` on OKX), `20s` heartbeat frames,
//! and single-pass `JsonScan` event extraction.

use crate::{
    Credentials, SpotVenue, UtaFrameEvent, UtaScanSummary, UtaVenueError, WS_PING_PERIOD_NS,
    check_bybit_auth_reply, check_okx_login_reply, encode_bybit_auth_into,
    encode_bybit_private_subscribe_step_into, encode_okx_login_into,
    encode_okx_subscribe_step_into, encode_ws_client_frame_into, encode_ws_ping_into,
    scan_bybit_frame, scan_okx_frame, try_decode_ws_server_frame, ws_envelope::WsLoginOutcome,
};
use tickwire_transport::ws::Opcode;

/// Authentication and subscription phase of a single-threaded [`UtaPrivateStream`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UtaPrivatePhase {
    Disconnected,
    Authenticating,
    Subscribing { next_step: u8 },
    Subscribed,
    DrainingForMaintenance,
    Failed,
}

/// Preallocated single-threaded `RESP` private WebSocket protocol driver.
pub struct UtaPrivateStream {
    venue: SpotVenue,
    phase: UtaPrivatePhase,
    rx_buf: Box<[u8]>,
    rx_len: usize,
    last_rx_mono_ns: u64,
    last_ping_mono_ns: u64,
    frame_seq: u64,
}

impl UtaPrivateStream {
    /// Allocates a single-threaded `RESP` private WebSocket driver with fixed RX capacity.
    pub fn new(venue: SpotVenue, rx_capacity: usize) -> Self {
        let cap = rx_capacity.max(16 * 1024);
        Self {
            venue,
            phase: UtaPrivatePhase::Disconnected,
            rx_buf: vec![0u8; cap].into_boxed_slice(),
            rx_len: 0,
            last_rx_mono_ns: 0,
            last_ping_mono_ns: 0,
            frame_seq: 1,
        }
    }

    /// Zero-allocation empty placeholder (`Box::new([])` performs zero heap allocation and zero
    /// deallocation on drop) used by `RESP` to swap [`UtaPrivateStream`] out of its fine-grained
    /// spinlock while decoding WebSocket frames outside all locks.
    #[inline]
    pub fn empty_placeholder(venue: SpotVenue) -> Self {
        Self {
            venue,
            phase: UtaPrivatePhase::Disconnected,
            rx_buf: Box::new([]),
            rx_len: 0,
            last_rx_mono_ns: 0,
            last_ping_mono_ns: 0,
            frame_seq: 1,
        }
    }

    #[inline]
    pub const fn has_rx_capacity(&self) -> bool {
        !self.rx_buf.is_empty()
    }

    #[inline]
    pub const fn venue(&self) -> SpotVenue {
        self.venue
    }

    #[inline]
    pub const fn phase(&self) -> UtaPrivatePhase {
        self.phase
    }

    #[inline]
    pub const fn is_subscribed(&self) -> bool {
        matches!(self.phase, UtaPrivatePhase::Subscribed)
    }

    /// Resets stream state upon socket handoff and encodes the masked WebSocket auth/login frame into `out`.
    pub fn begin_login(
        &mut self,
        credentials: &Credentials<'_>,
        utc_ms: u64,
        now_mono_ns: u64,
        out: &mut [u8],
    ) -> Result<usize, UtaVenueError> {
        self.rx_len = 0;
        self.last_rx_mono_ns = now_mono_ns;
        self.last_ping_mono_ns = now_mono_ns;
        let mut json_buf = [0u8; 512];
        let json_len = match self.venue {
            SpotVenue::Bybit => encode_bybit_auth_into(credentials, utc_ms, &mut json_buf)?,
            SpotVenue::Okx => encode_okx_login_into(credentials, utc_ms, &mut json_buf)?,
        };
        self.phase = UtaPrivatePhase::Authenticating;
        self.next_masked_frame(Opcode::Text, &json_buf[..json_len], out)
    }

    /// Encodes a due `20s` WebSocket text heartbeat into `out` when `now_mono_ns - last_ping_mono_ns >= 20s`.
    pub fn encode_due_ping(
        &mut self,
        now_mono_ns: u64,
        out: &mut [u8],
    ) -> Result<usize, UtaVenueError> {
        if !matches!(
            self.phase,
            UtaPrivatePhase::Subscribing { .. } | UtaPrivatePhase::Subscribed
        ) {
            return Ok(0);
        }
        if now_mono_ns.saturating_sub(self.last_ping_mono_ns) < WS_PING_PERIOD_NS {
            return Ok(0);
        }
        let mut ping_json = [0u8; 32];
        let plen = encode_ws_ping_into(self.venue, &mut ping_json)?;
        self.last_ping_mono_ns = now_mono_ns;
        self.next_masked_frame(Opcode::Text, &ping_json[..plen], out)
    }

    /// Appends raw TLS-decrypted bytes into the preallocated RX buffer and processes all complete
    /// RFC 6455 server frames via single-pass `JsonScan` without heap allocation.
    ///
    /// Any outbound follow-up control frame (subscription step or RFC 6455 `Pong`) is written into
    /// `out_tx` (`out_tx_written` returns the byte length to transmit).
    pub fn on_raw_bytes(
        &mut self,
        raw: &[u8],
        now_mono_ns: u64,
        out_tx: &mut [u8],
        mut on_event: impl FnMut(UtaFrameEvent<'_>),
    ) -> Result<(UtaScanSummary, usize), UtaVenueError> {
        if raw.is_empty() {
            return Ok((UtaScanSummary::default(), 0));
        }
        let end = self
            .rx_len
            .checked_add(raw.len())
            .ok_or(UtaVenueError::Bounds)?;
        if end > self.rx_buf.len() {
            self.phase = UtaPrivatePhase::Failed;
            return Err(UtaVenueError::Bounds);
        }
        self.rx_buf[self.rx_len..end].copy_from_slice(raw);
        self.rx_len = end;
        self.last_rx_mono_ns = now_mono_ns;

        let mut agg = UtaScanSummary::default();
        let mut tx_pos = 0usize;
        let mut consumed_total = 0usize;

        loop {
            let frame_opt = try_decode_ws_server_frame(&self.rx_buf[consumed_total..self.rx_len])?;
            let Some(frame) = frame_opt else {
                break;
            };
            let opcode_raw = frame.opcode_raw;
            let frame_consumed = frame.consumed;
            let payload_len = frame.payload.len();
            let payload_start = consumed_total + (frame_consumed - payload_len);
            let payload_end = consumed_total + frame_consumed;
            consumed_total += frame_consumed;

            match opcode_raw {
                9 => {
                    let mut ping_buf = [0u8; 128];
                    if payload_len > ping_buf.len() {
                        return Err(UtaVenueError::Bounds);
                    }
                    ping_buf[..payload_len]
                        .copy_from_slice(&self.rx_buf[payload_start..payload_end]);
                    let n = self.next_masked_frame(
                        Opcode::Pong,
                        &ping_buf[..payload_len],
                        &mut out_tx[tx_pos..],
                    )?;
                    tx_pos += n;
                }
                8 => {
                    self.phase = UtaPrivatePhase::Disconnected;
                    break;
                }
                1 => {
                    let mut text_buf = [0u8; 16 * 1024];
                    if payload_len > text_buf.len() {
                        return Err(UtaVenueError::Bounds);
                    }
                    text_buf[..payload_len]
                        .copy_from_slice(&self.rx_buf[payload_start..payload_end]);
                    let step_tx = self.handle_text_payload(
                        &text_buf[..payload_len],
                        &mut out_tx[tx_pos..],
                        &mut agg,
                        &mut on_event,
                    )?;
                    tx_pos += step_tx;
                }
                _ => {}
            }
        }

        if consumed_total > 0 {
            self.rx_buf.copy_within(consumed_total..self.rx_len, 0);
            self.rx_len -= consumed_total;
        }
        Ok((agg, tx_pos))
    }

    fn handle_text_payload(
        &mut self,
        payload: &[u8],
        out_tx: &mut [u8],
        agg: &mut UtaScanSummary,
        on_event: &mut impl FnMut(UtaFrameEvent<'_>),
    ) -> Result<usize, UtaVenueError> {
        if self.phase == UtaPrivatePhase::Authenticating {
            let outcome = match self.venue {
                SpotVenue::Bybit => check_bybit_auth_reply(payload),
                SpotVenue::Okx => check_okx_login_reply(payload),
            };
            match outcome {
                WsLoginOutcome::Accepted => {
                    let mut sub_json = [0u8; 256];
                    let slen = self.encode_sub_step(0, &mut sub_json)?;
                    self.phase = UtaPrivatePhase::Subscribing { next_step: 1 };
                    return self.next_masked_frame(Opcode::Text, &sub_json[..slen], out_tx);
                }
                WsLoginOutcome::Rejected => {
                    self.phase = UtaPrivatePhase::Failed;
                    return Err(UtaVenueError::Identity);
                }
                WsLoginOutcome::NotLoginFrame => {}
            }
        }

        let mut saw_sub_ok = false;
        let summary = match self.venue {
            SpotVenue::Bybit => scan_bybit_frame(payload, |ev| {
                if let UtaFrameEvent::Control {
                    op: "subscribe",
                    success: true,
                } = ev
                {
                    saw_sub_ok = true;
                }
                on_event(ev);
            }),
            SpotVenue::Okx => scan_okx_frame(payload, |ev| {
                if let UtaFrameEvent::Control {
                    op: "subscribe",
                    success: true,
                } = ev
                {
                    saw_sub_ok = true;
                }
                on_event(ev);
            }),
        }?;

        agg.events_emitted = agg.events_emitted.saturating_add(summary.events_emitted);
        agg.rate_limited |= summary.rate_limited;
        if summary.graceful_drain_connection {
            agg.graceful_drain_connection = true;
            self.phase = UtaPrivatePhase::DrainingForMaintenance;
        }

        if saw_sub_ok && let UtaPrivatePhase::Subscribing { next_step } = self.phase {
            let mut sub_json = [0u8; 256];
            let slen = self.encode_sub_step(next_step, &mut sub_json)?;
            if slen == 0 {
                self.phase = UtaPrivatePhase::Subscribed;
            } else {
                self.phase = UtaPrivatePhase::Subscribing {
                    next_step: next_step + 1,
                };
                return self.next_masked_frame(Opcode::Text, &sub_json[..slen], out_tx);
            }
        }
        Ok(0)
    }

    fn encode_sub_step(&self, step: u8, out: &mut [u8]) -> Result<usize, UtaVenueError> {
        match self.venue {
            SpotVenue::Bybit => encode_bybit_private_subscribe_step_into(step, out),
            SpotVenue::Okx => encode_okx_subscribe_step_into(step, out),
        }
    }

    fn next_masked_frame(
        &mut self,
        opcode: Opcode,
        payload: &[u8],
        out: &mut [u8],
    ) -> Result<usize, UtaVenueError> {
        let seed = self.frame_seq;
        self.frame_seq = self.frame_seq.wrapping_add(1);
        encode_ws_client_frame_into(opcode, payload, seed, out)
    }
}
