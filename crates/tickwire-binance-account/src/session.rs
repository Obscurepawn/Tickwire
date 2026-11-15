//! Unified `Send + Sync` full-market `BinanceSession` across Spot, USD-M, Coin-M, and European
//! Options (`human-docs/8.4`, `8.5`, `12/binance.md`, `13.9`).
//!
//! Operates concurrently across physically isolated `REQ`, `RESP`, `Dialer`, and `AsyncRestQuerier`
//! threads with zero WAL/journal dependencies, zero single-threaded cell wrappers, zero coarse mutexes, and
//! strict `Order -> StpBook` / non-overlapping `Connection` spinlock discipline.

use core::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use tickwire_account::{
    InlineAscii, OrderManager, OrsAccountDomainConfig, OrsBootstrapSummary, OrsCancelObservation,
    OrsCancelRejectedReport, OrsCancelRequest, OrsFeeSpec, OrsFillObservation, OrsModifyRequest,
    OrsNewOrderRequest, OrsOrderQuerySnapshot, OrsOrderRefusal, OrsOrderStatus,
    OrsQueriedOrderStatus, OrsQueryOrigin, OrsQueryReconcileAction, OrsReport, make_ors_order_id,
    ors_query_desk::MAX_CANCEL_BACKOFF_ATTEMPTS, split_ors_order_id,
};
use tickwire_binance::{
    auth::SigningKey,
    full_market::{
        BinanceVenueMarket, OptionsHttpCancelParams, OptionsHttpNewParams, WsOrderCancelParams,
        WsOrderModifyParams, WsOrderNewParams, classify_binance_terminal_reason,
        decorate_listen_key_stream_url, encode_options_http_cancel, encode_options_http_place,
        encode_signed_account_target, encode_signed_open_orders_target,
        encode_signed_order_status_target, encode_ws_order_cancel, encode_ws_order_modify,
        encode_ws_order_place, encode_ws_session_logon, encode_ws_user_stream_subscribe,
        explain_binance_error,
    },
    request::Transport,
};
use tickwire_binance_quota::{
    AccountRateOverrides, BinanceMarket, CommandAction, ExchRateLimitEvent, ExchRateLimitLevel,
    IpBudgetSpec, IpKey, RateFlags, RateLimitId, RateReject, RouteAdmissionReject,
    SharedBinanceOrsQuota,
};
use tickwire_transport::mio::Waker;
use tickwire_transport::{
    Error as TransportError,
    dialer::{DialSpec, DialedLink, Dialer},
    json_scan::parse_u64_ascii,
    latency_tracer::{LatencyTracer, ThreadTag},
    links::{
        GuardedValue, HttpsLink, LinkKind, LinkOps, LockedConnection, RequestRef, SendOutcome,
        UrlBindPair, UrlConnections, WsLink, WsLoginState,
    },
    order_router::{MAX_PUBLISHED_ROUTES, PublishedRoutes, RouterConfig, SampleKind, SampleMask},
    ws,
};
use tickwire_venue as v;

use crate::{
    bootstrap::{
        BinanceBootstrapCatalog, BinanceBootstrapError, bootstrap_market_into_order_manager,
        parse_scaled_i128,
    },
    msg_parser::{
        BinanceMsgParser, BinanceRateLimitRow, BinanceRequestKind, decode_ws_request_id,
        encode_ws_request_id, pack_ws_reply_id, parse_step_i64, unpack_ws_reply_id,
    },
    urls::{
        BinanceConfigError, BinanceResolvedMarketUrls, BinanceUrlId, UrlRole, resolve_market_urls,
    },
};

/// Default delay (`6,000 ms`) before firing a `StatusAsk` query after `-1007` / `-1000` / link drop
/// (`human-docs/8.5`, `12/binance.md`).
pub const DEFAULT_STATUS_QUERY_DELAY_MS: u64 = 6_000;

/// Default `recvWindow` (`60,000 ms`) on `GET /{api,fapi,dapi,eapi}/v*/order` status queries so
/// `AsyncRestQuerier` tolerates clock skew during reconciliation (`human-docs/12/binance.md`).
pub const STATUS_QUERY_RECV_WINDOW_MS: u32 = 60_000;

/// Default `recvWindow` (`5,000 ms`) on live order-entry and session logon frames.
pub const DEFAULT_ORDER_RECV_WINDOW_MS: u32 = 5_000;

/// `listenKey` refresh interval (`1,800 s` = 30 min, half of Binance's 60 min expiry; `8.5`, `13.9`).
pub const LISTEN_KEY_REFRESH_S: u32 = 1_800;

/// Default WebSocket ping interval (`15 s`) on Binance order and user-data stream sockets.
pub const DEFAULT_WS_PING_PERIOD_S: u32 = 15;

/// Default maximum client WebSocket control frame size (`256 B`).
const WS_CONTROL_MAX_BYTES: usize = 256;

const NS_PER_MS: u64 = 1_000_000;
const NS_PER_SEC: u64 = 1_000_000_000;
const REQ_SEQ_MASK: u64 = 0x00ff_ffff_ffff_ffff;

#[inline]
const fn quota_market(market: BinanceVenueMarket) -> BinanceMarket {
    match market {
        BinanceVenueMarket::Spot => BinanceMarket::Spot,
        BinanceVenueMarket::Usdm => BinanceMarket::Usdm,
        BinanceVenueMarket::Coinm => BinanceMarket::Coinm,
        BinanceVenueMarket::Options => BinanceMarket::Options,
    }
}

#[inline]
const fn command_transport(market: BinanceVenueMarket) -> Transport {
    match market {
        BinanceVenueMarket::Spot | BinanceVenueMarket::Usdm | BinanceVenueMarket::Coinm => {
            Transport::WebSocket
        }
        BinanceVenueMarket::Options => Transport::Rest,
    }
}

#[inline]
fn ipv4_u32_to_ip_key(ip_u32: u32) -> IpKey {
    if ip_u32 == 0 {
        IpKey::NONE
    } else {
        IpKey::from_ip(IpAddr::V4(Ipv4Addr::from(ip_u32)))
    }
}

#[inline]
fn scaled_atoms_i64(steps: i64, step_atoms: i128) -> Option<i64> {
    if steps <= 0 || step_atoms <= 0 {
        return None;
    }
    let prod = i128::from(steps).checked_mul(step_atoms)?;
    i64::try_from(prod).ok().filter(|&v| v > 0)
}

/// Wraps `buf[..json_len]` in-place into a masked RFC 6455 client `Text` frame (`0x81`, `MASK`,
/// 4-byte SplitMix64 mask) with zero heap allocation and zero syscalls.
#[inline]
fn wrap_ws_client_text_frame_in_place(buf: &mut [u8], json_len: usize, seed: u64) -> Option<usize> {
    if json_len > buf.len() || core::str::from_utf8(&buf[..json_len]).is_err() {
        return None;
    }
    let header_len = if json_len < 126 {
        6usize
    } else if json_len <= usize::from(u16::MAX) {
        8usize
    } else {
        return None;
    };
    let total = json_len.checked_add(header_len)?;
    if total > buf.len() {
        return None;
    }
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    let mask = ((z as u32) | 0x0101_0101).to_be_bytes();

    buf.copy_within(0..json_len, header_len);
    buf[0] = 0x81;
    if json_len < 126 {
        buf[1] = 0x80 | (json_len as u8);
        buf[2..6].copy_from_slice(&mask);
    } else {
        buf[1] = 0x80 | 126;
        buf[2..4].copy_from_slice(&(json_len as u16).to_be_bytes());
        buf[4..8].copy_from_slice(&mask);
    }
    for (i, byte) in buf[header_len..total].iter_mut().enumerate() {
        *byte ^= mask[i & 3];
    }
    Some(total)
}

/// Decodes and unmasks an outbound RFC 6455 client `Text` frame (`FIN | Text`, masked) into `out`
/// with zero heap allocation, returning the UTF-8 JSON payload byte length.
pub fn unmask_ws_client_text_frame(frame: &[u8], out: &mut [u8]) -> Option<usize> {
    if frame.len() < 6 || frame[0] != 0x81 || (frame[1] & 0x80) == 0 {
        return None;
    }
    let len_code = usize::from(frame[1] & 0x7f);
    let (payload_len, mask_offset) = if len_code < 126 {
        (len_code, 2usize)
    } else if len_code == 126 {
        if frame.len() < 8 {
            return None;
        }
        (
            usize::from(u16::from_be_bytes([frame[2], frame[3]])),
            4usize,
        )
    } else {
        return None;
    };
    let payload_offset = mask_offset + 4;
    if frame.len() != payload_offset.checked_add(payload_len)? || out.len() < payload_len {
        return None;
    }
    let mask = [
        frame[mask_offset],
        frame[mask_offset + 1],
        frame[mask_offset + 2],
        frame[mask_offset + 3],
    ];
    for i in 0..payload_len {
        out[i] = frame[payload_offset + i] ^ mask[i & 3];
    }
    core::str::from_utf8(&out[..payload_len]).ok()?;
    Some(payload_len)
}

/// Correlation hint attached to an outbound `WsLink` or `HttpsLink` order request (`8.4`, `8.5`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BinanceRequestRef {
    pub ors_order_id: u64,
    pub kind: BinanceRequestKind,
}

impl BinanceRequestRef {
    #[inline]
    pub const fn new(ors_order_id: u64, kind: BinanceRequestKind) -> Self {
        Self { ors_order_id, kind }
    }

    #[inline]
    pub const fn to_transport_ref(self, seq: u64, sent_tsc: u64, sent_mono_ns: u64) -> RequestRef {
        let kind_tag = match self.kind {
            BinanceRequestKind::Place => 1u64,
            BinanceRequestKind::Cancel => 2u64,
            BinanceRequestKind::Modify => 3u64,
            BinanceRequestKind::Other => 4u64,
        };
        RequestRef {
            order_key: self.ors_order_id,
            client_seq: (kind_tag << 56) | (seq & REQ_SEQ_MASK),
            sent_tsc,
            sent_mono_ns,
        }
    }

    #[inline]
    pub const fn from_transport_ref(raw: RequestRef) -> Option<Self> {
        if raw.order_key == 0 {
            return None;
        }
        let kind = match (raw.client_seq >> 56) as u8 {
            1 => BinanceRequestKind::Place,
            2 => BinanceRequestKind::Cancel,
            3 => BinanceRequestKind::Modify,
            4 => BinanceRequestKind::Other,
            _ => return None,
        };
        Some(Self {
            ors_order_id: raw.order_key,
            kind,
        })
    }
}

/// Result of inspecting a candidate WebSocket `session.logon` reply (`8.4` `LoginReply`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinanceLoginReply {
    Pending,
    Ok,
    Failed,
}

/// Outbound `NewOrder` intent submitted to [`BinanceSession::submit_new`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BinanceNewOrderIntent {
    pub strat_id: u8,
    pub account_ref: u8,
    pub msg_seqnum: u64,
    pub order_seqnum: u64,
    pub symbol_id: u32,
    pub side: v::Side,
    pub order_type: v::OrderType,
    /// Limit price ticks (`> 0`); for `OrderType::Market`, acts as the worst-case hold/collar
    /// reference price ticks inside `OrderManager` while omitting `"price"` on the wire.
    pub price_ticks: i64,
    pub qty_lots: i64,
    pub tif: v::TimeInForce,
    pub post_only: bool,
    pub reduce_only: bool,
    pub position_side: v::PositionSide,
    pub stp_mode: v::SelfTradePrevention,
    pub check_stp: bool,
    pub dry_run: bool,
    pub wall_ms: u64,
    pub now_mono_ns: u64,
}

impl BinanceNewOrderIntent {
    #[inline]
    pub const fn to_ors_request(&self) -> OrsNewOrderRequest {
        OrsNewOrderRequest {
            strat_id: self.strat_id,
            account_ref: self.account_ref,
            msg_seqnum: self.msg_seqnum,
            order_seqnum: self.order_seqnum,
            symbol_id: self.symbol_id,
            side: self.side,
            price_ticks: self.price_ticks,
            qty_lots: self.qty_lots,
            tif: self.tif,
            post_only: self.post_only,
            reduce_only: self.reduce_only,
            check_stp: self.check_stp,
            dry_run: self.dry_run,
            now_mono_ns: self.now_mono_ns,
        }
    }
}

/// Result of [`BinanceSession::submit_new`] on `REQ`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinanceSubmitOutcome {
    Sent {
        ors_order_id: u64,
        conn_id: u16,
        client_id: InlineAscii<64>,
        encoded_len: usize,
    },
    DryRunEncoded {
        ors_order_id: u64,
        client_id: InlineAscii<64>,
        encoded_len: usize,
    },
    LocalNack {
        ors_order_id: u64,
        reason: v::TerminalReason,
    },
    AmbiguousSendScheduledQuery {
        ors_order_id: u64,
        conn_id: u16,
    },
}

/// Result of [`BinanceSession::submit_cancel`] on `REQ`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinanceCancelOutcome {
    Sent {
        ors_order_id: u64,
        conn_id: u16,
        encoded_len: usize,
    },
    QueuedBeforeAckOrQuery {
        ors_order_id: u64,
        coalesced_in_query_desk: bool,
    },
    LocalCancelNack {
        ors_order_id: u64,
        reason: v::TerminalReason,
    },
    AmbiguousSendScheduledQuery {
        ors_order_id: u64,
        conn_id: u16,
    },
}

/// Result of [`BinanceSession::submit_modify`] on `REQ`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinanceModifyOutcome {
    Sent {
        ors_order_id: u64,
        conn_id: u16,
        encoded_len: usize,
    },
    LocalModifyNack {
        ors_order_id: u64,
        reason: v::TerminalReason,
    },
}

/// Rising-edge exchange `rateLimits[]` notification broadcast to strategies (`8.5`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExchRateLimitNotice {
    pub account_ref: u8,
    pub market: BinanceVenueMarket,
    pub rate_limit_type: InlineAscii<24>,
    pub interval_initial: u8,
    pub interval_num: u32,
    pub count: u64,
    pub limit: u64,
    pub warn_pct: u8,
    pub hard_limit_hit: bool,
    pub observed_at_ns: u64,
}

/// Event sink receiving `RESP` / `REQ` / `QueryDesk` reports and notices outside spinlocks.
pub trait BinanceEventSink {
    fn on_report(&mut self, report: OrsReport);

    fn on_exch_rate_limit(&mut self, _notice: ExchRateLimitNotice) {}

    fn on_stream_token_expired(&mut self, _account_ref: u8, _market: BinanceVenueMarket) {}

    fn on_venue_amendment(
        &mut self,
        _account_ref: u8,
        _ors_order_id: u64,
        _new_price_ticks: i64,
        _new_qty_lots: i64,
    ) {
    }

    fn on_cancel_query_exhausted(&mut self, _account_ref: u8, _ors_order_id: u64) {}
}

impl BinanceEventSink for Vec<OrsReport> {
    #[inline]
    fn on_report(&mut self, report: OrsReport) {
        self.push(report);
    }
}

/// Signed/authenticated REST request descriptor built off the hot path for `AsyncRestQuerier`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinanceRestCall {
    pub account_ref: u8,
    pub market: BinanceVenueMarket,
    pub method: &'static str,
    pub host: String,
    pub port: u16,
    pub target: String,
    pub api_key: String,
    pub ors_order_id: u64,
    pub kind: BinanceRequestKind,
}

/// Outcome of [`BinanceSession::apply_status`] on `RESP` / `QueryDesk` completion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinanceStatusApplyOutcome {
    Reconciled,
    DispatchedCoalescedCancel {
        ors_order_id: u64,
    },
    CancelRejectedOpen {
        ors_order_id: u64,
        remaining_pending_cxl: u8,
    },
    RetryingPlace {
        ors_order_id: u64,
        next_due_ns: u64,
    },
    RetryingCancel {
        ors_order_id: u64,
        next_due_ns: Option<u64>,
    },
    ClosedInternalCancel {
        ors_order_id: u64,
    },
    AlreadyClosed,
}

/// Session configuration and runtime errors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinanceSessionError {
    UrlConfig(BinanceConfigError),
    Bootstrap(BinanceBootstrapError),
    Quota(RateReject),
    Transport(String),
    UnconfiguredMarket(BinanceVenueMarket),
    UnknownSymbol(u32),
    MissingListenKey(BinanceVenueMarket),
    OrderNotFound(u64),
    EncodeFailed(&'static str),
    ParseFailed,
    InvalidConfig(&'static str),
}

impl From<BinanceConfigError> for BinanceSessionError {
    #[inline]
    fn from(err: BinanceConfigError) -> Self {
        Self::UrlConfig(err)
    }
}

impl From<BinanceBootstrapError> for BinanceSessionError {
    #[inline]
    fn from(err: BinanceBootstrapError) -> Self {
        Self::Bootstrap(err)
    }
}

impl From<RateReject> for BinanceSessionError {
    #[inline]
    fn from(err: RateReject) -> Self {
        Self::Quota(err)
    }
}

impl From<TransportError> for BinanceSessionError {
    #[inline]
    fn from(err: TransportError) -> Self {
        Self::Transport(format!("{err:?}"))
    }
}

/// Cold-startup authentication material parsed into three isolated [`SigningKey`] instances
/// (`REQ`, `Dialer` `session.logon`, and `AsyncRestQuerier`) without requiring `Clone` on
/// key material (`human-docs/8.5`, `12/binance.md`).
#[derive(Clone)]
pub enum BinanceMarketAuth {
    /// Ed25519 PKCS#8 PEM (`-----BEGIN PRIVATE KEY-----`) for Spot, USD-M, and Coin-M WebSocket
    /// `session.logon` and signed REST queries, with optional HMAC secret override for REST.
    Ed25519Pem {
        api_key: String,
        private_key_pem: String,
        rest_hmac_secret: Option<Vec<u8>>,
    },
    /// Ed25519 PKCS#8 DER bytes for Spot, USD-M, and Coin-M.
    Ed25519Pkcs8 {
        api_key: String,
        private_key_der: Vec<u8>,
        rest_hmac_secret: Option<Vec<u8>>,
    },
    /// HMAC-SHA256 secret for European Options (`POST`/`DELETE /eapi/v1/order` + `/eapi/v1/*` REST).
    HmacSha256 { api_key: String, secret: Vec<u8> },
}

impl core::fmt::Debug for BinanceMarketAuth {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Ed25519Pem { api_key, .. } => f
                .debug_struct("Ed25519Pem")
                .field("api_key", api_key)
                .finish_non_exhaustive(),
            Self::Ed25519Pkcs8 { api_key, .. } => f
                .debug_struct("Ed25519Pkcs8")
                .field("api_key", api_key)
                .finish_non_exhaustive(),
            Self::HmacSha256 { api_key, .. } => f
                .debug_struct("HmacSha256")
                .field("api_key", api_key)
                .finish_non_exhaustive(),
        }
    }
}

impl BinanceMarketAuth {
    pub fn ed25519_pem(api_key: impl Into<String>, private_key_pem: impl Into<String>) -> Self {
        Self::Ed25519Pem {
            api_key: api_key.into(),
            private_key_pem: private_key_pem.into(),
            rest_hmac_secret: None,
        }
    }

    pub fn ed25519_pkcs8(api_key: impl Into<String>, private_key_der: Vec<u8>) -> Self {
        Self::Ed25519Pkcs8 {
            api_key: api_key.into(),
            private_key_der,
            rest_hmac_secret: None,
        }
    }

    pub fn hmac_sha256(api_key: impl Into<String>, secret: impl Into<Vec<u8>>) -> Self {
        Self::HmacSha256 {
            api_key: api_key.into(),
            secret: secret.into(),
        }
    }

    #[inline]
    pub fn api_key(&self) -> &str {
        match self {
            Self::Ed25519Pem { api_key, .. }
            | Self::Ed25519Pkcs8 { api_key, .. }
            | Self::HmacSha256 { api_key, .. } => api_key,
        }
    }

    fn build_isolated_signers(
        &self,
        market: BinanceVenueMarket,
    ) -> Result<(SigningKey, SigningKey, SigningKey), BinanceConfigError> {
        let api_key = self.api_key();
        if api_key.is_empty() || !api_key.is_ascii() {
            return Err(BinanceConfigError::MissingApiKey);
        }
        match (market, self) {
            (
                BinanceVenueMarket::Spot | BinanceVenueMarket::Usdm | BinanceVenueMarket::Coinm,
                Self::Ed25519Pem {
                    private_key_pem,
                    rest_hmac_secret,
                    ..
                },
            ) => {
                if private_key_pem.trim().is_empty() {
                    return Err(BinanceConfigError::MissingEd25519Pem { market });
                }
                let req_signer = SigningKey::ed25519_pem(private_key_pem)
                    .map_err(|_| BinanceConfigError::InvalidEd25519Pem { market })?;
                let dialer_signer = SigningKey::ed25519_pem(private_key_pem)
                    .map_err(|_| BinanceConfigError::InvalidEd25519Pem { market })?;
                let rest_signer = match rest_hmac_secret {
                    Some(sec) => {
                        SigningKey::hmac(sec).map_err(|_| BinanceConfigError::InvalidHmacSecret)?
                    }
                    None => SigningKey::ed25519_pem(private_key_pem)
                        .map_err(|_| BinanceConfigError::InvalidEd25519Pem { market })?,
                };
                Ok((req_signer, dialer_signer, rest_signer))
            }
            (
                BinanceVenueMarket::Spot | BinanceVenueMarket::Usdm | BinanceVenueMarket::Coinm,
                Self::Ed25519Pkcs8 {
                    private_key_der,
                    rest_hmac_secret,
                    ..
                },
            ) => {
                if private_key_der.is_empty() {
                    return Err(BinanceConfigError::MissingEd25519Pem { market });
                }
                let req_signer = SigningKey::ed25519_pkcs8(private_key_der)
                    .map_err(|_| BinanceConfigError::InvalidEd25519Pem { market })?;
                let dialer_signer = SigningKey::ed25519_pkcs8(private_key_der)
                    .map_err(|_| BinanceConfigError::InvalidEd25519Pem { market })?;
                let rest_signer = match rest_hmac_secret {
                    Some(sec) => {
                        SigningKey::hmac(sec).map_err(|_| BinanceConfigError::InvalidHmacSecret)?
                    }
                    None => SigningKey::ed25519_pkcs8(private_key_der)
                        .map_err(|_| BinanceConfigError::InvalidEd25519Pem { market })?,
                };
                Ok((req_signer, dialer_signer, rest_signer))
            }
            (
                BinanceVenueMarket::Spot | BinanceVenueMarket::Usdm | BinanceVenueMarket::Coinm,
                Self::HmacSha256 { .. },
            ) => Err(BinanceConfigError::MissingEd25519Pem { market }),
            (BinanceVenueMarket::Options, Self::HmacSha256 { secret, .. }) => {
                if secret.is_empty() {
                    return Err(BinanceConfigError::MissingHmacSecret { market });
                }
                let req_signer =
                    SigningKey::hmac(secret).map_err(|_| BinanceConfigError::InvalidHmacSecret)?;
                let dialer_signer =
                    SigningKey::hmac(secret).map_err(|_| BinanceConfigError::InvalidHmacSecret)?;
                let rest_signer =
                    SigningKey::hmac(secret).map_err(|_| BinanceConfigError::InvalidHmacSecret)?;
                Ok((req_signer, dialer_signer, rest_signer))
            }
            (BinanceVenueMarket::Options, Self::Ed25519Pem { .. } | Self::Ed25519Pkcs8 { .. }) => {
                Err(BinanceConfigError::MissingHmacSecret { market })
            }
        }
    }
}

/// Per-market configuration inside a single-market or unified multi-market `BinanceAccountSpec`.
#[derive(Clone, Debug)]
pub struct BinanceMarketConfig {
    pub market: BinanceVenueMarket,
    pub auth: BinanceMarketAuth,
    /// Optional explicit URL catalog overrides (`colo` OR `public`, validated via
    /// [`resolve_market_urls`]); empty selects canonical environment defaults (`8.5`).
    pub urls: Vec<BinanceUrlId>,
    /// Order pool `[local_ip, count]` bindings (`WsLink` on Spot/UM/CM; `HttpsLink` on Options).
    pub order_binds: Vec<UrlBindPair>,
    /// Stream pool `[local_ip, count]` bindings (`listenKey` WS on UM/CM/Options; empty on Spot
    /// because Spot subscribes via `userDataStream.subscribe` on its order pool).
    pub stream_binds: Vec<UrlBindPair>,
    pub rate_overrides: AccountRateOverrides,
    pub ip_budgets: Vec<IpBudgetSpec>,
}

/// Complete startup specification for one `BinanceSession` (`1 AccountSession = 1 Exchange UID`).
#[derive(Clone, Debug)]
pub struct BinanceAccountSpec {
    pub account_domain: OrsAccountDomainConfig,
    pub markets: Vec<BinanceMarketConfig>,
    pub router_config: RouterConfig,
    pub sample_mask: SampleMask,
    pub status_query_delay_ms: u64,
    pub order_recv_window_ms: u32,
}

impl BinanceAccountSpec {
    pub fn new(account_domain: OrsAccountDomainConfig, markets: Vec<BinanceMarketConfig>) -> Self {
        Self {
            account_domain,
            markets,
            router_config: RouterConfig::default(),
            sample_mask: SampleMask::IN_BAND,
            status_query_delay_ms: DEFAULT_STATUS_QUERY_DELAY_MS,
            order_recv_window_ms: DEFAULT_ORDER_RECV_WINDOW_MS,
        }
    }
}

struct BinanceMarketRuntime {
    market: BinanceVenueMarket,
    api_key: String,
    req_signer: SigningKey,
    dialer_logon_signer: SigningKey,
    rest_query_signer: SigningKey,
    urls: BinanceResolvedMarketUrls,
    listen_key: GuardedValue<InlineAscii<96>>,
}

impl core::fmt::Debug for BinanceMarketRuntime {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BinanceMarketRuntime")
            .field("market", &self.market)
            .field("api_key", &self.api_key)
            .field("urls", &self.urls)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
struct BinanceOrderConnSlot {
    market: BinanceVenueMarket,
    locked: Arc<LockedConnection>,
}

/// Disconnected link summary reaped from [`BinanceConnectionSet::reap_close_pending`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReapedOrderLink {
    pub conn_id: u16,
    pub market: BinanceVenueMarket,
    pub inflight: Option<BinanceRequestRef>,
}

/// Multi-market order and private-stream connection pools (`UrlConnections`) paired with a
/// [`BinanceSession`]. Owned by the `RESP` thread while `REQ` dispatches frames lock-free through
/// `PublishedRoutes` + `Arc<LockedConnection>`.
#[derive(Debug)]
pub struct BinanceConnectionSet {
    account_ref: u8,
    order_pools: [Option<UrlConnections>; 4],
    stream_pools: [Option<UrlConnections>; 4],
    combined_routes: Arc<PublishedRoutes>,
}

impl BinanceConnectionSet {
    #[inline]
    pub const fn account_ref(&self) -> u8 {
        self.account_ref
    }

    #[inline]
    pub fn combined_routes(&self) -> &Arc<PublishedRoutes> {
        &self.combined_routes
    }

    #[inline]
    pub fn order_pool(&self, market: BinanceVenueMarket) -> Option<&UrlConnections> {
        self.order_pools[market.index()].as_ref()
    }

    #[inline]
    pub fn order_pool_mut(&mut self, market: BinanceVenueMarket) -> Option<&mut UrlConnections> {
        self.order_pools[market.index()].as_mut()
    }

    #[inline]
    pub fn stream_pool(&self, market: BinanceVenueMarket) -> Option<&UrlConnections> {
        self.stream_pools[market.index()].as_ref()
    }

    #[inline]
    pub fn stream_pool_mut(&mut self, market: BinanceVenueMarket) -> Option<&mut UrlConnections> {
        self.stream_pools[market.index()].as_mut()
    }

    /// Aggregates every configured market's order `UrlConnections` snapshot into the unified
    /// `PublishedRoutes` table read by `REQ`.
    pub fn republish_combined(&self) -> Result<u32, TransportError> {
        let mut combined = [v::OrderRoute::default(); MAX_PUBLISHED_ROUTES];
        let mut total = 0usize;
        for pool in self.order_pools.iter().flatten() {
            let _ = pool.republish()?;
            let mut sub = [v::OrderRoute::default(); MAX_PUBLISHED_ROUTES];
            let n = pool.published().snapshot(&mut sub);
            if total + n > MAX_PUBLISHED_ROUTES {
                return Err(TransportError::Bounds);
            }
            combined[total..total + n].copy_from_slice(&sub[..n]);
            total += n;
        }
        self.combined_routes.publish_routes(&combined[..total])
    }

    /// Installs a live order [`LinkOps`] into `(market, conn_id)` and republishes combined routes.
    pub fn install_order_link(
        &mut self,
        market: BinanceVenueMarket,
        conn_id: u16,
        link: LinkOps,
    ) -> Result<u64, BinanceSessionError> {
        let pool = self
            .order_pool_mut(market)
            .ok_or(BinanceSessionError::UnconfiguredMarket(market))?;
        let epoch = pool.install_link(conn_id, link)?;
        self.republish_combined()?;
        Ok(epoch)
    }

    /// Installs a live stream [`LinkOps`] into `(market, conn_id)`.
    pub fn install_stream_link(
        &mut self,
        market: BinanceVenueMarket,
        conn_id: u16,
        link: LinkOps,
    ) -> Result<u64, BinanceSessionError> {
        let pool = self
            .stream_pool_mut(market)
            .ok_or(BinanceSessionError::UnconfiguredMarket(market))?;
        Ok(pool.install_link(conn_id, link)?)
    }

    /// Records a passive RTT sample on `(market, conn_id, epoch)` and republishes combined routes.
    pub fn note_order_sample(
        &mut self,
        market: BinanceVenueMarket,
        conn_id: u16,
        epoch: u64,
        kind: SampleKind,
        sample_us: u32,
    ) -> Result<bool, BinanceSessionError> {
        let pool = self
            .order_pool_mut(market)
            .ok_or(BinanceSessionError::UnconfiguredMarket(market))?;
        let updated = pool.note_sample(conn_id, epoch, kind, sample_us)?;
        if updated {
            self.republish_combined()?;
        }
        Ok(updated)
    }

    /// Registers all order and stream pools with a background [`Dialer`].
    pub fn register_with_dialer(
        &mut self,
        dialer: &Dialer,
        base_slot_id: u64,
        order_templates: &[Option<DialSpec>; 4],
        stream_templates: &[Option<DialSpec>; 4],
    ) -> Result<u64, BinanceSessionError> {
        let mut cursor = base_slot_id;
        for (idx, pool_opt) in self.order_pools.iter_mut().enumerate() {
            if let (Some(pool), Some(tpl)) = (pool_opt.as_mut(), order_templates[idx].as_ref()) {
                let count = pool.len() as u64;
                pool.register_with_dialer(dialer, cursor, tpl)?;
                cursor += count;
            }
        }
        for (idx, pool_opt) in self.stream_pools.iter_mut().enumerate() {
            if let (Some(pool), Some(tpl)) = (pool_opt.as_mut(), stream_templates[idx].as_ref()) {
                let count = pool.len() as u64;
                pool.register_with_dialer(dialer, cursor, tpl)?;
                cursor += count;
            }
        }
        Ok(cursor)
    }

    /// Harvests `DialState::Ready` links across order and stream pools (`Dialer` already completed
    /// `session.logon` challenge off-hot-path and preserved any early frames in `DialedLink`).
    pub fn poll_dials(
        &mut self,
        now_mono_ns: u64,
        max_rx_bytes: usize,
        max_http_headers: usize,
    ) -> Result<usize, BinanceSessionError> {
        let mut total_installed = 0usize;
        for market in BinanceVenueMarket::ALL {
            let idx = market.index();
            if let Some(pool) = self.order_pools[idx].as_mut() {
                let n = pool.poll_dials(now_mono_ns, |_cid, dialed: DialedLink| match market {
                    BinanceVenueMarket::Options => Ok(LinkOps::Https(HttpsLink::new(
                        dialed.connection,
                        max_rx_bytes,
                        max_http_headers,
                    )?)),
                    BinanceVenueMarket::Spot
                    | BinanceVenueMarket::Usdm
                    | BinanceVenueMarket::Coinm => {
                        let mut ws_link = WsLink::with_login(
                            dialed.connection,
                            max_rx_bytes,
                            max_rx_bytes,
                            WsLoginState::Authenticated,
                        )?;
                        for early in dialed.early_ws_frames {
                            ws_link.push_early_frame(early)?;
                        }
                        Ok(LinkOps::Ws(ws_link))
                    }
                });
                total_installed += n;
            }
            if let Some(pool) = self.stream_pools[idx].as_mut() {
                let n = pool.poll_dials(now_mono_ns, |_cid, dialed: DialedLink| {
                    let mut ws_link = WsLink::with_login(
                        dialed.connection,
                        max_rx_bytes,
                        max_rx_bytes,
                        WsLoginState::Authenticated,
                    )?;
                    for early in dialed.early_ws_frames {
                        ws_link.push_early_frame(early)?;
                    }
                    Ok(LinkOps::Ws(ws_link))
                });
                total_installed += n;
            }
        }
        if total_installed > 0 {
            self.republish_combined()?;
        }
        Ok(total_installed)
    }

    /// Reaps any order/stream connections marked `close_pending`, capturing any in-flight
    /// `HttpsLink` request on Options so the caller can invoke [`BinanceSession::on_disconnected`].
    pub fn reap_close_pending(
        &mut self,
        now_mono_ns: u64,
        out_reaped: &mut Vec<ReapedOrderLink>,
    ) -> Result<usize, BinanceSessionError> {
        let mut changed = false;
        let mut count = 0usize;
        for market in BinanceVenueMarket::ALL {
            let idx = market.index();
            if let Some(pool) = self.order_pools[idx].as_mut() {
                let mut sub = [v::OrderRoute::default(); MAX_PUBLISHED_ROUTES];
                let snap_len = pool.published().snapshot(&mut sub);
                for route in sub.iter().take(snap_len) {
                    let cid = route.connection_id;
                    if let Some(handle) = pool.connection_handle(cid)
                        && handle.is_close_pending()
                    {
                        let mut inflight = None;
                        if market == BinanceVenueMarket::Options
                            && let Some(raw_ref) = handle.complete_https_reply()
                        {
                            inflight = BinanceRequestRef::from_transport_ref(raw_ref);
                        }
                        out_reaped.push(ReapedOrderLink {
                            conn_id: cid,
                            market,
                            inflight,
                        });
                    }
                }
                let r = pool.reap_close_pending(now_mono_ns);
                if r > 0 {
                    changed = true;
                    count += r;
                }
            }
            if let Some(pool) = self.stream_pools[idx].as_mut() {
                count += pool.reap_close_pending(now_mono_ns);
            }
        }
        if changed {
            self.republish_combined()?;
        }
        Ok(count)
    }
}

/// Unified `Send + Sync` full-market Binance ORS account session (`1 AccountSession = 1 UID`).
#[derive(Debug)]
pub struct BinanceSession<const CHECK_STP: bool = true> {
    account_ref: u8,
    account_domain: OrsAccountDomainConfig,
    order_manager: Arc<OrderManager<CHECK_STP>>,
    quota: SharedBinanceOrsQuota,
    catalog: Arc<BinanceBootstrapCatalog>,
    markets: [Option<BinanceMarketRuntime>; 4],
    order_connections: Box<[Option<BinanceOrderConnSlot>]>,
    published_routes: Arc<PublishedRoutes>,
    last_picked_conn: [AtomicU16; 4],
    req_seq: AtomicU64,
    status_query_delay_ns: u64,
    order_recv_window_ms: u32,
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<BinanceSession<true>>();
    assert_send_sync::<BinanceSession<false>>();
};

impl<const CHECK_STP: bool> BinanceSession<CHECK_STP> {
    /// Configures a single-market or multi-market UID `BinanceSession` and constructs its matching
    /// [`BinanceConnectionSet`] at cold startup (`8.4` `configure`).
    pub fn configure(
        spec: BinanceAccountSpec,
        order_manager: Arc<OrderManager<CHECK_STP>>,
        quota: SharedBinanceOrsQuota,
        catalog: Arc<BinanceBootstrapCatalog>,
        resp_waker: Option<Arc<Waker>>,
        tracer: Option<Arc<LatencyTracer>>,
    ) -> Result<(Arc<Self>, BinanceConnectionSet), BinanceSessionError> {
        let account_ref = spec.account_domain.account_ref;
        if account_ref == 0 {
            return Err(BinanceConfigError::InvalidAccountRef(account_ref).into());
        }
        if spec.markets.is_empty() {
            return Err(BinanceConfigError::EmptyMarkets.into());
        }

        let mut markets_rt: [Option<BinanceMarketRuntime>; 4] = [None, None, None, None];
        let mut order_pools: [Option<UrlConnections>; 4] = [None, None, None, None];
        let mut stream_pools: [Option<UrlConnections>; 4] = [None, None, None, None];
        let mut order_connections: Vec<Option<BinanceOrderConnSlot>> =
            vec![None; MAX_PUBLISHED_ROUTES];

        let mut next_order_conn_id: u16 = 0;
        let mut next_stream_conn_id: u16 = 1_000;

        for m_cfg in spec.markets {
            let idx = m_cfg.market.index();
            if markets_rt[idx].is_some() {
                return Err(BinanceConfigError::DuplicateMarket(m_cfg.market).into());
            }
            if m_cfg.order_binds.is_empty() {
                return Err(BinanceConfigError::EmptySocketBinds {
                    market: m_cfg.market,
                    role: UrlRole::ORDER,
                }
                .into());
            }

            let (req_signer, dialer_logon_signer, rest_query_signer) =
                m_cfg.auth.build_isolated_signers(m_cfg.market)?;
            let api_key = String::from(m_cfg.auth.api_key());

            let resolved_urls =
                resolve_market_urls(m_cfg.market, spec.account_domain.environment, &m_cfg.urls)?;
            let q_market = quota_market(m_cfg.market);
            quota.register_account(q_market, account_ref, m_cfg.rate_overrides)?;
            if !m_cfg.ip_budgets.is_empty() {
                quota.install_ip_budgets(q_market, &m_cfg.ip_budgets)?;
            }

            let order_link_kind = resolved_urls.order_url.link_kind();
            let sub_published = Arc::new(PublishedRoutes::new());
            let mut order_binds = m_cfg.order_binds;
            for b in &mut order_binds {
                b.colo = resolved_urls.order_url.is_colo();
            }
            let order_pool = UrlConnections::new(
                account_ref,
                next_order_conn_id,
                order_link_kind,
                m_cfg.market.product_mask(),
                &order_binds,
                spec.router_config,
                spec.sample_mask,
                resp_waker.clone(),
                tracer.clone(),
                sub_published,
            )?;

            let pool_len = order_pool.len();
            if usize::from(next_order_conn_id) + pool_len > MAX_PUBLISHED_ROUTES {
                return Err(BinanceSessionError::InvalidConfig(
                    "total order connections exceed MAX_PUBLISHED_ROUTES (64)",
                ));
            }
            for offset in 0..pool_len {
                let cid = next_order_conn_id + offset as u16;
                let locked = order_pool
                    .connection_handle(cid)
                    .ok_or(BinanceSessionError::InvalidConfig(
                        "missing LockedConnection handle in UrlConnections",
                    ))?
                    .clone();
                order_connections[usize::from(cid)] = Some(BinanceOrderConnSlot {
                    market: m_cfg.market,
                    locked,
                });
            }
            next_order_conn_id += pool_len as u16;
            order_pools[idx] = Some(order_pool);

            if m_cfg.market.needs_listen_key() && !m_cfg.stream_binds.is_empty() {
                let stream_is_colo = resolved_urls.stream_url.is_some_and(BinanceUrlId::is_colo);
                let mut stream_binds = m_cfg.stream_binds;
                for b in &mut stream_binds {
                    b.colo = stream_is_colo;
                }
                let stream_published = Arc::new(PublishedRoutes::new());
                let stream_pool = UrlConnections::new(
                    account_ref,
                    next_stream_conn_id,
                    LinkKind::Ws,
                    m_cfg.market.product_mask(),
                    &stream_binds,
                    spec.router_config,
                    spec.sample_mask,
                    resp_waker.clone(),
                    tracer.clone(),
                    stream_published,
                )?;
                next_stream_conn_id += stream_pool.len() as u16;
                stream_pools[idx] = Some(stream_pool);
            } else if !m_cfg.market.needs_listen_key() && !m_cfg.stream_binds.is_empty() {
                return Err(BinanceSessionError::InvalidConfig(
                    "Spot subscribes via userDataStream.subscribe on its order pool; stream_binds must be empty",
                ));
            }

            markets_rt[idx] = Some(BinanceMarketRuntime {
                market: m_cfg.market,
                api_key,
                req_signer,
                dialer_logon_signer,
                rest_query_signer,
                urls: resolved_urls,
                listen_key: GuardedValue::new(
                    InlineAscii::empty(),
                    (u32::from(account_ref) << 4) | (idx as u32),
                    tracer.clone(),
                ),
            });
        }

        let combined_routes = Arc::new(PublishedRoutes::new());
        let conn_set = BinanceConnectionSet {
            account_ref,
            order_pools,
            stream_pools,
            combined_routes: combined_routes.clone(),
        };
        conn_set.republish_combined()?;

        let session = Arc::new(Self {
            account_ref,
            account_domain: spec.account_domain,
            order_manager,
            quota,
            catalog,
            markets: markets_rt,
            order_connections: order_connections.into_boxed_slice(),
            published_routes: combined_routes,
            last_picked_conn: [
                AtomicU16::new(v::OrderRoute::NO_CONNECTION),
                AtomicU16::new(v::OrderRoute::NO_CONNECTION),
                AtomicU16::new(v::OrderRoute::NO_CONNECTION),
                AtomicU16::new(v::OrderRoute::NO_CONNECTION),
            ],
            req_seq: AtomicU64::new(1),
            status_query_delay_ns: spec.status_query_delay_ms.saturating_mul(NS_PER_MS),
            order_recv_window_ms: spec.order_recv_window_ms.clamp(1, 60_000),
        });

        Ok((session, conn_set))
    }

    #[inline]
    pub const fn account_ref(&self) -> u8 {
        self.account_ref
    }

    #[inline]
    pub const fn account_domain(&self) -> &OrsAccountDomainConfig {
        &self.account_domain
    }

    #[inline]
    pub fn order_manager(&self) -> &Arc<OrderManager<CHECK_STP>> {
        &self.order_manager
    }

    #[inline]
    pub fn quota(&self) -> &SharedBinanceOrsQuota {
        &self.quota
    }

    #[inline]
    pub fn catalog(&self) -> &Arc<BinanceBootstrapCatalog> {
        &self.catalog
    }

    #[inline]
    pub fn published_routes(&self) -> &Arc<PublishedRoutes> {
        &self.published_routes
    }

    #[inline]
    pub const fn status_query_delay_ms(&self) -> u64 {
        self.status_query_delay_ns / NS_PER_MS
    }

    #[inline]
    pub fn resolved_urls(&self, market: BinanceVenueMarket) -> Option<&BinanceResolvedMarketUrls> {
        self.markets[market.index()].as_ref().map(|m| &m.urls)
    }

    #[inline]
    fn market_rt(
        &self,
        market: BinanceVenueMarket,
    ) -> Result<&BinanceMarketRuntime, BinanceSessionError> {
        self.markets[market.index()]
            .as_ref()
            .ok_or(BinanceSessionError::UnconfiguredMarket(market))
    }

    // =========================================================================
    // Dialer & Stream-Token Hooks (`8.4`, `8.5`, `13.9`)
    // =========================================================================

    /// Spot subscribes via `userDataStream.subscribe` (`false`); USD-M, Coin-M, and Options require
    /// a `listenKey` (`true`).
    #[inline]
    pub const fn needs_stream_token(&self, market: BinanceVenueMarket) -> bool {
        market.needs_listen_key()
    }

    /// Returns `true` once `GuardedValue` holds a non-empty `listenKey` (always `true` on Spot).
    #[inline]
    pub fn has_stream_token(&self, market: BinanceVenueMarket) -> bool {
        if !market.needs_listen_key() {
            return true;
        }
        self.markets[market.index()]
            .as_ref()
            .is_some_and(|rt| !rt.listen_key.get(ThreadTag::DIALER).0.is_empty())
    }

    /// Returns `1,800` seconds (`30 min`) on `listenKey` markets and `0` on Spot.
    #[inline]
    pub const fn stream_token_refresh_s(&self, market: BinanceVenueMarket) -> u32 {
        market.default_stream_token_refresh_s()
    }

    /// Returns `15s` for WebSocket order/stream pools and `0` for Options HTTP/1.1 keep-alive links.
    #[inline]
    pub const fn ping_period_s(&self, market: BinanceVenueMarket, role: UrlRole) -> u32 {
        if matches!(market, BinanceVenueMarket::Options) && role.has_order() && !role.has_stream() {
            0
        } else {
            DEFAULT_WS_PING_PERIOD_S
        }
    }

    /// Formats the dial target URL into `out` (`Dialer` thread):
    /// - `UrlRole::ORDER`: static `order_url` (`wss://...` or `https://eapi.binance.com`).
    /// - `UrlRole::STREAM`: on USD-M/Coin-M/Options, reads `listenKey` from `GuardedValue` and
    ///   appends `/ws/<key>` on colo (`fstream-mm`/`dstream-mm`) or `?listenKey=<key>` on public!
    /// - `UrlRole::NONE`: REST query base URL.
    pub fn connection_url(
        &self,
        market: BinanceVenueMarket,
        role: UrlRole,
        out: &mut [u8],
    ) -> Result<usize, BinanceSessionError> {
        let rt = self.market_rt(market)?;
        if role.has_stream() && market.needs_listen_key() {
            let stream_id = rt
                .urls
                .stream_url
                .ok_or(BinanceSessionError::InvalidConfig("missing stream_url"))?;
            let (key, _) = rt.listen_key.get(ThreadTag::DIALER);
            if key.is_empty() {
                return Err(BinanceSessionError::MissingListenKey(market));
            }
            return decorate_listen_key_stream_url(
                stream_id.base_url(),
                stream_id.is_colo(),
                key.as_str(),
                out,
            )
            .map_err(|_| {
                BinanceSessionError::EncodeFailed("decorate_listen_key_stream_url failed")
            });
        }

        let raw = if role.has_order() || role.has_stream() {
            rt.urls.order_url.base_url().as_bytes()
        } else {
            rt.urls.query_url.base_url().as_bytes()
        };
        if raw.len() > out.len() {
            return Err(BinanceSessionError::EncodeFailed(
                "connection_url buffer too small",
            ));
        }
        out[..raw.len()].copy_from_slice(raw);
        Ok(raw.len())
    }

    /// Encodes the Ed25519 `session.logon` frame on `Dialer` using its dedicated
    /// `dialer_logon_signer` (`8.4`, `8.5`).
    pub fn encode_login(
        &self,
        market: BinanceVenueMarket,
        role: UrlRole,
        req_id: &str,
        wall_ms: u64,
        out: &mut [u8],
    ) -> Result<Option<usize>, BinanceSessionError> {
        if !role.has_order() || !market.uses_ws_order_pool() {
            return Ok(None);
        }
        let rt = self.market_rt(market)?;
        let len = encode_ws_session_logon(
            req_id,
            &rt.api_key,
            wall_ms,
            Some(u64::from(self.order_recv_window_ms)),
            &rt.dialer_logon_signer,
            market,
            out,
        )
        .map_err(|_| {
            BinanceSessionError::EncodeFailed(
                "encode_ws_session_logon failed (requires Ed25519 PEM signer)",
            )
        })?;
        Ok(Some(len))
    }

    #[inline]
    pub const fn awaits_login_reply(&self, market: BinanceVenueMarket, role: UrlRole) -> bool {
        role.has_order() && market.uses_ws_order_pool()
    }

    /// Inspects a WebSocket payload for `session.logon` completion (`authorizedSince` + `200`).
    pub fn login_reply(&self, payload: &[u8]) -> BinanceLoginReply {
        let Ok(parsed) = BinanceMsgParser::parse(payload) else {
            return BinanceLoginReply::Failed;
        };
        if parsed.has_error() {
            return BinanceLoginReply::Failed;
        }
        if parsed.authorized_since.is_some() && parsed.http_status == Some(200) {
            return BinanceLoginReply::Ok;
        }
        BinanceLoginReply::Pending
    }

    /// Encodes Spot's `userDataStream.subscribe` frame after `session.logon` (`8.5`).
    pub fn encode_subscribe(
        &self,
        market: BinanceVenueMarket,
        role: UrlRole,
        req_id: &str,
        out: &mut [u8],
    ) -> Result<Option<usize>, BinanceSessionError> {
        if market == BinanceVenueMarket::Spot && role.has_order() {
            let len = encode_ws_user_stream_subscribe(req_id, out).map_err(|_| {
                BinanceSessionError::EncodeFailed("encode_ws_user_stream_subscribe buffer small")
            })?;
            return Ok(Some(len));
        }
        Ok(None)
    }

    /// Encodes a masked client WebSocket RFC 6455 `Ping` frame (`opcode 0x9`).
    #[inline]
    pub fn encode_ping(&self, payload: &[u8]) -> Result<Vec<u8>, BinanceSessionError> {
        Ok(ws::encode_client(
            ws::Opcode::Ping,
            payload,
            WS_CONTROL_MAX_BYTES,
        )?)
    }

    /// Returns `true` when `msg` is a WebSocket `Pong` control frame.
    #[inline]
    pub const fn is_pong(&self, msg: &ws::Message) -> bool {
        matches!(msg.opcode, ws::Opcode::Pong)
    }

    /// Builds a masked client WebSocket `Pong` echoing the venue's `Ping` payload (`8.4`).
    #[inline]
    pub fn reply_to_ping(&self, ping_payload: &[u8]) -> Result<Vec<u8>, BinanceSessionError> {
        Ok(ws::encode_client(
            ws::Opcode::Pong,
            ping_payload,
            WS_CONTROL_MAX_BYTES,
        )?)
    }

    /// Builds the `POST` (`keepalive == false`) or `PUT` (`keepalive == true`) `listenKey` REST
    /// call for USD-M, Coin-M, or Options (`13.9`).
    pub fn stream_token_query(
        &self,
        market: BinanceVenueMarket,
        keepalive: bool,
    ) -> Result<Option<BinanceRestCall>, BinanceSessionError> {
        let Some(path) = market.listen_key_path() else {
            return Ok(None);
        };
        let rt = self.market_rt(market)?;
        let method = if keepalive { "PUT" } else { "POST" };
        Ok(Some(BinanceRestCall {
            account_ref: self.account_ref,
            market,
            method,
            host: String::from(rt.urls.query_url.host()),
            port: rt.urls.query_url.port(),
            target: String::from(path),
            api_key: rt.api_key.clone(),
            ors_order_id: 0,
            kind: BinanceRequestKind::Other,
        }))
    }

    /// Processes a `POST`/`PUT /{fapi,dapi,eapi}/v1/listenKey` response, updating `GuardedValue`
    /// (`Ok(true)`) or clearing it when expired/invalid (`Ok(false)`).
    pub fn on_stream_token_reply(
        &self,
        market: BinanceVenueMarket,
        http_status: u16,
        body: &[u8],
    ) -> Result<bool, BinanceSessionError> {
        let rt = self.market_rt(market)?;
        if http_status >= 400 {
            rt.listen_key
                .set(ThreadTag::REST_QUERY, InlineAscii::empty());
            return Ok(false);
        }
        let trimmed = body.trim_ascii();
        if trimmed.is_empty() || trimmed == b"{}" {
            // Keepalive `PUT` on USD-M/Coin-M/Options returns `{}` on HTTP 200 when valid!
            let has_existing = !rt.listen_key.get(ThreadTag::REST_QUERY).0.is_empty();
            return Ok(has_existing);
        }
        let parsed = BinanceMsgParser::parse(body).map_err(|_| BinanceSessionError::ParseFailed)?;
        if parsed.has_error() {
            rt.listen_key
                .set(ThreadTag::REST_QUERY, InlineAscii::empty());
            return Ok(false);
        }
        if let Some(lk) = parsed.listen_key
            && !lk.is_empty()
        {
            let ascii = InlineAscii::<96>::try_from_str(lk).ok_or(
                BinanceSessionError::InvalidConfig("listenKey exceeds 96 ASCII bytes"),
            )?;
            rt.listen_key.set(ThreadTag::REST_QUERY, ascii);
            return Ok(true);
        }
        Ok(!rt.listen_key.get(ThreadTag::REST_QUERY).0.is_empty())
    }

    /// Explicitly seeds or rotates a `listenKey` (used in tests or custom bootstrap pipelines).
    pub fn set_listen_key(
        &self,
        market: BinanceVenueMarket,
        key: &str,
    ) -> Result<u64, BinanceSessionError> {
        let rt = self.market_rt(market)?;
        let ascii = InlineAscii::<96>::try_from_str(key).ok_or(
            BinanceSessionError::InvalidConfig("listenKey must be <= 96 ASCII bytes"),
        )?;
        Ok(rt.listen_key.set(ThreadTag::REST_QUERY, ascii))
    }

    // =========================================================================
    // REQ Hot-Path Order Dispatch (`submit_new`, `submit_cancel`, `submit_modify`)
    // =========================================================================

    fn pick_route_from_slice_or_published(
        &self,
        market: BinanceVenueMarket,
        routes_override: Option<&[v::OrderRoute]>,
    ) -> Option<v::OrderRoute> {
        let m_idx = market.index();
        let raw_last = self.last_picked_conn[m_idx].load(Ordering::Relaxed);
        let last_conn = (raw_last != v::OrderRoute::NO_CONNECTION).then_some(raw_last);
        let product = Some(market.primary_product());

        match routes_override {
            None => {
                let mut snap = [v::OrderRoute::default(); MAX_PUBLISHED_ROUTES];
                let count = self.published_routes.snapshot(&mut snap);
                for route in &mut snap[..count] {
                    if let Some(Some(slot)) =
                        self.order_connections.get(usize::from(route.connection_id))
                    {
                        route.ready = slot.locked.is_ready_relaxed();
                    }
                }
                pick_from_explicit_routes(self.account_ref, product, last_conn, &snap[..count])
            }
            Some(routes) => pick_from_explicit_routes(self.account_ref, product, last_conn, routes),
        }
    }

    /// Submits a `NewOrder` intent on `REQ` (`8.4` `submitNew`, `8.5`):
    /// 1. Admits order, checks lock-free STP BBO, reserves UTA wallet hold, allocates slot, and
    ///    releases the `Order` spinlock inside `OrderManager::place_order`.
    /// 2. Encodes wire frame into `frame_buf` (`WsLink` JSON-RPC for Spot/UM/CM or `HttpsLink`
    ///    HMAC-signed HTTP `POST /eapi/v1/order` for Options).
    /// 3. If `dry_run`, returns `DryRunEncoded` immediately without route pick, rate spend, or I/O.
    /// 4. Picks route and spends `SharedBinanceOrsQuota` strictly AFTER `pick` on `route.local_ip`
    ///    (single-shot NACK on limit, never retrying another connection).
    /// 5. Transmits via `LockedConnection::send_frame` (never overlapping `Order` & `Connection` locks).
    pub fn submit_new(
        &self,
        req: &BinanceNewOrderIntent,
        routes_override: Option<&[v::OrderRoute]>,
        frame_buf: &mut [u8],
        sink: &mut impl BinanceEventSink,
    ) -> Result<BinanceSubmitOutcome, OrsOrderRefusal> {
        let Some(binding) = self.catalog.symbol(req.symbol_id) else {
            return Err(OrsOrderRefusal {
                strat_id: req.strat_id,
                account_ref: self.account_ref,
                order_seqnum: req.order_seqnum,
                reason: v::RefusalReason::InvalidQuantity,
            });
        };
        let market = binding.market;
        let Some(rt) = self.markets[market.index()].as_ref() else {
            return Err(OrsOrderRefusal {
                strat_id: req.strat_id,
                account_ref: self.account_ref,
                order_seqnum: req.order_seqnum,
                reason: v::RefusalReason::MKT_REJECT_MARKET_DOWN,
            });
        };

        // 1. OrderManager admission (claims slot, reserves hold, inserts STP, drops Order lock).
        let placed = self.order_manager.place_order(&req.to_ors_request())?;
        let ors_order_id = placed.ors_order_id;

        // 2. Encode outbound wire frame into caller stack/scratch buffer (`dry_run` also runs this!).
        let price_atoms_opt = match req.order_type {
            v::OrderType::Limit => scaled_atoms_i64(req.price_ticks, binding.spec.price_tick_atoms),
            v::OrderType::Market => Some(0),
            _ => None,
        };
        let qty_atoms_opt = scaled_atoms_i64(req.qty_lots, binding.spec.qty_step_atoms);

        let encode_opt: Option<usize> = match (price_atoms_opt, qty_atoms_opt) {
            (Some(px_atoms), Some(qty_atoms)) => match market {
                BinanceVenueMarket::Spot | BinanceVenueMarket::Usdm | BinanceVenueMarket::Coinm => {
                    let mut id_buf = [0u8; 24];
                    let req_id =
                        encode_ws_request_id(ors_order_id, BinanceRequestKind::Place, &mut id_buf);
                    encode_ws_order_place(
                        &WsOrderNewParams {
                            market,
                            request_id: req_id,
                            symbol: &binding.venue_symbol,
                            client_order_id: placed.client_id.as_str(),
                            side: req.side,
                            order_type: req.order_type,
                            tif: req.tif,
                            post_only: req.post_only,
                            reduce_only: req.reduce_only,
                            position_side: req.position_side,
                            stp: req.stp_mode,
                            price_ticks: px_atoms,
                            price_scale: binding.spec.price_scale,
                            qty_lots: qty_atoms,
                            qty_scale: binding.spec.qty_scale,
                            timestamp_ms: req.wall_ms,
                            recv_window_ms: Some(u64::from(self.order_recv_window_ms)),
                            per_frame_auth: None,
                        },
                        frame_buf,
                    )
                    .ok()
                    .and_then(|json_len| {
                        wrap_ws_client_text_frame_in_place(
                            frame_buf,
                            json_len,
                            ors_order_id ^ req.now_mono_ns ^ req.wall_ms,
                        )
                    })
                }
                BinanceVenueMarket::Options => {
                    if req.order_type != v::OrderType::Limit || px_atoms <= 0 {
                        None
                    } else {
                        encode_options_http_place(
                            &OptionsHttpNewParams {
                                host: rt.urls.order_url.host(),
                                api_key: &rt.api_key,
                                hmac_signer: &rt.req_signer,
                                symbol: &binding.venue_symbol,
                                client_order_id: placed.client_id.as_str(),
                                side: req.side,
                                tif: req.tif,
                                post_only: req.post_only,
                                reduce_only: req.reduce_only,
                                price_ticks: px_atoms,
                                price_scale: binding.spec.price_scale,
                                qty_lots: qty_atoms,
                                qty_scale: binding.spec.qty_scale,
                                timestamp_ms: req.wall_ms,
                                recv_window_ms: Some(u64::from(self.order_recv_window_ms)),
                            },
                            frame_buf,
                        )
                        .ok()
                    }
                }
            },
            _ => None,
        };

        let Some(encoded_len) = encode_opt else {
            if placed.should_send_to_venue
                && let Some(rep) = self.order_manager.on_rejected(
                    ors_order_id,
                    None,
                    "frame_encode_error",
                    Some(v::TerminalReason::InvalidPrice),
                    req.now_mono_ns,
                )
            {
                sink.on_report(rep);
            }
            return Ok(BinanceSubmitOutcome::LocalNack {
                ors_order_id,
                reason: v::TerminalReason::InvalidPrice,
            });
        };

        // 3. `dry_run`: scratch slot was already validated and retired by `place_order`; skip I/O!
        if !placed.should_send_to_venue {
            return Ok(BinanceSubmitOutcome::DryRunEncoded {
                ors_order_id,
                client_id: placed.client_id,
                encoded_len,
            });
        }

        // 4. Single-pick route selection + post-pick `local_ip` rate-limit spend (NACK on hit!).
        let pick_res = self.quota.pick_route_and_spend_command_or_nack(
            quota_market(market),
            self.account_ref,
            command_transport(market),
            CommandAction::New,
            || self.pick_route_from_slice_or_published(market, routes_override),
            req.now_mono_ns,
        );

        let (route, _permit) = match pick_res {
            Ok(pair) => pair,
            Err(adm_err) => {
                let reason = match adm_err {
                    RouteAdmissionReject::NoReadyConnection
                    | RouteAdmissionReject::MissingLocalIp { .. } => {
                        v::TerminalReason::MKT_REJECT_MARKET_DOWN
                    }
                    RouteAdmissionReject::RateLimitedNack(_) => v::TerminalReason::RateLimited,
                };
                if let Some(rep) = self.order_manager.on_rejected(
                    ors_order_id,
                    None,
                    "local_route_or_rate_reject",
                    Some(reason),
                    req.now_mono_ns,
                ) {
                    sink.on_report(rep);
                }
                return Ok(BinanceSubmitOutcome::LocalNack {
                    ors_order_id,
                    reason,
                });
            }
        };

        // 5. Transmit on chosen `LockedConnection` (strictly outside any Order spinlock!).
        let conn_id = route.connection_id;
        let Some(conn_slot) = self
            .order_connections
            .get(usize::from(conn_id))
            .and_then(|s| s.as_ref())
        else {
            let reason = v::TerminalReason::MKT_REJECT_MARKET_DOWN;
            if let Some(rep) = self.order_manager.on_rejected(
                ors_order_id,
                None,
                "missing_order_connection",
                Some(reason),
                req.now_mono_ns,
            ) {
                sink.on_report(rep);
            }
            return Ok(BinanceSubmitOutcome::LocalNack {
                ors_order_id,
                reason,
            });
        };

        let seq = self.req_seq.fetch_add(1, Ordering::Relaxed);
        let req_ref = BinanceRequestRef::new(ors_order_id, BinanceRequestKind::Place)
            .to_transport_ref(seq, 0, req.now_mono_ns);
        let reply_id = pack_ws_reply_id(ors_order_id, BinanceRequestKind::Place);

        match conn_slot
            .locked
            .send_frame(&frame_buf[..encoded_len], reply_id, req_ref)
        {
            SendOutcome::Sent => {
                self.last_picked_conn[market.index()].store(conn_id, Ordering::Relaxed);
                Ok(BinanceSubmitOutcome::Sent {
                    ors_order_id,
                    conn_id,
                    client_id: placed.client_id,
                    encoded_len,
                })
            }
            SendOutcome::ShortWriteRetired => {
                self.schedule_delayed_place_query(
                    ors_order_id,
                    "short_write_retired",
                    req.now_mono_ns,
                );
                Ok(BinanceSubmitOutcome::AmbiguousSendScheduledQuery {
                    ors_order_id,
                    conn_id,
                })
            }
            SendOutcome::Busy | SendOutcome::Disconnected | SendOutcome::Bounds => {
                let reason = v::TerminalReason::MKT_REJECT_MARKET_DOWN;
                if let Some(rep) = self.order_manager.on_rejected(
                    ors_order_id,
                    None,
                    "link_not_ready",
                    Some(reason),
                    req.now_mono_ns,
                ) {
                    sink.on_report(rep);
                }
                Ok(BinanceSubmitOutcome::LocalNack {
                    ors_order_id,
                    reason,
                })
            }
        }
    }

    /// Submits a `CancelOrder` intent on `REQ` (`8.4` `submitCancel`, `8.5`).
    pub fn submit_cancel(
        &self,
        req: &OrsCancelRequest,
        routes_override: Option<&[v::OrderRoute]>,
        wall_ms: u64,
        frame_buf: &mut [u8],
        sink: &mut impl BinanceEventSink,
    ) -> Result<BinanceCancelOutcome, OrsOrderRefusal> {
        let dispatch = self.order_manager.request_cancel(req)?;
        if !dispatch.should_send_now {
            return Ok(BinanceCancelOutcome::QueuedBeforeAckOrQuery {
                ors_order_id: dispatch.ors_order_id,
                coalesced_in_query_desk: dispatch.coalesced_in_query_desk,
            });
        }
        Ok(self.dispatch_cancel_wire(
            dispatch.ors_order_id,
            dispatch.symbol_id,
            dispatch.client_id.as_str(),
            dispatch.venue_order_id.as_str(),
            routes_override,
            wall_ms,
            req.now_mono_ns,
            frame_buf,
            sink,
        ))
    }

    /// Re-transmits an already-registered cancel (`pending_cxl > 0`) without touching `DopState`'s
    /// strategy `msg_seqnum` or double-incrementing `pending_cxl`.
    pub fn retry_existing_cancel(
        &self,
        ors_order_id: u64,
        routes_override: Option<&[v::OrderRoute]>,
        wall_ms: u64,
        now_mono_ns: u64,
        frame_buf: &mut [u8],
        sink: &mut impl BinanceEventSink,
    ) -> Option<BinanceCancelOutcome> {
        let locked = self.order_manager.lock_order(ors_order_id)?;
        let o = *locked.order();
        drop(locked);
        Some(self.dispatch_cancel_wire(
            o.ors_order_id,
            o.symbol_id,
            o.client_id.as_str(),
            o.venue_order_id.as_str(),
            routes_override,
            wall_ms,
            now_mono_ns,
            frame_buf,
            sink,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_cancel_wire(
        &self,
        ors_order_id: u64,
        symbol_id: u32,
        client_id: &str,
        venue_order_id_str: &str,
        routes_override: Option<&[v::OrderRoute]>,
        wall_ms: u64,
        now_mono_ns: u64,
        frame_buf: &mut [u8],
        sink: &mut impl BinanceEventSink,
    ) -> BinanceCancelOutcome {
        let Some(binding) = self.catalog.symbol(symbol_id) else {
            if let Some(rep) = self.order_manager.on_cancel_rejected(
                ors_order_id,
                None,
                v::TerminalReason::InvalidQuantity,
                now_mono_ns,
            ) {
                sink.on_report(rep);
            }
            return BinanceCancelOutcome::LocalCancelNack {
                ors_order_id,
                reason: v::TerminalReason::InvalidQuantity,
            };
        };
        let market = binding.market;
        let Some(rt) = self.markets[market.index()].as_ref() else {
            let reason = v::TerminalReason::MKT_REJECT_MARKET_DOWN;
            if let Some(rep) =
                self.order_manager
                    .on_cancel_rejected(ors_order_id, None, reason, now_mono_ns)
            {
                sink.on_report(rep);
            }
            return BinanceCancelOutcome::LocalCancelNack {
                ors_order_id,
                reason,
            };
        };

        let venue_oid = parse_u64_ascii(venue_order_id_str.as_bytes()).filter(|&v| v > 0);
        let orig_cid = (!client_id.is_empty()).then_some(client_id);

        let encode_opt: Option<usize> = match market {
            BinanceVenueMarket::Spot | BinanceVenueMarket::Usdm | BinanceVenueMarket::Coinm => {
                let mut id_buf = [0u8; 24];
                let req_id =
                    encode_ws_request_id(ors_order_id, BinanceRequestKind::Cancel, &mut id_buf);
                encode_ws_order_cancel(
                    &WsOrderCancelParams {
                        market,
                        request_id: req_id,
                        symbol: &binding.venue_symbol,
                        orig_client_order_id: orig_cid,
                        venue_order_id: venue_oid,
                        timestamp_ms: wall_ms,
                        recv_window_ms: Some(u64::from(self.order_recv_window_ms)),
                        per_frame_auth: None,
                    },
                    frame_buf,
                )
                .ok()
                .and_then(|json_len| {
                    wrap_ws_client_text_frame_in_place(
                        frame_buf,
                        json_len,
                        ors_order_id ^ now_mono_ns ^ wall_ms ^ 0xCA4C_E100,
                    )
                })
            }
            BinanceVenueMarket::Options => encode_options_http_cancel(
                &OptionsHttpCancelParams {
                    host: rt.urls.order_url.host(),
                    api_key: &rt.api_key,
                    hmac_signer: &rt.req_signer,
                    symbol: &binding.venue_symbol,
                    client_order_id: orig_cid,
                    venue_order_id: venue_oid,
                    timestamp_ms: wall_ms,
                    recv_window_ms: Some(u64::from(self.order_recv_window_ms)),
                },
                frame_buf,
            )
            .ok(),
        };

        let Some(encoded_len) = encode_opt else {
            let reason = v::TerminalReason::InvalidQuantity;
            if let Some(rep) =
                self.order_manager
                    .on_cancel_rejected(ors_order_id, None, reason, now_mono_ns)
            {
                sink.on_report(rep);
            }
            return BinanceCancelOutcome::LocalCancelNack {
                ors_order_id,
                reason,
            };
        };

        let pick_res = self.quota.pick_route_and_spend_command_or_nack(
            quota_market(market),
            self.account_ref,
            command_transport(market),
            CommandAction::Cancel,
            || self.pick_route_from_slice_or_published(market, routes_override),
            now_mono_ns,
        );

        let (route, _permit) = match pick_res {
            Ok(pair) => pair,
            Err(adm_err) => {
                let reason = match adm_err {
                    RouteAdmissionReject::NoReadyConnection
                    | RouteAdmissionReject::MissingLocalIp { .. } => {
                        v::TerminalReason::MKT_REJECT_MARKET_DOWN
                    }
                    RouteAdmissionReject::RateLimitedNack(_) => v::TerminalReason::RateLimited,
                };
                if let Some(rep) =
                    self.order_manager
                        .on_cancel_rejected(ors_order_id, None, reason, now_mono_ns)
                {
                    sink.on_report(rep);
                }
                return BinanceCancelOutcome::LocalCancelNack {
                    ors_order_id,
                    reason,
                };
            }
        };

        let conn_id = route.connection_id;
        let Some(conn_slot) = self
            .order_connections
            .get(usize::from(conn_id))
            .and_then(|s| s.as_ref())
        else {
            let reason = v::TerminalReason::MKT_REJECT_MARKET_DOWN;
            if let Some(rep) =
                self.order_manager
                    .on_cancel_rejected(ors_order_id, None, reason, now_mono_ns)
            {
                sink.on_report(rep);
            }
            return BinanceCancelOutcome::LocalCancelNack {
                ors_order_id,
                reason,
            };
        };

        let seq = self.req_seq.fetch_add(1, Ordering::Relaxed);
        let req_ref = BinanceRequestRef::new(ors_order_id, BinanceRequestKind::Cancel)
            .to_transport_ref(seq, 0, now_mono_ns);
        let reply_id = pack_ws_reply_id(ors_order_id, BinanceRequestKind::Cancel);

        match conn_slot
            .locked
            .send_frame(&frame_buf[..encoded_len], reply_id, req_ref)
        {
            SendOutcome::Sent => {
                self.last_picked_conn[market.index()].store(conn_id, Ordering::Relaxed);
                BinanceCancelOutcome::Sent {
                    ors_order_id,
                    conn_id,
                    encoded_len,
                }
            }
            SendOutcome::ShortWriteRetired => {
                self.schedule_delayed_cancel_query(ors_order_id, now_mono_ns, sink);
                BinanceCancelOutcome::AmbiguousSendScheduledQuery {
                    ors_order_id,
                    conn_id,
                }
            }
            SendOutcome::Busy | SendOutcome::Disconnected | SendOutcome::Bounds => {
                let reason = v::TerminalReason::MKT_REJECT_MARKET_DOWN;
                if let Some(rep) =
                    self.order_manager
                        .on_cancel_rejected(ors_order_id, None, reason, now_mono_ns)
                {
                    sink.on_report(rep);
                }
                BinanceCancelOutcome::LocalCancelNack {
                    ors_order_id,
                    reason,
                }
            }
        }
    }

    /// Submits a native in-place `order.modify` (`PUT /fapi/v1/order`, `PUT /dapi/v1/order`) on
    /// USD-M / Coin-M or `order.cancelReplace` (`STOP_ON_FAILURE`) on Spot (`human-docs/3.8`, `8.5`).
    pub fn submit_modify(
        &self,
        req: &OrsModifyRequest,
        routes_override: Option<&[v::OrderRoute]>,
        wall_ms: u64,
        frame_buf: &mut [u8],
        sink: &mut impl BinanceEventSink,
    ) -> Result<BinanceModifyOutcome, OrsOrderRefusal> {
        if let Some(locked) = self.order_manager.lock_order(req.ors_order_id) {
            let sym_id = locked.order().symbol_id;
            let order_seq = locked.order().order_seqnum;
            drop(locked);
            if let Some(binding) = self.catalog.symbol(sym_id)
                && !binding.market.uses_ws_order_pool()
            {
                return Err(OrsOrderRefusal {
                    strat_id: req.strat_id,
                    account_ref: self.account_ref,
                    order_seqnum: order_seq,
                    reason: v::RefusalReason::MKT_REJECT,
                });
            }
        }

        let dispatch = self.order_manager.request_modify(req)?;
        let ors_order_id = dispatch.ors_order_id;

        let Some(binding) = self.catalog.symbol(dispatch.symbol_id) else {
            if let Some(rep) = self.order_manager.on_modify_rejected(
                ors_order_id,
                v::TerminalReason::InvalidQuantity,
                req.now_mono_ns,
            ) {
                sink.on_report(rep);
            }
            return Ok(BinanceModifyOutcome::LocalModifyNack {
                ors_order_id,
                reason: v::TerminalReason::InvalidQuantity,
            });
        };
        let market = binding.market;

        let Some((side, cid, vid_str)) = self.order_manager.lock_order(ors_order_id).map(|l| {
            (
                l.order().side,
                l.order().client_id,
                l.order().venue_order_id,
            )
        }) else {
            return Ok(BinanceModifyOutcome::LocalModifyNack {
                ors_order_id,
                reason: v::TerminalReason::OrderNotOpen,
            });
        };

        let venue_oid = parse_u64_ascii(vid_str.as_bytes()).filter(|&v| v > 0);
        let orig_cid = (!cid.is_empty()).then_some(cid.as_str());
        let px_atoms = scaled_atoms_i64(dispatch.new_price_ticks, binding.spec.price_tick_atoms);
        let qty_atoms = scaled_atoms_i64(dispatch.new_qty_lots, binding.spec.qty_step_atoms);

        let mut id_buf = [0u8; 24];
        let req_id = encode_ws_request_id(ors_order_id, BinanceRequestKind::Modify, &mut id_buf);
        let encode_opt = match (px_atoms, qty_atoms) {
            (Some(px), Some(qty)) => encode_ws_order_modify(
                &WsOrderModifyParams {
                    market,
                    request_id: req_id,
                    symbol: &binding.venue_symbol,
                    orig_client_order_id: orig_cid,
                    venue_order_id: venue_oid,
                    new_client_order_id: orig_cid,
                    side,
                    new_price_ticks: px,
                    price_scale: binding.spec.price_scale,
                    new_qty_lots: qty,
                    qty_scale: binding.spec.qty_scale,
                    timestamp_ms: wall_ms,
                    recv_window_ms: Some(u64::from(self.order_recv_window_ms)),
                    per_frame_auth: None,
                },
                frame_buf,
            )
            .ok()
            .and_then(|json_len| {
                wrap_ws_client_text_frame_in_place(
                    frame_buf,
                    json_len,
                    ors_order_id ^ req.now_mono_ns ^ wall_ms ^ 0xA3E4_D001,
                )
            }),
            _ => None,
        };

        let Some(encoded_len) = encode_opt else {
            let reason = v::TerminalReason::InvalidPrice;
            if let Some(rep) =
                self.order_manager
                    .on_modify_rejected(ors_order_id, reason, req.now_mono_ns)
            {
                sink.on_report(rep);
            }
            return Ok(BinanceModifyOutcome::LocalModifyNack {
                ors_order_id,
                reason,
            });
        };

        let pick_res = self.quota.pick_route_and_spend_command_or_nack(
            quota_market(market),
            self.account_ref,
            command_transport(market),
            CommandAction::Modify,
            || self.pick_route_from_slice_or_published(market, routes_override),
            req.now_mono_ns,
        );

        let (route, _permit) = match pick_res {
            Ok(pair) => pair,
            Err(adm_err) => {
                let reason = match adm_err {
                    RouteAdmissionReject::NoReadyConnection
                    | RouteAdmissionReject::MissingLocalIp { .. } => {
                        v::TerminalReason::MKT_REJECT_MARKET_DOWN
                    }
                    RouteAdmissionReject::RateLimitedNack(_) => v::TerminalReason::RateLimited,
                };
                if let Some(rep) =
                    self.order_manager
                        .on_modify_rejected(ors_order_id, reason, req.now_mono_ns)
                {
                    sink.on_report(rep);
                }
                return Ok(BinanceModifyOutcome::LocalModifyNack {
                    ors_order_id,
                    reason,
                });
            }
        };

        let conn_id = route.connection_id;
        let Some(conn_slot) = self
            .order_connections
            .get(usize::from(conn_id))
            .and_then(|s| s.as_ref())
        else {
            let reason = v::TerminalReason::MKT_REJECT_MARKET_DOWN;
            if let Some(rep) =
                self.order_manager
                    .on_modify_rejected(ors_order_id, reason, req.now_mono_ns)
            {
                sink.on_report(rep);
            }
            return Ok(BinanceModifyOutcome::LocalModifyNack {
                ors_order_id,
                reason,
            });
        };

        let seq = self.req_seq.fetch_add(1, Ordering::Relaxed);
        let req_ref = BinanceRequestRef::new(ors_order_id, BinanceRequestKind::Modify)
            .to_transport_ref(seq, 0, req.now_mono_ns);
        let reply_id = pack_ws_reply_id(ors_order_id, BinanceRequestKind::Modify);

        match conn_slot
            .locked
            .send_frame(&frame_buf[..encoded_len], reply_id, req_ref)
        {
            SendOutcome::Sent => {
                self.last_picked_conn[market.index()].store(conn_id, Ordering::Relaxed);
                Ok(BinanceModifyOutcome::Sent {
                    ors_order_id,
                    conn_id,
                    encoded_len,
                })
            }
            SendOutcome::ShortWriteRetired
            | SendOutcome::Busy
            | SendOutcome::Disconnected
            | SendOutcome::Bounds => {
                let reason = v::TerminalReason::MKT_REJECT_MARKET_DOWN;
                if let Some(rep) =
                    self.order_manager
                        .on_modify_rejected(ors_order_id, reason, req.now_mono_ns)
                {
                    sink.on_report(rep);
                }
                Ok(BinanceModifyOutcome::LocalModifyNack {
                    ors_order_id,
                    reason,
                })
            }
        }
    }

    // =========================================================================
    // RESP Hot-Path Single-Pass Frame Application (`reply_id`, `on_message`, `on_disconnected`)
    // =========================================================================

    /// Extracts the packed WebSocket `reply_id` (`pack_ws_reply_id(ors_order_id, kind)`) in one
    /// zero-allocation scan (`8.4` `replyId`).
    #[inline]
    pub fn reply_id(&self, payload: &[u8]) -> Option<u64> {
        let parsed = BinanceMsgParser::parse(payload).ok()?;
        let (ors_order_id, kind) = parsed.request_id.and_then(decode_ws_request_id)?;
        Some(pack_ws_reply_id(ors_order_id, kind))
    }

    /// Unpacks a `reply_id` back into [`BinanceRequestRef`].
    #[inline]
    pub const fn unpack_reply_ref(&self, reply_id: u64) -> Option<BinanceRequestRef> {
        match unpack_ws_reply_id(reply_id) {
            Some((ors_order_id, kind)) => Some(BinanceRequestRef { ors_order_id, kind }),
            None => None,
        }
    }

    #[inline]
    fn resolve_client_id_to_order_id(&self, market: BinanceVenueMarket, cid: &str) -> Option<u64> {
        if cid.is_empty() {
            return None;
        }
        let domain = self
            .account_domain
            .client_id_domain(market.primary_product());
        if let v::DecodedClientId::Managed(decoded) =
            v::SelfDescribingClientId::decode_str(cid, &domain)
            && let Some(strat_id) = decoded.ors_strat_id()
        {
            return Some(make_ors_order_id(decoded.intent_id, strat_id));
        }
        parse_u64_ascii(cid.as_bytes()).filter(|&id| id > 0)
    }

    fn schedule_delayed_place_query(&self, ors_order_id: u64, note: &str, now_mono_ns: u64) {
        let Some(mut locked) = self.order_manager.lock_order(ors_order_id) else {
            return;
        };
        locked.order_mut().notes.append_note(note);
        let o = *locked.order();
        drop(locked);
        let due_ns = now_mono_ns.saturating_add(self.status_query_delay_ns);
        let _ = self.order_manager.query_desk().schedule_status_ask(
            o.account_ref,
            o.ors_order_id,
            o.slot_idx,
            o.strat_id,
            o.symbol_id,
            o.order_seqnum,
            OrsQueryOrigin::Place,
            due_ns,
            |slot_idx, oid| self.order_manager.is_slot_live(slot_idx, oid),
        );
    }

    fn schedule_delayed_cancel_query(
        &self,
        ors_order_id: u64,
        now_mono_ns: u64,
        sink: &mut impl BinanceEventSink,
    ) {
        let Some(mut locked) = self.order_manager.lock_order(ors_order_id) else {
            return;
        };
        locked.order_mut().notes.append_note("cancel_mkt_delayed");
        // Leave `pending_cxl` untouched (`> 0`) per `human-docs/8.5` lines 69-71 & 194-197!
        let o = *locked.order();
        drop(locked);

        let due_ns = now_mono_ns.saturating_add(self.status_query_delay_ns);
        let scheduled = self
            .order_manager
            .query_desk()
            .schedule_status_ask(
                o.account_ref,
                o.ors_order_id,
                o.slot_idx,
                o.strat_id,
                o.symbol_id,
                o.order_seqnum,
                OrsQueryOrigin::Cancel,
                due_ns,
                |slot_idx, oid| self.order_manager.is_slot_live(slot_idx, oid),
            )
            .is_ok();

        sink.on_report(OrsReport::CancelRejected(OrsCancelRejectedReport {
            ors_order_id: o.ors_order_id,
            strat_id: o.strat_id,
            account_ref: o.account_ref,
            symbol_id: o.symbol_id,
            order_seqnum: o.order_seqnum,
            reason: v::TerminalReason::MKT_DELAYED,
            scheduled_status_query: scheduled,
            received_mono_ns: now_mono_ns,
        }));
    }

    fn reconcile_rate_limits_from_frame(
        &self,
        market: BinanceVenueMarket,
        conn_local_ip: IpKey,
        rows: &[BinanceRateLimitRow<'_>],
        http_status: Option<u16>,
        now_mono_ns: u64,
        sink: &mut impl BinanceEventSink,
    ) {
        if let Some(status) = http_status
            && (status == 429 || status == 418)
        {
            let cooldown_until = now_mono_ns.saturating_add(60 * NS_PER_SEC);
            let _ = self.quota.observe_http_status(
                self.account_ref,
                conn_local_ip,
                status,
                cooldown_until,
            );
        }

        let q_market = quota_market(market);
        for row in rows {
            let (Some(limit), Some(count), Some(rl_type), Some((interval_initial, interval_s))) = (
                row.limit,
                row.count,
                row.rate_limit_type,
                row.interval_spec(),
            ) else {
                continue;
            };
            if limit == 0 {
                continue;
            }
            let event_res: Result<Option<ExchRateLimitEvent>, RateReject> = if rl_type
                == "REQUEST_WEIGHT"
                && !conn_local_ip.is_none()
            {
                self.quota
                    .reconcile_ip_weight(q_market, conn_local_ip, count, limit, now_mono_ns)
            } else if rl_type == "ORDERS" {
                self.quota
                    .reconcile_order_count(
                        q_market,
                        self.account_ref,
                        interval_s,
                        count,
                        limit,
                        now_mono_ns,
                    )
                    .or_else(|_| {
                        let fallback_id = RateLimitId {
                            local_ip: IpKey::NONE,
                            interval_s,
                            account: u64::from(self.account_ref),
                            flags: RateFlags::PER_ACCOUNT
                                .union(RateFlags::PER_SYMBOL)
                                .union(RateFlags::ORDER),
                            symbol: q_market.code(),
                        };
                        self.quota
                            .reconcile_venue_usage(fallback_id, count, limit, now_mono_ns)
                    })
            } else {
                let custom_id = RateLimitId {
                    local_ip: IpKey::NONE,
                    interval_s,
                    account: u64::from(self.account_ref),
                    flags: RateFlags::PER_ACCOUNT
                        .union(RateFlags::PER_SYMBOL)
                        .union(RateFlags::REQUEST),
                    symbol: q_market.code(),
                };
                self.quota
                    .reconcile_venue_usage(custom_id, count, limit, now_mono_ns)
            };

            if let Ok(Some(ev)) = event_res {
                sink.on_exch_rate_limit(ExchRateLimitNotice {
                    account_ref: self.account_ref,
                    market,
                    rate_limit_type: InlineAscii::lossy_ascii(rl_type),
                    interval_initial,
                    interval_num: row.interval_num.unwrap_or(1),
                    count: ev.count,
                    limit: ev.limit,
                    warn_pct: ev.warn_pct,
                    hard_limit_hit: ev.level == ExchRateLimitLevel::CeilingReached
                        || ev.crossed_ceiling,
                    observed_at_ns: ev.observed_at_ns,
                });
            }
        }
    }

    /// Handles link teardown (`8.4` `onDisconnected`):
    /// - For Options `HttpsLink`, any in-flight `BinanceRequestRef` is treated identically to
    ///   `-1007` (`MKT_DELAYED`), scheduling `OrsQueryDesk` (`Place` or `Cancel`) after `6000ms`!
    pub fn on_disconnected(
        &self,
        _conn_id: u16,
        inflight: Option<BinanceRequestRef>,
        now_mono_ns: u64,
        sink: &mut impl BinanceEventSink,
    ) {
        let Some(req_ref) = inflight else {
            return;
        };
        match req_ref.kind {
            BinanceRequestKind::Place => {
                self.schedule_delayed_place_query(
                    req_ref.ors_order_id,
                    "options_https_link_dropped",
                    now_mono_ns,
                );
            }
            BinanceRequestKind::Cancel => {
                self.schedule_delayed_cancel_query(req_ref.ors_order_id, now_mono_ns, sink);
            }
            BinanceRequestKind::Modify => {
                if let Some(rep) = self.order_manager.on_modify_rejected(
                    req_ref.ors_order_id,
                    v::TerminalReason::MKT_DELAYED,
                    now_mono_ns,
                ) {
                    sink.on_report(rep);
                }
            }
            BinanceRequestKind::Other => {}
        }
    }

    /// Processes any inbound WebSocket or HTTP JSON frame on `RESP` in **one single-pass zero-alloc
    /// scan** (`8.4` `onMessage`, `8.5`).
    pub fn on_message(
        &self,
        market: BinanceVenueMarket,
        conn_local_ip: IpKey,
        payload: &[u8],
        hint: Option<BinanceRequestRef>,
        now_mono_ns: u64,
        sink: &mut impl BinanceEventSink,
    ) -> Result<(), BinanceSessionError> {
        let parsed =
            BinanceMsgParser::parse(payload).map_err(|_| BinanceSessionError::ParseFailed)?;

        // 1. Reconcile `rateLimits[]` & HTTP 429/418 feedback.
        self.reconcile_rate_limits_from_frame(
            market,
            conn_local_ip,
            parsed.rate_limits_slice(),
            parsed.http_status,
            now_mono_ns,
            sink,
        );

        // 2. Handle `listenKeyExpired` on USD-M, Coin-M, and Options private streams.
        if parsed.event_type == Some("listenKeyExpired") {
            if let Some(rt) = self.markets[market.index()].as_ref() {
                rt.listen_key.set(ThreadTag::RESP, InlineAscii::empty());
            }
            sink.on_stream_token_expired(self.account_ref, market);
            return Ok(());
        }

        // 3. Resolve `(ors_order_id, Option<BinanceRequestKind>)`.
        let ws_decoded = parsed.request_id.and_then(decode_ws_request_id);
        let (ors_order_id_opt, req_kind_opt) = if let Some(h) = hint {
            (Some(h.ors_order_id), Some(h.kind))
        } else if let Some((oid, k)) = ws_decoded {
            (Some(oid), Some(k))
        } else {
            let mut found_oid = None;
            for cid in parsed.client_id_candidates().into_iter().flatten() {
                if let Some(oid) = self.resolve_client_id_to_order_id(market, cid) {
                    found_oid = Some(oid);
                    break;
                }
            }
            (found_oid, None)
        };

        let Some(ors_order_id) = ors_order_id_opt else {
            return Ok(());
        };

        // 4. Error frame handling (`error: { code, msg }` or HTTP >= 400).
        if parsed.has_error() {
            let code = parsed.error_code.unwrap_or(-1);
            let msg = parsed.error_msg.unwrap_or("http_error");
            let (order_tif, order_post_only, inferred_kind) = self
                .order_manager
                .lock_order(ors_order_id)
                .map(|locked| {
                    let o = locked.order();
                    let k = if o.status == OrsOrderStatus::PendingNew {
                        BinanceRequestKind::Place
                    } else if o.modify_pending {
                        BinanceRequestKind::Modify
                    } else {
                        BinanceRequestKind::Cancel
                    };
                    (Some(o.tif), o.post_only, k)
                })
                .unwrap_or((None, false, BinanceRequestKind::Place));

            let reason = explain_binance_error(code, msg, order_tif, order_post_only);
            let effective_kind = match req_kind_opt {
                Some(k) if k != BinanceRequestKind::Other => k,
                _ => inferred_kind,
            };

            match effective_kind {
                BinanceRequestKind::Place | BinanceRequestKind::Other => {
                    if code == -1007 || code == -1000 || reason == v::TerminalReason::MKT_DELAYED {
                        self.schedule_delayed_place_query(ors_order_id, msg, now_mono_ns);
                    } else if let Some(rep) = self.order_manager.on_rejected(
                        ors_order_id,
                        Some(code),
                        msg,
                        Some(reason),
                        now_mono_ns,
                    ) {
                        sink.on_report(rep);
                    }
                }
                BinanceRequestKind::Cancel => {
                    if code == -1007 || code == -1000 || reason == v::TerminalReason::MKT_DELAYED {
                        self.schedule_delayed_cancel_query(ors_order_id, now_mono_ns, sink);
                    } else if let Some(rep) = self.order_manager.on_cancel_rejected(
                        ors_order_id,
                        Some(code),
                        reason,
                        now_mono_ns,
                    ) {
                        sink.on_report(rep);
                    }
                }
                BinanceRequestKind::Modify => {
                    if let Some(rep) =
                        self.order_manager
                            .on_modify_rejected(ors_order_id, reason, now_mono_ns)
                    {
                        sink.on_report(rep);
                    }
                }
            }
            return Ok(());
        }

        // 5. Resolve symbol binding & live order metadata (if still `Live`).
        let live_meta = self.order_manager.lock_order(ors_order_id).map(|l| {
            let o = *l.order();
            (
                o.symbol_id,
                o.side,
                o.status,
                o.modify_pending,
                o.tif,
                o.post_only,
                o.pending_cxl,
                o.admin_cancel_requested,
                o.price_ticks,
            )
        });

        let symbol_id_opt = live_meta.map(|m| m.0).or_else(|| {
            parsed
                .symbol
                .and_then(|s| self.catalog.find_symbol_id(market, s))
        });
        let Some(symbol_id) = symbol_id_opt else {
            return Ok(());
        };
        let Some(binding) = self.catalog.symbol(symbol_id) else {
            return Ok(());
        };
        let spec = &binding.spec;

        let venue_oid_buf = parsed
            .venue_order_id
            .filter(|v| !v.is_empty())
            .map(InlineAscii::<24>::lossy_ascii);
        let venue_oid_str = venue_oid_buf.as_ref().map(InlineAscii::as_str);

        // 6A. `TRADE_LITE` / `TRADE` / Post-Trade Fee & Rebate Patch (`on_fill`).
        let is_trade_lite = parsed.event_type == Some("TRADE_LITE");
        let valid_tid = parsed.valid_trade_id();
        let has_trade_execution = is_trade_lite
            || parsed.execution_type == Some("TRADE")
            || (valid_tid.is_some()
                && parsed
                    .last_executed_qty
                    .is_some_and(|s| s != "0" && s != "0.00000000"));

        let mut applied_granular_trade = false;
        if has_trade_execution
            && let (Some(qty_lots), Some(px_ticks)) = (
                parsed.parse_last_qty_lots(spec.qty_step_atoms, spec.qty_scale),
                parsed.parse_last_price_ticks(spec.price_tick_atoms, spec.price_scale),
            )
            && qty_lots > 0
            && px_ticks > 0
        {
            applied_granular_trade = true;
            let side = live_meta
                .map(|m| m.1)
                .or(match parsed.side {
                    Some("BUY") => Some(v::Side::Buy),
                    Some("SELL") => Some(v::Side::Sell),
                    _ => None,
                })
                .unwrap_or(v::Side::Buy);

            let liquidity = parsed.liquidity();

            // `TRADE_LITE` carries `fee = None`; full `ORDER_TRADE_UPDATE` / `executionReport`
            // carries `"n"` (`commission`), negated so fees are `< 0` and rebates are `> 0`.
            let fee = if is_trade_lite {
                None
            } else if let Some(comm_str) = parsed.commission {
                let (fee_key, fee_scale) = parsed
                    .commission_asset
                    .and_then(|code| self.catalog.find_asset(market, code))
                    .unwrap_or_else(|| spec.hold_asset_for_side(v::Side::Buy));
                parse_scaled_i128(comm_str, fee_scale, true).map(|raw_comm| OrsFeeSpec {
                    asset_key: fee_key,
                    scale: fee_scale,
                    signed_atoms: -raw_comm,
                })
            } else {
                None
            };

            let (tid_ascii, exec_ref) = match valid_tid {
                Some(tid_str) => (
                    InlineAscii::<64>::lossy_ascii(tid_str),
                    parse_u64_ascii(tid_str.as_bytes()).unwrap_or(0),
                ),
                None => (InlineAscii::empty(), 0),
            };

            let fill_obs = OrsFillObservation {
                ors_order_id,
                account_ref: self.account_ref,
                symbol_id,
                side,
                liquidity,
                exec_ref,
                trade_id: tid_ascii,
                qty_lots,
                price_ticks: px_ticks,
                fee,
                received_mono_ns: now_mono_ns,
            };

            if let Ok(Some(rep)) = self.order_manager.on_fill(&fill_obs) {
                sink.on_report(rep);
            }
        }

        // 6B. `AMENDMENT` or `order.modify` / `order.cancelReplace` reply.
        let is_amendment = parsed.execution_type == Some("AMENDMENT")
            || req_kind_opt == Some(BinanceRequestKind::Modify);
        if is_amendment {
            let new_px = parsed
                .parse_price_ticks(spec.price_tick_atoms, spec.price_scale)
                .filter(|&v| v > 0);
            let new_qty = parsed
                .parse_orig_qty_lots(spec.qty_step_atoms, spec.qty_scale)
                .filter(|&v| v > 0);

            if let (Some(px_ticks), Some(qty_lots)) = (new_px, new_qty) {
                let was_modify_pending = live_meta.is_some_and(|m| m.3)
                    || req_kind_opt == Some(BinanceRequestKind::Modify);
                let was_pending_new = live_meta.is_some_and(|m| m.2 == OrsOrderStatus::PendingNew);

                if was_modify_pending {
                    if let Ok(Some(rep)) = self.order_manager.on_modify_ack(
                        ors_order_id,
                        px_ticks,
                        qty_lots,
                        venue_oid_str,
                        now_mono_ns,
                    ) {
                        sink.on_report(rep);
                    }
                } else if let Ok(ack) = self.order_manager.on_ack(
                    ors_order_id,
                    venue_oid_str,
                    Some(px_ticks),
                    Some(qty_lots),
                    now_mono_ns,
                ) {
                    if was_pending_new {
                        if let Some(rep) = ack.report {
                            sink.on_report(rep);
                        }
                        if ack.dispatch_pending_cancel {
                            self.dispatch_queued_cancel_after_ack(ors_order_id, now_mono_ns, sink);
                        }
                    } else if ack.report.is_some() {
                        // Unsolicited venue `AMENDMENT` on an already-`Open` order (`8.5` line 54):
                        // order/hold/STP updated in place; notify INFO callback without emitting a
                        // strategy `Modified` message.
                        sink.on_venue_amendment(self.account_ref, ors_order_id, px_ticks, qty_lots);
                    }
                }
            }
            return Ok(());
        }

        // 6C. `One ack` (`PendingNew -> Open`) on `NEW` / `PARTIALLY_FILLED` / `FILLED` / `ACCEPTED`
        // (including `TRADE_LITE` when it arrives before `NEW`, where `order_status` is `None`!).
        let status_str = parsed.order_status.unwrap_or("");
        let is_ack_eligible = is_trade_lite
            || matches!(
                status_str,
                "NEW" | "PARTIALLY_FILLED" | "FILLED" | "ACCEPTED"
            );
        if is_ack_eligible {
            let ack_px = parsed
                .parse_price_ticks(spec.price_tick_atoms, spec.price_scale)
                .filter(|&v| v > 0);
            let ack_qty = parsed
                .parse_orig_qty_lots(spec.qty_step_atoms, spec.qty_scale)
                .filter(|&v| v > 0);

            let was_pending_new = self
                .order_manager
                .lock_order(ors_order_id)
                .is_some_and(|l| l.order().status == OrsOrderStatus::PendingNew);
            if was_pending_new
                && let Ok(ack) = self.order_manager.on_ack(
                    ors_order_id,
                    venue_oid_str,
                    ack_px,
                    ack_qty,
                    now_mono_ns,
                )
            {
                if let Some(rep) = ack.report {
                    sink.on_report(rep);
                }
                if ack.dispatch_pending_cancel {
                    self.dispatch_queued_cancel_after_ack(ors_order_id, now_mono_ns, sink);
                }
            }
        }

        // 6D. `ProjectedFill` (`on_cum_qty`) when `executedQty` (`z`) advances ahead of granular
        // fills (e.g., synchronous `order.place` / `order.cancel` reply arriving before stream fills).
        if !applied_granular_trade
            && let Some(cum_lots) =
                parsed.parse_executed_qty_lots(spec.qty_step_atoms, spec.qty_scale)
            && cum_lots > 0
        {
            let proj_px = self
                .derive_projected_price_ticks(&parsed, spec, cum_lots)
                .or_else(|| live_meta.map(|m| m.8))
                .filter(|&v| v > 0);
            if let Some(px_ticks) = proj_px
                && let Ok(Some(rep)) =
                    self.order_manager
                        .on_cum_qty(ors_order_id, cum_lots, px_ticks, now_mono_ns)
            {
                sink.on_report(rep);
            }
        }

        // 6E. Terminal status transitions (`REJECTED`, `CANCELED`, `EXPIRED`, `EXPIRED_IN_MATCH`).
        match status_str {
            "REJECTED" => {
                if let Some(rep) = self.order_manager.on_rejected(
                    ors_order_id,
                    parsed.error_code,
                    status_str,
                    Some(v::TerminalReason::MKT_REJECT),
                    now_mono_ns,
                ) {
                    sink.on_report(rep);
                }
            }
            "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" => {
                let (tif_opt, post_only, pending_cxl, admin_cancel) = self
                    .order_manager
                    .lock_order(ors_order_id)
                    .map(|l| {
                        let o = l.order();
                        (
                            Some(o.tif),
                            o.post_only,
                            o.pending_cxl,
                            o.admin_cancel_requested,
                        )
                    })
                    .unwrap_or((
                        live_meta.map(|m| m.4),
                        live_meta.is_some_and(|m| m.5),
                        live_meta.map_or(0, |m| m.6),
                        live_meta.is_some_and(|m| m.7),
                    ));

                let explicit_reason = classify_binance_terminal_reason(
                    market,
                    status_str,
                    parsed.execution_type.unwrap_or(""),
                    parsed.time_in_force.unwrap_or(""),
                    tif_opt,
                    post_only,
                    parsed.expired_reason.or(parsed.stp_mode),
                    pending_cxl,
                    admin_cancel,
                );

                let cum_lots_opt =
                    parsed.parse_executed_qty_lots(spec.qty_step_atoms, spec.qty_scale);

                if let Ok(Some(rep)) = self.order_manager.on_canceled(&OrsCancelObservation {
                    ors_order_id,
                    cum_qty_lots: cum_lots_opt,
                    error_code: parsed.error_code,
                    status_or_msg: status_str,
                    stp_mode_or_reason: parsed.expired_reason.or(parsed.execution_type),
                    explicit_reason: Some(explicit_reason),
                    received_mono_ns: now_mono_ns,
                }) {
                    sink.on_report(rep);
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn derive_projected_price_ticks(
        &self,
        parsed: &crate::msg_parser::BinanceParsedFrame<'_>,
        spec: &tickwire_account::OrsSymbolSpec,
        cum_lots: i64,
    ) -> Option<i64> {
        if let Some(ap_str) = parsed.avg_price
            && let Some(px) = parse_step_i64(ap_str, spec.price_tick_atoms, spec.price_scale)
            && px > 0
        {
            return Some(px);
        }
        // Spot / Options `Z / z` (`cummulativeQuoteQty / executedQty`):
        if let Some(z_quote_str) = parsed.cum_quote_qty
            && cum_lots > 0
        {
            let quote_atoms =
                parse_scaled_i128(z_quote_str, spec.price_scale + spec.qty_scale, true)?;
            if quote_atoms > 0 {
                let qty_atoms = i128::from(cum_lots).checked_mul(spec.qty_step_atoms)?;
                let tick_denom = qty_atoms.checked_mul(spec.price_tick_atoms)?;
                if tick_denom > 0 {
                    let ticks = quote_atoms / tick_denom;
                    if ticks > 0 && ticks <= i128::from(i64::MAX) {
                        return Some(ticks as i64);
                    }
                }
            }
        }
        parsed
            .parse_price_ticks(spec.price_tick_atoms, spec.price_scale)
            .filter(|&v| v > 0)
    }

    fn dispatch_queued_cancel_after_ack(
        &self,
        ors_order_id: u64,
        now_mono_ns: u64,
        sink: &mut impl BinanceEventSink,
    ) {
        let Some(locked) = self.order_manager.lock_order(ors_order_id) else {
            return;
        };
        let o = *locked.order();
        drop(locked);
        let mut frame_buf = [0u8; 1024];
        let wall_ms = (now_mono_ns / NS_PER_MS).max(1);
        let _ = self.dispatch_cancel_wire(
            o.ors_order_id,
            o.symbol_id,
            o.client_id.as_str(),
            o.venue_order_id.as_str(),
            None,
            wall_ms,
            now_mono_ns,
            &mut frame_buf,
            sink,
        );
    }

    // =========================================================================
    // Per-Account `OrsQueryDesk` & Cold REST Bootstrap (`8.5`, `12/binance.md`)
    // =========================================================================

    /// Builds a signed `GET /{api/v3,fapi/v1,dapi/v1,eapi/v1}/order` status query with
    /// `recvWindow=60000` (`human-docs/12/binance.md`).
    pub fn order_status_query(
        &self,
        ors_order_id: u64,
        kind: BinanceRequestKind,
        wall_ms: u64,
    ) -> Result<BinanceRestCall, BinanceSessionError> {
        let (symbol_id, cid) = {
            let locked = self
                .order_manager
                .lock_order(ors_order_id)
                .ok_or(BinanceSessionError::OrderNotFound(ors_order_id))?;
            (locked.order().symbol_id, locked.order().client_id)
        };
        let binding = self
            .catalog
            .symbol(symbol_id)
            .ok_or(BinanceSessionError::UnknownSymbol(symbol_id))?;
        let market = binding.market;
        let rt = self.market_rt(market)?;

        if cid.is_empty() {
            return Err(BinanceSessionError::EncodeFailed(
                "missing client_id on live order for status query",
            ));
        }

        let mut target_buf = [0u8; 768];
        let len = encode_signed_order_status_target(
            market,
            &binding.venue_symbol,
            cid.as_str(),
            u64::from(STATUS_QUERY_RECV_WINDOW_MS),
            wall_ms,
            &rt.rest_query_signer,
            &mut target_buf,
        )
        .map_err(|_| {
            BinanceSessionError::EncodeFailed("encode_signed_order_status_target failed")
        })?;
        let target = core::str::from_utf8(&target_buf[..len])
            .map(String::from)
            .map_err(|_| BinanceSessionError::EncodeFailed("non-utf8 query target"))?;

        Ok(BinanceRestCall {
            account_ref: self.account_ref,
            market,
            method: "GET",
            host: String::from(rt.urls.query_url.host()),
            port: rt.urls.query_url.port(),
            target,
            api_key: rt.api_key.clone(),
            ors_order_id,
            kind,
        })
    }

    /// Applies a `GET /.../order` status-query response (`8.4` `applyStatus`, `8.5`, `12`):
    /// - `-2013` (`Order does not exist`) closes `PendingNew` (`WaitMkt`) as `Rejected` (`NACK`).
    /// - Unanswered / transient / `-1021` retries `Place` every `1s` up to `max_query_count`
    ///   (`INTERNAL_CANCEL`) and `Cancel` with `1, 2, 4, 8, 16, 32, 64s` backoff (`<= 8` calls).
    /// - `Cancel` answered with `NEW` / `PARTIALLY_FILLED` decrements `pending_cxl` by `1` and
    ///   emits `CancelRejected` (`MKT_REJECT`), keeping `OrsQueryDesk` active if `pending_cxl > 0`.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_status(
        &self,
        ors_order_id: u64,
        kind: BinanceRequestKind,
        answered: bool,
        http_status: u16,
        body: &[u8],
        now_mono_ns: u64,
        sink: &mut impl BinanceEventSink,
    ) -> Result<BinanceStatusApplyOutcome, BinanceSessionError> {
        let Some((symbol_id, order_status, slot_idx, strat_id, order_seqnum)) =
            self.order_manager.lock_order(ors_order_id).map(|l| {
                let o = l.order();
                (
                    o.symbol_id,
                    o.status,
                    o.slot_idx,
                    o.strat_id,
                    o.order_seqnum,
                )
            })
        else {
            self.order_manager
                .query_desk()
                .complete_order(self.account_ref, ors_order_id);
            return Ok(BinanceStatusApplyOutcome::AlreadyClosed);
        };

        let binding = self
            .catalog
            .symbol(symbol_id)
            .ok_or(BinanceSessionError::UnknownSymbol(symbol_id))?;
        let spec = &binding.spec;

        if http_status == 429 || http_status == 418 {
            let _ = self.quota.observe_http_status(
                self.account_ref,
                IpKey::NONE,
                http_status,
                now_mono_ns.saturating_add(60 * NS_PER_SEC),
            );
        }

        if !answered || http_status >= 500 || http_status == 429 {
            return Ok(self.retry_status_query(
                ors_order_id,
                slot_idx,
                strat_id,
                symbol_id,
                order_seqnum,
                order_status,
                kind,
                now_mono_ns,
                sink,
            ));
        }

        let parsed = BinanceMsgParser::parse(body).map_err(|_| BinanceSessionError::ParseFailed)?;

        if let Some(code) = parsed.error_code
            && code != 0
        {
            if code == -2013 {
                // Venue definitively states the order never landed (`-2013 Order does not exist`).
                // On `PendingNew` (`WaitMkt`), `reconcile_query_snapshot` closes it as `Rejected` (`NACK`)!
                if order_status == OrsOrderStatus::PendingNew {
                    let mut reports = Vec::new();
                    let snap = OrsOrderQuerySnapshot {
                        ors_order_id,
                        account_ref: self.account_ref,
                        symbol_id,
                        status: OrsQueriedOrderStatus::NotFound,
                        venue_order_id: None,
                        price_ticks: None,
                        orig_qty_lots: None,
                        cum_filled_lots: 0,
                        avg_fill_price_ticks: None,
                        trades: &[],
                        received_mono_ns: now_mono_ns,
                    };
                    let _ = self
                        .order_manager
                        .reconcile_query_snapshot(&snap, &mut reports);
                    for rep in reports {
                        sink.on_report(rep);
                    }
                    return Ok(BinanceStatusApplyOutcome::Reconciled);
                }
            }
            // `-1021` timestamp skew, `-1000`, `-1007`, etc.: retry rather than guessing!
            return Ok(self.retry_status_query(
                ors_order_id,
                slot_idx,
                strat_id,
                symbol_id,
                order_seqnum,
                order_status,
                kind,
                now_mono_ns,
                sink,
            ));
        }

        let status_str = parsed.order_status.unwrap_or("");
        if status_str == "REJECTED" {
            if let Some(rep) = self.order_manager.on_rejected(
                ors_order_id,
                None,
                "REJECTED",
                Some(v::TerminalReason::MKT_REJECT),
                now_mono_ns,
            ) {
                sink.on_report(rep);
            }
            return Ok(BinanceStatusApplyOutcome::Reconciled);
        }

        let queried_status = match status_str {
            "NEW" => OrsQueriedOrderStatus::Open,
            "PARTIALLY_FILLED" => OrsQueriedOrderStatus::PartiallyFilled,
            "FILLED" => OrsQueriedOrderStatus::Filled,
            "CANCELED" => OrsQueriedOrderStatus::Canceled,
            "EXPIRED" | "EXPIRED_IN_MATCH" => OrsQueriedOrderStatus::Expired,
            _ => {
                return Ok(self.retry_status_query(
                    ors_order_id,
                    slot_idx,
                    strat_id,
                    symbol_id,
                    order_seqnum,
                    order_status,
                    kind,
                    now_mono_ns,
                    sink,
                ));
            }
        };

        let cum_filled_lots = parsed
            .parse_executed_qty_lots(spec.qty_step_atoms, spec.qty_scale)
            .unwrap_or(0)
            .max(0);
        let avg_px_ticks = self.derive_projected_price_ticks(&parsed, spec, cum_filled_lots);
        let px_ticks = parsed
            .parse_price_ticks(spec.price_tick_atoms, spec.price_scale)
            .filter(|&v| v > 0);
        let orig_qty_lots = parsed
            .parse_orig_qty_lots(spec.qty_step_atoms, spec.qty_scale)
            .filter(|&v| v > 0);

        let venue_oid_buf = parsed
            .venue_order_id
            .filter(|v| !v.is_empty())
            .map(InlineAscii::<24>::lossy_ascii);
        let venue_oid_str = venue_oid_buf.as_ref().map(InlineAscii::as_str);

        // Special `8.5` Cancel reconciliation rule when order is still `NEW` / `PARTIALLY_FILLED`:
        // the delayed cancel did NOT land on the book! Decrement `pending_cxl` by 1 and emit
        // `CancelRejected`, keeping status query scheduled only if additional cancels remain.
        if kind == BinanceRequestKind::Cancel
            && matches!(
                queried_status,
                OrsQueriedOrderStatus::Open | OrsQueriedOrderStatus::PartiallyFilled
            )
        {
            if cum_filled_lots > 0
                && let Some(px) = avg_px_ticks.or(px_ticks)
                && let Ok(Some(rep)) =
                    self.order_manager
                        .on_cum_qty(ors_order_id, cum_filled_lots, px, now_mono_ns)
            {
                sink.on_report(rep);
            }

            let Some(mut locked) = self.order_manager.lock_order(ors_order_id) else {
                self.order_manager
                    .query_desk()
                    .complete_order(self.account_ref, ors_order_id);
                return Ok(BinanceStatusApplyOutcome::AlreadyClosed);
            };
            locked.order_mut().pending_cxl = locked.order().pending_cxl.saturating_sub(1);
            let o = *locked.order();
            drop(locked);

            let remaining = o.pending_cxl;
            let scheduled = if remaining > 0 {
                let due = now_mono_ns.saturating_add(self.status_query_delay_ns);
                self.order_manager
                    .query_desk()
                    .schedule_status_ask(
                        self.account_ref,
                        ors_order_id,
                        o.slot_idx,
                        o.strat_id,
                        o.symbol_id,
                        o.order_seqnum,
                        OrsQueryOrigin::Cancel,
                        due,
                        |s_idx, oid| self.order_manager.is_slot_live(s_idx, oid),
                    )
                    .is_ok()
            } else {
                self.order_manager
                    .query_desk()
                    .complete_order(self.account_ref, ors_order_id);
                false
            };

            sink.on_report(OrsReport::CancelRejected(OrsCancelRejectedReport {
                ors_order_id: o.ors_order_id,
                strat_id: o.strat_id,
                account_ref: o.account_ref,
                symbol_id: o.symbol_id,
                order_seqnum: o.order_seqnum,
                reason: v::TerminalReason::MKT_REJECT,
                scheduled_status_query: scheduled,
                received_mono_ns: now_mono_ns,
            }));

            return Ok(BinanceStatusApplyOutcome::CancelRejectedOpen {
                ors_order_id,
                remaining_pending_cxl: remaining,
            });
        }

        // Standard `OrderManager::reconcile_query_snapshot` (applies catch-up fills first, emits
        // missing Ack, retires terminal orders, or signals `DispatchCancel` when `cancel_after`!).
        let snap = OrsOrderQuerySnapshot {
            ors_order_id,
            account_ref: self.account_ref,
            symbol_id,
            status: queried_status,
            venue_order_id: venue_oid_str,
            price_ticks: px_ticks,
            orig_qty_lots,
            cum_filled_lots,
            avg_fill_price_ticks: avg_px_ticks,
            trades: &[],
            received_mono_ns: now_mono_ns,
        };

        let mut reports = Vec::new();
        let action = self
            .order_manager
            .reconcile_query_snapshot(&snap, &mut reports)
            .map_err(|_| BinanceSessionError::ParseFailed)?;
        for rep in reports {
            sink.on_report(rep);
        }

        match action {
            OrsQueryReconcileAction::Resolved => Ok(BinanceStatusApplyOutcome::Reconciled),
            OrsQueryReconcileAction::DispatchCancel { ors_order_id, .. } => {
                self.dispatch_queued_cancel_after_ack(ors_order_id, now_mono_ns, sink);
                Ok(BinanceStatusApplyOutcome::DispatchedCoalescedCancel { ors_order_id })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn retry_status_query(
        &self,
        ors_order_id: u64,
        slot_idx: u32,
        strat_id: u8,
        symbol_id: u32,
        order_seqnum: u64,
        order_status: OrsOrderStatus,
        kind: BinanceRequestKind,
        now_mono_ns: u64,
        sink: &mut impl BinanceEventSink,
    ) -> BinanceStatusApplyOutcome {
        if kind == BinanceRequestKind::Cancel && order_status == OrsOrderStatus::Open {
            let desk = self.order_manager.query_desk();
            let existing = desk.get_entry(self.account_ref, ors_order_id);
            let next = if let Some(entry) = existing
                && entry.origin == OrsQueryOrigin::Cancel
                && entry.cancel_attempts > 0
                && entry.cancel_attempts < MAX_CANCEL_BACKOFF_ATTEMPTS
                && !entry.cancel_exhausted
                && entry.next_due_mono_ns > now_mono_ns
            {
                Some(entry.next_due_mono_ns)
            } else {
                desk.schedule_cancel_backoff(
                    self.account_ref,
                    ors_order_id,
                    slot_idx,
                    strat_id,
                    symbol_id,
                    order_seqnum,
                    now_mono_ns,
                    |s_idx, oid| self.order_manager.is_slot_live(s_idx, oid),
                )
                .ok()
                .flatten()
                .map(|entry| entry.next_due_mono_ns)
            };
            if next.is_none() {
                sink.on_cancel_query_exhausted(self.account_ref, ors_order_id);
            }
            return BinanceStatusApplyOutcome::RetryingCancel {
                ors_order_id,
                next_due_ns: next,
            };
        }

        if let Some(rep) = self
            .order_manager
            .on_query_inconclusive(ors_order_id, now_mono_ns)
        {
            sink.on_report(rep);
            return BinanceStatusApplyOutcome::ClosedInternalCancel { ors_order_id };
        }

        let next_due_ns = now_mono_ns.saturating_add(NS_PER_SEC);
        let _ = self.order_manager.query_desk().schedule_status_ask(
            self.account_ref,
            ors_order_id,
            slot_idx,
            strat_id,
            symbol_id,
            order_seqnum,
            OrsQueryOrigin::Place,
            next_due_ns,
            |s_idx, oid| self.order_manager.is_slot_live(s_idx, oid),
        );
        BinanceStatusApplyOutcome::RetryingPlace {
            ors_order_id,
            next_due_ns,
        }
    }

    /// Builds signed `GET /{api/v3,fapi/v1,dapi/v1,eapi/v1}/openOrders` queries across all
    /// configured markets for cold bootstrap (`8.4` `openOrdersQuery`, `12/binance.md`).
    pub fn open_orders_queries(
        &self,
        wall_ms: u64,
    ) -> Result<Vec<BinanceRestCall>, BinanceSessionError> {
        let mut out = Vec::new();
        for rt in self.markets.iter().flatten() {
            let mut target_buf = [0u8; 768];
            let len = encode_signed_open_orders_target(
                rt.market,
                None,
                u64::from(STATUS_QUERY_RECV_WINDOW_MS),
                wall_ms,
                &rt.rest_query_signer,
                &mut target_buf,
            )
            .map_err(|_| {
                BinanceSessionError::EncodeFailed("encode_signed_open_orders_target failed")
            })?;
            let target = core::str::from_utf8(&target_buf[..len])
                .map(String::from)
                .map_err(|_| BinanceSessionError::EncodeFailed("non-utf8 openOrders target"))?;
            out.push(BinanceRestCall {
                account_ref: self.account_ref,
                market: rt.market,
                method: "GET",
                host: String::from(rt.urls.query_url.host()),
                port: rt.urls.query_url.port(),
                target,
                api_key: rt.api_key.clone(),
                ors_order_id: 0,
                kind: BinanceRequestKind::Other,
            });
        }
        Ok(out)
    }

    /// Builds signed `GET /{api/v3/account,fapi/v2/account,dapi/v1/account,eapi/v1/account}`
    /// queries across all configured markets for cold bootstrap.
    pub fn account_bootstrap_queries(
        &self,
        wall_ms: u64,
    ) -> Result<Vec<BinanceRestCall>, BinanceSessionError> {
        let mut out = Vec::new();
        for rt in self.markets.iter().flatten() {
            let mut target_buf = [0u8; 768];
            let len = encode_signed_account_target(
                rt.market,
                u64::from(STATUS_QUERY_RECV_WINDOW_MS),
                wall_ms,
                &rt.rest_query_signer,
                &mut target_buf,
            )
            .map_err(|_| {
                BinanceSessionError::EncodeFailed("encode_signed_account_target failed")
            })?;
            let target = core::str::from_utf8(&target_buf[..len])
                .map(String::from)
                .map_err(|_| BinanceSessionError::EncodeFailed("non-utf8 account target"))?;
            out.push(BinanceRestCall {
                account_ref: self.account_ref,
                market: rt.market,
                method: "GET",
                host: String::from(rt.urls.query_url.host()),
                port: rt.urls.query_url.port(),
                target,
                api_key: rt.api_key.clone(),
                ors_order_id: 0,
                kind: BinanceRequestKind::Other,
            });
        }
        Ok(out)
    }

    /// Applies one market's cold-startup Exchange REST `Account + OpenOrders` snapshot into
    /// `OrderManager<CHECK_STP>`, reconstructing `SelfDescribingClientId` (`tw1...`) open orders,
    /// `UtaWalletLedger` balances/positions, `OrsStpBook`, and per-strategy `DopState` floors.
    pub fn bootstrap_market_from_rest(
        &self,
        market: BinanceVenueMarket,
        account_json: &[u8],
        open_orders_json: &[u8],
        extra_positions_json: Option<&[u8]>,
        now_mono_ns: u64,
    ) -> Result<OrsBootstrapSummary, BinanceSessionError> {
        let _ = self.market_rt(market)?;
        Ok(bootstrap_market_into_order_manager(
            &self.order_manager,
            self.account_ref,
            market,
            &self.catalog,
            account_json,
            open_orders_json,
            extra_positions_json,
            now_mono_ns,
        )?)
    }

    /// Convenience helper resolving `conn_id -> BinanceVenueMarket` for order-pool connections.
    #[inline]
    pub fn connection_market(&self, conn_id: u16) -> Option<BinanceVenueMarket> {
        self.order_connections
            .get(usize::from(conn_id))
            .and_then(|s| s.as_ref())
            .map(|s| s.market)
    }

    /// Convenience helper resolving `conn_id -> IpKey` from `PublishedRoutes`.
    pub fn connection_local_ip(&self, conn_id: u16) -> IpKey {
        let mut snap = [v::OrderRoute::default(); MAX_PUBLISHED_ROUTES];
        let n = self.published_routes.snapshot(&mut snap);
        for r in snap.iter().take(n) {
            if r.connection_id == conn_id {
                return ipv4_u32_to_ip_key(r.local_ip);
            }
        }
        IpKey::NONE
    }

    /// Helper splitting `ors_order_id -> (order_seqnum, strat_id)`.
    #[inline]
    pub const fn decode_ors_order_id(ors_order_id: u64) -> (u64, u8) {
        split_ors_order_id(ors_order_id)
    }
}

fn pick_from_explicit_routes(
    account_ref: u8,
    product: Option<v::Product>,
    last_conn_id: Option<u16>,
    routes: &[v::OrderRoute],
) -> Option<v::OrderRoute> {
    let mut first_eligible: Option<v::OrderRoute> = None;
    let mut next_after_last: Option<v::OrderRoute> = None;
    let mut best_measured: Option<v::OrderRoute> = None;
    let mut last_measured: Option<v::OrderRoute> = None;

    for &r in routes {
        if r.connection_id == v::OrderRoute::NO_CONNECTION {
            continue;
        }
        if account_ref != 0 && r.account_ref != 0 && r.account_ref != account_ref {
            continue;
        }
        if !(r.ready && r.matches_product(product)) {
            continue;
        }
        if first_eligible.is_none_or(|f| r.connection_id < f.connection_id) {
            first_eligible = Some(r);
        }
        if let Some(last_cid) = last_conn_id
            && r.connection_id > last_cid
            && next_after_last.is_none_or(|nxt| r.connection_id < nxt.connection_id)
        {
            next_after_last = Some(r);
        }
        if r.ema_score_us != v::OrderRoute::UNMEASURED_SCORE {
            if best_measured.is_none_or(|best| {
                (r.ema_score_us, r.connection_id) < (best.ema_score_us, best.connection_id)
            }) {
                best_measured = Some(r);
            }
            if last_conn_id == Some(r.connection_id) {
                last_measured = Some(r);
            }
        }
    }

    let first = first_eligible?;
    match best_measured {
        None => {
            if last_conn_id.is_some() {
                next_after_last.or(Some(first))
            } else {
                Some(first)
            }
        }
        Some(best) => {
            if let Some(last) = last_measured
                && u64::from(last.ema_score_us) * 8 <= u64::from(best.ema_score_us) * 9
            {
                return Some(last);
            }
            Some(best)
        }
    }
}
