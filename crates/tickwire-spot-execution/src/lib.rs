//! Stateless, full-product Unified Trading Account (UTA) execution sessions (`BybitUtaSession` and
//! `OkxUtaSession`), zero-allocation stack wire encoders, single-pass `JsonScan` frame decoders,
//! server-maintenance `notice` connection switchover, optional Options/Block `RFQ` quote hooks, and
//! cold Exchange REST bootstrap for Bybit V5 and OKX V5.
//!
//! # Architecture & Invariants
//!
//! - **Pure Stateless Execution**: Holds zero local write-ahead log, journal, blob store, or disk
//!   state. Cold boot and restart recover exclusively from authenticated Exchange REST snapshots
//!   (`Account + Positions + OpenOrders`) paired with 32-character `[0-9a-z]`
//!   `SelfDescribingClientId` (`tw1...`) tokens via [`rest_bootstrap::UtaBootstrapDecoder`] and
//!   [`tickwire_account::OrderManager::bootstrap_from_exchange`], keeping gateway receipts
//!   (`FrontAck`), book acceptance (`Ack`), cumulative projections (`ProjectedFill`), and granular
//!   executions (`Fill` + `amendment = true` fee updates) distinct.
//! - **All 6 UTA Products Supported**: `Product::Spot`, `Product::LinearPerp`, `Product::InversePerp`,
//!   `Product::LinearFutures`, `Product::InverseFutures`, and `Product::Options`, including Retail
//!   Price Improvement (`TimeInForce::Rpi`, Bybit `"RPI"` and OKX `ordType: "rpi"`) and optional
//!   Options/Block `RFQ` quoting ([`rfq::RfqQuoteHook`]).
//! - **Dual-Transport Shared UTA Connection Pool (`product_mask = 0`)**: Every [`UtaAccountSession`]
//!   (`1 AccountSession = 1 Exchange UID`) routes all UTA products across a shared `[local_ip, count]`
//!   connection pool (`WsLink` `Shared` 16-in-flight or `HttpsLink` `Exclusive`), selecting routes
//!   via [`tickwire_transport::order_router::PublishedRoutes::pick`] (`9/8` EWMA hysteresis) and
//!   spending [`tickwire_quota::shared::SharedRateLimiter`] (`BybitUta` / `OkxUta` + cross-account
//!   `local_ip` budget) strictly after `pick` (NACKing immediately on rate-limit exhaustion without
//!   retrying another connection).
//! - **Server Maintenance `notice` Graceful Switchover**: Detects OKX `"event":"notice","code":"64008"`
//!   and Bybit connection-retire/maintenance frames in one `JsonScan` pass, immediately marking the
//!   retiring link unpickable (`ready = false`) in `PublishedRoutes`, requesting `Dialer` to pre-warm
//!   a replacement link in parallel, draining in-flight replies, and preserving 100% of open orders.
//! - **Lock Discipline & Memory Safety**: `#![forbid(unsafe_code)]`, zero floating-point arithmetic,
//!   and zero heap allocation, logging, or JSON/HTTP/WS parsing while holding any spinlock. `Order`
//!   and `Connection` spinlocks never overlap.

#![forbid(unsafe_code)]

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::{
    digest::{SHA256, digest},
    hmac,
};
use std::fmt;

pub use tickwire_venue::{Environment, Product, Side, TerminalReason, TimeInForce, VenueTradeId};

pub mod json_parser;
pub mod network;
pub mod observe;
pub mod private;
pub mod rest_bootstrap;
pub mod rfq;
pub mod session;
pub mod warmup;
pub mod wire_encode;
pub mod ws_envelope;

pub use json_parser::{
    BybitUtaParser, OkxUtaParser, SpotIssueKind, UtaCommandReply, UtaExecRow, UtaFrameEvent,
    UtaOrderRow, UtaReplyOp, UtaScanSummary, UtaWalletCoinRow, classify_issue,
    is_http_account_hazard, is_venue_rate_limit_code, parse_positive_step_i64,
    parse_signed_scaled_i128, scan_bybit_frame, scan_okx_frame,
};
pub use rest_bootstrap::{
    DecodedStatusAsk, UtaAssetBinding, UtaBootstrapDecoder, UtaRestRequestBuilder,
    UtaSymbolBinding, decode_status_ask_snapshot,
};
pub use rfq::{
    BuiltRfqPayload, DEFAULT_MAX_OPEN_RFQ_QUOTES, MAX_RFQ_LEGS, NoopRfqQuoteHook,
    RfqCancelQuotesRequest, RfqQuoteHook, RfqQuoteLeg, RfqQuoteRequest, RfqQuoteSnapshot,
    RfqQuoteState, UtaRfqQuoteBook,
};
pub use session::{
    BybitUtaSession, OkxUtaSession, UtaAccountSession, UtaConnPoolEntry, UtaPollReport,
    UtaSessionConfig, UtaSubmitOutcome, UtaSubmitRefusal, UtaTransportMode,
};
pub use warmup::{
    SpotHotPathWarmer, SpotWarmupConfig, SpotWarmupReport, UtaHotPathWarmer, UtaWarmupConfig,
    UtaWarmupReport,
};
pub use wire_encode::{
    BufWriter, UtaCancelWireInput, UtaModifyWireInput, UtaNewOrderWireInput, UtaOrderParams,
    encode_bybit_cancel_arg, encode_bybit_cancel_body_into, encode_bybit_modify_arg,
    encode_bybit_modify_body_into, encode_bybit_new_order_arg, encode_bybit_new_order_body_into,
    encode_bybit_ws_trade_envelope_into, encode_okx_cancel_arg, encode_okx_cancel_body_into,
    encode_okx_modify_arg, encode_okx_modify_body_into, encode_okx_new_order_arg,
    encode_okx_new_order_body_into, encode_okx_ws_trade_envelope_into,
    encode_signed_https_post_into, format_okx_iso8601_into, write_hex_lower_32,
    write_scaled_atoms_into, write_u64_into,
};
pub use ws_envelope::{
    HttpScannedResponse, WS_PING_PERIOD_NS, WS_PING_PERIOD_S, WsLoginOutcome, WsServerFrame,
    check_bybit_auth_reply, check_okx_login_reply, encode_bybit_auth_into,
    encode_bybit_private_subscribe_step_into, encode_okx_login_into,
    encode_okx_subscribe_step_into, encode_ws_client_frame_into, encode_ws_ping_into,
    try_decode_http_response, try_decode_ws_server_frame,
};

/// Supported Unified Trading Account venues in `tickwire-spot-execution`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpotVenue {
    Bybit,
    Okx,
}

impl SpotVenue {
    /// Stable numeric venue ID used in `ClientIdDomain` and catalog metadata (`Bybit = 2`, `Okx = 3`).
    #[inline]
    pub const fn venue_id(self) -> u32 {
        match self {
            Self::Bybit => 2,
            Self::Okx => 3,
        }
    }

    /// Maps this venue to its `tickwire-quota` [`tickwire_quota::shared::OrsVenueProfile`].
    #[inline]
    pub const fn quota_profile(self) -> tickwire_quota::shared::OrsVenueProfile {
        match self {
            Self::Bybit => tickwire_quota::shared::OrsVenueProfile::BybitUta,
            Self::Okx => tickwire_quota::shared::OrsVenueProfile::OkxUta,
        }
    }

    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bybit => "bybit",
            Self::Okx => "okx",
        }
    }

    #[inline]
    pub const fn id(self) -> &'static str {
        self.as_str()
    }
}

/// Errors returned by UTA wire builders, protocol scanners, and cold REST bootstrap helpers.
#[derive(Debug)]
pub enum UtaVenueError {
    Bounds,
    Encoding,
    Identity,
    Protocol(&'static str),
    Transport(tickwire_transport::Error),
    Refusal(TerminalReason),
}

impl fmt::Display for UtaVenueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bounds => f.write_str("UTA wire buffer capacity exceeded"),
            Self::Encoding => f.write_str("invalid UTA wire parameter or numeric encoding"),
            Self::Identity => f.write_str("invalid UTA venue credentials or account identity"),
            Self::Protocol(msg) => write!(f, "UTA venue protocol error: {msg}"),
            Self::Transport(err) => write!(f, "UTA transport error: {err}"),
            Self::Refusal(reason) => write!(f, "UTA order refusal: {reason}"),
        }
    }
}

impl std::error::Error for UtaVenueError {}

impl From<tickwire_transport::Error> for UtaVenueError {
    fn from(value: tickwire_transport::Error) -> Self {
        Self::Transport(value)
    }
}

/// Bybit V5 Unified Trading Account `category` selector covering all 6 Tickwire UTA products.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BybitCategory {
    Spot,
    Linear,
    Inverse,
    Option,
}

impl BybitCategory {
    pub const ALL: [Self; 4] = [Self::Spot, Self::Linear, Self::Inverse, Self::Option];

    /// Maps a canonical [`Product`] to its Bybit V5 `category`.
    #[inline]
    pub const fn from_product(product: Product) -> Option<Self> {
        match product {
            Product::Spot => Some(Self::Spot),
            Product::LinearPerpetual | Product::LinearFuture | Product::UsdM => Some(Self::Linear),
            Product::InversePerpetual | Product::InverseFuture | Product::CoinM => {
                Some(Self::Inverse)
            }
            Product::Option => Some(Self::Option),
        }
    }

    #[inline]
    pub const fn parse(raw: &str) -> Option<Self> {
        if raw.eq_ignore_ascii_case("spot") {
            Some(Self::Spot)
        } else if raw.eq_ignore_ascii_case("linear") {
            Some(Self::Linear)
        } else if raw.eq_ignore_ascii_case("inverse") {
            Some(Self::Inverse)
        } else if raw.eq_ignore_ascii_case("option") {
            Some(Self::Option)
        } else {
            None
        }
    }

    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Linear => "linear",
            Self::Inverse => "inverse",
            Self::Option => "option",
        }
    }

    /// Product-bucket code expected by [`tickwire_quota::shared::OrsVenueProfile::BybitUta`]:
    /// `1` = Spot, `2` = Derivatives (`Linear` / `Inverse`), `3` = Option.
    #[inline]
    pub const fn rate_product_code(self) -> u8 {
        match self {
            Self::Spot => 1,
            Self::Linear | Self::Inverse => 2,
            Self::Option => 3,
        }
    }
}

/// Bybit V5 `positionIdx` selector for one-way (`0`) vs. hedge-mode (`1 = Buy`, `2 = Sell`) derivatives.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum BybitPositionIdx {
    #[default]
    OneWay = 0,
    BuyHedge = 1,
    SellHedge = 2,
}

impl BybitPositionIdx {
    #[inline]
    pub const fn code(self) -> u8 {
        self as u8
    }
}

/// OKX V5 Unified Trading Account `instType` selector covering all 6 Tickwire UTA products.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OkxInstType {
    Spot,
    Swap,
    Futures,
    Option,
}

impl OkxInstType {
    pub const ALL: [Self; 4] = [Self::Spot, Self::Swap, Self::Futures, Self::Option];

    /// Maps a canonical [`Product`] to its OKX V5 `instType`.
    #[inline]
    pub const fn from_product(product: Product) -> Option<Self> {
        match product {
            Product::Spot => Some(Self::Spot),
            Product::LinearPerpetual
            | Product::InversePerpetual
            | Product::UsdM
            | Product::CoinM => Some(Self::Swap),
            Product::LinearFuture | Product::InverseFuture => Some(Self::Futures),
            Product::Option => Some(Self::Option),
        }
    }

    #[inline]
    pub const fn parse(raw: &str) -> Option<Self> {
        if raw.eq_ignore_ascii_case("SPOT") {
            Some(Self::Spot)
        } else if raw.eq_ignore_ascii_case("SWAP") {
            Some(Self::Swap)
        } else if raw.eq_ignore_ascii_case("FUTURES") {
            Some(Self::Futures)
        } else if raw.eq_ignore_ascii_case("OPTION") {
            Some(Self::Option)
        } else {
            None
        }
    }

    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "SPOT",
            Self::Swap => "SWAP",
            Self::Futures => "FUTURES",
            Self::Option => "OPTION",
        }
    }
}

/// OKX V5 trade/margin mode (`tdMode`) for UTA orders.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum OkxMarginMode {
    Cash,
    #[default]
    Cross,
    Isolated,
}

impl OkxMarginMode {
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cash => "cash",
            Self::Cross => "cross",
            Self::Isolated => "isolated",
        }
    }
}

/// Borrowed API credentials with pre-expanded `ring::hmac::Key` so hot-path HMAC-SHA256 signing
/// performs zero heap allocation and zero key-schedule expansion per request.
#[derive(Clone)]
pub struct Credentials<'a> {
    pub(crate) venue: SpotVenue,
    pub(crate) key: &'a str,
    pub(crate) secret: &'a [u8],
    pub(crate) passphrase: Option<&'a str>,
    hmac_key: hmac::Key,
}

impl<'a> Credentials<'a> {
    pub fn bybit(key: &'a str, secret: &'a [u8]) -> Self {
        Self {
            venue: SpotVenue::Bybit,
            key,
            secret,
            passphrase: None,
            hmac_key: hmac::Key::new(hmac::HMAC_SHA256, secret),
        }
    }

    pub fn okx(key: &'a str, secret: &'a [u8], passphrase: &'a str) -> Self {
        Self {
            venue: SpotVenue::Okx,
            key,
            secret,
            passphrase: Some(passphrase),
            hmac_key: hmac::Key::new(hmac::HMAC_SHA256, secret),
        }
    }

    #[inline]
    pub const fn venue(&self) -> SpotVenue {
        self.venue
    }

    #[inline]
    pub const fn key(&self) -> &'a str {
        self.key
    }

    #[inline]
    pub const fn secret(&self) -> &'a [u8] {
        self.secret
    }

    #[inline]
    pub const fn passphrase(&self) -> Option<&'a str> {
        self.passphrase
    }

    #[inline]
    pub const fn hmac_key(&self) -> &hmac::Key {
        &self.hmac_key
    }

    #[inline]
    pub fn valid(&self) -> bool {
        header_ok(self.key) && !self.secret.is_empty() && self.passphrase.is_none_or(header_ok)
    }
}

/// Cold-path HTTP request descriptor used by `observe` and cold REST bootstrap helpers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub method: &'static str,
    pub path: String,
    pub body: Vec<u8>,
}

/// REST HTTPS authority hostname for `(venue, environment)`.
#[inline]
pub const fn authority(venue: SpotVenue, env: Environment) -> &'static str {
    match (venue, env) {
        (SpotVenue::Bybit, Environment::Test) => "api-testnet.bybit.com",
        (SpotVenue::Bybit, Environment::Production) => "api.bybit.com",
        (SpotVenue::Okx, Environment::Test | Environment::Production) => "www.okx.com",
    }
}

/// WebSocket trade (`order.create` / `op: "order"`) `(host, port, path)` endpoint for `(venue, env)`.
#[inline]
pub const fn ws_trade_endpoint(
    venue: SpotVenue,
    env: Environment,
) -> (&'static str, u16, &'static str) {
    match (venue, env) {
        (SpotVenue::Bybit, Environment::Test) => ("stream-testnet.bybit.com", 443, "/v5/trade"),
        (SpotVenue::Bybit, Environment::Production) => ("stream.bybit.com", 443, "/v5/trade"),
        (SpotVenue::Okx, Environment::Test) => ("wspap.okx.com", 8443, "/ws/v5/private"),
        (SpotVenue::Okx, Environment::Production) => ("ws.okx.com", 8443, "/ws/v5/private"),
    }
}

/// WebSocket private account/order/execution push `(host, port, path)` endpoint for `(venue, env)`.
#[inline]
pub const fn ws_private_endpoint(
    venue: SpotVenue,
    env: Environment,
) -> (&'static str, u16, &'static str) {
    match (venue, env) {
        (SpotVenue::Bybit, Environment::Test) => ("stream-testnet.bybit.com", 443, "/v5/private"),
        (SpotVenue::Bybit, Environment::Production) => ("stream.bybit.com", 443, "/v5/private"),
        (SpotVenue::Okx, Environment::Test) => ("wspap.okx.com", 8443, "/ws/v5/private"),
        (SpotVenue::Okx, Environment::Production) => ("ws.okx.com", 8443, "/ws/v5/private"),
    }
}

/// Validates an HTTP header token/value slice (`1..=1024` visible ASCII bytes).
#[inline]
pub fn header_ok(s: &str) -> bool {
    !s.is_empty() && s.len() <= 1024 && s.bytes().all(|b| (0x21..=0x7e).contains(&b))
}

/// Validates a Bybit/OKX instrument identifier across Spot, Swap, Futures, and Options
/// (e.g. `"BTCUSDT"`, `"BTC-USDT-SWAP"`, `"BTC-28MAR26-90000-C"`, `"BTC-USD-261225-90000-C"`).
#[inline]
pub fn symbol_ok(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Validates a venue client order ID (`orderLinkId` up to 36 chars on Bybit, `clOrdId` `1..=32`
/// alphanumeric on OKX — both accepting 32-char `[0-9a-z]` `tw1...` `SelfDescribingClientId`).
#[inline]
pub fn client_ok(venue: SpotVenue, id: &str) -> bool {
    match venue {
        SpotVenue::Bybit => {
            !id.is_empty()
                && id.len() <= 36
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        }
        SpotVenue::Okx => {
            !id.is_empty() && id.len() <= 32 && id.bytes().all(|b| b.is_ascii_alphanumeric())
        }
    }
}

/// Deterministic non-zero 64-bit FNV-1a hash of `s` for `InstrumentScope::Symbol` / `Family` keys.
#[inline]
pub fn fnv1a_nonzero_u64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    if hash == 0 { 1 } else { hash }
}

/// Computes the non-zero `InstrumentScope::Symbol` key for `symbol`.
#[inline]
pub fn symbol_rate_key(symbol: &str) -> u64 {
    fnv1a_nonzero_u64(symbol.as_bytes())
}

/// Extracts the OKX underlying/family prefix (`"BTC-USD"` from `"BTC-USD-261225-90000-C"`) and
/// hashes it into a non-zero `u64` for `InstrumentScope::Family`.
#[inline]
pub fn okx_option_family_key(inst_id: &str) -> u64 {
    let bytes = inst_id.as_bytes();
    let mut hyphens = 0usize;
    let mut prefix_end = bytes.len();
    for (idx, &b) in bytes.iter().enumerate() {
        if b == b'-' {
            hyphens += 1;
            if hyphens == 2 {
                prefix_end = idx;
                break;
            }
        }
    }
    fnv1a_nonzero_u64(&bytes[..prefix_end])
}

/// Computes the [`tickwire_quota::shared::InstrumentScope`] required by `SharedRateLimiter` for
/// `(venue, product, symbol)` with zero heap allocation.
#[inline]
pub fn instrument_rate_scope(
    venue: SpotVenue,
    product: Product,
    symbol: &str,
) -> tickwire_quota::shared::InstrumentScope {
    match venue {
        SpotVenue::Bybit => {
            let cat = BybitCategory::from_product(product).unwrap_or(BybitCategory::Spot);
            tickwire_quota::shared::InstrumentScope::Product(u64::from(cat.rate_product_code()))
        }
        SpotVenue::Okx => {
            if product == Product::Option {
                tickwire_quota::shared::InstrumentScope::Family(okx_option_family_key(symbol))
            } else {
                tickwire_quota::shared::InstrumentScope::Symbol(symbol_rate_key(symbol))
            }
        }
    }
}

/// Encodes `bytes` as lowercase hexadecimal ASCII.
pub fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[usize::from(b >> 4)] as char);
        out.push(HEX[usize::from(b & 0x0f)] as char);
    }
    out
}

/// Normalizes a venue execution ID (`execId` / `tradeId`) into a canonical [`VenueTradeId`],
/// hashing identifiers longer than 64 bytes or outside `[0-9A-Za-z._:-]` with SHA-256 (`h_<hex62>`).
pub fn trade_id(venue: SpotVenue, raw: &str) -> Result<VenueTradeId, &'static str> {
    if raw.is_empty() {
        return Err("empty trade identifier");
    }
    if raw.len() <= 64
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
    {
        return VenueTradeId::new(raw).map_err(|_| "invalid trade identifier");
    }
    let tag = match venue {
        SpotVenue::Bybit => "bybit:",
        SpotVenue::Okx => "okx:",
    };
    let mut data = Vec::with_capacity(tag.len() + raw.len());
    data.extend_from_slice(tag.as_bytes());
    data.extend_from_slice(raw.as_bytes());
    let d = hex(digest(&SHA256, &data).as_ref());
    VenueTradeId::new(format!("h_{}", &d[..62])).map_err(|_| "invalid hashed trade identifier")
}

/// Zero-allocation stack normalization of a venue execution ID into `InlineAscii<64>` and deterministic
/// non-zero `exec_ref: u64` for [`tickwire_account::OrsFillObservation`].
pub fn trade_id_inline(
    venue: SpotVenue,
    raw: &str,
) -> Option<(tickwire_account::InlineAscii<64>, u64)> {
    if raw.is_empty() {
        return None;
    }
    let exec_ref = fnv1a_nonzero_u64(raw.as_bytes());
    if raw.len() <= 64
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
    {
        let inline = tickwire_account::InlineAscii::try_from_str(raw)?;
        return Some((inline, exec_ref));
    }
    let prefix: &[u8] = match venue {
        SpotVenue::Bybit => b"bybit:",
        SpotVenue::Okx => b"okx:",
    };
    let mut ctx = ring::digest::Context::new(&SHA256);
    ctx.update(prefix);
    ctx.update(raw.as_bytes());
    let tag = ctx.finish();
    let mut hex64 = [0u8; 64];
    write_hex_lower_32(tag.as_ref(), &mut hex64);
    let mut hashed = [0u8; 64];
    hashed[0] = b'h';
    hashed[1] = b'_';
    hashed[2..64].copy_from_slice(&hex64[..62]);
    let s = core::str::from_utf8(&hashed).ok()?;
    let inline = tickwire_account::InlineAscii::try_from_str(s)?;
    Some((inline, exec_ref))
}

/// Computes authenticated HTTP headers (excluding `"Host"`) for cold-path REST `GET`/`POST` requests.
pub fn sign_headers(
    c: &Credentials<'_>,
    env: Environment,
    utc_ms: u64,
    method: &'static str,
    path: &str,
    body: &[u8],
) -> Result<Vec<(String, String)>, &'static str> {
    if !c.valid() || utc_ms == 0 {
        return Err("invalid credential material or timestamp");
    }
    let body_str = core::str::from_utf8(body).map_err(|_| "non-UTF8 request body")?;
    let mut headers = Vec::with_capacity(6);
    if !body.is_empty() {
        headers.push(("Content-Type".into(), "application/json".into()));
    }
    match c.venue {
        SpotVenue::Bybit => {
            let payload = if method == "GET" {
                path.split_once('?').map_or("", |(_, qs)| qs)
            } else {
                body_str
            };
            let ts = utc_ms.to_string();
            let preimage = format!("{ts}{}5000{payload}", c.key);
            let sig = hex(hmac::sign(c.hmac_key(), preimage.as_bytes()).as_ref());
            headers.extend([
                ("X-BAPI-API-KEY".into(), c.key.into()),
                ("X-BAPI-TIMESTAMP".into(), ts),
                ("X-BAPI-RECV-WINDOW".into(), "5000".into()),
                ("X-BAPI-SIGN".into(), sig),
            ]);
        }
        SpotVenue::Okx => {
            let pass = c.passphrase.ok_or("OKX passphrase is required")?;
            let mut exact_iso = [0u8; 24];
            format_okx_iso8601_into(utc_ms, &mut exact_iso)
                .map_err(|_| "timestamp out of range")?;
            let iso_ms =
                core::str::from_utf8(&exact_iso).map_err(|_| "invalid ISO8601 timestamp")?;
            let payload = if method == "GET" { "" } else { body_str };
            let preimage = format!("{iso_ms}{method}{path}{payload}");
            let sig = STANDARD.encode(hmac::sign(c.hmac_key(), preimage.as_bytes()).as_ref());
            headers.extend([
                ("OK-ACCESS-KEY".into(), c.key.into()),
                ("OK-ACCESS-SIGN".into(), sig),
                ("OK-ACCESS-TIMESTAMP".into(), iso_ms.into()),
                ("OK-ACCESS-PASSPHRASE".into(), pass.into()),
            ]);
            if env == Environment::Test {
                headers.push(("x-simulated-trading".into(), "1".into()));
            }
        }
    }
    Ok(headers)
}

/// Signs and encodes a cold-path HTTP/1.1 request (`GET` or `POST`) into wire bytes.
pub fn sign(
    c: &Credentials<'_>,
    env: Environment,
    utc_ms: u64,
    req: &Request,
) -> Result<Vec<u8>, &'static str> {
    let mut headers = Vec::with_capacity(7);
    headers.push(("Host".to_owned(), authority(c.venue, env).to_owned()));
    headers.extend(sign_headers(
        c, env, utc_ms, req.method, &req.path, &req.body,
    )?);
    let header_refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    tickwire_transport::http::encode_request(
        req.method,
        &req.path,
        &header_refs,
        &req.body,
        64 * 1024,
    )
    .map_err(|_| "cannot encode HTTP request")
}
