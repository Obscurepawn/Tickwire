//! Venue-agnostic [`VenueSession`] adapter (`UtaVenueSession`, `BybitVenueSession`, `OkxVenueSession`)
//! bridging `tickwire-spot-execution` directly onto `tickwire-runtime`'s generic
//! `OrderRoutingSystem<S>` and `OrsProcess<S>`.
//!
//! Covers all 6 Unified Trading Account products (`Spot`, `LinearPerpetual`, `InversePerpetual`,
//! `LinearFuture`, `InverseFuture`, `Option`), `TimeInForce::Rpi`, dual-transport order pools
//! (`WebSocketTrade` 16-in-flight `Shared` and `PooledHttps` `Exclusive`) with `product_mask = 0`,
//! optional Options/Block `RFQ` quoting (`RfqQuoteHook`), `execution.fast -> execution` fee
//! `amendment`, server-maintenance `notice` graceful switchover, and cold Exchange REST
//! `Account + Positions + OpenOrders` bootstrap via `tw1...` `SelfDescribingClientId`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tickwire_account::{
    InlineAscii, OrsCancelDispatch, OrsExchangeBootstrapSnapshot, OrsFeeSpec, OrsFillObservation,
    OrsModifyDispatch, OrsNewOrderRequest, OrsPlaceOutcome, OrsQueryOrigin,
};
use tickwire_account_gateway::AccountSpec;
use tickwire_ipc::{ReportBody, report_kind};
use tickwire_quota::rate_limiter::{IpKey, RateFlags, RateLimitId};
use tickwire_quota::shared::{InstrumentScope, OrsVenueProfile, VenueRateLimitRow};
use tickwire_runtime::{
    AccountSessionContext, EncodedOrderFrame, LoginReply, OwnedOrderQuerySnapshot,
    QueryStatusRequest, RfqQuoteCancelRequest, RfqQuoteRequest, SessionEndpoint,
    SessionMessageEffects, SessionRequestKind, SessionRestEndpoint, SessionUrlPoolSpec,
    SessionVenueEvent, StatusReplyOutcome, UrlRole, VenueSession,
};
use tickwire_spot_execution::network::OwnedCredentials;
use tickwire_spot_execution::rest_bootstrap::{
    StatusAskTarget, UtaAssetBinding, UtaBootstrapDecoder, UtaSymbolBinding,
    bybit_open_orders_path, bybit_positions_path, decode_status_ask_snapshot, okx_open_orders_path,
    okx_positions_path, sign_get_built_request, status_ask_order_path, wallet_balance_path,
};
use tickwire_spot_execution::rfq::{
    NoopRfqQuoteHook, RfqCancelQuotesRequest as SpotRfqCancelRequest, RfqQuoteHook, RfqQuoteLeg,
    RfqQuoteRequest as SpotRfqQuoteRequest,
};
use tickwire_spot_execution::warmup::{UtaHotPathWarmer, UtaWarmupConfig, UtaWarmupReport};
use tickwire_spot_execution::ws_envelope::WsLoginOutcome;
use tickwire_spot_execution::{
    BufWriter, BybitCategory, Environment, OkxInstType, OkxMarginMode, SpotIssueKind, SpotVenue,
    UtaCancelWireInput, UtaCommandReply, UtaExecRow, UtaFrameEvent, UtaModifyWireInput,
    UtaNewOrderWireInput, UtaOrderParams, UtaOrderRow, UtaReplyOp, UtaTransportMode,
    check_bybit_auth_reply, check_okx_login_reply, classify_issue, encode_bybit_auth_into,
    encode_bybit_cancel_arg, encode_bybit_modify_arg, encode_bybit_new_order_arg,
    encode_bybit_private_subscribe_step_into, encode_bybit_ws_trade_envelope_into,
    encode_okx_cancel_arg, encode_okx_login_into, encode_okx_modify_arg, encode_okx_new_order_arg,
    encode_okx_subscribe_step_into, encode_okx_ws_trade_envelope_into,
    encode_signed_https_post_into, encode_ws_client_frame_into, instrument_rate_scope,
    parse_positive_step_i64, parse_signed_scaled_i128, scan_bybit_frame, scan_okx_frame,
    trade_id_inline,
};
use tickwire_transport::json_scan::{parse_i64_ascii, parse_u64_ascii};
use tickwire_transport::latency_tracer::{LockDomain, LockId, ThreadTag, TimedSpinLock};
use tickwire_transport::links::{LinkKind, RequestRef, SendOutcome};
use tickwire_transport::rest_querier::{
    BuiltRequest, DialSecurity, RestReply, RestStatus, RestTask,
};
use tickwire_transport::{mono_now_ns, rustls, ws};
use tickwire_venue::{
    ClientIdDomain, DecodedClientId, Liquidity, OrderStatus, Product, RefusalReason,
    SelfDescribingClientId, Side, TerminalReason, TimeInForce,
};

const INFLIGHT_TAG_RING_CAP: usize = 128;
const VENUE_ID_CACHE_CAP: usize = 256;

/// Endpoint configuration for an order or private WebSocket URL pool.
#[derive(Clone, Debug)]
pub struct UtaEndpointSpec {
    pub host: String,
    pub port: u16,
    pub path: String,
    pub security: DialSecurity,
    pub initial_peers: Vec<SocketAddr>,
}

impl UtaEndpointSpec {
    /// Socketpair / manual-link mode (`host = ""`, `port = 0`) where tests install `LinkOps` directly.
    pub fn manual(path: impl Into<String>) -> Self {
        Self {
            host: String::new(),
            port: 0,
            path: path.into(),
            security: DialSecurity::Plain,
            initial_peers: Vec::new(),
        }
    }

    /// Loopback or live TLS endpoint dialed automatically by `Dialer` on `housekeeping_cpus`.
    pub fn tls(
        host: impl Into<String>,
        port: u16,
        path: impl Into<String>,
        tls: Arc<rustls::ClientConfig>,
        initial_peers: Vec<SocketAddr>,
    ) -> Result<Self, String> {
        let host_str = host.into();
        let server_name = rustls::pki_types::ServerName::try_from(host_str.clone())
            .map_err(|e| format!("invalid TLS ServerName '{host_str}': {e}"))?;
        Ok(Self {
            host: host_str,
            port,
            path: path.into(),
            security: DialSecurity::Tls {
                server_name,
                config: tls,
            },
            initial_peers,
        })
    }

    /// Plaintext loopback endpoint for deterministic non-TLS socket tests.
    pub fn plain(
        host: impl Into<String>,
        port: u16,
        path: impl Into<String>,
        initial_peers: Vec<SocketAddr>,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            path: path.into(),
            security: DialSecurity::Plain,
            initial_peers,
        }
    }
}

/// Startup configuration for [`UtaVenueSession`].
pub struct UtaVenueSessionConfig {
    pub venue: SpotVenue,
    pub environment: Environment,
    pub exchange_uid: u64,
    pub risk_domain_id: [u8; 16],
    pub credentials: OwnedCredentials,
    pub transport_mode: UtaTransportMode,
    pub order_endpoint: UtaEndpointSpec,
    pub stream_endpoint: Option<UtaEndpointSpec>,
    pub rest_endpoint: Option<SessionRestEndpoint>,
    pub symbols: Vec<UtaSymbolBinding>,
    pub assets: Vec<UtaAssetBinding>,
    pub ip_order_weight: u32,
    pub status_query_delay_ms: u64,
    pub okx_ws_auto_subscribe_private: bool,
    pub rfq_hook: Option<Box<dyn RfqQuoteHook>>,
    pub clock_utc_ms: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
    /// Optional cold-startup bootstrap snapshot override (used when `rest_endpoint` is `None`).
    pub bootstrap_override: Option<OrsExchangeBootstrapSnapshot>,
}

impl UtaVenueSessionConfig {
    pub fn bybit(
        environment: Environment,
        exchange_uid: u64,
        risk_domain_id: [u8; 16],
        credentials: OwnedCredentials,
        transport_mode: UtaTransportMode,
    ) -> Self {
        let order_path = match transport_mode {
            UtaTransportMode::WebSocketTrade => "/v5/trade",
            UtaTransportMode::PooledHttps => "/v5/order/create",
        };
        Self {
            venue: SpotVenue::Bybit,
            environment,
            exchange_uid,
            risk_domain_id,
            credentials,
            transport_mode,
            order_endpoint: UtaEndpointSpec::manual(order_path),
            stream_endpoint: None,
            rest_endpoint: None,
            symbols: Vec::new(),
            assets: Vec::new(),
            ip_order_weight: 1,
            status_query_delay_ms: 1_000,
            okx_ws_auto_subscribe_private: false,
            rfq_hook: None,
            clock_utc_ms: None,
            bootstrap_override: None,
        }
    }

    pub fn okx(
        environment: Environment,
        exchange_uid: u64,
        risk_domain_id: [u8; 16],
        credentials: OwnedCredentials,
        transport_mode: UtaTransportMode,
    ) -> Self {
        let order_path = match transport_mode {
            UtaTransportMode::WebSocketTrade => "/ws/v5/private",
            UtaTransportMode::PooledHttps => "/api/v5/trade/order",
        };
        Self {
            venue: SpotVenue::Okx,
            environment,
            exchange_uid,
            risk_domain_id,
            credentials,
            transport_mode,
            order_endpoint: UtaEndpointSpec::manual(order_path),
            stream_endpoint: None,
            rest_endpoint: None,
            symbols: Vec::new(),
            assets: Vec::new(),
            ip_order_weight: 1,
            status_query_delay_ms: 1_000,
            okx_ws_auto_subscribe_private: true,
            rfq_hook: None,
            clock_utc_ms: None,
            bootstrap_override: None,
        }
    }
}

#[derive(Clone, Copy)]
struct InflightOpTag {
    ors_order_id: u64,
    reply_id: u64,
    symbol_id: u32,
    op: UtaReplyOp,
    price_ticks: i64,
    qty_lots: i64,
    side: Option<Side>,
    tif: TimeInForce,
    post_only: bool,
}

impl Default for InflightOpTag {
    fn default() -> Self {
        Self {
            ors_order_id: 0,
            reply_id: 0,
            symbol_id: 0,
            op: UtaReplyOp::Unknown,
            price_ticks: 0,
            qty_lots: 0,
            side: None,
            tif: TimeInForce::Gtc,
            post_only: false,
        }
    }
}

struct InflightTagRing {
    entries: [InflightOpTag; INFLIGHT_TAG_RING_CAP],
    cursor: usize,
}

impl Default for InflightTagRing {
    fn default() -> Self {
        Self {
            entries: [InflightOpTag::default(); INFLIGHT_TAG_RING_CAP],
            cursor: 0,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct VenueIdCacheEntry {
    ors_order_id: u64,
    symbol_id: u32,
    venue_order_id: InlineAscii<64>,
}

struct VenueIdCache {
    entries: [VenueIdCacheEntry; VENUE_ID_CACHE_CAP],
    cursor: usize,
}

impl Default for VenueIdCache {
    fn default() -> Self {
        Self {
            entries: [VenueIdCacheEntry::default(); VENUE_ID_CACHE_CAP],
            cursor: 0,
        }
    }
}

/// Stateless Unified Trading Account [`VenueSession`] implementation for Bybit V5 and OKX V5.
///
/// Shares `&self` concurrently across `REQ`, `RESP`, `Dialer`, and `AsyncRestQuerier` with zero
/// coarse session mutexes, zero heap allocations on hot paths, and leaf-only fixed-size scalar
/// caches (`Order` and `Connection` spinlocks never overlap).
pub struct UtaVenueSession {
    venue: SpotVenue,
    environment: Environment,
    exchange_uid: u64,
    risk_domain_id: [u8; 16],
    credentials: OwnedCredentials,
    transport_mode: UtaTransportMode,
    order_endpoint: UtaEndpointSpec,
    stream_endpoint: Option<UtaEndpointSpec>,
    rest_endpoint: Option<SessionRestEndpoint>,
    symbols: Arc<[UtaSymbolBinding]>,
    assets: Arc<[UtaAssetBinding]>,
    ip_order_weight: u32,
    status_query_delay_ms: u64,
    okx_ws_auto_subscribe_private: bool,
    rfq_hook: Box<dyn RfqQuoteHook>,
    clock_utc_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    next_reply_id: AtomicU64,
    inflight_tags: TimedSpinLock<InflightTagRing>,
    venue_ids: TimedSpinLock<VenueIdCache>,
    bootstrap_override: Option<OrsExchangeBootstrapSnapshot>,
}

/// Type alias highlighting Bybit V5 Unified Trading Account configuration on [`UtaVenueSession`].
pub type BybitVenueSession = UtaVenueSession;

/// Type alias highlighting OKX V5 Unified Trading Account configuration on [`UtaVenueSession`].
pub type OkxVenueSession = UtaVenueSession;

fn system_utc_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(1_700_000_000_000)
}

impl UtaVenueSession {
    pub fn new(cfg: UtaVenueSessionConfig) -> Result<Self, String> {
        if !cfg.credentials.credentials().valid() {
            return Err(String::from("invalid UTA credentials"));
        }
        if cfg.credentials.credentials().venue() != cfg.venue {
            return Err(String::from("credentials venue mismatch"));
        }
        let clock_utc_ms = cfg.clock_utc_ms.unwrap_or_else(|| Arc::new(system_utc_ms));
        let rfq_hook = cfg.rfq_hook.unwrap_or_else(|| Box::new(NoopRfqQuoteHook));
        Ok(Self {
            venue: cfg.venue,
            environment: cfg.environment,
            exchange_uid: cfg.exchange_uid,
            risk_domain_id: cfg.risk_domain_id,
            credentials: cfg.credentials,
            transport_mode: cfg.transport_mode,
            order_endpoint: cfg.order_endpoint,
            stream_endpoint: cfg.stream_endpoint,
            rest_endpoint: cfg.rest_endpoint,
            symbols: cfg.symbols.into(),
            assets: cfg.assets.into(),
            ip_order_weight: cfg.ip_order_weight.max(1),
            status_query_delay_ms: cfg.status_query_delay_ms,
            okx_ws_auto_subscribe_private: cfg.okx_ws_auto_subscribe_private,
            rfq_hook,
            clock_utc_ms,
            next_reply_id: AtomicU64::new(1),
            inflight_tags: TimedSpinLock::new(
                InflightTagRing::default(),
                LockId::new(LockDomain::GuardedValue, 0x7101),
            ),
            venue_ids: TimedSpinLock::new(
                VenueIdCache::default(),
                LockId::new(LockDomain::GuardedValue, 0x7102),
            ),
            bootstrap_override: cfg.bootstrap_override,
        })
    }

    #[inline]
    pub const fn venue(&self) -> SpotVenue {
        self.venue
    }

    #[inline]
    pub const fn environment(&self) -> Environment {
        self.environment
    }

    #[inline]
    pub const fn transport_mode(&self) -> UtaTransportMode {
        self.transport_mode
    }

    #[inline]
    pub fn symbols(&self) -> &[UtaSymbolBinding] {
        &self.symbols
    }

    #[inline]
    pub fn assets(&self) -> &[UtaAssetBinding] {
        &self.assets
    }

    /// Runs `iterations` side-effect-free [`UtaHotPathWarmer`] passes.
    pub fn warm_hot_path(&self, iterations: u32) -> Result<UtaWarmupReport, String> {
        let mut warmer = UtaHotPathWarmer::new(UtaWarmupConfig::new(self.venue, self.environment))?;
        let mut last = None;
        let base_ns = mono_now_ns();
        let base_ms = (self.clock_utc_ms)();
        for idx in 0..iterations.max(1) {
            let rep = warmer.warm_once(
                base_ns.saturating_add(u64::from(idx) * 1_000),
                base_ms.saturating_add(u64::from(idx)),
            )?;
            last = Some(rep);
        }
        last.ok_or_else(|| String::from("warmup produced no report"))
    }

    #[inline]
    fn alloc_reply_id(&self) -> u64 {
        loop {
            let id = self.next_reply_id.fetch_add(1, Ordering::Relaxed);
            if id != 0 {
                return id;
            }
        }
    }

    #[inline]
    fn find_symbol_by_id(&self, symbol_id: u32) -> Option<UtaSymbolBinding> {
        self.symbols
            .iter()
            .find(|s| s.symbol_id == symbol_id)
            .copied()
    }

    #[inline]
    fn find_symbol_by_name(&self, name: &str) -> Option<UtaSymbolBinding> {
        self.symbols
            .iter()
            .find(|s| s.symbol.as_str() == name)
            .copied()
    }

    #[inline]
    fn find_asset_by_currency(&self, ccy: &str) -> Option<UtaAssetBinding> {
        self.assets
            .iter()
            .find(|a| a.currency.as_str().eq_ignore_ascii_case(ccy))
            .copied()
    }

    #[inline]
    fn record_inflight_tag(&self, tag: InflightOpTag) {
        let mut ring = self.inflight_tags.lock(ThreadTag::REQ);
        let idx = ring.cursor % INFLIGHT_TAG_RING_CAP;
        ring.cursor = ring.cursor.wrapping_add(1);
        ring.entries[idx] = tag;
    }

    #[inline]
    fn lookup_inflight_tag(
        &self,
        ors_order_id_hint: Option<u64>,
        reply_id_hint: Option<u64>,
    ) -> Option<InflightOpTag> {
        let ring = self.inflight_tags.lock(ThreadTag::RESP);
        if let Some(rid) = reply_id_hint.filter(|&r| r != 0) {
            for idx in 0..INFLIGHT_TAG_RING_CAP {
                let pos = ring.cursor.wrapping_add(INFLIGHT_TAG_RING_CAP - 1 - idx)
                    % INFLIGHT_TAG_RING_CAP;
                let entry = ring.entries[pos];
                if entry.ors_order_id != 0 && entry.reply_id == rid {
                    return Some(entry);
                }
            }
        }
        if let Some(oid) = ors_order_id_hint.filter(|&o| o != 0) {
            for idx in 0..INFLIGHT_TAG_RING_CAP {
                let pos = ring.cursor.wrapping_add(INFLIGHT_TAG_RING_CAP - 1 - idx)
                    % INFLIGHT_TAG_RING_CAP;
                let entry = ring.entries[pos];
                if entry.ors_order_id == oid {
                    return Some(entry);
                }
            }
        }
        None
    }

    #[inline]
    fn remember_venue_id(&self, venue_order_id: &str, ors_order_id: u64, symbol_id: u32) {
        if venue_order_id.is_empty() || ors_order_id == 0 {
            return;
        }
        let Some(inline) = InlineAscii::<64>::try_from_str(venue_order_id) else {
            return;
        };
        let mut cache = self.venue_ids.lock(ThreadTag::RESP);
        for entry in cache.entries.iter_mut() {
            if entry.ors_order_id == ors_order_id || entry.venue_order_id == inline {
                entry.ors_order_id = ors_order_id;
                if symbol_id != 0 {
                    entry.symbol_id = symbol_id;
                }
                entry.venue_order_id = inline;
                return;
            }
        }
        let idx = cache.cursor % VENUE_ID_CACHE_CAP;
        cache.cursor = cache.cursor.wrapping_add(1);
        cache.entries[idx] = VenueIdCacheEntry {
            ors_order_id,
            symbol_id,
            venue_order_id: inline,
        };
    }

    #[inline]
    fn lookup_by_venue_id(&self, venue_order_id: &str) -> Option<(u64, u32)> {
        if venue_order_id.is_empty() {
            return None;
        }
        let inline = InlineAscii::<64>::try_from_str(venue_order_id)?;
        let cache = self.venue_ids.lock(ThreadTag::RESP);
        cache
            .entries
            .iter()
            .find(|e| e.ors_order_id != 0 && e.venue_order_id == inline)
            .map(|e| (e.ors_order_id, e.symbol_id))
    }

    #[inline]
    const fn client_id_domain(&self, product: Product) -> ClientIdDomain {
        ClientIdDomain {
            risk_domain_id: self.risk_domain_id,
            venue: self.venue.venue_id(),
            environment: self.environment,
            account: self.exchange_uid,
            product,
        }
    }

    #[inline]
    fn decode_client_id_order_id(
        &self,
        product_hint: Option<Product>,
        client_id: &str,
    ) -> Option<u64> {
        if client_id.is_empty() {
            return None;
        }
        if let Some(prod) = product_hint {
            let domain = self.client_id_domain(prod);
            if let DecodedClientId::Managed(sd) =
                SelfDescribingClientId::decode_str(client_id, &domain)
                && let Some(strat_id) = sd.ors_strat_id()
            {
                return Some((sd.intent_id << 8) | u64::from(strat_id));
            }
        } else {
            for prod in [
                Product::Spot,
                Product::LinearPerpetual,
                Product::InversePerpetual,
                Product::LinearFuture,
                Product::InverseFuture,
                Product::Option,
            ] {
                let domain = self.client_id_domain(prod);
                if let DecodedClientId::Managed(sd) =
                    SelfDescribingClientId::decode_str(client_id, &domain)
                    && let Some(strat_id) = sd.ors_strat_id()
                {
                    return Some((sd.intent_id << 8) | u64::from(strat_id));
                }
            }
        }
        None
    }

    #[inline]
    fn resolve_order_and_symbol(
        &self,
        hint: Option<RequestRef>,
        reply_id: Option<u64>,
        raw_symbol: &str,
        client_id: &str,
        venue_order_id: &str,
    ) -> Option<(u64, Option<UtaSymbolBinding>, Option<InflightOpTag>)> {
        let binding_from_name = (!raw_symbol.is_empty())
            .then(|| self.find_symbol_by_name(raw_symbol))
            .flatten();
        let prod_hint = binding_from_name.map(|b| b.spec.product);

        let hint_oid = hint.map(|h| h.order_key).filter(|&k| k != 0);
        let tag = self.lookup_inflight_tag(hint_oid, reply_id);

        let oid = hint_oid
            .or_else(|| tag.map(|t| t.ors_order_id).filter(|&k| k != 0))
            .or_else(|| self.decode_client_id_order_id(prod_hint, client_id))
            .or_else(|| self.lookup_by_venue_id(venue_order_id).map(|(id, _)| id))?;

        let tag_final = tag.or_else(|| self.lookup_inflight_tag(Some(oid), None));
        let binding = binding_from_name
            .or_else(|| {
                let sid = tag_final.map(|t| t.symbol_id).filter(|&s| s != 0)?;
                self.find_symbol_by_id(sid)
            })
            .or_else(|| {
                let (_, sid) = self.lookup_by_venue_id(venue_order_id)?;
                self.find_symbol_by_id(sid)
            });

        Some((oid, binding, tag_final))
    }

    fn classify_reply_reason(
        &self,
        code: &str,
        msg: &str,
        post_only: bool,
    ) -> Option<TerminalReason> {
        if is_rate_limit(self.venue, code) {
            return Some(TerminalReason::RateLimited);
        }
        match (self.venue, code) {
            (SpotVenue::Bybit, "110007" | "170131") | (SpotVenue::Okx, "51008" | "51119") => {
                Some(TerminalReason::InsufficientFunds)
            }
            (SpotVenue::Bybit, "110079" | "170139" | "140001") | (SpotVenue::Okx, "51019") => {
                Some(TerminalReason::MKT_REJECT_POST_ONLY)
            }
            (SpotVenue::Bybit, "110017") | (SpotVenue::Okx, "51137") => {
                Some(TerminalReason::MKT_REDUCE_ONLY_CANCELED)
            }
            _ => {
                if post_only
                    && msg
                        .as_bytes()
                        .windows(8)
                        .any(|w| w.eq_ignore_ascii_case(b"postonly"))
                {
                    Some(TerminalReason::MKT_REJECT_POST_ONLY)
                } else {
                    None
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_command_reply(
        &self,
        account_ref: u8,
        hint: Option<RequestRef>,
        rep: UtaCommandReply<'_>,
        now_mono_ns: u64,
        effects: &mut SessionMessageEffects,
    ) {
        let Some((oid, binding_opt, tag_opt)) =
            self.resolve_order_and_symbol(hint, rep.reply_id, "", rep.client_id, rep.order_id)
        else {
            return;
        };

        let sym_id = binding_opt
            .map(|b| b.symbol_id)
            .or_else(|| tag_opt.map(|t| t.symbol_id))
            .unwrap_or(0);
        if !rep.order_id.is_empty() {
            self.remember_venue_id(rep.order_id, oid, sym_id);
        }

        let op = if rep.op != UtaReplyOp::Unknown {
            rep.op
        } else {
            tag_opt.map(|t| t.op).unwrap_or(UtaReplyOp::Create)
        };

        if self.rfq_hook.has_quote(oid) {
            if rep.is_ok() {
                if op == UtaReplyOp::Cancel {
                    let _ = self.rfq_hook.on_quote_terminal(oid);
                } else {
                    let vid = (!rep.order_id.is_empty()).then_some(rep.order_id);
                    let _ = self.rfq_hook.on_quote_ack(oid, vid);
                }
            } else {
                let _ = self.rfq_hook.on_quote_terminal(oid);
            }
            let _ = account_ref;
            return;
        }

        if rep.is_ok() {
            let vid_inline =
                (!rep.order_id.is_empty()).then(|| InlineAscii::<64>::lossy_ascii(rep.order_id));
            match op {
                UtaReplyOp::Create | UtaReplyOp::Unknown => {
                    effects.events.push(SessionVenueEvent::FrontAck {
                        ors_order_id: oid,
                        venue_order_id: vid_inline,
                        received_mono_ns: now_mono_ns,
                    });
                    let ack_px = binding_opt.and_then(|b| {
                        (!rep.clamped_price.is_empty()).then(|| {
                            parse_positive_step_i64(
                                rep.clamped_price.as_bytes(),
                                b.spec.price_scale,
                                b.spec.price_tick_atoms,
                            )
                        })?
                    });
                    let ack_qty = binding_opt.and_then(|b| {
                        (!rep.clamped_qty.is_empty()).then(|| {
                            parse_positive_step_i64(
                                rep.clamped_qty.as_bytes(),
                                b.spec.qty_scale,
                                b.spec.qty_step_atoms,
                            )
                        })?
                    });
                    if self.transport_mode == UtaTransportMode::PooledHttps
                        || ack_px.is_some()
                        || ack_qty.is_some()
                    {
                        effects.events.push(SessionVenueEvent::Ack {
                            ors_order_id: oid,
                            venue_order_id: vid_inline,
                            ack_price_ticks: ack_px,
                            ack_qty_lots: ack_qty,
                            received_mono_ns: now_mono_ns,
                        });
                    }
                }
                UtaReplyOp::Amend => {
                    let px = binding_opt
                        .and_then(|b| {
                            (!rep.clamped_price.is_empty()).then(|| {
                                parse_positive_step_i64(
                                    rep.clamped_price.as_bytes(),
                                    b.spec.price_scale,
                                    b.spec.price_tick_atoms,
                                )
                            })?
                        })
                        .or_else(|| tag_opt.map(|t| t.price_ticks))
                        .unwrap_or(0);
                    let qty = binding_opt
                        .and_then(|b| {
                            (!rep.clamped_qty.is_empty()).then(|| {
                                parse_positive_step_i64(
                                    rep.clamped_qty.as_bytes(),
                                    b.spec.qty_scale,
                                    b.spec.qty_step_atoms,
                                )
                            })?
                        })
                        .or_else(|| tag_opt.map(|t| t.qty_lots))
                        .unwrap_or(0);
                    if px > 0 && qty > 0 {
                        effects.events.push(SessionVenueEvent::Modified {
                            ors_order_id: oid,
                            price_ticks: px,
                            qty_lots: qty,
                            venue_order_id: vid_inline,
                            received_mono_ns: now_mono_ns,
                        });
                    }
                }
                UtaReplyOp::Cancel => {
                    if self.transport_mode == UtaTransportMode::PooledHttps {
                        effects.events.push(SessionVenueEvent::Canceled {
                            ors_order_id: oid,
                            cum_qty_lots: None,
                            error_code: None,
                            status_or_msg: InlineAscii::lossy_ascii("CANCELED"),
                            stp_mode_or_reason: None,
                            explicit_reason: None,
                            received_mono_ns: now_mono_ns,
                        });
                    }
                }
            }
            return;
        }

        let code_str = rep.effective_error_code();
        let code_i64 = parse_i64_ascii(code_str.as_bytes());
        let issue = classify_issue(self.venue, code_str);
        let post_only = tag_opt.is_some_and(|t| t.post_only);
        let explicit = if issue == SpotIssueKind::NotFound {
            Some(TerminalReason::MKT_REJECT_NO_SUCH_ORDER)
        } else {
            self.classify_reply_reason(code_str, rep.message, post_only)
        };
        let msg_ascii = InlineAscii::<64>::lossy_ascii(if rep.message.is_empty() {
            code_str
        } else {
            rep.message
        });

        match op {
            UtaReplyOp::Create | UtaReplyOp::Unknown => {
                if issue == SpotIssueKind::Ambiguous {
                    effects.query_status.push(QueryStatusRequest {
                        ors_order_id: oid,
                        origin: OrsQueryOrigin::Place,
                    });
                } else {
                    effects.events.push(SessionVenueEvent::Rejected {
                        ors_order_id: oid,
                        error_code: code_i64,
                        status_or_msg: msg_ascii,
                        explicit_reason: explicit.or(Some(TerminalReason::MKT_REJECT)),
                        received_mono_ns: now_mono_ns,
                    });
                }
            }
            UtaReplyOp::Amend => {
                let reason = explicit.unwrap_or(match issue {
                    SpotIssueKind::NotFound => TerminalReason::MKT_REJECT_NO_SUCH_ORDER,
                    SpotIssueKind::Ambiguous => TerminalReason::MKT_DELAYED,
                    _ => TerminalReason::MKT_REJECT,
                });
                effects.events.push(SessionVenueEvent::ModifyRejected {
                    ors_order_id: oid,
                    reason,
                    received_mono_ns: now_mono_ns,
                });
                if issue == SpotIssueKind::Ambiguous {
                    effects.query_status.push(QueryStatusRequest {
                        ors_order_id: oid,
                        origin: OrsQueryOrigin::Modify,
                    });
                }
            }
            UtaReplyOp::Cancel => {
                let reason = explicit.unwrap_or(match issue {
                    SpotIssueKind::NotFound => TerminalReason::MKT_REJECT_NO_SUCH_ORDER,
                    SpotIssueKind::Ambiguous => TerminalReason::MKT_DELAYED,
                    _ => TerminalReason::MKT_REJECT,
                });
                let desk_code = if issue == SpotIssueKind::Ambiguous {
                    Some(-1007)
                } else {
                    code_i64
                };
                effects.events.push(SessionVenueEvent::CancelRejected {
                    ors_order_id: oid,
                    error_code: desk_code,
                    reason,
                    received_mono_ns: now_mono_ns,
                });
            }
        }
    }

    fn handle_order_row(
        &self,
        account_ref: u8,
        row: &UtaOrderRow<'_>,
        now_mono_ns: u64,
        effects: &mut SessionMessageEffects,
    ) {
        let Some((oid, binding_opt, tag_opt)) =
            self.resolve_order_and_symbol(None, None, row.symbol, row.client_id, row.order_id)
        else {
            return;
        };
        let Some(binding) = binding_opt else {
            return;
        };
        if !row.order_id.is_empty() {
            self.remember_venue_id(row.order_id, oid, binding.symbol_id);
        }

        let spec = binding.spec;
        let px_ticks = (!row.price_raw.is_empty())
            .then(|| {
                parse_positive_step_i64(
                    row.price_raw.as_bytes(),
                    spec.price_scale,
                    spec.price_tick_atoms,
                )
            })
            .flatten();
        let qty_lots = (!row.qty_raw.is_empty())
            .then(|| {
                parse_positive_step_i64(row.qty_raw.as_bytes(), spec.qty_scale, spec.qty_step_atoms)
            })
            .flatten();
        let cum_lots = (!row.cum_exec_qty_raw.is_empty())
            .then(|| {
                parse_positive_step_i64(
                    row.cum_exec_qty_raw.as_bytes(),
                    spec.qty_scale,
                    spec.qty_step_atoms,
                )
            })
            .flatten();

        let vid_inline =
            (!row.order_id.is_empty()).then(|| InlineAscii::<64>::lossy_ascii(row.order_id));

        let mut emitted_inline_fill = false;
        if self.venue == SpotVenue::Okx
            && !row.inline_trade_id.is_empty()
            && row.inline_trade_id != "0"
            && let Some((tid_inline, exec_ref)) =
                trade_id_inline(SpotVenue::Okx, row.inline_trade_id)
            && let (Some(fill_px), Some(fill_qty)) = (
                parse_positive_step_i64(
                    row.inline_fill_px.as_bytes(),
                    spec.price_scale,
                    spec.price_tick_atoms,
                ),
                parse_positive_step_i64(
                    row.inline_fill_sz.as_bytes(),
                    spec.qty_scale,
                    spec.qty_step_atoms,
                ),
            )
            && fill_px > 0
            && fill_qty > 0
        {
            emitted_inline_fill = true;
            let side = row
                .normalized_side(SpotVenue::Okx)
                .or_else(|| tag_opt.and_then(|t| t.side))
                .unwrap_or(Side::Buy);
            let liquidity = match row.inline_exec_type {
                "M" => Liquidity::Maker,
                "T" => Liquidity::Taker,
                _ => Liquidity::Unknown,
            };
            let fee = self.parse_fee_spec(
                Some(row.inline_fill_fee_ccy),
                (!row.inline_fill_fee.is_empty()).then_some(row.inline_fill_fee),
                &spec,
                side,
            );
            effects.events.push(SessionVenueEvent::Ack {
                ors_order_id: oid,
                venue_order_id: vid_inline,
                ack_price_ticks: px_ticks,
                ack_qty_lots: qty_lots,
                received_mono_ns: now_mono_ns,
            });
            effects
                .events
                .push(SessionVenueEvent::Fill(OrsFillObservation {
                    ors_order_id: oid,
                    account_ref,
                    symbol_id: binding.symbol_id,
                    side,
                    liquidity,
                    exec_ref,
                    trade_id: tid_inline,
                    qty_lots: fill_qty,
                    price_ticks: fill_px,
                    fee,
                    received_mono_ns: now_mono_ns,
                }));
        }

        let Some(status) = row.normalized_status(self.venue) else {
            return;
        };
        let order_post_only = tag_opt.is_some_and(|t| t.post_only);
        let order_tif = tag_opt.map(|t| t.tif).unwrap_or(TimeInForce::Gtc);

        match status {
            OrderStatus::New | OrderStatus::PartiallyFilled | OrderStatus::Filled => {
                effects.events.push(SessionVenueEvent::Ack {
                    ors_order_id: oid,
                    venue_order_id: vid_inline,
                    ack_price_ticks: px_ticks,
                    ack_qty_lots: qty_lots,
                    received_mono_ns: now_mono_ns,
                });
                if !emitted_inline_fill
                    && let Some(cum) = cum_lots
                    && cum > 0
                {
                    let proj_px = px_ticks
                        .or_else(|| tag_opt.map(|t| t.price_ticks).filter(|&p| p > 0))
                        .unwrap_or(1);
                    effects.events.push(SessionVenueEvent::ProjectedFill {
                        ors_order_id: oid,
                        cum_qty_lots: cum,
                        price_ticks: proj_px,
                        received_mono_ns: now_mono_ns,
                    });
                }
            }
            OrderStatus::Canceled => {
                let explicit =
                    row.classify_explicit_terminal_reason(self.venue, order_post_only, order_tif);
                let stp_opt = (!row.smp_type.is_empty()
                    && !row.smp_type.eq_ignore_ascii_case("None"))
                .then(|| InlineAscii::<32>::lossy_ascii(row.smp_type));
                effects.events.push(SessionVenueEvent::Canceled {
                    ors_order_id: oid,
                    cum_qty_lots: cum_lots,
                    error_code: None,
                    status_or_msg: InlineAscii::lossy_ascii(if row.reject_reason.is_empty() {
                        row.status_raw
                    } else {
                        row.reject_reason
                    }),
                    stp_mode_or_reason: stp_opt,
                    explicit_reason: explicit,
                    received_mono_ns: now_mono_ns,
                });
            }
            OrderStatus::Rejected => {
                let explicit = row
                    .classify_explicit_terminal_reason(self.venue, order_post_only, order_tif)
                    .or(Some(TerminalReason::MKT_REJECT));
                effects.events.push(SessionVenueEvent::Rejected {
                    ors_order_id: oid,
                    error_code: parse_i64_ascii(row.reject_reason.as_bytes()),
                    status_or_msg: InlineAscii::lossy_ascii(if row.reject_reason.is_empty() {
                        row.status_raw
                    } else {
                        row.reject_reason
                    }),
                    explicit_reason: explicit,
                    received_mono_ns: now_mono_ns,
                });
            }
            _ => {}
        }
    }

    fn handle_exec_row(
        &self,
        account_ref: u8,
        row: &UtaExecRow<'_>,
        now_mono_ns: u64,
        effects: &mut SessionMessageEffects,
    ) {
        if !row.exec_type.is_empty() && !row.exec_type.eq_ignore_ascii_case("Trade") {
            return;
        }
        let Some((oid, binding_opt, tag_opt)) =
            self.resolve_order_and_symbol(None, None, row.symbol, row.client_id, row.order_id)
        else {
            return;
        };
        let Some(binding) = binding_opt else {
            return;
        };
        if !row.order_id.is_empty() {
            self.remember_venue_id(row.order_id, oid, binding.symbol_id);
        }
        let Some((tid_inline, exec_ref)) = trade_id_inline(self.venue, row.exec_id) else {
            return;
        };
        let spec = binding.spec;
        let (Some(px_ticks), Some(qty_lots)) = (
            parse_positive_step_i64(
                row.exec_price_raw.as_bytes(),
                spec.price_scale,
                spec.price_tick_atoms,
            ),
            parse_positive_step_i64(
                row.exec_qty_raw.as_bytes(),
                spec.qty_scale,
                spec.qty_step_atoms,
            ),
        ) else {
            return;
        };
        if px_ticks <= 0 || qty_lots <= 0 {
            return;
        }
        let side = match (self.venue, row.side_raw) {
            (SpotVenue::Bybit, "Buy") | (SpotVenue::Okx, "buy") => Side::Buy,
            (SpotVenue::Bybit, "Sell") | (SpotVenue::Okx, "sell") => Side::Sell,
            _ => tag_opt.and_then(|t| t.side).unwrap_or(Side::Buy),
        };
        let fee = if row.is_fast {
            None
        } else {
            self.parse_fee_spec(row.fee_currency, row.exec_fee_raw, &spec, side)
        };
        let vid_inline =
            (!row.order_id.is_empty()).then(|| InlineAscii::<64>::lossy_ascii(row.order_id));

        effects.events.push(SessionVenueEvent::Ack {
            ors_order_id: oid,
            venue_order_id: vid_inline,
            ack_price_ticks: None,
            ack_qty_lots: None,
            received_mono_ns: now_mono_ns,
        });
        effects
            .events
            .push(SessionVenueEvent::Fill(OrsFillObservation {
                ors_order_id: oid,
                account_ref,
                symbol_id: binding.symbol_id,
                side,
                liquidity: row.liquidity,
                exec_ref,
                trade_id: tid_inline,
                qty_lots,
                price_ticks: px_ticks,
                fee,
                received_mono_ns: now_mono_ns,
            }));
    }

    fn parse_fee_spec(
        &self,
        fee_ccy: Option<&str>,
        fee_raw: Option<&str>,
        spec: &tickwire_account::OrsSymbolSpec,
        side: Side,
    ) -> Option<OrsFeeSpec> {
        let raw_str = fee_raw.filter(|s| !s.is_empty())?;
        let (asset_key, scale) = fee_ccy
            .filter(|s| !s.is_empty())
            .and_then(|ccy| self.find_asset_by_currency(ccy))
            .map(|a| (a.key, a.scale))
            .unwrap_or_else(|| spec.hold_asset_for_side(side));
        let raw_atoms = parse_signed_scaled_i128(raw_str.as_bytes(), scale)?;
        let signed_atoms = match self.venue {
            SpotVenue::Bybit => raw_atoms.checked_neg()?,
            SpotVenue::Okx => raw_atoms,
        };
        Some(OrsFeeSpec {
            asset_key,
            scale,
            signed_atoms,
        })
    }

    fn fetch_cold_rest_get(&self, ep: &SessionRestEndpoint, path: &str) -> Result<Vec<u8>, String> {
        let addr = if let Some(&peer) = ep.initial_peers.first() {
            peer
        } else {
            use std::net::ToSocketAddrs;
            (ep.host.as_str(), ep.port)
                .to_socket_addrs()
                .map_err(|e| format!("DNS lookup failed for {}:{}: {e}", ep.host, ep.port))?
                .next()
                .ok_or_else(|| format!("no socket address for {}:{}", ep.host, ep.port))?
        };

        let tcp = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
            .map_err(|e| format!("cold REST connect {addr} failed: {e}"))?;
        tcp.set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| format!("set_read_timeout failed: {e}"))?;
        tcp.set_write_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| format!("set_write_timeout failed: {e}"))?;

        let creds = self.credentials.credentials();
        let built = sign_get_built_request(
            &creds,
            self.environment,
            (self.clock_utc_ms)(),
            path.to_owned(),
        )
        .map_err(|e| format!("sign_get_built_request({path}) failed: {e}"))?;

        let mut req_bytes = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
            built.path, ep.host
        )
        .into_bytes();
        for (k, v) in &built.headers {
            req_bytes.extend_from_slice(k.as_bytes());
            req_bytes.extend_from_slice(b": ");
            req_bytes.extend_from_slice(v.as_bytes());
            req_bytes.extend_from_slice(b"\r\n");
        }
        req_bytes.extend_from_slice(b"\r\n");

        match &ep.security {
            DialSecurity::Plain => {
                let mut stream = tcp;
                stream
                    .write_all(&req_bytes)
                    .map_err(|e| format!("cold REST write failed: {e}"))?;
                stream
                    .flush()
                    .map_err(|e| format!("cold REST flush failed: {e}"))?;
                read_http_body(BufReader::new(stream), path)
            }
            DialSecurity::Tls {
                server_name,
                config,
            } => {
                let conn = rustls::ClientConnection::new(Arc::clone(config), server_name.clone())
                    .map_err(|e| format!("TLS ClientConnection error: {e}"))?;
                let mut tls_stream = rustls::StreamOwned::new(conn, tcp);
                tls_stream
                    .write_all(&req_bytes)
                    .map_err(|e| format!("cold TLS REST write failed: {e}"))?;
                tls_stream
                    .flush()
                    .map_err(|e| format!("cold TLS REST flush failed: {e}"))?;
                read_http_body(BufReader::new(tls_stream), path)
            }
        }
    }
}

fn read_http_body<R: Read>(mut reader: BufReader<R>, path: &str) -> Result<Vec<u8>, String> {
    let mut status_line = String::new();
    reader
        .read_line(&mut status_line)
        .map_err(|e| format!("cold REST status read failed on {path}: {e}"))?;
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| format!("malformed HTTP status line on {path}: {status_line:?}"))?;

    let mut content_len: Option<usize> = None;
    loop {
        let mut line = String::new();
        let n = reader
            .read_line(&mut line)
            .map_err(|e| format!("cold REST header read failed on {path}: {e}"))?;
        if n == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        if let Some((name, val)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("content-length")
        {
            content_len = val.trim().parse::<usize>().ok();
        }
    }

    let mut body = Vec::new();
    if let Some(len) = content_len {
        body.resize(len, 0u8);
        reader
            .read_exact(&mut body)
            .map_err(|e| format!("cold REST body read_exact failed on {path}: {e}"))?;
    } else {
        reader
            .read_to_end(&mut body)
            .map_err(|e| format!("cold REST body read_to_end failed on {path}: {e}"))?;
    }

    if status_code != 200 {
        return Err(format!(
            "cold REST GET {path} returned HTTP {status_code}: {}",
            String::from_utf8_lossy(&body)
        ));
    }
    Ok(body)
}

#[inline]
fn is_rate_limit(venue: SpotVenue, code: &str) -> bool {
    matches!(
        (venue, code),
        (SpotVenue::Bybit, "10006" | "429") | (SpotVenue::Okx, "50011" | "429")
    )
}

impl VenueSession for UtaVenueSession {
    fn venue_profile(&self) -> OrsVenueProfile {
        self.venue.quota_profile()
    }

    fn url_pools(&self, _spec: &AccountSpec) -> Result<Vec<SessionUrlPoolSpec>, String> {
        let mut pools = Vec::with_capacity(2);
        let order_kind = match self.transport_mode {
            UtaTransportMode::WebSocketTrade => LinkKind::Ws,
            UtaTransportMode::PooledHttps => LinkKind::Https,
        };
        let order_role = if self.transport_mode == UtaTransportMode::WebSocketTrade
            && self.venue == SpotVenue::Okx
            && self.okx_ws_auto_subscribe_private
            && self.stream_endpoint.is_none()
        {
            UrlRole::ORDER_AND_STREAM
        } else {
            UrlRole::ORDER
        };

        pools.push(SessionUrlPoolSpec {
            url_id: format!("{}-uta-order", self.venue.as_str()),
            role: order_role,
            kind: order_kind,
            product_mask: 0,
            host: self.order_endpoint.host.clone(),
            port: self.order_endpoint.port,
            path: self.order_endpoint.path.clone(),
            security: self.order_endpoint.security.clone(),
            initial_peers: self.order_endpoint.initial_peers.clone(),
        });

        if let Some(stream_ep) = &self.stream_endpoint {
            pools.push(SessionUrlPoolSpec {
                url_id: format!("{}-uta-stream", self.venue.as_str()),
                role: UrlRole::STREAM,
                kind: LinkKind::Ws,
                product_mask: 0,
                host: stream_ep.host.clone(),
                port: stream_ep.port,
                path: stream_ep.path.clone(),
                security: stream_ep.security.clone(),
                initial_peers: stream_ep.initial_peers.clone(),
            });
        }
        Ok(pools)
    }

    fn rest_endpoint(&self, _spec: &AccountSpec) -> Option<SessionRestEndpoint> {
        self.rest_endpoint.clone()
    }

    fn instrument_scope(&self, symbol_id: u32, product: Product) -> InstrumentScope {
        if let Some(binding) = self.find_symbol_by_id(symbol_id) {
            instrument_rate_scope(self.venue, product, binding.symbol.as_str())
        } else {
            InstrumentScope::Product(u64::from(product.code()))
        }
    }

    fn place_ip_weight(&self, _product: Product) -> u32 {
        self.ip_order_weight
    }

    fn cancel_ip_weight(&self, _product: Option<Product>) -> u32 {
        self.ip_order_weight
    }

    fn encode_new_order(
        &self,
        _spec: &AccountSpec,
        req: &OrsNewOrderRequest,
        placed: &OrsPlaceOutcome,
        _conn_id: u16,
        scratch: &mut [u8],
    ) -> Result<EncodedOrderFrame, RefusalReason> {
        let binding = self
            .find_symbol_by_id(req.symbol_id)
            .ok_or(RefusalReason::InvalidQuantity)?;

        let mut params = if binding.spec.product == Product::Spot {
            UtaOrderParams::cash_spot_limit(req.tif)
        } else {
            UtaOrderParams::cross_derivative_limit(req.tif, req.reduce_only)
        };
        params.post_only |= req.post_only;
        params.reduce_only = req.reduce_only;
        if binding.spec.product == Product::Spot && req.reduce_only {
            params.okx_td_mode = OkxMarginMode::Cross;
        }

        let wire_in = UtaNewOrderWireInput {
            symbol: binding.symbol.as_str(),
            spec: &binding.spec,
            side: req.side,
            price_ticks: req.price_ticks,
            qty_lots: req.qty_lots,
            client_id: placed.client_id.as_str(),
            params,
        };

        let utc_ms = (self.clock_utc_ms)();
        let reply_id = match self.transport_mode {
            UtaTransportMode::WebSocketTrade => self.alloc_reply_id(),
            UtaTransportMode::PooledHttps => 0,
        };

        let mut arg_buf = [0u8; 1024];
        let arg_len = {
            let mut bw = BufWriter::new(&mut arg_buf);
            match self.venue {
                SpotVenue::Bybit => encode_bybit_new_order_arg(&wire_in, &mut bw),
                SpotVenue::Okx => encode_okx_new_order_arg(&wire_in, &mut bw),
            }
            .map_err(|_| RefusalReason::InvalidQuantity)?;
            bw.written()
        };

        let frame_len = match self.transport_mode {
            UtaTransportMode::WebSocketTrade => {
                let mut env_buf = [0u8; 1536];
                let env_len = match self.venue {
                    SpotVenue::Bybit => encode_bybit_ws_trade_envelope_into(
                        reply_id,
                        utc_ms,
                        "order.create",
                        &arg_buf[..arg_len],
                        &mut env_buf,
                    ),
                    SpotVenue::Okx => encode_okx_ws_trade_envelope_into(
                        reply_id,
                        "order",
                        &arg_buf[..arg_len],
                        &mut env_buf,
                    ),
                }
                .map_err(|_| RefusalReason::InvalidQuantity)?;
                encode_ws_client_frame_into(
                    ws::Opcode::Text,
                    &env_buf[..env_len],
                    reply_id ^ req.now_mono_ns,
                    scratch,
                )
                .map_err(|_| RefusalReason::AbortedBeforeSend)?
            }
            UtaTransportMode::PooledHttps => {
                let path = match self.venue {
                    SpotVenue::Bybit => "/v5/order/create",
                    SpotVenue::Okx => "/api/v5/trade/order",
                };
                let creds = self.credentials.credentials();
                encode_signed_https_post_into(
                    &creds,
                    self.environment,
                    utc_ms,
                    path,
                    &arg_buf[..arg_len],
                    scratch,
                )
                .map_err(|_| RefusalReason::AbortedBeforeSend)?
            }
        };

        if placed.should_send_to_venue {
            self.record_inflight_tag(InflightOpTag {
                ors_order_id: placed.ors_order_id,
                reply_id,
                symbol_id: req.symbol_id,
                op: UtaReplyOp::Create,
                price_ticks: req.price_ticks,
                qty_lots: req.qty_lots,
                side: Some(req.side),
                tif: req.tif,
                post_only: params.post_only,
            });
        }

        Ok(EncodedOrderFrame {
            frame_len,
            reply_or_seq_id: reply_id,
        })
    }

    fn encode_cancel_order(
        &self,
        _spec: &AccountSpec,
        cancel: &OrsCancelDispatch,
        _conn_id: u16,
        scratch: &mut [u8],
    ) -> Result<EncodedOrderFrame, RefusalReason> {
        let binding = self
            .find_symbol_by_id(cancel.symbol_id)
            .ok_or(RefusalReason::InvalidQuantity)?;

        let cid_str = cancel.client_id.as_str();
        let vid_str = cancel.venue_order_id.as_str();
        let wire_in = UtaCancelWireInput {
            symbol: binding.symbol.as_str(),
            product: binding.spec.product,
            client_id: (!cid_str.is_empty()).then_some(cid_str),
            venue_order_id: (!vid_str.is_empty()).then_some(vid_str),
        };

        let utc_ms = (self.clock_utc_ms)();
        let reply_id = match self.transport_mode {
            UtaTransportMode::WebSocketTrade => self.alloc_reply_id(),
            UtaTransportMode::PooledHttps => 0,
        };

        let mut arg_buf = [0u8; 768];
        let arg_len = {
            let mut bw = BufWriter::new(&mut arg_buf);
            match self.venue {
                SpotVenue::Bybit => encode_bybit_cancel_arg(&wire_in, &mut bw),
                SpotVenue::Okx => encode_okx_cancel_arg(&wire_in, &mut bw),
            }
            .map_err(|_| RefusalReason::InvalidQuantity)?;
            bw.written()
        };

        let frame_len = match self.transport_mode {
            UtaTransportMode::WebSocketTrade => {
                let mut env_buf = [0u8; 1280];
                let env_len = match self.venue {
                    SpotVenue::Bybit => encode_bybit_ws_trade_envelope_into(
                        reply_id,
                        utc_ms,
                        "order.cancel",
                        &arg_buf[..arg_len],
                        &mut env_buf,
                    ),
                    SpotVenue::Okx => encode_okx_ws_trade_envelope_into(
                        reply_id,
                        "cancel-order",
                        &arg_buf[..arg_len],
                        &mut env_buf,
                    ),
                }
                .map_err(|_| RefusalReason::InvalidQuantity)?;
                encode_ws_client_frame_into(
                    ws::Opcode::Text,
                    &env_buf[..env_len],
                    reply_id ^ cancel.ors_order_id,
                    scratch,
                )
                .map_err(|_| RefusalReason::AbortedBeforeSend)?
            }
            UtaTransportMode::PooledHttps => {
                let path = match self.venue {
                    SpotVenue::Bybit => "/v5/order/cancel",
                    SpotVenue::Okx => "/api/v5/trade/cancel-order",
                };
                let creds = self.credentials.credentials();
                encode_signed_https_post_into(
                    &creds,
                    self.environment,
                    utc_ms,
                    path,
                    &arg_buf[..arg_len],
                    scratch,
                )
                .map_err(|_| RefusalReason::AbortedBeforeSend)?
            }
        };

        self.record_inflight_tag(InflightOpTag {
            ors_order_id: cancel.ors_order_id,
            reply_id,
            symbol_id: cancel.symbol_id,
            op: UtaReplyOp::Cancel,
            price_ticks: 0,
            qty_lots: 0,
            side: None,
            tif: TimeInForce::Gtc,
            post_only: false,
        });

        Ok(EncodedOrderFrame {
            frame_len,
            reply_or_seq_id: reply_id,
        })
    }

    fn encode_modify_order(
        &self,
        _spec: &AccountSpec,
        modify: &OrsModifyDispatch,
        client_id: &str,
        venue_order_id: &str,
        _conn_id: u16,
        scratch: &mut [u8],
    ) -> Result<EncodedOrderFrame, RefusalReason> {
        let binding = self
            .find_symbol_by_id(modify.symbol_id)
            .ok_or(RefusalReason::InvalidQuantity)?;

        let wire_in = UtaModifyWireInput {
            symbol: binding.symbol.as_str(),
            spec: &binding.spec,
            client_id: (!client_id.is_empty()).then_some(client_id),
            venue_order_id: (!venue_order_id.is_empty()).then_some(venue_order_id),
            req_id: None,
            new_price_ticks: modify.new_price_ticks,
            new_qty_lots: modify.new_qty_lots,
        };

        let utc_ms = (self.clock_utc_ms)();
        let reply_id = match self.transport_mode {
            UtaTransportMode::WebSocketTrade => self.alloc_reply_id(),
            UtaTransportMode::PooledHttps => 0,
        };

        let mut arg_buf = [0u8; 1024];
        let arg_len = {
            let mut bw = BufWriter::new(&mut arg_buf);
            match self.venue {
                SpotVenue::Bybit => encode_bybit_modify_arg(&wire_in, &mut bw),
                SpotVenue::Okx => encode_okx_modify_arg(&wire_in, &mut bw),
            }
            .map_err(|_| RefusalReason::InvalidQuantity)?;
            bw.written()
        };

        let frame_len = match self.transport_mode {
            UtaTransportMode::WebSocketTrade => {
                let mut env_buf = [0u8; 1536];
                let env_len = match self.venue {
                    SpotVenue::Bybit => encode_bybit_ws_trade_envelope_into(
                        reply_id,
                        utc_ms,
                        "order.amend",
                        &arg_buf[..arg_len],
                        &mut env_buf,
                    ),
                    SpotVenue::Okx => encode_okx_ws_trade_envelope_into(
                        reply_id,
                        "amend-order",
                        &arg_buf[..arg_len],
                        &mut env_buf,
                    ),
                }
                .map_err(|_| RefusalReason::InvalidQuantity)?;
                encode_ws_client_frame_into(
                    ws::Opcode::Text,
                    &env_buf[..env_len],
                    reply_id ^ modify.ors_order_id,
                    scratch,
                )
                .map_err(|_| RefusalReason::AbortedBeforeSend)?
            }
            UtaTransportMode::PooledHttps => {
                let path = match self.venue {
                    SpotVenue::Bybit => "/v5/order/amend",
                    SpotVenue::Okx => "/api/v5/trade/amend-order",
                };
                let creds = self.credentials.credentials();
                encode_signed_https_post_into(
                    &creds,
                    self.environment,
                    utc_ms,
                    path,
                    &arg_buf[..arg_len],
                    scratch,
                )
                .map_err(|_| RefusalReason::AbortedBeforeSend)?
            }
        };

        self.record_inflight_tag(InflightOpTag {
            ors_order_id: modify.ors_order_id,
            reply_id,
            symbol_id: modify.symbol_id,
            op: UtaReplyOp::Amend,
            price_ticks: modify.new_price_ticks,
            qty_lots: modify.new_qty_lots,
            side: None,
            tif: TimeInForce::Gtc,
            post_only: false,
        });

        Ok(EncodedOrderFrame {
            frame_len,
            reply_or_seq_id: reply_id,
        })
    }

    fn has_quote(&self, ors_order_id: u64) -> bool {
        self.rfq_hook.has_quote(ors_order_id)
    }

    fn open_quotes(&self) -> usize {
        self.rfq_hook.open_quotes()
    }

    fn submit_quote(
        &self,
        ctx: &AccountSessionContext<'_>,
        quote: &RfqQuoteRequest,
        scratch: &mut [u8],
    ) -> Result<ReportBody, RefusalReason> {
        if !self.rfq_hook.is_enabled() {
            return Err(RefusalReason::RfqQuoteNotSupported);
        }
        let binding = self
            .find_symbol_by_id(quote.symbol_id)
            .ok_or(RefusalReason::InvalidQuantity)?;
        let sym_32 = InlineAscii::<32>::try_from_str(binding.symbol.as_str())
            .ok_or(RefusalReason::InvalidQuantity)?;
        let price_atoms = i128::from(quote.price_ticks)
            .checked_mul(binding.spec.price_tick_atoms)
            .ok_or(RefusalReason::InvalidPrice)?;
        let qty_atoms = i128::from(quote.qty_lots)
            .checked_mul(binding.spec.qty_step_atoms)
            .ok_or(RefusalReason::InvalidQuantity)?;

        let leg = RfqQuoteLeg {
            symbol: sym_32,
            product: binding.spec.product,
            side: quote.side,
            price_atoms,
            price_scale: binding.spec.price_scale,
            qty_atoms,
            qty_scale: binding.spec.qty_scale,
            ratio: 1,
        };

        let domain = self.client_id_domain(binding.spec.product);
        let client_id_tok = SelfDescribingClientId::from_ors_order(
            quote.strat_id,
            0,
            ctx.spec.epoch,
            quote.order_seqnum,
        )
        .and_then(|sd| sd.encode(&domain))
        .map_err(|_| RefusalReason::AbortedBeforeSend)?;

        let mut rfq_id_str = [0u8; 20];
        let n = tickwire_spot_execution::write_u64_into(&mut rfq_id_str, quote.ors_order_id)
            .unwrap_or(1);
        let rfq_str = core::str::from_utf8(&rfq_id_str[..n]).unwrap_or("1");

        let spot_req = SpotRfqQuoteRequest::single_leg(
            quote.ors_order_id,
            quote.strat_id,
            rfq_str,
            client_id_tok.as_str(),
            quote.side,
            leg,
            (self.clock_utc_ms)().saturating_add(60_000),
        )
        .map_err(|_| RefusalReason::InvalidQuantity)?;

        let mut body_buf = [0u8; 1536];
        let built = self
            .rfq_hook
            .submit_quote(self.venue, &spot_req, &mut body_buf)?;

        let scope =
            instrument_rate_scope(self.venue, binding.spec.product, binding.symbol.as_str());
        let route = match ctx.pick_and_place_or_nack(
            binding.spec.product,
            scope,
            1,
            self.ip_order_weight,
            quote.now_mono_ns,
        ) {
            Ok(r) => r,
            Err(_) => {
                let _ = self.rfq_hook.on_quote_terminal(quote.ors_order_id);
                return Err(RefusalReason::RateLimited);
            }
        };

        let creds = self.credentials.credentials();
        let utc_ms = (self.clock_utc_ms)();
        let frame_len = match self.transport_mode {
            UtaTransportMode::PooledHttps => encode_signed_https_post_into(
                &creds,
                self.environment,
                utc_ms,
                built.path,
                &body_buf[..built.body_len],
                scratch,
            ),
            UtaTransportMode::WebSocketTrade => encode_ws_client_frame_into(
                ws::Opcode::Text,
                &body_buf[..built.body_len],
                quote.ors_order_id ^ quote.now_mono_ns,
                scratch,
            ),
        }
        .map_err(|_| {
            let _ = self.rfq_hook.on_quote_terminal(quote.ors_order_id);
            RefusalReason::AbortedBeforeSend
        })?;

        let Some(conn) = ctx.connection_handle(route.connection_id) else {
            let _ = self.rfq_hook.on_quote_terminal(quote.ors_order_id);
            return Err(RefusalReason::MarketRejectMarketDown);
        };
        let reply_id = if self.transport_mode == UtaTransportMode::WebSocketTrade {
            self.alloc_reply_id()
        } else {
            0
        };
        self.record_inflight_tag(InflightOpTag {
            ors_order_id: quote.ors_order_id,
            reply_id,
            symbol_id: quote.symbol_id,
            op: UtaReplyOp::Create,
            price_ticks: quote.price_ticks,
            qty_lots: quote.qty_lots,
            side: Some(quote.side),
            tif: TimeInForce::Gtc,
            post_only: false,
        });

        let req_ref = RequestRef {
            order_key: quote.ors_order_id,
            client_seq: quote.order_seqnum,
            sent_tsc: 0,
            sent_mono_ns: quote.now_mono_ns,
        };
        if conn.send_frame(&scratch[..frame_len], reply_id, req_ref) != SendOutcome::Sent {
            let _ = self.rfq_hook.on_quote_terminal(quote.ors_order_id);
            return Err(RefusalReason::MarketRejectMarketDown);
        }

        let side_code = match quote.side {
            Side::Buy => 1,
            Side::Sell => 2,
        };
        Ok(ReportBody {
            ors_order_id: quote.ors_order_id,
            order_seqnum: quote.order_seqnum,
            orig_seqnum: quote.order_seqnum,
            instrument_id: binding.spec.instrument_id,
            event_mono_ns: quote.now_mono_ns,
            quantity_lots: quote.qty_lots,
            leaves_qty_lots: quote.qty_lots,
            price_ticks: quote.price_ticks,
            report_kind: report_kind::RFQ_QUOTE_ACK,
            order_status: 2,
            reason: TerminalReason::NO_REASON.code(),
            strat_id: quote.strat_id,
            account_ref: ctx.spec.account_ref,
            side: side_code,
            ..ReportBody::default()
        })
    }

    fn cancel_quotes(
        &self,
        ctx: &AccountSessionContext<'_>,
        cancel: &RfqQuoteCancelRequest,
        scratch: &mut [u8],
    ) -> Result<usize, RefusalReason> {
        if !self.rfq_hook.is_enabled() {
            return Err(RefusalReason::RfqQuoteNotSupported);
        }
        let open_before = self.rfq_hook.open_quotes();
        if open_before == 0 && cancel.ors_order_id.is_none() {
            return Ok(0);
        }
        let cxl_req = SpotRfqCancelRequest {
            ors_order_id: cancel.ors_order_id,
            rfq_id: None,
            venue_quote_id: None,
            client_quote_id: None,
        };
        let mut body_buf = [0u8; 768];
        let built = self
            .rfq_hook
            .cancel_quotes(self.venue, &cxl_req, &mut body_buf)?;

        let route = ctx
            .pick_and_cancel_or_nack(
                Some(Product::Option),
                InstrumentScope::Product(u64::from(Product::Option.code())),
                self.ip_order_weight,
                cancel.now_mono_ns,
            )
            .map_err(|_| RefusalReason::MarketRejectMarketDown)?;

        let creds = self.credentials.credentials();
        let utc_ms = (self.clock_utc_ms)();
        let frame_len = match self.transport_mode {
            UtaTransportMode::PooledHttps => encode_signed_https_post_into(
                &creds,
                self.environment,
                utc_ms,
                built.path,
                &body_buf[..built.body_len],
                scratch,
            ),
            UtaTransportMode::WebSocketTrade => encode_ws_client_frame_into(
                ws::Opcode::Text,
                &body_buf[..built.body_len],
                cancel.now_mono_ns ^ 0x51C0_CA4C,
                scratch,
            ),
        }
        .map_err(|_| RefusalReason::AbortedBeforeSend)?;

        let Some(conn) = ctx.connection_handle(route.connection_id) else {
            return Err(RefusalReason::MarketRejectMarketDown);
        };
        let oid = cancel.ors_order_id.unwrap_or(0);
        let reply_id = if self.transport_mode == UtaTransportMode::WebSocketTrade {
            self.alloc_reply_id()
        } else {
            0
        };
        if oid != 0 {
            self.record_inflight_tag(InflightOpTag {
                ors_order_id: oid,
                reply_id,
                symbol_id: cancel.symbol_id.unwrap_or(0),
                op: UtaReplyOp::Cancel,
                price_ticks: 0,
                qty_lots: 0,
                side: None,
                tif: TimeInForce::Gtc,
                post_only: false,
            });
        }
        let req_ref = RequestRef {
            order_key: oid,
            client_seq: 0,
            sent_tsc: 0,
            sent_mono_ns: cancel.now_mono_ns,
        };
        if conn.send_frame(&scratch[..frame_len], reply_id, req_ref) != SendOutcome::Sent {
            return Err(RefusalReason::MarketRejectMarketDown);
        }
        if let Some(target_oid) = cancel.ors_order_id {
            let _ = self.rfq_hook.on_quote_terminal(target_oid);
            Ok(1)
        } else {
            Ok(open_before)
        }
    }

    fn connection_url(
        &self,
        _spec: &AccountSpec,
        pool: &SessionUrlPoolSpec,
        _stream_token: Option<&[u8]>,
    ) -> Result<SessionEndpoint, String> {
        Ok(SessionEndpoint {
            host: pool.host.clone(),
            port: pool.port,
            path: pool.path.clone(),
            security: pool.security.clone(),
            initial_peers: pool.initial_peers.clone(),
        })
    }

    fn encode_login(
        &self,
        _spec: &AccountSpec,
        role: UrlRole,
        _seq: u32,
        out: &mut [u8],
    ) -> Result<usize, String> {
        if !self.awaits_login_reply(role) {
            return Ok(0);
        }
        let creds = self.credentials.credentials();
        let utc_ms = (self.clock_utc_ms)();
        match self.venue {
            SpotVenue::Bybit => encode_bybit_auth_into(&creds, utc_ms, out)
                .map_err(|e| format!("encode_bybit_auth_into failed: {e}")),
            SpotVenue::Okx => encode_okx_login_into(&creds, utc_ms, out)
                .map_err(|e| format!("encode_okx_login_into failed: {e}")),
        }
    }

    fn awaits_login_reply(&self, role: UrlRole) -> bool {
        role.is_stream() || self.transport_mode == UtaTransportMode::WebSocketTrade
    }

    fn login_reply(&self, payload: &[u8], _role: UrlRole) -> LoginReply {
        let out = match self.venue {
            SpotVenue::Bybit => check_bybit_auth_reply(payload),
            SpotVenue::Okx => check_okx_login_reply(payload),
        };
        match out {
            WsLoginOutcome::Accepted => LoginReply::Ok,
            WsLoginOutcome::Rejected => LoginReply::Fail,
            WsLoginOutcome::NotLoginFrame => LoginReply::Other,
        }
    }

    fn encode_subscribe(
        &self,
        _spec: &AccountSpec,
        role: UrlRole,
        step: usize,
        out: &mut [u8],
    ) -> Result<usize, String> {
        if !role.is_stream() {
            return Ok(0);
        }
        let Ok(step_u8) = u8::try_from(step) else {
            return Ok(0);
        };
        match self.venue {
            SpotVenue::Bybit => encode_bybit_private_subscribe_step_into(step_u8, out)
                .map_err(|e| format!("encode_bybit_private_subscribe_step_into failed: {e}")),
            SpotVenue::Okx => encode_okx_subscribe_step_into(step_u8, out)
                .map_err(|e| format!("encode_okx_subscribe_step_into failed: {e}")),
        }
    }

    fn is_pong(&self, payload: &[u8]) -> bool {
        match self.venue {
            SpotVenue::Okx => payload == b"pong",
            SpotVenue::Bybit => {
                payload.windows(11).any(|w| w == br#""op":"pong""#)
                    || payload.windows(16).any(|w| w == br#""ret_msg":"pong""#)
            }
        }
    }

    fn reply_id(&self, payload: &[u8]) -> Option<u64> {
        let mut found = None;
        match self.venue {
            SpotVenue::Bybit => {
                let _ = scan_bybit_frame(payload, |ev| {
                    if let UtaFrameEvent::CommandReply(rep) = ev
                        && found.is_none()
                    {
                        found = rep.reply_id;
                    }
                });
            }
            SpotVenue::Okx => {
                let _ = scan_okx_frame(payload, |ev| {
                    if let UtaFrameEvent::CommandReply(rep) = ev
                        && found.is_none()
                    {
                        found = rep.reply_id;
                    }
                });
            }
        }
        found
    }

    fn encode_ping(&self, _role: UrlRole, _now_ms: u64, out: &mut [u8]) -> usize {
        let ping_bytes: &[u8] = match self.venue {
            SpotVenue::Bybit => br#"{"op":"ping"}"#,
            SpotVenue::Okx => b"ping",
        };
        if out.len() < ping_bytes.len() {
            return 0;
        }
        out[..ping_bytes.len()].copy_from_slice(ping_bytes);
        ping_bytes.len()
    }

    fn on_message(
        &self,
        account_ref: u8,
        _url_id: &str,
        _role: UrlRole,
        _conn_id: u16,
        hint: Option<RequestRef>,
        payload: &[u8],
        now_mono_ns: u64,
        effects: &mut SessionMessageEffects,
    ) {
        let scan_res = match self.venue {
            SpotVenue::Bybit => scan_bybit_frame(payload, |ev| match ev {
                UtaFrameEvent::CommandReply(rep) => {
                    self.handle_command_reply(account_ref, hint, rep, now_mono_ns, effects);
                }
                UtaFrameEvent::OrderRow(row) => {
                    self.handle_order_row(account_ref, &row, now_mono_ns, effects);
                }
                UtaFrameEvent::ExecRow(row) => {
                    self.handle_exec_row(account_ref, &row, now_mono_ns, effects);
                }
                UtaFrameEvent::Control { .. }
                | UtaFrameEvent::MaintenanceNotice { .. }
                | UtaFrameEvent::WalletCoin(_) => {}
            }),
            SpotVenue::Okx => scan_okx_frame(payload, |ev| match ev {
                UtaFrameEvent::CommandReply(rep) => {
                    self.handle_command_reply(account_ref, hint, rep, now_mono_ns, effects);
                }
                UtaFrameEvent::OrderRow(row) => {
                    self.handle_order_row(account_ref, &row, now_mono_ns, effects);
                }
                UtaFrameEvent::ExecRow(row) => {
                    self.handle_exec_row(account_ref, &row, now_mono_ns, effects);
                }
                UtaFrameEvent::Control { .. }
                | UtaFrameEvent::MaintenanceNotice { .. }
                | UtaFrameEvent::WalletCoin(_) => {}
            }),
        };

        if let Ok(summary) = scan_res {
            if summary.graceful_drain_connection {
                effects.graceful_drain_connection = true;
            }
            if let Some((used, limit)) = summary.bapi_limit
                && limit > 0
            {
                effects.rate_limits.push(VenueRateLimitRow {
                    id: RateLimitId {
                        local_ip: IpKey::NONE,
                        interval_s: 1,
                        account: u64::from(account_ref),
                        flags: RateFlags::PER_ACCOUNT.union(RateFlags::ORDER),
                        symbol: 0,
                    },
                    count: used,
                    limit,
                });
            }
            if summary.rate_limited {
                effects.rate_limits.push(VenueRateLimitRow {
                    id: RateLimitId {
                        local_ip: IpKey::NONE,
                        interval_s: 1,
                        account: u64::from(account_ref),
                        flags: RateFlags::PER_ACCOUNT.union(RateFlags::ORDER),
                        symbol: 0,
                    },
                    count: 100,
                    limit: 100,
                });
            }
        }
    }

    fn open_orders_queries(&self, _spec: &AccountSpec) -> Vec<BuiltRequest> {
        let creds = self.credentials.credentials();
        let utc_ms = (self.clock_utc_ms)();
        let mut out = Vec::new();
        match self.venue {
            SpotVenue::Bybit => {
                for cat in BybitCategory::ALL {
                    let settle = (cat == BybitCategory::Linear).then_some("USDT");
                    if let Ok(path) = bybit_open_orders_path(cat, settle, None, None, None)
                        && let Ok(req) =
                            sign_get_built_request(&creds, self.environment, utc_ms, path)
                    {
                        out.push(req);
                    }
                }
            }
            SpotVenue::Okx => {
                for inst in OkxInstType::ALL {
                    if let Ok(path) = okx_open_orders_path(inst, None)
                        && let Ok(req) =
                            sign_get_built_request(&creds, self.environment, utc_ms, path)
                    {
                        out.push(req);
                    }
                }
            }
        }
        out
    }

    fn order_status_query(
        &self,
        spec: &AccountSpec,
        ors_order_id: u64,
        symbol_id: u32,
        client_id: &str,
        venue_order_id: &str,
        _kind: SessionRequestKind,
    ) -> Option<BuiltRequest> {
        let binding = self.find_symbol_by_id(symbol_id)?;
        let target = StatusAskTarget {
            ors_order_id,
            account_ref: spec.account_ref,
            symbol_id,
            product: binding.spec.product,
            symbol: binding.symbol,
            client_id: InlineAscii::lossy_ascii(client_id),
            venue_order_id: InlineAscii::lossy_ascii(venue_order_id),
        };
        let path = status_ask_order_path(self.venue, &target).ok()?;
        let creds = self.credentials.credentials();
        sign_get_built_request(&creds, self.environment, (self.clock_utc_ms)(), path).ok()
    }

    fn status_query_delay_ms(&self) -> u64 {
        self.status_query_delay_ms
    }

    fn apply_status(
        &self,
        account_ref: u8,
        ors_order_id: u64,
        symbol_id: u32,
        _kind: SessionRequestKind,
        _task: &RestTask,
        reply: &RestReply,
        now_mono_ns: u64,
    ) -> StatusReplyOutcome {
        if reply.status != RestStatus::Ok || reply.http_code != 200 {
            return StatusReplyOutcome {
                snapshot: None,
                retry_query: true,
                rate_limits: Vec::new(),
            };
        }
        let Some(binding) = self.find_symbol_by_id(symbol_id) else {
            return StatusReplyOutcome {
                snapshot: None,
                retry_query: false,
                rate_limits: Vec::new(),
            };
        };
        let target = StatusAskTarget {
            ors_order_id,
            account_ref,
            symbol_id,
            product: binding.spec.product,
            symbol: binding.symbol,
            client_id: InlineAscii::empty(),
            venue_order_id: InlineAscii::empty(),
        };
        match decode_status_ask_snapshot(
            self.venue,
            &target,
            &binding.spec,
            &self.assets,
            &reply.body,
            None,
            now_mono_ns,
        ) {
            Ok(decoded) => StatusReplyOutcome {
                snapshot: Some(OwnedOrderQuerySnapshot {
                    ors_order_id: decoded.ors_order_id,
                    account_ref: decoded.account_ref,
                    symbol_id: decoded.symbol_id,
                    status: decoded.status,
                    venue_order_id: decoded
                        .venue_order_id
                        .as_deref()
                        .map(InlineAscii::lossy_ascii),
                    price_ticks: decoded.price_ticks,
                    orig_qty_lots: decoded.orig_qty_lots,
                    cum_filled_lots: decoded.cum_filled_lots,
                    avg_fill_price_ticks: decoded.avg_fill_price_ticks,
                    trades: decoded.trades,
                    received_mono_ns: decoded.received_mono_ns,
                }),
                retry_query: false,
                rate_limits: Vec::new(),
            },
            Err(_) => StatusReplyOutcome {
                snapshot: None,
                retry_query: true,
                rate_limits: Vec::new(),
            },
        }
    }

    fn bootstrap_snapshots(
        &self,
        _spec: &AccountSpec,
    ) -> Result<OrsExchangeBootstrapSnapshot, String> {
        if let Some(snapshot) = &self.bootstrap_override {
            return Ok(snapshot.clone());
        }
        let Some(rest_ep) = &self.rest_endpoint else {
            return Ok(OrsExchangeBootstrapSnapshot {
                balances: Vec::new(),
                positions: Vec::new(),
                open_orders: Vec::new(),
                balances_include_open_order_locks: true,
            });
        };
        if rest_ep.port == 0 && rest_ep.initial_peers.is_empty() {
            return Ok(OrsExchangeBootstrapSnapshot {
                balances: Vec::new(),
                positions: Vec::new(),
                open_orders: Vec::new(),
                balances_include_open_order_locks: true,
            });
        }

        let mut decoder =
            UtaBootstrapDecoder::new(self.venue, self.symbols.to_vec(), self.assets.to_vec());

        let bal_body = self.fetch_cold_rest_get(rest_ep, wallet_balance_path(self.venue))?;
        decoder
            .ingest_wallet_balance_json(&bal_body)
            .map_err(|e| format!("ingest_wallet_balance_json failed: {e}"))?;

        let has_deriv_or_opt = self.symbols.iter().any(|s| s.spec.product != Product::Spot);
        if has_deriv_or_opt {
            match self.venue {
                SpotVenue::Bybit => {
                    for cat in [
                        BybitCategory::Linear,
                        BybitCategory::Inverse,
                        BybitCategory::Option,
                    ] {
                        if !self
                            .symbols
                            .iter()
                            .any(|s| BybitCategory::from_product(s.spec.product) == Some(cat))
                        {
                            continue;
                        }
                        let settle = (cat == BybitCategory::Linear).then_some("USDT");
                        let path = bybit_positions_path(cat, settle, None, None)
                            .map_err(|e| format!("bybit_positions_path failed: {e}"))?;
                        let pos_body = self.fetch_cold_rest_get(rest_ep, &path)?;
                        decoder
                            .ingest_positions_json(&pos_body)
                            .map_err(|e| format!("ingest_positions_json failed: {e}"))?;
                    }
                }
                SpotVenue::Okx => {
                    let pos_body = self.fetch_cold_rest_get(rest_ep, &okx_positions_path(None))?;
                    decoder
                        .ingest_positions_json(&pos_body)
                        .map_err(|e| format!("ingest_positions_json failed: {e}"))?;
                }
            }
        }

        match self.venue {
            SpotVenue::Bybit => {
                for cat in BybitCategory::ALL {
                    if !self
                        .symbols
                        .iter()
                        .any(|s| BybitCategory::from_product(s.spec.product) == Some(cat))
                    {
                        continue;
                    }
                    let mut cursor: Option<String> = None;
                    loop {
                        let settle = (cat == BybitCategory::Linear).then_some("USDT");
                        let path =
                            bybit_open_orders_path(cat, settle, None, None, cursor.as_deref())
                                .map_err(|e| format!("bybit_open_orders_path failed: {e}"))?;
                        let oo_body = self.fetch_cold_rest_get(rest_ep, &path)?;
                        let (_, next) = decoder
                            .ingest_open_orders_json(&oo_body, 0)
                            .map_err(|e| format!("ingest_open_orders_json failed: {e}"))?;
                        if next.is_none() {
                            break;
                        }
                        cursor = next;
                    }
                }
            }
            SpotVenue::Okx => {
                for inst in OkxInstType::ALL {
                    if !self
                        .symbols
                        .iter()
                        .any(|s| OkxInstType::from_product(s.spec.product) == Some(inst))
                    {
                        continue;
                    }
                    let mut after: Option<String> = None;
                    loop {
                        let path = okx_open_orders_path(inst, after.as_deref())
                            .map_err(|e| format!("okx_open_orders_path failed: {e}"))?;
                        let oo_body = self.fetch_cold_rest_get(rest_ep, &path)?;
                        let (_, next) = decoder
                            .ingest_open_orders_json(&oo_body, 0)
                            .map_err(|e| format!("ingest_open_orders_json failed: {e}"))?;
                        if next.is_none() {
                            break;
                        }
                        after = next;
                    }
                }
            }
        }

        let _ = parse_u64_ascii;
        Ok(decoder.build_snapshot())
    }
}
