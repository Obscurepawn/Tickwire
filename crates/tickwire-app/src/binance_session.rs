//! Stateless full-market [`BinanceVenueSession`] implementing [`tickwire_runtime::VenueSession`]
//! across Spot (`/api/v3`, `/ws-api/v3`), USD-M (`/fapi`, `/ws-fapi/v1`), Coin-M (`/dapi`,
//! `/ws-dapi/v1`), and European Options (`/eapi/v1`, `/private/ws`).
//!
//! # Architecture & Invariants
//! - **Pure Stateless Execution**: Zero local write-ahead logs, journals, or disk state. Cold boot
//!   and restart recover balances, derivative/option positions, open-order holds, `OrsStpBook`
//!   entries, strategy attribution, and per-strategy `DopState::next_order_seqnum` watermarks
//!   exclusively from authenticated Exchange REST snapshots (`Account + OpenOrders`) paired with
//!   32-character `[0-9a-z]` `SelfDescribingClientId` (`tw1...`) tokens.
//! - **Single-Pass Zero-Allocation Hot Path**: Outbound frames encode on stack scratch slices
//!   (`encode_ws_order_*` + RFC 6455 client masking on Spot/USD-M/Coin-M; `encode_options_http_*`
//!   on Options). Inbound frames scan in one pass via [`BinanceMsgParser`], preserving `Ack`,
//!   `ProjectedFill`, and `Fill` (`TRADE_LITE` `fee = None` followed by `ORDER_TRADE_UPDATE` /
//!   `executionReport` `fee = Some` -> `amendment = true`), `listenKey` rotation via `GuardedValue`,
//!   and `OrsQueryDesk` `GET /.../order` (`recvWindow = 60000`, `6_000 ms` default delay).

#![forbid(unsafe_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tickwire_account::{
    InlineAscii, OrsCancelDispatch, OrsExchangeBootstrapSnapshot, OrsFeeSpec, OrsFillObservation,
    OrsModifyDispatch, OrsNewOrderRequest, OrsPlaceOutcome, OrsQueriedOrderStatus, OrsQueryOrigin,
};
use tickwire_account_gateway::AccountSpec;
use tickwire_binance::auth::SigningKey;
use tickwire_binance::full_market::{
    BinanceVenueMarket, OptionsHttpCancelParams, OptionsHttpNewParams, WsOrderCancelParams,
    WsOrderModifyParams, WsOrderNewParams, WsPerFrameAuth, classify_binance_terminal_reason,
    decorate_listen_key_stream_url, encode_options_http_cancel, encode_options_http_place,
    encode_signed_account_target, encode_signed_open_orders_target,
    encode_signed_order_status_target, encode_ws_order_cancel, encode_ws_order_modify,
    encode_ws_order_place, encode_ws_session_logon, encode_ws_user_stream_subscribe,
    explain_binance_error,
};
use tickwire_binance_account::{
    BinanceBootstrapCatalog, BinanceMsgParser, BinanceRequestKind, DEFAULT_ORDER_RECV_WINDOW_MS,
    DEFAULT_STATUS_QUERY_DELAY_MS, LISTEN_KEY_REFRESH_S, STATUS_QUERY_RECV_WINDOW_MS,
    build_market_bootstrap_snapshot, decode_ws_request_id, encode_ws_request_id, pack_ws_reply_id,
    parse_scaled_i128, parse_step_i64, unpack_ws_reply_id,
};
use tickwire_binance_execution::BinanceHotPathWarmer;
use tickwire_quota::rate_limiter::{IpKey, RateFlags, RateLimitId};
use tickwire_quota::shared::{OrsVenueProfile, VenueRateLimitRow};
use tickwire_runtime::{
    EncodedOrderFrame, LoginReply, OwnedOrderQuerySnapshot, QueryStatusRequest, SessionEndpoint,
    SessionMessageEffects, SessionRequestKind, SessionRestEndpoint, SessionUrlPoolSpec,
    SessionVenueEvent, StatusReplyOutcome, UrlRole, VenueSession,
};
use tickwire_spot_execution::encode_ws_client_frame_into;
use tickwire_spot_runner::UtaEndpointSpec;
use tickwire_transport::json_scan::{JsonScan, parse_u64_ascii};
use tickwire_transport::latency_tracer::{LockDomain, LockId, ThreadTag, TimedSpinLock};
use tickwire_transport::links::{LinkKind, RequestRef};
use tickwire_transport::rest_querier::{
    BuiltRequest, DialSecurity, RestReply, RestStatus, RestTask,
};
use tickwire_transport::{rustls, ws};
use tickwire_venue::{
    ClientIdDomain, DecodedClientId, Environment, OrderType, PositionSide, RefusalReason,
    SelfDescribingClientId, SelfTradePrevention, Side, TerminalReason, TimeInForce, venue_id,
};

const INFLIGHT_RING_CAP: usize = 128;

/// Per-market authentication and endpoint configuration inside [`BinanceVenueSessionConfig`].
pub struct BinanceMarketEndpoint {
    pub market: BinanceVenueMarket,
    pub api_key: String,
    /// Optional Ed25519 private key for post-`101` WebSocket `session.logon` on Spot/USD-M/Coin-M.
    /// When `None` on a WS market, outbound WS frames use per-frame `hmac_signer` signatures.
    pub ed25519_signer: Option<SigningKey>,
    /// HMAC-SHA256 signer used for Options HTTP/1.1 order entry, `StatusAsk` queries, `listenKey`
    /// lifecycle, and cold Exchange REST bootstrap.
    pub hmac_signer: SigningKey,
    pub order_endpoint: UtaEndpointSpec,
    pub stream_endpoint: Option<UtaEndpointSpec>,
    pub rest_endpoint: Option<SessionRestEndpoint>,
    pub stream_colo: bool,
}

/// Startup configuration for [`BinanceVenueSession`].
pub struct BinanceVenueSessionConfig {
    pub environment: Environment,
    pub exchange_uid: u64,
    pub risk_domain_id: [u8; 16],
    pub catalog: Arc<BinanceBootstrapCatalog>,
    pub markets: Vec<BinanceMarketEndpoint>,
    pub order_recv_window_ms: u32,
    pub status_query_delay_ms: u64,
    pub clock_utc_ms: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
    pub bootstrap_override: Option<OrsExchangeBootstrapSnapshot>,
}

impl BinanceVenueSessionConfig {
    pub fn new(
        environment: Environment,
        exchange_uid: u64,
        risk_domain_id: [u8; 16],
        catalog: Arc<BinanceBootstrapCatalog>,
        markets: Vec<BinanceMarketEndpoint>,
    ) -> Self {
        Self {
            environment,
            exchange_uid,
            risk_domain_id,
            catalog,
            markets,
            order_recv_window_ms: DEFAULT_ORDER_RECV_WINDOW_MS,
            status_query_delay_ms: DEFAULT_STATUS_QUERY_DELAY_MS,
            clock_utc_ms: None,
            bootstrap_override: None,
        }
    }
}

#[derive(Clone, Copy)]
struct BinanceInflightTag {
    ors_order_id: u64,
    reply_id: u64,
    symbol_id: u32,
    kind: BinanceRequestKind,
    side: Side,
    tif: TimeInForce,
    post_only: bool,
    price_ticks: i64,
    qty_lots: i64,
}

impl Default for BinanceInflightTag {
    fn default() -> Self {
        Self {
            ors_order_id: 0,
            reply_id: 0,
            symbol_id: 0,
            kind: BinanceRequestKind::Place,
            side: Side::Buy,
            tif: TimeInForce::Gtc,
            post_only: false,
            price_ticks: 0,
            qty_lots: 0,
        }
    }
}

struct BinanceInflightRing {
    entries: [BinanceInflightTag; INFLIGHT_RING_CAP],
    cursor: usize,
}

impl Default for BinanceInflightRing {
    fn default() -> Self {
        Self {
            entries: [BinanceInflightTag::default(); INFLIGHT_RING_CAP],
            cursor: 0,
        }
    }
}

/// Stateless full-market Binance [`VenueSession`] implementation (`Spot`, `Usdm`, `Coinm`, `Options`).
pub struct BinanceVenueSession {
    environment: Environment,
    exchange_uid: u64,
    risk_domain_id: [u8; 16],
    catalog: Arc<BinanceBootstrapCatalog>,
    markets: [Option<BinanceMarketEndpoint>; 4],
    primary_market: BinanceVenueMarket,
    order_recv_window_ms: u32,
    status_query_delay_ms: u64,
    clock_utc_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    inflight_ring: TimedSpinLock<BinanceInflightRing>,
    bootstrap_override: Option<OrsExchangeBootstrapSnapshot>,
}

fn system_utc_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(1_700_000_000_000)
}

#[inline]
fn scaled_atoms_i64(steps: i64, step_atoms: i128) -> Option<i64> {
    if steps <= 0 || step_atoms <= 0 {
        return None;
    }
    let prod = i128::from(steps).checked_mul(step_atoms)?;
    i64::try_from(prod).ok().filter(|&v| v > 0)
}

impl BinanceVenueSession {
    pub fn new(cfg: BinanceVenueSessionConfig) -> Result<Self, String> {
        if cfg.markets.is_empty() {
            return Err(String::from(
                "BinanceVenueSession requires at least one configured market",
            ));
        }
        let primary_market = cfg.markets[0].market;
        let mut slots: [Option<BinanceMarketEndpoint>; 4] = [None, None, None, None];
        for m in cfg.markets {
            if m.api_key.trim().is_empty() {
                return Err(format!("empty api_key for Binance market {:?}", m.market));
            }
            let idx = m.market.index();
            if slots[idx].is_some() {
                return Err(format!(
                    "duplicate BinanceMarketEndpoint for {:?}",
                    m.market
                ));
            }
            slots[idx] = Some(m);
        }
        let clock_utc_ms = cfg.clock_utc_ms.unwrap_or_else(|| Arc::new(system_utc_ms));
        Ok(Self {
            environment: cfg.environment,
            exchange_uid: cfg.exchange_uid,
            risk_domain_id: cfg.risk_domain_id,
            catalog: cfg.catalog,
            markets: slots,
            primary_market,
            order_recv_window_ms: cfg.order_recv_window_ms.max(1),
            status_query_delay_ms: cfg.status_query_delay_ms,
            clock_utc_ms,
            inflight_ring: TimedSpinLock::new(
                BinanceInflightRing::default(),
                LockId::new(LockDomain::GuardedValue, 0x7201),
            ),
            bootstrap_override: cfg.bootstrap_override,
        })
    }

    #[inline]
    pub fn catalog(&self) -> &Arc<BinanceBootstrapCatalog> {
        &self.catalog
    }

    #[inline]
    pub const fn primary_market(&self) -> BinanceVenueMarket {
        self.primary_market
    }

    /// Runs `iterations` side-effect-free [`BinanceHotPathWarmer`] passes across all configured
    /// Binance markets (`Spot`, `Usdm`, `Coinm`, `Options`).
    pub fn warm_hot_path(&self, iterations: u32) -> Result<usize, String> {
        let mut total_bytes = 0usize;
        let base_ns = (self.clock_utc_ms)().saturating_mul(1_000_000);
        for ep in self.markets.iter().flatten() {
            let product = match ep.market {
                BinanceVenueMarket::Spot => tickwire_venue::Product::Spot,
                BinanceVenueMarket::Usdm => tickwire_venue::Product::UsdM,
                BinanceVenueMarket::Coinm => tickwire_venue::Product::CoinM,
                BinanceVenueMarket::Options => tickwire_venue::Product::Option,
            };
            let use_ws = ep.market != BinanceVenueMarket::Options;
            let mut warmer = BinanceHotPathWarmer::new(product, self.environment)
                .map_err(|e| format!("BinanceHotPathWarmer::new failed: {e}"))?;
            for idx in 0..iterations.max(1) {
                total_bytes = total_bytes.saturating_add(
                    warmer
                        .warm_once(use_ws, None, base_ns.saturating_add(u64::from(idx)))
                        .map_err(|e| format!("BinanceHotPathWarmer::warm_once failed: {e}"))?,
                );
            }
        }
        Ok(total_bytes)
    }

    #[inline]
    fn market_ep(&self, market: BinanceVenueMarket) -> Option<&BinanceMarketEndpoint> {
        self.markets[market.index()].as_ref()
    }

    #[inline]
    fn primary_ep(&self) -> &BinanceMarketEndpoint {
        self.markets[self.primary_market.index()]
            .as_ref()
            .expect("primary_market guaranteed present")
    }

    #[inline]
    fn market_from_url_id(&self, url_id: &str) -> BinanceVenueMarket {
        if url_id.contains("usdm") {
            BinanceVenueMarket::Usdm
        } else if url_id.contains("coinm") {
            BinanceVenueMarket::Coinm
        } else if url_id.contains("options") {
            BinanceVenueMarket::Options
        } else if url_id.contains("spot") {
            BinanceVenueMarket::Spot
        } else {
            self.primary_market
        }
    }

    #[inline]
    fn record_inflight(&self, tag: BinanceInflightTag) {
        let mut ring = self.inflight_ring.lock(ThreadTag::REQ);
        let idx = ring.cursor % INFLIGHT_RING_CAP;
        ring.cursor = ring.cursor.wrapping_add(1);
        ring.entries[idx] = tag;
    }

    #[inline]
    fn lookup_inflight(
        &self,
        ors_order_id_hint: Option<u64>,
        reply_id_hint: Option<u64>,
    ) -> Option<BinanceInflightTag> {
        let ring = self.inflight_ring.lock(ThreadTag::RESP);
        if let Some(rid) = reply_id_hint.filter(|&r| r != 0) {
            for idx in 0..INFLIGHT_RING_CAP {
                let pos = ring.cursor.wrapping_add(INFLIGHT_RING_CAP - 1 - idx) % INFLIGHT_RING_CAP;
                let e = ring.entries[pos];
                if e.ors_order_id != 0 && e.reply_id == rid {
                    return Some(e);
                }
            }
        }
        if let Some(oid) = ors_order_id_hint.filter(|&o| o != 0) {
            for idx in 0..INFLIGHT_RING_CAP {
                let pos = ring.cursor.wrapping_add(INFLIGHT_RING_CAP - 1 - idx) % INFLIGHT_RING_CAP;
                let e = ring.entries[pos];
                if e.ors_order_id == oid {
                    return Some(e);
                }
            }
        }
        None
    }

    #[inline]
    fn resolve_client_id(&self, market: BinanceVenueMarket, cid: &str) -> Option<u64> {
        if cid.is_empty() {
            return None;
        }
        let domain = ClientIdDomain {
            risk_domain_id: self.risk_domain_id,
            venue: venue_id::BINANCE,
            environment: self.environment,
            account: self.exchange_uid,
            product: market.primary_product(),
        };
        if let DecodedClientId::Managed(sd) = SelfDescribingClientId::decode_str(cid, &domain)
            && let Some(strat_id) = sd.ors_strat_id()
        {
            return Some((sd.intent_id << 8) | u64::from(strat_id));
        }
        for alt in BinanceVenueMarket::ALL {
            if alt == market {
                continue;
            }
            let alt_dom = ClientIdDomain {
                risk_domain_id: self.risk_domain_id,
                venue: venue_id::BINANCE,
                environment: self.environment,
                account: self.exchange_uid,
                product: alt.primary_product(),
            };
            if let DecodedClientId::Managed(sd) = SelfDescribingClientId::decode_str(cid, &alt_dom)
                && let Some(strat_id) = sd.ors_strat_id()
            {
                return Some((sd.intent_id << 8) | u64::from(strat_id));
            }
        }
        None
    }

    fn derive_projected_price_ticks(
        &self,
        parsed: &tickwire_binance_account::BinanceParsedFrame<'_>,
        spec: &tickwire_account::OrsSymbolSpec,
        cum_lots: i64,
    ) -> Option<i64> {
        if let Some(ap_str) = parsed.avg_price
            && let Some(px) = parse_step_i64(ap_str, spec.price_tick_atoms, spec.price_scale)
            && px > 0
        {
            return Some(px);
        }
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

    fn fetch_cold_rest_get(
        &self,
        ep: &SessionRestEndpoint,
        api_key: &str,
        target: &str,
    ) -> Result<Vec<u8>, String> {
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
            .map_err(|e| format!("cold Binance REST connect {addr} failed: {e}"))?;
        tcp.set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| format!("set_read_timeout failed: {e}"))?;
        tcp.set_write_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| format!("set_write_timeout failed: {e}"))?;

        let req_bytes = format!(
            "GET {target} HTTP/1.1\r\nHost: {}\r\nX-MBX-APIKEY: {api_key}\r\nConnection: close\r\n\r\n",
            ep.host
        )
        .into_bytes();

        match &ep.security {
            DialSecurity::Plain => {
                let mut stream = tcp;
                stream
                    .write_all(&req_bytes)
                    .map_err(|e| format!("cold REST write failed: {e}"))?;
                stream
                    .flush()
                    .map_err(|e| format!("cold REST flush failed: {e}"))?;
                read_http_body(BufReader::new(stream), target)
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
                read_http_body(BufReader::new(tls_stream), target)
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
            "cold Binance REST GET {path} returned HTTP {status_code}: {}",
            String::from_utf8_lossy(&body)
        ));
    }
    Ok(body)
}

fn extract_listen_key(body: &[u8]) -> Option<String> {
    let mut scan = JsonScan::new(body);
    scan.eat_byte(b'{').ok()?;
    let mut first = true;
    loop {
        if scan.eat_byte(b'}').is_ok() {
            break;
        }
        if !first {
            scan.eat_byte(b',').ok()?;
        }
        first = false;
        let key = scan.read_string_raw().ok()?;
        scan.eat_byte(b':').ok()?;
        if key == "listenKey" {
            let val = scan.read_string_raw().ok()?;
            if !val.is_empty() {
                return Some(val.to_owned());
            }
        } else {
            scan.skip_value().ok()?;
        }
    }
    None
}

impl VenueSession for BinanceVenueSession {
    fn venue_profile(&self) -> OrsVenueProfile {
        match self.primary_market {
            BinanceVenueMarket::Spot => OrsVenueProfile::BinanceSpot,
            BinanceVenueMarket::Usdm => OrsVenueProfile::BinanceUsdm,
            BinanceVenueMarket::Coinm => OrsVenueProfile::BinanceCoinm,
            BinanceVenueMarket::Options => OrsVenueProfile::BinanceOptions,
        }
    }

    fn url_pools(&self, _spec: &AccountSpec) -> Result<Vec<SessionUrlPoolSpec>, String> {
        let mut pools = Vec::new();
        for market in BinanceVenueMarket::ALL {
            let Some(m) = self.market_ep(market) else {
                continue;
            };
            let order_kind = if market.uses_https_order_pool() {
                LinkKind::Https
            } else {
                LinkKind::Ws
            };
            let order_role = if market == BinanceVenueMarket::Spot && m.stream_endpoint.is_none() {
                UrlRole::ORDER_AND_STREAM
            } else {
                UrlRole::ORDER
            };
            pools.push(SessionUrlPoolSpec {
                url_id: format!("binance-{}-order", market.as_str()),
                role: order_role,
                kind: order_kind,
                product_mask: market.product_mask(),
                host: m.order_endpoint.host.clone(),
                port: m.order_endpoint.port,
                path: m.order_endpoint.path.clone(),
                security: m.order_endpoint.security.clone(),
                initial_peers: m.order_endpoint.initial_peers.clone(),
            });
            if let Some(s_ep) = &m.stream_endpoint {
                pools.push(SessionUrlPoolSpec {
                    url_id: format!("binance-{}-stream", market.as_str()),
                    role: UrlRole::STREAM,
                    kind: LinkKind::Ws,
                    product_mask: market.product_mask(),
                    host: s_ep.host.clone(),
                    port: s_ep.port,
                    path: s_ep.path.clone(),
                    security: s_ep.security.clone(),
                    initial_peers: s_ep.initial_peers.clone(),
                });
            }
        }
        Ok(pools)
    }

    fn rest_endpoint(&self, _spec: &AccountSpec) -> Option<SessionRestEndpoint> {
        for market in BinanceVenueMarket::ALL {
            if let Some(m) = self.market_ep(market)
                && let Some(ep) = &m.rest_endpoint
            {
                return Some(ep.clone());
            }
        }
        None
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
            .catalog
            .symbol(req.symbol_id)
            .ok_or(RefusalReason::InvalidQuantity)?;
        let market = binding.market;
        let m_ep = self
            .market_ep(market)
            .ok_or(RefusalReason::MarketRejectMarketDown)?;

        let px_atoms = scaled_atoms_i64(req.price_ticks, binding.spec.price_tick_atoms)
            .ok_or(RefusalReason::InvalidPrice)?;
        let qty_atoms = scaled_atoms_i64(req.qty_lots, binding.spec.qty_step_atoms)
            .ok_or(RefusalReason::InvalidQuantity)?;
        let wall_ms = (self.clock_utc_ms)();

        let (frame_len, reply_id) = match market {
            BinanceVenueMarket::Spot | BinanceVenueMarket::Usdm | BinanceVenueMarket::Coinm => {
                let mut id_buf = [0u8; 24];
                let req_id = encode_ws_request_id(
                    placed.ors_order_id,
                    BinanceRequestKind::Place,
                    &mut id_buf,
                );
                let per_frame_auth = if m_ep.ed25519_signer.is_none() {
                    Some(WsPerFrameAuth {
                        api_key: &m_ep.api_key,
                        signer: &m_ep.hmac_signer,
                    })
                } else {
                    None
                };
                let mut json_buf = [0u8; 1536];
                let json_len = encode_ws_order_place(
                    &WsOrderNewParams {
                        market,
                        request_id: req_id,
                        symbol: &binding.venue_symbol,
                        client_order_id: placed.client_id.as_str(),
                        side: req.side,
                        order_type: OrderType::Limit,
                        tif: req.tif,
                        post_only: req.post_only,
                        reduce_only: req.reduce_only,
                        position_side: PositionSide::Both,
                        stp: SelfTradePrevention::Default,
                        price_ticks: px_atoms,
                        price_scale: binding.spec.price_scale,
                        qty_lots: qty_atoms,
                        qty_scale: binding.spec.qty_scale,
                        timestamp_ms: wall_ms,
                        recv_window_ms: Some(u64::from(self.order_recv_window_ms)),
                        per_frame_auth,
                    },
                    &mut json_buf,
                )
                .map_err(|_| RefusalReason::InvalidQuantity)?;
                let rid = pack_ws_reply_id(placed.ors_order_id, BinanceRequestKind::Place);
                let flen = encode_ws_client_frame_into(
                    ws::Opcode::Text,
                    &json_buf[..json_len],
                    rid ^ req.now_mono_ns,
                    scratch,
                )
                .map_err(|_| RefusalReason::AbortedBeforeSend)?;
                (flen, rid)
            }
            BinanceVenueMarket::Options => {
                let host = if m_ep.order_endpoint.host.is_empty() {
                    "eapi.binance.com"
                } else {
                    m_ep.order_endpoint.host.as_str()
                };
                let flen = encode_options_http_place(
                    &OptionsHttpNewParams {
                        host,
                        api_key: &m_ep.api_key,
                        hmac_signer: &m_ep.hmac_signer,
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
                        timestamp_ms: wall_ms,
                        recv_window_ms: Some(u64::from(self.order_recv_window_ms)),
                    },
                    scratch,
                )
                .map_err(|_| RefusalReason::InvalidQuantity)?;
                (flen, 0)
            }
        };

        if placed.should_send_to_venue {
            self.record_inflight(BinanceInflightTag {
                ors_order_id: placed.ors_order_id,
                reply_id,
                symbol_id: req.symbol_id,
                kind: BinanceRequestKind::Place,
                side: req.side,
                tif: req.tif,
                post_only: req.post_only,
                price_ticks: req.price_ticks,
                qty_lots: req.qty_lots,
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
            .catalog
            .symbol(cancel.symbol_id)
            .ok_or(RefusalReason::InvalidQuantity)?;
        let market = binding.market;
        let m_ep = self
            .market_ep(market)
            .ok_or(RefusalReason::MarketRejectMarketDown)?;

        let orig_cid = (!cancel.client_id.is_empty()).then_some(cancel.client_id.as_str());
        let venue_oid =
            parse_u64_ascii(cancel.venue_order_id.as_str().as_bytes()).filter(|&v| v > 0);
        let wall_ms = (self.clock_utc_ms)();

        let (frame_len, reply_id) = match market {
            BinanceVenueMarket::Spot | BinanceVenueMarket::Usdm | BinanceVenueMarket::Coinm => {
                let mut id_buf = [0u8; 24];
                let req_id = encode_ws_request_id(
                    cancel.ors_order_id,
                    BinanceRequestKind::Cancel,
                    &mut id_buf,
                );
                let per_frame_auth = if m_ep.ed25519_signer.is_none() {
                    Some(WsPerFrameAuth {
                        api_key: &m_ep.api_key,
                        signer: &m_ep.hmac_signer,
                    })
                } else {
                    None
                };
                let mut json_buf = [0u8; 1024];
                let json_len = encode_ws_order_cancel(
                    &WsOrderCancelParams {
                        market,
                        request_id: req_id,
                        symbol: &binding.venue_symbol,
                        orig_client_order_id: orig_cid,
                        venue_order_id: venue_oid,
                        timestamp_ms: wall_ms,
                        recv_window_ms: Some(u64::from(self.order_recv_window_ms)),
                        per_frame_auth,
                    },
                    &mut json_buf,
                )
                .map_err(|_| RefusalReason::InvalidQuantity)?;
                let rid = pack_ws_reply_id(cancel.ors_order_id, BinanceRequestKind::Cancel);
                let flen = encode_ws_client_frame_into(
                    ws::Opcode::Text,
                    &json_buf[..json_len],
                    rid ^ cancel.ors_order_id,
                    scratch,
                )
                .map_err(|_| RefusalReason::AbortedBeforeSend)?;
                (flen, rid)
            }
            BinanceVenueMarket::Options => {
                let host = if m_ep.order_endpoint.host.is_empty() {
                    "eapi.binance.com"
                } else {
                    m_ep.order_endpoint.host.as_str()
                };
                let flen = encode_options_http_cancel(
                    &OptionsHttpCancelParams {
                        host,
                        api_key: &m_ep.api_key,
                        hmac_signer: &m_ep.hmac_signer,
                        symbol: &binding.venue_symbol,
                        client_order_id: orig_cid,
                        venue_order_id: venue_oid,
                        timestamp_ms: wall_ms,
                        recv_window_ms: Some(u64::from(self.order_recv_window_ms)),
                    },
                    scratch,
                )
                .map_err(|_| RefusalReason::InvalidQuantity)?;
                (flen, 0)
            }
        };

        self.record_inflight(BinanceInflightTag {
            ors_order_id: cancel.ors_order_id,
            reply_id,
            symbol_id: cancel.symbol_id,
            kind: BinanceRequestKind::Cancel,
            side: Side::Buy,
            tif: TimeInForce::Gtc,
            post_only: false,
            price_ticks: 0,
            qty_lots: 0,
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
            .catalog
            .symbol(modify.symbol_id)
            .ok_or(RefusalReason::InvalidQuantity)?;
        let market = binding.market;
        if !market.uses_ws_order_pool() {
            return Err(RefusalReason::MKT_REJECT);
        }
        let m_ep = self
            .market_ep(market)
            .ok_or(RefusalReason::MarketRejectMarketDown)?;

        let prev_tag = self.lookup_inflight(Some(modify.ors_order_id), None);
        let side = prev_tag.map(|t| t.side).unwrap_or(Side::Buy);
        let px_atoms = scaled_atoms_i64(modify.new_price_ticks, binding.spec.price_tick_atoms)
            .ok_or(RefusalReason::InvalidPrice)?;
        let qty_atoms = scaled_atoms_i64(modify.new_qty_lots, binding.spec.qty_step_atoms)
            .ok_or(RefusalReason::InvalidQuantity)?;

        let orig_cid = (!client_id.is_empty()).then_some(client_id);
        let venue_oid = parse_u64_ascii(venue_order_id.as_bytes()).filter(|&v| v > 0);
        let mut id_buf = [0u8; 24];
        let req_id =
            encode_ws_request_id(modify.ors_order_id, BinanceRequestKind::Modify, &mut id_buf);
        let per_frame_auth = if m_ep.ed25519_signer.is_none() {
            Some(WsPerFrameAuth {
                api_key: &m_ep.api_key,
                signer: &m_ep.hmac_signer,
            })
        } else {
            None
        };

        let mut json_buf = [0u8; 1536];
        let json_len = encode_ws_order_modify(
            &WsOrderModifyParams {
                market,
                request_id: req_id,
                symbol: &binding.venue_symbol,
                orig_client_order_id: orig_cid,
                venue_order_id: venue_oid,
                new_client_order_id: orig_cid,
                side,
                new_price_ticks: px_atoms,
                price_scale: binding.spec.price_scale,
                new_qty_lots: qty_atoms,
                qty_scale: binding.spec.qty_scale,
                timestamp_ms: (self.clock_utc_ms)(),
                recv_window_ms: Some(u64::from(self.order_recv_window_ms)),
                per_frame_auth,
            },
            &mut json_buf,
        )
        .map_err(|_| RefusalReason::InvalidPrice)?;

        let rid = pack_ws_reply_id(modify.ors_order_id, BinanceRequestKind::Modify);
        let frame_len = encode_ws_client_frame_into(
            ws::Opcode::Text,
            &json_buf[..json_len],
            rid ^ modify.ors_order_id,
            scratch,
        )
        .map_err(|_| RefusalReason::AbortedBeforeSend)?;

        self.record_inflight(BinanceInflightTag {
            ors_order_id: modify.ors_order_id,
            reply_id: rid,
            symbol_id: modify.symbol_id,
            kind: BinanceRequestKind::Modify,
            side,
            tif: prev_tag.map(|t| t.tif).unwrap_or(TimeInForce::Gtc),
            post_only: prev_tag.is_some_and(|t| t.post_only),
            price_ticks: modify.new_price_ticks,
            qty_lots: modify.new_qty_lots,
        });

        Ok(EncodedOrderFrame {
            frame_len,
            reply_or_seq_id: rid,
        })
    }

    fn connection_url(
        &self,
        _spec: &AccountSpec,
        pool: &SessionUrlPoolSpec,
        stream_token: Option<&[u8]>,
    ) -> Result<SessionEndpoint, String> {
        let market = self.market_from_url_id(&pool.url_id);
        let path = if pool.role.is_stream() && market.needs_listen_key() {
            let tok = stream_token
                .and_then(|b| core::str::from_utf8(b).ok())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| String::from("Binance listenKey not ready"))?;
            let colo = self.market_ep(market).is_some_and(|m| m.stream_colo);
            let mut decorated = [0u8; 512];
            let len = decorate_listen_key_stream_url(&pool.path, colo, tok, &mut decorated)
                .map_err(|_| String::from("decorate_listen_key_stream_url failed"))?;
            core::str::from_utf8(&decorated[..len])
                .map_err(|_| String::from("invalid UTF-8 listenKey URL"))?
                .to_owned()
        } else {
            pool.path.clone()
        };
        Ok(SessionEndpoint {
            host: pool.host.clone(),
            port: pool.port,
            path,
            security: pool.security.clone(),
            initial_peers: pool.initial_peers.clone(),
        })
    }

    fn encode_login(
        &self,
        _spec: &AccountSpec,
        _role: UrlRole,
        _seq: u32,
        _out: &mut [u8],
    ) -> Result<usize, String> {
        Ok(0)
    }

    fn awaits_login_reply(&self, _role: UrlRole) -> bool {
        false
    }

    fn login_reply(&self, payload: &[u8], _role: UrlRole) -> LoginReply {
        let Ok(parsed) = BinanceMsgParser::parse(payload) else {
            return LoginReply::Other;
        };
        if parsed.request_id == Some("twlogon1") {
            if parsed.has_error() {
                LoginReply::Fail
            } else {
                LoginReply::Ok
            }
        } else {
            LoginReply::Other
        }
    }

    fn encode_subscribe(
        &self,
        _spec: &AccountSpec,
        role: UrlRole,
        step: usize,
        out: &mut [u8],
    ) -> Result<usize, String> {
        if !role.is_order() {
            // Dedicated private user-data stream sockets on USD-M/Coin-M/Options authenticate via URL listenKey.
            return Ok(0);
        }
        let m_ep = self.primary_ep();
        let Some(signer) = m_ep.ed25519_signer.as_ref() else {
            return Ok(0);
        };
        match step {
            0 => encode_ws_session_logon(
                "twlogon1",
                &m_ep.api_key,
                (self.clock_utc_ms)(),
                Some(u64::from(self.order_recv_window_ms)),
                signer,
                m_ep.market,
                out,
            )
            .map_err(|e| format!("encode_ws_session_logon failed: {e:?}")),
            1 if role.is_stream() && self.market_ep(BinanceVenueMarket::Spot).is_some() => {
                encode_ws_user_stream_subscribe("twsub1", out)
                    .map_err(|e| format!("encode_ws_user_stream_subscribe failed: {e:?}"))
            }
            _ => Ok(0),
        }
    }

    fn reply_id(&self, payload: &[u8]) -> Option<u64> {
        let parsed = BinanceMsgParser::parse(payload).ok()?;
        let (oid, kind) = parsed.request_id.and_then(decode_ws_request_id)?;
        Some(pack_ws_reply_id(oid, kind))
    }

    fn on_message(
        &self,
        account_ref: u8,
        url_id: &str,
        _role: UrlRole,
        _conn_id: u16,
        hint: Option<RequestRef>,
        payload: &[u8],
        now_mono_ns: u64,
        effects: &mut SessionMessageEffects,
    ) {
        let Ok(parsed) = BinanceMsgParser::parse(payload) else {
            return;
        };
        let market = self.market_from_url_id(url_id);

        for row in parsed.rate_limits_slice() {
            if let (Some(limit), Some(count), Some((_, interval_s))) =
                (row.limit, row.count, row.interval_spec())
                && limit > 0
            {
                let flags = if row.rate_limit_type == Some("ORDERS") {
                    RateFlags::PER_ACCOUNT.union(RateFlags::ORDER)
                } else {
                    RateFlags::PER_ACCOUNT.union(RateFlags::REQUEST)
                };
                effects.rate_limits.push(VenueRateLimitRow {
                    id: RateLimitId {
                        local_ip: IpKey::NONE,
                        interval_s,
                        account: u64::from(account_ref),
                        flags,
                        symbol: 0,
                    },
                    count,
                    limit,
                });
            }
        }

        if parsed.event_type == Some("listenKeyExpired") {
            effects.stream_token_expired = true;
            return;
        }

        let ws_decoded = parsed.request_id.and_then(decode_ws_request_id);
        let hint_oid = hint.map(|h| h.order_key).filter(|&k| k != 0);
        let (ors_order_id_opt, req_kind_opt) = if let Some((oid, k)) = ws_decoded {
            (Some(oid), Some(k))
        } else if let Some(oid) = hint_oid {
            let tag = self.lookup_inflight(Some(oid), None);
            (Some(oid), tag.map(|t| t.kind))
        } else {
            let mut found = None;
            for cid in parsed.client_id_candidates().into_iter().flatten() {
                if let Some(oid) = self.resolve_client_id(market, cid) {
                    found = Some(oid);
                    break;
                }
            }
            (found, None)
        };

        let Some(ors_order_id) = ors_order_id_opt else {
            return;
        };
        let tag_opt = self.lookup_inflight(
            Some(ors_order_id),
            ws_decoded.map(|(o, k)| pack_ws_reply_id(o, k)),
        );

        if parsed.has_error() {
            let code = parsed.error_code.unwrap_or(-1);
            let msg = parsed.error_msg.unwrap_or("http_error");
            let order_tif = tag_opt.map(|t| t.tif);
            let order_post_only = tag_opt.is_some_and(|t| t.post_only);
            let reason = explain_binance_error(code, msg, order_tif, order_post_only);
            let eff_kind = req_kind_opt
                .or_else(|| tag_opt.map(|t| t.kind))
                .unwrap_or(BinanceRequestKind::Place);

            match eff_kind {
                BinanceRequestKind::Place | BinanceRequestKind::Other => {
                    if code == -1007 || code == -1000 || reason == TerminalReason::MKT_DELAYED {
                        effects.query_status.push(QueryStatusRequest {
                            ors_order_id,
                            origin: OrsQueryOrigin::Place,
                        });
                    } else {
                        effects.events.push(SessionVenueEvent::Rejected {
                            ors_order_id,
                            error_code: Some(code),
                            status_or_msg: InlineAscii::lossy_ascii(msg),
                            explicit_reason: Some(reason),
                            received_mono_ns: now_mono_ns,
                        });
                    }
                }
                BinanceRequestKind::Cancel => {
                    let desk_code = if code == -1007
                        || code == -1000
                        || reason == TerminalReason::MKT_DELAYED
                    {
                        Some(-1007)
                    } else {
                        Some(code)
                    };
                    effects.events.push(SessionVenueEvent::CancelRejected {
                        ors_order_id,
                        error_code: desk_code,
                        reason,
                        received_mono_ns: now_mono_ns,
                    });
                }
                BinanceRequestKind::Modify => {
                    effects.events.push(SessionVenueEvent::ModifyRejected {
                        ors_order_id,
                        reason,
                        received_mono_ns: now_mono_ns,
                    });
                }
            }
            return;
        }

        let symbol_id = tag_opt
            .map(|t| t.symbol_id)
            .or_else(|| {
                parsed
                    .symbol
                    .and_then(|s| self.catalog.find_symbol_id(market, s))
            })
            .or_else(|| {
                let s = parsed.symbol?;
                for alt in BinanceVenueMarket::ALL {
                    if let Some(sid) = self.catalog.find_symbol_id(alt, s) {
                        return Some(sid);
                    }
                }
                None
            });
        let Some(symbol_id) = symbol_id else {
            return;
        };
        let Some(binding) = self.catalog.symbol(symbol_id) else {
            return;
        };
        let spec = &binding.spec;
        let sym_market = binding.market;

        let vid_inline = parsed
            .venue_order_id
            .filter(|v| !v.is_empty())
            .map(InlineAscii::<64>::lossy_ascii);

        // 1. Granular trade execution (`TRADE_LITE` fast fill or `TRADE` with commission/rebate).
        let is_trade_lite = parsed.event_type == Some("TRADE_LITE");
        let valid_tid = parsed.valid_trade_id();
        let has_trade = is_trade_lite
            || parsed.execution_type == Some("TRADE")
            || (valid_tid.is_some()
                && parsed
                    .last_executed_qty
                    .is_some_and(|s| s != "0" && s != "0.00000000"));

        let mut emitted_trade = false;
        if has_trade
            && let (Some(qty_lots), Some(px_ticks)) = (
                parsed.parse_last_qty_lots(spec.qty_step_atoms, spec.qty_scale),
                parsed.parse_last_price_ticks(spec.price_tick_atoms, spec.price_scale),
            )
            && qty_lots > 0
            && px_ticks > 0
        {
            emitted_trade = true;
            let side = match parsed.side {
                Some("BUY") => Side::Buy,
                Some("SELL") => Side::Sell,
                _ => tag_opt.map(|t| t.side).unwrap_or(Side::Buy),
            };
            let fee = if is_trade_lite {
                None
            } else if let Some(comm_str) = parsed.commission {
                let (fee_key, fee_scale) = parsed
                    .commission_asset
                    .and_then(|code| self.catalog.find_asset(sym_market, code))
                    .unwrap_or_else(|| spec.hold_asset_for_side(Side::Buy));
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

            effects.events.push(SessionVenueEvent::Ack {
                ors_order_id,
                venue_order_id: vid_inline,
                ack_price_ticks: None,
                ack_qty_lots: None,
                received_mono_ns: now_mono_ns,
            });
            effects
                .events
                .push(SessionVenueEvent::Fill(OrsFillObservation {
                    ors_order_id,
                    account_ref,
                    symbol_id,
                    side,
                    liquidity: parsed.liquidity(),
                    exec_ref,
                    trade_id: tid_ascii,
                    qty_lots,
                    price_ticks: px_ticks,
                    fee,
                    received_mono_ns: now_mono_ns,
                }));
        }

        // 2. In-place `AMENDMENT` / `order.modify` / `order.cancelReplace`.
        if parsed.execution_type == Some("AMENDMENT")
            || req_kind_opt == Some(BinanceRequestKind::Modify)
        {
            let px = parsed
                .parse_price_ticks(spec.price_tick_atoms, spec.price_scale)
                .or_else(|| tag_opt.map(|t| t.price_ticks))
                .filter(|&v| v > 0);
            let qty = parsed
                .parse_orig_qty_lots(spec.qty_step_atoms, spec.qty_scale)
                .or_else(|| tag_opt.map(|t| t.qty_lots))
                .filter(|&v| v > 0);
            if let (Some(px_ticks), Some(qty_lots)) = (px, qty) {
                effects.events.push(SessionVenueEvent::Modified {
                    ors_order_id,
                    price_ticks: px_ticks,
                    qty_lots,
                    venue_order_id: vid_inline,
                    received_mono_ns: now_mono_ns,
                });
            }
            return;
        }

        // 3. `Ack` (`NEW` / `PARTIALLY_FILLED` / `FILLED` / `ACCEPTED`).
        let status_str = parsed.order_status.unwrap_or("");
        if matches!(
            status_str,
            "NEW" | "PARTIALLY_FILLED" | "FILLED" | "ACCEPTED"
        ) {
            let ack_px = parsed
                .parse_price_ticks(spec.price_tick_atoms, spec.price_scale)
                .filter(|&v| v > 0);
            let ack_qty = parsed
                .parse_orig_qty_lots(spec.qty_step_atoms, spec.qty_scale)
                .filter(|&v| v > 0);
            effects.events.push(SessionVenueEvent::Ack {
                ors_order_id,
                venue_order_id: vid_inline,
                ack_price_ticks: ack_px,
                ack_qty_lots: ack_qty,
                received_mono_ns: now_mono_ns,
            });
        }

        // 4. `ProjectedFill` when `executedQty` advances ahead of granular trade frames.
        if !emitted_trade
            && let Some(cum_lots) =
                parsed.parse_executed_qty_lots(spec.qty_step_atoms, spec.qty_scale)
            && cum_lots > 0
            && let Some(proj_px) = self
                .derive_projected_price_ticks(&parsed, spec, cum_lots)
                .or_else(|| tag_opt.map(|t| t.price_ticks).filter(|&p| p > 0))
        {
            effects.events.push(SessionVenueEvent::ProjectedFill {
                ors_order_id,
                cum_qty_lots: cum_lots,
                price_ticks: proj_px,
                received_mono_ns: now_mono_ns,
            });
        }

        // 5. Terminal `CANCELED` / `EXPIRED` / `EXPIRED_IN_MATCH` / `REJECTED`.
        match status_str {
            "REJECTED" => {
                let explicit = classify_binance_terminal_reason(
                    market,
                    status_str,
                    parsed.execution_type.unwrap_or(""),
                    parsed.time_in_force.unwrap_or(""),
                    tag_opt.map(|t| t.tif),
                    tag_opt.is_some_and(|t| t.post_only),
                    parsed.expired_reason,
                    0,
                    false,
                );
                effects.events.push(SessionVenueEvent::Rejected {
                    ors_order_id,
                    error_code: parsed.error_code,
                    status_or_msg: InlineAscii::lossy_ascii(parsed.error_msg.unwrap_or("REJECTED")),
                    explicit_reason: Some(explicit),
                    received_mono_ns: now_mono_ns,
                });
            }
            "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" => {
                let cum_lots_opt =
                    parsed.parse_executed_qty_lots(spec.qty_step_atoms, spec.qty_scale);
                let stp_or_reason = parsed
                    .expired_reason
                    .or(parsed.execution_type)
                    .map(InlineAscii::<32>::lossy_ascii);
                let explicit = if status_str == "CANCELED" {
                    None
                } else {
                    Some(classify_binance_terminal_reason(
                        market,
                        status_str,
                        parsed.execution_type.unwrap_or(""),
                        parsed.time_in_force.unwrap_or(""),
                        tag_opt.map(|t| t.tif),
                        tag_opt.is_some_and(|t| t.post_only),
                        parsed.expired_reason,
                        0,
                        false,
                    ))
                };
                effects.events.push(SessionVenueEvent::Canceled {
                    ors_order_id,
                    cum_qty_lots: cum_lots_opt,
                    error_code: parsed.error_code,
                    status_or_msg: InlineAscii::lossy_ascii(status_str),
                    stp_mode_or_reason: stp_or_reason,
                    explicit_reason: explicit,
                    received_mono_ns: now_mono_ns,
                });
            }
            _ => {}
        }
    }

    fn has_stream_token(&self) -> bool {
        for market in BinanceVenueMarket::ALL {
            if market.needs_listen_key()
                && let Some(m) = self.market_ep(market)
                && m.stream_endpoint.is_some()
            {
                return true;
            }
        }
        false
    }

    fn stream_token_refresh_s(&self) -> u64 {
        u64::from(LISTEN_KEY_REFRESH_S)
    }

    fn stream_token_query(
        &self,
        _spec: &AccountSpec,
        refresh: bool,
        _current_token: Option<&[u8]>,
    ) -> Option<BuiltRequest> {
        for market in BinanceVenueMarket::ALL {
            if market.needs_listen_key()
                && let Some(m) = self.market_ep(market)
                && let Some(path) = market.listen_key_path()
            {
                return Some(BuiltRequest {
                    method: String::from(if refresh { "PUT" } else { "POST" }),
                    path: path.to_owned(),
                    headers: vec![(String::from("X-MBX-APIKEY"), m.api_key.clone())],
                    body: Vec::new(),
                });
            }
        }
        None
    }

    fn on_stream_token_reply(&self, body: &[u8], refresh: bool) -> Result<Option<Vec<u8>>, String> {
        if let Some(key) = extract_listen_key(body) {
            return Ok(Some(key.into_bytes()));
        }
        if refresh {
            Ok(None)
        } else {
            Err(String::from(
                "missing listenKey in Binance stream-token response",
            ))
        }
    }

    fn open_orders_queries(&self, _spec: &AccountSpec) -> Vec<BuiltRequest> {
        let mut out = Vec::new();
        let wall_ms = (self.clock_utc_ms)();
        for market in BinanceVenueMarket::ALL {
            let Some(m) = self.market_ep(market) else {
                continue;
            };
            let mut target_buf = [0u8; 768];
            if let Ok(len) = encode_signed_open_orders_target(
                market,
                None,
                u64::from(STATUS_QUERY_RECV_WINDOW_MS),
                wall_ms,
                &m.hmac_signer,
                &mut target_buf,
            ) && let Ok(target) = core::str::from_utf8(&target_buf[..len])
            {
                out.push(BuiltRequest {
                    method: String::from("GET"),
                    path: target.to_owned(),
                    headers: vec![(String::from("X-MBX-APIKEY"), m.api_key.clone())],
                    body: Vec::new(),
                });
            }
        }
        out
    }

    fn order_status_query(
        &self,
        _spec: &AccountSpec,
        _ors_order_id: u64,
        symbol_id: u32,
        client_id: &str,
        _venue_order_id: &str,
        _kind: SessionRequestKind,
    ) -> Option<BuiltRequest> {
        if client_id.is_empty() {
            return None;
        }
        let binding = self.catalog.symbol(symbol_id)?;
        let m = self.market_ep(binding.market)?;
        let mut target_buf = [0u8; 768];
        let len = encode_signed_order_status_target(
            binding.market,
            &binding.venue_symbol,
            client_id,
            u64::from(STATUS_QUERY_RECV_WINDOW_MS),
            (self.clock_utc_ms)(),
            &m.hmac_signer,
            &mut target_buf,
        )
        .ok()?;
        let target = core::str::from_utf8(&target_buf[..len]).ok()?;
        Some(BuiltRequest {
            method: String::from("GET"),
            path: target.to_owned(),
            headers: vec![(String::from("X-MBX-APIKEY"), m.api_key.clone())],
            body: Vec::new(),
        })
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
        if !matches!(reply.status, RestStatus::Ok | RestStatus::HttpError)
            || reply.http_code == 0
            || reply.http_code >= 500
            || reply.http_code == 429
        {
            return StatusReplyOutcome {
                snapshot: None,
                retry_query: true,
                rate_limits: Vec::new(),
            };
        }
        let Some(binding) = self.catalog.symbol(symbol_id) else {
            return StatusReplyOutcome {
                snapshot: None,
                retry_query: false,
                rate_limits: Vec::new(),
            };
        };
        let Ok(parsed) = BinanceMsgParser::parse(&reply.body) else {
            return StatusReplyOutcome {
                snapshot: None,
                retry_query: true,
                rate_limits: Vec::new(),
            };
        };
        if let Some(code) = parsed.error_code
            && code != 0
        {
            if code == -2013 {
                return StatusReplyOutcome {
                    snapshot: Some(OwnedOrderQuerySnapshot {
                        ors_order_id,
                        account_ref,
                        symbol_id,
                        status: OrsQueriedOrderStatus::NotFound,
                        venue_order_id: None,
                        price_ticks: None,
                        orig_qty_lots: None,
                        cum_filled_lots: 0,
                        avg_fill_price_ticks: None,
                        trades: Vec::new(),
                        received_mono_ns: now_mono_ns,
                    }),
                    retry_query: false,
                    rate_limits: Vec::new(),
                };
            }
            return StatusReplyOutcome {
                snapshot: None,
                retry_query: true,
                rate_limits: Vec::new(),
            };
        }

        let status = match parsed.order_status.unwrap_or("") {
            "NEW" | "ACCEPTED" => OrsQueriedOrderStatus::Open,
            "PARTIALLY_FILLED" => OrsQueriedOrderStatus::PartiallyFilled,
            "FILLED" => OrsQueriedOrderStatus::Filled,
            "CANCELED" | "REJECTED" => OrsQueriedOrderStatus::Canceled,
            "EXPIRED" | "EXPIRED_IN_MATCH" => OrsQueriedOrderStatus::Expired,
            _ => {
                return StatusReplyOutcome {
                    snapshot: None,
                    retry_query: true,
                    rate_limits: Vec::new(),
                };
            }
        };

        let spec = &binding.spec;
        let cum_filled_lots = parsed
            .parse_executed_qty_lots(spec.qty_step_atoms, spec.qty_scale)
            .unwrap_or(0)
            .max(0);
        let avg_fill_price_ticks =
            self.derive_projected_price_ticks(&parsed, spec, cum_filled_lots);
        let price_ticks = parsed
            .parse_price_ticks(spec.price_tick_atoms, spec.price_scale)
            .filter(|&v| v > 0);
        let orig_qty_lots = parsed
            .parse_orig_qty_lots(spec.qty_step_atoms, spec.qty_scale)
            .filter(|&v| v > 0);
        let venue_order_id = parsed
            .venue_order_id
            .filter(|v| !v.is_empty())
            .map(InlineAscii::lossy_ascii);

        let _ = unpack_ws_reply_id;
        StatusReplyOutcome {
            snapshot: Some(OwnedOrderQuerySnapshot {
                ors_order_id,
                account_ref,
                symbol_id,
                status,
                venue_order_id,
                price_ticks,
                orig_qty_lots,
                cum_filled_lots,
                avg_fill_price_ticks,
                trades: Vec::new(),
                received_mono_ns: now_mono_ns,
            }),
            retry_query: false,
            rate_limits: Vec::new(),
        }
    }

    fn bootstrap_snapshots(
        &self,
        _spec: &AccountSpec,
    ) -> Result<OrsExchangeBootstrapSnapshot, String> {
        if let Some(snap) = &self.bootstrap_override {
            return Ok(snap.clone());
        }
        let mut combined_balances = Vec::new();
        let mut combined_positions = Vec::new();
        let mut combined_open_orders = Vec::new();
        let mut balances_include_locks = true;

        let wall_ms = (self.clock_utc_ms)();
        for market in BinanceVenueMarket::ALL {
            let Some(m) = self.market_ep(market) else {
                continue;
            };
            let Some(rest_ep) = &m.rest_endpoint else {
                continue;
            };
            if rest_ep.port == 0 && rest_ep.initial_peers.is_empty() {
                continue;
            }

            let mut acct_target = [0u8; 512];
            let acct_len = encode_signed_account_target(
                market,
                u64::from(STATUS_QUERY_RECV_WINDOW_MS),
                wall_ms,
                &m.hmac_signer,
                &mut acct_target,
            )
            .map_err(|e| format!("encode_signed_account_target({market:?}) failed: {e:?}"))?;
            let acct_path = core::str::from_utf8(&acct_target[..acct_len])
                .map_err(|_| String::from("non-UTF8 account target"))?;

            let mut oo_target = [0u8; 512];
            let oo_len = encode_signed_open_orders_target(
                market,
                None,
                u64::from(STATUS_QUERY_RECV_WINDOW_MS),
                wall_ms,
                &m.hmac_signer,
                &mut oo_target,
            )
            .map_err(|e| format!("encode_signed_open_orders_target({market:?}) failed: {e:?}"))?;
            let oo_path = core::str::from_utf8(&oo_target[..oo_len])
                .map_err(|_| String::from("non-UTF8 openOrders target"))?;

            let acct_body = self.fetch_cold_rest_get(rest_ep, &m.api_key, acct_path)?;
            let oo_body = self.fetch_cold_rest_get(rest_ep, &m.api_key, oo_path)?;

            let snap = build_market_bootstrap_snapshot(
                market,
                &self.catalog,
                &acct_body,
                &oo_body,
                None,
                0,
            )
            .map_err(|e| format!("build_market_bootstrap_snapshot({market:?}) failed: {e:?}"))?;

            balances_include_locks &= snap.balances_include_open_order_locks;
            combined_balances.extend(snap.balances);
            combined_positions.extend(snap.positions);
            combined_open_orders.extend(snap.open_orders);
        }

        Ok(OrsExchangeBootstrapSnapshot {
            balances: combined_balances,
            positions: combined_positions,
            open_orders: combined_open_orders,
            balances_include_open_order_locks: balances_include_locks,
        })
    }
}
