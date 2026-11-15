//! Stateless full-product Unified Trading Account execution sessions (`UtaAccountSession`,
//! `BybitUtaSession`, and `OkxUtaSession`) for Bybit V5 and OKX V5.
//!
//! Covers all 6 Tickwire UTA products (`Spot`, `LinearPerp`, `InversePerp`, `LinearFutures`,
//! `InverseFutures`, `Options`), Retail Price Improvement (`TimeInForce::Rpi`), optional Options /
//! Block `RFQ` quoting (`RfqQuoteHook`), shared multi-IP connection pools (`OrderRoute.product_mask = 0`),
//! single-pick `SharedRateLimiter` admission (`pick -> spend -> NACK immediately`), zero-allocation
//! single-pass `JsonScan`, `execution.fast` -> `execution` fee amendment (`amendment = true`), and
//! server-maintenance `notice` graceful connection switchover with zero dropped open orders.
//!
//! # Concurrency & Lock Discipline (`Send + Sync`)
//!
//! `UtaAccountSession<CHECK_STP>` is `Send + Sync` and exposes `&self` across all `REQ` (`submit_*`)
//! and `RESP` (`poll_resp`, `apply_*`, `poll_due_queries`) entry points so the dedicated `REQ` and
//! `RESP` threads operate concurrently with zero coarse session locks and zero `std::sync::Mutex`:
//! - `Order` (`OrderManager::lock_order`) and `Connection` (`LockedConnection`) spinlocks never
//!   overlap.
//! - Every helper spinlock (`slot.tracked`, `slot.resp_rx`, `private_stream_lock`, `router_lock`,
//!   `publish_lock`, `venue_id_cache_lock`, `UtaRfqQuoteBook`) is a leaf `TimedSpinLock` held only
//!   for fixed-size scalar/buffer swaps with zero heap allocation, zero logging, and zero protocol
//!   parsing or wire formatting inside any critical section.

// `UtaSubmitRefusal` returns `Option<OrsReport>` inline on the stack so `REQ` refusals never allocate.
#![allow(clippy::result_large_err)]

use crate::{
    Environment, NoopRfqQuoteHook, RfqCancelQuotesRequest, RfqQuoteHook, RfqQuoteRequest,
    SpotIssueKind, SpotVenue, UtaAssetBinding, UtaCancelWireInput, UtaCommandReply, UtaExecRow,
    UtaFrameEvent, UtaModifyWireInput, UtaNewOrderWireInput, UtaOrderParams, UtaOrderRow,
    UtaSymbolBinding, UtaVenueError, UtaWalletCoinRow, check_bybit_auth_reply,
    check_okx_login_reply, classify_issue, encode_bybit_auth_into, encode_bybit_cancel_arg,
    encode_bybit_modify_arg, encode_bybit_new_order_arg, encode_bybit_ws_trade_envelope_into,
    encode_okx_cancel_arg, encode_okx_login_into, encode_okx_modify_arg, encode_okx_new_order_arg,
    encode_okx_subscribe_step_into, encode_okx_ws_trade_envelope_into,
    encode_signed_https_post_into, encode_ws_client_frame_into, encode_ws_ping_into,
    instrument_rate_scope, is_http_account_hazard, parse_positive_step_i64,
    parse_signed_scaled_i128,
    private::stream::UtaPrivateStream,
    rest_bootstrap::{decode_status_ask_snapshot, lookup_status_ask_target, parse_rest_fee},
    scan_bybit_frame, scan_okx_frame, symbol_rate_key, trade_id_inline, try_decode_http_response,
    try_decode_ws_server_frame,
    wire_encode::BufWriter,
    ws_envelope::{WS_PING_PERIOD_NS, WsLoginOutcome},
};
use std::{
    net::{IpAddr, Ipv4Addr},
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering},
    },
    time::Instant,
};
use tickwire_account::{
    DueQueryAction, InlineAscii, OrderManager, OrsCancelRequest, OrsFeeSpec, OrsFillObservation,
    OrsModifyRequest, OrsNewOrderRequest, OrsOrderRefusal, OrsQueryOrigin, OrsQueryReconcileAction,
    OrsReport,
};
use tickwire_quota::{
    rate_limiter::{IpKey, RateFlags, RateLimitId, RateReject},
    shared::{
        AccountRateOverrides, ExchRateLimitEvent, IpBudgetSpec, OrsAccountQuota,
        RouteAdmissionReject, SharedRateLimiter,
    },
};
use tickwire_transport::{
    Connection, IoBudget, Limits,
    dialer::DialBaton,
    latency_tracer::{LockDomain, LockId, ThreadTag, TimedSpinLock},
    links::{
        HttpsLink, LinkOps, LockedConnection, MAX_WS_INFLIGHT, RequestRef, SendOutcome, WsLink,
        WsLoginState,
    },
    order_router::{
        MAX_ORDER_CONNECTIONS, OrderRouter, PublishedRoutes, RouterConfig, SampleKind, SendSlot,
        UNMEASURED_SCORE,
    },
    rest_querier::{AsyncRestQuerier, RestReply, RestStatus, RestTask, query_kind},
    ws::Opcode,
};
use tickwire_venue::{
    ClientIdDomain, DecodedClientId, OrderRoute as VenueOrderRoute, OrderStatus, Product,
    SelfDescribingClientId, Side, TerminalReason, TimeInForce,
};

/// Default cooldown applied to an IP or account bucket when the venue returns `429` / `10006` / `50011`
/// without an explicit `Retry-After` header (`1` second).
pub const DEFAULT_RATE_LIMIT_COOLDOWN_NS: u64 = 1_000_000_000;

/// Preallocated per-connection RX buffer size (`64 KiB`) on `RESP`.
const SLOT_RX_BUF_CAP: usize = 64 * 1024;

/// Preallocated `(venue_order_id -> ors_order_id)` fallback ring capacity for venue frames omitting `client_id`.
const VENUE_ID_CACHE_CAP: usize = 512;

/// Maximum coalesced `cancel_on_ack` dispatches queued on the stack per decoded frame.
const MAX_PENDING_CANCELS_PER_FRAME: usize = 32;

const NO_PICKED_CONN: u16 = u16::MAX;

/// Execution wire transport mode for a UTA session's outbound order connection pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UtaTransportMode {
    /// Multiplexed WebSocket trade pool (`SendSlot::Shared`, up to `16` concurrent in-flight requests per socket).
    WebSocketTrade,
    /// Pooled HTTP/1.1 keep-alive `POST` connections (`SendSlot::Exclusive`, `1` in-flight request per socket).
    PooledHttps,
}

impl UtaTransportMode {
    #[inline]
    pub const fn send_slot(self) -> SendSlot {
        match self {
            Self::WebSocketTrade => SendSlot::Shared,
            Self::PooledHttps => SendSlot::Exclusive,
        }
    }
}

/// `[local_ip, count]` source-IP connection pool entry for [`UtaSessionConfig`].
#[derive(Clone, Debug)]
pub struct UtaConnPoolEntry {
    pub local_ip: Ipv4Addr,
    pub count: u16,
    pub colo: bool,
    /// Optional per-connection background `DialBaton`s (`len <= count`) for cold `Dialer` pre-warming
    /// and zero-downtime server-maintenance `notice` switchover.
    pub batons: Vec<Arc<DialBaton>>,
}

impl UtaConnPoolEntry {
    pub fn new(local_ip: Ipv4Addr, count: u16) -> Self {
        Self {
            local_ip,
            count: count.max(1),
            colo: false,
            batons: Vec::new(),
        }
    }

    pub fn with_colo(mut self, colo: bool) -> Self {
        self.colo = colo;
        self
    }

    pub fn with_batons(mut self, batons: Vec<Arc<DialBaton>>) -> Self {
        self.batons = batons;
        self
    }
}

/// Startup configuration for [`UtaAccountSession`], [`BybitUtaSession`], and [`OkxUtaSession`].
pub struct UtaSessionConfig {
    pub venue: SpotVenue,
    pub environment: Environment,
    pub account_ref: u8,
    pub exchange_uid: u64,
    pub risk_domain_id: [u8; 16],
    pub epoch: u32,
    pub credentials: crate::network::OwnedCredentials,
    pub transport_mode: UtaTransportMode,
    pub pool: Vec<UtaConnPoolEntry>,
    pub router_config: RouterConfig,
    pub limits: Limits,
    pub symbols: Vec<UtaSymbolBinding>,
    pub assets: Vec<UtaAssetBinding>,
    pub rate_limiter: Arc<SharedRateLimiter>,
    pub rate_overrides: AccountRateOverrides,
    /// IP weight charged per outbound order/cancel on `SharedRateLimiter`'s venue IP window (`1` default).
    pub ip_order_weight: u32,
    /// When `true` on OKX `WebSocketTrade`, automatically subscribes `orders` (`ANY`) + `balance_and_position`
    /// on authenticated WS trade sockets after login completes.
    pub okx_ws_auto_subscribe_private: bool,
    /// Optional dedicated private WS stream `DialBaton` for reconnect/maintenance switchover.
    pub private_stream_baton: Option<Arc<DialBaton>>,
    /// Optional Options/Block `RFQ` quote extension hook (`NoopRfqQuoteHook` when `None`).
    pub rfq_hook: Option<Box<dyn RfqQuoteHook>>,
}

/// Successful outbound command dispatch or `dry_run` wire-encoding result on `REQ`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UtaSubmitOutcome {
    Dispatched {
        ors_order_id: u64,
        conn_id: u16,
        reply_id: u64,
        wire_len: usize,
    },
    /// Order was queued (`cancel_on_ack` or coalesced into `OrsQueryDesk`) without immediate wire send.
    Queued {
        ors_order_id: u64,
        coalesced_in_query_desk: bool,
    },
    /// `dry_run == true`: order admission + stack wire encoding completed and slot retired with zero
    /// network I/O and zero rate-limiter spend.
    DryRun { ors_order_id: u64, wire_len: usize },
}

/// Synchronous refusal returned on `REQ` when `OrderManager`, `PublishedRoutes`, `SharedRateLimiter`,
/// `RfqQuoteHook`, or `LockedConnection` rejects an outbound request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UtaSubmitRefusal {
    OrderRefusal(OrsOrderRefusal),
    NoReadyRoute {
        ors_order_id: u64,
        report: Option<OrsReport>,
    },
    RateLimited {
        ors_order_id: u64,
        reject: RateReject,
        report: Option<OrsReport>,
    },
    RfqRefused {
        ors_order_id: u64,
        reason: TerminalReason,
    },
    SendFailed {
        ors_order_id: u64,
        outcome: SendOutcome,
        report: Option<OrsReport>,
    },
    Encoding(TerminalReason),
}

/// Summary of one `RESP` poll cycle across order and private connections.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UtaPollReport {
    pub frames_processed: u32,
    pub maintenance_drains_started: u32,
    pub replacements_installed: u32,
    pub pending_cancels_dispatched: u32,
    pub rate_limit_events: Vec<ExchRateLimitEvent>,
}

/// Stack-allocated `cancel_on_ack` collector avoiding all heap allocation on `RESP`.
struct PendingCancelList {
    ids: [u64; MAX_PENDING_CANCELS_PER_FRAME],
    len: usize,
}

impl PendingCancelList {
    #[inline]
    const fn new() -> Self {
        Self {
            ids: [0; MAX_PENDING_CANCELS_PER_FRAME],
            len: 0,
        }
    }

    #[inline]
    fn push(&mut self, ors_order_id: u64) {
        if self.len < MAX_PENDING_CANCELS_PER_FRAME {
            self.ids[self.len] = ors_order_id;
            self.len += 1;
        }
    }

    #[inline]
    fn as_slice(&self) -> &[u64] {
        &self.ids[..self.len]
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum InflightOpKind {
    #[default]
    New,
    Modify {
        new_price_ticks: i64,
        new_qty_lots: i64,
    },
    Cancel,
    RfqSubmit,
    RfqCancel,
}

#[derive(Clone, Copy, Debug, Default)]
struct TrackedInflight {
    active: bool,
    reply_id: u64,
    ors_order_id: u64,
    symbol_id: u32,
    kind: InflightOpKind,
    sent_mono_ns: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SlotWsSubPhase {
    None,
    Subscribing { next_step: u8 },
    Subscribed,
}

struct SlotRespRx {
    rx_buf: Box<[u8]>,
    rx_len: usize,
    ws_sub_phase: SlotWsSubPhase,
    last_ping_mono_ns: u64,
    generation: u64,
}

struct OrderConnSlot {
    conn_id: u16,
    local_ip: Ipv4Addr,
    colo: bool,
    max_inflight: u16,
    conn: LockedConnection,
    baton: Option<Arc<DialBaton>>,
    draining: AtomicBool,
    ws_authenticated: AtomicBool,
    established: AtomicBool,
    ema_score_us: AtomicU32,
    claimed_inflight: AtomicU16,
    tracked: TimedSpinLock<[TrackedInflight; MAX_WS_INFLIGHT]>,
    resp_rx: TimedSpinLock<SlotRespRx>,
}

impl OrderConnSlot {
    fn new(
        conn_id: u16,
        local_ip: Ipv4Addr,
        colo: bool,
        max_inflight: u16,
        baton: Option<Arc<DialBaton>>,
    ) -> Self {
        Self {
            conn_id,
            local_ip,
            colo,
            max_inflight,
            conn: LockedConnection::new(conn_id, None, None),
            baton,
            draining: AtomicBool::new(false),
            ws_authenticated: AtomicBool::new(false),
            established: AtomicBool::new(false),
            ema_score_us: AtomicU32::new(UNMEASURED_SCORE),
            claimed_inflight: AtomicU16::new(0),
            tracked: TimedSpinLock::new(
                [TrackedInflight::default(); MAX_WS_INFLIGHT],
                LockId::new(LockDomain::GuardedValue, 0x1000 | u32::from(conn_id)),
            ),
            resp_rx: TimedSpinLock::new(
                SlotRespRx {
                    rx_buf: vec![0u8; SLOT_RX_BUF_CAP].into_boxed_slice(),
                    rx_len: 0,
                    ws_sub_phase: SlotWsSubPhase::None,
                    last_ping_mono_ns: 0,
                    generation: 1,
                },
                LockId::new(LockDomain::GuardedValue, 0x2000 | u32::from(conn_id)),
            ),
        }
    }

    /// Atomically claims 1 in-flight slot before `SharedRateLimiter` spend.
    ///
    /// Returns `Some(true)` if this claim saturated `max_inflight` (caller should republish
    /// `PublishedRoutes`), `Some(false)` if claimed with spare capacity remaining, or `None` if
    /// the slot is unready, draining, or already at `max_inflight`.
    #[inline]
    fn try_claim_inflight(&self) -> Option<bool> {
        if self.draining.load(Ordering::Acquire)
            || !self.ws_authenticated.load(Ordering::Acquire)
            || !self.established.load(Ordering::Acquire)
            || !self.conn.is_ready_relaxed()
        {
            return None;
        }
        let mut cur = self.claimed_inflight.load(Ordering::Acquire);
        loop {
            if cur >= self.max_inflight {
                return None;
            }
            match self.claimed_inflight.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if self.draining.load(Ordering::SeqCst)
                        || !self.ws_authenticated.load(Ordering::Acquire)
                        || !self.established.load(Ordering::Acquire)
                    {
                        let _ = self.sub_claim_saturating();
                        return None;
                    }
                    return Some(cur + 1 == self.max_inflight);
                }
                Err(observed) => cur = observed,
            }
        }
    }

    /// Releases 1 claimed in-flight slot, saturating at `0`. Returns `true` when transitioning
    /// from `max_inflight` down to `max_inflight - 1`.
    #[inline]
    fn sub_claim_saturating(&self) -> bool {
        let mut cur = self.claimed_inflight.load(Ordering::Acquire);
        loop {
            if cur == 0 {
                return false;
            }
            match self.claimed_inflight.compare_exchange_weak(
                cur,
                cur - 1,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => return cur == self.max_inflight,
                Err(observed) => cur = observed,
            }
        }
    }

    #[inline]
    fn record_inflight(&self, caller: ThreadTag, entry: TrackedInflight) {
        let mut guard = self.tracked.lock(caller);
        for slot in guard.iter_mut() {
            if !slot.active {
                *slot = entry;
                return;
            }
        }
        guard[0] = entry;
    }

    #[inline]
    fn take_inflight_by_reply_id(
        &self,
        caller: ThreadTag,
        reply_id: u64,
    ) -> Option<TrackedInflight> {
        let mut guard = self.tracked.lock(caller);
        for slot in guard.iter_mut() {
            if slot.active && slot.reply_id == reply_id {
                let out = *slot;
                slot.active = false;
                return Some(out);
            }
        }
        None
    }

    #[inline]
    fn take_any_inflight(&self, caller: ThreadTag) -> Option<TrackedInflight> {
        let mut guard = self.tracked.lock(caller);
        for slot in guard.iter_mut() {
            if slot.active {
                let out = *slot;
                slot.active = false;
                return Some(out);
            }
        }
        None
    }

    #[inline]
    fn clear_tracked_and_claims(&self, caller: ThreadTag) {
        let mut guard = self.tracked.lock(caller);
        *guard = [TrackedInflight::default(); MAX_WS_INFLIGHT];
        self.claimed_inflight.store(0, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, Default)]
struct VenueIdEntry {
    venue_order_id: InlineAscii<64>,
    ors_order_id: u64,
}

struct VenueIdCache {
    entries: Box<[VenueIdEntry]>,
    cursor: usize,
}

/// Full-product Unified Trading Account execution session (`1 UtaAccountSession = 1 Exchange UID`)
/// supporting Bybit V5 and OKX V5 across `Spot`, `LinearPerp`, `InversePerp`, `LinearFutures`,
/// `InverseFutures`, and `Options`.
pub struct UtaAccountSession<const CHECK_STP: bool = true> {
    venue: SpotVenue,
    environment: Environment,
    account_ref: u8,
    exchange_uid: u64,
    risk_domain_id: [u8; 16],
    epoch: u32,
    credentials: crate::network::OwnedCredentials,
    transport_mode: UtaTransportMode,
    order_manager: Arc<OrderManager<CHECK_STP>>,
    rate_limiter: Arc<SharedRateLimiter>,
    rate_overrides: AccountRateOverrides,
    ip_order_weight: u32,
    router_lock: TimedSpinLock<OrderRouter>,
    publish_lock: TimedSpinLock<()>,
    published_routes: Arc<PublishedRoutes>,
    colo_mask: u16,
    last_picked_conn_id: AtomicU16,
    next_reply_id: AtomicU64,
    slots: Box<[OrderConnSlot]>,
    limits: Limits,
    okx_ws_auto_subscribe_private: bool,
    private_conn: LockedConnection,
    private_stream_lock: TimedSpinLock<UtaPrivateStream>,
    private_subscribed: AtomicBool,
    private_baton: Option<Arc<DialBaton>>,
    private_draining: AtomicBool,
    symbols: Arc<[UtaSymbolBinding]>,
    assets: Arc<[UtaAssetBinding]>,
    rfq_hook: Box<dyn RfqQuoteHook>,
    venue_id_cache_lock: TimedSpinLock<VenueIdCache>,
}

impl<const CHECK_STP: bool> UtaAccountSession<CHECK_STP> {
    /// Constructs a stateless UTA execution session, registering per-UID and per-`local_ip`
    /// rate-limit budgets on `SharedRateLimiter` and initializing `OrderRoute.product_mask = 0`
    /// slots across the shared `[local_ip, count]` pool.
    pub fn new(
        order_manager: Arc<OrderManager<CHECK_STP>>,
        cfg: UtaSessionConfig,
    ) -> Result<Self, UtaVenueError> {
        if cfg.account_ref == 0 || cfg.exchange_uid == 0 || cfg.pool.is_empty() {
            return Err(UtaVenueError::Identity);
        }
        if !cfg.credentials.credentials().valid() || cfg.credentials.venue() != cfg.venue {
            return Err(UtaVenueError::Identity);
        }

        let mut router = OrderRouter::new(cfg.router_config)?;
        let published_routes = Arc::new(PublishedRoutes::new());
        let send_slot = cfg.transport_mode.send_slot();
        let max_inflight = send_slot.max_inflight();

        let mut slots = Vec::new();
        let mut ip_specs = Vec::new();
        let mut colo_mask: u16 = 0;
        let mut next_conn_id: u16 = 0;

        for entry in &cfg.pool {
            let ip_spec = IpBudgetSpec::new(IpAddr::V4(entry.local_ip), 0);
            if !ip_specs.contains(&ip_spec) {
                ip_specs.push(ip_spec);
            }
            for idx in 0..entry.count {
                if usize::from(next_conn_id) >= MAX_ORDER_CONNECTIONS {
                    return Err(UtaVenueError::Bounds);
                }
                let baton = entry.batons.get(usize::from(idx)).cloned();
                // product_mask == 0 => all 6 UTA products share this connection pool!
                router.register_slot(
                    next_conn_id,
                    Some(IpAddr::V4(entry.local_ip)),
                    send_slot,
                    0,
                )?;
                if entry.colo && next_conn_id < 16 {
                    colo_mask |= 1u16 << next_conn_id;
                }
                slots.push(OrderConnSlot::new(
                    next_conn_id,
                    entry.local_ip,
                    entry.colo,
                    max_inflight,
                    baton,
                ));
                next_conn_id += 1;
            }
        }

        let profile = cfg.venue.quota_profile();
        cfg.rate_limiter
            .install_venue_ip_budgets(profile, &ip_specs)
            .map_err(|_| UtaVenueError::Encoding)?;
        let _ = OrsAccountQuota::register_with_overrides(
            &cfg.rate_limiter,
            profile,
            cfg.exchange_uid,
            cfg.rate_overrides,
        )
        .map_err(|_| UtaVenueError::Encoding)?;

        let rfq_hook = cfg.rfq_hook.unwrap_or_else(|| Box::new(NoopRfqQuoteHook));
        let private_conn_id = next_conn_id;
        let private_stream = UtaPrivateStream::new(cfg.venue, SLOT_RX_BUF_CAP);
        let acct_tag = u32::from(cfg.account_ref) << 8;

        let session = Self {
            venue: cfg.venue,
            environment: cfg.environment,
            account_ref: cfg.account_ref,
            exchange_uid: cfg.exchange_uid,
            risk_domain_id: cfg.risk_domain_id,
            epoch: cfg.epoch,
            credentials: cfg.credentials,
            transport_mode: cfg.transport_mode,
            order_manager,
            rate_limiter: cfg.rate_limiter,
            rate_overrides: cfg.rate_overrides,
            ip_order_weight: cfg.ip_order_weight,
            router_lock: TimedSpinLock::new(
                router,
                LockId::new(LockDomain::GuardedValue, 0x3000 | acct_tag),
            ),
            publish_lock: TimedSpinLock::new(
                (),
                LockId::new(LockDomain::GuardedValue, 0x3100 | acct_tag),
            ),
            published_routes,
            colo_mask,
            last_picked_conn_id: AtomicU16::new(NO_PICKED_CONN),
            next_reply_id: AtomicU64::new(1),
            slots: slots.into_boxed_slice(),
            limits: cfg.limits,
            okx_ws_auto_subscribe_private: cfg.okx_ws_auto_subscribe_private,
            private_conn: LockedConnection::new(private_conn_id, None, None),
            private_stream_lock: TimedSpinLock::new(
                private_stream,
                LockId::new(LockDomain::GuardedValue, 0x3200 | acct_tag),
            ),
            private_subscribed: AtomicBool::new(false),
            private_baton: cfg.private_stream_baton,
            private_draining: AtomicBool::new(false),
            symbols: cfg.symbols.into(),
            assets: cfg.assets.into(),
            rfq_hook,
            venue_id_cache_lock: TimedSpinLock::new(
                VenueIdCache {
                    entries: vec![VenueIdEntry::default(); VENUE_ID_CACHE_CAP].into_boxed_slice(),
                    cursor: 0,
                },
                LockId::new(LockDomain::GuardedValue, 0x3300 | acct_tag),
            ),
        };
        session.republish_routes()?;
        Ok(session)
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
    pub const fn account_ref(&self) -> u8 {
        self.account_ref
    }

    #[inline]
    pub const fn exchange_uid(&self) -> u64 {
        self.exchange_uid
    }

    #[inline]
    pub const fn epoch(&self) -> u32 {
        self.epoch
    }

    #[inline]
    pub fn is_slot_colo(&self, conn_id: u16) -> Option<bool> {
        self.slots.get(usize::from(conn_id)).map(|s| s.colo)
    }

    #[inline]
    pub const fn transport_mode(&self) -> UtaTransportMode {
        self.transport_mode
    }

    #[inline]
    pub fn order_manager(&self) -> &Arc<OrderManager<CHECK_STP>> {
        &self.order_manager
    }

    #[inline]
    pub fn published_routes(&self) -> &Arc<PublishedRoutes> {
        &self.published_routes
    }

    #[inline]
    pub fn rate_limiter(&self) -> &Arc<SharedRateLimiter> {
        &self.rate_limiter
    }

    /// Returns the immutable `UtaSymbolBinding` table shared with [`crate::UtaRestRequestBuilder`].
    #[inline]
    pub fn symbols(&self) -> Arc<[UtaSymbolBinding]> {
        Arc::clone(&self.symbols)
    }

    /// Returns the immutable `UtaAssetBinding` table for this account session.
    #[inline]
    pub fn assets(&self) -> Arc<[UtaAssetBinding]> {
        Arc::clone(&self.assets)
    }

    #[inline]
    pub fn connection_count(&self) -> usize {
        self.slots.len()
    }

    #[inline]
    pub fn is_slot_draining(&self, conn_id: u16) -> bool {
        self.slots
            .get(usize::from(conn_id))
            .is_some_and(|s| s.draining.load(Ordering::Acquire))
    }

    #[inline]
    pub fn is_slot_ready(&self, conn_id: u16) -> bool {
        let Some(slot) = self.slots.get(usize::from(conn_id)) else {
            return false;
        };
        slot.established.load(Ordering::Acquire)
            && slot.ws_authenticated.load(Ordering::Acquire)
            && !slot.draining.load(Ordering::Acquire)
            && slot.claimed_inflight.load(Ordering::Acquire) < slot.max_inflight
            && slot.conn.is_ready_relaxed()
    }

    #[inline]
    pub fn rfq_hook(&self) -> &dyn RfqQuoteHook {
        self.rfq_hook.as_ref()
    }

    pub fn set_rfq_hook(&mut self, hook: Box<dyn RfqQuoteHook>) {
        self.rfq_hook = hook;
    }

    /// Injects an EWMA RTT sample (in microseconds) on `conn_id` and republishes
    /// `PublishedRoutes` (used by `RESP` RTT sampling and deterministic router hysteresis tests).
    pub fn record_route_rtt_sample_us(
        &self,
        conn_id: u16,
        rtt_us: u32,
    ) -> Result<(), UtaVenueError> {
        self.note_router_rtt_sample(conn_id, SampleKind::Ping, rtt_us)?;
        self.republish_routes()
    }

    #[inline]
    fn note_router_rtt_sample(
        &self,
        conn_id: u16,
        kind: SampleKind,
        rtt_us: u32,
    ) -> Result<(), UtaVenueError> {
        let slot = self
            .slots
            .get(usize::from(conn_id))
            .ok_or(UtaVenueError::Bounds)?;
        let score = {
            let mut router = self.router_lock.lock(ThreadTag::RESP);
            let epoch = router.route(conn_id).ok_or(UtaVenueError::Bounds)?.epoch;
            router.note_sample(conn_id, epoch, kind, rtt_us)?;
            router
                .route(conn_id)
                .map_or(UNMEASURED_SCORE, |r| r.health.raw_score())
        };
        slot.ema_score_us.store(score, Ordering::Release);
        Ok(())
    }

    /// Installs a connected `tickwire_transport::Connection` into order pool slot `conn_id`.
    ///
    /// - For `PooledHttps`, marks the route ready immediately.
    /// - For `WebSocketTrade`, if `already_authenticated` is `false`, sends the zero-allocation
    ///   masked `auth`/`login` frame immediately and holds `ready = false` until `RESP` verifies
    ///   the venue auth acknowledgement.
    pub fn install_order_connection(
        &self,
        conn_id: u16,
        conn: Connection,
        already_authenticated: bool,
        utc_ms: u64,
        now_mono_ns: u64,
    ) -> Result<(), UtaVenueError> {
        let idx = usize::from(conn_id);
        let slot = self.slots.get(idx).ok_or(UtaVenueError::Bounds)?;
        let max_rx = self.limits.max_receive_bytes.max(16 * 1024);
        let creds = self.credentials.credentials();
        let mode = self.transport_mode;
        let venue = self.venue;

        let link = match mode {
            UtaTransportMode::PooledHttps => LinkOps::Https(HttpsLink::new(conn, max_rx, 128)?),
            UtaTransportMode::WebSocketTrade => {
                let state = if already_authenticated {
                    WsLoginState::Authenticated
                } else {
                    WsLoginState::AwaitingAuthAck
                };
                LinkOps::Ws(WsLink::with_login(conn, max_rx, max_rx, state)?)
            }
        };

        let prev = slot.conn.install(link);
        drop(prev);
        slot.draining.store(false, Ordering::SeqCst);
        slot.clear_tracked_and_claims(ThreadTag::RESP);

        let needs_fresh_buf = slot.resp_rx.lock(ThreadTag::RESP).rx_buf.is_empty();
        let fresh_buf = needs_fresh_buf.then(|| vec![0u8; SLOT_RX_BUF_CAP].into_boxed_slice());
        {
            let mut rx_guard = slot.resp_rx.lock(ThreadTag::RESP);
            if let Some(buf) = fresh_buf
                && rx_guard.rx_buf.is_empty()
            {
                rx_guard.rx_buf = buf;
            }
            rx_guard.rx_len = 0;
            rx_guard.ws_sub_phase = SlotWsSubPhase::None;
            rx_guard.last_ping_mono_ns = now_mono_ns;
            rx_guard.generation = rx_guard.generation.wrapping_add(1);
        }

        slot.ws_authenticated.store(
            mode == UtaTransportMode::PooledHttps || already_authenticated,
            Ordering::Release,
        );

        if mode == UtaTransportMode::WebSocketTrade && !already_authenticated {
            let mut auth_json = [0u8; 512];
            let jlen = match venue {
                SpotVenue::Bybit => encode_bybit_auth_into(&creds, utc_ms, &mut auth_json)?,
                SpotVenue::Okx => encode_okx_login_into(&creds, utc_ms, &mut auth_json)?,
            };
            let mut ws_frame = [0u8; 600];
            let flen = encode_ws_client_frame_into(
                Opcode::Text,
                &auth_json[..jlen],
                utc_ms ^ u64::from(conn_id),
                &mut ws_frame,
            )?;
            let outcome = slot.conn.send_control_frame(&ws_frame[..flen]);
            if outcome != SendOutcome::Sent {
                return Err(UtaVenueError::Protocol(
                    "failed to send WS trade login frame",
                ));
            }
        }

        let score = {
            let mut router = self.router_lock.lock(ThreadTag::RESP);
            router.on_connected(conn_id, Some(IpAddr::V4(slot.local_ip)), None)?;
            router
                .route(conn_id)
                .map_or(UNMEASURED_SCORE, |r| r.health.raw_score())
        };
        slot.ema_score_us.store(score, Ordering::Release);
        slot.established.store(true, Ordering::SeqCst);
        self.republish_routes()
    }

    /// Installs a connected WebSocket `Connection` onto the dedicated private stream slot and
    /// transmits its `op: "auth"` / `op: "login"` frame.
    pub fn install_private_connection(
        &self,
        conn: Connection,
        utc_ms: u64,
        now_mono_ns: u64,
    ) -> Result<(), UtaVenueError> {
        let max_rx = self.limits.max_receive_bytes.max(16 * 1024);
        let link = LinkOps::Ws(WsLink::with_login(
            conn,
            max_rx,
            max_rx,
            WsLoginState::AwaitingAuthAck,
        )?);
        let prev = self.private_conn.install(link);
        drop(prev);
        self.private_draining.store(false, Ordering::Release);
        self.private_subscribed.store(false, Ordering::Release);

        let creds = self.credentials.credentials();
        let mut ws_frame = [0u8; 640];
        let mut priv_stream = {
            let mut guard = self.private_stream_lock.lock(ThreadTag::RESP);
            core::mem::replace(&mut *guard, UtaPrivateStream::empty_placeholder(self.venue))
        };
        if !priv_stream.has_rx_capacity() {
            priv_stream = UtaPrivateStream::new(self.venue, SLOT_RX_BUF_CAP);
        }
        let flen_res = priv_stream.begin_login(&creds, utc_ms, now_mono_ns, &mut ws_frame);
        {
            let mut guard = self.private_stream_lock.lock(ThreadTag::RESP);
            *guard = priv_stream;
        }
        let flen = flen_res?;
        let outcome = self.private_conn.send_control_frame(&ws_frame[..flen]);
        if outcome != SendOutcome::Sent {
            return Err(UtaVenueError::Protocol(
                "failed to send private WS login frame",
            ));
        }
        Ok(())
    }

    /// Publishes an immutable `PublishedRoutes` snapshot from per-slot atomic readiness and EWMA
    /// state, overriding `ready = false` for any slot that is unauthenticated, at `max_inflight`,
    /// or draining for server maintenance.
    pub fn republish_routes(&self) -> Result<(), UtaVenueError> {
        let _pub_guard = self.publish_lock.lock(ThreadTag::UNKNOWN);
        let mut scratch = [VenueOrderRoute::default(); MAX_ORDER_CONNECTIONS];
        let mut count = 0usize;
        for slot in self.slots.iter() {
            let established = slot.established.load(Ordering::Acquire);
            let ws_auth = slot.ws_authenticated.load(Ordering::Acquire);
            let draining = slot.draining.load(Ordering::Acquire);
            let claimed = slot.claimed_inflight.load(Ordering::Acquire);
            let ready = established
                && ws_auth
                && !draining
                && claimed < slot.max_inflight
                && slot.conn.is_ready_relaxed();
            scratch[count] = VenueOrderRoute {
                connection_id: slot.conn_id,
                account_ref: self.account_ref,
                ready,
                colo: ((self.colo_mask >> (usize::from(slot.conn_id).min(15))) & 1) != 0,
                // product_mask == 0 => matches all 6 UTA products on this UID session!
                product_mask: 0,
                local_ip: slot.local_ip.to_bits(),
                ema_score_us: slot.ema_score_us.load(Ordering::Acquire),
            };
            count += 1;
        }
        self.published_routes.publish_routes(&scratch[..count])?;
        Ok(())
    }

    /// Picks a ready route from `PublishedRoutes` and atomically claims 1 in-flight slot before
    /// `SharedRateLimiter` spend so back-to-back or concurrent `REQ` sends never collide on an
    /// `Exclusive` (`PooledHttps`) or saturated `Shared` (`WebSocketTrade`) link.
    fn pick_and_claim_route(&self, product: Product) -> Option<VenueOrderRoute> {
        let raw_last = self.last_picked_conn_id.load(Ordering::Relaxed);
        let last_cid = (raw_last != NO_PICKED_CONN).then_some(raw_last);
        for _ in 0..MAX_ORDER_CONNECTIONS {
            let route =
                self.published_routes
                    .pick(self.account_ref, Some(product), last_cid, false)?;
            let slot = self.slots.get(usize::from(route.connection_id))?;
            match slot.try_claim_inflight() {
                Some(reached_max) => {
                    if reached_max {
                        let _ = self.republish_routes();
                    }
                    return Some(route);
                }
                None => {
                    let _ = self.republish_routes();
                }
            }
        }
        None
    }

    #[inline]
    fn release_claimed_inflight(&self, conn_id: u16) {
        if let Some(slot) = self.slots.get(usize::from(conn_id))
            && slot.sub_claim_saturating()
        {
            let _ = self.republish_routes();
        }
    }

    #[inline]
    fn rollback_failed_send(&self, conn_id: u16, reply_id: u64) {
        if let Some(slot) = self.slots.get(usize::from(conn_id))
            && slot
                .take_inflight_by_reply_id(ThreadTag::REQ, reply_id)
                .is_some()
        {
            let _ = slot.sub_claim_saturating();
        }
        let _ = self.republish_routes();
    }

    #[inline]
    fn quota(&self) -> Result<OrsAccountQuota<'_>, RateReject> {
        OrsAccountQuota::register_with_overrides(
            &self.rate_limiter,
            self.venue.quota_profile(),
            self.exchange_uid,
            self.rate_overrides,
        )
    }

    #[inline]
    fn find_symbol_by_id(&self, symbol_id: u32) -> Option<&UtaSymbolBinding> {
        self.symbols.iter().find(|s| s.symbol_id == symbol_id)
    }

    #[inline]
    fn find_symbol_by_name(&self, name: &str) -> Option<&UtaSymbolBinding> {
        self.symbols.iter().find(|s| s.symbol.as_str() == name)
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
    fn remember_venue_order_id(&self, venue_order_id: &str, ors_order_id: u64) {
        if venue_order_id.is_empty() || ors_order_id == 0 {
            return;
        }
        let Some(inline) = InlineAscii::try_from_str(venue_order_id) else {
            return;
        };
        let mut cache = self.venue_id_cache_lock.lock(ThreadTag::RESP);
        for entry in cache.entries.iter_mut() {
            if entry.ors_order_id == ors_order_id || entry.venue_order_id == inline {
                entry.venue_order_id = inline;
                entry.ors_order_id = ors_order_id;
                return;
            }
        }
        let len = cache.entries.len();
        let idx = cache.cursor % len;
        cache.cursor = cache.cursor.wrapping_add(1);
        cache.entries[idx] = VenueIdEntry {
            venue_order_id: inline,
            ors_order_id,
        };
    }

    #[inline]
    fn lookup_venue_order_id(&self, venue_order_id: &str) -> Option<u64> {
        if venue_order_id.is_empty() {
            return None;
        }
        let inline = InlineAscii::<64>::try_from_str(venue_order_id)?;
        let cache = self.venue_id_cache_lock.lock(ThreadTag::RESP);
        cache
            .entries
            .iter()
            .find(|e| e.ors_order_id != 0 && e.venue_order_id == inline)
            .map(|e| e.ors_order_id)
    }

    #[inline]
    fn resolve_order_id(
        &self,
        product_hint: Option<Product>,
        client_id: &str,
        venue_order_id: &str,
    ) -> Option<u64> {
        if !client_id.is_empty() {
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
        }
        self.lookup_venue_order_id(venue_order_id)
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

    /// Outbound `REQ` hot path: admits a `NewOrder` across any of the 6 UTA products (`Spot`,
    /// `LinearPerp`, `InversePerp`, `LinearFutures`, `InverseFutures`, `Options`, including `RPI`),
    /// encodes the WS/HTTPS wire frame on the stack outside all spinlocks, executes the single-pick
    /// `PublishedRoutes::pick -> SharedRateLimiter` sequence, and dispatches over `LockedConnection`.
    pub fn submit_new_order(
        &self,
        req: &OrsNewOrderRequest,
        mut params: UtaOrderParams,
        utc_ms: u64,
    ) -> Result<UtaSubmitOutcome, UtaSubmitRefusal> {
        let placed = self
            .order_manager
            .place_order(req)
            .map_err(UtaSubmitRefusal::OrderRefusal)?;

        let Some(binding) = self.find_symbol_by_id(req.symbol_id).copied() else {
            let rep = self.order_manager.on_rejected(
                placed.ors_order_id,
                None,
                "UNKNOWN_SYMBOL",
                Some(TerminalReason::InvalidQuantity),
                req.now_mono_ns,
            );
            let _ = rep;
            return Err(UtaSubmitRefusal::Encoding(TerminalReason::InvalidQuantity));
        };

        params.time_in_force = req.tif;
        params.post_only |= req.post_only;
        params.reduce_only = req.reduce_only;

        let reply_id = self.alloc_reply_id();
        let mut wire_buf = [0u8; 2048];
        let wire_len = match self.encode_new_order_wire(
            &binding,
            req,
            placed.client_id.as_str(),
            params,
            reply_id,
            utc_ms,
            &mut wire_buf,
        ) {
            Ok(n) => n,
            Err(_) => {
                if placed.should_send_to_venue {
                    let _ = self.order_manager.on_rejected(
                        placed.ors_order_id,
                        None,
                        "WIRE_ENCODE_ERR",
                        Some(TerminalReason::InvalidQuantity),
                        req.now_mono_ns,
                    );
                }
                return Err(UtaSubmitRefusal::Encoding(TerminalReason::InvalidQuantity));
            }
        };

        if !placed.should_send_to_venue {
            return Ok(UtaSubmitOutcome::DryRun {
                ors_order_id: placed.ors_order_id,
                wire_len,
            });
        }

        let scope =
            instrument_rate_scope(self.venue, binding.spec.product, binding.symbol.as_str());
        let quota = match self.quota() {
            Ok(q) => q,
            Err(rej) => {
                let rep = self.order_manager.on_rejected(
                    placed.ors_order_id,
                    None,
                    "RATE_LIMITED",
                    Some(TerminalReason::RateLimited),
                    req.now_mono_ns,
                );
                return Err(UtaSubmitRefusal::RateLimited {
                    ors_order_id: placed.ors_order_id,
                    reject: rej,
                    report: rep,
                });
            }
        };

        let prod = binding.spec.product;
        let mut claimed_conn: Option<u16> = None;
        let (route, _permit) = match quota.pick_route_and_place_or_nack(
            || {
                let r = self.pick_and_claim_route(prod)?;
                claimed_conn = Some(r.connection_id);
                Some(r)
            },
            scope,
            1,
            self.ip_order_weight,
            req.now_mono_ns,
        ) {
            Ok(pair) => pair,
            Err(
                RouteAdmissionReject::NoReadyConnection
                | RouteAdmissionReject::MissingLocalIp { .. },
            ) => {
                if let Some(cid) = claimed_conn {
                    self.release_claimed_inflight(cid);
                }
                let rep = self.order_manager.on_rejected(
                    placed.ors_order_id,
                    None,
                    "NO_READY_ROUTE",
                    Some(TerminalReason::MKT_REJECT_MARKET_DOWN),
                    req.now_mono_ns,
                );
                return Err(UtaSubmitRefusal::NoReadyRoute {
                    ors_order_id: placed.ors_order_id,
                    report: rep,
                });
            }
            Err(RouteAdmissionReject::RateLimitedNack(rej)) => {
                if let Some(cid) = claimed_conn {
                    self.release_claimed_inflight(cid);
                }
                let rep = self.order_manager.on_rejected(
                    placed.ors_order_id,
                    None,
                    "RATE_LIMITED",
                    Some(TerminalReason::RateLimited),
                    req.now_mono_ns,
                );
                return Err(UtaSubmitRefusal::RateLimited {
                    ors_order_id: placed.ors_order_id,
                    reject: rej,
                    report: rep,
                });
            }
        };

        let conn_id = route.connection_id;
        let slot = &self.slots[usize::from(conn_id)];
        slot.record_inflight(
            ThreadTag::REQ,
            TrackedInflight {
                active: true,
                reply_id,
                ors_order_id: placed.ors_order_id,
                symbol_id: req.symbol_id,
                kind: InflightOpKind::New,
                sent_mono_ns: req.now_mono_ns,
            },
        );
        let req_ref = RequestRef {
            order_key: placed.ors_order_id,
            client_seq: req.order_seqnum,
            sent_tsc: 0,
            sent_mono_ns: req.now_mono_ns,
        };
        let send_outcome = slot
            .conn
            .send_frame(&wire_buf[..wire_len], reply_id, req_ref);

        match send_outcome {
            SendOutcome::Sent => {
                self.last_picked_conn_id.store(conn_id, Ordering::Relaxed);
                Ok(UtaSubmitOutcome::Dispatched {
                    ors_order_id: placed.ors_order_id,
                    conn_id,
                    reply_id,
                    wire_len,
                })
            }
            SendOutcome::ShortWriteRetired => {
                self.rollback_failed_send(conn_id, reply_id);
                let _ = self
                    .order_manager
                    .on_socket_drop(self.account_ref, req.now_mono_ns);
                Err(UtaSubmitRefusal::SendFailed {
                    ors_order_id: placed.ors_order_id,
                    outcome: send_outcome,
                    report: None,
                })
            }
            other => {
                self.rollback_failed_send(conn_id, reply_id);
                let rep = self.order_manager.on_rejected(
                    placed.ors_order_id,
                    None,
                    "SEND_FAILED",
                    Some(TerminalReason::MKT_REJECT_MARKET_DOWN),
                    req.now_mono_ns,
                );
                Err(UtaSubmitRefusal::SendFailed {
                    ors_order_id: placed.ors_order_id,
                    outcome: other,
                    report: rep,
                })
            }
        }
    }

    /// Outbound `REQ` hot path: admits an in-place order amendment (`order.amend` / `amend-order`),
    /// encodes on the stack outside all spinlocks, executes single-pick `SharedRateLimiter` admission,
    /// and transmits over `LockedConnection`.
    pub fn submit_modify_order(
        &self,
        req: &OrsModifyRequest,
        utc_ms: u64,
    ) -> Result<UtaSubmitOutcome, UtaSubmitRefusal> {
        let disp = self
            .order_manager
            .request_modify(req)
            .map_err(UtaSubmitRefusal::OrderRefusal)?;

        let (client_id, venue_order_id) = self.read_order_identifiers(disp.ors_order_id);
        let Some(binding) = self.find_symbol_by_id(disp.symbol_id).copied() else {
            let rep = self.order_manager.on_modify_rejected(
                disp.ors_order_id,
                TerminalReason::InvalidQuantity,
                req.now_mono_ns,
            );
            let _ = rep;
            return Err(UtaSubmitRefusal::Encoding(TerminalReason::InvalidQuantity));
        };

        let reply_id = self.alloc_reply_id();
        let mut wire_buf = [0u8; 2048];
        let wire_len = match self.encode_modify_wire(
            &binding,
            (!client_id.is_empty()).then_some(client_id.as_str()),
            (!venue_order_id.is_empty()).then_some(venue_order_id.as_str()),
            disp.new_price_ticks,
            disp.new_qty_lots,
            reply_id,
            utc_ms,
            &mut wire_buf,
        ) {
            Ok(n) => n,
            Err(_) => {
                let _ = self.order_manager.on_modify_rejected(
                    disp.ors_order_id,
                    TerminalReason::InvalidQuantity,
                    req.now_mono_ns,
                );
                return Err(UtaSubmitRefusal::Encoding(TerminalReason::InvalidQuantity));
            }
        };

        let scope =
            instrument_rate_scope(self.venue, binding.spec.product, binding.symbol.as_str());
        let quota = self.quota().map_err(|rej| {
            let rep = self.order_manager.on_modify_rejected(
                disp.ors_order_id,
                TerminalReason::RateLimited,
                req.now_mono_ns,
            );
            UtaSubmitRefusal::RateLimited {
                ors_order_id: disp.ors_order_id,
                reject: rej,
                report: rep,
            }
        })?;

        let prod = binding.spec.product;
        let mut claimed_conn: Option<u16> = None;
        let (route, _permit) = match quota.pick_route_and_place_or_nack(
            || {
                let r = self.pick_and_claim_route(prod)?;
                claimed_conn = Some(r.connection_id);
                Some(r)
            },
            scope,
            1,
            self.ip_order_weight,
            req.now_mono_ns,
        ) {
            Ok(pair) => pair,
            Err(
                RouteAdmissionReject::NoReadyConnection
                | RouteAdmissionReject::MissingLocalIp { .. },
            ) => {
                if let Some(cid) = claimed_conn {
                    self.release_claimed_inflight(cid);
                }
                let rep = self.order_manager.on_modify_rejected(
                    disp.ors_order_id,
                    TerminalReason::MKT_REJECT_MARKET_DOWN,
                    req.now_mono_ns,
                );
                return Err(UtaSubmitRefusal::NoReadyRoute {
                    ors_order_id: disp.ors_order_id,
                    report: rep,
                });
            }
            Err(RouteAdmissionReject::RateLimitedNack(rej)) => {
                if let Some(cid) = claimed_conn {
                    self.release_claimed_inflight(cid);
                }
                let rep = self.order_manager.on_modify_rejected(
                    disp.ors_order_id,
                    TerminalReason::RateLimited,
                    req.now_mono_ns,
                );
                return Err(UtaSubmitRefusal::RateLimited {
                    ors_order_id: disp.ors_order_id,
                    reject: rej,
                    report: rep,
                });
            }
        };

        let conn_id = route.connection_id;
        let slot = &self.slots[usize::from(conn_id)];
        slot.record_inflight(
            ThreadTag::REQ,
            TrackedInflight {
                active: true,
                reply_id,
                ors_order_id: disp.ors_order_id,
                symbol_id: disp.symbol_id,
                kind: InflightOpKind::Modify {
                    new_price_ticks: disp.new_price_ticks,
                    new_qty_lots: disp.new_qty_lots,
                },
                sent_mono_ns: req.now_mono_ns,
            },
        );
        let req_ref = RequestRef {
            order_key: disp.ors_order_id,
            client_seq: req.msg_seqnum,
            sent_tsc: 0,
            sent_mono_ns: req.now_mono_ns,
        };
        let send_outcome = slot
            .conn
            .send_frame(&wire_buf[..wire_len], reply_id, req_ref);
        match send_outcome {
            SendOutcome::Sent => {
                self.last_picked_conn_id.store(conn_id, Ordering::Relaxed);
                Ok(UtaSubmitOutcome::Dispatched {
                    ors_order_id: disp.ors_order_id,
                    conn_id,
                    reply_id,
                    wire_len,
                })
            }
            SendOutcome::ShortWriteRetired => {
                self.rollback_failed_send(conn_id, reply_id);
                let _ = self
                    .order_manager
                    .on_socket_drop(self.account_ref, req.now_mono_ns);
                Err(UtaSubmitRefusal::SendFailed {
                    ors_order_id: disp.ors_order_id,
                    outcome: send_outcome,
                    report: None,
                })
            }
            other => {
                self.rollback_failed_send(conn_id, reply_id);
                let rep = self.order_manager.on_modify_rejected(
                    disp.ors_order_id,
                    TerminalReason::MKT_REJECT_MARKET_DOWN,
                    req.now_mono_ns,
                );
                Err(UtaSubmitRefusal::SendFailed {
                    ors_order_id: disp.ors_order_id,
                    outcome: other,
                    report: rep,
                })
            }
        }
    }

    /// Outbound `REQ` hot path: admits a strategy `Cancel` via `OrderManager::request_cancel`,
    /// spending ONLY `CANCEL` + IP windows (never touching `ORDER` placement windows!).
    pub fn submit_cancel_order(
        &self,
        req: &OrsCancelRequest,
        utc_ms: u64,
    ) -> Result<UtaSubmitOutcome, UtaSubmitRefusal> {
        let disp = self
            .order_manager
            .request_cancel(req)
            .map_err(UtaSubmitRefusal::OrderRefusal)?;

        if !disp.should_send_now {
            return Ok(UtaSubmitOutcome::Queued {
                ors_order_id: disp.ors_order_id,
                coalesced_in_query_desk: disp.coalesced_in_query_desk,
            });
        }

        self.dispatch_wire_cancel(
            disp.ors_order_id,
            disp.symbol_id,
            disp.client_id.as_str(),
            disp.venue_order_id.as_str(),
            req.msg_seqnum,
            req.now_mono_ns,
            utc_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_wire_cancel(
        &self,
        ors_order_id: u64,
        symbol_id: u32,
        client_id: &str,
        venue_order_id: &str,
        seq: u64,
        now_mono_ns: u64,
        utc_ms: u64,
    ) -> Result<UtaSubmitOutcome, UtaSubmitRefusal> {
        let Some(binding) = self.find_symbol_by_id(symbol_id).copied() else {
            return Err(UtaSubmitRefusal::Encoding(TerminalReason::InvalidQuantity));
        };
        let reply_id = self.alloc_reply_id();
        let mut wire_buf = [0u8; 2048];
        let wire_len = self
            .encode_cancel_wire(
                &binding,
                (!client_id.is_empty()).then_some(client_id),
                (!venue_order_id.is_empty()).then_some(venue_order_id),
                reply_id,
                utc_ms,
                &mut wire_buf,
            )
            .map_err(|_| UtaSubmitRefusal::Encoding(TerminalReason::InvalidQuantity))?;

        let scope =
            instrument_rate_scope(self.venue, binding.spec.product, binding.symbol.as_str());
        let quota = self.quota().map_err(|rej| {
            let rep = self.order_manager.on_cancel_rejected(
                ors_order_id,
                None,
                TerminalReason::RateLimited,
                now_mono_ns,
            );
            UtaSubmitRefusal::RateLimited {
                ors_order_id,
                reject: rej,
                report: rep,
            }
        })?;

        let prod = binding.spec.product;
        let mut claimed_conn: Option<u16> = None;
        let (route, _permit) = match quota.pick_route_and_cancel_or_nack(
            || {
                let r = self.pick_and_claim_route(prod)?;
                claimed_conn = Some(r.connection_id);
                Some(r)
            },
            scope,
            self.ip_order_weight,
            now_mono_ns,
        ) {
            Ok(pair) => pair,
            Err(
                RouteAdmissionReject::NoReadyConnection
                | RouteAdmissionReject::MissingLocalIp { .. },
            ) => {
                if let Some(cid) = claimed_conn {
                    self.release_claimed_inflight(cid);
                }
                let rep = self.order_manager.on_cancel_rejected(
                    ors_order_id,
                    None,
                    TerminalReason::MKT_REJECT_MARKET_DOWN,
                    now_mono_ns,
                );
                return Err(UtaSubmitRefusal::NoReadyRoute {
                    ors_order_id,
                    report: rep,
                });
            }
            Err(RouteAdmissionReject::RateLimitedNack(rej)) => {
                if let Some(cid) = claimed_conn {
                    self.release_claimed_inflight(cid);
                }
                let rep = self.order_manager.on_cancel_rejected(
                    ors_order_id,
                    None,
                    TerminalReason::RateLimited,
                    now_mono_ns,
                );
                return Err(UtaSubmitRefusal::RateLimited {
                    ors_order_id,
                    reject: rej,
                    report: rep,
                });
            }
        };

        let conn_id = route.connection_id;
        let slot = &self.slots[usize::from(conn_id)];
        slot.record_inflight(
            ThreadTag::REQ,
            TrackedInflight {
                active: true,
                reply_id,
                ors_order_id,
                symbol_id,
                kind: InflightOpKind::Cancel,
                sent_mono_ns: now_mono_ns,
            },
        );
        let req_ref = RequestRef {
            order_key: ors_order_id,
            client_seq: seq,
            sent_tsc: 0,
            sent_mono_ns: now_mono_ns,
        };
        let send_outcome = slot
            .conn
            .send_frame(&wire_buf[..wire_len], reply_id, req_ref);
        match send_outcome {
            SendOutcome::Sent => {
                self.last_picked_conn_id.store(conn_id, Ordering::Relaxed);
                Ok(UtaSubmitOutcome::Dispatched {
                    ors_order_id,
                    conn_id,
                    reply_id,
                    wire_len,
                })
            }
            other => {
                self.rollback_failed_send(conn_id, reply_id);
                let rep = self.order_manager.on_cancel_rejected(
                    ors_order_id,
                    None,
                    TerminalReason::MKT_REJECT_MARKET_DOWN,
                    now_mono_ns,
                );
                Err(UtaSubmitRefusal::SendFailed {
                    ors_order_id,
                    outcome: other,
                    report: rep,
                })
            }
        }
    }

    /// Submits an Options/Block `RFQ` quote via [`RfqQuoteHook::submit_quote`] (returning
    /// `TerminalReason::RFQ_QUOTE_NOT_SUPPORTED` when `NoopRfqQuoteHook` is active).
    pub fn submit_rfq_quote(
        &self,
        req: &RfqQuoteRequest,
        utc_ms: u64,
        now_mono_ns: u64,
    ) -> Result<UtaSubmitOutcome, UtaSubmitRefusal> {
        let mut body_buf = [0u8; 1536];
        let built = self
            .rfq_hook
            .submit_quote(self.venue, req, &mut body_buf)
            .map_err(|reason| UtaSubmitRefusal::RfqRefused {
                ors_order_id: req.ors_order_id,
                reason,
            })?;

        let creds = self.credentials.credentials();
        let mut wire_buf = [0u8; 2560];
        let wire_len = encode_signed_https_post_into(
            &creds,
            self.environment,
            utc_ms,
            built.path,
            &body_buf[..built.body_len],
            &mut wire_buf,
        )
        .map_err(|_| {
            let _ = self.rfq_hook.on_quote_terminal(req.ors_order_id);
            UtaSubmitRefusal::Encoding(TerminalReason::MKT_REJECT)
        })?;

        let first_sym = req.legs[0].symbol.as_str();
        let scope = instrument_rate_scope(self.venue, Product::Option, first_sym);
        let quota = match self.quota() {
            Ok(q) => q,
            Err(rej) => {
                let _ = self.rfq_hook.on_quote_terminal(req.ors_order_id);
                return Err(UtaSubmitRefusal::RateLimited {
                    ors_order_id: req.ors_order_id,
                    reject: rej,
                    report: None,
                });
            }
        };
        let mut claimed_conn: Option<u16> = None;
        let (route, _permit) = match quota.pick_route_and_place_or_nack(
            || {
                let r = self.pick_and_claim_route(Product::Option)?;
                claimed_conn = Some(r.connection_id);
                Some(r)
            },
            scope,
            1,
            self.ip_order_weight,
            now_mono_ns,
        ) {
            Ok(p) => p,
            Err(
                RouteAdmissionReject::NoReadyConnection
                | RouteAdmissionReject::MissingLocalIp { .. },
            ) => {
                if let Some(cid) = claimed_conn {
                    self.release_claimed_inflight(cid);
                }
                let _ = self.rfq_hook.on_quote_terminal(req.ors_order_id);
                return Err(UtaSubmitRefusal::NoReadyRoute {
                    ors_order_id: req.ors_order_id,
                    report: None,
                });
            }
            Err(RouteAdmissionReject::RateLimitedNack(rej)) => {
                if let Some(cid) = claimed_conn {
                    self.release_claimed_inflight(cid);
                }
                let _ = self.rfq_hook.on_quote_terminal(req.ors_order_id);
                return Err(UtaSubmitRefusal::RateLimited {
                    ors_order_id: req.ors_order_id,
                    reject: rej,
                    report: None,
                });
            }
        };

        let reply_id = self.alloc_reply_id();
        let conn_id = route.connection_id;
        let slot = &self.slots[usize::from(conn_id)];
        slot.record_inflight(
            ThreadTag::REQ,
            TrackedInflight {
                active: true,
                reply_id,
                ors_order_id: req.ors_order_id,
                symbol_id: 0,
                kind: InflightOpKind::RfqSubmit,
                sent_mono_ns: now_mono_ns,
            },
        );
        let req_ref = RequestRef {
            order_key: req.ors_order_id,
            client_seq: reply_id,
            sent_tsc: 0,
            sent_mono_ns: now_mono_ns,
        };
        let outcome = slot
            .conn
            .send_frame(&wire_buf[..wire_len], reply_id, req_ref);
        if outcome == SendOutcome::Sent {
            self.last_picked_conn_id.store(conn_id, Ordering::Relaxed);
            Ok(UtaSubmitOutcome::Dispatched {
                ors_order_id: req.ors_order_id,
                conn_id,
                reply_id,
                wire_len,
            })
        } else {
            self.rollback_failed_send(conn_id, reply_id);
            let _ = self.rfq_hook.on_quote_terminal(req.ors_order_id);
            Err(UtaSubmitRefusal::SendFailed {
                ors_order_id: req.ors_order_id,
                outcome,
                report: None,
            })
        }
    }

    /// Cancels one or all Options/Block `RFQ` quotes via [`RfqQuoteHook::cancel_quotes`].
    pub fn cancel_rfq_quotes(
        &self,
        req: &RfqCancelQuotesRequest,
        utc_ms: u64,
        now_mono_ns: u64,
    ) -> Result<UtaSubmitOutcome, UtaSubmitRefusal> {
        let oid = req.ors_order_id.unwrap_or(0);
        let mut body_buf = [0u8; 512];
        let built = self
            .rfq_hook
            .cancel_quotes(self.venue, req, &mut body_buf)
            .map_err(|reason| UtaSubmitRefusal::RfqRefused {
                ors_order_id: oid,
                reason,
            })?;

        let creds = self.credentials.credentials();
        let mut wire_buf = [0u8; 1536];
        let wire_len = encode_signed_https_post_into(
            &creds,
            self.environment,
            utc_ms,
            built.path,
            &body_buf[..built.body_len],
            &mut wire_buf,
        )
        .map_err(|_| UtaSubmitRefusal::Encoding(TerminalReason::MKT_REJECT))?;

        let scope = instrument_rate_scope(self.venue, Product::Option, "BTC-USD");
        let quota = self.quota().map_err(|rej| UtaSubmitRefusal::RateLimited {
            ors_order_id: oid,
            reject: rej,
            report: None,
        })?;
        let mut claimed_conn: Option<u16> = None;
        let (route, _permit) = match quota.pick_route_and_cancel_or_nack(
            || {
                let r = self.pick_and_claim_route(Product::Option)?;
                claimed_conn = Some(r.connection_id);
                Some(r)
            },
            scope,
            self.ip_order_weight,
            now_mono_ns,
        ) {
            Ok(p) => p,
            Err(
                RouteAdmissionReject::NoReadyConnection
                | RouteAdmissionReject::MissingLocalIp { .. },
            ) => {
                if let Some(cid) = claimed_conn {
                    self.release_claimed_inflight(cid);
                }
                return Err(UtaSubmitRefusal::NoReadyRoute {
                    ors_order_id: oid,
                    report: None,
                });
            }
            Err(RouteAdmissionReject::RateLimitedNack(rej)) => {
                if let Some(cid) = claimed_conn {
                    self.release_claimed_inflight(cid);
                }
                return Err(UtaSubmitRefusal::RateLimited {
                    ors_order_id: oid,
                    reject: rej,
                    report: None,
                });
            }
        };

        let reply_id = self.alloc_reply_id();
        let conn_id = route.connection_id;
        let slot = &self.slots[usize::from(conn_id)];
        slot.record_inflight(
            ThreadTag::REQ,
            TrackedInflight {
                active: true,
                reply_id,
                ors_order_id: oid,
                symbol_id: 0,
                kind: InflightOpKind::RfqCancel,
                sent_mono_ns: now_mono_ns,
            },
        );
        let req_ref = RequestRef {
            order_key: oid,
            client_seq: reply_id,
            sent_tsc: 0,
            sent_mono_ns: now_mono_ns,
        };
        let outcome = slot
            .conn
            .send_frame(&wire_buf[..wire_len], reply_id, req_ref);
        if outcome == SendOutcome::Sent {
            self.last_picked_conn_id.store(conn_id, Ordering::Relaxed);
            Ok(UtaSubmitOutcome::Dispatched {
                ors_order_id: oid,
                conn_id,
                reply_id,
                wire_len,
            })
        } else {
            self.rollback_failed_send(conn_id, reply_id);
            Err(UtaSubmitRefusal::SendFailed {
                ors_order_id: oid,
                outcome,
                report: None,
            })
        }
    }

    /// Single-threaded `RESP` poll loop: drives all order pool connections, dedicated private WS
    /// stream, `20s` WS heartbeats, graceful maintenance-drain completion, and `DialBaton`
    /// replacement handoffs with zero heap allocation on the steady-state receive path.
    pub fn poll_resp(
        &self,
        now: Instant,
        now_mono_ns: u64,
        utc_ms: u64,
        out_reports: &mut Vec<OrsReport>,
    ) -> Result<UtaPollReport, UtaVenueError> {
        let mut report = UtaPollReport::default();
        let mut raw_chunk = [0u8; 16 * 1024];

        for idx in 0..self.slots.len() {
            let slot = &self.slots[idx];
            let conn_id = slot.conn_id;

            // 1. Poll raw socket/TLS bytes under `Connection` spinlock, then release immediately.
            let poll_res = slot
                .conn
                .poll_raw_rx(now, IoBudget::default(), &mut raw_chunk);
            match poll_res {
                Ok((drv, read_n)) => {
                    if read_n > 0 {
                        self.process_slot_raw_bytes(
                            conn_id,
                            &raw_chunk[..read_n],
                            now_mono_ns,
                            utc_ms,
                            out_reports,
                            &mut report,
                        )?;
                    }
                    if drv.peer_closed || slot.conn.is_close_pending() {
                        self.handle_slot_disconnect(conn_id, now_mono_ns)?;
                    }
                }
                Err(tickwire_transport::Error::Closed) => {}
                Err(_) => {
                    self.handle_slot_disconnect(conn_id, now_mono_ns)?;
                }
            }

            // 2. Send due `20s` WS heartbeat if authenticated and not draining.
            if self.transport_mode == UtaTransportMode::WebSocketTrade
                && slot.ws_authenticated.load(Ordering::Acquire)
                && !slot.draining.load(Ordering::Acquire)
            {
                let last_ping = slot.resp_rx.lock(ThreadTag::RESP).last_ping_mono_ns;
                if now_mono_ns.saturating_sub(last_ping) >= WS_PING_PERIOD_NS {
                    let mut pjson = [0u8; 32];
                    let plen = encode_ws_ping_into(self.venue, &mut pjson)?;
                    let mut pframe = [0u8; 48];
                    let flen = encode_ws_client_frame_into(
                        Opcode::Text,
                        &pjson[..plen],
                        now_mono_ns ^ u64::from(conn_id),
                        &mut pframe,
                    )?;
                    if slot.conn.send_control_frame(&pframe[..flen]) == SendOutcome::Sent {
                        slot.resp_rx.lock(ThreadTag::RESP).last_ping_mono_ns = now_mono_ns;
                    }
                }
            }

            // 3. Complete graceful maintenance switchover once all claimed and wire in-flight replies have drained!
            if slot.draining.load(Ordering::SeqCst)
                && slot.claimed_inflight.load(Ordering::SeqCst) == 0
                && slot.conn.inflight_relaxed() == 0
            {
                let can_retire_now = slot
                    .baton
                    .as_ref()
                    .is_none_or(|b| b.state() == tickwire_transport::dialer::DialState::Ready);
                if can_retire_now {
                    slot.established.store(false, Ordering::SeqCst);
                    slot.ws_authenticated.store(false, Ordering::Release);
                    let retired = slot.conn.close_connection();
                    drop(retired);
                    {
                        let mut router = self.router_lock.lock(ThreadTag::RESP);
                        let _ = router.on_disconnect(conn_id);
                    }
                    slot.ema_score_us.store(UNMEASURED_SCORE, Ordering::Release);
                    slot.draining.store(false, Ordering::SeqCst);
                    slot.clear_tracked_and_claims(ThreadTag::RESP);
                    {
                        let mut rx_guard = slot.resp_rx.lock(ThreadTag::RESP);
                        rx_guard.rx_len = 0;
                        rx_guard.ws_sub_phase = SlotWsSubPhase::None;
                        rx_guard.generation = rx_guard.generation.wrapping_add(1);
                    }
                    self.republish_routes()?;
                }
            }

            // 4. Take ready pre-warmed `DialedLink` from cold `Dialer` when slot is empty.
            if !slot.established.load(Ordering::Acquire)
                && let Some(link) = slot.baton.as_ref().and_then(|b| b.take_ready())
            {
                self.install_order_connection(
                    conn_id,
                    link.connection,
                    false,
                    utc_ms,
                    now_mono_ns,
                )?;
                report.replacements_installed += 1;
            }
        }

        // 5. Drive dedicated private WS stream if installed.
        if let Ok((drv, read_n)) =
            self.private_conn
                .poll_raw_rx(now, IoBudget::default(), &mut raw_chunk)
        {
            if read_n > 0 {
                self.process_private_raw_bytes(
                    &raw_chunk[..read_n],
                    now_mono_ns,
                    utc_ms,
                    out_reports,
                    &mut report,
                )?;
            }
            if drv.peer_closed || self.private_conn.is_close_pending() {
                let retired = self.private_conn.close_connection();
                drop(retired);
                self.private_subscribed.store(false, Ordering::Release);
                let _ = self
                    .order_manager
                    .on_socket_drop(self.account_ref, now_mono_ns);
                if let Some(baton) = &self.private_baton {
                    let _ = baton.want(now_mono_ns);
                }
            }
        }
        if self.private_subscribed.load(Ordering::Acquire)
            && !self.private_draining.load(Ordering::Acquire)
        {
            let mut pframe = [0u8; 48];
            let mut priv_stream = {
                let mut guard = self.private_stream_lock.lock(ThreadTag::RESP);
                core::mem::replace(&mut *guard, UtaPrivateStream::empty_placeholder(self.venue))
            };
            let ping_res = if priv_stream.has_rx_capacity() {
                priv_stream.encode_due_ping(now_mono_ns, &mut pframe)
            } else {
                Ok(0)
            };
            if priv_stream.has_rx_capacity() {
                let mut guard = self.private_stream_lock.lock(ThreadTag::RESP);
                if !guard.has_rx_capacity() {
                    *guard = priv_stream;
                }
            }
            let flen = ping_res?;
            if flen > 0 {
                let _ = self.private_conn.send_control_frame(&pframe[..flen]);
            }
        }
        if self.private_draining.load(Ordering::Acquire)
            && let Some(link) = self.private_baton.as_ref().and_then(|b| b.take_ready())
        {
            self.install_private_connection(link.connection, utc_ms, now_mono_ns)?;
            report.replacements_installed += 1;
        }

        Ok(report)
    }

    /// Direct deterministic `RESP` frame injection helper for WS JSON payloads on order slot `conn_id`
    /// (executes the exact single-pass `JsonScan` + graceful `notice` drain + `OrderManager` state
    /// transition pipeline without requiring kernel sockets).
    pub fn apply_order_ws_payload(
        &self,
        conn_id: u16,
        payload: &[u8],
        now_mono_ns: u64,
        utc_ms: u64,
        out_reports: &mut Vec<OrsReport>,
    ) -> Result<UtaPollReport, UtaVenueError> {
        let mut report = UtaPollReport::default();
        self.handle_slot_ws_text(
            conn_id,
            payload,
            now_mono_ns,
            utc_ms,
            out_reports,
            &mut report,
        )?;
        Ok(report)
    }

    /// Direct deterministic `RESP` HTTP/1.1 response injection helper on order slot `conn_id`.
    pub fn apply_order_https_bytes(
        &self,
        conn_id: u16,
        raw_http: &[u8],
        now_mono_ns: u64,
        utc_ms: u64,
        out_reports: &mut Vec<OrsReport>,
    ) -> Result<UtaPollReport, UtaVenueError> {
        let mut report = UtaPollReport::default();
        self.process_slot_raw_bytes(
            conn_id,
            raw_http,
            now_mono_ns,
            utc_ms,
            out_reports,
            &mut report,
        )?;
        Ok(report)
    }

    /// Direct deterministic `RESP` private WS JSON payload injection helper.
    pub fn apply_private_ws_payload(
        &self,
        payload: &[u8],
        now_mono_ns: u64,
        utc_ms: u64,
        out_reports: &mut Vec<OrsReport>,
    ) -> Result<UtaPollReport, UtaVenueError> {
        let mut report = UtaPollReport::default();
        let mut pending_cancels = PendingCancelList::new();
        let summary = match self.venue {
            SpotVenue::Bybit => scan_bybit_frame(payload, |ev| {
                self.apply_scanned_event(
                    None,
                    ev,
                    now_mono_ns,
                    out_reports,
                    &mut pending_cancels,
                    &mut report,
                );
            })?,
            SpotVenue::Okx => scan_okx_frame(payload, |ev| {
                self.apply_scanned_event(
                    None,
                    ev,
                    now_mono_ns,
                    out_reports,
                    &mut pending_cancels,
                    &mut report,
                );
            })?,
        };
        report.frames_processed += 1;
        if summary.graceful_drain_connection && !self.private_draining.swap(true, Ordering::AcqRel)
        {
            report.maintenance_drains_started += 1;
            if let Some(baton) = &self.private_baton {
                let _ = baton.want(now_mono_ns);
            }
        }
        self.dispatch_queued_cancels(pending_cancels.as_slice(), now_mono_ns, utc_ms, &mut report);
        Ok(report)
    }

    /// Polls due `OrsQueryDesk` entries (`StatusAsk` and exponential `RetryCancel`) for this account:
    /// - Enqueues `RestTask::status_ask` onto `querier` for `StatusAsk` entries (`UtaRestRequestBuilder`
    ///   reads `(symbol_id, client_id, venue_order_id)` directly via `OrderManager::lock_order`).
    /// - Dispatches `RetryCancel` frames over the session's shared connection pool.
    pub fn poll_due_queries(
        &self,
        querier: Option<&AsyncRestQuerier>,
        now_mono_ns: u64,
        utc_ms: u64,
    ) -> usize {
        let mut actions = [DueQueryAction::RetryCancel {
            ors_order_id: 0,
            account_ref: 0,
            symbol_id: 0,
            attempt: 0,
        }; 16];
        let count = self.order_manager.query_desk().poll_due(
            self.account_ref,
            now_mono_ns,
            &mut actions,
            |s_idx, oid| self.order_manager.is_slot_live(s_idx, oid),
        );
        let mut dispatched = 0usize;
        for action in actions.iter().copied().take(count) {
            match action {
                DueQueryAction::StatusAsk { ors_order_id, .. } => {
                    if let Some(q) = querier {
                        let task = RestTask::status_ask(
                            ors_order_id,
                            1,
                            now_mono_ns,
                            now_mono_ns.saturating_add(5_000_000_000),
                        );
                        if q.enqueue(task) {
                            dispatched += 1;
                        }
                    }
                }
                DueQueryAction::RetryCancel {
                    ors_order_id,
                    symbol_id,
                    attempt,
                    ..
                } => {
                    let (cid, vid) = self.read_order_identifiers(ors_order_id);
                    if self
                        .dispatch_wire_cancel(
                            ors_order_id,
                            symbol_id,
                            cid.as_str(),
                            vid.as_str(),
                            u64::from(attempt),
                            now_mono_ns,
                            utc_ms,
                        )
                        .is_ok()
                    {
                        dispatched += 1;
                    }
                }
            }
        }
        dispatched
    }

    /// Applies a `RestReply` (`STATUS_ASK`) returned by `AsyncRestQuerier` via
    /// [`decode_status_ask_snapshot`] and [`OrderManager::reconcile_query_snapshot`], automatically
    /// dispatching any coalesced `cancel_after` cancel request if the order is confirmed live!
    pub fn apply_status_ask_reply(
        &self,
        reply: &RestReply,
        fills_body: Option<&[u8]>,
        now_mono_ns: u64,
        utc_ms: u64,
        out_reports: &mut Vec<OrsReport>,
    ) -> Result<OrsQueryReconcileAction, UtaVenueError> {
        if reply.task.kind != query_kind::STATUS_ASK {
            return Err(UtaVenueError::Encoding);
        }
        let ors_order_id = reply.task.key;
        if reply.status != RestStatus::Ok || is_http_account_hazard(reply.http_code) {
            if reply.http_code == 429 {
                let _ = self.rate_limiter.note_account_cooldown(
                    self.exchange_uid,
                    now_mono_ns.saturating_add(DEFAULT_RATE_LIMIT_COOLDOWN_NS),
                );
            }
            if let Some(rep) = self
                .order_manager
                .on_query_inconclusive(ors_order_id, now_mono_ns)
            {
                out_reports.push(rep);
            }
            return Ok(OrsQueryReconcileAction::Resolved);
        }

        let Some((target, binding)) =
            lookup_status_ask_target(&self.order_manager, &self.symbols, ors_order_id)
        else {
            return Ok(OrsQueryReconcileAction::Resolved);
        };

        let decoded = match decode_status_ask_snapshot(
            self.venue,
            &target,
            &binding.spec,
            &self.assets,
            &reply.body,
            fills_body,
            now_mono_ns,
        ) {
            Ok(d) => d,
            Err(_) => {
                if let Some(rep) = self
                    .order_manager
                    .on_query_inconclusive(ors_order_id, now_mono_ns)
                {
                    out_reports.push(rep);
                }
                return Ok(OrsQueryReconcileAction::Resolved);
            }
        };

        if let Some(vid) = decoded.venue_order_id.as_deref() {
            self.remember_venue_order_id(vid, ors_order_id);
        }
        let snapshot = decoded.as_query_snapshot();
        let action = self
            .order_manager
            .reconcile_query_snapshot(&snapshot, out_reports)
            .map_err(UtaVenueError::Refusal)?;

        if let OrsQueryReconcileAction::DispatchCancel {
            ors_order_id: cid_oid,
            symbol_id,
            ..
        } = action
        {
            let (cid, vid) = self.read_order_identifiers(cid_oid);
            let _ = self.dispatch_wire_cancel(
                cid_oid,
                symbol_id,
                cid.as_str(),
                vid.as_str(),
                1,
                now_mono_ns,
                utc_ms,
            );
        }
        Ok(action)
    }

    fn process_slot_raw_bytes(
        &self,
        conn_id: u16,
        raw: &[u8],
        now_mono_ns: u64,
        utc_ms: u64,
        out_reports: &mut Vec<OrsReport>,
        poll_report: &mut UtaPollReport,
    ) -> Result<(), UtaVenueError> {
        if raw.is_empty() {
            return Ok(());
        }
        let idx = usize::from(conn_id);
        let slot = self.slots.get(idx).ok_or(UtaVenueError::Bounds)?;

        // Swap the preallocated RX buffer out with a zero-allocation `Box::new([])` placeholder so
        // all WS/HTTP decoding, `JsonScan`, and `OrderManager` updates run outside every spinlock.
        let (mut rx_buf, mut rx_len, generation) = {
            let mut guard = slot.resp_rx.lock(ThreadTag::RESP);
            if guard.rx_buf.is_empty() {
                return Err(UtaVenueError::Bounds);
            }
            let buf = core::mem::replace(&mut guard.rx_buf, Box::new([]));
            (buf, guard.rx_len, guard.generation)
        };

        let proc_res = self.decode_and_apply_slot_rx(
            conn_id,
            &mut rx_buf,
            &mut rx_len,
            raw,
            now_mono_ns,
            utc_ms,
            out_reports,
            poll_report,
        );

        {
            let mut guard = slot.resp_rx.lock(ThreadTag::RESP);
            if guard.generation == generation && guard.rx_buf.is_empty() {
                guard.rx_buf = rx_buf;
                guard.rx_len = rx_len;
            }
        }

        proc_res
    }

    #[allow(clippy::too_many_arguments)]
    fn decode_and_apply_slot_rx(
        &self,
        conn_id: u16,
        rx_buf: &mut [u8],
        rx_len: &mut usize,
        raw: &[u8],
        now_mono_ns: u64,
        utc_ms: u64,
        out_reports: &mut Vec<OrsReport>,
        poll_report: &mut UtaPollReport,
    ) -> Result<(), UtaVenueError> {
        let slot = &self.slots[usize::from(conn_id)];
        let end = rx_len.checked_add(raw.len()).ok_or(UtaVenueError::Bounds)?;
        if end > rx_buf.len() {
            return Err(UtaVenueError::Bounds);
        }
        rx_buf[*rx_len..end].copy_from_slice(raw);
        *rx_len = end;

        let mut consumed_total = 0usize;
        let step_res: Result<(), UtaVenueError> = match self.transport_mode {
            UtaTransportMode::WebSocketTrade => loop {
                let frame_opt = try_decode_ws_server_frame(&rx_buf[consumed_total..*rx_len])?;
                let Some(frame) = frame_opt else {
                    break Ok(());
                };
                let frame_opcode = frame.opcode_raw;
                let frame_consumed = frame.consumed;
                let payload_start = consumed_total + (frame_consumed - frame.payload.len());
                let payload_end = consumed_total + frame_consumed;
                consumed_total += frame_consumed;

                match frame_opcode {
                    9 => {
                        let mut pong_frame = [0u8; 160];
                        let pslice = &rx_buf[payload_start..payload_end];
                        let flen = encode_ws_client_frame_into(
                            Opcode::Pong,
                            pslice,
                            now_mono_ns,
                            &mut pong_frame,
                        )?;
                        let _ = slot.conn.send_control_frame(&pong_frame[..flen]);
                    }
                    8 => {
                        slot.conn.request_close(ThreadTag::RESP);
                        break Ok(());
                    }
                    1 => {
                        let mut stack_payload = [0u8; 16 * 1024];
                        let plen = payload_end - payload_start;
                        if plen <= stack_payload.len() {
                            stack_payload[..plen]
                                .copy_from_slice(&rx_buf[payload_start..payload_end]);
                            if let Err(e) = self.handle_slot_ws_text(
                                conn_id,
                                &stack_payload[..plen],
                                now_mono_ns,
                                utc_ms,
                                out_reports,
                                poll_report,
                            ) {
                                break Err(e);
                            }
                        } else {
                            break Err(UtaVenueError::Bounds);
                        }
                    }
                    _ => {}
                }
            },
            UtaTransportMode::PooledHttps => loop {
                let resp_opt = try_decode_http_response(&rx_buf[consumed_total..*rx_len])?;
                let Some(resp) = resp_opt else {
                    break Ok(());
                };
                let http_status = resp.status;
                let bapi_limit = resp.bapi_limit;
                let bapi_limit_status = resp.bapi_limit_status;
                let retry_after_secs = resp.retry_after_secs;
                let resp_consumed = resp.consumed;
                let body_start = consumed_total + (resp_consumed - resp.body.len());
                let body_end = consumed_total + resp_consumed;
                consumed_total += resp_consumed;

                let mut stack_body = [0u8; 16 * 1024];
                let blen = body_end - body_start;
                if blen > stack_body.len() {
                    break Err(UtaVenueError::Bounds);
                }
                stack_body[..blen].copy_from_slice(&rx_buf[body_start..body_end]);

                if let Err(e) = self.handle_slot_http_response(
                    conn_id,
                    http_status,
                    bapi_limit,
                    bapi_limit_status,
                    retry_after_secs,
                    &stack_body[..blen],
                    now_mono_ns,
                    utc_ms,
                    out_reports,
                    poll_report,
                ) {
                    break Err(e);
                }
            },
        };

        if consumed_total > 0 {
            rx_buf.copy_within(consumed_total..*rx_len, 0);
            *rx_len -= consumed_total;
        }
        step_res
    }

    fn handle_slot_ws_text(
        &self,
        conn_id: u16,
        payload: &[u8],
        now_mono_ns: u64,
        utc_ms: u64,
        out_reports: &mut Vec<OrsReport>,
        poll_report: &mut UtaPollReport,
    ) -> Result<(), UtaVenueError> {
        let idx = usize::from(conn_id);
        let slot = self.slots.get(idx).ok_or(UtaVenueError::Bounds)?;
        poll_report.frames_processed += 1;

        if !slot.ws_authenticated.load(Ordering::Acquire) {
            let login_out = match self.venue {
                SpotVenue::Bybit => check_bybit_auth_reply(payload),
                SpotVenue::Okx => check_okx_login_reply(payload),
            };
            match login_out {
                WsLoginOutcome::Accepted => {
                    let _ = slot.conn.complete_ws_login();
                    slot.ws_authenticated.store(true, Ordering::Release);
                    if self.venue == SpotVenue::Okx && self.okx_ws_auto_subscribe_private {
                        let mut sub_json = [0u8; 256];
                        let slen = encode_okx_subscribe_step_into(0, &mut sub_json)?;
                        let mut sframe = [0u8; 300];
                        let flen = encode_ws_client_frame_into(
                            Opcode::Text,
                            &sub_json[..slen],
                            now_mono_ns,
                            &mut sframe,
                        )?;
                        let _ = slot.conn.send_control_frame(&sframe[..flen]);
                        slot.resp_rx.lock(ThreadTag::RESP).ws_sub_phase =
                            SlotWsSubPhase::Subscribing { next_step: 1 };
                    }
                    self.republish_routes()?;
                    return Ok(());
                }
                WsLoginOutcome::Rejected => {
                    self.handle_slot_disconnect(conn_id, now_mono_ns)?;
                    return Err(UtaVenueError::Identity);
                }
                WsLoginOutcome::NotLoginFrame => {}
            }
        }

        let mut pending_cancels = PendingCancelList::new();
        let mut saw_sub_ack = false;
        let summary = match self.venue {
            SpotVenue::Bybit => scan_bybit_frame(payload, |ev| {
                self.apply_scanned_event(
                    Some(conn_id),
                    ev,
                    now_mono_ns,
                    out_reports,
                    &mut pending_cancels,
                    poll_report,
                );
            })?,
            SpotVenue::Okx => scan_okx_frame(payload, |ev| {
                if let UtaFrameEvent::Control {
                    op: "subscribe",
                    success: true,
                } = ev
                {
                    saw_sub_ack = true;
                }
                self.apply_scanned_event(
                    Some(conn_id),
                    ev,
                    now_mono_ns,
                    out_reports,
                    &mut pending_cancels,
                    poll_report,
                );
            })?,
        };

        if saw_sub_ack {
            let phase = slot.resp_rx.lock(ThreadTag::RESP).ws_sub_phase;
            if let SlotWsSubPhase::Subscribing { next_step } = phase {
                let mut sub_json = [0u8; 256];
                let slen = encode_okx_subscribe_step_into(next_step, &mut sub_json)?;
                if slen == 0 {
                    slot.resp_rx.lock(ThreadTag::RESP).ws_sub_phase = SlotWsSubPhase::Subscribed;
                } else {
                    let mut sframe = [0u8; 300];
                    let flen = encode_ws_client_frame_into(
                        Opcode::Text,
                        &sub_json[..slen],
                        now_mono_ns ^ u64::from(next_step),
                        &mut sframe,
                    )?;
                    let _ = slot.conn.send_control_frame(&sframe[..flen]);
                    slot.resp_rx.lock(ThreadTag::RESP).ws_sub_phase = SlotWsSubPhase::Subscribing {
                        next_step: next_step + 1,
                    };
                }
            }
        }

        if let Some((used, limit)) = summary.bapi_limit {
            self.record_venue_rate_telemetry(conn_id, used, limit, now_mono_ns, poll_report);
        }
        if summary.rate_limited {
            self.record_rate_limit_hit(conn_id, None, false, now_mono_ns, poll_report);
        }
        if summary.graceful_drain_connection {
            self.initiate_graceful_maintenance_drain(conn_id, now_mono_ns, poll_report)?;
        }

        self.republish_routes()?;
        self.dispatch_queued_cancels(pending_cancels.as_slice(), now_mono_ns, utc_ms, poll_report);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_slot_http_response(
        &self,
        conn_id: u16,
        http_status: u16,
        bapi_limit: Option<u64>,
        bapi_limit_status: Option<u64>,
        retry_after_secs: Option<u64>,
        body: &[u8],
        now_mono_ns: u64,
        utc_ms: u64,
        out_reports: &mut Vec<OrsReport>,
        poll_report: &mut UtaPollReport,
    ) -> Result<(), UtaVenueError> {
        let idx = usize::from(conn_id);
        let slot = self.slots.get(idx).ok_or(UtaVenueError::Bounds)?;
        poll_report.frames_processed += 1;

        let req_ref = slot.conn.complete_https_reply();
        let tracked = slot.take_any_inflight(ThreadTag::RESP);
        if tracked.is_some() {
            let _ = slot.sub_claim_saturating();
        }
        let sent_ns = req_ref
            .map(|rr| rr.sent_mono_ns)
            .or_else(|| tracked.map(|tr| tr.sent_mono_ns));
        if let Some(sent_mono_ns) = sent_ns
            && sent_mono_ns > 0
            && now_mono_ns >= sent_mono_ns
        {
            let rtt_us = ((now_mono_ns - sent_mono_ns) / 1_000).min(u64::from(u32::MAX)) as u32;
            let _ = self.note_router_rtt_sample(conn_id, SampleKind::Order, rtt_us);
        }

        if let (Some(limit), Some(rem)) = (bapi_limit, bapi_limit_status) {
            let used = limit.saturating_sub(rem);
            self.record_venue_rate_telemetry(conn_id, used, limit, now_mono_ns, poll_report);
        }

        if http_status == 429 || http_status == 418 {
            self.record_rate_limit_hit(conn_id, retry_after_secs, true, now_mono_ns, poll_report);
            if let Some(tr) = tracked {
                self.apply_tracked_error(
                    tr,
                    Some(i64::from(http_status)),
                    "HTTP_RATE_LIMITED",
                    SpotIssueKind::DeterministicReject,
                    Some(TerminalReason::RateLimited),
                    now_mono_ns,
                    out_reports,
                );
            }
            self.republish_routes()?;
            return Ok(());
        }

        if http_status >= 500 {
            if let Some(tr) = tracked {
                self.apply_tracked_error(
                    tr,
                    Some(i64::from(http_status)),
                    "HTTP_5XX",
                    SpotIssueKind::Ambiguous,
                    Some(TerminalReason::MKT_DELAYED),
                    now_mono_ns,
                    out_reports,
                );
            }
            self.republish_routes()?;
            return Ok(());
        }

        // For RFQ HTTP responses (`{"retCode":0,"result":{"quoteId":...}}` / `{"code":"0","data":[{"quoteId":...}]}`):
        if let Some(tr) = tracked
            && matches!(
                tr.kind,
                InflightOpKind::RfqSubmit | InflightOpKind::RfqCancel
            )
        {
            self.handle_rfq_http_reply(tr, http_status, body);
            self.republish_routes()?;
            return Ok(());
        }

        let mut pending_cancels = PendingCancelList::new();
        let mut applied_reply = false;
        let summary = match self.venue {
            SpotVenue::Bybit => scan_bybit_frame(body, |ev| {
                if let UtaFrameEvent::CommandReply(mut rep) = ev {
                    applied_reply = true;
                    if rep.reply_id.is_none() {
                        rep.reply_id = tracked.map(|t| t.reply_id);
                    }
                    self.apply_command_reply_with_tracked(
                        conn_id,
                        rep,
                        tracked,
                        now_mono_ns,
                        out_reports,
                        &mut pending_cancels,
                    );
                } else {
                    self.apply_scanned_event(
                        Some(conn_id),
                        ev,
                        now_mono_ns,
                        out_reports,
                        &mut pending_cancels,
                        poll_report,
                    );
                }
            })?,
            SpotVenue::Okx => scan_okx_frame(body, |ev| {
                if let UtaFrameEvent::CommandReply(mut rep) = ev {
                    applied_reply = true;
                    if rep.reply_id.is_none() {
                        rep.reply_id = tracked.map(|t| t.reply_id);
                    }
                    self.apply_command_reply_with_tracked(
                        conn_id,
                        rep,
                        tracked,
                        now_mono_ns,
                        out_reports,
                        &mut pending_cancels,
                    );
                } else {
                    self.apply_scanned_event(
                        Some(conn_id),
                        ev,
                        now_mono_ns,
                        out_reports,
                        &mut pending_cancels,
                        poll_report,
                    );
                }
            })?,
        };

        if !applied_reply
            && http_status >= 400
            && let Some(tr) = tracked
        {
            self.apply_tracked_error(
                tr,
                Some(i64::from(http_status)),
                "HTTP_ERROR",
                SpotIssueKind::DeterministicReject,
                Some(TerminalReason::MKT_REJECT),
                now_mono_ns,
                out_reports,
            );
        }

        if let Some((used, limit)) = summary.bapi_limit {
            self.record_venue_rate_telemetry(conn_id, used, limit, now_mono_ns, poll_report);
        }
        if summary.rate_limited {
            self.record_rate_limit_hit(conn_id, retry_after_secs, false, now_mono_ns, poll_report);
        }
        if summary.graceful_drain_connection {
            self.initiate_graceful_maintenance_drain(conn_id, now_mono_ns, poll_report)?;
        }

        self.republish_routes()?;
        self.dispatch_queued_cancels(pending_cancels.as_slice(), now_mono_ns, utc_ms, poll_report);
        Ok(())
    }

    fn process_private_raw_bytes(
        &self,
        raw: &[u8],
        now_mono_ns: u64,
        utc_ms: u64,
        out_reports: &mut Vec<OrsReport>,
        poll_report: &mut UtaPollReport,
    ) -> Result<(), UtaVenueError> {
        let mut tx_buf = [0u8; 1024];
        let mut pending_cancels = PendingCancelList::new();

        // Swap `UtaPrivateStream` out of its leaf spinlock with a zero-allocation placeholder so
        // RFC 6455 frame decoding, `JsonScan`, and `OrderManager` updates execute outside all spinlocks.
        let mut priv_stream = {
            let mut guard = self.private_stream_lock.lock(ThreadTag::RESP);
            core::mem::replace(&mut *guard, UtaPrivateStream::empty_placeholder(self.venue))
        };
        if !priv_stream.has_rx_capacity() {
            return Ok(());
        }

        let res = priv_stream.on_raw_bytes(raw, now_mono_ns, &mut tx_buf, |ev| {
            self.apply_scanned_event(
                None,
                ev,
                now_mono_ns,
                out_reports,
                &mut pending_cancels,
                poll_report,
            );
        });
        let subscribed = priv_stream.is_subscribed();
        {
            let mut guard = self.private_stream_lock.lock(ThreadTag::RESP);
            if !guard.has_rx_capacity() {
                *guard = priv_stream;
                self.private_subscribed.store(subscribed, Ordering::Release);
            }
        }

        let (summary, tx_len) = res?;
        if tx_len > 0 {
            let _ = self.private_conn.send_control_frame(&tx_buf[..tx_len]);
        }
        if summary.events_emitted > 0 {
            poll_report.frames_processed += 1;
        }
        if summary.graceful_drain_connection && !self.private_draining.swap(true, Ordering::AcqRel)
        {
            poll_report.maintenance_drains_started += 1;
            if let Some(baton) = &self.private_baton {
                let _ = baton.want(now_mono_ns);
            }
        }
        self.dispatch_queued_cancels(pending_cancels.as_slice(), now_mono_ns, utc_ms, poll_report);
        Ok(())
    }

    fn apply_scanned_event(
        &self,
        conn_id: Option<u16>,
        ev: UtaFrameEvent<'_>,
        now_mono_ns: u64,
        out_reports: &mut Vec<OrsReport>,
        pending_cancels: &mut PendingCancelList,
        _poll_report: &mut UtaPollReport,
    ) {
        match ev {
            UtaFrameEvent::CommandReply(rep) => {
                if let Some(cid) = conn_id {
                    let slot = &self.slots[usize::from(cid)];
                    let tracked = rep.reply_id.and_then(|rid| {
                        let req_ref = slot.conn.match_ws_reply(rid);
                        let tr = slot.take_inflight_by_reply_id(ThreadTag::RESP, rid);
                        if tr.is_some() {
                            let _ = slot.sub_claim_saturating();
                        }
                        let sent_ns = req_ref
                            .map(|rr| rr.sent_mono_ns)
                            .or_else(|| tr.map(|t| t.sent_mono_ns));
                        if let Some(sent_mono_ns) = sent_ns
                            && sent_mono_ns > 0
                            && now_mono_ns >= sent_mono_ns
                        {
                            let rtt_us = ((now_mono_ns - sent_mono_ns) / 1_000)
                                .min(u64::from(u32::MAX))
                                as u32;
                            let _ = self.note_router_rtt_sample(cid, SampleKind::Order, rtt_us);
                        }
                        tr
                    });
                    self.apply_command_reply_with_tracked(
                        cid,
                        rep,
                        tracked,
                        now_mono_ns,
                        out_reports,
                        pending_cancels,
                    );
                }
            }
            UtaFrameEvent::OrderRow(row) => {
                self.apply_order_row(&row, now_mono_ns, out_reports, pending_cancels);
            }
            UtaFrameEvent::ExecRow(row) => {
                self.apply_exec_row(&row, now_mono_ns, out_reports);
            }
            UtaFrameEvent::WalletCoin(coin) => {
                self.apply_wallet_coin(&coin);
            }
            UtaFrameEvent::Control { .. } | UtaFrameEvent::MaintenanceNotice { .. } => {}
        }
    }

    fn apply_command_reply_with_tracked(
        &self,
        _conn_id: u16,
        rep: UtaCommandReply<'_>,
        tracked: Option<TrackedInflight>,
        now_mono_ns: u64,
        out_reports: &mut Vec<OrsReport>,
        pending_cancels: &mut PendingCancelList,
    ) {
        let ors_order_id = tracked
            .map(|t| t.ors_order_id)
            .or_else(|| self.resolve_order_id(None, rep.client_id, rep.order_id));
        let Some(oid) = ors_order_id.filter(|&id| id != 0) else {
            return;
        };

        if !rep.order_id.is_empty() {
            self.remember_venue_order_id(rep.order_id, oid);
        }

        let op_kind = tracked.map(|t| t.kind).unwrap_or(match rep.op {
            crate::UtaReplyOp::Create | crate::UtaReplyOp::Unknown => InflightOpKind::New,
            crate::UtaReplyOp::Amend => InflightOpKind::Modify {
                new_price_ticks: 0,
                new_qty_lots: 0,
            },
            crate::UtaReplyOp::Cancel => InflightOpKind::Cancel,
        });

        if rep.is_ok() {
            let symbol_id = tracked
                .map(|t| t.symbol_id)
                .or_else(|| self.read_order_symbol_id(oid))
                .unwrap_or(0);
            let spec_opt = self.find_symbol_by_id(symbol_id).map(|b| b.spec);

            match op_kind {
                InflightOpKind::New => {
                    let vid = (!rep.order_id.is_empty()).then_some(rep.order_id);
                    if let Some(front_rep) = self.order_manager.on_front_ack(oid, vid, now_mono_ns)
                    {
                        out_reports.push(front_rep);
                    }
                    let ack_px = spec_opt.and_then(|sp| {
                        (!rep.clamped_price.is_empty()).then(|| {
                            parse_positive_step_i64(
                                rep.clamped_price.as_bytes(),
                                sp.price_scale,
                                sp.price_tick_atoms,
                            )
                        })?
                    });
                    let ack_qty = spec_opt.and_then(|sp| {
                        (!rep.clamped_qty.is_empty()).then(|| {
                            parse_positive_step_i64(
                                rep.clamped_qty.as_bytes(),
                                sp.qty_scale,
                                sp.qty_step_atoms,
                            )
                        })?
                    });
                    if (ack_px.is_some() || ack_qty.is_some())
                        && let Ok(ack) =
                            self.order_manager
                                .on_ack(oid, vid, ack_px, ack_qty, now_mono_ns)
                    {
                        if let Some(r) = ack.report {
                            out_reports.push(r);
                        }
                        if ack.dispatch_pending_cancel {
                            pending_cancels.push(oid);
                        }
                    }
                }
                InflightOpKind::Modify {
                    new_price_ticks,
                    new_qty_lots,
                } => {
                    let px = spec_opt
                        .and_then(|sp| {
                            (!rep.clamped_price.is_empty()).then(|| {
                                parse_positive_step_i64(
                                    rep.clamped_price.as_bytes(),
                                    sp.price_scale,
                                    sp.price_tick_atoms,
                                )
                            })?
                        })
                        .unwrap_or(new_price_ticks);
                    let qty = spec_opt
                        .and_then(|sp| {
                            (!rep.clamped_qty.is_empty()).then(|| {
                                parse_positive_step_i64(
                                    rep.clamped_qty.as_bytes(),
                                    sp.qty_scale,
                                    sp.qty_step_atoms,
                                )
                            })?
                        })
                        .unwrap_or(new_qty_lots);
                    let vid = (!rep.order_id.is_empty()).then_some(rep.order_id);
                    if px > 0
                        && qty > 0
                        && let Ok(Some(r)) =
                            self.order_manager
                                .on_modify_ack(oid, px, qty, vid, now_mono_ns)
                    {
                        out_reports.push(r);
                    }
                }
                InflightOpKind::Cancel => {}
                InflightOpKind::RfqSubmit => {
                    let vid = (!rep.order_id.is_empty()).then_some(rep.order_id);
                    let _ = self.rfq_hook.on_quote_ack(oid, vid);
                }
                InflightOpKind::RfqCancel => {
                    let _ = self.rfq_hook.on_quote_terminal(oid);
                }
            }
            return;
        }

        let err_code_str = rep.effective_error_code();
        let err_code_i64 = tickwire_transport::json_scan::parse_i64_ascii(err_code_str.as_bytes());
        let issue = classify_issue(self.venue, err_code_str);
        let explicit_reason = if rep.rate_limited {
            Some(TerminalReason::RateLimited)
        } else if issue == SpotIssueKind::NotFound {
            Some(TerminalReason::MKT_REJECT_NO_SUCH_ORDER)
        } else {
            classify_reply_reject_reason(self.venue, err_code_str, rep.message)
        };

        let synth_tr = tracked.unwrap_or(TrackedInflight {
            active: false,
            reply_id: rep.reply_id.unwrap_or(0),
            ors_order_id: oid,
            symbol_id: self.read_order_symbol_id(oid).unwrap_or(0),
            kind: op_kind,
            sent_mono_ns: now_mono_ns,
        });
        let msg = if rep.message.is_empty() {
            err_code_str
        } else {
            rep.message
        };
        self.apply_tracked_error(
            synth_tr,
            err_code_i64,
            msg,
            issue,
            explicit_reason,
            now_mono_ns,
            out_reports,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_tracked_error(
        &self,
        tr: TrackedInflight,
        err_code: Option<i64>,
        msg: &str,
        issue: SpotIssueKind,
        explicit_reason: Option<TerminalReason>,
        now_mono_ns: u64,
        out_reports: &mut Vec<OrsReport>,
    ) {
        let oid = tr.ors_order_id;
        match tr.kind {
            InflightOpKind::New => {
                if issue == SpotIssueKind::Ambiguous {
                    // Schedule `OrsQueryDesk` `StatusAsk` via `-1007` / `MKT_DELAYED` without retiring!
                    self.schedule_ambiguous_place_query(oid, now_mono_ns);
                } else if let Some(rep) = self.order_manager.on_rejected(
                    oid,
                    err_code,
                    msg,
                    explicit_reason.or(Some(TerminalReason::MKT_REJECT)),
                    now_mono_ns,
                ) {
                    out_reports.push(rep);
                }
            }
            InflightOpKind::Modify { .. } => {
                let reason = explicit_reason.unwrap_or(match issue {
                    SpotIssueKind::NotFound => TerminalReason::MKT_REJECT_NO_SUCH_ORDER,
                    SpotIssueKind::Ambiguous => TerminalReason::MKT_DELAYED,
                    _ => TerminalReason::MKT_REJECT,
                });
                if let Some(rep) = self
                    .order_manager
                    .on_modify_rejected(oid, reason, now_mono_ns)
                {
                    out_reports.push(rep);
                }
            }
            InflightOpKind::Cancel => {
                let reason = explicit_reason.unwrap_or(match issue {
                    SpotIssueKind::NotFound => TerminalReason::MKT_REJECT_NO_SUCH_ORDER,
                    SpotIssueKind::Ambiguous => TerminalReason::MKT_DELAYED,
                    _ => TerminalReason::MKT_REJECT,
                });
                let code_for_desk = if issue == SpotIssueKind::Ambiguous {
                    Some(-1007)
                } else {
                    err_code
                };
                if let Some(rep) =
                    self.order_manager
                        .on_cancel_rejected(oid, code_for_desk, reason, now_mono_ns)
                {
                    out_reports.push(rep);
                }
            }
            InflightOpKind::RfqSubmit => {
                let _ = self.rfq_hook.on_quote_terminal(oid);
            }
            InflightOpKind::RfqCancel => {}
        }
    }

    fn apply_order_row(
        &self,
        row: &UtaOrderRow<'_>,
        now_mono_ns: u64,
        out_reports: &mut Vec<OrsReport>,
        pending_cancels: &mut PendingCancelList,
    ) {
        let binding_opt = self.find_symbol_by_name(row.symbol).copied();
        let prod_hint = binding_opt.map(|b| b.spec.product);
        let Some(oid) = self.resolve_order_id(prod_hint, row.client_id, row.order_id) else {
            return;
        };
        if !row.order_id.is_empty() {
            self.remember_venue_order_id(row.order_id, oid);
        }
        let Some(binding) = binding_opt.or_else(|| {
            let sid = self.read_order_symbol_id(oid)?;
            self.find_symbol_by_id(sid).copied()
        }) else {
            return;
        };
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

        let Some(status) = row.normalized_status(self.venue) else {
            return;
        };
        let vid = (!row.order_id.is_empty()).then_some(row.order_id);

        match status {
            OrderStatus::New | OrderStatus::PartiallyFilled => {
                if let Some(front_rep) = self.order_manager.on_front_ack(oid, vid, now_mono_ns) {
                    out_reports.push(front_rep);
                }
                if let Ok(ack) =
                    self.order_manager
                        .on_ack(oid, vid, px_ticks, qty_lots, now_mono_ns)
                {
                    if let Some(r) = ack.report {
                        out_reports.push(r);
                    }
                    if ack.dispatch_pending_cancel {
                        pending_cancels.push(oid);
                    }
                }
                if let Some(cum) = cum_lots
                    && cum > 0
                {
                    let proj_px = px_ticks
                        .or_else(|| self.read_order_price_ticks(oid))
                        .unwrap_or(1);
                    if let Ok(Some(proj_rep)) =
                        self.order_manager
                            .on_cum_qty(oid, cum, proj_px, now_mono_ns)
                    {
                        out_reports.push(proj_rep);
                    }
                }
            }
            OrderStatus::Filled => {
                if let Some(front_rep) = self.order_manager.on_front_ack(oid, vid, now_mono_ns) {
                    out_reports.push(front_rep);
                }
                if let Some(cum) = cum_lots
                    && cum > 0
                {
                    let proj_px = px_ticks
                        .or_else(|| self.read_order_price_ticks(oid))
                        .unwrap_or(1);
                    if let Ok(Some(proj_rep)) =
                        self.order_manager
                            .on_cum_qty(oid, cum, proj_px, now_mono_ns)
                    {
                        out_reports.push(proj_rep);
                    }
                }
            }
            OrderStatus::Canceled => {
                let (ord_post_only, ord_tif) = self.read_order_flags(oid);
                let explicit_reason =
                    row.classify_explicit_terminal_reason(self.venue, ord_post_only, ord_tif);
                let stp_tok = (!row.smp_type.is_empty()).then_some(row.smp_type);
                if let Ok(Some(rep)) =
                    self.order_manager
                        .on_canceled(&tickwire_account::OrsCancelObservation {
                            ors_order_id: oid,
                            cum_qty_lots: cum_lots,
                            error_code: None,
                            status_or_msg: row.status_raw,
                            stp_mode_or_reason: stp_tok,
                            explicit_reason,
                            received_mono_ns: now_mono_ns,
                        })
                {
                    out_reports.push(rep);
                }
            }
            OrderStatus::Rejected => {
                let (ord_post_only, ord_tif) = self.read_order_flags(oid);
                let explicit_reason = row
                    .classify_explicit_terminal_reason(self.venue, ord_post_only, ord_tif)
                    .or_else(|| {
                        classify_reply_reject_reason(
                            self.venue,
                            row.reject_reason,
                            row.reject_reason,
                        )
                    });
                let msg = if row.reject_reason.is_empty() {
                    row.status_raw
                } else {
                    row.reject_reason
                };
                if let Some(rep) =
                    self.order_manager
                        .on_rejected(oid, None, msg, explicit_reason, now_mono_ns)
                {
                    out_reports.push(rep);
                }
            }
            _ => {}
        }
    }

    fn apply_exec_row(
        &self,
        row: &UtaExecRow<'_>,
        now_mono_ns: u64,
        out_reports: &mut Vec<OrsReport>,
    ) {
        let binding_opt = self.find_symbol_by_name(row.symbol).copied();
        let prod_hint = binding_opt.map(|b| b.spec.product);
        let Some(oid) = self.resolve_order_id(prod_hint, row.client_id, row.order_id) else {
            return;
        };
        if !row.order_id.is_empty() {
            self.remember_venue_order_id(row.order_id, oid);
        }
        let Some(binding) = binding_opt.or_else(|| {
            let sid = self.read_order_symbol_id(oid)?;
            self.find_symbol_by_id(sid).copied()
        }) else {
            return;
        };
        let Some((tid_inline, exec_ref)) = trade_id_inline(self.venue, row.exec_id) else {
            return;
        };
        let side = match (self.venue, row.side_raw) {
            (SpotVenue::Bybit, "Buy") | (SpotVenue::Okx, "buy") => Side::Buy,
            (SpotVenue::Bybit, "Sell") | (SpotVenue::Okx, "sell") => Side::Sell,
            _ => self.read_order_side(oid).unwrap_or(Side::Buy),
        };
        let spec = binding.spec;
        let Some(price_ticks) = parse_positive_step_i64(
            row.exec_price_raw.as_bytes(),
            spec.price_scale,
            spec.price_tick_atoms,
        ) else {
            return;
        };
        let Some(qty_lots) = parse_positive_step_i64(
            row.exec_qty_raw.as_bytes(),
            spec.qty_scale,
            spec.qty_step_atoms,
        ) else {
            return;
        };

        // `execution.fast` sets `is_fast = true` (`exec_fee_raw = None` -> `fee = None`).
        // Subsequent full `execution` supplies `exec_fee_raw = Some(...)` with the identical
        // `exec_id`, causing `FillRecordPool` inside `OrderManager::on_fill` to emit an
        // `amendment = true` fee patch without double-counting filled quantity or positions!
        let fee: Option<OrsFeeSpec> = if row.is_fast {
            None
        } else {
            self.parse_ws_exec_fee(&spec, side, row.fee_currency, row.exec_fee_raw)
        };

        let obs = OrsFillObservation {
            ors_order_id: oid,
            account_ref: self.account_ref,
            symbol_id: binding.symbol_id,
            side,
            liquidity: row.liquidity,
            exec_ref,
            trade_id: tid_inline,
            qty_lots,
            price_ticks,
            fee,
            received_mono_ns: now_mono_ns,
        };
        if let Ok(Some(rep)) = self.order_manager.on_fill(&obs) {
            out_reports.push(rep);
        }
    }

    fn parse_ws_exec_fee(
        &self,
        spec: &tickwire_account::OrsSymbolSpec,
        side: Side,
        fee_ccy: Option<&str>,
        fee_raw: Option<&str>,
    ) -> Option<OrsFeeSpec> {
        let raw = fee_raw.filter(|s| !s.is_empty())?;
        if let Some(spec_fee) = parse_rest_fee(self.venue, &self.assets, fee_ccy, Some(raw)) {
            return Some(spec_fee);
        }
        // Fallback to settlement/received asset scale when Bybit omits `feeCurrency`:
        let (default_key, default_scale) = match spec.product {
            Product::Spot => match side {
                Side::Buy => (spec.base_key(), spec.base_scale),
                Side::Sell => (spec.quote_key(), spec.quote_scale),
            },
            _ => (spec.settlement_key(), spec.settlement_scale),
        };
        let parsed = parse_signed_scaled_i128(raw.as_bytes(), default_scale)?;
        let signed = match self.venue {
            SpotVenue::Bybit => parsed.checked_neg()?,
            SpotVenue::Okx => parsed,
        };
        Some(OrsFeeSpec::new(default_key, signed, default_scale))
    }

    fn apply_wallet_coin(&self, coin: &UtaWalletCoinRow<'_>) {
        // Keep local open-order holds authoritative; seed unknown wallet coins only when no local
        // balance entry exists yet.
        let Some(asset) = self
            .assets
            .iter()
            .find(|a| a.currency.as_str().eq_ignore_ascii_case(coin.coin))
        else {
            return;
        };
        if self
            .order_manager
            .wallet_ledger()
            .balance(self.account_ref, asset.key)
            .is_some()
        {
            return;
        }
        let avail = parse_signed_scaled_i128(coin.available_balance_raw.as_bytes(), asset.scale)
            .unwrap_or(0)
            .max(0);
        let locked = parse_signed_scaled_i128(coin.locked_raw.as_bytes(), asset.scale)
            .unwrap_or(0)
            .max(0);
        let _ = self.order_manager.wallet_ledger().set_balance(
            self.account_ref,
            asset.key,
            asset.scale,
            avail,
            locked,
        );
    }

    fn initiate_graceful_maintenance_drain(
        &self,
        conn_id: u16,
        now_mono_ns: u64,
        poll_report: &mut UtaPollReport,
    ) -> Result<(), UtaVenueError> {
        let idx = usize::from(conn_id);
        let slot = self.slots.get(idx).ok_or(UtaVenueError::Bounds)?;
        if slot.draining.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        poll_report.maintenance_drains_started += 1;

        // Immediately republish `ready = false` so `REQ` routes zero new orders to this socket!
        self.republish_routes()?;

        // Trigger cold `Dialer` pre-warm on this slot's baton (or any idle sibling baton) so a
        // replacement link establishes in parallel while in-flight replies drain!
        if let Some(baton) = &slot.baton {
            let _ = baton.want(now_mono_ns);
        }
        for sib in self.slots.iter() {
            if sib.conn_id != conn_id
                && !sib.established.load(Ordering::Acquire)
                && let Some(baton) = &sib.baton
            {
                let _ = baton.want(now_mono_ns);
            }
        }
        Ok(())
    }

    fn handle_slot_disconnect(&self, conn_id: u16, now_mono_ns: u64) -> Result<(), UtaVenueError> {
        let idx = usize::from(conn_id);
        let slot = self.slots.get(idx).ok_or(UtaVenueError::Bounds)?;
        let had_inflight =
            slot.claimed_inflight.load(Ordering::Acquire) > 0 || slot.conn.inflight_relaxed() > 0;
        slot.established.store(false, Ordering::SeqCst);
        slot.ws_authenticated.store(false, Ordering::Release);
        let retired = slot.conn.close_connection();
        drop(retired);
        slot.draining.store(false, Ordering::SeqCst);
        slot.clear_tracked_and_claims(ThreadTag::RESP);
        {
            let mut rx_guard = slot.resp_rx.lock(ThreadTag::RESP);
            rx_guard.rx_len = 0;
            rx_guard.ws_sub_phase = SlotWsSubPhase::None;
            rx_guard.generation = rx_guard.generation.wrapping_add(1);
        }
        {
            let mut router = self.router_lock.lock(ThreadTag::RESP);
            let _ = router.on_disconnect(conn_id);
        }
        slot.ema_score_us.store(UNMEASURED_SCORE, Ordering::Release);
        self.republish_routes()?;
        if had_inflight {
            let _ = self
                .order_manager
                .on_socket_drop(self.account_ref, now_mono_ns);
        }
        if let Some(baton) = &slot.baton {
            let _ = baton.want(now_mono_ns);
        }
        Ok(())
    }

    fn record_venue_rate_telemetry(
        &self,
        conn_id: u16,
        used: u64,
        limit: u64,
        now_mono_ns: u64,
        poll_report: &mut UtaPollReport,
    ) {
        if limit == 0 {
            return;
        }
        let ip_key = IpKey::from_v4(self.slots[usize::from(conn_id)].local_ip);
        let id = RateLimitId {
            local_ip: ip_key,
            interval_s: 1,
            account: self.exchange_uid,
            flags: RateFlags::ORDER,
            symbol: 0,
        };
        if let Ok(Some(ev)) = self
            .rate_limiter
            .reconcile_venue_usage(id, used, limit, now_mono_ns)
        {
            poll_report.rate_limit_events.push(ev);
        }
    }

    fn record_rate_limit_hit(
        &self,
        conn_id: u16,
        retry_after_secs: Option<u64>,
        is_http_429: bool,
        now_mono_ns: u64,
        poll_report: &mut UtaPollReport,
    ) {
        let cooldown_ns = retry_after_secs
            .filter(|&s| s > 0)
            .map(|s| s.saturating_mul(1_000_000_000))
            .unwrap_or(DEFAULT_RATE_LIMIT_COOLDOWN_NS);
        let until_ns = now_mono_ns.saturating_add(cooldown_ns);
        let ip_key = IpKey::from_v4(self.slots[usize::from(conn_id)].local_ip);
        if is_http_429 {
            let _ = self.rate_limiter.note_ip_cooldown(ip_key, until_ns);
        } else {
            let _ = self
                .rate_limiter
                .note_account_cooldown(self.exchange_uid, until_ns);
        }
        self.record_venue_rate_telemetry(conn_id, 100, 100, now_mono_ns, poll_report);
    }

    fn handle_rfq_http_reply(&self, tr: TrackedInflight, http_status: u16, body: &[u8]) {
        if http_status != 200 {
            if tr.kind == InflightOpKind::RfqSubmit {
                let _ = self.rfq_hook.on_quote_terminal(tr.ors_order_id);
            }
            return;
        }
        let ok = match self.venue {
            SpotVenue::Bybit => body.windows(11).any(|w| w == b"\"retCode\":0"),
            SpotVenue::Okx => body.windows(10).any(|w| w == b"\"code\":\"0\""),
        };
        if ok {
            match tr.kind {
                InflightOpKind::RfqSubmit => {
                    let _ = self.rfq_hook.on_quote_ack(tr.ors_order_id, None);
                }
                InflightOpKind::RfqCancel => {
                    let _ = self.rfq_hook.on_quote_terminal(tr.ors_order_id);
                }
                _ => {}
            }
        } else if tr.kind == InflightOpKind::RfqSubmit {
            let _ = self.rfq_hook.on_quote_terminal(tr.ors_order_id);
        }
    }

    fn dispatch_queued_cancels(
        &self,
        ors_order_ids: &[u64],
        now_mono_ns: u64,
        utc_ms: u64,
        poll_report: &mut UtaPollReport,
    ) {
        for &oid in ors_order_ids {
            let (cid, vid) = self.read_order_identifiers(oid);
            let Some(symbol_id) = self.read_order_symbol_id(oid) else {
                continue;
            };
            if self
                .dispatch_wire_cancel(
                    oid,
                    symbol_id,
                    cid.as_str(),
                    vid.as_str(),
                    1,
                    now_mono_ns,
                    utc_ms,
                )
                .is_ok()
            {
                poll_report.pending_cancels_dispatched += 1;
            }
        }
    }

    fn schedule_ambiguous_place_query(&self, ors_order_id: u64, now_mono_ns: u64) {
        let Some(locked) = self.order_manager.lock_order(ors_order_id) else {
            return;
        };
        let o = *locked.order();
        drop(locked);
        let _ = self.order_manager.query_desk().schedule_status_ask(
            o.account_ref,
            o.ors_order_id,
            o.slot_idx,
            o.strat_id,
            o.symbol_id,
            o.order_seqnum,
            OrsQueryOrigin::Place,
            now_mono_ns,
            |s_idx, oid| self.order_manager.is_slot_live(s_idx, oid),
        );
    }

    fn read_order_identifiers(&self, ors_order_id: u64) -> (InlineAscii<64>, InlineAscii<64>) {
        let Some(locked) = self.order_manager.lock_order(ors_order_id) else {
            return (InlineAscii::empty(), InlineAscii::empty());
        };
        let o = locked.order();
        (o.client_id, o.venue_order_id)
    }

    fn read_order_symbol_id(&self, ors_order_id: u64) -> Option<u32> {
        let locked = self.order_manager.lock_order(ors_order_id)?;
        Some(locked.order().symbol_id)
    }

    fn read_order_price_ticks(&self, ors_order_id: u64) -> Option<i64> {
        let locked = self.order_manager.lock_order(ors_order_id)?;
        Some(locked.order().price_ticks)
    }

    fn read_order_side(&self, ors_order_id: u64) -> Option<Side> {
        let locked = self.order_manager.lock_order(ors_order_id)?;
        Some(locked.order().side)
    }

    fn read_order_flags(&self, ors_order_id: u64) -> (bool, TimeInForce) {
        let Some(locked) = self.order_manager.lock_order(ors_order_id) else {
            return (false, TimeInForce::Gtc);
        };
        let o = locked.order();
        (o.post_only, o.tif)
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_new_order_wire(
        &self,
        binding: &UtaSymbolBinding,
        req: &OrsNewOrderRequest,
        client_id: &str,
        params: UtaOrderParams,
        reply_id: u64,
        utc_ms: u64,
        out: &mut [u8],
    ) -> Result<usize, UtaVenueError> {
        let input = UtaNewOrderWireInput {
            symbol: binding.symbol.as_str(),
            spec: &binding.spec,
            side: req.side,
            price_ticks: req.price_ticks,
            qty_lots: req.qty_lots,
            client_id,
            params,
        };
        let mut arg_buf = [0u8; 768];
        let mut w = BufWriter::new(&mut arg_buf);
        match self.venue {
            SpotVenue::Bybit => encode_bybit_new_order_arg(&input, &mut w)?,
            SpotVenue::Okx => encode_okx_new_order_arg(&input, &mut w)?,
        }
        let arg_len = w.written();

        match self.transport_mode {
            UtaTransportMode::WebSocketTrade => {
                let mut env_buf = [0u8; 1024];
                let env_len = match self.venue {
                    SpotVenue::Bybit => encode_bybit_ws_trade_envelope_into(
                        reply_id,
                        utc_ms,
                        "order.create",
                        &arg_buf[..arg_len],
                        &mut env_buf,
                    )?,
                    SpotVenue::Okx => encode_okx_ws_trade_envelope_into(
                        reply_id,
                        "order",
                        &arg_buf[..arg_len],
                        &mut env_buf,
                    )?,
                };
                encode_ws_client_frame_into(
                    Opcode::Text,
                    &env_buf[..env_len],
                    utc_ms ^ reply_id,
                    out,
                )
            }
            UtaTransportMode::PooledHttps => {
                let creds = self.credentials.credentials();
                let path = match self.venue {
                    SpotVenue::Bybit => "/v5/order/create",
                    SpotVenue::Okx => "/api/v5/trade/order",
                };
                encode_signed_https_post_into(
                    &creds,
                    self.environment,
                    utc_ms,
                    path,
                    &arg_buf[..arg_len],
                    out,
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_modify_wire(
        &self,
        binding: &UtaSymbolBinding,
        client_id: Option<&str>,
        venue_order_id: Option<&str>,
        new_price_ticks: i64,
        new_qty_lots: i64,
        reply_id: u64,
        utc_ms: u64,
        out: &mut [u8],
    ) -> Result<usize, UtaVenueError> {
        let mut req_id_buf = [0u8; 20];
        let rlen = crate::wire_encode::write_u64_into(&mut req_id_buf, reply_id)?;
        let req_id_str = core::str::from_utf8(&req_id_buf[..rlen]).ok();
        let input = UtaModifyWireInput {
            symbol: binding.symbol.as_str(),
            spec: &binding.spec,
            client_id,
            venue_order_id,
            req_id: req_id_str,
            new_price_ticks,
            new_qty_lots,
        };
        let mut arg_buf = [0u8; 640];
        let mut w = BufWriter::new(&mut arg_buf);
        match self.venue {
            SpotVenue::Bybit => encode_bybit_modify_arg(&input, &mut w)?,
            SpotVenue::Okx => encode_okx_modify_arg(&input, &mut w)?,
        }
        let arg_len = w.written();

        match self.transport_mode {
            UtaTransportMode::WebSocketTrade => {
                let mut env_buf = [0u8; 896];
                let env_len = match self.venue {
                    SpotVenue::Bybit => encode_bybit_ws_trade_envelope_into(
                        reply_id,
                        utc_ms,
                        "order.amend",
                        &arg_buf[..arg_len],
                        &mut env_buf,
                    )?,
                    SpotVenue::Okx => encode_okx_ws_trade_envelope_into(
                        reply_id,
                        "amend-order",
                        &arg_buf[..arg_len],
                        &mut env_buf,
                    )?,
                };
                encode_ws_client_frame_into(
                    Opcode::Text,
                    &env_buf[..env_len],
                    utc_ms ^ reply_id,
                    out,
                )
            }
            UtaTransportMode::PooledHttps => {
                let creds = self.credentials.credentials();
                let path = match self.venue {
                    SpotVenue::Bybit => "/v5/order/amend",
                    SpotVenue::Okx => "/api/v5/trade/amend-order",
                };
                encode_signed_https_post_into(
                    &creds,
                    self.environment,
                    utc_ms,
                    path,
                    &arg_buf[..arg_len],
                    out,
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_cancel_wire(
        &self,
        binding: &UtaSymbolBinding,
        client_id: Option<&str>,
        venue_order_id: Option<&str>,
        reply_id: u64,
        utc_ms: u64,
        out: &mut [u8],
    ) -> Result<usize, UtaVenueError> {
        let input = UtaCancelWireInput {
            symbol: binding.symbol.as_str(),
            product: binding.spec.product,
            client_id,
            venue_order_id,
        };
        let mut arg_buf = [0u8; 512];
        let mut w = BufWriter::new(&mut arg_buf);
        match self.venue {
            SpotVenue::Bybit => encode_bybit_cancel_arg(&input, &mut w)?,
            SpotVenue::Okx => encode_okx_cancel_arg(&input, &mut w)?,
        }
        let arg_len = w.written();

        match self.transport_mode {
            UtaTransportMode::WebSocketTrade => {
                let mut env_buf = [0u8; 768];
                let env_len = match self.venue {
                    SpotVenue::Bybit => encode_bybit_ws_trade_envelope_into(
                        reply_id,
                        utc_ms,
                        "order.cancel",
                        &arg_buf[..arg_len],
                        &mut env_buf,
                    )?,
                    SpotVenue::Okx => encode_okx_ws_trade_envelope_into(
                        reply_id,
                        "cancel-order",
                        &arg_buf[..arg_len],
                        &mut env_buf,
                    )?,
                };
                encode_ws_client_frame_into(
                    Opcode::Text,
                    &env_buf[..env_len],
                    utc_ms ^ reply_id,
                    out,
                )
            }
            UtaTransportMode::PooledHttps => {
                let creds = self.credentials.credentials();
                let path = match self.venue {
                    SpotVenue::Bybit => "/v5/order/cancel",
                    SpotVenue::Okx => "/api/v5/trade/cancel-order",
                };
                encode_signed_https_post_into(
                    &creds,
                    self.environment,
                    utc_ms,
                    path,
                    &arg_buf[..arg_len],
                    out,
                )
            }
        }
    }
}

fn classify_reply_reject_reason(venue: SpotVenue, code: &str, msg: &str) -> Option<TerminalReason> {
    let _ = symbol_rate_key(code);
    match venue {
        SpotVenue::Bybit => match code {
            "10006" | "429" => Some(TerminalReason::RateLimited),
            "110007" | "170131" => Some(TerminalReason::InsufficientFunds),
            "110017" => Some(TerminalReason::MKT_REDUCE_ONLY_CANCELED),
            "110079" | "140001" | "170139" => Some(TerminalReason::MKT_REJECT_POST_ONLY),
            "110001" | "170145" | "170213" => Some(TerminalReason::MKT_REJECT_NO_SUCH_ORDER),
            _ if msg.contains("PostOnly") || msg.contains("post only") => {
                Some(TerminalReason::MKT_REJECT_POST_ONLY)
            }
            _ => None,
        },
        SpotVenue::Okx => match code {
            "50011" | "429" => Some(TerminalReason::RateLimited),
            "51008" | "51119" => Some(TerminalReason::InsufficientFunds),
            "51116" | "51059" => Some(TerminalReason::MKT_REDUCE_ONLY_CANCELED),
            "51019" => Some(TerminalReason::MKT_REJECT_POST_ONLY),
            "51400" | "51401" | "51402" | "51503" | "51603" => {
                Some(TerminalReason::MKT_REJECT_NO_SUCH_ORDER)
            }
            _ if msg.contains("post_only") || msg.contains("post only") => {
                Some(TerminalReason::MKT_REJECT_POST_ONLY)
            }
            _ => None,
        },
    }
}

/// Bybit V5 Unified Trading Account session (`Spot`, `LinearPerp`, `InversePerp`, `LinearFutures`,
/// `InverseFutures`, `Options`, `RPI`, and optional `/v5/rfq/*` `RfqQuoteHook`).
pub struct BybitUtaSession<const CHECK_STP: bool = true>(UtaAccountSession<CHECK_STP>);

impl<const CHECK_STP: bool> BybitUtaSession<CHECK_STP> {
    pub fn new(
        order_manager: Arc<OrderManager<CHECK_STP>>,
        mut cfg: UtaSessionConfig,
    ) -> Result<Self, UtaVenueError> {
        cfg.venue = SpotVenue::Bybit;
        Ok(Self(UtaAccountSession::new(order_manager, cfg)?))
    }

    #[inline]
    pub fn into_inner(self) -> UtaAccountSession<CHECK_STP> {
        self.0
    }
}

impl<const CHECK_STP: bool> Deref for BybitUtaSession<CHECK_STP> {
    type Target = UtaAccountSession<CHECK_STP>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const CHECK_STP: bool> DerefMut for BybitUtaSession<CHECK_STP> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// OKX V5 Unified Trading Account session (`Spot`, `LinearPerp`, `InversePerp`, `LinearFutures`,
/// `InverseFutures`, `Options`, `ordType: "rpi"`, and optional `/api/v5/rfq/*` `RfqQuoteHook`).
pub struct OkxUtaSession<const CHECK_STP: bool = true>(UtaAccountSession<CHECK_STP>);

impl<const CHECK_STP: bool> OkxUtaSession<CHECK_STP> {
    pub fn new(
        order_manager: Arc<OrderManager<CHECK_STP>>,
        mut cfg: UtaSessionConfig,
    ) -> Result<Self, UtaVenueError> {
        cfg.venue = SpotVenue::Okx;
        Ok(Self(UtaAccountSession::new(order_manager, cfg)?))
    }

    #[inline]
    pub fn into_inner(self) -> UtaAccountSession<CHECK_STP> {
        self.0
    }
}

impl<const CHECK_STP: bool> Deref for OkxUtaSession<CHECK_STP> {
    type Target = UtaAccountSession<CHECK_STP>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const CHECK_STP: bool> DerefMut for OkxUtaSession<CHECK_STP> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

const fn assert_send_sync<T: Send + Sync>() {}
const _: () = {
    assert_send_sync::<UtaAccountSession<true>>();
    assert_send_sync::<UtaAccountSession<false>>();
    assert_send_sync::<BybitUtaSession<true>>();
    assert_send_sync::<OkxUtaSession<true>>();
};
