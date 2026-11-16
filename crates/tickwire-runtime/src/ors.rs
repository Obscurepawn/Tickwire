//! Venue-agnostic `OrderRoutingSystem<S: VenueSession>`, `AccountSession<S>`, `OrsCpuConfig`,
//! busy-spin `ReqWorker<S>` (`req_cpu`), and `mio::Poll` `RespWorker<S>` (`resp_cpu`).

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::time::{Duration, Instant};

use mio::{Events, Poll, Token, Waker};
use tickwire_account::ors_query_desk::DueQueryAction;
use tickwire_account::{
    InlineAscii, OrderManager, OrderManagerConfig, OrsAckOutcome, OrsCancelDispatch,
    OrsCancelRequest, OrsModifyRequest, OrsNewOrderRequest, OrsOrderFilter,
    OrsQueryReconcileAction, OrsReport, make_ors_order_id, split_ors_order_id,
};
use tickwire_account_gateway::{AccountManager, AccountSpec, DEFAULT_URL_CONNECTIONS};
use tickwire_control_sys::{CpuSet, Error as OrsControlError, validate_cpu_isolation};
use tickwire_ipc::{
    ALL_STRATS, IntentBody, IntentRecord, ReportBody, intent_flags, intent_kind, report_kind,
    tif_code,
};
use tickwire_quota::rate_limiter::RateFlags;
use tickwire_quota::shared::{
    AccountRateOverrides, ExchRateLimitEvent, IpBudgetSpec, OrsAccountQuota, SharedRateLimiter,
};
use tickwire_risk::{RmsEngine, RmsOrderCheck, RmsRejectReason, RmsReloadHandle, RmsSide};
use tickwire_runtime_policy::{
    AccountAuthSpec, Mode, OrsRuntimePolicy, PolicyAction, PolicyConfig,
};
use tickwire_strategy_sdk::{IntentReader, ReportWriter};
use tickwire_transport::dialer::{
    DialBaton, DialSpec, DialState, DialTransport, Dialer, WsDialAuth,
};
use tickwire_transport::links::{
    FixLink, FixSessionConfig, FixSessionState, GuardedValue, HttpsLink, LinkKind, LinkOps,
    LockedConnection, RequestRef, UrlBindPair, UrlConnections, WsLink, WsLoginState,
};
use tickwire_transport::order_router::{
    MAX_PUBLISHED_ROUTES, PublishedRoutes, RouterConfig, SampleKind, SampleMask,
};
use tickwire_transport::rest_querier::{
    AsyncRestQuerier, RestQuerierConfig, RestReply, RestStatus, RestTask, query_kind,
};
use tickwire_transport::{
    ClockFn, IoBudget, LatencyTracer, Limits, MarkSite, ThreadTag, fix, http, mono_now_ns, ws,
};
use tickwire_venue::{
    OrderRoute as VenueOrderRoute, RefusalReason, Side, TerminalReason, TimeInForce,
};

use crate::admin::{
    AdminCommands, AdminQueue, DopAccountSessionInfo, ReqAdminCommand, RespAdminCommand,
};
use crate::session::{
    AccountSessionContext, LoginReply, QueryStatusRequest, RfqQuoteCancelRequest, RfqQuoteRequest,
    SessionMessageEffects, SessionRequestKind, SessionUrlPoolSpec, SessionVenueEvent,
    SubmitNewOutcome, UrlRole, VenueSession,
};

/// Default microsecond stale-order gate on `REQ` (`23,000 ns` = `23 us`).
pub const DEFAULT_MAX_ORDER_AGE_NS: u64 = 23_000;

/// Default cross-thread `REQ -> RESP` synchronous report queue capacity.
pub const DEFAULT_REQ_REPORT_QUEUE_CAPACITY: usize = 4_096;

/// Default wire encoding scratch buffer size (`16 KiB`).
const WIRE_SCRATCH_BYTES: usize = 16 * 1024;

/// Default raw socket/TLS read chunk size on `RESP` (`64 KiB`).
const RAW_RX_BUFFER_BYTES: usize = 64 * 1024;

const RESP_WAKE_TOKEN: Token = Token(0);

/// Hot vs. cold CPU affinity & epoll wait configuration for an ORS execution process.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrsCpuConfig {
    /// Dedicated core for the `REQ` outbound busy-spin thread (`None` = unpinned).
    pub req_cpu: Option<u32>,
    /// Dedicated core for the `RESP` inbound epoll thread (`None` = unpinned).
    pub resp_cpu: Option<u32>,
    /// Disjoint housekeeping core set for `Dialer`, `AsyncRestQuerier`, `LatencyTracer`,
    /// `ControlServer`, and `main` `SignalWaiter` (`None` = unpinned).
    pub housekeeping_cpus: Option<CpuSet>,
    /// `mio::Poll` timeout in milliseconds on `RESP` (`0` = nonblocking spin-epoll default).
    pub epoll_wait_ms: i32,
}

impl OrsCpuConfig {
    /// Validates `req_cpu != resp_cpu`, `hot_cpus ∩ housekeeping_cpus == ∅`, and online CPU
    /// availability via [`tickwire_control_sys::validate_cpu_isolation`].
    pub fn validate(&self) -> Result<(), OrsControlError> {
        validate_cpu_isolation(
            &[("req_cpu", self.req_cpu), ("resp_cpu", self.resp_cpu)],
            self.housekeeping_cpus.as_ref(),
        )
    }
}

/// Process-level ORS runtime tuning parameters (`1 ORS process = 1 Venue`).
#[derive(Clone)]
pub struct OrsRuntimeConfig {
    pub cpu: OrsCpuConfig,
    /// Stale-quote threshold (`ns`) enforced on `REQ` against `IntentBody::tick_mono_ns`.
    /// `0` disables the stale-order gate.
    pub max_order_age_ns: u64,
    /// Whether `RmsEngine::check_order` is evaluated on `dry_run` orders (`true` by default).
    pub rms_on_dry_run: bool,
    pub exec_generation: u64,
    pub instrument_version: u64,
    pub ip_budgets: Vec<IpBudgetSpec>,
    pub router_config: RouterConfig,
    pub sample_mask: SampleMask,
    pub ws_ping_interval_ms: u64,
    pub clock: ClockFn,
}

impl std::fmt::Debug for OrsRuntimeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrsRuntimeConfig")
            .field("cpu", &self.cpu)
            .field("max_order_age_ns", &self.max_order_age_ns)
            .field("rms_on_dry_run", &self.rms_on_dry_run)
            .field("exec_generation", &self.exec_generation)
            .field("instrument_version", &self.instrument_version)
            .finish_non_exhaustive()
    }
}

impl Default for OrsRuntimeConfig {
    fn default() -> Self {
        Self {
            cpu: OrsCpuConfig::default(),
            max_order_age_ns: DEFAULT_MAX_ORDER_AGE_NS,
            rms_on_dry_run: true,
            exec_generation: 1,
            instrument_version: 1,
            ip_budgets: Vec::new(),
            router_config: RouterConfig::default(),
            sample_mask: SampleMask::default(),
            ws_ping_interval_ms: 15_000,
            clock: Arc::new(mono_now_ns),
        }
    }
}

/// Shared connection slot entry `(conn_id, local_ip, LockedConnection)` on an [`AccountSession`].
pub type ConnectionSlotEntry = (u16, Option<IpAddr>, Arc<LockedConnection>);

/// Shared, lock-free per-account state (`1 AccountSession = 1 Exchange UID`, `account_ref = 1..=255`).
pub struct AccountSession<S: VenueSession> {
    spec: AccountSpec,
    session: Arc<S>,
    published_routes: Arc<PublishedRoutes>,
    ready_flag: Arc<AtomicBool>,
    stream_token: Arc<GuardedValue<String>>,
    connections: Box<[ConnectionSlotEntry]>,
    rest_querier: Option<Arc<AsyncRestQuerier>>,
    last_conn_id: AtomicU16,
    rate_overrides: AccountRateOverrides,
    product_mask: u16,
    colo: bool,
}

impl<S: VenueSession> std::fmt::Debug for AccountSession<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountSession")
            .field("account_ref", &self.spec.account_ref)
            .field("name", &self.spec.name)
            .field("ready", &self.is_ready())
            .field("connection_slots", &self.connections.len())
            .finish_non_exhaustive()
    }
}

impl<S: VenueSession> AccountSession<S> {
    #[inline]
    pub fn spec(&self) -> &AccountSpec {
        &self.spec
    }

    #[inline]
    pub fn account_ref(&self) -> u8 {
        self.spec.account_ref
    }

    #[inline]
    pub fn session(&self) -> &Arc<S> {
        &self.session
    }

    #[inline]
    pub fn published_routes(&self) -> &Arc<PublishedRoutes> {
        &self.published_routes
    }

    #[inline]
    pub fn ready_flag(&self) -> &Arc<AtomicBool> {
        &self.ready_flag
    }

    #[inline]
    pub fn is_ready(&self) -> bool {
        self.ready_flag.load(Ordering::Acquire)
    }

    #[inline]
    pub fn stream_token(&self) -> &Arc<GuardedValue<String>> {
        &self.stream_token
    }

    #[inline]
    pub fn rest_querier(&self) -> Option<&Arc<AsyncRestQuerier>> {
        self.rest_querier.as_ref()
    }

    #[inline]
    pub fn connections(&self) -> &[(u16, Option<IpAddr>, Arc<LockedConnection>)] {
        &self.connections
    }

    #[inline]
    pub fn last_conn_id(&self) -> Option<u16> {
        let raw = self.last_conn_id.load(Ordering::Relaxed);
        (raw != VenueOrderRoute::NO_CONNECTION).then_some(raw)
    }

    #[inline]
    pub fn note_picked_conn(&self, conn_id: u16) {
        self.last_conn_id.store(conn_id, Ordering::Relaxed);
    }

    pub fn dop_session_info(&self) -> DopAccountSessionInfo {
        DopAccountSessionInfo {
            account_ref: self.spec.account_ref,
            local_ip: self.spec.local_ip,
            product_mask: self.product_mask,
            colo: self.colo,
            ready_flag: Arc::clone(&self.ready_flag),
        }
    }
}

enum ProtocolDecoder {
    Empty,
    Ws(ws::Decoder),
    Https(http::ResponseDecoder),
    Fix {
        decoder: fix::Decoder,
        config: Option<FixSessionConfig>,
        next_out_seq: u32,
    },
}

impl ProtocolDecoder {
    fn new(kind: LinkKind, fix_cfg: Option<FixSessionConfig>) -> Self {
        let limits = Limits::default();
        match kind {
            LinkKind::Ws => Self::Ws(
                ws::Decoder::new(limits.max_receive_bytes, limits.max_receive_bytes)
                    .expect("valid default ws::Decoder limits"),
            ),
            LinkKind::Https => Self::Https(
                http::ResponseDecoder::new(limits.max_receive_bytes, 128)
                    .expect("valid default http::ResponseDecoder limits"),
            ),
            LinkKind::Fix => Self::Fix {
                decoder: fix::Decoder::new(limits.max_receive_bytes)
                    .expect("valid default fix::Decoder limits"),
                config: fix_cfg,
                next_out_seq: 2,
            },
        }
    }

    fn reset(&mut self, kind: LinkKind, fix_cfg: Option<FixSessionConfig>) {
        *self = Self::new(kind, fix_cfg);
    }
}

fn encode_fix_admin_frame(
    cfg: &FixSessionConfig,
    seq: u32,
    msg_type: &str,
    extra_tags: &str,
) -> Option<Vec<u8>> {
    let body = format!(
        "35={msg_type}\x0149={}\x0156={}\x0134={seq}\x01{extra_tags}",
        cfg.sender_comp_id, cfg.target_comp_id
    );
    fix::encode(&cfg.begin_string, body.as_bytes(), cfg.max_frame_bytes).ok()
}

fn extract_fix_test_request_id(frame: &[u8]) -> Option<&str> {
    let mut is_test_req = false;
    let mut test_req_id: Option<&str> = None;
    for field in frame.split(|&b| b == 1) {
        if let Some(rest) = field.strip_prefix(b"35=") {
            if rest == b"1" {
                is_test_req = true;
            }
        } else if let Some(rest) = field.strip_prefix(b"112=")
            && let Ok(s) = core::str::from_utf8(rest)
        {
            test_req_id = Some(s);
        }
    }
    if is_test_req { test_req_id } else { None }
}

struct RespSlotRuntime {
    conn_id: u16,
    url_id: Arc<str>,
    role: UrlRole,
    kind: LinkKind,
    local_ip: Option<IpAddr>,
    colo: bool,
    product_mask: u16,
    locked: Arc<LockedConnection>,
    decoder: ProtocolDecoder,
    awaits_login_reply: bool,
    authenticated: bool,
    subscribed: bool,
    last_epoch: u64,
    last_ping_ms: u64,
    is_standby: bool,
    standby_baton: Option<Arc<DialBaton>>,
    standby_established: bool,
}

struct RespPoolState {
    spec: SessionUrlPoolSpec,
    pool: UrlConnections,
    standby_conn_id: Option<u16>,
}

pub(crate) struct RespAccountRuntime<S: VenueSession> {
    pub(crate) account: Arc<AccountSession<S>>,
    pools: Vec<RespPoolState>,
    slots: Vec<RespSlotRuntime>,
    draining_conns: Vec<u16>,
    last_token_refresh_ns: u64,
}

impl<S: VenueSession> RespAccountRuntime<S> {
    pub(crate) fn install_link(&mut self, conn_id: u16, link: LinkOps) -> Result<u64, String> {
        for p in &mut self.pools {
            if p.pool.connection_handle(conn_id).is_some() {
                let epoch = p
                    .pool
                    .install_link(conn_id, link)
                    .map_err(|e| format!("install_link(conn_id={conn_id}) failed: {e}"))?;
                self.on_slot_installed(conn_id, epoch);
                self.sync_published_routes();
                return Ok(epoch);
            }
            if p.standby_conn_id == Some(conn_id)
                && let Some(slot) = self.slots.iter_mut().find(|s| s.conn_id == conn_id)
            {
                if let Some(ip) = link.connection().local_ip() {
                    slot.local_ip = Some(ip);
                }
                let prev = slot.locked.install(link);
                drop(prev);
                slot.standby_established = true;
                slot.last_epoch = slot.last_epoch.wrapping_add(1).max(1);
                let ep = slot.last_epoch;
                let fix_cfg = self.account.session.fix_session(self.account.spec());
                slot.decoder.reset(slot.kind, fix_cfg);
                slot.authenticated = !slot.awaits_login_reply;
                slot.subscribed = false;
                self.run_post_connect_handshake(conn_id);
                self.sync_published_routes();
                return Ok(ep);
            }
        }
        Err(format!(
            "unknown conn_id {conn_id} on account_ref {}",
            self.account.account_ref()
        ))
    }

    fn on_slot_installed(&mut self, conn_id: u16, epoch: u64) {
        let fix_cfg = self.account.session.fix_session(self.account.spec());
        if let Some(slot) = self.slots.iter_mut().find(|s| s.conn_id == conn_id) {
            slot.last_epoch = epoch;
            slot.decoder.reset(slot.kind, fix_cfg);
            slot.authenticated = !slot.awaits_login_reply;
            slot.subscribed = false;
        }
        self.draining_conns.retain(|&id| id != conn_id);
        self.run_post_connect_handshake(conn_id);
    }

    fn run_post_connect_handshake(&mut self, conn_id: u16) {
        let Some(idx) = self.slots.iter().position(|s| s.conn_id == conn_id) else {
            return;
        };
        let role = self.slots[idx].role;
        let kind = self.slots[idx].kind;
        if kind != LinkKind::Ws {
            self.slots[idx].authenticated = true;
            self.slots[idx].subscribed = true;
            return;
        }
        let mut scratch = [0u8; 4096];
        if self.slots[idx].awaits_login_reply && !self.slots[idx].authenticated {
            if let Ok(n) =
                self.account
                    .session
                    .encode_login(self.account.spec(), role, 1, &mut scratch)
                && n > 0
                && let Ok(ws_frame) =
                    ws::encode_client(ws::Opcode::Text, &scratch[..n], scratch.len() + 64)
            {
                let _ = self.slots[idx].locked.send_control_frame(&ws_frame);
            }
            return;
        }
        self.send_subscriptions(idx);
    }

    fn send_subscriptions(&mut self, slot_idx: usize) {
        if self.slots[slot_idx].subscribed {
            return;
        }
        let role = self.slots[slot_idx].role;
        let mut scratch = [0u8; 4096];
        for step in 0..16 {
            match self.account.session.encode_subscribe(
                self.account.spec(),
                role,
                step,
                &mut scratch,
            ) {
                Ok(0) => break,
                Ok(n) if n <= scratch.len() => {
                    if let Ok(ws_frame) =
                        ws::encode_client(ws::Opcode::Text, &scratch[..n], scratch.len() + 64)
                    {
                        let _ = self.slots[slot_idx].locked.send_control_frame(&ws_frame);
                    }
                }
                _ => break,
            }
        }
        self.slots[slot_idx].subscribed = true;
    }

    pub(crate) fn sync_published_routes(&mut self) {
        let mut agg = [VenueOrderRoute::default(); MAX_PUBLISHED_ROUTES];
        let mut out_len = 0usize;
        let mut any_order_ready = false;
        let mut has_stream_pool = false;
        let mut any_stream_ready = false;

        for p in &self.pools {
            if p.spec.role.is_stream() {
                has_stream_pool = true;
            }
            let mut sub = [VenueOrderRoute::default(); MAX_PUBLISHED_ROUTES];
            let n = p.pool.published().snapshot(&mut sub);
            for route in sub.iter().copied().take(n) {
                let draining = self.draining_conns.contains(&route.connection_id);
                let slot_authed = self
                    .slots
                    .iter()
                    .find(|s| s.conn_id == route.connection_id)
                    .is_none_or(|s| s.authenticated);
                let effective_ready = route.ready && slot_authed && !draining;
                if p.spec.role.is_stream() && route.ready && slot_authed {
                    any_stream_ready = true;
                }
                if p.spec.role.is_order() && out_len < MAX_PUBLISHED_ROUTES {
                    let mut r = route;
                    r.ready = effective_ready;
                    if r.ready {
                        any_order_ready = true;
                    }
                    agg[out_len] = r;
                    out_len += 1;
                }
            }
            if let Some(sb_id) = p.standby_conn_id
                && p.spec.role.is_order()
                && out_len < MAX_PUBLISHED_ROUTES
                && let Some(sb_slot) = self.slots.iter().find(|s| s.conn_id == sb_id)
                && sb_slot.standby_established
            {
                let draining = self.draining_conns.contains(&sb_id);
                let ready = sb_slot.locked.is_ready_relaxed() && sb_slot.authenticated && !draining;
                if ready {
                    any_order_ready = true;
                }
                if p.spec.role.is_stream()
                    && sb_slot.locked.is_ready_relaxed()
                    && sb_slot.authenticated
                {
                    any_stream_ready = true;
                }
                agg[out_len] = VenueOrderRoute {
                    connection_id: sb_id,
                    account_ref: self.account.account_ref(),
                    ready,
                    colo: sb_slot.colo,
                    product_mask: sb_slot.product_mask,
                    local_ip: ipv4_u32(sb_slot.local_ip),
                    ema_score_us: VenueOrderRoute::UNMEASURED_SCORE,
                };
                out_len += 1;
            }
        }

        let _ = self
            .account
            .published_routes
            .publish_routes(&agg[..out_len]);
        let session_ready = any_order_ready && (!has_stream_pool || any_stream_ready);
        self.account
            .ready_flag
            .store(session_ready, Ordering::Release);
    }

    fn begin_graceful_drain(&mut self, conn_id: u16, now_mono_ns: u64) {
        if !self.draining_conns.contains(&conn_id) {
            self.draining_conns.push(conn_id);
        }
        // Trigger replacement dial on the owning pool's standby baton if idle.
        for p in &self.pools {
            if p.pool.connection_handle(conn_id).is_some()
                && let Some(sb_id) = p.standby_conn_id
                && let Some(sb_slot) = self.slots.iter().find(|s| s.conn_id == sb_id)
                && !sb_slot.standby_established
                && let Some(baton) = sb_slot.standby_baton.as_ref()
            {
                let _ = baton.want(now_mono_ns);
            }
        }
        self.sync_published_routes();
    }

    fn poll_graceful_drains(
        &mut self,
        now_mono_ns: u64,
        closed_out: &mut Vec<(usize, Option<RequestRef>)>,
    ) {
        if self.draining_conns.is_empty() {
            return;
        }
        let mut any_closed = false;
        let mut idx = 0usize;
        while idx < self.draining_conns.len() {
            let cid = self.draining_conns[idx];
            let inflight = self
                .slots
                .iter()
                .find(|s| s.conn_id == cid)
                .map_or(0, |s| s.locked.inflight_relaxed());
            if inflight == 0 {
                self.draining_conns.swap_remove(idx);
                if let Some(s_idx) = self.slots.iter().position(|s| s.conn_id == cid) {
                    let slot = &mut self.slots[s_idx];
                    slot.locked.request_close(ThreadTag::RESP);
                    if slot.is_standby {
                        let retired = slot.locked.close_connection();
                        let pending = retired.and_then(|mut l| {
                            if let LinkOps::Https(h) = &mut l {
                                h.complete_inflight()
                            } else {
                                None
                            }
                        });
                        slot.standby_established = false;
                        closed_out.push((s_idx, pending));
                    } else {
                        closed_out.push((s_idx, None));
                    }
                    any_closed = true;
                }
                for p in &mut self.pools {
                    if p.pool.connection_handle(cid).is_some() {
                        let _ = p.pool.reap_close_pending(now_mono_ns);
                    }
                }
            } else {
                idx += 1;
            }
        }
        if any_closed {
            self.sync_published_routes();
        }
    }
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

/// Thread-safe (`Send + 'static`) deferred constructor invoked on `"tw-ors-req"` after CPU pinning
/// to map a strategy's `!Send` 1,024-byte `IntentReader` directly on the owning `REQ` thread.
pub type IntentLaneFactory = Box<dyn FnOnce() -> Result<IntentReader, String> + Send + 'static>;

/// Thread-safe (`Send + 'static`) deferred constructor invoked on `"tw-ors-resp"` after CPU
/// pinning to map a strategy's `!Send` 512-byte `ReportWriter` directly on the owning `RESP`
/// thread.
pub type ReportLaneFactory = Box<dyn FnOnce() -> Result<ReportWriter, String> + Send + 'static>;

struct ReqIntentLane {
    strat_id: u8,
    reader: IntentReader,
    seen_login_epoch: u64,
}

struct RespReportLane {
    strat_id: u8,
    writer: ReportWriter,
}

pub(crate) struct ReqWorkerSeed<S: VenueSession> {
    config: OrsRuntimeConfig,
    policy: Arc<OrsRuntimePolicy>,
    order_manager: Arc<OrderManager<true>>,
    rms_engine: RmsEngine,
    rate_limiter: Arc<SharedRateLimiter>,
    accounts_by_ref: Box<[Option<Arc<AccountSession<S>>>; 256]>,
    intent_factories: Vec<(u8, u64, IntentLaneFactory)>,
    req_to_resp: Arc<AdminQueue<ReportBody>>,
    admin: AdminCommands,
    tracer: Arc<LatencyTracer>,
}

impl<S: VenueSession> ReqWorkerSeed<S> {
    pub(crate) fn into_worker(self) -> Result<ReqWorker<S>, String> {
        let mut intent_lanes = Vec::with_capacity(self.intent_factories.len());
        for (strat_id, seen_login_epoch, factory) in self.intent_factories {
            let reader = factory()?;
            intent_lanes.push(ReqIntentLane {
                strat_id,
                reader,
                seen_login_epoch,
            });
        }
        Ok(ReqWorker {
            config: self.config,
            policy: self.policy,
            order_manager: self.order_manager,
            rms_engine: self.rms_engine,
            rate_limiter: self.rate_limiter,
            accounts_by_ref: self.accounts_by_ref,
            intent_lanes,
            req_to_resp: self.req_to_resp,
            admin: self.admin,
            tracer: self.tracer,
            wire_scratch: vec![0u8; WIRE_SCRATCH_BYTES],
        })
    }
}

pub(crate) struct RespWorkerSeed<S: VenueSession> {
    config: OrsRuntimeConfig,
    poll: Poll,
    order_manager: Arc<OrderManager<true>>,
    rms_reload: RmsReloadHandle,
    rate_limiter: Arc<SharedRateLimiter>,
    accounts: Vec<RespAccountRuntime<S>>,
    report_factories: Vec<(u8, ReportLaneFactory)>,
    req_to_resp: Arc<AdminQueue<ReportBody>>,
    admin: AdminCommands,
    tracer: Arc<LatencyTracer>,
}

impl<S: VenueSession> RespWorkerSeed<S> {
    pub(crate) fn into_worker(self) -> Result<RespWorker<S>, String> {
        let mut report_lanes = Vec::with_capacity(self.report_factories.len());
        for (strat_id, factory) in self.report_factories {
            let writer = factory()?;
            report_lanes.push(RespReportLane { strat_id, writer });
        }
        Ok(RespWorker {
            config: self.config,
            poll: self.poll,
            order_manager: self.order_manager,
            rms_reload: self.rms_reload,
            rate_limiter: self.rate_limiter,
            accounts: self.accounts,
            report_lanes,
            req_to_resp: self.req_to_resp,
            admin: self.admin,
            tracer: self.tracer,
            raw_rx_buf: vec![0u8; RAW_RX_BUFFER_BYTES],
            wire_scratch: vec![0u8; WIRE_SCRATCH_BYTES],
            effects_scratch: SessionMessageEffects {
                events: Vec::with_capacity(16),
                rate_limits: Vec::with_capacity(8),
                query_status: Vec::with_capacity(8),
                direct_reports: Vec::with_capacity(8),
                stream_token_expired: false,
                graceful_drain_connection: false,
            },
            installed_scratch: Vec::with_capacity(16),
            disconnected_scratch: Vec::with_capacity(16),
            drain_scratch: Vec::with_capacity(16),
            rest_replies_scratch: Vec::with_capacity(16),
            reconcile_reports_scratch: Vec::with_capacity(16),
        })
    }
}

/// Outbound `REQ` hot-path worker (`"tw-ors-req"`, busy-spin, zero `sleep`).
pub struct ReqWorker<S: VenueSession> {
    config: OrsRuntimeConfig,
    policy: Arc<OrsRuntimePolicy>,
    order_manager: Arc<OrderManager<true>>,
    rms_engine: RmsEngine,
    rate_limiter: Arc<SharedRateLimiter>,
    accounts_by_ref: Box<[Option<Arc<AccountSession<S>>>; 256]>,
    intent_lanes: Vec<ReqIntentLane>,
    req_to_resp: Arc<AdminQueue<ReportBody>>,
    admin: AdminCommands,
    tracer: Arc<LatencyTracer>,
    wire_scratch: Vec<u8>,
}

/// Inbound `RESP` hot-path worker (`"tw-ors-resp"`, `mio::Poll` with `epoll_wait_ms`).
pub struct RespWorker<S: VenueSession> {
    config: OrsRuntimeConfig,
    poll: Poll,
    order_manager: Arc<OrderManager<true>>,
    rms_reload: RmsReloadHandle,
    rate_limiter: Arc<SharedRateLimiter>,
    accounts: Vec<RespAccountRuntime<S>>,
    report_lanes: Vec<RespReportLane>,
    req_to_resp: Arc<AdminQueue<ReportBody>>,
    admin: AdminCommands,
    tracer: Arc<LatencyTracer>,
    raw_rx_buf: Vec<u8>,
    wire_scratch: Vec<u8>,
    effects_scratch: SessionMessageEffects,
    installed_scratch: Vec<u16>,
    disconnected_scratch: Vec<(usize, Option<RequestRef>)>,
    drain_scratch: Vec<u16>,
    rest_replies_scratch: Vec<RestReply>,
    reconcile_reports_scratch: Vec<OrsReport>,
}

/// Cold-startup builder and shared-handle bundle for a single-venue Unified ORS instance
/// (`1 ORS process = 1 Venue`, owning `1..N` `AccountSession<S>`s).
pub struct OrderRoutingSystem<S: VenueSession> {
    config: OrsRuntimeConfig,
    policy: Arc<OrsRuntimePolicy>,
    order_manager: Arc<OrderManager<true>>,
    rms_engine: Option<RmsEngine>,
    rms_reload: RmsReloadHandle,
    rate_limiter: Arc<SharedRateLimiter>,
    accounts: Vec<Arc<AccountSession<S>>>,
    resp_accounts: Option<Vec<RespAccountRuntime<S>>>,
    resp_poll: Option<Poll>,
    resp_waker: Arc<Waker>,
    dialer: Arc<Dialer>,
    tracer: Arc<LatencyTracer>,
    admin: AdminCommands,
    req_to_resp: Arc<AdminQueue<ReportBody>>,
    intent_lane_factories: Vec<(u8, u64, IntentLaneFactory)>,
    report_lane_factories: Vec<(u8, ReportLaneFactory)>,
}

impl<S: VenueSession> std::fmt::Debug for OrderRoutingSystem<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrderRoutingSystem")
            .field("account_count", &self.accounts.len())
            .field("intent_lanes", &self.intent_lane_factories.len())
            .field("report_lanes", &self.report_lane_factories.len())
            .finish_non_exhaustive()
    }
}

impl<S: VenueSession> OrderRoutingSystem<S> {
    /// Builds and cold-bootstraps a stateless `OrderRoutingSystem<S>`:
    /// 1. Validates `OrsCpuConfig` (`req_cpu != resp_cpu`, `hot_cpus ∩ housekeeping_cpus == ∅`).
    /// 2. Configures `OrsRuntimePolicy` and `OrderManager<true>` (`DopState` account routing).
    /// 3. Spawns `housekeeping_cpus`-pinned `LatencyTracer`, `Dialer`, and per-account
    ///    `AsyncRestQuerier`s.
    /// 4. Registers `SharedRateLimiter` per-IP and per-account windows.
    /// 5. Runs `VenueSession::bootstrap_snapshots` -> `OrderManager::bootstrap_from_exchange`
    ///    for every enabled `AccountSession` (zero local WAL/disk state).
    pub fn build(
        config: OrsRuntimeConfig,
        account_manager: &AccountManager,
        allowed_strategies: &[u8],
        order_manager_config: OrderManagerConfig,
        rms_engine: RmsEngine,
        sessions: Vec<(u8, Arc<S>)>,
    ) -> Result<Self, String> {
        config
            .cpu
            .validate()
            .map_err(|e| format!("CPU isolation validation failed: {e}"))?;
        if config.exec_generation == 0 {
            return Err(String::from("exec_generation must be non-zero"));
        }

        let resp_poll = Poll::new().map_err(|e| format!("failed to create RESP mio::Poll: {e}"))?;
        let resp_waker = Arc::new(
            Waker::new(resp_poll.registry(), RESP_WAKE_TOKEN)
                .map_err(|e| format!("failed to create RESP mio::Waker: {e}"))?,
        );

        let admin = AdminCommands::default();
        admin.bind_resp_waker(Arc::clone(&resp_waker));

        let tracer_cfg = tickwire_transport::LatencyTracerConfig {
            housekeeping_cpus: config.cpu.housekeeping_cpus.clone(),
            ..Default::default()
        };
        let tracer = Arc::new(
            LatencyTracer::new(tracer_cfg)
                .map_err(|e| format!("failed to start LatencyTracer: {e}"))?,
        );

        let dialer = Arc::new(
            Dialer::with_config(tickwire_transport::dialer::DialerConfig {
                housekeeping_cpus: config.cpu.housekeeping_cpus.clone(),
                clock: Arc::clone(&config.clock),
            })
            .map_err(|e| format!("failed to start Dialer: {e}"))?,
        );

        let enabled_accounts_cfg: Vec<AccountAuthSpec> = account_manager
            .specs()
            .iter()
            .map(|spec| AccountAuthSpec {
                account_ref: spec.account_ref,
                name: spec.name.clone(),
                strategies: spec.strategies.clone(),
            })
            .collect();
        let policy = Arc::new(
            OrsRuntimePolicy::from_config(&PolicyConfig {
                initial_mode: Mode::Trading,
                strategies: allowed_strategies.to_vec(),
                enabled_accounts: enabled_accounts_cfg,
                default_active: true,
                default_enabled: true,
            })
            .map_err(|e| format!("OrsRuntimePolicy config error: {e}"))?,
        );

        let order_manager = Arc::new(OrderManager::<true>::new(order_manager_config)?);
        for &strat_id in allowed_strategies {
            let mut auth_refs = Vec::new();
            for spec in account_manager.specs() {
                if spec.permits(strat_id) {
                    auth_refs.push(spec.account_ref);
                }
            }
            if !auth_refs.is_empty() {
                order_manager
                    .dop_state()
                    .configure_strategy(strat_id, &auth_refs)
                    .map_err(|r| format!("configure_strategy({strat_id}) failed: {r:?}"))?;
            }
        }

        let rate_limiter = Arc::new(SharedRateLimiter::new());
        let rms_reload = rms_engine.reload_handle();

        let mut accounts = Vec::with_capacity(account_manager.specs().len());
        let mut resp_accounts = Vec::with_capacity(account_manager.specs().len());
        let mut next_conn_id: u16 = 1;
        let mut next_dial_slot_id: u64 = 1;

        for spec in account_manager.specs() {
            let session = sessions
                .iter()
                .find(|(aref, _)| *aref == spec.account_ref)
                .map(|(_, s)| Arc::clone(s))
                .ok_or_else(|| {
                    format!(
                        "missing VenueSession implementation for account '{}' (account_ref={})",
                        spec.name, spec.account_ref
                    )
                })?;

            let profile = session.venue_profile();
            if !config.ip_budgets.is_empty() {
                rate_limiter
                    .install_venue_ip_budgets(profile, &config.ip_budgets)
                    .map_err(|e| format!("install_venue_ip_budgets failed: {e:?}"))?;
            }
            let _ = OrsAccountQuota::register_with_overrides(
                &rate_limiter,
                profile,
                u64::from(spec.account_ref),
                spec.rate_limit,
            )
            .map_err(|e| format!("OrsAccountQuota::register failed: {e:?}"))?;

            let snapshot = session.bootstrap_snapshots(spec)?;
            order_manager
                .bootstrap_from_exchange(spec.account_ref, &snapshot)
                .map_err(|e| {
                    format!(
                        "bootstrap_from_exchange(account_ref={}) failed: {e:?}",
                        spec.account_ref
                    )
                })?;

            let stream_token = Arc::new(GuardedValue::new(
                String::new(),
                u32::from(spec.account_ref),
                Some(Arc::clone(&tracer)),
            ));
            let published_routes = Arc::new(PublishedRoutes::new());
            let ready_flag = Arc::new(AtomicBool::new(false));

            let rest_querier = if let Some(rest_ep) = session.rest_endpoint(spec) {
                let spec_clone = spec.clone();
                let session_clone = Arc::clone(&session);
                let om_clone = Arc::clone(&order_manager);
                let token_clone = Arc::clone(&stream_token);
                let rq_cfg = RestQuerierConfig {
                    host: rest_ep.host.clone(),
                    port: rest_ep.port,
                    initial_peers: rest_ep.initial_peers.clone(),
                    local_ip: spec.local_ip,
                    nic: spec.nic.clone(),
                    security: rest_ep.security.clone(),
                    limits: Limits::default(),
                    capacity: 256,
                    connect_timeout_ms: 3_000,
                    interval_ms: 20,
                    timeout_ms: 3_000,
                    housekeeping_cpus: config.cpu.housekeeping_cpus.clone(),
                    reply_waker: Some(Arc::clone(&resp_waker)),
                };
                let querier = AsyncRestQuerier::with_clock(
                    rq_cfg,
                    move |task: &RestTask, _host: &str, _scratch: &mut Vec<u8>| match task.kind {
                        query_kind::LISTEN_KEY_CREATE => {
                            Ok(session_clone.stream_token_query(&spec_clone, false, None))
                        }
                        query_kind::LISTEN_KEY_KEEPALIVE => {
                            let mut tok_buf = [0u8; 256];
                            let cur = token_clone
                                .copy_bytes(ThreadTag::REST_QUERY, &mut tok_buf)
                                .map(|(n, _)| &tok_buf[..n]);
                            Ok(session_clone.stream_token_query(&spec_clone, true, cur))
                        }
                        query_kind::OPEN_ORDERS_SNAPSHOT => {
                            let mut qs = session_clone.open_orders_queries(&spec_clone);
                            let idx = usize::try_from(task.key).unwrap_or(0);
                            if idx < qs.len() {
                                Ok(Some(qs.swap_remove(idx)))
                            } else {
                                Ok(None)
                            }
                        }
                        query_kind::STATUS_ASK => {
                            let Some(locked) = om_clone.lock_order(task.key) else {
                                return Ok(None);
                            };
                            let o = *locked.order();
                            drop(locked);
                            let req_kind = if o.pending_cxl > 0 {
                                SessionRequestKind::Cancel
                            } else if o.modify_pending {
                                SessionRequestKind::Modify
                            } else {
                                SessionRequestKind::Place
                            };
                            Ok(session_clone.order_status_query(
                                &spec_clone,
                                o.ors_order_id,
                                o.symbol_id,
                                o.client_id.as_str(),
                                o.venue_order_id.as_str(),
                                req_kind,
                            ))
                        }
                        _ => Ok(None),
                    },
                    Arc::clone(&config.clock),
                )
                .map_err(|e| format!("failed to start AsyncRestQuerier: {e}"))?;
                if session.has_stream_token() {
                    let _ =
                        querier.enqueue(RestTask::cold_snapshot(query_kind::LISTEN_KEY_CREATE, 0));
                }
                Some(Arc::new(querier))
            } else {
                None
            };

            let pool_specs = session.url_pools(spec)?;
            let mut resp_pools = Vec::with_capacity(pool_specs.len());
            let mut resp_slots = Vec::new();
            let mut all_conns = Vec::new();
            let mut acct_product_mask = 0u16;
            let mut acct_colo = false;

            for p_spec in pool_specs {
                let binds = spec.effective_url_binds(&p_spec.url_id).unwrap_or_else(|| {
                    let mut pair = UrlBindPair::new(spec.local_ip, DEFAULT_URL_CONNECTIONS);
                    if let Some(ref nic) = spec.nic {
                        pair = pair.with_nic(nic.clone());
                    }
                    vec![pair]
                });
                for b in &binds {
                    if b.colo {
                        acct_colo = true;
                    }
                }
                if p_spec.role.is_order() {
                    acct_product_mask |= p_spec.product_mask;
                }

                let pool_published = Arc::new(PublishedRoutes::new());
                let mut url_pool = UrlConnections::new(
                    spec.account_ref,
                    next_conn_id,
                    p_spec.kind,
                    p_spec.product_mask,
                    &binds,
                    config.router_config,
                    config.sample_mask,
                    Some(Arc::clone(&resp_waker)),
                    Some(Arc::clone(&tracer)),
                    pool_published,
                )
                .map_err(|e| format!("UrlConnections::new({}) failed: {e}", p_spec.url_id))?;

                let slot_count = url_pool.len() as u16;
                let fix_cfg = session.fix_session(spec);
                let awaits_login = session.awaits_login_reply(p_spec.role);

                // Register with Dialer when a dialable host/peer exists.
                let dial_template =
                    build_dial_spec(spec, &session, &p_spec, &stream_token, fix_cfg.as_ref());
                if let Some(ref template) = dial_template {
                    url_pool
                        .register_with_dialer(&dialer, next_dial_slot_id, template)
                        .map_err(|e| format!("register_with_dialer failed: {e}"))?;
                    for offset in 0..u64::from(slot_count) {
                        next_dial_slot_id += 1;
                        let _ = offset;
                    }
                }

                for offset in 0..slot_count {
                    let cid = next_conn_id + offset;
                    let handle = Arc::clone(
                        url_pool
                            .connection_handle(cid)
                            .expect("UrlConnections slot must exist"),
                    );
                    let lip = url_pool.slot_local_ip(cid).or(spec.local_ip);
                    let url_id_arc: Arc<str> = Arc::from(p_spec.url_id.as_str());
                    all_conns.push((cid, lip, Arc::clone(&handle)));
                    resp_slots.push(RespSlotRuntime {
                        conn_id: cid,
                        url_id: url_id_arc,
                        role: p_spec.role,
                        kind: p_spec.kind,
                        local_ip: lip,
                        colo: acct_colo,
                        product_mask: p_spec.product_mask,
                        locked: handle,
                        decoder: ProtocolDecoder::new(p_spec.kind, fix_cfg.clone()),
                        awaits_login_reply: awaits_login,
                        authenticated: !awaits_login,
                        subscribed: false,
                        last_epoch: 0,
                        last_ping_ms: 0,
                        is_standby: false,
                        standby_baton: None,
                        standby_established: false,
                    });
                }
                next_conn_id = next_conn_id
                    .checked_add(slot_count)
                    .ok_or_else(|| String::from("connection_id overflow"))?;

                // Preallocate one standby replacement slot per ORDER pool for `8.8` server-notice
                // `graceful_drain_connection` zero-allocation switchover.
                let standby_conn_id = if p_spec.role.is_order() {
                    let sb_id = next_conn_id;
                    next_conn_id = next_conn_id
                        .checked_add(1)
                        .ok_or_else(|| String::from("standby connection_id overflow"))?;
                    let sb_lip = binds.first().and_then(|b| b.local_ip).or(spec.local_ip);
                    let sb_locked = Arc::new(LockedConnection::new(
                        sb_id,
                        Some(Arc::clone(&resp_waker)),
                        Some(Arc::clone(&tracer)),
                    ));
                    let sb_baton = if let Some(mut tmpl) = dial_template {
                        tmpl.local_ip = sb_lip;
                        tmpl.nic = spec.nic.clone();
                        let baton = dialer
                            .add_slot(next_dial_slot_id, tmpl)
                            .map_err(|e| format!("standby add_slot failed: {e}"))?;
                        baton.set_ready_waker(Arc::clone(&resp_waker));
                        next_dial_slot_id += 1;
                        Some(baton)
                    } else {
                        None
                    };
                    all_conns.push((sb_id, sb_lip, Arc::clone(&sb_locked)));
                    resp_slots.push(RespSlotRuntime {
                        conn_id: sb_id,
                        url_id: Arc::from(p_spec.url_id.as_str()),
                        role: p_spec.role,
                        kind: p_spec.kind,
                        local_ip: sb_lip,
                        colo: acct_colo,
                        product_mask: p_spec.product_mask,
                        locked: sb_locked,
                        decoder: ProtocolDecoder::new(p_spec.kind, fix_cfg),
                        awaits_login_reply: awaits_login,
                        authenticated: !awaits_login,
                        subscribed: false,
                        last_epoch: 0,
                        last_ping_ms: 0,
                        is_standby: true,
                        standby_baton: sb_baton,
                        standby_established: false,
                    });
                    Some(sb_id)
                } else {
                    None
                };

                resp_pools.push(RespPoolState {
                    spec: p_spec,
                    pool: url_pool,
                    standby_conn_id,
                });
            }

            let acct_shared = Arc::new(AccountSession {
                spec: spec.clone(),
                session,
                published_routes,
                ready_flag,
                stream_token,
                connections: all_conns.into_boxed_slice(),
                rest_querier,
                last_conn_id: AtomicU16::new(VenueOrderRoute::NO_CONNECTION),
                rate_overrides: spec.rate_limit,
                product_mask: acct_product_mask,
                colo: acct_colo,
            });

            let mut runtime = RespAccountRuntime {
                account: Arc::clone(&acct_shared),
                pools: resp_pools,
                slots: resp_slots,
                draining_conns: Vec::new(),
                last_token_refresh_ns: 0,
            };
            runtime.sync_published_routes();

            accounts.push(acct_shared);
            resp_accounts.push(runtime);
        }

        Ok(Self {
            config,
            policy,
            order_manager,
            rms_engine: Some(rms_engine),
            rms_reload,
            rate_limiter,
            accounts,
            resp_accounts: Some(resp_accounts),
            resp_poll: Some(resp_poll),
            resp_waker,
            dialer,
            tracer,
            admin,
            req_to_resp: Arc::new(AdminQueue::new(DEFAULT_REQ_REPORT_QUEUE_CAPACITY)),
            intent_lane_factories: Vec::new(),
            report_lane_factories: Vec::new(),
        })
    }

    /// Registers `Send + 'static` deferred constructors for a strategy's outbound 1,024-byte
    /// `IntentReader` (opened on `"tw-ors-req"` after CPU pinning) and inbound 512-byte
    /// `ReportWriter` (opened on `"tw-ors-resp"` after CPU pinning).
    pub fn attach_strategy_lane_factories(
        &mut self,
        strat_id: u8,
        intent_factory: IntentLaneFactory,
        report_factory: ReportLaneFactory,
    ) -> Result<(), String> {
        if !self.policy.is_whitelisted(strat_id) {
            return Err(format!(
                "strat_id {strat_id} is not whitelisted in OrsRuntimePolicy"
            ));
        }
        if self
            .intent_lane_factories
            .iter()
            .any(|(s, _, _)| *s == strat_id)
            || self
                .report_lane_factories
                .iter()
                .any(|(s, _)| *s == strat_id)
        {
            return Err(format!(
                "duplicate strategy lane attachment for strat_id {strat_id}"
            ));
        }
        let seen_login_epoch = self.admin.login_epoch(strat_id);
        self.intent_lane_factories
            .push((strat_id, seen_login_epoch, intent_factory));
        self.report_lane_factories.push((strat_id, report_factory));
        Ok(())
    }

    /// Installs an already-connected [`LinkOps`] into `(account_ref, conn_id)` before spawning
    /// worker threads (used for deterministic socketpair / loopback integration tests).
    pub fn install_link(
        &mut self,
        account_ref: u8,
        conn_id: u16,
        link: LinkOps,
    ) -> Result<u64, String> {
        let resp_accounts = self
            .resp_accounts
            .as_mut()
            .ok_or_else(|| String::from("workers already split"))?;
        let acct = resp_accounts
            .iter_mut()
            .find(|a| a.account.account_ref() == account_ref)
            .ok_or_else(|| format!("unknown account_ref {account_ref}"))?;
        acct.install_link(conn_id, link)
    }

    #[inline]
    pub fn config(&self) -> &OrsRuntimeConfig {
        &self.config
    }

    #[inline]
    pub fn policy(&self) -> &Arc<OrsRuntimePolicy> {
        &self.policy
    }

    #[inline]
    pub fn order_manager(&self) -> &Arc<OrderManager<true>> {
        &self.order_manager
    }

    #[inline]
    pub fn rms_reload_handle(&self) -> &RmsReloadHandle {
        &self.rms_reload
    }

    #[inline]
    pub fn rate_limiter(&self) -> &Arc<SharedRateLimiter> {
        &self.rate_limiter
    }

    #[inline]
    pub fn accounts(&self) -> &[Arc<AccountSession<S>>] {
        &self.accounts
    }

    #[inline]
    pub fn dialer(&self) -> &Arc<Dialer> {
        &self.dialer
    }

    #[inline]
    pub fn latency_tracer(&self) -> &Arc<LatencyTracer> {
        &self.tracer
    }

    #[inline]
    pub fn admin(&self) -> &AdminCommands {
        &self.admin
    }

    #[inline]
    pub fn resp_waker(&self) -> &Arc<Waker> {
        &self.resp_waker
    }

    pub fn dop_account_infos(&self) -> Vec<DopAccountSessionInfo> {
        self.accounts.iter().map(|a| a.dop_session_info()).collect()
    }

    pub(crate) fn take_worker_seeds(
        &mut self,
    ) -> Result<(ReqWorkerSeed<S>, RespWorkerSeed<S>), String> {
        let rms_engine = self
            .rms_engine
            .take()
            .ok_or_else(|| String::from("ReqWorker already taken"))?;
        let resp_accounts = self
            .resp_accounts
            .take()
            .ok_or_else(|| String::from("RespWorker already taken"))?;
        let resp_poll = self
            .resp_poll
            .take()
            .ok_or_else(|| String::from("RespWorker Poll already taken"))?;

        let mut accounts_by_ref: Box<[Option<Arc<AccountSession<S>>>; 256]> =
            Box::new(core::array::from_fn(|_| None));
        for acct in &self.accounts {
            accounts_by_ref[usize::from(acct.account_ref())] = Some(Arc::clone(acct));
        }

        let req_seed = ReqWorkerSeed {
            config: self.config.clone(),
            policy: Arc::clone(&self.policy),
            order_manager: Arc::clone(&self.order_manager),
            rms_engine,
            rate_limiter: Arc::clone(&self.rate_limiter),
            accounts_by_ref,
            intent_factories: core::mem::take(&mut self.intent_lane_factories),
            req_to_resp: Arc::clone(&self.req_to_resp),
            admin: self.admin.clone(),
            tracer: Arc::clone(&self.tracer),
        };

        let resp_seed = RespWorkerSeed {
            config: self.config.clone(),
            poll: resp_poll,
            order_manager: Arc::clone(&self.order_manager),
            rms_reload: self.rms_reload.clone(),
            rate_limiter: Arc::clone(&self.rate_limiter),
            accounts: resp_accounts,
            report_factories: core::mem::take(&mut self.report_lane_factories),
            req_to_resp: Arc::clone(&self.req_to_resp),
            admin: self.admin.clone(),
            tracer: Arc::clone(&self.tracer),
        };

        Ok((req_seed, resp_seed))
    }

    /// Splits this `OrderRoutingSystem<S>` into `(ReqWorker<S>, RespWorker<S>)` on the current
    /// thread (invoking any registered lane factories plus attaching `local_lanes` directly).
    pub fn take_workers_with_lanes(
        &mut self,
        local_lanes: Vec<(u8, IntentReader, ReportWriter)>,
    ) -> Result<(ReqWorker<S>, RespWorker<S>), String> {
        for &(strat_id, _, _) in &local_lanes {
            if !self.policy.is_whitelisted(strat_id) {
                return Err(format!(
                    "strat_id {strat_id} is not whitelisted in OrsRuntimePolicy"
                ));
            }
        }
        let (req_seed, resp_seed) = self.take_worker_seeds()?;
        let mut req_worker = req_seed.into_worker()?;
        let mut resp_worker = resp_seed.into_worker()?;
        for (strat_id, intent_reader, report_writer) in local_lanes {
            if req_worker
                .intent_lanes
                .iter()
                .any(|l| l.strat_id == strat_id)
                || resp_worker
                    .report_lanes
                    .iter()
                    .any(|l| l.strat_id == strat_id)
            {
                return Err(format!(
                    "duplicate strategy lane attachment for strat_id {strat_id}"
                ));
            }
            let seen_login_epoch = self.admin.login_epoch(strat_id);
            req_worker.intent_lanes.push(ReqIntentLane {
                strat_id,
                reader: intent_reader,
                seen_login_epoch,
            });
            resp_worker.report_lanes.push(RespReportLane {
                strat_id,
                writer: report_writer,
            });
        }
        Ok((req_worker, resp_worker))
    }

    /// Splits this `OrderRoutingSystem<S>` into `(ReqWorker<S>, RespWorker<S>)` on the current
    /// thread using any registered lane factories.
    pub fn take_workers(&mut self) -> Result<(ReqWorker<S>, RespWorker<S>), String> {
        self.take_workers_with_lanes(Vec::new())
    }
}

fn build_dial_spec<S: VenueSession>(
    spec: &AccountSpec,
    session: &Arc<S>,
    pool: &SessionUrlPoolSpec,
    stream_token: &Arc<GuardedValue<String>>,
    fix_cfg: Option<&tickwire_transport::FixSessionConfig>,
) -> Option<DialSpec> {
    if pool.initial_peers.is_empty() && (pool.host.is_empty() || pool.port == 0) {
        return None;
    }
    let transport = match pool.kind {
        LinkKind::Https => DialTransport::PlainOrTls,
        LinkKind::Ws => {
            if pool.role.is_stream() && session.has_stream_token() {
                DialTransport::WebSocketAuthenticated {
                    host: pool.host.clone(),
                    path_prefix: pool.path.clone(),
                    url_token: Some(Arc::clone(stream_token)),
                    extra_headers: Vec::new(),
                    auth: WsDialAuth::None,
                }
            } else {
                DialTransport::WebSocket {
                    host: pool.host.clone(),
                    path: pool.path.clone(),
                    extra_headers: Vec::new(),
                }
            }
        }
        LinkKind::Fix => {
            let cfg = fix_cfg?;
            let extra = session
                .fix_logon(spec, "19700101-00:00:00.000")
                .unwrap_or_default();
            let logon_tags = format!(
                "52=19700101-00:00:00.000\x0198=0\x01108={}\x01{extra}",
                cfg.heart_bt_int_sec
            );
            let logon = encode_fix_admin_frame(cfg, 1, "A", &logon_tags)?;
            DialTransport::FixLogon {
                logon_frame: logon,
                max_frame_bytes: Limits::default().max_receive_bytes,
            }
        }
    };
    Some(DialSpec {
        host: pool.host.clone(),
        port: pool.port,
        initial_peers: pool.initial_peers.clone(),
        local_ip: spec.local_ip,
        nic: spec.nic.clone(),
        security: pool.security.clone(),
        limits: Limits::default(),
        transport,
        connect_timeout: Duration::from_secs(3),
        dial_timeout: Duration::from_secs(5),
        backoff: Default::default(),
    })
}

impl<S: VenueSession> ReqWorker<S> {
    /// Runs the dedicated `REQ` busy-spin loop (`core::hint::spin_loop()`, ZERO `sleep`) until
    /// `stop` is set.
    pub fn run_loop(&mut self, stop: &AtomicBool) {
        while !stop.load(Ordering::Acquire) {
            if self.poll_once() == 0 {
                core::hint::spin_loop();
            }
        }
    }

    /// Executes one nonblocking `REQ` pass across outbound strategy `IntentReader`s and, when
    /// outbound rings are idle, drains `control -> REQ` `AdminCommands`.
    pub fn poll_once(&mut self) -> usize {
        let mut work = 0usize;
        for lane_idx in 0..self.intent_lanes.len() {
            let sid = self.intent_lanes[lane_idx].strat_id;
            let cur_epoch = self.admin.login_epoch(sid);
            if cur_epoch != self.intent_lanes[lane_idx].seen_login_epoch {
                self.intent_lanes[lane_idx].reader.reset_msg_seqnum();
                self.intent_lanes[lane_idx].seen_login_epoch = cur_epoch;
            }

            let mut captured: Option<IntentRecord> = None;
            if let Ok(true) = self.intent_lanes[lane_idx]
                .reader
                .poll_zero_copy(|rec, _arena| {
                    captured = Some(rec.clone());
                    Ok(())
                })
                && let Some(record) = captured
            {
                let mut trace_iter = self.tracer.thread_tracer(ThreadTag::REQ);
                trace_iter.begin_iteration();
                trace_iter.mark(MarkSite::SHM_POLL);
                self.process_intent(&record.body);
                trace_iter.end_iteration();
                work += 1;
            }
        }

        if work == 0 {
            while let Some(cmd) = self.admin.pop_req() {
                self.apply_admin_command(cmd);
                work += 1;
            }
        }

        work
    }

    #[inline]
    fn enqueue_report(&self, body: ReportBody) {
        if self.req_to_resp.push(body).is_ok() {
            self.admin.wake_resp();
        }
    }

    fn process_intent(&mut self, body: &IntentBody) {
        let now_mono_ns = (self.config.clock)();
        match body.command_kind {
            intent_kind::NEW_ORDER => self.handle_new_order(body, now_mono_ns),
            intent_kind::CANCEL => self.handle_cancel(body, now_mono_ns),
            intent_kind::MODIFY
            | intent_kind::AMEND_KEEP_PRIORITY
            | intent_kind::CANCEL_REPLACE => self.handle_modify(body, now_mono_ns),
            intent_kind::CANCEL_ALL => self.handle_cancel_all_intent(body, now_mono_ns),
            intent_kind::NEW_RFQ_QUOTE | intent_kind::MODIFY_RFQ_QUOTE => {
                self.handle_rfq_quote(body, now_mono_ns)
            }
            intent_kind::CANCEL_RFQ_QUOTE | intent_kind::CANCEL_ALL_RFQ_QUOTES => {
                self.handle_rfq_cancel(body, now_mono_ns)
            }
            _ => {
                let ors_order_id = make_ors_order_id(body.order_seqnum.max(1), body.strat_id);
                let account_ref = self
                    .policy
                    .resolve_account_ref(body.account_ref, body.strat_id)
                    .unwrap_or(body.account_ref);
                self.enqueue_report(build_nack_report(
                    body,
                    account_ref,
                    ors_order_id,
                    RefusalReason::AbortedBeforeSend,
                    now_mono_ns,
                ));
            }
        }
    }

    fn consume_rejected_new_order_seq(
        &self,
        body: &IntentBody,
        reason: RefusalReason,
    ) -> (u8, RefusalReason) {
        match self.order_manager.dop_state().admit_management_request(
            body.strat_id,
            body.msg_seqnum,
            body.account_ref,
        ) {
            Ok(resolved_acct) => {
                let _ = self
                    .order_manager
                    .dop_state()
                    .observe_recovered_order(body.strat_id, body.order_seqnum);
                (resolved_acct, reason)
            }
            Err(seq_or_auth_err) => {
                let fallback_acct = self
                    .policy
                    .resolve_account_ref(body.account_ref, body.strat_id)
                    .unwrap_or(body.account_ref);
                (fallback_acct, seq_or_auth_err)
            }
        }
    }

    fn handle_new_order(&mut self, body: &IntentBody, now_mono_ns: u64) {
        let strat_id = body.strat_id;
        let ors_order_id = make_ors_order_id(body.order_seqnum, strat_id);

        // 1. Stateless OrsRuntimePolicy check (mode, login, active, account_ref = 0, enabled).
        let resolved_account_ref =
            match self
                .policy
                .evaluate(PolicyAction::NewOrder, body.account_ref, strat_id)
            {
                Ok(acct_ref) => acct_ref,
                Err(refusal) => {
                    let (acct, eff_reason) = self.consume_rejected_new_order_seq(body, refusal);
                    self.enqueue_report(build_nack_report(
                        body,
                        acct,
                        ors_order_id,
                        eff_reason,
                        now_mono_ns,
                    ));
                    return;
                }
            };

        // 2. Microsecond stale-order gate (`23 us` default): refuses ONLY this order with
        //    `RefusalReason::RejectStaleOrder` without freezing any account or strategy!
        if self.config.max_order_age_ns > 0
            && body.tick_mono_ns > 0
            && now_mono_ns.saturating_sub(body.tick_mono_ns) > self.config.max_order_age_ns
        {
            let (acct, eff_reason) =
                self.consume_rejected_new_order_seq(body, RefusalReason::RejectStaleOrder);
            self.enqueue_report(build_nack_report(
                body,
                acct,
                ors_order_id,
                eff_reason,
                now_mono_ns,
            ));
            return;
        }

        let Some(side) = decode_intent_side(body.side) else {
            let (acct, eff_reason) =
                self.consume_rejected_new_order_seq(body, RefusalReason::InvalidQuantity);
            self.enqueue_report(build_nack_report(
                body,
                acct,
                ors_order_id,
                eff_reason,
                now_mono_ns,
            ));
            return;
        };

        let tif = decode_intent_tif(body.time_in_force);
        let dry_run = (body.intent_flags & intent_flags::DRY_RUN) != 0;
        let bypass_rms = (body.intent_flags & intent_flags::BYPASS_RMS) != 0;
        let post_only =
            (body.intent_flags & intent_flags::POST_ONLY) != 0 || tif == TimeInForce::PostOnly;
        let reduce_only = (body.intent_flags & intent_flags::REDUCE_ONLY) != 0;
        let check_stp = (body.intent_flags & intent_flags::CHECK_STP) != 0;
        let symbol_id = u32::try_from(body.instrument_id).unwrap_or(u32::MAX);

        // 3. Flat lock-free RmsEngine check (`O(1)` symbol index, zero locks).
        if !dry_run || self.config.rms_on_dry_run {
            let price_opt = matches!(body.order_type, 1 | 3 | 5).then_some(body.price_ticks);
            let check = RmsOrderCheck {
                symbol_id: symbol_id as usize,
                side: match side {
                    Side::Buy => RmsSide::Buy,
                    Side::Sell => RmsSide::Sell,
                },
                price_ticks: price_opt,
                qty_lots: body.qty_lots,
                reference_price_ticks: None,
                now_mono_ns,
                bypass_rms,
            };
            if let Err(rms_err) = self.rms_engine.check_order(&check) {
                let refusal = map_rms_reject(rms_err);
                let (acct, eff_reason) = self.consume_rejected_new_order_seq(body, refusal);
                self.enqueue_report(build_nack_report(
                    body,
                    acct,
                    ors_order_id,
                    eff_reason,
                    now_mono_ns,
                ));
                return;
            }
        }

        let Some(acct_session) = self.accounts_by_ref[usize::from(resolved_account_ref)].as_ref()
        else {
            let (acct, eff_reason) =
                self.consume_rejected_new_order_seq(body, RefusalReason::InvalidAccountId);
            self.enqueue_report(build_nack_report(
                body,
                acct,
                ors_order_id,
                eff_reason,
                now_mono_ns,
            ));
            return;
        };

        let Ok(account_quota) = OrsAccountQuota::register_with_overrides(
            &self.rate_limiter,
            acct_session.session.venue_profile(),
            u64::from(resolved_account_ref),
            acct_session.rate_overrides,
        ) else {
            let (acct, eff_reason) =
                self.consume_rejected_new_order_seq(body, RefusalReason::RateLimited);
            self.enqueue_report(build_nack_report(
                body,
                acct,
                ors_order_id,
                eff_reason,
                now_mono_ns,
            ));
            return;
        };

        let ctx = AccountSessionContext::new(
            acct_session.spec(),
            &self.order_manager,
            acct_session.published_routes(),
            &self.rate_limiter,
            account_quota,
            acct_session.connections(),
            acct_session.last_conn_id(),
        );
        let req = OrsNewOrderRequest {
            strat_id,
            account_ref: body.account_ref,
            msg_seqnum: body.msg_seqnum,
            order_seqnum: body.order_seqnum,
            symbol_id,
            side,
            price_ticks: body.price_ticks,
            qty_lots: body.qty_lots,
            tif,
            post_only,
            reduce_only,
            check_stp,
            dry_run,
            now_mono_ns,
        };

        match acct_session
            .session
            .submit_new(&ctx, &req, &mut self.wire_scratch)
        {
            Ok(SubmitNewOutcome::Dispatched { connection_id, .. })
            | Ok(SubmitNewOutcome::InDoubtScheduled { connection_id, .. }) => {
                acct_session.note_picked_conn(connection_id);
            }
            Ok(SubmitNewOutcome::DryRunPassed {
                ors_order_id: oid,
                account_ref: aref,
                client_id,
            }) => {
                // Emit synthetic Canceled(AbortedBeforeSend) or FrontAck+Canceled so dry_run
                // callers observe deterministic completion with zero wire side-effects.
                let mut rep = ReportBody {
                    ors_order_id: oid,
                    order_seqnum: body.order_seqnum,
                    orig_seqnum: body.order_seqnum,
                    instrument_id: body.instrument_id,
                    event_mono_ns: now_mono_ns,
                    quantity_lots: body.qty_lots,
                    leaves_qty_lots: 0,
                    price_ticks: body.price_ticks,
                    report_kind: report_kind::CANCELED,
                    order_status: 5,
                    reason: TerminalReason::AbortedBeforeSend.code(),
                    strat_id,
                    account_ref: aref,
                    side: body.side,
                    position_side: body.position_side,
                    order_id: body.order_id,
                    ..Default::default()
                };
                copy_ascii_into(
                    &mut rep.client_id,
                    &mut rep.client_id_len,
                    client_id.as_str(),
                );
                self.enqueue_report(rep);
            }
            Err(refusal) => {
                self.enqueue_report(build_nack_report(
                    body,
                    resolved_account_ref,
                    ors_order_id,
                    refusal,
                    now_mono_ns,
                ));
            }
        }
    }

    fn handle_cancel(&mut self, body: &IntentBody, now_mono_ns: u64) {
        let strat_id = body.strat_id;
        let orig_seq = if body.orig_order_seqnum != 0 {
            body.orig_order_seqnum
        } else {
            body.order_seqnum
        };
        let target_ors_order_id = make_ors_order_id(orig_seq, strat_id);

        let resolved_account_ref =
            match self
                .policy
                .evaluate(PolicyAction::Cancel, body.account_ref, strat_id)
            {
                Ok(a) => a,
                Err(refusal) => {
                    let eff = self
                        .order_manager
                        .dop_state()
                        .admit_management_request(strat_id, body.msg_seqnum, body.account_ref)
                        .err()
                        .unwrap_or(refusal);
                    self.enqueue_report(build_cancel_nack_report(
                        body,
                        body.account_ref,
                        target_ors_order_id,
                        orig_seq,
                        eff,
                        now_mono_ns,
                    ));
                    return;
                }
            };

        let Some(acct_session) = self.accounts_by_ref[usize::from(resolved_account_ref)].as_ref()
        else {
            self.enqueue_report(build_cancel_nack_report(
                body,
                resolved_account_ref,
                target_ors_order_id,
                orig_seq,
                RefusalReason::InvalidAccountId,
                now_mono_ns,
            ));
            return;
        };

        let Ok(account_quota) = OrsAccountQuota::register_with_overrides(
            &self.rate_limiter,
            acct_session.session.venue_profile(),
            u64::from(resolved_account_ref),
            acct_session.rate_overrides,
        ) else {
            self.enqueue_report(build_cancel_nack_report(
                body,
                resolved_account_ref,
                target_ors_order_id,
                orig_seq,
                RefusalReason::RateLimited,
                now_mono_ns,
            ));
            return;
        };

        let ctx = AccountSessionContext::new(
            acct_session.spec(),
            &self.order_manager,
            acct_session.published_routes(),
            &self.rate_limiter,
            account_quota,
            acct_session.connections(),
            acct_session.last_conn_id(),
        );
        let req = OrsCancelRequest {
            strat_id,
            account_ref: body.account_ref,
            msg_seqnum: body.msg_seqnum,
            ors_order_id: target_ors_order_id,
            now_mono_ns,
        };
        if let Err(refusal) = acct_session
            .session
            .submit_cancel(&ctx, &req, &mut self.wire_scratch)
        {
            self.enqueue_report(build_cancel_nack_report(
                body,
                resolved_account_ref,
                target_ors_order_id,
                orig_seq,
                refusal,
                now_mono_ns,
            ));
        }
    }

    fn handle_modify(&mut self, body: &IntentBody, now_mono_ns: u64) {
        let strat_id = body.strat_id;
        let orig_seq = if body.orig_order_seqnum != 0 {
            body.orig_order_seqnum
        } else {
            body.order_seqnum
        };
        let target_ors_order_id = make_ors_order_id(orig_seq, strat_id);

        let resolved_account_ref =
            match self
                .policy
                .evaluate(PolicyAction::Modify, body.account_ref, strat_id)
            {
                Ok(a) => a,
                Err(refusal) => {
                    let eff = self
                        .order_manager
                        .dop_state()
                        .admit_management_request(strat_id, body.msg_seqnum, body.account_ref)
                        .err()
                        .unwrap_or(refusal);
                    self.enqueue_report(build_modify_nack_report(
                        body,
                        body.account_ref,
                        target_ors_order_id,
                        orig_seq,
                        eff,
                        now_mono_ns,
                    ));
                    return;
                }
            };

        let Some(acct_session) = self.accounts_by_ref[usize::from(resolved_account_ref)].as_ref()
        else {
            self.enqueue_report(build_modify_nack_report(
                body,
                resolved_account_ref,
                target_ors_order_id,
                orig_seq,
                RefusalReason::InvalidAccountId,
                now_mono_ns,
            ));
            return;
        };

        let Ok(account_quota) = OrsAccountQuota::register_with_overrides(
            &self.rate_limiter,
            acct_session.session.venue_profile(),
            u64::from(resolved_account_ref),
            acct_session.rate_overrides,
        ) else {
            self.enqueue_report(build_modify_nack_report(
                body,
                resolved_account_ref,
                target_ors_order_id,
                orig_seq,
                RefusalReason::RateLimited,
                now_mono_ns,
            ));
            return;
        };

        let ctx = AccountSessionContext::new(
            acct_session.spec(),
            &self.order_manager,
            acct_session.published_routes(),
            &self.rate_limiter,
            account_quota,
            acct_session.connections(),
            acct_session.last_conn_id(),
        );
        let req = OrsModifyRequest {
            strat_id,
            account_ref: body.account_ref,
            msg_seqnum: body.msg_seqnum,
            ors_order_id: target_ors_order_id,
            new_price_ticks: body.price_ticks,
            new_qty_lots: body.qty_lots,
            now_mono_ns,
        };
        if let Err(refusal) = acct_session
            .session
            .submit_modify(&ctx, &req, &mut self.wire_scratch)
        {
            self.enqueue_report(build_modify_nack_report(
                body,
                resolved_account_ref,
                target_ors_order_id,
                orig_seq,
                refusal,
                now_mono_ns,
            ));
        }
    }

    fn handle_cancel_all_intent(&mut self, body: &IntentBody, now_mono_ns: u64) {
        if body.strat_id != ALL_STRATS
            && let Err(refusal) = self.order_manager.dop_state().admit_management_request(
                body.strat_id,
                body.msg_seqnum,
                body.account_ref,
            )
        {
            self.enqueue_report(build_cancel_nack_report(
                body,
                body.account_ref,
                0,
                0,
                refusal,
                now_mono_ns,
            ));
            return;
        }
        let acct_filter = (body.account_ref != 0).then_some(body.account_ref);
        let sym_filter = (body.instrument_id != 0)
            .then(|| u32::try_from(body.instrument_id).ok())
            .flatten();
        self.sweep_admin_cancels(body.strat_id, acct_filter, sym_filter, now_mono_ns);
    }

    fn handle_rfq_quote(&mut self, body: &IntentBody, now_mono_ns: u64) {
        let strat_id = body.strat_id;
        let ors_order_id = make_ors_order_id(body.order_seqnum, strat_id);
        let resolved_account_ref =
            match self
                .policy
                .evaluate(PolicyAction::RfqQuote, body.account_ref, strat_id)
            {
                Ok(a) => a,
                Err(refusal) => {
                    let (acct, eff) = self.consume_rejected_new_order_seq(body, refusal);
                    self.enqueue_report(build_rfq_nack_report(
                        body,
                        acct,
                        ors_order_id,
                        eff,
                        now_mono_ns,
                    ));
                    return;
                }
            };
        let (acct, seq_check) =
            self.consume_rejected_new_order_seq(body, RefusalReason::RfqQuoteNotSupported);
        if seq_check != RefusalReason::RfqQuoteNotSupported {
            self.enqueue_report(build_rfq_nack_report(
                body,
                acct,
                ors_order_id,
                seq_check,
                now_mono_ns,
            ));
            return;
        }

        let Some(acct_session) = self.accounts_by_ref[usize::from(resolved_account_ref)].as_ref()
        else {
            self.enqueue_report(build_rfq_nack_report(
                body,
                resolved_account_ref,
                ors_order_id,
                RefusalReason::InvalidAccountId,
                now_mono_ns,
            ));
            return;
        };
        let Ok(account_quota) = OrsAccountQuota::register_with_overrides(
            &self.rate_limiter,
            acct_session.session.venue_profile(),
            u64::from(resolved_account_ref),
            acct_session.rate_overrides,
        ) else {
            self.enqueue_report(build_rfq_nack_report(
                body,
                resolved_account_ref,
                ors_order_id,
                RefusalReason::RateLimited,
                now_mono_ns,
            ));
            return;
        };
        let ctx = AccountSessionContext::new(
            acct_session.spec(),
            &self.order_manager,
            acct_session.published_routes(),
            &self.rate_limiter,
            account_quota,
            acct_session.connections(),
            acct_session.last_conn_id(),
        );
        let side = decode_intent_side(body.side).unwrap_or(Side::Buy);
        let req = RfqQuoteRequest {
            strat_id,
            account_ref: resolved_account_ref,
            msg_seqnum: body.msg_seqnum,
            order_seqnum: body.order_seqnum,
            ors_order_id,
            symbol_id: u32::try_from(body.instrument_id).unwrap_or(0),
            side,
            price_ticks: body.price_ticks,
            qty_lots: body.qty_lots,
            is_modify: body.command_kind == intent_kind::MODIFY_RFQ_QUOTE,
            now_mono_ns,
        };
        match acct_session
            .session
            .submit_quote(&ctx, &req, &mut self.wire_scratch)
        {
            Ok(rep) => self.enqueue_report(rep),
            Err(refusal) => self.enqueue_report(build_rfq_nack_report(
                body,
                resolved_account_ref,
                ors_order_id,
                refusal,
                now_mono_ns,
            )),
        }
    }

    fn handle_rfq_cancel(&mut self, body: &IntentBody, now_mono_ns: u64) {
        let strat_id = body.strat_id;
        let orig_seq = body.orig_order_seqnum.max(body.order_seqnum);
        let target_id = make_ors_order_id(orig_seq, strat_id);
        let resolved_account_ref =
            match self
                .policy
                .evaluate(PolicyAction::CancelRfqQuote, body.account_ref, strat_id)
            {
                Ok(a) => a,
                Err(refusal) => {
                    self.enqueue_report(build_rfq_nack_report(
                        body,
                        body.account_ref,
                        target_id,
                        refusal,
                        now_mono_ns,
                    ));
                    return;
                }
            };
        if strat_id != ALL_STRATS
            && let Err(err) = self.order_manager.dop_state().admit_management_request(
                strat_id,
                body.msg_seqnum,
                body.account_ref,
            )
        {
            self.enqueue_report(build_rfq_nack_report(
                body,
                resolved_account_ref,
                target_id,
                err,
                now_mono_ns,
            ));
            return;
        }
        let Some(acct_session) = self.accounts_by_ref[usize::from(resolved_account_ref)].as_ref()
        else {
            return;
        };
        let Ok(account_quota) = OrsAccountQuota::register_with_overrides(
            &self.rate_limiter,
            acct_session.session.venue_profile(),
            u64::from(resolved_account_ref),
            acct_session.rate_overrides,
        ) else {
            return;
        };
        let ctx = AccountSessionContext::new(
            acct_session.spec(),
            &self.order_manager,
            acct_session.published_routes(),
            &self.rate_limiter,
            account_quota,
            acct_session.connections(),
            acct_session.last_conn_id(),
        );
        let req = RfqQuoteCancelRequest {
            strat_id: (strat_id != ALL_STRATS).then_some(strat_id),
            ors_order_id: (body.command_kind == intent_kind::CANCEL_RFQ_QUOTE).then_some(target_id),
            symbol_id: (body.instrument_id != 0)
                .then(|| u32::try_from(body.instrument_id).ok())
                .flatten(),
            now_mono_ns,
        };
        if let Err(refusal) = acct_session
            .session
            .cancel_quotes(&ctx, &req, &mut self.wire_scratch)
        {
            self.enqueue_report(build_rfq_nack_report(
                body,
                resolved_account_ref,
                target_id,
                refusal,
                now_mono_ns,
            ));
        }
    }

    fn apply_admin_command(&mut self, cmd: ReqAdminCommand) {
        let now_mono_ns = (self.config.clock)();
        match cmd {
            ReqAdminCommand::CancelOne {
                ors_order_id,
                account_ref,
            } => {
                self.dispatch_admin_cancel_one(ors_order_id, account_ref, now_mono_ns);
            }
            ReqAdminCommand::CancelAll {
                strat_id,
                account_ref,
                symbol_id,
            } => {
                self.sweep_admin_cancels(strat_id, account_ref, symbol_id, now_mono_ns);
            }
            ReqAdminCommand::Suspend => {
                self.policy.set_mode(Mode::ManageOnly);
                let _ = self
                    .admin
                    .post_resp(RespAdminCommand::BroadcastOrsRestricted);
            }
            ReqAdminCommand::Resume => {
                self.policy.set_mode(Mode::Trading);
            }
            ReqAdminCommand::SetStratEnabled {
                account_ref,
                strat_id,
                enabled,
            } => {
                if let Ok(targets) =
                    self.order_manager
                        .set_strat_enabled_collect(account_ref, strat_id, enabled)
                {
                    for t in targets {
                        self.dispatch_admin_cancel_one(t.ors_order_id, t.account_ref, now_mono_ns);
                    }
                }
            }
            ReqAdminCommand::SetActive { strat_id, active } => {
                let _ = self.policy.set_active(strat_id, active);
            }
            ReqAdminCommand::LogoutClient {
                strat_id,
                cancel_open_orders,
            } => {
                let _ = self.policy.logout(strat_id);
                let _ = self.order_manager.dop_state().logout(strat_id);
                if let Some(lane) = self
                    .intent_lanes
                    .iter_mut()
                    .find(|l| l.strat_id == strat_id)
                {
                    lane.reader.reset_msg_seqnum();
                }
                if cancel_open_orders {
                    self.sweep_admin_cancels(strat_id, None, None, now_mono_ns);
                }
            }
            ReqAdminCommand::ResetIntentSeq { strat_id } => {
                if let Some(lane) = self
                    .intent_lanes
                    .iter_mut()
                    .find(|l| l.strat_id == strat_id)
                {
                    lane.reader.reset_msg_seqnum();
                }
            }
            ReqAdminCommand::RmsReload { generation: _ } => {
                // RmsEngine reads atomic SymbolConfigCell seqlocks directly on every check_order.
            }
        }
    }

    fn sweep_admin_cancels(
        &mut self,
        strat_id: u8,
        account_ref: Option<u8>,
        symbol_id: Option<u32>,
        now_mono_ns: u64,
    ) {
        let filter = OrsOrderFilter {
            strat_id: (strat_id != ALL_STRATS).then_some(strat_id),
            account_ref,
            symbol_id,
        };
        let mut ids = [0u64; 1024];
        let found = self.order_manager.find_orders(&filter, &mut ids);
        for &oid in &ids[..found] {
            let acct_opt = self
                .order_manager
                .lock_order(oid)
                .map(|l| l.order().account_ref);
            if let Some(aref) = acct_opt {
                self.dispatch_admin_cancel_one(oid, aref, now_mono_ns);
            }
        }
    }

    fn dispatch_admin_cancel_one(&mut self, ors_order_id: u64, account_ref: u8, now_mono_ns: u64) {
        let dispatch_opt = {
            if let Some(mut locked) = self.order_manager.lock_order(ors_order_id) {
                let o = locked.order_mut();
                if !o.is_open_or_pending() {
                    None
                } else {
                    o.pending_cxl = o.pending_cxl.saturating_add(1);
                    o.admin_cancel_requested = true;
                    if o.status == tickwire_account::OrsOrderStatus::PendingNew {
                        o.cancel_on_ack = true;
                        None
                    } else {
                        Some(OrsCancelDispatch {
                            ors_order_id: o.ors_order_id,
                            account_ref: o.account_ref,
                            symbol_id: o.symbol_id,
                            client_id: o.client_id,
                            venue_order_id: o.venue_order_id,
                            should_send_now: true,
                            coalesced_in_query_desk: false,
                        })
                    }
                }
            } else {
                None
            }
        };
        let Some(dispatch) = dispatch_opt else {
            return;
        };
        let Some(acct_session) = self.accounts_by_ref[usize::from(account_ref)].as_ref() else {
            return;
        };
        let Ok(account_quota) = OrsAccountQuota::register_with_overrides(
            &self.rate_limiter,
            acct_session.session.venue_profile(),
            u64::from(account_ref),
            acct_session.rate_overrides,
        ) else {
            return;
        };
        let ctx = AccountSessionContext::new(
            acct_session.spec(),
            &self.order_manager,
            acct_session.published_routes(),
            &self.rate_limiter,
            account_quota,
            acct_session.connections(),
            acct_session.last_conn_id(),
        );
        let _ = acct_session.session.dispatch_cancel_wire(
            &ctx,
            &dispatch,
            now_mono_ns,
            &mut self.wire_scratch,
        );
    }
}

impl<S: VenueSession> RespWorker<S> {
    /// Runs the dedicated `RESP` epoll loop (`epoll_wait_ms`) until `stop` is set.
    pub fn run_loop(&mut self, stop: &AtomicBool) {
        let mut events = Events::with_capacity(128);
        let timeout = if self.config.cpu.epoll_wait_ms < 0 {
            None
        } else {
            Some(Duration::from_millis(self.config.cpu.epoll_wait_ms as u64))
        };

        while !stop.load(Ordering::Acquire) {
            let _ = self.poll.poll(&mut events, timeout);
            let _ = self.poll_once();
        }
        let _ = self.poll_once();
    }

    /// Drives one complete `RESP` tick: `Dialer` handoffs, socket/TLS reads, protocol framing &
    /// `VenueSession::on_message` outside spinlocks, `OrsQueryDesk` + `AsyncRestQuerier` replies,
    /// `REQ -> RESP` synchronous reports, and `control -> RESP` `AdminCommands`.
    pub fn poll_once(&mut self) -> usize {
        let now_mono_ns = (self.config.clock)();
        let now_inst = Instant::now();
        let mut work = 0usize;

        let mut trace_iter = self.tracer.thread_tracer(ThreadTag::RESP);
        trace_iter.begin_iteration();

        // 1. Drain synchronous reports queued by `REQ`.
        while let Some(body) = self.req_to_resp.pop() {
            self.publish_report_body(body, now_mono_ns);
            work += 1;
        }

        // 2. Drive each AccountSession.
        for acct_idx in 0..self.accounts.len() {
            work += self.drive_account(acct_idx, now_inst, now_mono_ns);
        }

        // 3. Drain `control / main -> RESP` administrative commands.
        while let Some(cmd) = self.admin.pop_resp() {
            self.apply_resp_admin(cmd, now_mono_ns);
            work += 1;
        }

        trace_iter.end_iteration();
        work
    }

    fn drive_account(&mut self, acct_idx: usize, now_inst: Instant, now_mono_ns: u64) -> usize {
        let mut work = 0usize;
        let account_ref = self.accounts[acct_idx].account.account_ref();

        // 2a. Harvest ready Dialer links for primary pools and standby replacement slots.
        let mut newly_installed = core::mem::take(&mut self.installed_scratch);
        newly_installed.clear();
        for p_idx in 0..self.accounts[acct_idx].pools.len() {
            let kind = self.accounts[acct_idx].pools[p_idx].spec.kind;
            let role = self.accounts[acct_idx].pools[p_idx].spec.role;
            let awaits_login = self.accounts[acct_idx]
                .account
                .session
                .awaits_login_reply(role);
            let fix_cfg = self.accounts[acct_idx]
                .account
                .session
                .fix_session(self.accounts[acct_idx].account.spec());

            let installed =
                self.accounts[acct_idx].pools[p_idx]
                    .pool
                    .poll_dials(now_mono_ns, |cid, dialed| {
                        let link_ops =
                            upgrade_dialed_link(kind, dialed, awaits_login, fix_cfg.clone())?;
                        newly_installed.push(cid);
                        Ok(link_ops)
                    });
            work += installed;

            if let Some(sb_id) = self.accounts[acct_idx].pools[p_idx].standby_conn_id
                && let Some(sb_slot) = self.accounts[acct_idx]
                    .slots
                    .iter_mut()
                    .find(|s| s.conn_id == sb_id)
                && !sb_slot.standby_established
                && let Some(baton) = sb_slot.standby_baton.as_ref()
                && baton.state() == DialState::Ready
                && let Some(dialed) = baton.take_ready()
                && let Ok(link_ops) = upgrade_dialed_link(kind, dialed, awaits_login, fix_cfg)
            {
                if let Some(ip) = link_ops.connection().local_ip() {
                    sb_slot.local_ip = Some(ip);
                }
                let prev = sb_slot.locked.install(link_ops);
                drop(prev);
                sb_slot.standby_established = true;
                sb_slot.last_epoch = sb_slot.last_epoch.wrapping_add(1).max(1);
                newly_installed.push(sb_id);
                work += 1;
            }
        }
        for &cid in &newly_installed {
            let ep = self.accounts[acct_idx]
                .pools
                .iter()
                .find_map(|p| p.pool.slot_epoch(cid))
                .unwrap_or(1);
            self.accounts[acct_idx].on_slot_installed(cid, ep);
            self.accounts[acct_idx].sync_published_routes();
        }
        newly_installed.clear();
        self.installed_scratch = newly_installed;

        // 2b. Poll raw socket/TLS bytes under `LockedConnection`, then decode & invoke `on_message`
        //     STRICTLY OUTSIDE all spinlocks (`2 lock-on-critical-path.md`) with preallocated scratch.
        let mut effects = core::mem::take(&mut self.effects_scratch);
        effects.clear();
        let mut disconnected_slots = core::mem::take(&mut self.disconnected_scratch);
        disconnected_slots.clear();
        let mut drain_requested = core::mem::take(&mut self.drain_scratch);
        drain_requested.clear();
        let mut token_expired = false;

        let slot_len = self.accounts[acct_idx].slots.len();
        for s_idx in 0..slot_len {
            let conn_id = self.accounts[acct_idx].slots[s_idx].conn_id;
            let role = self.accounts[acct_idx].slots[s_idx].role;
            let kind = self.accounts[acct_idx].slots[s_idx].kind;
            let url_id = Arc::clone(&self.accounts[acct_idx].slots[s_idx].url_id);

            let rx_res = self.accounts[acct_idx].slots[s_idx].locked.poll_raw_rx(
                now_inst,
                IoBudget::default(),
                &mut self.raw_rx_buf,
            );
            let (report, read_n) = match rx_res {
                Ok(pair) => pair,
                Err(tickwire_transport::Error::Closed) => continue,
                Err(_) => {
                    disconnected_slots.push((s_idx, None));
                    continue;
                }
            };

            if read_n > 0 {
                work += 1;
                let session = Arc::clone(&self.accounts[acct_idx].account.session);
                let locked = Arc::clone(&self.accounts[acct_idx].slots[s_idx].locked);
                let mut authed = self.accounts[acct_idx].slots[s_idx].authenticated;
                let awaits = self.accounts[acct_idx].slots[s_idx].awaits_login_reply;
                let mut became_authed = false;
                let mut decode_failed = false;

                let mut decoder = core::mem::replace(
                    &mut self.accounts[acct_idx].slots[s_idx].decoder,
                    ProtocolDecoder::Empty,
                );

                match &mut decoder {
                    ProtocolDecoder::Empty => {}
                    ProtocolDecoder::Ws(ws_dec) => {
                        if ws_dec.push(&self.raw_rx_buf[..read_n]).is_err() {
                            decode_failed = true;
                        } else {
                            loop {
                                match ws_dec.poll_message() {
                                    Ok(ws::PollMessage::Message(msg)) => match msg.opcode {
                                        ws::Opcode::Ping => {
                                            if let Ok(pong) = ws::encode_client(
                                                ws::Opcode::Pong,
                                                &msg.payload,
                                                256,
                                            ) {
                                                let _ = locked.send_control_frame(&pong);
                                            }
                                        }
                                        ws::Opcode::Pong => {}
                                        ws::Opcode::Close => {
                                            decode_failed = true;
                                            break;
                                        }
                                        ws::Opcode::Text | ws::Opcode::Binary => {
                                            let mut pong_scratch = [0u8; 512];
                                            let pong_n = session.reply_to_ping(
                                                &msg.payload,
                                                role,
                                                &mut pong_scratch,
                                            );
                                            if pong_n > 0
                                                && let Ok(frame) = ws::encode_client(
                                                    ws::Opcode::Text,
                                                    &pong_scratch[..pong_n],
                                                    pong_n + 64,
                                                )
                                            {
                                                let _ = locked.send_control_frame(&frame);
                                                continue;
                                            }
                                            if session.is_pong(&msg.payload) {
                                                continue;
                                            }
                                            if awaits && !authed {
                                                match session.login_reply(&msg.payload, role) {
                                                    LoginReply::Ok => {
                                                        let _ = locked.complete_ws_login();
                                                        authed = true;
                                                        became_authed = true;
                                                    }
                                                    LoginReply::Fail => {
                                                        decode_failed = true;
                                                        break;
                                                    }
                                                    LoginReply::Other => {
                                                        let mut ch_buf = [0u8; 2048];
                                                        let acct_arc = Arc::clone(
                                                            &self.accounts[acct_idx].account,
                                                        );
                                                        if let Ok(n) = session.login_challenge(
                                                            acct_arc.spec(),
                                                            &msg.payload,
                                                            role,
                                                            &mut ch_buf,
                                                        ) && n > 0
                                                            && let Ok(f) = ws::encode_client(
                                                                ws::Opcode::Text,
                                                                &ch_buf[..n],
                                                                n + 64,
                                                            )
                                                        {
                                                            let _ = locked.send_control_frame(&f);
                                                        }
                                                    }
                                                }
                                                continue;
                                            }
                                            let hint = session
                                                .reply_id(&msg.payload)
                                                .and_then(|rid| locked.match_ws_reply(rid));
                                            effects.clear();
                                            session.on_message(
                                                account_ref,
                                                &url_id,
                                                role,
                                                conn_id,
                                                hint,
                                                &msg.payload,
                                                now_mono_ns,
                                                &mut effects,
                                            );
                                            if effects.stream_token_expired {
                                                token_expired = true;
                                            }
                                            if effects.graceful_drain_connection {
                                                drain_requested.push(conn_id);
                                            }
                                            self.apply_session_effects(
                                                acct_idx,
                                                conn_id,
                                                hint,
                                                &effects,
                                                now_mono_ns,
                                            );
                                        }
                                    },
                                    Ok(ws::PollMessage::NeedInput | ws::PollMessage::Yielded) => {
                                        break;
                                    }
                                    Err(_) => {
                                        decode_failed = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    ProtocolDecoder::Https(http_dec) => {
                        if http_dec.push(&self.raw_rx_buf[..read_n]).is_err() {
                            decode_failed = true;
                        } else {
                            loop {
                                match http_dec.next_response() {
                                    Ok(Some(resp)) => {
                                        let hint = locked.complete_https_reply();
                                        effects.clear();
                                        session.on_message(
                                            account_ref,
                                            &url_id,
                                            role,
                                            conn_id,
                                            hint,
                                            &resp.body,
                                            now_mono_ns,
                                            &mut effects,
                                        );
                                        if effects.stream_token_expired {
                                            token_expired = true;
                                        }
                                        if effects.graceful_drain_connection {
                                            drain_requested.push(conn_id);
                                        }
                                        self.apply_session_effects(
                                            acct_idx,
                                            conn_id,
                                            hint,
                                            &effects,
                                            now_mono_ns,
                                        );
                                    }
                                    Ok(None) => break,
                                    Err(_) => {
                                        decode_failed = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    ProtocolDecoder::Fix {
                        decoder: fix_dec,
                        config: fix_cfg,
                        next_out_seq,
                    } => {
                        if fix_dec.push(&self.raw_rx_buf[..read_n]).is_err() {
                            decode_failed = true;
                        } else {
                            loop {
                                match fix_dec.next_frame() {
                                    Ok(Some(frame)) => {
                                        if let Some(cfg) = fix_cfg.as_ref()
                                            && let Some(tr) = extract_fix_test_request_id(&frame)
                                        {
                                            let seq = *next_out_seq;
                                            *next_out_seq = next_out_seq.saturating_add(1);
                                            let extra =
                                                format!("52=19700101-00:00:00.000\x01112={tr}\x01");
                                            if let Some(hb) =
                                                encode_fix_admin_frame(cfg, seq, "0", &extra)
                                            {
                                                let _ = locked.send_control_frame(&hb);
                                                continue;
                                            }
                                        }
                                        effects.clear();
                                        session.on_message(
                                            account_ref,
                                            &url_id,
                                            role,
                                            conn_id,
                                            None,
                                            &frame,
                                            now_mono_ns,
                                            &mut effects,
                                        );
                                        if effects.stream_token_expired {
                                            token_expired = true;
                                        }
                                        if effects.graceful_drain_connection {
                                            drain_requested.push(conn_id);
                                        }
                                        self.apply_session_effects(
                                            acct_idx,
                                            conn_id,
                                            None,
                                            &effects,
                                            now_mono_ns,
                                        );
                                    }
                                    Ok(None) => break,
                                    Err(_) => {
                                        decode_failed = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                self.accounts[acct_idx].slots[s_idx].decoder = decoder;
                self.accounts[acct_idx].slots[s_idx].authenticated = authed;
                if became_authed {
                    self.accounts[acct_idx].send_subscriptions(s_idx);
                    self.accounts[acct_idx].sync_published_routes();
                }

                if decode_failed {
                    self.accounts[acct_idx].slots[s_idx]
                        .locked
                        .request_close(ThreadTag::RESP);
                }
            }

            // Periodic application ping on idle WebSocket links.
            if kind == LinkKind::Ws
                && self.config.ws_ping_interval_ms > 0
                && !report.peer_closed
                && !self.accounts[acct_idx].slots[s_idx]
                    .locked
                    .is_close_pending()
            {
                let now_ms = now_mono_ns / 1_000_000;
                if now_ms.saturating_sub(self.accounts[acct_idx].slots[s_idx].last_ping_ms)
                    >= self.config.ws_ping_interval_ms
                {
                    self.accounts[acct_idx].slots[s_idx].last_ping_ms = now_ms;
                    let mut pbuf = [0u8; 512];
                    let n = self.accounts[acct_idx]
                        .account
                        .session
                        .encode_ping(role, now_ms, &mut pbuf);
                    if n > 0
                        && let Ok(ws_f) = ws::encode_client(ws::Opcode::Text, &pbuf[..n], n + 64)
                    {
                        let _ = self.accounts[acct_idx].slots[s_idx]
                            .locked
                            .send_control_frame(&ws_f);
                    }
                }
            }

            if report.peer_closed
                || self.accounts[acct_idx].slots[s_idx]
                    .locked
                    .is_close_pending()
            {
                let pending = self.accounts[acct_idx].slots[s_idx]
                    .locked
                    .complete_https_reply();
                disconnected_slots.push((s_idx, pending));
            }
        }

        for &cid in &drain_requested {
            self.accounts[acct_idx].begin_graceful_drain(cid, now_mono_ns);
        }
        drain_requested.clear();
        self.drain_scratch = drain_requested;

        self.accounts[acct_idx].poll_graceful_drains(now_mono_ns, &mut disconnected_slots);

        if !disconnected_slots.is_empty() {
            for p in &mut self.accounts[acct_idx].pools {
                let _ = p.pool.reap_close_pending(now_mono_ns);
            }
            for &(s_idx, pending) in &disconnected_slots {
                let cid = self.accounts[acct_idx].slots[s_idx].conn_id;
                let role = self.accounts[acct_idx].slots[s_idx].role;
                let url_id = Arc::clone(&self.accounts[acct_idx].slots[s_idx].url_id);
                if self.accounts[acct_idx].slots[s_idx].is_standby {
                    let retired = self.accounts[acct_idx].slots[s_idx]
                        .locked
                        .close_connection();
                    drop(retired);
                    self.accounts[acct_idx].slots[s_idx].standby_established = false;
                }
                effects.clear();
                self.accounts[acct_idx].account.session.on_disconnected(
                    account_ref,
                    &url_id,
                    role,
                    cid,
                    pending,
                    now_mono_ns,
                    &mut effects,
                );
                self.apply_session_effects(acct_idx, cid, pending, &effects, now_mono_ns);
            }
            let _ = self.order_manager.on_socket_drop(account_ref, now_mono_ns);
            self.accounts[acct_idx].sync_published_routes();
        }

        disconnected_slots.clear();
        self.disconnected_scratch = disconnected_slots;
        effects.clear();
        self.effects_scratch = effects;

        if token_expired {
            if let Some(rq) = self.accounts[acct_idx].account.rest_querier() {
                let _ = rq.enqueue(RestTask::cold_snapshot(query_kind::LISTEN_KEY_CREATE, 0));
            }
        } else if self.accounts[acct_idx].account.session.has_stream_token() {
            let refresh_s = self.accounts[acct_idx]
                .account
                .session
                .stream_token_refresh_s();
            if refresh_s > 0 {
                let refresh_ns = refresh_s.saturating_mul(1_000_000_000);
                if now_mono_ns.saturating_sub(self.accounts[acct_idx].last_token_refresh_ns)
                    >= refresh_ns
                {
                    self.accounts[acct_idx].last_token_refresh_ns = now_mono_ns;
                    if let Some(rq) = self.accounts[acct_idx].account.rest_querier() {
                        let _ = rq
                            .enqueue(RestTask::cold_snapshot(query_kind::LISTEN_KEY_KEEPALIVE, 0));
                    }
                }
            }
        }

        // 2c. Poll per-account OrsQueryDesk due actions & drain AsyncRestQuerier replies.
        work += self.drive_account_queries(acct_idx, now_mono_ns);

        work
    }

    fn apply_session_effects(
        &mut self,
        acct_idx: usize,
        conn_id: u16,
        hint: Option<RequestRef>,
        effects: &SessionMessageEffects,
        now_mono_ns: u64,
    ) {
        let account_ref = self.accounts[acct_idx].account.account_ref();

        if let Some(req_ref) = hint
            && req_ref.sent_mono_ns > 0
            && now_mono_ns >= req_ref.sent_mono_ns
        {
            let rtt_us =
                u32::try_from((now_mono_ns - req_ref.sent_mono_ns) / 1_000).unwrap_or(u32::MAX);
            for p in &mut self.accounts[acct_idx].pools {
                if let Some(ep) = p.pool.slot_epoch(conn_id) {
                    let _ = p.pool.note_sample(conn_id, ep, SampleKind::Order, rtt_us);
                }
            }
            self.accounts[acct_idx].sync_published_routes();
        }

        for ev in &effects.events {
            match ev {
                SessionVenueEvent::Ack {
                    ors_order_id,
                    venue_order_id,
                    ack_price_ticks,
                    ack_qty_lots,
                    received_mono_ns,
                } => {
                    if let Ok(OrsAckOutcome {
                        report,
                        dispatch_pending_cancel,
                    }) = self.order_manager.on_ack(
                        *ors_order_id,
                        venue_order_id.as_ref().map(InlineAscii::as_str),
                        *ack_price_ticks,
                        *ack_qty_lots,
                        *received_mono_ns,
                    ) {
                        if let Some(rep) = report {
                            self.publish_ors_report(rep, now_mono_ns);
                        }
                        if dispatch_pending_cancel {
                            self.dispatch_followup_cancel(acct_idx, *ors_order_id, now_mono_ns);
                        }
                    }
                }
                other => {
                    if let SessionVenueEvent::Fill(fill_obs) = other
                        && let Some(fee) = fill_obs.fee
                    {
                        let fee_i64 =
                            i64::try_from(fee.signed_atoms).unwrap_or(if fee.signed_atoms < 0 {
                                i64::MIN
                            } else {
                                i64::MAX
                            });
                        let _ = self
                            .rms_reload
                            .record_execution_fee(fill_obs.symbol_id as usize, fee_i64);
                    }
                    if let Ok(Some(rep)) =
                        self.order_manager.apply_venue_event(other.as_ors_event())
                    {
                        self.publish_ors_report(rep, now_mono_ns);
                    }
                }
            }
        }

        if !effects.rate_limits.is_empty() {
            let mut rl_out = [None::<ExchRateLimitEvent>; 16];
            if let Ok(emitted) = self.rate_limiter.reconcile_venue_usage_batch(
                &effects.rate_limits,
                now_mono_ns,
                &mut rl_out,
            ) {
                for ev in rl_out.iter().take(emitted).flatten() {
                    self.broadcast_exch_rate_limit(ev, now_mono_ns);
                }
            }
        }

        for qs in &effects.query_status {
            self.schedule_status_query(account_ref, *qs, now_mono_ns);
        }

        for direct in &effects.direct_reports {
            self.publish_report_body(direct.clone(), now_mono_ns);
        }
    }

    fn schedule_status_query(&self, account_ref: u8, req: QueryStatusRequest, now_mono_ns: u64) {
        let Some(acct_idx) = self
            .accounts
            .iter()
            .position(|a| a.account.account_ref() == account_ref)
        else {
            return;
        };
        let delay_ns = self.accounts[acct_idx]
            .account
            .session
            .status_query_delay_ms()
            .saturating_mul(1_000_000);
        let Some(locked) = self.order_manager.lock_order(req.ors_order_id) else {
            return;
        };
        let o = *locked.order();
        drop(locked);
        let _ = self.order_manager.query_desk().schedule_status_ask(
            account_ref,
            o.ors_order_id,
            o.slot_idx,
            o.strat_id,
            o.symbol_id,
            o.order_seqnum,
            req.origin,
            now_mono_ns.saturating_add(delay_ns),
            |_, oid| self.order_manager.lock_order(oid).is_some(),
        );
    }

    fn dispatch_followup_cancel(&mut self, acct_idx: usize, ors_order_id: u64, now_mono_ns: u64) {
        let dispatch_opt = self.order_manager.lock_order(ors_order_id).and_then(|l| {
            let o = l.order();
            o.is_open_or_pending().then_some(OrsCancelDispatch {
                ors_order_id: o.ors_order_id,
                account_ref: o.account_ref,
                symbol_id: o.symbol_id,
                client_id: o.client_id,
                venue_order_id: o.venue_order_id,
                should_send_now: true,
                coalesced_in_query_desk: false,
            })
        });
        let Some(dispatch) = dispatch_opt else {
            return;
        };
        let acct = &self.accounts[acct_idx].account;
        let Ok(account_quota) = OrsAccountQuota::register_with_overrides(
            &self.rate_limiter,
            acct.session.venue_profile(),
            u64::from(acct.account_ref()),
            acct.rate_overrides,
        ) else {
            return;
        };
        let ctx = AccountSessionContext::new(
            acct.spec(),
            &self.order_manager,
            acct.published_routes(),
            &self.rate_limiter,
            account_quota,
            acct.connections(),
            acct.last_conn_id(),
        );
        let _ =
            acct.session
                .dispatch_cancel_wire(&ctx, &dispatch, now_mono_ns, &mut self.wire_scratch);
    }

    fn drive_account_queries(&mut self, acct_idx: usize, now_mono_ns: u64) -> usize {
        let mut work = 0usize;
        let account_ref = self.accounts[acct_idx].account.account_ref();

        let mut due_buf = [DueQueryAction::StatusAsk {
            ors_order_id: 0,
            account_ref: 0,
            symbol_id: 0,
            attempt: 1,
            cancel_after: false,
        }; 16];
        let due_n = self.order_manager.query_desk().poll_due(
            account_ref,
            now_mono_ns,
            &mut due_buf,
            |_, oid| self.order_manager.lock_order(oid).is_some(),
        );

        for action in due_buf.iter().copied().take(due_n) {
            work += 1;
            match action {
                DueQueryAction::StatusAsk { ors_order_id, .. } => {
                    if let Some(rq) = self.accounts[acct_idx].account.rest_querier() {
                        let _ = rq.enqueue(RestTask::status_ask(
                            ors_order_id,
                            3,
                            now_mono_ns,
                            now_mono_ns.saturating_add(5_000_000_000),
                        ));
                    } else {
                        let _ = self
                            .order_manager
                            .query_desk()
                            .complete_order(account_ref, ors_order_id);
                    }
                }
                DueQueryAction::RetryCancel { ors_order_id, .. } => {
                    self.dispatch_followup_cancel(acct_idx, ors_order_id, now_mono_ns);
                    let _ = self
                        .order_manager
                        .query_desk()
                        .complete_order(account_ref, ors_order_id);
                }
            }
        }

        let Some(rq) = self.accounts[acct_idx].account.rest_querier().cloned() else {
            return work;
        };
        let mut replies = core::mem::take(&mut self.rest_replies_scratch);
        replies.clear();
        rq.drain_replies(|r| replies.push(r));
        for reply in replies.drain(..) {
            work += 1;
            self.handle_rest_reply(acct_idx, reply, now_mono_ns);
        }
        self.rest_replies_scratch = replies;

        work
    }

    fn handle_rest_reply(&mut self, acct_idx: usize, reply: RestReply, now_mono_ns: u64) {
        let account_ref = self.accounts[acct_idx].account.account_ref();
        match reply.task.kind {
            query_kind::LISTEN_KEY_CREATE | query_kind::LISTEN_KEY_KEEPALIVE => {
                let is_refresh = reply.task.kind == query_kind::LISTEN_KEY_KEEPALIVE;
                if reply.status == RestStatus::Ok
                    && let Ok(Some(tok_bytes)) = self.accounts[acct_idx]
                        .account
                        .session
                        .on_stream_token_reply(&reply.body, is_refresh)
                    && let Ok(tok_str) = String::from_utf8(tok_bytes)
                {
                    self.accounts[acct_idx]
                        .account
                        .stream_token
                        .set(ThreadTag::RESP, tok_str);
                    self.accounts[acct_idx].last_token_refresh_ns = now_mono_ns;
                }
            }
            query_kind::STATUS_ASK => {
                let ors_order_id = reply.task.key;
                let (symbol_id, req_kind) = match self.order_manager.lock_order(ors_order_id) {
                    Some(locked) => {
                        let o = locked.order();
                        let kind = if o.pending_cxl > 0 {
                            SessionRequestKind::Cancel
                        } else if o.modify_pending {
                            SessionRequestKind::Modify
                        } else {
                            SessionRequestKind::Place
                        };
                        (o.symbol_id, kind)
                    }
                    None => {
                        let _ = self
                            .order_manager
                            .query_desk()
                            .complete_order(account_ref, ors_order_id);
                        return;
                    }
                };
                let outcome = self.accounts[acct_idx].account.session.apply_status(
                    account_ref,
                    ors_order_id,
                    symbol_id,
                    req_kind,
                    &reply.task,
                    &reply,
                    now_mono_ns,
                );
                if !outcome.rate_limits.is_empty() {
                    let mut rl_out = [None::<ExchRateLimitEvent>; 16];
                    if let Ok(emitted) = self.rate_limiter.reconcile_venue_usage_batch(
                        &outcome.rate_limits,
                        now_mono_ns,
                        &mut rl_out,
                    ) {
                        for ev in rl_out.iter().take(emitted).flatten() {
                            self.broadcast_exch_rate_limit(ev, now_mono_ns);
                        }
                    }
                }
                if let Some(snap) = outcome.snapshot {
                    let mut reps = core::mem::take(&mut self.reconcile_reports_scratch);
                    reps.clear();
                    if let Ok(action) = self
                        .order_manager
                        .reconcile_query_snapshot(&snap.as_borrowed(), &mut reps)
                    {
                        for r in reps.drain(..) {
                            self.publish_ors_report(r, now_mono_ns);
                        }
                        if matches!(action, OrsQueryReconcileAction::DispatchCancel { .. }) {
                            self.dispatch_followup_cancel(acct_idx, ors_order_id, now_mono_ns);
                        }
                    }
                    reps.clear();
                    self.reconcile_reports_scratch = reps;
                    let _ = self
                        .order_manager
                        .query_desk()
                        .complete_order(account_ref, ors_order_id);
                } else if outcome.retry_query {
                    if let Some(rep) = self
                        .order_manager
                        .on_query_inconclusive(ors_order_id, now_mono_ns)
                    {
                        self.publish_ors_report(rep, now_mono_ns);
                        let _ = self
                            .order_manager
                            .query_desk()
                            .complete_order(account_ref, ors_order_id);
                    }
                } else {
                    let _ = self
                        .order_manager
                        .query_desk()
                        .complete_order(account_ref, ors_order_id);
                }
            }
            _ => {}
        }
    }

    fn apply_resp_admin(&mut self, cmd: RespAdminCommand, now_mono_ns: u64) {
        match cmd {
            RespAdminCommand::QueryOpen { account_ref } => {
                for acct_rt in &self.accounts {
                    if account_ref.is_none_or(|r| r == acct_rt.account.account_ref())
                        && let Some(rq) = acct_rt.account.rest_querier()
                    {
                        let queries = acct_rt
                            .account
                            .session
                            .open_orders_queries(acct_rt.account.spec());
                        for idx in 0..queries.len() {
                            let _ = rq.enqueue(RestTask::cold_snapshot(
                                query_kind::OPEN_ORDERS_SNAPSHOT,
                                idx as u64,
                            ));
                        }
                    }
                }
            }
            RespAdminCommand::Dump { account_ref: _ } => {
                for acct_rt in &mut self.accounts {
                    acct_rt.sync_published_routes();
                }
            }
            RespAdminCommand::ForceClose {
                account_ref,
                conn_id,
            } => {
                if let Some(acct_rt) = self
                    .accounts
                    .iter_mut()
                    .find(|a| a.account.account_ref() == account_ref)
                {
                    for slot in &acct_rt.slots {
                        if conn_id.is_none_or(|cid| cid == slot.conn_id) {
                            slot.locked.request_close(ThreadTag::RESP);
                        }
                    }
                    for p in &mut acct_rt.pools {
                        let _ = p.pool.reap_close_pending(now_mono_ns);
                    }
                    acct_rt.sync_published_routes();
                }
            }
            RespAdminCommand::ResendHeartbeat { account_ref } => {
                let now_ms = now_mono_ns / 1_000_000;
                for acct_rt in &mut self.accounts {
                    if account_ref.is_none_or(|r| r == acct_rt.account.account_ref()) {
                        for slot in &mut acct_rt.slots {
                            if slot.kind == LinkKind::Ws {
                                let mut pbuf = [0u8; 512];
                                let n = acct_rt
                                    .account
                                    .session
                                    .encode_ping(slot.role, now_ms, &mut pbuf);
                                if n > 0
                                    && let Ok(ws_f) =
                                        ws::encode_client(ws::Opcode::Text, &pbuf[..n], n + 64)
                                {
                                    let _ = slot.locked.send_control_frame(&ws_f);
                                }
                            } else if let ProtocolDecoder::Fix {
                                config: Some(cfg),
                                next_out_seq,
                                ..
                            } = &mut slot.decoder
                            {
                                let seq = *next_out_seq;
                                *next_out_seq = next_out_seq.saturating_add(1);
                                if let Some(hb) = encode_fix_admin_frame(
                                    cfg,
                                    seq,
                                    "0",
                                    "52=19700101-00:00:00.000\x01",
                                ) {
                                    let _ = slot.locked.send_control_frame(&hb);
                                }
                            }
                        }
                    }
                }
            }
            RespAdminCommand::BroadcastOrsRestricted => {
                self.broadcast_system_report(report_kind::ORS_RESTRICTED, now_mono_ns);
            }
            RespAdminCommand::ShutdownCancelAll => {
                self.shutdown_cancel_all_open(now_mono_ns);
            }
            RespAdminCommand::BroadcastOrsShutDown => {
                self.broadcast_system_report(report_kind::ORS_SHUT_DOWN, now_mono_ns);
                self.admin.mark_shutdown_broadcast_done();
            }
        }
    }

    fn shutdown_cancel_all_open(&mut self, now_mono_ns: u64) {
        let mut ids = [0u64; 2048];
        let found = self
            .order_manager
            .find_orders(&OrsOrderFilter::default(), &mut ids);
        for &oid in &ids[..found] {
            let acct_opt = if let Some(mut locked) = self.order_manager.lock_order(oid) {
                let o = locked.order_mut();
                if o.is_open_or_pending() {
                    o.pending_cxl = o.pending_cxl.saturating_add(1);
                    o.admin_cancel_requested = true;
                    if o.status == tickwire_account::OrsOrderStatus::PendingNew {
                        o.cancel_on_ack = true;
                        None
                    } else {
                        Some(o.account_ref)
                    }
                } else {
                    None
                }
            } else {
                None
            };
            if let Some(aref) = acct_opt
                && let Some(acct_idx) = self
                    .accounts
                    .iter()
                    .position(|a| a.account.account_ref() == aref)
            {
                self.dispatch_followup_cancel(acct_idx, oid, now_mono_ns);
            }
        }

        // Also cancel any open RFQ maker quotes on each session (`8.2`).
        for acct_idx in 0..self.accounts.len() {
            if self.accounts[acct_idx].account.session.open_quotes() > 0 {
                let acct = &self.accounts[acct_idx].account;
                if let Ok(account_quota) = OrsAccountQuota::register_with_overrides(
                    &self.rate_limiter,
                    acct.session.venue_profile(),
                    u64::from(acct.account_ref()),
                    acct.rate_overrides,
                ) {
                    let ctx = AccountSessionContext::new(
                        acct.spec(),
                        &self.order_manager,
                        acct.published_routes(),
                        &self.rate_limiter,
                        account_quota,
                        acct.connections(),
                        acct.last_conn_id(),
                    );
                    let req = RfqQuoteCancelRequest {
                        strat_id: None,
                        ors_order_id: None,
                        symbol_id: None,
                        now_mono_ns,
                    };
                    let _ = acct
                        .session
                        .cancel_quotes(&ctx, &req, &mut self.wire_scratch);
                }
            }
        }
    }

    fn publish_ors_report(&mut self, report: OrsReport, now_mono_ns: u64) {
        let body = ors_report_to_wire(&self.order_manager, report, now_mono_ns);
        self.publish_report_body(body, now_mono_ns);
    }

    fn publish_report_body(&mut self, body: ReportBody, now_mono_ns: u64) {
        if body.strat_id == ALL_STRATS {
            for lane in &mut self.report_lanes {
                let _ = lane.writer.publish_report(
                    body.clone(),
                    self.config.instrument_version,
                    None,
                    now_mono_ns,
                );
            }
            return;
        }
        if let Some(lane) = self
            .report_lanes
            .iter_mut()
            .find(|l| l.strat_id == body.strat_id)
        {
            let _ =
                lane.writer
                    .publish_report(body, self.config.instrument_version, None, now_mono_ns);
        }
    }

    fn broadcast_system_report(&mut self, kind: u16, now_mono_ns: u64) {
        let body = ReportBody {
            event_mono_ns: now_mono_ns,
            report_kind: kind,
            order_status: 0,
            reason: TerminalReason::NO_REASON.code(),
            strat_id: ALL_STRATS,
            ..Default::default()
        };
        self.publish_report_body(body, now_mono_ns);
    }

    fn broadcast_exch_rate_limit(&mut self, ev: &ExchRateLimitEvent, now_mono_ns: u64) {
        let rl_type = if ev.id.flags.contains(RateFlags::WEIGHT) {
            1u8
        } else if ev.id.flags.contains(RateFlags::ORDER) {
            2u8
        } else {
            3u8
        };
        let (unit, num) = if ev.id.interval_s.is_multiple_of(86_400) && ev.id.interval_s >= 86_400 {
            (
                4u8,
                (ev.id.interval_s / 86_400).min(u32::from(u16::MAX)) as u16,
            )
        } else if ev.id.interval_s.is_multiple_of(60) && ev.id.interval_s >= 60 {
            (2u8, (ev.id.interval_s / 60).min(u32::from(u16::MAX)) as u16)
        } else {
            (1u8, ev.id.interval_s.min(u32::from(u16::MAX)) as u16)
        };
        let body = ReportBody {
            event_mono_ns: ev.observed_at_ns.max(now_mono_ns),
            rate_limit_limit: ev.limit.min(u64::from(u32::MAX)) as u32,
            rate_limit_count: ev.count.min(u64::from(u32::MAX)) as u32,
            report_kind: report_kind::EXCH_RATE_LIMIT,
            rate_limit_interval_num: num,
            strat_id: ALL_STRATS,
            account_ref: u8::try_from(ev.id.account).unwrap_or(0),
            rate_limit_type: rl_type,
            rate_limit_interval_unit: unit,
            hard_limit_hit: u8::from(ev.crossed_ceiling),
            ..Default::default()
        };
        self.publish_report_body(body, now_mono_ns);
    }
}

fn upgrade_dialed_link(
    kind: LinkKind,
    dialed: tickwire_transport::dialer::DialedLink,
    awaits_login: bool,
    fix_cfg: Option<FixSessionConfig>,
) -> tickwire_transport::Result<LinkOps> {
    let lim = Limits::default();
    match kind {
        LinkKind::Ws => {
            if awaits_login {
                let mut ws_link = WsLink::with_login(
                    dialed.connection,
                    lim.max_receive_bytes,
                    lim.max_receive_bytes,
                    WsLoginState::AwaitingChallenge,
                )?;
                for msg in dialed.early_ws_frames {
                    ws_link.push_early_frame(msg)?;
                }
                Ok(LinkOps::Ws(ws_link))
            } else {
                Ok(LinkOps::Ws(dialed.into_ws_link(
                    lim.max_receive_bytes,
                    lim.max_receive_bytes,
                )?))
            }
        }
        LinkKind::Https => Ok(LinkOps::Https(HttpsLink::new(
            dialed.connection,
            lim.max_receive_bytes,
            128,
        )?)),
        LinkKind::Fix => {
            let cfg = fix_cfg.ok_or(tickwire_transport::Error::Protocol(
                "missing FixSessionConfig",
            ))?;
            let fix_link = FixLink::new(dialed.connection, cfg, FixSessionState::Active)?;
            Ok(LinkOps::Fix(fix_link))
        }
    }
}

#[inline]
fn decode_intent_side(raw: u8) -> Option<Side> {
    match raw {
        1 => Some(Side::Buy),
        2 => Some(Side::Sell),
        _ => None,
    }
}

#[inline]
fn encode_side_code(side: Side) -> u8 {
    match side {
        Side::Buy => 1,
        Side::Sell => 2,
    }
}

#[inline]
fn encode_liquidity_code(liq: tickwire_venue::Liquidity) -> u8 {
    match liq {
        tickwire_venue::Liquidity::Unknown => 0,
        tickwire_venue::Liquidity::Maker => 1,
        tickwire_venue::Liquidity::Taker => 2,
    }
}

#[inline]
fn decode_intent_tif(raw: u8) -> TimeInForce {
    match raw {
        tif_code::IOC => TimeInForce::Ioc,
        tif_code::FOK => TimeInForce::Fok,
        tif_code::POST_ONLY => TimeInForce::PostOnly,
        _ => TimeInForce::Gtc,
    }
}

#[inline]
fn map_rms_reject(err: RmsRejectReason) -> RefusalReason {
    match err {
        RmsRejectReason::InvalidPrice
        | RmsRejectReason::MissingReferencePrice
        | RmsRejectReason::FatFingerPrice { .. } => RefusalReason::InvalidPrice,
        RmsRejectReason::InvalidQuantity
        | RmsRejectReason::UnknownSymbol
        | RmsRejectReason::MaxOrderQty { .. }
        | RmsRejectReason::MaxOrderNotional { .. }
        | RmsRejectReason::OrderQtyWindow { .. }
        | RmsRejectReason::ArithmeticOverflow => RefusalReason::InvalidQuantity,
        RmsRejectReason::NewCountWindow { .. } => RefusalReason::RateLimited,
        RmsRejectReason::SymbolDisabled => RefusalReason::AbortedBeforeSend,
    }
}

#[inline]
fn copy_ascii_into<const N: usize>(dst: &mut [u8; N], len_out: &mut u8, s: &str) {
    dst.fill(0);
    let bytes = s.as_bytes();
    let take = bytes.len().min(N).min(usize::from(u8::MAX));
    dst[..take].copy_from_slice(&bytes[..take]);
    *len_out = take as u8;
}

fn build_nack_report(
    intent: &IntentBody,
    account_ref: u8,
    ors_order_id: u64,
    reason: RefusalReason,
    now_mono_ns: u64,
) -> ReportBody {
    let mut rep = ReportBody {
        ors_order_id,
        order_seqnum: intent.order_seqnum,
        orig_seqnum: intent.order_seqnum,
        instrument_id: intent.instrument_id,
        event_mono_ns: now_mono_ns,
        quantity_lots: intent.qty_lots,
        price_ticks: intent.price_ticks,
        report_kind: report_kind::NACK,
        order_status: 7,
        reason: reason.code(),
        strat_id: intent.strat_id,
        account_ref,
        side: intent.side.min(2),
        position_side: intent.position_side.min(3),
        order_id: intent.order_id,
        ..Default::default()
    };
    let cid_len = usize::from(intent.client_id_len).min(40);
    rep.client_id[..cid_len].copy_from_slice(&intent.client_id[..cid_len]);
    rep.client_id_len = cid_len as u8;
    rep
}

fn build_cancel_nack_report(
    intent: &IntentBody,
    account_ref: u8,
    ors_order_id: u64,
    orig_seqnum: u64,
    reason: RefusalReason,
    now_mono_ns: u64,
) -> ReportBody {
    ReportBody {
        ors_order_id,
        order_seqnum: intent.order_seqnum,
        orig_seqnum,
        instrument_id: intent.instrument_id,
        event_mono_ns: now_mono_ns,
        report_kind: report_kind::CANCEL_NACK,
        order_status: 2,
        reason: reason.code(),
        strat_id: intent.strat_id,
        account_ref,
        order_id: intent.order_id,
        ..Default::default()
    }
}

fn build_modify_nack_report(
    intent: &IntentBody,
    account_ref: u8,
    ors_order_id: u64,
    orig_seqnum: u64,
    reason: RefusalReason,
    now_mono_ns: u64,
) -> ReportBody {
    ReportBody {
        ors_order_id,
        order_seqnum: intent.order_seqnum,
        orig_seqnum,
        instrument_id: intent.instrument_id,
        event_mono_ns: now_mono_ns,
        quantity_lots: intent.qty_lots,
        price_ticks: intent.price_ticks,
        report_kind: report_kind::MODIFY_REJECTED,
        order_status: 2,
        reason: reason.code(),
        strat_id: intent.strat_id,
        account_ref,
        side: intent.side.min(2),
        position_side: intent.position_side.min(3),
        order_id: intent.order_id,
        ..Default::default()
    }
}

fn build_rfq_nack_report(
    intent: &IntentBody,
    account_ref: u8,
    ors_order_id: u64,
    reason: RefusalReason,
    now_mono_ns: u64,
) -> ReportBody {
    ReportBody {
        ors_order_id,
        order_seqnum: intent.order_seqnum,
        orig_seqnum: intent.orig_order_seqnum.max(intent.order_seqnum),
        instrument_id: intent.instrument_id,
        event_mono_ns: now_mono_ns,
        quantity_lots: intent.qty_lots,
        price_ticks: intent.price_ticks,
        report_kind: report_kind::RFQ_QUOTE_NACK,
        order_status: 7,
        reason: reason.code(),
        strat_id: intent.strat_id,
        account_ref,
        side: intent.side.min(2),
        order_id: intent.order_id,
        ..Default::default()
    }
}

fn ors_report_to_wire(
    order_manager: &OrderManager<true>,
    report: OrsReport,
    now_mono_ns: u64,
) -> ReportBody {
    match report {
        OrsReport::FrontAck(r) => {
            let mut body = ReportBody {
                ors_order_id: r.ors_order_id,
                order_seqnum: r.order_seqnum,
                orig_seqnum: r.orig_seqnum,
                event_mono_ns: r.received_mono_ns.max(now_mono_ns),
                report_kind: report_kind::FRONT_ACK,
                order_status: 1,
                reason: TerminalReason::NO_REASON.code(),
                strat_id: r.strat_id,
                account_ref: r.account_ref,
                ..Default::default()
            };
            copy_ascii_into(
                &mut body.client_id,
                &mut body.client_id_len,
                r.client_id.as_str(),
            );
            copy_ascii_into(
                &mut body.venue_order_id,
                &mut body.venue_order_id_len,
                r.venue_order_id.as_str(),
            );
            body
        }
        OrsReport::Ack(r) => {
            let mut body = ReportBody {
                ors_order_id: r.ors_order_id,
                order_seqnum: r.order_seqnum,
                orig_seqnum: r.order_seqnum,
                instrument_id: u64::from(r.symbol_id),
                event_mono_ns: r.received_mono_ns.max(now_mono_ns),
                quantity_lots: r.effective_qty_lots,
                leaves_qty_lots: r.effective_qty_lots,
                price_ticks: r.price_ticks,
                report_kind: report_kind::ACK,
                order_status: 2,
                reason: TerminalReason::NO_REASON.code(),
                strat_id: r.strat_id,
                account_ref: r.account_ref,
                ..Default::default()
            };
            if let Some(locked) = order_manager.lock_order(r.ors_order_id) {
                let o = locked.order();
                body.side = encode_side_code(o.side);
                body.cumulative_qty_lots = o.filled_qty_lots;
                body.leaves_qty_lots = o.unfilled_qty_lots();
                copy_ascii_into(
                    &mut body.client_id,
                    &mut body.client_id_len,
                    o.client_id.as_str(),
                );
            }
            copy_ascii_into(
                &mut body.venue_order_id,
                &mut body.venue_order_id_len,
                r.venue_order_id.as_str(),
            );
            body
        }
        OrsReport::Modified(r) => {
            let mut body = ReportBody {
                ors_order_id: r.ors_order_id,
                order_seqnum: r.order_seqnum,
                orig_seqnum: r.order_seqnum,
                instrument_id: u64::from(r.symbol_id),
                event_mono_ns: r.received_mono_ns.max(now_mono_ns),
                quantity_lots: r.effective_qty_lots,
                cumulative_qty_lots: r.filled_qty_lots,
                leaves_qty_lots: r.effective_qty_lots.saturating_sub(r.filled_qty_lots),
                price_ticks: r.price_ticks,
                report_kind: report_kind::MODIFIED,
                order_status: if r.filled_qty_lots > 0 { 3 } else { 2 },
                reason: TerminalReason::NO_REASON.code(),
                strat_id: r.strat_id,
                account_ref: r.account_ref,
                ..Default::default()
            };
            if let Some(locked) = order_manager.lock_order(r.ors_order_id) {
                let o = locked.order();
                body.side = encode_side_code(o.side);
                copy_ascii_into(
                    &mut body.client_id,
                    &mut body.client_id_len,
                    o.client_id.as_str(),
                );
                copy_ascii_into(
                    &mut body.venue_order_id,
                    &mut body.venue_order_id_len,
                    o.venue_order_id.as_str(),
                );
            }
            body
        }
        OrsReport::Fill(r) => {
            let (has_fee, fee_units, fee_asset_id, asset_exp) = match r.fee {
                Some(f) => (
                    1u8,
                    i64::try_from(f.signed_atoms).unwrap_or(if f.signed_atoms < 0 {
                        i64::MIN
                    } else {
                        i64::MAX
                    }),
                    f.asset_key.asset_id,
                    -(f.scale as i8),
                ),
                None => (0u8, 0i64, 0u64, 0i8),
            };
            let mut eff_qty = r.cumulative_filled_lots.max(r.cumulative_projected_lots);
            let mut leaves = 0i64;
            let mut cid = InlineAscii::<64>::empty();
            let mut vid = InlineAscii::<64>::empty();
            if let Some(locked) = order_manager.lock_order(r.ors_order_id) {
                let o = locked.order();
                eff_qty = o.effective_qty_lots;
                leaves = o.unfilled_qty_lots();
                cid = o.client_id;
                vid = o.venue_order_id;
            }
            let status = if r.order_retired || (eff_qty > 0 && leaves == 0 && !r.projected) {
                4
            } else {
                3
            };
            let mut body = ReportBody {
                ors_order_id: r.ors_order_id,
                order_seqnum: r.order_seqnum,
                orig_seqnum: r.order_seqnum,
                instrument_id: u64::from(r.symbol_id),
                event_mono_ns: r.received_mono_ns.max(now_mono_ns),
                exec_ref: r.exec_ref,
                quantity_lots: eff_qty,
                cumulative_qty_lots: r.cumulative_filled_lots,
                projected_qty_lots: r.cumulative_projected_lots,
                leaves_qty_lots: leaves,
                price_ticks: r.last_price_ticks,
                last_qty_lots: r.last_qty_lots,
                last_price_ticks: r.last_price_ticks,
                fee_units,
                fee_asset_id,
                report_kind: report_kind::FILL,
                order_status: status,
                reason: TerminalReason::NO_REASON.code(),
                strat_id: r.strat_id,
                account_ref: r.account_ref,
                side: encode_side_code(r.side),
                liquidity: encode_liquidity_code(r.liquidity),
                amendment: u8::from(r.amendment),
                projected: u8::from(r.projected),
                has_fee,
                asset_exponent: asset_exp,
                ..Default::default()
            };
            copy_ascii_into(&mut body.client_id, &mut body.client_id_len, cid.as_str());
            copy_ascii_into(
                &mut body.venue_order_id,
                &mut body.venue_order_id_len,
                vid.as_str(),
            );
            copy_ascii_into(
                &mut body.venue_trade_id,
                &mut body.venue_trade_id_len,
                r.trade_id.as_str(),
            );
            body
        }
        OrsReport::Canceled(r) => ReportBody {
            ors_order_id: r.ors_order_id,
            order_seqnum: r.order_seqnum,
            orig_seqnum: r.order_seqnum,
            instrument_id: u64::from(r.symbol_id),
            event_mono_ns: r.received_mono_ns.max(now_mono_ns),
            quantity_lots: r.canceled_qty_lots.saturating_add(r.cumulative_filled_lots),
            cumulative_qty_lots: r.cumulative_filled_lots,
            leaves_qty_lots: 0,
            report_kind: report_kind::CANCELED,
            order_status: 5,
            reason: r.reason.code(),
            strat_id: r.strat_id,
            account_ref: r.account_ref,
            ..Default::default()
        },
        OrsReport::Rejected(r) => ReportBody {
            ors_order_id: r.ors_order_id,
            order_seqnum: r.order_seqnum,
            orig_seqnum: r.order_seqnum,
            instrument_id: u64::from(r.symbol_id),
            event_mono_ns: r.received_mono_ns.max(now_mono_ns),
            report_kind: report_kind::NACK,
            order_status: 7,
            reason: r.reason.code(),
            strat_id: r.strat_id,
            account_ref: r.account_ref,
            ..Default::default()
        },
        OrsReport::ModifyRejected(r) => ReportBody {
            ors_order_id: r.ors_order_id,
            order_seqnum: r.order_seqnum,
            orig_seqnum: r.order_seqnum,
            instrument_id: u64::from(r.symbol_id),
            event_mono_ns: r.received_mono_ns.max(now_mono_ns),
            report_kind: report_kind::MODIFY_REJECTED,
            order_status: 2,
            reason: r.reason.code(),
            strat_id: r.strat_id,
            account_ref: r.account_ref,
            ..Default::default()
        },
        OrsReport::CancelRejected(r) => {
            let (orig_seq, _) = split_ors_order_id(r.ors_order_id);
            ReportBody {
                ors_order_id: r.ors_order_id,
                order_seqnum: r.order_seqnum,
                orig_seqnum: orig_seq,
                instrument_id: u64::from(r.symbol_id),
                event_mono_ns: r.received_mono_ns.max(now_mono_ns),
                report_kind: report_kind::CANCEL_NACK,
                order_status: 2,
                reason: r.reason.code(),
                strat_id: r.strat_id,
                account_ref: r.account_ref,
                ..Default::default()
            }
        }
    }
}
