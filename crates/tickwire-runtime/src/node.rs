//! Multi-threaded `OrsProcess<S: VenueSession>` coordinator, CPU-isolated hot/cold worker
//! spawning, OS signal handling (`SIGUSR1`, `SIGUSR2`, `SIGINT`, `SIGTERM`), and ordered graceful
//! shutdown (`REQ` join -> open-order/quote drain -> `OrsShutDown` broadcast -> `ControlServer` ->
//! `RESP` + `Dialer`).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use mio::Waker;
use tickwire_account::{OrderManager, OrderManagerConfig, OrsOrderFilter};
use tickwire_account_gateway::AccountManager;
use tickwire_control_sys::{
    ControlListener, CpuSet, OrsSignal, SignalWaiter, block_ors_signals_current_thread,
    current_tid, pin_current_thread, spawn_pinned_thread,
};
use tickwire_ipc::ALL_STRATS;
use tickwire_quota::shared::SharedRateLimiter;
use tickwire_risk::{RmsEngine, RmsReloadHandle};
use tickwire_runtime_policy::{Mode, OrsRuntimePolicy};
use tickwire_transport::LatencyTracer;
use tickwire_transport::dialer::Dialer;

use crate::admin::{
    AdminCommands, ControlServer, ControlServerConfig, ReqAdminCommand, RespAdminCommand,
};
use crate::files::reload_rms_from_file;
use crate::ors::{AccountSession, OrderRoutingSystem, OrsCpuConfig, OrsRuntimeConfig};
use crate::session::VenueSession;

/// Default graceful-shutdown drain timeout while waiting for open orders and RFQ quotes to reach
/// zero (`2s`).
pub const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Configuration for spawning and supervising a multi-threaded [`OrsProcess`].
#[derive(Clone, Debug)]
pub struct OrsProcessConfig {
    /// Optional Unix `SOCK_SEQPACKET` path bound by [`ControlServer`] (`"tw-ors-ctrl"`).
    pub control_socket_path: Option<PathBuf>,
    /// DOP [`ControlServer`] parameters (`housekeeping_cpus` defaults to
    /// `OrsCpuConfig::housekeeping_cpus` when unset).
    pub control_config: ControlServerConfig,
    /// Optional static RMS limits file reloaded off-thread upon `SIGUSR1`.
    pub rms_config_path: Option<PathBuf>,
    /// Maximum time to wait for canceled open orders and RFQ quotes to hit `0` during shutdown.
    pub shutdown_drain_timeout: Duration,
    /// Whether `block_ors_signals_current_thread()` is invoked before spawning threads so only
    /// `main`'s [`SignalWaiter`] receives `SIGINT`, `SIGTERM`, `SIGUSR1`, and `SIGUSR2`.
    pub block_signals_on_spawn: bool,
}

impl Default for OrsProcessConfig {
    fn default() -> Self {
        Self {
            control_socket_path: None,
            control_config: ControlServerConfig::default(),
            rms_config_path: None,
            shutdown_drain_timeout: DEFAULT_SHUTDOWN_DRAIN_TIMEOUT,
            block_signals_on_spawn: true,
        }
    }
}

/// Outcome returned by [`OrsProcess::handle_signal`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrsSignalOutcome {
    /// `SIGUSR1`: reloaded static RMS limits off-thread (retaining previous limits on `Err`) and
    /// enqueued `QueryOpen` + `Dump` on `RESP`.
    ReloadedRms { generation: Result<u64, String> },
    /// `SIGUSR2`: entered `Mode::ManageOnly`, enqueued emergency `CancelAll`, and broadcast
    /// `OrsRestricted`.
    EmergencyCancelAll,
    /// `SIGINT` or `SIGTERM`: executed ordered graceful shutdown.
    ShutdownInitiated,
}

/// Live multi-threaded single-venue Unified ORS execution process (`1 ORS process = 1 Venue`).
///
/// Isolates dedicated `REQ` (`req_cpu`) and `RESP` (`resp_cpu`) hot threads from cold
/// `housekeeping_cpus` workers (`ControlServer`, `Dialer`, per-account `AsyncRestQuerier`,
/// `LatencyTracer`, and `main` `SignalWaiter`).
pub struct OrsProcess<S: VenueSession> {
    cpu: OrsCpuConfig,
    policy: Arc<OrsRuntimePolicy>,
    order_manager: Arc<OrderManager<true>>,
    rms_reload: RmsReloadHandle,
    rms_config_path: Option<PathBuf>,
    rate_limiter: Arc<SharedRateLimiter>,
    accounts: Vec<Arc<AccountSession<S>>>,
    admin: AdminCommands,
    control_server: Option<ControlServer>,
    dialer: Arc<Dialer>,
    tracer: Arc<LatencyTracer>,
    resp_waker: Arc<Waker>,
    req_stop: Arc<AtomicBool>,
    resp_stop: Arc<AtomicBool>,
    shut_down: AtomicBool,
    req_tid: Arc<AtomicI32>,
    resp_tid: Arc<AtomicI32>,
    worker_init_err: Arc<OnceLock<String>>,
    req_join: Mutex<Option<JoinHandle<()>>>,
    resp_join: Mutex<Option<JoinHandle<()>>>,
    shutdown_drain_timeout: Duration,
}

impl<S: VenueSession> std::fmt::Debug for OrsProcess<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrsProcess")
            .field("cpu", &self.cpu)
            .field("req_tid", &self.req_tid())
            .field("resp_tid", &self.resp_tid())
            .field("control_server_tid", &self.control_server_tid())
            .field("dialer_tid", &self.dialer_tid())
            .field("latency_tracer_tid", &self.latency_tracer_tid())
            .field("shut_down", &self.is_shut_down())
            .finish_non_exhaustive()
    }
}

impl<S: VenueSession> OrsProcess<S> {
    /// Masks `SIGINT`, `SIGTERM`, `SIGUSR1`, and `SIGUSR2` on the calling thread first (when
    /// `process_config.block_signals_on_spawn` is `true`) so every cold helper spawned during
    /// [`OrderRoutingSystem::build`] (`LatencyTracer`, `Dialer`, `AsyncRestQuerier`) as well as
    /// `ControlServer`, `REQ`, and `RESP` inherits the blocked signal mask, then builds, runs
    /// `setup`, and spawns [`OrsProcess`].
    #[allow(clippy::too_many_arguments)]
    pub fn build_and_spawn<F>(
        runtime_config: OrsRuntimeConfig,
        account_manager: &AccountManager,
        allowed_strategies: &[u8],
        order_manager_config: OrderManagerConfig,
        rms_engine: RmsEngine,
        sessions: Vec<(u8, Arc<S>)>,
        process_config: OrsProcessConfig,
        setup: F,
    ) -> Result<Self, String>
    where
        F: FnOnce(&mut OrderRoutingSystem<S>) -> Result<(), String>,
    {
        runtime_config
            .cpu
            .validate()
            .map_err(|e| format!("CPU isolation validation failed: {e}"))?;
        if process_config.block_signals_on_spawn {
            block_ors_signals_current_thread()
                .map_err(|e| format!("failed to block ORS signals before build: {e}"))?;
        }
        let mut ors = OrderRoutingSystem::build(
            runtime_config,
            account_manager,
            allowed_strategies,
            order_manager_config,
            rms_engine,
            sessions,
        )?;
        setup(&mut ors)?;
        let mut cfg = process_config;
        cfg.block_signals_on_spawn = false;
        Self::spawn(ors, cfg)
    }

    /// Validates CPU isolation, optionally masks ORS signals on the calling thread, starts any
    /// configured DOP [`ControlServer`], and spawns pinned `"tw-ors-req"` (`req_cpu`) and
    /// `"tw-ors-resp"` (`resp_cpu`) hot-path threads.
    pub fn spawn(ors: OrderRoutingSystem<S>, config: OrsProcessConfig) -> Result<Self, String> {
        Self::spawn_inner(ors, config, None)
    }

    /// Spawns [`OrsProcess`] using a pre-bound [`ControlListener`] for DOP control connections.
    pub fn spawn_with_control_listener(
        ors: OrderRoutingSystem<S>,
        config: OrsProcessConfig,
        listener: ControlListener,
    ) -> Result<Self, String> {
        Self::spawn_inner(ors, config, Some(listener))
    }

    fn spawn_inner(
        mut ors: OrderRoutingSystem<S>,
        mut config: OrsProcessConfig,
        prebound_listener: Option<ControlListener>,
    ) -> Result<Self, String> {
        let cpu = ors.config().cpu.clone();
        cpu.validate()
            .map_err(|e| format!("CPU isolation validation failed: {e}"))?;

        let req_cpuset = cpu
            .req_cpu
            .map(CpuSet::single)
            .transpose()
            .map_err(|e| format!("invalid req_cpu: {e}"))?;
        let resp_cpuset = cpu
            .resp_cpu
            .map(CpuSet::single)
            .transpose()
            .map_err(|e| format!("invalid resp_cpu: {e}"))?;

        if config.block_signals_on_spawn {
            block_ors_signals_current_thread()
                .map_err(|e| format!("failed to block ORS signals before thread spawn: {e}"))?;
        }

        if config.control_config.housekeeping_cpus.is_none() {
            config.control_config.housekeeping_cpus = cpu.housekeeping_cpus.clone();
        }

        let policy = Arc::clone(ors.policy());
        let order_manager = Arc::clone(ors.order_manager());
        let rms_reload = ors.rms_reload_handle().clone();
        let rate_limiter = Arc::clone(ors.rate_limiter());
        let accounts = ors.accounts().to_vec();
        let admin = ors.admin().clone();
        let dialer = Arc::clone(ors.dialer());
        let tracer = Arc::clone(ors.latency_tracer());
        let resp_waker = Arc::clone(ors.resp_waker());
        let dop_infos = ors.dop_account_infos();

        let control_server = if let Some(listener) = prebound_listener {
            Some(ControlServer::from_listener(
                listener,
                config.control_config.clone(),
                Arc::clone(&policy),
                Arc::clone(&order_manager),
                dop_infos,
                admin.clone(),
            )?)
        } else if let Some(ref sock_path) = config.control_socket_path {
            Some(ControlServer::bind_and_start(
                sock_path,
                config.control_config.clone(),
                Arc::clone(&policy),
                Arc::clone(&order_manager),
                dop_infos,
                admin.clone(),
            )?)
        } else {
            None
        };

        let (req_seed, resp_seed) = match ors.take_worker_seeds() {
            Ok(pair) => pair,
            Err(e) => {
                if let Some(ref ctrl) = control_server {
                    ctrl.stop();
                }
                return Err(e);
            }
        };

        let req_stop = Arc::new(AtomicBool::new(false));
        let resp_stop = Arc::new(AtomicBool::new(false));
        let req_tid = Arc::new(AtomicI32::new(0));
        let resp_tid = Arc::new(AtomicI32::new(0));
        let worker_init_err = Arc::new(OnceLock::new());

        let (resp_ready_tx, resp_ready_rx) = mpsc::sync_channel::<()>(1);
        let resp_stop_cl = Arc::clone(&resp_stop);
        let resp_tid_cl = Arc::clone(&resp_tid);
        let resp_err_cl = Arc::clone(&worker_init_err);
        let resp_handle =
            match spawn_pinned_thread("tw-ors-resp", resp_cpuset.as_ref(), move || {
                if let Ok(tid) = current_tid() {
                    resp_tid_cl.store(tid, Ordering::Release);
                }
                let _ = resp_ready_tx.send(());
                match resp_seed.into_worker() {
                    Ok(mut resp_worker) => resp_worker.run_loop(&resp_stop_cl),
                    Err(e) => {
                        let _ = resp_err_cl.set(format!("tw-ors-resp lane init failed: {e}"));
                    }
                }
            }) {
                Ok(h) => h,
                Err(e) => {
                    if let Some(ref ctrl) = control_server {
                        ctrl.stop();
                    }
                    return Err(format!("failed to spawn tw-ors-resp: {e}"));
                }
            };
        let _ = resp_ready_rx.recv();

        let (req_ready_tx, req_ready_rx) = mpsc::sync_channel::<()>(1);
        let req_stop_cl = Arc::clone(&req_stop);
        let req_tid_cl = Arc::clone(&req_tid);
        let req_err_cl = Arc::clone(&worker_init_err);
        let req_handle = match spawn_pinned_thread("tw-ors-req", req_cpuset.as_ref(), move || {
            if let Ok(tid) = current_tid() {
                req_tid_cl.store(tid, Ordering::Release);
            }
            let _ = req_ready_tx.send(());
            match req_seed.into_worker() {
                Ok(mut req_worker) => req_worker.run_loop(&req_stop_cl),
                Err(e) => {
                    let _ = req_err_cl.set(format!("tw-ors-req lane init failed: {e}"));
                }
            }
        }) {
            Ok(h) => h,
            Err(e) => {
                resp_stop.store(true, Ordering::Release);
                let _ = resp_waker.wake();
                let _ = resp_handle.join();
                if let Some(ref ctrl) = control_server {
                    ctrl.stop();
                }
                return Err(format!("failed to spawn tw-ors-req: {e}"));
            }
        };
        let _ = req_ready_rx.recv();

        Ok(Self {
            cpu,
            policy,
            order_manager,
            rms_reload,
            rms_config_path: config.rms_config_path,
            rate_limiter,
            accounts,
            admin,
            control_server,
            dialer,
            tracer,
            resp_waker,
            req_stop,
            resp_stop,
            shut_down: AtomicBool::new(false),
            req_tid,
            resp_tid,
            worker_init_err,
            req_join: Mutex::new(Some(req_handle)),
            resp_join: Mutex::new(Some(resp_handle)),
            shutdown_drain_timeout: config.shutdown_drain_timeout,
        })
    }

    /// Returns any deferred `IntentLaneFactory` / `ReportLaneFactory` initialization error.
    pub fn worker_init_error(&self) -> Option<&str> {
        self.worker_init_err.get().map(String::as_str)
    }

    /// Returns the Linux kernel TID (`SYS_gettid`) of `"tw-ors-req"`.
    pub fn req_tid(&self) -> Option<i32> {
        let tid = self.req_tid.load(Ordering::Acquire);
        (tid > 0).then_some(tid)
    }

    /// Returns the Linux kernel TID (`SYS_gettid`) of `"tw-ors-resp"`.
    pub fn resp_tid(&self) -> Option<i32> {
        let tid = self.resp_tid.load(Ordering::Acquire);
        (tid > 0).then_some(tid)
    }

    /// Returns the Linux kernel TID (`SYS_gettid`) of `"tw-ors-ctrl"` when DOP `ControlServer` is
    /// enabled.
    pub fn control_server_tid(&self) -> Option<i32> {
        self.control_server
            .as_ref()
            .and_then(ControlServer::worker_tid)
    }

    /// Returns the Linux kernel TID (`SYS_gettid`) of `"tw-dialer"`.
    pub fn dialer_tid(&self) -> Option<i32> {
        self.dialer.worker_tid()
    }

    /// Returns the Linux kernel TID (`SYS_gettid`) of `"tw-latency-sink"`.
    pub fn latency_tracer_tid(&self) -> Option<i32> {
        self.tracer.sink_tid()
    }

    /// Returns `(account_ref, worker_tid)` for every account configured with an `AsyncRestQuerier`
    /// (`"tw-rest-query"`).
    pub fn rest_querier_tids(&self) -> Vec<(u8, i32)> {
        let mut out = Vec::new();
        for acct in &self.accounts {
            if let Some(rq) = acct.rest_querier()
                && let Some(tid) = rq.worker_tid()
            {
                out.push((acct.account_ref(), tid));
            }
        }
        out
    }

    #[inline]
    pub fn cpu_config(&self) -> &OrsCpuConfig {
        &self.cpu
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
    pub fn control_server(&self) -> Option<&ControlServer> {
        self.control_server.as_ref()
    }

    #[inline]
    pub fn admin(&self) -> &AdminCommands {
        &self.admin
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
    pub fn is_shut_down(&self) -> bool {
        self.shut_down.load(Ordering::Acquire)
    }

    /// Returns the total count of non-closed orders in [`OrderManager`] plus open RFQ maker quotes
    /// across all enabled [`AccountSession`]s.
    pub fn open_order_and_quote_count(&self) -> usize {
        let mut ids = [0u64; 4096];
        let open_orders = self
            .order_manager
            .find_orders(&OrsOrderFilter::default(), &mut ids);
        let open_quotes: usize = self
            .accounts
            .iter()
            .map(|a| a.session().open_quotes())
            .sum();
        open_orders.saturating_add(open_quotes)
    }

    /// Dispatches a single [`OrsSignal`] (`SIGUSR1`, `SIGUSR2`, `SIGINT`, `SIGTERM`).
    pub fn handle_signal(&self, signal: OrsSignal) -> OrsSignalOutcome {
        match signal {
            OrsSignal::SigUsr1 => {
                let generation = match self.rms_config_path.as_deref() {
                    Some(path) => match reload_rms_from_file(path, &self.rms_reload) {
                        Ok(new_gen) => {
                            let _ = self.admin.post_req(ReqAdminCommand::RmsReload {
                                generation: new_gen,
                            });
                            Ok(new_gen)
                        }
                        Err(err) => Err(err.to_string()),
                    },
                    None => Ok(self.rms_reload.generation()),
                };
                let _ = self
                    .admin
                    .post_resp(RespAdminCommand::QueryOpen { account_ref: None });
                let _ = self
                    .admin
                    .post_resp(RespAdminCommand::Dump { account_ref: None });
                OrsSignalOutcome::ReloadedRms { generation }
            }
            OrsSignal::SigUsr2 => {
                self.policy.set_mode(Mode::ManageOnly);
                let _ = self.admin.post_req(ReqAdminCommand::CancelAll {
                    strat_id: ALL_STRATS,
                    account_ref: None,
                    symbol_id: None,
                });
                let _ = self
                    .admin
                    .post_resp(RespAdminCommand::BroadcastOrsRestricted);
                OrsSignalOutcome::EmergencyCancelAll
            }
            OrsSignal::SigInt | OrsSignal::SigTerm => {
                self.shutdown();
                OrsSignalOutcome::ShutdownInitiated
            }
        }
    }

    /// Pins the calling `main` thread to `housekeeping_cpus` (when configured) and polls `waiter`
    /// until `SIGINT`/`SIGTERM` (or external [`Self::shutdown`]) terminates the process.
    pub fn run_signal_loop(
        &self,
        waiter: &SignalWaiter,
        poll_timeout: Duration,
    ) -> Result<Option<OrsSignal>, String> {
        if let Some(ref hk) = self.cpu.housekeeping_cpus {
            pin_current_thread(hk)
                .map_err(|e| format!("failed to pin main signal loop to housekeeping_cpus: {e}"))?;
        }
        while !self.is_shut_down() {
            let maybe_sig = waiter
                .wait_timeout(poll_timeout)
                .map_err(|e| format!("SignalWaiter::wait_timeout failed: {e}"))?;
            if let Some(sig) = maybe_sig {
                let outcome = self.handle_signal(sig);
                if matches!(outcome, OrsSignalOutcome::ShutdownInitiated) {
                    return Ok(Some(sig));
                }
            }
        }
        Ok(None)
    }

    /// Executes the deterministic ordered ORS shutdown sequence:
    /// 1. Transition policy to [`Mode::Stopped`] and stop & join `REQ` (`"tw-ors-req"`).
    /// 2. Enqueue [`RespAdminCommand::ShutdownCancelAll`] so `RESP` transmits cancels for all open
    ///    orders and RFQ quotes, polling up to `shutdown_drain_timeout` (`2s` default) for open
    ///    count to hit `0`.
    /// 3. Broadcast `OrsShutDown` (`ReportRecord`) onto all strategy report rings.
    /// 4. Stop DOP [`ControlServer`] (`"tw-ors-ctrl"`).
    /// 5. Stop & join `RESP` (`"tw-ors-resp"`), `Dialer`, per-account `AsyncRestQuerier`s, and
    ///    `LatencyTracer`.
    pub fn shutdown(&self) {
        if self.shut_down.swap(true, Ordering::AcqRel) {
            return;
        }

        // Step 1: Stop & join REQ so no new intents enter OrderManager.
        self.policy.set_mode(Mode::Stopped);
        self.req_stop.store(true, Ordering::Release);
        if let Some(req_h) = self
            .req_join
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = req_h.join();
        }

        // Step 2: Cancel all remaining open orders & RFQ quotes and wait up to `2s` for drain.
        self.admin.reset_shutdown_broadcast();
        let _ = self.admin.post_resp(RespAdminCommand::ShutdownCancelAll);
        let drain_deadline = Instant::now() + self.shutdown_drain_timeout;
        while self.open_order_and_quote_count() > 0 && Instant::now() < drain_deadline {
            let _ = self.resp_waker.wake();
            std::thread::sleep(Duration::from_millis(1));
        }

        // Step 3: Publish `OrsShutDown` (`ReportRecord`) to all strategy lanes.
        let _ = self.admin.post_resp(RespAdminCommand::BroadcastOrsShutDown);
        let bcast_deadline = Instant::now() + Duration::from_millis(250);
        while !self.admin.is_shutdown_broadcast_done() && Instant::now() < bcast_deadline {
            let _ = self.resp_waker.wake();
            std::thread::sleep(Duration::from_millis(1));
        }

        // Step 4: Stop DOP ControlServer.
        if let Some(ref ctrl) = self.control_server {
            ctrl.stop();
        }

        // Step 5: Stop & join RESP, Dialer, per-account AsyncRestQuerier, and LatencyTracer.
        self.resp_stop.store(true, Ordering::Release);
        let _ = self.resp_waker.wake();
        if let Some(resp_h) = self
            .resp_join
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = resp_h.join();
        }

        self.dialer.stop();
        for acct in &self.accounts {
            if let Some(rq) = acct.rest_querier() {
                rq.stop();
            }
        }
        self.tracer.stop();
    }
}

impl<S: VenueSession> Drop for OrsProcess<S> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
