//! Lock-free `AdminCommands` rings (`control -> REQ` and `control -> RESP`) and
//! `housekeeping_cpus`-pinned DOP `ControlServer` (`"tw-ors-ctrl"`).

use std::net::IpAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use mio::Waker;
use tickwire_account::OrderManager;
use tickwire_control::encode_frame;
use tickwire_control_sys::{
    Connection as SeqPacketConnection, ControlListener, CpuSet, current_tid, spawn_pinned_thread,
};
use tickwire_ipc::{
    ALL_STRATS, ControlEnvelope, ControlFrame, DOP_PROTOCOL_FEATURE_MASK, DopEchoBody,
    DopHeartBeatBody, DopLoginBody, DopLogoutBody, MktSession, OrsSessionsBody, Wire, dop_opcode,
};
use tickwire_runtime_policy::{Mode, OrsRuntimePolicy};
use tickwire_transport::{LockDomain, LockId, ThreadTag, TimedSpinLock, mono_now_ns};
use tickwire_venue::RefusalReason;

/// Default DOP idle heartbeat interval (`1,021 ms`).
pub const DEFAULT_DOP_HB_INTERVAL_MS: u32 = 1_021;

/// Default bounded capacity of each `AdminQueue`.
pub const DEFAULT_ADMIN_QUEUE_CAPACITY: usize = 256;

struct AdminSlot<T> {
    seq: AtomicU64,
    cell: TimedSpinLock<Option<T>>,
}

/// `#![forbid(unsafe_code)]` lock-free bounded MPSC/MPMC Vyukov queue: Acquire/Release sequence
/// numbers establish exclusive per-slot ownership before userspace `TimedSpinLock::lock`,
/// guaranteeing zero futex syscalls and zero heap allocation after startup construction.
pub struct AdminQueue<T> {
    capacity: usize,
    slots: Box<[AdminSlot<T>]>,
    head: AtomicU64,
    tail: AtomicU64,
    len: AtomicUsize,
}

impl<T> std::fmt::Debug for AdminQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminQueue")
            .field("capacity", &self.capacity)
            .field("len", &self.len())
            .finish()
    }
}

impl<T> AdminQueue<T> {
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(2);
        let slots = (0..cap)
            .map(|idx| AdminSlot {
                seq: AtomicU64::new(idx as u64),
                cell: TimedSpinLock::new(None, LockId::new(LockDomain::Custom, idx as u32)),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            capacity: cap,
            slots,
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            len: AtomicUsize::new(0),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push(&self, item: T) -> Result<(), T> {
        let mut pos = self.tail.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[(pos as usize) % self.capacity];
            let seq = slot.seq.load(Ordering::Acquire);
            let diff = (seq as i64).wrapping_sub(pos as i64);
            if diff == 0 {
                match self.tail.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let mut guard = slot.cell.lock(ThreadTag::CONTROL);
                        *guard = Some(item);
                        drop(guard);
                        self.len.fetch_add(1, Ordering::AcqRel);
                        slot.seq.store(pos.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(cur) => pos = cur,
                }
            } else if diff < 0 {
                return Err(item);
            } else {
                pos = self.tail.load(Ordering::Relaxed);
            }
        }
    }

    pub fn pop(&self) -> Option<T> {
        let mut pos = self.head.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[(pos as usize) % self.capacity];
            let seq = slot.seq.load(Ordering::Acquire);
            let expected = pos.wrapping_add(1);
            let diff = (seq as i64).wrapping_sub(expected as i64);
            if diff == 0 {
                match self.head.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let mut guard = slot.cell.lock(ThreadTag::REQ);
                        let item = guard.take();
                        drop(guard);
                        self.len.fetch_sub(1, Ordering::AcqRel);
                        slot.seq
                            .store(pos.wrapping_add(self.capacity as u64), Ordering::Release);
                        return item;
                    }
                    Err(cur) => pos = cur,
                }
            } else if diff < 0 {
                return None;
            } else {
                pos = self.head.load(Ordering::Relaxed);
            }
        }
    }
}

/// Administrative commands posted to the `REQ` thread (drained when outbound rings are idle).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReqAdminCommand {
    /// Cancel a single open order by `ors_order_id`.
    CancelOne { ors_order_id: u64, account_ref: u8 },
    /// Cancel open orders matching `(strat_id, account_ref, symbol_id)` (`ALL_STRATS` cancels all).
    CancelAll {
        strat_id: u8,
        account_ref: Option<u8>,
        symbol_id: Option<u32>,
    },
    /// Suspend ORS into `Mode::ManageOnly` (`RESTRICTED_CANCEL_ONLY`) and emit `OrsRestricted`.
    Suspend,
    /// Resume ORS into `Mode::Trading`.
    Resume,
    /// Enable or disable `strat_id` (`account_ref == 0` applies across all accounts; `false`
    /// auto-cancels open orders on the disabled scope).
    SetStratEnabled {
        account_ref: u8,
        strat_id: u8,
        enabled: bool,
    },
    /// Toggle `active` flag on `strat_id` (`false` refuses new orders with `INVALID_STRAT_ID`
    /// without dropping the DOP login).
    SetActive { strat_id: u8, active: bool },
    /// Explicit DOP `Logout`: resets intent `msg_seqnum` expectation and cancels open orders for `strat_id`.
    LogoutClient {
        strat_id: u8,
        cancel_open_orders: bool,
    },
    /// Resets the SHM `IntentReader` `last_msg_seqnum` cursor to `0` upon fresh `DopLogin`.
    ResetIntentSeq { strat_id: u8 },
    /// Notification that off-thread `SIGUSR1` reloaded `RmsEngine` limits to `generation`.
    RmsReload { generation: u64 },
}

/// Administrative commands posted to the `RESP` thread (woken immediately via `mio::Waker`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RespAdminCommand {
    /// Enqueue `OpenOrders` diagnostic REST queries on `account_ref` (`None` = all accounts).
    QueryOpen { account_ref: Option<u8> },
    /// Snapshot open-order and route telemetry counters on `RESP`.
    Dump { account_ref: Option<u8> },
    /// Force-close connection `conn_id` (`None` = all connections on `account_ref`) to test failover.
    ForceClose {
        account_ref: u8,
        conn_id: Option<u16>,
    },
    /// Immediately transmit application/FIX heartbeats on `account_ref` (`None` = all accounts).
    ResendHeartbeat { account_ref: Option<u8> },
    /// Broadcast `OrsRestricted` (`strat_id = ALL_STRATS`) onto strategy report rings.
    BroadcastOrsRestricted,
    /// Graceful-shutdown step (`SIGINT`/`SIGTERM` after `REQ` joins): cancel all open orders and RFQ quotes.
    ShutdownCancelAll,
    /// Graceful-shutdown final step: publish `OrsShutDown` (`strat_id = ALL_STRATS`) to all report rings.
    BroadcastOrsShutDown,
}

/// Dual lock-free `AdminCommands` producer/consumer pair (`control / main -> REQ` & `-> RESP`).
#[derive(Clone)]
pub struct AdminCommands {
    req_queue: Arc<AdminQueue<ReqAdminCommand>>,
    resp_queue: Arc<AdminQueue<RespAdminCommand>>,
    resp_waker: Arc<OnceLock<Arc<Waker>>>,
    login_epochs: Arc<[AtomicU64; 256]>,
    shutdown_broadcast_done: Arc<AtomicBool>,
}

impl std::fmt::Debug for AdminCommands {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminCommands")
            .field("req_pending", &self.req_queue.len())
            .field("resp_pending", &self.resp_queue.len())
            .finish()
    }
}

impl Default for AdminCommands {
    fn default() -> Self {
        Self::new(DEFAULT_ADMIN_QUEUE_CAPACITY)
    }
}

impl AdminCommands {
    pub fn new(capacity: usize) -> Self {
        Self {
            req_queue: Arc::new(AdminQueue::new(capacity)),
            resp_queue: Arc::new(AdminQueue::new(capacity)),
            resp_waker: Arc::new(OnceLock::new()),
            login_epochs: Arc::new(core::array::from_fn(|_| AtomicU64::new(0))),
            shutdown_broadcast_done: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn bind_resp_waker(&self, waker: Arc<Waker>) {
        let _ = self.resp_waker.set(waker);
    }

    #[inline]
    pub fn wake_resp(&self) {
        if let Some(waker) = self.resp_waker.get() {
            let _ = waker.wake();
        }
    }

    #[inline]
    pub fn bump_login_epoch(&self, strat_id: u8) -> u64 {
        self.login_epochs[usize::from(strat_id)].fetch_add(1, Ordering::AcqRel) + 1
    }

    #[inline]
    pub fn login_epoch(&self, strat_id: u8) -> u64 {
        self.login_epochs[usize::from(strat_id)].load(Ordering::Acquire)
    }

    #[inline]
    pub fn reset_shutdown_broadcast(&self) {
        self.shutdown_broadcast_done.store(false, Ordering::Release);
    }

    #[inline]
    pub fn mark_shutdown_broadcast_done(&self) {
        self.shutdown_broadcast_done.store(true, Ordering::Release);
    }

    #[inline]
    pub fn is_shutdown_broadcast_done(&self) -> bool {
        self.shutdown_broadcast_done.load(Ordering::Acquire)
    }

    #[inline]
    pub fn post_req(&self, cmd: ReqAdminCommand) -> bool {
        self.req_queue.push(cmd).is_ok()
    }

    #[inline]
    pub fn pop_req(&self) -> Option<ReqAdminCommand> {
        self.req_queue.pop()
    }

    #[inline]
    pub fn post_resp(&self, cmd: RespAdminCommand) -> bool {
        let ok = self.resp_queue.push(cmd).is_ok();
        if ok {
            self.wake_resp();
        }
        ok
    }

    #[inline]
    pub fn pop_resp(&self) -> Option<RespAdminCommand> {
        self.resp_queue.pop()
    }
}

/// Static account descriptor exposed by `ControlServer` in `OrsSessions` (`opcode = 99`).
#[derive(Clone, Debug)]
pub struct DopAccountSessionInfo {
    pub account_ref: u8,
    pub local_ip: Option<IpAddr>,
    pub product_mask: u16,
    pub colo: bool,
    pub ready_flag: Arc<AtomicBool>,
}

/// Configuration for [`ControlServer`] (`"tw-ors-ctrl"`).
#[derive(Clone, Debug)]
pub struct ControlServerConfig {
    pub ors_instance: [u8; 16],
    /// Optional per-`strat_id` expected peer `instance_id` (`None` accepts any non-zero peer instance).
    pub expected_strategy_instances: Vec<(u8, [u8; 16])>,
    pub control_epoch: u64,
    pub expected_peer_uid: Option<u32>,
    pub hb_interval_ms: u32,
    pub poll_interval: Duration,
    pub housekeeping_cpus: Option<CpuSet>,
}

impl Default for ControlServerConfig {
    fn default() -> Self {
        Self {
            ors_instance: [0xA5; 16],
            expected_strategy_instances: Vec::new(),
            control_epoch: 1,
            expected_peer_uid: None,
            hb_interval_ms: DEFAULT_DOP_HB_INTERVAL_MS,
            poll_interval: Duration::from_millis(2),
            housekeeping_cpus: None,
        }
    }
}

struct LiveStrategyPeer {
    strat_id: u8,
    peer_instance: [u8; 16],
    conn: SeqPacketConnection,
    last_tx_mono_ns: u64,
    next_nonce: u64,
}

/// `housekeeping_cpus`-pinned Unix `SOCK_SEQPACKET` DOP `ControlServer` (`"tw-ors-ctrl"`).
///
/// Handles strategy `DopLogin` (`effective_seq = max(login_seq, next_order_seqnum)` +
/// `last_recv_seqnum = 0` -> `DopEcho` + `OrsSessions`), `DopLogout` (`CancelAll` for that
/// `strat_id`), abrupt socket drops (clearing `connected` without canceling resting orders while
/// preserving sequence floors), idle `DopHeartBeat` emission, and operator `set_strat_enabled` commands.
pub struct ControlServer {
    stopped: Arc<AtomicBool>,
    worker_tid: Arc<AtomicI32>,
    admin: AdminCommands,
    policy: Arc<OrsRuntimePolicy>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for ControlServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlServer")
            .field("worker_tid", &self.worker_tid())
            .finish_non_exhaustive()
    }
}

impl ControlServer {
    pub fn bind_and_start(
        socket_path: &Path,
        config: ControlServerConfig,
        policy: Arc<OrsRuntimePolicy>,
        order_manager: Arc<OrderManager<true>>,
        accounts: Vec<DopAccountSessionInfo>,
        admin: AdminCommands,
    ) -> Result<Self, String> {
        let listener = ControlListener::bind(socket_path).map_err(|e| {
            format!(
                "failed to bind DOP ControlListener {}: {e}",
                socket_path.display()
            )
        })?;
        Self::from_listener(listener, config, policy, order_manager, accounts, admin)
    }

    pub fn from_listener(
        listener: ControlListener,
        config: ControlServerConfig,
        policy: Arc<OrsRuntimePolicy>,
        order_manager: Arc<OrderManager<true>>,
        accounts: Vec<DopAccountSessionInfo>,
        admin: AdminCommands,
    ) -> Result<Self, String> {
        if config.ors_instance == [0; 16] || config.control_epoch == 0 {
            return Err(String::from(
                "invalid ControlServer ors_instance or control_epoch",
            ));
        }
        let stopped = Arc::new(AtomicBool::new(false));
        let worker_tid = Arc::new(AtomicI32::new(0));
        let (ready_tx, ready_rx) = mpsc::sync_channel::<()>(1);

        let stop_rx = Arc::clone(&stopped);
        let tid_cell = Arc::clone(&worker_tid);
        let policy_cl = Arc::clone(&policy);
        let admin_cl = admin.clone();
        let hk_cpus = config.housekeeping_cpus.clone();

        let handle = spawn_pinned_thread("tw-ors-ctrl", hk_cpus.as_ref(), move || {
            if let Ok(tid) = current_tid() {
                tid_cell.store(tid, Ordering::Release);
            }
            let _ = ready_tx.send(());
            run_control_loop(
                listener,
                config,
                policy_cl,
                order_manager,
                accounts,
                admin_cl,
                stop_rx,
            );
        })
        .map_err(|e| format!("failed to spawn tw-ors-ctrl: {e}"))?;

        let _ = ready_rx.recv();

        Ok(Self {
            stopped,
            worker_tid,
            admin,
            policy,
            worker: Mutex::new(Some(handle)),
        })
    }

    /// Returns the Linux kernel TID (`SYS_gettid`) of `"tw-ors-ctrl"`.
    pub fn worker_tid(&self) -> Option<i32> {
        let tid = self.worker_tid.load(Ordering::Acquire);
        (tid > 0).then_some(tid)
    }

    /// Operator command toggling `(account_ref, strat_id)` (`account_ref = 0` applies globally
    /// across all accounts). Immediately updates atomic policy flags and enqueues `SetStratEnabled`
    /// onto `REQ` so disabling auto-cancels open orders on the target scope.
    pub fn set_strat_enabled(
        &self,
        account_ref: u8,
        strat_id: u8,
        enabled: bool,
    ) -> Result<bool, RefusalReason> {
        let prev = if account_ref == 0 {
            self.policy.set_global_strat_enabled(strat_id, enabled)?
        } else {
            self.policy
                .set_strat_enabled(account_ref, strat_id, enabled)?
        };
        let _ = self.admin.post_req(ReqAdminCommand::SetStratEnabled {
            account_ref,
            strat_id,
            enabled,
        });
        Ok(prev)
    }

    /// Operator command toggling `strat_id` `active` flag (`false` refuses new orders with
    /// `INVALID_STRAT_ID` while preserving any open DOP session).
    pub fn set_strat_active(&self, strat_id: u8, active: bool) -> Result<bool, RefusalReason> {
        let prev = self.policy.set_active(strat_id, active)?;
        let _ = self
            .admin
            .post_req(ReqAdminCommand::SetActive { strat_id, active });
        Ok(prev)
    }

    pub fn admin(&self) -> &AdminCommands {
        &self.admin
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        let handle = self.worker.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(h) = handle {
            let _ = h.join();
        }
    }
}

impl Drop for ControlServer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn ipv4_octets(ip: Option<IpAddr>) -> [u8; 4] {
    match ip {
        Some(IpAddr::V4(v4)) => v4.octets(),
        Some(IpAddr::V6(v6)) => {
            let b = v6.octets();
            [b[12], b[13], b[14], b[15]]
        }
        None => [0; 4],
    }
}

fn ctrl_req_id(strat_id: u8, nonce: u64) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = b'O';
    id[1] = b'R';
    id[2] = b'S';
    id[3] = strat_id;
    id[8..16].copy_from_slice(&nonce.max(1).to_le_bytes());
    id
}

fn run_control_loop(
    listener: ControlListener,
    config: ControlServerConfig,
    policy: Arc<OrsRuntimePolicy>,
    order_manager: Arc<OrderManager<true>>,
    accounts: Vec<DopAccountSessionInfo>,
    admin: AdminCommands,
    stopped: Arc<AtomicBool>,
) {
    let mut unauth_conns: Vec<SeqPacketConnection> = Vec::new();
    let mut live_peers: Vec<LiveStrategyPeer> = Vec::new();
    let hb_interval_ns = u64::from(config.hb_interval_ms).saturating_mul(1_000_000);
    let poll_sleep = if config.poll_interval.is_zero() {
        Duration::from_millis(1)
    } else {
        config.poll_interval
    };

    while !stopped.load(Ordering::Acquire) {
        let mut progressed = false;

        while let Ok(Some(conn)) = listener.try_accept() {
            progressed = true;
            let creds = conn.peer_credentials();
            if creds.pid == 0 {
                continue;
            }
            if let Some(expected_uid) = config.expected_peer_uid
                && creds.uid != expected_uid
            {
                continue;
            }
            unauth_conns.push(conn);
        }

        let now_ns = mono_now_ns();
        let mut idx = 0usize;
        while idx < unauth_conns.len() {
            match unauth_conns[idx].try_recv() {
                Ok(None) => {
                    idx += 1;
                }
                Err(_) => {
                    let _ = unauth_conns.swap_remove(idx);
                }
                Ok(Some(msg)) => {
                    progressed = true;
                    let mut conn = unauth_conns.swap_remove(idx);
                    if !msg.fds().is_empty() {
                        continue;
                    }
                    let Ok(frame) =
                        ControlFrame::decode_negotiated(msg.bytes(), DOP_PROTOCOL_FEATURE_MASK)
                    else {
                        continue;
                    };
                    if frame.envelope.opcode != dop_opcode::DOP_LOGIN
                        || frame.envelope.target_instance != config.ors_instance
                        || frame.envelope.sender_instance == [0; 16]
                        || frame.envelope.sender_instance == config.ors_instance
                        || frame.envelope.control_epoch != config.control_epoch
                        || frame.envelope.request_id == [0; 16]
                    {
                        continue;
                    }
                    let Ok(login) = DopLoginBody::decode(frame.body) else {
                        continue;
                    };
                    let peer_instance = frame.envelope.sender_instance;
                    if let Some((_, expected_inst)) = config
                        .expected_strategy_instances
                        .iter()
                        .find(|(sid, _)| *sid == login.strat_id)
                        && *expected_inst != peer_instance
                    {
                        continue;
                    }

                    let send_deadline = Instant::now() + Duration::from_millis(250);
                    let login_res =
                        handle_dop_login(&login, &policy, &order_manager, &accounts, &admin);
                    match login_res {
                        Err(refusal) => {
                            let echo = DopEchoBody {
                                hb_interval_ms: config.hb_interval_ms,
                                refusal_reason: refusal.code(),
                                strat_id: login.strat_id,
                                ..Default::default()
                            };
                            let mut raw = [0u8; DopEchoBody::SIZE];
                            if echo.encode(&mut raw).is_ok()
                                && let Ok(reply) = encode_frame(
                                    ControlEnvelope {
                                        opcode: dop_opcode::DOP_ECHO,
                                        flags: 18,
                                        status: 1,
                                        request_id: frame.envelope.request_id,
                                        control_epoch: config.control_epoch,
                                        sender_instance: config.ors_instance,
                                        target_instance: peer_instance,
                                        ..Default::default()
                                    },
                                    &raw,
                                )
                            {
                                let _ = conn.send(&reply, &[], send_deadline);
                            }
                        }
                        Ok((echo, sessions_vec)) => {
                            let mut raw_echo = [0u8; DopEchoBody::SIZE];
                            if echo.encode(&mut raw_echo).is_err() {
                                drop_live_login(&policy, &order_manager, login.strat_id);
                                continue;
                            }
                            let Ok(echo_frame) = encode_frame(
                                ControlEnvelope {
                                    opcode: dop_opcode::DOP_ECHO,
                                    flags: 10,
                                    status: 0,
                                    request_id: frame.envelope.request_id,
                                    control_epoch: config.control_epoch,
                                    sender_instance: config.ors_instance,
                                    target_instance: peer_instance,
                                    ..Default::default()
                                },
                                &raw_echo,
                            ) else {
                                drop_live_login(&policy, &order_manager, login.strat_id);
                                continue;
                            };
                            if conn.send(&echo_frame, &[], send_deadline).is_err() {
                                drop_live_login(&policy, &order_manager, login.strat_id);
                                continue;
                            }

                            let mut sessions_arr: [MktSession; 32] =
                                core::array::from_fn(|_| MktSession::default());
                            let take_n = sessions_vec.len().min(32);
                            for (dst, src) in sessions_arr
                                .iter_mut()
                                .zip(sessions_vec.iter().take(take_n))
                            {
                                *dst = src.clone();
                            }
                            let ors_body = OrsSessionsBody {
                                server_mono_ns: now_ns,
                                session_count: take_n as u16,
                                strat_id: login.strat_id,
                                _pad_11: [0; 5],
                                sessions: sessions_arr,
                            };
                            let mut raw_sessions = [0u8; OrsSessionsBody::SIZE];
                            if ors_body.encode(&mut raw_sessions).is_err() {
                                drop_live_login(&policy, &order_manager, login.strat_id);
                                continue;
                            }
                            let Ok(sessions_frame) = encode_frame(
                                ControlEnvelope {
                                    opcode: dop_opcode::ORS_SESSIONS,
                                    flags: 10,
                                    status: 0,
                                    request_id: ctrl_req_id(login.strat_id, 1),
                                    control_epoch: config.control_epoch,
                                    sender_instance: config.ors_instance,
                                    target_instance: peer_instance,
                                    ..Default::default()
                                },
                                &raw_sessions,
                            ) else {
                                drop_live_login(&policy, &order_manager, login.strat_id);
                                continue;
                            };
                            if conn.send(&sessions_frame, &[], send_deadline).is_err() {
                                drop_live_login(&policy, &order_manager, login.strat_id);
                                continue;
                            }

                            live_peers.push(LiveStrategyPeer {
                                strat_id: login.strat_id,
                                peer_instance,
                                conn,
                                last_tx_mono_ns: now_ns,
                                next_nonce: 2,
                            });
                        }
                    }
                }
            }
        }

        let now_ns = mono_now_ns();
        let mut p_idx = 0usize;
        while p_idx < live_peers.len() {
            let mut drop_peer = false;
            let mut explicit_logout = false;
            match live_peers[p_idx].conn.try_recv() {
                Ok(None) => {}
                Err(_) => {
                    drop_peer = true;
                }
                Ok(Some(msg)) => {
                    progressed = true;
                    if !msg.fds().is_empty() {
                        drop_peer = true;
                    } else if let Ok(frame) =
                        ControlFrame::decode_negotiated(msg.bytes(), DOP_PROTOCOL_FEATURE_MASK)
                    {
                        if frame.envelope.sender_instance != live_peers[p_idx].peer_instance
                            || frame.envelope.target_instance != config.ors_instance
                            || frame.envelope.control_epoch != config.control_epoch
                        {
                            drop_peer = true;
                        } else {
                            match frame.envelope.opcode {
                                dop_opcode::DOP_HEART_BEAT => {
                                    if let Ok(hb) = DopHeartBeatBody::decode(frame.body)
                                        && hb.strat_id == live_peers[p_idx].strat_id
                                    {
                                        // Valid strategy heartbeat observed.
                                    } else {
                                        drop_peer = true;
                                    }
                                }
                                dop_opcode::DOP_LOGOUT => {
                                    if let Ok(lo) = DopLogoutBody::decode(frame.body)
                                        && lo.strat_id == live_peers[p_idx].strat_id
                                    {
                                        explicit_logout = true;
                                        drop_peer = true;
                                    } else {
                                        drop_peer = true;
                                    }
                                }
                                _ => {
                                    drop_peer = true;
                                }
                            }
                        }
                    } else {
                        drop_peer = true;
                    }
                }
            }

            if !drop_peer
                && hb_interval_ns > 0
                && now_ns.saturating_sub(live_peers[p_idx].last_tx_mono_ns) >= hb_interval_ns
            {
                let sid = live_peers[p_idx].strat_id;
                let nonce = live_peers[p_idx].next_nonce;
                live_peers[p_idx].next_nonce = nonce.saturating_add(1);
                let market_ready = accounts
                    .iter()
                    .any(|a| a.ready_flag.load(Ordering::Acquire));
                let ready = market_ready
                    && policy.mode() == Mode::Trading
                    && policy.is_global_strat_enabled(sid)
                    && policy.is_active(sid);
                let next_seq = order_manager
                    .dop_state()
                    .next_order_seqnum(sid)
                    .unwrap_or(1)
                    .max(1);
                let hb = DopHeartBeatBody {
                    mono_ns: now_ns,
                    next_order_seqnum: next_seq,
                    strat_id: sid,
                    ready_to_trade: u8::from(ready),
                    _pad_18: [0; 14],
                };
                let mut raw = [0u8; DopHeartBeatBody::SIZE];
                if hb.encode(&mut raw).is_ok()
                    && let Ok(frame) = encode_frame(
                        ControlEnvelope {
                            opcode: dop_opcode::DOP_HEART_BEAT,
                            flags: 32,
                            request_id: ctrl_req_id(sid, nonce),
                            control_epoch: config.control_epoch,
                            sender_instance: config.ors_instance,
                            target_instance: live_peers[p_idx].peer_instance,
                            ..Default::default()
                        },
                        &raw,
                    )
                {
                    let deadline = Instant::now() + Duration::from_millis(100);
                    if live_peers[p_idx].conn.send(&frame, &[], deadline).is_ok() {
                        live_peers[p_idx].last_tx_mono_ns = now_ns;
                    } else {
                        drop_peer = true;
                    }
                }
            }

            if drop_peer {
                let removed = live_peers.swap_remove(p_idx);
                drop_live_login(&policy, &order_manager, removed.strat_id);
                if explicit_logout {
                    // Explicit DOP LOGOUT posts CancelAll for that strat_id (`human-docs/11`).
                    let _ = admin.post_req(ReqAdminCommand::LogoutClient {
                        strat_id: removed.strat_id,
                        cancel_open_orders: true,
                    });
                }
            } else {
                p_idx += 1;
            }
        }

        if !progressed {
            std::thread::sleep(poll_sleep);
        }
    }
}

fn drop_live_login(policy: &OrsRuntimePolicy, order_manager: &OrderManager<true>, strat_id: u8) {
    let _ = policy.logout(strat_id);
    let _ = order_manager.dop_state().logout(strat_id);
}

fn handle_dop_login(
    login: &DopLoginBody,
    policy: &OrsRuntimePolicy,
    order_manager: &OrderManager<true>,
    accounts: &[DopAccountSessionInfo],
    admin: &AdminCommands,
) -> Result<(DopEchoBody, Vec<MktSession>), RefusalReason> {
    let strat_id = login.strat_id;
    if strat_id == ALL_STRATS || !policy.is_whitelisted(strat_id) {
        return Err(RefusalReason::INVALID_STRAT_ID);
    }
    policy.try_login(strat_id)?;
    let ack = match order_manager
        .dop_state()
        .login(strat_id, login.next_source_seqnum)
    {
        Ok(ack) => ack,
        Err(err) => {
            let _ = policy.logout(strat_id);
            return Err(err);
        }
    };

    admin.bump_login_epoch(strat_id);
    let _ = admin.post_req(ReqAdminCommand::ResetIntentSeq { strat_id });

    let enabled = policy.is_global_strat_enabled(strat_id);
    let market_connected = accounts
        .iter()
        .any(|a| a.ready_flag.load(Ordering::Acquire));
    let ready_to_trade =
        market_connected && enabled && policy.is_active(strat_id) && policy.mode() == Mode::Trading;

    let echo = DopEchoBody {
        next_source_order_seqnum: ack.next_order_seqnum,
        tx_key: u64::from(strat_id) + 1,
        rx_key: 0x1000 + u64::from(strat_id) + 1,
        tx_len: 1024,
        rx_len: 1024,
        hb_interval_ms: DEFAULT_DOP_HB_INTERVAL_MS,
        refusal_reason: 0,
        strat_id,
        connected: 1,
        enabled: u8::from(enabled),
        market_connected: u8::from(market_connected),
        ready_to_trade: u8::from(ready_to_trade),
        ..Default::default()
    };

    let mut sessions = Vec::with_capacity(accounts.len());
    for acct in accounts {
        let authorized = policy.is_authorized(acct.account_ref, strat_id);
        let ready = acct.ready_flag.load(Ordering::Acquire);
        sessions.push(MktSession {
            local_ip: u32::from_be_bytes(ipv4_octets(acct.local_ip)),
            account_ref: acct.account_ref,
            strat_id: if authorized { strat_id } else { ALL_STRATS },
            authorized: u8::from(authorized),
            colo: u8::from(acct.colo),
            product_mask: acct.product_mask,
            ready: u8::from(ready),
            _pad_11: [0; 5],
        });
    }

    Ok((echo, sessions))
}
