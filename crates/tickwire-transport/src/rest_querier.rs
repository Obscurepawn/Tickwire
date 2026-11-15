//! Off-hot-path `housekeeping_cpus`-pinned asynchronous HTTP/1.1 REST reconciliation and status
//! querier (`"tw-rest-query"`) over lock-free SPSC task and reply rings.
#![forbid(unsafe_code)]

pub use crate::dialer::DialSecurity;
use crate::{
    ClockFn, Connection, Error, IoBudget, Limits, PreparedSend, Result, SendState, dialer::IpPool,
    http, mono_now_ns,
};
use mio::{Events, Poll, Token, Waker};
use std::{
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::{
        Arc, Mutex, TryLockError,
        atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tickwire_control_sys::{CpuSet, current_tid, spawn_pinned_thread};

const HARD_LIMIT: usize = 16 * 1024 * 1024;
const MAX_RING_CAPACITY: usize = 65_536;
const WAKE_TOKEN: Token = Token(0);
const CONN_TOKEN: Token = Token(1);
const DNS_REFRESH_COOLDOWN: Duration = Duration::from_millis(500);

/// Well-known ORS background REST query kinds (`Account + OpenOrders`, `listenKey`/`urlToken`
/// lifecycle, and `QueryDesk` single-order `StatusAsk`).
pub mod query_kind {
    pub const ACCOUNT_SNAPSHOT: u32 = 1;
    pub const OPEN_ORDERS_SNAPSHOT: u32 = 2;
    pub const LISTEN_KEY_CREATE: u32 = 3;
    pub const LISTEN_KEY_KEEPALIVE: u32 = 4;
    pub const STATUS_ASK: u32 = 5;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestTask {
    pub key: u64,
    pub kind: u32,
    pub max_query_count: u32,
    pub start_mono_ns: u64,
    pub end_mono_ns: u64,
}

impl RestTask {
    #[inline]
    pub const fn cold_snapshot(kind: u32, key: u64) -> Self {
        Self {
            key,
            kind,
            max_query_count: 3,
            start_mono_ns: 0,
            end_mono_ns: 0,
        }
    }

    #[inline]
    pub const fn status_ask(
        order_key: u64,
        max_query_count: u32,
        start_mono_ns: u64,
        end_mono_ns: u64,
    ) -> Self {
        Self {
            key: order_key,
            kind: query_kind::STATUS_ASK,
            max_query_count,
            start_mono_ns,
            end_mono_ns,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestStatus {
    Ok,
    HttpError,
    NotBuilt,
    SendFail,
    ReadFail,
    Timeout,
    Expired,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestReply {
    pub task: RestTask,
    pub status: RestStatus,
    pub http_code: u16,
    pub query_count: u32,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuiltRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub trait RestRequestBuilder: Send + 'static {
    fn build_request(
        &mut self,
        task: &RestTask,
        host: &str,
        scratch: &mut Vec<u8>,
    ) -> Result<Option<BuiltRequest>>;
}

impl<F> RestRequestBuilder for F
where
    F: FnMut(&RestTask, &str, &mut Vec<u8>) -> Result<Option<BuiltRequest>> + Send + 'static,
{
    fn build_request(
        &mut self,
        task: &RestTask,
        host: &str,
        scratch: &mut Vec<u8>,
    ) -> Result<Option<BuiltRequest>> {
        self(task, host, scratch)
    }
}

#[derive(Clone, Debug)]
pub struct RestQuerierConfig {
    pub host: String,
    pub port: u16,
    pub initial_peers: Vec<SocketAddr>,
    pub local_ip: Option<IpAddr>,
    /// Optional Linux network interface (`SO_BINDTODEVICE`), taking precedence over `local_ip`.
    pub nic: Option<String>,
    pub security: DialSecurity,
    pub limits: Limits,
    pub capacity: usize,
    pub connect_timeout_ms: u64,
    pub interval_ms: u64,
    pub timeout_ms: u64,
    /// Optional housekeeping CPU mask applied before `"tw-rest-query"` allocates `mio::Poll` or buffers.
    pub housekeeping_cpus: Option<CpuSet>,
    /// Optional `RESP` / owner `mio::Waker` (`eventfd`) triggered whenever a `RestReply` is posted.
    pub reply_waker: Option<Arc<Waker>>,
}

impl Default for RestQuerierConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 443,
            initial_peers: Vec::new(),
            local_ip: None,
            nic: None,
            security: DialSecurity::Plain,
            limits: Limits::default(),
            capacity: 256,
            connect_timeout_ms: 5_000,
            interval_ms: 100,
            timeout_ms: 5_000,
            housekeeping_cpus: None,
            reply_waker: None,
        }
    }
}

impl RestQuerierConfig {
    fn validate(&self) -> Result<()> {
        if [
            self.limits.max_send_bytes,
            self.limits.max_receive_bytes,
            self.limits.tls_buffer_bytes,
            self.limits.max_accept_bytes,
        ]
        .iter()
        .any(|n| *n == 0 || *n > HARD_LIMIT)
        {
            return Err(Error::Bounds);
        }
        if self.capacity == 0
            || self.capacity > MAX_RING_CAPACITY
            || self.connect_timeout_ms == 0
            || self.timeout_ms == 0
        {
            return Err(Error::Bounds);
        }
        if let Some(iface) = self.nic.as_deref()
            && (iface.is_empty()
                || iface.len() > tickwire_control_sys::MAX_INTERFACE_NAME_LEN
                || !iface.bytes().all(|b| b.is_ascii_graphic()))
        {
            return Err(Error::Bounds);
        }
        if self.host.is_empty() {
            return Err(Error::Protocol("empty host"));
        }
        if self.initial_peers.is_empty() && self.port == 0 {
            return Err(Error::Bounds);
        }
        if self.nic.is_none()
            && let Some(lip) = self.local_ip
            && !self.initial_peers.is_empty()
            && self
                .initial_peers
                .iter()
                .all(|p| p.is_ipv4() != lip.is_ipv4())
            && self.port == 0
        {
            return Err(Error::Bounds);
        }
        self.security.validate()?;
        Ok(())
    }
}

struct RingSlot<T> {
    seq: AtomicU64,
    cell: Mutex<Option<T>>,
}

/// `#![forbid(unsafe_code)]` lock-free bounded Vyukov ring: Acquire/Release sequence numbers
/// establish exclusive per-slot ownership before `try_lock()`, guaranteeing zero futex syscalls and
/// zero heap allocation after construction.
struct SpscRing<T> {
    capacity: usize,
    slots: Box<[RingSlot<T>]>,
    head: AtomicU64,
    tail: AtomicU64,
    len: AtomicUsize,
}

impl<T> SpscRing<T> {
    fn new(capacity: usize) -> Self {
        let slots = (0..capacity)
            .map(|idx| RingSlot {
                seq: AtomicU64::new(idx as u64),
                cell: Mutex::new(None),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            capacity,
            slots,
            head: AtomicU64::new(0),
            tail: AtomicU64::new(0),
            len: AtomicUsize::new(0),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn push(&self, item: T) -> bool {
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
                        let mut guard = loop {
                            match slot.cell.try_lock() {
                                Ok(g) => break g,
                                Err(TryLockError::Poisoned(p)) => break p.into_inner(),
                                Err(TryLockError::WouldBlock) => core::hint::spin_loop(),
                            }
                        };
                        *guard = Some(item);
                        drop(guard);
                        self.len.fetch_add(1, Ordering::AcqRel);
                        slot.seq.store(pos.wrapping_add(1), Ordering::Release);
                        return true;
                    }
                    Err(cur) => pos = cur,
                }
            } else if diff < 0 {
                return false;
            } else {
                pos = self.tail.load(Ordering::Relaxed);
            }
        }
    }

    fn pop(&self) -> Option<T> {
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
                        let mut guard = loop {
                            match slot.cell.try_lock() {
                                Ok(g) => break g,
                                Err(TryLockError::Poisoned(p)) => break p.into_inner(),
                                Err(TryLockError::WouldBlock) => core::hint::spin_loop(),
                            }
                        };
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

struct SharedState {
    capacity: usize,
    task_queue: SpscRing<RestTask>,
    reply_queue: SpscRing<RestReply>,
    wake_pending: AtomicBool,
    stopped: AtomicBool,
    worker_tid: AtomicI32,
    waker: Arc<Waker>,
    reply_waker: Option<Arc<Waker>>,
}

impl SharedState {
    fn notify_worker(&self) {
        if !self.wake_pending.swap(true, Ordering::SeqCst) {
            let _ = self.waker.wake();
        }
    }

    fn notify_reply_consumer(&self) {
        if let Some(w) = self.reply_waker.as_ref() {
            let _ = w.wake();
        }
    }
}

pub struct AsyncRestQuerier {
    shared: Arc<SharedState>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for AsyncRestQuerier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncRestQuerier")
            .field("capacity", &self.shared.capacity)
            .field("worker_tid", &self.worker_tid())
            .field("pending_tasks", &self.pending_tasks())
            .field("pending_replies", &self.pending_replies())
            .finish_non_exhaustive()
    }
}

impl AsyncRestQuerier {
    pub fn new<B: RestRequestBuilder>(config: RestQuerierConfig, builder: B) -> Result<Self> {
        Self::with_clock(config, builder, Arc::new(mono_now_ns))
    }

    /// Spawns `"tw-rest-query"` via `tickwire_control_sys::spawn_pinned_thread` bound to
    /// `config.housekeeping_cpus` **before** creating `mio::Poll`, Worker buffers, DNS, or sockets.
    pub fn with_clock<B: RestRequestBuilder>(
        config: RestQuerierConfig,
        builder: B,
        clock: ClockFn,
    ) -> Result<Self> {
        config.validate()?;
        let hk_cpus = config.housekeeping_cpus.clone();
        let capacity = config.capacity;
        let reply_waker = config.reply_waker.clone();

        let (startup_tx, startup_rx) = mpsc::sync_channel::<Result<Arc<SharedState>>>(1);

        let handle = spawn_pinned_thread("tw-rest-query", hk_cpus.as_ref(), move || {
            let tid = current_tid().unwrap_or(0);
            let poll = match Poll::new() {
                Ok(p) => p,
                Err(e) => {
                    let _ = startup_tx.send(Err(Error::Io(e)));
                    return;
                }
            };
            let waker = match Waker::new(poll.registry(), WAKE_TOKEN) {
                Ok(w) => Arc::new(w),
                Err(e) => {
                    let _ = startup_tx.send(Err(Error::Io(e)));
                    return;
                }
            };
            let shared = Arc::new(SharedState {
                capacity,
                task_queue: SpscRing::new(capacity),
                reply_queue: SpscRing::new(capacity),
                wake_pending: AtomicBool::new(false),
                stopped: AtomicBool::new(false),
                worker_tid: AtomicI32::new(tid),
                waker,
                reply_waker,
            });
            let worker_shared = Arc::clone(&shared);
            if startup_tx.send(Ok(shared)).is_err() {
                return;
            }
            run_query_worker(poll, config, builder, worker_shared, clock);
        })
        .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;

        let shared = startup_rx.recv().map_err(|_| Error::Closed)??;

        Ok(Self {
            shared,
            worker: Mutex::new(Some(handle)),
        })
    }

    /// Returns the Linux kernel TID (`SYS_gettid`) of `"tw-rest-query"`.
    pub fn worker_tid(&self) -> Option<i32> {
        let tid = self.shared.worker_tid.load(Ordering::Acquire);
        (tid > 0).then_some(tid)
    }

    pub fn capacity(&self) -> usize {
        self.shared.capacity
    }

    pub fn pending_tasks(&self) -> usize {
        self.shared.task_queue.len()
    }

    pub fn pending_replies(&self) -> usize {
        self.shared.reply_queue.len()
    }

    pub fn enqueue(&self, task: RestTask) -> bool {
        if self.shared.stopped.load(Ordering::Acquire) {
            return false;
        }
        if !self.shared.task_queue.push(task) {
            return false;
        }
        self.shared.notify_worker();
        true
    }

    pub fn dequeue(&self) -> Option<RestReply> {
        let reply = self.shared.reply_queue.pop()?;
        if !self.shared.task_queue.is_empty() {
            self.shared.notify_worker();
        }
        Some(reply)
    }

    pub fn drain_replies(&self, mut on_reply: impl FnMut(RestReply)) -> usize {
        let mut count = 0;
        while let Some(reply) = self.shared.reply_queue.pop() {
            count += 1;
            on_reply(reply);
        }
        if count > 0 && !self.shared.task_queue.is_empty() {
            self.shared.notify_worker();
        }
        count
    }

    pub fn stop(&self) {
        self.shared.stopped.store(true, Ordering::SeqCst);
        let _ = self.shared.waker.wake();
        let handle = self.worker.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(h) = handle {
            let _ = h.join();
        }
    }
}

impl Drop for AsyncRestQuerier {
    fn drop(&mut self) {
        self.stop();
    }
}

enum LastFailure {
    Transport(RestStatus),
    Http5xx {
        http_code: u16,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
    },
}

struct WaitingTask {
    task: RestTask,
    query_count: u32,
    next_send_ns: u64,
    last_failure: Option<LastFailure>,
}

struct InflightTask {
    task: RestTask,
    query_count: u32,
    counted_send: bool,
    send_flushed: bool,
    is_head: bool,
    deadline: Instant,
    decoder: http::ResponseDecoder,
}

struct ActiveConn {
    conn: Connection,
    peer: SocketAddr,
    started_at: Instant,
    connect_deadline: Instant,
    tcp_rtt_us: Option<u32>,
    ready_noted: bool,
}

struct WorkerConnState {
    active: Option<ActiveConn>,
    ip_pool: IpPool,
    last_dns: Option<Instant>,
    connect_fails: u32,
    next_connect_mono_ns: u64,
}

impl WorkerConnState {
    fn new(config: &RestQuerierConfig) -> Self {
        let mut ip_pool = IpPool::new();
        for &addr in &config.initial_peers {
            if config.nic.is_some()
                || config
                    .local_ip
                    .is_none_or(|lip| lip.is_ipv4() == addr.is_ipv4())
            {
                ip_pool.add_ip(addr);
            }
        }
        Self {
            active: None,
            ip_pool,
            last_dns: None,
            connect_fails: 0,
            next_connect_mono_ns: 0,
        }
    }

    fn close_graceful(&mut self, poll: &Poll) {
        if let Some(mut c) = self.active.take() {
            let _ = c.conn.deregister(poll.registry());
            self.ip_pool.release_ip(c.peer);
        }
        self.connect_fails = 0;
        self.next_connect_mono_ns = 0;
    }

    fn close_with_backoff(&mut self, poll: &Poll, config: &RestQuerierConfig, now_mono_ns: u64) {
        if let Some(mut c) = self.active.take() {
            let _ = c.conn.deregister(poll.registry());
            self.ip_pool.release_ip(c.peer);
            self.ip_pool.note_syn(c.peer, None);
        }
        let delay_ns =
            reconnect_backoff_ns(config.interval_ms, config.timeout_ms, self.connect_fails);
        self.connect_fails = self.connect_fails.saturating_add(1);
        self.next_connect_mono_ns = now_mono_ns.saturating_add(delay_ns);
    }

    fn maybe_resolve_dns(&mut self, config: &RestQuerierConfig) {
        if config.port == 0 {
            return;
        }
        let need_dns = self.ip_pool.is_empty()
            || (config.initial_peers.is_empty() && self.ip_pool.spare_count() == 0);
        if !need_dns {
            return;
        }
        if !self.ip_pool.is_empty()
            && self
                .last_dns
                .is_some_and(|t| t.elapsed() < DNS_REFRESH_COOLDOWN)
        {
            return;
        }
        self.last_dns = Some(Instant::now());
        if let Ok(addrs) = (config.host.as_str(), config.port).to_socket_addrs() {
            for addr in addrs {
                if config.nic.is_some()
                    || config
                        .local_ip
                        .is_none_or(|lip| lip.is_ipv4() == addr.is_ipv4())
                {
                    self.ip_pool.add_ip(addr);
                }
            }
        }
    }

    fn ensure_connecting(&mut self, poll: &Poll, config: &RestQuerierConfig, now_mono_ns: u64) {
        if self.active.is_some() || now_mono_ns < self.next_connect_mono_ns {
            return;
        }
        self.maybe_resolve_dns(config);
        let Some(peer) = self.ip_pool.take_ip() else {
            self.close_with_backoff(poll, config, now_mono_ns);
            return;
        };
        let started_at = Instant::now();
        let connect_deadline = started_at + Duration::from_millis(config.connect_timeout_ms);
        let mut conn = match Connection::connect_from_with_nic(
            peer,
            config.local_ip,
            config.nic.as_deref(),
            config.security.to_security(),
            config.limits,
            connect_deadline,
        ) {
            Ok(c) => c,
            Err(_) => {
                self.ip_pool.release_ip(peer);
                self.ip_pool.note_syn(peer, None);
                self.close_with_backoff(poll, config, now_mono_ns);
                return;
            }
        };
        if conn.register(poll.registry(), CONN_TOKEN).is_err() {
            self.ip_pool.release_ip(peer);
            self.ip_pool.note_syn(peer, None);
            self.close_with_backoff(poll, config, now_mono_ns);
            return;
        }
        self.active = Some(ActiveConn {
            conn,
            peer,
            started_at,
            connect_deadline,
            tcp_rtt_us: None,
            ready_noted: false,
        });
    }
}

fn reconnect_backoff_ns(interval_ms: u64, timeout_ms: u64, connect_fails: u32) -> u64 {
    let shift = connect_fails.min(16);
    let ms = timeout_ms.min(interval_ms.saturating_mul(1u64 << shift));
    ms.saturating_mul(1_000_000)
}

fn task_backoff_ns(interval_ms: u64, timeout_ms: u64, query_count: u32) -> u64 {
    let shift = query_count.saturating_sub(1).min(16);
    let ms = timeout_ms.min(interval_ms.saturating_mul(1u64 << shift));
    ms.saturating_mul(1_000_000)
}

fn post_reply(shared: &SharedState, reply: RestReply) {
    let ok = shared.reply_queue.push(reply);
    debug_assert!(ok, "reply-slot reservation invariant violated");
    shared.notify_reply_consumer();
}

fn reply_from_waiting_expiry(w: WaitingTask) -> RestReply {
    if w.query_count == 0 {
        RestReply {
            task: w.task,
            status: RestStatus::Expired,
            http_code: 0,
            query_count: 0,
            body: Vec::new(),
            headers: Vec::new(),
        }
    } else {
        match w.last_failure {
            Some(LastFailure::Http5xx {
                http_code,
                body,
                headers,
            }) => RestReply {
                task: w.task,
                status: RestStatus::HttpError,
                http_code,
                query_count: w.query_count,
                body,
                headers,
            },
            Some(LastFailure::Transport(status)) => RestReply {
                task: w.task,
                status,
                http_code: 0,
                query_count: w.query_count,
                body: Vec::new(),
                headers: Vec::new(),
            },
            None => RestReply {
                task: w.task,
                status: RestStatus::Expired,
                http_code: 0,
                query_count: w.query_count,
                body: Vec::new(),
                headers: Vec::new(),
            },
        }
    }
}

fn can_retry(task: &RestTask, query_count: u32, next_send_ns: u64) -> bool {
    let under_count = task.max_query_count == 0 || query_count < task.max_query_count;
    let before_end = task.end_mono_ns == 0 || next_send_ns < task.end_mono_ns;
    under_count && before_end
}

fn convert_headers(headers: Vec<(String, Vec<u8>)>) -> Vec<(String, String)> {
    headers
        .into_iter()
        .map(|(k, v)| {
            let s = String::from_utf8(v)
                .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
            (k, s)
        })
        .collect()
}

fn has_close_token(resp: &http::Response) -> bool {
    resp.headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("connection")
            && v.split(|b| *b == b',')
                .any(|tok| tok.trim_ascii().eq_ignore_ascii_case(b"close"))
    })
}

fn run_query_worker<B: RestRequestBuilder>(
    mut poll: Poll,
    config: RestQuerierConfig,
    mut builder: B,
    shared: Arc<SharedState>,
    clock: ClockFn,
) {
    let mut events = Events::with_capacity(32);
    let mut waiting: Vec<WaitingTask> = Vec::with_capacity(config.capacity);
    let mut inflight: Option<InflightTask> = None;
    let mut conn_state = WorkerConnState::new(&config);
    let mut scratch: Vec<u8> = Vec::with_capacity(4096);

    loop {
        shared.wake_pending.store(false, Ordering::SeqCst);

        if shared.stopped.load(Ordering::Acquire) {
            conn_state.close_graceful(&poll);
            break;
        }

        // Enforce reply-slot reservation invariant before pulling tasks.
        while waiting.len() + usize::from(inflight.is_some()) + shared.reply_queue.len()
            < config.capacity
        {
            let Some(task) = shared.task_queue.pop() else {
                break;
            };
            waiting.push(WaitingTask {
                task,
                query_count: 0,
                next_send_ns: task.start_mono_ns,
                last_failure: None,
            });
        }

        // Expire any waiting tasks whose end_mono_ns has passed.
        let now_mono = clock();
        let mut idx = 0;
        while idx < waiting.len() {
            let end = waiting[idx].task.end_mono_ns;
            if end != 0 && now_mono >= end {
                let w = waiting.remove(idx);
                post_reply(&shared, reply_from_waiting_expiry(w));
            } else {
                idx += 1;
            }
        }

        // Drive idle/connecting socket or initiate connection if work is waiting.
        if inflight.is_none() {
            if conn_state.active.is_some() {
                drive_non_inflight_conn(&poll, &config, &mut conn_state, clock());
            }
            if conn_state.active.is_none() && !waiting.is_empty() {
                conn_state.ensure_connecting(&poll, &config, clock());
                if conn_state.active.is_some() {
                    drive_non_inflight_conn(&poll, &config, &mut conn_state, clock());
                }
            }
        }

        // Start next eligible waiting task when connection is ready and idle.
        while inflight.is_none()
            && conn_state
                .active
                .as_ref()
                .is_some_and(|a| a.conn.is_ready())
        {
            let now_mono = clock();
            let Some((pick_idx, _)) = waiting
                .iter()
                .enumerate()
                .filter(|(_, w)| w.next_send_ns <= now_mono)
                .min_by_key(|(i, w)| (w.next_send_ns, *i))
            else {
                break;
            };

            let w = waiting.remove(pick_idx);
            if w.task.end_mono_ns != 0 && now_mono >= w.task.end_mono_ns {
                post_reply(&shared, reply_from_waiting_expiry(w));
                continue;
            }

            scratch.clear();
            let built = match builder.build_request(&w.task, &config.host, &mut scratch) {
                Ok(Some(b)) => b,
                Ok(None) | Err(_) => {
                    post_reply(
                        &shared,
                        RestReply {
                            task: w.task,
                            status: RestStatus::NotBuilt,
                            http_code: 0,
                            query_count: w.query_count,
                            body: Vec::new(),
                            headers: Vec::new(),
                        },
                    );
                    continue;
                }
            };

            let is_head = built.method.eq_ignore_ascii_case("HEAD");
            let has_host = built
                .headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("host"));
            let has_conn = built
                .headers
                .iter()
                .any(|(k, _)| k.eq_ignore_ascii_case("connection"));

            let mut hdr_refs: Vec<(&str, &str)> = Vec::with_capacity(built.headers.len() + 2);
            if !has_host {
                hdr_refs.push(("Host", config.host.as_str()));
            }
            if !has_conn {
                hdr_refs.push(("Connection", "keep-alive"));
            }
            for (k, v) in &built.headers {
                hdr_refs.push((k.as_str(), v.as_str()));
            }

            let wire = match http::encode_request(
                &built.method,
                &built.path,
                &hdr_refs,
                &built.body,
                config.limits.max_send_bytes,
            ) {
                Ok(bytes) => bytes,
                Err(_) => {
                    post_reply(
                        &shared,
                        RestReply {
                            task: w.task,
                            status: RestStatus::NotBuilt,
                            http_code: 0,
                            query_count: w.query_count,
                            body: Vec::new(),
                            headers: Vec::new(),
                        },
                    );
                    continue;
                }
            };

            let deadline = Instant::now() + Duration::from_millis(config.timeout_ms);
            let prepared = match PreparedSend::new(&wire, deadline, config.limits.max_send_bytes) {
                Ok(p) => p,
                Err(_) => {
                    post_reply(
                        &shared,
                        RestReply {
                            task: w.task,
                            status: RestStatus::NotBuilt,
                            http_code: 0,
                            query_count: w.query_count,
                            body: Vec::new(),
                            headers: Vec::new(),
                        },
                    );
                    continue;
                }
            };

            let decoder = match http::ResponseDecoder::new(config.limits.max_receive_bytes, 128) {
                Ok(d) => d,
                Err(_) => {
                    post_reply(
                        &shared,
                        RestReply {
                            task: w.task,
                            status: RestStatus::NotBuilt,
                            http_code: 0,
                            query_count: w.query_count,
                            body: Vec::new(),
                            headers: Vec::new(),
                        },
                    );
                    continue;
                }
            };

            let queue_res = conn_state.active.as_mut().unwrap().conn.queue(prepared);
            if queue_res.is_err() {
                conn_state.close_with_backoff(&poll, &config, now_mono);
                waiting.insert(0, w);
                break;
            }

            inflight = Some(InflightTask {
                task: w.task,
                query_count: w.query_count,
                counted_send: false,
                send_flushed: false,
                is_head,
                deadline,
                decoder,
            });
        }

        // Step active inflight HTTP request/response.
        if inflight.is_some() {
            step_inflight(
                &poll,
                &config,
                &mut conn_state,
                &mut inflight,
                &mut waiting,
                &shared,
                &clock,
            );
        }

        if let Some(active) = conn_state.active.as_mut()
            && active.conn.reregister(poll.registry(), CONN_TOKEN).is_err()
        {
            let now_mono = clock();
            conn_state.close_with_backoff(&poll, &config, now_mono);
            if let Some(cur) = inflight.take() {
                let status = if cur.send_flushed {
                    RestStatus::ReadFail
                } else {
                    RestStatus::SendFail
                };
                finish_or_retry_transport(
                    cur.task,
                    cur.query_count,
                    status,
                    &config,
                    now_mono,
                    &mut waiting,
                    &shared,
                );
            }
        }

        // Compute sleep duration or loop immediately if more work is ready.
        let now_mono = clock();
        let now_inst = Instant::now();
        let can_pull = waiting.len() + usize::from(inflight.is_some()) + shared.reply_queue.len()
            < config.capacity
            && !shared.task_queue.is_empty();

        if can_pull || shared.stopped.load(Ordering::Acquire) {
            continue;
        }

        let mut immediate = false;
        let mut min_wait: Option<Duration> = None;

        if let Some(cur) = &inflight {
            if now_inst >= cur.deadline {
                immediate = true;
            } else {
                let rem = cur
                    .deadline
                    .saturating_duration_since(now_inst)
                    .min(Duration::from_millis(20));
                min_wait = Some(min_wait.map_or(rem, |v| v.min(rem)));
            }
        } else if !waiting.is_empty() {
            if let Some(active) = &conn_state.active {
                if active.conn.is_ready() {
                    for w in &waiting {
                        if w.next_send_ns <= now_mono {
                            immediate = true;
                        } else {
                            let rem = Duration::from_nanos(w.next_send_ns - now_mono);
                            min_wait = Some(min_wait.map_or(rem, |v| v.min(rem)));
                        }
                    }
                } else if now_inst >= active.connect_deadline {
                    immediate = true;
                } else {
                    let rem = active
                        .connect_deadline
                        .saturating_duration_since(now_inst)
                        .min(Duration::from_millis(20));
                    min_wait = Some(min_wait.map_or(rem, |v| v.min(rem)));
                }
            } else if conn_state.next_connect_mono_ns <= now_mono {
                immediate = true;
            } else {
                let rem = Duration::from_nanos(conn_state.next_connect_mono_ns - now_mono);
                min_wait = Some(min_wait.map_or(rem, |v| v.min(rem)));
            }
        } else if let Some(active) = &conn_state.active
            && !active.conn.is_ready()
        {
            let rem = active
                .connect_deadline
                .saturating_duration_since(now_inst)
                .min(Duration::from_millis(20));
            min_wait = Some(min_wait.map_or(rem, |v| v.min(rem)));
        }

        for w in &waiting {
            if w.task.end_mono_ns != 0 {
                if w.task.end_mono_ns <= now_mono {
                    immediate = true;
                } else {
                    let rem = Duration::from_nanos(w.task.end_mono_ns - now_mono);
                    min_wait = Some(min_wait.map_or(rem, |v| v.min(rem)));
                }
            }
        }

        if immediate {
            continue;
        }
        let _ = poll.poll(&mut events, min_wait);
    }
}

fn drive_non_inflight_conn(
    poll: &Poll,
    config: &RestQuerierConfig,
    conn_state: &mut WorkerConnState,
    now_mono_ns: u64,
) {
    let now = Instant::now();
    let Some(active) = conn_state.active.as_mut() else {
        return;
    };
    if !active.conn.is_ready() && now >= active.connect_deadline {
        conn_state.close_with_backoff(poll, config, now_mono_ns);
        return;
    }
    let report = match active.conn.drive(now, IoBudget::default()) {
        Ok(r) => r,
        Err(_) => {
            conn_state.close_with_backoff(poll, config, now_mono_ns);
            return;
        }
    };
    if active.tcp_rtt_us.is_none() && active.conn.is_tcp_connected() {
        active.tcp_rtt_us = Some(
            active
                .started_at
                .elapsed()
                .as_micros()
                .min(u128::from(u32::MAX)) as u32,
        );
    }
    if !active.ready_noted && active.conn.is_ready() {
        active.ready_noted = true;
        let rtt = active.tcp_rtt_us.unwrap_or_else(|| {
            active
                .started_at
                .elapsed()
                .as_micros()
                .min(u128::from(u32::MAX)) as u32
        });
        let peer = active.peer;
        conn_state.ip_pool.note_syn(peer, Some(rtt));
        conn_state.connect_fails = 0;
        conn_state.next_connect_mono_ns = 0;
    } else if report.peer_closed {
        conn_state.close_with_backoff(poll, config, now_mono_ns);
    }
}

fn finish_or_retry_transport(
    task: RestTask,
    query_count: u32,
    status: RestStatus,
    config: &RestQuerierConfig,
    now_mono_ns: u64,
    waiting: &mut Vec<WaitingTask>,
    shared: &SharedState,
) {
    let backoff = task_backoff_ns(config.interval_ms, config.timeout_ms, query_count);
    let next_send_ns = now_mono_ns.saturating_add(backoff);
    if can_retry(&task, query_count, next_send_ns) {
        waiting.push(WaitingTask {
            task,
            query_count,
            next_send_ns,
            last_failure: Some(LastFailure::Transport(status)),
        });
        return;
    }
    let final_status = if query_count == 0
        && task.end_mono_ns != 0
        && (now_mono_ns >= task.end_mono_ns || next_send_ns >= task.end_mono_ns)
    {
        RestStatus::Expired
    } else {
        status
    };
    post_reply(
        shared,
        RestReply {
            task,
            status: final_status,
            http_code: 0,
            query_count,
            body: Vec::new(),
            headers: Vec::new(),
        },
    );
}

fn finish_or_retry_response(
    task: RestTask,
    query_count: u32,
    resp: http::Response,
    config: &RestQuerierConfig,
    now_mono_ns: u64,
    waiting: &mut Vec<WaitingTask>,
    shared: &SharedState,
) {
    let http_code = resp.status;
    let headers = convert_headers(resp.headers);
    let body = resp.body;

    if (200..300).contains(&http_code) {
        post_reply(
            shared,
            RestReply {
                task,
                status: RestStatus::Ok,
                http_code,
                query_count,
                body,
                headers,
            },
        );
        return;
    }

    if (500..600).contains(&http_code) {
        let backoff = task_backoff_ns(config.interval_ms, config.timeout_ms, query_count);
        let next_send_ns = now_mono_ns.saturating_add(backoff);
        if can_retry(&task, query_count, next_send_ns) {
            waiting.push(WaitingTask {
                task,
                query_count,
                next_send_ns,
                last_failure: Some(LastFailure::Http5xx {
                    http_code,
                    body,
                    headers,
                }),
            });
            return;
        }
    }

    post_reply(
        shared,
        RestReply {
            task,
            status: RestStatus::HttpError,
            http_code,
            query_count,
            body,
            headers,
        },
    );
}

fn sync_counted_send(cur: &mut InflightTask, conn: &Connection) {
    if !cur.counted_send
        && conn
            .send_report()
            .is_some_and(|r| r.accepted_bytes > 0 || r.state == SendState::Flushed)
    {
        cur.counted_send = true;
        cur.query_count = cur.query_count.saturating_add(1);
    }
}

fn step_inflight(
    poll: &Poll,
    config: &RestQuerierConfig,
    conn_state: &mut WorkerConnState,
    inflight: &mut Option<InflightTask>,
    waiting: &mut Vec<WaitingTask>,
    shared: &SharedState,
    clock: &ClockFn,
) {
    let now = Instant::now();
    let timed_out = inflight.as_ref().is_some_and(|cur| now >= cur.deadline);
    if timed_out {
        let mut cur = inflight.take().unwrap();
        if let Some(active) = &conn_state.active {
            sync_counted_send(&mut cur, &active.conn);
        }
        let now_mono = clock();
        conn_state.close_with_backoff(poll, config, now_mono);
        finish_or_retry_transport(
            cur.task,
            cur.query_count,
            RestStatus::Timeout,
            config,
            now_mono,
            waiting,
            shared,
        );
        return;
    }

    if conn_state.active.is_none() {
        let cur = inflight.take().unwrap();
        let now_mono = clock();
        let status = if cur.send_flushed {
            RestStatus::ReadFail
        } else {
            RestStatus::SendFail
        };
        finish_or_retry_transport(
            cur.task,
            cur.query_count,
            status,
            config,
            now_mono,
            waiting,
            shared,
        );
        return;
    }

    if !inflight.as_ref().unwrap().send_flushed {
        let mut post_flush_drive_err = false;
        let mut post_flush_peer_closed = false;

        for _ in 0..16 {
            let now = Instant::now();
            let prev_accepted = conn_state
                .active
                .as_ref()
                .unwrap()
                .conn
                .send_report()
                .map_or(0, |r| r.accepted_bytes);

            let try_res = conn_state
                .active
                .as_mut()
                .unwrap()
                .conn
                .try_send(now, || true);
            sync_counted_send(
                inflight.as_mut().unwrap(),
                &conn_state.active.as_ref().unwrap().conn,
            );

            if try_res.is_err() {
                let cur = inflight.take().unwrap();
                let now_mono = clock();
                conn_state.close_with_backoff(poll, config, now_mono);
                finish_or_retry_transport(
                    cur.task,
                    cur.query_count,
                    RestStatus::SendFail,
                    config,
                    now_mono,
                    waiting,
                    shared,
                );
                return;
            }

            let drive_res = conn_state
                .active
                .as_mut()
                .unwrap()
                .conn
                .drive(now, IoBudget::default());
            sync_counted_send(
                inflight.as_mut().unwrap(),
                &conn_state.active.as_ref().unwrap().conn,
            );

            let report = conn_state.active.as_ref().unwrap().conn.send_report();
            if report.is_some_and(|r| r.state == SendState::Flushed) {
                let _ = conn_state.active.as_mut().unwrap().conn.take_finished();
                inflight.as_mut().unwrap().send_flushed = true;
                post_flush_drive_err = drive_res.is_err();
                post_flush_peer_closed = drive_res.as_ref().is_ok_and(|r| r.peer_closed);
                break;
            }

            if drive_res.is_err()
                || matches!(
                    report.map(|r| r.state),
                    Some(SendState::NoBytes(_) | SendState::Failed(_))
                )
            {
                let cur = inflight.take().unwrap();
                let now_mono = clock();
                conn_state.close_with_backoff(poll, config, now_mono);
                finish_or_retry_transport(
                    cur.task,
                    cur.query_count,
                    RestStatus::SendFail,
                    config,
                    now_mono,
                    waiting,
                    shared,
                );
                return;
            }

            let new_accepted = report.map_or(0, |r| r.accepted_bytes);
            if new_accepted == prev_accepted {
                break;
            }
        }

        if !inflight.as_ref().unwrap().send_flushed {
            return;
        }
        if post_flush_drive_err || post_flush_peer_closed {
            drain_and_complete_after_flush(
                poll, config, conn_state, inflight, waiting, shared, clock, true,
            );
            return;
        }
    }

    let drive_res = conn_state
        .active
        .as_mut()
        .unwrap()
        .conn
        .drive(Instant::now(), IoBudget::default());
    let peer_closed = drive_res.is_err()
        || drive_res.as_ref().is_ok_and(|r| r.peer_closed)
        || !conn_state.active.as_ref().unwrap().conn.is_ready();

    drain_and_complete_after_flush(
        poll,
        config,
        conn_state,
        inflight,
        waiting,
        shared,
        clock,
        peer_closed,
    );
}

#[allow(clippy::too_many_arguments)]
fn drain_and_complete_after_flush(
    poll: &Poll,
    config: &RestQuerierConfig,
    conn_state: &mut WorkerConnState,
    inflight: &mut Option<InflightTask>,
    waiting: &mut Vec<WaitingTask>,
    shared: &SharedState,
    clock: &ClockFn,
    peer_closed: bool,
) {
    let mut buf = [0u8; 8192];
    loop {
        let n = conn_state.active.as_mut().unwrap().conn.read(&mut buf);
        if n == 0 {
            break;
        }
        if inflight.as_mut().unwrap().decoder.push(&buf[..n]).is_err() {
            let cur = inflight.take().unwrap();
            let now_mono = clock();
            conn_state.close_with_backoff(poll, config, now_mono);
            finish_or_retry_transport(
                cur.task,
                cur.query_count,
                RestStatus::ReadFail,
                config,
                now_mono,
                waiting,
                shared,
            );
            return;
        }
    }

    let is_head = inflight.as_ref().unwrap().is_head;
    let parsed = if is_head {
        inflight.as_mut().unwrap().decoder.next_head_response()
    } else {
        inflight.as_mut().unwrap().decoder.next_response()
    };

    match parsed {
        Ok(Some(resp)) => {
            let mut cur = inflight.take().unwrap();
            let trailing = cur.decoder.take_buffered();
            let now_mono = clock();
            if !trailing.is_empty() {
                conn_state.close_with_backoff(poll, config, now_mono);
            } else if peer_closed || has_close_token(&resp) {
                conn_state.close_graceful(poll);
            }
            finish_or_retry_response(
                cur.task,
                cur.query_count,
                resp,
                config,
                now_mono,
                waiting,
                shared,
            );
        }
        Ok(None) if peer_closed => {
            let mut cur = inflight.take().unwrap();
            cur.decoder.finish();
            let eof_resp = if cur.is_head {
                cur.decoder.next_head_response()
            } else {
                cur.decoder.next_response()
            };
            let now_mono = clock();
            match eof_resp {
                Ok(Some(resp)) => {
                    conn_state.close_graceful(poll);
                    finish_or_retry_response(
                        cur.task,
                        cur.query_count,
                        resp,
                        config,
                        now_mono,
                        waiting,
                        shared,
                    );
                }
                _ => {
                    conn_state.close_with_backoff(poll, config, now_mono);
                    finish_or_retry_transport(
                        cur.task,
                        cur.query_count,
                        RestStatus::ReadFail,
                        config,
                        now_mono,
                        waiting,
                        shared,
                    );
                }
            }
        }
        Ok(None) => {}
        Err(_) => {
            let cur = inflight.take().unwrap();
            let now_mono = clock();
            conn_state.close_with_backoff(poll, config, now_mono);
            finish_or_retry_transport(
                cur.task,
                cur.query_count,
                RestStatus::ReadFail,
                config,
                now_mono,
                waiting,
                shared,
            );
        }
    }
}
