//! Off-hot-path `housekeeping_cpus`-pinned connection dialer (`"tw-dialer"`), lock-free atomic
//! `DialBaton`, RTT-ranked `IpPool`, off-hot-path WS `loginChallenge` / FIX `Logon` handshakes,
//! and active `Tcping` probing.
#![forbid(unsafe_code)]

use crate::{
    ClockFn, Connection, Error, IoBudget, Limits, PreparedSend, Result, Security, SendState, fix,
    http,
    json_scan::MsgParser,
    latency_tracer::{LockDomain, LockId, ThreadTag, TimedSpinLock},
    links::{GuardedValue, WsLink, WsLoginState},
    mono_now_ns, ws,
};
use mio::{Events, Poll, Token, Waker};
use std::{
    collections::VecDeque,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI32, AtomicU8, AtomicU32, AtomicU64, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tickwire_control_sys::{CpuSet, connect_bound_tcp_blocking, current_tid, spawn_pinned_thread};

pub const DEFAULT_MAX_PEERS: usize = 32;
const HARD_LIMIT: usize = 16 * 1024 * 1024;
const WAKE_TOKEN: Token = Token(0);
const DNS_REFRESH_COOLDOWN: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerEntry {
    pub addr: SocketAddr,
    pub in_use: u32,
    pub avg_rtt_us: Option<u32>,
    pub last_rtt_us: Option<u32>,
    pub last_failed: bool,
}

#[derive(Clone, Debug)]
pub struct IpPool {
    peers: Vec<PeerEntry>,
    max_peers: usize,
}

impl Default for IpPool {
    fn default() -> Self {
        Self::new()
    }
}

impl IpPool {
    pub fn new() -> Self {
        Self::with_max_peers(DEFAULT_MAX_PEERS)
    }

    pub fn with_max_peers(max_peers: usize) -> Self {
        Self {
            peers: Vec::with_capacity(max_peers.min(DEFAULT_MAX_PEERS)),
            max_peers,
        }
    }

    pub fn max_peers(&self) -> usize {
        self.max_peers
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    pub fn peers(&self) -> &[PeerEntry] {
        &self.peers
    }

    pub fn spare_count(&self) -> usize {
        self.peers.iter().filter(|p| p.in_use == 0).count()
    }

    pub fn entry(&self, addr: SocketAddr) -> Option<&PeerEntry> {
        self.peers.iter().find(|p| p.addr == addr)
    }

    pub fn add_ip(&mut self, addr: SocketAddr) {
        if self.peers.len() >= self.max_peers || self.peers.iter().any(|p| p.addr == addr) {
            return;
        }
        self.peers.push(PeerEntry {
            addr,
            in_use: 0,
            avg_rtt_us: None,
            last_rtt_us: None,
            last_failed: false,
        });
    }

    pub fn note_syn(&mut self, addr: SocketAddr, rtt_us: Option<u32>) {
        let Some(idx) = self.peers.iter().position(|p| p.addr == addr) else {
            return;
        };
        match rtt_us {
            Some(rtt) => {
                let entry = &mut self.peers[idx];
                entry.last_failed = false;
                entry.last_rtt_us = Some(rtt);
                entry.avg_rtt_us = Some(match entry.avg_rtt_us {
                    Some(old) => ((u64::from(old) * 29 + u64::from(rtt)) / 30) as u32,
                    None => rtt,
                });
            }
            None => {
                self.peers[idx].last_failed = true;
                if self.peers[idx].in_use == 0 && self.peers.len() > 1 {
                    self.peers.remove(idx);
                }
            }
        }
    }

    pub fn take_ip(&mut self) -> Option<SocketAddr> {
        let (idx, _) = self
            .peers
            .iter()
            .enumerate()
            .min_by_key(|(idx, p)| (p.in_use, quality_key(p), *idx))?;
        let entry = &mut self.peers[idx];
        entry.in_use = entry.in_use.saturating_add(1);
        Some(entry.addr)
    }

    pub fn release_ip(&mut self, addr: SocketAddr) {
        if let Some(entry) = self.peers.iter_mut().find(|p| p.addr == addr) {
            entry.in_use = entry.in_use.saturating_sub(1);
        }
    }

    pub fn replace_with_unused(&mut self, current: SocketAddr) -> Option<SocketAddr> {
        let (next_idx, _) = self
            .peers
            .iter()
            .enumerate()
            .filter(|(_, p)| p.addr != current && p.in_use == 0)
            .min_by_key(|(idx, p)| (quality_key(p), *idx))?;
        let next_addr = self.peers[next_idx].addr;
        self.peers[next_idx].in_use = self.peers[next_idx].in_use.saturating_add(1);
        if let Some(cur) = self.peers.iter_mut().find(|p| p.addr == current) {
            cur.in_use = cur.in_use.saturating_sub(1);
        }
        Some(next_addr)
    }
}

fn quality_key(p: &PeerEntry) -> (u8, u32) {
    let rank = match (p.avg_rtt_us, p.last_rtt_us) {
        (Some(avg), Some(last)) => Some(avg.max(last)),
        (Some(avg), None) => Some(avg),
        (None, Some(last)) => Some(last),
        (None, None) => None,
    };
    if p.last_failed {
        (2, rank.unwrap_or(u32::MAX))
    } else if let Some(r) = rank {
        (0, r)
    } else {
        (1, u32::MAX)
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DialState {
    Idle = 0,
    Wanted = 1,
    Dialing = 2,
    Ready = 3,
    Stopped = 4,
}

impl DialState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Idle,
            1 => Self::Wanted,
            2 => Self::Dialing,
            3 => Self::Ready,
            _ => Self::Stopped,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DialBackoff {
    pub initial_ms: u64,
    pub max_ms: u64,
}

impl Default for DialBackoff {
    fn default() -> Self {
        Self {
            initial_ms: 100,
            max_ms: 5_000,
        }
    }
}

impl DialBackoff {
    pub const fn new(initial_ms: u64, max_ms: u64) -> Self {
        Self { initial_ms, max_ms }
    }

    pub fn delay_ns(&self, failures: u32) -> u64 {
        let shift = failures.min(16);
        let ms = self
            .max_ms
            .min(self.initial_ms.saturating_mul(1u64 << shift));
        ms.saturating_mul(1_000_000)
    }

    pub fn delay(&self, failures: u32) -> Duration {
        Duration::from_nanos(self.delay_ns(failures))
    }
}

#[derive(Debug)]
pub struct DialedLink {
    pub connection: Connection,
    pub peer: SocketAddr,
    pub connect_rtt_us: u32,
    pub early_ws_frames: VecDeque<ws::Message>,
}

impl DialedLink {
    pub fn new(connection: Connection, peer: SocketAddr, connect_rtt_us: u32) -> Self {
        Self {
            connection,
            peer,
            connect_rtt_us,
            early_ws_frames: VecDeque::new(),
        }
    }

    /// Converts a dialed WebSocket connection into a [`WsLink`], transferring any `early_ws_frames`
    /// buffered during multi-step `loginChallenge` authentication for immediate FIFO replay.
    pub fn into_ws_link(self, max_frame: usize, max_message: usize) -> Result<WsLink> {
        let mut ws_link = WsLink::with_login(
            self.connection,
            max_frame,
            max_message,
            WsLoginState::Authenticated,
        )?;
        for msg in self.early_ws_frames {
            ws_link.push_early_frame(msg)?;
        }
        Ok(ws_link)
    }
}

pub struct DialBaton {
    state: AtomicU8,
    due_mono_ns: AtomicU64,
    failures: AtomicU32,
    product: TimedSpinLock<Option<DialedLink>>,
    dialer_waker: TimedSpinLock<Option<Arc<Waker>>>,
    ready_waker: TimedSpinLock<Option<Arc<Waker>>>,
}

impl std::fmt::Debug for DialBaton {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DialBaton")
            .field("state", &self.state())
            .field("due_mono_ns", &self.due_mono_ns())
            .field("failures", &self.failures())
            .finish_non_exhaustive()
    }
}

impl Default for DialBaton {
    fn default() -> Self {
        Self::new()
    }
}

impl DialBaton {
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(DialState::Idle as u8),
            due_mono_ns: AtomicU64::new(0),
            failures: AtomicU32::new(0),
            product: TimedSpinLock::new(None, LockId::new(LockDomain::Custom, 1)),
            dialer_waker: TimedSpinLock::new(None, LockId::new(LockDomain::Custom, 2)),
            ready_waker: TimedSpinLock::new(None, LockId::new(LockDomain::Custom, 3)),
        }
    }

    /// Attaches an optional `RESP` `mio::Waker` (`eventfd`) notified whenever `deliver_ready`
    /// publishes a newly established `DialedLink`.
    pub fn set_ready_waker(&self, waker: Arc<Waker>) {
        let mut guard = self.ready_waker.lock(ThreadTag::RESP);
        *guard = Some(waker);
    }

    pub fn state(&self) -> DialState {
        DialState::from_u8(self.state.load(Ordering::Acquire))
    }

    pub fn due_mono_ns(&self) -> u64 {
        self.due_mono_ns.load(Ordering::Acquire)
    }

    pub fn failures(&self) -> u32 {
        self.failures.load(Ordering::Acquire)
    }

    pub fn want(&self, due_mono_ns: u64) -> bool {
        if self.state.load(Ordering::Acquire) != DialState::Idle as u8 {
            return false;
        }
        let guard = self.product.lock(ThreadTag::RESP);
        if self.state.load(Ordering::Acquire) != DialState::Idle as u8 {
            return false;
        }
        debug_assert!(guard.is_none());
        self.due_mono_ns.store(due_mono_ns, Ordering::Release);
        self.state.store(DialState::Wanted as u8, Ordering::Release);
        drop(guard);
        self.notify_dialer_waker();
        true
    }

    pub fn claim(&self, now_mono_ns: u64) -> bool {
        if self.state.load(Ordering::Acquire) != DialState::Wanted as u8 {
            return false;
        }
        if now_mono_ns < self.due_mono_ns.load(Ordering::Acquire) {
            return false;
        }
        let guard = self.product.lock(ThreadTag::DIALER);
        if self.state.load(Ordering::Acquire) != DialState::Wanted as u8 {
            return false;
        }
        if now_mono_ns < self.due_mono_ns.load(Ordering::Acquire) {
            return false;
        }
        self.state
            .store(DialState::Dialing as u8, Ordering::Release);
        drop(guard);
        true
    }

    pub fn deliver_ready(&self, conn: DialedLink) -> bool {
        let mut guard = self.product.lock(ThreadTag::DIALER);
        if self.state.load(Ordering::Acquire) != DialState::Dialing as u8 {
            return false;
        }
        *guard = Some(conn);
        self.state.store(DialState::Ready as u8, Ordering::Release);
        drop(guard);
        self.notify_ready_waker();
        true
    }

    pub fn fail(&self, next_due_mono_ns: u64) -> bool {
        let guard = self.product.lock(ThreadTag::DIALER);
        if self.state.load(Ordering::Acquire) != DialState::Dialing as u8 {
            return false;
        }
        let next_failures = self.failures.load(Ordering::Acquire).saturating_add(1);
        self.failures.store(next_failures, Ordering::Release);
        self.due_mono_ns.store(next_due_mono_ns, Ordering::Release);
        self.state.store(DialState::Wanted as u8, Ordering::Release);
        drop(guard);
        true
    }

    pub fn take_ready(&self) -> Option<DialedLink> {
        if self.state.load(Ordering::Acquire) != DialState::Ready as u8 {
            return None;
        }
        let mut guard = self.product.lock(ThreadTag::RESP);
        if self.state.load(Ordering::Acquire) != DialState::Ready as u8 {
            return None;
        }
        let link = guard.take();
        self.failures.store(0, Ordering::Release);
        self.due_mono_ns.store(0, Ordering::Release);
        self.state.store(DialState::Idle as u8, Ordering::Release);
        link
    }

    pub fn reset(&self) {
        let mut guard = self.product.lock(ThreadTag::RESP);
        if self.state.load(Ordering::Acquire) == DialState::Stopped as u8 {
            return;
        }
        let dropped = guard.take();
        self.failures.store(0, Ordering::Release);
        self.due_mono_ns.store(0, Ordering::Release);
        self.state.store(DialState::Idle as u8, Ordering::Release);
        drop(guard);
        drop(dropped);
        self.notify_dialer_waker();
    }

    pub fn stop(&self) {
        let mut guard = self.product.lock(ThreadTag::RESP);
        let dropped = guard.take();
        self.state
            .store(DialState::Stopped as u8, Ordering::Release);
        drop(guard);
        drop(dropped);
        self.notify_dialer_waker();
    }

    fn set_waker(&self, waker: Arc<Waker>) {
        let mut guard = self.dialer_waker.lock(ThreadTag::RESP);
        *guard = Some(waker);
    }

    fn notify_dialer_waker(&self) {
        let waker = self.dialer_waker.lock(ThreadTag::RESP).clone();
        if let Some(w) = waker {
            let _ = w.wake();
        }
    }

    fn notify_ready_waker(&self) {
        let waker = self.ready_waker.lock(ThreadTag::DIALER).clone();
        if let Some(w) = waker {
            let _ = w.wake();
        }
    }
}

#[derive(Clone, Debug)]
pub enum DialSecurity {
    Plain,
    Tls {
        server_name: rustls::pki_types::ServerName<'static>,
        config: Arc<rustls::ClientConfig>,
    },
}

impl DialSecurity {
    pub fn to_security(&self) -> Security {
        match self {
            Self::Plain => Security::Plain,
            Self::Tls {
                server_name,
                config,
            } => Security::Tls {
                server_name: server_name.clone(),
                config: Arc::clone(config),
            },
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if let Self::Tls { config, .. } = self
            && config.enable_early_data
        {
            return Err(Error::Protocol("early data disabled"));
        }
        Ok(())
    }
}

pub type ChallengeResponseBuilder = Arc<dyn Fn(&str) -> Result<String> + Send + Sync>;

/// Off-hot-path WebSocket post-upgrade authentication workflow (`loginChallenge`).
#[derive(Clone)]
pub enum WsDialAuth {
    None,
    /// Sends `login_payload` immediately after HTTP `101` and waits for an auth-ack frame matching
    /// `expect_substr` (while buffering any interleaved venue frames into `early_ws_frames`).
    StaticLogin {
        login_payload: String,
        expect_substr: String,
    },
    /// Waits for a venue `challenge` / `nonce` frame (`MsgParser::scan`), invokes `responder` on
    /// `"tw-dialer"`, sends the signed response, and buffers any interleaved venue frames into
    /// `early_ws_frames` until `expect_substr` arrives.
    ChallengeResponse {
        responder: ChallengeResponseBuilder,
        expect_substr: String,
    },
}

impl std::fmt::Debug for WsDialAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::StaticLogin { expect_substr, .. } => f
                .debug_struct("StaticLogin")
                .field("expect_substr", expect_substr)
                .finish_non_exhaustive(),
            Self::ChallengeResponse { expect_substr, .. } => f
                .debug_struct("ChallengeResponse")
                .field("expect_substr", expect_substr)
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum DialTransport {
    PlainOrTls,
    WebSocket {
        host: String,
        path: String,
        extra_headers: Vec<(String, String)>,
    },
    /// Authenticated/private WebSocket supporting dynamic `listenKey`/`urlToken` read from
    /// [`GuardedValue`] and multi-step `loginChallenge` with `early_ws_frames` buffering.
    WebSocketAuthenticated {
        host: String,
        path_prefix: String,
        url_token: Option<Arc<GuardedValue<String>>>,
        extra_headers: Vec<(String, String)>,
        auth: WsDialAuth,
    },
    /// FIX session performing off-hot-path `Logon (35=A)` request + acknowledgement on `"tw-dialer"`.
    FixLogon {
        logon_frame: Vec<u8>,
        max_frame_bytes: usize,
    },
}

impl PartialEq for DialTransport {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::PlainOrTls, Self::PlainOrTls) => true,
            (
                Self::WebSocket {
                    host: h1,
                    path: p1,
                    extra_headers: e1,
                },
                Self::WebSocket {
                    host: h2,
                    path: p2,
                    extra_headers: e2,
                },
            ) => h1 == h2 && p1 == p2 && e1 == e2,
            (
                Self::FixLogon {
                    logon_frame: l1,
                    max_frame_bytes: m1,
                },
                Self::FixLogon {
                    logon_frame: l2,
                    max_frame_bytes: m2,
                },
            ) => l1 == l2 && m1 == m2,
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DialSpec {
    pub host: String,
    pub port: u16,
    pub initial_peers: Vec<SocketAddr>,
    pub local_ip: Option<IpAddr>,
    /// Optional Linux network interface (`SO_BINDTODEVICE`), taking precedence over `local_ip`.
    pub nic: Option<String>,
    pub security: DialSecurity,
    pub limits: Limits,
    pub transport: DialTransport,
    pub connect_timeout: Duration,
    pub dial_timeout: Duration,
    pub backoff: DialBackoff,
}

impl Default for DialSpec {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 443,
            initial_peers: Vec::new(),
            local_ip: None,
            nic: None,
            security: DialSecurity::Plain,
            limits: Limits::default(),
            transport: DialTransport::PlainOrTls,
            connect_timeout: Duration::from_secs(3),
            dial_timeout: Duration::from_secs(5),
            backoff: DialBackoff::default(),
        }
    }
}

impl DialSpec {
    pub fn with_nic(mut self, nic: impl Into<String>) -> Self {
        self.nic = Some(nic.into());
        self
    }

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
        if self.connect_timeout.is_zero() || self.dial_timeout.is_zero() {
            return Err(Error::Bounds);
        }
        if let Some(iface) = self.nic.as_deref()
            && (iface.is_empty()
                || iface.len() > tickwire_control_sys::MAX_INTERFACE_NAME_LEN
                || !iface.bytes().all(|b| b.is_ascii_graphic()))
        {
            return Err(Error::Bounds);
        }
        self.security.validate()?;
        if self.initial_peers.is_empty() && (self.host.is_empty() || self.port == 0) {
            return Err(Error::Bounds);
        }
        if self.nic.is_none()
            && let Some(lip) = self.local_ip
            && !self.initial_peers.is_empty()
            && self
                .initial_peers
                .iter()
                .all(|p| p.is_ipv4() != lip.is_ipv4())
            && (self.host.is_empty() || self.port == 0)
        {
            return Err(Error::Bounds);
        }
        match &self.transport {
            DialTransport::PlainOrTls => {}
            DialTransport::WebSocket { host, path, .. } => {
                if host.is_empty() || !path.starts_with('/') {
                    return Err(Error::Protocol("websocket dial spec"));
                }
            }
            DialTransport::WebSocketAuthenticated {
                host, path_prefix, ..
            } => {
                if host.is_empty() || !path_prefix.starts_with('/') {
                    return Err(Error::Protocol("websocket authenticated dial spec"));
                }
            }
            DialTransport::FixLogon {
                logon_frame,
                max_frame_bytes,
            } => {
                if logon_frame.is_empty() || *max_frame_bytes == 0 || *max_frame_bytes > HARD_LIMIT
                {
                    return Err(Error::Protocol("fix logon dial spec"));
                }
            }
        }
        Ok(())
    }
}

/// Active TCP connect-RTT probe measurement produced by `"tw-dialer"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpingResult {
    pub slot_id: u64,
    pub peer: SocketAddr,
    pub rtt_us: Option<u32>,
}

#[derive(Clone)]
pub struct DialerConfig {
    pub housekeeping_cpus: Option<CpuSet>,
    pub clock: ClockFn,
}

impl Default for DialerConfig {
    fn default() -> Self {
        Self {
            housekeeping_cpus: None,
            clock: Arc::new(mono_now_ns),
        }
    }
}

enum DialCommand {
    Register {
        slot_id: u64,
        spec: Box<DialSpec>,
        baton: Arc<DialBaton>,
    },
    Unregister {
        slot_id: u64,
    },
    ReleasePeer {
        slot_id: u64,
    },
    ProbeTcping {
        slot_id: u64,
        peer: SocketAddr,
        local_ip: Option<IpAddr>,
        nic: Option<String>,
        timeout: Duration,
    },
}

pub struct Dialer {
    commands: Arc<Mutex<Vec<DialCommand>>>,
    tcping_results: Arc<Mutex<VecDeque<TcpingResult>>>,
    stopped: Arc<AtomicBool>,
    worker_tid: Arc<AtomicI32>,
    waker: Arc<Waker>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for Dialer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dialer")
            .field("stopped", &self.stopped.load(Ordering::Acquire))
            .field("worker_tid", &self.worker_tid())
            .finish_non_exhaustive()
    }
}

impl Dialer {
    pub fn new() -> Result<Self> {
        Self::with_config(DialerConfig::default())
    }

    pub fn new_pinned(housekeeping_cpus: Option<&CpuSet>) -> Result<Self> {
        Self::with_config(DialerConfig {
            housekeeping_cpus: housekeeping_cpus.cloned(),
            clock: Arc::new(mono_now_ns),
        })
    }

    pub fn with_clock(clock: ClockFn) -> Result<Self> {
        Self::with_config(DialerConfig {
            housekeeping_cpus: None,
            clock,
        })
    }

    /// Spawns `"tw-dialer"` via `tickwire_control_sys::spawn_pinned_thread` bound to
    /// `config.housekeeping_cpus` **before** creating `mio::Poll`, resolving DNS, or opening sockets.
    pub fn with_config(config: DialerConfig) -> Result<Self> {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let tcping_results = Arc::new(Mutex::new(VecDeque::with_capacity(64)));
        let stopped = Arc::new(AtomicBool::new(false));
        let worker_tid = Arc::new(AtomicI32::new(0));

        let cmd_rx = Arc::clone(&commands);
        let tcping_tx = Arc::clone(&tcping_results);
        let stop_rx = Arc::clone(&stopped);
        let tid_cell = Arc::clone(&worker_tid);
        let clock = config.clock;

        let (startup_tx, startup_rx) = mpsc::sync_channel::<Result<Arc<Waker>>>(1);

        let handle =
            spawn_pinned_thread("tw-dialer", config.housekeeping_cpus.as_ref(), move || {
                if let Ok(tid) = current_tid() {
                    tid_cell.store(tid, Ordering::Release);
                }
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
                if startup_tx.send(Ok(Arc::clone(&waker))).is_err() {
                    return;
                }
                run_dialer_loop(poll, cmd_rx, tcping_tx, stop_rx, clock);
            })
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;

        let waker = startup_rx.recv().map_err(|_| Error::Closed)??;

        Ok(Self {
            commands,
            tcping_results,
            stopped,
            worker_tid,
            waker,
            worker: Mutex::new(Some(handle)),
        })
    }

    /// Returns the Linux kernel TID (`SYS_gettid`) of `"tw-dialer"`.
    pub fn worker_tid(&self) -> Option<i32> {
        let tid = self.worker_tid.load(Ordering::Acquire);
        (tid > 0).then_some(tid)
    }

    pub fn register(&self, slot_id: u64, spec: DialSpec, baton: Arc<DialBaton>) -> Result<()> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        spec.validate()?;
        baton.set_waker(Arc::clone(&self.waker));
        let mut cmds = self.commands.lock().unwrap_or_else(|e| e.into_inner());
        cmds.push(DialCommand::Register {
            slot_id,
            spec: Box::new(spec),
            baton,
        });
        drop(cmds);
        let _ = self.waker.wake();
        Ok(())
    }

    pub fn add_slot(&self, slot_id: u64, spec: DialSpec) -> Result<Arc<DialBaton>> {
        let baton = Arc::new(DialBaton::new());
        self.register(slot_id, spec, Arc::clone(&baton))?;
        Ok(baton)
    }

    pub fn unregister(&self, slot_id: u64) -> Result<()> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let mut cmds = self.commands.lock().unwrap_or_else(|e| e.into_inner());
        cmds.push(DialCommand::Unregister { slot_id });
        drop(cmds);
        let _ = self.waker.wake();
        Ok(())
    }

    pub fn release_slot_peer(&self, slot_id: u64) -> Result<()> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let mut cmds = self.commands.lock().unwrap_or_else(|e| e.into_inner());
        cmds.push(DialCommand::ReleasePeer { slot_id });
        drop(cmds);
        let _ = self.waker.wake();
        Ok(())
    }

    /// Enqueues an active TCP SYN-ACK RTT probe (`SampleKind::Tcping`) executed on `"tw-dialer"`.
    pub fn request_tcping(
        &self,
        slot_id: u64,
        peer: SocketAddr,
        local_ip: Option<IpAddr>,
        nic: Option<String>,
        timeout: Duration,
    ) -> Result<()> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if timeout.is_zero() {
            return Err(Error::Bounds);
        }
        let mut cmds = self.commands.lock().unwrap_or_else(|e| e.into_inner());
        cmds.push(DialCommand::ProbeTcping {
            slot_id,
            peer,
            local_ip,
            nic,
            timeout,
        });
        drop(cmds);
        let _ = self.waker.wake();
        Ok(())
    }

    pub fn drain_tcping_results(&self, mut on_result: impl FnMut(TcpingResult)) -> usize {
        let mut guard = self
            .tcping_results
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut count = 0usize;
        while let Some(res) = guard.pop_front() {
            count += 1;
            on_result(res);
        }
        count
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        let _ = self.waker.wake();
        let handle = self.worker.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(h) = handle {
            let _ = h.join();
        }
    }
}

impl Drop for Dialer {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(PartialEq, Eq)]
struct PoolKey {
    host: String,
    port: u16,
    local_ip: Option<IpAddr>,
    nic: Option<String>,
    initial_peers: Vec<SocketAddr>,
}

struct SharedPool {
    key: PoolKey,
    pool: IpPool,
    last_dns: Option<Instant>,
}

enum WsAuthPhase {
    SendingLogin {
        expect_substr: String,
    },
    WaitingChallenge {
        responder: ChallengeResponseBuilder,
        expect_substr: String,
    },
    WaitingAuthAck {
        expect_substr: String,
    },
}

enum DialPhase {
    Connecting,
    WsSending {
        handshake: ws::ClientHandshake,
    },
    WsReceiving {
        handshake: ws::ClientHandshake,
        decoder: http::ResponseDecoder,
    },
    WsAuthenticating {
        ws_decoder: ws::Decoder,
        auth_phase: WsAuthPhase,
    },
    FixLogonSending {
        max_frame_bytes: usize,
    },
    FixLogonReceiving {
        fix_decoder: fix::Decoder,
    },
}

struct ActiveAttempt {
    conn: Connection,
    peer: SocketAddr,
    started_at: Instant,
    connect_deadline: Instant,
    dial_deadline: Instant,
    tcp_rtt_us: Option<u32>,
    early_ws_frames: VecDeque<ws::Message>,
    phase: DialPhase,
}

struct ManagedSlot {
    slot_id: u64,
    token: Token,
    spec: DialSpec,
    baton: Arc<DialBaton>,
    pool_idx: usize,
    leased_peer: Option<SocketAddr>,
    active: Option<ActiveAttempt>,
}

fn measure_tcping_rtt_us(
    peer: SocketAddr,
    local_ip: Option<IpAddr>,
    nic: Option<&str>,
    timeout: Duration,
) -> Option<u32> {
    let start = Instant::now();
    let stream = connect_bound_tcp_blocking(peer, local_ip, nic, timeout).ok()?;
    let elapsed_us = start.elapsed().as_micros().min(u128::from(u32::MAX)) as u32;
    let _ = stream.shutdown(std::net::Shutdown::Both);
    Some(elapsed_us.max(1))
}

fn run_dialer_loop(
    mut poll: Poll,
    commands: Arc<Mutex<Vec<DialCommand>>>,
    tcping_results: Arc<Mutex<VecDeque<TcpingResult>>>,
    stopped: Arc<AtomicBool>,
    clock: ClockFn,
) {
    let mut events = Events::with_capacity(64);
    let mut pools: Vec<SharedPool> = Vec::new();
    let mut slots: Vec<ManagedSlot> = Vec::new();
    let mut next_token: usize = 1;

    loop {
        let batch = {
            let mut guard = commands.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *guard)
        };
        for cmd in batch {
            match cmd {
                DialCommand::Register {
                    slot_id,
                    spec,
                    baton,
                } => {
                    let spec = *spec;
                    if let Some(pos) = slots.iter().position(|s| s.slot_id == slot_id) {
                        let old = slots.remove(pos);
                        cleanup_slot(&poll, &mut pools, old);
                    }
                    let key = PoolKey {
                        host: spec.host.clone(),
                        port: spec.port,
                        local_ip: if spec.nic.is_some() {
                            None
                        } else {
                            spec.local_ip
                        },
                        nic: spec.nic.clone(),
                        initial_peers: spec.initial_peers.clone(),
                    };
                    let pool_idx = if let Some(idx) = pools.iter().position(|p| p.key == key) {
                        idx
                    } else {
                        let mut pool = IpPool::new();
                        for &addr in &spec.initial_peers {
                            if spec.nic.is_some()
                                || spec
                                    .local_ip
                                    .is_none_or(|lip| lip.is_ipv4() == addr.is_ipv4())
                            {
                                pool.add_ip(addr);
                            }
                        }
                        pools.push(SharedPool {
                            key,
                            pool,
                            last_dns: None,
                        });
                        pools.len() - 1
                    };
                    let token = Token(next_token);
                    next_token = next_token.saturating_add(1).max(1);
                    slots.push(ManagedSlot {
                        slot_id,
                        token,
                        spec,
                        baton,
                        pool_idx,
                        leased_peer: None,
                        active: None,
                    });
                }
                DialCommand::Unregister { slot_id } => {
                    if let Some(pos) = slots.iter().position(|s| s.slot_id == slot_id) {
                        let old = slots.remove(pos);
                        cleanup_slot(&poll, &mut pools, old);
                    }
                }
                DialCommand::ReleasePeer { slot_id } => {
                    if let Some(slot) = slots.iter_mut().find(|s| s.slot_id == slot_id)
                        && let Some(peer) = slot.leased_peer.take()
                    {
                        pools[slot.pool_idx].pool.release_ip(peer);
                    }
                }
                DialCommand::ProbeTcping {
                    slot_id,
                    peer,
                    local_ip,
                    nic,
                    timeout,
                } => {
                    let rtt_us = measure_tcping_rtt_us(peer, local_ip, nic.as_deref(), timeout);
                    if let Some(slot) = slots.iter().find(|s| s.slot_id == slot_id) {
                        pools[slot.pool_idx].pool.note_syn(peer, rtt_us);
                    }
                    let mut out = tcping_results.lock().unwrap_or_else(|e| e.into_inner());
                    if out.len() >= 1024 {
                        let _ = out.pop_front();
                    }
                    out.push_back(TcpingResult {
                        slot_id,
                        peer,
                        rtt_us,
                    });
                }
            }
        }

        if stopped.load(Ordering::Acquire) {
            for slot in slots.drain(..) {
                slot.baton.stop();
                cleanup_slot(&poll, &mut pools, slot);
            }
            break;
        }

        let now_mono = clock();
        for slot in &mut slots {
            if slot.active.is_none() && slot.baton.claim(now_mono) {
                start_slot_attempt(&poll, &mut pools, slot, &clock);
            }
        }

        for slot in &mut slots {
            if slot.active.is_some() {
                step_slot_attempt(&poll, &mut pools, slot, &clock);
            }
        }

        let now_mono = clock();
        let now_inst = Instant::now();
        let mut immediate = false;
        let mut min_wait: Option<Duration> = None;

        {
            let guard = commands.lock().unwrap_or_else(|e| e.into_inner());
            if !guard.is_empty() {
                immediate = true;
            }
        }
        if stopped.load(Ordering::Acquire) {
            immediate = true;
        }

        for slot in &slots {
            if let Some(active) = &slot.active {
                let rem = active
                    .dial_deadline
                    .min(active.connect_deadline)
                    .saturating_duration_since(now_inst)
                    .min(Duration::from_millis(20));
                min_wait = Some(min_wait.map_or(rem, |cur| cur.min(rem)));
            } else if slot.baton.state() == DialState::Wanted {
                let due = slot.baton.due_mono_ns();
                if due <= now_mono {
                    immediate = true;
                } else {
                    let rem = Duration::from_nanos(due - now_mono);
                    min_wait = Some(min_wait.map_or(rem, |cur| cur.min(rem)));
                }
            }
        }

        if immediate {
            continue;
        }
        let _ = poll.poll(&mut events, min_wait);
    }
}

fn cleanup_slot(poll: &Poll, pools: &mut [SharedPool], mut slot: ManagedSlot) {
    if let Some(mut active) = slot.active.take() {
        let _ = active.conn.deregister(poll.registry());
        pools[slot.pool_idx].pool.release_ip(active.peer);
    }
    if let Some(peer) = slot.leased_peer.take() {
        pools[slot.pool_idx].pool.release_ip(peer);
    }
}

fn maybe_resolve_dns(shared: &mut SharedPool, spec: &DialSpec) {
    if spec.host.is_empty() || spec.port == 0 {
        return;
    }
    let need_dns =
        shared.pool.is_empty() || (spec.initial_peers.is_empty() && shared.pool.spare_count() == 0);
    if !need_dns {
        return;
    }
    if !shared.pool.is_empty()
        && shared
            .last_dns
            .is_some_and(|t| t.elapsed() < DNS_REFRESH_COOLDOWN)
    {
        return;
    }
    shared.last_dns = Some(Instant::now());
    if let Ok(addrs) = (spec.host.as_str(), spec.port).to_socket_addrs() {
        for addr in addrs {
            if spec.nic.is_some()
                || spec
                    .local_ip
                    .is_none_or(|lip| lip.is_ipv4() == addr.is_ipv4())
            {
                shared.pool.add_ip(addr);
            }
        }
    }
}

fn fail_slot(slot: &ManagedSlot, clock: &ClockFn) {
    let failures = slot.baton.failures();
    let delay = slot.spec.backoff.delay_ns(failures);
    let next_due = clock().saturating_add(delay);
    slot.baton.fail(next_due);
}

fn start_slot_attempt(
    poll: &Poll,
    pools: &mut [SharedPool],
    slot: &mut ManagedSlot,
    clock: &ClockFn,
) {
    if let DialTransport::WebSocketAuthenticated {
        url_token: Some(token_guard),
        ..
    } = &slot.spec.transport
    {
        let token_empty = token_guard.read_with(ThreadTag::DIALER, |tok, _| tok.is_empty());
        if token_empty {
            fail_slot(slot, clock);
            return;
        }
    }

    let shared = &mut pools[slot.pool_idx];
    maybe_resolve_dns(shared, &slot.spec);

    let peer = if let Some(prev) = slot.leased_peer.take() {
        if let Some(next) = shared.pool.replace_with_unused(prev) {
            Some(next)
        } else {
            shared.pool.release_ip(prev);
            shared.pool.take_ip()
        }
    } else {
        shared.pool.take_ip()
    };

    let Some(peer) = peer else {
        fail_slot(slot, clock);
        return;
    };

    let started_at = Instant::now();
    let dial_deadline = started_at + slot.spec.dial_timeout;
    let connect_deadline = started_at + slot.spec.connect_timeout.min(slot.spec.dial_timeout);

    let mut conn = match Connection::connect_from_with_nic(
        peer,
        slot.spec.local_ip,
        slot.spec.nic.as_deref(),
        slot.spec.security.to_security(),
        slot.spec.limits,
        connect_deadline,
    ) {
        Ok(c) => c,
        Err(_) => {
            shared.pool.release_ip(peer);
            shared.pool.note_syn(peer, None);
            fail_slot(slot, clock);
            return;
        }
    };

    if conn.register(poll.registry(), slot.token).is_err() {
        shared.pool.release_ip(peer);
        shared.pool.note_syn(peer, None);
        fail_slot(slot, clock);
        return;
    }

    slot.active = Some(ActiveAttempt {
        conn,
        peer,
        started_at,
        connect_deadline,
        dial_deadline,
        tcp_rtt_us: None,
        early_ws_frames: VecDeque::new(),
        phase: DialPhase::Connecting,
    });
}

fn abort_attempt(
    poll: &Poll,
    pools: &mut [SharedPool],
    slot: &mut ManagedSlot,
    mark_failed: bool,
    clock: &ClockFn,
) {
    if let Some(mut active) = slot.active.take() {
        let _ = active.conn.deregister(poll.registry());
        let pool = &mut pools[slot.pool_idx].pool;
        pool.release_ip(active.peer);
        if mark_failed {
            pool.note_syn(active.peer, None);
            fail_slot(slot, clock);
        }
    }
}

fn finish_ready_attempt(poll: &Poll, pools: &mut [SharedPool], slot: &mut ManagedSlot) {
    let Some(mut active) = slot.active.take() else {
        return;
    };
    let rtt_us = active.tcp_rtt_us.unwrap_or_else(|| {
        active
            .started_at
            .elapsed()
            .as_micros()
            .min(u128::from(u32::MAX)) as u32
    });
    let _ = active.conn.deregister(poll.registry());
    let pool = &mut pools[slot.pool_idx].pool;
    pool.note_syn(active.peer, Some(rtt_us));
    let peer = active.peer;
    let link = DialedLink {
        connection: active.conn,
        peer,
        connect_rtt_us: rtt_us,
        early_ws_frames: active.early_ws_frames,
    };
    if slot.baton.deliver_ready(link) {
        slot.leased_peer = Some(peer);
    } else {
        pool.release_ip(peer);
    }
}

type WsUpgradeTarget = (String, String, Vec<(String, String)>);

fn build_ws_upgrade_path(spec: &DialSpec) -> Option<WsUpgradeTarget> {
    match &spec.transport {
        DialTransport::WebSocket {
            host,
            path,
            extra_headers,
        } => Some((host.clone(), path.clone(), extra_headers.clone())),
        DialTransport::WebSocketAuthenticated {
            host,
            path_prefix,
            url_token,
            extra_headers,
            ..
        } => {
            let full_path = if let Some(gv) = url_token {
                let (tok, _) = gv.get_cloned(ThreadTag::DIALER);
                if tok.is_empty() {
                    return None;
                }
                if path_prefix.ends_with('/') {
                    format!("{path_prefix}{tok}")
                } else {
                    format!("{path_prefix}/{tok}")
                }
            } else {
                path_prefix.clone()
            };
            Some((host.clone(), full_path, extra_headers.clone()))
        }
        _ => None,
    }
}

fn step_slot_attempt(
    poll: &Poll,
    pools: &mut [SharedPool],
    slot: &mut ManagedSlot,
    clock: &ClockFn,
) {
    if slot.baton.state() != DialState::Dialing {
        abort_attempt(poll, pools, slot, false, clock);
        return;
    }

    let now = Instant::now();
    let expired = slot.active.as_ref().is_some_and(|a| {
        now >= a.dial_deadline || (!a.conn.is_ready() && now >= a.connect_deadline)
    });
    if expired {
        abort_attempt(poll, pools, slot, true, clock);
        return;
    }

    let drive_res = slot
        .active
        .as_mut()
        .unwrap()
        .conn
        .drive(now, IoBudget::default());
    let drive_report = match drive_res {
        Ok(r) => r,
        Err(_) => {
            abort_attempt(poll, pools, slot, true, clock);
            return;
        }
    };

    {
        let active = slot.active.as_mut().unwrap();
        if active.tcp_rtt_us.is_none() && active.conn.is_tcp_connected() {
            active.tcp_rtt_us = Some(
                active
                    .started_at
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u32::MAX)) as u32,
            );
        }
    }

    if matches!(
        slot.active.as_ref().map(|a| &a.phase),
        Some(DialPhase::Connecting)
    ) && slot.active.as_ref().unwrap().conn.is_ready()
    {
        match &slot.spec.transport {
            DialTransport::PlainOrTls => {
                finish_ready_attempt(poll, pools, slot);
                return;
            }
            DialTransport::WebSocket { .. } | DialTransport::WebSocketAuthenticated { .. } => {
                let Some((host, path, extra_headers)) = build_ws_upgrade_path(&slot.spec) else {
                    abort_attempt(poll, pools, slot, true, clock);
                    return;
                };
                let hdr_refs: Vec<(&str, &str)> = extra_headers
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                let handshake = match ws::ClientHandshake::new(
                    &host,
                    &path,
                    &hdr_refs,
                    slot.spec.limits.max_send_bytes,
                ) {
                    Ok(h) => h,
                    Err(_) => {
                        abort_attempt(poll, pools, slot, true, clock);
                        return;
                    }
                };
                let active = slot.active.as_mut().unwrap();
                let prepared = match PreparedSend::new(
                    handshake.request(),
                    active.dial_deadline,
                    slot.spec.limits.max_send_bytes,
                ) {
                    Ok(p) => p,
                    Err(_) => {
                        abort_attempt(poll, pools, slot, true, clock);
                        return;
                    }
                };
                if active.conn.queue(prepared).is_err() {
                    abort_attempt(poll, pools, slot, true, clock);
                    return;
                }
                active.phase = DialPhase::WsSending { handshake };
            }
            DialTransport::FixLogon {
                logon_frame,
                max_frame_bytes,
            } => {
                let max_frame = *max_frame_bytes;
                let active = slot.active.as_mut().unwrap();
                let prepared = match PreparedSend::new(
                    logon_frame,
                    active.dial_deadline,
                    slot.spec.limits.max_send_bytes,
                ) {
                    Ok(p) => p,
                    Err(_) => {
                        abort_attempt(poll, pools, slot, true, clock);
                        return;
                    }
                };
                if active.conn.queue(prepared).is_err() {
                    abort_attempt(poll, pools, slot, true, clock);
                    return;
                }
                active.phase = DialPhase::FixLogonSending {
                    max_frame_bytes: max_frame,
                };
            }
        }
    }

    if matches!(
        slot.active.as_ref().map(|a| &a.phase),
        Some(DialPhase::WsSending { .. })
    ) {
        if !step_queued_send(&mut slot.active) {
            abort_attempt(poll, pools, slot, true, clock);
            return;
        }
        let flushed = slot
            .active
            .as_ref()
            .unwrap()
            .conn
            .send_report()
            .is_some_and(|r| r.state == SendState::Flushed);
        if flushed {
            let decoder = match http::ResponseDecoder::new(slot.spec.limits.max_receive_bytes, 128)
            {
                Ok(d) => d,
                Err(_) => {
                    abort_attempt(poll, pools, slot, true, clock);
                    return;
                }
            };
            let active = slot.active.as_mut().unwrap();
            let _ = active.conn.take_finished();
            let DialPhase::WsSending { handshake } =
                std::mem::replace(&mut active.phase, DialPhase::Connecting)
            else {
                unreachable!()
            };
            active.phase = DialPhase::WsReceiving { handshake, decoder };
        }
    }

    if matches!(
        slot.active.as_ref().map(|a| &a.phase),
        Some(DialPhase::WsReceiving { .. })
    ) {
        enum UpgradeStep {
            Pending,
            Ready,
            Failed,
        }
        let outcome = {
            let active = slot.active.as_mut().unwrap();
            let DialPhase::WsReceiving { handshake, decoder } = &mut active.phase else {
                unreachable!()
            };
            let mut byte = [0u8; 1];
            let mut step = UpgradeStep::Pending;
            while active.conn.read(&mut byte) == 1 {
                if decoder.push(&byte).is_err() {
                    step = UpgradeStep::Failed;
                    break;
                }
                match decoder.next_response() {
                    Ok(Some(resp)) => {
                        step = if handshake.validate(&resp).is_ok() {
                            UpgradeStep::Ready
                        } else {
                            UpgradeStep::Failed
                        };
                        break;
                    }
                    Ok(None) => {}
                    Err(_) => {
                        step = UpgradeStep::Failed;
                        break;
                    }
                }
            }
            if matches!(step, UpgradeStep::Pending)
                && (drive_report.peer_closed || !active.conn.is_ready())
            {
                step = UpgradeStep::Failed;
            }
            step
        };
        match outcome {
            UpgradeStep::Ready => match &slot.spec.transport {
                DialTransport::WebSocketAuthenticated { auth, .. } => match auth.clone() {
                    WsDialAuth::None => {
                        finish_ready_attempt(poll, pools, slot);
                        return;
                    }
                    WsDialAuth::StaticLogin {
                        login_payload,
                        expect_substr,
                    } => {
                        let Ok(ws_decoder) = ws::Decoder::new(
                            slot.spec.limits.max_receive_bytes,
                            slot.spec.limits.max_receive_bytes,
                        ) else {
                            abort_attempt(poll, pools, slot, true, clock);
                            return;
                        };
                        let Ok(frame) = ws::encode_client(
                            ws::Opcode::Text,
                            login_payload.as_bytes(),
                            slot.spec.limits.max_send_bytes,
                        ) else {
                            abort_attempt(poll, pools, slot, true, clock);
                            return;
                        };
                        let active = slot.active.as_mut().unwrap();
                        let Ok(prepared) = PreparedSend::new(
                            &frame,
                            active.dial_deadline,
                            slot.spec.limits.max_send_bytes,
                        ) else {
                            abort_attempt(poll, pools, slot, true, clock);
                            return;
                        };
                        if active.conn.queue(prepared).is_err() {
                            abort_attempt(poll, pools, slot, true, clock);
                            return;
                        }
                        active.phase = DialPhase::WsAuthenticating {
                            ws_decoder,
                            auth_phase: WsAuthPhase::SendingLogin { expect_substr },
                        };
                    }
                    WsDialAuth::ChallengeResponse {
                        responder,
                        expect_substr,
                    } => {
                        let Ok(ws_decoder) = ws::Decoder::new(
                            slot.spec.limits.max_receive_bytes,
                            slot.spec.limits.max_receive_bytes,
                        ) else {
                            abort_attempt(poll, pools, slot, true, clock);
                            return;
                        };
                        let active = slot.active.as_mut().unwrap();
                        active.phase = DialPhase::WsAuthenticating {
                            ws_decoder,
                            auth_phase: WsAuthPhase::WaitingChallenge {
                                responder,
                                expect_substr,
                            },
                        };
                    }
                },
                _ => {
                    finish_ready_attempt(poll, pools, slot);
                    return;
                }
            },
            UpgradeStep::Failed => {
                abort_attempt(poll, pools, slot, true, clock);
                return;
            }
            UpgradeStep::Pending => {}
        }
    }

    if matches!(
        slot.active.as_ref().map(|a| &a.phase),
        Some(DialPhase::WsAuthenticating { .. })
    ) {
        match step_ws_auth(slot, drive_report.peer_closed) {
            AuthStep::Ready => {
                finish_ready_attempt(poll, pools, slot);
                return;
            }
            AuthStep::Failed => {
                abort_attempt(poll, pools, slot, true, clock);
                return;
            }
            AuthStep::Pending => {}
        }
    }

    if matches!(
        slot.active.as_ref().map(|a| &a.phase),
        Some(DialPhase::FixLogonSending { .. })
    ) {
        if !step_queued_send(&mut slot.active) {
            abort_attempt(poll, pools, slot, true, clock);
            return;
        }
        let flushed = slot
            .active
            .as_ref()
            .unwrap()
            .conn
            .send_report()
            .is_some_and(|r| r.state == SendState::Flushed);
        if flushed {
            let active = slot.active.as_mut().unwrap();
            let _ = active.conn.take_finished();
            let DialPhase::FixLogonSending { max_frame_bytes } = active.phase else {
                unreachable!()
            };
            let Ok(fix_decoder) = fix::Decoder::new(max_frame_bytes) else {
                abort_attempt(poll, pools, slot, true, clock);
                return;
            };
            active.phase = DialPhase::FixLogonReceiving { fix_decoder };
        }
    }

    if matches!(
        slot.active.as_ref().map(|a| &a.phase),
        Some(DialPhase::FixLogonReceiving { .. })
    ) {
        let step = {
            let active = slot.active.as_mut().unwrap();
            let DialPhase::FixLogonReceiving { fix_decoder } = &mut active.phase else {
                unreachable!()
            };
            let mut buf = [0u8; 4096];
            let mut st = AuthStep::Pending;
            loop {
                let n = active.conn.read(&mut buf);
                if n == 0 {
                    break;
                }
                if fix_decoder.push(&buf[..n]).is_err() {
                    st = AuthStep::Failed;
                    break;
                }
                match fix_decoder.next_frame() {
                    Ok(Some(frame)) => {
                        let has_logon = frame.split(|&b| b == 1).any(|f| f == b"35=A");
                        st = if has_logon {
                            AuthStep::Ready
                        } else {
                            AuthStep::Failed
                        };
                        break;
                    }
                    Ok(None) => {}
                    Err(_) => {
                        st = AuthStep::Failed;
                        break;
                    }
                }
            }
            if matches!(st, AuthStep::Pending)
                && (drive_report.peer_closed || !active.conn.is_ready())
            {
                st = AuthStep::Failed;
            }
            st
        };
        match step {
            AuthStep::Ready => {
                finish_ready_attempt(poll, pools, slot);
                return;
            }
            AuthStep::Failed => {
                abort_attempt(poll, pools, slot, true, clock);
                return;
            }
            AuthStep::Pending => {}
        }
    }

    if let Some(active) = slot.active.as_mut()
        && active.conn.reregister(poll.registry(), slot.token).is_err()
    {
        abort_attempt(poll, pools, slot, true, clock);
    }
}

enum AuthStep {
    Pending,
    Ready,
    Failed,
}

fn step_queued_send(active_opt: &mut Option<ActiveAttempt>) -> bool {
    let Some(active) = active_opt.as_mut() else {
        return false;
    };
    let now = Instant::now();
    if active.conn.try_send(now, || true).is_err() {
        return false;
    }
    if active.conn.drive(now, IoBudget::default()).is_err() {
        return false;
    }
    !matches!(
        active.conn.send_report().map(|r| r.state),
        Some(SendState::NoBytes(_) | SendState::Failed(_))
    )
}

fn step_ws_auth(slot: &mut ManagedSlot, peer_closed: bool) -> AuthStep {
    let max_send = slot.spec.limits.max_send_bytes;
    let active = slot.active.as_mut().unwrap();

    if matches!(
        &active.phase,
        DialPhase::WsAuthenticating {
            auth_phase: WsAuthPhase::SendingLogin { .. },
            ..
        }
    ) {
        let now = Instant::now();
        if active.conn.try_send(now, || true).is_err()
            || active.conn.drive(now, IoBudget::default()).is_err()
            || matches!(
                active.conn.send_report().map(|r| r.state),
                Some(SendState::NoBytes(_) | SendState::Failed(_))
            )
        {
            return AuthStep::Failed;
        }
        if active
            .conn
            .send_report()
            .is_some_and(|r| r.state == SendState::Flushed)
        {
            let _ = active.conn.take_finished();
            if let DialPhase::WsAuthenticating { auth_phase, .. } = &mut active.phase
                && let WsAuthPhase::SendingLogin { expect_substr } = std::mem::replace(
                    auth_phase,
                    WsAuthPhase::WaitingAuthAck {
                        expect_substr: String::new(),
                    },
                )
            {
                *auth_phase = WsAuthPhase::WaitingAuthAck { expect_substr };
            }
        }
    }

    let DialPhase::WsAuthenticating {
        ws_decoder,
        auth_phase,
    } = &mut active.phase
    else {
        return AuthStep::Failed;
    };

    let mut rx_buf = [0u8; 4096];
    loop {
        let n = active.conn.read(&mut rx_buf);
        if n == 0 {
            break;
        }
        if ws_decoder.push(&rx_buf[..n]).is_err() {
            return AuthStep::Failed;
        }
    }

    loop {
        let msg = match ws_decoder.next_message() {
            Ok(Some(m)) => m,
            Ok(None) => break,
            Err(_) => return AuthStep::Failed,
        };
        if msg.opcode == ws::Opcode::Close {
            return AuthStep::Failed;
        }
        if !matches!(msg.opcode, ws::Opcode::Text | ws::Opcode::Binary) {
            continue;
        }
        match auth_phase {
            WsAuthPhase::WaitingChallenge {
                responder,
                expect_substr,
            } => {
                if let Ok(scanned) = MsgParser::scan(&msg.payload)
                    && let Some(ch) = scanned.challenge
                {
                    let Ok(response_text) = responder(ch) else {
                        return AuthStep::Failed;
                    };
                    let Ok(frame) =
                        ws::encode_client(ws::Opcode::Text, response_text.as_bytes(), max_send)
                    else {
                        return AuthStep::Failed;
                    };
                    let Ok(prepared) = PreparedSend::new(&frame, active.dial_deadline, max_send)
                    else {
                        return AuthStep::Failed;
                    };
                    if active.conn.queue(prepared).is_err() {
                        return AuthStep::Failed;
                    }
                    let expected = expect_substr.clone();
                    *auth_phase = WsAuthPhase::SendingLogin {
                        expect_substr: expected,
                    };
                    let now = Instant::now();
                    let _ = active.conn.try_send(now, || true);
                    let _ = active.conn.drive(now, IoBudget::default());
                    if active
                        .conn
                        .send_report()
                        .is_some_and(|r| r.state == SendState::Flushed)
                    {
                        let _ = active.conn.take_finished();
                        if let WsAuthPhase::SendingLogin { expect_substr: ex } = auth_phase {
                            let ex = ex.clone();
                            *auth_phase = WsAuthPhase::WaitingAuthAck { expect_substr: ex };
                        }
                    }
                } else {
                    if active.early_ws_frames.len() >= crate::links::MAX_EARLY_WS_FRAMES {
                        return AuthStep::Failed;
                    }
                    active.early_ws_frames.push_back(msg);
                }
            }
            WsAuthPhase::SendingLogin { expect_substr }
            | WsAuthPhase::WaitingAuthAck { expect_substr } => {
                let payload_str = core::str::from_utf8(&msg.payload).unwrap_or("");
                if !expect_substr.is_empty() && payload_str.contains(expect_substr.as_str()) {
                    return AuthStep::Ready;
                }
                if let Ok(scanned) = MsgParser::scan(&msg.payload)
                    && scanned.has_venue_error()
                {
                    return AuthStep::Failed;
                }
                if active.early_ws_frames.len() >= crate::links::MAX_EARLY_WS_FRAMES {
                    return AuthStep::Failed;
                }
                active.early_ws_frames.push_back(msg);
            }
        }
    }

    if peer_closed || !active.conn.is_ready() {
        return AuthStep::Failed;
    }
    AuthStep::Pending
}
