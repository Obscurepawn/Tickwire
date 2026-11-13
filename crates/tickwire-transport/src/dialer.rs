//! Off-hot-path connection dialer, single-slot atomic baton, and RTT-ranked IP pool.
use crate::{
    ClockFn, Connection, Error, IoBudget, Limits, PreparedSend, Result, Security, SendState, http,
    mono_now_ns, ws,
};
use mio::{Events, Poll, Token, Waker};
use std::{
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

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
}

pub struct DialBaton {
    state: AtomicU8,
    due_mono_ns: AtomicU64,
    failures: AtomicU32,
    product: Mutex<Option<DialedLink>>,
    waker: Mutex<Option<Arc<Waker>>>,
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
            product: Mutex::new(None),
            waker: Mutex::new(None),
        }
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
        let guard = self.product.lock().unwrap_or_else(|e| e.into_inner());
        if self.state.load(Ordering::Acquire) != DialState::Idle as u8 {
            return false;
        }
        debug_assert!(guard.is_none());
        self.due_mono_ns.store(due_mono_ns, Ordering::Release);
        self.state.store(DialState::Wanted as u8, Ordering::Release);
        drop(guard);
        self.notify_waker();
        true
    }

    pub fn claim(&self, now_mono_ns: u64) -> bool {
        if self.state.load(Ordering::Acquire) != DialState::Wanted as u8 {
            return false;
        }
        if now_mono_ns < self.due_mono_ns.load(Ordering::Acquire) {
            return false;
        }
        let guard = self.product.lock().unwrap_or_else(|e| e.into_inner());
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
        let mut guard = self.product.lock().unwrap_or_else(|e| e.into_inner());
        if self.state.load(Ordering::Acquire) != DialState::Dialing as u8 {
            return false;
        }
        *guard = Some(conn);
        self.state.store(DialState::Ready as u8, Ordering::Release);
        true
    }

    pub fn fail(&self, next_due_mono_ns: u64) -> bool {
        let guard = self.product.lock().unwrap_or_else(|e| e.into_inner());
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
        let mut guard = self.product.lock().unwrap_or_else(|e| e.into_inner());
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
        let mut guard = self.product.lock().unwrap_or_else(|e| e.into_inner());
        if self.state.load(Ordering::Acquire) == DialState::Stopped as u8 {
            return;
        }
        let dropped = guard.take();
        self.failures.store(0, Ordering::Release);
        self.due_mono_ns.store(0, Ordering::Release);
        self.state.store(DialState::Idle as u8, Ordering::Release);
        drop(guard);
        drop(dropped);
        self.notify_waker();
    }

    pub fn stop(&self) {
        let mut guard = self.product.lock().unwrap_or_else(|e| e.into_inner());
        let dropped = guard.take();
        self.state
            .store(DialState::Stopped as u8, Ordering::Release);
        drop(guard);
        drop(dropped);
        self.notify_waker();
    }

    fn set_waker(&self, waker: Arc<Waker>) {
        let mut guard = self.waker.lock().unwrap_or_else(|e| e.into_inner());
        *guard = Some(waker);
    }

    fn notify_waker(&self) {
        let waker = self.waker.lock().unwrap_or_else(|e| e.into_inner()).clone();
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DialTransport {
    PlainOrTls,
    WebSocket {
        host: String,
        path: String,
        extra_headers: Vec<(String, String)>,
    },
}

#[derive(Clone, Debug)]
pub struct DialSpec {
    pub host: String,
    pub port: u16,
    pub initial_peers: Vec<SocketAddr>,
    pub local_ip: Option<IpAddr>,
    pub security: DialSecurity,
    pub limits: Limits,
    pub transport: DialTransport,
    pub connect_timeout: Duration,
    pub dial_timeout: Duration,
    pub backoff: DialBackoff,
}

impl DialSpec {
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
        self.security.validate()?;
        if self.initial_peers.is_empty() && (self.host.is_empty() || self.port == 0) {
            return Err(Error::Bounds);
        }
        if let Some(lip) = self.local_ip
            && !self.initial_peers.is_empty()
            && self
                .initial_peers
                .iter()
                .all(|p| p.is_ipv4() != lip.is_ipv4())
            && (self.host.is_empty() || self.port == 0)
        {
            return Err(Error::Bounds);
        }
        if let DialTransport::WebSocket { host, path, .. } = &self.transport
            && (host.is_empty() || !path.starts_with('/'))
        {
            return Err(Error::Protocol("websocket dial spec"));
        }
        Ok(())
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
}

pub struct Dialer {
    commands: Arc<Mutex<Vec<DialCommand>>>,
    stopped: Arc<AtomicBool>,
    waker: Arc<Waker>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for Dialer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dialer")
            .field("stopped", &self.stopped.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl Dialer {
    pub fn new() -> Result<Self> {
        Self::with_clock(Arc::new(mono_now_ns))
    }

    pub fn with_clock(clock: ClockFn) -> Result<Self> {
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKE_TOKEN)?);
        let commands = Arc::new(Mutex::new(Vec::new()));
        let stopped = Arc::new(AtomicBool::new(false));
        let cmd_rx = Arc::clone(&commands);
        let stop_rx = Arc::clone(&stopped);
        let handle = thread::Builder::new()
            .name("tw-dialer".to_owned())
            .spawn(move || run_dialer_loop(poll, cmd_rx, stop_rx, clock))?;
        Ok(Self {
            commands,
            stopped,
            waker,
            worker: Mutex::new(Some(handle)),
        })
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
    initial_peers: Vec<SocketAddr>,
}

struct SharedPool {
    key: PoolKey,
    pool: IpPool,
    last_dns: Option<Instant>,
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
}

struct ActiveAttempt {
    conn: Connection,
    peer: SocketAddr,
    started_at: Instant,
    connect_deadline: Instant,
    dial_deadline: Instant,
    tcp_rtt_us: Option<u32>,
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

fn run_dialer_loop(
    mut poll: Poll,
    commands: Arc<Mutex<Vec<DialCommand>>>,
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
                        local_ip: spec.local_ip,
                        initial_peers: spec.initial_peers.clone(),
                    };
                    let pool_idx = if let Some(idx) = pools.iter().position(|p| p.key == key) {
                        idx
                    } else {
                        let mut pool = IpPool::new();
                        for &addr in &spec.initial_peers {
                            if spec
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
            if spec
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

    let mut conn = match Connection::connect_from(
        peer,
        slot.spec.local_ip,
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
    };
    if slot.baton.deliver_ready(link) {
        slot.leased_peer = Some(peer);
    } else {
        pool.release_ip(peer);
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
            DialTransport::WebSocket {
                host,
                path,
                extra_headers,
            } => {
                let hdr_refs: Vec<(&str, &str)> = extra_headers
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                let handshake = match ws::ClientHandshake::new(
                    host,
                    path,
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
        }
    }

    if matches!(
        slot.active.as_ref().map(|a| &a.phase),
        Some(DialPhase::WsSending { .. })
    ) {
        let send_failed = {
            let active = slot.active.as_mut().unwrap();
            let now = Instant::now();
            active.conn.try_send(now, || true).is_err()
                || active.conn.drive(now, IoBudget::default()).is_err()
                || matches!(
                    active.conn.send_report().map(|r| r.state),
                    Some(SendState::NoBytes(_) | SendState::Failed(_))
                )
        };
        if send_failed {
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
            UpgradeStep::Ready => {
                finish_ready_attempt(poll, pools, slot);
                return;
            }
            UpgradeStep::Failed => {
                abort_attempt(poll, pools, slot, true, clock);
                return;
            }
            UpgradeStep::Pending => {}
        }
    }

    if let Some(active) = slot.active.as_mut()
        && active.conn.reregister(poll.registry(), slot.token).is_err()
    {
        abort_attempt(poll, pools, slot, true, clock);
    }
}
