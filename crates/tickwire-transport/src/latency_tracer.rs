//! Hardware `rdtsc` stage latency tracer (`MarkSite`), userspace `TimedSpinLock` slow-path
//! contention attribution (`LockWait`), and `housekeeping_cpus`-pinned `"tw-latency-sink"` worker.
#![forbid(unsafe_code)]

use crate::{Error, Result};
use std::{
    collections::VecDeque,
    ops::{Deref, DerefMut},
    sync::{
        Arc, Condvar, Mutex, MutexGuard, TryLockError,
        atomic::{AtomicBool, AtomicI32, AtomicU8, AtomicU64, Ordering},
        mpsc,
    },
    thread::JoinHandle,
    time::Duration,
};
use tickwire_clock_sys::{TscCalibration, rdtsc};
use tickwire_control_sys::{CpuSet, current_tid, spawn_pinned_thread};

pub const DEFAULT_TRACE_RING_CAPACITY: usize = 4096;
pub const MAX_TRACE_RING_CAPACITY: usize = 1 << 20;
pub const DEFAULT_RECENT_FINDINGS: usize = 256;

/// Identifies the logical runtime thread emitting a `MarkSite` or waiting on / holding a spinlock.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ThreadTag(pub u8);

impl ThreadTag {
    pub const UNKNOWN: Self = Self(0);
    pub const REQ: Self = Self(1);
    pub const RESP: Self = Self(2);
    pub const MD: Self = Self(3);
    pub const STRATEGY: Self = Self(4);
    pub const DIALER: Self = Self(5);
    pub const REST_QUERY: Self = Self(6);
    pub const CONTROL: Self = Self(7);

    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self.0 {
            0 => "unknown",
            1 => "REQ",
            2 => "RESP",
            3 => "MD",
            4 => "STRATEGY",
            5 => "DIALER",
            6 => "REST_QUERY",
            7 => "CONTROL",
            _ => "CUSTOM",
        }
    }
}

/// Identifies a hot-path stage boundary recorded via `rdtsc()`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct MarkSite(pub u16);

impl MarkSite {
    pub const ITER_BEGIN: Self = Self(1);
    pub const SHM_POLL: Self = Self(2);
    pub const RISK_CHECK: Self = Self(3);
    pub const ROUTE_PICK: Self = Self(4);
    pub const RATE_SPEND: Self = Self(5);
    pub const WIRE_ENCODE: Self = Self(6);
    pub const SOCKET_SEND: Self = Self(7);
    pub const EPOLL_WAIT: Self = Self(8);
    pub const TLS_READ: Self = Self(9);
    pub const JSON_SCAN: Self = Self(10);
    pub const ORDER_MATCH: Self = Self(11);
    pub const REPORT_PUBLISH: Self = Self(12);
    pub const ITER_END: Self = Self(13);
}

/// Fine-grained spinlock domain tag encoded into the high byte of [`LockId`].
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LockDomain {
    Connection = 1,
    Order = 2,
    StpBook = 3,
    GuardedValue = 4,
    Custom = 5,
}

impl LockDomain {
    #[inline]
    pub const fn from_u8(raw: u8) -> Self {
        match raw {
            1 => Self::Connection,
            2 => Self::Order,
            3 => Self::StpBook,
            4 => Self::GuardedValue,
            _ => Self::Custom,
        }
    }
}

/// Compact 32-bit fine-grained spinlock identifier (`domain << 24 | index`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct LockId(pub u32);

impl LockId {
    #[inline]
    pub const fn new(domain: LockDomain, index: u32) -> Self {
        Self(((domain as u32) << 24) | (index & 0x00ff_ffff))
    }

    #[inline]
    pub const fn domain(self) -> LockDomain {
        LockDomain::from_u8((self.0 >> 24) as u8)
    }

    #[inline]
    pub const fn index(self) -> u32 {
        self.0 & 0x00ff_ffff
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecordKind {
    IterBegin = 1,
    StageMark = 2,
    IterEnd = 3,
    LockWait = 4,
}

impl RecordKind {
    #[inline]
    const fn from_u8(raw: u8) -> Option<Self> {
        match raw {
            1 => Some(Self::IterBegin),
            2 => Some(Self::StageMark),
            3 => Some(Self::IterEnd),
            4 => Some(Self::LockWait),
            _ => None,
        }
    }
}

/// Exact 32-byte atomic ring slot (`4 * AtomicU64`) written lock-free on hot threads.
#[repr(C)]
struct TraceSlot {
    seq: AtomicU64,
    meta: AtomicU64,
    tsc_or_wait: AtomicU64,
    budget_cycles: AtomicU64,
}

const _: () = assert!(core::mem::size_of::<TraceSlot>() == 32);

#[inline]
const fn pack_meta(
    kind: RecordKind,
    thread: ThreadTag,
    holder: ThreadTag,
    iter_seq: u16,
    site_or_lock: u32,
) -> u64 {
    (kind as u64)
        | ((thread.0 as u64) << 8)
        | ((holder.0 as u64) << 16)
        | ((iter_seq as u64) << 24)
        | ((site_or_lock as u64) << 32)
}

/// Note: `iter_seq` occupies bits `24..32` (`u8` wrap counter) so `site_or_lock` retains full 32 bits!
#[inline]
const fn unpack_meta(meta: u64) -> (Option<RecordKind>, ThreadTag, ThreadTag, u8, u32) {
    let kind = RecordKind::from_u8((meta & 0xff) as u8);
    let thread = ThreadTag(((meta >> 8) & 0xff) as u8);
    let holder = ThreadTag(((meta >> 16) & 0xff) as u8);
    let iter_seq = ((meta >> 24) & 0xff) as u8;
    let site_or_lock = (meta >> 32) as u32;
    (kind, thread, holder, iter_seq, site_or_lock)
}

struct TraceRing {
    slots: Box<[TraceSlot]>,
    mask: usize,
    write_pos: AtomicU64,
    dropped: AtomicU64,
}

impl TraceRing {
    fn new(capacity: usize) -> Result<Self> {
        if capacity < 16 || !capacity.is_power_of_two() || capacity > MAX_TRACE_RING_CAPACITY {
            return Err(Error::Bounds);
        }
        let slots = (0..capacity)
            .map(|idx| TraceSlot {
                seq: AtomicU64::new(idx as u64),
                meta: AtomicU64::new(0),
                tsc_or_wait: AtomicU64::new(0),
                budget_cycles: AtomicU64::new(0),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            slots,
            mask: capacity - 1,
            write_pos: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        })
    }

    #[inline]
    fn push(&self, meta: u64, tsc_or_wait: u64, budget_cycles: u64) -> bool {
        let mut pos = self.write_pos.load(Ordering::Relaxed);
        loop {
            let slot = &self.slots[(pos as usize) & self.mask];
            let seq = slot.seq.load(Ordering::Acquire);
            let diff = (seq as i64).wrapping_sub(pos as i64);
            if diff == 0 {
                match self.write_pos.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        slot.meta.store(meta, Ordering::Relaxed);
                        slot.tsc_or_wait.store(tsc_or_wait, Ordering::Relaxed);
                        slot.budget_cycles.store(budget_cycles, Ordering::Relaxed);
                        slot.seq.store(pos.wrapping_add(1), Ordering::Release);
                        return true;
                    }
                    Err(cur) => pos = cur,
                }
            } else if diff < 0 {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                return false;
            } else {
                pos = self.write_pos.load(Ordering::Relaxed);
            }
        }
    }
}

/// Diagnostic finding emitted by `"tw-latency-sink"` off the hot path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FindingKind {
    StageBudgetExceeded {
        thread: ThreadTag,
        from_site: MarkSite,
        to_site: MarkSite,
        delta_cycles: u64,
        delta_ns: u64,
        budget_cycles: u64,
    },
    IterationSpike {
        thread: ThreadTag,
        total_cycles: u64,
        total_ns: u64,
        budget_cycles: u64,
    },
    LockContention {
        waiter: ThreadTag,
        holder: ThreadTag,
        lock_id: LockId,
        wait_cycles: u64,
        wait_ns: u64,
        threshold_cycles: u64,
    },
    QueueWrap {
        dropped_delta: u64,
        total_dropped: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LatencyFinding {
    pub seq: u64,
    pub kind: FindingKind,
}

#[derive(Clone, Debug)]
pub struct LatencyTracerConfig {
    pub ring_capacity: usize,
    pub max_retained_findings: usize,
    pub poll_interval: Duration,
    pub calibration: TscCalibration,
    pub default_stage_budget_cycles: u64,
    pub default_iter_budget_cycles: u64,
    pub default_lock_threshold_cycles: u64,
    pub housekeeping_cpus: Option<CpuSet>,
}

impl Default for LatencyTracerConfig {
    fn default() -> Self {
        let calibration = TscCalibration::calibrate(Duration::from_millis(2));
        Self {
            ring_capacity: DEFAULT_TRACE_RING_CAPACITY,
            max_retained_findings: DEFAULT_RECENT_FINDINGS,
            poll_interval: Duration::from_millis(2),
            default_stage_budget_cycles: calibration.us_to_cycles(50),
            default_iter_budget_cycles: calibration.us_to_cycles(200),
            default_lock_threshold_cycles: calibration.ns_to_cycles(500),
            calibration,
            housekeeping_cpus: None,
        }
    }
}

pub type FindingCallback = Box<dyn FnMut(&LatencyFinding) + Send + 'static>;

struct SharedSinkState {
    ring: TraceRing,
    stopped: AtomicBool,
    sink_tid: AtomicI32,
    processed_pos: AtomicU64,
    findings_count: AtomicU64,
    wake_mu: Mutex<u64>,
    wake_cv: Condvar,
    retained: Mutex<VecDeque<LatencyFinding>>,
    max_retained: usize,
}

/// Hardware `rdtsc` stage & spinlock contention tracer backed by a `housekeeping_cpus`-pinned
/// `"tw-latency-sink"` thread.
pub struct LatencyTracer {
    shared: Arc<SharedSinkState>,
    calibration: TscCalibration,
    default_stage_budget_cycles: u64,
    default_iter_budget_cycles: u64,
    default_lock_threshold_cycles: u64,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for LatencyTracer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LatencyTracer")
            .field("sink_tid", &self.sink_tid())
            .field("dropped_records", &self.dropped_records())
            .field("findings_count", &self.findings_count())
            .finish_non_exhaustive()
    }
}

impl LatencyTracer {
    pub fn new(config: LatencyTracerConfig) -> Result<Self> {
        Self::with_callback(config, None)
    }

    pub fn with_callback(
        config: LatencyTracerConfig,
        callback: Option<FindingCallback>,
    ) -> Result<Self> {
        if config.max_retained_findings == 0 || config.poll_interval.is_zero() {
            return Err(Error::Bounds);
        }
        let ring = TraceRing::new(config.ring_capacity)?;
        let shared = Arc::new(SharedSinkState {
            ring,
            stopped: AtomicBool::new(false),
            sink_tid: AtomicI32::new(0),
            processed_pos: AtomicU64::new(0),
            findings_count: AtomicU64::new(0),
            wake_mu: Mutex::new(0),
            wake_cv: Condvar::new(),
            retained: Mutex::new(VecDeque::with_capacity(
                config.max_retained_findings.min(1024),
            )),
            max_retained: config.max_retained_findings,
        });

        let (ready_tx, ready_rx) = mpsc::sync_channel::<()>(1);
        let worker_shared = Arc::clone(&shared);
        let calibration = config.calibration;
        let poll_interval = config.poll_interval;

        let handle = spawn_pinned_thread(
            "tw-latency-sink",
            config.housekeeping_cpus.as_ref(),
            move || {
                if let Ok(tid) = current_tid() {
                    worker_shared.sink_tid.store(tid, Ordering::Release);
                }
                let _ = ready_tx.send(());
                run_sink_loop(worker_shared, calibration, poll_interval, callback);
            },
        )
        .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;

        let _ = ready_rx.recv();

        Ok(Self {
            shared,
            calibration: config.calibration,
            default_stage_budget_cycles: config.default_stage_budget_cycles,
            default_iter_budget_cycles: config.default_iter_budget_cycles,
            default_lock_threshold_cycles: config.default_lock_threshold_cycles,
            worker: Mutex::new(Some(handle)),
        })
    }

    #[inline]
    pub const fn calibration(&self) -> TscCalibration {
        self.calibration
    }

    #[inline]
    pub const fn default_lock_threshold_cycles(&self) -> u64 {
        self.default_lock_threshold_cycles
    }

    /// Returns the Linux kernel TID (`SYS_gettid`) of `"tw-latency-sink"` once started.
    pub fn sink_tid(&self) -> Option<i32> {
        let tid = self.shared.sink_tid.load(Ordering::Acquire);
        (tid > 0).then_some(tid)
    }

    #[inline]
    pub fn dropped_records(&self) -> u64 {
        self.shared.ring.dropped.load(Ordering::Acquire)
    }

    #[inline]
    pub fn findings_count(&self) -> u64 {
        self.shared.findings_count.load(Ordering::Acquire)
    }

    #[inline]
    pub fn thread_tracer(self: &Arc<Self>, thread: ThreadTag) -> ThreadTracer {
        ThreadTracer {
            tracer: Arc::clone(self),
            thread,
            iter_seq: 0,
        }
    }

    /// Hot-path zero-allocation stage mark at explicit cycle budget (`0` uses default budget).
    #[inline(always)]
    pub fn record_stage(
        &self,
        thread: ThreadTag,
        iter_seq: u8,
        site: MarkSite,
        budget_cycles: u64,
    ) {
        let tsc = rdtsc();
        let budget = if budget_cycles == 0 {
            self.default_stage_budget_cycles
        } else {
            budget_cycles
        };
        let meta = pack_meta(
            RecordKind::StageMark,
            thread,
            ThreadTag::UNKNOWN,
            u16::from(iter_seq),
            u32::from(site.0),
        );
        let _ = self.shared.ring.push(meta, tsc, budget);
    }

    #[inline(always)]
    pub fn record_iter_begin(&self, thread: ThreadTag, iter_seq: u8) {
        let tsc = rdtsc();
        let meta = pack_meta(
            RecordKind::IterBegin,
            thread,
            ThreadTag::UNKNOWN,
            u16::from(iter_seq),
            u32::from(MarkSite::ITER_BEGIN.0),
        );
        let _ = self.shared.ring.push(meta, tsc, 0);
    }

    #[inline(always)]
    pub fn record_iter_end(&self, thread: ThreadTag, iter_seq: u8, budget_cycles: u64) {
        let tsc = rdtsc();
        let budget = if budget_cycles == 0 {
            self.default_iter_budget_cycles
        } else {
            budget_cycles
        };
        let meta = pack_meta(
            RecordKind::IterEnd,
            thread,
            ThreadTag::UNKNOWN,
            u16::from(iter_seq),
            u32::from(MarkSite::ITER_END.0),
        );
        let _ = self.shared.ring.push(meta, tsc, budget);
    }

    /// Hot-path zero-allocation spinlock contention attribution record.
    #[inline(always)]
    pub fn record_lock_wait(
        &self,
        waiter: ThreadTag,
        holder: ThreadTag,
        lock_id: LockId,
        wait_cycles: u64,
        threshold_cycles: u64,
    ) {
        let meta = pack_meta(RecordKind::LockWait, waiter, holder, 0, lock_id.0);
        let _ = self.shared.ring.push(meta, wait_cycles, threshold_cycles);
    }

    /// Wakes `"tw-latency-sink"` and waits up to `timeout` for all currently queued ring records to drain.
    pub fn flush(&self, timeout: Duration) -> bool {
        let target = self.shared.ring.write_pos.load(Ordering::Acquire);
        {
            let mut g = self
                .shared
                .wake_mu
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *g = g.wrapping_add(1);
            self.shared.wake_cv.notify_all();
        }
        let deadline = std::time::Instant::now() + timeout;
        while self.shared.processed_pos.load(Ordering::Acquire) < target {
            if std::time::Instant::now() >= deadline {
                return false;
            }
            {
                let mut g = self
                    .shared
                    .wake_mu
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                *g = g.wrapping_add(1);
                self.shared.wake_cv.notify_all();
            }
            std::thread::sleep(Duration::from_micros(250));
        }
        true
    }

    pub fn drain_findings(&self) -> Vec<LatencyFinding> {
        let mut guard = self
            .shared
            .retained
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard.drain(..).collect()
    }

    pub fn stop(&self) {
        self.shared.stopped.store(true, Ordering::Release);
        {
            let mut g = self
                .shared
                .wake_mu
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *g = g.wrapping_add(1);
            self.shared.wake_cv.notify_all();
        }
        let handle = self.worker.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(h) = handle {
            let _ = h.join();
        }
    }
}

impl Drop for LatencyTracer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Per-thread hot-path handle tracking iteration sequence numbers without atomic contention.
#[derive(Clone, Debug)]
pub struct ThreadTracer {
    tracer: Arc<LatencyTracer>,
    thread: ThreadTag,
    iter_seq: u8,
}

impl ThreadTracer {
    #[inline(always)]
    pub const fn thread(&self) -> ThreadTag {
        self.thread
    }

    #[inline(always)]
    pub fn begin_iteration(&mut self) {
        self.iter_seq = self.iter_seq.wrapping_add(1);
        self.tracer.record_iter_begin(self.thread, self.iter_seq);
    }

    #[inline(always)]
    pub fn mark(&self, site: MarkSite) {
        self.tracer
            .record_stage(self.thread, self.iter_seq, site, 0);
    }

    #[inline(always)]
    pub fn mark_with_budget(&self, site: MarkSite, budget_cycles: u64) {
        self.tracer
            .record_stage(self.thread, self.iter_seq, site, budget_cycles);
    }

    #[inline(always)]
    pub fn end_iteration(&self) {
        self.tracer.record_iter_end(self.thread, self.iter_seq, 0);
    }

    #[inline(always)]
    pub fn end_iteration_with_budget(&self, budget_cycles: u64) {
        self.tracer
            .record_iter_end(self.thread, self.iter_seq, budget_cycles);
    }
}

#[derive(Clone, Copy, Default)]
struct ThreadTrackState {
    active: bool,
    iter_seq: u8,
    iter_start_tsc: u64,
    last_site: MarkSite,
    last_tsc: u64,
}

fn emit_finding(
    shared: &SharedSinkState,
    callback: &mut Option<FindingCallback>,
    kind: FindingKind,
) {
    let seq = shared.findings_count.fetch_add(1, Ordering::AcqRel) + 1;
    let finding = LatencyFinding { seq, kind };
    if let Some(cb) = callback.as_mut() {
        cb(&finding);
    }
    let mut guard = shared.retained.lock().unwrap_or_else(|e| e.into_inner());
    if guard.len() >= shared.max_retained {
        let _ = guard.pop_front();
    }
    guard.push_back(finding);
}

fn run_sink_loop(
    shared: Arc<SharedSinkState>,
    calibration: TscCalibration,
    poll_interval: Duration,
    mut callback: Option<FindingCallback>,
) {
    let mut read_pos: u64 = 0;
    let mut processed_records: u64 = 0;
    let mut prev_dropped: u64 = 0;
    let mut per_thread = [ThreadTrackState::default(); 256];

    loop {
        let mut progressed = false;
        loop {
            let slot = &shared.ring.slots[(read_pos as usize) & shared.ring.mask];
            let seq = slot.seq.load(Ordering::Acquire);
            let expected = read_pos.wrapping_add(1);
            if seq == expected {
                let meta = slot.meta.load(Ordering::Relaxed);
                let tsc_or_wait = slot.tsc_or_wait.load(Ordering::Relaxed);
                let budget_cycles = slot.budget_cycles.load(Ordering::Relaxed);
                slot.seq.store(
                    read_pos.wrapping_add((shared.ring.mask as u64) + 1),
                    Ordering::Release,
                );
                read_pos = read_pos.wrapping_add(1);
                processed_records = processed_records.saturating_add(1);
                shared
                    .processed_pos
                    .store(processed_records, Ordering::Release);
                progressed = true;

                let (kind_opt, thread, holder, iter_seq, site_or_lock) = unpack_meta(meta);
                let Some(kind) = kind_opt else {
                    continue;
                };
                match kind {
                    RecordKind::IterBegin => {
                        per_thread[usize::from(thread.0)] = ThreadTrackState {
                            active: true,
                            iter_seq,
                            iter_start_tsc: tsc_or_wait,
                            last_site: MarkSite::ITER_BEGIN,
                            last_tsc: tsc_or_wait,
                        };
                    }
                    RecordKind::StageMark => {
                        let site = MarkSite(site_or_lock as u16);
                        let st = &mut per_thread[usize::from(thread.0)];
                        if st.active && st.iter_seq == iter_seq && tsc_or_wait >= st.last_tsc {
                            let delta = tsc_or_wait - st.last_tsc;
                            if budget_cycles > 0 && delta >= budget_cycles {
                                emit_finding(
                                    &shared,
                                    &mut callback,
                                    FindingKind::StageBudgetExceeded {
                                        thread,
                                        from_site: st.last_site,
                                        to_site: site,
                                        delta_cycles: delta,
                                        delta_ns: calibration.cycles_to_ns(delta),
                                        budget_cycles,
                                    },
                                );
                            }
                            st.last_site = site;
                            st.last_tsc = tsc_or_wait;
                        } else {
                            *st = ThreadTrackState {
                                active: true,
                                iter_seq,
                                iter_start_tsc: tsc_or_wait,
                                last_site: site,
                                last_tsc: tsc_or_wait,
                            };
                        }
                    }
                    RecordKind::IterEnd => {
                        let st = &mut per_thread[usize::from(thread.0)];
                        if st.active && st.iter_seq == iter_seq && tsc_or_wait >= st.iter_start_tsc
                        {
                            let total = tsc_or_wait - st.iter_start_tsc;
                            if budget_cycles > 0 && total >= budget_cycles {
                                emit_finding(
                                    &shared,
                                    &mut callback,
                                    FindingKind::IterationSpike {
                                        thread,
                                        total_cycles: total,
                                        total_ns: calibration.cycles_to_ns(total),
                                        budget_cycles,
                                    },
                                );
                            }
                        }
                        st.active = false;
                    }
                    RecordKind::LockWait => {
                        let lock_id = LockId(site_or_lock);
                        if tsc_or_wait >= budget_cycles {
                            emit_finding(
                                &shared,
                                &mut callback,
                                FindingKind::LockContention {
                                    waiter: thread,
                                    holder,
                                    lock_id,
                                    wait_cycles: tsc_or_wait,
                                    wait_ns: calibration.cycles_to_ns(tsc_or_wait),
                                    threshold_cycles: budget_cycles,
                                },
                            );
                        }
                    }
                }
            } else {
                let write_head = shared.ring.write_pos.load(Ordering::Acquire);
                if write_head > read_pos {
                    // Check whether a producer is mid-store on `read_pos` or `read_pos` was skipped.
                    std::hint::spin_loop();
                }
                break;
            }
        }

        let total_dropped = shared.ring.dropped.load(Ordering::Acquire);
        if total_dropped > prev_dropped {
            let delta = total_dropped - prev_dropped;
            prev_dropped = total_dropped;
            emit_finding(
                &shared,
                &mut callback,
                FindingKind::QueueWrap {
                    dropped_delta: delta,
                    total_dropped,
                },
            );
        }

        if shared.stopped.load(Ordering::Acquire) && !progressed {
            break;
        }
        if !progressed {
            let guard = shared.wake_mu.lock().unwrap_or_else(|e| e.into_inner());
            if !shared.stopped.load(Ordering::Acquire) {
                let _ = shared.wake_cv.wait_timeout(guard, poll_interval);
            }
        }
    }
}

/// Pure userspace fine-grained spinlock (`try_lock` spin loop; zero kernel futex syscalls) with
/// hardware `rdtsc` slow-path `LockWait` attribution when contended.
pub struct TimedSpinLock<T> {
    inner: Mutex<T>,
    holder: AtomicU8,
    lock_id: LockId,
    threshold_cycles: u64,
    tracer: Option<Arc<LatencyTracer>>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for TimedSpinLock<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimedSpinLock")
            .field("lock_id", &self.lock_id)
            .field("holder", &ThreadTag(self.holder.load(Ordering::Acquire)))
            .finish_non_exhaustive()
    }
}

impl<T> TimedSpinLock<T> {
    pub fn new(value: T, lock_id: LockId) -> Self {
        Self {
            inner: Mutex::new(value),
            holder: AtomicU8::new(ThreadTag::UNKNOWN.0),
            lock_id,
            threshold_cycles: 0,
            tracer: None,
        }
    }

    pub fn with_tracer(
        value: T,
        lock_id: LockId,
        tracer: Option<Arc<LatencyTracer>>,
        threshold_cycles: Option<u64>,
    ) -> Self {
        let thresh = threshold_cycles.unwrap_or_else(|| {
            tracer
                .as_ref()
                .map_or(1_000, |t| t.default_lock_threshold_cycles())
        });
        Self {
            inner: Mutex::new(value),
            holder: AtomicU8::new(ThreadTag::UNKNOWN.0),
            lock_id,
            threshold_cycles: thresh,
            tracer,
        }
    }

    #[inline]
    pub const fn lock_id(&self) -> LockId {
        self.lock_id
    }

    #[inline(always)]
    pub fn try_lock(&self, caller: ThreadTag) -> Option<TimedSpinGuard<'_, T>> {
        match self.inner.try_lock() {
            Ok(guard) => {
                self.holder.store(caller.0, Ordering::Release);
                Some(TimedSpinGuard {
                    holder: &self.holder,
                    guard: Some(guard),
                })
            }
            Err(TryLockError::Poisoned(poisoned)) => {
                let guard = poisoned.into_inner();
                self.holder.store(caller.0, Ordering::Release);
                Some(TimedSpinGuard {
                    holder: &self.holder,
                    guard: Some(guard),
                })
            }
            Err(TryLockError::WouldBlock) => None,
        }
    }

    /// Acquires the spinlock in userspace via `try_lock()`, measuring contended `rdtsc()` wait
    /// cycles only on the slow path (`WouldBlock`).
    #[inline(always)]
    pub fn lock(&self, caller: ThreadTag) -> TimedSpinGuard<'_, T> {
        if let Some(fast) = self.try_lock(caller) {
            return fast;
        }
        self.lock_contended(caller)
    }

    #[cold]
    #[inline(never)]
    fn lock_contended(&self, caller: ThreadTag) -> TimedSpinGuard<'_, T> {
        let start_tsc = rdtsc();
        let mut observed_holder = ThreadTag(self.holder.load(Ordering::Acquire));
        loop {
            core::hint::spin_loop();
            if observed_holder == ThreadTag::UNKNOWN {
                observed_holder = ThreadTag(self.holder.load(Ordering::Relaxed));
            }
            if let Some(guard) = self.try_lock(caller) {
                let wait_cycles = rdtsc().wrapping_sub(start_tsc);
                if wait_cycles >= self.threshold_cycles
                    && let Some(tracer) = self.tracer.as_ref()
                {
                    tracer.record_lock_wait(
                        caller,
                        observed_holder,
                        self.lock_id,
                        wait_cycles,
                        self.threshold_cycles,
                    );
                }
                return guard;
            }
        }
    }
}

pub struct TimedSpinGuard<'a, T> {
    holder: &'a AtomicU8,
    guard: Option<MutexGuard<'a, T>>,
}

impl<T> Deref for TimedSpinGuard<'_, T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().expect("TimedSpinGuard active")
    }
}

impl<T> DerefMut for TimedSpinGuard<'_, T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.as_mut().expect("TimedSpinGuard active")
    }
}

impl<T> Drop for TimedSpinGuard<'_, T> {
    #[inline(always)]
    fn drop(&mut self) {
        self.holder.store(ThreadTag::UNKNOWN.0, Ordering::Release);
        drop(self.guard.take());
    }
}
