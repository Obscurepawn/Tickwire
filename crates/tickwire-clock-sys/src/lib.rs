//! Suspend-aware native monotonic time, bound to one externally assigned domain, plus audited
//! inline hardware cycle-counter (`rdtsc`, `rdtsc_serialized`) and integer `TscCalibration`
//! helpers for hot-path latency tracing.
#![deny(unsafe_op_in_unsafe_fn)]
use std::{
    io,
    time::{Duration, Instant},
};
use tickwire_clock::{ClockDomain, MonoTime};

#[derive(Debug)]
pub enum Error {
    Unsupported,
    InvalidTime,
    Overflow,
    Backwards,
    Faulted,
    System(io::Error),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug)]
pub struct SystemSample {
    pub instant: Instant,
    pub monotonic: MonoTime,
}
#[derive(Default)]
struct Guard {
    last: Option<(Instant, u64)>,
    faulted: bool,
}
impl Guard {
    fn observe(&mut self, instant: Instant, nanos: u64) -> Result<()> {
        if self.faulted {
            return Err(Error::Faulted);
        }
        if self
            .last
            .is_some_and(|(prior, ns)| instant < prior || nanos < ns)
        {
            self.fail();
            return Err(Error::Backwards);
        }
        self.last = Some((instant, nanos));
        Ok(())
    }
    fn fail(&mut self) {
        self.faulted = true;
    }
}
pub struct SystemClock {
    domain: ClockDomain,
    guard: Guard,
}
impl SystemClock {
    pub fn new(domain: ClockDomain) -> Result<Self> {
        let mut clock = Self {
            domain,
            guard: Guard::default(),
        };
        clock.sample()?;
        Ok(clock)
    }
    pub fn domain(&self) -> ClockDomain {
        self.domain
    }
    pub fn sample(&mut self) -> Result<SystemSample> {
        if self.guard.faulted {
            return Err(Error::Faulted);
        }
        let instant = Instant::now();
        let nanos = match native_nanos() {
            Ok(nanos) => nanos,
            Err(error) => {
                self.guard.fail();
                return Err(error);
            }
        };
        self.guard.observe(instant, nanos)?;
        Ok(SystemSample {
            instant,
            monotonic: MonoTime::new(self.domain, nanos),
        })
    }
}
fn decode_timespec(seconds: i128, nanos: i128) -> Result<u64> {
    if seconds < 0 || !(0..1_000_000_000).contains(&nanos) {
        return Err(Error::InvalidTime);
    }
    u64::try_from(seconds)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1_000_000_000))
        .and_then(|whole| whole.checked_add(nanos as u64))
        .ok_or(Error::Overflow)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn native_nanos() -> Result<u64> {
    #[cfg(target_os = "linux")]
    let clock_id = libc::CLOCK_BOOTTIME;
    #[cfg(target_os = "macos")]
    let clock_id = libc::CLOCK_MONOTONIC_RAW;
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: time is an aligned, initialized, writable timespec for the entire
    // call; clock_gettime does not retain the pointer. Both IDs are native clocks.
    if unsafe { libc::clock_gettime(clock_id, &mut time) } != 0 {
        return Err(Error::System(io::Error::last_os_error()));
    }
    decode_timespec(time.tv_sec as i128, time.tv_nsec as i128)
}
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn native_nanos() -> Result<u64> {
    Err(Error::Unsupported)
}

/// Reads the hardware cycle counter with minimal overhead (`_rdtsc` on `x86_64`, `cntvct_el0` on
/// `aarch64`, or `Instant`-anchored monotonic nanoseconds on other targets).
#[inline(always)]
pub fn rdtsc() -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: `_rdtsc` reads the architectural 64-bit time-stamp counter register without
        // touching memory or processor flags and is baseline on all `x86_64` CPUs.
        unsafe { core::arch::x86_64::_rdtsc() }
    }
    #[cfg(target_arch = "aarch64")]
    {
        let cnt: u64;
        // SAFETY: Reading the virtual timer count register `cntvct_el0` is unprivileged (`EL0`)
        // in AArch64 Linux/macOS userspace, accesses no memory, and preserves flags.
        unsafe {
            core::arch::asm!(
                "mrs {cnt}, cntvct_el0",
                cnt = out(reg) cnt,
                options(nomem, nostack, preserves_flags),
            );
        }
        cnt
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        static ANCHOR: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        let start = ANCHOR.get_or_init(Instant::now);
        u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

/// Reads the hardware cycle counter after a dispatch-serializing barrier (`_mm_lfence` + `_rdtsc`
/// on `x86_64`, `isb` + `cntvct_el0` on `aarch64`).
#[inline(always)]
pub fn rdtsc_serialized() -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: `_mm_lfence` (SSE2 baseline on `x86_64`) serializes prior instruction dispatch
        // before `_rdtsc` reads the 64-bit time-stamp counter; neither accesses memory.
        unsafe {
            core::arch::x86_64::_mm_lfence();
            core::arch::x86_64::_rdtsc()
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        let cnt: u64;
        // SAFETY: `isb` flushes the instruction pipeline so `mrs cntvct_el0` executes in order
        // without touching memory or flags.
        unsafe {
            core::arch::asm!(
                "isb",
                "mrs {cnt}, cntvct_el0",
                cnt = out(reg) cnt,
                options(nomem, nostack, preserves_flags),
            );
        }
        cnt
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        rdtsc()
    }
}

/// Exact integer conversion factor between [`rdtsc`] cycles and nanoseconds/microseconds.
///
/// Stores `cycles_per_ms` (`>= 1_000`, i.e. cycles per `1_000_000 ns`) so conversions preserve
/// sub-MHz precision in `u128` integer arithmetic with zero floating-point operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TscCalibration {
    cycles_per_ms: u64,
}

impl TscCalibration {
    const NS_PER_MS: u128 = 1_000_000;
    const MIN_CYCLES_PER_MS: u64 = 1_000;

    /// Constructs a [`TscCalibration`] from an explicit `cycles_per_ms` (`>= 1_000`).
    #[inline]
    pub const fn from_cycles_per_ms(cycles_per_ms: u64) -> Option<Self> {
        if cycles_per_ms < Self::MIN_CYCLES_PER_MS {
            None
        } else {
            Some(Self { cycles_per_ms })
        }
    }

    /// Constructs a [`TscCalibration`] from an integer `cycles_per_us` (`>= 1`).
    #[inline]
    pub const fn from_cycles_per_us(cycles_per_us: u64) -> Option<Self> {
        let Some(cycles_per_ms) = cycles_per_us.checked_mul(1_000) else {
            return None;
        };
        Self::from_cycles_per_ms(cycles_per_ms)
    }

    /// Calibrates the hardware cycle rate against `Instant` over `window` (clamped to `>= 1ms`).
    ///
    /// Intended for cold startup before spawning hot threads or opening `LatencyTracer` sinks.
    pub fn calibrate(window: Duration) -> Self {
        let target = window.max(Duration::from_millis(1));
        let t0 = Instant::now();
        let c0 = rdtsc_serialized();
        std::thread::sleep(target);
        let c1 = rdtsc_serialized();
        let elapsed_ns = t0.elapsed().as_nanos().max(1);
        let delta_cycles = u128::from(c1.saturating_sub(c0));
        let raw_per_ms = (delta_cycles.saturating_mul(Self::NS_PER_MS)) / elapsed_ns;
        let clamped = u64::try_from(raw_per_ms)
            .unwrap_or(u64::MAX)
            .max(Self::MIN_CYCLES_PER_MS);
        Self {
            cycles_per_ms: clamped,
        }
    }

    /// Calibrates using a 2ms cold-startup window.
    pub fn calibrate_default() -> Self {
        Self::calibrate(Duration::from_millis(2))
    }

    /// Calibrated cycles per millisecond (`1_000_000 ns`).
    #[inline]
    pub const fn cycles_per_ms(self) -> u64 {
        self.cycles_per_ms
    }

    /// Calibrated cycles per microsecond (`>= 1`).
    #[inline]
    pub const fn cycles_per_us(self) -> u64 {
        let per_us = self.cycles_per_ms / 1_000;
        if per_us == 0 { 1 } else { per_us }
    }

    /// Converts nanoseconds into cycle threshold units using exact integer floor division.
    #[inline]
    pub const fn ns_to_cycles(self, ns: u64) -> u64 {
        let prod = (ns as u128) * (self.cycles_per_ms as u128);
        let cycles = prod / Self::NS_PER_MS;
        if cycles > u64::MAX as u128 {
            u64::MAX
        } else {
            cycles as u64
        }
    }

    /// Converts microseconds into cycle threshold units using exact integer floor division.
    #[inline]
    pub const fn us_to_cycles(self, us: u64) -> u64 {
        self.ns_to_cycles(us.saturating_mul(1_000))
    }

    /// Converts elapsed cycles into nanoseconds using exact integer floor division.
    #[inline]
    pub const fn cycles_to_ns(self, cycles: u64) -> u64 {
        let prod = (cycles as u128) * Self::NS_PER_MS;
        let ns = prod / (self.cycles_per_ms as u128);
        if ns > u64::MAX as u128 {
            u64::MAX
        } else {
            ns as u64
        }
    }

    /// Converts elapsed cycles into microseconds using exact integer floor division.
    #[inline]
    pub const fn cycles_to_us(self, cycles: u64) -> u64 {
        self.cycles_to_ns(cycles) / 1_000
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn timespec_keeps_nanoseconds_and_checks_maximum_exactly() {
        assert_eq!(decode_timespec(12, 345).unwrap(), 12_000_000_345);
        assert_eq!(
            decode_timespec(
                (u64::MAX / 1_000_000_000) as i128,
                (u64::MAX % 1_000_000_000) as i128
            )
            .unwrap(),
            u64::MAX
        );
        assert!(matches!(
            decode_timespec(
                (u64::MAX / 1_000_000_000) as i128,
                (u64::MAX % 1_000_000_000) as i128 + 1
            ),
            Err(Error::Overflow)
        ));
    }
    #[test]
    fn invalid_timespec_fields_are_never_cast_to_unsigned() {
        for (s, n) in [(-1, 0), (0, -1), (0, 1_000_000_000)] {
            assert!(matches!(decode_timespec(s, n), Err(Error::InvalidTime)));
        }
        assert!(matches!(
            decode_timespec(i128::MAX, 0),
            Err(Error::Overflow)
        ));
    }
    #[test]
    fn same_tick_is_valid_but_monotonic_rollback_is_permanent() {
        let now = Instant::now();
        let mut guard = Guard::default();
        guard.observe(now, 10).unwrap();
        guard.observe(now, 10).unwrap();
        assert!(matches!(guard.observe(now, 9), Err(Error::Backwards)));
        assert!(matches!(guard.observe(now, 20), Err(Error::Faulted)));
    }
    #[test]
    fn instant_rollback_is_permanent_even_when_native_time_advances() {
        let now = Instant::now();
        let mut guard = Guard::default();
        guard.observe(now + Duration::from_millis(1), 10).unwrap();
        assert!(matches!(guard.observe(now, 11), Err(Error::Backwards)));
        assert!(matches!(
            guard.observe(now + Duration::from_secs(1), 12),
            Err(Error::Faulted)
        ));
    }
    #[test]
    fn native_error_cannot_be_cleared_by_a_later_success() {
        let mut guard = Guard::default();
        guard.fail();
        assert!(matches!(
            guard.observe(Instant::now(), 10),
            Err(Error::Faulted)
        ));
    }
}
