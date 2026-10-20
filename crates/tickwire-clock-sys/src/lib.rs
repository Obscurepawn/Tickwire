//! Suspend-aware native monotonic time, bound to one externally assigned domain.
#![deny(unsafe_op_in_unsafe_fn)]
use std::{io, time::Instant};
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
