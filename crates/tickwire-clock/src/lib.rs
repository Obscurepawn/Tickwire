//! Exchange-time intervals mapped onto one suspend-aware monotonic clock domain.
#![forbid(unsafe_code)]

pub const PPB: u64 = 1_000_000_000;
pub const MILLISECOND: u64 = 1_000_000;
pub const MICROSECOND: u64 = 1_000;
pub const SECOND: u64 = 1_000_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Busy,
    Stale,
    Domain,
    TimeBackwards,
    Overflow,
    Uncalibrated,
    Expired,
    Uncertain,
    RoundTrip,
    ClockJump,
    Faulted,
    Future,
    TooOld,
    Unsupported,
}
pub type Result<T> = core::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    SpotProduction,
    SpotTest,
    UsdmProduction,
    UsdmTest,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeUnit {
    Milliseconds,
    Microseconds,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClockDomain {
    boot_id: [u8; 16],
    generation: u64,
}
impl ClockDomain {
    pub fn new(boot_id: [u8; 16], generation: u64) -> Result<Self> {
        if boot_id == [0; 16] || generation == 0 {
            return Err(Error::Invalid);
        }
        Ok(Self {
            boot_id,
            generation,
        })
    }
    pub fn boot_id(self) -> [u8; 16] {
        self.boot_id
    }
    pub fn generation(self) -> u64 {
        self.generation
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MonoTime {
    domain: ClockDomain,
    ns: u64,
}
impl MonoTime {
    pub fn new(domain: ClockDomain, ns: u64) -> Self {
        Self { domain, ns }
    }
    pub fn nanos(self) -> u64 {
        self.ns
    }
    pub fn domain(self) -> ClockDomain {
        self.domain
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CalibrationConfig {
    pub max_round_trip_ns: u64,
    pub max_width_ns: u64,
    pub max_age_ns: u64,
    pub quantization_error_ns: u64,
    pub drift_ppb: u64,
    pub max_adjustment_ns: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SampleToken {
    domain: ClockDomain,
    source: Source,
    serial: u64,
    sent_ns: u64,
}
impl SampleToken {
    pub fn serial(&self) -> u64 {
        self.serial
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reading {
    lower_ns: u64,
    upper_ns: u64,
    sampled_at_ns: u64,
    valid_until_ns: u64,
    sample_serial: u64,
}
impl Reading {
    pub fn lower_ns(&self) -> u64 {
        self.lower_ns
    }
    pub fn upper_ns(&self) -> u64 {
        self.upper_ns
    }
    pub fn sampled_at_ns(&self) -> u64 {
        self.sampled_at_ns
    }
    pub fn valid_until_ns(&self) -> u64 {
        self.valid_until_ns
    }
    pub fn sample_serial(&self) -> u64 {
        self.sample_serial
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SignedTime {
    domain: ClockDomain,
    source: Source,
    timestamp: u64,
    unit: TimeUnit,
    sample_serial: u64,
}
impl SignedTime {
    pub fn domain(&self) -> ClockDomain {
        self.domain
    }
    pub fn source(&self) -> Source {
        self.source
    }
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
    pub fn unit(&self) -> TimeUnit {
        self.unit
    }
    pub fn sample_serial(&self) -> u64 {
        self.sample_serial
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CalibrationStatus {
    Uncalibrated,
    Calibrated {
        sample_serial: u64,
        valid_until_ns: u64,
    },
    Faulted(Error),
}
/// A single owner for one product/environment and monotonic clock domain.
/// `status` reports stored calibration metadata; freshness is checked by `read`.
pub struct Calibrator {
    config: CalibrationConfig,
    source: Source,
    domain: ClockDomain,
    next_serial: Option<u64>,
    pending: Option<SampleToken>,
    anchor: Option<Reading>,
    last_seen: Option<u64>,
    fault: Option<Error>,
}
impl Calibrator {
    pub fn new(
        config: CalibrationConfig,
        source: Source,
        domain: ClockDomain,
        first_serial: u64,
    ) -> Result<Self> {
        if config.max_round_trip_ns == 0
            || config.max_width_ns == 0
            || config.max_age_ns == 0
            || config.quantization_error_ns < MILLISECOND
            || config.drift_ppb >= PPB
            || first_serial == 0
        {
            return Err(Error::Invalid);
        }
        Ok(Self {
            config,
            source,
            domain,
            next_serial: Some(first_serial),
            pending: None,
            anchor: None,
            last_seen: None,
            fault: None,
        })
    }

    fn fail<T>(&mut self, error: Error) -> Result<T> {
        self.fault = Some(error);
        self.pending = None;
        self.anchor = None;
        Err(error)
    }

    fn observe(&mut self, now: MonoTime) -> Result<()> {
        if self.fault.is_some() {
            return Err(Error::Faulted);
        }
        if now.domain != self.domain {
            return self.fail(Error::Domain);
        }
        if self.last_seen.is_some_and(|last| now.ns < last) {
            return self.fail(Error::TimeBackwards);
        }
        self.last_seen = Some(now.ns);
        Ok(())
    }

    pub fn begin(&mut self, now: MonoTime) -> Result<SampleToken> {
        self.observe(now)?;
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        let Some(serial) = self.next_serial else {
            return self.fail(Error::Overflow);
        };
        let token = SampleToken {
            domain: self.domain,
            source: self.source,
            serial,
            sent_ns: now.ns,
        };
        self.next_serial = serial.checked_add(1);
        self.pending = Some(token);
        Ok(token)
    }

    pub fn finish(
        &mut self,
        token: &SampleToken,
        received: MonoTime,
        server_time_ms: u64,
    ) -> Result<Reading> {
        // An obsolete reply cannot change clock state, even with malformed time.
        if self.pending.as_ref() != Some(token) {
            return Err(Error::Stale);
        }
        self.observe(received)?;
        self.pending = None;
        if server_time_ms == 0 {
            return Err(Error::Invalid);
        }
        let round_trip = received.ns - token.sent_ns;
        if round_trip > self.config.max_round_trip_ns {
            return Err(Error::RoundTrip);
        }
        let candidate = (|| {
            let server = server_time_ms.checked_mul(MILLISECOND)?;
            let upper = server
                .checked_add(self.config.quantization_error_ns)?
                .checked_add(scale_up(round_trip, self.config.drift_ppb)?)?;
            Some(Reading {
                lower_ns: server.saturating_sub(self.config.quantization_error_ns),
                upper_ns: upper,
                sampled_at_ns: received.ns,
                valid_until_ns: received.ns.checked_add(self.config.max_age_ns)?,
                sample_serial: token.serial,
            })
        })();
        let Some(candidate) = candidate else {
            return self.fail(Error::Overflow);
        };
        if candidate.upper_ns - candidate.lower_ns > self.config.max_width_ns {
            return Err(Error::Uncertain);
        }
        if let Some(old) = self.anchor.filter(|old| received.ns < old.valid_until_ns) {
            let Some(old) = project(old, received.ns, self.config.drift_ppb) else {
                return self.fail(Error::Overflow);
            };
            let gap = candidate
                .lower_ns
                .saturating_sub(old.upper_ns)
                .max(old.lower_ns.saturating_sub(candidate.upper_ns));
            if gap > self.config.max_adjustment_ns {
                return self.fail(Error::ClockJump);
            }
        }
        self.anchor = Some(candidate);
        Ok(candidate)
    }

    pub fn cancel(&mut self, token: &SampleToken) -> Result<()> {
        if self.pending.as_ref() != Some(token) {
            return Err(Error::Stale);
        }
        self.pending = None;
        Ok(())
    }

    pub fn read(&mut self, now: MonoTime) -> Result<Reading> {
        self.observe(now)?;
        let anchor = self.anchor.ok_or(Error::Uncalibrated)?;
        if now.ns >= anchor.valid_until_ns {
            return Err(Error::Expired);
        }
        let Some(reading) = project(anchor, now.ns, self.config.drift_ppb) else {
            return self.fail(Error::Overflow);
        };
        if reading.upper_ns - reading.lower_ns > self.config.max_width_ns {
            return Err(Error::Uncertain);
        }
        Ok(reading)
    }

    pub fn sign_time(&mut self, now: MonoTime, unit: TimeUnit) -> Result<SignedTime> {
        if unit == TimeUnit::Microseconds
            && matches!(self.source, Source::UsdmProduction | Source::UsdmTest)
        {
            return Err(Error::Invalid);
        }
        let reading = self.read(now)?;
        let midpoint = reading.lower_ns + (reading.upper_ns - reading.lower_ns) / 2;
        let timestamp = midpoint / unit.nanos();
        Ok(SignedTime {
            domain: self.domain,
            source: self.source,
            timestamp,
            unit,
            sample_serial: reading.sample_serial,
        })
    }

    pub fn check_send(
        &mut self,
        stamp: &SignedTime,
        now: MonoTime,
        recv_window_ns: u64,
        remaining_budget_ns: u64,
    ) -> Result<()> {
        if stamp.domain != self.domain || stamp.source != self.source {
            return Err(Error::Stale);
        }
        let precision = if matches!(self.source, Source::UsdmProduction | Source::UsdmTest) {
            MILLISECOND
        } else {
            MICROSECOND
        };
        if recv_window_ns == 0
            || recv_window_ns > 60 * SECOND
            || !recv_window_ns.is_multiple_of(precision)
        {
            return Err(Error::Invalid);
        }
        let reading = self.read(now)?;
        let Some(end) = now.ns.checked_add(remaining_budget_ns) else {
            return self.fail(Error::Overflow);
        };
        if end >= reading.valid_until_ns {
            return Err(Error::Expired);
        }
        // SignedTime is private-constructed, so multiplication recovers the exact
        // quantized timestamp used by the encoder, not a newly chosen midpoint.
        let timestamp = u128::from(stamp.timestamp) * u128::from(stamp.unit.nanos());
        if timestamp >= u128::from(reading.lower_ns) + u128::from(SECOND) {
            return Err(Error::Future);
        }
        let Some(upper_at_budget) = scale_up(remaining_budget_ns, self.config.drift_ppb)
            .and_then(|budget| reading.upper_ns.checked_add(budget))
        else {
            return self.fail(Error::Overflow);
        };
        // This comparison preserves signed mathematical subtraction when T > U.
        if u128::from(upper_at_budget) > timestamp + u128::from(recv_window_ns) {
            return Err(Error::TooOld);
        }
        Ok(())
    }

    pub fn status(&self) -> CalibrationStatus {
        if let Some(error) = self.fault {
            CalibrationStatus::Faulted(error)
        } else if let Some(anchor) = self.anchor {
            CalibrationStatus::Calibrated {
                sample_serial: anchor.sample_serial,
                valid_until_ns: anchor.valid_until_ns,
            }
        } else {
            CalibrationStatus::Uncalibrated
        }
    }
}
impl TimeUnit {
    fn nanos(self) -> u64 {
        match self {
            Self::Milliseconds => MILLISECOND,
            Self::Microseconds => MICROSECOND,
        }
    }
}
fn scale_up(value: u64, drift_ppb: u64) -> Option<u64> {
    let numerator = u128::from(value) * (u128::from(PPB) + u128::from(drift_ppb));
    u64::try_from(numerator.div_ceil(u128::from(PPB))).ok()
}
fn project(anchor: Reading, now: u64, drift_ppb: u64) -> Option<Reading> {
    let age = now.checked_sub(anchor.sampled_at_ns)?;
    let lower_advance = u128::from(age) * u128::from(PPB - drift_ppb) / u128::from(PPB);
    Some(Reading {
        lower_ns: anchor
            .lower_ns
            .checked_add(u64::try_from(lower_advance).ok()?)?,
        upper_ns: anchor.upper_ns.checked_add(scale_up(age, drift_ppb)?)?,
        ..anchor
    })
}
