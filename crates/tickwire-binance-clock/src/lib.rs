//! One authenticated HTTPS connection per Binance clock calibration sample.
#![forbid(unsafe_code)]
use std::{net::SocketAddr, sync::Arc, time::Instant};
pub use tickwire_clock::{
    CalibrationConfig, CalibrationStatus, ClockDomain, MonoTime, Reading, SignedTime, Source,
    TimeUnit,
};
pub use tickwire_clock_sys::SystemClock;
pub use tickwire_transport::{IoBudget, Limits, mio, rustls};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Busy,
    Denied,
    Exhausted,
    Clock(tickwire_clock::Error),
    Transport,
    Protocol,
    HttpStatus(u16),
    Timeout,
    Closed,
    Budget,
    Unsupported,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Copy, Debug)]
pub struct LocalTime {
    pub instant: Instant,
    pub monotonic: MonoTime,
}
pub trait MonotonicClock {
    fn sample(&mut self) -> Result<LocalTime>;
}
impl MonotonicClock for SystemClock {
    fn sample(&mut self) -> Result<LocalTime> {
        let sampled = SystemClock::sample(self).map_err(|error| {
            Error::Clock(match error {
                tickwire_clock_sys::Error::Backwards => tickwire_clock::Error::TimeBackwards,
                tickwire_clock_sys::Error::Overflow => tickwire_clock::Error::Overflow,
                _ => tickwire_clock::Error::Faulted,
            })
        })?;
        Ok(LocalTime {
            instant: sampled.instant,
            monotonic: sampled.monotonic,
        })
    }
}
#[derive(Clone, Copy, Debug)]
pub struct ClockReading {
    pub local: LocalTime,
    pub signed: Option<SignedTime>,
}
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub source: Source,
    pub domain: ClockDomain,
    pub calibration: CalibrationConfig,
    pub unit: TimeUnit,
    pub first_sample_serial: u64,
    pub first_connection_epoch: u64,
    pub connect_timeout_ns: u64,
    pub request_timeout_ns: u64,
    pub max_response_bytes: usize,
    pub max_headers: usize,
    pub io: Limits,
}
pub struct NetworkConfig {
    pub address: SocketAddr,
    pub tls: Arc<rustls::ClientConfig>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChargeKind {
    Connect,
    Query,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeCharge {
    pub source: Source,
    pub domain: ClockDomain,
    pub connection_epoch: u64,
    pub kind: ChargeKind,
    pub api_weight: u32,
}
pub trait TimePermit {
    /// Recheck current permission on every call. Charge the same
    /// (source, domain, epoch, kind) at most once, even after a zero-byte attempt.
    fn allow(&mut self, charge: &TimeCharge, now: &LocalTime) -> bool;
}
impl<F: FnMut(&TimeCharge, &LocalTime) -> bool> TimePermit for F {
    fn allow(&mut self, charge: &TimeCharge, now: &LocalTime) -> bool {
        self(charge, now)
    }
}
#[derive(Debug)]
pub struct ResponseMetadata {
    pub status: u16,
    pub headers: Vec<(String, Vec<u8>)>,
}
#[derive(Debug)]
pub struct Outcome {
    pub connection_epoch: u64,
    pub http: Option<ResponseMetadata>,
    pub calibration: Result<Reading>,
}
#[derive(Debug, Default)]
pub struct PollReport {
    /// A failed drive may have consumed unreported IO; no more IO occurs this poll.
    pub io_budget_exhausted: bool,
    pub syscalls: usize,
    pub network_bytes: usize,
    pub outcome: Option<Outcome>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SampleProgress {
    pub connection_epoch: u64,
    pub measurement_started: bool,
    pub accepted_application_bytes: usize,
}
mod client;
pub use client::BinanceTimeClient;
