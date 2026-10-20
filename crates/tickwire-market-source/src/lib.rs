//! Authenticated normalized market streams, independent of runtime and strategy code.
#![deny(unsafe_code)]
use tickwire_clock::{ClockDomain, MonoTime};
pub mod protocol;
pub mod replica;
pub mod retirement;
#[allow(unsafe_code)]
pub mod shm;
pub use shm::{Client, ConnectConfig, ShmSource};
#[derive(Debug)]
pub enum Error {
    Wire(tickwire_ipc::Error),
    Join(tickwire_join::Error),
    Ring(tickwire_shm::MappedError),
    Control(tickwire_control::Error),
    Transport(tickwire_control_sys::Error),
    Io(std::io::Error),
    Json(serde_json::Error),
    Identity,
    Bounds,
    Stale,
    NotReady,
    Deadline,
    Sink,
}
pub type Result<T> = std::result::Result<T, Error>;
macro_rules! from {
    ($t:ty,$v:ident) => {
        impl From<$t> for Error {
            fn from(e: $t) -> Self {
                Self::$v(e)
            }
        }
    };
}
from!(tickwire_ipc::Error, Wire);
from!(tickwire_join::Error, Join);
from!(tickwire_shm::MappedError, Ring);
from!(tickwire_control::Error, Control);
from!(tickwire_control_sys::Error, Transport);
from!(std::io::Error, Io);
from!(serde_json::Error, Json);
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeedKey {
    pub topic_id: u64,
    pub instrument_id: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeedObservation {
    pub key: FeedKey,
    pub instrument_version: u64,
    pub feed_generation: u64,
    pub ring_epoch: u64,
    pub sequence: u64,
    pub published_at: MonoTime,
    pub received_at: MonoTime,
    pub live: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Invalidation {
    pub key: FeedKey,
    pub feed_generation: u64,
    pub ring_epoch: u64,
}
pub trait MarketSink {
    fn observed(&mut self, observation: FeedObservation) -> bool;
    fn invalidated(&mut self, invalid: Invalidation) -> bool;
}
#[derive(Clone, Copy, Debug, Default)]
pub struct PollReport {
    pub records: usize,
    pub runnable: bool,
    pub next_deadline_ns: Option<u64>,
}
/// Each implementation owns its registered cursor and authenticated control lease.
/// `stop` must invalidate downstream feeds before discarding the source.
pub trait MarketSource {
    fn domain(&self) -> ClockDomain;
    fn poll(
        &mut self,
        now: MonoTime,
        maximum_age_ns: u64,
        budget: usize,
        sink: &mut dyn MarketSink,
    ) -> Result<PollReport>;
    fn is_live(&self, now: MonoTime, maximum_age_ns: u64) -> bool;
    fn stop(&mut self, sink: &mut dyn MarketSink) -> Result<()>;
}
