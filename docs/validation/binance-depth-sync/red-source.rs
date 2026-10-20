//! Single-owner snapshot/delta coordination for ordinary Binance JSON depth.
use crate::{Error, feed::{DepthContext, TimeUnit}, request::Market};
use tickwire_book::{BinanceSpot, BinanceUsdM, Book, BookKey, BookView, SequencePolicy, ViewError};
use tickwire_venue::InstrumentBinding;

mod sealed {
    pub trait Sealed {}
    impl Sealed for tickwire_book::BinanceSpot {}
    impl Sealed for tickwire_book::BinanceUsdM {}
}
pub trait DepthPolicy: SequencePolicy + sealed::Sealed {
    const MARKET: Market;
}
impl DepthPolicy for BinanceSpot { const MARKET: Market = Market::Spot; }
impl DepthPolicy for BinanceUsdM { const MARKET: Market = Market::Usdm; }

#[derive(Debug, Clone)]
pub struct Config {
    pub instrument: InstrumentBinding,
    pub key: BookKey,
    pub time_unit: TimeUnit,
    pub snapshot_limit: usize,
    pub max_message_bytes: usize,
    pub initial_book_generation: u64,
    pub previous_connection_epoch: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionToken { key: BookKey, epoch: u64 }
impl ConnectionToken { pub fn epoch(self) -> u64 { self.epoch } }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotToken { connection: ConnectionToken, generation: u64, request: u64 }
#[derive(Debug)]
pub struct SnapshotRequest { token: SnapshotToken, path: String, deadline: u64 }
impl SnapshotRequest {
    pub fn token(&self) -> SnapshotToken { self.token }
    pub fn path(&self) -> &str { &self.path }
    pub fn deadline_ns(&self) -> u64 { self.deadline }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State { Disconnected, Buffering, Bridging, Live, Exhausted }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncError { InvalidConfig, InvalidState, EpochNotIncreasing, RequestPending, InvalidDeadline, Exhausted, Codec(Error) }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryReason { TooOld, Http(u16), Transport, Expired }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebuildReason { BufferOverflow, Decode(Error), Book(tickwire_book::BookError), ClockRegressed }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    IgnoredObsolete,
    Buffered { events: usize },
    Applied(tickwire_book::ApplyOutcome),
    SnapshotInstalled { applied: usize, discarded: usize, live: bool },
    RetrySnapshot(RetryReason),
    Rebuild(RebuildReason),
    Disconnected,
}
pub struct DepthSync<P, const LEVELS: usize, const UPDATES: usize, const BUFFER: usize> {
    book: Book<P, LEVELS, UPDATES>,
}
impl<P: DepthPolicy, const L: usize, const U: usize, const B: usize> DepthSync<P, L, U, B> {
    pub fn new(config: Config) -> Result<Self, SyncError> {
        let _ = DepthContext::new(P::MARKET, config.instrument, config.key, config.initial_book_generation, config.time_unit).map_err(SyncError::Codec)?;
        Ok(Self { book: Book::new(config.key, config.initial_book_generation) })
    }
    pub fn connect(&mut self, _epoch: u64) -> Result<ConnectionToken, SyncError> { Err(SyncError::InvalidState) }
    pub fn subscription_ready(&mut self, _token: ConnectionToken) -> Result<bool, SyncError> { Err(SyncError::InvalidState) }
    pub fn request_snapshot(&mut self, _now: u64, _deadline: u64) -> Result<SnapshotRequest, SyncError> { Err(SyncError::InvalidState) }
    pub fn on_delta(&mut self, _token: ConnectionToken, _bytes: &[u8], _received: u64) -> Result<Outcome, SyncError> { Err(SyncError::InvalidState) }
    pub fn on_snapshot(&mut self, _token: SnapshotToken, _status: u16, _bytes: &[u8], _now: u64) -> Result<Outcome, SyncError> { Err(SyncError::InvalidState) }
    pub fn snapshot_failed(&mut self, _token: SnapshotToken, _now: u64) -> Result<Outcome, SyncError> { Err(SyncError::InvalidState) }
    pub fn expire_snapshot(&mut self, _now: u64) -> Result<Option<Outcome>, SyncError> { Err(SyncError::InvalidState) }
    pub fn disconnect(&mut self, _token: ConnectionToken) -> Result<Outcome, SyncError> { Err(SyncError::InvalidState) }
    pub fn state(&self) -> State { State::Disconnected }
    pub fn buffered(&self) -> usize { 0 }
    pub fn generation(&self) -> u64 { self.book.generation() }
    pub fn last_update_id(&self) -> Option<u64> { self.book.last_update_id() }
    pub fn view(&self, now: u64, maximum_age: u64) -> Result<BookView<'_>, ViewError> { self.book.view(now, maximum_age) }
}
