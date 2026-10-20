//! Single-owner snapshot/delta coordination for ordinary Binance JSON depth.
use crate::{
    Error,
    feed::{self, DepthContext, DepthUpdate, TimeUnit},
    params::{Params, Value},
    request::Market,
};
use std::collections::VecDeque;
use tickwire_book::{
    ApplyOutcome, BinanceSpot, BinanceUsdM, Book, BookKey, BookState, BookView, InvalidReason,
    SequencePolicy, ViewError,
};
use tickwire_venue::InstrumentBinding;

mod sealed {
    pub trait Sealed {}
    impl Sealed for tickwire_book::BinanceSpot {}
    impl Sealed for tickwire_book::BinanceUsdM {}
}
pub trait DepthPolicy: SequencePolicy + sealed::Sealed {
    const MARKET: Market;
}
impl DepthPolicy for BinanceSpot {
    const MARKET: Market = Market::Spot;
}
impl DepthPolicy for BinanceUsdM {
    const MARKET: Market = Market::Usdm;
}

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
pub struct ConnectionToken {
    key: BookKey,
    epoch: u64,
}
impl ConnectionToken {
    pub fn epoch(self) -> u64 {
        self.epoch
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotToken {
    connection: ConnectionToken,
    generation: u64,
    request: u64,
}
#[derive(Debug)]
pub struct SnapshotRequest {
    token: SnapshotToken,
    path: String,
    deadline: u64,
}
impl SnapshotRequest {
    pub fn token(&self) -> SnapshotToken {
        self.token
    }
    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn deadline_ns(&self) -> u64 {
        self.deadline
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Disconnected,
    Buffering,
    Bridging,
    Live,
    Exhausted,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncError {
    InvalidConfig,
    InvalidState,
    EpochNotIncreasing,
    RequestPending,
    InvalidDeadline,
    Exhausted,
    Codec(Error),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryReason {
    TooOld,
    Http(u16),
    Transport,
    Expired,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebuildReason {
    BufferOverflow,
    Decode(Error),
    Book(tickwire_book::BookError),
    ClockRegressed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    IgnoredObsolete,
    Buffered {
        events: usize,
    },
    Applied(tickwire_book::ApplyOutcome),
    SnapshotInstalled {
        applied: usize,
        discarded: usize,
        live: bool,
    },
    RetrySnapshot(RetryReason),
    Rebuild(RebuildReason),
    Disconnected,
}
/// An owner must never reuse its connection epochs after replacing an instance.
pub struct DepthSync<P, const LEVELS: usize, const UPDATES: usize, const BUFFER: usize> {
    config: Config,
    context: DepthContext,
    // Allocate the fixed-capacity book once. Keeping its large arrays inline
    // multiplies constructor stack frames through the network owner's wrappers.
    book: Box<Book<P, LEVELS, UPDATES>>,
    buffer: VecDeque<DepthUpdate<UPDATES>>,
    state: State,
    connection: Option<ConnectionToken>,
    last_connection_epoch: u64,
    subscribed: bool,
    first_u: Option<u64>,
    pending: Option<(SnapshotToken, u64)>,
    request_id: u64,
    last_clock: Option<u64>,
    snapshot_path: String,
}
impl<P: DepthPolicy, const L: usize, const U: usize, const B: usize> DepthSync<P, L, U, B> {
    pub fn new(config: Config) -> Result<Self, SyncError> {
        if L == 0
            || L > feed::MAX_DEPTH_LEVELS
            || U == 0
            || U > feed::MAX_DEPTH_LEVELS
            || B == 0
            || B > 4096
            || std::mem::size_of::<DepthUpdate<U>>()
                .checked_mul(B)
                .is_none_or(|n| n > 64 * 1024 * 1024)
            || config.snapshot_limit == 0
            || config.snapshot_limit > L
            || config.max_message_bytes == 0
            || config.max_message_bytes > feed::MAX_MARKET_MESSAGE_BYTES
            || (P::MARKET == Market::Usdm
                && ![5, 10, 20, 50, 100, 500, 1000].contains(&config.snapshot_limit))
        {
            return Err(SyncError::InvalidConfig);
        }
        let context = DepthContext::new(
            P::MARKET,
            config.instrument.clone(),
            config.key,
            config.initial_book_generation,
            config.time_unit,
        )
        .map_err(SyncError::Codec)?;
        let mut params = Params::<2>::new();
        params
            .push("symbol", Value::String(config.instrument.symbol.as_str()))
            .map_err(SyncError::Codec)?;
        params
            .push("limit", Value::Integer(config.snapshot_limit as u64))
            .map_err(SyncError::Codec)?;
        let mut query = [0; 256];
        let n = params.rest(&mut query).map_err(SyncError::Codec)?;
        let prefix = match P::MARKET {
            Market::Spot => "/api/v3/depth?",
            Market::Usdm => "/fapi/v1/depth?",
        };
        let snapshot_path = format!(
            "{prefix}{}",
            std::str::from_utf8(&query[..n]).map_err(|_| SyncError::InvalidConfig)?
        );
        let mut buffer = VecDeque::new();
        buffer
            .try_reserve_exact(B)
            .map_err(|_| SyncError::InvalidConfig)?;
        Ok(Self {
            context,
            book: Box::new(Book::new(config.key, config.initial_book_generation)),
            buffer,
            state: State::Disconnected,
            connection: None,
            last_connection_epoch: config.previous_connection_epoch,
            subscribed: false,
            first_u: None,
            pending: None,
            request_id: 0,
            last_clock: None,
            snapshot_path,
            config,
        })
    }
    pub fn connect(&mut self, epoch: u64) -> Result<ConnectionToken, SyncError> {
        if self.state == State::Exhausted {
            return Err(SyncError::Exhausted);
        }
        if epoch <= self.last_connection_epoch {
            return Err(SyncError::EpochNotIncreasing);
        }
        self.reset_book()?;
        let token = ConnectionToken {
            key: self.config.key,
            epoch,
        };
        self.connection = Some(token);
        self.last_connection_epoch = epoch;
        self.subscribed = false;
        self.last_clock = None;
        Ok(token)
    }
    /// Call only after the exact stream's raw handshake or subscription ACK.
    pub fn subscription_ready(&mut self, token: ConnectionToken) -> Result<bool, SyncError> {
        if self.connection != Some(token) {
            return Ok(false);
        }
        self.subscribed = true;
        Ok(true)
    }
    pub fn request_snapshot(
        &mut self,
        now: u64,
        deadline: u64,
    ) -> Result<SnapshotRequest, SyncError> {
        if self.state == State::Exhausted {
            return Err(SyncError::Exhausted);
        }
        if self.state != State::Buffering || !self.subscribed || self.first_u.is_none() {
            return Err(SyncError::InvalidState);
        }
        if !self.observe_clock(now)? {
            return Err(SyncError::InvalidState);
        }
        if deadline <= now {
            return Err(SyncError::InvalidDeadline);
        }
        if self.pending.is_some() {
            return Err(SyncError::RequestPending);
        }
        let Some(request) = self.request_id.checked_add(1) else {
            return self.exhaust();
        };
        self.request_id = request;
        let token = SnapshotToken {
            connection: self.connection.expect("Buffering requires connection"),
            generation: self.book.generation(),
            request,
        };
        self.pending = Some((token, deadline));
        Ok(SnapshotRequest {
            token,
            path: self.snapshot_path.clone(),
            deadline,
        })
    }
    pub fn on_delta(
        &mut self,
        token: ConnectionToken,
        bytes: &[u8],
        received: u64,
    ) -> Result<Outcome, SyncError> {
        if self.connection != Some(token) {
            return Ok(Outcome::IgnoredObsolete);
        }
        if !self.observe_clock(received)? {
            return Ok(Outcome::Rebuild(RebuildReason::ClockRegressed));
        }
        let update = match feed::decode_diff::<U>(
            bytes,
            &self.context,
            received,
            self.config.max_message_bytes,
        ) {
            Ok(v) => v,
            Err(e) => return self.rebuild(RebuildReason::Decode(e)),
        };
        self.apply_update(update)
    }
    fn apply_update(&mut self, update: DepthUpdate<U>) -> Result<Outcome, SyncError> {
        let event = update.event();
        if event
            .bids
            .len()
            .checked_add(event.asks.len())
            .is_none_or(|n| n > U)
        {
            return self.rebuild(RebuildReason::Book(
                tickwire_book::BookError::TooManyUpdates,
            ));
        }
        if self.state == State::Buffering {
            if self.buffer.len() == B {
                return self.rebuild(RebuildReason::BufferOverflow);
            }
            self.first_u.get_or_insert(event.first_update_id);
            self.buffer.push_back(update);
            return Ok(Outcome::Buffered {
                events: self.buffer.len(),
            });
        }
        match self.book.apply(event) {
            Ok(outcome) => {
                self.state = if self.book.state() == BookState::Live {
                    State::Live
                } else {
                    State::Bridging
                };
                Ok(Outcome::Applied(outcome))
            }
            Err(e) => self.rebuild(RebuildReason::Book(e)),
        }
    }
    /// SBE data uses the same token, buffering, snapshot bridge and book sequencing.
    pub fn on_sbe_delta(
        &mut self,
        token: ConnectionToken,
        bytes: &[u8],
        received: u64,
    ) -> Result<Outcome, SyncError> {
        if self.connection != Some(token) {
            return Ok(Outcome::IgnoredObsolete);
        }
        if !self.observe_clock(received)? {
            return Ok(Outcome::Rebuild(RebuildReason::ClockRegressed));
        }
        let update = match feed::decode_sbe_diff::<U>(
            bytes,
            &self.context,
            received,
            self.config.max_message_bytes,
        ) {
            Ok(v) => v,
            Err(e) => return self.rebuild(RebuildReason::Decode(e)),
        };
        self.apply_update(update)
    }
    pub fn on_snapshot(
        &mut self,
        token: SnapshotToken,
        status: u16,
        bytes: &[u8],
        now: u64,
    ) -> Result<Outcome, SyncError> {
        let Some(deadline) = self.current_request(token) else {
            return Ok(Outcome::IgnoredObsolete);
        };
        if !self.observe_clock(now)? {
            return Ok(Outcome::Rebuild(RebuildReason::ClockRegressed));
        }
        self.pending = None;
        if now >= deadline {
            return Ok(Outcome::RetrySnapshot(RetryReason::Expired));
        }
        if status != 200 {
            return Ok(Outcome::RetrySnapshot(RetryReason::Http(status)));
        }
        let snapshot =
            match feed::decode_snapshot::<L>(bytes, &self.context, self.config.max_message_bytes) {
                Ok(v) => v,
                Err(e) => return self.rebuild(RebuildReason::Decode(e)),
            };
        let value = snapshot.snapshot();
        if value.bids.len() > self.config.snapshot_limit
            || value.asks.len() > self.config.snapshot_limit
        {
            return self.rebuild(RebuildReason::Decode(Error::Capacity));
        }
        if P::MARKET == Market::Spot && self.first_u.is_some_and(|u| value.last_update_id < u) {
            return Ok(Outcome::RetrySnapshot(RetryReason::TooOld));
        }
        if let Err(e) = self.book.install_snapshot(value) {
            return self.rebuild(RebuildReason::Book(e));
        }
        let (mut applied, mut discarded) = (0, 0);
        while let Some(update) = self.buffer.pop_front() {
            match self.book.apply(update.event()) {
                Ok(ApplyOutcome::Applied { .. }) => applied += 1,
                Ok(ApplyOutcome::IgnoredStale) => discarded += 1,
                Err(e) => return self.rebuild(RebuildReason::Book(e)),
            }
        }
        self.first_u = None;
        let live = self.book.state() == BookState::Live;
        self.state = if live { State::Live } else { State::Bridging };
        Ok(Outcome::SnapshotInstalled {
            applied,
            discarded,
            live,
        })
    }
    pub fn snapshot_failed(
        &mut self,
        token: SnapshotToken,
        now: u64,
    ) -> Result<Outcome, SyncError> {
        let Some(deadline) = self.current_request(token) else {
            return Ok(Outcome::IgnoredObsolete);
        };
        if !self.observe_clock(now)? {
            return Ok(Outcome::Rebuild(RebuildReason::ClockRegressed));
        }
        self.pending = None;
        Ok(Outcome::RetrySnapshot(if now >= deadline {
            RetryReason::Expired
        } else {
            RetryReason::Transport
        }))
    }
    pub fn expire_snapshot(&mut self, now: u64) -> Result<Option<Outcome>, SyncError> {
        if self.connection.is_none() {
            return Ok(None);
        }
        if !self.observe_clock(now)? {
            return Ok(Some(Outcome::Rebuild(RebuildReason::ClockRegressed)));
        }
        if self.pending.is_some_and(|(_, deadline)| now >= deadline) {
            self.pending = None;
            return Ok(Some(Outcome::RetrySnapshot(RetryReason::Expired)));
        }
        Ok(None)
    }
    pub fn disconnect(&mut self, token: ConnectionToken) -> Result<Outcome, SyncError> {
        if self.connection != Some(token) {
            return Ok(Outcome::IgnoredObsolete);
        }
        self.book
            .invalidate(self.book.generation(), InvalidReason::Sequence)
            .expect("current generation");
        self.clear_attempt();
        self.connection = None;
        self.subscribed = false;
        self.state = State::Disconnected;
        Ok(Outcome::Disconnected)
    }
    pub fn state(&self) -> State {
        self.state
    }
    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }
    pub fn generation(&self) -> u64 {
        self.book.generation()
    }
    pub fn last_update_id(&self) -> Option<u64> {
        self.book.last_update_id()
    }
    pub fn view(&self, now: u64, maximum_age: u64) -> Result<BookView<'_>, ViewError> {
        if self.state != State::Live {
            return Err(ViewError::NotLive);
        }
        self.book.view(now, maximum_age)
    }
    fn current_request(&self, token: SnapshotToken) -> Option<u64> {
        self.pending
            .filter(|(current, _)| *current == token)
            .map(|(_, deadline)| deadline)
    }
    fn clear_attempt(&mut self) {
        self.buffer.clear();
        self.pending = None;
        self.first_u = None;
    }
    fn reset_book(&mut self) -> Result<(), SyncError> {
        let Some(generation) = self.book.generation().checked_add(1) else {
            return self.exhaust();
        };
        self.book
            .begin_rebuild(generation)
            .expect("strictly increased generation");
        self.context = DepthContext::new(
            P::MARKET,
            self.config.instrument.clone(),
            self.config.key,
            generation,
            self.config.time_unit,
        )
        .expect("immutable validated context");
        self.clear_attempt();
        self.state = State::Buffering;
        Ok(())
    }
    fn exhaust<T>(&mut self) -> Result<T, SyncError> {
        self.book
            .invalidate(self.book.generation(), InvalidReason::Input)
            .expect("current generation");
        self.clear_attempt();
        self.connection = None;
        self.subscribed = false;
        self.state = State::Exhausted;
        Err(SyncError::Exhausted)
    }
    fn rebuild(&mut self, reason: RebuildReason) -> Result<Outcome, SyncError> {
        self.reset_book()?;
        Ok(Outcome::Rebuild(reason))
    }
    fn observe_clock(&mut self, now: u64) -> Result<bool, SyncError> {
        if self.last_clock.is_some_and(|last| now < last) {
            self.reset_book()?;
            return Ok(false);
        }
        self.last_clock = Some(now);
        Ok(true)
    }
}
