//! Bounded single-owner order book with independent venue sequence policies.
//!
//! Inputs borrow normalized integer levels. No transport, decoder, clock, or
//! allocator is owned by this crate. See `docs/implementation/book-interface.md`.
#![no_std]
#![forbid(unsafe_code)]

use core::marker::PhantomData;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BookKey {
    pub venue: u32,
    pub instrument: u32,
    pub source: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PriceTicks(pub i64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct QtyLots(pub i64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Level {
    pub price: PriceTicks,
    pub quantity: QtyLots,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, Copy, Debug)]
pub struct Snapshot<'a> {
    pub key: BookKey,
    pub generation: u64,
    pub last_update_id: u64,
    pub bids: &'a [Level],
    pub asks: &'a [Level],
    pub bids_complete: bool,
    pub asks_complete: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct DepthEvent<'a> {
    pub key: BookKey,
    pub generation: u64,
    pub first_update_id: u64,
    pub last_update_id: u64,
    pub previous_update_id: Option<u64>,
    pub received_at_ns: u64,
    pub bids: &'a [Level],
    pub asks: &'a [Level],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidReason {
    Sequence,
    Capacity,
    Input,
    Clock,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BookState {
    AwaitingSnapshot,
    Bridging,
    Live,
    Invalid(InvalidReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SequenceError {
    InvalidRange,
    MissingPrevious,
    Gap,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BookError {
    WrongBook,
    WrongGeneration,
    GenerationNotIncreasing,
    NeedsSnapshot,
    NeedsRebuild,
    SnapshotWhileLive,
    Sequence(SequenceError),
    InvalidLevel,
    DuplicatePrice { side: Side },
    TooManyUpdates,
    Capacity { side: Side },
    CrossedBook,
    ClockRegressed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ViewError {
    NotLive,
    Stale,
    ClockRegressed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyOutcome {
    Applied {
        last_update_id: u64,
        became_live: bool,
    },
    IgnoredStale,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SequenceDecision {
    Apply,
    IgnoreStale,
}

/// Sequence coordinates for a single event in an already installed snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SequenceInput {
    pub local_id: u64,
    pub bridging: bool,
    pub first_update_id: u64,
    pub last_update_id: u64,
    pub previous_update_id: Option<u64>,
}

/// An exchange adapter supplies its sequencing semantics independently of storage.
pub trait SequencePolicy {
    fn classify(input: SequenceInput) -> Result<SequenceDecision, SequenceError>;
}

#[derive(Debug)]
pub struct BinanceSpot;
impl SequencePolicy for BinanceSpot {
    fn classify(input: SequenceInput) -> Result<SequenceDecision, SequenceError> {
        validate_range(input)?;
        if input.last_update_id <= input.local_id {
            return Ok(SequenceDecision::IgnoreStale);
        }
        // last_update_id > local_id proves local_id < u64::MAX.
        if input.first_update_id > input.local_id + 1 {
            return Err(SequenceError::Gap);
        }
        Ok(SequenceDecision::Apply)
    }
}

#[derive(Debug)]
pub struct BinanceUsdM;
impl SequencePolicy for BinanceUsdM {
    fn classify(input: SequenceInput) -> Result<SequenceDecision, SequenceError> {
        validate_range(input)?;
        if input.last_update_id < input.local_id
            || (!input.bridging && input.last_update_id == input.local_id)
        {
            return Ok(SequenceDecision::IgnoreStale);
        }
        let previous = input
            .previous_update_id
            .ok_or(SequenceError::MissingPrevious)?;
        if (input.bridging && input.first_update_id > input.local_id)
            || (!input.bridging && previous != input.local_id)
        {
            return Err(SequenceError::Gap);
        }
        Ok(SequenceDecision::Apply)
    }
}

fn validate_range(input: SequenceInput) -> Result<(), SequenceError> {
    if input.first_update_id > input.last_update_id {
        Err(SequenceError::InvalidRange)
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuantityKnowledge {
    Known(QtyLots),
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BestLevel {
    Known(Level),
    Empty,
    Unknown,
}

/// A truncated snapshot proves every price on the better side of its boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SideCoverage {
    Complete,
    Through(PriceTicks),
    Unknown,
}

impl SideCoverage {
    fn contains(self, side: Side, price: PriceTicks) -> bool {
        if price.0 <= 0 {
            return false;
        }
        match self {
            Self::Complete => true,
            Self::Through(boundary) => match side {
                Side::Bid => price >= boundary,
                Side::Ask => price <= boundary,
            },
            Self::Unknown => false,
        }
    }
}

const EMPTY_LEVEL: Level = Level {
    price: PriceTicks(0),
    quantity: QtyLots(0),
};

#[derive(Debug)]
struct Levels<const N: usize> {
    data: [Level; N],
    len: usize,
}

impl<const N: usize> Levels<N> {
    fn new() -> Self {
        Self {
            data: [EMPTY_LEVEL; N],
            len: 0,
        }
    }
    fn as_slice(&self) -> &[Level] {
        &self.data[..self.len]
    }
    fn locate(&self, side: Side, price: PriceTicks) -> Result<usize, usize> {
        locate(self.as_slice(), side, price)
    }

    // Caller has validated unique prices and final capacity. Delete all zero
    // quantities before insertion, so intermediate occupancy cannot exceed N.
    fn apply_validated(&mut self, side: Side, updates: &[Level]) {
        for level in updates.iter().filter(|level| level.quantity.0 == 0) {
            if let Ok(index) = self.locate(side, level.price) {
                self.data.copy_within(index + 1..self.len, index);
                self.len -= 1;
            }
        }
        for level in updates.iter().filter(|level| level.quantity.0 != 0) {
            match self.locate(side, level.price) {
                Ok(index) => self.data[index] = *level,
                Err(index) => {
                    self.data.copy_within(index..self.len, index + 1);
                    self.data[index] = *level;
                    self.len += 1;
                }
            }
        }
    }
}

fn locate(levels: &[Level], side: Side, price: PriceTicks) -> Result<usize, usize> {
    levels.binary_search_by(|level| match side {
        Side::Bid => price.cmp(&level.price),
        Side::Ask => level.price.cmp(&price),
    })
}

#[derive(Debug)]
pub struct Book<P, const LEVELS: usize, const MAX_UPDATES: usize> {
    key: BookKey,
    generation: u64,
    state: BookState,
    last_update_id: Option<u64>,
    last_event_at_ns: Option<u64>,
    bids: Levels<LEVELS>,
    asks: Levels<LEVELS>,
    bids_coverage: SideCoverage,
    asks_coverage: SideCoverage,
    policy: PhantomData<P>,
}

impl<P: SequencePolicy, const N: usize, const M: usize> Book<P, N, M> {
    pub fn new(key: BookKey, generation: u64) -> Self {
        Self {
            key,
            generation,
            state: BookState::AwaitingSnapshot,
            last_update_id: None,
            last_event_at_ns: None,
            bids: Levels::new(),
            asks: Levels::new(),
            bids_coverage: SideCoverage::Unknown,
            asks_coverage: SideCoverage::Unknown,
            policy: PhantomData,
        }
    }

    pub fn key(&self) -> BookKey {
        self.key
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn state(&self) -> BookState {
        self.state
    }
    pub fn last_update_id(&self) -> Option<u64> {
        self.last_update_id
    }

    pub fn install_snapshot(&mut self, snapshot: Snapshot<'_>) -> Result<(), BookError> {
        self.check_identity(snapshot.key, snapshot.generation)?;
        match self.state {
            BookState::Live => return Err(BookError::SnapshotWhileLive),
            BookState::Invalid(_) => return Err(BookError::NeedsRebuild),
            BookState::AwaitingSnapshot | BookState::Bridging => {}
        }
        let validation = validate_snapshot::<N>(snapshot);
        if let Err(error) = validation {
            return self.fail(error);
        }

        self.bids.len = 0;
        self.asks.len = 0;
        self.bids.apply_validated(Side::Bid, snapshot.bids);
        self.asks.apply_validated(Side::Ask, snapshot.asks);
        self.bids_coverage = coverage(self.bids.as_slice(), snapshot.bids_complete);
        self.asks_coverage = coverage(self.asks.as_slice(), snapshot.asks_complete);
        self.last_update_id = Some(snapshot.last_update_id);
        self.last_event_at_ns = None;
        self.state = BookState::Bridging;
        Ok(())
    }

    pub fn apply(&mut self, event: DepthEvent<'_>) -> Result<ApplyOutcome, BookError> {
        self.check_identity(event.key, event.generation)?;
        let bridging = match self.state {
            BookState::AwaitingSnapshot => return Err(BookError::NeedsSnapshot),
            BookState::Invalid(_) => return Err(BookError::NeedsRebuild),
            BookState::Bridging => true,
            BookState::Live => false,
        };
        let input = SequenceInput {
            local_id: self
                .last_update_id
                .expect("snapshot installed in Bridging and Live"),
            bridging,
            first_update_id: event.first_update_id,
            last_update_id: event.last_update_id,
            previous_update_id: event.previous_update_id,
        };
        match P::classify(input) {
            Ok(SequenceDecision::IgnoreStale) => return Ok(ApplyOutcome::IgnoredStale),
            Err(error) => return self.fail(BookError::Sequence(error)),
            Ok(SequenceDecision::Apply) => {}
        }
        // Generic policies may choose their own bridge, but storage always
        // rejects decreasing watermarks and malformed ranges.
        if event.last_update_id < input.local_id || event.first_update_id > event.last_update_id {
            return self.fail(BookError::Sequence(SequenceError::InvalidRange));
        }
        if self
            .last_event_at_ns
            .is_some_and(|last| event.received_at_ns < last)
        {
            return self.fail(BookError::ClockRegressed);
        }
        let validation = self.validate_event(event);
        if let Err(error) = validation {
            return self.fail(error);
        }

        self.bids.apply_validated(Side::Bid, event.bids);
        self.asks.apply_validated(Side::Ask, event.asks);
        self.last_update_id = Some(event.last_update_id);
        self.last_event_at_ns = Some(event.received_at_ns);
        self.state = BookState::Live;
        Ok(ApplyOutcome::Applied {
            last_update_id: event.last_update_id,
            became_live: bridging,
        })
    }

    pub fn begin_rebuild(&mut self, new_generation: u64) -> Result<(), BookError> {
        if new_generation <= self.generation {
            return Err(BookError::GenerationNotIncreasing);
        }
        self.generation = new_generation;
        self.state = BookState::AwaitingSnapshot;
        self.last_update_id = None;
        self.last_event_at_ns = None;
        self.bids.len = 0;
        self.asks.len = 0;
        self.bids_coverage = SideCoverage::Unknown;
        self.asks_coverage = SideCoverage::Unknown;
        Ok(())
    }

    /// Explicitly invalidate after a decoder error, buffer loss, or disconnect.
    /// A delayed callback from an old generation cannot invalidate a new book.
    pub fn invalidate(&mut self, generation: u64, reason: InvalidReason) -> Result<(), BookError> {
        if generation != self.generation {
            return Err(BookError::WrongGeneration);
        }
        self.state = BookState::Invalid(reason);
        Ok(())
    }

    pub fn view(&self, now_ns: u64, maximum_age_ns: u64) -> Result<BookView<'_>, ViewError> {
        if self.state != BookState::Live {
            return Err(ViewError::NotLive);
        }
        let received_at_ns = self
            .last_event_at_ns
            .expect("Live requires an applied event");
        let age_ns = now_ns
            .checked_sub(received_at_ns)
            .ok_or(ViewError::ClockRegressed)?;
        if age_ns > maximum_age_ns {
            return Err(ViewError::Stale);
        }
        Ok(BookView {
            key: self.key,
            generation: self.generation,
            last_update_id: self.last_update_id.expect("Live requires a snapshot"),
            received_at_ns,
            age_ns,
            bids: self.bids.as_slice(),
            asks: self.asks.as_slice(),
            bids_coverage: self.bids_coverage,
            asks_coverage: self.asks_coverage,
        })
    }

    /// Inspection only: these levels remain available when the book is invalid.
    pub fn diagnostic_levels(&self, side: Side) -> &[Level] {
        match side {
            Side::Bid => self.bids.as_slice(),
            Side::Ask => self.asks.as_slice(),
        }
    }

    fn check_identity(&self, key: BookKey, generation: u64) -> Result<(), BookError> {
        if key != self.key {
            return Err(BookError::WrongBook);
        }
        if generation != self.generation {
            return Err(BookError::WrongGeneration);
        }
        Ok(())
    }

    fn fail<T>(&mut self, error: BookError) -> Result<T, BookError> {
        let reason = match error {
            BookError::Sequence(_) => InvalidReason::Sequence,
            BookError::Capacity { .. } | BookError::TooManyUpdates => InvalidReason::Capacity,
            BookError::ClockRegressed => InvalidReason::Clock,
            _ => InvalidReason::Input,
        };
        self.state = BookState::Invalid(reason);
        Err(error)
    }

    fn validate_event(&self, event: DepthEvent<'_>) -> Result<(), BookError> {
        if event
            .bids
            .len()
            .checked_add(event.asks.len())
            .is_none_or(|count| count > M)
        {
            return Err(BookError::TooManyUpdates);
        }
        validate_levels(Side::Bid, event.bids)?;
        validate_levels(Side::Ask, event.asks)?;
        validate_capacity(&self.bids, Side::Bid, event.bids)?;
        validate_capacity(&self.asks, Side::Ask, event.asks)?;
        validate_spread(
            final_best(self.bids.as_slice(), Side::Bid, event.bids),
            final_best(self.asks.as_slice(), Side::Ask, event.asks),
        )
    }
}

fn validate_levels(side: Side, levels: &[Level]) -> Result<(), BookError> {
    for (index, level) in levels.iter().enumerate() {
        if level.price.0 <= 0 || level.quantity.0 < 0 {
            return Err(BookError::InvalidLevel);
        }
        if levels[..index]
            .iter()
            .any(|earlier| earlier.price == level.price)
        {
            return Err(BookError::DuplicatePrice { side });
        }
    }
    Ok(())
}

fn validate_snapshot<const N: usize>(snapshot: Snapshot<'_>) -> Result<(), BookError> {
    for (side, levels) in [(Side::Bid, snapshot.bids), (Side::Ask, snapshot.asks)] {
        if levels.len() > N {
            return Err(BookError::Capacity { side });
        }
        validate_levels(side, levels)?;
    }
    validate_spread(
        final_best(&[], Side::Bid, snapshot.bids),
        final_best(&[], Side::Ask, snapshot.asks),
    )
}

fn validate_capacity<const N: usize>(
    levels: &Levels<N>,
    side: Side,
    updates: &[Level],
) -> Result<(), BookError> {
    let mut removed = 0;
    let mut added = 0;
    for level in updates {
        match (
            levels.locate(side, level.price).is_ok(),
            level.quantity.0 == 0,
        ) {
            (true, true) => removed += 1,
            (false, false) => added += 1,
            _ => {}
        }
    }
    if (levels.len - removed)
        .checked_add(added)
        .is_none_or(|count| count > N)
    {
        Err(BookError::Capacity { side })
    } else {
        Ok(())
    }
}

fn final_best(levels: &[Level], side: Side, updates: &[Level]) -> Option<PriceTicks> {
    // Existing levels are sorted. The first one not overwritten is their best.
    let existing = levels
        .iter()
        .find(|level| !updates.iter().any(|update| update.price == level.price));
    let mut best = existing.map(|level| level.price);
    for update in updates.iter().filter(|level| level.quantity.0 > 0) {
        best = Some(match best {
            None => update.price,
            Some(price) => match side {
                Side::Bid => price.max(update.price),
                Side::Ask => price.min(update.price),
            },
        });
    }
    best
}

fn validate_spread(bid: Option<PriceTicks>, ask: Option<PriceTicks>) -> Result<(), BookError> {
    if let (Some(bid), Some(ask)) = (bid, ask)
        && bid >= ask
    {
        return Err(BookError::CrossedBook);
    }
    Ok(())
}

fn coverage(levels: &[Level], complete: bool) -> SideCoverage {
    if complete {
        SideCoverage::Complete
    } else {
        levels.last().map_or(SideCoverage::Unknown, |level| {
            SideCoverage::Through(level.price)
        })
    }
}

#[derive(Debug)]
pub struct BookView<'a> {
    key: BookKey,
    generation: u64,
    last_update_id: u64,
    received_at_ns: u64,
    age_ns: u64,
    bids: &'a [Level],
    asks: &'a [Level],
    bids_coverage: SideCoverage,
    asks_coverage: SideCoverage,
}

impl BookView<'_> {
    pub fn key(&self) -> BookKey {
        self.key
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn last_update_id(&self) -> u64 {
        self.last_update_id
    }
    pub fn received_at_ns(&self) -> u64 {
        self.received_at_ns
    }
    /// Age at the instant `Book::view` was called, in the caller's clock domain.
    pub fn age_ns(&self) -> u64 {
        self.age_ns
    }
    pub fn known_bids(&self) -> &[Level] {
        self.bids
    }
    pub fn known_asks(&self) -> &[Level] {
        self.asks
    }
    pub fn coverage(&self, side: Side) -> SideCoverage {
        match side {
            Side::Bid => self.bids_coverage,
            Side::Ask => self.asks_coverage,
        }
    }
    pub fn quantity(&self, side: Side, price: PriceTicks) -> QuantityKnowledge {
        let levels = match side {
            Side::Bid => self.bids,
            Side::Ask => self.asks,
        };
        match locate(levels, side, price) {
            Ok(index) => QuantityKnowledge::Known(levels[index].quantity),
            Err(_) if self.coverage(side).contains(side, price) => {
                QuantityKnowledge::Known(QtyLots(0))
            }
            Err(_) => QuantityKnowledge::Unknown,
        }
    }
    pub fn best_bid(&self) -> BestLevel {
        self.best(Side::Bid, self.bids)
    }
    pub fn best_ask(&self) -> BestLevel {
        self.best(Side::Ask, self.asks)
    }
    fn best(&self, side: Side, levels: &[Level]) -> BestLevel {
        match levels.first() {
            Some(level) if self.coverage(side).contains(side, level.price) => {
                BestLevel::Known(*level)
            }
            None if self.coverage(side) == SideCoverage::Complete => BestLevel::Empty,
            _ => BestLevel::Unknown,
        }
    }
}
