//! Bounded single-owner order book with independent venue sequence policies.
//!
//! Inputs borrow normalized integer levels. No transport, decoder, clock, or
//! allocator is owned by this crate. See `specs/implementation/book-core.md`.
#![no_std]
#![forbid(unsafe_code)]

use core::marker::PhantomData;

#[cfg(test)]
mod work_tests;
// Count algorithmic work in unit tests; production builds contain no counters.
#[cfg(test)]
extern crate std;
#[cfg(test)]
mod work {
    std::thread_local! {
        static COUNTS: core::cell::Cell<(usize, usize)> = const { core::cell::Cell::new((0, 0)) };
    }
    pub fn comparison() {
        COUNTS.with(|c| {
            let (a, b) = c.get();
            c.set((a + 1, b));
        });
    }
    pub fn moved(n: usize) {
        COUNTS.with(|c| {
            let (a, b) = c.get();
            c.set((a, b + n));
        });
    }
    pub fn reset() {
        COUNTS.with(|c| c.set((0, 0)));
    }
    pub fn counts() -> (usize, usize) {
        COUNTS.with(core::cell::Cell::get)
    }
}

fn compare_prices(side: Side, a: PriceTicks, b: PriceTicks) -> core::cmp::Ordering {
    #[cfg(test)]
    work::comparison();
    match side {
        Side::Bid => b.cmp(&a),
        Side::Ask => a.cmp(&b),
    }
}

fn move_levels(levels: &mut [Level], range: core::ops::Range<usize>, destination: usize) {
    #[cfg(test)]
    work::moved(range.len());
    levels.copy_within(range, destination);
}

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

#[derive(Clone, Copy, Debug)]
struct PreparedLevel {
    level: Level,
    index: usize,
    exists: bool,
}
const EMPTY_UPDATE: PreparedLevel = PreparedLevel {
    level: EMPTY_LEVEL,
    index: 0,
    exists: false,
};

struct SidePlan {
    best: Option<PriceTicks>,
    new_len: usize,
    structural: bool,
    start: usize,
    end: usize,
}

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
                move_levels(&mut self.data, index + 1..self.len, index);
                self.len -= 1;
            }
        }
        for level in updates.iter().filter(|level| level.quantity.0 != 0) {
            match self.locate(side, level.price) {
                Ok(index) => self.data[index] = *level,
                Err(index) => {
                    move_levels(&mut self.data, index..self.len, index + 1);
                    self.data[index] = *level;
                    self.len += 1;
                }
            }
        }
    }

    fn apply_prepared(
        &mut self,
        side: Side,
        updates: &[PreparedLevel],
        plan: &SidePlan,
        scratch: &mut [Level; N],
    ) {
        if !plan.structural {
            for update in updates.iter().filter(|u| u.exists) {
                self.data[update.index] = update.level;
                #[cfg(test)]
                work::moved(1);
            }
            return;
        }
        // Merge only the affected span. The unaffected tail moves once, and
        // equal-size replacement (including top churn) never moves that tail.
        let mut old = plan.start;
        let mut next = 0;
        let mut written = 0;
        while old < plan.end || next < updates.len() {
            let take_update = next < updates.len()
                && (old == plan.end
                    || !compare_prices(side, updates[next].level.price, self.data[old].price)
                        .is_gt());
            let level = if take_update {
                let update = updates[next];
                next += 1;
                if update.exists {
                    old += 1;
                }
                if update.level.quantity.0 == 0 {
                    continue;
                }
                update.level
            } else {
                let level = self.data[old];
                old += 1;
                level
            };
            scratch[written] = level;
            written += 1;
            #[cfg(test)]
            work::moved(1);
        }
        if self.len != plan.new_len {
            move_levels(&mut self.data, plan.end..self.len, plan.start + written);
        }
        self.data[plan.start..plan.start + written].copy_from_slice(&scratch[..written]);
        #[cfg(test)]
        work::moved(written);
        self.len = plan.new_len;
    }
}

fn prepare_side<const N: usize>(
    side: Side,
    levels: &Levels<N>,
    input: &[Level],
    updates: &mut [PreparedLevel],
) -> Result<SidePlan, BookError> {
    for (target, level) in updates.iter_mut().zip(input) {
        if level.price.0 <= 0 || level.quantity.0 < 0 {
            return Err(BookError::InvalidLevel);
        }
        target.level = *level;
    }
    let small = input.len() <= 32;
    let directions = if small {
        input_directions(side, input)
    } else {
        3
    };
    let unordered_replacements =
        small && directions == 3 && input.iter().all(|level| level.quantity.0 > 0);
    let cyclic = if unordered_replacements {
        cyclic_order(side, input)
    } else {
        None
    };
    if unordered_replacements
        && cyclic.is_none()
        && let Some(plan) = prepare_replacements(side, levels, updates)?
    {
        return Ok(plan);
    }
    match (directions, cyclic) {
        (_, Some((split, reverse))) => {
            updates.rotate_left(split);
            if reverse {
                updates.reverse();
            }
        }
        (0 | 1, _) => {}
        (2, _) => updates.reverse(),
        _ => updates.sort_unstable_by(|a, b| compare_prices(side, a.level.price, b.level.price)),
    }
    let mut removed = 0;
    let mut added = 0;
    let mut removed_prefix = 0;
    let mut start = levels.len;
    let mut end = 0;
    let mut new_best = None;
    let mut cursor = 0;
    for index in 0..updates.len() {
        if index > 0
            && compare_prices(
                side,
                updates[index - 1].level.price,
                updates[index].level.price,
            )
            .is_eq()
        {
            return Err(BookError::DuplicatePrice { side });
        }
        let update = &mut updates[index];
        // Adjacent updates reuse the next position. For a distant price, fixed
        // full-book bounds avoid making every probe address depend on the
        // previous search result; no full-book scan is performed.
        (update.index, update.exists) = if cursor < levels.len
            && compare_prices(side, levels.data[cursor].price, update.level.price).is_eq()
        {
            (cursor, true)
        } else {
            match locate(levels.as_slice(), side, update.level.price) {
                Ok(index) => (index, true),
                Err(index) => (index, false),
            }
        };
        cursor = update.index + usize::from(update.exists);
        if update.exists && update.level.quantity.0 == 0 {
            removed += 1;
            if update.index == removed_prefix {
                removed_prefix += 1;
            }
        } else if !update.exists && update.level.quantity.0 > 0 {
            added += 1;
        }
        if update.exists || update.level.quantity.0 > 0 {
            start = start.min(update.index);
            end = end.max(update.index + usize::from(update.exists));
        }
        if new_best.is_none() && update.level.quantity.0 > 0 {
            new_best = Some(update.level.price);
        }
    }
    let new_len = (levels.len - removed)
        .checked_add(added)
        .filter(|len| *len <= N)
        .ok_or(BookError::Capacity { side })?;
    let existing_best = levels.as_slice().get(removed_prefix).map(|l| l.price);
    let best = match (existing_best, new_best) {
        (Some(a), Some(b)) => Some(if compare_prices(side, a, b).is_lt() {
            a
        } else {
            b
        }),
        (a, b) => a.or(b),
    };
    Ok(SidePlan {
        best,
        new_len,
        structural: removed != 0 || added != 0,
        start,
        end,
    })
}

fn input_directions(side: Side, input: &[Level]) -> u8 {
    // This replaces the small ordered batch's sorting pass. Unordered batches
    // stop as soon as both directions occur; adjacent equal prices are checked
    // by the selected plan, including after reversing a descending input.
    let mut directions = 0u8;
    for pair in input.windows(2) {
        directions |= match compare_prices(side, pair[0].price, pair[1].price) {
            core::cmp::Ordering::Less => 1,
            core::cmp::Ordering::Greater => 2,
            core::cmp::Ordering::Equal => 0,
        };
        if directions == 3 {
            break;
        }
    }
    directions
}

fn cyclic_order(side: Side, input: &[Level]) -> Option<(usize, bool)> {
    // The caller has already observed both directions. A single wrap in either
    // order should retain the sorted plan's adjacent-position reuse.
    let direction = compare_prices(side, input[input.len() - 1].price, input[0].price);
    if direction.is_eq() {
        return None;
    }
    let mut split = None;
    for (index, pair) in input.windows(2).enumerate() {
        let step = compare_prices(side, pair[0].price, pair[1].price);
        if !step.is_eq() && step != direction {
            if split.is_some() {
                return None;
            }
            split = Some(index + 1);
        }
    }
    split.map(|index| (index, direction.is_gt()))
}

// Bounded, unordered quantity-only plan. The bitset is a collision filter, not
// a uniqueness proof: matching bits always receive an exact price check.
fn prepare_replacements<const N: usize>(
    side: Side,
    levels: &Levels<N>,
    updates: &mut [PreparedLevel],
) -> Result<Option<SidePlan>, BookError> {
    let mut seen = 0u64;
    for i in 0..updates.len() {
        let level = updates[i].level;
        let Ok(index) = levels.locate(side, level.price) else {
            return Ok(None);
        };
        let bit = 1u64 << (index & 63);
        if seen & bit != 0
            && updates[..i]
                .iter()
                .any(|prior| compare_prices(side, prior.level.price, level.price).is_eq())
        {
            return Err(BookError::DuplicatePrice { side });
        }
        seen |= bit;
        updates[i].index = index;
        updates[i].exists = true;
    }
    Ok(Some(SidePlan {
        best: levels.as_slice().first().map(|l| l.price),
        new_len: levels.len,
        structural: false,
        start: 0,
        end: 0,
    }))
}

fn locate(levels: &[Level], side: Side, price: PriceTicks) -> Result<usize, usize> {
    levels.binary_search_by(|level| compare_prices(side, level.price, price))
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
    updates: [PreparedLevel; MAX_UPDATES],
    merge: [Level; LEVELS],
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
            updates: [EMPTY_UPDATE; M],
            merge: [EMPTY_LEVEL; N],
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
        if event
            .bids
            .len()
            .checked_add(event.asks.len())
            .is_none_or(|n| n > M)
        {
            return self.fail(BookError::TooManyUpdates);
        }
        let plans = (|| {
            let (bids, asks) =
                self.updates[..event.bids.len() + event.asks.len()].split_at_mut(event.bids.len());
            let bid_plan = prepare_side(Side::Bid, &self.bids, event.bids, bids)?;
            let ask_plan = prepare_side(Side::Ask, &self.asks, event.asks, asks)?;
            validate_spread(bid_plan.best, ask_plan.best)?;
            Ok((bid_plan, ask_plan))
        })();
        let (bid_plan, ask_plan) = match plans {
            Ok(plans) => plans,
            Err(error) => return self.fail(error),
        };
        let (bids, asks) =
            self.updates[..event.bids.len() + event.asks.len()].split_at(event.bids.len());
        self.bids
            .apply_prepared(Side::Bid, bids, &bid_plan, &mut self.merge);
        self.asks
            .apply_prepared(Side::Ask, asks, &ask_plan, &mut self.merge);
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
}

fn validate_levels(side: Side, levels: &[Level]) -> Result<(), BookError> {
    for (index, level) in levels.iter().enumerate() {
        if level.price.0 <= 0 || level.quantity.0 < 0 {
            return Err(BookError::InvalidLevel);
        }
        if levels[..index]
            .iter()
            .any(|earlier| compare_prices(side, earlier.price, level.price).is_eq())
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

fn final_best(levels: &[Level], side: Side, updates: &[Level]) -> Option<PriceTicks> {
    // Existing levels are sorted. The first one not overwritten is their best.
    let existing = levels.iter().find(|level| {
        !updates
            .iter()
            .any(|update| compare_prices(side, update.price, level.price).is_eq())
    });
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
