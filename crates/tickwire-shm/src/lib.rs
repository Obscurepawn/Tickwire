//! Single-owner publication and borrow state for shared-memory banks.
#![deny(unsafe_code)]

mod arena;
#[allow(unsafe_code)]
mod mapped;
pub use mapped::{
    MappedError, MarketMessage, Publisher, RecordBorrow, Subscriber, configure_market_arena,
    validate_bank,
};

const SEQUENCE_LIMIT: u64 = 1 << 63;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BankKey {
    pub resource: u128,
    pub channel: u128,
    pub channel_epoch: u64,
    pub ring_epoch: u64,
    pub feed_generation: u64,
}
impl BankKey {
    fn valid(self) -> bool {
        self.resource != 0
            && self.channel != 0
            && (1..SEQUENCE_LIMIT).contains(&self.channel_epoch)
            && (1..SEQUENCE_LIMIT).contains(&self.ring_epoch)
            && self.feed_generation < SEQUENCE_LIMIT
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionKey {
    pub bank: BankKey,
    pub instance: u128,
    pub generation: u64,
    pub cursor: u128,
}
impl SubscriptionKey {
    fn valid(self) -> bool {
        self.bank.valid()
            && self.instance != 0
            && (1..SEQUENCE_LIMIT).contains(&self.generation)
            && self.cursor != 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Identity,
    Capacity,
    BatchSize,
    SequenceExhausted,
    NotActive,
    OutstandingWrite,
    OutstandingRead,
    Full,
    ArenaExhausted,
    Cursor,
    Cutover,
    BorrowCapacity,
    NotQuiescing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Watermarks {
    pub read: u64,
    pub applied: u64,
    pub released: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Freeze {
    pub bank: BankKey,
    pub successor_ring_epoch: u64,
    pub cutover: u64,
    pub membership_version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Ready,
    Active,
    Frozen,
}
#[derive(Debug, Clone, Copy)]
struct Member {
    key: SubscriptionKey,
    baseline: u64,
    released: u64,
    active: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClaimState {
    serial: u64,
    first: u64,
    last: u64,
}

/// An exclusive batch reservation. Dropping it does not publish or release it.
#[derive(Debug)]
pub struct WriteClaim {
    bank: BankKey,
    state: ClaimState,
}
impl WriteClaim {
    pub fn first(&self) -> u64 {
        self.state.first
    }
    pub fn last(&self) -> u64 {
        self.state.last
    }
}

/// Single publisher state, with no memory access or operating-system authority.
///
/// The authenticated membership owner must never reuse a SubscriptionKey.
/// Cursor observations must come from that key's registered mapping.
#[derive(Debug)]
pub struct Producer<const M: usize> {
    key: BankKey,
    capacity: u64,
    first: u64,
    published: u64,
    lifecycle: Lifecycle,
    members: [Option<Member>; M],
    membership_version: u64,
    cached_min: Option<u64>,
    next_claim: u64,
    pending: Option<ClaimState>,
    planned: Option<(u64, u64)>,
    frozen: Option<Freeze>,
}
impl<const M: usize> Producer<M> {
    pub fn new(key: BankKey, capacity: u64, first: u64) -> Result<Self, Error> {
        if !key.valid() {
            return Err(Error::Identity);
        }
        if M == 0 || M > 32 || !(64..=1 << 20).contains(&capacity) || !capacity.is_power_of_two() {
            return Err(Error::Capacity);
        }
        if first == 0 || first >= SEQUENCE_LIMIT {
            return Err(Error::SequenceExhausted);
        }
        Ok(Self {
            key,
            capacity,
            first,
            published: first - 1,
            lifecycle: Lifecycle::Ready,
            members: [None; M],
            membership_version: 1,
            cached_min: None,
            next_claim: 1,
            pending: None,
            planned: None,
            frozen: None,
        })
    }
    pub fn activate(&mut self) -> Result<(), Error> {
        if self.lifecycle != Lifecycle::Ready {
            return Err(Error::NotActive);
        }
        self.lifecycle = Lifecycle::Active;
        Ok(())
    }
    pub fn published(&self) -> u64 {
        self.published
    }
    pub fn membership_version(&self) -> u64 {
        self.membership_version
    }
    pub fn slot(&self, sequence: u64) -> Result<usize, Error> {
        if sequence < self.first || sequence >= SEQUENCE_LIMIT {
            return Err(Error::Cursor);
        }
        Ok(((sequence - self.first) & (self.capacity - 1)) as usize)
    }
    fn boundary(&self) -> Result<(), Error> {
        if self.pending.is_some() {
            Err(Error::OutstandingWrite)
        } else {
            Ok(())
        }
    }
    fn member_index(&self, key: SubscriptionKey) -> Result<usize, Error> {
        self.members
            .iter()
            .position(|m| m.is_some_and(|m| m.key == key))
            .ok_or(Error::Identity)
    }
    fn next_version(&self) -> Result<u64, Error> {
        self.membership_version
            .checked_add(1)
            .filter(|v| *v < SEQUENCE_LIMIT)
            .ok_or(Error::SequenceExhausted)
    }
    /// Returns J, the first sequence protected for this pending reader.
    pub fn join(&mut self, key: SubscriptionKey) -> Result<u64, Error> {
        self.boundary()?;
        if self.lifecycle == Lifecycle::Frozen {
            return Err(Error::NotActive);
        }
        if !key.valid() || key.bank != self.key {
            return Err(Error::Identity);
        }
        if let Ok(index) = self.member_index(key) {
            return Ok(self.members[index].unwrap().baseline + 1);
        }
        if self
            .members
            .iter()
            .flatten()
            .any(|m| m.key.instance == key.instance || m.key.cursor == key.cursor)
        {
            return Err(Error::Identity);
        }
        let slot = self
            .members
            .iter()
            .position(Option::is_none)
            .ok_or(Error::Capacity)?;
        let version = self.next_version()?;
        let join = self
            .published
            .checked_add(1)
            .filter(|s| *s < SEQUENCE_LIMIT)
            .ok_or(Error::SequenceExhausted)?;
        self.members[slot] = Some(Member {
            key,
            baseline: self.published,
            released: self.published,
            active: false,
        });
        self.membership_version = version;
        self.cached_min = None;
        Ok(join)
    }
    /// Call only after resource and snapshot ReadReady bindings are verified.
    pub fn join_ready(&mut self, key: SubscriptionKey, released: u64) -> Result<(), Error> {
        self.boundary()?;
        if self.lifecycle == Lifecycle::Frozen {
            return Err(Error::NotActive);
        }
        let index = self.member_index(key)?;
        let member = self.members[index].as_mut().unwrap();
        if released < member.released || released > self.published {
            return Err(Error::Cursor);
        }
        member.active = true;
        member.released = released;
        self.cached_min = None;
        Ok(())
    }
    /// A single independently sampled field; no inconsistent multi-field snapshot.
    pub fn observe_release(&mut self, key: SubscriptionKey, released: u64) -> Result<(), Error> {
        let index = self.member_index(key)?;
        let member = self.members[index].as_mut().unwrap();
        if !member.active {
            return Err(Error::NotActive);
        }
        if released < member.released || released > self.published {
            return Err(Error::Cursor);
        }
        member.released = released;
        // Keep the old cached value: it is a conservative lower bound.
        Ok(())
    }
    fn refresh_min(&mut self) -> u64 {
        let min = self
            .members
            .iter()
            .flatten()
            .map(|m| m.released)
            .min()
            .unwrap_or(self.published);
        self.cached_min = Some(min);
        min
    }
    pub fn claim(&mut self, count: u8) -> Result<WriteClaim, Error> {
        self.boundary()?;
        if self.lifecycle != Lifecycle::Active {
            return Err(Error::NotActive);
        }
        if !(1..=64).contains(&count) {
            return Err(Error::BatchSize);
        }
        let last = self
            .published
            .checked_add(u64::from(count))
            .filter(|s| *s < SEQUENCE_LIMIT)
            .ok_or(Error::SequenceExhausted)?;
        if self.planned.is_some_and(|(_, cutoff)| last > cutoff) {
            return Err(Error::Cutover);
        }
        let mut min = self.cached_min.unwrap_or_else(|| self.refresh_min());
        if last - min > self.capacity {
            min = self.refresh_min();
            if last - min > self.capacity {
                return Err(Error::Full);
            }
        }
        let next = self
            .next_claim
            .checked_add(1)
            .ok_or(Error::SequenceExhausted)?;
        let state = ClaimState {
            serial: self.next_claim,
            first: self.published + 1,
            last,
        };
        self.next_claim = next;
        self.pending = Some(state);
        Ok(WriteClaim {
            bank: self.key,
            state,
        })
    }
    fn check_claim(&self, claim: &WriteClaim) -> Result<(), Error> {
        if claim.bank != self.key || self.pending != Some(claim.state) {
            Err(Error::Identity)
        } else {
            Ok(())
        }
    }
    /// Payload must be complete before the mapped SDK publishes the resulting watermark.
    pub fn publish(&mut self, claim: WriteClaim) -> Result<u64, Error> {
        self.check_claim(&claim)?;
        self.published = claim.state.last;
        self.pending = None;
        Ok(self.published)
    }
    pub fn abort(&mut self, claim: WriteClaim) -> Result<(), Error> {
        self.check_claim(&claim)?;
        self.pending = None;
        Ok(())
    }
    pub fn prepare_freeze(&mut self, successor: u64, cutoff: u64) -> Result<(), Error> {
        self.boundary()?;
        if self.lifecycle != Lifecycle::Active {
            return Err(Error::NotActive);
        }
        if successor <= self.key.ring_epoch
            || successor >= SEQUENCE_LIMIT
            || cutoff < self.published
            || cutoff >= SEQUENCE_LIMIT
        {
            return Err(Error::Cutover);
        }
        if self.planned.is_some_and(|p| p != (successor, cutoff)) {
            return Err(Error::Cutover);
        }
        self.planned = Some((successor, cutoff));
        Ok(())
    }
    pub fn freeze(&mut self) -> Result<Freeze, Error> {
        self.boundary()?;
        if let Some(frozen) = self.frozen {
            return Ok(frozen);
        }
        let (successor, cutoff) = self.planned.ok_or(Error::Cutover)?;
        if self.published != cutoff {
            return Err(Error::Cutover);
        }
        let frozen = Freeze {
            bank: self.key,
            successor_ring_epoch: successor,
            cutover: cutoff,
            membership_version: self.membership_version,
        };
        self.lifecycle = Lifecycle::Frozen;
        self.frozen = Some(frozen);
        Ok(frozen)
    }
    /// Consumes authenticated quiescence for this exact subscription.
    /// Process-exit based detach is intentionally a separate future authority.
    pub fn detach(&mut self, evidence: Quiesced) -> Result<(), Error> {
        self.boundary()?;
        let index = self.member_index(evidence.key)?;
        let member = self.members[index].unwrap();
        let w = evidence.watermarks;
        if w.read != w.released
            || w.read > self.published
            || w.applied > w.read
            || w.released < member.released
        {
            return Err(Error::Cursor);
        }
        let version = self.next_version()?;
        self.members[index] = None;
        self.membership_version = version;
        self.cached_min = None;
        Ok(())
    }
}

/// A logical borrow, consumed exactly once after the last payload access.
#[derive(Debug)]
pub struct ReadLease {
    key: SubscriptionKey,
    sequence: u64,
}
impl ReadLease {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}
#[derive(Debug)]
pub struct Quiesced {
    key: SubscriptionKey,
    watermarks: Watermarks,
}
impl Quiesced {
    pub fn key(&self) -> SubscriptionKey {
        self.key
    }
    pub fn watermarks(&self) -> Watermarks {
        self.watermarks
    }
}
#[derive(Debug, Clone, Copy, Default)]
struct Completion {
    sequence: u64,
    released: bool,
    applied: bool,
}
#[derive(Debug)]
pub struct Reader<const B: usize> {
    key: SubscriptionKey,
    first: u64,
    watermarks: Watermarks,
    observed_published: u64,
    slots: [Completion; B],
    outstanding: usize,
    quiescing: bool,
}
impl<const B: usize> Reader<B> {
    pub fn new(key: SubscriptionKey, baseline: u64) -> Result<Self, Error> {
        if !key.valid() {
            return Err(Error::Identity);
        }
        if B == 0 || B > 1 << 20 {
            return Err(Error::BorrowCapacity);
        }
        if baseline >= SEQUENCE_LIMIT - 1 {
            return Err(Error::SequenceExhausted);
        }
        Ok(Self {
            key,
            first: baseline + 1,
            watermarks: Watermarks {
                read: baseline,
                applied: baseline,
                released: baseline,
            },
            observed_published: baseline,
            slots: [Completion::default(); B],
            outstanding: 0,
            quiescing: false,
        })
    }
    pub fn watermarks(&self) -> Watermarks {
        self.watermarks
    }
    pub fn outstanding(&self) -> usize {
        self.outstanding
    }
    fn index(&self, sequence: u64) -> usize {
        ((sequence - self.first) % B as u64) as usize
    }
    pub fn borrow_next(&mut self, published: u64) -> Result<Option<ReadLease>, Error> {
        if self.quiescing {
            return Err(Error::NotActive);
        }
        if published >= SEQUENCE_LIMIT {
            return Err(Error::SequenceExhausted);
        }
        if published < self.observed_published {
            return Err(Error::Cursor);
        }
        self.observed_published = published;
        if published == self.watermarks.read {
            return Ok(None);
        }
        let sequence = self.watermarks.read + 1;
        let completed = self.watermarks.applied.min(self.watermarks.released);
        if sequence - completed > B as u64 {
            return Err(Error::BorrowCapacity);
        }
        let index = self.index(sequence);
        self.slots[index] = Completion {
            sequence,
            released: false,
            applied: false,
        };
        self.watermarks.read = sequence;
        self.outstanding += 1;
        Ok(Some(ReadLease {
            key: self.key,
            sequence,
        }))
    }
    pub fn release(&mut self, lease: ReadLease) -> Result<u64, Error> {
        if lease.key != self.key
            || lease.sequence < self.first
            || lease.sequence > self.watermarks.read
        {
            return Err(Error::Identity);
        }
        let index = self.index(lease.sequence);
        if self.slots[index].sequence != lease.sequence || self.slots[index].released {
            return Err(Error::Cursor);
        }
        self.slots[index].released = true;
        self.outstanding -= 1;
        while self.watermarks.released < self.watermarks.read {
            let next = self.watermarks.released + 1;
            let slot = self.slots[self.index(next)];
            if slot.sequence != next || !slot.released {
                break;
            }
            self.watermarks.released = next;
        }
        Ok(self.watermarks.released)
    }
    pub fn apply(&mut self, sequence: u64) -> Result<u64, Error> {
        if sequence < self.first || sequence > self.watermarks.read {
            return Err(Error::Cursor);
        }
        if sequence <= self.watermarks.applied {
            return Ok(self.watermarks.applied);
        }
        let index = self.index(sequence);
        if self.slots[index].sequence != sequence {
            return Err(Error::Cursor);
        }
        self.slots[index].applied = true;
        while self.watermarks.applied < self.watermarks.read {
            let next = self.watermarks.applied + 1;
            let slot = self.slots[self.index(next)];
            if slot.sequence != next || !slot.applied {
                break;
            }
            self.watermarks.applied = next;
        }
        Ok(self.watermarks.applied)
    }
    pub fn start_quiesce(&mut self) {
        self.quiescing = true;
    }
    pub fn quiesced(&self) -> Result<Quiesced, Error> {
        if !self.quiescing {
            return Err(Error::NotQuiescing);
        }
        if self.outstanding != 0 {
            return Err(Error::OutstandingRead);
        }
        Ok(Quiesced {
            key: self.key,
            watermarks: self.watermarks,
        })
    }
}
