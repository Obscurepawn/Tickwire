//! Supervisor allocations and single-owner local request quota consumption.
#![forbid(unsafe_code)]

pub use tickwire_clock::{ClockDomain, MonoTime};

pub const MAX_WINDOWS: usize = 256;
pub const MAX_CONSUMERS: usize = 256;
pub const MAX_GRANTS: usize = 4096;
pub const MAX_COSTS: usize = 16;
pub const MAX_ATTEMPTS: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Domain,
    ClockRegression,
    Expired,
    Frozen,
    Cooldown,
    Capacity,
    Exhausted,
    Conflict,
    AlreadyIssued,
    Identity,
    Store,
    Poisoned,
    Unsupported,
}
pub type Result<T> = core::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScopeKind {
    Ip,
    Uid,
    Account,
    Connection,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BucketId {
    pub namespace: [u8; 16],
    pub scope: ScopeKind,
    pub subject: [u8; 16],
    pub pool: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct WindowKey {
    pub bucket: BucketId,
    pub window: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    Normal,
    Recovery,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cost {
    pub window: WindowKey,
    pub units: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowConfig {
    pub key: WindowKey,
    pub revision: u64,
    pub starts_ns: u64,
    pub ends_ns: u64,
    pub limit: u64,
    pub recovery_reserve: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumerId {
    pub instance: [u8; 16],
    pub generation: u64,
}
/// Producer identity separates independently numbered request owners.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttemptId {
    pub producer: [u8; 16],
    pub sequence: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consumed {
    New,
    Duplicate,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    revision: u64,
    supervisor_generation: u64,
    domain: Option<ClockDomain>,
    last_ns: u64,
    next_grant: u64,
    windows: Vec<WindowConfig>,
    consumers: Vec<ConsumerRecord>,
    grants: Vec<GrantRecord>,
}
impl Snapshot {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn supervisor_generation(&self) -> u64 {
        self.supervisor_generation
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct ConsumerRecord {
    id: ConsumerId,
    supervisor_generation: u64,
    domain: ClockDomain,
    lease_until_ns: u64,
    closed: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct GrantRecord {
    id: u64,
    request_id: u64,
    owner: ConsumerRecord,
    class: Class,
    costs: Vec<Cost>,
    windows: Vec<WindowConfig>,
    expires_ns: u64,
    retained: Vec<u64>,
    reclaimed: bool,
}
/// Implementations must atomically persist `next` before acknowledging success.
/// An error may follow a committed write; callers must reopen, never retry blindly.
pub trait QuotaStore {
    fn load(&mut self) -> Result<Snapshot>;
    fn compare_exchange(&mut self, expected_revision: u64, next: &Snapshot) -> Result<()>;
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MockFailure {
    BeforeCommit,
    AfterCommit,
}
#[derive(Default)]
struct MockState {
    snapshot: Snapshot,
    failure: Option<MockFailure>,
}
/// Explicit in-memory persistence/CAS model, shared by cloned test handles.
#[derive(Clone, Default)]
pub struct MockQuotaStore(std::rc::Rc<std::cell::RefCell<MockState>>);
impl MockQuotaStore {
    pub fn fail_next(&self, failure: MockFailure) {
        self.0.borrow_mut().failure = Some(failure);
    }
    pub fn snapshot(&self) -> Snapshot {
        self.0.borrow().snapshot.clone()
    }
}
impl QuotaStore for MockQuotaStore {
    fn load(&mut self) -> Result<Snapshot> {
        Ok(self.snapshot())
    }
    fn compare_exchange(&mut self, revision: u64, next: &Snapshot) -> Result<()> {
        let mut state = self.0.borrow_mut();
        if state.snapshot.revision != revision || revision.checked_add(1) != Some(next.revision) {
            return Err(Error::Store);
        }
        let failure = state.failure.take();
        if failure == Some(MockFailure::BeforeCommit) {
            return Err(Error::Store);
        }
        state.snapshot = next.clone();
        if failure == Some(MockFailure::AfterCommit) {
            return Err(Error::Store);
        }
        Ok(())
    }
}
pub struct Coordinator<S: QuotaStore> {
    store: S,
    state: Snapshot,
    domain: ClockDomain,
    last_ns: u64,
    poisoned: bool,
}
pub struct ExecLimiter {
    owner: ConsumerRecord,
    grants: Vec<LocalGrant>,
    attempts: Vec<Attempt>,
    last_ns: u64,
    cooldown_until_ns: u64,
    frozen: bool,
}
struct LocalGrant {
    record: GrantRecord,
    remaining: Vec<u64>,
}
#[derive(Clone)]
struct Debit {
    grant: usize,
    cost: usize,
    units: u64,
}
struct Attempt {
    id: AttemptId,
    digest: [u8; 32],
    class: Class,
    costs: Vec<Cost>,
    debits: Vec<Debit>,
}
/// A single delivery capability. No Clone or public deserialization.
pub struct Grant {
    record: GrantRecord,
}
/// A validated allocation plan; it confers no spending capability.
pub struct PreparedGrant {
    revision: u64,
    supervisor: u64,
    domain: ClockDomain,
    record: GrantRecord,
}
/// A validated lease plan; only commit produces a renewal capability.
pub struct PreparedWindows {
    revision: u64,
    supervisor: u64,
    domain: ClockDomain,
    next: Snapshot,
}
pub struct PreparedRenewal {
    revision: u64,
    supervisor: u64,
    domain: ClockDomain,
    index: usize,
    renewal: LeaseRenewal,
}
pub struct LeaseRenewal {
    previous: ConsumerRecord,
    next: ConsumerRecord,
}
impl Grant {
    pub fn id(&self) -> u64 {
        self.record.id
    }
    pub fn costs(&self) -> &[Cost] {
        &self.record.costs
    }
}
/// Created only by consuming an ExecLimiter; no sender survives this receipt.
pub struct StoppedReceipt {
    owner: ConsumerRecord,
    final_grants: Vec<(GrantRecord, Vec<u64>)>,
}
fn validate_costs(costs: &[Cost]) -> Result<()> {
    if costs.is_empty()
        || costs.len() > MAX_COSTS
        || costs.iter().any(|c| c.units == 0)
        || costs.windows(2).any(|w| w[0].window >= w[1].window)
    {
        return Err(Error::Invalid);
    }
    Ok(())
}
fn valid_window(w: &WindowConfig) -> bool {
    w.key.bucket.namespace != [0; 16]
        && w.key.bucket.subject != [0; 16]
        && w.key.bucket.pool != 0
        && w.key.window != 0
        && w.revision != 0
        && w.starts_ns < w.ends_ns
        && w.limit > 0
        && w.recovery_reserve <= w.limit
}
fn same_consumer(a: &ConsumerRecord, b: &ConsumerRecord) -> bool {
    a.id == b.id && a.supervisor_generation == b.supervisor_generation && a.domain == b.domain
}
fn accounted(state: &Snapshot, key: WindowKey, class: Option<Class>) -> Result<u64> {
    state
        .grants
        .iter()
        .filter(|g| class.is_none_or(|c| c == g.class))
        .try_fold(0u64, |total, g| {
            let held = g
                .costs
                .iter()
                .zip(&g.retained)
                .find(|(c, _)| c.window == key)
                .map_or(0, |(_, n)| *n);
            total.checked_add(held).ok_or(Error::Capacity)
        })
}
impl<S: QuotaStore> Coordinator<S> {
    pub fn open(mut store: S, generation: u64, now: MonoTime) -> Result<Self> {
        let state = store.load()?;
        if generation == 0 || generation <= state.supervisor_generation {
            return Err(Error::Identity);
        }
        if state.domain.is_some_and(|d| d != now.domain()) {
            return Err(Error::Domain);
        }
        if now.nanos() < state.last_ns {
            return Err(Error::ClockRegression);
        }
        let mut this = Self {
            store,
            state,
            domain: now.domain(),
            last_ns: now.nanos(),
            poisoned: false,
        };
        let mut next = this.state.clone();
        next.supervisor_generation = generation;
        next.domain = Some(now.domain());
        this.commit(next)?;
        Ok(this)
    }
    fn observe(&mut self, now: MonoTime) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if now.domain() != self.domain {
            self.poisoned = true;
            return Err(Error::Domain);
        }
        if now.nanos() < self.last_ns {
            self.poisoned = true;
            return Err(Error::ClockRegression);
        }
        self.last_ns = now.nanos();
        Ok(())
    }
    fn commit(&mut self, mut next: Snapshot) -> Result<()> {
        next.revision = self.state.revision.checked_add(1).ok_or(Error::Capacity)?;
        next.last_ns = self.last_ns;
        if self
            .store
            .compare_exchange(self.state.revision, &next)
            .is_err()
        {
            self.poisoned = true;
            return Err(Error::Store);
        }
        self.state = next;
        Ok(())
    }
    pub fn prepare_windows(
        &mut self,
        windows: &[WindowConfig],
        now: MonoTime,
    ) -> Result<PreparedWindows> {
        self.observe(now)?;
        if windows.is_empty() || windows.len() > MAX_WINDOWS {
            return Err(Error::Invalid);
        }
        let mut next = self.state.clone();
        for config in windows {
            if !valid_window(config) || config.ends_ns <= now.nanos() {
                return Err(Error::Invalid);
            }
            if let Some(old) = next.windows.iter().find(|w| w.key == config.key) {
                if old != config {
                    return Err(Error::Conflict);
                }
                continue;
            }
            if next.windows.len() >= MAX_WINDOWS {
                return Err(Error::Capacity);
            }
            if next.windows.iter().any(|w| {
                w.key.bucket == config.key.bucket
                    && (w.key.window >= config.key.window || w.ends_ns > config.starts_ns)
            }) {
                return Err(Error::Conflict);
            }
            next.windows.push(config.clone());
        }
        Ok(PreparedWindows {
            revision: self.state.revision,
            supervisor: self.state.supervisor_generation,
            domain: self.domain,
            next,
        })
    }
    pub fn commit_windows(&mut self, prepared: PreparedWindows) -> Result<()> {
        self.check_prepared(prepared.revision, prepared.supervisor, prepared.domain)?;
        if prepared.next == self.state {
            return Ok(());
        }
        self.commit(prepared.next)
    }
    pub fn add_window(&mut self, config: WindowConfig, now: MonoTime) -> Result<()> {
        let prepared = self.prepare_windows(&[config], now)?;
        self.commit_windows(prepared)
    }
    pub fn register_consumer(
        &mut self,
        id: ConsumerId,
        lease_until_ns: u64,
        now: MonoTime,
    ) -> Result<ExecLimiter> {
        self.observe(now)?;
        if id.instance == [0; 16] || id.generation == 0 || lease_until_ns <= now.nanos() {
            return Err(Error::Invalid);
        }
        let index = self
            .state
            .consumers
            .iter()
            .position(|c| c.id.instance == id.instance);
        if index.is_some_and(|i| self.state.consumers[i].id.generation >= id.generation) {
            return Err(Error::Identity);
        }
        if index.is_none() && self.state.consumers.len() >= MAX_CONSUMERS {
            return Err(Error::Capacity);
        }
        let owner = ConsumerRecord {
            id,
            supervisor_generation: self.state.supervisor_generation,
            domain: self.domain,
            lease_until_ns,
            closed: false,
        };
        let mut next = self.state.clone();
        match index {
            Some(i) => next.consumers[i] = owner.clone(),
            None => next.consumers.push(owner.clone()),
        }
        self.commit(next)?;
        Ok(ExecLimiter {
            owner,
            grants: Vec::new(),
            attempts: Vec::new(),
            last_ns: now.nanos(),
            cooldown_until_ns: 0,
            frozen: false,
        })
    }
    pub fn prepare_renew_consumer(
        &mut self,
        consumer: &ExecLimiter,
        until_ns: u64,
        now: MonoTime,
    ) -> Result<PreparedRenewal> {
        self.observe(now)?;
        let index = self
            .state
            .consumers
            .iter()
            .position(|c| c == &consumer.owner)
            .ok_or(Error::Identity)?;
        if consumer.frozen
            || consumer.owner.supervisor_generation != self.state.supervisor_generation
        {
            return Err(Error::Identity);
        }
        if now.nanos() >= consumer.owner.lease_until_ns {
            return Err(Error::Expired);
        }
        if until_ns <= consumer.owner.lease_until_ns {
            return Err(Error::AlreadyIssued);
        }
        let mut owner = consumer.owner.clone();
        owner.lease_until_ns = until_ns;
        Ok(PreparedRenewal {
            revision: self.state.revision,
            supervisor: self.state.supervisor_generation,
            domain: self.domain,
            index,
            renewal: LeaseRenewal {
                previous: consumer.owner.clone(),
                next: owner,
            },
        })
    }
    pub fn commit_renew_consumer(&mut self, prepared: PreparedRenewal) -> Result<LeaseRenewal> {
        self.check_prepared(prepared.revision, prepared.supervisor, prepared.domain)?;
        let mut next = self.state.clone();
        next.consumers[prepared.index] = prepared.renewal.next.clone();
        self.commit(next)?;
        Ok(prepared.renewal)
    }
    pub fn renew_consumer(
        &mut self,
        consumer: &ExecLimiter,
        until_ns: u64,
        now: MonoTime,
    ) -> Result<LeaseRenewal> {
        let prepared = self.prepare_renew_consumer(consumer, until_ns, now)?;
        self.commit_renew_consumer(prepared)
    }
    pub fn prepare_issue(
        &mut self,
        request_id: u64,
        consumer: &ExecLimiter,
        class: Class,
        costs: &[Cost],
        expires_ns: u64,
        now: MonoTime,
    ) -> Result<PreparedGrant> {
        self.observe(now)?;
        validate_costs(costs)?;
        if request_id == 0 {
            return Err(Error::Invalid);
        }
        if self.state.grants.iter().any(|g| g.request_id == request_id) {
            return Err(Error::AlreadyIssued);
        }
        if consumer.frozen
            || consumer.owner.supervisor_generation != self.state.supervisor_generation
            || !self.state.consumers.contains(&consumer.owner)
        {
            return Err(Error::Identity);
        }
        if expires_ns <= now.nanos() || expires_ns > consumer.owner.lease_until_ns {
            return Err(Error::Expired);
        }
        if self.state.grants.len() >= MAX_GRANTS {
            return Err(Error::Capacity);
        }
        let mut windows = Vec::with_capacity(costs.len());
        for cost in costs {
            let w = self
                .state
                .windows
                .iter()
                .find(|w| w.key == cost.window)
                .ok_or(Error::Invalid)?;
            if expires_ns <= w.starts_ns || expires_ns > w.ends_ns {
                return Err(Error::Expired);
            }
            let all = accounted(&self.state, w.key, None)?;
            if cost.units > w.limit.saturating_sub(all) {
                return Err(Error::Exhausted);
            }
            let normal = accounted(&self.state, w.key, Some(Class::Normal))?;
            if class == Class::Normal
                && cost.units > (w.limit - w.recovery_reserve).saturating_sub(normal)
            {
                return Err(Error::Exhausted);
            }
            windows.push(w.clone());
        }
        let id = self
            .state
            .next_grant
            .checked_add(1)
            .ok_or(Error::Capacity)?;
        let record = GrantRecord {
            id,
            request_id,
            owner: consumer.owner.clone(),
            class,
            costs: costs.to_vec(),
            windows,
            expires_ns,
            retained: costs.iter().map(|c| c.units).collect(),
            reclaimed: false,
        };
        Ok(PreparedGrant {
            revision: self.state.revision,
            supervisor: self.state.supervisor_generation,
            domain: self.domain,
            record,
        })
    }
    fn check_prepared(&self, revision: u64, supervisor: u64, domain: ClockDomain) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if revision != self.state.revision
            || supervisor != self.state.supervisor_generation
            || domain != self.domain
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    pub fn commit_issue(&mut self, prepared: PreparedGrant) -> Result<Grant> {
        self.check_prepared(prepared.revision, prepared.supervisor, prepared.domain)?;
        let mut next = self.state.clone();
        next.next_grant = prepared.record.id;
        next.grants.push(prepared.record.clone());
        self.commit(next)?;
        Ok(Grant {
            record: prepared.record,
        })
    }
    pub fn issue(
        &mut self,
        request_id: u64,
        consumer: &ExecLimiter,
        class: Class,
        costs: &[Cost],
        expires_ns: u64,
        now: MonoTime,
    ) -> Result<Grant> {
        let prepared = self.prepare_issue(request_id, consumer, class, costs, expires_ns, now)?;
        self.commit_issue(prepared)
    }
    pub fn reclaim(&mut self, receipt: StoppedReceipt, now: MonoTime) -> Result<()> {
        self.observe(now)?;
        let mut next = self.state.clone();
        for (original, spent) in receipt.final_grants {
            let stored = next
                .grants
                .iter_mut()
                .find(|g| g.id == original.id)
                .ok_or(Error::Identity)?;
            if stored != &original
                || !same_consumer(&stored.owner, &receipt.owner)
                || stored.reclaimed
                || spent.len() != stored.costs.len()
                || spent.iter().zip(&stored.costs).any(|(n, c)| *n > c.units)
            {
                return Err(Error::Conflict);
            }
            stored.retained = spent;
            stored.reclaimed = true;
        }
        if let Some(c) = next
            .consumers
            .iter_mut()
            .find(|c| same_consumer(c, &receipt.owner))
        {
            c.closed = true;
        }
        self.commit(next)
    }
    pub fn accounted(&self, key: WindowKey) -> Result<u64> {
        if !self.state.windows.iter().any(|w| w.key == key) {
            return Err(Error::Invalid);
        }
        accounted(&self.state, key, None)
    }
    pub fn into_store(self) -> S {
        self.store
    }
}
impl ExecLimiter {
    fn observe(&mut self, now: MonoTime) -> Result<()> {
        if self.frozen {
            return Err(Error::Frozen);
        }
        if now.domain() != self.owner.domain {
            self.frozen = true;
            return Err(Error::Domain);
        }
        if now.nanos() < self.last_ns {
            self.frozen = true;
            return Err(Error::ClockRegression);
        }
        self.last_ns = now.nanos();
        if now.nanos() >= self.owner.lease_until_ns {
            return Err(Error::Expired);
        }
        Ok(())
    }
    pub fn install(&mut self, grant: Grant, now: MonoTime) -> Result<()> {
        self.observe(now)?;
        if !same_consumer(&grant.record.owner, &self.owner)
            || grant.record.expires_ns > self.owner.lease_until_ns
        {
            return Err(Error::Identity);
        }
        if self.grants.iter().any(|g| g.record.id == grant.record.id) {
            return Err(Error::Conflict);
        }
        if self.grants.len() >= MAX_GRANTS {
            return Err(Error::Capacity);
        }
        if now.nanos() >= grant.record.expires_ns {
            return Err(Error::Expired);
        }
        let remaining = grant.record.costs.iter().map(|c| c.units).collect();
        self.grants.push(LocalGrant {
            record: grant.record,
            remaining,
        });
        Ok(())
    }
    pub fn renew(&mut self, renewal: LeaseRenewal, now: MonoTime) -> Result<()> {
        self.observe(now)?;
        if renewal.previous != self.owner
            || renewal.next.lease_until_ns <= self.owner.lease_until_ns
        {
            return Err(Error::Identity);
        }
        self.owner = renewal.next;
        Ok(())
    }
    fn active(&self, debit: &Debit, now_ns: u64) -> bool {
        let g = &self.grants[debit.grant];
        let w = &g.record.windows[debit.cost];
        now_ns >= w.starts_ns && now_ns < w.ends_ns && now_ns < g.record.expires_ns
    }
    pub fn consume(
        &mut self,
        id: AttemptId,
        digest: [u8; 32],
        class: Class,
        costs: &[Cost],
        now: MonoTime,
    ) -> Result<Consumed> {
        self.observe(now)?;
        if id.sequence == 0 || id.producer == [0; 16] || digest == [0; 32] {
            return Err(Error::Invalid);
        }
        validate_costs(costs)?;
        if let Some(old) = self.attempts.iter().find(|a| a.id == id) {
            if old.digest != digest || old.class != class || old.costs != costs {
                self.frozen = true;
                return Err(Error::Conflict);
            }
            if now.nanos() < self.cooldown_until_ns {
                return Err(Error::Cooldown);
            }
            if old.debits.iter().any(|d| !self.active(d, now.nanos())) {
                return Err(Error::Expired);
            }
            return Ok(Consumed::Duplicate);
        }
        if now.nanos() < self.cooldown_until_ns {
            return Err(Error::Cooldown);
        }
        if self.attempts.len() >= MAX_ATTEMPTS {
            return Err(Error::Capacity);
        }
        let mut debits = Vec::with_capacity(costs.len());
        for cost in costs {
            let mut needed = cost.units;
            let mut inactive = false;
            for (gi, g) in self.grants.iter().enumerate() {
                if g.record.class != class {
                    continue;
                }
                let Some(ci) = g.record.costs.iter().position(|c| c.window == cost.window) else {
                    continue;
                };
                let mut debit = Debit {
                    grant: gi,
                    cost: ci,
                    units: 0,
                };
                if !self.active(&debit, now.nanos()) {
                    inactive = true;
                    continue;
                }
                debit.units = needed.min(g.remaining[ci]);
                needed -= debit.units;
                if debit.units > 0 {
                    debits.push(debit);
                }
                if needed == 0 {
                    break;
                }
            }
            if needed > 0 {
                return Err(if inactive {
                    Error::Expired
                } else {
                    Error::Exhausted
                });
            }
        }
        for d in &debits {
            self.grants[d.grant].remaining[d.cost] -= d.units;
        }
        self.attempts.push(Attempt {
            id,
            digest,
            class,
            costs: costs.to_vec(),
            debits,
        });
        Ok(Consumed::New)
    }
    pub fn stop(self) -> StoppedReceipt {
        let final_grants = self
            .grants
            .into_iter()
            .map(|g| {
                let spent = g
                    .record
                    .costs
                    .iter()
                    .zip(&g.remaining)
                    .map(|(c, r)| c.units - r)
                    .collect();
                (g.record, spent)
            })
            .collect();
        StoppedReceipt {
            owner: self.owner,
            final_grants,
        }
    }
    /// Accounting only; eligibility is checked by consume at the current time.
    pub fn remaining(&self, key: WindowKey, class: Class) -> u64 {
        self.grants
            .iter()
            .filter(|g| g.record.class == class)
            .map(|g| {
                g.record
                    .costs
                    .iter()
                    .position(|c| c.window == key)
                    .map_or(0, |i| g.remaining[i])
            })
            .sum()
    }
    pub fn cooldown(&mut self, until_ns: u64, now: MonoTime) -> Result<()> {
        self.observe(now)?;
        if until_ns <= now.nanos() {
            return Err(Error::Invalid);
        }
        self.cooldown_until_ns = self.cooldown_until_ns.max(until_ns);
        Ok(())
    }
    pub fn freeze(&mut self) {
        self.frozen = true;
    }
    pub fn is_frozen(&self) -> bool {
        self.frozen
    }
    pub fn identity(&self) -> ConsumerId {
        self.owner.id
    }
    pub fn clock_domain(&self) -> ClockDomain {
        self.owner.domain
    }
}

#[cfg(test)]
mod preparation_tests {
    use super::*;
    fn now(n: u64) -> MonoTime {
        MonoTime::new(ClockDomain::new([1; 16], 1).unwrap(), n)
    }
    fn window(number: u64, start: u64, end: u64) -> WindowConfig {
        WindowConfig {
            key: WindowKey {
                bucket: BucketId {
                    namespace: [2; 16],
                    scope: ScopeKind::Account,
                    subject: [3; 16],
                    pool: 1,
                },
                window: number,
            },
            revision: 1,
            starts_ns: start,
            ends_ns: end,
            limit: 100,
            recovery_reserve: 10,
        }
    }
    #[test]
    fn prepared_windows_are_atomic_and_stale_tickets_cannot_commit() {
        let mut c = Coordinator::open(MockQuotaStore::default(), 1, now(0)).unwrap();
        c.add_window(window(1, 0, 100), now(0)).unwrap();
        let before = c.state.clone();
        assert!(
            c.prepare_windows(&[window(2, 100, 200), window(3, 150, 250)], now(1))
                .is_err()
        );
        assert_eq!(c.state, before);
        let p = c.prepare_windows(&[window(2, 100, 200)], now(1)).unwrap();
        assert_eq!(c.state, before);
        c.register_consumer(
            ConsumerId {
                instance: [4; 16],
                generation: 1,
            },
            300,
            now(1),
        )
        .unwrap();
        assert_eq!(c.commit_windows(p), Err(Error::Conflict));
    }
    #[test]
    fn allocation_and_renewal_only_publish_from_committed_tickets() {
        let mut c = Coordinator::open(MockQuotaStore::default(), 1, now(0)).unwrap();
        let w = window(1, 0, 100);
        c.add_window(w.clone(), now(0)).unwrap();
        let mut local = c
            .register_consumer(
                ConsumerId {
                    instance: [4; 16],
                    generation: 1,
                },
                90,
                now(0),
            )
            .unwrap();
        let p = c
            .prepare_issue(
                1,
                &local,
                Class::Normal,
                &[Cost {
                    window: w.key,
                    units: 10,
                }],
                80,
                now(1),
            )
            .unwrap();
        assert_eq!(c.accounted(w.key).unwrap(), 0);
        assert_eq!(local.remaining(w.key, Class::Normal), 0);
        local.install(c.commit_issue(p).unwrap(), now(1)).unwrap();
        assert_eq!(c.accounted(w.key).unwrap(), 10);
        let p = c.prepare_renew_consumer(&local, 190, now(2)).unwrap();
        assert_eq!(local.owner.lease_until_ns, 90);
        local
            .renew(c.commit_renew_consumer(p).unwrap(), now(2))
            .unwrap();
        assert_eq!(local.owner.lease_until_ns, 190);
    }
}
