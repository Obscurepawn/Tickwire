//! Explicit supervisor grants, live feed dependencies and reconciled private state.
#![forbid(unsafe_code)]
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    rc::Rc,
};
use tickwire_account::{Admission, Scope};
use tickwire_account_gateway::{AuthorityError, Gate};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_runtime::authority::GateProvider;
use tickwire_venue::{CanonicalCommand, CommandBody, Hash, Product};
pub mod bootstrap;
pub const MAX_STRATEGIES: usize = 1024;
pub const MAX_FEEDS: usize = 256;
pub const MAX_FEED_MARKS: usize = 1024;
pub const MAX_REQUIRED_FEEDS: usize = 8;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Identity,
    Stale,
    Conflict,
    Capacity,
    Busy,
    Clock,
    Denied,
    Unreconciled,
}
pub type Result<T> = core::result::Result<T, Error>;
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Stopped,
    ManageOnly,
    Trading,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Versions {
    pub config_revision: u64,
    pub risk_policy_version: u64,
    pub order_config_version: u64,
    pub catalog_version: u64,
    pub catalog_hash: Hash,
}
#[derive(Debug, Clone)]
pub struct Config {
    pub scope: Scope,
    pub product: Product,
    pub executor_id: [u8; 16],
    pub owner_generation: u64,
    pub writer_epoch: u64,
    pub domain: ClockDomain,
    pub versions: Versions,
    pub max_strategies: usize,
    pub max_feeds: usize,
    pub marks_per_feed: usize,
}
/// Authenticated supervisor input. isolation_record names its actual isolation
/// evidence; this local reducer does not establish process isolation itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnerLease {
    pub executor_id: [u8; 16],
    pub owner_generation: u64,
    pub writer_epoch: u64,
    pub lease_epoch: u64,
    pub domain: ClockDomain,
    pub valid_until_ns: u64,
    pub isolation_record: Hash,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Actions {
    pub submit: bool,
    pub cancel: bool,
    pub query: bool,
    pub amend: bool,
}
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct FeedKey {
    pub topic_id: u64,
    pub instrument_id: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct FeedRequirement {
    pub key: FeedKey,
    pub instrument_version: u64,
    pub max_age_ns: u64,
    pub max_sequence_lag: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyGrant {
    pub logical_strategy_id: u64,
    pub instance_id: [u8; 16],
    pub strategy_generation: u64,
    pub authorization_epoch: u64,
    pub config_revision: u64,
    pub valid_until_ns: u64,
    pub actions: Actions,
    pub required_feeds: Vec<FeedRequirement>,
}
/// Executor management permission survives a revoked strategy grant.
/// It authorizes only cancellation and query for the named strategies.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManagementGrant {
    pub authorization_epoch: u64,
    pub config_revision: u64,
    pub valid_until_ns: u64,
    pub cancel: bool,
    pub query: bool,
    pub strategies: Vec<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedUpdate {
    pub key: FeedKey,
    pub instrument_version: u64,
    pub feed_generation: u64,
    pub ring_epoch: u64,
    pub sequence: u64,
    pub published_at: MonoTime,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationRequest {
    pub scope: Scope,
    pub product: Product,
    pub writer_epoch: u64,
    pub input_stream: [u8; 16],
    pub connection_epoch: u64,
    pub gap_generation: u64,
    pub through_sequence: u64,
    pub account_epoch: u64,
    pub ledger_sequence: u64,
    pub baseline_digest: Hash,
    pub history_digest: Hash,
    pub query_coverage_digest: Hash,
    pub valid_until: MonoTime,
}
/// Trusted account/order reconciliation service. Success attests the exact
/// persisted baseline/history and private prefix; reconnect alone cannot do so.
pub trait ReconciliationVerifier {
    fn verify(&mut self, request: &ReconciliationRequest) -> Result<()>;
}
impl From<&tickwire_binance_account::reconciliation::activation::ActivationProof>
    for ReconciliationRequest
{
    fn from(proof: &tickwire_binance_account::reconciliation::activation::ActivationProof) -> Self {
        let binding = proof.binding();
        Self {
            scope: binding.scope.clone(),
            product: binding.product,
            writer_epoch: binding.writer_epoch,
            input_stream: binding.input_stream,
            connection_epoch: binding.private_connection_epoch,
            gap_generation: binding.gap_generation,
            through_sequence: binding.through_sequence,
            account_epoch: binding.account_epoch,
            ledger_sequence: proof.ledger_sequence(),
            baseline_digest: binding.baseline_digest,
            history_digest: proof.history_digest(),
            query_coverage_digest: binding.query_coverage_digest,
            valid_until: proof.valid_until(),
        }
    }
}
impl ReconciliationVerifier
    for tickwire_binance_account::reconciliation::activation::ActivationProof
{
    fn verify(&mut self, request: &ReconciliationRequest) -> Result<()> {
        if &ReconciliationRequest::from(&*self) == request {
            Ok(())
        } else {
            Err(Error::Unreconciled)
        }
    }
}
pub trait PolicyClock {
    fn domain(&self) -> ClockDomain;
    fn sample(&mut self) -> Result<MonoTime>;
}
impl PolicyClock for tickwire_clock_sys::SystemClock {
    fn domain(&self) -> ClockDomain {
        self.domain()
    }
    fn sample(&mut self) -> Result<MonoTime> {
        tickwire_clock_sys::SystemClock::sample(self)
            .map(|s| s.monotonic)
            .map_err(|_| Error::Clock)
    }
}
#[derive(Clone)]
struct GrantSlot {
    epoch: u64,
    current: Option<StrategyGrant>,
}
#[derive(Clone)]
struct Feed {
    generation: u64,
    ring: u64,
    version: u64,
    live: bool,
    marks: VecDeque<(u64, u64)>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrivateFrontier {
    pub connection_epoch: u64,
    pub gap_generation: u64,
    pub through_sequence: u64,
    pub input_stream: Option<[u8; 16]>,
    pub connected: bool,
    pub reconciled: bool,
}
#[derive(Clone)]
struct Private {
    epoch: u64,
    gap: u64,
    stream: Option<[u8; 16]>,
    through: u64,
    connected: bool,
    reconciled: Option<ReconciliationRequest>,
    revocation: u64,
}
#[derive(Clone)]
struct State {
    config: Config,
    control_revision: u64,
    mode: Mode,
    last_mono: Option<u64>,
    faulted: bool,
    lease_epoch: u64,
    owner: Option<OwnerLease>,
    management_epoch: u64,
    management: Option<ManagementGrant>,
    strategies: BTreeMap<u64, GrantSlot>,
    feeds: BTreeMap<FeedKey, Feed>,
    private: Private,
}
/// Handles share one state. Only the authenticated supervisor and ordered input
/// owners call the mutation methods; the gate has no default permission.
#[derive(Clone)]
pub struct ControlPlane(Rc<RefCell<State>>);
impl ControlPlane {
    pub fn new(config: Config) -> Result<Self> {
        if config.scope.risk_domain_id == [0; 16]
            || config.scope.venue == 0
            || config.scope.account == 0
            || config.executor_id == [0; 16]
            || config.owner_generation == 0
            || config.writer_epoch == 0
            || config.max_strategies == 0
            || config.max_strategies > MAX_STRATEGIES
            || config.max_feeds == 0
            || config.max_feeds > MAX_FEEDS
            || config.marks_per_feed == 0
            || config.marks_per_feed > MAX_FEED_MARKS
            || !versions_valid(config.versions)
        {
            return Err(Error::Invalid);
        }
        Ok(Self(Rc::new(RefCell::new(State {
            config,
            control_revision: 0,
            mode: Mode::Stopped,
            last_mono: None,
            faulted: false,
            lease_epoch: 0,
            owner: None,
            management_epoch: 0,
            management: None,
            strategies: BTreeMap::new(),
            feeds: BTreeMap::new(),
            private: Private {
                epoch: 0,
                gap: 0,
                stream: None,
                through: 0,
                connected: false,
                reconciled: None,
                revocation: 0,
            },
        }))))
    }
    pub fn configuration(&self) -> Result<Config> {
        Ok(self.0.try_borrow().map_err(|_| Error::Busy)?.config.clone())
    }
    /// Isolated preflight state. Mutations here never affect live feed/private state.
    pub fn detached_copy(&self) -> Result<Self> {
        Ok(Self(Rc::new(RefCell::new(
            self.0.try_borrow().map_err(|_| Error::Busy)?.clone(),
        ))))
    }
    /// One-way local fault closure, used after an uncertain control persistence result.
    pub fn halt(&self) -> Result<()> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        s.faulted = true;
        s.mode = Mode::Stopped;
        Ok(())
    }
    pub fn provider<C: PolicyClock>(&self, clock: C) -> Result<PolicyGate<C>> {
        if clock.domain() != self.0.try_borrow().map_err(|_| Error::Busy)?.config.domain {
            return Err(Error::Identity);
        }
        Ok(PolicyGate {
            plane: self.clone(),
            clock,
        })
    }
    pub fn observer(&self) -> NodeObserver {
        NodeObserver {
            plane: self.clone(),
        }
    }
    pub fn set_mode(&self, revision: u64, mode: Mode) -> Result<()> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        if revision < s.control_revision || revision == 0 {
            return Err(Error::Stale);
        }
        if revision == s.control_revision {
            return if mode == s.mode {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        s.control_revision = revision;
        s.mode = mode;
        Ok(())
    }
    pub fn set_versions(&self, revision: u64, versions: Versions) -> Result<()> {
        if !versions_valid(versions) {
            return Err(Error::Invalid);
        }
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        if revision <= s.control_revision
            || versions.config_revision <= s.config.versions.config_revision
        {
            return Err(Error::Stale);
        }
        s.control_revision = revision;
        s.config.versions = versions;
        Ok(())
    }
    pub fn install_owner_lease(&self, lease: OwnerLease) -> Result<()> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        if lease.executor_id != s.config.executor_id
            || lease.owner_generation != s.config.owner_generation
            || lease.writer_epoch != s.config.writer_epoch
            || lease.domain != s.config.domain
            || lease.isolation_record == [0; 32]
            || lease.valid_until_ns == 0
            || lease.lease_epoch == 0
        {
            return Err(Error::Identity);
        }
        if lease.lease_epoch <= s.lease_epoch {
            return if s.owner == Some(lease) {
                Ok(())
            } else {
                Err(Error::Stale)
            };
        }
        s.lease_epoch = lease.lease_epoch;
        s.owner = Some(lease);
        Ok(())
    }
    /// Authenticated control-plane transition. The new isolated owner lease is
    /// installed atomically, but all strategy/management grants and reconciliation
    /// are revoked. Trading remains stopped until explicitly configured again.
    pub fn replace_writer(&self, revision: u64, lease: OwnerLease) -> Result<()> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        if revision <= s.control_revision
            || lease.writer_epoch <= s.config.writer_epoch
            || lease.owner_generation < s.config.owner_generation
            || lease.lease_epoch <= s.lease_epoch
        {
            return Err(Error::Stale);
        }
        if lease.executor_id != s.config.executor_id
            || lease.domain != s.config.domain
            || lease.isolation_record == [0; 32]
            || lease.valid_until_ns == 0
            || s.last_mono.is_some_and(|n| lease.valid_until_ns <= n)
        {
            return Err(Error::Identity);
        }
        let revocation = s.private.revocation.checked_add(1).ok_or(Error::Capacity)?;
        s.control_revision = revision;
        s.config.writer_epoch = lease.writer_epoch;
        s.config.owner_generation = lease.owner_generation;
        s.lease_epoch = lease.lease_epoch;
        s.owner = Some(lease);
        s.mode = Mode::Stopped;
        for grant in s.strategies.values_mut() {
            grant.current = None;
        }
        s.management = None;
        s.private.reconciled = None;
        s.private.revocation = revocation;
        Ok(())
    }
    pub fn revoke_owner(&self, lease_epoch: u64) -> Result<()> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        if lease_epoch < s.lease_epoch || lease_epoch == 0 {
            return Err(Error::Stale);
        }
        s.lease_epoch = lease_epoch;
        s.owner = None;
        Ok(())
    }
    pub fn install_strategy_grant(&self, grant: StrategyGrant) -> Result<()> {
        if grant.logical_strategy_id == 0
            || grant.instance_id == [0; 16]
            || grant.strategy_generation == 0
            || grant.authorization_epoch == 0
            || grant.config_revision == 0
            || grant.valid_until_ns == 0
            || grant.required_feeds.len() > MAX_REQUIRED_FEEDS
            || ((grant.actions.submit || grant.actions.amend) && grant.required_feeds.is_empty())
            || grant.required_feeds.iter().any(|r| {
                r.key.topic_id == 0
                    || r.key.instrument_id == 0
                    || r.instrument_version == 0
                    || r.max_age_ns == 0
            })
            || grant
                .required_feeds
                .windows(2)
                .any(|w| w[0].key >= w[1].key)
        {
            return Err(Error::Invalid);
        }
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        if let Some(old) = s.strategies.get(&grant.logical_strategy_id) {
            if grant.authorization_epoch <= old.epoch {
                return if old.current.as_ref() == Some(&grant) {
                    Ok(())
                } else {
                    Err(Error::Stale)
                };
            }
        } else if s.strategies.len() >= s.config.max_strategies {
            return Err(Error::Capacity);
        }
        s.strategies.insert(
            grant.logical_strategy_id,
            GrantSlot {
                epoch: grant.authorization_epoch,
                current: Some(grant),
            },
        );
        Ok(())
    }
    pub fn revoke_strategy(&self, strategy: u64, authorization_epoch: u64) -> Result<()> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        if strategy == 0 || authorization_epoch == 0 {
            return Err(Error::Invalid);
        }
        if s.strategies
            .get(&strategy)
            .is_some_and(|old| authorization_epoch < old.epoch)
        {
            return Err(Error::Stale);
        }
        if !s.strategies.contains_key(&strategy) && s.strategies.len() >= s.config.max_strategies {
            return Err(Error::Capacity);
        }
        s.strategies.insert(
            strategy,
            GrantSlot {
                epoch: authorization_epoch,
                current: None,
            },
        );
        Ok(())
    }
    /// Trusted local owner fault: revoke the current grant without inventing an
    /// authorization epoch. Reinstallation still requires a newer supervisor epoch.
    pub fn stop_strategy(&self, strategy: u64) -> Result<()> {
        if strategy == 0 {
            return Err(Error::Invalid);
        }
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        if let Some(slot) = s.strategies.get_mut(&strategy) {
            slot.current = None;
        }
        Ok(())
    }
    pub fn install_management_grant(&self, grant: ManagementGrant) -> Result<()> {
        if grant.authorization_epoch == 0
            || grant.config_revision == 0
            || grant.valid_until_ns == 0
            || (!grant.cancel && !grant.query)
            || grant.strategies.is_empty()
            || grant.strategies.len() > MAX_STRATEGIES
            || grant.strategies[0] == 0
            || grant.strategies.windows(2).any(|w| w[0] >= w[1])
        {
            return Err(Error::Invalid);
        }
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        if grant.authorization_epoch <= s.management_epoch {
            return if s.management.as_ref() == Some(&grant) {
                Ok(())
            } else {
                Err(Error::Stale)
            };
        }
        s.management_epoch = grant.authorization_epoch;
        s.management = Some(grant);
        Ok(())
    }
    pub fn revoke_management(&self, epoch: u64) -> Result<()> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        if epoch == 0 || epoch < s.management_epoch {
            return Err(Error::Stale);
        }
        s.management_epoch = epoch;
        s.management = None;
        Ok(())
    }
    pub fn update_feed(&self, update: FeedUpdate) -> Result<()> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        if update.key.topic_id == 0
            || update.key.instrument_id == 0
            || update.instrument_version == 0
            || update.feed_generation == 0
            || update.ring_epoch == 0
            || update.sequence == 0
            || update.published_at.domain() != s.config.domain
        {
            return Err(Error::Identity);
        }
        let capacity = s.config.marks_per_feed;
        if !s.feeds.contains_key(&update.key) {
            if s.feeds.len() >= s.config.max_feeds {
                return Err(Error::Capacity);
            }
            s.feeds.insert(
                update.key,
                Feed {
                    generation: update.feed_generation,
                    ring: update.ring_epoch,
                    version: update.instrument_version,
                    live: true,
                    marks: VecDeque::new(),
                },
            );
        }
        let feed = s.feeds.get_mut(&update.key).unwrap();
        if update.feed_generation < feed.generation || update.ring_epoch < feed.ring {
            return Err(Error::Stale);
        }
        if update.feed_generation > feed.generation || update.ring_epoch > feed.ring {
            feed.generation = update.feed_generation;
            feed.ring = update.ring_epoch;
            feed.version = update.instrument_version;
            feed.marks.clear();
            feed.live = true;
        }
        if update.instrument_version != feed.version {
            feed.live = false;
            return Err(Error::Identity);
        }
        if let Some(&(seq, at)) = feed.marks.back() {
            if update.sequence == seq && update.published_at.nanos() == at {
                return Ok(());
            }
            if update.sequence <= seq || update.published_at.nanos() < at {
                feed.live = false;
                return Err(Error::Stale);
            }
        }
        // A gap cannot be cleared by another publication from the same generation.
        if !feed.live {
            return Err(Error::Stale);
        }
        feed.marks
            .push_back((update.sequence, update.published_at.nanos()));
        if feed.marks.len() > capacity {
            feed.marks.pop_front();
        }
        Ok(())
    }
    pub fn invalidate_feed(&self, key: FeedKey, generation: u64, ring: u64) -> Result<()> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        let f = s.feeds.get_mut(&key).ok_or(Error::Identity)?;
        if generation != f.generation || ring != f.ring {
            return Err(Error::Stale);
        }
        f.live = false;
        Ok(())
    }
    pub fn private_connected(&self, epoch: u64) -> Result<()> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        let p = &mut s.private;
        if epoch == 0 || epoch < p.epoch {
            return Err(Error::Stale);
        }
        if epoch == p.epoch {
            p.connected = true;
            return Ok(());
        }
        p.revocation = p.revocation.checked_add(1).ok_or(Error::Capacity)?;
        p.gap = 0;
        p.epoch = epoch;
        p.connected = true;
        p.reconciled = None;
        Ok(())
    }
    /// Store the private source's actual gap identity, never a phase counter.
    pub fn private_gap(&self, epoch: u64, generation: u64) -> Result<()> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        let p = &mut s.private;
        if epoch == 0 || epoch != p.epoch || generation == 0 || generation < p.gap {
            return Err(Error::Stale);
        }
        p.revocation = p.revocation.checked_add(1).ok_or(Error::Capacity)?;
        p.reconciled = None;
        p.connected = false;
        p.gap = generation;
        Ok(())
    }
    pub fn private_disconnected(&self, epoch: u64) -> Result<()> {
        self.private_uncertain(epoch)?;
        self.0
            .try_borrow_mut()
            .map_err(|_| Error::Busy)?
            .private
            .connected = false;
        Ok(())
    }
    pub fn private_source(&self, token: &tickwire_binance_private::GapToken) -> Result<()> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        let scope = token.scope();
        if scope.risk_domain_id != s.config.scope.risk_domain_id
            || scope.venue != s.config.scope.venue
            || scope.account != s.config.scope.account
            || scope.environment != s.config.scope.environment
            || token.generation() == 0
            || token.connection_epoch() != s.private.epoch
        {
            return Err(Error::Identity);
        }
        let p = &mut s.private;
        if token.generation() < p.gap {
            return Err(Error::Stale);
        }
        if token.generation() != p.gap {
            p.revocation = p.revocation.checked_add(1).ok_or(Error::Capacity)?;
            p.gap = token.generation();
            p.reconciled = None;
        }
        Ok(())
    }
    /// Continuous processed physical-WAL prefix. A batch may contain registration,
    /// raw REST responses and child effects, so sequence need not advance by one.
    pub fn input_applied(&self, input_stream: [u8; 16], sequence: u64) -> Result<()> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        let p = &mut s.private;
        if sequence == 0 || input_stream == [0; 16] {
            return Err(Error::Identity);
        }
        if p.stream.is_some_and(|stream| stream != input_stream) {
            p.reconciled = None;
            p.revocation = p.revocation.checked_add(1).ok_or(Error::Capacity)?;
            return Err(Error::Identity);
        }
        p.stream = Some(input_stream);
        if sequence == p.through {
            return Ok(());
        }
        if sequence < p.through {
            p.reconciled = None;
            p.revocation = p.revocation.checked_add(1).ok_or(Error::Capacity)?;
            return Err(Error::Unreconciled);
        }
        p.through = sequence;
        Ok(())
    }
    pub fn private_uncertain(&self, epoch: u64) -> Result<()> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        if epoch == 0 || s.private.epoch != epoch {
            return Err(Error::Stale);
        }
        s.private.reconciled = None;
        s.private.revocation = s.private.revocation.checked_add(1).ok_or(Error::Capacity)?;
        Ok(())
    }
    pub fn private_frontier(&self) -> Result<PrivateFrontier> {
        let s = self.0.try_borrow().map_err(|_| Error::Busy)?;
        Ok(PrivateFrontier {
            connection_epoch: s.private.epoch,
            gap_generation: s.private.gap,
            through_sequence: s.private.through,
            input_stream: s.private.stream,
            connected: s.private.connected,
            reconciled: s.private.reconciled.is_some(),
        })
    }
    pub fn complete_reconciliation(
        &self,
        request: ReconciliationRequest,
        verifier: &mut impl ReconciliationVerifier,
    ) -> Result<()> {
        let revocation = {
            let s = self.0.try_borrow().map_err(|_| Error::Busy)?;
            check_reconciliation(&s, &request)?;
            s.private.revocation
        };
        verifier.verify(&request)?;
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        if s.private.revocation != revocation {
            return Err(Error::Unreconciled);
        }
        check_reconciliation(&s, &request)?;
        s.private.stream = Some(request.input_stream);
        s.private.reconciled = Some(request);
        Ok(())
    }
}
pub struct PolicyGate<C> {
    plane: ControlPlane,
    clock: C,
}
impl<C: PolicyClock> GateProvider for PolicyGate<C> {
    fn current(
        &mut self,
        command: &CanonicalCommand,
        admission: &Admission,
    ) -> core::result::Result<Gate, AuthorityError> {
        let now = match self.clock.sample() {
            Ok(now) => now,
            Err(_) => {
                if let Ok(mut s) = self.plane.0.try_borrow_mut() {
                    s.faulted = true;
                }
                return Err(AuthorityError::Denied);
            }
        };
        self.plane
            .evaluate(command, admission, now)
            .map_err(|_| AuthorityError::Denied)
    }
}
impl ControlPlane {
    fn evaluate(
        &self,
        command: &CanonicalCommand,
        admission: &Admission,
        now: MonoTime,
    ) -> Result<Gate> {
        let mut s = self.0.try_borrow_mut().map_err(|_| Error::Busy)?;
        if s.faulted
            || now.domain() != s.config.domain
            || s.last_mono.is_some_and(|n| now.nanos() < n)
        {
            s.faulted = true;
            return Err(Error::Clock);
        }
        s.last_mono = Some(now.nanos());
        let c = command.command();
        let r = &c.context.request;
        let strategy = &c.context.strategy;
        if admission.command_digest() != command.digest()
            || admission.intent().epoch != r.intent_exec_epoch
            || admission.intent().intent_id != r.intent_id
            || r.risk_domain_id != s.config.scope.risk_domain_id
            || r.venue != s.config.scope.venue
            || r.account != s.config.scope.account
            || r.environment != s.config.scope.environment
            || r.product != s.config.product
            || r.intent_exec_epoch != s.config.writer_epoch
            || r.config_revision != s.config.versions.config_revision
            || now.nanos() >= r.deadline
            || now.nanos() < strategy.created_mono_ns
            || s.mode == Mode::Stopped
        {
            return Err(Error::Denied);
        }
        let owner = s.owner.ok_or(Error::Denied)?;
        if now.nanos() >= owner.valid_until_ns {
            return Err(Error::Denied);
        }
        let action = action(&c.body);
        let administrative = matches!(action, Action::Cancel | Action::Query);
        let mut until = owner.valid_until_ns;
        let grant = s
            .strategies
            .get(&strategy.logical_strategy_id)
            .and_then(|g| g.current.as_ref())
            .filter(|g| {
                g.instance_id == strategy.instance_id
                    && g.strategy_generation == r.strategy_generation
                    && g.authorization_epoch == strategy.authorization_epoch
                    && g.config_revision == r.config_revision
                    && now.nanos() < g.valid_until_ns
                    && allowed(g.actions, action)
            });
        let management = s.management.as_ref().filter(|g| {
            administrative
                && g.authorization_epoch == strategy.authorization_epoch
                && g.config_revision == r.config_revision
                && now.nanos() < g.valid_until_ns
                && g.strategies
                    .binary_search(&strategy.logical_strategy_id)
                    .is_ok()
                && match action {
                    Action::Cancel => g.cancel,
                    Action::Query => g.query,
                    _ => false,
                }
        });
        if grant.is_none() && management.is_none() {
            return Err(Error::Denied);
        }
        if let Some(g) = grant {
            until = until.min(g.valid_until_ns);
        } else if let Some(g) = management {
            until = until.min(g.valid_until_ns);
        }
        let market_valid = if administrative {
            false
        } else {
            if s.mode != Mode::Trading
                || strategy.risk_policy_version != s.config.versions.risk_policy_version
                || strategy.order_config_version != s.config.versions.order_config_version
                || c.context.instrument.catalog_version != s.config.versions.catalog_version
                || c.context.instrument.catalog_hash != s.config.versions.catalog_hash
            {
                return Err(Error::Denied);
            }
            let private = s
                .private
                .reconciled
                .as_ref()
                .filter(|p| {
                    s.private.connected
                        && p.connection_epoch == s.private.epoch
                        && p.gap_generation == s.private.gap
                        && p.account_epoch == admission.account_epoch()
                        && p.ledger_sequence <= admission.ledger_sequence()
                        && now.nanos() < p.valid_until.nanos()
                })
                .ok_or(Error::Unreconciled)?;
            until = until.min(private.valid_until.nanos());
            until = until.min(market(
                &s,
                command,
                grant.ok_or(Error::Denied)?,
                now.nanos(),
            )?);
            true
        };
        Ok(Gate {
            now: now.nanos(),
            lease_until: until,
            writer_epoch: s.config.writer_epoch,
            strategy_generation: r.strategy_generation,
            config_revision: r.config_revision,
            owner_isolated: true,
            action_permitted: true,
            market_valid,
        })
    }
}
fn check_reconciliation(s: &State, r: &ReconciliationRequest) -> Result<()> {
    if r.scope != s.config.scope
        || r.product != s.config.product
        || r.input_stream == [0; 16]
        || s.private
            .stream
            .is_some_and(|stream| stream != r.input_stream)
        || !s.private.connected
        || r.connection_epoch != s.private.epoch
        || r.writer_epoch != s.config.writer_epoch
        || r.gap_generation == 0
        || r.gap_generation != s.private.gap
        || r.through_sequence != s.private.through
        || r.account_epoch == 0
        || r.baseline_digest == [0; 32]
        || r.history_digest == [0; 32]
        || r.query_coverage_digest == [0; 32]
        || r.valid_until.domain() != s.config.domain
        || r.valid_until.nanos() == 0
        || s.last_mono.is_some_and(|n| r.valid_until.nanos() <= n)
    {
        return Err(Error::Unreconciled);
    }
    Ok(())
}
fn versions_valid(v: Versions) -> bool {
    v.config_revision != 0
        && v.risk_policy_version != 0
        && v.order_config_version != 0
        && v.catalog_version != 0
        && v.catalog_hash != [0; 32]
}
#[derive(Clone, Copy)]
enum Action {
    Submit,
    Cancel,
    Query,
    Amend,
}
fn action(c: &CommandBody) -> Action {
    match c {
        CommandBody::Cancel(_) | CommandBody::CancelList(_) | CommandBody::CancelAlgo(_) => {
            Action::Cancel
        }
        CommandBody::Query(_) => Action::Query,
        CommandBody::AmendKeepPriority { .. } | CommandBody::Modify(_) => Action::Amend,
        _ => Action::Submit,
    }
}
fn allowed(a: Actions, k: Action) -> bool {
    match k {
        Action::Submit => a.submit,
        Action::Cancel => a.cancel,
        Action::Query => a.query,
        Action::Amend => a.amend,
    }
}
fn market(s: &State, c: &CanonicalCommand, g: &StrategyGrant, now: u64) -> Result<u64> {
    let refs = c.command().context.triggers.as_slice();
    if refs.is_empty() || g.required_feeds.is_empty() || refs.len() != g.required_feeds.len() {
        return Err(Error::Denied);
    }
    let mut valid_until = u64::MAX;
    for requirement in &g.required_feeds {
        let mut matching = refs.iter().filter(|r| {
            r.topic_id == requirement.key.topic_id
                && r.instrument_id == requirement.key.instrument_id
        });
        let r = matching.next().ok_or(Error::Denied)?;
        if matching.next().is_some() {
            return Err(Error::Denied);
        }
        let f = s.feeds.get(&requirement.key).ok_or(Error::Denied)?;
        if !f.live
            || f.generation != r.feed_generation
            || f.ring != r.ring_epoch
            || f.version != r.instrument_version
            || f.version != requirement.instrument_version
            || r.observed_mono_ns > now
        {
            return Err(Error::Denied);
        }
        let (last_seq, _) = f.marks.back().ok_or(Error::Denied)?;
        if r.sequence > *last_seq || last_seq - r.sequence > requirement.max_sequence_lag {
            return Err(Error::Denied);
        }
        let (_, published) = f
            .marks
            .iter()
            .find(|(seq, _)| *seq == r.sequence)
            .ok_or(Error::Denied)?;
        let expires = published
            .checked_add(requirement.max_age_ns)
            .ok_or(Error::Clock)?;
        if *published > r.observed_mono_ns || *published > now || now >= expires {
            return Err(Error::Denied);
        }
        valid_until = valid_until.min(expires);
    }
    Ok(valid_until)
}

pub struct NodeObserver {
    plane: ControlPlane,
}
impl tickwire_runtime::node::Observer for NodeObserver {
    fn private_source(&mut self, token: &tickwire_binance_private::GapToken) -> bool {
        self.plane.private_source(token).is_ok()
    }
    fn input_applied(
        &mut self,
        input: tickwire_binance_account::InputKey,
        account: &tickwire_account::Account,
    ) -> bool {
        if self
            .plane
            .input_applied(input.stream, input.sequence)
            .is_err()
        {
            return false;
        }
        if account.frozen() {
            let Ok(frontier) = self.plane.private_frontier() else {
                return false;
            };
            if frontier.reconciled {
                return self
                    .plane
                    .private_uncertain(frontier.connection_epoch)
                    .is_ok();
            }
        }
        true
    }
    fn private_state(&mut self, state: tickwire_binance_private::SessionStatus) -> bool {
        use tickwire_binance_private::Phase;
        if state.connection_epoch == 0 {
            return true;
        }
        let Ok(frontier) = self.plane.private_frontier() else {
            return false;
        };
        if state.connection_epoch < frontier.connection_epoch {
            return true;
        }
        if state.connection_epoch > frontier.connection_epoch
            && self
                .plane
                .private_connected(state.connection_epoch)
                .is_err()
        {
            return false;
        }
        if state.phase == Phase::Receiving {
            return self.plane.private_connected(state.connection_epoch).is_ok();
        }
        if state.connection_epoch > frontier.connection_epoch || frontier.connected {
            return self
                .plane
                .private_disconnected(state.connection_epoch)
                .is_ok();
        }
        true
    }
    fn private_gap(&mut self, gap: &tickwire_binance_private::GapNotice) -> bool {
        let Ok(s) = self.plane.0.try_borrow() else {
            return false;
        };
        let scope = gap.token.scope();
        if scope.risk_domain_id != s.config.scope.risk_domain_id
            || scope.venue != s.config.scope.venue
            || scope.account != s.config.scope.account
            || scope.environment != s.config.scope.environment
        {
            return false;
        }
        let current = s.private.epoch;
        drop(s);
        if gap.token.connection_epoch() < current {
            return true;
        }
        if gap.token.connection_epoch() > current
            && self
                .plane
                .private_connected(gap.token.connection_epoch())
                .is_err()
        {
            return false;
        }
        self.plane
            .private_gap(gap.token.connection_epoch(), gap.token.generation())
            .is_ok()
    }
    fn private_applied(
        &mut self,
        epoch: u64,
        outcome: tickwire_binance_account::Outcome,
        account: &tickwire_account::Account,
    ) -> bool {
        use tickwire_binance_account::Outcome;
        let quarantined = match outcome {
            Outcome::Applied { .. } => false,
            Outcome::Quarantined { .. } => true,
        };
        let Ok(frontier) = self.plane.private_frontier() else {
            return false;
        };
        if epoch != frontier.connection_epoch || !frontier.connected {
            return false;
        }
        if quarantined || account.frozen() {
            return self.plane.private_uncertain(epoch).is_ok();
        }
        true
    }
    fn health(
        &mut self,
        health: tickwire_runtime::node::Health,
        now: tickwire_binance_clock::LocalTime,
    ) -> bool {
        let Ok(mut s) = self.plane.0.try_borrow_mut() else {
            return false;
        };
        if now.monotonic.domain() != s.config.domain
            || s.last_mono.is_some_and(|n| now.monotonic.nanos() < n)
        {
            s.faulted = true;
            return false;
        }
        s.last_mono = Some(now.monotonic.nanos());
        if (health.account_frozen || health.sender_halted) && s.private.reconciled.is_some() {
            s.private.reconciled = None;
            let Some(next) = s.private.revocation.checked_add(1) else {
                return false;
            };
            s.private.revocation = next;
        }
        true
    }
}

#[cfg(test)]
mod tests;
