//! Single-owner local control authority. Operators publish consecutive JSON files
//! by atomic rename. Accepted commands are synchronized before changing live gates.
use crate::{Result, invalid};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};
use tickwire_binance_clock::SystemClock;
use tickwire_binance_quota::{Endpoint, bootstrap as quota_config};
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_journal::{Journal, store::FileStore};
use tickwire_quota::{self as quota, Coordinator, MockQuotaStore};
use tickwire_runtime_policy::{self as policy, ControlPlane, bootstrap as policy_config};

const SCHEMA: u32 = 1;
const MAX_COMMAND_BYTES: usize = 32 * 1024;
const MAX_RECORDS: usize = 16384;
const MAX_TOTAL_BYTES: usize = 64 * 1024 * 1024;
pub mod market;
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub inbox_directory: PathBuf,
    pub journal_directory: PathBuf,
    pub stream_epoch: [u8; 16],
    pub recover: bool,
    pub segment_max_bytes: u64,
    pub maximum_commands: usize,
    pub maximum_per_poll: usize,
    /// Explicit lease for this new process session. It grants no request quota.
    #[serde(default)]
    pub startup_consumer_lease_until_ns: Option<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub scope: policy_config::Scope,
    pub product: policy_config::Product,
    pub executor_id: [u8; 16],
    pub owner_generation: u64,
    pub writer_epoch: u64,
    pub domain: policy_config::Domain,
}
impl Binding {
    pub fn from_config(config: &policy_config::Config) -> Self {
        Self {
            scope: config.scope.clone(),
            product: config.product,
            executor_id: config.executor_id,
            owner_generation: config.owner_generation,
            writer_epoch: config.writer_epoch,
            domain: config.domain,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    Mode {
        revision: u64,
        mode: policy::Mode,
    },
    Versions {
        revision: u64,
        versions: policy::Versions,
    },
    OwnerLease {
        lease: policy_config::Lease,
    },
    RevokeOwner {
        lease_epoch: u64,
    },
    StrategyGrant {
        grant: policy::StrategyGrant,
    },
    RevokeStrategy {
        strategy: u64,
        authorization_epoch: u64,
    },
    ManagementGrant {
        grant: policy::ManagementGrant,
    },
    RevokeManagement {
        authorization_epoch: u64,
    },
    QuotaInstall {
        grant: quota_config::Grant,
    },
    QuotaWindows {
        windows: Vec<quota_config::Window>,
    },
    QuotaPolicy {
        generation: u64,
        starts_ns: u64,
        ends_ns: u64,
        rules: Vec<quota_config::Rule>,
    },
    QuotaRenew {
        until_ns: u64,
    },
    QuotaCooldown {
        until_ns: u64,
    },
    QuotaFreeze,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub schema: u32,
    pub sequence: u64,
    pub command_id: [u8; 16],
    pub binding: Binding,
    pub action: Action,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Bootstrap {
        schema: u32,
        digest: [u8; 32],
        at_ns: u64,
    },
    Session {
        number: u64,
        at_ns: u64,
        lease_until_ns: u64,
    },
    MarketDecision {
        session: u64,
        at_ns: u64,
        decision: tickwire_release::Object,
    },
    Command {
        session: u64,
        at_ns: u64,
        command: Command,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    New,
    Duplicate,
}
enum Prepared {
    Policy,
    Grant(quota::PreparedGrant),
    Renew(quota::PreparedRenewal),
    Windows(quota::PreparedWindows),
    QuotaPolicy(tickwire_binance_quota::PreparedPolicy),
    Cooldown,
    Freeze,
}
struct State {
    binding: Binding,
    binding_bytes: Vec<u8>,
    domain: ClockDomain,
    plane: ControlPlane,
    quota: quota_config::Config,
    coordinator: Coordinator<MockQuotaStore>,
    session: u64,
    lease_until_ns: u64,
    cooldown_until_ns: u64,
    quota_frozen: bool,
    next_sequence: u64,
    ids: BTreeMap<[u8; 16], (u64, [u8; 32])>,
    last_ns: u64,
    market_history: market::history::History,
}
impl State {
    fn new(
        policy: &policy_config::Config,
        quota: &quota_config::Config,
        plane: ControlPlane,
        at_ns: u64,
    ) -> Result<(Self, Endpoint)> {
        let domain = policy.domain().map_err(debug_error)?;
        let built = quota
            .build(MockQuotaStore::default(), MonoTime::new(domain, at_ns))
            .map_err(debug_error)?;
        let binding = Binding::from_config(policy);
        Ok((
            Self {
                binding_bytes: serde_json::to_vec(&binding)?,
                binding,
                domain,
                plane,
                quota: quota.clone(),
                coordinator: built.coordinator,
                session: 1,
                lease_until_ns: quota.consumer_lease_until_ns,
                cooldown_until_ns: 0,
                quota_frozen: false,
                next_sequence: 1,
                ids: BTreeMap::new(),
                last_ns: at_ns,
                market_history: market::history::History::default(),
            },
            built.endpoint,
        ))
    }
    fn check_time(&self, now: MonoTime) -> Result<()> {
        if now.domain() != self.domain || now.nanos() < self.last_ns {
            return Err(invalid("control clock domain or regression"));
        }
        Ok(())
    }
    fn check_binding(&self) -> Result<()> {
        let live = self.plane.configuration().map_err(debug_error)?;
        let environment = match self.binding.scope.environment {
            policy_config::Environment::Production => tickwire_venue::Environment::Production,
            policy_config::Environment::Test => tickwire_venue::Environment::Test,
        };
        let product = match self.binding.product {
            policy_config::Product::Spot => tickwire_venue::Product::Spot,
            policy_config::Product::LinearPerpetual => tickwire_venue::Product::LinearPerpetual,
            policy_config::Product::LinearFuture => tickwire_venue::Product::LinearFuture,
        };
        if live.scope.risk_domain_id != self.binding.scope.risk_domain_id
            || live.scope.venue != self.binding.scope.venue
            || live.scope.account != self.binding.scope.account
            || live.scope.environment != environment
            || live.product != product
            || live.executor_id != self.binding.executor_id
            || live.owner_generation != self.binding.owner_generation
            || live.writer_epoch != self.binding.writer_epoch
            || live.domain != self.domain
        {
            return Err(invalid("control live owner identity changed"));
        }
        Ok(())
    }
    fn command_digest(&self, c: &Command) -> Result<[u8; 32]> {
        self.check_binding()?;
        if c.schema != SCHEMA
            || c.command_id == [0; 16]
            || c.sequence == 0
            || serde_json::to_vec(&c.binding)? != self.binding_bytes
        {
            return Err(invalid("control command identity"));
        }
        let bytes = serde_json::to_vec(c)?;
        if bytes.len() > MAX_COMMAND_BYTES {
            return Err(invalid("control command bound"));
        }
        Ok(tickwire_recovery::digest(&bytes))
    }
    fn duplicate(&self, c: &Command, digest: [u8; 32]) -> Result<bool> {
        if let Some(old) = self.ids.get(&c.command_id) {
            return if *old == (c.sequence, digest) {
                Ok(true)
            } else {
                Err(invalid("control ID conflict"))
            };
        }
        if c.sequence != self.next_sequence {
            return Err(invalid("control sequence gap or conflict"));
        }
        Ok(false)
    }
    fn prepare(&mut self, c: &Command, endpoint: &Endpoint, now: MonoTime) -> Result<Prepared> {
        self.check_time(now)?;
        let p = match &c.action {
            Action::QuotaWindows { windows } => {
                if windows.is_empty()
                    || self
                        .quota
                        .windows
                        .len()
                        .checked_add(windows.len())
                        .is_none_or(|n| n > quota::MAX_WINDOWS)
                {
                    return Err(invalid("quota window count"));
                }
                for (i, w) in windows.iter().enumerate() {
                    if w.name.is_empty()
                        || w.name.len() > 64
                        || !w.name.is_ascii()
                        || self
                            .quota
                            .windows
                            .iter()
                            .any(|old| old.name == w.name || old.key() == w.key())
                        || windows[..i]
                            .iter()
                            .any(|old| old.name == w.name || old.key() == w.key())
                    {
                        return Err(invalid("quota window name or identity reused"));
                    }
                }
                let mut configs = windows
                    .iter()
                    .map(|w| quota::WindowConfig {
                        key: w.key(),
                        revision: w.revision,
                        starts_ns: w.starts_ns,
                        ends_ns: w.ends_ns,
                        limit: w.limit,
                        recovery_reserve: w.recovery_reserve,
                    })
                    .collect::<Vec<_>>();
                configs.sort_by_key(|w| w.key);
                Prepared::Windows(
                    self.coordinator
                        .prepare_windows(&configs, now)
                        .map_err(debug_error)?,
                )
            }
            Action::QuotaPolicy {
                generation,
                starts_ns,
                ends_ns,
                rules,
            } => {
                let mut candidate = self.quota.clone();
                candidate.policy_generation = *generation;
                candidate.policy_starts_ns = *starts_ns;
                candidate.policy_ends_ns = *ends_ns;
                candidate.rules = rules.clone();
                Prepared::QuotaPolicy(
                    endpoint
                        .prepare_policy(candidate.policy().map_err(debug_error)?, now)
                        .map_err(debug_error)?,
                )
            }
            Action::QuotaInstall { grant } => Prepared::Grant(
                self.coordinator
                    .prepare_issue(
                        grant.request_id,
                        endpoint.consumer(),
                        grant.class.build(),
                        &self
                            .quota
                            .resolve_costs(&grant.costs)
                            .map_err(debug_error)?,
                        grant.expires_ns,
                        now,
                    )
                    .map_err(debug_error)?,
            ),
            Action::QuotaRenew { until_ns } => Prepared::Renew(
                self.coordinator
                    .prepare_renew_consumer(endpoint.consumer(), *until_ns, now)
                    .map_err(debug_error)?,
            ),
            Action::QuotaCooldown { until_ns } => {
                if endpoint.is_frozen()
                    || *until_ns <= now.nanos()
                    || now.nanos() >= self.lease_until_ns
                {
                    return Err(invalid("quota cooldown state"));
                }
                Prepared::Cooldown
            }
            Action::QuotaFreeze => Prepared::Freeze,
            _ => {
                apply_policy(
                    &self.plane.detached_copy().map_err(debug_error)?,
                    &self.binding,
                    &c.action,
                    now.nanos(),
                )?;
                Prepared::Policy
            }
        };
        Ok(p)
    }
    fn publish(
        &mut self,
        c: &Command,
        digest: [u8; 32],
        p: Prepared,
        endpoint: &mut Endpoint,
        now: MonoTime,
    ) -> Result<()> {
        match p {
            Prepared::Windows(p) => {
                self.coordinator.commit_windows(p).map_err(debug_error)?;
                if let Action::QuotaWindows { windows } = &c.action {
                    self.quota.windows.extend(windows.iter().cloned());
                }
            }
            Prepared::QuotaPolicy(p) => {
                endpoint.commit_policy(p).map_err(debug_error)?;
                if let Action::QuotaPolicy {
                    generation,
                    starts_ns,
                    ends_ns,
                    rules,
                } = &c.action
                {
                    self.quota.policy_generation = *generation;
                    self.quota.policy_starts_ns = *starts_ns;
                    self.quota.policy_ends_ns = *ends_ns;
                    self.quota.rules = rules.clone();
                }
            }
            Prepared::Grant(p) => endpoint
                .install(self.coordinator.commit_issue(p).map_err(debug_error)?, now)
                .map_err(debug_error)?,
            Prepared::Renew(p) => {
                endpoint
                    .renew(
                        self.coordinator
                            .commit_renew_consumer(p)
                            .map_err(debug_error)?,
                        now,
                    )
                    .map_err(debug_error)?;
                if let Action::QuotaRenew { until_ns } = c.action {
                    self.lease_until_ns = until_ns;
                }
            }
            Prepared::Cooldown => {
                if let Action::QuotaCooldown { until_ns } = c.action {
                    endpoint.cooldown(until_ns, now).map_err(debug_error)?;
                    self.cooldown_until_ns = self.cooldown_until_ns.max(until_ns);
                }
            }
            Prepared::Freeze => {
                endpoint.freeze();
                self.quota_frozen = true;
            }
            Prepared::Policy => apply_policy(&self.plane, &self.binding, &c.action, now.nanos())?,
        }
        self.ids.insert(c.command_id, (c.sequence, digest));
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| invalid("control sequence exhausted"))?;
        self.last_ns = now.nanos();
        Ok(())
    }
    fn replay_command(
        &mut self,
        c: &Command,
        endpoint: &mut Endpoint,
        now: MonoTime,
    ) -> Result<()> {
        let digest = self.command_digest(c)?;
        if self.duplicate(c, digest)? {
            return Err(invalid("duplicate physical control command"));
        }
        let p = self.prepare(c, endpoint, now)?;
        self.publish(c, digest, p, endpoint, now)
    }
    fn restart(&mut self, number: u64, at_ns: u64, lease_until_ns: u64) -> Result<Endpoint> {
        let now = MonoTime::new(self.domain, at_ns);
        self.check_time(now)?;
        if self.session.checked_add(1) != Some(number) {
            return Err(invalid("control session order"));
        }
        let generation = self
            .quota
            .consumer_generation
            .checked_add(number - 1)
            .ok_or_else(|| invalid("quota consumer generation"))?;
        let local = self
            .coordinator
            .register_consumer(
                quota::ConsumerId {
                    instance: self.quota.consumer_instance,
                    generation,
                },
                lease_until_ns,
                now,
            )
            .map_err(debug_error)?;
        let mut producer = b"tickwire.local-control.producer.v1\0".to_vec();
        producer.extend_from_slice(&self.quota.producer_id);
        producer.extend_from_slice(&number.to_le_bytes());
        let hash = tickwire_recovery::digest(&producer);
        let producer_id = hash[..16].try_into().unwrap();
        let mut endpoint = Endpoint::new(
            local,
            producer_id,
            self.domain,
            self.quota.policy().map_err(debug_error)?,
        )
        .map_err(debug_error)?;
        if self.cooldown_until_ns > at_ns {
            endpoint
                .cooldown(self.cooldown_until_ns, now)
                .map_err(debug_error)?;
        }
        if self.quota_frozen {
            endpoint.freeze();
        }
        self.session = number;
        self.lease_until_ns = lease_until_ns;
        self.last_ns = at_ns;
        Ok(endpoint)
    }
}
fn debug_error(e: impl std::fmt::Debug) -> crate::Error {
    invalid(&format!("control: {e:?}"))
}
fn apply_policy(plane: &ControlPlane, b: &Binding, action: &Action, now: u64) -> Result<()> {
    let result = match action {
        Action::Mode { revision, mode } => plane.set_mode(*revision, *mode),
        Action::Versions { revision, versions } => plane.set_versions(*revision, *versions),
        Action::OwnerLease { lease } => {
            if lease.valid_until_ns <= now {
                return Err(invalid("expired owner lease"));
            }
            plane.install_owner_lease(policy::OwnerLease {
                executor_id: b.executor_id,
                owner_generation: b.owner_generation,
                writer_epoch: b.writer_epoch,
                lease_epoch: lease.lease_epoch,
                domain: b.domain.build().map_err(debug_error)?,
                valid_until_ns: lease.valid_until_ns,
                isolation_record: lease.isolation_record,
            })
        }
        Action::RevokeOwner { lease_epoch } => plane.revoke_owner(*lease_epoch),
        Action::StrategyGrant { grant } => {
            if grant.valid_until_ns <= now {
                return Err(invalid("expired strategy grant"));
            }
            plane.install_strategy_grant(grant.clone())
        }
        Action::RevokeStrategy {
            strategy,
            authorization_epoch,
        } => plane.revoke_strategy(*strategy, *authorization_epoch),
        Action::ManagementGrant { grant } => {
            if grant.valid_until_ns <= now {
                return Err(invalid("expired management grant"));
            }
            plane.install_management_grant(grant.clone())
        }
        Action::RevokeManagement {
            authorization_epoch,
        } => plane.revoke_management(*authorization_epoch),
        _ => return Err(invalid("expected policy command")),
    };
    result.map_err(debug_error)
}

pub struct ControlInbox {
    config: Config,
    state: State,
    journal: Journal<FileStore>,
    clock: SystemClock,
    records: usize,
    bytes: usize,
    poisoned: bool,
    market: Option<market::Controller>,
    market_objects: Option<tickwire_recovery::FileBlobStore>,
    next_market_scan_ns: u64,
}
impl ControlInbox {
    /// `plane` is the fresh bootstrap plane, before any runtime callbacks or grants.
    /// The file WAL is the sole authority for this quota namespace. Reopening an
    /// independent store under the same namespace would violate that ownership.
    pub fn open(
        config: Config,
        policy: &policy_config::Config,
        quota: &quota_config::Config,
        plane: ControlPlane,
        mut clock: SystemClock,
    ) -> Result<(Self, Endpoint)> {
        if config.maximum_commands == 0
            || config.maximum_commands > 8192
            || config.maximum_per_poll == 0
            || config.maximum_per_poll > 32
            || !config.inbox_directory.is_absolute()
            || !config.journal_directory.is_absolute()
            || config.stream_epoch == [0; 16]
        {
            return Err(invalid("control configuration bounds"));
        }
        let meta = std::fs::symlink_metadata(&config.inbox_directory)?;
        if !meta.is_dir() {
            return Err(invalid("control inbox must be a real directory"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o022 != 0 {
                return Err(invalid(
                    "control inbox must not be writable by group or others",
                ));
            }
        }
        let mut config = config;
        config.inbox_directory = std::fs::canonicalize(&config.inbox_directory)?;
        let domain = policy.domain().map_err(debug_error)?;
        if clock.domain() != domain {
            return Err(invalid("control clock domain"));
        }
        let now = clock.sample().map_err(debug_error)?.monotonic;
        let bootstrap = tickwire_recovery::digest(&serde_json::to_vec(&(SCHEMA, policy, quota))?);
        let physical = tickwire_journal::Config {
            stream_epoch: config.stream_epoch,
            domain: policy.scope.risk_domain_id,
            segment_max_bytes: config.segment_max_bytes,
        };
        let store = FileStore::open(&config.journal_directory)?;
        let mut market_objects = None;
        let (mut journal, state, mut endpoint, records, bytes) = if config.recover {
            let (journal, recovery) = Journal::recover(store, None)?;
            if journal.config() != &physical
                || recovery.records.is_empty()
                || recovery.records.len() > MAX_RECORDS
            {
                return Err(invalid("control WAL identity or record bound"));
            }
            let mut total = recovery
                .records
                .iter()
                .try_fold(0usize, |n, r| n.checked_add(r.payload.len()))
                .ok_or_else(|| invalid("control WAL size"))?;
            if total > MAX_TOTAL_BYTES {
                return Err(invalid("control WAL byte bound"));
            }
            let first: Record = decode_record(&recovery.records[0].payload)?;
            let Record::Bootstrap {
                schema,
                digest,
                at_ns,
            } = first
            else {
                return Err(invalid("missing control bootstrap"));
            };
            if schema != SCHEMA || digest != bootstrap {
                return Err(invalid("control bootstrap changed"));
            }
            let (mut state, mut endpoint) = State::new(policy, quota, plane, at_ns)?;
            for (index, r) in recovery.records.iter().enumerate() {
                if r.seq != index as u64 + 1 || r.exec_epoch != policy.writer_epoch {
                    return Err(invalid("control WAL epoch or sequence"));
                }
                if index == 0 {
                    continue;
                }
                match decode_record(&r.payload)? {
                    Record::Session {
                        number,
                        at_ns,
                        lease_until_ns,
                    } => endpoint = state.restart(number, at_ns, lease_until_ns)?,
                    Record::Command {
                        session,
                        at_ns,
                        command,
                    } => {
                        if session != state.session || state.ids.len() >= config.maximum_commands {
                            return Err(invalid("control command session or capacity"));
                        }
                        state.replay_command(
                            &command,
                            &mut endpoint,
                            MonoTime::new(domain, at_ns),
                        )?;
                    }
                    Record::MarketDecision {
                        session,
                        at_ns,
                        decision,
                    } => {
                        decision.validate(market::MAX_DECISION_BYTES as u64)?;
                        total = total
                            .checked_add(decision.bytes as usize)
                            .filter(|n| *n <= MAX_TOTAL_BYTES)
                            .ok_or_else(|| invalid("market decision object budget"))?;
                        if market_objects.is_none() {
                            market_objects = Some(market::open_objects(&config.journal_directory)?);
                        }
                        let decision =
                            market::read_decision(market_objects.as_ref().unwrap(), decision)?;
                        let now = MonoTime::new(domain, at_ns);
                        state.check_time(now)?;
                        state.check_binding()?;
                        if session != state.session
                            || serde_json::to_vec(&decision.binding)? != state.binding_bytes
                        {
                            return Err(invalid("market decision session or binding"));
                        }
                        state.market_history.record(decision, now, 128)?;
                        state.last_ns = at_ns;
                    }
                    Record::Bootstrap { .. } => return Err(invalid("duplicate control bootstrap")),
                }
            }
            (journal, state, endpoint, recovery.records.len(), total)
        } else {
            let (state, endpoint) = State::new(policy, quota, plane, now.nanos())?;
            let mut journal = Journal::create(store, physical)?;
            let payload = serde_json::to_vec(&Record::Bootstrap {
                schema: SCHEMA,
                digest: bootstrap,
                at_ns: now.nanos(),
            })?;
            journal.append_batch(policy.writer_epoch, &[&payload])?;
            (journal, state, endpoint, 1, payload.len())
        };
        let mut state = state;
        let mut records = records;
        let mut bytes = bytes;
        if config.recover {
            let number = state
                .session
                .checked_add(1)
                .ok_or_else(|| invalid("control session exhausted"))?;
            let lease = config
                .startup_consumer_lease_until_ns
                .unwrap_or(state.lease_until_ns);
            // No capability escapes before the new session itself is synchronized.
            endpoint = state.restart(number, now.nanos(), lease)?;
            let payload = serde_json::to_vec(&Record::Session {
                number,
                at_ns: now.nanos(),
                lease_until_ns: lease,
            })?;
            check_storage(records, bytes, payload.len())?;
            journal.append_batch(policy.writer_epoch, &[&payload])?;
            records += 1;
            bytes += payload.len();
        }
        Ok((
            Self {
                config,
                state,
                journal,
                clock,
                records,
                bytes,
                poisoned: false,
                market: None,
                market_objects,
                next_market_scan_ns: 0,
            },
            endpoint,
        ))
    }
    /// Startup approval is explicit; recovered decisions remain historical only.
    pub fn configure_market(
        &mut self,
        config: market::Config,
        sources: Option<crate::market::Sources>,
    ) -> Result<()> {
        if self.market.is_some() || self.poisoned {
            return Err(invalid("market control already configured or poisoned"));
        }
        self.state.check_binding()?;
        if self.market_objects.is_none() {
            self.market_objects = Some(market::open_objects(&self.config.journal_directory)?);
        }
        self.market = Some(market::Controller::new(
            config,
            sources,
            self.state.market_history.clone(),
        )?);
        self.state.market_history = market::history::History::default();
        Ok(())
    }
    pub fn poll_market(
        &mut self,
        endpoint: &mut Endpoint,
        strategy: &mut dyn crate::strategy::AppStrategy,
    ) -> Result<usize> {
        if self.market.is_none() {
            return Ok(0);
        }
        let now = match self.clock.sample() {
            Ok(sample) => sample.monotonic,
            Err(error) => {
                self.poison(endpoint);
                return Err(debug_error(error));
            }
        };
        self.poll_market_at(now, endpoint, strategy)
    }
    fn poll_market_at(
        &mut self,
        now: MonoTime,
        endpoint: &mut Endpoint,
        strategy: &mut dyn crate::strategy::AppStrategy,
    ) -> Result<usize> {
        if self.poisoned || self.state.check_time(now).is_err() {
            self.poison(endpoint);
            return Err(invalid("market control is poisoned or clock regressed"));
        }
        if now.nanos() < self.next_market_scan_ns {
            return Ok(0);
        }
        self.next_market_scan_ns = now.nanos().saturating_add(10_000_000);
        let Some(mut controller) = self.market.take() else {
            return Ok(0);
        };
        let result = (|| {
            if self.poisoned {
                return Err(invalid("control inbox poisoned"));
            }
            self.state.check_binding()?;
            self.state.check_time(now)?;
            let binding = self.state.binding.clone();
            controller.poll(&binding, strategy, now, |request| {
                self.market_boundary(request)
            })
        })();
        self.market = Some(controller);
        let result = result.and_then(|count| {
            let current = self.clock.sample().map_err(debug_error)?.monotonic;
            self.state.check_time(current)?;
            self.state.check_binding()?;
            self.next_market_scan_ns = current.nanos().saturating_add(10_000_000);
            Ok(count)
        });
        if result.is_err() {
            self.poison(endpoint);
        }
        result
    }
    fn market_boundary(
        &mut self,
        request: Option<(&market::Decision, MonoTime)>,
    ) -> Result<MonoTime> {
        let mut current = self.clock.sample().map_err(debug_error)?.monotonic;
        self.state.check_time(current)?;
        self.state.check_binding()?;
        if let Some((decision, admitted_at)) = request {
            self.state.check_time(admitted_at)?;
            if admitted_at.domain() != current.domain() || admitted_at.nanos() > current.nanos() {
                return Err(invalid(
                    "market admission time is not a verified past sample",
                ));
            }
            if serde_json::to_vec(&decision.binding)? != self.state.binding_bytes {
                return Err(invalid("market decision binding"));
            }
            let bytes = serde_json::to_vec(decision)?;
            if bytes.len() > market::MAX_DECISION_BYTES {
                return Err(invalid("market decision object bound"));
            }
            let object = tickwire_release::Object::of(&bytes);
            let payload = serde_json::to_vec(&Record::MarketDecision {
                session: self.state.session,
                at_ns: admitted_at.nanos(),
                decision: object,
            })?;
            let retained = self
                .bytes
                .checked_add(bytes.len())
                .ok_or_else(|| invalid("market decision budget"))?;
            check_storage(self.records, retained, payload.len())?;
            use tickwire_recovery::BlobStore;
            self.market_objects
                .as_mut()
                .ok_or_else(|| invalid("market object store missing"))?
                .put_sync(tickwire_recovery::StoreKey::Object(object.digest), &bytes)?;
            self.journal
                .append_batch(self.state.binding.writer_epoch, &[&payload])?;
            self.records += 1;
            self.bytes = retained + payload.len();
            self.state.last_ns = admitted_at.nanos();
            current = self.clock.sample().map_err(debug_error)?.monotonic;
        }
        self.state.check_time(current)?;
        self.state.check_binding()?;
        Ok(current)
    }
    pub fn next_sequence(&self) -> u64 {
        self.state.next_sequence
    }
    pub fn session(&self) -> u64 {
        self.state.session
    }
    pub fn poll(&mut self, endpoint: &mut Endpoint) -> Result<usize> {
        let result = self.poll_inner(endpoint);
        if result.is_err() {
            self.poison(endpoint);
        }
        result
    }
    fn poll_inner(&mut self, endpoint: &mut Endpoint) -> Result<usize> {
        if self.poisoned {
            return Err(invalid("control inbox poisoned"));
        }
        let mut applied = 0;
        for _ in 0..self.config.maximum_per_poll {
            let path = self
                .config
                .inbox_directory
                .join(format!("{:020}.json", self.next_sequence()));
            match std::fs::symlink_metadata(&path) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                Err(e) => return Err(e.into()),
                Ok(_) => {}
            }
            let raw = crate::config::read_file(&path, MAX_COMMAND_BYTES)?;
            let command: Command = serde_json::from_slice(&raw)?;
            if command.sequence != self.next_sequence() {
                return Err(invalid("control filename sequence mismatch"));
            }
            self.submit(command, endpoint)?;
            applied += 1;
        }
        Ok(applied)
    }
    pub fn submit(&mut self, command: Command, endpoint: &mut Endpoint) -> Result<Applied> {
        let result = self.submit_inner(command, endpoint);
        if result.is_err() {
            self.poison(endpoint);
        }
        result
    }
    fn submit_inner(&mut self, c: Command, endpoint: &mut Endpoint) -> Result<Applied> {
        if self.poisoned {
            return Err(invalid("control inbox poisoned"));
        }
        let digest = self.state.command_digest(&c)?;
        if self.state.duplicate(&c, digest)? {
            return Ok(Applied::Duplicate);
        }
        if self.state.ids.len() >= self.config.maximum_commands {
            return Err(invalid("control command capacity"));
        }
        let now = self.clock.sample().map_err(debug_error)?.monotonic;
        let prepared = self.state.prepare(&c, endpoint, now)?;
        let payload = serde_json::to_vec(&Record::Command {
            session: self.state.session,
            at_ns: now.nanos(),
            command: c.clone(),
        })?;
        check_storage(self.records, self.bytes, payload.len())?;
        self.journal
            .append_batch(self.state.binding.writer_epoch, &[&payload])?;
        self.records += 1;
        self.bytes += payload.len();
        self.state.publish(&c, digest, prepared, endpoint, now)?;
        Ok(Applied::New)
    }
    fn poison(&mut self, endpoint: &mut Endpoint) {
        self.poisoned = true;
        endpoint.freeze();
        let _ = self.state.plane.halt();
    }
}
fn check_storage(records: usize, bytes: usize, new: usize) -> Result<()> {
    if new > MAX_COMMAND_BYTES + 1024
        || records >= MAX_RECORDS
        || bytes.checked_add(new).is_none_or(|n| n > MAX_TOTAL_BYTES)
    {
        return Err(invalid("control WAL bound"));
    }
    Ok(())
}
fn decode_record(bytes: &[u8]) -> Result<Record> {
    if bytes.len() > MAX_COMMAND_BYTES + 1024 {
        return Err(invalid("control record bound"));
    }
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    pub(crate) fn inputs() -> (policy_config::Config, quota_config::Config) {
        let p = policy_config::Config {
            scope: policy_config::Scope {
                risk_domain_id: [1; 16],
                venue: 1,
                environment: policy_config::Environment::Test,
                account: 1,
            },
            product: policy_config::Product::Spot,
            executor_id: [2; 16],
            owner_generation: 1,
            writer_epoch: 1,
            domain: policy_config::Domain {
                process_instance: [3; 16],
                generation: 1,
            },
            versions: policy::Versions {
                config_revision: 1,
                risk_policy_version: 1,
                order_config_version: 1,
                catalog_version: 1,
                catalog_hash: [4; 32],
            },
            max_strategies: 8,
            max_feeds: 8,
            marks_per_feed: 8,
            control_revision: 1,
            mode: policy::Mode::Trading,
            owner_lease: Some(policy_config::Lease {
                lease_epoch: 1,
                valid_until_ns: 900,
                isolation_record: [5; 32],
            }),
            strategy_grants: vec![],
            management_grant: None,
        };
        let q = quota_config::Config {
            supervisor_generation: 1,
            consumer_instance: [6; 16],
            consumer_generation: 1,
            consumer_lease_until_ns: 900,
            producer_id: [7; 16],
            policy_generation: 1,
            policy_starts_ns: 0,
            policy_ends_ns: 1000,
            windows: vec![quota_config::Window {
                name: "api".into(),
                namespace: [8; 16],
                scope: quota_config::Scope::Account,
                subject: [9; 16],
                pool: 1,
                window: 1,
                revision: 1,
                starts_ns: 0,
                ends_ns: 1000,
                limit: 100,
                recovery_reserve: 10,
            }],
            grants: vec![quota_config::Grant {
                request_id: 1,
                class: quota_config::Class::Normal,
                expires_ns: 800,
                costs: vec![quota_config::Cost {
                    window: "api".into(),
                    units: 10,
                }],
            }],
            rules: vec![quota_config::Rule {
                source: quota_config::Source::SpotTest,
                action: quota_config::Action::Time {
                    kind: quota_config::TimeKind::Query,
                },
                class: quota_config::Class::Normal,
                api_weight: 1,
                websocket_messages: 0,
                costs: vec![quota_config::Cost {
                    window: "api".into(),
                    units: 1,
                }],
            }],
        };
        (p, q)
    }
    fn command(p: &policy_config::Config, sequence: u64, action: Action) -> Command {
        Command {
            schema: SCHEMA,
            sequence,
            command_id: [sequence as u8; 16],
            binding: Binding::from_config(p),
            action,
        }
    }
    #[test]
    fn revoke_preflight_does_not_mutate_live_and_replay_preserves_revocation() {
        let (p, q) = inputs();
        let plane = p.build().unwrap();
        let (mut state, mut endpoint) = State::new(&p, &q, plane.clone(), 1).unwrap();
        let command = command(&p, 1, Action::RevokeOwner { lease_epoch: 1 });
        let now = MonoTime::new(state.domain, 2);
        let prepared = state.prepare(&command, &endpoint, now).unwrap();
        let reinstall = Action::OwnerLease {
            lease: p.owner_lease.unwrap(),
        };
        apply_policy(&plane, &state.binding, &reinstall, 2).unwrap();
        let digest = state.command_digest(&command).unwrap();
        state
            .publish(&command, digest, prepared, &mut endpoint, now)
            .unwrap();
        assert!(apply_policy(&plane, &state.binding, &reinstall, 2).is_err());
        assert!(state.duplicate(&command, digest).unwrap());
        let record = serde_json::to_vec(&Record::Command {
            session: 1,
            at_ns: 2,
            command,
        })
        .unwrap();
        let restored = p.build().unwrap();
        let (mut recovered, mut ep) = State::new(&p, &q, restored.clone(), 1).unwrap();
        if let Record::Command { at_ns, command, .. } = decode_record(&record).unwrap() {
            recovered
                .replay_command(&command, &mut ep, MonoTime::new(recovered.domain, at_ns))
                .unwrap();
        }
        assert!(apply_policy(&restored, &recovered.binding, &reinstall, 3).is_err());
    }
    #[test]
    fn restart_burns_old_capacity_and_never_reinstalls_old_grant() {
        let (p, q) = inputs();
        let (mut state, endpoint) = State::new(&p, &q, p.build().unwrap(), 1).unwrap();
        let key = q.windows[0].key();
        assert_eq!(endpoint.remaining(key, quota::Class::Normal), 10);
        drop(endpoint);
        let mut endpoint = state.restart(2, 5, 950).unwrap();
        assert_eq!(endpoint.remaining(key, quota::Class::Normal), 0);
        assert_eq!(state.coordinator.accounted(key).unwrap(), 10);
        let command = command(
            &p,
            1,
            Action::QuotaInstall {
                grant: quota_config::Grant {
                    request_id: 2,
                    class: quota_config::Class::Normal,
                    expires_ns: 800,
                    costs: vec![quota_config::Cost {
                        window: "api".into(),
                        units: 7,
                    }],
                },
            },
        );
        let now = MonoTime::new(state.domain, 6);
        let digest = state.command_digest(&command).unwrap();
        let prepared = state.prepare(&command, &endpoint, now).unwrap();
        assert_eq!(endpoint.remaining(key, quota::Class::Normal), 0);
        state
            .publish(&command, digest, prepared, &mut endpoint, now)
            .unwrap();
        assert_eq!(endpoint.remaining(key, quota::Class::Normal), 7);
        assert_eq!(state.coordinator.accounted(key).unwrap(), 17);
        assert!(state.duplicate(&command, digest).unwrap());
        assert_eq!(endpoint.remaining(key, quota::Class::Normal), 7);
    }
    #[test]
    fn identity_conflicts_and_foreign_domains_are_rejected_before_mutation() {
        let (p, q) = inputs();
        let (mut state, mut endpoint) = State::new(&p, &q, p.build().unwrap(), 1).unwrap();
        let mut c = command(&p, 1, Action::QuotaFreeze);
        c.binding.writer_epoch = 2;
        assert!(state.command_digest(&c).is_err());
        c.binding.writer_epoch = 1;
        c.binding.domain.generation = 2;
        assert!(state.command_digest(&c).is_err());
        c.binding.domain.generation = 1;
        let digest = state.command_digest(&c).unwrap();
        let now = MonoTime::new(state.domain, 2);
        let prepared = state.prepare(&c, &endpoint, now).unwrap();
        state
            .publish(&c, digest, prepared, &mut endpoint, now)
            .unwrap();
        c.action = Action::QuotaRenew { until_ns: 990 };
        assert!(
            state
                .duplicate(&c, state.command_digest(&c).unwrap())
                .is_err()
        );
        assert!(state.restart(2, 3, 950).unwrap().is_frozen());
    }
    #[test]
    fn window_rollover_preserves_old_occupancy_and_rule_update_keeps_attempt_identity() {
        let (p, q) = inputs();
        let (mut state, mut endpoint) = State::new(&p, &q, p.build().unwrap(), 1).unwrap();
        let now = |n| MonoTime::new(p.domain().unwrap(), n);
        let binding = tickwire_binance_quota::TimeBinding {
            owner_id: [11; 16],
            source: tickwire_clock::Source::SpotTest,
            domain: state.domain,
        };
        let charge = tickwire_binance_clock::TimeCharge {
            source: binding.source,
            domain: binding.domain,
            connection_epoch: 1,
            kind: tickwire_binance_clock::ChargeKind::Query,
            api_weight: 1,
        };
        assert_eq!(
            endpoint.consume_time(&binding, &charge, now(2)).unwrap(),
            quota::Consumed::New
        );
        state
            .replay_command(
                &command(&p, 1, Action::QuotaRenew { until_ns: 1900 }),
                &mut endpoint,
                now(3),
            )
            .unwrap();
        let mut next = q.windows[0].clone();
        next.name = "api_next".into();
        next.window = 2;
        next.starts_ns = 1000;
        next.ends_ns = 2000;
        state
            .replay_command(
                &command(
                    &p,
                    2,
                    Action::QuotaWindows {
                        windows: vec![next.clone()],
                    },
                ),
                &mut endpoint,
                now(4),
            )
            .unwrap();
        state
            .replay_command(
                &command(
                    &p,
                    3,
                    Action::QuotaInstall {
                        grant: quota_config::Grant {
                            request_id: 2,
                            class: quota_config::Class::Normal,
                            expires_ns: 1800,
                            costs: vec![quota_config::Cost {
                                window: next.name.clone(),
                                units: 7,
                            }],
                        },
                    },
                ),
                &mut endpoint,
                now(5),
            )
            .unwrap();
        let mut rule = q.rules[0].clone();
        rule.costs[0].window = next.name.clone();
        state
            .replay_command(
                &command(
                    &p,
                    4,
                    Action::QuotaPolicy {
                        generation: 2,
                        starts_ns: 1000,
                        ends_ns: 2000,
                        rules: vec![rule],
                    },
                ),
                &mut endpoint,
                now(6),
            )
            .unwrap();
        assert_eq!(state.coordinator.accounted(q.windows[0].key()).unwrap(), 10);
        assert_eq!(state.coordinator.accounted(next.key()).unwrap(), 7);
        let mut fresh = charge;
        fresh.connection_epoch = 2;
        assert_eq!(
            endpoint.consume_time(&binding, &fresh, now(1001)).unwrap(),
            quota::Consumed::New
        );
        assert_eq!(endpoint.remaining(next.key(), quota::Class::Normal), 6);
        assert!(endpoint.consume_time(&binding, &charge, now(1002)).is_err());
        assert!(endpoint.is_frozen());
    }
}
