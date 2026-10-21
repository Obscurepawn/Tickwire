//! Typed Binance request classification over one shared local quota owner.
#![forbid(unsafe_code)]
pub mod bootstrap;
pub use quota::{Class, Consumed, Cost};
use sha2::{Digest, Sha256};
pub use tickwire_binance::request::Transport;
use tickwire_binance_account::reconciliation as read;
use tickwire_binance_clock as time;
use tickwire_binance_private as private;
use tickwire_binance_public as public;
pub use tickwire_clock::{ClockDomain, MonoTime, Source};
use tickwire_quota as quota;
use tickwire_venue as venue;
pub const MAX_RULES: usize = 256;
pub const MAX_IDENTITIES: usize = 4096;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Identity,
    Policy,
    Capacity,
    Quota(quota::Error),
}
pub type Result<T> = core::result::Result<T, Error>;
impl From<quota::Error> for Error {
    fn from(e: quota::Error) -> Self {
        Self::Quota(e)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandAction {
    New,
    Cancel,
    CancelReplace,
    Amend,
    Modify,
    Query,
    CancelList,
    Algo,
    CancelAlgo,
    SpotList {
        kind: venue::ListKind,
        legs: u8,
        advanced: bool,
    },
    PeggedNew,
    AdvancedCancelReplace,
    Sor,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    CommandSession(CommandSessionAction),
    GuardianConnect,
    TimeConnect,
    TimeQuery,
    PrivateStreamConnect,
    PrivateManagementConnect,
    PrivateUpgrade,
    PrivatePong,
    PrivateClose,
    SpotLogon,
    SpotSubscribe,
    SpotSubscribeSignature,
    SpotStatus,
    SpotSubscriptions,
    SpotUnsubscribe,
    SpotLogout,
    UsdmCreate,
    UsdmKeepalive,
    UsdmClose,
    Read(ReadAction),
    Public {
        lane: public::Lane,
        kind: public::ChargeKind,
    },
    Command {
        transport: Transport,
        action: CommandAction,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandSessionAction {
    Connect = 1,
    Upgrade = 2,
    Ping = 3,
    Pong = 4,
    Close = 5,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadAction {
    Connect,
    TradePage,
    AccountSnapshot,
    OpenOrders,
    ConditionalOrders,
    OrderQuery,
    Income,
    StructureQuery,
    AmendmentHistory,
}
#[derive(Clone, Copy, Debug)]
pub enum ReadQuery {
    Trade(read::TradeQuery),
    Snapshot(read::bootstrap::SnapshotQuery),
    Order(read::orders::OrderQuery),
    Income(read::income::IncomeQuery),
    Structure(read::structures::StructureQuery),
    Amendment(read::amendments::AmendmentQuery),
}
#[derive(Clone, Debug)]
pub struct ReadBinding {
    /// Uniquely allocated to this reader instance by the supervisor.
    pub owner_id: [u8; 16],
    pub scope: private::AccountScope,
    pub domain: ClockDomain,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    pub source: Source,
    pub action: Action,
    pub class: Class,
    pub api_weight: u32,
    pub websocket_messages: u32,
    pub costs: Vec<Cost>,
}
#[derive(Clone, Debug)]
pub struct Policy {
    pub generation: u64,
    pub starts_ns: u64,
    pub ends_ns: u64,
    pub rules: Vec<Rule>,
}
#[derive(Clone, Copy, Debug)]
pub struct TimeBinding {
    pub owner_id: [u8; 16],
    pub source: Source,
    pub domain: ClockDomain,
}
#[derive(Clone, Debug)]
pub struct PrivateBinding {
    pub owner_id: [u8; 16],
    pub scope: private::AccountScope,
    pub domain: ClockDomain,
}
#[derive(Clone, Copy, Debug)]
pub struct PublicBinding {
    pub owner_id: [u8; 16],
    pub source: Source,
    pub domain: ClockDomain,
}
#[derive(Clone, Copy, Debug)]
pub struct CommandBinding {
    pub owner_id: [u8; 16],
    pub risk_domain_id: [u8; 16],
    pub account: u64,
    pub product: venue::Product,
    pub environment: venue::Environment,
    pub domain: ClockDomain,
}
struct IdentityEntry {
    key: Vec<u8>,
    digest: [u8; 32],
    id: quota::AttemptId,
}
/// A validated rule change. It retains all prior attempt identities at commit.
pub struct PreparedPolicy {
    previous_generation: u64,
    producer: [u8; 16],
    domain: ClockDomain,
    policy: Policy,
}
pub struct Endpoint {
    limiter: quota::ExecLimiter,
    producer: [u8; 16],
    domain: ClockDomain,
    policy: Policy,
    identities: Vec<IdentityEntry>,
    next_sequence: u64,
    last_error: Option<Error>,
}
fn validate_policy(policy: &Policy) -> Result<()> {
    if policy.generation == 0
        || policy.starts_ns >= policy.ends_ns
        || policy.rules.is_empty()
        || policy.rules.len() > MAX_RULES
    {
        return Err(Error::Invalid);
    }
    for (i, r) in policy.rules.iter().enumerate() {
        if r.costs.is_empty()
            || r.costs.len() > quota::MAX_COSTS
            || r.costs.iter().any(|c| c.units == 0)
            || r.costs.windows(2).any(|w| w[0].window >= w[1].window)
            || policy.rules[..i]
                .iter()
                .any(|old| old.source == r.source && old.action == r.action)
        {
            return Err(Error::Invalid);
        }
    }
    Ok(())
}
impl Endpoint {
    pub fn consume_command_session(
        &mut self,
        binding: &CommandBinding,
        session: &tickwire_binance_account::ws::SessionIdentity,
        operation: u64,
        action: CommandSessionAction,
        now: MonoTime,
    ) -> Result<Consumed> {
        self.binding(binding.owner_id, binding.domain, now)?;
        if operation == 0
            || session.connection_epoch == 0
            || session.writer_epoch == 0
            || session.execution_instance == [0; 16]
            || session.scope.venue != 1
            || session.scope.risk_domain_id != binding.risk_domain_id
            || session.scope.account != binding.account
            || session.scope.environment != binding.environment
            || session.product != binding.product
        {
            return Err(Error::Identity);
        }
        let source = command_source(binding.product, binding.environment)?;
        let mut key = identity(7, binding.owner_id, source);
        key.extend(session.input_stream);
        key.extend(session.execution_instance);
        key.extend(session.writer_epoch.to_le_bytes());
        key.extend(session.connection_epoch.to_le_bytes());
        key.extend(operation.to_le_bytes());
        let mut content = vec![action as u8];
        content.extend(session.account_stream);
        content.extend(session.baseline_digest);
        content.extend(session.credential_ref_digest);
        self.consume(
            key,
            &content,
            source,
            Action::CommandSession(action),
            0,
            0,
            now,
        )
    }
    /// Independent guardian connection identity; no order/API weight is implied.
    pub fn consume_guardian_connect(
        &mut self,
        binding: &CommandBinding,
        connection_epoch: u64,
        now: MonoTime,
    ) -> Result<Consumed> {
        self.binding(binding.owner_id, binding.domain, now)?;
        if connection_epoch == 0 || binding.risk_domain_id == [0; 16] || binding.account == 0 {
            return Err(Error::Identity);
        }
        let source = command_source(binding.product, binding.environment)?;
        let mut key = identity(6, binding.owner_id, source);
        key.extend(binding.risk_domain_id);
        key.extend(binding.account.to_le_bytes());
        key.extend(connection_epoch.to_le_bytes());
        self.consume(key, &[], source, Action::GuardianConnect, 0, 0, now)
    }
    pub fn new(
        limiter: quota::ExecLimiter,
        producer: [u8; 16],
        domain: ClockDomain,
        policy: Policy,
    ) -> Result<Self> {
        if producer == [0; 16] || domain != limiter.clock_domain() {
            return Err(Error::Invalid);
        }
        validate_policy(&policy)?;
        Ok(Self {
            limiter,
            producer,
            domain,
            policy,
            identities: Vec::new(),
            next_sequence: 1,
            last_error: None,
        })
    }
    pub fn prepare_policy(&self, policy: Policy, now: MonoTime) -> Result<PreparedPolicy> {
        validate_policy(&policy)?;
        if now.domain() != self.domain
            || policy.generation <= self.policy.generation
            || policy.ends_ns <= now.nanos()
        {
            return Err(Error::Policy);
        }
        Ok(PreparedPolicy {
            previous_generation: self.policy.generation,
            producer: self.producer,
            domain: self.domain,
            policy,
        })
    }
    pub fn commit_policy(&mut self, prepared: PreparedPolicy) -> Result<()> {
        if self.policy.generation != prepared.previous_generation
            || self.producer != prepared.producer
            || self.domain != prepared.domain
        {
            return Err(Error::Policy);
        }
        self.policy = prepared.policy;
        Ok(())
    }
    fn binding(&mut self, owner: [u8; 16], domain: ClockDomain, now: MonoTime) -> Result<()> {
        if owner == [0; 16] {
            return Err(Error::Identity);
        }
        if domain != self.domain || now.domain() != self.domain {
            self.limiter.freeze();
            return Err(Error::Identity);
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    fn consume(
        &mut self,
        key: Vec<u8>,
        content: &[u8],
        source: Source,
        action: Action,
        api_weight: u32,
        messages: u32,
        now: MonoTime,
    ) -> Result<Consumed> {
        if now.nanos() < self.policy.starts_ns || now.nanos() >= self.policy.ends_ns {
            return Err(Error::Policy);
        }
        let rule = self
            .policy
            .rules
            .iter()
            .find(|r| r.source == source && r.action == action)
            .ok_or(Error::Policy)?;
        if rule.api_weight != api_weight || rule.websocket_messages != messages {
            return Err(Error::Identity);
        }
        let mut encoded = b"tickwire.binance-quota.charge.v1\0".to_vec();
        encoded.extend_from_slice(&self.policy.generation.to_le_bytes());
        encoded.extend_from_slice(&key);
        encoded.extend_from_slice(&action_bytes(action));
        encoded.extend_from_slice(&api_weight.to_le_bytes());
        encoded.extend_from_slice(&messages.to_le_bytes());
        encoded.extend_from_slice(content);
        encoded.push(match rule.class {
            Class::Normal => 1,
            Class::Recovery => 2,
        });
        for c in &rule.costs {
            encode_cost(&mut encoded, c);
        }
        let digest: [u8; 32] = Sha256::digest(&encoded).into();
        let id = if let Some(old) = self.identities.iter().find(|e| e.key == key) {
            if old.digest != digest {
                self.limiter.freeze();
                return Err(quota::Error::Conflict.into());
            }
            old.id
        } else {
            if self.identities.len() >= MAX_IDENTITIES {
                return Err(Error::Capacity);
            }
            let next = self.next_sequence.checked_add(1).ok_or(Error::Capacity)?;
            let id = quota::AttemptId {
                producer: self.producer,
                sequence: self.next_sequence,
            };
            self.next_sequence = next;
            self.identities.push(IdentityEntry { key, digest, id });
            id
        };
        self.limiter
            .consume(id, digest, rule.class, &rule.costs, now)
            .map_err(Into::into)
    }
    pub fn consume_time(
        &mut self,
        binding: &TimeBinding,
        charge: &time::TimeCharge,
        now: MonoTime,
    ) -> Result<Consumed> {
        self.binding(binding.owner_id, binding.domain, now)?;
        if charge.source != binding.source
            || charge.domain != binding.domain
            || charge.connection_epoch == 0
        {
            return Err(Error::Identity);
        }
        let action = match charge.kind {
            time::ChargeKind::Connect => Action::TimeConnect,
            time::ChargeKind::Query => Action::TimeQuery,
        };
        let mut key = identity(1, binding.owner_id, binding.source);
        key.extend_from_slice(&charge.connection_epoch.to_le_bytes());
        key.extend_from_slice(&action_bytes(action));
        self.consume(key, &[], binding.source, action, charge.api_weight, 0, now)
    }
    pub fn consume_private(
        &mut self,
        binding: &PrivateBinding,
        charge: &private::Charge,
        now: MonoTime,
    ) -> Result<Consumed> {
        self.binding(binding.owner_id, binding.domain, now)?;
        binding.scope.validate().map_err(|_| Error::Identity)?;
        if charge.scope != binding.scope || charge.connection_epoch == 0 || charge.attempt == 0 {
            return Err(Error::Identity);
        }
        let source = private_source(&binding.scope);
        let action = private_action(charge.kind);
        let mut key = identity(2, binding.owner_id, source);
        key.extend_from_slice(&binding.scope.risk_domain_id);
        key.extend_from_slice(&binding.scope.venue.to_le_bytes());
        key.extend_from_slice(&binding.scope.account.to_le_bytes());
        key.extend_from_slice(&binding.scope.credential_generation.to_le_bytes());
        key.extend_from_slice(&charge.connection_epoch.to_le_bytes());
        key.extend_from_slice(&charge.attempt.to_le_bytes());
        let mut content = charge.digest.to_vec();
        content.extend_from_slice(&(charge.bytes as u64).to_le_bytes());
        if let private::ChargeKind::Api(private::Operation::SpotUnsubscribe { subscription_id }) =
            charge.kind
        {
            content.extend_from_slice(&subscription_id.to_le_bytes());
        }
        self.consume(
            key,
            &content,
            source,
            action,
            charge.api_weight,
            charge.websocket_messages,
            now,
        )
    }
    pub fn consume_public(
        &mut self,
        binding: &PublicBinding,
        charge: &public::PublicCharge,
        now: MonoTime,
    ) -> Result<Consumed> {
        self.binding(binding.owner_id, binding.domain, now)?;
        if charge.owner_id != binding.owner_id
            || charge.source != binding.source
            || charge.domain != binding.domain
            || charge.connection_epoch == 0
            || charge.operation == 0
        {
            return Err(Error::Identity);
        }
        let action = Action::Public {
            lane: charge.lane,
            kind: charge.kind,
        };
        let mut key = identity(3, binding.owner_id, binding.source);
        key.extend_from_slice(&charge.connection_epoch.to_le_bytes());
        key.extend_from_slice(&charge.operation.to_le_bytes());
        key.extend_from_slice(&action_bytes(action));
        // PublicCharge does not carry message count. The typed action fixes it.
        let messages = u32::from(matches!(
            charge.kind,
            public::ChargeKind::Subscribe | public::ChargeKind::Pong | public::ChargeKind::Close
        ));
        self.consume(
            key,
            &[],
            binding.source,
            action,
            charge.api_weight,
            messages,
            now,
        )
    }
    /// A connection attempt and its signed query each consume their configured
    /// costs once. Repeated first-accept checks retain the same request epoch.
    pub fn consume_read(
        &mut self,
        binding: &ReadBinding,
        scope: &private::AccountScope,
        query: ReadQuery,
        request_epoch: u64,
        operation: read::ReadOperation,
        now: MonoTime,
    ) -> Result<Consumed> {
        self.binding(binding.owner_id, binding.domain, now)?;
        scope.validate().map_err(|_| Error::Identity)?;
        if scope != &binding.scope || request_epoch == 0 {
            return Err(Error::Identity);
        }
        let source = private_source(scope);
        let mut content = Vec::new();
        let (family, request_action) = match query {
            ReadQuery::Income(q) => {
                q.validate().map_err(|_| Error::Identity)?;
                if scope.market != tickwire_binance::request::Market::Usdm {
                    return Err(Error::Identity);
                }
                for n in [
                    q.start_time_ms,
                    q.end_time_ms,
                    u64::from(q.limit),
                    q.private_connection_epoch,
                    q.gap_generation,
                ] {
                    content.extend_from_slice(&n.to_le_bytes());
                }
                content.push(match q.purpose {
                    read::income::IncomeUse::BaselineCoverage => 1,
                    read::income::IncomeUse::Account => 2,
                });
                (4, ReadAction::Income)
            }
            ReadQuery::Amendment(q) => {
                if scope.market != tickwire_binance::request::Market::Spot
                    || q.intent.epoch == 0
                    || q.intent.intent_id == 0
                    || q.order_id == [0; 16]
                    || q.from_execution_id > i64::MAX as u64
                    || q.limit == 0
                    || q.limit > 16
                    || q.private_connection_epoch == 0
                    || q.gap_generation == 0
                {
                    return Err(Error::Identity);
                }
                for n in [
                    q.intent.epoch,
                    q.intent.intent_id,
                    q.from_execution_id,
                    u64::from(q.limit),
                    q.private_connection_epoch,
                    q.gap_generation,
                ] {
                    content.extend_from_slice(&n.to_le_bytes());
                }
                content.extend_from_slice(&q.order_id);
                (6, ReadAction::AmendmentHistory)
            }
            ReadQuery::Structure(q) => {
                if q.intent.epoch == 0
                    || q.intent.intent_id == 0
                    || q.structure_id == [0; 16]
                    || q.private_connection_epoch == 0
                    || q.gap_generation == 0
                {
                    return Err(Error::Identity);
                }
                for n in [
                    q.intent.epoch,
                    q.intent.intent_id,
                    q.private_connection_epoch,
                    q.gap_generation,
                ] {
                    content.extend_from_slice(&n.to_le_bytes());
                }
                content.extend_from_slice(&q.structure_id);
                (5, ReadAction::StructureQuery)
            }
            ReadQuery::Order(q) => {
                if q.intent.epoch == 0
                    || q.intent.intent_id == 0
                    || q.order_id == [0; 16]
                    || q.private_connection_epoch == 0
                    || q.gap_generation == 0
                {
                    return Err(Error::Identity);
                }
                for n in [
                    q.intent.epoch,
                    q.intent.intent_id,
                    q.private_connection_epoch,
                    q.gap_generation,
                ] {
                    content.extend_from_slice(&n.to_le_bytes());
                }
                content.extend_from_slice(&q.order_id);
                (3, ReadAction::OrderQuery)
            }
            ReadQuery::Trade(q) => {
                if q.intent.epoch == 0
                    || q.intent.intent_id == 0
                    || q.order_id == [0; 16]
                    || q.limit == 0
                    || q.limit > 16
                    || q.from_id > i64::MAX as u64
                    || q.private_connection_epoch == 0
                    || q.gap_generation == 0
                {
                    return Err(Error::Identity);
                }
                for n in [
                    q.intent.epoch,
                    q.intent.intent_id,
                    q.from_id,
                    u64::from(q.limit),
                    q.private_connection_epoch,
                    q.gap_generation,
                ] {
                    content.extend_from_slice(&n.to_le_bytes());
                }
                content.extend_from_slice(&q.order_id);
                (1, ReadAction::TradePage)
            }
            ReadQuery::Snapshot(q) => {
                if q.private_connection_epoch == 0 || q.gap_generation == 0 {
                    return Err(Error::Identity);
                }
                content.extend_from_slice(&q.private_connection_epoch.to_le_bytes());
                content.extend_from_slice(&q.gap_generation.to_le_bytes());
                let (tag, action) = match q.kind {
                    read::bootstrap::SnapshotKind::Account => (1, ReadAction::AccountSnapshot),
                    read::bootstrap::SnapshotKind::OpenOrders => (2, ReadAction::OpenOrders),
                    read::bootstrap::SnapshotKind::ConditionalOrders => {
                        (3, ReadAction::ConditionalOrders)
                    }
                };
                content.push(tag);
                (2, action)
            }
        };
        let action = match operation {
            read::ReadOperation::Connect => ReadAction::Connect,
            read::ReadOperation::TradePage => request_action,
        };
        let mut key = identity(5, binding.owner_id, source);
        key.extend_from_slice(&scope.risk_domain_id);
        key.extend_from_slice(&scope.venue.to_le_bytes());
        key.extend_from_slice(&scope.account.to_le_bytes());
        key.extend_from_slice(&scope.credential_generation.to_le_bytes());
        key.push(family);
        key.extend_from_slice(&request_epoch.to_le_bytes());
        key.push(u8::from(matches!(
            operation,
            read::ReadOperation::TradePage
        )));
        // The read API has no wire-reported weight. All endpoint costs are supplied
        // by the explicit rule, like command costs.
        self.consume(key, &content, source, Action::Read(action), 0, 0, now)
    }
    /// Invoke inside the fresh first-accept gate, never cache this result.
    pub fn consume_command(
        &mut self,
        binding: &CommandBinding,
        command: &venue::CanonicalCommand,
        transport: Transport,
        now: MonoTime,
    ) -> Result<Consumed> {
        self.binding(binding.owner_id, binding.domain, now)?;
        let c = command.command();
        let r = &c.context.request;
        if r.venue != 1
            || r.risk_domain_id != binding.risk_domain_id
            || r.account != binding.account
            || r.product != binding.product
            || r.environment != binding.environment
            || now.nanos() >= r.deadline
        {
            return Err(Error::Identity);
        }
        let source = command_source(binding.product, binding.environment)?;
        let action = Action::Command {
            transport,
            action: command_action(&c.body),
        };
        let mut key = identity(4, binding.owner_id, source);
        key.extend_from_slice(&r.risk_domain_id);
        key.extend_from_slice(&r.account.to_le_bytes());
        key.extend_from_slice(&r.intent_exec_epoch.to_le_bytes());
        key.extend_from_slice(&r.intent_id.to_le_bytes());
        key.push(transport_code(transport));
        // Command rule costs are explicit. Wire api-weight/message reports do not exist here.
        self.consume(key, &command.digest(), source, action, 0, 0, now)
    }
    pub fn remaining(&self, key: quota::WindowKey, class: Class) -> u64 {
        self.limiter.remaining(key, class)
    }
    /// Read-only identity/capability reference for supervisor issue and renewal.
    pub fn consumer(&self) -> &quota::ExecLimiter {
        &self.limiter
    }
    pub fn producer_id(&self) -> [u8; 16] {
        self.producer
    }
    pub fn install(&mut self, grant: quota::Grant, now: MonoTime) -> Result<()> {
        self.limiter.install(grant, now).map_err(Into::into)
    }
    pub fn renew(&mut self, renewal: quota::LeaseRenewal, now: MonoTime) -> Result<()> {
        self.limiter.renew(renewal, now).map_err(Into::into)
    }
    pub fn cooldown(&mut self, until_ns: u64, now: MonoTime) -> Result<()> {
        self.limiter.cooldown(until_ns, now).map_err(Into::into)
    }
    pub fn freeze(&mut self) {
        self.limiter.freeze();
    }
    pub fn is_frozen(&self) -> bool {
        self.limiter.is_frozen()
    }
    pub fn stop(self) -> quota::StoppedReceipt {
        self.limiter.stop()
    }
    pub fn last_error(&self) -> Option<Error> {
        self.last_error
    }
    fn allowed(&mut self, result: Result<Consumed>) -> bool {
        self.last_error = result.err();
        result.is_ok()
    }
    pub fn time(&mut self, binding: TimeBinding) -> TimePermit<'_> {
        TimePermit {
            endpoint: self,
            binding,
        }
    }
    pub fn private(&mut self, binding: PrivateBinding) -> ControlPermit<'_> {
        ControlPermit {
            endpoint: self,
            binding,
        }
    }
    pub fn public(&mut self, binding: PublicBinding) -> PublicPermit<'_> {
        PublicPermit {
            endpoint: self,
            binding,
        }
    }
    pub fn read(&mut self, binding: ReadBinding) -> ReadPermit<'_> {
        ReadPermit {
            endpoint: self,
            binding,
        }
    }
}
pub struct ReadPermit<'a> {
    endpoint: &'a mut Endpoint,
    binding: ReadBinding,
}
impl read::ReadPermit for ReadPermit<'_> {
    fn allow(
        &mut self,
        scope: &private::AccountScope,
        query: &read::TradeQuery,
        epoch: u64,
        operation: read::ReadOperation,
        now: &private::ClockSample,
    ) -> bool {
        let result = self.endpoint.consume_read(
            &self.binding,
            scope,
            ReadQuery::Trade(*query),
            epoch,
            operation,
            now.monotonic,
        );
        self.endpoint.allowed(result)
    }
}
impl read::bootstrap::SnapshotPermit for ReadPermit<'_> {
    fn allow(
        &mut self,
        scope: &private::AccountScope,
        query: read::bootstrap::SnapshotQuery,
        epoch: u64,
        operation: read::ReadOperation,
        now: &private::ClockSample,
    ) -> bool {
        let result = self.endpoint.consume_read(
            &self.binding,
            scope,
            ReadQuery::Snapshot(query),
            epoch,
            operation,
            now.monotonic,
        );
        self.endpoint.allowed(result)
    }
}
impl read::structures::StructurePermit for ReadPermit<'_> {
    fn allow(
        &mut self,
        scope: &private::AccountScope,
        query: &read::structures::StructureQuery,
        epoch: u64,
        operation: read::ReadOperation,
        now: &private::ClockSample,
    ) -> bool {
        let result = self.endpoint.consume_read(
            &self.binding,
            scope,
            ReadQuery::Structure(*query),
            epoch,
            operation,
            now.monotonic,
        );
        self.endpoint.allowed(result)
    }
}
impl read::orders::OrderPermit for ReadPermit<'_> {
    fn allow(
        &mut self,
        scope: &private::AccountScope,
        query: &read::orders::OrderQuery,
        epoch: u64,
        operation: read::ReadOperation,
        now: &private::ClockSample,
    ) -> bool {
        let result = self.endpoint.consume_read(
            &self.binding,
            scope,
            ReadQuery::Order(*query),
            epoch,
            operation,
            now.monotonic,
        );
        self.endpoint.allowed(result)
    }
}
impl read::amendments::AmendmentPermit for ReadPermit<'_> {
    fn allow(
        &mut self,
        scope: &private::AccountScope,
        query: &read::amendments::AmendmentQuery,
        epoch: u64,
        operation: read::ReadOperation,
        now: &private::ClockSample,
    ) -> bool {
        let result = self.endpoint.consume_read(
            &self.binding,
            scope,
            ReadQuery::Amendment(*query),
            epoch,
            operation,
            now.monotonic,
        );
        self.endpoint.allowed(result)
    }
}
impl read::income::IncomePermit for ReadPermit<'_> {
    fn allow(
        &mut self,
        scope: &private::AccountScope,
        query: &read::income::IncomeQuery,
        epoch: u64,
        operation: read::ReadOperation,
        now: &private::ClockSample,
    ) -> bool {
        let result = self.endpoint.consume_read(
            &self.binding,
            scope,
            ReadQuery::Income(*query),
            epoch,
            operation,
            now.monotonic,
        );
        self.endpoint.allowed(result)
    }
}
pub struct TimePermit<'a> {
    endpoint: &'a mut Endpoint,
    binding: TimeBinding,
}
impl time::TimePermit for TimePermit<'_> {
    fn allow(&mut self, charge: &time::TimeCharge, now: &time::LocalTime) -> bool {
        let result = self
            .endpoint
            .consume_time(&self.binding, charge, now.monotonic);
        self.endpoint.allowed(result)
    }
}
pub struct ControlPermit<'a> {
    endpoint: &'a mut Endpoint,
    binding: PrivateBinding,
}
impl private::ControlPermit for ControlPermit<'_> {
    fn allow(&mut self, charge: &private::Charge, now: &private::ClockSample) -> bool {
        let result = self
            .endpoint
            .consume_private(&self.binding, charge, now.monotonic);
        self.endpoint.allowed(result)
    }
}
pub struct PublicPermit<'a> {
    endpoint: &'a mut Endpoint,
    binding: PublicBinding,
}
impl public::PublicPermit for PublicPermit<'_> {
    fn allow(&mut self, charge: &public::PublicCharge, now: MonoTime) -> bool {
        let result = self.endpoint.consume_public(&self.binding, charge, now);
        self.endpoint.allowed(result)
    }
    fn observe_http(&mut self, feedback: &public::HttpFeedback) {
        if feedback.charge.owner_id != self.binding.owner_id
            || feedback.charge.source != self.binding.source
            || feedback.charge.domain != self.binding.domain
        {
            self.endpoint.last_error = Some(Error::Identity);
            return;
        }
        if matches!(feedback.status, 429 | 418) {
            self.endpoint.freeze();
            self.endpoint.last_error = Some(Error::Policy);
        }
    }
}
fn identity(family: u8, owner: [u8; 16], source: Source) -> Vec<u8> {
    let mut bytes = vec![
        family,
        match source {
            Source::SpotProduction => 1,
            Source::SpotTest => 2,
            Source::UsdmProduction => 3,
            Source::UsdmTest => 4,
        },
    ];
    bytes.extend_from_slice(&owner);
    bytes
}
fn private_source(scope: &private::AccountScope) -> Source {
    match (scope.market, scope.environment) {
        (private::Market::Spot, private::Environment::Production) => Source::SpotProduction,
        (private::Market::Spot, private::Environment::Test) => Source::SpotTest,
        (private::Market::Usdm, private::Environment::Production) => Source::UsdmProduction,
        (private::Market::Usdm, private::Environment::Test) => Source::UsdmTest,
    }
}
fn command_source(product: venue::Product, environment: venue::Environment) -> Result<Source> {
    match (product, environment) {
        (venue::Product::Spot, venue::Environment::Production) => Ok(Source::SpotProduction),
        (venue::Product::Spot, venue::Environment::Test) => Ok(Source::SpotTest),
        (
            venue::Product::LinearPerpetual | venue::Product::LinearFuture,
            venue::Environment::Production,
        ) => Ok(Source::UsdmProduction),
        (
            venue::Product::LinearPerpetual | venue::Product::LinearFuture,
            venue::Environment::Test,
        ) => Ok(Source::UsdmTest),
        _ => Err(Error::Identity),
    }
}
fn private_action(kind: private::ChargeKind) -> Action {
    match kind {
        private::ChargeKind::StreamConnect => Action::PrivateStreamConnect,
        private::ChargeKind::ManagementConnect => Action::PrivateManagementConnect,
        private::ChargeKind::Upgrade => Action::PrivateUpgrade,
        private::ChargeKind::Pong => Action::PrivatePong,
        private::ChargeKind::Close => Action::PrivateClose,
        private::ChargeKind::Api(op) => match op {
            private::Operation::SpotLogon => Action::SpotLogon,
            private::Operation::SpotSubscribe => Action::SpotSubscribe,
            private::Operation::SpotSubscribeSignature => Action::SpotSubscribeSignature,
            private::Operation::SpotStatus => Action::SpotStatus,
            private::Operation::SpotSubscriptions => Action::SpotSubscriptions,
            private::Operation::SpotUnsubscribe { .. } => Action::SpotUnsubscribe,
            private::Operation::SpotLogout => Action::SpotLogout,
            private::Operation::UsdmCreate => Action::UsdmCreate,
            private::Operation::UsdmKeepalive => Action::UsdmKeepalive,
            private::Operation::UsdmClose => Action::UsdmClose,
        },
    }
}
pub fn command_action(body: &venue::CommandBody) -> CommandAction {
    use venue::{CommandBody as C, SpotAdvancedCommand as A};
    match body {
        C::New(_) => CommandAction::New,
        C::Cancel(_) => CommandAction::Cancel,
        C::CancelReplace { .. } => CommandAction::CancelReplace,
        C::AmendKeepPriority { .. } => CommandAction::Amend,
        C::Modify(_) => CommandAction::Modify,
        C::Query(_) => CommandAction::Query,
        C::SpotList(list) => CommandAction::SpotList {
            kind: list.kind,
            legs: list.legs.as_slice().len() as u8,
            advanced: false,
        },
        C::CancelList(_) => CommandAction::CancelList,
        C::Algo(_) => CommandAction::Algo,
        C::CancelAlgo(_) => CommandAction::CancelAlgo,
        C::SpotAdvanced(A::New(_)) => CommandAction::PeggedNew,
        C::SpotAdvanced(A::CancelReplace { .. }) => CommandAction::AdvancedCancelReplace,
        C::SpotAdvanced(A::List(list)) => CommandAction::SpotList {
            kind: list.kind,
            legs: list.legs.as_slice().len() as u8,
            advanced: true,
        },
        C::SpotAdvanced(A::SorNew(_)) => CommandAction::Sor,
    }
}
fn transport_code(t: Transport) -> u8 {
    match t {
        Transport::Rest => 1,
        Transport::WebSocket => 2,
    }
}
fn action_bytes(a: Action) -> Vec<u8> {
    let tag = match a {
        Action::CommandSession(kind) => return vec![22, kind as u8],
        Action::GuardianConnect => 21,
        Action::TimeConnect => 1,
        Action::TimeQuery => 2,
        Action::PrivateStreamConnect => 3,
        Action::PrivateManagementConnect => 4,
        Action::PrivateUpgrade => 5,
        Action::PrivatePong => 6,
        Action::PrivateClose => 7,
        Action::SpotLogon => 8,
        Action::SpotSubscribe => 9,
        Action::SpotSubscribeSignature => 10,
        Action::SpotStatus => 11,
        Action::SpotSubscriptions => 12,
        Action::SpotUnsubscribe => 13,
        Action::SpotLogout => 14,
        Action::UsdmCreate => 15,
        Action::UsdmKeepalive => 16,
        Action::UsdmClose => 17,
        Action::Read(kind) => {
            return vec![
                20,
                match kind {
                    ReadAction::Connect => 1,
                    ReadAction::TradePage => 2,
                    ReadAction::AccountSnapshot => 3,
                    ReadAction::OpenOrders => 4,
                    ReadAction::ConditionalOrders => 5,
                    ReadAction::OrderQuery => 6,
                    ReadAction::Income => 7,
                    ReadAction::StructureQuery => 8,
                    ReadAction::AmendmentHistory => 9,
                },
            ];
        }
        Action::Public { lane, kind } => {
            return vec![
                18,
                match lane {
                    public::Lane::Stream => 1,
                    public::Lane::Snapshot => 2,
                },
                match kind {
                    public::ChargeKind::Connect => 1,
                    public::ChargeKind::Upgrade => 2,
                    public::ChargeKind::Subscribe => 3,
                    public::ChargeKind::Pong => 4,
                    public::ChargeKind::Close => 5,
                    public::ChargeKind::Snapshot => 6,
                },
            ];
        }
        Action::Command { transport, action } => {
            let mut out = vec![19, transport_code(transport)];
            let tag = match action {
                CommandAction::New => 1,
                CommandAction::Cancel => 2,
                CommandAction::CancelReplace => 3,
                CommandAction::Amend => 4,
                CommandAction::Modify => 5,
                CommandAction::Query => 6,
                CommandAction::CancelList => 7,
                CommandAction::Algo => 8,
                CommandAction::CancelAlgo => 9,
                CommandAction::PeggedNew => 10,
                CommandAction::AdvancedCancelReplace => 11,
                CommandAction::Sor => 12,
                CommandAction::SpotList {
                    kind,
                    legs,
                    advanced,
                } => {
                    out.extend_from_slice(&[
                        13,
                        match kind {
                            venue::ListKind::Oco => 1,
                            venue::ListKind::Oto => 2,
                            venue::ListKind::Otoco => 3,
                        },
                        legs,
                        u8::from(advanced),
                    ]);
                    return out;
                }
            };
            out.push(tag);
            return out;
        }
    };
    vec![tag]
}
fn encode_cost(out: &mut Vec<u8>, c: &Cost) {
    out.extend_from_slice(&c.window.bucket.namespace);
    out.push(match c.window.bucket.scope {
        quota::ScopeKind::Ip => 1,
        quota::ScopeKind::Uid => 2,
        quota::ScopeKind::Account => 3,
        quota::ScopeKind::Connection => 4,
    });
    out.extend_from_slice(&c.window.bucket.subject);
    out.extend_from_slice(&c.window.bucket.pool.to_le_bytes());
    out.extend_from_slice(&c.window.window.to_le_bytes());
    out.extend_from_slice(&c.units.to_le_bytes());
}

#[cfg(test)]
mod read_tests;
