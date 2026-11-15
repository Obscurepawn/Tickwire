//! Typed Binance request classification, multi-source-IP command/query budget isolation, and
//! process-wide cross-account ORS quota admission (`SharedBinanceOrsQuota`) across Spot, USD-M,
//! COIN-M, and European Options (`#![forbid(unsafe_code)]`).
//!
//! Binds directly to `tickwire-venue`, `tickwire-binance`, and `tickwire-binance-private` with
//! zero dependency on legacy `tickwire-binance-account` state, zero floating-point arithmetic,
//! and strict separation between `ORDER`, `CANCEL`, and `QUERY` budgets.
#![forbid(unsafe_code)]
pub mod bootstrap;

/// Stateless WebSocket trading session identity descriptors for `Endpoint::consume_command_session`.
pub mod ws {
    use tickwire_venue as v;

    /// Domain-separated account scope bound to a WebSocket trading session.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SessionScope {
        pub risk_domain_id: [u8; 16],
        pub venue: u32,
        pub environment: v::Environment,
        pub account: u64,
    }

    /// Immutable identity of a single-owner WebSocket trading session.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SessionIdentity {
        pub input_stream: [u8; 16],
        pub account_stream: [u8; 16],
        pub baseline_digest: v::Hash,
        pub scope: SessionScope,
        pub product: v::Product,
        pub execution_instance: [u8; 16],
        pub writer_epoch: u64,
        pub connection_epoch: u64,
        pub credential_ref_digest: v::Hash,
    }
}

/// Stateless read-reconciliation query descriptors and permit traits for `Endpoint::consume_read`.
pub mod reconciliation {
    use tickwire_binance_private::{AccountScope, ClockSample};
    pub use tickwire_venue::IntentKey;

    /// Phase of an authenticated read-only REST query.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum ReadOperation {
        Connect,
        TradePage,
    }

    /// Authenticated trade-history page query descriptor.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct TradeQuery {
        pub intent: IntentKey,
        pub order_id: [u8; 16],
        /// Inclusive native trade ID. Zero starts the endpoint's available retained range.
        pub from_id: u64,
        pub limit: u16,
        pub private_connection_epoch: u64,
        pub gap_generation: u64,
    }

    /// Permit gate invoked before each trade-history connect or page request.
    pub trait ReadPermit {
        fn allow(
            &mut self,
            scope: &AccountScope,
            query: &TradeQuery,
            request_epoch: u64,
            operation: ReadOperation,
            now: &ClockSample,
        ) -> bool;
    }

    impl<F: FnMut(&AccountScope, &TradeQuery, u64, ReadOperation, &ClockSample) -> bool> ReadPermit
        for F
    {
        fn allow(
            &mut self,
            s: &AccountScope,
            q: &TradeQuery,
            e: u64,
            o: ReadOperation,
            n: &ClockSample,
        ) -> bool {
            self(s, q, e, o, n)
        }
    }

    /// Cold-startup and gap-recovery snapshot query descriptors.
    pub mod bootstrap {
        use super::{AccountScope, ClockSample, ReadOperation};

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum SnapshotKind {
            Account,
            OpenOrders,
            ConditionalOrders,
        }

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub struct SnapshotQuery {
            pub kind: SnapshotKind,
            pub private_connection_epoch: u64,
            pub gap_generation: u64,
        }

        pub trait SnapshotPermit {
            fn allow(
                &mut self,
                scope: &AccountScope,
                query: SnapshotQuery,
                request_epoch: u64,
                operation: ReadOperation,
                now: &ClockSample,
            ) -> bool;
        }
    }

    /// Single-order REST status query descriptors.
    pub mod orders {
        use super::{AccountScope, ClockSample, IntentKey, ReadOperation};

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub struct OrderQuery {
            pub intent: IntentKey,
            pub order_id: [u8; 16],
            pub private_connection_epoch: u64,
            pub gap_generation: u64,
        }

        pub trait OrderPermit {
            fn allow(
                &mut self,
                scope: &AccountScope,
                query: &OrderQuery,
                request_epoch: u64,
                operation: ReadOperation,
                now: &ClockSample,
            ) -> bool;
        }
    }

    /// Parent order-list / conditional structure query descriptors.
    pub mod structures {
        use super::{AccountScope, ClockSample, IntentKey, ReadOperation};

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub struct StructureQuery {
            pub intent: IntentKey,
            pub structure_id: [u8; 16],
            pub private_connection_epoch: u64,
            pub gap_generation: u64,
        }

        pub trait StructurePermit {
            fn allow(
                &mut self,
                scope: &AccountScope,
                query: &StructureQuery,
                request_epoch: u64,
                operation: ReadOperation,
                now: &ClockSample,
            ) -> bool;
        }
    }

    /// Spot execution amendment / keep-priority history query descriptors.
    pub mod amendments {
        use super::{AccountScope, ClockSample, IntentKey, ReadOperation};

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub struct AmendmentQuery {
            pub intent: IntentKey,
            pub order_id: [u8; 16],
            pub from_execution_id: u64,
            pub limit: u16,
            pub private_connection_epoch: u64,
            pub gap_generation: u64,
        }

        pub trait AmendmentPermit {
            fn allow(
                &mut self,
                scope: &AccountScope,
                query: &AmendmentQuery,
                request_epoch: u64,
                operation: ReadOperation,
                now: &ClockSample,
            ) -> bool;
        }
    }

    /// USD-M bounded income-history query descriptors.
    pub mod income {
        use super::{AccountScope, ClockSample, ReadOperation};

        pub const MAX_INCOME_ROWS: usize = 16;

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum IncomeUse {
            BaselineCoverage,
            Account,
        }

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub struct IncomeQuery {
            pub start_time_ms: u64,
            pub end_time_ms: u64,
            pub limit: u16,
            pub private_connection_epoch: u64,
            pub gap_generation: u64,
            pub purpose: IncomeUse,
        }

        impl IncomeQuery {
            pub fn validate(self) -> crate::Result<()> {
                if self.start_time_ms > self.end_time_ms
                    || self.end_time_ms > i64::MAX as u64
                    || self.limit == 0
                    || usize::from(self.limit) > MAX_INCOME_ROWS
                    || self.private_connection_epoch == 0
                    || self.gap_generation == 0
                {
                    return Err(crate::Error::Identity);
                }
                Ok(())
            }
        }

        pub trait IncomePermit {
            fn allow(
                &mut self,
                scope: &AccountScope,
                query: &IncomeQuery,
                request_epoch: u64,
                operation: ReadOperation,
                now: &ClockSample,
            ) -> bool;
        }
    }
}

use self::reconciliation as read;
pub use quota::{Class, Consumed, Cost};
use sha2::{Digest, Sha256};
pub use tickwire_binance::request::Transport;
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
        session: &ws::SessionIdentity,
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

pub use tickwire_quota::rate_limiter::{
    DEFAULT_EXCH_WARN_PCT, ExchangeUsageReport, IpKey, RateFlags, RateLimitId, RatePermit,
    RateReject, VenuePreset, VenueRateLimiter,
};

/// Action-class and exact window debit classification for multi-IP Binance execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionBudgetClass {
    /// Order-creating action charging `order_units` against account `ORDER` windows plus `ip_weight`.
    Order { order_units: u32, ip_weight: u32 },
    /// Modification/amendment charging `ORDER` windows iff `counts_as_order` plus `ip_weight`.
    Modify {
        counts_as_order: bool,
        ip_weight: u32,
    },
    /// Pure cancellation: charges ONLY `ip_weight` and NEVER debits `ORDER` windows.
    Cancel { ip_weight: u32 },
    /// Pure status/reconciliation query: charges ONLY `ip_weight` and NEVER debits `ORDER` or `CANCEL` windows.
    Query { ip_weight: u32 },
}

/// Errors produced by [`MultiIpCommandQuota`] and multi-IP action classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MultiIpQuotaError {
    Invalid,
    Identity,
    UnsupportedAction,
    Capacity,
    Conflict,
    AccountBanned,
    Rate(RateReject),
}

impl From<RateReject> for MultiIpQuotaError {
    #[inline]
    fn from(err: RateReject) -> Self {
        match err {
            RateReject::Frozen { .. } => Self::AccountBanned,
            other => Self::Rate(other),
        }
    }
}

/// Classifies a Binance [`CommandAction`] for `source` and `transport` into isolated
/// [`ActionBudgetClass`] budgets.
///
/// * Order-creating actions (`New`, `CancelReplace`, `AdvancedCancelReplace`, `PeggedNew`,
///   `Sor`, `Algo`, and `SpotList { legs, .. }` charging `legs` order units) debit account
///   `ORDER` windows plus per-IP weight.
/// * `Amend` on Spot (`/api/v3/order/amend/keepPriority`) charges IP weight `4` with
///   `counts_as_order = false`; `Modify` on USD-M (`PUT /fapi/v1/order`) charges `counts_as_order = true`
///   plus REST weight `1` (or WS weight `0`).
/// * Pure cancel actions (`Cancel`, `CancelList`, `CancelAlgo`) NEVER debit `ORDER` windows.
/// * Pure query actions (`Query`) NEVER debit `ORDER` or `CANCEL` windows.
pub fn classify_command_action(
    source: Source,
    transport: Transport,
    action: CommandAction,
) -> core::result::Result<ActionBudgetClass, MultiIpQuotaError> {
    let is_spot = matches!(source, Source::SpotProduction | Source::SpotTest);
    if is_spot {
        match action {
            CommandAction::New
            | CommandAction::CancelReplace
            | CommandAction::AdvancedCancelReplace
            | CommandAction::PeggedNew
            | CommandAction::Sor => Ok(ActionBudgetClass::Order {
                order_units: 1,
                ip_weight: 1,
            }),
            CommandAction::SpotList { kind, legs, .. } => {
                let valid_legs = match kind {
                    venue::ListKind::Oco | venue::ListKind::Oto => legs == 2,
                    venue::ListKind::Otoco => legs == 3,
                };
                if !valid_legs {
                    return Err(MultiIpQuotaError::Invalid);
                }
                Ok(ActionBudgetClass::Order {
                    order_units: u32::from(legs),
                    ip_weight: 1,
                })
            }
            CommandAction::Amend => Ok(ActionBudgetClass::Modify {
                counts_as_order: false,
                ip_weight: 4,
            }),
            CommandAction::Cancel | CommandAction::CancelList => {
                Ok(ActionBudgetClass::Cancel { ip_weight: 1 })
            }
            CommandAction::Query => Ok(ActionBudgetClass::Query { ip_weight: 4 }),
            CommandAction::Modify | CommandAction::Algo | CommandAction::CancelAlgo => {
                Err(MultiIpQuotaError::UnsupportedAction)
            }
        }
    } else {
        let cmd_ip_weight = match transport {
            Transport::Rest => 1,
            Transport::WebSocket => 0,
        };
        match action {
            CommandAction::New | CommandAction::Algo => Ok(ActionBudgetClass::Order {
                order_units: 1,
                ip_weight: cmd_ip_weight,
            }),
            CommandAction::Modify => Ok(ActionBudgetClass::Modify {
                counts_as_order: true,
                ip_weight: cmd_ip_weight,
            }),
            CommandAction::Cancel | CommandAction::CancelAlgo => Ok(ActionBudgetClass::Cancel {
                ip_weight: cmd_ip_weight,
            }),
            CommandAction::Query => Ok(ActionBudgetClass::Query { ip_weight: 1 }),
            CommandAction::CancelReplace
            | CommandAction::AdvancedCancelReplace
            | CommandAction::PeggedNew
            | CommandAction::Sor
            | CommandAction::SpotList { .. }
            | CommandAction::CancelList
            | CommandAction::Amend => Err(MultiIpQuotaError::UnsupportedAction),
        }
    }
}

/// Classifies a reconciliation [`ReadAction`] for `source` into its pure [`ActionBudgetClass::Query`] IP weight.
pub fn classify_read_action(
    source: Source,
    action: ReadAction,
) -> core::result::Result<ActionBudgetClass, MultiIpQuotaError> {
    let is_spot = matches!(source, Source::SpotProduction | Source::SpotTest);
    let ip_weight = if is_spot {
        match action {
            ReadAction::Connect => 1,
            ReadAction::TradePage => 20,
            ReadAction::AccountSnapshot => 20,
            ReadAction::OpenOrders => 6,
            ReadAction::ConditionalOrders => 40,
            ReadAction::OrderQuery => 4,
            ReadAction::StructureQuery => 4,
            ReadAction::AmendmentHistory => 4,
            ReadAction::Income => return Err(MultiIpQuotaError::UnsupportedAction),
        }
    } else {
        match action {
            ReadAction::Connect => 1,
            ReadAction::TradePage => 5,
            ReadAction::AccountSnapshot => 5,
            ReadAction::OpenOrders => 40,
            ReadAction::ConditionalOrders => 40,
            ReadAction::OrderQuery => 1,
            ReadAction::Income => 30,
            ReadAction::StructureQuery => 1,
            ReadAction::AmendmentHistory => return Err(MultiIpQuotaError::UnsupportedAction),
        }
    };
    Ok(ActionBudgetClass::Query { ip_weight })
}

#[derive(Clone, Copy, Debug)]
struct MultiIpAttempt {
    intent_exec_epoch: u64,
    intent_id: u64,
    transport: Transport,
    local_ip: IpKey,
    digest: [u8; 32],
}

/// Multi-source-IP, class-isolated hot-path quota owner for Binance Spot and USD-M accounts.
///
/// Wraps [`VenueRateLimiter`] (`BinanceSpot` or `BinanceUsdm`) with:
/// * Strict isolation of `Cancel` and `Query`/`ReadQuery` sends from account `ORDER` caps,
/// * Per-IP weight accounting and per-IP `429` cooldown isolation (`note_ip_rate_limited`),
/// * Account-wide `418` ban freeze (`note_account_banned`),
/// * Idempotent zero-byte retry deduplication keyed by `(intent_exec_epoch, intent_id, transport, local_ip)`,
/// * Conservative `X-MBX-*` / WS `rateLimits[]` reconciliation (`reconcile_rate_limit_header`).
#[derive(Debug)]
pub struct MultiIpCommandQuota {
    binding: CommandBinding,
    source: Source,
    venue_limiter: VenueRateLimiter,
    attempts: Vec<MultiIpAttempt>,
}

impl MultiIpCommandQuota {
    /// Creates a [`MultiIpCommandQuota`] with [`DEFAULT_EXCH_WARN_PCT`] (`80%`).
    pub fn new(
        binding: CommandBinding,
        local_ips: &[IpKey],
    ) -> core::result::Result<Self, MultiIpQuotaError> {
        Self::with_warn_pct(binding, local_ips, DEFAULT_EXCH_WARN_PCT)
    }

    /// Creates a [`MultiIpCommandQuota`] with an explicit `exch_warn_pct` (`1..=100`).
    pub fn with_warn_pct(
        binding: CommandBinding,
        local_ips: &[IpKey],
        exch_warn_pct: u8,
    ) -> core::result::Result<Self, MultiIpQuotaError> {
        if binding.owner_id == [0; 16] || binding.risk_domain_id == [0; 16] || binding.account == 0
        {
            return Err(MultiIpQuotaError::Identity);
        }
        let source = command_source(binding.product, binding.environment)
            .map_err(|_| MultiIpQuotaError::Identity)?;
        let preset = match source {
            Source::SpotProduction | Source::SpotTest => VenuePreset::BinanceSpot,
            Source::UsdmProduction | Source::UsdmTest => VenuePreset::BinanceUsdm,
        };
        let venue_limiter =
            VenueRateLimiter::with_warn_pct(preset, binding.account, local_ips, exch_warn_pct)?;
        Ok(Self {
            binding,
            source,
            venue_limiter,
            attempts: Vec::with_capacity(MAX_IDENTITIES),
        })
    }

    #[inline]
    pub const fn binding(&self) -> &CommandBinding {
        &self.binding
    }

    #[inline]
    pub const fn source(&self) -> Source {
        self.source
    }

    #[inline]
    pub const fn preset(&self) -> VenuePreset {
        self.venue_limiter.preset()
    }

    #[inline]
    pub fn local_ips(&self) -> &[IpKey] {
        self.venue_limiter.local_ips()
    }

    #[inline]
    pub const fn venue_limiter(&self) -> &VenueRateLimiter {
        &self.venue_limiter
    }

    #[inline]
    pub fn is_account_banned(&self) -> bool {
        self.venue_limiter.is_account_frozen()
    }

    /// Read-only probe checking whether `local_ip` has sufficient headroom for `action` at `now_ns`.
    pub fn can_afford_ip(
        &self,
        local_ip: IpKey,
        transport: Transport,
        action: CommandAction,
        symbol: u64,
        now_ns: u64,
    ) -> core::result::Result<(), MultiIpQuotaError> {
        if self.is_account_banned() {
            return Err(MultiIpQuotaError::AccountBanned);
        }
        match classify_command_action(self.source, transport, action)? {
            ActionBudgetClass::Order {
                order_units,
                ip_weight,
            } => self
                .venue_limiter
                .can_afford_place_orders(local_ip, symbol, order_units, ip_weight, now_ns)
                .map_err(Into::into),
            ActionBudgetClass::Modify {
                counts_as_order,
                ip_weight,
            } => self
                .venue_limiter
                .can_afford_modify(local_ip, symbol, ip_weight, counts_as_order, now_ns)
                .map_err(Into::into),
            ActionBudgetClass::Cancel { ip_weight } => self
                .venue_limiter
                .can_afford_cancel(local_ip, symbol, ip_weight, now_ns)
                .map_err(Into::into),
            ActionBudgetClass::Query { ip_weight } => self
                .venue_limiter
                .can_afford_query(local_ip, ip_weight, now_ns)
                .map_err(Into::into),
        }
    }

    /// Read-only probe checking whether `local_ip` has sufficient IP weight for `ReadAction` at `now_ns`.
    pub fn can_afford_read_ip(
        &self,
        local_ip: IpKey,
        action: ReadAction,
        now_ns: u64,
    ) -> core::result::Result<(), MultiIpQuotaError> {
        if self.is_account_banned() {
            return Err(MultiIpQuotaError::AccountBanned);
        }
        let ActionBudgetClass::Query { ip_weight } = classify_read_action(self.source, action)?
        else {
            return Err(MultiIpQuotaError::Invalid);
        };
        self.venue_limiter
            .can_afford_query(local_ip, ip_weight, now_ns)
            .map_err(Into::into)
    }

    /// Consumes quota on `local_ip` for a classified `CommandAction` without intent deduplication.
    pub fn consume_for_ip(
        &mut self,
        local_ip: IpKey,
        transport: Transport,
        action: CommandAction,
        symbol: u64,
        now_ns: u64,
    ) -> core::result::Result<RatePermit, MultiIpQuotaError> {
        if self.is_account_banned() {
            return Err(MultiIpQuotaError::AccountBanned);
        }
        match classify_command_action(self.source, transport, action)? {
            ActionBudgetClass::Order {
                order_units,
                ip_weight,
            } => self
                .venue_limiter
                .allow_place_orders(local_ip, symbol, order_units, ip_weight, now_ns)
                .map_err(Into::into),
            ActionBudgetClass::Modify {
                counts_as_order,
                ip_weight,
            } => self
                .venue_limiter
                .allow_modify(local_ip, symbol, ip_weight, counts_as_order, now_ns)
                .map_err(Into::into),
            ActionBudgetClass::Cancel { ip_weight } => self
                .venue_limiter
                .allow_cancel(local_ip, symbol, ip_weight, now_ns)
                .map_err(Into::into),
            ActionBudgetClass::Query { ip_weight } => self
                .venue_limiter
                .allow_query(local_ip, ip_weight, now_ns)
                .map_err(Into::into),
        }
    }

    /// Consumes quota on `local_ip` for a reconciliation `ReadAction` (pure IP weight; never touches `ORDER`).
    pub fn consume_read_for_ip(
        &mut self,
        local_ip: IpKey,
        action: ReadAction,
        now_ns: u64,
    ) -> core::result::Result<RatePermit, MultiIpQuotaError> {
        if self.is_account_banned() {
            return Err(MultiIpQuotaError::AccountBanned);
        }
        let ActionBudgetClass::Query { ip_weight } = classify_read_action(self.source, action)?
        else {
            return Err(MultiIpQuotaError::Invalid);
        };
        self.venue_limiter
            .allow_query(local_ip, ip_weight, now_ns)
            .map_err(Into::into)
    }

    /// Consumes quota on `local_ip` with idempotent zero-byte retry deduplication keyed by
    /// `(intent_exec_epoch, intent_id, transport, local_ip)`.
    ///
    /// Retrying the same `PreparedSend` after a zero-byte `WouldBlock` poll on the same link
    /// returns `Ok(Consumed::Duplicate)` without double-debiting any account or IP window.
    /// Reusing `(intent_exec_epoch, intent_id)` with a conflicting `digest` freezes the account.
    #[allow(clippy::too_many_arguments)]
    pub fn consume_intent_for_ip(
        &mut self,
        local_ip: IpKey,
        intent_exec_epoch: u64,
        intent_id: u64,
        transport: Transport,
        action: CommandAction,
        symbol: u64,
        digest: [u8; 32],
        now_ns: u64,
    ) -> core::result::Result<Consumed, MultiIpQuotaError> {
        if self.is_account_banned() {
            return Err(MultiIpQuotaError::AccountBanned);
        }
        if intent_exec_epoch == 0 || intent_id == 0 || digest == [0; 32] {
            return Err(MultiIpQuotaError::Identity);
        }
        if self.venue_limiter.ip_window_id(local_ip).is_none() {
            return Err(MultiIpQuotaError::Invalid);
        }
        if now_ns < self.venue_limiter.limiter().last_now_ns() {
            return Err(RateReject::ClockRegression.into());
        }
        self.venue_limiter
            .limiter()
            .check_account(self.binding.account, now_ns)?;
        self.venue_limiter.limiter().check_ip(local_ip, now_ns)?;

        let mut exact_match = false;
        for entry in &self.attempts {
            if entry.intent_exec_epoch == intent_exec_epoch && entry.intent_id == intent_id {
                if entry.digest != digest {
                    let _ = self.venue_limiter.freeze_account();
                    return Err(MultiIpQuotaError::Conflict);
                }
                if entry.transport == transport && entry.local_ip == local_ip {
                    exact_match = true;
                }
            }
        }
        if exact_match {
            return Ok(Consumed::Duplicate);
        }
        if self.attempts.len() >= MAX_IDENTITIES {
            return Err(MultiIpQuotaError::Capacity);
        }
        self.consume_for_ip(local_ip, transport, action, symbol, now_ns)?;
        self.attempts.push(MultiIpAttempt {
            intent_exec_epoch,
            intent_id,
            transport,
            local_ip,
            digest,
        });
        Ok(Consumed::New)
    }

    /// Consumes quota inside the fresh first-byte gate for an authenticated [`venue::CanonicalCommand`]
    /// bound to `local_ip`, deduplicating zero-byte retries by `(intent_exec_epoch, intent_id, transport, local_ip)`.
    pub fn consume_command_for_ip(
        &mut self,
        local_ip: IpKey,
        command: &venue::CanonicalCommand,
        transport: Transport,
        now: MonoTime,
    ) -> core::result::Result<Consumed, MultiIpQuotaError> {
        if now.domain() != self.binding.domain {
            let _ = self.venue_limiter.freeze_account();
            return Err(MultiIpQuotaError::Identity);
        }
        if self.is_account_banned() {
            return Err(MultiIpQuotaError::AccountBanned);
        }
        let c = command.command();
        let r = &c.context.request;
        if r.venue != 1
            || r.risk_domain_id != self.binding.risk_domain_id
            || r.account != self.binding.account
            || r.product != self.binding.product
            || r.environment != self.binding.environment
            || now.nanos() >= r.deadline
        {
            return Err(MultiIpQuotaError::Identity);
        }
        let action = command_action(&c.body);
        let symbol = c.context.instrument.instrument_id;
        self.consume_intent_for_ip(
            local_ip,
            r.intent_exec_epoch,
            r.intent_id,
            transport,
            action,
            symbol,
            command.digest(),
            now.nanos(),
        )
    }

    /// Isolates an HTTP `429` / WS IP rate-limit cooldown strictly to `local_ip`, leaving sibling
    /// `local_ip` links live and unaffected.
    pub fn note_ip_rate_limited(
        &mut self,
        local_ip: IpKey,
        until_ns: u64,
    ) -> core::result::Result<(), MultiIpQuotaError> {
        self.venue_limiter
            .note_ip_cooldown(local_ip, until_ns)
            .map_err(Into::into)
    }

    /// Freezes the account across all bound `local_ip` links upon HTTP `418` or an account-wide ban.
    pub fn note_account_banned(
        &mut self,
        until_ns: Option<u64>,
    ) -> core::result::Result<(), MultiIpQuotaError> {
        if let Some(until) = until_ns {
            self.venue_limiter.note_account_cooldown(until)?;
        }
        self.venue_limiter.freeze_account().map_err(Into::into)
    }

    /// Applies HTTP/WebSocket status feedback (`429` -> per-IP cooldown on `local_ip`;
    /// `418` -> account-wide ban freeze).
    pub fn observe_http_status(
        &mut self,
        local_ip: IpKey,
        status: u16,
        retry_until_ns: u64,
    ) -> core::result::Result<(), MultiIpQuotaError> {
        match status {
            429 => self.note_ip_rate_limited(local_ip, retry_until_ns),
            418 => self.note_account_banned(Some(retry_until_ns)),
            _ => Ok(()),
        }
    }

    /// Reconciles an exchange-reported window header (`X-MBX-USED-WEIGHT-*` / `X-MBX-ORDER-COUNT-*`)
    /// or WS `rateLimits[]` counter against `window_id`.
    pub fn reconcile_rate_limit_header(
        &mut self,
        window_id: RateLimitId,
        reported_used: u64,
        now_ns: u64,
    ) -> core::result::Result<ExchangeUsageReport, MultiIpQuotaError> {
        self.venue_limiter
            .reconcile_exchange_usage(window_id, reported_used, now_ns)
            .map_err(Into::into)
    }

    /// Reconciles `X-MBX-USED-WEIGHT-1M` reported on `local_ip`.
    pub fn reconcile_ip_weight_header(
        &mut self,
        local_ip: IpKey,
        reported_used: u64,
        now_ns: u64,
    ) -> core::result::Result<ExchangeUsageReport, MultiIpQuotaError> {
        let win_id = self
            .venue_limiter
            .ip_window_id(local_ip)
            .ok_or(MultiIpQuotaError::Invalid)?;
        self.reconcile_rate_limit_header(win_id, reported_used, now_ns)
    }

    /// Reconciles `X-MBX-ORDER-COUNT-*` for the matching `interval_s` (`10`, `60`, or `86400`).
    pub fn reconcile_order_count_header(
        &mut self,
        interval_s: u32,
        reported_used: u64,
        now_ns: u64,
    ) -> core::result::Result<ExchangeUsageReport, MultiIpQuotaError> {
        let win_id = self
            .venue_limiter
            .account_order_window_ids()
            .iter()
            .copied()
            .find(|id| id.interval_s == interval_s)
            .ok_or(MultiIpQuotaError::Invalid)?;
        self.reconcile_rate_limit_header(win_id, reported_used, now_ns)
    }
}

pub use tickwire_quota::rate_limiter::{MAX_SPENDS_PER_CALL, RateRule};
pub use tickwire_quota::shared::{
    AccountRateOverrides, ExchRateLimitEvent, ExchRateLimitLevel, IpBudgetSpec, OrsAccountQuota,
    OrsVenueProfile, RouteAdmissionReject, SharedRateLimiter, SharedSpend, SharedWindowHandle,
    VenueRateLimitRow, route_ip_key,
};

/// Binance execution market line inside a unified multi-product ORS (`1 ORS process = 1 Venue`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum BinanceMarket {
    Spot = 1,
    Usdm = 2,
    Coinm = 3,
    Options = 4,
}

impl BinanceMarket {
    pub const ALL: [Self; 4] = [Self::Spot, Self::Usdm, Self::Coinm, Self::Options];

    #[inline]
    pub const fn code(self) -> u64 {
        self as u64
    }

    #[inline]
    pub const fn profile(self) -> OrsVenueProfile {
        match self {
            Self::Spot => OrsVenueProfile::BinanceSpot,
            Self::Usdm => OrsVenueProfile::BinanceUsdm,
            Self::Coinm => OrsVenueProfile::BinanceCoinm,
            Self::Options => OrsVenueProfile::BinanceOptions,
        }
    }

    #[inline]
    pub const fn from_product(product: venue::Product) -> Self {
        match product {
            venue::Product::Spot => Self::Spot,
            venue::Product::LinearPerpetual
            | venue::Product::LinearFuture
            | venue::Product::UsdM => Self::Usdm,
            venue::Product::InversePerpetual
            | venue::Product::InverseFuture
            | venue::Product::CoinM => Self::Coinm,
            venue::Product::Option => Self::Options,
        }
    }

    /// Returns the `OrderRoute.product_mask` bitmask served by this Binance market line.
    #[inline]
    pub const fn product_mask(self) -> u16 {
        match self {
            Self::Spot => venue::Product::Spot.mask_bit(),
            Self::Usdm => {
                venue::Product::LinearPerpetual.mask_bit()
                    | venue::Product::LinearFuture.mask_bit()
                    | venue::Product::UsdM.mask_bit()
            }
            Self::Coinm => {
                venue::Product::InversePerpetual.mask_bit()
                    | venue::Product::InverseFuture.mask_bit()
                    | venue::Product::CoinM.mask_bit()
            }
            Self::Options => venue::Product::Option.mask_bit(),
        }
    }

    /// Returns the shared cross-account IP weight [`RateLimitId`] (`account = 0`) for `(self, local_ip)`.
    #[inline]
    pub const fn ip_window_id(self, local_ip: IpKey) -> RateLimitId {
        let (interval_s, _, flags) = self.profile().defaults().shared_ip_budget;
        RateLimitId {
            local_ip,
            interval_s,
            account: 0,
            flags: RateFlags::PER_IP.union(flags),
            symbol: self.code(),
        }
    }

    /// Returns the primary account `ORDER` [`RateLimitId`] for `(account_ref, self)`.
    #[inline]
    pub const fn primary_order_window_id(self, account_ref: u8) -> Option<RateLimitId> {
        match self.profile().defaults().account_order_primary {
            Some((interval_s, _, flags)) if account_ref != 0 => {
                Some(RateRule::product(account_ref as u64, interval_s, flags, 1, self.code()).id)
            }
            _ => None,
        }
    }

    /// Returns the secondary account `ORDER` [`RateLimitId`] for `(account_ref, self)`, if any.
    #[inline]
    pub const fn secondary_order_window_id(self, account_ref: u8) -> Option<RateLimitId> {
        match self.profile().defaults().account_order_secondary {
            Some((interval_s, _, flags)) if account_ref != 0 => {
                Some(RateRule::product(account_ref as u64, interval_s, flags, 1, self.code()).id)
            }
            _ => None,
        }
    }
}

/// Classifies a [`CommandAction`] across all four Binance market lines (`Spot`, `Usdm`, `Coinm`, `Options`)
/// into isolated `ORDER`, `MODIFY`, `CANCEL`, or `QUERY` budgets.
pub fn classify_market_command_action(
    market: BinanceMarket,
    transport: Transport,
    action: CommandAction,
) -> core::result::Result<ActionBudgetClass, MultiIpQuotaError> {
    match market {
        BinanceMarket::Spot => classify_command_action(Source::SpotProduction, transport, action),
        BinanceMarket::Usdm => classify_command_action(Source::UsdmProduction, transport, action),
        BinanceMarket::Coinm => {
            let new_ip_weight = match transport {
                Transport::Rest => 1,
                Transport::WebSocket => 0,
            };
            match action {
                CommandAction::New | CommandAction::Algo => Ok(ActionBudgetClass::Order {
                    order_units: 1,
                    ip_weight: new_ip_weight,
                }),
                CommandAction::Modify => Ok(ActionBudgetClass::Modify {
                    counts_as_order: true,
                    ip_weight: 1,
                }),
                CommandAction::Cancel | CommandAction::CancelAlgo => {
                    Ok(ActionBudgetClass::Cancel { ip_weight: 1 })
                }
                CommandAction::Query => Ok(ActionBudgetClass::Query { ip_weight: 1 }),
                CommandAction::CancelReplace
                | CommandAction::AdvancedCancelReplace
                | CommandAction::PeggedNew
                | CommandAction::Sor
                | CommandAction::SpotList { .. }
                | CommandAction::CancelList
                | CommandAction::Amend => Err(MultiIpQuotaError::UnsupportedAction),
            }
        }
        BinanceMarket::Options => {
            let new_ip_weight = match transport {
                Transport::Rest => 1,
                Transport::WebSocket => 0,
            };
            match action {
                CommandAction::New => Ok(ActionBudgetClass::Order {
                    order_units: 1,
                    ip_weight: new_ip_weight,
                }),
                CommandAction::Modify => Ok(ActionBudgetClass::Modify {
                    counts_as_order: true,
                    ip_weight: 1,
                }),
                CommandAction::Cancel => Ok(ActionBudgetClass::Cancel { ip_weight: 1 }),
                CommandAction::Query => Ok(ActionBudgetClass::Query { ip_weight: 1 }),
                CommandAction::Algo
                | CommandAction::CancelAlgo
                | CommandAction::CancelReplace
                | CommandAction::AdvancedCancelReplace
                | CommandAction::PeggedNew
                | CommandAction::Sor
                | CommandAction::SpotList { .. }
                | CommandAction::CancelList
                | CommandAction::Amend => Err(MultiIpQuotaError::UnsupportedAction),
            }
        }
    }
}

/// Classifies a reconciliation/query [`ReadAction`] across all four Binance market lines.
pub fn classify_market_read_action(
    market: BinanceMarket,
    action: ReadAction,
) -> core::result::Result<ActionBudgetClass, MultiIpQuotaError> {
    match market {
        BinanceMarket::Spot => classify_read_action(Source::SpotProduction, action),
        BinanceMarket::Usdm => classify_read_action(Source::UsdmProduction, action),
        BinanceMarket::Coinm | BinanceMarket::Options => {
            let ip_weight = match action {
                ReadAction::Connect => 1,
                ReadAction::TradePage => 5,
                ReadAction::AccountSnapshot => 5,
                ReadAction::OpenOrders => 40,
                ReadAction::ConditionalOrders => 40,
                ReadAction::OrderQuery => 1,
                ReadAction::Income => 30,
                ReadAction::StructureQuery => 1,
                ReadAction::AmendmentHistory => return Err(MultiIpQuotaError::UnsupportedAction),
            };
            Ok(ActionBudgetClass::Query { ip_weight })
        }
    }
}

/// Pre-installed `(account_ref, market)` `ORDER` window handles inside [`SharedBinanceOrsQuota`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BinanceAccountMarketHandles {
    pub market: BinanceMarket,
    pub account_ref: u8,
    pub order_primary: SharedWindowHandle,
    pub order_secondary: Option<SharedWindowHandle>,
}

/// Process-wide multi-account, multi-market Binance ORS quota facade over [`SharedRateLimiter`].
///
/// Shares per-IP request-weight windows (`account = 0`, `ips = [{ ip, ip_weight_min }]`) across
/// all `AccountSession`s (`account_ref = 1..=255`) bound to the same `local_ip`, isolates per-account
/// per-market `ORDER` caps across Spot (`100/10s`, `200,000/86,400s`), USD-M (`300/10s`, `1,200/60s`),
/// COIN-M (`1,200/60s`), and Options (`100/10s`, `1,200/60s`), enforces single-pick NACK admission
/// (`pick -> spend(route.local_ip) -> NACK immediately`), and emits rising-edge [`ExchRateLimitEvent`]s
/// at `exch_warn_pct` (`80%`) and `100%` ceiling.
#[derive(Clone, Debug)]
pub struct SharedBinanceOrsQuota {
    limiter: std::sync::Arc<SharedRateLimiter>,
}

impl Default for SharedBinanceOrsQuota {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedBinanceOrsQuota {
    /// Creates a new [`SharedBinanceOrsQuota`] with default `4,096` slots and `80%` warn threshold.
    pub fn new() -> Self {
        Self {
            limiter: std::sync::Arc::new(SharedRateLimiter::new()),
        }
    }

    /// Wraps an existing process-wide [`SharedRateLimiter`].
    pub fn from_limiter(limiter: std::sync::Arc<SharedRateLimiter>) -> Self {
        Self { limiter }
    }

    /// Creates a [`SharedBinanceOrsQuota`] with explicit `table_slots` and `exch_warn_pct`.
    pub fn with_config(
        table_slots: usize,
        exch_warn_pct: u8,
    ) -> core::result::Result<Self, RateReject> {
        Ok(Self {
            limiter: std::sync::Arc::new(SharedRateLimiter::with_config(
                table_slots,
                exch_warn_pct,
            )?),
        })
    }

    #[inline]
    pub fn limiter(&self) -> &SharedRateLimiter {
        &self.limiter
    }

    #[inline]
    pub fn limiter_arc(&self) -> &std::sync::Arc<SharedRateLimiter> {
        &self.limiter
    }

    /// Installs `ORS.RATE_LIMITER.ips = [{ ip, ip_weight_min }]` for `market`.
    ///
    /// Every `AccountSession` bound to `ip` shares the installed `(ip, market)` weight window.
    pub fn install_ip_budgets(
        &self,
        market: BinanceMarket,
        ips: &[IpBudgetSpec],
    ) -> core::result::Result<(), RateReject> {
        let (_, default_limit, _) = market.profile().defaults().shared_ip_budget;
        for entry in ips {
            let ip_key = IpKey::from_ip(entry.ip);
            if ip_key.is_none() {
                return Err(RateReject::Invalid);
            }
            let limit = if entry.ip_weight_min == 0 {
                default_limit
            } else {
                entry.ip_weight_min
            };
            self.limiter.add(RateRule {
                id: market.ip_window_id(ip_key),
                limit,
            })?;
        }
        Ok(())
    }

    #[inline]
    fn resolve_ip_window(
        &self,
        market: BinanceMarket,
        local_ip: IpKey,
    ) -> core::result::Result<SharedWindowHandle, RateReject> {
        if local_ip.is_none() {
            return Err(RateReject::Invalid);
        }
        let (_, default_limit, _) = market.profile().defaults().shared_ip_budget;
        self.limiter.add(RateRule {
            id: market.ip_window_id(local_ip),
            limit: default_limit,
        })
    }

    /// Pre-registers `(account_ref, market)` `ORDER` windows with optional `ACCOUNT_MANAGER` cap overrides.
    pub fn register_account(
        &self,
        market: BinanceMarket,
        account_ref: u8,
        overrides: AccountRateOverrides,
    ) -> core::result::Result<BinanceAccountMarketHandles, RateReject> {
        if account_ref == 0 {
            return Err(RateReject::Invalid);
        }
        let defaults = market.profile().defaults();
        let (p_int, p_def, p_flags) = defaults.account_order_primary.ok_or(RateReject::Invalid)?;
        let p_cap = overrides.primary_order_limit.unwrap_or(p_def);
        let order_primary = self.limiter.add(RateRule::product(
            u64::from(account_ref),
            p_int,
            p_flags,
            p_cap,
            market.code(),
        ))?;

        let order_secondary = match defaults.account_order_secondary {
            Some((s_int, s_def, s_flags)) => {
                let s_cap = overrides.secondary_order_limit.unwrap_or(s_def);
                Some(self.limiter.add(RateRule::product(
                    u64::from(account_ref),
                    s_int,
                    s_flags,
                    s_cap,
                    market.code(),
                ))?)
            }
            None => None,
        };

        Ok(BinanceAccountMarketHandles {
            market,
            account_ref,
            order_primary,
            order_secondary,
        })
    }

    fn build_command_spends(
        &self,
        market: BinanceMarket,
        account_ref: u8,
        local_ip: IpKey,
        transport: Transport,
        action: CommandAction,
        out: &mut [SharedSpend; MAX_SPENDS_PER_CALL],
    ) -> core::result::Result<usize, MultiIpQuotaError> {
        if account_ref == 0 || local_ip.is_none() {
            return Err(MultiIpQuotaError::Identity);
        }
        let budget = classify_market_command_action(market, transport, action)?;
        let defaults = market.profile().defaults();
        let mut len = 0usize;

        let (order_units, ip_weight) = match budget {
            ActionBudgetClass::Order {
                order_units,
                ip_weight,
            } => (order_units, ip_weight),
            ActionBudgetClass::Modify {
                counts_as_order,
                ip_weight,
            } => (u32::from(counts_as_order), ip_weight),
            ActionBudgetClass::Cancel { ip_weight } | ActionBudgetClass::Query { ip_weight } => {
                (0, ip_weight)
            }
        };

        if order_units > 0 {
            if let Some((p_int, p_def, p_flags)) = defaults.account_order_primary {
                out[len] = SharedSpend::rule(
                    RateRule::product(u64::from(account_ref), p_int, p_flags, p_def, market.code()),
                    order_units,
                );
                len += 1;
            }
            if let Some((s_int, s_def, s_flags)) = defaults.account_order_secondary {
                out[len] = SharedSpend::rule(
                    RateRule::product(u64::from(account_ref), s_int, s_flags, s_def, market.code()),
                    order_units,
                );
                len += 1;
            }
        } else {
            // Even when order_units == 0 (Cancel/Query/Spot Amend), verify account ban/cooldown!
            if let Some((p_int, p_def, p_flags)) = defaults.account_order_primary {
                out[len] = SharedSpend::rule(
                    RateRule::product(u64::from(account_ref), p_int, p_flags, p_def, market.code()),
                    0,
                );
                len += 1;
            }
        }

        let ip_handle = self.resolve_ip_window(market, local_ip)?;
        out[len] = SharedSpend::handle(ip_handle, ip_weight);
        len += 1;
        Ok(len)
    }

    /// Spends `(account_ref, market)` and shared `(local_ip, market)` budgets for an outbound command.
    pub fn spend_command_for_ip(
        &self,
        market: BinanceMarket,
        account_ref: u8,
        local_ip: IpKey,
        transport: Transport,
        action: CommandAction,
        now_ns: u64,
    ) -> core::result::Result<RatePermit, MultiIpQuotaError> {
        let mut buf = [SharedSpend::rule(RateRule::account(1, 1, RateFlags::REQUEST, 1), 0);
            MAX_SPENDS_PER_CALL];
        let len =
            self.build_command_spends(market, account_ref, local_ip, transport, action, &mut buf)?;
        self.limiter.allow(&buf[..len], now_ns).map_err(Into::into)
    }

    /// Enforces `pick connection -> spend(conn.local_ip) -> on limit exceeded return NACK immediately`
    /// (`pick_once` is invoked at most once; never retries another connection on rate-limit hit).
    pub fn pick_route_and_spend_command_or_nack<FPick>(
        &self,
        market: BinanceMarket,
        account_ref: u8,
        transport: Transport,
        action: CommandAction,
        pick_once: FPick,
        now_ns: u64,
    ) -> core::result::Result<(venue::OrderRoute, RatePermit), RouteAdmissionReject>
    where
        FPick: FnOnce() -> Option<venue::OrderRoute>,
    {
        self.limiter.pick_and_spend_or_nack(
            pick_once,
            |route, buf| {
                let local_ip = route_ip_key(route)?;
                self.build_command_spends(market, account_ref, local_ip, transport, action, buf)
                    .map_err(|err| match err {
                        MultiIpQuotaError::Rate(r) => RouteAdmissionReject::RateLimitedNack(r),
                        MultiIpQuotaError::AccountBanned => {
                            RouteAdmissionReject::RateLimitedNack(RateReject::Frozen {
                                account: u64::from(account_ref),
                            })
                        }
                        _ => RouteAdmissionReject::RateLimitedNack(RateReject::Invalid),
                    })
            },
            now_ns,
        )
    }

    /// Spends pure IP request weight for `AsyncRestQuerier` / `QueryDesk` background queries on `local_ip`
    /// (verifying `account_ref` is not frozen while never touching `ORDER` windows).
    pub fn spend_query_for_ip(
        &self,
        market: BinanceMarket,
        account_ref: u8,
        local_ip: IpKey,
        action: ReadAction,
        now_ns: u64,
    ) -> core::result::Result<RatePermit, MultiIpQuotaError> {
        if account_ref == 0 || local_ip.is_none() {
            return Err(MultiIpQuotaError::Identity);
        }
        self.limiter.check_account(u64::from(account_ref), now_ns)?;
        let ActionBudgetClass::Query { ip_weight } = classify_market_read_action(market, action)?
        else {
            return Err(MultiIpQuotaError::Invalid);
        };
        let ip_handle = self.resolve_ip_window(market, local_ip)?;
        let spends = [SharedSpend::handle(ip_handle, ip_weight)];
        self.limiter.allow(&spends, now_ns).map_err(Into::into)
    }

    /// Applies HTTP/WebSocket status feedback (`429` -> per-IP cooldown across all accounts on `local_ip`;
    /// `418` -> freezes `account_ref` across all IPs).
    pub fn observe_http_status(
        &self,
        account_ref: u8,
        local_ip: IpKey,
        status: u16,
        retry_until_ns: u64,
    ) -> core::result::Result<(), MultiIpQuotaError> {
        match status {
            429 => self
                .limiter
                .note_ip_cooldown(local_ip, retry_until_ns)
                .map_err(Into::into),
            418 => {
                if account_ref == 0 {
                    return Err(MultiIpQuotaError::Identity);
                }
                let acct = u64::from(account_ref);
                self.limiter.note_account_cooldown(acct, retry_until_ns)?;
                self.limiter.freeze_account(acct).map_err(Into::into)
            }
            _ => Ok(()),
        }
    }

    /// Reconciles an arbitrary venue `rateLimits[]` row into a rising-edge [`ExchRateLimitEvent`].
    pub fn reconcile_venue_usage(
        &self,
        id: RateLimitId,
        reported_count: u64,
        reported_limit: u64,
        now_ns: u64,
    ) -> core::result::Result<Option<ExchRateLimitEvent>, RateReject> {
        self.limiter
            .reconcile_venue_usage(id, reported_count, reported_limit, now_ns)
    }

    /// Reconciles an exchange-reported `REQUEST_WEIGHT` row on `(market, local_ip)`.
    pub fn reconcile_ip_weight(
        &self,
        market: BinanceMarket,
        local_ip: IpKey,
        reported_count: u64,
        reported_limit: u64,
        now_ns: u64,
    ) -> core::result::Result<Option<ExchRateLimitEvent>, RateReject> {
        if local_ip.is_none() {
            return Err(RateReject::Invalid);
        }
        self.limiter.reconcile_venue_usage(
            market.ip_window_id(local_ip),
            reported_count,
            reported_limit,
            now_ns,
        )
    }

    /// Reconciles an exchange-reported `ORDERS` row for `(market, account_ref, interval_s)`.
    pub fn reconcile_order_count(
        &self,
        market: BinanceMarket,
        account_ref: u8,
        interval_s: u32,
        reported_count: u64,
        reported_limit: u64,
        now_ns: u64,
    ) -> core::result::Result<Option<ExchRateLimitEvent>, RateReject> {
        if account_ref == 0 {
            return Err(RateReject::Invalid);
        }
        let id = [
            market.primary_order_window_id(account_ref),
            market.secondary_order_window_id(account_ref),
        ]
        .into_iter()
        .flatten()
        .find(|candidate| candidate.interval_s == interval_s)
        .ok_or(RateReject::Invalid)?;
        self.limiter
            .reconcile_venue_usage(id, reported_count, reported_limit, now_ns)
    }
}
