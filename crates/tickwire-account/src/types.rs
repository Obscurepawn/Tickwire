use std::collections::BTreeMap;
use tickwire_risk::{Fixed, futures::Bracket};
use tickwire_venue as v;

pub const MAX_ASSETS: usize = 64;
pub const MAX_INSTRUMENTS: usize = 64;
pub const MAX_ORDERS: usize = 1024;
pub const MAX_FACTS: usize = 16384;
pub const MAX_STRATEGIES: usize = 1024;
pub const MAX_BRACKETS: usize = 128;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct AssetKey {
    pub wallet_id: u64,
    pub asset_id: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssetAmount {
    pub key: AssetKey,
    pub amount: Fixed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct IntentKey {
    pub epoch: u64,
    pub intent_id: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OrderKey {
    pub intent: IntentKey,
    pub client_id: v::ClientId,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructureView {
    pub structure_id: [u8; 16],
    pub intent: IntentKey,
    pub kind: v::StructureKind,
    pub command_digest: v::Hash,
    pub venue_parent_id: Option<v::VenueOrderId>,
    pub confirmed: bool,
    pub triggered: bool,
    pub terminal: bool,
    pub unknown: bool,
    pub legs: Vec<OrderKey>,
    pub children: Vec<v::StructureChild>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub risk_domain_id: [u8; 16],
    pub venue: u32,
    pub environment: v::Environment,
    pub account: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Revisions {
    pub risk: u64,
    pub fee: u64,
    pub bracket: u64,
    pub collateral: u64,
    pub valuation: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetSnapshot {
    pub key: AssetKey,
    pub asset: v::AssetRef,
    pub free: Fixed,
    pub locked: Fixed,
    pub nontrade_locked: Fixed,
    pub exclusive_fee_pool: bool,
    /// Strategy 0 is the explicit unattributed bucket. Missing remainder goes there.
    pub ownership: Vec<(u64, Fixed)>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionSnapshot {
    pub instrument_id: u64,
    pub side: v::PositionSide,
    pub quantity: Fixed,
    /// Conservative entry bound: long upper / short lower; no positive UPL is credited.
    pub entry_bound: Fixed,
    pub ownership: Vec<(u64, Fixed)>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub scope: Scope,
    pub account_epoch: u64,
    pub ledger_sequence: u64,
    pub revisions: Revisions,
    pub valid_until: u64,
    pub assets: Vec<AssetSnapshot>,
    pub positions: Vec<PositionSnapshot>,
    pub venue_available: Vec<(u64, Fixed)>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeePolicy {
    /// General, tax and special fees are rounded separately. All are nonnegative upper bounds.
    pub rates: [Fixed; 3],
    /// Trusted protocol/fee envelope bound, never inferred from LOT_SIZE.
    pub maximum_fills: u32,
    /// Optional account-exclusive third-asset pool; receiving-asset fallback is still reserved.
    pub exclusive_pool: Option<AssetKey>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuturesPolicy {
    pub position_mode: v::PositionMode,
    pub margin_mode: v::MarginMode,
    pub stress_low: Fixed,
    pub stress_high: Fixed,
    pub configured_leverage: Fixed,
    pub venue_leverage: Fixed,
    pub brackets: Vec<Bracket>,
    pub maintenance_addon: Fixed,
    pub exit_fee_rate: Fixed,
    pub funding_rate_abs_upper: Fixed,
    pub funding_settlements: u64,
    pub pending_outbound: Fixed,
    pub capital_buffer: Fixed,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentPolicy {
    pub instrument: v::InstrumentBinding,
    pub product: v::Product,
    pub wallet_id: u64,
    pub fees: FeePolicy,
    pub futures: Option<FuturesPolicy>,
    pub max_absolute_position: Fixed,
    pub max_gross_quantity: Fixed,
    /// Every submitting strategy needs its own explicit gross-quantity limit.
    pub strategy_limits: Vec<(u64, Fixed)>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    pub entries: Vec<InstrumentPolicy>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapitalHold {
    pub wallet_id: u64,
    pub settlement_asset: u64,
    pub amount: Fixed,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    pub(crate) digest: v::Hash,
    pub(crate) key: IntentKey,
    pub(crate) epoch: u64,
    pub(crate) sequence: u64,
    pub(crate) revisions: Revisions,
    pub(crate) deadline: u64,
    pub(crate) resources: Vec<AssetAmount>,
    pub(crate) capital: Vec<CapitalHold>,
    pub(crate) orders: Vec<OrderKey>,
    pub(crate) increases: bool,
}
impl Admission {
    pub fn command_digest(&self) -> v::Hash {
        self.digest
    }
    pub fn intent(&self) -> IntentKey {
        self.key
    }
    pub fn account_epoch(&self) -> u64 {
        self.epoch
    }
    pub fn ledger_sequence(&self) -> u64 {
        self.sequence
    }
    pub fn revisions(&self) -> Revisions {
        self.revisions
    }
    pub fn resources(&self) -> &[AssetAmount] {
        &self.resources
    }
    pub fn capital_holds(&self) -> &[CapitalHold] {
        &self.capital
    }
    pub fn orders(&self) -> &[OrderKey] {
        &self.orders
    }
    pub fn increases_risk(&self) -> bool {
        self.increases
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssetView {
    pub total: Fixed,
    pub reserved: Fixed,
    pub available: Fixed,
    pub deficit: Fixed,
    pub ownership: BTreeMap<u64, Fixed>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderView {
    pub key: OrderKey,
    pub command_digest: v::Hash,
    pub strategy: u64,
    pub origin: ManagerIdentity,
    pub manager: ManagerIdentity,
    pub owner_revision: u64,
    pub original_quantity: Fixed,
    pub effective_quantity: Fixed,
    pub filled_quantity: Fixed,
    pub remaining_quantity: Fixed,
    pub reduce_allocation: Fixed,
    pub unknown: bool,
    pub terminal: bool,
    pub fee_reconciled: bool,
    pub aborted: bool,
    pub uncreated: bool,
    pub confirmed: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmendmentView {
    pub intent: IntentKey,
    pub command_digest: v::Hash,
    pub order: OrderKey,
    pub manager: ManagerIdentity,
    pub owner_revision: u64,
    pub previous_quantity: Fixed,
    pub new_quantity: Fixed,
    pub aborted: bool,
    pub confirmed: bool,
    pub rejected: Option<v::BoundedText<32>>,
    pub executions: Vec<(v::AmendmentSource, u64)>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    New,
    Duplicate,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Risk(tickwire_risk::RiskError),
    InvalidSnapshot,
    Identity,
    Catalog,
    Stale,
    Frozen,
    Unsupported,
    UnboundedExecution,
    InsufficientAsset,
    InsufficientCapital,
    Limit,
    UnknownOrder,
    Conflict,
    Unreconciled,
    Capacity,
    FactAfterAbort,
}
impl From<tickwire_risk::RiskError> for Error {
    fn from(e: tickwire_risk::RiskError) -> Self {
        Self::Risk(e)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

/// Untrusted persistence image. Only deterministic replay can produce a live Admission from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionImage {
    pub command_digest: v::Hash,
    pub intent: IntentKey,
    pub account_epoch: u64,
    pub ledger_sequence: u64,
    pub revisions: Revisions,
    pub deadline: u64,
    pub resources: Vec<AssetAmount>,
    pub capital_holds: Vec<CapitalHold>,
    pub orders: Vec<OrderKey>,
    pub increases: bool,
}
impl Admission {
    pub fn image(&self) -> AdmissionImage {
        AdmissionImage {
            command_digest: self.digest,
            intent: self.key,
            account_epoch: self.epoch,
            ledger_sequence: self.sequence,
            revisions: self.revisions,
            deadline: self.deadline,
            resources: self.resources.clone(),
            capital_holds: self.capital.clone(),
            orders: self.orders.clone(),
            increases: self.increases,
        }
    }
}
/// Ordered inputs persisted by the caller. `evaluated_at` is the original admission clock,
/// never the clock at which recovery happens. This enum is data, not an authorization proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryRecord {
    UncreatedAlgoClosed(Box<UncreatedAlgoClosed>),
    IncomeObservationsAssociated(Box<IncomeObservationsAssociated>),
    AccountInputsReconciled(Box<AccountInputsReconciled>),
    IncomeBaseline(Box<IncomeBaseline>),
    IngressAccountInput {
        input_stream: [u8; 16],
        input_sequence: u64,
        input: Box<v::CanonicalAccountInput>,
    },
    StrategyHandover(Box<StrategyHandover>),
    Reactivated(Box<Reactivation>),
    Admit {
        command: Box<v::CanonicalCommand>,
        evaluated_at: u64,
        expected: AdmissionImage,
    },
    VenueEvent(Box<v::CanonicalEvent>),
    IngressEvent {
        input_stream: [u8; 16],
        input_sequence: u64,
        event: Box<v::CanonicalEvent>,
    },
    Aborted {
        intent: IntentKey,
        command_digest: v::Hash,
    },
    FeesReconciled {
        order: OrderKey,
        cumulative: Fixed,
    },
    UnattributedCashFlow {
        id: v::BoundedText<64>,
        delta: AssetAmount,
    },
    ExternalPosition {
        id: v::BoundedText<64>,
        instrument: u64,
        side: v::PositionSide,
        quantity: Fixed,
        entry_bound: Fixed,
    },
    TransferAsset {
        key: AssetKey,
        from: u64,
        to: u64,
        amount: Fixed,
    },
    TransferPosition {
        instrument: u64,
        side: v::PositionSide,
        from: u64,
        to: u64,
        quantity: Fixed,
    },
}

/// Baseline income coverage is data. The durable gateway must verify its raw
/// query/snapshot dependencies before publishing or replaying it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomeBaseline {
    pub scope: Scope,
    pub account_epoch: u64,
    pub ledger_sequence: u64,
    pub wallet_id: u64,
    pub through_time_ms: u64,
    pub coverage_digest: v::Hash,
    pub baseline_digest: v::Hash,
    pub input_stream: [u8; 16],
    pub through_sequence: u64,
    pub covered: Vec<v::CanonicalAccountInput>,
}
/// Persisted closure binding. Its query/snapshot provenance is checked by the owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountInputsReconciled {
    pub scope: Scope,
    pub account_epoch: u64,
    pub ledger_sequence: u64,
    pub baseline_digest: v::Hash,
    pub history_digest: v::Hash,
    pub input_stream: [u8; 16],
    pub through_sequence: u64,
    pub state_digest: v::Hash,
    pub coverage_digest: v::Hash,
}
/// Exact income identity within the binding's account scope. Amount and time
/// remain content, never identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IncomeIdentity {
    pub income_type: v::BoundedText<64>,
    pub transaction_id: v::BoundedText<32>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationAssociation {
    pub observation_digest: v::Hash,
    pub incomes: Vec<IncomeIdentity>,
}
/// Data for a separately verified, scoped risk-admin decision. The reducer
/// neither verifies remote evidence nor creates authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomeObservationsAssociated {
    pub scope: Scope,
    pub account_epoch: u64,
    pub ledger_sequence: u64,
    pub wallet_id: u64,
    pub request_id: [u8; 16],
    pub baseline_digest: v::Hash,
    pub history_digest: v::Hash,
    pub input_stream: [u8; 16],
    /// Includes the durable approval, whose manifest binds the preceding prefix.
    pub through_sequence: u64,
    pub state_digest: v::Hash,
    pub manifest_digest: v::Hash,
    pub approval_digest: v::Hash,
    pub associations: Vec<ObservationAssociation>,
}
pub const MAX_ASSOCIATION_OBSERVATIONS: usize = 16;
pub const MAX_ASSOCIATION_INCOMES: usize = 128;
pub const MAX_ASSOCIATION_REQUESTS: usize = 1024;
pub const MAX_BASELINE_INCOMES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManagerIdentity {
    pub instance: [u8; 16],
    pub generation: u64,
}
/// One atomic transfer of all order-management responsibilities for a logical
/// strategy. The exact preceding ledger and complete order set are bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyHandover {
    pub scope: Scope,
    pub account_epoch: u64,
    pub writer_epoch: u64,
    pub operation_id: [u8; 16],
    pub operation_revision: u64,
    pub logical_strategy_id: u64,
    pub expected_ledger_sequence: u64,
    pub expected_orders_digest: v::Hash,
    pub from: ManagerIdentity,
    pub to: ManagerIdentity,
    pub checkpoint_digest: v::Hash,
    pub input_seal_digest: v::Hash,
    pub evaluated_at: u64,
    pub valid_until: u64,
}

/// Persisted account-reconciliation decision. Its external evidence is verified
/// before publication and again before historical replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reactivation {
    pub scope: Scope,
    pub product: v::Product,
    pub account_epoch: u64,
    pub ledger_sequence: u64,
    pub baseline_digest: v::Hash,
    pub history_digest: v::Hash,
    pub input_stream: [u8; 16],
    pub private_connection_epoch: u64,
    pub gap_generation: u64,
    pub through_sequence: u64,
    pub query_coverage_digest: v::Hash,
    pub clock_boot_id: [u8; 16],
    pub clock_generation: u64,
    pub evaluated_at: u64,
    pub valid_until: u64,
    pub writer_epoch: u64,
    pub execution_connection_epoch: u64,
    pub venue_available: Vec<(u64, Fixed)>,
}

/// Persisted evidence binding, not a caller-created permission. The gateway
/// verifies the exact parent query and full input/snapshot closure before use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UncreatedAlgoClosed {
    pub scope: Scope,
    pub account_epoch: u64,
    pub ledger_sequence: u64,
    pub baseline_digest: v::Hash,
    pub history_digest: v::Hash,
    pub input_stream: [u8; 16],
    pub through_sequence: u64,
    pub state_digest: v::Hash,
    pub coverage_digest: v::Hash,
    pub structure_id: [u8; 16],
    pub intent: IntentKey,
    pub command_digest: v::Hash,
    pub venue_parent_id: v::VenueOrderId,
}
