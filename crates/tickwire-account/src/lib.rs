//! Zero-allocation ORS `OrderManager` slot pool, `FillRecordPool`, lock-free atomic-BBO
//! `OrsStpBook`, `DopState` strategy session table, `OrsQueryDesk`, multi-wallet `UtaWalletLedger`,
//! and single-domain `Account` state machine (`#![forbid(unsafe_code)]`).
//!
//! # Architecture & Hot-Path Guarantees
//!
//! 1. **Zero-Allocation ORS `OrderManager<CHECK_STP>` ([`order_manager`])**:
//!    - Preallocates power-of-two per-strategy order slot rings (`255 * slots_per_strat`) plus
//!      dedicated per-strategy `dry_run` scratch slots in an optional `mlock(2)`-backed
//!      [`tickwire_shm_sys::MlockedSlice`].
//!    - Refuses slot collisions on `Claimed`/`Live` occupants immediately with
//!      `RefusalReason::ALLOCATION_ERROR` without resizing or evicting active orders.
//!    - Enforces fine-grained per-order spinlocks ([`order_manager::LockedOrder`]) and compile-time
//!      `LockedOrder -> LockedSymbolBook` lock ordering via [`ors_stp::OrderLockProof`], while
//!      send-path `self_cross` reads each symbol's published `AtomicI64` opposite-side best price
//!      with zero spinlocks.
//!    - Transitions full fills, cancels to zero, and rejects immediately to
//!      [`order_manager::SlotState::Retired`] and unlinks from [`ors_stp::OrsStpBook`] in the same
//!      call—never waiting on `fee_reconciled`.
//! 2. **Deduplicated Fills & Post-Trade Fee Amendments ([`fill_record`])**:
//!    - Preallocated `2^fill_map_exponent` bucket [`fill_record::FillRecordPool`] keyed by
//!      `(ors_order_id, exec_ref, side, symbol_id)` with lazy `fill_memory_s` overwrite reuse.
//!    - Deduplicates identical executions across order replies and private streams, and upgrades
//!      `TRADE_LITE` (`fee == None`) or post-trade commission/rebate patches into `amendment = true`
//!      reports that adjust wallet fee deltas only without double-applying fill quantities, holds,
//!      or positions.
//! 3. **Strategy Sessions, Reconciliation, Multi-Wallet UTA & Cold Bootstrap**:
//!    - [`dop_state::DopState`]: `strat_id` (`0..=254`), dual-sequence `login` handshake,
//!      `account_ref` routing (`0` -> first authorized account), and granular `(account_ref, strat_id)`
//!      `set_strat_enabled` returning open orders for auto-cancellation.
//!    - [`ors_query_desk::OrsQueryDesk`]: `StatusAsk` scheduling on `-1007`/`-1000`/`MKT_DELAYED`
//!      and socket drops, `Place`+`Cancel` `cancel_after` coalescing, `Cancel` exponential backoff
//!      (`1s..=64s`, max `8` attempts), fills-before-cancel snapshot reconciliation, non-`Live`
//!      eviction on full table, and optional `max_query_count` dead-slot release.
//!    - [`uta_ledger::UtaWalletLedger`] & `bootstrap_from_exchange`: multi-wallet Spot/USD-M/Coin-M/
//!      Options/Perp/Futures balances keyed by `(account_ref, AssetKey)` and cold recovery from
//!      Exchange REST snapshots (`Account + OpenOrders`) + `SelfDescribingClientId` (`tw1...`).
//! 4. **In-Place Single-Domain `Account` State Machine**:
//!    - Zero whole-struct `Account` cloning across `admit`, `apply_event`, `apply_account_input`,
//!      `apply_external`, and `apply_record`; immediate slot retirement without waiting on
//!      `fee_reconciled`; and `TRADE_LITE` / fee-patch `amendment` reconciliation without freezing.
#![forbid(unsafe_code)]
#[cfg(test)]
extern crate self as tickwire_account;
mod account_inputs;
mod admission;
mod amendments;
mod calculation;
pub mod dop_state;
mod events;
mod external;
pub mod fill_record;
mod handover;
mod live_bootstrap;
mod modifications;
pub mod order_manager;
pub mod ors_query_desk;
pub mod ors_stp;
pub mod pool;
mod projected_fill;
pub mod query_desk;
mod recovery;
pub mod spinlock;
pub mod stp_book;
mod structures;
#[cfg(test)]
#[path = "../tests/common/mod.rs"]
mod test_common;
mod types;
pub mod uta_ledger;
pub use dop_state::{ACCOUNT_REF_SLOTS, BitMask256, DopLoginAck, DopState, MAX_ORS_STRATS};
pub use fill_record::{
    FILL_BUCKET_WAYS, FillKey, FillObservationInput, FillRecordOutcome, FillRecordPool, OrsFeeSpec,
    trade_id_to_exec_ref,
};
pub use live_bootstrap::LiveBootstrap;
pub use order_manager::{
    InlineAscii, LockedOrder, OrderManager, OrderManagerConfig, OrsAccountDomainConfig,
    OrsAckOutcome, OrsAckReport, OrsAutoCancelTarget, OrsBootstrapSummary, OrsCancelDispatch,
    OrsCancelObservation, OrsCancelRejectedReport, OrsCancelRequest, OrsCanceledReport,
    OrsExchangeBootstrapSnapshot, OrsFillObservation, OrsFillReport, OrsFrontAckReport,
    OrsModifiedReport, OrsModifyDispatch, OrsModifyRejectedReport, OrsModifyRequest,
    OrsNewOrderRequest, OrsOrder, OrsOrderFilter, OrsOrderQuerySnapshot, OrsOrderRefusal,
    OrsOrderStatus, OrsPlaceOutcome, OrsQueriedOrderStatus, OrsQueryReconcileAction,
    OrsRejectedReport, OrsReport, OrsRestBalanceRow, OrsRestOpenOrderRow, OrsRestPositionRow,
    OrsVenueEvent, SlotState, encode_tw1_client_id_inline, make_ors_order_id, split_ors_order_id,
};
pub use ors_query_desk::{
    DEFAULT_QUERY_BASE_BACKOFF_NS, DueQueryAction, MAX_CANCEL_BACKOFF_ATTEMPTS, OrsQueryDesk,
    OrsQueryEntry, OrsQueryOrigin, cancel_backoff_delay_ns, is_ambiguous_venue_outcome,
};
pub use ors_stp::{EMPTY_ASK_TICKS, EMPTY_BID_TICKS, LockedSymbolBook, OrderLockProof, OrsStpBook};
pub use pool::{PoolFull, WindowedTable};
pub use projected_fill::{MAX_PROJECTED_FILLS, ProjectedFill, ProjectedPriceInput};
pub use query_desk::{
    AskKind, MAX_REST_SLOTS, MAX_STATUS_ASKS, QueryConfig, QueryDesk, QueryResolution, QueryTicket,
    SnapshotSummary,
};
pub use spinlock::{CachePadded, SpinLockGuard, TtasSpinLock};
use std::collections::{BTreeMap, BTreeSet};
pub use stp_book::{DEFAULT_STP_ORDERS_PER_SIDE, DEFAULT_STP_SYMBOLS, StpBook};
pub use tickwire_risk::Fixed;
use tickwire_risk::{RiskError, Rounding};
use tickwire_venue as v;
pub use types::*;
pub use uta_ledger::{
    MAX_WALLET_ASSETS_PER_ACCOUNT, OrsSymbolSpec, UtaWalletLedger, WalletAssetBalance,
};

#[derive(Debug, Clone)]
struct Asset {
    baseline: AssetSnapshot,
    cashflow: Fixed,
    credits: Fixed,
    other_debits: Fixed,
    ownership: BTreeMap<u64, Fixed>,
}
#[derive(Debug, Clone)]
struct Position {
    quantity: Fixed,
    entry: Fixed,
    ownership: BTreeMap<u64, Fixed>,
}
#[derive(Debug, Clone)]
struct Order {
    key: OrderKey,
    digest: v::Hash,
    instrument: u64,
    strategy: u64,
    origin: ManagerIdentity,
    manager: ManagerIdentity,
    spec: v::OrderSpec,
    amendable: bool,
    original: Fixed,
    effective: Fixed,
    observed_cumulative: Fixed,
    filled: Fixed,
    price: Fixed,
    received: Fixed,
    notional: Fixed,
    paid: BTreeMap<AssetKey, Fixed>,
    terminal_cumulative: Option<Fixed>,
    observed_unknown: bool,
    recovery_unknown: bool,
    observation_unreconciled: bool,
    terminal: bool,
    fee_reconciled: bool,
    aborted: bool,
    uncreated: bool,
    economic: bool,
    confirmed: bool,
    reduce: Fixed,
    native: Option<v::VenueOrderId>,
    internal_id: Option<[u8; 16]>,
    owner_revision: u64,
    shadow_fee_credit: Fixed,
    pending_modify: Option<modifications::PendingModify>,
    known_quantities: Vec<Fixed>,
    client_id_aliases: Vec<v::ClientId>,
    native_aliases: Vec<v::VenueOrderId>,
    exch_cum_qty: Fixed,
    exch_cum_quote: Option<Fixed>,
    exch_quote_qty: Fixed,
    exch_avg_price: Option<Fixed>,
    exch_avg_qty: Fixed,
    projected_cum_qty: Fixed,
    terminal_reason: Option<v::TerminalReason>,
    last_modify_reject_reason: Option<v::TerminalReason>,
    created_mono_ns: u64,
    retired_mono_ns: Option<u64>,
    in_doubt: Option<DoubtReason>,
}
impl PartialEq for Order {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.digest == other.digest
            && self.instrument == other.instrument
            && self.strategy == other.strategy
            && self.origin == other.origin
            && self.manager == other.manager
            && self.spec == other.spec
            && self.amendable == other.amendable
            && self.original == other.original
            && self.effective == other.effective
            && self.observed_cumulative == other.observed_cumulative
            && self.filled == other.filled
            && self.price == other.price
            && self.received == other.received
            && self.notional == other.notional
            && self.paid == other.paid
            && self.terminal_cumulative == other.terminal_cumulative
            && self.observed_unknown == other.observed_unknown
            && self.recovery_unknown == other.recovery_unknown
            && self.observation_unreconciled == other.observation_unreconciled
            && self.terminal == other.terminal
            && self.fee_reconciled == other.fee_reconciled
            && self.aborted == other.aborted
            && self.uncreated == other.uncreated
            && self.economic == other.economic
            && self.confirmed == other.confirmed
            && self.reduce == other.reduce
            && self.native == other.native
            && self.internal_id == other.internal_id
            && self.owner_revision == other.owner_revision
            && self.shadow_fee_credit == other.shadow_fee_credit
            && self.pending_modify == other.pending_modify
            && self.known_quantities == other.known_quantities
            && self.client_id_aliases == other.client_id_aliases
            && self.native_aliases == other.native_aliases
    }
}
impl Eq for Order {}
impl Order {
    pub(crate) fn has_client_id(&self, cid: &v::ClientId) -> bool {
        &self.key.client_id == cid
            || &self.spec.client_id == cid
            || self.client_id_aliases.contains(cid)
    }
    pub(crate) fn has_native_id(&self, vid: &v::VenueOrderId) -> bool {
        self.native.as_ref() == Some(vid) || self.native_aliases.contains(vid)
    }
    // A contradictory venue fact restores the original liability before a later
    // reconciliation can reenable admission. It never allocates a new ticket.
    fn reopen_aborted(&mut self) -> Result<bool> {
        if !self.aborted && !self.uncreated {
            return Ok(false);
        }
        if self.uncreated {
            self.observation_unreconciled = true;
        }
        self.aborted = false;
        self.uncreated = false;
        self.fee_reconciled = false;
        self.retired_mono_ns = None;
        self.terminal_reason = None;
        if matches!(self.spec.mode, v::ProductMode::Derivative { .. })
            && (self.spec.reduce_only
                || matches!(
                    (self.spec.position_side, self.spec.side),
                    (v::PositionSide::Long, v::Side::Sell) | (v::PositionSide::Short, v::Side::Buy)
                ))
        {
            self.reduce = self.effective.checked_sub(self.filled)?.positive_part();
        }
        Ok(true)
    }
    fn unknown(&self) -> bool {
        self.observed_unknown || self.recovery_unknown
    }
    fn remaining(&self) -> Result<Fixed> {
        if self.aborted
            || self.uncreated
            || self.terminal
                && self.terminal_cumulative == Some(self.filled)
                && self.observed_cumulative.atoms() <= self.filled.atoms()
        {
            Ok(self.original.zero_like())
        } else {
            Ok(self
                .effective
                .max(self.observed_cumulative)?
                .checked_sub(self.filled)?
                .positive_part())
        }
    }
    pub(crate) fn effective_remaining(&self) -> Result<Fixed> {
        let base_rem = self.remaining()?;
        if !self.aborted
            && !self.uncreated
            && !self.terminal
            && let Some(pm) = &self.pending_modify
        {
            let new_rem = pm
                .requested_quantity
                .checked_sub(self.filled.max(self.observed_cumulative)?)?
                .positive_part();
            Ok(base_rem.max(new_rem)?)
        } else {
            Ok(base_rem)
        }
    }
    pub(crate) fn is_retirable(&self) -> bool {
        (self.aborted
            || self.uncreated
            || (self.terminal && self.remaining().map(|r| r.atoms() == 0).unwrap_or(false)))
            && !self.unknown()
            && !self.observation_unreconciled
            && self.in_doubt.is_none()
            && self.pending_modify.is_none()
            && self.reduce.atoms() == 0
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct FactKey {
    order: OrderKey,
    kind: u8,
    id: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct FactEntry {
    fact: EconomicFact,
    recorded_mono_ns: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
enum EconomicFact {
    Fill {
        quantity: v::Decimal,
        price: v::Decimal,
        quote: Option<v::Decimal>,
        pnl: Option<v::AssetDelta>,
        fees: Vec<v::AssetDelta>,
    },
    Fee(Vec<v::AssetDelta>),
}
#[derive(Debug, Clone)]
pub struct Account {
    scope: Scope,
    epoch: u64,
    sequence: u64,
    revisions: Revisions,
    valid_until: u64,
    assets: BTreeMap<AssetKey, Asset>,
    catalog: std::sync::Arc<BTreeMap<u64, InstrumentPolicy>>,
    positions: BTreeMap<(u64, u8), Position>,
    orders: BTreeMap<OrderKey, Order>,
    orders_by_client_id: BTreeMap<(u64, v::ClientId), Vec<OrderKey>>,
    orders_by_native_id: BTreeMap<(u64, v::VenueOrderId), Vec<OrderKey>>,
    orders_by_internal_id: BTreeMap<[u8; 16], Vec<OrderKey>>,
    orders_by_intent: BTreeMap<IntentKey, Vec<OrderKey>>,
    structures: BTreeMap<[u8; 16], structures::Structure>,
    structure_management: BTreeMap<IntentKey, structures::Management>,
    amendments: BTreeMap<IntentKey, amendments::Amendment>,
    modifications: BTreeMap<IntentKey, modifications::ModificationRecord>,
    stp_book: StpBook,
    stp_enforcement: bool,
    projected_fill_enabled: bool,
    projected_fills: std::collections::VecDeque<ProjectedFill>,
    structure_observed: bool,
    admissions: BTreeMap<IntentKey, Admission>,
    facts: BTreeMap<FactKey, FactEntry>,
    external_facts: BTreeMap<(u8, String), external::ExternalValue>,
    account_inputs: account_inputs::AccountInputs,
    handovers: BTreeMap<([u8; 16], u64), StrategyHandover>,
    venue_available: BTreeMap<u64, Fixed>,
    shadow_holds: BTreeMap<IntentKey, Vec<CapitalHold>>,
    uncovered_debits: BTreeMap<u64, Fixed>,
    shadow_unvalued: BTreeSet<u64>,
    last_reactivation_sequence: u64,
    strategy_sequence_floors: BTreeMap<u64, IntentKey>,
    retention_window_ns: Option<u64>,
    last_mono_ns: u64,
    frozen: bool,
    /// First unapplied observation is retained if arithmetic/identity validation fails.
    rejected_event: Option<v::CanonicalEvent>,
}
impl Account {
    pub fn from_snapshot(snapshot: Snapshot, catalog: Catalog) -> Result<Self> {
        if snapshot.account_epoch == 0
            || snapshot.valid_until == 0
            || snapshot.assets.is_empty()
            || snapshot.assets.len() > MAX_ASSETS
            || snapshot.positions.len() > 2 * MAX_INSTRUMENTS
            || snapshot.venue_available.len() > MAX_ASSETS
            || catalog.entries.is_empty()
            || catalog.entries.len() > MAX_INSTRUMENTS
        {
            return Err(Error::InvalidSnapshot);
        }
        let mut result = Self {
            scope: snapshot.scope,
            epoch: snapshot.account_epoch,
            sequence: snapshot.ledger_sequence,
            revisions: snapshot.revisions,
            valid_until: snapshot.valid_until,
            assets: BTreeMap::new(),
            catalog: std::sync::Arc::new(BTreeMap::new()),
            positions: BTreeMap::new(),
            orders: BTreeMap::new(),
            orders_by_client_id: BTreeMap::new(),
            orders_by_native_id: BTreeMap::new(),
            orders_by_internal_id: BTreeMap::new(),
            orders_by_intent: BTreeMap::new(),
            structures: BTreeMap::new(),
            structure_management: BTreeMap::new(),
            amendments: BTreeMap::new(),
            modifications: BTreeMap::new(),
            stp_book: StpBook::with_capacity(MAX_INSTRUMENTS, MAX_ORDERS),
            stp_enforcement: false,
            projected_fill_enabled: false,
            projected_fills: std::collections::VecDeque::new(),
            structure_observed: false,
            admissions: BTreeMap::new(),
            facts: BTreeMap::new(),
            external_facts: BTreeMap::new(),
            account_inputs: account_inputs::AccountInputs::default(),
            handovers: BTreeMap::new(),
            venue_available: BTreeMap::new(),
            shadow_holds: BTreeMap::new(),
            uncovered_debits: BTreeMap::new(),
            shadow_unvalued: BTreeSet::new(),
            last_reactivation_sequence: 0,
            strategy_sequence_floors: BTreeMap::new(),
            retention_window_ns: Some(DEFAULT_RETENTION_WINDOW_NS),
            last_mono_ns: 0,
            frozen: false,
            rejected_event: None,
        };
        for a in snapshot.assets {
            let scale = asset_scale(&a.asset)?;
            for x in [a.free, a.locked, a.nontrade_locked] {
                x.nonnegative()?;
                if x.scale() != scale {
                    return Err(Error::InvalidSnapshot);
                }
            }
            // Existing live-order coverage requires a closed import/rebase API; never guess it.
            if a.key.asset_id != a.asset.id || a.locked != a.nontrade_locked {
                return Err(Error::InvalidSnapshot);
            }
            let total = a.free.checked_add(a.locked)?;
            let ownership = ownership(total, &a.ownership)?;
            let z = Fixed::zero(scale)?;
            let key = a.key;
            if result
                .assets
                .insert(
                    key,
                    Asset {
                        baseline: a,
                        cashflow: z,
                        credits: z,
                        other_debits: z,
                        ownership,
                    },
                )
                .is_some()
            {
                return Err(Error::InvalidSnapshot);
            }
        }
        for (wallet, value) in snapshot.venue_available {
            value.nonnegative()?;
            if result.venue_available.insert(wallet, value).is_some() {
                return Err(Error::InvalidSnapshot);
            }
        }
        let mut built_catalog = BTreeMap::new();
        for p in catalog.entries {
            result.catalog = std::sync::Arc::new(built_catalog);
            result.validate_policy(&p)?;
            built_catalog =
                std::sync::Arc::try_unwrap(result.catalog).unwrap_or_else(|a| (*a).clone());
            let tick_size =
                decimal_lossless(p.instrument.price_tick).map_err(|_| Error::Catalog)?;
            result
                .stp_book
                .register_instrument(p.instrument.instrument_id, tick_size)
                .map_err(|_| Error::Catalog)?;
            if built_catalog
                .insert(p.instrument.instrument_id, p)
                .is_some()
            {
                return Err(Error::Catalog);
            }
        }
        result.catalog = std::sync::Arc::new(built_catalog);
        for p in snapshot.positions {
            let policy = result.catalog.get(&p.instrument_id).ok_or(Error::Catalog)?;
            if policy.futures.is_none()
                || p.quantity.scale() != asset_scale(&policy.instrument.base)?
            {
                return Err(Error::InvalidSnapshot);
            }
            p.entry_bound.positive()?;
            if p.side != v::PositionSide::Both {
                p.quantity.nonnegative()?;
            }
            let expected = policy.futures.as_ref().unwrap().position_mode;
            if (p.side == v::PositionSide::Both) != (expected == v::PositionMode::OneWay) {
                return Err(Error::InvalidSnapshot);
            }
            let ownership = ownership(p.quantity, &p.ownership)?;
            if result
                .positions
                .insert(
                    (p.instrument_id, side_code(p.side)),
                    Position {
                        quantity: p.quantity,
                        entry: p.entry_bound,
                        ownership,
                    },
                )
                .is_some()
            {
                return Err(Error::InvalidSnapshot);
            }
        }
        // Validate even a flat account's configured capital formulas and wallet units.
        result.evaluate()?;
        Ok(result)
    }
    pub fn frozen(&self) -> bool {
        self.frozen
    }
    /// Deadline supplied by the baseline or the last durable Reactivated record.
    /// Reading it does not extend admission or clear a freeze.
    pub fn valid_until(&self) -> u64 {
        self.valid_until
    }
    /// Closes risk-increasing admission without changing balances, reservations or sequence.
    pub fn freeze(&mut self) {
        self.frozen = true;
    }
    pub fn order_keys(&self) -> impl Iterator<Item = &OrderKey> {
        self.orders.keys()
    }
    /// Immutable account identity for input-owner and continuous scope checks.
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    pub fn account_epoch(&self) -> u64 {
        self.epoch
    }
    pub fn ledger_sequence(&self) -> u64 {
        self.sequence
    }
    pub fn rejected_event(&self) -> Option<&v::CanonicalEvent> {
        self.rejected_event.as_ref()
    }
    /// Native ID that the reducer accepted for this registered order, if observed.
    pub fn venue_order_id(&self, key: &OrderKey) -> Option<&v::VenueOrderId> {
        self.orders.get(key)?.native.as_ref()
    }
    /// Compare an individually identified cash trade to the recovered economic fact.
    pub fn matches_spot_fill(
        &self,
        key: &OrderKey,
        trade: &v::VenueTradeId,
        quantity: v::Decimal,
        price: v::Decimal,
        executed_quote: v::Decimal,
        fees: &[v::AssetDelta],
    ) -> bool {
        matches!(
            self.facts
                .get(&FactKey {
                    order: key.clone(),
                    kind: 0,
                    id: trade.as_str().to_owned(),
                })
                .map(|e| &e.fact),
            Some(EconomicFact::Fill {
                quantity: q,
                price: p,
                quote: Some(n),
                pnl: None,
                fees: f
            }) if *q == quantity && *p == price && *n == executed_quote && f == fees
        )
    }
    /// Terms of the admitted order for matching native order and balance snapshots.
    pub fn order_terms(&self, key: &OrderKey) -> Option<(&v::InstrumentBinding, &v::OrderSpec)> {
        let order = self.orders.get(key)?;
        Some((
            &self.catalog.get(&order.instrument)?.instrument,
            &order.spec,
        ))
    }
    pub fn order(&self, key: &OrderKey) -> Option<OrderView> {
        self.orders.get(key).and_then(|o| {
            Some(OrderView {
                key: key.clone(),
                command_digest: o.digest,
                strategy: o.strategy,
                origin: o.origin,
                manager: o.manager,
                owner_revision: o.owner_revision,
                original_quantity: o.original,
                effective_quantity: o.effective,
                filled_quantity: o.filled,
                remaining_quantity: o.remaining().ok()?,
                reduce_allocation: o.reduce,
                unknown: o.unknown(),
                terminal: o.terminal,
                fee_reconciled: o.fee_reconciled,
                aborted: o.aborted,
                uncreated: o.uncreated,
                confirmed: o.confirmed,
            })
        })
    }
    pub fn position_entry_bound(
        &self,
        instrument: u64,
        side: v::PositionSide,
    ) -> Result<Option<Fixed>> {
        self.catalog.get(&instrument).ok_or(Error::Catalog)?;
        Ok(self
            .positions
            .get(&(instrument, side_code(side)))
            .map(|p| p.entry))
    }
    pub fn position(
        &self,
        instrument: u64,
        side: v::PositionSide,
    ) -> Result<(Fixed, BTreeMap<u64, Fixed>)> {
        let p = self.catalog.get(&instrument).ok_or(Error::Catalog)?;
        Ok(self
            .positions
            .get(&(instrument, side_code(side)))
            .map(|p| (p.quantity, p.ownership.clone()))
            .unwrap_or((
                Fixed::zero(asset_scale(&p.instrument.base)?)?,
                BTreeMap::new(),
            )))
    }
    fn next_sequence(&mut self) -> Result<()> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(RiskError::ArithmeticOverflow)?;
        Ok(())
    }
    fn freeze_error<T>(&mut self, error: Error) -> Result<T> {
        self.frozen = true;
        Err(error)
    }
    /// Changes ownership only across strategies without altering actual asset balances.
    pub fn transfer_asset_attribution(
        &mut self,
        key: AssetKey,
        from: u64,
        to: u64,
        amount: Fixed,
    ) -> Result<()> {
        if from == to {
            return Err(Error::Identity);
        }
        amount.nonnegative()?;
        self.sequence
            .checked_add(1)
            .ok_or(RiskError::ArithmeticOverflow)?;
        let (new_from, new_to) = {
            let a = self.assets.get(&key).ok_or(Error::Identity)?;
            let old_from = a
                .ownership
                .get(&from)
                .copied()
                .unwrap_or(amount.zero_like());
            let old_to = a.ownership.get(&to).copied().unwrap_or(amount.zero_like());
            (old_from.checked_sub(amount)?, old_to.checked_add(amount)?)
        };
        let a = self.assets.get_mut(&key).unwrap();
        a.ownership.insert(from, new_from);
        a.ownership.insert(to, new_to);
        self.next_sequence()?;
        if self.check_limits().is_err() {
            self.frozen = true;
        }
        Ok(())
    }

    pub fn with_retention_window_ns(mut self, window_ns: Option<u64>) -> Self {
        self.retention_window_ns = window_ns;
        self
    }

    pub fn set_retention_window_ns(&mut self, window_ns: Option<u64>) {
        self.retention_window_ns = window_ns;
    }

    pub fn retention_window_ns(&self) -> Option<u64> {
        self.retention_window_ns
    }

    pub fn admission_keys(&self) -> impl Iterator<Item = IntentKey> + '_ {
        self.admissions.keys().copied()
    }

    pub fn has_admission(&self, key: IntentKey) -> bool {
        self.admissions.contains_key(&key)
    }

    pub fn is_confirmed_amendment(&self, key: IntentKey) -> bool {
        self.amendments.get(&key).is_some_and(|a| a.confirmed)
    }

    pub fn is_confirmed_modification(&self, key: IntentKey) -> bool {
        self.modifications.get(&key).is_some_and(|m| m.confirmed)
    }

    pub fn with_stp_enforcement(mut self, enabled: bool) -> Self {
        self.stp_enforcement = enabled;
        self
    }

    pub fn set_stp_enforcement(&mut self, enabled: bool) {
        self.stp_enforcement = enabled;
    }

    pub fn stp_enforcement(&self) -> bool {
        self.stp_enforcement
    }

    pub fn stp_book(&self) -> &StpBook {
        &self.stp_book
    }

    pub fn order_terminal_reason(&self, key: &OrderKey) -> Option<v::TerminalReason> {
        self.orders.get(key)?.terminal_reason
    }

    pub fn order_last_modify_rejection(&self, key: &OrderKey) -> Option<v::TerminalReason> {
        self.orders.get(key)?.last_modify_reject_reason
    }

    pub(crate) fn sync_order_in_stp_book(&mut self, key: &OrderKey) -> Result<()> {
        let Some(o) = self.orders.get(key) else {
            return Ok(());
        };
        if o.spec.order_type != v::OrderType::Limit {
            return Ok(());
        }
        let instrument = o.instrument;
        let side = o.spec.side;
        let active =
            !o.aborted && !o.uncreated && !o.terminal && o.effective_remaining()?.atoms() > 0;
        if active {
            let price = o.price;
            let pending = o.pending_modify.as_ref().map(|pm| pm.requested_price);
            self.stp_book
                .upsert_order(instrument, side, key.clone(), price, pending)
        } else {
            self.stp_book.remove_order(instrument, side, key)
        }
    }

    pub fn is_intent_active(&self, key: IntentKey) -> bool {
        if let Some(legs) = self.orders_by_intent.get(&key) {
            if legs.iter().any(|leg| !self.is_order_retirable(leg)) {
                return true;
            }
            if self
                .structures
                .values()
                .any(|s| s.intent == key && !self.is_structure_retirable(s))
            {
                return true;
            }
            return false;
        }
        if let Some(adm) = self.admissions.get(&key) {
            if !adm.orders.is_empty() {
                return false;
            }
            if let Some(a) = self.amendments.get(&key) {
                if !a.aborted && !a.confirmed && a.rejected.is_none() {
                    return true;
                }
                if a.confirmed {
                    return self.orders.contains_key(&a.order)
                        && !self.is_order_retirable(&a.order);
                }
                return false;
            }
            if let Some(m) = self.modifications.get(&key) {
                if !m.aborted && !m.confirmed && m.rejected.is_none() && !m.superseded {
                    return true;
                }
                if m.confirmed {
                    return self.orders.contains_key(&m.order)
                        && !self.is_order_retirable(&m.order);
                }
                return false;
            }
            return adm.retired_mono_ns.is_none();
        }
        false
    }

    pub fn set_management_retired(&mut self, key: IntentKey, retired_mono_ns: Option<u64>) {
        if let Some(adm) = self.admissions.get_mut(&key)
            && adm.orders.is_empty()
            && !self.amendments.contains_key(&key)
            && !self.modifications.contains_key(&key)
        {
            adm.retired_mono_ns = retired_mono_ns;
        }
    }

    pub fn intent_retired_mono_ns(&self, key: IntentKey) -> Option<u64> {
        if self.is_intent_active(key) {
            return None;
        }
        if let Some(legs) = self.orders_by_intent.get(&key) {
            let mut max_retired = 0u64;
            for leg in legs {
                let order_retired = self.orders.get(leg).and_then(|o| o.retired_mono_ns)?;
                max_retired = max_retired.max(order_retired);
                for (_, entry) in self
                    .facts
                    .range(
                        FactKey {
                            order: leg.clone(),
                            kind: 0,
                            id: String::new(),
                        }..,
                    )
                    .take_while(|(fk, _)| fk.order == *leg)
                {
                    max_retired = max_retired.max(entry.recorded_mono_ns);
                }
            }
            for s in self.structures.values().filter(|s| s.intent == key) {
                max_retired = max_retired.max(s.retired_mono_ns?);
            }
            return Some(max_retired);
        }
        if let Some(adm) = self.admissions.get(&key) {
            if let Some(a) = self.amendments.get(&key) {
                if a.confirmed {
                    let order_retired = self.intent_retired_mono_ns(a.order.intent)?;
                    return Some(a.retired_mono_ns.unwrap_or(0).max(order_retired));
                }
                if a.retired_mono_ns.is_some() {
                    return a.retired_mono_ns;
                }
            }
            if let Some(m) = self.modifications.get(&key) {
                if m.confirmed {
                    let order_retired = self.intent_retired_mono_ns(m.order.intent)?;
                    return Some(m.retired_mono_ns.unwrap_or(0).max(order_retired));
                }
                if m.retired_mono_ns.is_some() {
                    return m.retired_mono_ns;
                }
            }
            return adm.retired_mono_ns;
        }
        None
    }

    pub(crate) fn is_order_retirable(&self, key: &OrderKey) -> bool {
        let Some(o) = self.orders.get(key) else {
            return false;
        };
        o.is_retirable()
            && !self
                .amendments
                .values()
                .any(|a| a.order == *key && !a.aborted && !a.confirmed && a.rejected.is_none())
            && !self.modifications.values().any(|m| {
                m.order == *key
                    && !m.aborted
                    && !m.confirmed
                    && m.rejected.is_none()
                    && !m.superseded
            })
    }

    pub(crate) fn is_structure_retirable(&self, s: &structures::Structure) -> bool {
        !s.unknown
            && (s.terminal
                || s.legs
                    .iter()
                    .all(|k| self.orders.get(k).is_some_and(|o| o.aborted || o.uncreated)))
            && s.legs.iter().all(|k| self.is_order_retirable(k))
    }

    pub(crate) fn refresh_order_retirement(&mut self, key: &OrderKey, now_mono_ns: u64) {
        let stamp = now_mono_ns.max(self.last_mono_ns);
        let retirable = self.is_order_retirable(key);
        let intent = key.intent;
        if let Some(o) = self.orders.get_mut(key) {
            if retirable {
                if o.retired_mono_ns.is_none() {
                    o.retired_mono_ns = Some(stamp.max(o.created_mono_ns));
                }
            } else {
                o.retired_mono_ns = None;
            }
        }
        let structure_ids: Vec<[u8; 16]> = self
            .structures
            .iter()
            .filter_map(|(id, s)| (s.intent == intent).then_some(*id))
            .collect();
        for sid in structure_ids {
            let s_retirable = self
                .structures
                .get(&sid)
                .is_some_and(|s| self.is_structure_retirable(s));
            if let Some(s) = self.structures.get_mut(&sid) {
                if s_retirable {
                    if s.retired_mono_ns.is_none() {
                        s.retired_mono_ns = Some(stamp);
                    }
                } else {
                    s.retired_mono_ns = None;
                }
            }
        }
        if self.admissions.contains_key(&intent) {
            let intent_retired = self.intent_retired_mono_ns(intent);
            if let Some(adm) = self.admissions.get_mut(&intent)
                && !adm.orders.is_empty()
            {
                adm.retired_mono_ns = intent_retired;
            }
        }
    }

    pub(crate) fn index_order_insert(&mut self, order: &Order) {
        push_unique(
            self.orders_by_client_id
                .entry((order.instrument, order.key.client_id.clone()))
                .or_default(),
            order.key.clone(),
        );
        push_unique(
            self.orders_by_intent.entry(order.key.intent).or_default(),
            order.key.clone(),
        );
        if let Some(native) = &order.native {
            push_unique(
                self.orders_by_native_id
                    .entry((order.instrument, native.clone()))
                    .or_default(),
                order.key.clone(),
            );
        }
        if let Some(internal_id) = order.internal_id {
            push_unique(
                self.orders_by_internal_id.entry(internal_id).or_default(),
                order.key.clone(),
            );
        }
    }

    pub(crate) fn index_order_bind_native(
        &mut self,
        instrument: u64,
        key: &OrderKey,
        native: Option<&v::VenueOrderId>,
        internal_id: Option<[u8; 16]>,
    ) {
        if let Some(native) = native {
            push_unique(
                self.orders_by_native_id
                    .entry((instrument, native.clone()))
                    .or_default(),
                key.clone(),
            );
        }
        if let Some(internal_id) = internal_id {
            push_unique(
                self.orders_by_internal_id.entry(internal_id).or_default(),
                key.clone(),
            );
        }
    }

    pub(crate) fn index_order_remove(&mut self, order: &Order) {
        let mut cids = vec![order.key.client_id.clone(), order.spec.client_id.clone()];
        cids.extend(order.client_id_aliases.iter().cloned());
        if let Some(ref pm) = order.pending_modify
            && let Some(ref cid) = pm.new_client_id
        {
            cids.push(cid.clone());
        }
        for cid in cids {
            let cid_key = (order.instrument, cid);
            if let Some(vec) = self.orders_by_client_id.get_mut(&cid_key) {
                vec.retain(|k| k != &order.key);
                if vec.is_empty() {
                    self.orders_by_client_id.remove(&cid_key);
                }
            }
        }
        if let Some(vec) = self.orders_by_intent.get_mut(&order.key.intent) {
            vec.retain(|k| k != &order.key);
            if vec.is_empty() {
                self.orders_by_intent.remove(&order.key.intent);
            }
        }
        let mut nids: Vec<v::VenueOrderId> = order.native.iter().cloned().collect();
        nids.extend(order.native_aliases.iter().cloned());
        for native in nids {
            let nid_key = (order.instrument, native);
            if let Some(vec) = self.orders_by_native_id.get_mut(&nid_key) {
                vec.retain(|k| k != &order.key);
                if vec.is_empty() {
                    self.orders_by_native_id.remove(&nid_key);
                }
            }
        }
        if let Some(internal_id) = order.internal_id
            && let Some(vec) = self.orders_by_internal_id.get_mut(&internal_id)
        {
            vec.retain(|k| k != &order.key);
            if vec.is_empty() {
                self.orders_by_internal_id.remove(&internal_id);
            }
        }
        let _ = self
            .stp_book
            .remove_order(order.instrument, order.spec.side, &order.key);
    }

    fn facts_for_order_expired(&self, key: &OrderKey, now_mono_ns: u64, window_ns: u64) -> bool {
        self.facts
            .range(
                FactKey {
                    order: key.clone(),
                    kind: 0,
                    id: String::new(),
                }..,
            )
            .take_while(|(fk, _)| fk.order == *key)
            .all(|(_, entry)| {
                pool::is_expired(Some(entry.recorded_mono_ns), now_mono_ns, Some(window_ns))
            })
    }

    pub fn evict_retired_management_admission(&mut self, intent: IntentKey) -> bool {
        let reclaimable = self.admissions.get(&intent).is_some_and(|adm| {
            adm.orders.is_empty()
                && adm.retired_mono_ns.is_some()
                && self
                    .amendments
                    .get(&intent)
                    .is_none_or(|a| !a.confirmed && (a.aborted || a.rejected.is_some()))
                && self.modifications.get(&intent).is_none_or(|m| {
                    !m.confirmed
                        && (m.aborted
                            || m.rejected.is_some()
                            || (m.superseded && !self.orders.contains_key(&m.order)))
                })
        });
        if reclaimable {
            self.amendments.remove(&intent);
            self.modifications.remove(&intent);
            self.shadow_holds.remove(&intent);
            self.structure_management.remove(&intent);
            self.admissions.remove(&intent);
            true
        } else {
            false
        }
    }

    pub(crate) fn reclaim_one_retired_management_admission(&mut self) -> bool {
        let candidate = self
            .admissions
            .iter()
            .filter(|(intent, adm)| {
                adm.orders.is_empty()
                    && adm.retired_mono_ns.is_some()
                    && self
                        .amendments
                        .get(intent)
                        .is_none_or(|a| !a.confirmed && (a.aborted || a.rejected.is_some()))
                    && self.modifications.get(intent).is_none_or(|m| {
                        !m.confirmed
                            && (m.aborted
                                || m.rejected.is_some()
                                || (m.superseded && !self.orders.contains_key(&m.order)))
                    })
            })
            .min_by_key(|(intent, adm)| {
                (
                    adm.retired_mono_ns.unwrap_or(u64::MAX),
                    adm.sequence,
                    **intent,
                )
            })
            .map(|(intent, _)| *intent);
        candidate.is_some_and(|intent| self.evict_retired_management_admission(intent))
    }

    /// Lazily evicts retired orders, structures, management admissions, shadow holds, and facts
    /// whose monotonic retirement window has elapsed. Active or unreconciled orders are never
    /// evicted regardless of age.
    pub fn evict_expired(&mut self, now_mono_ns: u64) -> usize {
        if now_mono_ns == 0 {
            return 0;
        }
        self.last_mono_ns = self.last_mono_ns.max(now_mono_ns);
        let Some(window_ns) = self.retention_window_ns else {
            return 0;
        };
        let mut evicted = 0usize;
        let evict_order_intents: Vec<IntentKey> = self
            .orders_by_intent
            .iter()
            .filter_map(|(intent, legs)| {
                let orders_ready = legs.iter().all(|leg| {
                    self.is_order_retirable(leg)
                        && self
                            .orders
                            .get(leg)
                            .and_then(|o| o.retired_mono_ns)
                            .is_some_and(|t| {
                                pool::is_expired(Some(t), now_mono_ns, Some(window_ns))
                            })
                        && self.facts_for_order_expired(leg, now_mono_ns, window_ns)
                        && self
                            .amendments
                            .values()
                            .filter(|a| a.order == *leg)
                            .all(|a| {
                                pool::is_expired(a.retired_mono_ns, now_mono_ns, Some(window_ns))
                            })
                        && self
                            .modifications
                            .values()
                            .filter(|m| m.order == *leg)
                            .all(|m| {
                                pool::is_expired(m.retired_mono_ns, now_mono_ns, Some(window_ns))
                            })
                });
                if !orders_ready {
                    return None;
                }
                let structures_ready = self
                    .structures
                    .values()
                    .filter(|s| s.intent == *intent)
                    .all(|s| {
                        self.is_structure_retirable(s)
                            && pool::is_expired(s.retired_mono_ns, now_mono_ns, Some(window_ns))
                    });
                structures_ready.then_some(*intent)
            })
            .collect();

        for intent in evict_order_intents {
            if let Some(legs) = self.orders_by_intent.remove(&intent) {
                for leg_key in legs {
                    if let Some(removed) = self.orders.remove(&leg_key) {
                        self.index_order_remove(&removed);
                        evicted += 1;
                    }
                    let fact_keys: Vec<FactKey> = self
                        .facts
                        .range(
                            FactKey {
                                order: leg_key.clone(),
                                kind: 0,
                                id: String::new(),
                            }..,
                        )
                        .take_while(|(fk, _)| fk.order == leg_key)
                        .map(|(fk, _)| fk.clone())
                        .collect();
                    for fk in fact_keys {
                        self.facts.remove(&fk);
                    }
                    let amend_keys: Vec<IntentKey> = self
                        .amendments
                        .iter()
                        .filter_map(|(ak, a)| (a.order == leg_key).then_some(*ak))
                        .collect();
                    for ak in amend_keys {
                        self.amendments.remove(&ak);
                        self.admissions.remove(&ak);
                    }
                    let mod_keys: Vec<IntentKey> = self
                        .modifications
                        .iter()
                        .filter_map(|(mk, m)| (m.order == leg_key).then_some(*mk))
                        .collect();
                    for mk in mod_keys {
                        self.modifications.remove(&mk);
                        if let Some(holds) = self.shadow_holds.remove(&mk) {
                            for hold in holds {
                                let _ = self.add_uncovered(hold.wallet_id, hold.amount);
                            }
                        }
                        self.admissions.remove(&mk);
                    }
                }
            }
            let struct_ids: Vec<[u8; 16]> = self
                .structures
                .iter()
                .filter_map(|(sid, s)| (s.intent == intent).then_some(*sid))
                .collect();
            for sid in struct_ids {
                self.structures.remove(&sid);
                let mgmt_keys: Vec<IntentKey> = self
                    .structure_management
                    .iter()
                    .filter_map(|(mk, m)| (m.structure == sid).then_some(*mk))
                    .collect();
                for mk in mgmt_keys {
                    self.structure_management.remove(&mk);
                    self.admissions.remove(&mk);
                }
            }
            if let Some(holds) = self.shadow_holds.remove(&intent) {
                for hold in holds {
                    let _ = self.add_uncovered(hold.wallet_id, hold.amount);
                }
            }
            self.admissions.remove(&intent);
        }

        let evict_mgmt_intents: Vec<IntentKey> = self
            .admissions
            .iter()
            .filter_map(|(intent, adm)| {
                if !adm.orders.is_empty() {
                    return None;
                }
                if let Some(a) = self.amendments.get(intent)
                    && (a.confirmed || (!a.aborted && a.rejected.is_none()))
                {
                    return None;
                }
                if let Some(m) = self.modifications.get(intent)
                    && (m.confirmed || (!m.aborted && m.rejected.is_none() && !m.superseded))
                {
                    return None;
                }
                pool::is_expired(adm.retired_mono_ns, now_mono_ns, Some(window_ns))
                    .then_some(*intent)
            })
            .collect();
        for intent in evict_mgmt_intents {
            self.amendments.remove(&intent);
            self.modifications.remove(&intent);
            self.shadow_holds.remove(&intent);
            self.structure_management.remove(&intent);
            self.admissions.remove(&intent);
            evicted += 1;
        }
        evicted
    }

    pub(crate) fn reopen_order(&mut self, key: &OrderKey) -> Result<bool> {
        let reopened = self.orders.get_mut(key).unwrap().reopen_aborted()?;
        if reopened {
            if let Some(adm) = self.admissions.get(&key.intent)
                && adm.sequence > self.last_reactivation_sequence
                && !adm.capital.is_empty()
            {
                self.shadow_holds.insert(key.intent, adm.capital.clone());
            }
            self.sync_order_in_stp_book(key)?;
        }
        Ok(reopened)
    }

    pub fn mark_order_in_doubt(&mut self, key: &OrderKey, reason: DoubtReason) -> Result<()> {
        let o = self.orders.get_mut(key).ok_or(Error::UnknownOrder)?;
        if o.aborted || o.uncreated {
            return Err(Error::Unreconciled);
        }
        o.in_doubt = Some(reason);
        o.retired_mono_ns = None;
        if let Some(adm) = self.admissions.get_mut(&key.intent) {
            adm.retired_mono_ns = None;
        }
        let stamp = self.last_mono_ns;
        self.refresh_order_retirement(key, stamp);
        Ok(())
    }

    pub fn clear_order_in_doubt(&mut self, key: &OrderKey) -> Result<()> {
        let o = self.orders.get_mut(key).ok_or(Error::UnknownOrder)?;
        o.in_doubt = None;
        let stamp = self.last_mono_ns;
        self.refresh_order_retirement(key, stamp);
        Ok(())
    }

    pub fn order_in_doubt(&self, key: &OrderKey) -> Option<DoubtReason> {
        self.orders.get(key)?.in_doubt
    }

    pub fn in_doubt_orders(&self) -> impl Iterator<Item = (&OrderKey, DoubtReason)> {
        self.orders
            .iter()
            .filter_map(|(key, order)| order.in_doubt.map(|reason| (key, reason)))
    }
}
fn push_unique<T: PartialEq>(vec: &mut Vec<T>, item: T) {
    if !vec.contains(&item) {
        vec.push(item);
    }
}
fn asset_scale(a: &v::AssetRef) -> Result<u8> {
    if a.exponent > 0 || a.exponent < -38 {
        return Err(Error::Catalog);
    }
    Ok((-a.exponent) as u8)
}
fn decimal(d: v::Decimal, scale: u8, rounding: Rounding) -> Result<Fixed> {
    let source = if d.exponent() <= 0 {
        Fixed::from_atoms(d.coefficient(), (-d.exponent()) as u8)?
    } else {
        Fixed::from_atoms(d.coefficient(), 0)?.checked_mul_int(
            10i128
                .checked_pow(d.exponent() as u32)
                .ok_or(RiskError::ArithmeticOverflow)?,
        )?
    };
    Ok(source.rescale(scale, rounding)?)
}
fn exact_decimal(d: v::Decimal, scale: u8) -> Result<Fixed> {
    let down = decimal(d, scale, Rounding::Floor)?;
    if down != decimal(d, scale, Rounding::Ceil)? {
        return Err(Error::Risk(RiskError::ScaleMismatch));
    }
    Ok(down)
}
fn decimal_lossless(d: v::Decimal) -> Result<Fixed> {
    if d.exponent() <= 0 {
        let scale = u8::try_from(-i16::from(d.exponent())).map_err(|_| Error::Catalog)?;
        Ok(Fixed::from_atoms(d.coefficient(), scale)?)
    } else {
        let mul = 10i128
            .checked_pow(d.exponent() as u32)
            .ok_or(RiskError::ArithmeticOverflow)?;
        Ok(Fixed::from_atoms(d.coefficient(), 0)?.checked_mul_int(mul)?)
    }
}
fn side_code(side: v::PositionSide) -> u8 {
    match side {
        v::PositionSide::Both => 0,
        v::PositionSide::Long => 1,
        v::PositionSide::Short => 2,
    }
}
fn ownership(total: Fixed, rows: &[(u64, Fixed)]) -> Result<BTreeMap<u64, Fixed>> {
    if rows.len() > MAX_STRATEGIES {
        return Err(Error::Capacity);
    }
    let mut out = BTreeMap::new();
    let mut sum = total.zero_like();
    for &(id, n) in rows {
        sum = sum.checked_add(n)?;
        if out.insert(id, n).is_some() {
            return Err(Error::InvalidSnapshot);
        }
    }
    let rest = total.checked_sub(sum)?;
    let old = out.get(&0).copied().unwrap_or(total.zero_like());
    out.insert(0, old.checked_add(rest)?);
    Ok(out)
}
fn add(map: &mut BTreeMap<AssetKey, Fixed>, key: AssetKey, n: Fixed) -> Result<()> {
    let old = map.get(&key).copied().unwrap_or(n.zero_like());
    map.insert(key, old.checked_add(n)?);
    Ok(())
}
fn add_owner(map: &mut BTreeMap<u64, Fixed>, owner: u64, n: Fixed) -> Result<()> {
    let old = map.get(&owner).copied().unwrap_or(n.zero_like());
    map.insert(owner, old.checked_add(n)?);
    Ok(())
}
