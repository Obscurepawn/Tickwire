//! Single-owner multi-asset reservations and economic attribution.
#![forbid(unsafe_code)]
mod admission;
mod calculation;
mod events;
mod external;
mod recovery;
mod types;
use std::collections::{BTreeMap, BTreeSet};
pub use tickwire_risk::Fixed;
use tickwire_risk::{RiskError, Rounding};
use tickwire_venue as v;
pub use types::*;

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
#[derive(Debug, Clone, PartialEq, Eq)]
struct Order {
    key: OrderKey,
    digest: v::Hash,
    instrument: u64,
    strategy: u64,
    spec: v::OrderSpec,
    original: Fixed,
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
    fees_closed: bool,
    aborted: bool,
    economic: bool,
    confirmed: bool,
    reduce: Fixed,
    native: Option<v::VenueOrderId>,
    internal_id: Option<[u8; 16]>,
    owner_revision: u64,
    shadow_fee_credit: Fixed,
}
impl Order {
    fn unknown(&self) -> bool {
        self.observed_unknown || self.recovery_unknown
    }
    fn remaining(&self) -> Result<Fixed> {
        if self.aborted || self.terminal && self.terminal_cumulative == Some(self.filled) {
            Ok(self.original.zero_like())
        } else {
            Ok(self.original.checked_sub(self.filled)?.positive_part())
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FactKey {
    order: OrderKey,
    kind: u8,
    id: String,
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
    catalog: BTreeMap<u64, InstrumentPolicy>,
    positions: BTreeMap<(u64, u8), Position>,
    orders: BTreeMap<OrderKey, Order>,
    admissions: BTreeMap<IntentKey, Admission>,
    facts: BTreeMap<FactKey, EconomicFact>,
    external_facts: BTreeMap<(u8, String), external::ExternalValue>,
    venue_available: BTreeMap<u64, Fixed>,
    shadow_holds: BTreeMap<IntentKey, Vec<CapitalHold>>,
    uncovered_debits: BTreeMap<u64, Fixed>,
    shadow_unvalued: BTreeSet<u64>,
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
            catalog: BTreeMap::new(),
            positions: BTreeMap::new(),
            orders: BTreeMap::new(),
            admissions: BTreeMap::new(),
            facts: BTreeMap::new(),
            external_facts: BTreeMap::new(),
            venue_available: BTreeMap::new(),
            shadow_holds: BTreeMap::new(),
            uncovered_debits: BTreeMap::new(),
            shadow_unvalued: BTreeSet::new(),
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
        for p in catalog.entries {
            result.validate_policy(&p)?;
            if result
                .catalog
                .insert(p.instrument.instrument_id, p)
                .is_some()
            {
                return Err(Error::Catalog);
            }
        }
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
    /// Closes risk-increasing admission without changing balances, reservations or sequence.
    pub fn freeze(&mut self) {
        self.frozen = true;
    }
    pub fn order_keys(&self) -> impl Iterator<Item=&OrderKey> { self.orders.keys() }
    pub fn account_epoch(&self) -> u64 { self.epoch }
    pub fn ledger_sequence(&self) -> u64 {
        self.sequence
    }
    pub fn rejected_event(&self) -> Option<&v::CanonicalEvent> {
        self.rejected_event.as_ref()
    }
    pub fn order(&self, key: &OrderKey) -> Option<OrderView> {
        self.orders.get(key).and_then(|o| {
            Some(OrderView {
                key: key.clone(),
                command_digest: o.digest,
                strategy: o.strategy,
                original_quantity: o.original,
                filled_quantity: o.filled,
                remaining_quantity: o.remaining().ok()?,
                reduce_allocation: o.reduce,
                unknown: o.unknown(),
                terminal: o.terminal,
                fee_reconciled: o.fees_closed,
                aborted: o.aborted,
                confirmed: o.confirmed,
            })
        })
    }
    pub fn position_entry_bound(&self,instrument:u64,side:v::PositionSide)->Result<Option<Fixed>> {
        self.catalog.get(&instrument).ok_or(Error::Catalog)?;
        Ok(self.positions.get(&(instrument,side_code(side))).map(|p|p.entry))
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
    /// Changes ownership only. The caller journals this ordered fact before applying it.
    pub fn transfer_asset_attribution(
        &mut self,
        key: AssetKey,
        from: u64,
        to: u64,
        amount: Fixed,
    ) -> Result<()> {
        let mut candidate = self.clone();
        let a = candidate.assets.get_mut(&key).ok_or(Error::Identity)?;
        amount.nonnegative()?;
        transfer(&mut a.ownership, from, to, amount)?;
        candidate.next_sequence()?;
        if candidate.check_limits().is_err() {
            candidate.frozen = true;
        }
        *self = candidate;
        Ok(())
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
fn transfer(map: &mut BTreeMap<u64, Fixed>, from: u64, to: u64, n: Fixed) -> Result<()> {
    if from == to {
        return Err(Error::Identity);
    }
    add_owner(map, from, n.checked_neg()?)?;
    add_owner(map, to, n)
}
