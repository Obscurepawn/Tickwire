use super::*;

type IncomeKey = (String, String);
type ComponentKey = (String, String, u8, u64);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IncomeStatus {
    Baseline,
    Booked,
    Pending,
    Trade,
}
#[derive(Debug, Clone)]
struct IncomeEntry {
    input: v::CanonicalAccountInput,
    status: IncomeStatus,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct Component {
    order: OrderKey,
    native: v::VenueOrderId,
    wallet: u64,
    delta: v::AssetDelta,
}
#[derive(Debug, Clone, Default)]
pub(super) struct AccountInputs {
    baseline: BTreeMap<u64, IncomeBaseline>,
    incomes: BTreeMap<IncomeKey, IncomeEntry>,
    components: BTreeMap<ComponentKey, Component>,
    aliases: BTreeMap<ComponentKey, IncomeKey>,
    observations: BTreeMap<v::Hash, bool>,
    caps: BTreeMap<AssetKey, Fixed>,
    credit_holds: BTreeMap<AssetKey, Fixed>,
    rejected: Option<v::CanonicalAccountInput>,
}
impl AccountInputs {
    fn size(&self) -> usize {
        self.incomes.len() + self.components.len() + self.observations.len()
    }
}
fn income_key(row: &v::IncomeRecord) -> IncomeKey {
    (
        row.income_type.as_str().to_owned(),
        row.transaction_id.as_str().to_owned(),
    )
}
fn component_key(row: &v::IncomeRecord) -> Option<ComponentKey> {
    let kind = match row.income_type.as_str() {
        "REALIZED_PNL" => 0,
        "COMMISSION" => 1,
        _ => return None,
    };
    Some((
        row.symbol.as_ref()?.as_str().to_owned(),
        row.trade_id.as_ref()?.as_str().to_owned(),
        kind,
        row.delta.asset.id,
    ))
}
impl Account {
    pub fn income_baseline(&self, wallet: u64) -> Option<&IncomeBaseline> {
        self.account_inputs.baseline.get(&wallet)
    }
    pub fn income_baselines(&self) -> impl Iterator<Item = &IncomeBaseline> {
        self.account_inputs.baseline.values()
    }
    /// Applies already-verified baseline coverage, never authorization by itself.
    /// The caller must persist and verify its complete query/snapshot dependencies.
    pub fn apply_income_baseline(&mut self, baseline: &IncomeBaseline) -> Result<Applied> {
        if let Some(old) = self.account_inputs.baseline.get(&baseline.wallet_id) {
            return if old == baseline {
                Ok(Applied::Duplicate)
            } else {
                self.freeze_error(Error::Conflict)
            };
        }
        let result = (|| {
            if baseline.scope != self.scope
                || baseline.account_epoch != self.epoch
                || baseline.ledger_sequence != self.sequence
                || baseline.coverage_digest == [0; 32]
                || baseline.baseline_digest == [0; 32]
                || baseline.input_stream == [0; 16]
                || baseline.through_sequence == 0
                || baseline.through_time_ms == 0
                || baseline.covered.len() > MAX_BASELINE_INCOMES
                || !self.orders.is_empty()
                || !self.admissions.is_empty()
                || !self.facts.is_empty()
                || !self.external_facts.is_empty()
                || !self.account_inputs.incomes.is_empty()
                || !self.account_inputs.observations.is_empty()
                || self.account_inputs.rejected.is_some()
                || !self
                    .catalog
                    .values()
                    .any(|p| p.wallet_id == baseline.wallet_id && p.futures.is_some())
            {
                return Err(Error::Identity);
            }
            let mut candidate = self.clone();
            for input in &baseline.covered {
                candidate.check_account_identity(&input.input().identity)?;
                if input.input().identity.wallet_id != baseline.wallet_id {
                    return Err(Error::Identity);
                }
                let v::AccountInputBody::Income(row) = &input.input().body else {
                    return Err(Error::Identity);
                };
                candidate.account_delta(baseline.wallet_id, &row.delta)?;
                if row.venue_time_ms > baseline.through_time_ms {
                    return Err(Error::Identity);
                }
                if candidate
                    .account_inputs
                    .incomes
                    .insert(
                        income_key(row),
                        IncomeEntry {
                            input: input.clone(),
                            status: IncomeStatus::Baseline,
                        },
                    )
                    .is_some()
                {
                    return Err(Error::Conflict);
                }
            }
            candidate
                .account_inputs
                .baseline
                .insert(baseline.wallet_id, baseline.clone());
            candidate.next_sequence()?;
            *self = candidate;
            Ok(Applied::New)
        })();
        if result.is_err() {
            self.frozen = true;
        }
        result
    }
    pub fn apply_account_input(&mut self, input: &v::CanonicalAccountInput) -> Result<Applied> {
        let mut candidate = self.clone();
        match candidate.apply_account_candidate(input) {
            Ok(out) => {
                *self = candidate;
                Ok(out)
            }
            Err(e) => {
                self.frozen = true;
                if self.account_inputs.rejected.is_none() {
                    self.account_inputs.rejected = Some(input.clone());
                }
                Err(e)
            }
        }
    }
    fn check_account_identity(&self, id: &v::AccountIdentity) -> Result<()> {
        if id.risk_domain_id != self.scope.risk_domain_id
            || id.venue != self.scope.venue
            || id.environment != self.scope.environment
            || id.account != self.scope.account
            || !self
                .catalog
                .values()
                .any(|p| p.wallet_id == id.wallet_id && p.futures.is_some())
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
    fn account_delta(&self, wallet: u64, delta: &v::AssetDelta) -> Result<AssetAmount> {
        let key = AssetKey {
            wallet_id: wallet,
            asset_id: delta.asset.id,
        };
        if self
            .assets
            .get(&key)
            .is_none_or(|a| a.baseline.asset != delta.asset)
        {
            return Err(Error::Identity);
        }
        Ok(AssetAmount {
            key,
            amount: exact_decimal(delta.amount, asset_scale(&delta.asset)?)?,
        })
    }
    fn apply_account_candidate(&mut self, input: &v::CanonicalAccountInput) -> Result<Applied> {
        let id = &input.input().identity;
        self.check_account_identity(id)?;
        match &input.input().body {
            v::AccountInputBody::Income(row) => {
                let key = income_key(row);
                if let Some(old) = self.account_inputs.incomes.get(&key) {
                    if old.input != *input {
                        return Err(Error::Conflict);
                    }
                    return Ok(Applied::Duplicate);
                }
                if self.account_inputs.size() >= MAX_FACTS {
                    return Err(Error::Capacity);
                }
                let delta = self.account_delta(id.wallet_id, &row.delta)?;
                let after_baseline = self
                    .account_inputs
                    .baseline
                    .get(&id.wallet_id)
                    .is_some_and(|b| row.venue_time_ms > b.through_time_ms);
                let status = if after_baseline && row.income_type.as_str() == "FUNDING_FEE" {
                    // Real income is booked exactly once. Attribution and funding-period
                    // coverage remain unresolved; future reserves are not released here.
                    let a = self.assets.get_mut(&delta.key).unwrap();
                    a.cashflow = a.cashflow.checked_add(delta.amount)?;
                    add_owner(&mut a.ownership, 0, delta.amount)?;
                    if delta.amount.atoms() < 0 {
                        a.other_debits = a.other_debits.checked_sub(delta.amount)?;
                        if self.catalog.values().any(|p| {
                            p.wallet_id == id.wallet_id
                                && p.instrument.settlement.id == delta.key.asset_id
                        }) {
                            self.add_uncovered(id.wallet_id, delta.amount.checked_neg()?)?;
                        } else {
                            self.shadow_unvalued.insert(id.wallet_id);
                        }
                    } else {
                        add(
                            &mut self.account_inputs.credit_holds,
                            delta.key,
                            delta.amount,
                        )?;
                    }
                    IncomeStatus::Booked
                } else {
                    IncomeStatus::Pending
                };
                self.account_inputs.incomes.insert(
                    key.clone(),
                    IncomeEntry {
                        input: input.clone(),
                        status,
                    },
                );
                if after_baseline && let Some(component) = component_key(row) {
                    self.match_income_component(&key, &component)?;
                }
                if self.account_inputs.incomes[&key].status != IncomeStatus::Trade {
                    self.frozen = true;
                }
            }
            v::AccountInputBody::Observation(o) => {
                if self
                    .account_inputs
                    .observations
                    .contains_key(&input.digest())
                {
                    return Ok(Applied::Duplicate);
                }
                if self.account_inputs.size() >= MAX_FACTS {
                    return Err(Error::Capacity);
                }
                let mut unresolved = o.reason.as_str() != "ORDER";
                for balance in o.balances.as_slice() {
                    let delta = self.account_delta(
                        id.wallet_id,
                        &v::AssetDelta {
                            asset: balance.asset.clone(),
                            amount: balance.wallet_balance,
                        },
                    )?;
                    let a = &self.assets[&delta.key];
                    let total = a
                        .baseline
                        .free
                        .checked_add(a.baseline.locked)?
                        .checked_add(a.cashflow)?;
                    if delta.amount != total {
                        unresolved = true;
                    }
                    if delta.amount.atoms() < total.atoms() {
                        let old = self
                            .account_inputs
                            .caps
                            .get(&delta.key)
                            .copied()
                            .unwrap_or(total);
                        self.account_inputs
                            .caps
                            .insert(delta.key, old.min(delta.amount)?);
                    }
                }
                for observed in o.positions.as_slice() {
                    let p = self
                        .catalog
                        .get(&observed.instrument_id)
                        .ok_or(Error::Catalog)?;
                    if p.wallet_id != id.wallet_id
                        || p.instrument.instrument_version != observed.instrument_version
                        || p.instrument.symbol != observed.symbol
                        || p.futures.is_none()
                    {
                        return Err(Error::Identity);
                    }
                    let fp = p.futures.as_ref().unwrap();
                    if (observed.side == v::PositionSide::Both)
                        != (fp.position_mode == v::PositionMode::OneWay)
                        || fp.margin_mode != observed.margin_mode
                    {
                        return Err(Error::Identity);
                    }
                    let mut quantity =
                        exact_decimal(observed.quantity, asset_scale(&p.instrument.base)?)?;
                    // USD-M reports SHORT positionAmt as signed negative quantity.
                    if observed.side == v::PositionSide::Short {
                        quantity = quantity.checked_neg()?;
                    }
                    if observed.side != v::PositionSide::Both {
                        quantity.nonnegative()?;
                    }
                    if self.position_quantity(p, observed.side)? != quantity {
                        unresolved = true;
                    }
                }
                self.account_inputs
                    .observations
                    .insert(input.digest(), unresolved);
                self.frozen |= unresolved;
            }
            v::AccountInputBody::ExternalTrade(_) => return Err(Error::Unsupported),
        }
        self.next_sequence()?;
        if self.evaluate().is_err() {
            self.frozen = true;
        }
        Ok(Applied::New)
    }
    fn match_income_component(
        &mut self,
        key: &IncomeKey,
        component: &ComponentKey,
    ) -> Result<bool> {
        let Some(trade) = self.account_inputs.components.get(component) else {
            return Ok(false);
        };
        let entry = &self.account_inputs.incomes[key];
        let v::AccountInputBody::Income(row) = &entry.input.input().body else {
            unreachable!()
        };
        if trade.wallet != entry.input.input().identity.wallet_id
            || trade.delta != row.delta
            || self
                .account_inputs
                .aliases
                .get(component)
                .is_some_and(|old| old != key)
        {
            return Err(Error::Conflict);
        }
        if entry.status == IncomeStatus::Baseline {
            return Err(Error::Conflict);
        }
        if entry.status == IncomeStatus::Trade {
            return Ok(false);
        }
        self.account_inputs
            .aliases
            .insert(component.clone(), key.clone());
        self.account_inputs.incomes.get_mut(key).unwrap().status = IncomeStatus::Trade;
        Ok(true)
    }
    /// Called only within the same candidate transaction as the original fill.
    pub(super) fn register_fill_components(
        &mut self,
        key: &OrderKey,
        p: &InstrumentPolicy,
        fill: &v::FillEvent,
    ) -> Result<bool> {
        if p.futures.is_none() {
            return Ok(false);
        }
        let Some(native) = self.orders[key].native.clone() else {
            return Ok(false);
        };
        let mut changed = false;
        let components = fill
            .realized_pnl
            .iter()
            .map(|d| (0, d))
            .chain(fill.fees.as_slice().iter().map(|d| (1, d)));
        for (kind, delta) in components {
            let component = (
                p.instrument.symbol.as_str().to_owned(),
                fill.trade_id.as_str().to_owned(),
                kind,
                delta.asset.id,
            );
            let value = Component {
                order: key.clone(),
                native: native.clone(),
                wallet: p.wallet_id,
                delta: delta.clone(),
            };
            if let Some(old) = self.account_inputs.components.get(&component) {
                if old != &value {
                    return Err(Error::Conflict);
                }
            } else {
                if self.account_inputs.size() >= MAX_FACTS {
                    return Err(Error::Capacity);
                }
                self.account_inputs
                    .components
                    .insert(component.clone(), value);
                changed = true;
            }
            let pending: Vec<_> = self
                .account_inputs
                .incomes
                .iter()
                .filter_map(|(k, e)| {
                    let v::AccountInputBody::Income(row) = &e.input.input().body else {
                        return None;
                    };
                    let after = self
                        .account_inputs
                        .baseline
                        .get(&e.input.input().identity.wallet_id)
                        .is_some_and(|b| row.venue_time_ms > b.through_time_ms);
                    (after && component_key(row).as_ref() == Some(&component)).then_some(k.clone())
                })
                .collect();
            for k in pending {
                changed |= self.match_income_component(&k, &component)?;
            }
        }
        Ok(changed)
    }
    pub fn pending_account_debit(&self, key: AssetKey) -> Result<Fixed> {
        let mut total = self
            .assets
            .get(&key)
            .ok_or(Error::Identity)?
            .baseline
            .free
            .zero_like();
        for entry in self.account_inputs.incomes.values() {
            if entry.status != IncomeStatus::Pending {
                continue;
            }
            let v::AccountInputBody::Income(row) = &entry.input.input().body else {
                unreachable!()
            };
            if entry.input.input().identity.wallet_id == key.wallet_id
                && row.delta.asset.id == key.asset_id
            {
                let amount = exact_decimal(row.delta.amount, total.scale())?;
                if amount.atoms() < 0 {
                    total = total.checked_sub(amount)?;
                }
            }
        }
        Ok(total)
    }
    pub(super) fn account_effective_total(&self, key: AssetKey) -> Result<Fixed> {
        let a = self.assets.get(&key).ok_or(Error::Identity)?;
        let total = a
            .baseline
            .free
            .checked_add(a.baseline.locked)?
            .checked_add(a.cashflow)?;
        let held = self
            .account_inputs
            .credit_holds
            .get(&key)
            .copied()
            .unwrap_or(total.zero_like());
        let spendable = total.checked_sub(held)?;
        Ok(self
            .account_inputs
            .caps
            .get(&key)
            .copied()
            .unwrap_or(spendable)
            .min(spendable)?)
    }
    pub fn unresolved_account_inputs(&self) -> usize {
        self.account_inputs
            .incomes
            .values()
            .filter(|e| matches!(e.status, IncomeStatus::Pending | IncomeStatus::Booked))
            .count()
            + self
                .account_inputs
                .observations
                .values()
                .filter(|&&x| x)
                .count()
            + usize::from(self.account_inputs.rejected.is_some())
    }
}
