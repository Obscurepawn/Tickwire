use super::closure::StateWriter;
use super::*;

fn key(id: &IncomeIdentity) -> IncomeKey {
    (
        id.income_type.as_str().into(),
        id.transaction_id.as_str().into(),
    )
}
impl Account {
    pub fn income_associations(&self) -> impl Iterator<Item = &IncomeObservationsAssociated> {
        self.account_inputs.associations.values()
    }
    pub fn income_association(&self, request: [u8; 16]) -> Option<&IncomeObservationsAssociated> {
        self.account_inputs.associations.get(&request)
    }
    pub fn unexplained_account_observations(
        &self,
    ) -> impl Iterator<Item = &v::CanonicalAccountInput> {
        self.account_inputs
            .observation_inputs
            .iter()
            .filter_map(|(hash, input)| {
                self.account_inputs
                    .observations
                    .get(hash)
                    .copied()
                    .unwrap_or(false)
                    .then_some(input)
            })
    }
    pub fn income_association_state_digest(
        &self,
        baseline: v::Hash,
        history: v::Hash,
        wallet: u64,
    ) -> Result<v::Hash> {
        let s = &self.account_inputs;
        if !s.baseline.contains_key(&wallet) {
            return Err(Error::Identity);
        }
        if s.associations.len() > MAX_ASSOCIATION_REQUESTS
            || s.associated_observations.len() > MAX_FACTS
            || s.associated_incomes.len() > MAX_FACTS
            || s.observation_inputs.len() != s.observations.len()
        {
            return Err(Error::Capacity);
        }
        let mut w = StateWriter::new();
        w.put(b"tickwire.account-income-association-state.v1");
        w.put(&1u16.to_le_bytes());
        w.put(&self.account_inputs_state_digest(baseline, history)?);
        scope(&mut w, &self.scope);
        w.uint(wallet);
        w.uint(self.sequence);
        w.put(&history);
        w.count(s.associations.len());
        for (request, b) in &s.associations {
            w.put(request);
            w.put(&b.approval_digest);
            w.put(&association_binding_digest(b)?);
        }
        w.count(s.associated_observations.len());
        for (observation, request) in &s.associated_observations {
            w.put(observation);
            w.put(request);
        }
        w.count(s.associated_incomes.len());
        for (income, observation) in &s.associated_incomes {
            w.income_key(income);
            w.put(observation);
        }
        w.finish()
    }

    /// Applies data whose authority, exact WAL interval, snapshots and input
    /// dependencies have already been verified by the durable owner. No funding,
    /// order liability or risk reserve is released here.
    pub fn associate_income_observations(
        &mut self,
        binding: &IncomeObservationsAssociated,
    ) -> Result<Applied> {
        if let Some(old) = self.account_inputs.associations.get(&binding.request_id) {
            return if old == binding {
                Ok(Applied::Duplicate)
            } else {
                self.freeze_error(Error::Conflict)
            };
        }
        let result = (|| {
            self.check_association(binding)?;
            let mut candidate = self.clone();
            for group in &binding.associations {
                candidate
                    .account_inputs
                    .observations
                    .insert(group.observation_digest, false);
                candidate
                    .account_inputs
                    .associated_observations
                    .insert(group.observation_digest, binding.request_id);
                for income in &group.incomes {
                    candidate
                        .account_inputs
                        .associated_incomes
                        .insert(key(income), group.observation_digest);
                }
            }
            candidate
                .account_inputs
                .associations
                .insert(binding.request_id, binding.clone());
            candidate.next_sequence()?;
            candidate.frozen = true;
            candidate.evaluate()?;
            *self = candidate;
            Ok(Applied::New)
        })();
        if result.is_err() {
            self.frozen = true;
        }
        result
    }
    fn check_association(&self, b: &IncomeObservationsAssociated) -> Result<()> {
        let s = &self.account_inputs;
        if b.scope != self.scope
            || b.account_epoch != self.epoch
            || b.ledger_sequence != self.sequence
            || b.request_id == [0; 16]
            || b.input_stream == [0; 16]
            || b.through_sequence == 0
            || b.manifest_digest == [0; 32]
            || b.approval_digest == [0; 32]
            || b.state_digest
                != self.income_association_state_digest(
                    b.baseline_digest,
                    b.history_digest,
                    b.wallet_id,
                )?
        {
            return Err(Error::Identity);
        }
        if b.associations.is_empty()
            || b.associations.len() > MAX_ASSOCIATION_OBSERVATIONS
            || s.associations.len() >= MAX_ASSOCIATION_REQUESTS
        {
            return Err(Error::Capacity);
        }
        if s.rejected.is_some()
            || self.rejected_event.is_some()
            || s.incomes
                .values()
                .any(|entry| entry.status == IncomeStatus::Pending)
        {
            return Err(Error::Unreconciled);
        }
        if self
            .catalog
            .values()
            .filter(|p| p.wallet_id == b.wallet_id)
            .any(|p| {
                p.futures
                    .as_ref()
                    .is_none_or(|f| f.margin_mode != v::MarginMode::Cross)
            })
        {
            return Err(Error::Unsupported);
        }
        let baseline = s.baseline.get(&b.wallet_id).ok_or(Error::Identity)?;
        let mut observations = BTreeSet::new();
        let mut incomes = BTreeSet::new();
        for group in &b.associations {
            if !observations.insert(group.observation_digest)
                || s.associated_observations
                    .contains_key(&group.observation_digest)
            {
                return Err(Error::Conflict);
            }
            if s.observations.get(&group.observation_digest) != Some(&true) {
                return Err(Error::Identity);
            }
            let input = s
                .observation_inputs
                .get(&group.observation_digest)
                .ok_or(Error::Identity)?;
            if input.input().identity.wallet_id != b.wallet_id {
                return Err(Error::Identity);
            }
            let v::AccountInputBody::Observation(o) = &input.input().body else {
                return Err(Error::Identity);
            };
            if o.reason.as_str() != "FUNDING_FEE"
                || !o.positions.as_slice().is_empty()
                || o.balances.as_slice().is_empty()
            {
                return Err(Error::Unsupported);
            }
            if group.incomes.is_empty() || group.incomes.windows(2).any(|w| w[0] >= w[1]) {
                return Err(Error::Identity);
            }
            let mut amounts = BTreeMap::new();
            for id in &group.incomes {
                let k = key(id);
                if !incomes.insert(k.clone()) || s.associated_incomes.contains_key(&k) {
                    return Err(Error::Conflict);
                }
                if incomes.len() > MAX_ASSOCIATION_INCOMES {
                    return Err(Error::Capacity);
                }
                let e = s.incomes.get(&k).ok_or(Error::Identity)?;
                let v::AccountInputBody::Income(row) = &e.input.input().body else {
                    return Err(Error::Identity);
                };
                if !matches!(e.status, IncomeStatus::Booked | IncomeStatus::Reconciled)
                    || id.income_type.as_str() != "FUNDING_FEE"
                    || row.venue_time_ms <= baseline.through_time_ms
                    || e.input.input().identity != input.input().identity
                {
                    return Err(Error::Unreconciled);
                }
                let symbol = row.symbol.as_ref().ok_or(Error::Unsupported)?;
                if !self.catalog.values().any(|p| {
                    p.wallet_id == b.wallet_id
                        && p.product == v::Product::LinearPerpetual
                        && &p.instrument.symbol == symbol
                }) || o
                    .funding_symbol
                    .as_ref()
                    .is_some_and(|expected| expected != symbol)
                {
                    return Err(Error::Identity);
                }
                let delta = self.account_delta(b.wallet_id, &row.delta)?;
                add(&mut amounts, delta.key, delta.amount)?;
            }
            let mut reported = BTreeSet::new();
            for balance in o.balances.as_slice() {
                let delta = self.account_delta(
                    b.wallet_id,
                    &v::AssetDelta {
                        asset: balance.asset.clone(),
                        amount: balance.balance_change,
                    },
                )?;
                if !reported.insert(delta.key) {
                    return Err(Error::Identity);
                }
                if amounts
                    .remove(&delta.key)
                    .unwrap_or(delta.amount.zero_like())
                    != delta.amount
                    || balance.wallet_balance != balance.cross_wallet_balance
                {
                    return Err(Error::Unreconciled);
                }
            }
            if !amounts.is_empty() {
                return Err(Error::Unreconciled);
            }
        }
        if observations.len()
            != s.observations
                .values()
                .filter(|&&unresolved| unresolved)
                .count()
        {
            return Err(Error::Unreconciled);
        }
        if s.associated_observations
            .len()
            .checked_add(observations.len())
            .is_none_or(|n| n > MAX_FACTS)
            || s.associated_incomes
                .len()
                .checked_add(incomes.len())
                .is_none_or(|n| n > MAX_FACTS)
        {
            return Err(Error::Capacity);
        }
        Ok(())
    }
}
fn scope(w: &mut StateWriter, scope: &Scope) {
    w.put(&scope.risk_domain_id);
    w.put(&scope.venue.to_le_bytes());
    w.byte(match scope.environment {
        v::Environment::Production => 1,
        v::Environment::Test => 2,
    });
    w.uint(scope.account);
}
/// Stable complete record-data commitment used by the permanent request index.
pub(super) fn association_binding_digest(b: &IncomeObservationsAssociated) -> Result<v::Hash> {
    let mut w = StateWriter::new();
    w.put(b"tickwire.account-income-association-result.v1");
    w.put(&1u16.to_le_bytes());
    scope(&mut w, &b.scope);
    for n in [b.account_epoch, b.ledger_sequence, b.wallet_id] {
        w.uint(n);
    }
    w.put(&b.request_id);
    w.put(&b.baseline_digest);
    w.put(&b.history_digest);
    w.put(&b.input_stream);
    w.uint(b.through_sequence);
    for hash in [b.state_digest, b.manifest_digest, b.approval_digest] {
        w.put(&hash);
    }
    w.count(b.associations.len());
    for group in &b.associations {
        w.put(&group.observation_digest);
        w.count(group.incomes.len());
        for id in &group.incomes {
            w.income_key(&key(id));
        }
    }
    w.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_common::{self as c, venue::text};

    #[test]
    fn association_keeps_every_financial_and_order_field() {
        for delta in ["-2", "2"] {
            let (snapshot, catalog, cmd) = c::futures_fixture();
            let mut a = Account::from_snapshot(snapshot.clone(), catalog).unwrap();
            let baseline = IncomeBaseline {
                scope: snapshot.scope.clone(),
                account_epoch: 1,
                ledger_sequence: 0,
                wallet_id: 1,
                through_time_ms: 100,
                coverage_digest: [9; 32],
                baseline_digest: [8; 32],
                input_stream: [7; 16],
                through_sequence: 2,
                covered: vec![],
            };
            a.apply_income_baseline(&baseline).unwrap();
            a.admit(&c::canonical(cmd.clone()), 60).unwrap();
            let identity = v::AccountIdentity {
                risk_domain_id: snapshot.scope.risk_domain_id,
                venue: snapshot.scope.venue,
                environment: snapshot.scope.environment,
                account: snapshot.scope.account,
                market: v::AccountMarket::Usdm,
                wallet_id: 1,
            };
            let income = v::CanonicalAccountInput::new(v::AccountInput {
                identity: identity.clone(),
                body: v::AccountInputBody::Income(v::IncomeRecord {
                    income_type: text("FUNDING_FEE"),
                    transaction_id: text("90"),
                    delta: v::AssetDelta {
                        asset: cmd.context.instrument.settlement.clone(),
                        amount: delta.parse().unwrap(),
                    },
                    symbol: Some(cmd.context.instrument.symbol.clone()),
                    trade_id: None,
                    venue_time_ms: 101,
                    info: None,
                }),
            })
            .unwrap();
            let observation = v::CanonicalAccountInput::new(v::AccountInput {
                identity,
                body: v::AccountInputBody::Observation(v::AccountObservation {
                    reason: text("FUNDING_FEE"),
                    funding_symbol: Some(cmd.context.instrument.symbol.clone()),
                    event_time_ms: 101,
                    transaction_time_ms: Some(101),
                    positions: v::BoundedList::new(vec![]).unwrap(),
                    balances: v::BoundedList::new(vec![v::WalletObservation {
                        asset: cmd.context.instrument.settlement.clone(),
                        wallet_balance: if delta == "-2" { "998" } else { "1002" }.parse().unwrap(),
                        cross_wallet_balance: if delta == "-2" { "998" } else { "1002" }
                            .parse()
                            .unwrap(),
                        balance_change: delta.parse().unwrap(),
                    }])
                    .unwrap(),
                }),
            })
            .unwrap();
            a.apply_account_input(&observation).unwrap();
            a.apply_account_input(&income).unwrap();
            let before = a.clone();
            let binding = IncomeObservationsAssociated {
                scope: snapshot.scope,
                account_epoch: 1,
                ledger_sequence: a.sequence,
                wallet_id: 1,
                request_id: [1; 16],
                baseline_digest: [8; 32],
                history_digest: [10; 32],
                input_stream: [7; 16],
                through_sequence: 9,
                state_digest: a
                    .income_association_state_digest([8; 32], [10; 32], 1)
                    .unwrap(),
                manifest_digest: [11; 32],
                approval_digest: [12; 32],
                associations: vec![ObservationAssociation {
                    observation_digest: observation.digest(),
                    incomes: vec![IncomeIdentity {
                        income_type: text("FUNDING_FEE"),
                        transaction_id: text("90"),
                    }],
                }],
            };
            a.associate_income_observations(&binding).unwrap();
            // Explicit field assertions include private cash/cap/reserve and
            // persistent order metadata that the public views intentionally omit.
            macro_rules! unchanged { ($($field:ident),+) => { $(assert_eq!(format!("{:?}", a.$field), format!("{:?}", before.$field), stringify!($field));)+ }; }
            unchanged!(
                scope,
                epoch,
                revisions,
                valid_until,
                assets,
                catalog,
                positions,
                orders,
                admissions,
                facts,
                external_facts,
                handovers,
                venue_available,
                shadow_holds,
                uncovered_debits,
                shadow_unvalued,
                rejected_event
            );
            assert_eq!(a.account_inputs.caps, before.account_inputs.caps);
            assert_eq!(
                a.account_inputs.credit_holds,
                before.account_inputs.credit_holds
            );
            assert_eq!(a.account_inputs.baseline, before.account_inputs.baseline);
            assert_eq!(
                a.account_inputs.components,
                before.account_inputs.components
            );
            assert_eq!(a.account_inputs.aliases, before.account_inputs.aliases);
            assert_eq!(a.account_inputs.rejected, before.account_inputs.rejected);
            assert_eq!(a.account_inputs.closures, before.account_inputs.closures);
            assert_eq!(
                a.account_inputs.observation_inputs,
                before.account_inputs.observation_inputs
            );
            assert_eq!(
                format!("{:?}", a.account_inputs.incomes),
                format!("{:?}", before.account_inputs.incomes)
            );
            assert_eq!(a.sequence, before.sequence + 1);
            assert!(a.frozen);
            assert!(!a.account_inputs.observations[&observation.digest()]);
        }
    }
}
