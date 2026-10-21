use super::encoding::{Anchor, Group};
use super::*;
use crate::reconciliation::income_closure::encoding::Selected;
use std::collections::{BTreeMap, BTreeSet};
use tickwire_account::{Account, AssetKey, IncomeIdentity, RecoveryRecord};
use tickwire_account_codec::CanonicalRecord;
use tickwire_risk::Fixed;

pub(super) fn prefix(baseline: &CanonicalBaseline, prior: &[CanonicalRecord]) -> Result<v::Hash> {
    if prior.len() > tickwire_account::MAX_FACTS + tickwire_account::MAX_ORDERS {
        return Err(Error::Capacity);
    }
    let mut h = ring::digest::Context::new(&ring::digest::SHA256);
    h.update(b"tickwire.binance.income-association-account-prefix.v1");
    h.update(&baseline.digest());
    h.update(&(baseline.bytes().len() as u64).to_le_bytes());
    h.update(baseline.bytes());
    h.update(&(prior.len() as u64).to_le_bytes());
    for (i, r) in prior.iter().enumerate() {
        if r.sequence() != i as u64 + 1 || r.baseline() != baseline.digest() {
            return Err(Error::Identity);
        }
        h.update(&(r.bytes().len() as u64).to_le_bytes());
        h.update(r.bytes());
    }
    Ok(h.finish().as_ref().try_into().unwrap())
}
fn id(input: &v::CanonicalAccountInput) -> Option<IncomeIdentity> {
    let v::AccountInputBody::Income(row) = &input.input().body else {
        return None;
    };
    Some(IncomeIdentity {
        income_type: row.income_type.clone(),
        transaction_id: row.transaction_id.clone(),
    })
}
fn recover(baseline: &CanonicalBaseline, prior: &[CanonicalRecord]) -> Result<Account> {
    prefix(baseline, prior)?;
    Account::recover(
        baseline.snapshot().clone(),
        baseline.catalog().clone(),
        &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
    )
    .map_err(|_| Error::Identity)
}
pub(super) struct Interval {
    pub anchor: Anchor,
    pub selected: Vec<Selected>,
}
impl<S: Store> AccountIngress<S> {
    pub(super) fn association_interval(
        &self,
        account: &Account,
        prior: &[CanonicalRecord],
        groups: &[Group],
        wallet: u64,
    ) -> Result<Interval> {
        if groups.is_empty() || groups.len() > tickwire_account::MAX_ASSOCIATION_OBSERVATIONS {
            return Err(Error::Capacity);
        }
        let baseline = &self.config.baseline;
        let commitment = prefix(baseline, prior)?;
        let recovered = recover(baseline, prior)?;
        if recovered
            .account_inputs_state_digest(baseline.digest(), commitment)
            .map_err(|_| Error::Identity)?
            != account
                .account_inputs_state_digest(baseline.digest(), commitment)
                .map_err(|_| Error::Identity)?
        {
            return Err(Error::Identity);
        }
        let keys = groups
            .iter()
            .flat_map(|g| g.incomes.iter().cloned())
            .collect::<BTreeSet<_>>();
        if keys.len() != groups.iter().map(|g| g.incomes.len()).sum::<usize>()
            || keys.len() > tickwire_account::MAX_ASSOCIATION_INCOMES
        {
            return Err(Error::Identity);
        }
        let observations = groups.iter().map(|g| g.canonical).collect::<BTreeSet<_>>();
        if observations.len() != groups.len()
            || account
                .unexplained_account_observations()
                .map(|o| o.digest())
                .collect::<BTreeSet<_>>()
                != observations
        {
            return Err(Error::Identity);
        }
        let mut rows = BTreeMap::new();
        let mut first = None;
        for (index, r) in prior.iter().enumerate() {
            if let RecoveryRecord::IngressAccountInput {
                input_stream,
                input_sequence,
                input,
            } = r.record()
            {
                let wanted = observations.contains(&input.digest())
                    || id(input).is_some_and(|i| keys.contains(&i));
                if !wanted {
                    continue;
                }
                if *input_stream != self.input_stream()
                    || self.account_effect(
                        input_sequence.checked_sub(1).ok_or(Error::Identity)? as usize
                    ) != Some(input.as_ref())
                {
                    return Err(Error::Identity);
                }
                first.get_or_insert(index);
                if let Some(k) = id(input)
                    && keys.contains(&k)
                    && let Some(old) = rows.insert(k, input.as_ref().clone())
                    && old != **input
                {
                    return Err(Error::Conflict);
                }
            }
        }
        if rows.len() != keys.len() {
            return Err(Error::Identity);
        }
        let first = first.ok_or(Error::Identity)?;
        let anchor_index = prior[..first]
            .iter()
            .rposition(|r| {
                matches!(r.record(),RecoveryRecord::IncomeBaseline(b) if b.wallet_id==wallet)
                    || matches!(r.record(), RecoveryRecord::Reactivated(_))
            })
            .ok_or(Error::Identity)?
            + 1;
        let anchor_account = recover(baseline, &prior[..anchor_index])?;
        if anchor_account.income_baseline(wallet).is_none()
            || anchor_account.unresolved_account_inputs() != 0
        {
            return Err(Error::Identity);
        }
        let mut replay = anchor_account.clone();
        for r in &prior[anchor_index..] {
            match r.record() {
                RecoveryRecord::IngressAccountInput {
                    input_stream,
                    input_sequence,
                    input,
                } => {
                    if *input_stream != self.input_stream()
                        || self.account_effect(
                            input_sequence.checked_sub(1).ok_or(Error::Identity)? as usize,
                        ) != Some(input.as_ref())
                    {
                        return Err(Error::Identity);
                    }
                    let selected = observations.contains(&input.digest())
                        || id(input).is_some_and(|i| keys.contains(&i));
                    let applied = replay
                        .apply_account_input(input)
                        .map_err(|_| Error::Identity)?;
                    if !selected && applied != tickwire_account::Applied::Duplicate {
                        return Err(Error::Unsupported);
                    }
                }
                RecoveryRecord::AccountInputsReconciled(b) => {
                    replay
                        .reconcile_account_inputs(b)
                        .map_err(|_| Error::Identity)?;
                }
                RecoveryRecord::Reactivated(b) => {
                    replay.apply_reactivation(b).map_err(|_| Error::Identity)?;
                }
                // The allowed table is intentional: even zero-cash management,
                // order, fee or attribution changes are outside this interval.
                _ => return Err(Error::Unsupported),
            }
        }
        if replay
            .account_inputs_state_digest(baseline.digest(), commitment)
            .map_err(|_| Error::Identity)?
            != account
                .account_inputs_state_digest(baseline.digest(), commitment)
                .map_err(|_| Error::Identity)?
        {
            return Err(Error::Identity);
        }
        for p in &baseline.catalog().entries {
            for side in [
                v::PositionSide::Both,
                v::PositionSide::Long,
                v::PositionSide::Short,
            ] {
                if anchor_account
                    .position(p.instrument.instrument_id, side)
                    .map_err(|_| Error::Identity)?
                    != account
                        .position(p.instrument.instrument_id, side)
                        .map_err(|_| Error::Identity)?
                    || anchor_account
                        .position_entry_bound(p.instrument.instrument_id, side)
                        .map_err(|_| Error::Identity)?
                        != account
                            .position_entry_bound(p.instrument.instrument_id, side)
                            .map_err(|_| Error::Identity)?
                {
                    return Err(Error::Unsupported);
                }
            }
        }
        if anchor_account
            .order_keys()
            .map(|k| anchor_account.order(k))
            .collect::<Vec<_>>()
            != account
                .order_keys()
                .map(|k| account.order(k))
                .collect::<Vec<_>>()
        {
            return Err(Error::Unsupported);
        }
        let mut totals = BTreeMap::new();
        for a in &baseline.snapshot().assets {
            totals.insert(
                a.key,
                anchor_account
                    .asset(a.key)
                    .map_err(|_| Error::Identity)?
                    .total,
            );
        }
        let mut last = 0;
        let mut epoch = None;
        for g in groups {
            if g.input.sequence <= last || g.input != self.reference(g.input.sequence)? {
                return Err(Error::Identity);
            }
            let Record::Event(meta, raw, Decision::AccountBound(input)) = self
                .records
                .get(g.input.sequence as usize - 1)
                .ok_or(Error::Identity)?
            else {
                return Err(Error::Identity);
            };
            if meta.epoch != g.epoch
                || epoch.is_some_and(|e| e != g.epoch)
                || input.digest() != g.canonical
                || input.bytes().len() as u64 != g.canonical_length
                || authority::hash(raw) != g.raw_hash
            {
                return Err(Error::Identity);
            }
            epoch = Some(g.epoch);
            last = g.input.sequence;
            let v::AccountInputBody::Observation(observation) = &input.input().body else {
                return Err(Error::Identity);
            };
            let mut delta = BTreeMap::new();
            for key in &g.incomes {
                let row = rows.get(key).ok_or(Error::Identity)?;
                let v::AccountInputBody::Income(income) = &row.input().body else {
                    return Err(Error::Identity);
                };
                let asset_key = AssetKey {
                    wallet_id: wallet,
                    asset_id: income.delta.asset.id,
                };
                let total = totals.get_mut(&asset_key).ok_or(Error::Identity)?;
                let amount = fixed(income.delta.amount, total.scale())?;
                *total = total.checked_add(amount).map_err(|_| Error::Identity)?;
                let sum = delta.entry(asset_key).or_insert(amount.zero_like());
                *sum = sum.checked_add(amount).map_err(|_| Error::Identity)?;
            }
            for b in observation.balances.as_slice() {
                let k = AssetKey {
                    wallet_id: wallet,
                    asset_id: b.asset.id,
                };
                let total = totals.get(&k).ok_or(Error::Identity)?;
                if fixed(b.wallet_balance, total.scale())? != *total
                    || fixed(b.cross_wallet_balance, total.scale())? != *total
                    || fixed(b.balance_change, total.scale())?
                        != delta.remove(&k).unwrap_or(total.zero_like())
                {
                    return Err(Error::Identity);
                }
            }
            if !delta.is_empty() {
                return Err(Error::Identity);
            }
        }
        for (k, total) in totals {
            if account.asset(k).map_err(|_| Error::Identity)?.total != total {
                return Err(Error::Identity);
            }
        }
        let selected = rows
            .values()
            .map(income_closure::selected_row)
            .collect::<Result<Vec<_>>>()?;
        Ok(Interval {
            anchor: Anchor {
                sequence: anchor_index as u64,
                ledger: anchor_account.ledger_sequence(),
                prefix: prefix(baseline, &prior[..anchor_index])?,
            },
            selected,
        })
    }
}
fn fixed(value: v::Decimal, scale: u8) -> Result<Fixed> {
    let value = if value.exponent() <= 0 {
        Fixed::from_atoms(value.coefficient(), value.exponent().unsigned_abs())
    } else {
        10i128
            .checked_pow(value.exponent() as u32)
            .and_then(|p| value.coefficient().checked_mul(p))
            .ok_or(tickwire_risk::RiskError::ArithmeticOverflow)
            .and_then(|a| Fixed::from_atoms(a, 0))
    };
    let original = value.map_err(|_| Error::Identity)?;
    let converted = original
        .rescale(scale, tickwire_risk::Rounding::Floor)
        .map_err(|_| Error::Identity)?;
    if converted
        .rescale(original.scale(), tickwire_risk::Rounding::Floor)
        .map_err(|_| Error::Identity)?
        != original
    {
        return Err(Error::Identity);
    }
    Ok(converted)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconciliation::income_import::tests::approved_fixture;
    fn fixture() -> (
        AccountIngress<crate::reconciliation::income_import::tests::Memory>,
        Account,
        Vec<CanonicalRecord>,
        Vec<Group>,
    ) {
        let (mut input, mut account, mut journal, _) = approved_fixture();
        let row = v::CanonicalAccountInput::new(v::AccountInput {
            identity: v::AccountIdentity {
                risk_domain_id: [1; 16],
                venue: 1,
                environment: v::Environment::Test,
                account: 2,
                market: v::AccountMarket::Usdm,
                wallet_id: 1,
            },
            body: v::AccountInputBody::Income(v::IncomeRecord {
                income_type: v::BoundedText::new("FUNDING_FEE").unwrap(),
                transaction_id: v::BoundedText::new("43").unwrap(),
                symbol: Some(v::Symbol::new("BTCUSDT").unwrap()),
                trade_id: None,
                delta: v::AssetDelta {
                    asset: input
                        .config
                        .baseline
                        .snapshot()
                        .assets
                        .iter()
                        .find(|a| a.key.asset_id == 2)
                        .unwrap()
                        .asset
                        .clone(),
                    amount: v::Decimal::new(-2, 0).unwrap(),
                },
                venue_time_ms: 105,
                info: None,
            }),
        })
        .unwrap();
        let body=br#"[{"symbol":"BTCUSDT","incomeType":"FUNDING_FEE","income":"-2","asset":"USDT","time":105,"tranId":43,"tradeId":""}]"#;
        let source = input
            .append(Record::Income(
                crate::reconciliation::income::IncomeRecord {
                    query: crate::reconciliation::income::IncomeQuery {
                        start_time_ms: 101,
                        end_time_ms: 110,
                        limit: 2,
                        private_connection_epoch: 7,
                        gap_generation: 3,
                        purpose: crate::reconciliation::income::IncomeUse::Account,
                    },
                    epoch: 15,
                    received: 20,
                    clock_boot_id: [8; 16],
                    clock_generation: 1,
                    status: 200,
                    accepted: true,
                    raw: crate::reconciliation::income_import::tests::http(body),
                    rows: vec![row.clone()],
                },
            ))
            .unwrap();
        let effect = input
            .append(Record::IncomeEffect {
                response: source.sequence,
                index: 0,
            })
            .unwrap();
        account.apply_account_input(&row).unwrap();
        journal
            .append(RecoveryRecord::IngressAccountInput {
                input_stream: input.input_stream(),
                input_sequence: effect.sequence,
                input: Box::new(row.clone()),
            })
            .unwrap();
        let raw=br#"{"e":"ACCOUNT_UPDATE","E":106,"T":105,"a":{"m":"FUNDING_FEE","B":[{"a":"USDT","wb":"998","cw":"998","bc":"-2"}]}}"#;
        let meta = SourceMeta {
            epoch: 7,
            subscription: None,
            micros: false,
            received: 21,
        };
        let observation =
            crate::account_facts::decode_observation(&input.config.baseline, &meta, raw, 4096)
                .unwrap();
        let key = input
            .append(Record::Event(
                meta,
                raw.to_vec(),
                Decision::AccountBound(Box::new(observation.clone())),
            ))
            .unwrap();
        account.apply_account_input(&observation).unwrap();
        journal
            .append(RecoveryRecord::IngressAccountInput {
                input_stream: key.stream,
                input_sequence: key.sequence,
                input: Box::new(observation.clone()),
            })
            .unwrap();
        let groups = vec![Group {
            input: input.reference(key.sequence).unwrap(),
            canonical: observation.digest(),
            canonical_length: observation.bytes().len() as u64,
            epoch: 7,
            raw_hash: authority::hash(raw),
            incomes: vec![id(&row).unwrap()],
        }];
        (input, account, journal.records().to_vec(), groups)
    }
    #[test]
    fn actual_account_prefix_and_allowed_funding_interval_are_required() {
        let (input, account, prior, groups) = fixture();
        let result = input
            .association_interval(&account, &prior, &groups, 1)
            .unwrap();
        assert_eq!(result.anchor.sequence, 1);
        assert_eq!(result.anchor.ledger, 1);
        assert_eq!(result.selected.len(), 1);
        let mut wrong = groups.clone();
        wrong[0].canonical = [99; 32];
        assert!(
            input
                .association_interval(&account, &prior, &wrong, 1)
                .is_err()
        );
        let mut reverse = prior.clone();
        reverse.swap(1, 2);
        assert!(prefix(&input.config.baseline, &reverse).is_err());
        assert_ne!(
            prefix(&input.config.baseline, &prior).unwrap(),
            prefix(&input.config.baseline, &prior[..2]).unwrap()
        );
        let mut snapshot = input.config.baseline.snapshot().clone();
        snapshot.account_epoch += 1;
        let other =
            CanonicalBaseline::new(snapshot, input.config.baseline.catalog().clone()).unwrap();
        assert!(prefix(&other, &prior).is_err());
        let mut changed = prior.clone();
        changed[2] = CanonicalRecord::new(
            input.config.baseline.digest(),
            3,
            RecoveryRecord::FeesReconciled {
                order: tickwire_account::OrderKey {
                    intent: tickwire_account::IntentKey {
                        epoch: 1,
                        intent_id: 99,
                    },
                    client_id: v::ClientId::new("unknown").unwrap(),
                },
                cumulative: Fixed::zero(8).unwrap(),
            },
        )
        .unwrap();
        assert_ne!(
            prefix(&input.config.baseline, &prior).unwrap(),
            prefix(&input.config.baseline, &changed).unwrap()
        );
        assert!(
            input
                .association_interval(&account, &changed, &groups, 1)
                .is_err()
        );
    }
}
