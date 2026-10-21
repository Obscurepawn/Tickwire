use crate::{Error, MAX_ADMISSION_ORDERS, MAX_BRACKETS, MAX_OWNERS};
use std::collections::BTreeSet;
use std::result::Result;
use tickwire_account::*;
use tickwire_venue as v;

fn bound(n: usize, max: usize) -> Result<(), Error> {
    if n > max { Err(Error::Bounds) } else { Ok(()) }
}
fn sort_unique<T, K: Ord>(items: &mut [T], key: impl Fn(&T) -> K) -> Result<(), Error> {
    items.sort_by_key(&key);
    if items.windows(2).any(|w| key(&w[0]) == key(&w[1])) {
        Err(Error::Duplicate)
    } else {
        Ok(())
    }
}
fn unique<T, K: Ord>(items: &[T], key: impl Fn(&T) -> K) -> Result<(), Error> {
    let mut keys = BTreeSet::new();
    if items.iter().any(|x| !keys.insert(key(x))) {
        Err(Error::Duplicate)
    } else {
        Ok(())
    }
}
pub(crate) fn side(side: v::PositionSide) -> u8 {
    match side {
        v::PositionSide::Both => 1,
        v::PositionSide::Long => 2,
        v::PositionSide::Short => 3,
    }
}
pub(crate) fn baseline(s: &mut Snapshot, c: &mut Catalog) -> Result<(), Error> {
    bound(s.assets.len(), MAX_ASSETS)?;
    bound(s.positions.len(), 2 * MAX_INSTRUMENTS)?;
    bound(s.venue_available.len(), MAX_ASSETS)?;
    bound(c.entries.len(), MAX_INSTRUMENTS)?;
    sort_unique(&mut s.assets, |a| a.key)?;
    sort_unique(&mut s.positions, |p| (p.instrument_id, side(p.side)))?;
    sort_unique(&mut s.venue_available, |v| v.0)?;
    sort_unique(&mut c.entries, |p| p.instrument.instrument_id)?;
    for a in &mut s.assets {
        bound(a.ownership.len(), MAX_OWNERS)?;
        sort_unique(&mut a.ownership, |v| v.0)?;
    }
    for p in &mut s.positions {
        bound(p.ownership.len(), MAX_OWNERS)?;
        sort_unique(&mut p.ownership, |v| v.0)?;
    }
    for p in &mut c.entries {
        bound(p.strategy_limits.len(), MAX_OWNERS)?;
        sort_unique(&mut p.strategy_limits, |v| v.0)?;
        if let Some(fp) = &p.futures {
            bound(fp.brackets.len(), MAX_BRACKETS)?;
        }
    }
    Ok(())
}
pub(crate) fn record(r: &RecoveryRecord) -> Result<(), Error> {
    match r {
        RecoveryRecord::UncreatedAlgoClosed(b) => {
            if b.scope.risk_domain_id == [0; 16]
                || b.scope.venue == 0
                || b.scope.account == 0
                || b.account_epoch == 0
                || b.baseline_digest == [0; 32]
                || b.history_digest == [0; 32]
                || b.input_stream == [0; 16]
                || b.through_sequence == 0
                || b.state_digest == [0; 32]
                || b.coverage_digest == [0; 32]
                || b.structure_id == [0; 16]
                || b.intent.epoch == 0
                || b.intent.intent_id == 0
                || b.command_digest == [0; 32]
            {
                return Err(Error::Field);
            }
        }
        RecoveryRecord::IncomeObservationsAssociated(b) => {
            if b.scope.risk_domain_id == [0; 16]
                || b.scope.venue == 0
                || b.scope.account == 0
                || b.account_epoch == 0
                || b.wallet_id == 0
                || b.request_id == [0; 16]
                || b.baseline_digest == [0; 32]
                || b.history_digest == [0; 32]
                || b.input_stream == [0; 16]
                || b.through_sequence == 0
                || b.state_digest == [0; 32]
                || b.manifest_digest == [0; 32]
                || b.approval_digest == [0; 32]
                || b.associations.is_empty()
            {
                return Err(Error::Field);
            }
            bound(b.associations.len(), MAX_ASSOCIATION_OBSERVATIONS)?;
            unique(&b.associations, |group| group.observation_digest)?;
            let mut keys = BTreeSet::new();
            for group in &b.associations {
                if group.observation_digest == [0; 32]
                    || group.incomes.is_empty()
                    || group.incomes.windows(2).any(|w| w[0] >= w[1])
                {
                    return Err(Error::Field);
                }
                for id in &group.incomes {
                    let number = id
                        .transaction_id
                        .as_str()
                        .parse::<i64>()
                        .map_err(|_| Error::Field)?;
                    if id.income_type.as_str() != "FUNDING_FEE"
                        || number.to_string() != id.transaction_id.as_str()
                    {
                        return Err(Error::Field);
                    }
                    if !keys.insert(id) {
                        return Err(Error::Duplicate);
                    }
                    bound(keys.len(), MAX_ASSOCIATION_INCOMES)?;
                }
            }
        }
        RecoveryRecord::AccountInputsReconciled(b) => {
            if b.scope.risk_domain_id == [0; 16]
                || b.scope.venue == 0
                || b.scope.account == 0
                || b.account_epoch == 0
                || b.baseline_digest == [0; 32]
                || b.history_digest == [0; 32]
                || b.input_stream == [0; 16]
                || b.through_sequence == 0
                || b.state_digest == [0; 32]
                || b.coverage_digest == [0; 32]
            {
                return Err(Error::Field);
            }
        }
        RecoveryRecord::IncomeBaseline(b) => {
            if b.scope.risk_domain_id == [0; 16]
                || b.scope.venue == 0
                || b.scope.account == 0
                || b.account_epoch == 0
                || b.wallet_id == 0
                || b.through_time_ms == 0
                || b.coverage_digest == [0; 32]
                || b.baseline_digest == [0; 32]
                || b.input_stream == [0; 16]
                || b.through_sequence == 0
            {
                return Err(Error::Field);
            }
            bound(b.covered.len(), MAX_BASELINE_INCOMES)?;
            let mut keys = std::collections::BTreeSet::new();
            for input in &b.covered {
                let id = &input.input().identity;
                let tickwire_venue::AccountInputBody::Income(row) = &input.input().body else {
                    return Err(Error::Field);
                };
                if id.risk_domain_id != b.scope.risk_domain_id
                    || id.venue != b.scope.venue
                    || id.account != b.scope.account
                    || id.environment != b.scope.environment
                    || id.wallet_id != b.wallet_id
                    || row.venue_time_ms > b.through_time_ms
                {
                    return Err(Error::Field);
                }
                if !keys.insert((&row.income_type, &row.transaction_id)) {
                    return Err(Error::Duplicate);
                }
            }
        }
        RecoveryRecord::StrategyHandover(h) => {
            if h.scope.risk_domain_id == [0; 16]
                || h.scope.venue == 0
                || h.scope.account == 0
                || h.account_epoch == 0
                || h.writer_epoch == 0
                || h.operation_id == [0; 16]
                || h.operation_revision == 0
                || h.logical_strategy_id == 0
                || h.expected_orders_digest == [0; 32]
                || h.from.instance == [0; 16]
                || h.to.instance == [0; 16]
                || h.from.instance == h.to.instance
                || h.from.generation == 0
                || h.to.generation <= h.from.generation
                || h.checkpoint_digest == [0; 32]
                || h.input_seal_digest == [0; 32]
                || h.evaluated_at >= h.valid_until
            {
                return Err(Error::Field);
            }
        }
        RecoveryRecord::Reactivated(b) => {
            if b.venue_available.len() > MAX_ASSETS
                || b.venue_available.windows(2).any(|p| p[0].0 >= p[1].0)
                || b.venue_available
                    .iter()
                    .any(|(wallet, amount)| *wallet == 0 || amount.atoms() < 0)
            {
                return Err(Error::Field);
            }

            if b.scope.risk_domain_id == [0; 16]
                || b.scope.venue == 0
                || b.scope.account == 0
                || b.account_epoch == 0
                || b.baseline_digest == [0; 32]
                || b.history_digest == [0; 32]
                || b.query_coverage_digest == [0; 32]
                || b.input_stream == [0; 16]
                || b.private_connection_epoch == 0
                || b.gap_generation == 0
                || b.through_sequence == 0
                || b.clock_boot_id == [0; 16]
                || b.clock_generation == 0
                || b.evaluated_at >= b.valid_until
                || b.writer_epoch == 0
                || b.execution_connection_epoch == 0
            {
                return Err(Error::Field);
            }
        }

        RecoveryRecord::IngressEvent {
            input_stream,
            input_sequence,
            ..
        }
        | RecoveryRecord::IngressAccountInput {
            input_stream,
            input_sequence,
            ..
        } if *input_stream == [0; 16] || *input_sequence == 0 => return Err(Error::Field),
        RecoveryRecord::Admit {
            command,
            evaluated_at,
            expected: a,
        } => {
            bound(a.resources.len(), MAX_ASSETS)?;
            bound(a.capital_holds.len(), MAX_ASSETS)?;
            bound(a.orders.len(), MAX_ADMISSION_ORDERS)?;
            unique(&a.resources, |x| x.key)?;
            unique(&a.capital_holds, |x| x.wallet_id)?;
            unique(&a.orders, |x| x.clone())?;
            let c = &command.command().context;
            let key = IntentKey {
                epoch: c.request.intent_exec_epoch,
                intent_id: c.request.intent_id,
            };
            let increases = !matches!(
                command.command().body,
                tickwire_venue::CommandBody::Cancel(_)
                    | tickwire_venue::CommandBody::Query(_)
                    | tickwire_venue::CommandBody::CancelList(_)
                    | tickwire_venue::CommandBody::CancelAlgo(_)
                    | tickwire_venue::CommandBody::AmendKeepPriority { .. }
            );
            if a.command_digest != command.digest()
                || a.intent != key
                || a.increases != increases
                || a.account_epoch == 0
                || a.ledger_sequence == 0
                || a.deadline != c.request.deadline
                || *evaluated_at >= a.deadline
                || (increases && a.revisions.risk != c.strategy.risk_policy_version)
                || a.resources.iter().any(|x| x.amount.atoms() <= 0)
                || a.capital_holds.iter().any(|x| x.amount.atoms() <= 0)
                || a.orders.iter().any(|o| o.intent != key)
            {
                return Err(Error::Field);
            }
        }
        RecoveryRecord::Aborted {
            intent,
            command_digest,
        } => {
            if intent.epoch == 0 || intent.intent_id == 0 || *command_digest == [0; 32] {
                return Err(Error::Field);
            }
        }
        RecoveryRecord::FeesReconciled { order, cumulative }
            if order.intent.epoch == 0 || order.intent.intent_id == 0 || cumulative.atoms() < 0 =>
        {
            return Err(Error::Field);
        }
        _ => {}
    }
    Ok(())
}
