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
            if a.command_digest != command.digest()
                || a.intent != key
                || a.account_epoch == 0
                || a.ledger_sequence == 0
                || a.deadline != c.request.deadline
                || *evaluated_at >= a.deadline
                || a.revisions.risk != c.strategy.risk_policy_version
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
