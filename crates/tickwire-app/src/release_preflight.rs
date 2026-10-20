//! Release qualification uses only actual Host completion and current account data.
use crate::{Result, invalid};
use std::collections::{BTreeMap, BTreeSet};
use tickwire_account::{Account, RecoveryRecord};
use tickwire_account_gateway::{AccountGateway, Authority, Encoder};
use tickwire_account_journal::Backend;
use tickwire_lifecycle::{ControlRequest, Member, Operation};
use tickwire_release::{OrderKind, PositionMode, Product, Requirements, closure::Verified};
use tickwire_runtime::node::Strategy;

pub(super) fn qualify<S: Strategy, B: Backend, A: Authority, E: Encoder>(
    r: &ControlRequest,
    strategy: &mut S,
    gateway: &AccountGateway<B, A, E>,
) -> Result<Verified> {
    if r.operation.schema != 2 {
        return Err(invalid("PreflightRequired: use release operation schema 2"));
    }
    let roots = [
        r.operation.artifacts_digest,
        r.operation.checkpoint_digest,
        r.operation.rollback_digest,
    ];
    let objects = strategy
        .release_preflight(r.member.new_instance, r.member.new_generation, roots)
        .ok_or_else(|| invalid("actual restored process and rollback probe are not complete"))?;
    let mut map = BTreeMap::new();
    let mut total = 0usize;
    if objects.len() > 256 {
        return Err(invalid("release closure object capacity"));
    }
    for bytes in objects {
        total = total
            .checked_add(bytes.len())
            .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
            .ok_or_else(|| invalid("release closure byte capacity"))?;
        map.insert(tickwire_recovery::digest(&bytes), bytes);
    }
    let completions = map
        .iter()
        .filter_map(|(h, b)| {
            tickwire_release::receipt::Completion::decode(b)
                .ok()
                .filter(|c| c.operation == r.operation && c.member == r.member)
                .map(|_| *h)
        })
        .collect::<Vec<_>>();
    if completions.len() != 1 {
        return Err(invalid("missing or ambiguous release completion"));
    }
    let verified = tickwire_release::closure::verify(completions[0], |h| {
        map.get(&h).cloned().ok_or(tickwire_release::Error::Missing)
    })?;
    let release = verified.bundle.verify_operation(&r.operation, &r.member)?;
    let requirements = requirements(
        gateway.account(),
        gateway.baseline().catalog(),
        r.operation.logical_strategy_id,
        gateway
            .journal_records_after(0, tickwire_account_journal::MAX_RECORDS)?
            .map(|(_, r)| r),
    )?;
    release.check_capabilities(&release.target.capabilities, &requirements)?;
    // The fixed source callbacks must be the real retained account prefix, even
    // when the current account has progressed beyond this migration start.
    let bytes = find(&verified.objects, release.migration.source_proof.digest)?;
    let source = tickwire_recovery::HandoverSeal::decode(bytes)?;
    let source_ready = tickwire_runtime::node::HandoverReadiness {
        instance_id: source.instance,
        strategy_generation: source.generation,
        checkpoint_digest: source.checkpoint_digest,
        exec_report_sequence: source.report_position,
        orders_digest: source.orders_digest,
        ledger_sequence: source.ledger_sequence,
        input_seal_digest: release.migration.source_proof.digest,
    };
    let actual = strategy
        .handover_objects(&source_ready)
        .ok_or_else(|| invalid("fixed source has no confirmed live old Host proof"))?;
    for h in source
        .roots()
        .into_iter()
        .chain([release.migration.source_proof.digest])
    {
        if !actual.iter().any(|b| tickwire_recovery::digest(b) == h) {
            return Err(invalid("old Host does not retain the fixed source closure"));
        }
    }
    let history = if source.report_position == 0 {
        Vec::new()
    } else {
        gateway
            .journal_records_after(0, usize::try_from(source.report_position)?)?
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>()
    };
    if history.len() as u64 != source.report_position {
        return Err(invalid("migration account prefix is unavailable"));
    }
    let inputs = tickwire_handover::verify(
        &source,
        find(&verified.objects, source.checkpoint_digest)?,
        find(&verified.objects, source.input_digest)?,
        find(&verified.objects, source.account_input_digest)?,
    )
    .map_err(|_| invalid("fixed migration input closure"))?;
    inputs
        .matches_history(gateway.baseline(), &history)
        .map_err(|_| invalid("migration source account history differs"))?;
    Ok(verified)
}
fn find(objects: &[Vec<u8>], h: [u8; 32]) -> Result<&[u8]> {
    objects
        .iter()
        .find(|b| tickwire_recovery::digest(b) == h)
        .map(Vec::as_slice)
        .ok_or_else(|| invalid("release object missing"))
}
/// Called only after quiesce. Initial built-ins require exactly unchanged
/// business state; generic merged-input tail replay is not silently approximated.
pub(super) fn unchanged<S: Strategy>(strategy: &mut S, v: &Verified) -> Result<()> {
    let m = &v.completion.member;
    let (ready, objects) = strategy
        .handover_checkpoint(m.old_instance, m.old_generation)
        .ok_or_else(|| invalid("old process did not seal current quiesced callback state"))?;
    let release = v.bundle.verify_operation(&v.completion.operation, m)?;
    let fixed = find(&v.objects, release.migration.source_checkpoint.digest)?;
    let current = find(&objects, ready.checkpoint_digest)?;
    if !same_business_state(fixed, current) {
        return Err(invalid(
            "source state changed after fixed migration; create a new operation",
        ));
    }
    Ok(())
}
fn same_business_state(fixed: &[u8], current: &[u8]) -> bool {
    fixed.len() > 96 && current.len() > 96 && fixed[96..] == current[96..]
}
pub(super) fn root(v: &Verified) -> Result<[u8; 32]> {
    Ok(tickwire_recovery::digest(&v.completion.bytes()?))
}
pub(super) fn bind(v: &Verified, op: &Operation, m: &Member) -> Result<()> {
    if v.completion.operation != *op || v.completion.member != *m {
        return Err(invalid("release operation binding"));
    }
    Ok(())
}
fn order(o: &tickwire_venue::OrderSpec) -> OrderKind {
    use tickwire_venue::OrderType::*;
    match o.order_type {
        Limit => OrderKind::Limit,
        Market => OrderKind::Market,
        StopLimit => OrderKind::StopLimit,
        StopMarket => OrderKind::StopMarket,
        TakeProfitLimit => OrderKind::TakeProfitLimit,
        TakeProfitMarket => OrderKind::TakeProfitMarket,
        TrailingStop => OrderKind::TrailingStop,
    }
}
fn requirements<'a>(
    account: &Account,
    catalog: &tickwire_account::Catalog,
    logical: u64,
    records: impl Iterator<Item = &'a tickwire_account_codec::CanonicalRecord>,
) -> Result<Requirements> {
    let mut products = BTreeSet::new();
    let mut modes = BTreeSet::new();
    let mut orders = BTreeSet::new();
    for i in &catalog.entries {
        use tickwire_venue::Product as P;
        products.insert(match i.product {
            P::Spot => Product::Spot,
            P::LinearPerpetual => Product::LinearPerpetual,
            P::InversePerpetual => Product::InversePerpetual,
            P::LinearFuture => Product::LinearFuture,
            P::InverseFuture => Product::InverseFuture,
            P::Option => Product::Option,
        });
        modes.insert(match &i.futures {
            None => PositionMode::Spot,
            Some(f) => match f.position_mode {
                tickwire_venue::PositionMode::OneWay => PositionMode::OneWay,
                tickwire_venue::PositionMode::Hedge => PositionMode::Hedge,
            },
        });
    }
    let mut needed = account
        .order_keys()
        .filter_map(|k| account.order(k))
        .filter(|o| {
            o.strategy == logical && !o.aborted && (!o.terminal || !o.fee_reconciled || o.unknown)
        })
        .map(|o| o.command_digest)
        .collect::<BTreeSet<_>>();
    for r in records {
        let RecoveryRecord::Admit { command, .. } = r.record() else {
            continue;
        };
        if !needed.remove(&command.digest()) {
            continue;
        }
        use tickwire_venue::{CommandBody as C, SpotAdvancedCommand as S};
        match &command.command().body {
            C::New(o) | C::CancelReplace { replacement: o, .. } => {
                orders.insert(order(o));
            }
            C::SpotList(l) => {
                orders.insert(OrderKind::SpotList);
                for o in l.legs.as_slice() {
                    orders.insert(order(o));
                }
            }
            C::Algo(a) => {
                orders.insert(OrderKind::Algo);
                orders.insert(order(&a.order));
            }
            C::SpotAdvanced(a) => match a {
                S::New(o) | S::CancelReplace { replacement: o, .. } => {
                    if o.peg.is_some() {
                        orders.insert(OrderKind::Peg);
                    }
                    orders.insert(order(&o.order));
                }
                S::List(l) => {
                    orders.insert(OrderKind::SpotList);
                    for o in l.legs.as_slice() {
                        if o.peg.is_some() {
                            orders.insert(OrderKind::Peg);
                        }
                        orders.insert(order(&o.order));
                    }
                }
                S::SorNew(o) => {
                    orders.insert(OrderKind::Sor);
                    orders.insert(order(o));
                }
            },
            _ => return Err(invalid("managed order has unsupported original command")),
        }
    }
    if !needed.is_empty() {
        return Err(invalid("managed order command history is incomplete"));
    }
    Ok(Requirements {
        products: products.into_iter().collect(),
        position_modes: modes.into_iter().collect(),
        orders: orders.into_iter().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn refreshed_header_does_not_mask_any_business_state_change() {
        let mut fixed = vec![0; 96];
        fixed.extend(b"TWFD0002\x01actual-action-key");
        let mut current = fixed.clone();
        current[..96].fill(9);
        assert!(same_business_state(&fixed, &current));
        *current.last_mut().unwrap() ^= 1;
        assert!(!same_business_state(&fixed, &current));
        assert!(!same_business_state(&fixed, &fixed[..96]));
    }
}
