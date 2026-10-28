//! Release qualification uses only actual Host completion and current account data.
use crate::{Result, invalid};
use std::collections::{BTreeMap, BTreeSet};
use tickwire_account::{Account, RecoveryRecord};
use tickwire_account_gateway::{AccountGateway, Authority, Encoder};
use tickwire_account_journal::Backend;
use tickwire_lifecycle::{ControlRequest, Member, Operation};
use tickwire_release::{OrderKind, PositionMode, Product, Requirements, closure::Verified};
use tickwire_runtime::node::Strategy;

#[cfg(test)]
std::thread_local! { static HASHED_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
fn object_digest(bytes: &[u8]) -> [u8; 32] {
    #[cfg(test)]
    HASHED_BYTES.with(|n| n.set(n.get() + bytes.len()));
    tickwire_recovery::digest(bytes)
}
/// Immutable bytes are hashed once at this invocation's boundary. Reads through
/// the index keep all expected Object/length and release identity checks below.
struct ObjectIndex<'a> {
    objects: BTreeMap<[u8; 32], &'a [u8]>,
}
impl<'a> ObjectIndex<'a> {
    fn new(objects: &'a [Vec<u8>]) -> Result<Self> {
        if objects.len() > 512 {
            return Err(invalid("release closure object capacity"));
        }
        let mut total = 0usize;
        for b in objects {
            if b.is_empty() || b.len() > tickwire_recovery::MAX_OBJECT_BYTES {
                return Err(invalid("release closure object size"));
            }
            total = total
                .checked_add(b.len())
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES as usize)
                .ok_or_else(|| invalid("release closure byte capacity"))?;
        }
        let mut indexed = BTreeMap::new();
        for b in objects {
            insert_object(&mut indexed, object_digest(b), b)?;
        }
        Ok(Self { objects: indexed })
    }
    fn get(&self, hash: [u8; 32]) -> Result<&'a [u8]> {
        self.objects
            .get(&hash)
            .copied()
            .ok_or_else(|| invalid("release object missing"))
    }
}
fn insert_object<'a>(
    objects: &mut BTreeMap<[u8; 32], &'a [u8]>,
    hash: [u8; 32],
    bytes: &'a [u8],
) -> Result<()> {
    if let Some(previous) = objects.insert(hash, bytes)
        && previous != bytes
    {
        return Err(invalid("release object digest conflict"));
    }
    Ok(())
}

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
    check_source(r, strategy, gateway, &verified.bundle, &verified.objects)?;
    Ok(verified)
}
/// Schema 3 startup qualifies only the actual S restore, never its rollback.
pub(super) fn qualify_startup<S: Strategy, B: Backend, A: Authority, E: Encoder>(
    r: &ControlRequest,
    strategy: &mut S,
    gateway: &AccountGateway<B, A, E>,
) -> Result<tickwire_release::replay::startup::Verified> {
    if r.operation.schema != 3 {
        return Err(invalid("replay operation schema"));
    }
    let p = r
        .proof
        .as_ref()
        .ok_or_else(|| invalid("startup readiness missing"))?;
    let want = tickwire_runtime::node::HandoverReadiness {
        instance_id: r.member.new_instance,
        strategy_generation: r.member.new_generation,
        checkpoint_digest: p.checkpoint_digest,
        exec_report_sequence: p.exec_report_sequence,
        orders_digest: p.orders_digest,
        ledger_sequence: p.ledger_sequence,
        input_seal_digest: p.input_seal_digest,
    };
    let objects = strategy
        .handover_objects(&want)
        .ok_or_else(|| invalid("actual Shadow startup READY is unavailable"))?;
    if objects.len() > 512 {
        return Err(invalid("startup object capacity"));
    }
    let total = objects
        .iter()
        .try_fold(0usize, |n, b| n.checked_add(b.len()))
        .ok_or_else(|| invalid("startup size"))?;
    if total > tickwire_recovery::MAX_CLOSURE_BYTES as usize {
        return Err(invalid("startup size"));
    }
    let index = ObjectIndex::new(&objects)?;
    let seal = tickwire_recovery::HandoverSeal::decode(index.get(p.input_seal_digest)?)?;
    if seal.version != 3
        || seal.instance != want.instance_id
        || seal.generation != want.strategy_generation
        || seal.checkpoint_digest != p.checkpoint_digest
        || seal.report_position != p.exec_report_sequence
        || seal.ledger_sequence != p.ledger_sequence
        || seal.orders_digest != p.orders_digest
    {
        return Err(invalid("startup READY binding"));
    }
    let get = |h| {
        index
            .get(h)
            .map(Vec::from)
            .map_err(|_| tickwire_release::Error::Missing)
    };
    tickwire_release::replay::startup::verify_seal(
        &seal,
        index.get(seal.checkpoint_digest)?,
        index.get(seal.input_digest)?,
        get,
    )?;
    let verified = tickwire_release::replay::startup::verify(
        seal.replay_restore_digest
            .ok_or_else(|| invalid("startup restore missing"))?,
        get,
    )?;
    if verified.completion.operation != r.operation || verified.completion.member != r.member {
        return Err(invalid("startup operation binding"));
    }
    let release = verified.bundle.verify_operation(&r.operation, &r.member)?;
    let needs = requirements(
        gateway.account(),
        gateway.baseline().catalog(),
        r.operation.logical_strategy_id,
        gateway
            .journal_records_after(0, tickwire_account_journal::MAX_RECORDS)?
            .map(|(_, r)| r),
    )?;
    release.check_capabilities(&release.target.capabilities, &needs)?;
    check_source(r, strategy, gateway, &verified.bundle, &verified.objects)?;
    Ok(verified)
}
pub(super) fn fixed_ready(
    bundle: &tickwire_release::ReleaseBundle,
    op: &Operation,
    m: &Member,
    objects: &[Vec<u8>],
) -> Result<tickwire_runtime::node::HandoverReadiness> {
    let release = bundle.verify_operation(op, m)?;
    let index = ObjectIndex::new(objects)?;
    let s =
        tickwire_recovery::HandoverSeal::decode(index.get(release.migration.source_proof.digest)?)?;
    Ok(tickwire_runtime::node::HandoverReadiness {
        instance_id: s.instance,
        strategy_generation: s.generation,
        checkpoint_digest: s.checkpoint_digest,
        exec_report_sequence: s.report_position,
        orders_digest: s.orders_digest,
        ledger_sequence: s.ledger_sequence,
        input_seal_digest: release.migration.source_proof.digest,
    })
}
fn check_source<S: Strategy, B: Backend, A: Authority, E: Encoder>(
    r: &ControlRequest,
    strategy: &mut S,
    gateway: &AccountGateway<B, A, E>,
    bundle: &tickwire_release::ReleaseBundle,
    objects: &[Vec<u8>],
) -> Result<()> {
    let release = bundle.verify_operation(&r.operation, &r.member)?;
    // The fixed source callbacks must be the real retained account prefix, even
    // when the current account has progressed beyond this migration start.
    let index = ObjectIndex::new(objects)?;
    let bytes = index.get(release.migration.source_proof.digest)?;
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
    let actual = ObjectIndex::new(&actual)?;
    for h in source
        .roots()
        .into_iter()
        .chain([release.migration.source_proof.digest])
    {
        actual
            .get(h)
            .map_err(|_| invalid("old Host does not retain the fixed source closure"))?;
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
        index.get(source.checkpoint_digest)?,
        index.get(source.input_digest)?,
        index.get(source.account_input_digest)?,
    )
    .map_err(|_| invalid("fixed migration input closure"))?;
    inputs
        .matches_history(gateway.baseline(), &history)
        .map_err(|_| invalid("migration source account history differs"))?;
    Ok(())
}
/// Called only after quiesce. Initial built-ins require exactly unchanged
/// business state; generic merged-input tail replay is not silently approximated.
pub(super) fn unchanged<S: Strategy>(strategy: &mut S, v: &Verified) -> Result<()> {
    let m = &v.completion.member;
    let (ready, objects) = strategy
        .handover_checkpoint(m.old_instance, m.old_generation)
        .ok_or_else(|| invalid("old process did not seal current quiesced callback state"))?;
    let release = v.bundle.verify_operation(&v.completion.operation, m)?;
    let fixed_index = ObjectIndex::new(&v.objects)?;
    let current_index = ObjectIndex::new(&objects)?;
    let fixed = fixed_index.get(release.migration.source_checkpoint.digest)?;
    let current = current_index.get(ready.checkpoint_digest)?;
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
pub(super) fn requirements<'a>(
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
    #[test]
    fn repeated_qualification_lookups_hash_each_input_object_once() {
        let objects = vec![vec![1; 16384], vec![2; 10], vec![3; 1024]];
        let hashes = objects
            .iter()
            .map(|b| tickwire_recovery::digest(b))
            .collect::<Vec<_>>();
        HASHED_BYTES.with(|n| n.set(0));
        let index = ObjectIndex::new(&objects).unwrap();
        for _ in 0..4 {
            for (b, h) in objects.iter().zip(&hashes) {
                assert_eq!(index.get(*h).unwrap(), b);
            }
        }
        assert_eq!(
            HASHED_BYTES.with(|n| n.get()),
            objects.iter().map(Vec::len).sum::<usize>()
        );
    }
    #[test]
    fn indexed_objects_preserve_conflicts_missing_lengths_and_bounds() {
        let objects = vec![b"actual".to_vec(), b"actual".to_vec()];
        let hash = tickwire_recovery::digest(b"actual");
        let index = ObjectIndex::new(&objects).unwrap();
        assert_eq!(index.get(hash).unwrap(), b"actual");
        assert!(index.get([0; 32]).is_err());
        let mut expected = tickwire_release::Object::of(b"actual");
        expected.bytes += 1;
        assert!(expected.verify(index.get(hash).unwrap(), 64).is_err());
        assert!(ObjectIndex::new(&[vec![]]).is_err());
        assert!(ObjectIndex::new(&vec![vec![1]; 513]).is_err());
        let mut map = BTreeMap::new();
        insert_object(&mut map, hash, b"actual").unwrap();
        assert!(insert_object(&mut map, hash, b"different").is_err());
    }
    #[test]
    #[ignore = "requires retained actual schema3 volume via TICKWIRE_QUALIFY_FIXTURE"]
    fn retained_schema3_closure_keeps_all_checks_without_rehashing_lookup_prefixes() {
        use tickwire_ipc::HandoverReadyBody;
        use tickwire_release::replay::startup;
        let root = std::path::PathBuf::from(
            std::env::var_os("TICKWIRE_QUALIFY_FIXTURE").expect("fixture"),
        );
        let value: serde_json::Value = serde_json::from_slice(
            &std::fs::read(
                root.join("next/reports/CONSUMED-06060606060606060606060606060606-6.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let hash: [u8; 32] = serde_json::from_value(value["input_seal_digest"].clone()).unwrap();
        let name = hash.iter().map(|v| format!("{v:02x}")).collect::<String>();
        let directory = root.join("next/recovery");
        let seal_bytes = std::fs::read(directory.join(format!("{name}.bin"))).unwrap();
        let seal = tickwire_recovery::HandoverSeal::decode(&seal_bytes).unwrap();
        let body = HandoverReadyBody {
            instance_id: seal.instance,
            logical_strategy_id: seal.logical_strategy,
            strategy_generation: seal.generation,
            exec_report_sequence: seal.report_position,
            ledger_sequence: seal.ledger_sequence,
            orders_digest: seal.orders_digest,
            checkpoint_digest: seal.checkpoint_digest,
            checkpoint_bytes: seal.checkpoint_bytes,
            input_seal_digest: hash,
            input_seal_bytes: seal_bytes.len() as u64,
            ready_sequence: value["ready_sequence"].as_u64().unwrap(),
            report_ring_sequence: value["report_sequence"].as_u64().unwrap(),
        };
        let (_, objects) = tickwire_strategy_sdk::handover::objects(&directory, &body).unwrap();
        let total = objects.iter().map(Vec::len).sum::<usize>();
        HASHED_BYTES.with(|n| n.set(0));
        let now = std::time::Instant::now();
        let index = ObjectIndex::new(&objects).unwrap();
        let get = |h| {
            index
                .get(h)
                .map(Vec::from)
                .map_err(|_| tickwire_release::Error::Missing)
        };
        startup::verify_seal(
            &seal,
            index.get(seal.checkpoint_digest).unwrap(),
            index.get(seal.input_digest).unwrap(),
            get,
        )
        .unwrap();
        let qualified = startup::verify(seal.replay_restore_digest.unwrap(), get).unwrap();
        assert_eq!(qualified.completion.member.new_instance, seal.instance);
        assert_eq!(qualified.completion.member.new_generation, seal.generation);
        let bytes = HASHED_BYTES.with(|n| n.get());
        eprintln!(
            "actual closure: {} objects / {total} bytes; lookup hashed {bytes} bytes; qualification {:?}",
            objects.len(),
            now.elapsed()
        );
        assert_eq!(
            bytes, total,
            "all content remains qualified; lookup must not rehash preceding objects"
        );
        // Both the previous linear reader and the indexed reader retain the
        // complete shared validators. A real binary corruption and an omitted
        // account input must reject with either reader.
        let largest = objects.iter().max_by_key(|b| b.len()).unwrap();
        let binary = tickwire_recovery::digest(largest);
        let member = qualified
            .bundle
            .verify_operation(
                &qualified.completion.operation,
                &qualified.completion.member,
            )
            .unwrap();
        let source_raw = index.get(member.migration.source_proof.digest).unwrap();
        let source = tickwire_recovery::HandoverSeal::decode(source_raw).unwrap();
        for bad_kind in 0..2 {
            let mut bad = objects.clone();
            if bad_kind == 0 {
                for b in &mut bad {
                    if tickwire_recovery::digest(b) == binary {
                        *b.last_mut().unwrap() ^= 1;
                    }
                }
            } else {
                bad.retain(|b| tickwire_recovery::digest(b) != source.account_input_digest);
            }
            let indexed = ObjectIndex::new(&bad).unwrap();
            let cached = |h| {
                indexed
                    .get(h)
                    .map(Vec::from)
                    .map_err(|_| tickwire_release::Error::Missing)
            };
            let legacy = |h| {
                bad.iter()
                    .find(|b| tickwire_recovery::digest(b) == h)
                    .cloned()
                    .ok_or(tickwire_release::Error::Missing)
            };
            assert!(
                startup::verify(seal.replay_restore_digest.unwrap(), cached).is_err(),
                "indexed bad case {bad_kind}"
            );
            assert!(
                startup::verify(seal.replay_restore_digest.unwrap(), legacy).is_err(),
                "legacy bad case {bad_kind}"
            );
        }
        eprintln!(
            "actual corrupt binary / missing source account: legacy and indexed validators both rejected"
        );
    }
}
