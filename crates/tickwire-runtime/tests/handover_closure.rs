#[path = "../../tickwire-account-codec/tests/common/mod.rs"]
mod common;
use tickwire_account::{Account, ManagerIdentity, RecoveryRecord, StrategyHandover};
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord, InputSeal};
use tickwire_recovery::{
    BlobStore, HandoverSeal, InputRecord, MockBlobStore, Persistence, StoreKey, encode_inputs,
};
use tickwire_runtime::authority::verify_handover_objects;

fn callback(tag: u8, position: u64, bytes: &[u8]) -> Vec<u8> {
    let mut out = b"TWAI0001".to_vec();
    out.push(tag);
    out.extend(70u64.to_le_bytes());
    out.extend(position.to_le_bytes());
    out.extend((bytes.len() as u32).to_le_bytes());
    out.extend(bytes);
    out
}

fn fixture() -> (
    Persistence<MockBlobStore>,
    CanonicalBaseline,
    CanonicalRecord,
    HandoverSeal,
) {
    let (snapshot, catalog, command) = common::fixture();
    let account = Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
    let mut store = Persistence::new(MockBlobStore::default());
    let callback = callback(0, 0, baseline.bytes());
    let input = encode_inputs(
        [10; 16],
        1,
        [0; 32],
        &[InputRecord {
            kind: 2,
            payload: &callback,
        }],
    )
    .unwrap();
    let account_input = InputSeal::new(baseline.digest()).unwrap();
    let new = ManagerIdentity {
        instance: [71; 16],
        generation: command.context.request.strategy_generation + 1,
    };
    let logical = command.context.strategy.logical_strategy_id;
    let orders = account.strategy_orders_digest(logical);
    let mut checkpoint = b"TWHS0001".to_vec();
    checkpoint.extend(new.instance);
    checkpoint.extend(new.generation.to_le_bytes());
    checkpoint.extend(logical.to_le_bytes());
    checkpoint.extend(0u64.to_le_bytes());
    checkpoint.extend(account.ledger_sequence().to_le_bytes());
    checkpoint.extend(orders);
    checkpoint.extend(8u64.to_le_bytes());
    checkpoint.extend(0u64.to_le_bytes());
    let dependency = store.put_object(baseline.bytes()).unwrap();
    let seal = HandoverSeal {
        preflight_receipt_digest: None,
            replay_installation_digest: None,
            replay_restore_digest: None, replay_rollback_digest: None,
        version: 1,
        owner: new.instance,
        instance: new.instance,
        generation: new.generation,
        logical_strategy: logical,
        report_position: 0,
        ledger_sequence: account.ledger_sequence(),
        orders_digest: orders,
        checkpoint_digest: store.put_object(&checkpoint).unwrap(),
        checkpoint_bytes: checkpoint.len() as u64,
        account_input_digest: store.put_object(account_input.object_bytes()).unwrap(),
        account_input_bytes: account_input.object_bytes().len() as u64,
        input_digest: store.put_object(&input.bytes).unwrap(),
        input_bytes: input.bytes.len() as u64,
        dependencies: (2..=11).map(|kind| (kind, dependency)).collect(),
    };
    let handover = StrategyHandover {
        scope: baseline.snapshot().scope.clone(),
        account_epoch: baseline.snapshot().account_epoch,
        writer_epoch: command.context.request.intent_exec_epoch,
        operation_id: [70; 16],
        operation_revision: 1,
        logical_strategy_id: logical,
        expected_ledger_sequence: account.ledger_sequence(),
        expected_orders_digest: orders,
        from: ManagerIdentity {
            instance: command.context.strategy.instance_id,
            generation: command.context.request.strategy_generation,
        },
        to: new,
        checkpoint_digest: seal.checkpoint_digest,
        input_seal_digest: store.put_object(&seal.bytes().unwrap()).unwrap(),
        evaluated_at: 70,
        valid_until: 100,
    };
    let record = CanonicalRecord::new(
        baseline.digest(),
        1,
        RecoveryRecord::StrategyHandover(Box::new(handover)),
    )
    .unwrap();
    (store, baseline, record, seal)
}
#[test]
fn restored_handover_requires_the_exact_strategy_and_account_cut() {
    let (store, baseline, record, _) = fixture();
    verify_handover_objects(&store, &baseline, &record, &[]).unwrap();
    let RecoveryRecord::StrategyHandover(original) = record.record() else {
        panic!()
    };
    let mut changed = original.as_ref().clone();
    changed.to.instance = [72; 16];
    let changed = CanonicalRecord::new(
        baseline.digest(),
        1,
        RecoveryRecord::StrategyHandover(Box::new(changed)),
    )
    .unwrap();
    assert!(verify_handover_objects(&store, &baseline, &changed, &[]).is_err());
    assert!(
        verify_handover_objects(&store, &baseline, &record, std::slice::from_ref(&record)).is_err()
    );
}
struct Missing {
    inner: MockBlobStore,
    key: StoreKey,
}
impl BlobStore for Missing {
    fn get(&self, key: StoreKey, limit: usize) -> tickwire_recovery::Result<Vec<u8>> {
        if key == self.key {
            return Err(tickwire_recovery::Error::Missing);
        }
        self.inner.get(key, limit)
    }
    fn put_sync(&mut self, key: StoreKey, bytes: &[u8]) -> tickwire_recovery::Result<()> {
        self.inner.put_sync(key, bytes)
    }
}
#[test]
fn durable_account_record_cannot_replace_a_missing_strategy_dependency() {
    let (store, baseline, record, seal) = fixture();
    let missing = Persistence::new(Missing {
        inner: store.into_store(),
        key: StoreKey::Object(seal.input_digest),
    });
    assert!(verify_handover_objects(&missing, &baseline, &record, &[]).is_err());
}

fn rebind(
    store: &mut Persistence<MockBlobStore>,
    baseline: &CanonicalBaseline,
    original: &CanonicalRecord,
    seal: &HandoverSeal,
) -> CanonicalRecord {
    let RecoveryRecord::StrategyHandover(h) = original.record() else {
        panic!()
    };
    let mut h = h.as_ref().clone();
    h.input_seal_digest = store.put_object(&seal.bytes().unwrap()).unwrap();
    h.checkpoint_digest = seal.checkpoint_digest;
    h.expected_ledger_sequence = seal.ledger_sequence;
    h.expected_orders_digest = seal.orders_digest;
    CanonicalRecord::new(
        baseline.digest(),
        seal.report_position + 1,
        RecoveryRecord::StrategyHandover(Box::new(h)),
    )
    .unwrap()
}

#[test]
fn recomputed_input_hash_cannot_hide_the_baseline_callback() {
    let (mut store, baseline, record, mut seal) = fixture();
    verify_handover_objects(&store, &baseline, &record, &[]).unwrap();
    let without_account = encode_inputs([10; 16], 1, [0; 32], &[]).unwrap();
    seal.input_digest = store.put_object(&without_account.bytes).unwrap();
    seal.input_bytes = without_account.bytes.len() as u64;
    let changed = rebind(&mut store, &baseline, &record, &seal);
    assert!(
        verify_handover_objects(&store, &baseline, &changed, &[]).is_err(),
        "missing actual TWAI baseline must be rejected after all object hashes are recomputed"
    );
}

#[test]
fn recomputed_input_hash_cannot_hide_a_durable_account_callback() {
    let (mut store, baseline, record, mut seal) = fixture();
    let flow = CanonicalRecord::new(
        baseline.digest(),
        1,
        RecoveryRecord::UnattributedCashFlow {
            id: common::venue::text("cash-1"),
            delta: tickwire_account::AssetAmount {
                key: common::key(2),
                amount: common::units(7),
            },
        },
    )
    .unwrap();
    let account = Account::recover(
        baseline.snapshot().clone(),
        baseline.catalog().clone(),
        &[flow.record().clone()],
    )
    .unwrap();
    let mut account_input = InputSeal::new(baseline.digest()).unwrap();
    account_input.apply(&flow).unwrap();
    seal.account_input_digest = store.put_object(account_input.object_bytes()).unwrap();
    seal.account_input_bytes = account_input.object_bytes().len() as u64;
    seal.report_position = 1;
    seal.ledger_sequence = account.ledger_sequence();
    seal.orders_digest = account.strategy_orders_digest(seal.logical_strategy);
    let mut checkpoint = store.read_object(seal.checkpoint_digest, 65536).unwrap();
    checkpoint[40..48].copy_from_slice(&seal.report_position.to_le_bytes());
    checkpoint[48..56].copy_from_slice(&seal.ledger_sequence.to_le_bytes());
    checkpoint[56..88].copy_from_slice(&seal.orders_digest);
    seal.checkpoint_digest = store.put_object(&checkpoint).unwrap();
    let base = callback(0, 0, baseline.bytes());
    let row = callback(1, 1, flow.bytes());
    let inputs = encode_inputs(
        [10; 16],
        1,
        [0; 32],
        &[
            InputRecord {
                kind: 2,
                payload: &base,
            },
            InputRecord {
                kind: 2,
                payload: &row,
            },
        ],
    )
    .unwrap();
    seal.input_digest = store.put_object(&inputs.bytes).unwrap();
    seal.input_bytes = inputs.bytes.len() as u64;
    let valid = rebind(&mut store, &baseline, &record, &seal);
    verify_handover_objects(&store, &baseline, &valid, std::slice::from_ref(&flow)).unwrap();
    let incomplete = encode_inputs(
        [10; 16],
        1,
        [0; 32],
        &[InputRecord {
            kind: 2,
            payload: &base,
        }],
    )
    .unwrap();
    seal.input_digest = store.put_object(&incomplete.bytes).unwrap();
    seal.input_bytes = incomplete.bytes.len() as u64;
    let changed = rebind(&mut store, &baseline, &record, &seal);
    assert!(
        verify_handover_objects(&store, &baseline, &changed, &[flow]).is_err(),
        "a matching standalone InputSeal cannot replace the missing callback bytes"
    );
}
