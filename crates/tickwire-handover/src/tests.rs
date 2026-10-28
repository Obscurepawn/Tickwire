use super::*;
use tickwire_account::{AssetAmount, RecoveryRecord};
use tickwire_recovery::{InputRecord, encode_inputs};

#[path = "../../tickwire-account-codec/tests/common/mod.rs"]
mod fixture;

struct Bundle {
    seal: HandoverSeal,
    checkpoint: Vec<u8>,
    inputs: Vec<u8>,
    account_input: Vec<u8>,
    baseline: CanonicalBaseline,
    record: CanonicalRecord,
}
fn callback(tag: u8, position: u64, bytes: &[u8]) -> Vec<u8> {
    let mut out = b"TWAI0001".to_vec();
    out.push(tag);
    out.extend(70u64.to_le_bytes());
    out.extend(position.to_le_bytes());
    out.extend((bytes.len() as u32).to_le_bytes());
    out.extend(bytes);
    out
}
impl Bundle {
    fn new(amount: i128) -> Self {
        let (snapshot, catalog, _) = fixture::fixture();
        let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
        let record = CanonicalRecord::new(
            baseline.digest(),
            1,
            RecoveryRecord::UnattributedCashFlow {
                id: fixture::venue::text("cash-1"),
                delta: AssetAmount {
                    key: fixture::key(2),
                    amount: fixture::units(amount),
                },
            },
        )
        .unwrap();
        let account = Account::recover(
            baseline.snapshot().clone(),
            baseline.catalog().clone(),
            &[record.record().clone()],
        )
        .unwrap();
        let mut account_seal = InputSeal::new(baseline.digest()).unwrap();
        account_seal.apply(&record).unwrap();
        let mut b = Self {
            seal: HandoverSeal {
                preflight_receipt_digest: None,
                replay_installation_digest: None,
                replay_restore_digest: None,
                replay_rollback_digest: None,
                rebuild_installation_digest: None,
                rebuild_rollback_digest: None,
                version: 1,
                owner: [1; 16],
                instance: [1; 16],
                generation: 2,
                logical_strategy: 8,
                report_position: 1,
                ledger_sequence: account.ledger_sequence(),
                orders_digest: account.strategy_orders_digest(8),
                checkpoint_digest: [1; 32],
                checkpoint_bytes: 97,
                account_input_digest: account_seal.digest(),
                account_input_bytes: account_seal.object_bytes().len() as u64,
                input_digest: [2; 32],
                input_bytes: 1,
                dependencies: (2..=11).map(|kind| (kind, [kind as u8; 32])).collect(),
            },
            checkpoint: vec![],
            inputs: vec![],
            account_input: account_seal.object_bytes().to_vec(),
            baseline,
            record,
        };
        b.recheckpoint();
        b.rows(vec![
            callback(0, 0, b.baseline.bytes()),
            callback(1, 1, b.record.bytes()),
        ]);
        b
    }
    fn recheckpoint(&mut self) {
        let mut out = b"TWHS0001".to_vec();
        out.extend(self.seal.instance);
        for v in [
            self.seal.generation,
            self.seal.logical_strategy,
            self.seal.report_position,
            self.seal.ledger_sequence,
        ] {
            out.extend(v.to_le_bytes());
        }
        out.extend(self.seal.orders_digest);
        out.extend(1u64.to_le_bytes());
        out.push(1);
        self.seal.checkpoint_digest = digest(&out);
        self.seal.checkpoint_bytes = out.len() as u64;
        self.checkpoint = out;
    }
    fn rows(&mut self, rows: Vec<Vec<u8>>) {
        let borrowed = rows
            .iter()
            .map(|b| InputRecord {
                kind: 2,
                payload: b,
            })
            .collect::<Vec<_>>();
        self.inputs = encode_inputs([3; 16], 1, [0; 32], &borrowed).unwrap().bytes;
        self.seal.input_digest = digest(&self.inputs);
        self.seal.input_bytes = self.inputs.len() as u64;
    }
    fn check(&self) -> Result<VerifiedHandoverInputs, Error> {
        verify(
            &self.seal,
            &self.checkpoint,
            &self.inputs,
            &self.account_input,
        )
    }
}
#[test]
fn complete_callbacks_replay_and_match_the_actual_history() {
    let b = Bundle::new(7);
    let v = b.check().unwrap();
    v.matches_history(&b.baseline, std::slice::from_ref(&b.record))
        .unwrap();
    assert_eq!(v.records().len(), 1);
}
#[test]
fn missing_duplicate_reordered_or_unwrapped_callbacks_are_rejected() {
    let base = Bundle::new(7);
    let b = callback(0, 0, base.baseline.bytes());
    let r = callback(1, 1, base.record.bytes());
    for rows in [
        vec![],
        vec![b.clone()],
        vec![r.clone()],
        vec![r.clone(), b.clone()],
        vec![b.clone(), b.clone(), r.clone()],
        vec![b.clone(), r.clone(), r.clone()],
        vec![base.baseline.bytes().to_vec(), r],
    ] {
        let mut changed = Bundle::new(7);
        changed.rows(rows);
        assert!(changed.check().is_err());
    }
}
#[test]
fn callback_sequence_and_canonical_baseline_are_both_bound() {
    let mut b = Bundle::new(7);
    b.rows(vec![
        callback(0, 0, b.baseline.bytes()),
        callback(1, 2, b.record.bytes()),
    ]);
    assert!(matches!(b.check(), Err(Error::Sequence)));
    let foreign = CanonicalRecord::new([9; 32], 1, b.record.record().clone()).unwrap();
    b.rows(vec![
        callback(0, 0, b.baseline.bytes()),
        callback(1, 1, foreign.bytes()),
    ]);
    assert!(matches!(b.check(), Err(Error::AccountSeal)));
}
#[test]
fn claimed_ledger_or_orders_must_match_replay_even_after_rehashing() {
    let mut b = Bundle::new(7);
    b.seal.ledger_sequence += 1;
    b.recheckpoint();
    assert!(matches!(b.check(), Err(Error::State)));
    let mut b = Bundle::new(7);
    b.seal.orders_digest = [8; 32];
    b.recheckpoint();
    assert!(matches!(b.check(), Err(Error::State)));
}
#[test]
fn self_consistent_alternate_history_cannot_replace_actual_wal_bytes() {
    let actual = Bundle::new(7);
    let changed = Bundle::new(8);
    let verified = changed.check().unwrap();
    assert_eq!(
        verified.matches_history(&actual.baseline, &[actual.record]),
        Err(Error::History)
    );
}
#[test]
fn inputs_require_genesis_and_exact_seal_object() {
    let mut b = Bundle::new(7);
    let body = callback(0, 0, b.baseline.bytes());
    b.inputs = encode_inputs(
        [3; 16],
        2,
        [1; 32],
        &[InputRecord {
            kind: 2,
            payload: &body,
        }],
    )
    .unwrap()
    .bytes;
    b.seal.input_digest = digest(&b.inputs);
    b.seal.input_bytes = b.inputs.len() as u64;
    assert!(matches!(b.check(), Err(Error::Inputs)));
    let mut b = Bundle::new(7);
    b.account_input[0] ^= 1;
    assert!(matches!(b.check(), Err(Error::Inputs)));
}
