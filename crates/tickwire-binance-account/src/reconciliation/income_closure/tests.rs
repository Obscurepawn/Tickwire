use super::super::income_import::tests::{Memory, approved_fixture, http};
use super::*;
use tickwire_clock::MonoTime;
use tickwire_recovery::{MockBlobStore, SharedBlobStore};
fn fixture() -> (
    AccountIngress<Memory>,
    Account,
    HistoryFrontier,
    ClosureRequest,
) {
    fixture_delta("-2", "998")
}
fn fixture_delta(
    delta: &str,
    total: &str,
) -> (
    AccountIngress<Memory>,
    Account,
    HistoryFrontier,
    ClosureRequest,
) {
    let (input, mut account, mut journal, domain) = approved_fixture();
    let mut input = input.with_closure_store(SharedBlobStore::new(MockBlobStore::default()));
    let query = income::IncomeQuery {
        start_time_ms: 101,
        end_time_ms: 110,
        limit: 2,
        private_connection_epoch: 7,
        gap_generation: 3,
        purpose: income::IncomeUse::Account,
    };
    let body=serde_json::to_vec(&serde_json::json!([{"symbol":"","incomeType":"FUNDING_FEE","income":delta,"asset":"USDT","time":105,"tranId":43,"tradeId":""}])).unwrap();
    let rows = income::bind(&input.config.baseline, query, &body).unwrap();
    let row = rows[0].clone();
    let leaf = input
        .append(Record::Income(income::IncomeRecord {
            query,
            epoch: 15,
            received: 20,
            clock_boot_id: domain.boot_id(),
            clock_generation: domain.generation(),
            status: 200,
            accepted: true,
            raw: http(&body),
            rows,
        }))
        .unwrap()
        .sequence;
    let effect = input
        .append(Record::IncomeEffect {
            response: leaf,
            index: 0,
        })
        .unwrap();
    account.apply_account_input(&row).unwrap();
    journal
        .append(tickwire_account::RecoveryRecord::IngressAccountInput {
            input_stream: input.input_stream(),
            input_sequence: effect.sequence,
            input: Box::new(row),
        })
        .unwrap();
    let history = journal.history_frontier().clone();
    let Record::Snapshot(original) = &input.records[3] else {
        panic!("snapshot")
    };
    let mut snap = original.clone();
    snap.ledger = account.ledger_sequence();
    snap.received = 21;
    snap.epoch = 16;
    let raw = String::from_utf8(snap.raw.clone()).unwrap();
    let body = raw.split("\r\n\r\n").nth(1).unwrap().replace("1000", total);
    snap.raw = http(body.as_bytes());
    let mut snapshots = [0; 3];
    for (i, kind) in [
        bootstrap::SnapshotKind::Account,
        bootstrap::SnapshotKind::OpenOrders,
        bootstrap::SnapshotKind::ConditionalOrders,
    ]
    .into_iter()
    .enumerate()
    {
        let mut s = snap.clone();
        s.query.kind = kind;
        s.epoch += i as u64;
        s.received += i as u64;
        if i > 0 {
            s.raw = http(b"[]")
        }
        snapshots[i] = input.append(Record::Snapshot(s)).unwrap().sequence;
    }
    input.cursor = input.records.len();
    let request = ClosureRequest {
        income_leaves: vec![leaf],
        snapshots,
        cut: ActivationCut {
            private_connection_epoch: 7,
            gap_generation: 3,
            through_sequence: input.position(),
            now: MonoTime::new(domain, 24),
            valid_until: MonoTime::new(domain, 100),
            writer_epoch: 4,
            execution_connection_epoch: 1,
        },
    };
    (input, account, history, request)
}
#[test]
fn real_query_closure_is_persisted_and_revalidated_at_its_original_cut() {
    let (input, mut account, history, request) = fixture();
    let before = account.asset(crate::unit_fixture::key(2)).unwrap();
    let proof = input
        .prepare_account_inputs_closure(&account, &history, request)
        .unwrap();
    input
        .verify_account_inputs_closure(
            &input.config.baseline,
            &account,
            &history,
            proof.binding(),
            4,
        )
        .unwrap();
    let bytes = input
        .account_inputs_coverage_object(proof.binding())
        .unwrap();
    assert_eq!(hash(&bytes), proof.binding().coverage_digest);
    account.reconcile_account_inputs(proof.binding()).unwrap();
    assert_eq!(account.unresolved_account_inputs(), 0);
    assert!(account.frozen());
    assert_eq!(account.asset(crate::unit_fixture::key(2)).unwrap(), before);
    let mut cfg = input.config.clone();
    cfg.writer_epoch = 5;
    let store = input.closure_store;
    let mut recovered = AccountIngress::recover(input.journal.into_store(), cfg, None).unwrap();
    recovered.closure_store = store;
    let (_, original, _, _) = fixture();
    recovered
        .verify_account_inputs_closure(
            &recovered.config.baseline,
            &original,
            &history,
            proof.binding(),
            4,
        )
        .unwrap();
    assert!(
        recovered
            .verify_account_inputs_closure(
                &recovered.config.baseline,
                &original,
                &history,
                proof.binding(),
                5
            )
            .is_err()
    );
}
#[test]
fn absent_objects_and_wrong_purpose_never_create_a_closure() {
    let (mut input, account, history, request) = fixture();
    input.closure_store = None;
    assert!(
        input
            .prepare_account_inputs_closure(&account, &history, request)
            .is_err()
    );
    let (mut input, account, history, request) = fixture();
    let Record::Income(r) = &mut input.records[request.income_leaves[0] as usize - 1] else {
        unreachable!()
    };
    r.query.purpose = income::IncomeUse::BaselineCoverage;
    assert!(
        input
            .prepare_account_inputs_closure(&account, &history, request)
            .is_err()
    );
}
#[test]
fn longer_exact_prefix_is_historical_only_and_changed_prefix_is_rejected() {
    use tickwire_account_gateway::ReactivationVerifier;
    let (mut input, account, history, request) = fixture();
    let proof = input
        .prepare_account_inputs_closure(&account, &history, request)
        .unwrap();
    let extra = input.records.last().unwrap().clone();
    input.append(extra).unwrap();
    input.cursor = input.records.len();
    input
        .verify_account_inputs_closure(
            &input.config.baseline,
            &account,
            &history,
            proof.binding(),
            4,
        )
        .unwrap();
    assert!(
        input
            .activation_verifier()
            .verify_account_inputs_closure_live(
                &input.config.baseline,
                &account,
                &history,
                proof.binding(),
                4
            )
            .is_err()
    );
    let Record::Snapshot(s) = &mut input.records[3] else {
        unreachable!()
    };
    s.received += 1;
    assert!(
        input
            .verify_account_inputs_closure(
                &input.config.baseline,
                &account,
                &history,
                proof.binding(),
                4
            )
            .is_err()
    );
}
#[test]
fn coverage_is_strict_cbor_and_all_snapshot_ledger_values_must_match() {
    let (input, account, history, request) = fixture();
    let proof = input
        .prepare_account_inputs_closure(&account, &history, request)
        .unwrap();
    let bytes = input
        .account_inputs_coverage_object(proof.binding())
        .unwrap();
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(Manifest::decode(&extra).is_err());
    let mut long = vec![0x99, 0, 24];
    long.extend(&bytes[2..]);
    assert!(Manifest::decode(&long).is_err());
    let (mut input, account, history, request) = fixture();
    let Record::Snapshot(s) = &mut input.records[request.snapshots[1] as usize - 1] else {
        unreachable!()
    };
    s.ledger -= 1;
    assert!(
        input
            .prepare_account_inputs_closure(&account, &history, request)
            .is_err()
    );
}
#[test]
fn real_file_input_account_and_coverage_reopen_and_missing_object_refuses_recovery() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use tickwire_account_codec::CanonicalRecord;
    use tickwire_account_gateway::ReactivationVerifier;
    use tickwire_account_journal::{
        AccountJournal, FileBackend, RecoveryVerifier, VerificationError,
    };
    use tickwire_journal::store::FileStore;
    use tickwire_recovery::FileBlobStore;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "tickwire-income-files-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let (memory, account, _, request) = fixture();
    let config = memory.config.clone();
    let shared = SharedBlobStore::new(FileBlobStore::open(root.join("objects")).unwrap());
    let mut input =
        AccountIngress::create(FileStore::open(root.join("input")).unwrap(), config.clone())
            .unwrap()
            .with_closure_store(shared.clone());
    for record in memory.records.iter().skip(1) {
        input.append(record.clone()).unwrap();
    }
    input.cursor = input.records.len();
    let cfg = tickwire_account_journal::Config {
        physical: tickwire_journal::Config::new(config.account_stream, config.physical.domain),
        baseline_digest: config.baseline.digest(),
        initial_writer_epoch: 4,
        scope: config.baseline.snapshot().scope.clone(),
    };
    let mut journal = AccountJournal::create(
        FileBackend::open(root.join("account")).unwrap(),
        cfg.clone(),
        config.baseline.clone(),
    )
    .unwrap();
    journal
        .append(tickwire_account::RecoveryRecord::IncomeBaseline(Box::new(
            account.income_baseline(1).unwrap().clone(),
        )))
        .unwrap();
    let (effect, row) = input.account_inputs().next().unwrap();
    journal
        .append(tickwire_account::RecoveryRecord::IngressAccountInput {
            input_stream: effect.stream,
            input_sequence: effect.sequence,
            input: Box::new(row.clone()),
        })
        .unwrap();
    let proof = input
        .prepare_account_inputs_closure(&account, journal.history_frontier(), request)
        .unwrap();
    let binding = proof.binding().clone();
    journal
        .append(tickwire_account::RecoveryRecord::AccountInputsReconciled(
            Box::new(binding.clone()),
        ))
        .unwrap();
    drop(journal);
    drop(input);
    drop(shared);
    struct Verify<'a>(&'a AccountIngress<FileStore>);
    impl RecoveryVerifier for Verify<'_> {
        fn verify_admission(
            &mut self,
            _: &CanonicalBaseline,
            _: &CanonicalRecord,
            _: &HistoryFrontier,
        ) -> std::result::Result<(), VerificationError> {
            Err(VerificationError::Invalid)
        }
        fn verify_income_baseline(
            &mut self,
            b: &CanonicalBaseline,
            r: &CanonicalRecord,
            h: &HistoryFrontier,
            prior: &[CanonicalRecord],
        ) -> std::result::Result<(), VerificationError> {
            let tickwire_account::RecoveryRecord::IncomeBaseline(v) = r.record() else {
                return Err(VerificationError::Invalid);
            };
            let a = Account::recover(
                b.snapshot().clone(),
                b.catalog().clone(),
                &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
            )
            .map_err(|_| VerificationError::Invalid)?;
            self.0
                .activation_verifier()
                .verify_income_baseline(b, &a, h, v)
                .map_err(|_| VerificationError::Invalid)
        }
        fn verify_account_inputs_closure(
            &mut self,
            b: &CanonicalBaseline,
            r: &CanonicalRecord,
            h: &HistoryFrontier,
            prior: &[CanonicalRecord],
            writer: u64,
        ) -> std::result::Result<(), VerificationError> {
            let tickwire_account::RecoveryRecord::AccountInputsReconciled(v) = r.record() else {
                return Err(VerificationError::Invalid);
            };
            let a = Account::recover(
                b.snapshot().clone(),
                b.catalog().clone(),
                &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
            )
            .map_err(|_| VerificationError::Invalid)?;
            self.0
                .activation_verifier()
                .verify_account_inputs_closure(b, &a, h, v, writer)
                .map_err(|_| VerificationError::Invalid)
        }
    }
    let mut new = config;
    new.writer_epoch = 5;
    let input = AccountIngress::recover(FileStore::open(root.join("input")).unwrap(), new, None)
        .unwrap()
        .with_closure_store(FileBlobStore::open(root.join("objects")).unwrap());
    let restored = AccountJournal::recover(
        FileBackend::open(root.join("account")).unwrap(),
        cfg.clone(),
        5,
        None,
        &mut Verify(&input),
    )
    .unwrap();
    assert_eq!(restored.account.ledger_sequence(), 3);
    assert_eq!(restored.account.unresolved_account_inputs(), 0);
    assert!(restored.account.frozen());
    assert_eq!(
        restored.account.asset(crate::unit_fixture::key(2)),
        account.asset(crate::unit_fixture::key(2))
    );
    drop(restored);
    let hex: String = binding
        .coverage_digest
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    std::fs::remove_file(root.join("objects").join(format!("object-{hex}"))).unwrap();
    assert!(
        AccountJournal::recover(
            FileBackend::open(root.join("account")).unwrap(),
            cfg,
            6,
            None,
            &mut Verify(&input)
        )
        .is_err()
    );
    drop(input);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn closure_ledger_requires_three_new_snapshots_before_activation() {
    use tickwire_account_gateway::ReactivationVerifier;
    let (mut input, mut account, history, request) = fixture();
    let proof = input
        .prepare_account_inputs_closure(&account, &history, request)
        .unwrap();
    account.reconcile_account_inputs(proof.binding()).unwrap();
    let make = |input: &AccountIngress<Memory>| {
        let evidence = activation::evidence_at(input, input.position()).unwrap();
        Reactivation {
            scope: input.config.baseline.snapshot().scope.clone(),
            product: product(&input.config).unwrap(),
            account_epoch: account.account_epoch(),
            ledger_sequence: account.ledger_sequence(),
            baseline_digest: input.config.baseline.digest(),
            history_digest: history.digest(),
            input_stream: input.input_stream(),
            private_connection_epoch: 7,
            gap_generation: 3,
            through_sequence: input.position(),
            query_coverage_digest: activation::digest(&evidence),
            clock_boot_id: [8; 16],
            clock_generation: 1,
            evaluated_at: 30,
            valid_until: 100,
            writer_epoch: 4,
            execution_connection_epoch: 1,
            venue_available: snapshot::venue_available(&input.config.baseline, evidence[0].body())
                .unwrap(),
        }
    };
    let stale = make(&input);
    assert!(
        input
            .activation_verifier()
            .verify(&input.config.baseline, &account, &history, &stale)
            .is_err()
    );
    let old: Vec<_> = input.records[input.records.len() - 3..].to_vec();
    for record in old {
        let Record::Snapshot(mut r) = record else {
            unreachable!()
        };
        r.ledger = account.ledger_sequence();
        r.received += 4;
        r.epoch += 4;
        input.append(Record::Snapshot(r)).unwrap();
    }
    input.cursor = input.records.len();
    let fresh = make(&input);
    input
        .activation_verifier()
        .verify(&input.config.baseline, &account, &history, &fresh)
        .unwrap();
}

#[test]
fn authenticated_positive_funding_closure_keeps_the_credit_hold() {
    let (input, mut account, history, request) = fixture_delta("2", "1002");
    let before = account.asset(crate::unit_fixture::key(2)).unwrap();
    let proof = input
        .prepare_account_inputs_closure(&account, &history, request)
        .unwrap();
    account.reconcile_account_inputs(proof.binding()).unwrap();
    assert_eq!(account.asset(crate::unit_fixture::key(2)).unwrap(), before);
    assert_eq!(account.unresolved_account_inputs(), 0);
    assert!(account.frozen());
}
