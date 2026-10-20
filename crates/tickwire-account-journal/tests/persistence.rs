mod common;
use common::*;
use std::{
    fs, io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
};
use tickwire_account::{Account, AssetAmount, RecoveryRecord};
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord};
use tickwire_account_journal::*;
use tickwire_journal::{Journal, store::Store};
use tickwire_venue as v;

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static ID: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "tickwire-account-journal-{}-{}",
            std::process::id(),
            ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn backend(&self) -> FileBackend {
        FileBackend::open(self.0.join("wal")).unwrap()
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn setup() -> (CanonicalBaseline, Config, v::Command, RecoveryRecord) {
    let (s, c, cmd) = fixture();
    let baseline = CanonicalBaseline::new(s, c).unwrap();
    let config = Config {
        physical: tickwire_journal::Config::new([2; 16], [1; 16]),
        baseline_digest: baseline.digest(),
        initial_writer_epoch: 4,
        scope: baseline.snapshot().scope.clone(),
    };
    let mut account =
        Account::from_snapshot(baseline.snapshot().clone(), baseline.catalog().clone()).unwrap();
    let command = canonical(cmd.clone());
    let admission = account.admit(&command, 60).unwrap();
    let record = RecoveryRecord::Admit {
        command: Box::new(command),
        evaluated_at: 60,
        expected: admission.image(),
    };
    (baseline, config, cmd, record)
}
/// Test verifier checks the launcher's exact historical inputs; no permissive production verifier.
struct Verify {
    baseline: [u8; 32],
    expected: Vec<RecoveryRecord>,
    calls: usize,
    fail: bool,
}
impl Verify {
    fn new(b: &CanonicalBaseline, records: &[RecoveryRecord]) -> Self {
        Self {
            baseline: b.digest(),
            expected: records
                .iter()
                .filter(|r| matches!(r, RecoveryRecord::Admit { .. }))
                .cloned()
                .collect(),
            calls: 0,
            fail: false,
        }
    }
}
impl RecoveryVerifier for Verify {
    fn verify_admission(
        &mut self,
        b: &CanonicalBaseline,
        r: &CanonicalRecord,
        preceding: &HistoryFrontier,
    ) -> std::result::Result<(), VerificationError> {
        if self.fail {
            return Err(VerificationError::Missing);
        }
        if b.digest() != self.baseline
            || self.expected.get(self.calls) != Some(r.record())
            || preceding.record_count() + 1 != r.sequence()
        {
            return Err(VerificationError::Invalid);
        }
        self.calls += 1;
        Ok(())
    }
}
#[test]
fn file_creation_publishes_bound_baseline_and_bootstrap_and_holds_writer_lock() {
    let t = Temp::new();
    let (b, c, _, _) = setup();
    let j = AccountJournal::create(t.backend(), c.clone(), b.clone()).unwrap();
    assert_eq!(j.position(), 0);
    assert_eq!(j.writer_epoch(), 4);
    assert_eq!(
        fs::read(t.0.join("wal/ACCOUNT-BASELINE.cbor")).unwrap(),
        b.bytes()
    );
    let bootstrap = fs::read(t.0.join("wal/ACCOUNT-BOOTSTRAP.json")).unwrap();
    assert!(!bootstrap.is_empty());
    assert!(FileBackend::open(t.0.join("wal")).is_err());
    drop(j);
    assert!(AccountJournal::create(t.backend(), c, b).is_err());
    assert_eq!(
        fs::read(t.0.join("wal/ACCOUNT-BOOTSTRAP.json")).unwrap(),
        bootstrap
    );
}
#[test]
fn admission_retry_is_exact_and_conflict_poison_does_not_advance() {
    let t = Temp::new();
    let (b, c, _, r) = setup();
    let mut j = AccountJournal::create(t.backend(), c, b).unwrap();
    let receipt = j.append(r.clone()).unwrap();
    assert_eq!(j.append(r.clone()).unwrap(), receipt);
    assert_eq!(j.position(), 1);
    let mut conflict = r;
    if let RecoveryRecord::Admit { evaluated_at, .. } = &mut conflict {
        *evaluated_at = 61
    }
    assert!(matches!(j.append(conflict), Err(Error::Conflict)));
    assert!(j.poisoned());
    assert_eq!(j.position(), 1);
    assert!(matches!(
        j.append(RecoveryRecord::TransferAsset {
            key: key(2),
            from: 0,
            to: 8,
            amount: f(1)
        }),
        Err(Error::Poisoned)
    ));
}
#[test]
fn replay_recomputes_admission_and_remains_frozen_with_no_new_admit() {
    let t = Temp::new();
    let (b, c, _, r) = setup();
    let mut j = AccountJournal::create(t.backend(), c.clone(), b.clone()).unwrap();
    let receipt = j.append(r.clone()).unwrap();
    assert_eq!(receipt.baseline_digest(), b.digest());
    assert_eq!(receipt.sequence(), 1);
    assert_eq!(
        receipt.record_digest(),
        CanonicalRecord::new(b.digest(), 1, r.clone())
            .unwrap()
            .digest()
    );
    drop(j);
    let mut verifier = Verify::new(&b, std::slice::from_ref(&r));
    let mut recovered = AccountJournal::recover(
        t.backend(),
        c,
        5,
        Some(receipt.physical_frontier()),
        &mut verifier,
    )
    .unwrap();
    assert_eq!(verifier.calls, 1);
    assert!(recovered.account.frozen());
    assert!(recovered.journal.recovered());
    assert!(matches!(recovered.journal.append(r), Err(Error::Recovered)));
    assert_eq!(recovered.journal.writer_epoch(), 5);
}
#[test]
fn historical_verifier_failure_and_forged_admission_image_stop_recovery() {
    for forged in [false, true] {
        let t = Temp::new();
        let (b, c, _, mut r) = setup();
        if forged && let RecoveryRecord::Admit { expected, .. } = &mut r {
            expected.resources[0].amount = expected.resources[0].amount.checked_add(f(1)).unwrap();
        }
        let mut j = AccountJournal::create(t.backend(), c.clone(), b.clone()).unwrap();
        j.append(r.clone()).unwrap();
        drop(j);
        let mut verify = Verify::new(&b, &[r]);
        verify.fail = !forged;
        let result = AccountJournal::recover(t.backend(), c, 5, None, &mut verify);
        if forged {
            assert!(matches!(result, Err(Error::Account(_))))
        } else {
            assert!(matches!(
                result,
                Err(Error::Verification(VerificationError::Missing))
            ))
        }
    }
}
#[test]
fn duplicate_fill_observation_is_preserved_and_wal_sequence_is_not_ledger_sequence() {
    let t = Temp::new();
    let (b, c, cmd, r) = setup();
    let mut j = AccountJournal::create(t.backend(), c.clone(), b.clone()).unwrap();
    j.append(r.clone()).unwrap();
    let fill = event(fill(&cmd, "trade", "0.0004", "50000", "20"));
    let mut live = Account::from_snapshot(b.snapshot().clone(), b.catalog().clone()).unwrap();
    let RecoveryRecord::Admit {
        command,
        evaluated_at,
        ..
    } = &r
    else {
        unreachable!()
    };
    live.admit(command, *evaluated_at).unwrap();
    for _ in 0..2 {
        j.append(RecoveryRecord::VenueEvent(Box::new(fill.clone())))
            .unwrap();
        live.apply_event(&fill).unwrap();
    }
    assert_eq!(j.position(), 3);
    assert_eq!(live.ledger_sequence(), 2);
    drop(j);
    let recovered =
        AccountJournal::recover(t.backend(), c, 5, None, &mut Verify::new(&b, &[r])).unwrap();
    assert_eq!(
        recovered.account.asset(key(2)).unwrap(),
        live.asset(key(2)).unwrap()
    );
    assert_eq!(recovered.journal.records().len(), 3);
}
#[test]
fn baseline_and_bootstrap_substitution_reject_before_replay() {
    for mode in 0..5 {
        let t = Temp::new();
        let (b, mut c, _, r) = setup();
        let mut j = AccountJournal::create(t.backend(), c.clone(), b.clone()).unwrap();
        j.append(r.clone()).unwrap();
        drop(j);
        match mode {
            0 => c.scope.account += 1,
            1 => c.baseline_digest = [9; 32],
            2 => c.initial_writer_epoch += 1,
            3 => fs::write(t.0.join("wal/ACCOUNT-BASELINE.cbor"), b"truncated").unwrap(),
            _ => {
                let p = t.0.join("wal/ACCOUNT-BOOTSTRAP.json");
                let mut raw = fs::read(&p).unwrap();
                raw.push(b' ');
                fs::write(p, raw).unwrap();
            }
        }
        let mut verify = Verify::new(&b, &[r]);
        assert!(AccountJournal::recover(t.backend(), c, 5, None, &mut verify).is_err());
        assert_eq!(verify.calls, 0);
    }
}
struct Fault {
    inner: FileBackend,
    mode: Arc<AtomicU8>,
}
impl Store for Fault {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.inner.read_manifest()
    }
    fn publish_manifest(&mut self, b: &[u8]) -> io::Result<()> {
        self.inner.publish_manifest(b)
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        self.inner.segment_ids()
    }
    fn read_segment(&mut self, id: u64, max: u64) -> io::Result<Vec<u8>> {
        self.inner.read_segment(id, max)
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        self.inner.create_segment(id)
    }
    fn append_segment(&mut self, id: u64, off: u64, b: &[u8]) -> io::Result<()> {
        if self.mode.load(Ordering::SeqCst) == 1 {
            self.mode.store(0, Ordering::SeqCst);
            self.inner.append_segment(id, off, &b[..b.len() / 2])?;
            return Err(io::Error::other("partial append"));
        }
        self.inner.append_segment(id, off, b)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        self.inner.sync_segment(id)?;
        if self.mode.swap(0, Ordering::SeqCst) == 2 {
            return Err(io::Error::other("sync completed but acknowledgement lost"));
        }
        Ok(())
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.inner.truncate_segment(id, len)
    }
}
impl Backend for Fault {
    fn read_bootstrap(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.inner.read_bootstrap()
    }
    fn publish_bootstrap(&mut self, b: &[u8]) -> io::Result<()> {
        if self.mode.load(Ordering::SeqCst) == 3 {
            return Err(io::Error::other("bootstrap publication failed"));
        }
        self.inner.publish_bootstrap(b)
    }
    fn read_baseline(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.inner.read_baseline()
    }
    fn publish_baseline(&mut self, b: &[u8]) -> io::Result<()> {
        if self.mode.load(Ordering::SeqCst) == 4 {
            self.inner.publish_baseline(&b[..b.len() / 2])?;
            return Err(io::Error::other("partial baseline persisted"));
        }
        self.inner.publish_baseline(b)
    }
}
#[test]
fn partial_write_and_lost_sync_reply_poison_then_recover_actual_bytes() {
    for failure in [1, 2] {
        let t = Temp::new();
        let (b, c, _, r) = setup();
        let mode = Arc::new(AtomicU8::new(0));
        let mut j = AccountJournal::create(
            Fault {
                inner: t.backend(),
                mode: mode.clone(),
            },
            c.clone(),
            b.clone(),
        )
        .unwrap();
        let before = j.history_frontier().clone();
        mode.store(failure, Ordering::SeqCst);
        assert!(j.append(r.clone()).is_err());
        assert!(j.poisoned());
        assert_eq!(j.position(), 0);
        assert_eq!(j.history_frontier(), &before);
        assert!(j.append(r.clone()).is_err());
        drop(j);
        let mut verify = Verify::new(&b, &[r]);
        let recovered = AccountJournal::recover(t.backend(), c, 5, None, &mut verify).unwrap();
        assert_eq!(recovered.journal.position(), u64::from(failure == 2));
        assert_eq!(verify.calls, usize::from(failure == 2));
        assert_eq!(recovered.discarded_tail_bytes > 0, failure == 1);
        assert!(recovered.account.frozen());
    }
}
#[test]
fn trusted_frontier_mismatch_rejects_without_truncating_partial_tail() {
    let t = Temp::new();
    let (b, c, _, r) = setup();
    let mut j = AccountJournal::create(t.backend(), c.clone(), b.clone()).unwrap();
    let receipt = j.append(r.clone()).unwrap();
    drop(j);
    let path = t.0.join("wal/segment-00000000000000000001.wal");
    let mut original = fs::read(&path).unwrap();
    // A real incomplete next frame. Failed required-prefix verification must not repair it.
    original.extend_from_within(..20);
    fs::write(&path, &original).unwrap();
    let mut required = receipt.physical_frontier().clone();
    required.seal_hash[0] ^= 1;
    assert!(
        AccountJournal::recover(
            t.backend(),
            c,
            5,
            Some(&required),
            &mut Verify::new(&b, &[r])
        )
        .is_err()
    );
    assert_eq!(fs::read(path).unwrap(), original);
}

#[test]
fn failed_creation_never_publishes_bootstrap_before_baseline_or_overwrites_partial_files() {
    for failure in [3, 4] {
        let t = Temp::new();
        let (b, c, _, _) = setup();
        let mode = Arc::new(AtomicU8::new(failure));
        assert!(
            AccountJournal::create(
                Fault {
                    inner: t.backend(),
                    mode
                },
                c.clone(),
                b.clone(),
            )
            .is_err()
        );
        let saved = fs::read(t.0.join("wal/ACCOUNT-BASELINE.cbor")).unwrap();
        assert_eq!(
            saved.len(),
            if failure == 3 {
                b.bytes().len()
            } else {
                b.bytes().len() / 2
            }
        );
        assert!(!t.0.join("wal/ACCOUNT-BOOTSTRAP.json").exists());
        assert!(!t.0.join("wal/MANIFEST.json").exists());
        assert!(AccountJournal::create(t.backend(), c.clone(), b.clone()).is_err());
        assert_eq!(
            fs::read(t.0.join("wal/ACCOUNT-BASELINE.cbor")).unwrap(),
            saved
        );
        assert!(
            AccountJournal::recover(t.backend(), c, 5, None, &mut Verify::new(&b, &[])).is_err()
        );
    }
}

#[test]
fn child_append_then_exit_without_destructors() {
    let Some(path) = std::env::var_os("TICKWIRE_ACCOUNT_JOURNAL_CHILD") else {
        return;
    };
    let (baseline, config, cmd, admit) = setup();
    let mut journal =
        AccountJournal::create(FileBackend::open(path).unwrap(), config, baseline).unwrap();
    journal.append(admit).unwrap();
    journal
        .append(RecoveryRecord::VenueEvent(Box::new(event(fill(
            &cmd,
            "child-fill",
            "0.0004",
            "50000",
            "20",
        )))))
        .unwrap();
    // No Rust destructors run: successful append already completed file synchronization.
    std::process::exit(73);
}

#[test]
fn independent_exec_process_reopens_only_synchronized_history_as_frozen() {
    let t = Temp::new();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_append_then_exit_without_destructors",
            "--nocapture",
        ])
        .env("TICKWIRE_ACCOUNT_JOURNAL_CHILD", t.0.join("wal"))
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    let (baseline, config, _, admit) = setup();
    let recovered = AccountJournal::recover(
        t.backend(),
        config,
        5,
        None,
        &mut Verify::new(&baseline, &[admit]),
    )
    .unwrap();
    assert_eq!(recovered.journal.position(), 2);
    assert!(recovered.account.frozen());
    assert_eq!(recovered.account.asset(key(2)).unwrap().total, units(980));
}
#[test]
fn forged_record_sequence_or_baseline_is_rejected() {
    // A complete physical batch can still have a false account envelope or writer binding.
    for mode in 0..5 {
        let t = Temp::new();
        let (b, c, _, r) = setup();
        drop(AccountJournal::create(t.backend(), c.clone(), b.clone()).unwrap());
        let (mut physical, _) = Journal::recover(t.backend(), None).unwrap();
        let record = CanonicalRecord::new(
            if mode == 1 { [9; 32] } else { b.digest() },
            if mode == 0 { 2 } else { 1 },
            r.clone(),
        )
        .unwrap();
        let writer = match mode {
            2 => 3,
            3 => 6,
            4 => 5,
            _ => 4,
        };
        physical.append_batch(writer, &[record.bytes()]).unwrap();
        drop(physical);
        let mut verify = Verify::new(&b, &[r]);
        assert!(AccountJournal::recover(t.backend(), c, 5, None, &mut verify).is_err());
        assert_eq!(verify.calls, 0);
    }
}
#[test]
fn writer_epoch_and_complete_event_identity_are_checked() {
    let t = Temp::new();
    let (b, c, cmd, r) = setup();
    let mut j = AccountJournal::create(t.backend(), c.clone(), b.clone()).unwrap();
    j.append(r.clone()).unwrap();
    let mut e = fill(&cmd, "future", "0.0004", "50000", "20");
    let v::VenueEventBody::Fill(ref mut f) = e.body else {
        unreachable!()
    };
    f.observation.order.original_intent_exec_epoch = 5;
    assert!(matches!(
        j.append(RecoveryRecord::VenueEvent(Box::new(event(e)))),
        Err(Error::Epoch)
    ));
    let mut e = fill(&cmd, "foreign", "0.0004", "50000", "20");
    e.identity.account += 1;
    assert!(matches!(
        j.append(RecoveryRecord::VenueEvent(Box::new(event(e)))),
        Err(Error::Identity)
    ));
    drop(j);
    assert!(matches!(
        AccountJournal::recover(
            t.backend(),
            c.clone(),
            3,
            None,
            &mut Verify::new(&b, std::slice::from_ref(&r))
        ),
        Err(Error::Epoch)
    ));
    let mut recovered = AccountJournal::recover(
        t.backend(),
        c.clone(),
        5,
        None,
        &mut Verify::new(&b, std::slice::from_ref(&r)),
    )
    .unwrap();
    recovered
        .journal
        .append(RecoveryRecord::VenueEvent(Box::new(event(fill(
            &cmd, "late", "0.0004", "50000", "20",
        )))))
        .unwrap();
    drop(recovered);
    assert!(matches!(
        AccountJournal::recover(t.backend(), c, 4, None, &mut Verify::new(&b, &[r])),
        Err(Error::Epoch)
    ));
}
#[test]
fn rejected_account_fact_remains_durable_and_recovery_does_not_skip_it() {
    let t = Temp::new();
    let (b, c, _, r) = setup();
    let mut j = AccountJournal::create(t.backend(), c.clone(), b.clone()).unwrap();
    j.append(r.clone()).unwrap();
    j.append(RecoveryRecord::ExternalPosition {
        id: venue::text("bad-position"),
        instrument: 999,
        side: v::PositionSide::Both,
        quantity: f(0),
        entry_bound: units(1),
    })
    .unwrap();
    drop(j);
    assert!(matches!(
        AccountJournal::recover(t.backend(), c, 5, None, &mut Verify::new(&b, &[r])),
        Err(Error::Account(_))
    ));
}
#[test]
fn all_eight_record_kinds_replay_in_one_real_file_order() {
    let t = Temp::new();
    let (s, catalog, cmd) = futures_fixture();
    let baseline = CanonicalBaseline::new(s, catalog).unwrap();
    let config = Config {
        physical: tickwire_journal::Config::new([2; 16], [1; 16]),
        baseline_digest: baseline.digest(),
        initial_writer_epoch: 4,
        scope: baseline.snapshot().scope.clone(),
    };
    let mut account =
        Account::from_snapshot(baseline.snapshot().clone(), baseline.catalog().clone()).unwrap();
    let mut records = Vec::new();
    let command = canonical(cmd.clone());
    let a = account.admit(&command, 60).unwrap();
    records.push(RecoveryRecord::Admit {
        command: Box::new(command),
        evaluated_at: 60,
        expected: a.image(),
    });
    let mut second = cmd.clone();
    second.context.request.intent_id += 1;
    let v::CommandBody::New(ref mut order) = second.body else {
        unreachable!()
    };
    order.client_id = venue::text("second");
    let command = canonical(second);
    let a2 = account.admit(&command, 60).unwrap();
    records.push(RecoveryRecord::Admit {
        command: Box::new(command.clone()),
        evaluated_at: 60,
        expected: a2.image(),
    });
    let mut fill = fill(&cmd, "complete", "1", "100", "100");
    let v::VenueEventBody::Fill(ref mut body) = fill.body else {
        unreachable!()
    };
    body.realized_pnl = Some(v::AssetDelta {
        asset: cmd.context.instrument.settlement.clone(),
        amount: venue::d(0, 0),
    });
    let fill = event(fill);
    account.apply_event(&fill).unwrap();
    records.push(RecoveryRecord::VenueEvent(Box::new(fill)));
    let term = event(terminal(&cmd, "1"));
    account.apply_event(&term).unwrap();
    records.push(RecoveryRecord::VenueEvent(Box::new(term)));
    account.reconcile_fees(&a.orders()[0], units(1)).unwrap();
    records.push(RecoveryRecord::FeesReconciled {
        order: a.orders()[0].clone(),
        cumulative: units(1),
    });
    account.apply_abort(&a2).unwrap();
    records.push(RecoveryRecord::Aborted {
        intent: a2.intent(),
        command_digest: command.digest(),
    });
    account
        .transfer_asset_attribution(key(2), 0, 8, units(1))
        .unwrap();
    records.push(RecoveryRecord::TransferAsset {
        key: key(2),
        from: 0,
        to: 8,
        amount: units(1),
    });
    account
        .transfer_position_attribution(12, v::PositionSide::Both, 8, 0, f(20_000_000))
        .unwrap();
    records.push(RecoveryRecord::TransferPosition {
        instrument: 12,
        side: v::PositionSide::Both,
        from: 8,
        to: 0,
        quantity: f(20_000_000),
    });
    let delta = AssetAmount {
        key: key(2),
        amount: f(-1),
    };
    let id = venue::text("cash");
    account.apply_unattributed_cashflow(&id, delta).unwrap();
    records.push(RecoveryRecord::UnattributedCashFlow { id, delta });
    let id = venue::text("position");
    account
        .apply_external_position(&id, 12, v::PositionSide::Both, units(2), units(100))
        .unwrap();
    records.push(RecoveryRecord::ExternalPosition {
        id,
        instrument: 12,
        side: v::PositionSide::Both,
        quantity: units(2),
        entry_bound: units(100),
    });
    let mut j = AccountJournal::create(t.backend(), config.clone(), baseline.clone()).unwrap();
    for r in &records {
        j.append(r.clone()).unwrap();
    }
    assert_eq!(j.position(), records.len() as u64);
    drop(j);
    let mut verify = Verify::new(&baseline, &records);
    let recovered = AccountJournal::recover(t.backend(), config, 5, None, &mut verify).unwrap();
    assert_eq!(verify.calls, 2);
    assert!(recovered.account.frozen());
    assert_eq!(
        recovered
            .journal
            .records()
            .iter()
            .map(|r| r.record().clone())
            .collect::<Vec<_>>(),
        records
    );
    assert_eq!(
        recovered.account.asset(key(2)).unwrap(),
        account.asset(key(2)).unwrap()
    );
    assert_eq!(
        recovered.account.position(12, v::PositionSide::Both),
        account.position(12, v::PositionSide::Both)
    );
    assert_eq!(
        recovered.account.venue_shadow(1).unwrap(),
        account.venue_shadow(1).unwrap()
    );
}

#[test]
fn history_binds_bootstrap_and_successful_record_order_and_recovery_preceding_prefix() {
    let t = Temp::new();
    let (b, c, cmd, r) = setup();
    let mut j = AccountJournal::create(t.backend(), c.clone(), b.clone()).unwrap();
    let empty = j.history_frontier().clone();
    assert_eq!(empty.record_count(), 0);
    assert_ne!(empty.chain_hash(), [0; 32]);
    assert_eq!(&empty.bytes()[..8], b"TWAF0001");
    assert_eq!(empty.scope(), &c.scope);
    assert_eq!(empty.stream(), c.physical.stream_epoch);
    assert_eq!(empty.baseline_digest(), b.digest());
    assert_eq!(empty.bytes().len(), 128);
    j.append(r.clone()).unwrap();
    let first = j.history_frontier().clone();
    assert_eq!(first.record_count(), 1);
    assert_ne!(first.digest(), empty.digest());
    j.append(r.clone()).unwrap();
    assert_eq!(j.history_frontier(), &first);
    j.append(RecoveryRecord::VenueEvent(Box::new(event(fill(
        &cmd, "history", "0.0004", "50000", "20",
    )))))
    .unwrap();
    let last = j.history_frontier().clone();
    assert_eq!(last.record_count(), 2);
    assert_ne!(last.digest(), first.digest());
    drop(j);
    struct Prefix(Verify, HistoryFrontier);
    impl RecoveryVerifier for Prefix {
        fn verify_admission(
            &mut self,
            b: &CanonicalBaseline,
            r: &CanonicalRecord,
            p: &HistoryFrontier,
        ) -> std::result::Result<(), VerificationError> {
            if p != &self.1 {
                return Err(VerificationError::Invalid);
            }
            self.0.verify_admission(b, r, p)
        }
    }
    let mut verify = Prefix(Verify::new(&b, &[r]), empty.clone());
    let recovered = AccountJournal::recover(t.backend(), c.clone(), 5, None, &mut verify).unwrap();
    assert_eq!(recovered.journal.history_frontier(), &last);
    let other = Temp::new();
    let mut config = c;
    config.physical.segment_max_bytes /= 2;
    let j = AccountJournal::create(other.backend(), config, b).unwrap();
    assert_ne!(j.history_frontier().digest(), empty.digest());
}

#[test]
fn history_digest_is_the_actual_recovery_object_content_address() {
    let t = Temp::new();
    let (baseline, config, _, admit) = setup();
    let mut journal = AccountJournal::create(t.backend(), config, baseline).unwrap();
    let mut objects =
        tickwire_recovery::Persistence::new(tickwire_recovery::MockBlobStore::default());
    for after_admit in [false, true] {
        if after_admit {
            journal.append(admit.clone()).unwrap();
        }
        let history = journal.history_frontier();
        let content_hash = objects.put_object(history.bytes()).unwrap();
        assert_eq!(history.digest(), content_hash);
    }
}
