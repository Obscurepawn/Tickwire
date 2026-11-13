mod common;

use common::*;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tickwire_account::{Account, IntentKey, OrderKey, RecoveryRecord};
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord};
use tickwire_account_journal::{
    AccountJournal, Config, Error, HistoryFrontier, InMemoryBackend, MAX_RECORDS, MmapAsyncBackend,
};
use tickwire_venue as v;

fn temp_dir(tag: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "tickwire-acct-journal-evict-{tag}-{}-{n}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.parent().unwrap()).unwrap();
    dir
}

#[test]
fn default_journal_keeps_none_retention_window() {
    let (s, c, _) = fixture();
    let baseline = CanonicalBaseline::new(s, c).unwrap();
    let config = Config {
        physical: tickwire_journal::Config::new([2; 16], [1; 16]),
        baseline_digest: baseline.digest(),
        initial_writer_epoch: 4,
        scope: baseline.snapshot().scope.clone(),
    };
    let journal = AccountJournal::create(InMemoryBackend::new(), config, baseline).unwrap();
    assert_eq!(journal.retention_window_ns(), None);
    assert_eq!(journal.evicted_prefix(), 0);
}

#[test]
fn windowed_compaction_exceeds_max_records_and_preserves_frontier_and_active_admissions() {
    let (mut snapshot, mut catalog, base_cmd) = fixture();
    snapshot.valid_until = u64::MAX / 2;
    snapshot.assets[0].free = units(1);
    snapshot.assets[1].free = units(1_000_000_000);
    catalog.entries[0].max_absolute_position = units(100_000);
    catalog.entries[0].max_gross_quantity = units(100_000);
    catalog.entries[0].strategy_limits = vec![(8, units(100_000))];
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
    let config = Config {
        physical: tickwire_journal::Config::new([2; 16], [1; 16]),
        baseline_digest: baseline.digest(),
        initial_writer_epoch: 4,
        scope: baseline.snapshot().scope.clone(),
    };

    let mut account =
        Account::from_snapshot(baseline.snapshot().clone(), baseline.catalog().clone())
            .unwrap()
            .with_retention_window_ns(Some(1_000));

    let dir = temp_dir("mmap");
    let mut mem_journal =
        AccountJournal::create(InMemoryBackend::new(), config.clone(), baseline.clone())
            .unwrap()
            .with_retention_window_ns(Some(1_000));
    let mut mmap_journal = AccountJournal::create(
        MmapAsyncBackend::open(&dir).unwrap(),
        config.clone(),
        baseline.clone(),
    )
    .unwrap()
    .with_retention_window_ns(Some(1_000));

    // Order #1 rests open at WAL sequence 1 and never closes throughout > 18,000 subsequent records.
    let mut resting_cmd = base_cmd.clone();
    resting_cmd.context.request.intent_id = 1;
    resting_cmd.context.request.deadline = 100_000;
    if let v::CommandBody::New(ref mut s) = resting_cmd.body {
        s.client_id = venue::text("resting-1");
    }
    let resting_canonical = canonical(resting_cmd);
    let resting_adm = account.admit(&resting_canonical, 1_000).unwrap();
    let resting_record = RecoveryRecord::Admit {
        command: Box::new(resting_canonical.clone()),
        evaluated_at: 1_000,
        expected: resting_adm.image(),
    };
    let r_mem_1 = mem_journal.append(resting_record.clone()).unwrap();
    let r_mmap_1 = mmap_journal.append(resting_record.clone()).unwrap();
    assert_eq!(r_mem_1.sequence(), 1);
    assert_eq!(r_mmap_1.sequence(), 1);

    // Stream 6,200 completed orders x 3 records (Admit + IngressEvent + FeesReconciled) = 18,600+ records (> MAX_RECORDS).
    let cycles = 6_200u64;
    let mut now = 5_000u64;
    let mut last_ingress_event = None;
    let mut last_ingress_seq = 0u64;

    for idx in 2..=(cycles + 1) {
        now += 100;
        let mut cmd = base_cmd.clone();
        cmd.context.request.intent_id = idx;
        cmd.context.request.deadline = now + 10_000;
        let cid = venue::text(&format!("c-{idx}"));
        if let v::CommandBody::New(ref mut spec) = cmd.body {
            spec.client_id = cid.clone();
            spec.quantity = v::Quantity::Base("0.001".parse().unwrap());
            spec.price = Some("60000".parse().unwrap());
        }
        let c_cmd = canonical(cmd.clone());
        let adm = account.admit(&c_cmd, now).unwrap();
        let admit_rec = RecoveryRecord::Admit {
            command: Box::new(c_cmd),
            evaluated_at: now,
            expected: adm.image(),
        };
        mem_journal.append(admit_rec.clone()).unwrap();
        mmap_journal.append(admit_rec).unwrap();

        let mut fill_ev = fill(&cmd, &format!("tr-{idx}"), "0.001", "60000", "60");
        fill_ev.identity.received_mono_ns = now + 10;
        if let v::VenueEventBody::Fill(ref mut b) = fill_ev.body {
            b.observation.status = v::OrderStatus::Filled;
            b.observation.cumulative_quantity = Some("0.001".parse().unwrap());
        }
        let c_ev = event(fill_ev);
        account.apply_event(&c_ev).unwrap();
        let ingress_rec = RecoveryRecord::IngressEvent {
            input_stream: [7; 16],
            input_sequence: idx,
            event: Box::new(c_ev.clone()),
        };
        mem_journal.append(ingress_rec.clone()).unwrap();
        mmap_journal.append(ingress_rec).unwrap();
        last_ingress_event = Some(c_ev);
        last_ingress_seq = idx;

        let order_key = OrderKey {
            intent: adm.intent(),
            client_id: cid,
        };
        account.reconcile_fees(&order_key, f(100_000)).unwrap();
        let fee_rec = RecoveryRecord::FeesReconciled {
            order: order_key,
            cumulative: f(100_000),
        };
        mem_journal.append(fee_rec.clone()).unwrap();
        mmap_journal.append(fee_rec).unwrap();
    }

    let expected_pos = 1 + cycles * 3;
    assert!(expected_pos > MAX_RECORDS as u64);
    assert_eq!(mem_journal.position(), expected_pos);
    assert_eq!(mmap_journal.position(), expected_pos);
    assert!(mem_journal.evicted_prefix() > 0);
    assert!(mmap_journal.evicted_prefix() > 0);
    assert!(mem_journal.records().len() < MAX_RECORDS);
    assert!(mmap_journal.records().len() < MAX_RECORDS);
    assert_eq!(
        mem_journal.history_frontier().digest(),
        mmap_journal.history_frontier().digest()
    );
    assert_eq!(mem_journal.history_frontier().record_count(), expected_pos);

    // Active resting order #1 (evicted from prefix slice) still deduplicates and retains AdmissionOrigin!
    let retry_receipt = mem_journal.append(resting_record).unwrap();
    assert_eq!(retry_receipt, r_mem_1);
    assert!(
        mem_journal
            .admission_origin(IntentKey {
                epoch: 4,
                intent_id: 1,
            })
            .is_some()
    );

    // Recent ingress key in the retained suffix remains accessible and deduplicated.
    assert_eq!(
        mem_journal.ingress_event([7; 16], last_ingress_seq),
        last_ingress_event.as_ref()
    );

    // Conflicting retry of active resting order #1 still poisons and returns Error::Conflict.
    let mut bad_cmd = base_cmd;
    bad_cmd.context.request.intent_id = 1;
    bad_cmd.context.request.deadline = 999_999;
    let bad_record = RecoveryRecord::Admit {
        command: Box::new(canonical(bad_cmd)),
        evaluated_at: 1_000,
        expected: resting_adm.image(),
    };
    assert!(matches!(
        mem_journal.append(bad_record),
        Err(Error::Conflict)
    ));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn history_frontier_matches_uncompacted_chain_across_manual_compaction() {
    let (mut snapshot, catalog, base_cmd) = fixture();
    snapshot.valid_until = u64::MAX / 2;
    snapshot.assets[1].free = units(10_000_000);
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
    let config = Config {
        physical: tickwire_journal::Config::new([2; 16], [1; 16]),
        baseline_digest: baseline.digest(),
        initial_writer_epoch: 4,
        scope: baseline.snapshot().scope.clone(),
    };

    let mut account =
        Account::from_snapshot(baseline.snapshot().clone(), baseline.catalog().clone()).unwrap();
    let mut compacted =
        AccountJournal::create(InMemoryBackend::new(), config.clone(), baseline.clone())
            .unwrap()
            .with_retention_window_ns(Some(100));
    let mut full: AccountJournal<InMemoryBackend> =
        AccountJournal::create(InMemoryBackend::new(), config, baseline).unwrap();

    for idx in 1..=40u64 {
        let mut cmd = base_cmd.clone();
        cmd.context.request.intent_id = idx;
        cmd.context.request.deadline = 10_000 + idx * 100;
        if let v::CommandBody::New(ref mut s) = cmd.body {
            s.client_id = venue::text(&format!("m-{idx}"));
        }
        let c_cmd = canonical(cmd);
        let adm = account.admit(&c_cmd, 1_000 + idx * 10).unwrap();
        let rec = RecoveryRecord::Admit {
            command: Box::new(c_cmd),
            evaluated_at: 1_000 + idx * 10,
            expected: adm.image(),
        };
        let r1 = compacted.append(rec.clone()).unwrap();
        let r2 = full.append(rec).unwrap();
        assert_eq!(r1.sequence(), r2.sequence());
        assert_eq!(r1.record_digest(), r2.record_digest());

        if idx % 10 == 0 {
            let keep = std::iter::once(IntentKey {
                epoch: 4,
                intent_id: 1,
            })
            .collect();
            compacted.compact_before(&keep, 4);
        }
    }

    let f_compacted: &HistoryFrontier = compacted.history_frontier();
    let f_full: &HistoryFrontier = full.history_frontier();
    assert_eq!(f_compacted.bytes(), f_full.bytes());
    assert_eq!(f_compacted.digest(), f_full.digest());
    assert!(compacted.evicted_prefix() > 0);
    assert_eq!(compacted.records().len(), 4);
    assert!(
        compacted
            .admission_origin(IntentKey {
                epoch: 4,
                intent_id: 1,
            })
            .is_some()
    );
    assert!(
        compacted
            .admission_origin(IntentKey {
                epoch: 4,
                intent_id: 2,
            })
            .is_none()
    );
    let _: &[CanonicalRecord] = compacted.records();
}
