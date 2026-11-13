mod common;
use common::*;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tickwire_account::{Account, RecoveryRecord};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_journal::{
    AccountBackend, AccountJournal, Config, InMemoryBackend, MmapAsyncBackend, RecoveryVerifier,
    VerificationError,
};
use tickwire_journal::StorageMode;

fn temp_dir(tag: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "tickwire-acct-journal-modes-{tag}-{}-{n}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.parent().unwrap()).unwrap();
    dir
}

struct ExactAdmissionVerifier {
    expected_digest: [u8; 32],
}
impl RecoveryVerifier for ExactAdmissionVerifier {
    fn verify_admission(
        &mut self,
        _baseline: &CanonicalBaseline,
        record: &tickwire_account_codec::CanonicalRecord,
        _prior_history: &tickwire_account_journal::HistoryFrontier,
    ) -> std::result::Result<(), VerificationError> {
        if record.digest() == self.expected_digest {
            Ok(())
        } else {
            Err(VerificationError::Invalid)
        }
    }
}

#[test]
fn in_memory_and_mmap_async_backends_append_deduplicate_and_recover() {
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
    let command = canonical(cmd);
    let admission = account.admit(&command, 60).unwrap();
    let record = RecoveryRecord::Admit {
        command: Box::new(command),
        evaluated_at: 60,
        expected: admission.image(),
    };

    // 1. Pure RAM default (`InMemoryBackend` & `AccountBackend::open(StorageMode::InMemory, None)`)
    let mut mem_journal = AccountJournal::create(
        AccountBackend::open(StorageMode::InMemory, None).unwrap(),
        config.clone(),
        baseline.clone(),
    )
    .unwrap();
    let r1 = mem_journal.append(record.clone()).unwrap();
    let r1_dup = mem_journal.append(record.clone()).unwrap();
    assert_eq!(r1, r1_dup);
    assert_eq!(r1.sequence(), 1);

    let mem_backend = InMemoryBackend::new();
    let mut mem2 = AccountJournal::create(mem_backend, config.clone(), baseline.clone()).unwrap();
    assert_eq!(mem2.append(record.clone()).unwrap().sequence(), 1);

    // 2. `MmapAsyncBackend` append -> drop -> reopen & recover
    let dir = temp_dir("mmap-acct");
    let rec_digest = {
        let backend = MmapAsyncBackend::open(&dir).unwrap();
        let mut mmap_journal = AccountJournal::create(backend, config.clone(), baseline).unwrap();
        let receipt = mmap_journal.append(record).unwrap();
        receipt.record_digest()
    };

    let mut verifier = ExactAdmissionVerifier {
        expected_digest: rec_digest,
    };
    let recovered = AccountJournal::recover(
        AccountBackend::open(StorageMode::MmapAsync, Some(&dir)).unwrap(),
        config,
        5,
        None,
        &mut verifier,
    )
    .unwrap();
    assert!(recovered.account.frozen());
    assert_eq!(recovered.journal.position(), 1);
    let _ = fs::remove_dir_all(&dir);
}
