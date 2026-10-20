mod common;
use common::*;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use tickwire_account::{Account, RecoveryRecord};
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord};
use tickwire_account_journal::*;
use tickwire_journal::Journal;
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "tickwire-ingress-journal-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn backend(&self) -> FileBackend {
        FileBackend::open(&self.0).unwrap()
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn setup() -> (CanonicalBaseline, Config, RecoveryRecord, RecoveryRecord) {
    let (s, c, cmd) = fixture();
    let b = CanonicalBaseline::new(s, c).unwrap();
    let c = Config {
        physical: tickwire_journal::Config::new([2; 16], [1; 16]),
        baseline_digest: b.digest(),
        initial_writer_epoch: 4,
        scope: b.snapshot().scope.clone(),
    };
    let mut a = Account::from_snapshot(b.snapshot().clone(), b.catalog().clone()).unwrap();
    let command = canonical(cmd.clone());
    let admission = a.admit(&command, 60).unwrap();
    (
        b,
        c,
        RecoveryRecord::Admit {
            command: Box::new(command),
            evaluated_at: 60,
            expected: admission.image(),
        },
        RecoveryRecord::IngressEvent {
            input_stream: [7; 16],
            input_sequence: 1,
            event: Box::new(event(fill(&cmd, "input", "0.0001", "60000", "6"))),
        },
    )
}
struct Verify(RecoveryRecord);
impl RecoveryVerifier for Verify {
    fn verify_admission(
        &mut self,
        _: &CanonicalBaseline,
        r: &CanonicalRecord,
        _: &HistoryFrontier,
    ) -> std::result::Result<(), VerificationError> {
        if r.record() == &self.0 {
            Ok(())
        } else {
            Err(VerificationError::Invalid)
        }
    }
}
#[test]
fn exact_input_retry_returns_receipt_without_changing_history() {
    let t = Temp::new();
    let (b, c, admit, input) = setup();
    let mut j = AccountJournal::create(t.backend(), c.clone(), b).unwrap();
    j.append(admit.clone()).unwrap();
    let receipt = j.append(input.clone()).unwrap();
    let history = j.history_frontier().clone();
    assert_eq!(j.append(input.clone()).unwrap(), receipt);
    assert_eq!(j.position(), 2);
    assert_eq!(j.history_frontier(), &history);
    assert!(j.ingress_event([7; 16], 1).is_some());
    drop(j);
    let mut recovered =
        AccountJournal::recover(t.backend(), c, 5, None, &mut Verify(admit)).unwrap();
    let once = recovered.journal.append(input.clone()).unwrap();
    assert_eq!(once.record_digest(), receipt.record_digest());
    assert_eq!(once.sequence(), 2);
    assert_eq!(recovered.journal.position(), 2);
    let mut conflicting = input;
    if let RecoveryRecord::IngressEvent { event, .. } = &mut conflicting {
        let mut e = event.event().clone();
        e.identity.received_mono_ns += 1;
        **event = tickwire_venue::CanonicalEvent::new(e).unwrap();
    }
    assert!(matches!(
        recovered.journal.append(conflicting),
        Err(Error::Conflict)
    ));
    assert!(recovered.journal.poisoned());
    assert_eq!(recovered.journal.position(), 2);
}
#[test]
fn physically_duplicated_input_key_is_invalid_even_if_event_matches() {
    let t = Temp::new();
    let (b, c, admit, input) = setup();
    let mut j = AccountJournal::create(t.backend(), c.clone(), b).unwrap();
    j.append(admit.clone()).unwrap();
    j.append(input.clone()).unwrap();
    drop(j);
    let (mut physical, _) = Journal::recover(t.backend(), None).unwrap();
    let duplicate = CanonicalRecord::new(c.baseline_digest, 3, input).unwrap();
    physical.append_batch(4, &[duplicate.bytes()]).unwrap();
    drop(physical);
    assert!(matches!(
        AccountJournal::recover(t.backend(), c, 5, None, &mut Verify(admit)),
        Err(Error::Conflict)
    ));
}
