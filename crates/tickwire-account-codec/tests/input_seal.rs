use tickwire_account::{IntentKey, RecoveryRecord};
use tickwire_account_codec::{CanonicalRecord, InputSeal};

fn record(baseline: [u8; 32], sequence: u64, id: u64) -> CanonicalRecord {
    CanonicalRecord::new(
        baseline,
        sequence,
        RecoveryRecord::Aborted {
            intent: IntentKey {
                epoch: 1,
                intent_id: id,
            },
            command_digest: [id as u8; 32],
        },
    )
    .unwrap()
}
#[test]
fn independent_report_consumers_seal_the_same_exact_prefix() {
    let mut sender = InputSeal::new([1; 32]).unwrap();
    let mut receiver = InputSeal::new([1; 32]).unwrap();
    for sequence in 1..=3 {
        let value = record([1; 32], sequence, sequence);
        sender.apply(&value).unwrap();
        receiver
            .apply(&CanonicalRecord::decode(value.bytes()).unwrap())
            .unwrap();
    }
    assert_eq!(sender.position(), 3);
    assert_eq!(sender.digest(), receiver.digest());
    assert_eq!(sender.object_bytes(), receiver.object_bytes());
    let mut different = InputSeal::new([1; 32]).unwrap();
    different.apply(&record([1; 32], 1, 9)).unwrap();
    assert_ne!(different.digest(), sender.digest());
}
#[test]
fn missing_duplicate_or_foreign_report_does_not_advance_the_seal() {
    let mut seal = InputSeal::new([1; 32]).unwrap();
    let first = record([1; 32], 1, 1);
    seal.apply(&first).unwrap();
    let before = seal.digest();
    for invalid in [first, record([1; 32], 3, 3), record([2; 32], 2, 2)] {
        assert!(seal.apply(&invalid).is_err());
        assert_eq!(seal.position(), 1);
        assert_eq!(seal.digest(), before);
    }
    seal.apply(&record([1; 32], 2, 2)).unwrap();
    assert_eq!(seal.position(), 2);
}
