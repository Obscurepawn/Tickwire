mod common;
use common::*;
use tickwire_account::RecoveryRecord;
use tickwire_account_codec::CanonicalRecord;
fn input(stream: [u8; 16], sequence: u64) -> RecoveryRecord {
    let (_, cmd) = setup();
    RecoveryRecord::IngressEvent {
        input_stream: stream,
        input_sequence: sequence,
        event: Box::new(event(fill(&cmd, "ingress", "0.0001", "60000", "6"))),
    }
}
#[test]
fn tag9_roundtrips_and_binds_both_input_coordinates() {
    let a = CanonicalRecord::new([7; 32], 1, input([8; 16], 1)).unwrap();
    assert_eq!(CanonicalRecord::decode(a.bytes()).unwrap(), a);
    for r in [input([9; 16], 1), input([8; 16], 2)] {
        assert_ne!(
            CanonicalRecord::new([7; 32], 1, r).unwrap().digest(),
            a.digest()
        );
    }
}
#[test]
fn ingress_zero_identity_is_rejected() {
    assert!(CanonicalRecord::new([7; 32], 1, input([0; 16], 1)).is_err());
    assert!(CanonicalRecord::new([7; 32], 1, input([8; 16], 0)).is_err());
}
