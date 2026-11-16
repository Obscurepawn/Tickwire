use tickwire_ipc::*;
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn restore_applied_has_independent_literal_offsets_and_zero_reserved() {
    let value = RestoreAppliedBody {
        instance_id: [0x11; 16],
        logical_strategy_id: 1,
        strategy_generation: 2,
        receipt_digest: [0x22; 32],
        receipt_bytes: 3,
        receipt_sequence: 4,
        release_manifest_digest: [0x33; 32],
        schema: 1,
        completion_kind: 2,
    };
    let expected = hex(concat!(
        "11111111111111111111111111111111",
        "01000000000000000200000000000000",
        "2222222222222222222222222222222222222222222222222222222222222222",
        "03000000000000000400000000000000",
        "3333333333333333333333333333333333333333333333333333333333333333",
        "01000200000000000000000000000000"
    ));
    let mut raw = vec![0xcc; 128];
    value.encode(&mut raw).unwrap();
    assert_eq!(raw, expected);
    assert_eq!(RestoreAppliedBody::decode(&expected).unwrap(), value);
    for at in 116..128 {
        raw[at] = 1;
        assert_eq!(
            RestoreAppliedBody::decode(&raw),
            Err(Error::ReservedNonzero)
        );
        raw[at] = 0;
    }
}
