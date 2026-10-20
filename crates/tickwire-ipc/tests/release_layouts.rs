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
#[test]
fn fresh_seal_request_and_reply_keep_challenge_and_cut_separate_from_ready() {
    let value = CheckpointSealBody {
        instance_id: [0x11; 16],
        logical_strategy_id: 1,
        strategy_generation: 2,
        challenge: [0x22; 16],
        report_sequence: 3,
        valid_until_mono_ns: 4,
        seal_mode: 1,
        schema: 1,
    };
    let expected = hex(concat!(
        "11111111111111111111111111111111",
        "01000000000000000200000000000000",
        "22222222222222222222222222222222",
        "03000000000000000400000000000000",
        "0100010000000000000000000000000000000000000000000000000000000000"
    ));
    let mut raw = vec![0xcc; 96];
    value.encode(&mut raw).unwrap();
    assert_eq!(raw, expected);
    assert_eq!(CheckpointSealBody::decode(&expected).unwrap(), value);
    let reply = CheckpointSealReplyBody {
        challenge: [0x22; 16],
        report_sequence: 3,
        seal_mode: 1,
        schema: 1,
        ready: HandoverReadyBody {
            instance_id: [0x11; 16],
            ready_sequence: 5,
            ..Default::default()
        },
    };
    let mut expected = vec![0; 256];
    expected[..16].fill(0x22);
    expected[16] = 3;
    expected[24] = 1;
    expected[26] = 1;
    expected[32..48].fill(0x11);
    expected[200] = 5;
    let mut raw = vec![0xcc; 256];
    reply.encode(&mut raw).unwrap();
    assert_eq!(raw, expected);
    assert_eq!(CheckpointSealReplyBody::decode(&expected).unwrap(), reply);
}
