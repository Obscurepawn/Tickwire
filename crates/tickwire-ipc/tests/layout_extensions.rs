//! Literal wire samples for fields added after the original v1 sample set.
use tickwire_ipc::*;

fn hex(raw: &str) -> Vec<u8> {
    raw.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn intent_command_binding_has_fixed_offsets() {
    let value = IntentBody {
        command_digest: core::array::from_fn(|i| i as u8),
        command_bytes: 0x01020304,
        ..Default::default()
    };
    let mut raw = vec![0xCC; IntentBody::SIZE];
    value.encode(&mut raw).unwrap();
    assert_eq!(
        &raw[304..340],
        hex(concat!(
            "000102030405060708090a0b0c0d0e0f",
            "101112131415161718191a1b1c1d1e1f04030201"
        ))
    );
    assert!(raw[..304].iter().all(|b| *b == 0));
    assert!(raw[340..].iter().all(|b| *b == 0));
    assert_eq!(IntentBody::decode(&raw).unwrap(), value);
}

#[test]
fn exact_depth_coverage_literal_sample() {
    let value = DepthCoverage {
        version: 1,
        bid_kind: 2,
        ask_kind: 1,
        bid_boundary_ticks: -2,
        ask_boundary_ticks: 300,
        source_book_generation: 0x0102030405060708,
    };
    let expected = hex(concat!(
        "0100020100000000feffffffffffffff",
        "2c010000000000000807060504030201"
    ));
    let mut actual = vec![0xCC; DepthCoverage::SIZE];
    value.encode(&mut actual).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(DepthCoverage::decode(&expected).unwrap(), value);
}

#[test]
fn handover_ready_literal_sample() {
    let value = HandoverReadyBody {
        instance_id: [0x11; 16],
        logical_strategy_id: 1,
        strategy_generation: 2,
        exec_report_sequence: 3,
        report_ring_sequence: 4,
        ledger_sequence: 5,
        checkpoint_digest: [0x22; 32],
        orders_digest: [0x33; 32],
        input_seal_digest: [0x44; 32],
        checkpoint_bytes: 6,
        input_seal_bytes: 7,
        ready_sequence: 8,
    };
    let expected = hex(concat!(
        "11111111111111111111111111111111",
        "01000000000000000200000000000000",
        "03000000000000000400000000000000",
        "0500000000000000",
        "2222222222222222222222222222222222222222222222222222222222222222",
        "3333333333333333333333333333333333333333333333333333333333333333",
        "4444444444444444444444444444444444444444444444444444444444444444",
        "06000000000000000700000000000000",
        "080000000000000000000000000000000000000000000000"
    ));
    let mut actual = vec![0xCC; HandoverReadyBody::SIZE];
    value.encode(&mut actual).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(HandoverReadyBody::decode(&expected).unwrap(), value);
}
