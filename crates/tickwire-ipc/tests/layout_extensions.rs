//! Literal wire samples for inline SHM ORS records, DOP logon frames, and extended layouts.
#![forbid(unsafe_code)]

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
fn inline_intent_body_has_fixed_offsets() {
    let mut cid = [0u8; 40];
    cid[..32].copy_from_slice(b"tw100000000000000000000000000001");
    let value = IntentBody {
        msg_seqnum: 7,
        order_seqnum: 9,
        tick_mono_ns: 1_234_567,
        price_ticks: -42,
        qty_lots: 500,
        arena_offset: 8320,
        arena_len: 256,
        command_kind: intent_kind::NEW_ORDER,
        strat_id: 12,
        account_ref: 3,
        time_in_force: tif_code::RPI,
        intent_flags: intent_flags::BYPASS_RMS | intent_flags::POST_ONLY,
        client_id_len: 32,
        client_id: cid,
        ..Default::default()
    };
    let mut raw = vec![0xCC; IntentBody::SIZE];
    value.encode(&mut raw).unwrap();
    assert_eq!(u64::from_le_bytes(raw[0..8].try_into().unwrap()), 7);
    assert_eq!(u64::from_le_bytes(raw[8..16].try_into().unwrap()), 9);
    assert_eq!(
        u64::from_le_bytes(raw[24..32].try_into().unwrap()),
        1_234_567
    );
    assert_eq!(i64::from_le_bytes(raw[88..96].try_into().unwrap()), -42);
    assert_eq!(i64::from_le_bytes(raw[96..104].try_into().unwrap()), 500);
    assert_eq!(u64::from_le_bytes(raw[152..160].try_into().unwrap()), 8320);
    assert_eq!(u32::from_le_bytes(raw[160..164].try_into().unwrap()), 256);
    assert_eq!(u16::from_le_bytes(raw[164..166].try_into().unwrap()), 1);
    assert_eq!(raw[170], 12);
    assert_eq!(raw[171], 3);
    assert_eq!(raw[174], tif_code::RPI);
    assert_eq!(raw[179], intent_flags::BYPASS_RMS | intent_flags::POST_ONLY);
    assert_eq!(raw[181], 32);
    assert_eq!(&raw[184..216], b"tw100000000000000000000000000001");
    assert!(raw[360..384].iter().all(|&b| b == 0));
    assert_eq!(IntentBody::decode(&raw).unwrap(), value);
}

#[test]
fn inline_report_body_and_dop_frames_have_fixed_offsets() {
    let report = ReportBody {
        report_seq: 11,
        orig_seqnum: 7,
        order_seqnum: 9,
        ors_order_id: 9001,
        fee_units: -25,
        report_kind: report_kind::FILL,
        order_status: 3,
        reason: 0,
        strat_id: 12,
        account_ref: 3,
        liquidity: 1,
        amendment: 1,
        has_fee: 1,
        asset_exponent: -8,
        ..Default::default()
    };
    let mut raw = vec![0xCC; ReportBody::SIZE];
    report.encode(&mut raw).unwrap();
    assert_eq!(u64::from_le_bytes(raw[0..8].try_into().unwrap()), 11);
    assert_eq!(u64::from_le_bytes(raw[8..16].try_into().unwrap()), 7);
    assert_eq!(i64::from_le_bytes(raw[136..144].try_into().unwrap()), -25);
    assert_eq!(
        u16::from_le_bytes(raw[184..186].try_into().unwrap()),
        report_kind::FILL
    );
    assert_eq!(raw[192], 12);
    assert_eq!(raw[193], 3);
    assert_eq!(raw[197], 1);
    assert_eq!(raw[199], 1);
    assert_eq!(raw[200] as i8, -8);
    assert_eq!(ReportBody::decode(&raw).unwrap(), report);

    let login = DopLoginBody {
        next_source_seqnum: 100,
        client_id: 77,
        strat_id: 5,
        ..Default::default()
    };
    let mut login_raw = vec![0xCC; DopLoginBody::SIZE];
    login.encode(&mut login_raw).unwrap();
    assert_eq!(u64::from_le_bytes(login_raw[0..8].try_into().unwrap()), 100);
    assert_eq!(u32::from_le_bytes(login_raw[8..12].try_into().unwrap()), 77);
    assert_eq!(login_raw[12], 5);
    assert_eq!(DopLoginBody::decode(&login_raw).unwrap(), login);

    let echo = DopEchoBody {
        next_source_order_seqnum: 105,
        tx_key: 0xAA,
        rx_key: 0xBB,
        tx_len: 4_202_496,
        rx_len: 4_202_496,
        hb_interval_ms: 500,
        strat_id: 5,
        connected: 1,
        enabled: 1,
        market_connected: 1,
        ready_to_trade: 1,
        ..Default::default()
    };
    let mut echo_raw = vec![0xCC; DopEchoBody::SIZE];
    echo.encode(&mut echo_raw).unwrap();
    assert_eq!(echo_raw[38], 5);
    assert_eq!(&echo_raw[39..43], &[1, 1, 1, 1]);
    assert_eq!(DopEchoBody::decode(&echo_raw).unwrap(), echo);
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
