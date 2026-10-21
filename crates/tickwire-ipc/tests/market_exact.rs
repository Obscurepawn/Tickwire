use tickwire_ipc::*;
fn sample(kind: u16) -> MarketExactRecord {
    MarketExactRecord {
        header: RecordHeader {
            sequence: 1,
            message_type: kind,
            schema_major: 1,
            header_bytes: 128,
            body_bytes: 384,
            topic_or_scope_id: 1,
            producer_generation: 1,
            feed_generation: 1,
            instrument_id: 1,
            instrument_version: 1,
            ..Default::default()
        },
        body: MarketExactBody {
            source_id: 1,
            source_connection_epoch: 2,
            source_encoding: 1,
            feed_state: 2,
            ..Default::default()
        },
    }
}
fn image(r: &MarketExactRecord) -> [u8; 512] {
    let mut raw = [0; 512];
    r.encode(&mut raw).unwrap();
    raw
}
#[test]
fn literal_trade_offsets_and_semantics() {
    let mut r = sample(264);
    r.body.trade_id = 42;
    r.body.price_ticks = 9901;
    r.body.quantity_lots = 100;
    r.body.side = 2;
    r.body.trade_flags = 1;
    let raw = image(&r);
    assert_eq!(&raw[192..200], &42u64.to_le_bytes());
    assert_eq!(&raw[200..208], &9901i64.to_le_bytes());
    assert_eq!(raw[388], 2);
    assert_eq!(raw[389], 1);
    assert!(decode_record(&raw).is_ok());
}
#[test]
fn finer_than_tick_index_and_negative_funding_are_valid() {
    let mut r = sample(266);
    r.body.mark_price = DecimalWire {
        coefficient: 1179415i128.to_le_bytes(),
        exponent: -2,
    };
    r.body.index_price = DecimalWire {
        coefficient: 1178462659091i128.to_le_bytes(),
        exponent: -8,
    };
    r.body.funding_rate = DecimalWire {
        coefficient: (-3i128).to_le_bytes(),
        exponent: -4,
    };
    r.body.value_validity = 32;
    let raw = image(&r);
    assert_eq!(&raw[280..296], &1178462659091i128.to_le_bytes());
    assert!(decode_record(&raw).is_ok());
}
#[test]
fn quote_without_exchange_time_is_valid() {
    let mut r = sample(265);
    r.body.venue_seq_first = 8;
    r.body.venue_seq_last = 8;
    r.body.bid_price_ticks = 99;
    r.body.ask_price_ticks = 100;
    r.body.bid_quantity_lots = 1;
    r.body.ask_quantity_lots = 2;
    assert!(decode_record(&image(&r)).is_ok());
}
#[test]
fn rejects_presence_mismatch_noncanonical_decimal_and_wrong_fields() {
    let mut r = sample(266);
    r.body.mark_price = DecimalWire {
        coefficient: 100i128.to_le_bytes(),
        exponent: 0,
    };
    r.body.index_price = DecimalWire {
        coefficient: 1i128.to_le_bytes(),
        exponent: 2,
    };
    assert!(decode_record(&image(&r)).is_err());
    r.body.mark_price = r.body.index_price.clone();
    r.body.funding_rate = DecimalWire {
        coefficient: (-3i128).to_le_bytes(),
        exponent: -4,
    };
    assert!(decode_record(&image(&r)).is_err());
    r.body.value_validity = 32;
    r.body.trade_id = 5;
    assert!(decode_record(&image(&r)).is_err());
}
fn literal(text: &str) -> Vec<u8> {
    text.trim()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn independent_literal_frames_decode_and_encode_exactly() {
    for (kind, text) in [
        (264, include_str!("fixtures/exact-events/trade.hex")),
        (265, include_str!("fixtures/exact-events/quote.hex")),
        (266, include_str!("fixtures/exact-events/mark.hex")),
    ] {
        let bytes = literal(text);
        let record = MarketState::decode(&bytes).unwrap();
        assert_eq!(record.header().message_type, kind);
        let mut out = [0; 512];
        record.encode(&mut out).unwrap();
        assert_eq!(out.as_slice(), bytes);
        if let MarketState::Exact(r) = record {
            assert_eq!(r.body.source_connection_epoch, 2);
            if kind == 266 {
                assert_eq!(r.body.index_price.coefficient_value(), 1178462659091);
                assert_eq!(r.body.index_price.exponent, -8);
            }
        }
    }
}
#[test]
fn preflight_control_literal_payloads_keep_fixed_layout() {
    let bytes = literal(include_str!("fixtures/exact-events/restore-applied.hex"));
    let value = RestoreAppliedBody::decode(&bytes).unwrap();
    assert_eq!(value.receipt_bytes, 4);
    assert_eq!(value.strategy_generation, 3);
    let mut encoded = vec![0; 128];
    value.encode(&mut encoded).unwrap();
    assert_eq!(encoded, bytes);
    let bytes = literal(include_str!("fixtures/exact-events/checkpoint-seal.hex"));
    let value = CheckpointSealBody::decode(&bytes).unwrap();
    assert_eq!(value.schema, 2);
    assert_eq!(value.report_sequence, 4);
    let mut encoded = vec![0; 96];
    value.encode(&mut encoded).unwrap();
    assert_eq!(encoded, bytes);
    let bytes = literal(include_str!("fixtures/exact-events/checkpoint-sealed.hex"));
    let value = CheckpointSealReplyBody::decode(&bytes).unwrap();
    assert_eq!(value.ready.ledger_sequence, 7);
    let mut encoded = vec![0; 256];
    value.encode(&mut encoded).unwrap();
    assert_eq!(encoded, bytes);
}
