mod common;
use tickwire_binance::{feed::TimeUnit, market::*, request::Market};
use tickwire_venue::{ContractKind, Decimal};
fn context(market: Market, kind: EventKind) -> EventContext {
    let mut instrument = common::command().context.instrument;
    if market == Market::Usdm {
        instrument.contract = ContractKind::Perpetual;
    }
    EventContext {
        market,
        identity: StreamIdentity {
            source_id: 1,
            connection_epoch: 3,
            instrument_id: instrument.instrument_id,
            instrument_version: instrument.instrument_version,
            catalog_version: instrument.catalog_version,
            catalog_hash: instrument.catalog_hash,
        },
        instrument,
        kind,
        time_unit: TimeUnit::Milliseconds,
    }
}
const TRADE: &[u8] =
    br#"{"e":"trade","E":123,"s":"BTCUSDT","t":42,"p":"99.01","q":"0.001","T":122,"m":true}"#;
#[test]
fn spot_trade_retains_source_and_exact_units() {
    let e = decode_json(TRADE, &context(Market::Spot, EventKind::Trade), 50, 4096).unwrap();
    assert_eq!(
        e.times,
        EventTimes {
            received_mono_ns: 50,
            exchange_event_ns: Some(123000000),
            transaction_ns: Some(122000000)
        }
    );
    assert_eq!(e.identity.connection_epoch, 3);
    assert_eq!(
        e.value,
        EventValue::Trade(Trade {
            trade_id: 42,
            raw_trade_range: None,
            price_ticks: 9901,
            quantity_lots: 100,
            normal_quantity_lots: None,
            buyer_is_maker: true,
            aggregate: false
        })
    );
}
#[test]
fn usdm_aggregate_preserves_both_quantity_and_raw_range() {
    let e=decode_json(br#"{"e":"aggTrade","E":123,"s":"BTCUSDT","a":42,"f":9,"l":12,"p":"99.01","q":"0.001","nq":"0.0005","T":122,"m":false,"st":1}"#,&context(Market::Usdm,EventKind::Trade),50,4096).unwrap();
    let EventValue::Trade(t) = e.value else {
        panic!()
    };
    assert_eq!(t.raw_trade_range, Some((9, 12)));
    assert_eq!(t.normal_quantity_lots, Some(50));
    assert!(t.aggregate);
}
#[test]
fn spot_quote_has_no_invented_exchange_time() {
    let e = decode_json(
        br#"{"u":400,"s":"BTCUSDT","b":"99","B":"1","a":"100","A":"2"}"#,
        &context(Market::Spot, EventKind::BestBidAsk),
        50,
        4096,
    )
    .unwrap();
    assert_eq!(e.times.exchange_event_ns, None);
    assert_eq!(e.times.transaction_ns, None);
    let EventValue::BestBidAsk(q) = e.value else {
        panic!()
    };
    assert_eq!(q.update_id, 400);
    assert_eq!(q.ask_quantity_lots, 200000);
}
#[test]
fn mark_index_and_negative_funding_remain_decimal() {
    let e=decode_json(br#"{"e":"markPriceUpdate","E":123,"s":"BTCUSDT","p":"11794.15000000","i":"11784.62659091","P":"11784.25641265","r":"-0.00030000","ap":"11794.15","T":124,"st":1}"#,&context(Market::Usdm,EventKind::MarkFunding),50,4096).unwrap();
    let EventValue::MarkFunding(m) = e.value else {
        panic!()
    };
    assert_eq!(m.index_price, Decimal::new(1178462659091, -8).unwrap());
    assert_eq!(m.funding_rate, Some(Decimal::new(-3, -4).unwrap()));
    assert_eq!(m.next_funding_utc_ns, Some(124000000));
}
#[test]
fn delivery_optional_funding_is_distinct_from_zero() {
    let mut c = context(Market::Usdm, EventKind::MarkFunding);
    c.instrument.contract = ContractKind::Delivery { expiry_utc_ns: 999 };
    let e = decode_json(
        br#"{"e":"markPriceUpdate","E":123,"s":"BTCUSDT","p":"100","i":"100","r":"","T":0}"#,
        &c,
        50,
        4096,
    )
    .unwrap();
    let EventValue::MarkFunding(m) = e.value else {
        panic!()
    };
    assert_eq!(m.funding_rate, None);
    assert_eq!(m.next_funding_utc_ns, None);
}
fn fixture(name: &str) -> Vec<u8> {
    let raw = std::fs::read_to_string(format!("../tickwire-binance-sbe/tests/fixtures/{name}.hex"))
        .unwrap();
    raw.trim()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|n| u8::from_str_radix(std::str::from_utf8(n).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn sbe_frame_delivers_all_rows_with_original_times() {
    let bytes = fixture("trades");
    let e = decode_sbe(
        &bytes,
        &context(Market::Spot, EventKind::Trade),
        50,
        4096,
        10,
    )
    .unwrap();
    assert_eq!(e.len(), 2);
    assert_eq!(e[0].encoding, EventEncoding::SpotSbeV1);
    assert_eq!(e[0].times.transaction_ns, Some(999999000));
    assert!(
        decode_sbe(
            &bytes,
            &context(Market::Spot, EventKind::Trade),
            50,
            4096,
            1
        )
        .is_err()
    );
}
#[test]
fn wrong_identity_kind_duplicate_and_nonintegral_values_fail() {
    let c = context(Market::Spot, EventKind::Trade);
    for raw in [
        String::from_utf8(TRADE.to_vec())
            .unwrap()
            .replace("BTCUSDT", "ETHUSDT"),
        String::from_utf8(TRADE.to_vec())
            .unwrap()
            .replace("99.01", "99.001"),
        String::from_utf8(TRADE.to_vec())
            .unwrap()
            .replace(r#""t":42"#, r#""t":42,"t":43"#),
    ] {
        assert!(decode_json(raw.as_bytes(), &c, 50, 4096).is_err());
    }
    assert!(
        decode_json(
            TRADE,
            &context(Market::Spot, EventKind::BestBidAsk),
            50,
            4096
        )
        .is_err()
    );
    assert!(decode_json(TRADE, &c, 50, 4).is_err());
}
