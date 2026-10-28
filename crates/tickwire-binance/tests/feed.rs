mod common;
use tickwire_binance::{feed::*, request::Market};
use tickwire_book::{BinanceSpot, BinanceUsdM, Book, BookKey, PriceTicks, QtyLots};
use tickwire_venue::{ContractKind, Product};

fn context(market: Market) -> DepthContext {
    let mut command = common::command();
    if market == Market::Usdm {
        command.context.request.product = Product::LinearPerpetual;
        command.context.instrument.contract = ContractKind::Perpetual;
    }
    DepthContext::new(
        market,
        command.context.instrument,
        BookKey {
            venue: 1,
            instrument: 12,
            source: 1,
        },
        7,
        TimeUnit::Milliseconds,
    )
    .unwrap()
}
#[test]
fn compact_book_key_accepts_full_external_ids_and_retains_symbol_checks() {
    for external in [u64::from(u32::MAX) + 12, u64::MAX] {
        for local in [0, 7] {
            let mut instrument = common::command().context.instrument;
            instrument.instrument_id = external;
            let key = BookKey {
                venue: 1,
                instrument: local,
                source: 1,
            };
            let ctx = DepthContext::new(Market::Spot, instrument, key, 7, TimeUnit::Milliseconds)
                .unwrap();
            let snapshot = decode_snapshot::<8>(
                br#"{"lastUpdateId":100,"bids":[["99","1"]],"asks":[["101","2"]]}"#,
                &ctx,
                4096,
            )
            .unwrap();
            let delta = decode_diff::<8>(
                br#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":101,"u":101,"b":[],"a":[]}"#,
                &ctx,
                50,
                4096,
            )
            .unwrap();
            assert_eq!(snapshot.snapshot().key, key);
            assert_eq!(delta.event().key, key);
            assert_eq!(delta.event().generation, 7);
            let mut book = Book::<BinanceSpot, 8, 8>::new(key, 7);
            book.install_snapshot(snapshot.snapshot()).unwrap();
            book.apply(delta.event()).unwrap();
            assert_eq!(book.last_update_id(), Some(101));
            assert!(matches!(
                decode_diff::<8>(
                    br#"{"e":"depthUpdate","E":1,"s":"ETHUSDT","U":102,"u":102,"b":[],"a":[]}"#,
                    &ctx,
                    51,
                    4096,
                ),
                Err(tickwire_binance::Error::IdentityMismatch)
            ));
        }
    }
}

#[test]
fn compact_book_key_does_not_admit_zero_external_id_or_invalid_context() {
    let key = BookKey {
        venue: 1,
        instrument: 0,
        source: 1,
    };
    let mut invalid = common::command().context.instrument;
    invalid.instrument_id = 0;
    assert!(matches!(
        DepthContext::new(Market::Spot, invalid, key, 7, TimeUnit::Milliseconds),
        Err(tickwire_binance::Error::IdentityMismatch)
    ));
    for change in 0..6 {
        let mut instrument = common::command().context.instrument;
        instrument.instrument_id = u64::MAX;
        let mut key = key;
        match change {
            0 => key.venue = 0,
            1 => key.source = 0,
            2 => instrument.instrument_version = 0,
            3 => instrument.catalog_version = 0,
            4 => instrument.catalog_hash = [0; 32],
            _ => instrument.contract = ContractKind::Perpetual,
        }
        assert!(matches!(
            DepthContext::new(Market::Spot, instrument, key, 7, TimeUnit::Milliseconds),
            Err(tickwire_binance::Error::IdentityMismatch)
        ));
    }
}
#[test]
fn spot_snapshot_and_next_update_feed_the_real_sequence_policy() {
    let ctx = context(Market::Spot);
    let snap = decode_snapshot::<8>(
        br#"{"lastUpdateId":100,"bids":[["99.00","1.00000"]],"asks":[["101.00","2.00000"]]}"#,
        &ctx,
        4096,
    )
    .unwrap();
    let diff = decode_diff::<8>(br#"{"e":"depthUpdate","E":123,"s":"BTCUSDT","U":101,"u":102,"b":[["99.00","0.00000"],["98.00","3.00000"]],"a":[]}"#, &ctx, 50, 4096).unwrap();
    assert_eq!(snap.snapshot().bids[0].price, PriceTicks(9900));
    assert_eq!(snap.snapshot().bids[0].quantity, QtyLots(100000));
    assert!(!snap.snapshot().bids_complete);
    assert_eq!(diff.event_time_ns(), Some(123_000_000));
    let mut book = Book::<BinanceSpot, 8, 8>::new(snap.snapshot().key, 7);
    book.install_snapshot(snap.snapshot()).unwrap();
    book.apply(diff.event()).unwrap();
    assert_eq!(book.last_update_id(), Some(102));
}
#[test]
fn usdm_preserves_pu_and_accepts_the_snapshot_in_the_first_range() {
    let ctx = context(Market::Usdm);
    let snap = decode_snapshot::<8>(
        br#"{"lastUpdateId":100,"E":1,"T":1,"bids":[["99.00","1"]],"asks":[["101.00","2"]]}"#,
        &ctx,
        4096,
    )
    .unwrap();
    let diff = decode_diff::<8>(br#"{"e":"depthUpdate","E":3,"T":2,"s":"BTCUSDT","st":1,"U":99,"u":101,"pu":98,"b":[],"a":[]}"#, &ctx, 50, 4096).unwrap();
    assert_eq!(diff.event().previous_update_id, Some(98));
    let mut book = Book::<BinanceUsdM, 8, 8>::new(snap.snapshot().key, 7);
    book.install_snapshot(snap.snapshot()).unwrap();
    book.apply(diff.event()).unwrap();
    assert_eq!(book.last_update_id(), Some(101));
}
#[test]
fn exchange_information_preserves_unknown_rules_and_uses_filter_grids() {
    let data = br#"{"timezone":"UTC","serverTime":1,"rateLimits":[],"exchangeFilters":[],"symbols":[{"symbol":"BTCUSDT","status":"FUTURE_STATUS","baseAsset":"BTC","quoteAsset":"USDT","pricePrecision":8,"quantityPrecision":8,"filters":[{"filterType":"PRICE_FILTER","minPrice":"0.01","maxPrice":"1000000","tickSize":"0.05"},{"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"1000","stepSize":"0.001"},{"filterType":"NEW_FILTER","future":9007199254740993}]}]}"#;
    let parsed = decode_exchange_info::<4>(Market::Spot, data, 4096).unwrap();
    assert_eq!(parsed.symbols[0].status, "FUTURE_STATUS");
    assert_eq!(
        parsed.symbols[0].price_filter().unwrap().increment,
        tickwire_binance::decimal::Decimal::parse("0.05").unwrap()
    );
    assert_eq!(
        parsed.symbols[0].quantity_filter().unwrap().increment,
        tickwire_binance::decimal::Decimal::parse("0.001").unwrap()
    );
    assert!(
        parsed.symbols[0].filters[2]
            .raw
            .get()
            .contains("9007199254740993")
    );
}
#[test]
fn depth_rejects_wrong_products_missing_previous_and_wrong_symbol() {
    let ctx = context(Market::Usdm);
    for bytes in [
        r#"{"e":"depthUpdate","E":3,"T":2,"s":"BTCUSDT","st":2,"U":99,"u":101,"pu":98,"b":[],"a":[]}"#,
        r#"{"e":"depthUpdate","E":3,"T":2,"s":"BTCUSDT","st":3,"U":99,"u":101,"pu":98,"b":[],"a":[]}"#,
        r#"{"e":"depthUpdate","E":3,"T":2,"s":"BTCUSDT","st":1,"U":99,"u":101,"b":[],"a":[]}"#,
        r#"{"e":"depthUpdate","E":3,"T":2,"s":"ETHUSDT","st":1,"U":99,"u":101,"pu":98,"b":[],"a":[]}"#,
    ] {
        assert!(decode_diff::<8>(bytes.as_bytes(), &ctx, 50, 4096).is_err());
    }
}
#[test]
fn malformed_arrays_numbers_and_nonintegral_grid_values_are_rejected() {
    let ctx = context(Market::Spot);
    for pairs in [
        r#"[["99","1","extra"]]"#,
        r#"[["99"]]"#,
        r#"[[99,"1"]]"#,
        r#"[["9.9e1","1"]]"#,
        r#"[["99","-1"]]"#,
        r#"[["0","1"]]"#,
        r#"[["99.001","1"]]"#,
        r#"[["99","0.000001"]]"#,
        r#"[["92233720368547758.08","1"]]"#,
    ] {
        let bytes =
            format!(r#"{{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"b":{pairs},"a":[]}}"#);
        assert!(
            decode_diff::<8>(bytes.as_bytes(), &ctx, 50, 4096).is_err(),
            "{pairs}"
        );
    }
}
#[test]
fn message_and_level_capacity_fail_without_delivering_a_prefix() {
    let ctx = context(Market::Spot);
    let data = br#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"b":[["99","1"],["98","2"]],"a":[]}"#;
    assert!(matches!(
        decode_diff::<1>(data, &ctx, 50, 4096),
        Err(tickwire_binance::Error::Capacity)
    ));
    assert!(matches!(
        decode_diff::<8>(data, &ctx, 50, data.len() - 1),
        Err(tickwire_binance::Error::Capacity)
    ));
    assert_eq!(
        decode_diff::<2>(data, &ctx, 50, 4096)
            .unwrap()
            .event()
            .bids
            .len(),
        2
    );
}
#[test]
fn combined_stream_identity_excludes_partial_and_rpi_depth() {
    let ctx = context(Market::Spot);
    for stream in ["ethusdt@depth", "btcusdt@depth5", "btcusdt@rpiDepth@500ms"] {
        let data = format!(
            r#"{{"stream":"{stream}","data":{{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"b":[],"a":[]}}}}"#
        );
        assert!(decode_diff::<8>(data.as_bytes(), &ctx, 50, 4096).is_err());
    }
    let data=br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"b":[],"a":[]}}"#;
    assert!(decode_diff::<8>(data, &ctx, 50, 4096).is_ok());
}
#[test]
fn identifiers_above_binary_float_precision_and_time_units_are_exact() {
    let ctx = context(Market::Spot);
    let data=br#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":9007199254740993,"u":9007199254740994,"b":[],"a":[]}"#;
    let parsed = decode_diff::<8>(data, &ctx, 50, 4096).unwrap();
    assert_eq!(parsed.event().first_update_id, 9007199254740993);
    assert_eq!(parsed.event().last_update_id, 9007199254740994);
    assert_eq!(TimeUnit::Microseconds.nanoseconds(123), Ok(123000));
    assert!(TimeUnit::Milliseconds.nanoseconds(u64::MAX).is_err());
}
#[test]
fn known_duplicate_fields_and_invalid_ranges_cannot_override_prior_values() {
    let ctx = context(Market::Spot);
    for data in [
        r#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"u":2,"b":[],"a":[]}"#,
        r#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":1,"b":[],"a":[]}"#,
        r#"{"lastUpdateId":1,"bids":[["99","1"],["99","2"]],"asks":[]}"#,
    ] {
        assert!(decode_diff::<8>(data.as_bytes(), &ctx, 50, 4096).is_err());
    }
    assert!(
        decode_snapshot::<8>(
            br#"{"lastUpdateId":1,"bids":[["99","1"],["99","2"]],"asks":[]}"#,
            &ctx,
            4096
        )
        .is_err()
    );
}
#[test]
fn decoder_preserves_duplicate_updates_for_the_atomic_book_validator() {
    let ctx = context(Market::Spot);
    let snap = decode_snapshot::<8>(
        br#"{"lastUpdateId":1,"bids":[["99","1"]],"asks":[["101","2"]]}"#,
        &ctx,
        4096,
    )
    .unwrap();
    let diff=decode_diff::<8>(br#"{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":2,"b":[["99","2"],["99","3"]],"a":[]}"#,&ctx,50,4096).unwrap();
    let mut book = Book::<BinanceSpot, 8, 8>::new(snap.snapshot().key, 7);
    book.install_snapshot(snap.snapshot()).unwrap();
    assert!(book.apply(diff.event()).is_err());
    assert_eq!(book.last_update_id(), Some(1));
}
#[test]
fn mixed_cm_metadata_is_retained_but_never_classified_as_usdm() {
    let data=br#"{"rateLimits":[],"exchangeFilters":[],"assets":[],"symbols":[{"symbol":"BTCUSD_PERP","status":"TRADING","baseAsset":"BTC","quoteAsset":"USD","marginAsset":"BTC","contractType":"PERPETUAL","st":2,"filters":[]}]}"#;
    let metadata = decode_exchange_info::<2>(Market::Usdm, data, 4096).unwrap();
    assert_eq!(metadata.symbols[0].symbol_type, Some(2));
    assert_eq!(metadata.symbols[0].product(), None);
}
