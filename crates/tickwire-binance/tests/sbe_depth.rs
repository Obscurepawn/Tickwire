mod common;
use tickwire_binance::{depth_sync::*, feed::TimeUnit};
use tickwire_book::{BinanceSpot, BookKey, PriceTicks, QuantityKnowledge, Side};
fn config() -> Config {
    Config {
        instrument: common::command().context.instrument,
        key: BookKey {
            venue: 1,
            instrument: 12,
            source: 1,
        },
        time_unit: TimeUnit::Microseconds,
        snapshot_limit: 5,
        max_message_bytes: 4096,
        initial_book_generation: 0,
        previous_connection_epoch: 0,
    }
}
fn literal() -> Vec<u8> {
    include_str!("../../tickwire-binance-sbe/tests/fixtures/diff.hex")
        .trim()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
#[test]
fn compact_book_key_bridges_binary_depth_with_a_full_u64_catalog_id() {
    let mut cfg = config();
    cfg.instrument.instrument_id = u64::MAX;
    cfg.key.instrument = 0;
    let key = cfg.key;
    let mut sync = DepthSync::<BinanceSpot, 8, 8, 4>::new(cfg).unwrap();
    let token = sync.connect(1).unwrap();
    sync.subscription_ready(token).unwrap();
    assert!(matches!(
        sync.on_sbe_delta(token, &literal(), 10).unwrap(),
        Outcome::Buffered { .. }
    ));
    let request = sync.request_snapshot(11, 100).unwrap();
    assert!(matches!(sync.on_snapshot(request.token(), 200,
        br#"{"lastUpdateId":43,"bids":[["65000","0.1"],["64999","0.2"]],"asks":[["65001","0.3"]]}"#,
        12).unwrap(), Outcome::SnapshotInstalled { live: true, .. }));
    assert_eq!(sync.view(12, 100).unwrap().key(), key);
    assert_eq!(sync.last_update_id(), Some(44));
}
#[test]
fn binary_diff_bridges_real_snapshot_without_json_conversion() {
    let mut s = DepthSync::<BinanceSpot, 8, 8, 4>::new(config()).unwrap();
    let t = s.connect(1).unwrap();
    s.subscription_ready(t).unwrap();
    assert_eq!(
        s.on_sbe_delta(t, &literal(), 10).unwrap(),
        Outcome::Buffered { events: 1 }
    );
    let r = s.request_snapshot(11, 100).unwrap();
    let json =
        br#"{"lastUpdateId":43,"bids":[["65000","0.1"],["64999","0.2"]],"asks":[["65001","0.3"]]}"#;
    assert!(matches!(
        s.on_snapshot(r.token(), 200, json, 12).unwrap(),
        Outcome::SnapshotInstalled { live: true, .. }
    ));
    assert_eq!(s.last_update_id(), Some(44));
    let view = s.view(12, 100).unwrap();
    assert_eq!(
        view.quantity(Side::Ask, PriceTicks(6500100)),
        QuantityKnowledge::Known(tickwire_book::QtyLots(50000))
    );
    assert_eq!(
        view.quantity(Side::Bid, PriceTicks(6500000)),
        QuantityKnowledge::Known(tickwire_book::QtyLots(0))
    );
    assert!(
        s.view(111, 100).is_err(),
        "snapshot callback must not replace receive time"
    );
}
#[test]
fn old_binary_token_is_rejected_before_decode_and_malformed_current_rebuilds() {
    let mut s = DepthSync::<BinanceSpot, 8, 8, 4>::new(config()).unwrap();
    let old = s.connect(1).unwrap();
    let current = s.connect(2).unwrap();
    assert_eq!(
        s.on_sbe_delta(old, b"bad", 0).unwrap(),
        Outcome::IgnoredObsolete
    );
    assert!(matches!(
        s.on_sbe_delta(current, b"bad", 1).unwrap(),
        Outcome::Rebuild(RebuildReason::Decode(_))
    ));
}

#[test]
fn production_binary_depth_decodes_on_default_worker_stack() {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "production_decode_stack_child", "--nocapture"])
        .env("TICKWIRE_SBE_DECODE_STACK_CHILD", "1")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
#[test]
fn production_decode_stack_child() {
    if std::env::var_os("TICKWIRE_SBE_DECODE_STACK_CHILD").is_none() {
        return;
    }
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let c = config();
            let context = tickwire_binance::feed::DepthContext::new(
                tickwire_binance::request::Market::Spot,
                c.instrument,
                c.key,
                1,
                c.time_unit,
            )
            .unwrap();
            let raw = literal();
            let update =
                tickwire_binance::feed::decode_sbe_diff::<5000>(&raw, &context, 10, 4096).unwrap();
            assert_eq!(update.event().last_update_id, 44);
            assert_eq!(update.event().asks[0].quantity.0, 50000);
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn direct_typed_decode_preserves_identity_kind_and_exact_decimal_rejections() {
    use tickwire_binance::{
        Error,
        feed::{DepthContext, decode_sbe_diff},
        request::Market,
    };
    let c = config();
    let mut context = DepthContext::new(Market::Spot, c.instrument, c.key, 1, c.time_unit).unwrap();
    let decode = |raw: &[u8], context: &DepthContext| decode_sbe_diff::<8>(raw, context, 10, 4096);
    let raw = literal();
    assert_eq!(
        decode(&raw, &context).unwrap().event().bids[0].quantity.0,
        0
    );
    context.set_generation(0);
    assert_eq!(decode(&raw, &context).unwrap_err(), Error::IdentityMismatch);
    context.set_generation(1);
    let mut wrong_symbol = raw.clone();
    let symbol_start = wrong_symbol.len() - 7;
    wrong_symbol[symbol_start] = b'X';
    assert_eq!(
        decode(&wrong_symbol, &context).unwrap_err(),
        Error::IdentityMismatch
    );
    // Literal SBE frame: header 8, root 26, first group header 4.
    let mut fractional = raw.clone();
    fractional[32] = (-3i8) as u8;
    fractional[38..46].copy_from_slice(&6_500_001i64.to_le_bytes());
    assert_eq!(
        decode(&fractional, &context).unwrap_err(),
        Error::InvalidDecimal
    );
    let mut overflow = raw.clone();
    overflow[32] = 127;
    assert_eq!(decode(&overflow, &context).unwrap_err(), Error::Overflow);
    let mut negative_quantity = raw;
    negative_quantity[46..54].copy_from_slice(&(-1i64).to_le_bytes());
    assert_eq!(
        decode(&negative_quantity, &context).unwrap_err(),
        Error::InvalidResponse
    );
    let partial = include_str!("../../tickwire-binance-sbe/tests/fixtures/partial.hex")
        .trim()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        decode(&partial, &context).unwrap_err(),
        Error::InvalidResponse
    );
}
