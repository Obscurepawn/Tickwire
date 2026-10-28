use super::*;
use crate::replay::income_tests::common as account_fixture;
use serde_json::json;

fn deployment(instrument: u64) -> Deployment {
    serde_json::from_value(json!({
        "baseline":"/unused/baseline.cbor", "directories":{"account":"/unused/account","input":"/unused/input","recovery":"/unused/recovery"},
        "recover":false,"product":"spot","boot_id":([71u8;16]),"clock_generation":1,"owner_id":([72u8;16]),"writer_epoch":4,"initial_writer_epoch":4,
        "account_stream":([21u8;16]),"input_stream":([22u8;16]),"first_connection_epoch":1,"instrument_id":instrument,"book_source":9,
        "credential":{"api_key_file":"/unused/api","signing_key_file":"/unused/key","algorithm":"hmac","generation":1,"spot_logon":false},
        "network":{"rest":"127.0.0.1:1","time":"127.0.0.1:1","public_stream":"127.0.0.1:1","snapshot":"127.0.0.1:1","private_stream":"127.0.0.1:1","private_management":null,"trust_anchors":[]},
        "clock":{"max_round_trip_ns":1000000000,"max_width_ns":1000000000,"max_age_ns":10000000000u64,"quantization_error_ns":1000000,"drift_ppb":1,"max_adjustment_ns":1000000000,"calibration_interval_ns":1000000000,"calibration_retry_ns":100000000},
        "tuning":{"connect_timeout_ns":1000000000,"request_timeout_ns":1000000000,"delivery_budget_ns":1000000,"recv_window_micros":5000000,"maximum_book_age_ns":1000000000,"maximum_idle_wait_ns":1000000,"snapshot_limit":5,"maximum_intents":8,"maximum_input_records":64,"maximum_input_bytes":1048576,"wal_segment_bytes":1048576},
        "policy":{"scope":{"risk_domain_id":([1u8;16]),"venue":1,"environment":"test","account":2},"product":"spot","executor_id":([72u8;16]),"owner_generation":1,"writer_epoch":4,"domain":{"process_instance":([71u8;16]),"generation":1},"versions":{"config_revision":6,"risk_policy_version":10,"order_config_version":11,"catalog_version":14,"catalog_hash":([5u8;32])},"max_strategies":4,"max_feeds":4,"marks_per_feed":2,"control_revision":1,"mode":"stopped","owner_lease":null,"strategy_grants":[],"management_grant":null},
        "quota":{"supervisor_generation":1,"consumer_instance":([72u8;16]),"consumer_generation":1,"consumer_lease_until_ns":1000,"producer_id":([73u8;16]),"policy_generation":1,"policy_starts_ns":0,"policy_ends_ns":1000,"windows":[],"grants":[],"rules":[]},
        "strategy":{"mode":"observe"}, "reconciliation":{"enabled":false,"reader_id":([74u8;16]),"first_request_epoch":1,"first_execution_connection_epoch":2,"retry_ns":1000000000,"valid_for_ns":1000000000},
        "stop_file":"/unused/STOP","maximum_runtime_ns":1000000000
    })).unwrap()
}

fn baseline() -> CanonicalBaseline {
    let (snapshot, mut catalog, _) = account_fixture::fixture();
    let entry = catalog.entries[0].clone();
    // Input order differs from the canonical catalog order on purpose.
    catalog.entries = [u64::MAX, 7, 1 << 48]
        .into_iter()
        .map(|id| {
            let mut value = entry.clone();
            value.instrument.instrument_id = id;
            value
        })
        .collect();
    CanonicalBaseline::new(snapshot, catalog).unwrap()
}

#[test]
fn assemble_uses_distinct_dense_book_keys_without_narrowing_stable_ids() {
    let baseline = baseline();
    let mut keys = std::collections::BTreeSet::new();
    for (stable, dense) in [(1 << 48, 1), (7, 0), (u64::MAX, 2)] {
        let deploy = deployment(stable);
        let assembled = deploy
            .assemble(baseline.clone())
            .expect("all nonzero u64 catalog IDs can construct a local public owner");
        assert_eq!(assembled.market.depth.instrument.instrument_id, stable);
        assert_eq!(assembled.market.depth.key.instrument, dense);
        assert_eq!(assembled.market.depth.key.venue, 1);
        assert_eq!(assembled.market.depth.key.source, 9);
        assert!(keys.insert(assembled.market.depth.key.instrument));
        let key = assembled.market.depth.key;
        let decoded = CanonicalBaseline::decode(baseline.bytes()).unwrap();
        let repeated = deploy.assemble(decoded).unwrap();
        assert_eq!(repeated.market.depth.key, key);
        let mut reconnected = deployment(stable);
        reconnected.first_connection_epoch = 9;
        reconnected.recover = true;
        assert_eq!(
            reconnected
                .assemble(baseline.clone())
                .unwrap()
                .market
                .depth
                .key,
            key
        );
        assert!(
            tickwire_binance::depth_sync::DepthSync::<tickwire_book::BinanceSpot, 5, 5, 2>::new(
                assembled.market.depth
            )
            .is_ok()
        );
    }
}

#[test]
fn assemble_rejects_unknown_zero_or_wrong_product_before_selecting_a_local_slot() {
    let baseline = baseline();
    for id in [0, 8, (1 << 48) + 1] {
        assert!(deployment(id).assemble(baseline.clone()).is_err());
    }
    let mut wrong_product = deployment(1 << 48);
    wrong_product.product = ProductName::Usdm;
    assert!(wrong_product.assemble(baseline).is_err());
}

#[test]
fn assemble_usdm_owner_keeps_full_u64_binding_and_local_slot_zero() {
    let (snapshot, mut catalog, _) = account_fixture::futures_fixture();
    catalog.entries[0].instrument.instrument_id = u64::MAX;
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
    let mut deploy = deployment(u64::MAX);
    deploy.product = ProductName::Usdm;
    deploy.policy.product = tickwire_runtime_policy::bootstrap::Product::LinearPerpetual;
    let assembled = deploy.assemble(baseline).unwrap();
    assert_eq!(assembled.market.depth.instrument.instrument_id, u64::MAX);
    assert_eq!(assembled.market.depth.key.instrument, 0);
    assert!(
        tickwire_binance::depth_sync::DepthSync::<tickwire_book::BinanceUsdM, 5, 5, 2>::new(
            assembled.market.depth
        )
        .is_ok()
    );
}

#[test]
fn separate_feed_sources_keep_their_source_namespace() {
    let baseline = baseline();
    let first = deployment(u64::MAX).assemble(baseline.clone()).unwrap();
    let mut other = deployment(u64::MAX);
    other.book_source = 10;
    let second = other.assemble(baseline).unwrap();
    assert_eq!(
        first.market.depth.key.instrument,
        second.market.depth.key.instrument
    );
    assert_ne!(first.market.depth.key, second.market.depth.key);
    assert_eq!(
        first.market.depth.instrument,
        second.market.depth.instrument
    );
}
