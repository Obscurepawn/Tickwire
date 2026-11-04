use super::*;
use tickwire_strategy_sdk::bootstrap::{Bootstrap, Endpoint, IdentityConfig};
#[path = "../../../tickwire-account/tests/common/mod.rs"]
mod account;
fn bootstrap(path: &std::path::Path) -> Bootstrap {
    let local = IdentityConfig {
        instance: [2; 16],
        generation: 5,
        role: 3,
        logical_strategy: 8,
        host_boot: [5; 16],
        namespace: [6; 16],
        build_hash: [7; 32],
        qualification_hash: [8; 32],
        manifest_hash: [9; 32],
        arch: 1,
        trust_profile: 1,
    };
    let ep = Endpoint {
        path: path.join("control"),
        local: local.clone(),
        peer: local,
        peer_pid: 1,
        peer_uid: 0,
        peer_gid: 0,
        required_features: 0,
        supported_features: 0,
        control_epoch: 1,
        lease_duration_ns: 100,
        max_frame_bytes: 4096,
        nonce: [1; 32],
    };
    Bootstrap {
        instrument_catalog: None,
        version: 1,
        market: ep.clone(),
        external_market: None,
        market_admission_ready: None,
        intent: ep.clone(),
        report: ep,
        report_header: vec![],
        report_cursor_id: [1; 16],
        report_subscription_generation: 1,
        report_directory: path.join("reports"),
        join_file: path.join("join"),
        intent_header: vec![],
        intent_cursor_id: [2; 16],
        intent_subscription_generation: 1,
        risk_domain: [1; 16],
        command_directory: path.join("commands"),
        clock_generation: 1,
        startup_timeout_ms: 100,
        maximum_market_age_ns: 1_000_000_000,
        maximum_levels: 4,
        poll_records: 8,
        idle_sleep_us: 1,
        recovery_template: Some(path.join("recovery.json")),
        handover_enabled: true,
        preflight_job: None,
        factory: None,
        demo_enabled: true,
        demo_command_file: None,
        demo_minimum_bid_ticks: 1,
    }
}
pub(super) fn checked() -> std::sync::Arc<tickwire_instrument_catalog::Catalog> {
    let raw=br#"{"schema":1,"venue":1,"environment":"Test","market":"Spot","catalog_version":"5","assets":[{"id":"1","version":"1","symbol":"BTC","exponent":-8},{"id":"2","version":"1","symbol":"USDT","exponent":-8}],"instruments":[{"id":"1001","version":"5","symbol":"BTCUSDT","product":"Spot","base":"1","quote":"2","settlement":"2","price_tick":"0.01","quantity_step":"0.00001","contract":{"kind":"Cash"}}]}"#;
    std::sync::Arc::new(
        tickwire_instrument_catalog::Catalog::decode(raw, tickwire_recovery::digest(raw)).unwrap(),
    )
}
pub(super) fn baseline(
    catalog: &tickwire_instrument_catalog::Catalog,
) -> tickwire_account_codec::CanonicalBaseline {
    let (mut snapshot, mut policies, _) = account::fixture();
    for asset in &mut snapshot.assets {
        asset.asset = catalog.asset(asset.asset.id).unwrap().clone();
    }
    policies.entries[0].instrument = catalog.instrument(1001).unwrap().binding().clone();
    tickwire_account_codec::CanonicalBaseline::new(snapshot, policies).unwrap()
}
pub(super) fn dependency(
    path: &Path,
    source: &Source,
    catalog: &tickwire_instrument_catalog::Catalog,
) -> tickwire_strategy_sdk::preflight::ObjectFile {
    let mut b = bootstrap(path);
    b.instrument_catalog = Some(catalog.artifact());
    b.market.local.instance = source.instance;
    b.market.local.generation = source.generation;
    b.market.local.logical_strategy = source.logical_strategy;
    b.market.local.build_hash = source.binary_digest;
    b.market.local.host_boot = source.clock.boot_id;
    b.clock_generation = source.clock.generation;
    object(path, &serde_json::to_vec(&b).unwrap())
}
