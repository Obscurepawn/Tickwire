//! Stateless configuration projection, cgroup/CPU resource validation, and multi-venue
//! `UnifiedVenueSession` (`Binance`, `Bybit`, `Okx`) tests with zero local WAL or disk journal
//! assumptions.

#![forbid(unsafe_code)]

use serde_json::json;
use tickwire_app::{OrsAppConfig, inspect_ors_config, spawn_unified_ors_process};
use tickwire_control_sys::{CpuSet, OrsSignal};
use tickwire_runtime::OrsSignalOutcome;
use tickwire_strategy_host::ProcessResources;

#[test]
fn reference_stateless_topology_and_unified_venue_selector_validate_cleanly() {
    // Verify `config/reference.toml` parses as stateless reference config without journal/WAL assumptions.
    let ref_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("config")
        .join("reference.toml");
    let raw_toml = std::fs::read(&ref_path).unwrap();
    let candidate = tickwire_config::Candidate::parse(
        tickwire_config::Format::Toml,
        &raw_toml,
        tickwire_config::Limits::default(),
    )
    .unwrap();
    let val = candidate.value();
    assert!(val["strategies"].is_array());

    // Verify self-contained static JSON `OrsAppConfig` across all three supported venues:
    // 1. Binance (Spot + USD-M multi-account `"spot-main"` + `"usdm-main"`):
    let binance_doc = json!({
        "venue": "binance",
        "environment": "test",
        "allowed_strategies": [1],
        "account_manager": {
            "enabled_accounts": ["spot_main", "usdm_main"],
            "spot_main": {
                "account_ref": 1,
                "exchange_account_id": 1001,
                "strategies": [1],
                "local_ip": "127.0.0.1",
                "credentials": { "key": "k-spot", "secret": "s-spot" },
                "urls": {
                    "binance-spot-order": { "local_ip/connection_count": [["127.0.0.1", 1]] },
                    "binance-usdm-order": { "local_ip/connection_count": [["127.0.0.1", 1]] }
                }
            },
            "usdm_main": {
                "account_ref": 2,
                "exchange_account_id": 1002,
                "strategies": [1],
                "local_ip": "127.0.0.1",
                "credentials": { "key": "k-usdm", "secret": "s-usdm" },
                "urls": {
                    "binance-spot-order": { "local_ip/connection_count": [["127.0.0.1", 1]] },
                    "binance-usdm-order": { "local_ip/connection_count": [["127.0.0.1", 1]] }
                }
            }
        },
        "binance_markets": [
            { "market": "spot" },
            { "market": "usdm" }
        ],
        "binance_symbols": [
            {
                "symbol_id": 0,
                "market": "spot",
                "venue_symbol": "BTCUSDT",
                "base_asset_id": 1,
                "base_scale": 8,
                "quote_asset_id": 2,
                "quote_scale": 8,
                "settlement_asset_id": 2,
                "settlement_scale": 8,
                "price_tick_atoms": 1000000,
                "price_scale": 8,
                "qty_step_atoms": 1000,
                "qty_scale": 8
            },
            {
                "symbol_id": 1,
                "market": "usdm",
                "venue_symbol": "BTCUSDT",
                "wallet_id": 1,
                "base_asset_id": 1,
                "base_scale": 8,
                "quote_asset_id": 2,
                "quote_scale": 8,
                "settlement_asset_id": 2,
                "settlement_scale": 8,
                "price_tick_atoms": 1000000,
                "price_scale": 8,
                "qty_step_atoms": 100000,
                "qty_scale": 8,
                "initial_margin_bps": 1000
            }
        ],
        "binance_assets": [
            { "market": "spot", "asset_code": "BTC", "wallet_id": 0, "asset_id": 1, "scale": 8 },
            { "market": "spot", "asset_code": "USDT", "wallet_id": 0, "asset_id": 2, "scale": 8 },
            { "market": "usdm", "asset_code": "USDT", "wallet_id": 1, "asset_id": 2, "scale": 8 }
        ]
    });
    let binance_cfg: OrsAppConfig = serde_json::from_value(binance_doc).unwrap();
    let binance_inspect = inspect_ors_config(&binance_cfg).unwrap();
    assert_eq!(binance_inspect.venue, "binance");
    assert_eq!(binance_inspect.account_count, 2);
    assert_eq!(binance_inspect.symbol_count, 2);

    // 2. Bybit UTA & OKX UTA unified venue selector spawn + clean SIGTERM shutdown:
    for venue_name in ["bybit", "okx"] {
        let url_id = format!("{venue_name}-uta-order");
        let uta_doc = json!({
            "venue": venue_name,
            "environment": "test",
            "uta_transport": "ws",
            "allowed_strategies": [1],
            "account_manager": {
                "enabled_accounts": ["uta_acct"],
                "uta_acct": {
                    "account_ref": 1,
                    "exchange_account_id": 5001,
                    "strategies": [1],
                    "local_ip": "127.0.0.1",
                    "credentials": { "key": "ukey", "secret": "usecret", "passphrase": "upass" },
                    "urls": {
                        url_id: { "local_ip/connection_count": [["127.0.0.1", 1]] }
                    }
                }
            },
            "uta_symbols": [
                {
                    "symbol_id": 0,
                    "symbol": if venue_name == "bybit" { "BTCUSDT" } else { "BTC-USDT" },
                    "product": "spot",
                    "base_asset_id": 1,
                    "base_scale": 8,
                    "quote_asset_id": 2,
                    "quote_scale": 8,
                    "settlement_asset_id": 2,
                    "settlement_scale": 8,
                    "price_tick_atoms": 1000000,
                    "price_scale": 8,
                    "qty_step_atoms": 100000,
                    "qty_scale": 8
                }
            ],
            "uta_assets": [
                { "currency": "BTC", "asset_id": 1, "scale": 8 },
                { "currency": "USDT", "asset_id": 2, "scale": 8 }
            ]
        });
        let uta_cfg: OrsAppConfig = serde_json::from_value(uta_doc).unwrap();
        let handle = spawn_unified_ors_process(&uta_cfg, false).unwrap();
        assert_eq!(
            handle.process.handle_signal(OrsSignal::SigTerm),
            OrsSignalOutcome::ShutdownInitiated
        );
    }

    // Verify `CpuSet` disjointness check rejects overlapping hot/housekeeping CPUs.
    let bad_cpu = tickwire_runtime::OrsCpuConfig {
        req_cpu: Some(0),
        resp_cpu: None,
        housekeeping_cpus: Some(CpuSet::single(0).unwrap()),
        epoll_wait_ms: 0,
    };
    assert!(bad_cpu.validate().is_err());
    let _ = std::mem::size_of::<ProcessResources>();
}
