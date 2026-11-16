use std::path::PathBuf;

use serde_json::json;
use sha2::{Digest, Sha256};
use tickwire_app::{CatalogVerificationConfig, OrsAppConfig, inspect_ors_config};
use tickwire_control_sys::PrivateDirectory;

fn sample_catalog_bytes(version: u64) -> Vec<u8> {
    let doc = json!({
        "schema": 1,
        "venue": 1,
        "environment": "Production",
        "market": "Spot",
        "catalog_version": version.to_string(),
        "assets": [
            { "id": "1", "version": "1", "symbol": "BTC", "exponent": -8 },
            { "id": "2", "version": "1", "symbol": "USDT", "exponent": -8 }
        ],
        "instruments": [
            {
                "id": "1001",
                "version": "1",
                "symbol": "BTCUSDT",
                "product": "Spot",
                "base": "1",
                "quote": "2",
                "settlement": "2",
                "price_tick": "0.01",
                "quantity_step": "0.00001",
                "contract": { "kind": "Cash" }
            }
        ]
    });
    serde_json::to_vec_pretty(&doc).unwrap()
}

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[test]
fn ors_cold_startup_verifies_catalog_digest_and_version() {
    let dir = PrivateDirectory::create_in(&std::env::temp_dir(), "tw-app-cat").unwrap();
    let cat_path: PathBuf = dir.path().join("catalog.json");
    let cat_bytes = sample_catalog_bytes(7);
    let cat_hex = hex_sha256(&cat_bytes);
    std::fs::write(&cat_path, &cat_bytes).unwrap();

    let example_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("shared-market-strategies.json");
    let mut cfg = OrsAppConfig::from_file(&example_path).unwrap();
    cfg.instrument_catalog = Some(CatalogVerificationConfig {
        path: cat_path.clone(),
        expected_catalog_version: Some(7),
        expected_sha256_hex: Some(cat_hex),
    });

    let summary = inspect_ors_config(&cfg).unwrap();
    assert_eq!(summary.venue, "binance");
    assert_eq!(summary.catalog_version, Some(7));
    assert_eq!(summary.max_order_age_ns, 23_000);

    // Tampered SHA-256 digest must fail cold startup deterministically.
    cfg.instrument_catalog = Some(CatalogVerificationConfig {
        path: cat_path.clone(),
        expected_catalog_version: Some(7),
        expected_sha256_hex: Some("00".repeat(32)),
    });
    assert!(inspect_ors_config(&cfg).is_err());

    // Mismatched catalog_version must also fail cold startup deterministically.
    cfg.instrument_catalog = Some(CatalogVerificationConfig {
        path: cat_path,
        expected_catalog_version: Some(99),
        expected_sha256_hex: None,
    });
    assert!(inspect_ors_config(&cfg).is_err());
}
