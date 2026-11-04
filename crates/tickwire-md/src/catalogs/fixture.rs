//! Test-only approved directory generated from explicit feed economics.
use crate::config::{CatalogFile, Config, Feed};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path};
use tickwire_instrument_catalog::Catalog;

pub(crate) fn body(feeds: &[Feed]) -> Vec<u8> {
    let mut assets = BTreeMap::new();
    for feed in feeds {
        for asset in [
            &feed.instrument.base,
            &feed.instrument.quote,
            &feed.instrument.settlement,
        ] {
            assets.insert(asset.id, json!({"id":asset.id.to_string(),"version":asset.version.to_string(),"symbol":asset.symbol,"exponent":asset.exponent}));
        }
    }
    let first = &feeds[0];
    let spot = matches!(
        first.source.build(),
        tickwire_binance_public::Source::SpotProduction | tickwire_binance_public::Source::SpotTest
    );
    let production = matches!(
        first.source.build(),
        tickwire_binance_public::Source::SpotProduction
            | tickwire_binance_public::Source::UsdmProduction
    );
    let instruments = feeds.iter().map(|f| {
        let i = &f.instrument;
        let (product,contract) = if spot { ("Spot",json!({"kind":"Cash"})) } else if let Some(expiry) = i.expiry_utc_ns { ("LinearFuture", json!({"kind":"Delivery","expiry_utc_ns":expiry.to_string()})) } else { ("LinearPerpetual",json!({"kind":"Perpetual"})) };
        json!({"id":i.id.to_string(),"version":i.version.to_string(),"symbol":i.symbol,"product":product,"base":i.base.id.to_string(),"quote":i.quote.id.to_string(),"settlement":i.settlement.id.to_string(),"price_tick":i.price_tick,"quantity_step":i.quantity_step,"contract":contract})
    }).collect::<Vec<Value>>();
    serde_json::to_vec(&json!({"schema":1,"venue":first.venue_id,"environment":if production {"Production"} else {"Test"},"market":if spot {"Spot"} else {"Usdm"},"catalog_version":first.instrument.catalog_version.to_string(),"assets":assets.values().collect::<Vec<_>>(),"instruments":instruments})).unwrap()
}
pub(crate) fn approve(config: &mut Config, path: &Path, raw: &[u8]) {
    let digest: [u8; 32] = Sha256::digest(raw).into();
    let catalog = Catalog::decode(raw, digest).unwrap();
    for feed in &mut config.feeds {
        feed.instrument.catalog_hash = catalog.catalog_hash();
    }
    config.catalogs = Some(vec![CatalogFile {
        reference: format!(
            "artifact://fixture/catalog@sha256:{}",
            digest
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        ),
        path: path.to_owned(),
    }]);
}
pub(crate) fn install(config: &mut Config, path: &Path) -> Vec<u8> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let raw = body(&config.feeds);
    approve(config, path, &raw);
    std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .unwrap()
        .write_all(&raw)
        .unwrap();
    raw
}
