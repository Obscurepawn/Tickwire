use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tickwire_instrument_catalog::{Catalog, Market};
use tickwire_venue::{Environment, Product};

const BODY: &str = r#"{"schema":1,"venue":1,"environment":"Test","market":"Spot","catalog_version":"18446744073709551615","assets":[{"id":"3","version":"1","symbol":"BNB","exponent":-8},{"id":"2","version":"1","symbol":"USDT","exponent":-8},{"id":"1","version":"1","symbol":"BTC","exponent":-8}],"instruments":[{"id":"18446744073709551615","version":"18446744073709551615","symbol":"BTCUSDT","product":"Spot","base":"1","quote":"2","settlement":"2","price_tick":"0.01","quantity_step":"0.00001","contract":{"kind":"Cash"}}]}"#;

fn decode(bytes: &[u8]) -> tickwire_instrument_catalog::Result<Catalog> {
    Catalog::decode(bytes, Sha256::digest(bytes).into())
}

fn changed(f: impl FnOnce(&mut Value)) -> Vec<u8> {
    let mut value: Value = serde_json::from_str(BODY).unwrap();
    f(&mut value);
    serde_json::to_vec(&value).unwrap()
}

fn rejects(bytes: &[u8]) {
    assert!(
        decode(bytes).is_err(),
        "accepted {}",
        String::from_utf8_lossy(bytes)
    );
}

#[test]
fn full_u64_economic_binding_and_independent_fee_asset_are_available() {
    let c = decode(BODY.as_bytes()).expect("valid economic catalog");
    assert_eq!(c.catalog_version(), u64::MAX);
    assert_eq!(c.venue(), 1);
    assert_eq!(c.environment(), Environment::Test);
    assert_eq!(c.market(), Market::Spot);
    assert_eq!(
        c.assets().iter().map(|a| a.id).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(c.asset(3).unwrap().symbol.as_str(), "BNB");
    let i = c.instrument(u64::MAX).unwrap();
    assert_eq!(i.binding().instrument_version, u64::MAX);
    assert_eq!(i.binding().catalog_hash, c.catalog_hash());
    assert!(c.instrument_matches(Product::Spot, i.binding()));
    assert!(c.asset_matches(c.asset(3).unwrap()));
    assert_eq!(c.artifact().catalog().unwrap(), c);
}

#[test]
fn raw_artifact_identity_is_separate_from_canonical_economic_identity() {
    let first = decode(BODY.as_bytes()).unwrap();
    let mut value: Value = serde_json::from_str(BODY).unwrap();
    value["assets"].as_array_mut().unwrap().reverse();
    let raw = serde_json::to_vec_pretty(&value).unwrap();
    let second = decode(&raw).unwrap();
    assert_ne!(first.artifact_hash(), second.artifact_hash());
    assert_eq!(first.catalog_hash(), second.catalog_hash());
    assert_eq!(first.canonical_bytes(), second.canonical_bytes());
    assert_eq!(second.raw_bytes(), raw);
    assert_ne!(first.artifact_hash(), first.catalog_hash());
    let mut hasher = Sha256::new();
    hasher.update(b"tickwire.instrument-catalog.v1\0");
    hasher.update(first.canonical_bytes());
    assert_eq!(first.catalog_hash(), <[u8; 32]>::from(hasher.finalize()));
    assert!(Catalog::decode(BODY.as_bytes(), second.artifact_hash()).is_err());
}

#[test]
fn valid_rehashed_economic_changes_do_not_match_previous_bindings() {
    let old = decode(BODY.as_bytes()).unwrap();
    for field in ["price_tick", "quantity_step"] {
        let raw = changed(|v| v["instruments"][0][field] = json!("0.1"));
        let new = decode(&raw).unwrap();
        assert_ne!(new.catalog_hash(), old.catalog_hash());
        assert!(!new.instrument_matches(Product::Spot, old.instruments()[0].binding()));
    }
    let raw = changed(|v| v["assets"][0]["exponent"] = json!(-7));
    let new = decode(&raw).unwrap();
    assert!(!new.asset_matches(old.asset(3).unwrap()));
    assert!(!old.instrument_matches(Product::LinearPerpetual, old.instruments()[0].binding()));
    assert!(old.asset(0).is_none());
    assert!(old.instrument(0).is_none());
}

#[test]
fn every_binding_field_is_compared() {
    let catalog = decode(BODY.as_bytes()).unwrap();
    let original = catalog.instruments()[0].binding();
    for index in 0..12 {
        let mut value = original.clone();
        match index {
            0 => value.instrument_id -= 1,
            1 => value.instrument_version -= 1,
            2 => value.catalog_version -= 1,
            3 => value.catalog_hash[0] ^= 1,
            4 => value.symbol = tickwire_venue::Symbol::new("BTC_USDT").unwrap(),
            5 => value.base.version += 1,
            6 => value.quote.exponent -= 1,
            7 => value.settlement.symbol = tickwire_venue::AssetSymbol::new("USDC").unwrap(),
            8 => value.price_tick = "0.1".parse().unwrap(),
            9 => value.quantity_step = "0.1".parse().unwrap(),
            10 => value.contract = tickwire_venue::ContractKind::Perpetual,
            _ => value.base.id = 3,
        }
        assert!(
            !catalog.instrument_matches(Product::Spot, &value),
            "field {index}"
        );
    }
}

#[test]
fn full_u64_delivery_expiry_is_bound_and_products_are_explicit() {
    let raw = changed(|v| {
        v["market"] = json!("Usdm");
        v["instruments"][0]["product"] = json!("LinearFuture");
        v["instruments"][0]["contract"] =
            json!({"kind":"Delivery","expiry_utc_ns":u64::MAX.to_string()});
    });
    let catalog = decode(&raw).unwrap();
    assert_eq!(
        catalog.instruments()[0].binding().contract,
        tickwire_venue::ContractKind::Delivery {
            expiry_utc_ns: u64::MAX
        }
    );
    let raw = changed(|v| {
        v["market"] = json!("Usdm");
        v["instruments"][0]["product"] = json!("LinearPerpetual");
        v["instruments"][0]["contract"] = json!({"kind":"Perpetual"});
    });
    assert_eq!(
        decode(&raw).unwrap().instruments()[0].product(),
        Product::LinearPerpetual
    );
    for (market, product, contract) in [
        ("Spot", "Spot", json!({"kind":"Perpetual"})),
        ("Usdm", "Spot", json!({"kind":"Cash"})),
        (
            "Spot",
            "LinearFuture",
            json!({"kind":"Delivery","expiry_utc_ns":"1"}),
        ),
        (
            "Usdm",
            "LinearFuture",
            json!({"kind":"Delivery","expiry_utc_ns":"0"}),
        ),
        ("Usdm", "LinearFuture", json!({"kind":"Delivery"})),
        (
            "Usdm",
            "LinearPerpetual",
            json!({"kind":"Delivery","expiry_utc_ns":"1"}),
        ),
        ("Usdm", "InversePerpetual", json!({"kind":"Perpetual"})),
    ] {
        rejects(&changed(|v| {
            v["market"] = json!(market);
            v["instruments"][0]["product"] = json!(product);
            v["instruments"][0]["contract"] = contract;
        }));
    }
}

#[test]
fn unknown_fields_including_unit_contracts_are_rejected() {
    for pointer in ["", "/assets/0", "/instruments/0", "/instruments/0/contract"] {
        rejects(&changed(|v| {
            v.pointer_mut(pointer)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert("unknown".into(), json!(1));
        }));
    }
    rejects(&changed(|v| {
        v["instruments"][0]["contract"]["expiry_utc_ns"] = Value::Null
    }));
    for field in [
        "schema",
        "venue",
        "environment",
        "market",
        "catalog_version",
        "assets",
        "instruments",
    ] {
        rejects(&changed(|v| {
            v.as_object_mut().unwrap().remove(field);
        }));
    }
}

#[test]
fn duplicate_fields_at_every_level_and_escaped_duplicates_are_rejected() {
    for (needle, replacement) in [
        ("\"venue\":1", "\"venue\":1,\"venue\":1"),
        ("\"venue\":1", "\"venue\":1,\"ven\\u0075e\":1"),
        ("\"id\":\"3\"", "\"id\":\"3\",\"id\":\"3\""),
        (
            "\"price_tick\":\"0.01\"",
            "\"price_tick\":\"0.01\",\"price_tick\":\"0.01\"",
        ),
        ("\"kind\":\"Cash\"", "\"kind\":\"Cash\",\"kind\":\"Cash\""),
    ] {
        rejects(BODY.replace(needle, replacement).as_bytes());
    }
}

#[test]
fn identities_require_canonical_nonzero_full_width_decimal_strings() {
    for value in [
        json!("0"),
        json!("01"),
        json!("+1"),
        json!("-1"),
        json!("1e1"),
        json!("1.0"),
        json!("18446744073709551616"),
        json!(""),
        json!(" 1"),
        json!(1),
        json!(1.0),
        Value::Null,
    ] {
        for pointer in [
            "/catalog_version",
            "/assets/0/id",
            "/assets/0/version",
            "/instruments/0/id",
            "/instruments/0/version",
            "/instruments/0/base",
        ] {
            rejects(&changed(|v| {
                *v.pointer_mut(pointer).unwrap() = value.clone()
            }));
        }
    }
    for value in [
        json!(0),
        json!(-1),
        json!(4294967296u64),
        json!(1.0),
        json!("1"),
    ] {
        rejects(&changed(|v| v["venue"] = value));
    }
}

#[test]
fn decimal_steps_and_asset_metadata_are_exact() {
    for value in [
        "0",
        "-0.01",
        "00.1",
        "0.010",
        "1.0",
        ".1",
        "1.",
        "+1",
        "1e-2",
        "0.000000000000000000000000000000000000001",
        "170141183460469231731687303715884105728",
    ] {
        for field in ["price_tick", "quantity_step"] {
            rejects(&changed(|v| v["instruments"][0][field] = json!(value)));
        }
    }
    for value in [json!(-39), json!(39), json!(1.0), json!(128)] {
        rejects(&changed(|v| v["assets"][0]["exponent"] = value));
    }
    for value in ["", "BTC USDT", "BTC\n", "BTC\u{007f}"] {
        rejects(&changed(|v| v["instruments"][0]["symbol"] = json!(value)));
    }
    let unicode = changed(|v| v["instruments"][0]["symbol"] = json!("比特币USDT"));
    assert_eq!(
        decode(&unicode).unwrap().instruments()[0]
            .binding()
            .symbol
            .as_str(),
        "比特币USDT"
    );
}

#[test]
fn catalog_sets_are_nonempty_unique_and_all_asset_references_resolve() {
    for field in ["assets", "instruments"] {
        rejects(&changed(|v| v[field] = json!([])));
        rejects(&changed(|v| {
            let first = v[field][0].clone();
            v[field].as_array_mut().unwrap().push(first);
        }));
    }
    rejects(&changed(|v| v["assets"][0]["id"] = json!("2")));
    rejects(&changed(|v| v["assets"][0]["symbol"] = json!("USDT")));
    rejects(&changed(|v| {
        let mut next = v["instruments"][0].clone();
        next["id"] = json!("2");
        v["instruments"].as_array_mut().unwrap().push(next);
    }));
    for field in ["base", "quote", "settlement"] {
        rejects(&changed(|v| v["instruments"][0][field] = json!("99")));
    }
    rejects(&changed(|v| v["instruments"][0]["base"] = json!("2")));
}

#[test]
fn artifact_envelope_is_strict_and_verifies_raw_bytes() {
    let c = decode(BODY.as_bytes()).unwrap();
    let artifact = c.artifact();
    let bytes = serde_json::to_vec(&artifact).unwrap();
    assert_eq!(
        serde_json::from_slice::<tickwire_instrument_catalog::Artifact>(&bytes)
            .unwrap()
            .catalog()
            .unwrap(),
        c
    );
    let mut value = serde_json::to_value(&artifact).unwrap();
    value["extra"] = json!(0);
    assert!(serde_json::from_value::<tickwire_instrument_catalog::Artifact>(value).is_err());
    let duplicate = format!(
        "{{\"digest\":{},\"digest\":{},\"bytes\":[]}}",
        serde_json::to_string(&artifact.digest).unwrap(),
        serde_json::to_string(&artifact.digest).unwrap()
    );
    assert!(serde_json::from_str::<tickwire_instrument_catalog::Artifact>(&duplicate).is_err());
    let mut damaged = artifact;
    damaged.bytes[0] = b' ';
    assert!(damaged.catalog().is_err());
}

#[test]
fn byte_depth_and_collection_limits_are_enforced_before_acceptance() {
    let mut raw = BODY.as_bytes().to_vec();
    raw.resize(tickwire_instrument_catalog::MAX_BYTES, b' ');
    assert!(decode(&raw).is_ok());
    raw.push(b' ');
    assert_eq!(
        decode(&raw).unwrap_err(),
        tickwire_instrument_catalog::Error::Bounds
    );
    let nested = format!("{}0{}", "[".repeat(17), "]".repeat(17));
    assert_eq!(
        decode(nested.as_bytes()).unwrap_err(),
        tickwire_instrument_catalog::Error::Bounds
    );
    let asset_count = |count: usize| {
        changed(|v| {
            v["assets"]=Value::Array((1..=count).map(|i|json!({"id":i.to_string(),"version":"1","symbol":format!("A{i}"),"exponent":-8})).collect());
        })
    };
    assert!(decode(&asset_count(tickwire_instrument_catalog::MAX_ASSETS)).is_ok());
    rejects(&asset_count(tickwire_instrument_catalog::MAX_ASSETS + 1));
}

#[test]
fn truncation_trailing_invalid_unicode_and_unknown_enums_are_rejected() {
    for end in 0..BODY.len() {
        rejects(&BODY.as_bytes()[..end]);
    }
    rejects(format!("{BODY} null").as_bytes());
    rejects(BODY.replace("BNB", "\\ud800").as_bytes());
    let mut invalid = BODY.as_bytes().to_vec();
    let index = BODY.find("BNB").unwrap();
    invalid[index] = 0xff;
    rejects(&invalid);
    for field in ["environment", "market"] {
        rejects(&changed(|v| v[field] = json!("Unknown")));
    }
    rejects(&changed(|v| v["schema"] = json!(2)));
    rejects(&changed(|v| v["schema"] = json!(1.0)));
}

#[test]
fn enum_values_are_strings_and_contract_is_a_tagged_object() {
    for (pointer, value) in [
        ("/environment", json!({"Test":null})),
        ("/market", json!({"Spot":null})),
        ("/instruments/0/product", json!({"Spot":null})),
        ("/instruments/0/contract", json!("Cash")),
        ("/instruments/0/contract/kind", json!({"Cash":null})),
    ] {
        rejects(&changed(|v| *v.pointer_mut(pointer).unwrap() = value));
    }
}
