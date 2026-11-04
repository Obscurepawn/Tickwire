use super::*;
use crate::config::CatalogFile;
#[test]
fn enabled_directory_requires_its_approved_body_before_any_owner() {
    let mut config: Config = serde_json::from_str(include_str!("../../examples/md.json")).unwrap();
    config.catalogs = Some(vec![CatalogFile {
        reference: format!(
            "artifact://fixture/spot/instruments@sha256:{}",
            "ab".repeat(32)
        ),
        path: "/this-directory-must-not-exist/tickwire-economic-catalog.json".into(),
    }]);
    assert!(
        Catalogs::load(&config).is_err(),
        "enabled catalog cannot be silently ignored"
    );
}
#[test]
fn valid_artifact_digest_cannot_authorize_another_price_tick() {
    let root =
        tickwire_control_sys::PrivateDirectory::create_in(std::path::Path::new("/tmp"), "md-cat")
            .unwrap();
    let mut config: Config = serde_json::from_str(include_str!("../../examples/md.json")).unwrap();
    let raw = fixture::install(&mut config, &root.path().join("catalog.json"));
    assert!(!raw.is_empty());
    config.feeds[0].instrument.price_tick = "0.5".into();
    assert!(
        Catalogs::load(&config).is_err(),
        "artifact SHA is not an economic binding"
    );
}
#[test]
fn complete_economics_and_source_are_bound_to_fixed_content() {
    let root =
        tickwire_control_sys::PrivateDirectory::create_in(std::path::Path::new("/tmp"), "md-cat")
            .unwrap();
    let mut config: Config = serde_json::from_str(include_str!("../../examples/md.json")).unwrap();
    let raw = fixture::install(&mut config, &root.path().join("catalog.json"));
    let fixed = Catalogs::load(&config).unwrap();
    assert_eq!(fixed.objects(), [raw]);
    let original = serde_json::to_value(&config).unwrap();
    for (pointer, value) in [
        ("/feeds/0/venue_id", serde_json::json!(9)),
        ("/feeds/0/source", serde_json::json!("spot_test")),
        ("/feeds/0/instrument/id", serde_json::json!(9)),
        ("/feeds/0/instrument/version", serde_json::json!(2)),
        ("/feeds/0/instrument/catalog_version", serde_json::json!(2)),
        ("/feeds/0/instrument/symbol", serde_json::json!("BTCUSDC")),
        ("/feeds/0/instrument/base/exponent", serde_json::json!(-7)),
        ("/feeds/0/instrument/base/version", serde_json::json!(2)),
        (
            "/feeds/0/instrument/quote/symbol",
            serde_json::json!("USDC"),
        ),
        ("/feeds/0/instrument/settlement/id", serde_json::json!(1)),
        (
            "/feeds/0/instrument/quantity_step",
            serde_json::json!("0.5"),
        ),
        ("/feeds/0/instrument/expiry_utc_ns", serde_json::json!(100)),
    ] {
        let mut changed = original.clone();
        assert_ne!(
            changed.pointer(pointer).unwrap(),
            &value,
            "fixture must actually change {pointer}"
        );
        *changed.pointer_mut(pointer).unwrap() = value;
        let changed: Config = serde_json::from_value(changed).unwrap();
        assert!(fixed.validate(&changed).is_err(), "{pointer}");
    }
    let mut changed = config.clone();
    changed.catalogs = None;
    assert!(fixed.validate(&changed).is_err());
    let mut changed = config.clone();
    changed.catalogs.as_mut().unwrap()[0].path = root.path().join("other");
    assert!(fixed.validate(&changed).is_err());
}
#[test]
fn retained_content_does_not_reread_paths_and_rejects_missing_or_tampered_dependencies() {
    let root =
        tickwire_control_sys::PrivateDirectory::create_in(std::path::Path::new("/tmp"), "md-cat")
            .unwrap();
    let mut config: Config = serde_json::from_str(include_str!("../../examples/md.json")).unwrap();
    let path = root.path().join("catalog.json");
    let raw = fixture::install(&mut config, &path);
    let fixed = Catalogs::load(&config).unwrap();
    std::fs::remove_file(path).unwrap();
    assert!(Catalogs::load(&config).is_err());
    assert!(fixed.validate(&config).is_ok());
    assert!(Catalogs::from_objects(&config, std::slice::from_ref(&raw)).is_ok());
    assert!(Catalogs::from_objects(&config, &[]).is_err());
    let mut changed = raw;
    changed.push(b' ');
    assert!(Catalogs::from_objects(&config, &[changed]).is_err());
    config.catalogs = None;
    assert!(Catalogs::from_objects(&config, &fixed.objects()).is_err());
}
#[test]
fn approval_and_aggregate_budgets_are_strict() {
    let root =
        tickwire_control_sys::PrivateDirectory::create_in(std::path::Path::new("/tmp"), "md-cat")
            .unwrap();
    let mut config: Config = serde_json::from_str(include_str!("../../examples/md.json")).unwrap();
    let raw = fixture::install(&mut config, &root.path().join("catalog.json"));
    let approved = config.catalogs.clone().unwrap();
    for files in [
        vec![],
        vec![approved[0].clone(); 2],
        vec![approved[0].clone(); 33],
    ] {
        config.catalogs = Some(files);
        assert!(Catalogs::from_objects(&config, std::slice::from_ref(&raw)).is_err());
    }
    config.catalogs = Some(approved);
    assert!(Catalogs::from_objects(&config, &[vec![b' '; MAX_BYTES + 1]]).is_err());
    config.catalogs.as_mut().unwrap()[0].path = "relative.json".into();
    assert!(Catalogs::from_objects(&config, std::slice::from_ref(&raw)).is_err());
}
#[test]
fn total_raw_budget_applies_across_valid_individual_documents() {
    use sha2::{Digest, Sha256};
    let root =
        tickwire_control_sys::PrivateDirectory::create_in(std::path::Path::new("/tmp"), "md-cat")
            .unwrap();
    let mut config: Config = serde_json::from_str(include_str!("../../examples/md.json")).unwrap();
    let mut first = fixture::body(&config.feeds);
    first.resize(MAX_BYTES / 2, b' ');
    fixture::approve(&mut config, &root.path().join("one.json"), &first);
    let mut future = config.clone();
    for feed in &mut future.feeds {
        feed.instrument.catalog_version += 1;
    }
    let mut second = fixture::body(&future.feeds);
    second.resize(MAX_BYTES / 2, b' ');
    fixture::approve(&mut future, &root.path().join("two.json"), &second);
    config
        .catalogs
        .as_mut()
        .unwrap()
        .push(future.catalogs.unwrap().pop().unwrap());
    for (approved, raw) in config
        .catalogs
        .as_ref()
        .unwrap()
        .iter()
        .zip([&first, &second])
    {
        crate::config::write_bytes(&approved.path, raw).unwrap();
    }
    assert!(Catalogs::load(&config).is_ok());
    assert!(Catalogs::from_objects(&config, &[first.clone(), second.clone()]).is_ok());
    second.push(b' ');
    let digest: [u8; 32] = Sha256::digest(&second).into();
    tickwire_instrument_catalog::Catalog::decode(&second, digest).unwrap();
    let approved = &mut config.catalogs.as_mut().unwrap()[1];
    approved.reference = format!(
        "artifact://fixture/two@sha256:{}",
        digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    crate::config::write_bytes(&approved.path, &second).unwrap();
    assert!(Catalogs::load(&config).is_err());
    assert!(Catalogs::from_objects(&config, &[first, second]).is_err());
}
