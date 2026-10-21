use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tickwire_config::{Candidate, Error, Format, Limits, artifacts::*};

fn reference(bytes: &[u8]) -> String {
    let hex: String = Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("artifact://fixture/strategy/params@sha256:{hex}")
}
fn schema() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "$schema":"https://json-schema.org/draft/2020-12/schema",
        "type":"object","additionalProperties":false,
        "required":["spread_bps","quote_quantity","signal_window_events"],
        "$defs":{"positive":{"type":"integer","minimum":1,"maximum":2048}},
        "properties":{"spread_bps":{"type":"string"},"quote_quantity":{"type":"string"},
          "signal_window_events":{"$ref":"#/$defs/positive"}}
    }))
    .unwrap()
}
fn candidate(reference: &str, change: impl FnOnce(&mut Value)) -> Candidate {
    let mut value = Candidate::parse(
        Format::Toml,
        include_bytes!("../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap()
    .value()
    .clone();
    for strategy in value["strategies"].as_array_mut().unwrap() {
        strategy["parameter_schema_ref"] = reference.into();
    }
    change(&mut value);
    Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&value).unwrap(),
        Limits::default(),
    )
    .unwrap()
}
fn catalog() -> ArtifactCatalog {
    ArtifactCatalog::new(CatalogLimits::default()).unwrap()
}

#[test]
fn content_pin_checks_complete_bytes_and_exact_retry_before_capacity() {
    let bytes = b"abc";
    let reference = reference(bytes);
    let mut c = ArtifactCatalog::new(CatalogLimits {
        max_objects: 1,
        max_object_bytes: 3,
        max_total_bytes: 3,
    })
    .unwrap();
    c.insert(&reference, bytes).unwrap();
    c.insert(&reference, bytes).unwrap();
    assert_eq!(c.get(&reference), Some(bytes.as_slice()));
    assert_eq!(c.stored_bytes(), 3);
    assert!(c.insert(&reference, b"abd").is_err());
    assert_eq!(
        c.insert(&crate::reference(b"x"), b"x"),
        Err(Error::Capacity)
    );
    assert_eq!(c.get(&reference), Some(bytes.as_slice()));
}
#[test]
fn invalid_reference_limits_and_oversize_objects_are_rejected() {
    let mut c = catalog();
    for value in [
        "https://example.test/x",
        "artifact://X/a@sha256:abc",
        "artifact://fixture/../x@sha256:abc",
        "artifact://fixture/a?b@sha256:abc",
    ] {
        assert!(c.insert(value, b"x").is_err());
    }
    for limits in [
        CatalogLimits {
            max_objects: 0,
            ..CatalogLimits::default()
        },
        CatalogLimits {
            max_object_bytes: 17 * 1024 * 1024,
            ..CatalogLimits::default()
        },
        CatalogLimits {
            max_total_bytes: 513 * 1024 * 1024,
            ..CatalogLimits::default()
        },
    ] {
        assert!(ArtifactCatalog::new(limits).is_err());
    }
    let mut c = ArtifactCatalog::new(CatalogLimits {
        max_object_bytes: 2,
        ..CatalogLimits::default()
    })
    .unwrap();
    assert_eq!(c.insert(&reference(b"abc"), b"abc"), Err(Error::Capacity));
}
#[test]
fn dependency_inventory_is_complete_unique_and_separates_credentials() {
    let bytes = schema();
    let r = reference(&bytes);
    let candidate = candidate(&r, |_| {});
    let mut c = catalog();
    c.insert(&r, &bytes).unwrap();
    let report = DependencyReport::inspect(&candidate, &c).unwrap();
    assert_eq!(report.candidate_hash, candidate.hash());
    assert!(report.missing().count() > 10);
    let p = report.artifacts.iter().find(|d| d.reference == r).unwrap();
    assert!(p.available);
    assert_eq!(p.uses.len(), 32);
    assert!(
        p.uses
            .contains(&"/strategies/0/parameter_schema_ref".to_owned())
    );
    assert!(
        report
            .secrets
            .contains_key("secret://fixture/spot/execution/1")
    );
    assert!(
        !report
            .artifacts
            .iter()
            .any(|d| d.reference.starts_with("secret:"))
    );
    assert!(
        report
            .artifacts
            .windows(2)
            .all(|p| p[0].reference < p[1].reference)
    );
}
#[test]
fn actual_parameter_schema_validates_every_strategy_and_binds_candidate() {
    let bytes = schema();
    let r = reference(&bytes);
    let candidate = candidate(&r, |_| {});
    let mut c = catalog();
    c.insert(&r, &bytes).unwrap();
    let validated = validate_parameters(&candidate, &c).unwrap();
    assert_eq!(validated.candidate_hash(), candidate.hash());
    assert_eq!(validated.bindings().len(), 32);
    assert_eq!(
        validated.bindings()[0].schema_hash,
        <[u8; 32]>::from(Sha256::digest(&bytes))
    );
}
#[test]
fn invalid_parameter_and_missing_schema_are_distinct_failures() {
    let bytes = schema();
    let r = reference(&bytes);
    let valid = candidate(&r, |_| {});
    assert_eq!(
        validate_parameters(&valid, &catalog()).unwrap_err(),
        Error::Semantic("missing parameter schema")
    );
    let mut c = catalog();
    c.insert(&r, &bytes).unwrap();
    let invalid = candidate(&r, |v| {
        v["strategies"][31]["params"]["signal_window_events"] = 4096.into()
    });
    assert_eq!(
        validate_parameters(&invalid, &c).unwrap_err(),
        Error::Semantic("strategy parameters")
    );
    let extra = candidate(&r, |v| v["strategies"][0]["params"]["extra"] = true.into());
    assert_eq!(
        validate_parameters(&extra, &c).unwrap_err(),
        Error::Semantic("strategy parameters")
    );
}
#[test]
fn external_refs_wrong_draft_and_invalid_schema_do_not_pass_offline_validation() {
    for value in [
        json!({"$ref":"https://127.0.0.1:1/schema"}),
        json!({"$ref":"file:///etc/passwd"}),
        json!({"$dynamicRef":"other.json#x"}),
        json!({"$schema":"http://json-schema.org/draft-07/schema#","type":"object"}),
        json!({"type":17}),
    ] {
        let bytes = serde_json::to_vec(&value).unwrap();
        let r = reference(&bytes);
        let mut c = catalog();
        c.insert(&r, &bytes).unwrap();
        assert!(validate_parameters(&candidate(&r, |_| {}), &c).is_err());
    }
}
#[test]
fn strict_schema_rejects_duplicate_keys_and_schema_size_limit() {
    for bytes in [
        br#"{"type":"object","type":"object"}"#.to_vec(),
        vec![b' '; 1024 * 1024 + 1],
    ] {
        let r = reference(&bytes);
        let mut c = catalog();
        c.insert(&r, &bytes).unwrap();
        assert!(validate_parameters(&candidate(&r, |_| {}), &c).is_err());
    }
}

#[test]
fn parameter_patterns_use_bounded_linear_regex_engine() {
    let bytes = serde_json::to_vec(&json!({
        "type":"object",
        "properties":{"spread_bps":{"type":"string","pattern":"^(?=2)"}}
    }))
    .unwrap();
    let r = reference(&bytes);
    let mut c = catalog();
    c.insert(&r, &bytes).unwrap();
    assert!(validate_parameters(&candidate(&r, |_| {}), &c).is_err());
}
