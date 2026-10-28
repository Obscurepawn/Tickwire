#[path = "plan/support.rs"]
mod support;
use support::*;
use tickwire_config::{
    Candidate, Format, Limits,
    delivery::{ProjectionDelivery, ProjectionLimits},
    plan::{ComputedPlan, EncodedPlan, PlanOptions},
};

#[test]
fn stored_plan_round_trip_keeps_options_and_recomputes_the_complete_plan() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let mut v = old.value().clone();
    v["observability"]["diagnostic_level"] = "debug".into();
    let next = parse(&v);
    let prepared = prepared(&old, &next);
    let delivery =
        ProjectionDelivery::prepare(&old, &prepared, &catalog, ProjectionLimits::default())
            .unwrap();
    let plan =
        ComputedPlan::compute(7, &old, &prepared, &delivery, PlanOptions::default()).unwrap();
    let encoded = EncodedPlan::decode(plan.canonical_bytes(), plan.digest()).unwrap();
    assert_eq!(encoded.base_revision(), 7);
    assert_eq!(encoded.next_revision(), 8);
    assert_eq!(encoded.base_hash(), old.hash());
    assert_eq!(encoded.candidate_hash(), next.hash());
    assert_eq!(encoded.base_etag(), plan.base_etag());
    assert_eq!(encoded.canonical_bytes(), plan.canonical_bytes());
    assert_eq!(encoded.digest(), plan.digest());
    let replay = ComputedPlan::verify_encoded(&encoded, 7, &old, &prepared, &delivery).unwrap();
    assert_eq!(replay.canonical_bytes(), plan.canonical_bytes());
    assert!(ComputedPlan::verify_encoded(&encoded, 8, &old, &prepared, &delivery).is_err());
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"tickwire.config.computed-plan.v1\0");
    h.update(bytes);
    h.finalize().into()
}
fn canonical(v: &serde_json::Value) -> Vec<u8> {
    Candidate::parse(
        Format::Json,
        &serde_json::to_vec(v).unwrap(),
        Limits::default(),
    )
    .unwrap()
    .canonical_bytes()
    .to_vec()
}

#[test]
fn strict_decoder_rejects_duplicate_fields_unknown_fields_and_noncanonical_bytes() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let p = prepared(&old, &old);
    let d = ProjectionDelivery::prepare(&old, &p, &catalog, ProjectionLimits::default()).unwrap();
    let plan = ComputedPlan::compute(1, &old, &p, &d, PlanOptions::default()).unwrap();
    let bytes = plan.canonical_bytes();
    EncodedPlan::decode(bytes, plan.digest()).unwrap();
    let mut extra = bytes.to_vec();
    extra.push(b' ');
    assert!(EncodedPlan::decode(&extra, digest(&extra)).is_err());
    let duplicate = std::str::from_utf8(bytes).unwrap().replacen(
        "\"schema\":1",
        "\"schema\":1,\"schema\":1",
        1,
    );
    assert!(EncodedPlan::decode(duplicate.as_bytes(), digest(duplicate.as_bytes())).is_err());
    let mut v: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    v["unexpected"] = true.into();
    let extra = canonical(&v);
    assert!(EncodedPlan::decode(&extra, digest(&extra)).is_err());
}

#[test]
fn compiled_rules_identity_is_nonzero_and_stable() {
    assert_ne!(tickwire_config::plan::rules_digest(), [0; 32]);
    assert_eq!(
        tickwire_config::plan::rules_digest(),
        tickwire_config::plan::rules_digest()
    );
}

fn fixture() -> (
    Candidate,
    tickwire_config::prepared::PreparedCandidate,
    ProjectionDelivery,
    ComputedPlan,
) {
    use tickwire_config::plan::ExposurePolicy;
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let mut v = old.value().clone();
    v["observability"]["diagnostic_level"] = "debug".into();
    v["control"]["strategy_suspect_after_ms"] = 900.into();
    let next = parse(&v);
    let prepared = prepared(&old, &next);
    let delivery =
        ProjectionDelivery::prepare(&old, &prepared, &catalog, ProjectionLimits::default())
            .unwrap();
    let plan = ComputedPlan::compute(
        7,
        &old,
        &prepared,
        &delivery,
        PlanOptions {
            exposure_policy: ExposurePolicy::CancelThenFreeze,
            requested_target_ids: ["exec-spot-a".into(), "guardian-a".into()].into(),
            ..Default::default()
        },
    )
    .unwrap();
    (old, prepared, delivery, plan)
}
fn rejected(value: &serde_json::Value) {
    let bytes = canonical(value);
    assert!(
        EncodedPlan::decode(&bytes, digest(&bytes)).is_err(),
        "accepted mutation: {value}"
    );
}

#[test]
fn missing_unknown_fields_and_enums_are_rejected_at_every_record_layer() {
    use serde_json::json;
    let (_, _, _, plan) = fixture();
    let original: serde_json::Value = serde_json::from_slice(plan.canonical_bytes()).unwrap();
    for path in [
        "",
        "/options",
        "/targets/guardian-a",
        "/targets/guardian-a/old_projection",
        "/targets/guardian-a/new_projection",
    ] {
        let mut v = original.clone();
        v.pointer_mut(path).unwrap()["unknown"] = true.into();
        rejected(&v);
        for key in original.pointer(path).unwrap().as_object().unwrap().keys() {
            let mut v = original.clone();
            v.pointer_mut(path)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(key);
            rejected(&v);
        }
    }
    for path in [
        "/options/apply_ceiling",
        "/options/exposure_policy",
        "/required_class",
        "/targets/guardian-a/action",
        "/targets/guardian-a/required_class",
        "/targets/guardian-a/preconditions/0",
        "/preconditions/0",
    ] {
        let mut v = original.clone();
        *v.pointer_mut(path).unwrap() = json!("FutureCapability");
        rejected(&v);
    }
    for path in ["/schema", "/classification_version"] {
        for version in [json!(0), json!(2), json!("1")] {
            let mut v = original.clone();
            *v.pointer_mut(path).unwrap() = version;
            rejected(&v);
        }
    }
}

#[test]
fn ordered_sets_cannot_be_reordered_or_silently_deduplicated() {
    let (_, _, _, plan) = fixture();
    let original: serde_json::Value = serde_json::from_slice(plan.canonical_bytes()).unwrap();
    for path in [
        "/options/requested_target_ids",
        "/affected_accounts",
        "/preconditions",
        "/targets/guardian-a/accounts",
        "/targets/guardian-a/changes",
        "/targets/guardian-a/preconditions",
    ] {
        assert!(
            original.pointer(path).unwrap().as_array().unwrap().len() > 1,
            "{path}"
        );
        let mut v = original.clone();
        v.pointer_mut(path)
            .unwrap()
            .as_array_mut()
            .unwrap()
            .reverse();
        rejected(&v);
        let mut v = original.clone();
        let rows = v.pointer_mut(path).unwrap().as_array_mut().unwrap();
        rows.insert(0, rows[0].clone());
        rejected(&v);
    }
}

#[test]
fn decimal_and_baseline_bindings_are_strict_without_reducing_the_revision_domain() {
    use serde_json::json;
    let (old, p, d, plan) = fixture();
    let original: serde_json::Value = serde_json::from_slice(plan.canonical_bytes()).unwrap();
    for path in [
        "/base_revision",
        "/next_revision",
        "/targets/guardian-a/old_projection/policy_revision",
        "/targets/guardian-a/new_projection/policy_revision",
    ] {
        for value in [
            json!("0"),
            json!("01"),
            json!("+7"),
            json!("-1"),
            json!("7.0"),
            json!("7e0"),
            json!("9223372036854775808"),
            json!("18446744073709551616"),
            json!(7),
            json!(""),
        ] {
            let mut v = original.clone();
            *v.pointer_mut(path).unwrap() = value;
            rejected(&v);
        }
    }
    for path in [
        "/base_hash",
        "/candidate_hash",
        "/targets/guardian-a/old_projection/projection_hash",
    ] {
        for value in [
            "sha256:AA",
            "sha256:111",
            &format!("sha256:{}", "A".repeat(64)),
            &format!("sha256:{}", "g".repeat(64)),
        ] {
            let mut v = original.clone();
            *v.pointer_mut(path).unwrap() = value.into();
            rejected(&v);
        }
    }
    let mut v = original.clone();
    v["next_revision"] = "9".into();
    rejected(&v);
    let mut v = original.clone();
    v["base_etag"] = "\"cfg-8-deadbeef\"".into();
    rejected(&v);
    let maximum = (1u64 << 63) - 2;
    let plan = ComputedPlan::compute(maximum, &old, &p, &d, PlanOptions::default()).unwrap();
    let encoded = EncodedPlan::decode(plan.canonical_bytes(), plan.digest()).unwrap();
    assert_eq!(encoded.base_revision(), maximum);
    ComputedPlan::verify_encoded(&encoded, maximum, &old, &p, &d).unwrap();
}

#[test]
fn recomputation_rejects_structurally_valid_forgery_even_with_a_matching_digest() {
    let (old, p, d, plan) = fixture();
    let original: serde_json::Value = serde_json::from_slice(plan.canonical_bytes()).unwrap();
    for kind in 0..5 {
        let mut v = original.clone();
        match kind {
            0 => {
                v["targets"].as_object_mut().unwrap().remove("md-spot-a");
            }
            1 => {
                v["targets"]["guardian-a"]["old_projection"]["projection_hash"] =
                    format!("sha256:{}", "a".repeat(64)).into()
            }
            2 => {
                v["requires_risk_admin"] =
                    (!original["requires_risk_admin"].as_bool().unwrap()).into()
            }
            3 => {
                v["targets"]["guardian-a"]["changes"][1] =
                    "/observability/sample_per_million".into()
            }
            _ => {
                v["targets"]["guardian-a"]["action"] = "Hot".into();
                v["targets"]["guardian-a"]["required_class"] = "Hot".into();
            }
        }
        let bytes = canonical(&v);
        let encoded = EncodedPlan::decode(&bytes, digest(&bytes)).unwrap();
        assert!(
            ComputedPlan::verify_encoded(&encoded, 7, &old, &p, &d).is_err(),
            "{kind}"
        );
    }
    let encoded = EncodedPlan::decode(plan.canonical_bytes(), plan.digest()).unwrap();
    assert!(ComputedPlan::verify_encoded(&encoded, 7, p.candidate(), &p, &d).is_err());
    let other = prepared(&old, &old);
    let mut catalog = catalog();
    let v = old.value().clone();
    let old_again = pinned(v.clone(), policy(&v), &mut catalog);
    assert_eq!(old_again.hash(), old.hash());
    let other_delivery =
        ProjectionDelivery::prepare(&old, &other, &catalog, ProjectionLimits::default()).unwrap();
    assert!(ComputedPlan::verify_encoded(&encoded, 7, &old, &other, &other_delivery).is_err());
}

#[test]
fn target_side_identity_and_required_runtime_guards_cannot_be_removed() {
    use serde_json::json;
    let (_, _, _, plan) = fixture();
    let original: serde_json::Value = serde_json::from_slice(plan.canonical_bytes()).unwrap();
    for (path, value) in [
        ("/targets", json!({})),
        ("/options/requested_target_ids", json!(["unknown-target"])),
        ("/targets/guardian-a/action", json!("Hot")),
        ("/targets/guardian-a/old_projection", json!(null)),
        ("/targets/guardian-a/new_host", json!(null)),
        (
            "/targets/guardian-a/old_projection/global_hash",
            json!(format!("sha256:{}", "f".repeat(64))),
        ),
        ("/required_class", json!("ForbiddenRuntime")),
    ] {
        let mut v = original.clone();
        *v.pointer_mut(path).unwrap() = value;
        rejected(&v);
    }
    let mut v = original.clone();
    v["targets"]["guardian-a"]["preconditions"]
        .as_array_mut()
        .unwrap()
        .retain(|p| p != "RuntimeCapability");
    rejected(&v);
}

#[test]
fn every_emitted_lifecycle_action_and_explicit_options_can_be_read_back() {
    use tickwire_config::plan::{ApplyCeiling, ExposurePolicy};
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    for kind in 0..5 {
        let mut v = old.value().clone();
        match kind {
            0 => {}
            1 => v["observability"]["diagnostic_level"] = "debug".into(),
            2 => v["strategies"][0]["params"]["signal_window_events"] = 2048.into(),
            3 => v["local_config"]["root_dir"] = "/var/lib/tickwire/other".into(),
            _ => {
                v["processes"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|p| p["id"] == "recorder-a")
                    .unwrap()["id"] = "recorder-new".into()
            }
        }
        let next = if kind == 4 {
            pinned(v.clone(), policy(&v), &mut catalog)
        } else {
            parse(&v)
        };
        let p = prepared(&old, &next);
        let d =
            ProjectionDelivery::prepare(&old, &p, &catalog, ProjectionLimits::default()).unwrap();
        for exposure_policy in [
            ExposurePolicy::RejectIfExceeded,
            ExposurePolicy::FreezeUntilWithin,
            ExposurePolicy::CancelThenFreeze,
        ] {
            let options = PlanOptions {
                apply_ceiling: if kind < 2 {
                    ApplyCeiling::Hot
                } else if kind == 2 {
                    ApplyCeiling::Quiesce
                } else {
                    ApplyCeiling::RollingRestart
                },
                exposure_policy,
                requested_target_ids: ["guardian-a".into()].into(),
            };
            let plan = ComputedPlan::compute(1, &old, &p, &d, options.clone()).unwrap();
            let encoded = EncodedPlan::decode(plan.canonical_bytes(), plan.digest()).unwrap();
            assert_eq!(encoded.options(), &options);
            ComputedPlan::verify_encoded(&encoded, 1, &old, &p, &d).unwrap();
        }
    }
}

#[test]
fn body_budget_depth_truncation_duplicate_escaped_keys_and_hash_mismatch_are_rejected() {
    let (_, _, _, plan) = fixture();
    let bytes = plan.canonical_bytes();
    assert!(EncodedPlan::decode(bytes, [0; 32]).is_err());
    for length in (0..bytes.len())
        .step_by((bytes.len() / 32).max(1))
        .chain([bytes.len() - 1])
    {
        assert!(EncodedPlan::decode(&bytes[..length], digest(&bytes[..length])).is_err());
    }
    let oversized = vec![b' '; tickwire_config::plan::MAX_BYTES + 1];
    assert!(EncodedPlan::decode(&oversized, digest(&oversized)).is_err());
    let deep = format!("{}0{}", "[".repeat(17), "]".repeat(17));
    assert!(EncodedPlan::decode(deep.as_bytes(), digest(deep.as_bytes())).is_err());
    let escaped = std::str::from_utf8(bytes).unwrap().replacen(
        "\"schema\":1",
        "\"schema\":1,\"sch\\u0065ma\":1",
        1,
    );
    assert!(EncodedPlan::decode(escaped.as_bytes(), digest(escaped.as_bytes())).is_err());
    let duplicate = std::str::from_utf8(bytes).unwrap().replacen(
        "\"action\":\"Quiesce\"",
        "\"action\":\"Quiesce\",\"action\":\"Quiesce\"",
        1,
    );
    assert_ne!(duplicate.as_bytes(), bytes);
    assert!(EncodedPlan::decode(duplicate.as_bytes(), digest(duplicate.as_bytes())).is_err());
}

#[test]
fn rules_identity_matches_the_declared_files_in_the_actual_workspace() {
    use sha2::{Digest, Sha256};
    let files = [
        ".cargo/config.toml",
        "Cargo.lock",
        "Cargo.toml",
        "crates/tickwire-config/Cargo.toml",
        "crates/tickwire-config/src/artifacts.rs",
        "crates/tickwire-config/src/changes.rs",
        "crates/tickwire-config/src/delivery.rs",
        "crates/tickwire-config/src/lib.rs",
        "crates/tickwire-config/src/patch.rs",
        "crates/tickwire-config/src/plan.rs",
        "crates/tickwire-config/src/plan/dependencies.rs",
        "crates/tickwire-config/src/plan/encoded.rs",
        "crates/tickwire-config/src/plan/rules.rs",
        "crates/tickwire-config/src/prepared.rs",
        "crates/tickwire-config/src/projection.rs",
        "crates/tickwire-config/src/strict.rs",
        "crates/tickwire-config/src/validation.rs",
        "rust-toolchain.toml",
        "specs/contracts/control-api.openapi.json",
        "specs/contracts/system-config.schema.json",
    ];
    assert!(files.windows(2).all(|p| p[0] < p[1]));
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let mut hash = Sha256::new();
    hash.update(b"tickwire.config.planning-rules.v1\0");
    hash.update((files.len() as u64).to_le_bytes());
    for path in files {
        let bytes = std::fs::read(root.join(path)).unwrap();
        hash.update((path.len() as u64).to_le_bytes());
        hash.update(path.as_bytes());
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    }
    assert_eq!(
        tickwire_config::plan::rules_digest(),
        <[u8; 32]>::from(hash.finalize())
    );
}
