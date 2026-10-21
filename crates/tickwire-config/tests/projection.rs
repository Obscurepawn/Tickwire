use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tickwire_config::{
    Candidate, Format, Limits,
    materialized::{Kind, MAX_FILE_BYTES, ProjectionFile, Stamp},
    projection::{PinnedPolicy, project},
    projection_object::ProjectionObject,
};

fn stamp(kind: Kind) -> Stamp {
    Stamp {
        kind,
        operation_id: "10000000-0000-4000-8000-000000000001".into(),
        control_commit_ref: format!(
            "artifact://control/commits/config-41@sha256:{}",
            "12".repeat(32)
        ),
        process_boot_id: if kind == Kind::Desired {
            "00000000-0000-0000-0000-000000000000".into()
        } else {
            "20000000-0000-4000-8000-000000000002".into()
        },
        applied_event_seq: if kind == Kind::Desired { 0 } else { 1234 },
        journal_receipt_ref: format!(
            "artifact://agent/journal/config-41@sha256:{}",
            "34".repeat(32)
        ),
        instance_slot_id: "30000000-0000-4000-8000-000000000003".into(),
    }
}
#[test]
fn materialization_round_trip_binds_payload_metadata_and_actual_toml_bytes() {
    let object = ProjectionObject::build(41, &strategy_projection(), 4 * 1024 * 1024).unwrap();
    for kind in [Kind::Desired, Kind::Effective] {
        let stamp = stamp(kind);
        let file = ProjectionFile::render(&object, &stamp, MAX_FILE_BYTES).unwrap();
        let independent: toml::Value =
            toml::from_str(std::str::from_utf8(file.bytes()).unwrap()).unwrap();
        let v = serde_json::to_value(independent).unwrap();
        assert_eq!(v["payload"], *object.payload());
        assert_eq!(v["_materialization"]["revision"], "41");
        assert_eq!(v["_materialization"]["operation_id"], stamp.operation_id);
        assert_eq!(
            v["_materialization"]["applied_event_seq"],
            stamp.applied_event_seq.to_string()
        );
        assert_eq!(
            v["_materialization"]["process_boot_id"],
            stamp.process_boot_id
        );
        let hash: [u8; 32] = Sha256::digest(file.bytes()).into();
        assert_eq!(hash, file.render_hash());
        assert_ne!(hash, object.binding().projection_hash);
        let recovered =
            ProjectionFile::decode(file.bytes(), &object, &stamp, hash, MAX_FILE_BYTES).unwrap();
        assert_eq!(recovered.value(), &v);
        assert_eq!(recovered.bytes(), file.bytes());
        assert_eq!(
            file.bytes(),
            ProjectionFile::render(&object, &stamp, MAX_FILE_BYTES)
                .unwrap()
                .bytes()
        );
        assert!(
            Candidate::parse(Format::Toml, file.bytes(), Limits::default())
                .unwrap()
                .validate_structural()
                .is_err()
        );
    }
}
#[test]
fn materialization_rejects_metadata_substitution_even_with_a_new_file_hash() {
    let object = ProjectionObject::build(41, &strategy_projection(), 4 * 1024 * 1024).unwrap();
    let stamp = stamp(Kind::Effective);
    let file = ProjectionFile::render(&object, &stamp, MAX_FILE_BYTES).unwrap();
    for (field, replacement) in [
        ("revision", json!("42")),
        (
            "global_config_hash",
            json!(format!("sha256:{}", "aa".repeat(32))),
        ),
        (
            "projection_hash",
            json!(format!("sha256:{}", "bb".repeat(32))),
        ),
        ("projection_policy_revision", json!("2")),
        ("target_process_id", json!("strategy-02-active")),
        ("projection_kind", json!("desired")),
        (
            "operation_id",
            json!("10000000-0000-4000-8000-000000000099"),
        ),
        (
            "control_commit_ref",
            json!(format!(
                "artifact://control/commits/other@sha256:{}",
                "12".repeat(32)
            )),
        ),
        (
            "process_boot_id",
            json!("20000000-0000-4000-8000-000000000099"),
        ),
        ("applied_event_seq", json!("1235")),
        (
            "journal_receipt_ref",
            json!(format!(
                "artifact://agent/journal/other@sha256:{}",
                "34".repeat(32)
            )),
        ),
        (
            "instance_slot_id",
            json!("30000000-0000-4000-8000-000000000099"),
        ),
        ("render_version", json!(2)),
    ] {
        let mut value = file.value().clone();
        value["_materialization"][field] = replacement;
        let bytes = toml::to_string(&value).unwrap().into_bytes();
        assert!(
            ProjectionFile::decode(
                &bytes,
                &object,
                &stamp,
                Sha256::digest(&bytes).into(),
                MAX_FILE_BYTES
            )
            .is_err(),
            "{field}"
        );
    }
}
#[test]
fn materialization_detects_payload_drift_raw_byte_drift_and_strict_shape_errors() {
    let object = ProjectionObject::build(41, &strategy_projection(), 4 * 1024 * 1024).unwrap();
    let stamp = stamp(Kind::Effective);
    let file = ProjectionFile::render(&object, &stamp, MAX_FILE_BYTES).unwrap();
    let mut bytes = b"# changed bytes\n".to_vec();
    bytes.extend_from_slice(file.bytes());
    assert!(
        ProjectionFile::decode(&bytes, &object, &stamp, file.render_hash(), MAX_FILE_BYTES)
            .is_err()
    );
    for kind in 0..5 {
        let mut value = file.value().clone();
        match kind {
            0 => value["payload"]["strategy"]["params"]["signal_window_events"] = json!(999),
            1 => value["_materialization"]["unexpected"] = json!(true),
            2 => value["unexpected"] = json!(true),
            3 => value["_materialization"]["applied_event_seq"] = json!("01234"),
            _ => value["payload"]["strategy"]["params"]["illegal_float"] = json!(1.5),
        }
        let bytes = toml::to_string(&value).unwrap().into_bytes();
        assert!(
            ProjectionFile::decode(
                &bytes,
                &object,
                &stamp,
                Sha256::digest(&bytes).into(),
                MAX_FILE_BYTES
            )
            .is_err()
        );
    }
    let mut bytes = b"projection_schema_version = 1\n".to_vec();
    bytes.extend_from_slice(file.bytes());
    assert!(
        ProjectionFile::decode(
            &bytes,
            &object,
            &stamp,
            Sha256::digest(&bytes).into(),
            MAX_FILE_BYTES
        )
        .is_err()
    );
}
#[test]
fn materialization_effective_requires_a_real_instance_and_nonzero_application_boundary() {
    let object = ProjectionObject::build(41, &strategy_projection(), 4 * 1024 * 1024).unwrap();
    for kind in 0..8 {
        let mut stamp = stamp(Kind::Effective);
        match kind {
            0 => stamp.applied_event_seq = 0,
            1 => stamp.applied_event_seq = 1 << 63,
            2 => stamp.process_boot_id = "00000000-0000-0000-0000-000000000000".into(),
            3 => stamp.operation_id = "00000000-0000-0000-0000-000000000000".into(),
            4 => stamp.instance_slot_id = "00000000-0000-0000-0000-000000000000".into(),
            5 => stamp.kind = Kind::Desired,
            6 => stamp.control_commit_ref = "/tmp/control-receipt".into(),
            _ => stamp.process_boot_id = "invalid-uuid".into(),
        }
        assert!(ProjectionFile::render(&object, &stamp, MAX_FILE_BYTES).is_err());
    }
    let mut stamp = stamp(Kind::Effective);
    stamp.applied_event_seq = (1 << 63) - 1;
    assert!(ProjectionFile::render(&object, &stamp, MAX_FILE_BYTES).is_ok());
}
#[test]
fn materialization_enforces_the_exact_file_budget() {
    let object = ProjectionObject::build(41, &strategy_projection(), 4 * 1024 * 1024).unwrap();
    let stamp = stamp(Kind::Effective);
    let file = ProjectionFile::render(&object, &stamp, MAX_FILE_BYTES).unwrap();
    assert!(ProjectionFile::render(&object, &stamp, file.bytes().len()).is_ok());
    for limit in [0, file.bytes().len() - 1, MAX_FILE_BYTES + 1] {
        assert!(ProjectionFile::render(&object, &stamp, limit).is_err());
        assert!(
            ProjectionFile::decode(file.bytes(), &object, &stamp, file.render_hash(), limit)
                .is_err()
        );
    }
}
#[test]
fn materialization_preserves_nested_parameter_values_and_large_exact_integers() {
    let (candidate, policy, _, _) = setup(grant(
        "strategy-01-active",
        &["spot-main", "usdm-main"],
        &["binance-spot", "binance-usdm"],
        &[1, 2],
        &[],
    ));
    let mut value = candidate.value().clone();
    value["strategies"][0]["params"]["nested"] = json!({
        "empty_object":{},
        "empty_array":[],
        "maximum":9_007_199_254_740_991_i64,
        "minimum":-9_007_199_254_740_991_i64,
        "text":"中文\n\"quote\"\\slash\t",
        "mixed":[true, 12, "0.00000001", {"threshold":"1.50"}, [3, false]]
    });
    let candidate = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&value).unwrap(),
        Limits::default(),
    )
    .unwrap();
    let projection = project(&candidate, "strategy-01-active", &policy).unwrap();
    let object = ProjectionObject::build(41, &projection, 4 * 1024 * 1024).unwrap();
    let stamp = stamp(Kind::Effective);
    let file = ProjectionFile::render(&object, &stamp, MAX_FILE_BYTES).unwrap();
    let parsed: toml::Value = toml::from_str(std::str::from_utf8(file.bytes()).unwrap()).unwrap();
    let parsed = serde_json::to_value(parsed).unwrap();
    assert_eq!(
        parsed["payload"]["strategy"]["params"],
        value["strategies"][0]["params"]
    );
}

fn strategy_projection() -> tickwire_config::projection::Projection {
    let (c, p, _, _) = setup(grant(
        "strategy-01-active",
        &["spot-main", "usdm-main"],
        &["binance-spot", "binance-usdm"],
        &[1, 2],
        &[],
    ));
    project(&c, "strategy-01-active", &p).unwrap()
}
#[test]
fn projection_object_binds_actual_payload_and_transport_identity() {
    let projection = strategy_projection();
    let object = ProjectionObject::build(41, &projection, 4 * 1024 * 1024).unwrap();
    let independent: Value = serde_json::from_slice(object.bytes()).unwrap();
    assert_eq!(independent["global_revision"], "41");
    assert_eq!(independent["projection_policy_revision"], "1");
    assert_eq!(independent["target_process_id"], "strategy-01-active");
    assert_eq!(independent["payload"], *projection.value());
    let hash: [u8; 32] = Sha256::digest(object.bytes()).into();
    assert_eq!(object.binding().object_hash, hash);
    assert_eq!(object.binding().projection_hash, projection.hash());
    assert_eq!(object.binding().global_hash, projection.global_hash());
    assert_ne!(
        object.binding().object_hash,
        object.binding().projection_hash
    );
    let decoded =
        ProjectionObject::decode(object.bytes(), object.binding(), object.bytes().len()).unwrap();
    assert_eq!(decoded.payload(), projection.value());
    assert_eq!(decoded.binding(), object.binding());
}
#[test]
fn projection_object_checks_all_expected_fields_and_exact_byte_limit() {
    let projection = strategy_projection();
    let object = ProjectionObject::build(41, &projection, 4 * 1024 * 1024).unwrap();
    for kind in 0..6 {
        let mut expected = object.binding().clone();
        match kind {
            0 => expected.revision += 1,
            1 => expected.global_hash[0] ^= 1,
            2 => expected.policy_revision += 1,
            3 => expected.process_id = "another-process".into(),
            4 => expected.projection_hash[0] ^= 1,
            _ => expected.object_hash[0] ^= 1,
        }
        assert!(ProjectionObject::decode(object.bytes(), &expected, 4 * 1024 * 1024).is_err());
    }
    assert!(ProjectionObject::build(41, &projection, object.bytes().len()).is_ok());
    for max in [0, object.bytes().len() - 1, 4 * 1024 * 1024 + 1] {
        assert!(ProjectionObject::build(41, &projection, max).is_err());
        assert!(ProjectionObject::decode(object.bytes(), object.binding(), max).is_err());
    }
    for revision in [0, 1 << 63, u64::MAX] {
        assert!(ProjectionObject::build(revision, &projection, 4 * 1024 * 1024).is_err());
    }
    assert!(ProjectionObject::build((1 << 63) - 1, &projection, 4 * 1024 * 1024).is_ok());
}
#[test]
fn projection_object_recomputes_payload_hash_and_checks_embedded_target() {
    let object = ProjectionObject::build(41, &strategy_projection(), 4 * 1024 * 1024).unwrap();
    for kind in 0..5 {
        let mut v: Value = serde_json::from_slice(object.bytes()).unwrap();
        let mut expected = object.binding().clone();
        match kind {
            0 => v["payload"]["strategy"]["params"]["signal_window_events"] = 999.into(),
            1 => {
                v["payload"]["process"]["id"] = "another-process".into();
                // Updating both digests cannot make the embedded process
                // disagree with the separately pinned target process.
                let payload = Candidate::parse(
                    Format::Json,
                    &serde_json::to_vec(&v["payload"]).unwrap(),
                    Limits::default(),
                )
                .unwrap();
                expected.projection_hash = payload.hash();
                v["projection_hash"] = format!(
                    "sha256:{}",
                    expected
                        .projection_hash
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                )
                .into();
            }
            2 => v["extra"] = true.into(),
            3 => v["global_revision"] = "9223372036854775808".into(),
            _ => {
                v["payload"].as_object_mut().unwrap().remove("process");
            }
        }
        let bytes = serde_json::to_vec(&v).unwrap();
        expected.object_hash = Sha256::digest(&bytes).into();
        assert!(
            ProjectionObject::decode(&bytes, &expected, 4 * 1024 * 1024).is_err(),
            "kind {kind}"
        );
    }
}
#[test]
fn projection_object_formatting_changes_only_the_transport_digest() {
    let object = ProjectionObject::build(41, &strategy_projection(), 4 * 1024 * 1024).unwrap();
    let v: Value = serde_json::from_slice(object.bytes()).unwrap();
    let pretty = serde_json::to_vec_pretty(&v).unwrap();
    assert!(ProjectionObject::decode(&pretty, object.binding(), 4 * 1024 * 1024).is_err());
    let mut binding = object.binding().clone();
    binding.object_hash = Sha256::digest(&pretty).into();
    let decoded = ProjectionObject::decode(&pretty, &binding, 4 * 1024 * 1024).unwrap();
    assert_eq!(decoded.payload(), object.payload());
    let mut duplicate = object.bytes().to_vec();
    duplicate.splice(1..1, b"\"projection_schema_version\":1,".iter().copied());
    binding.object_hash = Sha256::digest(&duplicate).into();
    assert!(ProjectionObject::decode(&duplicate, &binding, 4 * 1024 * 1024).is_err());
}

fn setup(target: Value) -> (Candidate, PinnedPolicy, Vec<u8>, String) {
    let bytes = serde_json::to_vec(&json!({"schema":1,"revision":1,"targets":[target]})).unwrap();
    let hash = Sha256::digest(&bytes);
    let digest: String = hash.iter().map(|b| format!("{b:02x}")).collect();
    let reference = format!("artifact://fixture/config/projection-v1@sha256:{digest}");
    let mut root = Candidate::parse(
        Format::Toml,
        include_bytes!("../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap()
    .value()
    .clone();
    root["local_config"]["projection_policy_ref"] = reference.clone().into();
    root["local_config"]["projection_policy_revision"] = 1.into();
    let c = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&root).unwrap(),
        Limits::default(),
    )
    .unwrap();
    let p = PinnedPolicy::decode(&reference, &bytes).unwrap();
    (c, p, bytes, reference)
}
fn grant(id: &str, accounts: &[&str], venues: &[&str], topics: &[u64], purposes: &[&str]) -> Value {
    json!({"process_id":id,"account_ids":accounts,"venue_ids":venues,"topic_ids":topics,"secret_purposes":purposes})
}
#[test]
fn strategy_receives_own_state_and_credential_free_views() {
    let (c, p, _, _) = setup(grant(
        "strategy-01-active",
        &["spot-main", "usdm-main"],
        &["binance-spot", "binance-usdm"],
        &[1, 2],
        &[],
    ));
    let out = project(&c, "strategy-01-active", &p).unwrap();
    assert_eq!(out.value()["strategy"]["logical_strategy_id"], 1);
    assert_eq!(out.value()["secret_bindings"], json!([]));
    let text = serde_json::to_string(out.value()).unwrap();
    assert!(!text.contains("secret://"));
    assert!(!text.contains("account_identity_ref"));
    assert!(out.value().get("control").is_none());
    assert!(out.value().get("replication").is_none());
    assert_eq!(
        out.hash(),
        project(&c, "strategy-01-active", &p).unwrap().hash()
    );
}
#[test]
fn execution_and_guardian_bind_only_their_distinct_account_credentials() {
    let (c, p, _, _) = setup(grant(
        "exec-spot-a",
        &["spot-main"],
        &["binance-spot"],
        &[1],
        &["execution"],
    ));
    let out = project(&c, "exec-spot-a", &p).unwrap();
    assert_eq!(
        out.value()["secret_bindings"],
        json!([{"purpose":"execution","ref":"secret://fixture/spot/execution/1","account_id":"spot-main"}])
    );
    assert_eq!(out.value()["account_views"].as_array().unwrap().len(), 1);
    assert!(!serde_json::to_string(out.value()).unwrap().contains("usdm"));
    let (c, p, _, _) = setup(grant(
        "guardian-a",
        &["spot-main", "usdm-main"],
        &["binance-spot", "binance-usdm"],
        &[],
        &["guardian"],
    ));
    let out = project(&c, "guardian-a", &p).unwrap();
    let bindings = serde_json::to_string(&out.value()["secret_bindings"]).unwrap();
    assert!(bindings.contains("/guardian/1"));
    assert!(!bindings.contains("/execution/1"));
}
#[test]
fn policy_cannot_grant_strategy_secrets_or_unassigned_execution_accounts() {
    let (c, p, _, _) = setup(grant(
        "strategy-01-active",
        &["spot-main", "usdm-main"],
        &["binance-spot", "binance-usdm"],
        &[1, 2],
        &["execution"],
    ));
    assert!(project(&c, "strategy-01-active", &p).is_err());
    let (c, p, _, _) = setup(grant(
        "exec-spot-a",
        &["spot-main", "usdm-main"],
        &["binance-spot", "binance-usdm"],
        &[1],
        &["execution"],
    ));
    assert!(project(&c, "exec-spot-a", &p).is_err());
}
#[test]
fn md_receives_only_owned_topics_and_market_data_credentials() {
    let (c, p, _, _) = setup(grant(
        "md-spot-a",
        &[],
        &["binance-spot"],
        &[1],
        &["market-data"],
    ));
    let out = project(&c, "md-spot-a", &p).unwrap();
    assert_eq!(out.value()["ipc"]["topics"].as_array().unwrap().len(), 1);
    assert_eq!(out.value()["secret_bindings"][0]["purpose"], "market-data");
    let (c, p, _, _) = setup(grant(
        "md-spot-a",
        &[],
        &["binance-usdm"],
        &[2],
        &["market-data"],
    ));
    assert!(project(&c, "md-spot-a", &p).is_err());
}
#[test]
fn policies_bind_exact_bytes_revision_and_process_and_reject_ambiguous_inputs() {
    let (c, p, bytes, reference) = setup(grant(
        "exec-spot-a",
        &["spot-main"],
        &["binance-spot"],
        &[1],
        &["execution"],
    ));
    let mut changed = bytes.clone();
    changed.push(b' ');
    assert!(PinnedPolicy::decode(&reference, &changed).is_err());
    assert!(project(&c, "exec-usdm-a", &p).is_err());
    let mut root = c.value().clone();
    root["local_config"]["projection_policy_revision"] = 2.into();
    let other = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&root).unwrap(),
        Limits::default(),
    )
    .unwrap();
    assert!(project(&other, "exec-spot-a", &p).is_err());
    for raw in [
        b"{\"schema\":1,\"schema\":1}".as_slice(),
        b"{\"schema\":1,\"revision\":1,\"targets\":[],\"extra\":true}",
    ] {
        let hash = Sha256::digest(raw);
        let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
        assert!(
            PinnedPolicy::decode(&format!("artifact://fixture/config/p@sha256:{hex}"), raw)
                .is_err()
        );
    }
}
#[test]
fn projection_hash_changes_with_own_parameters_and_not_unrelated_strategy() {
    let (c, p, _, _) = setup(grant(
        "strategy-01-active",
        &["spot-main", "usdm-main"],
        &["binance-spot", "binance-usdm"],
        &[1, 2],
        &[],
    ));
    let original = project(&c, "strategy-01-active", &p).unwrap().hash();
    let mut root = c.value().clone();
    root["strategies"][1]["params"]["signal_window_events"] = 2048.into();
    let other = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&root).unwrap(),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(
        original,
        project(&other, "strategy-01-active", &p).unwrap().hash()
    );
    root["strategies"][0]["params"]["signal_window_events"] = 2048.into();
    let other = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&root).unwrap(),
        Limits::default(),
    )
    .unwrap();
    assert_ne!(
        original,
        project(&other, "strategy-01-active", &p).unwrap().hash()
    );
}

#[test]
fn strategy_cannot_smuggle_a_secret_reference_through_a_parameter_value() {
    let (c, p, _, _) = setup(grant(
        "strategy-01-active",
        &["spot-main", "usdm-main"],
        &["binance-spot", "binance-usdm"],
        &[1, 2],
        &[],
    ));
    let mut root = c.value().clone();
    root["strategies"][0]["params"]["external_source"] = "secret://fixture/spot/execution/1".into();
    let c = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&root).unwrap(),
        Limits::default(),
    )
    .unwrap();
    assert!(project(&c, "strategy-01-active", &p).is_err());
}
