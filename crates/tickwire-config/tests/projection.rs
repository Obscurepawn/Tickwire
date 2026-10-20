use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tickwire_config::{
    Candidate, Format, Limits,
    projection::{PinnedPolicy, project},
};

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
