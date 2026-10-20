use serde_json::{Value, json};
use tickwire_config::{
    Candidate, Format, Limits,
    changes::{Authority, ChangeSet, Class, Role, RoleGrant},
};
fn reference() -> Candidate {
    Candidate::parse(
        Format::Toml,
        include_bytes!("../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap()
}
fn candidate(v: &Value) -> Candidate {
    Candidate::parse(
        Format::Json,
        &serde_json::to_vec(v).unwrap(),
        Limits::default(),
    )
    .unwrap()
}
fn operator(accounts: &[&str]) -> Authority {
    Authority::from_grants(vec![RoleGrant {
        role: Role::Operator,
        accounts: accounts.iter().map(|s| s.to_string()).collect(),
        cluster_scope: false,
    }])
}
#[test]
fn lower_per_instrument_delivery_protection_requires_risk_admin() {
    let mut v = reference().value().clone();
    v["accounts"][1]["instrument_overrides"] = json!([{"symbol":v["accounts"][1]["allowed_symbols"][0],"contract_kind":"Delivery","expiry_epoch_ms":2000000000000u64,"delivery_guard_seconds":3600,"settlement_schedule_ref":v["risk_policies"][1]["settlement_schedule_ref"]}]);
    let old = candidate(&v);
    old.validate_static().unwrap();
    v["accounts"][1]["instrument_overrides"][0]["delivery_guard_seconds"] = 0.into();
    let new = candidate(&v);
    new.validate_static().unwrap();
    let changes = ChangeSet::between(&old, &new).unwrap();
    eprintln!(
        "delivery guard: class={:?}, risk={}, operator_only={:?}",
        changes.class(),
        changes.requires_risk_admin(),
        changes.authorize_commit(&operator(&["usdm-main"]))
    );
    assert!(changes.requires_risk_admin());
}
#[test]
fn adding_a_strategy_budget_cannot_bypass_risk_admin() {
    let mut v = reference().value().clone();
    let second_account = v["strategies"][0]["account_ids"][1].clone();
    let second_budget = v["strategies"][0]["risk_budgets"][1].clone();
    v["strategies"][0]["account_ids"]
        .as_array_mut()
        .unwrap()
        .pop();
    v["strategies"][0]["risk_budgets"]
        .as_array_mut()
        .unwrap()
        .pop();
    let old = candidate(&v);
    old.validate_static().unwrap();
    v["strategies"][0]["account_ids"]
        .as_array_mut()
        .unwrap()
        .push(second_account);
    v["strategies"][0]["risk_budgets"]
        .as_array_mut()
        .unwrap()
        .push(second_budget);
    let new = candidate(&v);
    new.validate_static().unwrap();
    let changes = ChangeSet::between(&old, &new).unwrap();
    eprintln!(
        "add budget: class={:?}, risk={}, operator_only={:?}",
        changes.class(),
        changes.requires_risk_admin(),
        changes.authorize_commit(&operator(&["spot-main", "usdm-main"]))
    );
    assert!(changes.requires_risk_admin());
}
#[test]
fn reorder_cannot_disappear_when_another_entity_is_also_edited() {
    let old = reference();
    let mut v = old.value().clone();
    v["risk_policies"].as_array_mut().unwrap().swap(0, 1);
    let reorder = ChangeSet::between(&old, &candidate(&v)).unwrap();
    assert!(reorder.accounts().contains("usdm-main"));
    v["risk_policies"][1]["max_order_quote"] = "1".into();
    // Keep child strategy budgets compatible with account order cap by use max gross instead.
    v["risk_policies"][1]["max_order_quote"] =
        old.value()["risk_policies"][0]["max_order_quote"].clone();
    v["risk_policies"][1]["max_gross_quote"] = "10000".into();
    let combined = ChangeSet::between(&old, &candidate(&v)).unwrap();
    eprintln!(
        "reorder alone scope={:?} class={:?}; with edit scope={:?} class={:?} operator_only={:?}",
        reorder.accounts(),
        reorder.class(),
        combined.accounts(),
        combined.class(),
        combined.authorize_commit(&operator(&["spot-main"]))
    );
    assert!(combined.accounts().contains("usdm-main"));
    assert_ne!(combined.class(), Class::Hot);
}
fn live(v: &mut Value) {
    match v {
        Value::String(s) => {
            *s = s
                .replace("artifact://fixture/", "artifact://release/")
                .replace("secret://fixture/", "secret://vault/")
                .replace(".invalid", ".example");
            if s == "mock" {
                *s = "ws-api".into();
            }
        }
        Value::Array(xs) => {
            for x in xs {
                live(x)
            }
        }
        Value::Object(xs) => {
            for x in xs.values_mut() {
                live(x)
            }
        }
        _ => {}
    }
}
#[test]
fn free_parameter_values_are_not_typed_deployment_references() {
    let mut v = reference().value().clone();
    live(&mut v);
    v["identity"]["mode"] = "live".into();
    v["identity"]["environment"] = "production".into();
    for x in v["venues"].as_array_mut().unwrap() {
        x["environment"] = "production".into();
    }
    candidate(&v).validate_static().unwrap();
    v["strategies"][0]["params"]["rejected_status_label"] = "mock".into();
    let c = candidate(&v);
    c.validate_structural().unwrap();
    eprintln!("free parameter static={:?}", c.validate_static());
    assert!(c.validate_static().is_ok());
}
#[test]
fn adding_a_whole_strategy_with_budgets_requires_risk_admin() {
    let mut v = reference().value().clone();
    let row = v["strategies"].as_array_mut().unwrap().remove(0);
    let process_index = v["processes"]
        .as_array()
        .unwrap()
        .iter()
        .position(|p| p["id"] == row["process_id"])
        .unwrap();
    let process = v["processes"].as_array_mut().unwrap().remove(process_index);
    let old = candidate(&v);
    old.validate_static().unwrap();
    v["strategies"].as_array_mut().unwrap().insert(0, row);
    v["processes"]
        .as_array_mut()
        .unwrap()
        .insert(process_index, process);
    let new = candidate(&v);
    new.validate_static().unwrap();
    let changes = ChangeSet::between(&old, &new).unwrap();
    let auth = Authority::from_grants(
        [Role::Operator, Role::ClusterAdmin]
            .into_iter()
            .map(|role| RoleGrant {
                role,
                accounts: Default::default(),
                cluster_scope: true,
            })
            .collect(),
    );
    eprintln!(
        "add whole strategy: class={:?}, risk={}, operator_cluster_only={:?}",
        changes.class(),
        changes.requires_risk_admin(),
        changes.authorize_commit(&auth)
    );
    assert!(changes.requires_risk_admin());
}

#[test]
fn every_declared_strategy_process_needs_its_strategy_payload() {
    let mut v = reference().value().clone();
    v["strategies"].as_array_mut().unwrap().remove(0);
    let c = candidate(&v);
    eprintln!("dangling process static={:?}", c.validate_static());
    assert!(c.validate_static().is_err());
}
#[test]
fn projected_bytes_cannot_exceed_the_candidate_reserved_capacity() {
    use sha2::{Digest, Sha256};
    use tickwire_config::projection::{PinnedPolicy, project};
    let target = json!({"process_id":"strategy-01-active","account_ids":["spot-main","usdm-main"],"venue_ids":["binance-spot","binance-usdm"],"topic_ids":[1,2],"secret_purposes":[]});
    let policy_bytes =
        serde_json::to_vec(&json!({"schema":1,"revision":1,"targets":[target]})).unwrap();
    let h: String = Sha256::digest(&policy_bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let reference = format!("artifact://fixture/config/policy@sha256:{h}");
    let policy = PinnedPolicy::decode(&reference, &policy_bytes).unwrap();
    let mut v = crate::reference().value().clone();
    v["local_config"]["projection_policy_ref"] = reference.into();
    v["local_config"]["projection_policy_revision"] = 1.into();
    v["ipc"]["config_projection_max_bytes"] = 4096.into();
    v["strategies"][0]["params"]["long_data"] = json!(vec!["x".repeat(256); 16]);
    let c = candidate(&v);
    c.validate_static().unwrap();
    let out = project(&c, "strategy-01-active", &policy);
    eprintln!(
        "projection result bytes={:?}, configured max=4096",
        out.as_ref().map(|v| v.canonical_bytes().len())
    );
    assert!(out.is_err());
}
