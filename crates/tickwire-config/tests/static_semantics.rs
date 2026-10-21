use serde_json::{Value, json};
use tickwire_config::{Candidate, Format, Limits};

fn reference() -> Candidate {
    Candidate::parse(
        Format::Toml,
        include_bytes!("../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap()
}
fn changed(mutate: impl FnOnce(&mut Value)) -> Candidate {
    let mut value = reference().value().clone();
    mutate(&mut value);
    Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&value).unwrap(),
        Limits::default(),
    )
    .unwrap()
}

#[test]
fn reference_resource_accounting_matches_independent_deployment_contract() {
    let r = reference().validate_static().unwrap();
    assert_eq!(r.strategy_instances, 32);
    assert_eq!(r.strategy_domain_pairs, 64);
    assert_eq!(r.banks_and_cursors, 3_496_542_208);
    assert_eq!(r.snapshots, 2_147_483_648);
    assert_eq!(r.projection_objects, 537_395_200);
    assert_eq!(r.total_shared_bytes, 6_449_856_512);
}

#[test]
fn duplicate_identity_missing_references_and_strategy_roles_are_rejected() {
    for mutation in [
        (|v: &mut Value| {
            let p = v["processes"][0].clone();
            v["processes"].as_array_mut().unwrap().push(p);
        }) as fn(&mut Value),
        |v| v["accounts"][0]["exec_process_id"] = "missing-exec".into(),
        |v| v["strategies"][0]["role"] = "Shadow".into(),
        |v| v["accounts"][1]["risk_domain_id"] = v["accounts"][0]["risk_domain_id"].clone(),
    ] {
        assert!(changed(mutation).validate_static().is_err());
    }
}

#[test]
fn shared_cpu_memory_capacity_and_invalid_bank_geometry_are_rejected() {
    for mutation in [
        (|v: &mut Value| v["processes"][4]["cpus"] = v["processes"][3]["cpus"].clone())
            as fn(&mut Value),
        |v| v["ipc"]["intent_capacity"] = 4097.into(),
        |v| v["hosts"][0]["shm_budget_bytes"] = 1_073_741_824u64.into(),
        |v| v["ipc"]["market_arena_classes"][0]["block_count"] = 1.into(),
    ] {
        assert!(changed(mutation).validate_static().is_err());
    }
}

#[test]
fn failure_domain_and_distinct_lease_expiries_remain_independent() {
    for mutation in [
        (|v: &mut Value| v["hosts"][1]["failure_domain"] = v["hosts"][0]["failure_domain"].clone())
            as fn(&mut Value),
        |v| v["control"]["exec_grant_renew_ms"] = 200.into(),
        |v| v["ipc"]["resource_heartbeat_ms"] = 3000.into(),
        |v| v["control"]["election_min_ms"] = 50.into(),
    ] {
        assert!(changed(mutation).validate_static().is_err());
    }
}

#[test]
fn strategy_budget_currency_and_order_bound_match_the_account() {
    for mutation in [
        (|v: &mut Value| v["strategies"][0]["risk_budgets"][0]["quote_asset"] = "BTC".into())
            as fn(&mut Value),
        |v| v["strategies"][0]["risk_budgets"][0]["max_order_quote"] = "999999999999".into(),
        |v| v["strategies"][0]["risk_budgets"] = json!([]),
    ] {
        assert!(changed(mutation).validate_static().is_err());
    }
}

#[test]
fn contract_overrides_must_match_the_account_product() {
    for (account, kind, expiry) in [(0, "Delivery", 2_000_000_000_000u64), (1, "Spot", 0)] {
        let c = changed(|v| {
            v["accounts"][account]["instrument_overrides"] = json!([{
                "symbol":v["accounts"][account]["allowed_symbols"][0],
                "contract_kind":kind,
                "expiry_epoch_ms":expiry,
                "delivery_guard_seconds":3600,
                "settlement_schedule_ref":v["risk_policies"][account]["settlement_schedule_ref"],
            }]);
        });
        c.validate_structural().unwrap();
        assert!(c.validate_static().is_err());
    }
}

#[test]
fn parameter_depth_and_secret_keys_cannot_hide_inside_freeform_params() {
    assert!(
        changed(|v| v["strategies"][0]["params"]["nested"] = json!({"api_key":"test-value"}))
            .validate_static()
            .is_err()
    );
    assert!(
        changed(|v| {
            let mut p = json!({"leaf":1});
            for _ in 0..16 {
                p = json!({"nested": p});
            }
            v["strategies"][0]["params"] = p;
        })
        .validate_static()
        .is_err()
    );
}
#[test]
fn cluster_role_scope_is_explicit_and_has_no_ambiguous_account_list() {
    let mut value = reference().value().clone();
    value["control"]["role_bindings"][0]["cluster_scope"] = true.into();
    value["control"]["role_bindings"][0]["account_ids"] = serde_json::json!([]);
    let candidate = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&value).unwrap(),
        Limits::default(),
    )
    .unwrap();
    candidate.validate_static().unwrap();
    value["control"]["role_bindings"][0]["account_ids"] = serde_json::json!(["spot-main"]);
    let ambiguous = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&value).unwrap(),
        Limits::default(),
    )
    .unwrap();
    assert!(ambiguous.validate_static().is_err());
}
