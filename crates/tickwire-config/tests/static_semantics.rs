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
    assert_eq!(r.strategy_instances, 16);
    assert_eq!(r.strategy_domain_pairs, 32);
    assert_eq!(r.banks_and_cursors, 2_957_574_144);
    assert_eq!(r.snapshots, 2_147_483_648);
    assert_eq!(r.projection_objects, 537_395_200);
    assert_eq!(r.total_shared_bytes, 5_910_888_448);
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
fn second_host_obsolete_control_settings_and_expired_sessions_are_rejected() {
    for mutation in [
        (|v: &mut Value| {
            let extra = v["hosts"][0].clone();
            v["hosts"].as_array_mut().unwrap().push(extra);
        }) as fn(&mut Value),
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

#[test]
fn cpu_isolation_rejects_equal_req_resp_housekeeping_overlap_and_bad_cpu_lists() {
    // Positive baseline: normalized_configuration fills exec defaults cleanly.
    let mut trimmed = reference().value().clone();
    for p in trimmed["processes"].as_array_mut().unwrap() {
        if p["role"] == "exec" {
            p.as_object_mut().unwrap().remove("epoll_wait_ms");
            p.as_object_mut().unwrap().remove("max_order_age_ns");
        }
    }
    let normalized = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&trimmed).unwrap(),
        Limits::default(),
    )
    .unwrap()
    .normalized_configuration(Limits::default())
    .unwrap();
    normalized.validate_static().unwrap();
    for p in normalized.value()["processes"].as_array().unwrap() {
        if p["role"] == "exec" {
            assert_eq!(p["epoll_wait_ms"], 0);
            assert_eq!(p["max_order_age_ns"], 23_000);
        }
    }

    for (label, mutation) in [
        (
            "req_cpu == resp_cpu",
            (|v: &mut Value| {
                let exec = v["processes"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|p| p["id"] == "exec-spot-a")
                    .unwrap();
                exec["req_cpu"] = 10.into();
                exec["resp_cpu"] = 10.into();
            }) as fn(&mut Value),
        ),
        ("hot_cpus overlaps process housekeeping_cpus", |v| {
            let exec = v["processes"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|p| p["id"] == "exec-spot-a")
                .unwrap();
            exec["housekeeping_cpus"] = json!([2, 10]);
        }),
        ("worker_cpu overlaps host housekeeping_cpus", |v| {
            let strat = v["processes"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|p| p["id"] == "strategy-01-active")
                .unwrap();
            strat["cpus"] = json!([4]);
            strat["worker_cpu"] = 4.into();
        }),
        ("reactor_cpu overlaps process housekeeping_cpus", |v| {
            let md = v["processes"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|p| p["id"] == "md-spot-a")
                .unwrap();
            md["housekeeping_cpus"] = json!([0, 8]);
        }),
        ("cpu >= 4096", |v| {
            let strat = v["processes"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|p| p["id"] == "strategy-01-active")
                .unwrap();
            strat["cpus"] = json!([4096]);
            strat["worker_cpu"] = 4096.into();
        }),
        ("empty housekeeping_cpus", |v| {
            v["processes"][1]["housekeeping_cpus"] = json!([]);
        }),
        ("duplicate housekeeping_cpus", |v| {
            v["processes"][1]["housekeeping_cpus"] = json!([0, 0]);
        }),
        ("unsorted housekeeping_cpus", |v| {
            v["processes"][1]["housekeeping_cpus"] = json!([1, 0]);
        }),
        ("wrong role cpu field", |v| {
            let md = v["processes"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|p| p["id"] == "md-spot-a")
                .unwrap();
            md["req_cpu"] = 8.into();
        }),
    ] {
        assert!(
            changed(mutation).validate_static().is_err(),
            "expected failure for {label}"
        );
    }
}

#[test]
fn multi_uid_ors_order_manager_and_rms_semantic_bounds_are_enforced() {
    for (label, mutation) in [
        (
            "non-power-of-two slots_per_strat (1000)",
            (|v: &mut Value| {
                let exec = v["processes"]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .find(|p| p["id"] == "exec-spot-a")
                    .unwrap();
                exec["order_manager"]["slots_per_strat"] = 1000.into();
            }) as fn(&mut Value),
        ),
        ("zero slots_per_strat", |v| {
            let exec = v["processes"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|p| p["id"] == "exec-spot-a")
                .unwrap();
            exec["order_manager"]["slots_per_strat"] = 0.into();
        }),
        ("account_ref == 0", |v| {
            let exec = v["processes"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|p| p["id"] == "exec-spot-a")
                .unwrap();
            exec["enabled_accounts"][0]["account_ref"] = 0.into();
        }),
        ("account_ref > 255", |v| {
            let exec = v["processes"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|p| p["id"] == "exec-spot-a")
                .unwrap();
            exec["enabled_accounts"][0]["account_ref"] = 256.into();
        }),
        ("duplicate account_ref", |v| {
            let exec = v["processes"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|p| p["id"] == "exec-spot-a")
                .unwrap();
            exec["enabled_accounts"][1]["account_ref"] = 1.into();
        }),
        ("foreign account_id in enabled_accounts", |v| {
            let exec = v["processes"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|p| p["id"] == "exec-spot-a")
                .unwrap();
            exec["enabled_accounts"][1]["account_id"] = "usdm-main".into();
        }),
        ("invalid url local_ip", |v| {
            let exec = v["processes"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|p| p["id"] == "exec-spot-a")
                .unwrap();
            exec["enabled_accounts"][0]["urls"]["order"] = json!([["0.0.0.0", 2]]);
        }),
        ("rms max_order_notional > max_pos_notional", |v| {
            let exec = v["processes"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|p| p["id"] == "exec-spot-a")
                .unwrap();
            exec["rms"]["max_order_notional"] = "200000.00".into();
        }),
        ("rms fat_finger_ratio > 1", |v| {
            let exec = v["processes"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|p| p["id"] == "exec-spot-a")
                .unwrap();
            exec["rms"]["fat_finger_ratio"] = "1.5".into();
        }),
        ("rms non-monotonic rolling window", |v| {
            let exec = v["processes"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|p| p["id"] == "exec-spot-a")
                .unwrap();
            exec["rms"]["5s"]["max_new_orders"] = 10.into();
        }),
    ] {
        assert!(
            changed(mutation).validate_static().is_err(),
            "expected failure for {label}"
        );
    }
}
