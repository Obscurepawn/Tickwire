use serde_json::Value;
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
fn changed(c: &Candidate, f: impl FnOnce(&mut Value)) -> Candidate {
    let mut root = c.value().clone();
    f(&mut root);
    Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&root).unwrap(),
        Limits::default(),
    )
    .unwrap()
}
fn authority(roles: &[Role], accounts: &[&str], cluster: bool) -> Authority {
    Authority::from_grants(
        roles
            .iter()
            .map(|role| RoleGrant {
                role: *role,
                accounts: accounts.iter().map(|s| s.to_string()).collect(),
                cluster_scope: cluster,
            })
            .collect(),
    )
}
#[test]
fn logging_is_hot_but_listener_and_storage_paths_require_replacement() {
    let c = reference();
    let n = changed(&c, |v| {
        v["observability"]["diagnostic_level"] = "debug".into()
    });
    assert_eq!(ChangeSet::between(&c, &n).unwrap().class(), Class::Hot);
    for section in ["observability", "journal", "local_config"] {
        let n = changed(&c, |v| {
            if section == "observability" {
                v[section]["metrics_listen_uri"] = "https://other.fixture.invalid:9445".into();
            } else {
                v[section]["root_dir"] = "/var/lib/tickwire/new".into();
            }
        });
        assert_eq!(
            ChangeSet::between(&c, &n).unwrap().class(),
            Class::RollingRestart
        );
    }
}
#[test]
fn exact_budget_tightening_is_hot_and_relaxation_requires_joint_roles() {
    let c = reference();
    let n = changed(&c, |v| {
        v["risk_policies"][0]["max_gross_quote"] = "999999999999999999.999999999999999999".into()
    });
    let changes = ChangeSet::between(&c, &n).unwrap();
    assert_eq!(changes.class(), Class::Quiesce);
    assert!(changes.requires_risk_admin());
    assert!(
        changes
            .authorize_commit(&authority(&[Role::Operator], &["spot-main"], false))
            .is_err()
    );
    changes
        .authorize_commit(&authority(
            &[Role::Operator, Role::RiskAdmin],
            &["spot-main"],
            false,
        ))
        .unwrap();
    let n = changed(&c, |v| {
        v["risk_policies"][0]["max_gross_quote"] = "10000.00".into()
    });
    assert_eq!(ChangeSet::between(&c, &n).unwrap().class(), Class::Hot);
}
#[test]
fn buffering_and_market_age_have_opposite_protection_directions() {
    let c = reference();
    let n = changed(&c, |v| {
        v["risk_policies"][0]["fee_buffer_rate"] = "0.000000001".into()
    });
    assert!(ChangeSet::between(&c, &n).unwrap().requires_risk_admin());
    let n = changed(&c, |v| {
        v["risk_policies"][0]["max_market_age_us"] = 1000000.into()
    });
    assert!(ChangeSet::between(&c, &n).unwrap().requires_risk_admin());
}
#[test]
fn identity_environment_and_monetary_units_are_forbidden_at_runtime() {
    let c = reference();
    let n = changed(&c, |v| {
        v["identity"]["namespace_id"] = "00000000-0000-4000-8000-000000000002".into()
    });
    let changes = ChangeSet::between(&c, &n).unwrap();
    assert_eq!(changes.class(), Class::ForbiddenRuntime);
    assert!(
        changes
            .authorize_commit(&authority(
                &[Role::Operator, Role::RiskAdmin, Role::ClusterAdmin],
                &[],
                true
            ))
            .is_err()
    );
    let n = changed(&c, |v| {
        v["accounts"][0]["account_identity_ref"]="artifact://fixture/spot/other@sha256:1111111111111111111111111111111111111111111111111111111111111111".into()
    });
    assert_eq!(
        ChangeSet::between(&c, &n).unwrap().class(),
        Class::ForbiddenRuntime
    );
}
#[test]
fn shared_policy_dependency_cannot_be_hidden_by_single_account_scope() {
    let c = changed(&reference(), |v| {
        v["accounts"][1]["risk_policy_id"] = "spot-risk".into()
    });
    let n = changed(&c, |v| {
        v["risk_policies"][0]["max_gross_quote"] = "999999999999999999".into()
    });
    let changes = ChangeSet::between(&c, &n).unwrap();
    assert_eq!(changes.accounts().len(), 2);
    assert!(
        changes
            .authorize_commit(&authority(
                &[Role::Operator, Role::RiskAdmin],
                &["spot-main"],
                false
            ))
            .is_err()
    );
    changes
        .authorize_commit(&authority(
            &[Role::Operator, Role::RiskAdmin],
            &["spot-main", "usdm-main"],
            false,
        ))
        .unwrap();
}
#[test]
fn strategy_parameters_quiesce_until_pinned_schema_proves_hot_and_delivery_is_complete() {
    let c = reference();
    let n = changed(&c, |v| {
        v["strategies"][0]["params"]["signal_window_events"] = 2048.into()
    });
    let changes = ChangeSet::between(&c, &n).unwrap();
    assert_eq!(changes.class(), Class::Quiesce);
    assert_eq!(
        changes.delivery_targets().len(),
        c.value()["processes"].as_array().unwrap().len()
    );
}

#[test]
fn risk_admin_for_another_account_cannot_authorize_this_account_relaxation() {
    let c = reference();
    let n = changed(&c, |v| {
        v["risk_policies"][0]["max_gross_quote"] = "999999999999999999".into()
    });
    let changes = ChangeSet::between(&c, &n).unwrap();
    let authority = Authority::from_grants(vec![
        RoleGrant {
            role: Role::Operator,
            accounts: ["spot-main".into()].into(),
            cluster_scope: false,
        },
        RoleGrant {
            role: Role::RiskAdmin,
            accounts: ["usdm-main".into()].into(),
            cluster_scope: false,
        },
    ]);
    assert!(changes.authorize_commit(&authority).is_err());
}

#[test]
fn unassigned_policy_is_cluster_scoped_and_hidden_from_account_readers() {
    let c = changed(&reference(), |v| {
        let mut policy = v["risk_policies"][0].clone();
        policy["id"] = "unused-risk".into();
        v["risk_policies"].as_array_mut().unwrap().push(policy);
    });
    let n = changed(&c, |v| {
        v["risk_policies"][2]["max_gross_quote"] = "999999999999999999".into()
    });
    let changes = ChangeSet::between(&c, &n).unwrap();
    let account = authority(
        &[Role::Viewer, Role::Operator, Role::RiskAdmin],
        &["spot-main"],
        false,
    );
    assert!(changes.visible_changes(&account).unwrap().is_empty());
    assert!(changes.authorize_commit(&account).is_err());
}

#[test]
fn parameter_names_cannot_masquerade_as_hot_budget_fields() {
    let c = changed(&reference(), |v| {
        v["strategies"][0]["params"]["risk_budgets"] = serde_json::json!({"max_gross_quote": "10"});
    });
    let n = changed(&c, |v| {
        v["strategies"][0]["params"]["risk_budgets"]["max_gross_quote"] = "5".into();
    });
    assert_eq!(ChangeSet::between(&c, &n).unwrap().class(), Class::Quiesce);
    let n = changed(&c, |v| {
        v["strategies"][0]["params"]["state_schema_ref"] = "custom-value".into();
    });
    assert_eq!(ChangeSet::between(&c, &n).unwrap().class(), Class::Quiesce);
    let c = changed(&reference(), |v| {
        v["strategies"][0]["params"]["accounts"] = serde_json::json!([{"id": "a", "value": 1}, {"id": "a", "value": 2}, {"id": "b", "value": 3}]);
    });
    let n = changed(&c, |v| {
        v["strategies"][0]["params"]["accounts"][0]["value"] = 10.into();
        v["strategies"][0]["params"]["accounts"][2]["value"] = 30.into();
    });
    let changes = ChangeSet::between(&c, &n).unwrap();
    assert_eq!(changes.class(), Class::Quiesce);
    assert_eq!(
        changes
            .visible_changes(&authority(
                &[Role::Viewer],
                &["spot-main", "usdm-main"],
                false
            ))
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn widening_tradable_symbols_requires_the_account_risk_admin() {
    let c = reference();
    let n = changed(&c, |v| {
        v["accounts"][0]["allowed_symbols"]
            .as_array_mut()
            .unwrap()
            .push("LTCUSDT".into());
    });
    let changes = ChangeSet::between(&c, &n).unwrap();
    assert!(changes.requires_risk_admin());
    assert!(
        changes
            .authorize_commit(&authority(&[Role::Operator], &["spot-main"], false))
            .is_err()
    );
}

#[test]
fn administrative_roles_include_scoped_read_access_without_operator_authority() {
    let c = reference();
    let n = changed(&c, |v| {
        v["risk_policies"][0]["max_gross_quote"] = "999999999999999999".into();
    });
    let changes = ChangeSet::between(&c, &n).unwrap();
    for role in [Role::RiskAdmin, Role::ClusterAdmin] {
        let auth = authority(&[role], &["spot-main"], false);
        assert_eq!(changes.visible_changes(&auth).unwrap().len(), 1);
        assert!(changes.authorize_commit(&auth).is_err());
        assert!(
            changes
                .visible_changes(&authority(&[role], &["usdm-main"], false))
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn delaying_disk_freeze_requires_risk_admin_even_with_cluster_authority() {
    let c = reference();
    let n = changed(&c, |v| v["journal"]["disk_freeze_percent"] = 99.into());
    let changes = ChangeSet::between(&c, &n).unwrap();
    assert!(changes.requires_risk_admin());
    assert!(
        changes
            .authorize_commit(&authority(&[Role::Operator, Role::ClusterAdmin], &[], true))
            .is_err()
    );
}

#[test]
fn widening_venue_request_window_requires_risk_admin() {
    let c = reference();
    let n = changed(&c, |v| v["venues"][0]["recv_window_ms"] = 60000.into());
    let changes = ChangeSet::between(&c, &n).unwrap();
    assert!(changes.requires_risk_admin());
    assert!(
        changes
            .authorize_commit(&authority(&[Role::Operator, Role::ClusterAdmin], &[], true))
            .is_err()
    );
}
