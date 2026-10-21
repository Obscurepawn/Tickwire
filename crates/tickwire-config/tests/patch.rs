use serde_json::{Value, json};
use tickwire_config::{
    Candidate, Error, Format, Limits,
    changes::{Authority, ChangeSet, Role, RoleGrant},
    patch::{Head, ScopedPatch},
};
fn reference() -> Candidate {
    Candidate::parse(
        Format::Toml,
        include_bytes!("../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap()
}
fn authority(accounts: &[&str], cluster: bool) -> Authority {
    Authority::from_grants(vec![RoleGrant {
        role: Role::Editor,
        accounts: accounts.iter().map(|s| s.to_string()).collect(),
        cluster_scope: cluster,
    }])
}
fn patch(base: &Candidate, changes: Value) -> (Head, ScopedPatch) {
    let head = Head::new(9, base).unwrap();
    let request = json!({"base_etag": head.etag(), "changes": changes});
    let patch =
        ScopedPatch::parse(&serde_json::to_vec(&request).unwrap(), Limits::default()).unwrap();
    (head, patch)
}
fn change(base: &Candidate, f: impl FnOnce(&mut Value)) -> Candidate {
    let mut value = base.value().clone();
    f(&mut value);
    Candidate::parse(
        Format::Json,
        &serde_json::to_vec(&value).unwrap(),
        Limits::default(),
    )
    .unwrap()
}
#[test]
fn authorized_policy_patch_produces_a_complete_new_candidate_without_mutating_base() {
    let base = reference();
    let id = base.value()["risk_policies"][0]["id"].clone();
    let (head, patch) = patch(
        &base,
        json!([{"op":"replace","selector":{"collection":"risk_policies","id":id},
                "path":"/max_gross_quote","value":"10000"}]),
    );
    let result = patch
        .apply(
            &head,
            &head.etag(),
            &base,
            &authority(&["spot-main"], false),
        )
        .unwrap();
    result.validate_static().unwrap();
    assert_ne!(result.hash(), base.hash());
    assert_eq!(result.value()["accounts"], base.value()["accounts"]);
    assert_eq!(
        result.value()["risk_policies"][0]["max_gross_quote"],
        "10000"
    );
    assert_ne!(base.value()["risk_policies"][0]["max_gross_quote"], "10000");
    assert_eq!(
        ChangeSet::between(&base, &result)
            .unwrap()
            .delivery_targets()
            .len(),
        base.value()["processes"].as_array().unwrap().len()
    );
}
#[test]
fn existing_and_missing_unauthorized_selectors_return_the_same_error() {
    let base = reference();
    for id in ["usdm-main", "does-not-exist"] {
        let (head, patch) = patch(
            &base,
            json!([{"op":"replace","selector":{"collection":"accounts","id":id},
                    "path":"/risk_policy_id","value":"hidden"}]),
        );
        assert_eq!(
            patch.apply(
                &head,
                &head.etag(),
                &base,
                &authority(&["spot-main"], false)
            ),
            Err(Error::Semantic("authorization"))
        );
    }
}
#[test]
fn shared_policy_requires_all_affected_account_scopes() {
    let base = change(&reference(), |v| {
        v["accounts"][1]["risk_policy_id"] = v["accounts"][0]["risk_policy_id"].clone();
    });
    let id = base.value()["risk_policies"][0]["id"].clone();
    let (head, patch) = patch(
        &base,
        json!([{"op":"replace","selector":{"collection":"risk_policies","id":id},
                "path":"/max_gross_quote","value":"10000"}]),
    );
    assert_eq!(
        patch.apply(
            &head,
            &head.etag(),
            &base,
            &authority(&["spot-main"], false)
        ),
        Err(Error::Semantic("authorization"))
    );
    patch
        .apply(
            &head,
            &head.etag(),
            &base,
            &authority(&["spot-main", "usdm-main"], false),
        )
        .unwrap();
}
#[test]
fn strategy_budget_selector_binds_both_strategy_and_account() {
    let base = reference();
    let strategy = &base.value()["strategies"][0];
    let budget = &strategy["risk_budgets"][0];
    let account = budget["account_id"].as_str().unwrap();
    let (head, patch) = patch(
        &base,
        json!([{"op":"replace","selector":{"collection":"strategy_risk_budgets",
                "id":strategy["process_id"],"account_id":account},
                "path":"/max_gross_quote","value":"1000"}]),
    );
    let result = patch
        .apply(&head, &head.etag(), &base, &authority(&[account], false))
        .unwrap();
    assert_eq!(
        result.value()["strategies"][0]["risk_budgets"][0]["max_gross_quote"],
        "1000"
    );
    assert_eq!(
        ChangeSet::between(&base, &result).unwrap().accounts(),
        &[account.to_owned()].into()
    );
}
#[test]
fn add_remove_and_explicit_null_follow_ordered_object_property_semantics() {
    let base = reference();
    let strategy = &base.value()["strategies"][0];
    let accounts: Vec<_> = strategy["account_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap())
        .collect();
    let selector = json!({"collection":"strategies","id":strategy["process_id"]});
    let (head, patch) = patch(
        &base,
        json!([
            {"op":"add","selector":selector,"path":"/params/temporary","value":null},
            {"op":"replace","selector":selector,"path":"/params/temporary","value":false},
            {"op":"remove","selector":selector,"path":"/params/temporary"}
        ]),
    );
    assert_eq!(
        patch
            .apply(&head, &head.etag(), &base, &authority(&accounts, false))
            .unwrap(),
        base
    );
}
#[test]
fn exact_if_match_and_pinned_head_are_required() {
    let base = reference();
    let id = base.value()["risk_policies"][0]["id"].clone();
    let (head, patch) = patch(
        &base,
        json!([{"op":"replace","selector":{"collection":"risk_policies","id":id},
                "path":"/max_gross_quote","value":"10000"}]),
    );
    for etag in [
        "*",
        "W/\"cfg-9-abc\"",
        &Head::new(10, &base).unwrap().etag(),
    ] {
        assert_eq!(
            patch.apply(&head, etag, &base, &authority(&["spot-main"], false)),
            Err(Error::Revision)
        );
    }
    let other = change(&base, |v| {
        v["observability"]["diagnostic_level"] = "debug".into();
    });
    assert_eq!(
        patch.apply(
            &head,
            &head.etag(),
            &other,
            &authority(&["spot-main"], false)
        ),
        Err(Error::Revision)
    );
}
#[test]
fn parser_rejects_duplicate_unknown_and_reading_operations_and_array_paths() {
    let base = reference();
    let head = Head::new(9, &base).unwrap();
    let valid = json!({"base_etag":head.etag(),"changes":[
        {"op":"replace","selector":{"collection":"accounts","id":"spot-main"},
         "path":"/allowed_symbols","value":["BTCUSDT"]}
    ]});
    for path in ["", "/", "/allowed_symbols/0", "/../id", "/id~1x"] {
        let mut request = valid.clone();
        request["changes"][0]["path"] = path.into();
        assert!(
            ScopedPatch::parse(&serde_json::to_vec(&request).unwrap(), Limits::default()).is_err()
        );
    }
    for op in ["test", "copy", "move"] {
        let mut request = valid.clone();
        request["changes"][0]["op"] = op.into();
        assert!(
            ScopedPatch::parse(&serde_json::to_vec(&request).unwrap(), Limits::default()).is_err()
        );
    }
    let duplicate = format!(
        "{{\"base_etag\":{},\"base_etag\":{},\"changes\":[]}}",
        serde_json::to_string(&head.etag()).unwrap(),
        serde_json::to_string(&head.etag()).unwrap()
    );
    assert!(ScopedPatch::parse(duplicate.as_bytes(), Limits::default()).is_err());
}
#[test]
fn stable_selector_identity_and_required_properties_cannot_be_removed_or_retargeted() {
    let base = reference();
    for (op, path, value) in [
        ("replace", "/id", Some(json!("different-account"))),
        ("remove", "/venue_id", None),
        ("add", "/id", Some(json!("spot-main"))),
    ] {
        let mut c = json!({"op":op,"selector":{"collection":"accounts","id":"spot-main"},
                          "path":path});
        if let Some(value) = value {
            c["value"] = value;
        }
        let (head, patch) = patch(&base, json!([c]));
        assert!(
            patch
                .apply(
                    &head,
                    &head.etag(),
                    &base,
                    &authority(&["spot-main"], false)
                )
                .is_err()
        );
    }
}
