use serde_json::json;
use tickwire_config::{
    Candidate, Error, Format, Limits,
    changes::{Authority, Class, Role, RoleGrant},
    patch::{Head, ScopedPatch},
    prepared::{Input, PreparedCandidate},
};
fn base() -> Candidate {
    Candidate::parse(
        Format::Toml,
        include_bytes!("../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap()
}
fn parse(value: &serde_json::Value) -> Candidate {
    Candidate::parse(
        Format::Json,
        &serde_json::to_vec(value).unwrap(),
        Limits::default(),
    )
    .unwrap()
}
fn editor(global: bool) -> Authority {
    Authority::from_grants(vec![RoleGrant {
        role: Role::Editor,
        accounts: if global {
            Default::default()
        } else {
            ["spot-main".into()].into()
        },
        cluster_scope: global,
    }])
}
#[test]
fn omitted_and_explicit_false_defaults_have_one_complete_configuration_identity() {
    let base = base();
    let mut explicit = base.value().clone();
    for row in explicit["control"]["role_bindings"].as_array_mut().unwrap() {
        if row.get("cluster_scope").is_none() {
            row["cluster_scope"] = false.into();
        }
    }
    let other = parse(&explicit);
    assert_ne!(base.hash(), other.hash());
    let normalized = base.normalized_configuration(Limits::default()).unwrap();
    assert!(normalized == other.normalized_configuration(Limits::default()).unwrap());
    assert_eq!(
        normalized.value()["control"]["role_bindings"][4]["cluster_scope"],
        true
    );
    assert!(
        base.value()["control"]["role_bindings"][0]
            .get("cluster_scope")
            .is_none()
    );
}
#[test]
fn normalization_validates_the_full_schema_and_enforces_the_final_byte_bound() {
    let base = base();
    assert_eq!(
        base.normalized_configuration(Limits {
            max_bytes: base.canonical_bytes().len(),
            max_depth: 64
        }),
        Err(Error::Capacity)
    );
    assert_eq!(
        parse(&json!({"control":{"role_bindings":[]}})).normalized_configuration(Limits::default()),
        Err(Error::Schema)
    );
}
#[test]
fn full_candidate_needs_global_editor_before_exposing_any_base_conflict() {
    let base = base();
    let head = Head::new(2, &base).unwrap();
    for etag in [head.etag(), "\"wrong-etag\"".into()] {
        assert_eq!(
            PreparedCandidate::prepare(
                &head,
                &etag,
                &base,
                Input::Full(&base),
                &editor(false),
                Limits::default()
            )
            .err(),
            Some(Error::Semantic("authorization"))
        );
    }
}
#[test]
fn default_completion_does_not_create_a_false_configuration_change() {
    let base = base();
    let head = Head::new(2, &base).unwrap();
    let prepared = PreparedCandidate::prepare(
        &head,
        &head.etag(),
        &base,
        Input::Full(&base),
        &editor(true),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(prepared.base_hash(), base.hash());
    assert_eq!(prepared.changes().class(), Class::Hot);
    assert!(
        prepared
            .changes()
            .visible_changes(&editor(true))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        prepared.candidate().value()["control"]["role_bindings"][0]["cluster_scope"],
        false
    );
}
#[test]
fn full_candidate_checks_both_base_identity_and_etag() {
    let base = base();
    let head = Head::new(2, &base).unwrap();
    let mut changed = base.value().clone();
    changed["risk_policies"][0]["max_order_quote"] = "100".into();
    let changed = parse(&changed);
    assert_eq!(
        PreparedCandidate::prepare(
            &head,
            &head.etag(),
            &changed,
            Input::Full(&base),
            &editor(true),
            Limits::default()
        )
        .err(),
        Some(Error::Revision)
    );
    assert_eq!(
        PreparedCandidate::prepare(
            &head,
            "wrong",
            &base,
            Input::Full(&base),
            &editor(true),
            Limits::default()
        )
        .err(),
        Some(Error::Revision)
    );
}
#[test]
fn scoped_patch_keeps_account_authority_and_normalizes_only_configuration_defaults() {
    let base = base();
    let head = Head::new(2, &base).unwrap();
    let patch = json!({"base_etag":head.etag(),"changes":[{
        "op":"replace","selector":{"collection":"risk_policies","id":base.value()["risk_policies"][0]["id"]},
        "path":"/max_gross_quote","value":"10000"
    }]});
    let patch =
        ScopedPatch::parse(&serde_json::to_vec(&patch).unwrap(), Limits::default()).unwrap();
    let prepared = PreparedCandidate::prepare(
        &head,
        &head.etag(),
        &base,
        Input::Patch(&patch),
        &editor(false),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(
        prepared.candidate().value()["risk_policies"][0]["max_gross_quote"],
        "10000"
    );
    assert_eq!(prepared.changes().accounts(), &["spot-main".into()].into());
    assert!(!prepared.changes().requires_cluster_admin());
}
#[test]
fn forbidden_runtime_identity_changes_remain_forbidden_after_normalization() {
    let base = base();
    let head = Head::new(2, &base).unwrap();
    let mut changed = base.value().clone();
    changed["identity"]["namespace_id"] = "00000000-0000-4000-8000-000000000002".into();
    let changed = parse(&changed);
    assert_eq!(
        PreparedCandidate::prepare(
            &head,
            &head.etag(),
            &base,
            Input::Full(&changed),
            &editor(true),
            Limits::default()
        )
        .err(),
        Some(Error::Semantic("runtime change forbidden"))
    );
}
