use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tickwire_config::{
    Candidate, Error, Format, Limits,
    artifacts::{ArtifactCatalog, CatalogLimits},
    changes::{Authority, Role, RoleGrant},
    delivery::{DeliveryKind, ProjectionDelivery, ProjectionLimits, ProjectionSet},
    patch::Head,
    prepared::{Input, PreparedCandidate},
    projection_object::ProjectionObject,
};

fn parse(v: &Value) -> Candidate {
    Candidate::parse(
        Format::Json,
        &serde_json::to_vec(v).unwrap(),
        Limits::default(),
    )
    .unwrap()
    .normalized_configuration(Limits::default())
    .unwrap()
}
fn base() -> Value {
    Candidate::parse(
        Format::Toml,
        include_bytes!("../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap()
    .value()
    .clone()
}
fn policy(v: &Value) -> Value {
    let targets: Vec<_> = v["processes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| {
            let id = p["id"].as_str().unwrap();
            let role = p["role"].as_str().unwrap();
            let strategy = v["strategies"]
                .as_array()
                .unwrap()
                .iter()
                .find(|s| s["process_id"] == id);
            let accounts: Vec<_> = v["accounts"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|a| match role {
                    "strategy" => strategy.unwrap()["account_ids"]
                        .as_array()
                        .unwrap()
                        .contains(&a["id"]),
                    "exec" => a["exec_process_id"] == id || a["standby_exec_process_id"] == id,
                    "guardian" => a["guardian_process_ids"]
                        .as_array()
                        .unwrap()
                        .contains(&p["id"]),
                    _ => false,
                })
                .collect();
            let topics: Vec<_> = v["ipc"]["topics"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|t| match role {
                    "strategy" => strategy.unwrap()["topic_ids"]
                        .as_array()
                        .unwrap()
                        .contains(&t["id"]),
                    "md" => {
                        t["publisher_process_id"] == id || t["standby_publisher_process_id"] == id
                    }
                    _ => false,
                })
                .collect();
            let venues: Vec<_> = v["venues"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|venue| {
                    accounts.iter().any(|a| a["venue_id"] == venue["id"])
                        || topics.iter().any(|t| t["product"] == venue["product"])
                })
                .map(|v| v["id"].clone())
                .collect();
            let purposes: &[&str] = match role {
                "supervisor" => &["control"],
                "replicator" => &["replication"],
                "exec" => &["execution"],
                "guardian" => &["guardian"],
                "md" => &["market-data"],
                _ => &[],
            };
            json!({
                "process_id":id,
                "account_ids":accounts.iter().map(|a|a["id"].clone()).collect::<Vec<_>>(),
                "venue_ids":venues,
                "topic_ids":topics.iter().map(|t|t["id"].clone()).collect::<Vec<_>>(),
                "secret_purposes":purposes
            })
        })
        .collect();
    json!({"schema":1,"revision":1,"targets":targets})
}
fn catalog() -> ArtifactCatalog {
    ArtifactCatalog::new(CatalogLimits::default()).unwrap()
}
fn pinned(mut value: Value, policy: Value, catalog: &mut ArtifactCatalog) -> Candidate {
    let bytes = serde_json::to_vec(&policy).unwrap();
    let hash: String = Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let reference = format!("artifact://fixture/config/projection-v1@sha256:{hash}");
    catalog.insert(&reference, &bytes).unwrap();
    value["local_config"]["projection_policy_ref"] = reference.into();
    value["local_config"]["projection_policy_revision"] = policy["revision"].clone();
    parse(&value)
}
fn prepared(base: &Candidate, next: &Candidate) -> PreparedCandidate {
    let head = Head::new(1, base).unwrap();
    let authority = Authority::from_grants(vec![RoleGrant {
        role: Role::Editor,
        accounts: Default::default(),
        cluster_scope: true,
    }]);
    PreparedCandidate::prepare(
        &head,
        &head.etag(),
        base,
        Input::Full(next),
        &authority,
        Limits::default(),
    )
    .unwrap()
}

#[test]
fn full_projection_set_contains_every_process_and_actual_payload_identity() {
    let v = base();
    let mut catalog = catalog();
    let c = pinned(v.clone(), policy(&v), &mut catalog);
    let set = ProjectionSet::build(&c, &catalog, ProjectionLimits::default()).unwrap();
    assert_eq!(set.candidate_hash(), c.hash());
    assert_eq!(
        set.targets().len(),
        c.value()["processes"].as_array().unwrap().len()
    );
    assert_eq!(
        set.stored_bytes(),
        set.targets()
            .values()
            .map(|p| p.canonical_bytes().len())
            .sum::<usize>()
    );
    for (id, projection) in set.targets() {
        assert_eq!(projection.value()["process"]["id"], *id);
        assert_eq!(projection.global_hash(), c.hash());
        let hash: [u8; 32] = Sha256::digest(projection.canonical_bytes()).into();
        assert_eq!(projection.hash(), hash);
    }
}
#[test]
fn every_policy_target_is_checked_without_missing_extra_or_invalid_tail_grants() {
    let v = base();
    let original = policy(&v);
    for kind in 0..3 {
        let mut p = original.clone();
        let targets = p["targets"].as_array_mut().unwrap();
        match kind {
            0 => {
                targets.pop();
            }
            1 => {
                let mut extra = targets[0].clone();
                extra["process_id"] = "unconfigured-process".into();
                targets.push(extra);
            }
            _ => targets.last_mut().unwrap()["secret_purposes"] = json!(["execution"]),
        }
        let mut catalog = catalog();
        let c = pinned(v.clone(), p, &mut catalog);
        assert!(ProjectionSet::build(&c, &catalog, ProjectionLimits::default()).is_err());
    }
}
#[test]
fn one_strategy_change_keeps_all_revision_only_targets_in_the_delivery_set() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let mut v = old.value().clone();
    v["strategies"][0]["params"]["signal_window_events"] = 2048.into();
    let next = parse(&v);
    let delivery = ProjectionDelivery::prepare(
        &old,
        &prepared(&old, &next),
        &catalog,
        ProjectionLimits::default(),
    )
    .unwrap();
    assert_eq!(delivery.base_hash(), old.hash());
    assert_eq!(delivery.candidate_hash(), next.hash());
    assert_eq!(
        delivery.targets().len(),
        old.value()["processes"].as_array().unwrap().len()
    );
    assert_eq!(
        delivery.targets()["strategy-01-active"].kind(),
        DeliveryKind::Changed
    );
    assert_eq!(
        delivery
            .targets()
            .values()
            .filter(|t| t.kind() == DeliveryKind::Changed)
            .count(),
        1
    );
    assert!(
        delivery
            .targets()
            .values()
            .all(|t| t.old_projection().is_some() && t.new_projection().is_some())
    );
}
#[test]
fn retired_and_added_processes_remain_in_the_union_with_their_own_projection() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let mut v = old.value().clone();
    let p = v["processes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|p| p["id"] == "recorder-a")
        .unwrap();
    p["id"] = "recorder-new".into();
    let next = pinned(v.clone(), policy(&v), &mut catalog);
    let delivery = ProjectionDelivery::prepare(
        &old,
        &prepared(&old, &next),
        &catalog,
        ProjectionLimits::default(),
    )
    .unwrap();
    assert_eq!(
        delivery.targets()["recorder-a"].kind(),
        DeliveryKind::Retired
    );
    assert!(delivery.targets()["recorder-a"].new_projection().is_none());
    assert_eq!(
        delivery.targets()["recorder-new"].kind(),
        DeliveryKind::Added
    );
    assert!(
        delivery.targets()["recorder-new"]
            .old_projection()
            .is_none()
    );
    assert_eq!(
        delivery.targets().len(),
        old.value()["processes"].as_array().unwrap().len() + 1
    );
    assert!(matches!(
        ProjectionDelivery::prepare(
            &old,
            &prepared(&old, &next),
            &catalog,
            ProjectionLimits {
                max_targets: old.value()["processes"].as_array().unwrap().len(),
                ..ProjectionLimits::default()
            }
        ),
        Err(Error::Capacity)
    ));
}
#[test]
fn set_and_delivery_enforce_total_payload_and_target_limits() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let set = ProjectionSet::build(&old, &catalog, ProjectionLimits::default()).unwrap();
    let exact = ProjectionLimits {
        max_targets: set.targets().len(),
        max_total_bytes: set.stored_bytes(),
    };
    assert!(ProjectionSet::build(&old, &catalog, exact).is_ok());
    for limits in [
        ProjectionLimits {
            max_targets: exact.max_targets - 1,
            ..exact
        },
        ProjectionLimits {
            max_total_bytes: exact.max_total_bytes - 1,
            ..exact
        },
        ProjectionLimits {
            max_targets: 0,
            ..exact
        },
    ] {
        assert!(ProjectionSet::build(&old, &catalog, limits).is_err());
    }
    let p = prepared(&old, &old);
    assert!(matches!(
        ProjectionDelivery::prepare(&old, &p, &catalog, exact),
        Err(Error::Capacity)
    ));
    let both = ProjectionLimits {
        max_total_bytes: exact.max_total_bytes * 2,
        ..exact
    };
    let result = ProjectionDelivery::prepare(&old, &p, &catalog, both).unwrap();
    assert_eq!(result.stored_bytes(), exact.max_total_bytes * 2);
    assert!(
        result
            .targets()
            .values()
            .all(|t| t.kind() == DeliveryKind::RevisionOnly)
    );
}
#[test]
fn absent_policy_and_wrong_base_cannot_produce_a_complete_delivery() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    assert!(ProjectionSet::build(&old, &crate::catalog(), ProjectionLimits::default()).is_err());
    let mut v = old.value().clone();
    v["strategies"][0]["params"]["signal_window_events"] = 2048.into();
    let other = parse(&v);
    assert!(matches!(
        ProjectionDelivery::prepare(
            &other,
            &prepared(&old, &other),
            &catalog,
            ProjectionLimits::default()
        ),
        Err(Error::Revision)
    ));
}

#[test]
fn retired_target_gets_its_original_payload_bound_to_the_new_delivery_hash() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let mut v = old.value().clone();
    v["processes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|p| p["id"] == "recorder-a")
        .unwrap()["id"] = "recorder-new".into();
    let next = pinned(v.clone(), policy(&v), &mut catalog);
    let delivery = ProjectionDelivery::prepare(
        &old,
        &prepared(&old, &next),
        &catalog,
        ProjectionLimits::default(),
    )
    .unwrap();
    for process in ["recorder-a", "recorder-new", "strategy-01-active"] {
        let object =
            ProjectionObject::for_delivery(2, &delivery, process, 4 * 1024 * 1024).unwrap();
        assert_eq!(object.binding().global_hash, next.hash());
        assert_eq!(object.binding().revision, 2);
        assert_eq!(object.binding().process_id, process);
        assert_eq!(object.payload()["process"]["id"], process);
    }
    let retired =
        ProjectionObject::for_delivery(2, &delivery, "recorder-a", 4 * 1024 * 1024).unwrap();
    let source = delivery.targets()["recorder-a"].old_projection().unwrap();
    assert_eq!(retired.payload(), source.value());
    assert_eq!(retired.binding().projection_hash, source.hash());
    assert_ne!(retired.binding().global_hash, source.global_hash());
    assert!(ProjectionObject::for_delivery(2, &delivery, "unconfigured", 4 * 1024 * 1024).is_err());
}
