#[path = "plan/support.rs"]
mod support;
use support::*;
use tickwire_config::{
    Candidate,
    changes::Class,
    delivery::{ProjectionDelivery, ProjectionLimits},
    plan::{Action, ApplyCeiling, ComputedPlan, ExposurePolicy, PlanOptions, Precondition},
};

fn compute(
    old: &Candidate,
    next: &Candidate,
    catalog: &tickwire_config::artifacts::ArtifactCatalog,
    revision: u64,
    options: PlanOptions,
) -> tickwire_config::Result<ComputedPlan> {
    let prepared = prepared(old, next);
    let delivery =
        ProjectionDelivery::prepare(old, &prepared, catalog, ProjectionLimits::default())?;
    ComputedPlan::compute(revision, old, &prepared, &delivery, options)
}

#[test]
fn private_strategy_change_keeps_every_target_without_restarting_unrelated_processes() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let mut v = old.value().clone();
    v["strategies"][0]["params"]["signal_window_events"] = 2048.into();
    let next = parse(&v);
    let plan = compute(&old, &next, &catalog, 7, PlanOptions::default()).unwrap();
    assert_eq!(
        plan.targets().len(),
        old.value()["processes"].as_array().unwrap().len()
    );
    assert_eq!(
        plan.targets()["strategy-01-active"].action(),
        Action::Quiesce
    );
    assert_eq!(
        plan.targets()["exec-spot-a"].action(),
        Action::NoOpRevisionAdvance
    );
    assert!(
        plan.targets()
            .values()
            .all(|t| t.preconditions().contains(&Precondition::RuntimeCapability))
    );
    let options = PlanOptions {
        apply_ceiling: ApplyCeiling::Hot,
        ..Default::default()
    };
    assert!(compute(&old, &next, &catalog, 7, options).is_err());
}

#[test]
fn same_candidate_hash_at_different_revisions_has_different_plan_identity() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let a = compute(&old, &old, &catalog, 1, PlanOptions::default()).unwrap();
    let b = compute(&old, &old, &catalog, 2, PlanOptions::default()).unwrap();
    assert_eq!(a.base_revision(), 1);
    assert_ne!(a.digest(), b.digest());
    for invalid in [0, (1u64 << 63) - 1, u64::MAX] {
        assert!(compute(&old, &old, &catalog, invalid, PlanOptions::default()).is_err());
    }
}

#[test]
fn lease_changes_quiesce_consumers_even_without_control_fields_in_their_projection() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let mut v = old.value().clone();
    v["control"]["strategy_suspect_after_ms"] = 900.into();
    let next = parse(&v);
    let p = prepared(&old, &next);
    let d = ProjectionDelivery::prepare(&old, &p, &catalog, ProjectionLimits::default()).unwrap();
    let target = &d.targets()["strategy-01-active"];
    assert_eq!(
        target.old_projection().unwrap().hash(),
        target.new_projection().unwrap().hash()
    );
    let plan = ComputedPlan::compute(1, &old, &p, &d, PlanOptions::default()).unwrap();
    assert_eq!(
        plan.targets()["strategy-01-active"].required_class(),
        Some(Class::Quiesce)
    );
}

#[test]
fn target_hints_never_trim_closure_and_unknown_or_missing_targets_are_rejected() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let plan = compute(
        &old,
        &old,
        &catalog,
        1,
        PlanOptions {
            requested_target_ids: ["strategy-01-active".into()].into(),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        plan.targets().len(),
        old.value()["processes"].as_array().unwrap().len()
    );
    assert!(
        compute(
            &old,
            &old,
            &catalog,
            1,
            PlanOptions {
                requested_target_ids: ["missing".into()].into(),
                ..Default::default()
            }
        )
        .is_err()
    );
    let mut bad_policy = policy(&v);
    bad_policy["targets"].as_array_mut().unwrap().pop();
    let next = pinned(v, bad_policy, &mut catalog);
    assert!(compute(&old, &next, &catalog, 1, PlanOptions::default()).is_err());
}

#[test]
fn base_and_candidate_cannot_be_spliced_with_another_delivery() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let mut v = old.value().clone();
    v["observability"]["diagnostic_level"] = "debug".into();
    let next = parse(&v);
    let p = prepared(&old, &next);
    let d = ProjectionDelivery::prepare(&old, &p, &catalog, ProjectionLimits::default()).unwrap();
    assert!(ComputedPlan::compute(1, &next, &p, &d, PlanOptions::default()).is_err());
    let p2 = prepared(&old, &old);
    assert!(ComputedPlan::compute(1, &old, &p2, &d, PlanOptions::default()).is_err());
}

#[test]
fn risk_direction_exposure_policy_and_unrelated_account_are_preserved() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    for (gross, class) in [
        ("10000.00", Class::Hot),
        ("999999999999999999", Class::Quiesce),
    ] {
        let mut v = old.value().clone();
        v["risk_policies"][0]["max_gross_quote"] = gross.into();
        let next = parse(&v);
        let plan = compute(&old, &next, &catalog, 1, PlanOptions::default()).unwrap();
        assert_eq!(plan.targets()["exec-spot-a"].required_class(), Some(class));
        assert!(
            plan.targets()["exec-spot-a"]
                .preconditions()
                .contains(&Precondition::Exposure)
        );
        assert_eq!(
            plan.targets()["exec-usdm-a"].action(),
            Action::NoOpRevisionAdvance
        );
        let freeze = compute(
            &old,
            &next,
            &catalog,
            1,
            PlanOptions {
                exposure_policy: ExposurePolicy::FreezeUntilWithin,
                ..Default::default()
            },
        )
        .unwrap();
        assert_ne!(freeze.digest(), plan.digest());
        let cancel = compute(
            &old,
            &next,
            &catalog,
            1,
            PlanOptions {
                exposure_policy: ExposurePolicy::CancelThenFreeze,
                ..Default::default()
            },
        )
        .unwrap();
        assert_ne!(freeze.digest(), cancel.digest());
    }
}

#[test]
fn retirement_and_addition_keep_each_side_and_require_real_lifecycle_evidence() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let mut v = old.value().clone();
    let process = v["processes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|p| p["id"] == "recorder-a")
        .unwrap();
    process["id"] = "recorder-new".into();
    let next = pinned(v.clone(), policy(&v), &mut catalog);
    let plan = compute(&old, &next, &catalog, 1, PlanOptions::default()).unwrap();
    let old_target = &plan.targets()["recorder-a"];
    let new_target = &plan.targets()["recorder-new"];
    assert_eq!(old_target.action(), Action::Retire);
    assert_eq!(new_target.action(), Action::RollingRestart);
    assert!(old_target.old_projection().is_some() && old_target.new_projection().is_none());
    assert!(new_target.old_projection().is_none() && new_target.new_projection().is_some());
    assert!(
        old_target
            .preconditions()
            .contains(&Precondition::Retirement)
    );
    assert!(
        new_target
            .preconditions()
            .contains(&Precondition::PlannedSuccessor)
    );
    assert!(
        !new_target
            .preconditions()
            .contains(&Precondition::EffectiveBaseline)
    );
    assert!(
        compute(
            &old,
            &next,
            &catalog,
            1,
            PlanOptions {
                apply_ceiling: ApplyCeiling::Quiesce,
                ..Default::default()
            }
        )
        .is_err()
    );
}

#[test]
fn producer_replacement_affects_consumers_and_host_supervisor_but_not_other_product() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let mut v = old.value().clone();
    let process = v["processes"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|p| p["id"] == "md-spot-a")
        .unwrap();
    process["memory_limit_bytes"] = 128_000_000.into();
    let next = parse(&v);
    let p = prepared(&old, &next);
    let d = ProjectionDelivery::prepare(&old, &p, &catalog, ProjectionLimits::default()).unwrap();
    let consumer = &d.targets()["strategy-01-active"];
    assert_eq!(
        consumer.old_projection().unwrap().hash(),
        consumer.new_projection().unwrap().hash()
    );
    let plan = ComputedPlan::compute(1, &old, &p, &d, PlanOptions::default()).unwrap();
    for id in ["md-spot-a", "supervisor-host-a", "strategy-01-active"] {
        assert_eq!(plan.targets()[id].action(), Action::RollingRestart);
    }
    assert_eq!(
        plan.targets()["md-usdm-a"].action(),
        Action::NoOpRevisionAdvance
    );
}

#[test]
fn canonical_digest_is_reproducible_and_binds_all_options_without_claiming_readiness() {
    use sha2::{Digest, Sha256};
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let a = compute(&old, &old, &catalog, 1u64 << 54, PlanOptions::default()).unwrap();
    let b = compute(&old, &old, &catalog, 1u64 << 54, PlanOptions::default()).unwrap();
    assert_eq!(a.canonical_bytes(), b.canonical_bytes());
    assert_eq!(a.base_hash(), old.hash());
    assert_eq!(a.candidate_hash(), old.hash());
    let mut hash = Sha256::new();
    hash.update(b"tickwire.config.computed-plan.v1\0");
    hash.update(a.canonical_bytes());
    assert_eq!(a.digest(), <[u8; 32]>::from(hash.finalize()));
    let value: serde_json::Value = serde_json::from_slice(a.canonical_bytes()).unwrap();
    assert_eq!(value["base_revision"], (1u64 << 54).to_string());
    assert!(value.get("ready").is_none());
    assert!(
        a.preconditions()
            .contains(&Precondition::ContentQualification)
    );
    assert!(a.preconditions().contains(&Precondition::CurrentAuthority));
}

#[test]
fn independent_changes_keep_their_own_class_and_cause_paths() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let mut v = old.value().clone();
    v["strategies"][0]["params"]["additional_signal"] = 1.into();
    v["observability"]["diagnostic_level"] = "debug".into();
    let next = parse(&v);
    let plan = compute(&old, &next, &catalog, 1, PlanOptions::default()).unwrap();
    assert_eq!(
        plan.targets()["strategy-01-active"].action(),
        Action::Quiesce
    );
    assert_eq!(plan.targets()["exec-spot-a"].action(), Action::Hot);
    assert!(
        plan.targets()["strategy-01-active"]
            .changes()
            .contains("/strategies/@strategy-01-active/params/additional_signal")
    );
}

#[test]
fn changing_credentials_and_directory_keeps_explicit_transition_preconditions() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let mut v = old.value().clone();
    v["accounts"][0]["credential_ref"] = "secret://fixture/spot/execution/2".into();
    v["local_config"]["root_dir"] = "/var/lib/tickwire/next".into();
    let next = parse(&v);
    let plan = compute(&old, &next, &catalog, 1, PlanOptions::default()).unwrap();
    assert!(
        plan.targets()["exec-spot-a"]
            .preconditions()
            .contains(&Precondition::CredentialHandoff)
    );
    assert!(plan.targets().values().all(|t| {
        t.preconditions()
            .contains(&Precondition::DirectoryMigration)
    }));
}

#[test]
fn plan_metadata_is_bounded_even_when_the_candidate_and_projection_fit() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let mut v = old.value().clone();
    v["strategies"][0]["params"]["x".repeat(tickwire_config::plan::MAX_BYTES)] = 1.into();
    let next = parse(&v);
    assert!(matches!(
        compute(&old, &next, &catalog, 1, PlanOptions::default()),
        Err(tickwire_config::Error::Capacity)
    ));
}

#[test]
fn one_account_strategy_budget_does_not_reconfigure_unrelated_market_publishers() {
    let v = base();
    let mut catalog = catalog();
    let old = pinned(v.clone(), policy(&v), &mut catalog);
    let mut v = old.value().clone();
    v["strategies"][0]["risk_budgets"][0]["max_gross_quote"] = "1000.00".into();
    let next = parse(&v);
    let plan = compute(&old, &next, &catalog, 1, PlanOptions::default()).unwrap();
    assert_eq!(plan.targets()["exec-spot-a"].action(), Action::Hot);
    assert_eq!(
        plan.targets()["exec-usdm-a"].action(),
        Action::NoOpRevisionAdvance
    );
    for id in ["md-spot-a", "md-usdm-a"] {
        assert_eq!(plan.targets()[id].action(), Action::NoOpRevisionAdvance);
    }
}
