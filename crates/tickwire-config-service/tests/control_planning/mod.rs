use super::*;
use tickwire_config::{patch::Head, plan::PlanOptions};
use tickwire_config_service::planning::{PlanInput, PlanProgress};
use tickwire_control_raft::driver::PeerDriver;

#[test]
fn computed_plan_waits_for_two_real_reads_and_preserves_the_committed_basis() {
    let tls = Fixture::new(&[SUBJECT], false);
    let sources = deployment_fixture();
    let candidate = sources.candidate.clone();
    let hash = candidate.hash();
    let mut cluster = ApiCluster::new(&candidate);
    cluster.nodes[0]
        .control()
        .install_deployment_owner(source_owner(&sources))
        .unwrap();
    let trust = tls.server();
    let (result, replied) = exchange(trust.clone(), tls.client(true), move |principal| {
        let before = cluster.nodes[0].scanned_through();
        let etag = Head::new(1, &candidate).unwrap().etag();
        let mut pending = cluster.nodes[0]
            .control()
            .begin_plan(
                &trust,
                principal,
                PlanInput::Full(candidate),
                etag,
                PlanOptions::default(),
            )
            .unwrap();
        cluster.pump(true);
        assert!(
            cluster.nodes[0]
                .control()
                .poll_plan(&trust, &mut pending)
                .unwrap()
                .is_none()
        );
        assert_eq!(pending.progress(), PlanProgress::ReadingBasis);
        for _ in 0..6 {
            cluster.nodes[0].tick().unwrap();
        }
        cluster.pump(false);
        assert!(
            cluster.nodes[0]
                .control()
                .poll_plan(&trust, &mut pending)
                .unwrap()
                .is_none()
        );
        assert_eq!(pending.progress(), PlanProgress::Computing);
        let deadline = Instant::now() + Duration::from_secs(15);
        while pending.progress() == PlanProgress::Computing {
            assert!(Instant::now() < deadline);
            assert!(
                cluster.nodes[0]
                    .control()
                    .poll_plan(&trust, &mut pending)
                    .unwrap()
                    .is_none()
            );
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(pending.progress(), PlanProgress::RecheckingBasis);
        cluster.pump(true);
        assert!(
            cluster.nodes[0]
                .control()
                .poll_plan(&trust, &mut pending)
                .unwrap()
                .is_none()
        );
        for _ in 0..6 {
            cluster.nodes[0].tick().unwrap();
        }
        cluster.pump(false);
        let draft = cluster.nodes[0]
            .control()
            .poll_plan(&trust, &mut pending)
            .unwrap()
            .unwrap();
        assert_eq!(draft.base().revision(), 1);
        assert_eq!(draft.base().candidate().hash(), hash);
        assert_eq!(draft.plan().candidate_hash(), hash);
        assert_eq!(
            draft.plan().targets().len(),
            draft.delivery().targets().len()
        );
        assert_eq!(draft.subject(), SUBJECT);
        assert_eq!(
            draft.checks_pending(),
            &[
                "retained_registry_head",
                "runtime_targets",
                "runtime_apply_driver",
                "durable_plan_admission"
            ]
        );
        assert_eq!(cluster.nodes[0].scanned_through(), before);
        assert_eq!(pending.progress(), PlanProgress::Completed);
        assert!(
            cluster.nodes[0]
                .control()
                .poll_plan(&trust, &mut pending)
                .is_err()
        );
    });
    assert!(result.is_ok() && replied);
}

#[test]
fn plan_rejects_stale_etag_and_current_roles_cannot_be_supplied_by_candidate() {
    for stale in [false, true] {
        let tls = Fixture::new(&[SUBJECT], false);
        let sources = deployment_fixture();
        let candidate = sources.candidate.clone();
        let mut base = candidate.value().clone();
        if !stale {
            base["control"]["role_bindings"][0]["role"] = "viewer".into();
        }
        let base = deployment_support::parse(&base);
        let mut cluster = ApiCluster::new(&base);
        cluster.nodes[0]
            .control()
            .install_deployment_owner(source_owner(&sources))
            .unwrap();
        let trust = tls.server();
        let (result, replied) = exchange(trust.clone(), tls.client(true), move |principal| {
            let etag = Head::new(if stale { 2 } else { 1 }, &base).unwrap().etag();
            let mut pending = cluster.nodes[0]
                .control()
                .begin_plan(
                    &trust,
                    principal,
                    PlanInput::Full(candidate),
                    etag,
                    PlanOptions::default(),
                )
                .unwrap();
            cluster.pump(false);
            let error = cluster.nodes[0]
                .control()
                .poll_plan(&trust, &mut pending)
                .err()
                .unwrap();
            assert_eq!(
                error,
                Error::Configuration(if stale {
                    tickwire_config::Error::Revision
                } else {
                    tickwire_config::Error::Semantic("authorization")
                })
            );
            assert_eq!(pending.progress(), PlanProgress::Completed);
            assert!(
                !cluster.nodes[0]
                    .control()
                    .cancel_plan(&mut pending)
                    .unwrap()
            );
        });
        assert!(result.is_ok() && replied);
    }
}

#[test]
fn plan_ticket_cannot_move_to_another_replica_and_cancellation_is_terminal() {
    let tls = Fixture::new(&[SUBJECT], false);
    let sources = deployment_fixture();
    let candidate = sources.candidate.clone();
    let mut cluster = ApiCluster::new(&candidate);
    let trust = tls.server();
    let (result, replied) = exchange(trust.clone(), tls.client(true), move |principal| {
        let etag = Head::new(1, &candidate).unwrap().etag();
        let mut pending = cluster.nodes[0]
            .control()
            .begin_plan(
                &trust,
                principal,
                PlanInput::Full(candidate),
                etag,
                PlanOptions::default(),
            )
            .unwrap();
        assert_eq!(
            cluster.nodes[1]
                .control()
                .poll_plan(&trust, &mut pending)
                .err(),
            Some(Error::Identity)
        );
        assert_eq!(
            cluster.nodes[1].control().cancel_plan(&mut pending),
            Err(Error::Identity)
        );
        assert_eq!(pending.progress(), PlanProgress::ReadingBasis);
        assert!(
            cluster.nodes[0]
                .control()
                .cancel_plan(&mut pending)
                .unwrap()
        );
        cluster.pump(false);
        assert_eq!(
            cluster.nodes[0]
                .control()
                .poll_plan(&trust, &mut pending)
                .err(),
            Some(Error::NotReady)
        );
    });
    assert!(result.is_ok() && replied);
}

#[test]
fn plan_rechecks_provider_and_certificate_after_background_computation() {
    for revoke in [false, true] {
        let tls = Fixture::new(&[SUBJECT], false);
        let revoked = ServerTls::new(tls.material(true, false), vec![tls.peer()]).unwrap();
        let sources = deployment_fixture();
        let candidate = sources.candidate.clone();
        let mut cluster = ApiCluster::new(&candidate);
        cluster.nodes[0]
            .control()
            .install_deployment_owner(source_owner(&sources))
            .unwrap();
        let trust = tls.server();
        let (result, replied) = exchange(trust.clone(), tls.client(true), move |principal| {
            let etag = Head::new(1, &candidate).unwrap().etag();
            let mut pending = cluster.nodes[0]
                .control()
                .begin_plan(
                    &trust,
                    principal,
                    PlanInput::Full(candidate),
                    etag,
                    PlanOptions::default(),
                )
                .unwrap();
            cluster.pump(false);
            assert!(
                cluster.nodes[0]
                    .control()
                    .poll_plan(&trust, &mut pending)
                    .unwrap()
                    .is_none()
            );
            let deadline = Instant::now() + Duration::from_secs(15);
            while pending.progress() == PlanProgress::Computing {
                assert!(Instant::now() < deadline);
                assert!(
                    cluster.nodes[0]
                        .control()
                        .poll_plan(&trust, &mut pending)
                        .unwrap()
                        .is_none()
                );
                thread::sleep(Duration::from_millis(1));
            }
            if !revoke {
                cluster.nodes[0]
                    .control()
                    .replace_deployment_sources(
                        tickwire_config_service::source_bundle::Bundle::decode(
                            &sources.bundle_bytes(),
                            Default::default(),
                        )
                        .unwrap(),
                        sources.trust(1),
                        Default::default(),
                    )
                    .unwrap();
            }
            cluster.pump(false);
            assert!(
                cluster.nodes[0]
                    .control()
                    .poll_plan(if revoke { &revoked } else { &trust }, &mut pending)
                    .is_err()
            );
            assert_eq!(pending.progress(), PlanProgress::Completed);
        });
        assert!(result.is_ok() && replied);
    }
}

#[test]
fn scoped_risk_edit_keeps_all_revision_recipients_in_the_internal_plan() {
    use tickwire_config::{patch::ScopedPatch, plan::Action};
    let tls = Fixture::new(&[SUBJECT], false);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let sources = deployment_support::Fixture::new_at(
        now,
        |v| {
            v["control"]["role_bindings"] = serde_json::json!([{
                "subject": SUBJECT, "role": "config-editor", "account_ids": ["spot-main"], "cluster_scope": false
            }]);
        },
        |_, _| {},
    );
    let next = &sources.candidate;
    let mut base = next.value().clone();
    base["risk_policies"][0]["max_gross_quote"] = "20000000".into();
    let base = deployment_support::parse(&base);
    base.validate_static().unwrap();
    let etag = Head::new(1, &base).unwrap().etag();
    let patch = ScopedPatch::parse(&serde_json::to_vec(&serde_json::json!({
        "base_etag": etag,
        "changes": [{"op": "replace", "selector": {"collection":"risk_policies", "id": next.value()["risk_policies"][0]["id"]},
            "path": "/max_gross_quote", "value": next.value()["risk_policies"][0]["max_gross_quote"]}]
    })).unwrap(), Limits::default()).unwrap();
    let expected_hash = next.hash();
    let expected_count = next.value()["processes"].as_array().unwrap().len();
    let mut cluster = ApiCluster::new(&base);
    cluster.nodes[0]
        .control()
        .install_deployment_owner(source_owner(&sources))
        .unwrap();
    let trust = tls.server();
    let (result, replied) = exchange(trust.clone(), tls.client(true), move |principal| {
        let mut pending = cluster.nodes[0]
            .control()
            .begin_plan(
                &trust,
                principal,
                PlanInput::Patch(patch),
                etag,
                PlanOptions::default(),
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        let draft = loop {
            cluster.pump(false);
            if let Some(draft) = cluster.nodes[0]
                .control()
                .poll_plan(&trust, &mut pending)
                .unwrap()
            {
                break draft;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        };
        assert_eq!(draft.plan().candidate_hash(), expected_hash);
        assert_eq!(draft.plan().targets().len(), expected_count);
        assert!(
            draft
                .plan()
                .targets()
                .values()
                .any(|t| t.action() == Action::Hot)
        );
        assert!(
            draft
                .plan()
                .targets()
                .values()
                .any(|t| t.action() == Action::NoOpRevisionAdvance)
        );
        assert_ne!(
            draft.plan().candidate_hash(),
            draft.base().candidate().hash()
        );
    });
    assert!(result.is_ok() && replied);
}
