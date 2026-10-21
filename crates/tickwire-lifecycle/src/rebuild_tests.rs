use super::*;

fn rebuild_answer() -> RebuildPrepared {
    let mut cfg = config();
    cfg.operation.schema = 4;
    let member = cfg.members[0].member.clone();
    let prepare = signer(50)
        .sign_request(&ControlRequest {
            schema: 3,
            coordinator_id: cfg.coordinator_id,
            operation: cfg.operation.clone(),
            member: member.clone(),
            request_id: [70; 16],
            action: ControlAction::Prepare,
            proof: cfg.members[0].readiness.clone(),
            release: None,
            replay: None,
            rebuild: None,
            valid_until_ns: member.valid_until_ns,
        })
        .unwrap();
    let object = |x| ReplayObject {
        digest: [x; 32],
        bytes: 128,
    };
    let cursor = |x| RebuildCursor {
        identity: [x; 32],
        generation: 1,
        sequence: 2,
        content_digest: [x + 1; 32],
    };
    RebuildPrepared {
        schema: 1,
        operation: cfg.operation,
        member,
        prepare: prepare.clone(),
        challenge: prepare,
        roots: RebuildRoots {
            plans: object(2),
            contract: object(30),
            inputs: object(31),
            rollback_plans: object(3),
            warm_completion: object(32),
            target_install: object(33),
            rollback_completion: object(34),
        },
        end: RebuildFrontier {
            now_ns: 1900,
            rows: 2,
            markets: vec![cursor(40)],
            accounts: vec![cursor(50)],
            external: vec![],
        },
        readiness: Readiness {
            checkpoint_digest: [60; 32],
            exec_report_sequence: 11,
            orders_digest: [61; 32],
            ledger_sequence: 4,
            input_seal_digest: [62; 32],
        },
        state_version: 1,
        risk_gate_revision: 7,
        issued_ns: 2000,
        valid_until_ns: 9000,
    }
}

#[test]
fn rebuild_prepared_has_a_separate_signature_and_fixed_window() {
    let value = rebuild_answer();
    let key = signer(value.member.risk_domain[0]);
    let signed = key
        .sign_rebuild_prepared(&value)
        .expect("schema4 actual rebuild result can be signed");
    assert_eq!(
        verify_rebuild_prepared(&signed, key.public_key(), &value.operation, now(2)).unwrap(),
        value
    );
    assert_eq!(
        verify_rebuild_prepared(&signed, signer(99).public_key(), &value.operation, now(2)),
        Err(Error::Signature)
    );
    let replay = SignedReplayPrepared {
        payload: signed.payload.clone(),
        signature: signed.signature.clone(),
    };
    assert_eq!(
        verify_replay_prepared(&replay, key.public_key(), &value.operation, now(2)),
        Err(Error::Signature)
    );
    assert_eq!(
        verify_rebuild_prepared(&signed, key.public_key(), &value.operation, now(1000)),
        Err(Error::Expired)
    );
}

fn for_challenge(original: SignedControlRequest) -> RebuildPrepared {
    let r = request(&original);
    let mut value = rebuild_answer();
    value.operation = r.operation;
    value.member = r.member;
    value.prepare = original.clone();
    value.challenge = original;
    value
}

#[test]
fn coordinator_rebuild_adopts_all_members_before_generating_bound_commit() {
    let mut cfg = config();
    cfg.operation.schema = 4;
    let mut owner =
        Coordinator::create(Mem::default(), cfg.clone(), 1, signer(50), now(1)).unwrap();
    let mut accepted = vec![];
    for challenge in owner.pending_requests().to_vec() {
        let value = for_challenge(challenge);
        let signed = signer(value.member.risk_domain[0])
            .sign_rebuild_prepared(&value)
            .unwrap();
        owner
            .ingest_rebuild(signed.clone(), owner.version(), now(2))
            .expect("persist and adopt actual rebuild result");
        assert!(
            owner
                .ingest_rebuild(signed.clone(), 0, now(0))
                .unwrap()
                .duplicate
        );
        accepted.push((signed, value));
    }
    assert_eq!(owner.stage(), Stage::PreparedAll);
    owner.begin_commit(owner.version(), now(3)).unwrap();
    for commit in owner.pending_requests() {
        let r = request(commit);
        let (signed, value) = accepted.iter().find(|(_, p)| p.member == r.member).unwrap();
        assert_eq!(r.schema, 3);
        assert!(r.replay.is_none());
        assert_eq!(r.proof.as_ref(), Some(&value.readiness));
        assert_eq!(
            r.rebuild.as_ref(),
            Some(&RebuildAdoption::from_receipt(signed).unwrap())
        );
        verify_control_request(
            commit,
            cfg.coordinator_key,
            cfg.coordinator_id,
            &r.member,
            member_now(&r.member, 3000),
        )
        .unwrap();
        for case in 0..3 {
            let mut changed = r.clone();
            match case {
                0 => changed.rebuild = None,
                1 => changed.proof.as_mut().unwrap().checkpoint_digest = [99; 32],
                2 => changed.rebuild.as_mut().unwrap().roots.inputs.bytes = 0,
                _ => unreachable!(),
            }
            let signed = signer(50).sign_request(&changed).unwrap();
            assert!(
                verify_control_request(
                    &signed,
                    cfg.coordinator_key,
                    cfg.coordinator_id,
                    &r.member,
                    member_now(&r.member, 3000)
                )
                .is_err(),
                "commit case {case}"
            );
        }
    }
}

#[test]
fn rebuild_prepared_sync_failure_never_exposes_a_commit_or_adopted_readiness() {
    let mut c = config();
    c.operation.schema = 4;
    let store = Mem::default();
    let mut owner = Coordinator::create(store.clone(), c, 1, signer(50), now(1)).unwrap();
    let r = for_challenge(owner.pending_requests()[0].clone());
    let s = signer(r.member.risk_domain[0])
        .sign_rebuild_prepared(&r)
        .unwrap();
    store.0.borrow_mut().fail_sync = true;
    assert_eq!(
        owner.ingest_rebuild(s, owner.version(), now(2)),
        Err(Error::Store)
    );
    assert_eq!(owner.stage(), Stage::Frozen);
    assert!(owner.pending_requests().is_empty());
    assert!(owner.latest_receipt(r.member.risk_domain).is_none());
}

#[test]
fn lost_rebuild_adoption_is_recovered_as_history_then_fresh_prepare_before_commit() {
    let mut cfg = config();
    cfg.operation.schema = 4;
    let store = Mem::default();
    let owner = Coordinator::create(store.clone(), cfg.clone(), 1, signer(50), now(1)).unwrap();
    let originals = owner.pending_requests().to_vec();
    drop(owner);
    let mut owner = Coordinator::recover(store, cfg, 2, signer(50), None, now(2)).unwrap();
    assert_eq!(
        owner.resume_after_queries(owner.version(), now(3)),
        Err(Error::Phase)
    );
    for query in owner.pending_requests().to_vec() {
        let q = request(&query);
        let original = originals
            .iter()
            .find(|r| request(r).member == q.member)
            .unwrap()
            .clone();
        let mut prepared = for_challenge(original);
        prepared.valid_until_ns = 6000;
        let prepared = signer(q.member.risk_domain[0])
            .sign_rebuild_prepared(&prepared)
            .unwrap();
        let mut applied = answer(&q, AppliedPhase::Prepared, 5);
        applied.issued_ns = 7000;
        applied.valid_until_ns = 9000;
        let observed = signer(q.member.risk_domain[0])
            .sign_rebuild_observed(&RebuildObserved {
                schema: 1,
                query,
                applied,
                prepared,
            })
            .unwrap();
        owner
            .ingest_rebuild_observed(observed, owner.version(), now(4))
            .unwrap();
    }
    assert_eq!(owner.stage(), Stage::AwaitingResume);
    owner.resume_after_queries(owner.version(), now(5)).unwrap();
    assert_eq!(owner.stage(), Stage::Preparing);
    assert_eq!(
        owner.begin_commit(owner.version(), now(6)),
        Err(Error::Phase)
    );
    for challenge in owner.pending_requests().to_vec() {
        let r = request(&challenge);
        assert_eq!(r.action, ControlAction::Prepare);
        let old = r.rebuild.as_ref().unwrap();
        assert!(originals.contains(&old.prepare));
        let mut refreshed = for_challenge(challenge);
        refreshed.prepare = old.prepare.clone();
        refreshed.state_version = 6;
        refreshed.issued_ns = 8000;
        refreshed.valid_until_ns = 9900;
        refreshed.readiness.exec_report_sequence = 12;
        let signed = signer(refreshed.member.risk_domain[0])
            .sign_rebuild_prepared(&refreshed)
            .unwrap();
        owner
            .ingest_rebuild(signed, owner.version(), now(7))
            .unwrap();
    }
    assert_eq!(owner.stage(), Stage::PreparedAll);
    owner.begin_commit(owner.version(), now(8)).unwrap();
    assert!(
        owner
            .pending_requests()
            .iter()
            .all(|r| request(r).proof.unwrap().exec_report_sequence == 12)
    );
}

#[test]
fn rebuild_shape_rejects_cross_binding_partial_window_and_unknown_fields() {
    let value = rebuild_answer();
    let key = signer(value.member.risk_domain[0]);
    for case in 0..21 {
        let mut bad = value.clone();
        match case {
            0 => bad.operation.schema = 3,
            1 => bad.member.exec_epoch += 1,
            2 => bad.roots.plans.digest = [90; 32],
            3 => bad.roots.rollback_plans.digest = [90; 32],
            4 => bad.roots.warm_completion.bytes = 0,
            5 => bad.roots.rollback_completion.digest = [0; 32],
            6 => bad.roots.target_install.bytes = 64 * 1024 * 1024 + 1,
            7 => bad.end.markets.clear(),
            8 => bad.end.accounts.clear(),
            9 => bad.end.rows = 65_537,
            10 => bad.end.markets.push(bad.end.markets[0].clone()),
            11 => bad.end.accounts[0].generation = 0,
            12 => bad.end.accounts[0].content_digest = [0; 32],
            13 => bad.end.now_ns = bad.issued_ns + 1,
            14 => bad.issued_ns = bad.valid_until_ns,
            15 => bad.state_version = 0,
            16 => bad.risk_gate_revision = 0,
            17 => bad.readiness.input_seal_digest = [0; 32],
            18 => bad.prepare.signature.pop().map(|_| ()).unwrap(),
            19 => {
                let mut r = request(&bad.challenge);
                r.coordinator_id = [1; 16];
                bad.challenge = signer(50).sign_request(&r).unwrap();
            }
            20 => {
                let mut r = request(&bad.challenge);
                r.request_id = [2; 16];
                bad.challenge = signer(50).sign_request(&r).unwrap();
            }
            _ => unreachable!(),
        }
        assert!(key.sign_rebuild_prepared(&bad).is_err(), "case {case}");
    }
    let mut json = serde_json::to_value(&value).unwrap();
    json.as_object_mut()
        .unwrap()
        .insert("source_checkpoint".into(), serde_json::json!([1, 2, 3]));
    let payload = serde_json::to_vec(&json).unwrap();
    let forged = SignedRebuildPrepared {
        signature: key.sign(b"tickwire.lifecycle.rebuild-prepared.v1", &payload),
        payload,
    };
    assert!(verify_rebuild_prepared(&forged, key.public_key(), &value.operation, now(2)).is_err());
}

#[test]
fn rebuild_refresh_keeps_all_fixed_roots_and_actual_cursor_vector() {
    let mut cfg = config();
    cfg.operation.schema = 4;
    let mut owner =
        Coordinator::create(Mem::default(), cfg.clone(), 1, signer(50), now(1)).unwrap();
    let original = for_challenge(owner.pending_requests()[0].clone());
    let key = signer(original.member.risk_domain[0]);
    let first = key.sign_rebuild_prepared(&original).unwrap();
    owner
        .ingest_rebuild(first.clone(), owner.version(), now(2))
        .unwrap();
    owner
        .refresh_rebuild(&original.member, owner.version(), now(3))
        .unwrap();
    let challenge = owner
        .pending_requests()
        .iter()
        .find(|p| request(p).member == original.member)
        .unwrap()
        .clone();
    let mut refreshed = original.clone();
    refreshed.challenge = challenge;
    refreshed.state_version += 1;
    refreshed.readiness.exec_report_sequence += 1;
    refreshed.readiness.ledger_sequence += 1;
    refreshed.readiness.checkpoint_digest = [80; 32];
    refreshed.readiness.input_seal_digest = [81; 32];
    for case in 0..11 {
        let mut bad = refreshed.clone();
        match case {
            0 => bad.roots.plans.bytes += 1,
            1 => bad.roots.contract.digest = [90; 32],
            2 => bad.roots.inputs.digest = [90; 32],
            3 => bad.roots.rollback_plans.bytes += 1,
            4 => bad.roots.warm_completion.digest = [90; 32],
            5 => bad.roots.target_install.digest = [90; 32],
            6 => bad.roots.rollback_completion.digest = [90; 32],
            7 => bad.end.markets[0].sequence += 1,
            8 => bad.end.now_ns += 1,
            9 => bad.readiness.exec_report_sequence = 0,
            10 => bad.readiness.ledger_sequence = 0,
            _ => unreachable!(),
        }
        assert!(
            key.sign_rebuild_prepared(&bad).is_err(),
            "refresh case {case}"
        );
    }
    let signed = key.sign_rebuild_prepared(&refreshed).unwrap();
    owner
        .ingest_rebuild(signed, owner.version(), now(4))
        .unwrap();
    assert_eq!(
        owner
            .latest_receipt(original.member.risk_domain)
            .unwrap()
            .state_version,
        2
    );
}

#[test]
fn schema4_rejects_legacy_prepared_and_forged_initial_challenge() {
    let mut cfg = config();
    cfg.operation.schema = 4;
    let mut owner = Coordinator::create(Mem::default(), cfg, 1, signer(50), now(1)).unwrap();
    let initial = owner.pending_requests()[0].clone();
    let mut value = for_challenge(initial.clone());
    let mut changed = request(&value.prepare);
    changed.request_id = [71; 16];
    value.prepare = signer(50).sign_request(&changed).unwrap();
    value.challenge = value.prepare.clone();
    let signed = signer(value.member.risk_domain[0])
        .sign_rebuild_prepared(&value)
        .unwrap();
    assert_eq!(
        owner.ingest_rebuild(signed, owner.version(), now(2)),
        Err(Error::Conflict)
    );
    assert_eq!(owner.pending_requests()[0], initial);
    let ordinary = answer(&request(&initial), AppliedPhase::Prepared, 1);
    assert_eq!(
        owner.ingest(sign(&ordinary), owner.version(), now(2)),
        Err(Error::Phase)
    );
    assert_eq!(owner.stage(), Stage::Halting);
}

#[test]
fn rebuild_release_and_restart_still_require_fresh_authenticated_queries() {
    let mut cfg = config();
    cfg.operation.schema = 4;
    let store = Mem::default();
    let mut owner = Coordinator::create(store.clone(), cfg.clone(), 1, signer(50), now(1)).unwrap();
    for challenge in owner.pending_requests().to_vec() {
        let value = for_challenge(challenge);
        let signed = signer(value.member.risk_domain[0])
            .sign_rebuild_prepared(&value)
            .unwrap();
        owner
            .ingest_rebuild(signed, owner.version(), now(2))
            .unwrap();
    }
    owner.begin_commit(owner.version(), now(3)).unwrap();
    all(&mut owner, AppliedPhase::Committed, 2, 4);
    owner.release(owner.version(), now(5)).unwrap();
    all(&mut owner, AppliedPhase::Released, 3, 6);
    assert_eq!(owner.stage(), Stage::ActiveAll);
    drop(owner);
    let mut recovered = Coordinator::recover(store, cfg, 2, signer(50), None, now(7)).unwrap();
    assert_eq!(recovered.stage(), Stage::Reconciling);
    assert_eq!(
        recovered.resume_after_queries(recovered.version(), now(8)),
        Err(Error::Phase)
    );
    all(&mut recovered, AppliedPhase::Released, 3, 9);
    assert_eq!(recovered.stage(), Stage::AwaitingResume);
    recovered
        .resume_after_queries(recovered.version(), now(10))
        .unwrap();
    assert_eq!(recovered.stage(), Stage::ActiveAll);
}

#[test]
fn rebuild_observation_rejects_halted_changed_gate_and_unissued_query() {
    let mut cfg = config();
    cfg.operation.schema = 4;
    let store = Mem::default();
    let owner = Coordinator::create(store.clone(), cfg.clone(), 1, signer(50), now(1)).unwrap();
    let original = owner.pending_requests()[0].clone();
    drop(owner);
    let value = for_challenge(original);
    let key = signer(value.member.risk_domain[0]);
    let historical = key.sign_rebuild_prepared(&value).unwrap();
    let mut owner = Coordinator::recover(store, cfg, 2, signer(50), None, now(2)).unwrap();
    let query = owner
        .pending_requests()
        .iter()
        .find(|r| request(r).member == value.member)
        .unwrap()
        .clone();
    let q = request(&query);
    let mut receipt = answer(&q, AppliedPhase::Prepared, 2);
    receipt.issued_ns = 3000;
    let valid = RebuildObserved {
        schema: 1,
        query,
        applied: receipt,
        prepared: historical,
    };
    for case in 0..6 {
        let mut bad = valid.clone();
        match case {
            0 => bad.applied.phase = AppliedPhase::Halted,
            1 => bad.applied.risk_gate_revision += 1,
            2 => bad.applied.member.exec_epoch += 1,
            3 => bad.applied.state_version = 0,
            4 => bad.applied.valid_until_ns = 10001,
            5 => bad.applied.control_request_id = [90; 16],
            _ => unreachable!(),
        }
        assert!(
            key.sign_rebuild_observed(&bad).is_err(),
            "observed case {case}"
        );
    }
    let mut swapped = valid.clone();
    let mut q = request(&swapped.query);
    q.request_id = [89; 16];
    swapped.query = signer(50).sign_request(&q).unwrap();
    swapped.applied.control_request_id = q.request_id;
    let signed = key.sign_rebuild_observed(&swapped).unwrap();
    assert_eq!(
        owner.ingest_rebuild_observed(signed, owner.version(), now(3)),
        Err(Error::Conflict)
    );
    let signed = key.sign_rebuild_observed(&valid).unwrap();
    owner
        .ingest_rebuild_observed(signed, owner.version(), now(3))
        .unwrap();
    assert_eq!(
        owner.begin_commit(owner.version(), now(4)),
        Err(Error::Phase)
    );
}

#[test]
fn old_control_bytes_do_not_gain_an_absent_rebuild_field() {
    let owner = owner(Mem::default());
    for signed in owner.pending_requests() {
        let value: serde_json::Value = serde_json::from_slice(&signed.payload).unwrap();
        assert!(value.get("rebuild").is_none());
        assert!(value.get("replay").is_none());
        let request: ControlRequest = serde_json::from_slice(&signed.payload).unwrap();
        assert_eq!(serde_json::to_vec(&request).unwrap(), signed.payload);
    }
}

#[test]
fn schema4_starts_without_business_ready_but_cannot_commit_or_release() {
    let mut value = serde_json::to_value(config()).unwrap();
    value["operation"]["schema"] = serde_json::json!(4);
    for member in value["members"].as_array_mut().unwrap() {
        member["readiness"] = serde_json::Value::Null;
    }
    let cfg: CoordinatorConfig =
        serde_json::from_value(value).expect("schema4 carries no fabricated initial READY");
    let mut owner =
        Coordinator::create(Mem::default(), cfg.clone(), 1, signer(50), now(1)).unwrap();
    for p in owner.pending_requests() {
        let r = request(p);
        assert!(r.proof.is_none());
        assert!(r.rebuild.is_none());
        verify_control_request(
            p,
            cfg.coordinator_key,
            cfg.coordinator_id,
            &r.member,
            member_now(&r.member, 100),
        )
        .unwrap();
    }
    assert_eq!(
        owner.begin_commit(owner.version(), now(2)),
        Err(Error::Phase)
    );
    assert_eq!(owner.release(owner.version(), now(2)), Err(Error::Phase));
    for challenge in owner.pending_requests().to_vec() {
        let value = for_challenge(challenge);
        let signed = signer(value.member.risk_domain[0])
            .sign_rebuild_prepared(&value)
            .unwrap();
        owner
            .ingest_rebuild(signed, owner.version(), now(3))
            .unwrap();
    }
    owner.begin_commit(owner.version(), now(4)).unwrap();
    assert!(
        owner
            .pending_requests()
            .iter()
            .all(|r| request(r).proof.is_some())
    );
}

#[test]
fn legacy_operation_rejects_missing_readiness_before_any_wal_write() {
    for schema in 1..=3 {
        for missing in [false, true] {
            let mut value = serde_json::to_value(config()).unwrap();
            value["operation"]["schema"] = serde_json::json!(schema);
            for member in value["members"].as_array_mut().unwrap() {
                if missing {
                    member.as_object_mut().unwrap().remove("readiness");
                } else {
                    member["readiness"] = serde_json::Value::Null;
                }
            }
            let cfg: CoordinatorConfig = serde_json::from_value(value).unwrap();
            let store = Mem::default();
            assert!(matches!(
                Coordinator::create(store.clone(), cfg, 1, signer(50), now(1)),
                Err(Error::Identity)
            ));
            assert!(store.0.borrow().segments.is_empty());
            assert!(store.0.borrow().manifest.is_none());
        }
    }
}

#[test]
fn some_readiness_preserves_the_existing_serialized_object_exactly() {
    let cfg = config();
    let expected = serde_json::json!({
        "checkpoint_digest":vec![2u8;32],"exec_report_sequence":10,
        "orders_digest":vec![4u8;32],"ledger_sequence":3,"input_seal_digest":vec![5u8;32],
    });
    let value = serde_json::to_value(&cfg).unwrap();
    for member in value["members"].as_array().unwrap() {
        assert_eq!(member["readiness"], expected);
    }
    let old_ready: Readiness = serde_json::from_value(expected).unwrap();
    assert_eq!(
        serde_json::to_vec(&Some(&old_ready)).unwrap(),
        serde_json::to_vec(&old_ready).unwrap()
    );
    let before = canonical(&cfg).unwrap();
    let decoded: CoordinatorConfig = decode_canonical(&before).unwrap();
    assert_eq!(canonical(&decoded).unwrap(), before);
}
