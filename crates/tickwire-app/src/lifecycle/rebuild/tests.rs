use super::*;
fn object(n: u8) -> protocol::ReplayObject {
    protocol::ReplayObject {
        digest: [n; 32],
        bytes: 1,
    }
}
fn facts() -> Facts {
    let mut state =
        super::super::operation_halt_tests::state(&super::super::operation_halt_tests::plane(), 6);
    state.operation.schema = 4;
    let request = protocol::ControlRequest {
        schema: 3,
        coordinator_id: [30; 16],
        operation: state.operation,
        member: state.member,
        request_id: [31; 16],
        action: ControlAction::Prepare,
        proof: None,
        release: None,
        replay: None,
        rebuild: None,
        valid_until_ns: 1000,
    };
    let cursor = protocol::RebuildCursor {
        identity: [8; 32],
        generation: 1,
        sequence: 2,
        content_digest: [9; 32],
    };
    Facts {
        prepare: SignedControlRequest {
            payload: serde_json::to_vec(&request).unwrap(),
            signature: vec![2; 64],
        },
        fixed: Fixed {
            request: object(10),
            capture_receipt: object(13),
            plans: object(11),
            contract: object(2),
            inputs: object(3),
            rollback_plans: object(12),
            capture_completion: object(5),
            capture_seal: object(6),
            end: protocol::RebuildFrontier {
                now_ns: 10,
                rows: 2,
                markets: vec![cursor.clone()],
                accounts: vec![cursor],
                external: vec![],
            },
        },
        warm: None,
        installation: None,
        rollback: None,
        result: None,
    }
}
#[test]
fn fixed_window_and_each_completed_stage_are_immutable() {
    let initial = facts();
    initial.follows(None).unwrap();
    let mut warm = initial.clone();
    warm.warm = Some(object(7));
    warm.follows(Some(&initial)).unwrap();
    let mut changed = warm.clone();
    changed.fixed.inputs = object(21);
    assert!(
        changed.follows(Some(&warm)).is_err(),
        "a later stage cannot replace the sealed window"
    );
    let mut changed = warm.clone();
    changed.warm = None;
    assert!(
        changed.follows(Some(&warm)).is_err(),
        "a permission halt cannot erase completed callbacks"
    );
}
#[test]
fn installation_and_probe_cannot_skip_their_durable_boundaries() {
    let initial = facts();
    let mut changed = initial.clone();
    changed.installation = Some(object(8));
    assert!(changed.follows(Some(&initial)).is_err());
    changed.warm = Some(object(7));
    changed.rollback = Some(object(9));
    assert!(changed.follows(Some(&initial)).is_err());
    assert!(changed.follows(None).is_err());
}

#[test]
fn uncertain_install_sync_keeps_memory_old_and_restart_preserves_frozen_facts() {
    let disk = replay::tests::Store::default();
    let config = tickwire_journal::Config {
        domain: [1; 16],
        stream_epoch: [2; 16],
        segment_max_bytes: 1 << 20,
    };
    let mut wal = protocol::EventJournal::create(disk.clone(), config.clone(), 1).unwrap();
    let mut states = BTreeMap::new();
    let mut state =
        super::super::operation_halt_tests::state(&super::super::operation_halt_tests::plane(), 6);
    state.operation.schema = 4;
    state.phase = AppliedPhase::Halted;
    state.rebuild = Some(facts());
    let key = (
        state.operation.operation_id,
        state.operation.operation_revision,
    );
    replay::append_state(&mut wal, &mut states, state).unwrap();
    let mut state = states[&key].clone();
    state.rebuild.as_mut().unwrap().warm = Some(object(7));
    replay::append_state(&mut wal, &mut states, state).unwrap();
    let mut next = states[&key].clone();
    next.rebuild.as_mut().unwrap().installation = Some(object(8));
    // The signed original request makes this event span multiple bounded WAL
    // chunks. Fail the commit reply, after every data frame has been synced.
    let mut persisted = next.clone();
    persisted.version += 1;
    let data_frames = serde_json::to_vec(&Event::State { state: persisted })
        .unwrap()
        .len()
        .div_ceil(8192);
    disk.fail_after(data_frames);
    assert!(replay::append_state(&mut wal, &mut states, next).is_err());
    assert!(
        states[&key]
            .rebuild
            .as_ref()
            .unwrap()
            .installation
            .is_none()
    );
    drop(wal);
    let (_, events) = protocol::EventJournal::recover::<Event>(disk, &config, 2, None).unwrap();
    let Event::State { mut state } = events.into_iter().last().unwrap() else {
        panic!("state")
    };
    assert_eq!(
        state.rebuild.as_ref().unwrap().installation,
        Some(object(8))
    );
    let saved = state.rebuild.clone();
    replay::close_after_restart(&mut state).unwrap();
    assert_eq!(state.rebuild, saved);
    assert_eq!(state.phase, AppliedPhase::Halted);
}

fn completed() -> (State, protocol::ControlRequest, SignedControlRequest) {
    let mut state =
        super::super::operation_halt_tests::state(&super::super::operation_halt_tests::plane(), 6);
    state.operation.schema = 4;
    let mut facts = facts();
    facts.fixed.plans.digest = state.operation.checkpoint_digest;
    facts.fixed.rollback_plans.digest = state.operation.rollback_digest;
    let ready = protocol::Readiness {
        checkpoint_digest: [21; 32],
        exec_report_sequence: 7,
        orders_digest: [22; 32],
        ledger_sequence: 7,
        input_seal_digest: [23; 32],
    };
    let initial = protocol::ControlRequest {
        schema: 3,
        coordinator_id: [30; 16],
        operation: state.operation.clone(),
        member: state.member.clone(),
        request_id: [31; 16],
        action: ControlAction::Prepare,
        proof: None,
        release: None,
        replay: None,
        rebuild: None,
        valid_until_ns: 1000,
    };
    let signed = SignedControlRequest {
        payload: serde_json::to_vec(&initial).unwrap(),
        signature: vec![9; 64],
    };
    facts.prepare = signed.clone();
    facts.warm = Some(object(7));
    facts.installation = Some(object(8));
    facts.rollback = Some(object(9));
    let body = protocol::RebuildPrepared {
        schema: 1,
        operation: state.operation.clone(),
        member: state.member.clone(),
        prepare: signed.clone(),
        challenge: signed.clone(),
        roots: facts.roots().unwrap(),
        end: facts.fixed.end.clone(),
        readiness: ready.clone(),
        state_version: 4,
        risk_gate_revision: state.gate.risk_gate_revision,
        issued_ns: 20,
        valid_until_ns: 1000,
    };
    let result = protocol::SignedRebuildPrepared {
        payload: serde_json::to_vec(&body).unwrap(),
        signature: vec![10; 64],
    };
    facts.result = Some(result.clone());
    state.proof = Some(ready.clone());
    state.rebuild = Some(facts);
    let mut commit = initial;
    commit.request_id = [32; 16];
    commit.action = ControlAction::Commit;
    commit.proof = Some(ready);
    commit.rebuild = Some(protocol::RebuildAdoption::from_receipt(&result).unwrap());
    (state, commit, signed)
}

#[test]
fn adoption_binds_all_seven_roots_fixed_frontier_and_actual_ready() {
    let (state, request, _) = completed();
    check_adoption(&state, &request).unwrap();
    for which in 0..9 {
        let mut changed = request.clone();
        let adopted = changed.rebuild.as_mut().unwrap();
        match which {
            0 => adopted.roots.plans = object(31),
            1 => adopted.roots.contract = object(31),
            2 => adopted.roots.inputs = object(31),
            3 => adopted.roots.rollback_plans = object(31),
            4 => adopted.roots.warm_completion = object(31),
            5 => adopted.roots.target_install = object(31),
            6 => adopted.roots.rollback_completion = object(31),
            7 => adopted.end.accounts[0].sequence += 1,
            _ => changed.proof.as_mut().unwrap().exec_report_sequence += 1,
        }
        assert!(check_adoption(&state, &changed).is_err());
    }
    let mut facts = state.rebuild.clone().unwrap();
    facts.fixed.capture_completion = object(33);
    assert!(facts.follows(state.rebuild.as_ref()).is_err());
}

#[test]
fn completed_retry_requires_same_session_and_exact_challenge() {
    let (mut state, _, signed) = completed();
    assert_eq!(
        saved_result(Some(&state), state.session, &signed).unwrap(),
        state.rebuild.as_ref().unwrap().result
    );
    assert!(
        saved_result(Some(&state), state.session + 1, &signed)
            .unwrap()
            .is_none()
    );
    let mut other = signed.clone();
    other.signature[0] ^= 1;
    assert!(
        saved_result(Some(&state), state.session, &other)
            .unwrap()
            .is_none()
    );
    state.phase = AppliedPhase::Halted;
    assert!(
        saved_result(Some(&state), state.session, &signed)
            .unwrap()
            .is_none()
    );
}

#[test]
fn fresh_query_wraps_only_current_prepared_session_history() {
    let (mut state, mut query, _) = completed();
    state.version = 4;
    query.action = ControlAction::QueryApplied;
    query.proof = None;
    query.rebuild = None;
    let signed = SignedControlRequest {
        payload: serde_json::to_vec(&query).unwrap(),
        signature: vec![1; 64],
    };
    let applied = protocol::AppliedReceipt {
        schema: 1,
        operation_id: query.operation.operation_id,
        operation_revision: query.operation.operation_revision,
        members_digest: query.operation.members_digest().unwrap(),
        logical_strategy_id: query.operation.logical_strategy_id,
        member: query.member.clone(),
        phase: AppliedPhase::Prepared,
        state_version: state.version,
        handover_sequence: 0,
        handover_digest: [0; 32],
        risk_gate_revision: state.gate.risk_gate_revision,
        control_request_id: query.request_id,
        issued_ns: 500,
        valid_until_ns: 1000,
    };
    let observed = query_observation(Some(&state), state.session, &signed, &applied)
        .unwrap()
        .unwrap();
    assert_eq!(
        Some(observed.prepared),
        state.rebuild.as_ref().unwrap().result
    );
    assert!(query_observation(Some(&state), state.session + 1, &signed, &applied).is_err());
    replay::close_after_restart(&mut state).unwrap();
    assert!(query_observation(Some(&state), state.session, &signed, &applied).is_err());
}

#[test]
fn prepared_requires_a_current_unfrozen_account_after_cold_work() {
    let (snapshot, catalog, _) = crate::replay::income_tests::common::fixture();
    let mut account = tickwire_account::Account::from_snapshot(snapshot, catalog).unwrap();
    let until = account.valid_until();
    assert!(until > 1);
    super::execution::account_ready(&account, false, until - 1).unwrap();
    assert!(
        super::execution::account_ready(&account, false, until).is_err(),
        "a fresh sample at expiry must reject before Prepared"
    );
    assert!(super::execution::account_ready(&account, false, until + 1).is_err());
    assert!(
        super::execution::account_ready(&account, true, until - 1).is_err(),
        "sender halt is not readiness"
    );
    account.freeze();
    assert!(
        super::execution::account_ready(&account, false, until - 1).is_err(),
        "a future deadline cannot unfreeze the actual account"
    );
}
