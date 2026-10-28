#![cfg(target_os = "linux")]
mod common;
use common::*;
use tickwire_control::MockRequestStore;
use tickwire_join::*;

#[test]
fn successful_builder_binds_real_pending_gate_and_exact_retry_is_replayed() {
    let (mut owner, data) = owner();
    assert!(owner.all_detached());
    let reg = registration(&owner, 4);
    let (mut supervisor, client) = session_pair(identity(1, 1));
    let raw = request(&client, &join_request(&owner, &reg), 2, 1);
    let mut store = MockRequestStore::new(16).unwrap();
    let result = owner
        .prepare(
            &mut supervisor,
            &mut store,
            &raw,
            &reg,
            [31; 16],
            NOW,
            |p| data.build(p),
        )
        .unwrap();
    assert!(matches!(result, PrepareOutcome::Prepared { .. }));
    assert_eq!(owner.membership_version(), 2);
    assert_eq!(owner.stage(reg.subscription), Some(Stage::Prepared));
    assert!(!owner.all_detached());
    let join = owner.join(reg.subscription).unwrap();
    assert_eq!(
        (join.snapshot_seq, join.join_seq, join.membership_version),
        (4, 5, 2)
    );
    assert_eq!(
        owner.snapshot_header(reg.subscription).unwrap().first_seq,
        1
    );
    let repeat = owner
        .prepare(
            &mut supervisor,
            &mut store,
            &raw,
            &reg,
            [31; 16],
            NOW,
            |_| panic!("retry rebuilt"),
        )
        .unwrap();
    assert!(matches!(repeat, PrepareOutcome::Completed(_)));
    assert_eq!(owner.membership_version(), 2);
    fill_pending(&mut owner, &data);
    assert_full(&mut owner, &data);
}

#[test]
fn builder_failure_retains_pending_gate_and_pending_request_does_not_reexecute() {
    let (mut owner, data) = owner();
    let reg = registration(&owner, 4);
    let (mut supervisor, client) = session_pair(identity(1, 1));
    let raw = request(&client, &join_request(&owner, &reg), 2, 1);
    let mut store = MockRequestStore::new(16).unwrap();
    assert!(
        owner
            .prepare(
                &mut supervisor,
                &mut store,
                &raw,
                &reg,
                [31; 16],
                NOW,
                |_| Err(tickwire_ipc::Error::Bounds)
            )
            .is_err()
    );
    assert_eq!(owner.stage(reg.subscription), Some(Stage::Failed));
    assert!(!owner.all_detached());
    assert_eq!(owner.membership_version(), 2);
    let retry = owner
        .prepare(
            &mut supervisor,
            &mut store,
            &raw,
            &reg,
            [31; 16],
            NOW,
            |_| panic!("failed request reexecuted"),
        )
        .unwrap();
    assert!(matches!(retry, PrepareOutcome::Pending));
    fill_pending(&mut owner, &data);
    owner.expire(DEADLINE);
    assert_eq!(owner.stage(reg.subscription), Some(Stage::Expired));
    assert!(!owner.all_detached());
    assert_full(&mut owner, &data);
}

#[test]
fn invalid_request_does_not_admit_a_member() {
    for case in 0..6 {
        let (mut owner, data) = owner();
        let reg = registration(&owner, 4);
        let (mut supervisor, client) = session_pair(identity(1, 1));
        let mut body = join_request(&owner, &reg);
        match case {
            0 => body.snapshot_seq = 4,
            1 => body.topic_instrument_set_hash[0] ^= 1,
            2 => body.subscription_generation += 1,
            3 => body.bank_resource_id = [99; 16],
            4 => body.readiness_deadline_mono_ns = NOW,
            _ => body.role = 4,
        }
        let raw = request(&client, &body, 2, 1);
        let mut store = MockRequestStore::new(16).unwrap();
        assert!(
            owner
                .prepare(
                    &mut supervisor,
                    &mut store,
                    &raw,
                    &reg,
                    [31; 16],
                    NOW,
                    |_| panic!("invalid request reached builder")
                )
                .is_err()
        );
        assert_eq!(owner.membership_version(), 1, "case {case}");
        fill_pending(&mut owner, &data);
        owner.publish(&[&data.record(69)]).unwrap();
    }
}

#[test]
fn active_subscriber_session_cannot_impersonate_supervisor_join_authority() {
    let (mut owner, _data) = owner();
    let reg = registration(&owner, 4);
    let (mut wrong, client) = session_pair(reg.identity.clone());
    let raw = request(&client, &join_request(&owner, &reg), 2, 1);
    let mut store = MockRequestStore::new(16).unwrap();
    assert!(
        owner
            .prepare(
                &mut wrong,
                &mut store,
                &raw,
                &reg,
                [31; 16],
                NOW,
                |_| panic!("unauthorized request reached builder")
            )
            .is_err()
    );
    assert_eq!(owner.membership_version(), 1);
}
