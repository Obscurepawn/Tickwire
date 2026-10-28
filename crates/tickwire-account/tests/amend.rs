mod common;
use common::*;
use tickwire_account::*;
use tickwire_venue as v;

fn known_order() -> (Account, v::Command, Admission, v::OrderObservation) {
    let (mut a, c) = setup();
    let original = a.admit(&canonical(c.clone()), 60).unwrap();
    let e = venue::event();
    let v::VenueEventBody::Fill(f) = e.body else {
        unreachable!()
    };
    let mut observation = f.observation;
    observation.status = v::OrderStatus::New;
    observation.cumulative_quantity = Some(venue::d(0, 0));
    let mut ack = venue::event();
    ack.body = v::VenueEventBody::Order(Box::new(observation.clone()));
    a.apply_event(&event(ack)).unwrap();
    (a, c, original, observation)
}
fn amend(c: &v::Command, o: &v::OrderObservation, id: u64) -> v::CanonicalCommand {
    let mut c = c.clone();
    c.context.request.intent_id = id;
    c.context.request.recovery_object += id;
    c.context.request.deadline = 2000;
    c.body = v::CommandBody::AmendKeepPriority {
        target: v::OrderTarget {
            order_id: o.order.order_id,
            expected_owner_revision: 1,
            original_intent_exec_epoch: o.order.original_intent_exec_epoch,
            client_id: o.order.client_id.clone(),
            venue_order_id: o.order.venue_order_id.clone(),
        },
        new_quantity: venue::d(8, -4),
        new_client_id: o.order.client_id.clone(),
    };
    canonical(c)
}

#[test]
fn frozen_known_order_accepts_same_client_reduce_without_releasing_reservation() {
    let (mut account, c, original, o) = known_order();
    let before = account.asset(key(2)).unwrap();
    account.freeze();
    let request = amend(&c, &o, 4);
    let a = account
        .admit(&request, 1001)
        .expect("nonincreasing amendment survives stale qualification");
    assert!(!a.image().increases);
    assert!(a.orders().is_empty());
    account.check_admission(&a, 1002).unwrap();
    assert_eq!(account.asset(key(2)).unwrap(), before);
    assert_eq!(
        account
            .order(&original.orders()[0])
            .unwrap()
            .remaining_quantity,
        f(100_000)
    );
    assert_eq!(account.admit(&request, 1003).unwrap(), a);
}

#[test]
fn one_pending_amendment_and_durable_abort_never_release_original_order() {
    let (mut account, c, original, o) = known_order();
    let a = account
        .admit(&amend(&c, &o, 4), 61)
        .expect("first amendment is supported");
    assert!(account.admit(&amend(&c, &o, 5), 62).is_err());
    let before = account.asset(key(2)).unwrap();
    assert_eq!(account.apply_abort(&a), Ok(Applied::New));
    assert_eq!(account.apply_abort(&a), Ok(Applied::Duplicate));
    assert_eq!(account.asset(key(2)).unwrap(), before);
    assert!(!account.order(&original.orders()[0]).unwrap().aborted);
    account.admit(&amend(&c, &o, 5), 63).unwrap();
}

fn result(
    request: &v::CanonicalCommand,
    o: &v::OrderObservation,
    execution: u64,
    cumulative: Option<&str>,
) -> v::CanonicalEvent {
    let v::CommandBody::AmendKeepPriority { new_quantity, .. } = request.command().body else {
        unreachable!()
    };
    let mut obs = o.clone();
    obs.original_quantity = Some(new_quantity);
    obs.cumulative_quantity = cumulative.map(|s| s.parse().unwrap());
    let mut e = venue::event();
    e.body = v::VenueEventBody::Amendment(Box::new(v::AmendmentObservation {
        order: o.order.clone(),
        request_intent_id: request.command().context.request.intent_id,
        request_intent_exec_epoch: request.command().context.request.intent_exec_epoch,
        request_digest: request.digest(),
        execution_source: v::AmendmentSource::RestResponse,
        execution_id: execution,
        previous_quantity: venue::d(1, -3),
        new_quantity,
        observation: cumulative.map(|_| obs),
    }));
    event(e)
}

#[test]
fn actual_amendment_reduces_only_unfilled_reserve_and_preserves_original_quantity() {
    let (mut account, c, original, o) = known_order();
    let request = amend(&c, &o, 4);
    account.admit(&request, 61).unwrap();
    account
        .apply_event(&event(fill(&c, "fill-before", "0.0002", "60000", "12")))
        .unwrap();
    let before = account.asset(key(2)).unwrap();
    let observed = result(&request, &o, 75, Some("0.0002"));
    assert_eq!(account.apply_event(&observed), Ok(Applied::New));
    let state = account.order(&original.orders()[0]).unwrap();
    assert_eq!(state.original_quantity, f(100_000));
    assert_eq!(state.effective_quantity, f(80_000));
    assert_eq!(state.filled_quantity, f(20_000));
    assert_eq!(state.remaining_quantity, f(60_000));
    let after = account.asset(key(2)).unwrap();
    assert_eq!(after.total, before.total);
    assert_eq!(after.ownership, before.ownership);
    assert_eq!(after.reserved, units(36));
    let seq = account.ledger_sequence();
    assert_eq!(account.apply_event(&observed), Ok(Applied::Duplicate));
    assert_eq!(account.ledger_sequence(), seq);
}

#[test]
fn larger_cumulative_on_duplicate_keeps_unexplained_responsibility_without_synthetic_fill() {
    let (mut account, c, original, o) = known_order();
    let request = amend(&c, &o, 4);
    account.admit(&request, 61).unwrap();
    account
        .apply_event(&result(&request, &o, 75, Some("0")))
        .unwrap();
    assert_eq!(
        account.apply_event(&result(&request, &o, 75, Some("0.0009"))),
        Ok(Applied::New)
    );
    assert!(account.frozen());
    let state = account.order(&original.orders()[0]).unwrap();
    assert_eq!(state.filled_quantity, f(0));
    assert_eq!(state.remaining_quantity, f(90_000));
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(54));
    assert_eq!(account.asset(key(2)).unwrap().total, units(1000));
}

#[test]
fn old_quantity_observation_cannot_undo_amend_but_new_trade_still_books_and_freezes() {
    let (mut account, c, original, o) = known_order();
    let request = amend(&c, &o, 4);
    account.admit(&request, 61).unwrap();
    account
        .apply_event(&result(&request, &o, 75, Some("0")))
        .unwrap();
    let mut old = venue::event();
    old.body = v::VenueEventBody::Order(Box::new(o));
    account.apply_event(&event(old)).unwrap();
    assert_eq!(
        account
            .order(&original.orders()[0])
            .unwrap()
            .effective_quantity,
        f(80_000)
    );
    account
        .apply_event(&event(fill(&c, "late-trade", "0.0009", "60000", "54")))
        .unwrap();
    assert!(account.frozen());
    assert_eq!(
        account
            .order(&original.orders()[0])
            .unwrap()
            .filled_quantity,
        f(90_000)
    );
    assert_eq!(account.asset(key(2)).unwrap().total, units(946));
}

#[test]
fn completed_a_replay_cannot_be_bound_to_pending_b_and_execution_conflicts_reject() {
    let (mut account, c, original, o) = known_order();
    let a = amend(&c, &o, 4);
    account.admit(&a, 61).unwrap();
    let event_a = result(&a, &o, 75, Some("0"));
    account.apply_event(&event_a).unwrap();
    let mut b = amend(&c, &o, 5).command().clone();
    let v::CommandBody::AmendKeepPriority { new_quantity, .. } = &mut b.body else {
        unreachable!()
    };
    *new_quantity = venue::d(6, -4);
    let b = canonical(b);
    let admitted_b = account.admit(&b, 62).unwrap();
    assert_eq!(account.apply_event(&event_a), Ok(Applied::Duplicate));
    account.check_admission(&admitted_b, 63).unwrap();
    let mut b_event = result(&b, &o, 75, Some("0")).event().clone();
    let v::VenueEventBody::Amendment(ref mut a) = b_event.body else {
        unreachable!()
    };
    a.previous_quantity = venue::d(8, -4);
    assert_eq!(account.apply_event(&event(b_event)), Err(Error::Conflict));
    assert_eq!(
        account
            .order(&original.orders()[0])
            .unwrap()
            .effective_quantity,
        f(80_000)
    );
}

#[test]
fn cross_source_ids_remain_distinct_without_applying_the_same_decrease_twice() {
    let (mut account, c, original, o) = known_order();
    let request = amend(&c, &o, 4);
    account.admit(&request, 61).unwrap();
    let first = result(&request, &o, 75, Some("0"));
    account.apply_event(&first).unwrap();
    let mut from_private = first.event().clone();
    let v::VenueEventBody::Amendment(ref mut a) = from_private.body else {
        unreachable!()
    };
    a.execution_source = v::AmendmentSource::PrivateStream;
    a.execution_id = 99;
    let private = event(from_private);
    let before = account.asset(key(2)).unwrap();
    assert_eq!(account.apply_event(&private), Ok(Applied::New));
    assert_eq!(account.apply_event(&private), Ok(Applied::Duplicate));
    assert_eq!(account.asset(key(2)).unwrap(), before);
    assert_eq!(account.amendments().next().unwrap().executions.len(), 2);
    assert_eq!(
        account
            .order(&original.orders()[0])
            .unwrap()
            .effective_quantity,
        f(80_000)
    );
}

#[test]
fn native_client_manager_and_quantity_checks_are_not_bypassed_by_frozen_account() {
    let (account, c, _, o) = known_order();
    for change in 0..6 {
        let mut a = account.clone();
        a.freeze();
        let mut command = amend(&c, &o, 4).command().clone();
        let v::CommandBody::AmendKeepPriority {
            target,
            new_quantity,
            new_client_id,
        } = &mut command.body
        else {
            unreachable!()
        };
        match change {
            0 => target.venue_order_id = None,
            1 => *new_client_id = None,
            2 => *new_client_id = Some(venue::text("random-replacement")),
            3 => target.expected_owner_revision += 1,
            4 => target.order_id = [99; 16],
            _ => *new_quantity = venue::d(1, -3),
        }
        let before = a.asset(key(2)).unwrap();
        assert!(a.admit(&canonical(command), 1001).is_err(), "case {change}");
        assert_eq!(a.asset(key(2)).unwrap(), before);
    }
}

#[test]
fn amended_state_and_pending_request_replay_exactly_with_recovery_unknown_excluded_from_digest() {
    let (snapshot, catalog, command) = fixture();
    let mut a = Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
    let create = canonical(command.clone());
    let admitted = a.admit(&create, 60).unwrap();
    let (_, _, _, observation) = known_order();
    let mut ack = venue::event();
    ack.body = v::VenueEventBody::Order(Box::new(observation.clone()));
    let ack = event(ack);
    a.apply_event(&ack).unwrap();
    let request = amend(&command, &observation, 4);
    let image = a.admit(&request, 61).unwrap().image();
    let observed = result(&request, &observation, 75, Some("0"));
    a.apply_event(&observed).unwrap();
    let records = vec![
        RecoveryRecord::Admit {
            command: Box::new(create),
            evaluated_at: 60,
            expected: admitted.image(),
        },
        RecoveryRecord::VenueEvent(Box::new(ack)),
        RecoveryRecord::Admit {
            command: Box::new(request),
            evaluated_at: 61,
            expected: image,
        },
        RecoveryRecord::VenueEvent(Box::new(observed)),
    ];
    let replay = Account::recover(snapshot, catalog, &records).unwrap();
    assert!(replay.frozen());
    assert_eq!(a.ledger_sequence(), replay.ledger_sequence());
    assert_eq!(a.asset(key(2)), replay.asset(key(2)));
    assert_eq!(
        a.amendments().collect::<Vec<_>>(),
        replay.amendments().collect::<Vec<_>>()
    );
    assert_eq!(
        a.account_inputs_state_digest([5; 32], [6; 32]).unwrap(),
        replay
            .account_inputs_state_digest([5; 32], [6; 32])
            .unwrap()
    );
    assert_eq!(
        a.strategy_orders_digest(8),
        replay.strategy_orders_digest(8)
    );
}

#[test]
fn partially_filled_while_amend_waits_blocks_first_send_when_target_has_been_reached() {
    let (mut account, c, _, o) = known_order();
    let admitted = account.admit(&amend(&c, &o, 4), 61).unwrap();
    account
        .apply_event(&event(fill(&c, "raced-fill", "0.0008", "60000", "48")))
        .unwrap();
    assert_eq!(
        account.check_admission(&admitted, 62),
        Err(Error::Unreconciled)
    );
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(12));
    account.apply_abort(&admitted).unwrap();
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(12));
}

#[test]
fn iceberg_and_other_order_types_are_not_classified_as_supported_decreases() {
    for iceberg in [true, false] {
        let (snapshot, catalog, mut command) = fixture();
        let v::CommandBody::New(ref mut spec) = command.body else {
            unreachable!()
        };
        if iceberg {
            spec.iceberg_quantity = Some(venue::d(1, -4));
        } else {
            spec.time_in_force = Some(v::TimeInForce::Ioc);
        }
        let mut account = Account::from_snapshot(snapshot, catalog).unwrap();
        account.admit(&canonical(command.clone()), 60).unwrap();
        let (_, _, _, o) = known_order();
        let mut ack = venue::event();
        ack.body = v::VenueEventBody::Order(Box::new(o.clone()));
        account.apply_event(&event(ack)).unwrap();
        assert_eq!(
            account.admit(&amend(&command, &o, 4), 61),
            Err(Error::Unsupported)
        );
    }
}

#[test]
fn new_unreconciled_observation_while_b_waits_rechecks_the_original_admission_condition() {
    let (mut account, c, _, o) = known_order();
    let a = amend(&c, &o, 4);
    account.admit(&a, 61).unwrap();
    account.apply_event(&result(&a, &o, 75, Some("0"))).unwrap();
    let mut b = amend(&c, &o, 5).command().clone();
    let v::CommandBody::AmendKeepPriority { new_quantity, .. } = &mut b.body else {
        unreachable!()
    };
    *new_quantity = venue::d(6, -4);
    let b = account.admit(&canonical(b), 62).unwrap();
    account
        .apply_event(&result(&a, &o, 75, Some("0.0004")))
        .unwrap();
    let before = account.asset(key(2)).unwrap();
    assert_eq!(account.check_admission(&b, 63), Err(Error::Unreconciled));
    account.apply_abort(&b).unwrap();
    assert_eq!(account.asset(key(2)).unwrap(), before);
}
