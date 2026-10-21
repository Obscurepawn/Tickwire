mod common;
use common::*;
use tickwire_account::*;
use tickwire_venue as v;

fn list_command() -> v::Command {
    let (_, _, mut command) = fixture();
    let v::CommandBody::New(first) = command.body.clone() else {
        unreachable!()
    };
    let mut second = first.clone();
    second.client_id = v::ClientId::new("pending-leg").unwrap();
    command.body = v::CommandBody::SpotList(v::ListOrder {
        list_id: [77; 16],
        client_id: v::ClientId::new("known-list").unwrap(),
        kind: v::ListKind::Oto,
        legs: v::BoundedList::new(vec![first, second]).unwrap(),
    });
    command
}
fn cancel_list(original: &v::Command) -> v::Command {
    let mut command = original.clone();
    let v::CommandBody::SpotList(list) = &original.body else {
        unreachable!()
    };
    command.context.request.intent_id += 1;
    command.context.request.deadline = 2000;
    command.body = v::CommandBody::CancelList(v::ListTarget {
        list_id: list.list_id,
        expected_owner_revision: 1,
        original_intent_exec_epoch: original.context.request.intent_exec_epoch,
        client_id: Some(list.client_id.clone()),
        venue_list_id: None,
    });
    command
}
#[test]
fn owned_list_cancel_is_admitted_without_releasing_any_potential_leg() {
    let (s, c, _) = fixture();
    let mut account = Account::from_snapshot(s, c).unwrap();
    let command = list_command();
    let admitted = account.admit(&canonical(command.clone()), 60).unwrap();
    assert_eq!(admitted.orders().len(), 2);
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(120));
    account.freeze();
    let cancel = canonical(cancel_list(&command));
    let management = account
        .admit(&cancel, 1001)
        .expect("owned cancel survives stale account qualification");
    assert!(!management.image().increases);
    account.check_admission(&management, 1002).unwrap();
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(120));
    assert_eq!(account.admit(&cancel, 1003).unwrap(), management);
}
#[test]
fn owned_algo_cancel_keeps_the_preallocated_potential_child() {
    let (s, c, mut command) = futures_fixture();
    let v::CommandBody::New(mut order) = command.body.clone() else {
        unreachable!()
    };
    order.order_type = v::OrderType::StopLimit;
    order.trigger = Some(v::Trigger {
        price: venue::d(110, 0),
        working_price: v::WorkingPrice::Contract,
        price_protect: false,
    });
    let client = order.client_id.clone();
    command.body = v::CommandBody::Algo(v::AlgoOrder {
        parent_order_id: [78; 16],
        order,
    });
    let mut account = Account::from_snapshot(s, c).unwrap();
    let a = account.admit(&canonical(command.clone()), 60).unwrap();
    let before = account.venue_shadow(1).unwrap();
    let mut cancel = command.clone();
    cancel.context.request.intent_id += 1;
    cancel.body = v::CommandBody::CancelAlgo(v::AlgoTarget {
        parent_order_id: [78; 16],
        expected_owner_revision: 1,
        original_intent_exec_epoch: command.context.request.intent_exec_epoch,
        client_id: Some(client),
        venue_algo_id: None,
    });
    let management = account
        .admit(&canonical(cancel), 61)
        .expect("exact original algo is owned");
    assert!(!management.image().increases);
    assert_eq!(account.venue_shadow(1).unwrap(), before);
    assert_eq!(
        account.order(&a.orders()[0]).unwrap().remaining_quantity,
        units(1)
    );
}

#[test]
fn parent_cancel_checks_full_identity_and_current_manager_after_handover() {
    let (snapshot, catalog, _) = fixture();
    let mut account = Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
    let original = list_command();
    let creating = canonical(original.clone());
    let admission = account.admit(&creating, 60).unwrap();
    let cancel = canonical(cancel_list(&original));
    let management = account.admit(&cancel, 61).unwrap();
    let before = account.asset(key(2)).unwrap();
    let handover = StrategyHandover {
        scope: snapshot.scope.clone(),
        account_epoch: snapshot.account_epoch,
        writer_epoch: original.context.request.intent_exec_epoch,
        operation_id: [70; 16],
        operation_revision: 1,
        logical_strategy_id: original.context.strategy.logical_strategy_id,
        expected_ledger_sequence: account.ledger_sequence(),
        expected_orders_digest: account
            .strategy_orders_digest(original.context.strategy.logical_strategy_id),
        from: ManagerIdentity {
            instance: original.context.strategy.instance_id,
            generation: original.context.request.strategy_generation,
        },
        to: ManagerIdentity {
            instance: [71; 16],
            generation: original.context.request.strategy_generation + 1,
        },
        checkpoint_digest: [72; 32],
        input_seal_digest: [73; 32],
        evaluated_at: 62,
        valid_until: 100,
    };
    account.apply_strategy_handover(&handover).unwrap();
    assert_eq!(
        account.check_admission(&management, 63),
        Err(Error::Identity)
    );
    assert_eq!(account.asset(key(2)).unwrap(), before);
    for mismatch in 0..5 {
        let mut changed = cancel_list(&original);
        changed.context.request.intent_id += 10 + mismatch;
        changed.context.strategy.instance_id = handover.to.instance;
        changed.context.request.strategy_generation = handover.to.generation;
        let v::CommandBody::CancelList(ref mut target) = changed.body else {
            unreachable!()
        };
        target.expected_owner_revision = 2;
        match mismatch {
            0 => target.list_id = [80; 16],
            1 => target.client_id = Some(v::ClientId::new("different-parent").unwrap()),
            2 => target.venue_list_id = Some(v::VenueOrderId::new("1234").unwrap()),
            3 => target.expected_owner_revision = 1,
            _ => changed.context.strategy.instance_id = [82; 16],
        }
        let sequence = account.ledger_sequence();
        assert!(account.admit(&canonical(changed), 64).is_err());
        assert_eq!(account.ledger_sequence(), sequence);
        assert_eq!(account.asset(key(2)).unwrap(), before);
    }
    let restored = Account::recover(
        snapshot,
        catalog,
        &[
            RecoveryRecord::Admit {
                command: Box::new(creating),
                evaluated_at: 60,
                expected: admission.image(),
            },
            RecoveryRecord::Admit {
                command: Box::new(cancel),
                evaluated_at: 61,
                expected: management.image(),
            },
            RecoveryRecord::StrategyHandover(Box::new(handover)),
        ],
    )
    .unwrap();
    assert_eq!(restored.asset(key(2)).unwrap(), before);
    assert_eq!(
        restored.check_admission(&management, 63),
        Err(Error::Identity)
    );
    assert!(restored.frozen());
}

fn parent_event(command: &v::Command, status: v::StructureStatus) -> v::VenueEvent {
    let v::CommandBody::SpotList(list) = &command.body else {
        unreachable!()
    };
    let mut e = venue::event();
    e.identity.product = command.context.request.product;
    e.body = v::VenueEventBody::Structure(Box::new(v::StructureObservation {
        parent: v::StructureIdentity {
            structure_id: list.list_id,
            original_intent_id: command.context.request.intent_id,
            original_intent_exec_epoch: command.context.request.intent_exec_epoch,
            kind: v::StructureKind::Oto,
            client_id: list.client_id.clone(),
            venue_parent_id: Some(v::VenueOrderId::new("500").unwrap()),
        },
        status,
        venue_status: v::BoundedText::new("ALL_DONE").unwrap(),
        children: v::BoundedList::new(vec![]).unwrap(),
    }));
    e
}
#[test]
fn terminal_parent_keeps_every_leg_and_late_child_only_binds_existing_reservation() {
    let (s, c, _) = fixture();
    let mut account = Account::from_snapshot(s.clone(), c.clone()).unwrap();
    let command = list_command();
    let creating = canonical(command.clone());
    let admission = account.admit(&creating, 60).unwrap();
    let before = account.asset(key(2)).unwrap();
    let terminal = event(parent_event(&command, v::StructureStatus::Terminal));
    assert_eq!(account.apply_event(&terminal), Ok(Applied::New));
    assert_eq!(account.asset(key(2)).unwrap(), before);
    for key in admission.orders() {
        let o = account.order(key).unwrap();
        assert!(!o.terminal);
        assert!(o.confirmed);
        assert_eq!(o.remaining_quantity, o.original_quantity);
    }
    let seq = account.ledger_sequence();
    assert_eq!(account.apply_event(&terminal), Ok(Applied::Duplicate));
    assert_eq!(account.ledger_sequence(), seq);
    let mut late = parent_event(&command, v::StructureStatus::Triggered);
    let v::VenueEventBody::Structure(ref mut parent) = late.body else {
        unreachable!()
    };
    let mut raw = venue::event();
    let v::VenueEventBody::Fill(fill) = &mut raw.body else {
        unreachable!()
    };
    let mut id = fill.observation.order.clone();
    id.order_id = [88; 16];
    id.original_intent_id = command.context.request.intent_id;
    id.original_intent_exec_epoch = command.context.request.intent_exec_epoch;
    id.client_id = Some(admission.orders()[1].client_id.clone());
    id.venue_order_id = Some(v::VenueOrderId::new("700").unwrap());
    id.list_id = Some(parent.parent.structure_id);
    id.venue_list_id = parent.parent.venue_parent_id.clone();
    id.list_client_id = Some(parent.parent.client_id.clone());
    id.parent_order_id = None;
    id.venue_algo_id = None;
    id.algo_client_id = None;
    parent.children = v::BoundedList::new(vec![v::StructureChild {
        order: id,
        reported_client_id: None,
    }])
    .unwrap();
    let late = event(late);
    assert_eq!(account.apply_event(&late), Ok(Applied::New));
    assert_eq!(account.asset(key(2)).unwrap(), before);
    assert!(!account.frozen());
    assert_eq!(account.apply_event(&late), Ok(Applied::Duplicate));
    let restored = Account::recover(
        s,
        c,
        &[
            RecoveryRecord::Admit {
                command: Box::new(creating),
                evaluated_at: 60,
                expected: admission.image(),
            },
            RecoveryRecord::VenueEvent(Box::new(terminal)),
            RecoveryRecord::VenueEvent(Box::new(late)),
        ],
    )
    .unwrap();
    assert_eq!(restored.asset(key(2)).unwrap(), before);
    assert_eq!(restored.ledger_sequence(), 3);
    assert!(restored.frozen());
}

#[test]
fn parent_unknown_is_durable_while_recovery_freeze_is_not_in_state_digest() {
    let (snapshot, catalog, _) = fixture();
    let mut account = Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
    let command = list_command();
    let creating = canonical(command.clone());
    let admission = account.admit(&creating, 60).unwrap();
    assert_eq!(account.conditional_state_version(), 1);
    let h0 = account.strategy_orders_digest(command.context.strategy.logical_strategy_id);
    let unknown = event(parent_event(&command, v::StructureStatus::Unknown));
    assert_eq!(account.apply_event(&unknown), Ok(Applied::New));
    assert!(account.frozen());
    assert_eq!(account.conditional_state_version(), 2);
    let parent = account.structure(&[77; 16]).unwrap();
    assert!(parent.unknown);
    assert!(
        admission
            .orders()
            .iter()
            .all(|key| account.order(key).unwrap().unknown)
    );
    let seq = account.ledger_sequence();
    assert_eq!(account.apply_event(&unknown), Ok(Applied::Duplicate));
    assert_eq!(account.ledger_sequence(), seq);
    let h1 = account.strategy_orders_digest(command.context.strategy.logical_strategy_id);
    assert_ne!(h1, h0);
    let state = account
        .account_inputs_state_digest([11; 32], [12; 32])
        .unwrap();
    let restored = Account::recover(
        snapshot,
        catalog,
        &[
            RecoveryRecord::Admit {
                command: Box::new(creating),
                evaluated_at: 60,
                expected: admission.image(),
            },
            RecoveryRecord::VenueEvent(Box::new(unknown)),
        ],
    )
    .unwrap();
    assert_eq!(
        restored
            .account_inputs_state_digest([11; 32], [12; 32])
            .unwrap(),
        state
    );
    assert_eq!(
        restored.strategy_orders_digest(command.context.strategy.logical_strategy_id),
        h1
    );
    assert_eq!(restored.structure(&[77; 16]).unwrap(), parent);
    assert_eq!(account.apply_abort(&admission), Err(Error::Unreconciled));
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(120));
}
#[test]
fn conflicting_native_parent_keeps_original_binding_and_reservations() {
    let (s, c, _) = fixture();
    let mut account = Account::from_snapshot(s, c).unwrap();
    let command = list_command();
    account.admit(&canonical(command.clone()), 60).unwrap();
    account
        .apply_event(&event(parent_event(&command, v::StructureStatus::Live)))
        .unwrap();
    let before = account.asset(key(2)).unwrap();
    let seq = account.ledger_sequence();
    let mut changed = parent_event(&command, v::StructureStatus::Terminal);
    let v::VenueEventBody::Structure(ref mut p) = changed.body else {
        unreachable!()
    };
    p.parent.venue_parent_id = Some(v::VenueOrderId::new("501").unwrap());
    let changed = event(changed);
    assert!(account.apply_event(&changed).is_err());
    assert!(account.frozen());
    assert_eq!(account.ledger_sequence(), seq);
    assert_eq!(account.asset(key(2)).unwrap(), before);
    assert_eq!(
        account.structure(&[77; 16]).unwrap().venue_parent_id,
        Some(v::VenueOrderId::new("500").unwrap())
    );
    assert_eq!(account.rejected_event(), Some(&changed));
}

#[test]
fn parent_ack_blocks_abort_and_late_ack_reopens_all_aborted_legs() {
    let (s, c, _) = fixture();
    let command = list_command();
    let creating = canonical(command.clone());
    let ack = event(parent_event(&command, v::StructureStatus::Live));
    let mut account = Account::from_snapshot(s.clone(), c.clone()).unwrap();
    let admission = account.admit(&creating, 60).unwrap();
    account.apply_event(&ack).unwrap();
    assert_eq!(account.apply_abort(&admission), Err(Error::Unreconciled));
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(120));

    let mut account = Account::from_snapshot(s.clone(), c.clone()).unwrap();
    let admission = account.admit(&creating, 60).unwrap();
    account.apply_abort(&admission).unwrap();
    assert_eq!(account.asset(key(2)).unwrap().reserved, f(0));
    assert_eq!(account.apply_event(&ack), Ok(Applied::New));
    assert!(account.frozen());
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(120));
    for key in admission.orders() {
        let leg = account.order(key).unwrap();
        assert!(!leg.aborted && leg.confirmed && !leg.fee_reconciled);
        assert_eq!(leg.remaining_quantity, leg.original_quantity);
    }
    let sequence = account.ledger_sequence();
    assert_eq!(account.apply_event(&ack), Ok(Applied::Duplicate));
    assert_eq!(account.ledger_sequence(), sequence);
    let recovered = Account::recover(
        s,
        c,
        &[
            RecoveryRecord::Admit {
                command: Box::new(creating.clone()),
                evaluated_at: 60,
                expected: admission.image(),
            },
            RecoveryRecord::Aborted {
                intent: admission.intent(),
                command_digest: creating.digest(),
            },
            RecoveryRecord::VenueEvent(Box::new(ack)),
        ],
    )
    .unwrap();
    assert_eq!(recovered.ledger_sequence(), sequence);
    assert_eq!(
        recovered.asset(key(2)).unwrap(),
        account.asset(key(2)).unwrap()
    );
    assert_eq!(
        recovered
            .account_inputs_state_digest([11; 32], [12; 32])
            .unwrap(),
        account
            .account_inputs_state_digest([11; 32], [12; 32])
            .unwrap()
    );
}

#[test]
fn late_algo_ack_restores_the_original_exclusive_reduce_ticket() {
    let (mut s, c, mut command) = futures_fixture();
    s.positions.push(PositionSnapshot {
        instrument_id: 12,
        side: v::PositionSide::Both,
        quantity: units(1),
        entry_bound: units(100),
        ownership: vec![(8, units(1))],
    });
    let v::CommandBody::New(mut order) = command.body.clone() else {
        unreachable!()
    };
    order.side = v::Side::Sell;
    order.reduce_only = true;
    order.order_type = v::OrderType::StopLimit;
    order.trigger = Some(v::Trigger {
        price: venue::d(110, 0),
        working_price: v::WorkingPrice::Contract,
        price_protect: false,
    });
    let client = order.client_id.clone();
    command.body = v::CommandBody::Algo(v::AlgoOrder {
        parent_order_id: [78; 16],
        order,
    });
    let creating = canonical(command.clone());
    let mut account = Account::from_snapshot(s.clone(), c.clone()).unwrap();
    let admission = account.admit(&creating, 60).unwrap();
    assert_eq!(
        account
            .order(&admission.orders()[0])
            .unwrap()
            .reduce_allocation,
        units(1)
    );
    account.apply_abort(&admission).unwrap();
    assert_eq!(
        account
            .order(&admission.orders()[0])
            .unwrap()
            .reduce_allocation,
        units(0)
    );
    let mut ack = venue::event();
    ack.identity.product = v::Product::LinearPerpetual;
    ack.body = v::VenueEventBody::Structure(Box::new(v::StructureObservation {
        parent: v::StructureIdentity {
            structure_id: [78; 16],
            original_intent_id: command.context.request.intent_id,
            original_intent_exec_epoch: command.context.request.intent_exec_epoch,
            kind: v::StructureKind::Algo,
            client_id: client,
            venue_parent_id: Some(v::VenueOrderId::new("800").unwrap()),
        },
        status: v::StructureStatus::Live,
        venue_status: v::BoundedText::new("NEW").unwrap(),
        children: v::BoundedList::new(vec![]).unwrap(),
    }));
    let ack = event(ack);
    account.apply_event(&ack).unwrap();
    assert!(account.frozen());
    assert_eq!(
        account
            .order(&admission.orders()[0])
            .unwrap()
            .reduce_allocation,
        units(1),
        "the original live reduce child still owns its actual-position ticket"
    );
    let recovered = Account::recover(
        s,
        c,
        &[
            RecoveryRecord::Admit {
                command: Box::new(creating.clone()),
                evaluated_at: 60,
                expected: admission.image(),
            },
            RecoveryRecord::Aborted {
                intent: admission.intent(),
                command_digest: creating.digest(),
            },
            RecoveryRecord::VenueEvent(Box::new(ack)),
        ],
    )
    .unwrap();
    assert_eq!(
        recovered
            .order(&admission.orders()[0])
            .unwrap()
            .reduce_allocation,
        units(1)
    );
}

#[test]
fn late_ordinary_ack_and_partial_fill_restore_only_unfilled_reduce_responsibility() {
    for fill_first in [false, true] {
        let (mut s, c, mut command) = futures_fixture();
        s.positions.push(PositionSnapshot {
            instrument_id: 12,
            side: v::PositionSide::Both,
            quantity: units(2),
            entry_bound: units(100),
            ownership: vec![(8, units(2))],
        });
        let v::CommandBody::New(ref mut order) = command.body else {
            unreachable!()
        };
        order.side = v::Side::Sell;
        order.reduce_only = true;
        order.quantity = v::Quantity::Base(venue::d(2, 0));
        let mut account = Account::from_snapshot(s, c).unwrap();
        let admitted = account.admit(&canonical(command.clone()), 60).unwrap();
        account.apply_abort(&admitted).unwrap();
        let mut report = fill(&command, "late-trade", "1", "100", "100");
        let v::VenueEventBody::Fill(ref mut f) = report.body else {
            unreachable!()
        };
        f.realized_pnl = Some(v::AssetDelta {
            asset: command.context.instrument.settlement.clone(),
            amount: venue::d(0, 0),
        });
        if !fill_first {
            f.observation.status = v::OrderStatus::New;
            report.body = v::VenueEventBody::Order(Box::new(f.observation.clone()));
        }
        let report = event(report);
        account.apply_event(&report).unwrap();
        assert!(account.frozen());
        assert_eq!(
            account
                .order(&admitted.orders()[0])
                .unwrap()
                .reduce_allocation,
            units(if fill_first { 1 } else { 2 })
        );
        let seq = account.ledger_sequence();
        account.apply_event(&report).unwrap();
        if fill_first {
            assert_eq!(account.ledger_sequence(), seq);
        }
        assert_eq!(
            account
                .order(&admitted.orders()[0])
                .unwrap()
                .reduce_allocation,
            units(if fill_first { 1 } else { 2 })
        );
    }
}

#[test]
fn verified_uncreated_close_preserves_cash_and_cannot_hide_a_late_child() {
    let (s, c, mut command) = futures_fixture();
    let v::CommandBody::New(mut order) = command.body.clone() else {
        unreachable!()
    };
    order.order_type = v::OrderType::StopLimit;
    order.trigger = Some(v::Trigger {
        price: venue::d(110, 0),
        working_price: v::WorkingPrice::Contract,
        price_protect: false,
    });
    let client = order.client_id.clone();
    command.body = v::CommandBody::Algo(v::AlgoOrder {
        parent_order_id: [78; 16],
        order,
    });
    let creating = canonical(command.clone());
    let mut account = Account::from_snapshot(s.clone(), c.clone()).unwrap();
    let a = account.admit(&creating, 60).unwrap();
    let mut raw = venue::event();
    raw.identity.product = v::Product::LinearPerpetual;
    raw.body = v::VenueEventBody::Structure(Box::new(v::StructureObservation {
        parent: v::StructureIdentity {
            structure_id: [78; 16],
            original_intent_id: a.intent().intent_id,
            original_intent_exec_epoch: a.intent().epoch,
            kind: v::StructureKind::Algo,
            client_id: client,
            venue_parent_id: Some(v::VenueOrderId::new("800").unwrap()),
        },
        status: v::StructureStatus::Terminal,
        venue_status: v::BoundedText::new("CANCELED").unwrap(),
        children: v::BoundedList::new(vec![]).unwrap(),
    }));
    let terminal = event(raw.clone());
    account.apply_event(&terminal).unwrap();
    let cash = account.asset(key(2)).unwrap();
    let shadow = account.venue_shadow(1).unwrap();
    let binding = UncreatedAlgoClosed {
        scope: s.scope.clone(),
        account_epoch: s.account_epoch,
        ledger_sequence: account.ledger_sequence(),
        baseline_digest: [10; 32],
        history_digest: [11; 32],
        input_stream: [12; 16],
        through_sequence: 9,
        state_digest: account
            .account_inputs_state_digest([10; 32], [11; 32])
            .unwrap(),
        coverage_digest: [13; 32],
        structure_id: [78; 16],
        intent: a.intent(),
        command_digest: creating.digest(),
        venue_parent_id: v::VenueOrderId::new("800").unwrap(),
    };
    assert_eq!(account.close_uncreated_algo(&binding), Ok(Applied::New));
    assert!(account.frozen());
    assert_eq!(account.asset(key(2)).unwrap(), cash);
    assert_eq!(
        account.venue_shadow(1).unwrap(),
        shadow,
        "only the later full reconciliation resets venue shadow"
    );
    let order = account.order(&a.orders()[0]).unwrap();
    assert_eq!(order.remaining_quantity, units(0));
    assert!(!order.terminal && !order.aborted);
    assert!(order.fee_reconciled);
    let sequence = account.ledger_sequence();
    assert_eq!(
        account.close_uncreated_algo(&binding),
        Ok(Applied::Duplicate)
    );
    assert_eq!(account.ledger_sequence(), sequence);
    account.apply_event(&terminal).unwrap();
    assert_eq!(
        account.order(&a.orders()[0]).unwrap().remaining_quantity,
        units(0)
    );
    let v::VenueEventBody::Structure(ref mut parent) = raw.body else {
        unreachable!()
    };
    parent.status = v::StructureStatus::Triggered;
    let v::VenueEventBody::Fill(fill) = venue::event().body else {
        unreachable!()
    };
    let mut child = fill.observation.order;
    child.order_id = [88; 16];
    child.original_intent_id = a.intent().intent_id;
    child.original_intent_exec_epoch = a.intent().epoch;
    child.client_id = Some(a.orders()[0].client_id.clone());
    child.venue_order_id = Some(v::VenueOrderId::new("42").unwrap());
    child.parent_order_id = Some([78; 16]);
    child.venue_algo_id = Some(v::VenueOrderId::new("800").unwrap());
    child.algo_client_id = Some(parent.parent.client_id.clone());
    parent.children = v::BoundedList::new(vec![v::StructureChild {
        order: child,
        reported_client_id: None,
    }])
    .unwrap();
    account.apply_event(&event(raw)).unwrap();
    assert!(account.frozen());
    let order = account.order(&a.orders()[0]).unwrap();
    assert_eq!(order.remaining_quantity, units(1));
    assert!(!order.fee_reconciled);
    assert_eq!(account.asset(key(2)).unwrap(), cash);
    assert_eq!(
        account.close_uncreated_algo(&binding),
        Ok(Applied::Duplicate),
        "history retry must not re-close a contradicted child"
    );
    assert_eq!(
        account.order(&a.orders()[0]).unwrap().remaining_quantity,
        units(1)
    );
}
