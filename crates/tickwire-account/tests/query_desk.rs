mod common;
use common::*;
use tickwire_account::pool::PoolFull;
use tickwire_account::*;
use tickwire_venue as v;

fn order_id_for(intent_id: u64, strategy: u64) -> [u8; 16] {
    let mut order_id = [0u8; 16];
    order_id[..8].copy_from_slice(&intent_id.to_le_bytes());
    order_id[8..].copy_from_slice(&strategy.to_le_bytes());
    order_id
}

fn order_command(base: &v::Command, intent_id: u64, client_id: &str, strategy: u64) -> v::Command {
    let mut cmd = base.clone();
    cmd.context.strategy.logical_strategy_id = strategy;
    cmd.context.request.intent_id = intent_id;
    cmd.context.request.recovery_object += intent_id;
    cmd.context.request.deadline = 5_000;
    let v::CommandBody::New(ref mut o) = cmd.body else {
        unreachable!()
    };
    o.client_id = venue::text(client_id);
    cmd
}

fn target_for(cmd: &v::Command) -> v::OrderTarget {
    let v::CommandBody::New(ref o) = cmd.body else {
        unreachable!()
    };
    v::OrderTarget {
        order_id: order_id_for(
            cmd.context.request.intent_id,
            cmd.context.strategy.logical_strategy_id,
        ),
        expected_owner_revision: 1,
        original_intent_exec_epoch: cmd.context.request.intent_exec_epoch,
        client_id: Some(o.client_id.clone()),
        venue_order_id: None,
    }
}

fn management_command(base: &v::Command, intent_id: u64, body: v::CommandBody) -> v::Command {
    let mut cmd = base.clone();
    cmd.context.request.intent_id = intent_id;
    cmd.context.request.recovery_object += intent_id;
    cmd.context.request.deadline = 5_000;
    cmd.body = body;
    cmd
}

fn order_observation(
    cmd: &v::Command,
    status: v::OrderStatus,
    cumulative: &str,
    venue_id: &str,
) -> v::VenueEvent {
    let mut ev = fill(cmd, "unused", "0.001", "60000", "60");
    let v::VenueEventBody::Fill(ref mut f) = ev.body else {
        unreachable!()
    };
    f.observation.order.order_id = order_id_for(
        cmd.context.request.intent_id,
        cmd.context.strategy.logical_strategy_id,
    );
    f.observation.order.venue_order_id = Some(venue::text(venue_id));
    f.observation.status = status;
    f.observation.cumulative_quantity = Some(cumulative.parse().unwrap());
    ev.body = v::VenueEventBody::Order(Box::new(f.observation.clone()));
    ev
}

fn order_fill(cmd: &v::Command, trade: &str, q: &str, price: &str, quote: &str) -> v::VenueEvent {
    let mut ev = fill(cmd, trade, q, price, quote);
    let v::VenueEventBody::Fill(ref mut f) = ev.body else {
        unreachable!()
    };
    f.observation.order.order_id = order_id_for(
        cmd.context.request.intent_id,
        cmd.context.strategy.logical_strategy_id,
    );
    ev
}

fn rejected_event(cmd: &v::Command, client_id: Option<&str>) -> v::VenueEvent {
    let mut ev = venue::event();
    ev.identity.product = cmd.context.request.product;
    ev.identity.instrument_id = cmd.context.instrument.instrument_id;
    ev.body = v::VenueEventBody::Rejected {
        client_id: client_id.map(venue::text),
        venue_code: venue::text("-2010"),
        message: v::BoundedMessage::new("Account has insufficient balance").unwrap(),
    };
    ev
}

#[test]
fn single_order_doubt_locks_spot_and_futures_reservations_without_freezing_sibling_strategy() {
    // 1. Spot isolation: Order 1 in doubt keeps 100% of quote reserve locked while sibling trades.
    let (s, mut c, base_cmd) = fixture();
    c.entries[0].strategy_limits.push((9, units(100)));
    let mut spot = Account::from_snapshot(s, c).unwrap();

    let cmd1 = order_command(&base_cmd, 10, "spot-doubt-1", 8);
    let adm1 = spot.admit(&canonical(cmd1.clone()), 60).unwrap();
    let key1 = adm1.orders()[0].clone();
    assert_eq!(spot.asset(key(2)).unwrap().reserved, units(60));

    spot.mark_order_in_doubt(&key1, DoubtReason::PlaceAmbiguous)
        .unwrap();
    assert!(!spot.frozen());
    assert_eq!(
        spot.order_in_doubt(&key1),
        Some(DoubtReason::PlaceAmbiguous)
    );
    assert_eq!(
        spot.in_doubt_orders().collect::<Vec<_>>(),
        vec![(&key1, DoubtReason::PlaceAmbiguous)]
    );

    // First-byte gate on the in-doubt order itself is blocked.
    assert_eq!(spot.check_admission(&adm1, 61), Err(Error::Unreconciled));

    // Cancel and Query on the in-doubt order remain admissible and pass check_admission.
    let cancel_cmd = management_command(&cmd1, 12, v::CommandBody::Cancel(target_for(&cmd1)));
    let cancel_adm = spot.admit(&canonical(cancel_cmd), 63).unwrap();
    spot.check_admission(&cancel_adm, 64).unwrap();

    let query_cmd = management_command(&cmd1, 13, v::CommandBody::Query(target_for(&cmd1)));
    let query_adm = spot.admit(&canonical(query_cmd), 65).unwrap();
    spot.check_admission(&query_adm, 66).unwrap();

    // Confirmed order marked in doubt rejects new AmendKeepPriority with Error::Unreconciled.
    let cmd_conf = order_command(&base_cmd, 14, "spot-conf-doubt", 8);
    let adm_conf = spot.admit(&canonical(cmd_conf.clone()), 67).unwrap();
    let key_conf = adm_conf.orders()[0].clone();
    spot.apply_event(&event(order_observation(
        &cmd_conf,
        v::OrderStatus::New,
        "0",
        "v-conf-14",
    )))
    .unwrap();
    spot.mark_order_in_doubt(&key_conf, DoubtReason::CancelAmbiguous)
        .unwrap();
    let mut conf_target = target_for(&cmd_conf);
    conf_target.venue_order_id = Some(venue::text("v-conf-14"));
    let amend_cmd = management_command(
        &cmd_conf,
        15,
        v::CommandBody::AmendKeepPriority {
            target: conf_target,
            new_quantity: venue::d(5, -4),
            new_client_id: Some(venue::text("spot-conf-doubt")),
        },
    );
    assert_eq!(
        spot.admit(&canonical(amend_cmd), 68),
        Err(Error::Unreconciled)
    );
    spot.clear_order_in_doubt(&key_conf).unwrap();
    spot.apply_event(&event(order_observation(
        &cmd_conf,
        v::OrderStatus::Canceled,
        "0",
        "v-conf-14",
    )))
    .unwrap();
    spot.reconcile_fees(&key_conf, f(0)).unwrap();

    // Eviction never reclaims an in-doubt order or its admission regardless of elapsed time.
    spot.set_retention_window_ns(Some(1_000));
    let _ = spot.evict_expired(10_000_000_000);
    assert!(spot.order(&key1).is_some());
    assert!(spot.has_admission(adm1.intent()));

    // Healthy sibling strategy 9 admits, passes first-byte gate, fills, and closes normally.
    let mut sib_instance = [22u8; 16];
    sib_instance[0] = 9;
    let mut cmd2 = order_command(&base_cmd, 20, "spot-sibling-2", 9);
    cmd2.context.strategy.instance_id = sib_instance;
    let adm2 = spot.admit(&canonical(cmd2.clone()), 70).unwrap();
    spot.check_admission(&adm2, 71).unwrap();
    assert_eq!(spot.asset(key(2)).unwrap().reserved, units(120));

    spot.apply_event(&event(terminal(&cmd2, "0"))).unwrap();
    spot.reconcile_fees(&adm2.orders()[0], f(0)).unwrap();
    // Order 1's 60-unit reservation remains 100% locked!
    assert_eq!(spot.asset(key(2)).unwrap().reserved, units(60));
    assert!(!spot.frozen());

    // 2. Futures isolation: both shadow_holds and reduce allocations stay locked while sibling trades.
    let (mut fs, mut fc, fcmd) = futures_fixture();
    fc.entries[0].strategy_limits.push((9, units(100)));
    fs.positions.push(PositionSnapshot {
        instrument_id: 12,
        side: v::PositionSide::Both,
        quantity: units(5),
        entry_bound: units(100),
        ownership: vec![(8, units(5))],
    });
    let mut fut = Account::from_snapshot(fs, fc).unwrap();
    let shadow_before = fut.venue_shadow(1).unwrap();

    let inc_cmd = order_command(&fcmd, 30, "fut-inc-1", 8);
    let inc_adm = fut.admit(&canonical(inc_cmd), 60).unwrap();
    let inc_key = inc_adm.orders()[0].clone();
    let shadow_after_inc = fut.venue_shadow(1).unwrap();
    assert!(shadow_after_inc.atoms() < shadow_before.atoms());

    let mut red_cmd = order_command(&fcmd, 31, "fut-red-1", 8);
    if let v::CommandBody::New(ref mut o) = red_cmd.body {
        o.side = v::Side::Sell;
        o.reduce_only = true;
        o.quantity = v::Quantity::Base(venue::d(2, 0));
    }
    let red_adm = fut.admit(&canonical(red_cmd), 61).unwrap();
    let red_key = red_adm.orders()[0].clone();
    assert_eq!(fut.order(&red_key).unwrap().reduce_allocation, units(2));

    fut.mark_order_in_doubt(&inc_key, DoubtReason::PlaceAmbiguous)
        .unwrap();
    fut.mark_order_in_doubt(&red_key, DoubtReason::CancelAmbiguous)
        .unwrap();
    assert!(!fut.frozen());

    let mut sib_fut = order_command(&fcmd, 32, "fut-sib-2", 9);
    sib_fut.context.strategy.instance_id = sib_instance;
    let sib_adm = fut.admit(&canonical(sib_fut), 62).unwrap();
    fut.check_admission(&sib_adm, 63).unwrap();
    assert_eq!(fut.order(&red_key).unwrap().reduce_allocation, units(2));
    assert!(fut.venue_shadow(1).unwrap().atoms() < shadow_after_inc.atoms());
}

#[test]
fn in_doubt_auto_resolves_across_order_fill_amendment_and_reactivation_and_preserves_replay() {
    let (s, c, base_cmd) = fixture();
    let mut live = Account::from_snapshot(s.clone(), c.clone()).unwrap();

    // Order A: PlaceAmbiguous -> terminal Filled observation (cumulative > filled) -> FillCatchup -> Fill -> None.
    let cmd_a = order_command(&base_cmd, 10, "order-a", 8);
    let adm_a = live.admit(&canonical(cmd_a.clone()), 60).unwrap();
    let key_a = adm_a.orders()[0].clone();
    live.mark_order_in_doubt(&key_a, DoubtReason::PlaceAmbiguous)
        .unwrap();

    let obs_a = event(order_observation(
        &cmd_a,
        v::OrderStatus::Filled,
        "0.001",
        "v-100",
    ));
    assert_eq!(live.apply_event(&obs_a), Ok(Applied::New));
    assert_eq!(live.order_in_doubt(&key_a), Some(DoubtReason::FillCatchup));

    let fill_a = event(order_fill(&cmd_a, "trade-a-1", "0.001", "60000", "60"));
    assert_eq!(live.apply_event(&fill_a), Ok(Applied::New));
    assert_eq!(live.order_in_doubt(&key_a), None);
    // Duplicate fill must return Applied::Duplicate even though in_doubt changed during first apply!
    assert_eq!(live.apply_event(&fill_a), Ok(Applied::Duplicate));

    // Offline replay (where volatile in_doubt was never set) must yield identical ledger & duplicate behavior.
    let log = vec![
        RecoveryRecord::Admit {
            command: Box::new(canonical(cmd_a.clone())),
            evaluated_at: 60,
            expected: adm_a.image(),
        },
        RecoveryRecord::VenueEvent(Box::new(obs_a)),
        RecoveryRecord::VenueEvent(Box::new(fill_a.clone())),
    ];
    let mut replayed = Account::recover(s.clone(), c.clone(), &log).unwrap();
    assert_eq!(live.asset(key(1)).unwrap(), replayed.asset(key(1)).unwrap());
    assert_eq!(live.asset(key(2)).unwrap(), replayed.asset(key(2)).unwrap());
    assert_eq!(replayed.apply_event(&fill_a), Ok(Applied::Duplicate));

    // Order B: CancelAmbiguous -> Canceled (cumulative == 0) -> None.
    let cmd_b = order_command(&base_cmd, 20, "order-b", 8);
    let adm_b = live.admit(&canonical(cmd_b.clone()), 65).unwrap();
    let key_b = adm_b.orders()[0].clone();
    live.mark_order_in_doubt(&key_b, DoubtReason::CancelAmbiguous)
        .unwrap();
    let obs_b = event(order_observation(
        &cmd_b,
        v::OrderStatus::Canceled,
        "0",
        "v-200",
    ));
    live.apply_event(&obs_b).unwrap();
    assert_eq!(live.order_in_doubt(&key_b), None);

    // Order C: AmendAmbiguous -> cleared by AmendmentRejected (and queued amendment blocked while in doubt).
    let cmd_c = order_command(&base_cmd, 30, "order-c", 8);
    let adm_c = live.admit(&canonical(cmd_c.clone()), 70).unwrap();
    let key_c = adm_c.orders()[0].clone();
    let obs_c_ev = order_observation(&cmd_c, v::OrderStatus::New, "0", "v-300");
    let v::VenueEventBody::Order(ref obs_c) = obs_c_ev.body else {
        unreachable!()
    };
    let obs_c_order_ident = obs_c.order.clone();
    live.apply_event(&event(obs_c_ev)).unwrap();
    let mut target_c = target_for(&cmd_c);
    target_c.venue_order_id = Some(venue::text("v-300"));
    let amend_c = canonical(management_command(
        &cmd_c,
        31,
        v::CommandBody::AmendKeepPriority {
            target: target_c,
            new_quantity: venue::d(5, -4),
            new_client_id: Some(venue::text("order-c")),
        },
    ));
    let amend_adm_c = live.admit(&amend_c, 71).unwrap();
    live.mark_order_in_doubt(&key_c, DoubtReason::AmendAmbiguous)
        .unwrap();
    assert_eq!(
        live.check_admission(&amend_adm_c, 72),
        Err(Error::Unreconciled)
    );

    let mut rej_ev = venue::event();
    rej_ev.body = v::VenueEventBody::AmendmentRejected(Box::new(v::AmendmentRejection {
        order: obs_c_order_ident,
        request_intent_id: 31,
        request_intent_exec_epoch: amend_c.command().context.request.intent_exec_epoch,
        request_digest: amend_c.digest(),
        venue_code: venue::text("-1000"),
    }));
    live.apply_event(&event(rej_ev)).unwrap();
    assert_eq!(live.order_in_doubt(&key_c), None);

    // OrderStatus::Unknown still freezes the account (preserving L0/L1 invariants), and Reactivation clears in_doubt.
    live.mark_order_in_doubt(&key_c, DoubtReason::PlaceAmbiguous)
        .unwrap();
    live.apply_event(&event(order_observation(
        &cmd_c,
        v::OrderStatus::Unknown,
        "0",
        "v-300",
    )))
    .unwrap();
    assert!(live.frozen());
    assert!(live.order(&key_c).unwrap().unknown);

    let reactivation = Reactivation {
        scope: s.scope,
        account_epoch: s.account_epoch,
        product: v::Product::Spot,
        baseline_digest: [7; 32],
        history_digest: [8; 32],
        ledger_sequence: live.ledger_sequence(),
        query_coverage_digest: [9; 32],
        input_stream: [1; 16],
        through_sequence: 1,
        private_connection_epoch: 1,
        gap_generation: 1,
        evaluated_at: 200,
        valid_until: 5_000,
        clock_boot_id: [2; 16],
        clock_generation: 1,
        writer_epoch: 4,
        execution_connection_epoch: 1,
        venue_available: vec![],
    };
    live.apply_reactivation(&reactivation).unwrap();
    assert!(!live.frozen());
    assert_eq!(live.order_in_doubt(&key_c), None);
}

#[test]
fn rejected_venue_event_closes_uncreated_order_releases_spot_and_futures_holds_and_replays_cleanly()
{
    // 1. Spot uncreated closure via VenueEventBody::Rejected
    let (s, c, base_cmd) = fixture();
    let mut spot = Account::from_snapshot(s.clone(), c.clone()).unwrap();
    spot.set_retention_window_ns(Some(1_000));

    let cmd = order_command(&base_cmd, 10, "rej-spot-1", 8);
    let ccmd = canonical(cmd.clone());
    let adm = spot.admit(&ccmd, 100).unwrap();
    let order_key = adm.orders()[0].clone();
    spot.mark_order_in_doubt(&order_key, DoubtReason::PlaceAmbiguous)
        .unwrap();
    assert_eq!(spot.asset(key(2)).unwrap().reserved, units(60));

    let mut rej = rejected_event(&cmd, Some("rej-spot-1"));
    rej.identity.received_mono_ns = 500;
    let crej = event(rej);
    assert_eq!(spot.apply_event(&crej), Ok(Applied::New));
    assert!(!spot.frozen());
    assert_eq!(spot.order_in_doubt(&order_key), None);
    let view = spot.order(&order_key).unwrap();
    assert!(view.uncreated);
    assert!(view.fee_reconciled);
    assert_eq!(view.remaining_quantity, f(0));
    assert_eq!(spot.asset(key(2)).unwrap().reserved, units(0));

    // Idempotent duplicate delivery
    let seq = spot.ledger_sequence();
    assert_eq!(spot.apply_event(&crej), Ok(Applied::Duplicate));
    assert_eq!(spot.ledger_sequence(), seq);

    // Deterministic offline recovery reproduces uncreated closure with zero reservation
    let recovered = Account::recover(
        s,
        c,
        &[
            RecoveryRecord::Admit {
                command: Box::new(ccmd),
                evaluated_at: 100,
                expected: adm.image(),
            },
            RecoveryRecord::VenueEvent(Box::new(crej)),
        ],
    )
    .unwrap();
    assert_eq!(recovered.asset(key(2)).unwrap().reserved, units(0));
    let rview = recovered.order(&order_key).unwrap();
    assert!(rview.uncreated);
    assert!(!rview.unknown);

    // Evicts cleanly once retention window passes
    assert_eq!(spot.evict_expired(2_000), 1);
    assert!(spot.order(&order_key).is_none());
    assert!(!spot.has_admission(adm.intent()));

    // 2. Futures uncreated closure releases both shadow_holds and reduce allocations without freezing
    let (mut fs, fc, fcmd) = futures_fixture();
    fs.positions.push(PositionSnapshot {
        instrument_id: 12,
        side: v::PositionSide::Both,
        quantity: units(4),
        entry_bound: units(100),
        ownership: vec![(8, units(4))],
    });
    let mut fut = Account::from_snapshot(fs, fc).unwrap();
    let baseline_shadow = fut.venue_shadow(1).unwrap();

    let inc = order_command(&fcmd, 40, "rej-fut-inc", 8);
    let inc_adm = fut.admit(&canonical(inc.clone()), 100).unwrap();
    assert!(fut.venue_shadow(1).unwrap().atoms() < baseline_shadow.atoms());

    let mut red = order_command(&fcmd, 41, "rej-fut-red", 8);
    if let v::CommandBody::New(ref mut o) = red.body {
        o.side = v::Side::Sell;
        o.reduce_only = true;
        o.quantity = v::Quantity::Base(venue::d(3, 0));
    }
    let red_adm = fut.admit(&canonical(red.clone()), 101).unwrap();
    assert_eq!(
        fut.order(&red_adm.orders()[0]).unwrap().reduce_allocation,
        units(3)
    );

    assert_eq!(
        fut.apply_event(&event(rejected_event(&inc, Some("rej-fut-inc")))),
        Ok(Applied::New)
    );
    assert_eq!(
        fut.apply_event(&event(rejected_event(&red, Some("rej-fut-red")))),
        Ok(Applied::New)
    );
    assert!(!fut.frozen());
    assert_eq!(fut.venue_shadow(1).unwrap(), baseline_shadow);
    assert_eq!(
        fut.order(&inc_adm.orders()[0]).unwrap().remaining_quantity,
        units(0)
    );
    assert_eq!(
        fut.order(&red_adm.orders()[0]).unwrap().reduce_allocation,
        units(0)
    );
}

#[test]
fn rejected_venue_event_freezes_on_contradictions_structures_and_late_execution_reopen() {
    // 1. Missing client_id freezes
    {
        let (mut a, cmd) = setup();
        a.admit(&canonical(cmd.clone()), 60).unwrap();
        assert_eq!(
            a.apply_event(&event(rejected_event(&cmd, None))),
            Err(Error::Unreconciled)
        );
        assert!(a.frozen());
    }

    // 2. Confirmed order receiving Rejected freezes and retains reservation
    {
        let (mut a, cmd) = setup();
        a.admit(&canonical(cmd.clone()), 60).unwrap();
        a.apply_event(&event(order_observation(
            &cmd,
            v::OrderStatus::New,
            "0",
            "v-1",
        )))
        .unwrap();
        assert_eq!(
            a.apply_event(&event(rejected_event(&cmd, Some("test-1")))),
            Err(Error::Unreconciled)
        );
        assert!(a.frozen());
        assert_eq!(a.asset(key(2)).unwrap().reserved, units(60));
    }

    // 3. Multi-leg structure leg receiving Rejected freezes (must use UncreatedAlgoClosed)
    {
        let (s, c, mut cmd) = fixture();
        let v::CommandBody::New(first) = cmd.body.clone() else {
            unreachable!()
        };
        let mut second = first.clone();
        second.client_id = venue::text("leg-2");
        cmd.body = v::CommandBody::SpotList(v::ListOrder {
            list_id: [88; 16],
            client_id: venue::text("list-1"),
            kind: v::ListKind::Oto,
            legs: v::BoundedList::new(vec![first, second]).unwrap(),
        });
        let mut a = Account::from_snapshot(s, c).unwrap();
        a.admit(&canonical(cmd.clone()), 60).unwrap();
        assert_eq!(
            a.apply_event(&event(rejected_event(&cmd, Some("test-1")))),
            Err(Error::Unreconciled)
        );
        assert!(a.frozen());
        assert_eq!(a.asset(key(2)).unwrap().reserved, units(120));
    }

    // 4. Late contradictory OrderObservation after uncreated == true reopens liability and freezes!
    {
        let (mut a, cmd) = setup();
        let adm = a.admit(&canonical(cmd.clone()), 60).unwrap();
        let okey = adm.orders()[0].clone();
        a.apply_event(&event(rejected_event(&cmd, Some("test-1"))))
            .unwrap();
        assert!(!a.frozen());
        assert_eq!(a.asset(key(2)).unwrap().reserved, units(0));
        assert!(a.order(&okey).unwrap().uncreated);

        // Late venue ack contradicts uncreated closure -> reopen_aborted restores reserve and freezes!
        a.set_retention_window_ns(Some(1_000));
        a.apply_event(&event(order_observation(
            &cmd,
            v::OrderStatus::New,
            "0",
            "late-v-1",
        )))
        .unwrap();
        assert!(a.frozen());
        let view = a.order(&okey).unwrap();
        assert!(!view.uncreated);
        assert!(!view.fee_reconciled);
        assert!(a.is_intent_active(adm.intent()));
        assert_eq!(a.evict_expired(10_000_000), 0);
        assert_eq!(a.asset(key(2)).unwrap().reserved, units(60));
    }

    // 5. Late contradictory Fill on Futures reduce-only order after uncreated == true restores reduce and freezes!
    {
        let (mut fs, fc, mut fcmd) = futures_fixture();
        fs.positions.push(PositionSnapshot {
            instrument_id: 12,
            side: v::PositionSide::Both,
            quantity: units(5),
            entry_bound: units(100),
            ownership: vec![(8, units(5))],
        });
        if let v::CommandBody::New(ref mut o) = fcmd.body {
            o.side = v::Side::Sell;
            o.reduce_only = true;
            o.quantity = v::Quantity::Base(venue::d(3, 0));
        }
        let mut fut = Account::from_snapshot(fs, fc).unwrap();
        fut.set_retention_window_ns(Some(1_000));
        let adm = fut.admit(&canonical(fcmd.clone()), 60).unwrap();
        let okey = adm.orders()[0].clone();
        fut.apply_event(&event(rejected_event(&fcmd, Some("test-1"))))
            .unwrap();
        assert!(!fut.frozen());
        assert_eq!(fut.order(&okey).unwrap().reduce_allocation, units(0));

        // Late partial fill of 1 lot arrives -> restores remaining 2-lot reduce allocation and freezes!
        fut.apply_event(&event(order_fill(
            &fcmd,
            "late-fut-fill",
            "1",
            "100",
            "100",
        )))
        .unwrap();
        assert!(fut.frozen());
        let view = fut.order(&okey).unwrap();
        assert!(!view.uncreated);
        assert!(fut.is_intent_active(adm.intent()));
        assert_eq!(fut.evict_expired(10_000_000), 0);
        assert_eq!(view.filled_quantity, units(1));
        assert_eq!(view.reduce_allocation, units(2));
        assert_eq!(fut.position(12, v::PositionSide::Both).unwrap().0, units(4));
    }
}

fn make_order_key(id: u64) -> OrderKey {
    OrderKey {
        intent: IntentKey {
            epoch: 1,
            intent_id: id,
        },
        client_id: v::ClientId::new(format!("tw1order{id:024}")).unwrap(),
    }
}

#[test]
fn query_desk_preallocates_once_purges_retired_asks_and_holds_slots_only_on_wire() {
    assert_eq!(MAX_REST_SLOTS, 32);
    assert_eq!(MAX_STATUS_ASKS, MAX_ORDERS + 32);

    let mut desk = QueryDesk::new(QueryConfig {
        max_in_flight_slots: 4,
        ..QueryConfig::default()
    });
    let slots_ptr = desk.slots_addr();
    let buckets_ptr = desk.buckets_addr();

    for i in 0..(MAX_STATUS_ASKS as u64) {
        desk.enqueue_ask(
            make_order_key(i),
            12,
            None,
            AskKind::Place,
            true,
            1_000,
            |_| true,
        )
        .unwrap();
    }
    assert_eq!(desk.waiting_count(), MAX_STATUS_ASKS);
    assert_eq!(desk.in_flight_count(), 0);
    assert_eq!(desk.next_deadline_mono_ns(), Some(6_000_001_000));

    // When all 1,056 asks are live, 1,057th returns Err(PoolFull) without reallocating.
    assert_eq!(
        desk.enqueue_ask(
            make_order_key(99_999),
            12,
            None,
            AskKind::Place,
            true,
            1_000,
            |_| true,
        ),
        Err(PoolFull)
    );

    // Non-live waiting asks are purged in place when capacity is needed.
    desk.enqueue_ask(
        make_order_key(99_999),
        12,
        None,
        AskKind::Place,
        true,
        2_000,
        |k| k.intent.intent_id < 10,
    )
    .unwrap();
    assert_eq!(desk.waiting_count(), 11);
    assert_eq!(desk.slots_addr(), slots_ptr);
    assert_eq!(desk.buckets_addr(), buckets_ptr);

    // poll_due prunes asks where !should_query without taking a wire slot, then caps at max_in_flight_slots (4).
    let mut tickets = Vec::new();
    while let Some(t) = desk.poll_due(7_000_000_000, |k, _| k.intent.intent_id >= 5) {
        tickets.push(t);
    }
    assert_eq!(tickets.len(), 4);
    assert_eq!(desk.in_flight_count(), 4);
    assert!(!desk.has_ask(&make_order_key(0)));
    assert_eq!(desk.slots_addr(), slots_ptr);
    assert_eq!(desk.buckets_addr(), buckets_ptr);
}

#[test]
fn query_desk_coalesces_asks_and_enforces_not_found_grace_and_trades_before_close() {
    let mut desk = QueryDesk::new(QueryConfig {
        status_query_delay_ns: 6_000_000_000,
        place_retry_ns: 1_000_000_000,
        not_found_grace_ns: 10_000_000_000,
        ..QueryConfig::default()
    });
    let k1 = make_order_key(1);

    // Coalesce duplicate Place asks and bind native_order_id when learned.
    desk.enqueue_ask(
        k1.clone(),
        12,
        None,
        AskKind::Place,
        true,
        1_000_000_000,
        |_| true,
    )
    .unwrap();
    desk.enqueue_ask(
        k1.clone(),
        12,
        Some(venue::text("native-1")),
        AskKind::Place,
        true,
        2_000_000_000,
        |_| true,
    )
    .unwrap();
    assert_eq!(desk.waiting_count(), 1);
    assert!(desk.poll_due(6_999_999_999, |_, _| true).is_none());

    let t1 = desk.poll_due(7_000_000_000, |_, _| true).unwrap();
    assert_eq!(t1.native_order_id, Some(venue::text("native-1")));
    assert_eq!(desk.in_flight_count(), 1);

    // 1. Inside not_found_grace_ns (7s < 1s + 10s = 11s): retries instead of closing uncreated!
    assert_eq!(
        desk.on_not_found(&t1, 7_500_000_000, false),
        Some(QueryResolution::RetryScheduled {
            order: k1.clone(),
            next_due_mono_ns: 8_500_000_000,
        })
    );
    assert_eq!(desk.in_flight_count(), 0);
    assert_eq!(desk.issued_query_count(&k1), Some(1));

    // 2. Local non-send (rate limit / transport unavailable before write) never increments issued_query_count!
    let t2 = desk.poll_due(8_500_000_000, |_, _| true).unwrap();
    assert_eq!(
        desk.on_not_sent(&t2, 8_600_000_000),
        Some(QueryResolution::RetryScheduled {
            order: k1.clone(),
            next_due_mono_ns: 9_600_000_000,
        })
    );
    assert_eq!(desk.issued_query_count(&k1), Some(1));

    // 3. At/after grace deadline (11s >= 1s + 10s): returns CloseUncreated and removes ask!
    let t3 = desk.poll_due(11_000_000_000, |_, _| true).unwrap();
    assert_eq!(
        desk.on_not_found(&t3, 11_000_000_000, false),
        Some(QueryResolution::CloseUncreated {
            order: k1.clone(),
            instrument_id: 12,
        })
    );
    assert!(!desk.has_ask(&k1));

    // 4. Confirmed order disappearing from openOrders -> FetchTradesBeforeClose -> Trades loop until caught up!
    let k2 = make_order_key(2);
    desk.enqueue_ask(
        k2.clone(),
        12,
        None,
        AskKind::Cancel,
        false,
        1_000_000_000,
        |_| true,
    )
    .unwrap();
    let tc = desk.poll_due(7_000_000_000, |_, _| true).unwrap();
    assert_eq!(
        desk.on_not_found(&tc, 7_100_000_000, true),
        Some(QueryResolution::FetchTradesBeforeClose {
            order: k2.clone(),
            instrument_id: 12,
        })
    );
    assert_eq!(desk.ask_kind(&k2), Some(AskKind::Trades));

    // Trades ask is immediately due at 7.1s; partial catch-up retries before resolving.
    let tt1 = desk.poll_due(7_100_000_000, |_, _| true).unwrap();
    assert_eq!(tt1.kind, AskKind::Trades);
    assert_eq!(
        desk.on_trades_applied(&tt1, 7_200_000_000, false),
        Some(QueryResolution::RetryScheduled {
            order: k2.clone(),
            next_due_mono_ns: 8_200_000_000,
        })
    );
    let tt2 = desk.poll_due(8_200_000_000, |_, _| true).unwrap();
    assert_eq!(
        desk.on_trades_applied(&tt2, 8_300_000_000, true),
        Some(QueryResolution::Resolved { order: k2.clone() })
    );
    assert!(!desk.has_ask(&k2));

    // 5. Snapshot with cumulative_atoms > local_filled_atoms also switches to FetchTradesBeforeClose!
    let k3 = make_order_key(3);
    desk.enqueue_ask(
        k3.clone(),
        12,
        None,
        AskKind::Place,
        true,
        1_000_000_000,
        |_| true,
    )
    .unwrap();
    let ts = desk.poll_due(7_000_000_000, |_, _| true).unwrap();
    assert_eq!(
        desk.on_status_snapshot(
            &ts,
            7_100_000_000,
            SnapshotSummary {
                confirmed: true,
                terminal: true,
                cumulative_atoms: 500,
                local_filled_atoms: 200,
                amend_resolved: true,
            },
        ),
        Some(QueryResolution::FetchTradesBeforeClose {
            order: k3.clone(),
            instrument_id: 12,
        })
    );
    assert_eq!(desk.ask_kind(&k3), Some(AskKind::Trades));
}

#[test]
fn query_desk_cancel_coalescing_cancel_after_doubling_backoff_cap_8_and_freeze_budget() {
    let mut desk = QueryDesk::default();

    // 1. Cancel joining while Place is in flight sets cancel_after and re-arms Cancel if order is open.
    let k1 = make_order_key(10);
    desk.enqueue_ask(
        k1.clone(),
        12,
        None,
        AskKind::Place,
        true,
        1_000_000_000,
        |_| true,
    )
    .unwrap();
    let tp = desk.poll_due(7_000_000_000, |_, _| true).unwrap();
    desk.enqueue_ask(
        k1.clone(),
        12,
        None,
        AskKind::Cancel,
        false,
        7_200_000_000,
        |_| true,
    )
    .unwrap();
    assert_eq!(
        desk.on_status_snapshot(
            &tp,
            7_500_000_000,
            SnapshotSummary {
                confirmed: true,
                terminal: false,
                cumulative_atoms: 0,
                local_filled_atoms: 0,
                amend_resolved: true,
            },
        ),
        Some(QueryResolution::RetryScheduled {
            order: k1.clone(),
            next_due_mono_ns: 13_500_000_000,
        })
    );
    assert_eq!(desk.ask_kind(&k1), Some(AskKind::Cancel));

    // Next Cancel snapshot showing still-open order returns CancelRejectedStillOpen and removes ask.
    let tc_open = desk.poll_due(13_500_000_000, |_, _| true).unwrap();
    assert_eq!(
        desk.on_status_snapshot(
            &tc_open,
            13_600_000_000,
            SnapshotSummary {
                confirmed: true,
                terminal: false,
                cumulative_atoms: 0,
                local_filled_atoms: 0,
                amend_resolved: true,
            },
        ),
        Some(QueryResolution::CancelRejectedStillOpen {
            order: k1.clone(),
            remaining_pending_cancels: 0,
        })
    );
    assert!(!desk.has_ask(&k1));

    // 2. Acked order Cancel transport failures double 1, 2, 4, 8, 16, 32, 64s and give up on call 8 without counting issued_query_count!
    let k2 = make_order_key(20);
    desk.enqueue_ask(k2.clone(), 12, None, AskKind::Cancel, false, 0, |_| true)
        .unwrap();
    let mut now = 6_000_000_000u64;
    let expected_delays_s = [1u64, 2, 4, 8, 16, 32, 64];
    for &delay_s in &expected_delays_s {
        let ticket = desk.poll_due(now, |_, _| true).unwrap();
        let next_due = now + delay_s * 1_000_000_000;
        assert_eq!(
            desk.on_transport_failure(&ticket, now),
            Some(QueryResolution::RetryScheduled {
                order: k2.clone(),
                next_due_mono_ns: next_due,
            })
        );
        assert_eq!(desk.issued_query_count(&k2), Some(0));
        now = next_due;
    }
    let ticket_8 = desk.poll_due(now, |_, _| true).unwrap();
    assert_eq!(
        desk.on_transport_failure(&ticket_8, now),
        Some(QueryResolution::CancelGivenUp {
            order: k2.clone(),
            given_up_cancels: 1,
        })
    );
    assert!(!desk.has_ask(&k2));

    // 3. Unacked-place + Cancel hitting 8 cancel failures downgrades back to AskKind::Place instead of dropping!
    let k3 = make_order_key(30);
    desk.enqueue_ask(k3.clone(), 12, None, AskKind::Place, true, 0, |_| true)
        .unwrap();
    desk.enqueue_ask(k3.clone(), 12, None, AskKind::Cancel, false, 0, |_| true)
        .unwrap();
    let mut t_now = 6_000_000_000u64;
    for _ in 0..7 {
        let ticket = desk.poll_due(t_now, |_, _| true).unwrap();
        let Some(QueryResolution::RetryScheduled {
            next_due_mono_ns, ..
        }) = desk.on_transport_failure(&ticket, t_now)
        else {
            panic!("expected retry");
        };
        t_now = next_due_mono_ns;
    }
    let ticket_last = desk.poll_due(t_now, |_, _| true).unwrap();
    assert_eq!(
        desk.on_transport_failure(&ticket_last, t_now),
        Some(QueryResolution::CancelGivenUp {
            order: k3.clone(),
            given_up_cancels: 1,
        })
    );
    assert!(desk.has_ask(&k3));
    assert_eq!(desk.ask_kind(&k3), Some(AskKind::Place));
    assert_eq!(desk.issued_query_count(&k3), Some(8));

    // 4. Optional max_query_count > 0 escalates to FreezeAccount ONLY on issued wire attempts!
    let mut strict_desk = QueryDesk::new(QueryConfig {
        max_query_count: 3,
        ..QueryConfig::default()
    });
    let k4 = make_order_key(40);
    strict_desk
        .enqueue_ask(k4.clone(), 12, None, AskKind::Place, true, 0, |_| true)
        .unwrap();
    let mut cur = 6_000_000_000u64;
    for attempt in 1..=2 {
        let t_unsent = strict_desk.poll_due(cur, |_, _| true).unwrap();
        let Some(QueryResolution::RetryScheduled {
            next_due_mono_ns, ..
        }) = strict_desk.on_not_sent(&t_unsent, cur)
        else {
            panic!("expected unsent retry");
        };
        cur = next_due_mono_ns;

        let t_sent = strict_desk.poll_due(cur, |_, _| true).unwrap();
        let Some(QueryResolution::RetryScheduled {
            next_due_mono_ns, ..
        }) = strict_desk.on_transport_failure(&t_sent, cur)
        else {
            panic!("expected retry on attempt {attempt}");
        };
        assert_eq!(strict_desk.issued_query_count(&k4), Some(attempt));
        cur = next_due_mono_ns;
    }
    let t_third = strict_desk.poll_due(cur, |_, _| true).unwrap();
    assert_eq!(
        strict_desk.on_transport_failure(&t_third, cur),
        Some(QueryResolution::FreezeAccount {
            order: k4.clone(),
            issued_queries: 3,
        })
    );
    assert!(!strict_desk.has_ask(&k4));
}

#[test]
fn contradicted_uncreated_or_aborted_futures_order_restores_shadow_holds_and_eviction_preserves_shadow()
 {
    // 1. Contradicted uncreated Futures order restores shadow_holds (`venue_shadow`) and freezes.
    let (fs, fc, fcmd) = futures_fixture();
    let mut fut = Account::from_snapshot(fs.clone(), fc.clone()).unwrap();
    let shadow_initial = fut.venue_shadow(1).unwrap();

    let cmd = order_command(&fcmd, 100, "fut-contradicted-1", 8);
    let adm = fut.admit(&canonical(cmd.clone()), 60).unwrap();
    let key = adm.orders()[0].clone();
    let shadow_after_admit = fut.venue_shadow(1).unwrap();
    assert!(shadow_after_admit.atoms() < shadow_initial.atoms());

    // Deterministic single-order reject refunds shadow_holds without freezing.
    assert_eq!(
        fut.apply_event(&event(rejected_event(&cmd, Some("fut-contradicted-1"))))
            .unwrap(),
        Applied::New
    );
    assert!(!fut.frozen());
    assert!(fut.order(&key).unwrap().uncreated);
    assert_eq!(fut.venue_shadow(1).unwrap(), shadow_initial);

    // Late contradictory venue ack arrives after uncreated=true -> must restore shadow_holds & freeze!
    assert_eq!(
        fut.apply_event(&event(order_observation(
            &cmd,
            v::OrderStatus::New,
            "0",
            "v-fut-100",
        )))
        .unwrap(),
        Applied::New
    );
    assert!(fut.frozen());
    assert!(!fut.order(&key).unwrap().uncreated);
    assert_eq!(
        fut.venue_shadow(1).unwrap(),
        shadow_after_admit,
        "reopen_order must restore unrefreshed Futures shadow_holds on contradicted uncreated order"
    );

    // 2. Evicting a terminal retired Futures order before Reactivated must preserve shadow hold in venue_shadow!
    let mut fut2 = Account::from_snapshot(fs, fc)
        .unwrap()
        .with_retention_window_ns(Some(1_000));
    let cmd2 = order_command(&fcmd, 101, "fut-evict-shadow", 8);
    let adm2 = fut2.admit(&canonical(cmd2.clone()), 100).unwrap();
    let key2 = adm2.orders()[0].clone();
    let shadow_held = fut2.venue_shadow(1).unwrap();
    assert!(shadow_held.atoms() < shadow_initial.atoms());

    let mut cancel_obs = order_observation(&cmd2, v::OrderStatus::Canceled, "0", "v-fut-101");
    cancel_obs.identity.received_mono_ns = 200;
    fut2.apply_event(&event(cancel_obs)).unwrap();
    fut2.reconcile_fees(&key2, f(0)).unwrap();
    assert_eq!(fut2.venue_shadow(1).unwrap(), shadow_held);

    assert_eq!(fut2.evict_expired(5_000), 1);
    assert!(fut2.order(&key2).is_none());
    assert_eq!(
        fut2.venue_shadow(1).unwrap(),
        shadow_held,
        "evict_expired before Reactivated must never inflate venue_shadow"
    );
}

#[test]
fn query_desk_preserves_cancel_after_across_non_terminal_trades_and_trades_after_across_inflight_status()
 {
    let mut desk = QueryDesk::default();

    // 1. Place in flight + Cancel joins (`cancel_after = true`) + non-terminal PartiallyFilled snapshot (`cum > filled`)
    //    -> FetchTradesBeforeClose -> on_trades_applied(..., true) MUST re-arm AskKind::Cancel!
    let k1 = OrderKey {
        intent: IntentKey {
            epoch: 4,
            intent_id: 201,
        },
        client_id: venue::text("coalesce-cancel-trades"),
    };
    desk.enqueue_ask(k1.clone(), 12, None, AskKind::Place, true, 0, |_| true)
        .unwrap();
    let t_place = desk.poll_due(6_000_000_000, |_, _| true).unwrap();
    assert_eq!(t_place.kind, AskKind::Place);

    // Ambiguous Cancel joins while Place status query is in flight.
    desk.enqueue_ask(
        k1.clone(),
        12,
        None,
        AskKind::Cancel,
        false,
        6_100_000_000,
        |_| true,
    )
    .unwrap();

    // Status snapshot returns OPEN PartiallyFilled (`!terminal`) with `cumulative_atoms > local_filled_atoms`.
    assert_eq!(
        desk.on_status_snapshot(
            &t_place,
            6_200_000_000,
            SnapshotSummary {
                confirmed: true,
                terminal: false,
                cumulative_atoms: 400_000,
                local_filled_atoms: 0,
                amend_resolved: true,
            },
        ),
        Some(QueryResolution::FetchTradesBeforeClose {
            order: k1.clone(),
            instrument_id: 12,
        })
    );
    assert_eq!(desk.ask_kind(&k1), Some(AskKind::Trades));

    let t_trades = desk.poll_due(6_200_000_000, |_, _| true).unwrap();
    assert_eq!(t_trades.kind, AskKind::Trades);
    assert_eq!(
        desk.on_trades_applied(&t_trades, 6_300_000_000, true),
        Some(QueryResolution::RetryScheduled {
            order: k1.clone(),
            next_due_mono_ns: 12_300_000_000,
        }),
        "CancelAfterNeverLost: non-terminal Trades catch-up must re-arm coalesced Cancel ask"
    );
    assert_eq!(desk.ask_kind(&k1), Some(AskKind::Cancel));

    // 2. Place OrderStatus in flight + lagging WS orders enqueues AskKind::Trades (`trades_after = true`)
    //    + stale OrderStatus(New, cum=0) reply -> must still transition to FetchTradesBeforeClose!
    let k2 = OrderKey {
        intent: IntentKey {
            epoch: 4,
            intent_id: 202,
        },
        client_id: venue::text("inflight-trades-after"),
    };
    desk.enqueue_ask(k2.clone(), 12, None, AskKind::Place, true, 0, |_| true)
        .unwrap();
    let t_status = desk.poll_due(6_000_000_000, |_, _| true).unwrap();
    desk.enqueue_ask(
        k2.clone(),
        12,
        Some(venue::text("native-202")),
        AskKind::Trades,
        false,
        6_050_000_000,
        |_| true,
    )
    .unwrap();
    assert_eq!(
        desk.on_status_snapshot(
            &t_status,
            6_100_000_000,
            SnapshotSummary {
                confirmed: true,
                terminal: false,
                cumulative_atoms: 0,
                local_filled_atoms: 0,
                amend_resolved: true,
            },
        ),
        Some(QueryResolution::FetchTradesBeforeClose {
            order: k2.clone(),
            instrument_id: 12,
        })
    );
    assert_eq!(desk.ask_kind(&k2), Some(AskKind::Trades));

    // 3. Confirmed order returning NotFound increments issued_query_count and respects max_query_count > 0.
    let mut bounded_desk = QueryDesk::new(QueryConfig {
        max_query_count: 1,
        ..QueryConfig::default()
    });
    let k3 = OrderKey {
        intent: IntentKey {
            epoch: 4,
            intent_id: 203,
        },
        client_id: venue::text("confirmed-nf-budget"),
    };
    bounded_desk
        .enqueue_ask(k3.clone(), 12, None, AskKind::Cancel, false, 0, |_| true)
        .unwrap();
    let t_nf = bounded_desk.poll_due(6_000_000_000, |_, _| true).unwrap();
    assert_eq!(
        bounded_desk.on_not_found(&t_nf, 6_100_000_000, true),
        Some(QueryResolution::FreezeAccount {
            order: k3.clone(),
            issued_queries: 1,
        })
    );

    // 4. `Place` in flight + `Cancel` coalesced (`cancel_after = true`) + `on_not_sent` (`NotBuilt`)
    //    or pre-grace `on_not_found` -> must immediately promote `ask.kind` to `AskKind::Cancel`!
    let k4 = OrderKey {
        intent: IntentKey {
            epoch: 4,
            intent_id: 204,
        },
        client_id: venue::text("inflight-notsent-cancel"),
    };
    desk.enqueue_ask(k4.clone(), 12, None, AskKind::Place, true, 0, |_| true)
        .unwrap();
    let t_p4 = desk.poll_due(6_000_000_000, |_, _| true).unwrap();
    desk.enqueue_ask(
        k4.clone(),
        12,
        None,
        AskKind::Cancel,
        false,
        6_010_000_000,
        |_| true,
    )
    .unwrap();
    desk.on_not_sent(&t_p4, 6_020_000_000).unwrap();
    assert_eq!(
        desk.ask_kind(&k4),
        Some(AskKind::Cancel),
        "on_not_sent after mid-flight Cancel must promote ask.kind to AskKind::Cancel"
    );

    let k5 = OrderKey {
        intent: IntentKey {
            epoch: 4,
            intent_id: 205,
        },
        client_id: venue::text("inflight-pregrace-cancel"),
    };
    desk.enqueue_ask(
        k5.clone(),
        12,
        None,
        AskKind::Place,
        true,
        1_000_000_000,
        |_| true,
    )
    .unwrap();
    let t_p5 = desk
        .poll_due(7_000_000_000 - 500_000_000, |_, _| true)
        .unwrap();
    desk.enqueue_ask(
        k5.clone(),
        12,
        None,
        AskKind::Cancel,
        false,
        6_510_000_000,
        |_| true,
    )
    .unwrap();
    desk.on_not_found(&t_p5, 6_520_000_000, false).unwrap();
    assert_eq!(
        desk.ask_kind(&k5),
        Some(AskKind::Cancel),
        "pre-grace on_not_found after mid-flight Cancel must promote ask.kind to AskKind::Cancel"
    );
}
