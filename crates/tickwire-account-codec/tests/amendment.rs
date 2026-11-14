mod common;
use common::*;
use tickwire_account::*;
use tickwire_account_codec::*;
use tickwire_venue as v;

#[test]
fn durable_amend_admit_and_result_roundtrip_replay_the_same_quantity_and_responsibility() {
    let (snapshot, catalog, command) = fixture();
    let baseline = CanonicalBaseline::new(snapshot.clone(), catalog.clone()).unwrap();
    let mut a = Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
    let original = canonical(command.clone());
    let create = a.admit(&original, 60).unwrap();
    let mut ack = venue::event();
    let v::VenueEventBody::Fill(fill) = ack.body else {
        unreachable!()
    };
    let mut o = fill.observation;
    o.status = v::OrderStatus::New;
    o.cumulative_quantity = Some(venue::d(0, 0));
    ack.body = v::VenueEventBody::Order(Box::new(o.clone()));
    let ack = event(ack);
    a.apply_event(&ack).unwrap();
    let mut c = command.clone();
    c.context.request.intent_id = 4;
    c.context.request.recovery_object = 8;
    c.context.strategy.risk_policy_version = 9;
    c.body = v::CommandBody::AmendKeepPriority {
        target: v::OrderTarget {
            order_id: o.order.order_id,
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: o.order.client_id.clone(),
            venue_order_id: o.order.venue_order_id.clone(),
        },
        new_quantity: venue::d(8, -4),
        new_client_id: o.order.client_id.clone(),
    };
    let c = canonical(c);
    let amend = a.admit(&c, 61).unwrap();
    o.original_quantity = Some(venue::d(8, -4));
    let mut result = venue::event();
    result.body = v::VenueEventBody::Amendment(Box::new(v::AmendmentObservation {
        order: o.order.clone(),
        request_intent_id: 4,
        request_intent_exec_epoch: 4,
        request_digest: c.digest(),
        execution_source: v::AmendmentSource::RestResponse,
        execution_id: 75,
        previous_quantity: venue::d(1, -3),
        new_quantity: venue::d(8, -4),
        observation: Some(o),
    }));
    let result = event(result);
    a.apply_event(&result).unwrap();
    let records = vec![
        RecoveryRecord::Admit {
            command: Box::new(original),
            evaluated_at: 60,
            expected: create.image(),
        },
        RecoveryRecord::VenueEvent(Box::new(ack)),
        RecoveryRecord::Admit {
            command: Box::new(c),
            evaluated_at: 61,
            expected: amend.image(),
        },
        RecoveryRecord::VenueEvent(Box::new(result)),
    ];
    let decoded: Vec<_> = records
        .into_iter()
        .enumerate()
        .map(|(i, r)| {
            let r = CanonicalRecord::new(baseline.digest(), i as u64 + 1, r).unwrap();
            let decoded = CanonicalRecord::decode(r.bytes()).unwrap();
            assert_eq!(decoded, r);
            decoded.record().clone()
        })
        .collect();
    let b = Account::recover(snapshot, catalog, &decoded).unwrap();
    assert_eq!(a.asset(key(2)), b.asset(key(2)));
    assert_eq!(
        b.order(&create.orders()[0]).unwrap().original_quantity,
        f(100_000)
    );
    assert_eq!(
        b.order(&create.orders()[0]).unwrap().effective_quantity,
        f(80_000)
    );
    assert_eq!(a.ledger_sequence(), b.ledger_sequence());
    assert_eq!(
        a.amendments().collect::<Vec<_>>(),
        b.amendments().collect::<Vec<_>>()
    );
}

#[test]
fn durable_modify_admit_and_events_roundtrip_canonical_record_and_replay() {
    let (snapshot, catalog, command) = fixture();
    let baseline = CanonicalBaseline::new(snapshot.clone(), catalog.clone()).unwrap();
    let mut a = Account::from_snapshot(snapshot.clone(), catalog.clone()).unwrap();
    let original = canonical(command.clone());
    let create = a.admit(&original, 60).unwrap();

    let mut ack = venue::event();
    let v::VenueEventBody::Fill(fill) = ack.body else {
        unreachable!()
    };
    let mut o = fill.observation;
    o.status = v::OrderStatus::New;
    o.cumulative_quantity = Some(venue::d(0, 0));
    ack.body = v::VenueEventBody::Order(Box::new(o.clone()));
    let ack = event(ack);
    a.apply_event(&ack).unwrap();

    let target = v::OrderTarget {
        order_id: o.order.order_id,
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: o.order.client_id.clone(),
        venue_order_id: o.order.venue_order_id.clone(),
    };
    let mut m_cmd = command.clone();
    m_cmd.context.request.intent_id = 5;
    m_cmd.context.request.recovery_object = 9;
    m_cmd.body = v::CommandBody::Modify(v::ModifyOrder {
        target: target.clone(),
        new_client_id: Some(venue::text("test-1-mod")),
        side: v::Side::Buy,
        quantity: venue::d(15, -4),
        price: venue::d(61000, 0),
        position_side: v::PositionSide::Both,
        reduce_only: false,
    });
    let m_cmd = canonical(m_cmd);
    let mod_adm = a.admit(&m_cmd, 61).unwrap();

    let mut mod_ev = venue::event();
    mod_ev.body = v::VenueEventBody::Modified(Box::new(v::ModifyObservation {
        target,
        amendment_id: Some(venue::text("amd-1")),
        intent: Some(v::IntentKey {
            epoch: 4,
            intent_id: 5,
        }),
        prior_client_id: o.order.client_id,
        new_client_id: Some(venue::text("test-1-mod")),
        venue_order_id: o.order.venue_order_id,
        new_quantity: venue::d(14, -4),
        new_price: venue::d(60500, 0),
        cumulative_filled: Some(venue::d(0, 0)),
    }));
    let mod_ev = event(mod_ev);
    a.apply_event(&mod_ev).unwrap();

    let records = vec![
        RecoveryRecord::Admit {
            command: Box::new(original),
            evaluated_at: 60,
            expected: create.image(),
        },
        RecoveryRecord::VenueEvent(Box::new(ack)),
        RecoveryRecord::Admit {
            command: Box::new(m_cmd),
            evaluated_at: 61,
            expected: mod_adm.image(),
        },
        RecoveryRecord::VenueEvent(Box::new(mod_ev)),
    ];
    let decoded: Vec<_> = records
        .into_iter()
        .enumerate()
        .map(|(i, r)| {
            let r = CanonicalRecord::new(baseline.digest(), i as u64 + 1, r).unwrap();
            let decoded = CanonicalRecord::decode(r.bytes()).unwrap();
            assert_eq!(decoded, r);
            decoded.record().clone()
        })
        .collect();
    let b = Account::recover(snapshot, catalog, &decoded).unwrap();
    assert_eq!(a.asset(key(2)), b.asset(key(2)));
    assert_eq!(
        b.order(&create.orders()[0]).unwrap().effective_quantity,
        f(140_000)
    );
    assert_eq!(b.stp_book().best_bid_ticks(12).unwrap(), Some(6_050_000));
}
