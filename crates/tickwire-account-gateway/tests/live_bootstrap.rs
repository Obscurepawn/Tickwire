mod common;

use common::*;
use std::time::Instant;
use tickwire_account::{LiveOpenOrder, ManagerIdentity, OrderKey};
use tickwire_account_gateway::*;
use tickwire_account_journal::InMemoryBackend;
use tickwire_transport::IoBudget;
use tickwire_venue::{self as v, ClientIdDomain, SelfDescribingClientId};

#[test]
fn in_memory_gateway_bootstraps_from_live_exchange_and_manages_imported_order() {
    let (mut snapshot, catalog, mut cmd) = fixture::fixture();
    let domain = ClientIdDomain {
        risk_domain_id: snapshot.scope.risk_domain_id,
        venue: snapshot.scope.venue,
        environment: snapshot.scope.environment,
        account: snapshot.scope.account,
        product: v::Product::Spot,
    };
    let sd = SelfDescribingClientId {
        logical_strategy_id: 8,
        leg: 0,
        epoch: 3,
        intent_id: 11,
    };
    let cid = sd.encode(&domain).unwrap();
    let det_id = sd.deterministic_order_id(&domain);

    // Remote exchange shows 880 free + 120 locked for this live open Buy order (0.002 @ 60_000).
    snapshot.assets[1].free = fixture::units(880);
    snapshot.assets[1].locked = fixture::units(120);

    let v::CommandBody::New(ref mut spec) = cmd.body else {
        unreachable!()
    };
    spec.client_id = cid.clone();
    spec.side = v::Side::Buy;
    spec.quantity = v::Quantity::Base("0.002".parse().unwrap());
    spec.price = Some("60000".parse().unwrap());

    let manager = ManagerIdentity {
        instance: cmd.context.strategy.instance_id,
        generation: cmd.context.request.strategy_generation,
    };
    let venue_id = v::VenueOrderId::new("55501").unwrap();
    let live = VerifiedLiveBootstrap::new(
        snapshot,
        catalog,
        &[LiveOpenOrder {
            instrument_id: cmd.context.instrument.instrument_id,
            spec: spec.clone(),
            venue_order_id: Some(venue_id.clone()),
            filled_quantity: fixture::f(0),
            manager,
            command_digest: [0x44; 32],
        }],
        true,
    )
    .unwrap();
    assert_eq!(live.imported_orders().len(), 1);
    assert_eq!(live.external_open_orders(), 0);
    assert_eq!(live.account().max_imported_epoch(), Some(3));

    let cfg = config(live.baseline());
    let (connection, peer) = peer(&cfg.route.authority, 16384);
    let auth = MockAuthority::default();

    // Pure volatile InMemoryBackend — zero temporary directories or disk files!
    let mut g = AccountGateway::create_from_live_exchange(
        InMemoryBackend::new(),
        cfg,
        live,
        auth,
        TestEncoder,
        connection,
    )
    .unwrap();

    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(120)
    );
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().available,
        fixture::units(880)
    );

    // Cancel the imported epoch-3 order from writer_epoch 4 over TLS loopback.
    let mut cancel = cmd.clone();
    cancel.context.request.intent_exec_epoch = 4;
    cancel.context.request.intent_id = 3;
    cancel.body = v::CommandBody::Cancel(v::OrderTarget {
        order_id: det_id,
        expected_owner_revision: 1,
        original_intent_exec_epoch: 3,
        client_id: Some(cid.clone()),
        venue_order_id: Some(venue_id.clone()),
    });
    assert_eq!(
        g.submit(fixture::canonical(cancel), 60, until()).unwrap(),
        Status::Queued
    );
    while g.status(intent()) != Some(Status::Flushed) {
        g.poll(Instant::now(), IoBudget::default()).unwrap();
    }

    // Apply venue Canceled event on the imported order via record_ingress_event.
    let mut cancel_obs = cmd;
    cancel_obs.context.request.intent_exec_epoch = 3;
    cancel_obs.context.request.intent_id = 11;
    let mut term = fixture::terminal(&cancel_obs, "0");
    if let v::VenueEventBody::Order(ref mut obs) = term.body {
        obs.order.order_id = det_id;
        obs.order.venue_order_id = Some(venue_id);
    }
    g.record_ingress_event([7; 16], 1, fixture::event(term))
        .unwrap();

    let order_key = OrderKey {
        intent: tickwire_account::IntentKey {
            epoch: 3,
            intent_id: 11,
        },
        client_id: cid,
    };
    assert!(g.account().order(&order_key).unwrap().terminal);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().available,
        fixture::units(1000)
    );

    drop(g);
    assert!(!peer.join().unwrap().is_empty());
}
