mod common;

use common::*;
use tickwire_account::*;
use tickwire_venue::{self as v, ClientIdDomain, SelfDescribingClientId};

fn mint_client_id(
    scope: &Scope,
    product: v::Product,
    strategy: u16,
    leg: u8,
    epoch: u32,
    intent_id: u64,
) -> (v::ClientId, [u8; 16]) {
    let domain = ClientIdDomain {
        risk_domain_id: scope.risk_domain_id,
        venue: scope.venue,
        environment: scope.environment,
        account: scope.account,
        product,
    };
    let sd = SelfDescribingClientId {
        logical_strategy_id: strategy,
        leg,
        epoch,
        intent_id,
    };
    let cid = sd.encode(&domain).unwrap();
    (cid, sd.deterministic_order_id(&domain))
}

#[test]
fn spot_live_bootstrap_normalizes_locks_enforces_floor_and_releases_on_cancel() {
    let (mut snapshot, catalog, mut cmd) = fixture();
    let (cid, det_order_id) = mint_client_id(&snapshot.scope, v::Product::Spot, 8, 0, 1, 5);

    // Remote exchange reports:
    // - 750 quote free
    // - 250 quote locked = 120 managed Buy (0.002 @ 60_000) + 130 external/manual order lock
    snapshot.assets[1].free = units(750);
    snapshot.assets[1].locked = units(250);
    snapshot.assets[1].nontrade_locked = f(0);

    let v::CommandBody::New(ref mut spec) = cmd.body else {
        unreachable!()
    };
    spec.client_id = cid.clone();
    spec.side = v::Side::Buy;
    spec.quantity = v::Quantity::Base("0.002".parse().unwrap());
    spec.price = Some("60000".parse().unwrap());

    let mut external_spec = spec.clone();
    external_spec.client_id = v::ClientId::new("manual_web_order_01").unwrap();

    let manager = ManagerIdentity {
        instance: cmd.context.strategy.instance_id,
        generation: cmd.context.request.strategy_generation,
    };
    let venue_id = v::VenueOrderId::new("90001").unwrap();
    let open_orders = vec![
        LiveOpenOrder {
            instrument_id: cmd.context.instrument.instrument_id,
            spec: spec.clone(),
            venue_order_id: Some(venue_id.clone()),
            filled_quantity: f(0),
            manager,
            command_digest: [0xAB; 32],
        },
        LiveOpenOrder {
            instrument_id: cmd.context.instrument.instrument_id,
            spec: external_spec,
            venue_order_id: Some(v::VenueOrderId::new("90002").unwrap()),
            filled_quantity: f(0),
            manager,
            command_digest: [0; 32],
        },
    ];

    let boot = Account::from_live_exchange(snapshot, catalog.clone(), &open_orders, false).unwrap();
    assert_eq!(boot.external_open_orders, 1);
    assert_eq!(boot.imported_orders.len(), 1);
    assert!(!boot.account.frozen());

    // Normalized snapshot satisfies locked == nontrade_locked == 130 so CanonicalBaseline::new succeeds.
    let flat_from_normalized =
        Account::from_snapshot(boot.normalized_snapshot.clone(), catalog).unwrap();
    assert!(!flat_from_normalized.frozen());
    assert_eq!(boot.normalized_snapshot.assets[1].free, units(870));
    assert_eq!(boot.normalized_snapshot.assets[1].locked, units(130));
    assert_eq!(
        boot.normalized_snapshot.assets[1].nontrade_locked,
        units(130)
    );

    let mut account = boot.account;
    let quote_view = account.asset(key(2)).unwrap();
    assert_eq!(quote_view.total, units(1000));
    assert_eq!(quote_view.reserved, units(120));
    assert_eq!(quote_view.available, units(750));

    let imported_key = OrderKey {
        intent: IntentKey {
            epoch: 1,
            intent_id: 5,
        },
        client_id: cid.clone(),
    };
    let view = account.order(&imported_key).unwrap();
    assert_eq!(view.strategy, 8);
    assert!(view.confirmed);
    assert_eq!(view.remaining_quantity, f(200_000));
    assert_eq!(account.venue_order_id(&imported_key), Some(&venue_id));
    assert_eq!(
        account.strategy_sequence_floor(8),
        Some(IntentKey {
            epoch: 1,
            intent_id: 5
        })
    );

    // Sequence floor rejects stale intent_id <= 5 in epoch 1, admits intent_id == 6.
    let mut stale_cmd = cmd.clone();
    stale_cmd.context.request.intent_exec_epoch = 1;
    stale_cmd.context.request.intent_id = 5;
    if let v::CommandBody::New(ref mut s) = stale_cmd.body {
        s.client_id = mint_client_id(&account.scope().clone(), v::Product::Spot, 8, 0, 1, 5).0;
    }
    assert_eq!(account.admit(&canonical(stale_cmd), 60), Err(Error::Stale));

    let mut fresh_same_epoch = cmd.clone();
    fresh_same_epoch.context.request.intent_exec_epoch = 1;
    fresh_same_epoch.context.request.intent_id = 6;
    if let v::CommandBody::New(ref mut s) = fresh_same_epoch.body {
        s.client_id = mint_client_id(&account.scope().clone(), v::Product::Spot, 8, 0, 1, 6).0;
        s.quantity = v::Quantity::Base("0.001".parse().unwrap());
        s.price = Some("50000".parse().unwrap());
    }
    let fresh_adm = account.admit(&canonical(fresh_same_epoch), 60).unwrap();
    account.apply_abort(&fresh_adm).unwrap();

    // Strategy can cancel the imported order using its deterministic_order_id, and canceling unlocks +120 quote!
    let mut cancel_cmd = cmd.clone();
    cancel_cmd.context.request.intent_exec_epoch = 2;
    cancel_cmd.context.request.intent_id = 1;
    cancel_cmd.body = v::CommandBody::Cancel(v::OrderTarget {
        order_id: det_order_id,
        expected_owner_revision: 1,
        original_intent_exec_epoch: 1,
        client_id: Some(cid.clone()),
        venue_order_id: Some(venue_id.clone()),
    });
    account.admit(&canonical(cancel_cmd), 60).unwrap();

    let mut cancel_obs = cmd.clone();
    cancel_obs.context.request.intent_exec_epoch = 1;
    cancel_obs.context.request.intent_id = 5;
    let mut term = terminal(&cancel_obs, "0");
    if let v::VenueEventBody::Order(ref mut obs) = term.body {
        obs.order.order_id = det_order_id;
        obs.order.venue_order_id = Some(venue_id);
    }
    account.apply_event(&event(term)).unwrap();
    account.reconcile_fees(&imported_key, f(0)).unwrap();

    let after_cancel = account.asset(key(2)).unwrap();
    assert_eq!(after_cancel.reserved, units(0));
    assert_eq!(after_cancel.available, units(870));
}

#[test]
fn spot_live_bootstrap_partial_fill_books_only_post_bootstrap_delta() {
    let (mut snapshot, catalog, mut cmd) = fixture();
    let (cid, det_order_id) = mint_client_id(&snapshot.scope, v::Product::Spot, 8, 0, 1, 9);

    // Order originally 0.003 BTC @ 50_000 USDT; 0.002 BTC already filled before bootstrap!
    // Remaining 0.001 BTC locks 50 USDT on the exchange.
    snapshot.assets[0].free = f(100_200_000); // 1.002 BTC already settled
    snapshot.assets[1].free = units(850);
    snapshot.assets[1].locked = units(50);

    let v::CommandBody::New(ref mut spec) = cmd.body else {
        unreachable!()
    };
    spec.client_id = cid.clone();
    spec.side = v::Side::Buy;
    spec.quantity = v::Quantity::Base("0.003".parse().unwrap());
    spec.price = Some("50000".parse().unwrap());

    let manager = ManagerIdentity {
        instance: cmd.context.strategy.instance_id,
        generation: cmd.context.request.strategy_generation,
    };
    let boot = Account::from_live_exchange(
        snapshot,
        catalog,
        &[LiveOpenOrder {
            instrument_id: cmd.context.instrument.instrument_id,
            spec: spec.clone(),
            venue_order_id: Some(v::VenueOrderId::new("777").unwrap()),
            filled_quantity: f(200_000), // 0.002 BTC pre-filled
            manager,
            command_digest: [0x11; 32],
        }],
        true,
    )
    .unwrap();
    let mut account = boot.account;
    assert!(!account.frozen());

    // Post-bootstrap fill of remaining 0.001 BTC with cumulative_quantity = 0.003 BTC
    let mut ref_cmd = cmd.clone();
    ref_cmd.context.request.intent_exec_epoch = 1;
    ref_cmd.context.request.intent_id = 9;
    let mut fill_ev = fill(&ref_cmd, "trade-final", "0.001", "50000", "50");
    if let v::VenueEventBody::Fill(ref mut fe) = fill_ev.body {
        fe.observation.order.order_id = det_order_id;
        fe.observation.order.venue_order_id = Some(v::VenueOrderId::new("777").unwrap());
        fe.observation.status = v::OrderStatus::Filled;
        fe.observation.cumulative_quantity = Some("0.003".parse().unwrap());
    }
    account.apply_event(&event(fill_ev)).unwrap();
    assert!(!account.frozen());

    let order_key = OrderKey {
        intent: IntentKey {
            epoch: 1,
            intent_id: 9,
        },
        client_id: cid,
    };
    account.reconcile_fees(&order_key, f(300_000)).unwrap();

    // Base rose by ONLY +0.001 BTC (to 1.003 BTC), quote total dropped by ONLY -50 USDT (to 850 free)!
    assert_eq!(account.asset(key(1)).unwrap().total, f(100_300_000));
    assert_eq!(account.asset(key(2)).unwrap().total, units(850));
    assert_eq!(account.asset(key(2)).unwrap().available, units(850));
}

#[test]
fn futures_live_bootstrap_reconstructs_shared_pool_position_and_open_order_margin_baseline() {
    let (mut snapshot, catalog, cmd) = futures_fixture();
    // Shared-pool settled long position of 2 units in strategy 0 (empty ownership -> strategy 0).
    snapshot.positions = vec![PositionSnapshot {
        instrument_id: cmd.context.instrument.instrument_id,
        side: v::PositionSide::Both,
        quantity: units(2),
        entry_bound: units(100),
        ownership: vec![],
    }];
    // Venue availableBalance already reflects initial margin of settled position + open orders.
    snapshot.venue_available = vec![(1, units(500))];

    let (open_cid, _) = mint_client_id(&snapshot.scope, v::Product::LinearPerpetual, 8, 0, 1, 3);
    let (reduce_cid, _) = mint_client_id(&snapshot.scope, v::Product::LinearPerpetual, 8, 0, 1, 4);

    let v::CommandBody::New(ref base_spec) = cmd.body else {
        unreachable!()
    };
    let mut buy_spec = base_spec.clone();
    buy_spec.client_id = open_cid;
    buy_spec.side = v::Side::Buy;
    buy_spec.quantity = v::Quantity::Base(venue::d(1, 0));

    let mut reduce_spec = base_spec.clone();
    reduce_spec.client_id = reduce_cid.clone();
    reduce_spec.side = v::Side::Sell;
    reduce_spec.reduce_only = true;
    reduce_spec.quantity = v::Quantity::Base(venue::d(1, 0));

    let manager = ManagerIdentity {
        instance: cmd.context.strategy.instance_id,
        generation: cmd.context.request.strategy_generation,
    };
    let boot = Account::from_live_exchange(
        snapshot,
        catalog,
        &[
            LiveOpenOrder {
                instrument_id: cmd.context.instrument.instrument_id,
                spec: buy_spec,
                venue_order_id: Some(v::VenueOrderId::new("f-1").unwrap()),
                filled_quantity: f(0),
                manager,
                command_digest: [0x21; 32],
            },
            LiveOpenOrder {
                instrument_id: cmd.context.instrument.instrument_id,
                spec: reduce_spec,
                venue_order_id: Some(v::VenueOrderId::new("f-2").unwrap()),
                filled_quantity: f(0),
                manager,
                command_digest: [0x22; 32],
            },
        ],
        true,
    )
    .unwrap();

    let mut account = boot.account;
    assert!(!account.frozen());
    let reduce_key = OrderKey {
        intent: IntentKey {
            epoch: 1,
            intent_id: 4,
        },
        client_id: reduce_cid,
    };
    assert_eq!(
        account.order(&reduce_key).unwrap().reduce_allocation,
        units(1)
    );

    // Subsequent order charges ONLY incremental margin delta (1 unit @ 100 / 10x = 10), not prior orders!
    let mut next_cmd = cmd.clone();
    next_cmd.context.request.intent_exec_epoch = 2;
    next_cmd.context.request.intent_id = 1;
    if let v::CommandBody::New(ref mut s) = next_cmd.body {
        s.client_id = mint_client_id(
            &account.scope().clone(),
            v::Product::LinearPerpetual,
            8,
            0,
            2,
            1,
        )
        .0;
    }
    let admission = account.admit(&canonical(next_cmd), 60).unwrap();
    assert_eq!(admission.capital_holds().len(), 1);
    assert_eq!(admission.capital_holds()[0].amount, units(10));
}
