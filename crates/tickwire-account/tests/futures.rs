mod common;
use common::*;
use tickwire_account::*;
use tickwire_venue as v;
#[test]
fn linear_capital_and_venue_shadow_are_distinct_from_cash_asset_reservations() {
    let (s, c, cmd) = futures_fixture();
    let mut a = Account::from_snapshot(s, c).unwrap();
    let admission = a.admit(&canonical(cmd.clone()), 60).unwrap();
    assert_eq!(
        admission.capital_holds(),
        &[CapitalHold {
            wallet_id: 1,
            settlement_asset: 2,
            amount: units(10)
        }]
    );
    assert_eq!(a.venue_shadow(1).unwrap(), units(990));
    a.apply_event(&event(fill(&cmd, "t1", "1", "100", "100")))
        .unwrap();
    assert_eq!(a.position(12, v::PositionSide::Both).unwrap().0, units(1));
    assert_eq!(a.venue_shadow(1).unwrap(), units(990));
}
#[test]
fn reduce_allocations_cannot_share_the_same_actual_position() {
    let (mut s, c, mut cmd) = futures_fixture();
    s.positions.push(PositionSnapshot {
        instrument_id: 12,
        side: v::PositionSide::Both,
        quantity: units(5),
        entry_bound: units(100),
        ownership: vec![(8, units(5))],
    });
    let v::CommandBody::New(ref mut o) = cmd.body else {
        unreachable!()
    };
    o.side = v::Side::Sell;
    o.reduce_only = true;
    o.quantity = v::Quantity::Base(venue::d(3, 0));
    let mut a = Account::from_snapshot(s, c).unwrap();
    let first = a.admit(&canonical(cmd.clone()), 60).unwrap();
    assert_eq!(
        a.order(&first.orders()[0]).unwrap().reduce_allocation,
        units(3)
    );
    cmd.context.request.intent_id += 1;
    let v::CommandBody::New(ref mut o) = cmd.body else {
        unreachable!()
    };
    o.client_id = venue::text("second");
    assert!(a.admit(&canonical(cmd), 60).is_err());
    assert_eq!(
        a.order(&first.orders()[0]).unwrap().reduce_allocation,
        units(3)
    );
}
#[test]
fn shared_wallet_gross_limit_includes_offsetting_strategy_positions() {
    let (mut s, mut c, cmd) = futures_fixture();
    s.positions.push(PositionSnapshot {
        instrument_id: 12,
        side: v::PositionSide::Both,
        quantity: units(2),
        entry_bound: units(100),
        ownership: vec![(8, units(5)), (9, units(-3))],
    });
    c.entries[0].max_gross_quantity = units(8);
    c.entries[0].strategy_limits.push((9, units(100)));
    let mut a = Account::from_snapshot(s, c).unwrap();
    assert_eq!(a.admit(&canonical(cmd), 60), Err(Error::Limit));
}
#[test]
fn fees_consume_the_original_shadow_fee_allowance_without_double_debit() {
    let (s, mut c, cmd) = futures_fixture();
    c.entries[0].fees.rates[0] = f(1_000_000);
    let mut a = Account::from_snapshot(s, c).unwrap();
    let admission = a.admit(&canonical(cmd.clone()), 60).unwrap();
    let before = a.venue_shadow(1).unwrap();
    assert_eq!(admission.capital_holds()[0].amount, f(1_100_000_010));
    a.apply_event(&event(fill(&cmd, "t1", "1", "100", "100")))
        .unwrap();
    a.apply_event(&event(fee(
        &cmd,
        "commission",
        cmd.context.instrument.settlement.clone(),
        "-1",
    )))
    .unwrap();
    assert_eq!(a.venue_shadow(1).unwrap(), before);
    assert!(!a.frozen());
    a.apply_event(&event(terminal(&cmd, "1"))).unwrap();
    a.reconcile_fees(&admission.orders()[0], units(1)).unwrap();
    assert_eq!(a.venue_shadow(1).unwrap(), before);
}
#[test]
fn hedge_long_and_short_reduce_tickets_are_separate_and_fill_with_actual_pnl() {
    let (mut s, mut c, mut cmd) = futures_fixture();
    let p = c.entries[0].futures.as_mut().unwrap();
    p.position_mode = v::PositionMode::Hedge;
    s.positions = vec![
        PositionSnapshot {
            instrument_id: 12,
            side: v::PositionSide::Long,
            quantity: units(3),
            entry_bound: units(100),
            ownership: vec![(8, units(3))],
        },
        PositionSnapshot {
            instrument_id: 12,
            side: v::PositionSide::Short,
            quantity: units(2),
            entry_bound: units(100),
            ownership: vec![(8, units(2))],
        },
    ];
    let v::CommandBody::New(ref mut o) = cmd.body else {
        unreachable!()
    };
    o.side = v::Side::Sell;
    o.position_side = v::PositionSide::Long;
    o.quantity = v::Quantity::Base(venue::d(3, 0));
    o.mode = v::ProductMode::Derivative {
        position_mode: v::PositionMode::Hedge,
        margin_mode: v::MarginMode::Cross,
    };
    let mut a = Account::from_snapshot(s, c).unwrap();
    let long = a.admit(&canonical(cmd.clone()), 60).unwrap();
    let longcmd = cmd.clone();
    cmd.context.request.intent_id += 1;
    let v::CommandBody::New(ref mut o) = cmd.body else {
        unreachable!()
    };
    o.side = v::Side::Buy;
    o.position_side = v::PositionSide::Short;
    o.quantity = v::Quantity::Base(venue::d(2, 0));
    o.client_id = venue::text("short-close");
    let short = a.admit(&canonical(cmd), 60).unwrap();
    let mut e = fill(&longcmd, "close", "3", "100", "300");
    let v::VenueEventBody::Fill(ref mut fill) = e.body else {
        unreachable!()
    };
    fill.realized_pnl = Some(v::AssetDelta {
        asset: longcmd.context.instrument.settlement.clone(),
        amount: venue::d(0, 0),
    });
    a.apply_event(&event(e)).unwrap();
    assert_eq!(
        a.order(&long.orders()[0]).unwrap().reduce_allocation,
        units(0)
    );
    assert_eq!(
        a.order(&short.orders()[0]).unwrap().reduce_allocation,
        units(2)
    );
    assert_eq!(a.position(12, v::PositionSide::Long).unwrap().0, units(0));
    assert!(!a.frozen());
}
#[test]
fn usdc_wallet_cannot_borrow_spare_usdt_collateral() {
    let (mut s, mut c, mut cmd) = futures_fixture();
    let mut usdc = cmd.context.instrument.quote.clone();
    usdc.id = 3;
    usdc.symbol = v::AssetSymbol::new("USDC").unwrap();
    s.assets.push(AssetSnapshot {
        key: AssetKey {
            wallet_id: 2,
            asset_id: 3,
        },
        asset: usdc.clone(),
        free: units(10),
        locked: units(0),
        nontrade_locked: units(0),
        exclusive_fee_pool: false,
        ownership: vec![],
    });
    s.venue_available.push((2, units(10)));
    let mut p = c.entries[0].clone();
    p.wallet_id = 2;
    p.instrument.instrument_id = 13;
    p.instrument.quote = usdc.clone();
    p.instrument.settlement = usdc;
    p.instrument.symbol = v::Symbol::new("BTCUSDC").unwrap();
    cmd.context.instrument = p.instrument.clone();
    c.entries.push(p);
    let v::CommandBody::New(ref mut o) = cmd.body else {
        unreachable!()
    };
    o.quantity = v::Quantity::Base(venue::d(2, 0));
    let mut a = Account::from_snapshot(s, c).unwrap();
    assert_eq!(
        a.admit(&canonical(cmd), 60),
        Err(Error::InsufficientCapital)
    );
    assert_eq!(a.venue_shadow(1).unwrap(), units(1000));
}
#[test]
fn delivery_policy_does_not_inherit_perpetual_funding_reserve() {
    let (s, mut c, mut cmd) = futures_fixture();
    cmd.context.request.product = v::Product::LinearFuture;
    cmd.context.instrument.contract = v::ContractKind::Delivery {
        expiry_utc_ns: 123456789,
    };
    c.entries[0].product = v::Product::LinearFuture;
    c.entries[0].instrument = cmd.context.instrument.clone();
    c.entries[0].futures.as_mut().unwrap().funding_settlements = 1;
    assert!(matches!(Account::from_snapshot(s, c), Err(Error::Catalog)));
}
#[test]
fn external_nonsettlement_debit_cannot_be_subtracted_as_settlement_atoms() {
    let (s, c, _) = futures_fixture();
    let mut a = Account::from_snapshot(s, c).unwrap();
    a.apply_unattributed_cashflow(
        &venue::text("external-btc"),
        AssetAmount {
            key: key(1),
            amount: f(-10_000_000),
        },
    )
    .unwrap();
    assert_eq!(a.asset(key(1)).unwrap().total, f(90_000_000));
    assert!(a.frozen());
    assert_eq!(a.venue_shadow(1), Err(Error::Unreconciled));
}
#[test]
fn fill_beyond_a_previous_terminal_cumulative_freezes_even_with_partial_observation() {
    let (mut s, c, mut cmd) = futures_fixture();
    s.positions.push(PositionSnapshot {
        instrument_id: 12,
        side: v::PositionSide::Both,
        quantity: units(1),
        entry_bound: units(100),
        ownership: vec![(8, units(1))],
    });
    let v::CommandBody::New(ref mut o) = cmd.body else {
        unreachable!()
    };
    o.side = v::Side::Sell;
    o.reduce_only = true;
    let mut a = Account::from_snapshot(s, c).unwrap();
    let first = a.admit(&canonical(cmd.clone()), 60).unwrap();
    a.apply_event(&event(terminal(&cmd, "0"))).unwrap();
    let mut second = cmd.clone();
    second.context.request.intent_id += 1;
    let v::CommandBody::New(ref mut o) = second.body else {
        unreachable!()
    };
    o.client_id = venue::text("second");
    let second = a.admit(&canonical(second), 60).unwrap();
    let mut late = fill(&cmd, "late", "1", "100", "100");
    let v::VenueEventBody::Fill(ref mut f) = late.body else {
        unreachable!()
    };
    f.realized_pnl = Some(v::AssetDelta {
        asset: cmd.context.instrument.settlement.clone(),
        amount: venue::d(0, 0),
    });
    a.apply_event(&event(late)).unwrap();
    assert_eq!(a.position(12, v::PositionSide::Both).unwrap().0, units(0));
    assert_eq!(
        a.order(&first.orders()[0]).unwrap().filled_quantity,
        units(1)
    );
    assert_eq!(
        a.order(&second.orders()[0]).unwrap().reduce_allocation,
        units(1)
    );
    assert!(a.frozen());
    assert_eq!(a.check_admission(&second, 60), Err(Error::Frozen));
}
#[test]
fn finite_model_instance_uses_four_capital_atoms_and_three_fee_atoms() {
    let (mut s, mut c, mut cmd) = futures_fixture();
    let n = |v| Fixed::from_atoms(v, 0).unwrap();
    for asset in &mut s.assets {
        asset.asset.exponent = 0;
        asset.free = n(16);
        asset.locked = n(0);
        asset.nontrade_locked = n(0);
    }
    cmd.context.instrument.base.exponent = 0;
    cmd.context.instrument.quote.exponent = 0;
    cmd.context.instrument.settlement.exponent = 0;
    cmd.context.instrument.price_tick = venue::d(1, 0);
    cmd.context.instrument.quantity_step = venue::d(1, 0);
    let v::CommandBody::New(ref mut o) = cmd.body else {
        unreachable!()
    };
    o.price = Some(venue::d(1, 0));
    o.side = v::Side::Sell;
    o.reduce_only = true;
    c.entries[0].instrument = cmd.context.instrument.clone();
    c.entries[0].max_absolute_position = n(100);
    c.entries[0].max_gross_quantity = n(100);
    c.entries[0].strategy_limits = vec![(8, n(100))];
    c.entries[0].fees.rates = [Fixed::from_atoms(5, 1).unwrap(), n(0), n(0)];
    c.entries[0].fees.maximum_fills = 2;
    let p = c.entries[0].futures.as_mut().unwrap();
    p.stress_low = n(1);
    p.stress_high = n(1);
    p.configured_leverage = n(1);
    p.venue_leverage = n(1);
    p.brackets = vec![tickwire_risk::futures::Bracket {
        floor: n(0),
        cap: n(1000),
        rate: n(0),
        cumulative: n(0),
        max_initial_leverage: n(1),
    }];
    p.maintenance_addon = n(0);
    p.pending_outbound = n(0);
    p.capital_buffer = n(0);
    s.venue_available = vec![(1, n(16))];
    s.positions = vec![PositionSnapshot {
        instrument_id: 12,
        side: v::PositionSide::Both,
        quantity: n(2),
        entry_bound: n(1),
        ownership: vec![(8, n(2))],
    }];
    let mut a = Account::from_snapshot(s, c).unwrap();
    let admission = a.admit(&canonical(cmd.clone()), 60).unwrap();
    assert_eq!(admission.capital_holds()[0].amount, n(4));
    assert_eq!(a.asset(key(2)).unwrap().reserved, n(3));
    assert_eq!(a.venue_shadow(1).unwrap(), n(12));
    a.apply_event(&event(fee(
        &cmd,
        "fee",
        cmd.context.instrument.settlement.clone(),
        "-1",
    )))
    .unwrap();
    assert_eq!(a.venue_shadow(1).unwrap(), n(12));
    let mut trade = fill(&cmd, "trade", "1", "1", "1");
    let v::VenueEventBody::Fill(ref mut f) = trade.body else {
        unreachable!()
    };
    f.realized_pnl = Some(v::AssetDelta {
        asset: cmd.context.instrument.settlement.clone(),
        amount: venue::d(0, 0),
    });
    a.apply_event(&event(trade)).unwrap();
    a.apply_event(&event(terminal(&cmd, "1"))).unwrap();
    a.reconcile_fees(&admission.orders()[0], n(1)).unwrap();
    assert_eq!(a.asset(key(2)).unwrap().total, n(15));
    assert_eq!(a.asset(key(2)).unwrap().reserved, n(0));
    assert_eq!(a.position(12, v::PositionSide::Both).unwrap().0, n(1));
    assert_eq!(a.venue_shadow(1).unwrap(), n(12));
    assert!(!a.frozen());
}
