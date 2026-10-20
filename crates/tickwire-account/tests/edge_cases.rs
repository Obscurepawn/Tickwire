mod common;
use common::*;
use tickwire_account::*;
use tickwire_venue as v;
#[test]
fn unproven_external_credit_is_booked_without_becoming_spendable() {
    let (mut a, _) = setup();
    let id = venue::text("deposit");
    a.apply_unattributed_cashflow(
        &id,
        AssetAmount {
            key: key(2),
            amount: units(500),
        },
    )
    .unwrap();
    let asset = a.asset(key(2)).unwrap();
    assert_eq!(asset.total, units(1500));
    assert_eq!(asset.available, units(1000));
    assert!(a.frozen());
    assert_eq!(
        a.apply_unattributed_cashflow(
            &id,
            AssetAmount {
                key: key(2),
                amount: units(500)
            }
        ),
        Ok(Applied::Duplicate)
    );
    assert_eq!(
        a.apply_unattributed_cashflow(
            &id,
            AssetAmount {
                key: key(2),
                amount: units(499)
            }
        ),
        Err(Error::Conflict)
    );
}
#[test]
fn arithmetic_overflow_retains_previous_reservation_and_the_unapplied_event() {
    let (mut s, c, cmd) = fixture();
    s.assets[0].free = f(i128::MAX);
    // Asset quantity limits also bound the imported exposure.
    let mut c = c;
    c.entries[0].max_absolute_position = f(i128::MAX);
    c.entries[0].max_gross_quantity = f(i128::MAX);
    let mut a = Account::from_snapshot(s, c).unwrap();
    // Selling permits an accepted order without making an overflowing future-buy envelope.
    let mut cmd = cmd;
    let v::CommandBody::New(ref mut o) = cmd.body else {
        unreachable!()
    };
    o.side = v::Side::Sell;
    // Gross would exceed MAX with any pending amount, so admission itself freezes atomically.
    assert_eq!(
        a.admit(&canonical(cmd), 60),
        Err(Error::Risk(tickwire_risk::RiskError::ArithmeticOverflow))
    );
    assert!(a.frozen());
    assert_eq!(a.asset(key(2)).unwrap().reserved, f(0));
}
#[test]
fn asset_unit_mismatch_is_rejected_without_partial_debits() {
    let (mut a, cmd) = setup();
    a.admit(&canonical(cmd.clone()), 60).unwrap();
    let mut asset = cmd.context.instrument.base.clone();
    asset.version += 1;
    assert_eq!(
        a.apply_event(&event(fee(&cmd, "wrong-unit", asset, "-0.1"))),
        Err(Error::Identity)
    );
    assert_eq!(a.asset(key(1)).unwrap().total, units(1));
    assert!(a.frozen());
}
#[test]
fn late_commission_after_reconciliation_reopens_risk_and_books_the_actual_debit() {
    let (mut a, cmd) = setup();
    let admission = a.admit(&canonical(cmd.clone()), 60).unwrap();
    a.apply_event(&event(terminal(&cmd, "0"))).unwrap();
    a.reconcile_fees(&admission.orders()[0], f(0)).unwrap();
    a.apply_event(&event(fee(
        &cmd,
        "late-fee",
        cmd.context.instrument.base.clone(),
        "-0.1",
    )))
    .unwrap();
    assert_eq!(a.asset(key(1)).unwrap().total, f(90_000_000));
    assert!(a.frozen());
    assert!(!a.order(&admission.orders()[0]).unwrap().fee_reconciled);
}
#[test]
fn external_position_change_preserves_allocated_reductions_and_attribution() {
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
    let admission = a.admit(&canonical(cmd), 60).unwrap();
    a.apply_external_position(
        &venue::text("liquidation"),
        12,
        v::PositionSide::Both,
        units(1),
        units(100),
    )
    .unwrap();
    assert_eq!(
        a.order(&admission.orders()[0]).unwrap().reduce_allocation,
        units(3)
    );
    assert!(a.frozen());
    let (actual, owners) = a.position(12, v::PositionSide::Both).unwrap();
    assert_eq!(actual, units(1));
    assert_eq!(owners[&0], units(-4));
    a.transfer_position_attribution(12, v::PositionSide::Both, 8, 9, units(2))
        .unwrap();
    let (same, owners) = a.position(12, v::PositionSide::Both).unwrap();
    assert_eq!(same, actual);
    assert_eq!(owners[&8], units(3));
    assert_eq!(owners[&9], units(2));
}
#[test]
fn unknown_locked_coverage_is_never_applied_to_a_new_order() {
    let (mut s, c, _) = fixture();
    s.assets[1].locked = units(50);
    assert!(matches!(
        Account::from_snapshot(s, c),
        Err(Error::InvalidSnapshot)
    ));
}
#[test]
fn spot_and_derivative_assets_cannot_be_shared_through_the_same_wallet_id() {
    let (s, mut c, _) = futures_fixture();
    let (_, spot, _) = fixture();
    let mut spot = spot.entries[0].clone();
    spot.instrument.instrument_id += 1;
    c.entries.push(spot);
    assert!(matches!(Account::from_snapshot(s, c), Err(Error::Catalog)));
}
#[test]
fn per_fill_rounding_tail_uses_explicit_fill_bound_not_lot_step() {
    let (s, mut c, cmd) = fixture();
    c.entries[0].fees.rates[0] = f(1);
    c.entries[0].fees.maximum_fills = 7;
    let mut a = Account::from_snapshot(s, c).unwrap();
    let admission = a.admit(&canonical(cmd), 60).unwrap();
    assert!(admission.resources().contains(&AssetAmount {
        key: key(1),
        amount: f(7)
    }));
}
