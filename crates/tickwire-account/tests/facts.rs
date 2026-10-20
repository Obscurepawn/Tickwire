mod common;
use common::*;
use tickwire_account::*;
#[test]
fn partial_fill_books_two_assets_and_original_strategy_once_across_connections() {
    let (mut a, cmd) = setup();
    let admission = a.admit(&canonical(cmd.clone()), 60).unwrap();
    let e = fill(&cmd, "t1", "0.0004", "50000", "20");
    a.apply_event(&event(e.clone())).unwrap();
    assert_eq!(a.asset(key(2)).unwrap().total, units(980));
    assert_eq!(a.asset(key(2)).unwrap().reserved, units(36));
    assert_eq!(a.asset(key(2)).unwrap().available, units(944));
    assert_eq!(a.asset(key(1)).unwrap().ownership[&8], f(40000));
    let mut replay = e;
    replay.identity.source_connection_epoch += 1;
    replay.identity.event_id = venue::text("different-source-id");
    assert_eq!(a.apply_event(&event(replay)), Ok(Applied::Duplicate));
    assert_eq!(
        a.order(&admission.orders()[0]).unwrap().filled_quantity,
        f(40000)
    );
}
#[test]
fn terminal_missing_fill_details_retains_principal_until_late_fill() {
    let (mut a, cmd) = setup();
    a.admit(&canonical(cmd.clone()), 60).unwrap();
    a.apply_event(&event(terminal(&cmd, "0.0004"))).unwrap();
    assert_eq!(a.asset(key(2)).unwrap().reserved, units(60));
    a.apply_event(&event(fill(&cmd, "late", "0.0004", "50000", "20")))
        .unwrap();
    assert_eq!(a.asset(key(2)).unwrap().reserved, units(0));
    assert_eq!(a.asset(key(2)).unwrap().available, units(980));
}
#[test]
fn duplicate_conflicting_trade_freezes_without_replacing_the_fact() {
    let (mut a, cmd) = setup();
    a.admit(&canonical(cmd.clone()), 60).unwrap();
    a.apply_event(&event(fill(&cmd, "t1", "0.0004", "50000", "20")))
        .unwrap();
    assert_eq!(
        a.apply_event(&event(fill(&cmd, "t1", "0.0005", "50000", "25"))),
        Err(Error::Conflict)
    );
    assert!(a.frozen());
    assert_eq!(a.asset(key(2)).unwrap().total, units(980));
    assert!(a.rejected_event().is_some());
}
#[test]
fn zero_fee_is_an_economic_fact_and_prevents_abort() {
    let (mut a, cmd) = setup();
    let admission = a.admit(&canonical(cmd.clone()), 60).unwrap();
    a.apply_event(&event(fee(
        &cmd,
        "fee0",
        cmd.context.instrument.base.clone(),
        "0",
    )))
    .unwrap();
    assert_eq!(a.apply_abort(&admission), Err(Error::Unreconciled));
    assert_eq!(a.asset(key(2)).unwrap().reserved, units(60));
}
#[test]
fn abort_is_idempotent_but_an_actual_late_fill_is_still_booked() {
    let (mut a, cmd) = setup();
    let admission = a.admit(&canonical(cmd.clone()), 60).unwrap();
    assert_eq!(a.apply_abort(&admission), Ok(Applied::New));
    assert_eq!(a.apply_abort(&admission), Ok(Applied::Duplicate));
    a.apply_event(&event(fill(&cmd, "late", "0.0004", "50000", "20")))
        .unwrap();
    assert_eq!(a.asset(key(2)).unwrap().total, units(980));
    assert!(a.frozen());
}
#[test]
fn fees_and_rebates_are_signed_original_asset_cashflow() {
    let (s, mut c, cmd) = fixture();
    c.entries[0].fees.rates[0] = f(1_000_000);
    let mut a = Account::from_snapshot(s, c).unwrap();
    let admission = a.admit(&canonical(cmd.clone()), 60).unwrap();
    a.apply_event(&event(fill(&cmd, "t1", "0.001", "60000", "60")))
        .unwrap();
    a.apply_event(&event(fee(
        &cmd,
        "f1",
        cmd.context.instrument.base.clone(),
        "-0.00001",
    )))
    .unwrap();
    a.apply_event(&event(fee(
        &cmd,
        "rebate",
        cmd.context.instrument.quote.clone(),
        "0.5",
    )))
    .unwrap();
    assert_eq!(a.asset(key(1)).unwrap().total, f(100_099_000));
    assert_eq!(a.asset(key(2)).unwrap().total, f(94_050_000_000));
    a.apply_event(&event(terminal(&cmd, "0.001"))).unwrap();
    assert!(a.asset(key(1)).unwrap().reserved.atoms() > 0);
    a.reconcile_fees(&admission.orders()[0], f(100_000))
        .unwrap();
    assert_eq!(a.asset(key(1)).unwrap().reserved, f(0));
}
#[test]
fn accepted_actual_quote_above_buy_budget_freezes_even_with_spare_funds() {
    let (mut a, cmd) = setup();
    a.admit(&canonical(cmd.clone()), 60).unwrap();
    a.apply_event(&event(fill(&cmd, "t1", "0.001", "60000", "65")))
        .unwrap();
    assert_eq!(a.asset(key(2)).unwrap().total, units(935));
    assert!(a.frozen());
}
#[test]
fn actual_fee_above_bound_freezes_even_when_balance_can_pay() {
    let (mut a, cmd) = setup();
    a.admit(&canonical(cmd.clone()), 60).unwrap();
    a.apply_event(&event(fee(
        &cmd,
        "unexpected",
        cmd.context.instrument.base.clone(),
        "-0.01",
    )))
    .unwrap();
    assert_eq!(a.asset(key(1)).unwrap().total, f(99_000_000));
    assert!(a.frozen());
}
#[test]
fn final_admission_check_rechecks_the_original_command_deadline() {
    let (mut a, cmd) = setup();
    let admission = a.admit(&canonical(cmd), 60).unwrap();
    assert_eq!(a.check_admission(&admission, 101), Err(Error::Stale));
}
#[test]
fn actual_fill_after_a_zero_fill_terminal_is_not_dropped_on_observation_conflict() {
    let (mut a, cmd) = setup();
    a.admit(&canonical(cmd.clone()), 60).unwrap();
    a.apply_event(&event(terminal(&cmd, "0"))).unwrap();
    let mut e = fill(&cmd, "late", "0.0004", "50000", "20");
    let tickwire_venue::VenueEventBody::Fill(ref mut f) = e.body else {
        unreachable!()
    };
    f.observation.status = tickwire_venue::OrderStatus::Canceled;
    f.observation.cumulative_quantity = Some("0.0004".parse().unwrap());
    a.apply_event(&event(e)).unwrap();
    assert!(a.frozen());
    assert_eq!(a.asset(key(2)).unwrap().total, units(980));
}
#[test]
fn exchange_acknowledgment_blocks_a_later_no_bytes_abort() {
    let (mut a, cmd) = setup();
    let admission = a.admit(&canonical(cmd.clone()), 60).unwrap();
    let mut ack = terminal(&cmd, "0");
    let tickwire_venue::VenueEventBody::Order(ref mut o) = ack.body else {
        unreachable!()
    };
    o.status = tickwire_venue::OrderStatus::New;
    a.apply_event(&event(ack)).unwrap();
    assert_eq!(a.apply_abort(&admission), Err(Error::Unreconciled));
    assert!(a.frozen());
    assert_eq!(a.asset(key(2)).unwrap().reserved, units(60));
}
#[test]
fn acknowledgment_after_abort_reopens_the_order_and_freezes() {
    let (mut a, cmd) = setup();
    let admission = a.admit(&canonical(cmd.clone()), 60).unwrap();
    a.apply_abort(&admission).unwrap();
    let mut ack = terminal(&cmd, "0");
    let tickwire_venue::VenueEventBody::Order(ref mut o) = ack.body else {
        unreachable!()
    };
    o.status = tickwire_venue::OrderStatus::New;
    a.apply_event(&event(ack)).unwrap();
    assert!(a.frozen());
    assert!(!a.order(&admission.orders()[0]).unwrap().aborted);
    assert_eq!(a.asset(key(2)).unwrap().reserved, units(60));
}
#[test]
fn repeated_trade_still_applies_a_new_unknown_observation_without_double_cashflow() {
    let (mut a, cmd) = setup();
    a.admit(&canonical(cmd.clone()), 60).unwrap();
    let mut trade = fill(&cmd, "same", "0.0004", "50000", "20");
    a.apply_event(&event(trade.clone())).unwrap();
    let tickwire_venue::VenueEventBody::Fill(ref mut f) = trade.body else {
        unreachable!()
    };
    f.observation.status = tickwire_venue::OrderStatus::Unknown;
    assert_eq!(a.apply_event(&event(trade)), Ok(Applied::New));
    assert!(a.frozen());
    assert_eq!(a.asset(key(2)).unwrap().total, units(980));
}
#[test]
fn repeated_trade_still_applies_a_new_terminal_observation_without_double_cashflow() {
    let (mut a, cmd) = setup();
    a.admit(&canonical(cmd.clone()), 60).unwrap();
    let mut trade = fill(&cmd, "same", "0.0004", "50000", "20");
    a.apply_event(&event(trade.clone())).unwrap();
    let tickwire_venue::VenueEventBody::Fill(ref mut f) = trade.body else {
        unreachable!()
    };
    f.observation.status = tickwire_venue::OrderStatus::Canceled;
    f.observation.cumulative_quantity = Some("0.0004".parse().unwrap());
    assert_eq!(a.apply_event(&event(trade)), Ok(Applied::New));
    assert_eq!(a.asset(key(2)).unwrap().reserved, units(0));
    assert_eq!(a.asset(key(2)).unwrap().total, units(980));
}
#[test]
fn new_fill_cannot_silently_exceed_a_previously_closed_cumulative() {
    let (mut a, cmd) = setup();
    a.admit(&canonical(cmd.clone()), 60).unwrap();
    a.apply_event(&event(terminal(&cmd, "0"))).unwrap();
    a.apply_event(&event(fill(&cmd, "late-partial", "0.0004", "50000", "20")))
        .unwrap();
    assert_eq!(a.asset(key(2)).unwrap().total, units(980));
    assert!(a.frozen());
}
