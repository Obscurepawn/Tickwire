mod common;
use common::*;
use tickwire_account::*;
use tickwire_venue as v;
#[test]
fn full_spot_command_reserves_only_the_spent_asset() {
    let (mut account, cmd) = setup();
    let before = account.asset(key(2)).unwrap().available;
    let admission = account.admit(&canonical(cmd), 60).unwrap();
    assert_eq!(
        admission.resources(),
        &[AssetAmount {
            key: key(2),
            amount: units(60)
        }]
    );
    assert_eq!(
        account.asset(key(2)).unwrap().available,
        before.checked_sub(units(60)).unwrap()
    );
    assert_eq!(account.asset(key(1)).unwrap().available, units(1));
}
#[test]
fn exact_retry_is_idempotent_and_changed_price_conflicts() {
    let (mut account, cmd) = setup();
    let a = account.admit(&canonical(cmd.clone()), 60).unwrap();
    assert_eq!(account.admit(&canonical(cmd.clone()), 60).unwrap(), a);
    let mut changed = cmd;
    if let v::CommandBody::New(ref mut o) = changed.body {
        o.price = Some(venue::d(61000, 0));
    }
    assert_eq!(account.admit(&canonical(changed), 60), Err(Error::Conflict));
    assert!(account.frozen());
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(60));
}
#[test]
fn native_market_cannot_turn_a_last_price_into_a_hard_bound() {
    let (mut account, mut cmd) = setup();
    if let v::CommandBody::New(ref mut o) = cmd.body {
        o.order_type = v::OrderType::Market;
        o.price = None;
        o.time_in_force = None;
    }
    assert_eq!(
        account.admit(&canonical(cmd), 60),
        Err(Error::UnboundedExecution)
    );
}
#[test]
fn stale_inputs_fail_before_reserving() {
    let (mut account, cmd) = setup();
    assert_eq!(account.admit(&canonical(cmd), 101), Err(Error::Stale));
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(0));
}
#[test]
fn exclusive_fee_pool_cannot_also_fund_a_sell() {
    let (mut s, mut c, mut cmd) = fixture();
    s.assets[0].exclusive_fee_pool = true;
    c.entries[0].fees.exclusive_pool = Some(key(1));
    let v::CommandBody::New(ref mut o) = cmd.body else {
        unreachable!()
    };
    o.side = v::Side::Sell;
    let mut a = Account::from_snapshot(s, c).unwrap();
    assert_eq!(a.admit(&canonical(cmd), 60), Err(Error::InsufficientAsset));
}
#[test]
fn unknown_order_keeps_all_reservations_and_allows_a_targeted_cancel() {
    let (mut a, cmd) = setup();
    let first = a.admit(&canonical(cmd.clone()), 60).unwrap();
    let mut e = terminal(&cmd, "0");
    let v::VenueEventBody::Order(ref mut o) = e.body else {
        unreachable!()
    };
    o.status = v::OrderStatus::Unknown;
    a.apply_event(&event(e)).unwrap();
    assert_eq!(a.asset(key(2)).unwrap().reserved, units(60));
    assert!(a.check_admission(&first, 60).is_err());
    let mut cancel = cmd;
    cancel.context.request.intent_id += 1;
    cancel.body = v::CommandBody::Cancel(v::OrderTarget {
        order_id: [9; 16],
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(venue::text("test-1")),
        venue_order_id: None,
    });
    let admission = a.admit(&canonical(cancel), 60).unwrap();
    assert!(admission.resources().is_empty());
    assert_eq!(a.check_admission(&admission, 60), Ok(()));
}
#[test]
fn deficit_freezes_new_orders_but_not_cancel_of_an_existing_order() {
    let (mut a, cmd) = setup();
    a.admit(&canonical(cmd.clone()), 60).unwrap();
    a.apply_event(&event(fee(
        &cmd,
        "debit",
        cmd.context.instrument.quote.clone(),
        "-2000",
    )))
    .unwrap();
    let mut cancel = cmd;
    cancel.context.request.intent_id += 1;
    cancel.body = v::CommandBody::Cancel(v::OrderTarget {
        order_id: [9; 16],
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(venue::text("test-1")),
        venue_order_id: None,
    });
    let admission = a.admit(&canonical(cancel), 60).unwrap();
    assert_eq!(a.check_admission(&admission, 60), Ok(()));
}
#[test]
fn cancel_replace_reserves_old_and_new_together_without_optimistic_netting() {
    let (mut a, cmd) = setup();
    let first = a.admit(&canonical(cmd.clone()), 60).unwrap();
    let mut replacement = cmd.clone();
    replacement.context.request.intent_id += 1;
    let v::CommandBody::New(mut order) = replacement.body else {
        unreachable!()
    };
    order.client_id = venue::text("replacement");
    order.price = Some(venue::d(70000, 0));
    replacement.body = v::CommandBody::CancelReplace {
        target: v::OrderTarget {
            order_id: [9; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(venue::text("test-1")),
            venue_order_id: None,
        },
        replacement: order,
        mode: v::CancelReplaceMode::StopOnFailure,
    };
    a.admit(&canonical(replacement), 60).unwrap();
    assert_eq!(a.asset(key(2)).unwrap().reserved, units(130));
    assert_eq!(
        a.order(&first.orders()[0]).unwrap().remaining_quantity,
        f(100000)
    );
}
#[test]
fn all_asset_candidate_failure_preserves_the_preexisting_order() {
    let (mut a, cmd) = setup();
    let first = a.admit(&canonical(cmd.clone()), 60).unwrap();
    let mut second = cmd;
    second.context.request.intent_id += 1;
    let v::CommandBody::New(ref mut o) = second.body else {
        unreachable!()
    };
    o.client_id = venue::text("too-large");
    o.quantity = v::Quantity::Base(venue::d(1, 0));
    assert_eq!(
        a.admit(&canonical(second), 60),
        Err(Error::InsufficientAsset)
    );
    assert_eq!(a.asset(key(2)).unwrap().reserved, units(60));
    assert_eq!(a.order(&first.orders()[0]).unwrap().filled_quantity, f(0));
}

#[test]
fn typed_spot_advanced_commands_do_not_inherit_basic_order_admission() {
    let (mut account, mut cmd) = setup();
    let v::CommandBody::New(order) = cmd.body else {
        unreachable!()
    };
    cmd.body = v::CommandBody::SpotAdvanced(v::SpotAdvancedCommand::SorNew(order));
    let command = canonical(cmd);
    assert_eq!(account.admit(&command, 60), Err(Error::Unsupported));
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(0));
}
