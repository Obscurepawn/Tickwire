mod common;
use common::*;
use tickwire_account::stp_book::{aggressive_ticks, price_to_ticks};
use tickwire_account::*;
use tickwire_venue as v;

fn px(cents: i128) -> Fixed {
    Fixed::from_atoms(cents, 2).unwrap()
}

fn spot_order_cmd(
    base: &v::Command,
    intent_id: u64,
    client_id: &str,
    side: v::Side,
    qty: v::Decimal,
    price: v::Decimal,
) -> v::CanonicalCommand {
    let mut cmd = base.clone();
    cmd.context.request.intent_id = intent_id;
    cmd.context.request.recovery_object += intent_id;
    cmd.context.request.deadline = 5_000;
    let v::CommandBody::New(ref mut o) = cmd.body else {
        unreachable!()
    };
    o.client_id = venue::text(client_id);
    o.side = side;
    o.quantity = v::Quantity::Base(qty);
    o.price = Some(price);
    canonical(cmd)
}

fn spot_ack(
    cmd: &v::Command,
    order_id: [u8; 16],
    venue_id: &str,
    cum_qty: v::Decimal,
) -> (v::OrderObservation, v::CanonicalEvent) {
    let f_ev = fill(cmd, "unused", "0.001", "100", "0.1");
    let v::VenueEventBody::Fill(f_box) = f_ev.body else {
        unreachable!()
    };
    let mut obs = f_box.observation;
    obs.order.order_id = order_id;
    obs.order.venue_order_id = Some(venue::text(venue_id));
    obs.status = if cum_qty.is_positive() {
        v::OrderStatus::PartiallyFilled
    } else {
        v::OrderStatus::New
    };
    obs.cumulative_quantity = Some(cum_qty);
    let v::CommandBody::New(ref spec) = cmd.body else {
        unreachable!()
    };
    obs.order_price = spec.price;
    let mut ev = venue::event();
    ev.identity.product = cmd.context.request.product;
    ev.body = v::VenueEventBody::Order(Box::new(obs.clone()));
    (obs, event(ev))
}

fn modify_cmd(
    base: &v::Command,
    intent_id: u64,
    target: v::OrderTarget,
    side: v::Side,
    quantity: v::Decimal,
    price: v::Decimal,
    new_client_id: Option<&str>,
) -> v::CanonicalCommand {
    let mut cmd = base.clone();
    cmd.context.request.intent_id = intent_id;
    cmd.context.request.recovery_object += intent_id;
    cmd.context.request.deadline = 5_000;
    cmd.body = v::CommandBody::Modify(v::ModifyOrder {
        target,
        new_client_id: new_client_id.map(venue::text),
        side,
        quantity,
        price,
        position_side: v::PositionSide::Both,
        reduce_only: false,
    });
    canonical(cmd)
}

#[test]
fn stp_book_atomic_bbo_dual_price_pending_modify_swap_remove_and_crossing_scan() {
    assert_eq!(DEFAULT_STP_SYMBOLS, 64);
    assert_eq!(DEFAULT_STP_ORDERS_PER_SIDE, 128);

    let tick = px(1); // 0.01
    assert_eq!(price_to_ticks(px(10_000), tick).unwrap(), 10_000);
    assert_eq!(
        price_to_ticks(Fixed::from_atoms(10_005, 3).unwrap(), tick),
        Err(Error::InvalidPrice)
    );
    assert_eq!(
        price_to_ticks(Fixed::from_atoms(0, 2).unwrap(), tick),
        Err(Error::InvalidPrice)
    );
    assert_eq!(aggressive_ticks(v::Side::Buy, 9_000, Some(10_000)), 10_000);
    assert_eq!(aggressive_ticks(v::Side::Buy, 10_000, Some(9_000)), 10_000);
    assert_eq!(
        aggressive_ticks(v::Side::Sell, 11_000, Some(10_000)),
        10_000
    );
    assert_eq!(
        aggressive_ticks(v::Side::Sell, 10_000, Some(11_000)),
        10_000
    );

    // Dynamic symbol growth past DEFAULT_STP_SYMBOLS (64) without breaking lookups.
    let mut standalone = StpBook::new();
    for inst in 1..=70u64 {
        standalone.register_instrument(inst, tick).unwrap();
        assert_eq!(standalone.best_bid_ticks(inst).unwrap(), None);
        assert_eq!(standalone.best_ask_ticks(inst).unwrap(), None);
    }

    // Drive StpBook via Account lifecycle (resting Sell @ 110.00, pending Modify -> 100.00).
    let (mut snap, cat, base_cmd) = fixture();
    snap.assets[0].free = units(10); // 10 BTC
    snap.assets[1].free = units(100_000); // 100,000 USDT
    let mut account = Account::from_snapshot(snap, cat)
        .unwrap()
        .with_stp_enforcement(true);
    assert!(account.stp_enforcement());
    assert_eq!(account.stp_book().best_bid_ticks(12).unwrap(), None);
    assert_eq!(account.stp_book().best_ask_ticks(12).unwrap(), None);

    // Rest Sell #1 at 110.00 and Sell #2 at 112.00.
    let sell1 = spot_order_cmd(
        &base_cmd,
        10,
        "sell-110",
        v::Side::Sell,
        venue::d(1, 0),
        venue::d(11000, -2),
    );
    let adm_s1 = account.admit(&sell1, 60).unwrap();
    let k_s1 = adm_s1.orders()[0].clone();
    let (obs_s1, ack_s1) = spot_ack(sell1.command(), [10; 16], "v-s1", venue::d(0, 0));
    account.apply_event(&ack_s1).unwrap();

    let sell2 = spot_order_cmd(
        &base_cmd,
        11,
        "sell-112",
        v::Side::Sell,
        venue::d(1, 0),
        venue::d(11200, -2),
    );
    let adm_s2 = account.admit(&sell2, 61).unwrap();
    let k_s2 = adm_s2.orders()[0].clone();
    assert_eq!(account.stp_book().best_ask_ticks(12).unwrap(), Some(11_000));

    // Incoming Buy @ 110.00 crosses; Buy @ 109.99 does not cross (`human-docs/3.6`).
    assert!(
        account
            .stp_book()
            .self_cross(12, v::Side::Buy, px(11_000))
            .unwrap()
    );
    assert!(
        !account
            .stp_book()
            .self_cross(12, v::Side::Buy, px(10_999))
            .unwrap()
    );

    let bad_buy = spot_order_cmd(
        &base_cmd,
        12,
        "buy-cross-110",
        v::Side::Buy,
        venue::d(1, 0),
        venue::d(11000, -2),
    );
    let err = account.admit(&bad_buy, 62).unwrap_err();
    assert_eq!(err, Error::CrossesBook);
    assert_eq!(classify_error_reason(&err), v::TerminalReason::CrossesBook);
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(0));

    // Pending Modify on Sell #1 from 110.00 down to 100.00 -> dual-price rule publishes min(110, 100) = 100.00!
    let target_s1 = v::OrderTarget {
        order_id: obs_s1.order.order_id,
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: obs_s1.order.client_id.clone(),
        venue_order_id: obs_s1.order.venue_order_id.clone(),
    };
    let mod_s1 = modify_cmd(
        &base_cmd,
        13,
        target_s1.clone(),
        v::Side::Sell,
        venue::d(1, 0),
        venue::d(10000, -2),
        None,
    );
    let adm_mod_s1 = account.admit(&mod_s1, 63).unwrap();
    assert_eq!(account.stp_book().best_ask_ticks(12).unwrap(), Some(10_000));

    // Buy @ 105.00 now crosses pending 100.00 and is refused, whereas Buy @ 99.99 is allowed!
    let cross_105 = spot_order_cmd(
        &base_cmd,
        14,
        "buy-105",
        v::Side::Buy,
        venue::d(1, 0),
        venue::d(10500, -2),
    );
    assert_eq!(account.admit(&cross_105, 64), Err(Error::CrossesBook));

    let mut crossing_buf = [
        OrderKey {
            intent: IntentKey {
                epoch: 1,
                intent_id: 1,
            },
            client_id: venue::text("placeholder"),
        },
        OrderKey {
            intent: IntentKey {
                epoch: 1,
                intent_id: 2,
            },
            client_id: venue::text("placeholder"),
        },
    ];
    let n_cross = account
        .stp_book()
        .find_crossing_orders(12, v::Side::Buy, px(11_500), &mut crossing_buf)
        .unwrap();
    assert_eq!(n_cross, 2);
    assert!(crossing_buf.contains(&k_s1) && crossing_buf.contains(&k_s2));
    // Truncated caller buffer stops cleanly at buffer capacity without allocation.
    let mut tiny_buf = [crossing_buf[0].clone()];
    assert_eq!(
        account
            .stp_book()
            .find_crossing_orders(12, v::Side::Buy, px(11_500), &mut tiny_buf)
            .unwrap(),
        1
    );

    let ok_buy = spot_order_cmd(
        &base_cmd,
        15,
        "buy-9999",
        v::Side::Buy,
        venue::d(1, 0),
        venue::d(9999, -2),
    );
    let adm_b1 = account.admit(&ok_buy, 65).unwrap();
    let k_b1 = adm_b1.orders()[0].clone();
    assert_eq!(account.stp_book().best_bid_ticks(12).unwrap(), Some(9_999));

    // Modifying Sell #1 down to 99.00 would cross resting Buy @ 99.99 -> refused with clean rollback!
    account.apply_abort(&adm_mod_s1).unwrap();
    assert_eq!(account.stp_book().best_ask_ticks(12).unwrap(), Some(11_000));
    let mod_cross = modify_cmd(
        &base_cmd,
        16,
        target_s1,
        v::Side::Sell,
        venue::d(1, 0),
        venue::d(9900, -2),
        None,
    );
    assert_eq!(account.admit(&mod_cross, 66), Err(Error::CrossesBook));
    assert_eq!(account.stp_book().best_ask_ticks(12).unwrap(), Some(11_000));

    // Swap-remove of best ask (Sell #1 @ 110.00) recomputes atomic BBO to Sell #2 (112.00), then None.
    let mut term_s1 = terminal(sell1.command(), "0");
    term_s1.identity.event_id = venue::text("term-s1");
    if let v::VenueEventBody::Order(ref mut o) = term_s1.body {
        o.order.order_id = [10; 16];
    }
    account.apply_event(&event(term_s1)).unwrap();
    assert_eq!(account.stp_book().best_ask_ticks(12).unwrap(), Some(11_200));

    let mut term_s2 = terminal(sell2.command(), "0");
    term_s2.identity.event_id = venue::text("term-s2");
    if let v::VenueEventBody::Order(ref mut o) = term_s2.body {
        o.order.order_id = [11; 16];
    }
    account.apply_event(&event(term_s2)).unwrap();
    assert_eq!(account.stp_book().best_ask_ticks(12).unwrap(), None);

    let mut term_b1 = terminal(ok_buy.command(), "0");
    term_b1.identity.event_id = venue::text("term-b1");
    if let v::VenueEventBody::Order(ref mut o) = term_b1.body {
        o.order.order_id = [15; 16];
    }
    account.apply_event(&event(term_b1)).unwrap();
    assert_eq!(account.stp_book().best_bid_ticks(12).unwrap(), None);
    assert!(account.order(&k_b1).unwrap().terminal);
}

#[test]
fn spot_native_modify_worst_case_holds_clamping_client_id_rotation_reject_and_abort() {
    let (mut snap, cat, base_cmd) = fixture();
    snap.assets[0].free = units(2); // 2 BTC
    snap.assets[1].free = units(200); // 200 USDT
    let mut account = Account::from_snapshot(snap.clone(), cat.clone()).unwrap();

    // 1. Place Spot Buy 1 BTC @ 100 USDT -> reserves 100 USDT.
    let buy_cmd = spot_order_cmd(
        &base_cmd,
        10,
        "spot-buy-1",
        v::Side::Buy,
        venue::d(1, 0),
        venue::d(100, 0),
    );
    let adm_new = account.admit(&buy_cmd, 60).unwrap();
    let okey = adm_new.orders()[0].clone();
    let (obs, ack_ev) = spot_ack(buy_cmd.command(), [31; 16], "v-spot-31", venue::d(0, 0));
    account.apply_event(&ack_ev).unwrap();
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(100));

    let target = v::OrderTarget {
        order_id: obs.order.order_id,
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: obs.order.client_id.clone(),
        venue_order_id: obs.order.venue_order_id.clone(),
    };

    // 2. Identical price & qty -> Error::NoChange.
    let same = modify_cmd(
        &base_cmd,
        11,
        target.clone(),
        v::Side::Buy,
        venue::d(1, 0),
        venue::d(100, 0),
        None,
    );
    let err_same = account.admit(&same, 61).unwrap_err();
    assert_eq!(err_same, Error::NoChange);
    assert_eq!(
        classify_error_reason(&err_same),
        v::TerminalReason::NoChange
    );

    // 3. Exceeding available USDT (modify to 2 BTC @ 120 = 240 > 200) -> InsufficientAsset rollback!
    let too_big = modify_cmd(
        &base_cmd,
        12,
        target.clone(),
        v::Side::Buy,
        venue::d(2, 0),
        venue::d(120, 0),
        Some("spot-buy-1-big"),
    );
    let err_funds = account.admit(&too_big, 62).unwrap_err();
    assert_eq!(err_funds, Error::InsufficientAsset);
    assert_eq!(
        classify_error_reason(&err_funds),
        v::TerminalReason::InsufficientFunds
    );
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(100));

    // 4. Admit Modify down in qty (0.5 BTC) but UP in price (150 USDT -> 75 USDT), while resting is 100 USDT.
    // Worst-case hold must remain max(100, 75) = 100 USDT while in flight!
    let mod_down_qty = modify_cmd(
        &base_cmd,
        13,
        target.clone(),
        v::Side::Buy,
        venue::d(5, -1),
        venue::d(150, 0),
        Some("spot-buy-1-r1"),
    );
    let adm_mod1 = account.admit(&mod_down_qty, 63).unwrap();
    assert!(adm_mod1.orders().is_empty());
    assert!(adm_mod1.resources().is_empty());
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(100));

    // Second concurrent Modify on the same order -> Error::TooManyPendingActions!
    let second_mod = modify_cmd(
        &base_cmd,
        14,
        target.clone(),
        v::Side::Buy,
        venue::d(8, -1),
        venue::d(110, 0),
        None,
    );
    let err_conc = account.admit(&second_mod, 64).unwrap_err();
    assert_eq!(err_conc, Error::TooManyPendingActions);
    assert_eq!(
        classify_error_reason(&err_conc),
        v::TerminalReason::TooManyPendingActions
    );

    // Zero-byte pre-send abort cleans up pending modify and tentative client_id alias!
    assert_eq!(account.apply_abort(&adm_mod1).unwrap(), Applied::New);
    assert_eq!(account.apply_abort(&adm_mod1).unwrap(), Applied::Duplicate);

    // 5. Admit Modify increasing exposure: 1.5 BTC @ 120 USDT = 180 USDT (+80 USDT delta).
    let mod_up = modify_cmd(
        &base_cmd,
        15,
        target.clone(),
        v::Side::Buy,
        venue::d(15, -1),
        venue::d(120, 0),
        Some("spot-buy-1-r2"),
    );
    let adm_mod_up = account.admit(&mod_up, 65).unwrap();
    assert_eq!(
        adm_mod_up.resources(),
        &[AssetAmount {
            key: key(2),
            amount: units(80),
        }]
    );
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(180));

    // ModifyRejected rolls back pending modify, releases the +80 USDT hold, and records TerminalReason!
    let mut rej_ev = venue::event();
    rej_ev.body = v::VenueEventBody::ModifyRejected(Box::new(v::ModifyRejection {
        target: target.clone(),
        amendment_id: Some(venue::text("rej-15")),
        intent: Some(IntentKey {
            epoch: 4,
            intent_id: 15,
        }),
        new_client_id: Some(venue::text("spot-buy-1-r2")),
        venue_order_id: Some(venue::text("v-spot-31")),
        code: Some(-5022),
        reason: v::TerminalReason::PostOnlyWouldTake,
    }));
    let rej_canonical = event(rej_ev);
    assert_eq!(account.apply_event(&rej_canonical).unwrap(), Applied::New);
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(100));
    assert_eq!(
        account.order_last_modify_rejection(&okey),
        Some(v::TerminalReason::PostOnlyWouldTake)
    );

    // 6. Admit Modify to 1.5 BTC @ 120 USDT with rotated client_id `spot-buy-1-r3` and venue-clamped Modified ack!
    // Venue clamps price to 110.00 and quantity to 1.4 BTC (reserve becomes 1.4 * 110 = 154 USDT) and rotates venue_order_id.
    let mod_clamped = modify_cmd(
        &base_cmd,
        16,
        target.clone(),
        v::Side::Buy,
        venue::d(15, -1),
        venue::d(120, 0),
        Some("spot-buy-1-r3"),
    );
    let adm_mod_clamped = account.admit(&mod_clamped, 66).unwrap();
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(180));

    let mut mod_ok_ev = venue::event();
    mod_ok_ev.identity.event_id = venue::text("mod-ack-16");
    mod_ok_ev.body = v::VenueEventBody::Modified(Box::new(v::ModifyObservation {
        target: target.clone(),
        amendment_id: Some(venue::text("ack-16")),
        intent: Some(IntentKey {
            epoch: 4,
            intent_id: 16,
        }),
        prior_client_id: Some(venue::text("spot-buy-1")),
        new_client_id: Some(venue::text("spot-buy-1-r3")),
        venue_order_id: Some(venue::text("v-spot-32")),
        new_quantity: venue::d(14, -1),
        new_price: venue::d(110, 0),
        cumulative_filled: Some(venue::d(0, 0)),
    }));
    let mod_ok_canonical = event(mod_ok_ev);
    assert_eq!(
        account.apply_event(&mod_ok_canonical).unwrap(),
        Applied::New
    );
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(154));
    assert_eq!(
        account.order(&okey).unwrap().effective_quantity,
        f(140_000_000)
    );
    assert_eq!(account.stp_book().best_bid_ticks(12).unwrap(), Some(11_000));

    // Partial fill of 0.6 BTC @ 110 USDT arriving under the ROTATED client_id (`spot-buy-1-r3`) and venue_id (`v-spot-32`)!
    let mut fill_rot = fill(buy_cmd.command(), "t-rot-1", "0.6", "110", "66");
    let v::VenueEventBody::Fill(ref mut fb) = fill_rot.body else {
        unreachable!()
    };
    fb.observation.order.order_id = obs.order.order_id;
    fb.observation.order.client_id = Some(venue::text("spot-buy-1-r3"));
    fb.observation.order.venue_order_id = Some(venue::text("v-spot-32"));
    fb.observation.original_quantity = Some(venue::d(14, -1));
    fb.observation.order_price = Some(venue::d(110, 0));
    let fill_rot_canonical = event(fill_rot);
    account.apply_event(&fill_rot_canonical).unwrap();
    assert_eq!(account.order(&okey).unwrap().filled_quantity, f(60_000_000));
    // Remaining = 0.8 BTC @ 110 = 88 USDT reserved.
    assert_eq!(account.asset(key(2)).unwrap().reserved, units(88));

    // Attempting to modify total quantity to <= filled (0.6 BTC) returns Error::InvalidQuantity!
    let target_r3 = v::OrderTarget {
        order_id: obs.order.order_id,
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(venue::text("spot-buy-1-r3")),
        venue_order_id: Some(venue::text("v-spot-32")),
    };
    let bad_qty = modify_cmd(
        &base_cmd,
        17,
        target_r3.clone(),
        v::Side::Buy,
        venue::d(6, -1),
        venue::d(110, 0),
        None,
    );
    let err_qty = account.admit(&bad_qty, 67).unwrap_err();
    assert_eq!(err_qty, Error::InvalidQuantity);
    assert_eq!(
        classify_error_reason(&err_qty),
        v::TerminalReason::InvalidQuantity
    );

    // Deterministic Account::recover replay reproduces exact balances, reservations, and order state!
    let records = vec![
        RecoveryRecord::Admit {
            command: Box::new(buy_cmd),
            evaluated_at: 60,
            expected: adm_new.image(),
        },
        RecoveryRecord::VenueEvent(Box::new(ack_ev)),
        RecoveryRecord::Admit {
            command: Box::new(mod_down_qty.clone()),
            evaluated_at: 63,
            expected: adm_mod1.image(),
        },
        RecoveryRecord::Aborted {
            intent: IntentKey {
                epoch: 4,
                intent_id: 13,
            },
            command_digest: mod_down_qty.digest(),
        },
        RecoveryRecord::Admit {
            command: Box::new(mod_up),
            evaluated_at: 65,
            expected: adm_mod_up.image(),
        },
        RecoveryRecord::VenueEvent(Box::new(rej_canonical)),
        RecoveryRecord::Admit {
            command: Box::new(mod_clamped),
            evaluated_at: 66,
            expected: adm_mod_clamped.image(),
        },
        RecoveryRecord::VenueEvent(Box::new(mod_ok_canonical)),
        RecoveryRecord::VenueEvent(Box::new(fill_rot_canonical)),
    ];
    let replayed = Account::recover(snap, cat, &records).unwrap();
    assert!(replayed.frozen());
    assert_eq!(replayed.asset(key(1)), account.asset(key(1)));
    assert_eq!(replayed.asset(key(2)), account.asset(key(2)));
    let mut expected_view = account.order(&okey).unwrap();
    expected_view.unknown = true;
    assert_eq!(replayed.order(&okey), Some(expected_view));

    // Cancel remaining order and verify Modify on closed order returns Error::OrderNotOpen + UserCanceled reason!
    let mut cancel_ev = venue::event();
    cancel_ev.identity.event_id = venue::text("cancel-r3");
    let mut cancel_obs = obs;
    cancel_obs.order.client_id = Some(venue::text("spot-buy-1-r3"));
    cancel_obs.order.venue_order_id = Some(venue::text("v-spot-32"));
    cancel_obs.status = v::OrderStatus::Canceled;
    cancel_obs.original_quantity = Some(venue::d(14, -1));
    cancel_obs.cumulative_quantity = Some(venue::d(6, -1));
    cancel_ev.body = v::VenueEventBody::Order(Box::new(cancel_obs));
    account.apply_event(&event(cancel_ev)).unwrap();
    assert_eq!(
        account.order_terminal_reason(&okey),
        Some(v::TerminalReason::UserCanceled)
    );

    let closed_mod = modify_cmd(
        &base_cmd,
        18,
        target_r3,
        v::Side::Buy,
        venue::d(1, 0),
        venue::d(110, 0),
        None,
    );
    let err_closed = account.admit(&closed_mod, 68).unwrap_err();
    assert_eq!(err_closed, Error::OrderNotOpen);
    assert_eq!(
        classify_error_reason(&err_closed),
        v::TerminalReason::OrderNotOpen
    );
}

#[test]
fn linear_perpetual_modify_enforces_worst_case_capital_limits_and_reduce_only_bounds() {
    let (mut s, mut c, base_cmd) = futures_fixture();
    s.assets[1].free = units(50);
    s.venue_available = vec![(1, units(50))];
    c.entries[0].max_gross_quantity = units(4);
    let mut account = Account::from_snapshot(s, c).unwrap();

    // Place LinearPerpetual Buy 1 @ 100 -> initial margin 10 USDT (shadow = 40 USDT).
    let create = canonical(base_cmd.clone());
    let adm_create = account.admit(&create, 60).unwrap();
    let okey = adm_create.orders()[0].clone();
    assert_eq!(account.venue_shadow(1).unwrap(), units(40));

    let (obs, ack_ev) = spot_ack(&base_cmd, [41; 16], "v-perp-41", venue::d(0, 0));
    account.apply_event(&ack_ev).unwrap();

    let target = v::OrderTarget {
        order_id: obs.order.order_id,
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: obs.order.client_id.clone(),
        venue_order_id: obs.order.venue_order_id.clone(),
    };

    // Modify to 5 BTC exceeds max_gross_quantity (4 BTC) -> Error::Limit with clean rollback!
    let over_limit = modify_cmd(
        &base_cmd,
        11,
        target.clone(),
        v::Side::Buy,
        venue::d(5, 0),
        venue::d(100, 0),
        None,
    );
    assert_eq!(account.admit(&over_limit, 61), Err(Error::Limit));
    assert_eq!(account.venue_shadow(1).unwrap(), units(40));

    // Modify to 3 BTC @ 110 -> stress_low=100 gives order loss 3*(110-100)=30 + IM 3*10=30 = 60 > 50 -> InsufficientCapital!
    let over_cap = modify_cmd(
        &base_cmd,
        12,
        target.clone(),
        v::Side::Buy,
        venue::d(3, 0),
        venue::d(110, 0),
        None,
    );
    assert_eq!(
        account.admit(&over_cap, 62),
        Err(Error::InsufficientCapital)
    );
    assert_eq!(account.venue_shadow(1).unwrap(), units(40));

    // Modify to 2 BTC @ 105 -> order loss 2*(105-100)=10 + IM 2*10=20 = 30 USDT (+20 capital hold, shadow = 20 USDT).
    let ok_mod = modify_cmd(
        &base_cmd,
        13,
        target.clone(),
        v::Side::Buy,
        venue::d(2, 0),
        venue::d(105, 0),
        None,
    );
    let adm_mod = account.admit(&ok_mod, 63).unwrap();
    assert_eq!(adm_mod.capital_holds()[0].amount, units(20));
    assert_eq!(account.venue_shadow(1).unwrap(), units(20));

    // Venue applies Modified(2 BTC @ 100) -> order loss drops to 0, IM is 20 USDT -> shadow becomes 30 USDT!
    let mut mod_ev = venue::event();
    mod_ev.identity.product = v::Product::LinearPerpetual;
    mod_ev.body = v::VenueEventBody::Modified(Box::new(v::ModifyObservation {
        target,
        amendment_id: None,
        intent: Some(IntentKey {
            epoch: 4,
            intent_id: 13,
        }),
        prior_client_id: obs.order.client_id,
        new_client_id: None,
        venue_order_id: obs.order.venue_order_id,
        new_quantity: venue::d(2, 0),
        new_price: venue::d(100, 0),
        cumulative_filled: Some(venue::d(0, 0)),
    }));
    account.apply_event(&event(mod_ev)).unwrap();
    assert_eq!(account.venue_shadow(1).unwrap(), units(30));
    assert_eq!(account.order(&okey).unwrap().effective_quantity, units(2));
}

#[test]
fn projected_fill_exact_average_and_quote_math_real_fill_convergence_stp_guard_and_zero_side_effects()
 {
    let (mut snap, cat, base_cmd) = fixture();
    snap.assets[0].free = units(20);
    snap.assets[1].free = units(10_000);
    let mut account = Account::from_snapshot(snap, cat).unwrap();

    // Buy 10 BTC @ 11.00 USDT limit (`human-docs/3.5` worked example prices 10.50 and 9.80).
    let order_10 = spot_order_cmd(
        &base_cmd,
        10,
        "proj-buy-10",
        v::Side::Buy,
        venue::d(10, 0),
        venue::d(1100, -2),
    );
    let adm = account.admit(&order_10, 60).unwrap();
    let okey = adm.orders()[0].clone();
    let asset_btc_before = account.asset(key(1)).unwrap();
    let asset_usdt_before = account.asset(key(2)).unwrap();
    let seq_before = account.ledger_sequence();

    // Step 1 (`human-docs/3.5`): cum_qty = 4 @ avg 10.50 -> gap = 4, gap_price = 10.50.
    let pf1 = account
        .on_cum_qty(
            &okey,
            units(4),
            ProjectedPriceInput::AveragePrice {
                avg_price: px(1_050),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(pf1.gap_quantity, units(4));
    assert_eq!(pf1.gap_price, f(1_050_000_000));
    assert_eq!(pf1.order_filled, units(0));
    assert_eq!(pf1.order_projected, units(4));
    assert!(pf1.projected);
    assert_eq!(account.order_projected_filled(&okey), Some(units(4)));

    // Step 2: duplicate cum_qty = 4 -> None.
    assert_eq!(
        account
            .on_cum_qty(
                &okey,
                units(4),
                ProjectedPriceInput::AveragePrice {
                    avg_price: px(1_050),
                },
            )
            .unwrap(),
        None
    );

    // Step 3: stale cum_qty = 1 (< 4) -> None.
    assert_eq!(
        account
            .on_cum_qty(
                &okey,
                units(1),
                ProjectedPriceInput::AveragePrice {
                    avg_price: px(1_050),
                },
            )
            .unwrap(),
        None
    );

    // Step 4: cum_qty = 7 @ avg 10.20 -> gap = 3, gap_price = (7*10.20 - 4*10.50)/3 = (71.40 - 42.00)/3 = 9.80!
    let pf2 = account
        .on_cum_qty(
            &okey,
            units(7),
            ProjectedPriceInput::AveragePrice {
                avg_price: px(1_020),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(pf2.gap_quantity, units(3));
    assert_eq!(pf2.gap_price, f(980_000_000));
    assert_eq!(pf2.order_filled, units(0));
    assert_eq!(pf2.order_projected, units(7));
    assert_eq!(account.drain_projected_fills(), vec![pf1, pf2]);

    // CRITICAL INVARIANT: settled assets, Order.filled, facts, and ledger_sequence are 100% untouched!
    assert_eq!(account.asset(key(1)).unwrap(), asset_btc_before);
    assert_eq!(account.asset(key(2)).unwrap(), asset_usdt_before);
    assert_eq!(account.order(&okey).unwrap().filled_quantity, units(0));
    assert_eq!(account.order(&okey).unwrap().remaining_quantity, units(10));
    assert_eq!(account.ledger_sequence(), seq_before);

    // Real fills arrive for 4 BTC and then 4 BTC (total settled = 8 BTC > projected 7 BTC).
    account
        .apply_event(&event(fill(order_10.command(), "tr-1", "4", "10.5", "42")))
        .unwrap();
    assert_eq!(account.order(&okey).unwrap().filled_quantity, units(4));
    assert_eq!(account.order_projected_filled(&okey), Some(units(7)));

    account
        .apply_event(&event(fill(
            order_10.command(),
            "tr-2",
            "4",
            "9.95",
            "39.8",
        )))
        .unwrap();
    assert_eq!(account.order(&okey).unwrap().filled_quantity, units(8));
    assert_eq!(account.order_projected_filled(&okey), Some(units(8)));

    // Late cumulative report at cum_qty = 8 emits nothing because settled filled is already 8!
    assert_eq!(
        account
            .on_cum_qty(
                &okey,
                units(8),
                ProjectedPriceInput::CumulativeQuote {
                    cum_quote: px(8_180),
                },
            )
            .unwrap(),
        None
    );

    // CumulativeQuote report at cum_qty = 10 with cum_quote = 101.80 -> delta_quote = 20.00 over delta_qty = 2 -> gap_price = 10.00!
    let pf3 = account
        .on_cum_qty(
            &okey,
            units(10),
            ProjectedPriceInput::CumulativeQuote {
                cum_quote: px(10_180),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(pf3.gap_quantity, units(2));
    assert_eq!(pf3.gap_price, units(10));
    assert_eq!(pf3.order_filled, units(8));
    assert_eq!(pf3.order_projected, units(10));
    assert_eq!(account.pop_projected_fill(), Some(pf3));

    // CHECK_STP guard: if an opposite-side resting order crosses our order, ProjectedFill is suppressed!
    let sell_cross = spot_order_cmd(
        &base_cmd,
        20,
        "resting-sell-9",
        v::Side::Sell,
        venue::d(1, 0),
        venue::d(900, -2),
    );
    account.admit(&sell_cross, 70).unwrap();
    let buy_cross = spot_order_cmd(
        &base_cmd,
        21,
        "crossing-buy-10",
        v::Side::Buy,
        venue::d(2, 0),
        venue::d(1000, -2),
    );
    let adm_bc = account.admit(&buy_cross, 71).unwrap();
    let k_bc = adm_bc.orders()[0].clone();
    assert!(
        account
            .stp_book()
            .self_cross(12, v::Side::Buy, px(1_000))
            .unwrap()
    );
    assert_eq!(
        account
            .on_cum_qty(&k_bc, units(1), ProjectedPriceInput::LimitFallback)
            .unwrap(),
        None
    );

    // Automatic Order observation hook when set_projected_fill(true) is enabled on a non-crossing order:
    let mut term_sc = terminal(sell_cross.command(), "0");
    term_sc.identity.event_id = venue::text("term-sell-cross");
    if let v::VenueEventBody::Order(ref mut o) = term_sc.body {
        o.order.order_id = [20; 16];
    }
    account.apply_event(&event(term_sc)).unwrap();
    account.set_projected_fill(true);
    assert!(account.projected_fill_enabled());
    let (_, ack_with_cum) = spot_ack(buy_cross.command(), [55; 16], "v-bc-55", venue::d(1, 0));
    account.apply_event(&ack_with_cum).unwrap();
    let auto_pf = account
        .pop_projected_fill()
        .expect("Order hook emits ProjectedFill");
    assert_eq!(auto_pf.order, k_bc);
    assert_eq!(auto_pf.gap_quantity, units(1));
    assert_eq!(auto_pf.gap_price, units(10));
}

#[test]
fn projected_fill_gap_price_tracks_quote_and_avg_denominators_across_limit_fallback_and_settled_first_fills()
 {
    let (mut snap, cat, base_cmd) = fixture();
    snap.assets[0].free = units(50);
    snap.assets[1].free = units(10_000);
    let mut account = Account::from_snapshot(snap, cat).unwrap();

    // Order A: settled fill (3 @ 10.00) arrives BEFORE CumulativeQuote (cum_qty=5, cum_quote=50.00),
    // followed by LimitFallback (cum_qty=7) and CumulativeQuote (cum_qty=8, cum_quote=83.00 -> delta 33.00 / 3 = 11.00).
    let cmd_a = spot_order_cmd(
        &base_cmd,
        101,
        "pf-denom-a",
        v::Side::Buy,
        venue::d(10, 0),
        venue::d(1200, -2),
    );
    let k_a = account.admit(&cmd_a, 10).unwrap().orders()[0].clone();
    account
        .apply_event(&event(fill(cmd_a.command(), "ta-1", "3", "10", "30")))
        .unwrap();
    let pf_a1 = account
        .on_cum_qty(
            &k_a,
            units(5),
            ProjectedPriceInput::CumulativeQuote {
                cum_quote: px(5_000),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(pf_a1.gap_quantity, units(2));
    assert_eq!(pf_a1.gap_price, units(10)); // 50.00 / 5 == 10.00

    let pf_a2 = account
        .on_cum_qty(&k_a, units(7), ProjectedPriceInput::LimitFallback)
        .unwrap()
        .unwrap();
    assert_eq!(pf_a2.gap_quantity, units(2));
    assert_eq!(pf_a2.gap_price, units(12));

    let pf_a3 = account
        .on_cum_qty(
            &k_a,
            units(8),
            ProjectedPriceInput::CumulativeQuote {
                cum_quote: px(8_300),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(pf_a3.gap_quantity, units(1));
    assert_eq!(pf_a3.gap_price, units(11)); // (83.00 - 50.00) / (8 - 5) == 11.00 (NOT 33.00 / 1!)

    // Order B: LimitFallback first (cum_qty=3), then CumulativeQuote (cum_qty=4, cum_quote=40.00 -> 40/4 = 10.00),
    // and Order C: LimitFallback first (cum_qty=2), then AveragePrice (cum_qty=4, avg=10.50 -> 10.50).
    let cmd_b = spot_order_cmd(
        &base_cmd,
        102,
        "pf-denom-b",
        v::Side::Buy,
        venue::d(10, 0),
        venue::d(1200, -2),
    );
    let k_b = account.admit(&cmd_b, 11).unwrap().orders()[0].clone();
    let _ = account
        .on_cum_qty(&k_b, units(3), ProjectedPriceInput::LimitFallback)
        .unwrap()
        .unwrap();
    let pf_b2 = account
        .on_cum_qty(
            &k_b,
            units(4),
            ProjectedPriceInput::CumulativeQuote {
                cum_quote: px(4_000),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(pf_b2.gap_quantity, units(1));
    assert_eq!(pf_b2.gap_price, units(10)); // 40.00 / 4 == 10.00 (NOT 40.00 / 1!)

    let cmd_c = spot_order_cmd(
        &base_cmd,
        103,
        "pf-denom-c",
        v::Side::Buy,
        venue::d(10, 0),
        venue::d(1200, -2),
    );
    let k_c = account.admit(&cmd_c, 12).unwrap().orders()[0].clone();
    let _ = account
        .on_cum_qty(&k_c, units(2), ProjectedPriceInput::LimitFallback)
        .unwrap()
        .unwrap();
    let pf_c2 = account
        .on_cum_qty(
            &k_c,
            units(3),
            ProjectedPriceInput::AveragePrice {
                avg_price: px(1_000),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(pf_c2.gap_price, units(10));
    let pf_c3 = account
        .on_cum_qty(
            &k_c,
            units(4),
            ProjectedPriceInput::AveragePrice {
                avg_price: px(1_050),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(pf_c3.gap_price, units(12)); // (4*10.50 - 3*10.00) / (4 - 3) == 12.00
}

#[test]
fn terminal_order_supersedes_pending_modify_releases_delta_hold_evicts_and_tolerates_late_modify_events()
 {
    // Part 1: Futures order canceled while pending_modify holds incremental shadow margin + new_client_id.
    let (mut f_snap, f_cat, mut base_fut) = futures_fixture();
    f_snap.assets[1].free = units(200);
    f_snap.venue_available = vec![(1, units(200))];
    let mut account = Account::from_snapshot(f_snap, f_cat).unwrap();
    base_fut.context.request.deadline = 5_000;
    let c_new = canonical(base_fut.clone());
    let adm_new = account.admit(&c_new, 100).unwrap();
    let k1 = adm_new.orders()[0].clone();
    let shadow_after_new = account.venue_shadow(1).unwrap();
    let (obs_fut, ack_fut) = spot_ack(&base_fut, [42; 16], "v-perp-42", venue::d(0, 0));
    account.apply_event(&ack_fut).unwrap();
    let fut_target = v::OrderTarget {
        order_id: obs_fut.order.order_id,
        expected_owner_revision: 1,
        original_intent_exec_epoch: base_fut.context.request.intent_exec_epoch,
        client_id: obs_fut.order.client_id.clone(),
        venue_order_id: obs_fut.order.venue_order_id.clone(),
    };

    // Admit Modify increasing Futures Buy 1 -> 4 and rotating client_id -> "fut-rot-cid".
    let v::CommandBody::New(ref orig_spec) = base_fut.body else {
        unreachable!()
    };
    let c_mod = modify_cmd(
        &base_fut,
        12,
        fut_target.clone(),
        orig_spec.side,
        venue::d(4, 0),
        orig_spec.price.unwrap(),
        Some("fut-rot-cid"),
    );
    let adm_mod = account.admit(&c_mod, 110).unwrap();
    let mod_intent = adm_mod.image().intent;
    assert!(account.venue_shadow(1).unwrap().atoms() < shadow_after_new.atoms());
    assert!(account.is_intent_active(mod_intent));

    // Order(Canceled, cum=0) arrives BEFORE Modify reply!
    let mut term_raw = terminal(c_new.command(), "0");
    if let v::VenueEventBody::Order(ref mut o) = term_raw.body {
        o.order.order_id = obs_fut.order.order_id;
        o.order.venue_order_id = obs_fut.order.venue_order_id.clone();
    }
    let term_ev = event(term_raw);
    account.apply_event(&term_ev).unwrap();
    assert!(!account.frozen());
    assert_eq!(account.order_pending_modify_intent(&k1), None);
    assert_eq!(
        account.order_superseded_modify_intent(&k1),
        Some(mod_intent)
    );
    assert!(!account.is_intent_active(mod_intent));
    // Incremental modify shadow hold rolled back, base order shadow hold preserved until snapshot:
    assert_eq!(account.venue_shadow(1).unwrap(), shadow_after_new);

    // Reusing "fut-rot-cid" on a brand-new order succeeds without Conflict because tentative alias rolled back!
    let mut reuse_fut = base_fut.clone();
    reuse_fut.context.request.intent_id = 13;
    reuse_fut.context.request.recovery_object += 13;
    if let v::CommandBody::New(ref mut s) = reuse_fut.body {
        s.client_id = venue::text("fut-rot-cid");
        s.quantity = v::Quantity::Base(venue::d(1, 0));
    }
    let c_reuse = canonical(reuse_fut);
    account.admit(&c_reuse, 115).unwrap();

    // Late ModifyRejected arrives for superseded Modify -> clean transition, no freeze!
    let mut rej_ev = venue::event();
    rej_ev.identity.event_id = venue::text("late-fut-mod-rej");
    rej_ev.identity.instrument_id = base_fut.context.instrument.instrument_id;
    rej_ev.identity.instrument_version = base_fut.context.instrument.instrument_version;
    rej_ev.identity.product = base_fut.context.request.product;
    rej_ev.body = v::VenueEventBody::ModifyRejected(Box::new(v::ModifyRejection {
        target: fut_target,
        amendment_id: None,
        intent: Some(mod_intent),
        new_client_id: Some(venue::text("fut-rot-cid")),
        venue_order_id: obs_fut.order.venue_order_id,
        code: Some(-5027),
        reason: v::TerminalReason::OrderNotOpen,
    }));
    let c_rej_ev = event(rej_ev);
    assert_eq!(account.apply_event(&c_rej_ev).unwrap(), Applied::New);
    assert!(!account.frozen());
    assert_eq!(
        account.order_last_modify_rejection(&k1),
        Some(v::TerminalReason::OrderNotOpen)
    );

    // Close fees and verify eviction reclaims both k1 and mod_intent!
    account.reconcile_fees(&k1, units(0)).unwrap();
    let evicted = account.evict_expired(DEFAULT_RETENTION_WINDOW_NS + 1_000);
    assert!(evicted >= 1);
    assert!(account.order(&k1).is_none());
    assert!(account.admission(mod_intent).is_none());

    // Part 2: Out-of-order Spot fills racing with in-flight Modify (qty 2->5, Buy price 10->10.50)
    // AND delayed fill at prior price 10.50 arriving after a second Modify tightens price to 10.00!
    let (mut s_snap, s_cat, s_base) = fixture();
    s_snap.assets[0].free = units(20);
    s_snap.assets[1].free = units(10_000);
    let mut spot_acc = Account::from_snapshot(s_snap.clone(), s_cat.clone()).unwrap();
    let mut records = Vec::new();
    let epoch = s_base.context.request.intent_exec_epoch;

    let c_s1 = spot_order_cmd(
        &s_base,
        10,
        "race-s1",
        v::Side::Buy,
        venue::d(2, 0),
        venue::d(1000, -2),
    );
    let adm_s1 = spot_acc.admit(&c_s1, 10).unwrap();
    let k_s1 = adm_s1.orders()[0].clone();
    records.push(RecoveryRecord::Admit {
        command: Box::new(c_s1.clone()),
        evaluated_at: 10,
        expected: adm_s1.image(),
    });

    let (_, ack_s1) = spot_ack(c_s1.command(), [71; 16], "v-race-1", venue::d(0, 0));
    spot_acc.apply_event(&ack_s1).unwrap();
    records.push(RecoveryRecord::VenueEvent(Box::new(ack_s1)));

    let c_smod1 = modify_cmd(
        &s_base,
        11,
        v::OrderTarget {
            order_id: [71; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: epoch,
            client_id: Some(venue::text("race-s1")),
            venue_order_id: Some(venue::text("v-race-1")),
        },
        v::Side::Buy,
        venue::d(5, 0),
        venue::d(1050, -2),
        None,
    );
    let adm_smod1 = spot_acc.admit(&c_smod1, 20).unwrap();
    records.push(RecoveryRecord::Admit {
        command: Box::new(c_smod1),
        evaluated_at: 20,
        expected: adm_smod1.image(),
    });

    // Partial fill of 3 @ 10.50 arrives BEFORE Modified(qty=5, price=10.50)!
    let mut f_race = fill(c_s1.command(), "tr-race-1", "3", "10.5", "31.5");
    if let v::VenueEventBody::Fill(ref mut fb) = f_race.body {
        fb.observation.order.order_id = [71; 16];
        fb.observation.order.venue_order_id = Some(venue::text("v-race-1"));
        fb.observation.original_quantity = Some(venue::d(5, 0));
        fb.observation.cumulative_quantity = Some(venue::d(3, 0));
        fb.observation.order_price = Some(venue::d(1050, -2));
    }
    let ev_f_race = event(f_race);
    spot_acc.apply_event(&ev_f_race).unwrap();
    records.push(RecoveryRecord::VenueEvent(Box::new(ev_f_race)));
    assert!(
        !spot_acc.frozen(),
        "fill at in-flight Modify price/qty must not freeze"
    );

    // Modified(qty=5, price=10.50) arrives, then Modify #2 tightens Buy price 10.50 -> 10.00 and confirms:
    let mut mod1_obs = venue::event();
    mod1_obs.identity.event_id = venue::text("ev-smod-1");
    mod1_obs.identity.product = s_base.context.request.product;
    mod1_obs.body = v::VenueEventBody::Modified(Box::new(v::ModifyObservation {
        target: v::OrderTarget {
            order_id: [71; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: epoch,
            client_id: Some(venue::text("race-s1")),
            venue_order_id: Some(venue::text("v-race-1")),
        },
        amendment_id: None,
        intent: Some(IntentKey {
            epoch,
            intent_id: 11,
        }),
        prior_client_id: Some(venue::text("race-s1")),
        new_client_id: Some(venue::text("race-s1")),
        venue_order_id: Some(venue::text("v-race-1")),
        new_quantity: venue::d(5, 0),
        new_price: venue::d(1050, -2),
        cumulative_filled: Some(venue::d(3, 0)),
    }));
    let ev_mod1 = event(mod1_obs);
    spot_acc.apply_event(&ev_mod1).unwrap();
    records.push(RecoveryRecord::VenueEvent(Box::new(ev_mod1)));

    let c_smod2 = modify_cmd(
        &s_base,
        12,
        v::OrderTarget {
            order_id: [71; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: epoch,
            client_id: Some(venue::text("race-s1")),
            venue_order_id: Some(venue::text("v-race-1")),
        },
        v::Side::Buy,
        venue::d(5, 0),
        venue::d(1000, -2),
        None,
    );
    let adm_smod2 = spot_acc.admit(&c_smod2, 30).unwrap();
    records.push(RecoveryRecord::Admit {
        command: Box::new(c_smod2),
        evaluated_at: 30,
        expected: adm_smod2.image(),
    });

    let mut mod2_obs = venue::event();
    mod2_obs.identity.event_id = venue::text("ev-smod-2");
    mod2_obs.identity.product = s_base.context.request.product;
    mod2_obs.body = v::VenueEventBody::Modified(Box::new(v::ModifyObservation {
        target: v::OrderTarget {
            order_id: [71; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: epoch,
            client_id: Some(venue::text("race-s1")),
            venue_order_id: Some(venue::text("v-race-1")),
        },
        amendment_id: None,
        intent: Some(IntentKey {
            epoch,
            intent_id: 12,
        }),
        prior_client_id: Some(venue::text("race-s1")),
        new_client_id: Some(venue::text("race-s1")),
        venue_order_id: Some(venue::text("v-race-1")),
        new_quantity: venue::d(5, 0),
        new_price: venue::d(1000, -2),
        cumulative_filled: Some(venue::d(4, 0)),
    }));
    let ev_mod2 = event(mod2_obs);
    spot_acc.apply_event(&ev_mod2).unwrap();
    records.push(RecoveryRecord::VenueEvent(Box::new(ev_mod2)));

    // Delayed trade report from pre-mod2 match at 10.50 arrives after price tightened to 10.00!
    let mut f_delayed = fill(c_s1.command(), "tr-race-2", "1", "10.5", "10.5");
    if let v::VenueEventBody::Fill(ref mut fb) = f_delayed.body {
        fb.observation.order.order_id = [71; 16];
        fb.observation.order.venue_order_id = Some(venue::text("v-race-1"));
        fb.observation.original_quantity = Some(venue::d(5, 0));
        fb.observation.cumulative_quantity = Some(venue::d(4, 0));
        fb.observation.order_price = Some(venue::d(1050, -2));
    }
    let ev_f_delayed = event(f_delayed);
    spot_acc.apply_event(&ev_f_delayed).unwrap();
    records.push(RecoveryRecord::VenueEvent(Box::new(ev_f_delayed)));
    assert!(
        !spot_acc.frozen(),
        "delayed fill at historical confirmed modify price must not freeze"
    );
    assert_eq!(spot_acc.order(&k_s1).unwrap().filled_quantity, units(4));

    let recovered = Account::recover(s_snap, s_cat, &records).unwrap();
    assert!(recovered.frozen());
    assert_eq!(recovered.asset(key(1)), spot_acc.asset(key(1)));
    assert_eq!(recovered.asset(key(2)), spot_acc.asset(key(2)));
    let mut expected_view = spot_acc.order(&k_s1).unwrap();
    expected_view.unknown = true;
    assert_eq!(recovered.order(&k_s1), Some(expected_view));
    assert_eq!(
        recovered.stp_book().best_bid_ticks(12).unwrap(),
        spot_acc.stp_book().best_bid_ticks(12).unwrap()
    );
}
