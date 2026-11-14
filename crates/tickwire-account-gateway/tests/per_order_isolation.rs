mod common;
use common::*;
use std::time::{Duration, Instant};
use tickwire_account::{Applied, DoubtReason, IntentKey};
use tickwire_account_gateway::*;
use tickwire_transport::{Connection, IoBudget, Security};
use tickwire_venue as v;

fn order_cmd(base: &v::CanonicalCommand, intent_id: u64, client_id: &str) -> v::CanonicalCommand {
    let mut cmd = base.command().clone();
    cmd.context.request.intent_id = intent_id;
    cmd.context.request.recovery_object += intent_id;
    cmd.context.request.deadline = 5_000;
    let v::CommandBody::New(ref mut o) = cmd.body else {
        unreachable!()
    };
    o.client_id = fixture::venue::text(client_id);
    fixture::canonical(cmd)
}

fn intent_of(cmd: &v::CanonicalCommand) -> IntentKey {
    IntentKey {
        epoch: cmd.command().context.request.intent_exec_epoch,
        intent_id: cmd.command().context.request.intent_id,
    }
}

fn drive_intent(g: &mut Gateway, key: IntentKey, wanted: Status) {
    let deadline = until();
    while g.status(key) != Some(wanted) {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {wanted:?}, got {:?}",
            g.status(key)
        );
        g.poll(Instant::now(), IoBudget::default()).unwrap();
        std::thread::yield_now();
    }
}

fn dial_replacement(
    host: &str,
    max_accept: usize,
) -> (
    Connection,
    tickwire_transport::Limits,
    std::thread::JoinHandle<Vec<u8>>,
) {
    let (cfg, handle) = peer(host, max_accept);
    let server_name = host.to_owned().try_into().unwrap();
    let conn = Connection::connect(
        cfg.address,
        Security::Tls {
            server_name,
            config: cfg.tls,
        },
        cfg.limits,
        cfg.connect_deadline,
    )
    .unwrap();
    (conn, cfg.limits, handle)
}

fn rejected_for(cmd: &v::CanonicalCommand, client_id: &str, mono_ns: u64) -> v::CanonicalEvent {
    let mut ev = fixture::venue::event();
    ev.identity.product = cmd.command().context.request.product;
    ev.identity.instrument_id = cmd.command().context.instrument.instrument_id;
    ev.identity.received_mono_ns = mono_ns;
    ev.body = v::VenueEventBody::Rejected {
        client_id: Some(fixture::venue::text(client_id)),
        venue_code: fixture::venue::text("-2010"),
        message: v::BoundedMessage::new("Order rejected by venue").unwrap(),
    };
    fixture::event(ev)
}

fn ack_for(
    cmd: &v::CanonicalCommand,
    order_id: [u8; 16],
    venue_id: &str,
    mono_ns: u64,
) -> v::CanonicalEvent {
    let mut ev = fixture::fill(cmd.command(), "unused", "0.001", "60000", "60");
    ev.identity.received_mono_ns = mono_ns;
    let v::VenueEventBody::Fill(ref mut f) = ev.body else {
        unreachable!()
    };
    f.observation.order.order_id = order_id;
    f.observation.order.venue_order_id = Some(fixture::venue::text(venue_id));
    f.observation.status = v::OrderStatus::New;
    f.observation.cumulative_quantity = Some(fixture::venue::d(0, 0));
    ev.body = v::VenueEventBody::Order(Box::new(f.observation.clone()));
    fixture::event(ev)
}

#[test]
fn default_gateway_halts_and_marks_flushed_siblings_unknown_when_isolation_disabled() {
    let (_dir, base_cmd, mut g, _auth, peer1) = make(false, 16);
    assert!(!g.per_order_doubt_isolation());

    let cmd0 = order_cmd(&base_cmd, 10, "sib-0");
    let key0 = intent_of(&cmd0);
    g.submit(cmd0, 60, until()).unwrap();
    drive_intent(&mut g, key0, Status::Flushed);

    let cmd1 = order_cmd(&base_cmd, 11, "sib-1");
    let key1 = intent_of(&cmd1);
    g.submit(cmd1, 61, until()).unwrap();
    drive_intent(&mut g, key1, Status::PartiallyAccepted);
    g.cancel_pending().unwrap();

    assert!(g.halted());
    assert!(g.account().frozen());
    assert_eq!(g.status(key0), Some(Status::Unknown));
    assert_eq!(g.status(key1), Some(Status::Unknown));

    drop(g);
    let _ = peer1.join().unwrap();
}

#[test]
fn per_order_isolation_preserves_flushed_sibling_reconnects_admits_new_order_and_closes_rejected() {
    let (_dir, base_cmd, g, auth, peer1) = make(false, 16);
    auth.policy.lock().unwrap().gate.now = 200;
    let mut g = g
        .with_per_order_doubt_isolation(true)
        .with_retention_window_ns(Some(1_000));
    assert!(g.per_order_doubt_isolation());
    assert!(g.has_transport_connection());

    // 1. Order 0 submits and flushes cleanly.
    let cmd0 = order_cmd(&base_cmd, 10, "iso-order-0");
    let key0 = intent_of(&cmd0);
    g.submit(cmd0.clone(), 100, until()).unwrap();
    drive_intent(&mut g, key0, Status::Flushed);
    let order0_key = g.committed(key0).unwrap().admission().orders()[0].clone();
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );

    // 2. Order 1 accepts partial bytes (MayHaveSent) and then suffers a transport send failure.
    let cmd1 = order_cmd(&base_cmd, 11, "iso-order-1");
    let key1 = intent_of(&cmd1);
    g.submit(cmd1.clone(), 110, until()).unwrap();
    drive_intent(&mut g, key1, Status::PartiallyAccepted);
    assert!(g.output_progress(key1).unwrap().accepted_bytes > 0);
    let order1_key = g.committed(key1).unwrap().admission().orders()[0].clone();

    // Poll past a send-expiry timestamp or cancel mid-stream -> SendState::Failed routes to drop_broken_transport_isolated!
    let _ = g.poll(
        Instant::now() + Duration::from_secs(60),
        IoBudget::default(),
    );

    // Only Order 1 is isolated in doubt; Order 0 stays Flushed, and Gateway + Account stay live & unfrozen!
    assert!(!g.halted());
    assert!(!g.account().frozen());
    assert!(!g.has_transport_connection());
    assert_eq!(g.status(key0), Some(Status::Flushed));
    assert_eq!(g.status(key1), Some(Status::Unknown));
    assert_eq!(g.account().order_in_doubt(&order0_key), None);
    assert_eq!(
        g.account().order_in_doubt(&order1_key),
        Some(DoubtReason::PlaceAmbiguous)
    );
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(120)
    );

    // Submitting before replacement transport is installed returns Busy without halting or mutating Account.
    let cmd2 = order_cmd(&base_cmd, 12, "iso-order-2");
    let key2 = intent_of(&cmd2);
    assert!(matches!(
        g.submit(cmd2.clone(), 120, until()),
        Err(Error::Busy)
    ));
    assert!(!g.halted());
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(120)
    );

    // 3. Install a fresh replacement TLS connection and immediately admit & flush Order 2 while Order 1 stays locked!
    let (conn2, limits2, peer2) = dial_replacement("testnet.binance.vision", 16384);
    g.install_transport_connection(conn2, limits2).unwrap();
    assert!(g.has_transport_connection());

    g.submit(cmd2, 120, until()).unwrap();
    drive_intent(&mut g, key2, Status::Flushed);
    assert_eq!(g.status(key0), Some(Status::Flushed));
    assert_eq!(g.status(key1), Some(Status::Unknown));
    assert_eq!(g.status(key2), Some(Status::Flushed));
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(180)
    );

    // Eviction never reclaims the isolated Unknown / in-doubt Order 1 regardless of elapsed time.
    let _ = g.evict_retired_entries(50_000);
    assert_eq!(g.status(key1), Some(Status::Unknown));
    assert!(g.account().order(&order1_key).is_some());

    // 4. VenueEventBody::Rejected closes Order 1 as uncreated and releases its 60-unit reservation cleanly!
    assert_eq!(
        g.record_ingress_event([7; 16], 1, rejected_for(&cmd1, "iso-order-1", 200))
            .unwrap(),
        Applied::New
    );
    assert!(!g.halted());
    assert!(!g.account().frozen());
    assert_eq!(g.account().order_in_doubt(&order1_key), None);
    let view1 = g.account().order(&order1_key).unwrap();
    assert!(view1.uncreated);
    assert!(view1.fee_reconciled);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(120)
    );

    drop(g);
    let _ = peer1.join().unwrap();
    assert!(!peer2.join().unwrap().is_empty());
}

#[test]
fn per_order_isolation_aborts_unsent_and_isolates_cancel_amend_query_while_structures_halt() {
    let (_dir, base_cmd, g, auth, peer1) = make(false, 16);
    auth.policy.lock().unwrap().gate.now = 200;
    let mut g = g.with_per_order_doubt_isolation(true);

    // 1. Zero-byte unsent drop aborts cleanly and refunds reservation without freezing.
    let unsent = order_cmd(&base_cmd, 20, "unsent-0");
    let unsent_key = intent_of(&unsent);
    assert_eq!(g.submit(unsent, 60, until()).unwrap(), Status::Queued);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );
    assert_eq!(g.drop_broken_transport_isolated(), Some(unsent_key));
    assert!(!g.halted());
    assert!(!g.account().frozen());
    assert_eq!(g.status(unsent_key), Some(Status::Aborted));
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(0)
    );

    // 2. Admit, flush, and ack a resting order, then isolate Query, AmendKeepPriority, and Cancel mid-send!
    let (conn2, limits2, peer2) = dial_replacement("testnet.binance.vision", 16);
    g.install_transport_connection(conn2, limits2).unwrap();

    let live_order = order_cmd(&base_cmd, 21, "live-order-21");
    let live_key = intent_of(&live_order);
    g.submit(live_order.clone(), 70, until()).unwrap();
    drive_intent(&mut g, live_key, Status::Flushed);
    let okey = g.committed(live_key).unwrap().admission().orders()[0].clone();
    let oid = [21u8; 16];
    g.record_event(ack_for(&live_order, oid, "v-live-21", 75))
        .unwrap();

    let target = v::OrderTarget {
        order_id: oid,
        expected_owner_revision: 1,
        original_intent_exec_epoch: live_order.command().context.request.intent_exec_epoch,
        client_id: Some(fixture::venue::text("live-order-21")),
        venue_order_id: Some(fixture::venue::text("v-live-21")),
    };

    // 2a. Query mid-send failure marks Query Unknown without marking the order in doubt.
    let mut query_raw = live_order.command().clone();
    query_raw.context.request.intent_id = 22;
    query_raw.context.request.recovery_object += 22;
    query_raw.body = v::CommandBody::Query(target.clone());
    let query_cmd = fixture::canonical(query_raw);
    let query_key = intent_of(&query_cmd);
    g.submit(query_cmd, 80, until()).unwrap();
    drive_intent(&mut g, query_key, Status::PartiallyAccepted);
    g.cancel_pending().unwrap();
    assert!(!g.halted() && !g.account().frozen());
    assert_eq!(g.status(query_key), Some(Status::Unknown));
    assert_eq!(g.account().order_in_doubt(&okey), None);

    // 2b. AmendKeepPriority mid-send failure marks target order AmendAmbiguous.
    let (conn3, limits3, peer3) = dial_replacement("testnet.binance.vision", 16);
    g.install_transport_connection(conn3, limits3).unwrap();
    let mut amend_raw = live_order.command().clone();
    amend_raw.context.request.intent_id = 23;
    amend_raw.context.request.recovery_object += 23;
    amend_raw.body = v::CommandBody::AmendKeepPriority {
        target: target.clone(),
        new_quantity: fixture::venue::d(5, -4),
        new_client_id: Some(fixture::venue::text("live-order-21")),
    };
    let amend_cmd = fixture::canonical(amend_raw);
    let amend_key = intent_of(&amend_cmd);
    g.submit(amend_cmd.clone(), 85, until()).unwrap();
    drive_intent(&mut g, amend_key, Status::PartiallyAccepted);
    g.cancel_pending().unwrap();
    assert!(!g.halted() && !g.account().frozen());
    assert_eq!(g.status(amend_key), Some(Status::Unknown));
    assert_eq!(
        g.account().order_in_doubt(&okey),
        Some(DoubtReason::AmendAmbiguous)
    );

    // Resolve AmendAmbiguous via AmendmentRejected, then test Cancel mid-send failure -> CancelAmbiguous.
    let mut amend_rej = fixture::venue::event();
    amend_rej.body = v::VenueEventBody::AmendmentRejected(Box::new(v::AmendmentRejection {
        order: v::OrderIdentity {
            order_id: oid,
            original_intent_id: 21,
            original_intent_exec_epoch: live_order.command().context.request.intent_exec_epoch,
            client_id: Some(fixture::venue::text("live-order-21")),
            venue_order_id: Some(fixture::venue::text("v-live-21")),
            parent_order_id: None,
            list_id: None,
            venue_algo_id: None,
            venue_list_id: None,
            list_client_id: None,
            algo_client_id: None,
        },
        request_intent_id: 23,
        request_intent_exec_epoch: amend_cmd.command().context.request.intent_exec_epoch,
        request_digest: amend_cmd.digest(),
        venue_code: fixture::venue::text("-1000"),
    }));
    g.record_event(fixture::event(amend_rej)).unwrap();
    assert_eq!(g.account().order_in_doubt(&okey), None);

    let (conn4, limits4, peer4) = dial_replacement("testnet.binance.vision", 16);
    g.install_transport_connection(conn4, limits4).unwrap();
    let mut cancel_raw = live_order.command().clone();
    cancel_raw.context.request.intent_id = 24;
    cancel_raw.context.request.recovery_object += 24;
    cancel_raw.body = v::CommandBody::Cancel(target);
    let cancel_cmd = fixture::canonical(cancel_raw);
    let cancel_key = intent_of(&cancel_cmd);
    g.submit(cancel_cmd, 90, until()).unwrap();
    drive_intent(&mut g, cancel_key, Status::PartiallyAccepted);
    g.cancel_pending().unwrap();
    assert!(!g.halted() && !g.account().frozen());
    assert_eq!(g.status(cancel_key), Some(Status::Unknown));
    assert_eq!(
        g.account().order_in_doubt(&okey),
        Some(DoubtReason::CancelAmbiguous)
    );

    // 3. Multi-leg SpotList mid-send failure falls back to full halt() + account.frozen()!
    let (conn5, limits5, peer5) = dial_replacement("testnet.binance.vision", 16);
    g.install_transport_connection(conn5, limits5).unwrap();
    let mut list_raw = base_cmd.command().clone();
    list_raw.context.request.intent_id = 30;
    list_raw.context.request.recovery_object += 30;
    list_raw.context.request.deadline = 5_000;
    let v::CommandBody::New(mut leg1) = list_raw.body.clone() else {
        unreachable!()
    };
    leg1.client_id = fixture::venue::text("list-leg-1");
    let mut leg2 = leg1.clone();
    leg2.client_id = fixture::venue::text("list-leg-2");
    list_raw.body = v::CommandBody::SpotList(v::ListOrder {
        list_id: [91; 16],
        client_id: fixture::venue::text("spot-list-30"),
        kind: v::ListKind::Oto,
        legs: v::BoundedList::new(vec![leg1, leg2]).unwrap(),
    });
    let list_cmd = fixture::canonical(list_raw);
    let list_key = intent_of(&list_cmd);
    g.submit(list_cmd, 95, until()).unwrap();
    drive_intent(&mut g, list_key, Status::PartiallyAccepted);
    g.cancel_pending().unwrap();
    assert!(g.halted());
    assert!(g.account().frozen());

    drop(g);
    for p in [peer1, peer2, peer3, peer4, peer5] {
        let _ = p.join().unwrap();
    }
}

#[test]
fn futures_per_order_isolation_locks_and_releases_shadow_hold_across_reconnect() {
    let (_dir, base_cmd, g, auth, peer1) = make(true, 16);
    auth.policy.lock().unwrap().gate.now = 200;
    let mut g = g.with_per_order_doubt_isolation(true);
    let initial_shadow = g.account().venue_shadow(1).unwrap();

    // Order 1 stalls mid-send -> isolated in PlaceAmbiguous with shadow hold retained.
    let cmd1 = order_cmd(&base_cmd, 40, "fut-iso-1");
    let key1 = intent_of(&cmd1);
    g.submit(cmd1.clone(), 100, until()).unwrap();
    drive_intent(&mut g, key1, Status::PartiallyAccepted);
    let shadow_after_1 = g.account().venue_shadow(1).unwrap();
    assert!(shadow_after_1.atoms() < initial_shadow.atoms());
    let order1_key = g.committed(key1).unwrap().admission().orders()[0].clone();

    let _ = g.poll(
        Instant::now() + Duration::from_secs(60),
        IoBudget::default(),
    );
    assert!(!g.halted() && !g.account().frozen());
    assert_eq!(g.status(key1), Some(Status::Unknown));
    assert_eq!(
        g.account().order_in_doubt(&order1_key),
        Some(DoubtReason::PlaceAmbiguous)
    );
    assert_eq!(g.account().venue_shadow(1).unwrap(), shadow_after_1);

    // Reconnect and immediately flush sibling Futures Order 2.
    let (conn2, limits2, peer2) = dial_replacement("demo-fapi.binance.com", 16384);
    g.install_transport_connection(conn2, limits2).unwrap();
    let cmd2 = order_cmd(&base_cmd, 41, "fut-iso-2");
    let key2 = intent_of(&cmd2);
    g.submit(cmd2, 110, until()).unwrap();
    drive_intent(&mut g, key2, Status::Flushed);
    let shadow_after_2 = g.account().venue_shadow(1).unwrap();
    assert!(shadow_after_2.atoms() < shadow_after_1.atoms());

    // Authoritative Rejected for Order 1 releases Order 1's shadow hold while Order 2 remains open.
    assert_eq!(
        g.record_event(rejected_for(&cmd1, "fut-iso-1", 150))
            .unwrap(),
        Applied::New
    );
    assert!(!g.halted() && !g.account().frozen());
    assert_eq!(g.account().order_in_doubt(&order1_key), None);
    assert!(g.account().venue_shadow(1).unwrap().atoms() > shadow_after_2.atoms());

    drop(g);
    let _ = peer1.join().unwrap();
    assert!(!peer2.join().unwrap().is_empty());
}

#[test]
fn per_order_isolation_isolates_native_modify_to_amend_ambiguous_and_exposes_stage4_apis() {
    let (_dir, base_cmd, g, auth, peer1) = make(false, 16);
    auth.policy.lock().unwrap().gate.now = 200;
    let mut g = g.with_per_order_doubt_isolation(true);

    g.set_stp_enforcement(true);
    assert!(g.stp_enforcement());
    g.set_projected_fill(true);
    assert!(g.projected_fill_enabled());

    // 1. Place and acknowledge Order 0 (resting Buy 0.001 BTC @ 60000 = 60 USDT) and sibling Order 1.
    let (conn1, limits1, peer2) = dial_replacement("testnet.binance.vision", 16384);
    let _ = g.drop_broken_transport_isolated();
    g.install_transport_connection(conn1, limits1).unwrap();

    let cmd0 = order_cmd(&base_cmd, 50, "mod-iso-0");
    let key0 = intent_of(&cmd0);
    g.submit(cmd0.clone(), 100, until()).unwrap();
    drive_intent(&mut g, key0, Status::Flushed);
    let order0_key = g.committed(key0).unwrap().admission().orders()[0].clone();
    let oid0 = [50u8; 16];
    g.record_event(ack_for(&cmd0, oid0, "v-mod-50", 105))
        .unwrap();
    assert_eq!(g.stp_book().best_bid_ticks(12).unwrap(), Some(6_000_000));

    let cmd1 = order_cmd(&base_cmd, 51, "mod-sib-1");
    let key1 = intent_of(&cmd1);
    g.submit(cmd1, 106, until()).unwrap();
    drive_intent(&mut g, key1, Status::Flushed);

    // ProjectedFill pass-through on Gateway works without touching settled reserves.
    let pf = g
        .on_cum_qty(
            &order0_key,
            fixture::f(40_000),
            ProjectedPriceInput::LimitFallback,
        )
        .unwrap()
        .unwrap();
    assert_eq!(pf.gap_quantity, fixture::f(40_000));
    assert_eq!(
        g.order_projected_filled(&order0_key),
        Some(fixture::f(40_000))
    );
    assert_eq!(g.pop_projected_fill(), Some(pf));
    assert!(g.drain_projected_fills().is_empty());

    // 2. Switch to a small-budget peer so CommandBody::Modify stalls mid-send -> AmendAmbiguous!
    let (conn2, limits2, peer3) = dial_replacement("testnet.binance.vision", 16);
    let _ = g.drop_broken_transport_isolated();
    g.install_transport_connection(conn2, limits2).unwrap();

    let target0 = v::OrderTarget {
        order_id: oid0,
        expected_owner_revision: 1,
        original_intent_exec_epoch: cmd0.command().context.request.intent_exec_epoch,
        client_id: Some(fixture::venue::text("mod-iso-0")),
        venue_order_id: Some(fixture::venue::text("v-mod-50")),
    };
    let mut mod_raw = cmd0.command().clone();
    mod_raw.context.request.intent_id = 52;
    mod_raw.context.request.recovery_object += 52;
    mod_raw.body = v::CommandBody::Modify(v::ModifyOrder {
        target: target0.clone(),
        new_client_id: Some(fixture::venue::text("mod-iso-0-r1")),
        side: v::Side::Buy,
        quantity: fixture::venue::d(15, -4),
        price: fixture::venue::d(60000, 0),
        position_side: v::PositionSide::Both,
        reduce_only: false,
    });
    let mod_cmd = fixture::canonical(mod_raw);
    let mod_key = intent_of(&mod_cmd);
    g.submit(mod_cmd, 110, until()).unwrap();
    drive_intent(&mut g, mod_key, Status::PartiallyAccepted);
    g.cancel_pending().unwrap();

    // Only Order 0 is marked AmendAmbiguous; Gateway & Account stay live and sibling Order 1 stays Flushed!
    assert!(!g.halted());
    assert!(!g.account().frozen());
    assert_eq!(g.status(key1), Some(Status::Flushed));
    assert_eq!(g.status(mod_key), Some(Status::Unknown));
    assert_eq!(
        g.account().order_in_doubt(&order0_key),
        Some(DoubtReason::AmendAmbiguous)
    );
    // Worst-case hold (0.0015 * 60000 = 90 + 60 sibling = 150 USDT) stays locked until venue resolution!
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(150)
    );

    // 3. VenueEventBody::ModifyRejected resolves AmendAmbiguous, releases +30 USDT hold, and records reason!
    let mut mod_rej = fixture::venue::event();
    mod_rej.identity.received_mono_ns = 150;
    mod_rej.body = v::VenueEventBody::ModifyRejected(Box::new(v::ModifyRejection {
        target: target0,
        amendment_id: Some(fixture::venue::text("rej-52")),
        intent: Some(mod_key),
        new_client_id: Some(fixture::venue::text("mod-iso-0-r1")),
        venue_order_id: Some(fixture::venue::text("v-mod-50")),
        code: Some(-5022),
        reason: v::TerminalReason::PostOnlyWouldTake,
    }));
    assert_eq!(
        g.record_event(fixture::event(mod_rej)).unwrap(),
        Applied::New
    );
    assert_eq!(g.account().order_in_doubt(&order0_key), None);
    assert_eq!(
        g.order_last_modify_rejection(&order0_key),
        Some(v::TerminalReason::PostOnlyWouldTake)
    );
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(120)
    );

    drop(g);
    for p in [peer1, peer2, peer3] {
        let _ = p.join().unwrap();
    }
}
