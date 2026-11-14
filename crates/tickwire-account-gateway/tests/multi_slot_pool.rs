mod common;
use common::*;
use std::{
    io::{self, Read, Write},
    net::{IpAddr, Ipv4Addr, TcpListener},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use tickwire_account::{Applied, DoubtReason, IntentKey, ManagerIdentity};
use tickwire_account_gateway::*;
use tickwire_transport::{
    Connection, IoBudget, Limits, Security,
    dialer::{DialBackoff, DialBaton, DialedLink},
    rustls,
};
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

fn dial_slot_peer(
    host: &str,
    max_accept: usize,
) -> (Connection, Limits, thread::JoinHandle<Vec<u8>>) {
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

fn dial_responding_peer(
    host: &str,
    response_bytes: Vec<u8>,
) -> (Connection, Limits, thread::JoinHandle<Vec<u8>>) {
    let cert = rcgen::generate_simple_self_signed(vec![host.into()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert.der().clone()], key.into())
            .unwrap(),
    );
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let tls = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let handle = thread::spawn(move || {
        let deadline = until();
        let socket = loop {
            match listener.accept() {
                Ok((s, _)) => break s,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() > deadline {
                        return Vec::new();
                    }
                    thread::yield_now();
                }
                Err(e) => panic!("{e}"),
            }
        };
        socket.set_nonblocking(false).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut stream =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server).unwrap(), socket);
        let mut out = Vec::new();
        let mut b = [0u8; 4096];
        if let Ok(n) = stream.read(&mut b)
            && n > 0
        {
            out.extend_from_slice(&b[..n]);
            let _ = stream.write_all(&response_bytes);
            let _ = stream.flush();
            stream.conn.send_close_notify();
            let _ = stream.flush();
        }
        out
    });
    let limits = Limits::default();
    let server_name = host.to_owned().try_into().unwrap();
    let conn = Connection::connect(
        address,
        Security::Tls {
            server_name,
            config: tls,
        },
        limits,
        until(),
    )
    .unwrap();
    (conn, limits, handle)
}

fn drive_slot_intent(g: &mut Gateway, conn_id: u16, key: IntentKey, wanted: Status) {
    let deadline = until();
    while g.status(key) != Some(wanted) {
        assert!(
            Instant::now() < deadline,
            "slot {conn_id} timed out waiting for {wanted:?}, got {:?}",
            g.status(key)
        );
        g.poll_slot(conn_id, Instant::now(), IoBudget::default())
            .unwrap();
        thread::yield_now();
    }
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

fn make_pooled(
    router_cfg: RouterConfig,
) -> (Directory, v::CanonicalCommand, Gateway, MockAuthority) {
    let dir = Directory::new();
    let (snap, mut cat, raw_base) = fixture::fixture();
    cat.entries[0]
        .strategy_limits
        .push((9, fixture::units(100)));
    let b = tickwire_account_codec::CanonicalBaseline::new(snap, cat).unwrap();
    let base_cmd = fixture::canonical(raw_base);
    let cfg = config(&b);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let tls = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth(),
    );
    let initial_conn = ConnectionConfig {
        address,
        tls,
        limits: Limits::default(),
        connect_deadline: until(),
    };
    let auth = MockAuthority::default();
    let g = AccountGateway::create(
        tickwire_account_journal::FileBackend::open(&dir.0).unwrap(),
        cfg,
        b,
        auth.clone(),
        TestEncoder,
        initial_conn,
    )
    .unwrap();
    let _ = listener.accept();
    let g = g.with_connection_pool(router_cfg).unwrap();
    (dir, base_cmd, g, auth)
}

#[test]
fn concurrent_exclusive_multi_connection_dispatch_round_robins_and_gates_inflight() {
    let (_dir, base_cmd, mut g, auth) = make_pooled(RouterConfig::default());
    auth.policy.lock().unwrap().gate.now = 200;
    assert!(g.has_connection_pool());
    assert!(g.per_order_doubt_isolation());
    assert!(!g.has_transport_connection());

    let mut peers = Vec::new();
    for expected_id in 0..3u16 {
        let (conn, limits, peer_handle) = dial_slot_peer("testnet.binance.vision", 16384);
        let cid = g
            .add_pool_slot(
                OrderSlotSpec::with_connection(SendSlot::Exclusive, conn),
                limits,
            )
            .unwrap();
        assert_eq!(cid, expected_id);
        peers.push(peer_handle);
    }

    // Submit 3 New orders concurrently before any HTTP response returns; router round-robins 0 -> 1 -> 2.
    let cmd0 = order_cmd(&base_cmd, 10, "pool-ex-0");
    let cmd1 = order_cmd(&base_cmd, 11, "pool-ex-1");
    let cmd2 = order_cmd(&base_cmd, 12, "pool-ex-2");
    let (key0, key1, key2) = (intent_of(&cmd0), intent_of(&cmd1), intent_of(&cmd2));

    let s0 = g.pick_pool_slot(0).unwrap();
    assert_eq!(s0, 0);
    let (st0, ep0) = g.submit_on_slot(s0, cmd0.clone(), 100, until()).unwrap();
    assert_eq!((st0, ep0), (Status::Queued, 1));

    let s1 = g.pick_pool_slot(0).unwrap();
    assert_eq!(s1, 1);
    let (st1, ep1) = g.submit_on_slot(s1, cmd1.clone(), 101, until()).unwrap();
    assert_eq!((st1, ep1), (Status::Queued, 1));

    let s2 = g.pick_pool_slot(0).unwrap();
    assert_eq!(s2, 2);
    let (st2, ep2) = g.submit_on_slot(s2, cmd2.clone(), 102, until()).unwrap();
    assert_eq!((st2, ep2), (Status::Queued, 1));

    // Sequential admission in AccountJournal (seq 1, 2, 3) and exact total reservation (3 * 60 = 180).
    assert_eq!(g.committed(key0).unwrap().receipt().sequence(), 1);
    assert_eq!(g.committed(key1).unwrap().receipt().sequence(), 2);
    assert_eq!(g.committed(key2).unwrap().receipt().sequence(), 3);
    assert_eq!(g.journal_position(), 3);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(180)
    );

    // Byte-identical retry precedes busy check and returns stored status + epoch.
    assert_eq!(
        g.submit_on_slot(0, cmd0.clone(), 103, until()).unwrap(),
        (Status::Queued, ep0)
    );

    // Drive all 3 slots concurrently to Status::Flushed while HTTP replies are still in-flight.
    let deadline = until();
    while [key0, key1, key2]
        .iter()
        .any(|k| g.status(*k) != Some(Status::Flushed))
    {
        assert!(Instant::now() < deadline, "timed out flushing 3 slots");
        for cid in 0..3u16 {
            g.poll_slot(cid, Instant::now(), IoBudget::default())
                .unwrap();
        }
        thread::yield_now();
    }

    for cid in 0..3u16 {
        assert_eq!(g.slot_pending_intent(cid), None);
        assert_eq!(g.slot_route(cid).unwrap().health.inflight(), 1);
    }

    // Attempting a 4th order while all 3 Exclusive slots still hold inflight == 1 returns None / Busy!
    let cmd3 = order_cmd(&base_cmd, 13, "pool-ex-3");
    let key3 = intent_of(&cmd3);
    assert_eq!(g.pick_pool_slot(0), None);
    assert!(matches!(
        g.submit_on_slot(0, cmd3.clone(), 104, until()),
        Err(Error::Busy)
    ));
    assert!(matches!(
        g.submit_on_slot(1, cmd3.clone(), 104, until()),
        Err(Error::Busy)
    ));
    assert_eq!(g.journal_position(), 3);
    assert_eq!(g.status(key3), None);

    // Releasing inflight on slot 1 with an RTT sample makes slot 1 immediately ready again!
    assert!(g.note_slot_sample(1, ep1, SampleKind::Order, 420).unwrap());
    assert!(g.release_slot_inflight(1, ep1).unwrap());
    assert_eq!(g.slot_route(1).unwrap().health.inflight(), 0);
    assert_eq!(g.slot_route(1).unwrap().health.ema_us(), 420);

    assert_eq!(g.pick_pool_slot(0), Some(1));
    let (st3, ep3) = g.submit_on_slot(1, cmd3, 105, until()).unwrap();
    assert_eq!((st3, ep3), (Status::Queued, 1));
    assert_eq!(g.committed(key3).unwrap().receipt().sequence(), 4);
    drive_slot_intent(&mut g, 1, key3, Status::Flushed);

    drop(g);
    let r0 = peers.remove(0).join().unwrap();
    let r1 = peers.remove(0).join().unwrap();
    let r2 = peers.remove(0).join().unwrap();
    assert!(!r0.is_empty());
    assert!(r1.len() >= r0.len() * 2);
    assert_eq!(r0.len(), r2.len());
}

#[test]
fn single_slot_mid_send_drop_isolates_order_and_preserves_siblings() {
    let (_dir, base_cmd, mut g, auth) = make_pooled(RouterConfig::default());
    auth.policy.lock().unwrap().gate.now = 200;

    // Slot 0 uses max_accept = 16 to deterministically stage a mid-send drop (accepted_bytes > 0).
    let (conn0, limits0, peer0) = dial_slot_peer("testnet.binance.vision", 16);
    let (conn1, limits1, peer1) = dial_slot_peer("testnet.binance.vision", 16384);
    let (conn2, limits2, peer2) = dial_slot_peer("testnet.binance.vision", 16384);
    assert_eq!(
        g.add_pool_slot(
            OrderSlotSpec::with_connection(SendSlot::Exclusive, conn0),
            limits0,
        )
        .unwrap(),
        0
    );
    assert_eq!(
        g.add_pool_slot(
            OrderSlotSpec::with_connection(SendSlot::Exclusive, conn1),
            limits1,
        )
        .unwrap(),
        1
    );
    assert_eq!(
        g.add_pool_slot(
            OrderSlotSpec::with_connection(SendSlot::Exclusive, conn2),
            limits2,
        )
        .unwrap(),
        2
    );

    let cmd0 = order_cmd(&base_cmd, 20, "slot0-drop");
    let cmd1 = order_cmd(&base_cmd, 21, "slot1-healthy");
    let cmd2 = order_cmd(&base_cmd, 22, "slot2-healthy");
    let (key0, key1, key2) = (intent_of(&cmd0), intent_of(&cmd1), intent_of(&cmd2));

    let (_, _ep0) = g.submit_on_slot(0, cmd0.clone(), 100, until()).unwrap();
    let (_, ep1) = g.submit_on_slot(1, cmd1, 101, until()).unwrap();
    let (_, ep2) = g.submit_on_slot(2, cmd2, 102, until()).unwrap();

    drive_slot_intent(&mut g, 0, key0, Status::PartiallyAccepted);
    assert!(g.output_progress(key0).unwrap().accepted_bytes > 0);
    drive_slot_intent(&mut g, 1, key1, Status::Flushed);
    drive_slot_intent(&mut g, 2, key2, Status::Flushed);

    let order0_key = g.committed(key0).unwrap().admission().orders()[0].clone();
    let order1_key = g.committed(key1).unwrap().admission().orders()[0].clone();
    let order2_key = g.committed(key2).unwrap().admission().orders()[0].clone();

    // Expire slot 0 mid-send -> isolates ONLY slot 0's order into PlaceAmbiguous!
    let _ = g.poll_slot(
        0,
        Instant::now() + Duration::from_secs(60),
        IoBudget::default(),
    );

    assert!(!g.halted());
    assert!(!g.account().frozen());
    assert!(!g.has_slot_connection(0));
    assert!(!g.slot_route(0).unwrap().established);
    assert_eq!(g.slot_route(0).unwrap().epoch, 2);
    assert!(g.has_slot_connection(1));
    assert!(g.has_slot_connection(2));

    assert_eq!(g.status(key0), Some(Status::Unknown));
    assert_eq!(g.status(key1), Some(Status::Flushed));
    assert_eq!(g.status(key2), Some(Status::Flushed));
    assert_eq!(
        g.account().order_in_doubt(&order0_key),
        Some(DoubtReason::PlaceAmbiguous)
    );
    assert_eq!(g.account().order_in_doubt(&order1_key), None);
    assert_eq!(g.account().order_in_doubt(&order2_key), None);

    // Slots 1 and 2 continue admitting and flushing new orders uninterrupted while slot 0 is down!
    assert!(g.release_slot_inflight(1, ep1).unwrap());
    assert!(g.release_slot_inflight(2, ep2).unwrap());

    let cmd3 = order_cmd(&base_cmd, 23, "slot1-next");
    let cmd4 = order_cmd(&base_cmd, 24, "slot2-next");
    let (key3, key4) = (intent_of(&cmd3), intent_of(&cmd4));

    let pick_a = g.pick_pool_slot(0).unwrap();
    let pick_b = {
        let (_, ep_a) = g.submit_on_slot(pick_a, cmd3, 110, until()).unwrap();
        let next = g.pick_pool_slot(0).unwrap();
        let (_, ep_b) = g.submit_on_slot(next, cmd4, 111, until()).unwrap();
        drive_slot_intent(&mut g, pick_a, key3, Status::Flushed);
        drive_slot_intent(&mut g, next, key4, Status::Flushed);
        assert!(g.release_slot_inflight(pick_a, ep_a).unwrap());
        assert!(g.release_slot_inflight(next, ep_b).unwrap());
        next
    };
    assert_ne!(pick_a, 0);
    assert_ne!(pick_b, 0);
    assert_ne!(pick_a, pick_b);

    // Reconnect slot 0 via install_pool_connection and immediately flush an order on slot 0!
    let (conn0_new, limits0_new, peer0_new) = dial_slot_peer("testnet.binance.vision", 16384);
    let new_ep0 = g
        .install_pool_connection(0, conn0_new, None, limits0_new)
        .unwrap();
    assert_eq!(new_ep0, 3);
    let cmd5 = order_cmd(&base_cmd, 25, "slot0-reconnected");
    let key5 = intent_of(&cmd5);
    g.submit_on_slot(0, cmd5, 120, until()).unwrap();
    drive_slot_intent(&mut g, 0, key5, Status::Flushed);

    // Authoritative Rejected for slot 0's ambiguous order releases its reservation cleanly!
    assert_eq!(
        g.record_ingress_event([8; 16], 1, rejected_for(&cmd0, "slot0-drop", 250))
            .unwrap(),
        Applied::New
    );
    assert_eq!(g.account().order_in_doubt(&order0_key), None);

    drop(g);
    let _ = peer0.join().unwrap();
    assert!(!peer1.join().unwrap().is_empty());
    assert!(!peer2.join().unwrap().is_empty());
    assert!(!peer0_new.join().unwrap().is_empty());
}

#[test]
fn pre_first_byte_gate_denial_aborts_and_releases_slot_inflight() {
    let (_dir, base_cmd, mut g, auth) = make_pooled(RouterConfig::default());
    auth.policy.lock().unwrap().gate.now = 200;

    let (conn0, limits0, peer0) = dial_slot_peer("testnet.binance.vision", 16384);
    let (conn1, limits1, peer1) = dial_slot_peer("testnet.binance.vision", 16384);
    g.add_pool_slot(
        OrderSlotSpec::with_connection(SendSlot::Exclusive, conn0),
        limits0,
    )
    .unwrap();
    g.add_pool_slot(
        OrderSlotSpec::with_connection(SendSlot::Exclusive, conn1),
        limits1,
    )
    .unwrap();

    // Complete TLS handshake on slot 1 before submitting so the next poll_slot hits try_send immediately.
    let deadline = until();
    while !g.slot_transport_ready(1) {
        assert!(Instant::now() < deadline, "slot 1 TLS handshake timed out");
        g.poll_slot(1, Instant::now(), IoBudget::default()).unwrap();
        thread::yield_now();
    }

    // Deny action_permitted right before first byte on slot 1.
    auth.policy.lock().unwrap().gate.action_permitted = false;

    let cmd1 = order_cmd(&base_cmd, 30, "gate-denied-slot1");
    let key1 = intent_of(&cmd1);
    let (st1, ep1) = g.submit_on_slot(1, cmd1, 100, until()).unwrap();
    assert_eq!((st1, ep1), (Status::Queued, 1));
    assert_eq!(g.journal_position(), 1);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(60)
    );
    assert_eq!(g.slot_route(1).unwrap().health.inflight(), 1);

    g.poll_slot(1, Instant::now(), IoBudget::default()).unwrap();

    // Zero bytes crossed TLS -> Aborted journaled, reservation released, and inflight == 0 on slot 1!
    assert_eq!(g.status(key1), Some(Status::Aborted));
    assert_eq!(g.journal_position(), 2);
    assert_eq!(
        g.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(0)
    );
    assert!(!g.halted());
    assert!(!g.account().frozen());
    assert!(g.has_slot_connection(1));
    assert_eq!(g.slot_route(1).unwrap().epoch, ep1);
    assert_eq!(g.slot_route(1).unwrap().health.inflight(), 0);
    assert_eq!(g.slot_pending_intent(1), None);

    // Re-enable gate and confirm slot 1 immediately admits and flushes a subsequent order on the same epoch!
    auth.policy.lock().unwrap().gate.action_permitted = true;
    let cmd2 = order_cmd(&base_cmd, 31, "gate-allowed-slot1");
    let key2 = intent_of(&cmd2);
    let (st2, ep2) = g.submit_on_slot(1, cmd2, 101, until()).unwrap();
    assert_eq!((st2, ep2), (Status::Queued, ep1));
    drive_slot_intent(&mut g, 1, key2, Status::Flushed);

    drop(g);
    let _ = peer0.join().unwrap();
    assert!(!peer1.join().unwrap().is_empty());
}

#[test]
fn shared_slot_multiplexes_up_to_sixteen_inflight_frames() {
    let (_dir, base_cmd, mut g, auth) = make_pooled(RouterConfig::default());
    auth.policy.lock().unwrap().gate.now = 300;

    let (conn0, limits0, peer0) = dial_slot_peer("testnet.binance.vision", 16384);
    let cid = g
        .add_pool_slot(
            OrderSlotSpec::with_connection(SendSlot::Shared, conn0),
            limits0,
        )
        .unwrap();
    assert_eq!(cid, 0);

    let small_order = |intent_id: u64, client_id: &str| {
        let mut cmd = base_cmd.command().clone();
        cmd.context.request.intent_id = intent_id;
        cmd.context.request.recovery_object += intent_id;
        cmd.context.request.deadline = 5_000;
        let v::CommandBody::New(ref mut o) = cmd.body else {
            unreachable!()
        };
        o.client_id = fixture::venue::text(client_id);
        o.price = Some("50000".parse().unwrap());
        fixture::canonical(cmd)
    };

    let mut frame_len = 0usize;
    for i in 0..u64::from(MAX_SHARED_INFLIGHT) {
        assert_eq!(g.pick_pool_slot(0), Some(0));
        let cmd = small_order(100 + i, &format!("ws-shared-{i}"));
        let key = intent_of(&cmd);
        let (st, ep) = g.submit_on_slot(0, cmd, 100 + i, until()).unwrap();
        assert_eq!((st, ep), (Status::Queued, 1));
        drive_slot_intent(&mut g, 0, key, Status::Flushed);
        frame_len = g.output_progress(key).unwrap().total_bytes;
        assert_eq!(g.slot_pending_intent(0), None);
        assert_eq!(g.slot_route(0).unwrap().health.inflight(), (i + 1) as u16);
    }

    // At inflight == 16, Shared slot is saturated until responses release inflight tokens.
    assert_eq!(
        g.slot_route(0).unwrap().health.inflight(),
        MAX_SHARED_INFLIGHT
    );
    assert_eq!(g.pick_pool_slot(0), None);
    let extra_cmd = small_order(200, "ws-shared-extra");
    let extra_key = intent_of(&extra_cmd);
    assert!(matches!(
        g.submit_on_slot(0, extra_cmd.clone(), 220, until()),
        Err(Error::Busy)
    ));

    // Release 3 responses -> inflight drops 16 -> 13, opening room for a 17th multiplexed frame!
    for expected in [15u16, 14, 13] {
        assert!(g.release_slot_inflight(0, 1).unwrap());
        assert_eq!(g.slot_route(0).unwrap().health.inflight(), expected);
    }

    assert_eq!(g.pick_pool_slot(0), Some(0));
    let (_, ep) = g.submit_on_slot(0, extra_cmd, 221, until()).unwrap();
    assert_eq!(ep, 1);
    drive_slot_intent(&mut g, 0, extra_key, Status::Flushed);
    assert_eq!(g.slot_route(0).unwrap().health.inflight(), 14);

    // Stale epoch release is a safe no-op.
    assert!(!g.release_slot_inflight(0, 999).unwrap());
    assert_eq!(g.slot_route(0).unwrap().health.inflight(), 14);

    for _ in 0..14 {
        assert!(g.release_slot_inflight(0, 1).unwrap());
    }
    assert_eq!(g.slot_route(0).unwrap().health.inflight(), 0);

    drop(g);
    let received = peer0.join().unwrap();
    assert_eq!(received.len(), frame_len * 17);
}

#[test]
fn pick_pool_slot_filtered_fails_over_across_local_ip_quotas() {
    let (_dir, _base_cmd, mut g, _auth) = make_pooled(RouterConfig::new(1, 4, 2));

    let ip0 = IpAddr::V4(Ipv4Addr::new(10, 0, 1, 1));
    let ip1 = IpAddr::V4(Ipv4Addr::new(10, 0, 1, 2));
    let ip2 = IpAddr::V4(Ipv4Addr::new(10, 0, 1, 3));
    let mut peers = Vec::new();
    for ip in [ip0, ip1, ip2] {
        let (conn, limits, peer_handle) = dial_slot_peer("testnet.binance.vision", 16384);
        let mut spec = OrderSlotSpec::with_connection(SendSlot::Exclusive, conn);
        spec.local_ip = Some(ip);
        g.add_pool_slot(spec, limits).unwrap();
        peers.push(peer_handle);
    }

    // Calibrate EWMA scores (ema_min_samples = 2): slot 0 = 500 us, slot 1 = 1200 us, slot 2 = 2000 us.
    for _ in 0..2 {
        assert!(g.note_slot_sample(0, 1, SampleKind::Order, 500).unwrap());
        assert!(g.note_slot_sample(1, 1, SampleKind::Order, 1200).unwrap());
        assert!(g.note_slot_sample(2, 1, SampleKind::Order, 2000).unwrap());
    }
    assert_eq!(g.slot_route(0).unwrap().health.score(), Some(500));
    assert_eq!(g.slot_route(1).unwrap().health.score(), Some(1200));
    assert_eq!(g.slot_route(2).unwrap().health.score(), Some(2000));

    // Unfiltered pick selects best EWMA slot 0 (500 us).
    assert_eq!(g.pick_pool_slot(0), Some(0));

    // Filtering out ip0 (e.g. ip0 exhausted weight / 429 cooldown) seamlessly fails over to slot 1 (1200 us)!
    assert_eq!(
        g.pick_pool_slot_filtered(0, |r| r.local_ip != Some(ip0)),
        Some(1)
    );

    // Filtering out both ip0 and ip1 selects slot 2 (2000 us); filtering all returns None.
    assert_eq!(
        g.pick_pool_slot_filtered(0, |r| r.local_ip == Some(ip2)),
        Some(2)
    );
    assert_eq!(g.pick_pool_slot_filtered(0, |_| false), None);

    // Verify 9/8 hysteresis when last_conn_id passes allow: bring slot 1 within 9/8 of 500 us (<= 562 us).
    for _ in 0..16 {
        g.note_slot_sample(1, 1, SampleKind::Order, 510).unwrap();
    }
    assert!(g.slot_route(1).unwrap().health.ema_us() <= 562);
    assert_eq!(
        g.pick_pool_slot_filtered(0, |r| r.local_ip == Some(ip1)),
        Some(1)
    );
    // Now last_conn_id == Some(1), and slot 1 passes allow and is within 9/8 of slot 0 -> sticks to slot 1!
    assert_eq!(g.pick_pool_slot_filtered(0, |_| true), Some(1));

    drop(g);
    for p in peers {
        let _ = p.join().unwrap();
    }
}

#[test]
fn pool_baton_harvest_retained_receive_and_unsent_strategy_termination() {
    let (_dir, base_cmd, mut g, auth) = make_pooled(RouterConfig::default());
    auth.policy.lock().unwrap().gate.now = 200;

    // 1. Harvest a background DialBaton link on slot 0 via poll_pool_dials without blocking.
    let baton = Arc::new(DialBaton::new());
    let slot0 = g
        .add_pool_slot(
            OrderSlotSpec::with_baton(SendSlot::Exclusive, baton.clone(), DialBackoff::new(10, 50)),
            Limits::default(),
        )
        .unwrap();
    assert_eq!(slot0, 0);
    assert!(!g.slot_route(0).unwrap().established);

    assert_eq!(g.poll_pool_dials(1_000), 0);
    assert!(baton.claim(1_000));
    let http_reply = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}".to_vec();
    let (dialed_conn, _limits, peer0) =
        dial_responding_peer("testnet.binance.vision", http_reply.clone());
    let peer_addr = dialed_conn.peer_addr_target();
    assert!(baton.deliver_ready(DialedLink {
        connection: dialed_conn,
        peer: peer_addr,
        connect_rtt_us: 310,
    }));
    assert_eq!(g.poll_pool_dials(1_100), 1);
    assert!(g.slot_route(0).unwrap().established);

    // Add slot 1 with a second strategy's unsent order to verify targeted terminate_unsent_strategy.
    let (conn1, limits1, peer1) = dial_slot_peer("testnet.binance.vision", 16384);
    g.add_pool_slot(
        OrderSlotSpec::with_connection(SendSlot::Exclusive, conn1),
        limits1,
    )
    .unwrap();

    let cmd_s1 = order_cmd(&base_cmd, 50, "strat-1-unsent");
    let key_s1 = intent_of(&cmd_s1);
    let mut raw_s2 = base_cmd.command().clone();
    raw_s2.context.strategy.logical_strategy_id = 9;
    raw_s2.context.strategy.instance_id = [9; 16];
    raw_s2.context.request.intent_id = 51;
    raw_s2.context.request.recovery_object += 51;
    raw_s2.context.request.deadline = 5_000;
    if let v::CommandBody::New(ref mut o) = raw_s2.body {
        o.client_id = fixture::venue::text("strat-2-keep");
    }
    let cmd_s2 = fixture::canonical(raw_s2);
    let key_s2 = intent_of(&cmd_s2);

    g.submit_on_slot(0, cmd_s1.clone(), 120, until()).unwrap();
    g.submit_on_slot(1, cmd_s2, 121, until()).unwrap();

    let ctx1 = &cmd_s1.command().context;
    assert!(
        g.terminate_unsent_strategy(
            ctx1.strategy.logical_strategy_id,
            ManagerIdentity {
                instance: ctx1.strategy.instance_id,
                generation: ctx1.request.strategy_generation,
            },
        )
        .unwrap()
    );
    assert_eq!(g.status(key_s1), Some(Status::Aborted));
    assert_eq!(g.slot_route(0).unwrap().health.inflight(), 0);
    assert_eq!(g.status(key_s2), Some(Status::Queued));
    assert_eq!(g.slot_route(1).unwrap().health.inflight(), 1);

    // 2. Submit and flush a fresh order on slot 0, then read HTTP response from retained_receive after peer EOF!
    let cmd_live = order_cmd(&base_cmd, 52, "slot0-eof-reply");
    let key_live = intent_of(&cmd_live);
    g.submit_on_slot(0, cmd_live, 130, until()).unwrap();
    drive_slot_intent(&mut g, 0, key_live, Status::Flushed);

    let deadline = until();
    let mut rx = Vec::new();
    let mut buf = [0u8; 256];
    while rx.len() < http_reply.len() {
        assert!(Instant::now() < deadline, "timed out reading slot 0 reply");
        if g.has_slot_connection(0) {
            let _ = g.poll_slot(0, Instant::now(), IoBudget::default());
        }
        let n = g.read_slot(0, &mut buf);
        if n > 0 {
            rx.extend_from_slice(&buf[..n]);
        } else {
            thread::yield_now();
        }
    }
    assert_eq!(rx, http_reply);
    g.drain_slot_retained_receive(0);

    drop(g);
    assert!(!peer0.join().unwrap().is_empty());
    let _ = peer1.join().unwrap();
}

#[test]
fn poll_pool_dials_preserves_unread_eof_retained_receive_until_drained() {
    let (_dir, base_cmd, mut g, auth) = make_pooled(RouterConfig::default());
    auth.policy.lock().unwrap().gate.now = 200;

    let baton = Arc::new(DialBaton::new());
    let http_reply = b"HTTP/1.1 200 OK\r\nContent-Length: 15\r\n\r\n{\"orderId\":777}".to_vec();
    let (conn0, limits0, peer0) =
        dial_responding_peer("testnet.binance.vision", http_reply.clone());
    let mut spec0 = OrderSlotSpec::with_connection(SendSlot::Exclusive, conn0);
    spec0.baton = Some(Arc::clone(&baton));
    spec0.backoff = DialBackoff::new(0, 10);
    assert_eq!(g.add_pool_slot(spec0, limits0).unwrap(), 0);
    assert_eq!(g.slot_route(0).unwrap().epoch, 1);

    let cmd = order_cmd(&base_cmd, 77, "eof-retain-guard");
    let key = intent_of(&cmd);
    g.submit_on_slot(0, cmd, 150, until()).unwrap();
    drive_slot_intent(&mut g, 0, key, Status::Flushed);

    // Wait until peer sends response + close_notify and poll_slot moves unread bytes into retained_receive.
    let deadline = until();
    while g.has_slot_connection(0) {
        assert!(Instant::now() < deadline, "timed out waiting for peer EOF");
        let _ = g.poll_slot(0, Instant::now(), IoBudget::default());
        thread::yield_now();
    }
    assert_eq!(g.slot_route(0).unwrap().epoch, 2);

    // Background dialer immediately delivers a fresh Ready connection onto baton BEFORE read_slot drains retained_receive!
    let _ = baton.claim(300);
    let (conn_re, _limits_re, peer_re) = dial_slot_peer("testnet.binance.vision", 16384);
    let re_peer = conn_re.peer_addr_target();
    assert!(baton.deliver_ready(DialedLink {
        connection: conn_re,
        peer: re_peer,
        connect_rtt_us: 120,
    }));

    // poll_pool_dials MUST NOT harvest the Ready baton or clobber retained_receive while unread bytes remain!
    assert_eq!(g.poll_pool_dials(400), 0);
    assert!(!g.has_slot_connection(0));

    let mut rx = Vec::new();
    let mut chunk = [0u8; 16];
    loop {
        let n = g.read_slot(0, &mut chunk);
        if n == 0 {
            break;
        }
        rx.extend_from_slice(&chunk[..n]);
    }
    assert_eq!(rx, http_reply);

    // Now that retained_receive is empty, poll_pool_dials harvests the Ready link immediately!
    assert_eq!(g.poll_pool_dials(401), 1);
    assert!(g.has_slot_connection(0));
    assert_eq!(g.slot_route(0).unwrap().epoch, 3);

    drop(g);
    assert!(!peer0.join().unwrap().is_empty());
    let _ = peer_re.join().unwrap();
}
