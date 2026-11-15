use std::{
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_transport::{
    Connection, Error, IoBudget, Limits, PreparedSend, Security, SendState,
    dialer::{DialBackoff, DialSecurity, DialSpec, DialState, DialTransport, Dialer, IpPool},
    mono_now_ns,
    order_router::{
        MAX_ORDER_CONNECTIONS, MAX_SHARED_INFLIGHT, OrderConnectionPool, OrderRouter,
        OrderSlotSpec, RouteHealth, RouterConfig, SampleKind, SendSlot, UNMEASURED_SCORE,
    },
};

fn test_deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

#[test]
fn router_config_validation_bounds_and_defaults() {
    let def = RouterConfig::default();
    assert_eq!(def.ema_new, 1);
    assert_eq!(def.ema_den, 8);
    assert_eq!(def.ema_min_samples, 8);
    assert!(def.validate().is_ok());

    for invalid in [
        RouterConfig::new(1, 0, 8),
        RouterConfig::new(1, 1, 8),
        RouterConfig::new(0, 8, 8),
        RouterConfig::new(8, 8, 8),
        RouterConfig::new(9, 8, 8),
        RouterConfig::new(1, 8, 0),
    ] {
        assert!(
            matches!(invalid.validate(), Err(Error::Bounds)),
            "expected Error::Bounds for {invalid:?}"
        );
        assert!(matches!(OrderRouter::new(invalid), Err(Error::Bounds)));
        assert!(matches!(
            OrderConnectionPool::new(invalid),
            Err(Error::Bounds)
        ));
    }

    for valid in [
        RouterConfig::new(1, 2, 1),
        RouterConfig::new(3, 8, 4),
        RouterConfig::new(1, 32, 16),
        RouterConfig::new(65_534, 65_535, 65_535),
    ] {
        assert!(valid.validate().is_ok());
        assert!(OrderRouter::new(valid).is_ok());
    }
}

#[test]
fn exact_integer_ewma_progression_sample_kinds_and_u16_saturation() {
    let cfg = RouterConfig::default(); // 1/8 weight, min 8 samples
    let mut h = RouteHealth::new(cfg.ema_min_samples);
    assert_eq!(h.ema_us(), 0);
    assert_eq!(h.ema_samples(), 0);
    assert_eq!(h.ema_min_samples(), 8);
    assert_eq!(h.inflight(), 0);
    assert_eq!(h.score(), None);
    assert_eq!(h.raw_score(), UNMEASURED_SCORE);
    assert!(!h.is_measured());

    // 1st sample stored verbatim, still unmeasured (1 < 8).
    h.note_sample(1_000, cfg);
    assert_eq!(h.ema_us(), 1_000);
    assert_eq!(h.ema_samples(), 1);
    assert_eq!(h.score(), None);
    assert_eq!(h.raw_score(), UNMEASURED_SCORE);

    // 2nd..7th samples follow exact integer floor division: (old * 7 + sample) / 8.
    let mut expected = 1_000u32;
    for step in 2..=7u16 {
        let sample = 200u32 + u32::from(step) * 3;
        expected = ((u64::from(expected) * 7 + u64::from(sample)) / 8) as u32;
        h.note_sample(sample, cfg);
        assert_eq!(h.ema_us(), expected);
        assert_eq!(h.ema_samples(), step);
        assert_eq!(h.score(), None);
        assert_eq!(h.raw_score(), UNMEASURED_SCORE);
    }

    // 8th sample reaches ema_min_samples -> measured!
    expected = ((u64::from(expected) * 7 + 500) / 8) as u32;
    h.note_sample(500, cfg);
    assert_eq!(h.ema_samples(), 8);
    assert!(h.is_measured());
    assert_eq!(h.score(), Some(expected));
    assert_eq!(h.raw_score(), expected);

    // Verify exact integer floor (never nearest-integer float rounding):
    // (100 * 7 + 107) / 8 = 807 / 8 = 100; (100 * 7 + 108) / 8 = 808 / 8 = 101.
    let mut floor_check = RouteHealth::new(1);
    floor_check.note_sample(100, cfg);
    floor_check.note_sample(107, cfg);
    assert_eq!(floor_check.score(), Some(100));
    floor_check.note_sample(108, cfg);
    assert_eq!(floor_check.score(), Some(101));

    // Verify large u32::MAX values do not overflow intermediate u64 math.
    let big_cfg = RouterConfig::new(65_534, 65_535, 2);
    let mut big_health = RouteHealth::new(2);
    big_health.note_sample(u32::MAX, big_cfg);
    big_health.note_sample(u32::MAX, big_cfg);
    assert_eq!(big_health.score(), Some(u32::MAX));

    // Verify saturation at u16::MAX without wrapping back to 0 / UNMEASURED.
    let mut sat = RouteHealth {
        ema_us: 250,
        ema_samples: u16::MAX - 1,
        ema_min_samples: 8,
        inflight: 0,
    };
    sat.note_sample(250, cfg);
    assert_eq!(sat.ema_samples(), u16::MAX);
    sat.note_sample(250, cfg);
    assert_eq!(sat.ema_samples(), u16::MAX);
    assert_eq!(sat.score(), Some(250));

    // Verify SampleKind behavior and that score NEVER multiplies ema_us by inflight.
    assert!(SampleKind::Order.updates_route_ewma());
    assert!(SampleKind::Ping.updates_route_ewma());
    assert!(SampleKind::App.updates_route_ewma());
    assert!(!SampleKind::Tcping.updates_route_ewma());

    let mut router = OrderRouter::new(RouterConfig::new(1, 8, 3)).unwrap();
    router.register_slot(0, None, SendSlot::Shared, 0).unwrap();
    let ep = router.on_connected(0, None, None).unwrap();
    assert!(router.note_sample(0, ep, SampleKind::Order, 400).unwrap());
    assert!(router.note_sample(0, ep, SampleKind::Ping, 400).unwrap());
    assert!(
        !router
            .note_sample(0, ep, SampleKind::Tcping, 9_999)
            .unwrap()
    );
    assert_eq!(router.route(0).unwrap().health.ema_samples(), 2);
    assert_eq!(router.route(0).unwrap().health.score(), None);

    assert!(router.note_sample(0, ep, SampleKind::App, 400).unwrap());
    for _ in 0..5 {
        assert_eq!(router.acquire_inflight(0).unwrap(), ep);
    }
    assert_eq!(router.route(0).unwrap().health.inflight(), 5);
    assert_eq!(router.route(0).unwrap().health.score(), Some(400));
}

#[test]
fn warmup_round_robin_skips_busy_exclusive_and_product_mismatched_links() {
    let mut router = OrderRouter::new(RouterConfig::default()).unwrap();
    for id in 0..4u16 {
        let slot_kind = if id == 1 {
            SendSlot::Exclusive
        } else {
            SendSlot::Shared
        };
        // Slot 0: wildcard (0), Slot 1: 0b01 (Spot), Slot 2: 0b10 (Perp), Slot 3: 0b11 (Both).
        let mask = match id {
            0 => 0,
            1 => 0b01,
            2 => 0b10,
            _ => 0b11,
        };
        router.register_slot(id, None, slot_kind, mask).unwrap();
        router.on_connected(id, None, None).unwrap();
    }

    // sym_type == 0 matches all 4 established unmeasured connections: 0 -> 1 -> 2 -> 3 -> 0.
    assert_eq!(router.pick(0, None, false), Some(0));
    assert_eq!(router.pick_next(0, false), Some(0));
    assert_eq!(router.pick_next(0, false), Some(1));
    assert_eq!(router.pick_next(0, false), Some(2));
    assert_eq!(router.pick_next(0, false), Some(3));
    assert_eq!(router.pick_next(0, false), Some(0));

    // Acquire Exclusive slot 1 (inflight == 1 -> !is_ready()): round-robin skips slot 1!
    let ep1 = router.acquire_inflight(1).unwrap();
    assert!(!router.route(1).unwrap().is_ready());
    assert_eq!(router.pick(0, Some(0), false), Some(2));
    assert_eq!(router.pick(0, Some(2), false), Some(3));
    assert_eq!(router.pick(0, Some(3), false), Some(0));
    // With force == true, slot 1 is still considered despite inflight == 1.
    assert_eq!(router.pick(0, Some(0), true), Some(1));

    // Release slot 1 and filter by sym_type = 0b10 (Perp): matches slots 0 (wildcard), 2, and 3 (skips 1).
    assert!(router.release_inflight(1, ep1).unwrap());
    assert_eq!(router.pick(0b10, Some(0), false), Some(2));
    assert_eq!(router.pick(0b10, Some(2), false), Some(3));
    assert_eq!(router.pick(0b10, Some(3), false), Some(0));

    // Unknown product bit only matches wildcard slot 0.
    assert_eq!(router.pick(0b1000, Some(0), false), Some(0));
}

#[test]
fn transition_to_measured_beats_unmeasured_and_breaks_ties_by_lower_conn_id() {
    let mut router = OrderRouter::new(RouterConfig::default()).unwrap();
    let mut epochs = [0u64; 4];
    for id in 0..4u16 {
        router.register_slot(id, None, SendSlot::Shared, 0).unwrap();
        epochs[usize::from(id)] = router.on_connected(id, None, None).unwrap();
    }

    // Feed 7 samples to all 4 routes -> still unmeasured.
    for id in 0..4u16 {
        for _ in 0..7 {
            router
                .note_sample(id, epochs[usize::from(id)], SampleKind::Order, 600)
                .unwrap();
        }
    }
    assert_eq!(router.pick(0, Some(0), false), Some(1));

    // Slot 2 receives its 8th sample first -> immediately beats all UNMEASURED links!
    router
        .note_sample(2, epochs[2], SampleKind::Order, 600)
        .unwrap();
    assert_eq!(router.route(2).unwrap().health.score(), Some(600));
    for last in [None, Some(0), Some(1), Some(2), Some(3)] {
        assert_eq!(router.pick(0, last, false), Some(2));
    }

    // Measure slot 3 at 350 us and slot 1 at 350 us: exact tie breaks to lower conn_id (1) when last is None or unmeasured (0).
    let mut router2 = OrderRouter::new(RouterConfig::default()).unwrap();
    let ep1 = {
        router2.register_slot(1, None, SendSlot::Shared, 0).unwrap();
        router2.on_connected(1, None, None).unwrap()
    };
    let ep3 = {
        router2.register_slot(3, None, SendSlot::Shared, 0).unwrap();
        router2.on_connected(3, None, None).unwrap()
    };
    router2.register_slot(0, None, SendSlot::Shared, 0).unwrap();
    router2.on_connected(0, None, None).unwrap();

    for _ in 0..8 {
        router2.note_sample(1, ep1, SampleKind::Order, 350).unwrap();
        router2.note_sample(3, ep3, SampleKind::Order, 350).unwrap();
    }
    assert_eq!(router2.pick(0, None, false), Some(1));
    assert_eq!(router2.pick(0, Some(0), false), Some(1));
    // When last is already 3 (equal score 350), 9/8 hysteresis keeps 3 instead of flapping!
    assert_eq!(router2.pick(0, Some(3), false), Some(3));
}

#[test]
fn hysteresis_nine_eighths_exact_boundary_800_vs_900_and_901() {
    let cfg = RouterConfig::new(1, 8, 1);
    let mut router = OrderRouter::new(cfg).unwrap();
    router
        .register_slot(0, None, SendSlot::Exclusive, 0)
        .unwrap();
    router
        .register_slot(1, None, SendSlot::Exclusive, 0)
        .unwrap();
    let ep0 = router.on_connected(0, None, None).unwrap();
    let ep1 = router.on_connected(1, None, None).unwrap();

    // best = 800 us (slot 0), last = 900 us (slot 1): 900 * 8 == 7200 <= 800 * 9 == 7200 -> keeps slot 1!
    router.note_sample(0, ep0, SampleKind::Order, 800).unwrap();
    router.note_sample(1, ep1, SampleKind::Order, 900).unwrap();
    assert_eq!(router.pick(0, Some(1), false), Some(1));

    // Reconnect slot 1 and set score = 901 us: 901 * 8 == 7208 > 7200 -> switches to slot 0!
    let ep1_next = router.on_connected(1, None, None).unwrap();
    router
        .note_sample(1, ep1_next, SampleKind::Order, 901)
        .unwrap();
    assert_eq!(router.pick(0, Some(1), false), Some(0));

    // Even inside the band (last = 850 us <= 900 us), if slot 1 is busy (Exclusive inflight == 1), pick switches to slot 0!
    let ep1_third = router.on_connected(1, None, None).unwrap();
    router
        .note_sample(1, ep1_third, SampleKind::Order, 850)
        .unwrap();
    assert_eq!(router.pick(0, Some(1), false), Some(1));
    router.acquire_inflight(1).unwrap();
    assert_eq!(router.pick(0, Some(1), false), Some(0));
}

#[test]
fn send_slot_exclusive_and_shared_inflight_caps_and_slot_limits() {
    assert_eq!(SendSlot::Exclusive.max_inflight(), 1);
    assert_eq!(SendSlot::Shared.max_inflight(), MAX_SHARED_INFLIGHT);
    assert_eq!(MAX_SHARED_INFLIGHT, 16);
    assert_eq!(MAX_ORDER_CONNECTIONS, 16);

    let mut router = OrderRouter::new(RouterConfig::default()).unwrap();
    assert!(matches!(
        router.register_slot(MAX_ORDER_CONNECTIONS as u16, None, SendSlot::Shared, 0),
        Err(Error::Bounds)
    ));

    router
        .register_slot(0, None, SendSlot::Exclusive, 0)
        .unwrap();
    router.register_slot(1, None, SendSlot::Shared, 0).unwrap();
    assert!(matches!(
        router.register_slot(0, None, SendSlot::Exclusive, 0),
        Err(Error::Busy)
    ));

    // Disconnected slot rejects acquire_inflight with Error::Closed.
    assert!(matches!(router.acquire_inflight(0), Err(Error::Closed)));

    let ep0 = router.on_connected(0, None, None).unwrap();
    let ep1 = router.on_connected(1, None, None).unwrap();

    // Exclusive cap == 1.
    assert!(router.route(0).unwrap().is_ready());
    assert_eq!(router.acquire_inflight(0).unwrap(), ep0);
    assert!(!router.route(0).unwrap().is_ready());
    assert!(matches!(router.acquire_inflight(0), Err(Error::Busy)));
    assert!(router.release_inflight(0, ep0).unwrap());
    assert!(router.route(0).unwrap().is_ready());

    // Shared cap == 16: ready at 0..15, not ready at 16, 17th returns Error::Busy.
    for count in 1..=MAX_SHARED_INFLIGHT {
        assert!(router.route(1).unwrap().is_ready());
        assert_eq!(router.acquire_inflight(1).unwrap(), ep1);
        assert_eq!(router.route(1).unwrap().health.inflight(), count);
    }
    assert!(!router.route(1).unwrap().is_ready());
    assert!(matches!(router.acquire_inflight(1), Err(Error::Busy)));

    assert!(router.release_inflight(1, ep1).unwrap());
    assert_eq!(router.route(1).unwrap().health.inflight(), 15);
    assert!(router.route(1).unwrap().is_ready());
}

#[test]
fn disconnect_health_reset_and_epoch_fencing_against_late_events() {
    let local_ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let peer_1: SocketAddr = "127.0.0.1:9101".parse().unwrap();
    let peer_2: SocketAddr = "127.0.0.1:9102".parse().unwrap();

    let mut router = OrderRouter::new(RouterConfig::default()).unwrap();
    router
        .register_slot(0, Some(local_ip), SendSlot::Shared, 0)
        .unwrap();

    let epoch_1 = router.on_connected(0, None, Some(peer_1)).unwrap();
    assert_eq!(epoch_1, 1);
    assert_eq!(router.route(0).unwrap().local_ip, Some(local_ip));
    assert_eq!(router.route(0).unwrap().peer_addr, Some(peer_1));

    for _ in 0..8 {
        assert!(
            router
                .note_sample(0, epoch_1, SampleKind::Order, 220)
                .unwrap()
        );
    }
    assert_eq!(router.acquire_inflight(0).unwrap(), epoch_1);
    assert_eq!(router.acquire_inflight(0).unwrap(), epoch_1);
    assert_eq!(router.route(0).unwrap().health.score(), Some(220));
    assert_eq!(router.route(0).unwrap().health.inflight(), 2);

    // Disconnect zeroes ema_us, ema_samples, inflight, clears peer_addr, retains slot local_ip, bumps epoch.
    let epoch_disc = router.on_disconnect(0).unwrap();
    assert!(epoch_disc > epoch_1);
    let r_down = router.route(0).unwrap();
    assert!(!r_down.established);
    assert_eq!(r_down.peer_addr, None);
    assert_eq!(r_down.local_ip, Some(local_ip));
    assert_eq!(r_down.health.ema_us(), 0);
    assert_eq!(r_down.health.ema_samples(), 0);
    assert_eq!(r_down.health.inflight(), 0);
    assert_eq!(r_down.health.score(), None);

    // Late sample/release while down is fenced.
    assert!(
        !router
            .note_sample(0, epoch_1, SampleKind::Order, 90)
            .unwrap()
    );
    assert!(!router.release_inflight(0, epoch_1).unwrap());

    // Reconnect starts cleanly in UNMEASURED with inflight == 0 and a higher epoch.
    let epoch_2 = router.on_connected(0, None, Some(peer_2)).unwrap();
    assert!(epoch_2 > epoch_disc);
    assert_eq!(router.acquire_inflight(0).unwrap(), epoch_2);
    assert_eq!(router.route(0).unwrap().health.inflight(), 1);
    assert_eq!(router.route(0).unwrap().health.score(), None);

    // Late release_inflight or note_sample from pre-disconnect epoch_1 or epoch_disc MUST NOT touch epoch_2!
    assert!(!router.release_inflight(0, epoch_1).unwrap());
    assert!(!router.release_inflight(0, epoch_disc).unwrap());
    assert!(
        !router
            .note_sample(0, epoch_1, SampleKind::Order, 50)
            .unwrap()
    );
    assert!(
        !router
            .note_sample(0, epoch_disc, SampleKind::Ping, 50)
            .unwrap()
    );

    let r_live = router.route(0).unwrap();
    assert_eq!(r_live.health.inflight(), 1);
    assert_eq!(r_live.health.ema_samples(), 0);
    assert_eq!(r_live.health.score(), None);
    assert_eq!(r_live.peer_addr, Some(peer_2));
}

#[test]
fn pool_ip_pool_lease_never_double_releases_or_leaks() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let ip_pool = Arc::new(Mutex::new(IpPool::new()));
    ip_pool.lock().unwrap().add_ip(addr);

    let leased_0 = ip_pool.lock().unwrap().take_ip().unwrap();
    let leased_1 = ip_pool.lock().unwrap().take_ip().unwrap();
    assert_eq!(leased_0, addr);
    assert_eq!(leased_1, addr);
    assert_eq!(ip_pool.lock().unwrap().entry(addr).unwrap().in_use, 2);

    let c0 =
        Connection::connect(addr, Security::Plain, Limits::default(), test_deadline()).unwrap();
    let c1 =
        Connection::connect(addr, Security::Plain, Limits::default(), test_deadline()).unwrap();
    let (_s0, _) = listener.accept().unwrap();
    let (_s1, _) = listener.accept().unwrap();

    let mut pool = OrderConnectionPool::new(RouterConfig::default()).unwrap();
    let id0 = pool
        .add_slot(OrderSlotSpec {
            send_slot: SendSlot::Exclusive,
            connection: Some(c0),
            peer_addr: Some(addr),
            ip_pool: Some(Arc::clone(&ip_pool)),
            ..OrderSlotSpec::default()
        })
        .unwrap();
    let id1 = pool
        .add_slot(OrderSlotSpec {
            send_slot: SendSlot::Exclusive,
            connection: Some(c1),
            peer_addr: Some(addr),
            ip_pool: Some(Arc::clone(&ip_pool)),
            ..OrderSlotSpec::default()
        })
        .unwrap();
    assert_eq!((id0, id1), (0, 1));
    assert_eq!(pool.established_count(), 2);

    // Disconnecting slot 0 twice must release ONLY slot 0's lease (in_use: 2 -> 1 -> 1).
    pool.disconnect_slot(0, 1_000, false).unwrap();
    assert_eq!(ip_pool.lock().unwrap().entry(addr).unwrap().in_use, 1);
    pool.disconnect_slot(0, 2_000, false).unwrap();
    assert_eq!(ip_pool.lock().unwrap().entry(addr).unwrap().in_use, 1);

    // Dropping the pool releases slot 1's remaining lease (in_use: 1 -> 0).
    drop(pool);
    assert_eq!(ip_pool.lock().unwrap().entry(addr).unwrap().in_use, 0);
}

#[test]
fn loopback_tcp_dialer_and_order_connection_pool_concurrent_flush_and_auto_reconnect() {
    let listener_0 = TcpListener::bind("127.0.0.1:0").unwrap();
    let listener_1 = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr_0 = listener_0.local_addr().unwrap();
    let addr_1 = listener_1.local_addr().unwrap();

    let stop_srv = Arc::new(AtomicBool::new(false));
    let stop_0 = Arc::clone(&stop_srv);

    // Server 0 accepts an initial connection (reads "slot0-a", replies "ack0-a"),
    // then accepts the automatic reconnect after slot 0 is dropped (reads "slot0-b", replies "ack0-b").
    let srv_0 = thread::spawn(move || {
        let (mut s1, from1) = listener_0.accept().unwrap();
        assert_eq!(from1.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        s1.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut msg1 = [0u8; 7];
        s1.read_exact(&mut msg1).unwrap();
        assert_eq!(&msg1, b"slot0-a");
        s1.write_all(b"ack0-a").unwrap();

        let (mut s2, from2) = listener_0.accept().unwrap();
        assert_eq!(from2.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        s2.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut msg2 = [0u8; 7];
        s2.read_exact(&mut msg2).unwrap();
        assert_eq!(&msg2, b"slot0-b");
        s2.write_all(b"ack0-b").unwrap();

        while !stop_0.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(5));
        }
    });

    // Server 1 stays up uninterrupted, handling "slot1-a" and "slot1-b" while slot 0 reconnects.
    let stop_1 = Arc::clone(&stop_srv);
    let srv_1 = thread::spawn(move || {
        let (mut s, from) = listener_1.accept().unwrap();
        assert_eq!(from.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

        let mut msg1 = [0u8; 7];
        s.read_exact(&mut msg1).unwrap();
        assert_eq!(&msg1, b"slot1-a");
        s.write_all(b"ack1-a").unwrap();

        let mut msg2 = [0u8; 7];
        s.read_exact(&mut msg2).unwrap();
        assert_eq!(&msg2, b"slot1-b");
        s.write_all(b"ack1-b").unwrap();

        while !stop_1.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(5));
        }
    });

    let dialer = Dialer::new().unwrap();
    let backoff = DialBackoff::new(10, 100);
    let make_spec = |addr: SocketAddr| DialSpec {
        host: "127.0.0.1".to_owned(),
        port: addr.port(),
        initial_peers: vec![addr],
        local_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        nic: None,
        security: DialSecurity::Plain,
        limits: Limits::default(),
        transport: DialTransport::PlainOrTls,
        connect_timeout: Duration::from_secs(2),
        dial_timeout: Duration::from_secs(2),
        backoff,
    };

    let baton_0 = dialer.add_slot(100, make_spec(addr_0)).unwrap();
    let baton_1 = dialer.add_slot(101, make_spec(addr_1)).unwrap();
    assert_eq!(baton_0.state(), DialState::Idle);
    assert_eq!(baton_1.state(), DialState::Idle);

    let mut pool = OrderConnectionPool::new(RouterConfig::default()).unwrap();
    let id_0 = pool
        .add_slot(OrderSlotSpec {
            local_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            send_slot: SendSlot::Shared,
            baton: Some(Arc::clone(&baton_0)),
            backoff,
            ..OrderSlotSpec::default()
        })
        .unwrap();
    let id_1 = pool
        .add_slot(OrderSlotSpec {
            local_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            send_slot: SendSlot::Shared,
            baton: Some(Arc::clone(&baton_1)),
            backoff,
            ..OrderSlotSpec::default()
        })
        .unwrap();
    assert_eq!((id_0, id_1), (0, 1));
    assert_eq!(pool.slot_count(), 2);
    assert_eq!(pool.established_count(), 0);
    assert!(!pool.has_ready_capacity(0));

    // Wait for poll_dials to request background connects and harvest both Ready connections.
    let until = test_deadline();
    while pool.established_count() < 2 {
        assert!(
            Instant::now() < until,
            "timed out establishing initial pool"
        );
        pool.poll_dials(mono_now_ns());
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(pool.ready_count(0), 2);
    assert!(pool.has_ready_capacity(0));

    let mut reactor = mio::Poll::new().unwrap();
    let mut events = mio::Events::with_capacity(16);
    pool.connection_mut(0)
        .unwrap()
        .register(reactor.registry(), mio::Token(10))
        .unwrap();
    pool.connection_mut(1)
        .unwrap()
        .register(reactor.registry(), mio::Token(11))
        .unwrap();

    // Warmup round-robin picks slot 0 then slot 1; flush both outbound frames concurrently.
    let (picked_a, ep_0_initial) = pool.pick_and_acquire(0).unwrap().unwrap();
    let (picked_b, ep_1_initial) = pool.pick_and_acquire(0).unwrap().unwrap();
    assert_eq!(picked_a, 0);
    assert_eq!(picked_b, 1);

    pool.connection_mut(0)
        .unwrap()
        .queue(PreparedSend::new(b"slot0-a", test_deadline(), 64).unwrap())
        .unwrap();
    pool.connection_mut(1)
        .unwrap()
        .queue(PreparedSend::new(b"slot1-a", test_deadline(), 64).unwrap())
        .unwrap();

    let mut got_0 = Vec::new();
    let mut got_1 = Vec::new();
    while got_0.len() < 6 || got_1.len() < 6 {
        assert!(
            Instant::now() < until,
            "timed out flushing concurrent frames"
        );
        for cid in [0u16, 1u16] {
            let conn = pool.connection_mut(cid).unwrap();
            if conn.send_report().is_some() {
                let rep = conn.try_send(Instant::now(), || true).unwrap();
                if rep.state == SendState::Flushed {
                    let _ = conn.take_finished();
                }
            }
            conn.drive(Instant::now(), IoBudget::default()).unwrap();
            let mut tmp = [0u8; 32];
            let n = conn.read(&mut tmp);
            if n > 0 {
                if cid == 0 {
                    got_0.extend_from_slice(&tmp[..n]);
                } else {
                    got_1.extend_from_slice(&tmp[..n]);
                }
            }
        }
        reactor
            .poll(&mut events, Some(Duration::from_millis(2)))
            .unwrap();
    }
    assert_eq!(&got_0, b"ack0-a");
    assert_eq!(&got_1, b"ack1-a");

    assert!(
        pool.note_sample(0, ep_0_initial, SampleKind::Order, 180)
            .unwrap()
    );
    assert!(pool.release_inflight(0, ep_0_initial).unwrap());
    assert!(
        pool.note_sample(1, ep_1_initial, SampleKind::Order, 190)
            .unwrap()
    );
    assert!(pool.release_inflight(1, ep_1_initial).unwrap());

    // Drop slot 0 with schedule_redial = true; slot 1 stays up and serves traffic immediately!
    let _ = pool
        .connection_mut(0)
        .unwrap()
        .deregister(reactor.registry());
    let ep_0_disc = pool.disconnect_slot(0, mono_now_ns(), true).unwrap();
    assert!(ep_0_disc > ep_0_initial);
    assert_eq!(pool.established_count(), 1);
    assert_eq!(pool.ready_count(0), 1);
    assert_eq!(pool.pick_ready(0), Some(1));

    let (picked_during_down, ep_1_during) = pool.pick_and_acquire(0).unwrap().unwrap();
    assert_eq!(picked_during_down, 1);
    assert_eq!(ep_1_during, ep_1_initial);
    pool.connection_mut(1)
        .unwrap()
        .queue(PreparedSend::new(b"slot1-b", test_deadline(), 64).unwrap())
        .unwrap();
    let mut got_1b = Vec::new();
    while got_1b.len() < 6 {
        assert!(
            Instant::now() < until,
            "timed out on healthy sibling slot 1"
        );
        let conn = pool.connection_mut(1).unwrap();
        if conn.send_report().is_some() {
            let rep = conn.try_send(Instant::now(), || true).unwrap();
            if rep.state == SendState::Flushed {
                let _ = conn.take_finished();
            }
        }
        conn.drive(Instant::now(), IoBudget::default()).unwrap();
        let mut tmp = [0u8; 32];
        let n = conn.read(&mut tmp);
        if n > 0 {
            got_1b.extend_from_slice(&tmp[..n]);
        }
        reactor
            .poll(&mut events, Some(Duration::from_millis(2)))
            .unwrap();
    }
    assert_eq!(&got_1b, b"ack1-b");
    assert!(pool.release_inflight(1, ep_1_during).unwrap());

    // Meanwhile "tw-dialer" automatically reconnects slot 0 in the background; harvest via poll_dials!
    while pool.established_count() < 2 {
        assert!(
            Instant::now() < until,
            "timed out waiting for slot 0 auto-reconnect"
        );
        pool.poll_dials(mono_now_ns());
        thread::sleep(Duration::from_millis(2));
    }

    let route_0_reconnected = pool.route(0).unwrap();
    assert!(route_0_reconnected.established);
    assert!(route_0_reconnected.epoch > ep_0_disc);
    assert_eq!(route_0_reconnected.health.ema_samples(), 0);
    assert_eq!(route_0_reconnected.health.inflight(), 0);
    assert_eq!(route_0_reconnected.health.score(), None);

    // Register reconnected slot 0 and exchange a frame to prove the new socket works end-to-end.
    pool.connection_mut(0)
        .unwrap()
        .register(reactor.registry(), mio::Token(12))
        .unwrap();
    let ep_0_new = pool.router_mut().acquire_inflight(0).unwrap();
    pool.connection_mut(0)
        .unwrap()
        .queue(PreparedSend::new(b"slot0-b", test_deadline(), 64).unwrap())
        .unwrap();
    let mut got_0b = Vec::new();
    while got_0b.len() < 6 {
        assert!(Instant::now() < until, "timed out on reconnected slot 0");
        let conn = pool.connection_mut(0).unwrap();
        if conn.send_report().is_some() {
            let rep = conn.try_send(Instant::now(), || true).unwrap();
            if rep.state == SendState::Flushed {
                let _ = conn.take_finished();
            }
        }
        conn.drive(Instant::now(), IoBudget::default()).unwrap();
        let mut tmp = [0u8; 32];
        let n = conn.read(&mut tmp);
        if n > 0 {
            got_0b.extend_from_slice(&tmp[..n]);
        }
        reactor
            .poll(&mut events, Some(Duration::from_millis(2)))
            .unwrap();
    }
    assert_eq!(&got_0b, b"ack0-b");
    assert!(pool.release_inflight(0, ep_0_new).unwrap());

    stop_srv.store(true, Ordering::Release);
    dialer.stop();
    srv_0.join().unwrap();
    srv_1.join().unwrap();
}
