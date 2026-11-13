use base64::Engine;
use sha1::Digest;
use std::{
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener},
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_transport::{
    Connection, Error, IoBudget, Limits, PreparedSend, Security, SendState,
    dialer::{
        DEFAULT_MAX_PEERS, DialBackoff, DialBaton, DialSecurity, DialSpec, DialState,
        DialTransport, DialedLink, Dialer, IpPool,
    },
    mono_now_ns,
    rest_querier::{
        AsyncRestQuerier, BuiltRequest, RestQuerierConfig, RestReply, RestStatus, RestTask,
    },
    rustls, ws,
};

fn test_deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

fn tls_pair() -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.cert.der().clone()], key.into())
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (Arc::new(client), Arc::new(server))
}

fn read_http_head(stream: &mut impl Read) -> Option<String> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        match stream.read(&mut byte) {
            Ok(0) => return None,
            Ok(_) => {
                buf.push(byte[0]);
                if buf.ends_with(b"\r\n\r\n") {
                    return Some(String::from_utf8(buf).unwrap());
                }
            }
            Err(_) => return None,
        }
    }
}

fn wait_for_reply(q: &AsyncRestQuerier) -> RestReply {
    let until = test_deadline();
    loop {
        if let Some(r) = q.dequeue() {
            return r;
        }
        assert!(Instant::now() < until, "timed out waiting for RestReply");
        thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn connect_from_local_ip_binding_and_family_mismatch_rejection() {
    let v4_target = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000);
    let v6_target = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 9000);

    assert!(matches!(
        Connection::connect_from(
            v4_target,
            Some(IpAddr::V6(Ipv6Addr::LOCALHOST)),
            Security::Plain,
            Limits::default(),
            test_deadline(),
        ),
        Err(Error::Bounds)
    ));
    assert!(matches!(
        Connection::connect_from(
            v6_target,
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            Security::Plain,
            Limits::default(),
            test_deadline(),
        ),
        Err(Error::Bounds)
    ));

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target = listener.local_addr().unwrap();
    let preferred_alt = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
    let (mut conn, expected_local) = match Connection::connect_from(
        target,
        Some(preferred_alt),
        Security::Plain,
        Limits::default(),
        test_deadline(),
    ) {
        Ok(c) => (c, preferred_alt),
        Err(_) => {
            let localhost = IpAddr::V4(Ipv4Addr::LOCALHOST);
            let c = Connection::connect_from(
                target,
                Some(localhost),
                Security::Plain,
                Limits::default(),
                test_deadline(),
            )
            .unwrap();
            (c, localhost)
        }
    };
    assert_eq!(conn.local_ip(), Some(expected_local));
    assert_eq!(conn.peer_addr_target(), target);

    let (mut server_sock, accepted_from) = listener.accept().unwrap();
    assert_eq!(accepted_from.ip(), expected_local);
    server_sock
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    // Register on poll_1, drive to ready, deregister, and re-register on poll_2.
    let mut poll_1 = mio::Poll::new().unwrap();
    let mut events = mio::Events::with_capacity(8);
    conn.register(poll_1.registry(), mio::Token(1)).unwrap();
    let until = test_deadline();
    while !conn.is_ready() {
        assert!(Instant::now() < until);
        poll_1
            .poll(&mut events, Some(Duration::from_millis(10)))
            .unwrap();
        conn.drive(Instant::now(), IoBudget::default()).unwrap();
    }
    assert!(conn.is_tcp_connected());
    conn.deregister(poll_1.registry()).unwrap();

    let poll_2 = mio::Poll::new().unwrap();
    conn.register(poll_2.registry(), mio::Token(2)).unwrap();
    conn.queue(PreparedSend::new(b"ping", test_deadline(), 64).unwrap())
        .unwrap();
    assert_eq!(
        conn.try_send(Instant::now(), || true).unwrap().state,
        SendState::Flushed
    );
    let mut got = [0u8; 4];
    server_sock.read_exact(&mut got).unwrap();
    assert_eq!(&got, b"ping");
}

#[test]
fn ip_pool_ewma_ranking_eviction_and_replace_with_unused() {
    let mut pool = IpPool::new();
    assert_eq!(pool.max_peers(), DEFAULT_MAX_PEERS);

    let a1: SocketAddr = "127.0.0.1:10001".parse().unwrap();
    let a2: SocketAddr = "127.0.0.1:10002".parse().unwrap();
    let a3: SocketAddr = "127.0.0.1:10003".parse().unwrap();

    pool.add_ip(a1);
    pool.add_ip(a1); // duplicate ignored
    pool.add_ip(a2);
    pool.add_ip(a3);
    assert_eq!(pool.len(), 3);

    // EWMA: first sample = rtt, subsequent = (old * 29 + rtt) / 30 without u32 overflow.
    pool.note_syn(a1, Some(300));
    assert_eq!(pool.entry(a1).unwrap().avg_rtt_us, Some(300));
    pool.note_syn(a1, Some(600));
    // (300 * 29 + 600) / 30 = 9300 / 30 = 310; rank = max(310, 600) = 600.
    assert_eq!(pool.entry(a1).unwrap().avg_rtt_us, Some(310));
    assert_eq!(pool.entry(a1).unwrap().last_rtt_us, Some(600));

    // a2 has lower max(avg, last) = 450 < 600; a3 is unprobed (sorts after probed healthy).
    pool.note_syn(a2, Some(450));

    // First take_ip picks a2 (spare, rank 450), second picks a1 (spare, rank 600), third picks unprobed a3.
    assert_eq!(pool.take_ip(), Some(a2));
    assert_eq!(pool.take_ip(), Some(a1));
    assert_eq!(pool.take_ip(), Some(a3));
    assert_eq!(pool.spare_count(), 0);

    // All peers have in_use == 1; release a3 -> no spare other than a3, replace_with_unused(a2) picks a3!
    pool.release_ip(a3);
    assert_eq!(pool.entry(a3).unwrap().in_use, 0);
    assert_eq!(pool.replace_with_unused(a2), Some(a3));
    assert_eq!(pool.entry(a2).unwrap().in_use, 0);
    assert_eq!(pool.entry(a3).unwrap().in_use, 1);

    // Release a2 is already 0; mark a2 failed while in_use == 0 and len > 1 -> evicted!
    pool.note_syn(a2, None);
    assert!(pool.entry(a2).is_none());
    assert_eq!(pool.len(), 2);

    // Both remaining peers (a1, a3) have in_use == 1, so replace_with_unused returns None and keeps lease.
    assert_eq!(pool.replace_with_unused(a1), None);
    assert_eq!(pool.entry(a1).unwrap().in_use, 1);

    // Failed peer with in_use > 0 is marked last_failed but NOT evicted while in use.
    pool.note_syn(a1, None);
    assert!(pool.entry(a1).unwrap().last_failed);
    assert_eq!(pool.len(), 2);

    // Large RTT near u32::MAX must not overflow u32 * 29.
    pool.note_syn(a3, Some(u32::MAX - 30));
    pool.note_syn(a3, Some(u32::MAX));
    assert!(pool.entry(a3).unwrap().avg_rtt_us.unwrap() >= u32::MAX - 30);

    // Bounded max_peers cap.
    let mut small = IpPool::with_max_peers(2);
    small.add_ip(a1);
    small.add_ip(a2);
    small.add_ip(a3);
    assert_eq!(small.len(), 2);

    // Last remaining peer is never evicted on failure even when in_use == 0.
    let mut single = IpPool::new();
    single.add_ip(a1);
    single.note_syn(a1, None);
    assert_eq!(single.len(), 1);
    assert!(single.entry(a1).unwrap().last_failed);
}

#[test]
fn dial_baton_state_machine_backoff_and_multithreaded_races() {
    let backoff = DialBackoff::new(25, 200);
    assert_eq!(backoff.delay_ns(0), 25_000_000);
    assert_eq!(backoff.delay_ns(1), 50_000_000);
    assert_eq!(backoff.delay_ns(3), 200_000_000);
    assert_eq!(backoff.delay_ns(99), 200_000_000);

    let baton = Arc::new(DialBaton::new());
    assert_eq!(baton.state(), DialState::Idle);
    assert!(!baton.claim(1_000));
    assert!(baton.take_ready().is_none());

    assert!(baton.want(500));
    assert!(!baton.want(100)); // already Wanted; must not overwrite due_mono_ns
    assert_eq!(baton.due_mono_ns(), 500);
    assert!(!baton.claim(499));
    assert!(baton.claim(500));
    assert_eq!(baton.state(), DialState::Dialing);

    // Failure increments failures and returns to Wanted with next_due_mono_ns.
    assert!(baton.fail(900));
    assert_eq!(baton.state(), DialState::Wanted);
    assert_eq!(baton.failures(), 1);
    assert!(!baton.claim(899));
    assert!(baton.claim(900));

    // Reset cancels Dialing -> Idle, so late fail() is ignored.
    baton.reset();
    assert_eq!(baton.state(), DialState::Idle);
    assert_eq!(baton.failures(), 0);
    assert!(!baton.fail(1_200));
    assert_eq!(baton.state(), DialState::Idle);

    // Concurrent multi-threaded handoff stress test over 64 cycles.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done_accept = Arc::clone(&done);
    let accept_thread = thread::spawn(move || {
        while !done_accept.load(Ordering::Acquire) {
            while listener.accept().is_ok() {}
            thread::yield_now();
        }
    });

    let shared_baton = Arc::new(DialBaton::new());
    let barrier = Arc::new(Barrier::new(2));
    let dialer_baton = Arc::clone(&shared_baton);
    let dialer_barrier = Arc::clone(&barrier);

    let producer = thread::spawn(move || {
        dialer_barrier.wait();
        let mut produced = 0usize;
        while produced < 64 {
            if dialer_baton.claim(100) {
                let conn =
                    Connection::connect(addr, Security::Plain, Limits::default(), test_deadline())
                        .unwrap();
                assert!(dialer_baton.deliver_ready(DialedLink {
                    connection: conn,
                    peer: addr,
                    connect_rtt_us: 42,
                }));
                produced += 1;
            } else {
                thread::yield_now();
            }
        }
    });

    barrier.wait();
    let mut consumed = 0usize;
    while consumed < 64 {
        let _ = shared_baton.want(100);
        if let Some(link) = shared_baton.take_ready() {
            assert_eq!(link.peer, addr);
            assert_eq!(link.connect_rtt_us, 42);
            assert_eq!(shared_baton.state(), DialState::Idle);
            consumed += 1;
        } else {
            thread::yield_now();
        }
    }

    producer.join().unwrap();
    done.store(true, Ordering::Release);
    accept_thread.join().unwrap();

    // Terminal Stopped state rejects subsequent want / reset.
    shared_baton.stop();
    assert_eq!(shared_baton.state(), DialState::Stopped);
    assert!(!shared_baton.want(0));
    shared_baton.reset();
    assert_eq!(shared_baton.state(), DialState::Stopped);
}

#[test]
fn dialer_tls_websocket_101_upgrade_preserves_coalesced_frames_and_reregisters() {
    let (client_cfg, server_cfg) = tls_pair();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = thread::spawn(move || {
        let (sock, from) = listener.accept().unwrap();
        assert_eq!(from.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut tls =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server_cfg).unwrap(), sock);

        let req = read_http_head(&mut tls).unwrap();
        assert!(req.starts_with("GET /ws/v1 HTTP/1.1\r\n"));
        assert!(
            req.lines()
                .any(|l| l.eq_ignore_ascii_case("x-api-key: demo-key"))
        );
        let key = req
            .lines()
            .find_map(|l| l.strip_prefix("Sec-WebSocket-Key: "))
            .unwrap();
        let mut sha = sha1::Sha1::new();
        sha.update(key);
        sha.update("258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        let accept = base64::engine::general_purpose::STANDARD.encode(sha.finalize());

        // Write HTTP 101 AND an unmasked server WebSocket Text frame in ONE single buffer!
        let payload = b"{\"ready\":true}";
        let mut wire = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        )
        .into_bytes();
        wire.push(0x81);
        wire.push(payload.len() as u8);
        wire.extend_from_slice(payload);
        tls.write_all(&wire).unwrap();
        tls.flush().unwrap();

        // Read client's masked WebSocket frame sent from the owner's reactor after handoff.
        let mut client_frame = [0u8; 8];
        tls.read_exact(&mut client_frame).unwrap();
        assert_eq!(&client_frame[..2], &[0x81, 0x82]);
        let unmasked = [
            client_frame[6] ^ client_frame[2],
            client_frame[7] ^ client_frame[3],
        ];
        assert_eq!(&unmasked, b"ok");
        tls.conn.send_close_notify();
        let _ = tls.flush();
    });

    let dialer = Dialer::new().unwrap();
    let spec = DialSpec {
        host: "localhost".to_owned(),
        port: addr.port(),
        initial_peers: vec![addr],
        local_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        security: DialSecurity::Tls {
            server_name: "localhost".try_into().unwrap(),
            config: client_cfg,
        },
        limits: Limits::default(),
        transport: DialTransport::WebSocket {
            host: "localhost".to_owned(),
            path: "/ws/v1".to_owned(),
            extra_headers: vec![("X-Api-Key".to_owned(), "demo-key".to_owned())],
        },
        connect_timeout: Duration::from_secs(2),
        dial_timeout: Duration::from_secs(3),
        backoff: DialBackoff::new(20, 200),
    };

    let baton = dialer.add_slot(7, spec).unwrap();
    assert!(baton.want(0));

    let until = test_deadline();
    let mut link = loop {
        if let Some(l) = baton.take_ready() {
            break l;
        }
        assert!(Instant::now() < until, "timed out waiting for Dialer");
        thread::sleep(Duration::from_millis(2));
    };

    assert_eq!(link.peer, addr);
    assert_eq!(
        link.connection.local_ip(),
        Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
    );

    // Owner registers handed-off connection on its own Poll and reads the coalesced WS frame.
    let mut owner_poll = mio::Poll::new().unwrap();
    let mut events = mio::Events::with_capacity(8);
    link.connection
        .register(owner_poll.registry(), mio::Token(99))
        .unwrap();

    let mut ws_dec = ws::Decoder::new(1024, 1024).unwrap();
    let msg = loop {
        assert!(Instant::now() < until);
        let mut buf = [0u8; 256];
        let n = link.connection.read(&mut buf);
        if n > 0 {
            ws_dec.push(&buf[..n]).unwrap();
            if let Some(m) = ws_dec.next_message().unwrap() {
                break m;
            }
        }
        owner_poll
            .poll(&mut events, Some(Duration::from_millis(5)))
            .unwrap();
        let _ = link.connection.drive(Instant::now(), IoBudget::default());
    };
    assert_eq!(msg.opcode, ws::Opcode::Text);
    assert_eq!(msg.payload, b"{\"ready\":true}");

    // Send a masked frame back to the server on the handed-off connection.
    let frame = ws::encode_client(ws::Opcode::Text, b"ok", 64).unwrap();
    link.connection
        .queue(PreparedSend::new(&frame, test_deadline(), 64).unwrap())
        .unwrap();
    while link.connection.send_report().unwrap().state != SendState::Flushed {
        assert!(Instant::now() < until);
        link.connection.try_send(Instant::now(), || true).unwrap();
        link.connection
            .drive(Instant::now(), IoBudget::default())
            .unwrap();
    }

    server.join().unwrap();
    dialer.stop();
}

#[test]
fn dialer_dead_peer_failover_and_exponential_reconnect_backoff() {
    // Obtain an unused closed port by binding and immediately dropping a listener.
    let dead_addr = {
        let dead = TcpListener::bind("127.0.0.1:0").unwrap();
        dead.local_addr().unwrap()
    };
    let live_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let live_addr = live_listener.local_addr().unwrap();

    let server = thread::spawn(move || {
        let (_sock, _) = live_listener.accept().unwrap();
    });

    let dialer = Dialer::new().unwrap();
    // Seed dead_addr first so attempt 1 hits dead_addr, fails, evicts it, backs off, and succeeds on live_addr.
    let spec = DialSpec {
        host: "127.0.0.1".to_owned(),
        port: live_addr.port(),
        initial_peers: vec![dead_addr, live_addr],
        local_ip: None,
        security: DialSecurity::Plain,
        limits: Limits::default(),
        transport: DialTransport::PlainOrTls,
        connect_timeout: Duration::from_millis(300),
        dial_timeout: Duration::from_millis(500),
        backoff: DialBackoff::new(25, 100),
    };

    let start = Instant::now();
    let baton = dialer.add_slot(1, spec).unwrap();
    assert!(baton.want(0));

    let until = test_deadline();
    let link = loop {
        if let Some(l) = baton.take_ready() {
            break l;
        }
        assert!(Instant::now() < until, "failover dial timed out");
        thread::sleep(Duration::from_millis(2));
    };

    assert_eq!(link.peer, live_addr);
    assert!(
        start.elapsed() >= Duration::from_millis(20),
        "expected backoff delay after dead peer failure"
    );
    assert_eq!(baton.failures(), 0); // take_ready resets failures to 0

    server.join().unwrap();
}

#[test]
fn rest_querier_keep_alive_reuse_and_fresh_send_time_builder() {
    let (client_cfg, server_cfg) = tls_pair();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_count = Arc::new(AtomicUsize::new(0));
    let accept_count_srv = Arc::clone(&accept_count);

    let server = thread::spawn(move || {
        let (sock, _) = listener.accept().unwrap();
        accept_count_srv.fetch_add(1, Ordering::SeqCst);
        sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut tls =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server_cfg).unwrap(), sock);

        for expected_seq in 1..=3 {
            let head = read_http_head(&mut tls).unwrap();
            let seq_hdr = format!("X-Sign-Seq: {expected_seq}");
            assert!(
                head.lines().any(|l| l == seq_hdr),
                "missing {seq_hdr} in {head}"
            );
            let body = format!("{{\"seq\":{expected_seq}}}");
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Echo: {expected_seq}\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            tls.write_all(resp.as_bytes()).unwrap();
            tls.flush().unwrap();
        }
        // Wait for client shutdown before closing socket so keep-alive stays open across all tasks.
        let mut eof = [0u8; 1];
        let _ = tls.read(&mut eof);
    });

    let build_seq = Arc::new(AtomicUsize::new(0));
    let build_seq_cl = Arc::clone(&build_seq);
    let querier = AsyncRestQuerier::new(
        RestQuerierConfig {
            host: "localhost".to_owned(),
            port: addr.port(),
            initial_peers: vec![addr],
            local_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            security: DialSecurity::Tls {
                server_name: "localhost".try_into().unwrap(),
                config: client_cfg,
            },
            capacity: 16,
            connect_timeout_ms: 2_000,
            interval_ms: 20,
            timeout_ms: 2_000,
            ..RestQuerierConfig::default()
        },
        move |task: &RestTask, _host: &str, _scratch: &mut Vec<u8>| {
            if task.kind == 99 {
                return Ok(None);
            }
            let seq = build_seq_cl.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(Some(BuiltRequest {
                method: "GET".to_owned(),
                path: format!("/api/v3/order?key={}", task.key),
                headers: vec![("X-Sign-Seq".to_owned(), seq.to_string())],
                body: Vec::new(),
            }))
        },
    )
    .unwrap();

    assert!(querier.enqueue(RestTask {
        key: 1,
        kind: 1,
        max_query_count: 2,
        start_mono_ns: 0,
        end_mono_ns: 0,
    }));
    // Enqueue a NotBuilt task (kind == 99) mid-stream and verify keep-alive isn't disturbed.
    assert!(querier.enqueue(RestTask {
        key: 4,
        kind: 99,
        max_query_count: 2,
        start_mono_ns: 0,
        end_mono_ns: 0,
    }));
    for key in 2..=3 {
        assert!(querier.enqueue(RestTask {
            key,
            kind: 1,
            max_query_count: 2,
            start_mono_ns: 0,
            end_mono_ns: 0,
        }));
    }

    let mut replies = Vec::new();
    while replies.len() < 4 {
        replies.push(wait_for_reply(&querier));
    }

    for i in 1..=3u64 {
        let r = replies.iter().find(|r| r.task.key == i).unwrap();
        assert_eq!(r.status, RestStatus::Ok);
        assert_eq!(r.http_code, 200);
        assert_eq!(r.query_count, 1);
        assert_eq!(r.body, format!("{{\"seq\":{i}}}").into_bytes());
    }
    let not_built = replies.iter().find(|r| r.task.key == 4).unwrap();
    assert_eq!(not_built.status, RestStatus::NotBuilt);
    assert_eq!(not_built.query_count, 0);

    querier.stop();
    assert_eq!(accept_count.load(Ordering::SeqCst), 1);
    server.join().unwrap();
}

#[test]
fn rest_querier_503_retry_recovery_max_count_cap_and_read_fail() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = thread::spawn(move || {
        // Connection 1 handles:
        // - key 10: 503, 503, 200 (recovers on attempt 3)
        // - key 20: 400 (non-retryable 4xx, 1 attempt)
        // - key 30: 502, 502 (capped at max_query_count = 2)
        // - key 40 attempt 1: closes mid-response -> ReadFail
        let (mut sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

        // key 10: 503 x 2 -> 200
        for code in [503u16, 503, 200] {
            let head = read_http_head(&mut sock).unwrap();
            assert!(head.starts_with("GET /q/10 "));
            let body = if code == 200 { "recovered" } else { "busy" };
            let msg = format!(
                "HTTP/1.1 {code} Status\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(msg.as_bytes()).unwrap();
        }

        // key 20: 400 Bad Request -> no retry
        let head = read_http_head(&mut sock).unwrap();
        assert!(head.starts_with("GET /q/20 "));
        sock.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 3\r\n\r\nbad")
            .unwrap();

        // key 30: 502 x 2 -> hits max_query_count = 2
        for _ in 0..2 {
            let head = read_http_head(&mut sock).unwrap();
            assert!(head.starts_with("GET /q/30 "));
            sock.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 4\r\n\r\ndown")
                .unwrap();
        }

        // key 40 attempt 1: truncated body then EOF -> ReadFail
        let head = read_http_head(&mut sock).unwrap();
        assert!(head.starts_with("GET /q/40 "));
        sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\nhalf")
            .unwrap();
        drop(sock);

        // Connection 2 handles key 40 attempt 2 -> 200 OK
        let (mut sock2, _) = listener.accept().unwrap();
        sock2
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let head = read_http_head(&mut sock2).unwrap();
        assert!(head.starts_with("GET /q/40 "));
        sock2
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone")
            .unwrap();
    });

    let build_calls = Arc::new(AtomicUsize::new(0));
    let build_calls_cl = Arc::clone(&build_calls);
    let querier = AsyncRestQuerier::new(
        RestQuerierConfig {
            host: "localhost".to_owned(),
            port: addr.port(),
            initial_peers: vec![addr],
            capacity: 16,
            connect_timeout_ms: 1_000,
            interval_ms: 10,
            timeout_ms: 1_000,
            ..RestQuerierConfig::default()
        },
        move |task: &RestTask, _host: &str, _scratch: &mut Vec<u8>| {
            let n = build_calls_cl.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(Some(BuiltRequest {
                method: "GET".to_owned(),
                path: format!("/q/{}", task.key),
                headers: vec![("X-Attempt-Global".to_owned(), n.to_string())],
                body: Vec::new(),
            }))
        },
    )
    .unwrap();

    assert!(querier.enqueue(RestTask {
        key: 10,
        kind: 1,
        max_query_count: 5,
        start_mono_ns: 0,
        end_mono_ns: 0,
    }));
    let r10 = wait_for_reply(&querier);
    assert_eq!(r10.status, RestStatus::Ok);
    assert_eq!(r10.http_code, 200);
    assert_eq!(r10.query_count, 3);
    assert_eq!(r10.body, b"recovered");

    assert!(querier.enqueue(RestTask {
        key: 20,
        kind: 1,
        max_query_count: 5,
        start_mono_ns: 0,
        end_mono_ns: 0,
    }));
    let r20 = wait_for_reply(&querier);
    assert_eq!(r20.status, RestStatus::HttpError);
    assert_eq!(r20.http_code, 400);
    assert_eq!(r20.query_count, 1);
    assert_eq!(r20.body, b"bad");

    assert!(querier.enqueue(RestTask {
        key: 30,
        kind: 1,
        max_query_count: 2,
        start_mono_ns: 0,
        end_mono_ns: 0,
    }));
    let r30 = wait_for_reply(&querier);
    assert_eq!(r30.status, RestStatus::HttpError);
    assert_eq!(r30.http_code, 502);
    assert_eq!(r30.query_count, 2);
    assert_eq!(r30.body, b"down");

    assert!(querier.enqueue(RestTask {
        key: 40,
        kind: 1,
        max_query_count: 3,
        start_mono_ns: 0,
        end_mono_ns: 0,
    }));
    let r40 = wait_for_reply(&querier);
    assert_eq!(r40.status, RestStatus::Ok);
    assert_eq!(r40.http_code, 200);
    assert_eq!(r40.query_count, 2);
    assert_eq!(r40.body, b"done");

    // Total sends = 3 + 1 + 2 + 2 = 8 fresh build_request calls.
    assert_eq!(build_calls.load(Ordering::SeqCst), 8);
    server.join().unwrap();
}

#[test]
fn rest_querier_end_mono_ns_expiry_before_and_after_first_send() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let head = read_http_head(&mut sock).unwrap();
        assert!(head.starts_with("GET /expire-after-503 "));
        sock.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 4\r\n\r\nwait")
            .unwrap();
    });

    let querier = AsyncRestQuerier::new(
        RestQuerierConfig {
            host: "localhost".to_owned(),
            port: addr.port(),
            initial_peers: vec![addr],
            capacity: 16,
            connect_timeout_ms: 1_000,
            interval_ms: 150, // retry backoff 150ms exceeds remaining end_mono_ns window
            timeout_ms: 1_000,
            ..RestQuerierConfig::default()
        },
        move |task: &RestTask, _host: &str, _scratch: &mut Vec<u8>| {
            Ok(Some(BuiltRequest {
                method: "GET".to_owned(),
                path: if task.key == 1 {
                    "/never-sent".to_owned()
                } else {
                    "/expire-after-503".to_owned()
                },
                headers: Vec::new(),
                body: Vec::new(),
            }))
        },
    )
    .unwrap();

    // Task 1: end_mono_ns already passed before any send -> Expired with query_count == 0.
    let now = mono_now_ns();
    assert!(querier.enqueue(RestTask {
        key: 1,
        kind: 1,
        max_query_count: 5,
        start_mono_ns: now + 500_000_000,
        end_mono_ns: now + 10_000_000, // 10ms < start_mono_ns, expires without sending
    }));
    let r1 = wait_for_reply(&querier);
    assert_eq!(r1.status, RestStatus::Expired);
    assert_eq!(r1.query_count, 0);

    // Task 2: sends once, receives 503, next_send_ns = now + 150ms >= end_mono_ns (now + 80ms) -> HttpError, query_count == 1.
    let now2 = mono_now_ns();
    assert!(querier.enqueue(RestTask {
        key: 2,
        kind: 1,
        max_query_count: 5,
        start_mono_ns: 0,
        end_mono_ns: now2 + 80_000_000,
    }));
    let r2 = wait_for_reply(&querier);
    assert_eq!(r2.status, RestStatus::HttpError);
    assert_eq!(r2.http_code, 503);
    assert_eq!(r2.query_count, 1);

    server.join().unwrap();
}

#[test]
fn rest_querier_full_ring_backpressure_and_reply_slot_reservation() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        for _ in 0..8 {
            let head = read_http_head(&mut sock).unwrap();
            let key = head
                .lines()
                .next()
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap()
                .trim_start_matches("/k/");
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{key}",
                key.len()
            );
            sock.write_all(resp.as_bytes()).unwrap();
            sock.flush().unwrap();
        }
    });

    let querier = AsyncRestQuerier::new(
        RestQuerierConfig {
            host: "localhost".to_owned(),
            port: addr.port(),
            initial_peers: vec![addr],
            capacity: 4,
            connect_timeout_ms: 1_000,
            interval_ms: 10,
            timeout_ms: 1_000,
            ..RestQuerierConfig::default()
        },
        move |task: &RestTask, _host: &str, _scratch: &mut Vec<u8>| {
            Ok(Some(BuiltRequest {
                method: "GET".to_owned(),
                path: format!("/k/{}", task.key),
                headers: Vec::new(),
                body: Vec::new(),
            }))
        },
    )
    .unwrap();

    // Enqueue 4 tasks and wait until all 4 replies fill reply_queue (capacity = 4) WITHOUT draining.
    for key in 1..=4 {
        assert!(querier.enqueue(RestTask {
            key,
            kind: 1,
            max_query_count: 1,
            start_mono_ns: 0,
            end_mono_ns: 0,
        }));
    }
    let until = test_deadline();
    while querier.pending_replies() < 4 {
        assert!(Instant::now() < until, "timed out filling reply_queue");
        thread::sleep(Duration::from_millis(2));
    }

    // Because reply_queue is full (4/4), worker must NOT pull from task_queue!
    // Enqueueing 4 more tasks fills task_queue (4/4), and the 9th enqueue MUST return false immediately.
    for key in 5..=8 {
        assert!(querier.enqueue(RestTask {
            key,
            kind: 1,
            max_query_count: 1,
            start_mono_ns: 0,
            end_mono_ns: 0,
        }));
    }
    assert!(!querier.enqueue(RestTask {
        key: 9,
        kind: 1,
        max_query_count: 1,
        start_mono_ns: 0,
        end_mono_ns: 0,
    }));
    assert_eq!(querier.pending_tasks(), 4);
    assert_eq!(querier.pending_replies(), 4);

    // Drain first 4 replies -> wakes worker to pull and complete tasks 5..=8!
    let mut got_keys = Vec::new();
    let drained_first = querier.drain_replies(|r| {
        assert_eq!(r.status, RestStatus::Ok);
        got_keys.push(r.task.key);
    });
    assert_eq!(drained_first, 4);

    while got_keys.len() < 8 {
        assert!(Instant::now() < until, "timed out draining second batch");
        querier.drain_replies(|r| {
            assert_eq!(r.status, RestStatus::Ok);
            got_keys.push(r.task.key);
        });
        thread::sleep(Duration::from_millis(2));
    }

    got_keys.sort_unstable();
    assert_eq!(got_keys, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    server.join().unwrap();
}
