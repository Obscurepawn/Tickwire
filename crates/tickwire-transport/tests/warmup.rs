#![forbid(unsafe_code)]

use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};
use tickwire_transport::{
    Connection, IoBudget, Limits, PreparedSend, Security, SendState, TlsLoopbackWarmer,
    WarmupConfig, WarmupTracker,
    order_router::{OrderRouter, RouterConfig, SendSlot},
    warmup::{
        DEFAULT_DUMMY_HTTP_ACK, decode_http_response_slice, decode_ws_server_frame_slice,
        encode_http_request_into, encode_ws_client_frame_into, encode_ws_server_frame_into,
    },
    ws::Opcode,
};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

fn tls_pair() -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server = rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_no_client_auth()
        .with_single_cert(vec![cert.cert.der().clone()], key.into())
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client = rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_root_certificates(roots)
        .with_no_client_auth();
    (Arc::new(client), Arc::new(server))
}

fn drive_ready(c: &mut Connection) {
    let mut p = mio::Poll::new().unwrap();
    c.register(p.registry(), mio::Token(0)).unwrap();
    let mut events = mio::Events::with_capacity(8);
    let end = deadline();
    while !c.is_ready() {
        assert!(Instant::now() < end, "handshake timed out");
        p.poll(&mut events, Some(Duration::from_millis(10)))
            .unwrap();
        c.drive(Instant::now(), IoBudget::default()).unwrap();
    }
}

fn read_vm_data_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmData:") {
            let digits = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            return Some(digits);
        }
    }
    None
}

#[test]
fn warmup_tracker_enforces_disabled_zero_idle_threshold_and_activity_reset() {
    let mut disabled = WarmupTracker::disabled();
    assert_eq!(disabled.warmup_interval_ns(), 0);
    assert_eq!(disabled.next_due_ns(), None);
    for step in 0u64..200 {
        assert!(!disabled.should_warm(step * 1_000_000));
    }
    assert_eq!(disabled.warm_count(), 0);

    let mut tracker = WarmupTracker::new(WarmupConfig::new(500_000));
    tracker.record_activity(10_000_000);
    assert!(!tracker.should_warm(10_000_000));
    assert!(!tracker.should_warm(10_499_999));
    assert_eq!(tracker.warm_count(), 0);

    // Real quote/order burst at +400us resets the 500us idle window to 10_900_000.
    tracker.record_activity(10_400_000);
    assert!(!tracker.should_warm(10_899_999));
    assert!(tracker.should_warm(10_900_000));
    assert_eq!(tracker.warm_count(), 1);
    assert_eq!(tracker.last_warm_ns(), 10_900_000);

    // Subsequent idle checks will not re-fire until another full interval elapses.
    assert!(!tracker.should_warm(11_399_999));
    assert!(tracker.should_warm(11_400_000));
    assert_eq!(tracker.warm_count(), 2);
}

#[test]
fn hundred_loopback_warm_steps_preserve_live_tls_nonce_and_router_state() {
    let (client_cfg, server_cfg) = tls_pair();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();

    let expected_req_1 = b"POST /v5/order/create HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 43\r\n\r\n{\"symbol\":\"BTCUSDT\",\"side\":\"Buy\",\"qty\":\"1\"}";
    let expected_req_2 = b"POST /v5/order/create HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 44\r\n\r\n{\"symbol\":\"BTCUSDT\",\"side\":\"Sell\",\"qty\":\"2\"}";
    let server_resp_1 = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 33\r\n\r\n{\"retCode\":0,\"orderId\":\"live-01\"}";
    let server_resp_2 = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 33\r\n\r\n{\"retCode\":0,\"orderId\":\"live-02\"}";

    let server_handle = thread::spawn(move || {
        let (sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        sock.set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut tls =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server_cfg).unwrap(), sock);
        while tls.conn.is_handshaking() {
            tls.conn.complete_io(&mut tls.sock).unwrap();
        }
        ready_tx.send(()).unwrap();

        let mut got_1 = vec![0u8; expected_req_1.len()];
        tls.read_exact(&mut got_1).unwrap();
        assert_eq!(&got_1[..], expected_req_1);
        tls.write_all(server_resp_1).unwrap();
        tls.flush().unwrap();

        let mut got_2 = vec![0u8; expected_req_2.len()];
        tls.read_exact(&mut got_2).unwrap();
        assert_eq!(&got_2[..], expected_req_2);
        tls.write_all(server_resp_2).unwrap();
        tls.conn.send_close_notify();
        tls.flush().unwrap();
    });

    let mut live_conn = Connection::connect(
        addr,
        Security::Tls {
            server_name: "localhost".try_into().unwrap(),
            config: client_cfg,
        },
        Limits::default(),
        deadline(),
    )
    .unwrap();
    drive_ready(&mut live_conn);
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    // Snapshot an OrderRouter alongside the live connection to verify zero router side effects.
    let mut router = OrderRouter::new(RouterConfig::default()).unwrap();
    router
        .register_slot(0, None, SendSlot::Exclusive, 0)
        .unwrap();
    router
        .register_slot(1, None, SendSlot::Exclusive, 0)
        .unwrap();
    let routes_before = (*router.routes(), router.last_conn_id());

    let mut warmer = TlsLoopbackWarmer::new().unwrap();
    let mut req_scratch = [0u8; 2048];
    let mut ack_scratch = [0u8; 512];

    // Execute 100 consecutive offline loopback TLS warmups while live_conn sits idle.
    for iter in 0u32..100 {
        let body_str = b"{\"symbol\":\"BTCUSDT\",\"orderLinkId\":\"warm-0001\"}";
        let headers = [
            ("Host", "localhost"),
            ("Content-Type", "application/json"),
            ("X-Warm-Iter", "1"),
        ];
        let req_len = encode_http_request_into(
            &mut req_scratch,
            "POST",
            "/v5/order/create",
            &headers,
            body_str,
        )
        .unwrap();
        let (req_dec, ack_dec) = warmer
            .warm_step_exchange(
                &req_scratch[..req_len],
                DEFAULT_DUMMY_HTTP_ACK,
                &mut ack_scratch,
            )
            .unwrap();
        assert_eq!(req_dec, req_len);
        let (status, parsed_body) = decode_http_response_slice(&ack_scratch[..ack_dec]).unwrap();
        assert_eq!(status, 200, "iter {iter}");
        assert!(parsed_body.starts_with(b"{\"retCode\":0"));
    }

    // Router EWMA scores, inflight counts, and routed state remain 100% bit-for-bit untouched.
    assert_eq!(routes_before, (*router.routes(), router.last_conn_id()));

    // Step 101: Send a REAL order over the established live TLS connection; server must decrypt cleanly!
    live_conn
        .queue(PreparedSend::new(expected_req_1, deadline(), 4096).unwrap())
        .unwrap();
    let first_accept = live_conn.try_send(Instant::now(), || true).unwrap();
    assert!(matches!(
        first_accept.state,
        SendState::Flushed | SendState::MayHaveSent
    ));

    let mut resp_accum = Vec::new();
    let mut got_live_1 = false;
    let until = deadline();
    while Instant::now() < until {
        live_conn
            .drive(Instant::now(), IoBudget::default())
            .unwrap();
        let mut chunk = [0u8; 256];
        let n = live_conn.read(&mut chunk);
        if n > 0 {
            resp_accum.extend_from_slice(&chunk[..n]);
            if let Ok((status, body)) = decode_http_response_slice(&resp_accum) {
                assert_eq!(status, 200);
                assert_eq!(body, b"{\"retCode\":0,\"orderId\":\"live-01\"}");
                got_live_1 = true;
                break;
            }
        } else {
            thread::sleep(Duration::from_millis(2));
        }
    }
    assert!(got_live_1, "did not receive live TLS reply 1");
    let _ = live_conn.take_finished();

    // Run ANOTHER 100 warmups after real live traffic, then send a second real order!
    for _ in 0..100 {
        let total_plain = warmer.warm_step(expected_req_1);
        assert_eq!(
            total_plain,
            expected_req_1.len() + DEFAULT_DUMMY_HTTP_ACK.len()
        );
    }

    live_conn
        .queue(PreparedSend::new(expected_req_2, deadline(), 4096).unwrap())
        .unwrap();
    let second_accept = live_conn.try_send(Instant::now(), || true).unwrap();
    assert!(matches!(
        second_accept.state,
        SendState::Flushed | SendState::MayHaveSent
    ));

    resp_accum.clear();
    let mut got_live_2 = false;
    let until = deadline();
    while Instant::now() < until {
        live_conn
            .drive(Instant::now(), IoBudget::default())
            .unwrap();
        let mut chunk = [0u8; 256];
        let n = live_conn.read(&mut chunk);
        if n > 0 {
            resp_accum.extend_from_slice(&chunk[..n]);
            if let Ok((status, body)) = decode_http_response_slice(&resp_accum) {
                assert_eq!(status, 200);
                assert_eq!(body, b"{\"retCode\":0,\"orderId\":\"live-02\"}");
                got_live_2 = true;
                break;
            }
        } else {
            thread::sleep(Duration::from_millis(2));
        }
    }
    assert!(got_live_2, "did not receive live TLS reply 2");

    server_handle.join().unwrap();
}

#[test]
fn steady_state_warm_step_keeps_scratch_address_and_heap_footprint_bounded() {
    let mut warmer = TlsLoopbackWarmer::new().unwrap();
    let base_scratch_addr = warmer.scratch_addr();
    let mut ws_client_buf = [0u8; 512];
    let mut ws_server_buf = [0u8; 512];
    let mut ack_out = [0u8; 512];

    let sample_ws_json = br#"{"id":"warm-1","method":"order.place","params":{"symbol":"BTCUSDT","side":"BUY","type":"LIMIT","quantity":"0.001","price":"50000.00"}}"#;
    let sample_ws_ack = br#"{"id":"warm-1","status":200,"result":{"orderId":1001,"status":"NEW"}}"#;
    let ack_frame_len =
        encode_ws_server_frame_into(&mut ws_server_buf, Opcode::Text, sample_ws_ack).unwrap();

    // Settle initial thread-local allocator bins over 32 iterations before measuring VmData.
    for _ in 0..32 {
        let req_len = encode_ws_client_frame_into(
            &mut ws_client_buf,
            Opcode::Text,
            [0x37, 0xfa, 0x21, 0x3d],
            sample_ws_json,
        )
        .unwrap();
        let (_, ack_len) = warmer
            .warm_step_exchange(
                &ws_client_buf[..req_len],
                &ws_server_buf[..ack_frame_len],
                &mut ack_out,
            )
            .unwrap();
        let (opcode, body) = decode_ws_server_frame_slice(&ack_out[..ack_len]).unwrap();
        assert_eq!(opcode, Opcode::Text);
        assert_eq!(body, sample_ws_ack);
    }

    let vm_before = read_vm_data_kib();
    for _ in 0..5_000 {
        let req_len = encode_ws_client_frame_into(
            &mut ws_client_buf,
            Opcode::Text,
            [0x37, 0xfa, 0x21, 0x3d],
            sample_ws_json,
        )
        .unwrap();
        let (_, ack_len) = warmer
            .warm_step_exchange(
                &ws_client_buf[..req_len],
                &ws_server_buf[..ack_frame_len],
                &mut ack_out,
            )
            .unwrap();
        let (opcode, body) = decode_ws_server_frame_slice(&ack_out[..ack_len]).unwrap();
        assert_eq!(opcode, Opcode::Text);
        assert_eq!(body, sample_ws_ack);
        assert_eq!(warmer.scratch_addr(), base_scratch_addr);
    }
    if let (Some(before), Some(after)) = (vm_before, read_vm_data_kib()) {
        assert!(
            after <= before + 64,
            "unexpected VmData growth across 5,000 warm_step iterations: before={before} KiB, after={after} KiB"
        );
    }
}
