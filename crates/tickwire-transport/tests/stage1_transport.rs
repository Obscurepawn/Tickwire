//! Stage 1 integration tests for zero-allocation `JsonScan`/`MsgParser`, vtable-free `LinkOps`
//! (`WsLink` `loginChallenge` + `early_frames` replay + `GuardedValue` `listenKey`, `HttpsLink`,
//! `FixLink` `Logon`/`TestRequest`/`SequenceGap`/`Reject` `RefSeqNum` hint), TLS/TCP short-write
//! self-retirement + `mio::Waker` wakeup, `UrlConnections` + `PublishedRoutes` `9/8` hysteresis
//! + `SampleKind::Tcping`, and `rdtsc` `LatencyTracer` + `TimedSpinLock` contention attribution.
#![forbid(unsafe_code)]

use base64::Engine;
use mio::{Events, Poll, Token, Waker};
use sha1::Digest;
use std::{
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, TcpListener},
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_clock_sys::TscCalibration;
use tickwire_transport::{
    Connection, FindingKind, FixInboundAction, FixLink, FixSessionConfig, FixSessionState,
    GuardedValue, HttpsLink, IoBudget, Limits, LinkKind, LinkOps, LockDomain, LockId,
    LockedConnection, MarkSite, MsgParser, PublishedRoutes, RequestRef, SampleMask, Security,
    SendOutcome, ThreadTag, TimedSpinLock, UrlBindPair, UrlConnections, WsLink,
    dialer::{DialBackoff, DialSecurity, DialSpec, DialTransport, Dialer, WsDialAuth},
    fix,
    json_scan::{parse_scaled_i64, parse_step_units_i64},
    latency_tracer::{LatencyTracer, LatencyTracerConfig},
    mono_now_ns,
    order_router::{OrderRouter, RouterConfig, SampleKind},
    ws,
};
use tickwire_venue::{OrderRoute as VenueOrderRoute, Product};

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

fn tls_pair() -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert_der = rustls::pki_types::CertificateDer::from(ck.cert.der().to_vec());
    let key_der =
        rustls::pki_types::PrivateKeyDer::try_from(ck.signing_key.serialize_der()).unwrap();

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert_der.clone()).unwrap();

    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key_der)
        .unwrap();
    (Arc::new(client), Arc::new(server))
}

fn encode_server_ws_text(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 10);
    out.push(0x81);
    if payload.len() < 126 {
        out.push(payload.len() as u8);
    } else {
        out.push(126);
        out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    }
    out.extend_from_slice(payload);
    out
}

fn read_client_masked_ws_text(stream: &mut impl Read) -> String {
    let mut hdr = [0u8; 2];
    stream.read_exact(&mut hdr).unwrap();
    assert_eq!(hdr[0], 0x81);
    assert_ne!(hdr[1] & 0x80, 0, "client frame must be masked");
    let len = match hdr[1] & 0x7f {
        126 => {
            let mut ext = [0u8; 2];
            stream.read_exact(&mut ext).unwrap();
            u16::from_be_bytes(ext) as usize
        }
        small => usize::from(small),
    };
    let mut mask = [0u8; 4];
    stream.read_exact(&mut mask).unwrap();
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).unwrap();
    for (i, b) in body.iter_mut().enumerate() {
        *b ^= mask[i & 3];
    }
    String::from_utf8(body).unwrap()
}

fn read_http_upgrade(stream: &mut impl Read) -> (String, String) {
    let mut raw = Vec::new();
    let mut byte = [0u8; 1];
    while !raw.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).unwrap();
        raw.push(byte[0]);
    }
    let text = String::from_utf8(raw).unwrap();
    let req_line = text.lines().next().unwrap().to_owned();
    let ws_key = text
        .lines()
        .find_map(|l| l.strip_prefix("Sec-WebSocket-Key:"))
        .unwrap()
        .trim()
        .to_owned();
    (req_line, ws_key)
}

fn ws_upgrade_response(ws_key: &str) -> Vec<u8> {
    let mut sha = sha1::Sha1::new();
    sha.update(ws_key.as_bytes());
    sha.update(WS_GUID.as_bytes());
    let accept = base64::engine::general_purpose::STANDARD.encode(sha.finalize());
    format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    )
    .into_bytes()
}

#[test]
fn json_scan_single_pass_extracts_binance_okx_bybit_fields_and_rate_limits() {
    // 1. Binance WS-API execution reply with rateLimits[] and integer ticks/lots.
    let binance_ws = br#"{
        "id": 42,
        "status": 200,
        "result": {
            "symbol": "BTCUSDT",
            "orderId": 987654321,
            "clientOrderId": "tw10101abc",
            "transactTime": 1700000000123,
            "price": "65432.10",
            "origQty": "1.250",
            "executedQty": "0.500",
            "status": "PARTIALLY_FILLED",
            "side": "BUY"
        },
        "rateLimits": [
            {"rateLimitType":"ORDERS","interval":"SECOND","intervalNum":10,"limit":100,"count":82},
            {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":6000,"count":1200}
        ]
    }"#;

    let scanned = MsgParser::scan_with_steps(binance_ws, 2, 3).unwrap();
    assert_eq!(scanned.request_id, Some("42"));
    assert_eq!(scanned.reply_id_u64, Some(42));
    assert_eq!(scanned.http_status, Some(200));
    assert_eq!(scanned.symbol, Some("BTCUSDT"));
    assert_eq!(scanned.venue_order_id, Some("987654321"));
    assert_eq!(scanned.client_order_id, Some("tw10101abc"));
    assert_eq!(scanned.status, Some("PARTIALLY_FILLED"));
    assert_eq!(scanned.side, Some("BUY"));
    assert_eq!(scanned.price_ticks, Some(6_543_210));
    assert_eq!(scanned.orig_lots, Some(1_250));
    assert_eq!(scanned.cum_fill_lots, Some(500));
    assert_eq!(scanned.event_time_ms, Some(1_700_000_000_123));
    assert_eq!(scanned.rate_limit_count, 2);
    assert_eq!(
        scanned.rate_limits_slice()[0].rate_limit_type,
        Some("ORDERS")
    );
    assert_eq!(scanned.rate_limits_slice()[0].limit, Some(100));
    assert_eq!(scanned.rate_limits_slice()[0].count, Some(82));
    assert_eq!(scanned.rate_limits_slice()[1].limit, Some(6000));
    assert_eq!(scanned.rate_limits_slice()[1].count, Some(1200));

    // Verify borrowed slices point directly inside `binance_ws` (zero String/Vec copy).
    let base = binance_ws.as_ptr() as usize;
    let end = base + binance_ws.len();
    let cid_ptr = scanned.client_order_id.unwrap().as_ptr() as usize;
    assert!(cid_ptr >= base && cid_ptr < end);

    // 2. OKX v5 UTA order reject inside `data[0]` array envelope.
    let okx_reject = br#"{
        "id": "19",
        "op": "order",
        "code": "1",
        "msg": "Order failed",
        "data": [{
            "clOrdId": "tw1okx999",
            "ordId": "",
            "sCode": "51008",
            "sMsg": "Insufficient balance"
        }]
    }"#;
    let okx = MsgParser::scan(okx_reject).unwrap();
    assert_eq!(okx.reply_id_u64, Some(19));
    assert_eq!(okx.event_kind, Some("order"));
    assert_eq!(okx.client_order_id, Some("tw1okx999"));
    assert_eq!(okx.error_code, Some(51008));
    assert_eq!(okx.error_msg, Some("Insufficient balance"));
    assert!(okx.has_venue_error());

    // 3. Bybit v5 UTA private execution frame.
    let bybit_fill = br#"{
        "topic": "execution",
        "data": [{
            "symbol": "ETHUSDT",
            "orderId": "bb-ord-1",
            "orderLinkId": "tw1bybit77",
            "execId": "trd-55",
            "execType": "Trade",
            "execPrice": "3120.50",
            "execQty": "2.00",
            "cumExecQty": "2.00",
            "execFee": "-0.62",
            "feeCurrency": "USDT"
        }]
    }"#;
    let bb = MsgParser::scan_with_steps(bybit_fill, 2, 2).unwrap();
    assert_eq!(bb.event_kind, Some("execution"));
    assert_eq!(bb.venue_order_id, Some("bb-ord-1"));
    assert_eq!(bb.client_order_id, Some("tw1bybit77"));
    assert_eq!(bb.trade_id, Some("trd-55"));
    assert_eq!(bb.last_fill_ticks, Some(312_050));
    assert_eq!(bb.last_fill_lots, Some(200));
    assert_eq!(bb.cum_fill_lots, Some(200));
    assert_eq!(bb.fee_scaled, Some(-62));
    assert_eq!(bb.fee_asset, Some("USDT"));

    assert_eq!(parse_scaled_i64("12.3400", 2), Some(1234));
    assert_eq!(parse_scaled_i64("12.345", 2), None);
    assert_eq!(parse_step_units_i64("65432.5", 5, 1), Some(130_865));
    assert_eq!(parse_step_units_i64("65432.3", 5, 1), None);
}

#[test]
fn dialer_ws_login_challenge_early_frames_replay_and_guarded_listen_key() {
    let (client_cfg, server_cfg) = tls_pair();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let listen_key = Arc::new(GuardedValue::new(String::new(), 1, None));
    assert_eq!(
        listen_key.set(ThreadTag::REST_QUERY, "lk_live_abc123".to_owned()),
        2
    );

    let server = thread::spawn(move || {
        let (sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut tls =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server_cfg).unwrap(), sock);

        let (req_line, ws_key) = read_http_upgrade(&mut tls);
        assert!(
            req_line.starts_with("GET /ws/lk_live_abc123 HTTP/1.1"),
            "unexpected upgrade path: {req_line}"
        );
        tls.write_all(&ws_upgrade_response(&ws_key)).unwrap();

        // Push an early frame BEFORE sending challenge, and another BEFORE sending auth-ack!
        tls.write_all(&encode_server_ws_text(
            b"{\"e\":\"early_1\",\"orderId\":\"101\"}",
        ))
        .unwrap();
        tls.write_all(&encode_server_ws_text(b"{\"challenge\":\"nonce_xyz\"}"))
            .unwrap();
        tls.flush().unwrap();

        let challenge_reply = read_client_masked_ws_text(&mut tls);
        assert_eq!(
            challenge_reply,
            "{\"op\":\"auth\",\"sig\":\"signed:nonce_xyz\"}"
        );

        tls.write_all(&encode_server_ws_text(
            b"{\"e\":\"early_2\",\"orderId\":\"102\"}",
        ))
        .unwrap();
        tls.write_all(&encode_server_ws_text(
            b"{\"event\":\"login\",\"code\":\"0\"}",
        ))
        .unwrap();
        tls.flush().unwrap();

        // Keep connection open until client reads order reply, then wait for done signal.
        let order_wire = read_client_masked_ws_text(&mut tls);
        assert!(order_wire.contains("\"id\":7"));
        tls.write_all(&encode_server_ws_text(
            b"{\"id\":7,\"status\":200,\"result\":{\"orderId\":555}}",
        ))
        .unwrap();
        tls.flush().unwrap();
        tls.conn.send_close_notify();
        let _ = tls.flush();
    });

    let dialer = Dialer::new().unwrap();
    let baton = dialer
        .add_slot(
            1,
            DialSpec {
                host: "localhost".to_owned(),
                port: addr.port(),
                initial_peers: vec![addr],
                local_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
                nic: None,
                security: DialSecurity::Tls {
                    server_name: "localhost".try_into().unwrap(),
                    config: client_cfg,
                },
                transport: DialTransport::WebSocketAuthenticated {
                    host: "localhost".to_owned(),
                    path_prefix: "/ws".to_owned(),
                    url_token: Some(Arc::clone(&listen_key)),
                    extra_headers: Vec::new(),
                    auth: WsDialAuth::ChallengeResponse {
                        responder: Arc::new(|ch| {
                            Ok(format!("{{\"op\":\"auth\",\"sig\":\"signed:{ch}\"}}"))
                        }),
                        expect_substr: "\"event\":\"login\"".to_owned(),
                    },
                },
                backoff: DialBackoff::new(10, 100),
                ..DialSpec::default()
            },
        )
        .unwrap();
    assert!(baton.want(0));

    let deadline = Instant::now() + Duration::from_secs(5);
    let dialed = loop {
        if let Some(d) = baton.take_ready() {
            break d;
        }
        assert!(Instant::now() < deadline, "WS authenticated dial timed out");
        thread::sleep(Duration::from_millis(2));
    };

    let mut ws_link = dialed.into_ws_link(65_536, 65_536).unwrap();
    assert!(ws_link.is_order_ready());
    assert_eq!(ws_link.early_frame_count(), 2);

    // Replay early_frames in exact arrival order before live traffic.
    let mut replayed = Vec::new();
    ws_link.drain_early_frames(|m| {
        let sc = MsgParser::scan(&m.payload).unwrap();
        replayed.push((
            sc.event_kind.unwrap().to_owned(),
            sc.venue_order_id.unwrap().to_owned(),
        ));
    });
    assert_eq!(
        replayed,
        vec![
            ("early_1".to_owned(), "101".to_owned()),
            ("early_2".to_owned(), "102".to_owned()),
        ]
    );

    // Verify 16-slot reply_id correlation on live order request/reply.
    let order_frame = ws::encode_client(
        ws::Opcode::Text,
        b"{\"id\":7,\"method\":\"order.place\"}",
        256,
    )
    .unwrap();
    ws_link
        .send_order_frame(
            &order_frame,
            7,
            RequestRef {
                order_key: 9001,
                client_seq: 1,
                sent_tsc: 100,
                sent_mono_ns: 1_000_000,
            },
        )
        .unwrap();
    assert_eq!(ws_link.inflight_count(), 1);

    let mut buf = [0u8; 512];
    let matched = loop {
        assert!(Instant::now() < deadline);
        let _ = ws_link
            .connection_mut()
            .drive(Instant::now(), IoBudget::default())
            .unwrap();
        let n = ws_link.connection_mut().read(&mut buf);
        if n > 0 {
            ws_link.decoder_mut().push(&buf[..n]).unwrap();
            if let Some(msg) = ws_link.decoder_mut().next_message().unwrap() {
                let sc = MsgParser::scan(&msg.payload).unwrap();
                break ws_link.match_reply_id(sc.reply_id_u64.unwrap()).unwrap();
            }
        }
        thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(matched.order_key, 9001);
    assert_eq!(ws_link.inflight_count(), 0);

    server.join().unwrap();
}

#[test]
fn fix_link_session_logon_test_request_sequence_gap_and_ref_seq_num_reject() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let stop_srv = Arc::new(AtomicBool::new(false));
    let stop_srv_bg = Arc::clone(&stop_srv);
    let srv = thread::spawn(move || {
        let (_s, _) = listener.accept().unwrap();
        while !stop_srv_bg.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(5));
        }
    });

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut conn = Connection::connect(addr, Security::Plain, Limits::default(), deadline).unwrap();
    while !conn.is_ready() {
        let _ = conn.drive(Instant::now(), IoBudget::default()).unwrap();
        thread::sleep(Duration::from_millis(1));
    }

    let mut fix_link = FixLink::new(
        conn,
        FixSessionConfig::default(),
        FixSessionState::AwaitingLogonAck,
    )
    .unwrap();
    let logon_out = fix_link
        .encode_logon("20261116-00:00:00.000", "141=Y\x01")
        .unwrap();
    assert!(!logon_out.is_empty());
    assert_eq!(fix_link.next_out_seq(), 2);

    // Inbound Logon Ack (34=1, 35=A) activates session.
    let srv_logon = fix::encode(
        "FIX.4.4",
        b"35=A\x0149=VENUE\x0156=TICKWIRE\x0134=1\x0198=0\x01108=30\x01",
        512,
    )
    .unwrap();
    assert_eq!(
        fix_link.on_inbound_frame(&srv_logon, 10_000_000).unwrap(),
        FixInboundAction::LogonAccepted { seq_num: 1 }
    );
    assert!(fix_link.is_order_ready());

    // Record outbound NewOrderSingle at MsgSeqNum=2.
    let order_seq = fix_link.alloc_next_out_seq();
    assert_eq!(order_seq, 2);
    let order_wire = fix::encode(
        "FIX.4.4",
        b"35=D\x0149=TICKWIRE\x0156=VENUE\x0134=2\x0111=tw1fix001\x01",
        512,
    )
    .unwrap();
    fix_link
        .send_order_frame(
            &order_wire,
            order_seq,
            RequestRef {
                order_key: 777,
                client_seq: 12,
                sent_tsc: 500,
                sent_mono_ns: 20_000_000,
            },
        )
        .unwrap();
    assert_eq!(fix_link.inflight_count(), 1);

    // Sequence gap (34=4 when expecting 2) triggers SequenceGap without advancing expected_in_seq.
    let gap_frame = fix::encode(
        "FIX.4.4",
        b"35=0\x0149=VENUE\x0156=TICKWIRE\x0134=4\x01",
        512,
    )
    .unwrap();
    assert_eq!(
        fix_link.on_inbound_frame(&gap_frame, 25_000_000).unwrap(),
        FixInboundAction::SequenceGap {
            expected: 2,
            received: 4
        }
    );
    assert_eq!(fix_link.expected_in_seq(), 2);
    let resend = fix_link
        .encode_resend_request("20261116-00:00:01.000", 2, 0)
        .unwrap();
    assert!(resend.windows(4).any(|w| w == b"35=2"));

    // Peer TestRequest (34=2, 35=1, 112=ping42).
    let peer_tr = fix::encode(
        "FIX.4.4",
        b"35=1\x0149=VENUE\x0156=TICKWIRE\x0134=2\x01112=ping42\x01",
        512,
    )
    .unwrap();
    assert_eq!(
        fix_link.on_inbound_frame(&peer_tr, 30_000_000).unwrap(),
        FixInboundAction::PeerTestRequest {
            seq_num: 2,
            test_req_id: "ping42"
        }
    );

    // Session Reject (34=3, 35=3, 45=2) correlates RefSeqNum=2 back to order_key=777!
    let reject = fix::encode(
        "FIX.4.4",
        b"35=3\x0149=VENUE\x0156=TICKWIRE\x0134=3\x0145=2\x01373=5\x0158=InvalidPrice\x01",
        512,
    )
    .unwrap();
    match fix_link.on_inbound_frame(&reject, 35_000_000).unwrap() {
        FixInboundAction::SessionReject {
            seq_num,
            ref_seq_num,
            request_ref,
            session_reject_reason,
            text,
        } => {
            assert_eq!(seq_num, 3);
            assert_eq!(ref_seq_num, Some(2));
            assert_eq!(request_ref.unwrap().order_key, 777);
            assert_eq!(session_reject_reason, Some(5));
            assert_eq!(text, Some("InvalidPrice"));
        }
        other => panic!("expected SessionReject, got {other:?}"),
    }
    assert_eq!(fix_link.inflight_count(), 0);
    stop_srv.store(true, Ordering::Release);
    srv.join().unwrap();
}

#[test]
fn tls_short_write_immediately_retires_connection_and_wakes_resp_eventfd() {
    let (client_cfg, server_cfg) = tls_pair();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let stop_srv = Arc::new(AtomicBool::new(false));
    let stop_srv_bg = Arc::clone(&stop_srv);

    let srv = thread::spawn(move || {
        let (sock, _) = listener.accept().unwrap();
        let mut tls =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server_cfg).unwrap(), sock);
        // Complete TLS handshake then intentionally stop reading so TCP send buffer fills!
        let mut one = [0u8; 4];
        tls.read_exact(&mut one).unwrap();
        assert_eq!(&one, b"init");
        while !stop_srv_bg.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(10));
        }
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    let limits = Limits {
        max_send_bytes: 1 << 20,
        max_receive_bytes: 1 << 20,
        tls_buffer_bytes: 262_144,
        max_accept_bytes: 262_144,
    };
    let mut conn = Connection::connect(
        addr,
        Security::Tls {
            server_name: "localhost".try_into().unwrap(),
            config: client_cfg,
        },
        limits,
        deadline,
    )
    .unwrap();
    while !conn.is_ready() {
        let _ = conn.drive(Instant::now(), IoBudget::default()).unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    conn.send_frame_nonblocking(b"init").unwrap();

    let mut resp_poll = Poll::new().unwrap();
    let resp_waker = Arc::new(Waker::new(resp_poll.registry(), Token(77)).unwrap());
    let ws_link = WsLink::new(conn, 262_144, 262_144).unwrap();
    let locked = LockedConnection::new(0, Some(Arc::clone(&resp_waker)), None);
    assert!(locked.install(LinkOps::Ws(ws_link)).is_none());
    assert!(locked.is_ready_relaxed());

    // Flood 64 KiB TLS frames without draining server side until kernel socket buffer short-writes.
    let big_payload = vec![b'x'; 65_536];
    let ws_frame = ws::encode_client(ws::Opcode::Binary, &big_payload, 131_072).unwrap();
    let mut short_write_seen = false;
    for seq in 1..=512u64 {
        let outcome = locked.send_frame(
            &ws_frame,
            seq,
            RequestRef {
                order_key: seq,
                client_seq: seq,
                sent_tsc: 0,
                sent_mono_ns: mono_now_ns(),
            },
        );
        // Clear previous in-flight slot if Sent so we don't cap at 16 before saturating socket buffer.
        if outcome == SendOutcome::Sent {
            let _ = locked.match_ws_reply(seq);
        } else if outcome == SendOutcome::ShortWriteRetired {
            short_write_seen = true;
            break;
        } else {
            break;
        }
    }

    assert!(short_write_seen, "expected TLS short-write self-retirement");
    assert!(locked.is_close_pending());
    assert!(!locked.is_ready_relaxed());

    // Subsequent REQ send must immediately fail with Disconnected (never reusing retired stream).
    assert_eq!(
        locked.send_frame(&ws_frame, 999, RequestRef::default()),
        SendOutcome::Disconnected
    );

    // Verify RESP's mio::Poll received the immediate eventfd wakeup on Token(77).
    let mut events = Events::with_capacity(8);
    resp_poll
        .poll(&mut events, Some(Duration::from_millis(200)))
        .unwrap();
    assert!(
        events.iter().any(|ev| ev.token() == Token(77)),
        "RESP eventfd waker must fire on short-write retirement"
    );

    let retired = locked.close_connection().expect("retired link present");
    assert!(retired.is_retired());
    drop(retired);

    stop_srv.store(true, Ordering::Release);
    srv.join().unwrap();
}

#[test]
fn url_connections_multi_socket_ws_and_https_pools_published_routes_and_tcping() {
    let published = Arc::new(PublishedRoutes::new());

    // Multi-connection WS pool (`[local_ip, count = 3]`, shared UTA `product_mask = 0`) +
    // Multi-connection HTTPS pool (`[local_ip, count = 2]`, Spot `product_mask = Spot.mask_bit()`).
    let ws_pool = UrlConnections::new(
        1,
        0,
        LinkKind::Ws,
        0,
        &[UrlBindPair::new(Some(IpAddr::V4(Ipv4Addr::LOCALHOST)), 3).with_colo(true)],
        RouterConfig::new(1, 8, 2),
        SampleMask::ALL,
        None,
        None,
        Arc::clone(&published),
    )
    .unwrap();
    assert_eq!(ws_pool.len(), 3);

    let https_published = Arc::new(PublishedRoutes::new());
    let mut https_pool = UrlConnections::new(
        1,
        10,
        LinkKind::Https,
        Product::Spot.mask_bit(),
        &[UrlBindPair::new(Some(IpAddr::V4(Ipv4Addr::LOCALHOST)), 2)],
        RouterConfig::new(1, 8, 2),
        SampleMask::ALL,
        None,
        None,
        Arc::clone(&https_published),
    )
    .unwrap();
    assert_eq!(https_pool.len(), 2);

    // Install two live plain HTTP keep-alive connections in `https_pool`.
    let l1 = TcpListener::bind("127.0.0.1:0").unwrap();
    let l2 = TcpListener::bind("127.0.0.1:0").unwrap();
    let a1 = l1.local_addr().unwrap();
    let a2 = l2.local_addr().unwrap();
    let accept_t = thread::spawn(move || {
        let (_s1, _) = l1.accept().unwrap();
        let (_s2, _) = l2.accept().unwrap();
        // Also accept one active Tcping probe on l1!
        let (_probe, _) = l1.accept().unwrap();
    });

    let dl = Instant::now() + Duration::from_secs(3);
    let mut c1 = Connection::connect(a1, Security::Plain, Limits::default(), dl).unwrap();
    let mut c2 = Connection::connect(a2, Security::Plain, Limits::default(), dl).unwrap();
    while !c1.is_ready() || !c2.is_ready() {
        let _ = c1.drive(Instant::now(), IoBudget::default()).unwrap();
        let _ = c2.drive(Instant::now(), IoBudget::default()).unwrap();
        thread::sleep(Duration::from_millis(1));
    }

    let e10 = https_pool
        .install_link(10, LinkOps::Https(HttpsLink::new(c1, 65_536, 64).unwrap()))
        .unwrap();
    let e11 = https_pool
        .install_link(11, LinkOps::Https(HttpsLink::new(c2, 65_536, 64).unwrap()))
        .unwrap();

    // Warmup round-robin when unmeasured: None -> 10, last=10 -> 11, last=11 -> 10.
    assert_eq!(
        https_published
            .pick(1, Some(Product::Spot), None, false)
            .unwrap()
            .connection_id,
        10
    );
    assert_eq!(
        https_published
            .pick(1, Some(Product::Spot), Some(10), false)
            .unwrap()
            .connection_id,
        11
    );
    assert!(
        https_published
            .pick(1, Some(Product::UsdM), None, false)
            .is_none()
    );

    // Measureactive Tcping probe via Dialer and feed SampleKind::Tcping into OrderRouter + UrlConnections!
    let dialer = Dialer::new().unwrap();
    dialer
        .request_tcping(
            10,
            a1,
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            None,
            Duration::from_secs(1),
        )
        .unwrap();
    let mut probe_rtt = None;
    while probe_rtt.is_none() {
        assert!(Instant::now() < dl);
        dialer.drain_tcping_results(|res| {
            probe_rtt = res.rtt_us;
        });
        thread::sleep(Duration::from_millis(2));
    }
    assert!(probe_rtt.unwrap() >= 1);

    // Verify 9/8 hysteresis band on PublishedRoutes:
    // Route 10 has ema = 108us, Route 11 has ema = 100us -> 108 * 8 = 864 <= 100 * 9 = 900 -> stays on 10!
    // Once Route 10 degrades to 113us -> 113 * 8 = 904 > 900 -> switches to 11!
    for _ in 0..2 {
        https_pool
            .note_sample(10, e10, SampleKind::Tcping, 108)
            .unwrap();
        https_pool
            .note_sample(11, e11, SampleKind::Order, 100)
            .unwrap();
    }
    assert_eq!(
        https_published
            .pick(1, Some(Product::Spot), Some(10), false)
            .unwrap()
            .connection_id,
        10
    );

    let routes_over_band = [
        VenueOrderRoute {
            connection_id: 10,
            account_ref: 1,
            ready: true,
            colo: false,
            product_mask: Product::Spot.mask_bit(),
            local_ip: 0x7f00_0001,
            ema_score_us: 113,
        },
        VenueOrderRoute {
            connection_id: 11,
            account_ref: 1,
            ready: true,
            colo: false,
            product_mask: Product::Spot.mask_bit(),
            local_ip: 0x7f00_0001,
            ema_score_us: 100,
        },
    ];
    https_published.publish_routes(&routes_over_band).unwrap();
    assert_eq!(
        https_published
            .pick(1, Some(Product::Spot), Some(10), false)
            .unwrap()
            .connection_id,
        11
    );

    // Also verify OrderRouter::note_tcping_sample directly.
    let mut router = OrderRouter::new(RouterConfig::new(1, 8, 1)).unwrap();
    router
        .register_slot(
            0,
            None,
            tickwire_transport::order_router::SendSlot::Shared,
            0,
        )
        .unwrap();
    let ep = router.on_connected(0, None, None).unwrap();
    assert!(
        router
            .note_tcping_sample(0, ep, probe_rtt.unwrap())
            .unwrap()
    );
    assert_eq!(router.route(0).unwrap().health.score(), probe_rtt);

    accept_t.join().unwrap();
}

#[test]
fn latency_tracer_records_stage_spikes_lock_contention_and_queue_wraps() {
    let cal = TscCalibration::from_cycles_per_us(1_000).unwrap();
    let tracer = Arc::new(
        LatencyTracer::new(LatencyTracerConfig {
            ring_capacity: 16,
            max_retained_findings: 64,
            poll_interval: Duration::from_millis(2),
            calibration: cal,
            default_stage_budget_cycles: 1,
            default_iter_budget_cycles: 1,
            default_lock_threshold_cycles: 1,
            housekeeping_cpus: None,
        })
        .unwrap(),
    );

    // 1. Emit stage budget exceeded + iteration spike.
    let mut req_t = tracer.thread_tracer(ThreadTag::REQ);
    req_t.begin_iteration();
    thread::sleep(Duration::from_micros(50));
    req_t.mark_with_budget(MarkSite::SOCKET_SEND, 1);
    req_t.end_iteration_with_budget(1);
    assert!(tracer.flush(Duration::from_secs(2)));

    // 2. Induce real cross-thread spinlock contention (`RESP` holds while `REQ` spins).
    let spin = Arc::new(TimedSpinLock::with_tracer(
        0u64,
        LockId::new(LockDomain::Connection, 7),
        Some(Arc::clone(&tracer)),
        Some(1),
    ));
    let barrier = Arc::new(Barrier::new(2));
    let spin_holder = Arc::clone(&spin);
    let bar_holder = Arc::clone(&barrier);
    let holder_thread = thread::spawn(move || {
        let mut g = spin_holder.lock(ThreadTag::RESP);
        bar_holder.wait();
        thread::sleep(Duration::from_millis(2));
        *g = 42;
    });
    barrier.wait();
    let val = *spin.lock(ThreadTag::REQ);
    assert_eq!(val, 42);
    holder_thread.join().unwrap();

    assert!(tracer.flush(Duration::from_secs(2)));
    let findings = tracer.drain_findings();
    assert!(
        findings.iter().any(|f| matches!(
            f.kind,
            FindingKind::StageBudgetExceeded {
                thread: ThreadTag::REQ,
                to_site: MarkSite::SOCKET_SEND,
                ..
            }
        )),
        "expected StageBudgetExceeded in {findings:?}"
    );
    assert!(
        findings.iter().any(|f| matches!(
            f.kind,
            FindingKind::IterationSpike {
                thread: ThreadTag::REQ,
                ..
            }
        )),
        "expected IterationSpike in {findings:?}"
    );
    assert!(
        findings.iter().any(|f| matches!(
            f.kind,
            FindingKind::LockContention {
                waiter: ThreadTag::REQ,
                holder: ThreadTag::RESP,
                lock_id,
                ..
            } if lock_id == LockId::new(LockDomain::Connection, 7)
        )),
        "expected LockContention(REQ waiting on RESP) in {findings:?}"
    );
}
