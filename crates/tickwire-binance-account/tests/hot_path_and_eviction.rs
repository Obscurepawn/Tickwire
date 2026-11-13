#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod gateway;
#[path = "../../tickwire-binance-private/tests/common/mod.rs"]
mod network;

use std::{
    io::{Read, Write},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_account::{Applied, IntentKey, OrderKey};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::AccountGateway;
use tickwire_account_journal::InMemoryBackend;
use tickwire_binance::request::{Credentials, Market};
use tickwire_binance_account::{
    AccountIngress, Config, Error, Outcome,
    reconciliation::{
        ReadOperation, ReconcileConfig, WireReadPool,
        bootstrap::{AccountSnapshotReader, SnapshotKind, SnapshotQuery},
    },
    rest::{RestConfig, RestOwner},
};
use tickwire_binance_private as private;
use tickwire_journal::store::InMemoryStore;
use tickwire_transport::{
    IoBudget, Limits,
    dialer::{DialBackoff, DialSecurity, DialSpec, DialState, DialTransport, Dialer},
    rustls,
};
use tickwire_venue as v;

fn make_new_order_cmd(
    template_cmd: &v::Command,
    intent_id: u64,
    client_id: &str,
    created_mono_ns: u64,
) -> v::CanonicalCommand {
    let mut cmd = template_cmd.clone();
    cmd.context.request.intent_id = intent_id;
    cmd.context.request.recovery_object = intent_id;
    cmd.context.strategy.strategy_intent_seq = intent_id;
    cmd.context.strategy.created_mono_ns = created_mono_ns;
    cmd.context.request.deadline = created_mono_ns + 5_000_000_000;
    let mut triggers = cmd.context.triggers.as_slice().to_vec();
    for t in &mut triggers {
        t.sequence = intent_id;
        t.observed_mono_ns = created_mono_ns.saturating_sub(1);
    }
    cmd.context.triggers = v::BoundedList::new(triggers).unwrap();
    let v::CommandBody::New(mut order) = cmd.body else {
        unreachable!()
    };
    order.client_id = v::ClientId::new(client_id).unwrap();
    cmd.body = v::CommandBody::New(order);
    v::CanonicalCommand::new(cmd).unwrap()
}

fn ws_cancel_frame(client_id: &str, venue_order_id: u64, event_ms: u64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "subscriptionId": 0,
        "event": {
            "e": "executionReport",
            "E": event_ms,
            "s": "BTCUSDT",
            "c": client_id,
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "0.001",
            "p": "60000",
            "g": -1,
            "C": client_id,
            "x": "CANCELED",
            "X": "CANCELED",
            "i": venue_order_id,
            "l": "0",
            "z": "0",
            "L": "0",
            "n": "0",
            "N": null,
            "T": event_ms,
            "t": -1,
            "I": venue_order_id,
            "m": false,
            "Z": "0",
            "Y": "0",
            "r": "NONE"
        }
    }))
    .unwrap()
}

fn spot_certs() -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let cert = rcgen::generate_simple_self_signed(vec![
        "testnet.binance.vision".into(),
        "ws-api.testnet.binance.vision".into(),
    ])
    .unwrap();
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
        )
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    (
        Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ),
        Arc::new(server),
    )
}

fn read_http_head(stream: &mut network::Server) -> String {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    let deadline = Instant::now() + Duration::from_secs(5);
    while !buf.ends_with(b"\r\n\r\n") {
        assert!(Instant::now() < deadline, "timed out reading HTTP request");
        match stream.read(&mut byte) {
            Ok(1) => buf.push(byte[0]),
            Ok(_) => break,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => thread::yield_now(),
            Err(e) => panic!("server read error: {e}"),
        }
    }
    String::from_utf8(buf).unwrap()
}

struct ManualClock {
    now_ns: u64,
}

impl private::Clock for ManualClock {
    fn sample(&mut self) -> private::Result<private::ClockSample> {
        self.now_ns += 1_000;
        Ok(network::test_sample(
            self.now_ns,
            1_770_000_000_000 + self.now_ns / 1_000_000,
        ))
    }

    fn check_signed(
        &mut self,
        scope: &private::AccountScope,
        stamp: &private::SignedTime,
        now: &private::ClockSample,
        recv_window_micros: u64,
    ) -> private::Result<()> {
        network::check_test_stamp(scope, stamp, now, recv_window_micros)
    }
}

fn drive_intent<B: tickwire_account_journal::Backend>(
    g: &mut AccountGateway<B, gateway::MockAuthority, gateway::TestEncoder>,
    key: IntentKey,
) {
    let deadline = gateway::until();
    while g.status(key) != Some(tickwire_account_gateway::Status::Flushed) {
        assert!(Instant::now() < deadline, "{:?}", g.status(key));
        g.poll(Instant::now(), IoBudget::default()).unwrap();
        thread::yield_now();
    }
}

#[test]
fn windowed_eviction_retires_expired_registrations_and_records_while_preserving_active_orders() {
    let window_ns = 10_000_000_000u64;
    let (mut snapshot, catalog, base_cmd) = gateway::fixture::fixture();
    snapshot.valid_until = u64::MAX / 2;
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
    let mut gw_cfg = gateway::config(&baseline);
    gw_cfg.max_intents = 6;
    let (conn, peer) = gateway::peer(&gw_cfg.route.authority, 256 * 1024);
    let auth = gateway::MockAuthority::default();
    {
        let mut p = auth.policy.lock().unwrap();
        p.gate.now = 1_000_000;
        p.gate.lease_until = u64::MAX / 2;
    }

    let mut gw = AccountGateway::create(
        InMemoryBackend::new(),
        gw_cfg,
        baseline.clone(),
        auth.clone(),
        gateway::TestEncoder,
        conn,
    )
    .unwrap()
    .with_retention_window_ns(Some(window_ns));

    let ingress_cfg = Config {
        physical: tickwire_journal::Config::new([81; 16], [1; 16]),
        writer_epoch: 4,
        baseline,
        account_stream: [2; 16],
        max_records: 20,
        income_association_authority: None,
        income_baseline_authority: None,
        max_registrations: 4,
        max_raw_bytes: 32_768,
        max_retained_bytes: 1 << 20,
    };
    let mut ingress = AccountIngress::create(InMemoryStore::new(), ingress_cfg)
        .unwrap()
        .with_retention_window_ns(Some(window_ns));

    // Set up a live private WebSocket server so all cancels + final fill flow through `ingress.ingest`.
    let (tls, server) = network::certs();
    let listener = network::listener();
    let address = listener.local_addr().unwrap();
    let (tx_frame, rx_frame) = std::sync::mpsc::channel::<Option<Vec<u8>>>();
    let ws_server = thread::spawn(move || {
        let mut s = network::accept(&listener, server);
        network::upgrade(&mut s, "/ws-api/v3");
        let req = network::json(&mut s);
        network::verify_signature(&req, 0);
        network::reply(&mut s, &req["id"], serde_json::json!({"subscriptionId": 0}));
        while let Ok(Some(payload)) = rx_frame.recv() {
            network::frame(&mut s, 1, &payload);
        }
        let _ = s.read(&mut [0]);
    });

    let signing = private::SigningKey::hmac(network::SECRET).unwrap();
    let mut client = private::PrivateClient::new(
        private::SessionConfig::new(network::scope(private::Market::Spot)),
        private::Authentication::SpotSignature {
            api_key: network::KEY,
            signing_key: &signing,
        },
        private::NetworkConfig {
            stream_address: address,
            management_address: None,
            tls,
        },
        9,
    )
    .unwrap();
    let mut clock = ManualClock { now_ns: 1_000_000 };
    let mut permit = |_: &private::Charge, _: &private::ClockSample| true;

    let mut deliver_one = |client: &mut private::PrivateClient<'_>,
                           clock: &mut ManualClock,
                           ingress: &mut AccountIngress<InMemoryStore>,
                           gw: &mut AccountGateway<_, _, _>|
     -> Outcome {
        let mut out = None;
        let deadline = gateway::until();
        while out.is_none() {
            assert!(Instant::now() < deadline);
            client
                .poll(clock, private::PollBudget::default(), &mut permit, |d| {
                    if let private::Delivery::Event(ev) = d {
                        out = Some(ingress.ingest(gw, &ev).map_err(|_| private::Error::Sink)?);
                    }
                    Ok(())
                })
                .unwrap();
            thread::yield_now();
        }
        out.unwrap()
    };

    // 1. Admit and register an active resting order (#1) that stays live across all windows.
    let resting_cid = "rest-active-1";
    let resting_cmd = make_new_order_cmd(&base_cmd, 1, resting_cid, 1_000_000);
    let exec_epoch = resting_cmd.command().context.request.intent_exec_epoch;
    let mut resting_id = [0u8; 16];
    resting_id[..8].copy_from_slice(&1u64.to_le_bytes());
    ingress
        .submit(
            &mut gw,
            resting_cmd.clone(),
            &[resting_id],
            1_000_000,
            gateway::until(),
        )
        .unwrap();
    drive_intent(
        &mut gw,
        IntentKey {
            epoch: exec_epoch,
            intent_id: 1,
        },
    );

    // 2. Within the same retention window, fill up the remaining 3 registration slots (total 4 == max_registrations).
    for idx in 2..=4u64 {
        let cid = format!("win0-{idx}");
        let now_ns = 1_000_000 + idx * 10_000;
        auth.policy.lock().unwrap().gate.now = now_ns;
        clock.now_ns = clock.now_ns.max(now_ns);
        let cmd = make_new_order_cmd(&base_cmd, idx, &cid, now_ns);
        let mut oid = [0u8; 16];
        oid[..8].copy_from_slice(&idx.to_le_bytes());
        ingress
            .submit(&mut gw, cmd, &[oid], now_ns, gateway::until())
            .unwrap();
        drive_intent(
            &mut gw,
            IntentKey {
                epoch: exec_epoch,
                intent_id: idx,
            },
        );
        tx_frame
            .send(Some(ws_cancel_frame(&cid, 10_000 + idx, 10 + idx)))
            .unwrap();
        let outcome = deliver_one(&mut client, &mut clock, &mut ingress, &mut gw);
        assert!(matches!(
            outcome,
            Outcome::Applied {
                applied: Applied::New,
                ..
            }
        ));
        gw.record_fact(tickwire_account::RecoveryRecord::FeesReconciled {
            order: OrderKey {
                intent: IntentKey {
                    epoch: exec_epoch,
                    intent_id: idx,
                },
                client_id: v::ClientId::new(&cid).unwrap(),
            },
            cumulative: gateway::fixture::f(0),
        })
        .unwrap();
    }

    // Before retention_window_ns elapses, submitting a 5th order inside the same window hits Error::Capacity!
    let overflow_now = clock.now_ns + 10_000;
    auth.policy.lock().unwrap().gate.now = overflow_now;
    let overflow_cmd = make_new_order_cmd(&base_cmd, 5, "win0-overflow", overflow_now);
    let mut overflow_oid = [0u8; 16];
    overflow_oid[..8].copy_from_slice(&5u64.to_le_bytes());
    assert!(matches!(
        ingress.submit(
            &mut gw,
            overflow_cmd,
            &[overflow_oid],
            overflow_now,
            gateway::until(),
        ),
        Err(Error::Capacity)
    ));

    // 3. Advance across 20 monotonic windows (> 4 * max_registrations and > 2 * max_records total records).
    for step in 0..20u64 {
        let intent_id = 10 + step;
        let now_ns = (step + 2) * (window_ns + 1_000_000_000);
        auth.policy.lock().unwrap().gate.now = now_ns;
        clock.now_ns = now_ns;
        let cid = format!("seq-ord-{intent_id}");
        let cmd = make_new_order_cmd(&base_cmd, intent_id, &cid, now_ns);
        let mut oid = [0u8; 16];
        oid[..8].copy_from_slice(&intent_id.to_le_bytes());
        ingress
            .submit(&mut gw, cmd, &[oid], now_ns, gateway::until())
            .unwrap();
        drive_intent(
            &mut gw,
            IntentKey {
                epoch: exec_epoch,
                intent_id,
            },
        );
        tx_frame
            .send(Some(ws_cancel_frame(&cid, 50_000 + step, 1_000 + step)))
            .unwrap();
        let outcome = deliver_one(&mut client, &mut clock, &mut ingress, &mut gw);
        assert!(matches!(
            outcome,
            Outcome::Applied {
                applied: Applied::New,
                ..
            }
        ));
        gw.record_fact(tickwire_account::RecoveryRecord::FeesReconciled {
            order: OrderKey {
                intent: IntentKey {
                    epoch: exec_epoch,
                    intent_id,
                },
                client_id: v::ClientId::new(&cid).unwrap(),
            },
            cumulative: gateway::fixture::f(0),
        })
        .unwrap();
    }

    // 4. Verify the long-lived resting order (#1) was NEVER evicted and still correlates on live WS fill!
    let resting_order_key = OrderKey {
        intent: IntentKey {
            epoch: resting_cmd.command().context.request.intent_exec_epoch,
            intent_id: 1,
        },
        client_id: v::ClientId::new(resting_cid).unwrap(),
    };
    assert!(gw.account().order(&resting_order_key).is_some());

    let ws_fill = serde_json::to_vec(&serde_json::json!({
        "subscriptionId": 0,
        "event": {
            "e": "executionReport",
            "E": 250_000,
            "s": "BTCUSDT",
            "c": resting_cid,
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "0.001",
            "p": "60000",
            "g": -1,
            "C": "",
            "x": "TRADE",
            "X": "FILLED",
            "i": 90001,
            "l": "0.001",
            "z": "0.001",
            "L": "60000",
            "n": "0.01",
            "N": "USDT",
            "T": 249_999,
            "t": 701,
            "I": 702,
            "m": true,
            "Z": "60",
            "Y": "60",
            "r": "NONE"
        }
    }))
    .unwrap();
    tx_frame.send(Some(ws_fill)).unwrap();
    let fill_outcome = deliver_one(&mut client, &mut clock, &mut ingress, &mut gw);
    assert!(matches!(
        fill_outcome,
        Outcome::Applied {
            applied: Applied::New,
            ..
        }
    ));
    assert!(gw.account().order(&resting_order_key).unwrap().terminal);

    let _ = tx_frame.send(None);
    client.stop(private::StopMode::Disconnect).unwrap();
    drop(client);
    ws_server.join().unwrap();
    drop(gw);
    let _ = peer.join().unwrap();
}

#[test]
fn wire_read_pool_reuses_keep_alive_tls_socket_and_accepts_dial_baton_handoff() {
    let (snapshot, catalog, _) = gateway::fixture::fixture();
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
    let gw_cfg = gateway::config(&baseline);
    let (conn, peer) = gateway::peer(&gw_cfg.route.authority, 16_384);
    let gw = AccountGateway::create(
        InMemoryBackend::new(),
        gw_cfg,
        baseline.clone(),
        gateway::MockAuthority::default(),
        gateway::TestEncoder,
        conn,
    )
    .unwrap();
    let ingress_cfg = Config {
        physical: tickwire_journal::Config::new([82; 16], [1; 16]),
        writer_epoch: 4,
        baseline,
        account_stream: [2; 16],
        max_records: 64,
        income_association_authority: None,
        income_baseline_authority: None,
        max_registrations: 16,
        max_raw_bytes: 32_768,
        max_retained_bytes: 1 << 20,
    };
    let ingress = AccountIngress::create(InMemoryStore::new(), ingress_cfg).unwrap();
    let mut owner = RestOwner::from_parts(
        ingress,
        gw,
        RestConfig {
            connection_epoch: 5,
            max_response_bytes: 32_768,
            max_headers: 32,
        },
    )
    .unwrap();

    let (tls, server_cfg) = spot_certs();
    let scope = network::scope(private::Market::Spot);
    let signing = private::SigningKey::hmac(network::SECRET).unwrap();

    // Establish an explicit reconciliation gap marker on owner.
    let mut private_client = private::PrivateClient::new(
        private::SessionConfig::new(scope.clone()),
        private::Authentication::SpotSignature {
            api_key: network::KEY,
            signing_key: &signing,
        },
        private::NetworkConfig {
            stream_address: "127.0.0.1:1".parse().unwrap(),
            management_address: None,
            tls: tls.clone(),
        },
        5,
    )
    .unwrap();
    let mut clock = network::TestClock {
        start: Instant::now(),
    };
    private_client
        .poll(
            &mut clock,
            private::PollBudget::default(),
            &mut |_: &private::Charge, _: &private::ClockSample| false,
            |_| Ok(()),
        )
        .unwrap();
    let gap = private_client.current_gap().clone();
    owner.begin_reconciliation(&gap).unwrap();

    // Part A: 3 sequential GET queries served over a SINGLE keep-alive TLS connection!
    let listener_a = network::listener();
    let addr_a = listener_a.local_addr().unwrap();
    let accepts_a = Arc::new(AtomicUsize::new(0));
    let accepts_a_srv = Arc::clone(&accepts_a);
    let srv_cfg_a = Arc::clone(&server_cfg);
    let server_a = thread::spawn(move || {
        let mut s = network::accept(&listener_a, srv_cfg_a);
        accepts_a_srv.fetch_add(1, Ordering::SeqCst);
        for expected in [
            "/api/v3/account?",
            "/api/v3/openOrders?",
            "/api/v3/openOrderList?",
        ] {
            let head = read_http_head(&mut s);
            let line = head.lines().next().unwrap();
            let path = line.split_whitespace().nth(1).unwrap();
            assert!(
                path.starts_with(expected),
                "unexpected request path: {path}"
            );
            let body = if expected == "/api/v3/account?" {
                r#"{"canTrade":true,"accountType":"SPOT","balances":[{"asset":"BTC","free":"1","locked":"0"},{"asset":"USDT","free":"1000","locked":"0"}]}"#
            } else {
                "[]"
            };
            write!(
                s,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
            s.flush().unwrap();
        }
        let _ = s.read(&mut [0u8; 1]);
    });

    let rc_a = ReconcileConfig {
        scope: scope.clone(),
        address: addr_a,
        tls: tls.clone(),
        limits: Limits::default(),
        first_connection_epoch: 10,
        request_timeout_ns: 5_000_000_000,
        recv_window_micros: 5_000_000,
        max_response_bytes: 32_768,
        max_headers: 32,
    };
    let credentials = Credentials {
        market: Market::Spot,
        environment: v::Environment::Test,
        account: 2,
        api_key: network::KEY,
        signing_key: &signing,
    };
    let mut reader_a = AccountSnapshotReader::new(rc_a).unwrap();
    for kind in [
        SnapshotKind::Account,
        SnapshotKind::OpenOrders,
        SnapshotKind::ConditionalOrders,
    ] {
        let q = SnapshotQuery {
            kind,
            private_connection_epoch: gap.connection_epoch(),
            gap_generation: gap.generation(),
        };
        let mut permit = |_: &private::AccountScope,
                          _: SnapshotQuery,
                          _: u64,
                          _: ReadOperation,
                          _: &private::ClockSample| true;
        reader_a
            .start(&mut owner, q, &credentials, &mut clock, &mut permit)
            .unwrap();
        let deadline = gateway::until();
        loop {
            assert!(Instant::now() < deadline);
            if let Some(outcome) = reader_a
                .poll(&mut owner, &mut clock, &mut permit, IoBudget::default())
                .unwrap()
            {
                assert!(outcome.evidence.is_some());
                break;
            }
            thread::yield_now();
        }
    }
    assert_eq!(accepts_a.load(Ordering::SeqCst), 1);
    assert_eq!(reader_a.pool().fresh_connects(), 1);
    assert_eq!(reader_a.pool().reused_connections(), 2);
    assert_eq!(reader_a.pool().idle_len(), 1);
    reader_a.pool().clear();
    server_a.join().unwrap();

    // Part B: Background Dialer + DialBaton pre-warmed TLS socket handoff + keep-alive reuse!
    let listener_b = network::listener();
    let addr_b = listener_b.local_addr().unwrap();
    let accepts_b = Arc::new(AtomicUsize::new(0));
    let accepts_b_srv = Arc::clone(&accepts_b);
    let server_b = thread::spawn(move || {
        let mut s = network::accept(&listener_b, server_cfg);
        accepts_b_srv.fetch_add(1, Ordering::SeqCst);
        for expected in ["/api/v3/account?", "/api/v3/openOrders?"] {
            let head = read_http_head(&mut s);
            let line = head.lines().next().unwrap();
            let path = line.split_whitespace().nth(1).unwrap();
            assert!(
                path.starts_with(expected),
                "unexpected request path: {path}"
            );
            let body = if expected == "/api/v3/account?" {
                r#"{"canTrade":true,"accountType":"SPOT","balances":[{"asset":"BTC","free":"1","locked":"0"},{"asset":"USDT","free":"1000","locked":"0"}]}"#
            } else {
                "[]"
            };
            write!(
                s,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
            s.flush().unwrap();
        }
        let _ = s.read(&mut [0u8; 1]);
    });

    let dialer = Dialer::new().unwrap();
    let spec = DialSpec {
        host: "testnet.binance.vision".to_owned(),
        port: addr_b.port(),
        initial_peers: vec![addr_b],
        local_ip: None,
        security: DialSecurity::Tls {
            server_name: "testnet.binance.vision".try_into().unwrap(),
            config: tls.clone(),
        },
        limits: Limits::default(),
        transport: DialTransport::PlainOrTls,
        connect_timeout: Duration::from_secs(5),
        dial_timeout: Duration::from_secs(5),
        backoff: DialBackoff::new(10, 100),
    };
    let baton = dialer.add_slot(1, spec).unwrap();
    let pool_b = WireReadPool::new().with_dial_baton(Arc::clone(&baton));
    assert!(pool_b.request_prewarm(0));
    let deadline = gateway::until();
    while baton.state() != DialState::Ready {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(2));
    }

    let rc_b = ReconcileConfig {
        scope,
        address: addr_b,
        tls,
        limits: Limits::default(),
        first_connection_epoch: 30,
        request_timeout_ns: 5_000_000_000,
        recv_window_micros: 5_000_000,
        max_response_bytes: 32_768,
        max_headers: 32,
    };
    let mut reader_b = AccountSnapshotReader::new(rc_b)
        .unwrap()
        .with_pool(pool_b.clone());
    for kind in [SnapshotKind::Account, SnapshotKind::OpenOrders] {
        let q = SnapshotQuery {
            kind,
            private_connection_epoch: gap.connection_epoch(),
            gap_generation: gap.generation(),
        };
        let mut permit = |_: &private::AccountScope,
                          _: SnapshotQuery,
                          _: u64,
                          _: ReadOperation,
                          _: &private::ClockSample| true;
        reader_b
            .start(&mut owner, q, &credentials, &mut clock, &mut permit)
            .unwrap();
        let deadline = gateway::until();
        loop {
            assert!(Instant::now() < deadline);
            if let Some(outcome) = reader_b
                .poll(&mut owner, &mut clock, &mut permit, IoBudget::default())
                .unwrap()
            {
                assert!(outcome.evidence.is_some());
                break;
            }
            thread::yield_now();
        }
    }
    assert_eq!(accepts_b.load(Ordering::SeqCst), 1);
    assert_eq!(pool_b.baton_handoffs(), 1);
    assert_eq!(pool_b.fresh_connects(), 0);
    assert_eq!(pool_b.reused_connections(), 1);
    pool_b.clear();
    dialer.stop();
    server_b.join().unwrap();

    drop(owner);
    let _ = peer.join().unwrap();
}
