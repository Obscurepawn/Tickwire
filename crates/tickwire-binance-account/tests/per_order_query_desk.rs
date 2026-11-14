#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod gateway;
#[path = "../../tickwire-binance-private/tests/common/mod.rs"]
mod network;

use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::{Shutdown, SocketAddr, TcpListener},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_account::{
    Applied, AskKind, DoubtReason, Fixed, IntentKey, OrderKey, QueryConfig, QueryResolution,
};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::{AccountGateway, ConnectionConfig, Status};
use tickwire_account_journal::FileBackend;
use tickwire_binance::request::{Credentials, Market};
use tickwire_binance_account::{
    AccountIngress, Config, Outcome, PerOrderQueryConfig, PerOrderQueryDriver, PerOrderQueryPermit,
    QueryOperation,
    rest::{RestConfig, RestDisposition, RestOutcome, RestOwner},
};
use tickwire_binance_private as private;
use tickwire_journal::store::FileStore;
use tickwire_transport::{Connection, IoBudget, Limits, Security, rustls};
use tickwire_venue as v;

#[derive(Clone, Debug)]
enum ServerStep {
    Respond {
        status: u16,
        body: Vec<u8>,
        close_after: bool,
    },
    DropAfterRequest,
}

struct ScriptedTlsHttpServer {
    address: SocketAddr,
    steps: Arc<Mutex<VecDeque<ServerStep>>>,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl ScriptedTlsHttpServer {
    fn spawn(server_cfg: Arc<rustls::ServerConfig>, verify_binance_hmac: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let steps = Arc::new(Mutex::new(VecDeque::<ServerStep>::new()));
        let requests = Arc::new(Mutex::new(Vec::<String>::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let steps_w = Arc::clone(&steps);
        let reqs_w = Arc::clone(&requests);
        let stop_w = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            while !stop_w.load(Ordering::Acquire) {
                let socket = match listener.accept() {
                    Ok((s, _)) => s,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_millis(25)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let conn = match rustls::ServerConnection::new(Arc::clone(&server_cfg)) {
                    Ok(c) => c,
                    Err(_) => continue,
                };
                let mut stream = rustls::StreamOwned::new(conn, socket);
                let mut buf = Vec::new();
                while !stop_w.load(Ordering::Acquire) {
                    let mut byte = [0u8; 1];
                    match stream.read(&mut byte) {
                        Ok(1) => {
                            buf.push(byte[0]);
                            if !buf.ends_with(b"\r\n\r\n") {
                                continue;
                            }
                            let head = String::from_utf8_lossy(&buf).into_owned();
                            buf.clear();
                            let first_line = head.lines().next().unwrap_or("").to_owned();
                            if verify_binance_hmac {
                                verify_http_get_hmac(&head);
                            }
                            reqs_w.lock().unwrap().push(first_line);
                            let action = loop {
                                if stop_w.load(Ordering::Acquire) {
                                    break None;
                                }
                                if let Some(step) = steps_w.lock().unwrap().pop_front() {
                                    break Some(step);
                                }
                                thread::sleep(Duration::from_millis(2));
                            };
                            let Some(action) = action else {
                                break;
                            };
                            match action {
                                ServerStep::Respond {
                                    status,
                                    body,
                                    close_after,
                                } => {
                                    let reason = match status {
                                        200 => "OK",
                                        400 => "Bad Request",
                                        401 => "Unauthorized",
                                        418 => "I'm a teapot",
                                        429 => "Too Many Requests",
                                        503 => "Service Unavailable",
                                        _ => "Response",
                                    };
                                    let conn_hdr = if close_after { "close" } else { "keep-alive" };
                                    let header = format!(
                                        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: {conn_hdr}\r\n\r\n",
                                        body.len()
                                    );
                                    if stream.write_all(header.as_bytes()).is_err()
                                        || stream.write_all(&body).is_err()
                                        || stream.flush().is_err()
                                    {
                                        break;
                                    }
                                    if close_after {
                                        let _ = stream.sock.shutdown(Shutdown::Both);
                                        break;
                                    }
                                }
                                ServerStep::DropAfterRequest => {
                                    let _ = stream.sock.shutdown(Shutdown::Both);
                                    break;
                                }
                            }
                        }
                        Ok(_) => break,
                        Err(e)
                            if matches!(
                                e.kind(),
                                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                            ) =>
                        {
                            continue;
                        }
                        Err(_) => break,
                    }
                }
            }
        });

        Self {
            address,
            steps,
            requests,
            stop,
            handle: Some(handle),
        }
    }

    fn push_json(&self, status: u16, value: serde_json::Value) {
        self.steps.lock().unwrap().push_back(ServerStep::Respond {
            status,
            body: serde_json::to_vec(&value).unwrap(),
            close_after: false,
        });
    }

    fn push_drop(&self) {
        self.steps
            .lock()
            .unwrap()
            .push_back(ServerStep::DropAfterRequest);
    }

    fn recorded_requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for ScriptedTlsHttpServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn verify_http_get_hmac(head: &str) {
    let first = head.lines().next().unwrap();
    let mut parts = first.split_whitespace();
    assert_eq!(parts.next(), Some("GET"));
    let target = parts.next().unwrap();
    let (_path, query) = target.split_once('?').expect("signed query string");
    let (preimage, sig_part) = query.rsplit_once("&signature=").expect("signature param");
    let has_api_key = head.lines().any(|line| {
        line.split_once(':').is_some_and(|(k, v)| {
            k.trim().eq_ignore_ascii_case("x-mbx-apikey") && v.trim() == network::KEY
        })
    });
    assert!(has_api_key, "missing X-MBX-APIKEY header in {head}");
    let mac = ring::hmac::sign(
        &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, network::SECRET),
        preimage.as_bytes(),
    );
    let expected: String = mac.as_ref().iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(sig_part, expected, "invalid HMAC signature on {target}");
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

struct ControllableClock {
    now_ns: Arc<std::sync::atomic::AtomicU64>,
}

impl ControllableClock {
    fn new(initial_ns: u64) -> Self {
        Self {
            now_ns: Arc::new(std::sync::atomic::AtomicU64::new(initial_ns)),
        }
    }

    fn set_ns(&self, ns: u64) {
        self.now_ns.store(ns, Ordering::SeqCst);
    }

    fn add_ns(&self, delta_ns: u64) -> u64 {
        self.now_ns.fetch_add(delta_ns, Ordering::SeqCst) + delta_ns
    }

    fn now_ns(&self) -> u64 {
        self.now_ns.load(Ordering::SeqCst)
    }

    fn querier_clock(&self) -> tickwire_transport::ClockFn {
        let shared = Arc::clone(&self.now_ns);
        Arc::new(move || shared.load(Ordering::SeqCst))
    }
}

impl private::Clock for ControllableClock {
    fn sample(&mut self) -> private::Result<private::ClockSample> {
        let ns = self.now_ns.fetch_add(1_000, Ordering::SeqCst) + 1_000;
        Ok(network::test_sample(ns, 1_770_000_000_000 + ns / 1_000_000))
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

fn build_spot_baseline(fee_rate_bps: Option<&str>) -> (CanonicalBaseline, v::Command) {
    let (mut snapshot, mut catalog, base_cmd) = gateway::fixture::fixture();
    snapshot.valid_until = u64::MAX / 2;
    catalog.entries[0]
        .strategy_limits
        .push((9, gateway::fixture::units(100)));
    if let Some(rate) = fee_rate_bps {
        let parsed = Fixed::parse(rate, 8).unwrap();
        catalog.entries[0].fees.rates = [parsed; 3];
    }
    (CanonicalBaseline::new(snapshot, catalog).unwrap(), base_cmd)
}

#[allow(clippy::too_many_arguments)]
fn make_strategy_new_cmd(
    base_cmd: &v::Command,
    strategy_id: u64,
    instance_id: u8,
    intent_id: u64,
    strategy_seq: u64,
    client_id: &str,
    tif: v::TimeInForce,
    now_ns: u64,
) -> (v::CanonicalCommand, OrderKey) {
    let mut cmd = base_cmd.clone();
    cmd.context.strategy.logical_strategy_id = strategy_id;
    cmd.context.strategy.instance_id = [instance_id; 16];
    cmd.context.strategy.strategy_intent_seq = strategy_seq;
    cmd.context.strategy.created_mono_ns = now_ns;
    cmd.context.request.intent_id = intent_id;
    cmd.context.request.recovery_object = intent_id;
    cmd.context.request.deadline = now_ns + 30_000_000_000;
    let mut triggers = cmd.context.triggers.as_slice().to_vec();
    for t in &mut triggers {
        t.sequence = intent_id;
        t.observed_mono_ns = now_ns.saturating_sub(1);
    }
    cmd.context.triggers = v::BoundedList::new(triggers).unwrap();
    let v::CommandBody::New(mut spec) = cmd.body else {
        unreachable!()
    };
    let cid = v::ClientId::new(client_id).unwrap();
    spec.client_id = cid.clone();
    spec.time_in_force = Some(tif);
    cmd.body = v::CommandBody::New(spec);
    let exec_epoch = cmd.context.request.intent_exec_epoch;
    let canonical = v::CanonicalCommand::new(cmd).unwrap();
    (
        canonical,
        OrderKey {
            intent: IntentKey {
                epoch: exec_epoch,
                intent_id,
            },
            client_id: cid,
        },
    )
}

struct AllowAllPermit;

impl PerOrderQueryPermit for AllowAllPermit {
    fn allow(
        &mut self,
        _scope: &private::AccountScope,
        _order: &OrderKey,
        _request_epoch: u64,
        _operation: QueryOperation,
        _now: &private::ClockSample,
    ) -> bool {
        true
    }
}

fn ingress_config(baseline: &CanonicalBaseline) -> Config {
    Config {
        physical: tickwire_journal::Config::new([81; 16], [1; 16]),
        writer_epoch: 4,
        baseline: baseline.clone(),
        account_stream: [2; 16],
        max_records: 64,
        income_association_authority: None,
        income_baseline_authority: None,
        max_registrations: 32,
        max_raw_bytes: 32_768,
        max_retained_bytes: 1 << 20,
    }
}

fn drive_rest_outcome(
    owner: &mut RestOwner<FileStore, FileBackend, gateway::MockAuthority, gateway::TestEncoder>,
    clock: &ControllableClock,
) -> RestOutcome {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for RestOwner outcome"
        );
        let mono = clock.add_ns(1_000);
        if let Some(outcome) = owner
            .poll(Instant::now(), mono, IoBudget::default())
            .unwrap()
        {
            return outcome;
        }
        thread::sleep(Duration::from_millis(2));
    }
}

fn drive_query_resolution(
    driver: &mut PerOrderQueryDriver,
    owner: &mut RestOwner<FileStore, FileBackend, gateway::MockAuthority, gateway::TestEncoder>,
    creds: &Credentials<'_>,
    clock: &mut ControllableClock,
) -> Vec<QueryResolution> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut dispatched = false;
    loop {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for PerOrderQueryDriver reply (dispatched={dispatched})"
        );
        let step = driver
            .poll(owner, creds, clock, &mut AllowAllPermit)
            .unwrap();
        if step.dispatched > 0 {
            dispatched = true;
        }
        if !step.resolutions.is_empty() || owner.account().frozen() {
            return step.resolutions;
        }
        thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn ambiguous_place_isolates_single_order_allows_sibling_strategy_and_closes_uncreated_after_grace_with_wal_recovery()
 {
    let (client_tls, server_tls) = spot_certs();
    let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), false);
    let query_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), true);

    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_spot_baseline(None);
    let gw_cfg = gateway::config(&baseline);
    let auth = gateway::MockAuthority::default();
    {
        let mut p = auth.policy.lock().unwrap();
        p.gate.now = 1_000_000;
        p.gate.lease_until = u64::MAX / 2;
    }

    let gw = AccountGateway::create(
        FileBackend::open(&account_dir.0).unwrap(),
        gateway::config(&baseline),
        baseline.clone(),
        auth.clone(),
        gateway::TestEncoder,
        ConnectionConfig {
            address: order_server.address,
            tls: Arc::clone(&client_tls),
            limits: Limits::default(),
            connect_deadline: gateway::until(),
        },
    )
    .unwrap();
    let ingress_cfg = ingress_config(&baseline);
    let ingress =
        AccountIngress::create(FileStore::open(&inbox_dir.0).unwrap(), ingress_cfg.clone())
            .unwrap();

    let query_cfg = QueryConfig {
        status_query_delay_ns: 6_000_000_000,
        place_retry_ns: 1_000_000_000,
        not_found_grace_ns: 10_000_000_000,
        ..QueryConfig::default()
    };
    let mut owner = RestOwner::from_parts(
        ingress,
        gw,
        RestConfig {
            connection_epoch: 10,
            max_response_bytes: 32_768,
            max_headers: 32,
        },
    )
    .unwrap()
    .with_per_order_query_desk(query_cfg);

    let mut clock = ControllableClock::new(1_000_000);
    let signing_key = tickwire_binance::auth::SigningKey::hmac(network::SECRET).unwrap();
    let creds = Credentials {
        market: Market::Spot,
        environment: v::Environment::Test,
        account: 2,
        api_key: network::KEY,
        signing_key: &signing_key,
    };
    let mut driver = PerOrderQueryDriver::with_querier_clock(
        PerOrderQueryConfig {
            scope: network::scope(Market::Spot),
            address: query_server.address,
            local_ip: None,
            tls: Arc::clone(&client_tls),
            limits: Limits::default(),
            first_connection_epoch: 100,
            request_timeout_ns: 2_000_000_000,
            connect_timeout_ms: 2_000,
            retry_interval_ms: 10,
            recv_window_micros: 5_000_000,
            max_response_bytes: 32_768,
            trade_page_limit: 16,
            queue_capacity: 64,
        },
        clock.querier_clock(),
    )
    .unwrap();

    // 1. Strategy A (id=8) submits Order 1 -> receives -1007 ambiguous timeout.
    order_server.push_json(
        503,
        serde_json::json!({
            "code": -1007,
            "msg": "Timeout waiting for response from backend server. Send status unknown; execution status unknown."
        }),
    );
    let now_1 = clock.now_ns();
    auth.policy.lock().unwrap().gate.now = now_1;
    let (cmd_1, order_1_key) = make_strategy_new_cmd(
        &base_cmd,
        8,
        4,
        1,
        1,
        "order-a-1",
        v::TimeInForce::Gtc,
        now_1,
    );
    owner
        .submit(cmd_1, &[[11; 16]], now_1, gateway::until())
        .unwrap();
    let out_1 = drive_rest_outcome(&mut owner, &clock);
    assert_eq!(out_1.disposition, RestDisposition::Unknown);
    assert!(
        !owner.account().frozen(),
        "single-order -1007 must NOT freeze account"
    );
    assert!(!owner.gateway().halted());
    assert_eq!(
        owner.account().order_in_doubt(&order_1_key),
        Some(DoubtReason::PlaceAmbiguous)
    );
    assert_eq!(
        owner
            .account()
            .asset(gateway::fixture::key(2))
            .unwrap()
            .reserved,
        gateway::fixture::units(60),
        "Order 1 reservation must remain locked while in doubt"
    );
    assert_eq!(
        owner.query_desk().unwrap().ask_kind(&order_1_key),
        Some(AskKind::Place)
    );

    // 2. Healthy sibling Strategy B (id=9) submits Order 2 and fills cleanly while Order 1 is in doubt.
    order_server.push_json(
        200,
        serde_json::json!({
            "symbol": "BTCUSDT",
            "orderId": 202,
            "orderListId": -1,
            "clientOrderId": "order-b-1",
            "transactTime": 1_770_000_000_001u64,
            "price": "60000",
            "origQty": "0.001",
            "executedQty": "0",
            "cummulativeQuoteQty": "0",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "workingTime": 1_770_000_000_001u64
        }),
    );
    let now_2 = clock.add_ns(10_000);
    auth.policy.lock().unwrap().gate.now = now_2;
    let (cmd_2, order_2_key) = make_strategy_new_cmd(
        &base_cmd,
        9,
        5,
        2,
        1,
        "order-b-1",
        v::TimeInForce::Gtc,
        now_2,
    );
    owner
        .submit(cmd_2, &[[22; 16]], now_2, gateway::until())
        .unwrap();
    let out_2 = drive_rest_outcome(&mut owner, &clock);
    assert_eq!(out_2.disposition, RestDisposition::Observed);
    assert!(!owner.account().frozen());
    assert!(owner.account().order(&order_2_key).unwrap().confirmed);
    assert_eq!(
        owner
            .account()
            .asset(gateway::fixture::key(2))
            .unwrap()
            .reserved,
        gateway::fixture::units(120)
    );

    // Deliver private WS executionReport FILL for sibling Order 2.
    let (ws_tls, ws_server_tls) = spot_certs();
    let ws_listener = network::listener();
    let ws_addr = ws_listener.local_addr().unwrap();
    let ws_fill = serde_json::to_vec(&serde_json::json!({
        "subscriptionId": 0,
        "event": {
            "e": "executionReport",
            "E": 20,
            "s": "BTCUSDT",
            "c": "order-b-1",
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "0.001",
            "p": "60000",
            "g": -1,
            "C": "",
            "x": "TRADE",
            "X": "FILLED",
            "i": 202,
            "l": "0.001",
            "z": "0.001",
            "L": "60000",
            "n": "0",
            "N": "USDT",
            "T": 19,
            "t": 7001,
            "I": 8001,
            "m": true,
            "Z": "60",
            "Y": "60",
            "r": "NONE"
        }
    }))
    .unwrap();
    let ws_thread = thread::spawn(move || {
        let mut s = network::accept(&ws_listener, ws_server_tls);
        network::upgrade(&mut s, "/ws-api/v3");
        let req = network::json(&mut s);
        network::verify_signature(&req, 0);
        network::reply(&mut s, &req["id"], serde_json::json!({"subscriptionId": 0}));
        network::frame(&mut s, 1, &ws_fill);
        let _ = s.read(&mut [0]);
    });
    let ws_key = private::SigningKey::hmac(network::SECRET).unwrap();
    let mut ws_client = private::PrivateClient::new(
        private::SessionConfig::new(network::scope(Market::Spot)),
        private::Authentication::SpotSignature {
            api_key: network::KEY,
            signing_key: &ws_key,
        },
        private::NetworkConfig {
            stream_address: ws_addr,
            management_address: None,
            tls: ws_tls,
        },
        5,
    )
    .unwrap();
    let mut ws_delivered = false;
    let ws_deadline = gateway::until();
    while !ws_delivered {
        assert!(Instant::now() < ws_deadline);
        ws_client
            .poll(
                &mut clock,
                private::PollBudget::default(),
                &mut |_: &private::Charge, _: &private::ClockSample| true,
                |d| {
                    if let private::Delivery::Event(ev) = d {
                        let out = owner.ingest_private(&ev).unwrap();
                        assert!(matches!(
                            out,
                            Outcome::Applied {
                                applied: Applied::New,
                                ..
                            }
                        ));
                        ws_delivered = true;
                    }
                    Ok(())
                },
            )
            .unwrap();
        thread::yield_now();
    }
    ws_client.stop(private::StopMode::Disconnect).unwrap();
    drop(ws_client);
    ws_thread.join().unwrap();

    assert_eq!(
        owner.account().order(&order_2_key).unwrap().filled_quantity,
        Fixed::parse("0.001", 8).unwrap()
    );
    assert!(!owner.account().frozen());
    assert_eq!(
        owner.account().order_in_doubt(&order_1_key),
        Some(DoubtReason::PlaceAmbiguous)
    );
    assert_eq!(
        owner
            .account()
            .asset(gateway::fixture::key(2))
            .unwrap()
            .reserved,
        gateway::fixture::units(60),
        "Order 2 freed its 60 USDT on fill, while Order 1 still holds its 60 USDT"
    );

    // 3. At +6.5s (inside 10s not_found_grace_ns), -2013 must NOT release Order 1!
    query_server.push_json(
        400,
        serde_json::json!({"code": -2013, "msg": "Order does not exist."}),
    );
    clock.set_ns(6_500_000_000);
    let res_grace = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
    assert!(matches!(
        res_grace.as_slice(),
        [QueryResolution::RetryScheduled { order, .. }] if order == &order_1_key
    ));
    assert!(!owner.account().frozen());
    assert!(!owner.account().order(&order_1_key).unwrap().uncreated);
    assert_eq!(
        owner.account().order_in_doubt(&order_1_key),
        Some(DoubtReason::PlaceAmbiguous)
    );
    assert_eq!(
        owner
            .account()
            .asset(gateway::fixture::key(2))
            .unwrap()
            .reserved,
        gateway::fixture::units(60)
    );

    // 4. Past not_found_grace_ns (+11s), -2013 closes Order 1 as uncreated via VenueEventBody::Rejected!
    query_server.push_json(
        400,
        serde_json::json!({"code": -2013, "msg": "Order does not exist."}),
    );
    clock.set_ns(11_000_000_000);
    let res_close = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
    assert!(matches!(
        res_close.as_slice(),
        [QueryResolution::CloseUncreated { order, .. }] if order == &order_1_key
    ));
    assert!(
        !owner.account().frozen(),
        "closing uncreated order must NOT freeze account"
    );
    let view_1 = owner.account().order(&order_1_key).unwrap();
    assert!(view_1.uncreated && view_1.fee_reconciled && !view_1.confirmed);
    assert_eq!(owner.account().order_in_doubt(&order_1_key), None);
    assert_eq!(
        owner
            .account()
            .asset(gateway::fixture::key(2))
            .unwrap()
            .reserved,
        gateway::fixture::f(0),
        "Order 1 reservation must be released after CloseUncreated"
    );
    assert_eq!(query_server.recorded_requests().len(), 2);
    assert!(query_server.recorded_requests()[0].contains("origClientOrderId=order-a-1"));

    let expected_seq = owner.account().ledger_sequence();
    let expected_journal_pos = owner.gateway().journal_position();
    let expected_usdt = owner.account().asset(gateway::fixture::key(2)).unwrap();
    driver.stop();
    drop(driver);
    drop(owner);

    // 5. Reopen both durable WALs and verify byte-exact deterministic recovery.
    let mut rec_gw_cfg = gw_cfg;
    rec_gw_cfg.writer_epoch = 5;
    let mut rec_gw = AccountGateway::recover(
        FileBackend::open(&account_dir.0).unwrap(),
        rec_gw_cfg,
        auth,
        gateway::TestEncoder,
        None,
    )
    .unwrap()
    .with_per_order_doubt_isolation(true);
    let mut rec_ingress =
        AccountIngress::recover(FileStore::open(&inbox_dir.0).unwrap(), ingress_cfg, None).unwrap();
    rec_ingress.replay_pending(&mut rec_gw, 32).unwrap();
    assert!(rec_gw.account().frozen());
    assert_eq!(rec_gw.account().ledger_sequence(), expected_seq);
    assert_eq!(rec_gw.journal_position(), expected_journal_pos);
    assert_eq!(
        rec_gw.account().asset(gateway::fixture::key(2)).unwrap(),
        expected_usdt
    );
    assert!(rec_gw.account().order(&order_1_key).unwrap().uncreated);
    assert_eq!(
        rec_gw
            .account()
            .order(&order_2_key)
            .unwrap()
            .filled_quantity,
        Fixed::parse("0.001", 8).unwrap()
    );
}

#[test]
fn socket_drop_after_send_isolates_order_installs_fresh_transport_and_resolves_via_query_desk() {
    let (client_tls, server_tls) = spot_certs();
    let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), false);
    let query_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), true);

    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_spot_baseline(None);
    let gw_cfg = gateway::config(&baseline);
    let auth = gateway::MockAuthority::default();
    {
        let mut p = auth.policy.lock().unwrap();
        p.gate.now = 1_000_000;
        p.gate.lease_until = u64::MAX / 2;
    }

    let gw = AccountGateway::create(
        FileBackend::open(&account_dir.0).unwrap(),
        gw_cfg,
        baseline.clone(),
        auth.clone(),
        gateway::TestEncoder,
        ConnectionConfig {
            address: order_server.address,
            tls: Arc::clone(&client_tls),
            limits: Limits::default(),
            connect_deadline: gateway::until(),
        },
    )
    .unwrap();
    let ingress = AccountIngress::create(
        FileStore::open(&inbox_dir.0).unwrap(),
        ingress_config(&baseline),
    )
    .unwrap();
    let mut owner = RestOwner::from_parts(
        ingress,
        gw,
        RestConfig {
            connection_epoch: 10,
            max_response_bytes: 32_768,
            max_headers: 32,
        },
    )
    .unwrap()
    .with_per_order_query_desk(QueryConfig {
        status_query_delay_ns: 2_000_000_000,
        place_retry_ns: 1_000_000_000,
        ..QueryConfig::default()
    });

    let mut clock = ControllableClock::new(1_000_000);
    let signing_key = tickwire_binance::auth::SigningKey::hmac(network::SECRET).unwrap();
    let creds = Credentials {
        market: Market::Spot,
        environment: v::Environment::Test,
        account: 2,
        api_key: network::KEY,
        signing_key: &signing_key,
    };
    let mut driver = PerOrderQueryDriver::with_querier_clock(
        PerOrderQueryConfig {
            scope: network::scope(Market::Spot),
            address: query_server.address,
            local_ip: None,
            tls: Arc::clone(&client_tls),
            limits: Limits::default(),
            first_connection_epoch: 200,
            request_timeout_ns: 2_000_000_000,
            connect_timeout_ms: 2_000,
            retry_interval_ms: 10,
            recv_window_micros: 5_000_000,
            max_response_bytes: 32_768,
            trade_page_limit: 16,
            queue_capacity: 64,
        },
        clock.querier_clock(),
    )
    .unwrap();

    // Order 1 sends over TLS, and server drops TCP socket before replying.
    order_server.push_drop();
    let now_1 = clock.now_ns();
    auth.policy.lock().unwrap().gate.now = now_1;
    let (cmd_1, order_1_key) = make_strategy_new_cmd(
        &base_cmd,
        8,
        4,
        1,
        1,
        "drop-order-1",
        v::TimeInForce::Gtc,
        now_1,
    );
    owner
        .submit(cmd_1, &[[31; 16]], now_1, gateway::until())
        .unwrap();
    let out_1 = drive_rest_outcome(&mut owner, &clock);
    assert_eq!(out_1.disposition, RestDisposition::Unknown);
    assert!(!owner.account().frozen());
    assert!(!owner.gateway().halted());
    assert!(!owner.gateway().has_transport_connection());
    assert_eq!(
        owner.account().order_in_doubt(&order_1_key),
        Some(DoubtReason::PlaceAmbiguous)
    );

    // Install a fresh transport connection and submit sibling Strategy B Order 2 while Order 1 is in doubt.
    let fresh_conn = Connection::connect(
        order_server.address,
        Security::Tls {
            server_name: "testnet.binance.vision".try_into().unwrap(),
            config: Arc::clone(&client_tls),
        },
        Limits::default(),
        gateway::until(),
    )
    .unwrap();
    owner
        .install_transport_connection(fresh_conn, Limits::default())
        .unwrap();
    assert!(owner.gateway().has_transport_connection());
    assert_eq!(owner.execution_connection_epoch(), 11);

    order_server.push_json(
        200,
        serde_json::json!({
            "symbol": "BTCUSDT",
            "orderId": 302,
            "orderListId": -1,
            "clientOrderId": "sibling-after-reconnect",
            "transactTime": 1_770_000_000_005u64,
            "price": "60000",
            "origQty": "0.001",
            "executedQty": "0",
            "cummulativeQuoteQty": "0",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "workingTime": 1_770_000_000_005u64
        }),
    );
    let now_2 = clock.add_ns(50_000);
    auth.policy.lock().unwrap().gate.now = now_2;
    let (cmd_2, order_2_key) = make_strategy_new_cmd(
        &base_cmd,
        9,
        5,
        2,
        1,
        "sibling-after-reconnect",
        v::TimeInForce::Gtc,
        now_2,
    );
    owner
        .submit(cmd_2, &[[32; 16]], now_2, gateway::until())
        .unwrap();
    let out_2 = drive_rest_outcome(&mut owner, &clock);
    assert_eq!(out_2.disposition, RestDisposition::Observed);
    assert!(owner.account().order(&order_2_key).unwrap().confirmed);
    assert!(!owner.account().frozen());

    // QueryDesk resolves Order 1 as CANCELED (0 fills) and closes its fees without freezing.
    query_server.push_json(
        200,
        serde_json::json!({
            "symbol": "BTCUSDT",
            "orderId": 301,
            "orderListId": -1,
            "clientOrderId": "drop-order-1",
            "price": "60000",
            "origQty": "0.001",
            "executedQty": "0",
            "cummulativeQuoteQty": "0",
            "status": "CANCELED",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "workingTime": 1_770_000_000_002u64
        }),
    );
    clock.set_ns(3_000_000_000);
    let res = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
    assert!(matches!(
        res.as_slice(),
        [QueryResolution::Resolved { order }] if order == &order_1_key
    ));
    let view_1 = owner.account().order(&order_1_key).unwrap();
    assert!(view_1.terminal && view_1.fee_reconciled);
    assert_eq!(owner.account().order_in_doubt(&order_1_key), None);
    assert_eq!(
        owner
            .account()
            .asset(gateway::fixture::key(2))
            .unwrap()
            .reserved,
        gateway::fixture::units(60),
        "only live Order 2 retains its 60 USDT reservation"
    );
    assert!(!owner.account().frozen());
}

#[test]
fn ambiguous_place_actually_filled_fetches_trades_before_fee_closure_and_recovers_deterministically()
 {
    let (client_tls, server_tls) = spot_certs();
    let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), false);
    let query_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), true);

    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    // 10 bps fee rate reserves 0.06 USDT per fill; actual fill fee is 0.05 USDT.
    let (baseline, base_cmd) = build_spot_baseline(Some("0.001"));
    let gw_cfg = gateway::config(&baseline);
    let auth = gateway::MockAuthority::default();
    {
        let mut p = auth.policy.lock().unwrap();
        p.gate.now = 1_000_000;
        p.gate.lease_until = u64::MAX / 2;
    }

    let gw = AccountGateway::create(
        FileBackend::open(&account_dir.0).unwrap(),
        gateway::config(&baseline),
        baseline.clone(),
        auth.clone(),
        gateway::TestEncoder,
        ConnectionConfig {
            address: order_server.address,
            tls: Arc::clone(&client_tls),
            limits: Limits::default(),
            connect_deadline: gateway::until(),
        },
    )
    .unwrap();
    let ingress_cfg = ingress_config(&baseline);
    let ingress =
        AccountIngress::create(FileStore::open(&inbox_dir.0).unwrap(), ingress_cfg.clone())
            .unwrap();
    let mut owner = RestOwner::from_parts(
        ingress,
        gw,
        RestConfig {
            connection_epoch: 10,
            max_response_bytes: 32_768,
            max_headers: 32,
        },
    )
    .unwrap()
    .with_per_order_query_desk(QueryConfig {
        status_query_delay_ns: 2_000_000_000,
        place_retry_ns: 1_000_000_000,
        ..QueryConfig::default()
    });

    let mut clock = ControllableClock::new(1_000_000);
    let signing_key = tickwire_binance::auth::SigningKey::hmac(network::SECRET).unwrap();
    let creds = Credentials {
        market: Market::Spot,
        environment: v::Environment::Test,
        account: 2,
        api_key: network::KEY,
        signing_key: &signing_key,
    };
    let mut driver = PerOrderQueryDriver::with_querier_clock(
        PerOrderQueryConfig {
            scope: network::scope(Market::Spot),
            address: query_server.address,
            local_ip: None,
            tls: Arc::clone(&client_tls),
            limits: Limits::default(),
            first_connection_epoch: 300,
            request_timeout_ns: 2_000_000_000,
            connect_timeout_ms: 2_000,
            retry_interval_ms: 10,
            recv_window_micros: 5_000_000,
            max_response_bytes: 32_768,
            trade_page_limit: 16,
            queue_capacity: 64,
        },
        clock.querier_clock(),
    )
    .unwrap();

    // 1. Order 1 receives -1007, though venue actually filled it.
    order_server.push_json(
        503,
        serde_json::json!({"code": -1007, "msg": "Send status unknown; execution status unknown."}),
    );
    let now_1 = clock.now_ns();
    auth.policy.lock().unwrap().gate.now = now_1;
    let (cmd_1, order_1_key) = make_strategy_new_cmd(
        &base_cmd,
        8,
        4,
        1,
        1,
        "order-fill-1",
        v::TimeInForce::Gtc,
        now_1,
    );
    owner
        .submit(cmd_1, &[[41; 16]], now_1, gateway::until())
        .unwrap();
    let out_1 = drive_rest_outcome(&mut owner, &clock);
    assert_eq!(out_1.disposition, RestDisposition::Unknown);
    assert!(!owner.account().frozen());

    // 2. OrderStatus query returns FILLED (executedQty = 0.001 > local_filled = 0).
    // Must transition to FetchTradesBeforeClose (AskKind::Trades) BEFORE fee closure!
    query_server.push_json(
        200,
        serde_json::json!({
            "symbol": "BTCUSDT",
            "orderId": 501,
            "orderListId": -1,
            "clientOrderId": "order-fill-1",
            "price": "60000",
            "origQty": "0.001",
            "executedQty": "0.001",
            "cummulativeQuoteQty": "59.5",
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "workingTime": 1_770_000_000_010u64
        }),
    );
    clock.set_ns(3_000_000_000);
    let res_status = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
    assert!(matches!(
        res_status.as_slice(),
        [QueryResolution::FetchTradesBeforeClose { order, .. }] if order == &order_1_key
    ));
    assert!(!owner.account().frozen());
    let mid_view = owner.account().order(&order_1_key).unwrap();
    assert!(mid_view.terminal && !mid_view.fee_reconciled);
    assert_eq!(mid_view.filled_quantity, gateway::fixture::f(0));
    assert_eq!(
        owner.account().order_in_doubt(&order_1_key),
        Some(DoubtReason::FillCatchup)
    );
    assert_eq!(
        owner.query_desk().unwrap().ask_kind(&order_1_key),
        Some(AskKind::Trades)
    );

    // 3. OrderTrades query fetches myTrades for orderId=501, applies fill + commission, and closes fees!
    query_server.push_json(
        200,
        serde_json::json!([
            {
                "symbol": "BTCUSDT",
                "id": 9001,
                "orderId": 501,
                "orderListId": -1,
                "price": "59500",
                "qty": "0.001",
                "quoteQty": "59.5",
                "commission": "0.0000005",
                "commissionAsset": "BTC",
                "time": 1_770_000_000_010u64,
                "isBuyer": true,
                "isMaker": false,
                "isBestMatch": true
            }
        ]),
    );
    let res_trades = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
    assert_eq!(
        res_trades,
        vec![QueryResolution::Resolved {
            order: order_1_key.clone()
        }],
        "frozen={} view={:?} rejected={:?}",
        owner.account().frozen(),
        owner.account().order(&order_1_key),
        owner.account().rejected_event()
    );
    assert!(!owner.account().frozen());
    let final_view = owner.account().order(&order_1_key).unwrap();
    assert_eq!(
        final_view.filled_quantity,
        Fixed::parse("0.001", 8).unwrap()
    );
    assert!(final_view.terminal && final_view.fee_reconciled);
    assert_eq!(owner.account().order_in_doubt(&order_1_key), None);

    let usdt = owner.account().asset(gateway::fixture::key(2)).unwrap();
    assert_eq!(usdt.reserved, gateway::fixture::f(0));
    assert_eq!(usdt.total, Fixed::parse("940.5", 8).unwrap());
    assert_eq!(usdt.available, Fixed::parse("940.5", 8).unwrap());
    let btc = owner.account().asset(gateway::fixture::key(1)).unwrap();
    assert_eq!(btc.reserved, gateway::fixture::f(0));
    assert_eq!(btc.total, Fixed::parse("1.0009995", 8).unwrap());
    assert!(query_server.recorded_requests()[1].starts_with("GET /api/v3/myTrades?"));
    assert!(query_server.recorded_requests()[1].contains("orderId=501"));

    let expected_seq = owner.account().ledger_sequence();
    let expected_pos = owner.gateway().journal_position();
    driver.stop();
    drop(driver);
    drop(owner);

    // 4. Reopen both WALs and verify deterministic recovery.
    let mut rec_gw_cfg = gw_cfg;
    rec_gw_cfg.writer_epoch = 5;
    let mut rec_gw = AccountGateway::recover(
        FileBackend::open(&account_dir.0).unwrap(),
        rec_gw_cfg,
        auth,
        gateway::TestEncoder,
        None,
    )
    .unwrap()
    .with_per_order_doubt_isolation(true);
    let mut rec_ingress =
        AccountIngress::recover(FileStore::open(&inbox_dir.0).unwrap(), ingress_cfg, None).unwrap();
    rec_ingress.replay_pending(&mut rec_gw, 32).unwrap();
    assert!(rec_gw.account().frozen());
    assert_eq!(rec_gw.account().ledger_sequence(), expected_seq);
    assert_eq!(rec_gw.journal_position(), expected_pos);
    assert_eq!(
        rec_gw.account().asset(gateway::fixture::key(2)).unwrap(),
        usdt
    );
    assert!(rec_gw.account().order(&order_1_key).unwrap().fee_reconciled);
}

#[test]
fn private_ws_confirmation_before_status_delay_cancels_query_with_zero_rest_calls() {
    let (client_tls, server_tls) = spot_certs();
    let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), false);
    let query_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), true);

    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_spot_baseline(None);
    let gw_cfg = gateway::config(&baseline);
    let auth = gateway::MockAuthority::default();
    {
        let mut p = auth.policy.lock().unwrap();
        p.gate.now = 1_000_000;
        p.gate.lease_until = u64::MAX / 2;
    }

    let gw = AccountGateway::create(
        FileBackend::open(&account_dir.0).unwrap(),
        gw_cfg,
        baseline.clone(),
        auth.clone(),
        gateway::TestEncoder,
        ConnectionConfig {
            address: order_server.address,
            tls: Arc::clone(&client_tls),
            limits: Limits::default(),
            connect_deadline: gateway::until(),
        },
    )
    .unwrap();
    let ingress = AccountIngress::create(
        FileStore::open(&inbox_dir.0).unwrap(),
        ingress_config(&baseline),
    )
    .unwrap();
    let mut owner = RestOwner::from_parts(
        ingress,
        gw,
        RestConfig {
            connection_epoch: 10,
            max_response_bytes: 32_768,
            max_headers: 32,
        },
    )
    .unwrap()
    .with_per_order_query_desk(QueryConfig {
        status_query_delay_ns: 6_000_000_000,
        ..QueryConfig::default()
    });

    let mut clock = ControllableClock::new(1_000_000);
    let signing_key = tickwire_binance::auth::SigningKey::hmac(network::SECRET).unwrap();
    let creds = Credentials {
        market: Market::Spot,
        environment: v::Environment::Test,
        account: 2,
        api_key: network::KEY,
        signing_key: &signing_key,
    };
    let mut driver = PerOrderQueryDriver::with_querier_clock(
        PerOrderQueryConfig {
            scope: network::scope(Market::Spot),
            address: query_server.address,
            local_ip: None,
            tls: Arc::clone(&client_tls),
            limits: Limits::default(),
            first_connection_epoch: 400,
            request_timeout_ns: 2_000_000_000,
            connect_timeout_ms: 2_000,
            retry_interval_ms: 10,
            recv_window_micros: 5_000_000,
            max_response_bytes: 32_768,
            trade_page_limit: 16,
            queue_capacity: 64,
        },
        clock.querier_clock(),
    )
    .unwrap();

    order_server.push_json(
        503,
        serde_json::json!({"code": -1007, "msg": "Send status unknown."}),
    );
    let now_1 = clock.now_ns();
    auth.policy.lock().unwrap().gate.now = now_1;
    let (cmd_1, order_1_key) = make_strategy_new_cmd(
        &base_cmd,
        8,
        4,
        1,
        1,
        "ws-early-1",
        v::TimeInForce::Gtc,
        now_1,
    );
    owner
        .submit(cmd_1, &[[51; 16]], now_1, gateway::until())
        .unwrap();
    let out_1 = drive_rest_outcome(&mut owner, &clock);
    assert_eq!(out_1.disposition, RestDisposition::Unknown);
    assert_eq!(owner.query_desk().unwrap().waiting_count(), 1);

    // Private WS confirms NEW at +1s (well before the 6s status_query_delay_ns).
    let (ws_tls, ws_server_tls) = spot_certs();
    let ws_listener = network::listener();
    let ws_addr = ws_listener.local_addr().unwrap();
    let ws_new = serde_json::to_vec(&serde_json::json!({
        "subscriptionId": 0,
        "event": {
            "e": "executionReport",
            "E": 25,
            "s": "BTCUSDT",
            "c": "ws-early-1",
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "0.001",
            "p": "60000",
            "g": -1,
            "C": "",
            "x": "NEW",
            "X": "NEW",
            "i": 601,
            "l": "0",
            "z": "0",
            "L": "0",
            "n": "0",
            "N": null,
            "T": 24,
            "t": -1,
            "I": 601,
            "m": false,
            "Z": "0",
            "Y": "0",
            "r": "NONE"
        }
    }))
    .unwrap();
    let ws_thread = thread::spawn(move || {
        let mut s = network::accept(&ws_listener, ws_server_tls);
        network::upgrade(&mut s, "/ws-api/v3");
        let req = network::json(&mut s);
        network::verify_signature(&req, 0);
        network::reply(&mut s, &req["id"], serde_json::json!({"subscriptionId": 0}));
        network::frame(&mut s, 1, &ws_new);
        let _ = s.read(&mut [0]);
    });
    let ws_key = private::SigningKey::hmac(network::SECRET).unwrap();
    let mut ws_client = private::PrivateClient::new(
        private::SessionConfig::new(network::scope(Market::Spot)),
        private::Authentication::SpotSignature {
            api_key: network::KEY,
            signing_key: &ws_key,
        },
        private::NetworkConfig {
            stream_address: ws_addr,
            management_address: None,
            tls: ws_tls,
        },
        5,
    )
    .unwrap();
    clock.set_ns(1_000_000_000);
    let mut ws_delivered = false;
    let ws_deadline = gateway::until();
    while !ws_delivered {
        assert!(Instant::now() < ws_deadline);
        ws_client
            .poll(
                &mut clock,
                private::PollBudget::default(),
                &mut |_: &private::Charge, _: &private::ClockSample| true,
                |d| {
                    if let private::Delivery::Event(ev) = d {
                        owner.ingest_private(&ev).unwrap();
                        ws_delivered = true;
                    }
                    Ok(())
                },
            )
            .unwrap();
        thread::yield_now();
    }
    ws_client.stop(private::StopMode::Disconnect).unwrap();
    drop(ws_client);
    ws_thread.join().unwrap();

    assert!(owner.account().order(&order_1_key).unwrap().confirmed);
    assert_eq!(owner.account().order_in_doubt(&order_1_key), None);

    // Advance past 6s delay: QueryDesk drops the ask with 0 HTTP requests issued!
    clock.set_ns(7_000_000_000);
    let step = driver
        .poll(&mut owner, &creds, &mut clock, &mut AllowAllPermit)
        .unwrap();
    assert_eq!(step.dispatched, 0);
    assert_eq!(driver.issued_requests(), 0);
    assert_eq!(owner.query_desk().unwrap().waiting_count(), 0);
    assert!(query_server.recorded_requests().is_empty());
    assert!(!owner.account().frozen());
}

#[test]
fn deterministic_new_order_rejections_emit_rejected_event_release_hold_immediately_and_recover() {
    let (client_tls, server_tls) = spot_certs();
    let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), false);

    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_spot_baseline(None);
    let gw_cfg = gateway::config(&baseline);
    let auth = gateway::MockAuthority::default();
    {
        let mut p = auth.policy.lock().unwrap();
        p.gate.now = 1_000_000;
        p.gate.lease_until = u64::MAX / 2;
    }

    let gw = AccountGateway::create(
        FileBackend::open(&account_dir.0).unwrap(),
        gateway::config(&baseline),
        baseline.clone(),
        auth.clone(),
        gateway::TestEncoder,
        ConnectionConfig {
            address: order_server.address,
            tls: Arc::clone(&client_tls),
            limits: Limits::default(),
            connect_deadline: gateway::until(),
        },
    )
    .unwrap();
    let ingress_cfg = ingress_config(&baseline);
    let ingress =
        AccountIngress::create(FileStore::open(&inbox_dir.0).unwrap(), ingress_cfg.clone())
            .unwrap();
    let mut owner = RestOwner::from_parts(
        ingress,
        gw,
        RestConfig {
            connection_epoch: 10,
            max_response_bytes: 32_768,
            max_headers: 32,
        },
    )
    .unwrap()
    .with_per_order_query_desk(QueryConfig::default());

    let clock = ControllableClock::new(1_000_000);

    // 1. -2010 deterministic insufficient balance / new order rejected.
    order_server.push_json(
        400,
        serde_json::json!({
            "code": -2010,
            "msg": "Account has insufficient balance for requested action."
        }),
    );
    let now_1 = clock.now_ns();
    auth.policy.lock().unwrap().gate.now = now_1;
    let (cmd_1, order_1_key) = make_strategy_new_cmd(
        &base_cmd,
        8,
        4,
        1,
        1,
        "rej-2010",
        v::TimeInForce::Gtc,
        now_1,
    );
    owner
        .submit(cmd_1, &[[61; 16]], now_1, gateway::until())
        .unwrap();
    let out_1 = drive_rest_outcome(&mut owner, &clock);
    assert_eq!(out_1.disposition, RestDisposition::Rejected);
    assert_eq!(out_1.applied_events, 1);
    assert!(!owner.account().frozen());
    let view_1 = owner.account().order(&order_1_key).unwrap();
    assert!(view_1.uncreated && view_1.fee_reconciled && !view_1.confirmed);
    assert_eq!(owner.account().order_in_doubt(&order_1_key), None);
    assert_eq!(
        owner
            .account()
            .asset(gateway::fixture::key(2))
            .unwrap()
            .reserved,
        gateway::fixture::f(0)
    );
    assert_eq!(owner.query_desk().unwrap().waiting_count(), 0);

    // 2. -5022 Post-Only GTX rejected as taker.
    order_server.push_json(
        400,
        serde_json::json!({
            "code": -5022,
            "msg": "Due to the order could not be executed as maker, the Post Only order will be rejected."
        }),
    );
    let now_2 = clock.add_ns(20_000);
    auth.policy.lock().unwrap().gate.now = now_2;
    let (cmd_2, order_2_key) = make_strategy_new_cmd(
        &base_cmd,
        8,
        4,
        2,
        2,
        "rej-5022",
        v::TimeInForce::PostOnly,
        now_2,
    );
    owner
        .submit(cmd_2, &[[62; 16]], now_2, gateway::until())
        .unwrap();
    let out_2 = drive_rest_outcome(&mut owner, &clock);
    assert_eq!(out_2.disposition, RestDisposition::Rejected);
    assert_eq!(out_2.applied_events, 1);
    assert!(!owner.account().frozen());
    let view_2 = owner.account().order(&order_2_key).unwrap();
    assert!(view_2.uncreated && view_2.fee_reconciled && !view_2.confirmed);
    assert_eq!(owner.account().order_in_doubt(&order_2_key), None);
    assert_eq!(
        owner
            .account()
            .asset(gateway::fixture::key(2))
            .unwrap()
            .reserved,
        gateway::fixture::f(0)
    );
    assert_eq!(owner.query_desk().unwrap().waiting_count(), 0);

    let expected_seq = owner.account().ledger_sequence();
    let expected_pos = owner.gateway().journal_position();
    drop(owner);

    // 3. Reopen both WALs and verify byte-exact validation of VenueEventBody::Rejected.
    let mut rec_gw_cfg = gw_cfg;
    rec_gw_cfg.writer_epoch = 5;
    let mut rec_gw = AccountGateway::recover(
        FileBackend::open(&account_dir.0).unwrap(),
        rec_gw_cfg,
        auth,
        gateway::TestEncoder,
        None,
    )
    .unwrap()
    .with_per_order_doubt_isolation(true);
    let mut rec_ingress =
        AccountIngress::recover(FileStore::open(&inbox_dir.0).unwrap(), ingress_cfg, None).unwrap();
    rec_ingress.replay_pending(&mut rec_gw, 16).unwrap();
    assert!(rec_gw.account().frozen());
    assert_eq!(rec_gw.account().ledger_sequence(), expected_seq);
    assert_eq!(rec_gw.journal_position(), expected_pos);
    assert!(rec_gw.account().order(&order_1_key).unwrap().uncreated);
    assert!(rec_gw.account().order(&order_2_key).unwrap().uncreated);
    assert_eq!(
        rec_gw
            .account()
            .asset(gateway::fixture::key(2))
            .unwrap()
            .reserved,
        gateway::fixture::f(0)
    );
}

#[test]
fn category_b_rate_limit_and_max_query_count_exhaustion_escalate_to_account_freeze() {
    let (client_tls, server_tls) = spot_certs();
    let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), false);
    let query_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), true);

    let signing_key = tickwire_binance::auth::SigningKey::hmac(network::SECRET).unwrap();
    let creds = Credentials {
        market: Market::Spot,
        environment: v::Environment::Test,
        account: 2,
        api_key: network::KEY,
        signing_key: &signing_key,
    };

    // Part A: max_query_count exhaustion on repeated 503 transport/server failures freezes Account.
    {
        let account_dir = gateway::Directory::new();
        let inbox_dir = gateway::Directory::new();
        let (baseline, base_cmd) = build_spot_baseline(None);
        let gw_cfg = gateway::config(&baseline);
        let auth = gateway::MockAuthority::default();
        {
            let mut p = auth.policy.lock().unwrap();
            p.gate.now = 1_000_000;
            p.gate.lease_until = u64::MAX / 2;
        }
        let gw = AccountGateway::create(
            FileBackend::open(&account_dir.0).unwrap(),
            gw_cfg,
            baseline.clone(),
            auth.clone(),
            gateway::TestEncoder,
            ConnectionConfig {
                address: order_server.address,
                tls: Arc::clone(&client_tls),
                limits: Limits::default(),
                connect_deadline: gateway::until(),
            },
        )
        .unwrap();
        let ingress = AccountIngress::create(
            FileStore::open(&inbox_dir.0).unwrap(),
            ingress_config(&baseline),
        )
        .unwrap();
        let mut owner = RestOwner::from_parts(
            ingress,
            gw,
            RestConfig {
                connection_epoch: 10,
                max_response_bytes: 32_768,
                max_headers: 32,
            },
        )
        .unwrap()
        .with_per_order_query_desk(QueryConfig {
            status_query_delay_ns: 1_000_000_000,
            place_retry_ns: 1_000_000_000,
            max_query_count: 2,
            ..QueryConfig::default()
        });

        let mut clock = ControllableClock::new(1_000_000);
        let mut driver = PerOrderQueryDriver::with_querier_clock(
            PerOrderQueryConfig {
                scope: network::scope(Market::Spot),
                address: query_server.address,
                local_ip: None,
                tls: Arc::clone(&client_tls),
                limits: Limits::default(),
                first_connection_epoch: 500,
                request_timeout_ns: 2_000_000_000,
                connect_timeout_ms: 2_000,
                retry_interval_ms: 10,
                recv_window_micros: 5_000_000,
                max_response_bytes: 32_768,
                trade_page_limit: 16,
                queue_capacity: 64,
            },
            clock.querier_clock(),
        )
        .unwrap();

        order_server.push_json(
            503,
            serde_json::json!({"code": -1007, "msg": "Send status unknown."}),
        );
        let now_1 = clock.now_ns();
        auth.policy.lock().unwrap().gate.now = now_1;
        let (cmd_1, order_1_key) = make_strategy_new_cmd(
            &base_cmd,
            8,
            4,
            1,
            1,
            "exhaust-1",
            v::TimeInForce::Gtc,
            now_1,
        );
        owner
            .submit(cmd_1, &[[71; 16]], now_1, gateway::until())
            .unwrap();
        drive_rest_outcome(&mut owner, &clock);
        assert!(!owner.account().frozen());

        // First issued query fails with 503 -> RetryScheduled (issued=1 < 2).
        query_server.push_json(
            503,
            serde_json::json!({"code": -1001, "msg": "Internal error."}),
        );
        clock.set_ns(1_500_000_000);
        let res_1 = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
        assert!(matches!(
            res_1.as_slice(),
            [QueryResolution::RetryScheduled { .. }]
        ));
        assert!(!owner.account().frozen());
        assert_eq!(
            owner.query_desk().unwrap().issued_query_count(&order_1_key),
            Some(1)
        );

        // Second issued query fails with 503 -> FreezeAccount (issued=2 == max_query_count)!
        query_server.push_json(
            503,
            serde_json::json!({"code": -1001, "msg": "Internal error."}),
        );
        clock.set_ns(3_000_000_000);
        let res_2 = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
        assert!(matches!(
            res_2.as_slice(),
            [QueryResolution::FreezeAccount {
                issued_queries: 2,
                ..
            }]
        ));
        assert!(owner.account().frozen());
    }

    // Part B1: Direct Category B HTTP 429 on RestOwner order placement freezes Account immediately.
    {
        let account_dir = gateway::Directory::new();
        let inbox_dir = gateway::Directory::new();
        let (baseline, base_cmd) = build_spot_baseline(None);
        let gw_cfg = gateway::config(&baseline);
        let auth = gateway::MockAuthority::default();
        {
            let mut p = auth.policy.lock().unwrap();
            p.gate.now = 1_000_000;
            p.gate.lease_until = u64::MAX / 2;
        }
        let gw = AccountGateway::create(
            FileBackend::open(&account_dir.0).unwrap(),
            gw_cfg,
            baseline.clone(),
            auth.clone(),
            gateway::TestEncoder,
            ConnectionConfig {
                address: order_server.address,
                tls: Arc::clone(&client_tls),
                limits: Limits::default(),
                connect_deadline: gateway::until(),
            },
        )
        .unwrap();
        let ingress = AccountIngress::create(
            FileStore::open(&inbox_dir.0).unwrap(),
            ingress_config(&baseline),
        )
        .unwrap();
        let mut owner = RestOwner::from_parts(
            ingress,
            gw,
            RestConfig {
                connection_epoch: 10,
                max_response_bytes: 32_768,
                max_headers: 32,
            },
        )
        .unwrap()
        .with_per_order_query_desk(QueryConfig::default());

        let clock = ControllableClock::new(1_000_000);
        order_server.push_json(
            429,
            serde_json::json!({"code": -1015, "msg": "Too many new orders."}),
        );
        let now_1 = clock.now_ns();
        auth.policy.lock().unwrap().gate.now = now_1;
        let (cmd_1, _) = make_strategy_new_cmd(
            &base_cmd,
            8,
            4,
            1,
            1,
            "ratelimit-direct",
            v::TimeInForce::Gtc,
            now_1,
        );
        owner
            .submit(cmd_1, &[[72; 16]], now_1, gateway::until())
            .unwrap();
        let out = drive_rest_outcome(&mut owner, &clock);
        assert_eq!(out.disposition, RestDisposition::Unknown);
        assert!(
            owner.account().frozen(),
            "Category B 429 must freeze Account"
        );
    }

    // Part B2: Category B HTTP 429 on PerOrderQueryDriver status query freezes Account immediately.
    {
        let account_dir = gateway::Directory::new();
        let inbox_dir = gateway::Directory::new();
        let (baseline, base_cmd) = build_spot_baseline(None);
        let gw_cfg = gateway::config(&baseline);
        let auth = gateway::MockAuthority::default();
        {
            let mut p = auth.policy.lock().unwrap();
            p.gate.now = 1_000_000;
            p.gate.lease_until = u64::MAX / 2;
        }
        let gw = AccountGateway::create(
            FileBackend::open(&account_dir.0).unwrap(),
            gw_cfg,
            baseline.clone(),
            auth.clone(),
            gateway::TestEncoder,
            ConnectionConfig {
                address: order_server.address,
                tls: Arc::clone(&client_tls),
                limits: Limits::default(),
                connect_deadline: gateway::until(),
            },
        )
        .unwrap();
        let ingress = AccountIngress::create(
            FileStore::open(&inbox_dir.0).unwrap(),
            ingress_config(&baseline),
        )
        .unwrap();
        let mut owner = RestOwner::from_parts(
            ingress,
            gw,
            RestConfig {
                connection_epoch: 10,
                max_response_bytes: 32_768,
                max_headers: 32,
            },
        )
        .unwrap()
        .with_per_order_query_desk(QueryConfig {
            status_query_delay_ns: 1_000_000_000,
            ..QueryConfig::default()
        });

        let mut clock = ControllableClock::new(1_000_000);
        let mut driver = PerOrderQueryDriver::with_querier_clock(
            PerOrderQueryConfig {
                scope: network::scope(Market::Spot),
                address: query_server.address,
                local_ip: None,
                tls: Arc::clone(&client_tls),
                limits: Limits::default(),
                first_connection_epoch: 600,
                request_timeout_ns: 2_000_000_000,
                connect_timeout_ms: 2_000,
                retry_interval_ms: 10,
                recv_window_micros: 5_000_000,
                max_response_bytes: 32_768,
                trade_page_limit: 16,
                queue_capacity: 64,
            },
            clock.querier_clock(),
        )
        .unwrap();

        order_server.push_json(
            503,
            serde_json::json!({"code": -1007, "msg": "Send status unknown."}),
        );
        let now_1 = clock.now_ns();
        auth.policy.lock().unwrap().gate.now = now_1;
        let (cmd_1, _) = make_strategy_new_cmd(
            &base_cmd,
            8,
            4,
            1,
            1,
            "ratelimit-query",
            v::TimeInForce::Gtc,
            now_1,
        );
        owner
            .submit(cmd_1, &[[73; 16]], now_1, gateway::until())
            .unwrap();
        drive_rest_outcome(&mut owner, &clock);
        assert!(!owner.account().frozen());

        query_server.push_json(
            429,
            serde_json::json!({"code": -1003, "msg": "Too much request weight used."}),
        );
        clock.set_ns(2_000_000_000);
        let _ = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
        assert!(
            owner.account().frozen(),
            "Category B 429 during background query must freeze Account"
        );
        let status_check: Option<Status> = owner.status(IntentKey {
            epoch: 4,
            intent_id: 1,
        });
        assert_eq!(status_check, Some(Status::Unknown));
    }
}

#[test]
fn ambiguous_cancel_isolates_order_and_queries_by_native_order_id_without_freezing() {
    let (client_tls, server_tls) = spot_certs();
    let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), false);
    let query_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), true);

    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_spot_baseline(None);
    let auth = gateway::MockAuthority::default();
    {
        let mut p = auth.policy.lock().unwrap();
        p.gate.now = 1_000_000;
        p.gate.lease_until = u64::MAX / 2;
    }

    let gw = AccountGateway::create(
        FileBackend::open(&account_dir.0).unwrap(),
        gateway::config(&baseline),
        baseline.clone(),
        auth.clone(),
        gateway::TestEncoder,
        ConnectionConfig {
            address: order_server.address,
            tls: Arc::clone(&client_tls),
            limits: Limits::default(),
            connect_deadline: gateway::until(),
        },
    )
    .unwrap();
    let ingress = AccountIngress::create(
        FileStore::open(&inbox_dir.0).unwrap(),
        ingress_config(&baseline),
    )
    .unwrap();
    let mut owner = RestOwner::from_parts(
        ingress,
        gw,
        RestConfig {
            connection_epoch: 10,
            max_response_bytes: 32_768,
            max_headers: 32,
        },
    )
    .unwrap()
    .with_per_order_query_desk(QueryConfig {
        status_query_delay_ns: 2_000_000_000,
        ..QueryConfig::default()
    });

    let mut clock = ControllableClock::new(1_000_000);
    let signing_key = tickwire_binance::auth::SigningKey::hmac(network::SECRET).unwrap();
    let creds = Credentials {
        market: Market::Spot,
        environment: v::Environment::Test,
        account: 2,
        api_key: network::KEY,
        signing_key: &signing_key,
    };
    let mut driver = PerOrderQueryDriver::with_querier_clock(
        PerOrderQueryConfig {
            scope: network::scope(Market::Spot),
            address: query_server.address,
            local_ip: None,
            tls: Arc::clone(&client_tls),
            limits: Limits::default(),
            first_connection_epoch: 700,
            request_timeout_ns: 2_000_000_000,
            connect_timeout_ms: 2_000,
            retry_interval_ms: 10,
            recv_window_micros: 5_000_000,
            max_response_bytes: 32_768,
            trade_page_limit: 16,
            queue_capacity: 64,
        },
        clock.querier_clock(),
    )
    .unwrap();

    // 1. Place Order 1 -> 200 OK NEW (orderId=801).
    order_server.push_json(
        200,
        serde_json::json!({
            "symbol": "BTCUSDT",
            "orderId": 801,
            "orderListId": -1,
            "clientOrderId": "cancel-target-1",
            "transactTime": 1_770_000_000_001u64,
            "price": "60000",
            "origQty": "0.001",
            "executedQty": "0",
            "cummulativeQuoteQty": "0",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "workingTime": 1_770_000_000_001u64
        }),
    );
    let now_1 = clock.now_ns();
    auth.policy.lock().unwrap().gate.now = now_1;
    let (cmd_1, order_1_key) = make_strategy_new_cmd(
        &base_cmd,
        8,
        4,
        1,
        1,
        "cancel-target-1",
        v::TimeInForce::Gtc,
        now_1,
    );
    owner
        .submit(cmd_1, &[[81; 16]], now_1, gateway::until())
        .unwrap();
    assert_eq!(
        drive_rest_outcome(&mut owner, &clock).disposition,
        RestDisposition::Observed
    );

    // 2. Submit Cancel for Order 1 -> receives -1007 ambiguous failure.
    order_server.push_json(
        503,
        serde_json::json!({"code": -1007, "msg": "Send status unknown."}),
    );
    let now_2 = clock.add_ns(20_000);
    auth.policy.lock().unwrap().gate.now = now_2;
    let mut cancel_raw = base_cmd.clone();
    cancel_raw.context.strategy.logical_strategy_id = 8;
    cancel_raw.context.strategy.instance_id = [4; 16];
    cancel_raw.context.strategy.strategy_intent_seq = 2;
    cancel_raw.context.strategy.created_mono_ns = now_2;
    cancel_raw.context.request.intent_id = 2;
    cancel_raw.context.request.recovery_object = 2;
    cancel_raw.context.request.deadline = now_2 + 30_000_000_000;
    let mut triggers = cancel_raw.context.triggers.as_slice().to_vec();
    for t in &mut triggers {
        t.sequence = 2;
        t.observed_mono_ns = now_2 - 1;
    }
    cancel_raw.context.triggers = v::BoundedList::new(triggers).unwrap();
    cancel_raw.body = v::CommandBody::Cancel(v::OrderTarget {
        order_id: [81; 16],
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(v::ClientId::new("cancel-target-1").unwrap()),
        venue_order_id: Some(v::VenueOrderId::new("801").unwrap()),
    });
    let cancel_cmd = v::CanonicalCommand::new(cancel_raw).unwrap();
    owner
        .submit(cancel_cmd, &[], now_2, gateway::until())
        .unwrap();
    let cancel_out = drive_rest_outcome(&mut owner, &clock);
    assert_eq!(cancel_out.disposition, RestDisposition::Unknown);
    assert!(!owner.account().frozen());
    assert_eq!(
        owner.account().order_in_doubt(&order_1_key),
        Some(DoubtReason::CancelAmbiguous)
    );
    assert_eq!(
        owner.query_desk().unwrap().ask_kind(&order_1_key),
        Some(AskKind::Cancel)
    );

    // 3. QueryDesk queries by native orderId=801 -> order still NEW -> CancelRejectedStillOpen!
    query_server.push_json(
        200,
        serde_json::json!({
            "symbol": "BTCUSDT",
            "orderId": 801,
            "orderListId": -1,
            "clientOrderId": "cancel-target-1",
            "price": "60000",
            "origQty": "0.001",
            "executedQty": "0",
            "cummulativeQuoteQty": "0",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "workingTime": 1_770_000_000_001u64
        }),
    );
    clock.set_ns(3_000_000_000);
    let res = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
    assert_eq!(
        res,
        vec![QueryResolution::CancelRejectedStillOpen {
            order: order_1_key.clone(),
            remaining_pending_cancels: 0,
        }]
    );
    assert!(!owner.account().frozen());
    assert_eq!(
        owner.account().order_in_doubt(&order_1_key),
        None,
        "CancelRejectedStillOpen with zero remaining pending cancels must clear CancelAmbiguous doubt"
    );
    assert!(query_server.recorded_requests()[0].contains("orderId=801"));
    assert_eq!(
        owner
            .account()
            .asset(gateway::fixture::key(2))
            .unwrap()
            .reserved,
        gateway::fixture::units(60)
    );
}

#[test]
fn stale_order_status_after_terminal_lagging_order_does_not_false_freeze_and_fetches_trades() {
    let (client_tls, server_tls) = spot_certs();
    let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), false);
    let query_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls), true);

    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_spot_baseline(None);
    let auth = gateway::MockAuthority::default();
    {
        let mut p = auth.policy.lock().unwrap();
        p.gate.now = 1_000_000;
        p.gate.lease_until = u64::MAX / 2;
    }

    let gw = AccountGateway::create(
        FileBackend::open(&account_dir.0).unwrap(),
        gateway::config(&baseline),
        baseline.clone(),
        auth.clone(),
        gateway::TestEncoder,
        ConnectionConfig {
            address: order_server.address,
            tls: Arc::clone(&client_tls),
            limits: Limits::default(),
            connect_deadline: gateway::until(),
        },
    )
    .unwrap();
    let ingress = AccountIngress::create(
        FileStore::open(&inbox_dir.0).unwrap(),
        ingress_config(&baseline),
    )
    .unwrap();
    let mut owner = RestOwner::from_parts(
        ingress,
        gw,
        RestConfig {
            connection_epoch: 10,
            max_response_bytes: 32_768,
            max_headers: 32,
        },
    )
    .unwrap()
    .with_per_order_query_desk(QueryConfig {
        status_query_delay_ns: 2_000_000_000,
        place_retry_ns: 100_000_000,
        ..QueryConfig::default()
    });

    let mut clock = ControllableClock::new(1_000_000);
    let signing_key = tickwire_binance::auth::SigningKey::hmac(network::SECRET).unwrap();
    let creds = Credentials {
        market: Market::Spot,
        environment: v::Environment::Test,
        account: 2,
        api_key: network::KEY,
        signing_key: &signing_key,
    };
    let mut driver = PerOrderQueryDriver::with_querier_clock(
        PerOrderQueryConfig {
            scope: network::scope(Market::Spot),
            address: query_server.address,
            local_ip: None,
            tls: Arc::clone(&client_tls),
            limits: Limits::default(),
            first_connection_epoch: 700,
            request_timeout_ns: 2_000_000_000,
            connect_timeout_ms: 2_000,
            retry_interval_ms: 10,
            recv_window_micros: 5_000_000,
            max_response_bytes: 32_768,
            trade_page_limit: 16,
            queue_capacity: 64,
        },
        clock.querier_clock(),
    )
    .unwrap();

    // 1. Order 1 receives -1007 ambiguous failure -> PlaceAmbiguous + AskKind::Place in QueryDesk.
    order_server.push_json(
        503,
        serde_json::json!({"code": -1007, "msg": "Send status unknown."}),
    );
    let now_1 = clock.now_ns();
    auth.policy.lock().unwrap().gate.now = now_1;
    let (cmd_1, order_1_key) = make_strategy_new_cmd(
        &base_cmd,
        8,
        4,
        1,
        1,
        "stale-status-race-1",
        v::TimeInForce::Gtc,
        now_1,
    );
    owner
        .submit(cmd_1.clone(), &[[82; 16]], now_1, gateway::until())
        .unwrap();
    assert_eq!(
        drive_rest_outcome(&mut owner, &clock).disposition,
        RestDisposition::Unknown
    );

    // 2. While OrderStatus is due, private WS already observed CANCELED with cumulative_quantity=0.001 (fills lagging!).
    let mut term_ev = gateway::fixture::terminal(cmd_1.command(), "0.001");
    term_ev.identity.venue = cmd_1.command().context.request.venue;
    term_ev.identity.environment = cmd_1.command().context.request.environment;
    term_ev.identity.account = cmd_1.command().context.request.account;
    term_ev.identity.risk_domain_id = cmd_1.command().context.request.risk_domain_id;
    term_ev.identity.received_mono_ns = now_1 + 100;
    if let v::VenueEventBody::Order(ref mut obs) = term_ev.body {
        obs.order.order_id = [82; 16];
        obs.order.client_id = Some(v::ClientId::new("stale-status-race-1").unwrap());
        obs.order.venue_order_id = Some(v::VenueOrderId::new("802").unwrap());
    }
    owner.with_gateway(|gw| {
        gw.record_fact(tickwire_account::RecoveryRecord::VenueEvent(Box::new(
            v::CanonicalEvent::new(term_ev).unwrap(),
        )))
        .unwrap();
    });
    assert_eq!(
        owner.account().order_in_doubt(&order_1_key),
        Some(DoubtReason::FillCatchup)
    );
    assert!(owner.account().order(&order_1_key).unwrap().terminal);

    // 3. Stale OrderStatus(NEW, executedQty="0") arrives from REST: must NOT false-freeze on FeesReconciled(0)!
    query_server.push_json(
        200,
        serde_json::json!({
            "symbol": "BTCUSDT",
            "orderId": 802,
            "orderListId": -1,
            "clientOrderId": "stale-status-race-1",
            "price": "60000",
            "origQty": "0.001",
            "executedQty": "0",
            "cummulativeQuoteQty": "0",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "workingTime": 1_770_000_000_001u64
        }),
    );
    clock.set_ns(3_000_000_000);
    let res1 = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
    assert!(
        !owner.account().frozen(),
        "stale OrderStatus(cum=0) must never false-freeze terminal FillCatchup order"
    );
    assert_eq!(
        res1,
        vec![QueryResolution::FetchTradesBeforeClose {
            order: order_1_key.clone(),
            instrument_id: cmd_1.command().context.instrument.instrument_id,
        }]
    );

    // 4. Subsequent myTrades delivers the missing 0.001 fill and reconciles fees cleanly!
    query_server.push_json(
        200,
        serde_json::json!([{
            "symbol": "BTCUSDT",
            "id": 9901,
            "orderId": 802,
            "orderListId": -1,
            "price": "60000",
            "qty": "0.001",
            "quoteQty": "60",
            "commission": "0",
            "commissionAsset": "USDT",
            "time": 1_770_000_000_005u64,
            "isBuyer": true,
            "isMaker": true,
            "isBestMatch": true
        }]),
    );
    clock.add_ns(100_000_000);
    let res2 = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
    assert_eq!(
        res2,
        vec![QueryResolution::Resolved {
            order: order_1_key.clone(),
        }]
    );
    assert!(!owner.account().frozen());
    assert!(owner.account().order(&order_1_key).unwrap().fee_reconciled);
    assert_eq!(owner.account().order_in_doubt(&order_1_key), None);
}
