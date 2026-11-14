#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod common;

use common::fixture;
use serde_json::json;
use std::{
    collections::VecDeque,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_account::{AskKind, DoubtReason, IntentKey, OrderKey, QueryConfig, QueryResolution};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::{AccountGateway, ConnectionConfig, Gate, Status};
use tickwire_account_journal::InMemoryBackend;
use tickwire_app::strategy::{Config as StrategyConfig, FileStrategy};
use tickwire_binance_clock::LocalTime;
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_recovery::{
    InputRecord, RecoveryMode, RuntimeBlobStore, SharedBlobStore, encode_inputs,
};
use tickwire_runtime::{
    authority::{Dependency, LocalAuthority, RecoveryBundle, RecoveryQueue},
    node::Submission,
};
use tickwire_spot_execution::{
    Credentials, Reply, RestSession, SpotEncoder, SpotVenue, authority, network::OwnedCredentials,
};
use tickwire_spot_runner::{
    AccountRunner, Completion, PerOrderQueryDriver, QueryStepOutcome,
    reconcile::{CashUpdate, PrivateCut},
};
use tickwire_transport::{
    Connection, IoBudget, Limits, Security,
    dialer::DialSecurity,
    rest_querier::RestQuerierConfig,
    rustls::{self, pki_types::ServerName},
};
use tickwire_venue::{CanonicalCommand, ClientId, CommandBody, Environment, Symbol};

#[derive(Clone)]
enum MockAction {
    Reply { status: u16, body: String },
    DropSocket,
}

struct LoopbackTlsServer {
    address: SocketAddr,
    host: String,
    client_tls: Arc<rustls::ClientConfig>,
    script: Arc<Mutex<VecDeque<(String, MockAction)>>>,
    seen_paths: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl LoopbackTlsServer {
    fn start(venue: SpotVenue) -> Self {
        let host = authority(venue, Environment::Test).to_owned();
        let cert = rcgen::generate_simple_self_signed(vec![host.clone()]).unwrap();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        let server_cfg = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![cert.cert.der().clone()], key.into())
                .unwrap(),
        );
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_tls = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let script: Arc<Mutex<VecDeque<(String, MockAction)>>> =
            Arc::new(Mutex::new(VecDeque::new()));
        let seen_paths: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let script_bg = Arc::clone(&script);
        let seen_bg = Arc::clone(&seen_paths);
        let stop_bg = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let mut conn_threads = Vec::new();
            while !stop_bg.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((sock, _)) => {
                        let server_tls = Arc::clone(&server_cfg);
                        let script_conn = Arc::clone(&script_bg);
                        let seen_conn = Arc::clone(&seen_bg);
                        let stop_conn = Arc::clone(&stop_bg);
                        conn_threads.push(thread::spawn(move || {
                            let _ = sock.set_nonblocking(false);
                            let _ = sock.set_read_timeout(Some(Duration::from_millis(200)));
                            let Ok(tls_conn) = rustls::ServerConnection::new(server_tls) else {
                                return;
                            };
                            let mut io = BufReader::new(rustls::StreamOwned::new(tls_conn, sock));
                            while !stop_conn.load(Ordering::Acquire) {
                                let mut req_line = String::new();
                                match io.read_line(&mut req_line) {
                                    Ok(0) => break,
                                    Ok(_) => {}
                                    Err(e)
                                        if matches!(
                                            e.kind(),
                                            std::io::ErrorKind::WouldBlock
                                                | std::io::ErrorKind::TimedOut
                                        ) =>
                                    {
                                        continue;
                                    }
                                    Err(_) => break,
                                }
                                let parts: Vec<&str> = req_line.split_whitespace().collect();
                                if parts.len() < 2 {
                                    break;
                                }
                                let path = parts[1].to_owned();
                                let mut content_len = 0usize;
                                let mut header_ok = true;
                                loop {
                                    let mut line = String::new();
                                    match io.read_line(&mut line) {
                                        Ok(0) | Err(_) => {
                                            header_ok = false;
                                            break;
                                        }
                                        Ok(_) => {
                                            if line == "\r\n" {
                                                break;
                                            }
                                            if let Some((k, v)) = line.split_once(':')
                                                && k.eq_ignore_ascii_case("content-length")
                                            {
                                                content_len = v.trim().parse().unwrap_or(0);
                                            }
                                        }
                                    }
                                }
                                if !header_ok {
                                    break;
                                }
                                if content_len > 0 {
                                    let mut body = vec![0u8; content_len];
                                    if io.read_exact(&mut body).is_err() {
                                        break;
                                    }
                                }
                                seen_conn.lock().unwrap().push(path.clone());
                                let next_action = {
                                    let mut q = script_conn.lock().unwrap();
                                    let pos = q
                                        .iter()
                                        .position(|(pat, _)| pat.is_empty() || path.contains(pat));
                                    pos.and_then(|idx| q.remove(idx)).map(|(_, act)| act)
                                };
                                match next_action {
                                    Some(MockAction::DropSocket) | None => break,
                                    Some(MockAction::Reply { status, body }) => {
                                        let reason = match status {
                                            200 => "OK",
                                            429 => "Too Many Requests",
                                            503 => "Service Unavailable",
                                            _ => "Error",
                                        };
                                        if write!(
                                            io.get_mut(),
                                            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
                                            body.len(),
                                            body
                                        )
                                        .is_err()
                                            || io.get_mut().flush().is_err()
                                        {
                                            break;
                                        }
                                    }
                                }
                            }
                        }));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
            for h in conn_threads {
                let _ = h.join();
            }
        });

        Self {
            address,
            host,
            client_tls,
            script,
            seen_paths,
            stop,
            handle: Some(handle),
        }
    }

    fn push_reply(&self, path_substr: &str, status: u16, body: impl Into<String>) {
        self.script.lock().unwrap().push_back((
            path_substr.to_owned(),
            MockAction::Reply {
                status,
                body: body.into(),
            },
        ));
    }

    fn push_drop(&self, path_substr: &str) {
        self.script
            .lock()
            .unwrap()
            .push_back((path_substr.to_owned(), MockAction::DropSocket));
    }

    fn connect(&self) -> Connection {
        let server_name = ServerName::try_from(self.host.clone()).unwrap();
        Connection::connect(
            self.address,
            Security::Tls {
                server_name,
                config: Arc::clone(&self.client_tls),
            },
            Limits::default(),
            common::until(),
        )
        .unwrap()
    }
}

impl Drop for LoopbackTlsServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

type GateClosure = Box<
    dyn FnMut(
        &CanonicalCommand,
        &tickwire_account::Admission,
    ) -> std::result::Result<Gate, tickwire_account_gateway::AuthorityError>,
>;

type TestRunner = AccountRunner<
    'static,
    InMemoryBackend,
    LocalAuthority<SharedBlobStore<RuntimeBlobStore>, GateClosure>,
    fn() -> std::result::Result<u64, tickwire_gateway::Error>,
    SharedBlobStore<RuntimeBlobStore>,
>;

struct Harness {
    venue: SpotVenue,
    symbol: Symbol,
    domain: ClockDomain,
    gate_now: Arc<AtomicU64>,
    template: tickwire_venue::Command,
    runner: TestRunner,
    driver: PerOrderQueryDriver,
    strategy: FileStrategy,
}

fn fixed_clock() -> std::result::Result<u64, tickwire_gateway::Error> {
    Ok(1_700_000_000_123)
}

impl Harness {
    fn new(
        venue: SpotVenue,
        exec_server: &LoopbackTlsServer,
        query_server: &LoopbackTlsServer,
        qcfg: QueryConfig,
    ) -> Self {
        let (mut snapshot, mut catalog, mut template) = fixture::fixture();
        snapshot.scope.venue = venue.id();
        snapshot.valid_until = u64::MAX;

        let symbol = Symbol::new(if venue == SpotVenue::Okx {
            "BTC-USDT"
        } else {
            "BTCUSDT"
        })
        .unwrap();
        catalog.entries[0].instrument.symbol = symbol.clone();
        catalog.entries[0].fees.rates[0] = tickwire_account::Fixed::parse("0.01", 8).unwrap();
        let lim8 = catalog.entries[0].strategy_limits[0].1;
        catalog.entries[0].strategy_limits.push((9, lim8));
        template.context.request.venue = venue.id();
        template.context.instrument.symbol = symbol.clone();

        let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();
        let creds: Credentials<'static> = if venue == SpotVenue::Okx {
            Credentials::okx("key", b"secret", "pass")
        } else {
            Credentials::bybit("key", b"secret")
        };
        let fn_ptr: fn() -> std::result::Result<u64, tickwire_gateway::Error> = fixed_clock;
        let encoder = SpotEncoder::new(creds, Environment::Test, 2, fn_ptr).unwrap();
        let mut cfg = common::config(&baseline);
        cfg.route = encoder.route();
        let queue = RecoveryQueue::new(16, 4 * 1024 * 1024).unwrap();
        let objects = SharedBlobStore::new(RuntimeBlobStore::in_memory());
        let gate_now = Arc::new(AtomicU64::new(1_000_000));
        let gate_clock = Arc::clone(&gate_now);
        let gate_box: GateClosure = Box::new(move |_, _| {
            let now = gate_clock.load(Ordering::Acquire);
            Ok(Gate {
                now,
                lease_until: u64::MAX / 2,
                writer_epoch: 4,
                strategy_generation: 5,
                config_revision: 6,
                owner_isolated: true,
                action_permitted: true,
                market_valid: true,
            })
        });
        let authority = LocalAuthority::new(objects.clone(), queue.clone(), gate_box, [2; 16]);
        let gateway = AccountGateway::create(
            InMemoryBackend::new(),
            cfg,
            baseline,
            authority,
            encoder,
            ConnectionConfig {
                address: exec_server.address,
                tls: Arc::clone(&exec_server.client_tls),
                limits: Limits::default(),
                connect_deadline: common::until(),
            },
        )
        .unwrap();
        let runner = AccountRunner::new(RestSession::new(gateway, 1).unwrap(), queue, objects)
            .with_per_order_query_desk(qcfg);

        let owned_creds = if venue == SpotVenue::Okx {
            OwnedCredentials::okx("key", "secret", "pass").unwrap()
        } else {
            OwnedCredentials::bybit("key", "secret").unwrap()
        };
        let server_name = ServerName::try_from(query_server.host.clone()).unwrap();
        let driver = PerOrderQueryDriver::new(
            venue,
            Environment::Test,
            RestQuerierConfig {
                host: query_server.host.clone(),
                port: query_server.address.port(),
                initial_peers: vec![query_server.address],
                security: DialSecurity::Tls {
                    server_name,
                    config: Arc::clone(&query_server.client_tls),
                },
                interval_ms: 5,
                connect_timeout_ms: 3_000,
                timeout_ms: 3_000,
                ..RestQuerierConfig::default()
            },
            owned_creds,
            || Ok(1_700_000_000_123),
        )
        .unwrap();

        Self {
            venue,
            symbol,
            domain: ClockDomain::new([4; 16], 1).unwrap(),
            gate_now,
            template,
            runner,
            driver,
            strategy: FileStrategy::new(&StrategyConfig::Observe).unwrap(),
        }
    }

    fn time(&self, mono_ns: u64) -> LocalTime {
        self.gate_now.store(mono_ns, Ordering::Release);
        LocalTime {
            monotonic: MonoTime::new(self.domain, mono_ns),
            instant: Instant::now(),
        }
    }

    fn submit_new(
        &mut self,
        strategy_id: u64,
        intent_id: u64,
        seq: u64,
        client_id: &str,
        mono_ns: u64,
    ) -> OrderKey {
        let mut raw = self.template.clone();
        raw.context.request.venue = self.venue.id();
        raw.context.request.intent_id = intent_id;
        raw.context.request.recovery_object = intent_id;
        raw.context.request.deadline = mono_ns + 60_000_000_000;
        raw.context.strategy.logical_strategy_id = strategy_id;
        raw.context.strategy.instance_id = [strategy_id as u8; 16];
        raw.context.strategy.strategy_intent_seq = seq;
        raw.context.strategy.created_mono_ns = mono_ns.saturating_sub(10);
        raw.context.instrument.symbol = self.symbol.clone();
        let CommandBody::New(ref mut order) = raw.body else {
            unreachable!()
        };
        order.client_id = ClientId::new(client_id).unwrap();
        let command = CanonicalCommand::new(raw).unwrap();
        let mut order_uuid = [0u8; 16];
        order_uuid[..8].copy_from_slice(&intent_id.to_be_bytes());
        let sub = Submission {
            command: command.clone(),
            order_ids: vec![order_uuid],
            deadline: common::until(),
            recovery: RecoveryBundle {
                command_digest: command.digest(),
                dependencies: (2..=11)
                    .map(|kind| Dependency {
                        kind,
                        schema_id: 1,
                        owner: [9; 16],
                        bytes: vec![kind; 4],
                    })
                    .collect(),
                inputs: encode_inputs(
                    [strategy_id as u8; 16],
                    intent_id,
                    [0; 32],
                    &[InputRecord {
                        kind: 1,
                        payload: b"quote",
                    }],
                )
                .unwrap(),
                mode: RecoveryMode::DeterministicReplay,
                fault_model_id: [4; 32],
                pin_id: [9; 16],
                owner: [9; 16],
            },
        };
        let t = self.time(mono_ns);
        assert_eq!(self.runner.submit(sub, t).unwrap(), Status::Queued);
        OrderKey {
            intent: IntentKey {
                epoch: 4,
                intent_id,
            },
            client_id: ClientId::new(client_id).unwrap(),
        }
    }

    fn drive_execution_completion(&mut self, mono_ns: u64) -> Completion {
        let deadline = common::until();
        let mut step_ns = mono_ns;
        loop {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for execution completion"
            );
            step_ns += 1;
            let t = self.time(step_ns);
            if let Some(done) = self
                .runner
                .poll(&mut self.strategy, t, IoBudget::default())
                .unwrap()
            {
                return done;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn drive_query_resolution(&mut self, mono_ns: u64) -> QueryStepOutcome {
        let cut = PrivateCut {
            stream: [7; 16],
            epoch: 1,
            gap_generation: 1,
            through: 10,
        };
        // First poll dispatches due tickets onto AsyncRestQuerier.
        let mut combined = self
            .driver
            .poll(&mut self.runner, Some(cut), mono_ns)
            .unwrap();
        if !combined.resolutions.is_empty() || combined.account_hazard_frozen {
            return combined;
        }
        let deadline = common::until();
        while Instant::now() < deadline {
            let step = self
                .driver
                .poll(&mut self.runner, Some(cut), mono_ns)
                .unwrap();
            combined.dispatched_tasks += step.dispatched_tasks;
            combined.processed_replies += step.processed_replies;
            combined.reconnected_execution |= step.reconnected_execution;
            combined.account_hazard_frozen |= step.account_hazard_frozen;
            combined.resolutions.extend(step.resolutions);
            if !combined.resolutions.is_empty() || combined.account_hazard_frozen {
                return combined;
            }
            thread::sleep(Duration::from_millis(2));
        }
        panic!("timed out waiting for PerOrderQueryDriver resolution");
    }
}

fn ack_json(venue: SpotVenue, native_id: &str, client_id: &str) -> String {
    if venue == SpotVenue::Bybit {
        json!({
            "retCode": 0,
            "result": { "orderId": native_id, "orderLinkId": client_id }
        })
        .to_string()
    } else {
        json!({
            "code": "0",
            "data": [{ "sCode": "0", "ordId": native_id, "clOrdId": client_id }]
        })
        .to_string()
    }
}

fn ws_order_json(
    venue: SpotVenue,
    symbol: &str,
    native_id: &str,
    client_id: &str,
    status: &str,
    cum_qty: &str,
) -> serde_json::Value {
    if venue == SpotVenue::Bybit {
        json!({
            "category": "spot",
            "isLeverage": "0",
            "symbol": symbol,
            "orderId": native_id,
            "orderLinkId": client_id,
            "side": "Buy",
            "orderType": "Limit",
            "timeInForce": "GTC",
            "price": "60000",
            "qty": "0.001",
            "cumExecQty": cum_qty,
            "orderStatus": status,
            "createdTime": "1700000000100",
            "updatedTime": "1700000000200"
        })
    } else {
        json!({
            "instType": "SPOT",
            "tdMode": "cash",
            "instId": symbol,
            "ordId": native_id,
            "clOrdId": client_id,
            "side": "buy",
            "ordType": "limit",
            "px": "60000",
            "sz": "0.001",
            "accFillSz": cum_qty,
            "state": status,
            "tradeId": "",
            "cTime": "1700000000100",
            "uTime": "1700000000200"
        })
    }
}

fn fill_row_json(
    venue: SpotVenue,
    symbol: &str,
    native_id: &str,
    client_id: &str,
    trade_id: &str,
    qty: &str,
) -> serde_json::Value {
    if venue == SpotVenue::Bybit {
        json!({
            "category": "spot",
            "symbol": symbol,
            "orderLinkId": client_id,
            "orderId": native_id,
            "execType": "Trade",
            "execId": trade_id,
            "execPrice": "60000",
            "execQty": qty,
            "execValue": "60",
            "execTime": "1700000000150",
            "execFee": "0.000001",
            "feeCurrency": "BTC",
            "isMaker": true,
            "side": "Buy",
            "extraFees": ""
        })
    } else {
        json!({
            "instType": "SPOT",
            "instId": symbol,
            "clOrdId": client_id,
            "ordId": native_id,
            "tradeId": trade_id,
            "subType": "1",
            "fillPx": "60000",
            "fillSz": qty,
            "fillTime": "1700000000150",
            "ts": "1700000000150",
            "fee": "-0.000001",
            "feeCcy": "BTC",
            "fillFee": "-0.000001",
            "fillFeeCcy": "BTC",
            "execType": "M",
            "side": "buy"
        })
    }
}

#[test]
fn single_order_socket_drop_and_ambiguity_isolate_order_allow_sibling_trading_and_close_after_grace()
 {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let exec_srv = LoopbackTlsServer::start(venue);
        let query_srv = LoopbackTlsServer::start(venue);
        let qcfg = QueryConfig {
            status_query_delay_ns: 1_000,
            place_retry_ns: 1_000,
            not_found_grace_ns: 6_000_000_000,
            ..QueryConfig::default()
        };
        let mut h = Harness::new(venue, &exec_srv, &query_srv, qcfg);

        // 1. Order 1 (Strategy 8) experiences TCP/TLS socket drop after send!
        exec_srv.push_drop("");
        let t0 = 10_000_000u64;
        let key1 = h.submit_new(8, 1, 1, "strat8ord1", t0);
        let done1 = h.drive_execution_completion(t0 + 10);
        assert_eq!(done1.response, Reply::Unknown);
        assert!(!h.runner.has_transport_connection());
        assert!(!h.runner.halted());
        assert!(!h.runner.account().frozen());
        assert_eq!(
            h.runner.account().order_in_doubt(&key1),
            Some(DoubtReason::PlaceAmbiguous)
        );
        assert_eq!(
            h.runner.query_desk().unwrap().ask_kind(&key1),
            Some(AskKind::Place)
        );
        let locked_quote_order1 = h.runner.account().asset(fixture::key(2)).unwrap().reserved;
        assert!(locked_quote_order1.atoms() > 0);

        // 2. Install replacement execution Connection without halting or restarting!
        h.runner
            .install_transport_connection(exec_srv.connect(), Limits::default())
            .unwrap();
        assert!(h.runner.has_transport_connection());

        // 3. Sibling Strategy 9 places Order 2 and trades cleanly while Order 1 remains isolated!
        exec_srv.push_reply("", 200, ack_json(venue, "native2", "strat9ord2"));
        let key2 = h.submit_new(9, 2, 1, "strat9ord2", t0 + 1_000);
        let done2 = h.drive_execution_completion(t0 + 1_010);
        assert!(matches!(done2.response, Reply::Acknowledged { .. }));
        assert!(!h.runner.account().frozen());

        let symbol_str = h.symbol.as_str().to_owned();
        let cut1 = PrivateCut {
            stream: [7; 16],
            epoch: 1,
            gap_generation: 1,
            through: 1,
        };
        let fill2 = fill_row_json(venue, &symbol_str, "native2", "strat9ord2", "1002", "0.001");
        assert_eq!(
            h.runner
                .apply_private_cash("fills", &fill2, cut1, t0 + 1_100)
                .unwrap(),
            CashUpdate::Accounted
        );
        let cut2 = PrivateCut { through: 2, ..cut1 };
        let term2 = ws_order_json(
            venue,
            &symbol_str,
            "native2",
            "strat9ord2",
            if venue == SpotVenue::Bybit {
                "Filled"
            } else {
                "filled"
            },
            "0.001",
        );
        assert_eq!(
            h.runner
                .apply_private_cash("orders", &term2, cut2, t0 + 1_200)
                .unwrap(),
            CashUpdate::Accounted
        );
        assert!(!h.runner.account().frozen());
        let v2 = h.runner.account().order(&key2).unwrap();
        assert!(v2.terminal && v2.fee_reconciled);

        // 4. Query Order 1 BEFORE `not_found_grace_ns` -> MUST block uncreated release & schedule retry!
        if venue == SpotVenue::Bybit {
            query_srv.push_reply(
                "/v5/order/realtime",
                200,
                r#"{"retCode":0,"result":{"category":"spot","list":[]}}"#,
            );
            query_srv.push_reply(
                "/v5/order/history",
                200,
                r#"{"retCode":0,"result":{"category":"spot","list":[]}}"#,
            );
        } else {
            query_srv.push_reply(
                "/api/v5/trade/order",
                200,
                r#"{"code":"51603","msg":"Order does not exist","data":[]}"#,
            );
        }
        let early_ns = t0 + 1_000_000; // Well inside 6s grace window!
        let step_early = h.drive_query_resolution(early_ns);
        assert!(
            matches!(
                step_early.resolutions.as_slice(),
                [QueryResolution::RetryScheduled { order, .. }] if *order == key1
            ),
            "got {:?}",
            step_early.resolutions
        );
        let v1_mid = h.runner.account().order(&key1).unwrap();
        assert!(!v1_mid.uncreated);
        assert_eq!(
            h.runner.account().order_in_doubt(&key1),
            Some(DoubtReason::PlaceAmbiguous)
        );
        assert_eq!(
            h.runner.account().asset(fixture::key(2)).unwrap().reserved,
            locked_quote_order1
        );

        // 5. Query Order 1 PAST `not_found_grace_ns` -> cleanly closes Order 1 (`uncreated = true`), frees reservation!
        if venue == SpotVenue::Bybit {
            query_srv.push_reply(
                "/v5/order/realtime",
                200,
                r#"{"retCode":0,"result":{"category":"spot","list":[]}}"#,
            );
            query_srv.push_reply(
                "/v5/order/history",
                200,
                r#"{"retCode":0,"result":{"category":"spot","list":[]}}"#,
            );
        } else {
            query_srv.push_reply(
                "/api/v5/trade/order",
                200,
                r#"{"code":"51603","msg":"Order does not exist","data":[]}"#,
            );
        }
        let after_grace_ns = t0 + 7_000_000_000;
        let step_late = h.drive_query_resolution(after_grace_ns);
        assert!(
            matches!(
                step_late.resolutions.as_slice(),
                [QueryResolution::CloseUncreated { order, .. }] if *order == key1
            ),
            "got {:?}",
            step_late.resolutions
        );
        let v1_done = h.runner.account().order(&key1).unwrap();
        assert!(v1_done.uncreated);
        assert_eq!(h.runner.account().order_in_doubt(&key1), None);
        assert!(!h.runner.account().is_intent_active(key1.intent));
        assert_eq!(
            h.runner.account().asset(fixture::key(2)).unwrap().reserved,
            fixture::units(0)
        );
        assert!(!h.runner.account().frozen());
        h.driver.stop();
    }
}

#[test]
fn bybit_empty_realtime_falls_back_to_history_and_applies_fills_before_terminal_closure() {
    let venue = SpotVenue::Bybit;
    let exec_srv = LoopbackTlsServer::start(venue);
    let query_srv = LoopbackTlsServer::start(venue);
    let qcfg = QueryConfig {
        status_query_delay_ns: 1_000,
        place_retry_ns: 1_000,
        ..QueryConfig::default()
    };
    let mut h = Harness::new(venue, &exec_srv, &query_srv, qcfg);

    // Order 1 gets Bybit 170007 timeout -> isolated in PlaceAmbiguous without dropping connection!
    exec_srv.push_reply(
        "",
        200,
        r#"{"retCode":170007,"retMsg":"Timeout waiting for backend"}"#,
    );
    let t0 = 5_000_000u64;
    let key1 = h.submit_new(8, 1, 1, "bybhist1", t0);
    let done1 = h.drive_execution_completion(t0 + 10);
    assert!(matches!(done1.response, Reply::RequestIssue { ref code } if code == "170007"));
    assert!(h.runner.has_transport_connection());
    assert!(!h.runner.account().frozen());

    // Realtime returns empty, History returns Filled (cumExecQty=0.001), Execution list returns the fill!
    query_srv.push_reply(
        "/v5/order/realtime",
        200,
        r#"{"retCode":0,"result":{"category":"spot","list":[]}}"#,
    );
    let hist_row = ws_order_json(
        venue,
        "BTCUSDT",
        "native-hist-1",
        "bybhist1",
        "Filled",
        "0.001",
    );
    query_srv.push_reply(
        "/v5/order/history",
        200,
        json!({
            "retCode": 0,
            "result": { "category": "spot", "list": [hist_row] }
        })
        .to_string(),
    );
    let trade_row = fill_row_json(
        venue,
        "BTCUSDT",
        "native-hist-1",
        "bybhist1",
        "exec-hist-1",
        "0.001",
    );
    query_srv.push_reply(
        "/v5/execution/list",
        200,
        json!({
            "retCode": 0,
            "result": { "category": "spot", "list": [trade_row] }
        })
        .to_string(),
    );

    let step1 = h.drive_query_resolution(t0 + 10_000);
    if !step1
        .resolutions
        .iter()
        .any(|r| matches!(r, QueryResolution::Resolved { .. }))
    {
        let step2 = h.drive_query_resolution(t0 + 20_000);
        assert!(
            step2
                .resolutions
                .iter()
                .any(|r| matches!(r, QueryResolution::Resolved { order } if *order == key1))
        );
    }

    let v1 = h.runner.account().order(&key1).unwrap();
    assert!(v1.terminal);
    assert!(v1.fee_reconciled);
    assert_eq!(h.runner.account().order_in_doubt(&key1), None);
    assert!(!h.runner.account().is_intent_active(key1.intent));
    assert_eq!(
        h.runner.account().asset(fixture::key(2)).unwrap().reserved,
        fixture::units(0)
    );
    assert!(!h.runner.account().frozen());
    let paths = query_srv.seen_paths.lock().unwrap().clone();
    assert!(paths.iter().any(|p| p.starts_with("/v5/order/realtime")));
    assert!(paths.iter().any(|p| p.starts_with("/v5/order/history")));
    assert!(paths.iter().any(|p| p.starts_with("/v5/execution/list")));
    h.driver.stop();
}

#[test]
fn lagging_private_ws_orders_ahead_of_fills_catches_up_via_async_rest_querier_without_freezing() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let exec_srv = LoopbackTlsServer::start(venue);
        let query_srv = LoopbackTlsServer::start(venue);
        let mut h = Harness::new(venue, &exec_srv, &query_srv, QueryConfig::default());

        // 1. Order 1 (Strategy 8) is acknowledged.
        exec_srv.push_reply("", 200, ack_json(venue, "native-lag-1", "lagord1"));
        let t0 = 20_000_000u64;
        let key1 = h.submit_new(8, 1, 1, "lagord1", t0);
        assert!(matches!(
            h.drive_execution_completion(t0 + 10).response,
            Reply::Acknowledged { .. }
        ));

        // 2. Private WS `orders` delivers `Filled` (`cum = 0.001`) BEFORE WS `fills`!
        let symbol_str = h.symbol.as_str().to_owned();
        let cut1 = PrivateCut {
            stream: [8; 16],
            epoch: 1,
            gap_generation: 1,
            through: 1,
        };
        let ahead_order = ws_order_json(
            venue,
            &symbol_str,
            "native-lag-1",
            "lagord1",
            if venue == SpotVenue::Bybit {
                "Filled"
            } else {
                "filled"
            },
            "0.001",
        );
        assert_eq!(
            h.runner
                .apply_private_cash("orders", &ahead_order, cut1, t0 + 100)
                .unwrap(),
            CashUpdate::Accounted
        );
        assert!(!h.runner.account().frozen());
        assert_eq!(
            h.runner.account().order_in_doubt(&key1),
            Some(DoubtReason::FillCatchup)
        );
        assert_eq!(
            h.runner.query_desk().unwrap().ask_kind(&key1),
            Some(AskKind::Trades)
        );

        // 3. Sibling Strategy 9 submits Order 2 unimpeded while Order 1 awaits trade catch-up!
        exec_srv.push_reply("", 200, ack_json(venue, "native-sib-2", "sibord2"));
        let _key2 = h.submit_new(9, 2, 1, "sibord2", t0 + 200);
        assert!(matches!(
            h.drive_execution_completion(t0 + 210).response,
            Reply::Acknowledged { .. }
        ));
        assert!(!h.runner.account().frozen());

        // 4. AsyncRestQuerier fetches missing fill -> books fill, records FeesReconciled, clears FillCatchup!
        let trade_row = fill_row_json(
            venue,
            &symbol_str,
            "native-lag-1",
            "lagord1",
            "2001",
            "0.001",
        );
        if venue == SpotVenue::Bybit {
            query_srv.push_reply(
                "/v5/execution/list",
                200,
                json!({
                    "retCode": 0,
                    "result": { "category": "spot", "list": [trade_row] }
                })
                .to_string(),
            );
        } else {
            query_srv.push_reply(
                "/api/v5/trade/fills",
                200,
                json!({
                    "code": "0",
                    "data": [trade_row]
                })
                .to_string(),
            );
        }
        let step = h.drive_query_resolution(t0 + 300);
        assert!(
            matches!(
                step.resolutions.as_slice(),
                [QueryResolution::Resolved { order }] if *order == key1
            ),
            "got {:?}",
            step.resolutions
        );
        let v1 = h.runner.account().order(&key1).unwrap();
        assert!(v1.terminal && v1.fee_reconciled);
        assert_eq!(h.runner.account().order_in_doubt(&key1), None);
        assert!(!h.runner.account().frozen());

        // 5. Late duplicate WS fill arrives afterwards -> deduplicated by trade ID without double-counting!
        let base_before = h.runner.account().asset(fixture::key(1)).unwrap().total;
        let cut2 = PrivateCut { through: 2, ..cut1 };
        assert_eq!(
            h.runner
                .apply_private_cash("fills", &trade_row, cut2, t0 + 400)
                .unwrap(),
            CashUpdate::Accounted
        );
        assert_eq!(
            h.runner.account().asset(fixture::key(1)).unwrap().total,
            base_before
        );
        assert!(!h.runner.account().frozen());
        h.driver.stop();
    }
}

#[test]
fn deterministic_new_rejection_and_rate_budget_on_not_sent_preserve_liveness_and_query_budget() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let exec_srv = LoopbackTlsServer::start(venue);
        let query_srv = LoopbackTlsServer::start(venue);
        let qcfg = QueryConfig {
            status_query_delay_ns: 100,
            place_retry_ns: 500,
            max_query_count: 2,
            ..QueryConfig::default()
        };
        let mut h = Harness::new(venue, &exec_srv, &query_srv, qcfg);

        // 1. Deterministic business reject on New (Bybit 170193 / OKX 51008):
        if venue == SpotVenue::Bybit {
            exec_srv.push_reply(
                "",
                200,
                r#"{"retCode":170193,"retMsg":"Post only would take liquidity"}"#,
            );
        } else {
            exec_srv.push_reply(
                "",
                200,
                r#"{"code":"1","data":[{"sCode":"51008","sMsg":"Insufficient balance","ordId":"","clOrdId":"detrej1"}]}"#,
            );
        }
        let t0 = 30_000_000u64;
        let key1 = h.submit_new(8, 1, 1, "detrej1", t0);
        let done1 = h.drive_execution_completion(t0 + 10);
        assert!(matches!(done1.response, Reply::RequestIssue { .. }));
        let v1 = h.runner.account().order(&key1).unwrap();
        assert!(v1.uncreated);
        assert!(!h.runner.account().is_intent_active(key1.intent));
        assert_eq!(h.runner.query_desk().unwrap().waiting_count(), 0);
        assert_eq!(
            h.runner.account().asset(fixture::key(2)).unwrap().reserved,
            fixture::units(0)
        );
        assert!(!h.runner.account().frozen());

        // 2. Ambiguous response on Order 2 + rate budget declining first dispatch (`on_not_sent`):
        if venue == SpotVenue::Bybit {
            exec_srv.push_reply("", 200, r#"{"retCode":170007,"retMsg":"Timeout"}"#);
        } else {
            exec_srv.push_reply("", 200, r#"{"code":"50004","msg":"Timeout","data":[]}"#);
        }
        let key2 = h.submit_new(9, 2, 1, "ambig2", t0 + 100);
        let _ = h.drive_execution_completion(t0 + 110);
        assert_eq!(
            h.runner.account().order_in_doubt(&key2),
            Some(DoubtReason::PlaceAmbiguous)
        );

        let allow = Arc::new(AtomicBool::new(false));
        let allow_flag = Arc::clone(&allow);
        h.driver
            .set_rate_budget(move |_| allow_flag.load(Ordering::Acquire));

        let cut = PrivateCut {
            stream: [9; 16],
            epoch: 1,
            gap_generation: 1,
            through: 1,
        };
        let not_sent = h.driver.poll(&mut h.runner, Some(cut), t0 + 1_000).unwrap();
        assert_eq!(not_sent.dispatched_tasks, 0);
        assert!(matches!(
            not_sent.resolutions.as_slice(),
            [QueryResolution::RetryScheduled { order, .. }] if *order == key2
        ));
        // `on_not_sent` MUST NOT increment `issued_query_count`!
        assert_eq!(
            h.runner.query_desk().unwrap().issued_query_count(&key2),
            Some(0)
        );
        assert!(!h.runner.account().frozen());
        h.driver.stop();
    }
}

#[test]
fn category_b_account_hazards_and_max_query_count_exhaustion_escalate_to_account_freeze() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        // Case A: Category B AccountHazard on AsyncRestQuerier (HTTP 429 / 10006 / 50011) freezes Account!
        {
            let exec_srv = LoopbackTlsServer::start(venue);
            let query_srv = LoopbackTlsServer::start(venue);
            let qcfg = QueryConfig {
                status_query_delay_ns: 100,
                place_retry_ns: 100,
                ..QueryConfig::default()
            };
            let mut h = Harness::new(venue, &exec_srv, &query_srv, qcfg);
            exec_srv.push_drop("");
            let t0 = 40_000_000u64;
            let _key1 = h.submit_new(8, 1, 1, "haz1", t0);
            let _ = h.drive_execution_completion(t0 + 10);
            assert!(!h.runner.account().frozen());

            if venue == SpotVenue::Bybit {
                query_srv.push_reply("", 200, r#"{"retCode":10006,"retMsg":"Too many visits"}"#);
            } else {
                query_srv.push_reply("", 429, r#"{"code":"50011","msg":"Too Many Requests"}"#);
            }
            let step = h.drive_query_resolution(t0 + 1_000);
            assert!(step.account_hazard_frozen);
            assert!(h.runner.account().frozen());
            h.driver.stop();
        }

        // Case B: `max_query_count` exhaustion on repeated HTTP 503 responses freezes Account!
        {
            let exec_srv = LoopbackTlsServer::start(venue);
            let query_srv = LoopbackTlsServer::start(venue);
            let qcfg = QueryConfig {
                status_query_delay_ns: 100,
                place_retry_ns: 100,
                max_query_count: 2,
                ..QueryConfig::default()
            };
            let mut h = Harness::new(venue, &exec_srv, &query_srv, qcfg);
            exec_srv.push_drop("");
            let t0 = 50_000_000u64;
            let key1 = h.submit_new(8, 1, 1, "exhaust1", t0);
            let _ = h.drive_execution_completion(t0 + 10);
            assert!(!h.runner.account().frozen());

            query_srv.push_reply("", 503, "service unavailable");
            let step1 = h.drive_query_resolution(t0 + 1_000);
            assert!(matches!(
                step1.resolutions.as_slice(),
                [QueryResolution::RetryScheduled { order, .. }] if *order == key1
            ));
            assert!(!h.runner.account().frozen());

            query_srv.push_reply("", 503, "service unavailable");
            let step2 = h.drive_query_resolution(t0 + 2_000);
            assert!(matches!(
                step2.resolutions.as_slice(),
                [QueryResolution::FreezeAccount { order, issued_queries: 2 }] if *order == key1
            ));
            assert!(h.runner.account().frozen());
            h.driver.stop();
        }
    }
}

#[test]
fn per_order_query_driver_execution_network_auto_reconnects_dropped_transport_via_dialer() {
    use tickwire_spot_execution::network::LiveNetwork;
    use tickwire_transport::dialer::Dialer;

    let venue = SpotVenue::Okx;
    let exec_srv = LoopbackTlsServer::start(venue);
    let query_srv = LoopbackTlsServer::start(venue);
    let mut h = Harness::new(venue, &exec_srv, &query_srv, QueryConfig::default());

    let dialer = Arc::new(Dialer::new().unwrap());
    let mut exec_net = LiveNetwork::new(
        exec_srv.host.clone(),
        exec_srv.address.port(),
        Arc::clone(&exec_srv.client_tls),
    )
    .with_peers(vec![exec_srv.address]);
    exec_net.attach_dialer(dialer, 77).unwrap();
    h.driver.set_execution_network(exec_net);

    // Order 1 drops the execution socket without halting or freezing!
    exec_srv.push_drop("");
    let t0 = 60_000_000u64;
    let key1 = h.submit_new(8, 1, 1, "dialdrop1", t0);
    let done1 = h.drive_execution_completion(t0 + 10);
    assert_eq!(done1.response, Reply::Unknown);
    assert!(!h.runner.has_transport_connection());
    assert!(!h.runner.halted());
    assert!(!h.runner.account().frozen());
    assert_eq!(
        h.runner.account().order_in_doubt(&key1),
        Some(DoubtReason::PlaceAmbiguous)
    );

    // Polling `PerOrderQueryDriver` notifies `Dialer` off the hot path and installs the pre-warmed socket!
    let cut = PrivateCut {
        stream: [5; 16],
        epoch: 1,
        gap_generation: 1,
        through: 1,
    };
    let deadline = common::until();
    let mut reconnected = false;
    while Instant::now() < deadline {
        let out = h.driver.poll(&mut h.runner, Some(cut), t0 + 100).unwrap();
        if out.reconnected_execution {
            reconnected = true;
            break;
        }
        thread::sleep(Duration::from_millis(2));
    }
    assert!(
        reconnected,
        "Dialer did not hand off replacement execution Connection"
    );
    assert!(h.runner.has_transport_connection());

    // Sibling Strategy 9 immediately submits Order 2 on the Dialer-installed Connection!
    exec_srv.push_reply("", 200, ack_json(venue, "native-dial-2", "dialord2"));
    let _key2 = h.submit_new(9, 2, 1, "dialord2", t0 + 200);
    assert!(matches!(
        h.drive_execution_completion(t0 + 210).response,
        Reply::Acknowledged { .. }
    ));
    assert!(!h.runner.account().frozen());
    h.driver.stop();
}

#[test]
fn spot_query_driver_not_built_preserves_issued_budget_and_bybit_mid_chain_ws_resolution_skips_history()
 {
    // 1. Pre-send `RestStatus::NotBuilt` (clock failure before wire bytes) must route to `on_not_sent`,
    //    never incrementing `issued_query_count` or freezing when `max_query_count = 1`.
    let venue = SpotVenue::Bybit;
    let exec_srv = LoopbackTlsServer::start(venue);
    let query_srv = LoopbackTlsServer::start(venue);
    let qcfg = QueryConfig {
        status_query_delay_ns: 100,
        place_retry_ns: 500,
        max_query_count: 1,
        ..QueryConfig::default()
    };
    let mut h = Harness::new(venue, &exec_srv, &query_srv, qcfg);
    h.driver.stop();

    let fail_clock = Arc::new(AtomicBool::new(true));
    let fail_clock_bg = Arc::clone(&fail_clock);
    let server_name = ServerName::try_from(query_srv.host.clone()).unwrap();
    h.driver = PerOrderQueryDriver::new(
        venue,
        Environment::Test,
        RestQuerierConfig {
            host: query_srv.host.clone(),
            port: query_srv.address.port(),
            initial_peers: vec![query_srv.address],
            security: DialSecurity::Tls {
                server_name,
                config: Arc::clone(&query_srv.client_tls),
            },
            interval_ms: 5,
            connect_timeout_ms: 3_000,
            timeout_ms: 3_000,
            ..RestQuerierConfig::default()
        },
        OwnedCredentials::bybit("key", "secret").unwrap(),
        move || {
            if fail_clock_bg.load(Ordering::Acquire) {
                Err(tickwire_gateway::Error::Encoding)
            } else {
                Ok(1_700_000_000_123)
            }
        },
    )
    .unwrap();

    exec_srv.push_drop("");
    let t0 = 70_000_000u64;
    let key1 = h.submit_new(8, 1, 1, "notbuilt1", t0);
    assert_eq!(
        h.drive_execution_completion(t0 + 10).response,
        Reply::Unknown
    );

    // First due poll hits `NotBuilt` in `AsyncRestQuerier`: must NOT freeze or count as an issued query!
    let step_unsent = h.drive_query_resolution(t0 + 200);
    assert!(
        matches!(
            step_unsent.resolutions.as_slice(),
            [QueryResolution::RetryScheduled { order, .. }] if *order == key1
        ),
        "expected RetryScheduled from on_not_sent, got {:?}",
        step_unsent.resolutions
    );
    assert!(!h.runner.account().frozen());
    assert_eq!(
        h.runner.query_desk().unwrap().issued_query_count(&key1),
        Some(0)
    );
    assert!(query_srv.seen_paths.lock().unwrap().is_empty());

    // Restore clock; now Bybit `realtime` returns empty `list: []`, AND before `handle_reply` polls it,
    // private WS cancels & closes Order 1 (`remove_order`), verifying `dispatch_bybit_history` does NOT
    // dispatch a stale `/v5/order/history` onto the freed slot!
    fail_clock.store(false, Ordering::Release);
    query_srv.push_reply(
        "/v5/order/realtime",
        200,
        json!({"retCode": 0, "result": {"category": "spot", "list": []}}).to_string(),
    );
    let cut = PrivateCut {
        stream: [8; 16],
        epoch: 1,
        gap_generation: 1,
        through: 1,
    };
    // Dispatch `realtime` onto slot 0.
    let dispatched = h.driver.poll(&mut h.runner, Some(cut), t0 + 1_000).unwrap();
    assert_eq!(dispatched.dispatched_tasks, 1);

    // Wait until `realtime` has been received by the mock server, then resolve Order 1 via WS `orders`!
    let wait_deadline = common::until();
    while query_srv.seen_paths.lock().unwrap().is_empty() && Instant::now() < wait_deadline {
        thread::sleep(Duration::from_millis(2));
    }
    let ws_cancel = ws_order_json(
        venue,
        "BTCUSDT",
        "native-nb-1",
        "notbuilt1",
        "Cancelled",
        "0",
    );
    assert_eq!(
        h.runner
            .apply_private_cash("orders", &ws_cancel, cut, t0 + 1_100)
            .unwrap(),
        CashUpdate::Accounted
    );
    assert!(!h.runner.query_desk().unwrap().has_ask(&key1));

    // Drain `AsyncRestQuerier` reply: must NOT dispatch `/v5/order/history`!
    thread::sleep(Duration::from_millis(25));
    let after = h.driver.poll(&mut h.runner, Some(cut), t0 + 1_200).unwrap();
    assert_eq!(after.dispatched_tasks, 0);
    let paths = query_srv.seen_paths.lock().unwrap().clone();
    assert!(
        paths.iter().all(|p| !p.contains("/v5/order/history")),
        "stale Bybit realtime empty reply after WS removal must not dispatch /v5/order/history: {paths:?}"
    );
    assert!(!h.runner.account().frozen());
    h.driver.stop();
}

#[test]
fn ws_partial_fill_catchup_preserves_cancel_ambiguous_and_skips_redundant_trades_query() {
    let venue = SpotVenue::Bybit;
    let exec_srv = LoopbackTlsServer::start(venue);
    let query_srv = LoopbackTlsServer::start(venue);
    let mut h = Harness::new(
        venue,
        &exec_srv,
        &query_srv,
        QueryConfig {
            status_query_delay_ns: 100,
            place_retry_ns: 100,
            not_found_grace_ns: 500,
            max_in_flight_slots: 1,
            max_query_count: 3,
            ..QueryConfig::default()
        },
    );

    let t0 = 90_000_000u64;
    // 1. Order 1: confirmed open order (`0.001` BTC), receives lagging WS `PartiallyFilled` (`0.001`)
    // followed immediately by matching WS `fills` (`0.001`). Polling `driver` must skip the resolved
    // `Trades` ask without dispatching any redundant REST `/v5/execution/list` request!
    exec_srv.push_reply("", 200, ack_json(venue, "native-pf-1", "pfcatch1"));
    let key1 = h.submit_new(8, 1, 1, "pfcatch1", t0);
    assert!(matches!(
        h.drive_execution_completion(t0 + 10).response,
        Reply::Acknowledged { .. }
    ));

    let cut1 = PrivateCut {
        stream: [9; 16],
        epoch: 1,
        gap_generation: 1,
        through: 1,
    };
    let ws_part1 = ws_order_json(
        venue,
        "BTCUSDT",
        "native-pf-1",
        "pfcatch1",
        "PartiallyFilled",
        "0.0004",
    );
    assert_eq!(
        h.runner
            .apply_private_cash("orders", &ws_part1, cut1, t0 + 20)
            .unwrap(),
        CashUpdate::Accounted
    );
    assert_eq!(
        h.runner.account().order_in_doubt(&key1),
        Some(DoubtReason::FillCatchup)
    );
    assert_eq!(
        h.runner.query_desk().unwrap().ask_kind(&key1),
        Some(AskKind::Trades)
    );

    let cut2 = PrivateCut {
        stream: [9; 16],
        epoch: 1,
        gap_generation: 1,
        through: 2,
    };
    let mut ws_fill1 = fill_row_json(
        venue,
        "BTCUSDT",
        "native-pf-1",
        "pfcatch1",
        "1001",
        "0.0004",
    );
    ws_fill1["execValue"] = json!("24");
    assert_eq!(
        h.runner
            .apply_private_cash("fills", &ws_fill1, cut2, t0 + 30)
            .unwrap(),
        CashUpdate::Accounted
    );
    assert_eq!(h.runner.account().order_in_doubt(&key1), None);

    let step1 = h.driver.poll(&mut h.runner, Some(cut2), t0 + 50).unwrap();
    assert_eq!(
        step1.dispatched_tasks, 0,
        "caught-up WS partial fill on open order must not dispatch redundant REST trades query"
    );
    assert!(!h.runner.query_desk().unwrap().has_ask(&key1));

    // 2. Order 2: confirmed open order (`0.001` BTC) with an ambiguous `Cancel` isolated, followed by
    // lagging WS `PartiallyFilled` (`0.001`) + matching WS `fills` (`0.001`). `DoubtReason::CancelAmbiguous`
    // must NOT be overwritten by `FillCatchup`, and `poll_due` must re-arm `AskKind::Cancel` cleanly!
    exec_srv.push_reply("", 200, ack_json(venue, "native-pf-2", "pfcatch2"));
    let key2 = h.submit_new(9, 2, 1, "pfcatch2", t0 + 1_000);
    assert!(matches!(
        h.drive_execution_completion(t0 + 1_100).response,
        Reply::Acknowledged { .. }
    ));

    exec_srv.push_drop("");
    let mut cancel_raw = h.template.clone();
    cancel_raw.context.request.venue = venue.id();
    cancel_raw.context.request.intent_id = 3;
    cancel_raw.context.request.recovery_object = 3;
    cancel_raw.context.request.deadline = t0 + 60_000_000_000;
    cancel_raw.context.strategy.logical_strategy_id = 9;
    cancel_raw.context.strategy.instance_id = [9; 16];
    cancel_raw.context.strategy.strategy_intent_seq = 2;
    cancel_raw.context.strategy.created_mono_ns = t0 + 2_000;
    cancel_raw.context.instrument.symbol = h.symbol.clone();
    let mut order_uuid = [0u8; 16];
    order_uuid[..8].copy_from_slice(&2u64.to_be_bytes());
    cancel_raw.body = CommandBody::Cancel(tickwire_venue::OrderTarget {
        order_id: order_uuid,
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(ClientId::new("pfcatch2").unwrap()),
        venue_order_id: None,
    });
    let cancel_cmd = CanonicalCommand::new(cancel_raw).unwrap();
    let cancel_sub = Submission {
        command: cancel_cmd.clone(),
        order_ids: Vec::new(),
        deadline: common::until(),
        recovery: RecoveryBundle {
            command_digest: cancel_cmd.digest(),
            dependencies: (2..=11)
                .map(|kind| Dependency {
                    kind,
                    schema_id: 1,
                    owner: [9; 16],
                    bytes: vec![kind; 4],
                })
                .collect(),
            inputs: encode_inputs(
                [9; 16],
                3,
                [0; 32],
                &[InputRecord {
                    kind: 1,
                    payload: b"quote",
                }],
            )
            .unwrap(),
            mode: RecoveryMode::DeterministicReplay,
            fault_model_id: [4; 32],
            pin_id: [9; 16],
            owner: [9; 16],
        },
    };
    let tc = h.time(t0 + 2_100);
    assert_eq!(h.runner.submit(cancel_sub, tc).unwrap(), Status::Queued);
    assert_eq!(
        h.drive_execution_completion(t0 + 2_200).response,
        Reply::Unknown
    );
    assert_eq!(
        h.runner.account().order_in_doubt(&key2),
        Some(DoubtReason::CancelAmbiguous)
    );

    let cut3 = PrivateCut {
        stream: [9; 16],
        epoch: 1,
        gap_generation: 1,
        through: 3,
    };
    let ws_part2 = ws_order_json(
        venue,
        "BTCUSDT",
        "native-pf-2",
        "pfcatch2",
        "PartiallyFilled",
        "0.0004",
    );
    assert_eq!(
        h.runner
            .apply_private_cash("orders", &ws_part2, cut3, t0 + 3_000)
            .unwrap(),
        CashUpdate::Accounted
    );
    assert_eq!(
        h.runner.account().order_in_doubt(&key2),
        Some(DoubtReason::CancelAmbiguous),
        "non-terminal lagging WS order must preserve CancelAmbiguous"
    );

    let cut4 = PrivateCut {
        stream: [9; 16],
        epoch: 1,
        gap_generation: 1,
        through: 4,
    };
    let mut ws_fill2 = fill_row_json(
        venue,
        "BTCUSDT",
        "native-pf-2",
        "pfcatch2",
        "1002",
        "0.0004",
    );
    ws_fill2["execValue"] = json!("24");
    assert_eq!(
        h.runner
            .apply_private_cash("fills", &ws_fill2, cut4, t0 + 3_010)
            .unwrap(),
        CashUpdate::Accounted
    );
    assert_eq!(
        h.runner.account().order_in_doubt(&key2),
        Some(DoubtReason::CancelAmbiguous)
    );

    // At `t0 + 3_020` (before re-armed Cancel delay `100` elapses), `poll` skips `Trades` and re-arms `Cancel`!
    let step2 = h
        .driver
        .poll(&mut h.runner, Some(cut4), t0 + 3_020)
        .unwrap();
    assert_eq!(step2.dispatched_tasks, 0);
    assert_eq!(
        h.runner.query_desk().unwrap().ask_kind(&key2),
        Some(AskKind::Cancel)
    );
    assert!(!h.runner.account().frozen());
    h.driver.stop();
}

#[test]
fn confirmed_order_not_found_fetches_trades_before_close_and_clears_fill_catchup() {
    let venue = SpotVenue::Bybit;
    let exec_srv = LoopbackTlsServer::start(venue);
    let query_srv = LoopbackTlsServer::start(venue);
    let cfg = QueryConfig {
        status_query_delay_ns: 100,
        ..QueryConfig::default()
    };
    let mut h = Harness::new(venue, &exec_srv, &query_srv, cfg);
    let t0 = 60_000_000u64;

    // 1. Order is acknowledged on the execution channel (`is_order_confirmed_or_acked == true`).
    exec_srv.push_reply("", 200, ack_json(venue, "native-nf-1", "nfconf1"));
    let key1 = h.submit_new(8, 1, 1, "nfconf1", t0);
    assert!(matches!(
        h.drive_execution_completion(t0 + 10).response,
        Reply::Acknowledged { .. }
    ));

    // 2. Outbound Cancel drops mid-flight (`Reply::Unknown`) -> `CancelAmbiguous` + `AskKind::Cancel`.
    exec_srv.push_drop("");
    let mut cancel_raw = h.template.clone();
    cancel_raw.context.request.venue = venue.id();
    cancel_raw.context.request.intent_id = 2;
    cancel_raw.context.request.recovery_object = 2;
    cancel_raw.context.request.deadline = t0 + 60_000_000_000;
    cancel_raw.context.strategy.logical_strategy_id = 8;
    cancel_raw.context.strategy.instance_id = [8; 16];
    cancel_raw.context.strategy.strategy_intent_seq = 2;
    cancel_raw.context.strategy.created_mono_ns = t0 + 1_000;
    cancel_raw.context.instrument.symbol = h.symbol.clone();
    let mut order_uuid = [0u8; 16];
    order_uuid[..8].copy_from_slice(&1u64.to_be_bytes());
    cancel_raw.body = CommandBody::Cancel(tickwire_venue::OrderTarget {
        order_id: order_uuid,
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(ClientId::new("nfconf1").unwrap()),
        venue_order_id: None,
    });
    let cancel_cmd = CanonicalCommand::new(cancel_raw).unwrap();
    let cancel_sub = Submission {
        command: cancel_cmd.clone(),
        order_ids: Vec::new(),
        deadline: common::until(),
        recovery: RecoveryBundle {
            command_digest: cancel_cmd.digest(),
            dependencies: (2..=11)
                .map(|kind| Dependency {
                    kind,
                    schema_id: 1,
                    owner: [8; 16],
                    bytes: vec![kind; 4],
                })
                .collect(),
            inputs: encode_inputs(
                [8; 16],
                2,
                [0; 32],
                &[InputRecord {
                    kind: 1,
                    payload: b"quote",
                }],
            )
            .unwrap(),
            mode: RecoveryMode::DeterministicReplay,
            fault_model_id: [4; 32],
            pin_id: [8; 16],
            owner: [8; 16],
        },
    };
    let tc = h.time(t0 + 1_100);
    assert_eq!(h.runner.submit(cancel_sub, tc).unwrap(), Status::Queued);
    assert_eq!(
        h.drive_execution_completion(t0 + 1_200).response,
        Reply::Unknown
    );
    assert_eq!(
        h.runner.account().order_in_doubt(&key1),
        Some(DoubtReason::CancelAmbiguous)
    );

    // 3. Both `/v5/order/realtime` and `/v5/order/history` return empty `list: []` (`NotFound` on
    //    confirmed order), while `/v5/execution/list` holds a partial fill (`0.0004` BTC).
    query_srv.push_reply(
        "/v5/order/realtime",
        200,
        json!({
            "retCode": 0,
            "result": { "category": "spot", "list": [] }
        })
        .to_string(),
    );
    query_srv.push_reply(
        "/v5/order/history",
        200,
        json!({
            "retCode": 0,
            "result": { "category": "spot", "list": [] }
        })
        .to_string(),
    );
    let mut trade_row = fill_row_json(
        venue,
        "BTCUSDT",
        "native-nf-1",
        "nfconf1",
        "exec-nf-1",
        "0.0004",
    );
    trade_row["execValue"] = json!("24");
    query_srv.push_reply(
        "/v5/execution/list",
        200,
        json!({
            "retCode": 0,
            "result": { "category": "spot", "list": [trade_row] }
        })
        .to_string(),
    );

    let step1 = h.drive_query_resolution(t0 + 2_000);
    assert!(
        step1.resolutions.iter().any(|r| matches!(
            r,
            QueryResolution::FetchTradesBeforeClose { order, .. } if *order == key1
        )),
        "confirmed NotFound must emit FetchTradesBeforeClose"
    );

    let step2 = h.drive_query_resolution(t0 + 3_000);
    assert!(
        step2
            .resolutions
            .iter()
            .any(|r| matches!(r, QueryResolution::Resolved { order } if *order == key1)),
        "subsequent Trades query must execute and resolve once fills are booked"
    );
    let paths = query_srv.seen_paths.lock().unwrap().clone();
    assert!(
        paths.iter().any(|p| p.starts_with("/v5/execution/list")),
        "driver must query /v5/execution/list instead of dropping AskKind::Trades"
    );
    let v1 = h.runner.account().order(&key1).unwrap();
    assert_eq!(v1.filled_quantity.atoms(), 40_000);
    assert_eq!(h.runner.account().order_in_doubt(&key1), None);
    assert!(!h.runner.account().frozen());
    h.driver.stop();
}
