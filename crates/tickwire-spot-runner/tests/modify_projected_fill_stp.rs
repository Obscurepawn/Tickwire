#![forbid(unsafe_code)]

#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod common;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use common::fixture;
use ring::hmac;
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    io::{BufRead, BufReader, Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    rc::Rc,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Sender},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_account::{
    AskKind, DoubtReason, Fixed, IntentKey, OrderKey, QueryConfig, QueryResolution,
};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::{
    AccountGateway, ConnectionConfig, Gate, Status,
    pool::{OrderSlotSpec, RouterConfig, SendSlot},
};
use tickwire_account_journal::InMemoryBackend;
use tickwire_app::strategy::{AppStrategy, Config as StrategyConfig, FileStrategy, MultiStrategy};
use tickwire_binance_clock::LocalTime;
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_quota::rate_limiter::{IpKey, VenuePreset, VenueRateLimiter};
use tickwire_recovery::{
    InputRecord, RecoveryMode, RuntimeBlobStore, SharedBlobStore, encode_inputs,
};
use tickwire_runtime::{
    authority::{Dependency, LocalAuthority, RecoveryBundle, RecoveryQueue},
    node::{Health, Strategy, Submission, SubmissionResult},
};
use tickwire_runtime_policy::{ControlPlane, Versions};
use tickwire_spot_execution::{
    Credentials, Reply, RestSession, SpotEncoder, SpotVenue, authority, network::OwnedCredentials,
    symbol_rate_key,
};
use tickwire_spot_runner::{
    AccountRunner, Completion, PerOrderQueryDriver, QueryStepOutcome, StrategyRejection,
    TerminalReason,
    reconcile::{CashUpdate, PrivateCut},
};
use tickwire_transport::{
    Connection, IoBudget, Limits, Security,
    dialer::DialSecurity,
    rest_querier::RestQuerierConfig,
    rustls::{self, pki_types::ServerName},
};
use tickwire_venue::{
    CanonicalCommand, ClientId, CommandBody, Decimal, Environment, ModifyOrder, OrderTarget,
    OrderType, PositionSide, Quantity, Side, Symbol, TimeInForce, VenueOrderId,
};

#[derive(Clone, Debug)]
struct CapturedRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: String,
}

#[derive(Clone, Debug)]
enum MockAction {
    Reply { status: u16, body: String },
    DropSocket,
}

struct HeldRequest {
    request: CapturedRequest,
    respond: Sender<MockAction>,
}

struct LoopbackTlsServer {
    address: SocketAddr,
    host: String,
    client_tls: Arc<rustls::ClientConfig>,
    hold_mode: Arc<AtomicBool>,
    held: Arc<(Mutex<VecDeque<HeldRequest>>, Condvar)>,
    script: Arc<Mutex<VecDeque<(String, MockAction)>>>,
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
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
        let hold_mode = Arc::new(AtomicBool::new(false));
        let held = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
        let script: Arc<Mutex<VecDeque<(String, MockAction)>>> =
            Arc::new(Mutex::new(VecDeque::new()));
        let captured: Arc<Mutex<Vec<CapturedRequest>>> = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let hold_bg = Arc::clone(&hold_mode);
        let held_bg = Arc::clone(&held);
        let script_bg = Arc::clone(&script);
        let captured_bg = Arc::clone(&captured);
        let stop_bg = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            let mut conn_threads = Vec::new();
            while !stop_bg.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((sock, _)) => {
                        let server_tls = Arc::clone(&server_cfg);
                        let hold_conn = Arc::clone(&hold_bg);
                        let held_conn = Arc::clone(&held_bg);
                        let script_conn = Arc::clone(&script_bg);
                        let cap_conn = Arc::clone(&captured_bg);
                        let stop_conn = Arc::clone(&stop_bg);
                        conn_threads.push(thread::spawn(move || {
                            let _ = sock.set_nonblocking(false);
                            let _ = sock.set_read_timeout(Some(Duration::from_millis(100)));
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
                                let method = parts[0].to_owned();
                                let path = parts[1].to_owned();
                                let mut headers = BTreeMap::new();
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
                                            if let Some((k, v)) = line.split_once(':') {
                                                let key_lc = k.trim().to_ascii_lowercase();
                                                let val_trim = v.trim().to_owned();
                                                if key_lc == "content-length" {
                                                    content_len = val_trim.parse().unwrap_or(0);
                                                }
                                                headers.insert(key_lc, val_trim);
                                            }
                                        }
                                    }
                                }
                                if !header_ok {
                                    break;
                                }
                                let mut body_bytes = vec![0u8; content_len];
                                if content_len > 0 && io.read_exact(&mut body_bytes).is_err() {
                                    break;
                                }
                                let body_str = String::from_utf8_lossy(&body_bytes).into_owned();
                                let req = CapturedRequest {
                                    method,
                                    path: path.clone(),
                                    headers,
                                    body: body_str,
                                };
                                cap_conn.lock().unwrap().push(req.clone());

                                let next_action = if hold_conn.load(Ordering::Acquire) {
                                    let (tx, rx) = mpsc::channel();
                                    {
                                        let (lock, cvar) = &*held_conn;
                                        lock.lock().unwrap().push_back(HeldRequest {
                                            request: req,
                                            respond: tx,
                                        });
                                        cvar.notify_all();
                                    }
                                    let mut recv_act = None;
                                    while !stop_conn.load(Ordering::Acquire) {
                                        match rx.recv_timeout(Duration::from_millis(50)) {
                                            Ok(a) => {
                                                recv_act = Some(a);
                                                break;
                                            }
                                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                                        }
                                    }
                                    recv_act
                                } else {
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
                                            400 => "Bad Request",
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
                        thread::sleep(Duration::from_millis(1));
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
            hold_mode,
            held,
            script,
            captured,
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

    fn take_captured(&self) -> Vec<CapturedRequest> {
        std::mem::take(&mut *self.captured.lock().unwrap())
    }

    fn wait_held(&self) -> HeldRequest {
        let deadline = common::until();
        let (lock, cvar) = &*self.held;
        let mut guard = lock.lock().unwrap();
        loop {
            if let Some(req) = guard.pop_front() {
                return req;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for held HTTP request"
            );
            let (next, _) = cvar.wait_timeout(guard, Duration::from_millis(20)).unwrap();
            guard = next;
        }
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
        self.held.1.notify_all();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

const FIXED_UTC_MS: u64 = 1_700_000_000_123;
const FIXED_OKX_ISO: &str = "2023-11-14T22:13:20.123Z";

fn fixed_clock() -> std::result::Result<u64, tickwire_gateway::Error> {
    Ok(FIXED_UTC_MS)
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut s, "{b:02x}");
    }
    s
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

impl Harness {
    fn new(
        venue: SpotVenue,
        exec_server: &LoopbackTlsServer,
        query_server: &LoopbackTlsServer,
        qcfg: QueryConfig,
        pool_slots: usize,
    ) -> Self {
        let (mut snapshot, mut catalog, mut template) = fixture::fixture();
        snapshot.scope.venue = venue.id();
        snapshot.valid_until = u64::MAX;
        snapshot.assets[0].free = fixture::units(10);
        snapshot.assets[1].free = fixture::units(50_000);

        let symbol = Symbol::new(if venue == SpotVenue::Okx {
            "BTC-USDT"
        } else {
            "BTCUSDT"
        })
        .unwrap();
        catalog.entries[0].instrument.symbol = symbol.clone();
        catalog.entries[0].fees.rates[0] = Fixed::parse("0.01", 8).unwrap();
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
        let queue = RecoveryQueue::new(32, 4 * 1024 * 1024).unwrap();
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
        let mut session = RestSession::new(gateway, 1).unwrap();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let preset = if venue == SpotVenue::Okx {
            VenuePreset::OkxSpot
        } else {
            VenuePreset::BybitSpot
        };
        let limiter = VenueRateLimiter::new(preset, 2, &[IpKey::from_ip(ip)]).unwrap();
        session.set_venue_rate_limiter(limiter);
        if pool_slots > 1 {
            session
                .enable_connection_pool(RouterConfig::default())
                .unwrap();
            for slot in 0..pool_slots {
                let mut spec =
                    OrderSlotSpec::with_connection(SendSlot::Exclusive, exec_server.connect());
                spec.local_ip = Some(ip);
                let conn_id = session.add_pool_slot(spec, Limits::default()).unwrap();
                assert_eq!(usize::from(conn_id), slot);
            }
        }
        let runner = AccountRunner::new(session, queue, objects).with_per_order_query_desk(qcfg);

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
            || Ok(FIXED_UTC_MS),
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

    fn build_submission(
        &self,
        strategy_id: u64,
        intent_id: u64,
        command: CanonicalCommand,
        order_ids: Vec<[u8; 16]>,
    ) -> Submission {
        Submission {
            command: command.clone(),
            order_ids,
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
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn make_new_sub(
        &self,
        strategy_id: u64,
        intent_id: u64,
        seq: u64,
        client_id: &str,
        side: Side,
        qty: &str,
        price: &str,
        tif: TimeInForce,
        mono_ns: u64,
    ) -> (Submission, OrderKey, [u8; 16]) {
        let mut raw = self.template.clone();
        raw.context.request.venue = self.venue.id();
        raw.context.request.intent_id = intent_id;
        raw.context.request.recovery_object = intent_id;
        raw.context.request.deadline = mono_ns + 60_000_000_000;
        raw.context.strategy.logical_strategy_id = strategy_id;
        raw.context.strategy.instance_id = [strategy_id as u8; 16];
        raw.context.strategy.economic_intent_id = [strategy_id as u8; 16];
        raw.context.strategy.strategy_intent_seq = seq;
        raw.context.strategy.created_mono_ns = mono_ns.saturating_sub(10);
        raw.context.instrument.symbol = self.symbol.clone();
        let CommandBody::New(ref mut order) = raw.body else {
            unreachable!()
        };
        order.client_id = ClientId::new(client_id).unwrap();
        order.side = side;
        order.order_type = OrderType::Limit;
        order.time_in_force = Some(tif);
        order.quantity = Quantity::Base(qty.parse::<Decimal>().unwrap());
        order.price = Some(price.parse::<Decimal>().unwrap());
        let command = CanonicalCommand::new(raw).unwrap();
        let mut order_uuid = [0u8; 16];
        order_uuid[..8].copy_from_slice(&intent_id.to_be_bytes());
        let sub = self.build_submission(strategy_id, intent_id, command, vec![order_uuid]);
        let key = OrderKey {
            intent: IntentKey {
                epoch: 4,
                intent_id,
            },
            client_id: ClientId::new(client_id).unwrap(),
        };
        (sub, key, order_uuid)
    }

    #[allow(clippy::too_many_arguments)]
    fn make_modify_sub(
        &self,
        strategy_id: u64,
        intent_id: u64,
        seq: u64,
        target: OrderTarget,
        new_client_id: Option<&str>,
        side: Side,
        qty: &str,
        price: &str,
        mono_ns: u64,
    ) -> Submission {
        let mut raw = self.template.clone();
        raw.context.request.venue = self.venue.id();
        raw.context.request.intent_id = intent_id;
        raw.context.request.recovery_object = intent_id;
        raw.context.request.deadline = mono_ns + 60_000_000_000;
        raw.context.strategy.logical_strategy_id = strategy_id;
        raw.context.strategy.instance_id = [strategy_id as u8; 16];
        raw.context.strategy.economic_intent_id = [strategy_id as u8; 16];
        raw.context.strategy.strategy_intent_seq = seq;
        raw.context.strategy.created_mono_ns = mono_ns.saturating_sub(10);
        raw.context.instrument.symbol = self.symbol.clone();
        raw.body = CommandBody::Modify(ModifyOrder {
            target,
            new_client_id: new_client_id.map(|s| ClientId::new(s).unwrap()),
            side,
            position_side: PositionSide::Both,
            quantity: qty.parse::<Decimal>().unwrap(),
            price: price.parse::<Decimal>().unwrap(),
            reduce_only: false,
        });
        let command = CanonicalCommand::new(raw).unwrap();
        self.build_submission(strategy_id, intent_id, command, vec![])
    }

    fn make_cancel_sub(
        &self,
        strategy_id: u64,
        intent_id: u64,
        seq: u64,
        target: OrderTarget,
        mono_ns: u64,
    ) -> Submission {
        let mut raw = self.template.clone();
        raw.context.request.venue = self.venue.id();
        raw.context.request.intent_id = intent_id;
        raw.context.request.recovery_object = intent_id;
        raw.context.request.deadline = mono_ns + 60_000_000_000;
        raw.context.strategy.logical_strategy_id = strategy_id;
        raw.context.strategy.instance_id = [strategy_id as u8; 16];
        raw.context.strategy.economic_intent_id = [strategy_id as u8; 16];
        raw.context.strategy.strategy_intent_seq = seq;
        raw.context.strategy.created_mono_ns = mono_ns.saturating_sub(10);
        raw.context.instrument.symbol = self.symbol.clone();
        raw.body = CommandBody::Cancel(target);
        let command = CanonicalCommand::new(raw).unwrap();
        self.build_submission(strategy_id, intent_id, command, vec![])
    }

    fn drive_execution_completion(&mut self, mono_ns: &mut u64) -> Completion {
        let deadline = common::until();
        loop {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for execution completion"
            );
            *mono_ns += 10;
            let t = self.time(*mono_ns);
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

#[allow(clippy::too_many_arguments)]
fn ws_order_row(
    venue: SpotVenue,
    symbol: &str,
    native_id: &str,
    client_id: &str,
    side_buy: bool,
    tif: TimeInForce,
    qty: &str,
    price: &str,
    cum_qty: &str,
    cum_quote: Option<&str>,
    avg_px: Option<&str>,
    status: &str,
    updated_ms: u64,
) -> Value {
    if venue == SpotVenue::Bybit {
        let tif_str = match tif {
            TimeInForce::PostOnly => "PostOnly",
            TimeInForce::Ioc => "IOC",
            TimeInForce::Fok => "FOK",
            _ => "GTC",
        };
        let mut obj = json!({
            "category": "spot",
            "isLeverage": "0",
            "symbol": symbol,
            "orderId": native_id,
            "orderLinkId": client_id,
            "side": if side_buy { "Buy" } else { "Sell" },
            "orderType": "Limit",
            "timeInForce": tif_str,
            "price": price,
            "qty": qty,
            "cumExecQty": cum_qty,
            "orderStatus": status,
            "createdTime": "1700000000100",
            "updatedTime": updated_ms.to_string()
        });
        if let Some(cq) = cum_quote {
            obj["cumExecValue"] = json!(cq);
        }
        if let Some(ap) = avg_px {
            obj["avgPrice"] = json!(ap);
        }
        obj
    } else {
        let ord_type = match tif {
            TimeInForce::PostOnly => "post_only",
            TimeInForce::Ioc => "ioc",
            TimeInForce::Fok => "fok",
            _ => "limit",
        };
        let mut obj = json!({
            "instType": "SPOT",
            "tdMode": "cash",
            "instId": symbol,
            "ordId": native_id,
            "clOrdId": client_id,
            "side": if side_buy { "buy" } else { "sell" },
            "ordType": ord_type,
            "px": price,
            "sz": qty,
            "accFillSz": cum_qty,
            "state": status,
            "tradeId": "",
            "cTime": "1700000000100",
            "uTime": updated_ms.to_string()
        });
        if let Some(ap) = avg_px {
            obj["avgPx"] = json!(ap);
        }
        obj
    }
}

fn rest_order_query_reply(venue: SpotVenue, row: Option<Value>) -> String {
    let list = match row {
        Some(r) => vec![r],
        None => vec![],
    };
    if venue == SpotVenue::Bybit {
        json!({
            "retCode": 0,
            "result": {
                "category": "spot",
                "list": list
            }
        })
        .to_string()
    } else {
        json!({
            "code": "0",
            "data": list
        })
        .to_string()
    }
}

#[allow(clippy::too_many_arguments)]
fn fill_row_with_price(
    venue: SpotVenue,
    symbol: &str,
    native_id: &str,
    client_id: &str,
    trade_id: &str,
    qty: &str,
    price: &str,
    quote_val: &str,
    time_ms: u64,
) -> Value {
    if venue == SpotVenue::Bybit {
        json!({
            "category": "spot",
            "symbol": symbol,
            "orderLinkId": client_id,
            "orderId": native_id,
            "execType": "Trade",
            "execId": trade_id,
            "execPrice": price,
            "execQty": qty,
            "execValue": quote_val,
            "execTime": time_ms.to_string(),
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
            "fillPx": price,
            "fillSz": qty,
            "fillTime": time_ms.to_string(),
            "ts": time_ms.to_string(),
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
fn bybit_and_okx_modify_wire_signing_confirmation_clamping_and_rejection() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let exec_srv = LoopbackTlsServer::start(venue);
        let query_srv = LoopbackTlsServer::start(venue);
        let mut h = Harness::new(venue, &exec_srv, &query_srv, QueryConfig::default(), 1);
        let mut now_ns = 10_000_000u64;

        // 1. Place resting PostOnly Buy order: 0.001 BTC @ 60000 USDT.
        exec_srv.push_reply("", 200, ack_json(venue, "nat1001", "cid1001"));
        let (new_sub, key, order_uuid) = h.make_new_sub(
            8,
            1,
            1,
            "cid1001",
            Side::Buy,
            "0.001",
            "60000",
            TimeInForce::PostOnly,
            now_ns,
        );
        let t = h.time(now_ns);
        assert_eq!(h.runner.submit(new_sub, t).unwrap(), Status::Queued);
        let done_new = h.drive_execution_completion(&mut now_ns);
        assert!(matches!(done_new.response, Reply::Acknowledged { .. }));
        let _ = exec_srv.take_captured();

        // Bind native order ID via private WS order confirmation.
        let mut cut = PrivateCut {
            stream: [5; 16],
            epoch: 1,
            gap_generation: 1,
            through: 1,
        };
        let ws_open = ws_order_row(
            venue,
            h.symbol.as_str(),
            "nat1001",
            "cid1001",
            true,
            TimeInForce::PostOnly,
            "0.001",
            "60000",
            "0",
            None,
            None,
            if venue == SpotVenue::Bybit {
                "New"
            } else {
                "live"
            },
            1_700_000_000_200,
        );
        now_ns += 100;
        assert_eq!(
            h.runner
                .apply_private_cash("orders", &ws_open, cut, now_ns)
                .unwrap(),
            CashUpdate::Accounted
        );

        // 2. Submit native Modify (0.002 BTC @ 61000 USDT) and verify wire path, body & HMAC signature!
        let amend_ack = if venue == SpotVenue::Bybit {
            json!({
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "orderId": "nat1001",
                    "orderLinkId": "cid1001"
                }
            })
            .to_string()
        } else {
            json!({
                "code": "0",
                "msg": "",
                "data": [{
                    "ordId": "nat1001",
                    "clOrdId": "cid1001",
                    "reqId": "reqmod2",
                    "sCode": "0",
                    "sMsg": ""
                }]
            })
            .to_string()
        };
        exec_srv.push_reply("", 200, amend_ack);

        let target = OrderTarget {
            order_id: order_uuid,
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(ClientId::new("cid1001").unwrap()),
            venue_order_id: Some(VenueOrderId::new("nat1001").unwrap()),
        };
        now_ns += 100;
        let mod_sub_1 = h.make_modify_sub(
            8,
            2,
            2,
            target.clone(),
            if venue == SpotVenue::Okx {
                Some("reqmod2")
            } else {
                None
            },
            Side::Buy,
            "0.002",
            "61000",
            now_ns,
        );
        let t = h.time(now_ns);
        assert_eq!(h.runner.submit(mod_sub_1, t).unwrap(), Status::Queued);
        let done_mod_1 = h.drive_execution_completion(&mut now_ns);
        assert!(matches!(done_mod_1.response, Reply::Acknowledged { .. }));
        assert!(!h.runner.account().frozen());

        let captured = exec_srv.take_captured();
        assert_eq!(captured.len(), 1);
        let req = &captured[0];
        assert_eq!(req.method, "POST");
        let parsed_body: Value = serde_json::from_str(&req.body).unwrap();
        let hmac_key = hmac::Key::new(hmac::HMAC_SHA256, b"secret");
        if venue == SpotVenue::Bybit {
            assert_eq!(req.path, "/v5/order/amend");
            assert_eq!(
                req.headers.get("x-bapi-api-key").map(String::as_str),
                Some("key")
            );
            assert_eq!(
                req.headers.get("x-bapi-timestamp").map(String::as_str),
                Some(&FIXED_UTC_MS.to_string() as &str)
            );
            assert_eq!(
                req.headers.get("x-bapi-recv-window").map(String::as_str),
                Some("5000")
            );
            let expected_sig = hex_lower(
                hmac::sign(
                    &hmac_key,
                    format!("{FIXED_UTC_MS}key5000{}", req.body).as_bytes(),
                )
                .as_ref(),
            );
            assert_eq!(
                req.headers.get("x-bapi-sign").map(String::as_str),
                Some(expected_sig.as_str())
            );
            assert_eq!(parsed_body["category"], "spot");
            assert_eq!(parsed_body["symbol"], "BTCUSDT");
            assert_eq!(parsed_body["orderId"], "nat1001");
            assert_eq!(parsed_body["qty"], "0.002");
            assert_eq!(parsed_body["price"], "61000");
        } else {
            assert_eq!(req.path, "/api/v5/trade/amend-order");
            assert_eq!(
                req.headers.get("ok-access-key").map(String::as_str),
                Some("key")
            );
            assert_eq!(
                req.headers.get("ok-access-passphrase").map(String::as_str),
                Some("pass")
            );
            assert_eq!(
                req.headers.get("ok-access-timestamp").map(String::as_str),
                Some(FIXED_OKX_ISO)
            );
            let expected_sig = STANDARD.encode(
                hmac::sign(
                    &hmac_key,
                    format!("{FIXED_OKX_ISO}POST/api/v5/trade/amend-order{}", req.body).as_bytes(),
                )
                .as_ref(),
            );
            assert_eq!(
                req.headers.get("ok-access-sign").map(String::as_str),
                Some(expected_sig.as_str())
            );
            assert_eq!(parsed_body["instId"], "BTC-USDT");
            assert_eq!(parsed_body["ordId"], "nat1001");
            assert_eq!(parsed_body["reqId"], "reqmod2");
            assert_eq!(parsed_body["newSz"], "0.002");
            assert_eq!(parsed_body["newPx"], "61000");
        }

        // Verify Account applied VenueEventBody::Modified.
        let view = h.runner.account().order(&key).unwrap();
        assert_eq!(view.effective_quantity, Fixed::parse("0.002", 8).unwrap());
        let (_, spec) = h.runner.account().order_terms(&key).unwrap();
        assert_eq!(spec.price, Some("61000".parse::<Decimal>().unwrap()));

        // 3. Venue clamping: submit Modify (0.003 @ 62000); HTTP ack clamps to (0.0028 @ 61800),
        // followed by private WS `order.spot` / `orders` clamping further to (0.0025 @ 61500)!
        let clamped_http = if venue == SpotVenue::Bybit {
            json!({
                "retCode": 0,
                "result": {
                    "orderId": "nat1001",
                    "orderLinkId": "cid1001",
                    "qty": "0.0028",
                    "price": "61800"
                }
            })
            .to_string()
        } else {
            json!({
                "code": "0",
                "data": [{
                    "ordId": "nat1001",
                    "clOrdId": "cid1001",
                    "newSz": "0.0028",
                    "newPx": "61800",
                    "sCode": "0",
                    "sMsg": ""
                }]
            })
            .to_string()
        };
        exec_srv.push_reply("", 200, clamped_http);
        let active_cid = h
            .runner
            .account()
            .order_terms(&key)
            .unwrap()
            .1
            .client_id
            .clone();
        let target2 = OrderTarget {
            client_id: Some(active_cid.clone()),
            ..target.clone()
        };
        now_ns += 100;
        let mod_sub_2 =
            h.make_modify_sub(8, 3, 3, target2, None, Side::Buy, "0.003", "62000", now_ns);
        let t = h.time(now_ns);
        assert_eq!(h.runner.submit(mod_sub_2, t).unwrap(), Status::Queued);
        let done_mod_2 = h.drive_execution_completion(&mut now_ns);
        assert!(matches!(done_mod_2.response, Reply::Acknowledged { .. }));
        assert_eq!(
            h.runner.account().order(&key).unwrap().effective_quantity,
            Fixed::parse("0.0028", 8).unwrap()
        );
        assert_eq!(
            h.runner.account().order_terms(&key).unwrap().1.price,
            Some("61800".parse::<Decimal>().unwrap())
        );

        // Private stream delivers clamped resting order terms (0.0025 @ 61500).
        cut.through += 1;
        let ws_clamped = ws_order_row(
            venue,
            h.symbol.as_str(),
            "nat1001",
            active_cid.as_str(),
            true,
            TimeInForce::PostOnly,
            "0.0025",
            "61500",
            "0",
            None,
            None,
            if venue == SpotVenue::Bybit {
                "New"
            } else {
                "live"
            },
            1_700_000_000_400,
        );
        now_ns += 100;
        assert_eq!(
            h.runner
                .apply_private_cash("orders", &ws_clamped, cut, now_ns)
                .unwrap(),
            CashUpdate::Accounted
        );
        assert!(!h.runner.account().frozen());
        assert_eq!(
            h.runner.account().order(&key).unwrap().effective_quantity,
            Fixed::parse("0.0025", 8).unwrap()
        );
        assert_eq!(
            h.runner.account().order_terms(&key).unwrap().1.price,
            Some("61500".parse::<Decimal>().unwrap())
        );
        let quote_reserved_after_clamp =
            h.runner.account().asset(fixture::key(2)).unwrap().reserved;

        // 4. Structured Modify rejection (PostOnly would take liquidity & Insufficient funds):
        // Resting order stays open at (0.0025 @ 61500), shadow hold rolls back, Account stays unfrozen!
        let post_only_reject = if venue == SpotVenue::Bybit {
            json!({
                "retCode": 170193,
                "retMsg": "EC_PostOnlyWillTakeLiquidity",
                "result": {}
            })
            .to_string()
        } else {
            json!({
                "code": "1",
                "msg": "All operations failed",
                "data": [{
                    "ordId": "nat1001",
                    "clOrdId": active_cid.as_str(),
                    "sCode": "51019",
                    "sMsg": "Order failed to be amended due to Post Only"
                }]
            })
            .to_string()
        };
        exec_srv.push_reply("", 200, post_only_reject);
        let target3 = OrderTarget {
            client_id: Some(active_cid.clone()),
            ..target.clone()
        };
        now_ns += 100;
        let mod_sub_3 = h.make_modify_sub(
            8,
            4,
            4,
            target3.clone(),
            None,
            Side::Buy,
            "0.004",
            "65000",
            now_ns,
        );
        let t = h.time(now_ns);
        assert_eq!(h.runner.submit(mod_sub_3, t).unwrap(), Status::Queued);
        let done_mod_3 = h.drive_execution_completion(&mut now_ns);
        assert!(matches!(done_mod_3.response, Reply::RequestIssue { .. }));
        assert!(!h.runner.account().frozen());
        let view_after_rej = h.runner.account().order(&key).unwrap();
        assert!(!view_after_rej.terminal);
        assert_eq!(
            view_after_rej.effective_quantity,
            Fixed::parse("0.0025", 8).unwrap()
        );
        assert_eq!(
            h.runner.account().order_terms(&key).unwrap().1.price,
            Some("61500".parse::<Decimal>().unwrap())
        );
        assert_eq!(
            h.runner.account().asset(fixture::key(2)).unwrap().reserved,
            quote_reserved_after_clamp
        );
        assert_eq!(
            h.runner.order_last_modify_rejection(&key),
            Some(TerminalReason::PostOnlyWouldTake)
        );

        // Structured InsufficientFunds rejection on venue (`170131` Bybit / `51008` OKX).
        let insuff_reject = if venue == SpotVenue::Bybit {
            json!({
                "retCode": 170131,
                "retMsg": "Insufficient balance.",
                "result": {}
            })
            .to_string()
        } else {
            json!({
                "code": "1",
                "msg": "Order amendment failed",
                "data": [{
                    "ordId": "nat1001",
                    "clOrdId": active_cid.as_str(),
                    "sCode": "51008",
                    "sMsg": "Order failed due to insufficient balance"
                }]
            })
            .to_string()
        };
        exec_srv.push_reply("", 200, insuff_reject);
        now_ns += 100;
        let mod_sub_4 =
            h.make_modify_sub(8, 5, 5, target3, None, Side::Buy, "0.003", "63000", now_ns);
        let t = h.time(now_ns);
        assert_eq!(h.runner.submit(mod_sub_4, t).unwrap(), Status::Queued);
        let _ = h.drive_execution_completion(&mut now_ns);
        assert!(!h.runner.account().frozen());
        assert_eq!(
            h.runner.order_last_modify_rejection(&key),
            Some(TerminalReason::InsufficientFunds)
        );

        // 5. Private WS SMP cancel classifies exact TerminalReason::SelfTradePreventedByVenue.
        cut.through += 1;
        let mut ws_smp_cancel = ws_order_row(
            venue,
            h.symbol.as_str(),
            "nat1001",
            active_cid.as_str(),
            true,
            TimeInForce::PostOnly,
            "0.0025",
            "61500",
            "0",
            None,
            None,
            if venue == SpotVenue::Bybit {
                "Cancelled"
            } else {
                "canceled"
            },
            1_700_000_000_800,
        );
        if venue == SpotVenue::Bybit {
            ws_smp_cancel["cancelType"] = json!("CancelBySmp");
            ws_smp_cancel["smpType"] = json!("CancelTaker");
        } else {
            ws_smp_cancel["cancelSource"] = json!("32");
            ws_smp_cancel["cancelSourceReason"] = json!("Self trade prevention");
        }
        now_ns += 100;
        assert_eq!(
            h.runner
                .apply_private_cash("orders", &ws_smp_cancel, cut, now_ns)
                .unwrap(),
            CashUpdate::Accounted
        );
        assert!(h.runner.account().order(&key).unwrap().terminal);
        assert_eq!(
            h.runner.order_terminal_reason(&key),
            Some(TerminalReason::SelfTradePreventedByVenue)
        );
    }
}

#[test]
fn ambiguous_modify_disconnect_isolated_to_query_desk_and_resolved_via_async_rest_while_peers_trade()
 {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let exec_srv = LoopbackTlsServer::start(venue);
        let query_srv = LoopbackTlsServer::start(venue);
        let qcfg = QueryConfig {
            status_query_delay_ns: 1_000,
            place_retry_ns: 1_000,
            ..QueryConfig::default()
        };
        let mut h = Harness::new(venue, &exec_srv, &query_srv, qcfg, 2);
        let mut now_ns = 20_000_000u64;

        // 1. Strategy 8 places Order 1 (0.001 @ 60000) and binds native ID.
        exec_srv.push_reply("", 200, ack_json(venue, "nata1", "cida1"));
        let (sub1, key1, uuid1) = h.make_new_sub(
            8,
            1,
            1,
            "cida1",
            Side::Buy,
            "0.001",
            "60000",
            TimeInForce::Gtc,
            now_ns,
        );
        assert_eq!(
            h.runner.submit(sub1, h.time(now_ns)).unwrap(),
            Status::Queued
        );
        let _ = h.drive_execution_completion(&mut now_ns);

        let cut = PrivateCut {
            stream: [6; 16],
            epoch: 1,
            gap_generation: 1,
            through: 1,
        };
        let ws_open = ws_order_row(
            venue,
            h.symbol.as_str(),
            "nata1",
            "cida1",
            true,
            TimeInForce::Gtc,
            "0.001",
            "60000",
            "0",
            None,
            None,
            if venue == SpotVenue::Bybit {
                "New"
            } else {
                "live"
            },
            1_700_000_001_000,
        );
        now_ns += 100;
        assert_eq!(
            h.runner
                .apply_private_cash("orders", &ws_open, cut, now_ns)
                .unwrap(),
            CashUpdate::Accounted
        );

        // 2. Strategy 8 submits Modify (0.002 @ 61000) on Order 1; socket drops mid-flight!
        exec_srv.push_drop("amend");
        let target1 = OrderTarget {
            order_id: uuid1,
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(ClientId::new("cida1").unwrap()),
            venue_order_id: Some(VenueOrderId::new("nata1").unwrap()),
        };
        now_ns += 100;
        let mod_sub_a = h.make_modify_sub(
            8,
            2,
            2,
            target1.clone(),
            None,
            Side::Buy,
            "0.002",
            "61000",
            now_ns,
        );
        assert_eq!(
            h.runner.submit(mod_sub_a, h.time(now_ns)).unwrap(),
            Status::Queued
        );
        let done_drop = h.drive_execution_completion(&mut now_ns);
        assert_eq!(done_drop.response, Reply::Unknown);
        assert!(!h.runner.account().frozen());
        assert_eq!(
            h.runner.account().order_in_doubt(&key1),
            Some(DoubtReason::AmendAmbiguous)
        );
        assert_eq!(
            h.runner.query_desk().unwrap().ask_kind(&key1),
            Some(AskKind::Amend)
        );

        // 3. While Order 1 is isolated in AmendAmbiguous, Strategy 9 trades uninterrupted on healthy slot!
        exec_srv.push_reply("", 200, ack_json(venue, "natpeer2", "cidpeer2"));
        now_ns += 100;
        let (sub_peer, key_peer, _) = h.make_new_sub(
            9,
            10,
            1,
            "cidpeer2",
            Side::Buy,
            "0.001",
            "59000",
            TimeInForce::Gtc,
            now_ns,
        );
        assert_eq!(
            h.runner.submit(sub_peer, h.time(now_ns)).unwrap(),
            Status::Queued
        );
        let done_peer = h.drive_execution_completion(&mut now_ns);
        assert!(matches!(done_peer.response, Reply::Acknowledged { .. }));
        assert!(h.runner.account().order_in_doubt(&key_peer).is_none());

        // 4. Async REST query resolves AmendAmbiguous (Case A: exchange applied modify clamped at 60800):
        // On Bybit, exercise empty `/v5/order/realtime` chaining to `/v5/order/history` as well!
        let snap_applied = ws_order_row(
            venue,
            h.symbol.as_str(),
            "nata1",
            "cida1",
            true,
            TimeInForce::Gtc,
            "0.002",
            "60800",
            "0",
            None,
            None,
            if venue == SpotVenue::Bybit {
                "New"
            } else {
                "live"
            },
            1_700_000_001_500,
        );
        if venue == SpotVenue::Bybit {
            query_srv.push_reply("realtime", 200, rest_order_query_reply(venue, None));
            query_srv.push_reply(
                "history",
                200,
                rest_order_query_reply(venue, Some(snap_applied)),
            );
        } else {
            query_srv.push_reply(
                "/api/v5/trade/order",
                200,
                rest_order_query_reply(venue, Some(snap_applied)),
            );
        }
        now_ns += 5_000;
        let outcome_a = h.drive_query_resolution(now_ns);
        assert!(!outcome_a.account_hazard_frozen);
        assert!(
            outcome_a
                .resolutions
                .iter()
                .any(|r| matches!(r, QueryResolution::Resolved { order } if order == &key1))
        );
        assert_eq!(h.runner.account().order_in_doubt(&key1), None);
        assert_eq!(
            h.runner.account().order(&key1).unwrap().effective_quantity,
            Fixed::parse("0.002", 8).unwrap()
        );
        assert_eq!(
            h.runner.account().order_terms(&key1).unwrap().1.price,
            Some("60800".parse::<Decimal>().unwrap())
        );
        let reserved_before_second_drop =
            h.runner.account().asset(fixture::key(2)).unwrap().reserved;

        // Reinstall replacement connection on dropped pool slot so both slots are ready again.
        for slot in 0..2u16 {
            if !h.runner.session().gateway().has_slot_connection(slot) {
                h.runner
                    .session_mut()
                    .install_pool_connection(
                        slot,
                        exec_srv.connect(),
                        Some(exec_srv.address),
                        Limits::default(),
                    )
                    .unwrap();
            }
        }

        // 5. Case B: Second Modify (0.003 @ 62000) drops mid-flight, and async REST query shows
        // exchange still holds pre-modify (0.002 @ 60800) -> emits ModifyRejected & rolls back!
        exec_srv.push_drop("amend");
        now_ns += 1_000;
        let mod_sub_b =
            h.make_modify_sub(8, 3, 3, target1, None, Side::Buy, "0.003", "62000", now_ns);
        assert_eq!(
            h.runner.submit(mod_sub_b, h.time(now_ns)).unwrap(),
            Status::Queued
        );
        let done_drop_b = h.drive_execution_completion(&mut now_ns);
        assert_eq!(done_drop_b.response, Reply::Unknown);
        assert_eq!(
            h.runner.account().order_in_doubt(&key1),
            Some(DoubtReason::AmendAmbiguous)
        );
        assert!(
            h.runner
                .account()
                .asset(fixture::key(2))
                .unwrap()
                .reserved
                .atoms()
                > reserved_before_second_drop.atoms()
        );

        let snap_unchanged = ws_order_row(
            venue,
            h.symbol.as_str(),
            "nata1",
            "cida1",
            true,
            TimeInForce::Gtc,
            "0.002",
            "60800",
            "0",
            None,
            None,
            if venue == SpotVenue::Bybit {
                "New"
            } else {
                "live"
            },
            1_700_000_002_000,
        );
        query_srv.push_reply("", 200, rest_order_query_reply(venue, Some(snap_unchanged)));
        now_ns += 5_000;
        let outcome_b = h.drive_query_resolution(now_ns);
        assert!(!outcome_b.account_hazard_frozen);
        assert!(
            outcome_b
                .resolutions
                .iter()
                .any(|r| matches!(r, QueryResolution::Resolved { order } if order == &key1))
        );
        assert_eq!(h.runner.account().order_in_doubt(&key1), None);
        assert!(!h.runner.account().frozen());
        assert_eq!(
            h.runner.account().order(&key1).unwrap().effective_quantity,
            Fixed::parse("0.002", 8).unwrap()
        );
        assert_eq!(
            h.runner.account().asset(fixture::key(2)).unwrap().reserved,
            reserved_before_second_drop
        );
        assert_eq!(
            h.runner.order_last_modify_rejection(&key1),
            Some(TerminalReason::VenueRejected)
        );
    }
}

#[test]
fn projected_fill_from_bybit_and_okx_order_streams_converges_with_settled_trades_without_double_counting()
 {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let exec_srv = LoopbackTlsServer::start(venue);
        let query_srv = LoopbackTlsServer::start(venue);
        let qcfg = QueryConfig {
            status_query_delay_ns: 1_000,
            place_retry_ns: 1_000,
            ..QueryConfig::default()
        };
        let mut h = Harness::new(venue, &exec_srv, &query_srv, qcfg, 1);
        h.runner.set_projected_fill(true);
        assert!(h.runner.projected_fill_enabled());

        let mut now_ns = 30_000_000u64;
        let initial_btc_total = h.runner.account().asset(fixture::key(1)).unwrap().total;

        // 1. Place Limit Buy 0.002 BTC @ 60000 USDT.
        exec_srv.push_reply("", 200, ack_json(venue, "natpf1", "cidpf1"));
        let (sub, key, _) = h.make_new_sub(
            8,
            1,
            1,
            "cidpf1",
            Side::Buy,
            "0.002",
            "60000",
            TimeInForce::Gtc,
            now_ns,
        );
        assert_eq!(
            h.runner.submit(sub, h.time(now_ns)).unwrap(),
            Status::Queued
        );
        let _ = h.drive_execution_completion(&mut now_ns);

        // 2. First partial order update arrives AHEAD of execution trade:
        // cum = 0.0008 BTC @ 59900 USDT (cumExecValue = 47.92 on Bybit, avgPx = 59900 on OKX).
        let mut cut = PrivateCut {
            stream: [8; 16],
            epoch: 1,
            gap_generation: 1,
            through: 1,
        };
        let ws_part_1 = ws_order_row(
            venue,
            h.symbol.as_str(),
            "natpf1",
            "cidpf1",
            true,
            TimeInForce::Gtc,
            "0.002",
            "60000",
            "0.0008",
            if venue == SpotVenue::Bybit {
                Some("47.92")
            } else {
                None
            },
            Some("59900"),
            if venue == SpotVenue::Bybit {
                "PartiallyFilled"
            } else {
                "partially_filled"
            },
            1_700_000_003_100,
        );
        now_ns += 100;
        assert_eq!(
            h.runner
                .apply_private_cash("orders", &ws_part_1, cut, now_ns)
                .unwrap(),
            CashUpdate::Accounted
        );

        let proj_1 = h.runner.drain_projected_fills();
        assert_eq!(proj_1.len(), 1);
        assert_eq!(proj_1[0].order, key);
        assert_eq!(proj_1[0].gap_quantity, Fixed::parse("0.0008", 8).unwrap());
        assert_eq!(proj_1[0].gap_price, Fixed::parse("59900", 8).unwrap());
        assert_eq!(proj_1[0].order_filled, Fixed::parse("0", 8).unwrap());
        assert_eq!(
            proj_1[0].order_projected,
            Fixed::parse("0.0008", 8).unwrap()
        );
        assert!(proj_1[0].projected);

        // Settled balance & filled_quantity remain untouched until real trade execution arrives!
        assert_eq!(
            h.runner.account().order(&key).unwrap().filled_quantity,
            Fixed::parse("0", 8).unwrap()
        );
        assert_eq!(
            h.runner.account().asset(fixture::key(1)).unwrap().total,
            initial_btc_total
        );

        // Duplicate WS order frame at same cum_qty = 0.0008 emits zero additional ProjectedFill.
        cut.through += 1;
        now_ns += 100;
        assert_eq!(
            h.runner
                .apply_private_cash("orders", &ws_part_1, cut, now_ns)
                .unwrap(),
            CashUpdate::Accounted
        );
        assert!(h.runner.drain_projected_fills().is_empty());

        // 3. Second WS order frame advances to Filled (cum = 0.002 BTC, incremental 0.0012 @ 60000):
        // Total quote = 47.92 + 72.00 = 119.92 (avgPx = 119.92 / 0.002 = 59960).
        cut.through += 1;
        let ws_filled = ws_order_row(
            venue,
            h.symbol.as_str(),
            "natpf1",
            "cidpf1",
            true,
            TimeInForce::Gtc,
            "0.002",
            "60000",
            "0.002",
            if venue == SpotVenue::Bybit {
                Some("119.92")
            } else {
                None
            },
            Some("59960"),
            if venue == SpotVenue::Bybit {
                "Filled"
            } else {
                "filled"
            },
            1_700_000_003_300,
        );
        now_ns += 100;
        assert_eq!(
            h.runner
                .apply_private_cash("orders", &ws_filled, cut, now_ns)
                .unwrap(),
            CashUpdate::Accounted
        );
        let proj_2 = h.runner.drain_projected_fills();
        assert_eq!(proj_2.len(), 1);
        assert_eq!(proj_2[0].gap_quantity, Fixed::parse("0.0012", 8).unwrap());
        assert_eq!(proj_2[0].gap_price, Fixed::parse("60000", 8).unwrap());
        assert_eq!(proj_2[0].order_projected, Fixed::parse("0.002", 8).unwrap());

        // 4. Actual trades arrive on WS `fills` AND duplicate via AsyncRestQuerier `OrderTrades`:
        // Settled ledger converges to exact 0.002 BTC filled (minus 2 * 0.000001 BTC fees) with zero double-counting!
        let trade_1 = fill_row_with_price(
            venue,
            h.symbol.as_str(),
            "natpf1",
            "cidpf1",
            "10001",
            "0.0008",
            "59900",
            "47.92",
            1_700_000_003_090,
        );
        let trade_2 = fill_row_with_price(
            venue,
            h.symbol.as_str(),
            "natpf1",
            "cidpf1",
            "10002",
            "0.0012",
            "60000",
            "72",
            1_700_000_003_290,
        );
        cut.through += 1;
        now_ns += 100;
        assert_eq!(
            h.runner
                .apply_private_cash("fills", &trade_1, cut, now_ns)
                .unwrap(),
            CashUpdate::Accounted
        );
        cut.through += 1;
        now_ns += 100;
        assert_eq!(
            h.runner
                .apply_private_cash("fills", &trade_2, cut, now_ns)
                .unwrap(),
            CashUpdate::Accounted
        );

        // Also answer the queued FillCatchup trade query with both trades to verify deduplication.
        let trades_body = if venue == SpotVenue::Bybit {
            json!({
                "retCode": 0,
                "result": {
                    "category": "spot",
                    "list": [trade_1, trade_2]
                }
            })
            .to_string()
        } else {
            json!({
                "code": "0",
                "data": [trade_1, trade_2]
            })
            .to_string()
        };
        query_srv.push_reply("", 200, trades_body);
        now_ns += 5_000;
        let _ = h.driver.poll(&mut h.runner, Some(cut), now_ns).unwrap();
        thread::sleep(Duration::from_millis(15));
        let _ = h.driver.poll(&mut h.runner, Some(cut), now_ns).unwrap();

        assert!(h.runner.drain_projected_fills().is_empty());
        assert!(!h.runner.account().frozen());
        let final_view = h.runner.account().order(&key).unwrap();
        assert_eq!(
            final_view.filled_quantity,
            Fixed::parse("0.002", 8).unwrap()
        );
        assert_eq!(
            h.runner.order_projected_filled(&key),
            Some(Fixed::parse("0.002", 8).unwrap())
        );
        assert_eq!(final_view.remaining_quantity, Fixed::parse("0", 8).unwrap());
        assert!(final_view.fee_reconciled);
        assert_eq!(
            h.runner.order_terminal_reason(&key),
            Some(TerminalReason::Filled)
        );
        let expected_btc_gain = Fixed::parse("0.001998", 8).unwrap();
        assert_eq!(
            h.runner.account().asset(fixture::key(1)).unwrap().total,
            initial_btc_gain_sum(initial_btc_total, expected_btc_gain)
        );
    }
}

fn initial_btc_gain_sum(base: Fixed, gain: Fixed) -> Fixed {
    base.checked_add(gain).unwrap()
}

#[derive(Default, Debug)]
struct StrategyReceipts {
    accepted: Vec<Status>,
    refusals: Vec<(tickwire_account::Error, Option<TerminalReason>)>,
}

struct QueueMember {
    id: u64,
    queue: Rc<RefCell<VecDeque<Submission>>>,
    receipts: Rc<RefCell<StrategyReceipts>>,
}

impl AppStrategy for QueueMember {
    fn failed(&self) -> bool {
        false
    }
}

impl Strategy for QueueMember {
    fn on_market(&mut self, _: &tickwire_binance_public::Notice<'_>) -> bool {
        true
    }
    fn next(
        &mut self,
        _: &tickwire_account::Account,
        _: Health,
        _: LocalTime,
    ) -> Option<Submission> {
        self.queue.borrow_mut().pop_front()
    }
    fn on_submission(&mut self, c: &CanonicalCommand, r: &SubmissionResult<'_>) {
        assert_eq!(self.id, c.command().context.strategy.logical_strategy_id);
        match r {
            Ok(st) => self.receipts.borrow_mut().accepted.push(*st),
            Err(err) => {
                let runner_err = err
                    .downcast_ref::<tickwire_spot_runner::Error>()
                    .expect("expected runner Error");
                let reason = runner_err.terminal_reason();
                let tickwire_spot_runner::Error::Session(
                    tickwire_spot_execution::SessionError::Gateway(
                        tickwire_account_gateway::Error::Account(acc_err),
                    ),
                ) = runner_err
                else {
                    panic!("expected Account refusal, got: {runner_err}");
                };
                self.receipts.borrow_mut().refusals.push((*acc_err, reason));
            }
        }
    }
    fn on_request_reply(&mut self, _: &CanonicalCommand, _: &tickwire_venue::RequestReply) {}
}

fn total_order_tokens_used(runner: &TestRunner, instrument_id: u64, now_ns: u64) -> u64 {
    let limiter = runner.session().venue_rate_limiter().unwrap();
    let sym_key = symbol_rate_key(instrument_id);
    if let Some(sym_win) = limiter.symbol_order_window_id(sym_key) {
        limiter.limiter().window_used(sym_win, now_ns).unwrap_or(0)
    } else {
        let win = limiter.account_order_window_ids()[0];
        limiter.limiter().window_used(win, now_ns).unwrap_or(0)
    }
}

#[test]
fn stp_crosses_book_and_modify_refusals_do_not_freeze_runner_or_burn_rate_limiter_tokens() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let exec_srv = LoopbackTlsServer::start(venue);
        let query_srv = LoopbackTlsServer::start(venue);
        let mut h = Harness::new(venue, &exec_srv, &query_srv, QueryConfig::default(), 2);
        h.runner.set_stp_enforcement(true);
        assert!(h.runner.stp_enforcement());

        let control = ControlPlane::new(tickwire_runtime_policy::Config {
            scope: h.runner.scope().clone(),
            product: h.template.context.request.product,
            executor_id: [2; 16],
            owner_generation: 1,
            writer_epoch: h.template.context.request.intent_exec_epoch,
            domain: h.domain,
            versions: Versions {
                config_revision: h.template.context.request.config_revision,
                risk_policy_version: h.template.context.strategy.risk_policy_version,
                order_config_version: h.template.context.strategy.order_config_version,
                catalog_version: h.template.context.instrument.catalog_version,
                catalog_hash: h.template.context.instrument.catalog_hash,
            },
            max_strategies: 2,
            max_feeds: 1,
            marks_per_feed: 1,
        })
        .unwrap();

        let q8 = Rc::new(RefCell::new(VecDeque::new()));
        let q9 = Rc::new(RefCell::new(VecDeque::new()));
        let r8 = Rc::new(RefCell::new(StrategyReceipts::default()));
        let r9 = Rc::new(RefCell::new(StrategyReceipts::default()));
        let mut group = MultiStrategy::new(
            vec![
                (
                    8,
                    Box::new(QueueMember {
                        id: 8,
                        queue: Rc::clone(&q8),
                        receipts: Rc::clone(&r8),
                    }),
                ),
                (
                    9,
                    Box::new(QueueMember {
                        id: 9,
                        queue: Rc::clone(&q9),
                        receipts: Rc::clone(&r9),
                    }),
                ),
            ],
            control,
        )
        .unwrap();

        let health = Health {
            calibrated: true,
            market_live: true,
            private_receiving: true,
            account_frozen: false,
            sender_halted: false,
        };
        let inst_id = h.template.context.instrument.instrument_id;
        let mut now_ns = 40_000_000u64;

        // 1. Strategy 8 places resting Limit Buy 0.001 BTC @ 60000 USDT -> 1 order token used.
        exec_srv.push_reply("", 200, ack_json(venue, "natstp1", "cidstp1"));
        let (sub_buy, _key_buy, uuid_buy) = h.make_new_sub(
            8,
            1,
            1,
            "cidstp1",
            Side::Buy,
            "0.001",
            "60000",
            TimeInForce::Gtc,
            now_ns,
        );
        q8.borrow_mut().push_back(sub_buy);
        assert!(
            h.runner
                .poll_strategy(&mut group, health, h.time(now_ns))
                .unwrap()
        );
        while h.runner.inflight_count() > 0 {
            now_ns += 10;
            let _ = h
                .runner
                .poll(&mut group, h.time(now_ns), IoBudget::default())
                .unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(total_order_tokens_used(&h.runner, inst_id, now_ns), 1);

        // 2. Strategy 8 places non-crossing Limit Sell 0.001 BTC @ 62000 USDT -> 2nd order token used.
        exec_srv.push_reply("", 200, ack_json(venue, "natstp2", "cidstp2"));
        now_ns += 100;
        let (sub_sell, _key_sell, uuid_sell) = h.make_new_sub(
            8,
            2,
            2,
            "cidstp2",
            Side::Sell,
            "0.001",
            "62000",
            TimeInForce::Gtc,
            now_ns,
        );
        q8.borrow_mut().push_back(sub_sell);
        assert!(
            h.runner
                .poll_strategy(&mut group, health, h.time(now_ns))
                .unwrap()
        );
        while h.runner.inflight_count() > 0 {
            now_ns += 10;
            let _ = h
                .runner
                .poll(&mut group, h.time(now_ns), IoBudget::default())
                .unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(total_order_tokens_used(&h.runner, inst_id, now_ns), 2);
        let _ = exec_srv.take_captured();

        // 3. Local Refusal #1: Strategy 9 submits New Sell @ 59500 crossing Strategy 8's 60000 Buy!
        now_ns += 100;
        let (cross_new, _, _) = h.make_new_sub(
            9,
            101,
            1,
            "cidcross9",
            Side::Sell,
            "0.001",
            "59500",
            TimeInForce::Gtc,
            now_ns,
        );
        q9.borrow_mut().push_back(cross_new);
        assert!(
            h.runner
                .poll_strategy(&mut group, health, h.time(now_ns))
                .unwrap()
        );

        // 4. Local Refusal #2: Strategy 8 modifies its 62000 Sell down to 60000 (crosses its 60000 Buy)!
        now_ns += 100;
        let target_sell = OrderTarget {
            order_id: uuid_sell,
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(ClientId::new("cidstp2").unwrap()),
            venue_order_id: None,
        };
        let cross_mod = h.make_modify_sub(
            8,
            3,
            3,
            target_sell,
            None,
            Side::Sell,
            "0.001",
            "60000",
            now_ns,
        );
        q8.borrow_mut().push_back(cross_mod);
        assert!(
            h.runner
                .poll_strategy(&mut group, health, h.time(now_ns))
                .unwrap()
        );

        // 5. Local Refusal #3: Strategy 8 modifies its 60000 Buy with identical (0.001, 60000) -> NoChange!
        now_ns += 100;
        let target_buy = OrderTarget {
            order_id: uuid_buy,
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(ClientId::new("cidstp1").unwrap()),
            venue_order_id: None,
        };
        let nochange_mod = h.make_modify_sub(
            8,
            4,
            4,
            target_buy.clone(),
            None,
            Side::Buy,
            "0.001",
            "60000",
            now_ns,
        );
        q8.borrow_mut().push_back(nochange_mod);
        assert!(
            h.runner
                .poll_strategy(&mut group, health, h.time(now_ns))
                .unwrap()
        );

        // 6. Local Refusal #4: Strategy 9 submits Modify for non-existent order -> OrderNotOpen!
        now_ns += 100;
        let unknown_target = OrderTarget {
            order_id: [0xee; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(ClientId::new("nonexistent").unwrap()),
            venue_order_id: None,
        };
        let not_open_mod = h.make_modify_sub(
            9,
            102,
            2,
            unknown_target,
            None,
            Side::Buy,
            "0.001",
            "58000",
            now_ns,
        );
        q9.borrow_mut().push_back(not_open_mod);
        assert!(
            h.runner
                .poll_strategy(&mut group, health, h.time(now_ns))
                .unwrap()
        );

        // Verify ZERO network packets and ZERO burned rate-limiter tokens across all 4 refusals so far!
        assert!(exec_srv.take_captured().is_empty());
        assert_eq!(total_order_tokens_used(&h.runner, inst_id, now_ns), 2);
        assert!(!h.runner.account().frozen());
        assert!(!h.runner.session().gateway().halted());

        // 7. Hold a valid Modify in-flight on Slot 0 -> uses 3rd order token (and 0 cancel tokens!),
        // then submit a concurrent 2nd Modify on the SAME order -> TooManyPendingActions with 0 extra tokens burned!
        exec_srv.hold_mode.store(true, Ordering::Release);
        now_ns += 100;
        let inflight_mod = h.make_modify_sub(
            8,
            5,
            5,
            target_buy.clone(),
            None,
            Side::Buy,
            "0.002",
            "60100",
            now_ns,
        );
        q8.borrow_mut().push_back(inflight_mod);
        assert!(
            h.runner
                .poll_strategy(&mut group, health, h.time(now_ns))
                .unwrap()
        );
        let flush_deadline = common::until();
        let held_intent = IntentKey {
            epoch: 4,
            intent_id: 5,
        };
        loop {
            assert!(Instant::now() < flush_deadline);
            now_ns += 10;
            let _ = h
                .runner
                .poll_all(&mut group, h.time(now_ns), IoBudget::default())
                .unwrap();
            if h.runner.session().gateway().status(held_intent) == Some(Status::Flushed) {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        let held_amend = exec_srv.wait_held();
        assert!(held_amend.request.path.contains("amend"));
        assert_eq!(total_order_tokens_used(&h.runner, inst_id, now_ns), 3);
        if let Some(cancel_win) = h
            .runner
            .session()
            .venue_rate_limiter()
            .unwrap()
            .symbol_cancel_window_id(symbol_rate_key(inst_id))
        {
            assert_eq!(
                h.runner
                    .session()
                    .venue_rate_limiter()
                    .unwrap()
                    .limiter()
                    .window_used(cancel_win, now_ns),
                Some(0)
            );
        }

        // Concurrent 2nd Modify on same order fails locally with TooManyPendingActions on sibling slot!
        now_ns += 100;
        let second_mod = h.make_modify_sub(
            8,
            6,
            6,
            target_buy,
            None,
            Side::Buy,
            "0.003",
            "60200",
            now_ns,
        );
        q8.borrow_mut().push_back(second_mod);
        assert!(
            h.runner
                .poll_strategy(&mut group, health, h.time(now_ns))
                .unwrap()
        );
        // Still 3 tokens used — TooManyPendingActions burned ZERO tokens!
        assert_eq!(total_order_tokens_used(&h.runner, inst_id, now_ns), 3);

        // Meanwhile Strategy 9 submits a valid non-crossing Buy @ 59000 on Slot 1 while Slot 0 holds Strategy 8's Modify!
        exec_srv.hold_mode.store(false, Ordering::Release);
        exec_srv.push_reply("", 200, ack_json(venue, "natok9", "cidok9"));
        now_ns += 100;
        let (ok_sub_9, _, _) = h.make_new_sub(
            9,
            103,
            3,
            "cidok9",
            Side::Buy,
            "0.001",
            "59000",
            TimeInForce::Gtc,
            now_ns,
        );
        q9.borrow_mut().push_back(ok_sub_9);
        assert!(
            h.runner
                .poll_strategy(&mut group, health, h.time(now_ns))
                .unwrap()
        );
        assert_eq!(total_order_tokens_used(&h.runner, inst_id, now_ns), 4);

        // Release held Modify on Slot 0 and drain both completions.
        let amend_reply = if venue == SpotVenue::Bybit {
            ack_json(venue, "natstp1", "cidstp1")
        } else {
            json!({
                "code": "0",
                "data": [{ "sCode": "0", "ordId": "natstp1", "clOrdId": "cidstp1" }]
            })
            .to_string()
        };
        held_amend
            .respond
            .send(MockAction::Reply {
                status: 200,
                body: amend_reply,
            })
            .unwrap();
        let deadline = common::until();
        while h.runner.inflight_count() > 0 {
            assert!(Instant::now() < deadline);
            now_ns += 10;
            let _ = h
                .runner
                .poll_all(&mut group, h.time(now_ns), IoBudget::default())
                .unwrap();
            thread::sleep(Duration::from_millis(1));
        }

        assert!(!h.runner.account().frozen());
        assert_eq!(
            r8.borrow().refusals,
            vec![
                (
                    tickwire_account::Error::CrossesBook,
                    Some(TerminalReason::CrossesBook)
                ),
                (
                    tickwire_account::Error::NoChange,
                    Some(TerminalReason::NoChange)
                ),
                (
                    tickwire_account::Error::TooManyPendingActions,
                    Some(TerminalReason::TooManyPendingActions)
                ),
            ]
        );
        assert_eq!(
            r9.borrow().refusals,
            vec![
                (
                    tickwire_account::Error::CrossesBook,
                    Some(TerminalReason::CrossesBook)
                ),
                (
                    tickwire_account::Error::OrderNotOpen,
                    Some(TerminalReason::OrderNotOpen)
                ),
            ]
        );

        let rejections = h.runner.drain_strategy_rejections();
        assert_eq!(
            rejections,
            vec![
                StrategyRejection {
                    logical_strategy_id: 9,
                    intent: IntentKey {
                        epoch: 4,
                        intent_id: 101
                    },
                    reason: TerminalReason::CrossesBook,
                },
                StrategyRejection {
                    logical_strategy_id: 8,
                    intent: IntentKey {
                        epoch: 4,
                        intent_id: 3
                    },
                    reason: TerminalReason::CrossesBook,
                },
                StrategyRejection {
                    logical_strategy_id: 8,
                    intent: IntentKey {
                        epoch: 4,
                        intent_id: 4
                    },
                    reason: TerminalReason::NoChange,
                },
                StrategyRejection {
                    logical_strategy_id: 9,
                    intent: IntentKey {
                        epoch: 4,
                        intent_id: 102
                    },
                    reason: TerminalReason::OrderNotOpen,
                },
                StrategyRejection {
                    logical_strategy_id: 8,
                    intent: IntentKey {
                        epoch: 4,
                        intent_id: 6
                    },
                    reason: TerminalReason::TooManyPendingActions,
                },
            ]
        );

        // Cancel cleanup check so make_cancel_sub helper is exercised too.
        exec_srv.push_reply("", 200, ack_json(venue, "natok9", "cidok9"));
        let mut ok9_uuid = [0u8; 16];
        ok9_uuid[..8].copy_from_slice(&103u64.to_be_bytes());
        now_ns += 100;
        let cancel_ok9 = h.make_cancel_sub(
            9,
            104,
            4,
            OrderTarget {
                order_id: ok9_uuid,
                expected_owner_revision: 1,
                original_intent_exec_epoch: 4,
                client_id: Some(ClientId::new("cidok9").unwrap()),
                venue_order_id: None,
            },
            now_ns,
        );
        assert_eq!(
            h.runner.submit(cancel_ok9, h.time(now_ns)).unwrap(),
            Status::Queued
        );
        let _ = h.drive_execution_completion(&mut now_ns);

        // Bug 5 regression: repeated local CrossesBook refusals must not consume Origins slots
        // or VenueRateLimiter tokens, and pre-send Status::Aborted burns zero VenueRateLimiter tokens!
        let tokens_before_loop = total_order_tokens_used(&h.runner, inst_id, now_ns);
        // Resting Sell @ 61000 is still open from Strategy 9 (`sell_sub` @ 61000).
        // Hammer > DEFAULT_ORIGIN_ORDERS (1030) local CrossesBook Buy @ 62000 refusals inside 24h window:
        for i in 0..1_040u64 {
            now_ns += 1;
            let (bad_sub, _, _) = h.make_new_sub(
                8,
                10_000 + i,
                1_000 + i,
                &format!("cbref{i}"),
                Side::Buy,
                "0.001",
                "62000",
                TimeInForce::Gtc,
                now_ns,
            );
            let err = h.runner.submit(bad_sub, h.time(now_ns)).unwrap_err();
            assert_eq!(err.terminal_reason(), Some(TerminalReason::CrossesBook));
        }
        assert!(!h.runner.halted());
        assert!(!h.runner.account().frozen());
        assert_eq!(
            total_order_tokens_used(&h.runner, inst_id, now_ns),
            tokens_before_loop
        );
        let _ = h.runner.drain_strategy_rejections();

        // A valid non-crossing order STILL admits and registers in Origins immediately afterward!
        exec_srv.push_reply("", 200, ack_json(venue, "natafter", "cidafter"));
        now_ns += 100;
        let (good_sub, _, _) = h.make_new_sub(
            8,
            30_000,
            3_000,
            "cidafter",
            Side::Buy,
            "0.001",
            "58000",
            TimeInForce::Gtc,
            now_ns,
        );
        assert_eq!(
            h.runner.submit(good_sub, h.time(now_ns)).unwrap(),
            Status::Queued
        );
        let _ = h.drive_execution_completion(&mut now_ns);
        assert!(!h.runner.halted());
        assert!(!h.runner.account().frozen());
    }
}
