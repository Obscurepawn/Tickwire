#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod common;

use common::fixture;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    io::{BufRead, BufReader, Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Sender},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_account::{DoubtReason, Fixed, IntentKey, OrderKey, QueryConfig, QueryResolution};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::{
    AccountGateway, ConnectionConfig, Gate, Status,
    pool::{OrderSlotSpec, RouterConfig, SendSlot},
};
use tickwire_account_journal::InMemoryBackend;
use tickwire_app::strategy::{Config as StrategyConfig, FileStrategy};
use tickwire_binance_clock::LocalTime;
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_quota::rate_limiter::{IpKey, RateFlags, RateReject, VenuePreset, VenueRateLimiter};
use tickwire_recovery::{
    InputRecord, RecoveryMode, RuntimeBlobStore, SharedBlobStore, encode_inputs,
};
use tickwire_runtime::{
    authority::{Dependency, LocalAuthority, RecoveryBundle, RecoveryQueue},
    node::{Health, Strategy, Submission},
};
use tickwire_spot_execution::{
    Credentials, Reply, RestSession, SessionError, SpotEncoder, SpotVenue, authority,
    network::{LiveNetwork, OwnedCredentials},
    symbol_rate_key,
};
use tickwire_spot_runner::{
    AccountRunner, Completion, Error as RunnerError, PerOrderQueryDriver, reconcile::PrivateCut,
};
use tickwire_transport::{
    Connection, IoBudget, Limits, Security,
    dialer::{DialBackoff, DialSecurity, Dialer},
    rest_querier::RestQuerierConfig,
    rustls::{self, pki_types::ServerName},
};
use tickwire_venue::{CanonicalCommand, ClientId, CommandBody, Environment, OrderTarget, Symbol};

#[derive(Clone, Debug)]
enum ServerDirective {
    Reply {
        status: u16,
        headers: Vec<(String, String)>,
        body: String,
    },
    DropSocket,
}

struct HeldRequest {
    _path: String,
    client_id: Option<String>,
    _peer_ip: IpAddr,
    respond: Sender<ServerDirective>,
}

#[derive(Clone)]
struct SharedTlsMaterial {
    host: String,
    server_cfg: Arc<rustls::ServerConfig>,
    client_tls: Arc<rustls::ClientConfig>,
}

impl SharedTlsMaterial {
    fn new(venue: SpotVenue) -> Self {
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
        Self {
            host,
            server_cfg,
            client_tls,
        }
    }
}

struct GatedTlsServer {
    address: SocketAddr,
    host: String,
    client_tls: Arc<rustls::ClientConfig>,
    hold_mode: Arc<AtomicBool>,
    held: Arc<(Mutex<VecDeque<HeldRequest>>, Condvar)>,
    script: Arc<Mutex<VecDeque<(String, ServerDirective)>>>,
    seen: Arc<Mutex<Vec<(String, IpAddr)>>>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl GatedTlsServer {
    fn start(tls: &SharedTlsMaterial, auto_ack_venue: Option<SpotVenue>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let hold_mode = Arc::new(AtomicBool::new(false));
        let held = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
        let script: Arc<Mutex<VecDeque<(String, ServerDirective)>>> =
            Arc::new(Mutex::new(VecDeque::new()));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));

        let server_cfg = Arc::clone(&tls.server_cfg);
        let hold_bg = Arc::clone(&hold_mode);
        let held_bg = Arc::clone(&held);
        let script_bg = Arc::clone(&script);
        let seen_bg = Arc::clone(&seen);
        let stop_bg = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            let mut conn_threads = Vec::new();
            while !stop_bg.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((sock, peer_addr)) => {
                        let peer_ip = peer_addr.ip();
                        let srv_tls = Arc::clone(&server_cfg);
                        let hold_conn = Arc::clone(&hold_bg);
                        let held_conn = Arc::clone(&held_bg);
                        let script_conn = Arc::clone(&script_bg);
                        let seen_conn = Arc::clone(&seen_bg);
                        let stop_conn = Arc::clone(&stop_bg);
                        conn_threads.push(thread::spawn(move || {
                            let _ = sock.set_nonblocking(false);
                            let _ = sock.set_read_timeout(Some(Duration::from_millis(100)));
                            let Ok(tls_conn) = rustls::ServerConnection::new(srv_tls) else {
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
                                let mut body_bytes = vec![0u8; content_len];
                                if content_len > 0 && io.read_exact(&mut body_bytes).is_err() {
                                    break;
                                }
                                let parsed_client_id = serde_json::from_slice::<Value>(&body_bytes)
                                    .ok()
                                    .and_then(|v| {
                                        v.get("clOrdId")
                                            .or_else(|| v.get("orderLinkId"))
                                            .and_then(Value::as_str)
                                            .map(str::to_owned)
                                    });
                                seen_conn.lock().unwrap().push((path.clone(), peer_ip));

                                let directive = if hold_conn.load(Ordering::Acquire) {
                                    let (tx, rx) = mpsc::channel();
                                    {
                                        let (lock, cvar) = &*held_conn;
                                        lock.lock().unwrap().push_back(HeldRequest {
                                            _path: path.clone(),
                                            client_id: parsed_client_id.clone(),
                                            _peer_ip: peer_ip,
                                            respond: tx,
                                        });
                                        cvar.notify_all();
                                    }
                                    let mut recv_dir = None;
                                    while !stop_conn.load(Ordering::Acquire) {
                                        match rx.recv_timeout(Duration::from_millis(50)) {
                                            Ok(d) => {
                                                recv_dir = Some(d);
                                                break;
                                            }
                                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                                        }
                                    }
                                    recv_dir
                                } else {
                                    let scripted = {
                                        let mut q = script_conn.lock().unwrap();
                                        let pos = q.iter().position(|(pat, _)| {
                                            pat.is_empty() || path.contains(pat)
                                        });
                                        pos.and_then(|idx| q.remove(idx)).map(|(_, act)| act)
                                    };
                                    scripted.or_else(|| {
                                        let v = auto_ack_venue?;
                                        let cid = parsed_client_id.as_deref().unwrap_or("cid");
                                        let native = format!("nat-{cid}");
                                        Some(ServerDirective::Reply {
                                            status: 200,
                                            headers: Vec::new(),
                                            body: ack_json(v, &native, cid),
                                        })
                                    })
                                };

                                match directive {
                                    Some(ServerDirective::DropSocket) | None => break,
                                    Some(ServerDirective::Reply {
                                        status,
                                        headers,
                                        body,
                                    }) => {
                                        let reason = match status {
                                            200 => "OK",
                                            401 => "Unauthorized",
                                            429 => "Too Many Requests",
                                            _ => "Error",
                                        };
                                        let mut extra = String::new();
                                        for (hk, hv) in headers {
                                            extra.push_str(&format!("{hk}: {hv}\r\n"));
                                        }
                                        if write!(
                                            io.get_mut(),
                                            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: keep-alive\r\n{extra}\r\n{body}",
                                            body.len(),
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
            host: tls.host.clone(),
            client_tls: Arc::clone(&tls.client_tls),
            hold_mode,
            held,
            script,
            seen,
            stop,
            handle: Some(handle),
        }
    }

    fn set_hold_mode(&self, hold: bool) {
        self.hold_mode.store(hold, Ordering::Release);
    }

    fn wait_held(&self, timeout: Duration) -> HeldRequest {
        let deadline = Instant::now() + timeout;
        let (lock, cvar) = &*self.held;
        let mut guard = lock.lock().unwrap();
        while guard.is_empty() {
            let rem = deadline
                .checked_duration_since(Instant::now())
                .expect("timed out waiting for held HTTP request");
            let (next_guard, _) = cvar.wait_timeout(guard, rem).unwrap();
            guard = next_guard;
        }
        guard.pop_front().unwrap()
    }

    fn push_reply(&self, path_substr: &str, status: u16, body: impl Into<String>) {
        self.script.lock().unwrap().push_back((
            path_substr.to_owned(),
            ServerDirective::Reply {
                status,
                headers: Vec::new(),
                body: body.into(),
            },
        ));
    }

    fn push_reply_with_headers(
        &self,
        path_substr: &str,
        status: u16,
        headers: Vec<(&str, &str)>,
        body: impl Into<String>,
    ) {
        self.script.lock().unwrap().push_back((
            path_substr.to_owned(),
            ServerDirective::Reply {
                status,
                headers: headers
                    .into_iter()
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect(),
                body: body.into(),
            },
        ));
    }

    fn connect_from(&self, local_ip: Option<IpAddr>) -> Connection {
        let server_name = ServerName::try_from(self.host.clone()).unwrap();
        Connection::connect_from(
            self.address,
            local_ip,
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

impl Drop for GatedTlsServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let (_, cvar) = &*self.held;
        cvar.notify_all();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
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

fn status_query_json(
    venue: SpotVenue,
    symbol: &str,
    native_id: &str,
    client_id: &str,
    status: &str,
    cum_qty: &str,
) -> String {
    if venue == SpotVenue::Bybit {
        json!({
            "retCode": 0,
            "result": {
                "category": "spot",
                "list": [{
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
                }]
            }
        })
        .to_string()
    } else {
        json!({
            "code": "0",
            "data": [{
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
            }]
        })
        .to_string()
    }
}

type GateClosure = Box<
    dyn FnMut(
        &CanonicalCommand,
        &tickwire_account::Admission,
    ) -> std::result::Result<Gate, tickwire_account_gateway::AuthorityError>,
>;

type PooledTestRunner = AccountRunner<
    'static,
    InMemoryBackend,
    LocalAuthority<SharedBlobStore<RuntimeBlobStore>, GateClosure>,
    fn() -> std::result::Result<u64, tickwire_gateway::Error>,
    SharedBlobStore<RuntimeBlobStore>,
>;

fn fixed_clock() -> std::result::Result<u64, tickwire_gateway::Error> {
    Ok(1_700_000_000_123)
}

struct PooledHarness {
    venue: SpotVenue,
    symbol: Symbol,
    instrument_id: u64,
    domain: ClockDomain,
    gate_now: Arc<AtomicU64>,
    template: tickwire_venue::Command,
    runner: PooledTestRunner,
    driver: PerOrderQueryDriver,
    strategy: FileStrategy,
}

impl PooledHarness {
    fn new(
        venue: SpotVenue,
        bootstrap_srv: &GatedTlsServer,
        query_srv: &GatedTlsServer,
        router_cfg: RouterConfig,
        qcfg: QueryConfig,
    ) -> Self {
        let (mut snapshot, mut catalog, mut template) = fixture::fixture();
        snapshot.scope.venue = venue.id();
        snapshot.valid_until = u64::MAX;
        // Provision generous quote cash and per-strategy limits so 60+ orders never hit capital caps.
        snapshot.assets[1].free = fixture::units(1_000_000);

        let symbol = Symbol::new(if venue == SpotVenue::Okx {
            "BTC-USDT"
        } else {
            "BTCUSDT"
        })
        .unwrap();
        catalog.entries[0].instrument.symbol = symbol.clone();
        catalog.entries[0].fees.rates[0] = Fixed::parse("0.01", 8).unwrap();
        catalog.entries[0].max_absolute_position = fixture::units(10_000);
        catalog.entries[0].max_gross_quantity = fixture::units(10_000);
        catalog.entries[0].strategy_limits =
            vec![(8, fixture::units(10_000)), (9, fixture::units(10_000))];

        let instrument_id = catalog.entries[0].instrument.instrument_id;
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
        cfg.max_intents = 256;
        let queue = RecoveryQueue::new(256, 16 * 1024 * 1024).unwrap();
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
                address: bootstrap_srv.address,
                tls: Arc::clone(&bootstrap_srv.client_tls),
                limits: Limits::default(),
                connect_deadline: common::until(),
            },
        )
        .unwrap();
        let session = RestSession::new(gateway, 1)
            .unwrap()
            .with_connection_pool(router_cfg)
            .unwrap();
        let runner = AccountRunner::new(session, queue, objects).with_per_order_query_desk(qcfg);

        let owned_creds = if venue == SpotVenue::Okx {
            OwnedCredentials::okx("key", "secret", "pass").unwrap()
        } else {
            OwnedCredentials::bybit("key", "secret").unwrap()
        };
        let server_name = ServerName::try_from(query_srv.host.clone()).unwrap();
        let driver = PerOrderQueryDriver::new(
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
            owned_creds,
            || Ok(1_700_000_000_123),
        )
        .unwrap();

        Self {
            venue,
            symbol,
            instrument_id,
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

    fn make_new_submission(
        &self,
        strategy_id: u64,
        intent_id: u64,
        seq: u64,
        client_id: &str,
        mono_ns: u64,
    ) -> (Submission, OrderKey, [u8; 16]) {
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
            recovery: make_recovery_bundle(strategy_id, intent_id, command.digest()),
        };
        let key = OrderKey {
            intent: IntentKey {
                epoch: 4,
                intent_id,
            },
            client_id: ClientId::new(client_id).unwrap(),
        };
        (sub, key, order_uuid)
    }

    fn make_cancel_submission(
        &self,
        strategy_id: u64,
        intent_id: u64,
        seq: u64,
        target_uuid: [u8; 16],
        client_id: &str,
        mono_ns: u64,
    ) -> Submission {
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
        raw.body = CommandBody::Cancel(OrderTarget {
            order_id: target_uuid,
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(ClientId::new(client_id).unwrap()),
            venue_order_id: None,
        });
        let command = CanonicalCommand::new(raw).unwrap();
        Submission {
            command: command.clone(),
            order_ids: Vec::new(),
            deadline: common::until(),
            recovery: make_recovery_bundle(strategy_id, intent_id, command.digest()),
        }
    }

    fn drive_until_flushed(&mut self, intents: &[IntentKey], base_mono_ns: &mut u64) {
        let deadline = common::until();
        while Instant::now() < deadline {
            *base_mono_ns += 10;
            let t = self.time(*base_mono_ns);
            let _ = self
                .runner
                .poll_all(&mut self.strategy, t, IoBudget::default())
                .unwrap();
            if intents
                .iter()
                .all(|k| self.runner.session().gateway().status(*k) == Some(Status::Flushed))
            {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("timed out flushing pooled intents");
    }

    fn drive_n_completions(&mut self, want: usize, recv_mono_ns: u64) -> Vec<Completion> {
        let deadline = common::until();
        let mut out = Vec::with_capacity(want);
        let mut mono = recv_mono_ns;
        while out.len() < want {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {want} completions (got {})",
                out.len()
            );
            let t = self.time(mono);
            let batch = self
                .runner
                .poll_all(&mut self.strategy, t, IoBudget::default())
                .unwrap();
            out.extend(batch);
            if out.len() < want {
                mono += 1;
                thread::sleep(Duration::from_millis(1));
            }
        }
        out
    }
}

fn make_recovery_bundle(strategy_id: u64, intent_id: u64, digest: [u8; 32]) -> RecoveryBundle {
    RecoveryBundle {
        command_digest: digest,
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
    }
}

struct OneShotStrategy {
    pending: Option<Submission>,
    last_err: Option<String>,
}

impl Strategy for OneShotStrategy {
    fn on_market(&mut self, _notice: &tickwire_binance_public::Notice<'_>) -> bool {
        true
    }
    fn next(
        &mut self,
        _account: &tickwire_account::Account,
        _health: Health,
        _now: LocalTime,
    ) -> Option<Submission> {
        self.pending.take()
    }
    fn on_submission(
        &mut self,
        _command: &CanonicalCommand,
        result: &tickwire_runtime::node::SubmissionResult<'_>,
    ) {
        if let Err(e) = result {
            self.last_err = Some((*e).to_string());
        }
    }
}

#[test]
fn three_slot_concurrent_pool_ewma_hysteresis_and_single_link_drop() {
    let tls = SharedTlsMaterial::new(SpotVenue::Okx);
    let srv0 = GatedTlsServer::start(&tls, Some(SpotVenue::Okx));
    let srv1 = GatedTlsServer::start(&tls, Some(SpotVenue::Okx));
    let srv2 = GatedTlsServer::start(&tls, Some(SpotVenue::Okx));
    let query_srv = GatedTlsServer::start(&tls, None);

    let dialer = Arc::new(Dialer::new().unwrap());
    let fast_backoff = DialBackoff {
        initial_ms: 5,
        max_ms: 50,
    };

    let mut h = PooledHarness::new(
        SpotVenue::Okx,
        &srv0,
        &query_srv,
        RouterConfig::default(),
        QueryConfig {
            status_query_delay_ns: 1_000_000,
            place_retry_ns: 5_000_000,
            not_found_grace_ns: 20_000_000,
            max_query_count: 6,
            ..QueryConfig::default()
        },
    );

    for (i, srv) in [&srv0, &srv1, &srv2].into_iter().enumerate() {
        let net = LiveNetwork::new(&tls.host, srv.address.port(), Arc::clone(&tls.client_tls))
            .with_peers(vec![srv.address])
            .with_backoff(fast_backoff);
        let cid = net
            .attach_pool_slot(h.runner.session_mut(), &dialer, 100 + i as u64, None, None)
            .unwrap();
        assert_eq!(cid, i as u16);
    }

    // Wait for background "tw-dialer" to pre-warm and install all 3 slots.
    let wait_deadline = common::until();
    let mut now_ns = 10_000_000u64;
    while h
        .runner
        .session()
        .gateway()
        .connection_pool()
        .unwrap()
        .router()
        .established_count()
        < 3
    {
        assert!(Instant::now() < wait_deadline, "dialer prewarm timeout");
        now_ns += 100_000;
        let _ = h.runner.poll_pool_dials(now_ns);
        thread::sleep(Duration::from_millis(2));
    }
    assert!(h.runner.available());

    // 1. Hold all 3 servers and submit 3 concurrent `New` orders before any server replies!
    srv0.set_hold_mode(true);
    srv1.set_hold_mode(true);
    srv2.set_hold_mode(true);

    let sent_batch1_ns = 20_000_000u64;
    let (sub1, key1, _) = h.make_new_submission(8, 1, 1, "conc1", sent_batch1_ns);
    let (sub2, key2, _) = h.make_new_submission(9, 2, 1, "conc2", sent_batch1_ns);
    let (sub3, key3, _) = h.make_new_submission(8, 3, 2, "conc3", sent_batch1_ns);

    let t_send = h.time(sent_batch1_ns);
    assert_eq!(h.runner.submit(sub1, t_send).unwrap(), Status::Queued);
    assert!(h.runner.available());
    assert_eq!(h.runner.submit(sub2, t_send).unwrap(), Status::Queued);
    assert!(h.runner.available());
    assert_eq!(h.runner.submit(sub3, t_send).unwrap(), Status::Queued);
    assert_eq!(h.runner.inflight_count(), 3);
    assert!(!h.runner.available());

    // A 4th concurrent order while all 3 exclusive HTTP/1.1 slots are busy returns Busy.
    let (sub_busy, _, _) = h.make_new_submission(9, 999, 2, "concbusy", sent_batch1_ns);
    assert!(matches!(
        h.runner.submit(sub_busy, t_send),
        Err(RunnerError::Busy)
    ));

    // Warmup round-robins across slots 0 -> 1 -> 2.
    let gw = h.runner.session().gateway();
    assert_eq!(gw.intent_slot(key1.intent).map(|(c, _)| c), Some(0));
    assert_eq!(gw.intent_slot(key2.intent).map(|(c, _)| c), Some(1));
    assert_eq!(gw.intent_slot(key3.intent).map(|(c, _)| c), Some(2));

    // Flush all 3 orders over TLS before any server sends a byte back.
    now_ns = sent_batch1_ns;
    h.drive_until_flushed(&[key1.intent, key2.intent, key3.intent], &mut now_ns);

    let held0 = srv0.wait_held(Duration::from_secs(3));
    let held1 = srv1.wait_held(Duration::from_secs(3));
    let held2 = srv2.wait_held(Duration::from_secs(3));
    assert_eq!(held0.client_id.as_deref(), Some("conc1"));
    assert_eq!(held1.client_id.as_deref(), Some("conc2"));
    assert_eq!(held2.client_id.as_deref(), Some("conc3"));

    // Reply on slot 0 at +100us, slot 1 at +200us, slot 2 at +300us so each slot accrues its distinct RTT.
    held0
        .respond
        .send(ServerDirective::Reply {
            status: 200,
            headers: Vec::new(),
            body: ack_json(SpotVenue::Okx, "nat1", "conc1"),
        })
        .unwrap();
    let c0 = h.drive_n_completions(1, sent_batch1_ns + 100_000);
    assert_eq!(c0[0].intent, key1.intent);
    assert_eq!(c0[0].logical_strategy_id, 8);

    held1
        .respond
        .send(ServerDirective::Reply {
            status: 200,
            headers: Vec::new(),
            body: ack_json(SpotVenue::Okx, "nat2", "conc2"),
        })
        .unwrap();
    let c1 = h.drive_n_completions(1, sent_batch1_ns + 108_000);
    assert_eq!(c1[0].intent, key2.intent);
    assert_eq!(c1[0].logical_strategy_id, 9);

    held2
        .respond
        .send(ServerDirective::Reply {
            status: 200,
            headers: Vec::new(),
            body: ack_json(SpotVenue::Okx, "nat3", "conc3"),
        })
        .unwrap();
    let c2 = h.drive_n_completions(1, sent_batch1_ns + 300_000);
    assert_eq!(c2[0].intent, key3.intent);
    assert_eq!(c2[0].logical_strategy_id, 8);

    // 2. Drive 7 more warmup samples per slot (total = 8 samples each):
    // slot 0 = 100us, slot 1 = 108us, slot 2 = 300us.
    srv0.set_hold_mode(false);
    srv1.set_hold_mode(false);
    srv2.set_hold_mode(false);

    let mut next_intent_id = 10u64;
    let mut seq8 = 3u64;
    now_ns = 30_000_000;
    for round in 1..7u64 {
        for (expected_slot, rtt_ns) in [(0u16, 100_000u64), (1u16, 108_000u64), (2u16, 300_000u64)]
        {
            now_ns += 1_000_000;
            let cid = format!("warm{round}s{expected_slot}");
            let (sub, key, _) = h.make_new_submission(8, next_intent_id, seq8, &cid, now_ns);
            next_intent_id += 1;
            seq8 += 1;
            let t = h.time(now_ns);
            assert_eq!(h.runner.submit(sub, t).unwrap(), Status::Queued);
            assert_eq!(
                h.runner
                    .session()
                    .gateway()
                    .intent_slot(key.intent)
                    .map(|(c, _)| c),
                Some(expected_slot),
                "warmup round {round} should round-robin to slot {expected_slot}"
            );
            let done = h.drive_n_completions(1, now_ns + rtt_ns);
            assert_eq!(done[0].intent, key.intent);
        }
    }

    // Final warmup round (8th sample on each slot): submit concurrently while all 3 still have 7 samples!
    srv0.set_hold_mode(true);
    srv1.set_hold_mode(true);
    srv2.set_hold_mode(true);
    now_ns += 1_000_000;
    let round8_sent_ns = now_ns;
    let mut round8_keys = Vec::new();
    for expected_slot in 0..3u16 {
        let cid = format!("warm7s{expected_slot}");
        let (sub, key, _) = h.make_new_submission(8, next_intent_id, seq8, &cid, round8_sent_ns);
        next_intent_id += 1;
        seq8 += 1;
        assert_eq!(
            h.runner.submit(sub, h.time(round8_sent_ns)).unwrap(),
            Status::Queued
        );
        assert_eq!(
            h.runner
                .session()
                .gateway()
                .intent_slot(key.intent)
                .map(|(c, _)| c),
            Some(expected_slot)
        );
        round8_keys.push(key.intent);
    }
    h.drive_until_flushed(&round8_keys, &mut now_ns);
    let r8_0 = srv0.wait_held(Duration::from_secs(3));
    let r8_1 = srv1.wait_held(Duration::from_secs(3));
    let r8_2 = srv2.wait_held(Duration::from_secs(3));
    srv0.set_hold_mode(false);
    srv1.set_hold_mode(false);
    srv2.set_hold_mode(false);
    r8_0.respond
        .send(ServerDirective::Reply {
            status: 200,
            headers: Vec::new(),
            body: ack_json(SpotVenue::Okx, "nat-w7s0", "warm7s0"),
        })
        .unwrap();
    let _ = h.drive_n_completions(1, round8_sent_ns + 100_000);
    r8_1.respond
        .send(ServerDirective::Reply {
            status: 200,
            headers: Vec::new(),
            body: ack_json(SpotVenue::Okx, "nat-w7s1", "warm7s1"),
        })
        .unwrap();
    let _ = h.drive_n_completions(1, round8_sent_ns + 108_000);
    r8_2.respond
        .send(ServerDirective::Reply {
            status: 200,
            headers: Vec::new(),
            body: ack_json(SpotVenue::Okx, "nat-w7s2", "warm7s2"),
        })
        .unwrap();
    let _ = h.drive_n_completions(1, round8_sent_ns + 300_000);
    now_ns = round8_sent_ns + 300_000;

    // All 3 slots now have >= 8 samples: slot 0 (100us), slot 1 (108us), slot 2 (300us).
    // `last_conn_id` was 2 (300us); 300 * 8 = 2400 > 100 * 9 = 900, so router locks onto fastest slot 0!
    for idx in 0..3u64 {
        now_ns += 1_000_000;
        let cid = format!("fast{idx}");
        let (sub, key, _) = h.make_new_submission(8, next_intent_id, seq8, &cid, now_ns);
        next_intent_id += 1;
        seq8 += 1;
        let t = h.time(now_ns);
        assert_eq!(h.runner.submit(sub, t).unwrap(), Status::Queued);
        assert_eq!(
            h.runner
                .session()
                .gateway()
                .intent_slot(key.intent)
                .map(|(c, _)| c),
            Some(0),
            "post-warmup order {idx} must lock onto fastest slot 0"
        );
        let _ = h.drive_n_completions(1, now_ns + 100_000);
    }

    // 3. Verify `9/8` (12.5%) hysteresis band!
    // Hold slot 0 (100us) briefly while submitting one order onto slot 1 (108us) so `last_conn_id`
    // becomes `Some(1)`, then complete both at +100us/+108us so `score0 = 100us < score1 = 108us`
    // with `108 * 8 = 864 <= 100 * 9 = 900`.
    srv0.set_hold_mode(true);
    srv1.set_hold_mode(true);
    now_ns += 1_000_000;
    let hold0_sent = now_ns;
    let (sub_hold0, key_hold0, _) =
        h.make_new_submission(8, next_intent_id, seq8, "holdslot0", hold0_sent);
    next_intent_id += 1;
    seq8 += 1;
    assert_eq!(
        h.runner.submit(sub_hold0, h.time(hold0_sent)).unwrap(),
        Status::Queued
    );
    assert_eq!(
        h.runner
            .session()
            .gateway()
            .intent_slot(key_hold0.intent)
            .map(|(c, _)| c),
        Some(0)
    );

    let step1_sent = hold0_sent + 100;
    let (sub_step1, key_step1, _) =
        h.make_new_submission(8, next_intent_id, seq8, "stepslot1", step1_sent);
    next_intent_id += 1;
    seq8 += 1;
    assert_eq!(
        h.runner.submit(sub_step1, h.time(step1_sent)).unwrap(),
        Status::Queued
    );
    assert_eq!(
        h.runner
            .session()
            .gateway()
            .intent_slot(key_step1.intent)
            .map(|(c, _)| c),
        Some(1)
    );

    now_ns = step1_sent;
    h.drive_until_flushed(&[key_hold0.intent, key_step1.intent], &mut now_ns);
    let held_on_0 = srv0.wait_held(Duration::from_secs(3));
    let held_on_1 = srv1.wait_held(Duration::from_secs(3));
    srv0.set_hold_mode(false);
    srv1.set_hold_mode(false);
    held_on_0
        .respond
        .send(ServerDirective::Reply {
            status: 200,
            headers: Vec::new(),
            body: ack_json(SpotVenue::Okx, "nathold0", "holdslot0"),
        })
        .unwrap();
    let _ = h.drive_n_completions(1, hold0_sent + 100_000);
    held_on_1
        .respond
        .send(ServerDirective::Reply {
            status: 200,
            headers: Vec::new(),
            body: ack_json(SpotVenue::Okx, "natstep1", "stepslot1"),
        })
        .unwrap();
    let _ = h.drive_n_completions(1, step1_sent + 108_000);
    now_ns = step1_sent + 108_000;

    let score0 = h
        .runner
        .session()
        .gateway()
        .slot_route(0)
        .unwrap()
        .health
        .score()
        .unwrap();
    let score1 = h
        .runner
        .session()
        .gateway()
        .slot_route(1)
        .unwrap()
        .health
        .score()
        .unwrap();
    assert!(
        score0 < score1,
        "expected score0 ({score0}) < score1 ({score1})"
    );
    assert!(
        u64::from(score1) * 8 <= u64::from(score0) * 9,
        "expected slot 1 ({score1}us) within 9/8 hysteresis of slot 0 ({score0}us)"
    );

    // Next order MUST stay sticky on slot 1 (within 9/8 hysteresis band), NOT flap to slot 0!
    now_ns += 1_000_000;
    let (sub_sticky, key_sticky, _) =
        h.make_new_submission(8, next_intent_id, seq8, "hyststicky", now_ns);
    next_intent_id += 1;
    seq8 += 1;
    assert_eq!(
        h.runner.submit(sub_sticky, h.time(now_ns)).unwrap(),
        Status::Queued
    );
    assert_eq!(
        h.runner
            .session()
            .gateway()
            .intent_slot(key_sticky.intent)
            .map(|(c, _)| c),
        Some(1),
        "router must stay sticky on slot 1 inside 9/8 hysteresis band"
    );
    // Complete `sub_sticky` with a degraded 500us sample so slot 1 exits the 9/8 band!
    let _ = h.drive_n_completions(1, now_ns + 500_000);
    now_ns += 1_000_000;

    let degraded1 = h
        .runner
        .session()
        .gateway()
        .slot_route(1)
        .unwrap()
        .health
        .score()
        .unwrap();
    assert!(
        u64::from(degraded1) * 8 > u64::from(score0) * 9,
        "expected degraded slot 1 ({degraded1}us) outside 9/8 band of slot 0 ({score0}us)"
    );

    // Next order immediately switches back to slot 0!
    let (sub_switch, key_switch, _) =
        h.make_new_submission(8, next_intent_id, seq8, "hystswitch", now_ns);
    next_intent_id += 1;
    seq8 += 1;
    assert_eq!(
        h.runner.submit(sub_switch, h.time(now_ns)).unwrap(),
        Status::Queued
    );
    assert_eq!(
        h.runner
            .session()
            .gateway()
            .intent_slot(key_switch.intent)
            .map(|(c, _)| c),
        Some(0),
        "router must switch back to slot 0 once slot 1 exceeds 9/8 hysteresis"
    );
    let _ = h.drive_n_completions(1, now_ns + 100_000);

    // 4. Mid-request single-link drop on slot 0 while slots 1 & 2 complete concurrently!
    srv0.set_hold_mode(true);
    srv1.set_hold_mode(true);
    srv2.set_hold_mode(true);

    now_ns += 2_000_000;
    let drop_sent_ns = now_ns;
    let (sub_d1, key_d1, _) =
        h.make_new_submission(8, next_intent_id, seq8, "dropord1", drop_sent_ns);
    next_intent_id += 1;
    seq8 += 1;
    let (sub_d2, key_d2, _) = h.make_new_submission(9, next_intent_id, 3, "sibord2", drop_sent_ns);
    next_intent_id += 1;
    let (sub_d3, key_d3, _) =
        h.make_new_submission(8, next_intent_id, seq8, "sibord3", drop_sent_ns);
    next_intent_id += 1;

    let t_drop = h.time(drop_sent_ns);
    assert_eq!(h.runner.submit(sub_d1, t_drop).unwrap(), Status::Queued);
    assert_eq!(h.runner.submit(sub_d2, t_drop).unwrap(), Status::Queued);
    assert_eq!(h.runner.submit(sub_d3, t_drop).unwrap(), Status::Queued);
    assert_eq!(
        h.runner
            .session()
            .gateway()
            .intent_slot(key_d1.intent)
            .map(|(c, _)| c),
        Some(0)
    );
    assert_eq!(
        h.runner
            .session()
            .gateway()
            .intent_slot(key_d2.intent)
            .map(|(c, _)| c),
        Some(1)
    );
    assert_eq!(
        h.runner
            .session()
            .gateway()
            .intent_slot(key_d3.intent)
            .map(|(c, _)| c),
        Some(2)
    );

    h.drive_until_flushed(&[key_d1.intent, key_d2.intent, key_d3.intent], &mut now_ns);
    let drop_req0 = srv0.wait_held(Duration::from_secs(3));
    let ok_req1 = srv1.wait_held(Duration::from_secs(3));
    let ok_req2 = srv2.wait_held(Duration::from_secs(3));

    srv0.set_hold_mode(false);
    srv1.set_hold_mode(false);
    srv2.set_hold_mode(false);

    // Drop server 0 mid-request while servers 1 & 2 reply 200 OK!
    drop_req0.respond.send(ServerDirective::DropSocket).unwrap();
    ok_req1
        .respond
        .send(ServerDirective::Reply {
            status: 200,
            headers: Vec::new(),
            body: ack_json(SpotVenue::Okx, "natsib2", "sibord2"),
        })
        .unwrap();
    ok_req2
        .respond
        .send(ServerDirective::Reply {
            status: 200,
            headers: Vec::new(),
            body: ack_json(SpotVenue::Okx, "natsib3", "sibord3"),
        })
        .unwrap();

    now_ns += 200_000;
    let completions = h.drive_n_completions(3, now_ns);
    assert!(!h.runner.halted());
    assert!(!h.runner.account().frozen());

    for comp in &completions {
        if comp.intent == key_d1.intent {
            assert_eq!(comp.response, Reply::Unknown);
        } else {
            assert!(matches!(comp.response, Reply::Acknowledged { .. }));
        }
    }
    assert_eq!(
        h.runner.account().order_in_doubt(&key_d1),
        Some(DoubtReason::PlaceAmbiguous)
    );
    assert_eq!(h.runner.account().order_in_doubt(&key_d2), None);
    assert_eq!(h.runner.account().order_in_doubt(&key_d3), None);
    assert!(h.runner.query_desk().unwrap().has_ask(&key_d1));
    assert_eq!(h.runner.query_desk().unwrap().waiting_count(), 1);

    // Immediately serve order 4 on healthy sibling slot 1/2 even before slot 0 reconnects!
    now_ns += 100_000;
    let (sub_d4, key_d4, _) = h.make_new_submission(9, next_intent_id, 4, "sibord4", now_ns);
    assert_eq!(
        h.runner.submit(sub_d4, h.time(now_ns)).unwrap(),
        Status::Queued
    );
    let slot_d4 = h
        .runner
        .session()
        .gateway()
        .intent_slot(key_d4.intent)
        .map(|(c, _)| c)
        .unwrap();
    assert!(matches!(slot_d4, 1 | 2));
    let comp4 = h.drive_n_completions(1, now_ns + 150_000);
    assert!(matches!(comp4[0].response, Reply::Acknowledged { .. }));

    // Meanwhile "tw-dialer" reconnects slot 0 AND PerOrderQueryDriver resolves `dropord1`!
    query_srv.push_reply(
        "/api/v5/trade/order",
        200,
        status_query_json(
            SpotVenue::Okx,
            "BTC-USDT",
            "natdrop1",
            "dropord1",
            "live",
            "0",
        ),
    );
    let cut = PrivateCut {
        stream: [7; 16],
        epoch: 1,
        gap_generation: 1,
        through: 10,
    };
    let recon_deadline = common::until();
    let mut saw_slot0_up = h.runner.session().gateway().has_slot_connection(0);
    let mut resolved_drop1 = false;
    now_ns += 5_000_000;
    while !(saw_slot0_up && resolved_drop1) {
        assert!(
            Instant::now() < recon_deadline,
            "timed out waiting for slot 0 dialer reconnect ({saw_slot0_up}) and QueryDesk resolution ({resolved_drop1})"
        );
        now_ns += 2_000_000;
        let out = h.driver.step(&mut h.runner, Some(cut), now_ns).unwrap();
        if out.reconnected_execution || h.runner.session().gateway().has_slot_connection(0) {
            saw_slot0_up = true;
        }
        if out
            .resolutions
            .iter()
            .any(|r| matches!(r, QueryResolution::Resolved { order } if *order == key_d1))
        {
            resolved_drop1 = true;
        }
        thread::sleep(Duration::from_millis(2));
    }
    assert!(h.runner.session().gateway().slot_route(0).unwrap().epoch >= 2);
    assert_eq!(h.runner.account().order_in_doubt(&key_d1), None);
}

#[test]
fn okx_and_bybit_place_cancel_query_quota_isolation() {
    // Part A: OKX (60/2s Symbol ORDER vs 60/2s Symbol CANCEL vs 20/2s REQUEST)
    {
        let tls = SharedTlsMaterial::new(SpotVenue::Okx);
        let srv = GatedTlsServer::start(&tls, Some(SpotVenue::Okx));
        let query_srv = GatedTlsServer::start(&tls, None);
        let mut h = PooledHarness::new(
            SpotVenue::Okx,
            &srv,
            &query_srv,
            RouterConfig::default(),
            QueryConfig {
                status_query_delay_ns: 0,
                place_retry_ns: 5_000_000,
                not_found_grace_ns: 50_000_000,
                max_query_count: 6,
                ..QueryConfig::default()
            },
        );
        let ip_key = IpKey::from_v4(Ipv4Addr::LOCALHOST);
        let limiter = VenueRateLimiter::new(VenuePreset::OkxSpot, 2, &[ip_key]).unwrap();
        h.runner.session_mut().set_venue_rate_limiter(limiter);
        for _ in 0..2 {
            let conn = srv.connect_from(Some(IpAddr::V4(Ipv4Addr::LOCALHOST)));
            h.runner
                .session_mut()
                .add_pool_slot(
                    OrderSlotSpec::with_connection(SendSlot::Exclusive, conn),
                    Limits::default(),
                )
                .unwrap();
        }

        let mut now_ns = 1_000_000_000u64;
        // Order #1: resting open order we will cancel after exhausting the 60/2s ORDER window.
        let (sub1, key1, uuid1) = h.make_new_submission(8, 1, 1, "okxrest1", now_ns);
        assert_eq!(
            h.runner.submit(sub1, h.time(now_ns)).unwrap(),
            Status::Queued
        );
        let _ = h.drive_n_completions(1, now_ns + 50_000);

        // Order #2: ambiguous response (OKX 50001) so it sits queued in QueryDesk!
        now_ns += 100_000;
        srv.push_reply(
            "/api/v5/trade/order",
            200,
            json!({ "code": "50001", "data": [] }).to_string(),
        );
        let (sub2, key2, _) = h.make_new_submission(8, 2, 2, "okxdoubt2", now_ns);
        assert_eq!(
            h.runner.submit(sub2, h.time(now_ns)).unwrap(),
            Status::Queued
        );
        let _ = h.drive_n_completions(1, now_ns + 50_000);
        assert_eq!(
            h.runner.account().order_in_doubt(&key2),
            Some(DoubtReason::PlaceAmbiguous)
        );
        assert!(h.runner.query_desk().unwrap().has_ask(&key2));

        // Orders #3..=#60: exhaust the remaining 58 slots of OKX's 60/2s per-symbol ORDER window!
        for i in 3..=60u64 {
            now_ns += 100_000;
            let cid = format!("okxfill{i}");
            let (sub, _, _) = h.make_new_submission(8, i, i, &cid, now_ns);
            assert_eq!(
                h.runner.submit(sub, h.time(now_ns)).unwrap(),
                Status::Queued
            );
            let _ = h.drive_n_completions(1, now_ns + 50_000);
        }

        let sym_key = symbol_rate_key(h.instrument_id);
        assert_ne!(sym_key, 0);

        // Snapshot Account & Journal before the 61st New order inside the same 2s window.
        let pos_before = h.runner.position();
        let orders_before = h.runner.account().order_keys().count();
        let usdt_key = h.runner.session().gateway().baseline().snapshot().assets[1].key;
        let reserved_before = h.runner.account().asset(usdt_key).unwrap().reserved;

        now_ns += 100_000;
        let (sub61, key61, _) = h.make_new_submission(8, 61, 61, "okxover61", now_ns);
        let err = h.runner.submit(sub61, h.time(now_ns)).unwrap_err();
        match err {
            RunnerError::Session(SessionError::RateLimited(RateReject::Exhausted {
                id,
                limit,
                ..
            })) => {
                assert_eq!(limit, 60);
                assert_eq!(id.symbol, sym_key);
                assert!(id.flags.contains(RateFlags::ORDER | RateFlags::PER_SYMBOL));
            }
            other => panic!("expected OKX 60/2s per-symbol RateLimited error, got {other:?}"),
        }
        // Zero Account hold change, zero journal append, and runner is NOT halted/frozen!
        assert_eq!(h.runner.position(), pos_before);
        assert_eq!(h.runner.account().order_keys().count(), orders_before);
        assert!(h.runner.account().order(&key61).is_none());
        assert_eq!(
            h.runner.account().asset(usdt_key).unwrap().reserved,
            reserved_before
        );
        assert!(!h.runner.halted());
        assert!(!h.runner.account().frozen());

        // `poll_strategy` also treats `SessionError::RateLimited` as a non-fatal refusal!
        now_ns += 100_000;
        let (sub62, _, _) = h.make_new_submission(9, 62, 1, "okxover62", now_ns);
        let mut one_shot = OneShotStrategy {
            pending: Some(sub62),
            last_err: None,
        };
        let healthy = Health {
            calibrated: true,
            market_live: true,
            private_receiving: true,
            account_frozen: false,
            sender_halted: false,
        };
        assert!(
            h.runner
                .poll_strategy(&mut one_shot, healthy, h.time(now_ns))
                .unwrap()
        );
        assert!(
            one_shot
                .last_err
                .as_deref()
                .is_some_and(|s| s.contains("RateLimited"))
        );
        assert!(!h.runner.halted());
        assert!(!h.runner.account().frozen());

        // IMMEDIATELY submit `Cancel` on active resting order #1 while `ORDER` cap is 60/60 exhausted!
        now_ns += 100_000;
        srv.push_reply(
            "/api/v5/trade/cancel-order",
            200,
            ack_json(SpotVenue::Okx, "nat-okxrest1", "okxrest1"),
        );
        let cancel_sub = h.make_cancel_submission(8, 70, 62, uuid1, "okxrest1", now_ns);
        assert_eq!(
            h.runner.submit(cancel_sub, h.time(now_ns)).unwrap(),
            Status::Queued,
            "OKX Cancel must succeed immediately despite exhausted 60/2s ORDER window"
        );
        let cancel_done = h.drive_n_completions(1, now_ns + 50_000);
        assert!(matches!(
            cancel_done[0].response,
            Reply::Acknowledged { .. }
        ));
        let _ = key1;

        // IMMEDIATELY run `PerOrderQueryDriver` status query for `key2` while `ORDER` cap is exhausted!
        query_srv.push_reply(
            "/api/v5/trade/order",
            200,
            status_query_json(
                SpotVenue::Okx,
                "BTC-USDT",
                "nat-okx2",
                "okxdoubt2",
                "live",
                "0",
            ),
        );
        let cut = PrivateCut {
            stream: [7; 16],
            epoch: 1,
            gap_generation: 1,
            through: 10,
        };
        let q_deadline = common::until();
        let mut resolved = false;
        while !resolved {
            assert!(Instant::now() < q_deadline, "OKX status query timeout");
            now_ns += 100_000;
            let step = h.driver.step(&mut h.runner, Some(cut), now_ns).unwrap();
            if step
                .resolutions
                .iter()
                .any(|r| matches!(r, QueryResolution::Resolved { order } if *order == key2))
            {
                resolved = true;
            }
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(h.runner.account().order_in_doubt(&key2), None);
    }

    // Part B: Bybit (20/1s ORDER vs 20/1s CANCEL vs 50/1s REQUEST, plus temporary REQUEST cap deferral)
    {
        let tls = SharedTlsMaterial::new(SpotVenue::Bybit);
        let srv = GatedTlsServer::start(&tls, Some(SpotVenue::Bybit));
        let query_srv = GatedTlsServer::start(&tls, None);
        let mut h = PooledHarness::new(
            SpotVenue::Bybit,
            &srv,
            &query_srv,
            RouterConfig::default(),
            QueryConfig {
                status_query_delay_ns: 0,
                place_retry_ns: 5_000_000,
                not_found_grace_ns: 50_000_000,
                max_query_count: 6,
                ..QueryConfig::default()
            },
        );
        let ip_key = IpKey::from_v4(Ipv4Addr::LOCALHOST);
        let limiter = VenueRateLimiter::new(VenuePreset::BybitSpot, 2, &[ip_key]).unwrap();
        h.runner.session_mut().set_venue_rate_limiter(limiter);
        for _ in 0..2 {
            let conn = srv.connect_from(Some(IpAddr::V4(Ipv4Addr::LOCALHOST)));
            h.runner
                .session_mut()
                .add_pool_slot(
                    OrderSlotSpec::with_connection(SendSlot::Exclusive, conn),
                    Limits::default(),
                )
                .unwrap();
        }

        let mut now_ns = 2_000_000_000u64;
        let (sub1, _, uuid1) = h.make_new_submission(8, 1, 1, "bybitrest1", now_ns);
        assert_eq!(
            h.runner.submit(sub1, h.time(now_ns)).unwrap(),
            Status::Queued
        );
        let _ = h.drive_n_completions(1, now_ns + 50_000);

        now_ns += 100_000;
        srv.push_reply(
            "/v5/order/create",
            200,
            json!({ "retCode": 10016, "result": {} }).to_string(),
        );
        let (sub2, key2, _) = h.make_new_submission(8, 2, 2, "bybitdoubt2", now_ns);
        assert_eq!(
            h.runner.submit(sub2, h.time(now_ns)).unwrap(),
            Status::Queued
        );
        let _ = h.drive_n_completions(1, now_ns + 50_000);
        assert_eq!(
            h.runner.account().order_in_doubt(&key2),
            Some(DoubtReason::PlaceAmbiguous)
        );

        for i in 3..=20u64 {
            now_ns += 100_000;
            let cid = format!("bybitfill{i}");
            let (sub, _, _) = h.make_new_submission(8, i, i, &cid, now_ns);
            assert_eq!(
                h.runner.submit(sub, h.time(now_ns)).unwrap(),
                Status::Queued
            );
            let _ = h.drive_n_completions(1, now_ns + 50_000);
        }

        // 21st Bybit New order inside 1s is rejected locally with zero Account hold or WAL write!
        let pos_before = h.runner.position();
        now_ns += 100_000;
        let (sub21, key21, _) = h.make_new_submission(8, 21, 21, "bybitover21", now_ns);
        let err = h.runner.submit(sub21, h.time(now_ns)).unwrap_err();
        match err {
            RunnerError::Session(SessionError::RateLimited(RateReject::Exhausted {
                id,
                limit,
                ..
            })) => {
                assert_eq!(limit, 20);
                assert!(id.flags.contains(RateFlags::ORDER));
            }
            other => panic!("expected Bybit 20/1s RateLimited error, got {other:?}"),
        }
        assert_eq!(h.runner.position(), pos_before);
        assert!(h.runner.account().order(&key21).is_none());

        // Cancel on resting order #1 succeeds immediately!
        now_ns += 100_000;
        srv.push_reply(
            "/v5/order/cancel",
            200,
            ack_json(SpotVenue::Bybit, "nat-bybit1", "bybitrest1"),
        );
        let cancel_sub = h.make_cancel_submission(8, 30, 21, uuid1, "bybitrest1", now_ns);
        assert_eq!(
            h.runner.submit(cancel_sub, h.time(now_ns)).unwrap(),
            Status::Queued
        );
        let cancel_done = h.drive_n_completions(1, now_ns + 50_000);
        assert!(matches!(
            cancel_done[0].response,
            Reply::Acknowledged { .. }
        ));

        // Verify temporary REQUEST cap exhaustion leaves QueryDesk ask queued with issued_query_count == 0,
        // and then succeeds as soon as REQUEST budget opens!
        for _ in 0..50 {
            h.runner
                .session_mut()
                .venue_rate_limiter_mut()
                .unwrap()
                .allow_query(ip_key, 1, now_ns)
                .unwrap();
        }
        let cut = PrivateCut {
            stream: [7; 16],
            epoch: 1,
            gap_generation: 1,
            through: 10,
        };
        let blocked_step = h
            .driver
            .step(&mut h.runner, Some(cut), now_ns + 1_000)
            .unwrap();
        assert_eq!(blocked_step.dispatched_tasks, 0);
        assert_eq!(
            h.runner.query_desk().unwrap().issued_query_count(&key2),
            Some(0),
            "temporary REQUEST quota deferral must not increment issued_query_count"
        );

        // Advance +1.1s past Bybit's 1s REQUEST rolling window -> query dispatches and resolves!
        query_srv.push_reply(
            "/v5/order/realtime",
            200,
            status_query_json(
                SpotVenue::Bybit,
                "BTCUSDT",
                "nat-bybit2",
                "bybitdoubt2",
                "New",
                "0",
            ),
        );
        now_ns += 1_100_000_000;
        let q_deadline = common::until();
        let mut resolved = false;
        while !resolved {
            assert!(Instant::now() < q_deadline, "Bybit status query timeout");
            now_ns += 100_000;
            let step = h.driver.step(&mut h.runner, Some(cut), now_ns).unwrap();
            if step
                .resolutions
                .iter()
                .any(|r| matches!(r, QueryResolution::Resolved { order } if *order == key2))
            {
                resolved = true;
            }
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(h.runner.account().order_in_doubt(&key2), None);
    }
}

#[test]
fn multi_source_ip_http_429_cooldown_failover() {
    let tls = SharedTlsMaterial::new(SpotVenue::Bybit);
    let srv0 = GatedTlsServer::start(&tls, None);
    let srv1 = GatedTlsServer::start(&tls, None);
    let query_srv = GatedTlsServer::start(&tls, None);

    let mut h = PooledHarness::new(
        SpotVenue::Bybit,
        &srv0,
        &query_srv,
        RouterConfig::default(),
        QueryConfig::default(),
    );

    let ip0 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let ip1 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
    let ip_key0 = IpKey::from_ip(ip0);
    let ip_key1 = IpKey::from_ip(ip1);
    let limiter = VenueRateLimiter::new(VenuePreset::BybitSpot, 2, &[ip_key0, ip_key1]).unwrap();
    h.runner.session_mut().set_venue_rate_limiter(limiter);

    let conn0 = srv0.connect_from(Some(ip0));
    let conn1 = srv1.connect_from(Some(ip1));
    let mut spec0 = OrderSlotSpec::with_connection(SendSlot::Exclusive, conn0);
    spec0.local_ip = Some(ip0);
    let mut spec1 = OrderSlotSpec::with_connection(SendSlot::Exclusive, conn1);
    spec1.local_ip = Some(ip1);
    assert_eq!(
        h.runner
            .session_mut()
            .add_pool_slot(spec0, Limits::default())
            .unwrap(),
        0
    );
    assert_eq!(
        h.runner
            .session_mut()
            .add_pool_slot(spec1, Limits::default())
            .unwrap(),
        1
    );

    // Order #1 routes over slot 0 (127.0.0.1), which returns HTTP 429 with Retry-After: 5!
    srv0.push_reply_with_headers(
        "/v5/order/create",
        429,
        vec![("Retry-After", "5")],
        json!({ "retCode": 10006, "result": {} }).to_string(),
    );
    let mut now_ns = 5_000_000_000u64;
    let (sub1, key1, _) = h.make_new_submission(8, 1, 1, "ipfailover1", now_ns);
    assert_eq!(
        h.runner.submit(sub1, h.time(now_ns)).unwrap(),
        Status::Queued
    );
    assert_eq!(
        h.runner
            .session()
            .gateway()
            .intent_slot(key1.intent)
            .map(|(c, _)| c),
        Some(0)
    );

    let done1 = h.drive_n_completions(1, now_ns + 100_000);
    assert_eq!(done1[0].intent, key1.intent);
    assert_eq!(done1[0].response, Reply::Unknown);

    // 1. Account is NOT frozen and NOT halted!
    assert!(!h.runner.account().frozen());
    assert!(!h.runner.halted());
    // 2. Only order #1 is isolated into QueryDesk (`PlaceAmbiguous`)!
    assert_eq!(
        h.runner.account().order_in_doubt(&key1),
        Some(DoubtReason::PlaceAmbiguous)
    );
    assert!(h.runner.query_desk().unwrap().has_ask(&key1));
    assert_eq!(h.runner.query_desk().unwrap().waiting_count(), 1);
    // 3. `127.0.0.1` is now in per-IP cooldown for 5s, while `127.0.0.2` is completely healthy!
    let sym_key = symbol_rate_key(h.instrument_id);
    assert!(matches!(
        h.runner
            .session()
            .venue_rate_limiter()
            .unwrap()
            .can_afford_place(ip_key0, sym_key, 1, now_ns + 200_000),
        Err(RateReject::IpCooldown { local_ip, .. }) if local_ip == ip_key0
    ));
    assert!(
        h.runner
            .session()
            .venue_rate_limiter()
            .unwrap()
            .can_afford_place(ip_key1, sym_key, 1, now_ns + 200_000)
            .is_ok()
    );

    // 4. Next `New` order immediately fails over to slot 1 (`127.0.0.2`) and succeeds!
    now_ns += 500_000;
    srv1.push_reply(
        "/v5/order/create",
        200,
        ack_json(SpotVenue::Bybit, "nat-ip2", "ipfailover2"),
    );
    let (sub2, key2, _) = h.make_new_submission(9, 2, 1, "ipfailover2", now_ns);
    assert_eq!(
        h.runner.submit(sub2, h.time(now_ns)).unwrap(),
        Status::Queued
    );
    assert_eq!(
        h.runner
            .session()
            .gateway()
            .intent_slot(key2.intent)
            .map(|(c, _)| c),
        Some(1),
        "must immediately route over healthy sibling IP 127.0.0.2 on slot 1"
    );
    let done2 = h.drive_n_completions(1, now_ns + 100_000);
    assert!(matches!(done2[0].response, Reply::Acknowledged { .. }));
    assert_eq!(srv1.seen.lock().unwrap()[0].1, ip1);

    // 5. Verify HTTP 401 / auth hazard STILL freezes `Account` immediately!
    now_ns += 500_000;
    srv1.push_reply(
        "/v5/order/create",
        401,
        json!({ "retCode": 10003, "result": {} }).to_string(),
    );
    let (sub3, _, _) = h.make_new_submission(9, 3, 2, "ipauth401", now_ns);
    assert_eq!(
        h.runner.submit(sub3, h.time(now_ns)).unwrap(),
        Status::Queued
    );
    let _ = h.drive_n_completions(1, now_ns + 100_000);
    assert!(
        h.runner.account().frozen(),
        "HTTP 401 / invalid API key hazard must still freeze Account immediately"
    );
}

#[test]
fn post_flush_eof_baton_guard_and_runner_inflight_origin_consistency() {
    let tls = SharedTlsMaterial::new(SpotVenue::Bybit);
    let srv0 = GatedTlsServer::start(&tls, None);
    let query_srv = GatedTlsServer::start(&tls, None);

    let clock_ns = Arc::new(AtomicU64::new(1_000_000_000));
    let clock_c = Arc::clone(&clock_ns);
    let dialer = Dialer::with_clock(Arc::new(move || clock_c.load(Ordering::SeqCst))).unwrap();

    let mut h = PooledHarness::new(
        SpotVenue::Bybit,
        &srv0,
        &query_srv,
        RouterConfig::default(),
        QueryConfig::default(),
    );
    h.runner.set_retention_window_ns(Some(1_000_000));

    // Spawn a one-shot HTTP server that replies 200 OK and immediately closes TCP (EOF),
    // while the slot's DialBaton reconnects to srv0!
    let eof_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let eof_addr = eof_listener.local_addr().unwrap();
    let srv_tls_eof = Arc::clone(&tls.server_cfg);
    let eof_th = thread::spawn(move || {
        let (sock, _) = eof_listener.accept().unwrap();
        let _ = sock.set_read_timeout(Some(Duration::from_secs(5)));
        let tls_conn = rustls::ServerConnection::new(srv_tls_eof).unwrap();
        let mut io = BufReader::new(rustls::StreamOwned::new(tls_conn, sock));
        let mut req_line = String::new();
        io.read_line(&mut req_line).unwrap();
        let mut content_len = 0usize;
        loop {
            let mut line = String::new();
            io.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            if let Some((k, v)) = line.split_once(':')
                && k.eq_ignore_ascii_case("content-length")
            {
                content_len = v.trim().parse().unwrap_or(0);
            }
        }
        let mut body_bytes = vec![0u8; content_len];
        if content_len > 0 {
            io.read_exact(&mut body_bytes).unwrap();
        }
        let ack = ack_json(SpotVenue::Bybit, "nat-eof-1", "runner-eof-1");
        let stream = io.get_mut();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{ack}",
            ack.len()
        )
        .unwrap();
        stream.flush().unwrap();
        stream.conn.send_close_notify();
        let _ = stream.flush();
    });

    let backoff = DialBackoff::new(0, 5);
    let baton = dialer
        .add_slot(
            0,
            tickwire_transport::dialer::DialSpec {
                host: tls.host.clone(),
                port: srv0.address.port(),
                initial_peers: vec![srv0.address],
                local_ip: None,
                security: DialSecurity::Tls {
                    server_name: ServerName::try_from(tls.host.clone()).unwrap(),
                    config: Arc::clone(&tls.client_tls),
                },
                limits: Limits::default(),
                transport: tickwire_transport::dialer::DialTransport::PlainOrTls,
                connect_timeout: Duration::from_secs(2),
                dial_timeout: Duration::from_secs(2),
                backoff,
            },
        )
        .unwrap();
    let eof_conn = Connection::connect_from(
        eof_addr,
        None,
        Security::Tls {
            server_name: ServerName::try_from(tls.host.clone()).unwrap(),
            config: Arc::clone(&tls.client_tls),
        },
        Limits::default(),
        common::until(),
    )
    .unwrap();
    let mut spec0 = OrderSlotSpec::with_connection(SendSlot::Exclusive, eof_conn);
    spec0.baton = Some(Arc::clone(&baton));
    spec0.backoff = DialBackoff::new(0, 5);
    assert_eq!(
        h.runner
            .session_mut()
            .add_pool_slot(spec0, Limits::default())
            .unwrap(),
        0
    );

    // 1. Submit runner-eof-1, flush, wait for server EOF + Ready DialBaton, then poll with small IoBudget!
    let mut now_ns = 1_000_000_000u64;
    let (sub_eof, key_eof, _) = h.make_new_submission(8, 1, 1, "runner-eof-1", now_ns);
    assert_eq!(
        h.runner.submit(sub_eof, h.time(now_ns)).unwrap(),
        Status::Queued
    );
    h.drive_until_flushed(&[key_eof.intent], &mut now_ns);
    eof_th.join().unwrap();

    let eof_wait = common::until();
    while h.runner.session().gateway().has_slot_connection(0) {
        assert!(Instant::now() < eof_wait);
        let _ =
            h.runner
                .session_mut()
                .gateway_mut()
                .poll_slot(0, Instant::now(), IoBudget::default());
        thread::sleep(Duration::from_millis(1));
    }
    let ready_wait = common::until();
    while baton.state() != tickwire_transport::dialer::DialState::Ready {
        assert!(Instant::now() < ready_wait);
        clock_ns.fetch_add(10_000_000, Ordering::SeqCst);
        let _ = baton.want(0);
        thread::sleep(Duration::from_millis(2));
    }

    let tiny = IoBudget {
        max_bytes: 40,
        max_syscalls: 8,
    };
    let mut got = None;
    let poll_dl = common::until();
    while got.is_none() {
        assert!(Instant::now() < poll_dl);
        now_ns += 1_000;
        let t = h.time(now_ns);
        got = h.runner.poll(&mut h.strategy, t, tiny).unwrap();
    }
    assert!(matches!(got.unwrap().response, Reply::Acknowledged { .. }));
    assert_eq!(h.runner.account().order_in_doubt(&key_eof), None);
    assert!(h.runner.session().gateway().has_slot_connection(0));
    assert_eq!(h.runner.inflight_count(), 0);

    // 2. Direct gateway abort (terminate_unsent_strategy) immediately frees AccountRunner availability & inflight!
    now_ns += 10_000_000;
    let (sub_abort, key_abort, _) = h.make_new_submission(8, 2, 2, "runner-abort-2", now_ns);
    let sub_abort_retry = Submission {
        command: sub_abort.command.clone(),
        order_ids: sub_abort.order_ids.clone(),
        deadline: sub_abort.deadline,
        recovery: make_recovery_bundle(8, 2, sub_abort.command.digest()),
    };
    assert_eq!(
        h.runner.submit(sub_abort, h.time(now_ns)).unwrap(),
        Status::Queued
    );
    assert_eq!(h.runner.inflight_count(), 1);
    assert!(
        h.runner
            .session_mut()
            .gateway_mut()
            .terminate_unsent_strategy(
                8,
                tickwire_account::ManagerIdentity {
                    instance: [8; 16],
                    generation: 5,
                },
            )
            .unwrap()
    );
    assert_eq!(
        h.runner.session().gateway().status(key_abort.intent),
        Some(Status::Aborted)
    );
    assert!(
        h.runner.available(),
        "runner must report available() immediately after gateway aborts sole in-flight order"
    );
    assert_eq!(
        h.runner.inflight_count(),
        0,
        "inflight_count() must ignore already-Aborted intents"
    );

    // Byte-identical retry of aborted intent past retention window returns Status::Aborted cleanly!
    now_ns += 5_000_000;
    assert_eq!(
        h.runner.submit(sub_abort_retry, h.time(now_ns)).unwrap(),
        Status::Aborted
    );

    // Subsequent order submits and completes cleanly on slot 0!
    now_ns += 1_000_000;
    srv0.push_reply(
        "/v5/order/create",
        200,
        ack_json(SpotVenue::Bybit, "nat-ok-3", "runner-ok-3"),
    );
    let (sub_ok, _, _) = h.make_new_submission(8, 3, 3, "runner-ok-3", now_ns);
    assert_eq!(
        h.runner.submit(sub_ok, h.time(now_ns)).unwrap(),
        Status::Queued
    );
    let done_ok = h.drive_n_completions(1, now_ns + 50_000);
    assert!(matches!(done_ok[0].response, Reply::Acknowledged { .. }));
}
