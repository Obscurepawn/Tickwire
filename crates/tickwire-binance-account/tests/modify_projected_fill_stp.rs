#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod gateway;
#[path = "../../tickwire-binance-private/tests/common/mod.rs"]
mod network;

use base64::Engine as _;
use sha1::{Digest as _, Sha1};
use std::{
    collections::{BTreeMap, VecDeque},
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpListener},
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_account::{
    Admission, AskKind, DoubtReason, Fixed, IntentKey, OrderKey, QueryConfig, QueryResolution,
};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::{
    AccountGateway, AdmissionRequest, Authority, AuthorityError, CommitState, ConnectionConfig,
    EncodedRequest, Encoder, Gate, Status,
};
use tickwire_account_journal::{AppendReceipt, FileBackend};
use tickwire_binance::{
    auth::SigningKey,
    classify_order_update_reason,
    events::{PrivateBody, PrivateStream, SessionContext, SessionIdentity, decode_private},
    feed::TimeUnit,
    request::{self, Credentials, Market, RequestOptions, Timestamp, Transport},
};
use tickwire_binance_account::{
    AccountIngress, Config, PerOrderQueryConfig, PerOrderQueryDriver, PerOrderQueryPermit,
    QueryOperation,
    rest::{RestConfig, RestDisposition, RestOutcome, RestOwner},
    ws::{
        ControlAction, DurableWsBinding, DurableWsSession, SessionState, SignedWire, WsConfig,
        WsRequestPlan,
    },
};
use tickwire_binance_private as private;
use tickwire_clock::{ClockDomain, Source, TimeUnit as ClockTimeUnit};
use tickwire_journal::store::FileStore;
use tickwire_quota::rate_limiter::{IpKey, VenuePreset, VenueRateLimiter};
use tickwire_transport::{
    Connection, IoBudget, Limits, Security, http, rustls,
    ws::{Opcode, encode_client},
};
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
    fn spawn(server_cfg: Arc<rustls::ServerConfig>) -> Self {
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
            let mut conn_threads = Vec::new();
            while !stop_w.load(Ordering::Acquire) {
                let socket = match listener.accept() {
                    Ok((s, _)) => s,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                let server_cfg_c = Arc::clone(&server_cfg);
                let steps_c = Arc::clone(&steps_w);
                let reqs_c = Arc::clone(&reqs_w);
                let stop_c = Arc::clone(&stop_w);
                conn_threads.push(thread::spawn(move || {
                    socket.set_nonblocking(false).unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_millis(25)))
                        .unwrap();
                    socket
                        .set_write_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    let conn = match rustls::ServerConnection::new(server_cfg_c) {
                        Ok(c) => c,
                        Err(_) => return,
                    };
                    let mut stream = rustls::StreamOwned::new(conn, socket);
                    let mut buf = Vec::new();
                    while !stop_c.load(Ordering::Acquire) {
                        let mut byte = [0u8; 1];
                        match stream.read(&mut byte) {
                            Ok(1) => {
                                buf.push(byte[0]);
                                if !buf.ends_with(b"\r\n\r\n") {
                                    continue;
                                }
                                let head = String::from_utf8_lossy(&buf).into_owned();
                                buf.clear();
                                let content_len = head
                                    .lines()
                                    .find_map(|line| {
                                        let (k, v) = line.split_once(':')?;
                                        k.trim()
                                            .eq_ignore_ascii_case("content-length")
                                            .then(|| v.trim().parse::<usize>().ok())?
                                    })
                                    .unwrap_or(0);
                                if content_len > 0 {
                                    let mut req_body = vec![0u8; content_len];
                                    let mut got = 0usize;
                                    while got < content_len && !stop_c.load(Ordering::Acquire) {
                                        match stream.read(&mut req_body[got..]) {
                                            Ok(0) => break,
                                            Ok(n) => got += n,
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
                                    }
                                }
                                let first_line = head.lines().next().unwrap_or("").to_owned();
                                reqs_c.lock().unwrap().push(first_line);
                                let action = loop {
                                    if stop_c.load(Ordering::Acquire) {
                                        break None;
                                    }
                                    if let Some(step) = steps_c.lock().unwrap().pop_front() {
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
                                            409 => "Conflict",
                                            429 => "Too Many Requests",
                                            503 => "Service Unavailable",
                                            _ => "Response",
                                        };
                                        let conn_hdr =
                                            if close_after { "close" } else { "keep-alive" };
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
                }));
            }
            for th in conn_threads {
                let _ = th.join();
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

fn test_certs() -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let cert = rcgen::generate_simple_self_signed(vec![
        "demo-fapi.binance.com".into(),
        "demo-fstream.binance.com".into(),
        "testnet.binancefuture.com".into(),
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

struct RealRestEncoder {
    market: Market,
    host_override: &'static str,
}

impl Encoder for RealRestEncoder {
    fn encode(&mut self, c: &v::CanonicalCommand) -> tickwire_gateway::Result<EncodedRequest> {
        let key = SigningKey::hmac(network::SECRET).unwrap();
        let creds = Credentials {
            account: 2,
            environment: v::Environment::Test,
            market: self.market,
            api_key: network::KEY,
            signing_key: &key,
        };
        let prepared = request::prepare(
            c,
            &creds,
            RequestOptions {
                transport: Transport::Rest,
                timestamp: Timestamp::Milliseconds(1_770_000_000_000),
                recv_window_micros: 5_000_000,
                request_id: "rest",
            },
            request::MAX_REQUEST_BYTES,
        )
        .map_err(|_| tickwire_gateway::Error::Encoding)?;
        let bytes = http::encode_request(
            prepared.method().as_str(),
            prepared.path(),
            &[
                ("Host", self.host_override),
                (
                    "X-MBX-APIKEY",
                    prepared
                        .api_key_header()
                        .ok_or(tickwire_gateway::Error::Encoding)?,
                ),
            ],
            prepared.payload(),
            16_384,
        )
        .map_err(|_| tickwire_gateway::Error::Encoding)?;
        EncodedRequest::new(c.digest(), self.host_override.into(), bytes)
    }
}

struct RealWsShared {
    plans: BTreeMap<v::Hash, WsRequestPlan>,
    wires: BTreeMap<v::Hash, SignedWire>,
    bindings: BTreeMap<v::Hash, DurableWsBinding>,
    frames: BTreeMap<v::Hash, Vec<u8>>,
    discarded: Vec<v::Hash>,
}

#[derive(Clone)]
struct RealWsAdapter {
    inner: Rc<std::cell::RefCell<RealWsShared>>,
    mock_auth: gateway::MockAuthority,
    market: Market,
    authority_host: &'static str,
}

impl RealWsAdapter {
    fn new(mock_auth: gateway::MockAuthority, market: Market) -> Self {
        let authority_host = match market {
            Market::Spot => "ws-api.testnet.binance.vision",
            Market::Usdm => "testnet.binancefuture.com",
        };
        Self {
            inner: Rc::new(std::cell::RefCell::new(RealWsShared {
                plans: BTreeMap::new(),
                wires: BTreeMap::new(),
                bindings: BTreeMap::new(),
                frames: BTreeMap::new(),
                discarded: Vec::new(),
            })),
            mock_auth,
            market,
            authority_host,
        }
    }

    fn discarded_digests(&self) -> Vec<v::Hash> {
        self.inner.borrow().discarded.clone()
    }
}

impl SessionState for RealWsAdapter {
    fn prepare(&mut self, plan: WsRequestPlan) -> tickwire_binance_account::Result<()> {
        self.inner
            .borrow_mut()
            .plans
            .insert(plan.command().digest(), plan);
        Ok(())
    }

    fn discard_uncommitted(&mut self, digest: v::Hash) {
        let mut st = self.inner.borrow_mut();
        if !st.bindings.contains_key(&digest) {
            st.plans.remove(&digest);
            st.wires.remove(&digest);
            st.frames.remove(&digest);
            st.discarded.push(digest);
        }
    }

    fn signed_wire(&self, digest: v::Hash) -> tickwire_binance_account::Result<SignedWire> {
        self.inner
            .borrow()
            .wires
            .get(&digest)
            .cloned()
            .ok_or(tickwire_binance_account::Error::Identity)
    }

    fn install(&mut self, binding: DurableWsBinding) -> tickwire_binance_account::Result<()> {
        self.inner
            .borrow_mut()
            .bindings
            .insert(binding.command_digest(), binding);
        Ok(())
    }

    fn feedback(&mut self, _status: u16) -> tickwire_binance_account::Result<()> {
        Ok(())
    }

    fn control(
        &mut self,
        session: &DurableWsSession,
        _action: ControlAction,
        _operation: u64,
        _deadline: u64,
    ) -> bool {
        session.is_current()
    }
}

impl Encoder for RealWsAdapter {
    fn encode(&mut self, c: &v::CanonicalCommand) -> tickwire_gateway::Result<EncodedRequest> {
        let digest = c.digest();
        let mut st = self.inner.borrow_mut();
        if let Some(frame) = st.frames.get(&digest) {
            return EncodedRequest::new(digest, self.authority_host.into(), frame.clone());
        }
        let plan = st
            .plans
            .get(&digest)
            .cloned()
            .ok_or(tickwire_gateway::Error::Encoding)?;
        let key = SigningKey::hmac(network::SECRET).unwrap();
        let creds = Credentials {
            account: 2,
            environment: v::Environment::Test,
            market: self.market,
            api_key: network::KEY,
            signing_key: &key,
        };
        let prepared = request::prepare(
            c,
            &creds,
            RequestOptions {
                transport: Transport::WebSocket,
                timestamp: Timestamp::Milliseconds(1_770_000_000_000),
                recv_window_micros: 5_000_000,
                request_id: plan.request_id().as_str(),
            },
            request::MAX_REQUEST_BYTES,
        )
        .map_err(|_| tickwire_gateway::Error::Encoding)?;
        let payload = prepared.payload().to_vec();
        let frame = encode_client(Opcode::Text, &payload, 16_384)
            .map_err(|_| tickwire_gateway::Error::Encoding)?;
        let req_hash: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, &payload)
            .as_ref()
            .try_into()
            .unwrap();
        let frame_hash: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, &frame)
            .as_ref()
            .try_into()
            .unwrap();
        let source = match self.market {
            Market::Spot => Source::SpotTest,
            Market::Usdm => Source::UsdmTest,
        };
        let wire = SignedWire {
            source,
            domain: ClockDomain::new([5; 16], 1).unwrap(),
            timestamp: 1_770_000_000_000,
            unit: ClockTimeUnit::Milliseconds,
            window_ns: 5_000_000_000,
            request_hash: req_hash,
            frame_hash,
            request_bytes: payload.len() as u32,
            frame_bytes: frame.len() as u32,
        };
        st.wires.insert(digest, wire);
        st.frames.insert(digest, frame.clone());
        EncodedRequest::new(digest, self.authority_host.into(), frame)
    }
}

impl Authority for RealWsAdapter {
    fn prepare(
        &mut self,
        r: AdmissionRequest<'_>,
    ) -> std::result::Result<tickwire_recovery::VerifiedClosure, AuthorityError> {
        self.mock_auth.prepare(r)
    }

    fn verify(
        &mut self,
        r: AdmissionRequest<'_>,
    ) -> std::result::Result<tickwire_recovery::VerifiedClosure, AuthorityError> {
        self.mock_auth.verify(r)
    }

    fn commit_state(
        &mut self,
        admission: &Admission,
        receipt: &AppendReceipt,
    ) -> std::result::Result<CommitState, AuthorityError> {
        let st = self.inner.borrow();
        let Some(bound) = st.bindings.get(&admission.command_digest()) else {
            return Ok(CommitState::Pending);
        };
        if !bound.is_current() || bound.account_sequence() != receipt.sequence() {
            return Err(AuthorityError::Denied);
        }
        Ok(CommitState::Ready)
    }

    fn current_gate(
        &mut self,
        c: &v::CanonicalCommand,
        a: &Admission,
    ) -> std::result::Result<Gate, AuthorityError> {
        let st = self.inner.borrow();
        let Some(bound) = st.bindings.get(&c.digest()) else {
            return Err(AuthorityError::Denied);
        };
        if !bound.is_current() {
            return Err(AuthorityError::Denied);
        }
        self.mock_auth.current_gate(c, a)
    }
}

fn read_masked_ws_text(stream: &mut impl Read) -> serde_json::Value {
    let mut head = [0u8; 2];
    stream.read_exact(&mut head).unwrap();
    assert_eq!(head[0], 0x81, "expected FIN Text opcode");
    assert_ne!(head[1] & 0x80, 0, "expected client masked frame");
    let len = match head[1] & 0x7f {
        126 => {
            let mut ext = [0u8; 2];
            stream.read_exact(&mut ext).unwrap();
            u16::from_be_bytes(ext) as usize
        }
        127 => {
            let mut ext = [0u8; 8];
            stream.read_exact(&mut ext).unwrap();
            u64::from_be_bytes(ext) as usize
        }
        n => n as usize,
    };
    let mut mask = [0u8; 4];
    stream.read_exact(&mut mask).unwrap();
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).unwrap();
    for (i, b) in payload.iter_mut().enumerate() {
        *b ^= mask[i % 4];
    }
    serde_json::from_slice(&payload).unwrap()
}

fn write_server_ws_text(stream: &mut impl Write, value: &serde_json::Value) {
    let bytes = serde_json::to_vec(value).unwrap();
    if bytes.len() < 126 {
        stream.write_all(&[0x81, bytes.len() as u8]).unwrap();
    } else {
        stream.write_all(&[0x81, 126]).unwrap();
        stream
            .write_all(&(bytes.len() as u16).to_be_bytes())
            .unwrap();
    }
    stream.write_all(&bytes).unwrap();
}

fn complete_ws_upgrade(stream: &mut (impl Read + Write)) {
    let mut req = Vec::new();
    while !req.ends_with(b"\r\n\r\n") {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b).unwrap();
        req.push(b[0]);
    }
    let text = String::from_utf8(req).unwrap();
    let ws_key = text
        .lines()
        .find_map(|line| {
            line.split_once(':').and_then(|(k, v)| {
                k.trim()
                    .eq_ignore_ascii_case("sec-websocket-key")
                    .then_some(v.trim())
            })
        })
        .unwrap();
    let mut sha = Sha1::new();
    sha.update(ws_key.as_bytes());
    sha.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let accept = base64::engine::general_purpose::STANDARD.encode(sha.finalize());
    let resp = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    stream.write_all(resp.as_bytes()).unwrap();
    stream.flush().unwrap();
}

struct ControllableClock {
    now_ns: Arc<AtomicU64>,
    market: Market,
}

impl ControllableClock {
    fn new(initial_ns: u64, market: Market) -> Self {
        Self {
            now_ns: Arc::new(AtomicU64::new(initial_ns)),
            market,
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
        let unix_ms = 1_770_000_000_000 + ns / 1_000_000;
        let source = match self.market {
            Market::Spot => Source::SpotTest,
            Market::Usdm => Source::UsdmTest,
        };
        let mono = private::MonoTime::new(network::test_domain(), ns);
        let mut cal = network::calibrated(network::test_domain(), source, ns, unix_ms);
        Ok(private::ClockSample {
            instant: Instant::now(),
            monotonic: mono,
            signing_time: Some(cal.sign_time(mono, ClockTimeUnit::Milliseconds).unwrap()),
        })
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

fn build_usdm_baseline() -> (CanonicalBaseline, v::Command) {
    let (mut snapshot, mut catalog, base_cmd) = gateway::fixture::futures_fixture();
    snapshot.valid_until = u64::MAX / 2;
    catalog.entries[0]
        .strategy_limits
        .push((9, gateway::fixture::units(100)));
    (CanonicalBaseline::new(snapshot, catalog).unwrap(), base_cmd)
}

fn build_spot_baseline() -> (CanonicalBaseline, v::Command) {
    let (mut snapshot, mut catalog, base_cmd) = gateway::fixture::fixture();
    snapshot.valid_until = u64::MAX / 2;
    snapshot.assets[1].free = gateway::fixture::units(100_000);
    catalog.entries[0]
        .strategy_limits
        .push((9, gateway::fixture::units(100)));
    (CanonicalBaseline::new(snapshot, catalog).unwrap(), base_cmd)
}

fn ingress_config(baseline: &CanonicalBaseline) -> Config {
    Config {
        physical: tickwire_journal::Config::new([88; 16], [1; 16]),
        writer_epoch: 4,
        baseline: baseline.clone(),
        account_stream: [2; 16],
        max_records: 128,
        income_association_authority: None,
        income_baseline_authority: None,
        max_registrations: 64,
        max_raw_bytes: 32_768,
        max_retained_bytes: 1 << 20,
    }
}

#[allow(clippy::too_many_arguments)]
fn make_new_cmd(
    base_cmd: &v::Command,
    strategy_id: u64,
    instance_id: u8,
    intent_id: u64,
    strategy_seq: u64,
    client_id: &str,
    side: v::Side,
    qty_str: &str,
    price_str: &str,
    tif: v::TimeInForce,
    now_ns: u64,
) -> (v::CanonicalCommand, OrderKey, [u8; 16]) {
    let mut cmd = base_cmd.clone();
    cmd.context.strategy.logical_strategy_id = strategy_id;
    cmd.context.strategy.instance_id = [instance_id; 16];
    cmd.context.strategy.strategy_intent_seq = strategy_seq;
    cmd.context.strategy.created_mono_ns = now_ns;
    cmd.context.request.intent_id = intent_id;
    cmd.context.request.recovery_object = intent_id;
    cmd.context.request.deadline = now_ns + 60_000_000_000;
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
    spec.side = side;
    spec.quantity = v::Quantity::Base(qty_str.parse().unwrap());
    spec.price = Some(price_str.parse().unwrap());
    spec.time_in_force = Some(tif);
    cmd.body = v::CommandBody::New(spec);
    let exec_epoch = cmd.context.request.intent_exec_epoch;
    let canonical = v::CanonicalCommand::new(cmd).unwrap();
    let mut local_oid = [0u8; 16];
    local_oid[..8].copy_from_slice(&intent_id.to_le_bytes());
    local_oid[8] = instance_id;
    (
        canonical,
        OrderKey {
            intent: IntentKey {
                epoch: exec_epoch,
                intent_id,
            },
            client_id: cid,
        },
        local_oid,
    )
}

#[allow(clippy::too_many_arguments)]
fn make_modify_cmd(
    base_cmd: &v::Command,
    strategy_id: u64,
    instance_id: u8,
    intent_id: u64,
    strategy_seq: u64,
    target_order_id: [u8; 16],
    orig_client_id: &str,
    venue_order_id: Option<u64>,
    new_client_id: Option<&str>,
    side: v::Side,
    qty_str: &str,
    price_str: &str,
    now_ns: u64,
) -> v::CanonicalCommand {
    let mut cmd = base_cmd.clone();
    cmd.context.strategy.logical_strategy_id = strategy_id;
    cmd.context.strategy.instance_id = [instance_id; 16];
    cmd.context.strategy.strategy_intent_seq = strategy_seq;
    cmd.context.strategy.created_mono_ns = now_ns;
    cmd.context.request.intent_id = intent_id;
    cmd.context.request.recovery_object = intent_id;
    cmd.context.request.deadline = now_ns + 60_000_000_000;
    let mut triggers = cmd.context.triggers.as_slice().to_vec();
    for t in &mut triggers {
        t.sequence = intent_id;
        t.observed_mono_ns = now_ns.saturating_sub(1);
    }
    cmd.context.triggers = v::BoundedList::new(triggers).unwrap();
    let orig_epoch = cmd.context.request.intent_exec_epoch;
    cmd.body = v::CommandBody::Modify(v::ModifyOrder {
        target: v::OrderTarget {
            order_id: target_order_id,
            expected_owner_revision: 1,
            original_intent_exec_epoch: orig_epoch,
            client_id: Some(v::ClientId::new(orig_client_id).unwrap()),
            venue_order_id: venue_order_id.map(|id| v::VenueOrderId::new(id.to_string()).unwrap()),
        },
        new_client_id: new_client_id.map(|s| v::ClientId::new(s).unwrap()),
        side,
        quantity: qty_str.parse().unwrap(),
        price: price_str.parse().unwrap(),
        position_side: v::PositionSide::Both,
        reduce_only: false,
    });
    v::CanonicalCommand::new(cmd).unwrap()
}

#[allow(clippy::too_many_arguments)]
fn make_cancel_cmd(
    base_cmd: &v::Command,
    strategy_id: u64,
    instance_id: u8,
    intent_id: u64,
    strategy_seq: u64,
    target_order_id: [u8; 16],
    orig_client_id: &str,
    venue_order_id: u64,
    now_ns: u64,
) -> v::CanonicalCommand {
    let mut cmd = base_cmd.clone();
    cmd.context.strategy.logical_strategy_id = strategy_id;
    cmd.context.strategy.instance_id = [instance_id; 16];
    cmd.context.strategy.strategy_intent_seq = strategy_seq;
    cmd.context.strategy.created_mono_ns = now_ns;
    cmd.context.request.intent_id = intent_id;
    cmd.context.request.recovery_object = intent_id;
    cmd.context.request.deadline = now_ns + 60_000_000_000;
    let mut triggers = cmd.context.triggers.as_slice().to_vec();
    for t in &mut triggers {
        t.sequence = intent_id;
        t.observed_mono_ns = now_ns.saturating_sub(1);
    }
    cmd.context.triggers = v::BoundedList::new(triggers).unwrap();
    let orig_epoch = cmd.context.request.intent_exec_epoch;
    cmd.body = v::CommandBody::Cancel(v::OrderTarget {
        order_id: target_order_id,
        expected_owner_revision: 1,
        original_intent_exec_epoch: orig_epoch,
        client_id: Some(v::ClientId::new(orig_client_id).unwrap()),
        venue_order_id: Some(v::VenueOrderId::new(venue_order_id.to_string()).unwrap()),
    });
    v::CanonicalCommand::new(cmd).unwrap()
}

fn drive_rest_outcome<E: Encoder>(
    owner: &mut RestOwner<FileStore, FileBackend, gateway::MockAuthority, E>,
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

fn drive_query_resolution<E: Encoder>(
    driver: &mut PerOrderQueryDriver,
    owner: &mut RestOwner<FileStore, FileBackend, gateway::MockAuthority, E>,
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

fn deliver_private_frames<A: Authority, E: Encoder>(
    owner: &mut RestOwner<FileStore, FileBackend, A, E>,
    market: Market,
    frames: Vec<Vec<u8>>,
    clock: &mut ControllableClock,
    ws_tls: Arc<rustls::ClientConfig>,
    ws_server_tls: Arc<rustls::ServerConfig>,
) {
    let ws_listener = network::listener();
    let ws_addr = ws_listener.local_addr().unwrap();
    let expected_count = frames.len();
    match market {
        Market::Spot => {
            let ws_thread = thread::spawn(move || {
                let mut s = network::accept(&ws_listener, ws_server_tls);
                network::upgrade(&mut s, "/ws-api/v3");
                let req = network::json(&mut s);
                network::verify_signature(&req, 0);
                network::reply(&mut s, &req["id"], serde_json::json!({"subscriptionId": 0}));
                for frame in &frames {
                    network::frame(&mut s, 1, frame);
                }
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
            let mut delivered = 0usize;
            let deadline = gateway::until();
            while delivered < expected_count {
                assert!(
                    Instant::now() < deadline,
                    "timed out receiving Spot private frames"
                );
                ws_client
                    .poll(
                        clock,
                        private::PollBudget::default(),
                        &mut |_: &private::Charge, _: &private::ClockSample| true,
                        |d| {
                            if let private::Delivery::Event(ev) = d {
                                let out = owner.ingest_private(&ev).unwrap();
                                assert!(
                                    matches!(
                                        out,
                                        tickwire_binance_account::Outcome::Applied { .. }
                                    ),
                                    "Spot private frame {delivered} not Applied: {out:?}"
                                );
                                delivered += 1;
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
        }
        Market::Usdm => {
            let mgmt_listener = network::listener();
            let mgmt_addr = mgmt_listener.local_addr().unwrap();
            let mgmt_tls = Arc::clone(&ws_server_tls);
            let mgmt_thread = thread::spawn(move || {
                let mut m = network::accept(&mgmt_listener, mgmt_tls);
                let _ = network::headers(&mut m);
                let body = br#"{"listenKey":"lk-usdm-test-12345"}"#;
                write!(
                    m,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .unwrap();
                m.write_all(body).unwrap();
                m.flush().unwrap();
            });
            let ws_thread = thread::spawn(move || {
                let mut s = network::accept(&ws_listener, ws_server_tls);
                network::upgrade(&mut s, "/private/ws/lk-usdm-test-12345");
                for frame in &frames {
                    network::frame(&mut s, 1, frame);
                }
                let _ = s.read(&mut [0]);
            });
            let mut ws_client = private::PrivateClient::new(
                private::SessionConfig::new(network::scope(Market::Usdm)),
                private::Authentication::UsdmApiKey {
                    api_key: network::KEY,
                },
                private::NetworkConfig {
                    stream_address: ws_addr,
                    management_address: Some(mgmt_addr),
                    tls: ws_tls,
                },
                5,
            )
            .unwrap();
            let mut delivered = 0usize;
            let deadline = gateway::until();
            while delivered < expected_count {
                assert!(
                    Instant::now() < deadline,
                    "timed out receiving USDM private frames"
                );
                ws_client
                    .poll(
                        clock,
                        private::PollBudget::default(),
                        &mut |_: &private::Charge, _: &private::ClockSample| true,
                        |d| {
                            if let private::Delivery::Event(ev) = d {
                                let out = owner.ingest_private(&ev).unwrap();
                                assert!(
                                    matches!(
                                        out,
                                        tickwire_binance_account::Outcome::Applied { .. }
                                    ),
                                    "Spot private frame {delivered} not Applied: {out:?}"
                                );
                                delivered += 1;
                            }
                            Ok(())
                        },
                    )
                    .unwrap();
                thread::yield_now();
            }
            ws_client.stop(private::StopMode::Disconnect).unwrap();
            drop(ws_client);
            mgmt_thread.join().unwrap();
            ws_thread.join().unwrap();
        }
    }
}

fn usdm_order_reply(
    client_id: &str,
    order_id: u64,
    status: &str,
    orig_qty: &str,
    price: &str,
    exec_qty: &str,
    avg_price: &str,
) -> serde_json::Value {
    serde_json::json!({
        "symbol": "BTCUSDT",
        "orderId": order_id,
        "clientOrderId": client_id,
        "price": price,
        "avgPrice": avg_price,
        "origQty": orig_qty,
        "executedQty": exec_qty,
        "cumQuote": "0",
        "status": status,
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "positionSide": "BOTH",
        "updateTime": 1_770_000_000_100u64
    })
}

#[test]
fn usdm_rest_and_ws_modify_roundtrip_applies_venue_clamped_terms_releases_worst_case_margin_and_recovers()
 {
    let (client_tls, server_tls) = test_certs();

    // --- Part A: USDM REST PUT /fapi/v1/order + Private-Stream AMENDMENT + WAL Recovery ---
    {
        let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));
        let account_dir = gateway::Directory::new();
        let inbox_dir = gateway::Directory::new();
        let (baseline, base_cmd) = build_usdm_baseline();
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
            RealRestEncoder {
                market: Market::Usdm,
                host_override: "demo-fapi.binance.com",
            },
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
        let mut clock = ControllableClock::new(1_000_000, Market::Usdm);

        // 1. Place BUY 2.0 BTC @ 100 (10x leverage => initial margin hold = 20 USDT, shadow = 980).
        order_server.push_json(
            200,
            usdm_order_reply("usdm-r-1", 9001, "NEW", "2", "100", "0", "0"),
        );
        let now_1 = clock.now_ns();
        auth.policy.lock().unwrap().gate.now = now_1;
        let (cmd_new, order_key, local_oid) = make_new_cmd(
            &base_cmd,
            8,
            4,
            1,
            1,
            "usdm-r-1",
            v::Side::Buy,
            "2",
            "100",
            v::TimeInForce::Gtc,
            now_1,
        );
        owner
            .submit(cmd_new, &[local_oid], now_1, gateway::until())
            .unwrap();
        let out_new = drive_rest_outcome(&mut owner, &clock);
        assert_eq!(out_new.disposition, RestDisposition::Observed);
        assert_eq!(
            owner.account().venue_shadow(1).unwrap(),
            gateway::fixture::units(980)
        );

        // 2. Submit Modify -> BUY 5.0 BTC @ 120 (worst-case margin + open-loss = 150 USDT => shadow = 850 while in flight).
        // Venue clamps price to 110 and origQty to 4.0 (applied hold = 80 USDT => releases 70 USDT excess hold -> shadow = 920!).
        order_server.push_json(
            200,
            usdm_order_reply("usdm-r-1", 9001, "NEW", "4", "110", "0", "0"),
        );
        let now_2 = clock.add_ns(10_000);
        auth.policy.lock().unwrap().gate.now = now_2;
        let cmd_mod = make_modify_cmd(
            &base_cmd,
            8,
            4,
            2,
            2,
            local_oid,
            "usdm-r-1",
            Some(9001),
            None,
            v::Side::Buy,
            "5",
            "120",
            now_2,
        );
        owner.submit(cmd_mod, &[], now_2, gateway::until()).unwrap();
        assert_eq!(
            owner.account().venue_shadow(1).unwrap(),
            gateway::fixture::units(850),
            "in-flight Modify must lock worst-case max(20, 150) = 150 USDT hold"
        );
        let out_mod = drive_rest_outcome(&mut owner, &clock);
        assert_eq!(out_mod.disposition, RestDisposition::Observed);
        assert!(!owner.account().frozen());
        let reqs = order_server.recorded_requests();
        assert!(reqs[1].starts_with("PUT /fapi/v1/order?"), "{reqs:?}");
        assert_eq!(
            owner
                .account()
                .order(&order_key)
                .unwrap()
                .effective_quantity,
            Fixed::parse("4", 8).unwrap()
        );
        assert_eq!(
            owner.account().order_terms(&order_key).unwrap().1.price,
            Some("110".parse().unwrap())
        );
        assert_eq!(
            owner.account().venue_shadow(1).unwrap(),
            gateway::fixture::units(920),
            "venue-clamped Modify (4 @ 110 => 80 USDT) must release 70 USDT of worst-case hold"
        );

        // 3. Deliver unsolicited private-stream ORDER_TRADE_UPDATE with x="AMENDMENT" (q="3", p="105").
        let amendment_ws = serde_json::to_vec(&serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 200,
            "T": 199,
            "o": {
                "s": "BTCUSDT",
                "c": "usdm-r-1",
                "S": "BUY",
                "o": "LIMIT",
                "f": "GTC",
                "q": "3",
                "p": "105",
                "ap": "0",
                "x": "AMENDMENT",
                "X": "NEW",
                "i": 9001,
                "l": "0",
                "z": "0",
                "L": "0",
                "T": 199,
                "t": 0,
                "m": true,
                "ps": "BOTH"
            }
        }))
        .unwrap();
        deliver_private_frames(
            &mut owner,
            Market::Usdm,
            vec![amendment_ws],
            &mut clock,
            Arc::clone(&client_tls),
            Arc::clone(&server_tls),
        );
        assert!(!owner.account().frozen());
        assert_eq!(
            owner
                .account()
                .order(&order_key)
                .unwrap()
                .effective_quantity,
            Fixed::parse("3", 8).unwrap()
        );
        assert_eq!(
            owner.account().order_terms(&order_key).unwrap().1.price,
            Some("105".parse().unwrap())
        );
        assert_eq!(
            owner.account().venue_shadow(1).unwrap(),
            gateway::fixture::units(920),
            "unsolicited private AMENDMENT without ACCOUNT_UPDATE preserves conservative venue_shadow"
        );

        let expected_shadow = owner.account().venue_shadow(1).unwrap();
        drop(owner);
        drop(order_server);

        // 4. Deterministic WAL recovery + replay_pending check.
        let mut gw_cfg = gateway::config(&baseline);
        gw_cfg.writer_epoch = 5;
        let mut recovered_gw = AccountGateway::recover(
            FileBackend::open(&account_dir.0).unwrap(),
            gw_cfg,
            auth,
            RealRestEncoder {
                market: Market::Usdm,
                host_override: "demo-fapi.binance.com",
            },
            None,
        )
        .unwrap()
        .with_per_order_doubt_isolation(true);
        let mut rec_ing_cfg = ingress_config(&baseline);
        rec_ing_cfg.writer_epoch = 5;
        let mut recovered_ing =
            AccountIngress::recover(FileStore::open(&inbox_dir.0).unwrap(), rec_ing_cfg, None)
                .unwrap();
        recovered_ing.replay_pending(&mut recovered_gw, 32).unwrap();
        assert_eq!(
            recovered_gw.account().venue_shadow(1).unwrap(),
            expected_shadow
        );
        assert_eq!(
            recovered_gw
                .account()
                .order(&order_key)
                .unwrap()
                .effective_quantity,
            Fixed::parse("3", 8).unwrap()
        );
    }

    // --- Part B: USDM WebSocket "order.modify" Roundtrip + WAL Recovery ---
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let ws_addr = listener.local_addr().unwrap();
        let server_tls_ws = Arc::clone(&server_tls);
        let ws_thread = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut stream = rustls::StreamOwned::new(
                rustls::ServerConnection::new(server_tls_ws).unwrap(),
                socket,
            );
            complete_ws_upgrade(&mut stream);

            let req_place = read_masked_ws_text(&mut stream);
            assert_eq!(req_place["method"], "order.place");
            write_server_ws_text(
                &mut stream,
                &serde_json::json!({
                    "id": req_place["id"],
                    "status": 200,
                    "result": usdm_order_reply("usdm-ws-1", 9101, "NEW", "2", "100", "0", "0")
                }),
            );
            stream.flush().unwrap();

            let req_mod = read_masked_ws_text(&mut stream);
            assert_eq!(req_mod["method"], "order.modify");
            assert_eq!(req_mod["params"]["orderId"], 9101u64);
            assert_eq!(req_mod["params"]["origClientOrderId"], "usdm-ws-1");
            assert_eq!(req_mod["params"]["quantity"], "4");
            assert_eq!(req_mod["params"]["price"], "115");
            // Return clamped quantity 3 @ price 110 (60 USDT hold).
            write_server_ws_text(
                &mut stream,
                &serde_json::json!({
                    "id": req_mod["id"],
                    "status": 200,
                    "result": usdm_order_reply("usdm-ws-1", 9101, "NEW", "3", "110", "0", "0")
                }),
            );
            stream.flush().unwrap();
            let _ = stream.read(&mut [0u8; 1]);
        });

        let account_dir = gateway::Directory::new();
        let inbox_dir = gateway::Directory::new();
        let (baseline, base_cmd) = build_usdm_baseline();
        let mut gw_cfg = gateway::config(&baseline);
        gw_cfg.route.authority = "testnet.binancefuture.com".into();
        let mock_auth = gateway::MockAuthority::default();
        {
            let mut p = mock_auth.policy.lock().unwrap();
            p.gate.now = 10_000_000;
            p.gate.lease_until = u64::MAX / 2;
        }
        let ws_adapter = RealWsAdapter::new(mock_auth.clone(), Market::Usdm);
        let mut ingress = AccountIngress::create(
            FileStore::open(&inbox_dir.0).unwrap(),
            ingress_config(&baseline),
        )
        .unwrap();
        let session = ingress.reserve_ws_session([41; 16], [42; 32]).unwrap();
        let gw = AccountGateway::create(
            FileBackend::open(&account_dir.0).unwrap(),
            gw_cfg,
            baseline.clone(),
            ws_adapter.clone(),
            ws_adapter.clone(),
            ConnectionConfig {
                address: ws_addr,
                tls: Arc::clone(&client_tls),
                limits: Limits::default(),
                connect_deadline: gateway::until(),
            },
        )
        .unwrap();
        let mut owner = RestOwner::from_parts_websocket(
            ingress,
            gw,
            session,
            Box::new(ws_adapter),
            WsConfig {
                lifetime_ns: 60_000_000_000,
                control_timeout_ns: 5_000_000_000,
                max_messages_per_poll: 16,
            },
            Instant::now(),
            10_000_000,
        )
        .unwrap()
        .with_per_order_query_desk(QueryConfig::default());

        let budget = IoBudget {
            max_bytes: 65_536,
            max_syscalls: 64,
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while !owner.websocket_ready() {
            assert!(Instant::now() < deadline);
            owner.poll(Instant::now(), 10_000_000, budget).unwrap();
            thread::sleep(Duration::from_millis(1));
        }

        let now_1 = 20_000_000u64;
        mock_auth.policy.lock().unwrap().gate.now = now_1;
        let (cmd_new, order_key, local_oid) = make_new_cmd(
            &base_cmd,
            8,
            4,
            1,
            1,
            "usdm-ws-1",
            v::Side::Buy,
            "2",
            "100",
            v::TimeInForce::Gtc,
            now_1,
        );
        assert_eq!(
            owner
                .submit(cmd_new, &[local_oid], now_1, gateway::until())
                .unwrap(),
            Status::AwaitingCommit
        );
        let mut out_1 = None;
        while out_1.is_none() {
            assert!(Instant::now() < deadline);
            out_1 = owner.poll(Instant::now(), now_1 + 1_000, budget).unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(out_1.unwrap().disposition, RestDisposition::Observed);

        let now_2 = 30_000_000u64;
        mock_auth.policy.lock().unwrap().gate.now = now_2;
        let cmd_mod = make_modify_cmd(
            &base_cmd,
            8,
            4,
            2,
            2,
            local_oid,
            "usdm-ws-1",
            Some(9101),
            None,
            v::Side::Buy,
            "4",
            "115",
            now_2,
        );
        assert_eq!(
            owner.submit(cmd_mod, &[], now_2, gateway::until()).unwrap(),
            Status::AwaitingCommit
        );
        assert_eq!(
            owner.account().venue_shadow(1).unwrap(),
            gateway::fixture::units(900),
            "4 @ 115 => 100 USDT worst-case hold while WS order.modify is in flight"
        );
        let mut out_2 = None;
        while out_2.is_none() {
            assert!(Instant::now() < deadline);
            out_2 = owner.poll(Instant::now(), now_2 + 1_000, budget).unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(out_2.unwrap().disposition, RestDisposition::Observed);
        assert!(!owner.account().frozen());
        assert_eq!(
            owner
                .account()
                .order(&order_key)
                .unwrap()
                .effective_quantity,
            Fixed::parse("3", 8).unwrap()
        );
        assert_eq!(
            owner.account().venue_shadow(1).unwrap(),
            gateway::fixture::units(940),
            "clamped 3 @ 110 => 60 USDT hold, releasing 40 USDT excess hold"
        );

        drop(owner);
        ws_thread.join().unwrap();

        let mut rec_gw_cfg = gateway::config(&baseline);
        rec_gw_cfg.route.authority = "testnet.binancefuture.com".into();
        rec_gw_cfg.writer_epoch = 5;
        let rec_ws_adapter = RealWsAdapter::new(mock_auth, Market::Usdm);
        let mut rec_gw = AccountGateway::recover(
            FileBackend::open(&account_dir.0).unwrap(),
            rec_gw_cfg,
            rec_ws_adapter.clone(),
            rec_ws_adapter,
            None,
        )
        .unwrap()
        .with_per_order_doubt_isolation(true);
        let mut rec_ing_cfg = ingress_config(&baseline);
        rec_ing_cfg.writer_epoch = 5;
        let mut rec_ing =
            AccountIngress::recover(FileStore::open(&inbox_dir.0).unwrap(), rec_ing_cfg, None)
                .unwrap();
        rec_ing.replay_pending(&mut rec_gw, 32).unwrap();
        assert_eq!(
            rec_gw.account().venue_shadow(1).unwrap(),
            gateway::fixture::units(940)
        );
    }
}

#[test]
fn usdm_modify_venue_rejections_classify_reasons_rollback_pending_modify_and_keep_peer_orders_active()
 {
    let (client_tls, server_tls) = test_certs();
    let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));
    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_usdm_baseline();
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
        RealRestEncoder {
            market: Market::Usdm,
            host_override: "demo-fapi.binance.com",
        },
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
    let clock = ControllableClock::new(1_000_000, Market::Usdm);

    // Place Strategy A PostOnly Order 1 (BUY 2 @ 100 => 20 USDT) and Strategy B Peer Order 2 (BUY 1 @ 95 => 9.5 USDT).
    order_server.push_json(
        200,
        usdm_order_reply("strat-a-1", 9201, "NEW", "2", "100", "0", "0"),
    );
    let now_1 = clock.now_ns();
    auth.policy.lock().unwrap().gate.now = now_1;
    let (cmd_a, order_a_key, oid_a) = make_new_cmd(
        &base_cmd,
        8,
        4,
        1,
        1,
        "strat-a-1",
        v::Side::Buy,
        "2",
        "100",
        v::TimeInForce::PostOnly,
        now_1,
    );
    owner
        .submit(cmd_a, &[oid_a], now_1, gateway::until())
        .unwrap();
    assert_eq!(
        drive_rest_outcome(&mut owner, &clock).disposition,
        RestDisposition::Observed
    );

    order_server.push_json(
        200,
        usdm_order_reply("strat-b-1", 9202, "NEW", "1", "95", "0", "0"),
    );
    let now_2 = clock.add_ns(10_000);
    auth.policy.lock().unwrap().gate.now = now_2;
    let (cmd_b, order_b_key, oid_b) = make_new_cmd(
        &base_cmd,
        9,
        5,
        2,
        1,
        "strat-b-1",
        v::Side::Buy,
        "1",
        "95",
        v::TimeInForce::Gtc,
        now_2,
    );
    owner
        .submit(cmd_b, &[oid_b], now_2, gateway::until())
        .unwrap();
    assert_eq!(
        drive_rest_outcome(&mut owner, &clock).disposition,
        RestDisposition::Observed
    );
    let baseline_shadow = owner.account().venue_shadow(1).unwrap();
    assert_eq!(baseline_shadow, gateway::fixture::units(970));

    // Rejection 1: -5027 -> TerminalReason::NoChange; rolls back pending_modify & extra margin!
    // Rejection 2: -5022 -> TerminalReason::PostOnlyWouldTake!
    // Rejection 3: -2013 -> TerminalReason::OrderNotOpen!
    for (idx, (code, msg, expected_reason)) in [
        (-5027, "No need to modify the order.", v::TerminalReason::NoChange),
        (
            -5022,
            "Due to the order could not be executed as maker, the Post Only order will be rejected.",
            v::TerminalReason::PostOnlyWouldTake,
        ),
        (-2013, "Order does not exist.", v::TerminalReason::OrderNotOpen),
    ]
    .into_iter()
    .enumerate()
    {
        order_server.push_json(400, serde_json::json!({ "code": code, "msg": msg }));
        let now_m = clock.add_ns(10_000);
        auth.policy.lock().unwrap().gate.now = now_m;
        let intent_id = 10 + idx as u64;
        let cmd_mod = make_modify_cmd(
            &base_cmd,
            8,
            4,
            intent_id,
            2 + idx as u64,
            oid_a,
            "strat-a-1",
            Some(9201),
            None,
            v::Side::Buy,
            "4",
            "110",
            now_m,
        );
        owner
            .submit(cmd_mod, &[], now_m, gateway::until())
            .unwrap();
        assert_eq!(
            owner.account().venue_shadow(1).unwrap(),
            gateway::fixture::units(910),
            "in-flight Modify temporarily locks extra 60 USDT hold"
        );
        let out = drive_rest_outcome(&mut owner, &clock);
        assert_eq!(out.disposition, RestDisposition::Rejected);
        assert!(!owner.account().frozen(), "Modify rejection {code} must NOT freeze account");
        assert!(!owner.gateway().halted());
        assert_eq!(
            owner.account().order_last_modify_rejection(&order_a_key),
            Some(expected_reason)
        );
        assert_eq!(
            owner.account().venue_shadow(1).unwrap(),
            baseline_shadow,
            "ModifyRejected {code} must immediately roll back extra margin hold"
        );
        assert_eq!(
            owner.account().order(&order_a_key).unwrap().effective_quantity,
            Fixed::parse("2", 8).unwrap()
        );
    }

    // Peer Strategy B Order 2 remains completely active and can cancel cleanly!
    order_server.push_json(
        200,
        usdm_order_reply("strat-b-1", 9202, "CANCELED", "1", "95", "0", "0"),
    );
    let now_cancel = clock.add_ns(10_000);
    auth.policy.lock().unwrap().gate.now = now_cancel;
    let cmd_cancel_b =
        make_cancel_cmd(&base_cmd, 9, 5, 20, 2, oid_b, "strat-b-1", 9202, now_cancel);
    owner
        .submit(cmd_cancel_b, &[], now_cancel, gateway::until())
        .unwrap();
    assert_eq!(
        drive_rest_outcome(&mut owner, &clock).disposition,
        RestDisposition::Observed
    );
    assert!(owner.account().order(&order_b_key).unwrap().terminal);
    assert!(!owner.account().frozen());

    drop(owner);
    let mut rec_gw_cfg = gateway::config(&baseline);
    rec_gw_cfg.writer_epoch = 5;
    let mut rec_gw = AccountGateway::recover(
        FileBackend::open(&account_dir.0).unwrap(),
        rec_gw_cfg,
        auth,
        RealRestEncoder {
            market: Market::Usdm,
            host_override: "demo-fapi.binance.com",
        },
        None,
    )
    .unwrap()
    .with_per_order_doubt_isolation(true);
    let mut rec_ing_cfg = ingress_config(&baseline);
    rec_ing_cfg.writer_epoch = 5;
    let mut rec_ing =
        AccountIngress::recover(FileStore::open(&inbox_dir.0).unwrap(), rec_ing_cfg, None).unwrap();
    rec_ing.replay_pending(&mut rec_gw, 32).unwrap();
    assert_eq!(
        rec_gw.account().order_last_modify_rejection(&order_a_key),
        Some(v::TerminalReason::OrderNotOpen)
    );
}

#[test]
fn ambiguous_modify_transport_disconnect_isolates_amend_ambiguous_and_resolves_via_async_order_requery()
 {
    let (client_tls, server_tls) = test_certs();
    let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));
    let query_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));
    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_usdm_baseline();
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
        RealRestEncoder {
            market: Market::Usdm,
            host_override: "demo-fapi.binance.com",
        },
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
    let query_cfg = QueryConfig {
        status_query_delay_ns: 2_000_000_000,
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

    let mut clock = ControllableClock::new(1_000_000, Market::Usdm);
    let signing_key = SigningKey::hmac(network::SECRET).unwrap();
    let creds = Credentials {
        market: Market::Usdm,
        environment: v::Environment::Test,
        account: 2,
        api_key: network::KEY,
        signing_key: &signing_key,
    };
    let mut driver = PerOrderQueryDriver::with_querier_clock(
        PerOrderQueryConfig {
            scope: network::scope(Market::Usdm),
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

    // 1. Place Order 1 (BUY 2 @ 100, orderId=9301).
    order_server.push_json(
        200,
        usdm_order_reply("mod-amb-1", 9301, "NEW", "2", "100", "0", "0"),
    );
    let now_1 = clock.now_ns();
    auth.policy.lock().unwrap().gate.now = now_1;
    let (cmd_1, order_1_key, oid_1) = make_new_cmd(
        &base_cmd,
        8,
        4,
        1,
        1,
        "mod-amb-1",
        v::Side::Buy,
        "2",
        "100",
        v::TimeInForce::Gtc,
        now_1,
    );
    owner
        .submit(cmd_1, &[oid_1], now_1, gateway::until())
        .unwrap();
    assert_eq!(
        drive_rest_outcome(&mut owner, &clock).disposition,
        RestDisposition::Observed
    );

    // 2. Submit Modify #1 (BUY 4 @ 110 => 44 USDT hold); server drops TLS socket after receiving PUT!
    order_server.push_drop();
    let now_2 = clock.add_ns(10_000);
    auth.policy.lock().unwrap().gate.now = now_2;
    let cmd_mod_1 = make_modify_cmd(
        &base_cmd,
        8,
        4,
        2,
        2,
        oid_1,
        "mod-amb-1",
        Some(9301),
        None,
        v::Side::Buy,
        "4",
        "110",
        now_2,
    );
    owner
        .submit(cmd_mod_1, &[], now_2, gateway::until())
        .unwrap();
    let out_drop = drive_rest_outcome(&mut owner, &clock);
    assert_eq!(out_drop.disposition, RestDisposition::Unknown);
    assert!(
        !owner.account().frozen(),
        "Modify socket drop must isolate to AmendAmbiguous"
    );
    assert_eq!(
        owner.account().order_in_doubt(&order_1_key),
        Some(DoubtReason::AmendAmbiguous)
    );
    assert_eq!(
        owner.query_desk().unwrap().ask_kind(&order_1_key),
        Some(AskKind::Amend)
    );
    assert_eq!(
        owner.account().venue_shadow(1).unwrap(),
        gateway::fixture::units(920),
        "worst-case 80 USDT hold remains locked while AmendAmbiguous"
    );

    // Install fresh connection on RestOwner after socket drop so sibling orders continue uninterrupted.
    let fresh_conn = Connection::connect(
        order_server.address,
        Security::Tls {
            server_name: "demo-fapi.binance.com".try_into().unwrap(),
            config: Arc::clone(&client_tls),
        },
        Limits::default(),
        gateway::until(),
    )
    .unwrap();
    owner
        .install_transport_connection(fresh_conn, Limits::default())
        .unwrap();

    // 3. Advance clock past status_query_delay_ns; GET /fapi/v1/order returns applied (clamped) 4 @ 108!
    query_server.push_json(
        200,
        usdm_order_reply("mod-amb-1", 9301, "NEW", "4", "108", "0", "0"),
    );
    clock.set_ns(3_000_000_000);
    let res_1 = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
    assert!(matches!(
        res_1.as_slice(),
        [QueryResolution::Resolved { order }] if order == &order_1_key
    ));
    let q_reqs = query_server.recorded_requests();
    assert!(q_reqs[0].starts_with("GET /fapi/v1/order?"), "{q_reqs:?}");
    assert!(!owner.account().frozen());
    assert_eq!(owner.account().order_in_doubt(&order_1_key), None);
    assert_eq!(
        owner
            .account()
            .order(&order_1_key)
            .unwrap()
            .effective_quantity,
        Fixed::parse("4", 8).unwrap()
    );
    assert_eq!(
        owner.account().venue_shadow(1).unwrap(),
        gateway::fixture::units(928),
        "applied 4 @ 108 = 72 USDT hold"
    );

    // 4. Submit Modify #2 (BUY 5 @ 112 => 110 USDT hold); server returns 503 -1007 ambiguous timeout!
    order_server.push_json(
        503,
        serde_json::json!({
            "code": -1007,
            "msg": "Timeout waiting for response from backend server."
        }),
    );
    let now_3 = clock.add_ns(10_000);
    auth.policy.lock().unwrap().gate.now = now_3;
    let cmd_mod_2 = make_modify_cmd(
        &base_cmd,
        8,
        4,
        3,
        3,
        oid_1,
        "mod-amb-1",
        Some(9301),
        None,
        v::Side::Buy,
        "5",
        "112",
        now_3,
    );
    owner
        .submit(cmd_mod_2, &[], now_3, gateway::until())
        .unwrap();
    assert_eq!(
        drive_rest_outcome(&mut owner, &clock).disposition,
        RestDisposition::Unknown
    );
    assert_eq!(
        owner.account().order_in_doubt(&order_1_key),
        Some(DoubtReason::AmendAmbiguous)
    );

    // GET /fapi/v1/order returns unchanged 4 @ 108 -> Modify #2 never landed, rolled back via ModifyRejected!
    query_server.push_json(
        200,
        usdm_order_reply("mod-amb-1", 9301, "NEW", "4", "108", "0", "0"),
    );
    clock.add_ns(3_000_000_000);
    let res_2 = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
    assert!(matches!(
        res_2.as_slice(),
        [QueryResolution::Resolved { order }] if order == &order_1_key
    ));
    assert!(!owner.account().frozen());
    assert_eq!(owner.account().order_in_doubt(&order_1_key), None);
    assert_eq!(
        owner.account().order_last_modify_rejection(&order_1_key),
        Some(v::TerminalReason::Unknown)
    );
    assert_eq!(
        owner
            .account()
            .order(&order_1_key)
            .unwrap()
            .effective_quantity,
        Fixed::parse("4", 8).unwrap()
    );
    assert_eq!(
        owner.account().venue_shadow(1).unwrap(),
        gateway::fixture::units(928),
        "rolled-back Modify #2 restores 72 USDT hold"
    );

    driver.stop();
    drop(driver);
    drop(owner);
    let mut rec_gw_cfg = gateway::config(&baseline);
    rec_gw_cfg.writer_epoch = 5;
    let mut rec_gw = AccountGateway::recover(
        FileBackend::open(&account_dir.0).unwrap(),
        rec_gw_cfg,
        auth,
        RealRestEncoder {
            market: Market::Usdm,
            host_override: "demo-fapi.binance.com",
        },
        None,
    )
    .unwrap()
    .with_per_order_doubt_isolation(true);
    let mut rec_ing_cfg = ingress_config(&baseline);
    rec_ing_cfg.writer_epoch = 5;
    let mut rec_ing =
        AccountIngress::recover(FileStore::open(&inbox_dir.0).unwrap(), rec_ing_cfg, None).unwrap();
    rec_ing.replay_pending(&mut rec_gw, 32).unwrap();
    assert_eq!(rec_gw.account().order_in_doubt(&order_1_key), None);
    assert_eq!(
        rec_gw.account().venue_shadow(1).unwrap(),
        gateway::fixture::units(928)
    );
}

#[test]
fn projected_fill_emits_early_from_spot_cum_quote_and_usdm_avg_price_without_double_counting_trade_frame()
 {
    let (client_tls, server_tls) = test_certs();

    // --- Sub-case A: Spot Z (cumulative quote) early ProjectedFill before TRADE frame ---
    {
        let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));
        let account_dir = gateway::Directory::new();
        let inbox_dir = gateway::Directory::new();
        let (baseline, base_cmd) = build_spot_baseline();
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
            RealRestEncoder {
                market: Market::Spot,
                host_override: "testnet.binance.vision",
            },
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
        .with_projected_fill(true);
        assert!(owner.projected_fill_enabled());
        let mut clock = ControllableClock::new(1_000_000, Market::Spot);

        order_server.push_json(
            200,
            serde_json::json!({
                "symbol": "BTCUSDT",
                "orderId": 9401,
                "orderListId": -1,
                "clientOrderId": "spot-pf-1",
                "transactTime": 1_770_000_000_001u64,
                "price": "60000",
                "origQty": "0.002",
                "executedQty": "0",
                "cummulativeQuoteQty": "0",
                "status": "NEW",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "BUY"
            }),
        );
        let now_1 = clock.now_ns();
        auth.policy.lock().unwrap().gate.now = now_1;
        let (cmd_1, order_key, oid_1) = make_new_cmd(
            &base_cmd,
            8,
            4,
            1,
            1,
            "spot-pf-1",
            v::Side::Buy,
            "0.002",
            "60000",
            v::TimeInForce::Gtc,
            now_1,
        );
        owner
            .submit(cmd_1, &[oid_1], now_1, gateway::until())
            .unwrap();
        assert_eq!(
            drive_rest_outcome(&mut owner, &clock).disposition,
            RestDisposition::Observed
        );
        assert!(owner.drain_projected_fills().is_empty());

        // 1. Non-trade status update arrives FIRST with z="0.001", Z="59.95" (implied price = 59950)!
        let early_report = serde_json::to_vec(&serde_json::json!({
            "subscriptionId": 0,
            "event": {
                "e": "executionReport",
                "E": 100,
                "s": "BTCUSDT",
                "c": "spot-pf-1",
                "S": "BUY",
                "o": "LIMIT",
                "f": "GTC",
                "q": "0.002",
                "p": "60000",
                "g": -1,
                "C": "",
                "x": "NEW",
                "X": "PARTIALLY_FILLED",
                "i": 9401,
                "l": "0",
                "z": "0.001",
                "L": "0",
                "n": "0",
                "N": "USDT",
                "T": 99,
                "t": -1,
                "I": 1,
                "m": false,
                "Z": "59.95",
                "Y": "0",
                "r": "NONE"
            }
        }))
        .unwrap();
        deliver_private_frames(
            &mut owner,
            Market::Spot,
            vec![early_report],
            &mut clock,
            Arc::clone(&client_tls),
            Arc::clone(&server_tls),
        );

        let pf_list = owner.drain_projected_fills();
        assert_eq!(pf_list.len(), 1);
        let pf = &pf_list[0];
        assert_eq!(pf.order, order_key);
        assert_eq!(pf.gap_quantity, Fixed::parse("0.001", 8).unwrap());
        assert_eq!(pf.gap_price, Fixed::parse("59950", 8).unwrap());
        assert_eq!(pf.order_filled, Fixed::parse("0", 8).unwrap());
        assert_eq!(pf.order_projected, Fixed::parse("0.001", 8).unwrap());
        assert!(pf.projected);
        assert_eq!(
            owner.account().order(&order_key).unwrap().filled_quantity,
            Fixed::parse("0", 8).unwrap(),
            "settled filled_quantity must remain 0 before TRADE frame"
        );

        // 2. Subsequent TRADE frame arrives for the exact same 0.001 fill: settles Fill with ZERO duplicate ProjectedFill!
        let trade_report = serde_json::to_vec(&serde_json::json!({
            "subscriptionId": 0,
            "event": {
                "e": "executionReport",
                "E": 101,
                "s": "BTCUSDT",
                "c": "spot-pf-1",
                "S": "BUY",
                "o": "LIMIT",
                "f": "GTC",
                "q": "0.002",
                "p": "60000",
                "g": -1,
                "C": "",
                "x": "TRADE",
                "X": "PARTIALLY_FILLED",
                "i": 9401,
                "l": "0.001",
                "z": "0.001",
                "L": "59950",
                "n": "0",
                "N": "USDT",
                "T": 100,
                "t": 7701,
                "I": 2,
                "m": false,
                "Z": "59.95",
                "Y": "59.95",
                "r": "NONE"
            }
        }))
        .unwrap();
        deliver_private_frames(
            &mut owner,
            Market::Spot,
            vec![trade_report],
            &mut clock,
            Arc::clone(&client_tls),
            Arc::clone(&server_tls),
        );
        assert!(
            owner.drain_projected_fills().is_empty(),
            "settled TRADE frame must NOT emit a duplicate ProjectedFill"
        );
        assert_eq!(
            owner.account().order(&order_key).unwrap().filled_quantity,
            Fixed::parse("0.001", 8).unwrap()
        );
        assert_eq!(
            owner.account().order_projected_filled(&order_key),
            Some(Fixed::parse("0.001", 8).unwrap())
        );
    }

    // --- Sub-case B: USDM avgPrice (REST reply) + ap (ORDER_TRADE_UPDATE) before TRADE frames ---
    {
        let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));
        let account_dir = gateway::Directory::new();
        let inbox_dir = gateway::Directory::new();
        let (baseline, base_cmd) = build_usdm_baseline();
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
            RealRestEncoder {
                market: Market::Usdm,
                host_override: "demo-fapi.binance.com",
            },
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
        owner.set_projected_fill(true);
        let mut clock = ControllableClock::new(1_000_000, Market::Usdm);

        // Synchronous REST OrderReply reports executedQty="0.5", avgPrice="99.50" immediately!
        order_server.push_json(
            200,
            usdm_order_reply(
                "usdm-pf-1",
                9402,
                "PARTIALLY_FILLED",
                "2",
                "100",
                "0.5",
                "99.50",
            ),
        );
        let now_1 = clock.now_ns();
        auth.policy.lock().unwrap().gate.now = now_1;
        let (cmd_1, order_key, oid_1) = make_new_cmd(
            &base_cmd,
            8,
            4,
            1,
            1,
            "usdm-pf-1",
            v::Side::Buy,
            "2",
            "100",
            v::TimeInForce::Gtc,
            now_1,
        );
        owner
            .submit(cmd_1, &[oid_1], now_1, gateway::until())
            .unwrap();
        let out_1 = drive_rest_outcome(&mut owner, &clock);
        assert_eq!(out_1.disposition, RestDisposition::NeedsReconciliation);
        let pf_1 = owner
            .pop_projected_fill()
            .expect("ProjectedFill from REST OrderReply");
        assert!(owner.pop_projected_fill().is_none());
        assert_eq!(pf_1.gap_quantity, Fixed::parse("0.5", 8).unwrap());
        assert_eq!(pf_1.gap_price, Fixed::parse("99.5", 8).unwrap());
        assert_eq!(pf_1.order_projected, Fixed::parse("0.5", 8).unwrap());

        // Non-trade ORDER_TRADE_UPDATE advances z from 0.5 -> 1.5 with ap="99.70"
        // (0.5*99.50 = 49.75; 1.5*99.70 = 149.55 => delta = 99.80 over gap_qty=1.0 => gap_price = 99.80!).
        let usdm_early = serde_json::to_vec(&serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 200,
            "T": 199,
            "o": {
                "s": "BTCUSDT",
                "c": "usdm-pf-1",
                "S": "BUY",
                "o": "LIMIT",
                "f": "GTC",
                "q": "2",
                "p": "100",
                "ap": "99.70",
                "x": "AMENDMENT",
                "X": "PARTIALLY_FILLED",
                "i": 9402,
                "l": "0",
                "z": "1.5",
                "L": "0",
                "T": 199,
                "t": 0,
                "m": false,
                "ps": "BOTH"
            }
        }))
        .unwrap();
        let usdm_trade_1 = serde_json::to_vec(&serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 201,
            "T": 200,
            "o": {
                "s": "BTCUSDT",
                "c": "usdm-pf-1",
                "S": "BUY",
                "o": "LIMIT",
                "f": "GTC",
                "q": "2",
                "p": "100",
                "ap": "99.50",
                "x": "TRADE",
                "X": "PARTIALLY_FILLED",
                "i": 9402,
                "l": "0.5",
                "z": "0.5",
                "L": "99.50",
                "n": "0",
                "N": "USDT",
                "T": 200,
                "t": 8801,
                "m": false,
                "ps": "BOTH",
                "rp": "0"
            }
        }))
        .unwrap();
        let usdm_trade_2 = serde_json::to_vec(&serde_json::json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 202,
            "T": 201,
            "o": {
                "s": "BTCUSDT",
                "c": "usdm-pf-1",
                "S": "BUY",
                "o": "LIMIT",
                "f": "GTC",
                "q": "2",
                "p": "100",
                "ap": "99.70",
                "x": "TRADE",
                "X": "PARTIALLY_FILLED",
                "i": 9402,
                "l": "1.0",
                "z": "1.5",
                "L": "99.80",
                "n": "0",
                "N": "USDT",
                "T": 201,
                "t": 8802,
                "m": false,
                "ps": "BOTH",
                "rp": "0"
            }
        }))
        .unwrap();

        deliver_private_frames(
            &mut owner,
            Market::Usdm,
            vec![usdm_early],
            &mut clock,
            Arc::clone(&client_tls),
            Arc::clone(&server_tls),
        );
        let pf_2 = owner
            .pop_projected_fill()
            .expect("ProjectedFill from USDM ORDER_TRADE_UPDATE ap");
        assert!(owner.pop_projected_fill().is_none());
        assert_eq!(pf_2.gap_quantity, Fixed::parse("1", 8).unwrap());
        assert_eq!(pf_2.gap_price, Fixed::parse("99.8", 8).unwrap());
        assert_eq!(pf_2.order_filled, Fixed::parse("0", 8).unwrap());
        assert_eq!(pf_2.order_projected, Fixed::parse("1.5", 8).unwrap());

        deliver_private_frames(
            &mut owner,
            Market::Usdm,
            vec![usdm_trade_1, usdm_trade_2],
            &mut clock,
            Arc::clone(&client_tls),
            Arc::clone(&server_tls),
        );
        assert!(
            owner.drain_projected_fills().is_empty(),
            "subsequent USDM TRADE frames must book settled Fill with zero double-counting"
        );
        assert_eq!(
            owner.account().order(&order_key).unwrap().filled_quantity,
            Fixed::parse("1.5", 8).unwrap()
        );
        assert!(!owner.account().frozen());
    }
}

#[test]
fn local_stp_book_crosses_book_refusal_preserves_rate_limiter_and_ws_session_and_classifies_venue_stp_expiry()
 {
    let (client_tls, server_tls) = test_certs();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let ws_addr = listener.local_addr().unwrap();
    let server_tls_ws = Arc::clone(&server_tls);

    let ws_thread = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut stream = rustls::StreamOwned::new(
            rustls::ServerConnection::new(server_tls_ws).unwrap(),
            socket,
        );
        complete_ws_upgrade(&mut stream);

        // Only TWO valid orders (stp-buy-1 and stp-sell-ok) should EVER reach the wire!
        // Both crossing New and crossing Modify must be refused locally with ZERO network frames!
        let req_1 = read_masked_ws_text(&mut stream);
        assert_eq!(req_1["method"], "order.place");
        assert_eq!(req_1["params"]["newClientOrderId"], "stp-buy-1");
        write_server_ws_text(
            &mut stream,
            &serde_json::json!({
                "id": req_1["id"],
                "status": 200,
                "result": usdm_order_reply("stp-buy-1", 9501, "NEW", "1", "100", "0", "0")
            }),
        );
        stream.flush().unwrap();

        let req_2 = read_masked_ws_text(&mut stream);
        assert_eq!(req_2["method"], "order.place");
        assert_eq!(req_2["params"]["newClientOrderId"], "stp-sell-ok");
        let mut sell_reply = usdm_order_reply("stp-sell-ok", 9503, "NEW", "1", "105", "0", "0");
        sell_reply["side"] = serde_json::json!("SELL");
        write_server_ws_text(
            &mut stream,
            &serde_json::json!({
                "id": req_2["id"],
                "status": 200,
                "result": sell_reply
            }),
        );
        stream.flush().unwrap();
        let _ = stream.read(&mut [0u8; 1]);
    });

    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_usdm_baseline();
    let mut gw_cfg = gateway::config(&baseline);
    gw_cfg.route.authority = "testnet.binancefuture.com".into();
    let mock_auth = gateway::MockAuthority::default();
    {
        let mut p = mock_auth.policy.lock().unwrap();
        p.gate.now = 10_000_000;
        p.gate.lease_until = u64::MAX / 2;
    }
    let ws_adapter = RealWsAdapter::new(mock_auth.clone(), Market::Usdm);
    let mut ingress = AccountIngress::create(
        FileStore::open(&inbox_dir.0).unwrap(),
        ingress_config(&baseline),
    )
    .unwrap();
    let session = ingress.reserve_ws_session([41; 16], [42; 32]).unwrap();
    let gw = AccountGateway::create(
        FileBackend::open(&account_dir.0).unwrap(),
        gw_cfg,
        baseline,
        ws_adapter.clone(),
        ws_adapter.clone(),
        ConnectionConfig {
            address: ws_addr,
            tls: Arc::clone(&client_tls),
            limits: Limits::default(),
            connect_deadline: gateway::until(),
        },
    )
    .unwrap();
    let ip_ws = IpKey::from_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
    let limiter = VenueRateLimiter::new(VenuePreset::BinanceUsdm, 2, &[ip_ws]).unwrap();

    let mut owner = RestOwner::from_parts_websocket(
        ingress,
        gw,
        session,
        Box::new(ws_adapter.clone()),
        WsConfig {
            lifetime_ns: 60_000_000_000,
            control_timeout_ns: 5_000_000_000,
            max_messages_per_poll: 16,
        },
        Instant::now(),
        10_000_000,
    )
    .unwrap()
    .with_stp_enforcement(true)
    .with_venue_rate_limiter(limiter);
    assert!(owner.stp_enforcement());

    let budget = IoBudget {
        max_bytes: 65_536,
        max_syscalls: 64,
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while !owner.websocket_ready() {
        assert!(Instant::now() < deadline);
        owner.poll(Instant::now(), 10_000_000, budget).unwrap();
        thread::sleep(Duration::from_millis(1));
    }

    // 1. Place resting Buy Order 1 (BUY 1 @ 100).
    let now_1 = 20_000_000u64;
    mock_auth.policy.lock().unwrap().gate.now = now_1;
    let (cmd_buy, buy_key, oid_buy) = make_new_cmd(
        &base_cmd,
        8,
        4,
        1,
        1,
        "stp-buy-1",
        v::Side::Buy,
        "1",
        "100",
        v::TimeInForce::Gtc,
        now_1,
    );
    owner
        .submit(cmd_buy, &[oid_buy], now_1, gateway::until())
        .unwrap();
    let mut out_1 = None;
    while out_1.is_none() {
        assert!(Instant::now() < deadline);
        out_1 = owner.poll(Instant::now(), now_1 + 1_000, budget).unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(out_1.unwrap().disposition, RestDisposition::Observed);
    let acct_win = owner
        .venue_rate_limiter()
        .unwrap()
        .account_order_window_ids()[0];
    assert_eq!(
        owner
            .venue_rate_limiter()
            .unwrap()
            .limiter()
            .window_used(acct_win, now_1),
        Some(1)
    );

    // 2. Submit crossing Sell Order 2 (SELL 1 @ 99 <= resting bid 100): locally refused with CrossesBook!
    let now_2 = 30_000_000u64;
    mock_auth.policy.lock().unwrap().gate.now = now_2;
    let (cmd_cross_new, _cross_key, oid_cross) = make_new_cmd(
        &base_cmd,
        9,
        5,
        2,
        1,
        "stp-sell-cross",
        v::Side::Sell,
        "1",
        "99",
        v::TimeInForce::Gtc,
        now_2,
    );
    let cross_new_digest = cmd_cross_new.digest();
    let err_new = owner
        .submit(cmd_cross_new, &[oid_cross], now_2, gateway::until())
        .unwrap_err();
    assert_eq!(
        err_new.classify_refusal_reason(),
        Some(v::TerminalReason::CrossesBook)
    );
    assert!(!owner.account().frozen());
    assert!(!owner.gateway().halted());
    assert!(owner.websocket_ready());
    assert_eq!(ws_adapter.discarded_digests(), vec![cross_new_digest]);
    assert_eq!(
        owner
            .venue_rate_limiter()
            .unwrap()
            .limiter()
            .window_used(acct_win, now_2),
        Some(1),
        "local StpBook CrossesBook refusal must burn ZERO VenueRateLimiter quota"
    );

    // 3. Place non-crossing Sell Order 3 (SELL 1 @ 105 > resting bid 100) on the SAME healthy WebSocket session!
    let now_3 = 40_000_000u64;
    mock_auth.policy.lock().unwrap().gate.now = now_3;
    let (cmd_sell_ok, sell_ok_key, oid_sell_ok) = make_new_cmd(
        &base_cmd,
        9,
        5,
        3,
        2,
        "stp-sell-ok",
        v::Side::Sell,
        "1",
        "105",
        v::TimeInForce::Gtc,
        now_3,
    );
    owner
        .submit(cmd_sell_ok, &[oid_sell_ok], now_3, gateway::until())
        .unwrap();
    let mut out_3 = None;
    while out_3.is_none() {
        assert!(Instant::now() < deadline);
        out_3 = owner.poll(Instant::now(), now_3 + 1_000, budget).unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(out_3.unwrap().disposition, RestDisposition::Observed);
    assert_eq!(
        owner
            .venue_rate_limiter()
            .unwrap()
            .limiter()
            .window_used(acct_win, now_3),
        Some(2)
    );

    // 4. Submit crossing Modify on Sell Order 3 (price -> 100, crossing resting bid @ 100): locally refused!
    let now_4 = 50_000_000u64;
    mock_auth.policy.lock().unwrap().gate.now = now_4;
    let cmd_cross_mod = make_modify_cmd(
        &base_cmd,
        9,
        5,
        4,
        3,
        oid_sell_ok,
        "stp-sell-ok",
        Some(9503),
        None,
        v::Side::Sell,
        "1",
        "100",
        now_4,
    );
    let cross_mod_digest = cmd_cross_mod.digest();
    let err_mod = owner
        .submit(cmd_cross_mod, &[], now_4, gateway::until())
        .unwrap_err();
    assert_eq!(
        err_mod.classify_refusal_reason(),
        Some(v::TerminalReason::CrossesBook)
    );
    assert!(!owner.account().frozen());
    assert!(!owner.gateway().halted());
    assert!(owner.websocket_ready());
    assert_eq!(
        ws_adapter.discarded_digests(),
        vec![cross_new_digest, cross_mod_digest]
    );
    assert_eq!(
        owner
            .venue_rate_limiter()
            .unwrap()
            .limiter()
            .window_used(acct_win, now_4),
        Some(2),
        "crossing Modify refusal must burn ZERO VenueRateLimiter quota"
    );
    assert_eq!(
        owner.account().order_terms(&sell_ok_key).unwrap().1.price,
        Some("105".parse().unwrap())
    );

    // 5. Venue STP expiry (EXPIRED_IN_MATCH + V="EXPIRE_MAKER") classifies to SelfTradePreventedByVenue
    // and cleanly closes the resting order on private stream ingestion!
    let mut clock = ControllableClock::new(60_000_000, Market::Usdm);
    let stp_expire_raw = serde_json::to_vec(&serde_json::json!({
        "e": "ORDER_TRADE_UPDATE",
        "E": 300,
        "T": 299,
        "o": {
            "s": "BTCUSDT",
            "c": "stp-buy-1",
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "1",
            "p": "100",
            "ap": "0",
            "x": "TRADE_PREVENTION",
            "X": "EXPIRED_IN_MATCH",
            "i": 9501,
            "l": "0",
            "z": "0",
            "L": "0",
            "T": 299,
            "t": 0,
            "m": true,
            "ps": "BOTH",
            "V": "EXPIRE_MAKER"
        }
    }))
    .unwrap();
    let usdm_session = SessionContext::new(
        SessionIdentity {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: v::Environment::Test,
            account: 2,
            connection_epoch: 1,
        },
        Market::Usdm,
        PrivateStream::UsdmListenKey,
        TimeUnit::Milliseconds,
    )
    .unwrap();
    let decoded_stp = decode_private::<4>(&usdm_session, &stp_expire_raw, 4096, 300).unwrap();
    let PrivateBody::Order(stp_wire) = decoded_stp.body() else {
        panic!("expected order update");
    };
    assert_eq!(
        classify_order_update_reason(stp_wire),
        Some(v::TerminalReason::SelfTradePreventedByVenue)
    );

    deliver_private_frames(
        &mut owner,
        Market::Usdm,
        vec![stp_expire_raw],
        &mut clock,
        Arc::clone(&client_tls),
        Arc::clone(&server_tls),
    );
    assert!(owner.account().order(&buy_key).unwrap().terminal);
    assert!(!owner.account().frozen());

    drop(owner);
    ws_thread.join().unwrap();
}

#[test]
fn spot_modify_cancel_replace_rotates_client_id_and_handles_stop_on_failure_rejection() {
    let (client_tls, server_tls) = test_certs();
    let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));
    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_spot_baseline();
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
        RealRestEncoder {
            market: Market::Spot,
            host_override: "testnet.binance.vision",
        },
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
    let mut clock = ControllableClock::new(1_000_000, Market::Spot);

    // 1. Place Spot Order 1 (BUY 0.002 BTC @ 60000 => 120 USDT).
    order_server.push_json(
        200,
        serde_json::json!({
            "symbol": "BTCUSDT",
            "orderId": 9601,
            "orderListId": -1,
            "clientOrderId": "spot-mod-1",
            "transactTime": 1_770_000_000_001u64,
            "price": "60000",
            "origQty": "0.002",
            "executedQty": "0",
            "cummulativeQuoteQty": "0",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY"
        }),
    );
    let now_1 = clock.now_ns();
    auth.policy.lock().unwrap().gate.now = now_1;
    let (cmd_1, order_key, oid_1) = make_new_cmd(
        &base_cmd,
        8,
        4,
        1,
        1,
        "spot-mod-1",
        v::Side::Buy,
        "0.002",
        "60000",
        v::TimeInForce::Gtc,
        now_1,
    );
    owner
        .submit(cmd_1, &[oid_1], now_1, gateway::until())
        .unwrap();
    assert_eq!(
        drive_rest_outcome(&mut owner, &clock).disposition,
        RestDisposition::Observed
    );

    // 2. Spot Modify #1 via POST /api/v3/order/cancelReplace (STOP_ON_FAILURE) rotating clientOrderId -> "spot-mod-2"!
    order_server.push_json(
        200,
        serde_json::json!({
            "cancelResult": "SUCCESS",
            "newOrderResult": "SUCCESS",
            "cancelResponse": {
                "symbol": "BTCUSDT",
                "origClientOrderId": "spot-mod-1",
                "orderId": 9601,
                "orderListId": -1,
                "clientOrderId": "cancel-spot-mod-1",
                "transactTime": 1_770_000_000_050u64,
                "price": "60000",
                "origQty": "0.002",
                "executedQty": "0",
                "cummulativeQuoteQty": "0",
                "status": "CANCELED",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "BUY"
            },
            "newOrderResponse": {
                "symbol": "BTCUSDT",
                "orderId": 9602,
                "orderListId": -1,
                "clientOrderId": "spot-mod-2",
                "transactTime": 1_770_000_000_050u64,
                "price": "60100",
                "origQty": "0.003",
                "executedQty": "0",
                "cummulativeQuoteQty": "0",
                "status": "NEW",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "BUY"
            }
        }),
    );
    let now_2 = clock.add_ns(10_000);
    auth.policy.lock().unwrap().gate.now = now_2;
    let cmd_mod_1 = make_modify_cmd(
        &base_cmd,
        8,
        4,
        2,
        2,
        oid_1,
        "spot-mod-1",
        Some(9601),
        Some("spot-mod-2"),
        v::Side::Buy,
        "0.003",
        "60100",
        now_2,
    );
    owner
        .submit(cmd_mod_1, &[], now_2, gateway::until())
        .unwrap();
    assert_eq!(
        drive_rest_outcome(&mut owner, &clock).disposition,
        RestDisposition::Observed
    );
    let reqs = order_server.recorded_requests();
    assert!(
        reqs[1].starts_with("POST /api/v3/order/cancelReplace?"),
        "{reqs:?}"
    );
    assert_eq!(
        owner
            .account()
            .order(&order_key)
            .unwrap()
            .effective_quantity,
        Fixed::parse("0.003", 8).unwrap()
    );
    assert_eq!(
        owner
            .account()
            .order_terms(&order_key)
            .unwrap()
            .1
            .client_id
            .as_str(),
        "spot-mod-2"
    );

    // 3. Spot Modify #2 hits STOP_ON_FAILURE (400 code -2022 with inner cancelResponse -2011 -> OrderNotOpen)!
    order_server.push_json(
        400,
        serde_json::json!({
            "code": -2022,
            "msg": "Order cancel-replace failed.",
            "data": {
                "cancelResult": "FAILURE",
                "newOrderResult": "NOT_ATTEMPTED",
                "cancelResponse": {
                    "code": -2011,
                    "msg": "Unknown order sent."
                },
                "newOrderResponse": null
            }
        }),
    );
    let now_3 = clock.add_ns(10_000);
    auth.policy.lock().unwrap().gate.now = now_3;
    let cmd_mod_2 = make_modify_cmd(
        &base_cmd,
        8,
        4,
        3,
        3,
        oid_1,
        "spot-mod-2",
        Some(9602),
        Some("spot-mod-3"),
        v::Side::Buy,
        "0.004",
        "60200",
        now_3,
    );
    owner
        .submit(cmd_mod_2, &[], now_3, gateway::until())
        .unwrap();
    let out_rej = drive_rest_outcome(&mut owner, &clock);
    assert_eq!(out_rej.disposition, RestDisposition::Rejected);
    assert!(!owner.account().frozen());
    assert_eq!(
        owner.account().order_last_modify_rejection(&order_key),
        Some(v::TerminalReason::OrderNotOpen)
    );

    // 4. Subsequent private-stream TRADE on rotated ("spot-mod-2", orderId=9602) resolves and settles cleanly!
    let spot_fill = serde_json::to_vec(&serde_json::json!({
        "subscriptionId": 0,
        "event": {
            "e": "executionReport",
            "E": 300,
            "s": "BTCUSDT",
            "c": "spot-mod-2",
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "0.003",
            "p": "60100",
            "g": -1,
            "C": "",
            "x": "TRADE",
            "X": "FILLED",
            "i": 9602,
            "l": "0.003",
            "z": "0.003",
            "L": "60100",
            "n": "0",
            "N": "USDT",
            "T": 299,
            "t": 8901,
            "I": 10,
            "m": true,
            "Z": "180.3",
            "Y": "180.3",
            "r": "NONE"
        }
    }))
    .unwrap();
    deliver_private_frames(
        &mut owner,
        Market::Spot,
        vec![spot_fill],
        &mut clock,
        Arc::clone(&client_tls),
        Arc::clone(&server_tls),
    );
    assert!(!owner.account().frozen());
    assert!(owner.account().order(&order_key).unwrap().terminal);
    assert_eq!(
        owner.account().order(&order_key).unwrap().filled_quantity,
        Fixed::parse("0.003", 8).unwrap()
    );
}

#[test]
fn spot_cancel_replace_amend_ambiguous_query_driver_new_cid_probe_fallback_and_out_of_order_ws_replay()
 {
    let (client_tls, server_tls) = test_certs();
    let order_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));
    let query_server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));
    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_spot_baseline();
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
        RealRestEncoder {
            market: Market::Spot,
            host_override: "testnet.binance.vision",
        },
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
    let query_cfg = QueryConfig {
        status_query_delay_ns: 1_000_000,
        place_retry_ns: 1_000_000,
        cancel_initial_retry_ns: 1_000_000,
        cancel_max_retry_ns: 10_000_000,
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

    let spot_scope = network::scope(Market::Spot);
    let mut clock = ControllableClock::new(1_000_000, Market::Spot);
    let signing_key = SigningKey::hmac(network::SECRET).unwrap();
    let creds = Credentials {
        market: Market::Spot,
        environment: spot_scope.environment,
        account: spot_scope.account,
        api_key: network::KEY,
        signing_key: &signing_key,
    };
    let mut driver = PerOrderQueryDriver::with_querier_clock(
        PerOrderQueryConfig {
            scope: spot_scope,
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

    // 1. Place Spot Order 1 ("spot-amb-1", orderId=9701, BUY 0.002 @ 60000).
    order_server.push_json(
        200,
        serde_json::json!({
            "symbol": "BTCUSDT",
            "orderId": 9701,
            "orderListId": -1,
            "clientOrderId": "spot-amb-1",
            "transactTime": 1_770_000_000_001u64,
            "price": "60000",
            "origQty": "0.002",
            "executedQty": "0",
            "cummulativeQuoteQty": "0",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY"
        }),
    );
    let now_1 = clock.now_ns();
    auth.policy.lock().unwrap().gate.now = now_1;
    let (cmd_1, order_key, oid_1) = make_new_cmd(
        &base_cmd,
        8,
        4,
        1,
        1,
        "spot-amb-1",
        v::Side::Buy,
        "0.002",
        "60000",
        v::TimeInForce::Gtc,
        now_1,
    );
    owner
        .submit(cmd_1, &[oid_1], now_1, gateway::until())
        .unwrap();
    assert_eq!(
        drive_rest_outcome(&mut owner, &clock).disposition,
        RestDisposition::Observed
    );

    let reconnect_spot = |owner: &mut RestOwner<_, _, _, _>| {
        let conn = Connection::connect(
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
            .install_transport_connection(conn, Limits::default())
            .unwrap();
    };

    // 2. Spot Modify #1 ("spot-amb-1" -> "spot-amb-2", BUY 0.003 @ 60100) drops TLS -> AmendAmbiguous!
    order_server.push_drop();
    let now_2 = clock.add_ns(10_000);
    auth.policy.lock().unwrap().gate.now = now_2;
    let cmd_mod_1 = make_modify_cmd(
        &base_cmd,
        8,
        4,
        2,
        2,
        oid_1,
        "spot-amb-1",
        Some(9701),
        Some("spot-amb-2"),
        v::Side::Buy,
        "0.003",
        "60100",
        now_2,
    );
    owner
        .submit(cmd_mod_1, &[], now_2, gateway::until())
        .unwrap();
    assert_eq!(
        drive_rest_outcome(&mut owner, &clock).disposition,
        RestDisposition::Unknown
    );
    assert_eq!(
        owner.account().order_in_doubt(&order_key),
        Some(DoubtReason::AmendAmbiguous)
    );
    reconnect_spot(&mut owner);

    // PerOrderQueryDriver probes origClientOrderId=spot-amb-2 -> 200 OK (cancelReplace succeeded on Spot!).
    query_server.push_json(
        200,
        serde_json::json!({
            "symbol": "BTCUSDT",
            "orderId": 9702,
            "orderListId": -1,
            "clientOrderId": "spot-amb-2",
            "price": "60100",
            "origQty": "0.003",
            "executedQty": "0",
            "cummulativeQuoteQty": "0",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY"
        }),
    );
    clock.add_ns(5_000_000);
    let res_1 = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
    assert!(matches!(
        res_1.as_slice(),
        [QueryResolution::Resolved { order }] if order == &order_key
    ));
    let q_reqs_1 = query_server.recorded_requests();
    assert!(
        q_reqs_1[0].contains("origClientOrderId=spot-amb-2"),
        "{q_reqs_1:?}"
    );
    assert!(!owner.account().frozen());
    assert_eq!(owner.account().order_in_doubt(&order_key), None);
    assert_eq!(
        owner
            .account()
            .order(&order_key)
            .unwrap()
            .effective_quantity,
        Fixed::parse("0.003", 8).unwrap()
    );
    assert_eq!(
        owner
            .account()
            .order_terms(&order_key)
            .unwrap()
            .1
            .client_id
            .as_str(),
        "spot-amb-2"
    );

    // 3. Spot Modify #2 ("spot-amb-2" -> "spot-amb-3", BUY 0.004 @ 60200) drops TLS -> AmendAmbiguous,
    // where cancelReplace did NOT execute on Spot (-2013 on "spot-amb-3", fallback 200 OK on orderId=9702)!
    order_server.push_drop();
    let now_3 = clock.add_ns(10_000);
    auth.policy.lock().unwrap().gate.now = now_3;
    let cmd_mod_2 = make_modify_cmd(
        &base_cmd,
        8,
        4,
        3,
        3,
        oid_1,
        "spot-amb-2",
        Some(9702),
        Some("spot-amb-3"),
        v::Side::Buy,
        "0.004",
        "60200",
        now_3,
    );
    owner
        .submit(cmd_mod_2, &[], now_3, gateway::until())
        .unwrap();
    assert_eq!(
        drive_rest_outcome(&mut owner, &clock).disposition,
        RestDisposition::Unknown
    );
    reconnect_spot(&mut owner);

    query_server.push_json(
        400,
        serde_json::json!({ "code": -2013, "msg": "Order does not exist." }),
    );
    query_server.push_json(
        200,
        serde_json::json!({
            "symbol": "BTCUSDT",
            "orderId": 9702,
            "orderListId": -1,
            "clientOrderId": "spot-amb-2",
            "price": "60100",
            "origQty": "0.003",
            "executedQty": "0",
            "cummulativeQuoteQty": "0",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY"
        }),
    );
    clock.add_ns(5_000_000);
    let res_2 = drive_query_resolution(&mut driver, &mut owner, &creds, &mut clock);
    assert!(matches!(
        res_2.as_slice(),
        [QueryResolution::Resolved { order }] if order == &order_key
    ));
    let q_reqs_2 = query_server.recorded_requests();
    assert!(
        q_reqs_2[1].contains("origClientOrderId=spot-amb-3"),
        "{q_reqs_2:?}"
    );
    assert!(q_reqs_2[2].contains("orderId=9702"), "{q_reqs_2:?}");
    assert!(!owner.account().frozen());
    assert_eq!(owner.account().order_in_doubt(&order_key), None);
    assert_eq!(
        owner
            .account()
            .order(&order_key)
            .unwrap()
            .effective_quantity,
        Fixed::parse("0.003", 8).unwrap()
    );

    // 4. Spot Modify #3 ("spot-amb-2" -> "spot-amb-4", BUY 0.005 @ 60050) drops TLS, and Private WS
    // delivers leg-1 CANCELED ("spot-amb-2", 9702) + leg-2 NEW ("spot-amb-4", 9704) before query!
    order_server.push_drop();
    let now_4 = clock.add_ns(10_000);
    auth.policy.lock().unwrap().gate.now = now_4;
    let cmd_mod_3 = make_modify_cmd(
        &base_cmd,
        8,
        4,
        4,
        4,
        oid_1,
        "spot-amb-2",
        Some(9702),
        Some("spot-amb-4"),
        v::Side::Buy,
        "0.005",
        "60050",
        now_4,
    );
    owner
        .submit(cmd_mod_3, &[], now_4, gateway::until())
        .unwrap();
    assert_eq!(
        drive_rest_outcome(&mut owner, &clock).disposition,
        RestDisposition::Unknown
    );
    let ws_leg1_canceled = serde_json::to_vec(&serde_json::json!({
        "subscriptionId": 0,
        "event": {
            "e": "executionReport",
            "E": 400,
            "s": "BTCUSDT",
            "c": "spot-amb-2",
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "0.003",
            "p": "60100",
            "g": -1,
            "C": "spot-amb-2",
            "x": "CANCELED",
            "X": "CANCELED",
            "i": 9702,
            "l": "0",
            "z": "0",
            "L": "0",
            "T": 399,
            "t": -1,
            "I": 20,
            "m": false,
            "Z": "0",
            "Y": "0",
            "r": "NONE"
        }
    }))
    .unwrap();
    let ws_leg2_new = serde_json::to_vec(&serde_json::json!({
        "subscriptionId": 0,
        "event": {
            "e": "executionReport",
            "E": 401,
            "s": "BTCUSDT",
            "c": "spot-amb-4",
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "0.005",
            "p": "60050",
            "g": -1,
            "C": "",
            "x": "NEW",
            "X": "NEW",
            "i": 9704,
            "l": "0",
            "z": "0",
            "L": "0",
            "T": 400,
            "t": -1,
            "I": 21,
            "m": false,
            "Z": "0",
            "Y": "0",
            "r": "NONE"
        }
    }))
    .unwrap();
    deliver_private_frames(
        &mut owner,
        Market::Spot,
        vec![ws_leg1_canceled, ws_leg2_new],
        &mut clock,
        Arc::clone(&client_tls),
        Arc::clone(&server_tls),
    );
    assert!(!owner.account().frozen());
    let view_after = owner.account().order(&order_key).unwrap();
    assert!(!view_after.terminal);
    assert_eq!(
        view_after.effective_quantity,
        Fixed::parse("0.005", 8).unwrap()
    );
    assert_eq!(
        owner
            .account()
            .order_terms(&order_key)
            .unwrap()
            .1
            .client_id
            .as_str(),
        "spot-amb-4"
    );

    // Verify full WAL recovery replays both OrderQuery and out-of-order Spot WS Modified cleanly!
    driver.stop();
    drop(driver);
    drop(owner);
    let mut rec_gw_cfg = gateway::config(&baseline);
    rec_gw_cfg.writer_epoch = 5;
    let mut rec_gw = AccountGateway::recover(
        FileBackend::open(&account_dir.0).unwrap(),
        rec_gw_cfg,
        auth,
        RealRestEncoder {
            market: Market::Spot,
            host_override: "testnet.binance.vision",
        },
        None,
    )
    .unwrap()
    .with_per_order_doubt_isolation(true);
    let mut rec_ing_cfg = ingress_config(&baseline);
    rec_ing_cfg.writer_epoch = 5;
    let mut rec_ing =
        AccountIngress::recover(FileStore::open(&inbox_dir.0).unwrap(), rec_ing_cfg, None).unwrap();
    rec_ing.replay_pending(&mut rec_gw, 32).unwrap();
    assert!(rec_gw.account().frozen());
    assert!(!rec_gw.account().order(&order_key).unwrap().terminal);
    assert_eq!(
        rec_gw
            .account()
            .order_terms(&order_key)
            .unwrap()
            .1
            .client_id
            .as_str(),
        "spot-amb-4"
    );
    assert_eq!(
        rec_gw
            .account()
            .order(&order_key)
            .unwrap()
            .effective_quantity,
        Fixed::parse("0.005", 8).unwrap()
    );
}
