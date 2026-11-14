#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod gateway;

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
use tickwire_account::{Admission, DoubtReason, IntentKey, OrderKey, QueryConfig};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::{
    AccountGateway, AdmissionRequest, Authority, AuthorityError, CommitState, EncodedRequest,
    Encoder, Gate, Status,
    pool::{MAX_SHARED_INFLIGHT, OrderSlotSpec, RouterConfig, SendSlot},
};
use tickwire_account_journal::{AppendReceipt, FileBackend};
use tickwire_binance_account::{
    AccountIngress, Config, Error as AccountError,
    rest::{RestConfig, RestDisposition, RestOutcome, RestOwner},
    ws::{
        ControlAction, DurableWsBinding, DurableWsSession, SessionState, SignedWire, WsConfig,
        WsRequestPlan,
    },
};
use tickwire_clock::{ClockDomain, Source, TimeUnit};
use tickwire_journal::store::FileStore;
use tickwire_quota::rate_limiter::{IpKey, RateReject, VenuePreset, VenueRateLimiter};
use tickwire_transport::{
    Connection, IoBudget, Limits, Security,
    dialer::{DialBackoff, DialSecurity, DialSpec, DialTransport, Dialer},
    rustls,
    ws::{Opcode, encode_client},
};
use tickwire_venue as v;

#[derive(Clone, Debug)]
enum ServerStep {
    Respond {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
        close_after: bool,
    },
    DropAfterRequest,
}

struct ScriptedTlsHttpServer {
    address: SocketAddr,
    steps: Arc<Mutex<VecDeque<ServerStep>>>,
    requests: Arc<Mutex<Vec<String>>>,
    connections_accepted: Arc<AtomicU64>,
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
        let connections_accepted = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let steps_w = Arc::clone(&steps);
        let reqs_w = Arc::clone(&requests);
        let accepted_w = Arc::clone(&connections_accepted);
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
                accepted_w.fetch_add(1, Ordering::AcqRel);
                let server_cfg_c = Arc::clone(&server_cfg);
                let steps_c = Arc::clone(&steps_w);
                let reqs_c = Arc::clone(&reqs_w);
                let stop_c = Arc::clone(&stop_w);
                conn_threads.push(thread::spawn(move || {
                    socket.set_nonblocking(false).unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_millis(20)))
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
                                    thread::sleep(Duration::from_millis(1));
                                };
                                let Some(action) = action else {
                                    break;
                                };
                                match action {
                                    ServerStep::Respond {
                                        status,
                                        headers,
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
                                        let conn_hdr =
                                            if close_after { "close" } else { "keep-alive" };
                                        let mut extra_hdrs = String::new();
                                        for (k, v) in headers {
                                            extra_hdrs.push_str(&format!("{k}: {v}\r\n"));
                                        }
                                        let header = format!(
                                            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: {conn_hdr}\r\n{extra_hdrs}\r\n",
                                            body.len()
                                        );
                                        if stream.write_all(header.as_bytes()).is_err()
                                            || stream.write_all(&body).is_err()
                                            || stream.flush().is_err()
                                        {
                                            break;
                                        }
                                        if close_after {
                                            stream.conn.send_close_notify();
                                            let _ = stream.flush();
                                            let _ = stream.sock.shutdown(Shutdown::Write);
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
            connections_accepted,
            stop,
            handle: Some(handle),
        }
    }

    fn push_json(&self, status: u16, value: serde_json::Value) {
        self.push_json_with_headers(status, Vec::new(), value);
    }

    fn push_json_with_headers(
        &self,
        status: u16,
        headers: Vec<(&str, &str)>,
        value: serde_json::Value,
    ) {
        self.steps.lock().unwrap().push_back(ServerStep::Respond {
            status,
            headers: headers
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
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

    fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }

    fn accepted_count(&self) -> u64 {
        self.connections_accepted.load(Ordering::Acquire)
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

fn connect_tls(
    addr: SocketAddr,
    local_ip: Option<IpAddr>,
    host: &'static str,
    client_tls: &Arc<rustls::ClientConfig>,
) -> Connection {
    let security = Security::Tls {
        server_name: rustls::pki_types::ServerName::try_from(host).unwrap(),
        config: Arc::clone(client_tls),
    };
    Connection::connect_from(
        addr,
        local_ip,
        security,
        Limits::default(),
        gateway::until(),
    )
    .unwrap()
}

fn build_spot_baseline() -> (CanonicalBaseline, v::Command) {
    let (mut snapshot, mut catalog, base_cmd) = gateway::fixture::fixture();
    snapshot.valid_until = u64::MAX / 2;
    snapshot.assets[1].free = gateway::fixture::units(1_000_000);
    catalog.entries[0]
        .strategy_limits
        .push((9, gateway::fixture::units(1_000)));
    (CanonicalBaseline::new(snapshot, catalog).unwrap(), base_cmd)
}

fn ingress_config(baseline: &CanonicalBaseline) -> Config {
    Config {
        physical: tickwire_journal::Config::new([81; 16], [1; 16]),
        writer_epoch: 4,
        baseline: baseline.clone(),
        account_stream: [2; 16],
        max_records: 256,
        income_association_authority: None,
        income_baseline_authority: None,
        max_registrations: 128,
        max_raw_bytes: 32_768,
        max_retained_bytes: 1 << 20,
    }
}

fn make_new_cmd(
    base_cmd: &v::Command,
    intent_id: u64,
    client_id: &str,
    now_ns: u64,
) -> (v::CanonicalCommand, OrderKey, [u8; 16]) {
    let mut cmd = base_cmd.clone();
    cmd.context.strategy.logical_strategy_id = 8;
    cmd.context.strategy.instance_id = [8; 16];
    cmd.context.strategy.strategy_intent_seq = intent_id;
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
    spec.time_in_force = Some(v::TimeInForce::Gtc);
    cmd.body = v::CommandBody::New(spec);
    let exec_epoch = cmd.context.request.intent_exec_epoch;
    let canonical = v::CanonicalCommand::new(cmd).unwrap();
    let mut local_oid = [0u8; 16];
    local_oid[..8].copy_from_slice(&intent_id.to_le_bytes());
    local_oid[8] = 1;
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

fn make_target_cmd(
    base_cmd: &v::Command,
    intent_id: u64,
    is_cancel: bool,
    target_order_id: [u8; 16],
    client_id: &str,
    venue_order_id: u64,
    now_ns: u64,
) -> v::CanonicalCommand {
    let mut cmd = base_cmd.clone();
    cmd.context.strategy.logical_strategy_id = 8;
    cmd.context.strategy.instance_id = [8; 16];
    cmd.context.strategy.strategy_intent_seq = intent_id;
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
    let target = v::OrderTarget {
        order_id: target_order_id,
        expected_owner_revision: 1,
        original_intent_exec_epoch: cmd.context.request.intent_exec_epoch,
        client_id: Some(v::ClientId::new(client_id).unwrap()),
        venue_order_id: Some(v::VenueOrderId::new(venue_order_id.to_string()).unwrap()),
    };
    cmd.body = if is_cancel {
        v::CommandBody::Cancel(target)
    } else {
        v::CommandBody::Query(target)
    };
    v::CanonicalCommand::new(cmd).unwrap()
}

fn spot_order_json(client_id: &str, order_id: u64, status: &str) -> serde_json::Value {
    serde_json::json!({
        "symbol": "BTCUSDT",
        "orderId": order_id,
        "orderListId": -1,
        "clientOrderId": client_id,
        "transactTime": 1_770_000_000_100u64,
        "price": "60000",
        "origQty": "0.001",
        "executedQty": "0",
        "cummulativeQuoteQty": "0",
        "status": status,
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "fills": []
    })
}

fn create_pooled_owner(
    baseline: &CanonicalBaseline,
    account_dir: &gateway::Directory,
    inbox_dir: &gateway::Directory,
    router_cfg: RouterConfig,
) -> (
    RestOwner<FileStore, FileBackend, gateway::MockAuthority, gateway::TestEncoder>,
    gateway::MockAuthority,
) {
    let mut gw_cfg = gateway::config(baseline);
    gw_cfg.max_intents = 128;
    let auth = gateway::MockAuthority::default();
    {
        let mut p = auth.policy.lock().unwrap();
        p.gate.now = 1_000_000;
        p.gate.lease_until = u64::MAX / 2;
    }
    let (boot_conn, boot_peer) = gateway::peer("testnet.binance.vision", 16_384);
    let gw = AccountGateway::create(
        FileBackend::open(&account_dir.0).unwrap(),
        gw_cfg,
        baseline.clone(),
        auth.clone(),
        gateway::TestEncoder,
        boot_conn,
    )
    .unwrap();
    let ingress = AccountIngress::create(
        FileStore::open(&inbox_dir.0).unwrap(),
        ingress_config(baseline),
    )
    .unwrap();
    let owner = RestOwner::from_parts(
        ingress,
        gw,
        RestConfig {
            connection_epoch: 10,
            max_response_bytes: 32_768,
            max_headers: 32,
        },
    )
    .unwrap()
    .with_per_order_query_desk(QueryConfig::default())
    .with_connection_pool(router_cfg)
    .unwrap();
    let _ = boot_peer.join();
    (owner, auth)
}

fn flush_pool_requests(
    owner: &mut RestOwner<FileStore, FileBackend, gateway::MockAuthority, gateway::TestEncoder>,
    servers: &[&ScriptedTlsHttpServer],
    expected_total_requests: usize,
    mono_ns: u64,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for pooled HTTP requests to flush"
        );
        let _ = owner
            .poll(Instant::now(), mono_ns, IoBudget::default())
            .unwrap();
        let total: usize = servers.iter().map(|s| s.request_count()).sum();
        if total >= expected_total_requests {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn wait_next_outcome(
    owner: &mut RestOwner<FileStore, FileBackend, gateway::MockAuthority, gateway::TestEncoder>,
    mono_ns: u64,
) -> RestOutcome {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for pooled RestOutcome"
        );
        if let Some(out) = owner
            .poll(Instant::now(), mono_ns, IoBudget::default())
            .unwrap()
        {
            return out;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

// ---------------------------------------------------------------------------
// Scenario 1: 3-Slot Exclusive HTTP/1.1 Pool + EWMA Router + 9/8 Hysteresis
// ---------------------------------------------------------------------------
#[test]
fn three_slot_exclusive_http_pool_ewma_router_warmup_convergence_and_hysteresis() {
    let (client_tls, server_tls) = spot_certs();
    let server_0 = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));
    let server_1 = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));
    let server_2 = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));

    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_spot_baseline();
    let (mut owner, auth) =
        create_pooled_owner(&baseline, &account_dir, &inbox_dir, RouterConfig::default());

    for srv in [&server_0, &server_1, &server_2] {
        let conn = connect_tls(srv.address, None, "testnet.binance.vision", &client_tls);
        owner
            .add_pool_slot(
                OrderSlotSpec::with_connection(SendSlot::Exclusive, conn),
                Limits::default(),
            )
            .unwrap();
    }
    assert!(owner.submission_ready());
    assert!(!owner.busy());

    let mut now_ns = 10_000_000u64;
    let mut next_id = 1u64;

    // 1. Submit 3 concurrent New orders simultaneously while servers hold replies.
    let mut round1_ids = Vec::new();
    for slot in 0..3u64 {
        auth.policy.lock().unwrap().gate.now = now_ns;
        let (cmd, _okey, loid) =
            make_new_cmd(&base_cmd, next_id, &format!("pool-c-{next_id}"), now_ns);
        let st = owner
            .submit(cmd, &[loid], now_ns, gateway::until())
            .unwrap();
        assert_eq!(st, Status::Queued);
        round1_ids.push((slot, next_id));
        next_id += 1;
    }
    // All 3 exclusive slots are now in flight -> 4th concurrent order returns Busy!
    assert!(owner.busy());
    assert!(!owner.submission_ready());
    let (cmd_busy, _, loid_busy) = make_new_cmd(&base_cmd, 999, "pool-busy-4", now_ns);
    assert!(matches!(
        owner.submit(cmd_busy, &[loid_busy], now_ns, gateway::until()),
        Err(AccountError::Busy)
    ));

    // Flush all 3 concurrent requests over TLS and verify unmeasured round-robin 0 -> 1 -> 2.
    flush_pool_requests(&mut owner, &[&server_0, &server_1, &server_2], 3, now_ns);
    assert_eq!(server_0.request_count(), 1);
    assert_eq!(server_1.request_count(), 1);
    assert_eq!(server_2.request_count(), 1);

    // Complete each slot in ascending RTT order so received_mono_ns stays strictly monotonic:
    // Slot 0 = 930 us, Slot 1 = 500 us (fastest), Slot 2 = 3_000 us.
    let rtts_ns = [930_000u64, 500_000u64, 3_000_000u64];
    let servers = [&server_0, &server_1, &server_2];
    for idx in [1usize, 0, 2] {
        let (_, oid) = round1_ids[idx];
        servers[idx].push_json(
            200,
            spot_order_json(&format!("pool-c-{oid}"), 1000 + oid, "NEW"),
        );
        let out = wait_next_outcome(&mut owner, now_ns + rtts_ns[idx]);
        assert_eq!(out.disposition, RestDisposition::Observed);
    }
    now_ns += 5_000_000;
    assert!(!owner.busy());

    // 2. Complete warmup rounds 2..=7 sequentially: while all 3 slots remain unmeasured
    // (< MIN_ROUTE_SAMPLES = 8), round-robin cycles strictly 0 -> 1 -> 2.
    for _round in 2..=7 {
        for slot_idx in 0..3usize {
            now_ns += 10_000_000;
            auth.policy.lock().unwrap().gate.now = now_ns;
            let oid = next_id;
            next_id += 1;
            let before_cnt = servers[slot_idx].request_count();
            let (cmd, _, loid) = make_new_cmd(&base_cmd, oid, &format!("pool-w-{oid}"), now_ns);
            owner
                .submit(cmd, &[loid], now_ns, gateway::until())
                .unwrap();
            let total_expected =
                server_0.request_count() + server_1.request_count() + server_2.request_count() + 1;
            flush_pool_requests(
                &mut owner,
                &[&server_0, &server_1, &server_2],
                total_expected,
                now_ns,
            );
            assert_eq!(
                servers[slot_idx].request_count(),
                before_cnt + 1,
                "warmup round-robin expected slot {slot_idx}"
            );
            servers[slot_idx].push_json(
                200,
                spot_order_json(&format!("pool-w-{oid}"), 1000 + oid, "NEW"),
            );
            let out = wait_next_outcome(&mut owner, now_ns + rtts_ns[slot_idx]);
            assert_eq!(out.disposition, RestDisposition::Observed);
        }
    }

    // Submit round 8 concurrently across all 3 slots so all 3 reach ema_samples == 8 together.
    now_ns += 10_000_000;
    auth.policy.lock().unwrap().gate.now = now_ns;
    let mut round8_ids = Vec::new();
    for _slot_idx in 0..3usize {
        let oid = next_id;
        next_id += 1;
        let (cmd, _, loid) = make_new_cmd(&base_cmd, oid, &format!("pool-w8-{oid}"), now_ns);
        owner
            .submit(cmd, &[loid], now_ns, gateway::until())
            .unwrap();
        round8_ids.push(oid);
    }
    flush_pool_requests(&mut owner, &[&server_0, &server_1, &server_2], 24, now_ns);
    for idx in [1usize, 0, 2] {
        let oid = round8_ids[idx];
        servers[idx].push_json(
            200,
            spot_order_json(&format!("pool-w8-{oid}"), 1000 + oid, "NEW"),
        );
        let out = wait_next_outcome(&mut owner, now_ns + rtts_ns[idx]);
        assert_eq!(out.disposition, RestDisposition::Observed);
    }
    now_ns += 5_000_000;

    assert_eq!(owner.gateway().slot_route(0).unwrap().health.ema_samples, 8);
    assert_eq!(owner.gateway().slot_route(1).unwrap().health.ema_samples, 8);
    assert_eq!(owner.gateway().slot_route(2).unwrap().health.ema_samples, 8);

    // 3. Convergence after 8 samples: last_conn_id was 2 (ema=3_000 us), while Slot 1 has
    // ema=500 us and Slot 0 has ema=930 us -> next idle order converges to fastest server_1!
    now_ns += 10_000_000;
    auth.policy.lock().unwrap().gate.now = now_ns;
    let before_s1 = server_1.request_count();
    let oid_conv = next_id;
    next_id += 1;
    let (cmd_conv, _, loid_conv) = make_new_cmd(
        &base_cmd,
        oid_conv,
        &format!("pool-conv-{oid_conv}"),
        now_ns,
    );
    owner
        .submit(cmd_conv, &[loid_conv], now_ns, gateway::until())
        .unwrap();
    flush_pool_requests(
        &mut owner,
        &[&server_0, &server_1, &server_2],
        before_s1 + server_0.request_count() + server_2.request_count() + 1,
        now_ns,
    );
    assert_eq!(server_1.request_count(), before_s1 + 1);
    server_1.push_json(
        200,
        spot_order_json(&format!("pool-conv-{oid_conv}"), 1000 + oid_conv, "NEW"),
    );
    // Complete with 4_500 us RTT so Slot 1's EWMA rises from 500 us to ~1_000 us
    // while Slot 0 stays at 930 us (faster, yet inside the 9/8 hysteresis band!).
    let _ = wait_next_outcome(&mut owner, now_ns + 4_500_000);
    let s0_ewma = owner.gateway().slot_route(0).unwrap().health.ema_us;
    let s1_ewma_now = owner.gateway().slot_route(1).unwrap().health.ema_us;
    assert!(
        s0_ewma < s1_ewma_now,
        "expected s0 ({s0_ewma}) faster than s1 ({s1_ewma_now})"
    );
    assert!(
        u64::from(s0_ewma) * 9 >= u64::from(s1_ewma_now) * 8,
        "expected inside 9/8 hysteresis band: s0={s0_ewma}, s1={s1_ewma_now}"
    );

    // 4. 9/8 Hysteresis stickiness:
    // Both Slot 0 (930 us) and incumbent Slot 1 (~1_000 us) are idle -> next order MUST
    // stick to incumbent Slot 1 without flapping!
    now_ns += 10_000_000;
    auth.policy.lock().unwrap().gate.now = now_ns;
    let before_s1_hyst = server_1.request_count();
    let hyst_oid = next_id;
    next_id += 1;
    let (cmd_hyst, _, loid_hyst) = make_new_cmd(
        &base_cmd,
        hyst_oid,
        &format!("pool-hyst-{hyst_oid}"),
        now_ns,
    );
    owner
        .submit(cmd_hyst, &[loid_hyst], now_ns, gateway::until())
        .unwrap();
    let tot_hyst =
        server_0.request_count() + server_1.request_count() + server_2.request_count() + 1;
    flush_pool_requests(
        &mut owner,
        &[&server_0, &server_1, &server_2],
        tot_hyst,
        now_ns,
    );
    assert_eq!(
        server_1.request_count(),
        before_s1_hyst + 1,
        "9/8 hysteresis must keep incumbent server_1 without flapping to marginally faster server_0"
    );
    server_1.push_json(
        200,
        spot_order_json(&format!("pool-hyst-{hyst_oid}"), 1000 + hyst_oid, "NEW"),
    );
    // Complete with 3_000 us RTT so Slot 1's EWMA exceeds 930 * 9 / 8 (~1_046 us),
    // crossing outside the 9/8 hysteresis band!
    let _ = wait_next_outcome(&mut owner, now_ns + 3_000_000);
    let s1_ewma_slow = owner.gateway().slot_route(1).unwrap().health.ema_us;
    assert!(
        u64::from(s0_ewma) * 9 < u64::from(s1_ewma_slow) * 8,
        "expected outside 9/8 band: s0={s0_ewma}, s1_slow={s1_ewma_slow}"
    );

    // 5. Once outside the 9/8 band, router cleanly switches from Slot 1 to Slot 0!
    now_ns += 10_000_000;
    auth.policy.lock().unwrap().gate.now = now_ns;
    let before_s0_switch = server_0.request_count();
    let sw_oid = next_id;
    let (cmd_sw, _, loid_sw) =
        make_new_cmd(&base_cmd, sw_oid, &format!("pool-switch-{sw_oid}"), now_ns);
    owner
        .submit(cmd_sw, &[loid_sw], now_ns, gateway::until())
        .unwrap();
    let tot_sw = server_0.request_count() + server_1.request_count() + server_2.request_count() + 1;
    flush_pool_requests(
        &mut owner,
        &[&server_0, &server_1, &server_2],
        tot_sw,
        now_ns,
    );
    assert_eq!(server_0.request_count(), before_s0_switch + 1);
    server_0.push_json(
        200,
        spot_order_json(&format!("pool-switch-{sw_oid}"), 1000 + sw_oid, "NEW"),
    );
    let _ = wait_next_outcome(&mut owner, now_ns + 930_000);
}

// ---------------------------------------------------------------------------
// Scenario 2: Single-Link Mid-Request Drop Preserves Sibling Links + Auto-Redial
// ---------------------------------------------------------------------------
#[test]
fn single_link_mid_request_drop_isolates_order_preserves_siblings_and_auto_redials() {
    let (client_tls, server_tls) = spot_certs();
    let server_0 = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));
    let server_1 = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));
    let server_2 = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));

    let clock_ns = Arc::new(AtomicU64::new(10_000_000));
    let clock_for_dialer = Arc::clone(&clock_ns);
    let dialer =
        Dialer::with_clock(Arc::new(move || clock_for_dialer.load(Ordering::SeqCst))).unwrap();

    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_spot_baseline();
    let (mut owner, auth) =
        create_pooled_owner(&baseline, &account_dir, &inbox_dir, RouterConfig::default());

    let backoff = DialBackoff::new(5, 50);
    for (slot_id, srv) in [(&server_0), (&server_1), (&server_2)]
        .into_iter()
        .enumerate()
    {
        let baton = dialer
            .add_slot(
                slot_id as u64,
                DialSpec {
                    host: "testnet.binance.vision".into(),
                    port: srv.address.port(),
                    initial_peers: vec![srv.address],
                    local_ip: None,
                    security: DialSecurity::Tls {
                        server_name: rustls::pki_types::ServerName::try_from(
                            "testnet.binance.vision",
                        )
                        .unwrap(),
                        config: Arc::clone(&client_tls),
                    },
                    limits: Limits::default(),
                    transport: DialTransport::PlainOrTls,
                    connect_timeout: Duration::from_secs(2),
                    dial_timeout: Duration::from_secs(2),
                    backoff,
                },
            )
            .unwrap();
        let conn = connect_tls(srv.address, None, "testnet.binance.vision", &client_tls);
        let mut spec = OrderSlotSpec::with_baton(SendSlot::Exclusive, baton, backoff);
        spec.connection = Some(conn);
        spec.peer_addr = Some(srv.address);
        owner.add_pool_slot(spec, Limits::default()).unwrap();
    }

    let initial_epoch_0 = owner.gateway().slot_route(0).unwrap().epoch;

    // Server 0 will drop TCP mid-request right after receiving Order 1!
    server_0.push_drop();

    let now_1 = clock_ns.load(Ordering::SeqCst);
    auth.policy.lock().unwrap().gate.now = now_1;
    let (cmd_1, okey_1, loid_1) = make_new_cmd(&base_cmd, 1, "drop-slot0-1", now_1);
    owner
        .submit(cmd_1, &[loid_1], now_1, gateway::until())
        .unwrap();

    let out_1 = wait_next_outcome(&mut owner, now_1 + 1_000_000);
    assert_eq!(out_1.disposition, RestDisposition::Unknown);
    assert!(!owner.account().frozen());
    assert!(!owner.gateway().halted());
    assert_eq!(
        owner.account().order_in_doubt(&okey_1),
        Some(DoubtReason::PlaceAmbiguous)
    );
    assert!(owner.query_desk().unwrap().ask_kind(&okey_1).is_some());

    // Sibling slots 1 and 2 immediately accept and complete concurrent orders!
    let now_2 = now_1 + 2_000_000;
    clock_ns.store(now_2, Ordering::SeqCst);
    auth.policy.lock().unwrap().gate.now = now_2;
    server_1.push_json(200, spot_order_json("sib-slot1-2", 2002, "NEW"));
    server_2.push_json(200, spot_order_json("sib-slot2-3", 2003, "NEW"));

    let (cmd_2, okey_2, loid_2) = make_new_cmd(&base_cmd, 2, "sib-slot1-2", now_2);
    let (cmd_3, okey_3, loid_3) = make_new_cmd(&base_cmd, 3, "sib-slot2-3", now_2);
    owner
        .submit(cmd_2, &[loid_2], now_2, gateway::until())
        .unwrap();
    owner
        .submit(cmd_3, &[loid_3], now_2, gateway::until())
        .unwrap();

    let out_a = wait_next_outcome(&mut owner, now_2 + 500_000);
    let out_b = wait_next_outcome(&mut owner, now_2 + 500_000);
    assert_eq!(out_a.disposition, RestDisposition::Observed);
    assert_eq!(out_b.disposition, RestDisposition::Observed);
    assert_eq!(owner.account().order_in_doubt(&okey_2), None);
    assert_eq!(owner.account().order_in_doubt(&okey_3), None);
    assert_eq!(server_1.request_count(), 1);
    assert_eq!(server_2.request_count(), 1);

    // Advance clock past 5ms backoff so background Dialer reconnects Slot 0 automatically!
    let now_redial = now_2 + 25_000_000;
    clock_ns.store(now_redial, Ordering::SeqCst);
    let redial_deadline = Instant::now() + Duration::from_secs(5);
    while !owner.gateway().slot_route(0).unwrap().established {
        assert!(
            Instant::now() < redial_deadline,
            "timed out waiting for background Dialer to reconnect slot 0"
        );
        owner.poll_pool_dials(now_redial);
        thread::sleep(Duration::from_millis(2));
    }
    assert!(owner.gateway().slot_route(0).unwrap().epoch > initial_epoch_0);
    assert!(server_0.accepted_count() >= 2);

    // Verify reconnected Slot 0 serves new orders cleanly!
    auth.policy.lock().unwrap().gate.now = now_redial;
    server_0.push_json(200, spot_order_json("redialed-slot0-4", 2004, "NEW"));
    let (cmd_4, _, loid_4) = make_new_cmd(&base_cmd, 4, "redialed-slot0-4", now_redial);
    owner
        .submit(cmd_4, &[loid_4], now_redial, gateway::until())
        .unwrap();
    let out_4 = wait_next_outcome(&mut owner, now_redial + 400_000);
    assert_eq!(out_4.disposition, RestDisposition::Observed);
    assert_eq!(server_0.request_count(), 2);

    dialer.stop();
}

// ---------------------------------------------------------------------------
// Scenario 3: Multi-IP & Class-Isolated Rate Limiter + Per-IP 429 Failover
// ---------------------------------------------------------------------------
#[test]
fn multi_ip_class_isolated_rate_limiter_and_per_ip_429_failover() {
    let (client_tls, server_tls) = spot_certs();
    let server_a = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));
    let server_b = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));

    let ip_a = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let ip_b = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
    let key_a = IpKey::from_ip(ip_a);
    let key_b = IpKey::from_ip(ip_b);

    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_spot_baseline();
    let limiter = VenueRateLimiter::new(VenuePreset::BinanceSpot, 2, &[key_a, key_b]).unwrap();
    let (mut owner, auth) =
        create_pooled_owner(&baseline, &account_dir, &inbox_dir, RouterConfig::default());
    owner.set_venue_rate_limiter(limiter);

    // Slot 0 bound to IP_A (127.0.0.1), Slot 1 bound to IP_B (127.0.0.2).
    let conn_a = connect_tls(
        server_a.address,
        Some(ip_a),
        "testnet.binance.vision",
        &client_tls,
    );
    let mut spec_a = OrderSlotSpec::with_connection(SendSlot::Exclusive, conn_a);
    spec_a.local_ip = Some(ip_a);
    owner.add_pool_slot(spec_a, Limits::default()).unwrap();

    let conn_b = connect_tls(
        server_b.address,
        Some(ip_b),
        "testnet.binance.vision",
        &client_tls,
    );
    let mut spec_b = OrderSlotSpec::with_connection(SendSlot::Exclusive, conn_b);
    spec_b.local_ip = Some(ip_b);
    owner.add_pool_slot(spec_b, Limits::default()).unwrap();

    // Start at t0 = 10s (start of [10s, 20s) Spot 10s ORDER window, limit = 100).
    let t0 = 10_000_000_000u64;
    auth.policy.lock().unwrap().gate.now = t0;

    // 1. Place resting Order 1; server_a reports X-MBX-ORDER-COUNT-10S: 100, exhausting the
    // 10s ORDER cap (100/100) via conservative exchange header reconciliation!
    server_a.push_json_with_headers(
        200,
        vec![
            ("X-MBX-ORDER-COUNT-10S", "100"),
            ("X-MBX-USED-WEIGHT-1M", "42"),
        ],
        spot_order_json("resting-1", 9001, "NEW"),
    );
    let (cmd_1, _okey_1, loid_1) = make_new_cmd(&base_cmd, 1, "resting-1", t0);
    owner
        .submit(cmd_1, &[loid_1], t0, gateway::until())
        .unwrap();
    let out_1 = wait_next_outcome(&mut owner, t0 + 500_000);
    assert_eq!(out_1.disposition, RestDisposition::Observed);

    // 2. 101st New order in the same 10s window MUST fail locally with Error::RateLimited
    // BEFORE mutating AccountJournal or AccountIngress!
    let acct_pos_before = owner.gateway().journal_position();
    let ing_pos_before = owner.ingress().position();
    let t_exhausted = t0 + 1_000_000;
    auth.policy.lock().unwrap().gate.now = t_exhausted;
    let (cmd_101, _, loid_101) = make_new_cmd(&base_cmd, 2, "exhausted-new-101", t_exhausted);
    let err_101 = owner
        .submit(cmd_101, &[loid_101], t_exhausted, gateway::until())
        .unwrap_err();
    assert!(
        matches!(
            err_101,
            AccountError::RateLimited(RateReject::Exhausted { .. })
        ),
        "expected RateLimited(Exhausted), got {err_101:?}"
    );
    assert_eq!(owner.gateway().journal_position(), acct_pos_before);
    assert_eq!(owner.ingress().position(), ing_pos_before);

    // 3. While ORDER window is 100/100 exhausted, Query AND Cancel on resting-1 MUST succeed
    // immediately because Cancel and Query never check or debit ORDER windows!
    let t_query = t0 + 2_000_000;
    auth.policy.lock().unwrap().gate.now = t_query;
    server_b.push_json(200, spot_order_json("resting-1", 9001, "NEW"));
    let query_cmd = make_target_cmd(&base_cmd, 3, false, loid_1, "resting-1", 9001, t_query);
    owner
        .submit(query_cmd, &[], t_query, gateway::until())
        .unwrap();
    let out_query = wait_next_outcome(&mut owner, t_query + 500_000);
    assert_eq!(out_query.disposition, RestDisposition::Observed);

    let t_cancel = t0 + 3_000_000;
    auth.policy.lock().unwrap().gate.now = t_cancel;
    server_a.push_json(200, spot_order_json("resting-1", 9001, "CANCELED"));
    let cancel_cmd = make_target_cmd(&base_cmd, 4, true, loid_1, "resting-1", 9001, t_cancel);
    owner
        .submit(cancel_cmd, &[], t_cancel, gateway::until())
        .unwrap();
    let out_cancel = wait_next_outcome(&mut owner, t_cancel + 500_000);
    assert_eq!(out_cancel.disposition, RestDisposition::Observed);

    // 4. Advance into fresh 10s ORDER window [20s, 30s).
    // Route next New order to IP_A (Slot 0, next round-robin after Slot 1 -> 0 -> now Slot 1,
    // so submit 1 order on Slot 1 first or check request counts; after slot 0, 1, 0 above, next
    // unmeasured round-robin is Slot 1, so let's complete 1 quick order on Slot 1 so next hits Slot 0!).
    let t1 = 20_000_000_000u64;
    auth.policy.lock().unwrap().gate.now = t1;
    server_b.push_json(200, spot_order_json("pre-429-b", 9002, "NEW"));
    let (cmd_pre, _, loid_pre) = make_new_cmd(&base_cmd, 5, "pre-429-b", t1);
    owner
        .submit(cmd_pre, &[loid_pre], t1, gateway::until())
        .unwrap();
    let _ = wait_next_outcome(&mut owner, t1 + 500_000);

    // Now next order targets Slot 0 (IP_A = 127.0.0.1), which returns HTTP 429 + Retry-After: 5!
    let t_429 = t1 + 2_000_000;
    auth.policy.lock().unwrap().gate.now = t_429;
    let before_sa = server_a.request_count();
    server_a.push_json_with_headers(
        429,
        vec![("Retry-After", "5")],
        serde_json::json!({"code": -1003, "msg": "Too many requests"}),
    );
    let (cmd_429, okey_429, loid_429) = make_new_cmd(&base_cmd, 6, "hit-429-ipa", t_429);
    owner
        .submit(cmd_429, &[loid_429], t_429, gateway::until())
        .unwrap();
    let out_429 = wait_next_outcome(&mut owner, t_429 + 500_000);
    assert_eq!(server_a.request_count(), before_sa + 1);
    assert_eq!(out_429.disposition, RestDisposition::Unknown);
    assert!(
        !owner.account().frozen(),
        "per-IP 429 in multi-IP pool mode must isolate order without freezing Account"
    );
    assert!(!owner.gateway().halted());
    assert_eq!(
        owner.account().order_in_doubt(&okey_429),
        Some(DoubtReason::PlaceAmbiguous)
    );
    assert!(matches!(
        owner.venue_rate_limiter().unwrap().can_afford_place_orders(
            key_a,
            1,
            1,
            1,
            t_429 + 1_000_000
        ),
        Err(RateReject::IpCooldown { .. })
    ));

    // 5. Next New order immediately fails over away from cooled-down IP_A to healthy IP_B (server_b)!
    let t_failover = t_429 + 2_000_000;
    auth.policy.lock().unwrap().gate.now = t_failover;
    let before_sb = server_b.request_count();
    server_b.push_json(200, spot_order_json("failover-ipb", 9003, "NEW"));
    let (cmd_fo, okey_fo, loid_fo) = make_new_cmd(&base_cmd, 7, "failover-ipb", t_failover);
    owner
        .submit(cmd_fo, &[loid_fo], t_failover, gateway::until())
        .unwrap();
    let out_fo = wait_next_outcome(&mut owner, t_failover + 500_000);
    assert_eq!(server_b.request_count(), before_sb + 1);
    assert_eq!(server_a.request_count(), before_sa + 1);
    assert_eq!(out_fo.disposition, RestDisposition::Observed);
    assert_eq!(owner.account().order_in_doubt(&okey_fo), None);
}

// ---------------------------------------------------------------------------
// Scenario 4: Multiplexed WsOwner (16 Concurrent In-Flight + Out-of-Order Replies)
// ---------------------------------------------------------------------------
struct TestWsShared {
    plans: BTreeMap<v::Hash, WsRequestPlan>,
    wires: BTreeMap<v::Hash, SignedWire>,
    bindings: BTreeMap<v::Hash, DurableWsBinding>,
    frames: BTreeMap<v::Hash, Vec<u8>>,
}

#[derive(Clone)]
struct TestWsAdapter {
    inner: Rc<std::cell::RefCell<TestWsShared>>,
    mock_auth: gateway::MockAuthority,
}

impl TestWsAdapter {
    fn new(mock_auth: gateway::MockAuthority) -> Self {
        Self {
            inner: Rc::new(std::cell::RefCell::new(TestWsShared {
                plans: BTreeMap::new(),
                wires: BTreeMap::new(),
                bindings: BTreeMap::new(),
                frames: BTreeMap::new(),
            })),
            mock_auth,
        }
    }
}

impl SessionState for TestWsAdapter {
    fn prepare(&mut self, plan: WsRequestPlan) -> tickwire_binance_account::Result<()> {
        self.inner
            .borrow_mut()
            .plans
            .insert(plan.command().digest(), plan);
        Ok(())
    }

    fn signed_wire(&self, digest: v::Hash) -> tickwire_binance_account::Result<SignedWire> {
        self.inner
            .borrow()
            .wires
            .get(&digest)
            .cloned()
            .ok_or(AccountError::Identity)
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

impl Encoder for TestWsAdapter {
    fn encode(&mut self, c: &v::CanonicalCommand) -> tickwire_gateway::Result<EncodedRequest> {
        let digest = c.digest();
        let mut st = self.inner.borrow_mut();
        if let Some(frame) = st.frames.get(&digest) {
            return EncodedRequest::new(
                digest,
                "ws-api.testnet.binance.vision".into(),
                frame.clone(),
            );
        }
        let plan = st
            .plans
            .get(&digest)
            .cloned()
            .ok_or(tickwire_gateway::Error::Encoding)?;
        let (method, cid_str) = match &c.command().body {
            v::CommandBody::New(s) => ("order.place", s.client_id.as_str().to_owned()),
            v::CommandBody::Cancel(t) => (
                "order.cancel",
                t.client_id
                    .as_ref()
                    .map(|x| x.as_str().to_owned())
                    .unwrap_or_default(),
            ),
            _ => ("order.status", String::new()),
        };
        let payload = serde_json::to_vec(&serde_json::json!({
            "id": plan.request_id().as_str(),
            "method": method,
            "params": {
                "symbol": "BTCUSDT",
                "newClientOrderId": cid_str,
                "origClientOrderId": cid_str,
                "side": "BUY",
                "type": "LIMIT",
                "price": "60000",
                "quantity": "0.001"
            }
        }))
        .unwrap();
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
        let wire = SignedWire {
            source: Source::SpotTest,
            domain: ClockDomain::new([5; 16], 1).unwrap(),
            timestamp: 1_770_000_000_000,
            unit: TimeUnit::Milliseconds,
            window_ns: 5_000_000_000,
            request_hash: req_hash,
            frame_hash,
            request_bytes: payload.len() as u32,
            frame_bytes: frame.len() as u32,
        };
        st.wires.insert(digest, wire);
        st.frames.insert(digest, frame.clone());
        EncodedRequest::new(digest, "ws-api.testnet.binance.vision".into(), frame)
    }
}

impl Authority for TestWsAdapter {
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

#[test]
fn multiplexed_ws_owner_16_inflight_out_of_order_replies_and_capacity_release() {
    let (client_tls, server_tls) = spot_certs();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let received_count = Arc::new(AtomicU64::new(0));
    let release_replies = Arc::new(AtomicBool::new(false));
    let recv_c = Arc::clone(&received_count);
    let rel_c = Arc::clone(&release_replies);

    let server_thread = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut stream =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server_tls).unwrap(), socket);
        complete_ws_upgrade(&mut stream);

        // Read all 16 concurrent WS requests before sending a single response!
        let mut requests = Vec::with_capacity(16);
        for _ in 0..16 {
            let req = read_masked_ws_text(&mut stream);
            requests.push(req);
            recv_c.fetch_add(1, Ordering::AcqRel);
        }

        while !rel_c.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(1));
        }

        // Send all 16 replies strictly OUT OF ORDER (16 down to 1)!
        for (rev_idx, req) in requests.iter().enumerate().rev() {
            let cid = req["params"]["newClientOrderId"].as_str().unwrap();
            let reply = serde_json::json!({
                "id": req["id"],
                "status": 200,
                "result": spot_order_json(cid, 5000 + rev_idx as u64, "NEW"),
                "rateLimits": [
                    {
                        "rateLimitType": "ORDERS",
                        "interval": "SECOND",
                        "intervalNum": 10,
                        "limit": 100,
                        "count": 16
                    }
                ]
            });
            write_server_ws_text(&mut stream, &reply);
        }
        stream.flush().unwrap();

        // Receive 17th command (Cancel on ws-mux-1) after capacity frees up!
        let req_17 = read_masked_ws_text(&mut stream);
        assert_eq!(req_17["method"], "order.cancel");
        let cid_17 = req_17["params"]["origClientOrderId"].as_str().unwrap();
        let reply_17 = serde_json::json!({
            "id": req_17["id"],
            "status": 200,
            "result": spot_order_json(cid_17, 5000, "CANCELED")
        });
        write_server_ws_text(&mut stream, &reply_17);
        stream.flush().unwrap();
    });

    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_spot_baseline();
    let mut gw_cfg = gateway::config(&baseline);
    gw_cfg.route.authority = "ws-api.testnet.binance.vision".into();
    gw_cfg.max_intents = 64;

    let mock_auth = gateway::MockAuthority::default();
    {
        let mut p = mock_auth.policy.lock().unwrap();
        p.gate.now = 10_000_000;
        p.gate.lease_until = u64::MAX / 2;
    }
    let ws_adapter = TestWsAdapter::new(mock_auth.clone());

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
        tickwire_account_gateway::ConnectionConfig {
            address: addr,
            tls: Arc::clone(&client_tls),
            limits: Limits {
                max_send_bytes: 16_384,
                max_receive_bytes: 65_536,
                tls_buffer_bytes: 65_536,
                max_accept_bytes: 16_384,
            },
            connect_deadline: gateway::until(),
        },
    )
    .unwrap();

    let ip_ws = IpKey::from_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
    let limiter = VenueRateLimiter::new(VenuePreset::BinanceSpot, 2, &[ip_ws]).unwrap();

    let mut owner = RestOwner::from_parts_websocket(
        ingress,
        gw,
        session,
        Box::new(ws_adapter),
        WsConfig {
            lifetime_ns: 60_000_000_000,
            control_timeout_ns: 5_000_000_000,
            max_messages_per_poll: 32,
        },
        Instant::now(),
        10_000_000,
    )
    .unwrap()
    .with_per_order_query_desk(QueryConfig::default())
    .with_venue_rate_limiter(limiter);

    let budget = IoBudget {
        max_bytes: 65_536,
        max_syscalls: 64,
    };
    let upgrade_deadline = Instant::now() + Duration::from_secs(5);
    while !owner.websocket_ready() {
        assert!(Instant::now() < upgrade_deadline, "WS upgrade timed out");
        owner.poll(Instant::now(), 10_000_000, budget).unwrap();
        thread::sleep(Duration::from_millis(1));
    }

    // Submit 16 New orders back-to-back over the single upgraded WebSocket connection!
    let mut now_ns = 10_000_000_000u64;
    let mut first_loid = [0u8; 16];
    for idx in 1..=(MAX_SHARED_INFLIGHT as u64) {
        now_ns += 100_000;
        mock_auth.policy.lock().unwrap().gate.now = now_ns;
        let (cmd, _, loid) = make_new_cmd(&base_cmd, idx, &format!("ws-mux-{idx}"), now_ns);
        if idx == 1 {
            first_loid = loid;
        }
        let st = owner
            .submit(cmd, &[loid], now_ns, gateway::until())
            .unwrap();
        assert_eq!(st, Status::AwaitingCommit);

        // Flush this WS frame out to the TLS socket so gateway.pending clears for the next frame.
        let flush_deadline = Instant::now() + Duration::from_secs(5);
        while received_count.load(Ordering::Acquire) < idx {
            assert!(
                Instant::now() < flush_deadline,
                "timed out flushing WS request {idx}"
            );
            let out = owner.poll(Instant::now(), now_ns, budget).unwrap();
            assert!(out.is_none(), "server has not replied yet");
            thread::sleep(Duration::from_millis(1));
        }
    }

    assert_eq!(owner.websocket_inflight_count(), 16);
    assert!(owner.busy());
    assert!(!owner.submission_ready());

    // 17th concurrent submission while 16 are in-flight MUST return Error::Busy!
    let (cmd_17_busy, _, loid_17_busy) = make_new_cmd(&base_cmd, 99, "ws-mux-overflow", now_ns);
    assert!(matches!(
        owner.submit(cmd_17_busy, &[loid_17_busy], now_ns, gateway::until()),
        Err(AccountError::Busy)
    ));

    // Release all 16 out-of-order server responses (16 down to 1)!
    release_replies.store(true, Ordering::Release);
    let reply_mono = now_ns + 800_000;
    let mut outcomes = Vec::new();
    let drain_deadline = Instant::now() + Duration::from_secs(5);
    while outcomes.len() < 16 {
        assert!(
            Instant::now() < drain_deadline,
            "timed out draining 16 multiplexed WS replies (got {})",
            outcomes.len()
        );
        if let Some(out) = owner.poll(Instant::now(), reply_mono, budget).unwrap() {
            outcomes.push(out);
            while owner.has_queued_outcomes() {
                if let Some(queued) = owner.poll(Instant::now(), reply_mono, budget).unwrap() {
                    outcomes.push(queued);
                }
            }
        } else {
            thread::sleep(Duration::from_millis(1));
        }
    }

    assert_eq!(outcomes.len(), 16);
    for out in &outcomes {
        assert_eq!(out.disposition, RestDisposition::Observed);
        assert_eq!(out.applied_events, 1);
    }
    assert_eq!(owner.websocket_inflight_count(), 0);
    assert!(!owner.busy());
    assert!(owner.submission_ready());

    let rh = owner.websocket_route_health().unwrap();
    assert_eq!(rh.ema_samples, 16);
    assert_eq!(rh.inflight, 0);
    assert!(rh.ema_us >= 800, "ema_us={}", rh.ema_us);

    // Capacity is free again: submit 17th command (Cancel on ws-mux-1) and verify it succeeds!
    let cancel_mono = reply_mono + 500_000;
    mock_auth.policy.lock().unwrap().gate.now = cancel_mono;
    let cancel_cmd = make_target_cmd(
        &base_cmd,
        17,
        true,
        first_loid,
        "ws-mux-1",
        5000,
        cancel_mono,
    );
    owner
        .submit(cancel_cmd, &[], cancel_mono, gateway::until())
        .unwrap();
    let cancel_deadline = Instant::now() + Duration::from_secs(5);
    let cancel_out = loop {
        assert!(Instant::now() < cancel_deadline);
        if let Some(out) = owner
            .poll(Instant::now(), cancel_mono + 300_000, budget)
            .unwrap()
        {
            break out;
        }
        thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(cancel_out.disposition, RestDisposition::Observed);

    server_thread.join().unwrap();
}

#[test]
fn multiplexed_ws_disconnect_isolates_all_inflight_orders_into_query_desk_without_freezing() {
    let (client_tls, server_tls) = spot_certs();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let received_count = Arc::new(AtomicU64::new(0));
    let drop_now = Arc::new(AtomicBool::new(false));
    let recv_c = Arc::clone(&received_count);
    let drop_c = Arc::clone(&drop_now);

    let server_thread = thread::spawn(move || {
        let (socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut stream =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server_tls).unwrap(), socket);
        complete_ws_upgrade(&mut stream);

        for _ in 0..4 {
            let _ = read_masked_ws_text(&mut stream);
            recv_c.fetch_add(1, Ordering::AcqRel);
        }
        while !drop_c.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(1));
        }
        let _ = stream.sock.shutdown(Shutdown::Both);
    });

    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_spot_baseline();
    let mut gw_cfg = gateway::config(&baseline);
    gw_cfg.route.authority = "ws-api.testnet.binance.vision".into();
    gw_cfg.max_intents = 32;

    let mock_auth = gateway::MockAuthority::default();
    {
        let mut p = mock_auth.policy.lock().unwrap();
        p.gate.now = 10_000_000;
        p.gate.lease_until = u64::MAX / 2;
    }
    let ws_adapter = TestWsAdapter::new(mock_auth.clone());

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
        tickwire_account_gateway::ConnectionConfig {
            address: addr,
            tls: Arc::clone(&client_tls),
            limits: Limits {
                max_send_bytes: 16_384,
                max_receive_bytes: 65_536,
                tls_buffer_bytes: 65_536,
                max_accept_bytes: 16_384,
            },
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
            max_messages_per_poll: 32,
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

    let mut now_ns = 10_000_000_000u64;
    let mut okeys = Vec::new();
    for idx in 1..=4u64 {
        now_ns += 100_000;
        mock_auth.policy.lock().unwrap().gate.now = now_ns;
        let (cmd, okey, loid) = make_new_cmd(&base_cmd, idx, &format!("ws-drop-{idx}"), now_ns);
        okeys.push(okey);
        owner
            .submit(cmd, &[loid], now_ns, gateway::until())
            .unwrap();
        while received_count.load(Ordering::Acquire) < idx {
            assert!(Instant::now() < deadline);
            let _ = owner.poll(Instant::now(), now_ns, budget).unwrap();
            thread::sleep(Duration::from_millis(1));
        }
    }
    assert_eq!(owner.websocket_inflight_count(), 4);

    // Drop the WS TCP socket with 4 orders simultaneously in flight!
    drop_now.store(true, Ordering::Release);
    server_thread.join().unwrap();

    let mut drop_outcomes = Vec::new();
    let drop_deadline = Instant::now() + Duration::from_secs(5);
    while drop_outcomes.len() < 4 {
        assert!(Instant::now() < drop_deadline);
        if let Some(out) = owner
            .poll(Instant::now(), now_ns + 500_000, budget)
            .unwrap()
        {
            drop_outcomes.push(out);
            while owner.has_queued_outcomes() {
                if let Some(q) = owner
                    .poll(Instant::now(), now_ns + 500_000, budget)
                    .unwrap()
                {
                    drop_outcomes.push(q);
                }
            }
        } else {
            thread::sleep(Duration::from_millis(1));
        }
    }

    assert!(!owner.account().frozen());
    for out in &drop_outcomes {
        assert_eq!(out.disposition, RestDisposition::Unknown);
    }
    for okey in &okeys {
        assert_eq!(
            owner.account().order_in_doubt(okey),
            Some(DoubtReason::PlaceAmbiguous)
        );
        assert!(owner.query_desk().unwrap().ask_kind(okey).is_some());
    }
}

#[test]
fn pooled_rest_eof_ready_baton_and_abort_inflight_and_local_reject_quota() {
    let (client_tls, server_tls) = spot_certs();
    let server = ScriptedTlsHttpServer::spawn(Arc::clone(&server_tls));

    let clock_ns = Arc::new(AtomicU64::new(10_000_000));
    let clock_c = Arc::clone(&clock_ns);
    let dialer = Dialer::with_clock(Arc::new(move || clock_c.load(Ordering::SeqCst))).unwrap();

    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, base_cmd) = build_spot_baseline();
    let (mut owner, auth) =
        create_pooled_owner(&baseline, &account_dir, &inbox_dir, RouterConfig::default());

    let ip_a = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let ip_key_a = IpKey::from_ip(ip_a);
    let limiter = VenueRateLimiter::new(VenuePreset::BinanceSpot, 2, &[ip_key_a]).unwrap();
    owner.set_venue_rate_limiter(limiter);

    let backoff = DialBackoff::new(0, 5);
    let baton = dialer
        .add_slot(
            0,
            DialSpec {
                host: "testnet.binance.vision".into(),
                port: server.address.port(),
                initial_peers: vec![server.address],
                local_ip: Some(ip_a),
                security: DialSecurity::Tls {
                    server_name: rustls::pki_types::ServerName::try_from("testnet.binance.vision")
                        .unwrap(),
                    config: Arc::clone(&client_tls),
                },
                limits: Limits::default(),
                transport: DialTransport::PlainOrTls,
                connect_timeout: Duration::from_secs(2),
                dial_timeout: Duration::from_secs(2),
                backoff,
            },
        )
        .unwrap();
    let mut spec0 = OrderSlotSpec::with_baton(SendSlot::Shared, Arc::clone(&baton), backoff);
    spec0.local_ip = Some(ip_a);
    owner.add_pool_slot(spec0, Limits::default()).unwrap();

    let harvest_deadline = Instant::now() + Duration::from_secs(5);
    while !owner.gateway().has_slot_connection(0) {
        assert!(Instant::now() < harvest_deadline);
        owner.poll_pool_dials(10_000_000);
        thread::sleep(Duration::from_millis(1));
    }

    // 1. Local Account::admit rejection (InsufficientAsset) MUST NOT burn VenueRateLimiter quota!
    let mut now_ns = 20_000_000u64;
    auth.policy.lock().unwrap().gate.now = now_ns;
    let (mut huge_cmd_base, _, _) = make_new_cmd(&base_cmd, 1, "local-reject-huge", now_ns);
    let mut huge_raw = huge_cmd_base.command().clone();
    if let v::CommandBody::New(ref mut s) = huge_raw.body {
        s.price = Some("2000000000".parse().unwrap());
    }
    huge_cmd_base = v::CanonicalCommand::new(huge_raw).unwrap();
    assert!(
        owner
            .submit(huge_cmd_base, &[[7u8; 16]], now_ns, gateway::until())
            .is_err()
    );
    let vl = owner.venue_rate_limiter().unwrap();
    let ord_win = vl.account_order_window_ids()[0];
    let ip_win = vl.ip_window_id(ip_key_a).unwrap();
    assert_eq!(vl.limiter().window_used(ord_win, now_ns), Some(0));
    assert_eq!(vl.limiter().window_used(ip_win, now_ns), Some(0));

    // 2. Status::Aborted on Shared slot MUST NOT double-decrement sibling inflight!
    let hand_deadline = Instant::now() + Duration::from_secs(5);
    while !owner.gateway().slot_transport_ready(0) {
        assert!(Instant::now() < hand_deadline);
        owner
            .with_gateway(|g| g.poll_slot(0, Instant::now(), IoBudget::default()))
            .unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    now_ns += 1_000_000;
    auth.policy.lock().unwrap().gate.now = now_ns;
    auth.policy.lock().unwrap().gate.action_permitted = false;
    let (cmd_aborted, _, loid_aborted) = make_new_cmd(&base_cmd, 2, "abort-shared-2", now_ns);
    owner
        .submit(
            cmd_aborted.clone(),
            &[loid_aborted],
            now_ns,
            gateway::until(),
        )
        .unwrap();
    // Abort at gateway layer (releases 1st inflight -> 0), then acquire a sibling intent on the same Shared slot epoch!
    owner
        .with_gateway(|g| g.poll_slot(0, Instant::now(), IoBudget::default()))
        .unwrap();
    let ep_shared = owner.gateway().slot_route(0).unwrap().epoch;
    assert_eq!(owner.gateway().slot_route(0).unwrap().health.inflight(), 0);
    auth.policy.lock().unwrap().gate.action_permitted = true;
    auth.policy.lock().unwrap().gate.now = now_ns + 1;
    let (cmd_sib, _, _) = make_new_cmd(&base_cmd, 3, "sibling-shared-3", now_ns + 1);
    let (_, ep_sib) = owner
        .with_gateway(|g| g.submit_on_slot(0, cmd_sib.clone(), now_ns + 1, gateway::until()))
        .unwrap();
    assert_eq!(ep_sib, ep_shared);
    assert_eq!(owner.gateway().slot_route(0).unwrap().health.inflight(), 1);

    // Draining cmd_aborted's NoBytes outcome in RestOwner::poll MUST leave sibling's inflight == 1 intact!
    let out_aborted = wait_next_outcome(&mut owner, now_ns + 10_000);
    assert_eq!(out_aborted.disposition, RestDisposition::NoBytes);
    assert_eq!(
        owner.gateway().slot_route(0).unwrap().health.inflight(),
        1,
        "NoBytes cleanup must not double-decrement sibling inflight on Shared slot"
    );
    // Clean up sibling cmd_sib directly on gateway so slot 0 is idle again.
    server.push_json(200, spot_order_json("sibling-shared-3", 2003, "NEW"));
    let sib_drain_deadline = Instant::now() + Duration::from_secs(5);
    let mut sib_rx = Vec::new();
    while !sib_rx.ends_with(b"}") {
        assert!(Instant::now() < sib_drain_deadline);
        owner
            .with_gateway(
                |g| -> std::result::Result<(), tickwire_account_gateway::Error> {
                    let _ = g.poll_slot(0, Instant::now(), IoBudget::default());
                    let mut buf = [0u8; 1024];
                    loop {
                        let n = g.read_slot(0, &mut buf);
                        if n == 0 {
                            break;
                        }
                        sib_rx.extend_from_slice(&buf[..n]);
                    }
                    Ok(())
                },
            )
            .unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    owner.with_gateway(|g| {
        while g.read_slot(0, &mut [0u8; 1024]) > 0 {}
        let _ = g.release_slot_inflight(0, ep_sib);
    });

    // 3. Post-flush HTTP 200 OK + Connection: close (EOF) + immediate Ready DialBaton across small IoBudget!
    now_ns += 5_000_000;
    clock_ns.store(now_ns, Ordering::SeqCst);
    auth.policy.lock().unwrap().gate.now = now_ns;
    server.steps.lock().unwrap().push_back(ServerStep::Respond {
        status: 200,
        headers: Vec::new(),
        body: serde_json::to_vec(&spot_order_json("eof-baton-ok-4", 2004, "NEW")).unwrap(),
        close_after: true,
    });
    let (cmd_eof, okey_eof, loid_eof) = make_new_cmd(&base_cmd, 4, "eof-baton-ok-4", now_ns);
    owner
        .submit(cmd_eof, &[loid_eof], now_ns, gateway::until())
        .unwrap();

    // Flush request at gateway layer and wait until server sends 200 OK + closes TCP (EOF) and baton becomes Ready!
    let eof_wait = Instant::now() + Duration::from_secs(5);
    while owner.gateway().has_slot_connection(0) || server.request_count() < 2 {
        assert!(Instant::now() < eof_wait);
        owner.with_gateway(|g| {
            let _ = g.poll_slot(0, Instant::now(), IoBudget::default());
        });
        thread::sleep(Duration::from_millis(1));
    }
    // Wait until background dialer marks baton Ready BEFORE RestOwner drains retained_receive!
    let ready_wait = Instant::now() + Duration::from_secs(5);
    while baton.state() != tickwire_transport::dialer::DialState::Ready {
        assert!(
            Instant::now() < ready_wait,
            "timed out waiting for Ready baton"
        );
        clock_ns.fetch_add(10_000_000, Ordering::SeqCst);
        let _ = baton.want(0);
        thread::sleep(Duration::from_millis(2));
    }

    // Drain across multiple tiny 48-byte budgets while baton is Ready!
    let tiny_budget = IoBudget {
        max_bytes: 48,
        max_syscalls: 8,
    };
    let mut eof_outcome = None;
    let decode_deadline = Instant::now() + Duration::from_secs(5);
    while eof_outcome.is_none() {
        assert!(Instant::now() < decode_deadline);
        now_ns += 1_000;
        eof_outcome = owner.poll(Instant::now(), now_ns, tiny_budget).unwrap();
    }
    assert_eq!(eof_outcome.unwrap().disposition, RestDisposition::Observed);
    assert_eq!(owner.account().order_in_doubt(&okey_eof), None);
    assert!(
        owner.gateway().has_slot_connection(0),
        "Ready baton should harvest immediately once SlotPending completes"
    );
}

#[test]
fn ws_owner_drains_all_pre_eof_frames_under_message_cap_and_isolates_peer_close() {
    let (client_tls, server_tls) = spot_certs();

    // Part A: 3 multiplexed WS orders, max_messages_per_poll = 1; server replies to 2 and closes TCP (EOF).
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let recv_cnt = Arc::new(AtomicU64::new(0));
        let go_eof = Arc::new(AtomicBool::new(false));
        let recv_c = Arc::clone(&recv_cnt);
        let go_c = Arc::clone(&go_eof);

        let srv_th = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut stream = rustls::StreamOwned::new(
                rustls::ServerConnection::new(server_tls).unwrap(),
                socket,
            );
            complete_ws_upgrade(&mut stream);
            let mut reqs = Vec::new();
            for _ in 0..3 {
                reqs.push(read_masked_ws_text(&mut stream));
                recv_c.fetch_add(1, Ordering::AcqRel);
            }
            while !go_c.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(1));
            }
            for (idx, req) in reqs.iter().take(2).enumerate() {
                let cid = req["params"]["newClientOrderId"].as_str().unwrap();
                let reply = serde_json::json!({
                    "id": req["id"],
                    "status": 200,
                    "result": spot_order_json(cid, 7000 + idx as u64, "NEW")
                });
                write_server_ws_text(&mut stream, &reply);
            }
            stream.flush().unwrap();
            stream.conn.send_close_notify();
            let _ = stream.flush();
            let _ = stream.sock.shutdown(Shutdown::Both);
        });

        let account_dir = gateway::Directory::new();
        let inbox_dir = gateway::Directory::new();
        let (baseline, base_cmd) = build_spot_baseline();
        let mut gw_cfg = gateway::config(&baseline);
        gw_cfg.route.authority = "ws-api.testnet.binance.vision".into();
        gw_cfg.max_intents = 32;
        let mock_auth = gateway::MockAuthority::default();
        {
            let mut p = mock_auth.policy.lock().unwrap();
            p.gate.now = 10_000_000;
            p.gate.lease_until = u64::MAX / 2;
        }
        let ws_adapter = TestWsAdapter::new(mock_auth.clone());
        let mut ingress = AccountIngress::create(
            FileStore::open(&inbox_dir.0).unwrap(),
            ingress_config(&baseline),
        )
        .unwrap();
        let session = ingress.reserve_ws_session([51; 16], [52; 32]).unwrap();
        let gw = AccountGateway::create(
            FileBackend::open(&account_dir.0).unwrap(),
            gw_cfg,
            baseline,
            ws_adapter.clone(),
            ws_adapter.clone(),
            tickwire_account_gateway::ConnectionConfig {
                address: addr,
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
                max_messages_per_poll: 1,
            },
            Instant::now(),
            10_000_000,
        )
        .unwrap()
        .with_per_order_query_desk(QueryConfig::default());

        let budget = IoBudget::default();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !owner.websocket_ready() {
            assert!(Instant::now() < deadline);
            owner.poll(Instant::now(), 10_000_000, budget).unwrap();
            thread::sleep(Duration::from_millis(1));
        }

        let mut now_ns = 10_000_000_000u64;
        let mut okeys = Vec::new();
        for idx in 1..=3u64 {
            now_ns += 100_000;
            mock_auth.policy.lock().unwrap().gate.now = now_ns;
            let (cmd, okey, loid) = make_new_cmd(&base_cmd, idx, &format!("ws-eof-{idx}"), now_ns);
            okeys.push(okey);
            owner
                .submit(cmd, &[loid], now_ns, gateway::until())
                .unwrap();
            while recv_cnt.load(Ordering::Acquire) < idx {
                assert!(Instant::now() < deadline);
                let _ = owner.poll(Instant::now(), now_ns, budget).unwrap();
                thread::sleep(Duration::from_millis(1));
            }
        }
        go_eof.store(true, Ordering::Release);
        srv_th.join().unwrap();

        let mut outs = Vec::new();
        let drain_dl = Instant::now() + Duration::from_secs(5);
        while outs.len() < 3 {
            assert!(Instant::now() < drain_dl);
            now_ns += 1_000;
            if let Some(o) = owner.poll(Instant::now(), now_ns, budget).unwrap() {
                outs.push(o);
            } else {
                thread::sleep(Duration::from_millis(1));
            }
        }
        assert!(!owner.account().frozen());
        assert!(!owner.gateway().halted());
        assert_eq!(outs[0].disposition, RestDisposition::Observed);
        assert_eq!(outs[1].disposition, RestDisposition::Observed);
        assert_eq!(outs[2].disposition, RestDisposition::Unknown);
        assert_eq!(owner.account().order_in_doubt(&okeys[0]), None);
        assert_eq!(owner.account().order_in_doubt(&okeys[1]), None);
        assert_eq!(
            owner.account().order_in_doubt(&okeys[2]),
            Some(DoubtReason::PlaceAmbiguous)
        );
    }

    // Part B: Peer sends WebSocket Opcode::Close (0x88) while 1 order is in-flight under per_order_doubt_isolation.
    let (_, server_tls_b) = spot_certs();
    let (client_tls_b, server_tls_b2) = spot_certs();
    drop(server_tls_b);
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let srv_th = thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut stream = rustls::StreamOwned::new(
                rustls::ServerConnection::new(server_tls_b2).unwrap(),
                socket,
            );
            complete_ws_upgrade(&mut stream);
            let _ = read_masked_ws_text(&mut stream);
            // Send unmasked WS Close frame (FIN | Close = 0x88, payload len = 2, code = 1001).
            stream.write_all(&[0x88, 0x02, 0x03, 0xe9]).unwrap();
            stream.flush().unwrap();
            let mut echo = [0u8; 16];
            let _ = stream.read(&mut echo);
        });

        let account_dir = gateway::Directory::new();
        let inbox_dir = gateway::Directory::new();
        let (baseline, base_cmd) = build_spot_baseline();
        let mut gw_cfg = gateway::config(&baseline);
        gw_cfg.route.authority = "ws-api.testnet.binance.vision".into();
        gw_cfg.max_intents = 16;
        let mock_auth = gateway::MockAuthority::default();
        {
            let mut p = mock_auth.policy.lock().unwrap();
            p.gate.now = 10_000_000;
            p.gate.lease_until = u64::MAX / 2;
        }
        let ws_adapter = TestWsAdapter::new(mock_auth.clone());
        let mut ingress = AccountIngress::create(
            FileStore::open(&inbox_dir.0).unwrap(),
            ingress_config(&baseline),
        )
        .unwrap();
        let session = ingress.reserve_ws_session([61; 16], [62; 32]).unwrap();
        let gw = AccountGateway::create(
            FileBackend::open(&account_dir.0).unwrap(),
            gw_cfg,
            baseline,
            ws_adapter.clone(),
            ws_adapter.clone(),
            tickwire_account_gateway::ConnectionConfig {
                address: addr,
                tls: Arc::clone(&client_tls_b),
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
                max_messages_per_poll: 8,
            },
            Instant::now(),
            10_000_000,
        )
        .unwrap()
        .with_per_order_query_desk(QueryConfig::default());

        let budget = IoBudget::default();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !owner.websocket_ready() {
            assert!(Instant::now() < deadline);
            owner.poll(Instant::now(), 10_000_000, budget).unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        let mut now_ns = 10_000_000_000u64;
        mock_auth.policy.lock().unwrap().gate.now = now_ns;
        let (cmd, okey, loid) = make_new_cmd(&base_cmd, 1, "ws-close-iso-1", now_ns);
        owner
            .submit(cmd, &[loid], now_ns, gateway::until())
            .unwrap();

        let mut close_out = None;
        while close_out.is_none() {
            assert!(Instant::now() < deadline);
            now_ns += 10_000;
            mock_auth.policy.lock().unwrap().gate.now = now_ns;
            close_out = owner.poll(Instant::now(), now_ns, budget).unwrap();
            if close_out.is_none() {
                thread::sleep(Duration::from_millis(1));
            }
        }
        assert_eq!(close_out.unwrap().disposition, RestDisposition::Unknown);
        assert!(
            !owner.account().frozen(),
            "WS Close echo under per_order_doubt_isolation must not freeze Account"
        );
        assert!(
            !owner.gateway().halted(),
            "WS Close echo under per_order_doubt_isolation must not halt AccountGateway"
        );
        assert_eq!(
            owner.account().order_in_doubt(&okey),
            Some(DoubtReason::PlaceAmbiguous)
        );
        srv_th.join().unwrap();
    }
}
