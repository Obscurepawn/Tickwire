//! Drives the production Controller and FileNode against authenticated local TLS.
//! The dedicated test risk-admin signs only the initial baseline coverage claim.
use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
use sha1::{Digest, Sha1};
use std::{
    fs,
    io::{Read, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
};
use tickwire_account::{Account, RecoveryRecord};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_binance_clock::SystemClock;
use tickwire_clock::ClockDomain;
use tickwire_market_source as market;
use tickwire_runtime::{
    files,
    node::{Health, Submission},
};

use crate::replay::income_tests::common as fixture;
const START_MS: u64 = 1_000_000;
const SECRET: &[u8] = b"TEST-INCOME-ONLY-NOT-A-CREDENTIAL";
struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "tickwire-income-controller-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(fs::canonicalize(p).unwrap())
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct NoMarket(ClockDomain);
impl market::MarketSource for NoMarket {
    fn domain(&self) -> ClockDomain {
        self.0
    }
    fn poll(
        &mut self,
        _: MonoTime,
        _: u64,
        _: usize,
        _: &mut dyn market::MarketSink,
    ) -> market::Result<market::PollReport> {
        Ok(market::PollReport::default())
    }
    fn is_live(&self, _: MonoTime, _: u64) -> bool {
        false
    }
    fn stop(&mut self, _: &mut dyn market::MarketSink) -> market::Result<()> {
        Ok(())
    }
}
struct Observe;
impl Strategy for Observe {
    fn on_market(&mut self, _: &public::Notice<'_>) -> bool {
        true
    }
    fn next(
        &mut self,
        _: &Account,
        _: Health,
        _: tickwire_binance_clock::LocalTime,
    ) -> Option<Submission> {
        None
    }
    fn on_submission(
        &mut self,
        _: &tickwire_venue::CanonicalCommand,
        _: &std::result::Result<tickwire_account_gateway::Status, tickwire_binance_account::Error>,
    ) {
        panic!("read-only fixture cannot submit")
    }
}
struct Output;
impl public::PublicSink for Output {
    fn on_notice(&mut self, _: public::Notice<'_>) -> bool {
        true
    }
}
struct Server {
    address: SocketAddr,
    tls: Arc<rustls::ClientConfig>,
    stop: Arc<AtomicBool>,
    paths: Arc<Mutex<Vec<String>>>,
    fail_snapshots: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}
fn account_body(funded: bool) -> Value {
    let total = if funded { "998" } else { "1000" };
    let assets: Vec<_> = [("BTC", "1"), ("USDT", total)].into_iter().map(|(asset,n)|json!({"asset":asset,"walletBalance":n,"availableBalance":n,"marginBalance":n,"unrealizedProfit":"0","maintMargin":"0","initialMargin":"0","positionInitialMargin":"0","openOrderInitialMargin":"0","crossWalletBalance":n,"crossUnPnl":"0"})).collect();
    json!({"totalInitialMargin":"0","totalMaintMargin":"0","totalUnrealizedProfit":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","totalCrossUnPnl":"0","assets":assets,"positions":[]})
}
impl Server {
    fn start(eligible: bool, association: bool) -> Self {
        let cert = rcgen::generate_simple_self_signed(vec![
            "demo-fapi.binance.com".into(),
            "demo-fstream.binance.com".into(),
        ])
        .unwrap();
        let server = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![cert.cert.der().clone()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der())
                        .into(),
                )
                .unwrap(),
        );
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let tls = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let paths = Arc::new(Mutex::new(Vec::new()));
        let fail_snapshots = Arc::new(AtomicBool::new(false));
        let failed = fail_snapshots.clone();
        let (done, requests) = (stop.clone(), paths.clone());
        let worker = thread::spawn(move || {
            let start = Instant::now();
            let funded = Arc::new(AtomicBool::new(false));
            let observation_sent = Arc::new(AtomicBool::new(false));
            let mut threads = Vec::new();
            while !done.load(Ordering::SeqCst) {
                let (socket, _) = match listener.accept() {
                    Ok(v) => v,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(e) => panic!("{e}"),
                };
                assert!(threads.len() < 64, "bounded fixture connections");
                socket
                    .set_read_timeout(Some(Duration::from_millis(250)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let (tls, stop, paths, funded, observation_sent) = (
                    server.clone(),
                    done.clone(),
                    requests.clone(),
                    funded.clone(),
                    observation_sent.clone(),
                );
                let failed = failed.clone();
                threads.push(thread::spawn(move || {
                    let mut s = rustls::StreamOwned::new(rustls::ServerConnection::new(tls).unwrap(),socket);
                    let mut h=Vec::new();
                    while !h.ends_with(b"\r\n\r\n") {
                        let mut one=[0]; match s.read(&mut one) { Ok(0)=>return, Ok(_)=>h.push(one[0]), Err(e) if matches!(e.kind(),std::io::ErrorKind::TimedOut|std::io::ErrorKind::WouldBlock)=> {if stop.load(Ordering::SeqCst){return}}, Err(_)=>return }
                        assert!(h.len()<16384);
                    }
                    let h=String::from_utf8(h).unwrap(); let path=h.lines().next().unwrap().split_whitespace().nth(1).unwrap().to_owned();
                    paths.lock().unwrap().push(path.clone());
                    if path.starts_with("/fapi/v3/account?") && failed.load(Ordering::SeqCst) {
                        s.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").unwrap();
                        s.flush().unwrap();
                        return;
                    }
                    if path.starts_with("/private/ws/") {
                        let key=h.lines().find_map(|l| l.split_once(':').filter(|(n,_)|n.eq_ignore_ascii_case("sec-websocket-key")).map(|(_,v)|v.trim())).unwrap();
                        let accept=STANDARD.encode(Sha1::digest(format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11")));
                        write!(s,"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").unwrap(); s.flush().unwrap();
                        while !stop.load(Ordering::SeqCst) {
                            if association&&funded.load(Ordering::SeqCst)&&!observation_sent.load(Ordering::SeqCst){
                                let b=serde_json::to_vec(&json!({"e":"ACCOUNT_UPDATE","E":START_MS+1,"T":START_MS+1,"a":{"m":"FUNDING_FEE","B":[{"a":"USDT","wb":"998","cw":"998","bc":"-2"}]}})).unwrap();
                                let mut frame=vec![0x81];if b.len()<126 {frame.push(b.len() as u8)}else{frame.push(126);frame.extend((b.len() as u16).to_be_bytes());}frame.extend(b);s.write_all(&frame).unwrap();s.flush().unwrap();observation_sent.store(true,Ordering::SeqCst);
                            }
                            thread::sleep(Duration::from_millis(1));
                        } return;
                    }
                    let body=if path=="/fapi/v1/time" { json!({"serverTime":START_MS+start.elapsed().as_millis() as u64}) }
                    else if path.starts_with("/fapi/v1/listenKey") { json!({"listenKey":"income-test-session"}) }
                    else {
                        assert!(h.contains("X-MBX-APIKEY: TEST-INCOME") || h.contains("X-MBX-APIKEY:TEST-INCOME") || h.to_ascii_lowercase().contains("x-mbx-apikey: test-income"),"missing API identity {h}");
                        let (unsigned,sig)=path.rsplit_once("&signature=").expect("signed read");
                        let params=unsigned.split_once('?').unwrap().1;
                        let expected=ring::hmac::sign(&ring::hmac::Key::new(ring::hmac::HMAC_SHA256,SECRET),params.as_bytes());
                        assert_eq!(sig,hex(expected.as_ref()),"signature binds exact query");
                        if path.starts_with("/fapi/v1/income?") {
                            if params.split('&').any(|p|p==format!("endTime={START_MS}")) { json!([]) } else { funded.store(true,Ordering::SeqCst);
                                if association {let until=Instant::now()+Duration::from_secs(2);while !observation_sent.load(Ordering::SeqCst){assert!(Instant::now()<until,"private observation delivery");thread::sleep(Duration::from_millis(1));}}
                                json!([{"symbol":if association {"BTCUSDT"}else{""},"incomeType":if eligible { "FUNDING_FEE" } else { "TRANSFER" },"income":"-2","asset":"USDT","time":START_MS+1,"tranId":43,"tradeId":""}]) }
                        } else if path.starts_with("/fapi/v3/account?") { account_body(funded.load(Ordering::SeqCst)) }
                        else if path.starts_with("/fapi/v1/openOrders?") || path.starts_with("/fapi/v1/openAlgoOrders?") {json!([])}
                        else {panic!("unexpected endpoint {path}")}
                    };
                    let b=serde_json::to_vec(&body).unwrap(); write!(s,"HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",b.len()).unwrap(); s.write_all(&b).unwrap(); s.flush().unwrap();
                }));
            }
            for t in threads {
                t.join().unwrap();
            }
        });
        Self {
            address,
            tls,
            stop,
            paths,
            fail_snapshots,
            worker: Some(worker),
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.worker.take()
            && let Err(e) = t.join()
            && !thread::panicking()
        {
            std::panic::resume_unwind(e)
        }
    }
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|v| format!("{v:02x}")).collect()
}
fn deployment(root: &Path, now: u64, key: &Ed25519KeyPair) -> crate::config::Deployment {
    let end = now + 60_000_000_000;
    let mut rules = Vec::new();
    for (family, kind, weight, messages) in [
        ("time", "connect", 0, 0),
        ("time", "query", 1, 0),
        ("private", "stream_connect", 0, 0),
        ("private", "management_connect", 0, 0),
        ("private", "upgrade", 0, 0),
        ("private", "usdm_create", 1, 0),
        ("read", "connect", 0, 0),
        ("read", "income", 0, 0),
        ("read", "account_snapshot", 0, 0),
        ("read", "open_orders", 0, 0),
        ("read", "conditional_orders", 0, 0),
    ] {
        rules.push(json!({"source":"usdm_test","action":{"family":family,"kind":kind},"class":"recovery","api_weight":weight,"websocket_messages":messages,"costs":[{"window":"test","units":1}]}));
    }
    let policy = json!({"scope":{"risk_domain_id":([1u8;16]),"venue":1,"environment":"test","account":2},"product":"linear_perpetual","executor_id":([72u8;16]),"owner_generation":1,"writer_epoch":4,"domain":{"process_instance":([71u8;16]),"generation":1},"versions":{"config_revision":6,"risk_policy_version":10,"order_config_version":11,"catalog_version":14,"catalog_hash":([5u8;32])},"max_strategies":4,"max_feeds":4,"marks_per_feed":2,"control_revision":1,"mode":"stopped","owner_lease":null,"strategy_grants":[],"management_grant":null});
    let quota = json!({"supervisor_generation":1,"consumer_instance":([72u8;16]),"consumer_generation":1,"consumer_lease_until_ns":end,"producer_id":([73u8;16]),"policy_generation":1,"policy_starts_ns":now,"policy_ends_ns":end,"windows":[{"name":"test","namespace":([75u8;16]),"scope":"ip","subject":([76u8;16]),"pool":1,"window":1,"revision":1,"starts_ns":now,"ends_ns":end,"limit":1000,"recovery_reserve":1000}],"grants":[{"request_id":1,"class":"recovery","expires_ns":end,"costs":[{"window":"test","units":1000}]}],"rules":rules});
    let reconciliation = json!({"enabled":true,"reader_id":([74u8;16]),"first_request_epoch":1,"first_execution_connection_epoch":2,"retry_ns":100000000,"valid_for_ns":10000000000u64,"income_baseline_import":{"wallet_id":1,"start_time_ms":1,"through_time_ms":START_MS,"limit":16,"maximum_queries":16,"request_id":([99u8;16]),"approval_valid_for_ns":10000000000u64,"file_poll_interval_ns":1000000,"proposal_file":root.join("proposal.json"),"approval_file":root.join("approval.json")},"income_account_scan":{"wallet_id":1,"lookback_ms":1000000,"interval_ns":30000000000u64,"limit":16,"maximum_queries":16}});
    let clock = json!({"max_round_trip_ns":1000000000,"max_width_ns":1000000000,"max_age_ns":30000000000u64,"quantization_error_ns":1000000,"drift_ppb":1,"max_adjustment_ns":1000000000,"calibration_interval_ns":10000000000u64,"calibration_retry_ns":100000000});
    let tuning = json!({"connect_timeout_ns":3000000000u64,"request_timeout_ns":3000000000u64,"delivery_budget_ns":1000000,"recv_window_micros":5000000,"maximum_book_age_ns":1000000000,"maximum_idle_wait_ns":1000000,"snapshot_limit":5,"maximum_intents":8,"maximum_input_records":256,"maximum_input_bytes":4194304,"wal_segment_bytes":1048576});
    serde_json::from_value(json!({
        "baseline":root.join("baseline.cbor"),"directories":{"account":root.join("account"),"input":root.join("input"),"recovery":root.join("recovery")},
        "recover":false,"product":"usdm","boot_id":([71u8;16]),"clock_generation":1,"owner_id":([72u8;16]),"writer_epoch":4,"initial_writer_epoch":4,
        "account_stream":([21u8;16]),"input_stream":([22u8;16]),"first_connection_epoch":1,"instrument_id":12,"book_source":1,
        "credential":{"api_key_file":root.join("unused"),"signing_key_file":root.join("unused2"),"algorithm":"hmac","generation":1,"spot_logon":false},
        "network":{"rest":"127.0.0.1:1","time":"127.0.0.1:1","public_stream":"127.0.0.1:1","snapshot":"127.0.0.1:1","private_stream":"127.0.0.1:1","private_management":null,"trust_anchors":[]},
        "clock":clock,
        "tuning":tuning,
        "policy":policy,
        "quota":quota,
        "strategy":{"mode":"observe"}, "reconciliation":reconciliation,
        "income_baseline_authority":{"controller_id":([31u8;16]),"public_key":key.public_key().as_ref()},
        "stop_file":root.join("STOP"),"maximum_runtime_ns":10000000000u64
    })).unwrap()
}
fn drive_controller(eligible: bool) {
    drive_controller_case(eligible, false, false)
}
fn drive_controller_case(eligible: bool, association: bool, invalid_signature: bool) {
    let temp = Temporary::new();
    let server = Server::start(eligible, association);
    let domain = ClockDomain::new([71; 16], 1).unwrap();
    let now = SystemClock::new(domain)
        .unwrap()
        .sample()
        .unwrap()
        .monotonic;
    let admin = Ed25519KeyPair::from_seed_unchecked(&[42; 32]).unwrap();
    let mut deploy = deployment(&temp.0, now.nanos(), &admin);
    use tickwire_binance_account::reconciliation::income_association::{
        ActiveAssociationPolicy, AssociationAuthority, AssociationProposal, AuthorityClaims,
        SignedAssociationAuthority,
    };
    let root = Ed25519KeyPair::from_seed_unchecked(&[50; 32]).unwrap();
    let leaf = Ed25519KeyPair::from_seed_unchecked(&[51; 32]).unwrap();
    let authorization = b"test dedicated controller authorized exact risk scope";
    let hash = |b: &[u8]| -> [u8; 32] {
        ring::digest::digest(&ring::digest::SHA256, b)
            .as_ref()
            .try_into()
            .unwrap()
    };
    let claims = AuthorityClaims {
        controller_id: [44; 16],
        key_id: [45; 16],
        revision: 1,
        scope: tickwire_account::Scope {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: tickwire_venue::Environment::Test,
            account: 2,
        },
        wallet_id: 1,
        execution_instance: [72; 16],
        writer_epoch: 4,
        clock_domain: domain,
        issued: now.nanos(),
        expires: now.nanos() + 60_000_000_000,
        public_key: leaf.public_key().as_ref().try_into().unwrap(),
        authorization_digest: hash(authorization),
    };
    let certificate_bytes = claims.bytes().unwrap();
    let certificate =
        SignedAssociationAuthority::new(&certificate_bytes, root.sign(&certificate_bytes).as_ref())
            .unwrap();
    if association {
        deploy.income_association_authority = Some(AssociationAuthority {
            controller_id: [44; 16],
            public_key: root.public_key().as_ref().try_into().unwrap(),
            execution_instance: [72; 16],
            active_policy: Some(ActiveAssociationPolicy {
                certificate_digest: certificate.digest(),
                authorization_revision: 1,
            }),
        });
        deploy.reconciliation.income_association = Some(association::Config {
            selection_file: temp.0.join("association-selection.json"),
            proposal_file: temp.0.join("association-proposal.json"),
            approval_file: temp.0.join("association-approval.json"),
            file_poll_interval_ns: 1_000_000,
        });
    }
    let (s, c, _) = fixture::futures_fixture();
    let baseline = CanonicalBaseline::new(s, c).unwrap();
    fs::write(&deploy.baseline, baseline.bytes()).unwrap();
    let config = deploy.assemble(baseline.clone()).unwrap();
    let plane = deploy.policy.build().unwrap();
    let provider = deploy.policy.native_provider(&plane).unwrap();
    let quota = deploy
        .quota
        .build(tickwire_quota::MockQuotaStore::default(), now)
        .unwrap();
    let keys = crate::config::CredentialMaterial {
        api_key: "TEST-INCOME".into(),
        signing_key: tickwire_binance::auth::SigningKey::hmac(SECRET).unwrap(),
    };
    let credentials = deploy.credentials(&baseline, &keys);
    let network = files::Networks {
        websocket: None,
        rest: tickwire_account_gateway::ConnectionConfig {
            address: server.address,
            tls: server.tls.clone(),
            limits: Limits::default(),
            connect_deadline: Instant::now() + Duration::from_secs(3),
        },
        time: tickwire_binance_clock::NetworkConfig {
            address: server.address,
            tls: server.tls.clone(),
        },
        public: public::NetworkConfig {
            stream_address: server.address,
            snapshot_address: server.address,
            tls: server.tls.clone(),
        },
        private: private::NetworkConfig {
            stream_address: server.address,
            management_address: Some(server.address),
            tls: server.tls.clone(),
        },
    };
    let readscope = config.bindings.private.scope.clone();
    let mut node = files::open::<public::BinanceUsdM, _, _, _, 8, 8, 2>(
        config,
        network,
        credentials,
        deploy.authentication(&keys),
        quota.endpoint,
        provider,
        Observe,
        Output,
        Box::new(plane.observer()),
    )
    .unwrap();
    node.set_market_source(Box::new(NoMarket(domain))).unwrap();
    let mut controller = Controller::new(
        deploy.reconciliation.clone(),
        ReconcileConfig {
            scope: readscope.clone(),
            first_connection_epoch: 1,
            address: server.address,
            tls: server.tls.clone(),
            limits: Limits::default(),
            request_timeout_ns: 3_000_000_000,
            recv_window_micros: 5_000_000,
            max_response_bytes: 32768,
            max_headers: 64,
        },
        ReadBinding {
            owner_id: deploy.reconciliation.reader_id,
            scope: readscope,
            domain,
        },
        plane.clone(),
        deploy.credentials(&baseline, &keys),
        4,
        1_000_000,
        3_000_000_000,
    )
    .unwrap();
    let poll = mio::Poll::new().unwrap();
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut signed = false;
    let mut saw_closure = false;
    let mut association_selected = false;
    let mut association_signed = false;
    let mut saw_association = false;
    loop {
        assert!(
            Instant::now() < deadline,
            "controller did not complete: ledger={} paths={:?}",
            node.account().ledger_sequence(),
            server.paths.lock().unwrap()
        );
        node.step().unwrap();
        controller.drive(&mut node, poll.registry()).unwrap();
        let proposal = temp.0.join("proposal.json");
        if !signed && proposal.exists() {
            let value: Value = serde_json::from_slice(&fs::read(proposal).unwrap()).unwrap();
            let p = value["approval_payload_hex"].as_str().unwrap();
            let payload: Vec<u8> = p
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
                .collect();
            fs::write(temp.0.join("approval.json"),serde_json::to_vec(&json!({"format":"tickwire.income-baseline-approval.v1","payload_hex":p,"signature_hex":hex(admin.sign(&payload).as_ref())})).unwrap()).unwrap();
            signed = true;
        }
        if association
            && !association_selected
            && let Some(observation) = node.account().unexplained_account_observations().next()
        {
            fs::write(temp.0.join("association-selection.json"),serde_json::to_vec(&json!({"format":"tickwire.income-association-selection.v1","request_id_hex":hex(&[55;16]),"wallet_id":1,"certificate_payload_hex":hex(certificate.payload()),"certificate_signature_hex":hex(certificate.signature()),"authorization_evidence_hex":hex(authorization),"associations":[{"observation_digest_hex":hex(&observation.digest()),"incomes":[{"income_type":"FUNDING_FEE","transaction_id":"43"}]}]})).unwrap()).unwrap();
            association_selected = true;
        }
        let proposal_file = temp.0.join("association-proposal.json");
        if association && !association_signed && proposal_file.exists() {
            let value: Value = serde_json::from_slice(&fs::read(&proposal_file).unwrap()).unwrap();
            let bytes = income::unhex(value["manifest_hex"].as_str().unwrap(), 54 * 1024).unwrap();
            let proposal = AssociationProposal::decode(&bytes).unwrap();
            let audit=serde_json::to_vec(&json!({"test_risk_admin":true,"decision":"associate exact funding observation", "manifest":hex(&proposal.digest())})).unwrap();
            let current = node.local_time().unwrap().monotonic;
            let payload = proposal
                .approval_payload(
                    &certificate,
                    hash(&audit),
                    current.nanos(),
                    proposal.valid_until().unwrap().nanos(),
                )
                .unwrap();
            let signer = if invalid_signature { &admin } else { &leaf };
            fs::write(temp.0.join("association-approval.json"),serde_json::to_vec(&json!({"format":"tickwire.income-association-approval.v1","payload_hex":hex(&payload),"signature_hex":hex(signer.sign(&payload).as_ref()),"audit_evidence_hex":hex(&audit)})).unwrap()).unwrap();
            association_signed = true;
        }
        let records: Vec<_> = node
            .rest()
            .gateway()
            .journal_records_after(0, 32)
            .unwrap()
            .map(|(_, r)| r.record().clone())
            .collect();
        if association
            && records
                .iter()
                .any(|r| matches!(r, RecoveryRecord::IncomeObservationsAssociated(_)))
        {
            if !saw_association {
                assert!(node.account().frozen());
                assert!(controller.snapshots.is_empty());
                assert_eq!(
                    node.account().asset(fixture::key(2)).unwrap().total,
                    fixture::units(998)
                );
            }
            saw_association = true;
        }
        if association
            && node.account().ledger_sequence() == 3
            && controller.retry_at > node.local_time().unwrap().monotonic.nanos()
        {
            if invalid_signature && association_signed {
                assert!(!saw_association);
                assert!(node.account().frozen());
                assert_eq!(node.account().unexplained_account_observations().count(), 1);
                break;
            }
            assert!(
                proposal_file.exists(),
                "controller must propose the explicit association before retrying activation"
            );
        }
        if records
            .iter()
            .any(|r| matches!(r, RecoveryRecord::AccountInputsReconciled(_)))
        {
            if !saw_closure {
                assert!(node.account().frozen());
                assert!(!plane.private_frontier().unwrap().reconciled);
                assert!(controller.snapshots.is_empty());
            }
            saw_closure = true;
        }
        if !eligible
            && node.account().ledger_sequence() == 2
            && controller.retry_at > node.local_time().unwrap().monotonic.nanos()
        {
            assert!(node.account().frozen());
            assert!(node.account().unresolved_account_inputs() > 0);
            assert!(!plane.private_frontier().unwrap().reconciled);
            assert!(matches!(
                records.as_slice(),
                [
                    RecoveryRecord::IncomeBaseline(_),
                    RecoveryRecord::IngressAccountInput { .. }
                ]
            ));
            assert_eq!(
                node.account().asset(fixture::key(2)).unwrap().total,
                fixture::units(1000)
            );
            assert_eq!(
                node.account()
                    .pending_account_debit(fixture::key(2))
                    .unwrap(),
                fixture::units(2)
            );
            assert_eq!(
                node.account().asset(fixture::key(2)).unwrap().available,
                fixture::units(998)
            );
            break;
        }
        if saw_closure && !node.account().frozen() {
            assert!(plane.private_frontier().unwrap().reconciled);
            assert_eq!(
                node.account().asset(fixture::key(2)).unwrap().total,
                fixture::units(998)
            );
            assert_eq!(
                node.account().ledger_sequence(),
                if association { 6 } else { 4 }
            );
            assert_eq!(node.account().unresolved_account_inputs(), 0);
            if association {
                assert!(saw_association);
                assert_eq!(records.len(), 6);
                assert!(matches!(
                    records[3],
                    RecoveryRecord::IncomeObservationsAssociated(_)
                ));
            } else {
                assert!(matches!(
                    records.as_slice(),
                    [
                        RecoveryRecord::IncomeBaseline(_),
                        RecoveryRecord::IngressAccountInput { .. },
                        RecoveryRecord::AccountInputsReconciled(_),
                        RecoveryRecord::Reactivated(_)
                    ]
                ));
            }
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let paths = server.paths.lock().unwrap().clone();
    let queries: Vec<_> = paths
        .iter()
        .filter(|p| {
            p.starts_with("/fapi/v1/income?")
                || p.starts_with("/fapi/v3/account?")
                || p.starts_with("/fapi/v1/openOrders?")
                || p.starts_with("/fapi/v1/openAlgoOrders?")
        })
        .map(|p| p.split('?').next().unwrap())
        .collect();
    if eligible && !association {
        assert_eq!(
            queries,
            [
                "/fapi/v1/income",
                "/fapi/v3/account",
                "/fapi/v1/openOrders",
                "/fapi/v1/openAlgoOrders",
                "/fapi/v1/income",
                "/fapi/v3/account",
                "/fapi/v1/openOrders",
                "/fapi/v1/openAlgoOrders",
                "/fapi/v3/account",
                "/fapi/v1/openOrders",
                "/fapi/v1/openAlgoOrders"
            ]
        );
    }
    node.stop();
    drop(node);
    let report = crate::replay::run(&deploy).unwrap();
    let expected = if association {
        if invalid_signature { 3 } else { 6 }
    } else if eligible {
        4
    } else {
        2
    };
    assert_eq!(report["ledger_sequence"], expected);
    assert_eq!(report["account_records"], expected);
    assert_eq!(report["frozen"], true);
    assert_eq!(report["input_facts_not_in_account"], 0);
    let asset = report["assets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["asset_id"] == 2)
        .unwrap();
    assert_eq!(
        asset["total"]["atoms"],
        if eligible {
            "99800000000"
        } else {
            "100000000000"
        }
    );
    eprintln!(
        "actual TLS controller eligible={eligible}; exact wallet survives dual-WAL/blob-store recovery"
    );
}

#[test]
fn tls_controller_closes_income_then_collects_new_ledger_snapshots_before_activation() {
    drive_controller(true);
}
#[test]
fn tls_controller_keeps_unexplained_income_frozen_and_durable() {
    drive_controller(false);
}

#[test]
fn tls_controller_associates_explicit_funding_then_requeries_after_each_ledger_change() {
    drive_controller_case(true, true, false)
}
#[test]
fn tls_controller_rejects_other_purpose_signer_for_observation_association() {
    drive_controller_case(true, true, true)
}

#[test]
fn tls_empty_account_renews_twice_and_failed_refresh_cannot_extend_recovered_deadline() {
    let temp = Temporary::new();
    let server = Server::start(true, false);
    let domain = ClockDomain::new([71; 16], 1).unwrap();
    let now = SystemClock::new(domain)
        .unwrap()
        .sample()
        .unwrap()
        .monotonic;
    let admin = Ed25519KeyPair::from_seed_unchecked(&[42; 32]).unwrap();
    let mut deploy = deployment(&temp.0, now.nanos(), &admin);
    deploy.income_baseline_authority = None;
    deploy.reconciliation.income_baseline_import = None;
    deploy.reconciliation.income_account_scan = None;
    deploy.reconciliation.valid_for_ns = 2_000_000_000;
    // The fee timer must not postpone qualification renewal.
    deploy.reconciliation.fee_maintenance_interval_ns = 30_000_000_000;
    let (s, c, _) = fixture::futures_fixture();
    let baseline = CanonicalBaseline::new(s, c).unwrap();
    fs::write(&deploy.baseline, baseline.bytes()).unwrap();
    let config = deploy.assemble(baseline.clone()).unwrap();
    let plane = deploy.policy.build().unwrap();
    let provider = deploy.policy.native_provider(&plane).unwrap();
    let quota = deploy
        .quota
        .build(tickwire_quota::MockQuotaStore::default(), now)
        .unwrap();
    let keys = crate::config::CredentialMaterial {
        api_key: "TEST-INCOME".into(),
        signing_key: tickwire_binance::auth::SigningKey::hmac(SECRET).unwrap(),
    };
    let credentials = deploy.credentials(&baseline, &keys);
    let network = files::Networks {
        websocket: None,
        rest: tickwire_account_gateway::ConnectionConfig {
            address: server.address,
            tls: server.tls.clone(),
            limits: Limits::default(),
            connect_deadline: Instant::now() + Duration::from_secs(3),
        },
        time: tickwire_binance_clock::NetworkConfig {
            address: server.address,
            tls: server.tls.clone(),
        },
        public: public::NetworkConfig {
            stream_address: server.address,
            snapshot_address: server.address,
            tls: server.tls.clone(),
        },
        private: private::NetworkConfig {
            stream_address: server.address,
            management_address: Some(server.address),
            tls: server.tls.clone(),
        },
    };
    let readscope = config.bindings.private.scope.clone();
    let mut node = files::open::<public::BinanceUsdM, _, _, _, 8, 8, 2>(
        config,
        network,
        credentials,
        deploy.authentication(&keys),
        quota.endpoint,
        provider,
        Observe,
        Output,
        Box::new(plane.observer()),
    )
    .unwrap();
    node.set_market_source(Box::new(NoMarket(domain))).unwrap();
    let mut controller = Controller::new(
        deploy.reconciliation.clone(),
        ReconcileConfig {
            scope: readscope.clone(),
            first_connection_epoch: 1,
            address: server.address,
            tls: server.tls.clone(),
            limits: Limits::default(),
            request_timeout_ns: 3_000_000_000,
            recv_window_micros: 5_000_000,
            max_response_bytes: 32768,
            max_headers: 64,
        },
        ReadBinding {
            owner_id: deploy.reconciliation.reader_id,
            scope: readscope,
            domain,
        },
        plane.clone(),
        deploy.credentials(&baseline, &keys),
        4,
        1_000_000,
        3_000_000_000,
    )
    .unwrap();
    let poll = mio::Poll::new().unwrap();

    let limit = Instant::now() + Duration::from_secs(8);
    let mut first_until = None;
    let mut second_until = None;
    let mut failed_while_frozen = false;
    loop {
        assert!(
            Instant::now() < limit,
            "missing actual renewal; ledger={} paths={:?}",
            node.account().ledger_sequence(),
            server.paths.lock().unwrap()
        );
        node.step().unwrap();
        controller.drive(&mut node, poll.registry()).unwrap();
        let records: Vec<_> = node
            .rest()
            .gateway()
            .journal_records_after(0, 8)
            .unwrap()
            .map(|(_, r)| r.record().clone())
            .collect();
        if records.len() == 1 && first_until.is_none() {
            let RecoveryRecord::Reactivated(b) = &records[0] else {
                panic!("activation required")
            };
            first_until = Some(b.valid_until);
            assert!(!node.account().frozen());
            assert_eq!(node.account().valid_until(), b.valid_until);
            assert_eq!(
                b.valid_until - b.evaluated_at,
                deploy.reconciliation.valid_for_ns
            );
        }
        if records.len() == 2 && second_until.is_none() {
            let RecoveryRecord::Reactivated(b) = &records[1] else {
                panic!("second activation required")
            };
            assert!(b.valid_until > first_until.unwrap());
            assert_eq!(node.account().valid_until(), b.valid_until);
            assert!(!node.account().frozen());
            assert!(plane.private_frontier().unwrap().reconciled);
            assert_eq!(records.len(), 2);
            let paths = server.paths.lock().unwrap();
            for path in [
                "/fapi/v3/account?",
                "/fapi/v1/openOrders?",
                "/fapi/v1/openAlgoOrders?",
            ] {
                assert_eq!(
                    paths.iter().filter(|p| p.starts_with(path)).count(),
                    2,
                    "each activation needs fresh snapshots"
                );
            }
            assert!(paths.iter().all(|p| !p.starts_with("/fapi/v1/income?")));
            drop(paths);
            second_until = Some(b.valid_until);
            server.fail_snapshots.store(true, Ordering::SeqCst);
        }
        if let Some(until) = second_until {
            assert_eq!(
                records.len(),
                2,
                "failed snapshot cannot commit an activation"
            );
            assert_eq!(node.account().valid_until(), until);
            let now = node.local_time().unwrap().monotonic.nanos();
            if controller.retry_at > now {
                failed_while_frozen = true;
                assert!(node.account().frozen());
                assert!(controller.snapshots.is_empty());
                assert!(!controller.work.loaded);
            }
            if now >= until && failed_while_frozen && controller.retry_at > now {
                assert!(node.account().frozen());
                let mut recovered = Account::recover(
                    baseline.snapshot().clone(),
                    baseline.catalog().clone(),
                    &records,
                )
                .unwrap();
                assert!(recovered.frozen());
                assert_eq!(recovered.valid_until(), until);
                assert_eq!(recovered.ledger_sequence(), 2);
                let (_, _, mut command) = fixture::futures_fixture();
                command.context.request.deadline = until + 1_000_000_000;
                assert!(matches!(
                    recovered.admit(
                        &tickwire_venue::CanonicalCommand::new(command).unwrap(),
                        until
                    ),
                    Err(tickwire_account::Error::Stale)
                ));
                assert_eq!(
                    recovered.asset(fixture::key(2)).unwrap().total,
                    fixture::units(1000)
                );
                break;
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
    node.stop();
    drop(node);
    // Reopen both real WALs and the persistent closure store through the production verifier.
    let report = crate::replay::run(&deploy).unwrap();
    assert_eq!(report["account_records"], 2);
    assert_eq!(report["ledger_sequence"], 2);
    assert_eq!(report["frozen"], true);
    assert_eq!(report["input_facts_not_in_account"], 0);
    eprintln!(
        "actual TLS: two durable activations; failed refresh and dual-WAL recovery preserve expiry={}",
        second_until.unwrap()
    );
}
