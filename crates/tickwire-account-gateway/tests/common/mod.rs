#![allow(dead_code)]
#[path = "../../../tickwire-account/tests/common/mod.rs"]
pub mod fixture;
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read},
    net::TcpListener,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_account::{Admission, IntentKey, RecoveryRecord};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::*;
use tickwire_account_journal::FileBackend;
use tickwire_recovery::{self as recovery, BlobStore};
use tickwire_transport::{IoBudget, Limits, rustls};
use tickwire_venue::{CanonicalCommand, Product};

pub fn until() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
pub fn intent() -> IntentKey {
    IntentKey {
        epoch: 4,
        intent_id: 3,
    }
}
static NEXT: AtomicU64 = AtomicU64::new(0);
pub struct Directory(pub PathBuf);
impl Directory {
    pub fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "tickwire-account-gateway-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub fn baseline(futures: bool) -> (CanonicalBaseline, CanonicalCommand) {
    let (s, c, command) = if futures {
        fixture::futures_fixture()
    } else {
        fixture::fixture()
    };
    (
        CanonicalBaseline::new(s, c).unwrap(),
        fixture::canonical(command),
    )
}
pub fn config(b: &CanonicalBaseline) -> Config {
    let product = b.catalog().entries[0].product;
    Config {
        journal: tickwire_account_journal::Config {
            physical: tickwire_journal::Config::new([2; 16], [1; 16]),
            baseline_digest: b.digest(),
            initial_writer_epoch: 4,
            scope: b.snapshot().scope.clone(),
        },
        writer_epoch: 4,
        route: Route {
            venue: 1,
            account: 2,
            environment: tickwire_venue::Environment::Test,
            product,
            authority: if product == Product::Spot {
                "testnet.binance.vision"
            } else {
                "demo-fapi.binance.com"
            }
            .into(),
        },
        max_intents: 32,
    }
}
#[derive(Default, Clone)]
pub struct SharedBlobs(pub Arc<Mutex<BTreeMap<recovery::StoreKey, Vec<u8>>>>);
impl BlobStore for SharedBlobs {
    fn get(&self, key: recovery::StoreKey, max: usize) -> recovery::Result<Vec<u8>> {
        let map = self.0.lock().unwrap();
        let bytes = map.get(&key).ok_or(recovery::Error::Missing)?;
        if bytes.len() > max {
            return Err(recovery::Error::Capacity);
        }
        Ok(bytes.clone())
    }
    fn put_sync(&mut self, key: recovery::StoreKey, bytes: &[u8]) -> recovery::Result<()> {
        let mut map = self.0.lock().unwrap();
        if let Some(old) = map.get(&key) {
            if old != bytes {
                return Err(recovery::Error::Conflict);
            }
        } else {
            map.insert(key, bytes.to_vec());
        }
        Ok(())
    }
}
pub struct Policy {
    pub gate: Gate,
    pub prepare_calls: u64,
    pub gate_calls: u64,
    pub deny_prepare: bool,
    pub wrong_baseline: bool,
    pub wrong_history: bool,
}
#[derive(Clone)]
pub struct MockAuthority {
    pub blobs: SharedBlobs,
    pub policy: Arc<Mutex<Policy>>,
}
impl Default for MockAuthority {
    fn default() -> Self {
        Self {
            blobs: SharedBlobs::default(),
            policy: Arc::new(Mutex::new(Policy {
                gate: Gate {
                    now: 61,
                    lease_until: 1000,
                    writer_epoch: 4,
                    strategy_generation: 5,
                    config_revision: 6,
                    owner_isolated: true,
                    action_permitted: true,
                    market_valid: true,
                },
                prepare_calls: 0,
                gate_calls: 0,
                deny_prepare: false,
                wrong_baseline: false,
                wrong_history: false,
            })),
        }
    }
}
impl Authority for MockAuthority {
    fn prepare(
        &mut self,
        r: AdmissionRequest<'_>,
    ) -> std::result::Result<recovery::VerifiedClosure, AuthorityError> {
        let mut flags = self.policy.lock().unwrap();
        flags.prepare_calls += 1;
        if flags.deny_prepare {
            return Err(AuthorityError::Denied);
        }
        let RecoveryRecord::Admit { command, .. } = r.record.record() else {
            panic!("Admit")
        };
        let input = recovery::encode_inputs(
            [9; 16],
            1,
            [0; 32],
            &[recovery::InputRecord {
                kind: 1,
                payload: b"mock-observed-quote",
            }],
        )
        .unwrap();
        let mut p = recovery::Persistence::new(self.blobs.clone());
        let mut objects = Vec::new();
        for kind in 1..=15 {
            let bytes = match kind {
                1 => r.record.bytes().to_vec(),
                12 => input.bytes.clone(),
                13 => command.bytes().to_vec(),
                14 if !flags.wrong_baseline => r.baseline.bytes().to_vec(),
                15 if !flags.wrong_history => r.preceding.bytes().to_vec(),
                _ => vec![kind; kind as usize + 1],
            };
            objects.push(recovery::DependencyObject {
                kind,
                content_hash: p.put_object(&bytes).map_err(AuthorityError::Recovery)?,
                byte_length: bytes.len() as u64,
                schema_id: 1,
                owner_identity: [1; 16],
            });
        }
        let m = recovery::RecoveryManifest {
            binding: r.binding,
            logical_strategy_id: command.command().context.strategy.logical_strategy_id,
            instance_id: command.command().context.strategy.instance_id,
            action_grant_ref: objects[3].content_hash,
            operation_ref: objects[6].content_hash,
            mode: recovery::RecoveryMode::DeterministicReplay,
            fault_model_id: [4; 32],
            pin_id: [5; 16],
            input_boundary: input.boundary,
            objects,
            edges: (1..15).map(|i| (0, i)).collect(),
        };
        p.publish(&m).map_err(AuthorityError::Recovery)
    }
    fn verify(
        &mut self,
        r: AdmissionRequest<'_>,
    ) -> std::result::Result<recovery::VerifiedClosure, AuthorityError> {
        recovery::Persistence::new(self.blobs.clone())
            .verify(&r.binding)
            .map_err(AuthorityError::Recovery)
    }
    fn current_gate(
        &mut self,
        _: &CanonicalCommand,
        _: &Admission,
    ) -> std::result::Result<Gate, AuthorityError> {
        let mut p = self.policy.lock().unwrap();
        p.gate_calls += 1;
        Ok(p.gate)
    }
}
#[derive(Default)]
pub struct TestEncoder;
impl Encoder for TestEncoder {
    fn encode(&mut self, c: &CanonicalCommand) -> tickwire_gateway::Result<EncodedRequest> {
        let host = if c.command().context.request.product == Product::Spot {
            "testnet.binance.vision"
        } else {
            "demo-fapi.binance.com"
        };
        EncodedRequest::new(
            c.digest(),
            host.into(),
            format!("POST /test HTTP/1.1\r\nHost: {host}\r\nContent-Length: 0\r\n\r\n")
                .into_bytes(),
        )
    }
}
pub fn peer(host: &str, max_accept: usize) -> (ConnectionConfig, thread::JoinHandle<Vec<u8>>) {
    let cert = rcgen::generate_simple_self_signed(vec![host.into()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert.der().clone()], key.into())
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
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let handle = thread::spawn(move || {
        let deadline = until();
        let socket = loop {
            match listener.accept() {
                Ok((s, _)) => break s,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() > deadline {
                        return Vec::new();
                    }
                    thread::yield_now();
                }
                Err(e) => panic!("{e}"),
            }
        };
        // Darwin inherits O_NONBLOCK from the listening socket.
        socket.set_nonblocking(false).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut stream =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server).unwrap(), socket);
        let mut out = Vec::new();
        let mut b = [0; 4096];
        loop {
            match stream.read(&mut b) {
                Ok(0) | Err(_) => break,
                Ok(n) => out.extend_from_slice(&b[..n]),
            }
        }
        out
    });
    (
        ConnectionConfig {
            address,
            tls,
            limits: Limits {
                max_accept_bytes: max_accept,
                ..Default::default()
            },
            connect_deadline: until(),
        },
        handle,
    )
}
pub type Gateway = AccountGateway<FileBackend, MockAuthority, TestEncoder>;
pub fn make(
    futures: bool,
    max_accept: usize,
) -> (
    Directory,
    CanonicalCommand,
    Gateway,
    MockAuthority,
    thread::JoinHandle<Vec<u8>>,
) {
    let dir = Directory::new();
    let (b, c) = baseline(futures);
    let cfg = config(&b);
    let (connection, peer) = peer(&cfg.route.authority, max_accept);
    let auth = MockAuthority::default();
    let g = AccountGateway::create(
        FileBackend::open(&dir.0).unwrap(),
        cfg,
        b,
        auth.clone(),
        TestEncoder,
        connection,
    )
    .unwrap();
    (dir, c, g, auth, peer)
}
pub fn drive(g: &mut Gateway, wanted: Status) {
    let deadline = until();
    while g.status(intent()) != Some(wanted) {
        assert!(Instant::now() < deadline, "{:?}", g.status(intent()));
        g.poll(Instant::now(), IoBudget::default()).unwrap();
        thread::yield_now();
    }
}
