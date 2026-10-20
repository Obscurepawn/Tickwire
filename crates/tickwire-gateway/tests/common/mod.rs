#![allow(dead_code)]
mod command;
pub use command::command;
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read},
    net::TcpListener,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_business_journal::{self as wal, AuthorizationError, AuthorizationRequest, Backend};
use tickwire_execution as ex;
use tickwire_gateway::{
    Authorization, Config, ConnectionConfig, EncodedRequest, Encoder, JournaledSender, Route,
    Status,
};
use tickwire_journal::store::Store;
use tickwire_recovery::{self as recovery, BlobStore};
use tickwire_transport::{self as net, rustls};
use tickwire_venue::*;
static NEXT: AtomicU64 = AtomicU64::new(0);
pub fn end() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
pub struct Directory(pub PathBuf);
impl Directory {
    pub fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "tickwire-gateway-{}-{}",
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
pub fn config() -> Config {
    Config {
        business: wal::BusinessConfig {
            physical: tickwire_journal::Config::new([2; 16], [1; 16]),
            domain: ex::Domain {
                venue: 1,
                account: 2,
                product: ex::Product::Spot,
            },
            initial_capital: 1_000_000,
        },
        writer_epoch: 4,
        route: Route {
            venue: 1,
            account: 2,
            environment: Environment::Test,
            product: Product::Spot,
            authority: "testnet.binance.vision".into(),
        },
        max_intents: 32,
    }
}
pub fn gate() -> ex::Gate {
    ex::Gate {
        epoch: 4,
        strategy_generation: 5,
        config_revision: 6,
        now: 51,
        lease_until: 110,
        enabled: true,
        market_valid: true,
        account_valid: true,
        recovered: true,
        owner_isolated: true,
    }
}
pub fn canonical() -> CanonicalCommand {
    CanonicalCommand::new(command()).unwrap()
}
pub fn record(c: &CanonicalCommand) -> ex::DurableRecord {
    ex::DurableRecord {
        sequence: 1,
        request: ex::OrderRequest {
            key: ex::OrderKey {
                venue: 1,
                account: 2,
                product: ex::Product::Spot,
                intent: 3,
            },
            command_digest: c.digest(),
            quantity: 100_000,
            unit_budget: 6,
            fee_budget: 1,
            epoch: 4,
            strategy_generation: 5,
            config_revision: 6,
            deadline: 100,
            recovery_object: 7,
            commit_profile: ex::CommitProfile::LocalDurable,
        },
    }
}
#[derive(Clone, Default)]
pub struct Blobs(pub Arc<Mutex<BTreeMap<recovery::StoreKey, Vec<u8>>>>);
impl BlobStore for Blobs {
    fn get(&self, k: recovery::StoreKey, max: usize) -> recovery::Result<Vec<u8>> {
        let m = self.0.lock().unwrap();
        let v = m.get(&k).ok_or(recovery::Error::Missing)?;
        if v.len() > max {
            return Err(recovery::Error::Capacity);
        }
        Ok(v.clone())
    }
    fn put_sync(&mut self, k: recovery::StoreKey, v: &[u8]) -> recovery::Result<()> {
        let mut m = self.0.lock().unwrap();
        if let Some(old) = m.get(&k) {
            if old != v {
                return Err(recovery::Error::Conflict);
            }
        } else {
            m.insert(k, v.to_vec());
        }
        Ok(())
    }
}
pub struct PolicyState {
    pub gate: ex::Gate,
    pub gate_calls: usize,
    pub deny: bool,
    pub wrong_command: bool,
}
#[derive(Clone)]
pub struct MockAuthorization {
    pub blobs: Blobs,
    pub state: Arc<Mutex<PolicyState>>,
}
impl Default for MockAuthorization {
    fn default() -> Self {
        Self {
            blobs: Blobs::default(),
            state: Arc::new(Mutex::new(PolicyState {
                gate: gate(),
                gate_calls: 0,
                deny: false,
                wrong_command: false,
            })),
        }
    }
}
fn input_chunk() -> (Vec<u8>, recovery::InputBoundary) {
    let stream = [9; 16];
    let previous = [0; 32];
    let mut bytes = b"TWINP001".to_vec();
    bytes.extend(stream);
    bytes.extend(1u64.to_be_bytes());
    bytes.extend(1u32.to_be_bytes());
    bytes.extend(previous);
    let mut row = 1u64.to_be_bytes().to_vec();
    row.push(1);
    row.extend(5u32.to_be_bytes());
    row.extend(b"quote");
    let mut pre = b"tickwire.input-row.v1".to_vec();
    pre.extend(previous);
    pre.extend(&row);
    let last_hash = recovery::digest(&pre);
    bytes.extend(row);
    (
        bytes,
        recovery::InputBoundary {
            stream,
            first_sequence: 1,
            previous_hash: previous,
            last_sequence: 1,
            last_hash,
        },
    )
}
impl Authorization for MockAuthorization {
    fn authorize(
        &mut self,
        command: &CanonicalCommand,
        r: AuthorizationRequest<'_>,
    ) -> Result<recovery::VerifiedClosure, AuthorizationError> {
        if self.state.lock().unwrap().deny {
            return Err(AuthorizationError::Denied);
        }
        let mut p = recovery::Persistence::new(self.blobs.clone());
        let mut objects = Vec::new();
        for kind in 1..=11 {
            let b = if kind == 1 {
                r.intent_payload.to_vec()
            } else {
                vec![kind; kind as usize + 1]
            };
            let hash = p.put_object(&b).map_err(AuthorizationError::Recovery)?;
            objects.push(recovery::DependencyObject {
                kind,
                content_hash: hash,
                byte_length: b.len() as u64,
                schema_id: 1,
                owner_identity: [1; 16],
            });
        }
        let (b, boundary) = input_chunk();
        let h = p.put_object(&b).map_err(AuthorizationError::Recovery)?;
        objects.push(recovery::DependencyObject {
            kind: 12,
            content_hash: h,
            byte_length: b.len() as u64,
            schema_id: 1,
            owner_identity: [1; 16],
        });
        let bytes = if self.state.lock().unwrap().wrong_command {
            b"wrong command".as_slice()
        } else {
            command.bytes()
        };
        let h = p.put_object(bytes).map_err(AuthorizationError::Recovery)?;
        objects.push(recovery::DependencyObject {
            kind: 13,
            content_hash: h,
            byte_length: bytes.len() as u64,
            schema_id: 1,
            owner_identity: [1; 16],
        });
        let edges = (1..objects.len() as u16).map(|i| (0, i)).collect();
        let m = recovery::RecoveryManifest {
            binding: *r.binding,
            logical_strategy_id: 8,
            instance_id: [2; 16],
            action_grant_ref: objects[3].content_hash,
            operation_ref: objects[6].content_hash,
            mode: recovery::RecoveryMode::DeterministicReplay,
            fault_model_id: [4; 32],
            pin_id: [5; 16],
            input_boundary: boundary,
            objects,
            edges,
        };
        p.publish(&m).map_err(AuthorizationError::Recovery)
    }
    fn current_gate(
        &mut self,
        _: &CanonicalCommand,
        _: ex::DurableRecord,
    ) -> Result<ex::Gate, AuthorizationError> {
        let mut s = self.state.lock().unwrap();
        s.gate_calls += 1;
        if s.deny {
            Err(AuthorizationError::Denied)
        } else {
            Ok(s.gate)
        }
    }
    fn recover(
        &mut self,
        r: AuthorizationRequest<'_>,
    ) -> Result<(CanonicalCommand, recovery::VerifiedClosure), AuthorizationError> {
        let cap = recovery::Persistence::new(self.blobs.clone())
            .verify(r.binding)
            .map_err(AuthorizationError::Recovery)?;
        let bytes = self
            .blobs
            .get(
                recovery::StoreKey::Object(r.record.request.command_digest),
                16 * 1024,
            )
            .map_err(AuthorizationError::Recovery)?;
        let command = CanonicalCommand::decode(&bytes).map_err(|_| AuthorizationError::Denied)?;
        Ok((command, cap))
    }
}
#[derive(Default)]
pub struct TestEncoder {
    pub wrong_digest: bool,
    pub wrong_host: bool,
}
impl Encoder for TestEncoder {
    fn encode(&mut self, c: &CanonicalCommand) -> tickwire_gateway::Result<EncodedRequest> {
        EncodedRequest::new(
            if self.wrong_digest {
                [9; 32]
            } else {
                c.digest()
            },
            if self.wrong_host {
                "other.invalid"
            } else {
                "testnet.binance.vision"
            }
            .into(),
            b"POST /test HTTP/1.1\r\nHost: testnet.binance.vision\r\nContent-Length: 0\r\n\r\n"
                .to_vec(),
        )
    }
}
pub struct FailingBackend {
    inner: wal::FileBackend,
    pub fail: Arc<AtomicBool>,
}
impl FailingBackend {
    pub fn open(p: &Directory) -> Self {
        Self {
            inner: wal::FileBackend::open(&p.0).unwrap(),
            fail: Arc::new(AtomicBool::new(false)),
        }
    }
}
impl Store for FailingBackend {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.inner.read_manifest()
    }
    fn publish_manifest(&mut self, b: &[u8]) -> io::Result<()> {
        self.inner.publish_manifest(b)
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        self.inner.segment_ids()
    }
    fn read_segment(&mut self, id: u64, max: u64) -> io::Result<Vec<u8>> {
        self.inner.read_segment(id, max)
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        self.inner.create_segment(id)
    }
    fn append_segment(&mut self, id: u64, offset: u64, b: &[u8]) -> io::Result<()> {
        self.inner.append_segment(id, offset, b)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            Err(io::Error::other("injected sync failure"))
        } else {
            self.inner.sync_segment(id)
        }
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.inner.truncate_segment(id, len)
    }
}
impl Backend for FailingBackend {
    fn read_bootstrap(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.inner.read_bootstrap()
    }
    fn publish_bootstrap(&mut self, b: &[u8]) -> io::Result<()> {
        self.inner.publish_bootstrap(b)
    }
}
pub fn peer(max_accept: usize) -> (ConnectionConfig, thread::JoinHandle<Vec<u8>>) {
    let cert = rcgen::generate_simple_self_signed(vec!["testnet.binance.vision".into()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert.der().clone()], key.into())
            .unwrap(),
    );
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = l.local_addr().unwrap();
    l.set_nonblocking(true).unwrap();
    let h = thread::spawn(move || {
        let until = end();
        let socket = loop {
            match l.accept() {
                Ok((s, _)) => break s,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() >= until {
                        return Vec::new();
                    }
                    thread::yield_now()
                }
                Err(e) => panic!("accept {e}"),
            }
        };
        socket.set_nonblocking(false).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut tls =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server).unwrap(), socket);
        let mut out = Vec::new();
        let mut b = [0; 4096];
        loop {
            match tls.read(&mut b) {
                Ok(0) | Err(_) => return out,
                Ok(n) => out.extend_from_slice(&b[..n]),
            }
        }
    });
    (
        ConnectionConfig {
            address,
            tls: client,
            limits: net::Limits {
                max_accept_bytes: max_accept,
                ..Default::default()
            },
            connect_deadline: end(),
        },
        h,
    )
}
pub fn drive<S: Backend, A: Authorization, E: Encoder>(
    s: &mut JournaledSender<S, A, E>,
    wanted: Status,
) {
    let until = end();
    while s.status(3) != Some(wanted) {
        assert!(Instant::now() < until, "status {:?}", s.status(3));
        s.poll(Instant::now(), net::IoBudget::default()).unwrap();
        thread::yield_now();
    }
}
