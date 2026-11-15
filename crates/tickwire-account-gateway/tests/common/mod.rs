#![allow(dead_code)]
#[path = "../../../tickwire-account/tests/common/mod.rs"]
pub mod fixture;

use std::{
    io::{self, Read},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
use tickwire_account::{Admission, IntentKey};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::*;
use tickwire_transport::{IoBudget, Limits, rustls};
use tickwire_venue::{CanonicalCommand, Product};

pub const SPOT_HOST: &str = "spot.venue.invalid";
pub const FUTURES_HOST: &str = "deriv.venue.invalid";

pub fn until() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

pub fn intent() -> IntentKey {
    IntentKey {
        epoch: 4,
        intent_id: 3,
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
        scope: b.snapshot().scope.clone(),
        baseline_digest: b.digest(),
        writer_epoch: 4,
        route: Route {
            venue: 1,
            account: 2,
            environment: tickwire_venue::Environment::Test,
            product,
            authority: if product == Product::Spot {
                SPOT_HOST
            } else {
                FUTURES_HOST
            }
            .into(),
        },
        max_intents: 32,
    }
}

pub struct Policy {
    pub gate: Gate,
    pub authorize_calls: u64,
    pub gate_calls: u64,
    pub deny_authorize: bool,
}

#[derive(Clone)]
pub struct MockAuthority {
    pub policy: Arc<Mutex<Policy>>,
}

impl Default for MockAuthority {
    fn default() -> Self {
        Self {
            policy: Arc::new(Mutex::new(Policy {
                gate: Gate {
                    now: 61,
                    writer_epoch: 4,
                    strategy_generation: 5,
                    config_revision: 6,
                    action_permitted: true,
                    market_valid: true,
                },
                authorize_calls: 0,
                gate_calls: 0,
                deny_authorize: false,
            })),
        }
    }
}

impl Authority for MockAuthority {
    fn authorize(
        &mut self,
        _baseline: &CanonicalBaseline,
        _command: &CanonicalCommand,
        _admission: &Admission,
        _evaluated_at: u64,
    ) -> std::result::Result<(), AuthorityError> {
        let mut p = self.policy.lock().unwrap();
        p.authorize_calls += 1;
        if p.deny_authorize {
            return Err(AuthorityError::Denied);
        }
        Ok(())
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
    fn encode(
        &mut self,
        c: &CanonicalCommand,
    ) -> std::result::Result<EncodedRequest, EncodingError> {
        let host = if c.command().context.request.product == Product::Spot {
            SPOT_HOST
        } else {
            FUTURES_HOST
        };
        EncodedRequest::new(
            c.digest(),
            host,
            format!("POST /test HTTP/1.1\r\nHost: {host}\r\nContent-Length: 0\r\n\r\n")
                .into_bytes(),
        )
        .map_err(|_| EncodingError::InvalidRequest)
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

pub type Gateway = AccountGateway<MockAuthority, TestEncoder>;

pub fn make(
    futures: bool,
    max_accept: usize,
) -> (
    CanonicalCommand,
    Gateway,
    MockAuthority,
    thread::JoinHandle<Vec<u8>>,
) {
    let (b, c) = baseline(futures);
    let cfg = config(&b);
    let (connection, peer) = peer(&cfg.route.authority, max_accept);
    let auth = MockAuthority::default();
    let g = AccountGateway::create(cfg, b, auth.clone(), TestEncoder, connection).unwrap();
    (c, g, auth, peer)
}

pub fn drive(g: &mut Gateway, wanted: Status) {
    let deadline = until();
    while g.status(intent()) != Some(wanted) {
        assert!(Instant::now() < deadline, "{:?}", g.status(intent()));
        g.poll(Instant::now(), IoBudget::default()).unwrap();
        thread::yield_now();
    }
}
