#![allow(dead_code, unused_imports)]
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::signature::KeyPair;
use sha1::{Digest, Sha1};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
use tickwire_binance_private::*;
pub const KEY: &str = "TEST-PRIVATE-KEY";
pub const SECRET: &[u8] = b"TEST-PRIVATE-SECRET";
pub const ED: &[u8] =
    include_bytes!("../../../tickwire-binance/tests/data/PUBLIC_BINANCE_ED25519.pk8.der");
pub const RSA: &[u8] = include_bytes!("../../../tickwire-binance/tests/data/TEST_ONLY_RSA.pk8.der");
pub type Server = rustls::StreamOwned<rustls::ServerConnection, TcpStream>;
pub fn scope(market: Market) -> AccountScope {
    AccountScope {
        risk_domain_id: [1; 16],
        venue: 1,
        environment: Environment::Test,
        account: 2,
        market,
        credential_generation: 1,
    }
}
pub fn certs() -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let cert = rcgen::generate_simple_self_signed(vec![
        "ws-api.testnet.binance.vision".into(),
        "demo-fapi.binance.com".into(),
        "demo-fstream.binance.com".into(),
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
pub fn listener() -> TcpListener {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.set_nonblocking(true).unwrap();
    l
}
pub fn accept(l: &TcpListener, c: Arc<rustls::ServerConfig>) -> Server {
    let end = Instant::now() + Duration::from_secs(8);
    let socket = loop {
        match l.accept() {
            Ok((s, _)) => break s,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < end);
                thread::yield_now();
            }
            Err(e) => panic!("{e}"),
        }
    };
    socket.set_nonblocking(false).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(8)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(8)))
        .unwrap();
    rustls::StreamOwned::new(rustls::ServerConnection::new(c).unwrap(), socket)
}
pub fn headers(s: &mut Server) -> String {
    let mut b = Vec::new();
    while !b.ends_with(b"\r\n\r\n") {
        let mut x = [0];
        s.read_exact(&mut x).unwrap();
        b.push(x[0]);
        assert!(b.len() < 16384);
    }
    String::from_utf8(b).unwrap()
}
pub fn upgrade(s: &mut Server, path: &str) {
    let h = headers(s);
    assert!(h.starts_with(&format!("GET {path} HTTP/1.1\r\n")));
    let key = h
        .lines()
        .find_map(|l| {
            l.split_once(':')
                .filter(|(n, _)| n.eq_ignore_ascii_case("sec-websocket-key"))
                .map(|(_, v)| v.trim())
        })
        .unwrap();
    let accept = STANDARD.encode(Sha1::digest(format!(
        "{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    )));
    write!(s,"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").unwrap();
    s.flush().unwrap();
}
pub fn frame(s: &mut Server, opcode: u8, bytes: &[u8]) {
    s.write_all(&[0x80 | opcode]).unwrap();
    if bytes.len() < 126 {
        s.write_all(&[bytes.len() as u8]).unwrap();
    } else {
        s.write_all(&[126]).unwrap();
        s.write_all(&(bytes.len() as u16).to_be_bytes()).unwrap();
    }
    s.write_all(bytes).unwrap();
    s.flush().unwrap();
}
pub fn read_frame(s: &mut Server) -> (u8, Vec<u8>) {
    let mut h = [0; 2];
    s.read_exact(&mut h).unwrap();
    assert!(h[1] & 0x80 != 0);
    let len = match h[1] & 127 {
        126 => {
            let mut b = [0; 2];
            s.read_exact(&mut b).unwrap();
            u16::from_be_bytes(b) as usize
        }
        127 => panic!("unexpected large client frame"),
        n => n as usize,
    };
    assert!(len <= 16384);
    let mut mask = [0; 4];
    s.read_exact(&mut mask).unwrap();
    let mut b = vec![0; len];
    s.read_exact(&mut b).unwrap();
    for (i, v) in b.iter_mut().enumerate() {
        *v ^= mask[i % 4];
    }
    (h[0] & 15, b)
}
pub fn json(s: &mut Server) -> serde_json::Value {
    let (op, b) = read_frame(s);
    assert_eq!(op, 1);
    serde_json::from_slice(&b).unwrap()
}
pub fn reply(s: &mut Server, id: &serde_json::Value, result: serde_json::Value) {
    frame(
        s,
        1,
        &serde_json::to_vec(&serde_json::json!({"id":id,"status":200,"result":result})).unwrap(),
    );
}
pub fn verify_signature(j: &serde_json::Value, algorithm: u8) {
    let p = j["params"].as_object().unwrap();
    let mut keys: Vec<_> = p.keys().filter(|k| k.as_str() != "signature").collect();
    keys.sort();
    let preimage = keys
        .into_iter()
        .map(|k| {
            format!(
                "{k}={}",
                p[k].as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| p[k].to_string())
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    assert_eq!(p["apiKey"], KEY);
    let sig = p["signature"].as_str().unwrap();
    match algorithm {
        0 => {
            let mac = ring::hmac::sign(
                &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, SECRET),
                preimage.as_bytes(),
            );
            let hex: String = mac.as_ref().iter().map(|b| format!("{b:02x}")).collect();
            assert_eq!(sig, hex);
        }
        1 => {
            let p = ring::signature::Ed25519KeyPair::from_pkcs8_maybe_unchecked(ED).unwrap();
            ring::signature::UnparsedPublicKey::new(
                &ring::signature::ED25519,
                p.public_key().as_ref(),
            )
            .verify(preimage.as_bytes(), &STANDARD.decode(sig).unwrap())
            .unwrap();
        }
        2 => {
            let p = ring::signature::RsaKeyPair::from_pkcs8(RSA).unwrap();
            ring::signature::UnparsedPublicKey::new(
                &ring::signature::RSA_PKCS1_2048_8192_SHA256,
                p.public_key().as_ref(),
            )
            .verify(preimage.as_bytes(), &STANDARD.decode(sig).unwrap())
            .unwrap();
        }
        _ => unreachable!(),
    }
}
pub struct TestClock {
    pub start: Instant,
}
impl Clock for TestClock {
    fn check_signed(
        &mut self,
        s: &AccountScope,
        t: &SignedTime,
        n: &ClockSample,
        w: u64,
    ) -> Result<()> {
        check_test_stamp(s, t, n, w)
    }
    fn sample(&mut self) -> Result<ClockSample> {
        let ns = self.start.elapsed().as_nanos() as u64;
        Ok(test_sample(ns, 1_770_000_000_000 + ns / 1_000_000))
    }
}

pub fn test_domain() -> ClockDomain {
    ClockDomain::new([42; 16], 1).unwrap()
}
pub fn calibrated(
    domain: ClockDomain,
    source: tickwire_clock::Source,
    ns: u64,
    unix_ms: u64,
) -> tickwire_clock::Calibrator {
    let mut c = tickwire_clock::Calibrator::new(
        tickwire_clock::CalibrationConfig {
            max_round_trip_ns: 1_000_000,
            max_width_ns: 10_000_000,
            max_age_ns: 60_000_000_000,
            quantization_error_ns: 1_000_000,
            drift_ppb: 0,
            max_adjustment_ns: 10_000_000,
        },
        source,
        domain,
        1,
    )
    .unwrap();
    let mono = MonoTime::new(domain, ns);
    let t = c.begin(mono).unwrap();
    c.finish(&t, mono, unix_ms).unwrap();
    c
}
pub fn test_sample(ns: u64, unix_ms: u64) -> ClockSample {
    let mono = MonoTime::new(test_domain(), ns);
    let mut c = calibrated(test_domain(), tickwire_clock::Source::SpotTest, ns, unix_ms);
    ClockSample {
        instant: Instant::now(),
        monotonic: mono,
        signing_time: Some(
            c.sign_time(mono, tickwire_clock::TimeUnit::Milliseconds)
                .unwrap(),
        ),
    }
}
pub fn check_test_stamp(
    scope: &AccountScope,
    stamp: &SignedTime,
    now: &ClockSample,
    window: u64,
) -> Result<()> {
    let source = match (scope.market, scope.environment) {
        (Market::Spot, Environment::Test) => tickwire_clock::Source::SpotTest,
        (Market::Spot, Environment::Production) => tickwire_clock::Source::SpotProduction,
        (Market::Usdm, Environment::Test) => tickwire_clock::Source::UsdmTest,
        (Market::Usdm, Environment::Production) => tickwire_clock::Source::UsdmProduction,
    };
    let mut c = calibrated(
        now.monotonic.domain(),
        source,
        now.monotonic_ns(),
        1_770_000_000_000 + now.monotonic_ns() / 1_000_000,
    );
    c.check_send(stamp, now.monotonic, window * 1000, 1000)
        .map_err(|_| Error::Clock)
}
