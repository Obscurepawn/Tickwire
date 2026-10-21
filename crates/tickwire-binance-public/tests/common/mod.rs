#![allow(dead_code)]
#[path = "../../../tickwire-binance/tests/common/mod.rs"]
mod venue_common;
use base64::{Engine, engine::general_purpose::STANDARD};
use sha1::{Digest, Sha1};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
pub use tickwire_binance_public::*;
pub const NS: u64 = 1_000_000_000;
pub type Server = rustls::StreamOwned<rustls::ServerConnection, TcpStream>;
pub fn domain() -> ClockDomain {
    ClockDomain::new([7; 16], 1).unwrap()
}
pub struct Clock(pub Instant);
impl MonotonicClock for Clock {
    fn sample(&mut self) -> tickwire_binance_clock::Result<LocalTime> {
        Ok(LocalTime {
            instant: Instant::now(),
            monotonic: MonoTime::new(domain(), self.0.elapsed().as_nanos() as u64),
        })
    }
}
#[derive(Default)]
pub struct Permit {
    pub charges: Vec<PublicCharge>,
    pub feedback: Vec<u16>,
}
impl PublicPermit for Permit {
    fn allow(&mut self, c: &PublicCharge, _: MonoTime) -> bool {
        self.charges.push(*c);
        true
    }
    fn observe_http(&mut self, f: &HttpFeedback) {
        self.feedback.push(f.status);
    }
}
#[derive(Default)]
pub struct Sink {
    pub outcomes: Vec<SyncOutcome>,
    pub gaps: Vec<GapNotice>,
    pub stamps: Vec<u64>,
}
impl PublicSink for Sink {
    fn on_notice(&mut self, n: Notice<'_>) -> bool {
        match n {
            Notice::Sync {
                outcome,
                received_mono_ns,
                ..
            } => {
                self.outcomes.push(outcome);
                if let Some(t) = received_mono_ns {
                    self.stamps.push(t);
                }
            }
            Notice::Gap(g) => self.gaps.push(g),
            Notice::Http(_) => {}
        }
        true
    }
}
pub fn config<P: DepthPolicy>(source: Source, mode: SubscribeMode) -> Config {
    let mut instrument = venue_common::command().context.instrument;
    if P::MARKET == tickwire_binance::request::Market::Usdm {
        instrument.contract = tickwire_venue::ContractKind::Perpetual;
    }
    Config {
        owner_id: [1; 16],
        source,
        domain: domain(),
        first_connection_epoch: 1,
        first_operation: 1,
        depth: DepthConfig {
            instrument,
            key: tickwire_book::BookKey {
                venue: 1,
                instrument: 12,
                source: 1,
            },
            time_unit: TimeUnit::Milliseconds,
            snapshot_limit: 5,
            max_message_bytes: 4096,
            initial_book_generation: 0,
            previous_connection_epoch: 0,
        },
        subscribe: mode,
        speed: DepthSpeed::Default,
        timing: Timing {
            connect_ns: 3 * NS,
            request_ns: 3 * NS,
            first_event_ns: 3 * NS,
            heartbeat_ns: 30 * NS,
            pong_ns: NS,
            rotate_ns: 82_800 * NS,
            backoff_ns: NS / 100,
            max_backoff_ns: NS,
        },
        limits: Limits {
            io: IoLimits {
                max_accept_bytes: 13,
                ..IoLimits::default()
            },
            request_bytes: 4096,
            message_bytes: 4096,
            http_bytes: 8192,
            http_headers: 32,
            control_frames: 8,
            timestamp_segments: 1024,
            total_buffer_bytes: 16 << 20,
        },
    }
}
pub fn certs() -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let names = [
        "stream.binance.com",
        "stream-sbe.binance.com",
        "stream.testnet.binance.vision",
        "fstream.binance.com",
        "demo-fstream.binance.com",
        "api.binance.com",
        "testnet.binance.vision",
        "fapi.binance.com",
        "demo-fapi.binance.com",
    ];
    let c = rcgen::generate_simple_self_signed(
        names.into_iter().map(str::to_owned).collect::<Vec<_>>(),
    )
    .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(c.cert.der().clone()).unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![c.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(c.signing_key.serialize_der()).into(),
        )
        .unwrap();
    (Arc::new(client), Arc::new(server))
}
pub fn listener() -> TcpListener {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.set_nonblocking(true).unwrap();
    l
}
pub fn accept(l: &TcpListener, tls: Arc<rustls::ServerConfig>) -> Server {
    let end = Instant::now() + Duration::from_secs(5);
    let s = loop {
        match l.accept() {
            Ok((s, _)) => break s,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < end, "client did not connect");
                thread::sleep(Duration::from_millis(1));
            }
            Err(e) => panic!("{e}"),
        }
    };
    s.set_nonblocking(false).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    rustls::StreamOwned::new(rustls::ServerConnection::new(tls).unwrap(), s)
}
pub fn headers(s: &mut Server) -> String {
    let mut b = Vec::new();
    while !b.ends_with(b"\r\n\r\n") {
        let mut x = [0];
        s.read_exact(&mut x).unwrap();
        b.push(x[0]);
        assert!(b.len() < 8192);
    }
    String::from_utf8(b).unwrap()
}
pub fn upgrade(s: &mut Server, path: &str, host: &str) {
    let h = headers(s);
    assert!(h.starts_with(&format!("GET {path} HTTP/1.1\r\n")), "{h}");
    assert!(h.lines().any(|l| l == format!("Host: {host}")), "{h}");
    assert!(!h.to_lowercase().contains("api-key"));
    let key = h
        .lines()
        .find_map(|l| {
            l.split_once(':')
                .filter(|(n, _)| n.eq_ignore_ascii_case("sec-websocket-key"))
                .map(|(_, v)| v.trim())
        })
        .unwrap();
    let a = STANDARD.encode(Sha1::digest(format!(
        "{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    )));
    write!(s,"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {a}\r\n\r\n").unwrap();
    s.flush().unwrap();
}
pub fn frame(s: &mut Server, code: u8, b: &[u8]) {
    s.write_all(&[0x80 | code]).unwrap();
    if b.len() < 126 {
        s.write_all(&[b.len() as u8]).unwrap();
    } else {
        s.write_all(&[126]).unwrap();
        s.write_all(&(b.len() as u16).to_be_bytes()).unwrap();
    }
    s.write_all(b).unwrap();
    s.flush().unwrap();
}
pub fn client_frame(s: &mut Server) -> Vec<u8> {
    let (code, bytes) = read_frame(s);
    assert_eq!(code, 1);
    bytes
}
pub fn read_frame(s: &mut Server) -> (u8, Vec<u8>) {
    let mut h = [0; 2];
    s.read_exact(&mut h).unwrap();
    assert_ne!(h[1] & 128, 0);
    assert_eq!(h[0] & 128, 128);
    assert!(h[1] & 127 < 126);
    let mut mask = [0; 4];
    s.read_exact(&mut mask).unwrap();
    let mut b = vec![0; (h[1] & 127) as usize];
    s.read_exact(&mut b).unwrap();
    for (i, v) in b.iter_mut().enumerate() {
        *v ^= mask[i % 4];
    }
    (h[0] & 15, b)
}
pub fn delta<P: DepthPolicy>(first: u64, last: u64, previous: u64) -> Vec<u8> {
    let extra = if P::MARKET == tickwire_binance::request::Market::Usdm {
        format!(r#","T":1,"pu":{previous},"st":1"#)
    } else {
        String::new()
    };
    format!(r#"{{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":{first},"u":{last},"b":[["99","2"]],"a":[]{extra}}}"#).into_bytes()
}
pub fn snapshot<P: DepthPolicy>(id: u64) -> Vec<u8> {
    let extra = if P::MARKET == tickwire_binance::request::Market::Usdm {
        r#","E":1,"T":1"#
    } else {
        ""
    };
    format!(r#"{{"lastUpdateId":{id},"bids":[["99","1"]],"asks":[["101","1"]]{extra}}}"#)
        .into_bytes()
}
pub fn reply(s: &mut Server, b: &[u8]) {
    write!(
        s,
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nX-MBX-USED-WEIGHT-1M: 5\r\n\r\n",
        b.len()
    )
    .unwrap();
    s.write_all(b).unwrap();
    s.flush().unwrap();
}
