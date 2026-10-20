//! Real sockets and TLS; the server validates actual signed protocol bytes.
mod common;
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::{
    hmac,
    signature::{self, KeyPair},
};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use tickwire_binance::{
    auth::{Algorithm, SigningKey},
    request::*,
    response::*,
};
use tickwire_transport::{self as net, http, mio, rustls, ws};
use tickwire_venue::*;
fn end() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
fn configs() -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.cert.der().clone()], key.into())
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (Arc::new(client), Arc::new(server))
}
fn client(addr: SocketAddr, config: Arc<rustls::ClientConfig>) -> net::Connection {
    let mut c = net::Connection::connect(
        addr,
        net::Security::Tls {
            server_name: "localhost".try_into().unwrap(),
            config,
        },
        net::Limits {
            max_accept_bytes: 17,
            ..Default::default()
        },
        end(),
    )
    .unwrap();
    let mut poll = mio::Poll::new().unwrap();
    c.register(poll.registry(), mio::Token(0)).unwrap();
    let mut events = mio::Events::with_capacity(4);
    let until = end();
    while !c.is_ready() {
        assert!(Instant::now() < until);
        poll.poll(&mut events, Some(Duration::from_millis(2)))
            .unwrap();
        c.drive(Instant::now(), net::IoBudget::default()).unwrap();
    }
    c
}
fn send(c: &mut net::Connection, bytes: &[u8], digest: [u8; 32], p: &PreparedRequest) {
    c.queue(net::PreparedSend::new(bytes, end(), 32768).unwrap())
        .unwrap();
    let mut called = 0;
    let until = end();
    loop {
        assert!(Instant::now() < until);
        let r = c
            .try_send(Instant::now(), || {
                called += 1;
                p.verify_digest(digest).is_ok()
            })
            .unwrap();
        assert!(r.accepted_bytes > 0);
        assert!(!matches!(r.state, net::SendState::NoBytes(_)));
        c.drive(Instant::now(), net::IoBudget::default()).unwrap();
        if c.send_report().unwrap().state == net::SendState::Flushed {
            break;
        }
    }
    assert_eq!(called, 1);
    c.take_finished().unwrap();
}
fn read_http(c: &mut net::Connection) -> http::Response {
    let mut d = http::ResponseDecoder::new(16384, 32).unwrap();
    let until = end();
    loop {
        assert!(Instant::now() < until);
        let r = c.drive(Instant::now(), net::IoBudget::default());
        let mut b = [0; 2048];
        let n = c.read(&mut b);
        d.push(&b[..n]).unwrap();
        if let Some(v) = d.next_response().unwrap() {
            return v;
        }
        r.unwrap();
        thread::yield_now();
    }
}
type Server = rustls::StreamOwned<rustls::ServerConnection, TcpStream>;
fn accept(l: TcpListener, config: Arc<rustls::ServerConfig>) -> Server {
    let (s, _) = l.accept().unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s.set_write_timeout(Some(Duration::from_secs(10))).unwrap();
    rustls::StreamOwned::new(rustls::ServerConnection::new(config).unwrap(), s)
}
fn headers(s: &mut Server) -> String {
    let mut out = Vec::new();
    while !out.ends_with(b"\r\n\r\n") {
        let mut b = [0];
        s.read_exact(&mut b).unwrap();
        out.push(b[0]);
        assert!(out.len() < 32768);
    }
    String::from_utf8(out).unwrap()
}
fn unpercent(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    out
}
fn key(a: Algorithm) -> SigningKey {
    match a {
        Algorithm::HmacSha256 => SigningKey::hmac(b"test-only-secret").unwrap(),
        Algorithm::RsaSha256 => {
            SigningKey::rsa_pkcs8(include_bytes!("data/TEST_ONLY_RSA.pk8.der")).unwrap()
        }
        Algorithm::Ed25519 => {
            SigningKey::ed25519_pkcs8(include_bytes!("data/PUBLIC_BINANCE_ED25519.pk8.der"))
                .unwrap()
        }
    }
}
fn verify(a: Algorithm, preimage: &[u8], sig: &[u8]) {
    match a {
        Algorithm::HmacSha256 => {
            let text = std::str::from_utf8(sig).unwrap();
            let bytes: Vec<u8> = (0..text.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
                .collect();
            hmac::verify(
                &hmac::Key::new(hmac::HMAC_SHA256, b"test-only-secret"),
                preimage,
                &bytes,
            )
            .unwrap();
        }
        Algorithm::Ed25519 => {
            let k = signature::Ed25519KeyPair::from_pkcs8_maybe_unchecked(include_bytes!(
                "data/PUBLIC_BINANCE_ED25519.pk8.der"
            ))
            .unwrap();
            signature::UnparsedPublicKey::new(&signature::ED25519, k.public_key())
                .verify(preimage, &STANDARD.decode(sig).unwrap())
                .unwrap();
        }
        Algorithm::RsaSha256 => {
            let k = signature::RsaKeyPair::from_pkcs8(include_bytes!("data/TEST_ONLY_RSA.pk8.der"))
                .unwrap();
            signature::UnparsedPublicKey::new(
                &signature::RSA_PKCS1_2048_8192_SHA256,
                k.public_key(),
            )
            .verify(preimage, &STANDARD.decode(sig).unwrap())
            .unwrap();
        }
    }
}
fn request(
    a: Algorithm,
    market: Market,
    transport: Transport,
) -> (CanonicalCommand, PreparedRequest) {
    let mut command = common::command();
    // Exercise UTF-8 encoding across the real HTTP path.
    if market == Market::Spot {
        command.context.instrument.symbol = Symbol::new("ＢＴＣUSDT").unwrap();
    } else {
        command.context.request.product = Product::LinearPerpetual;
        command.context.instrument.contract = ContractKind::Perpetual;
        if let CommandBody::New(ref mut o) = command.body {
            o.mode = ProductMode::Derivative {
                position_mode: PositionMode::OneWay,
                margin_mode: MarginMode::Cross,
            };
        }
    }
    let canonical = CanonicalCommand::new(command).unwrap();
    let key = key(a);
    let p = prepare(
        &canonical,
        &Credentials {
            account: 2,
            environment: Environment::Test,
            market,
            api_key: "public-test-key",
            signing_key: &key,
        },
        RequestOptions {
            transport,
            timestamp: Timestamp::Milliseconds(1668481559918),
            recv_window_micros: 5000000,
            request_id: "request-1",
        },
        16384,
    )
    .unwrap();
    (canonical, p)
}
#[test]
fn prepared_rest_all_three_signatures_over_actual_tls_and_chunked_reply() {
    for algorithm in [
        Algorithm::HmacSha256,
        Algorithm::RsaSha256,
        Algorithm::Ed25519,
    ] {
        let (config, server) = configs();
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let h = thread::spawn(move || {
            let mut s = accept(l, server);
            let raw = headers(&mut s);
            assert!(raw.contains("X-MBX-APIKEY: public-test-key\r\n"));
            let path = raw
                .lines()
                .next()
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap();
            let query = path.split_once('?').unwrap().1;
            let (preimage, sig) = query.rsplit_once("&signature=").unwrap();
            assert!(preimage.starts_with("symbol=%EF%BC%A2%EF%BC%B4%EF%BC%A3USDT&"));
            assert!(preimage.contains("quantity=0.001"));
            verify(algorithm, preimage.as_bytes(), &unpercent(sig));
            let body=br#"{"symbol":"BTCUSDT","orderId":9007199254740993,"clientOrderId":"test-1","status":"NEW","executedQty":"0.000"}"#;
            s.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                .unwrap();
            for piece in body.chunks(7) {
                write!(s, "{:x}\r\n", piece.len()).unwrap();
                s.write_all(piece).unwrap();
                s.write_all(b"\r\n").unwrap();
                s.flush().unwrap();
            }
            s.write_all(b"0\r\n\r\n").unwrap();
            s.flush().unwrap();
            s.conn.send_close_notify();
            s.flush().unwrap();
        });
        let (canonical, p) = request(algorithm, Market::Spot, Transport::Rest);
        let mut c = client(addr, config);
        let bytes = http::encode_request(
            p.method().as_str(),
            p.path(),
            &[
                ("Host", p.authority()),
                ("X-MBX-APIKEY", p.api_key_header().unwrap()),
            ],
            p.payload(),
            32768,
        )
        .unwrap();
        send(&mut c, &bytes, canonical.digest(), &p);
        let response = read_http(&mut c);
        let envelope = decode_rest(Market::Spot, response.status, &response.body).unwrap();
        let order = decode_order(envelope.result.unwrap().get().as_bytes()).unwrap();
        assert_eq!(order.order_id, 9007199254740993);
        assert_eq!(order.status.as_deref(), Some("NEW"));
        h.join().unwrap();
    }
}
fn frame(s: &mut Server) -> Vec<u8> {
    let mut h = [0; 2];
    s.read_exact(&mut h).unwrap();
    assert_eq!(h[0], 0x81);
    assert_ne!(h[1] & 0x80, 0);
    let n = match h[1] & 127 {
        126 => {
            let mut b = [0; 2];
            s.read_exact(&mut b).unwrap();
            u16::from_be_bytes(b) as usize
        }
        127 => panic!("unexpected oversized frame"),
        n => n as usize,
    };
    assert!(n <= 16384);
    let mut mask = [0; 4];
    s.read_exact(&mut mask).unwrap();
    let mut out = vec![0; n];
    s.read_exact(&mut out).unwrap();
    for (i, b) in out.iter_mut().enumerate() {
        *b ^= mask[i % 4];
    }
    out
}
#[test]
fn prepared_usdm_websocket_hmac_ed25519_and_unknown_response_over_actual_tls() {
    use sha1::Digest;
    for algorithm in [Algorithm::HmacSha256, Algorithm::Ed25519] {
        let (config, server) = configs();
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let h = thread::spawn(move || {
            let mut s = accept(l, server);
            let raw = headers(&mut s);
            assert!(raw.starts_with("GET /ws-fapi/v1 HTTP/1.1\r\n"));
            let key = raw
                .lines()
                .find_map(|l| l.strip_prefix("Sec-WebSocket-Key: "))
                .unwrap();
            let accept = STANDARD.encode(sha1::Sha1::digest(
                format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes(),
            ));
            write!(s,"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").unwrap();
            s.flush().unwrap();
            let v: serde_json::Value = serde_json::from_slice(&frame(&mut s)).unwrap();
            assert_eq!(v["method"], "order.place");
            assert_eq!(v["params"]["price"], "60000");
            assert_eq!(v["params"]["quantity"], "0.001");
            let fields: std::collections::BTreeMap<_, _> = v["params"]
                .as_object()
                .unwrap()
                .iter()
                .filter(|(k, _)| k.as_str() != "signature")
                .collect();
            let preimage = fields
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{k}={}",
                        v.as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| v.to_string())
                    )
                })
                .collect::<Vec<_>>()
                .join("&");
            verify(
                algorithm,
                preimage.as_bytes(),
                v["params"]["signature"].as_str().unwrap().as_bytes(),
            );
            let reply=br#"{"id":"request-1","status":503,"error":{"code":-1007,"msg":"Timeout waiting for response from backend server."}}"#;
            let split = 40;
            for (op, piece) in [(0x01, &reply[..split]), (0x80, &reply[split..])] {
                s.write_all(&[op, piece.len() as u8]).unwrap();
                s.write_all(piece).unwrap();
                s.flush().unwrap();
            }
            s.conn.send_close_notify();
            s.flush().unwrap();
        });
        let (canonical, p) = request(algorithm, Market::Usdm, Transport::WebSocket);
        let mut c = client(addr, config);
        let handshake = ws::ClientHandshake::new(p.authority(), p.path(), &[], 4096).unwrap();
        send(&mut c, handshake.request(), canonical.digest(), &p);
        handshake.validate(&read_http(&mut c)).unwrap();
        send(
            &mut c,
            &ws::encode_client(ws::Opcode::Text, p.payload(), 32768).unwrap(),
            canonical.digest(),
            &p,
        );
        let mut decoder = ws::Decoder::new(16384, 16384).unwrap();
        let until = end();
        loop {
            assert!(Instant::now() < until);
            let r = c.drive(Instant::now(), net::IoBudget::default());
            let mut b = [0; 1024];
            let n = c.read(&mut b);
            decoder.push(&b[..n]).unwrap();
            match decoder.poll_message().unwrap() {
                ws::PollMessage::Message(msg) => {
                    let response = decode_websocket(Market::Usdm, &msg.payload).unwrap();
                    assert_eq!(response.failure, Some(FailureKind::Unknown));
                    assert_eq!(response.request_id.unwrap().get(), "\"request-1\"");
                    break;
                }
                ws::PollMessage::Yielded => continue,
                ws::PollMessage::NeedInput => {}
            }
            r.unwrap();
            thread::yield_now();
        }
        h.join().unwrap();
    }
}
