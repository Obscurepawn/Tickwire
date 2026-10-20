mod common;
use base64::{Engine, engine::general_purpose::STANDARD};
use sha1::{Digest, Sha1};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use tickwire_binance::{depth_sync::*, feed::TimeUnit, request::Market};
use tickwire_book::{BinanceSpot, BinanceUsdM, BookKey};
use tickwire_transport::{self as net, http, rustls, ws};
use tickwire_venue::ContractKind;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
type Server = rustls::StreamOwned<rustls::ServerConnection, TcpStream>;
fn certificates() -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
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
fn accept(listener: &TcpListener, config: Arc<rustls::ServerConfig>) -> Server {
    let (socket, _) = listener.accept().unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    socket
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    rustls::StreamOwned::new(rustls::ServerConnection::new(config).unwrap(), socket)
}
fn headers(server: &mut Server) -> String {
    let mut out = Vec::new();
    while !out.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        server.read_exact(&mut byte).unwrap();
        out.push(byte[0]);
        assert!(out.len() < 16384);
    }
    String::from_utf8(out).unwrap()
}
fn client(address: std::net::SocketAddr, config: Arc<rustls::ClientConfig>) -> net::Connection {
    let mut conn = net::Connection::connect(
        address,
        net::Security::Tls {
            server_name: "localhost".try_into().unwrap(),
            config,
        },
        net::Limits {
            max_accept_bytes: 13,
            ..Default::default()
        },
        deadline(),
    )
    .unwrap();
    let end = deadline();
    while !conn.is_ready() {
        assert!(Instant::now() < end);
        conn.drive(Instant::now(), net::IoBudget::default())
            .unwrap();
        thread::yield_now();
    }
    conn
}
fn send(conn: &mut net::Connection, bytes: &[u8]) {
    conn.queue(net::PreparedSend::new(bytes, deadline(), 16384).unwrap())
        .unwrap();
    let end = deadline();
    loop {
        assert!(Instant::now() < end);
        conn.try_send(Instant::now(), || true).unwrap();
        conn.drive(Instant::now(), net::IoBudget::default())
            .unwrap();
        if conn.send_report().unwrap().state == net::SendState::Flushed {
            break;
        }
        thread::yield_now();
    }
    conn.take_finished().unwrap();
}
fn response(conn: &mut net::Connection) -> http::Response {
    let mut decoder = http::ResponseDecoder::new(16384, 32).unwrap();
    let end = deadline();
    loop {
        assert!(Instant::now() < end);
        let drive = conn.drive(Instant::now(), net::IoBudget::default());
        let mut out = [0; 37];
        let n = conn.read(&mut out);
        decoder.push(&out[..n]).unwrap();
        if let Some(response) = decoder.next_response().unwrap() {
            return response;
        }
        if n == 0 {
            drive.unwrap();
        }
        thread::yield_now();
    }
}
fn masked_message(server: &mut Server) -> Vec<u8> {
    let mut head = [0; 2];
    server.read_exact(&mut head).unwrap();
    assert_eq!(head[0], 0x81);
    assert_ne!(head[1] & 0x80, 0);
    let len = usize::from(head[1] & 0x7f);
    assert!(len < 126);
    let mut mask = [0; 4];
    server.read_exact(&mut mask).unwrap();
    let mut data = vec![0; len];
    server.read_exact(&mut data).unwrap();
    for (i, b) in data.iter_mut().enumerate() {
        *b ^= mask[i % 4];
    }
    data
}
fn websocket(server: &mut Server, path: &str) {
    let request = headers(server);
    assert!(request.starts_with(&format!("GET {path} HTTP/1.1\r\n")));
    let key = request
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(k, _)| k.eq_ignore_ascii_case("sec-websocket-key"))
                .map(|(_, v)| v.trim())
        })
        .unwrap();
    let accept = STANDARD.encode(Sha1::digest(format!(
        "{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    )));
    write!(server,"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").unwrap();
    server.flush().unwrap();
    assert_eq!(
        masked_message(server),
        br#"{"method":"SUBSCRIBE","params":["btcusdt@depth"],"id":1}"#
    );
    frame(server, br#"{"result":null,"id":1}"#);
}
fn frame(server: &mut Server, bytes: &[u8]) {
    let split = bytes.len() / 2;
    for (head, part) in [(0x01, &bytes[..split]), (0x80, &bytes[split..])] {
        assert!(part.len() < 126);
        server.write_all(&[head, part.len() as u8]).unwrap();
        server.write_all(part).unwrap();
        server.flush().unwrap();
    }
}
fn reply(server: &mut Server, bytes: &[u8]) {
    server.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\n\r\n").unwrap();
    for chunk in bytes.chunks(11) {
        write!(server, "{:x}\r\n", chunk.len()).unwrap();
        server.write_all(chunk).unwrap();
        server.write_all(b"\r\n").unwrap();
    }
    server.write_all(b"0\r\n\r\n").unwrap();
    server.flush().unwrap();
}
fn event(market: Market, u: u64, end: u64, pu: u64) -> Vec<u8> {
    let extra = if market == Market::Usdm {
        format!(r#", "T":1,"pu":{pu},"st":1"#)
    } else {
        String::new()
    };
    format!(r#"{{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":{u},"u":{end},"b":[["99","2"]],"a":[]{extra}}}"#).into_bytes()
}
fn snap(market: Market, id: u64) -> Vec<u8> {
    let extra = if market == Market::Usdm {
        r#", "E":1,"T":1"#
    } else {
        ""
    };
    format!(r#"{{"lastUpdateId":{id},"bids":[["99","1"]],"asks":[["101","2"]]{extra}}}"#)
        .into_bytes()
}
fn message(conn: &mut net::Connection, decoder: &mut ws::Decoder) -> ws::Message {
    let end = deadline();
    loop {
        assert!(Instant::now() < end);
        match decoder.poll_message().unwrap() {
            ws::PollMessage::Message(message) => return message,
            ws::PollMessage::Yielded => continue,
            ws::PollMessage::NeedInput => {}
        }
        let driven = conn.drive(Instant::now(), net::IoBudget::default());
        let mut bytes = [0; 19];
        let n = conn.read(&mut bytes);
        decoder.push(&bytes[..n]).unwrap();
        if n == 0 {
            driven.unwrap();
        }
        thread::yield_now();
    }
}
fn fixture<P: DepthPolicy>() {
    let market = P::MARKET;
    let (tls, server_tls) = certificates();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let prefix = if market == Market::Spot {
        "/api/v3"
    } else {
        "/fapi/v1"
    };
    let ws_path = if market == Market::Spot {
        "/ws"
    } else {
        "/public/ws"
    };
    let server = thread::spawn(move || {
        for round in 0..2 {
            let base = if round == 0 { 100 } else { 500 };
            let mut stream = accept(&listener, server_tls.clone());
            websocket(&mut stream, ws_path);
            frame(&mut stream, &event(market, base - 1, base, base - 2));
            // The first accepted connection is WSS and its subscription has been
            // received before this server accepts any REST request.
            let mut http = accept(&listener, server_tls.clone());
            let request = headers(&mut http);
            assert!(request.starts_with(&format!(
                "GET {prefix}/depth?symbol=BTCUSDT&limit=5 HTTP/1.1\r\n"
            )));
            frame(&mut stream, &event(market, base + 1, base + 2, base));
            reply(&mut http, &snap(market, base));
            if round == 0 {
                frame(&mut stream, &event(market, 105, 106, 104));
                frame(&mut stream, &event(market, 110, 112, 109));
                let request = headers(&mut http);
                assert!(request.starts_with(&format!(
                    "GET {prefix}/depth?symbol=BTCUSDT&limit=5 HTTP/1.1\r\n"
                )));
                frame(&mut stream, &event(market, 113, 114, 112));
                reply(&mut http, &snap(market, 111));
            }
            stream.write_all(&[0x88, 0]).unwrap();
            stream.flush().unwrap();
        }
    });
    let mut instrument = common::command().context.instrument;
    if market == Market::Usdm {
        instrument.contract = ContractKind::Perpetual;
    }
    let mut sync = DepthSync::<P, 8, 8, 8>::new(Config {
        instrument,
        key: BookKey {
            venue: 1,
            instrument: 12,
            source: 1,
        },
        time_unit: TimeUnit::Milliseconds,
        snapshot_limit: 5,
        max_message_bytes: 4096,
        initial_book_generation: 10,
        previous_connection_epoch: 0,
    })
    .unwrap();
    let mut old = None;
    for round in 0..2 {
        let token = sync.connect(round + 1).unwrap();
        let mut stream = client(address, tls.clone());
        let handshake = ws::ClientHandshake::new("localhost", ws_path, &[], 4096).unwrap();
        send(&mut stream, handshake.request());
        handshake.validate(&response(&mut stream)).unwrap();
        send(
            &mut stream,
            &ws::encode_client(
                ws::Opcode::Text,
                br#"{"method":"SUBSCRIBE","params":["btcusdt@depth"],"id":1}"#,
                4096,
            )
            .unwrap(),
        );
        let mut decoder = ws::Decoder::new(4096, 4096).unwrap();
        assert_eq!(
            message(&mut stream, &mut decoder).payload,
            br#"{"result":null,"id":1}"#
        );
        sync.subscription_ready(token).unwrap();
        assert_eq!(
            sync.request_snapshot(1, 100).unwrap_err(),
            SyncError::InvalidState
        );
        sync.on_delta(token, &message(&mut stream, &mut decoder).payload, 10)
            .unwrap();
        let request = sync.request_snapshot(11, 100).unwrap();
        let mut http_conn = client(address, tls.clone());
        send(
            &mut http_conn,
            &http::encode_request("GET", request.path(), &[("Host", "localhost")], &[], 4096)
                .unwrap(),
        );
        sync.on_delta(token, &message(&mut stream, &mut decoder).payload, 12)
            .unwrap();
        let http_response = response(&mut http_conn);
        let result = sync
            .on_snapshot(
                request.token(),
                http_response.status,
                &http_response.body,
                13,
            )
            .unwrap();
        assert!(matches!(
            result,
            Outcome::SnapshotInstalled { live: true, .. }
        ));
        assert!(sync.view(13, 10).is_ok());
        if let Some((old_connection, old_request)) = old {
            assert_eq!(
                sync.on_delta(old_connection, b"invalid", 0).unwrap(),
                Outcome::IgnoredObsolete
            );
            assert_eq!(
                sync.on_snapshot(old_request, 200, b"invalid", 0).unwrap(),
                Outcome::IgnoredObsolete
            );
        }
        if round == 0 {
            assert!(matches!(
                sync.on_delta(token, &message(&mut stream, &mut decoder).payload, 14)
                    .unwrap(),
                Outcome::Rebuild(_)
            ));
            assert!(sync.view(14, 100).is_err());
            sync.on_delta(token, &message(&mut stream, &mut decoder).payload, 15)
                .unwrap();
            let retry = sync.request_snapshot(16, 100).unwrap();
            assert_eq!(
                sync.on_snapshot(request.token(), 200, &http_response.body, 0)
                    .unwrap(),
                Outcome::IgnoredObsolete
            );
            send(
                &mut http_conn,
                &http::encode_request("GET", retry.path(), &[("Host", "localhost")], &[], 4096)
                    .unwrap(),
            );
            sync.on_delta(token, &message(&mut stream, &mut decoder).payload, 17)
                .unwrap();
            let http_response = response(&mut http_conn);
            sync.on_snapshot(retry.token(), http_response.status, &http_response.body, 18)
                .unwrap();
            assert_eq!(sync.last_update_id(), Some(114));
        } else {
            assert_eq!(sync.last_update_id(), Some(502));
        }
        assert_eq!(message(&mut stream, &mut decoder).opcode, ws::Opcode::Close);
        sync.disconnect(token).unwrap();
        assert!(sync.view(19, 100).is_err());
        old = Some((token, request.token()));
    }
    server.join().unwrap();
}
#[test]
fn spot_real_wss_before_https_gap_rebuild_and_new_connection() {
    fixture::<BinanceSpot>();
}
#[test]
fn usdm_real_wss_before_https_gap_rebuild_and_new_connection() {
    fixture::<BinanceUsdM>();
}
