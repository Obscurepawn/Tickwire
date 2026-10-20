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
use tickwire_binance::{feed::*, request::Market};
use tickwire_book::{BinanceSpot, BinanceUsdM, Book, BookKey, SequencePolicy};
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
fn fixture<P: SequencePolicy>(market: Market) {
    let (config, server_config) = certificates();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let prefix = if market == Market::Spot {
        "/api/v3"
    } else {
        "/fapi/v1"
    };
    let ws_path = if market == Market::Spot {
        "/ws/btcusdt@depth"
    } else {
        "/public/ws/btcusdt@depth"
    };
    let snapshot = if market == Market::Spot {
        r#"{"lastUpdateId":100,"bids":[["99","1"]],"asks":[["101","2"]]}"#
    } else {
        r#"{"lastUpdateId":100,"E":1,"T":1,"bids":[["99","1"]],"asks":[["101","2"]]}"#
    };
    let update = if market == Market::Spot {
        r#"{"e":"depthUpdate","E":2,"s":"BTCUSDT","U":101,"u":102,"b":[["99","0"],["98","3"]],"a":[]}"#
    } else {
        r#"{"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","st":1,"U":99,"u":102,"pu":98,"b":[["99","0"],["98","3"]],"a":[]}"#
    };
    let server = thread::spawn(move || {
        let mut socket = accept(&listener, server_config.clone());
        let request = headers(&mut socket);
        assert!(request.starts_with(&format!(
            "GET {prefix}/depth?symbol=BTCUSDT&limit=100 HTTP/1.1\r\n"
        )));
        socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\n\r\n").unwrap();
        for chunk in snapshot.as_bytes().chunks(11) {
            write!(socket, "{:x}\r\n", chunk.len()).unwrap();
            socket.write_all(chunk).unwrap();
            socket.write_all(b"\r\n").unwrap();
            socket.flush().unwrap();
        }
        socket.write_all(b"0\r\n\r\n").unwrap();
        socket.flush().unwrap();
        socket.conn.send_close_notify();
        socket.flush().unwrap();
        drop(socket);
        let mut socket = accept(&listener, server_config);
        let request = headers(&mut socket);
        assert!(request.starts_with(&format!("GET {ws_path} HTTP/1.1\r\n")));
        let key = request
            .lines()
            .find_map(|line| {
                line.split_once(':')
                    .filter(|(name, _)| name.eq_ignore_ascii_case("sec-websocket-key"))
                    .map(|(_, value)| value.trim())
            })
            .unwrap();
        let accept = STANDARD.encode(Sha1::digest(format!(
            "{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        )));
        write!(socket,"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").unwrap();
        socket.flush().unwrap();
        assert_eq!(
            masked_message(&mut socket),
            br#"{"method":"SUBSCRIBE","params":["btcusdt@depth"],"id":1}"#
        );
        let split = 40;
        for (head, part) in [
            (0x01, &update.as_bytes()[..split]),
            (0x80, &update.as_bytes()[split..]),
        ] {
            assert!(part.len() < 126);
            socket.write_all(&[head, part.len() as u8]).unwrap();
            socket.write_all(part).unwrap();
            socket.flush().unwrap();
        }
        socket.conn.send_close_notify();
        socket.flush().unwrap();
    });
    let mut ctx = common::command().context.instrument;
    if market == Market::Usdm {
        ctx.contract = ContractKind::Perpetual;
    }
    let ctx = DepthContext::new(
        market,
        ctx,
        BookKey {
            venue: 1,
            instrument: 12,
            source: 1,
        },
        7,
        TimeUnit::Milliseconds,
    )
    .unwrap();
    let mut conn = client(address, config.clone());
    let request = http::encode_request(
        "GET",
        &format!("{prefix}/depth?symbol=BTCUSDT&limit=100"),
        &[("Host", "localhost")],
        &[],
        4096,
    )
    .unwrap();
    send(&mut conn, &request);
    let body = response(&mut conn);
    assert_eq!(body.status, 200);
    let snapshot = decode_snapshot::<8>(&body.body, &ctx, 16384).unwrap();
    let mut book = Book::<P, 8, 8>::new(snapshot.snapshot().key, 7);
    book.install_snapshot(snapshot.snapshot()).unwrap();
    drop(conn);
    let mut conn = client(address, config);
    let handshake = ws::ClientHandshake::new("localhost", ws_path, &[], 4096).unwrap();
    send(&mut conn, handshake.request());
    handshake.validate(&response(&mut conn)).unwrap();
    send(
        &mut conn,
        &ws::encode_client(
            ws::Opcode::Text,
            br#"{"method":"SUBSCRIBE","params":["btcusdt@depth"],"id":1}"#,
            4096,
        )
        .unwrap(),
    );
    let mut decoder = ws::Decoder::new(4096, 4096).unwrap();
    let end = deadline();
    loop {
        assert!(Instant::now() < end);
        let driven = conn.drive(Instant::now(), net::IoBudget::default());
        let mut buffer = [0; 19];
        let n = conn.read(&mut buffer);
        decoder.push(&buffer[..n]).unwrap();
        match decoder.poll_message().unwrap() {
            ws::PollMessage::Message(msg) => {
                let update = decode_diff::<8>(&msg.payload, &ctx, 50, 4096).unwrap();
                book.apply(update.event()).unwrap();
                break;
            }
            ws::PollMessage::Yielded => continue,
            ws::PollMessage::NeedInput => {}
        }
        if n == 0 {
            driven.unwrap();
        }
        thread::yield_now();
    }
    assert_eq!(book.last_update_id(), Some(102));
    assert!(book.view(50, 10).is_ok());
    server.join().unwrap();
}
#[test]
fn spot_chunked_https_snapshot_and_fragmented_wss_update_reach_live_book() {
    fixture::<BinanceSpot>(Market::Spot);
}
#[test]
fn usdm_chunked_https_snapshot_and_fragmented_wss_update_reach_live_book() {
    fixture::<BinanceUsdM>(Market::Usdm);
}
