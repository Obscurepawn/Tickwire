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
use tickwire_binance::{events::*, feed::TimeUnit, request::Market};
use tickwire_transport::{self as net, http, rustls, ws};
use tickwire_venue as v;

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
fn response(conn: &mut net::Connection) -> (http::Response, Vec<u8>) {
    let mut decoder = http::ResponseDecoder::new(16384, 32).unwrap();
    let end = deadline();
    loop {
        assert!(Instant::now() < end);
        let drive = conn.drive(Instant::now(), net::IoBudget::default());
        let mut out = [0; 37];
        let n = conn.read(&mut out);
        decoder.push(&out[..n]).unwrap();
        if let Some(response) = decoder.next_response().unwrap() {
            return (response, decoder.take_buffered());
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
fn fixture(market: Market) {
    let (config, server_config) = certificates();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let path = if market == Market::Spot {
        "/ws-api/v3"
    } else {
        "/private/ws/test-listen-key"
    };
    let event = if market == Market::Spot {
        r#"{"subscriptionId":9,"event":{"e":"executionReport","E":10,"s":"BTCUSDT","c":"test-1","S":"BUY","o":"LIMIT","f":"GTC","q":"0.001","p":"60000","g":-1,"C":"","x":"TRADE","X":"FILLED","i":22,"l":"0.001","z":"0.001","L":"60000","n":"0.01","N":"USDT","T":9,"t":33,"I":44,"m":true,"Z":"60","Y":"60","r":"NONE"}}"#
    } else {
        r#"{"e":"ORDER_TRADE_UPDATE","E":10,"T":9,"o":{"s":"BTCUSDT","c":"test-1","S":"BUY","o":"LIMIT","f":"GTC","q":"0.001","p":"60000","x":"TRADE","X":"FILLED","i":22,"l":"0.001","z":"0.001","L":"60000","n":"0.01","N":"USDT","T":9,"t":33,"m":true,"ps":"BOTH","rp":"0"}}"#
    };
    let server = thread::spawn(move || {
        let mut socket = accept(&listener, server_config);
        let request = headers(&mut socket);
        assert!(request.starts_with(&format!("GET {path} HTTP/1.1\r\n")));
        let key = request
            .lines()
            .find_map(|l| {
                l.split_once(':')
                    .filter(|(k, _)| k.eq_ignore_ascii_case("sec-websocket-key"))
                    .map(|(_, v)| v.trim())
            })
            .unwrap();
        let accept = STANDARD.encode(Sha1::digest(format!(
            "{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        )));
        write!(socket,"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").unwrap();
        socket.flush().unwrap();
        // The fixture starts with an authenticated Spot session / owned listenKey.
        // Authentication/reconnection are separate session work, not simulated by
        // turning a parsed order into a fabricated account identity.
        if market == Market::Spot {
            assert_eq!(
                masked_message(&mut socket),
                br#"{"id":"1","method":"userDataStream.subscribe"}"#
            );
            let reply = br#"{"id":"1","status":200,"result":{"subscriptionId":9}}"#;
            socket.write_all(&[0x81, reply.len() as u8]).unwrap();
            socket.write_all(reply).unwrap();
        }
        let chunks: Vec<_> = event.as_bytes().chunks(73).collect();
        for (index, part) in chunks.iter().enumerate() {
            let head =
                (if index == 0 { 1 } else { 0 }) | if index + 1 == chunks.len() { 0x80 } else { 0 };
            socket.write_all(&[head, part.len() as u8]).unwrap();
            socket.write_all(part).unwrap();
            socket.flush().unwrap();
        }
        socket.conn.send_close_notify();
        socket.flush().unwrap();
    });
    let mut conn = client(addr, config);
    let handshake = ws::ClientHandshake::new("localhost", path, &[], 4096).unwrap();
    send(&mut conn, handshake.request());
    let (response, leftovers) = response(&mut conn);
    handshake.validate(&response).unwrap();
    if market == Market::Spot {
        send(
            &mut conn,
            &ws::encode_client(
                ws::Opcode::Text,
                br#"{"id":"1","method":"userDataStream.subscribe"}"#,
                4096,
            )
            .unwrap(),
        );
    }
    let ctx = SessionContext::new(
        SessionIdentity {
            risk_domain_id: [1; 16],
            venue: 1,
            environment: v::Environment::Test,
            account: 2,
            connection_epoch: 8,
        },
        market,
        if market == Market::Spot {
            PrivateStream::SpotSubscription { subscription_id: 9 }
        } else {
            PrivateStream::UsdmListenKey
        },
        TimeUnit::Milliseconds,
    )
    .unwrap();
    let mut c = common::command();
    if market == Market::Usdm {
        c.context.request.product = v::Product::LinearPerpetual;
        c.context.instrument.contract = v::ContractKind::Perpetual;
        let v::CommandBody::New(o) = &mut c.body else {
            unreachable!()
        };
        o.mode = v::ProductMode::Derivative {
            position_mode: v::PositionMode::OneWay,
            margin_mode: v::MarginMode::Cross,
        };
    }
    let command = v::CanonicalCommand::new(c).unwrap();
    let identity = v::OrderIdentity {
        order_id: [8; 16],
        original_intent_id: 3,
        original_intent_exec_epoch: 4,
        client_id: Some(common::text("test-1")),
        venue_order_id: None,
        parent_order_id: None,
        list_id: None,
        venue_algo_id: None,
        venue_list_id: None,
        list_client_id: None,
        algo_client_id: None,
    };
    let mut decoder = ws::Decoder::new(4096, 4096).unwrap();
    decoder.push(&leftovers).unwrap();
    let mut acknowledged = market == Market::Usdm;
    let end = deadline();
    loop {
        assert!(Instant::now() < end);
        let driven = conn.drive(Instant::now(), net::IoBudget::default());
        let mut buf = [0; 17];
        let n = conn.read(&mut buf);
        decoder.push(&buf[..n]).unwrap();
        match decoder.poll_message().unwrap() {
            ws::PollMessage::Message(msg) => {
                assert_eq!(msg.opcode, ws::Opcode::Text);
                if !acknowledged {
                    assert_eq!(
                        msg.payload,
                        br#"{"id":"1","status":200,"result":{"subscriptionId":9}}"#
                    );
                    acknowledged = true;
                } else {
                    let e = decode_private::<8>(&ctx, &msg.payload, 4096, 100).unwrap();
                    let bound = bind_order(&e, &command, &identity, &[]).unwrap();
                    let v::VenueEventBody::Fill(f) = &bound.event().body else {
                        panic!("fill")
                    };
                    assert_eq!(f.trade_id.as_str(), "33");
                    assert_eq!(f.last_quantity, common::d(1, -3));
                    assert_eq!(f.fees.as_slice()[0].amount, common::d(-1, -2));
                    assert_eq!(bound.event().identity.account, 2);
                    assert_eq!(bound.event().identity.source_connection_epoch, 8);
                    break;
                }
            }
            ws::PollMessage::NeedInput => {
                if n == 0 {
                    driven.unwrap();
                }
            }
            ws::PollMessage::Yielded => {}
        }
        thread::yield_now();
    }
    server.join().unwrap();
}
#[test]
fn spot_private_wss_fragments_bind_exact_trade_to_original_command() {
    fixture(Market::Spot);
}
#[test]
fn usdm_private_wss_fragments_bind_exact_trade_to_original_command() {
    fixture(Market::Usdm);
}
