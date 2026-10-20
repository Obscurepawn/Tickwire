mod common;
use common::{Directory, MockAuthorization, end};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tickwire_binance::{
    auth::SigningKey,
    request::{Credentials, Market, Timestamp},
    response,
};
use tickwire_business_journal::{BusinessJournal, FileBackend};
use tickwire_execution as ex;
use tickwire_gateway::{
    ConnectionConfig, Encoder, Error, JournaledSender, Status, binance::RestEncoder,
};
use tickwire_transport::{self as net, http, rustls};
use tickwire_venue::*;

const NOW_MS: u64 = 1_668_481_559_918;
fn credentials(key: &SigningKey, market: Market) -> Credentials<'_> {
    Credentials {
        account: 2,
        environment: Environment::Test,
        market,
        api_key: "public-protocol-test-key",
        signing_key: key,
    }
}
fn setup(
    market: Market,
) -> (
    CanonicalCommand,
    ex::DurableRecord,
    tickwire_gateway::Config,
) {
    let mut command = common::command();
    let mut config = common::config();
    if market == Market::Usdm {
        command.context.request.product = Product::LinearPerpetual;
        command.context.instrument.contract = ContractKind::Perpetual;
        if let CommandBody::New(ref mut o) = command.body {
            o.mode = ProductMode::Derivative {
                position_mode: PositionMode::OneWay,
                margin_mode: MarginMode::Cross,
            };
        }
        config.route.product = Product::LinearPerpetual;
        config.route.authority = "demo-fapi.binance.com".into();
        config.business.domain.product = ex::Product::UsdM;
    }
    let c = CanonicalCommand::new(command).unwrap();
    let mut r = common::record(&c);
    r.request.key.product = config.business.domain.product;
    (c, r, config)
}
fn peer(authority: &str) -> (ConnectionConfig, thread::JoinHandle<Vec<u8>>) {
    let cert = rcgen::generate_simple_self_signed(vec![authority.into()]).unwrap();
    let private = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let server = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert.der().clone()], private.into())
            .unwrap(),
    );
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let tls = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = l.local_addr().unwrap();
    let h = thread::spawn(move || {
        let (socket, _) = l.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut tls =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server).unwrap(), socket);
        let mut bytes = Vec::new();
        loop {
            let mut b = [0; 1];
            match tls.read(&mut b) {
                Ok(0) | Err(_) => return bytes,
                Ok(_) => bytes.push(b[0]),
            };
            assert!(bytes.len() <= 32768);
            if bytes.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let body=br#"{"symbol":"BTCUSDT","orderId":9007199254740993,"clientOrderId":"test-1","status":"NEW","executedQty":"0.000"}"#;
        write!(
            tls,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .unwrap();
        tls.write_all(body).unwrap();
        tls.flush().unwrap();
        // Keep this connection alive until its single owner drops it.
        let mut b = [0; 256];
        loop {
            match tls.read(&mut b) {
                Ok(0) | Err(_) => break,
                Ok(n) => bytes.extend_from_slice(&b[..n]),
            }
        }
        bytes
    });
    (
        ConnectionConfig {
            address,
            tls,
            limits: net::Limits {
                max_accept_bytes: 13,
                ..Default::default()
            },
            connect_deadline: end(),
        },
        h,
    )
}
fn verify_request(bytes: &[u8], market: Market) {
    let request = std::str::from_utf8(bytes).unwrap();
    let host = if market == Market::Spot {
        "testnet.binance.vision"
    } else {
        "demo-fapi.binance.com"
    };
    assert!(request.contains(&format!("Host: {host}\r\n")));
    assert!(request.contains("X-MBX-APIKEY: public-protocol-test-key\r\n"));
    assert!(request.ends_with("Content-Length: 0\r\n\r\n"));
    let mut first = request.lines().next().unwrap().split_whitespace();
    assert_eq!(first.next(), Some("POST"));
    let path = first.next().unwrap();
    assert!(path.starts_with(if market == Market::Spot {
        "/api/v3/order?"
    } else {
        "/fapi/v1/order?"
    }));
    let (preimage, signature) = path
        .split_once('?')
        .unwrap()
        .1
        .rsplit_once("&signature=")
        .unwrap();
    let sig: Vec<u8> = (0..signature.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&signature[i..i + 2], 16).unwrap())
        .collect();
    ring::hmac::verify(
        &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, b"test-only-secret"),
        preimage.as_bytes(),
        &sig,
    )
    .unwrap();
    let fields: std::collections::BTreeMap<_, _> = preimage
        .split('&')
        .map(|kv| kv.split_once('=').unwrap())
        .collect();
    for (k, v) in [
        ("symbol", "BTCUSDT"),
        ("side", "BUY"),
        ("type", "LIMIT"),
        ("quantity", "0.001"),
        ("price", "60000"),
        ("newClientOrderId", "test-1"),
        ("timeInForce", "GTC"),
        ("recvWindow", "5000"),
        ("timestamp", "1668481559918"),
    ] {
        assert_eq!(fields[k], v);
    }
    if market == Market::Usdm {
        assert_eq!(fields["positionSide"], "BOTH");
    }
}
fn roundtrip(market: Market) {
    let directory = Directory::new();
    let (command, record, config) = setup(market);
    let business = config.business.clone();
    let (connection, peer) = peer(&config.route.authority);
    let authorization = MockAuthorization::default();
    let state = authorization.state.clone();
    let recovery_authorization = authorization.clone();
    let blobs = authorization.blobs.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let time = move || {
        observed.fetch_add(1, Ordering::SeqCst);
        Ok(Timestamp::Milliseconds(NOW_MS))
    };
    let key = SigningKey::hmac(b"test-only-secret").unwrap();
    let encoder = RestEncoder::new(credentials(&key, market), time, 5_000_000, 32768).unwrap();
    let mut sender = JournaledSender::create(
        FileBackend::open(&directory.0).unwrap(),
        config,
        authorization,
        encoder,
        connection,
    )
    .unwrap();
    assert_eq!(
        sender.submit(record, command.clone(), end()).unwrap(),
        Status::Queued
    );
    assert_eq!(sender.journal_position(), 1);
    assert_eq!(state.lock().unwrap().gate_calls, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        blobs
            .0
            .lock()
            .unwrap()
            .get(&tickwire_recovery::StoreKey::Object(command.digest()))
            .unwrap(),
        command.bytes()
    );
    common::drive(&mut sender, Status::Sending);
    {
        let mut p = state.lock().unwrap();
        assert_eq!(p.gate_calls, 1);
        p.gate.enabled = false;
    }
    common::drive(&mut sender, Status::AwaitingResponse);
    assert_eq!(state.lock().unwrap().gate_calls, 1);
    let mut decoder = http::ResponseDecoder::new(16384, 16).unwrap();
    let until = end();
    let reply = loop {
        assert!(Instant::now() < until);
        sender
            .poll(Instant::now(), net::IoBudget::default())
            .unwrap();
        let mut b = [0; 1024];
        let n = sender.read(&mut b);
        decoder.push(&b[..n]).unwrap();
        if let Some(r) = decoder.next_response().unwrap() {
            break r;
        }
        thread::yield_now();
    };
    let envelope = response::decode_rest(market, reply.status, &reply.body).unwrap();
    assert_eq!(
        response::decode_order(envelope.result.unwrap().get().as_bytes())
            .unwrap()
            .order_id,
        9007199254740993
    );
    drop(sender);
    let bytes = peer.join().unwrap();
    verify_request(&bytes, market);
    let journal =
        BusinessJournal::recover(FileBackend::open(&directory.0).unwrap(), business, 4, None)
            .unwrap();
    assert_eq!(journal.events().len(), 1);
    let tickwire_journal_codec::Event::PreparedIntent(p) = journal.events()[0].event else {
        panic!("prepared intent absent")
    };
    assert_eq!(p.command_digest, command.digest());
    drop(journal);
    let (_, _, mut config) = setup(market);
    config.writer_epoch = 5;
    let encoder = RestEncoder::new(
        credentials(&key, market),
        || Ok(Timestamp::Milliseconds(NOW_MS)),
        5_000_000,
        32768,
    )
    .unwrap();
    let mut recovered = JournaledSender::recover(
        FileBackend::open(&directory.0).unwrap(),
        config,
        recovery_authorization,
        encoder,
        None,
    )
    .unwrap();
    assert!(recovered.frozen());
    assert_eq!(recovered.status(3), Some(Status::Unknown));
    assert_eq!(
        recovered.submit(record, command, end()).unwrap(),
        Status::Unknown
    );
}
#[test]
fn spot_rest_real_wal_closure_final_gate_and_tls() {
    roundtrip(Market::Spot)
}
#[test]
fn usdm_rest_real_wal_closure_final_gate_and_tls() {
    roundtrip(Market::Usdm)
}
#[test]
fn identity_time_and_http_capacity_fail_before_a_prepared_request() {
    let key = SigningKey::hmac(b"test-only-secret").unwrap();
    let (command, _, _) = setup(Market::Spot);
    let mut auth = credentials(&key, Market::Spot);
    auth.account = 9;
    let mut encoder = RestEncoder::new(
        auth,
        || Ok(Timestamp::Milliseconds(NOW_MS)),
        5_000_000,
        32768,
    )
    .unwrap();
    assert!(matches!(encoder.encode(&command), Err(Error::Identity)));
    let mut encoder = RestEncoder::new(
        credentials(&key, Market::Spot),
        || Err(Error::Encoding),
        5_000_000,
        32768,
    )
    .unwrap();
    assert!(matches!(encoder.encode(&command), Err(Error::Encoding)));
    let mut encoder = RestEncoder::new(
        credentials(&key, Market::Spot),
        || Ok(Timestamp::Milliseconds(NOW_MS)),
        5_000_000,
        16,
    )
    .unwrap();
    assert!(matches!(encoder.encode(&command), Err(Error::Capacity)));
    let (usdm, _, _) = setup(Market::Usdm);
    let mut encoder = RestEncoder::new(
        credentials(&key, Market::Usdm),
        || Ok(Timestamp::Milliseconds(NOW_MS)),
        5_000_001,
        32768,
    )
    .unwrap();
    assert!(matches!(encoder.encode(&usdm), Err(Error::Encoding)));
}
#[test]
fn real_signed_request_gate_denial_is_durable_abort_with_zero_application_bytes() {
    let directory = Directory::new();
    let (command, record, config) = setup(Market::Spot);
    let business = config.business.clone();
    let (connection, peer) = peer(&config.route.authority);
    let authorization = MockAuthorization::default();
    let state = authorization.state.clone();
    let key = SigningKey::hmac(b"test-only-secret").unwrap();
    let encoder = RestEncoder::new(
        credentials(&key, Market::Spot),
        || Ok(Timestamp::Milliseconds(NOW_MS)),
        5_000_000,
        32768,
    )
    .unwrap();
    let mut sender = JournaledSender::create(
        FileBackend::open(&directory.0).unwrap(),
        config,
        authorization,
        encoder,
        connection,
    )
    .unwrap();
    sender.submit(record, command, end()).unwrap();
    state.lock().unwrap().gate.enabled = false;
    common::drive(&mut sender, Status::Aborted);
    assert_eq!(sender.journal_position(), 2);
    assert_eq!(state.lock().unwrap().gate_calls, 1);
    drop(sender);
    assert!(peer.join().unwrap().is_empty());
    let journal =
        BusinessJournal::recover(FileBackend::open(&directory.0).unwrap(), business, 4, None)
            .unwrap();
    assert!(matches!(
        journal.events()[1].event,
        tickwire_journal_codec::Event::AbortedBeforeSend { .. }
    ));
}

#[test]
fn unavailable_trusted_clock_leaves_no_intent_and_no_application_bytes() {
    let directory = Directory::new();
    let (command, record, config) = setup(Market::Spot);
    let (connection, peer) = peer(&config.route.authority);
    let authorization = MockAuthorization::default();
    let state = authorization.state.clone();
    let blobs = authorization.blobs.clone();
    let key = SigningKey::hmac(b"test-only-secret").unwrap();
    let encoder = RestEncoder::new(
        credentials(&key, Market::Spot),
        || Err(Error::Encoding),
        5_000_000,
        32768,
    )
    .unwrap();
    let mut sender = JournaledSender::create(
        FileBackend::open(&directory.0).unwrap(),
        config,
        authorization,
        encoder,
        connection,
    )
    .unwrap();
    assert!(matches!(
        sender.submit(record, command, end()),
        Err(Error::Encoding)
    ));
    assert_eq!(sender.journal_position(), 0);
    assert_eq!(sender.status(3), None);
    assert_eq!(state.lock().unwrap().gate_calls, 0);
    assert!(blobs.0.lock().unwrap().is_empty());
    drop(sender);
    assert!(peer.join().unwrap().is_empty());
}
