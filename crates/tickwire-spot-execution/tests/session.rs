#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod common;
use common::{Directory, MockAuthority, until};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::{AccountGateway, ConnectionConfig, Status};
use tickwire_account_journal::FileBackend;
use tickwire_spot_execution::{Credentials, Reply, RestSession, SpotEncoder, SpotVenue, authority};
use tickwire_transport::{IoBudget, rustls};
use tickwire_venue as v;
fn peer(venue: SpotVenue) -> (ConnectionConfig, thread::JoinHandle<Vec<String>>) {
    let host = authority(venue, v::Environment::Test);
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
    let job = thread::spawn(move || {
        let (sock, _) = listener.accept().unwrap();
        sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut io = BufReader::new(rustls::StreamOwned::new(
            rustls::ServerConnection::new(server).unwrap(),
            sock,
        ));
        let mut requests = vec![];
        for i in 0..3 {
            let mut head = String::new();
            io.read_line(&mut head).unwrap();
            requests.push(head);
            let mut size = 0;
            loop {
                let mut line = String::new();
                assert!(io.read_line(&mut line).unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
                if let Some(s) = line.strip_prefix("Content-Length: ") {
                    size = s.trim().parse().unwrap();
                }
            }
            let mut b = vec![0; size];
            io.read_exact(&mut b).unwrap();
            let reply = match (venue, i) {
                (SpotVenue::Bybit, 2) => {
                    r#"{"retCode":0,"result":{"category":"spot","list":[{"symbol":"BTCUSDT","orderId":"555","orderLinkId":"client1","orderStatus":"Cancelled","side":"Buy","cumExecQty":"0.0005","updatedTime":"1700000000123"}]}}"#
                }
                (SpotVenue::Bybit, _) => {
                    r#"{"retCode":0,"result":{"orderId":"555","orderLinkId":"client1"}}"#
                }
                (SpotVenue::Okx, 2) => {
                    r#"{"code":"0","data":[{"instType":"SPOT","instId":"BTC-USDT","ordId":"555","clOrdId":"client1","state":"canceled","side":"buy","accFillSz":"0.0005","uTime":"1700000000123"}]}"#
                }
                (SpotVenue::Okx, _) => {
                    r#"{"code":"0","data":[{"sCode":"0","ordId":"555","clOrdId":"client1"}]}"#
                }
            };
            write!(
                io.get_mut(),
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                reply.len(),
                reply
            )
            .unwrap();
            io.get_mut().flush().unwrap();
        }
        requests
    });
    (
        ConnectionConfig {
            address,
            tls,
            limits: Default::default(),
            connect_deadline: until(),
        },
        job,
    )
}
#[test]
fn both_venues_go_through_durable_admission_and_only_order_status_closes_the_order() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let dir = Directory::new();
        let (mut snapshot, mut catalog, mut cmd) = common::fixture::fixture();
        snapshot.scope.venue = venue.id();
        cmd.context.request.venue = venue.id();
        let symbol = v::Symbol::new(if venue == SpotVenue::Okx {
            "BTC-USDT"
        } else {
            "BTCUSDT"
        })
        .unwrap();
        catalog.entries[0].instrument.symbol = symbol.clone();
        cmd.context.instrument.symbol = symbol;
        let id = v::ClientId::new("client1").unwrap();
        let v::CommandBody::New(new) = &mut cmd.body else {
            unreachable!()
        };
        new.client_id = id.clone();
        let source_scope = snapshot.scope.clone();
        let base = CanonicalBaseline::new(snapshot, catalog).unwrap();
        let mut cfg = common::config(&base);
        let credentials = if venue == SpotVenue::Okx {
            Credentials::okx("key", b"secret", "pass")
        } else {
            Credentials::bybit("key", b"secret")
        };
        let encoder =
            SpotEncoder::new(credentials, v::Environment::Test, 2, || Ok(1700000000123)).unwrap();
        cfg.route = encoder.route();
        let (connection, worker) = peer(venue);
        let auth = MockAuthority::default();
        let g = AccountGateway::create(
            FileBackend::open(&dir.0).unwrap(),
            cfg,
            base,
            auth.clone(),
            encoder,
            connection,
        )
        .unwrap();
        let mut session = RestSession::new(g, 1).unwrap();
        let target = v::OrderTarget {
            order_id: [9; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(id.clone()),
            venue_order_id: None,
        };
        for i in 0..3 {
            if i > 0 {
                cmd.context.request.intent_id += 1;
                cmd.context.request.recovery_object += 1;
                cmd.context.strategy.strategy_intent_seq += 1;
                cmd.body = if i == 1 {
                    v::CommandBody::Cancel(target.clone())
                } else {
                    v::CommandBody::Query(target.clone())
                };
            }
            let command = v::CanonicalCommand::new(cmd.clone()).unwrap();
            let deadline = until();
            assert_eq!(
                session.submit(command.clone(), 60, deadline).unwrap(),
                Status::Queued
            );
            assert_eq!(
                session.submit(command, 60, deadline).unwrap(),
                Status::Queued
            );
            let reply = loop {
                assert!(Instant::now() < deadline);
                if let Some(reply) = session
                    .poll(Instant::now(), 62, IoBudget::default())
                    .unwrap()
                {
                    break reply;
                }
                thread::yield_now();
            };
            let order_key = tickwire_account::OrderKey {
                intent: common::intent(),
                client_id: id.clone(),
            };
            if i == 0 {
                let row = if venue == SpotVenue::Bybit {
                    br#"{"category":"spot","symbol":"BTCUSDT","orderLinkId":"client1","orderId":"555","execType":"Trade","execId":"0ab1bdf7-4219-438b-b30a-32ec863018f7","execPrice":"60000","execQty":"0.0005","execValue":"30","execTime":"1700000000123","execFee":"0.001","feeCurrency":"USDT","isMaker":true,"side":"Buy","extraFees":""}"#.as_slice()
                } else {
                    br#"{"instType":"SPOT","instId":"BTC-USDT","clOrdId":"client1","ordId":"555","tradeId":"42","fillPx":"60000","fillSz":"0.0005","fillTime":"1700000000123","fillFee":"-0.001","fillFeeCcy":"USDT","fee":"-999","execType":"M","side":"buy"}"#.as_slice()
                };
                let source = |input_sequence| tickwire_spot_execution::FillSource {
                    scope: &source_scope,
                    intent: common::intent(),
                    order_id: [9; 16],
                    input_stream: [7; 16],
                    input_sequence,
                    connection_epoch: 3,
                    received_mono_ns: 63,
                };
                assert_eq!(
                    session.record_fill(source(1), row).unwrap(),
                    tickwire_account::Applied::New
                );
                assert_eq!(
                    session.record_fill(source(1), row).unwrap(),
                    tickwire_account::Applied::Duplicate
                );
                assert_eq!(
                    session.record_fill(source(2), row).unwrap(),
                    tickwire_account::Applied::Duplicate
                );
                let base_wallet = session
                    .gateway()
                    .account()
                    .asset(common::fixture::key(1))
                    .unwrap();
                let quote_wallet = session
                    .gateway()
                    .account()
                    .asset(common::fixture::key(2))
                    .unwrap();
                assert_eq!(base_wallet.total, common::fixture::f(100050000));
                assert_eq!(quote_wallet.total, common::fixture::f(96999900000));
            }
            let order = session.gateway().account().order(&order_key).unwrap();
            if i < 2 {
                assert!(matches!(reply, Reply::Acknowledged { .. }));
                assert!(!order.terminal);
            } else {
                assert!(matches!(reply, Reply::Order(_)));
                assert!(order.terminal);
            }
        }
        assert_eq!(auth.policy.lock().unwrap().prepare_calls, 3);
        assert_eq!(session.gateway().journal_position(), 6);
        let requests = worker.join().unwrap();
        assert!(requests[0].starts_with("POST"));
        assert!(requests[1].starts_with("POST"));
        assert!(requests[2].starts_with("GET"));
    }
}
