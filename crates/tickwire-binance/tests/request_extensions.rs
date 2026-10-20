mod common;
use common::*;
use serde_json::{Value, json};
use tickwire_binance::{auth::SigningKey, request::*};
use tickwire_venue::*;

fn order(id: &str, kind: OrderType) -> OrderSpec {
    let CommandBody::New(mut o) = command().body else {
        panic!()
    };
    o.client_id = text(id);
    o.order_type = kind;
    if matches!(
        kind,
        OrderType::Market | OrderType::StopMarket | OrderType::TakeProfitMarket
    ) {
        o.price = None;
        o.time_in_force = None;
    }
    if matches!(
        kind,
        OrderType::StopLimit
            | OrderType::StopMarket
            | OrderType::TakeProfitLimit
            | OrderType::TakeProfitMarket
    ) {
        o.trigger = Some(Trigger {
            price: d(55000, 0),
            working_price: WorkingPrice::Contract,
            price_protect: false,
        });
    }
    o
}
fn list(kind: ListKind) -> Command {
    let mut c = command();
    let mut maker = order("above", OrderType::Limit);
    maker.time_in_force = Some(TimeInForce::PostOnly);
    let mut stop = order("below", OrderType::StopLimit);
    stop.trigger.as_mut().unwrap().price = d(65000, 0);
    let legs = match kind {
        ListKind::Oco => vec![maker, stop],
        ListKind::Oto => vec![
            order("working", OrderType::Limit),
            order("pending", OrderType::Market),
        ],
        ListKind::Otoco => vec![order("working", OrderType::Limit), maker, stop],
    };
    c.body = CommandBody::SpotList(ListOrder {
        list_id: [8; 16],
        client_id: text("list-1"),
        kind,
        legs: BoundedList::new(legs).unwrap(),
    });
    c
}
fn prepared(c: Command, transport: Transport) -> tickwire_binance::Result<PreparedRequest> {
    let key = SigningKey::hmac(b"extension-test-key").unwrap();
    prepare(
        &CanonicalCommand::new(c).unwrap(),
        &Credentials {
            account: 2,
            environment: Environment::Test,
            market: Market::Spot,
            api_key: "test-key",
            signing_key: &key,
        },
        RequestOptions {
            transport,
            timestamp: Timestamp::Milliseconds(1668481559918),
            recv_window_micros: 5_000_000,
            request_id: "extensions",
        },
        MAX_REQUEST_BYTES,
    )
}
fn ws(c: Command) -> Value {
    serde_json::from_slice(prepared(c, Transport::WebSocket).unwrap().payload()).unwrap()
}
#[test]
fn all_list_endpoints_and_exact_leg_fields() {
    for (kind, suffix, method) in [
        (ListKind::Oco, "oco", "orderList.place.oco"),
        (ListKind::Oto, "oto", "orderList.place.oto"),
        (ListKind::Otoco, "otoco", "orderList.place.otoco"),
    ] {
        let c = list(kind);
        let p = prepared(c.clone(), Transport::Rest).unwrap();
        assert!(
            p.path()
                .starts_with(&format!("/api/v3/orderList/{suffix}?"))
        );
        let v = ws(c);
        assert_eq!(v["method"], method);
        let mut fields = v["params"].as_object().unwrap().clone();
        for key in ["apiKey", "signature", "timestamp", "recvWindow"] {
            fields.remove(key).unwrap();
        }
        let expected = match kind {
            ListKind::Oco => {
                json!({"symbol":"BTCUSDT","listClientOrderId":"list-1","side":"BUY","quantity":"0.001","aboveType":"LIMIT_MAKER","aboveClientOrderId":"above","abovePrice":"60000","belowType":"STOP_LOSS_LIMIT","belowClientOrderId":"below","belowPrice":"60000","belowStopPrice":"65000","belowTimeInForce":"GTC"})
            }
            ListKind::Oto => {
                json!({"symbol":"BTCUSDT","listClientOrderId":"list-1","workingType":"LIMIT","workingSide":"BUY","workingClientOrderId":"working","workingPrice":"60000","workingQuantity":"0.001","workingTimeInForce":"GTC","pendingType":"MARKET","pendingSide":"BUY","pendingClientOrderId":"pending","pendingQuantity":"0.001"})
            }
            ListKind::Otoco => {
                json!({"symbol":"BTCUSDT","listClientOrderId":"list-1","workingType":"LIMIT","workingSide":"BUY","workingClientOrderId":"working","workingPrice":"60000","workingQuantity":"0.001","workingTimeInForce":"GTC","pendingSide":"BUY","pendingQuantity":"0.001","pendingAboveType":"LIMIT_MAKER","pendingAboveClientOrderId":"above","pendingAbovePrice":"60000","pendingBelowType":"STOP_LOSS_LIMIT","pendingBelowClientOrderId":"below","pendingBelowPrice":"60000","pendingBelowStopPrice":"65000","pendingBelowTimeInForce":"GTC"})
            }
        };
        assert_eq!(Value::Object(fields), expected);
    }
}
#[test]
fn list_shared_fields_are_never_silently_discarded() {
    for kind in [ListKind::Oco, ListKind::Otoco] {
        for field in 0..3 {
            let mut c = list(kind);
            if let CommandBody::SpotList(ref mut l) = c.body {
                let mut legs = l.legs.as_slice().to_vec();
                let last = legs.last_mut().unwrap();
                match field {
                    0 => last.side = Side::Sell,
                    1 => last.quantity = Quantity::Base(d(2, -3)),
                    _ => last.self_trade_prevention = SelfTradePrevention::ExpireBoth,
                }
                l.legs = BoundedList::new(legs).unwrap();
            }
            assert!(prepared(c, Transport::Rest).is_err());
        }
    }
}
#[test]
fn list_conditional_trailing_iceberg_and_shared_stp_survive() {
    let mut c = list(ListKind::Oco);
    if let CommandBody::SpotList(ref mut l) = c.body {
        let mut legs = l.legs.as_slice().to_vec();
        legs[1].trailing = Some(Trailing {
            activation_price: None,
            working_price: WorkingPrice::Contract,
            distance: TrailingDistance::BasisPoints(250),
        });
        legs[1].iceberg_quantity = Some(d(1, -4));
        for leg in &mut legs {
            leg.self_trade_prevention = SelfTradePrevention::ExpireBoth;
        }
        l.legs = BoundedList::new(legs).unwrap();
    }
    let v = ws(c);
    assert_eq!(v["params"]["belowTrailingDelta"], 250);
    assert_eq!(v["params"]["belowIcebergQty"], "0.0001");
    assert_eq!(v["params"]["selfTradePreventionMode"], "EXPIRE_BOTH");
}
fn pegged_order(id: &str, kind: OrderType, peg: PegPriceType, offset: Option<u16>) -> SpotOrder {
    let mut o = order(id, kind);
    o.price = None;
    SpotOrder {
        order: o,
        peg: Some(Peg {
            price_type: peg,
            offset_levels: offset,
        }),
    }
}
fn advanced(a: SpotAdvancedCommand) -> Command {
    let mut c = command();
    c.body = CommandBody::SpotAdvanced(a);
    c
}
#[test]
fn peg_new_cancel_replace_preserve_price_and_exact_offset_pair() {
    for replacement in [false, true] {
        for offset in [None, Some(0), Some(100)] {
            let mut o = pegged_order("peg", OrderType::Limit, PegPriceType::Market, offset);
            o.order.time_in_force = Some(TimeInForce::Ioc);
            if replacement {
                o.order.price = Some(d(60000, 0));
            }
            let a = if replacement {
                SpotAdvancedCommand::CancelReplace {
                    target: OrderTarget {
                        order_id: [1; 16],
                        expected_owner_revision: 1,
                        original_intent_exec_epoch: 4,
                        client_id: Some(text("old")),
                        venue_order_id: None,
                    },
                    replacement: o,
                    mode: CancelReplaceMode::StopOnFailure,
                }
            } else {
                SpotAdvancedCommand::New(o)
            };
            let c = advanced(a);
            let v = ws(c.clone());
            assert_eq!(
                v["method"],
                if replacement {
                    "order.cancelReplace"
                } else {
                    "order.place"
                }
            );
            let p = &v["params"];
            assert_eq!(p["pegPriceType"], "MARKET_PEG");
            assert_eq!(p["timeInForce"], "IOC");
            assert_eq!(p.get("price").is_some(), replacement);
            assert_eq!(p.get("pegOffsetType").is_some(), offset.is_some());
            assert_eq!(p.get("pegOffsetValue").is_some(), offset.is_some());
            if let Some(n) = offset {
                assert_eq!(p["pegOffsetType"], "PRICE_LEVEL");
                assert_eq!(p["pegOffsetValue"], n);
            }
            assert!(
                prepared(c, Transport::Rest)
                    .unwrap()
                    .path()
                    .contains("pegPriceType=MARKET_PEG")
            );
        }
    }
}
#[test]
fn peg_fields_are_prefixed_for_every_list_role() {
    for kind in [ListKind::Oco, ListKind::Oto, ListKind::Otoco] {
        let CommandBody::SpotList(l) = list(kind).body else {
            panic!()
        };
        let mut legs: Vec<_> = l
            .legs
            .as_slice()
            .iter()
            .cloned()
            .map(|order| SpotOrder {
                order,
                peg: Some(Peg {
                    price_type: PegPriceType::Primary,
                    offset_levels: Some(1),
                }),
            })
            .collect();
        if kind == ListKind::Oto {
            legs[1].order = order("pending", OrderType::Limit);
        }
        for leg in &mut legs {
            leg.order.price = None;
        }
        let v = ws(advanced(SpotAdvancedCommand::List(SpotAdvancedList {
            list_id: l.list_id,
            client_id: l.client_id,
            kind,
            legs: BoundedList::new(legs).unwrap(),
        })));
        let prefixes: &[&str] = match kind {
            ListKind::Oco => &["above", "below"],
            ListKind::Oto => &["working", "pending"],
            ListKind::Otoco => &["working", "pendingAbove", "pendingBelow"],
        };
        for prefix in prefixes {
            let p = &v["params"];
            assert_eq!(p[format!("{prefix}PegPriceType")], "PRIMARY_PEG");
            assert_eq!(p[format!("{prefix}PegOffsetType")], "PRICE_LEVEL");
            assert_eq!(p[format!("{prefix}PegOffsetValue")], 1);
            assert!(p.get(format!("{prefix}Price")).is_none());
        }
        assert!(v["params"].get("pegPriceType").is_none());
    }
}
#[test]
fn sor_routes_only_explicit_command_and_keeps_base_quantity() {
    for kind in [OrderType::Limit, OrderType::Market] {
        let c = advanced(SpotAdvancedCommand::SorNew(order("sor", kind)));
        assert!(
            prepared(c.clone(), Transport::Rest)
                .unwrap()
                .path()
                .starts_with("/api/v3/sor/order?")
        );
        let v = ws(c);
        assert_eq!(v["method"], "sor.order.place");
        assert_eq!(v["params"]["quantity"], "0.001");
        assert!(v["params"].get("quoteOrderQty").is_none());
    }
    assert_eq!(ws(command())["method"], "order.place");
}
#[test]
fn illegal_list_roles_fail_before_any_prepared_request() {
    for kind in [ListKind::Oco, ListKind::Oto, ListKind::Otoco] {
        for change in 0..3 {
            let mut c = list(kind);
            if let CommandBody::SpotList(ref mut l) = c.body {
                let mut legs = l.legs.as_slice().to_vec();
                match change {
                    0 => legs[0] = order("wrong-working", OrderType::Market),
                    1 => {
                        let last = legs.last_mut().unwrap();
                        *last = order("wrong-below", OrderType::Limit);
                        last.time_in_force = Some(TimeInForce::PostOnly);
                    }
                    _ => legs[1].self_trade_prevention = SelfTradePrevention::ExpireMaker,
                }
                // An OTO pending LIMIT_MAKER is explicitly legal.
                if kind == ListKind::Oto && change == 1 {
                    continue;
                }
                l.legs = BoundedList::new(legs).unwrap();
            }
            assert!(prepared(c, Transport::WebSocket).is_err());
        }
    }
}
#[test]
fn maker_icebergs_and_partial_peg_preserve_each_leg() {
    let mut c = list(ListKind::Oco);
    if let CommandBody::SpotList(ref mut l) = c.body {
        let mut legs = l.legs.as_slice().to_vec();
        legs[0].iceberg_quantity = Some(d(1, -4));
        l.legs = BoundedList::new(legs).unwrap();
    }
    let v = ws(c.clone());
    assert_eq!(v["params"]["aboveType"], "LIMIT_MAKER");
    assert_eq!(v["params"]["aboveTimeInForce"], "GTC");
    assert_eq!(v["params"]["aboveIcebergQty"], "0.0001");
    let CommandBody::SpotList(l) = c.body else {
        panic!()
    };
    let mut legs: Vec<_> = l
        .legs
        .as_slice()
        .iter()
        .cloned()
        .map(|order| SpotOrder { order, peg: None })
        .collect();
    legs[1].order.price = None;
    legs[1].peg = Some(Peg {
        price_type: PegPriceType::Market,
        offset_levels: None,
    });
    let v = ws(advanced(SpotAdvancedCommand::List(SpotAdvancedList {
        list_id: l.list_id,
        client_id: l.client_id,
        kind: l.kind,
        legs: BoundedList::new(legs).unwrap(),
    })));
    assert_eq!(v["params"]["abovePrice"], "60000");
    assert!(v["params"].get("abovePegPriceType").is_none());
    assert_eq!(v["params"]["belowPegPriceType"], "MARKET_PEG");
}
#[test]
fn advanced_request_budget_and_identity_checks_remain_exact() {
    for transport in [Transport::Rest, Transport::WebSocket] {
        let c = CanonicalCommand::new(advanced(SpotAdvancedCommand::New(pegged_order(
            "peg",
            OrderType::Limit,
            PegPriceType::Primary,
            Some(100),
        ))))
        .unwrap();
        let key = SigningKey::hmac(b"extension-test-key").unwrap();
        let mut cred = Credentials {
            account: 2,
            environment: Environment::Test,
            market: Market::Spot,
            api_key: "test-key",
            signing_key: &key,
        };
        let opt = RequestOptions {
            transport,
            timestamp: Timestamp::Milliseconds(1668481559918),
            recv_window_micros: 5_000_000,
            request_id: "extensions",
        };
        let p = prepare(&c, &cred, opt, MAX_REQUEST_BYTES).unwrap();
        let n = if transport == Transport::Rest {
            p.path().len()
        } else {
            p.payload().len()
        };
        assert!(prepare(&c, &cred, opt, n).is_ok());
        assert!(prepare(&c, &cred, opt, n - 1).is_err());
        assert_eq!(p.command_digest(), c.digest());
        for wrong in 0..3 {
            match wrong {
                0 => cred.account = 3,
                1 => {
                    cred.account = 2;
                    cred.environment = Environment::Production;
                }
                _ => {
                    cred.environment = Environment::Test;
                    cred.market = Market::Usdm;
                }
            }
            assert!(matches!(
                prepare(&c, &cred, opt, MAX_REQUEST_BYTES),
                Err(tickwire_binance::Error::IdentityMismatch)
            ));
        }
    }
}

mod real_network {
    use super::*;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use ring::hmac;
    use sha1::Digest;
    use std::{
        io::{Read, Write},
        net::{SocketAddr, TcpListener, TcpStream},
        sync::Arc,
        thread,
        time::{Duration, Instant},
    };
    use tickwire_transport::{self as net, http, mio, rustls, ws};
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

    fn unpercent(s: &str) -> String {
        let b = s.as_bytes();
        let mut out = Vec::new();
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
        String::from_utf8(out).unwrap()
    }
    fn verify(preimage: &str, signature: &str) {
        let bytes: Vec<_> = (0..signature.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&signature[i..i + 2], 16).unwrap())
            .collect();
        hmac::verify(
            &hmac::Key::new(hmac::HMAC_SHA256, b"extension-test-key"),
            preimage.as_bytes(),
            &bytes,
        )
        .unwrap();
    }
    fn cases() -> Vec<(
        Command,
        &'static str,
        &'static str,
        &'static str,
        &'static str,
    )> {
        vec![
            (
                list(ListKind::Oco),
                "/api/v3/orderList/oco",
                "orderList.place.oco",
                "aboveType",
                "LIMIT_MAKER",
            ),
            (
                list(ListKind::Oto),
                "/api/v3/orderList/oto",
                "orderList.place.oto",
                "pendingType",
                "MARKET",
            ),
            (
                list(ListKind::Otoco),
                "/api/v3/orderList/otoco",
                "orderList.place.otoco",
                "pendingBelowType",
                "STOP_LOSS_LIMIT",
            ),
            (
                advanced(SpotAdvancedCommand::New(pegged_order(
                    "peg",
                    OrderType::Limit,
                    PegPriceType::Primary,
                    Some(100),
                ))),
                "/api/v3/order",
                "order.place",
                "pegPriceType",
                "PRIMARY_PEG",
            ),
            (
                advanced(SpotAdvancedCommand::SorNew(order("sor", OrderType::Limit))),
                "/api/v3/sor/order",
                "sor.order.place",
                "type",
                "LIMIT",
            ),
        ]
    }
    #[test]
    fn extensions_cross_real_tls_http_and_wss_with_verified_signature() {
        for transport in [Transport::Rest, Transport::WebSocket] {
            let (config, server) = configs();
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = l.local_addr().unwrap();
            let h = thread::spawn(move || {
                let mut s = accept(l, server);
                if transport == Transport::WebSocket {
                    let raw = headers(&mut s);
                    assert!(raw.starts_with("GET /ws-api/v3 HTTP/1.1\r\n"));
                    let key = raw
                        .lines()
                        .find_map(|l| l.strip_prefix("Sec-WebSocket-Key: "))
                        .unwrap();
                    let accept = STANDARD.encode(sha1::Sha1::digest(
                        format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes(),
                    ));
                    write!(s,"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").unwrap();
                    s.flush().unwrap();
                }
                for (_, path, method, key, value) in cases() {
                    if transport == Transport::Rest {
                        let raw = headers(&mut s);
                        assert!(raw.contains("X-MBX-APIKEY: test-key\r\n"));
                        let actual = raw
                            .lines()
                            .next()
                            .unwrap()
                            .split_whitespace()
                            .nth(1)
                            .unwrap();
                        let (endpoint, query) = actual.split_once('?').unwrap();
                        assert_eq!(endpoint, path);
                        let (preimage, sig) = query.rsplit_once("&signature=").unwrap();
                        verify(preimage, &unpercent(sig));
                        let fields: std::collections::BTreeMap<_, _> = preimage
                            .split('&')
                            .map(|field| {
                                let (k, v) = field.split_once('=').unwrap();
                                (k, unpercent(v))
                            })
                            .collect();
                        assert_eq!(fields.get(key).unwrap(), value);
                        assert_eq!(fields.get("symbol").unwrap(), "BTCUSDT");
                        s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}")
                            .unwrap();
                        s.flush().unwrap();
                    } else {
                        let v: Value = serde_json::from_slice(&frame(&mut s)).unwrap();
                        assert_eq!(v["method"], method);
                        assert_eq!(v["params"][key], value);
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
                        verify(&preimage, v["params"]["signature"].as_str().unwrap());
                        s.write_all(b"\x81\x02{}").unwrap();
                        s.flush().unwrap();
                    }
                }
                s.conn.send_close_notify();
                s.flush().unwrap();
            });
            let mut c = client(addr, config);
            for (index, (command, _, _, _, _)) in cases().into_iter().enumerate() {
                let canonical = CanonicalCommand::new(command.clone()).unwrap();
                let p = prepared(command, transport).unwrap();
                if transport == Transport::WebSocket && index == 0 {
                    let handshake =
                        ws::ClientHandshake::new(p.authority(), p.path(), &[], 4096).unwrap();
                    send(&mut c, handshake.request(), canonical.digest(), &p);
                    handshake.validate(&read_http(&mut c)).unwrap();
                }
                let bytes = if transport == Transport::Rest {
                    http::encode_request(
                        p.method().as_str(),
                        p.path(),
                        &[
                            ("Host", p.authority()),
                            ("X-MBX-APIKEY", p.api_key_header().unwrap()),
                        ],
                        p.payload(),
                        32768,
                    )
                    .unwrap()
                } else {
                    ws::encode_client(ws::Opcode::Text, p.payload(), 32768).unwrap()
                };
                send(&mut c, &bytes, canonical.digest(), &p);
                if transport == Transport::Rest {
                    assert_eq!(read_http(&mut c).body, b"{}");
                } else {
                    let mut decoder = ws::Decoder::new(16384, 16384).unwrap();
                    let until = end();
                    loop {
                        assert!(Instant::now() < until);
                        let result = c.drive(Instant::now(), net::IoBudget::default());
                        let mut b = [0; 1024];
                        let n = c.read(&mut b);
                        decoder.push(&b[..n]).unwrap();
                        match decoder.poll_message().unwrap() {
                            ws::PollMessage::Message(m) => {
                                assert_eq!(m.payload, b"{}");
                                break;
                            }
                            ws::PollMessage::Yielded => continue,
                            ws::PollMessage::NeedInput => {}
                        }
                        result.unwrap();
                        thread::yield_now();
                    }
                }
            }
            h.join().unwrap();
        }
    }
}
