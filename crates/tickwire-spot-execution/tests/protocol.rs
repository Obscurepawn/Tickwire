#[path = "../../tickwire-venue/tests/common/mod.rs"]
mod fixture;
use tickwire_gateway::Encoder;
use tickwire_spot_execution::{Credentials, SpotEncoder, SpotVenue};
use tickwire_venue::{CanonicalCommand, CommandBody, Environment, Symbol};

fn command(venue: SpotVenue) -> CanonicalCommand {
    let mut cmd = fixture::command();
    cmd.context.request.venue = venue.id();
    cmd.context.instrument.symbol = Symbol::new(if venue == SpotVenue::Okx {
        "BTC-USDT"
    } else {
        "BTCUSDT"
    })
    .unwrap();
    let CommandBody::New(order) = &mut cmd.body else {
        unreachable!()
    };
    order.client_id = fixture::text("client1");
    CanonicalCommand::new(cmd).unwrap()
}
#[test]
fn new_requests_are_scoped_signed_and_keep_the_canonical_id() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let key = if venue == SpotVenue::Bybit {
            Credentials::bybit("public", b"secret")
        } else {
            Credentials::okx("public", b"secret", "pass")
        };
        let mut encoder =
            SpotEncoder::new(key, Environment::Test, 2, || Ok(1_700_000_000_123)).unwrap();
        let cmd = command(venue);
        let request = encoder.encode(&cmd).unwrap();
        assert_eq!(request.command_digest(), cmd.digest());
        let http = std::str::from_utf8(request.bytes()).unwrap();
        assert!(http.contains("client1"));
        assert!(http.starts_with(if venue == SpotVenue::Bybit {
            "POST /v5/order/create "
        } else {
            "POST /api/v5/trade/order "
        }));
        assert!(!http.contains("secret"));
    }
}

#[test]
fn fixed_hmac_vectors_cover_both_post_bodies_and_get_queries() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let key = if venue == SpotVenue::Bybit {
            Credentials::bybit("public", b"secret")
        } else {
            Credentials::okx("public", b"secret", "pass")
        };
        let mut enc =
            SpotEncoder::new(key, Environment::Test, 2, || Ok(1_700_000_000_123)).unwrap();
        let original = command(venue);
        let mut raw = original.command().clone();
        raw.body = CommandBody::Query(fixture::target());
        let query = CanonicalCommand::new(raw).unwrap();
        let golden = if venue == SpotVenue::Bybit {
            [
                "918483e53076a1b9d02e4d4b70388c956907850aba8a247cdd659dc23589d95e",
                "b4797d65995496d2e083dacb925bc4138861e8885bd6598b32bc0a35da582f73",
            ]
        } else {
            [
                "JKWZEJ+eDqP04yz21c3YmomultQIchW8OHOhYw5YlTs=",
                "eKQRMKhT5Z6q0mJ8VEbZpXgmjXsoDueIC5hHggaoFpE=",
            ]
        };
        for (c, signature) in [original, query].iter().zip(golden) {
            let output = enc.encode(c).unwrap();
            let http = std::str::from_utf8(output.bytes()).unwrap();
            assert!(http.contains(signature));
            if venue == SpotVenue::Okx {
                assert!(http.contains("x-simulated-trading: 1\r\n"));
                assert!(http.contains("2023-11-14T22:13:20.123Z"))
            }
        }
    }
}
#[test]
fn errors_and_missing_orders_never_become_order_states() {
    use tickwire_spot_execution::{Reply, ReportError, decode_reply};
    let original = command(SpotVenue::Okx);
    let mut raw = original.command().clone();
    raw.body = CommandBody::Query(fixture::target());
    let query = CanonicalCommand::new(raw).unwrap();
    assert_eq!(
        decode_reply(&query, 503, b"unavailable"),
        Ok(Reply::Unknown)
    );
    assert_eq!(
        decode_reply(&query, 200, br#"{"code":"0","data":[]}"#),
        Ok(Reply::Unknown)
    );
    assert_eq!(
        decode_reply(&query, 200, br#"{"code":"51603","data":[]}"#),
        Ok(Reply::RequestIssue {
            code: "51603".into()
        })
    );
    assert_eq!(
        decode_reply(
            &original,
            200,
            br#"{"code":"0","data":[{"sCode":"0","ordId":"1","clOrdId":"wrong"}]}"#
        ),
        Err(ReportError::Identity)
    );
    let mut encoder = SpotEncoder::new(
        Credentials::bybit("key", b"secret"),
        Environment::Test,
        2,
        || Ok(1000),
    )
    .unwrap();
    assert!(encoder.encode(&original).is_err());
}
#[test]
fn market_order_wire_preserves_base_units_and_forbids_okx_balance_trimming() {
    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let original = command(venue);
        let mut c = original.command().clone();
        let CommandBody::New(ref mut new) = c.body else {
            unreachable!()
        };
        new.order_type = tickwire_venue::OrderType::Market;
        new.time_in_force = None;
        new.price = None;
        let credentials = if venue == SpotVenue::Bybit {
            Credentials::bybit("key", b"secret")
        } else {
            Credentials::okx("key", b"secret", "pass")
        };
        let mut encoder =
            SpotEncoder::new(credentials, Environment::Test, 2, || Ok(1700000000123)).unwrap();
        let wire = encoder.encode(&CanonicalCommand::new(c).unwrap()).unwrap();
        let (_, body) = std::str::from_utf8(wire.bytes())
            .unwrap()
            .split_once("\r\n\r\n")
            .unwrap();
        let body: serde_json::Value = serde_json::from_str(body).unwrap();
        if venue == SpotVenue::Bybit {
            assert_eq!(body["marketUnit"], "baseCoin");
        } else {
            assert_eq!(body["tgtCcy"], "base_ccy");
            assert_eq!(body["banAmend"], true);
        }
    }
}

#[test]
fn classify_issue_partitions_hazards_not_found_deterministic_rejects_and_unknown_codes() {
    use tickwire_spot_execution::{SpotIssueKind, classify_issue, is_http_account_hazard};

    for code in ["10003", "10004", "10005", "10006", "403"] {
        assert_eq!(
            classify_issue(SpotVenue::Bybit, code),
            SpotIssueKind::AccountHazard,
            "bybit {code}"
        );
    }
    for code in ["50011", "50101", "50103", "50111", "50113", "50119"] {
        assert_eq!(
            classify_issue(SpotVenue::Okx, code),
            SpotIssueKind::AccountHazard,
            "okx {code}"
        );
    }

    for code in ["110001", "170213"] {
        assert_eq!(
            classify_issue(SpotVenue::Bybit, code),
            SpotIssueKind::NotFound,
            "bybit {code}"
        );
    }
    for code in ["51603", "51400"] {
        assert_eq!(
            classify_issue(SpotVenue::Okx, code),
            SpotIssueKind::NotFound,
            "okx {code}"
        );
    }

    for code in ["10000", "10002", "10016", "170007", "999999", "unknown"] {
        assert_eq!(
            classify_issue(SpotVenue::Bybit, code),
            SpotIssueKind::Ambiguous,
            "bybit {code}"
        );
    }
    for code in [
        "50000", "50001", "50004", "50013", "50026", "59999", "unknown",
    ] {
        assert_eq!(
            classify_issue(SpotVenue::Okx, code),
            SpotIssueKind::Ambiguous,
            "okx {code}"
        );
    }

    for code in ["170124", "170131", "170135", "170136", "170140", "170193"] {
        assert_eq!(
            classify_issue(SpotVenue::Bybit, code),
            SpotIssueKind::DeterministicReject,
            "bybit {code}"
        );
    }
    for code in ["51000", "51004", "51006", "51008", "51019", "51020"] {
        assert_eq!(
            classify_issue(SpotVenue::Okx, code),
            SpotIssueKind::DeterministicReject,
            "okx {code}"
        );
    }

    for status in [401, 403, 418, 429] {
        assert!(is_http_account_hazard(status), "status {status}");
    }
    for status in [200, 400, 404, 500, 502, 503, 504] {
        assert!(!is_http_account_hazard(status), "status {status}");
    }
}

#[test]
fn signed_rest_get_builders_cover_bybit_and_okx_status_and_trade_endpoints() {
    use tickwire_spot_execution::{
        build_order_status_get, build_order_trades_get, bybit_order_history_path,
        bybit_order_realtime_path, bybit_order_trades_path, encode_signed_get,
        okx_order_status_path, okx_order_trades_path,
    };

    assert_eq!(
        bybit_order_realtime_path(Some("BTCUSDT"), "client1").unwrap(),
        "/v5/order/realtime?category=spot&symbol=BTCUSDT&orderLinkId=client1"
    );
    assert_eq!(
        bybit_order_realtime_path(None, "client1").unwrap(),
        "/v5/order/realtime?category=spot&orderLinkId=client1"
    );
    assert_eq!(
        bybit_order_history_path(Some("BTCUSDT"), "client1").unwrap(),
        "/v5/order/history?category=spot&symbol=BTCUSDT&orderLinkId=client1"
    );
    assert_eq!(
        bybit_order_trades_path(Some("BTCUSDT"), Some("client1"), None).unwrap(),
        "/v5/execution/list?category=spot&symbol=BTCUSDT&orderLinkId=client1"
    );
    assert_eq!(
        bybit_order_trades_path(None, None, Some("ord99")).unwrap(),
        "/v5/execution/list?category=spot&orderId=ord99"
    );
    assert_eq!(
        okx_order_status_path("BTC-USDT", "client1").unwrap(),
        "/api/v5/trade/order?instId=BTC-USDT&clOrdId=client1"
    );
    assert_eq!(
        okx_order_trades_path("BTC-USDT", Some("ord99")).unwrap(),
        "/api/v5/trade/fills?instType=SPOT&instId=BTC-USDT&ordId=ord99"
    );

    let bybit_creds = Credentials::bybit("public", b"secret");
    let bybit_status = build_order_status_get(
        &bybit_creds,
        Environment::Test,
        1_700_000_000_123,
        "BTCUSDT",
        "client1",
        false,
    )
    .unwrap();
    let expected_wire = encode_signed_get(
        &bybit_creds,
        Environment::Test,
        1_700_000_000_123,
        &bybit_status.path,
    )
    .unwrap();
    let expected_http = std::str::from_utf8(&expected_wire).unwrap();
    assert_eq!(bybit_status.method, "GET");
    assert_eq!(
        bybit_status.path,
        "/v5/order/realtime?category=spot&symbol=BTCUSDT&orderLinkId=client1"
    );
    let sign_hdr = bybit_status
        .headers
        .iter()
        .find(|(k, _)| k == "X-BAPI-SIGN")
        .map(|(_, v)| v.as_str())
        .unwrap();
    assert!(expected_http.contains(sign_hdr));
    assert!(
        !bybit_status
            .headers
            .iter()
            .any(|(_, v)| v.contains("secret"))
    );

    let okx_creds = Credentials::okx("public", b"secret", "pass");
    let okx_trades = build_order_trades_get(
        &okx_creds,
        Environment::Test,
        1_700_000_000_123,
        "BTC-USDT",
        Some("client1"),
        Some("ord99"),
    )
    .unwrap();
    assert_eq!(okx_trades.method, "GET");
    assert_eq!(
        okx_trades.path,
        "/api/v5/trade/fills?instType=SPOT&instId=BTC-USDT&ordId=ord99"
    );
    assert!(
        okx_trades
            .headers
            .iter()
            .any(|(k, v)| k == "x-simulated-trading" && v == "1")
    );
}

#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod gw_common;

#[test]
fn rest_session_per_order_isolation_handles_rejects_ambiguity_socket_drops_and_hazards() {
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::TcpListener,
        sync::Arc,
        thread,
        time::{Duration, Instant},
    };
    use tickwire_account::{DoubtReason, IntentKey, OrderKey};
    use tickwire_account_codec::CanonicalBaseline;
    use tickwire_account_gateway::{AccountGateway, ConnectionConfig, Status};
    use tickwire_account_journal::InMemoryBackend;
    use tickwire_spot_execution::{Reply, RestSession, authority};
    use tickwire_transport::{Connection, IoBudget, Limits, Security, rustls};

    for venue in [SpotVenue::Bybit, SpotVenue::Okx] {
        let host = authority(venue, Environment::Test);
        let cert = rcgen::generate_simple_self_signed(vec![host.into()]).unwrap();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        let server_cfg = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![cert.cert.der().clone()], key.into())
                .unwrap(),
        );
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let client_tls = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server_tls = Arc::clone(&server_cfg);
        let server_job = thread::spawn(move || {
            let read_one_http = |io: &mut BufReader<
                rustls::StreamOwned<rustls::ServerConnection, std::net::TcpStream>,
            >| {
                let mut head = String::new();
                io.read_line(&mut head).unwrap();
                let mut size = 0usize;
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
                let mut body = vec![0u8; size];
                io.read_exact(&mut body).unwrap();
            };

            // Socket 1:
            // Req 0 (New client1): DeterministicReject -> keep-alive stays open!
            // Req 1 (New client2): Ambiguous code -> keep-alive stays open!
            // Req 2 (New client3): Ack -> keep-alive stays open!
            // Req 3 (Cancel client3): NotFound -> keep-alive stays open!
            // Req 4 (New client4): Close TCP socket without writing a response!
            {
                let (sock, _) = listener.accept().unwrap();
                sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                let mut io = BufReader::new(rustls::StreamOwned::new(
                    rustls::ServerConnection::new(Arc::clone(&server_tls)).unwrap(),
                    sock,
                ));
                let replies = if venue == SpotVenue::Bybit {
                    [
                        r#"{"retCode":170193,"retMsg":"post only would take liquidity"}"#,
                        r#"{"retCode":170007,"retMsg":"timeout waiting for backend"}"#,
                        r#"{"retCode":0,"result":{"orderId":"native3","orderLinkId":"client3"}}"#,
                        r#"{"retCode":110001,"retMsg":"order does not exist"}"#,
                    ]
                } else {
                    [
                        r#"{"code":"1","data":[{"sCode":"51008","sMsg":"insufficient balance","ordId":"","clOrdId":"client1"}]}"#,
                        r#"{"code":"50004","msg":"endpoint request timeout","data":[]}"#,
                        r#"{"code":"0","data":[{"sCode":"0","ordId":"native3","clOrdId":"client3"}]}"#,
                        r#"{"code":"1","data":[{"sCode":"51400","sMsg":"order does not exist","ordId":"native3","clOrdId":"client3"}]}"#,
                    ]
                };
                for body in replies {
                    read_one_http(&mut io);
                    write!(
                        io.get_mut(),
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    )
                    .unwrap();
                    io.get_mut().flush().unwrap();
                }
                read_one_http(&mut io);
                // Drop `io` immediately to simulate post-send socket EOF!
            }

            // Socket 2 (installed via `install_transport_connection`):
            // Req 5 (New client5): Ack!
            // Req 6 (New client6): AccountHazard (rate limit)!
            {
                let (sock, _) = listener.accept().unwrap();
                sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                let mut io = BufReader::new(rustls::StreamOwned::new(
                    rustls::ServerConnection::new(server_tls).unwrap(),
                    sock,
                ));
                let ack = if venue == SpotVenue::Bybit {
                    r#"{"retCode":0,"result":{"orderId":"native5","orderLinkId":"client5"}}"#
                } else {
                    r#"{"code":"0","data":[{"sCode":"0","ordId":"native5","clOrdId":"client5"}]}"#
                };
                read_one_http(&mut io);
                write!(
                    io.get_mut(),
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    ack.len(),
                    ack
                )
                .unwrap();
                io.get_mut().flush().unwrap();

                let hazard = if venue == SpotVenue::Bybit {
                    r#"{"retCode":10006,"retMsg":"Too many visits"}"#
                } else {
                    r#"{"code":"50011","msg":"Rate limit reached","data":[]}"#
                };
                read_one_http(&mut io);
                write!(
                    io.get_mut(),
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    hazard.len(),
                    hazard
                )
                .unwrap();
                io.get_mut().flush().unwrap();
            }
        });

        let (mut snapshot, mut catalog, template) = gw_common::fixture::fixture();
        snapshot.scope.venue = venue.id();
        snapshot.valid_until = u64::MAX;
        let symbol = Symbol::new(if venue == SpotVenue::Okx {
            "BTC-USDT"
        } else {
            "BTCUSDT"
        })
        .unwrap();
        catalog.entries[0].instrument.symbol = symbol.clone();
        let base = CanonicalBaseline::new(snapshot, catalog).unwrap();
        let mut cfg = gw_common::config(&base);
        let credentials = if venue == SpotVenue::Okx {
            Credentials::okx("key", b"secret", "pass")
        } else {
            Credentials::bybit("key", b"secret")
        };
        let encoder =
            SpotEncoder::new(credentials, Environment::Test, 2, || Ok(1_700_000_000_123)).unwrap();
        cfg.route = encoder.route();
        let auth = gw_common::MockAuthority::default();
        auth.policy.lock().unwrap().gate.now = 500;
        auth.policy.lock().unwrap().gate.lease_until = 10_000;
        let gw = AccountGateway::create(
            InMemoryBackend::new(),
            cfg,
            base,
            auth.clone(),
            encoder,
            ConnectionConfig {
                address,
                tls: Arc::clone(&client_tls),
                limits: Limits::default(),
                connect_deadline: gw_common::until(),
            },
        )
        .unwrap();
        let mut session = RestSession::new(gw, 1)
            .unwrap()
            .with_per_order_doubt_isolation(true);
        assert!(session.per_order_doubt_isolation());

        let make_new = |idx: u64, client: &str| {
            let mut cmd = template.clone();
            cmd.context.request.venue = venue.id();
            cmd.context.request.intent_id = idx;
            cmd.context.request.recovery_object = idx;
            cmd.context.request.deadline = 5_000;
            cmd.context.strategy.strategy_intent_seq = idx;
            cmd.context.instrument.symbol = symbol.clone();
            let CommandBody::New(ref mut order) = cmd.body else {
                unreachable!()
            };
            order.client_id = tickwire_venue::ClientId::new(client).unwrap();
            CanonicalCommand::new(cmd).unwrap()
        };
        let drive_reply = |session: &mut RestSession<_, _, _>, mono_ns: u64| {
            let deadline = gw_common::until();
            loop {
                assert!(Instant::now() < deadline);
                if let Some(r) = session
                    .poll(Instant::now(), mono_ns, IoBudget::default())
                    .unwrap()
                {
                    return r;
                }
                thread::yield_now();
            }
        };

        // 1. DeterministicReject on New: marks uncreated = true, releases reservation, does NOT freeze!
        let cmd1 = make_new(1, "client1");
        assert_eq!(
            session.submit(cmd1, 100, gw_common::until()).unwrap(),
            Status::Queued
        );
        let rep1 = drive_reply(&mut session, 101);
        assert!(matches!(rep1, Reply::RequestIssue { .. }));
        assert!(!session.gateway().account().frozen());
        assert!(!session.gateway().halted());
        let key1 = OrderKey {
            intent: IntentKey {
                epoch: 4,
                intent_id: 1,
            },
            client_id: tickwire_venue::ClientId::new("client1").unwrap(),
        };
        let v1 = session.gateway().account().order(&key1).unwrap();
        assert!(v1.uncreated);
        assert_eq!(
            session
                .gateway()
                .account()
                .asset(gw_common::fixture::key(2))
                .unwrap()
                .reserved,
            gw_common::fixture::units(0)
        );

        // 2. Ambiguous code on New: isolates ONLY client2 in PlaceAmbiguous, keeps account unfrozen & socket open!
        let cmd2 = make_new(2, "client2");
        assert_eq!(
            session.submit(cmd2, 110, gw_common::until()).unwrap(),
            Status::Queued
        );
        let rep2 = drive_reply(&mut session, 111);
        assert!(matches!(rep2, Reply::RequestIssue { .. }));
        assert!(!session.gateway().account().frozen());
        let key2 = OrderKey {
            intent: IntentKey {
                epoch: 4,
                intent_id: 2,
            },
            client_id: tickwire_venue::ClientId::new("client2").unwrap(),
        };
        assert_eq!(
            session.gateway().account().order_in_doubt(&key2),
            Some(DoubtReason::PlaceAmbiguous)
        );
        let reserved_after_2 = session
            .gateway()
            .account()
            .asset(gw_common::fixture::key(2))
            .unwrap()
            .reserved;
        assert!(reserved_after_2.atoms() > 0);

        // 3. Reuse the SAME HTTP keep-alive connection for client3 New + Cancel NotFound!
        let cmd3 = make_new(3, "client3");
        assert_eq!(
            session.submit(cmd3, 120, gw_common::until()).unwrap(),
            Status::Queued
        );
        assert!(matches!(
            drive_reply(&mut session, 121),
            Reply::Acknowledged { .. }
        ));
        let mut cancel_raw = template.clone();
        cancel_raw.context.request.venue = venue.id();
        cancel_raw.context.request.intent_id = 4;
        cancel_raw.context.request.recovery_object = 4;
        cancel_raw.context.request.deadline = 5_000;
        cancel_raw.context.strategy.strategy_intent_seq = 4;
        cancel_raw.context.instrument.symbol = symbol.clone();
        cancel_raw.body = CommandBody::Cancel(tickwire_venue::OrderTarget {
            order_id: [9; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(tickwire_venue::ClientId::new("client3").unwrap()),
            venue_order_id: None,
        });
        let cancel_cmd = CanonicalCommand::new(cancel_raw).unwrap();
        assert_eq!(
            session.submit(cancel_cmd, 125, gw_common::until()).unwrap(),
            Status::Queued
        );
        assert!(matches!(
            drive_reply(&mut session, 126),
            Reply::RequestIssue { .. }
        ));
        assert!(!session.gateway().account().frozen());

        // 4. Socket EOF after send on client4: drops broken transport WITHOUT halting or freezing!
        let cmd4 = make_new(5, "client4");
        assert_eq!(
            session.submit(cmd4, 130, gw_common::until()).unwrap(),
            Status::Queued
        );
        assert_eq!(drive_reply(&mut session, 131), Reply::Unknown);
        assert!(!session.has_transport_connection());
        assert!(!session.gateway().halted());
        assert!(!session.gateway().account().frozen());
        let key4 = OrderKey {
            intent: IntentKey {
                epoch: 4,
                intent_id: 5,
            },
            client_id: tickwire_venue::ClientId::new("client4").unwrap(),
        };
        assert_eq!(
            session.gateway().account().order_in_doubt(&key4),
            Some(DoubtReason::PlaceAmbiguous)
        );

        // 5. Install replacement transport connection and immediately submit client5!
        let server_name = rustls::pki_types::ServerName::try_from(host.to_owned()).unwrap();
        let replacement = Connection::connect(
            address,
            Security::Tls {
                server_name,
                config: Arc::clone(&client_tls),
            },
            Limits::default(),
            gw_common::until(),
        )
        .unwrap();
        session
            .install_transport_connection(replacement, Limits::default())
            .unwrap();
        assert!(session.has_transport_connection());

        let cmd5 = make_new(6, "client5");
        assert_eq!(
            session.submit(cmd5, 140, gw_common::until()).unwrap(),
            Status::Queued
        );
        assert!(matches!(
            drive_reply(&mut session, 141),
            Reply::Acknowledged { .. }
        ));
        assert!(!session.gateway().account().frozen());

        // 6. AccountHazard (rate limit) freezes Account immediately!
        let cmd6 = make_new(7, "client6");
        assert_eq!(
            session.submit(cmd6, 150, gw_common::until()).unwrap(),
            Status::Queued
        );
        assert!(matches!(
            drive_reply(&mut session, 151),
            Reply::RequestIssue { .. }
        ));
        assert!(session.gateway().account().frozen());

        server_job.join().unwrap();
    }
}
