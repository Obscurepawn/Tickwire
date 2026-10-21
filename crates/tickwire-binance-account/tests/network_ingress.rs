#[path = "../../tickwire-account-gateway/tests/common/fault_backend.rs"]
mod faults;
#[path = "../../tickwire-account-gateway/tests/common/mod.rs"]
mod gateway;
#[path = "../../tickwire-binance-private/tests/common/mod.rs"]
mod network;
use std::{
    io::Read,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    thread,
    time::Instant,
};
use tickwire_account::Applied;
use tickwire_account_gateway::AccountGateway;
use tickwire_account_journal::FileBackend;
use tickwire_binance_account::*;
use tickwire_binance_private as private;
use tickwire_journal::store::FileStore;

fn config(b: &tickwire_account_codec::CanonicalBaseline) -> Config {
    Config {
        physical: tickwire_journal::Config::new([81; 16], [1; 16]),
        writer_epoch: 4,
        baseline: b.clone(),
        account_stream: [2; 16],
        max_records: 64,
        income_association_authority: None,
        income_baseline_authority: None,
        max_registrations: 16,
        max_raw_bytes: 32_768,
        max_retained_bytes: 1 << 20,
    }
}
fn messages(unknown: bool) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    if unknown {
        out.push(
            serde_json::to_vec(&serde_json::json!({
                "subscriptionId": 0,
                "event": {"e":"balanceUpdate","E":10,"a":"USDT","d":"1","T":9}
            }))
            .unwrap(),
        );
    }
    out.push(br#"{"subscriptionId":0,"event":{"e":"executionReport","E":10,"s":"BTCUSDT","c":"test-1","S":"BUY","o":"LIMIT","f":"GTC","q":"0.001","p":"60000","g":-1,"C":"","x":"TRADE","X":"FILLED","i":22,"l":"0.001","z":"0.001","L":"60000","n":"0.01","N":"USDT","T":9,"t":33,"I":44,"m":true,"Z":"60","Y":"60","r":"NONE"}}"#.to_vec());
    out
}
fn receive(
    messages: Vec<Vec<u8>>,
    mut sink: impl FnMut(private::Delivery<'_>) -> private::Result<()>,
) {
    let count = messages.len();
    let (tls, server) = network::certs();
    let listener = network::listener();
    let address = listener.local_addr().unwrap();
    let h = thread::spawn(move || {
        let mut s = network::accept(&listener, server);
        network::upgrade(&mut s, "/ws-api/v3");
        let request = network::json(&mut s);
        network::verify_signature(&request, 0);
        network::reply(
            &mut s,
            &request["id"],
            serde_json::json!({"subscriptionId":0}),
        );
        for message in messages {
            network::frame(&mut s, 1, &message);
        }
        let _ = s.read(&mut [0]);
    });
    let key = private::SigningKey::hmac(network::SECRET).unwrap();
    let mut client = private::PrivateClient::new(
        private::SessionConfig::new(network::scope(private::Market::Spot)),
        private::Authentication::SpotSignature {
            api_key: network::KEY,
            signing_key: &key,
        },
        private::NetworkConfig {
            stream_address: address,
            management_address: None,
            tls,
        },
        5,
    )
    .unwrap();
    let mut clock = network::TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &private::Charge, _: &private::ClockSample| true;
    let mut delivered = 0;
    let end = gateway::until();
    while delivered < count && client.status().phase != private::Phase::Stopped {
        assert!(Instant::now() < end);
        let result = client.poll(
            &mut clock,
            private::PollBudget::default(),
            &mut permit,
            |d| {
                if matches!(d, private::Delivery::Event(_)) {
                    delivered += 1;
                }
                sink(d)
            },
        );
        assert!(result.is_ok() || matches!(result, Err(private::Error::Sink)));
        thread::yield_now();
    }
    client.stop(private::StopMode::Disconnect).unwrap();
    drop(client);
    h.join().unwrap();
    assert_eq!(delivered, count);
}
#[test]
fn real_wss_fill_enters_both_file_logs_and_replay_is_idempotent() {
    let (account_dir, command, mut g, auth, peer) = gateway::make(false, 16384);
    let inbox_dir = gateway::Directory::new();
    let cfg = config(g.baseline());
    let mut input =
        AccountIngress::create(FileStore::open(&inbox_dir.0).unwrap(), cfg.clone()).unwrap();
    input
        .submit(&mut g, command, &[[7; 16]], 60, gateway::until())
        .unwrap();
    let mut outcomes = Vec::new();
    receive(messages(false), |d| {
        if let private::Delivery::Event(e) = d {
            outcomes.push(input.ingest(&mut g, &e).unwrap());
        }
        Ok(())
    });
    assert!(matches!(
        outcomes.as_slice(),
        [Outcome::Applied {
            applied: Applied::New,
            ..
        }]
    ));
    assert_eq!(input.position(), 3);
    assert_eq!(g.journal_position(), 2);
    let balance = g.account().asset(gateway::fixture::key(2)).unwrap();
    let ledger = g.account().ledger_sequence();
    drop(input);
    drop(g);
    assert!(peer.join().unwrap().is_empty());
    let mut gcfg = gateway::config(&cfg.baseline);
    gcfg.writer_epoch = 5;
    let mut g = AccountGateway::recover(
        FileBackend::open(&account_dir.0).unwrap(),
        gcfg,
        auth,
        gateway::TestEncoder,
        None,
    )
    .unwrap();
    let mut input =
        AccountIngress::recover(FileStore::open(&inbox_dir.0).unwrap(), cfg, None).unwrap();
    assert_eq!(input.replay_pending(&mut g, 1).unwrap(), 1);
    assert_eq!(input.replay_pending(&mut g, 10).unwrap(), 1);
    assert_eq!(input.replay_pending(&mut g, 10).unwrap(), 0);
    assert_eq!(g.journal_position(), 2);
    assert_eq!(g.account().ledger_sequence(), ledger);
    assert_eq!(
        g.account().asset(gateway::fixture::key(2)).unwrap(),
        balance
    );
    assert!(g.account().frozen());
}
#[test]
fn quarantine_freezes_new_risk_and_keeps_later_known_fills() {
    let (_account_dir, command, mut g, _auth, peer) = gateway::make(false, 16384);
    let inbox_dir = gateway::Directory::new();
    let mut input =
        AccountIngress::create(FileStore::open(&inbox_dir.0).unwrap(), config(g.baseline()))
            .unwrap();
    input
        .submit(&mut g, command, &[[7; 16]], 60, gateway::until())
        .unwrap();
    let mut outcomes = Vec::new();
    receive(messages(true), |d| {
        if let private::Delivery::Event(e) = d {
            outcomes.push(input.ingest(&mut g, &e).unwrap());
        }
        Ok(())
    });
    assert!(matches!(
        outcomes.as_slice(),
        [
            Outcome::Quarantined {
                reason: QuarantineReason::AccountReconciliation,
                ..
            },
            Outcome::Applied {
                applied: Applied::New,
                ..
            },
        ]
    ));
    assert_eq!(input.position(), 4);
    assert_eq!(input.quarantined(), 1);
    assert!(g.account().frozen() && !g.halted());
    assert_eq!(g.journal_position(), 2);
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}
fn crash_between_logs(mode: u8) {
    crash_between_logs_structure(mode, false)
}
fn crash_between_logs_structure(mode: u8, structure: bool) {
    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, mut command) = gateway::baseline(false);
    let mut ids = vec![[7; 16]];
    let mut incoming = messages(false);
    if structure {
        use tickwire_venue as v;
        let mut c = command.command().clone();
        let v::CommandBody::New(first) = c.body.clone() else {
            unreachable!()
        };
        let mut second = first.clone();
        second.client_id = v::ClientId::new("pending").unwrap();
        c.body = v::CommandBody::SpotList(v::ListOrder {
            list_id: [70; 16],
            client_id: v::ClientId::new("list-parent").unwrap(),
            kind: v::ListKind::Oto,
            legs: v::BoundedList::new(vec![first, second]).unwrap(),
        });
        command = v::CanonicalCommand::new(c).unwrap();
        ids.push([8; 16]);
        incoming=vec![br#"{"subscriptionId":0,"event":{"e":"listStatus","E":10,"T":9,"s":"BTCUSDT","g":55,"c":"OTO","l":"ALL_DONE","L":"ALL_DONE","r":"NONE","C":"list-parent","O":[{"s":"BTCUSDT","i":21,"c":"test-1"},{"s":"BTCUSDT","i":22,"c":"pending"}]}}"#.to_vec()];
    }
    let cfg = config(&baseline);
    let gcfg = gateway::config(&baseline);
    let auth = gateway::MockAuthority::default();
    let (connection, peer) = gateway::peer(&gcfg.route.authority, 16384);
    let fault = Arc::new(AtomicU8::new(0));
    let mut g = AccountGateway::create(
        faults::FaultBackend {
            inner: FileBackend::open(&account_dir.0).unwrap(),
            next: fault.clone(),
        },
        gateway::config(&baseline),
        baseline,
        auth.clone(),
        gateway::TestEncoder,
        connection,
    )
    .unwrap();
    let mut input =
        AccountIngress::create(FileStore::open(&inbox_dir.0).unwrap(), cfg.clone()).unwrap();
    input
        .submit(&mut g, command, &ids, 60, gateway::until())
        .unwrap();
    fault.store(mode, Ordering::SeqCst);
    receive(incoming, |d| {
        if let private::Delivery::Event(e) = d {
            assert!(matches!(input.ingest(&mut g, &e), Err(Error::Gateway(_))));
            return Err(private::Error::Sink);
        }
        Ok(())
    });
    assert_eq!(fault.load(Ordering::SeqCst), 0);
    assert!(input.stopped() && g.account().frozen());
    assert_eq!(input.position(), 3);
    assert_eq!(g.journal_position(), 1);
    drop(input);
    drop(g);
    assert!(peer.join().unwrap().is_empty());
    let mut gcfg = gcfg;
    gcfg.writer_epoch = 5;
    let mut g = AccountGateway::recover(
        FileBackend::open(&account_dir.0).unwrap(),
        gcfg,
        auth,
        gateway::TestEncoder,
        None,
    )
    .unwrap();
    assert_eq!(
        g.journal_position(),
        if mode == faults::LOST_SYNC_REPLY {
            2
        } else {
            1
        }
    );
    let mut input =
        AccountIngress::recover(FileStore::open(&inbox_dir.0).unwrap(), cfg, None).unwrap();
    assert_eq!(input.replay_pending(&mut g, 10).unwrap(), 2);
    assert_eq!(g.journal_position(), 2);
    assert_eq!(input.replay_pending(&mut g, 10).unwrap(), 0);
    if structure {
        assert_eq!(
            g.account()
                .asset(gateway::fixture::key(2))
                .unwrap()
                .reserved,
            gateway::fixture::units(120)
        );
        let parent = g.account().structure(&[70; 16]).unwrap();
        assert!(parent.terminal);
        assert!(
            parent
                .legs
                .iter()
                .all(|k| !g.account().order(k).unwrap().terminal)
        );
    }
}
#[test]
fn input_survives_torn_account_append_and_replays_once() {
    crash_between_logs(faults::TORN_APPEND);
}
#[test]
fn account_sync_reply_loss_does_not_double_apply_on_replay() {
    crash_between_logs(faults::LOST_SYNC_REPLY);
}
#[test]
fn parent_input_survives_torn_account_write_and_lost_sync_reply_without_release() {
    crash_between_logs_structure(faults::TORN_APPEND, true);
    crash_between_logs_structure(faults::LOST_SYNC_REPLY, true);
}
#[test]
fn disconnected_initial_gap_is_durable_and_recoverable() {
    let (_account_dir, _, mut g, _auth, peer) = gateway::make(false, 16384);
    let inbox_dir = gateway::Directory::new();
    let cfg = config(g.baseline());
    let mut input =
        AccountIngress::create(FileStore::open(&inbox_dir.0).unwrap(), cfg.clone()).unwrap();
    let (tls, _) = network::certs();
    let key = private::SigningKey::hmac(network::SECRET).unwrap();
    let client = private::PrivateClient::new(
        private::SessionConfig::new(network::scope(private::Market::Spot)),
        private::Authentication::SpotSignature {
            api_key: network::KEY,
            signing_key: &key,
        },
        private::NetworkConfig {
            stream_address: "127.0.0.1:1".parse().unwrap(),
            management_address: None,
            tls,
        },
        5,
    )
    .unwrap();
    let gap = private::GapNotice {
        token: client.current_gap().clone(),
        reason: private::Error::Closed,
    };
    assert_eq!(gap.token.connection_epoch(), 0);
    input.source_gap(&mut g, &gap).unwrap();
    assert!(g.account().frozen());
    drop(input);
    let mut input =
        AccountIngress::recover(FileStore::open(&inbox_dir.0).unwrap(), cfg, None).unwrap();
    assert_eq!(input.quarantined(), 1);
    assert_eq!(input.replay_pending(&mut g, 4).unwrap(), 1);
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn unknown_order_is_quarantined_before_later_known_fill() {
    let (_account_dir, command, mut g, _auth, peer) = gateway::make(false, 16384);
    let inbox_dir = gateway::Directory::new();
    let mut input =
        AccountIngress::create(FileStore::open(&inbox_dir.0).unwrap(), config(g.baseline()))
            .unwrap();
    input
        .submit(&mut g, command, &[[7; 16]], 60, gateway::until())
        .unwrap();
    let known = messages(false).remove(0);
    let unknown = String::from_utf8(known.clone())
        .unwrap()
        .replace("test-1", "external-order");
    let mut outcomes = Vec::new();
    receive(vec![unknown.into_bytes(), known], |d| {
        if let private::Delivery::Event(e) = d {
            outcomes.push(input.ingest(&mut g, &e).unwrap());
        }
        Ok(())
    });
    assert!(matches!(
        outcomes.as_slice(),
        [
            Outcome::Quarantined {
                reason: QuarantineReason::UnknownOrder,
                ..
            },
            Outcome::Applied {
                applied: Applied::New,
                ..
            },
        ]
    ));
    assert_eq!(g.journal_position(), 2);
    assert!(g.account().frozen());
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn durable_registration_alone_does_not_authorize_order_facts() {
    let (_account_dir, command, mut g, _auth, peer) = gateway::make(false, 16384);
    let inbox_dir = gateway::Directory::new();
    let mut input =
        AccountIngress::create(FileStore::open(&inbox_dir.0).unwrap(), config(g.baseline()))
            .unwrap();
    input.register_command(command, &[[7; 16]]).unwrap();
    receive(messages(false), |d| {
        if let private::Delivery::Event(e) = d {
            assert!(matches!(
                input.ingest(&mut g, &e).unwrap(),
                Outcome::Quarantined {
                    reason: QuarantineReason::UnadmittedOrder,
                    ..
                }
            ));
        }
        Ok(())
    });
    assert_eq!(g.journal_position(), 0);
    assert!(g.account().frozen());
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn learned_native_id_cannot_change_for_the_same_client_id() {
    let (_account_dir, command, mut g, _auth, peer) = gateway::make(false, 16384);
    let inbox_dir = gateway::Directory::new();
    let mut input =
        AccountIngress::create(FileStore::open(&inbox_dir.0).unwrap(), config(g.baseline()))
            .unwrap();
    input
        .submit(&mut g, command, &[[7; 16]], 60, gateway::until())
        .unwrap();
    let first = messages(false).remove(0);
    let changed = String::from_utf8(first.clone())
        .unwrap()
        .replace("\"i\":22", "\"i\":23");
    let mut outcomes = Vec::new();
    receive(vec![first, changed.into_bytes()], |d| {
        if let private::Delivery::Event(e) = d {
            outcomes.push(input.ingest(&mut g, &e).unwrap());
        }
        Ok(())
    });
    assert!(matches!(
        outcomes.as_slice(),
        [
            Outcome::Applied {
                applied: Applied::New,
                ..
            },
            Outcome::Quarantined {
                reason: QuarantineReason::BindingConflict,
                ..
            },
        ]
    ));
    assert_eq!(g.journal_position(), 2);
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn account_wal_cannot_pair_with_an_older_input_prefix() {
    let (_account_dir, command, mut g, _auth, peer) = gateway::make(false, 16384);
    let inbox_dir = gateway::Directory::new();
    let old_dir = gateway::Directory::new();
    let cfg = config(g.baseline());
    let mut input =
        AccountIngress::create(FileStore::open(&inbox_dir.0).unwrap(), cfg.clone()).unwrap();
    input
        .submit(&mut g, command, &[[7; 16]], 60, gateway::until())
        .unwrap();
    // Copy the complete synchronized input prefix before a later event exists.
    for entry in std::fs::read_dir(&inbox_dir.0).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() != "LOCK" {
            std::fs::copy(entry.path(), old_dir.0.join(entry.file_name())).unwrap();
        }
    }
    receive(messages(false), |d| {
        if let private::Delivery::Event(e) = d {
            input.ingest(&mut g, &e).unwrap();
        }
        Ok(())
    });
    let mut old = AccountIngress::recover(FileStore::open(&old_dir.0).unwrap(), cfg, None).unwrap();
    assert!(matches!(
        old.replay_pending(&mut g, 10),
        Err(Error::Identity)
    ));
    assert!(g.account().frozen());
    assert_eq!(g.journal_position(), 2);
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn input_sync_reply_loss_defers_account_write_until_recovery() {
    let (_account_dir, command, mut g, _auth, peer) = gateway::make(false, 16384);
    let inbox_dir = gateway::Directory::new();
    let cfg = config(g.baseline());
    let fault = Arc::new(AtomicU8::new(0));
    let mut input = AccountIngress::create(
        faults::FaultBackend {
            inner: FileBackend::open(&inbox_dir.0).unwrap(),
            next: fault.clone(),
        },
        cfg.clone(),
    )
    .unwrap();
    input
        .submit(&mut g, command, &[[7; 16]], 60, gateway::until())
        .unwrap();
    fault.store(faults::LOST_SYNC_REPLY, Ordering::SeqCst);
    receive(messages(false), |d| {
        if let private::Delivery::Event(e) = d {
            assert!(matches!(input.ingest(&mut g, &e), Err(Error::Journal(_))));
            return Err(private::Error::Sink);
        }
        Ok(())
    });
    assert!(input.stopped() && g.account().frozen());
    assert_eq!(input.position(), 2);
    assert_eq!(g.journal_position(), 1);
    drop(input);
    let mut input =
        AccountIngress::recover(FileStore::open(&inbox_dir.0).unwrap(), cfg, None).unwrap();
    assert_eq!(input.position(), 3);
    assert_eq!(input.replay_pending(&mut g, 10).unwrap(), 2);
    assert_eq!(g.journal_position(), 2);
    drop(g);
    assert!(peer.join().unwrap().is_empty());
}

#[test]
fn real_list_parent_and_late_child_replay_both_wals_without_duplicate_reservation() {
    use tickwire_venue as v;
    let (account_dir, command, mut g, auth, peer) = gateway::make(false, 16384);
    let mut command = command.command().clone();
    let v::CommandBody::New(first) = command.body.clone() else {
        unreachable!()
    };
    let mut second = first.clone();
    second.client_id = v::ClientId::new("pending").unwrap();
    command.body = v::CommandBody::SpotList(v::ListOrder {
        list_id: [70; 16],
        client_id: v::ClientId::new("list-parent").unwrap(),
        kind: v::ListKind::Oto,
        legs: v::BoundedList::new(vec![first, second]).unwrap(),
    });
    let command = v::CanonicalCommand::new(command).unwrap();
    let inbox_dir = gateway::Directory::new();
    let cfg = config(g.baseline());
    let mut input =
        AccountIngress::create(FileStore::open(&inbox_dir.0).unwrap(), cfg.clone()).unwrap();
    input
        .submit(&mut g, command, &[[7; 16], [8; 16]], 60, gateway::until())
        .unwrap();
    let before = g.account().asset(gateway::fixture::key(2)).unwrap();
    let parent=br#"{"subscriptionId":0,"event":{"e":"listStatus","E":10,"T":9,"s":"BTCUSDT","g":55,"c":"OTO","l":"ALL_DONE","L":"ALL_DONE","r":"NONE","C":"list-parent","O":[{"s":"BTCUSDT","i":21,"c":"test-1"},{"s":"BTCUSDT","i":22,"c":"pending"}]}}"#.to_vec();
    let fill = String::from_utf8(messages(false).remove(0))
        .unwrap()
        .replace("\"i\":22", "\"i\":21")
        .replace("\"g\":-1", "\"g\":55")
        .into_bytes();
    let mut outcomes = Vec::new();
    receive(vec![parent.clone(), parent, fill.clone(), fill], |d| {
        if let private::Delivery::Event(e) = d {
            outcomes.push(input.ingest(&mut g, &e).unwrap());
            if outcomes.len() <= 2 {
                assert_eq!(g.account().asset(gateway::fixture::key(2)).unwrap(), before);
            }
        }
        Ok(())
    });
    assert!(matches!(
        outcomes.as_slice(),
        [
            Outcome::Applied {
                applied: Applied::New,
                ..
            },
            Outcome::Applied {
                applied: Applied::Duplicate,
                ..
            },
            Outcome::Applied {
                applied: Applied::New,
                ..
            },
            Outcome::Applied {
                applied: Applied::Duplicate,
                ..
            }
        ]
    ));
    let balance = g.account().asset(gateway::fixture::key(2)).unwrap();
    assert_eq!(balance.reserved, gateway::fixture::units(60));
    let sequence = g.account().ledger_sequence();
    assert_eq!(sequence, 3);
    assert_eq!(input.position(), 6);
    assert_eq!(g.journal_position(), 5);
    assert!(g.account().structure(&[70; 16]).unwrap().terminal);
    drop(input);
    drop(g);
    assert!(peer.join().unwrap().is_empty());
    let mut gcfg = gateway::config(&cfg.baseline);
    gcfg.writer_epoch = 5;
    let mut g = AccountGateway::recover(
        FileBackend::open(&account_dir.0).unwrap(),
        gcfg,
        auth,
        gateway::TestEncoder,
        None,
    )
    .unwrap();
    let mut input =
        AccountIngress::recover(FileStore::open(&inbox_dir.0).unwrap(), cfg, None).unwrap();
    input.replay_pending(&mut g, 16).unwrap();
    assert_eq!(g.account().ledger_sequence(), sequence);
    assert_eq!(g.journal_position(), 5);
    assert_eq!(
        g.account().asset(gateway::fixture::key(2)).unwrap(),
        balance
    );
    assert!(g.account().frozen());
}

fn receive_usdm(
    messages: Vec<Vec<u8>>,
    mut sink: impl FnMut(private::Delivery<'_>) -> private::Result<()>,
) {
    use std::io::Write;
    let count = messages.len();
    let (tls, server) = network::certs();
    let management = network::listener();
    let stream = network::listener();
    let ma = management.local_addr().unwrap();
    let sa = stream.local_addr().unwrap();
    let management_server = server.clone();
    let mh = thread::spawn(move || {
        let mut socket = network::accept(&management, management_server);
        assert!(network::headers(&mut socket).starts_with("POST /fapi/v1/listenKey "));
        let body = r#"{"listenKey":"CONDITIONAL-TEST-KEY"}"#;
        write!(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
        socket.flush().unwrap();
    });
    let wh = thread::spawn(move || {
        let mut socket = network::accept(&stream, server);
        network::upgrade(&mut socket, "/private/ws/CONDITIONAL-TEST-KEY");
        for message in messages {
            network::frame(&mut socket, 1, &message);
        }
        let _ = socket.read(&mut [0]);
    });
    let mut client = private::PrivateClient::new(
        private::SessionConfig::new(network::scope(private::Market::Usdm)),
        private::Authentication::UsdmApiKey {
            api_key: network::KEY,
        },
        private::NetworkConfig {
            stream_address: sa,
            management_address: Some(ma),
            tls,
        },
        5,
    )
    .unwrap();
    let mut clock = network::TestClock {
        start: Instant::now(),
    };
    let mut permit = |_: &private::Charge, _: &private::ClockSample| true;
    let mut delivered = 0;
    let until = gateway::until();
    while delivered < count {
        assert!(Instant::now() < until);
        client
            .poll(
                &mut clock,
                private::PollBudget::default(),
                &mut permit,
                |d| {
                    if matches!(d, private::Delivery::Event(_)) {
                        delivered += 1;
                    }
                    sink(d)
                },
            )
            .unwrap();
        thread::yield_now();
    }
    client.stop(private::StopMode::Disconnect).unwrap();
    drop(client);
    mh.join().unwrap();
    wh.join().unwrap();
}

#[test]
fn real_usdm_child_before_parent_stays_quarantined_then_original_native_link_replays_once() {
    use tickwire_venue as v;
    let (account_dir, command, mut g, auth, peer) = gateway::make(true, 16384);
    let mut command = command.command().clone();
    let v::CommandBody::New(mut order) = command.body.clone() else {
        unreachable!()
    };
    order.order_type = v::OrderType::StopLimit;
    order.trigger = Some(v::Trigger {
        price: v::Decimal::new(110, 0).unwrap(),
        working_price: v::WorkingPrice::Contract,
        price_protect: false,
    });
    command.body = v::CommandBody::Algo(v::AlgoOrder {
        parent_order_id: [72; 16],
        order,
    });
    let command = v::CanonicalCommand::new(command).unwrap();
    let inbox_dir = gateway::Directory::new();
    let cfg = config(g.baseline());
    let mut input =
        AccountIngress::create(FileStore::open(&inbox_dir.0).unwrap(), cfg.clone()).unwrap();
    input
        .submit(&mut g, command, &[[7; 16]], 60, gateway::until())
        .unwrap();
    let key = g.account().admission(gateway::intent()).unwrap().orders()[0].clone();
    let parent=br#"{"e":"ALGO_UPDATE","E":10,"T":9,"o":{"s":"BTCUSDT","caid":"test-1","aid":55,"at":"CONDITIONAL","o":"STOP","S":"BUY","ps":"BOTH","f":"GTC","q":"1","X":"FINISHED","ai":"21","ap":"100","aq":"1","act":"LIMIT","tp":"110","p":"100","tt":9,"gtd":0,"rm":"","ia":false}}"#.to_vec();
    let fill=br#"{"e":"ORDER_TRADE_UPDATE","E":10,"T":9,"o":{"s":"BTCUSDT","c":"exchange-child-client","S":"BUY","o":"LIMIT","f":"GTC","q":"1","p":"100","x":"TRADE","X":"FILLED","i":21,"l":"1","z":"1","L":"100","n":"0","N":"USDT","T":9,"t":33,"m":false,"ps":"BOTH","rp":"0"}}"#.to_vec();
    let mut outcomes = Vec::new();
    receive_usdm(vec![fill.clone(), parent, fill.clone(), fill], |d| {
        if let private::Delivery::Event(e) = d {
            outcomes.push(input.ingest(&mut g, &e).unwrap());
            if outcomes.len() <= 2 {
                assert_eq!(
                    g.account().order(&key).unwrap().filled_quantity,
                    gateway::fixture::f(0)
                );
            }
        }
        Ok(())
    });
    assert!(matches!(
        outcomes.as_slice(),
        [
            Outcome::Quarantined { .. },
            Outcome::Applied {
                applied: Applied::New,
                ..
            },
            Outcome::Applied {
                applied: Applied::New,
                ..
            },
            Outcome::Applied {
                applied: Applied::Duplicate,
                ..
            }
        ]
    ));
    assert_eq!(
        g.account().order(&key).unwrap().filled_quantity,
        gateway::fixture::units(1)
    );
    assert_eq!(g.account().ledger_sequence(), 3);
    assert_eq!(g.journal_position(), 4);
    let balance = g.account().asset(gateway::fixture::key(2)).unwrap();
    drop(input);
    drop(g);
    assert!(peer.join().unwrap().is_empty());
    let mut gcfg = gateway::config(&cfg.baseline);
    gcfg.writer_epoch = 5;
    let mut g = AccountGateway::recover(
        FileBackend::open(&account_dir.0).unwrap(),
        gcfg,
        auth,
        gateway::TestEncoder,
        None,
    )
    .unwrap();
    let mut input =
        AccountIngress::recover(FileStore::open(&inbox_dir.0).unwrap(), cfg, None).unwrap();
    input.replay_pending(&mut g, 16).unwrap();
    assert_eq!(g.account().ledger_sequence(), 3);
    assert_eq!(g.journal_position(), 4);
    assert_eq!(
        g.account().asset(gateway::fixture::key(2)).unwrap(),
        balance
    );
    assert!(g.account().frozen());
}

struct ParentClock {
    started: Instant,
    market: private::Market,
}
impl private::Clock for ParentClock {
    fn sample(&mut self) -> private::Result<private::ClockSample> {
        let ns = self.started.elapsed().as_nanos() as u64;
        let domain = network::test_domain();
        let mono = tickwire_clock::MonoTime::new(domain, ns);
        let source = if self.market == private::Market::Spot {
            tickwire_clock::Source::SpotTest
        } else {
            tickwire_clock::Source::UsdmTest
        };
        let mut c = network::calibrated(domain, source, ns, 1_770_000_000_000 + ns / 1_000_000);
        Ok(private::ClockSample {
            instant: Instant::now(),
            monotonic: mono,
            signing_time: Some(
                c.sign_time(mono, tickwire_clock::TimeUnit::Milliseconds)
                    .unwrap(),
            ),
        })
    }
    fn check_signed(
        &mut self,
        s: &private::AccountScope,
        t: &private::SignedTime,
        n: &private::ClockSample,
        w: u64,
    ) -> private::Result<()> {
        network::check_test_stamp(s, t, n, w)
    }
}
#[test]
fn real_https_parent_queries_bind_original_identity_and_reopen_both_wals() {
    use std::io::Write;
    use tickwire_binance::request::Credentials;
    use tickwire_binance_account::{
        reconciliation::{
            ReadOperation, ReconcileConfig,
            structures::{ReadOnlyStructureReader, StructureQuery},
        },
        rest::{RestConfig, RestOwner},
    };
    use tickwire_transport::{IoBudget, Limits, rustls};
    use tickwire_venue as v;
    for futures in [false, true] {
        let (account_dir, original, mut g, auth, peer) = gateway::make(futures, 16384);
        let mut command = original.command().clone();
        let v::CommandBody::New(mut order) = command.body.clone() else {
            unreachable!()
        };
        let ids = if futures {
            order.order_type = v::OrderType::StopLimit;
            order.trigger = Some(v::Trigger {
                price: v::Decimal::new(110, 0).unwrap(),
                working_price: v::WorkingPrice::Contract,
                price_protect: false,
            });
            command.body = v::CommandBody::Algo(v::AlgoOrder {
                parent_order_id: [73; 16],
                order,
            });
            vec![[7; 16]]
        } else {
            let mut second = order.clone();
            second.client_id = v::ClientId::new("pending").unwrap();
            command.body = v::CommandBody::SpotList(v::ListOrder {
                list_id: [73; 16],
                client_id: v::ClientId::new("list-parent").unwrap(),
                kind: v::ListKind::Oto,
                legs: v::BoundedList::new(vec![order, second]).unwrap(),
            });
            vec![[7; 16], [8; 16]]
        };
        let command = v::CanonicalCommand::new(command).unwrap();
        let input_dir = gateway::Directory::new();
        let cfg = config(g.baseline());
        let mut input =
            AccountIngress::create(FileStore::open(&input_dir.0).unwrap(), cfg.clone()).unwrap();
        input
            .submit(&mut g, command, &ids, 60, gateway::until())
            .unwrap();
        let before = g.account().asset(gateway::fixture::key(2)).unwrap();
        let mut owner = RestOwner::from_parts(
            input,
            g,
            RestConfig {
                connection_epoch: 5,
                max_response_bytes: 32768,
                max_headers: 32,
            },
        )
        .unwrap();
        let cert = rcgen::generate_simple_self_signed(vec![
            "testnet.binance.vision".into(),
            "demo-fapi.binance.com".into(),
        ])
        .unwrap();
        let server = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![cert.cert.der().clone()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der())
                        .into(),
                )
                .unwrap(),
        );
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let tls = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let listener = network::listener();
        let address = listener.local_addr().unwrap();
        let remote = thread::spawn(move || {
            for iteration in 0..2 {
                let mut socket = network::accept(&listener, server.clone());
                let request = network::headers(&mut socket);
                let path = request
                    .lines()
                    .next()
                    .unwrap()
                    .strip_prefix("GET ")
                    .unwrap()
                    .strip_suffix(" HTTP/1.1")
                    .unwrap();
                assert!(path.starts_with(if futures {
                    "/fapi/v1/algoOrder?"
                } else {
                    "/api/v3/orderList?"
                }));
                let (_, params) = path.split_once('?').unwrap();
                let (preimage, signature) = params.rsplit_once("&signature=").unwrap();
                let signed = ring::hmac::sign(
                    &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, network::SECRET),
                    preimage.as_bytes(),
                );
                let expected = signed
                    .as_ref()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>();
                assert_eq!(signature, expected);
                assert!(preimage.contains(match (futures, iteration) {
                    (false, 0) => "origClientOrderId=list-parent",
                    (false, _) => "orderListId=55",
                    (true, 0) => "clientAlgoId=test-1",
                    (true, _) => "algoId=55",
                }));
                let body = if futures {
                    r#"{"algoId":55,"clientAlgoId":"test-1","algoType":"CONDITIONAL","symbol":"BTCUSDT","orderType":"STOP","side":"BUY","positionSide":"BOTH","timeInForce":"GTC","quantity":"1","price":"100","triggerPrice":"110","algoStatus":"CANCELED","actualOrderId":"","actualPrice":"0","actualQty":"0","actualType":"","triggerTime":0}"#
                } else {
                    r#"{"orderListId":55,"contingencyType":"OTO","listStatusType":"ALL_DONE","listOrderStatus":"ALL_DONE","listClientOrderId":"list-parent","symbol":"BTCUSDT","orders":[{"symbol":"BTCUSDT","orderId":21,"clientOrderId":"test-1"},{"symbol":"BTCUSDT","orderId":22,"clientOrderId":"pending"}]}"#
                };
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .unwrap();
                socket.flush().unwrap();
            }
        });
        let market = if futures {
            private::Market::Usdm
        } else {
            private::Market::Spot
        };
        let mut reader = ReadOnlyStructureReader::new(ReconcileConfig {
            scope: network::scope(market),
            address,
            tls,
            limits: Limits::default(),
            first_connection_epoch: 7,
            request_timeout_ns: 5_000_000_000,
            recv_window_micros: 5_000_000,
            max_response_bytes: 32768,
            max_headers: 32,
        })
        .unwrap();
        let signing = private::SigningKey::hmac(network::SECRET).unwrap();
        let credentials = Credentials {
            market,
            environment: v::Environment::Test,
            account: 2,
            api_key: network::KEY,
            signing_key: &signing,
        };
        let mut clock = ParentClock {
            started: Instant::now(),
            market,
        };
        let query = StructureQuery {
            intent: gateway::intent(),
            structure_id: [73; 16],
            private_connection_epoch: 5,
            gap_generation: 1,
        };
        let mut permissions = Vec::new();
        for _ in 0..2 {
            let mut permit = |_: &private::AccountScope,
                              q: &StructureQuery,
                              epoch: u64,
                              operation: ReadOperation,
                              _: &private::ClockSample| {
                assert_eq!(*q, query);
                permissions.push((epoch, operation));
                true
            };
            reader
                .start(&mut owner, query, &credentials, &mut clock, &mut permit)
                .unwrap();
            let deadline = gateway::until();
            loop {
                assert!(Instant::now() < deadline);
                if let Some(result) = reader
                    .poll(&mut owner, &mut clock, &mut permit, IoBudget::default())
                    .unwrap()
                {
                    assert!(result.accepted);
                    break;
                }
                thread::yield_now();
            }
        }
        remote.join().unwrap();
        assert!(
            permissions.contains(&(7, ReadOperation::Connect))
                && permissions.contains(&(8, ReadOperation::TradePage))
        );
        assert_eq!(
            owner.account().asset(gateway::fixture::key(2)).unwrap(),
            before
        );
        assert!(owner.account().structure(&[73; 16]).unwrap().terminal);
        assert!(
            owner
                .account()
                .structure(&[73; 16])
                .unwrap()
                .legs
                .iter()
                .all(|k| !owner.account().order(k).unwrap().terminal)
        );
        assert_eq!(owner.account().ledger_sequence(), 2);
        assert_eq!(
            owner
                .ingress()
                .stored_rest()
                .filter(|r| r.structure_query == Some(query))
                .count(),
            2
        );
        drop(owner);
        assert!(peer.join().unwrap().is_empty());
        let mut next = gateway::config(&cfg.baseline);
        next.writer_epoch = 5;
        let mut gateway = AccountGateway::recover(
            FileBackend::open(&account_dir.0).unwrap(),
            next,
            auth,
            gateway::TestEncoder,
            None,
        )
        .unwrap();
        let mut input =
            AccountIngress::recover(FileStore::open(&input_dir.0).unwrap(), cfg, None).unwrap();
        input.replay_pending(&mut gateway, 32).unwrap();
        assert_eq!(gateway.account().ledger_sequence(), 2);
        assert_eq!(
            gateway.account().asset(gateway::fixture::key(2)).unwrap(),
            before
        );
        assert!(gateway.account().frozen());
    }
}

#[test]
fn real_https_never_created_algo_closes_with_fresh_snapshots_and_replays_exact_objects() {
    use std::io::Write;
    use tickwire_binance::request::Credentials;
    use tickwire_binance_account::{
        reconciliation::{
            ReadOperation, ReconcileConfig,
            activation::ActivationCut,
            bootstrap::{AccountSnapshotReader, SnapshotKind, SnapshotQuery},
            structure_closure,
            structures::{ReadOnlyStructureReader, StructureQuery},
        },
        rest::{RestConfig, RestOwner},
    };
    use tickwire_recovery::{FileBlobStore, SharedBlobStore};
    use tickwire_transport::{IoBudget, Limits, rustls};
    use tickwire_venue as v;
    let (account_dir, original, mut g, auth, peer) = gateway::make(true, 16384);
    let mut c = original.command().clone();
    let v::CommandBody::New(mut order) = c.body.clone() else {
        unreachable!()
    };
    order.order_type = v::OrderType::StopLimit;
    order.trigger = Some(v::Trigger {
        price: v::Decimal::new(110, 0).unwrap(),
        working_price: v::WorkingPrice::Contract,
        price_protect: false,
    });
    c.body = v::CommandBody::Algo(v::AlgoOrder {
        parent_order_id: [73; 16],
        order,
    });
    let c = v::CanonicalCommand::new(c).unwrap();
    let input_dir = gateway::Directory::new();
    let objects_dir = gateway::Directory::new();
    let cfg = config(g.baseline());
    let objects = SharedBlobStore::new(FileBlobStore::open(&objects_dir.0).unwrap());
    let mut input = AccountIngress::create(FileStore::open(&input_dir.0).unwrap(), cfg.clone())
        .unwrap()
        .with_closure_store(objects.clone());
    input
        .submit(&mut g, c, &[[7; 16]], 60, gateway::until())
        .unwrap();
    gateway::drive(&mut g, tickwire_account_gateway::Status::Flushed);
    let mut owner = RestOwner::from_parts(
        input,
        g,
        RestConfig {
            connection_epoch: 5,
            max_response_bytes: 32768,
            max_headers: 32,
        },
    )
    .unwrap();
    let cert = rcgen::generate_simple_self_signed(vec!["demo-fapi.binance.com".into()]).unwrap();
    let server = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der())
                    .into(),
            )
            .unwrap(),
    );
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let tls = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let listener = network::listener();
    let address = listener.local_addr().unwrap();
    let remote = thread::spawn(move || {
        let assets:Vec<_>=[("BTC","1"),("USDT","1000")].into_iter().map(|(asset,n)|serde_json::json!({"asset":asset,"walletBalance":n,"availableBalance":n,"marginBalance":n,"unrealizedProfit":"0","maintMargin":"0","initialMargin":"0","positionInitialMargin":"0","openOrderInitialMargin":"0","crossWalletBalance":n,"crossUnPnl":"0"})).collect();
        let account=serde_json::to_string(&serde_json::json!({"totalInitialMargin":"0","totalMaintMargin":"0","totalUnrealizedProfit":"0","totalPositionInitialMargin":"0","totalOpenOrderInitialMargin":"0","totalCrossUnPnl":"0","assets":assets,"positions":[]})).unwrap();
        for i in 0..7 {
            let mut socket = network::accept(&listener, server.clone());
            let request = network::headers(&mut socket);
            let path = request
                .lines()
                .next()
                .unwrap()
                .strip_prefix("GET ")
                .unwrap()
                .strip_suffix(" HTTP/1.1")
                .unwrap();
            let expected = match i {
                0 => "/fapi/v1/algoOrder?",
                1 | 4 => "/fapi/v3/account?",
                2 | 5 => "/fapi/v1/openOrders?",
                _ => "/fapi/v1/openAlgoOrders?",
            };
            assert!(path.starts_with(expected), "{path}");
            let (_, params) = path.split_once('?').unwrap();
            let (preimage, signature) = params.rsplit_once("&signature=").unwrap();
            let signed = ring::hmac::sign(
                &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, network::SECRET),
                preimage.as_bytes(),
            );
            assert_eq!(
                signature,
                signed
                    .as_ref()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            );
            let body = match i {
                0 => {
                    r#"{"algoId":55,"clientAlgoId":"test-1","algoType":"CONDITIONAL","symbol":"BTCUSDT","orderType":"STOP","side":"BUY","positionSide":"BOTH","timeInForce":"GTC","quantity":"1","price":"100","triggerPrice":"110","algoStatus":"CANCELED","actualOrderId":"","actualPrice":"0","actualQty":"0","actualType":"","triggerTime":0}"#
                }
                1 | 4 => &account,
                _ => "[]",
            };
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
            socket.flush().unwrap();
        }
    });
    let market = private::Market::Usdm;
    let scope = network::scope(market);
    let mut clock = ParentClock {
        started: Instant::now(),
        market,
    };
    let signing = private::SigningKey::hmac(network::SECRET).unwrap();
    // Create a genuine private-owner generation; quota denies its socket opening.
    // The reconciliation read channel below is the only fixture network traffic.
    let mut private = private::PrivateClient::new(
        private::SessionConfig::new(scope.clone()),
        private::Authentication::UsdmApiKey {
            api_key: network::KEY,
        },
        private::NetworkConfig {
            stream_address: "127.0.0.1:1".parse().unwrap(),
            management_address: Some("127.0.0.1:1".parse().unwrap()),
            tls: tls.clone(),
        },
        5,
    )
    .unwrap();
    private
        .poll(
            &mut clock,
            private::PollBudget::default(),
            &mut |_: &private::Charge, _: &private::ClockSample| false,
            |_| Ok(()),
        )
        .unwrap();
    let gap = private.current_gap().clone();
    assert_eq!(gap.connection_epoch(), 5);
    owner.begin_reconciliation(&gap).unwrap();
    let rc = ReconcileConfig {
        scope: scope.clone(),
        address,
        tls,
        limits: Limits::default(),
        first_connection_epoch: 7,
        request_timeout_ns: 5_000_000_000,
        recv_window_micros: 5_000_000,
        max_response_bytes: 32768,
        max_headers: 32,
    };
    let credentials = Credentials {
        market,
        environment: v::Environment::Test,
        account: 2,
        api_key: network::KEY,
        signing_key: &signing,
    };
    let query = StructureQuery {
        intent: gateway::intent(),
        structure_id: [73; 16],
        private_connection_epoch: gap.connection_epoch(),
        gap_generation: gap.generation(),
    };
    let mut reader = ReadOnlyStructureReader::new(rc.clone()).unwrap();
    let mut permit = |_: &private::AccountScope,
                      _: &StructureQuery,
                      _: u64,
                      _: ReadOperation,
                      _: &private::ClockSample| true;
    reader
        .start(&mut owner, query, &credentials, &mut clock, &mut permit)
        .unwrap();
    let deadline = gateway::until();
    let parent = loop {
        assert!(Instant::now() < deadline);
        if let Some(result) = reader
            .poll(&mut owner, &mut clock, &mut permit, IoBudget::default())
            .unwrap()
        {
            assert!(result.accepted);
            break result.input.sequence;
        }
        thread::yield_now()
    };
    let mut snapshots = AccountSnapshotReader::new(ReconcileConfig {
        first_connection_epoch: 20,
        ..rc
    })
    .unwrap();
    for round in 0..2 {
        let mut evidence = Vec::new();
        for kind in [
            SnapshotKind::Account,
            SnapshotKind::OpenOrders,
            SnapshotKind::ConditionalOrders,
        ] {
            let q = SnapshotQuery {
                kind,
                private_connection_epoch: gap.connection_epoch(),
                gap_generation: gap.generation(),
            };
            let mut permit = |_: &private::AccountScope,
                              _: SnapshotQuery,
                              _: u64,
                              _: ReadOperation,
                              _: &private::ClockSample| true;
            snapshots
                .start(&mut owner, q, &credentials, &mut clock, &mut permit)
                .unwrap();
            let deadline = gateway::until();
            loop {
                assert!(Instant::now() < deadline);
                if let Some(result) = snapshots
                    .poll(&mut owner, &mut clock, &mut permit, IoBudget::default())
                    .unwrap()
                {
                    evidence.push(result.evidence.unwrap());
                    break;
                }
                thread::yield_now()
            }
        }
        let now = private::Clock::sample(&mut clock).unwrap().monotonic;
        let cut = ActivationCut {
            private_connection_epoch: gap.connection_epoch(),
            gap_generation: gap.generation(),
            through_sequence: owner.applied_position(),
            now,
            valid_until: tickwire_clock::MonoTime::new(now.domain(), now.nanos() + 5_000_000_000),
            writer_epoch: 4,
            execution_connection_epoch: 5,
        };
        if round == 0 {
            let before = owner.account().asset(gateway::fixture::key(2)).unwrap();
            let shadow = owner.account().venue_shadow(1).unwrap();
            owner
                .close_uncreated_algo(structure_closure::ClosureRequest {
                    parent_query: parent,
                    snapshots: std::array::from_fn(|i| evidence[i].input().sequence),
                    cut,
                })
                .unwrap();
            assert!(owner.account().frozen());
            assert_eq!(
                owner
                    .account()
                    .asset(gateway::fixture::key(2))
                    .unwrap()
                    .total,
                before.total
            );
            assert_eq!(owner.account().venue_shadow(1).unwrap(), shadow);
            assert!(
                owner
                    .reactivate(
                        evidence
                            .try_into()
                            .unwrap_or_else(|_| panic!("three snapshots")),
                        cut,
                        None
                    )
                    .is_err(),
                "old ledger snapshots cannot authorize the new tag16 ledger"
            );
        } else {
            owner
                .reactivate(
                    evidence
                        .try_into()
                        .unwrap_or_else(|_| panic!("three snapshots")),
                    cut,
                    None,
                )
                .unwrap();
            assert!(!owner.account().frozen());
        }
    }
    remote.join().unwrap();
    let ledger = owner.account().ledger_sequence();
    assert_eq!(ledger, 4);
    drop(owner);
    assert!(!peer.join().unwrap().is_empty());
    let mut cfg = cfg;
    cfg.writer_epoch = 5;
    let mut input =
        AccountIngress::recover(FileStore::open(&input_dir.0).unwrap(), cfg.clone(), None)
            .unwrap()
            .with_closure_store(objects);
    let mut next = gateway::config(&cfg.baseline);
    next.writer_epoch = 5;
    let mut recovered = AccountGateway::recover_with_reactivation(
        FileBackend::open(&account_dir.0).unwrap(),
        next,
        auth,
        gateway::TestEncoder,
        None,
        &mut input.activation_verifier(),
    )
    .unwrap();
    input.replay_pending(&mut recovered, 64).unwrap();
    assert!(recovered.account().frozen());
    assert_eq!(recovered.account().ledger_sequence(), ledger);
    assert!(
        recovered
            .account()
            .order(&recovered.account().structure(&[73; 16]).unwrap().legs[0])
            .unwrap()
            .uncreated
    );
}

#[test]
fn signed_rest_list_and_algo_cancels_use_durable_management_and_never_release_on_ack() {
    use std::io::Write;
    use tickwire_binance::request::{Credentials, Timestamp};
    use tickwire_binance_account::rest::{RestConfig, RestDisposition, RestOwner};
    use tickwire_gateway::binance::RestEncoder;
    use tickwire_transport::{IoBudget, Limits, rustls};
    use tickwire_venue as v;
    for futures in [false, true] {
        let (baseline, original) = gateway::baseline(futures);
        let mut command = original.command().clone();
        let v::CommandBody::New(mut order) = command.body.clone() else {
            unreachable!()
        };
        let ids = if futures {
            order.order_type = v::OrderType::StopLimit;
            order.trigger = Some(v::Trigger {
                price: v::Decimal::new(110, 0).unwrap(),
                working_price: v::WorkingPrice::Contract,
                price_protect: false,
            });
            command.body = v::CommandBody::Algo(v::AlgoOrder {
                parent_order_id: [73; 16],
                order,
            });
            vec![[7; 16]]
        } else {
            let mut second = order.clone();
            second.client_id = v::ClientId::new("pending").unwrap();
            command.body = v::CommandBody::SpotList(v::ListOrder {
                list_id: [73; 16],
                client_id: v::ClientId::new("list-parent").unwrap(),
                kind: v::ListKind::Oto,
                legs: v::BoundedList::new(vec![order, second]).unwrap(),
            });
            vec![[7; 16], [8; 16]]
        };
        let original = v::CanonicalCommand::new(command.clone()).unwrap();
        command.context.request.intent_id += 1;
        command.context.request.recovery_object += 1;
        command.body = if futures {
            v::CommandBody::CancelAlgo(v::AlgoTarget {
                parent_order_id: [73; 16],
                expected_owner_revision: 1,
                original_intent_exec_epoch: 4,
                client_id: Some(v::ClientId::new("test-1").unwrap()),
                venue_algo_id: Some(v::VenueOrderId::new("55").unwrap()),
            })
        } else {
            v::CommandBody::CancelList(v::ListTarget {
                list_id: [73; 16],
                expected_owner_revision: 1,
                original_intent_exec_epoch: 4,
                client_id: Some(v::ClientId::new("list-parent").unwrap()),
                venue_list_id: Some(v::VenueOrderId::new("55").unwrap()),
            })
        };
        let cancel = v::CanonicalCommand::new(command).unwrap();
        let host = if futures {
            "demo-fapi.binance.com"
        } else {
            "testnet.binance.vision"
        };
        let cert = rcgen::generate_simple_self_signed(vec![host.into()]).unwrap();
        let server = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![cert.cert.der().clone()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der())
                        .into(),
                )
                .unwrap(),
        );
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let tls = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let listener = network::listener();
        let address = listener.local_addr().unwrap();
        let remote = thread::spawn(move || {
            let mut socket = network::accept(&listener, server);
            for n in 0..2 {
                let h = network::headers(&mut socket);
                let path = match (futures, n) {
                    (false, 0) => "POST /api/v3/orderList/oto?",
                    (false, _) => "DELETE /api/v3/orderList?",
                    (true, 0) => "POST /fapi/v1/algoOrder?",
                    _ => "DELETE /fapi/v1/algoOrder?",
                };
                assert!(h.starts_with(path), "{h}");
                let len: usize = h
                    .lines()
                    .find_map(|l| {
                        l.split_once(':')
                            .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                            .map(|(_, v)| v.trim().parse().unwrap())
                    })
                    .unwrap();
                let mut body = vec![0; len];
                socket.read_exact(&mut body).unwrap();
                let text = h
                    .lines()
                    .next()
                    .unwrap()
                    .split_once('?')
                    .unwrap()
                    .1
                    .strip_suffix(" HTTP/1.1")
                    .unwrap();
                assert_eq!(body.len(), 0);
                let (preimage, sig) = text.rsplit_once("&signature=").unwrap();
                let signed = ring::hmac::sign(
                    &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, network::SECRET),
                    preimage.as_bytes(),
                );
                assert_eq!(
                    sig,
                    signed
                        .as_ref()
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                );
                if n == 1 {
                    assert!(preimage.contains(if futures {
                        "algoId=55"
                    } else {
                        "orderListId=55"
                    }));
                    if futures {
                        assert!(!preimage.contains("symbol="))
                    }
                }
                let body = if futures {
                    if n == 0 {
                        r#"{"algoId":55,"clientAlgoId":"test-1","algoType":"CONDITIONAL","symbol":"BTCUSDT","orderType":"STOP","side":"BUY","positionSide":"BOTH","timeInForce":"GTC","quantity":"1","price":"100","triggerPrice":"110","algoStatus":"NEW","actualOrderId":"","actualPrice":"0","actualQty":"0","actualType":"","triggerTime":0}"#.to_owned()
                    } else {
                        r#"{"algoId":55,"code":200,"msg":"success"}"#.to_owned()
                    }
                } else {
                    format!(
                        r#"{{"orderListId":55,"contingencyType":"OTO","listStatusType":"{}","listOrderStatus":"{}","listClientOrderId":"list-parent","symbol":"BTCUSDT","orders":[{{"symbol":"BTCUSDT","orderId":21,"clientOrderId":"test-1"}},{{"symbol":"BTCUSDT","orderId":22,"clientOrderId":"pending"}}]}}"#,
                        if n == 0 { "EXEC_STARTED" } else { "ALL_DONE" },
                        if n == 0 { "EXECUTING" } else { "ALL_DONE" }
                    )
                };
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                )
                .unwrap();
                socket.flush().unwrap();
            }
        });
        let key = private::SigningKey::hmac(network::SECRET).unwrap();
        let market = if futures {
            private::Market::Usdm
        } else {
            private::Market::Spot
        };
        let encoder = RestEncoder::new(
            Credentials {
                account: 2,
                environment: v::Environment::Test,
                market,
                api_key: network::KEY,
                signing_key: &key,
            },
            || Ok(Timestamp::Milliseconds(1_770_000_000_000)),
            5_000_000,
            16384,
        )
        .unwrap();
        let account_dir = gateway::Directory::new();
        let input_dir = gateway::Directory::new();
        let cfg = gateway::config(&baseline);
        let g = AccountGateway::create(
            FileBackend::open(&account_dir.0).unwrap(),
            cfg,
            baseline.clone(),
            gateway::MockAuthority::default(),
            encoder,
            tickwire_account_gateway::ConnectionConfig {
                address,
                tls,
                limits: Limits::default(),
                connect_deadline: gateway::until(),
            },
        )
        .unwrap();
        let input =
            AccountIngress::create(FileStore::open(&input_dir.0).unwrap(), config(&baseline))
                .unwrap();
        let mut owner = RestOwner::from_parts(
            input,
            g,
            RestConfig {
                connection_epoch: 5,
                max_response_bytes: 32768,
                max_headers: 32,
            },
        )
        .unwrap();
        let mut before = None;
        for (n, c) in [original, cancel].into_iter().enumerate() {
            owner
                .submit(
                    c,
                    if n == 0 { &ids } else { &[] },
                    60 + n as u64,
                    gateway::until(),
                )
                .unwrap();
            let deadline = gateway::until();
            loop {
                assert!(Instant::now() < deadline);
                if let Some(result) = owner
                    .poll(Instant::now(), 70 + n as u64, IoBudget::default())
                    .unwrap()
                {
                    if futures && n == 1 {
                        assert_eq!(result.disposition, RestDisposition::Unknown)
                    } else {
                        assert_eq!(result.disposition, RestDisposition::Observed)
                    }
                    break;
                }
                thread::yield_now()
            }
            let state = owner.account().structure(&[73; 16]).unwrap();
            let quantity: Vec<_> = state
                .legs
                .iter()
                .map(|k| owner.account().order(k).unwrap().remaining_quantity)
                .collect();
            if n == 0 {
                before = Some(quantity)
            } else {
                assert_eq!(Some(quantity), before);
                assert!(
                    state
                        .legs
                        .iter()
                        .all(|k| !owner.account().order(k).unwrap().terminal)
                );
            }
        }
        assert_eq!(
            owner
                .gateway()
                .journal_records_after(0, 16)
                .unwrap()
                .filter(|(_, r)| matches!(
                    r.record(),
                    tickwire_account::RecoveryRecord::Admit { .. }
                ))
                .count(),
            2
        );
        drop(owner);
        remote.join().unwrap();
    }
}

#[test]
fn real_https_created_children_reconcile_each_trade_fee_and_reopen_both_wals() {
    use std::io::Write;
    use tickwire_binance::request::Credentials;
    use tickwire_binance_account::{
        reconciliation::{
            ReadOnlyReconciler, ReadOperation, ReconcileConfig, TradeQuery,
            activation::ActivationCut,
            bootstrap::{AccountSnapshotReader, SnapshotKind, SnapshotQuery},
            orders::{OrderQuery, ReadOnlyOrderReader},
            structures::{ReadOnlyStructureReader, StructureQuery},
        },
        rest::{RestConfig, RestOwner},
    };
    use tickwire_transport::{IoBudget, Limits, rustls};
    use tickwire_venue as v;
    for futures in [false, true] {
        let (account_dir, original, mut g, auth, peer) = gateway::make(futures, 16384);
        let mut command = original.command().clone();
        let v::CommandBody::New(mut order) = command.body.clone() else {
            unreachable!()
        };
        let ids = if futures {
            order.order_type = v::OrderType::StopLimit;
            order.trigger = Some(v::Trigger {
                price: v::Decimal::new(110, 0).unwrap(),
                working_price: v::WorkingPrice::Contract,
                price_protect: false,
            });
            command.body = v::CommandBody::Algo(v::AlgoOrder {
                parent_order_id: [73; 16],
                order,
            });
            vec![[7; 16]]
        } else {
            let mut second = order.clone();
            second.client_id = v::ClientId::new("pending").unwrap();
            command.body = v::CommandBody::SpotList(v::ListOrder {
                list_id: [73; 16],
                client_id: v::ClientId::new("list-parent").unwrap(),
                kind: v::ListKind::Oto,
                legs: v::BoundedList::new(vec![order, second]).unwrap(),
            });
            vec![[7; 16], [8; 16]]
        };
        let command = v::CanonicalCommand::new(command).unwrap();
        let input_dir = gateway::Directory::new();
        let cfg = config(g.baseline());
        let mut input =
            AccountIngress::create(FileStore::open(&input_dir.0).unwrap(), cfg.clone()).unwrap();
        input
            .submit(&mut g, command, &ids, 60, gateway::until())
            .unwrap();
        gateway::drive(&mut g, tickwire_account_gateway::Status::Flushed);
        let mut owner = RestOwner::from_parts(
            input,
            g,
            RestConfig {
                connection_epoch: 5,
                max_response_bytes: 32768,
                max_headers: 32,
            },
        )
        .unwrap();
        let market = if futures {
            private::Market::Usdm
        } else {
            private::Market::Spot
        };
        let host = if futures {
            "demo-fapi.binance.com"
        } else {
            "testnet.binance.vision"
        };
        let cert = rcgen::generate_simple_self_signed(vec![host.into()]).unwrap();
        let server = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![cert.cert.der().clone()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der())
                        .into(),
                )
                .unwrap(),
        );
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let tls = Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let listener = network::listener();
        let address = listener.local_addr().unwrap();
        let remote = thread::spawn(move || {
            let parent = if futures {
                r#"{"algoId":55,"clientAlgoId":"test-1","algoType":"CONDITIONAL","symbol":"BTCUSDT","orderType":"STOP","side":"BUY","positionSide":"BOTH","timeInForce":"GTC","quantity":"1","price":"100","triggerPrice":"110","algoStatus":"FINISHED","actualOrderId":"21","actualPrice":"100","actualQty":"1","actualType":"LIMIT","triggerTime":20}"#.to_owned()
            } else {
                r#"{"orderListId":55,"contingencyType":"OTO","listStatusType":"ALL_DONE","listOrderStatus":"ALL_DONE","listClientOrderId":"list-parent","symbol":"BTCUSDT","orders":[{"symbol":"BTCUSDT","orderId":21,"clientOrderId":"test-1"},{"symbol":"BTCUSDT","orderId":22,"clientOrderId":"pending"}]}"#.to_owned()
            };
            let mut responses = vec![(
                if futures {
                    "/fapi/v1/algoOrder?"
                } else {
                    "/api/v3/orderList?"
                },
                None,
                parent,
            )];
            for i in 0..if futures { 1 } else { 2 } {
                let native = 21 + i;
                let client = if futures {
                    "actual-child"
                } else if i == 0 {
                    "test-1"
                } else {
                    "pending"
                };
                let qty = if futures { "1" } else { "0.001" };
                let price = if futures { "100" } else { "60000" };
                let quote = if futures { "100" } else { "60" };
                let mut row = serde_json::json!({"symbol":"BTCUSDT","orderId":native,"clientOrderId":client,"status":"FILLED","origQty":qty,"executedQty":qty,"price":price,"side":"BUY","positionSide":"BOTH","type":"LIMIT","timeInForce":"GTC","cummulativeQuoteQty":quote});
                if !futures {
                    row["orderListId"] = serde_json::json!(55)
                } else {
                    row.as_object_mut().unwrap().remove("cummulativeQuoteQty");
                    row["cumQuote"] = serde_json::json!(quote);
                }
                responses.push((
                    if futures {
                        "/fapi/v1/order?"
                    } else {
                        "/api/v3/order?"
                    },
                    Some(native),
                    row.to_string(),
                ));
                let mut trade = serde_json::json!({"symbol":"BTCUSDT","id":100+i,"orderId":native,"price":price,"qty":qty,"quoteQty":quote,"commission":"0","commissionAsset":"USDT","isBuyer":true,"isMaker":false,"time":100});
                if futures {
                    trade["side"] = serde_json::json!("BUY");
                    trade["positionSide"] = serde_json::json!("BOTH");
                    trade["realizedPnl"] = serde_json::json!("0")
                }
                // Replay the same complete page at a second actual HTTP request.
                for _ in 0..2 {
                    responses.push((
                        if futures {
                            "/fapi/v1/userTrades?"
                        } else {
                            "/api/v3/myTrades?"
                        },
                        Some(native),
                        format!("[{trade}]"),
                    ));
                }
            }
            let account = if futures {
                let assets:Vec<_>=[("BTC","1","1"),("USDT","1000","990")].into_iter().map(|(asset,wallet,available)|serde_json::json!({"asset":asset,"walletBalance":wallet,"crossWalletBalance":wallet,"marginBalance":wallet,"availableBalance":available,"unrealizedProfit":"0","maintMargin":"0","initialMargin":"0","positionInitialMargin":"0","openOrderInitialMargin":"0","crossUnPnl":"0"})).collect();
                serde_json::json!({"totalInitialMargin":"10","totalMaintMargin":"0","totalUnrealizedProfit":"0","totalPositionInitialMargin":"10","totalOpenOrderInitialMargin":"0","totalCrossUnPnl":"0","assets":assets,"positions":[{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"1","entryPrice":"100","unrealizedProfit":"0","isolatedMargin":"0","isolatedWallet":"0","notional":"100","initialMargin":"10","maintMargin":"0"}]}).to_string()
            } else {
                serde_json::json!({"canTrade":true,"accountType":"SPOT","balances":[{"asset":"BTC","free":"1.002","locked":"0"},{"asset":"USDT","free":"880","locked":"0"}]}).to_string()
            };
            responses.extend([
                (
                    if futures {
                        "/fapi/v3/account?"
                    } else {
                        "/api/v3/account?"
                    },
                    None,
                    account,
                ),
                (
                    if futures {
                        "/fapi/v1/openOrders?"
                    } else {
                        "/api/v3/openOrders?"
                    },
                    None,
                    "[]".into(),
                ),
                (
                    if futures {
                        "/fapi/v1/openAlgoOrders?"
                    } else {
                        "/api/v3/openOrderList?"
                    },
                    None,
                    "[]".into(),
                ),
            ]);
            for (expected, native, body) in responses {
                let mut socket = network::accept(&listener, server.clone());
                let request = network::headers(&mut socket);
                let path = request
                    .lines()
                    .next()
                    .unwrap()
                    .strip_prefix("GET ")
                    .unwrap()
                    .strip_suffix(" HTTP/1.1")
                    .unwrap();
                assert!(path.starts_with(expected), "{path}");
                let (_, params) = path.split_once('?').unwrap();
                let (preimage, sig) = params.rsplit_once("&signature=").unwrap();
                let signed = ring::hmac::sign(
                    &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, network::SECRET),
                    preimage.as_bytes(),
                );
                assert_eq!(
                    sig,
                    signed
                        .as_ref()
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                );
                if let Some(n) = native {
                    assert!(preimage.contains(&format!("orderId={n}")), "{preimage}")
                };
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .unwrap();
                socket.flush().unwrap();
            }
        });
        let scope = network::scope(market);
        let mut clock = ParentClock {
            started: Instant::now(),
            market,
        };
        let signing = private::SigningKey::hmac(network::SECRET).unwrap();
        let authentication = if futures {
            private::Authentication::UsdmApiKey {
                api_key: network::KEY,
            }
        } else {
            private::Authentication::SpotSignature {
                api_key: network::KEY,
                signing_key: &signing,
            }
        };
        let mut private = private::PrivateClient::new(
            private::SessionConfig::new(scope.clone()),
            authentication,
            private::NetworkConfig {
                stream_address: "127.0.0.1:1".parse().unwrap(),
                management_address: Some("127.0.0.1:1".parse().unwrap()),
                tls: tls.clone(),
            },
            5,
        )
        .unwrap();
        private
            .poll(
                &mut clock,
                private::PollBudget::default(),
                &mut |_: &private::Charge, _: &private::ClockSample| false,
                |_| Ok(()),
            )
            .unwrap();
        let gap = private.current_gap().clone();
        owner.begin_reconciliation(&gap).unwrap();
        let rc = ReconcileConfig {
            scope,
            address,
            tls,
            limits: Limits::default(),
            first_connection_epoch: 7,
            request_timeout_ns: 5_000_000_000,
            recv_window_micros: 5_000_000,
            max_response_bytes: 32768,
            max_headers: 32,
        };
        let credentials = Credentials {
            market,
            environment: v::Environment::Test,
            account: 2,
            api_key: network::KEY,
            signing_key: &signing,
        };
        let mut parent = ReadOnlyStructureReader::new(rc.clone()).unwrap();
        let q = StructureQuery {
            intent: gateway::intent(),
            structure_id: [73; 16],
            private_connection_epoch: gap.connection_epoch(),
            gap_generation: gap.generation(),
        };
        let mut parent_permit = |_: &private::AccountScope,
                                 _: &StructureQuery,
                                 _: u64,
                                 _: ReadOperation,
                                 _: &private::ClockSample| true;
        parent
            .start(&mut owner, q, &credentials, &mut clock, &mut parent_permit)
            .unwrap();
        let deadline = gateway::until();
        loop {
            assert!(Instant::now() < deadline);
            if let Some(result) = parent
                .poll(
                    &mut owner,
                    &mut clock,
                    &mut parent_permit,
                    IoBudget::default(),
                )
                .unwrap()
            {
                assert!(result.accepted);
                break;
            }
            thread::yield_now()
        }
        let state = owner.account().structure(&[73; 16]).unwrap();
        assert!(state.terminal);
        assert!(
            state
                .legs
                .iter()
                .all(|k| !owner.account().order(k).unwrap().terminal)
        );
        let mut orders = ReadOnlyOrderReader::new(ReconcileConfig {
            first_connection_epoch: 20,
            ..rc.clone()
        })
        .unwrap();
        let mut trades = ReadOnlyReconciler::new(ReconcileConfig {
            first_connection_epoch: 30,
            ..rc.clone()
        })
        .unwrap();
        for id in ids {
            let query = OrderQuery {
                intent: gateway::intent(),
                order_id: id,
                private_connection_epoch: gap.connection_epoch(),
                gap_generation: gap.generation(),
            };
            let mut permit = |_: &private::AccountScope,
                              _: &OrderQuery,
                              _: u64,
                              _: ReadOperation,
                              _: &private::ClockSample| true;
            orders
                .start(&mut owner, query, &credentials, &mut clock, &mut permit)
                .unwrap();
            let deadline = gateway::until();
            loop {
                assert!(Instant::now() < deadline);
                if let Some(result) = orders
                    .poll(&mut owner, &mut clock, &mut permit, IoBudget::default())
                    .unwrap()
                {
                    assert!(result.accepted, "order futures={futures} id={id:?}");
                    break;
                }
                thread::yield_now()
            }
            let trade = TradeQuery {
                intent: query.intent,
                order_id: id,
                from_id: 0,
                limit: 2,
                private_connection_epoch: query.private_connection_epoch,
                gap_generation: query.gap_generation,
            };
            let mut cash = None;
            for pass in 0..2 {
                let mut permit = |_: &private::AccountScope,
                                  _: &TradeQuery,
                                  _: u64,
                                  _: ReadOperation,
                                  _: &private::ClockSample| true;
                trades
                    .start(&mut owner, trade, &credentials, &mut clock, &mut permit)
                    .unwrap();
                let deadline = gateway::until();
                loop {
                    assert!(Instant::now() < deadline);
                    if let Some(result) = trades
                        .poll(&mut owner, &mut clock, &mut permit, IoBudget::default())
                        .unwrap()
                    {
                        assert!(result.accepted && result.page_exhausted);
                        assert_eq!(result.returned, 1);
                        break;
                    }
                    thread::yield_now()
                }
                let totals: Vec<_> = [1, 2]
                    .into_iter()
                    .map(|id| {
                        owner
                            .account()
                            .asset(gateway::fixture::key(id))
                            .unwrap()
                            .total
                    })
                    .collect();
                if pass == 0 {
                    owner.complete_trade_reconciliation(query).unwrap();
                    cash = Some(totals)
                } else {
                    assert_eq!(Some(totals), cash)
                }
            }
        }
        let state = owner.account().structure(&[73; 16]).unwrap();
        assert!(state.legs.iter().all(|k| {
            let o = owner.account().order(k).unwrap();
            o.terminal && o.fee_reconciled && o.remaining_quantity.atoms() == 0
        }));
        let mut snapshots = AccountSnapshotReader::new(ReconcileConfig {
            first_connection_epoch: 50,
            ..rc
        })
        .unwrap();
        let mut evidence = Vec::new();
        for kind in [
            SnapshotKind::Account,
            SnapshotKind::OpenOrders,
            SnapshotKind::ConditionalOrders,
        ] {
            let q = SnapshotQuery {
                kind,
                private_connection_epoch: gap.connection_epoch(),
                gap_generation: gap.generation(),
            };
            let mut permit = |_: &private::AccountScope,
                              _: SnapshotQuery,
                              _: u64,
                              _: ReadOperation,
                              _: &private::ClockSample| true;
            snapshots
                .start(&mut owner, q, &credentials, &mut clock, &mut permit)
                .unwrap();
            let deadline = gateway::until();
            loop {
                assert!(Instant::now() < deadline);
                if let Some(result) = snapshots
                    .poll(&mut owner, &mut clock, &mut permit, IoBudget::default())
                    .unwrap()
                {
                    evidence.push(result.evidence.unwrap());
                    break;
                }
                thread::yield_now()
            }
        }
        let now = private::Clock::sample(&mut clock).unwrap().monotonic;
        owner
            .reactivate(
                evidence
                    .try_into()
                    .unwrap_or_else(|_| panic!("three snapshots")),
                ActivationCut {
                    private_connection_epoch: gap.connection_epoch(),
                    gap_generation: gap.generation(),
                    through_sequence: owner.applied_position(),
                    now,
                    valid_until: tickwire_clock::MonoTime::new(
                        now.domain(),
                        now.nanos() + 5_000_000_000,
                    ),
                    writer_epoch: 4,
                    execution_connection_epoch: 5,
                },
                None,
            )
            .unwrap();
        assert!(!owner.account().frozen());
        remote.join().unwrap();
        let ledger = owner.account().ledger_sequence();
        let assets: Vec<_> = [1, 2]
            .into_iter()
            .map(|id| {
                owner
                    .account()
                    .asset(gateway::fixture::key(id))
                    .unwrap()
                    .total
            })
            .collect();
        drop(owner);
        assert!(!peer.join().unwrap().is_empty());
        let mut cfg = cfg;
        cfg.writer_epoch = 5;
        let mut input =
            AccountIngress::recover(FileStore::open(&input_dir.0).unwrap(), cfg.clone(), None)
                .unwrap();
        let mut next = gateway::config(&cfg.baseline);
        next.writer_epoch = 5;
        let mut recovered = AccountGateway::recover_with_reactivation(
            FileBackend::open(&account_dir.0).unwrap(),
            next,
            auth,
            gateway::TestEncoder,
            None,
            &mut input.activation_verifier(),
        )
        .unwrap();
        input.replay_pending(&mut recovered, 64).unwrap();
        assert_eq!(recovered.account().ledger_sequence(), ledger);
        assert!(recovered.account().frozen());
        assert_eq!(
            assets,
            [1, 2]
                .into_iter()
                .map(|id| recovered
                    .account()
                    .asset(gateway::fixture::key(id))
                    .unwrap()
                    .total)
                .collect::<Vec<_>>()
        );
        assert!(
            recovered
                .account()
                .structure(&[73; 16])
                .unwrap()
                .legs
                .iter()
                .all(|k| recovered.account().order(k).unwrap().fee_reconciled)
        );
    }
}
