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
    let account_dir = gateway::Directory::new();
    let inbox_dir = gateway::Directory::new();
    let (baseline, command) = gateway::baseline(false);
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
        .submit(&mut g, command, &[[7; 16]], 60, gateway::until())
        .unwrap();
    fault.store(mode, Ordering::SeqCst);
    receive(messages(false), |d| {
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
