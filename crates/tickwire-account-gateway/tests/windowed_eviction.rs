mod common;

use common::*;
use std::io::{self, Read};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tickwire_account::{Applied, DEFAULT_RETENTION_WINDOW_NS, IntentKey, OrderKey, RecoveryRecord};
use tickwire_account_codec::CanonicalBaseline;
use tickwire_account_gateway::*;
use tickwire_transport::{IoBudget, Limits, rustls};
use tickwire_venue as v;

fn long_lived_peer(host: &str) -> (ConnectionConfig, thread::JoinHandle<usize>) {
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
    listener.set_nonblocking(true).unwrap();
    let handle = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(30);
        let socket = loop {
            match listener.accept() {
                Ok((s, _)) => break s,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() > deadline {
                        return 0;
                    }
                    thread::yield_now();
                }
                Err(e) => panic!("{e}"),
            }
        };
        socket.set_nonblocking(false).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let mut stream =
            rustls::StreamOwned::new(rustls::ServerConnection::new(server).unwrap(), socket);
        let mut total = 0usize;
        let mut buf = [0u8; 8192];
        loop {
            match stream.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => total += n,
            }
        }
        total
    });
    (
        ConnectionConfig {
            address,
            tls,
            limits: Limits {
                max_accept_bytes: 16 * 1024 * 1024,
                ..Default::default()
            },
            connect_deadline: Instant::now() + Duration::from_secs(30),
        },
        handle,
    )
}

fn unique_recovery_object(tag: u8, idx: u64) -> u64 {
    ((tag as u64) << 32) | idx
}

fn drive_intent(g: &mut Gateway, key: IntentKey, wanted: Status) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while g.status(key) != Some(wanted) {
        assert!(Instant::now() < deadline, "timed out: {:?}", g.status(key));
        g.poll(Instant::now(), IoBudget::default()).unwrap();
        thread::yield_now();
    }
}

#[test]
fn gateway_processes_over_three_times_max_intents_in_memory() {
    let (mut snapshot, mut catalog, base_cmd) = fixture::fixture();
    snapshot.valid_until = u64::MAX / 2;
    snapshot.assets[0].free = fixture::units(1);
    snapshot.assets[1].free = fixture::units(1_000_000_000);
    catalog.entries[0].max_absolute_position = fixture::units(100_000);
    catalog.entries[0].max_gross_quantity = fixture::units(100_000);
    catalog.entries[0].strategy_limits = vec![(8, fixture::units(100_000))];
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();

    let mut cfg = config(&baseline);
    cfg.max_intents = 16;

    let (connection, peer_handle) = long_lived_peer(&cfg.route.authority);
    let auth = MockAuthority::default();

    let mut g =
        AccountGateway::create(cfg, baseline, auth.clone(), TestEncoder, connection).unwrap();
    assert_eq!(g.retention_window_ns(), Some(DEFAULT_RETENTION_WINDOW_NS));
    g.set_retention_window_ns(Some(1_000));
    assert_eq!(g.retention_window_ns(), Some(1_000));
    assert_eq!(g.account().retention_window_ns(), Some(1_000));

    let mut resting_cmd = base_cmd.clone();
    resting_cmd.context.request.recovery_object = unique_recovery_object(1, 1);
    resting_cmd.context.request.intent_id = 1;
    resting_cmd.context.request.deadline = u64::MAX / 2;
    if let v::CommandBody::New(ref mut spec) = resting_cmd.body {
        spec.client_id = fixture::venue::text("resting-gw-1");
        spec.quantity = v::Quantity::Base("0.001".parse().unwrap());
        spec.price = Some("60000".parse().unwrap());
    }
    let resting_key = IntentKey {
        epoch: 4,
        intent_id: 1,
    };
    auth.policy.lock().unwrap().gate.now = 1_010;
    assert_eq!(
        g.submit(fixture::canonical(resting_cmd.clone()), 1_000, until())
            .unwrap(),
        Status::Queued
    );
    drive_intent(&mut g, resting_key, Status::Flushed);

    let mut now = 5_000u64;
    for idx in 2..=65u64 {
        now += 200;
        auth.policy.lock().unwrap().gate.now = now + 10;
        let mut cmd = base_cmd.clone();
        cmd.context.request.recovery_object = unique_recovery_object(2, idx);
        cmd.context.request.intent_id = idx;
        cmd.context.request.deadline = now + 50_000;
        let cid = fixture::venue::text(&format!("gw-{idx}"));
        if let v::CommandBody::New(ref mut spec) = cmd.body {
            spec.client_id = cid.clone();
            spec.quantity = v::Quantity::Base("0.001".parse().unwrap());
            spec.price = Some("60000".parse().unwrap());
        }
        let key = IntentKey {
            epoch: 4,
            intent_id: idx,
        };
        assert_eq!(
            g.submit(fixture::canonical(cmd.clone()), now, until())
                .unwrap(),
            Status::Queued
        );
        drive_intent(&mut g, key, Status::Flushed);

        let mut fill_ev = fixture::fill(&cmd, &format!("gw-tr-{idx}"), "0.001", "60000", "60");
        fill_ev.identity.received_mono_ns = now + 20;
        if let v::VenueEventBody::Fill(ref mut b) = fill_ev.body {
            b.observation.status = v::OrderStatus::Filled;
            b.observation.cumulative_quantity = Some("0.001".parse().unwrap());
        }
        assert_eq!(
            g.record_ingress_event([8; 16], idx, fixture::event(fill_ev))
                .unwrap(),
            Applied::New
        );

        let order_key = OrderKey {
            intent: key,
            client_id: cid,
        };
        let _ = g.record_fact(RecoveryRecord::FeesReconciled {
            order: order_key,
            cumulative: fixture::f(100_000),
        });
    }

    let week_later = now + 7 * DEFAULT_RETENTION_WINDOW_NS;
    let _ = g.evict_retired_entries(week_later);
    assert_eq!(g.status(resting_key), Some(Status::Flushed));
    assert!(g.admission_origin(resting_key).is_some());
    assert!(!g.halted());
    assert!(!g.account().frozen());

    assert_eq!(
        g.status(IntentKey {
            epoch: 4,
            intent_id: 2,
        }),
        None
    );

    let mut resting_fill = fixture::fill(&resting_cmd, "gw-tr-resting", "0.001", "60000", "60");
    resting_fill.identity.received_mono_ns = week_later + 10;
    if let v::VenueEventBody::Fill(ref mut b) = resting_fill.body {
        b.observation.status = v::OrderStatus::Filled;
        b.observation.cumulative_quantity = Some("0.001".parse().unwrap());
    }
    g.record_ingress_event([8; 16], 999, fixture::event(resting_fill))
        .unwrap();
    let _ = g.record_fact(RecoveryRecord::FeesReconciled {
        order: OrderKey {
            intent: resting_key,
            client_id: fixture::venue::text("resting-gw-1"),
        },
        cumulative: fixture::f(100_000),
    });

    assert_eq!(g.evict_retired_entries(week_later + 5_000), 1);
    assert_eq!(g.status(resting_key), None);

    drop(g);
    assert!(peer_handle.join().unwrap() > 0);
}

#[test]
fn gateway_management_commands_succeed_when_max_intents_live_orders_are_open() {
    let (mut snapshot, catalog, base_cmd) = fixture::fixture();
    snapshot.valid_until = u64::MAX / 2;
    snapshot.assets[1].free = fixture::units(100_000_000);
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();

    let mut cfg = config(&baseline);
    cfg.max_intents = 8;

    let (connection, peer_handle) = long_lived_peer(&cfg.route.authority);
    let auth = MockAuthority::default();
    auth.policy.lock().unwrap().gate.now = 500;

    let mut g = AccountGateway::create(cfg, baseline, auth, TestEncoder, connection).unwrap();

    for idx in 1..=8u64 {
        let mut cmd = base_cmd.clone();
        cmd.context.request.recovery_object = unique_recovery_object(3, idx);
        cmd.context.request.intent_id = idx;
        cmd.context.request.deadline = 100_000;
        if let v::CommandBody::New(ref mut spec) = cmd.body {
            spec.client_id = fixture::venue::text(&format!("open-gw-{idx}"));
        }
        let key = IntentKey {
            epoch: 4,
            intent_id: idx,
        };
        g.submit(fixture::canonical(cmd), 100, until()).unwrap();
        drive_intent(&mut g, key, Status::Flushed);
    }

    let mut overflow = base_cmd.clone();
    overflow.context.request.recovery_object = unique_recovery_object(3, 9);
    overflow.context.request.intent_id = 9;
    overflow.context.request.deadline = 100_000;
    if let v::CommandBody::New(ref mut spec) = overflow.body {
        spec.client_id = fixture::venue::text("open-gw-9");
    }
    assert!(matches!(
        g.submit(fixture::canonical(overflow), 100, until()),
        Err(Error::Capacity)
    ));

    for idx in 1..=8u64 {
        let target = v::OrderTarget {
            order_id: [9; 16],
            expected_owner_revision: 1,
            original_intent_exec_epoch: 4,
            client_id: Some(fixture::venue::text(&format!("open-gw-{idx}"))),
            venue_order_id: None,
        };

        let mut query = base_cmd.clone();
        query.context.request.recovery_object = unique_recovery_object(4, idx);
        query.context.request.intent_id = 100 + idx;
        query.context.request.deadline = 100_000;
        query.body = v::CommandBody::Query(target.clone());
        let q_key = IntentKey {
            epoch: 4,
            intent_id: 100 + idx,
        };
        g.submit(fixture::canonical(query), 200, until()).unwrap();
        drive_intent(&mut g, q_key, Status::Flushed);

        let mut cancel = base_cmd.clone();
        cancel.context.request.recovery_object = unique_recovery_object(5, idx);
        cancel.context.request.intent_id = 200 + idx;
        cancel.context.request.deadline = 100_000;
        cancel.body = v::CommandBody::Cancel(target);
        let c_key = IntentKey {
            epoch: 4,
            intent_id: 200 + idx,
        };
        g.submit(fixture::canonical(cancel), 250, until()).unwrap();
        drive_intent(&mut g, c_key, Status::Flushed);
    }

    assert!(!g.halted());
    drop(g);
    assert!(peer_handle.join().unwrap() > 0);
}

#[test]
fn gateway_and_account_align_management_victim_selection_under_heterogeneous_deadlines() {
    let (mut snapshot, catalog, base_cmd) = fixture::fixture();
    snapshot.valid_until = u64::MAX / 2;
    snapshot.assets[1].free = fixture::units(100_000_000);
    let baseline = CanonicalBaseline::new(snapshot, catalog).unwrap();

    let cfg = config(&baseline);
    let (connection, peer_handle) = long_lived_peer(&cfg.route.authority);
    let auth = MockAuthority::default();
    auth.policy.lock().unwrap().gate.now = 1_000;

    let mut g =
        AccountGateway::create(cfg, baseline, auth.clone(), TestEncoder, connection).unwrap();

    let mut open = base_cmd.clone();
    open.context.request.recovery_object = unique_recovery_object(9, 1);
    open.context.request.intent_id = 1;
    open.context.request.deadline = u64::MAX / 2;
    if let v::CommandBody::New(ref mut spec) = open.body {
        spec.client_id = fixture::venue::text("hetero-target");
    }
    let open_key = IntentKey {
        epoch: 4,
        intent_id: 1,
    };
    g.submit(fixture::canonical(open), 100, until()).unwrap();
    drive_intent(&mut g, open_key, Status::Flushed);

    let target = v::OrderTarget {
        order_id: [9; 16],
        expected_owner_revision: 1,
        original_intent_exec_epoch: 4,
        client_id: Some(fixture::venue::text("hetero-target")),
        venue_order_id: None,
    };

    let cap = tickwire_account::MAX_MANAGEMENT_ADMISSIONS as u64;
    for idx in 1..=(cap + 4) {
        let now = 200 + idx;
        auth.policy.lock().unwrap().gate.now = now;
        let mut q = base_cmd.clone();
        q.context.request.recovery_object = unique_recovery_object(10, idx);
        q.context.request.intent_id = 1_000 + idx;
        q.context.request.deadline = if idx == 1 { u64::MAX / 2 } else { now + 10 };
        q.body = v::CommandBody::Query(target.clone());
        let key = IntentKey {
            epoch: 4,
            intent_id: 1_000 + idx,
        };
        g.submit(fixture::canonical(q), now, until()).unwrap();
        drive_intent(&mut g, key, Status::Flushed);
    }

    let m1 = IntentKey {
        epoch: 4,
        intent_id: 1_001,
    };
    assert_eq!(g.status(m1), None);
    assert!(!g.account().has_admission(m1));
    assert!(!g.halted());

    drop(g);
    assert!(peer_handle.join().unwrap() > 0);
}
