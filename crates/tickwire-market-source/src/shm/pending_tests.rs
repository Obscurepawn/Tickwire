use super::*;
use crate::test_fixture as fixture;
use tickwire_control_sys::{ControlListener, PrivateDirectory};

fn config(path: std::path::PathBuf) -> ConnectConfig {
    let local = fixture::identity(4, 3);
    let peer = fixture::identity(3, 2);
    let mut policy = fixture::policy(
        local,
        peer,
        ProcessIdentity {
            pid: std::process::id(),
            uid: std::fs::metadata("/proc/self").unwrap().uid(),
            gid: std::fs::metadata("/proc/self").unwrap().gid(),
        },
        fixture::NOW,
    );
    policy.supported_features |= 64;
    let mut bank = fixture::header();
    bank.required_features |= 64;
    ConnectConfig {
        endpoint: Endpoint::new(path, &policy),
        bank_header: wire_bytes(&bank).unwrap(),
        catalog_hash: fixture::registry().catalog_hash,
        instrument_set_version: 1,
        instruments: vec![Instrument {
            id: 1001,
            version: 5,
            required_states: 1,
        }],
        cursor_id: [104; 16],
        subscription_generation: 1,
        request_id: [25; 16],
        maximum_levels: 32,
        startup_timeout_ms: 500,
        clock_generation: 1,
    }
}
use std::os::unix::fs::MetadataExt;
#[test]
fn silent_peer_does_not_block_pending_join_and_cancel_is_terminal() {
    let dir = PrivateDirectory::create_in(std::path::Path::new("/tmp"), "tw-pending-join").unwrap();
    let path = dir.path().join("control.sock");
    let listener = ControlListener::bind(&path).unwrap();
    let config = config(path);
    let now = MonoTime::new(config.domain().unwrap(), fixture::NOW);
    // SAFETY: the local listener is controlled by this test; no unqualified memory is offered.
    let mut pending = unsafe { PendingJoin::new(&config, now).unwrap() };
    assert!(!pending.poll(now, 1).unwrap().ready);
    let mut server = listener.try_accept().unwrap().unwrap();
    assert!(!pending.poll(now, 1).unwrap().ready);
    // Repeated polls remain bounded with a silent peer; an interrupted send is retried.
    assert!(!pending.poll(now, 1).unwrap().ready);
    assert!(!pending.poll(now, 1).unwrap().runnable);
    let hello = server
        .try_recv()
        .unwrap()
        .expect("HELLO is a real queued packet");
    assert_eq!(
        ControlEnvelope::decode(&hello.bytes()[..128])
            .unwrap()
            .opcode,
        1
    );
    assert!(pending.take_client().is_none());
    assert!(!pending.poll(now, 1).unwrap().runnable);
    pending.cancel();
    assert!(matches!(pending.poll(now, 1), Err(Error::NotReady)));
    assert!(matches!(
        server.try_recv(),
        Err(tickwire_control_sys::Error::Closed)
    ));
}

mod peer;
mod transition;
use peer::Peer;

#[test]
fn welcome_issued_after_poll_time_waits_for_a_fresh_sample_before_validation() {
    let mut peer = Peer::new();
    let mut pending = peer.pending();
    let before_receive = peer.now();
    let after_receive = MonoTime::new(before_receive.domain(), before_receive.nanos() + 1);
    pending.poll(before_receive, 64).unwrap();
    peer.step_at(after_receive.nanos());
    peer.step_at(after_receive.nanos());
    assert_eq!(
        peer.stage, 1,
        "real peer queued WELCOME after client sampled now"
    );

    let report = pending.poll(before_receive, 64).unwrap();
    assert_eq!(
        report.work, 1,
        "receive must yield even with remaining budget"
    );
    assert!(report.runnable && !report.ready);
    assert!(pending.take_client().is_none());
    assert!(
        peer.connection
            .as_mut()
            .unwrap()
            .try_recv()
            .unwrap()
            .is_none()
    );

    for _ in 0..128 {
        let report = pending.poll(after_receive, 1).unwrap();
        assert!(report.work <= 1 && report.records <= 1);
        if report.ready {
            let client = pending.take_client().unwrap();
            assert_eq!(client.consumer.watermarks().applied, peer.owner.published());
            assert_eq!(
                client.session.deadline(),
                after_receive.nanos() + peer.config.endpoint.lease_duration_ns
            );
            return;
        }
        peer.step_at(after_receive.nanos());
    }
    panic!("the freshly sampled authenticated handshake must reach a real active JOIN");
}

#[test]
fn deferred_welcome_preserves_deadlines_clock_budget_and_cancellation() {
    for case in 0..6 {
        let mut peer = Peer::new();
        if case == 1 {
            peer.config.endpoint.lease_duration_ns = 1_000_000;
        }
        let mut pending = peer.pending();
        let old = peer.now();
        pending.poll(old, 64).unwrap();
        peer.step_at(old.nanos() + 1);
        peer.step_at(old.nanos() + 1);
        assert!(pending.poll(old, 64).unwrap().runnable);
        assert!(pending.take_client().is_none());
        let result = match case {
            0 => pending.poll(MonoTime::new(old.domain(), old.nanos() + 500_000_000), 64),
            1 => pending.poll(MonoTime::new(old.domain(), old.nanos() + 1_000_001), 64),
            2 => pending.poll(MonoTime::new(old.domain(), old.nanos() - 1), 64),
            3 => pending.poll(MonoTime::new(old.domain(), old.nanos() + 1), 0),
            4 => {
                pending.cancel();
                pending.poll(MonoTime::new(old.domain(), old.nanos() + 1), 64)
            }
            _ => pending.poll(old, 64),
        };
        assert!(match (case, result) {
            (0, Err(Error::Deadline))
            | (1, Err(Error::Control(tickwire_control::Error::Expired)))
            | (2, Err(Error::Stale))
            | (3, Err(Error::Bounds))
            | (4, Err(Error::NotReady))
            | (5, Err(Error::Control(tickwire_control::Error::Expired))) => true,
            other => panic!("unexpected deferred WELCOME rejection: {other:?}"),
        });
        assert!(pending.take_client().is_none());
        assert!(matches!(pending.poll(old, 1), Err(Error::NotReady)));
        assert!(matches!(
            peer.connection.as_mut().unwrap().try_recv(),
            Err(tickwire_control_sys::Error::Closed)
        ));
        assert_eq!(
            peer.stage, 1,
            "no JOIN request escaped the failed handshake"
        );
    }
}

#[test]
fn descriptor_offer_instead_of_welcome_is_rejected_before_deferred_validation() {
    let mut peer = Peer::new();
    let mut pending = peer.pending();
    let now = peer.now();
    pending.poll(now, 64).unwrap();
    peer.step();
    let connection = peer.connection.as_mut().unwrap();
    let hello = connection.try_recv().unwrap().unwrap();
    let mut policy = peer.config.endpoint.policy();
    std::mem::swap(&mut policy.local, &mut policy.peer);
    let actual = connection.peer_credentials();
    let (session, _) = Session::accept(
        &policy,
        ProcessIdentity {
            pid: actual.pid,
            uid: actual.uid,
            gid: actual.gid,
        },
        hello.bytes(),
        now.nanos(),
    )
    .unwrap();
    let offer = resource_offer(
        &session,
        &ResourceHeader::decode(&peer.config.bank_header).unwrap(),
        [73; 16],
        session.deadline(),
        1,
        false,
    )
    .unwrap();
    let descriptor = File::open("/dev/null").unwrap();
    assert!(connection.try_send(&offer, &[descriptor.as_fd()]).unwrap());
    assert!(matches!(pending.poll(now, 64), Err(Error::Identity)));
    assert!(pending.take_client().is_none());
    assert!(matches!(pending.poll(now, 64), Err(Error::NotReady)));
}

#[test]
fn real_join_preserves_baseline_catchup_and_budget_before_authenticated_activation() {
    let mut peer = Peer::new();
    peer.force_catchup = true;
    let mut pending = peer.pending();
    let now = peer.now();
    let mut published = false;
    let mut records = 0;
    let mut ready = false;
    for _ in 0..128 {
        let report = pending.poll(now, 1).unwrap();
        assert!(report.work <= 1 && report.records <= 1);
        records += report.records;
        if report.ready {
            ready = true;
            break;
        }
        assert!(pending.take_client().is_none());
        peer.step();
        if peer.stage == 2 && !published {
            peer.publish();
            peer.publish();
            published = true;
        }
    }
    assert!(ready);
    assert_eq!(peer.pending_replies, 1);
    assert_eq!(records, 3);
    assert_eq!(
        peer.owner.stage(peer.registration.subscription),
        Some(tickwire_join::Stage::Active)
    );
    let client = pending.take_client().unwrap();
    assert!(pending.take_client().is_none());
    assert_eq!(client.join.snapshot_seq, 4);
    assert_eq!(client.replica.sequence(), 7);
    assert_eq!(client.consumer.watermarks().applied, 7);
    assert_eq!(client.catchup.len(), 3);
    for (index, record) in client.catchup.iter().enumerate() {
        assert_eq!(record.record, peer.data.record(index as u64 + 5));
        assert!(record.payload.is_empty());
        assert_eq!(record.instrument, 1001);
        assert_eq!(record.applied_mono_ns, now.nanos());
    }
    assert_eq!(client.baseline_now, now.nanos());
    assert!(!client.baseline_bytes.is_empty());
}

#[test]
fn wrong_join_or_activation_watermark_never_yields_a_client() {
    for wrong_join in [true, false] {
        let mut peer = Peer::new();
        peer.wrong_join = wrong_join;
        peer.ahead_ack = !wrong_join;
        let mut pending = peer.pending();
        let now = peer.now();
        let error = (0..128)
            .find_map(|_| match pending.poll(now, 1) {
                Ok(report) => {
                    assert!(!report.ready);
                    peer.step();
                    None
                }
                Err(error) => Some(error),
            })
            .expect("authenticated mismatch must fail");
        assert!(matches!(error, Error::Identity));
        assert!(pending.take_client().is_none());
        assert!(matches!(pending.poll(now, 1), Err(Error::NotReady)));
        assert!(peer.owner.stage(peer.registration.subscription).is_some());
    }
}

#[test]
fn invalid_clock_budget_deadline_and_eof_close_pending_resources() {
    for case in 0..5 {
        let mut peer = Peer::new();
        let mut pending = peer.pending();
        let now = peer.now();
        pending.poll(now, 1).unwrap();
        peer.step();
        let result = match case {
            0 => pending.poll(now, 0),
            1 => pending.poll(
                MonoTime::new(ClockDomain::new([99; 16], 1).unwrap(), now.nanos()),
                1,
            ),
            2 => pending.poll(MonoTime::new(now.domain(), now.nanos() - 1), 1),
            3 => pending.poll(MonoTime::new(now.domain(), now.nanos() + 500_000_000), 1),
            _ => {
                pending.poll(now, 1).unwrap();
                peer.connection = None;
                pending.poll(now, 1)
            }
        };
        assert!(result.is_err(), "case {case}");
        assert!(pending.take_client().is_none());
        assert!(matches!(pending.poll(now, 1), Err(Error::NotReady)));
    }
}

#[test]
fn cancel_after_cursor_registration_retains_owner_pending_gate() {
    let mut peer = Peer::new();
    let mut pending = peer.pending();
    let now = peer.now();
    for _ in 0..64 {
        pending.poll(now, 1).unwrap();
        peer.step();
        if peer.stage == 3 {
            break;
        }
    }
    assert_eq!(peer.stage, 3);
    pending.cancel();
    assert!(pending.take_client().is_none());
    assert_eq!(
        peer.owner.stage(peer.registration.subscription),
        Some(tickwire_join::Stage::Delivered)
    );
    assert!(!peer.owner.all_detached());
}

#[test]
fn compatibility_connect_drives_the_same_real_join_to_completion() {
    let (config_tx, config_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let owner = std::thread::spawn(move || {
        let mut peer = Peer::new();
        config_tx.send(peer.config.clone()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while peer.stage != 6 {
            assert!(Instant::now() < deadline);
            peer.step();
            std::thread::yield_now();
        }
        release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    });
    let config = config_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let now = MonoTime::new(config.domain().unwrap(), fixture::NOW);
    // SAFETY: the trusted fixture thread retains the exact owner and pending gate until release.
    let client = unsafe { Client::connect(&config, &mut || Ok(now)).unwrap() };
    assert_eq!(client.join.snapshot_seq, 4);
    assert_eq!(client.replica.sequence(), 4);
    assert!(client.catchup.is_empty());
    drop(client);
    release_tx.send(()).unwrap();
    owner.join().unwrap();
}
