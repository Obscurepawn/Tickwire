use super::*;
use tickwire_control_sys::{ControlListener, PrivateDirectory};
#[path = "../../../tickwire-join/tests/common/mod.rs"]
mod fixture;

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
use peer::Peer;

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
