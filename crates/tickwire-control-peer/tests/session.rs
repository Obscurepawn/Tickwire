mod common;
use common::*;
use std::{
    collections::BTreeMap,
    io::Write,
    net::{TcpListener, TcpStream},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tickwire_config_service::{Config, ObjectLimits, Replica};
use tickwire_control_peer::{
    Error,
    frame::{Frame, Header, Kind},
    service::*,
    tls::Scope,
};
use tickwire_control_raft::Node;
use tickwire_journal::store::FileStore;
static NEXT: AtomicU64 = AtomicU64::new(1);
#[test]
fn authenticated_but_cross_group_forged_sender_and_prehello_raft_never_enter_node() {
    for case in 0..3 {
        let fault = run(case);
        assert!(
            matches!(fault, Error::Identity | Error::Protocol),
            "{fault:?}"
        );
    }
}
#[test]
fn half_frame_disconnect_and_slow_frame_close_without_waiting_for_new_data() {
    assert_eq!(run(3), Error::Closed);
    assert_eq!(run(4), Error::Timeout);
}
fn run(case: u8) -> Error {
    let path = std::env::temp_dir().join(format!(
        "tickwire-peer-session-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    let f = Fixture::new();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let node = Node::create(
        FileStore::open(path.join("node")).unwrap(),
        tickwire_control_raft::Config {
            physical: tickwire_journal::Config::new([3; 16], scope().group),
            node_id: 3,
            voters: [1, 2, 3],
            writer_epoch: 1,
            max_command_bytes: 1024 * 1024,
            max_retained_bytes: 8 * 1024 * 1024,
        },
    )
    .unwrap();
    let replica = Replica::new(
        node,
        Config {
            cluster: scope().cluster,
            objects: ObjectLimits {
                max_objects: 8,
                max_inflight: 4,
                max_payload_bytes: 4 * 1024 * 1024,
            },
        },
    )
    .unwrap();
    let mut service = PeerService::new(
        replica,
        listener,
        [3; 16],
        BTreeMap::from([
            (1, "127.0.0.1:9".parse().unwrap()),
            (2, "127.0.0.1:9".parse().unwrap()),
        ]),
        f.tls(3),
        Limits {
            frame_timeout: Duration::from_millis(80),
            handshake_timeout: Duration::from_millis(200),
            ..Limits::default()
        },
    )
    .unwrap();
    let client = f.tls(1);
    let writer = std::thread::spawn(move || {
        let mut socket = TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut tls = client.client(3).unwrap();
        while tls.is_handshaking() {
            tls.complete_io(&mut socket).unwrap();
        }
        client.authenticate(&tls, Some(3)).unwrap();
        let scope = if case == 0 {
            Scope {
                group: [99; 16],
                ..scope()
            }
        } else {
            scope()
        };
        let frame = Frame {
            header: Header {
                kind: if case == 2 { Kind::Raft } else { Kind::Hello },
                cluster: scope.cluster,
                group: scope.group,
                from: if case == 1 { 2 } else { 1 },
                to: 3,
                incarnation: [1; 16],
                challenge: [7; 16],
                sequence: if case == 2 { 1 } else { 0 },
            },
            payload: if case == 2 {
                vec![8, 8, 16, 3, 24, 1, 32, 1]
            } else {
                vec![]
            },
        }
        .encode()
        .unwrap();
        tls.writer()
            .write_all(if case >= 3 { &frame[..64] } else { &frame })
            .unwrap();
        while tls.wants_write() {
            tls.write_tls(&mut socket).unwrap();
        }
        if case == 4 {
            std::thread::sleep(Duration::from_millis(150));
        }
    });
    let deadline = Instant::now() + Duration::from_secs(2);
    let fault = loop {
        assert!(Instant::now() < deadline);
        let status = service.poll(Instant::now(), PeerBudget::default()).unwrap();
        if let Some((_, error)) = status.faults.into_iter().next() {
            break error;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(service.status().term, 0);
    assert_eq!(service.status().raft_applied, 0);
    writer.join().unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
    fault
}

#[test]
fn joining_owner_rejects_ca_valid_peer_missing_from_trusted_directory_before_raft() {
    use tickwire_control_peer::tls::PeerTls;
    let path = std::env::temp_dir().join(format!(
        "tickwire-join-trust-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    let f = Fixture::with_nodes(5);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let node = Node::create_joining(
        FileStore::open(path.join("node")).unwrap(),
        tickwire_control_raft::Config {
            physical: tickwire_journal::Config::new([5; 16], scope().group),
            node_id: 5,
            voters: [1, 2, 3],
            writer_epoch: 1,
            max_command_bytes: 1048576,
            max_retained_bytes: 8 * 1048576,
        },
    )
    .unwrap();
    let replica = Replica::new(
        node,
        Config {
            cluster: scope().cluster,
            objects: ObjectLimits {
                max_objects: 8,
                max_inflight: 4,
                max_payload_bytes: 4 * 1048576,
            },
        },
    )
    .unwrap();
    let tls = PeerTls::new(
        scope(),
        5,
        f.material(5, None, false),
        f.peers(5).into_iter().filter(|p| p.node_id == 4).collect(),
    )
    .unwrap();
    let mut service = PeerService::new(
        replica,
        listener,
        [5; 16],
        BTreeMap::from([(4, "127.0.0.1:9".parse().unwrap())]),
        tls,
        Limits::default(),
    )
    .unwrap();
    let client = f.tls(1);
    let writer = std::thread::spawn(move || {
        let mut socket = TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut tls = client.client(5).unwrap();
        while tls.is_handshaking() {
            if tls.complete_io(&mut socket).is_err() {
                return;
            }
        }
        let _ = tls.writer().write_all(b"untrusted application bytes");
        while tls.wants_write() {
            if tls.write_tls(&mut socket).is_err() {
                break;
            }
        }
    });
    let end = Instant::now() + Duration::from_secs(3);
    loop {
        assert!(Instant::now() < end);
        let polled = service.poll(Instant::now(), PeerBudget::default()).unwrap();
        if polled
            .faults
            .iter()
            .any(|(_, e)| *e == Error::Authentication)
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(service.status().term, 0);
    assert_eq!(service.status().raft_applied, 0);
    assert!(service.status().joining);
    assert!(service.take_applied(10, 1048576).is_empty());
    writer.join().unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
}
