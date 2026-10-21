mod common;
use common::*;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::{Duration, Instant},
};
use tickwire_config_service::{
    Config, ObjectLimits, Replica,
    auth::{IdentityMaterial, Peer, ServerTls},
    https,
};
use tickwire_control_peer::{
    api_runner::ApiOwner,
    service::{Limits, PeerBudget, PeerService},
};
use tickwire_control_raft::{
    Node, driver::Role, generation::SnapshotDirectory, snapshot::SnapshotLimits,
};

#[test]
fn actual_https_and_three_tls_peers_share_the_applied_owner_and_shutdown() {
    let path = std::env::temp_dir().join(format!("tickwire-api-owner-{}", std::process::id()));
    std::fs::create_dir(&path).unwrap();
    let f = Fixture::new();
    let listeners: Vec<_> = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: BTreeMap<_, _> = listeners
        .iter()
        .enumerate()
        .map(|(i, l)| (i as u64 + 1, l.local_addr().unwrap()))
        .collect();
    let mut owners = Vec::new();
    for (i, listener) in listeners.into_iter().enumerate() {
        let id = i as u64 + 1;
        let directory = SnapshotDirectory::open(
            path.join(id.to_string()),
            scope().group,
            id,
            SnapshotLimits::default(),
        )
        .unwrap();
        let node = Node::create_in(
            directory,
            tickwire_control_raft::Config {
                physical: tickwire_journal::Config::new([id as u8; 16], scope().group),
                node_id: id,
                voters: [1, 2, 3],
                writer_epoch: 1,
                max_command_bytes: 1048576,
                max_retained_bytes: 32 * 1048576,
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
        let peer = PeerService::new(
            replica,
            listener,
            [id as u8; 16],
            addresses
                .iter()
                .filter(|(n, _)| **n != id)
                .map(|(n, a)| (*n, *a))
                .collect(),
            f.tls(id),
            Limits::default(),
        )
        .unwrap();
        let m = f.material(id, None, false);
        let auth = ServerTls::new(
            IdentityMaterial {
                chain_der: m.chain_der,
                private_key_der: m.private_key_der,
                roots_der: m.roots_der,
                crls_der: m.crls_der,
            },
            vec![Peer {
                subject: tickwire_control_peer::tls::peer_uri(scope(), 2),
                leaf_sha256: Sha256::digest(f.certs[1].der()).into(),
            }],
        )
        .unwrap();
        let http = https::Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            auth,
            https::Limits::default(),
        )
        .unwrap();
        owners.push(ApiOwner::from_parts(peer, http));
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    let leader = loop {
        for owner in &mut owners {
            owner
                .poll(Instant::now(), PeerBudget::default(), 8)
                .unwrap();
        }
        if let Some(i) = owners
            .iter()
            .position(|o| o.status().role == Role::Leader && o.status().raft_applied > 0)
        {
            break i;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    };
    let proposal = owners[leader]
        .with_control(|mut c| c.initialize())
        .unwrap()
        .unwrap();
    // Do not consume Applied while driving: it must remain in the exact peer
    // queue even when HTTPS is attached and turns continue.
    loop {
        for owner in &mut owners {
            owner
                .poll(Instant::now(), PeerBudget::default(), 8)
                .unwrap();
        }
        if owners.iter().all(|o| o.status().raft_applied >= 2) {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    for owner in &mut owners {
        let applied = owner.take_applied(100, 1048576);
        assert_eq!(
            applied
                .iter()
                .filter(|a| a.matches(&proposal) && a.result().is_ok())
                .count(),
            1
        );
        assert!(owner.take_applied(100, 1048576).is_empty());
    }
    let address = owners[leader].http_addr().unwrap();
    let m = f.material(2, None, false);
    let mut roots = rustls::RootCertStore::empty();
    for root in m.roots_der {
        roots.add(root.into()).unwrap();
    }
    let provider = rustls::crypto::ring::default_provider();
    let mut tls = rustls::ClientConfig::builder_with_provider(provider.into())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_client_auth_cert(
            m.chain_der.into_iter().map(Into::into).collect(),
            rustls::pki_types::PrivateKeyDer::try_from(m.private_key_der).unwrap(),
        )
        .unwrap();
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    let (send, recv) = mpsc::channel();
    let client = std::thread::spawn(move || {
        let socket = TcpStream::connect(address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let session =
            rustls::ClientConnection::new(tls.into(), "localhost".try_into().unwrap()).unwrap();
        let mut stream = rustls::StreamOwned::new(session, socket);
        stream
            .write_all(
                b"GET /v1/config/schema HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .unwrap();
        stream.flush().unwrap();
        let mut bytes = Vec::new();
        let _ = stream.read_to_end(&mut bytes);
        send.send(bytes).unwrap();
    });
    let mut pending_seen = false;
    let response = loop {
        for owner in &mut owners {
            let status = owner
                .poll(Instant::now(), PeerBudget::default(), 8)
                .unwrap();
            pending_seen |= status.http.pending > 0;
        }
        if let Ok(bytes) = recv.try_recv() {
            break bytes;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    };
    client.join().unwrap();
    assert!(
        pending_seen,
        "HTTP read passed through the owner's pending ReadIndex"
    );
    // Initialization has no signed business head; a completed safe read reports
    // unavailable rather than inventing a configuration bootstrap.
    assert!(
        response.starts_with(b"HTTP/1.1 503"),
        "{}",
        String::from_utf8_lossy(&response)
    );
    assert!(owners[leader].status().role == Role::Leader);
    let cut = owners[leader].status().raft_applied;
    let pending = owners[leader]
        .with_control(|mut c| {
            c.propose(tickwire_config_service::ObjectCommand::Begin(
                tickwire_config_service::ObjectSpec {
                    key: [77; 16],
                    owner: [78; 16],
                    purpose: tickwire_config_service::Purpose::Candidate,
                    length: 1,
                    hash: Sha256::digest([1]).into(),
                },
            ))
        })
        .unwrap()
        .unwrap();
    loop {
        for owner in &mut owners {
            owner
                .poll(Instant::now(), PeerBudget::default(), 8)
                .unwrap();
        }
        if owners.iter().all(|o| o.status().raft_applied > cut) {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(1));
    }
    let invalid = PeerBudget {
        frames: 0,
        ..PeerBudget::default()
    };
    assert!(owners[0].poll(Instant::now(), invalid, 8).is_err());
    assert!(owners[0].http_addr().is_err());
    // Fatal peer cleanup and graceful HTTP shutdown both preserve durable results.
    for owner in &mut owners {
        owner.shutdown().unwrap();
        assert_eq!(
            owner
                .take_applied(10, 1048576)
                .iter()
                .filter(|r| r.matches(&pending) && r.result().is_ok())
                .count(),
            1
        );
        assert!(
            owner
                .poll(Instant::now(), PeerBudget::default(), 8)
                .is_err()
        );
    }
    drop(owners);
    std::fs::remove_dir_all(path).unwrap();
}
