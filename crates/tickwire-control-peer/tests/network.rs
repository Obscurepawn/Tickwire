mod common;
use common::*;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tickwire_config_service::{
    AppliedResult, Config, ObjectCommand, ObjectLimits, ObjectSpec, Purpose, Read, Replica,
};
use tickwire_control_peer::service::*;
use tickwire_control_raft::{Node, driver::Role};
use tickwire_journal::store::FileStore;
static NEXT: AtomicU64 = AtomicU64::new(1);
type Service = PeerService<Replica<FileStore>>;
struct Cluster {
    path: PathBuf,
    nodes: Vec<Service>,
    applied: Vec<(usize, AppliedResult)>,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.nodes.clear();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
impl Cluster {
    fn new() -> Self {
        Self::with_limits(Limits::default())
    }
    fn with_limits(limits: Limits) -> Self {
        let path = std::env::temp_dir().join(format!(
            "tickwire-peer-network-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let f = Fixture::new();
        let listeners: Vec<_> = (0..3)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addresses: BTreeMap<_, _> = listeners
            .iter()
            .enumerate()
            .map(|(i, l)| ((i + 1) as u64, l.local_addr().unwrap()))
            .collect();
        let nodes = listeners
            .into_iter()
            .enumerate()
            .map(|(i, listener)| {
                let id = (i + 1) as u64;
                let node = Node::create(
                    FileStore::open(path.join(id.to_string())).unwrap(),
                    tickwire_control_raft::Config {
                        physical: tickwire_journal::Config::new([id as u8; 16], scope().group),
                        node_id: id,
                        voters: [1, 2, 3],
                        writer_epoch: 1,
                        max_command_bytes: 1024 * 1024,
                        max_retained_bytes: 32 * 1024 * 1024,
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
                let peers = addresses
                    .iter()
                    .filter(|(peer, _)| **peer != id)
                    .map(|(k, v)| (*k, *v))
                    .collect();
                PeerService::new(
                    replica,
                    listener,
                    [id as u8; 16],
                    peers,
                    f.tls(id),
                    limits.clone(),
                )
                .unwrap()
            })
            .collect();
        Self {
            path,
            nodes,
            applied: Vec::new(),
        }
    }
    fn turn(&mut self, isolate: Option<usize>) {
        for (id, node) in self.nodes.iter_mut().enumerate() {
            if isolate == Some(id) {
                continue;
            }
            let status = node.poll(Instant::now(), PeerBudget::default()).unwrap();
            assert!(
                status.faults.iter().all(|(_, e)| matches!(
                    e,
                    tickwire_control_peer::Error::Closed
                        | tickwire_control_peer::Error::Transport
                        | tickwire_control_peer::Error::Timeout
                )),
                "{:?}",
                status.faults
            );
            self.applied.extend(
                node.take_applied(8192, 128 * 1024 * 1024)
                    .into_iter()
                    .map(|r| (id, r)),
            );
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    fn until(&mut self, mut done: impl FnMut(&mut Self) -> bool, isolate: Option<usize>) {
        let end = Instant::now() + Duration::from_secs(6);
        while !done(self) {
            assert!(
                Instant::now() < end,
                "network did not converge: {:?}",
                self.nodes.iter().map(|n| n.status()).collect::<Vec<_>>()
            );
            self.turn(isolate);
        }
    }
    fn leader(&self, isolate: Option<usize>) -> Option<usize> {
        self.nodes
            .iter()
            .enumerate()
            .find(|(i, n)| {
                Some(*i) != isolate
                    && n.status().role == Role::Leader
                    && n.status().raft_applied > 0
            })
            .map(|(i, _)| i)
    }
}
#[test]
fn three_actual_tls_file_replicas_apply_typed_objects_before_safe_reads() {
    let mut c = Cluster::new();
    c.until(|c| c.leader(None).is_some(), None);
    let leader = c.leader(None).unwrap();
    let p = c.nodes[leader]
        .with_control(|mut r| r.initialize())
        .unwrap()
        .unwrap();
    c.until(
        |c| {
            c.applied
                .iter()
                .filter(|(_, r)| r.matches(&p) && r.result().is_ok())
                .count()
                == 3
        },
        None,
    );
    let body = vec![0x5a; 64 * 1024];
    let key = [8; 16];
    for cmd in [
        ObjectCommand::Begin(ObjectSpec {
            key,
            owner: [9; 16],
            purpose: Purpose::Candidate,
            length: body.len() as u32,
            hash: Sha256::digest(&body).into(),
        }),
        ObjectCommand::Chunk {
            key,
            owner: [9; 16],
            offset: 0,
            bytes: body.clone(),
        },
        ObjectCommand::Seal {
            key,
            owner: [9; 16],
        },
    ] {
        let p = c.nodes[leader]
            .with_control(|mut r| r.propose(cmd))
            .unwrap()
            .unwrap();
        c.until(
            |c| {
                c.applied
                    .iter()
                    .filter(|(_, r)| r.matches(&p) && r.result().is_ok())
                    .count()
                    == 3
            },
            None,
        );
    }
    let read = c.nodes[leader]
        .with_control(|mut r| r.request_read())
        .unwrap()
        .unwrap();
    c.until(
        |c| match c.nodes[leader]
            .with_control(|mut r| r.read_object(&read, key))
            .unwrap()
            .unwrap()
        {
            Read::Pending => false,
            Read::Ready(Some(o)) => {
                assert_eq!(o.bytes(), body);
                true
            }
            _ => panic!("missing object"),
        },
        None,
    );
    assert!(
        c.nodes
            .iter()
            .all(|n| n.status().raft_applied == c.nodes[leader].status().raft_applied)
    );
    // Drop every owner, then reopen the same three real WALs without any queue.
    c.nodes.clear();
    for id in 1..=3 {
        let node = Node::recover(
            FileStore::open(c.path.join(id.to_string())).unwrap(),
            tickwire_control_raft::Config {
                physical: tickwire_journal::Config::new([id as u8; 16], scope().group),
                node_id: id,
                voters: [1, 2, 3],
                writer_epoch: 2,
                max_command_bytes: 1024 * 1024,
                max_retained_bytes: 32 * 1024 * 1024,
            },
        )
        .unwrap();
        assert_eq!(node.history().len(), 4);
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
        assert!(replica.scanned_through() >= 5);
        assert!(!replica.is_leader());
    }
}
#[test]
fn isolated_old_leader_cannot_complete_read_and_reconnects_to_new_term() {
    let mut c = Cluster::new();
    c.until(|c| c.leader(None).is_some(), None);
    let old = c.leader(None).unwrap();
    let p = c.nodes[old]
        .with_control(|mut r| r.initialize())
        .unwrap()
        .unwrap();
    c.until(
        |c| c.applied.iter().filter(|(_, r)| r.matches(&p)).count() == 3,
        None,
    );
    let read = c.nodes[old]
        .with_control(|mut r| r.request_read())
        .unwrap()
        .unwrap();
    c.until(|c| c.leader(Some(old)).is_some(), Some(old));
    assert!(matches!(
        c.nodes[old]
            .with_control(|mut r| r.read_object(&read, [8; 16]))
            .unwrap()
            .unwrap(),
        Read::Pending
    ));
    let leader = c.leader(Some(old)).unwrap();
    let term = c.nodes[leader].status().term;
    c.until(
        |c| c.nodes[old].status().term == term && c.nodes[old].status().role != Role::Leader,
        None,
    );
    assert!(
        c.nodes[old]
            .with_control(|mut r| r.read_object(&read, [8; 16]))
            .unwrap()
            .is_err()
    );
}

#[test]
fn unconsumed_applied_results_backpressure_drive_without_losing_results() {
    let mut c = Cluster::with_limits(Limits {
        max_applied_items: 1,
        ..Limits::default()
    });
    c.until(|c| c.leader(None).is_some(), None);
    let leader = c.leader(None).unwrap();
    let first = c.nodes[leader]
        .with_control(|mut r| r.initialize())
        .unwrap()
        .unwrap();
    let end = Instant::now() + Duration::from_secs(3);
    while !c.nodes.iter().all(|n| n.status().raft_applied >= 2) {
        assert!(Instant::now() < end);
        for node in &mut c.nodes {
            node.poll(Instant::now(), PeerBudget::default()).unwrap();
        }
        // Once the initialized log entry is applied on all nodes, no result has
        // been consumed. The leader's durable index is no-op + initialization.
        if c.nodes.iter().all(|n| n.status().raft_applied >= 2) {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let payload = b"queued";
    let second = c.nodes[leader]
        .with_control(|mut r| {
            r.propose(ObjectCommand::Begin(ObjectSpec {
                key: [7; 16],
                owner: [8; 16],
                purpose: Purpose::Candidate,
                length: payload.len() as u32,
                hash: Sha256::digest(payload).into(),
            }))
        })
        .unwrap()
        .unwrap();
    assert!(
        c.nodes[leader]
            .poll(Instant::now(), PeerBudget::default())
            .unwrap()
            .application_backpressure
    );
    assert!(c.nodes[leader].take_applied(0, 1024).is_empty());
    for node in &mut c.nodes {
        let results = node.take_applied(1, 1024);
        assert_eq!(results.len(), 1);
        assert!(results[0].matches(&first));
        assert!(results[0].result().is_ok());
    }
    c.until(
        |c| {
            c.applied
                .iter()
                .filter(|(_, r)| r.matches(&second) && r.result().is_ok())
                .count()
                == 3
        },
        None,
    );
}

#[test]
fn finite_network_queue_refuses_oversize_append_without_reporting_applied() {
    let mut c = Cluster::with_limits(Limits {
        max_peer_bytes: 512,
        max_queued_bytes: 1024,
        ..Limits::default()
    });
    c.until(|c| c.leader(None).is_some(), None);
    let leader = c.leader(None).unwrap();
    let p = c.nodes[leader]
        .with_control(|mut r| r.initialize())
        .unwrap()
        .unwrap();
    c.until(
        |c| c.applied.iter().filter(|(_, r)| r.matches(&p)).count() == 3,
        None,
    );
    let bytes = vec![3; 4096];
    let p = c.nodes[leader]
        .with_control(|mut r| {
            r.propose(ObjectCommand::Begin(ObjectSpec {
                key: [8; 16],
                owner: [9; 16],
                purpose: Purpose::Candidate,
                length: bytes.len() as u32,
                hash: Sha256::digest(&bytes).into(),
            }))
        })
        .unwrap()
        .unwrap();
    c.until(
        |c| c.applied.iter().filter(|(_, r)| r.matches(&p)).count() == 3,
        None,
    );
    let blocked = c.nodes[leader]
        .with_control(|mut r| {
            r.propose(ObjectCommand::Chunk {
                key: [8; 16],
                owner: [9; 16],
                offset: 0,
                bytes,
            })
        })
        .unwrap()
        .unwrap();
    for _ in 0..50 {
        c.turn(None);
    }
    assert!(!c.applied.iter().any(|(_, r)| r.matches(&blocked)));
    assert!(c.nodes.iter().all(|n| !n.status().poisoned));
}
