mod common;
use common::*;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tickwire_config_service::{
    AppliedResult, Config, ObjectCommand, ObjectLimits, ObjectSpec, Purpose, Read, Replica,
};
use tickwire_control_peer::service::*;
use tickwire_control_raft::{
    Node,
    driver::Role,
    generation::{GenerationStore, SnapshotDirectory},
    snapshot::SnapshotLimits,
};
static NEXT: AtomicU64 = AtomicU64::new(1);
type Service = PeerService<Replica<GenerationStore>>;
struct Cluster {
    path: PathBuf,
    nodes: Vec<Option<Service>>,
    addresses: BTreeMap<u64, SocketAddr>,
    fixture: Fixture,
    applied: Vec<(usize, AppliedResult)>,
    held_clock: BTreeMap<usize, Instant>,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.nodes.clear();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
fn physical(id: u64, writer: u64) -> tickwire_control_raft::Config {
    tickwire_control_raft::Config {
        physical: tickwire_journal::Config::new([id as u8; 16], scope().group),
        node_id: id,
        voters: [1, 2, 3],
        writer_epoch: writer,
        max_command_bytes: 1024 * 1024,
        max_retained_bytes: 32 * 1024 * 1024,
    }
}
fn application() -> Config {
    Config {
        cluster: scope().cluster,
        objects: ObjectLimits {
            max_objects: 8,
            max_inflight: 4,
            max_payload_bytes: 4 * 1024 * 1024,
        },
    }
}
impl Cluster {
    fn new() -> Self {
        Self::with_nodes(3)
    }
    fn with_nodes(count: u64) -> Self {
        let path = std::env::temp_dir().join(format!(
            "raft-snapshot-network-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let fixture = Fixture::with_nodes(count);
        let listeners = (1..=count)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect::<Vec<_>>();
        let addresses = listeners
            .iter()
            .enumerate()
            .map(|(i, l)| ((i + 1) as u64, l.local_addr().unwrap()))
            .collect::<BTreeMap<_, _>>();
        let mut nodes = Vec::new();
        for (i, l) in listeners.into_iter().enumerate() {
            let id = (i + 1) as u64;
            let directory = SnapshotDirectory::open(
                path.join(id.to_string()),
                scope().group,
                id,
                SnapshotLimits::default(),
            )
            .unwrap();
            let node = if id <= 3 {
                Node::create_in(directory, physical(id, 1))
            } else {
                Node::create_joining_in(directory, physical(id, 1))
            }
            .unwrap();
            let r = Replica::new(node, application()).unwrap();
            nodes.push(Some(
                PeerService::new(
                    r,
                    l,
                    [id as u8; 16],
                    addresses
                        .iter()
                        .filter(|(n, _)| **n != id)
                        .map(|(n, a)| (*n, *a))
                        .collect(),
                    fixture.tls(id),
                    Limits::default(),
                )
                .unwrap(),
            ));
        }
        Self {
            path,
            nodes,
            addresses,
            fixture,
            applied: Vec::new(),
            held_clock: BTreeMap::new(),
        }
    }
    fn turn(&mut self) {
        for (i, node) in self.nodes.iter_mut().enumerate() {
            if let Some(node) = node {
                let status = node
                    .poll(
                        self.held_clock
                            .get(&i)
                            .copied()
                            .unwrap_or_else(Instant::now),
                        PeerBudget::default(),
                    )
                    .unwrap();
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
                        .map(|r| (i, r)),
                );
            }
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    fn until(&mut self, mut done: impl FnMut(&mut Self) -> bool) {
        let end = Instant::now() + Duration::from_secs(10);
        while !done(self) {
            assert!(
                Instant::now() < end,
                "convergence: {:?}",
                self.nodes
                    .iter()
                    .map(|n| n.as_ref().map(|n| n.status()))
                    .collect::<Vec<_>>()
            );
            self.turn();
        }
    }
    fn leader(&self) -> Option<usize> {
        self.nodes.iter().position(|n| {
            n.as_ref()
                .is_some_and(|n| n.status().role == Role::Leader && n.status().raft_applied > 0)
        })
    }
    fn propose(&mut self, leader: usize, cmd: ObjectCommand, count: usize) {
        let p = self.nodes[leader]
            .as_mut()
            .unwrap()
            .with_control(|mut r| r.propose(cmd))
            .unwrap()
            .unwrap();
        self.until(|c| {
            c.applied
                .iter()
                .filter(|(_, r)| r.matches(&p) && r.result().is_ok())
                .count()
                == count
        });
    }
    fn restart(&mut self, index: usize) {
        self.restart_campaign(index, false);
    }
    fn restart_campaign(&mut self, index: usize, campaign: bool) {
        let id = (index + 1) as u64;
        let directory = SnapshotDirectory::open(
            self.path.join(id.to_string()),
            scope().group,
            id,
            SnapshotLimits::default(),
        )
        .unwrap();
        let mut node = Node::recover_in(directory, physical(id, 2)).unwrap();
        if campaign {
            node.campaign().unwrap();
        }
        let replica = Replica::recover_generation(node, application(), None).unwrap();
        let listener = TcpListener::bind(self.addresses[&id]).unwrap();
        self.nodes[index] = Some(
            PeerService::new(
                replica,
                listener,
                [id as u8 + 10; 16],
                self.addresses
                    .iter()
                    .filter(|(p, _)| **p != id)
                    .map(|(p, a)| (*p, *a))
                    .collect(),
                self.fixture.tls(id),
                Limits::default(),
            )
            .unwrap(),
        );
    }
}
#[test]
fn lagging_replica_receives_actual_tls_snapshot_and_tail_then_recovers_application_objects() {
    let mut c = Cluster::new();
    c.until(|c| c.leader().is_some());
    let leader = c.leader().unwrap();
    let p = c.nodes[leader]
        .as_mut()
        .unwrap()
        .with_control(|mut r| r.initialize())
        .unwrap()
        .unwrap();
    c.until(|c| {
        c.applied
            .iter()
            .filter(|(_, r)| r.matches(&p) && r.result().is_ok())
            .count()
            == 3
    });
    let lag = (leader + 1) % 3;
    drop(c.nodes[lag].take());
    for _ in 0..30 {
        c.turn();
    }
    let body = vec![0x5a; 131_073];
    let key = [8; 16];
    let owner = [9; 16];
    c.propose(
        leader,
        ObjectCommand::Begin(ObjectSpec {
            key,
            owner,
            purpose: Purpose::Candidate,
            length: body.len() as u32,
            hash: Sha256::digest(&body).into(),
        }),
        2,
    );
    // The application snapshot includes an unfinished object, not merely a sealed cache.
    c.propose(
        leader,
        ObjectCommand::Chunk {
            key,
            owner,
            offset: 0,
            bytes: body[..65536].to_vec(),
        },
        2,
    );
    let cut = c.nodes[leader]
        .as_mut()
        .unwrap()
        .with_control(|mut r| r.compact_snapshot())
        .unwrap()
        .unwrap()
        .cut()
        .index();
    for (i, chunk) in body[65536..].chunks(65536).enumerate() {
        c.propose(
            leader,
            ObjectCommand::Chunk {
                key,
                owner,
                offset: (65536 + i * 65536) as u32,
                bytes: chunk.to_vec(),
            },
            2,
        );
    }
    c.propose(leader, ObjectCommand::Seal { key, owner }, 2);
    let last = c.nodes[leader].as_ref().unwrap().status().raft_applied;
    c.restart(lag);
    c.until(|c| c.nodes[lag].as_ref().unwrap().status().raft_applied == last);
    let ticket = c.nodes[leader]
        .as_mut()
        .unwrap()
        .with_control(|mut r| r.request_read())
        .unwrap()
        .unwrap();
    c.until(|c| {
        match c.nodes[leader]
            .as_mut()
            .unwrap()
            .with_control(|mut r| r.read_object(&ticket, key))
            .unwrap()
            .unwrap()
        {
            Read::Pending => false,
            Read::Ready(Some(o)) => {
                assert_eq!(o.bytes(), body);
                true
            }
            _ => panic!("missing object"),
        }
    });
    drop(c.nodes[lag].take());
    let id = (lag + 1) as u64;
    let node = Node::recover_in(
        SnapshotDirectory::open(
            c.path.join(id.to_string()),
            scope().group,
            id,
            SnapshotLimits::default(),
        )
        .unwrap(),
        physical(id, 3),
    )
    .unwrap();
    assert_eq!(node.snapshot_base_index(), cut);
    let recovered = Replica::recover_generation(node, application(), None).unwrap();
    assert_eq!(recovered.scanned_through(), last);
    assert!(cut > 0);
    // Capture the reopened business state into immutable objects and independently
    // prepare it again, so a Raft-only advance cannot satisfy this assertion.
    let mut objects =
        tickwire_control_raft::snapshot::MemoryObjects::new(SnapshotLimits::default()).unwrap();
    let (current, root) = recovered.capture_snapshot(&mut objects).unwrap();
    let prepared = recovered
        .prepare_snapshot(&current, &root, &objects)
        .unwrap();
    assert_eq!(prepared.object(key).unwrap().bytes(), body);
}

#[test]
fn fourth_tls_node_joins_via_snapshot_joint_replacement_and_preserves_receipts_after_compaction() {
    use tickwire_control_raft::membership::*;
    let mut c = Cluster::with_nodes(4);
    c.until(|c| {
        c.leader().is_some()
            && c.nodes.iter().all(|n| {
                n.as_ref().unwrap().status().term > 0 || n.as_ref().unwrap().status().node_id == 4
            })
    });
    let leader = c.leader().unwrap();
    assert!(leader < 3);
    let init = c.nodes[leader]
        .as_mut()
        .unwrap()
        .with_control(|mut r| r.initialize())
        .unwrap()
        .unwrap();
    c.until(|c| {
        c.applied
            .iter()
            .filter(|(_, r)| r.matches(&init) && r.result().is_ok())
            .count()
            == 3
    });
    // The trusted fourth peer is connected, but has no Raft voting/proposal role.
    let add = MembershipRequest {
        operation: [71; 16],
        expected_membership: c.nodes[leader]
            .as_ref()
            .unwrap()
            .current_membership()
            .digest(),
        action: MembershipAction::AddLearner { node_id: 4 },
    };
    // Admission requires an actual authenticated session; handshake can still be progressing.
    c.until(|c| {
        match c.nodes[leader]
            .as_mut()
            .unwrap()
            .with_control(|mut r| r.propose_membership(add.clone()))
            .unwrap()
        {
            Ok(_) => true,
            Err(tickwire_config_service::Error::Consensus(
                tickwire_control_raft::Error::NotReady,
            )) => false,
            other => panic!("{other:?}"),
        }
    });
    drop(c.nodes[3].take());
    c.until(|c| {
        c.nodes[leader]
            .as_mut()
            .unwrap()
            .with_control(|r| r.membership_receipt(add.operation).unwrap().cloned())
            .unwrap()
            .is_some()
    });
    let compact = c.nodes[leader]
        .as_mut()
        .unwrap()
        .with_control(|mut r| r.compact_snapshot())
        .unwrap()
        .unwrap();
    let cut = compact.cut().index();
    assert_eq!(compact.cut().membership().learners(), &[4]);
    c.restart(3);
    c.until(|c| c.nodes[3].as_ref().unwrap().status().raft_applied >= cut);
    let remove = (0..3).find(|i| *i != leader).unwrap();
    let removed = (remove + 1) as u64;
    let replace = MembershipRequest {
        operation: [72; 16],
        expected_membership: c.nodes[leader]
            .as_ref()
            .unwrap()
            .current_membership()
            .digest(),
        action: MembershipAction::ReplaceVoter {
            remove: removed,
            promote: 4,
            caught_up_through: c.nodes[leader].as_ref().unwrap().status().committed,
        },
    };
    c.until(|c| {
        match c.nodes[leader]
            .as_mut()
            .unwrap()
            .with_control(|mut r| r.propose_membership(replace.clone()))
            .unwrap()
        {
            Ok(_) => true,
            Err(tickwire_config_service::Error::Consensus(
                tickwire_control_raft::Error::NotReady,
            )) => false,
            other => panic!("{other:?}"),
        }
    });
    c.until(|c| {
        !c.nodes[leader]
            .as_ref()
            .unwrap()
            .current_membership()
            .voters_outgoing()
            .is_empty()
    });
    let leave = MembershipRequest {
        operation: [73; 16],
        expected_membership: c.nodes[leader]
            .as_ref()
            .unwrap()
            .current_membership()
            .digest(),
        action: MembershipAction::LeaveJoint,
    };
    c.nodes[leader]
        .as_mut()
        .unwrap()
        .with_control(|mut r| r.propose_membership(leave.clone()))
        .unwrap()
        .unwrap();
    c.until(|c| {
        c.nodes[leader]
            .as_ref()
            .unwrap()
            .current_membership()
            .retired()
            == [removed]
    });
    let result = c.nodes[leader]
        .as_mut()
        .unwrap()
        .with_control(|r| r.membership_receipt(leave.operation).unwrap().cloned())
        .unwrap()
        .unwrap();
    let final_cut = c.nodes[leader]
        .as_mut()
        .unwrap()
        .with_control(|mut r| r.compact_snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(final_cut.cut().membership().retired(), &[removed]);
    drop(c.nodes[leader].take());
    let id = (leader + 1) as u64;
    let node = Node::recover_in(
        SnapshotDirectory::open(
            c.path.join(id.to_string()),
            scope().group,
            id,
            SnapshotLimits::default(),
        )
        .unwrap(),
        physical(id, 2),
    )
    .unwrap();
    assert_eq!(node.membership_receipt(leave.operation), Some(&result));
    assert_eq!(node.current_membership(), *final_cut.cut().membership());
    let mut r = Replica::recover_generation(node, application(), None).unwrap();
    // Stable result lookup does not need a new election, ETag or another mutation.
    use tickwire_control_raft::driver::PeerDriver;
    assert_eq!(
        r.control().propose_membership(leave).unwrap(),
        MembershipProposal::Applied(result)
    );
}

#[test]
fn promoted_d_leader_admits_new_e_over_tls_snapshot_after_original_voter_retirement() {
    use tickwire_control_raft::membership::*;
    fn change(c: &mut Cluster, leader: usize, operation: u8, action: MembershipAction) {
        let req = MembershipRequest {
            operation: [operation; 16],
            expected_membership: c.nodes[leader]
                .as_ref()
                .unwrap()
                .current_membership()
                .digest(),
            action,
        };
        c.until(|c| {
            match c.nodes[leader]
                .as_mut()
                .unwrap()
                .with_control(|mut r| r.propose_membership(req.clone()))
                .unwrap()
            {
                Ok(_) => true,
                Err(tickwire_config_service::Error::Consensus(
                    tickwire_control_raft::Error::NotReady,
                )) => false,
                other => panic!("{other:?}"),
            }
        });
        c.until(|c| {
            c.nodes[leader]
                .as_mut()
                .unwrap()
                .with_control(|r| r.membership_receipt(req.operation).unwrap().is_some())
                .unwrap()
        });
    }
    let mut c = Cluster::with_nodes(5);
    c.until(|c| c.leader().is_some());
    let original = c.leader().unwrap();
    let p = c.nodes[original]
        .as_mut()
        .unwrap()
        .with_control(|mut r| r.initialize())
        .unwrap()
        .unwrap();
    c.until(|c| {
        c.applied
            .iter()
            .filter(|(_, a)| a.matches(&p) && a.result().is_ok())
            .count()
            == 3
    });
    let body = b"state before any new voter".to_vec();
    let key = [81; 16];
    let owner = [82; 16];
    c.propose(
        original,
        ObjectCommand::Begin(ObjectSpec {
            key,
            owner,
            purpose: Purpose::Candidate,
            length: body.len() as u32,
            hash: Sha256::digest(&body).into(),
        }),
        3,
    );
    c.propose(
        original,
        ObjectCommand::Chunk {
            key,
            owner,
            offset: 0,
            bytes: body.clone(),
        },
        3,
    );
    c.propose(original, ObjectCommand::Seal { key, owner }, 3);
    change(
        &mut c,
        original,
        80,
        MembershipAction::AddLearner { node_id: 4 },
    );
    let through = c.nodes[original].as_ref().unwrap().status().committed;
    c.until(|c| c.nodes[3].as_ref().unwrap().status().raft_applied >= through);
    let removed = (0..3).find(|n| *n != original).unwrap();
    change(
        &mut c,
        original,
        81,
        MembershipAction::ReplaceVoter {
            remove: (removed + 1) as u64,
            promote: 4,
            caught_up_through: through,
        },
    );
    change(&mut c, original, 82, MembershipAction::LeaveJoint);
    c.until(|c| {
        c.nodes[3]
            .as_ref()
            .unwrap()
            .current_membership()
            .retired()
            .contains(&((removed + 1) as u64))
    });
    // A real new election: stop the old leader; D campaigns in a newly recovered
    // owner, then wins the remaining voter over authenticated TLS.
    drop(c.nodes[original].take());
    let remaining = (0..3).find(|n| *n != original && *n != removed).unwrap();
    drop(c.nodes[remaining].take());
    drop(c.nodes[3].take());
    // Reopen the other voter before polling either owner: its old volatile
    // leader lease is gone, so D's explicit campaign is the first election.
    c.restart(remaining);
    c.restart_campaign(3, true);
    // Control election scheduling, not votes: the other owner still handles real
    // TLS I/O, but its equal monotonic samples produce no election ticks. D's
    // clock progresses until a real peer vote makes it leader.
    c.held_clock.insert(remaining, Instant::now());
    c.until(|c| c.nodes[3].as_ref().unwrap().status().role == Role::Leader);
    c.held_clock.remove(&remaining);
    c.until(|c| {
        c.nodes[3]
            .as_mut()
            .unwrap()
            .with_control(|mut r| r.request_read())
            .unwrap()
            .is_ok()
    });
    let add = MembershipRequest {
        operation: [83; 16],
        expected_membership: c.nodes[3].as_ref().unwrap().current_membership().digest(),
        action: MembershipAction::AddLearner { node_id: 5 },
    };
    c.until(|c| {
        match c.nodes[3]
            .as_mut()
            .unwrap()
            .with_control(|mut r| r.propose_membership(add.clone()))
            .unwrap()
        {
            Ok(_) => true,
            Err(tickwire_config_service::Error::Consensus(
                tickwire_control_raft::Error::NotReady,
            )) => false,
            other => panic!("{other:?}"),
        }
    });
    // E has no applied membership yet. It reconnects only after D compacts the
    // AddLearner entry, forcing a snapshot from a leader outside the origin set.
    drop(c.nodes[4].take());
    c.until(|c| {
        c.nodes[3]
            .as_mut()
            .unwrap()
            .with_control(|r| r.membership_receipt(add.operation).unwrap().is_some())
            .unwrap()
    });
    let compact = c.nodes[3]
        .as_mut()
        .unwrap()
        .with_control(|mut r| r.compact_snapshot())
        .unwrap()
        .unwrap();
    assert!(compact.cut().membership().voters().contains(&4));
    assert!(compact.cut().membership().learners().contains(&5));
    c.restart(4);
    assert!(c.nodes[4].as_ref().unwrap().status().joining);
    c.until(|c| c.nodes[4].as_ref().unwrap().status().raft_applied >= compact.cut().index());
    assert!(!c.nodes[4].as_ref().unwrap().status().joining);
    drop(c.nodes[4].take());
    let node = Node::recover_in(
        SnapshotDirectory::open(
            c.path.join("5"),
            scope().group,
            5,
            SnapshotLimits::default(),
        )
        .unwrap(),
        physical(5, 3),
    )
    .unwrap();
    assert!(!node.status().joining);
    assert_eq!(node.current_membership(), *compact.cut().membership());
    assert_eq!(node.snapshot_base_index(), compact.cut().index());
    let recovered = Replica::recover_generation(node, application(), None).unwrap();
    let mut objects =
        tickwire_control_raft::snapshot::MemoryObjects::new(SnapshotLimits::default()).unwrap();
    let (current, root) = recovered.capture_snapshot(&mut objects).unwrap();
    let prepared = recovered
        .prepare_snapshot(&current, &root, &objects)
        .unwrap();
    assert_eq!(prepared.object(key).unwrap().bytes(), body);
}
