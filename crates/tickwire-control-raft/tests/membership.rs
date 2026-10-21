use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use tickwire_control_raft::{membership::*, *};
use tickwire_journal::store::FileStore;
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Cluster {
    path: PathBuf,
    nodes: Vec<Node<FileStore>>,
}
fn config(id: u64) -> Config {
    Config {
        physical: tickwire_journal::Config::new([id as u8; 16], [8; 16]),
        node_id: id,
        voters: [1, 2, 3],
        writer_epoch: 1,
        max_command_bytes: 262144,
        max_retained_bytes: 8 * 1024 * 1024,
    }
}
impl Cluster {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "tickwire-membership-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let nodes = (1..=3)
            .map(|id| {
                Node::create(
                    FileStore::open(path.join(id.to_string())).unwrap(),
                    config(id),
                )
                .unwrap()
            })
            .collect();
        Self { path, nodes }
    }
    fn sessions(&mut self) {
        for node in &mut self.nodes {
            for peer in 1..=4 {
                if peer != node.status().node_id {
                    node.peer_session(peer, Some([peer as u8; 16])).unwrap();
                }
            }
        }
    }
    fn pump(&mut self, allowed: &[u64]) {
        for _ in 0..128 {
            let mut out = VecDeque::new();
            for n in &mut self.nodes {
                out.extend(n.drive().unwrap().messages);
            }
            if out.is_empty() {
                return;
            }
            for m in out {
                if allowed.contains(&m.from)
                    && allowed.contains(&m.to)
                    && m.to <= self.nodes.len() as u64
                {
                    let to = (m.to - 1) as usize;
                    let from = m.from;
                    let kind = m.get_msg_type();
                    if let Err(e) = self.nodes[to].step(from, m) {
                        panic!(
                            "step {from} -> {} {kind:?}: {e:?}; {:?}",
                            to + 1,
                            self.nodes[to].current_membership()
                        );
                    }
                }
            }
        }
        panic!("not settled")
    }
    fn elect(&mut self) {
        self.nodes[0].campaign().unwrap();
        self.pump(&[1, 2, 3, 4]);
        assert!(self.nodes[0].is_leader());
    }
    fn request(&self, id: u8, action: MembershipAction) -> MembershipRequest {
        MembershipRequest {
            operation: [id; 16],
            expected_membership: self.nodes[0].current_membership().digest(),
            action,
        }
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
#[test]
fn learner_admission_is_a_durable_entry_with_an_exact_retry_result() {
    let mut c = Cluster::new();
    c.sessions();
    c.elect();
    let req = c.request(1, MembershipAction::AddLearner { node_id: 4 });
    assert!(matches!(
        c.nodes[0].propose_membership(req.clone()).unwrap(),
        MembershipProposal::Pending(_)
    ));
    c.pump(&[1, 2, 3]);
    let r = c.nodes[0]
        .membership_receipt(req.operation)
        .unwrap()
        .clone();
    assert_eq!(r.configuration().learners, vec![4]);
    assert_eq!(c.nodes[0].current_membership().learners(), &[4]);
    assert_eq!(
        c.nodes[0].propose_membership(req.clone()).unwrap(),
        MembershipProposal::Applied(r.clone())
    );
    let mut wrong = req.clone();
    wrong.action = MembershipAction::AddLearner { node_id: 5 };
    assert_eq!(c.nodes[0].propose_membership(wrong), Err(Error::Identity));
    c.nodes.clear();
    for id in 1..=3 {
        c.nodes.push(
            Node::recover(
                FileStore::open(c.path.join(id.to_string())).unwrap(),
                config(id),
            )
            .unwrap(),
        );
    }
    assert_eq!(c.nodes[0].membership_receipt(req.operation), Some(&r));
}
#[test]
fn explicit_join_catchup_joint_and_leave_preserve_both_quorums() {
    let mut c = Cluster::new();
    assert!(
        Node::create(
            FileStore::open(c.path.join("invalid-4")).unwrap(),
            config(4)
        )
        .is_err()
    );
    c.nodes
        .push(Node::create_joining(FileStore::open(c.path.join("4")).unwrap(), config(4)).unwrap());
    assert_eq!(c.nodes[3].campaign(), Err(Error::NotReady));
    assert!(c.nodes[3].request_read().is_err());
    c.sessions();
    c.elect();
    let add = c.request(2, MembershipAction::AddLearner { node_id: 4 });
    c.nodes[0].propose_membership(add).unwrap();
    c.pump(&[1, 2, 3]);
    let early = c.request(
        3,
        MembershipAction::ReplaceVoter {
            remove: 3,
            promote: 4,
            caught_up_through: c.nodes[0].status().committed,
        },
    );
    assert_eq!(c.nodes[0].propose_membership(early), Err(Error::NotReady));
    for _ in 0..6 {
        c.nodes[0].tick().unwrap();
    }
    c.pump(&[1, 2, 3, 4]);
    let replace = c.request(
        4,
        MembershipAction::ReplaceVoter {
            remove: 3,
            promote: 4,
            caught_up_through: c.nodes[0].status().committed,
        },
    );
    c.nodes[0].propose_membership(replace.clone()).unwrap();
    c.pump(&[1, 2, 3, 4]);
    assert_eq!(c.nodes[0].current_membership().voters(), &[1, 2, 4]);
    assert_eq!(
        c.nodes[0].current_membership().voters_outgoing(),
        &[1, 2, 3]
    );
    let leave = c.request(5, MembershipAction::LeaveJoint);
    c.nodes[0].propose_membership(leave.clone()).unwrap();
    c.pump(&[1, 4]);
    assert!(c.nodes[0].membership_receipt(leave.operation).is_none());
    for _ in 0..6 {
        c.nodes[0].tick().unwrap();
    }
    c.pump(&[1, 2, 3, 4]);
    assert!(c.nodes[0].membership_receipt(leave.operation).is_some());
    assert!(c.nodes[0].current_membership().voters_outgoing().is_empty());
    assert_eq!(c.nodes[0].current_membership().retired(), &[3]);
    assert_eq!(c.nodes[2].campaign(), Err(Error::NotReady));
    let reuse = c.request(6, MembershipAction::AddLearner { node_id: 3 });
    assert_eq!(c.nodes[0].propose_membership(reuse), Err(Error::Identity));
    c.nodes.clear();
    for id in 1..=4 {
        c.nodes.push(
            Node::recover(
                FileStore::open(c.path.join(id.to_string())).unwrap(),
                config(id),
            )
            .unwrap(),
        );
    }
    assert_eq!(c.nodes[0].current_membership().voters(), &[1, 2, 4]);
    assert_eq!(c.nodes[0].current_membership().retired(), &[3]);
    assert_eq!(
        c.nodes[0]
            .membership_receipt(replace.operation)
            .unwrap()
            .request(),
        &replace
    );
}

#[test]
fn reconnect_erases_promotion_evidence_and_retired_packets_do_not_advance_term() {
    let mut c = Cluster::new();
    c.nodes
        .push(Node::create_joining(FileStore::open(c.path.join("4")).unwrap(), config(4)).unwrap());
    c.sessions();
    c.elect();
    let add = c.request(11, MembershipAction::AddLearner { node_id: 4 });
    c.nodes[0].propose_membership(add).unwrap();
    c.pump(&[1, 2, 3, 4]);
    let promote = c.request(
        12,
        MembershipAction::ReplaceVoter {
            remove: 3,
            promote: 4,
            caught_up_through: c.nodes[0].status().committed,
        },
    );
    c.nodes[0].peer_session(4, None).unwrap();
    c.nodes[0].peer_session(4, Some([99; 16])).unwrap();
    assert_eq!(
        c.nodes[0].propose_membership(promote.clone()),
        Err(Error::NotReady)
    );
    for _ in 0..6 {
        c.nodes[0].tick().unwrap();
    }
    c.pump(&[1, 2, 3, 4]);
    c.nodes[0].propose_membership(promote).unwrap();
    c.pump(&[1, 2, 3, 4]);
    let leave = c.request(13, MembershipAction::LeaveJoint);
    c.nodes[0].propose_membership(leave).unwrap();
    c.pump(&[1, 2, 3, 4]);
    let term = c.nodes[0].status().term;
    let mut old = Message {
        from: 3,
        to: 1,
        term: term + 10,
        ..Default::default()
    };
    old.set_msg_type(MessageType::MsgRequestVote);
    c.nodes[0].step(3, old).unwrap();
    assert_eq!(c.nodes[0].status().term, term);
    assert!(c.nodes[0].drive().unwrap().messages.is_empty());
    assert!(c.nodes[2].drive().unwrap().messages.is_empty());
    assert!(c.nodes[2].request_read().is_err());
}
#[test]
fn lifetime_identity_limit_does_not_discard_old_receipts() {
    let mut c = Cluster::new();
    c.elect();
    let mut first = None;
    for id in 4..=16 {
        c.nodes[0].peer_session(id, Some([id as u8; 16])).unwrap();
        let r = c.request(id as u8, MembershipAction::AddLearner { node_id: id });
        if first.is_none() {
            first = Some(r.clone())
        }
        c.nodes[0].propose_membership(r).unwrap();
        c.pump(&[1, 2, 3]);
    }
    c.nodes[0].peer_session(17, Some([17; 16])).unwrap();
    let over = c.request(17, MembershipAction::AddLearner { node_id: 17 });
    assert_eq!(c.nodes[0].propose_membership(over), Err(Error::Bounds));
    assert!(matches!(
        c.nodes[0].propose_membership(first.unwrap()).unwrap(),
        MembershipProposal::Applied(_)
    ));
}

#[test]
fn persisted_join_accepts_only_authenticated_replication_from_a_non_origin_leader() {
    let c = Cluster::new();
    let mut e =
        Node::create_joining(FileStore::open(c.path.join("5")).unwrap(), config(5)).unwrap();
    let replication = Message {
        from: 4,
        to: 5,
        term: 20,
        ..Default::default()
    };
    let mut append = replication.clone();
    append.set_msg_type(MessageType::MsgAppend);
    assert_eq!(e.step(4, append.clone()), Err(Error::Identity));
    assert_eq!(e.status().term, 0);
    e.peer_session(4, Some([4; 16])).unwrap();
    let mut vote = replication.clone();
    vote.set_msg_type(MessageType::MsgRequestVote);
    assert_eq!(e.step(4, vote), Err(Error::Identity));
    assert_eq!(e.status().term, 0);
    assert_eq!(e.step(4, append), Ok(()));
    e.drive().unwrap();
    assert_eq!(e.status().term, 20);
    assert!(e.history().is_empty());
    assert_eq!(e.campaign(), Err(Error::NotReady));
    drop(e);
    let mut e = Node::recover(FileStore::open(c.path.join("5")).unwrap(), config(5)).unwrap();
    e.peer_session(4, Some([5; 16])).unwrap();
    let mut heartbeat = replication;
    heartbeat.term = 21;
    heartbeat.set_msg_type(MessageType::MsgHeartbeat);
    assert_eq!(e.step(4, heartbeat), Ok(()));
    assert_eq!(e.status().term, 21);
}

#[test]
fn once_joined_then_retired_identity_never_reopens_join_exception_after_recovery() {
    let mut c = Cluster::new();
    for id in 4..=5 {
        c.nodes.push(
            Node::create_joining(
                FileStore::open(c.path.join(id.to_string())).unwrap(),
                config(id),
            )
            .unwrap(),
        );
    }
    for n in &mut c.nodes {
        for peer in 1..=6 {
            if peer != n.status().node_id {
                n.peer_session(peer, Some([peer as u8; 16])).unwrap();
            }
        }
    }
    c.elect();
    for (add, remove, op) in [(4, 3, 101), (5, 4, 104)] {
        let request = c.request(op, MembershipAction::AddLearner { node_id: add });
        c.nodes[0].propose_membership(request).unwrap();
        c.pump(&[1, 2, 3, 4, 5]);
        assert!(!c.nodes[(add - 1) as usize].status().joining);
        let request = c.request(
            op + 1,
            MembershipAction::ReplaceVoter {
                remove,
                promote: add,
                caught_up_through: c.nodes[0].status().committed,
            },
        );
        c.nodes[0].propose_membership(request).unwrap();
        c.pump(&[1, 2, 3, 4, 5]);
        let request = c.request(op + 2, MembershipAction::LeaveJoint);
        c.nodes[0].propose_membership(request).unwrap();
        c.pump(&[1, 2, 3, 4, 5]);
    }
    assert!(c.nodes[3].current_membership().retired().contains(&4));
    let applied = c.nodes[3].applied_index();
    c.nodes.clear();
    let mut retired = Node::recover(FileStore::open(c.path.join("4")).unwrap(), config(4)).unwrap();
    assert!(!retired.status().joining);
    let term = retired.status().term;
    retired.peer_session(6, Some([6; 16])).unwrap();
    let mut message = Message {
        from: 6,
        to: 4,
        term: term + 10,
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgAppend);
    retired.step(6, message).unwrap();
    assert_eq!(retired.status().term, term);
    assert_eq!(retired.applied_index(), applied);
    assert!(retired.drive().unwrap().messages.is_empty());
    assert_eq!(retired.campaign(), Err(Error::NotReady));
}
