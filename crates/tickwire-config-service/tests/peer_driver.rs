use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use tickwire_config_service::*;
use tickwire_control_raft::{
    Node, Proposal,
    driver::{PeerDriver, ReadyBudget},
    membership::{MembershipAction, MembershipProposal, MembershipRequest},
};
use tickwire_journal::store::FileStore;

static NEXT: AtomicU64 = AtomicU64::new(1);
fn raft_config(id: u64) -> tickwire_control_raft::Config {
    tickwire_control_raft::Config {
        physical: tickwire_journal::Config::new([id as u8; 16], [18; 16]),
        node_id: id,
        voters: [1, 2, 3],
        writer_epoch: 1,
        max_command_bytes: MAX_COMMAND_BYTES,
        max_retained_bytes: 32 * 1024 * 1024,
    }
}
fn service_config() -> Config {
    Config {
        cluster: [19; 16],
        objects: ObjectLimits {
            max_objects: 8,
            max_inflight: 4,
            max_payload_bytes: 4 * 1024 * 1024,
        },
    }
}
struct Cluster {
    path: PathBuf,
    nodes: Vec<Replica<FileStore>>,
    applied: Vec<(usize, AppliedResult)>,
    saw_zero_business_advance: bool,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.nodes.clear();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
impl Cluster {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "tickwire-config-peer-driver-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        let nodes = (1..=3)
            .map(|id| {
                Replica::new(
                    Node::create(
                        FileStore::open(path.join(id.to_string())).unwrap(),
                        raft_config(id),
                    )
                    .unwrap(),
                    service_config(),
                )
                .unwrap()
            })
            .collect();
        Self {
            path,
            nodes,
            applied: vec![],
            saw_zero_business_advance: false,
        }
    }
    fn pump(&mut self) {
        let budget = ReadyBudget::new(1).unwrap();
        for _ in 0..512 {
            let mut messages = VecDeque::new();
            let mut more = false;
            for (id, node) in self.nodes.iter_mut().enumerate() {
                let previous = node.scanned_through();
                let bound = PeerDriver::output_bound(node, budget).unwrap();
                let out = PeerDriver::drive(node, budget).unwrap();
                let charge = out.charge().unwrap();
                assert!(
                    charge.applied.items <= bound.items,
                    "item reservation exceeded"
                );
                assert!(
                    charge.applied.bytes <= bound.bytes,
                    "retained byte reservation exceeded"
                );
                assert_eq!(out.raft_applied_through, out.application_scanned_through);
                assert_eq!(out.application_scanned_through, node.scanned_through());
                if out.applied.is_empty() && out.application_scanned_through > previous {
                    self.saw_zero_business_advance = true;
                }
                more |= out.more_ready;
                messages.extend(out.messages);
                self.applied
                    .extend(out.applied.into_iter().map(|v| (id, v)));
            }
            if messages.is_empty() && !more {
                return;
            }
            for message in messages {
                self.nodes[(message.to - 1) as usize]
                    .step(message.from, message)
                    .unwrap();
            }
        }
        panic!("one-ready driver did not settle");
    }
    fn ready(&mut self) {
        self.nodes[0].campaign().unwrap();
        self.pump();
        let proposal = self.nodes[0].control().initialize().unwrap();
        self.pump();
        assert_eq!(self.result(&proposal), Ok(Change::Initialized));
    }
    fn result(&self, p: &Proposal) -> Result<Change> {
        self.applied
            .iter()
            .find(|(id, a)| *id == 0 && a.matches(p))
            .unwrap()
            .1
            .result()
    }
    fn command(&mut self, command: ObjectCommand) -> Result<Change> {
        let proposal = self.nodes[0].control().propose(command).unwrap();
        self.pump();
        self.result(&proposal)
    }
}
#[test]
fn one_ready_driver_applies_business_before_returning_and_counts_leader_noops() {
    let mut c = Cluster::new();
    c.ready();
    assert!(c.saw_zero_business_advance);
    for node in &c.nodes {
        let membership = node.current_membership();
        assert_eq!(membership.voters(), &[1, 2, 3]);
        assert!(!node.status().poisoned);
        assert_eq!(node.status().raft_applied, node.scanned_through());
    }
}
#[test]
fn retained_result_reservation_covers_large_successful_and_rejected_commands() {
    let mut c = Cluster::new();
    c.ready();
    let bytes = vec![3; CHUNK_BYTES * 3 + 5];
    let key = [8; 16];
    let owner = [9; 16];
    assert_eq!(
        c.command(ObjectCommand::Begin(ObjectSpec {
            key,
            owner,
            purpose: Purpose::Candidate,
            length: bytes.len() as u32,
            hash: Sha256::digest(&bytes).into()
        })),
        Ok(Change::Begun)
    );
    assert_eq!(
        c.command(ObjectCommand::Seal { key, owner }),
        Err(Error::Incomplete)
    );
    // Several proposals are pending together, including an exact chunk retry.
    let mut proposals = vec![];
    for (index, chunk) in bytes.chunks(CHUNK_BYTES).enumerate() {
        proposals.push(
            c.nodes[0]
                .control()
                .propose(ObjectCommand::Chunk {
                    key,
                    owner,
                    offset: (index * CHUNK_BYTES) as u32,
                    bytes: chunk.to_vec(),
                })
                .unwrap(),
        );
    }
    c.pump();
    for proposal in proposals {
        assert!(c.result(&proposal).is_ok());
    }
    assert_eq!(
        c.command(ObjectCommand::Seal { key, owner }),
        Ok(Change::Sealed)
    );
    let read = c.nodes[0].control().request_read().unwrap();
    assert!(matches!(
        c.nodes[0].control().read_object(&read, key).unwrap(),
        Read::Pending
    ));
    c.pump();
    let Read::Ready(Some(object)) = c.nodes[0].control().read_object(&read, key).unwrap() else {
        panic!("sealed object not readable");
    };
    assert_eq!(object.bytes(), bytes);
}

#[test]
fn membership_entries_advance_business_scan_without_decoding_as_object_commands() {
    let mut c = Cluster::new();
    c.ready();
    let key = [31; 16];
    let owner = [32; 16];
    let bytes = b"committed before learner joins".to_vec();
    for command in [
        ObjectCommand::Begin(ObjectSpec {
            key,
            owner,
            purpose: Purpose::Candidate,
            length: bytes.len() as u32,
            hash: Sha256::digest(&bytes).into(),
        }),
        ObjectCommand::Chunk {
            key,
            owner,
            offset: 0,
            bytes: bytes.clone(),
        },
        ObjectCommand::Seal { key, owner },
    ] {
        assert!(c.command(command).is_ok());
    }
    c.nodes.push(
        Replica::new(
            Node::create_joining(FileStore::open(c.path.join("4")).unwrap(), raft_config(4))
                .unwrap(),
            service_config(),
        )
        .unwrap(),
    );
    for (index, node) in c.nodes.iter_mut().enumerate() {
        for peer in 1..=4 {
            if index as u64 + 1 != peer {
                PeerDriver::peer_session(node, peer, Some([peer as u8; 16])).unwrap();
            }
        }
    }
    let mut receipts = Vec::new();
    for (operation, action) in [
        (33, MembershipAction::AddLearner { node_id: 4 }),
        (
            34,
            MembershipAction::ReplaceVoter {
                remove: 3,
                promote: 4,
                caught_up_through: 0,
            },
        ),
        (35, MembershipAction::LeaveJoint),
    ] {
        let action = match action {
            MembershipAction::ReplaceVoter {
                remove, promote, ..
            } => MembershipAction::ReplaceVoter {
                remove,
                promote,
                caught_up_through: c.nodes[0].status().committed,
            },
            other => other,
        };
        let request = MembershipRequest {
            operation: [operation; 16],
            expected_membership: c.nodes[0].current_membership().digest(),
            action,
        };
        let before = c.applied.iter().filter(|(id, _)| *id == 0).count();
        assert!(matches!(
            c.nodes[0]
                .control()
                .propose_membership(request.clone())
                .unwrap(),
            MembershipProposal::Pending(_)
        ));
        c.pump();
        let receipt = c.nodes[0]
            .control()
            .membership_receipt(request.operation)
            .unwrap()
            .unwrap()
            .clone();
        assert_eq!(
            c.applied.iter().filter(|(id, _)| *id == 0).count(),
            before,
            "membership is not an object command or object rejection"
        );
        assert_eq!(
            c.nodes[0].control().propose_membership(request).unwrap(),
            MembershipProposal::Applied(receipt.clone())
        );
        receipts.push(receipt);
        for node in &c.nodes {
            assert!(!node.status().poisoned);
            assert_eq!(node.status().raft_applied, node.scanned_through());
        }
    }
    assert_eq!(c.nodes[0].current_membership().voters(), &[1, 2, 4]);
    assert_eq!(c.nodes[0].current_membership().retired(), &[3]);
    c.nodes.clear();
    for id in 1..=4 {
        let node = Replica::new(
            Node::recover(
                FileStore::open(c.path.join(id.to_string())).unwrap(),
                raft_config(id),
            )
            .unwrap(),
            service_config(),
        )
        .unwrap();
        assert_eq!(node.status().raft_applied, node.scanned_through());
        c.nodes.push(node);
    }
    for receipt in &receipts {
        assert_eq!(
            c.nodes[0]
                .control()
                .membership_receipt(receipt.request().operation)
                .unwrap(),
            Some(receipt)
        );
    }
    c.nodes[3].campaign().unwrap();
    c.pump();
    let ticket = c.nodes[3].control().request_read().unwrap();
    c.pump();
    let Read::Ready(Some(object)) = c.nodes[3].control().read_object(&ticket, key).unwrap() else {
        panic!("promoted and reopened learner lost prior business state");
    };
    assert_eq!(object.bytes(), bytes);
}
