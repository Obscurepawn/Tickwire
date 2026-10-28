use super::*;
use crate::{
    AppliedResult, CHUNK_BYTES, MAX_COMMAND_BYTES, ObjectCommand, Read, Replica,
    bootstrap::{Bootstrap, BootstrapTrust, HeadRead},
    plan_archive::content::tests::{fixture, fixture_at_position},
};
use ring::signature::{Ed25519KeyPair, KeyPair};
use sha2::{Digest, Sha256};
use std::{collections::VecDeque, path::PathBuf};
use tickwire_control_raft::{
    Node, Proposal,
    generation::{GenerationStore, SnapshotDirectory},
    snapshot::SnapshotLimits,
};

const CLUSTER: Identity = [19; 16];
const GROUP: Identity = [18; 16];
fn config() -> crate::Config {
    crate::Config {
        cluster: CLUSTER,
        objects: crate::ObjectLimits {
            max_objects: 4096,
            max_inflight: 64,
            max_payload_bytes: 512 * 1024 * 1024,
        },
    }
}
fn signer() -> Ed25519KeyPair {
    Ed25519KeyPair::from_seed_unchecked(&[31; 32]).unwrap()
}
fn trust() -> BootstrapTrust {
    BootstrapTrust::new(
        CLUSTER,
        [0, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 1],
        "tickwire-qualification".into(),
        signer().public_key().as_ref().try_into().unwrap(),
    )
    .unwrap()
}
fn raft_config(id: u64) -> tickwire_control_raft::Config {
    tickwire_control_raft::Config {
        physical: tickwire_journal::Config::new([id as u8; 16], GROUP),
        node_id: id,
        voters: [1, 2, 3],
        writer_epoch: 1,
        max_command_bytes: MAX_COMMAND_BYTES,
        max_retained_bytes: 32 * 1024 * 1024,
    }
}
struct Cluster {
    directory: PathBuf,
    nodes: Vec<Replica<GenerationStore>>,
    results: Vec<AppliedResult>,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.nodes.clear();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}
impl Cluster {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "tickwire-prepared-generation-{}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        let mut cluster = Self {
            directory,
            nodes: Vec::new(),
            results: Vec::new(),
        };
        for id in 1..=3 {
            let node = Node::create_in(cluster.directory(id), raft_config(id)).unwrap();
            cluster
                .nodes
                .push(Replica::new_with_bootstrap(node, config(), trust()).unwrap());
        }
        cluster.nodes[0].campaign().unwrap();
        cluster.pump();
        let proposal = cluster.nodes[0].initialize().unwrap();
        assert_eq!(cluster.applied(&proposal), Change::Initialized);
        cluster
    }
    fn directory(&self, id: u64) -> SnapshotDirectory {
        SnapshotDirectory::open(
            self.directory.join(id.to_string()),
            GROUP,
            id,
            SnapshotLimits::default(),
        )
        .unwrap()
    }
    fn pump(&mut self) {
        for _ in 0..128 {
            let mut messages = VecDeque::new();
            for (index, node) in self.nodes.iter_mut().enumerate() {
                let output = node.drive().unwrap();
                messages.extend(output.messages);
                if index == 0 {
                    self.results.extend(output.applied);
                }
            }
            if messages.is_empty() {
                return;
            }
            for message in messages {
                self.nodes[(message.to - 1) as usize]
                    .step(message.from, message)
                    .unwrap();
            }
        }
        panic!("generation cluster did not settle");
    }
    fn applied(&mut self, proposal: &Proposal) -> Change {
        self.pump();
        self.results
            .iter()
            .find(|result| result.matches(proposal))
            .unwrap()
            .result()
            .unwrap()
    }
    fn command(&mut self, command: ObjectCommand) {
        let proposal = self.nodes[0].propose(command).unwrap();
        self.applied(&proposal);
    }
    fn store(&mut self, spec: &ObjectSpec, bytes: &[u8]) {
        self.command(ObjectCommand::Begin(spec.clone()));
        for (index, chunk) in bytes.chunks(CHUNK_BYTES).enumerate() {
            self.command(ObjectCommand::Chunk {
                key: spec.key,
                owner: spec.owner,
                offset: (index * CHUNK_BYTES) as u32,
                bytes: chunk.to_vec(),
            });
        }
        self.command(ObjectCommand::Seal {
            key: spec.key,
            owner: spec.owner,
        });
    }
    fn record(&mut self, record: &RecordPrepared) -> Change {
        let proposal = self.nodes[0]
            .node
            .propose(crate::codec::record_prepared(CLUSTER, record).unwrap())
            .unwrap();
        self.applied(&proposal)
    }
    fn head(&mut self) -> CommittedHead {
        let ticket = self.nodes[0].request_read().unwrap();
        self.pump();
        let HeadRead::Ready(Some(head)) = self.nodes[0].read_head(&ticket).unwrap() else {
            panic!("head not available after actual ReadIndex");
        };
        *head
    }
    fn stored(&mut self, id: Identity) -> StoredPrepared {
        let ticket = self.nodes[0].request_read().unwrap();
        self.pump();
        let PreparedRead::Ready(view) = self.nodes[0]
            .read_prepared(&ticket, PreparedQuery::Id(id))
            .unwrap()
        else {
            panic!("archive not available after actual ReadIndex");
        };
        assert_eq!(view.head().unwrap().revision(), 1);
        view.record().unwrap().clone()
    }
}

#[test]
fn nonempty_prepared_archive_compacts_and_recovers_actual_generation_with_durable_tail() {
    let initial = fixture();
    let mut cluster = Cluster::new();
    let bytes = initial.head.candidate().canonical_bytes();
    let spec = ObjectSpec {
        key: [99; 16],
        owner: [98; 16],
        purpose: Purpose::Candidate,
        length: bytes.len() as u32,
        hash: Sha256::digest(bytes).into(),
    };
    cluster.store(&spec, bytes);
    let nonce = [10; 16];
    let command = Bootstrap {
        candidate: spec.key,
        candidate_hash: spec.hash,
        nonce,
        signature: signer()
            .sign(&trust().signing_bytes(spec.key, spec.hash, nonce).unwrap())
            .as_ref()
            .try_into()
            .unwrap(),
    };
    let proposal = cluster.nodes[0].bootstrap(command).unwrap();
    assert_eq!(
        cluster.applied(&proposal),
        Change::Bootstrapped { revision: 1 }
    );
    let actual_head = cluster.head();
    let fixture = fixture_at_position(actual_head.raft_index(), actual_head.raft_term());
    assert_eq!(fixture.head, actual_head);
    for object in fixture.archive.objects() {
        cluster.store(object.spec(), object.bytes());
    }
    let first = cluster.record(&fixture.record);
    let stored = cluster.stored(fixture.record.id);
    assert_eq!(stored.command(), &fixture.record);
    let installed = cluster.nodes[0].compact_snapshot().unwrap();
    assert_eq!(installed.application().schema(), 2);
    assert!(installed.cut().index() >= stored.index());
    assert_eq!(cluster.record(&fixture.record), first);
    let tail = b"sealed after the actual snapshot";
    let tail_spec = ObjectSpec {
        key: [100; 16],
        owner: [98; 16],
        purpose: Purpose::Qualification,
        length: tail.len() as u32,
        hash: Sha256::digest(tail).into(),
    };
    cluster.store(&tail_spec, tail);
    cluster.nodes.clear();
    cluster.results.clear();
    let node = Node::recover_in(cluster.directory(1), raft_config(1)).unwrap();
    assert!(matches!(
        Replica::new_with_bootstrap(node, config(), trust()),
        Err(Error::NotReady)
    ));
    for id in 1..=3 {
        let node = Node::recover_in(cluster.directory(id), raft_config(id)).unwrap();
        cluster
            .nodes
            .push(Replica::recover_generation(node, config(), Some(trust())).unwrap());
    }
    cluster.nodes[0].campaign().unwrap();
    cluster.pump();
    assert_eq!(cluster.head(), actual_head);
    assert_eq!(cluster.stored(fixture.record.id), stored);
    let ticket = cluster.nodes[0].request_read().unwrap();
    cluster.pump();
    let Read::Ready(Some(object)) = cluster.nodes[0]
        .read_object(&ticket, tail_spec.key)
        .unwrap()
    else {
        panic!("durable tail was not replayed");
    };
    assert_eq!(object.bytes(), tail);
    assert_eq!(cluster.record(&fixture.record), first);
    assert_eq!(cluster.stored(fixture.record.id), stored);
}
