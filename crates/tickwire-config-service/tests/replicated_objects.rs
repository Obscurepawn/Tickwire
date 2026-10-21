use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use tickwire_config_service::*;
use tickwire_control_raft::{Node, Proposal};
use tickwire_journal::store::FileStore;

static NEXT: AtomicU64 = AtomicU64::new(1);
const OWNER: Identity = [7; 16];
const KEY: ObjectKey = [6; 16];
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "tickwire-config-objects-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn open(&self, id: u64) -> FileStore {
        FileStore::open(self.0.join(id.to_string())).unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn raft_config(id: u64) -> tickwire_control_raft::Config {
    tickwire_control_raft::Config {
        physical: tickwire_journal::Config::new([id as u8; 16], [18; 16]),
        node_id: id,
        voters: [1, 2, 3],
        writer_epoch: 1,
        max_command_bytes: MAX_COMMAND_BYTES,
        max_retained_bytes: 128 * 1024 * 1024,
    }
}
fn config() -> Config {
    Config {
        cluster: [19; 16],
        objects: ObjectLimits {
            max_objects: 16,
            max_inflight: 4,
            max_payload_bytes: 32 * 1024 * 1024,
        },
    }
}
struct Cluster {
    dir: Directory,
    config: Config,
    nodes: Vec<Replica<FileStore>>,
    results: Vec<(usize, AppliedResult)>,
}
impl Cluster {
    fn new(config: Config) -> Self {
        let dir = Directory::new();
        let nodes = (1..=3)
            .map(|id| {
                Replica::new(Node::create(dir.open(id), raft_config(id)).unwrap(), config).unwrap()
            })
            .collect();
        let mut out = Self {
            dir,
            config,
            nodes,
            results: Vec::new(),
        };
        out.elect(0);
        let proposal = out.nodes[0].initialize().unwrap();
        out.pump(None);
        assert_eq!(out.result(0, &proposal), Ok(Change::Initialized));
        out
    }
    fn pump(&mut self, isolated: Option<u64>) {
        for _ in 0..128 {
            let mut messages = VecDeque::new();
            for (id, node) in self.nodes.iter_mut().enumerate() {
                let output = node.drive().unwrap();
                messages.extend(output.messages);
                self.results
                    .extend(output.applied.into_iter().map(|result| (id, result)));
            }
            if messages.is_empty() {
                return;
            }
            for message in messages {
                if isolated.is_some_and(|id| message.from == id || message.to == id) {
                    continue;
                }
                self.nodes[(message.to - 1) as usize]
                    .step(message.from, message)
                    .unwrap();
            }
        }
        panic!("raft did not settle");
    }
    fn elect(&mut self, id: usize) {
        self.nodes[id].campaign().unwrap();
        self.pump(None);
        assert!(self.nodes[id].is_leader());
    }
    fn result(&self, node: usize, proposal: &Proposal) -> Result<Change> {
        self.results
            .iter()
            .find(|(id, r)| *id == node && r.matches(proposal))
            .expect("proposal did not commit")
            .1
            .result()
    }
    fn command(&mut self, command: ObjectCommand) -> Result<Change> {
        let proposal = self.nodes[0].propose(command).unwrap();
        self.pump(None);
        self.result(0, &proposal)
    }
    fn read(&mut self, node: usize, key: ObjectKey) -> Option<Object> {
        let read = self.nodes[node].request_read().unwrap();
        self.pump(None);
        match self.nodes[node].read_object(&read, key).unwrap() {
            Read::Ready(object) => object,
            Read::Pending => panic!("read did not complete"),
        }
    }
    fn restart(&mut self) {
        self.nodes.clear();
        self.results.clear();
        self.nodes = (1..=3)
            .map(|id| {
                Replica::new(
                    Node::recover(self.dir.open(id), raft_config(id)).unwrap(),
                    self.config,
                )
                .unwrap()
            })
            .collect();
    }
}
fn spec(bytes: &[u8]) -> ObjectSpec {
    ObjectSpec {
        key: KEY,
        owner: OWNER,
        purpose: Purpose::Candidate,
        length: bytes.len() as u32,
        hash: Sha256::digest(bytes).into(),
    }
}
fn chunk(offset: usize, bytes: &[u8]) -> ObjectCommand {
    ObjectCommand::Chunk {
        key: KEY,
        owner: OWNER,
        offset: offset as u32,
        bytes: bytes.to_vec(),
    }
}
fn seal() -> ObjectCommand {
    ObjectCommand::Seal {
        key: KEY,
        owner: OWNER,
    }
}
#[test]
fn full_sized_candidate_crosses_small_commands_and_recovers_on_every_voter() {
    let mut cluster = Cluster::new(config());
    let bytes: Vec<_> = (0..16 * 1024 * 1024).map(|n| (n % 251) as u8).collect();
    assert_eq!(
        cluster.command(ObjectCommand::Begin(spec(&bytes))),
        Ok(Change::Begun)
    );
    for (n, bytes) in bytes.chunks(CHUNK_BYTES).enumerate() {
        assert_eq!(
            cluster.command(chunk(n * CHUNK_BYTES, bytes)),
            Ok(Change::ChunkStored {
                received: ((n + 1) * CHUNK_BYTES) as u32
            })
        );
    }
    assert!(cluster.read(0, KEY).is_none());
    assert_eq!(cluster.command(seal()), Ok(Change::Sealed));
    assert_eq!(cluster.read(0, KEY).unwrap().bytes(), bytes);
    for id in 0..3 {
        cluster.restart();
        cluster.elect(id);
        let object = cluster.read(id, KEY).unwrap();
        assert_eq!(object.bytes(), bytes);
        assert_eq!(object.spec(), &spec(&bytes));
    }
}
#[test]
fn missing_conflicting_and_wrong_owner_chunks_never_publish_an_object() {
    let mut cluster = Cluster::new(config());
    let bytes = vec![3; CHUNK_BYTES + 9];
    assert_eq!(
        cluster.command(ObjectCommand::Begin(spec(&bytes))),
        Ok(Change::Begun)
    );
    assert_eq!(cluster.command(seal()), Err(Error::Incomplete));
    assert_eq!(
        cluster.command(chunk(CHUNK_BYTES, &bytes[CHUNK_BYTES..])),
        Err(Error::Incomplete)
    );
    assert_eq!(
        cluster.command(ObjectCommand::Chunk {
            key: KEY,
            owner: [8; 16],
            offset: 0,
            bytes: bytes[..CHUNK_BYTES].to_vec(),
        }),
        Err(Error::Identity)
    );
    assert_eq!(
        cluster.command(chunk(0, &bytes[..CHUNK_BYTES])),
        Ok(Change::ChunkStored {
            received: CHUNK_BYTES as u32
        })
    );
    assert_eq!(
        cluster.command(chunk(0, &bytes[..CHUNK_BYTES])),
        Ok(Change::ChunkStored {
            received: CHUNK_BYTES as u32
        })
    );
    assert_eq!(
        cluster.command(chunk(0, &vec![4; CHUNK_BYTES])),
        Err(Error::Conflict)
    );
    cluster.restart();
    cluster.elect(0);
    assert!(cluster.read(0, KEY).is_none());
    assert_eq!(
        cluster.command(chunk(CHUNK_BYTES, &bytes[CHUNK_BYTES..])),
        Ok(Change::ChunkStored {
            received: bytes.len() as u32
        })
    );
    assert_eq!(cluster.command(seal()), Ok(Change::Sealed));
    assert_eq!(cluster.command(seal()), Ok(Change::Sealed));
    assert_eq!(
        cluster.command(ObjectCommand::Begin(spec(&bytes))),
        Ok(Change::Begun)
    );
    assert_eq!(cluster.read(0, KEY).unwrap().bytes(), bytes);
}
#[test]
fn digest_and_purpose_binding_are_checked_without_replacing_previous_bytes() {
    let mut cluster = Cluster::new(config());
    let bytes = b"candidate";
    cluster.command(ObjectCommand::Begin(spec(bytes))).unwrap();
    let mut conflict = spec(bytes);
    conflict.purpose = Purpose::Receipt;
    assert_eq!(
        cluster.command(ObjectCommand::Begin(conflict)),
        Err(Error::Conflict)
    );
    cluster.command(chunk(0, b"incorrect")).unwrap();
    assert_eq!(cluster.command(seal()), Err(Error::Digest));
    assert!(cluster.read(0, KEY).is_none());
    cluster.restart();
    cluster.elect(0);
    assert_eq!(cluster.command(seal()), Err(Error::Digest));
    assert_eq!(cluster.command(chunk(0, bytes)), Err(Error::Conflict));
}
#[test]
fn begin_reserves_bounded_capacity_and_exact_retries_do_not_consume_it_twice() {
    let mut cfg = config();
    cfg.objects.max_inflight = 1;
    cfg.objects.max_payload_bytes = 10;
    let mut cluster = Cluster::new(cfg);
    let bytes = b"0123456789";
    cluster.command(ObjectCommand::Begin(spec(bytes))).unwrap();
    cluster.command(ObjectCommand::Begin(spec(bytes))).unwrap();
    let mut other = spec(b"1");
    other.key = [2; 16];
    assert_eq!(
        cluster.command(ObjectCommand::Begin(other.clone())),
        Err(Error::Capacity)
    );
    cluster.command(chunk(0, bytes)).unwrap();
    cluster.command(seal()).unwrap();
    assert_eq!(
        cluster.command(ObjectCommand::Begin(other)),
        Err(Error::Capacity)
    );
    assert_eq!(cluster.read(0, KEY).unwrap().bytes(), bytes);
}
#[test]
fn pending_proposal_is_not_a_commit_and_safe_reads_require_a_majority() {
    let mut cluster = Cluster::new(config());
    let proposal = cluster.nodes[0]
        .propose(ObjectCommand::Begin(spec(b"pending")))
        .unwrap();
    let read = cluster.nodes[0].request_read().unwrap();
    cluster.pump(Some(1));
    assert!(cluster.results.iter().all(|(_, r)| !r.matches(&proposal)));
    assert!(matches!(
        cluster.nodes[0].read_object(&read, KEY).unwrap(),
        Read::Pending
    ));
    cluster.pump(None);
    for _ in 0..10 {
        cluster.nodes[0].tick().unwrap();
        cluster.pump(None);
    }
    assert_eq!(cluster.result(0, &proposal), Ok(Change::Begun));
    assert!(cluster.read(0, KEY).is_none());
}
#[test]
fn new_leader_noop_and_recovery_advance_the_complete_scanned_watermark() {
    let mut cluster = Cluster::new(config());
    let before = cluster.nodes[0].scanned_through();
    cluster.restart();
    assert!(cluster.nodes.iter().all(|n| n.scanned_through() >= before));
    cluster.elect(1);
    assert!(cluster.nodes[1].scanned_through() > before);
    assert!(cluster.read(1, KEY).is_none());
}
#[test]
fn replicated_limits_are_required_unchanged_on_recovery() {
    let mut cluster = Cluster::new(config());
    cluster.nodes.clear();
    let mut altered = config();
    altered.objects.max_objects += 1;
    let node = Node::recover(cluster.dir.open(1), raft_config(1)).unwrap();
    assert!(matches!(Replica::new(node, altered), Err(Error::Identity)));
}
