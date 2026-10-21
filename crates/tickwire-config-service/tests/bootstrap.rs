use ring::signature::{Ed25519KeyPair, KeyPair};
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};
use tickwire_config::{Candidate, Format, Limits};
use tickwire_config_service::{bootstrap::*, *};
use tickwire_control_raft::snapshot::{
    ApplicationRoot, MemoryObjects, ObjectRef, SnapshotApplication, SnapshotLimits, SnapshotReader,
    SnapshotWriter,
};
use tickwire_control_raft::{Node, Proposal};
use tickwire_journal::store::FileStore;

const CLUSTER: Identity = [19; 16];
const KEY: Identity = [6; 16];
const OWNER: Identity = [7; 16];
static NEXT: AtomicU64 = AtomicU64::new(1);
fn key() -> Ed25519KeyPair {
    Ed25519KeyPair::from_seed_unchecked(&[31; 32]).unwrap()
}
fn trust() -> BootstrapTrust {
    BootstrapTrust::new(
        CLUSTER,
        [0, 0, 0, 0, 0, 0, 64, 0, 128, 0, 0, 0, 0, 0, 0, 1],
        "tickwire-qualification".into(),
        key().public_key().as_ref().try_into().unwrap(),
    )
    .unwrap()
}
fn candidate() -> Candidate {
    static BASE: OnceLock<Candidate> = OnceLock::new();
    BASE.get_or_init(|| {
        Candidate::parse(
            Format::Toml,
            include_bytes!("../../../config/reference.toml"),
            Limits::default(),
        )
        .unwrap()
        .normalized_configuration(Limits::default())
        .unwrap()
    })
    .clone()
}
fn sign_with(
    trust: &BootstrapTrust,
    object: ObjectKey,
    bytes: &[u8],
    nonce: Identity,
) -> Bootstrap {
    let candidate_hash = Sha256::digest(bytes).into();
    Bootstrap {
        candidate: object,
        candidate_hash,
        nonce,
        signature: key()
            .sign(&trust.signing_bytes(object, candidate_hash, nonce).unwrap())
            .as_ref()
            .try_into()
            .unwrap(),
    }
}
fn sign(bytes: &[u8]) -> Bootstrap {
    sign_with(&trust(), KEY, bytes, [10; 16])
}
fn config() -> Config {
    Config {
        cluster: CLUSTER,
        objects: ObjectLimits {
            max_objects: 16,
            max_inflight: 4,
            max_payload_bytes: 32 * 1024 * 1024,
        },
    }
}
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
struct Cluster {
    dir: PathBuf,
    nodes: Vec<Replica<FileStore>>,
    results: Vec<(usize, AppliedResult)>,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.nodes.clear();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
impl Cluster {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "tickwire-config-bootstrap-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        let nodes = (1..=3)
            .map(|id| {
                Replica::new_with_bootstrap(
                    Node::create(
                        FileStore::open(dir.join(id.to_string())).unwrap(),
                        raft_config(id),
                    )
                    .unwrap(),
                    config(),
                    trust(),
                )
                .unwrap()
            })
            .collect();
        let mut out = Self {
            dir,
            nodes,
            results: vec![],
        };
        out.elect(0);
        let proposal = out.nodes[0].initialize().unwrap();
        out.pump(false);
        assert_eq!(out.result(0, &proposal), Ok(Change::Initialized));
        out
    }
    fn pump(&mut self, isolated_leader: bool) {
        for _ in 0..128 {
            let mut messages = VecDeque::new();
            for (id, node) in self.nodes.iter_mut().enumerate() {
                let out = node.drive().unwrap();
                messages.extend(out.messages);
                self.results
                    .extend(out.applied.into_iter().map(|r| (id, r)));
            }
            if messages.is_empty() {
                return;
            }
            for message in messages {
                if isolated_leader && (message.from == 1 || message.to == 1) {
                    continue;
                }
                self.nodes[(message.to - 1) as usize]
                    .step(message.from, message)
                    .unwrap();
            }
        }
        panic!("raft did not settle");
    }
    fn elect(&mut self, node: usize) {
        self.nodes[node].campaign().unwrap();
        self.pump(false);
        assert!(self.nodes[node].is_leader());
    }
    fn result(&self, node: usize, proposal: &Proposal) -> Result<Change> {
        self.results
            .iter()
            .find(|(n, r)| *n == node && r.matches(proposal))
            .unwrap()
            .1
            .result()
    }
    fn object_command(&mut self, command: ObjectCommand) -> Result<Change> {
        let p = self.nodes[0].propose(command).unwrap();
        self.pump(false);
        self.result(0, &p)
    }
    fn store(&mut self, object: ObjectKey, bytes: &[u8], purpose: Purpose, seal: bool) {
        self.object_command(ObjectCommand::Begin(ObjectSpec {
            key: object,
            owner: OWNER,
            purpose,
            length: bytes.len() as u32,
            hash: Sha256::digest(bytes).into(),
        }))
        .unwrap();
        for (i, chunk) in bytes.chunks(CHUNK_BYTES).enumerate() {
            self.object_command(ObjectCommand::Chunk {
                key: object,
                owner: OWNER,
                offset: (i * CHUNK_BYTES) as u32,
                bytes: chunk.to_vec(),
            })
            .unwrap();
        }
        if seal {
            self.object_command(ObjectCommand::Seal {
                key: object,
                owner: OWNER,
            })
            .unwrap();
        }
    }
    fn bootstrap(&mut self, request: Bootstrap) -> Result<Change> {
        let p = self.nodes[0].bootstrap(request).unwrap();
        self.pump(false);
        self.result(0, &p)
    }
    fn head(&mut self, node: usize) -> Option<CommittedHead> {
        let read = self.nodes[node].request_read().unwrap();
        self.pump(false);
        match self.nodes[node].read_head(&read).unwrap() {
            HeadRead::Ready(head) => head.map(|head| *head),
            HeadRead::Pending => panic!("missing safe-read barrier"),
        }
    }
    fn restart(&mut self) {
        self.nodes.clear();
        self.results.clear();
        self.nodes = (1..=3)
            .map(|id| {
                Replica::new_with_bootstrap(
                    Node::recover(
                        FileStore::open(self.dir.join(id.to_string())).unwrap(),
                        raft_config(id),
                    )
                    .unwrap(),
                    config(),
                    trust(),
                )
                .unwrap()
            })
            .collect();
    }
}
#[test]
fn signed_initial_candidate_commits_once_and_recovers_on_each_actual_file_voter() {
    let mut c = Cluster::new();
    let candidate = candidate();
    assert!(c.head(0).is_none());
    c.store(KEY, candidate.canonical_bytes(), Purpose::Candidate, true);
    let request = sign(candidate.canonical_bytes());
    assert_eq!(
        c.bootstrap(request.clone()),
        Ok(Change::Bootstrapped { revision: 1 })
    );
    let original = c.head(0).unwrap();
    assert_eq!(original.candidate(), &candidate);
    assert_eq!(original.revision(), 1);
    assert!(original.raft_index() > 0);
    assert!(original.raft_term() > 0);
    assert!(original.etag().starts_with("\"cfg-1-"));
    assert_eq!(
        c.bootstrap(request),
        Ok(Change::Bootstrapped { revision: 1 })
    );
    assert_eq!(c.head(0).unwrap(), original);
    for node in 0..3 {
        c.restart();
        c.elect(node);
        assert_eq!(c.head(node).unwrap(), original);
    }
}
#[test]
fn pending_bootstrap_requires_a_durable_majority_and_safe_read_barrier() {
    let mut c = Cluster::new();
    let candidate = candidate();
    c.store(KEY, candidate.canonical_bytes(), Purpose::Candidate, true);
    let p = c.nodes[0]
        .bootstrap(sign(candidate.canonical_bytes()))
        .unwrap();
    let read = c.nodes[0].request_read().unwrap();
    c.pump(true);
    assert!(c.results.iter().all(|(_, r)| !r.matches(&p)));
    assert!(matches!(
        c.nodes[0].read_head(&read).unwrap(),
        HeadRead::Pending
    ));
    for _ in 0..10 {
        c.nodes[0].tick().unwrap();
        c.pump(false);
    }
    assert_eq!(c.result(0, &p), Ok(Change::Bootstrapped { revision: 1 }));
    assert_eq!(c.head(0).unwrap().candidate(), &candidate);
}
#[test]
fn bad_signature_wrong_domain_and_content_substitution_cannot_create_a_head() {
    let mut c = Cluster::new();
    let candidate = candidate();
    c.store(KEY, candidate.canonical_bytes(), Purpose::Candidate, true);
    let mut bad = sign(candidate.canonical_bytes());
    bad.signature[0] ^= 1;
    assert_eq!(c.bootstrap(bad), Err(Error::Authentication));
    let other = BootstrapTrust::new(
        [20; 16],
        [21; 16],
        "other".into(),
        key().public_key().as_ref().try_into().unwrap(),
    )
    .unwrap();
    assert_eq!(
        c.bootstrap(sign_with(
            &other,
            KEY,
            candidate.canonical_bytes(),
            [10; 16]
        )),
        Err(Error::Authentication)
    );
    let bad_hash = sign(b"another candidate");
    assert_eq!(c.bootstrap(bad_hash), Err(Error::Digest));
    assert!(c.head(0).is_none());
    assert_eq!(
        c.bootstrap(sign(candidate.canonical_bytes())),
        Ok(Change::Bootstrapped { revision: 1 })
    );
}
#[test]
fn missing_unsealed_wrong_purpose_and_noncanonical_objects_never_become_head() {
    let mut c = Cluster::new();
    let candidate = candidate();
    assert_eq!(
        c.bootstrap(sign(candidate.canonical_bytes())),
        Err(Error::NotFound)
    );
    c.store(KEY, candidate.canonical_bytes(), Purpose::Candidate, false);
    assert_eq!(
        c.bootstrap(sign(candidate.canonical_bytes())),
        Err(Error::NotFound)
    );
    c.object_command(ObjectCommand::Seal {
        key: KEY,
        owner: OWNER,
    })
    .unwrap();
    c.store(
        [11; 16],
        candidate.canonical_bytes(),
        Purpose::Receipt,
        true,
    );
    assert_eq!(
        c.bootstrap(sign_with(
            &trust(),
            [11; 16],
            candidate.canonical_bytes(),
            [10; 16]
        )),
        Err(Error::Identity)
    );
    let pretty = serde_json::to_vec_pretty(candidate.value()).unwrap();
    c.store([12; 16], &pretty, Purpose::Candidate, true);
    assert_eq!(
        c.bootstrap(sign_with(&trust(), [12; 16], &pretty, [10; 16])),
        Err(Error::Digest)
    );
    c.store([13; 16], b"{}", Purpose::Candidate, true);
    assert!(matches!(
        c.bootstrap(sign_with(&trust(), [13; 16], b"{}", [10; 16])),
        Err(Error::Configuration(_))
    ));
    assert!(c.head(0).is_none());
}
#[test]
fn signed_candidate_must_match_cluster_namespace_and_actual_voter_membership() {
    for (field, value) in [
        ("cluster_id", serde_json::json!("other")),
        (
            "namespace_id",
            serde_json::json!("00000000-0000-4000-8000-000000000002"),
        ),
        ("voter", serde_json::json!(4)),
    ] {
        let mut c = Cluster::new();
        let mut value_doc = candidate().value().clone();
        if field == "voter" {
            value_doc["control"]["voters"][2]["node_id"] = value;
        } else {
            value_doc["identity"][field] = value;
        }
        let changed = Candidate::parse(
            Format::Json,
            &serde_json::to_vec(&value_doc).unwrap(),
            Limits::default(),
        )
        .unwrap();
        changed.validate_static().unwrap();
        c.store(KEY, changed.canonical_bytes(), Purpose::Candidate, true);
        assert_eq!(
            c.bootstrap(sign(changed.canonical_bytes())),
            Err(Error::Identity)
        );
        assert!(c.head(0).is_none());
    }
}
#[test]
fn a_second_valid_bootstrap_cannot_replace_the_first_desired_or_reuse_its_revision() {
    let mut c = Cluster::new();
    let candidate = candidate();
    c.store(KEY, candidate.canonical_bytes(), Purpose::Candidate, true);
    c.bootstrap(sign(candidate.canonical_bytes())).unwrap();
    let first = c.head(0).unwrap();
    assert_eq!(
        c.bootstrap(sign_with(
            &trust(),
            KEY,
            candidate.canonical_bytes(),
            [11; 16]
        )),
        Err(Error::Conflict)
    );
    c.store(
        [12; 16],
        candidate.canonical_bytes(),
        Purpose::Candidate,
        true,
    );
    assert_eq!(
        c.bootstrap(sign_with(
            &trust(),
            [12; 16],
            candidate.canonical_bytes(),
            [10; 16]
        )),
        Err(Error::Conflict)
    );
    assert_eq!(c.head(0).unwrap(), first);
}
#[test]
fn recovery_rejects_a_changed_or_removed_offline_trust_anchor() {
    let mut c = Cluster::new();
    c.nodes.clear();
    let other = BootstrapTrust::new(
        CLUSTER,
        [21; 16],
        "other".into(),
        key().public_key().as_ref().try_into().unwrap(),
    )
    .unwrap();
    let node = Node::recover(FileStore::open(c.dir.join("1")).unwrap(), raft_config(1)).unwrap();
    assert!(matches!(
        Replica::new_with_bootstrap(node, config(), other),
        Err(Error::Identity)
    ));
    let node = Node::recover(FileStore::open(c.dir.join("1")).unwrap(), raft_config(1)).unwrap();
    assert!(matches!(Replica::new(node, config()), Err(Error::Identity)));
}
#[test]
fn signed_initial_candidate_must_already_contain_explicit_schema_defaults() {
    let mut c = Cluster::new();
    let raw = Candidate::parse(
        Format::Toml,
        include_bytes!("../../../config/reference.toml"),
        Limits::default(),
    )
    .unwrap();
    c.store(KEY, raw.canonical_bytes(), Purpose::Candidate, true);
    assert_eq!(c.bootstrap(sign(raw.canonical_bytes())), Err(Error::Digest));
    assert!(c.head(0).is_none());
}

fn snapshot_fixture() -> Cluster {
    let mut c = Cluster::new();
    let candidate = candidate();
    c.store(KEY, candidate.canonical_bytes(), Purpose::Candidate, true);
    c.bootstrap(sign(candidate.canonical_bytes())).unwrap();
    let pending = ObjectSpec {
        key: [9; 16],
        owner: OWNER,
        purpose: Purpose::Qualification,
        length: (CHUNK_BYTES + 7) as u32,
        hash: [13; 32],
    };
    c.object_command(ObjectCommand::Begin(pending)).unwrap();
    c.object_command(ObjectCommand::Chunk {
        key: [9; 16],
        owner: OWNER,
        offset: 0,
        bytes: vec![23; CHUNK_BYTES],
    })
    .unwrap();
    c
}

#[test]
fn snapshot_preparation_preserves_signed_head_and_sealed_and_pending_objects() {
    let mut c = snapshot_fixture();
    let original = c.head(0).unwrap();
    let mut objects = MemoryObjects::new(SnapshotLimits::default()).unwrap();
    let (cut, root) = c.nodes[0].capture_snapshot(&mut objects).unwrap();
    assert_eq!(cut.index(), c.nodes[0].scanned_through());
    assert!(cut.index() > original.raft_index());
    for node in &c.nodes {
        let prepared = node.prepare_snapshot(&cut, &root, &objects).unwrap();
        assert_eq!(prepared.head().unwrap(), original);
        assert_eq!(
            prepared.object(KEY).unwrap().bytes(),
            candidate().canonical_bytes()
        );
        assert!(prepared.object([9; 16]).is_none());
        assert_eq!(prepared.cut(), &cut);
        assert_eq!(prepared.root(), &root);
    }
    assert_eq!(c.head(0).unwrap(), original);
    // Preparation must not install the isolated state or consume upload progress.
    assert_eq!(
        c.object_command(ObjectCommand::Chunk {
            key: [9; 16],
            owner: OWNER,
            offset: 0,
            bytes: vec![23; CHUNK_BYTES],
        }),
        Ok(Change::ChunkStored {
            received: CHUNK_BYTES as u32
        })
    );
}

struct BrokenReader<'a> {
    inner: &'a MemoryObjects,
    root: ObjectRef,
    corrupt: bool,
}
impl SnapshotReader for BrokenReader<'_> {
    fn read(&self, object: &ObjectRef) -> tickwire_control_raft::Result<Vec<u8>> {
        if *object == self.root {
            return self.inner.read(object);
        }
        if !self.corrupt {
            return Err(tickwire_control_raft::Error::Corrupt);
        }
        let mut bytes = self.inner.read(object)?;
        bytes[0] ^= 1;
        Ok(bytes)
    }
}
#[test]
fn snapshot_prepare_rechecks_reader_content_and_rejects_missing_dependencies() {
    let c = snapshot_fixture();
    let mut objects = MemoryObjects::new(SnapshotLimits::default()).unwrap();
    let (cut, root) = c.nodes[0].capture_snapshot(&mut objects).unwrap();
    for corrupt in [false, true] {
        let reader = BrokenReader {
            inner: &objects,
            root: root.root(),
            corrupt,
        };
        assert!(c.nodes[1].prepare_snapshot(&cut, &root, &reader).is_err());
    }
    let root_bytes = objects.read(&root.root()).unwrap();
    for mutate in [0, 1, 2, 3] {
        let mut bytes = root_bytes.clone();
        match mutate {
            0 => bytes.push(0),
            1 => bytes[40..48].copy_from_slice(&0u64.to_le_bytes()),
            2 => bytes[104] = 2,
            3 => {
                let a = bytes[142..216].to_vec();
                let b = bytes[216..290].to_vec();
                bytes[142..216].copy_from_slice(&b);
                bytes[216..290].copy_from_slice(&a);
            }
            _ => unreachable!(),
        }
        let altered = ApplicationRoot::new(1, objects.put(&bytes).unwrap()).unwrap();
        assert!(
            c.nodes[1]
                .prepare_snapshot(&cut, &altered, &objects)
                .is_err()
        );
    }
    let unknown = ApplicationRoot::new(2, root.root()).unwrap();
    assert!(
        c.nodes[1]
            .prepare_snapshot(&cut, &unknown, &objects)
            .is_err()
    );
}
#[test]
fn snapshot_bindings_reject_other_trust_configuration_and_raft_group() {
    let c = snapshot_fixture();
    let mut objects = MemoryObjects::new(SnapshotLimits::default()).unwrap();
    let (cut, root) = c.nodes[0].capture_snapshot(&mut objects).unwrap();
    for kind in 0..3 {
        let mut cfg = config();
        let mut raft = raft_config(1);
        let mut trusted = trust();
        match kind {
            0 => cfg.objects.max_objects = 15,
            1 => {
                trusted = BootstrapTrust::new(
                    CLUSTER,
                    [21; 16],
                    "other".into(),
                    key().public_key().as_ref().try_into().unwrap(),
                )
                .unwrap()
            }
            2 => raft.physical = tickwire_journal::Config::new([1; 16], [20; 16]),
            _ => unreachable!(),
        }
        let node = Node::create(
            FileStore::open(c.dir.join(format!("receiver-{kind}"))).unwrap(),
            raft,
        )
        .unwrap();
        let replica = Replica::new_with_bootstrap(node, cfg, trusted).unwrap();
        assert!(replica.prepare_snapshot(&cut, &root, &objects).is_err());
    }
}
#[test]
fn initialized_without_head_has_independent_literal_snapshot_header() {
    let c = Cluster::new();
    let mut objects = MemoryObjects::new(SnapshotLimits::default()).unwrap();
    let (cut, root) = c.nodes[0].capture_snapshot(&mut objects).unwrap();
    let bytes = objects.read(&root.root()).unwrap();
    let mut expected = b"TWCS0001".to_vec();
    expected.extend_from_slice(&CLUSTER);
    expected.extend_from_slice(&[18; 16]);
    expected.extend_from_slice(&cut.index().to_le_bytes());
    expected.extend_from_slice(&cut.term().to_le_bytes());
    expected.extend_from_slice(&cut.membership().digest());
    expected.extend_from_slice(&16u32.to_le_bytes());
    expected.extend_from_slice(&4u32.to_le_bytes());
    expected.extend_from_slice(&(32u64 * 1024 * 1024).to_le_bytes());
    expected.extend_from_slice(&[1, 1]);
    expected.extend_from_slice(&trust().digest());
    expected.extend_from_slice(&0u32.to_le_bytes());
    expected.extend_from_slice(&0u32.to_le_bytes());
    expected.push(0);
    assert_eq!(bytes, expected);
    let prepared = c.nodes[1].prepare_snapshot(&cut, &root, &objects).unwrap();
    assert!(prepared.head().is_none());
    assert_eq!(objects.objects(), vec![root.root()]);
}

use tickwire_control_raft::generation::{GenerationStore, SnapshotDirectory};
struct GenerationCluster {
    dir: PathBuf,
    nodes: Vec<Replica<GenerationStore>>,
    results: Vec<(usize, AppliedResult)>,
}
impl Drop for GenerationCluster {
    fn drop(&mut self) {
        self.nodes.clear();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
impl GenerationCluster {
    fn directory(&self, id: u64) -> SnapshotDirectory {
        SnapshotDirectory::open(
            self.dir.join(id.to_string()),
            [18; 16],
            id,
            SnapshotLimits::default(),
        )
        .unwrap()
    }
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "tickwire-config-generation-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        let mut c = Self {
            dir,
            nodes: Vec::new(),
            results: Vec::new(),
        };
        c.nodes = (1..=3)
            .map(|id| {
                Replica::new_with_bootstrap(
                    Node::create_in(c.directory(id), raft_config(id)).unwrap(),
                    config(),
                    trust(),
                )
                .unwrap()
            })
            .collect();
        c.nodes[0].campaign().unwrap();
        c.pump();
        let p = c.nodes[0].initialize().unwrap();
        c.pump();
        c.result(&p).unwrap();
        c
    }
    fn pump(&mut self) {
        self.pump_without(None);
    }
    fn pump_without(&mut self, excluded: Option<u64>) {
        for _ in 0..128 {
            let mut messages = VecDeque::new();
            for (index, node) in self.nodes.iter_mut().enumerate() {
                let out = node.drive().unwrap();
                messages.extend(out.messages);
                self.results
                    .extend(out.applied.into_iter().map(|a| (index, a)));
            }
            if messages.is_empty() {
                return;
            }
            for message in messages {
                if excluded.is_some_and(|id| message.from == id || message.to == id) {
                    continue;
                }
                self.nodes[(message.to - 1) as usize]
                    .step(message.from, message)
                    .unwrap();
            }
        }
        panic!("generation cluster did not settle");
    }
    fn result(&self, p: &Proposal) -> Result<Change> {
        self.results
            .iter()
            .find(|(id, a)| *id == 0 && a.matches(p))
            .unwrap()
            .1
            .result()
    }
    fn command(&mut self, command: ObjectCommand) -> Result<Change> {
        let p = self.nodes[0].propose(command).unwrap();
        self.pump();
        self.result(&p)
    }
    fn store(&mut self, key: Identity, bytes: &[u8], purpose: Purpose) {
        self.command(ObjectCommand::Begin(ObjectSpec {
            key,
            owner: OWNER,
            purpose,
            length: bytes.len() as u32,
            hash: Sha256::digest(bytes).into(),
        }))
        .unwrap();
        for (i, chunk) in bytes.chunks(CHUNK_BYTES).enumerate() {
            self.command(ObjectCommand::Chunk {
                key,
                owner: OWNER,
                offset: (i * CHUNK_BYTES) as u32,
                bytes: chunk.to_vec(),
            })
            .unwrap();
        }
        self.command(ObjectCommand::Seal { key, owner: OWNER })
            .unwrap();
    }
    fn head(&mut self) -> CommittedHead {
        let r = self.nodes[0].request_read().unwrap();
        self.pump();
        let HeadRead::Ready(Some(head)) = self.nodes[0].read_head(&r).unwrap() else {
            panic!("head")
        };
        *head
    }
}
#[test]
fn generation_snapshot_reopens_business_state_then_applies_only_durable_tail() {
    let mut c = GenerationCluster::new();
    let candidate = candidate();
    c.store(KEY, candidate.canonical_bytes(), Purpose::Candidate);
    let signed = sign(candidate.canonical_bytes());
    let p = c.nodes[0].bootstrap(signed.clone()).unwrap();
    c.pump();
    c.result(&p).unwrap();
    let original = c.head();
    let pending = vec![29; CHUNK_BYTES + 7];
    c.command(ObjectCommand::Begin(ObjectSpec {
        key: [9; 16],
        owner: OWNER,
        purpose: Purpose::Qualification,
        length: pending.len() as u32,
        hash: Sha256::digest(&pending).into(),
    }))
    .unwrap();
    c.command(ObjectCommand::Chunk {
        key: [9; 16],
        owner: OWNER,
        offset: 0,
        bytes: pending[..CHUNK_BYTES].to_vec(),
    })
    .unwrap();
    let receipt = c.nodes[0].compact_snapshot().unwrap();
    assert!(receipt.cut().index() > original.raft_index());
    assert_eq!(c.head(), original);
    c.store([10; 16], b"tail", Purpose::Qualification);
    c.nodes.clear();
    // Generic opening must not silently omit the compacted business state.
    let node = Node::recover_in(c.directory(1), raft_config(1)).unwrap();
    assert!(matches!(
        Replica::new_with_bootstrap(node, config(), trust()),
        Err(Error::NotReady)
    ));
    for id in 1..=3 {
        let node = Node::recover_in(c.directory(id), raft_config(id)).unwrap();
        c.nodes
            .push(Replica::recover_generation(node, config(), Some(trust())).unwrap());
    }
    c.nodes[0].campaign().unwrap();
    c.pump();
    assert_eq!(c.head(), original);
    let read = c.nodes[0].request_read().unwrap();
    c.pump();
    let Read::Ready(Some(tail)) = c.nodes[0].read_object(&read, [10; 16]).unwrap() else {
        panic!("tail")
    };
    assert_eq!(tail.bytes(), b"tail");
    assert_eq!(
        c.command(ObjectCommand::Chunk {
            key: [9; 16],
            owner: OWNER,
            offset: CHUNK_BYTES as u32,
            bytes: pending[CHUNK_BYTES..].to_vec()
        }),
        Ok(Change::ChunkStored {
            received: pending.len() as u32
        })
    );
    assert_eq!(
        c.command(ObjectCommand::Seal {
            key: [9; 16],
            owner: OWNER
        }),
        Ok(Change::Sealed)
    );
    let p = c.nodes[0].bootstrap(signed).unwrap();
    c.pump();
    assert_eq!(c.result(&p), Ok(Change::Bootstrapped { revision: 1 }));
    assert_eq!(c.head(), original);
}

#[test]
fn snapshot_install_requires_this_nodes_current_receipt_and_never_rewinds_tail() {
    let mut c = GenerationCluster::new();
    c.store([9; 16], b"before", Purpose::Qualification);
    let mut objects = MemoryObjects::new(SnapshotLimits::default()).unwrap();
    let (cut, root) = c.nodes[0].capture_snapshot(&mut objects).unwrap();
    let foreign = c.nodes[1].prepare_snapshot(&cut, &root, &objects).unwrap();
    let old = c.nodes[0].prepare_snapshot(&cut, &root, &objects).unwrap();
    let receipt = c.nodes[0].compact_snapshot().unwrap();
    assert_eq!(receipt.application(), &root);
    let other_scanned = c.nodes[1].scanned_through();
    assert_eq!(
        c.nodes[1].install_prepared(foreign, &receipt),
        Err(Error::Identity)
    );
    assert_eq!(c.nodes[1].scanned_through(), other_scanned);
    assert!(matches!(
        c.nodes[0].compact_snapshot(),
        Err(Error::NotReady)
    ));
    c.store([10; 16], b"after", Purpose::Qualification);
    let through = c.nodes[0].scanned_through();
    assert!(through > cut.index());
    assert_eq!(
        c.nodes[0].install_prepared(old, &receipt),
        Err(Error::NotReady)
    );
    assert_eq!(c.nodes[0].scanned_through(), through);
    let mut next = MemoryObjects::new(SnapshotLimits::default()).unwrap();
    assert_eq!(c.nodes[0].capture(&cut, &mut next), Err(Error::Identity));
    let read = c.nodes[0].request_read().unwrap();
    c.pump();
    let Read::Ready(Some(tail)) = c.nodes[0].read_object(&read, [10; 16]).unwrap() else {
        panic!("tail survived rejected installation");
    };
    assert_eq!(tail.bytes(), b"after");
    // A rejected stale request leaves the owner usable for the next real cut.
    assert!(c.nodes[0].compact_snapshot().unwrap().cut().index() > cut.index());
}

#[test]
fn generation_recovery_requires_original_business_configuration_and_trust() {
    let mut c = GenerationCluster::new();
    c.nodes[0].compact_snapshot().unwrap();
    c.nodes.clear();
    let changed = Config {
        objects: ObjectLimits {
            max_inflight: config().objects.max_inflight + 1,
            ..config().objects
        },
        ..config()
    };
    for (configuration, trusted) in [(changed, Some(trust())), (config(), None)] {
        let node = Node::recover_in(c.directory(1), raft_config(1)).unwrap();
        assert!(matches!(
            Replica::recover_generation(node, configuration, trusted),
            Err(Error::Identity)
        ));
    }
    let node = Node::recover_in(c.directory(1), raft_config(1)).unwrap();
    let recovered = Replica::recover_generation(node, config(), Some(trust())).unwrap();
    assert!(recovered.scanned_through() > 0);
}

fn lagging_snapshot_fixture() -> GenerationCluster {
    let mut c = GenerationCluster::new();
    let bytes = b"missed by third voter";
    for command in [
        ObjectCommand::Begin(ObjectSpec {
            key: [9; 16],
            owner: OWNER,
            purpose: Purpose::Qualification,
            length: bytes.len() as u32,
            hash: Sha256::digest(bytes).into(),
        }),
        ObjectCommand::Chunk {
            key: [9; 16],
            owner: OWNER,
            offset: 0,
            bytes: bytes.to_vec(),
        },
        ObjectCommand::Seal {
            key: [9; 16],
            owner: OWNER,
        },
    ] {
        let p = c.nodes[0].propose(command).unwrap();
        c.pump_without(Some(3));
        c.result(&p).unwrap();
    }
    c.nodes[0].compact_snapshot().unwrap();
    c
}

fn transfer_snapshot(
    c: &mut GenerationCluster,
    corrupt_business_root: bool,
) -> (
    tickwire_control_raft::Message,
    tickwire_control_raft::generation::VerifiedSnapshot,
) {
    use tickwire_control_raft::driver::PeerDriver;
    let source = c.nodes[0].snapshot_reader().unwrap().unwrap();
    let mut stage = c.nodes[2].stage_snapshot().unwrap().unwrap();
    for object in source.manifest().objects() {
        assert_eq!(stage.put(&source.read(object).unwrap()).unwrap(), *object);
    }
    let root = if corrupt_business_root {
        ApplicationRoot::new(1, stage.put(b"wrong application schema").unwrap()).unwrap()
    } else {
        source.manifest().application().clone()
    };
    let received = stage.finish(source.manifest().cut().clone(), root).unwrap();
    let mut message = tickwire_control_raft::Message::default();
    message.set_msg_type(tickwire_control_raft::MessageType::MsgSnapshot);
    message.from = 1;
    message.to = 3;
    message.term = c.nodes[0].status().term;
    message.set_snapshot(received.manifest().raft_snapshot().unwrap());
    (message, received)
}

#[test]
fn received_snapshot_restores_business_before_reply_then_continues_tail_and_reopens() {
    use tickwire_control_raft::driver::PeerDriver;
    let mut c = lagging_snapshot_fixture();
    let old = c.nodes[2].scanned_through();
    let (message, received) = transfer_snapshot(&mut c, false);
    let cut = received.manifest().cut().index();
    assert!(cut > old);
    assert!(c.nodes[2].step_snapshot(1, message, received).unwrap());
    let output = c.nodes[2].drive().unwrap();
    assert_eq!(c.nodes[2].scanned_through(), cut);
    let mut snapshot = MemoryObjects::new(SnapshotLimits::default()).unwrap();
    let (cut, root) = c.nodes[2].capture_snapshot(&mut snapshot).unwrap();
    let state = c.nodes[2].prepare_snapshot(&cut, &root, &snapshot).unwrap();
    assert_eq!(
        state.object([9; 16]).unwrap().bytes(),
        b"missed by third voter"
    );
    assert!(!output.messages.is_empty());
    for message in output.messages {
        c.nodes[(message.to - 1) as usize]
            .step(message.from, message)
            .unwrap();
    }
    c.pump();
    c.store([10; 16], b"after snapshot transfer", Purpose::Qualification);
    c.nodes.clear();
    let node = Node::recover_in(c.directory(3), raft_config(3)).unwrap();
    let replica = Replica::recover_generation(node, config(), Some(trust())).unwrap();
    let mut snapshot = MemoryObjects::new(SnapshotLimits::default()).unwrap();
    let (cut, root) = replica.capture_snapshot(&mut snapshot).unwrap();
    let state = replica.prepare_snapshot(&cut, &root, &snapshot).unwrap();
    assert_eq!(
        state.object([9; 16]).unwrap().bytes(),
        b"missed by third voter"
    );
    assert_eq!(
        state.object([10; 16]).unwrap().bytes(),
        b"after snapshot transfer"
    );
}

#[test]
fn bad_received_business_root_is_rejected_before_current_changes() {
    use tickwire_control_raft::driver::PeerDriver;
    let mut c = lagging_snapshot_fixture();
    let before = c.nodes[2].scanned_through();
    let (message, received) = transfer_snapshot(&mut c, true);
    assert!(c.nodes[2].step_snapshot(1, message, received).unwrap());
    assert!(c.nodes[2].drive().is_err());
    assert!(c.nodes[2].status().poisoned);
    c.nodes.clear();
    let node = Node::recover_in(c.directory(3), raft_config(3)).unwrap();
    assert_eq!(node.snapshot_base_index(), 0);
    let replica = Replica::recover_generation(node, config(), Some(trust())).unwrap();
    assert_eq!(replica.scanned_through(), before);
}
