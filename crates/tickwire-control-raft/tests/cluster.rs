use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use tickwire_control_raft::*;
use tickwire_journal::store::FileStore;

static NEXT: AtomicU64 = AtomicU64::new(1);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "tickwire-control-raft-{}-{}",
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
struct Cluster {
    dir: Directory,
    nodes: Vec<Node<FileStore>>,
}
impl Cluster {
    fn new() -> Self {
        let dir = Directory::new();
        let nodes = (1..=3)
            .map(|id| Node::create(dir.open(id), config(id)).unwrap())
            .collect();
        Self { dir, nodes }
    }
    fn pump(&mut self, isolated: Option<u64>) {
        for _ in 0..128 {
            let mut messages = VecDeque::new();
            for node in &mut self.nodes {
                messages.extend(node.drive().unwrap().messages);
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
        assert!(self.nodes[id].applied_index() > 0);
    }
    fn restart(&mut self) {
        self.nodes.clear();
        self.nodes = (1..=3)
            .map(|id| Node::recover(self.dir.open(id), config(id)).unwrap())
            .collect();
    }
}
#[test]
fn three_real_file_voters_commit_and_recover_the_same_complete_commands() {
    let mut cluster = Cluster::new();
    cluster.elect(0);
    cluster.nodes[0].propose(b"config-1".to_vec()).unwrap();
    cluster.pump(None);
    let large = vec![0x75; 100_000];
    cluster.nodes[0].propose(large.clone()).unwrap();
    cluster.pump(None);
    for node in &cluster.nodes {
        let history = node.history();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].command(), b"config-1");
        assert_eq!(history[1].command(), large);
    }
    cluster.restart();
    for node in &cluster.nodes {
        assert_eq!(node.history()[1].command(), large);
        assert!(!node.is_leader());
    }
    cluster.elect(1);
    cluster.nodes[1].propose(b"config-2".to_vec()).unwrap();
    cluster.pump(None);
    assert!(cluster.nodes.iter().all(|n| n.history().len() == 3));
}
#[test]
fn isolated_leader_cannot_complete_a_safe_read_or_new_command() {
    let mut cluster = Cluster::new();
    cluster.elect(0);
    let read = cluster.nodes[0].request_read().unwrap();
    assert!(cluster.nodes[0].take_read(&read).unwrap().is_none());
    cluster.pump(None);
    assert!(cluster.nodes[0].take_read(&read).unwrap().is_some());
    assert!(cluster.nodes[0].take_read(&read).is_err());

    let isolated = cluster.nodes[0].request_read().unwrap();
    cluster.nodes[0].propose(b"uncommitted".to_vec()).unwrap();
    cluster.pump(Some(1));
    assert!(cluster.nodes[0].take_read(&isolated).unwrap().is_none());
    assert!(cluster.nodes[0].history().is_empty());
    for _ in 0..120 {
        for node in &mut cluster.nodes {
            node.tick().unwrap();
        }
        cluster.pump(Some(1));
    }
    assert!(!cluster.nodes[0].is_leader());
    assert!(cluster.nodes[0].take_read(&isolated).is_err());
    let leader = (1..3).find(|i| cluster.nodes[*i].is_leader()).unwrap();
    cluster.nodes[leader].propose(b"majority".to_vec()).unwrap();
    cluster.pump(Some(1));
    assert_eq!(cluster.nodes[leader].history()[0].command(), b"majority");
    cluster.pump(None);
    for _ in 0..10 {
        cluster.nodes[leader].tick().unwrap();
        cluster.pump(None);
    }
    assert_eq!(cluster.nodes[0].history()[0].command(), b"majority");
}
#[test]
fn authenticated_sender_and_network_message_kind_are_checked() {
    let mut cluster = Cluster::new();
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgHup);
    message.from = 2;
    message.to = 1;
    assert!(cluster.nodes[0].step(2, message.clone()).is_err());
    message.set_msg_type(MessageType::MsgAppend);
    message.from = 3;
    assert!(cluster.nodes[0].step(2, message.clone()).is_err());
    message.from = 9;
    assert!(cluster.nodes[0].step(9, message).is_err());
}
#[test]
fn bootstrap_requires_three_distinct_bound_voters_and_recovery_identity_matches() {
    let dir = Directory::new();
    let mut cfg = config(1);
    cfg.voters = [1, 1, 3];
    assert!(Node::create(dir.open(1), cfg).is_err());
    let node = Node::create(dir.open(2), config(2)).unwrap();
    drop(node);
    let mut changed = config(2);
    changed.voters = [2, 3, 4];
    assert!(Node::recover(dir.open(2), changed).is_err());
}

struct FaultFile {
    file: FileStore,
    fault: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl tickwire_journal::store::Store for FaultFile {
    fn read_manifest(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        self.file.read_manifest()
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.file.publish_manifest(bytes)
    }
    fn segment_ids(&mut self) -> std::io::Result<Vec<u64>> {
        self.file.segment_ids()
    }
    fn read_segment(&mut self, id: u64, max: u64) -> std::io::Result<Vec<u8>> {
        self.file.read_segment(id, max)
    }
    fn create_segment(&mut self, id: u64) -> std::io::Result<()> {
        self.file.create_segment(id)
    }
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
        self.file.append_segment(id, offset, bytes)
    }
    fn sync_segment(&mut self, id: u64) -> std::io::Result<()> {
        let at = self.fault.load(Ordering::Relaxed);
        if at > 0 && self.fault.fetch_sub(1, Ordering::Relaxed) == 1 {
            return Err(std::io::Error::other("injected sync failure"));
        }
        self.file.sync_segment(id)
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> std::io::Result<()> {
        self.file.truncate_segment(id, len)
    }
}
fn append_message() -> Message {
    let entry = raft::eraftpb::Entry {
        index: 1,
        term: 1,
        data: b"settled".to_vec().into(),
        ..Default::default()
    };
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgAppend);
    message.from = 2;
    message.to = 1;
    message.term = 1;
    message.commit = 1;
    message.entries.push(entry);
    message
}
#[test]
fn every_ready_and_apply_sync_failure_withholds_ack_and_recovers_a_valid_prefix() {
    for failure in 1..=6 {
        let dir = Directory::new();
        let fault = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let file = FaultFile {
            file: dir.open(1),
            fault: fault.clone(),
        };
        let mut node = Node::create(file, config(1)).unwrap();
        node.step(2, append_message()).unwrap();
        fault.store(failure, Ordering::Relaxed);
        assert!(node.drive().is_err(), "sync cut {failure}");
        assert!(node.drive().is_err());
        assert!(node.tick().is_err());
        assert!(node.propose(b"after-failure".to_vec()).is_err());
        assert!(node.history().is_empty());
        drop(node);

        let mut node = Node::recover(dir.open(1), config(1)).unwrap();
        node.drive().unwrap();
        assert!(node.history().len() <= 1);
        node.step(2, append_message()).unwrap();
        let output = node.drive().unwrap();
        assert!(
            output
                .messages
                .iter()
                .any(|m| m.get_msg_type() == MessageType::MsgAppendResponse && !m.reject)
        );
        assert_eq!(node.history().len(), 1);
        assert_eq!(node.history()[0].command(), b"settled");
        drop(node);
        let node = Node::recover(dir.open(1), config(1)).unwrap();
        assert_eq!(node.history()[0].command(), b"settled");
    }
}
#[test]
fn a_positive_vote_survives_reopen_and_prevents_a_second_vote_in_that_term() {
    let dir = Directory::new();
    let mut node = Node::create(dir.open(1), config(1)).unwrap();
    let mut vote = Message::default();
    vote.set_msg_type(MessageType::MsgRequestVote);
    vote.from = 2;
    vote.to = 1;
    vote.term = 3;
    node.step(2, vote.clone()).unwrap();
    let output = node.drive().unwrap();
    assert!(
        output
            .messages
            .iter()
            .any(|m| m.get_msg_type() == MessageType::MsgRequestVoteResponse && !m.reject)
    );
    drop(node);
    let mut node = Node::recover(dir.open(1), config(1)).unwrap();
    vote.from = 3;
    node.step(3, vote).unwrap();
    let output = node.drive().unwrap();
    assert!(
        output
            .messages
            .iter()
            .any(|m| m.get_msg_type() == MessageType::MsgRequestVoteResponse && m.reject)
    );
}

#[test]
fn malformed_append_indices_are_rejected_before_entering_raft() {
    let dir = Directory::new();
    let mut node = Node::create(dir.open(1), config(1)).unwrap();
    let mut message = append_message();
    message.entries[0].index = 2;
    assert_eq!(node.step(2, message), Err(Error::Protocol));
}

#[test]
fn heartbeat_cannot_commit_beyond_the_actual_local_log() {
    let dir = Directory::new();
    let mut node = Node::create(dir.open(1), config(1)).unwrap();
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgHeartbeat);
    message.from = 2;
    message.to = 1;
    message.term = 1;
    message.commit = 1;
    assert_eq!(node.step(2, message), Err(Error::Protocol));
}
