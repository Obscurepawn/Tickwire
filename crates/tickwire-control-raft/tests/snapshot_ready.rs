use raft::eraftpb::{ConfState, Snapshot, SnapshotMetadata};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use tickwire_control_raft::{
    Config, Message, MessageType, Node,
    driver::ReadyBudget,
    generation::{GenerationStore, SnapshotDirectory, VerifiedSnapshot},
    snapshot::{
        ApplicationRoot, InstalledGeneration, SnapshotDriveError, SnapshotLimits, SnapshotReader,
        SnapshotRestore, SnapshotWriter,
    },
};
static NEXT: AtomicU64 = AtomicU64::new(1);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "raft-ready-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn config(id: u64, writer: u64) -> Config {
    Config {
        physical: tickwire_journal::Config::new([id as u8; 16], [8; 16]),
        node_id: id,
        voters: [1, 2, 3],
        writer_epoch: writer,
        max_command_bytes: 262144,
        max_retained_bytes: 8 * 1024 * 1024,
    }
}
fn pump(nodes: &mut [Node<GenerationStore>], lag: bool) {
    for _ in 0..100 {
        let mut messages = Vec::new();
        for node in nodes.iter_mut() {
            messages.extend(node.drive().unwrap().messages);
        }
        if messages.is_empty() {
            return;
        }
        for m in messages {
            if (!lag || m.to != 3) && m.to <= nodes.len() as u64 {
                nodes[(m.to - 1) as usize].step(m.from, m).unwrap();
            }
        }
    }
    panic!("did not settle");
}
struct Hook {
    directory: SnapshotDirectory,
    fail_prepare: bool,
    fail_install: bool,
    prepared: usize,
    installed: usize,
}
impl SnapshotRestore for Hook {
    type Prepared = Vec<u8>;
    type Error = &'static str;
    fn prepare(&mut self, s: &VerifiedSnapshot) -> Result<Vec<u8>, Self::Error> {
        self.prepared += 1;
        assert!(self.directory.current().unwrap().is_none());
        if self.fail_prepare {
            return Err("schema");
        }
        s.read(&s.manifest().application().root())
            .map_err(|_| "read")
    }
    fn install(
        &mut self,
        bytes: Vec<u8>,
        receipt: &InstalledGeneration,
    ) -> Result<(), Self::Error> {
        self.installed += 1;
        let current = self.directory.current().unwrap().unwrap();
        assert_eq!(current.generation(), receipt.generation());
        assert_eq!(bytes, b"complete business state");
        if self.fail_install {
            return Err("apply");
        }
        Ok(())
    }
}
fn setup() -> (
    Temp,
    Vec<Node<GenerationStore>>,
    SnapshotDirectory,
    Message,
    VerifiedSnapshot,
) {
    let t = Temp::new();
    let mut nodes = Vec::new();
    let mut target = None;
    for id in 1..=3 {
        let d = SnapshotDirectory::open(
            t.0.join(id.to_string()),
            [8; 16],
            id,
            SnapshotLimits::default(),
        )
        .unwrap();
        if id == 3 {
            target = Some(d.clone());
        }
        nodes.push(Node::create_in(d, config(id, 1)).unwrap());
    }
    nodes[0].campaign().unwrap();
    pump(&mut nodes, false);
    nodes[0].propose(b"before snapshot".to_vec()).unwrap();
    pump(&mut nodes, true);
    let cut = nodes[0].snapshot_cut(nodes[0].applied_index()).unwrap();
    let mut stage = nodes[2].stage_snapshot().unwrap();
    let object = stage.put(b"complete business state").unwrap();
    let snapshot = stage
        .finish(cut.clone(), ApplicationRoot::new(1, object).unwrap())
        .unwrap();
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgSnapshot);
    message.from = 1;
    message.to = 3;
    message.term = nodes[0].status().term;
    message.set_snapshot(Snapshot {
        data: snapshot.manifest().encode().unwrap().into(),
        metadata: Some(SnapshotMetadata {
            index: cut.index(),
            term: cut.term(),
            conf_state: Some(ConfState {
                voters: vec![1, 2, 3],
                ..Default::default()
            })
            .into(),
            ..Default::default()
        })
        .into(),
        ..Default::default()
    });
    (t, nodes, target.unwrap(), message, snapshot)
}
#[test]
fn received_snapshot_is_prepared_before_current_then_installed_before_reply() {
    let (_t, mut nodes, d, m, s) = setup();
    let cut = s.manifest().cut().index();
    nodes[2].step_snapshot(1, m, s).unwrap();
    let mut hook = Hook {
        directory: d,
        fail_prepare: false,
        fail_install: false,
        prepared: 0,
        installed: 0,
    };
    let output = nodes[2]
        .drive_with_snapshot(ReadyBudget::new(1).unwrap(), &mut hook)
        .unwrap();
    assert_eq!((hook.prepared, hook.installed), (1, 1));
    assert_eq!(nodes[2].snapshot_base_index(), cut);
    assert_eq!(nodes[2].applied_index(), cut);
    assert!(!output.messages.is_empty());
    assert!(nodes[2].matches_installed(nodes[2].installed_generation().unwrap()));
}
#[test]
fn bad_business_snapshot_does_not_replace_current_or_emit_success() {
    let (t, mut nodes, d, m, s) = setup();
    let old = nodes[2].applied_index();
    nodes[2].step_snapshot(1, m, s).unwrap();
    let mut hook = Hook {
        directory: d,
        fail_prepare: true,
        fail_install: false,
        prepared: 0,
        installed: 0,
    };
    assert!(matches!(
        nodes[2].drive_with_snapshot(ReadyBudget::new(1).unwrap(), &mut hook),
        Err(SnapshotDriveError::Application("schema"))
    ));
    assert_eq!(hook.installed, 0);
    assert!(hook.directory.current().unwrap().is_none());
    assert!(nodes[2].status().poisoned);
    drop(hook);
    drop(nodes);
    let n = Node::recover_in(
        SnapshotDirectory::open(t.0.join("3"), [8; 16], 3, SnapshotLimits::default()).unwrap(),
        config(3, 2),
    )
    .unwrap();
    assert_eq!(n.applied_index(), old);
    assert_eq!(n.snapshot_base_index(), 0);
}
#[test]
fn post_current_application_failure_poison_withholds_reply_but_reopens_exact_new_base() {
    let (t, mut nodes, d, m, s) = setup();
    let cut = s.manifest().cut().index();
    nodes[2].step_snapshot(1, m, s).unwrap();
    let mut hook = Hook {
        directory: d,
        fail_prepare: false,
        fail_install: true,
        prepared: 0,
        installed: 0,
    };
    assert!(matches!(
        nodes[2].drive_with_snapshot(ReadyBudget::new(1).unwrap(), &mut hook),
        Err(SnapshotDriveError::Application("apply"))
    ));
    assert!(nodes[2].status().poisoned);
    assert_eq!((hook.prepared, hook.installed), (1, 1));
    drop(hook);
    drop(nodes);
    let n = Node::recover_in(
        SnapshotDirectory::open(t.0.join("3"), [8; 16], 3, SnapshotLimits::default()).unwrap(),
        config(3, 2),
    )
    .unwrap();
    assert_eq!(n.applied_index(), cut);
    assert_eq!(n.snapshot_base_index(), cut);
}

#[test]
fn joining_snapshot_checks_manifest_sender_and_session_before_term_or_business_changes() {
    use tickwire_control_raft::membership::*;
    let (t, mut nodes, _, _, _) = setup();
    nodes[0].peer_session(5, Some([5; 16])).unwrap();
    let request = MembershipRequest {
        operation: [91; 16],
        expected_membership: nodes[0].current_membership().digest(),
        action: MembershipAction::AddLearner { node_id: 5 },
    };
    nodes[0].propose_membership(request).unwrap();
    pump(&mut nodes, false);
    let cut = nodes[0].snapshot_cut(nodes[0].applied_index()).unwrap();
    let d = SnapshotDirectory::open(t.0.join("5"), [8; 16], 5, SnapshotLimits::default()).unwrap();
    let mut join = Node::create_joining_in(d.clone(), config(5, 1)).unwrap();
    let stage = |join: &Node<GenerationStore>| {
        let mut writer = join.stage_snapshot().unwrap();
        let root = writer.put(b"complete business state").unwrap();
        writer
            .finish(cut.clone(), ApplicationRoot::new(1, root).unwrap())
            .unwrap()
    };
    let message = |sender, snapshot: &VerifiedSnapshot| {
        let mut m = Message {
            from: sender,
            to: 5,
            term: 50,
            ..Default::default()
        };
        m.set_msg_type(MessageType::MsgSnapshot);
        m.set_snapshot(snapshot.manifest().raft_snapshot().unwrap());
        m
    };
    let valid = stage(&join);
    assert!(join.step_snapshot(1, message(1, &valid), valid).is_err()); // no authenticated session
    join.peer_session(4, Some([4; 16])).unwrap();
    let invalid = stage(&join);
    assert!(
        join.step_snapshot(4, message(4, &invalid), invalid)
            .is_err()
    ); // authenticated but not manifest voter
    assert_eq!(join.status().term, 0);
    assert_eq!(join.applied_index(), 0);
    assert!(join.history().is_empty());
    assert!(d.current().unwrap().is_none());
    join.peer_session(1, Some([1; 16])).unwrap();
    let valid = stage(&join);
    join.step_snapshot(1, message(1, &valid), valid).unwrap();
    let mut hook = Hook {
        directory: d,
        fail_prepare: false,
        fail_install: false,
        prepared: 0,
        installed: 0,
    };
    join.drive_with_snapshot(ReadyBudget::new(1).unwrap(), &mut hook)
        .unwrap();
    assert!(!join.status().joining);
    assert_eq!(join.applied_index(), cut.index());
    let before = join.status().term;
    let mut unrelated = Message {
        from: 4,
        to: 5,
        term: before + 10,
        ..Default::default()
    };
    unrelated.set_msg_type(MessageType::MsgAppend);
    assert!(join.step(4, unrelated).is_err()); // exception ended at actual admission
    assert_eq!(join.status().term, before);
}
