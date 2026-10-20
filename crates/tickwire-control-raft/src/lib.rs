//! Durable adapter for the fixed control consensus group.
#![forbid(unsafe_code)]

use protobuf::Message as PbMessage;
pub use raft::eraftpb::{Message, MessageType};
use raft::{RawNode, ReadOnlyOption, StateRole, Storage, eraftpb::Entry};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use storage::DurableStorage;
use tickwire_journal::store::Store;
mod storage;
mod transactions;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Identity,
    Bounds,
    Store,
    Corrupt,
    NotLeader,
    NotReady,
    Protocol,
    Poisoned,
}
pub type Result<T> = core::result::Result<T, Error>;
#[derive(Clone)]
pub struct Config {
    pub physical: tickwire_journal::Config,
    pub node_id: u64,
    pub voters: [u64; 3],
    pub writer_epoch: u64,
    pub max_command_bytes: usize,
    pub max_retained_bytes: usize,
}
#[derive(Debug)]
pub struct Applied {
    index: u64,
    term: u64,
    command: Vec<u8>,
}
impl Applied {
    pub fn index(&self) -> u64 {
        self.index
    }
    pub fn term(&self) -> u64 {
        self.term
    }
    pub fn command(&self) -> &[u8] {
        &self.command
    }
}
#[derive(Default)]
pub struct Output {
    pub messages: Vec<Message>,
    pub applied: Vec<Applied>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    term: u64,
    index: u64,
    hash: [u8; 32],
}
impl Proposal {
    pub fn matches(&self, applied: &Applied) -> bool {
        let hash: [u8; 32] = Sha256::digest(applied.command()).into();
        self.term == applied.term && self.index == applied.index && self.hash == hash
    }
}
#[derive(Debug)]
pub struct ReadId {
    context: [u8; 32],
    term: u64,
}
#[derive(Debug)]
pub struct ReadBarrier {
    term: u64,
    index: u64,
}
impl ReadBarrier {
    pub fn term(&self) -> u64 {
        self.term
    }
    pub fn index(&self) -> u64 {
        self.index
    }
}
struct PendingRead {
    term: u64,
    index: Option<u64>,
}
pub struct Node<S: Store> {
    raw: RawNode<DurableStorage<S>>,
    reads: BTreeMap<[u8; 32], PendingRead>,
    poisoned: bool,
}
impl<S: Store> Node<S> {
    pub fn create(store: S, config: Config) -> Result<Self> {
        Self::open(DurableStorage::create(store, config)?)
    }
    pub fn recover(store: S, config: Config) -> Result<Self> {
        Self::open(DurableStorage::recover(store, config)?)
    }
    fn open(storage: DurableStorage<S>) -> Result<Self> {
        let config = raft::Config {
            id: storage.config.node_id,
            election_tick: 50,
            heartbeat_tick: 5,
            min_election_tick: 50,
            max_election_tick: 100,
            check_quorum: true,
            pre_vote: true,
            read_only_option: ReadOnlyOption::Safe,
            applied: storage.applied,
            max_size_per_msg: 1024 * 1024,
            max_committed_size_per_ready: 16 * 1024 * 1024,
            max_uncommitted_size: 32 * 1024 * 1024,
            max_inflight_msgs: 64,
            ..Default::default()
        };
        config.validate().map_err(|_| Error::Bounds)?;
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let raw = RawNode::new(&config, storage, &logger).map_err(|_| Error::Corrupt)?;
        Ok(Self {
            raw,
            reads: BTreeMap::new(),
            poisoned: false,
        })
    }
    fn healthy(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
    pub fn tick(&mut self) -> Result<()> {
        self.healthy()?;
        self.raw.tick();
        self.forget_stale_reads();
        Ok(())
    }
    pub fn campaign(&mut self) -> Result<()> {
        self.healthy()?;
        self.raw.campaign().map_err(|_| Error::Protocol)
    }
    pub fn step(&mut self, peer: u64, message: Message) -> Result<()> {
        self.healthy()?;
        let config = &self.raw.store().config;
        if message.from != peer
            || message.to != config.node_id
            || peer == config.node_id
            || !config.voters.contains(&peer)
        {
            return Err(Error::Identity);
        }
        if !matches!(
            message.get_msg_type(),
            MessageType::MsgAppend
                | MessageType::MsgAppendResponse
                | MessageType::MsgRequestVote
                | MessageType::MsgRequestVoteResponse
                | MessageType::MsgHeartbeat
                | MessageType::MsgHeartbeatResponse
                | MessageType::MsgRequestPreVote
                | MessageType::MsgRequestPreVoteResponse
        ) || message.term == 0
            || [
                message.term,
                message.index,
                message.log_term,
                message.commit,
                message.reject_hint,
            ]
            .iter()
            .any(|n| *n >= 1 << 63)
            || message.compute_size() as usize > transactions::MAX_UPDATE
            || message.entries.len() > 262_144
            || message.entries.iter().any(|e| {
                e.get_entry_type() != raft::eraftpb::EntryType::EntryNormal
                    || e.data.len() > config.max_command_bytes
                    || e.context.len() > 256
                    || e.index == 0
                    || e.index >= 1 << 63
                    || e.term == 0
                    || e.term > message.term
            })
        {
            return Err(Error::Protocol);
        }
        if message.get_msg_type() == MessageType::MsgHeartbeat
            && message.commit > self.raw.raft.raft_log.last_index()
        {
            return Err(Error::Protocol);
        }
        if message.get_msg_type() == MessageType::MsgAppend {
            let mut previous_index = message.index;
            let mut previous_term = message.log_term;
            if previous_term > message.term {
                return Err(Error::Protocol);
            }
            for entry in &message.entries {
                if entry.index != previous_index + 1 || entry.term < previous_term {
                    return Err(Error::Protocol);
                }
                previous_index = entry.index;
                previous_term = entry.term;
            }
        }
        self.raw.step(message).map_err(|_| Error::Protocol)?;
        self.forget_stale_reads();
        Ok(())
    }
    pub fn drive(&mut self) -> Result<Output> {
        self.healthy()?;
        let result = self.drive_inner();
        if result.is_err() {
            self.poisoned = true;
            self.reads.clear();
        }
        result
    }
    fn drive_inner(&mut self) -> Result<Output> {
        let mut output = Output::default();
        for _ in 0..128 {
            if !self.raw.has_ready() {
                return Ok(output);
            }
            let mut ready = self.raw.ready();
            if !ready.snapshot().is_empty() {
                return Err(Error::Protocol);
            }
            // Even leader messages are held until the entire Ready is durable.
            self.raw.mut_store().persist(ready.hs(), ready.entries())?;
            let mut messages = ready.take_messages();
            messages.extend(ready.take_persisted_messages());
            let reads = ready.take_read_states();
            let committed = ready.take_committed_entries();
            self.apply(&committed, &mut output)?;
            let mut light = self.raw.advance_append(ready);
            if let Some(commit) = light.commit_index() {
                let mut hard = self.raw.store().hard.clone();
                hard.commit = commit;
                self.raw.mut_store().persist(Some(&hard), &[])?;
            }
            self.apply(&light.take_committed_entries(), &mut output)?;
            let applied = self.raw.store().applied;
            self.raw.advance_apply_to(applied);
            messages.extend(light.take_messages());
            output.messages.extend(messages);
            for state in reads {
                if let Ok(context) = <[u8; 32]>::try_from(state.request_ctx.as_slice())
                    && let Some(pending) = self.reads.get_mut(&context)
                    && pending.term == self.raw.raft.term
                {
                    pending.index = Some(state.index);
                }
            }
            self.forget_stale_reads();
        }
        Err(Error::Bounds)
    }
    fn apply(&mut self, entries: &[Entry], output: &mut Output) -> Result<()> {
        self.raw.mut_store().persist_applied(entries)?;
        output
            .applied
            .extend(entries.iter().filter(|e| !e.data.is_empty()).map(applied));
        Ok(())
    }
    fn leader_ready(&self) -> Result<()> {
        self.healthy()?;
        if self.raw.raft.state != StateRole::Leader {
            return Err(Error::NotLeader);
        }
        let store = self.raw.store();
        if store.applied == 0
            || store.term(store.applied).map_err(|_| Error::Corrupt)? != self.raw.raft.term
        {
            return Err(Error::NotReady);
        }
        Ok(())
    }
    pub fn propose(&mut self, command: Vec<u8>) -> Result<Proposal> {
        self.leader_ready()?;
        if command.is_empty() || command.len() > self.raw.store().config.max_command_bytes {
            return Err(Error::Bounds);
        }
        let hash = Sha256::digest(&command).into();
        let index = self
            .raw
            .raft
            .raft_log
            .last_index()
            .checked_add(1)
            .ok_or(Error::Bounds)?;
        self.raw
            .propose(Vec::new(), command)
            .map_err(|_| Error::NotReady)?;
        if self.raw.raft.raft_log.last_index() != index {
            return Err(Error::NotReady);
        }
        Ok(Proposal {
            term: self.raw.raft.term,
            index,
            hash,
        })
    }
    pub fn request_read(&mut self) -> Result<ReadId> {
        self.leader_ready()?;
        if self.reads.len() >= 1024 {
            return Err(Error::Bounds);
        }
        let mut context = [0; 32];
        getrandom::getrandom(&mut context).map_err(|_| Error::Protocol)?;
        if self.reads.contains_key(&context) {
            return Err(Error::Protocol);
        }
        let term = self.raw.raft.term;
        self.reads
            .insert(context, PendingRead { term, index: None });
        self.raw.read_index(context.to_vec());
        Ok(ReadId { context, term })
    }
    pub fn take_read(&mut self, id: &ReadId) -> Result<Option<ReadBarrier>> {
        self.leader_ready()?;
        if id.term != self.raw.raft.term {
            return Err(Error::NotReady);
        }
        let pending = self.reads.get(&id.context).ok_or(Error::Protocol)?;
        if pending.term != id.term {
            return Err(Error::Protocol);
        }
        if let Some(index) = pending.index
            && self.raw.store().applied >= index
        {
            self.reads.remove(&id.context);
            return Ok(Some(ReadBarrier {
                term: id.term,
                index,
            }));
        }
        Ok(None)
    }
    fn forget_stale_reads(&mut self) {
        if self.raw.raft.state != StateRole::Leader {
            self.reads.clear();
        } else {
            self.reads.retain(|_, r| r.term == self.raw.raft.term);
        }
    }
    pub fn is_leader(&self) -> bool {
        !self.poisoned && self.raw.raft.state == StateRole::Leader
    }
    pub fn applied_index(&self) -> u64 {
        self.raw.store().applied
    }
    /// The durable control-command history; business reducers interpret it separately.
    pub fn history(&self) -> Vec<Applied> {
        self.raw
            .store()
            .log
            .iter()
            .take(self.raw.store().applied as usize)
            .filter(|e| !e.data.is_empty())
            .map(applied)
            .collect()
    }
}
fn applied(entry: &Entry) -> Applied {
    Applied {
        index: entry.index,
        term: entry.term,
        command: entry.data.to_vec(),
    }
}
