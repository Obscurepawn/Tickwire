//! Durable adapter for the fixed control consensus group.
#![forbid(unsafe_code)]

use protobuf::Message as PbMessage;
pub use raft::eraftpb::{Message, MessageType};
use raft::{RawNode, ReadOnlyOption, StateRole, Storage, eraftpb::Entry};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use storage::DurableStorage;
use tickwire_journal::store::Store;
pub mod driver;
pub mod generation;
pub mod membership;
pub mod snapshot;
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
    pub more_ready: bool,
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
    _lease: std::sync::Arc<()>,
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
    canceled: bool,
    lease: std::sync::Weak<()>,
}
type GenerationFactory<S> = fn(&generation::SnapshotDirectory, [u8; 16]) -> Result<S>;
pub struct Node<S: Store> {
    raw: RawNode<DurableStorage<S>>,
    reads: BTreeMap<[u8; 32], PendingRead>,
    poisoned: bool,
    installed: Option<snapshot::InstalledGeneration>,
    generation_directory: Option<generation::SnapshotDirectory>,
    generation_owner: Option<std::sync::Arc<[u8; 16]>>,
    generation_factory: Option<GenerationFactory<S>>,
    pending_snapshot: Option<generation::VerifiedSnapshot>,
    peer_sessions: BTreeMap<u64, membership::PeerProgress>,
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
            installed: None,
            generation_directory: None,
            generation_owner: None,
            generation_factory: None,
            pending_snapshot: None,
            peer_sessions: BTreeMap::new(),
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
        if self
            .current_membership()
            .retired()
            .contains(&self.status().node_id)
        {
            return Ok(());
        }
        self.raw.tick();
        self.forget_stale_reads();
        Ok(())
    }
    pub fn campaign(&mut self) -> Result<()> {
        self.healthy()?;
        let m = self.current_membership();
        if !m.voters().contains(&self.status().node_id)
            && !m.voters_outgoing().contains(&self.status().node_id)
        {
            return Err(Error::NotReady);
        }
        self.raw.campaign().map_err(|_| Error::Protocol)
    }
    pub fn step(&mut self, peer: u64, message: Message) -> Result<()> {
        self.healthy()?;
        let config = &self.raw.store().config;
        if message.from != peer || message.to != config.node_id || peer == config.node_id {
            return Err(Error::Identity);
        }
        let membership = self.current_membership();
        if membership.retired().contains(&peer) || membership.retired().contains(&config.node_id) {
            return Ok(());
        }
        let joining = self.joining_unadmitted();
        if joining {
            if !self.peer_sessions.contains_key(&peer)
                || !matches!(
                    message.get_msg_type(),
                    MessageType::MsgAppend | MessageType::MsgHeartbeat
                )
            {
                return Err(Error::Identity);
            }
        } else if !membership.contains(peer) {
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
                !matches!(
                    e.get_entry_type(),
                    raft::eraftpb::EntryType::EntryNormal
                        | raft::eraftpb::EntryType::EntryConfChangeV2
                ) || e.data.len() > config.max_command_bytes
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
        for e in &message.entries {
            if e.get_entry_type() == raft::eraftpb::EntryType::EntryConfChangeV2 {
                membership::parse_entry(e)?;
            }
        }
        let progress = (message.get_msg_type() == MessageType::MsgAppendResponse
            && !message.reject
            && message.term == self.raw.raft.term
            && message.index <= self.raw.raft.raft_log.last_index())
        .then_some((message.term, message.index));
        self.raw.step(message).map_err(|_| Error::Protocol)?;
        if let Some((term, index)) = progress
            && self.raw.raft.state == StateRole::Leader
            && let Some(p) = self.peer_sessions.get_mut(&peer)
        {
            if p.term != term {
                p.index = 0;
            }
            p.term = term;
            p.index = p.index.max(index);
        }
        self.forget_stale_reads();
        Ok(())
    }
    pub fn drive_bounded(&mut self, budget: driver::ReadyBudget) -> Result<Output> {
        self.healthy()?;
        let result = self
            .drive_inner(budget.rounds(), &mut RejectSnapshots)
            .map_err(|e| match e {
                snapshot::SnapshotDriveError::Raft(e)
                | snapshot::SnapshotDriveError::Application(e) => e,
            });
        if result.is_err() {
            self.poisoned = true;
            self.reads.clear();
        }
        result
    }
    pub fn drive_with_snapshot<R: snapshot::SnapshotRestore>(
        &mut self,
        budget: driver::ReadyBudget,
        restore: &mut R,
    ) -> core::result::Result<Output, snapshot::SnapshotDriveError<R::Error>> {
        self.healthy()?;
        let result = self.drive_inner(budget.rounds(), restore);
        if result.is_err() {
            self.poisoned = true;
            self.reads.clear();
        }
        result
    }
    pub fn supports_snapshots(&self) -> bool {
        self.generation_factory.is_some()
    }
    /// Only a fully materialized closure from this owner's staging directory can
    /// be offered to Raft. Application preparation follows only if Raft accepts it.
    pub fn step_snapshot(
        &mut self,
        peer: u64,
        message: Message,
        snapshot: generation::VerifiedSnapshot,
    ) -> Result<()> {
        self.healthy()?;
        if !self.supports_snapshots() {
            return Err(Error::NotReady);
        }
        self.check_snapshot_owner(&snapshot)?;
        if self.pending_snapshot.is_some() {
            return Err(Error::NotReady);
        }
        let cut = snapshot.manifest().cut();
        let conf = self.current_membership();
        let incoming = cut.membership();
        let joining = self.joining_unadmitted();
        if conf.retired().contains(&self.status().node_id)
            || conf.retired().contains(&peer)
            || !incoming.contains(self.status().node_id)
            || (!incoming.voters().contains(&peer) && !incoming.voters_outgoing().contains(&peer))
            || (joining && !self.peer_sessions.contains_key(&peer))
            || message.from != peer
            || message.to != self.status().node_id
            || peer == message.to
            || (!joining
                && !conf.voters().contains(&peer)
                && !conf.voters_outgoing().contains(&peer))
            || message.get_msg_type() != MessageType::MsgSnapshot
            || message.term == 0
            || message.term >= 1 << 63
            || message.term < cut.term()
            || !message.entries.is_empty()
            || message.get_snapshot() != &snapshot.manifest().raft_snapshot()?
        {
            return Err(Error::Identity);
        }
        let prior = &self.raw.store().member;
        if cut.index() >= self.raw.store().applied
            && (cut.membership().origin != prior.origin
                || !cut.membership().operations.starts_with(&prior.operations))
        {
            return Err(Error::Identity);
        }
        self.raw.step(message).map_err(|_| Error::Protocol)?;
        if self.raw.raft.raft_log.unstable_snapshot().is_some() {
            self.pending_snapshot = Some(snapshot);
        }
        self.forget_stale_reads();
        Ok(())
    }
    pub fn report_snapshot(&mut self, peer: u64, success: bool) -> Result<()> {
        self.healthy()?;
        if peer == self.status().node_id || peer == 0 {
            return Err(Error::Identity);
        }
        if !self.current_membership().contains(peer) {
            return Ok(());
        }
        self.raw.report_snapshot(
            peer,
            if success {
                raft::SnapshotStatus::Finish
            } else {
                raft::SnapshotStatus::Failure
            },
        );
        Ok(())
    }
    /// Conservative result allocation bound with no intervening step/proposal.
    /// Count both durable and unstable suffixes when they overlap.
    pub fn pending_applied_bound(&self) -> Result<driver::OutputBound> {
        self.healthy()?;
        let mut items = 0usize;
        let mut bytes = 0usize;
        for entry in self
            .raw
            .store()
            .log
            .iter()
            .chain(self.raw.raft.raft_log.unstable_entries())
        {
            if entry.index > self.raw.store().applied && !entry.data.is_empty() {
                items = items.checked_add(1).ok_or(Error::Bounds)?;
                bytes = bytes.checked_add(entry.data.len()).ok_or(Error::Bounds)?;
            }
        }
        let capacity = if items == 0 {
            0
        } else {
            items.checked_mul(2).ok_or(Error::Bounds)?.max(4)
        };
        bytes = bytes
            .checked_add(
                capacity
                    .checked_mul(std::mem::size_of::<Applied>())
                    .ok_or(Error::Bounds)?,
            )
            .ok_or(Error::Bounds)?;
        Ok(driver::OutputBound { items, bytes })
    }
    pub fn status(&self) -> driver::DriverStatus {
        driver::DriverStatus {
            group: self.raw.store().config.physical.domain,
            node_id: self.raw.store().config.node_id,
            term: self.raw.raft.term,
            committed: self.raw.store().hard.commit,
            raft_applied: self.raw.store().applied,
            role: match self.raw.raft.state {
                StateRole::Follower => driver::Role::Follower,
                StateRole::Candidate => driver::Role::Candidate,
                StateRole::PreCandidate => driver::Role::PreCandidate,
                StateRole::Leader => driver::Role::Leader,
            },
            poisoned: self.poisoned,
            joining: self.joining_unadmitted(),
        }
    }
    fn joining_unadmitted(&self) -> bool {
        let s = self.raw.store();
        s.joining
            && !s.member.contains(s.config.node_id)
            && !s.member.retired().contains(&s.config.node_id)
            && !s.member.operations.iter().any(|r| {
                let c = r.configuration();
                c.voters.contains(&s.config.node_id)
                    || c.voters_outgoing.contains(&s.config.node_id)
                    || c.learners.contains(&s.config.node_id)
                    || c.learners_next.contains(&s.config.node_id)
            })
    }
    pub fn current_membership(&self) -> driver::Membership {
        self.raw.store().membership()
    }
    /// Capture only the exact durable applied cut already scanned by the application.
    pub fn snapshot_cut(&self, application_scanned: u64) -> Result<snapshot::SnapshotCut> {
        self.healthy()?;
        let store = self.raw.store();
        if application_scanned == 0
            || application_scanned != store.applied
            || application_scanned > store.hard.commit
        {
            return Err(Error::NotReady);
        }
        snapshot::SnapshotCut::new(
            store.config.physical.domain,
            application_scanned,
            store
                .term(application_scanned)
                .map_err(|_| Error::Corrupt)?,
            self.current_membership(),
        )
    }
    pub fn report_unreachable(&mut self, peer: u64) -> Result<()> {
        self.healthy()?;
        if peer == self.raw.store().config.node_id || peer == 0 {
            return Err(Error::Identity);
        }
        self.raw.report_unreachable(peer);
        Ok(())
    }
    pub fn drive(&mut self) -> Result<Output> {
        self.drive_bounded(driver::ReadyBudget::new(128)?)
    }
    fn drive_inner<R: snapshot::SnapshotRestore>(
        &mut self,
        rounds: usize,
        restore: &mut R,
    ) -> core::result::Result<Output, snapshot::SnapshotDriveError<R::Error>> {
        let mut output = Output::default();
        self.reap_dropped_reads();
        for _ in 0..rounds {
            if !self.raw.has_ready() {
                return Ok(output);
            }
            let mut ready = self.raw.ready();
            if !ready.snapshot().is_empty() {
                let snapshot = self.pending_snapshot.take().ok_or(Error::Protocol)?;
                if ready.snapshot() != &snapshot.manifest().raft_snapshot()? {
                    return Err(Error::Identity.into());
                }
                let prepared = restore
                    .prepare(&snapshot)
                    .map_err(snapshot::SnapshotDriveError::Application)?;
                let hard = ready
                    .hs()
                    .cloned()
                    .unwrap_or_else(|| self.raw.store().hard.clone());
                let receipt =
                    self.install_remote_snapshot(&snapshot, hard, ready.entries().clone())?;
                if !self.matches_installed(&receipt) {
                    return Err(Error::Identity.into());
                }
                restore
                    .install(prepared, &receipt)
                    .map_err(snapshot::SnapshotDriveError::Application)?;
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
            if !self
                .current_membership()
                .retired()
                .contains(&self.status().node_id)
            {
                output.messages.extend(messages);
            }
            for state in reads {
                if let Ok(context) = <[u8; 32]>::try_from(state.request_ctx.as_slice())
                    && let Some(pending) = self.reads.get_mut(&context)
                    && pending.term == self.raw.raft.term
                {
                    if pending.canceled {
                        self.reads.remove(&context);
                    } else {
                        pending.index = Some(state.index);
                    }
                }
            }
            self.forget_stale_reads();
        }
        output.more_ready = self.raw.has_ready();
        Ok(output)
    }
    fn apply(&mut self, entries: &[Entry], output: &mut Output) -> Result<()> {
        for entry in entries {
            if entry.get_entry_type() == raft::eraftpb::EntryType::EntryConfChangeV2 {
                let (request, cc) = membership::parse_entry(entry)?;
                let expected =
                    self.current_membership()
                        .advance(request, entry.index, entry.term)?;
                let mut actual = self
                    .raw
                    .apply_conf_change(&cc)
                    .map_err(|_| Error::Protocol)?;
                actual.voters.sort_unstable();
                actual.voters_outgoing.sort_unstable();
                actual.learners.sort_unstable();
                actual.learners_next.sort_unstable();
                if actual != expected.configuration() {
                    return Err(Error::Corrupt);
                }
                self.raw.mut_store().persist_conf(expected)?;
            } else {
                self.raw
                    .mut_store()
                    .persist_applied(std::slice::from_ref(entry))?;
                if !entry.data.is_empty() {
                    output.applied.push(applied(entry));
                }
            }
        }
        Ok(())
    }
    fn leader_ready(&self) -> Result<()> {
        self.healthy()?;
        if self.raw.raft.state != StateRole::Leader
            || !self.current_membership().contains(self.status().node_id)
        {
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
        self.reap_dropped_reads();
        if self.reads.len() >= 1024 {
            return Err(Error::Bounds);
        }
        let mut context = [0; 32];
        getrandom::getrandom(&mut context).map_err(|_| Error::Protocol)?;
        if self.reads.contains_key(&context) {
            return Err(Error::Protocol);
        }
        let term = self.raw.raft.term;
        let lease = std::sync::Arc::new(());
        self.reads.insert(
            context,
            PendingRead {
                term,
                index: None,
                canceled: false,
                lease: std::sync::Arc::downgrade(&lease),
            },
        );
        self.raw.read_index(context.to_vec());
        Ok(ReadId {
            context,
            term,
            _lease: lease,
        })
    }
    /// Revoke this exact ticket without a quorum or leader check. Uncompleted
    /// reads retain their capacity charge until Raft produces ReadState (or the
    /// term changes), bounding raft-rs's own pending read queue under isolation.
    pub fn cancel_read(&mut self, id: &ReadId) -> Result<bool> {
        self.healthy()?;
        let Some(pending) = self.reads.get_mut(&id.context) else {
            return Ok(false);
        };
        if pending.term != id.term || pending.canceled {
            return Ok(false);
        }
        if pending.index.is_some() {
            self.reads.remove(&id.context);
        } else {
            pending.canceled = true;
        }
        Ok(true)
    }
    pub fn take_read(&mut self, id: &ReadId) -> Result<Option<ReadBarrier>> {
        self.leader_ready()?;
        if id.term != self.raw.raft.term {
            return Err(Error::NotReady);
        }
        let pending = self.reads.get(&id.context).ok_or(Error::Protocol)?;
        if pending.term != id.term || pending.canceled {
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
    fn reap_dropped_reads(&mut self) {
        self.reads.retain(|_, pending| {
            if pending.lease.strong_count() == 0 {
                pending.canceled = true;
            }
            !pending.canceled || pending.index.is_none()
        });
    }
    fn forget_stale_reads(&mut self) {
        self.reap_dropped_reads();
        if self.raw.raft.state != StateRole::Leader {
            self.reads.clear();
        } else {
            self.reads.retain(|_, r| r.term == self.raw.raft.term);
        }
    }
    pub fn is_leader(&self) -> bool {
        !self.poisoned
            && self.raw.raft.state == StateRole::Leader
            && self.current_membership().contains(self.status().node_id)
    }
    /// The fixed voter set bound to this durable store.
    pub fn configured_voters(&self) -> [u64; 3] {
        self.raw.store().config.voters
    }
    pub fn applied_index(&self) -> u64 {
        self.raw.store().applied
    }
    pub fn snapshot_base_index(&self) -> u64 {
        self.raw.store().base_index()
    }
    pub fn current_snapshot(&self) -> Option<&snapshot::SnapshotManifest> {
        self.raw.store().snapshot.as_ref()
    }
    pub fn installed_generation(&self) -> Option<&snapshot::InstalledGeneration> {
        self.installed.as_ref()
    }
    pub fn matches_installed(&self, receipt: &snapshot::InstalledGeneration) -> bool {
        !self.poisoned
            && self.installed.as_ref().is_some_and(|own| {
                own.owner == receipt.owner
                    && own.node_id == receipt.node_id
                    && own.generation == receipt.generation
                    && own.manifest == receipt.manifest
            })
    }
    /// The durable control-command history; business reducers interpret it separately.
    pub fn history(&self) -> Vec<Applied> {
        self.raw
            .store()
            .log
            .iter()
            .take_while(|e| e.index <= self.raw.store().applied)
            .filter(|e| {
                !e.data.is_empty() && e.get_entry_type() == raft::eraftpb::EntryType::EntryNormal
            })
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

impl driver::RetainedSize for Applied {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.command.capacity()
    }
}

struct RejectSnapshots;
impl snapshot::SnapshotRestore for RejectSnapshots {
    type Prepared = ();
    type Error = Error;
    fn prepare(&mut self, _: &generation::VerifiedSnapshot) -> Result<()> {
        Err(Error::NotReady)
    }
    fn install(&mut self, _: (), _: &snapshot::InstalledGeneration) -> Result<()> {
        Err(Error::NotReady)
    }
}
