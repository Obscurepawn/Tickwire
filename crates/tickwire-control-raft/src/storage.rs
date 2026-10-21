use crate::{Config, Error, Result, transactions::Transactions};
use protobuf::Message as PbMessage;
use raft::{
    Storage, StorageError,
    eraftpb::{ConfState, Entry, EntryType, HardState, Snapshot},
    storage::{GetEntriesContext, RaftState},
};
use tickwire_journal::store::Store;

pub(crate) struct DurableStorage<S: Store> {
    transactions: Transactions<S>,
    pub(crate) config: Config,
    pub(crate) hard: HardState,
    conf: ConfState,
    conf_applied: u64,
    pub(crate) member: crate::driver::Membership,
    pub(crate) joining: bool,
    pub(crate) log: Vec<Entry>,
    pub(crate) applied: u64,
    pub(crate) snapshot: Option<crate::snapshot::SnapshotManifest>,
}
impl Config {
    pub(crate) fn validate(&self) -> Result<()> {
        self.validate_identity()?;
        if !self.voters.contains(&self.node_id) {
            return Err(Error::Identity);
        }
        Ok(())
    }
    pub(crate) fn validate_identity(&self) -> Result<()> {
        if self.node_id == 0
            || self.voters.contains(&0)
            || self.voters[0] >= self.voters[1]
            || self.voters[1] >= self.voters[2]
            || self.writer_epoch == 0
        {
            return Err(Error::Identity);
        }
        if !(1..=16 * 1024 * 1024).contains(&self.max_command_bytes)
            || !(1024 * 1024..=1024 * 1024 * 1024).contains(&self.max_retained_bytes)
            || self.max_command_bytes > self.max_retained_bytes / 4
        {
            return Err(Error::Bounds);
        }
        Ok(())
    }
    pub(crate) fn identity(&self) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(&self.physical.domain);
        bytes.extend_from_slice(&self.physical.stream_epoch);
        for n in [
            self.node_id,
            self.voters[0],
            self.voters[1],
            self.voters[2],
            self.max_command_bytes as u64,
            self.max_retained_bytes as u64,
        ] {
            bytes.extend_from_slice(&n.to_le_bytes());
        }
        bytes
    }
}
impl<S: Store> DurableStorage<S> {
    fn empty(transactions: Transactions<S>, config: Config) -> Self {
        let conf = ConfState {
            voters: config.voters.to_vec(),
            ..Default::default()
        };
        let member = crate::driver::Membership::initial(config.voters);
        Self {
            joining: false,
            member,
            transactions,
            config,
            hard: HardState::default(),
            conf,
            conf_applied: 0,
            log: Vec::new(),
            applied: 0,
            snapshot: None,
        }
    }
    pub(crate) fn create(store: S, config: Config) -> Result<Self> {
        config.validate()?;
        let transactions = Transactions::create(store, &config)?;
        let mut storage = Self::empty(transactions, config);
        storage.transactions.append(&storage.config.identity())?;
        Ok(storage)
    }
    pub(crate) fn create_joining(store: S, config: Config) -> Result<Self> {
        config.validate_identity()?;
        if config.voters.contains(&config.node_id) {
            return Err(Error::Identity);
        }
        let transactions = Transactions::create(store, &config)?;
        let mut storage = Self::empty(transactions, config);
        storage.transactions.append(&storage.config.identity())?;
        storage.transactions.append(&[6])?;
        storage.joining = true;
        Ok(storage)
    }
    pub(crate) fn recover(store: S, config: Config) -> Result<Self> {
        config.validate_identity()?;
        let (transactions, records) = Transactions::recover(store, &config)?;
        let mut records = records.into_iter().peekable();
        if records.next().as_deref() != Some(config.identity().as_slice()) {
            return Err(Error::Identity);
        }
        let joining = !config.voters.contains(&config.node_id);
        if joining {
            if records.next().as_deref() != Some(&[6]) {
                return Err(Error::Identity);
            }
        } else if records.peek().is_some_and(|r| r == &[6]) {
            return Err(Error::Identity);
        }
        let mut storage = Self::empty(transactions, config);
        storage.joining = joining;
        for bytes in records {
            let mut r = Reader(&bytes);
            match r.byte()? {
                2 => {
                    let hard =
                        HardState::parse_from_bytes(r.bytes()?).map_err(|_| Error::Corrupt)?;
                    let count = r.u64()?;
                    if count > 262_144 {
                        return Err(Error::Bounds);
                    }
                    let mut entries = Vec::new();
                    for _ in 0..count {
                        entries
                            .push(Entry::parse_from_bytes(r.bytes()?).map_err(|_| Error::Corrupt)?);
                    }
                    if !r.0.is_empty() {
                        return Err(Error::Corrupt);
                    }
                    storage.check_update(&hard, &entries)?;
                    storage.install_update(hard, entries);
                }
                3 => {
                    let applied = r.u64()?;
                    if !r.0.is_empty() || applied < storage.applied || applied > storage.hard.commit
                    {
                        return Err(Error::Corrupt);
                    }
                    if storage.log.iter().any(|e| {
                        e.index > storage.applied
                            && e.index <= applied
                            && e.get_entry_type() != EntryType::EntryNormal
                    }) {
                        return Err(Error::Corrupt);
                    }
                    storage.applied = applied;
                }
                4 if storage.snapshot.is_none()
                    && storage.log.is_empty()
                    && storage.hard == HardState::default()
                    && storage.applied == 0 =>
                {
                    let manifest = crate::snapshot::SnapshotManifest::decode(
                        r.bytes()?,
                        crate::snapshot::SnapshotLimits {
                            max_closure_bytes: crate::snapshot::MAX_CLOSURE_BYTES,
                            ..Default::default()
                        },
                    )?;
                    let hard =
                        HardState::parse_from_bytes(r.bytes()?).map_err(|_| Error::Corrupt)?;
                    let applied = r.u64()?;
                    let count = r.u64()?;
                    if count > 262_144 {
                        return Err(Error::Bounds);
                    }
                    let mut entries = Vec::new();
                    for _ in 0..count {
                        entries
                            .push(Entry::parse_from_bytes(r.bytes()?).map_err(|_| Error::Corrupt)?);
                    }
                    if !r.0.is_empty() {
                        return Err(Error::Corrupt);
                    }
                    storage.install_seed(manifest, hard, entries, applied)?;
                }
                5 => {
                    let member = crate::driver::Membership::decode_history(r.bytes()?)?;
                    if !r.0.is_empty() {
                        return Err(Error::Corrupt);
                    }
                    storage.check_conf(&member)?;
                    storage.install_conf(member);
                }
                _ => return Err(Error::Corrupt),
            }
        }
        Ok(storage)
    }
    fn check_update(&self, hard: &HardState, entries: &[Entry]) -> Result<()> {
        if hard.term < self.hard.term
            || hard.commit < self.hard.commit
            || (hard.term == self.hard.term && self.hard.vote != 0 && hard.vote != self.hard.vote)
            || (hard.vote != 0
                && !self.member.contains(hard.vote)
                && !(hard.term == self.hard.term && hard.vote == self.hard.vote))
        {
            return Err(Error::Corrupt);
        }
        let base = self.base_index();
        let mut last = base + self.log.len() as u64;
        if let Some(first) = entries.first() {
            if first.index <= base || first.index > last + 1 {
                return Err(Error::Corrupt);
            }
            let mut previous_term = self.term(first.index - 1).map_err(|_| Error::Corrupt)?;
            for (n, entry) in entries.iter().enumerate() {
                if entry.index != first.index + n as u64
                    || entry.term == 0
                    || entry.term > hard.term
                    || entry.term < previous_term
                    || !matches!(
                        entry.get_entry_type(),
                        EntryType::EntryNormal | EntryType::EntryConfChangeV2
                    )
                    || entry.data.len() > self.config.max_command_bytes
                    || entry.context.len() > 256
                {
                    return Err(Error::Corrupt);
                }
                if entry.get_entry_type() == EntryType::EntryConfChangeV2 {
                    crate::membership::parse_entry(entry)?;
                }
                previous_term = entry.term;
                if entry.index <= self.hard.commit
                    && self.log.get((entry.index - base - 1) as usize) != Some(entry)
                {
                    return Err(Error::Corrupt);
                }
            }
            last = entries.last().unwrap().index;
        }
        if hard.commit > last || self.applied > hard.commit {
            return Err(Error::Corrupt);
        }
        Ok(())
    }
    fn install_update(&mut self, hard: HardState, entries: Vec<Entry>) {
        if let Some(first) = entries.first() {
            self.log
                .truncate((first.index - self.base_index() - 1) as usize);
            self.log.extend(entries);
        }
        self.hard = hard;
    }
    pub(crate) fn persist(&mut self, hard: Option<&HardState>, entries: &[Entry]) -> Result<()> {
        let hard = hard.unwrap_or(&self.hard).clone();
        self.check_update(&hard, entries)?;
        if hard == self.hard && entries.is_empty() {
            return Ok(());
        }
        let mut bytes = vec![2];
        put_bytes(
            &mut bytes,
            &hard.write_to_bytes().map_err(|_| Error::Protocol)?,
        )?;
        bytes.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for entry in entries {
            put_bytes(
                &mut bytes,
                &entry.write_to_bytes().map_err(|_| Error::Protocol)?,
            )?;
        }
        self.transactions.append(&bytes)?;
        self.install_update(hard, entries.to_vec());
        Ok(())
    }
    pub(crate) fn persist_applied(&mut self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        for (n, entry) in entries.iter().enumerate() {
            if entry.index != self.applied + n as u64 + 1
                || entry.index > self.hard.commit
                || self.log.get((entry.index - self.base_index() - 1) as usize) != Some(entry)
            {
                return Err(Error::Corrupt);
            }
        }
        let next = entries.last().unwrap().index;
        let mut bytes = vec![3];
        bytes.extend_from_slice(&next.to_le_bytes());
        self.transactions.append(&bytes)?;
        self.applied = next;
        Ok(())
    }
    pub(crate) fn membership(&self) -> crate::driver::Membership {
        self.member.clone()
    }
    fn check_conf(&self, next: &crate::driver::Membership) -> Result<()> {
        let index = next.applied_index();
        if index != self.applied + 1 || index > self.hard.commit {
            return Err(Error::Corrupt);
        }
        let entry = self
            .log
            .get((index - self.base_index() - 1) as usize)
            .ok_or(Error::Corrupt)?;
        let (request, _) = crate::membership::parse_entry(entry)?;
        if &self.member.advance(request, index, entry.term)? != next {
            return Err(Error::Corrupt);
        }
        Ok(())
    }
    fn install_conf(&mut self, member: crate::driver::Membership) {
        self.conf = member.configuration();
        self.conf_applied = member.applied_index();
        self.applied = member.applied_index();
        self.member = member;
    }
    pub(crate) fn persist_conf(&mut self, member: crate::driver::Membership) -> Result<()> {
        self.check_conf(&member)?;
        let mut bytes = vec![5];
        put_bytes(&mut bytes, &member.encode_history()?)?;
        self.transactions.append(&bytes)?;
        self.install_conf(member);
        Ok(())
    }
    pub(crate) fn base_index(&self) -> u64 {
        self.snapshot.as_ref().map_or(0, |s| s.cut().index())
    }
    pub(crate) fn seed(
        store: S,
        config: Config,
        manifest: crate::snapshot::SnapshotManifest,
        hard: HardState,
        tail: Vec<Entry>,
        applied: u64,
    ) -> Result<Self> {
        let mut storage = if config.voters.contains(&config.node_id) {
            Self::create(store, config)?
        } else {
            Self::create_joining(store, config)?
        };
        storage.install_seed(manifest.clone(), hard.clone(), tail.clone(), applied)?;
        let mut bytes = vec![4];
        put_bytes(&mut bytes, &manifest.encode()?)?;
        put_bytes(
            &mut bytes,
            &hard.write_to_bytes().map_err(|_| Error::Protocol)?,
        )?;
        bytes.extend_from_slice(&applied.to_le_bytes());
        bytes.extend_from_slice(&(tail.len() as u64).to_le_bytes());
        for entry in tail {
            put_bytes(
                &mut bytes,
                &entry.write_to_bytes().map_err(|_| Error::Protocol)?,
            )?;
        }
        storage.transactions.append(&bytes)?;
        Ok(storage)
    }
    fn install_seed(
        &mut self,
        manifest: crate::snapshot::SnapshotManifest,
        hard: HardState,
        tail: Vec<Entry>,
        applied: u64,
    ) -> Result<()> {
        let cut = manifest.cut();
        if cut.group() != self.config.physical.domain
            || hard.term < cut.term()
            || hard.commit < cut.index()
            || applied < cut.index()
            || applied > hard.commit
        {
            return Err(Error::Corrupt);
        }
        let m = cut.membership();
        if m.origin != self.config.voters {
            return Err(Error::Identity);
        }
        self.member = m.clone();
        self.conf = ConfState {
            voters: m.voters().to_vec(),
            voters_outgoing: m.voters_outgoing().to_vec(),
            learners: m.learners().to_vec(),
            learners_next: m.learners_next().to_vec(),
            auto_leave: m.auto_leave(),
            ..Default::default()
        };
        self.conf_applied = m.applied_index();
        self.hard = HardState {
            term: cut.term(),
            commit: cut.index(),
            ..Default::default()
        };
        self.applied = cut.index();
        self.snapshot = Some(manifest);
        self.check_update(&hard, &tail)?;
        self.install_update(hard, tail);
        self.applied = applied;
        Ok(())
    }
}
fn unavailable() -> raft::Error {
    raft::Error::Store(StorageError::Unavailable)
}
impl<S: Store> Storage for DurableStorage<S> {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState {
            hard_state: self.hard.clone(),
            conf_state: self.conf.clone(),
        })
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let base = self.base_index();
        if low <= base {
            return Err(raft::Error::Store(StorageError::Compacted));
        }
        if low > high || high > base + self.log.len() as u64 + 1 {
            return Err(unavailable());
        }
        let mut result = Vec::new();
        let limit = max_size.into().unwrap_or(u64::MAX);
        let mut size = 0u64;
        for entry in &self.log[(low - base - 1) as usize..(high - base - 1) as usize] {
            size = size.saturating_add(entry.compute_size() as u64);
            if !result.is_empty() && size > limit {
                break;
            }
            result.push(entry.clone());
        }
        Ok(result)
    }
    fn term(&self, index: u64) -> raft::Result<u64> {
        let base = self.base_index();
        if index < base {
            return Err(raft::Error::Store(StorageError::Compacted));
        }
        if index == base {
            return Ok(self.snapshot.as_ref().map_or(0, |s| s.cut().term()));
        }
        self.log
            .get((index - base - 1) as usize)
            .map(|e| e.term)
            .ok_or_else(unavailable)
    }
    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.base_index() + 1)
    }
    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.base_index() + self.log.len() as u64)
    }
    fn snapshot(&self, request: u64, _: u64) -> raft::Result<Snapshot> {
        let snapshot = self
            .snapshot
            .as_ref()
            .filter(|s| s.cut().index() >= request)
            .ok_or(raft::Error::Store(
                StorageError::SnapshotTemporarilyUnavailable,
            ))?;
        snapshot.raft_snapshot().map_err(|_| unavailable())
    }
}
fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| Error::Bounds)?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        if len > self.0.len() {
            return Err(Error::Corrupt);
        }
        let (head, tail) = self.0.split_at(len);
        self.0 = tail;
        Ok(head)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize;
        self.take(len)
    }
}
