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
    pub(crate) log: Vec<Entry>,
    pub(crate) applied: u64,
}
impl Config {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.node_id == 0
            || !self.voters.contains(&self.node_id)
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
    fn identity(&self) -> Vec<u8> {
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
        Self {
            transactions,
            config,
            hard: HardState::default(),
            conf,
            log: Vec::new(),
            applied: 0,
        }
    }
    pub(crate) fn create(store: S, config: Config) -> Result<Self> {
        config.validate()?;
        let transactions = Transactions::create(store, &config)?;
        let mut storage = Self::empty(transactions, config);
        storage.transactions.append(&storage.config.identity())?;
        Ok(storage)
    }
    pub(crate) fn recover(store: S, config: Config) -> Result<Self> {
        config.validate()?;
        let (transactions, records) = Transactions::recover(store, &config)?;
        let mut records = records.into_iter();
        if records.next().as_deref() != Some(config.identity().as_slice()) {
            return Err(Error::Identity);
        }
        let mut storage = Self::empty(transactions, config);
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
                    storage.applied = applied;
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
            || (hard.vote != 0 && !self.config.voters.contains(&hard.vote))
        {
            return Err(Error::Corrupt);
        }
        let mut last = self.log.len() as u64;
        if let Some(first) = entries.first() {
            if first.index == 0 || first.index > last + 1 {
                return Err(Error::Corrupt);
            }
            let mut previous_term = self
                .log
                .get(first.index.saturating_sub(2) as usize)
                .filter(|_| first.index > 1)
                .map_or(0, |entry| entry.term);
            for (n, entry) in entries.iter().enumerate() {
                if entry.index != first.index + n as u64
                    || entry.term == 0
                    || entry.term > hard.term
                    || entry.term < previous_term
                    || entry.get_entry_type() != EntryType::EntryNormal
                    || entry.data.len() > self.config.max_command_bytes
                    || entry.context.len() > 256
                {
                    return Err(Error::Corrupt);
                }
                previous_term = entry.term;
                if entry.index <= self.hard.commit
                    && self.log.get((entry.index - 1) as usize) != Some(entry)
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
            self.log.truncate((first.index - 1) as usize);
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
                || self.log.get((entry.index - 1) as usize) != Some(entry)
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
        if low == 0 || low > high || high > self.log.len() as u64 + 1 {
            return Err(unavailable());
        }
        let mut result = Vec::new();
        let limit = max_size.into().unwrap_or(u64::MAX);
        let mut size = 0u64;
        for entry in &self.log[(low - 1) as usize..(high - 1) as usize] {
            size = size.saturating_add(entry.compute_size() as u64);
            if !result.is_empty() && size > limit {
                break;
            }
            result.push(entry.clone());
        }
        Ok(result)
    }
    fn term(&self, index: u64) -> raft::Result<u64> {
        if index == 0 {
            return Ok(0);
        }
        self.log
            .get((index - 1) as usize)
            .map(|e| e.term)
            .ok_or_else(unavailable)
    }
    fn first_index(&self) -> raft::Result<u64> {
        Ok(1)
    }
    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.log.len() as u64)
    }
    fn snapshot(&self, _: u64, _: u64) -> raft::Result<Snapshot> {
        Err(raft::Error::Store(
            StorageError::SnapshotTemporarilyUnavailable,
        ))
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
