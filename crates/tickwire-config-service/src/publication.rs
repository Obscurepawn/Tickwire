//! Durable prepared content, without desired revision or runtime authority.
use crate::{
    Change, Error, Hash, Identity, ObjectSpec, Purpose, Result, bootstrap::CommittedHead, objects,
};
use std::collections::BTreeMap;

pub(crate) const MAX_RECORDS: usize = 4096;
pub(crate) const MAX_RECORD_BYTES: usize = 1024;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordPrepared {
    pub id: Identity,
    pub subject: String,
    pub leaf_sha256: Hash,
    pub idempotency_key: String,
    pub request_digest: Hash,
    pub if_match: String,
    pub root: ObjectSpec,
    pub admitted_unix_ms: u64,
}
impl RecordPrepared {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.id == [0; 16]
            || !crate::auth::valid_subject(&self.subject)
            || self.leaf_sha256 == [0; 32]
            || self.request_digest == [0; 32]
            || self.root.owner != self.id
            || self.root.purpose != Purpose::PlanMetadata
            || self.root.hash == [0; 32]
        {
            return Err(Error::Identity);
        }
        if !valid_key(&self.idempotency_key)
            || self.if_match.is_empty()
            || self.if_match.len() > 128
            || !self.if_match.bytes().all(|b| (33..=126).contains(&b))
            || self.admitted_unix_ms == 0
            || self.admitted_unix_ms >= 1 << 53
        {
            return Err(Error::Bounds);
        }
        self.root.validate()
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPrepared {
    command: RecordPrepared,
    index: u64,
    term: u64,
}
impl StoredPrepared {
    pub fn id(&self) -> Identity {
        self.command.id
    }
    pub fn subject(&self) -> &str {
        &self.command.subject
    }
    pub fn leaf_sha256(&self) -> Hash {
        self.command.leaf_sha256
    }
    pub fn idempotency_key(&self) -> &str {
        &self.command.idempotency_key
    }
    pub fn request_digest(&self) -> Hash {
        self.command.request_digest
    }
    pub fn if_match(&self) -> &str {
        &self.command.if_match
    }
    pub fn root(&self) -> &ObjectSpec {
        &self.command.root
    }
    pub fn admitted_unix_ms(&self) -> u64 {
        self.command.admitted_unix_ms
    }
    pub fn index(&self) -> u64 {
        self.index
    }
    pub fn term(&self) -> u64 {
        self.term
    }
    pub(crate) fn command(&self) -> &RecordPrepared {
        &self.command
    }
}
#[derive(Debug, Clone, Copy)]
pub enum PreparedQuery<'a> {
    Id(Identity),
    Idempotency { subject: &'a str, key: &'a str },
}
#[derive(Debug)]
pub struct PreparedView {
    pub(crate) head: Option<Box<CommittedHead>>,
    pub(crate) record: Option<StoredPrepared>,
}
impl PreparedView {
    pub fn head(&self) -> Option<&CommittedHead> {
        self.head.as_deref()
    }
    pub fn record(&self) -> Option<&StoredPrepared> {
        self.record.as_ref()
    }
}
#[derive(Debug)]
pub enum PreparedRead {
    Pending,
    Ready(Box<PreparedView>),
}

fn valid_key(key: &str) -> bool {
    (1..=128).contains(&key.len()) && key.bytes().all(|b| (33..=126).contains(&b))
}
#[derive(Default)]
pub(crate) struct State {
    records: BTreeMap<Identity, StoredPrepared>,
    keys: BTreeMap<(String, String), Identity>,
}
impl State {
    pub(crate) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
    pub(crate) fn get(&self, query: PreparedQuery<'_>) -> Result<Option<StoredPrepared>> {
        let id = match query {
            PreparedQuery::Id(id) => {
                if id == [0; 16] {
                    return Err(Error::Identity);
                }
                Some(id)
            }
            PreparedQuery::Idempotency { subject, key } => {
                if !crate::auth::valid_subject(subject) || !valid_key(key) {
                    return Err(Error::Bounds);
                }
                self.keys
                    .get(&(subject.to_owned(), key.to_owned()))
                    .copied()
            }
        };
        Ok(id.and_then(|id| self.records.get(&id).cloned()))
    }
    pub(crate) fn apply(
        &mut self,
        record: RecordPrepared,
        objects: &objects::State,
        head: &CommittedHead,
        index: u64,
        term: u64,
    ) -> Result<Change> {
        record.validate()?;
        if index <= head.raft_index() || term < head.raft_term() {
            return Err(Error::Protocol);
        }
        let key = (record.subject.clone(), record.idempotency_key.clone());
        if let Some(id) = self.keys.get(&key) {
            let old = self.records.get(id).ok_or(Error::Protocol)?;
            return if old.command == record {
                Ok(change(old))
            } else {
                Err(Error::Conflict)
            };
        }
        if self.records.contains_key(&record.id) {
            return Err(Error::Conflict);
        }
        if self.records.len() >= MAX_RECORDS {
            return Err(Error::Capacity);
        }
        crate::plan_archive::validate_record(&record, objects, head)?;
        let stored = StoredPrepared {
            command: record,
            index,
            term,
        };
        let result = change(&stored);
        self.keys.insert(key, stored.id());
        self.records.insert(stored.id(), stored);
        Ok(result)
    }
    pub(crate) fn snapshot(&self, cluster: Identity) -> Result<Vec<u8>> {
        let mut bytes = b"TWCP0001".to_vec();
        bytes.extend_from_slice(&(self.records.len() as u32).to_le_bytes());
        for stored in self.records.values() {
            let command = crate::codec::record_prepared(cluster, stored.command())?;
            bytes.extend_from_slice(&stored.index.to_le_bytes());
            bytes.extend_from_slice(&stored.term.to_le_bytes());
            bytes.extend_from_slice(&(command.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&command);
        }
        if bytes.len() > SNAPSHOT_BYTES {
            return Err(Error::Capacity);
        }
        Ok(bytes)
    }
    pub(crate) fn restore(
        bytes: &[u8],
        cluster: Identity,
        objects: &objects::State,
        head: &CommittedHead,
        cut_index: u64,
        cut_term: u64,
    ) -> Result<Self> {
        if bytes.len() > SNAPSHOT_BYTES {
            return Err(Error::Bounds);
        }
        let mut reader = Reader(bytes);
        if reader.take(8)? != b"TWCP0001" {
            return Err(Error::Protocol);
        }
        let count = reader.u32()? as usize;
        if count == 0 || count > MAX_RECORDS {
            return Err(Error::Bounds);
        }
        let mut state = Self::default();
        let mut previous = None;
        let mut positions = BTreeMap::new();
        for _ in 0..count {
            let index = reader.u64()?;
            let term = reader.u64()?;
            if index == 0
                || index > cut_index
                || term == 0
                || term > cut_term
                || positions.insert(index, term).is_some()
            {
                return Err(Error::Protocol);
            }
            let length = reader.u32()? as usize;
            if length > MAX_RECORD_BYTES {
                return Err(Error::Bounds);
            }
            let crate::codec::Decoded::RecordPrepared(record) =
                crate::codec::decode(cluster, reader.take(length)?)?
            else {
                return Err(Error::Protocol);
            };
            if previous.is_some_and(|id| id >= record.id) {
                return Err(Error::Protocol);
            }
            previous = Some(record.id);
            state.apply(record, objects, head, index, term)?;
        }
        if !reader.0.is_empty() || state.records.len() != count {
            return Err(Error::Protocol);
        }
        let mut previous_term = 0;
        for term in positions.values() {
            if *term < previous_term {
                return Err(Error::Protocol);
            }
            previous_term = *term;
        }
        Ok(state)
    }
}
pub(crate) const SNAPSHOT_BYTES: usize = 12 + MAX_RECORDS * (MAX_RECORD_BYTES + 20);
fn change(stored: &StoredPrepared) -> Change {
    Change::PreparedStored {
        id: stored.id(),
        index: stored.index,
        term: stored.term,
    }
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let (bytes, rest) = self.0.split_at_checked(n).ok_or(Error::Protocol)?;
        self.0 = rest;
        Ok(bytes)
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().map_err(|_| Error::Protocol)?,
        ))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| Error::Protocol)?,
        ))
    }
}

#[cfg(test)]
mod generation_tests;
#[cfg(test)]
mod state_tests;

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Purpose;
    pub(crate) fn record() -> RecordPrepared {
        RecordPrepared {
            id: [1; 16],
            subject: "spiffe://tickwire.test/operators/admin".into(),
            leaf_sha256: [2; 32],
            idempotency_key: "00000000-0000-4000-8000-000000000001".into(),
            request_digest: [3; 32],
            if_match: format!("\"cfg-1-{}\"", "ab".repeat(32)),
            root: ObjectSpec {
                key: [4; 16],
                owner: [1; 16],
                purpose: Purpose::PlanMetadata,
                length: 100,
                hash: [5; 32],
            },
            admitted_unix_ms: 1_800_000_000_000,
        }
    }
    #[test]
    fn prepared_record_has_bounded_canonical_identity_and_root() {
        let valid = record();
        valid.validate().unwrap();
        for case in 0..11 {
            let mut bad = valid.clone();
            match case {
                0 => bad.id = [0; 16],
                1 => bad.subject = "admin".into(),
                2 => bad.leaf_sha256 = [0; 32],
                3 => bad.idempotency_key = String::new(),
                4 => bad.idempotency_key = "x".repeat(129),
                5 => bad.request_digest = [0; 32],
                6 => bad.if_match = String::new(),
                7 => bad.root.owner = [9; 16],
                8 => bad.root.purpose = Purpose::Receipt,
                9 => bad.admitted_unix_ms = 0,
                _ => bad.admitted_unix_ms = 1u64 << 53,
            }
            assert!(bad.validate().is_err(), "case {case}");
        }
    }
}
