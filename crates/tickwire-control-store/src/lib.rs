//! Durable control request admission and completion.
#![forbid(unsafe_code)]
use std::collections::BTreeMap;
use tickwire_control::{Completion, Error, RequestKey, RequestStore, Result, StoredRequest};
use tickwire_ipc::{ControlFrame, StatusBody, Wire};
use tickwire_journal::{Journal, PhysicalFrontier, store::Store};
const LIMIT: u64 = 1 << 63;
#[derive(Clone)]
pub struct Config {
    pub physical: tickwire_journal::Config,
    pub writer_epoch: u64,
    pub owner: [u8; 16],
    pub max_requests: usize,
    pub max_pending: usize,
    pub max_response_bytes: usize,
    pub max_retained_bytes: usize,
}
pub struct DurableRequests<S: Store> {
    journal: Journal<S>,
    config: Config,
    entries: BTreeMap<RequestKey, StoredRequest>,
    pending: usize,
    retained: usize,
    sequence: u64,
    poisoned: bool,
}
impl Config {
    fn validate(&self) -> Result<()> {
        if !(1..LIMIT).contains(&self.writer_epoch)
            || self.owner == [0; 16]
            || self.max_requests == 0
            || self.max_requests > 1_048_576
            || self.max_pending == 0
            || self.max_pending > self.max_requests
            || !(320..=65536).contains(&self.max_response_bytes)
            || self.max_retained_bytes < self.max_response_bytes + 256
            || self.max_retained_bytes > 512 * 1024 * 1024
        {
            return Err(Error::Bounds);
        }
        Ok(())
    }
    fn bytes(&self) -> Vec<u8> {
        let mut out = b"TWCR0001".to_vec();
        out.push(1);
        out.extend(self.owner);
        out.extend(self.physical.domain);
        out.extend(self.physical.stream_epoch);
        out.extend(self.physical.segment_max_bytes.to_le_bytes());
        for n in [
            self.max_requests,
            self.max_pending,
            self.max_response_bytes,
            self.max_retained_bytes,
        ] {
            out.extend((n as u64).to_le_bytes());
        }
        out
    }
}
fn valid_key(key: RequestKey) -> Result<()> {
    if key.subject == [0; 16] || key.request_id == [0; 16] || !(1..LIMIT).contains(&key.epoch) {
        return Err(Error::Identity);
    }
    Ok(())
}
fn prefix(tag: u8, key: RequestKey, digest: [u8; 32]) -> Result<Vec<u8>> {
    valid_key(key)?;
    let mut out = b"TWCR0001".to_vec();
    out.push(tag);
    out.extend(key.subject);
    out.extend(key.epoch.to_le_bytes());
    out.extend(key.request_id);
    out.extend(digest);
    Ok(out)
}
fn completion_bytes(key: RequestKey, digest: [u8; 32], result: &Completion) -> Result<Vec<u8>> {
    let mut out = prefix(3, key, digest)?;
    out.extend(result.state_version.to_le_bytes());
    out.extend((u32::try_from(result.response.len()).map_err(|_| Error::Bounds)?).to_le_bytes());
    out.extend(&result.response);
    Ok(out)
}
fn validate_completion(c: &Config, key: RequestKey, result: &Completion) -> Result<()> {
    if result.response.len() > c.max_response_bytes || result.state_version >= LIMIT {
        return Err(Error::Bounds);
    }
    let f = ControlFrame::decode(&result.response)?;
    let e = &f.envelope;
    if !matches!(e.flags, 10 | 18)
        || e.request_id != key.request_id
        || e.control_epoch != key.epoch
        || e.target_instance != key.subject
        || e.sender_instance != c.owner
        || e.fd_count != 0
    {
        return Err(Error::Identity);
    }
    if matches!(e.opcode, 49 | 51)
        && StatusBody::decode(f.body)?.state_version != result.state_version
    {
        return Err(Error::Version);
    }
    Ok(())
}
impl<S: Store> DurableRequests<S> {
    pub fn create(store: S, config: Config) -> Result<Self> {
        config.validate()?;
        let journal = Journal::create(store, config.physical.clone()).map_err(|_| Error::Store)?;
        let bytes = config.bytes();
        let mut out = Self {
            journal,
            config,
            entries: BTreeMap::new(),
            pending: 0,
            retained: 0,
            sequence: 0,
            poisoned: false,
        };
        out.append(&bytes)?;
        Ok(out)
    }
    pub fn recover(store: S, config: Config, required: Option<&PhysicalFrontier>) -> Result<Self> {
        config.validate()?;
        if required.is_some_and(|f| {
            f.domain != config.physical.domain || f.stream_epoch != config.physical.stream_epoch
        }) {
            return Err(Error::Identity);
        }
        let (journal, recovery) = Journal::recover(store, required).map_err(|_| Error::Store)?;
        if journal.config() != &config.physical || recovery.records.is_empty() {
            return Err(Error::Identity);
        }
        let mut out = Self {
            journal,
            config,
            entries: BTreeMap::new(),
            pending: 0,
            retained: 0,
            sequence: 0,
            poisoned: false,
        };
        let expected = out.config.bytes();
        let mut previous_epoch = 0;
        for record in recovery.records {
            if record.seq != out.sequence + 1
                || record.exec_epoch < previous_epoch
                || record.exec_epoch == 0
                || record.exec_epoch > out.config.writer_epoch
            {
                return Err(Error::Identity);
            }
            previous_epoch = record.exec_epoch;
            if out.sequence == 0 {
                if record.payload != expected {
                    return Err(Error::Identity);
                }
            } else {
                out.restore(&record.payload)?;
            }
            out.retained = out
                .retained
                .checked_add(record.payload.len())
                .ok_or(Error::Capacity)?;
            if out.retained > out.config.max_retained_bytes {
                return Err(Error::Capacity);
            }
            out.sequence += 1;
        }
        Ok(out)
    }
    pub fn position(&self) -> u64 {
        self.sequence
    }
    pub fn poisoned(&self) -> bool {
        self.poisoned
    }
    pub fn pending(&self) -> usize {
        self.pending
    }
    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        if self.poisoned {
            return Err(Error::Store);
        }
        let total = self
            .retained
            .checked_add(bytes.len())
            .ok_or(Error::Capacity)?;
        let next = self.sequence.checked_add(1).ok_or(Error::Capacity)?;
        if total > self.config.max_retained_bytes {
            return Err(Error::Capacity);
        }
        if self
            .journal
            .append_batch(self.config.writer_epoch, &[bytes])
            .is_err()
        {
            self.poisoned = true;
            return Err(Error::Store);
        }
        self.retained = total;
        self.sequence = next;
        Ok(())
    }
    fn room(&self) -> Result<()> {
        if self.entries.len() >= self.config.max_requests || self.pending >= self.config.max_pending
        {
            return Err(Error::Capacity);
        }
        Ok(())
    }
    fn restore(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() < 81 || &bytes[..8] != b"TWCR0001" {
            return Err(Error::Protocol);
        }
        let key = RequestKey {
            subject: bytes[9..25].try_into().unwrap(),
            epoch: u64::from_le_bytes(bytes[25..33].try_into().unwrap()),
            request_id: bytes[33..49].try_into().unwrap(),
        };
        valid_key(key)?;
        let digest = bytes[49..81].try_into().unwrap();
        match bytes[8] {
            2 if bytes.len() == 81 => {
                self.room()?;
                if self
                    .entries
                    .insert(
                        key,
                        StoredRequest {
                            digest,
                            completion: None,
                        },
                    )
                    .is_some()
                {
                    return Err(Error::Conflict);
                }
                self.pending += 1;
            }
            3 if bytes.len() >= 93 => {
                let size = u32::from_le_bytes(bytes[89..93].try_into().unwrap()) as usize;
                if bytes.len() != 93 + size {
                    return Err(Error::Protocol);
                }
                let result = Completion {
                    state_version: u64::from_le_bytes(bytes[81..89].try_into().unwrap()),
                    response: bytes[93..].to_vec(),
                };
                validate_completion(&self.config, key, &result)?;
                let old = self.entries.get_mut(&key).ok_or(Error::Identity)?;
                if old.digest != digest || old.completion.is_some() {
                    return Err(Error::Conflict);
                }
                old.completion = Some(result);
                self.pending -= 1;
            }
            _ => return Err(Error::Protocol),
        }
        Ok(())
    }
}
impl<S: Store> RequestStore for DurableRequests<S> {
    fn lookup(&mut self, key: RequestKey) -> Result<Option<StoredRequest>> {
        if self.poisoned {
            return Err(Error::Store);
        }
        valid_key(key)?;
        Ok(self.entries.get(&key).cloned())
    }
    fn insert_received(&mut self, key: RequestKey, digest: [u8; 32]) -> Result<bool> {
        if self.poisoned {
            return Err(Error::Store);
        }
        valid_key(key)?;
        if let Some(old) = self.entries.get(&key) {
            return if old.digest == digest {
                Ok(false)
            } else {
                Err(Error::Conflict)
            };
        }
        self.room()?;
        self.append(&prefix(2, key, digest)?)?;
        self.entries.insert(
            key,
            StoredRequest {
                digest,
                completion: None,
            },
        );
        self.pending += 1;
        Ok(true)
    }
    fn complete(&mut self, key: RequestKey, digest: [u8; 32], result: Completion) -> Result<()> {
        if self.poisoned {
            return Err(Error::Store);
        }
        valid_key(key)?;
        validate_completion(&self.config, key, &result)?;
        let old = self.entries.get(&key).ok_or(Error::Identity)?;
        if old.digest != digest {
            return Err(Error::Conflict);
        }
        if let Some(saved) = &old.completion {
            return if *saved == result {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        self.append(&completion_bytes(key, digest, &result)?)?;
        self.entries.get_mut(&key).unwrap().completion = Some(result);
        self.pending -= 1;
        Ok(())
    }
}
