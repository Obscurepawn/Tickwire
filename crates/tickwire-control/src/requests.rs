use crate::{Error, LIMIT, Result, Session};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tickwire_ipc::ControlFrame;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RequestKey {
    pub subject: [u8; 16],
    pub epoch: u64,
    pub request_id: [u8; 16],
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completion {
    pub response: Vec<u8>,
    pub state_version: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRequest {
    pub digest: [u8; 32],
    pub completion: Option<Completion>,
}

/// One logical owner serializes calls. Production implementations must durably commit
/// insert/completion before returning success, preserve terminal results, and implement
/// insert-if-absent atomically. An ambiguous failure must remain discoverable via lookup.
/// Owner resource changes and economic operation identities require separate recovery.
pub trait RequestStore {
    fn lookup(&mut self, key: RequestKey) -> Result<Option<StoredRequest>>;
    /// Returns true only for the caller that inserted Received for the first time.
    fn insert_received(&mut self, key: RequestKey, digest: [u8; 32]) -> Result<bool>;
    /// Compare digest, then set once (or accept a byte-identical prior completion).
    fn complete(&mut self, key: RequestKey, digest: [u8; 32], result: Completion) -> Result<()>;
}

#[derive(Debug)]
pub enum Admission {
    Execute(ExecutionTicket),
    Pending,
    Completed(Completion),
}
/// Admission returns this ticket once; callers cannot construct or clone it.
/// Persisting a completion can be retried with the same bytes after a store error.
#[derive(Debug)]
pub struct ExecutionTicket {
    key: RequestKey,
    digest: [u8; 32],
    owner: [u8; 16],
    operation: [u8; 16],
    revision: u64,
    expected: u64,
    opcode: u16,
}
impl ExecutionTicket {
    pub fn complete<S: RequestStore>(
        &self,
        store: &mut S,
        response: &[u8],
        state_version: u64,
    ) -> Result<()> {
        let frame = ControlFrame::decode(response)?;
        let e = &frame.envelope;
        if state_version >= LIMIT
            || !matches!(e.flags, 10 | 18)
            || e.request_id != self.key.request_id
            || e.control_epoch != self.key.epoch
            || e.sender_instance != self.owner
            || e.target_instance != self.key.subject
            || e.operation_id != self.operation
            || e.operation_revision != self.revision
            || e.expected_state_version != self.expected
            || e.fd_count != 0
        {
            return Err(Error::Protocol);
        }
        let response_opcode = if e.flags == 18 {
            e.opcode == 51
                || (self.opcode == 19 && e.opcode == 19)
                || ((80..=84).contains(&self.opcode) && e.opcode == 84)
        } else {
            e.opcode
                == match self.opcode {
                    16 | 19 | 20 => self.opcode,
                    53 => 54,
                    80 => 80,
                    81 => 82,
                    82..=84 => 84,
                    _ => 49,
                }
        };
        if !response_opcode {
            return Err(Error::Protocol);
        }
        if matches!(e.opcode, 49 | 51) {
            use tickwire_ipc::Wire;
            if tickwire_ipc::StatusBody::decode(frame.body)?.state_version != state_version {
                return Err(Error::Version);
            }
        }
        store.complete(
            self.key,
            self.digest,
            Completion {
                response: response.to_vec(),
                state_version,
            },
        )
    }
}
fn digest(frame: &ControlFrame<'_>) -> [u8; 32] {
    let e = &frame.envelope;
    let mut hash = Sha256::new();
    hash.update(e.opcode.to_le_bytes());
    hash.update(e.operation_id);
    hash.update(e.operation_revision.to_le_bytes());
    hash.update(e.expected_state_version.to_le_bytes());
    hash.update(frame.body);
    hash.finalize().into()
}
fn replay(entry: StoredRequest, digest: [u8; 32]) -> Result<Admission> {
    if entry.digest != digest {
        return Err(Error::Conflict);
    }
    Ok(match entry.completion {
        Some(v) => Admission::Completed(v),
        None => Admission::Pending,
    })
}
impl Session {
    /// Validate session and register a request before delivering the only dispatch ticket.
    /// A pending retry needs owner reconciliation, not a second dispatch.
    pub fn admit<S: RequestStore>(
        &mut self,
        store: &mut S,
        bytes: &[u8],
        now: u64,
        current_version: u64,
    ) -> Result<Admission> {
        let frame = self.validate(bytes, now)?;
        let e = &frame.envelope;
        if e.flags != 1 || current_version >= LIMIT {
            return Err(Error::Protocol);
        }
        let key = RequestKey {
            subject: self.policy.peer.instance,
            epoch: self.epoch(),
            request_id: e.request_id,
        };
        let digest = digest(&frame);
        // Retries must see their saved outcome even when the owner version has advanced.
        if let Some(entry) = store.lookup(key)? {
            return replay(entry, digest);
        }
        if e.expected_state_version != current_version {
            return Err(Error::Version);
        }
        if !store.insert_received(key, digest)? {
            return replay(store.lookup(key)?.ok_or(Error::Store)?, digest);
        }
        Ok(Admission::Execute(ExecutionTicket {
            key,
            digest,
            owner: self.policy.local.instance,
            operation: e.operation_id,
            revision: e.operation_revision,
            expected: e.expected_state_version,
            opcode: e.opcode,
        }))
    }
}

/// Bounded in-memory test service. Keeps records across Session values but not process death.
pub struct MockRequestStore {
    entries: BTreeMap<RequestKey, StoredRequest>,
    capacity: usize,
    fail_next: bool,
}
impl MockRequestStore {
    pub fn new(capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(Error::Capacity);
        }
        Ok(Self {
            entries: BTreeMap::new(),
            capacity,
            fail_next: false,
        })
    }
    pub fn fail_next(&mut self) {
        self.fail_next = true;
    }
    fn maybe_fail(&mut self) -> Result<()> {
        if std::mem::take(&mut self.fail_next) {
            Err(Error::Store)
        } else {
            Ok(())
        }
    }
}
impl RequestStore for MockRequestStore {
    fn lookup(&mut self, key: RequestKey) -> Result<Option<StoredRequest>> {
        self.maybe_fail()?;
        Ok(self.entries.get(&key).cloned())
    }
    fn insert_received(&mut self, key: RequestKey, digest: [u8; 32]) -> Result<bool> {
        self.maybe_fail()?;
        if self.entries.contains_key(&key) {
            return Ok(false);
        }
        if self.entries.len() >= self.capacity
            || self
                .entries
                .values()
                .filter(|v| v.completion.is_none())
                .count()
                >= 256
        {
            return Err(Error::Capacity);
        }
        self.entries.insert(
            key,
            StoredRequest {
                digest,
                completion: None,
            },
        );
        Ok(true)
    }
    fn complete(&mut self, key: RequestKey, digest: [u8; 32], result: Completion) -> Result<()> {
        self.maybe_fail()?;
        let entry = self.entries.get_mut(&key).ok_or(Error::Store)?;
        if entry.digest != digest {
            return Err(Error::Conflict);
        }
        if let Some(old) = &entry.completion {
            return if old == &result {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        entry.completion = Some(result);
        Ok(())
    }
}
