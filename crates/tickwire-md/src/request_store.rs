//! Bounded pure in-memory JOIN control request idempotency store (zero disk I/O).
use std::collections::{BTreeMap, VecDeque};
use tickwire_control::{Completion, Error, RequestKey, RequestStore, Result, StoredRequest};

pub const DEFAULT_MAX_JOIN_REQUESTS: usize = 4_096;
pub const MAX_JOIN_REQUESTS_CAP: usize = 65_536;
pub const DEFAULT_MAX_PENDING_REQUESTS: usize = 128;
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 65_536;

/// Stateless bounded in-memory [`RequestStore`] for `tickwire-md` control sessions.
///
/// Completed entries are evicted deterministically in FIFO completion order when `capacity`
/// is reached; `Pending` entries are never evicted.
#[derive(Debug)]
pub struct BoundedRequestStore {
    capacity: usize,
    max_pending: usize,
    max_response_bytes: usize,
    pending_count: usize,
    entries: BTreeMap<RequestKey, StoredRequest>,
    completed_fifo: VecDeque<RequestKey>,
}

impl BoundedRequestStore {
    pub fn new(capacity: usize) -> Result<Self> {
        let max_pending = DEFAULT_MAX_PENDING_REQUESTS.min(capacity);
        Self::with_limits(capacity, max_pending, DEFAULT_MAX_RESPONSE_BYTES)
    }

    pub fn with_limits(
        capacity: usize,
        max_pending: usize,
        max_response_bytes: usize,
    ) -> Result<Self> {
        if capacity == 0
            || capacity > MAX_JOIN_REQUESTS_CAP
            || max_pending == 0
            || max_pending > capacity
            || max_response_bytes == 0
            || max_response_bytes > 1_048_576
        {
            return Err(Error::Bounds);
        }
        Ok(Self {
            capacity,
            max_pending,
            max_response_bytes,
            pending_count: 0,
            entries: BTreeMap::new(),
            completed_fifo: VecDeque::with_capacity(capacity.min(256)),
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn pending_count(&self) -> usize {
        self.pending_count
    }

    pub fn completed_count(&self) -> usize {
        self.completed_fifo.len()
    }
}

impl Default for BoundedRequestStore {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_JOIN_REQUESTS).expect("default BoundedRequestStore limits")
    }
}

impl RequestStore for BoundedRequestStore {
    fn lookup(&mut self, key: RequestKey) -> Result<Option<StoredRequest>> {
        Ok(self.entries.get(&key).cloned())
    }

    fn insert_received(&mut self, key: RequestKey, digest: [u8; 32]) -> Result<bool> {
        if let Some(existing) = self.entries.get(&key) {
            if existing.digest != digest {
                return Err(Error::Conflict);
            }
            return Ok(false);
        }
        if self.pending_count >= self.max_pending {
            return Err(Error::Capacity);
        }
        while self.entries.len() >= self.capacity {
            let Some(oldest) = self.completed_fifo.pop_front() else {
                return Err(Error::Capacity);
            };
            let removed = self.entries.remove(&oldest);
            debug_assert!(removed.is_some_and(|r| r.completion.is_some()));
        }
        self.entries.insert(
            key,
            StoredRequest {
                digest,
                completion: None,
            },
        );
        self.pending_count += 1;
        Ok(true)
    }

    fn complete(&mut self, key: RequestKey, digest: [u8; 32], result: Completion) -> Result<()> {
        if result.response.len() > self.max_response_bytes {
            return Err(Error::Bounds);
        }
        let Some(entry) = self.entries.get_mut(&key) else {
            return Err(Error::Store);
        };
        if entry.digest != digest {
            return Err(Error::Conflict);
        }
        if let Some(existing) = &entry.completion {
            return if existing == &result {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        entry.completion = Some(result);
        self.pending_count = self.pending_count.checked_sub(1).ok_or(Error::Store)?;
        self.completed_fifo.push_back(key);
        Ok(())
    }
}
