//! Preallocated fixed-capacity hash-indexed arena table with monotonic time-window eviction.
//!
//! Slots and hash buckets are allocated once in [`WindowedTable::with_capacity`]. Active entries
//! (`retired_at_mono_ns == None`) are never eligible for eviction regardless of clock advancement;
//! retired entries (`retired_at_mono_ns == Some(t)`) become eligible for lazy reclamation once
//! `window_ns.is_some_and(|w| now_mono_ns >= t.saturating_add(w))`.

use std::{
    collections::hash_map::DefaultHasher,
    fmt,
    hash::{Hash, Hasher},
};

const NONE: u32 = u32::MAX;

/// Returned when all preallocated slots in a [`WindowedTable`] hold active or non-expired entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolFull;

impl fmt::Display for PoolFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("windowed pool at capacity with no expired retired slots")
    }
}

impl std::error::Error for PoolFull {}

#[derive(Debug, Clone)]
enum Slot<K, V> {
    Free {
        next_free: u32,
    },
    Occupied {
        hash: u64,
        next_in_bucket: u32,
        key: K,
        value: V,
        retired_at_mono_ns: Option<u64>,
    },
}

/// Returns `true` iff an entry has been retired (`Some(t)`) and its retention window has elapsed.
///
/// Active entries (`retired_at_mono_ns == None`) and unbounded windows (`window_ns == None`)
/// never expire.
#[inline]
pub fn is_expired(
    retired_at_mono_ns: Option<u64>,
    now_mono_ns: u64,
    window_ns: Option<u64>,
) -> bool {
    match (retired_at_mono_ns, window_ns) {
        (Some(retired_at), Some(window)) => now_mono_ns >= retired_at.saturating_add(window),
        _ => false,
    }
}

fn hash_key<K: Hash>(key: &K) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Fixed-capacity, zero-reallocation arena hash table with active-protected time-window eviction.
#[derive(Debug, Clone)]
pub struct WindowedTable<K, V> {
    slots: Vec<Slot<K, V>>,
    buckets: Vec<u32>,
    free_head: u32,
    active_count: usize,
    retired_count: usize,
}

impl<K: Eq + Hash, V> WindowedTable<K, V> {
    /// Preallocates `capacity` arena slots and power-of-two hash bucket heads once.
    ///
    /// # Panics
    ///
    /// Panics if `capacity >= u32::MAX as usize` or bucket count overflows `usize`.
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(
            capacity < NONE as usize,
            "WindowedTable capacity must be < u32::MAX"
        );
        if capacity == 0 {
            return Self {
                slots: Vec::new(),
                buckets: Vec::new(),
                free_head: NONE,
                active_count: 0,
                retired_count: 0,
            };
        }
        let bucket_count = capacity
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .expect("WindowedTable bucket count overflow");
        let mut slots = Vec::with_capacity(capacity);
        for idx in 0..capacity {
            let next_free = if idx + 1 < capacity {
                (idx + 1) as u32
            } else {
                NONE
            };
            slots.push(Slot::Free { next_free });
        }
        let buckets = vec![NONE; bucket_count];
        Self {
            slots,
            buckets,
            free_head: 0,
            active_count: 0,
            retired_count: 0,
        }
    }

    /// Maximum number of slots preallocated in this table.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// Total occupied slots (active plus non-evicted retired).
    #[inline]
    pub fn len(&self) -> usize {
        self.active_count + self.retired_count
    }

    /// Returns `true` when no slots are occupied.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of occupied active (`retired_at_mono_ns == None`) slots.
    #[inline]
    pub fn active_len(&self) -> usize {
        self.active_count
    }

    /// Number of occupied retired (`retired_at_mono_ns == Some(_)`) slots awaiting eviction.
    #[inline]
    pub fn retired_len(&self) -> usize {
        self.retired_count
    }

    /// Base address of the preallocated slot buffer (for verifying zero reallocation in tests).
    #[inline]
    pub fn slots_addr(&self) -> usize {
        self.slots.as_ptr() as usize
    }

    /// Base address of the preallocated bucket buffer (for verifying zero reallocation in tests).
    #[inline]
    pub fn buckets_addr(&self) -> usize {
        self.buckets.as_ptr() as usize
    }

    #[inline]
    fn bucket_index(&self, hash: u64) -> usize {
        (hash as usize) & (self.buckets.len() - 1)
    }

    fn find_slot(&self, key: &K) -> Option<usize> {
        if self.buckets.is_empty() {
            return None;
        }
        let hash = hash_key(key);
        let mut cur = self.buckets[self.bucket_index(hash)];
        while cur != NONE {
            let idx = cur as usize;
            match &self.slots[idx] {
                Slot::Occupied {
                    hash: slot_hash,
                    next_in_bucket,
                    key: slot_key,
                    ..
                } => {
                    if *slot_hash == hash && slot_key == key {
                        return Some(idx);
                    }
                    cur = *next_in_bucket;
                }
                Slot::Free { .. } => unreachable!("corrupt bucket chain"),
            }
        }
        None
    }

    /// Raw lookup ignoring time window expiration.
    pub fn get(&self, key: &K) -> Option<&V> {
        let idx = self.find_slot(key)?;
        match &self.slots[idx] {
            Slot::Occupied { value, .. } => Some(value),
            Slot::Free { .. } => None,
        }
    }

    /// Raw mutable lookup ignoring time window expiration.
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let idx = self.find_slot(key)?;
        match &mut self.slots[idx] {
            Slot::Occupied { value, .. } => Some(value),
            Slot::Free { .. } => None,
        }
    }

    /// Raw membership check ignoring time window expiration.
    pub fn contains_key(&self, key: &K) -> bool {
        self.find_slot(key).is_some()
    }

    /// Returns the retirement timestamp (`None` if active, `Some(t)` if retired) for `key`.
    pub fn retired_at(&self, key: &K) -> Option<Option<u64>> {
        let idx = self.find_slot(key)?;
        match &self.slots[idx] {
            Slot::Occupied {
                retired_at_mono_ns, ..
            } => Some(*retired_at_mono_ns),
            Slot::Free { .. } => None,
        }
    }

    /// Window-aware lookup: treats expired retired entries as absent while never hiding active ones.
    pub fn get_live(&self, key: &K, now_mono_ns: u64, window_ns: Option<u64>) -> Option<&V> {
        let idx = self.find_slot(key)?;
        match &self.slots[idx] {
            Slot::Occupied {
                value,
                retired_at_mono_ns,
                ..
            } if !is_expired(*retired_at_mono_ns, now_mono_ns, window_ns) => Some(value),
            _ => None,
        }
    }

    /// Window-aware mutable lookup: treats expired retired entries as absent.
    pub fn get_live_mut(
        &mut self,
        key: &K,
        now_mono_ns: u64,
        window_ns: Option<u64>,
    ) -> Option<&mut V> {
        let idx = self.find_slot(key)?;
        match &mut self.slots[idx] {
            Slot::Occupied {
                value,
                retired_at_mono_ns,
                ..
            } if !is_expired(*retired_at_mono_ns, now_mono_ns, window_ns) => Some(value),
            _ => None,
        }
    }

    /// Window-aware membership check: treats expired retired entries as absent.
    pub fn contains_live_key(&self, key: &K, now_mono_ns: u64, window_ns: Option<u64>) -> bool {
        self.get_live(key, now_mono_ns, window_ns).is_some()
    }

    /// Marks an occupied entry as retired at `retired_at_mono_ns`.
    pub fn retire(&mut self, key: &K, retired_at_mono_ns: u64) -> bool {
        let Some(idx) = self.find_slot(key) else {
            return false;
        };
        if let Slot::Occupied {
            retired_at_mono_ns: slot_retired,
            ..
        } = &mut self.slots[idx]
        {
            if slot_retired.is_none() {
                self.active_count -= 1;
                self.retired_count += 1;
            }
            *slot_retired = Some(retired_at_mono_ns);
            return true;
        }
        false
    }

    /// Restores a retired entry back to the active (`None`) state so it cannot be evicted.
    pub fn reactivate(&mut self, key: &K) -> bool {
        let Some(idx) = self.find_slot(key) else {
            return false;
        };
        if let Slot::Occupied {
            retired_at_mono_ns: slot_retired,
            ..
        } = &mut self.slots[idx]
        {
            if slot_retired.is_some() {
                self.retired_count -= 1;
                self.active_count += 1;
            }
            *slot_retired = None;
            return true;
        }
        false
    }

    /// Removes `key` immediately and returns its slot to the intrusive free-list.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if self.buckets.is_empty() {
            return None;
        }
        let hash = hash_key(key);
        let bucket = self.bucket_index(hash);
        let mut prev = NONE;
        let mut cur = self.buckets[bucket];
        while cur != NONE {
            let idx = cur as usize;
            let next = match &self.slots[idx] {
                Slot::Occupied {
                    hash: slot_hash,
                    next_in_bucket,
                    key: slot_key,
                    ..
                } => {
                    if *slot_hash == hash && slot_key == key {
                        let next_chain = *next_in_bucket;
                        if prev == NONE {
                            self.buckets[bucket] = next_chain;
                        } else if let Slot::Occupied {
                            next_in_bucket: prev_next,
                            ..
                        } = &mut self.slots[prev as usize]
                        {
                            *prev_next = next_chain;
                        }
                        let old = std::mem::replace(
                            &mut self.slots[idx],
                            Slot::Free {
                                next_free: self.free_head,
                            },
                        );
                        self.free_head = cur;
                        if let Slot::Occupied {
                            value,
                            retired_at_mono_ns,
                            ..
                        } = old
                        {
                            if retired_at_mono_ns.is_some() {
                                self.retired_count -= 1;
                            } else {
                                self.active_count -= 1;
                            }
                            return Some(value);
                        }
                        unreachable!();
                    }
                    *next_in_bucket
                }
                Slot::Free { .. } => unreachable!("corrupt bucket chain"),
            };
            prev = cur;
            cur = next;
        }
        None
    }

    /// Reclaims all retired slots whose `retired_at_mono_ns + window_ns <= now_mono_ns` into the
    /// free-list without any heap allocation. Active entries (`None`) are never evicted.
    pub fn evict_expired(&mut self, now_mono_ns: u64, window_ns: Option<u64>) -> usize {
        self.evict_expired_with(now_mono_ns, window_ns, |_, _| {})
    }

    /// Like [`Self::evict_expired`], invoking `on_evict(key, value)` for each reclaimed entry.
    pub fn evict_expired_with<F: FnMut(K, V)>(
        &mut self,
        now_mono_ns: u64,
        window_ns: Option<u64>,
        mut on_evict: F,
    ) -> usize {
        if window_ns.is_none() || self.retired_count == 0 {
            return 0;
        }
        let mut evicted = 0usize;
        for bucket in 0..self.buckets.len() {
            let mut prev = NONE;
            let mut cur = self.buckets[bucket];
            while cur != NONE {
                let idx = cur as usize;
                let (expired, next) = match &self.slots[idx] {
                    Slot::Occupied {
                        next_in_bucket,
                        retired_at_mono_ns,
                        ..
                    } => (
                        is_expired(*retired_at_mono_ns, now_mono_ns, window_ns),
                        *next_in_bucket,
                    ),
                    Slot::Free { .. } => unreachable!("corrupt bucket chain"),
                };
                if expired {
                    if prev == NONE {
                        self.buckets[bucket] = next;
                    } else if let Slot::Occupied {
                        next_in_bucket: prev_next,
                        ..
                    } = &mut self.slots[prev as usize]
                    {
                        *prev_next = next;
                    }
                    let old = std::mem::replace(
                        &mut self.slots[idx],
                        Slot::Free {
                            next_free: self.free_head,
                        },
                    );
                    self.free_head = cur;
                    self.retired_count -= 1;
                    evicted += 1;
                    if let Slot::Occupied { key, value, .. } = old {
                        on_evict(key, value);
                    }
                } else {
                    prev = cur;
                }
                cur = next;
            }
        }
        evicted
    }

    /// Inserts or updates `(key, value, retired_at)` without heap reallocation.
    ///
    /// Reuses an existing slot for `key`, pops from the free-list, or lazily runs
    /// [`Self::evict_expired`] when full. Returns `Err(PoolFull)` only when all `capacity` slots
    /// hold active or non-expired retired entries.
    pub fn insert_or_evict(
        &mut self,
        key: K,
        value: V,
        retired_at: Option<u64>,
        now_mono_ns: u64,
        window_ns: Option<u64>,
    ) -> Result<Option<V>, PoolFull> {
        if self.slots.is_empty() {
            return Err(PoolFull);
        }
        if let Some(idx) = self.find_slot(&key)
            && let Slot::Occupied {
                value: slot_val,
                retired_at_mono_ns: slot_retired,
                ..
            } = &mut self.slots[idx]
        {
            let was_expired = is_expired(*slot_retired, now_mono_ns, window_ns);
            match (slot_retired.is_some(), retired_at.is_some()) {
                (false, true) => {
                    self.active_count -= 1;
                    self.retired_count += 1;
                }
                (true, false) => {
                    self.retired_count -= 1;
                    self.active_count += 1;
                }
                _ => {}
            }
            *slot_retired = retired_at;
            let previous = std::mem::replace(slot_val, value);
            return Ok(if was_expired { None } else { Some(previous) });
        }
        if self.free_head == NONE {
            self.evict_expired(now_mono_ns, window_ns);
        }
        if self.free_head == NONE {
            return Err(PoolFull);
        }
        let slot_idx = self.free_head;
        let next_free = match &self.slots[slot_idx as usize] {
            Slot::Free { next_free } => *next_free,
            Slot::Occupied { .. } => unreachable!("corrupt free list"),
        };
        self.free_head = next_free;
        let hash = hash_key(&key);
        let bucket = self.bucket_index(hash);
        let next_in_bucket = self.buckets[bucket];
        self.slots[slot_idx as usize] = Slot::Occupied {
            hash,
            next_in_bucket,
            key,
            value,
            retired_at_mono_ns: retired_at,
        };
        self.buckets[bucket] = slot_idx;
        if retired_at.is_some() {
            self.retired_count += 1;
        } else {
            self.active_count += 1;
        }
        Ok(None)
    }

    /// Iterates over all occupied `(key, value, retired_at_mono_ns)` entries (including non-evicted retired).
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V, Option<u64>)> {
        self.slots.iter().filter_map(|slot| match slot {
            Slot::Occupied {
                key,
                value,
                retired_at_mono_ns,
                ..
            } => Some((key, value, *retired_at_mono_ns)),
            Slot::Free { .. } => None,
        })
    }

    /// Iterates over all occupied keys.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.iter().map(|(k, _, _)| k)
    }

    /// Iterates over all occupied values.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, v, _)| v)
    }

    /// Iterates mutably over all occupied values.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.slots.iter_mut().filter_map(|slot| match slot {
            Slot::Occupied { value, .. } => Some(value),
            Slot::Free { .. } => None,
        })
    }
}

impl<K: Eq + Hash, V: PartialEq> PartialEq for WindowedTable<K, V> {
    fn eq(&self, other: &Self) -> bool {
        if self.active_count != other.active_count || self.retired_count != other.retired_count {
            return false;
        }
        self.iter().all(|(k, v, r)| {
            other
                .find_slot(k)
                .and_then(|idx| match &other.slots[idx] {
                    Slot::Occupied {
                        value,
                        retired_at_mono_ns,
                        ..
                    } => Some((value, *retired_at_mono_ns)),
                    Slot::Free { .. } => None,
                })
                .is_some_and(|(ov, or)| v == ov && r == or)
        })
    }
}

impl<K: Eq + Hash, V: Eq> Eq for WindowedTable<K, V> {}
