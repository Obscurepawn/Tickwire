//! Bounded instrument lookup. Dense IDs belong to the index that produced them.

use core::fmt;

pub const MAX_KEYS: usize = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    DuplicateKey,
    Capacity,
    Allocation,
}
pub type Result<T> = core::result::Result<T, Error>;
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::DuplicateKey => "duplicate instrument ID",
            Self::Capacity => "instrument index capacity exceeded",
            Self::Allocation => "instrument index allocation failed",
        })
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DenseId(usize);
impl DenseId {
    pub fn index(self) -> usize {
        self.0
    }
}

const EMPTY: u32 = u32::MAX;
const MAX_BIT_SLOTS: usize = 1 << 22;

/// Reserved heap storage, excluding the index value and allocator bookkeeping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryStats {
    pub key_bytes: usize,
    pub slot_bytes: usize,
}
impl MemoryStats {
    pub fn total_bytes(self) -> usize {
        self.key_bytes + self.slot_bytes
    }
}
fn memory_stats(keys: &[u64], key_capacity: usize, slots: &Vec<u32>) -> MemoryStats {
    debug_assert!(keys.len() <= key_capacity);
    MemoryStats {
        key_bytes: key_capacity * size_of::<u64>(),
        slot_bytes: slots.capacity() * size_of::<u32>(),
    }
}

fn reserved<T>(capacity: usize) -> Result<Vec<T>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| Error::Allocation)?;
    Ok(values)
}
fn slots(count: usize) -> Result<Vec<u32>> {
    let mut values = reserved(count)?;
    values.resize(count, EMPTY);
    Ok(values)
}
fn copy_keys(keys: &[u64], capacity: usize) -> Result<Vec<u64>> {
    let mut values = reserved(capacity)?;
    values.extend_from_slice(keys);
    Ok(values)
}

/// An immutable directory. Dense order is exactly the supplied key order.
#[derive(Debug)]
pub struct FixedIndex {
    keys: Vec<u64>,
    layout: Layout,
}
#[derive(Debug)]
enum Layout {
    Gather { gather: Gather, slots: Vec<u32> },
    Flat(Flat),
}
impl FixedIndex {
    pub fn new(keys: &[u64]) -> Result<Self> {
        if keys.len() > MAX_KEYS {
            return Err(Error::Capacity);
        }
        let mut scratch = copy_keys(keys, keys.len())?;
        scratch.sort_unstable();
        if scratch.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(Error::DuplicateKey);
        }
        let mask = greedy_mask(keys, &mut scratch);
        let bit_slots = 1usize
            .checked_shl(mask.count_ones())
            .filter(|&count| count <= MAX_BIT_SLOTS && count <= keys.len().saturating_mul(4));
        let layout = if let Some(count) = bit_slots {
            let gather = Gather::new(mask);
            let mut slots = slots(count)?;
            for (dense, &key) in keys.iter().enumerate() {
                slots[gather.apply(key) as usize] = dense as u32;
            }
            Layout::Gather { gather, slots }
        } else {
            Layout::Flat(Flat::from_keys(keys, keys.len())?)
        };
        Ok(Self {
            keys: copy_keys(keys, keys.len())?,
            layout,
        })
    }
    #[inline]
    pub fn lookup(&self, key: u64) -> Option<DenseId> {
        match &self.layout {
            Layout::Gather { gather, slots } => {
                let dense = slots[gather.apply(key) as usize] as usize;
                (self.keys.get(dense) == Some(&key)).then_some(DenseId(dense))
            }
            Layout::Flat(table) => table.lookup(&self.keys, key),
        }
    }
    #[inline]
    pub fn key(&self, id: DenseId) -> Option<u64> {
        self.keys.get(id.0).copied()
    }
    pub fn len(&self) -> usize {
        self.keys.len()
    }
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
    pub fn memory_stats(&self) -> MemoryStats {
        let slots = match &self.layout {
            Layout::Gather { slots, .. } => slots,
            Layout::Flat(table) => &table.slots,
        };
        memory_stats(&self.keys, self.keys.capacity(), slots)
    }
}

fn greedy_mask(keys: &[u64], scratch: &mut [u64]) -> u64 {
    let Some(&first) = keys.first() else { return 0 };
    let mut mask = keys.iter().fold(0, |mask, key| mask | (key ^ first));
    for bit in (0..64).rev() {
        let trial = mask & !(1u64 << bit);
        if trial == mask {
            continue;
        }
        for (value, key) in scratch.iter_mut().zip(keys) {
            *value = key & trial;
        }
        scratch.sort_unstable();
        if scratch.windows(2).all(|pair| pair[0] != pair[1]) {
            mask = trial;
        }
    }
    mask
}

/// Parallel right compression, with all move masks computed off the lookup path.
#[derive(Debug)]
struct Gather {
    mask: u64,
    moves: [(u64, u32); 6],
    count: usize,
}
impl Gather {
    fn new(mask: u64) -> Self {
        let mut result = Self {
            mask,
            moves: [(0, 0); 6],
            count: 0,
        };
        let mut selected = mask;
        let mut gaps = !mask << 1;
        for stage in 0..6 {
            let mut parity = gaps ^ (gaps << 1);
            for shift in [2, 4, 8, 16, 32] {
                parity ^= parity << shift;
            }
            let moving = parity & selected;
            let shift = 1 << stage;
            selected = (selected ^ moving) | (moving >> shift);
            gaps &= !parity;
            if moving != 0 {
                result.moves[result.count] = (moving, shift);
                result.count += 1;
            }
        }
        result
    }
    #[inline]
    fn apply(&self, key: u64) -> u64 {
        let mut value = key & self.mask;
        for &(mask, shift) in &self.moves[..self.count] {
            let moving = value & mask;
            value = (value ^ moving) | (moving >> shift);
        }
        value
    }
}

/// Stable, append-only dense IDs. Reserve additional total capacity off the hot path.
#[derive(Debug)]
pub struct AppendIndex {
    keys: Vec<u64>,
    table: Flat,
    capacity: usize,
}
impl AppendIndex {
    pub fn with_capacity(max: usize) -> Result<Self> {
        if max > MAX_KEYS {
            return Err(Error::Capacity);
        }
        Ok(Self {
            keys: reserved(max)?,
            table: Flat::from_keys(&[], max)?,
            capacity: max,
        })
    }
    #[inline]
    pub fn lookup(&self, key: u64) -> Option<DenseId> {
        self.table.lookup(&self.keys, key)
    }
    #[inline]
    pub fn key(&self, id: DenseId) -> Option<u64> {
        self.keys.get(id.0).copied()
    }
    pub fn len(&self) -> usize {
        self.keys.len()
    }
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
    pub fn memory_stats(&self) -> MemoryStats {
        memory_stats(&self.keys, self.keys.capacity(), &self.table.slots)
    }
    #[inline]
    pub fn insert(&mut self, key: u64) -> Result<DenseId> {
        let slot = match self.table.probe(&self.keys, key) {
            Probe::Found(dense) => return Ok(DenseId(dense)),
            Probe::Vacant(slot) => slot,
            Probe::Full => return Err(Error::Capacity),
        };
        if self.keys.len() == self.capacity {
            return Err(Error::Capacity);
        }
        let id = DenseId(self.keys.len());
        self.keys.push(key);
        self.table.slots[slot] = id.0 as u32;
        Ok(id)
    }
    /// Grow to at least this total key capacity. Smaller requests are no-ops.
    pub fn reserve(&mut self, new_capacity: usize) -> Result<()> {
        if new_capacity > MAX_KEYS {
            return Err(Error::Capacity);
        }
        if new_capacity <= self.capacity {
            return Ok(());
        }
        let table = Flat::from_keys(&self.keys, new_capacity)?;
        let keys = copy_keys(&self.keys, new_capacity)?;
        self.table = table;
        self.keys = keys;
        self.capacity = new_capacity;
        Ok(())
    }
}

#[derive(Debug)]
struct Flat {
    slots: Vec<u32>,
}
enum Probe {
    Found(usize),
    Vacant(usize),
    Full,
}
impl Flat {
    fn from_keys(keys: &[u64], capacity: usize) -> Result<Self> {
        let count = if capacity == 0 {
            0
        } else {
            capacity
                .checked_mul(2)
                .and_then(usize::checked_next_power_of_two)
                .ok_or(Error::Capacity)?
        };
        let mut table = Self {
            slots: slots(count)?,
        };
        for (dense, &key) in keys.iter().enumerate() {
            let Probe::Vacant(slot) = table.probe(keys, key) else {
                return Err(Error::DuplicateKey);
            };
            table.slots[slot] = dense as u32;
        }
        Ok(table)
    }
    #[inline]
    fn lookup(&self, keys: &[u64], key: u64) -> Option<DenseId> {
        match self.probe(keys, key) {
            Probe::Found(dense) => Some(DenseId(dense)),
            _ => None,
        }
    }
    #[inline]
    fn probe(&self, keys: &[u64], key: u64) -> Probe {
        if self.slots.is_empty() {
            return Probe::Full;
        }
        let mask = self.slots.len() - 1;
        let mut slot = hash(key) as usize & mask;
        for _ in 0..self.slots.len() {
            let dense = self.slots[slot];
            if dense == EMPTY {
                return Probe::Vacant(slot);
            }
            if keys[dense as usize] == key {
                return Probe::Found(dense as usize);
            }
            slot = (slot + 1) & mask;
        }
        Probe::Full
    }
}

#[inline]
fn hash(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests;
