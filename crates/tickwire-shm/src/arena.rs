//! Private allocation state; blocks are retired by the same gated prefix as slots.
use crate::{Error, MappedError};
use std::collections::VecDeque;
use tickwire_ipc::{ArenaClass, ResourceHeader};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Allocation {
    pub class: usize,
    pub block: u32,
    pub sequence: u64,
}
pub(crate) struct Arena {
    free: [Vec<u32>; 4],
    retired: VecDeque<Allocation>,
}
pub(crate) fn validate(h: &ResourceHeader, slots_end: u64) -> Result<(), MappedError> {
    let count = h.arena_class_count as usize;
    if count == 0 {
        return if h.arena_offset == 0
            && h.arena_bytes == 0
            && h.class_descriptors
                .iter()
                .all(|c| *c == ArenaClass::default())
            && h.resource_bytes == slots_end
            && h.required_features & 2 == 0
        {
            Ok(())
        } else {
            Err(MappedError::Layout)
        };
    }
    if count > 4
        || h.resource_kind != 1
        || h.required_features & 2 == 0
        || h.arena_offset != slots_end
    {
        return Err(MappedError::Layout);
    }
    let mut cursor = slots_end;
    let mut prior = 192;
    let mut blocks = 0u64;
    for (i, c) in h.class_descriptors.iter().enumerate() {
        if i >= count {
            if *c != ArenaClass::default() {
                return Err(MappedError::Layout);
            }
            continue;
        }
        if c.class_id as usize != i + 1
            || c.payload_limit <= prior
            || c.payload_limit > 65536
            || c.block_count == 0
            || c.block_bytes < 128 + c.payload_limit
            || c.block_bytes > 65664
            || c.block_bytes % 128 != 0
            || c.offset != cursor
            || c.length != u64::from(c.block_bytes) * u64::from(c.block_count)
        {
            return Err(MappedError::Layout);
        }
        cursor = cursor.checked_add(c.length).ok_or(MappedError::Layout)?;
        blocks += u64::from(c.block_count);
        prior = c.payload_limit;
    }
    if cursor != h.resource_bytes
        || cursor - slots_end != h.arena_bytes
        || blocks > u64::from(h.capacity)
    {
        return Err(MappedError::Layout);
    }
    Ok(())
}
impl Arena {
    pub fn new(h: &ResourceHeader) -> Self {
        let free = std::array::from_fn(|i| {
            (0..h.class_descriptors[i].block_count)
                .rev()
                .collect::<Vec<_>>()
        });
        let count = free.iter().map(Vec::len).sum();
        Self {
            free,
            retired: VecDeque::with_capacity(count),
        }
    }
    pub fn reclaim(&mut self, released: u64) {
        while self.retired.front().is_some_and(|a| a.sequence <= released) {
            let a = self.retired.pop_front().unwrap();
            self.rollback(a);
        }
    }
    pub fn reserve(
        &mut self,
        h: &ResourceHeader,
        bytes: u32,
        sequence: u64,
    ) -> Result<Allocation, MappedError> {
        let class = h.class_descriptors[..h.arena_class_count as usize]
            .iter()
            .position(|c| bytes <= c.payload_limit)
            .ok_or(MappedError::Layout)?;
        let block = self.free[class].pop().ok_or(Error::ArenaExhausted)?;
        Ok(Allocation {
            class,
            block,
            sequence,
        })
    }
    pub fn rollback(&mut self, a: Allocation) {
        self.free[a.class].push(a.block);
    }
    pub fn commit(&mut self, a: Allocation) {
        self.retired.push_back(a);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn header() -> ResourceHeader {
        let mut h = ResourceHeader {
            resource_kind: 1,
            capacity: 64,
            required_features: 2,
            arena_class_count: 2,
            arena_offset: 8192,
            ..Default::default()
        };
        h.class_descriptors[0] = ArenaClass {
            class_id: 1,
            block_bytes: 512,
            block_count: 1,
            payload_limit: 384,
            offset: 8192,
            length: 512,
        };
        h.class_descriptors[1] = ArenaClass {
            class_id: 2,
            block_bytes: 1152,
            block_count: 1,
            payload_limit: 1024,
            offset: 8704,
            length: 1152,
        };
        h.arena_bytes = 1664;
        h.resource_bytes = 9856;
        h
    }
    #[test]
    fn smallest_class_never_borrows_a_larger_free_block() {
        let h = header();
        validate(&h, 8192).unwrap();
        let mut a = Arena::new(&h);
        let first = a.reserve(&h, 208, 4).unwrap();
        a.commit(first);
        assert!(matches!(
            a.reserve(&h, 208, 5),
            Err(MappedError::Protocol(Error::ArenaExhausted))
        ));
        a.reclaim(3);
        assert!(a.reserve(&h, 208, 5).is_err());
        a.reclaim(4);
        assert_eq!(a.reserve(&h, 208, 5).unwrap().block, first.block);
    }
    #[test]
    fn failed_batch_reservations_can_be_returned_without_publication() {
        let h = header();
        let mut a = Arena::new(&h);
        let first = a.reserve(&h, 208, 4).unwrap();
        assert!(a.reserve(&h, 208, 5).is_err());
        a.rollback(first);
        assert_eq!(a.reserve(&h, 208, 4).unwrap().block, first.block);
    }
}
