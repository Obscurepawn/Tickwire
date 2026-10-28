use super::*;

fn bit_by_bit(value: u64, mut mask: u64) -> u64 {
    let mut output = 0;
    let mut position = 0;
    while mask != 0 {
        let bit = mask.trailing_zeros();
        output |= ((value >> bit) & 1) << position;
        mask &= mask - 1;
        position += 1;
    }
    output
}

#[test]
fn precomputed_gather_matches_bit_extraction_for_exhaustive_small_and_full_width_masks() {
    for mask in 0..1024 {
        let gather = Gather::new(mask);
        for value in 0..1024 {
            assert_eq!(gather.apply(value), bit_by_bit(value, mask));
        }
    }
    for mask in [0, u64::MAX, 1 << 63, 0x5555555555555555, 0xaaaaaaaaaaaaaaaa] {
        for value in [0, 1, u64::MAX, 1 << 63, 0x123456789abcdef0] {
            assert_eq!(Gather::new(mask).apply(value), bit_by_bit(value, mask));
        }
    }
    let mut seed = 0xabcdef0123456789u64;
    for _ in 0..10_000 {
        seed = hash(seed.wrapping_add(1));
        let mask = hash(seed);
        assert_eq!(Gather::new(mask).apply(seed), bit_by_bit(seed, mask));
    }
}

#[test]
fn required_sparse_mask_uses_bounded_flat_fallback() {
    let keys: Vec<_> = std::iter::once(0).chain((0..64).map(|b| 1 << b)).collect();
    let fixed = FixedIndex::new(&keys).unwrap();
    let Layout::Flat(table) = fixed.layout else {
        panic!("64 independent distinguishing bits require fallback")
    };
    assert_eq!(table.slots.len(), 256);
    let compact = FixedIndex::new(&(0..256).map(|i| (i << 20) | 7).collect::<Vec<_>>()).unwrap();
    let Layout::Gather { slots, .. } = compact.layout else {
        panic!("compact mask should use direct slots")
    };
    assert_eq!(slots.len(), 256);
}

#[test]
fn colliding_hashes_probe_exact_keys_and_stop_at_empty_slot() {
    let keys: Vec<_> = (0..10_000)
        .filter(|&key| hash(key) & 15 == 0)
        .take(9)
        .collect();
    let mut index = AppendIndex::with_capacity(8).unwrap();
    for (i, &key) in keys[..8].iter().enumerate() {
        assert_eq!(index.insert(key).unwrap().index(), i);
    }
    assert_eq!(index.lookup(keys[8]), None);
    assert_eq!(index.insert(keys[8]), Err(Error::Capacity));
    for (i, &key) in keys[..8].iter().enumerate() {
        assert_eq!(index.lookup(key).unwrap().index(), i);
    }
}

#[test]
fn reserved_insert_and_lookup_keep_all_storage_addresses_and_capacities() {
    let mut index = AppendIndex::with_capacity(256).unwrap();
    let before = (
        index.keys.as_ptr(),
        index.keys.capacity(),
        index.table.slots.as_ptr(),
        index.table.slots.capacity(),
    );
    for i in 0..256 {
        let key = (i as u64) << 32;
        let id = index.insert(key).unwrap();
        assert_eq!(id.index(), i);
        assert_eq!(index.insert(key), Ok(id));
        assert_eq!(index.lookup(key), Some(id));
        assert_eq!(index.lookup(key + 1), None);
    }
    assert_eq!(index.insert(1), Err(Error::Capacity));
    let after = (
        index.keys.as_ptr(),
        index.keys.capacity(),
        index.table.slots.as_ptr(),
        index.table.slots.capacity(),
    );
    assert_eq!(before, after);
}
