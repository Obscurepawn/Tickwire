use std::collections::BTreeMap;
use tickwire_instrument_index::{AppendIndex, Error, FixedIndex, MAX_KEYS};

#[test]
fn fixed_indices_preserve_input_order_and_reject_unknown_aliases() {
    let keys = [u64::MAX, 0, 1 << 63, 17, 32];
    let index = FixedIndex::new(&keys).unwrap();
    assert_eq!(index.len(), keys.len());
    assert!(!index.is_empty());
    for (dense, key) in keys.into_iter().enumerate() {
        let id = index.lookup(key).unwrap();
        assert_eq!(id.index(), dense);
        assert_eq!(index.key(id), Some(key));
    }
    for unknown in [1, 16, 18, u64::MAX - 1] {
        assert_eq!(index.lookup(unknown), None);
    }
    let packed = FixedIndex::new(&[0x100, 0x101]).unwrap();
    assert_eq!(packed.lookup(0x200), None);
    assert_eq!(packed.lookup(0x201), None);
    assert_eq!(packed.key(index.lookup(32).unwrap()), None);
}

#[test]
fn fixed_empty_singleton_duplicates_and_all_sixty_four_required_bits() {
    let empty = FixedIndex::new(&[]).unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.lookup(0), None);
    let singleton = FixedIndex::new(&[u64::MAX]).unwrap();
    assert_eq!(singleton.lookup(u64::MAX).unwrap().index(), 0);
    assert_eq!(singleton.lookup(0), None);
    assert!(matches!(
        FixedIndex::new(&[3, 7, 3]),
        Err(Error::DuplicateKey)
    ));
    let keys: Vec<_> = std::iter::once(0)
        .chain((0..64).map(|b| 1u64 << b))
        .collect();
    let index = FixedIndex::new(&keys).unwrap();
    for (i, key) in keys.into_iter().enumerate() {
        assert_eq!(index.lookup(key).unwrap().index(), i);
    }
    assert_eq!(index.lookup(3), None);
}

#[test]
fn append_is_stable_idempotent_and_capacity_failure_is_atomic() {
    let mut index = AppendIndex::with_capacity(2).unwrap();
    assert!(index.is_empty());
    let a = index.insert(u64::MAX).unwrap();
    let b = index.insert(0).unwrap();
    assert_eq!((a.index(), b.index()), (0, 1));
    assert_eq!(index.insert(u64::MAX), Ok(a));
    assert_eq!(index.insert(9), Err(Error::Capacity));
    assert_eq!(index.len(), 2);
    assert_eq!(index.lookup(9), None);
    assert_eq!(index.key(a), Some(u64::MAX));
    assert_eq!(index.key(b), Some(0));
    assert_eq!(index.reserve(MAX_KEYS + 1), Err(Error::Capacity));
    assert_eq!(index.insert(9), Err(Error::Capacity));
    index.reserve(0).unwrap();
    index.reserve(4).unwrap();
    let c = index.insert(9).unwrap();
    assert_eq!(c.index(), 2);
    assert_eq!(index.lookup(u64::MAX), Some(a));
    assert_eq!(index.lookup(0), Some(b));
    assert_eq!(index.key(c), Some(9));
}

#[test]
fn empty_append_can_grow_and_limits_are_explicit() {
    let mut index = AppendIndex::with_capacity(0).unwrap();
    assert_eq!(index.insert(0), Err(Error::Capacity));
    index.reserve(1).unwrap();
    assert_eq!(index.insert(0).unwrap().index(), 0);
    assert!(matches!(
        AppendIndex::with_capacity(MAX_KEYS + 1),
        Err(Error::Capacity)
    ));
    assert!(matches!(
        FixedIndex::new(&vec![0; MAX_KEYS + 1]),
        Err(Error::Capacity)
    ));
}

#[test]
fn fixed_and_append_match_ordered_reference_for_discrete_keys_and_misses() {
    for count in [32, 256, 4096] {
        let keys: Vec<_> = (0..count as u64)
            .map(|i| i.wrapping_mul(0x9e3779b97f4a7c15).rotate_left(19))
            .collect();
        let expected: BTreeMap<_, _> = keys.iter().enumerate().map(|(i, &k)| (k, i)).collect();
        let fixed = FixedIndex::new(&keys).unwrap();
        let mut append = AppendIndex::with_capacity(count).unwrap();
        for &key in &keys {
            assert_eq!(append.insert(key).unwrap().index(), expected[&key]);
        }
        for &key in &keys {
            for query in [key, key ^ 0x5555555555555555] {
                let want = expected.get(&query).copied();
                assert_eq!(fixed.lookup(query).map(|id| id.index()), want);
                assert_eq!(append.lookup(query).map(|id| id.index()), want);
            }
        }
    }
}
