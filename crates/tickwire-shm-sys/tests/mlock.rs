#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use tickwire_shm_sys::{
    MlockOutcome, MlockPolicy, MlockedBox, MlockedSlice, PAGE_BYTES, mlock_slice, munlock_slice,
    try_mlock_slice,
};

#[repr(C, align(64))]
#[derive(Debug)]
struct PaddedAtomicSlot {
    identity: AtomicU64,
    tag: u8,
    value: i64,
}

#[test]
fn empty_slice_mlock_and_munlock_are_noops() {
    mlock_slice(&[]).unwrap();
    munlock_slice(&[]).unwrap();
    assert_eq!(
        try_mlock_slice(&[], MlockPolicy::Strict).unwrap(),
        MlockOutcome::NotRequested
    );
    assert_eq!(
        try_mlock_slice(&[1u8, 2, 3], MlockPolicy::Disabled).unwrap(),
        MlockOutcome::NotRequested
    );
}

#[test]
fn best_effort_slice_lock_handles_host_memlock_budget() {
    let buf = vec![0xA5_u8; 2 * PAGE_BYTES];
    let outcome = try_mlock_slice(&buf, MlockPolicy::BestEffort).unwrap();
    assert!(matches!(
        outcome,
        MlockOutcome::Locked | MlockOutcome::SkippedLimitOrPermission
    ));
    if outcome.is_locked() {
        munlock_slice(&buf).unwrap();
    }
}

#[test]
fn mlocked_slice_and_box_support_padded_atomic_structs_without_unsafe() {
    let raw: Vec<PaddedAtomicSlot> = (0..64)
        .map(|i| PaddedAtomicSlot {
            identity: AtomicU64::new(i),
            tag: (i & 0xff) as u8,
            value: -(i as i64),
        })
        .collect();

    let mut slice = MlockedSlice::from_vec(raw, MlockPolicy::BestEffort).unwrap();
    assert_eq!(slice.len(), 64);
    assert!(matches!(
        slice.outcome(),
        MlockOutcome::Locked | MlockOutcome::SkippedLimitOrPermission
    ));

    slice[7].identity.store(999, Ordering::Release);
    slice[7].value = 42;
    assert_eq!(slice[7].identity.load(Ordering::Acquire), 999);
    assert_eq!(slice[7].value, 42);
    assert_eq!(slice[7].tag, 7);

    let disabled = MlockedSlice::<u64>::from_vec(vec![1, 2, 3, 4], MlockPolicy::Disabled).unwrap();
    assert_eq!(disabled.outcome(), MlockOutcome::NotRequested);
    assert!(!disabled.is_locked());
    assert_eq!(disabled.as_slice(), &[1, 2, 3, 4]);

    let mut boxed = MlockedBox::new([0u64; 512], MlockPolicy::BestEffort).unwrap();
    boxed[0] = 0xDEAD_BEEF;
    boxed[511] = 0xCAFE_BABE;
    assert_eq!(boxed[0], 0xDEAD_BEEF);
    assert_eq!(boxed[511], 0xCAFE_BABE);
}

#[test]
fn oversized_best_effort_request_degrades_or_succeeds_cleanly() {
    // Even if a multi-megabyte allocation exceeds a small default 64 KiB RLIMIT_MEMLOCK in CI,
    // BestEffort must never fail startup.
    let large = vec![0u64; 512 * 1024]; // 4 MiB
    let locked = MlockedSlice::from_vec(large, MlockPolicy::BestEffort).unwrap();
    assert!(matches!(
        locked.outcome(),
        MlockOutcome::Locked | MlockOutcome::SkippedLimitOrPermission
    ));
}
