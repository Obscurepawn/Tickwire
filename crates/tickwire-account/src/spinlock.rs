//! Fine-grained TTAS spinlock and cache-line padding primitives (`#![forbid(unsafe_code)]`).
//!
//! Used by `OrderManager`, `OrsStpBook`, `FillRecordPool`, `DopState`, `OrsQueryDesk`, and
//! `UtaWalletLedger` to synchronize critical-path state transitions without coarse process-wide
//! mutexes, heap allocation, or `unsafe` blocks.

use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, TryLockError};

/// Aligns `T` to a 64-byte cache line boundary to prevent false sharing across `REQ` and `RESP`
/// cores.
#[repr(align(64))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CachePadded<T>(pub T);

impl<T> CachePadded<T> {
    #[inline]
    pub const fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for CachePadded<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Test-and-test-and-set (TTAS) spinlock implemented strictly within `#![forbid(unsafe_code)]`.
///
/// Lock acquisition spins on an `AtomicBool` in shared L1 cache (`Ordering::Relaxed`) before
/// attempting `Ordering::Acquire` ownership, then takes the inline `std::sync::Mutex<T>` via
/// `try_lock()`. Unlock drops the inner `MutexGuard` strictly *before* releasing `flag` with
/// `Ordering::Release`, guaranteeing the next acquirer observes an unlocked inner `Mutex` without
/// futex syscalls or heap allocation.
#[derive(Debug)]
pub struct TtasSpinLock<T> {
    flag: AtomicBool,
    inner: Mutex<T>,
}

impl<T: Default> Default for TtasSpinLock<T> {
    #[inline]
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> TtasSpinLock<T> {
    #[inline]
    pub const fn new(value: T) -> Self {
        Self {
            flag: AtomicBool::new(false),
            inner: Mutex::new(value),
        }
    }

    /// Acquires the spinlock, spinning in user space until ownership is obtained.
    #[inline]
    pub fn lock(&self) -> SpinLockGuard<'_, T> {
        while self
            .flag
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            while self.flag.load(Ordering::Relaxed) {
                core::hint::spin_loop();
            }
        }
        let guard = loop {
            match self.inner.try_lock() {
                Ok(g) => break g,
                Err(TryLockError::WouldBlock) => core::hint::spin_loop(),
                Err(TryLockError::Poisoned(p)) => break p.into_inner(),
            }
        };
        SpinLockGuard {
            guard: Some(guard),
            flag: &self.flag,
        }
    }

    /// Attempts to acquire the spinlock without spinning.
    #[inline]
    pub fn try_lock(&self) -> Option<SpinLockGuard<'_, T>> {
        if self
            .flag
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }
        match self.inner.try_lock() {
            Ok(guard) => Some(SpinLockGuard {
                guard: Some(guard),
                flag: &self.flag,
            }),
            Err(TryLockError::Poisoned(p)) => Some(SpinLockGuard {
                guard: Some(p.into_inner()),
                flag: &self.flag,
            }),
            Err(TryLockError::WouldBlock) => {
                self.flag.store(false, Ordering::Release);
                None
            }
        }
    }
}

/// RAII guard for [`TtasSpinLock`].
///
/// Callers must perform zero logging, zero heap allocation, and zero protocol parsing while
/// holding this guard.
#[derive(Debug)]
pub struct SpinLockGuard<'a, T> {
    guard: Option<MutexGuard<'a, T>>,
    flag: &'a AtomicBool,
}

impl<T> Deref for SpinLockGuard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.guard
            .as_ref()
            .expect("SpinLockGuard inner guard present until Drop")
    }
}

impl<T> DerefMut for SpinLockGuard<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard
            .as_mut()
            .expect("SpinLockGuard inner guard present until Drop")
    }
}

impl<T> Drop for SpinLockGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        drop(self.guard.take());
        self.flag.store(false, Ordering::Release);
    }
}
