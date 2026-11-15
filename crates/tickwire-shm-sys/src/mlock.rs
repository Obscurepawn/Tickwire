//! Audited Linux `mlock(2)` / `munlock(2)` boundary for startup-preallocated hot tables.
//!
//! Downstream crates (`tickwire-account`, `tickwire-risk`, ORS execution) default to
//! `#![forbid(unsafe_code)]`. This module encapsulates page locking behind safe byte-slice
//! functions and fixed-capacity heap containers (`MlockedSlice<T>`, `MlockedBox<T>`) that pass raw
//! allocation addresses and byte lengths directly to the kernel without forming `&[u8]` aliases
//! over struct padding or interior atomics.

use crate::{Error, Result};
use std::io;
use std::ops::{Deref, DerefMut, Index, IndexMut};

/// Policy governing startup physical-page locking via `mlock(2)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MlockPolicy {
    /// Do not call `mlock(2)`; pages remain ordinary swappable heap memory.
    Disabled,
    /// Attempt `mlock(2)` and degrade cleanly when `RLIMIT_MEMLOCK` or `CAP_IPC_LOCK` denies
    /// locking (`ENOMEM`, `EPERM`, or `EAGAIN`), suitable for unprivileged CI and dev hosts.
    #[default]
    BestEffort,
    /// Require `mlock(2)` to succeed on non-empty allocations; fail startup otherwise.
    Strict,
}

/// Outcome reported after applying an [`MlockPolicy`] to a preallocated memory region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MlockOutcome {
    /// Locking was disabled or the region had zero byte length.
    NotRequested,
    /// All virtual pages covering the region were locked into physical RAM via `mlock(2)`.
    Locked,
    /// `MlockPolicy::BestEffort` encountered `ENOMEM`, `EPERM`, or `EAGAIN` from `RLIMIT_MEMLOCK`.
    SkippedLimitOrPermission,
}

impl MlockOutcome {
    /// Returns `true` iff `mlock(2)` succeeded and locked the region's pages.
    #[inline]
    pub const fn is_locked(self) -> bool {
        matches!(self, Self::Locked)
    }
}

/// Locks all virtual pages intersecting `bytes` into physical RAM (`Strict` semantics).
///
/// Empty slices succeed as a no-op without invoking a syscall.
pub fn mlock_slice(bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    // SAFETY: `bytes` is a live initialized slice covering `[bytes.as_ptr(), bytes.as_ptr() + len)`
    // in this process's virtual address space for the duration of the syscall.
    unsafe { os::mlock_raw(bytes.as_ptr().cast(), bytes.len()) }
}

/// Unlocks virtual pages intersecting `bytes` previously locked with [`mlock_slice`].
///
/// Empty slices succeed as a no-op without invoking a syscall.
pub fn munlock_slice(bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    // SAFETY: `bytes` is a live initialized slice covering `[bytes.as_ptr(), bytes.as_ptr() + len)`
    // in this process's virtual address space for the duration of the syscall.
    unsafe { os::munlock_raw(bytes.as_ptr().cast(), bytes.len()) }
}

/// Locks `bytes` into physical RAM according to `policy`, tolerating `RLIMIT_MEMLOCK` / permission
/// limits when `policy == MlockPolicy::BestEffort`.
pub fn try_mlock_slice(bytes: &[u8], policy: MlockPolicy) -> Result<MlockOutcome> {
    // SAFETY: `bytes` is a live initialized slice for the duration of the syscall.
    unsafe { apply_policy_raw(bytes.as_ptr().cast(), bytes.len(), policy) }
}

/// Preallocated, fixed-length `Box<[T]>` whose backing pages are optionally `mlock`ed at creation.
///
/// Unlike `mlock_slice`, `MlockedSlice<T>` accepts arbitrary `T` (including types with padding
/// bytes or interior `Atomic*` / `Mutex` fields) without ever constructing a `&[u8]` alias.
#[derive(Debug)]
pub struct MlockedSlice<T> {
    inner: Box<[T]>,
    outcome: MlockOutcome,
}

impl<T> MlockedSlice<T> {
    /// Converts `items` into a fixed-address `Box<[T]>` and applies `policy` via `mlock(2)`.
    pub fn from_vec(items: Vec<T>, policy: MlockPolicy) -> Result<Self> {
        Self::from_boxed_slice(items.into_boxed_slice(), policy)
    }

    /// Wraps `inner` and applies `policy` via `mlock(2)`.
    pub fn from_boxed_slice(inner: Box<[T]>, policy: MlockPolicy) -> Result<Self> {
        let byte_len = std::mem::size_of_val::<[T]>(&inner);
        let ptr = inner.as_ptr().cast::<libc::c_void>();
        // SAFETY: `inner` is a heap-allocated `Box<[T]>` whose address `ptr` and byte span
        // `byte_len` remain valid and unmoved until `inner` is dropped. `mlock` inspects VMA page
        // tables without reading padding bytes through Rust references.
        let outcome = unsafe { apply_policy_raw(ptr, byte_len, policy)? };
        Ok(Self { inner, outcome })
    }

    /// Outcome of the startup `mlock` call for this allocation.
    #[inline]
    pub const fn outcome(&self) -> MlockOutcome {
        self.outcome
    }

    /// Returns `true` iff `mlock(2)` locked this slice's pages into RAM.
    #[inline]
    pub const fn is_locked(&self) -> bool {
        self.outcome.is_locked()
    }

    /// Shared slice view of all preallocated elements.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        &self.inner
    }

    /// Exclusive slice view of all preallocated elements.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.inner
    }
}

impl<T> Deref for MlockedSlice<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        &self.inner
    }
}

impl<T> DerefMut for MlockedSlice<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.inner
    }
}

impl<T> Index<usize> for MlockedSlice<T> {
    type Output = T;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.inner[index]
    }
}

impl<T> IndexMut<usize> for MlockedSlice<T> {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.inner[index]
    }
}

impl<T> Drop for MlockedSlice<T> {
    fn drop(&mut self) {
        if self.outcome == MlockOutcome::Locked {
            let byte_len = std::mem::size_of_val::<[T]>(&self.inner);
            if byte_len > 0 {
                // SAFETY: `self.inner` is still live until after `drop` returns, and this exact
                // `[ptr, ptr + byte_len)` range was locked during construction.
                let _ = unsafe { os::munlock_raw(self.inner.as_ptr().cast(), byte_len) };
            }
        }
    }
}

/// Preallocated single heap value `Box<T>` whose backing pages are optionally `mlock`ed at startup.
#[derive(Debug)]
pub struct MlockedBox<T> {
    inner: Box<T>,
    outcome: MlockOutcome,
}

impl<T> MlockedBox<T> {
    /// Allocates `value` on the heap and applies `policy` via `mlock(2)`.
    pub fn new(value: T, policy: MlockPolicy) -> Result<Self> {
        Self::from_box(Box::new(value), policy)
    }

    /// Wraps `inner` and applies `policy` via `mlock(2)`.
    pub fn from_box(inner: Box<T>, policy: MlockPolicy) -> Result<Self> {
        let byte_len = std::mem::size_of::<T>();
        let ptr = (&*inner as *const T).cast::<libc::c_void>();
        // SAFETY: `inner` owns a heap allocation of `byte_len` bytes at fixed address `ptr` until
        // `Drop`. `mlock` operates on virtual pages without forming Rust byte references.
        let outcome = unsafe { apply_policy_raw(ptr, byte_len, policy)? };
        Ok(Self { inner, outcome })
    }

    /// Outcome of the startup `mlock` call for this allocation.
    #[inline]
    pub const fn outcome(&self) -> MlockOutcome {
        self.outcome
    }

    /// Returns `true` iff `mlock(2)` locked this allocation's pages into RAM.
    #[inline]
    pub const fn is_locked(&self) -> bool {
        self.outcome.is_locked()
    }
}

impl<T> Deref for MlockedBox<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T> DerefMut for MlockedBox<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T> Drop for MlockedBox<T> {
    fn drop(&mut self) {
        if self.outcome == MlockOutcome::Locked {
            let byte_len = std::mem::size_of::<T>();
            if byte_len > 0 {
                let ptr = (&*self.inner as *const T).cast::<libc::c_void>();
                // SAFETY: `self.inner` remains valid until after `drop` completes.
                let _ = unsafe { os::munlock_raw(ptr, byte_len) };
            }
        }
    }
}

/// # Safety
/// If `byte_len > 0`, `[ptr, ptr + byte_len)` must lie within a live mapped object in this
/// process's virtual address space for the duration of the call.
unsafe fn apply_policy_raw(
    ptr: *const libc::c_void,
    byte_len: usize,
    policy: MlockPolicy,
) -> Result<MlockOutcome> {
    if policy == MlockPolicy::Disabled || byte_len == 0 {
        return Ok(MlockOutcome::NotRequested);
    }
    if byte_len > isize::MAX as usize {
        return Err(Error::InvalidLength);
    }
    // SAFETY: forwarded caller contract for `[ptr, ptr + byte_len)`.
    match unsafe { os::mlock_raw(ptr, byte_len) } {
        Ok(()) => Ok(MlockOutcome::Locked),
        Err(Error::System { source, .. })
            if policy == MlockPolicy::BestEffort && is_limit_or_permission_errno(&source) =>
        {
            Ok(MlockOutcome::SkippedLimitOrPermission)
        }
        Err(err) => Err(err),
    }
}

fn is_limit_or_permission_errno(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::ENOMEM | libc::EPERM | libc::EAGAIN)
    )
}

#[cfg(all(
    target_os = "linux",
    target_endian = "little",
    target_has_atomic = "64",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod os {
    use super::*;

    /// # Safety
    /// `[ptr, ptr + len)` must designate a valid mapped range in the current address space.
    pub unsafe fn mlock_raw(ptr: *const libc::c_void, len: usize) -> Result<()> {
        crate::platform::check()?;
        // SAFETY: caller guarantees `[ptr, ptr + len)` is a live mapped range.
        if unsafe { libc::mlock(ptr, len) } != 0 {
            return Err(Error::System {
                operation: "mlock",
                source: io::Error::last_os_error(),
            });
        }
        Ok(())
    }

    /// # Safety
    /// `[ptr, ptr + len)` must designate a valid mapped range in the current address space.
    pub unsafe fn munlock_raw(ptr: *const libc::c_void, len: usize) -> Result<()> {
        crate::platform::check()?;
        // SAFETY: caller guarantees `[ptr, ptr + len)` is a live mapped range.
        if unsafe { libc::munlock(ptr, len) } != 0 {
            return Err(Error::System {
                operation: "munlock",
                source: io::Error::last_os_error(),
            });
        }
        Ok(())
    }
}

#[cfg(not(all(
    target_os = "linux",
    target_endian = "little",
    target_has_atomic = "64",
    any(target_arch = "x86_64", target_arch = "aarch64")
)))]
mod os {
    use super::*;

    pub unsafe fn mlock_raw(_: *const libc::c_void, _: usize) -> Result<()> {
        Err(Error::UnsupportedPlatform)
    }

    pub unsafe fn munlock_raw(_: *const libc::c_void, _: usize) -> Result<()> {
        Err(Error::UnsupportedPlatform)
    }
}
