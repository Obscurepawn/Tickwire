//! Safe single-writer `MAP_SHARED` regular-file segments and double-buffered cursor pages.

use crate::{Error, ISOLATION_BYTES, PAGE_BYTES, Result};
use sha2::{Digest, Sha256};
use std::io;
use std::os::fd::OwnedFd;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering, fence};

const CURSOR_MAGIC: u64 = u64::from_le_bytes(*b"TWMC0001");
const CURSOR_SLOT_BYTES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorState {
    pub generation: u64,
    pub segment_id: u64,
    pub logical_len: u64,
}

/// Exclusive single-writer `MAP_SHARED` file segment pinned to one thread (`!Send`, `!Sync`).
pub struct MappedSegment {
    ptr: NonNull<u8>,
    len: usize,
    fd: Option<OwnedFd>,
}

impl MappedSegment {
    pub fn open_or_create(path: &Path, min_capacity: usize) -> Result<Self> {
        os::open_or_create(path, min_capacity)
    }

    pub fn capacity(&self) -> usize {
        self.len
    }

    pub fn write_at(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        self.check_range(offset, data.len())?;
        if !data.is_empty() {
            // SAFETY: `self` holds an exclusive `&mut` borrow, exclusive advisory `flock`,
            // `MADV_DONTFORK`, and `offset + data.len() <= self.len` was verified above.
            // `data` is a stack/heap slice disjoint from the file mapping.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    self.ptr.as_ptr().add(offset),
                    data.len(),
                );
            }
            fence(Ordering::Release);
        }
        Ok(())
    }

    pub fn read_to_vec(&self, len: usize) -> Result<Vec<u8>> {
        self.check_range(0, len)?;
        let mut out = vec![0u8; len];
        if len > 0 {
            // SAFETY: `0..len` is within `self.len`, `self` holds exclusive file lock,
            // and `out` is a freshly allocated disjoint heap buffer.
            unsafe {
                std::ptr::copy_nonoverlapping(self.ptr.as_ptr(), out.as_mut_ptr(), len);
            }
        }
        Ok(out)
    }

    pub fn duplicate_sync_fd(&self) -> Result<OwnedFd> {
        let fd = self.fd.as_ref().ok_or(Error::InvalidLength)?;
        fd.try_clone().map_err(|source| Error::System {
            operation: "duplicate mapped segment fd",
            source,
        })
    }

    pub fn grow(&mut self, min_capacity: usize) -> Result<()> {
        os::grow(self, min_capacity)
    }

    pub fn truncate_unmap(mut self, logical_len: u64) -> Result<()> {
        os::truncate_unmap(&mut self, logical_len)
    }

    fn check_range(&self, offset: usize, bytes: usize) -> Result<()> {
        if offset.checked_add(bytes).is_none_or(|end| end > self.len) {
            return Err(Error::OutOfBounds);
        }
        Ok(())
    }

    fn store_u64_release(&mut self, offset: usize, value: u64) -> Result<()> {
        self.check_range(offset, size_of::<u64>())?;
        if !offset.is_multiple_of(align_of::<AtomicU64>()) {
            return Err(Error::Misaligned);
        }
        // SAFETY: `ptr` is page-aligned (`4096`), `offset` is 8-byte aligned and in-bounds,
        // and `self` holds exclusive `&mut` access to this single-writer mapping.
        unsafe {
            (&*self.ptr.as_ptr().add(offset).cast::<AtomicU64>()).store(value, Ordering::Release);
        }
        Ok(())
    }

    fn read_slot(&self, offset: usize) -> Result<[u8; CURSOR_SLOT_BYTES]> {
        self.check_range(offset, CURSOR_SLOT_BYTES)?;
        let mut out = [0u8; CURSOR_SLOT_BYTES];
        // SAFETY: `offset..offset + 64` is in-bounds and disjoint from stack array `out`.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.ptr.as_ptr().add(offset),
                out.as_mut_ptr(),
                CURSOR_SLOT_BYTES,
            );
        }
        Ok(out)
    }
}

impl Drop for MappedSegment {
    fn drop(&mut self) {
        os::drop_segment(self);
    }
}

/// Double-buffered 4-KiB `MMAP-CURSOR.bin` page tracking logical tail length across `SIGKILL`.
pub struct MappedCursorPage {
    segment: MappedSegment,
    current: Option<CursorState>,
}

impl MappedCursorPage {
    pub fn open_or_create(path: &Path) -> Result<Self> {
        let segment = MappedSegment::open_or_create(path, PAGE_BYTES)?;
        let slot0 = parse_slot(&segment.read_slot(0)?);
        let slot1 = parse_slot(&segment.read_slot(ISOLATION_BYTES)?);
        let current = match (slot0, slot1) {
            (SlotStatus::Empty, SlotStatus::Empty) => None,
            (SlotStatus::Valid(a), SlotStatus::Valid(b)) => {
                if a.generation >= b.generation {
                    Some(a)
                } else {
                    Some(b)
                }
            }
            (SlotStatus::Valid(a), _) => Some(a),
            (_, SlotStatus::Valid(b)) => Some(b),
            (SlotStatus::Corrupt, SlotStatus::Empty)
            | (SlotStatus::Empty, SlotStatus::Corrupt)
            | (SlotStatus::Corrupt, SlotStatus::Corrupt) => return Err(Error::Checksum),
        };
        Ok(Self { segment, current })
    }

    pub fn latest(&self) -> Result<Option<CursorState>> {
        Ok(self.current)
    }

    pub fn duplicate_sync_fd(&self) -> Result<OwnedFd> {
        self.segment.duplicate_sync_fd()
    }

    pub fn record(&mut self, segment_id: u64, logical_len: u64) -> Result<CursorState> {
        if segment_id == 0 {
            return Err(Error::Identity);
        }
        let generation = self
            .current
            .map_or(Some(1), |c| c.generation.checked_add(1))
            .ok_or(Error::InvalidLength)?;
        let state = CursorState {
            generation,
            segment_id,
            logical_len,
        };
        let slot_offset = ((generation as usize) & 1) * ISOLATION_BYTES;
        // Invalidate old magic in the target slot before writing body fields.
        self.segment.store_u64_release(slot_offset, 0)?;

        let digest = slot_digest(state);
        let mut body = [0u8; 56];
        body[0..8].copy_from_slice(&state.generation.to_le_bytes());
        body[8..16].copy_from_slice(&state.segment_id.to_le_bytes());
        body[16..24].copy_from_slice(&state.logical_len.to_le_bytes());
        body[24..56].copy_from_slice(&digest);
        self.segment.write_at(slot_offset + 8, &body)?;

        // Atomically publish magic after body bytes are visible in page cache.
        self.segment.store_u64_release(slot_offset, CURSOR_MAGIC)?;
        self.current = Some(state);
        Ok(state)
    }
}

enum SlotStatus {
    Empty,
    Valid(CursorState),
    Corrupt,
}

fn parse_slot(bytes: &[u8; CURSOR_SLOT_BYTES]) -> SlotStatus {
    if bytes.iter().all(|&b| b == 0) {
        return SlotStatus::Empty;
    }
    let magic = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    if magic != CURSOR_MAGIC {
        return SlotStatus::Corrupt;
    }
    let generation = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let segment_id = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let logical_len = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
    if generation == 0 || segment_id == 0 {
        return SlotStatus::Corrupt;
    }
    let state = CursorState {
        generation,
        segment_id,
        logical_len,
    };
    if bytes[32..64] != slot_digest(state) {
        return SlotStatus::Corrupt;
    }
    SlotStatus::Valid(state)
}

fn slot_digest(state: CursorState) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"tickwire.mmap-cursor.v1");
    hasher.update(state.generation.to_le_bytes());
    hasher.update(state.segment_id.to_le_bytes());
    hasher.update(state.logical_len.to_le_bytes());
    hasher.finalize().into()
}

pub fn align_page_capacity(min_capacity: usize) -> Result<usize> {
    if min_capacity == 0 || min_capacity > isize::MAX as usize {
        return Err(Error::InvalidLength);
    }
    let rem = min_capacity % PAGE_BYTES;
    if rem == 0 {
        return Ok(min_capacity);
    }
    let aligned = min_capacity
        .checked_add(PAGE_BYTES - rem)
        .ok_or(Error::InvalidLength)?;
    if aligned > isize::MAX as usize {
        return Err(Error::InvalidLength);
    }
    Ok(aligned)
}

#[cfg(all(
    target_os = "linux",
    target_endian = "little",
    target_has_atomic = "64",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod os {
    use super::*;
    use std::fs::OpenOptions;
    use std::mem::MaybeUninit;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;

    fn sys(operation: &'static str) -> Error {
        Error::System {
            operation,
            source: io::Error::last_os_error(),
        }
    }

    pub fn open_or_create(path: &Path, min_capacity: usize) -> Result<MappedSegment> {
        crate::platform::check()?;
        let aligned_min = align_page_capacity(min_capacity)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(path)
            .map_err(|source| Error::System {
                operation: "open mapped segment",
                source,
            })?;
        let fd = OwnedFd::from(file);

        // SAFETY: `fd` is a valid open descriptor; `flock` acquires non-blocking exclusive lock.
        if unsafe { libc::flock(fd.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(sys("flock LOCK_EX"));
        }

        let current_size = stat_regular_size(&fd)?;
        let target_len = align_page_capacity((current_size as usize).max(aligned_min))?;
        if (current_size as usize) < target_len {
            let off: libc::off_t = target_len.try_into().map_err(|_| Error::InvalidLength)?;
            // SAFETY: `fd` is exclusively locked regular file; extends capacity to `target_len`.
            if unsafe { libc::ftruncate(fd.as_raw_fd(), off) } != 0 {
                let err = sys("ftruncate expand");
                // SAFETY: release advisory lock before closing `fd`.
                let _ = unsafe { libc::flock(fd.as_raw_fd(), libc::LOCK_UN) };
                return Err(err);
            }
        }

        match map_and_prefault(&fd, target_len, 0) {
            Ok(ptr) => Ok(MappedSegment {
                ptr,
                len: target_len,
                fd: Some(fd),
            }),
            Err(err) => {
                // SAFETY: release advisory lock before closing `fd`.
                let _ = unsafe { libc::flock(fd.as_raw_fd(), libc::LOCK_UN) };
                Err(err)
            }
        }
    }

    pub fn grow(seg: &mut MappedSegment, min_capacity: usize) -> Result<()> {
        let target_len = align_page_capacity(min_capacity)?;
        if target_len <= seg.len {
            return Ok(());
        }
        let fd = seg.fd.as_ref().ok_or(Error::InvalidLength)?;
        let old_len = seg.len;
        let off: libc::off_t = target_len.try_into().map_err(|_| Error::InvalidLength)?;
        // SAFETY: `fd` is exclusively locked regular file; extends capacity to `target_len`.
        if unsafe { libc::ftruncate(fd.as_raw_fd(), off) } != 0 {
            return Err(sys("ftruncate grow"));
        }
        let new_ptr = map_and_prefault(fd, target_len, old_len)?;
        // SAFETY: `seg.ptr..seg.ptr + old_len` is the previously owned mapping, replaced atomically in `seg`.
        unsafe {
            libc::munmap(seg.ptr.as_ptr().cast(), old_len);
        }
        seg.ptr = new_ptr;
        seg.len = target_len;
        Ok(())
    }

    pub fn truncate_unmap(seg: &mut MappedSegment, logical_len: u64) -> Result<()> {
        let Some(fd) = seg.fd.take() else {
            return Ok(());
        };
        if logical_len > seg.len as u64 {
            seg.fd = Some(fd);
            return Err(Error::OutOfBounds);
        }
        // SAFETY: `seg.ptr..seg.ptr + seg.len` is owned by `seg`; `seg.fd` was taken so `Drop` will not unmap twice.
        if unsafe { libc::munmap(seg.ptr.as_ptr().cast(), seg.len) } != 0 {
            let err = sys("munmap");
            let _ = unsafe { libc::flock(fd.as_raw_fd(), libc::LOCK_UN) };
            return Err(err);
        }
        seg.len = 0;
        let off: libc::off_t = logical_len.try_into().map_err(|_| Error::InvalidLength)?;
        // SAFETY: unmapped regular file under exclusive `flock`; trims unused preallocated tail.
        let trunc_res = unsafe { libc::ftruncate(fd.as_raw_fd(), off) };
        // SAFETY: explicit unlock before closing `OwnedFd`.
        let _ = unsafe { libc::flock(fd.as_raw_fd(), libc::LOCK_UN) };
        if trunc_res != 0 {
            return Err(sys("ftruncate trim"));
        }
        Ok(())
    }

    pub fn drop_segment(seg: &mut MappedSegment) {
        if let Some(fd) = seg.fd.take() {
            if seg.len > 0 {
                // SAFETY: `seg` owns `seg.ptr..seg.ptr + seg.len` until `Drop`.
                let _ = unsafe { libc::munmap(seg.ptr.as_ptr().cast(), seg.len) };
                seg.len = 0;
            }
            // SAFETY: explicit unlock before closing `OwnedFd`.
            let _ = unsafe { libc::flock(fd.as_raw_fd(), libc::LOCK_UN) };
        }
    }

    fn stat_regular_size(fd: &OwnedFd) -> Result<u64> {
        let mut stat = MaybeUninit::<libc::stat>::uninit();
        // SAFETY: `fd` is open and `stat` points to valid `libc::stat` storage.
        if unsafe { libc::fstat(fd.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return Err(sys("fstat"));
        }
        // SAFETY: `fstat` returned 0, initializing `stat`.
        let stat = unsafe { stat.assume_init() };
        if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Err(Error::WrongFileType);
        }
        if stat.st_size < 0 {
            return Err(Error::InvalidLength);
        }
        Ok(stat.st_size as u64)
    }

    fn map_and_prefault(fd: &OwnedFd, len: usize, prefault_from: usize) -> Result<NonNull<u8>> {
        // SAFETY: `fd` is validated regular file of at least `len` bytes mapped `MAP_SHARED`.
        let addr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if addr == libc::MAP_FAILED {
            return Err(sys("mmap segment"));
        }
        let Some(ptr) = NonNull::new(addr.cast::<u8>()) else {
            // SAFETY: unmap unexpected zero address.
            let _ = unsafe { libc::munmap(addr, len) };
            return Err(Error::PlatformMismatch("null mapping address"));
        };
        // SAFETY: `addr..addr + len` is our live mapping; `MADV_DONTFORK` isolates child address spaces.
        if unsafe { libc::madvise(addr, len, libc::MADV_DONTFORK) } != 0 {
            let err = sys("MADV_DONTFORK");
            // SAFETY: unmap on setup error.
            let _ = unsafe { libc::munmap(addr, len) };
            return Err(err);
        }
        // Pre-fault writable PTEs (`page_mkwrite`) without altering any existing file byte.
        let mut page_offset = prefault_from - (prefault_from % PAGE_BYTES);
        while page_offset < len {
            // SAFETY: `ptr` is 4096-byte aligned and `page_offset + 8 <= len`; `fetch_or(0)` faults
            // writable page-table entries cleanly while preserving every bit of existing content.
            unsafe {
                (&*ptr.as_ptr().add(page_offset).cast::<AtomicU64>())
                    .fetch_or(0, Ordering::Relaxed);
            }
            page_offset += PAGE_BYTES;
        }
        Ok(ptr)
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
    pub fn open_or_create(_: &Path, _: usize) -> Result<MappedSegment> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn grow(_: &mut MappedSegment, _: usize) -> Result<()> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn truncate_unmap(_: &mut MappedSegment, _: u64) -> Result<()> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn drop_segment(_: &mut MappedSegment) {}
}
