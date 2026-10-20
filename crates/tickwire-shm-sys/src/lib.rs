//! Linux shared mappings and the small unsafe boundary used by the ring SDK.
//!
//! Memory access is unsafe even after checking offsets: the caller must uphold
//! the cross-process publication/reclamation protocol and consistent atomic
//! access widths. No reference or pointer to an atomic is exported.
#![deny(unsafe_op_in_unsafe_fn)]

use std::fmt;
use std::io;
use std::os::fd::OwnedFd;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering, fence};

pub const PAGE_BYTES: usize = 4096;
pub const ISOLATION_BYTES: usize = 128;
pub const SINGLE_WRITER_SEALS: i32 = 0x17;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    UnsupportedPlatform,
    PlatformMismatch(&'static str),
    InvalidLength,
    InvalidName,
    OutOfBounds,
    Misaligned,
    WrongFileType,
    LengthMismatch {
        expected: usize,
        actual: u64,
    },
    WrongSeals {
        actual: i32,
    },
    NotSealed,
    System {
        operation: &'static str,
        source: io::Error,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::System { operation, source } => write!(f, "{operation}: {source}"),
            other => write!(f, "{other:?}"),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::System { source, .. } => Some(source),
            _ => None,
        }
    }
}
fn validate_length(len: usize) -> Result<()> {
    if len == 0 || !len.is_multiple_of(PAGE_BYTES) || len > isize::MAX as usize {
        return Err(Error::InvalidLength);
    }
    Ok(())
}

/// Owns one mapping and its descriptor; deliberately neither Clone nor Sync.
/// NonNull also prevents Send, keeping this low-level ownership on one thread.
struct Mapping {
    ptr: NonNull<u8>,
    len: usize,
    fd: OwnedFd,
}

impl Mapping {
    fn check_range(&self, offset: usize, bytes: usize) -> Result<()> {
        if offset.checked_add(bytes).is_none_or(|end| end > self.len) {
            return Err(Error::OutOfBounds);
        }
        Ok(())
    }

    fn check_atomic(&self, offset: usize) -> Result<()> {
        self.check_range(offset, size_of::<u64>())?;
        if !offset.is_multiple_of(align_of::<AtomicU64>()) {
            return Err(Error::Misaligned);
        }
        Ok(())
    }

    unsafe fn read_bytes(&self, offset: usize, len: usize) -> Result<&[u8]> {
        self.check_range(offset, len)?;
        // SAFETY: bounds were checked; caller guarantees that all referenced
        // bytes remain immutable for the returned reference's lifetime.
        Ok(unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().add(offset), len) })
    }

    unsafe fn load_u64_acquire(&self, offset: usize) -> Result<u64> {
        self.check_atomic(offset)?;
        // SAFETY: mmap base and offset are suitably aligned and in bounds.
        // Caller guarantees initialized, consistently sized atomic access.
        // Rust guarantees <=8-byte Relaxed loads on RO x86_64/aarch64 pages.
        let value = unsafe {
            (&*self.ptr.as_ptr().add(offset).cast::<AtomicU64>()).load(Ordering::Relaxed)
        };
        fence(Ordering::Acquire);
        Ok(value)
    }

    fn duplicate_fd(&self) -> Result<OwnedFd> {
        self.fd.try_clone().map_err(|source| Error::System {
            operation: "duplicate fd with CLOEXEC",
            source,
        })
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: this object owns exactly this successful mmap region and
        // never exposes a way to unmap it. Drop ends all borrowed references.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
        // OwnedFd closes after the mapping is gone.
    }
}

/// The single writable mapping created before F_SEAL_FUTURE_WRITE.
pub struct WriterMapping {
    inner: Mapping,
    sealed: bool,
}

impl WriterMapping {
    /// Creates a private zero-filled memfd and its sole writable mapping.
    /// No descriptor can be exported through this API before sealing.
    pub fn create(name: &str, len: usize) -> Result<Self> {
        platform::check()?;
        validate_length(len)?;
        let fd = platform::create(name, len)?;
        let inner = platform::map(fd, len, true)?;
        Ok(Self {
            inner,
            sealed: false,
        })
    }

    /// Locks the ABI single-writer seal profile, retaining this existing mapping.
    /// Repeated calls verify the same immutable seal set.
    pub fn seal_for_readers(&mut self) -> Result<()> {
        if !self.sealed {
            platform::seal(&self.inner.fd)?;
        }
        platform::check_seals(&self.inner.fd)?;
        self.sealed = true;
        Ok(())
    }

    /// Returns a CLOEXEC descriptor only after the locked seals are verified.
    pub fn duplicate_fd(&self) -> Result<OwnedFd> {
        if !self.sealed {
            return Err(Error::NotSealed);
        }
        platform::check_seals(&self.inner.fd)?;
        self.inner.duplicate_fd()
    }

    pub fn len(&self) -> usize {
        self.inner.len
    }
    pub fn is_empty(&self) -> bool {
        false
    }

    /// # Safety
    /// Every returned byte must be initialized and remain unmodified until the
    /// reference is no longer used, including through other process mappings.
    /// Publication must happen-before this access; no atomic field may be
    /// concurrently written while covered by this ordinary byte reference.
    pub unsafe fn read_bytes(&self, offset: usize, len: usize) -> Result<&[u8]> {
        // SAFETY: forwarded caller obligations; helper checks the range.
        unsafe { self.inner.read_bytes(offset, len) }
    }

    /// # Safety
    /// Caller owns exclusive write access to this range across all mappings.
    /// No reader may retain a reference into it. Atomic fields may only be
    /// initialized this way before publication and before any atomic access.
    /// Source and destination ranges must not overlap.
    pub unsafe fn write_bytes(&mut self, offset: usize, bytes: &[u8]) -> Result<()> {
        self.inner.check_range(offset, bytes.len())?;
        // SAFETY: range checked; caller establishes exclusive non-overlapping
        // initialized source and exclusive destination access.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.inner.ptr.as_ptr().add(offset),
                bytes.len(),
            )
        };
        Ok(())
    }

    /// # Safety
    /// This initialized location must be used only for aligned 64-bit atomic
    /// accesses for the lifetime of the shared resource. No overlapping byte
    /// references or non-atomic writes may coexist with a conflicting access.
    pub unsafe fn load_u64_acquire(&self, offset: usize) -> Result<u64> {
        // SAFETY: forwarded caller obligations; helper checks alignment/bounds.
        unsafe { self.inner.load_u64_acquire(offset) }
    }

    /// # Safety
    /// Caller is the resource's unique authorized writer for this atomic field.
    /// All concurrent accesses must use the same aligned 64-bit atomic width.
    /// Payload covered by a publication must already be complete, and any
    /// release cursor must exclude every byte reference still in use.
    pub unsafe fn store_u64_release(&mut self, offset: usize, value: u64) -> Result<()> {
        self.inner.check_atomic(offset)?;
        // SAFETY: range/alignment checked; caller upholds atomic/owner contract.
        unsafe {
            (&*self.inner.ptr.as_ptr().add(offset).cast::<AtomicU64>())
                .store(value, Ordering::Release)
        };
        Ok(())
    }
}

/// A PROT_READ mapping of an exactly sized, sealed single-writer resource.
pub struct ReaderMapping {
    inner: Mapping,
}

impl ReaderMapping {
    /// Consumes the received fd on both success and error. This validates the
    /// OS resource, not its Tickwire header, identity, hash or authorization.
    pub fn map(fd: OwnedFd, expected_len: usize) -> Result<Self> {
        platform::check()?;
        validate_length(expected_len)?;
        platform::check_file(&fd, expected_len)?;
        platform::check_seals(&fd)?;
        Ok(Self {
            inner: platform::map(fd, expected_len, false)?,
        })
    }

    pub fn len(&self) -> usize {
        self.inner.len
    }
    pub fn is_empty(&self) -> bool {
        false
    }

    /// # Safety
    /// Caller has observed the required release publication, and the writer
    /// cannot overwrite any returned byte until this borrow ends. The range
    /// must exclude atomic fields that can concurrently change.
    pub unsafe fn read_bytes(&self, offset: usize, len: usize) -> Result<&[u8]> {
        // SAFETY: forwarded caller obligations; helper checks the range.
        unsafe { self.inner.read_bytes(offset, len) }
    }

    /// # Safety
    /// The position is initialized and every process accesses it exclusively
    /// as an aligned 64-bit atomic; no conflicting non-atomic access is allowed.
    /// The acquired value must be checked by the caller before using payload.
    pub unsafe fn load_u64_acquire(&self, offset: usize) -> Result<u64> {
        // SAFETY: forwarded caller obligations; helper checks alignment/bounds.
        unsafe { self.inner.load_u64_acquire(offset) }
    }
}

#[cfg(all(
    target_os = "linux",
    target_endian = "little",
    target_has_atomic = "64",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
mod platform {
    use super::*;
    use std::ffi::CString;
    use std::os::fd::{AsRawFd, FromRawFd};

    fn system(operation: &'static str) -> Error {
        Error::System {
            operation,
            source: io::Error::last_os_error(),
        }
    }

    pub fn check() -> Result<()> {
        // SAFETY: sysconf takes a constant and does not access Rust memory.
        if unsafe { libc::sysconf(libc::_SC_PAGESIZE) } != PAGE_BYTES as libc::c_long {
            return Err(Error::PlatformMismatch("page size must be 4096"));
        }

        Ok(())
    }

    pub fn create(name: &str, len: usize) -> Result<OwnedFd> {
        let name = CString::new(name).map_err(|_| Error::InvalidName)?;
        if name.as_bytes().is_empty() || name.as_bytes().len() > 249 {
            return Err(Error::InvalidName);
        }
        // SAFETY: CString is NUL terminated; flags request sealing and CLOEXEC.
        let raw = unsafe {
            libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING)
        };
        if raw < 0 {
            return Err(system("memfd_create"));
        }
        // SAFETY: successful memfd_create returned a fresh owned descriptor.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        // SAFETY: validated length fits isize/off_t on supported 64-bit targets.
        if unsafe { libc::ftruncate(fd.as_raw_fd(), len as libc::off_t) } != 0 {
            return Err(system("ftruncate"));
        }
        Ok(fd)
    }

    pub fn seal(fd: &OwnedFd) -> Result<()> {
        // SAFETY: fd is live, flags are the fixed ABI single-writer profile.
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_ADD_SEALS, SINGLE_WRITER_SEALS) } != 0 {
            return Err(system("F_ADD_SEALS"));
        }
        Ok(())
    }

    pub fn check_seals(fd: &OwnedFd) -> Result<()> {
        // SAFETY: read-only query on a live descriptor.
        let actual = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GET_SEALS) };
        if actual < 0 {
            return Err(system("F_GET_SEALS"));
        }
        if actual != SINGLE_WRITER_SEALS {
            return Err(Error::WrongSeals { actual });
        }
        Ok(())
    }

    pub fn check_file(fd: &OwnedFd, expected: usize) -> Result<()> {
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: valid fd, writable correctly sized stat output.
        if unsafe { libc::fstat(fd.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return Err(system("fstat"));
        }
        // SAFETY: successful fstat initialized the whole result.
        let stat = unsafe { stat.assume_init() };
        if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Err(Error::WrongFileType);
        }
        if stat.st_size < 0 || stat.st_size as u64 != expected as u64 {
            return Err(Error::LengthMismatch {
                expected,
                actual: stat.st_size as u64,
            });
        }
        Ok(())
    }

    pub fn map(fd: OwnedFd, len: usize, writable: bool) -> Result<Mapping> {
        check_file(&fd, len)?;
        // SAFETY: fd is live; checked file size and zero mapping offset.
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
        if flags < 0 {
            return Err(system("F_GETFD"));
        }
        // SAFETY: only adds descriptor close-on-exec, not file status flags.
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, flags | libc::FD_CLOEXEC) } != 0 {
            return Err(system("F_SETFD"));
        }
        let protection = libc::PROT_READ | if writable { libc::PROT_WRITE } else { 0 };
        // SAFETY: entire validated file mapped MAP_SHARED; address chosen by OS.
        let address = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                protection,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if address == libc::MAP_FAILED {
            return Err(system("mmap"));
        }
        let Some(ptr) = NonNull::new(address.cast::<u8>()) else {
            // SAFETY: mmap succeeded at zero, which cannot back Rust references.
            unsafe { libc::munmap(address, len) };
            return Err(Error::PlatformMismatch("null mapping address"));
        };
        let mapping = Mapping { ptr, len, fd };
        // SAFETY: applies to exactly the owned mmap region. Error drops/unmaps.
        if unsafe { libc::madvise(address, len, libc::MADV_DONTFORK) } != 0 {
            return Err(system("MADV_DONTFORK"));
        }
        Ok(mapping)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod os_tests {
    use super::*;

    #[test]
    fn read_only_mapping_cannot_be_upgraded_to_write() {
        let mut writer = WriterMapping::create("read-only-upgrade", PAGE_BYTES).unwrap();
        writer.seal_for_readers().unwrap();
        let reader = ReaderMapping::map(writer.duplicate_fd().unwrap(), PAGE_BYTES).unwrap();
        // SAFETY: live mapped region. No memory access is attempted; sealing
        // removes the kernel's permission to upgrade this new mapping.
        let result = unsafe {
            libc::mprotect(
                reader.inner.ptr.as_ptr().cast(),
                reader.len(),
                libc::PROT_READ | libc::PROT_WRITE,
            )
        };
        assert_eq!(result, -1);
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::EACCES)
        );
    }

    #[test]
    fn kernel_maps_are_shared_and_excluded_from_fork() {
        let mut writer = WriterMapping::create("vm-flags", PAGE_BYTES).unwrap();
        writer.seal_for_readers().unwrap();
        let reader = ReaderMapping::map(writer.duplicate_fd().unwrap(), PAGE_BYTES).unwrap();
        let smaps = std::fs::read_to_string("/proc/self/smaps").unwrap();
        for (mapping, permissions) in [(&writer.inner, "rw-s"), (&reader.inner, "r--s")] {
            let start = mapping.ptr.as_ptr() as usize;
            let mut inside = false;
            let mut checked = false;
            for line in smaps.lines() {
                let mut fields = line.split_whitespace();
                let first = fields.next().unwrap_or("");
                if let Some((low, high)) = first.split_once('-')
                    && let (Ok(low), Ok(high)) = (
                        usize::from_str_radix(low, 16),
                        usize::from_str_radix(high, 16),
                    )
                {
                    inside = low <= start && start < high;
                    if inside {
                        assert_eq!(fields.next(), Some(permissions));
                    }
                }
                if inside && first == "VmFlags:" {
                    assert!(fields.any(|flag| flag == "dc"), "missing MADV_DONTFORK");
                    checked = true;
                    break;
                }
            }
            assert!(checked, "owned mapping must appear in smaps");
        }
    }
}

#[cfg(not(all(
    target_os = "linux",
    target_endian = "little",
    target_has_atomic = "64",
    any(target_arch = "x86_64", target_arch = "aarch64")
)))]
mod platform {
    use super::*;
    pub fn check() -> Result<()> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn create(_: &str, _: usize) -> Result<OwnedFd> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn seal(_: &OwnedFd) -> Result<()> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn check_seals(_: &OwnedFd) -> Result<()> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn check_file(_: &OwnedFd, _: usize) -> Result<()> {
        Err(Error::UnsupportedPlatform)
    }
    pub fn map(_: OwnedFd, _: usize, _: bool) -> Result<Mapping> {
        Err(Error::UnsupportedPlatform)
    }
}
