//! The only unsafe boundary: owned descriptors and checked libc return values.
use std::{
    ffi::CStr,
    fs::File,
    io,
    os::fd::{AsRawFd, FromRawFd},
};

pub fn open(directory: &File, name: &CStr, flags: i32, mode: u32) -> io::Result<File> {
    // SAFETY: directory remains owned for the call; CStr is terminated. A
    // successful fresh fd is transferred once into File and never aliased.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            mode,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}
pub fn mkdir(directory: &File, name: &CStr) -> io::Result<()> {
    // SAFETY: both borrowed inputs remain alive and the pathname is terminated.
    let result = unsafe { libc::mkdirat(directory.as_raw_fd(), name.as_ptr(), 0o700) };
    outcome(result)
}
pub fn rename(directory: &File, old: &CStr, new: &CStr) -> io::Result<()> {
    // SAFETY: same pinned directory and two terminated component names.
    let result = unsafe {
        libc::renameat(
            directory.as_raw_fd(),
            old.as_ptr(),
            directory.as_raw_fd(),
            new.as_ptr(),
        )
    };
    outcome(result)
}
pub fn unlink(directory: &File, name: &CStr) -> io::Result<()> {
    // SAFETY: a terminated component in the live pinned directory; no recursive
    // traversal and no directory removal are exposed.
    let result = unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) };
    outcome(result)
}
/// Read a fresh directory description without path lookup through /proc.
pub fn entries(directory: &File, maximum: usize) -> io::Result<Vec<Vec<u8>>> {
    let reader = open(directory, c".", libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
    let mut names = Vec::new();
    let mut buffer = [0u8; 8192];
    loop {
        // SAFETY: reader is a live directory FD; the mutable output buffer is
        // valid for the exact supplied length. The returned bytes are parsed
        // with bounds checks, without casting to potentially unaligned structs.
        let result = unsafe {
            libc::syscall(
                libc::SYS_getdents64,
                reader.as_raw_fd(),
                buffer.as_mut_ptr(),
                buffer.len(),
            )
        };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if result == 0 {
            return Ok(names);
        }
        let count = usize::try_from(result).map_err(|_| io::Error::other("directory size"))?;
        if count > buffer.len() {
            return Err(io::Error::other("directory buffer size"));
        }
        let mut offset = 0usize;
        while offset < count {
            let remaining = &buffer[offset..count];
            if remaining.len() < 20 {
                return Err(io::Error::other("short directory entry"));
            }
            let size = usize::from(u16::from_ne_bytes([remaining[16], remaining[17]]));
            if size < 20 || size > remaining.len() || !size.is_multiple_of(8) {
                return Err(io::Error::other("directory entry length"));
            }
            let end = remaining[19..size]
                .iter()
                .position(|b| *b == 0)
                .ok_or_else(|| io::Error::other("directory name terminator"))?;
            let name = &remaining[19..19 + end];
            if name.is_empty() || name.contains(&b'/') {
                return Err(io::Error::other("directory name"));
            }
            if name != b"." && name != b".." {
                if names.len() >= maximum {
                    return Err(io::Error::other("directory entry budget"));
                }
                names.push(name.to_vec());
            }
            offset += size;
        }
    }
}
fn outcome(result: i32) -> io::Result<()> {
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
