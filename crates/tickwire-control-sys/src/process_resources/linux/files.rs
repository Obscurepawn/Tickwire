use super::*;
use std::{
    ffi::{CStr, CString},
    io::Read,
    mem::MaybeUninit,
    os::{
        fd::{FromRawFd, OwnedFd},
        unix::{ffi::OsStrExt, fs::OpenOptionsExt},
    },
};

pub(super) fn directory(path: &Path) -> Result<File> {
    let bytes = path.as_os_str().as_bytes();
    if !path.is_absolute() || bytes.len() > 4096 || bytes.contains(&0) {
        return Err(Error::InvalidPath);
    }
    let mut dir = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open("/")
        .map_err(|e| system("open resource path root", e))?;
    for component in bytes[1..].split(|b| *b == b'/') {
        if component.is_empty() || component == b"." || component == b".." {
            return Err(Error::InvalidPath);
        }
        let name = CString::new(component).map_err(|_| Error::InvalidPath)?;
        dir = open_raw(&dir, &name, libc::O_RDONLY | libc::O_DIRECTORY)?;
    }
    Ok(dir)
}
pub(super) fn open(dir: &File, name: &CStr, flags: i32) -> Result<File> {
    let file = open_raw(dir, name, flags)?;
    cgroupfs(&file)?;
    let metadata = file
        .metadata()
        .map_err(|e| system("stat resource file", e))?;
    if !metadata.is_file()
        || metadata.dev()
            != dir
                .metadata()
                .map_err(|e| system("stat resource directory", e))?
                .dev()
    {
        return Err(Error::InvalidResources);
    }
    Ok(file)
}
fn open_raw(dir: &File, name: &CStr, flags: i32) -> Result<File> {
    // SAFETY: both the borrowed directory FD and NUL-terminated single name live through openat.
    let raw = unsafe {
        libc::openat(
            dir.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
        )
    };
    if raw < 0 {
        return Err(system(
            "open resource file",
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: openat returned a fresh exclusively owned descriptor.
    Ok(File::from(unsafe { OwnedFd::from_raw_fd(raw) }))
}
pub(super) fn cgroupfs(file: &File) -> Result<()> {
    let mut stat = MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: stat points to suitably sized writable storage and file is a live FD.
    if unsafe { libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) } < 0 {
        return Err(system(
            "stat resource filesystem",
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: fstatfs succeeded and initialized the entire result.
    if unsafe { stat.assume_init() }.f_type != 0x6367_7270 {
        return Err(Error::InvalidResources);
    }
    Ok(())
}
pub(super) fn lease(directory: &File) -> Result<()> {
    // SAFETY: a live directory FD, documented nonblocking advisory flock flags.
    if unsafe { libc::flock(directory.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } < 0 {
        let error = std::io::Error::last_os_error();
        return Err(if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
            Error::ResourceConflict
        } else {
            system("lease resource domain", error)
        });
    }
    Ok(())
}
pub(super) fn read(dir: &File, name: &CStr) -> Result<Vec<u8>> {
    bounded(open(dir, name, libc::O_RDONLY)?)
}
pub(super) fn bounded(file: File) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    file.take((MAX_FILE + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|e| system("read resource file", e))?;
    if bytes.len() > MAX_FILE {
        return Err(Error::InvalidResources);
    }
    Ok(bytes)
}
