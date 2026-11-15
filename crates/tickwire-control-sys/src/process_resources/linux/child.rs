use super::*;
pub(super) use crate::affinity::Mask;
use std::{io, os::unix::process::CommandExt};

pub(super) struct Setup {
    procs: File,
    checks: Vec<(File, Vec<u8>)>,
    mask: Mask,
}
impl Setup {
    pub(super) fn new(procs: File, checks: Vec<(File, Vec<u8>)>, cpus: &[u32]) -> Self {
        Self {
            procs,
            checks,
            mask: Mask::new(cpus),
        }
    }
    pub(super) fn install(self, command: &mut Command) {
        // SAFETY: all heap data/FDs were prepared in the parent. This callback
        // performs only bounded raw reads/writes/affinity syscalls and comparisons;
        // it never allocates, acquires locks, formats errors or invokes user code.
        // std applies requested UID/GID before this callback. Linux checks cgroup
        // migration against the opener's file credentials; failure aborts exec.
        unsafe {
            command.pre_exec(move || self.run());
        }
    }
    fn run(&self) -> io::Result<()> {
        for (file, expected) in &self.checks {
            compare(file, expected)?;
        }
        // SAFETY: the live inherited cgroup.procs FD and constant bytes are valid.
        let written = unsafe { libc::write(self.procs.as_raw_fd(), b"0".as_ptr().cast(), 1) };
        if written < 0 {
            return Err(io::Error::last_os_error());
        }
        if written != 1 {
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
        self.mask.apply()?;
        // The final two checks are initial procs/threads emptiness; occupancy
        // necessarily changed. Settings and descendant count must remain identical.
        for (file, expected) in self.checks.iter().take(self.checks.len().saturating_sub(2)) {
            compare(file, expected)?;
        }
        Ok(())
    }
}
fn compare(file: &File, expected: &[u8]) -> io::Result<()> {
    let mut buffer = [0u8; MAX_FILE + 1];
    let mut used = 0;
    loop {
        // SAFETY: bounded writable slice is valid; pread leaves the parent's shared FD offset untouched.
        let read = unsafe {
            libc::pread(
                file.as_raw_fd(),
                buffer[used..].as_mut_ptr().cast(),
                buffer.len() - used,
                used as libc::off_t,
            )
        };
        if read < 0 {
            return Err(io::Error::last_os_error());
        }
        if read == 0 {
            break;
        }
        used += read as usize;
        if used > expected.len() {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
    }
    if &buffer[..used] != expected {
        return Err(io::Error::from_raw_os_error(libc::EINVAL));
    }
    Ok(())
}
