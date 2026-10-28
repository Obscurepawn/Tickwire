use super::*;
use std::{io, os::unix::process::CommandExt};
const WORDS: usize = 4096 / usize::BITS as usize;
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Mask([usize; WORDS]);
impl Mask {
    pub(super) fn new(cpus: &[u32]) -> Self {
        let mut mask = Self([0; WORDS]);
        for cpu in cpus {
            mask.0[*cpu as usize / usize::BITS as usize] |= 1usize << (*cpu % usize::BITS);
        }
        mask
    }
    pub(super) fn read(pid: u32) -> io::Result<Self> {
        let pid: libc::pid_t = pid
            .try_into()
            .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
        let mut result = Self([0; WORDS]);
        // SAFETY: initialized aligned storage holds exactly the mask byte size passed to libc.
        if unsafe { libc::sched_getaffinity(pid, size_of::<Self>(), result.0.as_mut_ptr().cast()) }
            < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(result)
    }
    pub(super) fn apply(&self) -> io::Result<()> {
        // SAFETY: mask is initialized, aligned, and lives through the syscall; PID 0 is this child.
        if unsafe { libc::sched_setaffinity(0, size_of::<Self>(), self.0.as_ptr().cast()) } < 0 {
            return Err(io::Error::last_os_error());
        }
        if Self::read(0)? != *self {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn cpus(&self) -> Vec<u32> {
        (0..4096)
            .filter(|cpu| {
                self.0[*cpu as usize / usize::BITS as usize] & (1usize << (*cpu % usize::BITS)) != 0
            })
            .collect()
    }
}

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
