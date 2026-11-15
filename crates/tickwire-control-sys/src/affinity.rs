//! Audited Linux thread-level CPU affinity (`CpuSet`), read-back verification, and core isolation.

#[cfg(target_os = "linux")]
use std::io;

use crate::{Error, Result};

/// Maximum number of Linux CPU IDs representable in a [`CpuSet`] (`0..4096`).
pub const MAX_CPUS: usize = 4096;

const MAX_CPU_ID: u32 = (MAX_CPUS - 1) as u32;
const WORDS: usize = MAX_CPUS / usize::BITS as usize;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct Mask([usize; WORDS]);

#[cfg(target_os = "linux")]
const _: () = {
    assert!(size_of::<Mask>() == 512);
    assert!(size_of::<usize>() == size_of::<libc::c_ulong>());
    assert!(align_of::<Mask>() >= align_of::<libc::cpu_set_t>());
};

impl Mask {
    pub(crate) const fn empty() -> Self {
        Self([0; WORDS])
    }

    pub(crate) fn new(cpus: &[u32]) -> Self {
        let mut mask = Self::empty();
        for &cpu in cpus {
            debug_assert!(cpu <= MAX_CPU_ID);
            mask.0[cpu as usize / usize::BITS as usize] |= 1usize << (cpu % usize::BITS);
        }
        mask
    }

    fn insert(&mut self, cpu: u32) -> bool {
        let word = &mut self.0[cpu as usize / usize::BITS as usize];
        let bit = 1usize << (cpu % usize::BITS);
        if *word & bit != 0 {
            return false;
        }
        *word |= bit;
        true
    }

    fn count_ones(&self) -> usize {
        self.0.iter().map(|word| word.count_ones() as usize).sum()
    }

    pub(crate) fn cpus(&self) -> Vec<u32> {
        let mut out = Vec::with_capacity(self.count_ones());
        for (word_idx, &(mut word)) in self.0.iter().enumerate() {
            let base = (word_idx as u32) * usize::BITS;
            while word != 0 {
                let offset = word.trailing_zeros();
                out.push(base + offset);
                word &= word - 1;
            }
        }
        out
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn read(pid: u32) -> io::Result<Self> {
        let pid: libc::pid_t = pid
            .try_into()
            .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
        let mut result = Self::empty();
        // SAFETY: `result` is zero-initialized, `Mask` satisfies `size_of::<Mask>() == 512` and
        // `align_of::<Mask>() >= align_of::<libc::cpu_set_t>()` (checked at compile time), and the
        // mutable pointer remains exclusively valid for the duration of the syscall.
        if unsafe {
            libc::sched_getaffinity(
                pid,
                size_of::<Self>(),
                result.0.as_mut_ptr().cast::<libc::cpu_set_t>(),
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(result)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn write_current(&self) -> io::Result<()> {
        // SAFETY: `self` is initialized, `Mask` satisfies `size_of::<Mask>() == 512` and
        // `align_of::<Mask>() >= align_of::<libc::cpu_set_t>()` (checked at compile time), the
        // shared pointer remains valid for the duration of the syscall, and PID `0` targets only
        // the calling kernel thread (TID).
        if unsafe {
            libc::sched_setaffinity(
                0,
                size_of::<Self>(),
                self.0.as_ptr().cast::<libc::cpu_set_t>(),
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Allocation-free, async-signal-safe set-and-verify helper for `pre_exec` child setup.
    #[cfg(target_os = "linux")]
    pub(crate) fn apply(&self) -> io::Result<()> {
        self.write_current()?;
        if Self::read(0)? != *self {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        Ok(())
    }
}

/// Validated non-empty, strictly ascending, duplicate-free CPU ID set in `0..4096`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CpuSet {
    cpus: Vec<u32>,
}

impl CpuSet {
    /// Constructs a [`CpuSet`] from an iterator of CPU IDs in `0..4096`.
    ///
    /// Valid inputs are canonicalized into strictly ascending order. Returns
    /// [`Error::InvalidResources`] if the iterator is empty, contains any CPU ID `>= 4096`, or
    /// contains duplicate CPU IDs.
    pub fn from_cpus(cpus: impl IntoIterator<Item = u32>) -> Result<Self> {
        let mut mask = Mask::empty();
        for cpu in cpus {
            if cpu > MAX_CPU_ID || !mask.insert(cpu) {
                return Err(Error::InvalidResources);
            }
        }
        let cpus = mask.cpus();
        if cpus.is_empty() {
            return Err(Error::InvalidResources);
        }
        Ok(Self { cpus })
    }

    /// Constructs a single-CPU [`CpuSet`] for `cpu` in `0..4096`.
    ///
    /// Returns [`Error::InvalidResources`] if `cpu >= 4096`.
    pub fn single(cpu: u32) -> Result<Self> {
        if cpu > MAX_CPU_ID {
            return Err(Error::InvalidResources);
        }
        Ok(Self { cpus: vec![cpu] })
    }

    /// Returns the strictly ascending, duplicate-free slice of CPU IDs in `0..4096`.
    pub fn as_slice(&self) -> &[u32] {
        &self.cpus
    }

    /// Returns the number of CPUs in this set (`1..=4096`).
    pub fn len(&self) -> usize {
        self.cpus.len()
    }

    /// Returns `false`; a validated [`CpuSet`] is guaranteed to be non-empty.
    pub fn is_empty(&self) -> bool {
        self.cpus.is_empty()
    }

    /// Returns `true` if `cpu` belongs to this set.
    pub fn contains(&self, cpu: u32) -> bool {
        self.cpus.binary_search(&cpu).is_ok()
    }

    /// Returns `true` if `self` and `other` share no CPU IDs.
    pub fn is_disjoint(&self, other: &CpuSet) -> bool {
        let (mut i, mut j) = (0, 0);
        let (left, right) = (&self.cpus, &other.cpus);
        while i < left.len() && j < right.len() {
            match left[i].cmp(&right[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => return false,
            }
        }
        true
    }
}

fn parse_cpu_id(token: &str) -> Result<u32> {
    if token.is_empty()
        || (token.len() > 1 && token.starts_with('0'))
        || !token.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(Error::InvalidResources);
    }
    let cpu: u32 = token.parse().map_err(|_| Error::InvalidResources)?;
    if cpu > MAX_CPU_ID {
        return Err(Error::InvalidResources);
    }
    Ok(cpu)
}

impl std::str::FromStr for CpuSet {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if s.is_empty() {
            return Err(Error::InvalidResources);
        }
        let mut mask = Mask::empty();
        for part in s.split(',') {
            let (start, end) = if let Some((a, b)) = part.split_once('-') {
                let start = parse_cpu_id(a)?;
                let end = parse_cpu_id(b)?;
                if start > end {
                    return Err(Error::InvalidResources);
                }
                (start, end)
            } else {
                let cpu = parse_cpu_id(part)?;
                (cpu, cpu)
            };
            for cpu in start..=end {
                if !mask.insert(cpu) {
                    return Err(Error::InvalidResources);
                }
            }
        }
        let cpus = mask.cpus();
        if cpus.is_empty() {
            return Err(Error::InvalidResources);
        }
        Ok(Self { cpus })
    }
}

impl std::fmt::Display for CpuSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cpus = &self.cpus;
        let mut index = 0;
        let mut first = true;
        while index < cpus.len() {
            let start = cpus[index];
            let mut end = start;
            while index + 1 < cpus.len() && cpus[index + 1] == end + 1 {
                index += 1;
                end = cpus[index];
            }
            if !first {
                f.write_str(",")?;
            }
            first = false;
            if start == end {
                write!(f, "{start}")?;
            } else {
                write!(f, "{start}-{end}")?;
            }
            index += 1;
        }
        Ok(())
    }
}

/// Validates cold-startup CPU isolation between named hot-thread slots and optional housekeeping
/// cores.
///
/// Verifies that:
/// 1. Every `Some(cpu)` in `hot_cpus` satisfies `cpu < 4096` ([`Error::InvalidResources`]).
/// 2. All `Some(cpu)` entries are pairwise distinct across hot roles ([`Error::ResourceConflict`]).
/// 3. `hot_cpus` and `housekeeping_cpus` are disjoint ([`Error::ResourceConflict`]).
pub fn validate_cpu_isolation(
    hot_cpus: &[(&'static str, Option<u32>)],
    housekeeping_cpus: Option<&CpuSet>,
) -> Result<()> {
    for &(_role, slot) in hot_cpus {
        if let Some(cpu) = slot
            && cpu > MAX_CPU_ID
        {
            return Err(Error::InvalidResources);
        }
    }
    let mut assigned = Mask::empty();
    for &(_role, slot) in hot_cpus {
        if let Some(cpu) = slot {
            if !assigned.insert(cpu) {
                return Err(Error::ResourceConflict);
            }
            if housekeeping_cpus.is_some_and(|hk| hk.contains(cpu)) {
                return Err(Error::ResourceConflict);
            }
        }
    }
    Ok(())
}

/// Pins the calling kernel thread (TID) to `cpus` via `sched_setaffinity(0, ...)` and immediately
/// verifies exact read-back equality via `sched_getaffinity(0, ...)`.
///
/// Fails immediately rather than silently degrading if the kernel masks off offline cores or
/// narrows the requested mask through cgroup `cpuset.cpus.effective`.
#[cfg(target_os = "linux")]
pub fn pin_current_thread(cpus: &CpuSet) -> Result<()> {
    let expected = Mask::new(cpus.as_slice());
    expected.write_current().map_err(|source| Error::System {
        operation: "set thread affinity",
        source,
    })?;
    let actual = Mask::read(0).map_err(|source| Error::System {
        operation: "read thread affinity",
        source,
    })?;
    if actual != expected {
        return Err(Error::InvalidResources);
    }
    Ok(())
}

/// Non-Linux stub returning [`Error::UnsupportedPlatform`].
#[cfg(not(target_os = "linux"))]
pub fn pin_current_thread(_cpus: &CpuSet) -> Result<()> {
    Err(Error::UnsupportedPlatform)
}

/// Reads the calling kernel thread's (TID) active CPU affinity mask via
/// `sched_getaffinity(0, ...)`.
#[cfg(target_os = "linux")]
pub fn current_thread_affinity() -> Result<CpuSet> {
    let mask = Mask::read(0).map_err(|source| Error::System {
        operation: "read thread affinity",
        source,
    })?;
    let cpus = mask.cpus();
    if cpus.is_empty() {
        return Err(Error::InvalidResources);
    }
    Ok(CpuSet { cpus })
}

/// Non-Linux stub returning [`Error::UnsupportedPlatform`].
#[cfg(not(target_os = "linux"))]
pub fn current_thread_affinity() -> Result<CpuSet> {
    Err(Error::UnsupportedPlatform)
}

/// Spawns a named OS thread that optionally pins itself to `affinity` before executing `f`.
///
/// When `affinity` is `Some(set)`, the child thread calls [`pin_current_thread`] before running
/// `f` and reports the result over a synchronous startup handshake channel. On failure, `f` is
/// dropped without executing, the child exits cleanly and is joined, and `Err(e)` is returned
/// synchronously to the caller. When `affinity` is `None`, the child inherits the parent thread's
/// CPU affinity mask unchanged.
#[cfg(target_os = "linux")]
pub fn spawn_pinned_thread<F, T>(
    name: impl Into<String>,
    affinity: Option<&CpuSet>,
    f: F,
) -> Result<std::thread::JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let name = name.into();
    if name.as_bytes().contains(&0) {
        return Err(Error::InvalidResources);
    }
    match affinity {
        None => std::thread::Builder::new()
            .name(name)
            .spawn(f)
            .map_err(|source| Error::System {
                operation: "spawn thread",
                source,
            }),
        Some(cpus) => {
            let target = cpus.clone();
            let (tx, rx) = std::sync::mpsc::sync_channel::<Result<()>>(1);
            let handle = std::thread::Builder::new()
                .name(name)
                .spawn(move || match pin_current_thread(&target) {
                    Ok(()) => {
                        if tx.send(Ok(())).is_err() {
                            drop(f);
                            std::panic::resume_unwind(Box::new(()));
                        }
                        f()
                    }
                    Err(error) => {
                        let _ = tx.send(Err(error));
                        drop(f);
                        std::panic::resume_unwind(Box::new(()));
                    }
                })
                .map_err(|source| Error::System {
                    operation: "spawn thread",
                    source,
                })?;
            match rx.recv() {
                Ok(Ok(())) => Ok(handle),
                Ok(Err(error)) => {
                    let _ = handle.join();
                    Err(error)
                }
                Err(_) => {
                    let _ = handle.join();
                    Err(Error::System {
                        operation: "handshake pinned thread",
                        source: io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "pinned thread exited before completing affinity handshake",
                        ),
                    })
                }
            }
        }
    }
}

/// Non-Linux stub returning [`Error::UnsupportedPlatform`].
#[cfg(not(target_os = "linux"))]
pub fn spawn_pinned_thread<F, T>(
    _name: impl Into<String>,
    _affinity: Option<&CpuSet>,
    _f: F,
) -> Result<std::thread::JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    Err(Error::UnsupportedPlatform)
}
