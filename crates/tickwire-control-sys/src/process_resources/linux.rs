use super::{parse, *};
use crate::ExecutableImage;
use std::{
    fs::File,
    os::fd::AsRawFd,
    os::unix::fs::MetadataExt,
    path::Path,
    process::{Child, Command},
};
mod child;
mod files;
mod observe;

const MAX_FILE: usize = 32_768;
const MAX_TASKS: usize = 4096;
const SETTINGS: [&std::ffi::CStr; 6] = [
    c"cgroup.type",
    c"cpuset.cpus.effective",
    c"cpuset.mems.effective",
    c"memory.max",
    c"memory.swap.max",
    c"cgroup.stat",
];

/// Exclusive cooperative lease on one preconfigured cgroup v2 leaf.
/// This owns no processes and never modifies controller settings or removes groups.
/// Keep it alive until the separately owned Child has exited.
pub struct ResourceDomain {
    directory: files::DirectoryLease,
    procs: File,
    limits: ResourceLimits,
    attempted: bool,
    pid: Option<u32>,
    device: u64,
    inode: u64,
}
impl ResourceDomain {
    pub fn acquire(path: &Path, limits: ResourceLimits) -> Result<Self> {
        limits.validate()?;
        let directory = files::directory(path)?;
        files::cgroupfs(&directory)?;
        let directory = files::DirectoryLease::acquire(directory)?;
        let metadata = directory
            .metadata()
            .map_err(|e| system("stat resource domain", e))?;
        // Validate all settings and occupancy before even opening the migration endpoint.
        settings(&directory, &limits)?;
        if !parse::ids(&files::read(&directory, c"cgroup.procs")?)?.is_empty()
            || !parse::ids(&files::read(&directory, c"cgroup.threads")?)?.is_empty()
        {
            return Err(Error::ResourceConflict);
        }
        let procs = files::open(&directory, c"cgroup.procs", libc::O_WRONLY)?;
        Ok(Self {
            directory,
            procs,
            limits,
            attempted: false,
            pid: None,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    pub fn limits(&self) -> &ResourceLimits {
        &self.limits
    }
    /// Exactly one attempt per lease. A failure never falls back to unconstrained exec.
    pub fn spawn(
        &mut self,
        image: &ExecutableImage,
        configure: impl FnOnce(&mut Command),
    ) -> Result<Child> {
        if self.attempted {
            return Err(Error::ResourceConflict);
        }
        self.attempted = true;
        let mut command = image.configured_command(configure)?;
        let snapshot = settings(&self.directory, &self.limits)?;
        let mut checks = Vec::new();
        for (name, bytes) in SETTINGS.into_iter().zip(snapshot) {
            checks.push((files::open(&self.directory, name, libc::O_RDONLY)?, bytes));
        }
        for name in [c"cgroup.procs", c"cgroup.threads"] {
            if !files::read(&self.directory, name)?.is_empty() {
                return Err(Error::ResourceConflict);
            }
            checks.push((
                files::open(&self.directory, name, libc::O_RDONLY)?,
                Vec::new(),
            ));
        }
        let setup = child::Setup::new(
            self.procs
                .try_clone()
                .map_err(|e| system("clone migration fd", e))?,
            checks,
            &self.limits.cpus,
        );
        setup.install(&mut command);
        let mut child = command
            .spawn()
            .map_err(|e| system("spawn resource domain", e))?;
        self.pid = Some(child.id());
        if let Err(error) = self.observe(child.id()) {
            // Only this directly created child is ours to terminate; never cgroup.kill.
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        Ok(child)
    }
    /// Cold sample. The caller must independently hold and check the live Child
    /// before/after sampling to exclude PID reuse and bind executable identity.
    pub fn observe(&self, pid: u32) -> Result<ResourceObservation> {
        if self.pid != Some(pid) || pid == 0 {
            return Err(Error::InvalidResources);
        }
        observe::sample(self, pid)
    }
}
fn settings(directory: &File, limits: &ResourceLimits) -> Result<Vec<Vec<u8>>> {
    files::cgroupfs(directory)?;
    let bytes: Vec<_> = SETTINGS
        .iter()
        .map(|name| files::read(directory, name))
        .collect::<Result<_>>()?;
    validate_settings(&bytes, limits)?;
    Ok(bytes)
}
fn validate_settings(bytes: &[Vec<u8>], limits: &ResourceLimits) -> Result<()> {
    if bytes.len() != SETTINGS.len() {
        return Err(Error::InvalidResources);
    }
    if bytes[0] != b"domain\n"
        || parse::list(&bytes[1], 4095)? != limits.cpus
        || parse::list(&bytes[2], 31)? != [limits.numa_node]
        || parse::finite(&bytes[3])? != limits.memory_max
        || parse::finite(&bytes[4])? != 0
    {
        return Err(Error::InvalidResources);
    }
    parse::no_descendants(&bytes[5])?;
    Ok(())
}
fn system(operation: &'static str, source: std::io::Error) -> Error {
    Error::System { operation, source }
}

#[cfg(test)]
mod tests;
