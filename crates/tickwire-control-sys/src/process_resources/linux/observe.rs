use super::*;
use std::os::unix::fs::OpenOptionsExt;

pub(super) fn sample(domain: &ResourceDomain, pid: u32) -> Result<ResourceObservation> {
    let sampled_at = Instant::now();
    settings(&domain.directory, &domain.limits)?;
    let before = tasks(pid)?;
    membership(domain, pid, &before)?;
    let expected = child::Mask::new(&domain.limits.cpus);
    for tid in &before {
        if child::Mask::read(*tid).map_err(|e| system("read thread affinity", e))? != expected {
            return Err(Error::InvalidResources);
        }
        let status = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(format!("/proc/{pid}/task/{tid}/status"))
            .map_err(|e| system("open thread status", e))?;
        parse::thread_status(&files::bounded(status)?, pid, *tid, domain.limits.numa_node)?;
    }
    let after = tasks(pid)?;
    if before != after {
        return Err(Error::ResourceConflict);
    }
    membership(domain, pid, &after)?;
    settings(&domain.directory, &domain.limits)?;
    Ok(ResourceObservation {
        pid,
        limits: domain.limits.clone(),
        thread_ids: after,
        group_device: domain.device,
        group_inode: domain.inode,
        sampled_at,
    })
}
fn membership(domain: &ResourceDomain, pid: u32, threads: &[u32]) -> Result<()> {
    if parse::ids(&files::read(&domain.directory, c"cgroup.procs")?)? != [pid]
        || parse::ids(&files::read(&domain.directory, c"cgroup.threads")?)? != threads
    {
        return Err(Error::ResourceConflict);
    }
    Ok(())
}
pub(super) fn tasks(pid: u32) -> Result<Vec<u32>> {
    let entries = std::fs::read_dir(format!("/proc/{pid}/task"))
        .map_err(|e| system("list process threads", e))?;
    let mut tids = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| system("read process thread", e))?;
        let name = entry.file_name();
        let id: u32 = parse::decimal(name.to_str().ok_or(Error::InvalidResources)?)?
            .try_into()
            .map_err(|_| Error::InvalidResources)?;
        if id == 0 || tids.len() == MAX_TASKS {
            return Err(Error::InvalidResources);
        }
        tids.push(id);
    }
    tids.sort_unstable();
    if tids.is_empty() || !tids.contains(&pid) || tids.windows(2).any(|v| v[0] == v[1]) {
        return Err(Error::InvalidResources);
    }
    Ok(tids)
}
