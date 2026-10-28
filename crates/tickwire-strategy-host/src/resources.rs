//! Explicit startup resource policy, independent of execution authorization.
use super::*;
use serde::{Deserialize, Serialize};
use tickwire_supervisor::{ResourceLimits, ResourcePlan};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupResources {
    pub cgroup: PathBuf,
    pub cpus: Vec<u32>,
    pub numa_node: u32,
    pub memory_max: u64,
}
impl GroupResources {
    fn plan(&self) -> Result<ResourcePlan> {
        let raw = self.cgroup.as_os_str().as_encoded_bytes();
        if self.cpus.len() > 4096
            || !self.cgroup.is_absolute()
            || raw.len() > 4096
            || raw.contains(&0)
            || raw[1..]
                .split(|v| *v == b'/')
                .any(|v| v.is_empty() || v == b"." || v == b"..")
        {
            return Err(Error::Invalid);
        }
        let limits = ResourceLimits {
            cpus: self.cpus.clone(),
            numa_node: self.numa_node,
            memory_max: self.memory_max,
        };
        limits.validate()?;
        Ok(ResourcePlan {
            cgroup: self.cgroup.clone(),
            limits,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessResources {
    pub worker: GroupResources,
    pub source_replay: GroupResources,
    pub rollback: GroupResources,
}
impl ProcessResources {
    pub fn validate(&self) -> Result<()> {
        self.worker.plan()?;
        self.source_replay.plan()?;
        self.rollback.plan()?;
        if self.worker.cgroup == self.source_replay.cgroup
            || self.worker.cgroup == self.rollback.cgroup
            || self.source_replay.cgroup == self.rollback.cgroup
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) enum Purpose {
    Worker,
    SourceReplay,
    Rollback,
}

pub(super) fn validate(resources: Option<&ProcessResources>) -> Result<()> {
    resources.map_or(Ok(()), ProcessResources::validate)
}

pub(super) fn spawn(
    registry: &mut Registry,
    spec: LaunchSpec,
    resources: Option<&ProcessResources>,
    purpose: Purpose,
) -> Result<ProcessIdentity> {
    let Some(resources) = resources else {
        return Ok(registry.spawn(spec)?);
    };
    resources.validate()?;
    let group = match purpose {
        Purpose::Worker => &resources.worker,
        Purpose::SourceReplay => &resources.source_replay,
        Purpose::Rollback => &resources.rollback,
    };
    Ok(registry.spawn_with_resources(spec, group.plan()?)?)
}

#[cfg(test)]
mod tests;
