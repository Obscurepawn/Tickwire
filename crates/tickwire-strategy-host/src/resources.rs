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
#[serde(from = "ResourceConfig")]
pub struct ProcessResources {
    pub worker: GroupResources,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceConfig {
    worker: GroupResources,
    #[serde(default, rename = "source_replay")]
    _source: Option<GroupResources>,
    #[serde(default, rename = "rollback")]
    _rollback: Option<GroupResources>,
}
impl From<ResourceConfig> for ProcessResources {
    fn from(config: ResourceConfig) -> Self {
        Self {
            worker: config.worker,
        }
    }
}
impl ProcessResources {
    pub fn validate(&self) -> Result<()> {
        self.worker.plan()?;
        Ok(())
    }
}

pub(super) fn validate(resources: Option<&ProcessResources>) -> Result<()> {
    resources.map_or(Ok(()), ProcessResources::validate)
}

pub(super) fn spawn(
    registry: &mut Registry,
    spec: LaunchSpec,
    resources: Option<&ProcessResources>,
) -> Result<ProcessIdentity> {
    let Some(resources) = resources else {
        return Ok(registry.spawn(spec)?);
    };
    Ok(registry.spawn_with_resources(spec, resources.worker.plan()?)?)
}

#[cfg(test)]
mod tests;
