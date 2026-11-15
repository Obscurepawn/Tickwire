//! Explicit startup cgroup v2 resource policy (`GroupResources`, `ProcessResources`), independent
//! of execution authorization.
#![forbid(unsafe_code)]

use std::path::PathBuf;
#[cfg(test)]
use std::{
    path::Path,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
#[cfg(test)]
use tickwire_control::Identity;
use tickwire_control::ProcessIdentity;
#[cfg(test)]
use tickwire_control_sys::PrivateDirectory;
use tickwire_supervisor::{LaunchSpec, Registry, ResourceLimits, ResourcePlan};

use crate::{Error, Result};

/// Explicit Linux cgroup v2 CPU, NUMA, and memory ceiling for one supervised worker process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupResources {
    pub cgroup: PathBuf,
    pub cpus: Vec<u32>,
    pub numa_node: u32,
    pub memory_max: u64,
}

impl GroupResources {
    pub(crate) fn plan(&self) -> Result<ResourcePlan> {
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

/// Validated startup resource plan for a supervised strategy worker.
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
    /// Validate cgroup path normalization, CPU set bounds, NUMA node bounds, and `memory_max`.
    pub fn validate(&self) -> Result<()> {
        self.worker.plan()?;
        Ok(())
    }
}

pub(crate) fn validate(resources: Option<&ProcessResources>) -> Result<()> {
    resources.map_or(Ok(()), ProcessResources::validate)
}

pub(crate) fn spawn(
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
