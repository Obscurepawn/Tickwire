//! Preconfigured Linux resource facts, independent of deployment authorization.
use crate::{Error, Result};
use std::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceLimits {
    pub cpus: Vec<u32>,
    pub numa_node: u32,
    pub memory_max: u64,
}
impl ResourceLimits {
    pub fn validate(&self) -> Result<()> {
        if self.cpus.is_empty()
            || self.cpus.len() > 4096
            || self.cpus.iter().any(|cpu| *cpu > 4095)
            || self.cpus.windows(2).any(|pair| pair[0] >= pair[1])
            || self.numa_node > 31
            || !(1_048_576..=9_007_199_254_740_991).contains(&self.memory_max)
        {
            return Err(Error::InvalidResources);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ResourceObservation {
    pid: u32,
    limits: ResourceLimits,
    thread_ids: Vec<u32>,
    group_device: u64,
    group_inode: u64,
    sampled_at: Instant,
}
impl ResourceObservation {
    pub fn pid(&self) -> u32 {
        self.pid
    }
    pub fn limits(&self) -> &ResourceLimits {
        &self.limits
    }
    pub fn cpus(&self) -> &[u32] {
        &self.limits.cpus
    }
    pub fn numa_node(&self) -> u32 {
        self.limits.numa_node
    }
    pub fn memory_max(&self) -> u64 {
        self.limits.memory_max
    }
    pub fn thread_ids(&self) -> &[u32] {
        &self.thread_ids
    }
    pub fn group_device(&self) -> u64 {
        self.group_device
    }
    pub fn group_inode(&self) -> u64 {
        self.group_inode
    }
    pub fn sampled_at(&self) -> Instant {
        self.sampled_at
    }
}

#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(target_os = "linux"))]
pub use crate::unsupported::ResourceDomain;
#[cfg(target_os = "linux")]
pub use linux::ResourceDomain;
#[cfg(target_os = "linux")]
mod parse;
