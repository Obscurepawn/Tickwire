//! Explicit resource-constrained launches, separate from ordinary child ownership.
use super::*;
pub use tickwire_control_sys::ResourceLimits;
use tickwire_control_sys::ResourceObservation;

#[derive(Clone, Debug)]
pub struct ResourcePlan {
    pub cgroup: PathBuf,
    pub limits: ResourceLimits,
}

/// Current kernel resource facts for the same directly owned, fixed-image child.
/// This is neither a configuration Applied receipt nor a resource release proof.
#[derive(Debug)]
pub struct LiveResourceObservation {
    process: LiveChildObservation,
    resources: ResourceObservation,
}
impl LiveResourceObservation {
    pub fn process(&self) -> &LiveChildObservation {
        &self.process
    }
    pub fn resources(&self) -> &ResourceObservation {
        &self.resources
    }
}
impl Registry {
    pub fn spawn_with_resources(
        &mut self,
        spec: LaunchSpec,
        plan: ResourcePlan,
    ) -> Result<ProcessIdentity> {
        self.spawn_inner(spec, Some(plan))
    }
    /// Samples every current thread and the actual configured resource domain.
    /// The same owned child and executable image must survive the whole sample.
    pub fn observe_resources(&mut self, instance: [u8; 16]) -> Result<LiveResourceObservation> {
        let before = self.observe(instance)?;
        let resources = self
            .entries
            .get(&instance)
            .ok_or(Error::Identity)?
            .resources
            .as_ref()
            .ok_or(Error::ResourcesRequired)?
            .observe(before.process().pid)
            .map_err(Error::Transport)?;
        let after = self.observe(instance)?;
        if before.identity() != after.identity()
            || before.process() != after.process()
            || before.host_boot() != after.host_boot()
            || before.start_ticks() != after.start_ticks()
            || before.image_hash() != after.image_hash()
            || before.image_bytes() != after.image_bytes()
            || resources.pid() != before.process().pid
        {
            return Err(Error::Identity);
        }
        Ok(LiveResourceObservation {
            process: before,
            resources,
        })
    }
}
