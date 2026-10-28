//! Explicit, cold, fresh observations of qualified mode-2 SDK instances.
use super::*;
use tickwire_clock::MonoTime;
use tickwire_ipc::HandoverReadyBody;
use tickwire_supervisor::{LiveChildObservation, LiveResourceObservation};
pub(crate) mod configuration;
pub use configuration::ObservedConfiguration;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservationRequest {
    pub request_id: [u8; 16],
    pub deadline: MonoTime,
    pub require_resources: bool,
}
pub(crate) enum Kernel {
    Process(LiveChildObservation),
    Resources(LiveResourceObservation),
}
impl Kernel {
    pub(crate) fn process(&self) -> &LiveChildObservation {
        match self {
            Self::Process(p) => p,
            Self::Resources(p) => p.process(),
        }
    }
    pub(crate) fn resources(&self) -> Option<&tickwire_control_sys::ResourceObservation> {
        match self {
            Self::Process(_) => None,
            Self::Resources(p) => Some(p.resources()),
        }
    }
}
/// Constructor is private: content supplied by a caller is never a live observation.
pub struct FreshObservation {
    pub(crate) request: ObservationRequest,
    pub(crate) challenge: [u8; 16],
    pub(crate) epoch: u64,
    pub(crate) floor: u64,
    pub(crate) ready: HandoverReadyBody,
    pub(crate) account: [u8; 32],
    pub(crate) before: Kernel,
    pub(crate) after: Kernel,
    pub(crate) started: MonoTime,
    pub(crate) completed: MonoTime,
    pub(crate) subscription: tickwire_shm::SubscriptionKey,
    pub(crate) objects: Vec<Vec<u8>>,
    pub(crate) configuration: Option<ObservedConfiguration>,
}
impl FreshObservation {
    pub fn request(&self) -> &ObservationRequest {
        &self.request
    }
    pub fn request_id(&self) -> [u8; 16] {
        self.request.request_id
    }
    pub fn challenge(&self) -> [u8; 16] {
        self.challenge
    }
    pub fn control_epoch(&self) -> u64 {
        self.epoch
    }
    pub fn report_sequence_floor(&self) -> u64 {
        self.floor
    }
    pub fn ready(&self) -> &HandoverReadyBody {
        &self.ready
    }
    pub fn account_input_digest(&self) -> [u8; 32] {
        self.account
    }
    pub fn process(&self) -> &LiveChildObservation {
        self.after.process()
    }
    pub fn before_process(&self) -> &LiveChildObservation {
        self.before.process()
    }
    pub fn resources(&self) -> Option<&tickwire_control_sys::ResourceObservation> {
        self.after.resources()
    }
    pub fn started_at(&self) -> MonoTime {
        self.started
    }
    pub fn observed_at(&self) -> MonoTime {
        self.completed
    }
    pub fn deadline(&self) -> MonoTime {
        self.request.deadline
    }
    pub fn subscription(&self) -> tickwire_shm::SubscriptionKey {
        self.subscription
    }
    pub fn objects(&self) -> &[Vec<u8>] {
        &self.objects
    }
    pub fn configuration(&self) -> Option<&ObservedConfiguration> {
        self.configuration.as_ref()
    }
}
mod owner;
mod prefix;
pub(crate) use prefix::Prefixes;

pub(crate) fn sample(registry: &mut Registry, id: [u8; 16], resources: bool) -> Result<Kernel> {
    if resources {
        Ok(Kernel::Resources(registry.observe_resources(id)?))
    } else {
        Ok(Kernel::Process(registry.observe(id)?))
    }
}
pub(crate) fn same_process(before: &Kernel, after: &Kernel) -> bool {
    let a = before.process();
    let b = after.process();
    a.identity() == b.identity()
        && a.process() == b.process()
        && a.host_boot() == b.host_boot()
        && a.start_ticks() == b.start_ticks()
        && a.image_bytes() == b.image_bytes()
        && a.image_hash() == b.image_hash()
        && a.connected_epoch() == b.connected_epoch()
        && match (before.resources(), after.resources()) {
            (None, None) => true,
            (Some(a), Some(b)) => {
                a.group_device() == b.group_device()
                    && a.group_inode() == b.group_inode()
                    && a.limits() == b.limits()
            }
            _ => false,
        }
}
