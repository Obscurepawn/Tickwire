//! Explicit cold-path process identity observations, without config authority.
use super::*;

/// Reads this process's kernel PID/UID/GID on an explicit cold path.
///
/// Requires matching real, effective, saved and filesystem credentials and
/// stable kernel identity across both reads. This is not an owned-child,
/// executable-image or configuration-application observation.
pub fn current_process_identity() -> Result<ProcessIdentity> {
    #[cfg(target_os = "linux")]
    {
        linux::current_process_identity()
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(Error::Transport(
            tickwire_control_sys::Error::UnsupportedPlatform,
        ))
    }
}

/// Kernel facts and approved launch identity at one local sampling interval.
/// A connected epoch is historical connection state, not a fresh challenge response.
#[derive(Debug)]
pub struct LiveChildObservation {
    identity: Identity,
    process: ProcessIdentity,
    host_boot: [u8; 16],
    start_ticks: u64,
    image_bytes: u64,
    connected_epoch: Option<u64>,
    sampled_at: Instant,
}
impl LiveChildObservation {
    pub fn identity(&self) -> &Identity {
        &self.identity
    }
    pub fn process(&self) -> ProcessIdentity {
        self.process
    }
    pub fn host_boot(&self) -> [u8; 16] {
        self.host_boot
    }
    pub fn start_ticks(&self) -> u64 {
        self.start_ticks
    }
    pub fn image_bytes(&self) -> u64 {
        self.image_bytes
    }
    pub fn image_hash(&self) -> [u8; 32] {
        self.identity.build_hash
    }
    pub fn connected_epoch(&self) -> Option<u64> {
        self.connected_epoch
    }
    /// Sampling start, for conservative freshness checks across this interval.
    pub fn sampled_at(&self) -> Instant {
        self.sampled_at
    }
}
impl Registry {
    /// Samples current kernel facts. Does not read or apply a configuration.
    pub fn observe(&mut self, instance: [u8; 16]) -> Result<LiveChildObservation> {
        #[cfg(target_os = "linux")]
        {
            linux::observe(self.entries.get_mut(&instance).ok_or(Error::Identity)?)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = instance;
            Err(Error::Transport(
                tickwire_control_sys::Error::UnsupportedPlatform,
            ))
        }
    }
}

#[cfg(target_os = "linux")]
mod linux;
