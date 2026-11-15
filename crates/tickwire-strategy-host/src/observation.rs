//! Cold-path double-sampled OS and cgroup v2 observation (`DoubleSampledObservation`,
//! `FreshObservation`) with zero WAL, checkpoint, or direct raw-FFI dependencies.
#![forbid(unsafe_code)]

use std::fmt;

use tickwire_clock_sys::SystemClock;
use tickwire_runtime_policy::OrsRuntimePolicy;
use tickwire_supervisor::{LiveChildObservation, LiveResourceObservation, Registry};

use crate::readiness::{ReadyProof, WorkerReadiness};

/// Cold-path observation failure.
#[derive(Debug)]
pub enum ObservationError {
    /// Process PID, start ticks, boot ID, or sealed executable digest changed mid-sample.
    IdentityChanged,
    /// Sampling interval exceeded `ObservationRequest::max_sample_age_ns`.
    StaleSample { elapsed_ns: u64, max_age_ns: u64 },
    /// `ObservationRequest` or `OrsRuntimePolicy` account resolution rejected the query.
    InvalidPolicy,
    /// Underlying supervisor or `/proc` / cgroup v2 error.
    Supervisor(tickwire_supervisor::Error),
    /// Monotonic clock read failed.
    Clock(tickwire_clock_sys::Error),
}

impl fmt::Display for ObservationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ObservationError {}

impl From<tickwire_supervisor::Error> for ObservationError {
    fn from(value: tickwire_supervisor::Error) -> Self {
        Self::Supervisor(value)
    }
}

impl From<tickwire_clock_sys::Error> for ObservationError {
    fn from(value: tickwire_clock_sys::Error) -> Self {
        Self::Clock(value)
    }
}

/// Parameters bounding a cold-path [`FreshObservation`] sample.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservationRequest {
    /// Maximum monotonic elapsed duration permitted across the double-sampled `/proc` read.
    pub max_sample_age_ns: u64,
    /// When `true`, require cgroup v2 [`LiveResourceObservation`] to be present.
    pub require_resources: bool,
    /// Account reference (`0` = strategy's default/first authorized account, `1..=255` explicit).
    pub account_ref: u8,
}

impl Default for ObservationRequest {
    fn default() -> Self {
        Self {
            max_sample_age_ns: 50_000_000, // 50 ms default cold-path ceiling
            require_resources: false,
            account_ref: 0,
        }
    }
}

/// Double-sampled child process identity and optional cgroup v2 resource telemetry.
///
/// Samples `before` (`Registry::observe`), optional `resources` (`Registry::observe_resources`),
/// and `after` (`Registry::observe`), failing closed with [`ObservationError::IdentityChanged`]
/// if the PID exited or was recycled mid-read.
#[derive(Debug)]
pub struct DoubleSampledObservation {
    before: LiveChildObservation,
    resources: Option<LiveResourceObservation>,
    after: LiveChildObservation,
}

impl DoubleSampledObservation {
    /// Perform a double-sampled `/proc` + optional cgroup v2 read on `instance`.
    pub fn sample(
        registry: &mut Registry,
        instance: [u8; 16],
        require_resources: bool,
    ) -> std::result::Result<Self, ObservationError> {
        let before = registry.observe(instance)?;
        let resources = match registry.observe_resources(instance) {
            Ok(res) => Some(res),
            Err(tickwire_supervisor::Error::ResourcesRequired) if !require_resources => None,
            Err(err) => return Err(ObservationError::Supervisor(err)),
        };
        let after = registry.observe(instance)?;
        if !same_child_identity(&before, &after) {
            return Err(ObservationError::IdentityChanged);
        }
        Ok(Self {
            before,
            resources,
            after,
        })
    }

    #[inline]
    pub fn before(&self) -> &LiveChildObservation {
        &self.before
    }

    #[inline]
    pub fn after(&self) -> &LiveChildObservation {
        &self.after
    }

    #[inline]
    pub fn resources(&self) -> Option<&LiveResourceObservation> {
        self.resources.as_ref()
    }
}

fn same_child_identity(a: &LiveChildObservation, b: &LiveChildObservation) -> bool {
    a.identity() == b.identity()
        && a.process() == b.process()
        && a.start_ticks() == b.start_ticks()
        && a.host_boot() == b.host_boot()
        && a.image_hash() == b.image_hash()
        && a.image_bytes() == b.image_bytes()
}

/// Bounded-age cold-path observation combining [`DoubleSampledObservation`] and [`WorkerReadiness`].
#[derive(Debug)]
pub struct FreshObservation {
    pub sample: DoubleSampledObservation,
    pub readiness: WorkerReadiness,
    pub sampled_mono_ns: u64,
    pub sample_duration_ns: u64,
}

impl FreshObservation {
    /// Collect a fresh double-sampled kernel and readiness observation on a cold housekeeping thread.
    #[allow(clippy::too_many_arguments)]
    pub fn collect(
        registry: &mut Registry,
        clock: &mut SystemClock,
        proof: &ReadyProof,
        policy: &OrsRuntimePolicy,
        request: &ObservationRequest,
        lanes_attached: bool,
        intent_last_msg_seqnum: u64,
        report_next_seq: u64,
    ) -> std::result::Result<Self, ObservationError> {
        if request.max_sample_age_ns == 0 {
            return Err(ObservationError::InvalidPolicy);
        }
        let start_ns = clock.sample()?.monotonic.nanos();
        let sample = DoubleSampledObservation::sample(
            registry,
            proof.instance_id(),
            request.require_resources,
        )?;
        let end_ns = clock.sample()?.monotonic.nanos();
        let elapsed_ns = end_ns.saturating_sub(start_ns);
        if elapsed_ns > request.max_sample_age_ns {
            return Err(ObservationError::StaleSample {
                elapsed_ns,
                max_age_ns: request.max_sample_age_ns,
            });
        }
        let account_override = (request.account_ref != 0).then_some(request.account_ref);
        let readiness = WorkerReadiness::evaluate(
            proof,
            policy,
            account_override,
            true,
            lanes_attached,
            intent_last_msg_seqnum,
            report_next_seq,
        )
        .map_err(|_| ObservationError::InvalidPolicy)?;
        Ok(Self {
            sample,
            readiness,
            sampled_mono_ns: end_ns,
            sample_duration_ns: elapsed_ns,
        })
    }
}
