//! Approved parameter content and actual registered worker consumption.
use super::profile;
use crate::{Result, invalid};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_release::Object;
use tickwire_strategy_host::{FreshObservation, ObservationRequest};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub object_file: PathBuf,
    pub object: profile::Binding,
    pub scope: profile::Scope,
    pub schema_file: PathBuf,
    pub timeout_ns: u64,
}

/// Historical consumption at a real, fresh checkpoint; no global Applied authority.
pub struct ParametersProof {
    object: profile::Binding,
    scope: profile::Scope,
    schema_hash: [u8; 32],
    parameter_hash: [u8; 32],
    private_revision: u64,
    private_object: Object,
    observation: FreshObservation,
}
impl ParametersProof {
    pub fn object(&self) -> &profile::Binding {
        &self.object
    }
    pub fn scope(&self) -> &profile::Scope {
        &self.scope
    }
    pub fn schema_hash(&self) -> [u8; 32] {
        self.schema_hash
    }
    pub fn parameter_hash(&self) -> [u8; 32] {
        self.parameter_hash
    }
    pub fn private_revision(&self) -> u64 {
        self.private_revision
    }
    pub fn private_object(&self) -> Object {
        self.private_object
    }
    pub fn observation(&self) -> &FreshObservation {
        &self.observation
    }
}

pub(super) struct Prepared {
    object: profile::Binding,
    scope: profile::Scope,
    identity: tickwire_control::Identity,
    uid: u32,
    gid: u32,
    schema_hash: [u8; 32],
    parameter_hash: [u8; 32],
    private_revision: u64,
    private_object: Object,
    domain: ClockDomain,
    timeout_ns: u64,
}
pub(super) fn prepare(
    config: &super::HostConfig,
    bootstrap: &tickwire_strategy_sdk::bootstrap::Bootstrap,
) -> Result<Option<Prepared>> {
    let Some(input) = &config.projection_parameters else {
        return Ok(None);
    };
    check(
        (1..=30_000_000_000).contains(&input.timeout_ns),
        "query deadline bounds",
    )?;
    check(
        bootstrap.handover_enabled || config.shadow,
        "checkpoint queries must be enabled",
    )?;
    if let Some(launch) = &config.projection_launch {
        check(
            launch.object == input.object && launch.scope == input.scope,
            "launch/parameter projection mismatch",
        )?;
    }
    let projection = profile::read_projection(
        config,
        bootstrap,
        &input.object_file,
        &input.object,
        &input.scope,
    )?;
    let schema_hash = tickwire_config::artifacts::reference_hash(
        projection.payload()["strategy"]["parameter_schema_ref"]
            .as_str()
            .ok_or_else(|| invalid("parameter schema reference"))?,
    )
    .map_err(|e| invalid(&format!("parameter schema reference: {e:?}")))?;
    let schema = profile::read_object(&input.schema_file)?;
    let factory = bootstrap
        .factory
        .as_ref()
        .ok_or_else(|| invalid("parameter factory"))?;
    check(factory.revision < 1 << 63, "private revision bounds")?;
    let raw = profile::read_object(&factory.configuration_file)?;
    factory.configuration.verify(&raw, 2 * 1024 * 1024)?;
    let value = tickwire_config::artifacts::validate_parameter_document(&schema, schema_hash, &raw)
        .map_err(|e| invalid(&format!("parameter document: {e:?}")))?;
    check(
        value.value() == &projection.payload()["strategy"]["params"],
        "factory/projection values differ",
    )?;
    let identity = bootstrap.market.local.identity();
    let domain = ClockDomain::new(identity.host_boot, bootstrap.clock_generation)
        .map_err(|e| invalid(&format!("parameter clock domain: {e:?}")))?;
    Ok(Some(Prepared {
        object: input.object.clone(),
        scope: input.scope.clone(),
        identity,
        uid: config.uid,
        gid: config.gid,
        schema_hash,
        parameter_hash: value.hash(),
        private_revision: factory.revision,
        private_object: factory.configuration,
        domain,
        timeout_ns: input.timeout_ns,
    }))
}
impl Prepared {
    fn observe(
        self,
        request: &ObservationRequest,
        observed: FreshObservation,
    ) -> Result<ParametersProof> {
        let process = observed.process();
        let actual = observed
            .configuration()
            .ok_or_else(|| invalid("worker has no current merged configuration"))?;
        check(
            observed.request() == request
                && observed.challenge() != [0; 16]
                && process.identity() == &self.identity
                && process.image_hash() == self.identity.build_hash
                && process.host_boot() != [0; 16]
                && process.process().uid == self.uid
                && process.process().gid == self.gid
                && process.process().pid != 0
                && actual.revision() == self.private_revision
                && actual.object() == self.private_object,
            "actual worker configuration binding",
        )?;
        let now = sample(self.domain)?;
        check(
            observed.started_at().domain() == self.domain
                && observed.observed_at().domain() == self.domain
                && now.nanos() >= observed.observed_at().nanos()
                && now.nanos() < request.deadline.nanos(),
            "actual worker query expired",
        )?;
        Ok(ParametersProof {
            object: self.object,
            scope: self.scope,
            schema_hash: self.schema_hash,
            parameter_hash: self.parameter_hash,
            private_revision: self.private_revision,
            private_object: self.private_object,
            observation: observed,
        })
    }
}
fn check(valid: bool, reason: &str) -> Result<()> {
    if valid {
        Ok(())
    } else {
        Err(invalid(&format!("projection parameters {reason}")))
    }
}
fn sample(domain: ClockDomain) -> Result<MonoTime> {
    Ok(tickwire_clock_sys::SystemClock::new(domain)?
        .sample()?
        .monotonic)
}

pub(super) trait Owner {
    fn begin(&mut self, request: ObservationRequest) -> tickwire_strategy_host::Result<()>;
    fn poll(&mut self) -> tickwire_strategy_host::Result<Option<FreshObservation>>;
}
impl<R: tickwire_strategy_sdk::runtime::RecoveryResolver + 'static> Owner
    for tickwire_strategy_host::RemoteHostedStrategy<R>
{
    fn begin(&mut self, request: ObservationRequest) -> tickwire_strategy_host::Result<()> {
        self.begin_observation(request)
    }
    fn poll(&mut self) -> tickwire_strategy_host::Result<Option<FreshObservation>> {
        self.poll_observation()
    }
}
impl<R: tickwire_strategy_sdk::runtime::RecoveryResolver + 'static, const M: usize> Owner
    for tickwire_strategy_host::HostedStrategy<R, M>
{
    fn begin(&mut self, request: ObservationRequest) -> tickwire_strategy_host::Result<()> {
        self.begin_observation(request)
    }
    fn poll(&mut self) -> tickwire_strategy_host::Result<Option<FreshObservation>> {
        self.poll_observation()
    }
}
#[derive(Default)]
pub(super) struct Progress {
    prepared: Option<Prepared>,
    request: Option<ObservationRequest>,
    sent: bool,
    next_poll_ns: u64,
    proof: Option<ParametersProof>,
}
impl Progress {
    pub(super) fn new(prepared: Option<Prepared>) -> Self {
        Self {
            prepared,
            ..Self::default()
        }
    }
    pub(super) fn busy(&self) -> bool {
        self.prepared.is_some()
    }
    pub(super) fn proof(&self) -> Option<&ParametersProof> {
        self.proof.as_ref()
    }
    pub(super) fn poll(&mut self, owner: &mut impl Owner) -> Result<bool> {
        let Some(prepared) = &self.prepared else {
            return Ok(true);
        };
        let now = sample(prepared.domain)?;
        self.poll_at(owner, now)
    }
    pub(super) fn poll_at(&mut self, owner: &mut impl Owner, now: MonoTime) -> Result<bool> {
        let Some(prepared) = &self.prepared else {
            return Ok(true);
        };
        check(now.domain() == prepared.domain, "query clock domain")?;
        if self.request.is_none() {
            let mut seed = b"tickwire.app.parameter-consumption.v1".to_vec();
            seed.extend(prepared.object.object_hash);
            seed.extend(prepared.identity.instance);
            seed.extend(prepared.identity.generation.to_le_bytes());
            let hash = tickwire_recovery::digest(&seed);
            let request_id = hash[..16]
                .try_into()
                .map_err(|_| invalid("parameter request ID"))?;
            let deadline = now
                .nanos()
                .checked_add(prepared.timeout_ns)
                .ok_or_else(|| invalid("parameter deadline overflow"))?;
            self.request = Some(ObservationRequest {
                request_id,
                deadline: MonoTime::new(prepared.domain, deadline),
                require_resources: false,
            });
        }
        let request = self
            .request
            .as_ref()
            .ok_or_else(|| invalid("parameter query missing"))?;
        check(
            now.nanos() < request.deadline.nanos(),
            "startup query deadline",
        )?;
        if now.nanos() < self.next_poll_ns {
            return Ok(false);
        }
        self.next_poll_ns = now.nanos().saturating_add(10_000_000);
        if !self.sent {
            match owner.begin(request.clone()) {
                Ok(()) => self.sent = true,
                Err(
                    tickwire_strategy_host::Error::NotReady | tickwire_strategy_host::Error::Busy,
                ) => return Ok(false),
                Err(e) => return Err(e.into()),
            }
        }
        if let Some(observed) = owner.poll()? {
            let prepared = self
                .prepared
                .take()
                .ok_or_else(|| invalid("parameter query state"))?;
            self.proof = Some(prepared.observe(request, observed)?);
            return Ok(true);
        }
        Ok(false)
    }
}
