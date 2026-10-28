//! One bounded fresh query for all explicitly enabled startup consumers.
use super::{inputs, parameters, profile};
use crate::{Result, invalid};
use std::sync::Arc;
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_strategy_host::{FreshObservation, ObservationRequest};

#[derive(Clone)]
pub(super) struct Binding {
    pub object: profile::Binding,
    pub scope: profile::Scope,
    pub identity: tickwire_control::Identity,
    pub uid: u32,
    pub gid: u32,
    pub domain: ClockDomain,
    pub timeout_ns: u64,
}
impl Binding {
    pub(super) fn new(
        config: &super::HostConfig,
        bootstrap: &tickwire_strategy_sdk::bootstrap::Bootstrap,
        object: &profile::Binding,
        scope: &profile::Scope,
        timeout_ns: u64,
    ) -> Result<Self> {
        check(
            (1..=30_000_000_000).contains(&timeout_ns),
            "query deadline bounds",
        )?;
        check(
            bootstrap.handover_enabled || config.shadow,
            "checkpoint queries must be enabled",
        )?;
        for selected in [
            config
                .projection_launch
                .as_ref()
                .map(|v| (&v.object, &v.scope)),
            config
                .projection_parameters
                .as_ref()
                .map(|v| (&v.object, &v.scope)),
            config
                .projection_inputs
                .as_ref()
                .map(|v| (&v.object, &v.scope)),
        ]
        .into_iter()
        .flatten()
        {
            check(selected == (object, scope), "projection bindings differ")?;
        }
        let identity = bootstrap.market.local.identity();
        let domain = ClockDomain::new(identity.host_boot, bootstrap.clock_generation)
            .map_err(|e| invalid(&format!("startup query clock: {e:?}")))?;
        Ok(Self {
            object: object.clone(),
            scope: scope.clone(),
            identity,
            uid: config.uid,
            gid: config.gid,
            domain,
            timeout_ns,
        })
    }
    pub(super) fn verify(
        &self,
        request: &ObservationRequest,
        observed: &FreshObservation,
    ) -> Result<()> {
        let process = observed.process();
        check(
            observed.request() == request
                && observed.challenge() != [0; 16]
                && process.identity() == &self.identity
                && process.image_hash() == self.identity.build_hash
                && process.host_boot() != [0; 16]
                && process.process().uid == self.uid
                && process.process().gid == self.gid
                && process.process().pid != 0,
            "actual worker identity",
        )?;
        let now = sample(self.domain)?;
        check(
            observed.started_at().domain() == self.domain
                && observed.observed_at().domain() == self.domain
                && request.deadline.domain() == self.domain
                && now.nanos() >= observed.observed_at().nanos()
                && now.nanos() < request.deadline.nanos(),
            "actual worker query expired",
        )
    }
}
fn check(valid: bool, reason: &str) -> Result<()> {
    if valid {
        Ok(())
    } else {
        Err(invalid(&format!("startup query {reason}")))
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Disabled,
    Pending,
    Ready,
    Closed,
}
pub(super) struct Progress {
    phase: Phase,
    binding: Option<Binding>,
    parameters: Option<parameters::Prepared>,
    inputs: Option<inputs::Prepared>,
    guard: Option<inputs::Guard>,
    request: Option<ObservationRequest>,
    sent: bool,
    next_poll_ns: u64,
    parameter_proof: Option<parameters::ParametersProof>,
    input_proof: Option<inputs::InputsProof>,
}
impl Progress {
    pub(super) fn new(
        parameters: Option<parameters::Prepared>,
        inputs: Option<inputs::Prepared>,
    ) -> Self {
        let mut binding = parameters
            .as_ref()
            .map(|p| p.binding.clone())
            .or_else(|| inputs.as_ref().map(|p| p.binding.clone()));
        if let (Some(binding), Some(inputs)) = (&mut binding, &inputs) {
            binding.timeout_ns = binding.timeout_ns.min(inputs.binding.timeout_ns);
        }
        let guard = inputs.as_ref().map(inputs::Prepared::guard);
        Self {
            phase: if binding.is_some() {
                Phase::Pending
            } else {
                Phase::Disabled
            },
            binding,
            parameters,
            inputs,
            guard,
            request: None,
            sent: false,
            next_poll_ns: 0,
            parameter_proof: None,
            input_proof: None,
        }
    }
    pub(super) fn busy(&self) -> bool {
        self.phase == Phase::Pending
    }
    pub(super) fn inputs_enabled(&self) -> bool {
        self.guard.is_some()
    }
    pub(super) fn proof(&self) -> Option<&parameters::ParametersProof> {
        self.parameter_proof.as_ref()
    }
    pub(super) fn inputs_proof(&self) -> Option<&inputs::InputsProof> {
        self.input_proof.as_ref()
    }
    fn close(&mut self) {
        self.phase = Phase::Closed;
        self.parameter_proof = None;
        self.input_proof = None;
    }
    pub(super) fn matches_account(
        &mut self,
        baseline: Option<&tickwire_account_codec::CanonicalBaseline>,
        account: &tickwire_account::Account,
        instance: Option<([u8; 16], u64)>,
    ) -> bool {
        if self.phase == Phase::Closed {
            return false;
        }
        if self
            .guard
            .as_ref()
            .is_some_and(|guard| !guard.matches(baseline, account, instance))
        {
            self.close();
            return false;
        }
        true
    }
    pub(super) fn poll(&mut self, owner: &mut impl Owner) -> Result<bool> {
        match self.phase {
            Phase::Disabled | Phase::Ready => return Ok(true),
            Phase::Closed => return Err(invalid("startup input gate closed")),
            Phase::Pending => {}
        }
        let now = self
            .binding
            .as_ref()
            .ok_or_else(|| invalid("startup binding missing"))
            .and_then(|binding| sample(binding.domain));
        match now {
            Ok(now) => self.poll_at(owner, now),
            Err(error) => {
                self.close();
                Err(error)
            }
        }
    }
    pub(super) fn poll_at(&mut self, owner: &mut impl Owner, now: MonoTime) -> Result<bool> {
        let result = self.advance(owner, now);
        if result.is_err() {
            self.close();
        }
        result
    }
    fn advance(&mut self, owner: &mut impl Owner, now: MonoTime) -> Result<bool> {
        match self.phase {
            Phase::Disabled | Phase::Ready => return Ok(true),
            Phase::Closed => return Err(invalid("startup input gate closed")),
            Phase::Pending => {}
        }
        let binding = self
            .binding
            .as_ref()
            .ok_or_else(|| invalid("startup binding missing"))?;
        check(now.domain() == binding.domain, "query clock domain")?;
        if self.request.is_none() {
            let mut seed = b"tickwire.app.startup-consumption.v1".to_vec();
            seed.extend(binding.object.object_hash);
            seed.extend(binding.identity.instance);
            seed.extend(binding.identity.generation.to_le_bytes());
            let hash = tickwire_recovery::digest(&seed);
            let request_id = hash[..16]
                .try_into()
                .map_err(|_| invalid("startup request ID"))?;
            let deadline = now
                .nanos()
                .checked_add(binding.timeout_ns)
                .ok_or_else(|| invalid("startup deadline overflow"))?;
            self.request = Some(ObservationRequest {
                request_id,
                deadline: MonoTime::new(binding.domain, deadline),
                require_resources: false,
            });
        }
        let request = self
            .request
            .as_ref()
            .ok_or_else(|| invalid("startup query missing"))?;
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
            let observed = Arc::new(observed);
            let parameters = self
                .parameters
                .take()
                .map(|p| p.observe(request, observed.clone()))
                .transpose()?;
            let inputs = self
                .inputs
                .take()
                .map(|p| p.observe(request, observed.clone()))
                .transpose()?;
            // Consumer validation can span a scheduling pause. Recheck the
            // original deadline before publishing either proof or opening intents.
            let completed = sample(binding.domain)?;
            check(
                completed.nanos() >= observed.observed_at().nanos()
                    && completed.nanos() < request.deadline.nanos(),
                "startup consumer validation expired",
            )?;
            // Publish both only after every enabled consumer accepted the same cut.
            self.parameter_proof = parameters;
            self.input_proof = inputs;
            self.phase = Phase::Ready;
            return Ok(true);
        }
        Ok(false)
    }
}
