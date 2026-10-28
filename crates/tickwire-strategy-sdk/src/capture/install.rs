//! Same-process installation control after a verified capture window.
use super::{Bootstrap as CaptureBootstrap, Receipt as CaptureReceipt, *};
use crate::bootstrap::Bootstrap;
use tickwire_release::rebuild::{install as content, window::Seal};
pub mod rollback;
pub const KIND: u16 = 9;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub request: content::Request,
    pub bootstrap: Bootstrap,
    pub files: Vec<tickwire_release::files::Location>,
    /// Sorted immutable objects in the capture output CAS. Configuration bytes
    /// come from the original verified window, never from a replacement path.
    pub objects: Vec<Object>,
}
impl Request {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        self.request.bytes()?;
        if self.request.target_bootstrap
            != Object::of(&tickwire_release::replay::canonical(&self.bootstrap)?)
            || self.files.is_empty()
            || self.files.len() > tickwire_release::MAX_FILES
            || self.files.windows(2).any(|w| w[0].role >= w[1].role)
            || self.files.iter().any(|f| !f.path.is_absolute())
            || self.objects.is_empty()
            || self.objects.len() > 512
            || self.objects.windows(2).any(|w| w[0].digest >= w[1].digest)
        {
            return Err(Error::Identity);
        }
        let binding = self.bootstrap.factory.as_ref().ok_or(Error::Identity)?;
        binding.validate()?;
        let location = |role| {
            self.files
                .iter()
                .find(|f| f.role == role)
                .map(|f| f.path.as_path())
        };
        if location(tickwire_release::Role::StrategyConfig)
            != Some(binding.configuration_file.as_path())
            || location(tickwire_release::Role::RecoveryTemplate)
                != self.bootstrap.recovery_template.as_deref()
            || self.bootstrap.recovery_template.is_none()
        {
            return Err(Error::Identity);
        }
        let mut total = 0u64;
        for object in &self.objects {
            object.validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
            total = total
                .checked_add(object.bytes)
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
                .ok_or(Error::Bounds)?;
        }
        for required in [
            self.request.plans,
            self.request.manifest,
            self.request.capture_completion,
            self.request.capture_seal,
            self.request.capture_receipt,
            self.request.target_bootstrap,
        ] {
            if !self.objects.contains(&required) {
                return Err(Error::Identity);
            }
        }
        let bytes = tickwire_release::replay::canonical(self)?;
        if bytes.len() > MAX_BYTES {
            return Err(Error::Bounds);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(Error::Bounds);
        }
        let value: Self = serde_json::from_slice(bytes)?;
        if value.bytes()? != bytes {
            return Err(Error::Identity);
        }
        Ok(value)
    }
    pub fn validate(
        &self,
        capture: &CaptureBootstrap,
        receipt: &CaptureReceipt,
        window: &crate::replay::rebuild::capture::VerifiedWindow,
        now: MonoTime,
    ) -> Result<()> {
        self.bytes()?;
        capture.validate(now)?;
        let b = &self.bootstrap;
        let r = &self.request;
        let identity = &capture.identity;
        if r.capture_receipt != Object::of(&receipt.bytes()?)
            || r.capture_completion != Object::of(&receipt.completion.bytes()?)
            || r.capture_seal != Object::of(&window.seal().bytes()?)
            || receipt.completion.identity != *identity
            || window.seal().identity != *identity
            || receipt.bootstrap != Object::of(&capture.bytes()?)
            || receipt.observed_mono_ns > now.nanos()
            || r.operation.logical_strategy_id != identity.logical_strategy
            || r.member.risk_domain != identity.risk_domain
            || r.member.new_instance != identity.instance
            || r.member.new_generation != identity.generation
            || r.member.clock_boot_id != identity.clock.boot_id
            || r.member.clock_generation != identity.clock.generation
            || r.deadline_mono_ns > capture.deadline_mono_ns
            || now.nanos() >= r.deadline_mono_ns
            || b.version != 1
            || b.clock_generation != identity.clock.generation
            || b.risk_domain != identity.risk_domain
            || tickwire_release::replay::canonical(&b.market)?
                != tickwire_release::replay::canonical(&capture.market.endpoint)?
            || b.external_market
                .as_ref()
                .map(tickwire_release::replay::canonical)
                .transpose()?
                != Some(tickwire_release::replay::canonical(&capture.market)?)
            || b.market_admission_ready.as_ref() != Some(&capture.market_admission_ready)
            || tickwire_release::replay::canonical(&b.report)?
                != tickwire_release::replay::canonical(&capture.report)?
            || b.report_header != capture.report_header
            || b.report_cursor_id != capture.report_cursor_id
            || b.report_subscription_generation != capture.report_subscription_generation
            || b.report_directory != capture.report_directory
            || b.intent.local.identity() != capture.market.endpoint.local.identity()
            || b.intent.peer.identity() != capture.report.peer.identity()
            || b.intent.peer_pid != capture.report.peer_pid
            || b.intent.peer_uid != capture.report.peer_uid
            || b.intent.peer_gid != capture.report.peer_gid
            || b.intent.path == b.report.path
            || b.intent.path == b.market.path
            || b.intent.local.role != 3
            || b.intent.peer.role != 4
            || b.intent_cursor_id == [0; 16]
            || b.intent_cursor_id == b.report_cursor_id
            || b.intent_cursor_id == capture.market.cursor_id
            || b.intent_subscription_generation == 0
            || b.maximum_levels != capture.market.maximum_levels
            || b.maximum_market_age_ns != capture.contract.maximum_age_ns
            || b.poll_records == 0
            || b.poll_records > 4096
            || b.idle_sleep_us > 1_000_000
            || b.startup_timeout_ms == 0
            || b.startup_timeout_ms > 60_000
            || !b.handover_enabled
            || b.preflight_job.is_some()
            || b.demo_enabled
            || b.demo_command_file.is_some()
            || b.demo_minimum_bid_ticks != 0
            || b.recovery_template.is_none()
            || b.command_directory == capture.output_directory
            || b.command_directory == capture.report_directory
            || [&b.command_directory, &b.intent.path, &b.join_file]
                .iter()
                .any(|p| !p.is_absolute())
            || b.recovery_template
                .as_ref()
                .is_none_or(|p| !p.is_absolute())
        {
            return Err(Error::Identity);
        }
        let binding = b.factory.as_ref().ok_or(Error::Identity)?;
        binding.validate()?;
        if binding.id != capture.contract.factory
            || binding.configuration != capture.contract.config
            || binding.revision != capture.config.revision
        {
            return Err(Error::Identity);
        }
        let intent = b.intent_resource()?;
        if intent.resource_kind != 2
            || intent.record_bytes != 1024
            || intent.required_features & 128 == 0
            || intent.risk_scope == 0
            || intent.producer_instance != identity.instance
            || intent.producer_generation != identity.generation
            || intent.consumer_instance != b.intent.peer.instance
            || intent.first_seq == 0
            || intent.namespace_id != b.intent.local.namespace
            || intent.host_boot_id != identity.clock.boot_id
            || intent.resource_id == ResourceHeader::decode(&capture.report_header)?.resource_id
            || intent.resource_id == capture.qualified_market_header()?.resource_id
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
pub fn wire_request(
    config: &CaptureBootstrap,
    request: &Request,
    seal: &Seal,
    until: u64,
) -> Result<ReplayControlBody> {
    let object = Object::of(&request.bytes()?);
    request
        .request
        .capture_seal
        .verify(&seal.bytes()?, tickwire_release::replay::MAX_BYTES as u64)?;
    let mut challenge = b"tickwire.rebuild.install.v1".to_vec();
    challenge.extend(object.digest);
    challenge.extend(config.identity.capture);
    Ok(ReplayControlBody {
        instance_id: config.identity.instance,
        strategy_generation: config.identity.generation,
        logical_strategy_id: config.identity.logical_strategy,
        challenge: tickwire_recovery::digest(&challenge)[..16]
            .try_into()
            .map_err(|_| Error::Bounds)?,
        valid_until_mono_ns: request.request.deadline_mono_ns.min(until),
        object_digest: object.digest,
        object_bytes: object.bytes,
        expected_sequence: seal.end.rows,
        expected_cut_digest: request.request.capture_seal.digest,
        operation_id: request.request.operation.operation_id,
        operation_revision: request.request.operation.operation_revision,
        schema: 1,
        replay_kind: KIND,
    })
}
pub fn verify_request(
    envelope: &ControlEnvelope,
    body: &ReplayControlBody,
    config: &CaptureBootstrap,
    request: &Request,
    seal: &Seal,
    now: MonoTime,
    until: u64,
) -> Result<()> {
    if envelope.opcode != 41
        || envelope.flags != 1
        || envelope.status != 0
        || envelope.expected_state_version != 0
        || envelope.request_id != body.challenge
        || now.domain() != config.market.domain()?
        || now.nanos() >= body.valid_until_mono_ns
        || *body != wire_request(config, request, seal, until)?
    {
        return Err(Error::Identity);
    }
    Ok(())
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub schema: u16,
    pub request: Object,
    pub installation: Object,
    pub pid: u32,
    pub report_ring_sequence: u64,
    pub market_applied: u64,
    pub observed_mono_ns: u64,
}
impl Receipt {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        if self.schema != 1 || self.pid == 0 || self.observed_mono_ns == 0 {
            return Err(Error::Identity);
        }
        self.request.validate(MAX_BYTES as u64)?;
        self.installation
            .validate(tickwire_release::replay::MAX_BYTES as u64)?;
        Ok(tickwire_release::replay::canonical(self)?)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(Error::Bounds);
        }
        let value: Self = serde_json::from_slice(bytes)?;
        if value.bytes()? != bytes {
            return Err(Error::Identity);
        }
        Ok(value)
    }
    pub fn verify(
        &self,
        config: &CaptureBootstrap,
        request: &Request,
        captured: &CaptureReceipt,
        verified: &content::Verified,
        pid: u32,
        now: MonoTime,
    ) -> Result<()> {
        self.bytes()?;
        let installed = verified.installation();
        if self.request != Object::of(&request.bytes()?)
            || self.installation != verified.root()
            || self.pid != pid
            || self.pid != captured.pid
            || self.pid != installed.pid
            || installed.request != Object::of(&request.request.bytes()?)
            || installed.capture_receipt != Object::of(&captured.bytes()?)
            || self.report_ring_sequence != captured.report_ring_sequence
            || self.market_applied != captured.market_applied
            || installed.completed_ns > self.observed_mono_ns
            || self.observed_mono_ns > now.nanos()
            || now.domain() != config.market.domain()?
            || now.nanos() >= request.request.deadline_mono_ns
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests;
