//! Typed capture-only control; authentication and actual cursor checks belong to Host.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tickwire_clock::MonoTime;
use tickwire_ipc::{ControlEnvelope, ReplayControlBody, ResourceHeader, Wire};
use tickwire_release::{
    Object,
    rebuild::{Contract, window::Identity},
    replay::{Model, VersionedBytes},
};
pub const KIND: u16 = 8;
pub const MAX_BYTES: usize = 2 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub schema: u16,
    pub identity: Identity,
    pub contract: Contract,
    pub config: VersionedBytes,
    pub models: Vec<Model>,
    pub market: tickwire_market_source::ConnectConfig,
    pub market_admission_ready: PathBuf,
    pub report: crate::bootstrap::Endpoint,
    pub report_header: Vec<u8>,
    pub report_cursor_id: [u8; 16],
    pub report_subscription_generation: u64,
    pub report_directory: PathBuf,
    pub output_directory: PathBuf,
    pub initial_account_position: u64,
    pub deadline_mono_ns: u64,
    pub poll_records: usize,
    pub idle_sleep_us: u64,
}
impl Bootstrap {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        Ok(tickwire_release::replay::canonical(self)?)
    }
    pub fn qualified_market_header(&self) -> Result<ResourceHeader> {
        let mut bank = ResourceHeader::decode(&self.market.bank_header)?;
        let declared = bank.descriptor_hash;
        bank.descriptor_hash = [0; 32];
        bank.descriptor_hash = tickwire_recovery::digest(&crate::bootstrap::wire_bytes(&bank)?);
        if declared != [0; 32] && declared != bank.descriptor_hash {
            return Err(Error::Identity);
        }
        Ok(bank)
    }
    pub fn validate(&self, now: MonoTime) -> Result<()> {
        self.identity.validate()?;
        self.contract.validate()?;
        let local = &self.market.endpoint.local;
        let bank = self.qualified_market_header()?;
        let report = ResourceHeader::decode(&self.report_header)?;
        let stream = crate::replay::application::market_stream(&bank)?;
        if self.schema != 1
            || self.bytes()?.len() > MAX_BYTES
            || self.identity.instance != local.instance
            || self.identity.generation != local.generation
            || self.identity.logical_strategy != local.logical_strategy
            || self.identity.binary != local.build_hash
            || self.contract.binary != self.identity.binary
            || self.identity.clock.boot_id != local.host_boot
            || self.identity.clock.generation != self.market.clock_generation
            || now.domain() != self.market.domain()?
            || now.nanos() >= self.deadline_mono_ns
            || self.report.local.identity() != local.identity()
            || local.role != 3
            || self.market.endpoint.peer.role != 2
            || self.report.peer.role != 4
            || self.config.revision == 0
            || Object::of(&self.config.bytes) != self.contract.config
            || self.models.len() != self.contract.models.len()
            || self.models.iter().zip(&self.contract.models).any(|(a, b)| {
                a.id != b.id || a.value.revision == 0 || Object::of(&a.value.bytes) != b.value
            })
            || self.contract.ordering != tickwire_release::rebuild::Ordering::ObservedRebuildWindow
            || self.contract.markets.iter().any(|m| {
                m.stream != stream || !self.market.instruments.iter().any(|i| i.id == m.instrument)
            })
            || self.poll_records == 0
            || self.poll_records > 4096
            || self.idle_sleep_us > 1_000_000
            || self.report_cursor_id == [0; 16]
            || self.report_subscription_generation == 0
            || self.report_cursor_id == self.market.cursor_id
            || self.report_directory == self.output_directory
            || report.resource_kind != 3
            || report.record_bytes != 512
            || report.required_features & crate::reports::FEATURE == 0
            || report.consumer_instance != local.instance
            || report.producer_instance != self.report.peer.instance
            || report.producer_generation != self.report.peer.generation
            || report.host_boot_id != local.host_boot
            || report.namespace_id != local.namespace
            || report.risk_scope == 0
            || report.first_seq == 0
            || report
                .first_seq
                .checked_add(self.initial_account_position)
                .is_none()
            || [
                &self.market_admission_ready,
                &self.report_directory,
                &self.output_directory,
                &self.report.path,
                &self.market.endpoint.path,
            ]
            .iter()
            .any(|p| !p.is_absolute())
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealRequest {
    pub schema: u16,
    pub capture: [u8; 16],
    pub bootstrap: Object,
    pub contract: Object,
    pub account_position: u64,
    pub deadline_mono_ns: u64,
}
impl SealRequest {
    pub fn bytes(&self) -> Result<Vec<u8>> {
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
    pub fn validate(&self, bootstrap: &Bootstrap, now: MonoTime) -> Result<()> {
        bootstrap.validate(now)?;
        if self.schema != 1
            || self.capture != bootstrap.identity.capture
            || self.bootstrap != Object::of(&bootstrap.bytes()?)
            || self.contract != Object::of(&bootstrap.contract.bytes()?)
            || self.account_position < bootstrap.initial_account_position
            || self.deadline_mono_ns > bootstrap.deadline_mono_ns
            || now.nanos() >= self.deadline_mono_ns
            || ResourceHeader::decode(&bootstrap.report_header)?
                .first_seq
                .checked_add(self.account_position)
                .is_none()
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
pub fn wire_request(
    config: &Bootstrap,
    request: &SealRequest,
    until: u64,
) -> Result<ReplayControlBody> {
    let object = Object::of(&request.bytes()?);
    let mut challenge = Vec::from(b"tickwire.capture.seal.v1".as_slice());
    challenge.extend(object.digest);
    challenge.extend(config.identity.capture);
    Ok(ReplayControlBody {
        instance_id: config.identity.instance,
        strategy_generation: config.identity.generation,
        logical_strategy_id: config.identity.logical_strategy,
        challenge: tickwire_recovery::digest(&challenge)[..16]
            .try_into()
            .map_err(|_| Error::Bounds)?,
        valid_until_mono_ns: request.deadline_mono_ns.min(until),
        object_digest: object.digest,
        object_bytes: object.bytes,
        expected_sequence: request.account_position,
        expected_cut_digest: request.contract.digest,
        operation_id: config.identity.capture,
        operation_revision: 1,
        schema: 1,
        replay_kind: KIND,
    })
}
pub fn verify_request(
    envelope: &ControlEnvelope,
    body: &ReplayControlBody,
    config: &Bootstrap,
    request: &SealRequest,
    now: MonoTime,
    until: u64,
) -> Result<()> {
    request.validate(config, now)?;
    if envelope.opcode != 41
        || envelope.flags != 1
        || envelope.status != 0
        || envelope.expected_state_version != 0
        || envelope.request_id != body.challenge
        || now.nanos() >= body.valid_until_mono_ns
        || *body != wire_request(config, request, until)?
    {
        return Err(Error::Identity);
    }
    Ok(())
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub schema: u16,
    pub bootstrap: Object,
    pub request: Object,
    pub completion: crate::replay::rebuild::capture::store::Completion,
    pub pid: u32,
    pub report_ring_sequence: u64,
    pub market_applied: u64,
    pub observed_mono_ns: u64,
}
impl Receipt {
    pub fn bytes(&self) -> Result<Vec<u8>> {
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
        config: &Bootstrap,
        request: &SealRequest,
        pid: u32,
        now: MonoTime,
        window: &crate::replay::rebuild::capture::VerifiedWindow,
    ) -> Result<()> {
        request.validate(config, now)?;
        self.completion.bytes()?;
        let seal = window.seal();
        let seed = window.seed();
        if seed.config != config.config
            || seed.models != config.models
            || seed.markets.len() != 1
            || seed.markets[0].resource
                != crate::bootstrap::wire_bytes(&config.qualified_market_header()?)?
            || seed.markets[0].maximum_levels != config.market.maximum_levels
            || seal.start.accounts.len() != 1
            || seal.start.accounts[0].sequence != config.initial_account_position
        {
            return Err(Error::Identity);
        }
        let market = tickwire_market_source::replica::MarketReplica::restore_checkpoint(
            &seed.markets[0].checkpoint,
            config.market.maximum_levels,
        )?;
        if market.instruments().count() != config.market.instruments.len()
            || config
                .market
                .instruments
                .iter()
                .any(|i| market.view(i.id).is_none_or(|v| v.version() != i.version))
        {
            return Err(Error::Identity);
        }
        if self.schema != 1
            || pid == 0
            || self.pid != pid
            || self.bootstrap != request.bootstrap
            || self.request != Object::of(&request.bytes()?)
            || self.completion.identity != config.identity
            || window.identity() != &config.identity
            || window.contract() != &config.contract
            || self.completion.contract != request.contract
            || self.completion.seed != Object::of(&window.seed().bytes()?)
            || self.completion.inputs != Object::of(&window.inputs().bytes()?)
            || self.completion.seal != Object::of(&seal.bytes()?)
            || seal.end.accounts.len() != 1
            || seal.end.accounts[0].sequence != request.account_position
            || seal.end.markets.len() != 1
            || seal.end.markets[0].sequence != self.market_applied
            || seal.end.now_ns > self.observed_mono_ns
            || self.observed_mono_ns > now.nanos()
            || self.observed_mono_ns >= request.deadline_mono_ns
            || self.report_ring_sequence
                != ResourceHeader::decode(&config.report_header)?
                    .first_seq
                    .checked_add(request.account_position)
                    .ok_or(Error::Bounds)?
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
pub use crate::market_transition::{
    encode_rejection, encode_reply, encode_request, verify_rejection,
};
#[cfg(test)]
pub(crate) mod tests;
