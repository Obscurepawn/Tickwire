//! Exact content binding for an externally authorized market transition.
//! Decoding and validation do not confer authority to replace a subscription.
use crate::protocol::wire_bytes;
use crate::{Client, ConnectConfig, Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tickwire_clock::MonoTime;
use tickwire_ipc::{ResourceHeader, Wire};

pub const MAX_CONTROL_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_TRANSITION_NS: u64 = 60_000_000_000;

/// Bind a publisher QUIESCE to the approved expansion and exact old cursor.
pub fn quiesce_request_id(operation: [u8; 16], sub: tickwire_shm::SubscriptionKey) -> [u8; 16] {
    let mut hash = Sha256::new();
    hash.update(b"tickwire.md.expansion.quiesce.v1");
    hash.update(operation);
    hash.update(sub.bank.resource.to_le_bytes());
    hash.update(sub.instance.to_le_bytes());
    hash.update(sub.generation.to_le_bytes());
    hash.update(sub.cursor.to_le_bytes());
    hash.finalize()[..16]
        .try_into()
        .expect("fixed digest prefix")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Approval {
    pub schema: u16,
    pub operation_id: [u8; 16],
    pub approval_digest: [u8; 32],
    pub revision: u64,
    pub previous: ConnectConfig,
    pub next_template: ConnectConfig,
    pub deadline_mono_ns: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Commit {
    pub schema: u16,
    pub operation_id: [u8; 16],
    pub approval_digest: [u8; 32],
    pub revision: u64,
    pub config: ConnectConfig,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Armed,
    Quiesced,
    Activated,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub schema: u16,
    pub operation_id: [u8; 16],
    pub approval_digest: [u8; 32],
    pub approval_hash: [u8; 32],
    pub revision: u64,
    pub phase: Phase,
    pub pid: u32,
    pub observed_mono_ns: u64,
    pub previous_applied: Option<u64>,
    pub next_descriptor_hash: Option<[u8; 32]>,
    pub activated_sequence: Option<u64>,
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.is_empty() || bytes.len() > MAX_CONTROL_BYTES {
        return Err(Error::Bounds);
    }
    Ok(serde_json::from_slice(bytes)?)
}
fn encoded<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > MAX_CONTROL_BYTES {
        return Err(Error::Bounds);
    }
    Ok(bytes)
}
impl Approval {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode(bytes)
    }
    pub fn digest(&self) -> Result<[u8; 32]> {
        Ok(Sha256::digest(encoded(self)?).into())
    }
    pub fn validate(&self, current: &ConnectConfig, now: MonoTime) -> Result<()> {
        let before = &self.previous;
        let after = &self.next_template;
        if self.schema != 1
            || self.operation_id == [0; 16]
            || self.approval_digest == [0; 32]
            || self.revision < 2
            || encoded(before)? != encoded(current)?
            || now.domain() != before.domain()?
            || after.domain()? != before.domain()?
            || now.nanos() >= self.deadline_mono_ns
            || self.deadline_mono_ns - now.nanos() > MAX_TRANSITION_NS
            || before.maximum_levels == 0
            || before.maximum_levels > 10000
            || before.instrument_set_version == 0
            || before.instrument_set_version >= 1 << 63
            || before.subscription_generation == 0
            || before.subscription_generation >= 1 << 63
            || before.cursor_id == [0; 16]
            || after.maximum_levels != before.maximum_levels
            || after.catalog_hash != before.catalog_hash
            || before.catalog_hash == [0; 32]
            || after.instrument_set_version <= before.instrument_set_version
            || after.instrument_set_version >= 1 << 63
            || after.subscription_generation <= before.subscription_generation
            || after.subscription_generation >= 1 << 63
            || after.cursor_id == [0; 16]
            || after.cursor_id == before.cursor_id
            || after.request_id == [0; 16]
            || after.request_id == before.request_id
            || after.endpoint.control_epoch <= before.endpoint.control_epoch
            || after.endpoint.nonce == before.endpoint.nonce
            || after.startup_timeout_ms == 0
            || after.startup_timeout_ms > 60000
        {
            return Err(Error::Identity);
        }
        tickwire_control::ClientHandshake::new(before.endpoint.policy(), before.request_id)?;
        tickwire_control::ClientHandshake::new(after.endpoint.policy(), after.request_id)?;
        let mut endpoint = after.endpoint.clone();
        endpoint.control_epoch = before.endpoint.control_epoch;
        endpoint.nonce = before.endpoint.nonce;
        if encoded(&endpoint)? != encoded(&before.endpoint)? {
            return Err(Error::Identity);
        }
        validate_members(before)?;
        validate_members(after)?;
        if after.instruments.len() <= before.instruments.len()
            || before.instruments.iter().any(|old| {
                !after.instruments.iter().any(|new| {
                    old.id == new.id
                        && old.version == new.version
                        && old.required_states == new.required_states
                })
            })
        {
            return Err(Error::Identity);
        }
        let old = ResourceHeader::decode(&before.bank_header)?;
        let next = ResourceHeader::decode(&after.bank_header)?;
        tickwire_shm::validate_bank(&old)?;
        tickwire_shm::validate_bank(&next)?;
        if old.descriptor_hash != [0; 32] {
            verify_hash(&old)?;
        }
        if now.nanos() < old.created_mono_ns
            || old.producer_instance != before.endpoint.peer.instance
            || old.producer_generation != before.endpoint.peer.generation
            || old.host_boot_id != before.endpoint.local.host_boot
            || old.namespace_id != before.endpoint.local.namespace
            || before.endpoint.peer.role != 2
            || !matches!(before.endpoint.local.role, 3 | 4)
            || next.resource_id == [0; 16]
            || next.resource_id == old.resource_id
            || next.resource_id == before.cursor_id
            || after.cursor_id == old.resource_id
            || after.cursor_id == next.resource_id
            || next.ring_epoch <= old.ring_epoch
            || next.feed_generation <= old.feed_generation
            || next.predecessor_ring_epoch != old.ring_epoch
            || next.first_seq != 1
            || next.cutover_seq != 0
            || next.created_mono_ns != 0
            || next.descriptor_hash != [0; 32]
        {
            return Err(Error::Identity);
        }
        let mut normalized = next;
        normalized.resource_id = old.resource_id;
        normalized.ring_epoch = old.ring_epoch;
        normalized.feed_generation = old.feed_generation;
        normalized.predecessor_ring_epoch = old.predecessor_ring_epoch;
        normalized.first_seq = old.first_seq;
        normalized.cutover_seq = old.cutover_seq;
        normalized.created_mono_ns = old.created_mono_ns;
        normalized.descriptor_hash = old.descriptor_hash;
        if wire_bytes(&normalized)? != before.bank_header {
            return Err(Error::Identity);
        }
        let _ = encoded(self)?;
        Ok(())
    }
    pub fn validate_client(&self, client: &Client, now: MonoTime) -> Result<()> {
        self.validate(&self.previous, now)?;
        matches_client(&self.previous, client)?;
        if now.nanos() < client.baseline_now || now.nanos() >= client.session.deadline() {
            return Err(Error::Deadline);
        }
        Ok(())
    }
    /// The caller authenticates the receipt producer. Activated header/cursor facts
    /// must additionally be compared with the actual successor by the MD owner.
    pub fn validate_receipt(
        &self,
        receipt: &Receipt,
        pid: u32,
        phase: Phase,
        now: MonoTime,
    ) -> Result<()> {
        self.validate(&self.previous, now)?;
        if receipt.schema != 1
            || receipt.operation_id != self.operation_id
            || receipt.approval_digest != self.approval_digest
            || receipt.approval_hash != self.digest()?
            || receipt.revision != self.revision
            || receipt.phase != phase
            || pid == 0
            || receipt.pid != pid
            || receipt.observed_mono_ns > now.nanos()
            || receipt.observed_mono_ns
                < ResourceHeader::decode(&self.previous.bank_header)?.created_mono_ns
        {
            return Err(Error::Identity);
        }
        match phase {
            Phase::Armed
                if receipt.previous_applied.is_none()
                    && receipt.next_descriptor_hash.is_none()
                    && receipt.activated_sequence.is_none() =>
            {
                Ok(())
            }
            Phase::Quiesced
                if receipt
                    .previous_applied
                    .is_some_and(|v| valid_previous(self, v))
                    && receipt.next_descriptor_hash.is_none()
                    && receipt.activated_sequence.is_none() =>
            {
                Ok(())
            }
            Phase::Activated
                if receipt
                    .previous_applied
                    .is_some_and(|v| valid_previous(self, v))
                    && receipt.next_descriptor_hash.is_some_and(|v| v != [0; 32])
                    && receipt.activated_sequence.is_some_and(|v| {
                        v < 1 << 63 && v >= receipt.previous_applied.unwrap_or(u64::MAX)
                    }) =>
            {
                Ok(())
            }
            _ => Err(Error::Identity),
        }
    }
}
fn validate_members(config: &ConnectConfig) -> Result<()> {
    if config.instruments.is_empty()
        || config.instruments.len() > 4096
        || config.instruments.windows(2).any(|w| w[0].id >= w[1].id)
        || config.instruments.iter().any(|i| {
            i.id == 0
                || i.version == 0
                || i.version >= 1 << 63
                || i.required_states == 0
                || i.required_states & !7 != 0
        })
    {
        return Err(Error::Identity);
    }
    Ok(())
}
fn verify_hash(bank: &ResourceHeader) -> Result<()> {
    let mut zero = bank.clone();
    zero.descriptor_hash = [0; 32];
    if bank.descriptor_hash != <[u8; 32]>::from(Sha256::digest(wire_bytes(&zero)?)) {
        return Err(Error::Identity);
    }
    Ok(())
}
fn valid_previous(approval: &Approval, value: u64) -> bool {
    ResourceHeader::decode(&approval.previous.bank_header)
        .is_ok_and(|b| value >= b.first_seq.saturating_sub(1) && value < 1 << 63)
}
fn matches_client(config: &ConnectConfig, client: &Client) -> Result<()> {
    let actual = client.connection.peer_credentials();
    let j = &client.join;
    let mut approved = ResourceHeader::decode(&config.bank_header)?;
    if approved.descriptor_hash == [0; 32] {
        approved.descriptor_hash = client.bank.descriptor_hash;
    }
    if config.domain()? != client.domain
        || approved != client.bank
        || wire_bytes(&client.bank)? != client.baseline.bank_header
        || client.session.peer() != &config.endpoint.peer.identity()
        || actual.pid != config.endpoint.peer_pid
        || actual.uid != config.endpoint.peer_uid
        || actual.gid != config.endpoint.peer_gid
        || j.subscriber_instance != config.endpoint.local.instance
        || j.logical_strategy_id != config.endpoint.local.logical_strategy
        || j.subscription_generation != config.subscription_generation
        || j.cursor_resource_id != config.cursor_id
        || j.bank_resource_id != client.bank.resource_id
        || j.topic_instrument_set_version != config.instrument_set_version
        || client.baseline.catalog_hash != config.catalog_hash
        || client.baseline.instrument_set_version != config.instrument_set_version
        || encoded(&client.baseline.instruments)? != encoded(&config.instruments)?
        || client.consumer.watermarks().applied != client.replica.sequence()
    {
        return Err(Error::Identity);
    }
    Ok(())
}
impl Commit {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode(bytes)
    }
    pub fn validate(
        &self,
        approval: &Approval,
        previous_applied: u64,
        now: MonoTime,
    ) -> Result<ResourceHeader> {
        approval.validate(&approval.previous, now)?;
        let bank = ResourceHeader::decode(&self.config.bank_header)?;
        tickwire_shm::validate_bank(&bank)?;
        verify_hash(&bank)?;
        if self.schema != 1
            || self.operation_id != approval.operation_id
            || self.approval_digest != approval.approval_digest
            || self.revision != approval.revision
            || !valid_previous(approval, previous_applied)
            || bank.cutover_seq < previous_applied
            || Some(bank.first_seq) != bank.cutover_seq.checked_add(1)
            || bank.created_mono_ns > now.nanos()
            || bank.created_mono_ns
                < ResourceHeader::decode(&approval.previous.bank_header)?.created_mono_ns
        {
            return Err(Error::Identity);
        }
        let mut normalized = bank.clone();
        normalized.created_mono_ns = 0;
        normalized.cutover_seq = 0;
        normalized.first_seq = 1;
        normalized.descriptor_hash = [0; 32];
        let mut config = self.config.clone();
        config.bank_header = wire_bytes(&normalized)?;
        if encoded(&config)? != encoded(&approval.next_template)? {
            return Err(Error::Identity);
        }
        let _ = encoded(self)?;
        Ok(bank)
    }
}
impl Receipt {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode(bytes)
    }
    pub fn armed(approval: &Approval, pid: u32, now: MonoTime) -> Result<Self> {
        let out = Self {
            schema: 1,
            operation_id: approval.operation_id,
            approval_digest: approval.approval_digest,
            approval_hash: approval.digest()?,
            revision: approval.revision,
            phase: Phase::Armed,
            pid,
            observed_mono_ns: now.nanos(),
            previous_applied: None,
            next_descriptor_hash: None,
            activated_sequence: None,
        };
        approval.validate_receipt(&out, pid, Phase::Armed, now)?;
        Ok(out)
    }
    pub fn quiesced(
        approval: &Approval,
        pid: u32,
        previous_applied: u64,
        now: MonoTime,
    ) -> Result<Self> {
        let mut out = Self::armed(approval, pid, now)?;
        out.phase = Phase::Quiesced;
        out.previous_applied = Some(previous_applied);
        approval.validate_receipt(&out, pid, Phase::Quiesced, now)?;
        Ok(out)
    }
    pub fn activated(
        approval: &Approval,
        pid: u32,
        previous_applied: u64,
        client: &Client,
        now: MonoTime,
    ) -> Result<Self> {
        let mut config = approval.next_template.clone();
        config.bank_header = wire_bytes(&client.bank)?;
        Commit {
            schema: 1,
            operation_id: approval.operation_id,
            approval_digest: approval.approval_digest,
            revision: approval.revision,
            config: config.clone(),
        }
        .validate(approval, previous_applied, now)?;
        matches_client(&config, client)?;
        if now.nanos() < client.baseline_now || now.nanos() >= client.session.deadline() {
            return Err(Error::Deadline);
        }
        let mut out = Self::quiesced(approval, pid, previous_applied, now)?;
        out.phase = Phase::Activated;
        out.next_descriptor_hash = Some(client.bank.descriptor_hash);
        out.activated_sequence = Some(client.consumer.watermarks().applied);
        approval.validate_receipt(&out, pid, Phase::Activated, now)?;
        Ok(out)
    }
}
#[cfg(test)]
mod tests;
