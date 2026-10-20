//! Trusted launcher data; files are written atomically before the corresponding offer.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tickwire_control::{Identity, Policy, ProcessIdentity};
use tickwire_ipc::{JoinBody, ResourceHeader, Wire};
use tickwire_join::TopicRegistry;
use tickwire_snapshot::InstrumentSpec;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityConfig {
    pub instance: [u8; 16],
    pub generation: u64,
    pub role: u16,
    pub logical_strategy: u64,
    pub host_boot: [u8; 16],
    pub namespace: [u8; 16],
    pub build_hash: [u8; 32],
    pub qualification_hash: [u8; 32],
    pub manifest_hash: [u8; 32],
    pub arch: u16,
    pub trust_profile: u8,
}
impl From<&Identity> for IdentityConfig {
    fn from(v: &Identity) -> Self {
        Self {
            instance: v.instance,
            generation: v.generation,
            role: v.role,
            logical_strategy: v.logical_strategy,
            host_boot: v.host_boot,
            namespace: v.namespace,
            build_hash: v.build_hash,
            qualification_hash: v.qualification_hash,
            manifest_hash: v.manifest_hash,
            arch: v.arch,
            trust_profile: v.trust_profile,
        }
    }
}
impl IdentityConfig {
    pub fn identity(&self) -> Identity {
        Identity {
            instance: self.instance,
            generation: self.generation,
            role: self.role,
            logical_strategy: self.logical_strategy,
            host_boot: self.host_boot,
            namespace: self.namespace,
            build_hash: self.build_hash,
            qualification_hash: self.qualification_hash,
            manifest_hash: self.manifest_hash,
            arch: self.arch,
            trust_profile: self.trust_profile,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub path: PathBuf,
    pub local: IdentityConfig,
    pub peer: IdentityConfig,
    pub peer_pid: u32,
    pub peer_uid: u32,
    pub peer_gid: u32,
    pub required_features: u64,
    pub supported_features: u64,
    pub control_epoch: u64,
    pub lease_duration_ns: u64,
    pub max_frame_bytes: u32,
    pub nonce: [u8; 32],
}
impl Endpoint {
    pub fn new(path: PathBuf, p: &Policy) -> Self {
        Self {
            path,
            local: (&p.local).into(),
            peer: (&p.peer).into(),
            peer_pid: p.peer_process.pid,
            peer_uid: p.peer_process.uid,
            peer_gid: p.peer_process.gid,
            required_features: p.required_features,
            supported_features: p.supported_features,
            control_epoch: p.control_epoch,
            lease_duration_ns: p.lease_duration_ns,
            max_frame_bytes: p.max_frame_bytes,
            nonce: p.nonce,
        }
    }
    pub fn policy(&self) -> Policy {
        Policy {
            local: self.local.identity(),
            peer: self.peer.identity(),
            peer_process: ProcessIdentity {
                pid: self.peer_pid,
                uid: self.peer_uid,
                gid: self.peer_gid,
            },
            required_features: self.required_features,
            supported_features: self.supported_features,
            control_epoch: self.control_epoch,
            lease_duration_ns: self.lease_duration_ns,
            max_frame_bytes: self.max_frame_bytes,
            nonce: self.nonce,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub version: u16,
    pub market: Endpoint,
    pub intent: Endpoint,
    /// Independent execution-owned ordered account report channel.
    pub report: Endpoint,
    pub report_header: Vec<u8>,
    pub report_cursor_id: [u8; 16],
    pub report_subscription_generation: u64,
    /// Execution owns the writer; worker opens only a FileBlobReader.
    pub report_directory: PathBuf,
    /// Written after market HELLO, before market RESOURCE_OFFER. Contains exact JOIN identities.
    pub join_file: PathBuf,
    /// Wire-encoded ResourceHeader for the strategy-owned 1024-byte SPSC bank.
    pub intent_header: Vec<u8>,
    pub intent_cursor_id: [u8; 16],
    pub intent_subscription_generation: u64,
    pub risk_domain: [u8; 16],
    pub command_directory: PathBuf,
    pub clock_generation: u64,
    pub startup_timeout_ms: u64,
    pub maximum_market_age_ns: u64,
    pub maximum_levels: usize,
    pub poll_records: usize,
    pub idle_sleep_us: u64,
    /// Default false; a supplied canonical order is emitted at most once.
    #[serde(default)]
    pub recovery_template: Option<PathBuf>,
    pub demo_enabled: bool,
    pub demo_command_file: Option<PathBuf>,
    pub demo_minimum_bid_ticks: i64,
}
impl Bootstrap {
    pub fn report_resource(&self) -> Result<ResourceHeader> {
        Ok(ResourceHeader::decode(&self.report_header)?)
    }
    pub fn intent_resource(&self) -> Result<ResourceHeader> {
        Ok(ResourceHeader::decode(&self.intent_header)?)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Instrument {
    pub id: u64,
    pub version: u64,
    pub required_states: u8,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinBootstrap {
    pub bank_header: Vec<u8>,
    pub snapshot_header: Vec<u8>,
    pub join: Vec<u8>,
    pub catalog_hash: [u8; 32],
    pub instrument_set_version: u64,
    pub instruments: Vec<Instrument>,
}
pub fn wire_bytes<T: Wire>(v: &T) -> Result<Vec<u8>> {
    let mut out = vec![0; T::SIZE];
    v.encode(&mut out)?;
    Ok(out)
}
impl JoinBootstrap {
    pub fn new(
        bank: &ResourceHeader,
        snapshot: &ResourceHeader,
        join: &JoinBody,
        registry: &TopicRegistry,
    ) -> Result<Self> {
        Ok(Self {
            bank_header: wire_bytes(bank)?,
            snapshot_header: wire_bytes(snapshot)?,
            join: wire_bytes(join)?,
            catalog_hash: registry.catalog_hash,
            instrument_set_version: registry.instrument_set_version,
            instruments: registry
                .instruments
                .iter()
                .map(|i| Instrument {
                    id: i.instrument_id,
                    version: i.instrument_version,
                    required_states: i.required_states,
                })
                .collect(),
        })
    }
    pub fn decode(&self) -> Result<(ResourceHeader, ResourceHeader, JoinBody, TopicRegistry)> {
        if self.instruments.is_empty() || self.instruments.len() > 4096 {
            return Err(Error::Bounds);
        }
        Ok((
            ResourceHeader::decode(&self.bank_header)?,
            ResourceHeader::decode(&self.snapshot_header)?,
            JoinBody::decode(&self.join)?,
            TopicRegistry {
                catalog_hash: self.catalog_hash,
                instrument_set_version: self.instrument_set_version,
                instruments: self
                    .instruments
                    .iter()
                    .map(|i| InstrumentSpec {
                        instrument_id: i.id,
                        instrument_version: i.version,
                        required_states: i.required_states,
                    })
                    .collect(),
            },
        ))
    }
}
