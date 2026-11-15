//! Static launcher bootstrap schema (`Bootstrap`) for stateless strategy workers, including
//! optional `worker_cpu` and `housekeeping_cpus` CPU affinity isolation.
#![forbid(unsafe_code)]

use std::path::PathBuf;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use tickwire_control_sys::{CpuSet, validate_cpu_isolation};
use tickwire_ipc::{ALL_STRATS, ResourceHeader, Wire};

use crate::{Error, Result};

/// Maximum permitted `Bootstrap` JSON size (`512 KiB`).
pub const MAX_BOOTSTRAP_BYTES: usize = 512 * 1024;

/// Maximum permitted strategy parameter or catalog JSON size (`2 MiB`).
pub const MAX_PARAM_BYTES: usize = 2 * 1024 * 1024;

pub use tickwire_market_source::protocol::{
    Endpoint, IdentityConfig, Instrument, JoinBootstrap, wire_bytes,
};

/// Cryptographic SHA-256 digest and exact byte length of an immutable startup artifact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactDigest {
    pub sha256: [u8; 32],
    pub size_bytes: u64,
}

impl ArtifactDigest {
    /// Compute the SHA-256 digest and byte size of `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        Self {
            sha256: Sha256::digest(bytes).into(),
            size_bytes: bytes.len() as u64,
        }
    }

    /// Validate non-empty size within `max_bytes`.
    pub fn validate(&self, max_bytes: usize) -> Result<()> {
        if self.size_bytes == 0 || self.size_bytes > max_bytes as u64 || self.sha256 == [0; 32] {
            return Err(Error::Identity);
        }
        Ok(())
    }

    /// Verify `bytes` against both `size_bytes`, `max_bytes`, and `sha256`.
    pub fn verify(&self, bytes: &[u8], max_bytes: usize) -> Result<()> {
        self.validate(max_bytes)?;
        if bytes.len() > max_bytes || bytes.len() as u64 != self.size_bytes {
            return Err(Error::Identity);
        }
        let actual: [u8; 32] = Sha256::digest(bytes).into();
        if actual != self.sha256 {
            return Err(Error::Identity);
        }
        Ok(())
    }
}

/// Static startup configuration for one stateless strategy worker process.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub version: u16,
    pub logical_strategy_id: u64,
    pub strat_id: u8,
    pub instance_id: [u8; 16],
    pub generation: u64,
    #[serde(default)]
    pub account_ref: u8,
    /// Optional dedicated CPU core for the hot market/intent/report polling loop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_cpu: Option<u32>,
    /// Optional CPU set strictly disjoint from `worker_cpu` for cold housekeeping tasks.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "opt_cpuset_serde"
    )]
    pub housekeeping_cpus: Option<CpuSet>,
    /// Direct authenticated `tickwire-md` SHM topic subscription configuration.
    pub market: tickwire_market_source::ConnectConfig,
    /// Optional ORS Unix `SOCK_SEQPACKET` DOP control socket path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dop_socket: Option<PathBuf>,
    /// Optional DOP login and session parameters (`strat_id`, `control_epoch`, `max_order_age_ns`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dop: Option<crate::dop::DopConfig>,
    /// Optional SHM control endpoint for the 1,024-byte outbound `IntentRecord` ring.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent: Option<Endpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent_header: Option<Vec<u8>>,
    #[serde(default)]
    pub intent_cursor_id: [u8; 16],
    #[serde(default)]
    pub intent_subscription_generation: u64,
    /// Optional SHM control endpoint for the 512-byte inbound `ReportRecord` ring.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<Endpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report_header: Option<Vec<u8>>,
    #[serde(default)]
    pub report_cursor_id: [u8; 16],
    #[serde(default)]
    pub report_subscription_generation: u64,
    #[serde(default)]
    pub risk_domain: [u8; 16],
    /// Optional inline instrument catalog artifact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument_catalog: Option<tickwire_instrument_catalog::Artifact>,
    /// Optional path to an immutable instrument catalog JSON file (`<= 2 MiB`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_file: Option<PathBuf>,
    /// Optional SHA-256 digest bound to `catalog_file`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_digest: Option<ArtifactDigest>,
    /// Optional statically registered user strategy factory binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub factory: Option<crate::registered::FactoryBinding>,
    pub clock_generation: u64,
    pub startup_timeout_ms: u64,
    pub maximum_market_age_ns: u64,
    pub maximum_levels: usize,
    pub poll_records: usize,
    pub idle_sleep_us: u64,
    #[serde(default)]
    pub demo_enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub demo_command_file: Option<PathBuf>,
    #[serde(default)]
    pub demo_minimum_bid_ticks: i64,
}

impl Bootstrap {
    /// Validate static worker identities, bounds, DOP coherence, and CPU affinity isolation.
    pub fn validate(&self) -> Result<()> {
        validate_cpu_isolation(
            &[("worker_cpu", self.worker_cpu)],
            self.housekeeping_cpus.as_ref(),
        )?;
        if self.version != 1
            || self.logical_strategy_id == 0
            || self.strat_id == ALL_STRATS
            || self.instance_id == [0; 16]
            || self.generation == 0
            || self.generation >= (1 << 63)
            || self.maximum_market_age_ns == 0
            || self.poll_records == 0
            || self.poll_records > 4096
            || self.startup_timeout_ms == 0
            || self.startup_timeout_ms > 60_000
            || self.idle_sleep_us > 1_000_000
            || self.market.maximum_levels != self.maximum_levels
            || self.market.endpoint.local.role != 3
            || self.market.endpoint.peer.role != 2
            || self.market.endpoint.local.instance != self.instance_id
            || self.market.endpoint.local.generation != self.generation
            || self.market.endpoint.local.logical_strategy != self.logical_strategy_id
        {
            return Err(Error::Identity);
        }
        if self.dop_socket.is_some() != self.dop.is_some() {
            return Err(Error::Identity);
        }
        if let Some(dop) = &self.dop {
            dop.validate()?;
            if dop.strat_id != self.strat_id || dop.local_instance != self.instance_id {
                return Err(Error::Identity);
            }
        }
        if self.intent.is_some() != self.intent_header.is_some()
            || self.report.is_some() != self.report_header.is_some()
        {
            return Err(Error::Identity);
        }
        if let Some(binding) = &self.factory {
            binding.validate()?;
        }
        Ok(())
    }

    pub fn intent_resource(&self) -> Result<Option<ResourceHeader>> {
        self.intent_header
            .as_deref()
            .map(|raw| Ok(ResourceHeader::decode(raw)?))
            .transpose()
    }

    pub fn report_resource(&self) -> Result<Option<ResourceHeader>> {
        self.report_header
            .as_deref()
            .map(|raw| Ok(ResourceHeader::decode(raw)?))
            .transpose()
    }
}

mod opt_cpuset_serde {
    use super::*;

    pub fn serialize<S>(
        value: &Option<CpuSet>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            None => serializer.serialize_none(),
            Some(set) => serializer.serialize_some(set.as_slice()),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<Option<CpuSet>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<Vec<u32>>::deserialize(deserializer)?;
        match opt {
            None => Ok(None),
            Some(cpus) => CpuSet::from_cpus(cpus)
                .map(Some)
                .map_err(serde::de::Error::custom),
        }
    }
}
