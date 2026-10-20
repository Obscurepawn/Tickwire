//! Trusted launcher bootstrap plus shared authenticated market JOIN identities.
use crate::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tickwire_ipc::{ResourceHeader, Wire};
pub use tickwire_market_source::protocol::{
    Endpoint, IdentityConfig, Instrument, JoinBootstrap, wire_bytes,
};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub version: u16,
    pub market: Endpoint,
    /// Direct authenticated MD topic. A topic may contain many instruments.
    #[serde(default)]
    pub external_market: Option<tickwire_market_source::ConnectConfig>,
    /// Launcher writes this only after registering the actual spawned child at MD.
    #[serde(default)]
    pub market_admission_ready: Option<PathBuf>,
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
    /// Produce synchronized actual handover evidence after account callbacks.
    #[serde(default)]
    pub handover_enabled: bool,
    /// Private loader-written job. If present, normal un-restored startup is rejected.
    #[serde(default)]
    pub preflight_job: Option<PathBuf>,
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
