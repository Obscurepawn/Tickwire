//! Bounded durable MD history. Persisted facts grant no launch or JOIN authority.
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
pub type Hash = [u8; 32];
pub const MAX_SNAPSHOTS: usize = 1024;
pub const MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_TOTAL_BYTES: usize = 512 * 1024 * 1024;
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub directory: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_from: Option<Hash>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Phase {
    Starting,
    Ready,
    Reserved,
    Revoking,
    Applied,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Consumed {
    pub operation_id: [u8; 16],
    pub approval_digest: Hash,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub schema: u16,
    pub sequence: u64,
    pub previous: Hash,
    pub phase: Phase,
    pub revision: u64,
    pub config_hash: Hash,
    pub config: crate::config::Config,
    pub instrument_ids: Vec<u64>,
    pub banks: Vec<Vec<u8>>,
    pub operations: Vec<Consumed>,
    pub reserved_owners: Vec<[u8; 16]>,
}
mod store;
pub use store::Store;
#[cfg(all(test, target_os = "linux"))]
mod tests;
