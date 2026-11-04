//! Explicit loopback-only deployment laboratory; never loaded by production defaults.
#![forbid(unsafe_code)]
#![recursion_limit = "512"]
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub mod fixture;
pub mod generate;
pub mod launch;
mod model;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf};
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub schema: u32,
    pub fixture_only: bool,
    pub root: PathBuf,
    pub bin_directory: PathBuf,
    pub listen: SocketAddr,
    pub valid_until_ns: u64,
    pub boot: [u8; 16],
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub api_key: PathBuf,
    pub api_secret: PathBuf,
    pub md_config: PathBuf,
    pub app_config: PathBuf,
    pub worker_bootstrap: PathBuf,
}
impl Scenario {
    pub fn read(path: &std::path::Path) -> Result<Self> {
        let bytes = std::fs::read(path)?;
        if bytes.len() > 65536 {
            return Err("scenario size".into());
        }
        let s: Self = serde_json::from_slice(&bytes)?;
        if s.schema != 1
            || !s.fixture_only
            || !s.listen.ip().is_loopback()
            || !s.root.is_absolute()
            || !s.bin_directory.is_absolute()
        {
            return Err("not a loopback fixture scenario".into());
        }
        Ok(s)
    }
}
