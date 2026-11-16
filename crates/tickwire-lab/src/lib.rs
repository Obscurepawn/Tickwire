//! Explicit loopback-only 3-process stateless Linux laboratory (`tickwire-md` + `tickwire-app` +
//! `tickwire-strategy-worker`).
//!
//! # Architecture & Safety Fencing
//!
//! - **Loopback-Only Fencing**: [`Scenario::read`] rejects any scenario where `fixture_only` is
//!   `false`, `listen.ip()` is non-loopback, or paths are non-absolute. Production configurations
//!   never reference `tickwire-lab`.
//! - **Pure Stateless 3-Process Verification**: Spawns `tickwire-md` (sealed `memfd` market
//!   broadcast ring), `tickwire-app` (unified stateless ORS cold-bootstrapping from authenticated
//!   Exchange REST `Account + OpenOrders` and `tw1...` `SelfDescribingClientId` tokens while
//!   signing Spot WS API v3 orders with Ed25519), and `tickwire-strategy-worker` (`DOP`
//!   dual-seqnum login plus inline `1,024B` `IntentRecord` and `512B` `ReportRecord` SPSC SHM
//!   rings) with zero local write-ahead logs, zero journals, and zero disk blob files.

#![forbid(unsafe_code)]
#![recursion_limit = "512"]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub mod fixture;
pub mod generate;
pub mod launch;
pub mod model;

/// Self-contained single-use loopback laboratory scenario descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub schema: u32,
    pub fixture_only: bool,
    pub root: PathBuf,
    pub bin_directory: PathBuf,
    pub listen: SocketAddr,
    pub valid_until_ns: u64,
    pub boot: [u8; 16],
    pub risk_domain_id: [u8; 16],
    pub exchange_uid: u64,
    pub strat_id: u8,
    pub cold_open_order_seqnum: u64,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub api_key: PathBuf,
    pub api_secret: PathBuf,
    pub ed25519_public_key: PathBuf,
    pub md_config: PathBuf,
    pub app_config: PathBuf,
    pub worker_bootstrap: PathBuf,
}

impl Scenario {
    /// Reads and validates a single-use loopback [`Scenario`] JSON file (`<= 64 KiB`).
    pub fn read(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)?;
        if bytes.len() > 65_536 {
            return Err("scenario exceeds 64 KiB limit".into());
        }
        let s: Self = serde_json::from_slice(&bytes)?;
        s.validate()?;
        Ok(s)
    }

    /// Enforces loopback-only fencing and absolute directory invariants.
    pub fn validate(&self) -> Result<()> {
        if self.schema != 2
            || !self.fixture_only
            || !self.listen.ip().is_loopback()
            || !self.root.is_absolute()
            || !self.bin_directory.is_absolute()
            || self.strat_id == 0
            || self.exchange_uid == 0
        {
            return Err("invalid or non-loopback laboratory scenario".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn scenario_fencing_rejects_non_loopback_and_relative_paths() {
        let valid = Scenario {
            schema: 2,
            fixture_only: true,
            root: PathBuf::from("/tmp/tw-lab"),
            bin_directory: PathBuf::from("/tmp/tw-bin"),
            listen: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18443),
            valid_until_ns: 100_000,
            boot: [1; 16],
            risk_domain_id: [2; 16],
            exchange_uid: 9001,
            strat_id: 1,
            cold_open_order_seqnum: 3,
            certificate: PathBuf::from("/tmp/tw-lab/tls/fixture.der"),
            private_key: PathBuf::from("/tmp/tw-lab/tls/fixture.pk8"),
            api_key: PathBuf::from("/tmp/tw-lab/keys/api-key"),
            api_secret: PathBuf::from("/tmp/tw-lab/keys/api-secret"),
            ed25519_public_key: PathBuf::from("/tmp/tw-lab/keys/ed25519.pub"),
            md_config: PathBuf::from("/tmp/tw-lab/md/config.json"),
            app_config: PathBuf::from("/tmp/tw-lab/ors/config.json"),
            worker_bootstrap: PathBuf::from("/tmp/tw-lab/strategy/bootstrap.json"),
        };
        assert!(valid.validate().is_ok());

        let mut public_ip = valid.clone();
        public_ip.listen = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 443);
        assert!(public_ip.validate().is_err());

        let mut relative_root = valid.clone();
        relative_root.root = PathBuf::from("relative/lab");
        assert!(relative_root.validate().is_err());

        let mut non_fixture = valid;
        non_fixture.fixture_only = false;
        assert!(non_fixture.validate().is_err());
    }
}
