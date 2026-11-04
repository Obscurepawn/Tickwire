use super::Result;
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};
use tickwire_account_codec::{CanonicalBaseline, MAX_BASELINE_BYTES};
use tickwire_app::strategy::{AppStrategy, Config as StrategyConfig, FileStrategy, MultiStrategy};
use tickwire_market_source::ConnectConfig;
use tickwire_recovery::{BlobStore, StoreKey, digest};
use tickwire_runtime_policy::{ControlPlane, bootstrap};
use tickwire_spot_execution::SpotVenue;
use tickwire_venue::Product;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Deployment {
    pub state_directory: PathBuf,
    pub baseline: PathBuf,
    pub expected_uid: String,
    pub baseline_utc_ms: u64,
    pub account_stream: [u8; 16],
    pub private_stream: [u8; 16],
    pub initial_writer_epoch: u64,
    pub policy: bootstrap::Config,
    pub market_sources: Vec<ConnectConfig>,
    pub strategies: Vec<Queue>,
    pub stop_file: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Queue {
    logical_strategy_id: u64,
    directory: PathBuf,
    first_sequence: u64,
}

impl Deployment {
    pub fn read(path: &Path) -> Result<Self> {
        let bytes = tickwire_app::config::read_file(path, 4 * 1024 * 1024)?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub fn validate(&self, trading: bool) -> Result<(CanonicalBaseline, SpotVenue)> {
        if !self.state_directory.is_absolute()
            || !self.baseline.is_absolute()
            || !self.stop_file.is_absolute()
            || self.stop_file.try_exists()?
            || !matches!(self.expected_uid.len(), 1..=32)
            || !self.expected_uid.bytes().all(|b| b.is_ascii_digit())
            || self.baseline_utc_ms == 0
            || self.account_stream == [0; 16]
            || self.private_stream == [0; 16]
            || self.account_stream == self.private_stream
            || self.initial_writer_epoch == 0
            || self.initial_writer_epoch > self.policy.writer_epoch
            || (trading && self.market_sources.is_empty())
            || self.market_sources.len() > 32
            || (trading && self.strategies.is_empty())
            || self.strategies.len() > 32
        {
            return Err("invalid live spot deployment identity, paths or capacity".into());
        }
        let base = CanonicalBaseline::decode(&tickwire_app::config::read_file(
            &self.baseline,
            MAX_BASELINE_BYTES,
        )?)
        .map_err(super::protocol_error)?;
        let scope = &base.snapshot().scope;
        let venue = SpotVenue::from_id(scope.venue).ok_or("expected a Bybit or OKX account")?;
        if self.policy.scope() != *scope
            || self.policy.product() != Product::Spot
            || base.catalog().entries.is_empty()
            || base.catalog().entries.iter().any(|e| {
                e.product != Product::Spot
                    || e.instrument.catalog_version != self.policy.versions.catalog_version
                    || e.instrument.catalog_hash != self.policy.versions.catalog_hash
            })
            || self
                .market_sources
                .iter()
                .any(|s| s.catalog_hash != self.policy.versions.catalog_hash)
        {
            return Err("live spot deployment does not match the account scope or catalog".into());
        }
        Ok((base, venue))
    }

    pub fn strategies(&self, plane: ControlPlane) -> Result<MultiStrategy> {
        let mut directories = BTreeSet::new();
        let mut members: Vec<(u64, Box<dyn AppStrategy>)> = Vec::new();
        for q in &self.strategies {
            if !q.directory.is_absolute() || !directories.insert(fs::canonicalize(&q.directory)?) {
                return Err("each strategy needs its own absolute queue directory".into());
            }
            let strategy = FileStrategy::new(&StrategyConfig::FileQueue {
                directory: q.directory.clone(),
                first_sequence: q.first_sequence,
                enabled: true,
            })?;
            members.push((q.logical_strategy_id, Box::new(strategy)));
        }
        MultiStrategy::new(members, plane)
    }

    // The baseline timestamp and UID are operator-supplied genesis facts. A restart
    // may change grants and queues, but cannot silently move the history cut.
    pub fn bind(
        &self,
        store: &mut impl BlobStore,
        baseline: &CanonicalBaseline,
        existing: bool,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(&(
            self.expected_uid.as_str(),
            self.baseline_utc_ms,
            self.account_stream,
            self.private_stream,
            self.initial_writer_epoch,
            baseline.digest(),
        ))?;
        let key = StoreKey::Binding(digest(b"tickwire.spot.account.genesis.v1"));
        let identity = digest(&bytes);
        if existing && store.get(key, 32)? != identity {
            return Err("live account genesis differs from stored identity".into());
        }
        if !existing {
            match store.get(initialized(), 32) {
                Ok(_) => {
                    return Err(
                        "account journal is missing from an initialized state directory".into(),
                    );
                }
                Err(tickwire_recovery::Error::Missing) => {}
                Err(error) => return Err(error.into()),
            }
        }
        store.put_sync(key, &identity)?;
        Ok(())
    }
}

pub(super) fn initialized() -> StoreKey {
    StoreKey::Binding(digest(b"tickwire.spot.account.initialized.v1"))
}
