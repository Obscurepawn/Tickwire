//! Fixed, launcher-approved economic directories. No authority is inferred from a file.
use crate::{
    Result,
    config::{self, CatalogFile, Config},
    invalid,
};
use std::{collections::BTreeSet, path::Component};
use tickwire_instrument_catalog::{Catalog, Market};
use tickwire_venue::{Environment, Product};

const MAX_FILES: usize = 32;
const MAX_BYTES: usize = 2 * 1024 * 1024;
pub(crate) struct Catalogs {
    approvals: Option<Vec<CatalogFile>>,
    documents: Vec<Catalog>,
}
impl Catalogs {
    pub(crate) fn load(config: &Config) -> Result<Self> {
        let approved = approvals(config)?;
        let mut remaining = MAX_BYTES;
        let mut objects = Vec::with_capacity(approved.len());
        for file in approved {
            let raw = config::read(&file.path, remaining)?;
            remaining = remaining
                .checked_sub(raw.len())
                .ok_or_else(|| invalid("MD catalog total capacity"))?;
            objects.push(raw);
        }
        Self::from_objects(config, &objects)
    }
    /// Restore only the exact retained bytes. A current pathname is not a recovery dependency.
    pub(crate) fn from_objects(config: &Config, objects: &[Vec<u8>]) -> Result<Self> {
        let approved = approvals(config)?;
        if approved.len() != objects.len()
            || objects
                .iter()
                .try_fold(0usize, |n, b| n.checked_add(b.len()))
                .is_none_or(|n| n > MAX_BYTES)
        {
            return Err(invalid("MD catalog retained dependency bounds"));
        }
        let mut documents = Vec::with_capacity(objects.len());
        let mut identities = BTreeSet::new();
        for (file, bytes) in approved.iter().zip(objects) {
            let expected = tickwire_config::artifacts::reference_hash(&file.reference)
                .map_err(|e| invalid(&format!("MD catalog reference {e:?}")))?;
            let catalog = Catalog::decode(bytes, expected)
                .map_err(|e| invalid(&format!("MD catalog body {e:?}")))?;
            if !identities.insert(catalog.catalog_hash()) {
                return Err(invalid("MD catalog duplicate economic identity"));
            }
            documents.push(catalog);
        }
        let value = Self {
            approvals: config.catalogs.clone(),
            documents,
        };
        value.validate(config)?;
        Ok(value)
    }
    pub(crate) fn validate(&self, config: &Config) -> Result<()> {
        if self.approvals != config.catalogs {
            return Err(invalid("MD catalog approval changed"));
        }
        if self.approvals.is_none() {
            return Ok(());
        }
        for feed in &config.feeds {
            use tickwire_binance_public::Source;
            let (market, environment) = match feed.source.build() {
                Source::SpotProduction => (Market::Spot, Environment::Production),
                Source::SpotTest => (Market::Spot, Environment::Test),
                Source::UsdmProduction => (Market::Usdm, Environment::Production),
                Source::UsdmTest => (Market::Usdm, Environment::Test),
            };
            let product = if market == Market::Spot {
                Product::Spot
            } else if feed.instrument.expiry_utc_ns.is_some() {
                Product::LinearFuture
            } else {
                Product::LinearPerpetual
            };
            let binding = feed.instrument.build(feed.source.build())?;
            if self
                .documents
                .iter()
                .filter(|catalog| {
                    catalog.venue() == feed.venue_id
                        && catalog.market() == market
                        && catalog.environment() == environment
                        && catalog.instrument_matches(product, &binding)
                })
                .count()
                != 1
            {
                return Err(invalid("MD catalog feed economic binding"));
            }
        }
        Ok(())
    }
    pub(crate) fn objects(&self) -> Vec<Vec<u8>> {
        self.documents
            .iter()
            .map(|c| c.raw_bytes().to_vec())
            .collect()
    }
}
fn approvals(config: &Config) -> Result<&[CatalogFile]> {
    let Some(files) = &config.catalogs else {
        return Ok(&[]);
    };
    if files.is_empty() || files.len() > MAX_FILES {
        return Err(invalid("MD catalog approval count"));
    }
    let mut references = BTreeSet::new();
    let mut paths = BTreeSet::new();
    for file in files {
        tickwire_config::artifacts::reference_hash(&file.reference)
            .map_err(|e| invalid(&format!("MD catalog reference {e:?}")))?;
        if !file.path.is_absolute()
            || file.path.as_os_str().len() > 4096
            || file.path.file_name().is_none()
            || file
                .path
                .components()
                .any(|c| !matches!(c, Component::RootDir | Component::Normal(_)))
            || !references.insert(&file.reference)
            || !paths.insert(&file.path)
        {
            return Err(invalid("MD catalog approval path/duplicate"));
        }
    }
    Ok(files)
}
#[cfg(all(test, target_os = "linux"))]
pub(crate) mod fixture;
#[cfg(all(test, target_os = "linux"))]
mod tests;
