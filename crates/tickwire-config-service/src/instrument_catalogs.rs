//! Complete economic directory contents; these facts confer no deployment authority.
use crate::{Error, Hash, Result};
use std::collections::BTreeMap;
use tickwire_config::{
    Candidate,
    artifacts::{ArtifactCatalog, reference_hash},
};
use tickwire_instrument_catalog::{Catalog, Market};
use tickwire_venue::Environment;

pub struct VenueCatalog {
    reference: String,
    catalog: Catalog,
}
impl VenueCatalog {
    pub fn reference(&self) -> &str {
        &self.reference
    }
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
}
pub struct Catalogs {
    candidate_hash: Hash,
    venues: BTreeMap<String, VenueCatalog>,
}
impl Catalogs {
    pub fn check(candidate: &Candidate, artifacts: &ArtifactCatalog) -> Result<Self> {
        candidate.validate_static().map_err(Error::Configuration)?;
        let mut venues = BTreeMap::new();
        for venue in candidate.value()["venues"]
            .as_array()
            .ok_or(Error::Protocol)?
        {
            let id = venue["id"].as_str().ok_or(Error::Protocol)?;
            let reference = venue["instrument_catalog_ref"]
                .as_str()
                .ok_or(Error::Protocol)?;
            let raw = artifacts.get(reference).ok_or(Error::NotFound)?;
            let catalog = Catalog::decode(
                raw,
                reference_hash(reference).map_err(Error::Configuration)?,
            )
            .map_err(|_| Error::Protocol)?;
            let market = match venue["product"].as_str() {
                Some("Spot") => Market::Spot,
                Some("Usdm") => Market::Usdm,
                _ => return Err(Error::Identity),
            };
            let environment = match venue["environment"].as_str() {
                Some("production") => Environment::Production,
                Some("simulated") => Environment::Test,
                _ => return Err(Error::Identity),
            };
            if venue["venue"] != "Binance"
                || catalog.market() != market
                || catalog.environment() != environment
            {
                return Err(Error::Identity);
            }
            if venues
                .insert(
                    id.to_owned(),
                    VenueCatalog {
                        reference: reference.to_owned(),
                        catalog,
                    },
                )
                .is_some()
            {
                return Err(Error::Identity);
            }
        }
        Ok(Self {
            candidate_hash: candidate.hash(),
            venues,
        })
    }
    pub fn candidate_hash(&self) -> Hash {
        self.candidate_hash
    }
    pub fn venues(&self) -> &BTreeMap<String, VenueCatalog> {
        &self.venues
    }
}
