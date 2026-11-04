//! Bounded economic catalog content, independent from artifact and account identity.
#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use tickwire_venue::{AssetRef, Environment, InstrumentBinding, Product};

mod format;

pub type Hash = [u8; 32];
pub type Result<T> = std::result::Result<T, Error>;
pub const MAX_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_DEPTH: usize = 16;
pub const MAX_ASSETS: usize = 4096;
pub const MAX_INSTRUMENTS: usize = 16384;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Bounds,
    Digest,
    Encoding,
    Identity,
    Duplicate,
    Economics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub enum Market {
    Spot,
    Usdm,
}
impl TryFrom<String> for Market {
    type Error = &'static str;
    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        match value.as_str() {
            "Spot" => Ok(Self::Spot),
            "Usdm" => Ok(Self::Usdm),
            _ => Err("unknown market"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub digest: Hash,
    pub bytes: Vec<u8>,
}
impl Artifact {
    pub fn catalog(&self) -> Result<Catalog> {
        Catalog::decode(&self.bytes, self.digest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instrument {
    product: Product,
    binding: InstrumentBinding,
}
impl Instrument {
    pub fn product(&self) -> Product {
        self.product
    }
    pub fn binding(&self) -> &InstrumentBinding {
        &self.binding
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    raw: Vec<u8>,
    artifact_hash: Hash,
    canonical: Vec<u8>,
    catalog_hash: Hash,
    version: u64,
    venue: u32,
    environment: Environment,
    market: Market,
    assets: Vec<AssetRef>,
    instruments: Vec<Instrument>,
}
impl Catalog {
    pub fn decode(raw: &[u8], expected_artifact_sha256: Hash) -> Result<Self> {
        format::decode(raw, expected_artifact_sha256)
    }
    pub fn raw_bytes(&self) -> &[u8] {
        &self.raw
    }
    pub fn artifact_hash(&self) -> Hash {
        self.artifact_hash
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }
    pub fn catalog_hash(&self) -> Hash {
        self.catalog_hash
    }
    pub fn catalog_version(&self) -> u64 {
        self.version
    }
    pub fn venue(&self) -> u32 {
        self.venue
    }
    pub fn environment(&self) -> Environment {
        self.environment
    }
    pub fn market(&self) -> Market {
        self.market
    }
    pub fn assets(&self) -> &[AssetRef] {
        &self.assets
    }
    pub fn instruments(&self) -> &[Instrument] {
        &self.instruments
    }
    pub fn asset(&self, id: u64) -> Option<&AssetRef> {
        self.assets
            .binary_search_by_key(&id, |a| a.id)
            .ok()
            .map(|i| &self.assets[i])
    }
    pub fn instrument(&self, id: u64) -> Option<&Instrument> {
        self.instruments
            .binary_search_by_key(&id, |i| i.binding.instrument_id)
            .ok()
            .map(|i| &self.instruments[i])
    }
    pub fn asset_matches(&self, asset: &AssetRef) -> bool {
        self.asset(asset.id) == Some(asset)
    }
    pub fn instrument_matches(&self, product: Product, binding: &InstrumentBinding) -> bool {
        self.instrument(binding.instrument_id)
            .is_some_and(|i| i.product == product && i.binding == *binding)
    }
    pub fn artifact(&self) -> Artifact {
        Artifact {
            digest: self.artifact_hash,
            bytes: self.raw.clone(),
        }
    }
}
