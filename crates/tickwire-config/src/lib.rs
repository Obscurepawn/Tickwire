//! Versioned configuration values. No process, file or trading authority.
#![forbid(unsafe_code)]

use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

pub mod artifacts;
pub mod capacity;
pub mod changes;
pub mod delivery;
pub mod locator;
pub mod materialized;
pub mod patch;
pub mod plan;
pub mod prepared;
pub mod projection;
pub mod projection_object;
mod strict;
pub mod validation;

pub type Hash = [u8; 32];
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Json,
    Toml,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Encoding,
    Capacity,
    Scalar,
    Duplicate,
    Depth,
    Schema,
    Revision,
    Semantic(&'static str),
}
pub type Result<T> = core::result::Result<T, Error>;
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_bytes: usize,
    pub max_depth: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_bytes: 16 * 1024 * 1024,
            max_depth: 32,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    value: Value,
    canonical: Vec<u8>,
    hash: Hash,
}
impl Candidate {
    pub fn parse(format: Format, bytes: &[u8], limits: Limits) -> Result<Self> {
        if limits.max_bytes == 0
            || limits.max_bytes > 16 * 1024 * 1024
            || bytes.len() > limits.max_bytes
        {
            return Err(Error::Capacity);
        }
        if limits.max_depth == 0 || limits.max_depth > 64 {
            return Err(Error::Depth);
        }
        let value = match format {
            Format::Json => strict::json(bytes, limits.max_depth)?,
            Format::Toml => strict::toml(bytes, limits.max_depth)?,
        };
        let canonical = strict::canonical(&value)?;
        if canonical.len() > limits.max_bytes {
            return Err(Error::Capacity);
        }
        let hash = Sha256::digest(&canonical).into();
        Ok(Self {
            value,
            canonical,
            hash,
        })
    }
    pub fn value(&self) -> &Value {
        &self.value
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }
    pub fn hash(&self) -> Hash {
        self.hash
    }
    /// Complete configuration semantics, separate from general strict parsing.
    pub fn normalized_configuration(&self, limits: Limits) -> Result<Self> {
        self.validate_structural()?;
        let mut value = self.value.clone();
        for binding in value["control"]["role_bindings"]
            .as_array_mut()
            .ok_or(Error::Schema)?
        {
            binding
                .as_object_mut()
                .ok_or(Error::Schema)?
                .entry("cluster_scope".to_owned())
                .or_insert(Value::Bool(false));
        }
        Self::parse(
            Format::Json,
            &serde_json::to_vec(&value).map_err(|_| Error::Encoding)?,
            limits,
        )
    }
    pub fn validate_structural(&self) -> Result<()> {
        static SCHEMA: OnceLock<core::result::Result<jsonschema::Validator, ()>> = OnceLock::new();
        let schema = SCHEMA
            .get_or_init(|| {
                let document: Value = serde_json::from_str(include_str!(
                    "../../../specs/contracts/system-config.schema.json"
                ))
                .map_err(|_| ())?;
                // This fixed schema has only local references. Network and
                // filesystem retrieval are disabled at the dependency boundary.
                jsonschema::options()
                    .with_draft(jsonschema::Draft::Draft202012)
                    .should_validate_formats(true)
                    .build(&document)
                    .map_err(|_| ())
            })
            .as_ref()
            .map_err(|_| Error::Schema)?;
        if schema.is_valid(&self.value) {
            Ok(())
        } else {
            Err(Error::Schema)
        }
    }
    pub fn validate_static(&self) -> Result<validation::Resources> {
        self.validate_structural()?;
        validation::check(&self.value)
    }
}
