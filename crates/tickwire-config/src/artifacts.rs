//! Content-pinned configuration dependencies; no retrieval or deployment authority.
use crate::{Candidate, Error, Format, Hash, Limits, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
pub struct CatalogLimits {
    pub max_objects: usize,
    pub max_object_bytes: usize,
    pub max_total_bytes: usize,
}
impl Default for CatalogLimits {
    fn default() -> Self {
        Self {
            max_objects: 4096,
            max_object_bytes: 16 * 1024 * 1024,
            max_total_bytes: 512 * 1024 * 1024,
        }
    }
}
pub struct ArtifactCatalog {
    limits: CatalogLimits,
    objects: BTreeMap<String, Vec<u8>>,
    bytes: usize,
}
impl ArtifactCatalog {
    pub fn new(limits: CatalogLimits) -> Result<Self> {
        if !(1..=4096).contains(&limits.max_objects)
            || !(1..=16 * 1024 * 1024).contains(&limits.max_object_bytes)
            || !(1..=512 * 1024 * 1024).contains(&limits.max_total_bytes)
        {
            return Err(Error::Capacity);
        }
        Ok(Self {
            limits,
            objects: BTreeMap::new(),
            bytes: 0,
        })
    }
    pub fn insert(&mut self, reference: &str, bytes: &[u8]) -> Result<()> {
        let expected = reference_hash(reference)?;
        if bytes.len() > self.limits.max_object_bytes {
            return Err(Error::Capacity);
        }
        let actual: Hash = Sha256::digest(bytes).into();
        if actual != expected {
            return Err(Error::Semantic("artifact digest"));
        }
        if let Some(existing) = self.objects.get(reference) {
            return if existing == bytes {
                Ok(())
            } else {
                Err(Error::Semantic("artifact conflict"))
            };
        }
        let size = self.bytes.checked_add(bytes.len()).ok_or(Error::Capacity)?;
        if self.objects.len() >= self.limits.max_objects || size > self.limits.max_total_bytes {
            return Err(Error::Capacity);
        }
        self.objects.insert(reference.to_owned(), bytes.to_vec());
        self.bytes = size;
        Ok(())
    }
    pub fn get(&self, reference: &str) -> Option<&[u8]> {
        self.objects.get(reference).map(Vec::as_slice)
    }
    pub fn stored_bytes(&self) -> usize {
        self.bytes
    }
    /// Immutable source bytes, sorted by full reference. No retrieval or trust is implied.
    pub fn objects(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.objects
            .iter()
            .map(|(reference, bytes)| (reference.as_str(), bytes.as_slice()))
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dependency {
    pub reference: String,
    pub uses: Vec<String>,
    pub available: bool,
}
#[derive(Debug)]
pub struct DependencyReport {
    pub candidate_hash: Hash,
    pub artifacts: Vec<Dependency>,
    pub secrets: BTreeMap<String, Vec<String>>,
}
impl DependencyReport {
    pub fn inspect(candidate: &Candidate, catalog: &ArtifactCatalog) -> Result<Self> {
        candidate.validate_static()?;
        let mut artifacts = BTreeMap::new();
        let mut secrets = BTreeMap::new();
        collect(candidate.value(), "", &mut artifacts, &mut secrets, &mut 0)?;
        Ok(Self {
            candidate_hash: candidate.hash(),
            artifacts: artifacts
                .into_iter()
                .map(|(reference, uses)| {
                    let available = catalog.get(&reference).is_some();
                    Dependency {
                        reference,
                        uses,
                        available,
                    }
                })
                .collect(),
            secrets,
        })
    }
    pub fn missing(&self) -> impl Iterator<Item = &Dependency> {
        self.artifacts.iter().filter(|d| !d.available)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterBinding {
    pub process_id: String,
    pub schema_hash: Hash,
}
#[derive(Debug)]
pub struct ParametersValidated {
    candidate_hash: Hash,
    bindings: Vec<ParameterBinding>,
}
impl ParametersValidated {
    pub fn candidate_hash(&self) -> Hash {
        self.candidate_hash
    }
    pub fn bindings(&self) -> &[ParameterBinding] {
        &self.bindings
    }
}
pub fn validate_parameters(
    candidate: &Candidate,
    catalog: &ArtifactCatalog,
) -> Result<ParametersValidated> {
    candidate.validate_static()?;
    let mut validators = BTreeMap::new();
    let mut bindings = Vec::new();
    for strategy in candidate.value()["strategies"]
        .as_array()
        .ok_or(Error::Schema)?
    {
        let reference = strategy["parameter_schema_ref"]
            .as_str()
            .ok_or(Error::Schema)?;
        let hash = reference_hash(reference)?;
        let bytes = catalog
            .get(reference)
            .ok_or(Error::Semantic("missing parameter schema"))?;
        if !validators.contains_key(reference) {
            let schema = Candidate::parse(
                Format::Json,
                bytes,
                Limits {
                    max_bytes: 1024 * 1024,
                    max_depth: 32,
                },
            )?;
            local_references(schema.value())?;
            let validator = jsonschema::options()
                .with_draft(jsonschema::Draft::Draft202012)
                .should_validate_formats(true)
                .with_pattern_options(
                    jsonschema::PatternOptions::regex()
                        .size_limit(1024 * 1024)
                        .dfa_size_limit(1024 * 1024),
                )
                .offline()
                .build(schema.value())
                .map_err(|_| Error::Schema)?;
            validators.insert(reference.to_owned(), validator);
        }
        if !validators
            .get(reference)
            .ok_or(Error::Schema)?
            .is_valid(&strategy["params"])
        {
            return Err(Error::Semantic("strategy parameters"));
        }
        bindings.push(ParameterBinding {
            process_id: strategy["process_id"]
                .as_str()
                .ok_or(Error::Schema)?
                .to_owned(),
            schema_hash: hash,
        });
    }
    bindings.sort_by(|a, b| a.process_id.cmp(&b.process_id));
    Ok(ParametersValidated {
        candidate_hash: candidate.hash(),
        bindings,
    })
}

/// Parse a bounded content-addressed reference without retrieving its object.
pub fn reference_hash(reference: &str) -> Result<Hash> {
    let invalid = || Error::Semantic("artifact reference");
    if reference.len() > 2048 {
        return Err(invalid());
    }
    let (path, hex) = reference
        .strip_prefix("artifact://")
        .ok_or_else(invalid)?
        .split_once("@sha256:")
        .ok_or_else(invalid)?;
    let (namespace, path) = path.split_once('/').ok_or_else(invalid)?;
    if namespace.is_empty()
        || !namespace.as_bytes()[0].is_ascii_lowercase()
        || !namespace
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        || path.is_empty()
        || !path.as_bytes()[0].is_ascii_alphanumeric()
        || !path.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'/' | b'_' | b'-')
        })
        || hex.len() != 64
        || !hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid());
    }
    let mut hash = [0; 32];
    for (out, pair) in hash.iter_mut().zip(hex.as_bytes().as_chunks::<2>().0) {
        let digit = |b| if b <= b'9' { b - b'0' } else { b - b'a' + 10 };
        *out = digit(pair[0]) * 16 + digit(pair[1]);
    }
    Ok(hash)
}
type Uses = BTreeMap<String, Vec<String>>;
fn collect(
    value: &Value,
    path: &str,
    artifacts: &mut Uses,
    secrets: &mut Uses,
    count: &mut usize,
) -> Result<()> {
    match value {
        Value::String(s) if s.starts_with("artifact://") || s.starts_with("secret://") => {
            *count = count.checked_add(1).ok_or(Error::Capacity)?;
            if *count > 65536 || s.len() > 2048 {
                return Err(Error::Capacity);
            }
            let map = if s.starts_with("artifact://") {
                reference_hash(s)?;
                artifacts
            } else {
                secrets
            };
            if !map.contains_key(s) && map.len() >= 4096 {
                return Err(Error::Capacity);
            }
            map.entry(s.clone()).or_default().push(path.to_owned());
        }
        Value::Object(values) => {
            for (key, value) in values {
                let key = key.replace('~', "~0").replace('/', "~1");
                collect(value, &format!("{path}/{key}"), artifacts, secrets, count)?;
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                collect(value, &format!("{path}/{index}"), artifacts, secrets, count)?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn local_references(value: &Value) -> Result<()> {
    match value {
        Value::Object(values) => {
            for (key, value) in values {
                if matches!(key.as_str(), "$ref" | "$dynamicRef")
                    && !value.as_str().is_some_and(|v| v.starts_with('#'))
                {
                    return Err(Error::Schema);
                }
                if key == "$schema" && value != "https://json-schema.org/draft/2020-12/schema" {
                    return Err(Error::Schema);
                }
                local_references(value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                local_references(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}
