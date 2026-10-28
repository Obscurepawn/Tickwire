//! One owned, bounded source package. Its bytes do not establish issuer trust.
use crate::{
    Error, Hash, Result,
    deployment::Sources,
    evidence::{Catalog, Kind},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::File,
    io::{self, Read, Seek, Write},
};
use tickwire_config::{
    Candidate, Format,
    artifacts::{ArtifactCatalog, CatalogLimits},
};

const MAX_BUNDLE: usize = 528 * 1024 * 1024 + 12;
#[derive(Clone, Copy)]
pub struct Limits {
    pub max_bundle_bytes: usize,
    pub max_header_bytes: usize,
    pub artifacts: CatalogLimits,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_bundle_bytes: 64 * 1024 * 1024,
            max_header_bytes: 4 * 1024 * 1024,
            artifacts: CatalogLimits::default(),
        }
    }
}
pub struct Bundle {
    artifacts: ArtifactCatalog,
    signatures: Catalog,
    retained_ref: String,
    credential_refs: Vec<String>,
    digest: Hash,
    stored_bytes: usize,
}
impl Bundle {
    /// Canonical packaging of the complete owned sources. This does not replace
    /// the original input identity reported by `digest` and `stored_bytes`.
    pub fn encode(&self, limits: Limits) -> Result<Vec<u8>> {
        limits.validate()?;
        if self.artifacts.objects().count() > limits.artifacts.max_objects
            || self.artifacts.stored_bytes() > limits.artifacts.max_total_bytes
            || self
                .artifacts
                .objects()
                .any(|(_, bytes)| bytes.len() > limits.artifacts.max_object_bytes)
        {
            return Err(Error::Bounds);
        }
        let header_budget = limits
            .max_bundle_bytes
            .checked_sub(12)
            .and_then(|n| n.checked_sub(self.artifacts.stored_bytes()))
            .ok_or(Error::Bounds)?
            .min(limits.max_header_bytes);
        let mut credential_refs = self.credential_refs.clone();
        credential_refs.sort();
        let header = Header {
            schema: 1,
            artifacts: self
                .artifacts
                .objects()
                .map(|(reference, bytes)| Entry {
                    reference: reference.to_owned(),
                    byte_len: bytes.len(),
                })
                .collect(),
            evidence: self
                .signatures
                .envelopes()
                .map(|(_, _, bytes)| serde_json::from_slice(bytes).map_err(|_| Error::Protocol))
                .collect::<Result<Vec<_>>>()?,
            retained_ref: self.retained_ref.clone(),
            credential_refs,
        };
        let mut encoded = HeaderWriter {
            bytes: Vec::new(),
            limit: header_budget,
        };
        serde_json::to_writer(&mut encoded, &header).map_err(|_| Error::Bounds)?;
        let canonical = Candidate::parse(
            Format::Json,
            &encoded.bytes,
            tickwire_config::Limits {
                max_bytes: limits.max_header_bytes,
                max_depth: 16,
            },
        )
        .map_err(Error::Configuration)?;
        let header = canonical.canonical_bytes();
        let header_len = u32::try_from(header.len()).map_err(|_| Error::Bounds)?;
        let total = 12usize
            .checked_add(header.len())
            .and_then(|n| n.checked_add(self.artifacts.stored_bytes()))
            .filter(|n| *n <= limits.max_bundle_bytes)
            .ok_or(Error::Bounds)?;
        let mut bytes = Vec::with_capacity(total);
        bytes.extend_from_slice(b"TWDSRC01");
        bytes.extend_from_slice(&header_len.to_le_bytes());
        bytes.extend_from_slice(header);
        for (_, object) in self.artifacts.objects() {
            bytes.extend_from_slice(object);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8], limits: Limits) -> Result<Self> {
        limits.validate()?;
        if bytes.len() > limits.max_bundle_bytes {
            return Err(Error::Bounds);
        }
        if bytes.len() < 12 || &bytes[..8] != b"TWDSRC01" {
            return Err(Error::Protocol);
        }
        let header_len =
            u32::from_le_bytes(bytes[8..12].try_into().map_err(|_| Error::Protocol)?) as usize;
        if header_len == 0 || header_len > limits.max_header_bytes {
            return Err(Error::Bounds);
        }
        let mut offset = 12usize.checked_add(header_len).ok_or(Error::Bounds)?;
        let parsed = Candidate::parse(
            Format::Json,
            bytes.get(12..offset).ok_or(Error::Incomplete)?,
            tickwire_config::Limits {
                max_bytes: limits.max_header_bytes,
                max_depth: 16,
            },
        )
        .map_err(Error::Configuration)?;
        let header: Header =
            serde_json::from_value(parsed.value().clone()).map_err(|_| Error::Protocol)?;
        if header.schema != 1 {
            return Err(Error::Protocol);
        }
        if header.artifacts.is_empty()
            || header.artifacts.len() > limits.artifacts.max_objects
            || header.evidence.len() > 4096
            || header.credential_refs.len() > 4096
        {
            return Err(Error::Bounds);
        }
        let mut artifacts = ArtifactCatalog::new(limits.artifacts).map_err(Error::Configuration)?;
        let mut references = BTreeSet::new();
        for object in header.artifacts {
            if !references.insert(object.reference.clone()) {
                return Err(Error::Conflict);
            }
            if object.byte_len > limits.artifacts.max_object_bytes {
                return Err(Error::Bounds);
            }
            let end = offset.checked_add(object.byte_len).ok_or(Error::Bounds)?;
            let data = bytes.get(offset..end).ok_or(Error::Incomplete)?;
            artifacts
                .insert(&object.reference, data)
                .map_err(Error::Configuration)?;
            offset = end;
        }
        if offset != bytes.len() {
            return Err(Error::Protocol);
        }
        let count = header.evidence.len();
        let mut signatures = Catalog::default();
        for evidence in header.evidence {
            signatures.insert(&serde_json::to_vec(&evidence).map_err(|_| Error::Protocol)?)?;
        }
        if signatures.envelopes().count() != count {
            return Err(Error::Conflict);
        }
        for (_, reference, _) in signatures.envelopes() {
            if artifacts.get(reference).is_none() {
                return Err(Error::NotFound);
            }
        }
        if artifacts.get(&header.retained_ref).is_none()
            || signatures
                .get(Kind::RetainedResources, &header.retained_ref)
                .is_none()
        {
            return Err(Error::NotFound);
        }
        let mut unique = BTreeSet::new();
        for reference in &header.credential_refs {
            if !unique.insert(reference) {
                return Err(Error::Conflict);
            }
            if artifacts.get(reference).is_none()
                || signatures
                    .get(Kind::CredentialMetadata, reference)
                    .is_none()
            {
                return Err(Error::NotFound);
            }
        }
        Ok(Self {
            artifacts,
            signatures,
            retained_ref: header.retained_ref,
            credential_refs: header.credential_refs,
            digest: Sha256::digest(bytes).into(),
            stored_bytes: bytes.len(),
        })
    }
    pub fn from_file(mut file: File, limits: Limits) -> Result<Self> {
        limits.validate()?;
        let metadata = file.metadata().map_err(|_| Error::Transport)?;
        if !metadata.is_file() {
            return Err(Error::Protocol);
        }
        if metadata.len() > limits.max_bundle_bytes as u64 {
            return Err(Error::Bounds);
        }
        file.rewind().map_err(|_| Error::Transport)?;
        let mut bytes = Vec::new();
        file.take(limits.max_bundle_bytes as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error::Transport)?;
        Self::decode(&bytes, limits)
    }
    pub fn sources(&self) -> Sources<'_> {
        Sources {
            artifacts: &self.artifacts,
            signatures: &self.signatures,
            retained_ref: &self.retained_ref,
            credential_refs: &self.credential_refs,
        }
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
    pub fn stored_bytes(&self) -> usize {
        self.stored_bytes
    }
}
impl Limits {
    fn validate(self) -> Result<()> {
        if !(12..=MAX_BUNDLE).contains(&self.max_bundle_bytes)
            || !(1..=16 * 1024 * 1024).contains(&self.max_header_bytes)
        {
            return Err(Error::Bounds);
        }
        ArtifactCatalog::new(self.artifacts).map_err(Error::Configuration)?;
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    schema: u32,
    artifacts: Vec<Entry>,
    evidence: Vec<Value>,
    retained_ref: String,
    credential_refs: Vec<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    reference: String,
    byte_len: usize,
}

struct HeaderWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for HeaderWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|len| len > self.limit)
        {
            return Err(io::Error::other("source bundle header limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
