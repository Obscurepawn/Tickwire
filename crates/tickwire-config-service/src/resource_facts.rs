//! Signed retained-resource snapshots plus candidate capacity arithmetic.
use crate::{
    Error, Result,
    evidence::{Catalog, Kind, Trust, VerifiedEvidence},
};
use serde::Deserialize;
use serde_json::Value;
use tickwire_config::{
    Candidate, Format, Limits as ParseLimits,
    artifacts::ArtifactCatalog,
    capacity::{CapacityAssessment, RetainedResources},
};

#[derive(Clone, Copy)]
pub struct Limits {
    pub max_age_ms: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self { max_age_ms: 5_000 }
    }
}
pub struct RetainedCapacity {
    retained_revision: u64,
    capacity: CapacityAssessment,
    evidence: VerifiedEvidence,
    expires_unix_ms: u64,
}
impl RetainedCapacity {
    pub fn check(
        candidate: &Candidate,
        artifacts: &ArtifactCatalog,
        trust: &Trust,
        signatures: &Catalog,
        reference: &str,
        now_unix_ms: u64,
        limits: Limits,
    ) -> Result<Self> {
        if !(1..=60_000).contains(&limits.max_age_ms) {
            return Err(Error::Bounds);
        }
        candidate.validate_static().map_err(Error::Configuration)?;
        let evidence = trust.verify(
            signatures
                .get(Kind::RetainedResources, reference)
                .ok_or(Error::NotFound)?,
            artifacts,
            Kind::RetainedResources,
            reference,
            now_unix_ms,
        )?;
        let parsed = Candidate::parse(
            Format::Json,
            artifacts.get(reference).ok_or(Error::NotFound)?,
            ParseLimits {
                max_bytes: 4 * 1024 * 1024,
                max_depth: 16,
            },
        )
        .map_err(Error::Configuration)?;
        let document: Snapshot =
            serde_json::from_value(parsed.value().clone()).map_err(|_| Error::Protocol)?;
        if document.schema != 1
            || candidate.value()["identity"]["cluster_id"] != document.cluster_id
            || candidate.value()["identity"]["namespace_id"] != document.namespace_id
            || evidence.claim().cluster_id != document.cluster_id
            || evidence.claim().namespace_id != document.namespace_id
        {
            return Err(Error::Identity);
        }
        let expires_unix_ms = document
            .observed_unix_ms
            .checked_add(limits.max_age_ms)
            .ok_or(Error::Bounds)?
            .min(evidence.claim().expires_unix_ms);
        if document.observed_unix_ms == 0
            || document.observed_unix_ms > evidence.claim().issued_unix_ms
            || now_unix_ms >= expires_unix_ms
        {
            return Err(Error::NotReady);
        }
        let retained = RetainedResources::decode(
            &serde_json::to_vec(&document.retained).map_err(|_| Error::Protocol)?,
        )
        .map_err(Error::Configuration)?;
        let capacity = CapacityAssessment::check(candidate, artifacts, &retained)
            .map_err(Error::Configuration)?;
        Ok(Self {
            retained_revision: retained.revision(),
            capacity,
            evidence,
            expires_unix_ms,
        })
    }
    pub fn retained_revision(&self) -> u64 {
        self.retained_revision
    }
    pub fn capacity(&self) -> &CapacityAssessment {
        &self.capacity
    }
    pub fn evidence(&self) -> &VerifiedEvidence {
        &self.evidence
    }
    pub fn expires_unix_ms(&self) -> u64 {
        self.expires_unix_ms
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    schema: u32,
    cluster_id: String,
    namespace_id: String,
    observed_unix_ms: u64,
    retained: Value,
}
