//! Signed observations of the complete current baseline, not live registry authority.
use crate::{
    Error, Hash, Result,
    evidence::{Catalog, Kind, Trust, VerifiedEvidence},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use tickwire_config::{
    Candidate, Format,
    artifacts::{ArtifactCatalog, reference_hash},
    delivery::ProjectionSet,
};
const MAX_HOSTS: usize = 256;
const MAX_TARGETS: usize = 4096;
const MAX_DOCUMENT_BYTES: usize = 4 * 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 16 * 1024 * 1024;

/// Startup-owned scope. A signed document cannot extend this permission.
#[derive(Clone, Serialize)]
pub struct IssuerScope {
    pub issuer_id: String,
    pub host_id: String,
    pub process_ids: BTreeSet<String>,
}
pub struct RuntimeTrust {
    trust_hash: Hash,
    scopes: BTreeMap<String, IssuerScope>,
    digest: Hash,
}
impl RuntimeTrust {
    pub fn new(trust: &Trust, scopes: Vec<IssuerScope>) -> Result<Self> {
        if scopes.is_empty() || scopes.len() > MAX_HOSTS {
            return Err(Error::Bounds);
        }
        let mut by_host = BTreeMap::new();
        let mut processes = BTreeSet::new();
        for scope in scopes {
            if !name(&scope.issuer_id)
                || !name(&scope.host_id)
                || scope.process_ids.is_empty()
                || !trust.permits(&scope.issuer_id, Kind::RuntimeTargets)
            {
                return Err(Error::Authentication);
            }
            for process in &scope.process_ids {
                if !name(process) || !processes.insert(process.clone()) {
                    return Err(Error::Conflict);
                }
                if processes.len() > MAX_TARGETS {
                    return Err(Error::Bounds);
                }
            }
            if by_host.insert(scope.host_id.clone(), scope).is_some() {
                return Err(Error::Conflict);
            }
        }
        let digest = content_hash(
            b"tickwire.config.runtime-trust.v1\0",
            &json!({
                "trust_hash":trust.digest(),"scopes":by_host.values().collect::<Vec<_>>()
            }),
        )?;
        Ok(Self {
            trust_hash: trust.digest(),
            scopes: by_host,
            digest,
        })
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
}
pub struct Input<'a> {
    pub base: &'a Candidate,
    /// Supplied by the caller's authenticated committed-head read.
    pub current_revision: u64,
    pub projections: &'a ProjectionSet,
}
pub struct Sources<'a> {
    pub artifacts: &'a ArtifactCatalog,
    pub signatures: &'a Catalog,
    pub trust: &'a Trust,
    pub runtime_trust: &'a RuntimeTrust,
    pub references: &'a [String],
}
#[derive(Clone, Copy)]
pub struct Limits {
    pub max_age_ms: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self { max_age_ms: 5_000 }
    }
}
/// Untrusted data until included in a successfully checked RuntimeTargets.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub process_id: String,
    pub host_id: String,
    pub role: String,
    pub build_hash: Hash,
    pub instance: [u8; 16],
    pub boot: [u8; 16],
    pub pid: u32,
    #[serde(with = "decimal")]
    pub exec_generation: u64,
    pub instance_slot: [u8; 16],
    #[serde(with = "decimal")]
    pub locator_generation: u64,
    #[serde(with = "decimal")]
    pub runtime_revision: u64,
    pub global_hash: Hash,
    pub projection_hash: Hash,
    #[serde(with = "decimal")]
    pub projection_policy_revision: u64,
    #[serde(with = "decimal")]
    pub applied_event_seq: u64,
}
/// One complete host observation, never a mergeable fragment of a host epoch.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Document {
    pub schema: u32,
    pub cluster_id: String,
    pub namespace_id: String,
    pub host_id: String,
    pub host_boot: [u8; 16],
    pub source_epoch: [u8; 16],
    #[serde(with = "decimal")]
    pub source_sequence: u64,
    pub observed_unix_ms: u64,
    pub targets: Vec<Target>,
}
pub struct HostFact {
    host_boot: [u8; 16],
    source_epoch: [u8; 16],
    source_sequence: u64,
    observed_unix_ms: u64,
    evidence: VerifiedEvidence,
}
impl HostFact {
    pub fn host_boot(&self) -> [u8; 16] {
        self.host_boot
    }
    pub fn source_epoch(&self) -> [u8; 16] {
        self.source_epoch
    }
    pub fn source_sequence(&self) -> u64 {
        self.source_sequence
    }
    pub fn observed_unix_ms(&self) -> u64 {
        self.observed_unix_ms
    }
    pub fn evidence(&self) -> &VerifiedEvidence {
        &self.evidence
    }
}
pub struct RuntimeTargets {
    base_hash: Hash,
    current_revision: u64,
    targets: BTreeMap<String, Target>,
    hosts: BTreeMap<String, HostFact>,
    expires_unix_ms: u64,
    checked_unix_ms: u64,
    digest: Hash,
}
impl RuntimeTargets {
    pub fn check(
        input: Input<'_>,
        sources: Sources<'_>,
        now_unix_ms: u64,
        limits: Limits,
    ) -> Result<Self> {
        if !positive(input.current_revision)
            || !(1..=60_000).contains(&limits.max_age_ms)
            || sources.references.is_empty()
            || sources.references.len() > MAX_HOSTS
        {
            return Err(Error::Bounds);
        }
        if sources.runtime_trust.trust_hash != sources.trust.digest() {
            return Err(Error::Authentication);
        }
        input.base.validate_static().map_err(Error::Configuration)?;
        if input.projections.candidate_hash() != input.base.hash() {
            return Err(Error::Conflict);
        }
        let base = input.base.value();
        let expected: BTreeMap<_, _> = base["processes"]
            .as_array()
            .ok_or(Error::Protocol)?
            .iter()
            .map(|p| Ok((p["id"].as_str().ok_or(Error::Protocol)?, p)))
            .collect::<Result<_>>()?;
        if expected.is_empty() || expected.len() > MAX_TARGETS {
            return Err(Error::Bounds);
        }
        if expected.keys().copied().collect::<BTreeSet<_>>()
            != input
                .projections
                .targets()
                .keys()
                .map(String::as_str)
                .collect()
        {
            return Err(Error::Conflict);
        }
        let mut targets = BTreeMap::new();
        let mut hosts = BTreeMap::new();
        let mut references = BTreeSet::new();
        let mut instances = BTreeSet::new();
        let mut boots = BTreeSet::new();
        let mut slots = BTreeSet::new();
        let mut pids = BTreeSet::new();
        let mut total = 0usize;
        let mut expires_unix_ms = u64::MAX;
        for reference in sources.references {
            if !references.insert(reference) {
                return Err(Error::Conflict);
            }
            let bytes = sources.artifacts.get(reference).ok_or(Error::NotFound)?;
            if bytes.is_empty() || bytes.len() > MAX_DOCUMENT_BYTES {
                return Err(Error::Bounds);
            }
            total = total
                .checked_add(bytes.len())
                .filter(|n| *n <= MAX_TOTAL_BYTES)
                .ok_or(Error::Bounds)?;
            let evidence = sources.trust.verify(
                sources
                    .signatures
                    .get(Kind::RuntimeTargets, reference)
                    .ok_or(Error::NotFound)?,
                sources.artifacts,
                Kind::RuntimeTargets,
                reference,
                now_unix_ms,
            )?;
            let parsed = Candidate::parse(
                Format::Json,
                bytes,
                tickwire_config::Limits {
                    max_bytes: MAX_DOCUMENT_BYTES,
                    max_depth: 16,
                },
            )
            .map_err(Error::Configuration)?;
            let document: Document =
                serde_json::from_value(parsed.value().clone()).map_err(|_| Error::Protocol)?;
            let claim = evidence.claim();
            if document.schema != 1
                || document.cluster_id != claim.cluster_id
                || document.namespace_id != claim.namespace_id
                || base["identity"]["cluster_id"] != document.cluster_id
                || base["identity"]["namespace_id"] != document.namespace_id
                || document.host_boot == [0; 16]
                || document.source_epoch == [0; 16]
                || document.targets.is_empty()
                || document.targets.len() > MAX_TARGETS
            {
                return Err(Error::Identity);
            }
            let scope = sources
                .runtime_trust
                .scopes
                .get(&document.host_id)
                .ok_or(Error::Authorization)?;
            if scope.issuer_id != claim.issuer_id {
                return Err(Error::Authorization);
            }
            if hosts.contains_key(&document.host_id) {
                return Err(Error::Conflict);
            }
            let expiry = document
                .observed_unix_ms
                .checked_add(limits.max_age_ms)
                .ok_or(Error::Bounds)?
                .min(claim.expires_unix_ms);
            if document.observed_unix_ms == 0
                || document.observed_unix_ms > claim.issued_unix_ms
                || now_unix_ms >= expiry
            {
                return Err(Error::NotReady);
            }
            expires_unix_ms = expires_unix_ms.min(expiry);
            for target in document.targets {
                if !scope.process_ids.contains(&target.process_id) {
                    return Err(Error::Authorization);
                }
                let process = expected
                    .get(target.process_id.as_str())
                    .ok_or(Error::Identity)?;
                let projection = input
                    .projections
                    .targets()
                    .get(&target.process_id)
                    .ok_or(Error::Identity)?;
                let build =
                    reference_hash(process["artifact_ref"].as_str().ok_or(Error::Protocol)?)
                        .map_err(Error::Configuration)?;
                if target.host_id != document.host_id
                    || process["host_id"] != target.host_id
                    || process["role"] != target.role
                    || target.build_hash == [0; 32]
                    || target.build_hash != build
                    || target.instance == [0; 16]
                    || target.boot == [0; 16]
                    || target.instance_slot == [0; 16]
                    || target.pid == 0
                    || target.pid > i32::MAX as u32
                    || target.runtime_revision != input.current_revision
                    || target.global_hash != input.base.hash()
                    || target.projection_hash != projection.hash()
                    || target.projection_policy_revision != projection.policy_revision()
                {
                    return Err(Error::Identity);
                }
                if !instances.insert(target.instance)
                    || !boots.insert(target.boot)
                    || !slots.insert(target.instance_slot)
                    || !pids.insert((target.host_id.clone(), target.pid))
                    || targets.insert(target.process_id.clone(), target).is_some()
                {
                    return Err(Error::Conflict);
                }
                if targets.len() > MAX_TARGETS {
                    return Err(Error::Bounds);
                }
            }
            hosts.insert(
                document.host_id,
                HostFact {
                    host_boot: document.host_boot,
                    source_epoch: document.source_epoch,
                    source_sequence: document.source_sequence,
                    observed_unix_ms: document.observed_unix_ms,
                    evidence,
                },
            );
        }
        if targets.keys().map(String::as_str).collect::<BTreeSet<_>>()
            != expected.keys().copied().collect()
        {
            return Err(Error::Incomplete);
        }
        let source_objects: Vec<_> = hosts.iter().map(|(host,fact)|json!({
            "host":host,"reference":fact.evidence.claim().artifact_ref,
            "artifact_hash":fact.evidence.artifact_hash(),"envelope_hash": <Hash>::from(Sha256::digest(fact.evidence.envelope()))
        })).collect();
        let digest = content_hash(
            b"tickwire.config.runtime-targets.v1\0",
            &json!({
                "base_hash":input.base.hash(),"revision":input.current_revision.to_string(),
                "runtime_trust":sources.runtime_trust.digest(),"sources":source_objects,"max_age_ms":limits.max_age_ms
            }),
        )?;
        Ok(Self {
            base_hash: input.base.hash(),
            current_revision: input.current_revision,
            targets,
            hosts,
            expires_unix_ms,
            checked_unix_ms: now_unix_ms,
            digest,
        })
    }
    pub fn base_hash(&self) -> Hash {
        self.base_hash
    }
    pub fn current_revision(&self) -> u64 {
        self.current_revision
    }
    pub fn targets(&self) -> &BTreeMap<String, Target> {
        &self.targets
    }
    pub fn hosts(&self) -> &BTreeMap<String, HostFact> {
        &self.hosts
    }
    pub fn expires_unix_ms(&self) -> u64 {
        self.expires_unix_ms
    }
    pub fn checked_unix_ms(&self) -> u64 {
        self.checked_unix_ms
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
}
fn positive(value: u64) -> bool {
    value > 0 && value < 1 << 63
}
fn name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
fn content_hash(domain: &[u8], value: &serde_json::Value) -> Result<Hash> {
    let candidate = Candidate::parse(
        Format::Json,
        &serde_json::to_vec(value).map_err(|_| Error::Protocol)?,
        tickwire_config::Limits::default(),
    )
    .map_err(Error::Configuration)?;
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(candidate.canonical_bytes());
    Ok(hash.finalize().into())
}
mod decimal {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        let text = String::deserialize(d)?;
        let value = text.parse::<u64>().map_err(D::Error::custom)?;
        if !super::positive(value) || value.to_string() != text {
            return Err(D::Error::custom("canonical positive revision"));
        }
        Ok(value)
    }
    pub fn serialize<S: Serializer>(value: &u64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&value.to_string())
    }
}
