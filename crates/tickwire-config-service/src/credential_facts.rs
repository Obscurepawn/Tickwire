//! Signed credential usage metadata over the complete generated projection set.
use crate::{
    Error, Hash, Result,
    evidence::{Catalog, Kind, Trust, VerifiedEvidence},
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use tickwire_config::{Candidate, Format, artifacts::ArtifactCatalog, delivery::ProjectionSet};

const MAX_ITEMS: usize = 4096;
const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 16 * 1024 * 1024;
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_age_ms: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self { max_age_ms: 5000 }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    schema: u32,
    cluster_id: String,
    namespace_id: String,
    secret_ref: String,
    secret_version: String,
    key_fingerprint: String,
    enabled: bool,
    observed_unix_ms: u64,
    grants: Vec<Grant>,
}
#[derive(Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
struct Grant {
    process_id: String,
    host_id: String,
    os_identity: String,
    os_uid: u32,
    purpose: String,
    account_id: Option<String>,
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Expected {
    process_id: String,
    host_id: String,
    os_identity: String,
    purpose: String,
    account_id: Option<String>,
    secret_ref: String,
}
impl Expected {
    fn matches(&self, grant: &Grant) -> bool {
        self.process_id == grant.process_id
            && self.host_id == grant.host_id
            && self.os_identity == grant.os_identity
            && self.purpose == grant.purpose
            && self.account_id == grant.account_id
    }
    fn with_uid(self, os_uid: u32) -> Usage {
        Usage {
            process_id: self.process_id,
            host_id: self.host_id,
            os_identity: self.os_identity,
            os_uid,
            purpose: self.purpose,
            account_id: self.account_id,
            secret_ref: self.secret_ref,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Usage {
    pub process_id: String,
    pub host_id: String,
    pub os_identity: String,
    pub os_uid: u32,
    pub purpose: String,
    pub account_id: Option<String>,
    pub secret_ref: String,
}
pub struct CredentialFacts {
    candidate_hash: Hash,
    uses: Vec<Usage>,
    evidence: Vec<VerifiedEvidence>,
    expires_unix_ms: u64,
}
impl CredentialFacts {
    pub fn check(
        projections: &ProjectionSet,
        artifacts: &ArtifactCatalog,
        trust: &Trust,
        signatures: &Catalog,
        metadata_refs: &[String],
        now_unix_ms: u64,
        limits: Limits,
    ) -> Result<Self> {
        if !(1..=60000).contains(&limits.max_age_ms)
            || now_unix_ms == 0
            || now_unix_ms > MAX_SAFE_INTEGER
            || metadata_refs.len() > MAX_ITEMS
        {
            return Err(Error::Bounds);
        }
        let (scope, expected) = actual_uses(projections)?;
        let secrets: BTreeSet<_> = expected.iter().map(|u| u.secret_ref.as_str()).collect();
        let mut seen_refs = BTreeSet::new();
        let mut docs = BTreeMap::new();
        let mut evidence = vec![];
        let mut expires_unix_ms = u64::MAX;
        let mut bytes = 0usize;
        let mut grants = 0usize;
        let mut by_identity = BTreeMap::new();
        let mut by_uid = BTreeMap::new();
        let mut key_purposes: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for reference in metadata_refs {
            if !seen_refs.insert(reference) {
                return Err(Error::Conflict);
            }
            let verified = trust.verify(
                signatures
                    .get(Kind::CredentialMetadata, reference)
                    .ok_or(Error::NotFound)?,
                artifacts,
                Kind::CredentialMetadata,
                reference,
                now_unix_ms,
            )?;
            if verified.claim().cluster_id != scope.0 || verified.claim().namespace_id != scope.1 {
                return Err(Error::Identity);
            }
            let raw = artifacts.get(reference).ok_or(Error::NotFound)?;
            bytes = bytes.checked_add(raw.len()).ok_or(Error::Bounds)?;
            if bytes > MAX_TOTAL_BYTES {
                return Err(Error::Bounds);
            }
            let parsed = Candidate::parse(
                Format::Json,
                raw,
                tickwire_config::Limits {
                    max_bytes: MAX_DOCUMENT_BYTES,
                    max_depth: 8,
                },
            )
            .map_err(Error::Configuration)?;
            let doc: Metadata =
                serde_json::from_value(parsed.value().clone()).map_err(|_| Error::Protocol)?;
            if doc.schema != 1
                || doc.cluster_id != scope.0
                || doc.namespace_id != scope.1
                || !secrets.contains(doc.secret_ref.as_str())
            {
                return Err(Error::Identity);
            }
            let version = doc.secret_ref.rsplit('/').next().ok_or(Error::Identity)?;
            let number = doc
                .secret_version
                .parse::<u64>()
                .map_err(|_| Error::Identity)?;
            if number == 0
                || number.to_string() != doc.secret_version
                || version != doc.secret_version
            {
                return Err(Error::Identity);
            }
            fingerprint(&doc.key_fingerprint)?;
            if !doc.enabled
                || doc.observed_unix_ms == 0
                || doc.observed_unix_ms > verified.claim().issued_unix_ms
            {
                return Err(Error::Authentication);
            }
            let until = doc
                .observed_unix_ms
                .checked_add(limits.max_age_ms)
                .ok_or(Error::Bounds)?
                .min(verified.claim().expires_unix_ms);
            if now_unix_ms >= until {
                return Err(Error::Authentication);
            }
            expires_unix_ms = expires_unix_ms.min(until);
            grants = grants.checked_add(doc.grants.len()).ok_or(Error::Bounds)?;
            if doc.grants.is_empty() || grants > MAX_ITEMS {
                return Err(Error::Bounds);
            }
            let mut unique = BTreeSet::new();
            for grant in &doc.grants {
                validate_grant(grant)?;
                if !unique.insert(grant.clone()) {
                    return Err(Error::Conflict);
                }
                let identity = (grant.host_id.clone(), grant.os_identity.clone());
                if by_identity
                    .insert(identity, grant.os_uid)
                    .is_some_and(|uid| uid != grant.os_uid)
                    || by_uid
                        .insert(
                            (grant.host_id.clone(), grant.os_uid),
                            grant.os_identity.clone(),
                        )
                        .is_some_and(|id| id != grant.os_identity)
                {
                    return Err(Error::Conflict);
                }
                let purposes = key_purposes.entry(doc.key_fingerprint.clone()).or_default();
                purposes.insert(grant.purpose.clone());
                if purposes.contains("execution") && purposes.contains("guardian") {
                    return Err(Error::Conflict);
                }
            }
            if docs.insert(doc.secret_ref.clone(), doc).is_some() {
                return Err(Error::Conflict);
            }
            evidence.push(verified);
        }
        if docs.len() != secrets.len() {
            return Err(Error::NotFound);
        }
        let mut uses = Vec::with_capacity(expected.len());
        for usage in expected {
            let doc = docs.get(&usage.secret_ref).ok_or(Error::NotFound)?;
            let mut matching = doc.grants.iter().filter(|g| usage.matches(g));
            let grant = matching.next().ok_or(Error::Authorization)?;
            if matching.next().is_some() {
                return Err(Error::Conflict);
            }
            uses.push(usage.with_uid(grant.os_uid));
        }
        Ok(Self {
            candidate_hash: projections.candidate_hash(),
            uses,
            evidence,
            expires_unix_ms,
        })
    }
    pub fn candidate_hash(&self) -> Hash {
        self.candidate_hash
    }
    pub fn uses(&self) -> &[Usage] {
        &self.uses
    }
    pub fn evidence(&self) -> &[VerifiedEvidence] {
        &self.evidence
    }
    pub fn expires_unix_ms(&self) -> u64 {
        self.expires_unix_ms
    }
}

fn text(value: &Value, key: &str) -> Result<String> {
    value[key]
        .as_str()
        .map(str::to_owned)
        .ok_or(Error::Protocol)
}
fn actual_uses(projections: &ProjectionSet) -> Result<((String, String), BTreeSet<Expected>)> {
    let mut scope = None;
    let mut uses = BTreeSet::new();
    for (id, p) in projections.targets() {
        let v = p.value();
        let identity = (
            text(&v["identity"], "cluster_id")?,
            text(&v["identity"], "namespace_id")?,
        );
        if scope.as_ref().is_some_and(|s| s != &identity)
            || p.global_hash() != projections.candidate_hash()
            || v["process"]["id"] != *id
        {
            return Err(Error::Identity);
        }
        scope = Some(identity);
        let bindings = v["secret_bindings"].as_array().ok_or(Error::Protocol)?;
        for b in bindings {
            let u = Expected {
                process_id: id.clone(),
                host_id: text(&v["process"], "host_id")?,
                os_identity: text(&v["process"], "os_identity")?,
                purpose: text(b, "purpose")?,
                account_id: b
                    .get("account_id")
                    .map(|a| a.as_str().map(str::to_owned).ok_or(Error::Protocol))
                    .transpose()?,
                secret_ref: text(b, "ref")?,
            };
            // ProjectionSet can only be built by the role-restricted projector.
            if !uses.insert(u) {
                return Err(Error::Conflict);
            }
            if uses.len() > MAX_ITEMS {
                return Err(Error::Bounds);
            }
        }
    }
    Ok((scope.ok_or(Error::Identity)?, uses))
}
fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}
fn validate_grant(g: &Grant) -> Result<()> {
    if !valid_name(&g.process_id)
        || !valid_name(&g.host_id)
        || !valid_name(&g.os_identity)
        || g.os_uid == u32::MAX
        || g.account_id.as_ref().is_some_and(|a| !valid_name(a))
    {
        return Err(Error::Identity);
    }
    if !matches!(
        g.purpose.as_str(),
        "execution" | "guardian" | "market-data" | "control" | "replication" | "metrics"
    ) || matches!(g.purpose.as_str(), "execution" | "guardian") != g.account_id.is_some()
    {
        return Err(Error::Protocol);
    }
    Ok(())
}
fn fingerprint(value: &str) -> Result<()> {
    let hash = value.strip_prefix("sha256:").ok_or(Error::Digest)?;
    if hash.len() != 64
        || hash.bytes().all(|b| b == b'0')
        || !hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::Digest);
    }
    Ok(())
}
