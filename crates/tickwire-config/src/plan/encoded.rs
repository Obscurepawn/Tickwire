//! Strictly parsed content, with no runtime or commit authority.
use super::{ApplyCeiling, ExposurePolicy, MAX_BYTES, PlanOptions, Precondition};
use crate::{Candidate, Error, Format, Hash, Limits, Result, changes::Class};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[derive(Debug)]
pub struct EncodedPlan {
    canonical: Vec<u8>,
    digest: Hash,
    options: PlanOptions,
    base_revision: u64,
    next_revision: u64,
    base_hash: Hash,
    base_etag: String,
    candidate_hash: Hash,
}
impl EncodedPlan {
    pub fn decode(bytes: &[u8], expected_digest: Hash) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_BYTES {
            return Err(Error::Capacity);
        }
        let parsed = Candidate::parse(
            Format::Json,
            bytes,
            Limits {
                max_bytes: MAX_BYTES,
                max_depth: 16,
            },
        )?;
        if parsed.canonical_bytes() != bytes {
            return Err(invalid());
        }
        let mut digest = Sha256::new();
        digest.update(b"tickwire.config.computed-plan.v1\0");
        digest.update(bytes);
        let digest: Hash = digest.finalize().into();
        if digest != expected_digest {
            return Err(invalid());
        }
        let v = parsed.value();
        fields(
            v,
            &[
                "schema",
                "classification_version",
                "base_revision",
                "next_revision",
                "base_hash",
                "base_etag",
                "candidate_hash",
                "required_class",
                "requires_risk_admin",
                "requires_cluster_admin",
                "affected_accounts",
                "options",
                "targets",
                "preconditions",
            ],
        )?;
        if v["schema"] != 1 || v["classification_version"] != 1 {
            return Err(invalid());
        }
        let base_revision = decimal(&v["base_revision"])?;
        let next_revision = decimal(&v["next_revision"])?;
        if base_revision.checked_add(1) != Some(next_revision) {
            return Err(Error::Revision);
        }
        let base_hash = hash(&v["base_hash"])?;
        let candidate_hash = hash(&v["candidate_hash"])?;
        let base_etag = text(&v["base_etag"])?;
        if base_etag != format!("\"cfg-{}-{}\"", base_revision, &text(&v["base_hash"])?[7..]) {
            return Err(Error::Revision);
        }
        for field in ["requires_risk_admin", "requires_cluster_admin"] {
            v[field].as_bool().ok_or_else(invalid)?;
        }
        strings(&v["affected_accounts"], true)?;
        if conditions(&v["preconditions"])?
            != BTreeSet::from([
                Precondition::CurrentAuthority,
                Precondition::ContentQualification,
                Precondition::RetainedRegistryHead,
            ])
        {
            return Err(invalid());
        }
        let options = options(&v["options"])?;
        let required = class(&v["required_class"])?;
        if !options.apply_ceiling.permits(required) {
            return Err(invalid());
        }
        let targets = v["targets"].as_object().ok_or_else(invalid)?;
        if targets.is_empty() || targets.len() > 4096 {
            return Err(Error::Capacity);
        }
        for id in &options.requested_target_ids {
            if !targets.contains_key(id) {
                return Err(invalid());
            }
        }
        for (id, target) in targets {
            if !name(id) {
                return Err(invalid());
            }
            check_target(target, base_hash, candidate_hash, required)?;
        }
        Ok(Self {
            canonical: bytes.to_vec(),
            digest,
            options,
            base_revision,
            next_revision,
            base_hash,
            base_etag: base_etag.to_owned(),
            candidate_hash,
        })
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }
    pub fn digest(&self) -> Hash {
        self.digest
    }
    pub fn options(&self) -> &PlanOptions {
        &self.options
    }
    pub fn base_revision(&self) -> u64 {
        self.base_revision
    }
    pub fn next_revision(&self) -> u64 {
        self.next_revision
    }
    pub fn base_hash(&self) -> Hash {
        self.base_hash
    }
    pub fn base_etag(&self) -> &str {
        &self.base_etag
    }
    pub fn candidate_hash(&self) -> Hash {
        self.candidate_hash
    }
}

fn options(v: &Value) -> Result<PlanOptions> {
    fields(
        v,
        &["apply_ceiling", "exposure_policy", "requested_target_ids"],
    )?;
    let apply_ceiling = match text(&v["apply_ceiling"])? {
        "Hot" => ApplyCeiling::Hot,
        "Quiesce" => ApplyCeiling::Quiesce,
        "RollingRestart" => ApplyCeiling::RollingRestart,
        _ => return Err(invalid()),
    };
    let exposure_policy = match text(&v["exposure_policy"])? {
        "RejectIfExceeded" => ExposurePolicy::RejectIfExceeded,
        "FreezeUntilWithin" => ExposurePolicy::FreezeUntilWithin,
        "CancelThenFreeze" => ExposurePolicy::CancelThenFreeze,
        _ => return Err(invalid()),
    };
    Ok(PlanOptions {
        apply_ceiling,
        exposure_policy,
        requested_target_ids: strings(&v["requested_target_ids"], true)?
            .into_iter()
            .map(str::to_owned)
            .collect(),
    })
}
fn check_target(v: &Value, base: Hash, next: Hash, maximum: Class) -> Result<()> {
    fields(
        v,
        &[
            "action",
            "required_class",
            "old_projection",
            "new_projection",
            "old_host",
            "new_host",
            "accounts",
            "changes",
            "preconditions",
        ],
    )?;
    let old = projection(&v["old_projection"], base)?;
    let new = projection(&v["new_projection"], next)?;
    for (present, field) in [(old.is_some(), "old_host"), (new.is_some(), "new_host")] {
        if present {
            if !name(text(&v[field])?) {
                return Err(invalid());
            }
        } else if !v[field].is_null() {
            return Err(invalid());
        }
    }
    if old.is_none() && new.is_none() {
        return Err(invalid());
    }
    strings(&v["accounts"], true)?;
    let changes = strings(&v["changes"], false)?;
    for path in &changes {
        if !pointer(path) {
            return Err(invalid());
        }
    }
    let required = if v["required_class"].is_null() {
        None
    } else {
        Some(class(&v["required_class"])?)
    };
    if required.is_some_and(|c| c > maximum || c == Class::ForbiddenRuntime) {
        return Err(invalid());
    }
    let conditions = conditions(&v["preconditions"])?;
    if !conditions.contains(&Precondition::RuntimeIdentity)
        || !conditions.contains(&Precondition::RuntimeCapability)
        || old.is_some() != conditions.contains(&Precondition::EffectiveBaseline)
    {
        return Err(invalid());
    }
    match text(&v["action"])? {
        "NoOpRevisionAdvance"
            if required.is_none() && old.is_some() && old == new && changes.is_empty() => {}
        "Hot" if required == Some(Class::Hot) && old.is_some() && new.is_some() => {}
        "Quiesce"
            if required == Some(Class::Quiesce)
                && old.is_some()
                && new.is_some()
                && conditions.contains(&Precondition::Quiescence) => {}
        "RollingRestart"
            if required == Some(Class::RollingRestart)
                && new.is_some()
                && conditions.contains(&Precondition::PlannedSuccessor)
                && (old.is_none() || conditions.contains(&Precondition::Quiescence)) => {}
        "Retire"
            if required == Some(Class::RollingRestart)
                && old.is_some()
                && new.is_none()
                && conditions.contains(&Precondition::Retirement)
                && conditions.contains(&Precondition::Quiescence) => {}
        _ => return Err(invalid()),
    }
    Ok(())
}
fn projection(v: &Value, global: Hash) -> Result<Option<(Hash, u64)>> {
    if v.is_null() {
        return Ok(None);
    }
    fields(v, &["global_hash", "projection_hash", "policy_revision"])?;
    if hash(&v["global_hash"])? != global {
        return Err(invalid());
    }
    Ok(Some((
        hash(&v["projection_hash"])?,
        decimal(&v["policy_revision"])?,
    )))
}
fn conditions(v: &Value) -> Result<BTreeSet<Precondition>> {
    let mut result = BTreeSet::new();
    let mut previous = None;
    for value in v.as_array().ok_or_else(invalid)? {
        let item = match text(value)? {
            "CurrentAuthority" => Precondition::CurrentAuthority,
            "ContentQualification" => Precondition::ContentQualification,
            "RetainedRegistryHead" => Precondition::RetainedRegistryHead,
            "RuntimeIdentity" => Precondition::RuntimeIdentity,
            "EffectiveBaseline" => Precondition::EffectiveBaseline,
            "RuntimeCapability" => Precondition::RuntimeCapability,
            "Exposure" => Precondition::Exposure,
            "Quiescence" => Precondition::Quiescence,
            "PlannedSuccessor" => Precondition::PlannedSuccessor,
            "Retirement" => Precondition::Retirement,
            "DirectoryMigration" => Precondition::DirectoryMigration,
            "CredentialHandoff" => Precondition::CredentialHandoff,
            _ => return Err(invalid()),
        };
        if previous.is_some_and(|last| last >= item) {
            return Err(invalid());
        }
        previous = Some(item);
        result.insert(item);
    }
    Ok(result)
}
fn strings(v: &Value, names: bool) -> Result<Vec<&str>> {
    let values = v.as_array().ok_or_else(invalid)?;
    let mut result = Vec::with_capacity(values.len());
    let mut previous = None;
    for v in values {
        let s = text(v)?;
        if names && !name(s) || previous.is_some_and(|last| last >= s) {
            return Err(invalid());
        }
        result.push(s);
        previous = Some(s);
    }
    Ok(result)
}
fn class(v: &Value) -> Result<Class> {
    match text(v)? {
        "Hot" => Ok(Class::Hot),
        "Quiesce" => Ok(Class::Quiesce),
        "RollingRestart" => Ok(Class::RollingRestart),
        "ForbiddenRuntime" => Ok(Class::ForbiddenRuntime),
        _ => Err(invalid()),
    }
}
fn fields(v: &Value, expected: &[&str]) -> Result<()> {
    let object = v.as_object().ok_or_else(invalid)?;
    if object.len() != expected.len() || expected.iter().any(|key| !object.contains_key(*key)) {
        return Err(invalid());
    }
    Ok(())
}
fn decimal(v: &Value) -> Result<u64> {
    let s = text(v)?;
    let value = s.parse::<u64>().map_err(|_| Error::Revision)?;
    if value == 0 || value >= 1 << 63 || value.to_string() != s {
        return Err(Error::Revision);
    }
    Ok(value)
}
fn hash(v: &Value) -> Result<Hash> {
    let raw = text(v)?.strip_prefix("sha256:").ok_or_else(invalid)?;
    if raw.len() != 64
        || !raw
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid());
    }
    let mut hash = [0; 32];
    for (i, byte) in hash.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&raw[2 * i..2 * i + 2], 16).map_err(|_| invalid())?;
    }
    Ok(hash)
}
fn text(v: &Value) -> Result<&str> {
    v.as_str().ok_or_else(invalid)
}
fn name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.as_bytes()[0].is_ascii_lowercase()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
fn pointer(path: &str) -> bool {
    if !path.starts_with('/') || path.len() == 1 {
        return false;
    }
    for part in path[1..].split('/') {
        if part.is_empty() {
            return false;
        }
        let mut bytes = part.bytes();
        while let Some(byte) = bytes.next() {
            if byte == b'~' && !matches!(bytes.next(), Some(b'0' | b'1')) {
                return false;
            }
        }
    }
    true
}
fn invalid() -> Error {
    Error::Semantic("plan encoding")
}
