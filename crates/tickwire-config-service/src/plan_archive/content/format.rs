use super::*;
use serde::{Deserialize, Serialize};
use std::io::{self, Write};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Reference {
    pub key: Identity,
    pub owner: Identity,
    pub purpose: u8,
    pub length: u32,
    pub hash: Hash,
}
impl Reference {
    pub fn spec(&self) -> Result<ObjectSpec> {
        let spec = ObjectSpec {
            key: self.key,
            owner: self.owner,
            purpose: Purpose::from_byte(self.purpose)?,
            length: self.length,
            hash: self.hash,
        };
        spec.validate()?;
        if spec.key != object_key(spec.owner, spec.purpose, spec.hash) {
            return Err(Error::Identity);
        }
        Ok(spec)
    }
}
impl From<&ObjectSpec> for Reference {
    fn from(spec: &ObjectSpec) -> Self {
        Self {
            key: spec.key,
            owner: spec.owner,
            purpose: spec.purpose as u8,
            length: spec.length,
            hash: spec.hash,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProjectionRef {
    pub object: Reference,
    pub policy_revision: String,
    pub projection_hash: Hash,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Target {
    pub old: Option<ProjectionRef>,
    pub next: ProjectionRef,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum InputKind {
    Full,
    Patch,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Limits {
    account_max_age_ms: u64,
    credential_max_age_ms: u64,
    resource_max_age_ms: u64,
    projection_max_targets: usize,
    projection_max_total_bytes: usize,
}
impl From<deployment::Limits> for Limits {
    fn from(v: deployment::Limits) -> Self {
        Self {
            account_max_age_ms: v.accounts.max_age_ms,
            credential_max_age_ms: v.credentials.max_age_ms,
            resource_max_age_ms: v.resources.max_age_ms,
            projection_max_targets: v.projections.max_targets,
            projection_max_total_bytes: v.projections.max_total_bytes,
        }
    }
}
impl Limits {
    pub fn decode(self) -> Result<deployment::Limits> {
        if [
            self.account_max_age_ms,
            self.credential_max_age_ms,
            self.resource_max_age_ms,
        ]
        .iter()
        .any(|v| !(1..=60_000).contains(v))
            || !(1..=MAX_OBJECTS).contains(&self.projection_max_targets)
            || !(1..=MAX_BYTES).contains(&self.projection_max_total_bytes)
        {
            return Err(Error::Bounds);
        }
        Ok(deployment::Limits {
            accounts: crate::account_facts::Limits {
                max_age_ms: self.account_max_age_ms,
            },
            credentials: crate::credential_facts::Limits {
                max_age_ms: self.credential_max_age_ms,
            },
            resources: crate::resource_facts::Limits {
                max_age_ms: self.resource_max_age_ms,
            },
            projections: tickwire_config::delivery::ProjectionLimits {
                max_targets: self.projection_max_targets,
                max_total_bytes: self.projection_max_total_bytes,
            },
        })
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Manifest {
    pub schema: u16,
    pub id: Identity,
    pub subject: String,
    pub leaf_sha256: Hash,
    pub request_digest: Hash,
    pub if_match: String,
    pub base_revision: String,
    pub base_index: String,
    pub base_term: String,
    pub base: Reference,
    pub candidate: Reference,
    pub input_kind: InputKind,
    pub input: Reference,
    pub plan: Reference,
    pub plan_digest: Hash,
    pub rules_digest: Hash,
    pub trust: Reference,
    pub trust_digest: Hash,
    pub bundle: Vec<Reference>,
    pub bundle_length: u64,
    pub bundle_hash: Hash,
    pub checked_unix_ms: u64,
    pub expires_unix_ms: u64,
    pub sources_digest: Hash,
    pub limits: Limits,
    pub targets: BTreeMap<String, Target>,
}
impl Manifest {
    pub fn bytes(&self, limit: usize) -> Result<Vec<u8>> {
        let mut writer = Bounded {
            bytes: Vec::new(),
            limit: limit.min(Purpose::PlanMetadata.max_bytes() as usize),
        };
        serde_json::to_writer(&mut writer, self).map_err(|_| Error::Bounds)?;
        Ok(
            parse(&writer.bytes, Purpose::PlanMetadata.max_bytes() as usize)?
                .canonical_bytes()
                .to_vec(),
        )
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let canonical = parse(bytes, Purpose::PlanMetadata.max_bytes() as usize)?;
        if canonical.canonical_bytes() != bytes {
            return Err(Error::Protocol);
        }
        let manifest: Self =
            serde_json::from_value(canonical.value().clone()).map_err(|_| Error::Protocol)?;
        if manifest.schema != 1 || manifest.bytes(bytes.len())? != bytes {
            return Err(Error::Protocol);
        }
        Ok(manifest)
    }
    pub fn references(&self) -> Vec<&Reference> {
        let mut refs = vec![
            &self.input,
            &self.base,
            &self.candidate,
            &self.plan,
            &self.trust,
        ];
        refs.extend(&self.bundle);
        for target in self.targets.values() {
            if let Some(old) = &target.old {
                refs.push(&old.object);
            }
            refs.push(&target.next.object);
        }
        refs
    }
}
struct Bounded {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.limit)
        {
            return Err(io::Error::other("archive metadata budget"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(super) fn parse(bytes: &[u8], max_bytes: usize) -> Result<Candidate> {
    Candidate::parse(
        Format::Json,
        bytes,
        tickwire_config::Limits {
            max_bytes,
            max_depth: 32,
        },
    )
    .map_err(Error::Configuration)
}
pub(super) fn decimal(text: &str) -> Result<u64> {
    let value = text.parse::<u64>().map_err(|_| Error::Protocol)?;
    if value == 0 || value.to_string() != text {
        return Err(Error::Protocol);
    }
    Ok(value)
}
pub(super) fn object_key(id: Identity, purpose: Purpose, hash: Hash) -> Identity {
    let mut digest = Sha256::new();
    digest.update(b"tickwire.config.plan-archive-object.v1\0");
    digest.update(id);
    digest.update([purpose as u8]);
    digest.update(hash);
    let full: Hash = digest.finalize().into();
    let mut key = [0; 16];
    key.copy_from_slice(&full[..16]);
    key
}
pub(super) fn hash_text(hash: Hash) -> String {
    format!(
        "sha256:{}",
        hash.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}
