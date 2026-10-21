//! Complete desired projection sets. Runtime observations and authority are separate.
use crate::{
    Candidate, Error, Hash, Result,
    artifacts::ArtifactCatalog,
    prepared::PreparedCandidate,
    projection::{PinnedPolicy, Projection, project},
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy)]
pub struct ProjectionLimits {
    pub max_targets: usize,
    pub max_total_bytes: usize,
}
impl Default for ProjectionLimits {
    fn default() -> Self {
        Self {
            max_targets: 4096,
            max_total_bytes: 512 * 1024 * 1024,
        }
    }
}
impl ProjectionLimits {
    fn validate(self) -> Result<()> {
        if !(1..=4096).contains(&self.max_targets)
            || !(1..=512 * 1024 * 1024).contains(&self.max_total_bytes)
        {
            return Err(Error::Capacity);
        }
        Ok(())
    }
}
pub struct ProjectionSet {
    candidate_hash: Hash,
    targets: BTreeMap<String, Projection>,
    bytes: usize,
}
impl ProjectionSet {
    pub fn build(
        candidate: &Candidate,
        catalog: &ArtifactCatalog,
        limits: ProjectionLimits,
    ) -> Result<Self> {
        limits.validate()?;
        candidate.validate_static()?;
        let ids: BTreeSet<_> = candidate.value()["processes"]
            .as_array()
            .ok_or(Error::Schema)?
            .iter()
            .map(|p| p["id"].as_str().ok_or(Error::Schema))
            .collect::<Result<_>>()?;
        if ids.len() > limits.max_targets {
            return Err(Error::Capacity);
        }
        let reference = candidate.value()["local_config"]["projection_policy_ref"]
            .as_str()
            .ok_or(Error::Schema)?;
        let policy = PinnedPolicy::decode(
            reference,
            catalog
                .get(reference)
                .ok_or(Error::Semantic("missing projection policy"))?,
        )?;
        if policy.process_ids().collect::<BTreeSet<_>>() != ids {
            return Err(Error::Semantic("projection target set"));
        }
        let mut targets = BTreeMap::new();
        let mut bytes = 0usize;
        for id in ids {
            let projection = project(candidate, id, &policy)?;
            bytes = bytes
                .checked_add(projection.canonical_bytes().len())
                .ok_or(Error::Capacity)?;
            if bytes > limits.max_total_bytes {
                return Err(Error::Capacity);
            }
            targets.insert(id.to_owned(), projection);
        }
        Ok(Self {
            candidate_hash: candidate.hash(),
            targets,
            bytes,
        })
    }
    pub fn candidate_hash(&self) -> Hash {
        self.candidate_hash
    }
    pub fn targets(&self) -> &BTreeMap<String, Projection> {
        &self.targets
    }
    pub fn stored_bytes(&self) -> usize {
        self.bytes
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryKind {
    Added,
    Retired,
    Changed,
    RevisionOnly,
}
pub struct DeliveryTarget {
    old: Option<Projection>,
    new: Option<Projection>,
    kind: DeliveryKind,
}
impl DeliveryTarget {
    pub fn old_projection(&self) -> Option<&Projection> {
        self.old.as_ref()
    }
    pub fn new_projection(&self) -> Option<&Projection> {
        self.new.as_ref()
    }
    pub fn kind(&self) -> DeliveryKind {
        self.kind
    }
}
pub struct ProjectionDelivery {
    base_hash: Hash,
    candidate_hash: Hash,
    targets: BTreeMap<String, DeliveryTarget>,
    bytes: usize,
}
impl ProjectionDelivery {
    pub fn prepare(
        base: &Candidate,
        prepared: &PreparedCandidate,
        catalog: &ArtifactCatalog,
        limits: ProjectionLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if base.hash() != prepared.base_hash() {
            return Err(Error::Revision);
        }
        if prepared.changes().delivery_targets().len() > limits.max_targets {
            return Err(Error::Capacity);
        }
        let mut old = ProjectionSet::build(base, catalog, limits)?;
        let mut new = ProjectionSet::build(
            prepared.candidate(),
            catalog,
            ProjectionLimits {
                max_total_bytes: limits
                    .max_total_bytes
                    .checked_sub(old.bytes)
                    .ok_or(Error::Capacity)?,
                ..limits
            },
        )?;
        let mut targets = BTreeMap::new();
        for id in prepared.changes().delivery_targets() {
            let old = old.targets.remove(id);
            let new = new.targets.remove(id);
            let kind = match (&old, &new) {
                (None, Some(_)) => DeliveryKind::Added,
                (Some(_), None) => DeliveryKind::Retired,
                (Some(a), Some(b))
                    if a.hash() == b.hash() && a.policy_revision() == b.policy_revision() =>
                {
                    DeliveryKind::RevisionOnly
                }
                (Some(_), Some(_)) => DeliveryKind::Changed,
                (None, None) => return Err(Error::Semantic("projection target set")),
            };
            targets.insert(id.clone(), DeliveryTarget { old, new, kind });
        }
        if !old.targets.is_empty() || !new.targets.is_empty() {
            return Err(Error::Semantic("projection target set"));
        }
        Ok(Self {
            base_hash: base.hash(),
            candidate_hash: prepared.candidate().hash(),
            targets,
            bytes: old.bytes.checked_add(new.bytes).ok_or(Error::Capacity)?,
        })
    }
    pub fn base_hash(&self) -> Hash {
        self.base_hash
    }
    pub fn candidate_hash(&self) -> Hash {
        self.candidate_hash
    }
    pub fn targets(&self) -> &BTreeMap<String, DeliveryTarget> {
        &self.targets
    }
    pub fn stored_bytes(&self) -> usize {
        self.bytes
    }
}
