//! Immutable plan inputs and outputs. This module grants no runtime authority.
use crate::{
    Error, Hash, Identity, ObjectSpec, Purpose, Result,
    bootstrap::CommittedHead,
    deployment::{self, ContentValidated},
    evidence::Trust,
    objects,
    publication::RecordPrepared,
    source_bundle::Bundle,
};
use tickwire_config::{
    Candidate, Format,
    delivery::ProjectionDelivery,
    plan::ComputedPlan,
    prepared::{Input, PreparedCandidate},
};

pub(crate) struct Capture<'a> {
    pub id: Identity,
    pub subject: &'a str,
    pub leaf_sha256: Hash,
    pub request_digest: Hash,
    pub if_match: &'a str,
    pub base: &'a CommittedHead,
    pub prepared: &'a PreparedCandidate,
    pub input: Input<'a>,
    pub plan: &'a ComputedPlan,
    pub delivery: &'a ProjectionDelivery,
    pub content: &'a ContentValidated,
    pub bundle: &'a Bundle,
    pub trust: &'a Trust,
    pub limits: deployment::Limits,
}
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use tickwire_config::{plan::EncodedPlan, projection_object::ProjectionObject};
mod format;
mod verify;
use format::*;
const MAX_OBJECTS: usize = 4096;
const MAX_BYTES: usize = 512 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct ArchivedObject {
    spec: ObjectSpec,
    bytes: Vec<u8>,
}
impl ArchivedObject {
    pub(crate) fn spec(&self) -> &ObjectSpec {
        &self.spec
    }
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
pub(crate) struct ContentArchive {
    root: ObjectSpec,
    objects: Vec<ArchivedObject>,
}
impl ContentArchive {
    pub(crate) fn capture(input: Capture<'_>) -> Result<Self> {
        let Capture {
            id,
            subject,
            leaf_sha256,
            request_digest,
            if_match,
            base,
            prepared,
            input,
            plan,
            delivery,
            content,
            bundle,
            trust,
            limits,
        } = input;
        if id == [0; 16]
            || !crate::auth::valid_subject(subject)
            || leaf_sha256 == [0; 32]
            || request_digest == [0; 32]
        {
            return Err(Error::Identity);
        }
        if if_match != base.etag()
            || plan.base_revision() != base.revision()
            || plan.base_hash() != base.candidate().hash()
            || prepared.base_hash() != plan.base_hash()
            || prepared.candidate().hash() != plan.candidate_hash()
            || content.candidate_hash() != plan.candidate_hash()
        {
            return Err(Error::Conflict);
        }
        let mut builder = Builder::new(id);
        let (kind, input_bytes) = match input {
            Input::Full(candidate) => (InputKind::Full, candidate.canonical_bytes()),
            Input::Patch(patch) => (InputKind::Patch, patch.canonical_bytes()),
        };
        let input = builder.add(Purpose::Candidate, input_bytes)?;
        let base_ref = builder.add(Purpose::Candidate, base.candidate().canonical_bytes())?;
        let candidate = builder.add(Purpose::Candidate, prepared.candidate().canonical_bytes())?;
        let plan_ref = builder.add(Purpose::PlanMetadata, plan.canonical_bytes())?;
        let trust_ref = builder.add(Purpose::Qualification, trust.canonical_bytes())?;
        let mut targets = BTreeMap::new();
        for (id, target) in delivery.targets() {
            let old = target
                .old_projection()
                .map(|projection| {
                    let object = ProjectionObject::build(
                        base.revision(),
                        projection,
                        Purpose::Projection.max_bytes() as usize,
                    )
                    .map_err(Error::Configuration)?;
                    builder.projection(&object)
                })
                .transpose()?;
            let next = ProjectionObject::for_delivery(
                plan.next_revision(),
                delivery,
                id,
                Purpose::Projection.max_bytes() as usize,
            )
            .map_err(Error::Configuration)?;
            targets.insert(
                id.clone(),
                Target {
                    old,
                    next: builder.projection(&next)?,
                },
            );
        }
        let bundle_bytes = bundle.encode(bundle_limits(builder.remaining()))?;
        let bundle_hash = Sha256::digest(&bundle_bytes).into();
        let bundle_length = bundle_bytes.len() as u64;
        let bundle = bundle_bytes
            .chunks(Purpose::Qualification.max_bytes() as usize)
            .map(|chunk| builder.add(Purpose::Qualification, chunk))
            .collect::<Result<_>>()?;
        let manifest = Manifest {
            schema: 1,
            id,
            subject: subject.into(),
            leaf_sha256,
            request_digest,
            if_match: if_match.into(),
            base_revision: base.revision().to_string(),
            base_index: base.raft_index().to_string(),
            base_term: base.raft_term().to_string(),
            base: base_ref,
            candidate,
            input_kind: kind,
            input,
            plan: plan_ref,
            plan_digest: plan.digest(),
            rules_digest: tickwire_config::plan::rules_digest(),
            trust: trust_ref,
            trust_digest: trust.digest(),
            bundle,
            bundle_length,
            bundle_hash,
            checked_unix_ms: content.checked_unix_ms(),
            expires_unix_ms: content.expires_unix_ms(),
            sources_digest: content.sources_digest(),
            limits: limits.into(),
            targets,
        };
        let bytes = manifest.bytes(builder.remaining())?;
        let root = builder.add(Purpose::PlanMetadata, &bytes)?.spec()?;
        let archive = Self {
            root,
            objects: builder.objects,
        };
        archive.validate(&manifest)?;
        archive.reverify()?;
        Ok(archive)
    }
    pub(crate) fn objects(&self) -> &[ArchivedObject] {
        &self.objects
    }
    pub(crate) fn root(&self) -> &ObjectSpec {
        &self.root
    }
    pub(crate) fn reopen(
        record: &RecordPrepared,
        objects: &objects::State,
        head: &CommittedHead,
    ) -> Result<Self> {
        record.validate()?;
        let root = objects.object(record.root.key).ok_or(Error::NotFound)?;
        if root.spec() != &record.root {
            return Err(Error::Identity);
        }
        let manifest = Manifest::decode(root.bytes())?;
        if manifest.id != record.id
            || manifest.subject != record.subject
            || manifest.leaf_sha256 != record.leaf_sha256
            || manifest.request_digest != record.request_digest
            || manifest.if_match != record.if_match
            || manifest.if_match != head.etag()
            || decimal(&manifest.base_revision)? != head.revision()
            || decimal(&manifest.base_index)? != head.raft_index()
            || decimal(&manifest.base_term)? != head.raft_term()
            || manifest.base.hash != head.candidate().hash()
            || manifest.checked_unix_ms > record.admitted_unix_ms
            || record.admitted_unix_ms >= manifest.expires_unix_ms
        {
            return Err(Error::Conflict);
        }
        let mut specs = BTreeMap::new();
        let mut total = 0usize;
        for reference in manifest
            .references()
            .into_iter()
            .chain(std::iter::once(&Reference::from(&record.root)))
        {
            let spec = reference.spec()?;
            if spec.owner != record.id {
                return Err(Error::Identity);
            }
            if let Some(previous) = specs.get(&spec.key) {
                if previous != &spec {
                    return Err(Error::Conflict);
                }
            } else {
                total = total
                    .checked_add(spec.length as usize)
                    .ok_or(Error::Capacity)?;
                if total > MAX_BYTES || specs.len() >= MAX_OBJECTS {
                    return Err(Error::Capacity);
                }
                specs.insert(spec.key, spec);
            }
        }
        let mut builder = Builder::new(record.id);
        for spec in specs.values().filter(|spec| spec.key != record.root.key) {
            let object = objects.object(spec.key).ok_or(Error::NotFound)?;
            if object.spec() != spec {
                return Err(Error::Identity);
            }
            builder.add(spec.purpose, object.bytes())?;
        }
        builder.add(record.root.purpose, root.bytes())?;
        let archive = Self {
            root: record.root.clone(),
            objects: builder.objects,
        };
        archive.validate(&manifest)?;
        Ok(archive)
    }
    pub(crate) fn reverify(&self) -> Result<ComputedPlan> {
        let manifest = Manifest::decode(self.bytes(&Reference::from(&self.root))?)?;
        if manifest.rules_digest != tickwire_config::plan::rules_digest() {
            return Err(Error::Conflict);
        }
        let base = parse(
            self.bytes(&manifest.base)?,
            Purpose::Candidate.max_bytes() as usize,
        )?;
        let input = parse(
            self.bytes(&manifest.input)?,
            Purpose::Candidate.max_bytes() as usize,
        )?;
        let patch = if matches!(manifest.input_kind, InputKind::Patch) {
            Some(
                tickwire_config::patch::ScopedPatch::parse(
                    input.canonical_bytes(),
                    tickwire_config::Limits::default(),
                )
                .map_err(Error::Configuration)?,
            )
        } else {
            None
        };
        let source = match &patch {
            Some(patch) => Input::Patch(patch),
            None => Input::Full(&input),
        };
        let authority = crate::authorization::RoleBindings::from_candidate(&base)?
            .historical_authority(&manifest.subject)?;
        let revision = decimal(&manifest.base_revision)?;
        let prepared = PreparedCandidate::prepare(
            &tickwire_config::patch::Head::new(revision, &base).map_err(Error::Configuration)?,
            &manifest.if_match,
            &base,
            source,
            &authority,
            tickwire_config::Limits::default(),
        )
        .map_err(Error::Configuration)?;
        if prepared.candidate().canonical_bytes() != self.bytes(&manifest.candidate)? {
            return Err(Error::Conflict);
        }
        let bundle_bytes = self.bundle(&manifest)?;
        let bundle = Bundle::decode(&bundle_bytes, bundle_limits(MAX_BYTES))?;
        let trust = Trust::decode(self.bytes(&manifest.trust)?, manifest.trust_digest)?;
        let limits = manifest.limits.decode()?;
        let content = ContentValidated::check(
            prepared.candidate(),
            bundle.sources(),
            &trust,
            manifest.checked_unix_ms,
            limits,
        )?;
        if content.sources_digest() != manifest.sources_digest
            || content.expires_unix_ms() != manifest.expires_unix_ms
        {
            return Err(Error::Conflict);
        }
        let delivery = ProjectionDelivery::prepare(
            &base,
            &prepared,
            bundle.sources().artifacts,
            limits.projections,
        )
        .map_err(Error::Configuration)?;
        let encoded = EncodedPlan::decode(self.bytes(&manifest.plan)?, manifest.plan_digest)
            .map_err(Error::Configuration)?;
        let plan = ComputedPlan::verify_encoded(&encoded, revision, &base, &prepared, &delivery)
            .map_err(Error::Configuration)?;
        if delivery.targets().keys().ne(manifest.targets.keys()) {
            return Err(Error::Conflict);
        }
        for (id, target) in delivery.targets() {
            let stored = &manifest.targets[id];
            match (target.old_projection(), &stored.old) {
                (None, None) => {}
                (Some(projection), Some(reference)) => {
                    let expected = ProjectionObject::build(
                        revision,
                        projection,
                        Purpose::Projection.max_bytes() as usize,
                    )
                    .map_err(Error::Configuration)?;
                    if expected.bytes() != self.bytes(&reference.object)? {
                        return Err(Error::Conflict);
                    }
                }
                _ => return Err(Error::Conflict),
            }
            let expected = ProjectionObject::for_delivery(
                plan.next_revision(),
                &delivery,
                id,
                Purpose::Projection.max_bytes() as usize,
            )
            .map_err(Error::Configuration)?;
            if expected.bytes() != self.bytes(&stored.next.object)? {
                return Err(Error::Conflict);
            }
        }
        Ok(plan)
    }
    fn bytes(&self, reference: &Reference) -> Result<&[u8]> {
        let spec = reference.spec()?;
        let object = self
            .objects
            .iter()
            .find(|object| object.spec.key == spec.key)
            .ok_or(Error::NotFound)?;
        if object.spec != spec {
            return Err(Error::Identity);
        }
        Ok(&object.bytes)
    }
    fn bundle(&self, manifest: &Manifest) -> Result<Vec<u8>> {
        let length = usize::try_from(manifest.bundle_length).map_err(|_| Error::Bounds)?;
        if length > MAX_BYTES {
            return Err(Error::Bounds);
        }
        let mut bytes = Vec::with_capacity(length);
        for reference in &manifest.bundle {
            bytes.extend_from_slice(self.bytes(reference)?);
        }
        if bytes.len() != length || <Hash>::from(Sha256::digest(&bytes)) != manifest.bundle_hash {
            return Err(Error::Digest);
        }
        Ok(bytes)
    }
}
pub(crate) fn validate_record(
    record: &RecordPrepared,
    objects: &objects::State,
    head: &CommittedHead,
) -> Result<()> {
    ContentArchive::reopen(record, objects, head).map(|_| ())
}
#[cfg(test)]
pub(crate) mod tests;

struct Builder {
    id: Identity,
    objects: Vec<ArchivedObject>,
    keys: BTreeMap<Identity, usize>,
    bytes: usize,
}
impl Builder {
    fn new(id: Identity) -> Self {
        Self {
            id,
            objects: Vec::new(),
            keys: BTreeMap::new(),
            bytes: 0,
        }
    }
    fn remaining(&self) -> usize {
        MAX_BYTES - self.bytes
    }
    fn add(&mut self, purpose: Purpose, bytes: &[u8]) -> Result<Reference> {
        let hash = Sha256::digest(bytes).into();
        let spec = ObjectSpec {
            key: object_key(self.id, purpose, hash),
            owner: self.id,
            purpose,
            length: u32::try_from(bytes.len()).map_err(|_| Error::Bounds)?,
            hash,
        };
        spec.validate()?;
        if let Some(index) = self.keys.get(&spec.key) {
            let old = &self.objects[*index];
            if old.spec != spec || old.bytes != bytes {
                return Err(Error::Conflict);
            }
            return Ok(Reference::from(&spec));
        }
        let total = self.bytes.checked_add(bytes.len()).ok_or(Error::Capacity)?;
        if total > MAX_BYTES || self.objects.len() >= MAX_OBJECTS {
            return Err(Error::Capacity);
        }
        self.bytes = total;
        self.keys.insert(spec.key, self.objects.len());
        self.objects.push(ArchivedObject {
            spec: spec.clone(),
            bytes: bytes.to_vec(),
        });
        Ok(Reference::from(&spec))
    }
    fn projection(&mut self, object: &ProjectionObject) -> Result<ProjectionRef> {
        Ok(ProjectionRef {
            object: self.add(Purpose::Projection, object.bytes())?,
            policy_revision: object.binding().policy_revision.to_string(),
            projection_hash: object.binding().projection_hash,
        })
    }
}
fn bundle_limits(max: usize) -> crate::source_bundle::Limits {
    crate::source_bundle::Limits {
        max_bundle_bytes: max.min(528 * 1024 * 1024 + 12),
        max_header_bytes: 16 * 1024 * 1024,
        artifacts: tickwire_config::artifacts::CatalogLimits::default(),
    }
}
