//! Same-owner incremental archive upload and durable admission of content only.
use crate::{
    AppliedResult, CHUNK_BYTES, Change, Error, Hash, Identity, ObjectCommand, ReplicaControl,
    Result,
    auth::{AuthenticatedPrincipal, ServerTls},
    authorization::RoleBindings,
    deployment_owner::{ArchiveAssessment, ArchiveJob},
    planning::PlanDraft,
    publication::{PreparedQuery, PreparedRead, RecordPrepared, StoredPrepared},
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tickwire_control_raft::{Proposal, ReadId};
use tickwire_journal::store::Store;

pub(crate) struct ArchiveRequest {
    pub id: Identity,
    pub subject: String,
    pub leaf_sha256: Hash,
    pub request_digest: Hash,
    pub if_match: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordProgress {
    ReadingExisting,
    Building,
    Uploading,
    Rechecking,
    Recording,
    ReadingResult,
    Completed,
}
enum Stage {
    ReadingExisting(Box<PlanDraft>),
    Building(Box<ArchiveJob>),
    Uploading(Box<ArchiveAssessment>, Upload),
    Rechecking(Box<ArchiveAssessment>),
    Recording,
    ReadingResult,
    Completed,
}
#[derive(Default)]
struct Upload {
    object: usize,
    offset: usize,
    began: bool,
}
struct Awaited {
    proposal: Proposal,
    result: Option<Result<Change>>,
}
/// Feed actual driver results through `observe`, poll to completion, and cancel
/// explicitly on the originating replica if abandoning the request.
#[must_use]
pub struct PendingRecord {
    owner: Arc<()>,
    principal: AuthenticatedPrincipal,
    request: ArchiveRequest,
    key: String,
    accounts: std::collections::BTreeSet<String>,
    cluster_scope: bool,
    ticket: Option<ReadId>,
    waiting: Option<Awaited>,
    stage: Stage,
}
impl PendingRecord {
    pub fn progress(&self) -> RecordProgress {
        match self.stage {
            Stage::ReadingExisting(_) => RecordProgress::ReadingExisting,
            Stage::Building(_) => RecordProgress::Building,
            Stage::Uploading(..) => RecordProgress::Uploading,
            Stage::Rechecking(_) => RecordProgress::Rechecking,
            Stage::Recording => RecordProgress::Recording,
            Stage::ReadingResult => RecordProgress::ReadingResult,
            Stage::Completed => RecordProgress::Completed,
        }
    }
    pub fn observe(&mut self, applied: &AppliedResult) -> Result<bool> {
        let Some(waiting) = &mut self.waiting else {
            return Ok(false);
        };
        if waiting.result.is_some() || !applied.matches(&waiting.proposal) {
            return Ok(false);
        }
        waiting.result = Some(applied.result());
        Ok(true)
    }
}
impl<S: Store> ReplicaControl<'_, S> {
    pub fn begin_record_plan(
        &mut self,
        trust: &ServerTls,
        principal: &AuthenticatedPrincipal,
        draft: PlanDraft,
        key: String,
    ) -> Result<PendingRecord> {
        trust.revalidate(principal)?;
        let owner = self.planning_identity()?;
        if !Arc::ptr_eq(&owner, draft.owner())
            || principal.subject() != draft.subject()
            || principal.leaf_sha256() != draft.leaf_sha256()
        {
            return Err(Error::Identity);
        }
        if !(1..=128).contains(&key.len()) || !key.bytes().all(|b| (33..=126).contains(&b)) {
            return Err(Error::Bounds);
        }
        let mut identity = Sha256::new();
        identity.update(b"tickwire.config.prepared-id.v1\0");
        frame(&mut identity, principal.subject().as_bytes());
        frame(&mut identity, key.as_bytes());
        let id: Identity = identity.finalize()[..16]
            .try_into()
            .map_err(|_| Error::Identity)?;
        if id == [0; 16] {
            return Err(Error::Identity);
        }
        let request = ArchiveRequest {
            id,
            subject: principal.subject().to_owned(),
            leaf_sha256: principal.leaf_sha256(),
            request_digest: request_digest(&draft)?,
            if_match: draft.base().etag(),
        };
        let accounts = draft.assessment().prepared().changes().accounts().clone();
        let cluster_scope = draft
            .assessment()
            .prepared()
            .changes()
            .requires_cluster_admin();
        Ok(PendingRecord {
            owner,
            principal: principal.clone(),
            request,
            key,
            accounts,
            cluster_scope,
            ticket: Some(self.request_read()?),
            waiting: None,
            stage: Stage::ReadingExisting(Box::new(draft)),
        })
    }
    pub fn cancel_record_plan(&mut self, pending: &mut PendingRecord) -> Result<bool> {
        if !Arc::ptr_eq(&self.planning_identity()?, &pending.owner) {
            return Err(Error::Identity);
        }
        let active = !matches!(pending.stage, Stage::Completed);
        pending.stage = Stage::Completed;
        pending.waiting.take();
        if let Some(ticket) = pending.ticket.take() {
            self.cancel_read(&ticket)?;
        }
        Ok(active)
    }
    pub fn poll_record_plan(
        &mut self,
        trust: &ServerTls,
        pending: &mut PendingRecord,
    ) -> Result<Option<StoredPrepared>> {
        if !Arc::ptr_eq(&self.planning_identity()?, &pending.owner) {
            return Err(Error::Identity);
        }
        let result = self.poll_record_inner(trust, pending);
        if result.is_err() {
            let _ = self.cancel_record_plan(pending);
        }
        result
    }
    fn poll_record_inner(
        &mut self,
        trust: &ServerTls,
        pending: &mut PendingRecord,
    ) -> Result<Option<StoredPrepared>> {
        trust.revalidate(&pending.principal)?;
        if matches!(pending.stage, Stage::Completed) {
            return Err(Error::NotReady);
        }
        if let Stage::Uploading(assessment, _) | Stage::Rechecking(assessment) = &pending.stage {
            self.deployment_owner()
                .ok_or(Error::NotReady)?
                .revalidate_archive(assessment)?;
        }
        if let Some(waiting) = &mut pending.waiting {
            let Some(result) = waiting.result.take() else {
                return Ok(None);
            };
            pending.waiting.take();
            if matches!(pending.stage, Stage::Recording) {
                match result {
                    Ok(Change::PreparedStored { id, .. }) if id == pending.request.id => {}
                    // Another identical request may have won after our first read.
                    Err(Error::Conflict) => {}
                    Err(error) => return Err(error),
                    _ => return Err(Error::Protocol),
                }
                pending.ticket = Some(self.request_read()?);
                pending.stage = Stage::ReadingResult;
                return Ok(None);
            }
            match result? {
                Change::Begun | Change::ChunkStored { .. } | Change::Sealed => {}
                _ => return Err(Error::Protocol),
            }
        }
        if let Stage::Building(job) = &mut pending.stage {
            if let Some(assessment) = self
                .deployment_owner()
                .ok_or(Error::NotReady)?
                .poll_archive(job)?
            {
                pending.stage = Stage::Uploading(Box::new(assessment), Upload::default());
            }
            return Ok(None);
        }
        if let Stage::Uploading(assessment, upload) = &mut pending.stage {
            let Some(object) = assessment.archive().objects().get(upload.object) else {
                let Stage::Uploading(assessment, _) =
                    std::mem::replace(&mut pending.stage, Stage::Completed)
                else {
                    return Err(Error::Protocol);
                };
                pending.ticket = Some(self.request_read()?);
                pending.stage = Stage::Rechecking(assessment);
                return Ok(None);
            };
            let spec = object.spec();
            let command = if !upload.began {
                upload.began = true;
                ObjectCommand::Begin(spec.clone())
            } else if upload.offset < object.bytes().len() {
                let start = upload.offset;
                let end = (start + CHUNK_BYTES).min(object.bytes().len());
                upload.offset = end;
                ObjectCommand::Chunk {
                    key: spec.key,
                    owner: spec.owner,
                    offset: start.try_into().map_err(|_| Error::Bounds)?,
                    bytes: object.bytes()[start..end].to_vec(),
                }
            } else {
                upload.object += 1;
                upload.offset = 0;
                upload.began = false;
                ObjectCommand::Seal {
                    key: spec.key,
                    owner: spec.owner,
                }
            };
            pending.waiting = Some(Awaited {
                proposal: self.propose(command)?,
                result: None,
            });
            return Ok(None);
        }
        let view = match self.read_prepared(
            pending.ticket.as_ref().ok_or(Error::NotReady)?,
            PreparedQuery::Idempotency {
                subject: &pending.request.subject,
                key: &pending.key,
            },
        )? {
            PreparedRead::Pending => return Ok(None),
            PreparedRead::Ready(view) => {
                pending.ticket.take();
                view
            }
        };
        let head = view.head().ok_or(Error::NotReady)?;
        let authority =
            RoleBindings::from_candidate(head.candidate())?.authorize(trust, &pending.principal)?;
        authority
            .authorize_read(&pending.accounts, pending.cluster_scope)
            .map_err(Error::Configuration)?;
        if let Some(record) = view.record() {
            if record.subject() != pending.request.subject
                || record.request_digest() != pending.request.request_digest
                || record.if_match() != pending.request.if_match
            {
                return Err(Error::Conflict);
            }
            let record = record.clone();
            pending.stage = Stage::Completed;
            pending.waiting.take();
            return Ok(Some(record));
        }
        match std::mem::replace(&mut pending.stage, Stage::Completed) {
            Stage::ReadingExisting(draft) => {
                if head != draft.base() {
                    return Err(Error::Conflict);
                }
                draft
                    .assessment()
                    .prepared()
                    .changes()
                    .authorize_plan(&authority)
                    .map_err(Error::Configuration)?;
                let request = ArchiveRequest {
                    id: pending.request.id,
                    subject: pending.request.subject.clone(),
                    leaf_sha256: pending.request.leaf_sha256,
                    request_digest: pending.request.request_digest,
                    if_match: pending.request.if_match.clone(),
                };
                let job = self
                    .deployment_owner()
                    .ok_or(Error::NotReady)?
                    .start_archive(*draft, request)?;
                pending.stage = Stage::Building(Box::new(job));
                Ok(None)
            }
            Stage::Rechecking(assessment) => {
                if head != assessment.base() {
                    return Err(Error::Conflict);
                }
                assessment
                    .prepared()
                    .changes()
                    .authorize_plan(&authority)
                    .map_err(Error::Configuration)?;
                let admitted_unix_ms = self
                    .deployment_owner()
                    .ok_or(Error::NotReady)?
                    .revalidate_archive(&assessment)?;
                let record = RecordPrepared {
                    id: pending.request.id,
                    subject: pending.request.subject.clone(),
                    leaf_sha256: pending.request.leaf_sha256,
                    idempotency_key: pending.key.clone(),
                    request_digest: pending.request.request_digest,
                    if_match: pending.request.if_match.clone(),
                    root: assessment.archive().root().clone(),
                    admitted_unix_ms,
                };
                pending.waiting = Some(Awaited {
                    proposal: self.record_prepared(record)?,
                    result: None,
                });
                pending.stage = Stage::Recording;
                Ok(None)
            }
            Stage::ReadingResult => Err(Error::NotFound),
            _ => Err(Error::Protocol),
        }
    }
}
fn frame(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
}
fn request_digest(draft: &PlanDraft) -> Result<Hash> {
    let mut hash = Sha256::new();
    hash.update(b"tickwire.config.record-prepared-request.v1\0");
    frame(&mut hash, draft.subject().as_bytes());
    frame(&mut hash, draft.base().etag().as_bytes());
    hash.update(draft.base().raft_index().to_le_bytes());
    hash.update(draft.base().raft_term().to_le_bytes());
    // Idempotency follows the original request, including its fixed options;
    // later qualification times or planner implementation changes are not inputs.
    let options = tickwire_config::Candidate::parse(
        tickwire_config::Format::Json,
        &serde_json::to_vec(draft.plan().options()).map_err(|_| Error::Protocol)?,
        tickwire_config::Limits {
            max_bytes: tickwire_config::plan::MAX_BYTES,
            max_depth: 8,
        },
    )
    .map_err(Error::Configuration)?;
    frame(&mut hash, options.canonical_bytes());
    hash.update([match draft.input() {
        crate::planning::PlanInput::Full(_) => 0,
        crate::planning::PlanInput::Patch(_) => 1,
    }]);
    frame(&mut hash, draft.input().canonical_bytes());
    Ok(hash.finalize().into())
}
