//! Service-internal planning drafts over an actual committed basis.
//! A draft is neither an HTTP view nor admission or runtime authority.
use crate::{
    Error, Hash, ReplicaControl, Result,
    auth::{AuthenticatedPrincipal, ServerTls},
    authorization::RoleBindings,
    bootstrap::{CommittedHead, HeadRead},
    deployment::ContentValidated,
    deployment_owner::{PlanAssessment, PlanJob},
};
use std::sync::Arc;
use tickwire_config::{
    Candidate, Limits,
    changes::Authority,
    delivery::ProjectionDelivery,
    patch::{Head, ScopedPatch},
    plan::{ComputedPlan, PlanOptions},
    prepared::{Input, PreparedCandidate},
};
use tickwire_control_raft::ReadId;
use tickwire_journal::store::Store;

pub enum PlanInput {
    Full(Candidate),
    Patch(ScopedPatch),
}
impl PlanInput {
    fn input(&self) -> Input<'_> {
        match self {
            Self::Full(candidate) => Input::Full(candidate),
            Self::Patch(patch) => Input::Patch(patch),
        }
    }
    fn bytes(&self) -> usize {
        match self {
            Self::Full(candidate) => candidate.canonical_bytes().len(),
            Self::Patch(patch) => patch.canonical_bytes().len(),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanProgress {
    ReadingBasis,
    Computing,
    RecheckingBasis,
    Completed,
}
enum Stage {
    ReadingBasis,
    Computing(Box<PlanJob>),
    RecheckingBasis(Box<PlanAssessment>),
    Completed,
}
/// Poll to completion or explicitly cancel on the originating replica. Dropping
/// the handle releases its worker share but cannot cancel an owner's read ticket.
#[must_use]
pub struct PendingPlan {
    owner: Arc<()>,
    principal: AuthenticatedPrincipal,
    ticket: Option<ReadId>,
    input: Option<PlanInput>,
    if_match: String,
    options: Option<PlanOptions>,
    stage: Stage,
}
impl PendingPlan {
    pub fn progress(&self) -> PlanProgress {
        match self.stage {
            Stage::ReadingBasis => PlanProgress::ReadingBasis,
            Stage::Computing(_) => PlanProgress::Computing,
            Stage::RecheckingBasis(_) => PlanProgress::RecheckingBasis,
            Stage::Completed => PlanProgress::Completed,
        }
    }
}
/// Full internal content closure. A network caller still needs a scoped view,
/// current registry and runtime checks, and durable plan admission.
pub struct PlanDraft {
    assessment: PlanAssessment,
    principal: AuthenticatedPrincipal,
}
impl PlanDraft {
    pub fn base(&self) -> &CommittedHead {
        self.assessment.base()
    }
    pub fn plan(&self) -> &ComputedPlan {
        self.assessment.plan()
    }
    pub fn content(&self) -> &ContentValidated {
        self.assessment.content()
    }
    pub fn delivery(&self) -> &ProjectionDelivery {
        self.assessment.delivery()
    }
    pub fn subject(&self) -> &str {
        self.principal.subject()
    }
    pub fn leaf_sha256(&self) -> Hash {
        self.principal.leaf_sha256()
    }
    pub fn checks_pending(&self) -> &'static [&'static str] {
        &[
            "retained_registry_head",
            "runtime_targets",
            "runtime_apply_driver",
            "durable_plan_admission",
        ]
    }
}
impl<S: Store> ReplicaControl<'_, S> {
    /// Trusted service assembly entry. Does not expose the draft over HTTP.
    pub fn begin_plan(
        &mut self,
        trust: &ServerTls,
        principal: &AuthenticatedPrincipal,
        input: PlanInput,
        if_match: String,
        options: PlanOptions,
    ) -> Result<PendingPlan> {
        trust.revalidate(principal)?;
        if input.bytes() > Limits::default().max_bytes || if_match.len() > 128 {
            return Err(Error::Bounds);
        }
        let owner = self.planning_identity()?;
        let ticket = self.request_read()?;
        Ok(PendingPlan {
            owner,
            principal: principal.clone(),
            ticket: Some(ticket),
            input: Some(input),
            if_match,
            options: Some(options),
            stage: Stage::ReadingBasis,
        })
    }
    pub fn cancel_plan(&mut self, pending: &mut PendingPlan) -> Result<bool> {
        if !Arc::ptr_eq(&self.planning_identity()?, &pending.owner) {
            return Err(Error::Identity);
        }
        let active = !matches!(pending.stage, Stage::Completed);
        pending.stage = Stage::Completed;
        pending.input.take();
        pending.options.take();
        if let Some(ticket) = pending.ticket.take() {
            self.cancel_read(&ticket)?;
        }
        Ok(active)
    }
    pub fn poll_plan(
        &mut self,
        trust: &ServerTls,
        pending: &mut PendingPlan,
    ) -> Result<Option<PlanDraft>> {
        if !Arc::ptr_eq(&self.planning_identity()?, &pending.owner) {
            return Err(Error::Identity);
        }
        let result = self.poll_plan_inner(trust, pending);
        if result.is_err() {
            let _ = self.cancel_plan(pending);
        }
        result
    }
    fn poll_plan_inner(
        &mut self,
        trust: &ServerTls,
        pending: &mut PendingPlan,
    ) -> Result<Option<PlanDraft>> {
        trust.revalidate(&pending.principal)?;
        if matches!(pending.stage, Stage::Completed) {
            return Err(Error::NotReady);
        }
        if let Stage::Computing(job) = &mut pending.stage {
            let Some(assessment) = self
                .deployment_owner()
                .ok_or(Error::NotReady)?
                .poll_plan(job)?
            else {
                return Ok(None);
            };
            pending.ticket = Some(self.request_read()?);
            pending.stage = Stage::RecheckingBasis(Box::new(assessment));
            return Ok(None);
        }
        let head = match self.read_head(pending.ticket.as_ref().ok_or(Error::NotReady)?)? {
            HeadRead::Pending => return Ok(None),
            HeadRead::Ready(head) => {
                pending.ticket.take();
                head.ok_or(Error::NotReady)?
            }
        };
        let authority =
            RoleBindings::from_candidate(head.candidate())?.authorize(trust, &pending.principal)?;
        let prepared = prepare(&head, pending, &authority)?;
        match std::mem::replace(&mut pending.stage, Stage::Completed) {
            Stage::ReadingBasis => {
                let job = self.deployment_owner().ok_or(Error::NotReady)?.start_plan(
                    *head,
                    prepared,
                    pending.options.take().ok_or(Error::NotReady)?,
                )?;
                pending.stage = Stage::Computing(Box::new(job));
                Ok(None)
            }
            Stage::RecheckingBasis(assessment) => {
                if *head != *assessment.base()
                    || prepared.candidate() != assessment.prepared().candidate()
                {
                    return Err(Error::Conflict);
                }
                self.deployment_owner()
                    .ok_or(Error::NotReady)?
                    .revalidate_plan(&assessment)?;
                pending.input.take();
                Ok(Some(PlanDraft {
                    assessment: *assessment,
                    principal: pending.principal.clone(),
                }))
            }
            Stage::Computing(_) | Stage::Completed => Err(Error::NotReady),
        }
    }
}
fn prepare(
    head: &CommittedHead,
    pending: &PendingPlan,
    authority: &Authority,
) -> Result<PreparedCandidate> {
    PreparedCandidate::prepare(
        &Head::new(head.revision(), head.candidate()).map_err(Error::Configuration)?,
        &pending.if_match,
        head.candidate(),
        pending.input.as_ref().ok_or(Error::NotReady)?.input(),
        authority,
        Limits::default(),
    )
    .map_err(Error::Configuration)
}
