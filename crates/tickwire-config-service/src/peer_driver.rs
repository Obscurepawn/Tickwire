//! A single owner for consensus, deterministic business state and peer I/O.
use crate::{AppliedResult, Error, ObjectCommand, Read, Replica, Result, bootstrap};
use tickwire_control_raft::{
    Message, Proposal, ReadId,
    driver::{
        DriverOutput, DriverStatus, Membership, OutputBound, PeerDriver, ReadyBudget, RetainedSize,
    },
    generation::{GenerationStore, StagingSnapshot, VerifiedSnapshot},
    membership::{MembershipProposal, MembershipReceipt, MembershipRequest},
    snapshot::InstalledGeneration,
};
use tickwire_journal::store::Store;

/// The peer service exposes only this facade, never its mutation/drive owner.
pub struct ReplicaControl<'a, S: Store>(&'a mut Replica<S>);
impl<S: Store> ReplicaControl<'_, S> {
    pub(crate) fn planning_identity(&self) -> Result<std::sync::Arc<()>> {
        self.0.healthy()?;
        Ok(self.0.planning_owner.clone())
    }
    /// Trusted service assembly only; source trust is not accepted from HTTP.
    pub fn install_deployment_owner(
        &mut self,
        owner: crate::deployment_owner::Owner,
    ) -> Result<()> {
        self.0.healthy()?;
        if self.0.deployment.is_some() {
            return Err(Error::Conflict);
        }
        self.0.deployment = Some(owner);
        Ok(())
    }
    pub fn replace_deployment_sources(
        &mut self,
        bundle: crate::source_bundle::Bundle,
        trust: crate::evidence::Trust,
        limits: crate::deployment::Limits,
    ) -> Result<()> {
        self.0.healthy()?;
        self.0
            .deployment
            .as_mut()
            .ok_or(Error::NotReady)?
            .replace(bundle, trust, limits)
    }
    pub(crate) fn deployment_owner(&mut self) -> Option<&mut crate::deployment_owner::Owner> {
        self.0.deployment.as_mut()
    }
    pub fn initialize(&mut self) -> Result<Proposal> {
        self.0.initialize()
    }
    pub fn propose(&mut self, command: ObjectCommand) -> Result<Proposal> {
        self.0.propose(command)
    }
    pub fn bootstrap(&mut self, request: bootstrap::Bootstrap) -> Result<Proposal> {
        self.0.bootstrap(request)
    }
    pub fn request_read(&mut self) -> Result<ReadId> {
        self.0.request_read()
    }
    pub(crate) fn cancel_read(&mut self, ticket: &ReadId) -> Result<bool> {
        self.0.healthy()?;
        self.0.node.cancel_read(ticket).map_err(Into::into)
    }
    pub fn read_head(&mut self, ticket: &ReadId) -> Result<bootstrap::HeadRead> {
        self.0.read_head(ticket)
    }
    pub fn read_object(&mut self, ticket: &ReadId, key: crate::ObjectKey) -> Result<Read> {
        self.0.read_object(ticket, key)
    }
    pub fn propose_membership(&mut self, request: MembershipRequest) -> Result<MembershipProposal> {
        self.0.healthy()?;
        self.0.node.propose_membership(request).map_err(Into::into)
    }
    pub fn membership_receipt(&self, operation: [u8; 16]) -> Result<Option<&MembershipReceipt>> {
        self.0.healthy()?;
        Ok(self.0.node.membership_receipt(operation))
    }
}
impl ReplicaControl<'_, GenerationStore> {
    pub fn compact_snapshot(&mut self) -> Result<InstalledGeneration> {
        self.0.compact_snapshot()
    }
}
impl RetainedSize for AppliedResult {
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.applied.retained_bytes()
            - std::mem::size_of_val(&self.applied)
    }
}
impl<S: Store> PeerDriver for Replica<S> {
    type Error = Error;
    type Applied = AppliedResult;
    type Control<'a>
        = ReplicaControl<'a, S>
    where
        S: 'a;
    fn control(&mut self) -> Self::Control<'_> {
        ReplicaControl(self)
    }
    fn tick(&mut self) -> Result<()> {
        Replica::tick(self)
    }
    fn step(&mut self, peer: u64, message: Message) -> Result<()> {
        Replica::step(self, peer, message)
    }
    fn output_bound(&self, _budget: ReadyBudget) -> Result<OutputBound> {
        self.healthy()?;
        let mut bound = self.node.pending_applied_bound()?;
        // Node's bound includes its own inline Applied capacity. Our Vec is
        // allocated once for the exact returned item count; reserve the complete
        // extra result size conservatively for every possible pending item.
        let extra = bound
            .items
            .checked_mul(std::mem::size_of::<AppliedResult>())
            .ok_or(Error::Capacity)?;
        bound.bytes = bound.bytes.checked_add(extra).ok_or(Error::Capacity)?;
        Ok(bound)
    }
    fn drive(&mut self, budget: ReadyBudget) -> Result<DriverOutput<Self::Applied>> {
        let output = self.drive_with_budget(budget)?;
        Ok(DriverOutput {
            messages: output.messages,
            applied: output.applied,
            raft_applied_through: self.node.applied_index(),
            application_scanned_through: self.scanned_through,
            more_ready: output.more_ready,
        })
    }
    fn status(&self) -> DriverStatus {
        let mut status = self.node.status();
        status.poisoned |= self.poisoned;
        status
    }
    fn current_membership(&self) -> Membership {
        self.node.current_membership()
    }
    fn peer_session(&mut self, peer: u64, session: Option<[u8; 16]>) -> Result<()> {
        self.healthy()?;
        self.node.peer_session(peer, session).map_err(Into::into)
    }
    fn snapshot_reader(&self) -> Result<Option<VerifiedSnapshot>> {
        self.healthy()?;
        if !self.node.supports_snapshots() {
            return Ok(None);
        }
        self.node.snapshot_reader().map_err(Into::into)
    }
    fn stage_snapshot(&self) -> Result<Option<StagingSnapshot>> {
        self.healthy()?;
        if !self.node.supports_snapshots() {
            return Ok(None);
        }
        self.node.stage_snapshot().map(Some).map_err(Into::into)
    }
    fn step_snapshot(
        &mut self,
        peer: u64,
        message: Message,
        snapshot: VerifiedSnapshot,
    ) -> Result<bool> {
        self.healthy()?;
        if !self.node.supports_snapshots() {
            return Ok(false);
        }
        self.node.step_snapshot(peer, message, snapshot)?;
        Ok(true)
    }
    fn report_snapshot(&mut self, peer: u64, success: bool) -> Result<()> {
        self.healthy()?;
        self.node.report_snapshot(peer, success).map_err(Into::into)
    }
    fn report_unreachable(&mut self, peer: u64) -> Result<()> {
        self.healthy()?;
        self.node.report_unreachable(peer).map_err(Into::into)
    }
}
