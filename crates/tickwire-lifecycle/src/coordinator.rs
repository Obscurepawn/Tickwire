use crate::wal::Wal;
use crate::*;
use std::collections::BTreeMap;
use tickwire_journal::{PhysicalFrontier, store::Store};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberAuthority {
    pub member: Member,
    pub public_key: Hash,
    pub readiness: Readiness,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorConfig {
    pub coordinator_id: [u8; 16],
    pub coordinator_key: Hash,
    pub operation: Operation,
    pub members: Vec<MemberAuthority>,
    pub audit: tickwire_journal::Config,
}
impl CoordinatorConfig {
    fn validate(&self) -> Result<()> {
        self.operation.validate()?;
        if self.coordinator_id == [0; 16]
            || self.coordinator_key == [0; 32]
            || self.members.len() != self.operation.members.len()
        {
            return Err(Error::Identity);
        }
        for (expected, authority) in self.operation.members.iter().zip(&self.members) {
            if expected != &authority.member
                || authority.public_key == [0; 32]
                || [
                    authority.readiness.checkpoint_digest,
                    authority.readiness.orders_digest,
                    authority.readiness.input_seal_digest,
                ]
                .contains(&[0; 32])
            {
                return Err(Error::Identity);
            }
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Preparing,
    PreparedAll,
    Committing,
    PartialCommitted,
    CommittedAll,
    Releasing,
    ActiveAll,
    Reconciling,
    AwaitingResume,
    Halting,
    Halted,
    Frozen,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyResult {
    pub version: u64,
    pub stage: Stage,
    pub duplicate: bool,
}
#[derive(Clone)]
struct Slot {
    latest: Option<AppliedReceipt>,
    prepared: Option<SignedReceipt>,
    committed: Option<SignedReceipt>,
    released: Option<SignedReceipt>,
    queried: bool,
}
#[derive(Clone)]
struct State {
    version: u64,
    stage: Stage,
    slots: Vec<Slot>,
    pending: Vec<SignedControlRequest>,
    pending_version: u64,
    readiness: Vec<Readiness>,
    release: Option<SignedGroupRelease>,
    seen: BTreeMap<Hash, ApplyResult>,
    last_ns: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Event {
    Config(CoordinatorConfig),
    Dispatch {
        version: u64,
        at_ns: u64,
        action: ControlAction,
        request_digests: Vec<Hash>,
        release: Option<SignedGroupRelease>,
    },
    Receipt {
        version: u64,
        at_ns: u64,
        signed: SignedReceipt,
    },
    Resume {
        version: u64,
        at_ns: u64,
    },
    Readiness {
        version: u64,
        at_ns: u64,
        member: Member,
        proof: Readiness,
        request_digest: Hash,
    },
}
pub struct Coordinator<S: Store> {
    config: CoordinatorConfig,
    signer: Signer,
    wal: Wal<S>,
    state: State,
    poisoned: bool,
}
impl<S: Store> Coordinator<S> {
    pub fn create(
        store: S,
        config: CoordinatorConfig,
        writer: u64,
        signer: Signer,
        now: MonoTime,
    ) -> Result<Self> {
        config.validate()?;
        config.operation.check_time(now)?;
        if signer.public_key() != config.coordinator_key {
            return Err(Error::Identity);
        }
        let mut wal = Wal::create(store, config.audit.clone(), writer)?;
        wal.append(&Event::Config(config.clone()))?;
        let state = State::new(&config.members);
        let mut owner = Self {
            config,
            signer,
            wal,
            state,
            poisoned: false,
        };
        owner.dispatch(ControlAction::Prepare, 0, now)?;
        Ok(owner)
    }
    /// Reopening always persists new QueryApplied challenges. Old signed
    /// receipts are history, not a fresh observation of remote authorization.
    pub fn recover(
        store: S,
        config: CoordinatorConfig,
        writer: u64,
        signer: Signer,
        required: Option<&PhysicalFrontier>,
        now: MonoTime,
    ) -> Result<Self> {
        config.validate()?;
        if signer.public_key() != config.coordinator_key {
            return Err(Error::Identity);
        }
        let (wal, events) = Wal::<S>::recover::<Event>(store, &config.audit, writer, required)?;
        if !matches!(events.first(),Some(Event::Config(c))if c==&config) {
            return Err(Error::Identity);
        }
        let state = State::new(&config.members);
        let mut owner = Self {
            config,
            signer,
            wal,
            state,
            poisoned: false,
        };
        for event in events.iter().skip(1) {
            owner.apply(event)?;
        }
        let version = owner.state.version;
        owner.dispatch(ControlAction::QueryApplied, version, now)?;
        Ok(owner)
    }
    pub fn config(&self) -> &CoordinatorConfig {
        &self.config
    }
    pub fn version(&self) -> u64 {
        self.state.version
    }
    pub fn stage(&self) -> Stage {
        if self.poisoned {
            Stage::Frozen
        } else {
            self.state.stage
        }
    }
    pub fn audit_position(&self) -> u64 {
        self.wal.position()
    }
    pub fn audit_frontier(&self) -> Option<&PhysicalFrontier> {
        if self.poisoned {
            None
        } else {
            self.wal.frontier()
        }
    }
    pub fn pending_requests(&self) -> &[SignedControlRequest] {
        if self.poisoned {
            &[]
        } else {
            &self.state.pending
        }
    }
    /// Durable dispatch version used for ordered outbox publication.
    pub fn pending_version(&self) -> u64 {
        self.state.pending_version
    }
    pub fn latest_receipt(&self, domain: [u8; 16]) -> Option<&AppliedReceipt> {
        self.config
            .members
            .iter()
            .position(|m| m.member.risk_domain == domain)
            .and_then(|i| self.state.slots[i].latest.as_ref())
    }
    pub fn group_release(&self) -> Option<&SignedGroupRelease> {
        if !self.poisoned && matches!(self.state.stage, Stage::Releasing | Stage::ActiveAll) {
            self.state.release.as_ref()
        } else {
            None
        }
    }
    pub fn begin_commit(&mut self, expected_version: u64, now: MonoTime) -> Result<ApplyResult> {
        self.dispatch(ControlAction::Commit, expected_version, now)
    }
    pub fn release(&mut self, expected_version: u64, now: MonoTime) -> Result<ApplyResult> {
        self.dispatch(ControlAction::Release, expected_version, now)
    }
    pub fn halt(&mut self, expected_version: u64, now: MonoTime) -> Result<ApplyResult> {
        self.dispatch(ControlAction::Halt, expected_version, now)
    }
    pub fn query_applied(&mut self, expected_version: u64, now: MonoTime) -> Result<ApplyResult> {
        self.dispatch(ControlAction::QueryApplied, expected_version, now)
    }
    /// Replace stale readiness only before this member has committed. A new
    /// Prepare challenge must be applied before a new Commit is issued.
    pub fn update_readiness(
        &mut self,
        member: &Member,
        proof: Readiness,
        expected_version: u64,
        now: MonoTime,
    ) -> Result<ApplyResult> {
        self.check_version(expected_version)?;
        self.check_now(now, true)?;
        let next = expected_version.checked_add(1).ok_or(Error::Capacity)?;
        let request = self.readiness_request(member, &proof, next)?;
        self.commit(Event::Readiness {
            version: next,
            at_ns: now.nanos(),
            member: member.clone(),
            proof,
            request_digest: request.digest(),
        })
    }
    pub fn resume_after_queries(
        &mut self,
        expected_version: u64,
        now: MonoTime,
    ) -> Result<ApplyResult> {
        self.check_version(expected_version)?;
        self.check_now(now, true)?;
        let event = Event::Resume {
            version: expected_version.checked_add(1).ok_or(Error::Capacity)?,
            at_ns: now.nanos(),
        };
        self.commit(event)
    }
    pub fn ingest(
        &mut self,
        signed: SignedReceipt,
        expected_version: u64,
        now: MonoTime,
    ) -> Result<ApplyResult> {
        if let Some(prior) = self.state.seen.get(&signed.digest()) {
            let mut result = prior.clone();
            result.duplicate = true;
            return Ok(result);
        }
        self.check_version(expected_version)?;
        self.check_now(now, false)?;
        let event = Event::Receipt {
            version: expected_version.checked_add(1).ok_or(Error::Capacity)?,
            at_ns: now.nanos(),
            signed: signed.clone(),
        };
        let result = self.commit(event);
        if matches!(
            result,
            Err(Error::Conflict | Error::Phase | Error::Frozen | Error::Missing)
        ) {
            // Only an authenticated answer to a currently pending request can
            // trigger this close. Unrelated and stale callbacks are rejected.
            if let Ok(probe) = decode_canonical::<AppliedReceipt>(&signed.payload)
                && let Some(authority) = self
                    .config
                    .members
                    .iter()
                    .find(|a| a.member == probe.member)
                && verify_receipt(&signed, authority.public_key, &self.config.operation, now)
                    .is_ok()
                && self.state.pending.iter().any(|p| {
                    decode_canonical::<ControlRequest>(&p.payload)
                        .is_ok_and(|r| r.request_id == probe.control_request_id)
                })
            {
                self.halt(self.state.version, now)?;
            }
        }
        result
    }
    fn check_version(&self, expected: u64) -> Result<()> {
        if self.poisoned {
            return Err(Error::Store);
        }
        if expected != self.state.version {
            return Err(Error::Version);
        }
        if expected >= 4095 {
            return Err(Error::Capacity);
        }
        Ok(())
    }
    fn check_now(&self, now: MonoTime, live: bool) -> Result<()> {
        if now.domain().boot_id() != self.config.operation.clock_boot_id
            || now.domain().generation() != self.config.operation.clock_generation
            || now.nanos() < self.state.last_ns
        {
            return Err(Error::Identity);
        }
        if live {
            self.config.operation.check_time(now)?;
        }
        Ok(())
    }
    fn dispatch(
        &mut self,
        action: ControlAction,
        version: u64,
        now: MonoTime,
    ) -> Result<ApplyResult> {
        self.check_version(version)?;
        self.check_now(
            now,
            !matches!(action, ControlAction::Halt | ControlAction::QueryApplied),
        )?;
        let next = version.checked_add(1).ok_or(Error::Capacity)?;
        let (requests, release) = self.plan(action, next, now.nanos())?;
        self.commit(Event::Dispatch {
            version: next,
            at_ns: now.nanos(),
            action,
            request_digests: requests.iter().map(SignedControlRequest::digest).collect(),
            release,
        })
    }
    fn plan(
        &self,
        action: ControlAction,
        next: u64,
        at_ns: u64,
    ) -> Result<(Vec<SignedControlRequest>, Option<SignedGroupRelease>)> {
        let allowed = match action {
            ControlAction::Prepare => self.state.version == 0,
            ControlAction::Commit => self.state.stage == Stage::PreparedAll,
            ControlAction::Release => self.state.stage == Stage::CommittedAll,
            ControlAction::Halt | ControlAction::QueryApplied => true,
        };
        if !allowed {
            return Err(Error::Phase);
        }
        let mut release = None;
        if action == ControlAction::Release {
            let mut members = Vec::new();
            for (i, authority) in self.config.members.iter().enumerate() {
                let slot = &self.state.slots[i];
                let latest = slot.latest.as_ref().ok_or(Error::Missing)?;
                if !matches!(
                    latest.phase,
                    AppliedPhase::Committed | AppliedPhase::Released
                ) {
                    return Err(Error::Phase);
                }
                let receipt = slot.committed.as_ref().ok_or(Error::Missing)?;
                members.push(MemberGate {
                    member: authority.member.clone(),
                    risk_gate_revision: latest.risk_gate_revision,
                    handover_sequence: latest.handover_sequence,
                    handover_digest: latest.handover_digest,
                    committed_receipt_digest: receipt.digest(),
                    release_request_id: self.request_id(next, action, &authority.member)?,
                });
            }
            release = Some(self.signer.sign_release(&GroupRelease {
                schema: 1,
                operation_id: self.config.operation.operation_id,
                operation_revision: self.config.operation.operation_revision,
                members_digest: self.config.operation.members_digest()?,
                logical_strategy_id: self.config.operation.logical_strategy_id,
                coordinator_id: self.config.coordinator_id,
                coordinator_version: next,
                members,
                clock_boot_id: self.config.operation.clock_boot_id,
                clock_generation: self.config.operation.clock_generation,
                issued_ns: at_ns,
                valid_until_ns: self.config.operation.valid_until_ns,
            })?);
        }
        let mut requests = Vec::new();
        for (i, a) in self.config.members.iter().enumerate() {
            // Recovery queries can establish that a member already applied
            // Release. Keep its original Committed receipt in the group proof,
            // but do not issue it a second Release challenge.
            if action == ControlAction::Release
                && self.state.slots[i]
                    .latest
                    .as_ref()
                    .is_some_and(|r| r.phase == AppliedPhase::Released)
            {
                continue;
            }
            if action == ControlAction::Commit
                && self.state.slots[i].latest.as_ref().is_some_and(|r| {
                    matches!(r.phase, AppliedPhase::Committed | AppliedPhase::Released)
                })
            {
                continue;
            }
            let request = ControlRequest {
                schema: 1,
                coordinator_id: self.config.coordinator_id,
                operation: self.config.operation.clone(),
                member: a.member.clone(),
                request_id: self.request_id(next, action, &a.member)?,
                action,
                proof: if matches!(action, ControlAction::Prepare | ControlAction::Commit) {
                    Some(self.state.readiness[i].clone())
                } else {
                    None
                },
                release: release.clone(),
                valid_until_ns: a.member.valid_until_ns,
            };
            requests.push(self.signer.sign_request(&request)?);
        }
        Ok((requests, release))
    }
    fn request_id(&self, version: u64, action: ControlAction, member: &Member) -> Result<[u8; 16]> {
        let hash = digest(
            b"tickwire.lifecycle.request-id.v1",
            &(
                self.config.coordinator_id,
                self.config.operation.operation_id,
                self.config.operation.operation_revision,
                version,
                action,
                member,
            ),
        )?;
        Ok(hash[..16].try_into().expect("hash prefix"))
    }
    fn readiness_request(
        &self,
        member: &Member,
        proof: &Readiness,
        next: u64,
    ) -> Result<SignedControlRequest> {
        if !matches!(
            self.state.stage,
            Stage::Preparing | Stage::PreparedAll | Stage::Committing | Stage::PartialCommitted
        ) {
            return Err(Error::Phase);
        }
        let i = self
            .config
            .members
            .iter()
            .position(|a| &a.member == member)
            .ok_or(Error::Identity)?;
        if self.state.slots[i].committed.is_some()
            || self.state.slots[i].latest.as_ref().is_some_and(|r| {
                matches!(
                    r.phase,
                    AppliedPhase::Committed | AppliedPhase::Released | AppliedPhase::Halted
                )
            })
        {
            return Err(Error::Phase);
        }
        if [
            proof.checkpoint_digest,
            proof.orders_digest,
            proof.input_seal_digest,
        ]
        .contains(&[0; 32])
        {
            return Err(Error::Invalid);
        }
        self.signer.sign_request(&ControlRequest {
            schema: 1,
            coordinator_id: self.config.coordinator_id,
            operation: self.config.operation.clone(),
            member: member.clone(),
            request_id: self.request_id(next, ControlAction::Prepare, member)?,
            action: ControlAction::Prepare,
            proof: Some(proof.clone()),
            release: None,
            valid_until_ns: member.valid_until_ns,
        })
    }
    fn commit(&mut self, event: Event) -> Result<ApplyResult> {
        let before = self.state.clone();
        if let Err(error) = self.apply(&event) {
            self.state = before;
            return Err(error);
        }
        if let Err(error) = self.wal.append(&event) {
            self.state = before;
            self.poisoned = true;
            return Err(error);
        }
        Ok(ApplyResult {
            version: self.state.version,
            stage: self.state.stage,
            duplicate: false,
        })
    }
    fn apply(&mut self, event: &Event) -> Result<()> {
        let (version, at_ns) = match event {
            Event::Dispatch { version, at_ns, .. }
            | Event::Receipt { version, at_ns, .. }
            | Event::Resume { version, at_ns }
            | Event::Readiness { version, at_ns, .. } => (*version, *at_ns),
            Event::Config(_) => return Err(Error::Store),
        };
        if version != self.state.version + 1 || at_ns < self.state.last_ns {
            return Err(Error::Store);
        }
        let domain = tickwire_clock::ClockDomain::new(
            self.config.operation.clock_boot_id,
            self.config.operation.clock_generation,
        )
        .map_err(|_| Error::Identity)?;
        let now = MonoTime::new(domain, at_ns);
        match event {
            Event::Dispatch {
                action,
                request_digests,
                release,
                ..
            } => {
                self.check_now(
                    now,
                    !matches!(action, ControlAction::Halt | ControlAction::QueryApplied),
                )?;
                let expected = self.plan(*action, version, at_ns)?;
                if request_digests
                    != &expected
                        .0
                        .iter()
                        .map(SignedControlRequest::digest)
                        .collect::<Vec<_>>()
                    || release != &expected.1
                {
                    return Err(Error::Identity);
                }
                self.state.pending = expected.0;
                self.state.pending_version = version;
                match action {
                    ControlAction::Prepare => self.state.stage = Stage::Preparing,
                    ControlAction::Commit => {
                        self.state.stage = if self.state.pending.is_empty() {
                            Stage::CommittedAll
                        } else {
                            Stage::Committing
                        }
                    }
                    ControlAction::Release => {
                        self.state.stage = Stage::Releasing;
                        self.state.release = release.clone();
                    }
                    ControlAction::QueryApplied => {
                        self.state.stage = Stage::Reconciling;
                        for slot in &mut self.state.slots {
                            slot.queried = false;
                        }
                    }
                    ControlAction::Halt => self.state.stage = Stage::Halting,
                }
            }
            Event::Receipt { signed, .. } => self.apply_receipt(signed, version, now)?,
            Event::Resume { .. } => {
                self.check_now(now, true)?;
                if self.state.stage != Stage::AwaitingResume
                    || !self.state.slots.iter().all(|s| s.queried)
                {
                    return Err(Error::Phase);
                }
                if self.state.slots.iter().any(|s| {
                    s.latest
                        .as_ref()
                        .is_none_or(|r| r.phase == AppliedPhase::Halted)
                }) {
                    return Err(Error::Frozen);
                }
                self.state.stage = if self.state.slots.iter().all(|s| {
                    s.latest
                        .as_ref()
                        .is_some_and(|r| r.phase == AppliedPhase::Released)
                }) {
                    Stage::ActiveAll
                } else if self.state.slots.iter().all(|s| {
                    s.latest.as_ref().is_some_and(|r| {
                        matches!(r.phase, AppliedPhase::Committed | AppliedPhase::Released)
                    })
                }) {
                    Stage::CommittedAll
                } else {
                    Stage::PreparedAll
                };
            }
            Event::Readiness {
                member,
                proof,
                request_digest,
                ..
            } => {
                self.check_now(now, true)?;
                let request = self.readiness_request(member, proof, version)?;
                if request.digest() != *request_digest {
                    return Err(Error::Identity);
                }
                let i = self
                    .config
                    .members
                    .iter()
                    .position(|a| &a.member == member)
                    .ok_or(Error::Identity)?;
                self.state.readiness[i] = proof.clone();
                self.state.slots[i].prepared = None;
                self.state.pending.retain(|p| {
                    decode_canonical::<ControlRequest>(&p.payload)
                        .is_ok_and(|r| r.member != *member)
                });
                self.state.pending.push(request);
                self.state.pending_version = version;
                self.state.stage = Stage::Preparing;
            }
            Event::Config(_) => unreachable!(),
        }
        self.state.version = version;
        self.state.last_ns = at_ns;
        Ok(())
    }
    fn apply_receipt(&mut self, signed: &SignedReceipt, version: u64, now: MonoTime) -> Result<()> {
        // Parse only to find a pinned key; no parsed field has authority yet.
        let probe: AppliedReceipt = decode_canonical(&signed.payload)?;
        let index = self
            .config
            .members
            .iter()
            .position(|a| a.member == probe.member)
            .ok_or(Error::Identity)?;
        let receipt = verify_receipt(
            signed,
            self.config.members[index].public_key,
            &self.config.operation,
            now,
        )?;
        let pending_index = self
            .state
            .pending
            .iter()
            .position(|p| {
                decode_canonical::<ControlRequest>(&p.payload).is_ok_and(|r| {
                    r.request_id == receipt.control_request_id && r.member == receipt.member
                })
            })
            .ok_or(Error::Conflict)?;
        let request: ControlRequest = decode_canonical(&self.state.pending[pending_index].payload)?;
        let expected = match request.action {
            ControlAction::Prepare => Some(AppliedPhase::Prepared),
            ControlAction::Commit => Some(AppliedPhase::Committed),
            ControlAction::Release => Some(AppliedPhase::Released),
            ControlAction::Halt => Some(AppliedPhase::Halted),
            ControlAction::QueryApplied => None,
        };
        if expected.is_some_and(|p| receipt.phase != p) {
            return Err(Error::Phase);
        }
        let slot = &mut self.state.slots[index];
        if let Some(prior) = &slot.latest {
            if receipt.state_version < prior.state_version
                || receipt.risk_gate_revision < prior.risk_gate_revision
            {
                return Err(Error::Conflict);
            }
            if receipt.phase != AppliedPhase::Halted && prior.phase == AppliedPhase::Halted {
                return Err(Error::Frozen);
            }
            if receipt.phase != AppliedPhase::Halted
                && receipt.risk_gate_revision != prior.risk_gate_revision
            {
                return Err(Error::Frozen);
            }
            if prior.handover_sequence != 0
                && (receipt.handover_sequence != prior.handover_sequence
                    || receipt.handover_digest != prior.handover_digest)
            {
                return Err(Error::Conflict);
            }
            if request.action != ControlAction::QueryApplied
                && receipt.state_version <= prior.state_version
            {
                return Err(Error::Conflict);
            }
            if request.action == ControlAction::QueryApplied
                && receipt.state_version == prior.state_version
                && (receipt.phase != prior.phase
                    || receipt.risk_gate_revision != prior.risk_gate_revision)
            {
                return Err(Error::Conflict);
            }
            if matches!(
                request.action,
                ControlAction::Commit | ControlAction::Release
            ) && receipt.risk_gate_revision != prior.risk_gate_revision
            {
                return Err(Error::Conflict);
            }
        }
        match receipt.phase {
            AppliedPhase::Prepared => {
                if slot.committed.is_some() {
                    return Err(Error::Conflict);
                }
                slot.prepared = Some(signed.clone());
            }
            AppliedPhase::Committed => {
                if slot.prepared.is_none() {
                    return Err(Error::Missing);
                }
                slot.committed = Some(signed.clone());
            }
            AppliedPhase::Released => {
                if slot.committed.is_none() || self.state.release.is_none() {
                    return Err(Error::Missing);
                }
                slot.released = Some(signed.clone());
            }
            AppliedPhase::Halted => {}
        }
        slot.latest = Some(receipt);
        if request.action == ControlAction::QueryApplied {
            slot.queried = true;
        }
        self.state.pending.remove(pending_index);
        if request.action == ControlAction::QueryApplied {
            if self.state.slots.iter().all(|s| s.queried) {
                self.state.stage = Stage::AwaitingResume;
            }
        } else if self.state.slots.iter().all(|s| {
            s.latest
                .as_ref()
                .is_some_and(|r| r.phase == AppliedPhase::Halted)
        }) {
            self.state.stage = Stage::Halted;
        } else if request.action == ControlAction::Halt {
            self.state.stage = Stage::Halting;
        } else {
            self.state.stage = self.applied_stage()?;
        }
        self.state.seen.insert(
            signed.digest(),
            ApplyResult {
                version,
                stage: self.state.stage,
                duplicate: false,
            },
        );
        Ok(())
    }
    fn applied_stage(&self) -> Result<Stage> {
        let all = |phases: &[AppliedPhase]| {
            self.state
                .slots
                .iter()
                .all(|s| s.latest.as_ref().is_some_and(|r| phases.contains(&r.phase)))
        };
        let actions = self
            .state
            .pending
            .iter()
            .map(|p| decode_canonical::<ControlRequest>(&p.payload).map(|r| r.action))
            .collect::<Result<Vec<_>>>()?;
        if all(&[AppliedPhase::Released]) && actions.is_empty() {
            return Ok(Stage::ActiveAll);
        }
        if actions.contains(&ControlAction::Release) {
            return Ok(Stage::Releasing);
        }
        if all(&[AppliedPhase::Committed, AppliedPhase::Released]) && actions.is_empty() {
            return Ok(Stage::CommittedAll);
        }
        if actions.contains(&ControlAction::Commit) {
            return Ok(if self.state.slots.iter().any(|s| s.committed.is_some()) {
                Stage::PartialCommitted
            } else {
                Stage::Committing
            });
        }
        if actions.contains(&ControlAction::Prepare) {
            return Ok(Stage::Preparing);
        }
        if all(&[
            AppliedPhase::Prepared,
            AppliedPhase::Committed,
            AppliedPhase::Released,
        ]) {
            return Ok(Stage::PreparedAll);
        }
        Err(Error::Phase)
    }
}
impl State {
    fn new(members: &[MemberAuthority]) -> Self {
        Self {
            version: 0,
            stage: Stage::Preparing,
            slots: (0..members.len())
                .map(|_| Slot {
                    latest: None,
                    prepared: None,
                    committed: None,
                    released: None,
                    queried: false,
                })
                .collect(),
            pending: vec![],
            pending_version: 0,
            readiness: members.iter().map(|m| m.readiness.clone()).collect(),
            release: None,
            seen: BTreeMap::new(),
            last_ns: 0,
        }
    }
}
