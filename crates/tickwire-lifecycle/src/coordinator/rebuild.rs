use super::*;
impl<S: Store> Coordinator<S> {
    pub fn refresh_rebuild(
        &mut self,
        member: &Member,
        expected_version: u64,
        now: MonoTime,
    ) -> Result<ApplyResult> {
        self.check_version(expected_version)?;
        self.check_now(now, true)?;
        let next = expected_version.checked_add(1).ok_or(Error::Capacity)?;
        let request = self.rebuild_refresh_request(member, next)?;
        self.commit(Event::RebuildRefresh {
            version: next,
            at_ns: now.nanos(),
            member: member.clone(),
            request_digest: request.digest(),
        })
    }
    pub(super) fn rebuild_refresh_request(
        &self,
        member: &Member,
        next: u64,
    ) -> Result<SignedControlRequest> {
        if self.config.operation.schema != 4
            || !matches!(
                self.state.stage,
                Stage::Preparing
                    | Stage::PreparedAll
                    | Stage::Committing
                    | Stage::PartialCommitted
                    | Stage::AwaitingResume
            )
        {
            return Err(Error::Phase);
        }
        let i = self
            .config
            .members
            .iter()
            .position(|a| &a.member == member)
            .ok_or(Error::Identity)?;
        let slot = &self.state.slots[i];
        if slot.committed.is_some()
            || slot
                .latest
                .as_ref()
                .is_none_or(|r| r.phase != AppliedPhase::Prepared)
        {
            return Err(Error::Phase);
        }
        let adopted = RebuildAdoption::from_receipt(slot.rebuild.as_ref().ok_or(Error::Missing)?)?;
        self.signer.sign_request(&ControlRequest {
            schema: 3,
            coordinator_id: self.config.coordinator_id,
            operation: self.config.operation.clone(),
            member: member.clone(),
            request_id: self.request_id(next, ControlAction::Prepare, member)?,
            action: ControlAction::Prepare,
            proof: Some(adopted.readiness.clone()),
            release: None,
            replay: None,
            rebuild: Some(adopted),
            valid_until_ns: member.valid_until_ns,
        })
    }
    pub fn ingest_rebuild_observed(
        &mut self,
        signed: SignedRebuildObserved,
        expected_version: u64,
        now: MonoTime,
    ) -> Result<ApplyResult> {
        if let Some(prior) = self.state.seen.get(&signed.digest()) {
            let mut result = prior.clone();
            result.duplicate = true;
            return Ok(result);
        }
        self.check_version(expected_version)?;
        self.check_now(now, true)?;
        self.commit(Event::RebuildObserved {
            version: expected_version.checked_add(1).ok_or(Error::Capacity)?,
            at_ns: now.nanos(),
            signed,
        })
    }
    pub fn ingest_rebuild(
        &mut self,
        signed: SignedRebuildPrepared,
        expected_version: u64,
        now: MonoTime,
    ) -> Result<ApplyResult> {
        if let Some(prior) = self.state.seen.get(&signed.digest()) {
            let mut r = prior.clone();
            r.duplicate = true;
            return Ok(r);
        }
        self.check_version(expected_version)?;
        self.check_now(now, true)?;
        self.commit(Event::RebuildPrepared {
            version: expected_version.checked_add(1).ok_or(Error::Capacity)?,
            at_ns: now.nanos(),
            signed,
        })
    }
    pub(super) fn apply_rebuild(
        &mut self,
        signed: &SignedRebuildPrepared,
        version: u64,
        now: MonoTime,
    ) -> Result<()> {
        let probe: RebuildPrepared = decode_canonical(&signed.payload)?;
        let i = self
            .config
            .members
            .iter()
            .position(|a| a.member == probe.member)
            .ok_or(Error::Identity)?;
        let r = verify_rebuild_prepared(
            signed,
            self.config.members[i].public_key,
            &self.config.operation,
            now,
        )?;
        let pending = self
            .state
            .pending
            .iter()
            .position(|p| p == &r.challenge)
            .ok_or(Error::Conflict)?;
        let request = crate::rebuild::validate(&r)?;
        let slot = &mut self.state.slots[i];
        if slot.committed.is_some()
            || slot.latest.as_ref().is_some_and(|old| {
                old.phase == AppliedPhase::Halted
                    || r.state_version <= old.state_version
                    || r.risk_gate_revision != old.risk_gate_revision
            })
        {
            return Err(Error::Conflict);
        }
        match (&slot.rebuild, &request.rebuild) {
            (Some(previous), Some(adopted))
                if RebuildAdoption::from_receipt(previous)? == *adopted => {}
            (None, None) if r.prepare == r.challenge => {}
            _ => return Err(Error::Conflict),
        }
        // Internal phase projection only: this is never signed or returned as a
        // legacy AppliedReceipt. The original installed proof remains in WAL.
        slot.latest = Some(AppliedReceipt {
            schema: 1,
            operation_id: r.operation.operation_id,
            operation_revision: r.operation.operation_revision,
            members_digest: r.operation.members_digest()?,
            logical_strategy_id: r.operation.logical_strategy_id,
            member: r.member.clone(),
            phase: AppliedPhase::Prepared,
            state_version: r.state_version,
            handover_sequence: 0,
            handover_digest: [0; 32],
            risk_gate_revision: r.risk_gate_revision,
            control_request_id: request.request_id,
            issued_ns: r.issued_ns,
            valid_until_ns: r.valid_until_ns,
        });
        slot.prepared = Some(signed.digest());
        slot.rebuild = Some(signed.clone());
        slot.refresh_required = false;
        self.state.readiness[i] = Some(r.readiness);
        self.state.pending.remove(pending);
        self.state.stage = self.applied_stage()?;
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
    pub(super) fn apply_rebuild_observed(
        &mut self,
        signed: &SignedRebuildObserved,
        version: u64,
        now: MonoTime,
    ) -> Result<()> {
        let probe: RebuildObserved = decode_canonical(&signed.payload)?;
        let i = self
            .config
            .members
            .iter()
            .position(|a| a.member == probe.applied.member)
            .ok_or(Error::Identity)?;
        let observed = verify_rebuild_observed(
            signed,
            self.config.members[i].public_key,
            &self.config.operation,
            now,
        )?;
        if !self.state.pending.contains(&observed.query) {
            return Err(Error::Conflict);
        }
        let historical: RebuildPrepared = decode_canonical(&observed.prepared.payload)?;
        let request = crate::rebuild::validate(&historical)?;
        let slot = &mut self.state.slots[i];
        if slot.committed.is_some()
            || slot
                .latest
                .as_ref()
                .is_some_and(|p| p.phase == AppliedPhase::Halted)
            || (slot.rebuild.as_ref() != Some(&observed.prepared)
                && slot.prepare_challenge.as_ref() != Some(&historical.challenge))
        {
            return Err(Error::Conflict);
        }
        match (&slot.rebuild, &request.rebuild) {
            (Some(previous), _) if previous == &observed.prepared => {}
            (Some(previous), Some(adopted))
                if RebuildAdoption::from_receipt(previous)? == *adopted => {}
            (None, None) if historical.prepare == historical.challenge => {}
            _ => return Err(Error::Conflict),
        }
        slot.prepared = Some(observed.prepared.digest());
        slot.rebuild = Some(observed.prepared);
        slot.refresh_required = true;
        self.state.readiness[i] = Some(historical.readiness);
        self.apply_receipt_value(observed.applied, signed.digest(), None, i, version)
    }
}
