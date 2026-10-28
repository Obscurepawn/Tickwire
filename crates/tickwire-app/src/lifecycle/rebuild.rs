//! Fixed rebuild facts are durable before a publication stage may advance.
use super::*;
mod execution;
mod preflight;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Fixed {
    pub request: protocol::ReplayObject,
    pub plans: protocol::ReplayObject,
    pub contract: protocol::ReplayObject,
    pub inputs: protocol::ReplayObject,
    pub rollback_plans: protocol::ReplayObject,
    pub capture_completion: protocol::ReplayObject,
    pub capture_seal: protocol::ReplayObject,
    pub capture_receipt: protocol::ReplayObject,
    pub end: protocol::RebuildFrontier,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Facts {
    pub prepare: SignedControlRequest,
    pub fixed: Fixed,
    pub warm: Option<protocol::ReplayObject>,
    pub installation: Option<protocol::ReplayObject>,
    pub rollback: Option<protocol::ReplayObject>,
    pub result: Option<protocol::SignedRebuildPrepared>,
}

impl Facts {
    pub(super) fn follows(&self, previous: Option<&Self>) -> Result<()> {
        if self.prepare.payload.is_empty()
            || self.prepare.signature.len() != 64
            || self.installation.is_some() && self.warm.is_none()
            || self.rollback.is_some() && self.installation.is_none()
            || self.result.is_some() && self.rollback.is_none()
        {
            return Err(invalid("rebuild completion ordering"));
        }
        for root in [
            Some(&self.fixed.request),
            Some(&self.fixed.plans),
            Some(&self.fixed.contract),
            Some(&self.fixed.inputs),
            Some(&self.fixed.rollback_plans),
            Some(&self.fixed.capture_completion),
            Some(&self.fixed.capture_seal),
            Some(&self.fixed.capture_receipt),
            self.warm.as_ref(),
            self.installation.as_ref(),
            self.rollback.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            tickwire_release::Object {
                digest: root.digest,
                bytes: root.bytes,
            }
            .validate(tickwire_release::replay::MAX_BYTES as u64)?;
        }
        let stage = |f: &Self| {
            usize::from(f.warm.is_some())
                + usize::from(f.installation.is_some())
                + usize::from(f.rollback.is_some())
        };
        let current = stage(self);
        if previous.is_none() && current != 0 || current > previous.map_or(0, stage) + 1 {
            return Err(invalid("rebuild skipped a durable completion boundary"));
        }
        if let Some(old) = previous
            && (self.prepare != old.prepare
                || self.fixed != old.fixed
                || old
                    .warm
                    .as_ref()
                    .is_some_and(|v| self.warm.as_ref() != Some(v))
                || old
                    .installation
                    .as_ref()
                    .is_some_and(|v| self.installation.as_ref() != Some(v))
                || old
                    .rollback
                    .as_ref()
                    .is_some_and(|v| self.rollback.as_ref() != Some(v))
                || old.result.is_some() && self.result.is_none())
        {
            return Err(invalid("durable rebuild fact cannot be erased or rebound"));
        }
        if let Some(done) = &self.result {
            let adopted = protocol::RebuildAdoption::from_receipt(done)?;
            if adopted.prepare != self.prepare
                || adopted.roots != self.roots()?
                || adopted.end != self.fixed.end
            {
                return Err(invalid("rebuild receipt differs from durable facts"));
            }
        }
        Ok(())
    }
    pub(super) fn roots(&self) -> Result<protocol::RebuildRoots> {
        Ok(protocol::RebuildRoots {
            plans: self.fixed.plans.clone(),
            contract: self.fixed.contract.clone(),
            inputs: self.fixed.inputs.clone(),
            rollback_plans: self.fixed.rollback_plans.clone(),
            warm_completion: self
                .warm
                .clone()
                .ok_or_else(|| invalid("rebuild warm completion missing"))?,
            target_install: self
                .installation
                .clone()
                .ok_or_else(|| invalid("rebuild installation missing"))?,
            rollback_completion: self
                .rollback
                .clone()
                .ok_or_else(|| invalid("rebuild rollback missing"))?,
        })
    }
}

pub(super) fn validate_state(state: &State, previous: Option<&State>) -> Result<()> {
    if state.replay.is_some() && state.rebuild.is_some() {
        return Err(invalid("conflicting lifecycle preparation facts"));
    }
    match &state.rebuild {
        Some(facts) => {
            if state.operation.schema != 4 {
                return Err(invalid("rebuild facts require schema 4"));
            }
            let request: protocol::ControlRequest = serde_json::from_slice(&facts.prepare.payload)?;
            if request.schema != 3
                || request.operation != state.operation
                || request.member != state.member
                || request.action != ControlAction::Prepare
                || request.rebuild.is_some()
                || request.replay.is_some()
                || request.release.is_some()
                || facts.fixed.plans.digest != state.operation.checkpoint_digest
                || facts.fixed.rollback_plans.digest != state.operation.rollback_digest
            {
                return Err(invalid(
                    "rebuild facts differ from original member decision",
                ));
            }
            facts.follows(previous.and_then(|s| s.rebuild.as_ref()))
        }
        None if previous.is_some_and(|s| s.rebuild.is_some()) => {
            Err(invalid("rebuild facts missing"))
        }
        None => Ok(()),
    }
}

pub(super) fn saved_result(
    state: Option<&State>,
    session: u64,
    signed: &SignedControlRequest,
) -> Result<Option<protocol::SignedRebuildPrepared>> {
    let Some(result) = state
        .filter(|s| s.session == session && s.phase == AppliedPhase::Prepared)
        .and_then(|s| s.rebuild.as_ref())
        .and_then(|f| f.result.as_ref())
    else {
        return Ok(None);
    };
    let prepared: protocol::RebuildPrepared = serde_json::from_slice(&result.payload)?;
    Ok((prepared.challenge == *signed).then(|| result.clone()))
}
pub(super) fn check_adoption(state: &State, r: &protocol::ControlRequest) -> Result<()> {
    let facts = state
        .rebuild
        .as_ref()
        .ok_or_else(|| invalid("rebuild facts missing"))?;
    let signed = facts
        .result
        .as_ref()
        .ok_or_else(|| invalid("rebuild result is not durable"))?;
    let adopted = protocol::RebuildAdoption::from_receipt(signed)?;
    if r.rebuild.as_ref() != Some(&adopted) || r.proof.as_ref() != Some(&adopted.readiness) {
        return Err(invalid(
            "coordinator did not adopt the exact rebuild result",
        ));
    }
    Ok(())
}
/// Only the freshly applied query from this execution session may wrap history.
fn query_observation(
    state: Option<&State>,
    session: u64,
    query: &SignedControlRequest,
    applied: &protocol::AppliedReceipt,
) -> Result<Option<protocol::RebuildObserved>> {
    let r: protocol::ControlRequest = serde_json::from_slice(&query.payload)?;
    if r.operation.schema != 4
        || r.action != ControlAction::QueryApplied
        || applied.phase != AppliedPhase::Prepared
    {
        return Ok(None);
    }
    let state = state.ok_or_else(|| invalid("query rebuild state missing"))?;
    if state.session != session
        || state.operation != r.operation
        || state.member != r.member
        || state.phase != AppliedPhase::Prepared
        || state.version != applied.state_version
        || state.gate.risk_gate_revision != applied.risk_gate_revision
    {
        return Err(invalid("query rebuild current state mismatch"));
    }
    let prepared = state
        .rebuild
        .as_ref()
        .and_then(|f| f.result.as_ref())
        .ok_or_else(|| invalid("query rebuild result missing"))?
        .clone();
    Ok(Some(protocol::RebuildObserved {
        schema: 1,
        query: query.clone(),
        applied: applied.clone(),
        prepared,
    }))
}
impl Inbox {
    pub(super) fn observe_rebuild_query(
        &self,
        query: &SignedControlRequest,
        receipt: SignedReceipt,
    ) -> Result<Applied> {
        let applied: protocol::AppliedReceipt = serde_json::from_slice(&receipt.payload)?;
        let state = self
            .states
            .get(&(applied.operation_id, applied.operation_revision));
        match query_observation(state, self.session, query, &applied)? {
            Some(value) => Ok(Applied::RebuildObserved(
                self.signer.sign_rebuild_observed(&value)?,
            )),
            None => Ok(Applied::Legacy(receipt)),
        }
    }
    pub(super) fn record_rebuild_observed_outcome(
        &mut self,
        r: &protocol::ControlRequest,
        hash: [u8; 32],
        receipt: protocol::SignedRebuildObserved,
    ) -> Result<()> {
        self.persist(&Event::RebuildObservedOutcome {
            session: self.session,
            request_id: r.request_id,
            request_digest: hash,
            receipt: receipt.clone(),
        })?;
        let outcome = Outcome {
            session: self.session,
            digest: hash,
            receipt: None,
            replay: None,
            observed: None,
            rebuild: None,
            rebuild_observed: Some(receipt),
            error: None,
        };
        self.outcomes.insert(r.request_id, outcome.clone());
        self.publish_outcome(r.request_id, &outcome)
    }

    pub(super) fn record_rebuild_outcome(
        &mut self,
        r: &protocol::ControlRequest,
        hash: [u8; 32],
        receipt: protocol::SignedRebuildPrepared,
    ) -> Result<()> {
        self.persist(&Event::RebuildOutcome {
            session: self.session,
            request_id: r.request_id,
            request_digest: hash,
            receipt: receipt.clone(),
        })?;
        let outcome = Outcome {
            session: self.session,
            digest: hash,
            receipt: None,
            replay: None,
            observed: None,
            rebuild: Some(receipt),
            rebuild_observed: None,
            error: None,
        };
        self.outcomes.insert(r.request_id, outcome.clone());
        self.publish_outcome(r.request_id, &outcome)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
