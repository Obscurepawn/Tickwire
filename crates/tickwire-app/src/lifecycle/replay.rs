//! Durable replay facts survive a later permission halt. Each object is synced
//! before the state containing its digest is appended to the lifecycle journal.
use super::*;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Source {
    pub fixed: protocol::ReplayCut,
    pub current: protocol::ReplayCut,
    pub completion: protocol::ReplayObject,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Facts {
    pub prepare: SignedControlRequest,
    pub startup: [u8; 32],
    pub source: Option<Source>,
    pub installation: Option<protocol::ReplayObject>,
    pub rollback: Option<protocol::ReplayObject>,
    pub result: Option<protocol::SignedReplayPrepared>,
}
impl Facts {
    pub(super) fn follows(&self, previous: Option<&Self>) -> Result<()> {
        if self.startup == [0; 32]
            || self.prepare.payload.is_empty()
            || self.prepare.signature.len() != 64
            || self.installation.is_some() && self.source.is_none()
            || self.rollback.is_some() && self.installation.is_none()
            || self.result.is_some() && self.rollback.is_none()
        {
            return Err(invalid("replay completion ordering"));
        }
        let stage = usize::from(self.source.is_some())
            + usize::from(self.installation.is_some())
            + usize::from(self.rollback.is_some());
        let before = previous.map_or(0, |old| {
            usize::from(old.source.is_some())
                + usize::from(old.installation.is_some())
                + usize::from(old.rollback.is_some())
        });
        if (previous.is_none() && stage != 0) || stage > before + 1 {
            return Err(invalid("replay skipped a durable completion boundary"));
        }
        if let Some(done) = &self.result {
            let adopted = protocol::ReplayAdoption::from_receipt(done)?;
            let source = self
                .source
                .as_ref()
                .ok_or_else(|| invalid("result lacks source"))?;
            if adopted.prepare != self.prepare
                || adopted.fixed != source.fixed
                || adopted.current != source.current
                || adopted.source_completion != source.completion
                || Some(adopted.target_install) != self.installation
            {
                return Err(invalid("replay receipt differs from durable facts"));
            }
        }
        if let Some(old) = previous
            && (self.prepare != old.prepare
                || self.startup != old.startup
                || old
                    .source
                    .as_ref()
                    .is_some_and(|v| self.source.as_ref() != Some(v))
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
            return Err(invalid("durable replay fact cannot be erased or rebound"));
        }
        Ok(())
    }
}

/// Shared by the running Inbox and fault tests: memory cannot advance before
/// the journal confirms its physical completion.
pub(super) fn append_state<W: tickwire_journal::store::Store>(
    wal: &mut protocol::EventJournal<W>,
    states: &mut BTreeMap<([u8; 16], u64), State>,
    mut state: State,
) -> Result<()> {
    let key = (
        state.operation.operation_id,
        state.operation.operation_revision,
    );
    state.version = state
        .version
        .checked_add(1)
        .ok_or_else(|| invalid("lifecycle state version"))?;
    if let Some(facts) = &state.replay {
        facts.follows(states.get(&key).and_then(|s| s.replay.as_ref()))?;
    } else if states.get(&key).is_some_and(|s| s.replay.is_some()) {
        return Err(invalid("replay facts missing"));
    }
    rebuild::validate_state(&state, states.get(&key))?;
    wal.append(&Event::State {
        state: state.clone(),
    })?;
    states.insert(key, state);
    Ok(())
}
pub(super) fn close_after_restart(state: &mut State) -> Result<()> {
    state.phase = AppliedPhase::Halted;
    state.gate.risk_gate_revision = state
        .gate
        .risk_gate_revision
        .checked_add(1)
        .ok_or_else(|| invalid("recovered risk gate revision"))?;
    Ok(())
}
pub(super) fn saved_result(
    state: Option<&State>,
    session: u64,
    signed: &SignedControlRequest,
) -> Result<Option<protocol::SignedReplayPrepared>> {
    let Some(result) = state
        .filter(|s| s.session == session)
        .and_then(|s| s.replay.as_ref())
        .and_then(|f| f.result.as_ref())
    else {
        return Ok(None);
    };
    let prepared: protocol::ReplayPrepared = serde_json::from_slice(&result.payload)?;
    Ok((prepared.challenge == *signed).then(|| result.clone()))
}
fn object(o: tickwire_release::Object) -> protocol::ReplayObject {
    protocol::ReplayObject {
        digest: o.digest,
        bytes: o.bytes,
    }
}
fn readiness(p: &HandoverReadiness) -> protocol::Readiness {
    protocol::Readiness {
        checkpoint_digest: p.checkpoint_digest,
        exec_report_sequence: p.exec_report_sequence,
        orders_digest: p.orders_digest,
        ledger_sequence: p.ledger_sequence,
        input_seal_digest: p.input_seal_digest,
    }
}
pub(super) fn check_adoption(state: &State, r: &protocol::ControlRequest) -> Result<()> {
    let facts = state
        .replay
        .as_ref()
        .ok_or_else(|| invalid("replay facts missing"))?;
    let signed = facts
        .result
        .as_ref()
        .ok_or_else(|| invalid("replay result is not durable"))?;
    let adopted = protocol::ReplayAdoption::from_receipt(signed)?;
    if r.replay.as_ref() != Some(&adopted) || r.proof.as_ref() != Some(&adopted.readiness) {
        return Err(invalid("coordinator did not adopt the exact replay result"));
    }
    Ok(())
}
/// Only the freshly applied query from this execution session may wrap history.
fn query_observation(
    state: Option<&State>,
    session: u64,
    query: &SignedControlRequest,
    applied: &protocol::AppliedReceipt,
) -> Result<Option<protocol::ReplayObserved>> {
    let r: protocol::ControlRequest = serde_json::from_slice(&query.payload)?;
    if r.operation.schema != 3
        || r.action != ControlAction::QueryApplied
        || applied.phase != AppliedPhase::Prepared
    {
        return Ok(None);
    }
    let state = state.ok_or_else(|| invalid("query replay state missing"))?;
    if state.session != session
        || state.operation != r.operation
        || state.member != r.member
        || state.phase != AppliedPhase::Prepared
        || state.version != applied.state_version
        || state.gate.risk_gate_revision != applied.risk_gate_revision
    {
        return Err(invalid("query replay current state mismatch"));
    }
    let prepared = state
        .replay
        .as_ref()
        .and_then(|f| f.result.as_ref())
        .ok_or_else(|| invalid("query replay result missing"))?
        .clone();
    Ok(Some(protocol::ReplayObserved {
        schema: 1,
        query: query.clone(),
        applied: applied.clone(),
        prepared,
    }))
}
impl Inbox {
    pub(super) fn observe_replay_query(
        &self,
        query: &SignedControlRequest,
        receipt: SignedReceipt,
    ) -> Result<Applied> {
        let applied: protocol::AppliedReceipt = serde_json::from_slice(&receipt.payload)?;
        let state = self
            .states
            .get(&(applied.operation_id, applied.operation_revision));
        match query_observation(state, self.session, query, &applied)? {
            Some(value) => Ok(Applied::Observed(self.signer.sign_replay_observed(&value)?)),
            None => Ok(Applied::Legacy(receipt)),
        }
    }
    pub(super) fn record_observed_outcome(
        &mut self,
        r: &protocol::ControlRequest,
        hash: [u8; 32],
        receipt: protocol::SignedReplayObserved,
    ) -> Result<()> {
        self.persist(&Event::ReplayObservedOutcome {
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
            observed: Some(receipt),
            rebuild: None,
            rebuild_observed: None,
            error: None,
        };
        self.outcomes.insert(r.request_id, outcome.clone());
        self.publish_outcome(r.request_id, &outcome)
    }

    pub(super) fn sync_replay_objects(&mut self, objects: &[Vec<u8>]) -> Result<()> {
        if objects.len() > 512 {
            return Err(invalid("replay object capacity"));
        }
        let total = objects
            .iter()
            .try_fold(0usize, |n, b| n.checked_add(b.len()))
            .ok_or_else(|| invalid("replay byte capacity"))?;
        if total > tickwire_recovery::MAX_CLOSURE_BYTES as usize {
            return Err(invalid("replay byte capacity"));
        }
        for b in objects {
            self.store.put_object(b)?;
        }
        Ok(())
    }
    pub(super) fn replay_gate(
        &mut self,
        state: &State,
        r: &protocol::ControlRequest,
    ) -> Result<MonoTime> {
        let now = self
            .clock
            .sample()
            .map_err(|_| invalid("replay clock"))?
            .monotonic;
        if now.nanos() >= r.valid_until_ns
            || now.nanos() >= r.member.valid_until_ns
            || !self
                .plane
                .handover_is_current(&state.gate, now)
                .map_err(policy_error)?
        {
            return Err(invalid("replay deadline or gate changed"));
        }
        Ok(now)
    }
    pub(super) fn apply_replay_prepare<S: Strategy, B: Backend, A: Authority, E: Encoder>(
        &mut self,
        signed: &SignedControlRequest,
        r: &protocol::ControlRequest,
        strategy: &mut S,
        gateway: &mut AccountGateway<B, A, E>,
        now: MonoTime,
    ) -> Result<protocol::SignedReplayPrepared> {
        let key = (r.operation.operation_id, r.operation.operation_revision);
        let mut state = if let Some(state) = self.states.get(&key) {
            if state.operation != r.operation
                || state.member != r.member
                || state.session != self.session
                || state.phase != AppliedPhase::Prepared
            {
                return Err(invalid("replay operation is not refreshable"));
            }
            check_adoption(state, r)?;
            state.clone()
        } else {
            if self.states.len() >= 64 || r.replay.is_some() {
                return Err(invalid("initial replay operation"));
            }
            // This is a real target startup/restore proof, not a C rollback proof.
            let startup = preflight::qualify_startup(r, strategy, gateway)?;
            self.sync_replay_objects(&startup.objects)?;
            let root = digest(&startup.completion.bytes()?);
            let binding = HandoverBinding {
                operation_id: r.operation.operation_id,
                operation_revision: r.operation.operation_revision,
                members_digest: r.operation.members_digest()?,
                logical_strategy_id: r.operation.logical_strategy_id,
                instance_id: r.member.new_instance,
                strategy_generation: r.member.new_generation,
                valid_until_ns: r.member.valid_until_ns,
            };
            if self
                .plane
                .strategy_grant(r.operation.logical_strategy_id)
                .map_err(policy_error)?
                .is_some_and(|g| {
                    g.instance_id != r.member.old_instance
                        || g.strategy_generation != r.member.old_generation
                })
            {
                return Err(invalid("replay old manager identity"));
            }
            let gate = self.plane.begin_handover(binding).map_err(policy_error)?;
            let state = State {
                operation: r.operation.clone(),
                member: r.member.clone(),
                phase: AppliedPhase::Halted,
                version: 0,
                gate,
                proof: None,
                committed_receipt: None,
                session: self.session,
                rebuild: None,
                replay: Some(Facts {
                    prepare: signed.clone(),
                    startup: root,
                    source: None,
                    installation: None,
                    rollback: None,
                    result: None,
                }),
            };
            self.save_state(state)?;
            self.states[&key].clone()
        };
        let result = (|| {
            if !self
                .plane
                .handover_is_current(&state.gate, now)
                .map_err(policy_error)?
            {
                return Err(invalid("replay gate closed by another operation"));
            }
            if state.replay.as_ref().is_some_and(|f| f.source.is_none()) {
                if !strategy.handover_quiesce(&switch(r)) {
                    return Err(invalid("old strategy quiesce failed"));
                }
                gateway.terminate_unsent_strategy(
                    r.operation.logical_strategy_id,
                    ManagerIdentity {
                        instance: r.member.old_instance,
                        generation: r.member.old_generation,
                    },
                )?;
                let startup_root = state.replay.as_ref().unwrap().startup;
                let startup = tickwire_release::replay::startup::verify(startup_root, |h| {
                    self.store
                        .read_object(h, tickwire_recovery::MAX_OBJECT_BYTES)
                        .map_err(|_| tickwire_release::Error::Missing)
                })?;
                let fixed = preflight::fixed_ready(
                    &startup.bundle,
                    &r.operation,
                    &r.member,
                    &startup.objects,
                )?;
                let source = strategy
                    .handover_replay_source(&r.operation, &r.member, &fixed)
                    .ok_or_else(|| invalid("actual old binary replay did not complete"))?;
                source.bind_release(&startup.bundle, &r.operation)?;
                let objects = source.objects().map(Vec::from).collect::<Vec<_>>();
                self.sync_replay_objects(&objects)?;
                let facts = state.replay.as_mut().unwrap();
                facts.source = Some(Source {
                    fixed: protocol::ReplayCut {
                        checkpoint: object(tickwire_release::Object::of(&source.fixed().bytes()?)),
                        sequence: source.fixed().cut.sequence,
                        chain: source.fixed().cut.chain,
                    },
                    current: protocol::ReplayCut {
                        checkpoint: object(tickwire_release::Object::of(
                            &source.current().bytes()?,
                        )),
                        sequence: source.current().cut.sequence,
                        chain: source.current().cut.chain,
                    },
                    completion: object(source.root()),
                });
                self.save_state(state.clone())?;
                state = self.states[&key].clone();
                self.replay_gate(&state, r)?;
                let installed = strategy.handover_install_replay(&source).ok_or_else(|| {
                    invalid("target replay installation is unconfirmed; keep both instances closed")
                })?;
                self.sync_replay_objects(installed.objects())?;
                state.replay.as_mut().unwrap().installation = Some(object(installed.root()));
                self.save_state(state.clone())?;
                state = self.states[&key].clone();
                self.replay_gate(&state, r)?;
                let completed = strategy
                    .handover_finish_replay(installed)
                    .ok_or_else(|| invalid("C rollback probe or final READY is unconfirmed"))?;
                self.sync_replay_objects(completed.objects())?;
                state.replay.as_mut().unwrap().rollback = Some(object(completed.root()));
                state.proof = Some(readiness(completed.readiness()));
                self.save_state(state.clone())?;
                state = self.states[&key].clone();
            } else {
                // A new challenge may refresh consumption only. The old S/C and
                // installed state remain fixed; no replay or factory call occurs.
                let (p, _) = strategy
                    .handover_checkpoint(r.member.new_instance, r.member.new_generation)
                    .ok_or_else(|| invalid("installed target cannot refresh READY"))?;
                state.proof = Some(readiness(&p));
            }
            let facts = state
                .replay
                .as_ref()
                .ok_or_else(|| invalid("replay stage missing"))?;
            let proof = state
                .proof
                .as_ref()
                .ok_or_else(|| invalid("installed readiness missing"))?;
            self.capture_replay(strategy, gateway, r, proof, facts)?;
            self.replay_gate(&state, r)?;
            state.phase = AppliedPhase::Prepared;
            self.save_state(state.clone())?;
            state = self.states[&key].clone();
            let current = self.replay_gate(&state, r)?;
            let facts = state.replay.as_ref().unwrap();
            let source = facts
                .source
                .as_ref()
                .ok_or_else(|| invalid("durable source missing"))?;
            let receipt = self
                .signer
                .sign_replay_prepared(&protocol::ReplayPrepared {
                    schema: 1,
                    operation: r.operation.clone(),
                    member: r.member.clone(),
                    prepare: facts.prepare.clone(),
                    challenge: signed.clone(),
                    fixed: source.fixed.clone(),
                    current: source.current.clone(),
                    source_completion: source.completion.clone(),
                    target_install: facts
                        .installation
                        .clone()
                        .ok_or_else(|| invalid("durable installation missing"))?,
                    readiness: state.proof.clone().unwrap(),
                    state_version: state.version,
                    risk_gate_revision: state.gate.risk_gate_revision,
                    issued_ns: current.nanos(),
                    valid_until_ns: r.valid_until_ns.min(r.member.valid_until_ns),
                })?;
            state.replay.as_mut().unwrap().result = Some(receipt.clone());
            self.save_state(state.clone())?;
            Ok(receipt)
        })();
        if result.is_err() {
            // Even a failure after the install or rollback sync keeps those
            // immutable facts. Restart never reruns that attempt.
            self.halt_state(&mut state, strategy)?;
            self.save_state(state)?;
        }
        result
    }
    pub(super) fn capture_replay<S: Strategy, B: Backend, A: Authority, E: Encoder>(
        &mut self,
        strategy: &mut S,
        gateway: &AccountGateway<B, A, E>,
        r: &protocol::ControlRequest,
        p: &protocol::Readiness,
        facts: &Facts,
    ) -> Result<()> {
        if gateway.journal_position() != p.exec_report_sequence
            || gateway.account().ledger_sequence() != p.ledger_sequence
            || gateway
                .account()
                .strategy_orders_digest(r.operation.logical_strategy_id)
                != p.orders_digest
        {
            return Err(invalid("installed strategy is behind current account"));
        }
        let want = HandoverReadiness {
            instance_id: r.member.new_instance,
            strategy_generation: r.member.new_generation,
            checkpoint_digest: p.checkpoint_digest,
            exec_report_sequence: p.exec_report_sequence,
            orders_digest: p.orders_digest,
            ledger_sequence: p.ledger_sequence,
            input_seal_digest: p.input_seal_digest,
        };
        if !strategy.handover_ready(&want) {
            return Err(invalid("installed target READY is not authenticated"));
        }
        let objects = strategy
            .handover_objects(&want)
            .ok_or_else(|| invalid("installed target objects missing"))?;
        self.sync_replay_objects(&objects)?;
        let mut get = |h| {
            self.store
                .read_object(h, tickwire_recovery::MAX_OBJECT_BYTES)
                .map_err(|_| tickwire_release::Error::Missing)
        };
        let seal = tickwire_recovery::HandoverSeal::decode(&get(p.input_seal_digest)?)?;
        if seal.version != 3
            || seal.instance != want.instance_id
            || seal.generation != want.strategy_generation
            || seal.logical_strategy != r.operation.logical_strategy_id
            || seal.report_position != p.exec_report_sequence
            || seal.ledger_sequence != p.ledger_sequence
            || seal.orders_digest != p.orders_digest
            || seal.checkpoint_digest != p.checkpoint_digest
            || seal.replay_restore_digest != Some(facts.startup)
            || seal.replay_installation_digest != facts.installation.as_ref().map(|o| o.digest)
            || seal.replay_rollback_digest != facts.rollback.as_ref().map(|o| o.digest)
            || facts.rollback.is_none()
        {
            return Err(invalid("installed target stage binding"));
        }
        tickwire_release::replay::startup::verify_seal(
            &seal,
            &get(seal.checkpoint_digest)?,
            &get(seal.input_digest)?,
            &mut get,
        )?;
        let rollback = tickwire_release::replay::rollback::verify(
            seal.replay_rollback_digest.unwrap(),
            &mut get,
        )?;
        if rollback.installation.request.operation != r.operation
            || rollback.installation.request.member != r.member
            || Some(object(rollback.installation.request.source_completion))
                != facts.source.as_ref().map(|s| s.completion.clone())
        {
            return Err(invalid("replay result operation binding"));
        }
        let bundle = tickwire_release::ReleaseBundle::decode(
            &get(r.operation.artifacts_digest)?,
            &get(r.operation.checkpoint_digest)?,
            &get(r.operation.rollback_digest)?,
        )?;
        let release = bundle.verify_operation(&r.operation, &r.member)?;
        let needs = preflight::requirements(
            gateway.account(),
            gateway.baseline().catalog(),
            r.operation.logical_strategy_id,
            gateway
                .journal_records_after(0, tickwire_account_journal::MAX_RECORDS)?
                .map(|(_, r)| r),
        )?;
        release.check_capabilities(&release.target.capabilities, &needs)?;
        let inputs = tickwire_handover::verify(
            &seal,
            &get(seal.checkpoint_digest)?,
            &get(seal.input_digest)?,
            &get(seal.account_input_digest)?,
        )
        .map_err(|_| invalid("installed input closure"))?;
        let history = gateway
            .journal_records_after(0, tickwire_account_journal::MAX_RECORDS)?
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();
        inputs
            .matches_history(gateway.baseline(), &history)
            .map_err(|_| invalid("installed account prefix differs"))?;
        Ok(())
    }
    pub(super) fn record_replay_outcome(
        &mut self,
        r: &protocol::ControlRequest,
        hash: [u8; 32],
        receipt: protocol::SignedReplayPrepared,
    ) -> Result<()> {
        self.persist(&Event::ReplayOutcome {
            session: self.session,
            request_id: r.request_id,
            request_digest: hash,
            receipt: receipt.clone(),
        })?;
        let outcome = Outcome {
            session: self.session,
            digest: hash,
            receipt: None,
            replay: Some(receipt),
            observed: None,
            rebuild: None,
            rebuild_observed: None,
            error: None,
        };
        self.outcomes.insert(r.request_id, outcome.clone());
        self.publish_outcome(r.request_id, &outcome)?;
        Ok(())
    }
}
#[cfg(test)]
pub(super) mod tests {
    use super::*;
    fn object(n: u8) -> protocol::ReplayObject {
        protocol::ReplayObject {
            digest: [n; 32],
            bytes: 1,
        }
    }
    fn source() -> Source {
        Source {
            fixed: protocol::ReplayCut {
                checkpoint: object(1),
                sequence: 0,
                chain: [0; 32],
            },
            current: protocol::ReplayCut {
                checkpoint: object(2),
                sequence: 1,
                chain: [3; 32],
            },
            completion: object(4),
        }
    }
    fn facts() -> Facts {
        Facts {
            prepare: SignedControlRequest {
                payload: vec![1],
                signature: vec![2; 64],
            },
            startup: [5; 32],
            source: None,
            installation: None,
            rollback: None,
            result: None,
        }
    }
    #[test]
    fn installation_cannot_precede_the_durable_source_cut() {
        let old = facts();
        let mut next = old.clone();
        next.installation = Some(object(6));
        assert!(next.follows(Some(&old)).is_err());
    }
    #[test]
    fn permission_halt_must_not_erase_an_installed_fact() {
        let mut old = facts();
        old.source = Some(source());
        old.installation = Some(object(6));
        let mut next = old.clone();
        next.installation = None;
        assert!(next.follows(Some(&old)).is_err());
    }
    #[test]
    fn source_cut_and_original_challenge_cannot_be_rebound() {
        let mut old = facts();
        old.source = Some(source());
        let mut next = old.clone();
        next.source.as_mut().unwrap().current.sequence = 2;
        assert!(next.follows(Some(&old)).is_err());
        let mut next = old.clone();
        next.prepare.payload.push(3);
        assert!(next.follows(Some(&old)).is_err());
    }
    #[derive(Default)]
    struct Disk {
        manifest: Option<Vec<u8>>,
        segments: BTreeMap<u64, Vec<u8>>,
        fail_after: Option<usize>,
    }
    #[derive(Clone, Default)]
    pub(in crate::lifecycle) struct Store(std::rc::Rc<std::cell::RefCell<Disk>>);
    impl Store {
        pub(in crate::lifecycle) fn fail_after(&self, n: usize) {
            self.0.borrow_mut().fail_after = Some(n);
        }
    }
    impl tickwire_journal::store::Store for Store {
        fn read_manifest(&mut self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(self.0.borrow().manifest.clone())
        }
        fn publish_manifest(&mut self, b: &[u8]) -> std::io::Result<()> {
            self.0.borrow_mut().manifest = Some(b.to_vec());
            Ok(())
        }
        fn segment_ids(&mut self) -> std::io::Result<Vec<u64>> {
            Ok(self.0.borrow().segments.keys().copied().collect())
        }
        fn read_segment(&mut self, id: u64, max: u64) -> std::io::Result<Vec<u8>> {
            let b = self
                .0
                .borrow()
                .segments
                .get(&id)
                .cloned()
                .ok_or_else(|| std::io::Error::other("missing"))?;
            if b.len() as u64 > max {
                return Err(std::io::Error::other("size"));
            }
            Ok(b)
        }
        fn create_segment(&mut self, id: u64) -> std::io::Result<()> {
            if self.0.borrow_mut().segments.insert(id, vec![]).is_some() {
                return Err(std::io::Error::other("exists"));
            }
            Ok(())
        }
        fn append_segment(&mut self, id: u64, off: u64, bytes: &[u8]) -> std::io::Result<()> {
            let mut d = self.0.borrow_mut();
            let b = d
                .segments
                .get_mut(&id)
                .ok_or_else(|| std::io::Error::other("missing"))?;
            if b.len() as u64 != off {
                return Err(std::io::Error::other("offset"));
            }
            b.extend_from_slice(bytes);
            Ok(())
        }
        fn sync_segment(&mut self, _: u64) -> std::io::Result<()> {
            let mut d = self.0.borrow_mut();
            if let Some(n) = d.fail_after.as_mut() {
                if *n == 0 {
                    d.fail_after = None;
                    return Err(std::io::Error::other("lost sync reply"));
                }
                *n -= 1;
            }
            Ok(())
        }
        fn truncate_segment(&mut self, id: u64, n: u64) -> std::io::Result<()> {
            self.0
                .borrow_mut()
                .segments
                .get_mut(&id)
                .ok_or_else(|| std::io::Error::other("missing"))?
                .truncate(n as usize);
            Ok(())
        }
    }
    #[test]
    fn uncertain_install_stage_sync_keeps_memory_old_and_restart_closes_recovered_fact() {
        let disk = Store::default();
        let cfg = tickwire_journal::Config {
            domain: [1; 16],
            stream_epoch: [2; 16],
            segment_max_bytes: 1 << 20,
        };
        let mut wal = protocol::EventJournal::create(disk.clone(), cfg.clone(), 1).unwrap();
        let mut states = BTreeMap::new();
        let mut state = super::super::operation_halt_tests::state(
            &super::super::operation_halt_tests::plane(),
            6,
        );
        state.phase = AppliedPhase::Halted;
        state.replay = Some(facts());
        append_state(&mut wal, &mut states, state.clone()).unwrap();
        let key = (
            state.operation.operation_id,
            state.operation.operation_revision,
        );
        state = states[&key].clone();
        state.replay.as_mut().unwrap().source = Some(source());
        append_state(&mut wal, &mut states, state).unwrap();
        let mut next = states[&key].clone();
        next.replay.as_mut().unwrap().installation = Some(object(6));
        disk.0.borrow_mut().fail_after = Some(1); // data sync succeeds; commit sync reply is lost
        assert!(append_state(&mut wal, &mut states, next).is_err());
        assert!(states[&key].replay.as_ref().unwrap().installation.is_none());
        drop(wal);
        let (_, events) = protocol::EventJournal::recover::<Event>(disk, &cfg, 2, None).unwrap();
        let Event::State { mut state } = events.into_iter().last().unwrap() else {
            panic!("state expected")
        };
        assert_eq!(state.replay.as_ref().unwrap().installation, Some(object(6)));
        let recorded = state.replay.clone();
        close_after_restart(&mut state).unwrap();
        assert_eq!(state.phase, AppliedPhase::Halted);
        assert_eq!(state.replay, recorded);
    }
    #[test]
    fn durable_boundaries_cannot_be_skipped_even_with_all_roots_present() {
        let old = facts();
        let mut next = old.clone();
        next.source = Some(source());
        next.installation = Some(object(6));
        next.rollback = Some(object(7));
        assert!(next.follows(Some(&old)).is_err());
        assert!(next.follows(None).is_err());
    }
    fn completed() -> (State, protocol::ControlRequest, SignedControlRequest) {
        let mut state = super::super::operation_halt_tests::state(
            &super::super::operation_halt_tests::plane(),
            6,
        );
        state.operation.schema = 3;
        let ready = protocol::Readiness {
            checkpoint_digest: [21; 32],
            exec_report_sequence: 7,
            orders_digest: [22; 32],
            ledger_sequence: 7,
            input_seal_digest: [23; 32],
        };
        let initial = protocol::ControlRequest {
            schema: 2,
            coordinator_id: [30; 16],
            operation: state.operation.clone(),
            member: state.member.clone(),
            request_id: [31; 16],
            action: ControlAction::Prepare,
            proof: Some(ready.clone()),
            release: None,
            replay: None,
            rebuild: None,
            valid_until_ns: 1000,
        };
        let signed = SignedControlRequest {
            payload: serde_json::to_vec(&initial).unwrap(),
            signature: vec![9; 64],
        };
        let source = source();
        let body = protocol::ReplayPrepared {
            schema: 1,
            operation: state.operation.clone(),
            member: state.member.clone(),
            prepare: signed.clone(),
            challenge: signed.clone(),
            fixed: source.fixed.clone(),
            current: source.current.clone(),
            source_completion: source.completion.clone(),
            target_install: object(6),
            readiness: ready.clone(),
            state_version: 3,
            risk_gate_revision: 1,
            issued_ns: 20,
            valid_until_ns: 1000,
        };
        let result = protocol::SignedReplayPrepared {
            payload: serde_json::to_vec(&body).unwrap(),
            signature: vec![10; 64],
        };
        state.proof = Some(ready);
        state.replay = Some(Facts {
            prepare: signed.clone(),
            startup: [5; 32],
            source: Some(source),
            installation: Some(object(6)),
            rollback: Some(object(7)),
            result: Some(result.clone()),
        });
        let mut commit = initial;
        commit.request_id = [32; 16];
        commit.action = ControlAction::Commit;
        commit.replay = Some(protocol::ReplayAdoption::from_receipt(&result).unwrap());
        (state, commit, signed)
    }
    #[test]
    fn commit_needs_the_exact_durably_adopted_result_and_readiness() {
        let (state, mut request, _) = completed();
        check_adoption(&state, &request).unwrap();
        let saved = request.replay.clone();
        request.replay = None;
        assert!(check_adoption(&state, &request).is_err());
        request.replay = saved;
        request.replay.as_mut().unwrap().current.sequence += 1;
        assert!(check_adoption(&state, &request).is_err());
        let (state, mut request, _) = completed();
        request.proof.as_mut().unwrap().exec_report_sequence += 1;
        assert!(check_adoption(&state, &request).is_err());
    }
    #[test]
    fn completed_result_retries_preserve_original_bytes_without_a_new_install() {
        let (state, _, mut original) = completed();
        let expected = state.replay.as_ref().unwrap().result.clone();
        assert_eq!(
            saved_result(Some(&state), state.session, &original).unwrap(),
            expected
        );
        assert!(
            saved_result(Some(&state), state.session + 1, &original)
                .unwrap()
                .is_none()
        );
        original.payload.push(0);
        assert!(
            saved_result(Some(&state), state.session, &original)
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn fresh_query_wraps_saved_history_only_in_the_same_prepared_session() {
        let (mut state, mut query, _) = completed();
        state.phase = AppliedPhase::Prepared;
        state.version = 3;
        query.action = ControlAction::QueryApplied;
        query.proof = None;
        query.replay = None;
        let signed = SignedControlRequest {
            payload: serde_json::to_vec(&query).unwrap(),
            signature: vec![1; 64],
        };
        let applied = protocol::AppliedReceipt {
            schema: 1,
            operation_id: query.operation.operation_id,
            operation_revision: query.operation.operation_revision,
            members_digest: query.operation.members_digest().unwrap(),
            logical_strategy_id: query.operation.logical_strategy_id,
            member: query.member.clone(),
            phase: AppliedPhase::Prepared,
            state_version: 3,
            handover_sequence: 0,
            handover_digest: [0; 32],
            risk_gate_revision: state.gate.risk_gate_revision,
            control_request_id: query.request_id,
            issued_ns: 500,
            valid_until_ns: 1000,
        };
        let observed = query_observation(Some(&state), state.session, &signed, &applied)
            .unwrap()
            .unwrap();
        assert_eq!(
            Some(observed.prepared),
            state.replay.as_ref().unwrap().result
        );
        assert!(query_observation(Some(&state), state.session + 1, &signed, &applied).is_err());
        let mut halted = applied.clone();
        halted.phase = AppliedPhase::Halted;
        assert!(
            query_observation(Some(&state), state.session, &signed, &halted)
                .unwrap()
                .is_none()
        );
        state.replay.as_mut().unwrap().result = None;
        assert!(query_observation(Some(&state), state.session, &signed, &applied).is_err());
    }
}
