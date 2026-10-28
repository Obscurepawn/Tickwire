use super::*;
use tickwire_release::{Object, rebuild as content};

fn object(root: Object) -> protocol::ReplayObject {
    protocol::ReplayObject {
        digest: root.digest,
        bytes: root.bytes,
    }
}
fn readiness(value: &HandoverReadiness) -> protocol::Readiness {
    protocol::Readiness {
        checkpoint_digest: value.checkpoint_digest,
        exec_report_sequence: value.exec_report_sequence,
        orders_digest: value.orders_digest,
        ledger_sequence: value.ledger_sequence,
        input_seal_digest: value.input_seal_digest,
    }
}
pub(super) fn account_ready(
    account: &tickwire_account::Account,
    sender_halted: bool,
    now: u64,
) -> Result<()> {
    if account.frozen() || sender_halted || now >= account.valid_until() {
        return Err(invalid("rebuild account expired, frozen or sender halted"));
    }
    Ok(())
}
impl Inbox {
    fn rebuild_gate<B: Backend, A: Authority, E: Encoder>(
        &mut self,
        state: &State,
        r: &protocol::ControlRequest,
        gateway: &AccountGateway<B, A, E>,
    ) -> Result<MonoTime> {
        let now = self.replay_gate(state, r)?;
        account_ready(gateway.account(), gateway.halted(), now.nanos())?;
        Ok(now)
    }

    pub(in crate::lifecycle) fn apply_rebuild_prepare<
        S: Strategy,
        B: Backend,
        A: Authority,
        E: Encoder,
    >(
        &mut self,
        signed: &SignedControlRequest,
        r: &protocol::ControlRequest,
        strategy: &mut S,
        gateway: &mut AccountGateway<B, A, E>,
        now: MonoTime,
    ) -> Result<protocol::SignedRebuildPrepared> {
        let key = (r.operation.operation_id, r.operation.operation_revision);
        let mut fresh_input = None;
        let mut state = if let Some(state) = self.states.get(&key) {
            if state.operation != r.operation
                || state.member != r.member
                || state.session != self.session
                || state.phase != AppliedPhase::Prepared
            {
                return Err(invalid("rebuild requires current prepared operation"));
            }
            check_adoption(state, r)?;
            state.clone()
        } else {
            if self.states.len() >= 64 || r.rebuild.is_some() || r.replay.is_some() {
                return Err(invalid("rebuild initial challenge"));
            }
            let input = strategy
                .handover_rebuild_input(&r.operation, &r.member)
                .ok_or_else(|| invalid("actual sealed capture is unavailable"))?;
            account_ready(gateway.account(), gateway.halted(), now.nanos())?;
            let fixed = preflight::qualify(&input, r, gateway, now)?;
            self.sync_replay_objects(input.objects())?;
            let current = self
                .clock
                .sample()
                .map_err(|_| invalid("rebuild clock sample"))?
                .monotonic;
            if current.nanos() >= r.valid_until_ns
                || current.nanos() >= input.request().deadline_mono_ns
            {
                return Err(invalid("rebuild expired during input persistence"));
            }
            if self
                .plane
                .strategy_grant(r.operation.logical_strategy_id)
                .map_err(policy_error)?
                .is_some_and(|g| {
                    g.instance_id != r.member.old_instance
                        || g.strategy_generation != r.member.old_generation
                })
            {
                return Err(invalid("rebuild old manager identity"));
            }
            let gate = self
                .plane
                .begin_handover(HandoverBinding {
                    operation_id: r.operation.operation_id,
                    operation_revision: r.operation.operation_revision,
                    members_digest: r.operation.members_digest()?,
                    logical_strategy_id: r.operation.logical_strategy_id,
                    instance_id: r.member.new_instance,
                    strategy_generation: r.member.new_generation,
                    valid_until_ns: r.member.valid_until_ns,
                })
                .map_err(policy_error)?;
            let state = State {
                operation: r.operation.clone(),
                member: r.member.clone(),
                phase: AppliedPhase::Halted,
                version: 0,
                gate,
                proof: None,
                committed_receipt: None,
                session: self.session,
                replay: None,
                rebuild: Some(Facts {
                    prepare: signed.clone(),
                    fixed,
                    warm: None,
                    installation: None,
                    rollback: None,
                    result: None,
                }),
            };
            self.save_state(state)?;
            fresh_input = Some(input);
            self.states[&key].clone()
        };
        let result = (|| {
            self.rebuild_gate(&state, r, gateway)?;
            if let Some(input) = fresh_input {
                if !strategy.handover_quiesce(&switch(r)) {
                    return Err(invalid("rebuild old strategy quiesce failed"));
                }
                gateway.terminate_unsent_strategy(
                    r.operation.logical_strategy_id,
                    ManagerIdentity {
                        instance: r.member.old_instance,
                        generation: r.member.old_generation,
                    },
                )?;
                preflight::current(
                    input.contract(),
                    input.inputs(),
                    input.manifest(),
                    input.rollback_plans(),
                    r,
                    gateway,
                )?;
                self.rebuild_gate(&state, r, gateway)?;
                let installed = strategy
                    .handover_install_rebuild(input)
                    .ok_or_else(|| invalid("actual same-process rebuild installation failed"))?;
                self.sync_replay_objects(installed.objects())?;
                state.rebuild.as_mut().unwrap().warm =
                    Some(object(installed.verified().installation().warm_completion));
                self.save_state(state.clone())?;
                state = self.states[&key].clone();
                self.rebuild_gate(&state, r, gateway)?;
                state.rebuild.as_mut().unwrap().installation = Some(object(installed.root()));
                self.save_state(state.clone())?;
                state = self.states[&key].clone();
                self.rebuild_gate(&state, r, gateway)?;
                strategy.on_account_history(gateway);
                let completed = strategy
                    .handover_finish_rebuild(installed)
                    .ok_or_else(|| invalid("independent rebuild rollback probe failed"))?;
                self.sync_replay_objects(completed.objects())?;
                state.rebuild.as_mut().unwrap().rollback =
                    Some(object(completed.rollback_completion()));
                state.proof = Some(readiness(completed.readiness()));
                self.save_state(state.clone())?;
                state = self.states[&key].clone();
            } else {
                strategy.on_account_history(gateway);
                let (actual, _) = strategy
                    .handover_checkpoint(r.member.new_instance, r.member.new_generation)
                    .ok_or_else(|| invalid("rebuild target READY refresh failed"))?;
                state.proof = Some(readiness(&actual));
            }
            let proof = state
                .proof
                .as_ref()
                .ok_or_else(|| invalid("rebuild READY missing"))?
                .clone();
            self.capture_rebuild(
                strategy,
                gateway,
                r,
                &proof,
                state.rebuild.as_ref().unwrap(),
            )?;
            self.rebuild_gate(&state, r, gateway)?;
            state.phase = AppliedPhase::Prepared;
            self.save_state(state.clone())?;
            state = self.states[&key].clone();
            let current = self.rebuild_gate(&state, r, gateway)?;
            let facts = state.rebuild.as_ref().unwrap();
            let prepared = self
                .signer
                .sign_rebuild_prepared(&protocol::RebuildPrepared {
                    schema: 1,
                    operation: r.operation.clone(),
                    member: r.member.clone(),
                    prepare: facts.prepare.clone(),
                    challenge: signed.clone(),
                    roots: facts.roots()?,
                    end: facts.fixed.end.clone(),
                    readiness: proof,
                    state_version: state.version,
                    risk_gate_revision: state.gate.risk_gate_revision,
                    issued_ns: current.nanos(),
                    valid_until_ns: r.valid_until_ns.min(r.member.valid_until_ns),
                })?;
            state.rebuild.as_mut().unwrap().result = Some(prepared.clone());
            self.save_state(state.clone())?;
            self.rebuild_gate(&state, r, gateway)?;
            Ok(prepared)
        })();
        if result.is_err() {
            self.halt_state(&mut state, strategy)?;
            self.save_state(state)?;
        }
        result
    }

    pub(in crate::lifecycle) fn capture_rebuild<
        S: Strategy,
        B: Backend,
        A: Authority,
        E: Encoder,
    >(
        &mut self,
        strategy: &mut S,
        gateway: &AccountGateway<B, A, E>,
        r: &protocol::ControlRequest,
        proof: &protocol::Readiness,
        facts: &Facts,
    ) -> Result<()> {
        let want = HandoverReadiness {
            instance_id: r.member.new_instance,
            strategy_generation: r.member.new_generation,
            checkpoint_digest: proof.checkpoint_digest,
            exec_report_sequence: proof.exec_report_sequence,
            orders_digest: proof.orders_digest,
            ledger_sequence: proof.ledger_sequence,
            input_seal_digest: proof.input_seal_digest,
        };
        if gateway.journal_position() != proof.exec_report_sequence
            || gateway.account().ledger_sequence() != proof.ledger_sequence
            || gateway
                .account()
                .strategy_orders_digest(r.operation.logical_strategy_id)
                != proof.orders_digest
            || !strategy.handover_ready(&want)
        {
            return Err(invalid(
                "rebuild READY is not the actual current account boundary",
            ));
        }
        let objects = strategy
            .handover_objects(&want)
            .ok_or_else(|| invalid("rebuild READY objects missing"))?;
        self.sync_replay_objects(&objects)?;
        let get = |root: Object| -> tickwire_release::Result<Vec<u8>> {
            let bytes = self
                .store
                .read_object(root.digest, tickwire_recovery::MAX_OBJECT_BYTES)
                .map_err(|_| tickwire_release::Error::Missing)?;
            root.verify(&bytes, tickwire_release::replay::MAX_BYTES as u64)?;
            Ok(bytes)
        };
        let root = |v: &protocol::ReplayObject| Object {
            digest: v.digest,
            bytes: v.bytes,
        };
        let request = content::install::Request::decode(&get(root(&facts.fixed.request))?)?;
        let plans = content::Plans::decode(&get(root(&facts.fixed.plans))?)?;
        let contract = content::Contract::decode(&get(root(&facts.fixed.contract))?)?;
        let inputs = content::Inputs::decode(&get(root(&facts.fixed.inputs))?)?;
        let rollback = content::rollback::Plans::decode(&get(root(&facts.fixed.rollback_plans))?)?;
        let seal = content::window::Seal::decode(&get(root(&facts.fixed.capture_seal))?)?;
        let manifest: tickwire_release::ReleaseManifest = serde_json::from_slice(
            &self
                .store
                .read_object(r.operation.artifacts_digest, tickwire_release::MAX_DOCUMENT)?,
        )?;
        if request.operation != r.operation
            || request.member != r.member
            || request.capture_completion != root(&facts.fixed.capture_completion)
            || request.capture_seal != root(&facts.fixed.capture_seal)
            || request.capture_receipt != root(&facts.fixed.capture_receipt)
        {
            return Err(invalid("rebuild installation request changed"));
        }
        let installed = content::install::Verified::verify(
            root(
                facts
                    .installation
                    .as_ref()
                    .ok_or_else(|| invalid("installation missing"))?,
            ),
            &request,
            &plans,
            &contract,
            &inputs,
            &manifest,
            &seal,
            get,
        )?;
        if Some(object(installed.installation().warm_completion)) != facts.warm {
            return Err(invalid("rebuild warm root changed"));
        }
        tickwire_runtime::node::verify_rebuild_ready(&installed, &want, &objects)?;
        preflight::current(&contract, &inputs, &manifest, &rollback, r, gateway)?;
        let index = crate::lifecycle::preflight::ObjectIndex::new(&objects)?;
        let current = tickwire_recovery::HandoverSeal::decode(index.get(proof.input_seal_digest)?)?;
        let rollback_root = root(
            facts
                .rollback
                .as_ref()
                .ok_or_else(|| invalid("rollback completion missing"))?,
        );
        if current.rebuild_rollback_digest != Some(rollback_root.digest) {
            return Err(invalid(
                "rebuild READY lacks the actual rollback completion",
            ));
        }
        let captured = tickwire_handover::verify(
            &current,
            index.get(current.checkpoint_digest)?,
            index.get(current.input_digest)?,
            index.get(current.account_input_digest)?,
        )
        .map_err(|_| invalid("rebuild current account closure"))?;
        let history = gateway
            .journal_records_after(0, tickwire_account_journal::MAX_RECORDS)?
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();
        captured
            .matches_history(gateway.baseline(), &history)
            .map_err(|_| invalid("rebuild current account history differs"))?;
        get(rollback_root)?;
        if !strategy.handover_ready(&want) {
            return Err(invalid(
                "rebuild target lost readiness during closure persistence",
            ));
        }
        let fresh = self
            .clock
            .sample()
            .map_err(|_| invalid("rebuild account clock"))?
            .monotonic;
        account_ready(gateway.account(), gateway.halted(), fresh.nanos())?;
        Ok(())
    }
}
