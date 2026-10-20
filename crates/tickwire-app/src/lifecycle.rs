//! Authenticated local lifecycle decisions applied by the actual account owner.
use crate::{Result, config::read_file, invalid};
#[path = "release_preflight.rs"]
mod preflight;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
};
use tickwire_account::{ManagerIdentity, RecoveryRecord, StrategyHandover};
use tickwire_account_gateway::{AccountGateway, Authority, Encoder};
use tickwire_account_journal::Backend;
use tickwire_clock::{ClockDomain, MonoTime};
use tickwire_journal::store::FileStore;
use tickwire_lifecycle::{
    self as protocol, AppliedPhase, ControlAction, SignedControlRequest, SignedReceipt,
};
use tickwire_recovery::{Persistence, digest};
use tickwire_runtime::{
    files::SharedRecoveryStore,
    node::{HandoverReadiness, Strategy, StrategySwitch},
};
use tickwire_runtime_policy::{ControlPlane, HandoverBinding, HandoverGate};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The coordinator's directory for this risk domain only.
    pub requests_directory: PathBuf,
    pub receipts_directory: PathBuf,
    pub journal_directory: PathBuf,
    pub stream_epoch: [u8; 16],
    pub recover: bool,
    pub segment_max_bytes: u64,
    pub coordinator_id: [u8; 16],
    pub coordinator_public_key: [u8; 32],
    pub signing_key_pkcs8: PathBuf,
    pub maximum_requests: usize,
    pub maximum_per_poll: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    operation: protocol::Operation,
    member: protocol::Member,
    phase: AppliedPhase,
    version: u64,
    gate: HandoverGate,
    proof: Option<protocol::Readiness>,
    committed_receipt: Option<SignedReceipt>,
    /// This process session alone may release this state.
    session: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
// Journal events are processed one at a time with an explicitly bounded size.
#[allow(clippy::large_enum_variant)]
enum Event {
    Bootstrap {
        binding: [u8; 32],
    },
    Session {
        number: u64,
    },
    Decision {
        session: u64,
        authenticated: bool,
        request: SignedControlRequest,
    },
    State {
        state: State,
    },
    Outcome {
        session: u64,
        request_id: [u8; 16],
        request_digest: [u8; 32],
        receipt: Option<SignedReceipt>,
        error: Option<String>,
    },
}
#[derive(Clone)]
struct Outcome {
    session: u64,
    digest: [u8; 32],
    receipt: Option<SignedReceipt>,
    error: Option<String>,
}
pub struct Inbox {
    config: Config,
    plane: ControlPlane,
    wal: protocol::EventJournal<FileStore>,
    store: Persistence<SharedRecoveryStore>,
    signer: protocol::Signer,
    domain: ClockDomain,
    risk_domain: [u8; 16],
    exec_instance: [u8; 16],
    exec_epoch: u64,
    clock: tickwire_binance_clock::SystemClock,
    session: u64,
    states: BTreeMap<([u8; 16], u64), State>,
    outcomes: BTreeMap<[u8; 16], Outcome>,
    decisions: BTreeMap<[u8; 16], [u8; 32]>,
    poisoned: bool,
}
impl Inbox {
    pub fn open(
        config: Config,
        deployment: &crate::config::Deployment,
        plane: ControlPlane,
        store: SharedRecoveryStore,
    ) -> Result<Self> {
        if config.maximum_requests == 0
            || config.maximum_requests > 1024
            || config.maximum_per_poll == 0
            || config.maximum_per_poll > 16
            || config.stream_epoch == [0; 16]
            || config.coordinator_id == [0; 16]
            || config.coordinator_public_key == [0; 32]
        {
            return Err(invalid("lifecycle configuration"));
        }
        for path in [
            &config.requests_directory,
            &config.receipts_directory,
            &config.journal_directory,
        ] {
            private_directory(path)?;
        }
        let domain = deployment.domain()?;
        let risk_domain = deployment.policy.scope.risk_domain_id;
        let physical = tickwire_journal::Config {
            domain: risk_domain,
            stream_epoch: config.stream_epoch,
            segment_max_bytes: config.segment_max_bytes,
        };
        let signer = protocol::Signer::from_pkcs8(&read_file(&config.signing_key_pkcs8, 65536)?)?;
        // Epochs may advance on restart; the immutable authority and account
        // binding cannot change while reusing this journal.
        let binding = digest(&serde_json::to_vec(&(
            1u32,
            risk_domain,
            deployment.account_stream,
            config.coordinator_id,
            config.coordinator_public_key,
            signer.public_key(),
        ))?);
        let backend = FileStore::open(&config.journal_directory)?;
        let (mut wal, events) = if config.recover {
            protocol::EventJournal::recover::<Event>(
                backend,
                &physical,
                deployment.writer_epoch,
                None,
            )?
        } else {
            (
                protocol::EventJournal::create(backend, physical, deployment.writer_epoch)?,
                Vec::new(),
            )
        };
        let mut states = BTreeMap::new();
        let mut outcomes = BTreeMap::new();
        let mut decisions = BTreeMap::new();
        let mut interrupted_strategies = BTreeSet::new();
        let mut session = 0;
        if config.recover {
            if !matches!(events.first(),Some(Event::Bootstrap{binding:b}) if *b==binding) {
                return Err(invalid("lifecycle bootstrap binding"));
            }
            for event in events.into_iter().skip(1) {
                match event {
                    Event::Bootstrap { .. } => {
                        return Err(invalid("duplicate lifecycle bootstrap"));
                    }
                    Event::Session { number } => {
                        if number != session + 1 {
                            return Err(invalid("lifecycle session order"));
                        }
                        session = number;
                    }
                    Event::Decision {
                        session: s,
                        authenticated,
                        request,
                    } => {
                        if s != session {
                            return Err(invalid("lifecycle decision session"));
                        }
                        let decoded: protocol::ControlRequest =
                            serde_json::from_slice(&request.payload)?;
                        if authenticated && decoded.action != ControlAction::QueryApplied {
                            interrupted_strategies.insert(decoded.operation.logical_strategy_id);
                        }
                        let hash = request.digest();
                        if decisions.insert(decoded.request_id, hash).is_some() {
                            return Err(invalid("duplicate lifecycle decision"));
                        }
                    }
                    Event::State { state } => {
                        if state.session != session {
                            return Err(invalid("lifecycle state session"));
                        }
                        let key = (
                            state.operation.operation_id,
                            state.operation.operation_revision,
                        );
                        if states
                            .get(&key)
                            .is_some_and(|s: &State| s.version >= state.version)
                        {
                            return Err(invalid("lifecycle state version"));
                        }
                        states.insert(key, state);
                    }
                    Event::Outcome {
                        session: s,
                        request_id,
                        request_digest,
                        receipt,
                        error,
                    } => {
                        if s != session
                            || decisions.get(&request_id) != Some(&request_digest)
                            || (receipt.is_some() == error.is_some())
                            || outcomes.contains_key(&request_id)
                        {
                            return Err(invalid("lifecycle outcome binding"));
                        }
                        outcomes.insert(
                            request_id,
                            Outcome {
                                session: s,
                                digest: request_digest,
                                receipt,
                                error,
                            },
                        );
                    }
                }
            }
        } else {
            wal.append(&Event::Bootstrap { binding })?;
        }
        if decisions.len() > config.maximum_requests || states.len() > 64 {
            return Err(invalid("lifecycle history capacity"));
        }
        // A previous process's released state never recreates authority. Both
        // its completed and interrupted decisions require fresh reconciliation.
        for strategy in interrupted_strategies {
            plane.stop_strategy(strategy).map_err(policy_error)?;
        }
        for state in states.values_mut() {
            state.phase = AppliedPhase::Halted;
            state.gate.risk_gate_revision = state
                .gate
                .risk_gate_revision
                .checked_add(1)
                .ok_or_else(|| invalid("risk gate revision"))?;
        }
        session = session
            .checked_add(1)
            .ok_or_else(|| invalid("lifecycle session"))?;
        wal.append(&Event::Session { number: session })?;
        for state in states.values_mut() {
            state.session = session;
            state.version = state
                .version
                .checked_add(1)
                .ok_or_else(|| invalid("lifecycle recovered version"))?;
            wal.append(&Event::State {
                state: state.clone(),
            })?;
        }
        let clock = tickwire_binance_clock::SystemClock::new(domain)
            .map_err(|_| invalid("lifecycle system clock"))?;
        Ok(Self {
            config,
            plane,
            wal,
            store: Persistence::new(store),
            signer,
            domain,
            risk_domain,
            clock,
            exec_instance: deployment.owner_id,
            exec_epoch: deployment.policy.owner_generation,
            session,
            states,
            outcomes,
            decisions,
            poisoned: false,
        })
    }
    pub fn poll<S: Strategy, B: Backend, A: Authority, E: Encoder>(
        &mut self,
        strategy: &mut S,
        gateway: &mut AccountGateway<B, A, E>,
        now: MonoTime,
    ) -> Result<usize> {
        if self.poisoned {
            return Err(invalid("lifecycle journal unavailable"));
        }
        if now.domain() != self.domain {
            return Err(invalid("lifecycle clock domain"));
        }
        // A private gap, an explicit revoke, or expiry invalidates an earlier
        // group release before another strategy command can be selected.
        let stale = self
            .states
            .iter()
            .filter_map(|(key, state)| {
                (state.session == self.session
                    && state.phase != AppliedPhase::Halted
                    && !self
                        .plane
                        .handover_is_current(&state.gate, now)
                        .unwrap_or(false))
                .then_some(*key)
            })
            .collect::<Vec<_>>();
        for key in stale {
            let mut state = self.states[&key].clone();
            self.halt_state(&mut state, strategy)?;
            self.save_state(state)?;
        }
        let mut paths = Vec::new();
        for entry in fs::read_dir(&self.config.requests_directory)? {
            let entry = entry?;
            if entry.path().extension().is_some_and(|e| e == "json") {
                paths.push(entry.path());
            }
            if paths.len() > 4096 {
                return Err(invalid("lifecycle inbox capacity"));
            }
        }
        paths.sort();
        let mut count = 0;
        for path in paths {
            if count >= self.config.maximum_per_poll {
                break;
            }
            let signed: SignedControlRequest =
                serde_json::from_slice(&read_file(&path, protocol::MAX_SIGNED_BYTES * 5)?)?;
            // The untrusted decode supplies only the expected member. All fields,
            // including that exact member, are authenticated before any action.
            let decoded: protocol::ControlRequest = serde_json::from_slice(&signed.payload)?;
            if decoded.member.risk_domain != self.risk_domain
                || decoded.member.exec_instance != self.exec_instance
                || decoded.member.exec_epoch != self.exec_epoch
            {
                return Err(invalid("lifecycle recipient identity"));
            }
            let hash = signed.digest();
            if let Some(outcome) = self.outcomes.get(&decoded.request_id) {
                if outcome.digest != hash {
                    return Err(invalid("lifecycle request ID conflict"));
                }
                self.publish_outcome(decoded.request_id, outcome)?;
                continue;
            }
            if let Some(previous) = self.decisions.get(&decoded.request_id) {
                if *previous != hash {
                    return Err(invalid("lifecycle interrupted request conflict"));
                }
                // Never repeat an interrupted mutation under its old challenge.
                self.record_outcome(
                    &decoded,
                    hash,
                    None,
                    Some(
                        "execution session interrupted; query applied state with a new request ID"
                            .into(),
                    ),
                )?;
                count += 1;
                continue;
            }
            if self.decisions.len() >= self.config.maximum_requests {
                return Err(invalid("lifecycle request capacity"));
            }
            let verified = protocol::verify_control_request(
                &signed,
                self.config.coordinator_public_key,
                self.config.coordinator_id,
                &decoded.member,
                now,
            );
            self.persist(&Event::Decision {
                session: self.session,
                authenticated: verified.is_ok(),
                request: signed,
            })?;
            self.decisions.insert(decoded.request_id, hash);
            let verified = match verified {
                Ok(value) => value,
                Err(error) => {
                    self.record_outcome(
                        &decoded,
                        hash,
                        None,
                        Some(format!("control request rejected: {error}")),
                    )?;
                    count += 1;
                    continue;
                }
            };
            let fresh = self
                .clock
                .sample()
                .map_err(|_| invalid("lifecycle clock sample"))?
                .monotonic;
            let applied = if fresh.nanos() >= decoded.valid_until_ns
                && decoded.action != ControlAction::Halt
            {
                Err(invalid("lifecycle request expired before application"))
            } else {
                self.apply(verified.request(), strategy, gateway, fresh)
            };
            match applied {
                Ok(receipt) => self.record_outcome(&decoded, hash, Some(receipt), None)?,
                Err(error) => self.record_outcome(&decoded, hash, None, Some(error.to_string()))?,
            }
            count += 1;
        }
        Ok(count)
    }
    fn apply<S: Strategy, B: Backend, A: Authority, E: Encoder>(
        &mut self,
        r: &protocol::ControlRequest,
        strategy: &mut S,
        gateway: &mut AccountGateway<B, A, E>,
        now: MonoTime,
    ) -> Result<SignedReceipt> {
        let key = (r.operation.operation_id, r.operation.operation_revision);
        let switch = switch(r);
        // Preflight rejection before first Prepare leaves the old owner intact.
        // Once a gate exists, all later errors use the current operation's halt.
        let prepared_release =
            if r.action == ControlAction::Prepare && !self.states.contains_key(&key) {
                Some(preflight::qualify(r, strategy, gateway)?)
            } else {
                None
            };
        let mut state = if let Some(state) = self.states.get(&key) {
            if state.operation != r.operation || state.member != r.member {
                return Err(invalid("lifecycle operation conflict"));
            }
            state.clone()
        } else {
            if !matches!(
                r.action,
                ControlAction::Prepare | ControlAction::QueryApplied | ControlAction::Halt
            ) || self.states.len() >= 64
            {
                return Err(invalid("lifecycle operation not prepared"));
            }
            let binding = HandoverBinding {
                operation_id: r.operation.operation_id,
                operation_revision: r.operation.operation_revision,
                members_digest: r.operation.members_digest()?,
                logical_strategy_id: r.operation.logical_strategy_id,
                instance_id: r.member.new_instance,
                strategy_generation: r.member.new_generation,
                valid_until_ns: r.member.valid_until_ns,
            };
            let gate = if r.action == ControlAction::Prepare {
                if self
                    .plane
                    .strategy_grant(r.operation.logical_strategy_id)
                    .map_err(policy_error)?
                    .is_some_and(|grant| {
                        grant.instance_id != r.member.old_instance
                            || grant.strategy_generation != r.member.old_generation
                    })
                {
                    return Err(invalid("handover does not name the current strategy owner"));
                }
                self.plane.begin_handover(binding).map_err(policy_error)?
            } else {
                HandoverGate {
                    binding,
                    risk_gate_revision: 1,
                    handover_sequence: 0,
                    handover_digest: [0; 32],
                }
            };
            State {
                operation: r.operation.clone(),
                member: r.member.clone(),
                phase: AppliedPhase::Halted,
                version: 0,
                gate,
                proof: None,
                committed_receipt: None,
                session: self.session,
            }
        };
        if state.session != self.session {
            if !matches!(r.action, ControlAction::QueryApplied | ControlAction::Halt) {
                return Err(invalid(
                    "prior execution session requires a new lifecycle operation",
                ));
            }
            state.session = self.session;
            state.phase = AppliedPhase::Halted;
        }
        let result = (|| {
            match r.action {
                ControlAction::Prepare => {
                    let release = match prepared_release {
                        Some(v) => v,
                        None => preflight::qualify(r, strategy, gateway)?,
                    };
                    if state.gate.handover_sequence != 0 {
                        return Err(invalid("ownership is already committed"));
                    }
                    if !self
                        .plane
                        .handover_is_current(&state.gate, now)
                        .map_err(policy_error)?
                    {
                        return Err(invalid("handover gate was revoked; use a new generation"));
                    }
                    if !strategy.handover_quiesce(&switch) {
                        return Err(invalid("old strategy has not quiesced"));
                    }
                    preflight::unchanged(strategy, &release)?;
                    gateway.terminate_unsent_strategy(
                        r.operation.logical_strategy_id,
                        ManagerIdentity {
                            instance: r.member.old_instance,
                            generation: r.member.old_generation,
                        },
                    )?;
                    let proof = r
                        .proof
                        .as_ref()
                        .ok_or_else(|| invalid("missing readiness"))?;
                    self.capture(strategy, gateway, r, proof, &release)?;
                    state.proof = Some(proof.clone());
                    state.phase = AppliedPhase::Prepared;
                }
                ControlAction::Commit => {
                    let release = preflight::qualify(r, strategy, gateway)?;
                    if state.phase != AppliedPhase::Prepared
                        || !self
                            .plane
                            .handover_is_current(&state.gate, now)
                            .map_err(policy_error)?
                    {
                        return Err(invalid("handover is not prepared"));
                    }
                    let proof = r
                        .proof
                        .as_ref()
                        .ok_or_else(|| invalid("missing readiness"))?;
                    if state.proof.as_ref() != Some(proof) {
                        return Err(invalid("readiness changed after prepare"));
                    }
                    self.capture(strategy, gateway, r, proof, &release)?;
                    let now = self
                        .clock
                        .sample()
                        .map_err(|_| invalid("lifecycle clock sample"))?
                        .monotonic;
                    if now.nanos() >= r.valid_until_ns || now.nanos() >= r.member.valid_until_ns {
                        return Err(invalid(
                            "handover deadline expired during checkpoint persistence",
                        ));
                    }
                    let handover = StrategyHandover {
                        scope: gateway.baseline().snapshot().scope.clone(),
                        account_epoch: gateway.baseline().snapshot().account_epoch,
                        writer_epoch: gateway.writer_epoch(),
                        operation_id: r.operation.operation_id,
                        operation_revision: r.operation.operation_revision,
                        logical_strategy_id: r.operation.logical_strategy_id,
                        expected_ledger_sequence: proof.ledger_sequence,
                        expected_orders_digest: proof.orders_digest,
                        from: ManagerIdentity {
                            instance: r.member.old_instance,
                            generation: r.member.old_generation,
                        },
                        to: ManagerIdentity {
                            instance: r.member.new_instance,
                            generation: r.member.new_generation,
                        },
                        checkpoint_digest: proof.checkpoint_digest,
                        input_seal_digest: proof.input_seal_digest,
                        evaluated_at: now.nanos(),
                        valid_until: r.member.valid_until_ns,
                    };
                    gateway.commit_strategy_handover(handover)?;
                    let (position, record) = gateway
                        .journal_records_after(gateway.journal_position() - 1, 1)?
                        .next()
                        .ok_or_else(|| invalid("missing handover WAL record"))?;
                    if !matches!(record.record(),RecoveryRecord::StrategyHandover(h) if h.operation_id==r.operation.operation_id && h.operation_revision==r.operation.operation_revision)
                    {
                        return Err(invalid("handover WAL boundary"));
                    }
                    let before = state.gate.clone();
                    state.gate.handover_sequence = position;
                    state.gate.handover_digest = record.digest();
                    state.gate = self
                        .plane
                        .handover_committed(&before, position, record.digest())
                        .map_err(policy_error)?;
                    state.phase = AppliedPhase::Committed;
                }
                ControlAction::Release => {
                    if state.phase != AppliedPhase::Committed {
                        return Err(invalid("ownership is not committed"));
                    }
                    let release = protocol::verify_group_release_for_member(
                        r.release
                            .as_ref()
                            .ok_or_else(|| invalid("missing group release"))?,
                        self.config.coordinator_public_key,
                        self.config.coordinator_id,
                        &r.operation,
                        &r.member,
                        now,
                    )?;
                    let member = release
                        .member(self.risk_domain)
                        .ok_or_else(|| invalid("group member missing"))?;
                    if member.risk_gate_revision != state.gate.risk_gate_revision
                        || member.handover_sequence != state.gate.handover_sequence
                        || member.handover_digest != state.gate.handover_digest
                        || member.release_request_id != r.request_id
                        || state.committed_receipt.as_ref().map(SignedReceipt::digest)
                            != Some(member.committed_receipt_digest)
                    {
                        return Err(invalid(
                            "group release does not match the applied account handover",
                        ));
                    }
                    let grant = self
                        .plane
                        .strategy_grant(r.operation.logical_strategy_id)
                        .map_err(policy_error)?
                        .ok_or_else(|| invalid("new strategy authorization is not installed"))?;
                    self.plane
                        .release_handover(&state.gate, grant, now)
                        .map_err(policy_error)?;
                    if !strategy.handover_activate(&switch) {
                        self.halt_state(&mut state, strategy)?;
                        return Err(invalid("new strategy could not activate"));
                    }
                    state.phase = AppliedPhase::Released;
                }
                ControlAction::Halt => self.halt_state(&mut state, strategy)?,
                ControlAction::QueryApplied => {
                    if state.phase != AppliedPhase::Halted
                        && !self
                            .plane
                            .handover_is_current(&state.gate, now)
                            .map_err(policy_error)?
                    {
                        self.halt_state(&mut state, strategy)?;
                    }
                    if state.phase == AppliedPhase::Released
                        && !self
                            .plane
                            .handover_is_released(&state.gate, now)
                            .map_err(policy_error)?
                    {
                        self.halt_state(&mut state, strategy)?;
                    }
                    // The account append can survive a crash before this inbox
                    // records Committed. Reconcile from that actual ordered WAL.
                    if let Some(h) = gateway
                        .account()
                        .handover(r.operation.operation_id, r.operation.operation_revision)
                    {
                        if h.logical_strategy_id != r.operation.logical_strategy_id
                            || h.from
                                != (ManagerIdentity {
                                    instance: r.member.old_instance,
                                    generation: r.member.old_generation,
                                })
                            || h.to
                                != (ManagerIdentity {
                                    instance: r.member.new_instance,
                                    generation: r.member.new_generation,
                                })
                        {
                            return Err(invalid("recovered ownership identity"));
                        }
                        let (sequence,record)=gateway.journal_records_after(0,tickwire_account_journal::MAX_RECORDS)?
                            .find(|(_,record)|matches!(record.record(),RecoveryRecord::StrategyHandover(v) if v.as_ref()==h))
                            .ok_or_else(||invalid("recovered handover missing from WAL"))?;
                        state.gate.handover_sequence = sequence;
                        state.gate.handover_digest = record.digest();
                    }
                    // The local control WAL alone cannot manufacture an applied
                    // account transition.
                    if state.gate.handover_sequence != 0 {
                        let record = gateway
                            .journal_records_after(state.gate.handover_sequence - 1, 1)?
                            .next();
                        if record.is_none_or(|(_, r)| r.digest() != state.gate.handover_digest) {
                            return Err(invalid("applied handover is absent from the account WAL"));
                        }
                    }
                }
            }
            Ok(())
        })();
        // Save even a failed Prepare: the old authority has already closed.
        // Failure after Account WAL commit remains closed and is reconciled via
        // QueryApplied; no receipt is issued from a mere command acceptance.
        if result.is_err() {
            self.halt_state(&mut state, strategy)?;
        }
        self.save_state(state.clone())?;
        result?;
        state = self.states[&key].clone();
        let current = self
            .clock
            .sample()
            .map_err(|_| invalid("lifecycle receipt clock"))?
            .monotonic;
        if current.nanos() >= r.valid_until_ns && state.phase != AppliedPhase::Halted {
            self.halt_state(&mut state, strategy)?;
            self.save_state(state.clone())?;
            state = self.states[&key].clone();
        }
        let receipt = self.sign(&state, r, current)?;
        if state.phase == AppliedPhase::Committed {
            state.committed_receipt = Some(receipt.clone());
            self.save_state(state)?;
        }
        Ok(receipt)
    }
    fn capture<S: Strategy, B: Backend, A: Authority, E: Encoder>(
        &mut self,
        strategy: &mut S,
        gateway: &AccountGateway<B, A, E>,
        r: &protocol::ControlRequest,
        p: &protocol::Readiness,
        release: &tickwire_release::closure::Verified,
    ) -> Result<()> {
        if gateway.journal_position() != p.exec_report_sequence
            || gateway.account().ledger_sequence() != p.ledger_sequence
            || gateway
                .account()
                .strategy_orders_digest(r.operation.logical_strategy_id)
                != p.orders_digest
        {
            return Err(invalid(
                "strategy readiness is behind the current account state",
            ));
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
            return Err(invalid("qualified strategy READY is not applied"));
        }
        preflight::bind(release, &r.operation, &r.member)?;
        let mut objects = strategy
            .handover_objects(&want)
            .ok_or_else(|| invalid("strategy checkpoint closure is unavailable"))?;
        objects.extend(release.objects.clone());
        if objects.is_empty() || objects.len() > 512 {
            return Err(invalid("strategy checkpoint closure bounds"));
        }
        let mut total = 0usize;
        let mut hashes = Vec::new();
        for object in &objects {
            total = total
                .checked_add(object.len())
                .ok_or_else(|| invalid("strategy checkpoint size"))?;
            if total > 128 * 1024 * 1024 {
                return Err(invalid("strategy checkpoint closure size"));
            }
            hashes.push(self.store.put_object(object)?);
        }
        if !hashes.contains(&p.checkpoint_digest) || !hashes.contains(&p.input_seal_digest) {
            return Err(invalid("strategy checkpoint closure roots"));
        }
        let seal = tickwire_recovery::HandoverSeal::decode(&self.store.read_object(
            p.input_seal_digest,
            tickwire_recovery::HandoverSeal::MAX_BYTES,
        )?)?;
        if seal.instance != r.member.new_instance
            || seal.version != 2
            || seal.preflight_receipt_digest != Some(preflight::root(release)?)
            || seal.generation != r.member.new_generation
            || seal.logical_strategy != r.operation.logical_strategy_id
            || seal.report_position != p.exec_report_sequence
            || seal.ledger_sequence != p.ledger_sequence
            || seal.orders_digest != p.orders_digest
            || seal.checkpoint_digest != p.checkpoint_digest
            || seal.roots().iter().any(|hash| !hashes.contains(hash))
        {
            return Err(invalid("strategy handover seal binding"));
        }
        seal.verify_checkpoint(
            &self
                .store
                .read_object(p.checkpoint_digest, tickwire_recovery::MAX_OBJECT_BYTES)?,
        )?;
        let mut account_seal = tickwire_account_codec::InputSeal::new(gateway.baseline().digest())
            .map_err(|_| invalid("account input seal baseline"))?;
        for (_, record) in
            gateway.journal_records_after(0, tickwire_account_journal::MAX_RECORDS)?
        {
            account_seal
                .apply(record)
                .map_err(|_| invalid("account input seal ordering"))?;
        }
        if account_seal.digest() != seal.account_input_digest
            || account_seal.position() != p.exec_report_sequence
        {
            return Err(invalid("strategy did not consume the exact account prefix"));
        }
        Ok(())
    }
    fn halt_state<S: Strategy>(&self, state: &mut State, strategy: &mut S) -> Result<()> {
        halt_operation(&self.plane, state, strategy)
    }
    fn save_state(&mut self, mut state: State) -> Result<()> {
        let key = (
            state.operation.operation_id,
            state.operation.operation_revision,
        );
        state.version = state
            .version
            .checked_add(1)
            .ok_or_else(|| invalid("lifecycle state version"))?;
        self.persist(&Event::State {
            state: state.clone(),
        })?;
        self.states.insert(key, state);
        Ok(())
    }
    fn sign(
        &self,
        state: &State,
        r: &protocol::ControlRequest,
        now: MonoTime,
    ) -> Result<SignedReceipt> {
        Ok(self.signer.sign_receipt(&protocol::AppliedReceipt {
            schema: 1,
            operation_id: r.operation.operation_id,
            operation_revision: r.operation.operation_revision,
            members_digest: r.operation.members_digest()?,
            logical_strategy_id: r.operation.logical_strategy_id,
            member: r.member.clone(),
            phase: state.phase,
            state_version: state.version,
            handover_sequence: state.gate.handover_sequence,
            handover_digest: state.gate.handover_digest,
            risk_gate_revision: state.gate.risk_gate_revision,
            control_request_id: r.request_id,
            issued_ns: now.nanos(),
            valid_until_ns: r.valid_until_ns.min(r.member.valid_until_ns),
        })?)
    }
    fn record_outcome(
        &mut self,
        r: &protocol::ControlRequest,
        hash: [u8; 32],
        receipt: Option<SignedReceipt>,
        error: Option<String>,
    ) -> Result<()> {
        self.persist(&Event::Outcome {
            session: self.session,
            request_id: r.request_id,
            request_digest: hash,
            receipt: receipt.clone(),
            error: error.clone(),
        })?;
        let outcome = Outcome {
            session: self.session,
            digest: hash,
            receipt,
            error,
        };
        self.publish_outcome(r.request_id, &outcome)?;
        self.outcomes.insert(r.request_id, outcome);
        Ok(())
    }
    fn publish_outcome(&self, id: [u8; 16], outcome: &Outcome) -> Result<()> {
        let name = id.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let (path, bytes) = if outcome.session == self.session {
            if let Some(receipt) = &outcome.receipt {
                (
                    self.config.receipts_directory.join(format!("{name}.json")),
                    serde_json::to_vec(receipt)?,
                )
            } else {
                (
                    self.config
                        .receipts_directory
                        .join(format!("{name}.rejected")),
                    serde_json::to_vec(
                        &serde_json::json!({"request_id":id,"request_digest":outcome.digest,"error":outcome.error}),
                    )?,
                )
            }
        } else {
            return Ok(());
        };
        publish(&path, &bytes)
    }
    fn persist(&mut self, event: &Event) -> Result<()> {
        if let Err(error) = self.wal.append(event) {
            self.poisoned = true;
            for state in self.states.values() {
                let _ = self
                    .plane
                    .stop_strategy(state.operation.logical_strategy_id);
            }
            return Err(error.into());
        }
        Ok(())
    }
}
fn switch(r: &protocol::ControlRequest) -> StrategySwitch {
    StrategySwitch {
        logical_strategy_id: r.operation.logical_strategy_id,
        old_instance: r.member.old_instance,
        old_generation: r.member.old_generation,
        new_instance: r.member.new_instance,
        new_generation: r.member.new_generation,
    }
}
fn policy_error(error: tickwire_runtime_policy::Error) -> crate::Error {
    invalid(&format!("lifecycle policy: {error:?}"))
}
fn private_directory(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(invalid("lifecycle directory must be absolute"));
    }
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    match builder.create(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e.into()),
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() {
        return Err(invalid("lifecycle directory type"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o077 != 0 {
            return Err(invalid("lifecycle directory must be private"));
        }
    }
    Ok(())
}
fn publish(path: &Path, bytes: &[u8]) -> Result<()> {
    if path.exists() {
        if read_file(path, protocol::MAX_SIGNED_BYTES * 5)? == bytes {
            return Ok(());
        }
        return Err(invalid("lifecycle receipt filename conflict"));
    }
    let temporary = path.with_extension("temporary");
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, path)?;
    fs::File::open(path.parent().ok_or_else(|| invalid("receipt parent"))?)?.sync_all()?;
    Ok(())
}

fn halt_operation<S: Strategy>(
    plane: &ControlPlane,
    state: &mut State,
    strategy: &mut S,
) -> Result<()> {
    // Historical operations remain queryable. They may become Halted, but
    // must not close a different operation that now owns this logical slot.
    // members_digest binds the complete member identities, risk domains and
    // execution epochs; comparing only logical ID or generation is insufficient.
    let current = plane
        .handover_gate(state.operation.logical_strategy_id)
        .map_err(policy_error)?;
    let applies = match &current {
        Some(gate) => gate.binding == state.gate.binding,
        None => plane
            .strategy_grant(state.operation.logical_strategy_id)
            .map_err(policy_error)?
            .is_some_and(|grant| {
                (grant.instance_id, grant.strategy_generation)
                    == (state.member.old_instance, state.member.old_generation)
                    || (grant.instance_id, grant.strategy_generation)
                        == (state.member.new_instance, state.member.new_generation)
            }),
    };
    if applies {
        plane
            .stop_strategy(state.operation.logical_strategy_id)
            .map_err(policy_error)?;
        strategy.handover_halt(state.operation.logical_strategy_id);
    }
    if let Some(gate) = plane
        .handover_gate(state.operation.logical_strategy_id)
        .map_err(policy_error)?
    {
        if gate.binding == state.gate.binding {
            let handover = (state.gate.handover_sequence, state.gate.handover_digest);
            state.gate = gate;
            if state.gate.handover_sequence == 0 {
                state.gate.handover_sequence = handover.0;
                state.gate.handover_digest = handover.1;
            }
        } else {
            state.gate.risk_gate_revision = state
                .gate
                .risk_gate_revision
                .checked_add(1)
                .ok_or_else(|| invalid("risk gate revision"))?;
        }
    } else {
        state.gate.risk_gate_revision = state
            .gate
            .risk_gate_revision
            .checked_add(1)
            .ok_or_else(|| invalid("risk gate revision"))?;
    }
    state.phase = AppliedPhase::Halted;
    Ok(())
}

#[cfg(test)]
mod operation_halt_tests {
    use super::*;
    use tickwire_runtime_policy::{Actions, Config as PolicyConfig, StrategyGrant, Versions};

    #[derive(Default)]
    struct Workers {
        halts: usize,
    }
    impl Strategy for Workers {
        fn on_market(&mut self, _: &tickwire_binance_public::Notice<'_>) -> bool {
            true
        }
        fn next(
            &mut self,
            _: &tickwire_account::Account,
            _: tickwire_runtime::node::Health,
            _: tickwire_binance_clock::LocalTime,
        ) -> Option<tickwire_runtime::node::Submission> {
            None
        }
        fn on_submission(
            &mut self,
            _: &tickwire_venue::CanonicalCommand,
            _: &std::result::Result<
                tickwire_account_gateway::Status,
                tickwire_binance_account::Error,
            >,
        ) {
        }
        fn handover_halt(&mut self, _: u64) -> bool {
            self.halts += 1;
            true
        }
    }
    fn now() -> MonoTime {
        MonoTime::new(ClockDomain::new([3; 16], 1).unwrap(), 20)
    }
    fn plane() -> ControlPlane {
        ControlPlane::new(PolicyConfig {
            scope: tickwire_account::Scope {
                risk_domain_id: [1; 16],
                venue: 1,
                environment: tickwire_venue::Environment::Test,
                account: 1,
            },
            product: tickwire_venue::Product::Spot,
            executor_id: [2; 16],
            owner_generation: 1,
            writer_epoch: 1,
            domain: now().domain(),
            versions: Versions {
                config_revision: 1,
                risk_policy_version: 1,
                order_config_version: 1,
                catalog_version: 1,
                catalog_hash: [4; 32],
            },
            max_strategies: 4,
            max_feeds: 2,
            marks_per_feed: 2,
        })
        .unwrap()
    }
    fn grant(generation: u64) -> StrategyGrant {
        StrategyGrant {
            logical_strategy_id: 8,
            instance_id: [generation as u8; 16],
            strategy_generation: generation,
            authorization_epoch: generation,
            config_revision: 1,
            valid_until_ns: 1000,
            actions: Actions {
                submit: false,
                cancel: true,
                query: true,
                amend: false,
            },
            required_feeds: vec![],
        }
    }
    fn state(plane: &ControlPlane, generation: u64) -> State {
        let member = protocol::Member {
            risk_domain: [1; 16],
            exec_instance: [2; 16],
            exec_epoch: 1,
            old_instance: [(generation - 1) as u8; 16],
            old_generation: generation - 1,
            new_instance: [generation as u8; 16],
            new_generation: generation,
            clock_boot_id: [3; 16],
            clock_generation: 1,
            valid_until_ns: 1000,
        };
        let operation = protocol::Operation {
            schema: 1,
            operation_id: [generation as u8; 16],
            operation_revision: 1,
            logical_strategy_id: 8,
            members: vec![member.clone()],
            artifacts_digest: [10; 32],
            checkpoint_digest: [11; 32],
            rollback_digest: [12; 32],
            clock_boot_id: [3; 16],
            clock_generation: 1,
            valid_until_ns: 1000,
        };
        let gate = plane
            .begin_handover(HandoverBinding {
                operation_id: operation.operation_id,
                operation_revision: 1,
                members_digest: operation.members_digest().unwrap(),
                logical_strategy_id: 8,
                instance_id: member.new_instance,
                strategy_generation: generation,
                valid_until_ns: 1000,
            })
            .unwrap();
        State {
            operation,
            member,
            phase: AppliedPhase::Prepared,
            version: 1,
            gate,
            proof: None,
            committed_receipt: None,
            session: 1,
        }
    }
    fn successors() -> (ControlPlane, State, State) {
        let p = plane();
        p.install_strategy_grant(grant(5)).unwrap();
        let mut old = state(&p, 6);
        old.gate = p.handover_committed(&old.gate, 10, [20; 32]).unwrap();
        p.release_handover(&old.gate, grant(6), now()).unwrap();
        old.phase = AppliedPhase::Released;
        let new = state(&p, 7);
        (p, old, new)
    }
    #[test]
    fn stale_released_history_cannot_halt_the_next_prepared_operation() {
        let (p, mut old, new) = successors();
        assert!(!p.handover_is_current(&old.gate, now()).unwrap());
        let mut workers = Workers::default();
        halt_operation(&p, &mut old, &mut workers).unwrap();
        assert!(p.handover_is_current(&new.gate, now()).unwrap());
        assert_eq!(workers.halts, 0);
        assert_eq!(old.phase, AppliedPhase::Halted);
    }
    #[test]
    fn delayed_explicit_halt_cannot_close_a_successor() {
        let (p, mut old, new) = successors();
        old.phase = AppliedPhase::Halted;
        let mut workers = Workers::default();
        halt_operation(&p, &mut old, &mut workers).unwrap();
        assert!(p.handover_is_current(&new.gate, now()).unwrap());
        assert_eq!(workers.halts, 0);
    }
    #[test]
    fn current_operation_failure_closes_its_gate_and_workers() {
        let (p, _, mut current) = successors();
        p.install_strategy_grant(grant(7)).unwrap();
        let mut workers = Workers::default();
        halt_operation(&p, &mut current, &mut workers).unwrap();
        assert!(!p.handover_is_current(&current.gate, now()).unwrap());
        assert!(p.strategy_grant(8).unwrap().is_none());
        assert_eq!(workers.halts, 1);
        assert_eq!(current.phase, AppliedPhase::Halted);
    }

    #[test]
    fn same_generation_with_a_different_complete_member_binding_cannot_halt() {
        let (p, _, mut mismatched) = successors();
        let current = mismatched.gate.clone();
        mismatched.gate.binding.members_digest = [91; 32];
        let mut workers = Workers::default();
        halt_operation(&p, &mut mismatched, &mut workers).unwrap();
        assert!(p.handover_is_current(&current, now()).unwrap());
        assert_eq!(workers.halts, 0);
    }

    #[test]
    fn first_halt_without_a_gate_only_stops_an_exact_member_grant() {
        for generation in [5, 6, 7] {
            let p = plane();
            p.install_strategy_grant(grant(generation)).unwrap();
            let mut pending = state(&plane(), 6);
            pending.phase = AppliedPhase::Halted;
            let mut workers = Workers::default();
            halt_operation(&p, &mut pending, &mut workers).unwrap();
            assert_eq!(p.strategy_grant(8).unwrap().is_none(), generation < 7);
            assert_eq!(workers.halts, usize::from(generation < 7));
        }
    }
}
