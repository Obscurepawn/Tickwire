use std::{cell::RefCell, collections::BTreeMap, rc::Rc};
use tickwire_account::{Admission, RecoveryRecord};
use tickwire_account_gateway::{AdmissionRequest, Authority, AuthorityError, Gate};
use tickwire_recovery::{
    BlobStore, DependencyObject, EncodedInputs, Persistence, RecoveryManifest, RecoveryMode,
    VerifiedClosure,
};
use tickwire_venue::{CanonicalCommand, Hash};

/// A trusted control-policy provider. The runtime does not supply an allow-all
/// default; local or remote control implementations must inspect current state.
pub trait GateProvider {
    fn current(
        &mut self,
        command: &CanonicalCommand,
        admission: &Admission,
    ) -> Result<Gate, AuthorityError>;
}
pub trait CommitPolicy {
    fn state(
        &mut self,
        admission: &Admission,
        receipt: &tickwire_account_journal::AppendReceipt,
    ) -> Result<tickwire_account_gateway::CommitState, AuthorityError>;
}
impl<F> GateProvider for F
where
    F: FnMut(&CanonicalCommand, &Admission) -> Result<Gate, AuthorityError>,
{
    fn current(&mut self, c: &CanonicalCommand, a: &Admission) -> Result<Gate, AuthorityError> {
        self(c, a)
    }
}
#[derive(Clone)]
pub struct Dependency {
    pub kind: u8,
    pub schema_id: u64,
    pub owner: [u8; 16],
    pub bytes: Vec<u8>,
}
pub struct RecoveryBundle {
    pub command_digest: Hash,
    /// Required control/configuration/checkpoint dependencies, kinds 2..=11.
    pub dependencies: Vec<Dependency>,
    /// Actual sequenced strategy inputs, not a synthetic quote or marker.
    pub inputs: EncodedInputs,
    pub mode: RecoveryMode,
    pub fault_model_id: Hash,
    pub pin_id: [u8; 16],
    pub owner: [u8; 16],
}
struct Staged {
    bundles: BTreeMap<Hash, RecoveryBundle>,
    capacity: usize,
    retained: usize,
    maximum_bytes: usize,
}
#[derive(Clone)]
pub struct RecoveryQueue(Rc<RefCell<Staged>>);
impl RecoveryQueue {
    pub fn new(capacity: usize, maximum_bytes: usize) -> Result<Self, AuthorityError> {
        if capacity == 0
            || capacity > 1024
            || maximum_bytes == 0
            || maximum_bytes > tickwire_recovery::MAX_CLOSURE_BYTES as usize
        {
            return Err(AuthorityError::Denied);
        }
        Ok(Self(Rc::new(RefCell::new(Staged {
            bundles: BTreeMap::new(),
            capacity,
            retained: 0,
            maximum_bytes,
        }))))
    }
    /// Called before submit; exact command identity selects the staged closure.
    pub fn stage(&self, bundle: RecoveryBundle) -> Result<(), AuthorityError> {
        if bundle.command_digest == [0; 32]
            || bundle.pin_id == [0; 16]
            || bundle.owner == [0; 16]
            || bundle.fault_model_id == [0; 32]
            || bundle.dependencies.len() != 10
        {
            return Err(AuthorityError::Denied);
        }
        let mut total = bundle.inputs.bytes.len();
        for (i, d) in bundle.dependencies.iter().enumerate() {
            if d.kind != i as u8 + 2 || d.schema_id == 0 || d.owner == [0; 16] || d.bytes.is_empty()
            {
                return Err(AuthorityError::Denied);
            }
            total = total
                .checked_add(d.bytes.len())
                .ok_or(AuthorityError::Denied)?;
        }
        let mut q = self
            .0
            .try_borrow_mut()
            .map_err(|_| AuthorityError::Unavailable)?;
        if q.bundles.contains_key(&bundle.command_digest)
            || q.bundles.len() >= q.capacity
            || q.retained
                .checked_add(total)
                .is_none_or(|n| n > q.maximum_bytes)
        {
            return Err(AuthorityError::Denied);
        }
        q.retained += total;
        q.bundles.insert(bundle.command_digest, bundle);
        Ok(())
    }
    /// Safe before Admit or after the caller has retained an immutable binding.
    pub fn remove(&self, digest: Hash) -> Result<(), AuthorityError> {
        let mut q = self
            .0
            .try_borrow_mut()
            .map_err(|_| AuthorityError::Unavailable)?;
        if let Some(b) = q.bundles.remove(&digest) {
            q.retained -=
                b.inputs.bytes.len() + b.dependencies.iter().map(|d| d.bytes.len()).sum::<usize>();
        }
        Ok(())
    }
}
pub struct LocalAuthority<S: BlobStore, P: GateProvider> {
    persistence: Persistence<S>,
    staged: RecoveryQueue,
    policy: P,
    commit: Option<Box<dyn CommitPolicy>>,
    executor_id: [u8; 16],
}
impl<S: BlobStore, P: GateProvider> LocalAuthority<S, P> {
    pub fn new(store: S, staged: RecoveryQueue, policy: P, executor_id: [u8; 16]) -> Self {
        Self {
            persistence: Persistence::new(store),
            staged,
            policy,
            commit: None,
            executor_id,
        }
    }
    pub fn with_commit_policy(mut self, policy: Box<dyn CommitPolicy>) -> Self {
        self.commit = Some(policy);
        self
    }
}
impl<S: BlobStore, P: GateProvider> Authority for LocalAuthority<S, P> {
    fn verify_handover(
        &mut self,
        baseline: &tickwire_account_codec::CanonicalBaseline,
        record: &tickwire_account_codec::CanonicalRecord,
        prior: &[tickwire_account_codec::CanonicalRecord],
    ) -> Result<(), AuthorityError> {
        verify_handover_objects(&self.persistence, baseline, record, prior)
    }
    fn commit_state(
        &mut self,
        admission: &Admission,
        receipt: &tickwire_account_journal::AppendReceipt,
    ) -> Result<tickwire_account_gateway::CommitState, AuthorityError> {
        self.commit
            .as_mut()
            .map_or(Ok(tickwire_account_gateway::CommitState::Ready), |policy| {
                policy.state(admission, receipt)
            })
    }
    fn prepare(&mut self, r: AdmissionRequest<'_>) -> Result<VerifiedClosure, AuthorityError> {
        if self.executor_id == [0; 16] {
            return Err(AuthorityError::Denied);
        }
        let RecoveryRecord::Admit { command, .. } = r.record.record() else {
            return Err(AuthorityError::Denied);
        };
        let q = self
            .staged
            .0
            .try_borrow()
            .map_err(|_| AuthorityError::Unavailable)?;
        let b = q
            .bundles
            .get(&command.digest())
            .ok_or(AuthorityError::Denied)?;
        let mut objects = Vec::with_capacity(15);
        let mut add =
            |kind: u8, schema: u64, owner: [u8; 16], bytes: &[u8]| -> Result<(), AuthorityError> {
                let content_hash = self
                    .persistence
                    .put_object(bytes)
                    .map_err(AuthorityError::Recovery)?;
                objects.push(DependencyObject {
                    kind,
                    content_hash,
                    byte_length: bytes.len() as u64,
                    schema_id: schema,
                    owner_identity: owner,
                });
                Ok(())
            };
        add(1, 1, self.executor_id, r.record.bytes())?;
        for d in &b.dependencies {
            // A strategy can name its inputs and configuration, but only the
            // account owner knows the accepted reservation and deduplication
            // prefix. Their recovery representation is the actual Admit plus
            // the exact preceding account history, never worker placeholders.
            match d.kind {
                2 => add(2, 1, self.executor_id, r.record.bytes())?,
                3 => add(3, 1, self.executor_id, r.preceding.bytes())?,
                _ => add(d.kind, d.schema_id, d.owner, &d.bytes)?,
            }
        }
        add(12, 1, b.owner, &b.inputs.bytes)?;
        add(13, 1, b.owner, command.bytes())?;
        add(14, 1, self.executor_id, r.baseline.bytes())?;
        add(15, 1, self.executor_id, r.preceding.bytes())?;
        let manifest = RecoveryManifest {
            binding: r.binding,
            logical_strategy_id: command.command().context.strategy.logical_strategy_id,
            instance_id: command.command().context.strategy.instance_id,
            action_grant_ref: objects[3].content_hash,
            operation_ref: objects[6].content_hash,
            mode: b.mode,
            fault_model_id: b.fault_model_id,
            pin_id: b.pin_id,
            input_boundary: b.inputs.boundary,
            edges: (1..15).map(|n| (0, n)).collect(),
            objects,
        };
        self.persistence
            .publish(&manifest)
            .map_err(AuthorityError::Recovery)
    }
    fn verify(&mut self, r: AdmissionRequest<'_>) -> Result<VerifiedClosure, AuthorityError> {
        self.persistence
            .verify(&r.binding)
            .map_err(AuthorityError::Recovery)
    }
    fn current_gate(
        &mut self,
        c: &CanonicalCommand,
        a: &Admission,
    ) -> Result<Gate, AuthorityError> {
        self.policy.current(c, a)
    }
}

/// Validate historical strategy ownership against the retained strategy closure
/// and the exact canonical account prefix. It grants no live trading authority.
pub fn verify_handover_objects<S: BlobStore>(
    persistence: &Persistence<S>,
    baseline: &tickwire_account_codec::CanonicalBaseline,
    record: &tickwire_account_codec::CanonicalRecord,
    prior: &[tickwire_account_codec::CanonicalRecord],
) -> Result<(), AuthorityError> {
    let RecoveryRecord::StrategyHandover(h) = record.record() else {
        return Err(AuthorityError::Denied);
    };
    let invalid = || AuthorityError::Denied;
    let seal = tickwire_recovery::HandoverSeal::decode(
        &persistence
            .read_object(
                h.input_seal_digest,
                tickwire_recovery::HandoverSeal::MAX_BYTES,
            )
            .map_err(AuthorityError::Recovery)?,
    )
    .map_err(AuthorityError::Recovery)?;
    if record.baseline() != baseline.digest()
        || h.scope != baseline.snapshot().scope
        || seal.instance != h.to.instance
        || seal.generation != h.to.generation
        || seal.logical_strategy != h.logical_strategy_id
        || seal.ledger_sequence != h.expected_ledger_sequence
        || seal.orders_digest != h.expected_orders_digest
        || seal.checkpoint_digest != h.checkpoint_digest
        || seal.report_position.checked_add(1) != Some(record.sequence())
        || seal.report_position != prior.len() as u64
    {
        return Err(invalid());
    }
    let checkpoint = persistence
        .read_object(seal.checkpoint_digest, tickwire_recovery::MAX_OBJECT_BYTES)
        .map_err(AuthorityError::Recovery)?;
    let inputs = persistence
        .read_object(seal.input_digest, tickwire_recovery::MAX_OBJECT_BYTES)
        .map_err(AuthorityError::Recovery)?;
    let account_object = persistence
        .read_object(
            seal.account_input_digest,
            tickwire_recovery::MAX_OBJECT_BYTES,
        )
        .map_err(AuthorityError::Recovery)?;
    let verified = tickwire_handover::verify(&seal, &checkpoint, &inputs, &account_object)
        .map_err(|_| invalid())?;
    verified
        .matches_history(baseline, prior)
        .map_err(|_| invalid())?;
    let mut sizes = std::collections::BTreeMap::from([
        (seal.checkpoint_digest, checkpoint.len()),
        (seal.account_input_digest, account_object.len()),
        (seal.input_digest, inputs.len()),
        (
            h.input_seal_digest,
            seal.bytes().map_err(|_| invalid())?.len(),
        ),
    ]);
    if let Some(root) = seal.preflight_receipt_digest {
        let release = tickwire_release::closure::verify(root, |hash| {
            persistence
                .read_object(hash, tickwire_recovery::MAX_OBJECT_BYTES)
                .map_err(|_| tickwire_release::Error::Missing)
        })
        .map_err(|_| invalid())?;
        let c = &release.completion;
        if c.operation.schema != 2
            || c.operation.operation_id != h.operation_id
            || c.operation.operation_revision != h.operation_revision
            || c.logical_strategy != h.logical_strategy_id
            || c.member.risk_domain != h.scope.risk_domain_id
            || c.member.old_instance != h.from.instance
            || c.member.old_generation != h.from.generation
            || c.member.new_instance != h.to.instance
            || c.member.new_generation != h.to.generation
        {
            return Err(invalid());
        }
        for bytes in &release.objects {
            sizes.insert(tickwire_recovery::digest(bytes), bytes.len());
        }
    }
    if seal.version == 3 {
        let root = seal.replay_rollback_digest.ok_or_else(invalid)?;
        let objects =
            tickwire_release::replay::startup::verify_seal(&seal, &checkpoint, &inputs, |hash| {
                persistence
                    .read_object(hash, tickwire_recovery::MAX_OBJECT_BYTES)
                    .map_err(|_| tickwire_release::Error::Missing)
            })
            .map_err(|_| invalid())?;
        for bytes in objects {
            sizes.insert(tickwire_recovery::digest(&bytes), bytes.len());
        }
        let completed = tickwire_release::replay::rollback::verify(root, |hash| {
            persistence
                .read_object(hash, tickwire_recovery::MAX_OBJECT_BYTES)
                .map_err(|_| tickwire_release::Error::Missing)
        })
        .map_err(|_| invalid())?;
        let installed = &completed.installation;
        installed
            .bind_seal(&seal, &checkpoint, &inputs)
            .map_err(|_| invalid())?;
        let op = &installed.request.operation;
        let member = &installed.request.member;
        if op.operation_id != h.operation_id
            || op.operation_revision != h.operation_revision
            || op.logical_strategy_id != h.logical_strategy_id
            || member.risk_domain != h.scope.risk_domain_id
            || member.old_instance != h.from.instance
            || member.old_generation != h.from.generation
            || member.new_instance != h.to.instance
            || member.new_generation != h.to.generation
        {
            return Err(invalid());
        }
        for bytes in completed.objects {
            sizes.insert(tickwire_recovery::digest(&bytes), bytes.len());
        }
    }
    if seal.version == 4 {
        let root = seal.rebuild_installation_digest.ok_or_else(invalid)?;
        if seal.rebuild_rollback_digest.is_none() {
            return Err(invalid());
        }
        let objects =
            tickwire_release::rebuild::install::verify_seal(&seal, &checkpoint, &inputs, |hash| {
                persistence
                    .read_object(hash, tickwire_recovery::MAX_OBJECT_BYTES)
                    .map_err(|_| tickwire_release::Error::Missing)
            })
            .map_err(|_| invalid())?;
        for bytes in objects {
            sizes.insert(tickwire_recovery::digest(&bytes), bytes.len());
        }
        let bytes = persistence
            .read_object(root, tickwire_release::replay::MAX_BYTES)
            .map_err(|_| invalid())?;
        let installed = tickwire_release::rebuild::install::verify(
            tickwire_release::Object::of(&bytes),
            |hash| {
                persistence
                    .read_object(hash, tickwire_recovery::MAX_OBJECT_BYTES)
                    .map_err(|_| tickwire_release::Error::Missing)
            },
        )
        .map_err(|_| invalid())?;
        let request = installed.request();
        let op = &request.operation;
        let member = &request.member;
        if op.schema != 4
            || op.operation_id != h.operation_id
            || op.operation_revision != h.operation_revision
            || op.logical_strategy_id != h.logical_strategy_id
            || member.risk_domain != h.scope.risk_domain_id
            || member.old_instance != h.from.instance
            || member.old_generation != h.from.generation
            || member.new_instance != h.to.instance
            || member.new_generation != h.to.generation
        {
            return Err(invalid());
        }
    }
    for (_, hash) in &seal.dependencies {
        let bytes = persistence
            .read_object(*hash, tickwire_recovery::MAX_OBJECT_BYTES)
            .map_err(AuthorityError::Recovery)?;
        sizes.insert(*hash, bytes.len());
    }
    let total = sizes
        .values()
        .try_fold(0usize, |total, n| total.checked_add(*n))
        .ok_or_else(invalid)?;
    if sizes.len() > 512 || total > tickwire_recovery::MAX_CLOSURE_BYTES as usize {
        return Err(invalid());
    }
    Ok(())
}
