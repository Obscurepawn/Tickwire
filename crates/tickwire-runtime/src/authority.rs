mod historical;
pub use historical::verify_handover_objects;

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
    executor_id: [u8; 16],
}
impl<S: BlobStore, P: GateProvider> LocalAuthority<S, P> {
    pub fn new(store: S, staged: RecoveryQueue, policy: P, executor_id: [u8; 16]) -> Self {
        Self {
            persistence: Persistence::new(store),
            staged,
            policy,
            executor_id,
        }
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
        let mut payloads = Vec::with_capacity(15);
        let mut add = |kind, schema_id, owner_identity, bytes| {
            let bytes: &[u8] = bytes;
            objects.push(DependencyObject {
                kind,
                content_hash: tickwire_recovery::digest(bytes),
                byte_length: bytes.len() as u64,
                schema_id,
                owner_identity,
            });
            payloads.push(bytes);
        };
        add(1, 1, self.executor_id, r.record.bytes());
        for d in &b.dependencies {
            // A strategy can name its inputs and configuration, but only the
            // account owner knows the accepted reservation and deduplication
            // prefix. Their recovery representation is the actual Admit plus
            // the exact preceding account history, never worker placeholders.
            match d.kind {
                2 => add(2, 1, self.executor_id, r.record.bytes()),
                3 => add(3, 1, self.executor_id, r.preceding.bytes()),
                _ => add(d.kind, d.schema_id, d.owner, &d.bytes),
            }
        }
        add(12, 1, b.owner, &b.inputs.bytes);
        add(13, 1, b.owner, command.bytes());
        add(14, 1, self.executor_id, r.baseline.bytes());
        add(15, 1, self.executor_id, r.preceding.bytes());
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
            .publish_with_objects(&manifest, &payloads)
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
