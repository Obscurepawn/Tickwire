//! Executes the fixed old factory without any live transport or output writer.
//! A returned computation still requires an authenticated, exited probe child.
use super::*;
use tickwire_release::rebuild::rollback::Verified;

pub trait ExactFactory: SourceReplayFactory {
    fn factory_id(&self) -> &str;
    fn consumed_actions(&self, _business: &[u8]) -> Result<Vec<[u8; 16]>> {
        Err(Error::Unsupported)
    }
}
pub struct ExactCandidate {
    rollback: Object,
    checkpoint: Checkpoint,
    anchor: Vec<u8>,
    transcript: Vec<u8>,
}
impl ExactCandidate {
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
}
pub fn prepare_exact<F: ExactFactory>(factory: &F, input: &Verified) -> Result<ExactCandidate> {
    let exact = input.exact().ok_or(Error::Unsupported)?;
    if factory.factory_id() != input.plan().factory
        || factory.binary_digest() != exact.checkpoint().source.binary_digest
        || factory.state_schema() != input.plan().capabilities.state_schema
    {
        return Err(Error::Identity);
    }
    let current = replay_source(factory, exact.anchor(), exact.tail())?;
    if current.checkpoint() != exact.checkpoint() {
        return Err(Error::StateMismatch);
    }
    let checkpoint = current.checkpoint();
    application::verify_rebuild_views(&checkpoint.business, input.forward_inputs())?;
    let contract = input.forward_contract();
    let mut admitted = BTreeSet::new();
    for raw in input
        .forward_inputs()
        .accounts
        .iter()
        .flat_map(|p| &p.records)
        .chain(
            input
                .forward_inputs()
                .warm
                .iter()
                .filter_map(|row| match &row.input {
                    Input::AccountCallback {
                        position,
                        canonical,
                        ..
                    } if *position > 0 => Some(canonical),
                    _ => None,
                }),
        )
    {
        let record = CanonicalRecord::decode(raw).map_err(|_| Error::Identity)?;
        if let RecoveryRecord::Admit { command, .. } = record.record()
            && covered_action(contract, checkpoint.source.logical_strategy, command)?
            && !admitted.insert(command.command().context.strategy.economic_intent_id)
        {
            return Err(Error::Identity);
        }
    }
    let required = contract
        .actions
        .iter()
        .map(|a| a.economic_intent)
        .collect::<Vec<_>>();
    if admitted.into_iter().collect::<Vec<_>>() != required {
        return Err(Error::Identity);
    }
    let business = application::business_bytes(&checkpoint.business)?;
    if factory.consumed_actions(&business)? != required {
        return Err(Error::StateMismatch);
    }
    // Replay completion alone is not a restore probe at C. Invoke the fixed
    // old factory again on C, compare its complete checkpoint and action state.
    let models = checkpoint
        .runtime
        .models
        .iter()
        .map(|m| (m.id, Object::of(&m.value.bytes)))
        .collect::<Vec<_>>();
    let at_c = verify_anchor(
        &checkpoint.bytes()?,
        &checkpoint.source,
        Object::of(&checkpoint.runtime.config.bytes),
        &models,
    )?;
    let restored = factory.restore_source(&at_c)?;
    let restored_bytes = restored.checkpoint()?;
    if restored_bytes != checkpoint.business
        || factory.consumed_actions(&application::business_bytes(&restored_bytes)?)? != required
    {
        return Err(Error::StateMismatch);
    }
    Ok(ExactCandidate {
        rollback: input.root(),
        checkpoint: checkpoint.clone(),
        anchor: exact.anchor().checkpoint().bytes()?,
        transcript: encode_tail(exact.anchor(), exact.tail().rows())?,
    })
}

/// Actual old-version rebuild computation. Host must authenticate its fresh
/// isolated process and successful exit before recording a rollback probe.
pub struct DeclaredCandidate {
    rollback: Object,
    checkpoint: Checkpoint,
    anchor: Vec<u8>,
    transcript: Vec<u8>,
    observations: Vec<Observation>,
    account_position: u64,
    account_seal: Hash,
    consumed_actions: Vec<[u8; 16]>,
}
impl DeclaredCandidate {
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
    pub fn observations(&self) -> &[Observation] {
        &self.observations
    }
    pub fn account_position(&self) -> u64 {
        self.account_position
    }
    pub fn account_seal(&self) -> Hash {
        self.account_seal
    }
    pub fn consumed_actions(&self) -> &[[u8; 16]] {
        &self.consumed_actions
    }
}
pub fn prepare_declared<F: Factory>(factory: &F, input: &Verified) -> Result<DeclaredCandidate> {
    let declared = input.declared().ok_or(Error::Unsupported)?;
    let plan = input.plan();
    let contract = declared.contract();
    let source = Source {
        logical_strategy: input.logical_strategy(),
        instance: plan.probe.instance,
        generation: plan.probe.generation,
        binary_digest: contract.binary,
        state_schema: contract.state_schema,
        input_stream: plan.probe.input_stream,
        clock: declared.inputs().clock.clone(),
    };
    // Use the approved old contract directly. No target operation, grant or
    // synthetic old checkpoint is constructed to bypass forward validation.
    let work = FactoryInput::verify(
        input.logical_strategy(),
        plan.member.risk_domain,
        source,
        declared.end().clone(),
        contract,
        declared.inputs(),
    )?;
    let window = execute_window(factory, work)?;
    Ok(DeclaredCandidate {
        rollback: input.root(),
        checkpoint: window.checkpoint,
        anchor: window.anchor,
        transcript: window.transcript,
        observations: window.observations,
        account_position: window.account_position,
        account_seal: window.account_seal,
        consumed_actions: window.consumed_actions,
    })
}

/// Only the two actual callback executors can construct this value. It is not
/// a process receipt or authorization to attach an input/output lane.
pub struct Computation {
    pub(super) rollback: Object,
    pub(super) checkpoint: Checkpoint,
    pub(super) anchor: Vec<u8>,
    pub(super) transcript: Vec<u8>,
    pub(super) exact: bool,
}
impl From<ExactCandidate> for Computation {
    fn from(v: ExactCandidate) -> Self {
        Self {
            rollback: v.rollback,
            checkpoint: v.checkpoint,
            anchor: v.anchor,
            transcript: v.transcript,
            exact: true,
        }
    }
}
impl From<DeclaredCandidate> for Computation {
    fn from(v: DeclaredCandidate) -> Self {
        Self {
            rollback: v.rollback,
            checkpoint: v.checkpoint,
            anchor: v.anchor,
            transcript: v.transcript,
            exact: false,
        }
    }
}
