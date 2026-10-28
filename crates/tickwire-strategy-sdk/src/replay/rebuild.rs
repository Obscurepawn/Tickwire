//! Declared rebuild executes real callbacks over a qualified, bounded new input
//! window. No result from this module is a READY, live grant or rollback proof.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use tickwire_account::RecoveryRecord;
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord};
use tickwire_ipc::{RecordHeader, ResourceHeader, Wire};
use tickwire_release::{Member, Operation, ReleaseManifest, rebuild::*};
pub mod capture;
pub mod probe;
pub mod rollback;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompiledFactory {
    pub id: String,
    pub binary: Hash,
    pub state_schema: u64,
}
/// All actions in the first contract are positively tied to an accepted command.
/// No missing-record inference can turn an old action back into a new order.
#[derive(Clone)]
pub struct KnownAction {
    pub economic_intent: [u8; 16],
    pub command: CanonicalCommand,
}
fn covered_action(contract: &Contract, logical: u64, command: &CanonicalCommand) -> Result<bool> {
    let identity = &command.command().context.strategy;
    if identity.logical_strategy_id != logical {
        return Ok(false);
    }
    let required = contract
        .actions
        .iter()
        .find(|a| a.economic_intent == identity.economic_intent_id)
        .ok_or(Error::Identity)?;
    required
        .admitted_command
        .verify(command.bytes(), MAX_BYTES as u64)?;
    Ok(true)
}
pub struct VerifiedInput {
    operation: Operation,
    plans: Plans,
    manifest: ReleaseManifest,
    plan: Plan,
    work: FactoryInput,
}
/// Qualified computation input for either target rebuild or explicitly approved
/// old-version rebuild. It contains no synthetic Operation or trade authority.
pub struct FactoryInput {
    source: Source,
    end: Frontier,
    logical_strategy: u64,
    contract: Contract,
    inputs: Inputs,
    markets: Vec<(ResourceHeader, crate::MarketReplica, usize)>,
    account: account::AccountReplica,
    account_cursor: Cursor,
    actions: Vec<KnownAction>,
}
impl FactoryInput {
    pub fn source(&self) -> &Source {
        &self.source
    }
    pub fn contract(&self) -> &Contract {
        &self.contract
    }
    pub fn account(&self) -> &tickwire_account::Account {
        self.account.account()
    }
    pub fn actions(&self) -> &[KnownAction] {
        &self.actions
    }
    pub fn market(&self, stream: &StreamKey, instrument: u64) -> Option<crate::MarketView<'_>> {
        self.markets
            .iter()
            .find(|(r, _, _)| application::market_stream(r).as_ref() == Ok(stream))
            .and_then(|(_, r, _)| r.view(instrument))
    }
    fn verify(
        logical_strategy: u64,
        risk_domain: [u8; 16],
        source: Source,
        end: Frontier,
        contract: &Contract,
        inputs: &Inputs,
    ) -> Result<Self> {
        let views = QualifiedViews::verify(logical_strategy, risk_domain, contract, inputs)?;
        Ok(Self {
            source,
            end,
            logical_strategy,
            contract: contract.clone(),
            inputs: inputs.clone(),
            markets: views.markets,
            account: views.account,
            account_cursor: views.account_cursor,
            actions: views.actions,
        })
    }
}

struct QualifiedViews {
    markets: Vec<(ResourceHeader, crate::MarketReplica, usize)>,
    account: account::AccountReplica,
    account_cursor: Cursor,
    actions: Vec<KnownAction>,
}
impl QualifiedViews {
    fn verify(
        logical_strategy: u64,
        risk_domain: [u8; 16],
        contract: &Contract,
        inputs: &Inputs,
    ) -> Result<Self> {
        if inputs.accounts.len() != 1 {
            return Err(Error::Unsupported);
        }
        let prefix = &inputs.accounts[0];
        let baseline = CanonicalBaseline::decode(&prefix.baseline).map_err(|_| Error::Identity)?;
        if baseline.snapshot().scope.risk_domain_id != risk_domain {
            return Err(Error::Identity);
        }
        let stream = account::AccountReplica::stream(&baseline);
        let mut account = account::AccountReplica::from_baseline(&stream, &prefix.baseline)?;
        let mut actions = BTreeMap::new();
        let mut last = baseline.digest();
        for raw in &prefix.records {
            let record = CanonicalRecord::decode(raw).map_err(|_| Error::Identity)?;
            account.apply(&stream, record.sequence(), raw)?;
            last = record.digest();
            if let RecoveryRecord::Admit { command, .. } = record.record() {
                let identity = &command.command().context.strategy;
                if covered_action(contract, logical_strategy, command)?
                    && actions
                        .insert(identity.economic_intent_id, command.as_ref().clone())
                        .is_some()
                {
                    return Err(Error::Identity);
                }
            }
        }
        let mut known = Vec::new();
        for required in &contract.actions {
            let command = actions
                .remove(&required.economic_intent)
                .ok_or(Error::Identity)?;
            required
                .admitted_command
                .verify(command.bytes(), MAX_BYTES as u64)?;
            known.push(KnownAction {
                economic_intent: required.economic_intent,
                command,
            });
        }
        let account_cursor = Cursor {
            stream,
            sequence: account.position(),
            content_digest: last,
        };
        let mut markets = Vec::new();
        let mut seen = BTreeSet::new();
        for seed in &inputs.markets {
            let resource = ResourceHeader::decode(&seed.resource).map_err(|_| Error::Identity)?;
            let key = application::market_stream(&resource)?;
            if !seen.insert(key.clone()) || !contract.markets.iter().any(|r| r.stream == key) {
                return Err(Error::Identity);
            }
            let replica =
                crate::MarketReplica::restore_checkpoint(&seed.checkpoint, seed.maximum_levels)
                    .map_err(|_| Error::Identity)?;
            for id in replica.instruments() {
                let mark = replica.view(id).ok_or(Error::Identity)?.trigger();
                if mark.topic_id != resource.topic_id
                    || mark.feed_generation != resource.feed_generation
                    || mark.ring_epoch != resource.ring_epoch
                {
                    return Err(Error::Identity);
                }
            }
            markets.push((resource, replica, seed.maximum_levels));
        }
        if contract.markets.iter().any(|r| !seen.contains(&r.stream)) {
            return Err(Error::Identity);
        }
        Ok(Self {
            markets,
            account,
            account_cursor,
            actions: known,
        })
    }
}
impl VerifiedInput {
    pub fn contract(&self) -> &Contract {
        self.work.contract()
    }
    pub fn plan(&self) -> &Plan {
        &self.plan
    }
    pub fn account(&self) -> &tickwire_account::Account {
        self.work.account()
    }
    pub fn actions(&self) -> &[KnownAction] {
        self.work.actions()
    }
    pub fn market(&self, stream: &StreamKey, instrument: u64) -> Option<crate::MarketView<'_>> {
        self.work.market(stream, instrument)
    }
    pub fn verify(
        op: &Operation,
        member: &Member,
        plans: &Plans,
        contract: &Contract,
        inputs: &Inputs,
        manifest: &ReleaseManifest,
    ) -> Result<Self> {
        let plan = plans
            .verify(op, member, contract, inputs, manifest)?
            .clone();
        let work = FactoryInput::verify(
            op.logical_strategy_id,
            member.risk_domain,
            plan.target.clone(),
            plan.end.clone(),
            contract,
            inputs,
        )?;
        Ok(Self {
            operation: op.clone(),
            plans: plans.clone(),
            manifest: manifest.clone(),
            plan,
            work,
        })
    }
}
pub trait Factory {
    type Running: crate::Strategy;
    fn binding(&self) -> CompiledFactory;
    fn begin(&self, _input: &FactoryInput) -> Result<Self::Running> {
        Err(Error::Unsupported)
    }
    /// Decode actual strategy bytes, not metadata supplied with the completion.
    fn consumed_actions(&self, _checkpoint: &[u8]) -> Result<Vec<[u8; 16]>> {
        Err(Error::Unsupported)
    }
    fn validate_checkpoint(&self, _checkpoint: &[u8]) -> Result<()> {
        Err(Error::Unsupported)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub stream: StreamKey,
    pub instrument: u64,
    pub category: u8,
    pub count: u32,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub schema: u16,
    pub operation: Object,
    pub plans: Object,
    pub manifest: Object,
    pub contract: Object,
    pub inputs: Object,
    pub anchor: Object,
    pub checkpoint: Object,
    pub transcript: Object,
    pub account_position: u64,
    pub account_seal: Hash,
    pub observations: Vec<Observation>,
    pub consumed_actions: Vec<[u8; 16]>,
}
pub struct Candidate {
    checkpoint: Checkpoint,
    completion: Completion,
    objects: Vec<Vec<u8>>,
}
impl Candidate {
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
    pub fn completion(&self) -> &Completion {
        &self.completion
    }
    pub fn persist(self, store: &mut impl tickwire_recovery::BlobStore) -> Result<Persisted> {
        for bytes in &self.objects {
            store
                .put_sync(
                    tickwire_recovery::StoreKey::Object(tickwire_recovery::digest(bytes)),
                    bytes,
                )
                .map_err(|_| Error::Storage)?;
        }
        let bytes = canonical(&self.completion)?;
        let object = Object::of(&bytes);
        store
            .put_sync(tickwire_recovery::StoreKey::Object(object.digest), &bytes)
            .map_err(|_| Error::Storage)?;
        Ok(Persisted {
            object,
            completion: self.completion,
        })
    }
}
/// Persisted computation only. Host must independently qualify process/inputs.
pub struct Persisted {
    object: Object,
    completion: Completion,
}
impl Persisted {
    pub fn object(&self) -> Object {
        self.object
    }
    pub fn completion(&self) -> &Completion {
        &self.completion
    }
}
struct NoEffects<S>(application::Application<S>);
impl<S: crate::Strategy> ReplayableStrategy for NoEffects<S> {
    fn checkpoint(&self) -> Result<Vec<u8>> {
        self.0.checkpoint()
    }
    fn apply(
        &mut self,
        input: &Input,
        context: &mut ReplayContext<'_>,
        _effects: &mut ReplayEffects,
    ) -> Result<()> {
        self.0.apply(
            input,
            context,
            &mut ReplayEffects {
                allowed: false,
                commands: vec![],
            },
        )
    }
}
fn action_state<F: Factory>(
    factory: &F,
    application: &application::Application<F::Running>,
    required: &[[u8; 16]],
) -> Result<()> {
    let bytes = application.business_state()?;
    factory.validate_checkpoint(&bytes)?;
    if factory.consumed_actions(&bytes)? != required {
        return Err(Error::StateMismatch);
    }
    Ok(())
}
fn view_valid<S: crate::Strategy>(
    app: &application::Application<S>,
    r: &MarketRequirement,
    now: u64,
    age: u64,
) -> Result<()> {
    let view = app
        .market_view(&r.stream, r.instrument)
        .ok_or(Error::Identity)?;
    if !view.continuous() {
        return Err(Error::Sequence);
    }
    let received = if r.category == 3 {
        now.checked_sub(view.received_mono_ns())
    } else {
        view.state_age_ns(r.category, now)
    };
    if received.is_none_or(|n| n > age)
        || (r.category == 3 && !view.live())
        || r.known_bid_prices
            .iter()
            .any(|p| view.quantity(true, *p).is_none())
        || r.known_ask_prices
            .iter()
            .any(|p| view.quantity(false, *p).is_none())
    {
        return Err(Error::StateMismatch);
    }
    Ok(())
}
struct Window {
    checkpoint: Checkpoint,
    anchor: Vec<u8>,
    transcript: Vec<u8>,
    account_position: u64,
    account_seal: Hash,
    observations: Vec<Observation>,
    consumed_actions: Vec<[u8; 16]>,
}
fn execute_window<F: Factory>(factory: &F, input: FactoryInput) -> Result<Window> {
    let binding = factory.binding();
    if binding.id != input.contract.factory
        || binding.binary != input.contract.binary
        || binding.state_schema != input.contract.state_schema
    {
        return Err(Error::Identity);
    }
    let strategy = factory.begin(&input)?;
    let FactoryInput {
        source,
        end,
        logical_strategy,
        contract,
        inputs,
        markets,
        account,
        account_cursor,
        actions,
    } = input;
    let required = actions
        .iter()
        .map(|a| a.economic_intent)
        .collect::<Vec<_>>();
    let account_stream = account_cursor.stream.clone();
    let mut app = application::Application::new(strategy, contract.maximum_age_ns, markets)?;
    app.seed_rebuild_account(account)?;
    action_state(factory, &app, &required)?;
    let start = Checkpoint::new(
        source.clone(),
        0,
        [0; 32],
        app.checkpoint()?,
        RuntimeState {
            now_ns: inputs.start_ns,
            config: inputs.config.clone(),
            models: inputs.models.clone(),
            markets: app.market_cursors()?,
            accounts: vec![account_cursor],
            external: vec![],
            timers: vec![],
            rng: contract.rng.clone(),
            effects: EffectCut {
                count: 0,
                digest: [0; 32],
            },
        },
    )?;
    let start_bytes = start.bytes()?;
    let models = inputs
        .models
        .iter()
        .map(|m| (m.id, Object::of(&m.value.bytes)))
        .collect::<Vec<_>>();
    let anchor = verify_anchor(&start_bytes, &source, contract.config, &models)?;
    let mut engine = Engine::new(&anchor, NoEffects(app))?;
    let mut latest = BTreeMap::new();
    let mut observed = BTreeSet::new();
    let mut counts = vec![0u32; contract.markets.len()];
    for row in &inputs.warm {
        if let Input::ConfigApply { .. } | Input::ModelApply { .. } = &row.input {
            return Err(Error::Unsupported);
        }
        if let Input::AccountCallback {
            position,
            canonical,
            ..
        } = &row.input
            && *position != 0
        {
            let record = CanonicalRecord::decode(canonical).map_err(|_| Error::Identity)?;
            if let RecoveryRecord::Admit { command, .. } = record.record() {
                covered_action(&contract, logical_strategy, command)?;
            }
        }
        let applied = engine.apply_live(row.input.clone(), row.now_ns)?;
        if !applied.commands.is_empty() {
            return Err(Error::EffectsMismatch);
        }
        match &row.input {
            Input::MarketApplied { stream, record, .. } => {
                let h = RecordHeader::decode(record.get(..128).ok_or(Error::Identity)?)
                    .map_err(|_| Error::Identity)?;
                let category = if h.message_type == 259 {
                    Some(3)
                } else {
                    tickwire_ipc::market_state_category(h.message_type)
                        .ok()
                        .filter(|c| *c <= 4)
                };
                if let Some(category) = category {
                    latest.insert(
                        (stream.clone(), h.instrument_id, category),
                        (h.sequence, h.ingress_mono_ns),
                    );
                }
            }
            Input::MarketCallback {
                stream,
                instrument,
                invoked: true,
                ..
            } => {
                for (i, r) in contract
                    .markets
                    .iter()
                    .enumerate()
                    .filter(|(_, r)| &r.stream == stream && r.instrument == *instrument)
                {
                    view_valid(&engine.strategy.0, r, row.now_ns, contract.maximum_age_ns)?;
                    if let Some((sequence, received)) =
                        latest.get(&(stream.clone(), *instrument, r.category))
                    {
                        if row
                            .now_ns
                            .checked_sub(*received)
                            .is_none_or(|age| age > contract.maximum_age_ns)
                        {
                            return Err(Error::Time);
                        }
                        if observed.insert((stream.clone(), *instrument, r.category, *sequence)) {
                            counts[i] = counts[i].checked_add(1).ok_or(Error::Capacity)?;
                        }
                    }
                }
            }
            _ => {}
        }
        action_state(factory, &engine.strategy.0, &required)?;
    }
    let checkpoint = engine.checkpoint()?;
    let runtime = &checkpoint.runtime;
    if checkpoint.cut.sequence != end.rows
        || runtime.now_ns != end.now_ns
        || runtime.markets != end.markets
        || runtime.accounts != end.accounts
        || runtime.external != end.external
        || runtime.config != inputs.config
        || runtime.models != inputs.models
        || runtime.effects.count != 0
    {
        return Err(Error::StateMismatch);
    }
    for (r, count) in contract.markets.iter().zip(&counts) {
        view_valid(
            &engine.strategy.0,
            r,
            runtime.now_ns,
            contract.maximum_age_ns,
        )?;
        if *count < r.minimum_observations {
            return Err(Error::StateMismatch);
        }
    }
    let (account_position, account_seal) = engine.strategy.0.account_seal(&account_stream)?;
    let tail = encode_tail(&anchor, engine.recorded())?;
    verify_tail(&anchor, &tail, &checkpoint.cut)?;
    Ok(Window {
        checkpoint,
        anchor: start_bytes,
        transcript: tail,
        account_position,
        account_seal,
        observations: contract
            .markets
            .iter()
            .zip(counts)
            .map(|(r, count)| Observation {
                stream: r.stream.clone(),
                instrument: r.instrument,
                count,
                category: r.category,
            })
            .collect(),
        consumed_actions: required,
    })
}
pub fn execute<F: Factory>(factory: &F, input: VerifiedInput) -> Result<Candidate> {
    let VerifiedInput {
        operation,
        plans,
        manifest,
        plan: _,
        work,
    } = input;
    let contract = work.contract.clone();
    let inputs = work.inputs.clone();
    let window = execute_window(factory, work)?;
    let mut objects = vec![
        canonical(&operation)?,
        plans.bytes()?,
        canonical(&manifest)?,
        contract.bytes()?,
        inputs.bytes()?,
        window.anchor,
        window.checkpoint.bytes()?,
        window.transcript,
    ];
    let completion = Completion {
        schema: 1,
        operation: Object::of(&objects[0]),
        plans: Object::of(&objects[1]),
        manifest: Object::of(&objects[2]),
        contract: Object::of(&objects[3]),
        inputs: Object::of(&objects[4]),
        anchor: Object::of(&objects[5]),
        checkpoint: Object::of(&objects[6]),
        transcript: Object::of(&objects[7]),
        account_position: window.account_position,
        account_seal: window.account_seal,
        observations: window.observations,
        consumed_actions: window.consumed_actions,
    };
    // Stable order and de-duplication are bounded by this fixed eight-object closure.
    let mut seen = BTreeSet::new();
    objects.retain(|b| seen.insert(tickwire_recovery::digest(b)));
    Ok(Candidate {
        checkpoint: window.checkpoint,
        completion,
        objects,
    })
}
#[cfg(test)]
mod tests;

#[cfg(test)]
mod rollback_tests;
