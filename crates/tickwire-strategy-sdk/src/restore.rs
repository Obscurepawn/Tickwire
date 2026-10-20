//! State restoration executes qualified strategy code. Successful restoration is
//! not a live grant or READY: the worker must still catch up and seal its output.
use crate::{FixedDemo, ObserveOnly, Strategy};
use tickwire_release::{
    Capabilities, Hash, MemberRelease, Method, OrderKind, PositionMode, Product,
};
use tickwire_venue::CanonicalCommand;
pub const OBSERVE_SCHEMA: u64 = 0x54574f420001;
pub const DEMO_V1_SCHEMA: u64 = 0x545746440001;
pub const DEMO_SCHEMA: u64 = 0x545746440002;
pub const DEMO_MIGRATION: u64 = 1;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Unsupported,
    Identity,
    State,
    Input,
    Bounds,
    Callback,
}
pub type Result<T> = core::result::Result<T, Error>;
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

/// Fresh target authority comes from the immutable target configuration. None
/// of these fields is loaded from a source strategy's saved command.
#[derive(Clone)]
pub struct Target {
    pub instance: [u8; 16],
    pub generation: u64,
    pub logical_strategy: u64,
    pub command: Option<CanonicalCommand>,
    pub minimum_bid_ticks: i64,
    pub enabled: bool,
}
/// Validated checkpoint and complete source callbacks. Host separately proves
/// the source really belongs to its registered old process and fixed release.
pub struct VerifiedRestoreInput {
    target: Target,
    method: Method,
    expected: Capabilities,
    state: Vec<u8>,
    source_schema: u64,
    source_checkpoint: Hash,
    source_instance: [u8; 16],
    source_generation: u64,
    source_report_position: u64,
    callbacks: Option<tickwire_handover::VerifiedHandoverInputs>,
}
impl VerifiedRestoreInput {
    pub fn verify(
        release: &MemberRelease<'_>,
        target: Target,
        seal_bytes: &[u8],
        checkpoint: &[u8],
        inputs: &[u8],
        account_input: &[u8],
    ) -> Result<Self> {
        let migration = release.migration;
        let member = &migration.member;
        validate_target(&target)?;
        if target.logical_strategy != release.logical_strategy()
            || target.instance != member.new_instance
            || target.generation != member.new_generation
            || release.target.member != *member
            || release.rollback.member != *member
        {
            return Err(Error::Identity);
        }
        migration
            .source_proof
            .verify(
                seal_bytes,
                tickwire_recovery::HandoverSeal::MAX_BYTES as u64,
            )
            .map_err(|_| Error::Input)?;
        migration
            .source_checkpoint
            .verify(checkpoint, tickwire_recovery::MAX_OBJECT_BYTES as u64)
            .map_err(|_| Error::Input)?;
        let seal = tickwire_recovery::HandoverSeal::decode(seal_bytes).map_err(|_| Error::Input)?;
        if seal.instance != member.old_instance
            || seal.generation != member.old_generation
            || seal.logical_strategy != target.logical_strategy
        {
            return Err(Error::Identity);
        }
        let callbacks = tickwire_handover::verify(&seal, checkpoint, inputs, account_input)
            .map_err(|_| Error::Input)?;
        if callbacks.baseline().snapshot().scope.risk_domain_id != member.risk_domain {
            return Err(Error::Identity);
        }
        if let Some(prefix) = &migration.inputs {
            prefix.verify(inputs.to_vec()).map_err(|_| Error::Input)?;
        } else if !matches!(migration.method, Method::Rebuild { .. }) {
            return Err(Error::Input);
        }
        migration
            .method
            .check_source(migration.source_schema)
            .map_err(|_| Error::Unsupported)?;
        if !release
            .target
            .capabilities
            .methods
            .contains(&migration.method)
            || migration.method.target_schema() != release.target.capabilities.state_schema
        {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            target,
            method: migration.method.clone(),
            expected: release.target.capabilities.clone(),
            state: checkpoint[96..].to_vec(),
            source_schema: migration.source_schema,
            source_checkpoint: seal.checkpoint_digest,
            source_instance: seal.instance,
            source_generation: seal.generation,
            source_report_position: seal.report_position,
            callbacks: Some(callbacks),
        })
    }
    pub fn target(&self) -> &Target {
        &self.target
    }
    /// Rollback consumes the real target callback output, never the original
    /// checkpoint again. The source callback history remains fully validated.
    pub fn for_probe(
        mut self,
        release: &MemberRelease<'_>,
        target: Target,
        parent: &tickwire_release::receipt::Receipt,
        output: &[u8],
    ) -> Result<Self> {
        validate_target(&target)?;
        parent.verify(release, None).map_err(|_| Error::Identity)?;
        parent
            .output
            .verify(output, tickwire_recovery::MAX_OBJECT_BYTES as u64)
            .map_err(|_| Error::Input)?;
        if target.logical_strategy != release.logical_strategy()
            || target.instance == release.target.member.old_instance
            || target.instance == release.target.member.new_instance
            || parent.source_checkpoint != self.source_checkpoint
            || parent.source_report_position != self.source_report_position
        {
            return Err(Error::Identity);
        }
        release
            .rollback
            .method
            .check_source(parent.output_schema)
            .map_err(|_| Error::Unsupported)?;
        self.target = target;
        self.method = release.rollback.method.clone();
        self.expected = release.rollback.capabilities.clone();
        self.state = output.to_vec();
        self.source_schema = parent.output_schema;
        self.source_checkpoint = parent.output.digest;
        self.source_instance = parent.process.instance;
        self.source_generation = parent.process.generation;
        Ok(self)
    }
    pub fn state(&self) -> &[u8] {
        &self.state
    }
    pub fn source_checkpoint(&self) -> Hash {
        self.source_checkpoint
    }
    pub fn source_identity(&self) -> ([u8; 16], u64) {
        (self.source_instance, self.source_generation)
    }
    pub fn source_report_position(&self) -> u64 {
        self.source_report_position
    }
    pub fn callbacks(&self) -> Option<&tickwire_handover::VerifiedHandoverInputs> {
        self.callbacks.as_ref()
    }
}
pub trait RestoreFactory {
    type Running: Strategy;
    fn capabilities(&self) -> Capabilities;
    fn restore(&self, _input: &VerifiedRestoreInput) -> Result<Self::Running> {
        Err(Error::Unsupported)
    }
    fn migrate(&self, _input: &VerifiedRestoreInput) -> Result<Self::Running> {
        Err(Error::Unsupported)
    }
    fn rebuild(&self, _input: &VerifiedRestoreInput) -> Result<Self::Running> {
        Err(Error::Unsupported)
    }
    fn validate_checkpoint(&self, _state: &[u8]) -> Result<()> {
        Err(Error::Unsupported)
    }
}
pub struct Restored<S: Strategy> {
    strategy: S,
    checkpoint: Vec<u8>,
    method: Method,
    source_checkpoint: Hash,
}
impl<S: Strategy> Restored<S> {
    pub fn checkpoint(&self) -> &[u8] {
        &self.checkpoint
    }
    pub fn method(&self) -> &Method {
        &self.method
    }
    pub fn source_checkpoint(&self) -> Hash {
        self.source_checkpoint
    }
    pub fn into_strategy(self) -> S {
        self.strategy
    }
}
/// Run the selected implementation, then validate its actual serialized output.
/// This result does not persist bytes or certify catch-up after the source cut.
pub fn execute<F: RestoreFactory>(
    factory: &F,
    input: &VerifiedRestoreInput,
) -> Result<Restored<F::Running>> {
    let caps = factory.capabilities();
    caps.validate().map_err(|_| Error::Unsupported)?;
    if caps != input.expected || !caps.methods.contains(&input.method) {
        return Err(Error::Unsupported);
    }
    let strategy = match input.method {
        Method::Restore { .. } => factory.restore(input)?,
        Method::Migrate { .. } => factory.migrate(input)?,
        Method::Rebuild { .. } => factory.rebuild(input)?,
    };
    let checkpoint = strategy.checkpoint().map_err(|_| Error::Callback)?;
    if checkpoint.is_empty() || checkpoint.len() > tickwire_recovery::MAX_OBJECT_BYTES {
        return Err(Error::Bounds);
    }
    factory.validate_checkpoint(&checkpoint)?;
    Ok(Restored {
        strategy,
        checkpoint,
        method: input.method.clone(),
        source_checkpoint: input.source_checkpoint,
    })
}
fn validate_target(t: &Target) -> Result<()> {
    if t.instance == [0; 16] || t.generation == 0 || t.logical_strategy == 0 {
        return Err(Error::Identity);
    }
    if let Some(c) = &t.command {
        let c = c.command();
        if c.context.strategy.instance_id != t.instance
            || c.context.request.strategy_generation != t.generation
            || c.context.strategy.logical_strategy_id != t.logical_strategy
        {
            return Err(Error::Identity);
        }
    }
    Ok(())
}
fn base(schema: u64, methods: Vec<Method>, orders: Vec<OrderKind>) -> Capabilities {
    Capabilities {
        state_schema: schema,
        input_schema: 1,
        output_schema: 1,
        products: vec![Product::Spot],
        position_modes: vec![PositionMode::Spot],
        orders,
        methods,
    }
}
pub struct ObserveFactory;
impl RestoreFactory for ObserveFactory {
    type Running = ObserveOnly;
    fn capabilities(&self) -> Capabilities {
        base(
            OBSERVE_SCHEMA,
            vec![Method::Restore {
                schema: OBSERVE_SCHEMA,
            }],
            vec![],
        )
    }
    fn restore(&self, input: &VerifiedRestoreInput) -> Result<ObserveOnly> {
        validate_target(&input.target)?;
        if input.source_schema != OBSERVE_SCHEMA
            || input.target.command.is_some()
            || input.target.enabled
        {
            return Err(Error::Identity);
        }
        self.validate_checkpoint(&input.state)?;
        Ok(ObserveOnly)
    }
    fn validate_checkpoint(&self, state: &[u8]) -> Result<()> {
        if state == b"TWOBS001" {
            Ok(())
        } else {
            Err(Error::State)
        }
    }
}
pub struct FixedDemoFactory;
pub struct RestoredDemo {
    inner: FixedDemo,
    action: Hash,
}
impl Strategy for RestoredDemo {
    fn checkpoint(&self) -> crate::Result<Vec<u8>> {
        let mut bytes = b"TWFD0002".to_vec();
        bytes.push(u8::from(self.inner.command.is_none()));
        bytes.extend(self.action);
        Ok(bytes)
    }
    fn on_market(
        &mut self,
        view: crate::MarketView<'_>,
        now: u64,
    ) -> crate::Result<Option<CanonicalCommand>> {
        self.inner.on_market(view, now)
    }
    fn on_report(
        &mut self,
        r: &crate::reports::AccountReport,
        n: u64,
    ) -> crate::Result<Option<CanonicalCommand>> {
        self.inner.on_report(r, n)
    }
}
fn action_key(c: &CanonicalCommand) -> Hash {
    tickwire_recovery::digest(&c.command().context.strategy.economic_intent_id)
}
fn construct(input: &VerifiedRestoreInput, spent: bool, action: Hash) -> Result<RestoredDemo> {
    validate_target(&input.target)?;
    let command = input.target.command.clone().ok_or(Error::Identity)?;
    if command.command().context.request.product != tickwire_venue::Product::Spot
        || !matches!(&command.command().body,tickwire_venue::CommandBody::New(order) if order.order_type==tickwire_venue::OrderType::Limit && order.mode==tickwire_venue::ProductMode::Spot)
    {
        return Err(Error::Unsupported);
    }
    if action_key(&command) != action {
        return Err(Error::Identity);
    }
    let inner = FixedDemo {
        original: command.clone(),
        command: if spent { None } else { Some(command) },
        minimum_bid_ticks: input.target.minimum_bid_ticks,
        enabled: input.target.enabled,
    };
    Ok(RestoredDemo { inner, action })
}
impl RestoreFactory for FixedDemoFactory {
    type Running = RestoredDemo;
    fn capabilities(&self) -> Capabilities {
        base(
            DEMO_SCHEMA,
            vec![
                Method::Restore {
                    schema: DEMO_SCHEMA,
                },
                Method::Migrate {
                    source_schema: DEMO_V1_SCHEMA,
                    target_schema: DEMO_SCHEMA,
                    method_id: DEMO_MIGRATION,
                },
            ],
            vec![OrderKind::Limit],
        )
    }
    fn restore(&self, input: &VerifiedRestoreInput) -> Result<RestoredDemo> {
        if input.source_schema != DEMO_SCHEMA {
            return Err(Error::Unsupported);
        }
        self.validate_checkpoint(&input.state)?;
        construct(
            input,
            input.state[8] == 1,
            input.state[9..].try_into().map_err(|_| Error::State)?,
        )
    }
    fn migrate(&self, input: &VerifiedRestoreInput) -> Result<RestoredDemo> {
        if input.method
            != (Method::Migrate {
                source_schema: DEMO_V1_SCHEMA,
                target_schema: DEMO_SCHEMA,
                method_id: DEMO_MIGRATION,
            })
            || input.source_schema != DEMO_V1_SCHEMA
        {
            return Err(Error::Unsupported);
        }
        let b = &input.state;
        if b.len() < 21 || &b[..8] != b"TWFD0001" || b[8] > 1 {
            return Err(Error::State);
        }
        let len = u32::from_le_bytes(b[17..21].try_into().map_err(|_| Error::State)?) as usize;
        if len.checked_add(21) != Some(b.len()) {
            return Err(Error::State);
        }
        let old = CanonicalCommand::decode(&b[21..]).map_err(|_| Error::State)?;
        if old.command().context.strategy.instance_id != input.source_instance
            || old.command().context.request.strategy_generation != input.source_generation
            || old.command().context.strategy.logical_strategy_id != input.target.logical_strategy
        {
            return Err(Error::Identity);
        }
        construct(input, b[8] == 0, action_key(&old))
    }
    fn validate_checkpoint(&self, b: &[u8]) -> Result<()> {
        if b.len() == 41 && &b[..8] == b"TWFD0002" && b[8] <= 1 && b[9..] != [0; 32] {
            Ok(())
        } else {
            Err(Error::State)
        }
    }
}
#[cfg(test)]
#[path = "restore_tests.rs"]
mod tests;
