//! The same deterministic callback executor serves live recording and isolated
//! old-binary replay. Replay returns state, never commands eligible for sending.
use sha2::{Digest, Sha256};
use tickwire_release::{Hash, Object, replay::*};
use tickwire_venue::CanonicalCommand;
pub mod account;
pub mod application;
pub mod catchup;
pub mod control;
pub mod durable;
pub mod install;
pub mod live;
pub mod native;
pub mod process;
pub mod rebuild;
pub mod retained;
pub mod rollback;
#[cfg(test)]
mod source_tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Format(tickwire_release::Error),
    Unsupported,
    Identity,
    Sequence,
    Time,
    Capacity,
    Callback,
    StateMismatch,
    EffectsMismatch,
    Poisoned,
    Storage,
}
pub type Result<T> = core::result::Result<T, Error>;
impl From<tickwire_release::Error> for Error {
    fn from(e: tickwire_release::Error) -> Self {
        Self::Format(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

pub trait ReplayableStrategy {
    /// Implementations include private market/account views in this state.
    fn checkpoint(&self) -> Result<Vec<u8>>;
    fn apply(
        &mut self,
        input: &Input,
        context: &mut ReplayContext<'_>,
        effects: &mut ReplayEffects,
    ) -> Result<()>;
}
pub trait SourceReplayFactory {
    type Running: ReplayableStrategy;
    fn binary_digest(&self) -> Hash;
    fn state_schema(&self) -> u64;
    fn restore_source(&self, _source: &VerifiedAnchor) -> Result<Self::Running> {
        Err(Error::Unsupported)
    }
}
/// SDK-controlled auxiliary state, with no real clock or outbound transport.
pub trait ReplayViews {
    fn market(&self, stream: &StreamKey, instrument: u64) -> Option<crate::MarketView<'_>>;
    fn account(&self, stream: &StreamKey) -> Option<&tickwire_account::Account>;
}
pub struct ReplayContext<'a> {
    views: Option<&'a dyn ReplayViews>,
    runtime: &'a mut RuntimeState,
}
impl ReplayContext<'_> {
    pub fn market_streams(&self) -> impl Iterator<Item = &StreamKey> {
        self.runtime.markets.iter().map(|c| &c.stream)
    }
    pub fn account_streams(&self) -> impl Iterator<Item = &StreamKey> {
        self.runtime.accounts.iter().map(|c| &c.stream)
    }
    pub fn market(&self, stream: &StreamKey, instrument: u64) -> Option<crate::MarketView<'_>> {
        self.views?.market(stream, instrument)
    }
    pub fn account(&self, stream: &StreamKey) -> Option<&tickwire_account::Account> {
        self.views?.account(stream)
    }
    fn with_views<'a>(&'a mut self, views: &'a dyn ReplayViews) -> ReplayContext<'a> {
        ReplayContext {
            runtime: self.runtime,
            views: Some(views),
        }
    }
    pub fn now_ns(&self) -> u64 {
        self.runtime.now_ns
    }
    pub fn config(&self) -> &VersionedBytes {
        &self.runtime.config
    }
    pub fn model(&self, id: u16) -> Option<&VersionedBytes> {
        self.runtime
            .models
            .iter()
            .find(|m| m.id == id)
            .map(|m| &m.value)
    }
    pub fn arm_timer(&mut self, id: u64, deadline_ns: u64) -> Result<u64> {
        if id == 0 || deadline_ns < self.now_ns() {
            return Err(Error::Time);
        }
        match self.runtime.timers.binary_search_by_key(&id, |t| t.id) {
            Ok(i) => {
                let t = &mut self.runtime.timers[i];
                t.generation = t.generation.checked_add(1).ok_or(Error::Capacity)?;
                t.deadline_ns = Some(deadline_ns);
                Ok(t.generation)
            }
            Err(i) => {
                if self.runtime.timers.len() == MAX_TIMERS {
                    return Err(Error::Capacity);
                }
                self.runtime.timers.insert(
                    i,
                    Timer {
                        id,
                        generation: 1,
                        deadline_ns: Some(deadline_ns),
                    },
                );
                Ok(1)
            }
        }
    }
    pub fn cancel_timer(&mut self, id: u64, generation: u64) -> Result<()> {
        let t = self
            .runtime
            .timers
            .iter_mut()
            .find(|t| t.id == id && t.generation == generation)
            .ok_or(Error::Sequence)?;
        t.deadline_ns = None;
        Ok(())
    }
    pub fn random_u64(&mut self) -> Result<u64> {
        let Rng::Xoshiro256StarStarV1 { state, draws } = &mut self.runtime.rng else {
            return Err(Error::Unsupported);
        };
        *draws = draws.checked_add(1).ok_or(Error::Capacity)?;
        let out = state[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = state[1] << 17;
        state[2] ^= state[0];
        state[3] ^= state[1];
        state[1] ^= state[2];
        state[0] ^= state[3];
        state[2] ^= t;
        state[3] = state[3].rotate_left(45);
        Ok(out)
    }
}
pub struct ReplayEffects {
    allowed: bool,
    commands: Vec<CanonicalCommand>,
}
impl ReplayEffects {
    pub fn emit(&mut self, command: CanonicalCommand) -> Result<()> {
        if !self.allowed {
            return Err(Error::Unsupported);
        }
        if self.commands.len() == MAX_EFFECTS {
            return Err(Error::Capacity);
        }
        self.commands.push(command);
        Ok(())
    }
}
pub struct LiveApplied {
    pub record: MergedInput,
    pub commands: Vec<CanonicalCommand>,
}
pub struct Engine<S> {
    source: Source,
    runtime: RuntimeState,
    strategy: S,
    cut: Cut,
    poisoned: bool,
    rows: Vec<MergedInput>,
    row_bytes: usize,
}
impl<S: ReplayableStrategy> Engine<S> {
    pub fn new(anchor: &VerifiedAnchor, strategy: S) -> Result<Self> {
        let c = anchor.checkpoint();
        if strategy.checkpoint()? != c.business {
            return Err(Error::StateMismatch);
        }
        Ok(Self {
            source: c.source.clone(),
            runtime: c.runtime.clone(),
            strategy,
            cut: c.cut.clone(),
            poisoned: false,
            rows: vec![],
            row_bytes: 68,
        })
    }
    pub fn last_completed_cut(&self) -> &Cut {
        &self.cut
    }
    pub fn recorded(&self) -> &[MergedInput] {
        &self.rows
    }
    pub fn next_deadline(&self) -> Option<u64> {
        self.runtime
            .timers
            .iter()
            .filter_map(|t| t.deadline_ns)
            .min()
    }
    /// Delivers due timers in (deadline, id) order, using the same callback and
    /// completed-input path as market/account events. The caller supplies a
    /// fresh clock reading; replay never calls this producer.
    pub fn poll_timers(&mut self, now_ns: u64, budget: usize) -> Result<Vec<LiveApplied>> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if budget == 0 || budget > MAX_TIMERS {
            return Err(Error::Capacity);
        }
        if now_ns < self.runtime.now_ns {
            self.poisoned = true;
            return Err(Error::Time);
        }
        let mut delivered = Vec::new();
        for _ in 0..budget {
            let due = self
                .runtime
                .timers
                .iter()
                .filter_map(|t| {
                    t.deadline_ns
                        .filter(|d| *d <= now_ns)
                        .map(|d| (d, t.id, t.generation))
                })
                .min();
            let Some((deadline_ns, id, generation)) = due else {
                break;
            };
            delivered.push(self.apply_live(
                Input::TimerFired {
                    id,
                    generation,
                    deadline_ns,
                },
                now_ns,
            )?);
        }
        Ok(delivered)
    }
    pub fn checkpoint(&self) -> Result<Checkpoint> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        Ok(Checkpoint::new(
            self.source.clone(),
            self.cut.sequence,
            self.cut.chain,
            self.strategy.checkpoint()?,
            self.runtime.clone(),
        )?)
    }
    pub fn apply_live(&mut self, input: Input, now_ns: u64) -> Result<LiveApplied> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let result = self.execute(input, now_ns, None);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn apply_replay(&mut self, row: &MergedInput) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if self.cut.sequence.checked_add(1) != Some(row.sequence) {
            self.poisoned = true;
            return Err(Error::Sequence);
        }
        let result = self
            .execute(row.input.clone(), row.now_ns, Some(&row.effects))
            .map(|_| ());
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn execute(
        &mut self,
        input: Input,
        now_ns: u64,
        expected: Option<&[Effect]>,
    ) -> Result<LiveApplied> {
        if self.rows.len() == MAX_ROWS {
            return Err(Error::Capacity);
        }
        if now_ns < self.runtime.now_ns {
            return Err(Error::Time);
        }
        let mut record = MergedInput {
            sequence: self.cut.sequence.checked_add(1).ok_or(Error::Capacity)?,
            now_ns,
            input,
            effects: vec![],
        };
        record.validate()?;
        self.runtime.now_ns = now_ns;
        advance(&mut self.runtime, &record.input)?;
        let allowed = !matches!(
            record.input,
            Input::MarketApplied { .. }
                | Input::MarketTransition { .. }
                | Input::ConfigApply { .. }
                | Input::ModelApply { .. }
                | Input::Entropy { .. }
                | Input::MarketCallback { invoked: false, .. }
        );
        let mut effects = ReplayEffects {
            allowed,
            commands: vec![],
        };
        self.strategy.apply(
            &record.input,
            &mut ReplayContext {
                views: None,
                runtime: &mut self.runtime,
            },
            &mut effects,
        )?;
        for (ordinal, command) in effects.commands.iter().enumerate() {
            let c = &command.command().context;
            if c.strategy.instance_id != self.source.instance
                || c.strategy.logical_strategy_id != self.source.logical_strategy
                || c.request.strategy_generation != self.source.generation
                || c.strategy.created_mono_ns != now_ns
            {
                return Err(Error::Identity);
            }
            record.effects.push(Effect {
                ordinal: ordinal as u32,
                economic_intent: c.strategy.economic_intent_id,
                command: command.bytes().to_vec(),
            });
        }
        if expected.is_some_and(|e| e != record.effects) {
            return Err(Error::EffectsMismatch);
        }
        record.validate()?;
        let len = canonical(&record)?.len();
        let next_bytes = self
            .row_bytes
            .checked_add(4 + len)
            .filter(|n| *n <= MAX_BYTES)
            .ok_or(Error::Capacity)?;
        for effect in &record.effects {
            let mut h = Sha256::new();
            h.update(b"tickwire.strategy-effect.v2");
            h.update(self.runtime.effects.digest);
            h.update(self.source.instance);
            h.update(self.source.input_stream);
            h.update(record.sequence.to_be_bytes());
            h.update(canonical(effect)?);
            self.runtime.effects.count = self
                .runtime
                .effects
                .count
                .checked_add(1)
                .ok_or(Error::Capacity)?;
            self.runtime.effects.digest = h.finalize().into();
        }
        self.runtime.validate()?;
        let next = Checkpoint::new(
            self.source.clone(),
            record.sequence,
            row_chain(self.cut.chain, &record)?,
            self.strategy.checkpoint()?,
            self.runtime.clone(),
        )?;
        self.row_bytes = next_bytes;
        self.cut = next.cut;
        self.rows.push(record.clone());
        Ok(LiveApplied {
            record,
            commands: effects.commands,
        })
    }
}
fn advance_cursor(
    cursors: &mut Vec<Cursor>,
    stream: &StreamKey,
    sequence: u64,
    content: Hash,
    first: Option<u64>,
) -> Result<()> {
    match cursors.binary_search_by(|c| c.stream.cmp(stream)) {
        Ok(i) => {
            if cursors[i].sequence.checked_add(1) != Some(sequence) {
                return Err(Error::Sequence);
            }
            cursors[i].sequence = sequence;
            cursors[i].content_digest = content;
        }
        Err(i) => {
            if first != Some(sequence)
                || cursors.iter().any(|c| c.stream.identity == stream.identity)
            {
                return Err(Error::Sequence);
            }
            if cursors.len() == MAX_STREAMS {
                return Err(Error::Capacity);
            }
            cursors.insert(
                i,
                Cursor {
                    stream: stream.clone(),
                    sequence,
                    content_digest: content,
                },
            );
        }
    }
    Ok(())
}
fn advance(runtime: &mut RuntimeState, input: &Input) -> Result<()> {
    let hash = tickwire_recovery::digest(&canonical(input)?);
    match input {
        Input::MarketTransition {
            previous,
            next,
            activated_sequence,
            ..
        } => {
            let index = runtime
                .markets
                .iter()
                .position(|c| c == previous)
                .ok_or(Error::Identity)?;
            if runtime
                .markets
                .iter()
                .any(|c| c.stream.identity == next.identity)
            {
                return Err(Error::Identity);
            }
            runtime.markets.remove(index);
            let at = runtime
                .markets
                .binary_search_by(|c| c.stream.cmp(next))
                .unwrap_err();
            runtime.markets.insert(
                at,
                Cursor {
                    stream: next.clone(),
                    sequence: *activated_sequence,
                    content_digest: hash,
                },
            );
        }
        Input::MarketApplied {
            stream, sequence, ..
        } => advance_cursor(&mut runtime.markets, stream, *sequence, hash, None)?,
        Input::MarketCallback { stream, .. } => {
            if !runtime.markets.iter().any(|c| c.stream == *stream) {
                return Err(Error::Identity);
            }
        }
        Input::AccountCallback {
            stream, position, ..
        } => advance_cursor(&mut runtime.accounts, stream, *position, hash, Some(0))?,
        Input::External {
            stream, sequence, ..
        }
        | Input::Entropy {
            stream, sequence, ..
        } => {
            advance_cursor(&mut runtime.external, stream, *sequence, hash, Some(1))?;
            if let Input::Entropy { seed, .. } = input {
                runtime.rng = Rng::Xoshiro256StarStarV1 {
                    state: *seed,
                    draws: 0,
                };
            }
        }
        Input::TimerFired {
            id,
            generation,
            deadline_ns,
        } => {
            let timer = runtime
                .timers
                .iter_mut()
                .find(|t| {
                    t.id == *id
                        && t.generation == *generation
                        && t.deadline_ns == Some(*deadline_ns)
                })
                .ok_or(Error::Sequence)?;
            timer.deadline_ns = None;
        }
        Input::ConfigApply { previous, next } => {
            if Object::of(&runtime.config.bytes) != *previous
                || next.revision <= runtime.config.revision
            {
                return Err(Error::Identity);
            }
            runtime.config = next.clone();
        }
        Input::ModelApply { id, previous, next } => {
            let model = runtime
                .models
                .iter_mut()
                .find(|m| m.id == *id)
                .ok_or(Error::Unsupported)?;
            if Object::of(&model.value.bytes) != *previous || next.revision <= model.value.revision
            {
                return Err(Error::Identity);
            }
            model.value = next.clone();
        }
    }
    Ok(())
}
pub struct VerifiedStateAtCut {
    checkpoint: Checkpoint,
}
impl VerifiedStateAtCut {
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
}
pub fn replay_source<F: SourceReplayFactory>(
    factory: &F,
    anchor: &VerifiedAnchor,
    tail: &VerifiedTail,
) -> Result<VerifiedStateAtCut> {
    let source = &anchor.checkpoint().source;
    if source != tail.source()
        || &anchor.checkpoint().cut != tail.start()
        || factory.binary_digest() != source.binary_digest
        || factory.state_schema() != source.state_schema
    {
        return Err(Error::Identity);
    }
    let strategy = factory.restore_source(anchor)?;
    let mut engine = Engine::new(anchor, strategy)?;
    for row in tail.rows() {
        engine.apply_replay(row)?;
    }
    let checkpoint = engine.checkpoint()?;
    if &checkpoint.cut != tail.end() {
        return Err(Error::StateMismatch);
    }
    Ok(VerifiedStateAtCut { checkpoint })
}

#[cfg(test)]
mod tests;
