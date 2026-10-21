//! Synchronized input completion before any command is handed to IntentWriter.
use super::*;
use tickwire_recovery::{BlobStore, StoreKey};

pub struct DurableEngine<S, B> {
    pub(super) engine: Engine<S>,
    pub(super) store: B,
    pub(super) anchor: VerifiedAnchor,
    pub(super) durable_object: Object,
    pub(super) durable: Checkpoint,
    pub(super) commit: Object,
    pub(super) pending: Vec<CanonicalCommand>,
    pub(super) stopped: bool,
}
/// Not Clone: the commands are delivered once by consuming this result. This is
/// input durability, not executor admission or permission to send to a venue.
pub struct CommittedBatch {
    commit: Object,
    cut: Cut,
    commands: Vec<CanonicalCommand>,
}
impl CommittedBatch {
    pub fn commit(&self) -> Object {
        self.commit
    }
    pub fn cut(&self) -> &Cut {
        &self.cut
    }
    pub fn publish(self, mut write: impl FnMut(&CanonicalCommand) -> Result<()>) -> Result<()> {
        for command in &self.commands {
            write(command)?;
        }
        Ok(())
    }
}
fn put(store: &mut impl BlobStore, bytes: &[u8]) -> Result<Object> {
    let object = Object::of(bytes);
    store
        .put_sync(StoreKey::Object(object.digest), bytes)
        .map_err(|_| Error::Storage)?;
    Ok(object)
}

pub use tickwire_release::replay::history::CommittedHistory;
/// Read-only content verification; a valid HEAD does not grant runtime access.
pub fn verify_committed_history(
    store: &impl BlobStore,
    head: &Head,
    maximum_commits: usize,
) -> Result<CommittedHistory> {
    head.bytes()?;
    let history =
        tickwire_release::replay::history::verify(head.commit, maximum_commits, |object| {
            store
                .get(StoreKey::Object(object.digest), MAX_BYTES)
                .map_err(|_| tickwire_release::Error::Missing)
        })?;
    if history.checkpoint().source != head.source
        || history.checkpoint().cut != head.cut
        || Object::of(&history.checkpoint().bytes()?) != head.checkpoint
    {
        return Err(Error::Identity);
    }
    Ok(history)
}
impl<S: ReplayableStrategy, B: BlobStore> DurableEngine<S, B> {
    pub fn new(anchor: VerifiedAnchor, strategy: S, mut store: B) -> Result<Self> {
        let engine = Engine::new(&anchor, strategy)?;
        let anchor_object = put(&mut store, &anchor.checkpoint().bytes()?)?;
        let tail = put(&mut store, &encode_tail(&anchor, &[])?)?;
        let input_commit = InputCommit {
            schema: 2,
            anchor: anchor_object,
            tail,
            checkpoint: anchor_object,
            previous_commit: None,
        };
        let commit = put(&mut store, &input_commit.bytes()?)?;
        Ok(Self {
            durable: anchor.checkpoint().clone(),
            engine,
            store,
            anchor,
            durable_object: anchor_object,
            commit,
            pending: vec![],
            stopped: false,
        })
    }
    pub fn read_object(&self, object: Object) -> Result<Vec<u8>> {
        object.validate(MAX_BYTES as u64)?;
        let bytes = self
            .store
            .get(StoreKey::Object(object.digest), MAX_BYTES)
            .map_err(|_| Error::Storage)?;
        object.verify(&bytes, MAX_BYTES as u64)?;
        Ok(bytes)
    }
    pub fn store_object(&mut self, bytes: &[u8]) -> Result<Object> {
        if self.stopped {
            return Err(Error::Poisoned);
        }
        match put(&mut self.store, bytes) {
            Ok(o) => Ok(o),
            Err(e) => {
                self.stop();
                Err(e)
            }
        }
    }
    /// Re-anchor the retained exact tail at a previously synchronized checkpoint.
    /// This does not run callbacks or discard the active history.
    pub fn capture(&mut self, request: &ControlInput) -> Result<Object> {
        if self.stopped || !self.pending.is_empty() || self.memory_cut() != self.last_durable_cut()
        {
            return Err(Error::Poisoned);
        }
        let ControlInput::Capture {
            source,
            anchor,
            expected,
            pause,
        } = request
        else {
            return Err(Error::Identity);
        };
        if source != &self.durable.source {
            return Err(Error::Identity);
        }
        let bytes = self.read_object(*anchor)?;
        let checkpoint = Checkpoint::decode(&bytes)?;
        if checkpoint.source != *source
            || checkpoint.cut != *expected
            || expected.sequence < self.anchor.checkpoint().cut.sequence
            || expected.sequence > self.durable.cut.sequence
        {
            return Err(Error::Identity);
        }
        let offset = usize::try_from(expected.sequence - self.anchor.checkpoint().cut.sequence)
            .map_err(|_| Error::Capacity)?;
        if (offset == 0 && expected != &self.anchor.checkpoint().cut)
            || (offset > 0
                && self.engine.rows.get(offset - 1).map(|r| r.sequence) != Some(expected.sequence))
        {
            return Err(Error::Sequence);
        }
        let models = checkpoint
            .runtime
            .models
            .iter()
            .map(|m| (m.id, Object::of(&m.value.bytes)))
            .collect::<Vec<_>>();
        let verified = verify_anchor(
            &bytes,
            source,
            Object::of(&checkpoint.runtime.config.bytes),
            &models,
        )?;
        let tail_bytes = encode_tail(
            &verified,
            self.engine.rows.get(offset..).ok_or(Error::Sequence)?,
        )?;
        verify_tail(&verified, &tail_bytes, &self.durable.cut)?;
        let tail = self.store_object(&tail_bytes)?;
        let checkpoint_bytes = self.durable.bytes()?;
        let current = self.store_object(&checkpoint_bytes)?;
        let request_object = self.store_object(&request.bytes()?)?;
        self.store_object(
            &CapturedTail {
                schema: 1,
                request: request_object,
                source: source.clone(),
                anchor: *anchor,
                tail,
                checkpoint: current,
                cut: self.durable.cut.clone(),
                paused: *pause,
            }
            .bytes()?,
        )
    }
    pub fn head(&self) -> Result<Head> {
        let commit = InputCommit::decode(&self.read_object(self.commit)?)?;
        Ok(Head {
            schema: 1,
            source: self.durable.source.clone(),
            commit: self.commit,
            checkpoint: commit.checkpoint,
            cut: self.durable.cut.clone(),
        })
    }
    pub fn anchor(&self) -> &Checkpoint {
        self.anchor.checkpoint()
    }
    pub fn recorded(&self) -> &[MergedInput] {
        self.engine.recorded()
    }
    pub fn strategy(&self) -> &S {
        &self.engine.strategy
    }
    pub fn durable_checkpoint(&self) -> &Checkpoint {
        &self.durable
    }
    pub fn commit(&self) -> Object {
        self.commit
    }
    pub fn last_durable_cut(&self) -> &Cut {
        &self.durable.cut
    }
    pub fn memory_cut(&self) -> &Cut {
        self.engine.last_completed_cut()
    }
    pub fn stopped(&self) -> bool {
        self.stopped
    }
    pub fn next_deadline(&self) -> Option<u64> {
        if self.stopped {
            None
        } else {
            self.engine.next_deadline()
        }
    }
    pub(super) fn stop(&mut self) {
        self.stopped = true;
        self.engine.poisoned = true;
        self.pending.clear();
    }
    pub fn apply(&mut self, input: Input, now_ns: u64) -> Result<()> {
        if self.stopped {
            return Err(Error::Poisoned);
        }
        match self.engine.apply_live(input, now_ns) {
            Ok(applied) => {
                if self.pending.len() + applied.commands.len() > MAX_EFFECTS {
                    self.stop();
                    return Err(Error::Capacity);
                }
                self.pending.extend(applied.commands);
                Ok(())
            }
            Err(e) => {
                self.stop();
                Err(e)
            }
        }
    }
    pub fn poll_timers(&mut self, now_ns: u64, budget: usize) -> Result<usize> {
        if self.stopped {
            return Err(Error::Poisoned);
        }
        match self.engine.poll_timers(now_ns, budget) {
            Ok(applied) => {
                let count = applied.len();
                for input in applied {
                    if self.pending.len() + input.commands.len() > MAX_EFFECTS {
                        self.stop();
                        return Err(Error::Capacity);
                    }
                    self.pending.extend(input.commands);
                }
                Ok(count)
            }
            Err(e) => {
                self.stop();
                Err(e)
            }
        }
    }
    pub fn flush(&mut self) -> Result<CommittedBatch> {
        if self.stopped {
            return Err(Error::Poisoned);
        }
        if self.engine.last_completed_cut() == &self.durable.cut {
            if !self.pending.is_empty() {
                self.stop();
                return Err(Error::Identity);
            }
            return Ok(CommittedBatch {
                commit: self.commit,
                cut: self.durable.cut.clone(),
                commands: vec![],
            });
        }
        let result = (|| {
            let checkpoint = self.engine.checkpoint()?;
            let models = self
                .durable
                .runtime
                .models
                .iter()
                .map(|m| (m.id, Object::of(&m.value.bytes)))
                .collect::<Vec<_>>();
            let anchor = verify_anchor(
                &self.durable.bytes()?,
                &self.durable.source,
                Object::of(&self.durable.runtime.config.bytes),
                &models,
            )?;
            let offset = usize::try_from(
                self.durable
                    .cut
                    .sequence
                    .checked_sub(self.anchor.checkpoint().cut.sequence)
                    .ok_or(Error::Sequence)?,
            )
            .map_err(|_| Error::Capacity)?;
            let rows = self
                .engine
                .recorded()
                .get(offset..)
                .ok_or(Error::Sequence)?;
            let tail = put(&mut self.store, &encode_tail(&anchor, rows)?)?;
            let object = put(&mut self.store, &checkpoint.bytes()?)?;
            let commit = InputCommit {
                schema: 2,
                anchor: self.durable_object,
                tail,
                checkpoint: object,
                previous_commit: Some(self.commit),
            };
            let stored = put(&mut self.store, &commit.bytes()?)?;
            Ok::<_, Error>((checkpoint, object, stored))
        })();
        match result {
            Ok((checkpoint, object, stored)) => {
                self.durable = checkpoint;
                self.durable_object = object;
                self.commit = stored;
                Ok(CommittedBatch {
                    commit: self.commit,
                    cut: self.durable.cut.clone(),
                    commands: std::mem::take(&mut self.pending),
                })
            }
            Err(e) => {
                self.stop();
                Err(e)
            }
        }
    }
}
