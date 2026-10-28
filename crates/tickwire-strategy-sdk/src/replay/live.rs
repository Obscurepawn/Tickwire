//! Worker-owned actual callbacks. Commands leave this module only after input sync.
use super::*;
use crate::{Strategy, bootstrap::Bootstrap, reports::AccountReport};
use application::{Application, market_stream};
use durable::DurableEngine;
use tickwire_ipc::{RecordHeader, ResourceHeader, Wire};
use tickwire_recovery::FileBlobStore;

enum Owner<S> {
    Legacy {
        strategy: S,
        replica: Box<crate::MarketReplica>,
        maximum_age: u64,
    },
    Recorded(Box<DurableEngine<Application<S>, FileBlobStore>>),
}
pub struct Live<S> {
    owner: Owner<S>,
    market: StreamKey,
    account: Option<StreamKey>,
    maximum_age: u64,
    directory: std::path::PathBuf,
}
/// The owner binds this request to its authenticated expansion plan before
/// quiescing the old consumer. Digests record that decision, not authorization.
pub struct MarketTransitionRequest {
    pub operation_id: [u8; 16],
    pub approval_digest: Hash,
    pub previous_applied: u64,
    pub maximum_levels: usize,
}
fn persist_transition<S: Strategy>(
    engine: &mut DurableEngine<Application<S>, FileBlobStore>,
    directory: &std::path::Path,
    input: Input,
    expected_view: &[u8],
    now: u64,
) -> Result<StreamKey> {
    let result = (|| {
        if engine.memory_cut() != engine.last_durable_cut() || !engine.pending.is_empty() {
            return Err(Error::Identity);
        }
        let record = MergedInput {
            sequence: engine
                .memory_cut()
                .sequence
                .checked_add(1)
                .ok_or(Error::Capacity)?,
            now_ns: now,
            input,
            effects: vec![],
        };
        record.validate()?;
        if canonical(&record)?
            .len()
            .checked_add(engine.engine.row_bytes + 4)
            .is_none_or(|n| n > MAX_BYTES)
        {
            return Err(Error::Capacity);
        }
        if engine.strategy().transition_state(&record.input, now)? != expected_view {
            return Err(Error::StateMismatch);
        }
        let Input::MarketTransition { next, .. } = &record.input else {
            return Err(Error::Identity);
        };
        let next = next.clone();
        engine.apply(record.input, now)?;
        drop(engine.flush()?);
        sync_head(engine, directory).map_err(|_| Error::Storage)?;
        Ok(next)
    })();
    if result.is_err() {
        engine.stop();
    }
    result
}
impl<S: Strategy> Live<S> {
    pub fn supports_market_transition(&self) -> bool {
        matches!(&self.owner, Owner::Recorded(e) if !e.stopped())
    }
    /// Records the actual completed JOIN before the caller swaps its Client or
    /// permits any market callback. The caller supplies the authenticated plan
    /// and the old consumer's post-QUIESCE applied watermark.
    pub fn transition_market(
        &mut self,
        request: MarketTransitionRequest,
        client: &tickwire_market_source::shm::Client,
        now: u64,
    ) -> crate::Result<()> {
        let Owner::Recorded(e) = &mut self.owner else {
            return Err(crate::Error::NotReady);
        };
        let result = (|| {
            if e.memory_cut() != e.last_durable_cut()
                || !e.pending.is_empty()
                || client.domain.boot_id() != e.durable_checkpoint().source.clock.boot_id
                || client.domain.generation() != e.durable_checkpoint().source.clock.generation
            {
                return Err(Error::Identity);
            }
            let previous = e
                .durable_checkpoint()
                .runtime
                .markets
                .iter()
                .find(|c| c.stream == self.market)
                .ok_or(Error::Identity)?
                .clone();
            if previous.sequence != request.previous_applied
                || client.consumer.watermarks().applied != client.replica.sequence()
                || client.baseline.join
                    != crate::bootstrap::wire_bytes(&client.join).map_err(|_| Error::Identity)?
            {
                return Err(Error::Identity);
            }
            client
                .catchup
                .iter()
                .try_fold(client.baseline_bytes.len(), |n, row| {
                    n.checked_add(row.record.len())
                        .and_then(|n| n.checked_add(row.payload.len()))
                        .filter(|n| *n <= MAX_BYTES)
                        .ok_or(Error::Capacity)
                })?;
            let next = market_stream(&client.bank)?;
            let input = Input::MarketTransition {
                operation_id: request.operation_id,
                approval_digest: request.approval_digest,
                previous,
                next: next.clone(),
                baseline_sequence: client.join.snapshot_seq,
                activated_sequence: client.consumer.watermarks().applied,
                baseline: crate::recovery::snapshot_payload(
                    &client.baseline,
                    &client.baseline_bytes,
                    client.baseline_now,
                    request.maximum_levels,
                )
                .map_err(|_| Error::Identity)?,
                catchup: client
                    .catchup
                    .iter()
                    .map(|r| MarketTransitionRecord {
                        record: r.record.clone(),
                        payload: r.payload.clone(),
                        instrument: r.instrument,
                        applied_mono_ns: r.applied_mono_ns,
                    })
                    .collect(),
            };
            let expected = client.replica.checkpoint().map_err(|_| Error::Identity)?;
            self.market = persist_transition(e, &self.directory, input, &expected, now)?;
            Ok(())
        })();
        if result.is_err() {
            e.stop();
        }
        Ok(result?)
    }
    pub fn new(
        config: &Bootstrap,
        strategy: S,
        resource: ResourceHeader,
        replica: crate::MarketReplica,
        at: u64,
    ) -> crate::Result<Self> {
        let key = market_stream(&resource)?;
        let owner = if let Some(registration) = strategy.replay_registration()?
            && config.recovery_template.is_some()
        {
            let binary = executable_digest()?;
            let source = Source {
                logical_strategy: config.market.local.logical_strategy,
                instance: config.market.local.instance,
                generation: config.market.local.generation,
                binary_digest: binary,
                state_schema: registration.schema,
                input_stream: config.market.local.instance,
                clock: ClockDomain {
                    boot_id: config.market.local.host_boot,
                    generation: config.clock_generation,
                },
            };
            let app = Application::new(
                strategy,
                config.maximum_market_age_ns,
                vec![(resource, replica, config.maximum_levels)],
            )?;
            let runtime = RuntimeState {
                now_ns: at,
                config: registration.config,
                models: registration.models,
                markets: app.market_cursors()?,
                accounts: vec![],
                external: vec![],
                timers: vec![],
                rng: registration.rng,
                effects: EffectCut {
                    count: 0,
                    digest: [0; 32],
                },
            };
            let checkpoint =
                Checkpoint::new(source.clone(), 0, [0; 32], app.checkpoint()?, runtime)?;
            let models = checkpoint
                .runtime
                .models
                .iter()
                .map(|m| (m.id, Object::of(&m.value.bytes)))
                .collect::<Vec<_>>();
            let anchor = verify_anchor(
                &checkpoint.bytes()?,
                &source,
                Object::of(&checkpoint.runtime.config.bytes),
                &models,
            )?;
            let store = FileBlobStore::open(config.command_directory.join("merged-inputs"))?;
            Owner::Recorded(Box::new(DurableEngine::new(anchor, app, store)?))
        } else {
            Owner::Legacy {
                strategy,
                replica: Box::new(replica),
                maximum_age: config.maximum_market_age_ns,
            }
        };
        let mut value = Self {
            owner,
            market: key,
            account: None,
            maximum_age: config.maximum_market_age_ns,
            directory: config.command_directory.join("merged-inputs"),
        };
        if let Owner::Recorded(e) = &mut value.owner {
            sync_head(e, &value.directory)?;
        }
        Ok(value)
    }
    pub fn anchor_bytes(&self) -> crate::Result<Option<Vec<u8>>> {
        match &self.owner {
            Owner::Recorded(e) => Ok(Some(e.anchor().bytes()?)),
            _ => Ok(None),
        }
    }
    pub fn recorded_after(&self, sequence: u64) -> &[MergedInput] {
        match &self.owner {
            Owner::Recorded(e) => {
                let rows = e.recorded();
                &rows[rows.partition_point(|r| r.sequence <= sequence)..]
            }
            _ => &[],
        }
    }
    pub fn checkpoint(&self) -> crate::Result<Vec<u8>> {
        match &self.owner {
            Owner::Legacy { strategy, .. } => strategy.checkpoint(),
            Owner::Recorded(e) => Ok(e.strategy().business_state()?),
        }
    }
    pub fn committed(&self) -> Option<(&Checkpoint, Object)> {
        match &self.owner {
            Owner::Recorded(e) => Some((e.durable_checkpoint(), e.commit())),
            _ => None,
        }
    }
    /// Exact completed installation retries are checked against their original
    /// target cut. A different request cannot reuse the selected instance.
    pub fn control_checkpoint(&self, input: &ControlInput) -> crate::Result<Checkpoint> {
        let Owner::Recorded(e) = &self.owner else {
            return Err(crate::Error::NotReady);
        };
        if matches!(input, ControlInput::Install { .. }) {
            match e.selection()? {
                Some(install::Selection::Continued(root)) => {
                    let done = tickwire_release::replay::install::Continued::decode(
                        &e.read_object(root)?,
                    )?;
                    let installed = tickwire_release::replay::install::Installation::decode(
                        &e.read_object(done.installation)?,
                    )?;
                    if installed.request != Object::of(&input.bytes()?) {
                        return Err(crate::Error::Identity);
                    }
                    return Ok(Checkpoint::decode(
                        &e.read_object(installed.previous_target)?,
                    )?);
                }
                Some(_) => return Err(crate::Error::NotReady),
                None => {}
            }
        }
        Ok(e.durable_checkpoint().clone())
    }
    /// The caller has authenticated the exact request and loaded its own
    /// qualified artifact files. Every callback, retained input, selection and
    /// resulting HEAD is synchronized here before returning a completion root.
    pub fn install(
        &mut self,
        input: &ControlInput,
        release: &tickwire_release::MemberRelease<'_>,
        target: crate::restore::Target,
        proof: &tickwire_release::replay::source::VerifiedSourceReplay,
        maximum_levels: usize,
        mut now: impl FnMut() -> Result<u64>,
    ) -> crate::Result<Object> {
        self.control_checkpoint(input)?;
        let Owner::Recorded(e) = &mut self.owner else {
            return Err(crate::Error::NotReady);
        };
        let result = (|| {
            let ControlInput::Install {
                source,
                expected,
                request,
            } = input
            else {
                return Err(Error::Identity);
            };
            request.bytes()?;
            if let Some(install::Selection::Continued(root)) = e.selection()? {
                return Ok(root);
            }
            if source != &e.durable_checkpoint().source
                || expected != e.last_durable_cut()
                || proof.root() != request.source_completion
                || &request.operation != release.operation()
                || request.member != release.target.member
            {
                return Err(Error::Identity);
            }
            let continuation = catchup::plan(
                proof.current(),
                e.anchor(),
                e.recorded(),
                e.durable_checkpoint(),
            )?;
            now()?;
            let mut candidate = e.strategy().prepare_replayed(
                release,
                target,
                proof,
                request.target.clone(),
                request.config.clone(),
            )?;
            candidate.configure_views(self.maximum_age, maximum_levels)?;
            if candidate.checkpoint().runtime.markets.len() != 1
                || candidate.checkpoint().runtime.accounts.len() > 1
            {
                return Err(Error::Identity);
            }
            now()?;
            let installed = candidate.persist(e, &input.bytes()?, proof)?;
            continuation.apply(e, &mut now)?;
            if e.durable_checkpoint().runtime.markets[0].stream != self.market {
                return Err(Error::Identity);
            }
            sync_head(e, &self.directory).map_err(|_| Error::Storage)?;
            let completed = e.mark_continued(installed, continuation.inputs(), now()?)?;
            self.account = e
                .durable_checkpoint()
                .runtime
                .accounts
                .first()
                .map(|c| c.stream.clone());
            Ok(completed)
        })();
        if result.is_err() {
            e.stop();
        }
        Ok(result?)
    }
    pub fn next_deadline(&self) -> Option<u64> {
        match &self.owner {
            Owner::Recorded(e) => e.next_deadline(),
            _ => None,
        }
    }
    fn outputs(
        e: &mut DurableEngine<Application<S>, FileBlobStore>,
        directory: &std::path::Path,
    ) -> crate::Result<Vec<CanonicalCommand>> {
        let mut out = vec![];
        e.flush()?.publish(|c| {
            out.push(c.clone());
            Ok(())
        })?;
        sync_head(e, directory)?;
        Ok(out)
    }
    pub fn market(
        &mut self,
        record: &[u8],
        payload: &[u8],
        now: u64,
    ) -> crate::Result<Vec<CanonicalCommand>> {
        let h = RecordHeader::decode(record.get(..128).ok_or(crate::Error::Bounds)?)?;
        match &mut self.owner {
            Owner::Legacy {
                strategy,
                replica,
                maximum_age,
            } => {
                let id = replica.apply(record, payload)?;
                let view = replica.view(id).ok_or(crate::Error::Identity)?;
                if !view.live()
                    || now
                        .checked_sub(view.received_mono_ns())
                        .is_none_or(|age| age > *maximum_age)
                {
                    return Ok(vec![]);
                }
                Ok(strategy.on_market(view, now)?.into_iter().collect())
            }
            Owner::Recorded(e) => {
                e.apply(
                    Input::MarketApplied {
                        stream: self.market.clone(),
                        sequence: h.sequence,
                        received_ns: h.ingress_mono_ns,
                        record: record.to_vec(),
                        payload: payload.to_vec(),
                    },
                    now,
                )?;
                let view = e
                    .strategy()
                    .market_view(&self.market, h.instrument_id)
                    .ok_or(crate::Error::Identity)?;
                let invoked = view.live()
                    && now
                        .checked_sub(view.received_mono_ns())
                        .is_some_and(|age| age <= self.maximum_age);
                e.apply(
                    Input::MarketCallback {
                        stream: self.market.clone(),
                        instrument: h.instrument_id,
                        invoked,
                        maximum_age_ns: self.maximum_age,
                    },
                    now,
                )?;
                Self::outputs(e, &self.directory)
            }
        }
    }
    pub fn report(
        &mut self,
        report: &AccountReport,
        now: u64,
    ) -> crate::Result<Vec<CanonicalCommand>> {
        match &mut self.owner {
            Owner::Legacy { strategy, .. } => {
                Ok(strategy.on_report(report, now)?.into_iter().collect())
            }
            Owner::Recorded(e) => {
                let (position, bytes) = match report {
                    AccountReport::Baseline(b) => {
                        if self.account.is_some() {
                            return Err(crate::Error::Identity);
                        }
                        self.account = Some(account::AccountReplica::stream(b));
                        (0, b.bytes())
                    }
                    AccountReport::Record { position, record } => (*position, record.bytes()),
                };
                e.apply(
                    Input::AccountCallback {
                        stream: self.account.clone().ok_or(crate::Error::Identity)?,
                        position,
                        canonical: bytes.to_vec(),
                    },
                    now,
                )?;
                Self::outputs(e, &self.directory)
            }
        }
    }
    pub fn timers(&mut self, now: u64, budget: usize) -> crate::Result<Vec<CanonicalCommand>> {
        match &mut self.owner {
            Owner::Recorded(e) => {
                if e.next_deadline().is_none_or(|at| at > now) {
                    return Ok(vec![]);
                }
                e.poll_timers(now, budget)?;
                Self::outputs(e, &self.directory)
            }
            _ => Ok(vec![]),
        }
    }
    /// Used only after an authenticated control request has bound the exact
    /// immutable Input object and its predecessor cut; time is sampled locally.
    pub fn capture(&mut self, request: &ControlInput) -> crate::Result<Object> {
        let Owner::Recorded(e) = &mut self.owner else {
            return Err(crate::Error::NotReady);
        };
        Ok(e.capture(request)?)
    }
    pub fn store_object(&mut self, bytes: &[u8]) -> crate::Result<Object> {
        let Owner::Recorded(e) = &mut self.owner else {
            return Err(crate::Error::NotReady);
        };
        Ok(e.store_object(bytes)?)
    }
    pub fn installation_objects(
        &self,
        root: Object,
    ) -> crate::Result<tickwire_release::replay::install::Verified> {
        use tickwire_recovery::BlobStore;
        let Owner::Recorded(e) = &self.owner else {
            return Err(crate::Error::NotReady);
        };
        let verified = tickwire_release::replay::install::verify(root.digest, |hash| {
            e.store
                .get(
                    tickwire_recovery::StoreKey::Object(hash),
                    tickwire_recovery::MAX_OBJECT_BYTES,
                )
                .map_err(|_| tickwire_release::Error::Missing)
        })?;
        root.verify(&verified.continued.bytes()?, MAX_BYTES as u64)?;
        Ok(verified)
    }
    pub fn auxiliary(&mut self, input: Input, now: u64) -> crate::Result<Vec<CanonicalCommand>> {
        if !matches!(
            input,
            Input::ConfigApply { .. }
                | Input::ModelApply { .. }
                | Input::External { .. }
                | Input::Entropy { .. }
        ) {
            return Err(crate::Error::Identity);
        }
        let Owner::Recorded(e) = &mut self.owner else {
            return Err(crate::Error::NotReady);
        };
        e.apply(input, now)?;
        Self::outputs(e, &self.directory)
    }
}
pub(crate) fn executable_digest() -> crate::Result<Hash> {
    use std::io::Read;
    let path = std::env::current_exe()?;
    let mut file = std::fs::File::open(path)?;
    let before = file.metadata()?;
    if !before.is_file() || before.len() > tickwire_release::MAX_BINARY {
        return Err(crate::Error::Bounds);
    }
    let mut hash = Sha256::new();
    let mut count = 0u64;
    let mut buf = [0; 65536];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        count = count
            .checked_add(n as u64)
            .filter(|v| *v <= before.len())
            .ok_or(crate::Error::Bounds)?;
        hash.update(&buf[..n]);
    }
    if count != before.len() || file.metadata()?.modified()? != before.modified()? {
        return Err(crate::Error::Identity);
    }
    Ok(hash.finalize().into())
}

fn sync_head<S: Strategy>(
    engine: &mut DurableEngine<Application<S>, FileBlobStore>,
    root: &std::path::Path,
) -> crate::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let result = (|| {
        let bytes = engine.head()?.bytes()?;
        let temporary = root.join("HEAD.pending");
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(temporary, root.join("HEAD.json"))?;
        std::fs::File::open(root)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        engine.stop();
    }
    result
}

#[cfg(test)]
mod tests;
