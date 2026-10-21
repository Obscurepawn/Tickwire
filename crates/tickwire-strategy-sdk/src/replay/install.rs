//! Target candidate construction. Migration and configuration application have
//! no intent writer; the running worker is replaced only after durable prepare.
use super::*;
use crate::restore::{RestoreFactory, Target};
use tickwire_release::{MemberRelease, replay::source::VerifiedSourceReplay};
pub struct Candidate<S: crate::Strategy> {
    application: application::Application<S>,
    checkpoint: Checkpoint,
    source_proof: Object,
    migrated: Object,
    migrated_state: Vec<u8>,
    config_apply: Vec<u8>,
}
impl<S: crate::Strategy> Candidate<S> {
    pub(crate) fn configure_views(
        &mut self,
        maximum_age: u64,
        maximum_levels: usize,
    ) -> Result<()> {
        self.application
            .configure_views(maximum_age, maximum_levels)?;
        self.checkpoint = Checkpoint::new(
            self.checkpoint.source.clone(),
            0,
            [0; 32],
            self.application.checkpoint()?,
            self.checkpoint.runtime.clone(),
        )?;
        Ok(())
    }
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
    pub fn source_proof(&self) -> Object {
        self.source_proof
    }
    pub fn migrated(&self) -> Object {
        self.migrated
    }
    pub fn config_apply(&self) -> &[u8] {
        &self.config_apply
    }
    pub fn persist<B: tickwire_recovery::BlobStore>(
        self,
        owner: &mut durable::DurableEngine<application::Application<S>, B>,
        request: &[u8],
        proof: &VerifiedSourceReplay,
    ) -> Result<Object> {
        if self.source_proof != proof.root() {
            return Err(Error::Identity);
        }
        owner.install_candidate(self, request, proof)
    }
    pub fn into_parts(self) -> (application::Application<S>, Checkpoint) {
        (self.application, self.checkpoint)
    }
}
pub fn prepare<F: RestoreFactory>(
    factory: &F,
    release: &MemberRelease<'_>,
    target: Target,
    proof: &VerifiedSourceReplay,
    source: Source,
    config: VersionedBytes,
) -> Result<Candidate<F::Running>> {
    let member = &release.target.member;
    let binary = release
        .target
        .files
        .iter()
        .find(|f| f.role == tickwire_release::Role::Binary)
        .ok_or(Error::Identity)?
        .object
        .digest;
    if source.logical_strategy != release.logical_strategy()
        || source.instance != member.new_instance
        || source.generation != member.new_generation
        || source.binary_digest != binary
        || source.state_schema != release.target.capabilities.state_schema
        || source.input_stream == proof.current().source.input_stream
        || source.clock != proof.current().source.clock
        || proof.completion().operation != *release.operation()
        || factory
            .configuration(&target)
            .map_err(|_| Error::Unsupported)?
            != config
    {
        return Err(Error::Identity);
    }
    let input = crate::restore::VerifiedRestoreInput::from_replayed(release, target, proof)
        .map_err(|_| Error::Identity)?;
    let restored = crate::restore::execute(factory, &input).map_err(|_| Error::Unsupported)?;
    let migrated_state = restored.checkpoint().to_vec();
    let migrated = Object::of(&migrated_state);
    let mut application = application::Application::with_migrated_strategy(
        &proof.current().business,
        restored.into_strategy(),
    )?;
    let mut runtime = proof.current().runtime.clone();
    let callback = Input::ConfigApply {
        previous: Object::of(&runtime.config.bytes),
        next: config.clone(),
    };
    // This is a new input stream boundary. Configuration revisions are scoped
    // to the fresh target, so old and new revisions need not be comparable.
    // The old runtime time stays C; installation wall time is recorded later.
    runtime.config = config;
    runtime.validate()?;
    application.apply(
        &callback,
        &mut ReplayContext {
            views: None,
            runtime: &mut runtime,
        },
        &mut ReplayEffects {
            allowed: false,
            commands: vec![],
        },
    )?;
    factory
        .validate_checkpoint(&application.business_state()?)
        .map_err(|_| Error::StateMismatch)?;
    let checkpoint = Checkpoint::new(source, 0, [0; 32], application.checkpoint()?, runtime)?;
    Ok(Candidate {
        application,
        checkpoint,
        source_proof: proof.root(),
        migrated,
        migrated_state,
        config_apply: canonical(&callback)?,
    })
}
#[cfg(test)]
pub(super) mod tests;

/// A durable selection record is separate from an active, authenticated READY.
/// On a new process, even an Installed record is only restoration input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selection {
    Prepared(Object),
    Installed(Object),
    Continued(Object),
}
fn continued_key(source: &Source) -> Result<tickwire_recovery::StoreKey> {
    Ok(tickwire_recovery::StoreKey::Binding(
        tickwire_recovery::digest(&canonical(&(
            "tickwire.target-continued.v1",
            source.logical_strategy,
            source.instance,
            source.generation,
            source.binary_digest,
            source.state_schema,
            &source.clock,
        ))?),
    ))
}
fn selection_key(source: &Source, installed: bool) -> Result<tickwire_recovery::StoreKey> {
    let key = canonical(&(
        "tickwire.target-selection.v1",
        source.logical_strategy,
        source.instance,
        source.generation,
        source.binary_digest,
        source.state_schema,
        &source.clock,
        installed,
    ))?;
    Ok(tickwire_recovery::StoreKey::Binding(
        tickwire_recovery::digest(&key),
    ))
}
impl<S: crate::Strategy, B: tickwire_recovery::BlobStore>
    durable::DurableEngine<application::Application<S>, B>
{
    pub fn selection(&self) -> Result<Option<Selection>> {
        match self.store.get(continued_key(&self.durable.source)?, 32) {
            Ok(raw) => {
                let hash: Hash = raw.try_into().map_err(|_| Error::Storage)?;
                let raw = self
                    .store
                    .get(tickwire_recovery::StoreKey::Object(hash), MAX_BYTES)
                    .map_err(|_| Error::Storage)?;
                let root = Object::of(&raw);
                if root.digest != hash {
                    return Err(Error::Storage);
                }
                let completed = tickwire_release::replay::install::Continued::decode(&raw)?;
                let installed = tickwire_release::replay::install::Installation::decode(
                    &self.read_object(completed.installation)?,
                )?;
                if installed.target.instance != self.durable.source.instance
                    || installed.target.generation != self.durable.source.generation
                {
                    return Err(Error::Identity);
                }
                return Ok(Some(Selection::Continued(root)));
            }
            Err(tickwire_recovery::Error::Missing) => {}
            Err(_) => return Err(Error::Storage),
        }
        for installed in [true, false] {
            let key = selection_key(&self.durable.source, installed)?;
            match self.store.get(key, 32) {
                Ok(bytes) => {
                    let h: Hash = bytes.try_into().map_err(|_| Error::Storage)?;
                    let bytes = self
                        .store
                        .get(tickwire_recovery::StoreKey::Object(h), MAX_BYTES)
                        .map_err(|_| Error::Storage)?;
                    let object = Object::of(&bytes);
                    if object.digest != h {
                        return Err(Error::Storage);
                    }
                    let record = tickwire_release::replay::install::Installation::decode(&bytes)?;
                    if record.target.instance != self.durable.source.instance
                        || record.target.generation != self.durable.source.generation
                    {
                        return Err(Error::Identity);
                    }
                    return Ok(Some(if installed {
                        Selection::Installed(object)
                    } else {
                        Selection::Prepared(object)
                    }));
                }
                Err(tickwire_recovery::Error::Missing) => {}
                Err(_) => return Err(Error::Storage),
            }
        }
        Ok(None)
    }
    pub(crate) fn mark_continued(
        &mut self,
        installed: Object,
        inputs: &[Input],
        completed_ns: u64,
    ) -> Result<Object> {
        let result = (|| {
            if self.stopped
                || !self.pending.is_empty()
                || self.memory_cut() != self.last_durable_cut()
                || self.selection() != Ok(Some(Selection::Installed(installed)))
            {
                return Err(Error::Poisoned);
            }
            let inputs = self.store_object(&canonical(&inputs)?)?;
            let checkpoint = self.store_object(&self.durable.bytes()?)?;
            let record = tickwire_release::replay::install::Continued {
                schema: 1,
                installation: installed,
                inputs,
                checkpoint,
                commit: self.commit,
                completed_ns,
            };
            let object = self.store_object(&record.bytes()?)?;
            self.store
                .put_sync(continued_key(&self.durable.source)?, &object.digest)
                .map_err(|_| Error::Storage)?;
            Ok(object)
        })();
        if result.is_err() {
            self.stop();
        }
        result
    }
    fn install_candidate(
        &mut self,
        candidate: Candidate<S>,
        request: &[u8],
        proof: &VerifiedSourceReplay,
    ) -> Result<Object> {
        let result = (|| {
            if self.stopped
                || !self.pending.is_empty()
                || self.memory_cut() != self.last_durable_cut()
            {
                return Err(Error::Poisoned);
            }
            if self.selection()?.is_some() {
                return Err(Error::Poisoned);
            }
            let old = &self.durable.source;
            let new = &candidate.checkpoint.source;
            if old.instance != new.instance
                || old.generation != new.generation
                || old.logical_strategy != new.logical_strategy
                || old.binary_digest != new.binary_digest
                || old.state_schema != new.state_schema
                || old.clock != new.clock
                || old.input_stream == new.input_stream
                || candidate.checkpoint.cut.sequence != 0
            {
                return Err(Error::Identity);
            }
            let request = self.store_object(request)?;
            for bytes in proof.objects() {
                self.store_object(bytes)?;
            }
            let previous = self.store_object(&self.durable.bytes()?)?;
            let migrated = self.store_object(&candidate.migrated_state)?;
            let config_apply = self.store_object(&candidate.config_apply)?;
            let bytes = candidate.checkpoint.bytes()?;
            let models = candidate
                .checkpoint
                .runtime
                .models
                .iter()
                .map(|m| (m.id, Object::of(&m.value.bytes)))
                .collect::<Vec<_>>();
            let anchor = verify_anchor(
                &bytes,
                &candidate.checkpoint.source,
                Object::of(&candidate.checkpoint.runtime.config.bytes),
                &models,
            )?;
            let anchor_object = self.store_object(&bytes)?;
            let tail = self.store_object(&encode_tail(&anchor, &[])?)?;
            let commit = self.store_object(
                &InputCommit {
                    schema: 2,
                    anchor: anchor_object,
                    tail,
                    checkpoint: anchor_object,
                    previous_commit: None,
                }
                .bytes()?,
            )?;
            let next = Engine::new(&anchor, candidate.application)?;
            let record = tickwire_release::replay::install::Installation {
                schema: 1,
                request,
                source_completion: candidate.source_proof,
                previous_target: previous,
                previous_commit: self.commit,
                target: candidate.checkpoint.source.clone(),
                migrated,
                config_apply,
                checkpoint: anchor_object,
                commit,
            };
            let completed = self.store_object(&record.bytes()?)?;
            // Immutable locators distinguish a fully materialized candidate from
            // the selected target. An uncertain return poisons this owner.
            self.store
                .put_sync(selection_key(&record.target, false)?, &completed.digest)
                .map_err(|_| Error::Storage)?;
            self.store
                .put_sync(selection_key(&record.target, true)?, &completed.digest)
                .map_err(|_| Error::Storage)?;
            self.engine = next;
            self.anchor = anchor;
            self.durable_object = anchor_object;
            self.durable = candidate.checkpoint;
            self.commit = commit;
            Ok(completed)
        })();
        if result.is_err() {
            self.stop();
        }
        result
    }
}
