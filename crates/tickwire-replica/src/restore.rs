//! Management-plane reconstruction. Returned physical writers are closed.
use crate::*;
use tickwire_journal::{Journal, store::Store};
use tickwire_recovery::digest;
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountBootstrap {
    format: String,
    version: u32,
    account_codec_version: u32,
    physical: tickwire_journal::Config,
    baseline_digest: Hash,
    initial_writer_epoch: u64,
    risk_domain_id: [u8; 16],
    venue: u32,
    account: u64,
    environment: u8,
}
impl<T: Store, S: BlobStore> Mirror<T, S> {
    pub fn object(&self, hash: Hash, max: usize) -> Result<Vec<u8>> {
        Ok(self.restore_persistence().read_object(hash, max)?)
    }
    pub fn input_roots(&self, complete: &CompletePrefix) -> Result<Vec<ObjectRef>> {
        self.check_complete(complete)?;
        if let Some(roots) = complete.final_inputs() {
            return Ok(roots.to_vec());
        }
        let mut best = std::collections::BTreeMap::<[u8; 16], (u64, ObjectRef)>::new();
        for entry in self.restore_entries() {
            for root in &entry.extra_objects {
                let bytes = self.object(root.hash, root.bytes as usize)?;
                if !bytes.starts_with(INPUT_PREFIX_MAGIC) {
                    continue;
                }
                let p = InputPrefix::decode(&bytes)?;
                if best
                    .get(&p.input_stream)
                    .is_none_or(|(n, _)| *n < p.through_sequence)
                {
                    best.insert(p.input_stream, (p.through_sequence, root.clone()));
                }
            }
        }
        Ok(best.into_values().map(|(_, root)| root).collect())
    }
    /// Copy the immutable dependency closure to the eventual recovery backend.
    /// Every target object and binding completes its own synchronization.
    pub fn copy_recovery_closure<P: BlobStore>(
        &self,
        complete: &CompletePrefix,
        target: &mut Persistence<P>,
    ) -> Result<()> {
        self.check_complete(complete)?;
        for object in &self.stream().initial_objects {
            self.copy_object_to(object, target)?;
        }
        for entry in self.restore_entries() {
            target.put_replica_entry(self.stream().digest(), entry.sequence, &entry.bytes())?;
            self.copy_object_to(&entry.payload, target)?;
            for object in &entry.extra_objects {
                self.copy_object_to(object, target)?;
            }
            let (objects, _) = crate::input::input_dependencies(
                self.restore_persistence(),
                self.stream(),
                &entry.extra_objects,
            )?;
            for object in &objects {
                self.copy_object_to(object, target)?;
            }
            for object in
                crate::handover::dependencies(self.restore_persistence(), self.stream(), entry)?
            {
                self.copy_object_to(&object, target)?;
            }
            if let Some(reference) = &entry.manifest {
                let manifest = self
                    .restore_persistence()
                    .read_manifest(&reference.binding)?;
                if digest(&manifest.canonical_bytes()?) != reference.root {
                    return Err(Error::Conflict);
                }
                for object in &manifest.objects {
                    self.copy_object_to(
                        &ObjectRef {
                            hash: object.content_hash,
                            bytes: object.byte_length,
                        },
                        target,
                    )?;
                }
                if target.publish(&manifest)?.manifest_root() != reference.root {
                    return Err(Error::Conflict);
                }
            }
        }
        let seal = self
            .restore_persistence()
            .read_object(complete.recovery_seal_hash(), MAX_FRAME_BYTES)?;
        target.put_object(&seal)?;
        // Source recovery can include a final input suffix absent from all account records.
        if let Some(inputs) = complete.final_inputs() {
            for root in inputs {
                self.copy_object_to(root, target)?;
            }
            let (objects, _) = crate::input::input_dependencies(
                self.restore_persistence(),
                self.stream(),
                inputs,
            )?;
            for object in &objects {
                self.copy_object_to(object, target)?;
            }
        }
        Ok(())
    }
    fn copy_object_to<P: BlobStore>(
        &self,
        object: &ObjectRef,
        target: &mut Persistence<P>,
    ) -> Result<()> {
        let bytes = self
            .restore_persistence()
            .read_object(object.hash, object.bytes as usize)?;
        if bytes.len() as u64 != object.bytes || target.put_object(&bytes)? != object.hash {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    /// The baseline and bootstrap are the source's original synchronized bytes.
    /// No account reducer is run here and no execution capability is created.
    pub fn materialize_account<B: tickwire_account_journal::Backend>(
        &self,
        complete: &CompletePrefix,
        bootstrap: &ObjectRef,
        mut backend: B,
    ) -> Result<ClosedJournal<B>> {
        self.check_complete(complete)?;
        let Format::Account { baseline } = self.stream().format else {
            return Err(Error::Invalid("not an account stream"));
        };
        if !self.stream().initial_objects.contains(bootstrap) {
            return Err(Error::Missing);
        }
        let raw = self
            .restore_persistence()
            .read_object(bootstrap.hash, bootstrap.bytes as usize)?;
        let config: AccountBootstrap =
            serde_json::from_slice(&raw).map_err(|_| Error::Invalid("account bootstrap"))?;
        let baseline_bytes = self
            .restore_persistence()
            .read_object(baseline, MAX_OBJECT_BYTES)?;
        let baseline = tickwire_account_codec::CanonicalBaseline::decode(&baseline_bytes)
            .map_err(|_| Error::Invalid("baseline"))?;
        let scope = &baseline.snapshot().scope;
        let environment = match scope.environment {
            tickwire_venue::Environment::Production => 1,
            tickwire_venue::Environment::Test => 2,
        };
        if config.format != "tickwire.account-journal"
            || config.version != 1
            || config.account_codec_version != 1
            || config.physical.domain != self.stream().domain
            || config.physical.stream_epoch != self.stream().journal_epoch
            || config.baseline_digest != baseline.digest()
            || config.initial_writer_epoch == 0
            || config.initial_writer_epoch > self.stream().source.exec_epoch
            || config.risk_domain_id != scope.risk_domain_id
            || config.venue != scope.venue
            || config.account != scope.account
            || config.environment != environment
        {
            return Err(Error::Conflict);
        }
        if backend.read_bootstrap()?.is_some()
            || backend.read_baseline()?.is_some()
            || backend.read_manifest()?.is_some()
            || !backend.segment_ids()?.is_empty()
        {
            return Err(Error::Conflict);
        }
        backend.publish_baseline(&baseline_bytes)?;
        backend.publish_bootstrap(&raw)?;
        let mut journal = Journal::create(backend, config.physical)?;
        for entry in self.restore_entries() {
            let bytes = self
                .restore_persistence()
                .read_object(entry.payload.hash, entry.payload.bytes as usize)?;
            let sync = journal.append_batch(entry.writer_epoch, &[&bytes])?;
            if sync.frontier().last_seq != entry.sequence {
                return Err(Error::Conflict);
            }
        }
        Ok(journal.close_for_recovery()?)
    }
}
