//! Real durable exports and a bounded background mTLS replication worker.
use crate::{Result, config::read_file, invalid};
use serde::Deserialize;
use std::{
    cell::RefCell, collections::BTreeMap, net::SocketAddr, path::PathBuf, rc::Rc,
    sync::mpsc::TrySendError,
};
use tickwire_account::RecoveryRecord;
use tickwire_account_codec::CanonicalRecord;
use tickwire_account_gateway::{AuthorityError, CommitState};
use tickwire_clock::ClockDomain;
use tickwire_recovery::{FileBlobStore, Persistence, SharedBlobStore};
use tickwire_replica::{self as replica, ObjectRef, WorkerEvent};
use tickwire_runtime::{
    authority::CommitPolicy,
    files::{Services, SharedRecoveryStore},
};

#[derive(Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum Profile {
    LocalDurableAsync {
        maximum_unreplicated_age_ns: u64,
    },
    SyncReplica {
        mirror_id: [u8; 16],
        assignment_epoch: u64,
    },
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub stream_file: PathBuf,
    pub mirror: SocketAddr,
    pub mirror_server_name: String,
    pub tls: replica::config::TlsFiles,
    pub signing_key_pkcs8: PathBuf,
    pub mirror_signing_public_key: [u8; 32],
    pub profile_version: u64,
    pub profile: Profile,
    pub maximum_records: usize,
    pub queue_capacity: usize,
    pub maximum_pending_bytes: usize,
    pub network: replica::config::NetworkConfig,
}
struct Barrier {
    ready: BTreeMap<u64, ([u8; 32], replica::CommitEvidence)>,
    failed: bool,
    worker: Option<Rc<replica::WorkerHandle>>,
}
struct Gate(Rc<RefCell<Barrier>>, tickwire_binance_clock::SystemClock);
impl CommitPolicy for Gate {
    fn state(
        &mut self,
        _admission: &tickwire_account::Admission,
        receipt: &tickwire_account_journal::AppendReceipt,
    ) -> std::result::Result<CommitState, AuthorityError> {
        let b = self
            .0
            .try_borrow()
            .map_err(|_| AuthorityError::Unavailable)?;
        if b.failed {
            return Err(AuthorityError::Denied);
        }
        let Some(worker) = &b.worker else {
            return Ok(CommitState::Pending);
        };
        let now = self
            .1
            .sample()
            .map_err(|_| AuthorityError::Unavailable)?
            .monotonic
            .nanos();
        if !worker.healthy(now) {
            return Err(AuthorityError::Denied);
        }
        let Some((digest, evidence)) = b.ready.get(&receipt.sequence()) else {
            return Ok(CommitState::Pending);
        };
        let physical = evidence.local().physical();
        if *digest != receipt.record_digest()
            || evidence.local().prefix().sequence < receipt.sequence()
            || physical.domain != receipt.physical_frontier().domain
            || physical.stream_epoch != receipt.physical_frontier().stream_epoch
        {
            return Err(AuthorityError::Denied);
        }
        Ok(CommitState::Ready)
    }
}
struct Submitted {
    end: u64,
    physical: tickwire_journal::PhysicalFrontier,
    records: Vec<(u64, [u8; 32])>,
}
struct InputEntry {
    sequence: u64,
    writer: u64,
    object: ObjectRef,
}
pub struct Driver {
    source: Option<replica::SourceWorker<SharedRecoveryStore>>,
    sender: Option<replica::SourceSender>,
    queue_capacity: usize,
    store: Persistence<SharedRecoveryStore>,
    barrier: Rc<RefCell<Barrier>>,
    pending: Option<replica::ExportWork>,
    submitted: BTreeMap<u64, Submitted>,
    next_account: u64,
    next_request: u64,
    input_entries: Vec<InputEntry>,
    input_root: Option<ObjectRef>,
    domain: ClockDomain,
    maximum_records: usize,
}
impl Driver {
    pub fn open(
        config: &Config,
        deployment: &crate::config::Deployment,
        baseline: &tickwire_account_codec::CanonicalBaseline,
    ) -> Result<(Self, Services)> {
        let stream = replica::Stream::decode(&read_file(&config.stream_file, 65536)?)?;
        let domain = deployment.domain()?;
        if stream.domain != baseline.snapshot().scope.risk_domain_id
            || stream.journal_epoch != deployment.account_stream
            || stream.source.exec_instance_id != deployment.owner_id
            || stream.source.exec_epoch != deployment.writer_epoch
            || stream.source.boot_id != deployment.boot_id
            || stream.format
                != (replica::Format::Account {
                    baseline: baseline.digest(),
                })
            || config.maximum_records == 0
            || config.maximum_records > tickwire_account_journal::MAX_RECORDS
        {
            return Err(invalid("replication source identity"));
        }
        if let Some(parent) = deployment.directories.recovery.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let shared = SharedBlobStore::new(FileBlobStore::open(&deployment.directories.recovery)?);
        let mut store = Persistence::new(shared.clone());
        if store.put_object(baseline.bytes())? != baseline.digest() {
            return Err(invalid("replication baseline"));
        }
        let bootstrap = tickwire_account_journal::bootstrap_bytes(
            &deployment.assemble(baseline.clone())?.account.journal,
        )?;
        let bootstrap_object = ObjectRef::new(&bootstrap)?;
        if !stream.initial_objects.contains(&bootstrap_object) {
            return Err(invalid("replication stream must bind account bootstrap"));
        }
        store.put_object(&bootstrap)?;
        let mode = match config.profile {
            Profile::LocalDurableAsync {
                maximum_unreplicated_age_ns,
            } => replica::CommitMode::LocalDurableAsync {
                max_unreplicated_age_ns: maximum_unreplicated_age_ns,
            },
            Profile::SyncReplica {
                mirror_id,
                assignment_epoch,
            } => replica::CommitMode::SyncReplica {
                mirror_id,
                assignment_epoch,
            },
        };
        let source = replica::SourceWorker::new(
            replica::SourceConfig {
                stream: stream.clone(),
                profile_version: config.profile_version,
                mode,
                max_records: config.maximum_records,
                max_pending_batches: config.queue_capacity,
                max_pending_bytes: config.maximum_pending_bytes,
            },
            Persistence::new(shared.clone()),
        )?;
        let peer = replica::PeerClient::new(
            config.mirror,
            &stream,
            replica::Signer::from_pkcs8(&read_file(&config.signing_key_pkcs8, 65536)?)?,
            config.mirror_signing_public_key,
            config.tls.client(config.mirror_server_name.clone())?,
            config.network.build()?,
        )?;
        let barrier = Rc::new(RefCell::new(Barrier {
            ready: BTreeMap::new(),
            failed: false,
            worker: None,
        }));
        let gate_clock = tickwire_binance_clock::SystemClock::new(domain)
            .map_err(|_| invalid("replication clock"))?;
        let services = Services {
            recovery_store: Some(shared),
            commit_policy: Some(Box::new(Gate(barrier.clone(), gate_clock))),
        };
        Ok((
            Self {
                source: Some(source),
                sender: Some(replica::SourceSender::new(peer)),
                queue_capacity: config.queue_capacity,
                store,
                barrier,
                pending: None,
                submitted: BTreeMap::new(),
                next_account: 1,
                next_request: 1,
                input_entries: Vec::new(),
                input_root: None,
                domain,
                maximum_records: config.maximum_records,
            },
            services,
        ))
    }
    /// Startup recovery precedes private/network polling and new submissions.
    pub fn start<S, B, A, E>(
        &mut self,
        rest: &mut tickwire_binance_account::rest::RestOwner<S, B, A, E>,
        now: tickwire_clock::MonoTime,
    ) -> Result<()>
    where
        S: tickwire_journal::store::Store,
        B: tickwire_account_journal::Backend,
        A: tickwire_account_gateway::Authority,
        E: tickwire_account_gateway::Encoder,
    {
        if now.domain() != self.domain || self.barrier.borrow().worker.is_some() {
            return Err(invalid("replication startup state"));
        }
        let mut source = self
            .source
            .take()
            .ok_or_else(|| invalid("replication startup source"))?;
        while self.next_account <= rest.gateway().journal_position() {
            if let Some(work) = self.export_batch(rest)? {
                source.restore_prefix(&work.export, &work.extra_objects, now.nanos())?;
                self.submitted.remove(&work.request_id);
            }
        }
        let sender = self
            .sender
            .take()
            .ok_or_else(|| invalid("replication startup sender"))?;
        let mut clock = tickwire_binance_clock::SystemClock::new(self.domain)
            .map_err(|_| invalid("replication clock"))?;
        let worker =
            replica::spawn_source_worker(source, sender, self.queue_capacity, move || {
                clock.sample().map_or(0, |t| t.monotonic.nanos())
            })?;
        self.barrier.borrow_mut().worker = Some(Rc::new(worker));
        Ok(())
    }
    pub fn drive<S, B, A, E>(
        &mut self,
        rest: &mut tickwire_binance_account::rest::RestOwner<S, B, A, E>,
        now: tickwire_clock::MonoTime,
    ) -> Result<()>
    where
        S: tickwire_journal::store::Store,
        B: tickwire_account_journal::Backend,
        A: tickwire_account_gateway::Authority,
        E: tickwire_account_gateway::Encoder,
    {
        if now.domain() != self.domain {
            return Err(invalid("replication clock domain"));
        }
        let worker = self
            .barrier
            .borrow()
            .worker
            .clone()
            .ok_or_else(|| invalid("replication not started"))?;
        if !worker.healthy(now.nanos()) {
            self.barrier.borrow_mut().failed = true;
            return Err(invalid("replication lag or worker failure"));
        }
        while let Some(event) = worker.try_event()? {
            match event {
                WorkerEvent::Committed {
                    request_id,
                    evidence,
                } => {
                    let work = self
                        .submitted
                        .remove(&request_id)
                        .ok_or_else(|| invalid("unknown replication acknowledgement"))?;
                    if work.end != evidence.local().prefix().sequence
                        || &work.physical != evidence.local().physical()
                    {
                        self.barrier.borrow_mut().failed = true;
                        return Err(invalid("replication acknowledgement prefix"));
                    }
                    let mut state = self.barrier.borrow_mut();
                    for (sequence, digest) in work.records {
                        state.ready.insert(sequence, (digest, evidence.clone()));
                    }
                }
                WorkerEvent::Rejected { error, .. } => {
                    self.barrier.borrow_mut().failed = true;
                    return Err(invalid(&format!(
                        "replication rejected durable source: {error}"
                    )));
                }
                WorkerEvent::Frozen => {
                    self.barrier.borrow_mut().failed = true;
                    return Err(invalid("replication worker frozen"));
                }
                WorkerEvent::NetworkUnavailable | WorkerEvent::NetworkRestored => {}
                WorkerEvent::Replicated { .. } => {
                    return Err(invalid("opaque stream cannot authorize account sends"));
                }
            }
        }
        if let Some(work) = self.pending.take() {
            match worker.try_export(work) {
                Ok(()) => {}
                Err(TrySendError::Full(work)) => {
                    self.pending = Some(work);
                    return Ok(());
                }
                Err(TrySendError::Disconnected(_)) => {
                    self.barrier.borrow_mut().failed = true;
                    return Err(invalid("replication worker disconnected"));
                }
            }
        }
        if self.submitted.len() >= 128 {
            return Ok(());
        }
        if let Some(work) = self.export_batch(rest)? {
            match worker.try_export(work) {
                Ok(()) => {}
                Err(TrySendError::Full(work)) => self.pending = Some(work),
                Err(TrySendError::Disconnected(_)) => {
                    self.barrier.borrow_mut().failed = true;
                    return Err(invalid("replication worker disconnected"));
                }
            }
        }
        Ok(())
    }
    fn export_batch<S, B, A, E>(
        &mut self,
        rest: &mut tickwire_binance_account::rest::RestOwner<S, B, A, E>,
    ) -> Result<Option<replica::ExportWork>>
    where
        S: tickwire_journal::store::Store,
        B: tickwire_account_journal::Backend,
        A: tickwire_account_gateway::Authority,
        E: tickwire_account_gateway::Encoder,
    {
        if self.next_account > rest.gateway().journal_position() {
            return Ok(None);
        }
        // Read one bounded batch of the actual input WAL per loop iteration.
        let next_input = self.input_entries.len() as u64 + 1;
        if next_input <= rest.ingress().position() {
            let export = rest.with_ingress(|i| i.export_durable(next_input, 32, 64 * 1024))?;
            for record in export.records() {
                if record.seq != self.input_entries.len() as u64 + 1
                    || self.input_entries.len() >= 16384
                {
                    return Err(invalid("input replication prefix"));
                }
                let object = ObjectRef::new(&record.payload)?;
                self.store.put_object(&record.payload)?;
                self.input_entries.push(InputEntry {
                    sequence: record.seq,
                    writer: record.exec_epoch,
                    object,
                });
            }
            if self.input_entries.len() as u64 != rest.ingress().position() {
                return Ok(None);
            }
            let mut bytes = b"TWIPFX01".to_vec();
            bytes.extend(rest.ingress().input_stream());
            bytes.extend(rest.gateway().journal_config().physical.stream_epoch);
            bytes.extend(rest.gateway().baseline().digest());
            bytes.extend((self.input_entries.len() as u64).to_le_bytes());
            bytes.extend((self.input_entries.len() as u32).to_le_bytes());
            for e in &self.input_entries {
                bytes.extend(e.sequence.to_le_bytes());
                bytes.extend(e.writer.to_le_bytes());
                bytes.extend(e.object.bytes.to_le_bytes());
                bytes.extend(e.object.hash);
            }
            self.store.put_object(&bytes)?;
            self.input_root = Some(ObjectRef::new(&bytes)?);
        }
        let export = rest.with_gateway(|g| g.export_durable(self.next_account, 32, 64 * 1024))?;
        let mut extras = BTreeMap::new();
        let mut records = Vec::new();
        for raw in export.records() {
            let record = CanonicalRecord::decode(&raw.payload)
                .map_err(|_| invalid("replication account record"))?;
            let mut objects = self.input_root.clone().into_iter().collect::<Vec<_>>();
            if let RecoveryRecord::IncomeBaseline(binding) = record.record() {
                let bytes = rest.ingress().income_coverage_object(binding)?;
                self.store.put_object(&bytes)?;
                objects.push(ObjectRef::new(&bytes)?);
            }
            if let RecoveryRecord::Reactivated(binding) = record.record() {
                let bytes = rest.ingress().activation_coverage_object(binding)?;
                self.store.put_object(&bytes)?;
                objects.push(ObjectRef::new(&bytes)?);
            }
            if let RecoveryRecord::StrategyHandover(binding) = record.record() {
                for hash in [binding.checkpoint_digest, binding.input_seal_digest] {
                    let bytes = self
                        .store
                        .read_object(hash, tickwire_recovery::MAX_OBJECT_BYTES)?;
                    objects.push(ObjectRef::new(&bytes)?);
                }
            }
            objects.sort_by_key(|o| o.hash);
            objects.dedup_by_key(|o| o.hash);
            extras.insert(raw.seq, objects);
            records.push((raw.seq, record.digest()));
        }
        let end = records
            .last()
            .ok_or_else(|| invalid("empty replication export"))?
            .0;
        if end > self.maximum_records as u64 {
            return Err(invalid("replication record limit"));
        }
        let request = self.next_request;
        self.next_request = request
            .checked_add(1)
            .ok_or_else(|| invalid("replication request overflow"))?;
        self.next_account = end
            .checked_add(1)
            .ok_or_else(|| invalid("replication sequence overflow"))?;
        self.submitted.insert(
            request,
            Submitted {
                end,
                physical: export
                    .frontier()
                    .ok_or_else(|| invalid("missing durable frontier"))?
                    .clone(),
                records,
            },
        );
        Ok(Some(replica::ExportWork {
            request_id: request,
            export,
            extra_objects: extras,
        }))
    }
}
