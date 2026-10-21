//! Offline fenced-source sealing and frozen reconstruction from a complete mirror.
use crate::{
    Result,
    config::{Deployment, read_file},
    invalid,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use tickwire_account::{Account, RecoveryRecord};
use tickwire_account_codec::{CanonicalBaseline, CanonicalRecord};
use tickwire_account_gateway::ReactivationVerifier;
use tickwire_account_journal::{
    AccountJournal, Backend, FileBackend, HistoryFrontier, RecoveryVerifier, VerificationError,
};
use tickwire_binance_account::AccountIngress;
use tickwire_clock::ClockDomain;
use tickwire_fencing::{Scope, SignedDocument, VerifiedFence, Verifier};
use tickwire_journal::{Journal, PhysicalFrontier, store::FileStore};
use tickwire_recovery::{ClosureBinding, FileBlobStore, Persistence, SharedBlobStore};
use tickwire_replica::{self as replica, Hash, ObjectRef};

const STARTED_FILE: &str = "FAILOVER-IN-PROGRESS";
const COMPLETE_FILE: &str = "FAILOVER-COMPLETE.json";
const COMPLETION_LIMIT: usize = 256 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetBinding {
    risk_domain: [u8; 16],
    venue: u32,
    account: u64,
    environment: u8,
    product: u8,
    baseline: Hash,
    account_bootstrap: Hash,
    account_stream: [u8; 16],
    input_stream: [u8; 16],
    owner: [u8; 16],
    boot: [u8; 16],
    clock_generation: u64,
    writer_epoch: u64,
    initial_writer_epoch: u64,
    account_directory: PathBuf,
    input_directory: PathBuf,
    recovery_directory: PathBuf,
}
impl TargetBinding {
    fn new(d: &Deployment, baseline: &CanonicalBaseline) -> Result<Self> {
        let configured = d.assemble(baseline.clone())?;
        let bootstrap = tickwire_account_journal::bootstrap_bytes(&configured.account.journal)?;
        let scope = &baseline.snapshot().scope;
        Ok(Self {
            risk_domain: scope.risk_domain_id,
            venue: scope.venue,
            account: scope.account,
            environment: match scope.environment {
                tickwire_venue::Environment::Production => 1,
                tickwire_venue::Environment::Test => 2,
            },
            product: match d.product.product() {
                tickwire_venue::Product::Spot => 1,
                tickwire_venue::Product::LinearPerpetual => 2,
                tickwire_venue::Product::LinearFuture => 3,
                _ => return Err(invalid("unsupported failover product")),
            },
            baseline: baseline.digest(),
            account_bootstrap: tickwire_recovery::digest(&bootstrap),
            account_stream: d.account_stream,
            input_stream: d.input_stream,
            owner: d.owner_id,
            boot: d.boot_id,
            clock_generation: d.clock_generation,
            writer_epoch: d.writer_epoch,
            initial_writer_epoch: d.initial_writer_epoch,
            account_directory: d.directories.account.clone(),
            input_directory: d.directories.input.clone(),
            recovery_directory: d.directories.recovery.clone(),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreStarted {
    format: String,
    target: TargetBinding,
    source_stream: Vec<u8>,
    complete_prefix: replica::Prefix,
    recovery_seal_sha256: Hash,
    fence_sha256: Hash,
    final_input_root: Hash,
    final_input_bytes: u64,
    final_input_sequence: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Frontier {
    stream_epoch: [u8; 16],
    domain: [u8; 16],
    segment_id: u64,
    end_offset: u64,
    end_lsn: u64,
    last_seq: u64,
    exec_epoch: u64,
    batch_number: u64,
    seal_hash: Hash,
}
impl From<PhysicalFrontier> for Frontier {
    fn from(f: PhysicalFrontier) -> Self {
        Self {
            stream_epoch: f.stream_epoch,
            domain: f.domain,
            segment_id: f.segment_id,
            end_offset: f.end_offset,
            end_lsn: f.end_lsn,
            last_seq: f.last_seq,
            exec_epoch: f.exec_epoch,
            batch_number: f.batch_number,
            seal_hash: f.seal_hash,
        }
    }
}
impl Frontier {
    fn required(
        &self,
        config: &tickwire_journal::Config,
        expected: u64,
        writer: u64,
    ) -> Result<PhysicalFrontier> {
        if self.domain != config.domain
            || self.stream_epoch != config.stream_epoch
            || self.last_seq != expected
            || self.last_seq == 0
            || self.segment_id == 0
            || self.end_offset == 0
            || self.end_offset > config.segment_max_bytes
            || !self.end_offset.is_multiple_of(8)
            || self.end_lsn == 0
            || self.exec_epoch == 0
            || self.exec_epoch > writer
            || self.batch_number == 0
            || self.seal_hash == [0; 32]
        {
            return Err(invalid("failover physical frontier"));
        }
        Ok(PhysicalFrontier {
            stream_epoch: self.stream_epoch,
            domain: self.domain,
            segment_id: self.segment_id,
            end_offset: self.end_offset,
            end_lsn: self.end_lsn,
            last_seq: self.last_seq,
            exec_epoch: self.exec_epoch,
            batch_number: self.batch_number,
            seal_hash: self.seal_hash,
        })
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreComplete {
    format: String,
    started_sha256: Hash,
    started: RestoreStarted,
    account_frontier: Option<Frontier>,
    input_frontier: Frontier,
    ledger_sequence: u64,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FenceAuthority {
    pub controller_id: [u8; 16],
    pub fence_public_key: Hash,
    pub emergency_public_key: Hash,
    pub document: PathBuf,
    pub clock_boot_id: [u8; 16],
    pub clock_generation: u64,
}
impl FenceAuthority {
    fn clock(&self) -> Result<tickwire_binance_clock::SystemClock> {
        let domain = ClockDomain::new(self.clock_boot_id, self.clock_generation)
            .map_err(|_| invalid("fence clock domain"))?;
        tickwire_binance_clock::SystemClock::new(domain).map_err(|_| invalid("fence system clock"))
    }
    fn verify(
        &self,
        stream: &replica::Stream,
        baseline: &CanonicalBaseline,
        product: tickwire_venue::Product,
        clock: &mut tickwire_binance_clock::SystemClock,
    ) -> Result<VerifiedFence> {
        let scope = Scope {
            risk_domain: baseline.snapshot().scope.risk_domain_id,
            venue: baseline.snapshot().scope.venue,
            account: baseline.snapshot().scope.account,
            environment: match baseline.snapshot().scope.environment {
                tickwire_venue::Environment::Production => 1,
                tickwire_venue::Environment::Test => 2,
            },
            product: match product {
                tickwire_venue::Product::Spot => 1,
                tickwire_venue::Product::LinearPerpetual => 2,
                tickwire_venue::Product::LinearFuture => 3,
                _ => return Err(invalid("unsupported failover product")),
            },
        };
        if stream.domain != scope.risk_domain {
            return Err(invalid("fence/source domain"));
        }
        let signed: SignedDocument =
            serde_json::from_slice(&read_file(&self.document, 256 * 1024)?)?;
        let now = clock
            .sample()
            .map_err(|_| invalid("fence clock"))?
            .monotonic;
        Verifier::new(
            self.controller_id,
            self.fence_public_key,
            self.emergency_public_key,
        )
        .map_err(|_| invalid("fence authority"))?
        .verify_fence(&signed, &scope, &stream.source, now)
        .map_err(|_| invalid("invalid or expired fence"))
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealConfig {
    pub deployment: PathBuf,
    pub fence: FenceAuthority,
    pub verifier_id: [u8; 16],
    pub output_seal: PathBuf,
    pub copy_to_mirror: bool,
    pub maximum_copy_rounds: usize,
}
#[derive(Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum RestoreMode {
    SourceSeal {
        seal_hash: Hash,
        source_signing_public_key: Hash,
    },
    SyncReplica,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreConfig {
    pub receiver_config: PathBuf,
    pub deployment: PathBuf,
    pub fence: FenceAuthority,
    pub proof: RestoreMode,
    pub account_bootstrap_hash: Hash,
}
impl SealConfig {
    pub fn read(path: &Path) -> Result<Self> {
        Ok(serde_json::from_slice(&read_file(path, 1024 * 1024)?)?)
    }
}
impl RestoreConfig {
    pub fn read(path: &Path) -> Result<Self> {
        Ok(serde_json::from_slice(&read_file(path, 1024 * 1024)?)?)
    }
}
/// Validate a previous offline reconstruction before opening either runtime WAL.
/// The returned frontiers are minimum required physical prefixes, not authority
/// to reactivate the still-frozen account. An empty account has no BatchSeal.
pub fn validate_start(
    d: &Deployment,
) -> Result<Option<(Option<PhysicalFrontier>, PhysicalFrontier)>> {
    let directories = [
        &d.directories.account,
        &d.directories.input,
        &d.directories.recovery,
    ];
    let markers = directories
        .iter()
        .map(|p| read_marker(&p.join(STARTED_FILE)))
        .collect::<Result<Vec<_>>>()?;
    let complete = read_marker(&d.directories.account.join(COMPLETE_FILE))?;
    if markers.iter().all(Option::is_none) && complete.is_none() {
        return Ok(None);
    }
    if !d.recover || markers.iter().any(Option::is_none) {
        return Err(invalid("incomplete failover directories"));
    }
    let marker = markers[0].as_ref().unwrap();
    if markers.iter().any(|bytes| bytes.as_ref() != Some(marker)) {
        return Err(invalid("conflicting failover directory markers"));
    }
    let started: RestoreStarted = serde_json::from_slice(marker)?;
    let complete: RestoreComplete =
        serde_json::from_slice(&complete.ok_or_else(|| invalid("failover is still in progress"))?)?;
    let baseline = d.baseline()?;
    let configured = d.assemble(baseline.clone())?;
    if started.format != "tickwire.failover-in-progress.v1"
        || complete.format != "tickwire.frozen-failover.v2"
        || complete.started != started
        || complete.started_sha256 != tickwire_recovery::digest(marker)
        || started.target != TargetBinding::new(d, &baseline)?
        || complete.ledger_sequence < baseline.snapshot().ledger_sequence
    {
        return Err(invalid("failover completion binding"));
    }
    let stream = replica::Stream::decode(&started.source_stream)?;
    if stream.domain != started.target.risk_domain
        || stream.journal_epoch != d.account_stream
        || stream.format
            != (replica::Format::Account {
                baseline: baseline.digest(),
            })
        || stream.source.exec_epoch >= d.writer_epoch
        || stream.source.exec_instance_id == d.owner_id
        || stream.source.boot_id == d.boot_id
        || started.recovery_seal_sha256 == [0; 32]
        || started.fence_sha256 == [0; 32]
        || started.complete_prefix.writer_epoch > stream.source.exec_epoch
    {
        return Err(invalid("failover source and target identity"));
    }
    if started.complete_prefix.sequence == 0
        && started.complete_prefix != replica::Prefix::genesis(&stream)
    {
        return Err(invalid("empty failover prefix"));
    }
    let actual_baseline = read_file(
        &d.directories.account.join("ACCOUNT-BASELINE.cbor"),
        tickwire_account_codec::MAX_BASELINE_BYTES,
    )?;
    let actual_bootstrap = read_file(&d.directories.account.join("ACCOUNT-BOOTSTRAP.json"), 65536)?;
    let expected_bootstrap =
        tickwire_account_journal::bootstrap_bytes(&configured.account.journal)?;
    if actual_baseline != baseline.bytes()
        || actual_bootstrap != expected_bootstrap
        || !stream
            .initial_objects
            .contains(&ObjectRef::new(&actual_bootstrap)?)
        || !stream
            .initial_objects
            .contains(&ObjectRef::new(&actual_baseline)?)
    {
        return Err(invalid("failover baseline or bootstrap"));
    }
    let store = Persistence::new(FileBlobStore::open(&d.directories.recovery)?);
    replica::Signed::decode(
        &store.read_object(started.recovery_seal_sha256, replica::MAX_FRAME_BYTES)?,
    )?;
    let size =
        usize::try_from(started.final_input_bytes).map_err(|_| invalid("failover input bound"))?;
    let input_bytes = store.read_object(started.final_input_root, size)?;
    if input_bytes.len() != size {
        return Err(invalid("failover input length"));
    }
    let input = replica::InputPrefix::decode(&input_bytes)?;
    if input.input_stream != d.input_stream
        || input.account_stream != d.account_stream
        || input.baseline != baseline.digest()
        || input.through_sequence != started.final_input_sequence
    {
        return Err(invalid("failover input prefix"));
    }
    let account_required = match &complete.account_frontier {
        Some(f) => Some(f.required(
            &configured.account.journal.physical,
            started.complete_prefix.sequence,
            stream.source.exec_epoch,
        )?),
        None if started.complete_prefix.sequence == 0 => None,
        None => return Err(invalid("missing failover account frontier")),
    };
    let input_required = complete.input_frontier.required(
        &configured.input.physical,
        started.final_input_sequence,
        stream.source.exec_epoch,
    )?;
    Ok(Some((account_required, input_required)))
}
fn read_marker(path: &Path) -> Result<Option<Vec<u8>>> {
    match std::fs::symlink_metadata(path) {
        Ok(m) if m.file_type().is_file() => Ok(Some(read_file(path, COMPLETION_LIMIT)?)),
        Ok(_) => Err(invalid("invalid failover marker type")),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}
fn create_marked_directory(path: &Path, marker: &[u8]) -> Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)?;
    File::open(path.parent().ok_or_else(|| invalid("failover parent"))?)?.sync_all()?;
    write_sync(&path.join(STARTED_FILE), marker)
}
fn source_config(c: &crate::replication::Config, stream: replica::Stream) -> replica::SourceConfig {
    replica::SourceConfig {
        stream,
        profile_version: c.profile_version,
        mode: match c.profile {
            crate::replication::Profile::LocalDurableAsync {
                maximum_unreplicated_age_ns,
            } => replica::CommitMode::LocalDurableAsync {
                max_unreplicated_age_ns: maximum_unreplicated_age_ns,
            },
            crate::replication::Profile::SyncReplica {
                mirror_id,
                assignment_epoch,
            } => replica::CommitMode::SyncReplica {
                mirror_id,
                assignment_epoch,
            },
        },
        max_records: c.maximum_records,
        max_pending_batches: c.queue_capacity,
        max_pending_bytes: c.maximum_pending_bytes,
    }
}
fn input_root<S: tickwire_journal::store::Store, P: tickwire_recovery::BlobStore>(
    input: &mut AccountIngress<S>,
    account_stream: [u8; 16],
    baseline: Hash,
    store: &mut Persistence<P>,
) -> Result<ObjectRef> {
    let mut entries = Vec::new();
    let mut next = 1;
    while next <= input.position() {
        let export = input.export_durable(next, 32, 65536)?;
        if export.records().is_empty() {
            return Err(invalid("missing input export"));
        }
        for r in export.records() {
            if r.seq != next || entries.len() >= replica::MAX_INPUT_RECORDS {
                return Err(invalid("input prefix bound"));
            }
            let object = ObjectRef::new(&r.payload)?;
            store.put_object(&r.payload)?;
            entries.push((r.seq, r.exec_epoch, object));
            next = next
                .checked_add(1)
                .ok_or_else(|| invalid("input sequence overflow"))?;
        }
    }
    let mut bytes = b"TWIPFX01".to_vec();
    bytes.extend(input.input_stream());
    bytes.extend(account_stream);
    bytes.extend(baseline);
    bytes.extend((entries.len() as u64).to_le_bytes());
    bytes.extend((entries.len() as u32).to_le_bytes());
    for (seq, writer, o) in entries {
        bytes.extend(seq.to_le_bytes());
        bytes.extend(writer.to_le_bytes());
        bytes.extend(o.bytes.to_le_bytes());
        bytes.extend(o.hash);
    }
    replica::InputPrefix::decode(&bytes)?;
    store.put_object(&bytes)?;
    Ok(ObjectRef::new(&bytes)?)
}
pub fn seal(c: &SealConfig) -> Result<Value> {
    if !c.output_seal.is_absolute()
        || c.verifier_id == [0; 16]
        || c.maximum_copy_rounds == 0
        || c.maximum_copy_rounds > 100_000
    {
        return Err(invalid("seal bounds"));
    }
    let d = Deployment::read(&c.deployment)?;
    let baseline = d.baseline()?;
    let configured = d.assemble(baseline.clone())?;
    let r = d
        .replication
        .as_ref()
        .ok_or_else(|| invalid("source replication configuration required"))?;
    let stream = replica::Stream::decode(&read_file(&r.stream_file, 65536)?)?;
    if stream.journal_epoch != d.account_stream
        || stream.source.exec_epoch != d.writer_epoch
        || stream.source.exec_instance_id != d.owner_id
        || stream.source.boot_id != d.boot_id
        || stream.format
            != (replica::Format::Account {
                baseline: baseline.digest(),
            })
    {
        return Err(invalid("source deployment identity"));
    }
    let mut clock = c.fence.clock()?;
    let fence = c
        .fence
        .verify(&stream, &baseline, d.product.product(), &mut clock)?;
    // Locks exclude every source writer. No trading sockets are constructed.
    let mut backend = FileBackend::open(&d.directories.account)?;
    let bootstrap = tickwire_account_journal::bootstrap_bytes(&configured.account.journal)?;
    if backend.read_bootstrap()?.as_deref() != Some(bootstrap.as_slice())
        || backend.read_baseline()?.as_deref() != Some(baseline.bytes())
    {
        return Err(invalid("source account bootstrap"));
    }
    let (mut raw, recovered) = Journal::recover(backend, None)?;
    if raw.config() != &configured.account.journal.physical {
        return Err(invalid("source account stream"));
    }
    let shared = SharedBlobStore::new(FileBlobStore::open(&d.directories.recovery)?);
    let mut input = AccountIngress::recover(
        FileStore::open(&d.directories.input)?,
        configured.input,
        None,
    )?
    .with_closure_store(shared.clone());
    let mut store = Persistence::new(shared.clone());
    store.put_object(baseline.bytes())?;
    store.put_object(&bootstrap)?;
    if !stream
        .initial_objects
        .contains(&ObjectRef::new(&bootstrap)?)
    {
        return Err(invalid("source bootstrap dependency"));
    }
    let final_input = input_root(&mut input, d.account_stream, baseline.digest(), &mut store)?;
    let mut extras = BTreeMap::new();
    for raw in &recovered.records {
        let record =
            CanonicalRecord::decode(&raw.payload).map_err(|_| invalid("account record"))?;
        let mut objects = vec![final_input.clone()];
        if let RecoveryRecord::UncreatedAlgoClosed(binding) = record.record() {
            let bytes = input.uncreated_algo_coverage_object(binding)?;
            store.put_object(&bytes)?;
            objects.push(ObjectRef::new(&bytes)?);
        }
        if let RecoveryRecord::AccountInputsReconciled(binding) = record.record() {
            let bytes = input.account_inputs_coverage_object(binding)?;
            store.put_object(&bytes)?;
            objects.push(ObjectRef::new(&bytes)?);
        }
        if let RecoveryRecord::IncomeBaseline(binding) = record.record() {
            let bytes = input.income_coverage_object(binding)?;
            store.put_object(&bytes)?;
            objects.push(ObjectRef::new(&bytes)?);
        }
        if let RecoveryRecord::Reactivated(binding) = record.record() {
            let bytes = input.activation_coverage_object(binding)?;
            store.put_object(&bytes)?;
            objects.push(ObjectRef::new(&bytes)?);
        }
        if let RecoveryRecord::StrategyHandover(binding) = record.record() {
            for hash in [binding.checkpoint_digest, binding.input_seal_digest] {
                let bytes = store.read_object(hash, replica::MAX_OBJECT_BYTES)?;
                objects.push(ObjectRef::new(&bytes)?);
            }
        }
        objects.sort_by_key(|o| o.hash);
        objects.dedup_by_key(|o| o.hash);
        extras.insert(raw.seq, objects);
    }
    let mut closed_input = input.close_for_recovery()?;
    let mut source =
        replica::SourceWorker::new(source_config(r, stream.clone()), Persistence::new(shared))?;
    let mut next = 1;
    while next <= recovered.records.len() as u64 {
        let export = raw.export_durable(next, 32, 65536)?;
        if export.records().is_empty() {
            return Err(invalid("source prefix gap"));
        }
        source.restore_prefix(
            &export,
            &extras,
            clock
                .sample()
                .map_err(|_| invalid("seal clock"))?
                .monotonic
                .nanos(),
        )?;
        next = export.records().last().unwrap().seq + 1;
    }
    let mut closed_account = raw.close_for_recovery()?;
    let signer = replica::Signer::from_pkcs8(&read_file(&r.signing_key_pkcs8, 65536)?)?;
    let signed = source.seal_account(
        &mut closed_account,
        &mut closed_input,
        final_input.clone(),
        &fence,
        c.verifier_id,
        baseline.digest(),
        clock.sample().map_err(|_| invalid("seal clock"))?.monotonic,
        &signer,
    )?;
    write_sync(&c.output_seal, &signed.bytes())?;
    let seal_hash = tickwire_recovery::digest(&signed.bytes());
    if c.copy_to_mirror {
        let peer = replica::PeerClient::new(
            r.mirror,
            &stream,
            replica::Signer::from_pkcs8(&read_file(&r.signing_key_pkcs8, 65536)?)?,
            r.mirror_signing_public_key,
            r.tls.client(r.mirror_server_name.clone())?,
            r.network.build()?,
        )?;
        let mut sender = replica::SourceSender::new(peer);
        let mut complete = false;
        for _ in 0..c.maximum_copy_rounds {
            fence
                .check(clock.sample().map_err(|_| invalid("copy clock"))?.monotonic)
                .map_err(|_| invalid("fence expired during copy"))?;
            let progress = sender.poll(&mut source)?;
            if progress.durable_sequence == source.prefix().sequence {
                complete = true;
                break;
            }
        }
        if !complete {
            return Err(invalid("source sealed; mirror copy bound exhausted"));
        }
        if sender.upload_final(&source, std::slice::from_ref(&final_input), &signed)? != seal_hash {
            return Err(invalid("seal object transfer"));
        }
    }
    Ok(
        json!({"source_sequence":source.prefix().sequence,"source_seal_sha256":seal_hash,"final_input_root":final_input.hash,"seal_file":c.output_seal,"copied_to_mirror":c.copy_to_mirror,"source_writers_closed":true}),
    )
}
pub fn restore(c: &RestoreConfig) -> Result<Value> {
    let receiver = replica::config::ReceiverConfig::from_file(&c.receiver_config)?;
    if receiver.create {
        return Err(invalid("restore requires an existing mirror"));
    }
    let d = Deployment::read(&c.deployment)?;
    let baseline = d.baseline()?;
    let configured = d.assemble(baseline.clone())?;
    let stream = replica::Stream::decode(&read_file(&receiver.stream_file, 65536)?)?;
    if !d.recover
        || d.writer_epoch <= stream.source.exec_epoch
        || d.owner_id == stream.source.exec_instance_id
        || d.boot_id == stream.source.boot_id
        || d.account_stream != stream.journal_epoch
        || stream.format
            != (replica::Format::Account {
                baseline: baseline.digest(),
            })
    {
        return Err(invalid("new frozen recovery identity"));
    }
    let mut clock = c.fence.clock()?;
    let fence = c
        .fence
        .verify(&stream, &baseline, d.product.product(), &mut clock)?;
    let mut mirror = receiver.open_store()?;
    let now = clock
        .sample()
        .map_err(|_| invalid("restore clock"))?
        .monotonic;
    let complete = match c.proof {
        RestoreMode::SourceSeal {
            seal_hash,
            source_signing_public_key,
        } => {
            if source_signing_public_key != receiver.source_signing_public_key {
                return Err(invalid("source seal signer"));
            }
            let signed =
                replica::Signed::decode(&mirror.object(seal_hash, replica::MAX_FRAME_BYTES)?)?;
            mirror.verify_source_complete(&signed, source_signing_public_key, &fence, now)?
        }
        RestoreMode::SyncReplica => {
            let signer =
                replica::Signer::from_pkcs8(&read_file(&receiver.signing_key_pkcs8, 65536)?)?;
            let signed = mirror.close_sync(&fence, now, &signer)?;
            mirror.verify_sync_complete(&signed, signer.public_key(), &fence, now)?
        }
    };
    let roots = mirror.input_roots(&complete)?;
    if roots.len() != 1 {
        return Err(invalid(
            "exactly one complete account input stream required",
        ));
    }
    let input_prefix =
        replica::InputPrefix::decode(&mirror.object(roots[0].hash, roots[0].bytes as usize)?)?;
    if input_prefix.input_stream != d.input_stream {
        return Err(invalid("input restore identity"));
    }
    let bootstrap = stream
        .initial_objects
        .iter()
        .find(|o| o.hash == c.account_bootstrap_hash)
        .ok_or_else(|| invalid("account bootstrap reference"))?;
    let target = TargetBinding::new(&d, &baseline)?;
    if target.account_bootstrap != bootstrap.hash {
        return Err(invalid("target bootstrap differs from source"));
    }
    for path in [
        &d.directories.account,
        &d.directories.input,
        &d.directories.recovery,
    ] {
        if !path.is_absolute() || path.exists() {
            return Err(invalid(
                "restore destination must be a new absolute directory",
            ));
        }
    }
    let directories = [
        &d.directories.account,
        &d.directories.input,
        &d.directories.recovery,
    ];
    for (index, path) in directories.iter().enumerate() {
        for other in &directories[index + 1..] {
            if path.starts_with(other) || other.starts_with(path) {
                return Err(invalid("restore destination overlap"));
            }
        }
    }
    for path in [
        &d.directories.account,
        &d.directories.input,
        &d.directories.recovery,
    ] {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let started = RestoreStarted {
        format: "tickwire.failover-in-progress.v1".into(),
        target,
        source_stream: stream.bytes(),
        complete_prefix: complete.prefix(),
        recovery_seal_sha256: complete.recovery_seal_hash(),
        fence_sha256: complete.fence_evidence_hash(),
        final_input_root: roots[0].hash,
        final_input_bytes: roots[0].bytes,
        final_input_sequence: input_prefix.through_sequence,
    };
    let marker = serde_json::to_vec(&started)?;
    // Publish markers before any baseline, journal, or recovery object. They
    // remain after completion so ordinary RecoverFrozen cannot bypass the cut.
    for directory in directories {
        create_marked_directory(directory, &marker)?;
    }
    let shared = SharedBlobStore::new(FileBlobStore::open(&d.directories.recovery)?);
    let mut blobs = Persistence::new(shared.clone());
    mirror.copy_recovery_closure(&complete, &mut blobs)?;
    let account = mirror.materialize_account(
        &complete,
        bootstrap,
        FileBackend::open(&d.directories.account)?,
    )?;
    let account_frontier = account.durable_frontier().cloned();
    if account_frontier.as_ref().map_or(0, |f| f.last_seq) != complete.prefix().sequence {
        return Err(invalid("materialized account frontier"));
    }
    drop(account);
    let input = mirror.materialize_input_with_config(
        &roots[0],
        FileStore::open(&d.directories.input)?,
        configured.input.physical.clone(),
    )?;
    let input_frontier = input
        .durable_frontier()
        .cloned()
        .ok_or_else(|| invalid("materialized input frontier"))?;
    if input_frontier.last_seq != input_prefix.through_sequence {
        return Err(invalid("materialized input prefix"));
    }
    drop(input);
    // Reopen through the real semantic recovery path before publishing completion.
    let input = AccountIngress::recover(
        FileStore::open(&d.directories.input)?,
        configured.input,
        Some(&input_frontier),
    )?
    .with_closure_store(shared);
    let mut verifier = Historical {
        blobs,
        input: &input,
    };
    let recovered = AccountJournal::recover(
        FileBackend::open(&d.directories.account)?,
        configured.account.journal,
        d.writer_epoch,
        account_frontier.as_ref(),
        &mut verifier,
    )?;
    if !recovered.account.frozen() {
        return Err(invalid("reconstructed account is not frozen"));
    }
    fence
        .check(
            clock
                .sample()
                .map_err(|_| invalid("restore clock"))?
                .monotonic,
        )
        .map_err(|_| invalid("fence expired during restore"))?;
    let report = RestoreComplete {
        format: "tickwire.frozen-failover.v2".into(),
        started_sha256: tickwire_recovery::digest(&marker),
        started,
        account_frontier: account_frontier.map(Frontier::from),
        input_frontier: input_frontier.into(),
        ledger_sequence: recovered.account.ledger_sequence(),
    };
    write_sync(
        &d.directories.account.join(COMPLETE_FILE),
        &serde_json::to_vec_pretty(&report)?,
    )?;
    Ok(serde_json::to_value(report)?)
}
struct Historical<'a> {
    blobs: Persistence<SharedBlobStore<FileBlobStore>>,
    input: &'a AccountIngress<FileStore>,
}
impl RecoveryVerifier for Historical<'_> {
    fn verify_income_association(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        preceding: &HistoryFrontier,
        prior: &[CanonicalRecord],
        writer_epoch: u64,
    ) -> std::result::Result<(), VerificationError> {
        let RecoveryRecord::IncomeObservationsAssociated(binding) = record.record() else {
            return Err(VerificationError::Invalid);
        };
        let account = Account::recover(
            baseline.snapshot().clone(),
            baseline.catalog().clone(),
            &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
        )
        .map_err(|_| VerificationError::Invalid)?;
        self.input
            .activation_verifier()
            .verify_income_association(baseline, &account, preceding, prior, binding, writer_epoch)
            .map_err(|_| VerificationError::Invalid)
    }

    fn verify_uncreated_algo(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        preceding: &HistoryFrontier,
        prior: &[CanonicalRecord],
        writer_epoch: u64,
    ) -> std::result::Result<(), VerificationError> {
        let RecoveryRecord::UncreatedAlgoClosed(binding) = record.record() else {
            return Err(VerificationError::Invalid);
        };
        let account = Account::recover(
            baseline.snapshot().clone(),
            baseline.catalog().clone(),
            &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
        )
        .map_err(|_| VerificationError::Invalid)?;
        self.input
            .activation_verifier()
            .verify_uncreated_algo(baseline, &account, preceding, binding, writer_epoch)
            .map_err(|_| VerificationError::Invalid)
    }

    fn verify_account_inputs_closure(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        preceding: &HistoryFrontier,
        prior: &[CanonicalRecord],
        writer_epoch: u64,
    ) -> std::result::Result<(), VerificationError> {
        let RecoveryRecord::AccountInputsReconciled(binding) = record.record() else {
            return Err(VerificationError::Invalid);
        };
        let account = Account::recover(
            baseline.snapshot().clone(),
            baseline.catalog().clone(),
            &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
        )
        .map_err(|_| VerificationError::Invalid)?;
        self.input
            .activation_verifier()
            .verify_account_inputs_closure(baseline, &account, preceding, binding, writer_epoch)
            .map_err(|_| VerificationError::Invalid)
    }

    fn verify_income_baseline(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        preceding: &HistoryFrontier,
        prior: &[CanonicalRecord],
    ) -> std::result::Result<(), VerificationError> {
        let RecoveryRecord::IncomeBaseline(binding) = record.record() else {
            return Err(VerificationError::Invalid);
        };
        let account = Account::recover(
            baseline.snapshot().clone(),
            baseline.catalog().clone(),
            &prior.iter().map(|r| r.record().clone()).collect::<Vec<_>>(),
        )
        .map_err(|_| VerificationError::Invalid)?;
        self.input
            .activation_verifier()
            .verify_income_baseline(baseline, &account, preceding, binding)
            .map_err(|_| VerificationError::Invalid)
    }

    fn verify_handover(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        prior: &[CanonicalRecord],
    ) -> std::result::Result<(), VerificationError> {
        tickwire_runtime::authority::verify_handover_objects(&self.blobs, baseline, record, prior)
            .map_err(|_| VerificationError::Invalid)
    }
    fn verify_admission(
        &mut self,
        baseline: &CanonicalBaseline,
        r: &CanonicalRecord,
        prior: &HistoryFrontier,
    ) -> std::result::Result<(), VerificationError> {
        let RecoveryRecord::Admit { command, .. } = r.record() else {
            return Err(VerificationError::Invalid);
        };
        let q = &command.command().context.request;
        let binding = ClosureBinding {
            risk_domain_id: q.risk_domain_id,
            intent_id: q.intent_id,
            intent_exec_epoch: q.intent_exec_epoch,
            business_digest: r.digest(),
            strategy_generation: q.strategy_generation,
            config_revision: q.config_revision,
            recovery_object: q.recovery_object,
        };
        let cap = self
            .blobs
            .verify(&binding)
            .map_err(|_| VerificationError::Invalid)?;
        if cap.command_digest() != Some(command.digest())
            || cap.account_baseline_digest() != Some(baseline.digest())
            || cap.account_history_digest() != Some(prior.digest())
            || prior.record_count().checked_add(1) != Some(r.sequence())
            || prior.baseline_digest() != baseline.digest()
        {
            return Err(VerificationError::Invalid);
        }
        Ok(())
    }
    fn verify_reactivation(
        &mut self,
        baseline: &CanonicalBaseline,
        record: &CanonicalRecord,
        prior: &HistoryFrontier,
        records: &[CanonicalRecord],
    ) -> std::result::Result<(), VerificationError> {
        let RecoveryRecord::Reactivated(binding) = record.record() else {
            return Err(VerificationError::Invalid);
        };
        let inputs: Vec<_> = records.iter().map(|r| r.record().clone()).collect();
        let account = Account::recover(
            baseline.snapshot().clone(),
            baseline.catalog().clone(),
            &inputs,
        )
        .map_err(|_| VerificationError::Invalid)?;
        self.input
            .activation_verifier()
            .verify(baseline, &account, prior, binding)
            .map_err(|_| VerificationError::Invalid)
    }
}
fn write_sync(path: &Path, bytes: &[u8]) -> Result<()> {
    if !path.is_absolute() {
        return Err(invalid("output path must be absolute"));
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if read_file(path, replica::MAX_FRAME_BYTES)? != bytes {
                return Err(invalid("conflicting existing output"));
            }
            File::open(path)?.sync_all()?;
        }
        Err(e) => return Err(e.into()),
    }
    File::open(path.parent().ok_or_else(|| invalid("output parent"))?)?.sync_all()?;
    Ok(())
}
