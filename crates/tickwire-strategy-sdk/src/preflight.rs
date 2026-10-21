//! Actual startup restore. A completed callback is saved before any market or
//! intent attachment; Host separately authenticates the receipt's process.
use crate::{Error, Result, Strategy, bootstrap::Bootstrap, restore};
use serde::{Deserialize, Serialize};
use std::{
    os::unix::fs::{DirBuilderExt, MetadataExt},
    path::PathBuf,
};
use tickwire_release::{Hash, Member, Object, Operation, ReleaseBundle, Role, files};

/// Bind the template and every referenced input before any restore callback.
pub fn verify_recovery_inputs(
    path: Option<&std::path::Path>,
    loaded: &files::LoadedFiles,
) -> Result<()> {
    let path = path.ok_or(Error::Identity)?;
    if loaded.path(&Role::RecoveryTemplate) != Some(path) {
        return Err(Error::Identity);
    }
    let template: crate::recovery::Template = serde_json::from_slice(
        loaded
            .bytes(&Role::RecoveryTemplate)
            .ok_or(Error::Identity)?,
    )?;
    if template.dependencies.is_empty() || template.dependencies.len() > 10 {
        return Err(Error::Bounds);
    }
    for dependency in &template.dependencies {
        if !loaded.contains_input(&dependency.path, dependency.digest) {
            return Err(Error::Identity);
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "preflight_tests.rs"]
mod tests;

pub use tickwire_release::files::ObjectFile;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleFiles {
    pub manifest: ObjectFile,
    pub migration: ObjectFile,
    pub rollback: ObjectFile,
}
impl BundleFiles {
    pub fn load(&self, operation: &Operation) -> Result<ReleaseBundle> {
        if [
            self.manifest.object.digest,
            self.migration.object.digest,
            self.rollback.object.digest,
        ] != [
            operation.artifacts_digest,
            operation.checkpoint_digest,
            operation.rollback_digest,
        ] {
            return Err(Error::Identity);
        }
        Ok(ReleaseBundle::decode(
            &self.manifest.read()?,
            &self.migration.read()?,
            &self.rollback.read()?,
        )?)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceFiles {
    pub proof: ObjectFile,
    pub checkpoint: ObjectFile,
    pub inputs: ObjectFile,
    pub account_inputs: ObjectFile,
    pub dependencies: Vec<ObjectFile>,
    #[serde(default)]
    pub prior_preflight: Vec<ObjectFile>,
}
impl SourceFiles {
    pub fn objects(&self) -> Result<Vec<Vec<u8>>> {
        if self.dependencies.len() > 10 || self.prior_preflight.len() > 256 {
            return Err(Error::Bounds);
        }
        // Reject a caller-controlled oversized graph before allocating its files.
        let mut bytes = 0u64;
        for file in [
            &self.proof,
            &self.checkpoint,
            &self.inputs,
            &self.account_inputs,
        ]
        .into_iter()
        .chain(&self.dependencies)
        .chain(&self.prior_preflight)
        {
            file.object
                .validate(tickwire_recovery::MAX_OBJECT_BYTES as u64)?;
            bytes = bytes
                .checked_add(file.object.bytes)
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
                .ok_or(Error::Bounds)?;
        }
        let proof = self.proof.read()?;
        let seal = tickwire_recovery::HandoverSeal::decode(&proof)?;
        let checkpoint = self.checkpoint.read()?;
        let inputs = self.inputs.read()?;
        let account = self.account_inputs.read()?;
        tickwire_handover::verify(&seal, &checkpoint, &inputs, &account)
            .map_err(|_| Error::Identity)?;
        if self.dependencies.len() != seal.dependencies.len() {
            return Err(Error::Identity);
        }
        let mut all = vec![proof, checkpoint, inputs, account];
        for (file, (_, digest)) in self.dependencies.iter().zip(&seal.dependencies) {
            if file.object.digest != *digest {
                return Err(Error::Identity);
            }
            all.push(file.read()?);
        }
        if seal.preflight_receipt_digest.is_some() || seal.version == 3 {
            let mut extra = std::collections::BTreeMap::new();
            for f in &self.prior_preflight {
                extra.insert(f.object.digest, f.read()?);
            }
            if let Some(root) = seal.preflight_receipt_digest {
                let prior = tickwire_release::closure::verify(root, |h| {
                    extra
                        .get(&h)
                        .cloned()
                        .ok_or(tickwire_release::Error::Missing)
                })?;
                if prior.completion.member.new_instance != seal.instance
                    || prior.completion.member.new_generation != seal.generation
                {
                    return Err(Error::Identity);
                }
                all.extend(prior.objects);
            }
            if seal.version == 3 {
                let prior =
                    tickwire_release::replay::startup::verify_seal(&seal, &all[1], &all[2], |h| {
                        extra
                            .get(&h)
                            .cloned()
                            .ok_or(tickwire_release::Error::Missing)
                    })?;
                all.extend(prior);
            }
        } else if !self.prior_preflight.is_empty() {
            return Err(Error::Identity);
        }
        if all.iter().map(Vec::len).sum::<usize>() > tickwire_recovery::MAX_CLOSURE_BYTES as usize {
            return Err(Error::Bounds);
        }
        Ok(all)
    }
}
/// Verify Host's completion against this process's actual callback receipt. The
/// caller authenticates the STATUS frame before passing its referenced root.
pub fn completion(
    job: &Job,
    completed: &Completed,
    root: Hash,
) -> Result<tickwire_release::closure::Verified> {
    let mut known = std::collections::BTreeMap::new();
    for f in [
        &job.bundle.manifest,
        &job.bundle.migration,
        &job.bundle.rollback,
    ] {
        known.insert(f.object.digest, f.read()?);
    }
    for b in job.source.objects()? {
        known.insert(tickwire_recovery::digest(&b), b);
    }
    let closure = tickwire_release::closure::verify(root, |h| {
        if let Some(b) = known.get(&h) {
            return Ok(b.clone());
        }
        let name = h.iter().map(|b| format!("{b:02x}")).collect::<String>();
        crate::recovery::read(
            &job.output_directory.join(format!("{name}.bin")),
            tickwire_recovery::MAX_OBJECT_BYTES,
        )
        .map_err(|_| tickwire_release::Error::Missing)
    })?;
    if closure.completion.operation != job.operation
        || closure.completion.member != job.member
        || closure.completion.restored != Object::of(completed.bytes())
        || closure.restored != *completed.receipt()
    {
        return Err(Error::Identity);
    }
    Ok(closure)
}
pub fn startup_completion(
    job: &Job,
    completed: &Completed,
    root: Hash,
) -> Result<tickwire_release::replay::startup::Verified> {
    let mut known = std::collections::BTreeMap::new();
    for f in [
        &job.bundle.manifest,
        &job.bundle.migration,
        &job.bundle.rollback,
    ] {
        known.insert(f.object.digest, f.read()?);
    }
    for b in job.source.objects()? {
        known.insert(tickwire_recovery::digest(&b), b);
    }
    let closure = tickwire_release::replay::startup::verify(root, |h| {
        if let Some(b) = known.get(&h) {
            return Ok(b.clone());
        }
        let name = h.iter().map(|b| format!("{b:02x}")).collect::<String>();
        crate::recovery::read(
            &job.output_directory.join(format!("{name}.bin")),
            tickwire_recovery::MAX_OBJECT_BYTES,
        )
        .map_err(|_| tickwire_release::Error::Missing)
    })?;
    if closure.completion.operation != job.operation
        || closure.completion.member != job.member
        || closure.completion.restored != Object::of(completed.bytes())
        || closure.restored != *completed.receipt()
    {
        return Err(Error::Identity);
    }
    Ok(closure)
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Job {
    pub schema: u16,
    pub operation: Operation,
    pub member: Member,
    pub bundle: BundleFiles,
    pub target_files: Vec<files::Location>,
    pub rollback_files: Vec<files::Location>,
    pub source: SourceFiles,
    /// Separate private output directory, never an old strategy's writer store.
    pub output_directory: PathBuf,
    /// Filled by Host after its strictly limited Bootstrap derivation.
    pub effective_bootstrap: Object,
}
pub enum Running {
    Observe(crate::ObserveOnly),
    Demo(restore::RestoredDemo),
}
struct RunningFactory(bool);
impl restore::RestoreFactory for RunningFactory {
    type Running = Running;
    fn capabilities(&self) -> tickwire_release::Capabilities {
        if self.0 {
            restore::ObserveFactory.capabilities()
        } else {
            restore::FixedDemoFactory.capabilities()
        }
    }
    fn configuration(
        &self,
        target: &restore::Target,
    ) -> restore::Result<tickwire_release::replay::VersionedBytes> {
        if self.0 {
            restore::ObserveFactory.configuration(target)
        } else {
            restore::FixedDemoFactory.configuration(target)
        }
    }
    fn restore(&self, input: &restore::VerifiedRestoreInput) -> restore::Result<Running> {
        if self.0 {
            restore::ObserveFactory.restore(input).map(Running::Observe)
        } else {
            restore::FixedDemoFactory.restore(input).map(Running::Demo)
        }
    }
    fn migrate(&self, input: &restore::VerifiedRestoreInput) -> restore::Result<Running> {
        if self.0 {
            Err(restore::Error::Unsupported)
        } else {
            restore::FixedDemoFactory.migrate(input).map(Running::Demo)
        }
    }
    fn validate_checkpoint(&self, bytes: &[u8]) -> restore::Result<()> {
        if self.0 {
            restore::ObserveFactory.validate_checkpoint(bytes)
        } else {
            restore::FixedDemoFactory.validate_checkpoint(bytes)
        }
    }
}
impl Strategy for Running {
    fn prepare_replayed(
        &self,
        release: &tickwire_release::MemberRelease<'_>,
        target: restore::Target,
        proof: &tickwire_release::replay::source::VerifiedSourceReplay,
        source: tickwire_release::replay::Source,
        config: tickwire_release::replay::VersionedBytes,
    ) -> crate::replay::Result<crate::replay::install::Candidate<Self>> {
        crate::replay::install::prepare(
            &RunningFactory(matches!(self, Self::Observe(_))),
            release,
            target,
            proof,
            source,
            config,
        )
    }
    fn apply_config(
        &mut self,
        bytes: &[u8],
        context: &mut crate::replay::ReplayContext<'_>,
    ) -> crate::Result<()> {
        match self {
            Self::Observe(s) => s.apply_config(bytes, context),
            Self::Demo(s) => s.apply_config(bytes, context),
        }
    }
    fn replay_registration(&self) -> Result<Option<crate::replay::native::Registration>> {
        match self {
            Self::Observe(s) => s.replay_registration(),
            Self::Demo(s) => s.replay_registration(),
        }
    }
    fn checkpoint(&self) -> Result<Vec<u8>> {
        match self {
            Self::Observe(s) => s.checkpoint(),
            Self::Demo(s) => s.checkpoint(),
        }
    }
    fn on_report(
        &mut self,
        r: &crate::reports::AccountReport,
        n: u64,
    ) -> Result<Option<tickwire_venue::CanonicalCommand>> {
        match self {
            Self::Observe(s) => s.on_report(r, n),
            Self::Demo(s) => s.on_report(r, n),
        }
    }
    fn on_market(
        &mut self,
        v: crate::MarketView<'_>,
        n: u64,
    ) -> Result<Option<tickwire_venue::CanonicalCommand>> {
        match self {
            Self::Observe(s) => s.on_market(v, n),
            Self::Demo(s) => s.on_market(v, n),
        }
    }
}
/// Fields are private: workers cannot claim completion without executing a factory.
pub struct Completed {
    receipt: tickwire_release::receipt::Receipt,
    receipt_bytes: Vec<u8>,
}
impl Completed {
    pub fn receipt(&self) -> &tickwire_release::receipt::Receipt {
        &self.receipt
    }
    pub fn bytes(&self) -> &[u8] {
        &self.receipt_bytes
    }
    pub fn digest(&self) -> Hash {
        tickwire_recovery::digest(&self.receipt_bytes)
    }
}
fn same(a: impl Serialize, b: impl Serialize) -> Result<bool> {
    Ok(serde_json::to_vec(&a)? == serde_json::to_vec(&b)?)
}
/// Verify only Host-generated socket locations, peer OS identity, and computed
/// report descriptor hash changed. Every strategy/control/catalog field remains bound.
pub fn verify_bootstrap(template: &Bootstrap, effective: &Bootstrap) -> Result<()> {
    let mut normalized = effective.clone();
    if template.external_market.is_some() != effective.external_market.is_some()
        || !effective.handover_enabled
        || effective.preflight_job.is_none()
    {
        return Err(Error::Identity);
    }
    let parent = effective.intent.path.parent().ok_or(Error::Identity)?;
    if !parent.is_absolute()
        || effective.report.path.parent() != Some(parent)
        || effective.intent.peer_pid == 0
        || effective.intent.peer_pid != effective.report.peer_pid
        || effective.intent.peer_uid != effective.report.peer_uid
        || effective.intent.peer_gid != effective.report.peer_gid
    {
        return Err(Error::Identity);
    }
    let reset = |actual: &mut crate::bootstrap::Endpoint, original: &crate::bootstrap::Endpoint| {
        actual.path = original.path.clone();
        actual.peer_pid = original.peer_pid;
        actual.peer_uid = original.peer_uid;
        actual.peer_gid = original.peer_gid;
    };
    reset(&mut normalized.intent, &template.intent);
    reset(&mut normalized.report, &template.report);
    if effective.external_market.is_none() {
        if effective.market.path.parent() != Some(parent)
            || effective.join_file.parent() != Some(parent)
            || effective.market.peer_pid != effective.intent.peer_pid
            || effective.market.peer_uid != effective.intent.peer_uid
            || effective.market.peer_gid != effective.intent.peer_gid
        {
            return Err(Error::Identity);
        }
        reset(&mut normalized.market, &template.market);
        normalized.join_file = template.join_file.clone();
    } else {
        if effective
            .market_admission_ready
            .as_ref()
            .and_then(|p| p.parent())
            != Some(parent)
        {
            return Err(Error::Identity);
        }
        normalized.market_admission_ready = template.market_admission_ready.clone();
    }
    let mut actual = effective.report_resource()?;
    let expected = template.report_resource()?;
    // Publisher only computes descriptor_hash for this immutable header.
    actual.descriptor_hash = expected.descriptor_hash;
    if crate::bootstrap::wire_bytes(&actual)? != crate::bootstrap::wire_bytes(&expected)? {
        return Err(Error::Identity);
    }
    normalized.report_header = template.report_header.clone();
    if !same(&normalized, template)? {
        return Err(Error::Identity);
    }
    Ok(())
}
pub fn target(config: &Bootstrap, loaded: &files::LoadedFiles) -> Result<restore::Target> {
    verify_recovery_inputs(config.recovery_template.as_deref(), loaded)?;
    let state = loaded.bytes(&Role::StrategyConfig).ok_or(Error::Identity)?;
    let command = if let Some(factory) = &config.factory {
        factory.validate()?;
        if config.demo_enabled
            || config.demo_command_file.is_some()
            || config.demo_minimum_bid_ticks != 0
            || loaded.path(&Role::StrategyConfig) != Some(factory.configuration_file.as_path())
            || loaded.object(&Role::StrategyConfig) != Some(factory.configuration)
        {
            return Err(Error::Identity);
        }
        None
    } else if config.demo_enabled {
        if config.demo_command_file.as_deref() != loaded.path(&Role::StrategyConfig) {
            return Err(Error::Identity);
        }
        Some(tickwire_venue::CanonicalCommand::decode(state)?)
    } else {
        if config.demo_command_file.is_some() || state != b"TWOBS001" {
            return Err(Error::Identity);
        }
        None
    };
    Ok(restore::Target {
        instance: config.market.local.instance,
        generation: config.market.local.generation,
        logical_strategy: config.market.local.logical_strategy,
        command,
        minimum_bid_ticks: config.demo_minimum_bid_ticks,
        enabled: config.demo_enabled,
    })
}
/// This process has only one control connection. No market/report/intent bank is
/// attached, and its temporary instance receives no trading grant.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeJob {
    pub primary: Job,
    pub target_bootstrap: ObjectFile,
    pub parent_receipt: ObjectFile,
    pub target_output: ObjectFile,
    pub endpoint: crate::bootstrap::Endpoint,
    pub output_directory: PathBuf,
}
fn save(
    directory: &std::path::Path,
    state: &[u8],
    receipt: tickwire_release::receipt::Receipt,
) -> Result<Completed> {
    let receipt_bytes = receipt.bytes()?;
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(directory)
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })?;
    let m = std::fs::symlink_metadata(directory)?;
    if !m.is_dir() || m.mode() & 0o077 != 0 || m.uid() != receipt.process.uid {
        return Err(Error::Identity);
    }
    std::fs::File::open(directory.parent().ok_or(Error::Identity)?)?.sync_all()?;
    crate::recovery::object(directory, state)?;
    crate::recovery::object(directory, &receipt_bytes)?;
    Ok(Completed {
        receipt,
        receipt_bytes,
    })
}
pub fn applied_body(completed: &Completed) -> tickwire_ipc::RestoreAppliedBody {
    let r = completed.receipt();
    tickwire_ipc::RestoreAppliedBody {
        instance_id: r.process.instance,
        logical_strategy_id: r.logical_strategy,
        strategy_generation: r.process.generation,
        receipt_digest: completed.digest(),
        receipt_bytes: completed.bytes().len() as u64,
        receipt_sequence: 1,
        release_manifest_digest: r.roots[0],
        schema: 1,
        completion_kind: if r.kind == tickwire_release::receipt::Kind::Restore {
            1
        } else {
            2
        },
    }
}
pub fn applied_status(
    body: &tickwire_ipc::RestoreAppliedBody,
    request_id: [u8; 16],
    control_epoch: u64,
) -> tickwire_ipc::StatusBody {
    tickwire_ipc::StatusBody {
        subject_id: body.instance_id,
        original_request_id: request_id,
        original_control_epoch: control_epoch,
        state: 3,
        state_version: body.receipt_sequence,
        result_hash: body.receipt_digest,
        ..Default::default()
    }
}
#[allow(unsafe_code)]
fn restore_probe_with<F: restore::RestoreFactory>(
    job: &ProbeJob,
    load: impl FnOnce(&Bootstrap, &files::LoadedFiles) -> Result<F>,
) -> Result<Completed> {
    let primary = &job.primary;
    let bundle = primary.bundle.load(&primary.operation)?;
    let release = bundle.verify_operation(&primary.operation, &primary.member)?;
    let target_files = files::load(&release.target.files, &primary.target_files)?;
    let rollback_files = files::load(&release.rollback.files, &primary.rollback_files)?;
    let actual = job.target_bootstrap.read()?;
    primary
        .effective_bootstrap
        .verify(&actual, 2 * 1024 * 1024)?;
    let config: Bootstrap = serde_json::from_slice(&actual)?;
    let template: Bootstrap = serde_json::from_slice(
        target_files
            .bytes(&Role::Bootstrap)
            .ok_or(Error::Identity)?,
    )?;
    verify_bootstrap(&template, &config)?;
    let rollback: Bootstrap = serde_json::from_slice(
        rollback_files
            .bytes(&Role::Bootstrap)
            .ok_or(Error::Identity)?,
    )?;
    let mut normalized = job.endpoint.clone();
    normalized.path = rollback.report.path.clone();
    normalized.peer_pid = rollback.report.peer_pid;
    normalized.peer_uid = rollback.report.peer_uid;
    normalized.peer_gid = rollback.report.peer_gid;
    if !same(&normalized, &rollback.report)?
        || job.output_directory == primary.output_directory
        || !job.output_directory.is_absolute()
        || std::fs::canonicalize(std::env::current_exe()?)?
            != std::fs::canonicalize(rollback_files.path(&Role::Binary).ok_or(Error::Identity)?)?
    {
        return Err(Error::Identity);
    }
    let parent_raw = job.parent_receipt.read()?;
    let parent = tickwire_release::receipt::Receipt::decode(&parent_raw)?;
    let output = job.target_output.read()?;
    if job.target_output.object != parent.output
        || parent.effective_bootstrap_digest != primary.effective_bootstrap.digest
    {
        return Err(Error::Identity);
    }
    let input = restore::VerifiedRestoreInput::verify(
        &release,
        target(&config, &target_files)?,
        &primary.source.proof.read()?,
        &primary.source.checkpoint.read()?,
        &primary.source.inputs.read()?,
        &primary.source.account_inputs.read()?,
    )?
    .for_probe(
        &release,
        target(&rollback, &rollback_files)?,
        &parent,
        &output,
    )?;
    if job.endpoint.local.identity() != rollback.market.local.identity() {
        return Err(Error::Identity);
    }
    target_files.recheck()?;
    rollback_files.recheck()?;
    let factory = load(&rollback, &rollback_files)?;
    let state = restore::execute(&factory, &input)?.checkpoint().to_vec();
    let receipt = tickwire_release::receipt::Receipt {
        schema: 1,
        kind: tickwire_release::receipt::Kind::RollbackProbe {
            parent_receipt: job.parent_receipt.object.digest,
        },
        roots: bundle.roots(),
        member: primary.member.clone(),
        logical_strategy: release.logical_strategy(),
        process: tickwire_release::receipt::Process {
            instance: rollback.market.local.instance,
            generation: rollback.market.local.generation,
            pid: std::process::id(),
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
        },
        binary_digest: rollback_files
            .object(&Role::Binary)
            .ok_or(Error::Identity)?
            .digest,
        effective_bootstrap_digest: rollback_files
            .object(&Role::Bootstrap)
            .ok_or(Error::Identity)?
            .digest,
        source_proof: job.parent_receipt.object.digest,
        source_checkpoint: parent.output.digest,
        source_schema: parent.output_schema,
        method: release.rollback.method.clone(),
        output_schema: release.rollback.capabilities.state_schema,
        output: Object::of(&state),
        source_report_position: parent.source_report_position,
    };
    receipt.verify(&release, Some(&parent))?;
    save(&job.output_directory, &state, receipt)
}
/// Authenticate before running the callback, acknowledge its persisted output,
/// then exit normally. No strategy callbacks or order submission can run here.
pub fn run_probe(job: &ProbeJob) -> Result<()> {
    run_probe_with(job, |b, _| {
        if b.factory.is_some() {
            return Err(Error::NotReady);
        }
        Ok(RunningFactory(!b.demo_enabled))
    })
}
pub fn run_probe_with<F: restore::RestoreFactory>(
    job: &ProbeJob,
    load: impl FnOnce(&Bootstrap, &files::LoadedFiles) -> Result<F>,
) -> Result<()> {
    use tickwire_ipc::{ControlEnvelope, Wire};
    let domain = tickwire_clock::ClockDomain::new(
        job.endpoint.local.host_boot,
        job.primary.member.new_generation,
    )
    .map_err(|_| Error::Identity)?;
    let mut clock = tickwire_clock_sys::SystemClock::new(domain)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let (mut connection, mut session) =
        crate::worker::connect(&job.endpoint, &mut clock, deadline, [38; 16])?;
    let completed = restore_probe_with(job, load)?;
    let body = applied_body(&completed);
    let id: [u8; 16] = body.receipt_digest[..16]
        .try_into()
        .map_err(|_| Error::Identity)?;
    let request = ControlEnvelope {
        opcode: 38,
        flags: 32,
        request_id: id,
        expected_state_version: 1,
        ..Default::default()
    };
    connection.send(
        &session.encode(request.clone(), &crate::bootstrap::wire_bytes(&body)?)?,
        &[],
        deadline,
    )?;
    let packet = connection.recv(deadline)?;
    if !packet.fds().is_empty() {
        return Err(Error::Identity);
    }
    let frame = session.validate(packet.bytes(), clock.sample()?.monotonic.nanos())?;
    if frame.envelope.opcode != 49
        || frame.envelope.flags != 10
        || frame.envelope.request_id != id
        || frame.envelope.expected_state_version != 1
        || tickwire_ipc::StatusBody::decode(frame.body)?
            != applied_status(&body, id, session.epoch())
    {
        return Err(Error::Identity);
    }
    Ok(())
}
/// The executable path is obtained from the running process, not from a claimed
/// build hash. The parent still rechecks Registry ownership and authenticated IPC.
pub fn start(
    job: &Job,
    config: &Bootstrap,
    effective_bytes: &[u8],
) -> Result<(Running, Completed)> {
    if config.factory.is_some() {
        return Err(Error::NotReady);
    }
    start_with_factory(job, config, effective_bytes, |target, _| {
        Ok(RunningFactory(target.command.is_none()))
    })
}
/// The registered loader sees only files already bound to the target release.
#[allow(unsafe_code)]
pub fn start_with_factory<F: restore::RestoreFactory>(
    job: &Job,
    config: &Bootstrap,
    effective_bytes: &[u8],
    load: impl FnOnce(&restore::Target, &files::LoadedFiles) -> Result<F>,
) -> Result<(F::Running, Completed)> {
    if job.schema != 1
        || !job.output_directory.is_absolute()
        || job.output_directory == config.command_directory
        || job.output_directory == config.report_directory
    {
        return Err(Error::Identity);
    }
    job.effective_bootstrap
        .verify(effective_bytes, 2 * 1024 * 1024)?;
    let parsed: Bootstrap = serde_json::from_slice(effective_bytes)?;
    if !same(&parsed, config)? {
        return Err(Error::Identity);
    }
    let bundle = job.bundle.load(&job.operation)?;
    let release = bundle.verify_operation(&job.operation, &job.member)?;
    let loaded = files::load(&release.target.files, &job.target_files)?;
    let rollback = files::load(&release.rollback.files, &job.rollback_files)?;
    let template: Bootstrap =
        serde_json::from_slice(loaded.bytes(&Role::Bootstrap).ok_or(Error::Identity)?)?;
    verify_bootstrap(&template, config)?;
    if std::fs::canonicalize(std::env::current_exe()?)?
        != std::fs::canonicalize(loaded.path(&Role::Binary).ok_or(Error::Identity)?)?
    {
        return Err(Error::Identity);
    }
    if job.source.proof.object != release.migration.source_proof
        || job.source.checkpoint.object != release.migration.source_checkpoint
    {
        return Err(Error::Identity);
    }
    let proof = job.source.proof.read()?;
    let checkpoint = job.source.checkpoint.read()?;
    let inputs = job.source.inputs.read()?;
    let account = job.source.account_inputs.read()?;
    let input = restore::VerifiedRestoreInput::verify(
        &release,
        target(config, &loaded)?,
        &proof,
        &checkpoint,
        &inputs,
        &account,
    )?;
    loaded.recheck()?;
    rollback.recheck()?;
    let factory = load(input.target(), &loaded)?;
    let restored = restore::execute(&factory, &input)?;
    let state = restored.checkpoint().to_vec();
    let strategy = restored.into_strategy();
    let receipt = tickwire_release::receipt::Receipt {
        schema: 1,
        kind: tickwire_release::receipt::Kind::Restore,
        roots: bundle.roots(),
        member: job.member.clone(),
        logical_strategy: release.logical_strategy(),
        process: tickwire_release::receipt::Process {
            instance: config.market.local.instance,
            generation: config.market.local.generation,
            pid: std::process::id(),
            // SAFETY: these process credential queries have no preconditions.
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
        },
        binary_digest: loaded.object(&Role::Binary).ok_or(Error::Identity)?.digest,
        effective_bootstrap_digest: tickwire_recovery::digest(effective_bytes),
        source_proof: release.migration.source_proof.digest,
        source_checkpoint: release.migration.source_checkpoint.digest,
        source_schema: release.migration.source_schema,
        method: release.migration.method.clone(),
        output_schema: release.target.capabilities.state_schema,
        output: Object::of(&state),
        source_report_position: input.source_report_position(),
    };
    receipt.verify(&release, None)?;
    let receipt_bytes = receipt.bytes()?;
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&job.output_directory)
        .or_else(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        })?;
    let directory = std::fs::symlink_metadata(&job.output_directory)?;
    if !directory.is_dir()
        || directory.mode() & 0o077 != 0
        || directory.uid() != receipt.process.uid
    {
        return Err(Error::Identity);
    }
    std::fs::File::open(job.output_directory.parent().ok_or(Error::Identity)?)?.sync_all()?;
    crate::recovery::object(&job.output_directory, &state)?;
    crate::recovery::object(&job.output_directory, &receipt_bytes)?;
    Ok((
        strategy,
        Completed {
            receipt,
            receipt_bytes,
        },
    ))
}
