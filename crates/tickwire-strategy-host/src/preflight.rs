//! Host-owned completion: files + authenticated Registry child + actual probe exit.
use super::*;
use tickwire_release::{Object, Role, files, receipt::Receipt};
use tickwire_strategy_sdk::preflight::{Job, ObjectFile, ProbeJob};

pub(super) fn object_file(directory: &Path, object: Object) -> ObjectFile {
    ObjectFile {
        path: directory.join(format!(
            "{}.bin",
            object
                .digest
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        )),
        object,
    }
}
/// Finalize only fields derived by Host. The signed Bootstrap template is checked
/// before any child is created; an unbound template cannot become a runtime job.
pub(crate) fn prepare(config: &Bootstrap, spec: &LaunchSpec) -> Result<()> {
    let Some(path) = &config.preflight_job else {
        return Ok(());
    };
    let mut job: Job = worker::read_config(path, 2 * 1024 * 1024)?;
    let bundle = job.bundle.load(&job.operation)?;
    let release = bundle.verify_operation(&job.operation, &job.member)?;
    let loaded = files::load(&release.target.files, &job.target_files)?;
    let rollback = files::load(&release.rollback.files, &job.rollback_files)?;
    job.source.objects()?;
    let template: Bootstrap =
        serde_json::from_slice(loaded.bytes(&Role::Bootstrap).ok_or(Error::Invalid)?)
            .map_err(|_| Error::Invalid)?;
    tickwire_strategy_sdk::preflight::verify_bootstrap(&template, config)?;
    tickwire_strategy_sdk::preflight::verify_recovery_inputs(
        config.recovery_template.as_deref(),
        &loaded,
    )?;
    let rollback_template: Bootstrap =
        serde_json::from_slice(rollback.bytes(&Role::Bootstrap).ok_or(Error::Invalid)?)
            .map_err(|_| Error::Invalid)?;
    tickwire_strategy_sdk::preflight::verify_recovery_inputs(
        rollback_template.recovery_template.as_deref(),
        &rollback,
    )?;
    if spec.identity != config.report.local.identity()
        || spec.identity.instance != job.member.new_instance
        || spec.identity.generation != job.member.new_generation
        || spec.identity.logical_strategy != job.operation.logical_strategy_id
        || spec.identity.build_hash != loaded.object(&Role::Binary).ok_or(Error::Invalid)?.digest
        || std::fs::canonicalize(&spec.executable)?
            != std::fs::canonicalize(loaded.path(&Role::Binary).ok_or(Error::Invalid)?)?
    {
        return Err(Error::Invalid);
    }
    loaded.recheck()?;
    rollback.recheck()?;
    job.effective_bootstrap = Object::of(&serde_json::to_vec(config).map_err(|_| Error::Invalid)?);
    write_json(path, &job)
}

pub(crate) struct Proof {
    resources: Option<ProcessResources>,
    job: Job,
    bootstrap: Bootstrap,
    primary: Option<(Receipt, Vec<u8>)>,
    completed: Option<Vec<Vec<u8>>>,
}
impl Proof {
    pub(super) fn installed_probe(
        &self,
        installed: &tickwire_runtime::node::InstalledReplay,
    ) -> Result<tickwire_release::replay::rollback::Verified> {
        use tickwire_release::replay::rollback::{
            Completion as ReplayCompletion, Job as ReplayJob, Receipt as ReplayReceipt,
        };
        if self.job.operation.schema != 3
            || self.job.operation != installed.verified().request.operation
            || self.job.member != installed.verified().request.member
        {
            return Err(Error::Invalid);
        }
        let bundle = self.job.bundle.load(&self.job.operation)?;
        let release = bundle.verify_operation(&self.job.operation, &self.job.member)?;
        let files = files::load(&release.rollback.files, &self.job.rollback_files)?;
        let bootstrap: Bootstrap =
            serde_json::from_slice(files.bytes(&Role::Bootstrap).ok_or(Error::Invalid)?)
                .map_err(|_| Error::Invalid)?;
        tickwire_strategy_sdk::preflight::verify_recovery_inputs(
            bootstrap.recovery_template.as_deref(),
            &files,
        )?;
        let identity = bootstrap.report.local.identity();
        if identity != bootstrap.market.local.identity()
            || identity.logical_strategy != self.job.operation.logical_strategy_id
            || [self.job.member.new_instance, self.job.member.old_instance]
                .contains(&identity.instance)
            || identity.build_hash != files.object(&Role::Binary).ok_or(Error::Invalid)?.digest
        {
            return Err(Error::Invalid);
        }
        let tag = identity
            .instance
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let completed_path = self
            .job
            .output_directory
            .join(format!("C-PROBE-{tag}.json"));
        if let Ok(meta) = std::fs::symlink_metadata(&completed_path) {
            if !meta.is_file() || meta.len() > tickwire_release::MAX_DOCUMENT as u64 {
                return Err(Error::Invalid);
            }
            let bytes = std::fs::read(&completed_path)?;
            let completion = ReplayCompletion::decode(&bytes)?;
            if completion.installation != installed.root() {
                return Err(Error::Invalid);
            }
            File::open(&completed_path)?.sync_all()?;
            File::open(&self.job.output_directory)?.sync_all()?;
            return Ok(tickwire_release::replay::rollback::verify(
                Object::of(&bytes).digest,
                |h| read_probe_object(&self.job.output_directory, h),
            )?);
        }
        let parent = self.job.output_directory.parent().ok_or(Error::Invalid)?;
        let directory = PrivateDirectory::create_in(parent, "c-rollback")?;
        let mut objects = Vec::new();
        for bytes in &installed.verified().objects {
            persist_object(&self.job.output_directory, bytes)?;
            objects.push(object_file(&self.job.output_directory, Object::of(bytes)));
        }
        objects.sort_by_key(|f| f.object.digest);
        objects.dedup_by_key(|f| f.object.digest);
        let job = ReplayJob {
            schema: 1,
            installation: installed.root(),
            objects,
            rollback_files: self.job.rollback_files.clone(),
            output_directory: directory.path().join("output"),
        };
        let bytes = job.bytes()?;
        persist_object(&self.job.output_directory, &bytes)?;
        let job_file = object_file(&self.job.output_directory, Object::of(&bytes));
        files.recheck()?;
        // Persist an attempt before spawn. An incomplete attempt is uncertainty,
        // never permission to reuse this fixed probe identity after a restart.
        let marker = self
            .job
            .output_directory
            .join(format!("C-PROBE-{tag}.started"));
        let mut attempt = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(marker)?;
        attempt.write_all(&bytes)?;
        attempt.sync_all()?;
        File::open(&self.job.output_directory)?.sync_all()?;
        let mut registry = Registry::new(1)?;
        let child = resources::spawn(
            &mut registry,
            LaunchSpec {
                identity: identity.clone(),
                executable: files.path(&Role::Binary).ok_or(Error::Invalid)?.to_owned(),
                arguments: vec![
                    "--replay-rollback".into(),
                    job_file.path.to_str().ok_or(Error::Invalid)?.into(),
                ],
                working_directory: directory.path().to_owned(),
                environment: Default::default(),
                uid: self.primary.as_ref().ok_or(Error::Invalid)?.0.process.uid,
                gid: self.primary.as_ref().ok_or(Error::Invalid)?.0.process.gid,
                inherit_output: true,
            },
            self.resources.as_ref(),
            resources::Purpose::Rollback,
        )?;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(exit) = registry
                .observe_exits()?
                .into_iter()
                .find(|e| e.instance() == identity.instance)
            {
                if exit.pid() != child.pid || !exit.status().success() {
                    return Err(Error::Invalid);
                }
                break;
            }
            if Instant::now() >= deadline {
                return Err(Error::NotReady);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let path = job.output_directory.join("ROLLBACK_REPLAY.json");
        let meta = std::fs::symlink_metadata(&path)?;
        if !meta.is_file() || meta.len() > tickwire_release::MAX_DOCUMENT as u64 {
            return Err(Error::Invalid);
        }
        let bytes = std::fs::read(path)?;
        if bytes.len() as u64 != meta.len() {
            return Err(Error::Invalid);
        }
        let receipt = ReplayReceipt::decode(&bytes)?;
        receipt.verify(installed.verified())?;
        if receipt.job != job_file.object
            || receipt.process.pid != child.pid
            || receipt.process.uid != child.uid
            || receipt.process.gid != child.gid
            || receipt.process.instance != identity.instance
            || receipt.process.generation != identity.generation
        {
            return Err(Error::Invalid);
        }
        let input = object_file(&job.output_directory, receipt.input_state).read()?;
        let output = object_file(&job.output_directory, receipt.output).read()?;
        for value in [&bytes, &input, &output] {
            persist_object(&self.job.output_directory, value)?;
        }
        let completion = ReplayCompletion {
            schema: 1,
            installation: installed.root(),
            rollback: Object::of(&bytes),
        };
        let bytes = completion.bytes()?;
        persist_object(&self.job.output_directory, &bytes)?;
        let verified =
            tickwire_release::replay::rollback::verify(Object::of(&bytes).digest, |h| {
                read_probe_object(&self.job.output_directory, h)
            })?;
        // Atomic named publication is safe here: all referenced objects and the
        // successful Registry exit are already fixed; no second process runs.
        let mut f = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&completed_path)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        File::open(&self.job.output_directory)?.sync_all()?;
        Ok(verified)
    }
    pub(super) fn job(&self) -> &Job {
        &self.job
    }
    pub fn new(config: &Bootstrap, resources: Option<ProcessResources>) -> Result<Option<Self>> {
        resources::validate(resources.as_ref())?;
        let Some(path) = &config.preflight_job else {
            return Ok(None);
        };
        let job: Job = worker::read_config(path, 2 * 1024 * 1024)?;
        job.effective_bootstrap.verify(
            &serde_json::to_vec(config).map_err(|_| Error::Invalid)?,
            2 * 1024 * 1024,
        )?;
        Ok(Some(Self {
            resources,
            job,
            bootstrap: config.clone(),
            primary: None,
            completed: None,
        }))
    }
    pub fn observe(
        &mut self,
        envelope: &ControlEnvelope,
        raw: &[u8],
        lane: &mut AuthenticatedChild,
    ) -> Result<()> {
        let body = tickwire_ipc::RestoreAppliedBody::decode(raw)?;
        let expected = &self.bootstrap.report.local;
        if envelope.opcode != 38
            || envelope.flags != 32
            || envelope.request_id != expected.instance
            || envelope.expected_state_version != 0
            || body.schema != 1
            || body.completion_kind != 1
            || body.receipt_sequence != 1
            || body.instance_id != expected.instance
            || body.strategy_generation != expected.generation
            || body.logical_strategy_id != expected.logical_strategy
            || lane.session.peer() != &expected.identity()
        {
            return Err(Error::Invalid);
        }
        let bytes = object_file(
            &self.job.output_directory,
            Object {
                digest: body.receipt_digest,
                bytes: body.receipt_bytes,
            },
        )
        .read()?;
        let receipt = Receipt::decode(&bytes)?;
        let bundle = self.job.bundle.load(&self.job.operation)?;
        let release = bundle.verify_operation(&self.job.operation, &self.job.member)?;
        receipt.verify(&release, None)?;
        verify_process(&receipt, lane)?;
        if receipt.effective_bootstrap_digest != self.job.effective_bootstrap.digest
            || body.release_manifest_digest != receipt.roots[0]
        {
            return Err(Error::Invalid);
        }
        let proof = tickwire_recovery::HandoverSeal::decode(&self.job.source.proof.read()?)?;
        if receipt.source_report_position != proof.report_position {
            return Err(Error::Invalid);
        }
        object_file(&self.job.output_directory, receipt.output).read()?;
        if let Some((previous, previous_bytes)) = &self.primary
            && (previous != &receipt || previous_bytes != &bytes)
        {
            return Err(Error::Invalid);
        }
        self.primary = Some((receipt.clone(), bytes.clone()));
        let objects = if self.job.operation.schema == 3 {
            self.startup_objects(&receipt, &bytes)?
        } else {
            self.objects(bundle.roots())?.ok_or(Error::NotReady)?
        };
        let completion = objects.first().ok_or(Error::Invalid)?;
        let status = tickwire_ipc::StatusBody {
            subject_id: body.instance_id,
            original_request_id: envelope.request_id,
            original_control_epoch: envelope.control_epoch,
            state_version: body.receipt_sequence,
            state: 3,
            result_hash: tickwire_recovery::digest(completion),
            ..Default::default()
        };
        let reply = lane.session.encode(
            ControlEnvelope {
                opcode: 49,
                flags: 10,
                request_id: envelope.request_id,
                expected_state_version: envelope.expected_state_version,
                ..Default::default()
            },
            &wire_bytes(&status)?,
        )?;
        lane.connection
            .send(&reply, &[], Instant::now() + Duration::from_secs(1))?;
        Ok(())
    }
    pub fn objects(&mut self, roots: [[u8; 32]; 3]) -> Result<Option<Vec<Vec<u8>>>> {
        if self.job.operation.schema == 3 {
            return Ok(None);
        }
        if roots
            != [
                self.job.operation.artifacts_digest,
                self.job.operation.checkpoint_digest,
                self.job.operation.rollback_digest,
            ]
        {
            return Ok(None);
        }
        if let Some(objects) = &self.completed {
            return Ok(Some(objects.clone()));
        }
        let Some((primary, primary_bytes)) = self.primary.clone() else {
            return Ok(None);
        };
        let objects = self.probe(&primary, &primary_bytes)?;
        self.completed = Some(objects.clone());
        Ok(Some(objects))
    }
    fn startup_objects(&self, receipt: &Receipt, bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
        let completion = tickwire_release::replay::startup::Completion {
            schema: 1,
            operation: self.job.operation.clone(),
            member: self.job.member.clone(),
            restored: Object::of(bytes),
        };
        let mut objects = vec![
            completion.bytes()?,
            self.job.bundle.manifest.read()?,
            self.job.bundle.migration.read()?,
            self.job.bundle.rollback.read()?,
            bytes.to_vec(),
            object_file(&self.job.output_directory, receipt.output).read()?,
        ];
        objects.extend(self.job.source.objects()?);
        for bytes in &objects {
            persist_object(&self.job.output_directory, bytes)?;
        }
        Ok(objects)
    }
    fn probe(&self, primary: &Receipt, primary_bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
        let bundle = self.job.bundle.load(&self.job.operation)?;
        let release = bundle.verify_operation(&self.job.operation, &self.job.member)?;
        let files = files::load(&release.rollback.files, &self.job.rollback_files)?;
        let rollback: Bootstrap =
            serde_json::from_slice(files.bytes(&Role::Bootstrap).ok_or(Error::Invalid)?)
                .map_err(|_| Error::Invalid)?;
        let id = rollback.report.local.identity();
        if id != rollback.market.local.identity()
            || id.instance == primary.process.instance
            || id.instance == release.target.member.old_instance
            || id.logical_strategy != primary.logical_strategy
            || id.role != 3
            || id.build_hash != files.object(&Role::Binary).ok_or(Error::Invalid)?.digest
            || rollback.report.peer.identity() != self.bootstrap.report.peer.identity()
        {
            return Err(Error::Invalid);
        }
        let parent = self.job.output_directory.parent().ok_or(Error::Invalid)?;
        let directory = PrivateDirectory::create_in(parent, "tickwire-rollback")?;
        let mut endpoint = rollback.report.clone();
        endpoint.path = directory.path().join("probe.sock");
        endpoint.peer_pid = self.bootstrap.report.peer_pid;
        endpoint.peer_uid = self.bootstrap.report.peer_uid;
        endpoint.peer_gid = self.bootstrap.report.peer_gid;
        let listener = ControlListener::bind(&endpoint.path)?;
        let effective_path = directory.path().join("target.json");
        write_json(&effective_path, &self.bootstrap)?;
        let output_directory = directory.path().join("output");
        let job = ProbeJob {
            primary: self.job.clone(),
            target_bootstrap: ObjectFile {
                path: effective_path,
                object: self.job.effective_bootstrap,
            },
            parent_receipt: object_file(&self.job.output_directory, Object::of(primary_bytes)),
            target_output: object_file(&self.job.output_directory, primary.output),
            endpoint: endpoint.clone(),
            output_directory: output_directory.clone(),
        };
        let job_path = directory.path().join("probe.json");
        write_json(&job_path, &job)?;
        files.recheck()?;
        let mut registry = Registry::new(1)?;
        let child = resources::spawn(
            &mut registry,
            LaunchSpec {
                identity: id.clone(),
                executable: files.path(&Role::Binary).ok_or(Error::Invalid)?.to_owned(),
                arguments: vec![
                    "--restore-probe".into(),
                    job_path.to_str().ok_or(Error::Invalid)?.into(),
                ],
                working_directory: directory.path().to_owned(),
                environment: Default::default(),
                uid: primary.process.uid,
                gid: primary.process.gid,
                inherit_output: true,
            },
            self.resources.as_ref(),
            resources::Purpose::Rollback,
        )?;
        let mut clock = SystemClock::new(
            tickwire_clock::ClockDomain::new(id.host_boot, self.bootstrap.clock_generation)
                .map_err(|_| Error::Invalid)?,
        )?;
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut lane = registry.accept(
            &listener,
            server_policy(&endpoint, child),
            clock.sample()?.monotonic.nanos(),
            deadline,
        )?;
        let packet = lane.connection.recv(deadline)?;
        if !packet.fds().is_empty() {
            return Err(Error::Invalid);
        }
        let frame = lane
            .session
            .validate(packet.bytes(), clock.sample()?.monotonic.nanos())?;
        let envelope = frame.envelope.clone();
        let body = tickwire_ipc::RestoreAppliedBody::decode(frame.body)?;
        if envelope.opcode != 38
            || envelope.flags != 32
            || envelope.expected_state_version != 1
            || envelope.request_id != body.receipt_digest[..16]
            || body.receipt_sequence != 1
            || body.schema != 1
            || body.completion_kind != 2
            || body.instance_id != id.instance
            || body.strategy_generation != id.generation
            || body.logical_strategy_id != id.logical_strategy
            || body.release_manifest_digest != primary.roots[0]
        {
            return Err(Error::Invalid);
        }
        let bytes = object_file(
            &output_directory,
            Object {
                digest: body.receipt_digest,
                bytes: body.receipt_bytes,
            },
        )
        .read()?;
        let receipt = Receipt::decode(&bytes)?;
        receipt.verify(&release, Some(primary))?;
        verify_process(&receipt, &lane)?;
        if receipt.effective_bootstrap_digest
            != files.object(&Role::Bootstrap).ok_or(Error::Invalid)?.digest
        {
            return Err(Error::Invalid);
        }
        let output = object_file(&output_directory, receipt.output).read()?;
        let reply = lane.session.encode(
            ControlEnvelope {
                opcode: 49,
                flags: 10,
                request_id: envelope.request_id,
                expected_state_version: 1,
                ..Default::default()
            },
            &wire_bytes(&tickwire_strategy_sdk::preflight::applied_status(
                &body,
                envelope.request_id,
                envelope.control_epoch,
            ))?,
        )?;
        lane.connection.send(&reply, &[], deadline)?;
        loop {
            if let Some(exit) = registry
                .observe_exits()?
                .into_iter()
                .find(|e| e.instance() == id.instance)
            {
                if !exit.status().success() {
                    return Err(Error::Invalid);
                }
                break;
            }
            if Instant::now() >= deadline {
                return Err(Error::NotReady);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        // Persist probe evidence outside the temporary process directory before
        // treating completion as reusable by this live Host.
        for value in [&bytes, &output] {
            persist_object(&self.job.output_directory, value)?;
        }
        let completion = tickwire_release::receipt::Completion {
            schema: 1,
            operation: self.job.operation.clone(),
            roots: bundle.roots(),
            member: self.job.member.clone(),
            logical_strategy: primary.logical_strategy,
            restored: Object::of(primary_bytes),
            rollback: Object::of(&bytes),
        };
        completion.verify(&release, primary_bytes, &bytes)?;
        let completion = completion.bytes()?;
        persist_object(&self.job.output_directory, &completion)?;
        let mut objects = vec![
            completion,
            self.job.bundle.manifest.read()?,
            self.job.bundle.migration.read()?,
            self.job.bundle.rollback.read()?,
            primary_bytes.to_vec(),
            object_file(&self.job.output_directory, primary.output).read()?,
            bytes,
            output,
        ];
        objects.extend(self.job.source.objects()?);
        Ok(objects)
    }
}
fn verify_process(receipt: &Receipt, lane: &AuthenticatedChild) -> Result<()> {
    if receipt.process.instance != lane.session.peer().instance
        || receipt.process.generation != lane.session.peer().generation
        || receipt.process.pid != lane.process.pid
        || receipt.process.uid != lane.process.uid
        || receipt.process.gid != lane.process.gid
        || receipt.binary_digest != lane.session.peer().build_hash
    {
        return Err(Error::Invalid);
    }
    Ok(())
}
fn read_probe_object(directory: &Path, h: [u8; 32]) -> tickwire_release::Result<Vec<u8>> {
    let path = directory.join(format!(
        "{}.bin",
        h.iter().map(|b| format!("{b:02x}")).collect::<String>()
    ));
    let meta = std::fs::symlink_metadata(&path).map_err(|_| tickwire_release::Error::Missing)?;
    if !meta.is_file() || meta.len() > tickwire_recovery::MAX_OBJECT_BYTES as u64 {
        return Err(tickwire_release::Error::Bounds);
    }
    object_file(
        directory,
        Object {
            digest: h,
            bytes: meta.len(),
        },
    )
    .read()
}
pub(super) fn persist_object(directory: &Path, bytes: &[u8]) -> Result<()> {
    persist_object_with_sync(directory, bytes, &mut File::sync_all)
}
fn persist_object_with_sync(
    directory: &Path,
    bytes: &[u8],
    sync: &mut impl FnMut(&File) -> std::io::Result<()>,
) -> Result<()> {
    let path = object_file(directory, Object::of(bytes)).path;
    let file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
    {
        Ok(mut f) => {
            f.write_all(bytes)?;
            f
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            object_file(directory, Object::of(bytes)).read()?;
            File::open(&path)?
        }
        Err(e) => return Err(e.into()),
    };
    // A previous file-sync failure may leave identical readable bytes. Establish
    // the file completion point on both paths before publishing its directory.
    sync(&file)?;
    sync(&File::open(directory)?)?;
    Ok(())
}

#[cfg(test)]
mod persistence_tests {
    use super::*;
    #[test]
    fn probe_dependency_reader_uses_recovery_object_budget_and_checks_digest() {
        struct Directory(std::path::PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let directory = Directory(std::env::temp_dir().join(format!(
            "tickwire-probe-budget-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory.0)
            .unwrap();
        let bytes = vec![37; tickwire_release::replay::MAX_BYTES + 1];
        let object = Object::of(&bytes);
        persist_object(&directory.0, &bytes).unwrap();
        assert_eq!(
            read_probe_object(&directory.0, object.digest).unwrap(),
            bytes
        );
        let path = object_file(&directory.0, object).path;
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all(&[38])
            .unwrap();
        assert_eq!(
            read_probe_object(&directory.0, object.digest),
            Err(tickwire_release::Error::Identity)
        );
        OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_len(tickwire_recovery::MAX_OBJECT_BYTES as u64 + 1)
            .unwrap();
        assert_eq!(
            read_probe_object(&directory.0, object.digest),
            Err(tickwire_release::Error::Bounds)
        );
    }

    #[test]
    fn failed_object_sync_requires_a_real_file_sync_again_before_directory_sync() {
        struct Directory(std::path::PathBuf);
        impl Directory {
            fn path(&self) -> &Path {
                &self.0
            }
        }
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let path = std::env::temp_dir().join(format!(
            "tickwire-preflight-sync-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        let directory = Directory(path);
        let bytes = b"actual preflight completion bytes";
        let path = object_file(directory.path(), Object::of(bytes)).path;
        let mut calls = Vec::new();
        let mut fail_file = |file: &File| {
            let regular = file.metadata()?.is_file();
            calls.push(regular);
            if regular {
                Err(std::io::Error::other("injected file-sync failure"))
            } else {
                Ok(())
            }
        };
        assert!(persist_object_with_sync(directory.path(), bytes, &mut fail_file).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert!(
            persist_object_with_sync(directory.path(), bytes, &mut fail_file).is_err(),
            "visible identical bytes after a failed file sync do not establish durability"
        );
        assert_eq!(calls, [true, true]);
        let mut order = Vec::new();
        persist_object_with_sync(directory.path(), bytes, &mut |file| {
            order.push(file.metadata()?.is_file());
            file.sync_all()
        })
        .unwrap();
        assert_eq!(order, [true, false]);
        std::fs::write(&path, b"corrupt retained completion").unwrap();
        let mut synced = false;
        assert!(
            persist_object_with_sync(directory.path(), bytes, &mut |_| {
                synced = true;
                Ok(())
            })
            .is_err()
        );
        assert!(!synced);
    }
}
