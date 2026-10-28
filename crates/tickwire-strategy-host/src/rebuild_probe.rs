//! Owns an isolated fixed-old-binary probe. No market/report/intent resources or
//! trading authority are inherited. Only a real successful child exit completes it.
use super::*;
use tickwire_release::{Object, Role, rebuild::rollback::Probe};
use tickwire_strategy_sdk::replay::rebuild::probe::{self, Job, ReadBack};

pub struct Completed {
    result: ReadBack,
    exit: tickwire_supervisor::ExitObservation,
    completion: Object,
    completion_bytes: Vec<u8>,
}
impl Completed {
    pub fn result(&self) -> &ReadBack {
        &self.result
    }
    pub fn exit(&self) -> &tickwire_supervisor::ExitObservation {
        &self.exit
    }
    pub fn completion(&self) -> Object {
        self.completion
    }
    pub fn objects(&self) -> impl Iterator<Item = &[u8]> {
        self.result
            .objects()
            .chain(std::iter::once(self.completion_bytes.as_slice()))
    }
}
/// `directory` is the control owner's durable private attempt registry, retained
/// across restart. The fixed probe instance/generation cannot be launched twice.
pub fn run(spec: &LaunchSpec, directory: &Path, job: &Job, deadline: Instant) -> Result<Completed> {
    run_selected(spec, directory, job, deadline, None)
}
/// Executes the probe with the explicitly approved rollback resource group.
pub fn run_with_resources(
    spec: &LaunchSpec,
    directory: &Path,
    job: &Job,
    deadline: Instant,
    resources: &ProcessResources,
) -> Result<Completed> {
    run_selected(spec, directory, job, deadline, Some(resources))
}
pub(crate) fn run_selected(
    spec: &LaunchSpec,
    directory: &Path,
    job: &Job,
    deadline: Instant,
    resources: Option<&ProcessResources>,
) -> Result<Completed> {
    resources::validate(resources)?;
    use std::os::unix::fs::MetadataExt;
    let qualified = probe::qualify(job)?;
    let plan = qualified.input().plan();
    let binary = plan
        .files
        .iter()
        .find(|f| f.role == Role::Binary)
        .ok_or(Error::Invalid)?
        .object;
    if spec.identity.instance != plan.probe.instance
        || spec.identity.generation != plan.probe.generation
        || spec.identity.logical_strategy != job.operation.logical_strategy_id
        || spec.identity.build_hash != binary.digest
        || spec.identity.role != 3
        || spec.identity.host_boot != qualified.input().forward_inputs().clock.boot_id
        || qualified.files().path(&Role::Binary) != Some(spec.executable.as_path())
        || !spec.arguments.is_empty()
        || !spec.environment.is_empty()
        || Instant::now() >= deadline
    {
        return Err(Error::Invalid);
    }
    let probe_identity = plan.probe.clone();
    let meta = std::fs::symlink_metadata(directory)?;
    if !meta.is_dir() || meta.mode() & 0o022 != 0 {
        return Err(Error::Invalid);
    }
    let job_bytes = job.bytes()?;
    let name = probe_identity
        .instance
        .iter()
        .map(|v| format!("{v:02x}"))
        .collect::<String>();
    let selection = directory.join(format!(
        "rebuild-probe-{name}-{}.json",
        probe_identity.generation
    ));
    // Persist before spawn; an uncertain attempt is never retried as fresh work.
    let mut record = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&selection)?;
    record.write_all(&job_bytes)?;
    record.sync_all()?;
    File::open(directory)?.sync_all()?;
    let mut registry = Registry::new(1)?;
    let process = resources::spawn(
        &mut registry,
        LaunchSpec {
            identity: spec.identity.clone(),
            executable: spec.executable.clone(),
            arguments: vec![
                "--rebuild-rollback".into(),
                selection.to_str().ok_or(Error::Invalid)?.into(),
            ],
            working_directory: directory.to_owned(),
            environment: Default::default(),
            uid: spec.uid,
            gid: spec.gid,
            inherit_output: spec.inherit_output,
        },
        resources,
        resources::Purpose::Rollback,
    )?;
    let exit = await_exit(&mut registry, process.pid, &probe_identity, deadline)?;
    let result = probe::read_result(job, qualified, process.pid)?;
    if Instant::now() >= deadline {
        return Err(Error::NotReady);
    }
    // Only this actual exit observation, coupled to the exact immutable job,
    // can produce Completed. Read-only receipt verification cannot construct it.
    let body = tickwire_release::rebuild::probe::Completion {
        schema: 1,
        job: Object::of(&job_bytes),
        receipt: Object::of(&result.receipt().bytes()?),
        pid: exit.pid(),
        uid: process.uid,
        gid: process.gid,
        instance: exit.instance(),
        generation: exit.generation(),
        success: true,
    }
    .bytes()?;
    preflight::persist_object(directory, &body)?;
    Ok(Completed {
        result,
        exit,
        completion: Object::of(&body),
        completion_bytes: body,
    })
}

fn await_exit(
    registry: &mut Registry,
    pid: u32,
    probe: &Probe,
    deadline: Instant,
) -> Result<tickwire_supervisor::ExitObservation> {
    loop {
        if Instant::now() >= deadline {
            return Err(Error::NotReady);
        }
        if let Some(exit) = registry.observe_exits()?.into_iter().next() {
            if exit.instance() != probe.instance
                || exit.generation() != probe.generation
                || exit.pid() != pid
                || !exit.status().success()
            {
                return Err(Error::Invalid);
            }
            return Ok(exit);
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn spawn(code: &str) -> (Registry, ProcessIdentity, Probe) {
        // Some Linux distributions expose /bin/sh as a symlink. Registry
        // deliberately requires a regular executable, including in this test.
        let executable = std::fs::canonicalize("/bin/sh").unwrap();
        let hash = tickwire_recovery::digest(&std::fs::read(&executable).unwrap());
        let identity = Identity {
            instance: [7; 16],
            generation: 3,
            logical_strategy: 8,
            role: 3,
            arch: 1,
            trust_profile: 1,
            host_boot: [1; 16],
            namespace: [2; 16],
            build_hash: hash,
            qualification_hash: [3; 32],
            manifest_hash: [4; 32],
        };
        let mut registry = Registry::new(1).unwrap();
        // The test uses the current process UID/GID without introducing unsafe code.
        use std::os::unix::fs::MetadataExt;
        let owner = std::fs::metadata(std::env::current_exe().unwrap()).unwrap();
        let process = registry
            .spawn(LaunchSpec {
                identity,
                executable,
                arguments: vec!["-c".into(), code.into()],
                working_directory: std::env::current_dir().unwrap(),
                environment: Default::default(),
                uid: owner.uid(),
                gid: owner.gid(),
                inherit_output: false,
            })
            .unwrap();
        (
            registry,
            process,
            Probe {
                instance: [7; 16],
                generation: 3,
                input_stream: [8; 16],
            },
        )
    }
    #[test]
    fn probe_completion_requires_real_owned_successful_exit() {
        let (mut registry, p, probe) = spawn("exit 0");
        let result = await_exit(
            &mut registry,
            p.pid,
            &probe,
            Instant::now() + Duration::from_secs(2),
        );
        assert!(
            result.is_ok(),
            "owned successful child was not observed: {:?}",
            result.as_ref().err()
        );
        let exit = result.unwrap();
        assert_eq!(exit.pid(), p.pid);
        assert!(exit.status().success());
        let (mut registry, p, probe) = spawn("exit 7");
        assert!(
            await_exit(
                &mut registry,
                p.pid,
                &probe,
                Instant::now() + Duration::from_secs(2)
            )
            .is_err()
        );
        let (mut registry, p, mut probe) = spawn("exit 0");
        probe.generation += 1;
        assert!(
            await_exit(
                &mut registry,
                p.pid,
                &probe,
                Instant::now() + Duration::from_secs(2)
            )
            .is_err()
        );
        let (mut registry, p, probe) = spawn("sleep 2");
        assert!(await_exit(&mut registry, p.pid, &probe, Instant::now()).is_err());
        let exit = registry.terminate(probe.instance).unwrap();
        assert_eq!(exit.pid(), p.pid);
        assert!(!exit.status().success());
    }
}
