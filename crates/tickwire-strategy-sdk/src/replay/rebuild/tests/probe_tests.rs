use super::*;
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};
use tickwire_release::{
    Role,
    files::{Location, ObjectFile},
    rebuild::rollback as policy,
};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let p = std::env::temp_dir().join(format!(
            "tickwire-rebuild-probe-{}-{}",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self(p)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn object(dir: &Path, b: &[u8]) -> ObjectFile {
    let (path, digest) = crate::recovery::object(dir, b).unwrap();
    ObjectFile {
        path,
        object: Object {
            digest,
            bytes: b.len() as u64,
        },
    }
}
fn job(dir: &Path) -> probe::Job {
    let mut f = Fixture::new();
    let binary = std::env::current_exe().unwrap();
    let binary_object = Object::of(&std::fs::read(&binary).unwrap());
    f.contract.binary = binary_object.digest;
    f.plans.members[0].target.binary_digest = binary_object.digest;
    f.manifest.targets[0].files[0].object = binary_object;
    f.rebind();
    let files = f.manifest.targets[0].files.clone();
    let rollback = policy::Plans {
        schema: 1,
        binding: f.plans.binding.clone(),
        members: vec![policy::MemberPlan {
            member: f.op.members[0].clone(),
            version: [31; 32],
            factory: f.contract.factory.clone(),
            files,
            capabilities: f.manifest.targets[0].capabilities.clone(),
            probe: policy::Probe {
                instance: [32; 16],
                generation: 8,
                input_stream: [33; 16],
            },
            recovery: policy::Recovery::ApprovedOldRebuild {
                contract: Object::of(&f.contract.bytes().unwrap()),
                inputs: Object::of(&f.inputs.bytes().unwrap()),
                end: f.plans.members[0].end.clone(),
            },
        }],
    };
    let rollback = object(dir, &rollback.bytes().unwrap());
    f.op.rollback_digest = rollback.object.digest;
    let contract = object(dir, &f.contract.bytes().unwrap());
    let inputs = object(dir, &f.inputs.bytes().unwrap());
    probe::Job {
        schema: 1,
        attempt: [44; 16],
        operation: f.op.clone(),
        member: f.op.members[0].clone(),
        plans: object(dir, &f.plans.bytes().unwrap()),
        manifest: object(dir, &canonical(&f.manifest).unwrap()),
        contract: contract.clone(),
        inputs: inputs.clone(),
        rollback,
        objects: vec![contract, inputs],
        files: vec![
            Location {
                role: Role::Binary,
                path: binary,
            },
            Location {
                role: Role::HostConfig,
                path: object(dir, b"host").path,
            },
            Location {
                role: Role::Bootstrap,
                path: object(dir, b"bootstrap").path,
            },
            Location {
                role: Role::StrategyConfig,
                path: object(dir, b"parameters").path,
            },
        ],
        output_directory: dir.join("output"),
    }
}
struct ActualFactory(TestFactory, Hash);
impl Factory for ActualFactory {
    type Running = State;
    fn binding(&self) -> CompiledFactory {
        let mut b = self.0.binding();
        b.binary = self.1;
        b
    }
    fn begin(&self, i: &FactoryInput) -> Result<State> {
        self.0.begin(i)
    }
    fn consumed_actions(&self, b: &[u8]) -> Result<Vec<[u8; 16]>> {
        self.0.consumed_actions(b)
    }
    fn validate_checkpoint(&self, b: &[u8]) -> Result<()> {
        self.0.validate_checkpoint(b)
    }
}
fn compute(
    i: &policy::Verified,
    _: &tickwire_release::files::LoadedFiles,
) -> crate::Result<rollback::Computation> {
    let factory = ActualFactory(TestFactory::new(), live::executable_digest()?);
    Ok(rollback::prepare_declared(&factory, i)?.into())
}
#[test]
fn probe_persists_actual_old_callbacks_before_its_receipt() {
    let dir = Directory::new();
    let job = job(&dir.0);
    let receipt = probe::run_with(&job, compute);
    assert!(
        receipt.is_ok(),
        "actual probe execution: {:?}",
        receipt.as_ref().err()
    );
    let receipt = receipt.unwrap();
    let verified = probe::read_result(&job, probe::qualify(&job).unwrap(), receipt.pid).unwrap();
    assert_eq!(verified.receipt(), &receipt);
    assert!(verified.objects().count() > 5);
    assert!(probe::read_result(&job, probe::qualify(&job).unwrap(), receipt.pid + 1).is_err());
    assert_eq!(receipt.pid, std::process::id());
    assert_eq!(receipt.job, Object::of(&job.bytes().unwrap()));
    assert_eq!(receipt.cut.sequence, 2);
    let read = |o: Object| {
        let p = job.output_directory.join(format!(
            "{}.bin",
            o.digest
                .iter()
                .map(|n| format!("{n:02x}"))
                .collect::<String>()
        ));
        tickwire_release::files::read_object(&p, o).unwrap()
    };
    let checkpoint = Checkpoint::decode(&read(receipt.checkpoint)).unwrap();
    let state: State =
        serde_json::from_slice(&application::business_bytes(&checkpoint.business).unwrap())
            .unwrap();
    assert_eq!(state.observations, 1);
    assert_eq!(state.consumed, vec![[3; 16]]);
    assert_eq!(checkpoint.runtime.effects.count, 0);
    let anchor = Checkpoint::decode(&read(receipt.anchor)).unwrap();
    let anchor = verify_anchor(
        &anchor.bytes().unwrap(),
        &anchor.source,
        Object::of(&anchor.runtime.config.bytes),
        &[],
    )
    .unwrap();
    assert_eq!(
        verify_tail(&anchor, &read(receipt.transcript), &checkpoint.cut)
            .unwrap()
            .rows()
            .len(),
        2
    );
    assert_eq!(
        probe::Receipt::decode(
            &std::fs::read(job.output_directory.join("REBUILD_PROBE.json")).unwrap()
        )
        .unwrap(),
        receipt
    );
    assert!(
        probe::run_with(&job, |_, _| panic!(
            "a completed attempt must not execute twice"
        ))
        .is_err()
    );
}

#[test]
fn probe_recovery_uses_only_the_complete_saved_object_graph() {
    use tickwire_release::rebuild::probe as saved;
    let dir = Directory::new();
    let job = job(&dir.0);
    let receipt = probe::run_with(&job, compute).unwrap();
    let result = probe::read_result(&job, probe::qualify(&job).unwrap(), receipt.pid).unwrap();
    let completion = saved::Completion {
        generation: receipt.probe.generation,
        gid: 0,
        instance: receipt.probe.instance,
        job: receipt.job,
        pid: receipt.pid,
        receipt: Object::of(&receipt.bytes().unwrap()),
        schema: 1,
        success: true,
        uid: 0,
    };
    let prior = serde_json::to_vec(&serde_json::json!({"schema":1,"job":receipt.job,
        "receipt":Object::of(&receipt.bytes().unwrap()),"pid":receipt.pid,"uid":0,"gid":0,
        "instance":receipt.probe.instance,"generation":receipt.probe.generation,"success":true}))
    .unwrap();
    assert_eq!(
        completion.bytes().unwrap(),
        prior,
        "keep historical Host completion bytes"
    );
    let mut objects = result
        .objects()
        .map(|b| (Object::of(b).digest, b.to_vec()))
        .collect::<BTreeMap<_, _>>();
    let root = Object::of(&prior);
    objects.insert(root.digest, prior);
    let plans = Plans::decode(&objects[&job.plans.object.digest]).unwrap();
    let contract = Contract::decode(&objects[&job.contract.object.digest]).unwrap();
    let inputs = Inputs::decode(&objects[&job.inputs.object.digest]).unwrap();
    let manifest: ReleaseManifest =
        serde_json::from_slice(&objects[&job.manifest.object.digest]).unwrap();
    // Original input and output paths no longer exist; only immutable exported
    // bytes are available to recovery. This is content verification, not exit proof.
    std::fs::remove_dir_all(&dir.0).unwrap();
    let verify = |root, map: &BTreeMap<Hash, Vec<u8>>| {
        saved::Verified::verify(
            root,
            &job.operation,
            &job.member,
            policy::Forward {
                plans: &plans,
                contract: &contract,
                inputs: &inputs,
                manifest: &manifest,
            },
            |hash| {
                map.get(&hash)
                    .cloned()
                    .ok_or(tickwire_release::Error::Missing)
            },
        )
    };
    let verified = verify(root, &objects).unwrap();
    assert_eq!(verified.receipt(), &receipt);
    assert_eq!(verified.checkpoint(), result.checkpoint());
    assert_eq!(verified.root(), root);
    assert!(matches!(
        saved::Verified::verify_at(
            root,
            &job.operation,
            &job.member,
            policy::Forward {
                plans: &plans,
                contract: &contract,
                inputs: &inputs,
                manifest: &manifest
            },
            &mut |_| panic!("depth rejection must precede object reads"),
            16
        ),
        Err(tickwire_release::Error::Bounds)
    ));
    for hash in objects.keys() {
        let mut missing = objects.clone();
        missing.remove(hash);
        assert!(verify(root, &missing).is_err(), "missing {hash:?}");
    }
    for which in 0..5 {
        let mut bad = completion.clone();
        match which {
            0 => bad.pid += 1,
            1 => bad.generation += 1,
            2 => bad.instance = [9; 16],
            3 => bad.success = false,
            _ => bad.job = bad.receipt,
        }
        let bytes = canonical(&bad).unwrap();
        let root = Object::of(&bytes);
        let mut changed = objects.clone();
        changed.insert(root.digest, bytes);
        assert!(
            verify(root, &changed).is_err(),
            "completion substitution {which}"
        );
    }
}

#[test]
fn probe_opaque_dependencies_allow_seventeen_mib_while_typed_roots_stay_bounded() {
    let dir = Directory::new();
    let mut job = job(&dir.0);
    let large = object(&dir.0, &vec![37; 17 * 1024 * 1024]);
    job.objects.push(large.clone());
    assert!(
        probe::qualify(&job).is_ok(),
        "opaque input must retain the recovery object budget"
    );
    job.inputs = large;
    assert!(matches!(
        probe::qualify(&job),
        Err(crate::Error::Release(tickwire_release::Error::Bounds))
    ));
}

#[test]
fn probe_rejects_wrong_binding_before_callbacks_and_keeps_failed_attempt_closed() {
    let dir = Directory::new();
    let job = job(&dir.0);
    let mut wrong = job.clone();
    wrong.operation.rollback_digest = [99; 32];
    assert!(
        probe::run_with(&wrong, |_, _| panic!(
            "wrong fixed binding reached callback"
        ))
        .is_err()
    );
    assert!(!job.output_directory.exists());
    let mut oversized = job.clone();
    oversized.objects = vec![job.inputs.clone(); 508];
    assert!(
        probe::run_with(&oversized, |_, _| panic!(
            "unbounded input reached callback"
        ))
        .is_err()
    );
    assert!(!job.output_directory.exists());
    // A real filesystem failure after callbacks and all object writes prevents
    // completion publication. This is fault injection, not a power-loss test.
    let result = probe::run_with(&job, |i, f| {
        let c = compute(i, f)?;
        std::fs::create_dir(job.output_directory.join("REBUILD_PROBE.json"))?;
        Ok(c)
    });
    assert!(result.is_err());
    assert!(job.output_directory.join("ATTEMPT.json").is_file());
    assert!(!job.output_directory.join("REBUILD_PROBE.json").is_file());
    assert!(probe::run_with(&job, |_, _| panic!("failed attempt must not be repeated")).is_err());
}

#[test]
fn isolated_probe_child() {
    let Some(path) = std::env::var_os("TICKWIRE_REBUILD_PROBE_UNIT_JOB") else {
        return;
    };
    let bytes = std::fs::read(path).unwrap();
    let job: probe::Job = serde_json::from_slice(&bytes).unwrap();
    let receipt = probe::run_with(&job, compute).unwrap();
    assert_ne!(
        receipt.pid,
        std::env::var("TICKWIRE_REBUILD_PROBE_PARENT")
            .unwrap()
            .parse::<u32>()
            .unwrap()
    );
}

#[test]
fn isolated_old_executable_produces_real_pid_and_synced_callback_result() {
    let dir = Directory::new();
    let job = job(&dir.0);
    let file = object(&dir.0, &job.bytes().unwrap());
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("replay::rebuild::tests::probe_tests::isolated_probe_child")
        .arg("--nocapture")
        .env("TICKWIRE_REBUILD_PROBE_UNIT_JOB", file.path)
        .env(
            "TICKWIRE_REBUILD_PROBE_PARENT",
            std::process::id().to_string(),
        )
        .spawn()
        .unwrap();
    let pid = child.id();
    assert!(child.wait().unwrap().success());
    let receipt = probe::Receipt::decode(
        &std::fs::read(job.output_directory.join("REBUILD_PROBE.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(receipt.pid, pid);
    assert_ne!(pid, std::process::id());
    assert_eq!(receipt.job, Object::of(&job.bytes().unwrap()));
    assert_eq!(receipt.cut.sequence, 2);
}
