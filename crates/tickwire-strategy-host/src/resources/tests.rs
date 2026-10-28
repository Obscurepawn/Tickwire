use super::*;
use std::os::unix::fs::MetadataExt;

fn plan(root: &Path) -> ProcessResources {
    let group = |name| GroupResources {
        cgroup: root.join(name),
        cpus: vec![0],
        numa_node: 0,
        memory_max: 1_048_576,
    };
    ProcessResources {
        worker: group("worker"),
        source_replay: group("source"),
        rollback: group("rollback"),
    }
}

#[test]
fn resource_plans_are_strict_complete_and_bounded() {
    let valid = plan(Path::new("/configured"));
    valid.validate().unwrap();
    let bytes = serde_json::to_vec(&valid).unwrap();
    assert_eq!(
        serde_json::from_slice::<ProcessResources>(&bytes).unwrap(),
        valid
    );
    for field in ["worker", "source_replay", "rollback"] {
        let mut value = serde_json::to_value(&valid).unwrap();
        value.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<ProcessResources>(value).is_err());
    }
    let mut unknown = serde_json::to_value(&valid).unwrap();
    unknown["worker"]["fallback"] = true.into();
    assert!(serde_json::from_value::<ProcessResources>(unknown).is_err());
    let duplicate = String::from_utf8(bytes).unwrap().replacen(
        "{",
        &format!(
            "{{\"worker\":{},",
            serde_json::to_string(&valid.worker).unwrap()
        ),
        1,
    );
    assert!(serde_json::from_str::<ProcessResources>(&duplicate).is_err());
    let mut variants = Vec::new();
    for path in [
        "relative",
        "/configured/../worker",
        "/configured/./worker",
        "/",
        "/configured//worker",
        "/configured/worker/",
    ] {
        let mut v = valid.clone();
        v.worker.cgroup = path.into();
        variants.push(v);
    }
    let mut v = valid.clone();
    v.rollback.cgroup = v.worker.cgroup.clone();
    variants.push(v);
    let mut v = valid.clone();
    v.worker.cgroup = format!("/{}", "x".repeat(4096)).into();
    variants.push(v);
    let mut v = valid.clone();
    v.worker.cgroup = "/contains\0nul".into();
    variants.push(v);
    for cpus in [
        vec![],
        vec![0, 0],
        vec![1, 0],
        vec![4096],
        (0..4097).collect(),
    ] {
        let mut v = valid.clone();
        v.source_replay.cpus = cpus;
        variants.push(v);
    }
    let mut v = valid.clone();
    v.rollback.numa_node = 32;
    variants.push(v);
    for memory in [0, 1_048_575, 9_007_199_254_740_992] {
        let mut v = valid.clone();
        v.rollback.memory_max = memory;
        variants.push(v);
    }
    for v in variants {
        assert!(v.validate().is_err(), "{v:?}");
    }
    let mut edge = valid;
    edge.worker.cpus = (0..4096).collect();
    edge.worker.numa_node = 31;
    edge.worker.memory_max = 9_007_199_254_740_991;
    edge.validate().unwrap();
}

fn spec(root: &Path) -> LaunchSpec {
    let executable = std::fs::canonicalize("/bin/sh").unwrap();
    let owner = std::fs::metadata("/proc/self").unwrap();
    LaunchSpec {
        identity: Identity {
            instance: [7; 16],
            generation: 3,
            logical_strategy: 8,
            role: 3,
            arch: 1,
            trust_profile: 1,
            host_boot: [1; 16],
            namespace: [2; 16],
            build_hash: tickwire_recovery::digest(&std::fs::read(&executable).unwrap()),
            qualification_hash: [3; 32],
            manifest_hash: [4; 32],
        },
        executable,
        arguments: vec![
            "-c".into(),
            "printf executed > marker; while :; do :; done".into(),
        ],
        working_directory: root.to_owned(),
        environment: Default::default(),
        uid: owner.uid(),
        gid: owner.gid(),
        inherit_output: false,
    }
}
fn rejected(purpose: Purpose) {
    let root = PrivateDirectory::create_in(&std::env::temp_dir(), "host-resource").unwrap();
    let resources = plan(root.path());
    for group in [
        &resources.worker,
        &resources.source_replay,
        &resources.rollback,
    ] {
        std::fs::create_dir(&group.cgroup).unwrap();
    }
    resources.validate().unwrap();
    let mut registry = Registry::new(1).unwrap();
    let result = spawn(&mut registry, spec(root.path()), Some(&resources), purpose);
    if result.is_ok() {
        let _ = registry.terminate([7; 16]);
    }
    assert!(
        result.is_err(),
        "a plain directory must not fall back to unconstrained execution"
    );
    assert!(!root.path().join("marker").exists());
    // A rejected constrained launch must not occupy a successful Registry entry.
    assert!(registry.process([7; 16]).is_err());
}
#[test]
fn worker_resource_failure_never_executes_the_target() {
    rejected(Purpose::Worker);
}
#[test]
fn source_replay_resource_failure_never_executes_the_target() {
    rejected(Purpose::SourceReplay);
}
#[test]
fn rollback_resource_failure_never_executes_the_target() {
    rejected(Purpose::Rollback);
}

#[test]
fn legacy_launch_still_executes_the_same_elf() {
    let root = PrivateDirectory::create_in(&std::env::temp_dir(), "host-legacy").unwrap();
    let mut registry = Registry::new(1).unwrap();
    spawn(&mut registry, spec(root.path()), None, Purpose::Worker).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while std::fs::read(root.path().join("marker")).ok().as_deref() != Some(b"executed".as_slice())
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        std::fs::read(root.path().join("marker")).unwrap(),
        b"executed"
    );
    assert!(matches!(
        registry.observe_resources([7; 16]),
        Err(tickwire_supervisor::Error::ResourcesRequired)
    ));
    registry.terminate([7; 16]).unwrap();
}

#[test]
fn only_the_selected_purpose_is_acquired_after_whole_plan_validation() {
    for purpose in [Purpose::Worker, Purpose::SourceReplay, Purpose::Rollback] {
        let root = PrivateDirectory::create_in(&std::env::temp_dir(), "host-purpose").unwrap();
        let resources = plan(root.path());
        for group in [
            &resources.worker,
            &resources.source_replay,
            &resources.rollback,
        ] {
            std::fs::create_dir(&group.cgroup).unwrap();
        }
        let selected = match purpose {
            Purpose::Worker => &resources.worker,
            Purpose::SourceReplay => &resources.source_replay,
            Purpose::Rollback => &resources.rollback,
        };
        std::fs::remove_dir(&selected.cgroup).unwrap();
        let mut registry = Registry::new(1).unwrap();
        let result = spawn(&mut registry, spec(root.path()), Some(&resources), purpose);
        assert!(
            matches!(result, Err(Error::Supervisor(tickwire_supervisor::Error::Transport(
            tickwire_control_sys::Error::System { source, .. }
        ))) if source.kind() == std::io::ErrorKind::NotFound)
        );
        assert!(!root.path().join("marker").exists());
        // A malformed unused group rejects the plan before acquiring the selected missing group.
        for invalid_path in [
            "relative",
            "/",
            "/configured//worker",
            "/configured/worker/",
        ] {
            let mut invalid = resources.clone();
            match purpose {
                Purpose::Worker => invalid.rollback.cgroup = invalid_path.into(),
                _ => invalid.worker.cgroup = invalid_path.into(),
            }
            assert!(matches!(
                spawn(&mut registry, spec(root.path()), Some(&invalid), purpose),
                Err(Error::Invalid)
            ));
        }
    }
}
