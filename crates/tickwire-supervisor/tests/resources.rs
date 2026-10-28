#![cfg(target_os = "linux")]

use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tickwire_control::Identity;
use tickwire_control_sys::PrivateDirectory;
use tickwire_supervisor::{Error, LaunchSpec, Registry, ResourceLimits, ResourcePlan};

const WORKER_DIRECTORY: &str = "TICKWIRE_RESOURCE_WORKER_DIRECTORY";
fn limits() -> ResourceLimits {
    ResourceLimits {
        cpus: vec![0],
        numa_node: 0,
        memory_max: 128 * 1024 * 1024,
    }
}
fn spec(directory: &Path, id: u8) -> LaunchSpec {
    let executable = std::env::current_exe().unwrap();
    let status = fs::read_to_string("/proc/self/status").unwrap();
    let credential = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap()
    };
    LaunchSpec {
        identity: Identity {
            instance: [id; 16],
            generation: 1,
            role: 3,
            logical_strategy: 7,
            host_boot: [8; 16],
            namespace: [9; 16],
            build_hash: Sha256::digest(fs::read(&executable).unwrap()).into(),
            qualification_hash: [11; 32],
            manifest_hash: [12; 32],
            arch: if cfg!(target_arch = "aarch64") { 2 } else { 1 },
            trust_profile: 1,
        },
        executable,
        arguments: vec![
            "--exact".into(),
            "resource_worker".into(),
            "--ignored".into(),
            "--nocapture".into(),
        ],
        working_directory: directory.to_owned(),
        environment: BTreeMap::from([(
            WORKER_DIRECTORY.into(),
            directory.to_str().unwrap().into(),
        )]),
        uid: credential("Uid:"),
        gid: credential("Gid:"),
        inherit_output: true,
    }
}
fn wait_ready(directory: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !directory.join("ready").exists() {
        assert!(Instant::now() < deadline, "resource child did not start");
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn fake_cgroup_is_rejected_before_exec_and_failed_attempt_keeps_registry_slot_free() {
    let directory = PrivateDirectory::create_in(Path::new("/tmp"), "tw-resource-fake").unwrap();
    let group = directory.path().join("group");
    fs::create_dir(&group).unwrap();
    for (name, body) in [
        ("cgroup.type", "domain\n"),
        ("cgroup.procs", ""),
        ("cgroup.threads", ""),
        ("cpuset.cpus.effective", "0\n"),
        ("cpuset.mems.effective", "0\n"),
        ("memory.max", "134217728\n"),
        ("memory.swap.max", "0\n"),
    ] {
        fs::write(group.join(name), body).unwrap();
    }
    let launch = spec(directory.path(), 2);
    let mut registry = Registry::new(1).unwrap();
    assert!(
        registry
            .spawn_with_resources(
                launch.clone(),
                ResourcePlan {
                    cgroup: group,
                    limits: limits()
                }
            )
            .is_err(),
        "ordinary files must not authorize an unconstrained executable"
    );
    assert!(matches!(registry.process([2; 16]), Err(Error::Identity)));
    assert!(!directory.path().join("ready").exists());
    let process = registry.spawn(launch).unwrap();
    wait_ready(directory.path());
    assert_eq!(registry.observe([2; 16]).unwrap().process(), process);
    assert!(matches!(
        registry.observe_resources([2; 16]),
        Err(Error::ResourcesRequired)
    ));
    registry.terminate([2; 16]).unwrap();
    fs::remove_dir_all(directory.path()).unwrap();
}

#[test]
fn missing_instance_has_no_resource_observation() {
    let mut registry = Registry::new(1).unwrap();
    assert!(matches!(
        registry.observe_resources([3; 16]),
        Err(Error::Identity)
    ));
}

// This test never creates or rewrites controllers. The operator supplies an
// empty delegated leaf and independently states its intended exact limits.
fn delegated_plan() -> ResourcePlan {
    ResourcePlan {
        cgroup: PathBuf::from(
            std::env::var_os("TICKWIRE_RESOURCE_CGROUP").expect("delegated leaf"),
        ),
        limits: ResourceLimits {
            cpus: std::env::var("TICKWIRE_RESOURCE_CPUS")
                .expect("comma-separated CPU IDs")
                .split(',')
                .map(|id| id.parse().unwrap())
                .collect(),
            numa_node: std::env::var("TICKWIRE_RESOURCE_NUMA")
                .expect("NUMA node")
                .parse()
                .unwrap(),
            memory_max: std::env::var("TICKWIRE_RESOURCE_MEMORY_MAX")
                .expect("memory bytes")
                .parse()
                .unwrap(),
        },
    }
}
fn kernel_set(value: &str) -> Vec<u32> {
    let mut result = Vec::new();
    for part in value.trim().split(',') {
        if let Some((first, last)) = part.split_once('-') {
            let first: u32 = first.parse().unwrap();
            let last: u32 = last.parse().unwrap();
            assert!(first <= last && last <= 4095);
            result.extend(first..=last);
        } else {
            result.push(part.parse().unwrap());
        }
    }
    result
}

#[test]
#[ignore = "requires a preconfigured empty delegated cgroup v2 leaf and exact resource environment"]
fn delegated_domain_binds_all_threads_and_retains_lease_after_owned_child_exit() {
    let plan = delegated_plan();
    plan.limits.validate().unwrap();
    let directory = PrivateDirectory::create_in(Path::new("/tmp"), "tw-resource-real").unwrap();
    let launch = spec(directory.path(), 4);
    let mut registry = Registry::new(1).unwrap();
    let process = registry
        .spawn_with_resources(launch.clone(), plan.clone())
        .unwrap();
    wait_ready(directory.path());
    let observed = registry.observe_resources([4; 16]).unwrap();
    assert_eq!(observed.process().process(), process);
    assert_eq!(observed.process().image_hash(), launch.identity.build_hash);
    let resources = observed.resources();
    assert_eq!(resources.pid(), process.pid);
    assert_eq!(resources.limits(), &plan.limits);
    assert!(resources.sampled_at() >= observed.process().sampled_at());
    let metadata = fs::metadata(&plan.cgroup).unwrap();
    assert_eq!(resources.group_device(), metadata.dev());
    assert_eq!(resources.group_inode(), metadata.ino());
    let mut threads: Vec<u32> = fs::read_dir(format!("/proc/{}/task", process.pid))
        .unwrap()
        .map(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_str()
                .unwrap()
                .parse()
                .unwrap()
        })
        .collect();
    threads.sort_unstable();
    assert!(threads.len() >= 2);
    assert_eq!(resources.thread_ids(), threads);
    for tid in threads {
        let status =
            fs::read_to_string(format!("/proc/{}/task/{tid}/status", process.pid)).unwrap();
        let field = |name: &str| {
            status
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .unwrap()
        };
        assert_eq!(kernel_set(field("Cpus_allowed_list:")), plan.limits.cpus);
        assert_eq!(
            kernel_set(field("Mems_allowed_list:")),
            vec![plan.limits.numa_node]
        );
    }
    assert_eq!(
        fs::read_to_string(plan.cgroup.join("memory.max"))
            .unwrap()
            .trim(),
        plan.limits.memory_max.to_string()
    );
    assert_eq!(
        fs::read_to_string(plan.cgroup.join("memory.swap.max"))
            .unwrap()
            .trim(),
        "0"
    );
    assert_eq!(
        fs::read_to_string(plan.cgroup.join("cgroup.procs"))
            .unwrap()
            .trim(),
        process.pid.to_string()
    );

    let mut other = Registry::new(1).unwrap();
    let mut second = launch.clone();
    second.identity.instance = [5; 16];
    assert!(matches!(
        other.spawn_with_resources(second.clone(), plan.clone()),
        Err(Error::Transport(
            tickwire_control_sys::Error::ResourceConflict
        ))
    ));
    assert!(matches!(other.process([5; 16]), Err(Error::Identity)));
    registry.terminate([4; 16]).unwrap();
    assert!(matches!(
        registry.observe_resources([4; 16]),
        Err(Error::Exited)
    ));
    assert!(
        matches!(
            other.spawn_with_resources(second.clone(), plan.clone()),
            Err(Error::Transport(
                tickwire_control_sys::Error::ResourceConflict
            ))
        ),
        "exiting the direct child must not release its Entry's domain lease"
    );
    drop(registry);
    fs::remove_file(directory.path().join("ready")).unwrap();
    other.spawn_with_resources(second, plan).unwrap();
    wait_ready(directory.path());
    other.observe_resources([5; 16]).unwrap();
    other.terminate([5; 16]).unwrap();
    fs::remove_file(directory.path().join("ready")).unwrap();
    fs::remove_dir(directory.path()).unwrap();
}

#[test]
#[ignore = "actual fixture entry; parent Registry owns this process"]
fn resource_worker() {
    let directory = PathBuf::from(std::env::var_os(WORKER_DIRECTORY).unwrap());
    let (sender, receiver) = std::sync::mpsc::sync_channel(0);
    let other = std::thread::spawn(move || {
        sender.send(()).unwrap();
        loop {
            std::thread::sleep(Duration::from_millis(10));
        }
    });
    receiver.recv().unwrap();
    fs::write(directory.join("ready"), std::process::id().to_string()).unwrap();
    other.join().unwrap();
}
