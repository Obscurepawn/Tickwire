#![cfg(target_os = "linux")]

use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tickwire_control::{Identity, ProcessIdentity};
use tickwire_supervisor::{Error, LaunchSpec, Registry};

const WORKER_DIRECTORY: &str = "TICKWIRE_OBSERVATION_WORKER_DIRECTORY";
static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "tickwire-observation-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn wait_for(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(Instant::now() < deadline, "missing child marker {path:?}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn host_boot() -> [u8; 16] {
    let text = fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap();
    let hex: String = text.trim().chars().filter(|c| *c != '-').collect();
    assert_eq!(hex.len(), 32);
    std::array::from_fn(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap())
}

fn credentials(pid: u32) -> ProcessIdentity {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    let value = |label: &str| {
        let row = status.lines().find(|line| line.starts_with(label)).unwrap();
        let ids: Vec<u32> = row
            .split_whitespace()
            .skip(1)
            .map(|s| s.parse().unwrap())
            .collect();
        assert_eq!(ids.len(), 4);
        assert!(ids.iter().all(|id| *id == ids[0]));
        ids[0]
    };
    ProcessIdentity {
        pid,
        uid: value("Uid:"),
        gid: value("Gid:"),
    }
}

fn start_ticks(pid: u32) -> u64 {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    // The final ')' terminates comm; field 3 is the first token after it.
    stat[stat.rfind(')').unwrap() + 1..]
        .split_whitespace()
        .nth(19)
        .unwrap()
        .parse()
        .unwrap()
}

fn identity(executable: &Path) -> Identity {
    let mut approved_boot = host_boot();
    approved_boot[0] ^= 0x80;
    Identity {
        instance: [3; 16],
        generation: 4,
        role: 3,
        logical_strategy: 5,
        host_boot: approved_boot,
        namespace: [6; 16],
        build_hash: Sha256::digest(fs::read(executable).unwrap()).into(),
        qualification_hash: [7; 32],
        manifest_hash: [8; 32],
        arch: if cfg!(target_arch = "aarch64") { 2 } else { 1 },
        trust_profile: 1,
    }
}

fn spawn(registry: &mut Registry, directory: &Directory, executable: &Path) -> (Identity, u32) {
    let expected = identity(executable);
    let caller = credentials(std::process::id());
    let process = registry
        .spawn(LaunchSpec {
            identity: expected.clone(),
            executable: executable.to_owned(),
            arguments: vec![
                "--exact".into(),
                "observation_worker".into(),
                "--ignored".into(),
                "--nocapture".into(),
            ],
            working_directory: directory.0.clone(),
            environment: BTreeMap::from([(
                WORKER_DIRECTORY.into(),
                directory.0.to_str().unwrap().into(),
            )]),
            uid: caller.uid,
            gid: caller.gid,
            inherit_output: true,
        })
        .unwrap();
    wait_for(&directory.0.join("ready"));
    assert_eq!(
        fs::read_to_string(directory.0.join("ready")).unwrap(),
        process.pid.to_string()
    );
    (expected, process.pid)
}

#[test]
fn live_observation_binds_actual_kernel_facts_without_rewriting_approved_identity() {
    let directory = Directory::new();
    let mut registry = Registry::new(1).unwrap();
    let executable = std::env::current_exe().unwrap();
    let bytes = fs::metadata(&executable).unwrap().len();
    let (expected, pid) = spawn(&mut registry, &directory, &executable);
    let before = Instant::now();
    let actual = registry.observe(expected.instance).unwrap();
    let after = Instant::now();
    assert_eq!(actual.identity(), &expected);
    assert_eq!(actual.process(), credentials(pid));
    assert_eq!(actual.host_boot(), host_boot());
    assert_ne!(actual.host_boot(), actual.identity().host_boot);
    assert_eq!(actual.start_ticks(), start_ticks(pid));
    assert!(actual.start_ticks() > 0);
    assert_eq!(actual.image_bytes(), bytes);
    assert_eq!(actual.image_hash(), expected.build_hash);
    assert_eq!(actual.connected_epoch(), None);
    assert!((before..=after).contains(&actual.sampled_at()));
    assert_eq!(
        fs::read(format!("/proc/{pid}/comm")).unwrap(),
        b"tw (x)\n)y\n"
    );
}

#[test]
fn unknown_or_terminated_child_cannot_produce_observation() {
    let directory = Directory::new();
    let mut registry = Registry::new(1).unwrap();
    assert!(matches!(registry.observe([9; 16]), Err(Error::Identity)));
    let (expected, pid) = spawn(&mut registry, &directory, &std::env::current_exe().unwrap());
    assert_eq!(
        registry.observe(expected.instance).unwrap().process().pid,
        pid
    );
    assert_eq!(registry.terminate(expected.instance).unwrap().pid(), pid);
    assert!(matches!(
        registry.observe(expected.instance),
        Err(Error::Exited)
    ));
}

#[test]
fn replacing_and_deleting_launch_path_does_not_change_running_image_observation() {
    let directory = Directory::new();
    let mut registry = Registry::new(1).unwrap();
    let entry = directory.0.join("worker");
    fs::copy(std::env::current_exe().unwrap(), &entry).unwrap();
    let (expected, pid) = spawn(&mut registry, &directory, &entry);
    let initial = registry.observe(expected.instance).unwrap();
    let replacement = directory.0.join("replacement");
    fs::copy("/bin/sleep", &replacement).unwrap();
    fs::rename(&replacement, &entry).unwrap();
    let changed = registry.observe(expected.instance).unwrap();
    assert_eq!(changed.process().pid, pid);
    assert_eq!(changed.image_hash(), initial.image_hash());
    assert_eq!(changed.image_bytes(), initial.image_bytes());
    assert_eq!(changed.start_ticks(), initial.start_ticks());
    fs::remove_file(&entry).unwrap();
    let deleted = registry.observe(expected.instance).unwrap();
    assert_eq!(deleted.image_hash(), expected.build_hash);
    assert_eq!(deleted.start_ticks(), initial.start_ticks());
    assert_eq!(deleted.process().pid, pid);
}

#[test]
fn same_owned_pid_execing_another_elf_cannot_reuse_launch_identity() {
    let directory = Directory::new();
    let mut registry = Registry::new(1).unwrap();
    let (expected, pid) = spawn(&mut registry, &directory, &std::env::current_exe().unwrap());
    assert_eq!(
        registry.observe(expected.instance).unwrap().process().pid,
        pid
    );
    let initial_image = fs::read_link(format!("/proc/{pid}/exe")).unwrap();
    fs::write(directory.0.join("exec"), b"go").unwrap();
    wait_for(&directory.0.join("replaced"));
    assert_ne!(
        fs::read_link(format!("/proc/{pid}/exe")).unwrap(),
        initial_image
    );
    assert_eq!(registry.process(expected.instance).unwrap().pid, pid);
    assert!(registry.observe(expected.instance).is_err());
    assert_eq!(registry.process(expected.instance).unwrap().pid, pid);
}

#[test]
#[ignore = "self-spawned fixture owns the child until its Registry drops"]
fn observation_worker() {
    let directory = PathBuf::from(std::env::var_os(WORKER_DIRECTORY).unwrap());
    fs::write("/proc/self/comm", b"tw (x)\n)y").unwrap();
    fs::write(
        directory.join("ready.pending"),
        std::process::id().to_string(),
    )
    .unwrap();
    fs::rename(directory.join("ready.pending"), directory.join("ready")).unwrap();
    loop {
        if directory.join("exec").exists() {
            let error = Command::new("/bin/sh")
                .arg("-c")
                .arg("printf replaced > \"$1\"; exec /bin/sleep 30")
                .arg("observation-replacement")
                .arg(directory.join("replaced"))
                .exec();
            panic!("replacement exec failed: {error}");
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}
