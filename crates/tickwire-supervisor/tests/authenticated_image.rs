#![cfg(target_os = "linux")]
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    process::Command,
    time::{Duration, Instant},
};
use tickwire_control::{ClientHandshake, Identity, Policy, ProcessIdentity};
use tickwire_control_sys::{Connection, ControlListener, PrivateDirectory, monotonic_ns};
use tickwire_supervisor::{Error, LaunchSpec, Registry};
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn credentials() -> (u32, u32) {
    let status = fs::read_to_string("/proc/self/status").unwrap();
    let field = |name: &str| {
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
    (field("Uid:"), field("Gid:"))
}
fn identity(id: u8, role: u16, hash: [u8; 32]) -> Identity {
    Identity {
        instance: [id; 16],
        generation: 1,
        role,
        logical_strategy: if role == 3 { 7 } else { 0 },
        host_boot: [8; 16],
        namespace: [9; 16],
        build_hash: hash,
        qualification_hash: [11; 32],
        manifest_hash: [12; 32],
        arch: if cfg!(target_arch = "aarch64") { 2 } else { 1 },
        trust_profile: 1,
    }
}
fn policy(server: bool, peer_pid: u32, hash: [u8; 32]) -> Policy {
    let (uid, gid) = credentials();
    Policy {
        local: identity(if server { 1 } else { 2 }, if server { 1 } else { 3 }, hash),
        peer: identity(if server { 2 } else { 1 }, if server { 3 } else { 1 }, hash),
        peer_process: ProcessIdentity {
            pid: peer_pid,
            uid,
            gid,
        },
        required_features: 0x1c,
        supported_features: 0x1f,
        control_epoch: 13,
        lease_duration_ns: 3_000_000_000,
        max_frame_bytes: 65536,
        nonce: [14; 32],
    }
}
fn spec(path: &Path, listener: &Path, reexec: bool) -> LaunchSpec {
    let (uid, gid) = credentials();
    LaunchSpec {
        identity: identity(2, 3, Sha256::digest(fs::read(path).unwrap()).into()),
        executable: path.into(),
        arguments: vec![
            "--exact".into(),
            "authenticated_image_child".into(),
            "--ignored".into(),
            "--nocapture".into(),
        ],
        working_directory: path.parent().unwrap().into(),
        environment: BTreeMap::from([
            ("TW_IMAGE_SOCKET".into(), listener.to_str().unwrap().into()),
            ("TW_IMAGE_PARENT".into(), std::process::id().to_string()),
            ("TW_IMAGE_ORIGINAL".into(), path.to_str().unwrap().into()),
            (
                "TW_IMAGE_REEXEC".into(),
                if reexec { "first" } else { "none" }.into(),
            ),
        ]),
        uid,
        gid,
        inherit_output: false,
    }
}
#[test]
fn real_sealed_child_authenticates_and_connected_epoch_is_only_connection_state() {
    let dir = PrivateDirectory::create_in(Path::new("/tmp"), "tw-image-auth").unwrap();
    let path = dir.path().join("worker");
    fs::copy(std::env::current_exe().unwrap(), &path).unwrap();
    let socket = dir.path().join("control.sock");
    let listener = ControlListener::bind(&socket).unwrap();
    let spec = spec(&path, &socket, false);
    let hash = spec.identity.build_hash;
    let mut registry = Registry::new(1).unwrap();
    let child = registry.spawn(spec).unwrap();
    let authenticated = registry
        .accept(
            &listener,
            policy(true, child.pid, hash),
            monotonic_ns().unwrap(),
            deadline(),
        )
        .unwrap();
    assert_eq!(authenticated.process, child);
    assert_eq!(
        registry.observe([2; 16]).unwrap().connected_epoch(),
        Some(13)
    );
    registry.disconnected([2; 16], 13).unwrap();
    assert_eq!(registry.observe([2; 16]).unwrap().connected_epoch(), None);
    registry.terminate([2; 16]).unwrap();
    fs::remove_file(path).unwrap();
}
#[test]
fn same_pid_reexec_cannot_authenticate_using_the_original_hello_identity() {
    let dir = PrivateDirectory::create_in(Path::new("/tmp"), "tw-image-reexec").unwrap();
    let path = dir.path().join("worker");
    fs::copy(std::env::current_exe().unwrap(), &path).unwrap();
    let socket = dir.path().join("control.sock");
    let listener = ControlListener::bind(&socket).unwrap();
    let spec = spec(&path, &socket, true);
    let hash = spec.identity.build_hash;
    let mut registry = Registry::new(1).unwrap();
    let child = registry.spawn(spec).unwrap();
    assert!(matches!(
        registry.accept(
            &listener,
            policy(true, child.pid, hash),
            monotonic_ns().unwrap(),
            deadline()
        ),
        Err(Error::Identity)
    ));
    registry.terminate([2; 16]).unwrap();
    fs::remove_file(path).unwrap();
}
#[test]
fn wrong_approved_hash_cannot_spawn_or_register_a_child() {
    let dir = PrivateDirectory::create_in(Path::new("/tmp"), "tw-image-hash").unwrap();
    let path = dir.path().join("worker");
    fs::copy(std::env::current_exe().unwrap(), &path).unwrap();
    let mut spec = spec(&path, &dir.path().join("control.sock"), false);
    spec.identity.build_hash[0] ^= 1;
    let mut registry = Registry::new(1).unwrap();
    assert!(matches!(registry.spawn(spec), Err(Error::Identity)));
    assert!(matches!(registry.process([2; 16]), Err(Error::Identity)));
    fs::remove_file(path).unwrap();
}
#[test]
#[ignore = "real child entry for authenticated image tests"]
fn authenticated_image_child() {
    let mode = std::env::var("TW_IMAGE_REEXEC").unwrap();
    if mode == "first" {
        use std::os::unix::process::CommandExt;
        let error = Command::new(std::env::var_os("TW_IMAGE_ORIGINAL").unwrap())
            .args([
                "--exact",
                "authenticated_image_child",
                "--ignored",
                "--nocapture",
            ])
            .env("TW_IMAGE_REEXEC", "second")
            .exec();
        panic!("second exec failed: {error}");
    }
    let hash = Sha256::digest(fs::read("/proc/self/exe").unwrap()).into();
    let parent = std::env::var("TW_IMAGE_PARENT").unwrap().parse().unwrap();
    let socket = std::env::var("TW_IMAGE_SOCKET").unwrap();
    let mut connection = Connection::connect(Path::new(&socket), deadline()).unwrap();
    let handshake = ClientHandshake::new(policy(false, parent, hash), [15; 16]).unwrap();
    connection.send(handshake.hello(), &[], deadline()).unwrap();
    let welcome = connection.recv(deadline());
    if mode == "second" {
        assert!(welcome.is_err());
        return;
    }
    let credentials = connection.peer_credentials();
    handshake
        .finish(
            ProcessIdentity {
                pid: credentials.pid,
                uid: credentials.uid,
                gid: credentials.gid,
            },
            welcome.unwrap().bytes(),
            monotonic_ns().unwrap(),
        )
        .unwrap();
    std::thread::sleep(Duration::from_secs(5));
}
