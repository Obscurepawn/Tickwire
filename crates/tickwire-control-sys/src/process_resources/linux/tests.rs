use super::*;
use std::{os::unix::process::CommandExt, time::Duration};

fn limits() -> ResourceLimits {
    ResourceLimits {
        cpus: vec![0],
        numa_node: 0,
        memory_max: 64 * 1024 * 1024,
    }
}

#[test]
fn fake_filesystem_and_symlink_components_never_create_a_domain() {
    let directory =
        crate::PrivateDirectory::create_in(Path::new("/tmp"), "tw-resource-reject").unwrap();
    for (name, bytes) in [
        ("cgroup.type", "domain\n"),
        ("cpuset.cpus.effective", "0\n"),
        ("cpuset.mems.effective", "0\n"),
        ("memory.max", "67108864\n"),
        ("memory.swap.max", "0\n"),
        ("cgroup.procs", ""),
        ("cgroup.threads", ""),
        ("cgroup.stat", "nr_descendants 0\nnr_dying_descendants 0\n"),
    ] {
        std::fs::write(directory.path().join(name), bytes).unwrap();
    }
    assert!(matches!(
        ResourceDomain::acquire(directory.path(), limits()),
        Err(Error::InvalidResources)
    ));
    let link = directory.path().join("link");
    std::os::unix::fs::symlink("/sys/fs/cgroup", &link).unwrap();
    assert!(ResourceDomain::acquire(&link, limits()).is_err());
    assert!(ResourceDomain::acquire(&link.join("child"), limits()).is_err());
    for bad in [
        "relative",
        "/sys/./fs/cgroup",
        "/sys/fs/../fs/cgroup",
        "/sys//fs/cgroup",
    ] {
        assert!(ResourceDomain::acquire(Path::new(bad), limits()).is_err());
    }
    std::fs::remove_dir_all(directory.path()).unwrap();
}
#[test]
fn actual_cgroup_root_rejects_configuration_and_has_an_exclusive_owner_lease() {
    let path = Path::new("/sys/fs/cgroup");
    let one = files::directory(path).unwrap();
    files::cgroupfs(&one).unwrap();
    // Both assertions use the same actual inode. Keep them in one test so an
    // unrelated test's lease cannot masquerade as an invalid root configuration.
    match ResourceDomain::acquire(path, limits()) {
        Err(Error::InvalidResources) => {}
        Err(Error::System { operation, source })
            if operation == "open resource file" && source.raw_os_error() == Some(libc::ENOENT) =>
        {
            // The real hierarchy root may omit controller limit files; require
            // evidence of that exact missing setting, not an arbitrary I/O error.
            assert!(SETTINGS.iter().any(|name| matches!(
                files::open(&one, name, libc::O_RDONLY),
                Err(Error::System { source, .. })
                    if source.raw_os_error() == Some(libc::ENOENT)
            )));
        }
        Err(error) => panic!("root configuration must fail without lease contention: {error:?}"),
        Ok(_) => panic!("the shared hierarchy root is not a dedicated configured leaf"),
    }
    let two = files::directory(path).unwrap();
    assert_eq!(one.metadata().unwrap().dev(), two.metadata().unwrap().dev());
    assert_eq!(one.metadata().unwrap().ino(), two.metadata().unwrap().ino());
    let first = files::DirectoryLease::acquire(one).unwrap();
    let inherited = first.try_clone().unwrap();
    assert!(matches!(
        files::DirectoryLease::acquire(two.try_clone().unwrap()),
        Err(Error::ResourceConflict)
    ));
    drop(first);
    // The production guard unlocks immediately even if fork copied the FD.
    let second = files::DirectoryLease::acquire(two).unwrap();
    drop(inherited);
    assert!(matches!(
        files::DirectoryLease::acquire(files::directory(path).unwrap()),
        Err(Error::ResourceConflict)
    ));
    drop(second);
    drop(files::DirectoryLease::acquire(files::directory(path).unwrap()).unwrap());
}
#[test]
fn preparation_limits_and_bounded_reads_reject_before_spawning() {
    for value in [
        ResourceLimits {
            cpus: vec![],
            ..limits()
        },
        ResourceLimits {
            cpus: vec![1, 0],
            ..limits()
        },
        ResourceLimits {
            cpus: vec![0, 0],
            ..limits()
        },
        ResourceLimits {
            cpus: vec![4096],
            ..limits()
        },
        ResourceLimits {
            numa_node: 32,
            ..limits()
        },
        ResourceLimits {
            memory_max: 1,
            ..limits()
        },
        ResourceLimits {
            memory_max: u64::MAX,
            ..limits()
        },
    ] {
        assert!(value.validate().is_err());
    }
    let file = File::open("/dev/zero").unwrap();
    assert!(matches!(files::bounded(file), Err(Error::InvalidResources)));
}
#[test]
fn failed_migration_syscall_prevents_target_execution() {
    let directory =
        crate::PrivateDirectory::create_in(Path::new("/tmp"), "tw-resource-noexec").unwrap();
    let marker = directory.path().join("ran");
    let shell = std::fs::canonicalize("/bin/sh").unwrap();
    let image = ExecutableImage::open(&shell, 512 * 1024 * 1024).unwrap();
    let mut command = image
        .configured_command(|c| {
            c.args(["-c", "touch \"$1\"", "--"]).arg(&marker);
        })
        .unwrap();
    // Test only the shared pre-exec syscall failure mechanism, not a successful cgroup.
    let full = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/full")
        .unwrap();
    child::Setup::new(full, vec![], &[0]).install(&mut command);
    assert!(command.spawn().is_err());
    assert!(!marker.exists());
}
#[test]
fn affinity_is_set_before_real_exec_and_inherited_by_all_child_threads() {
    let original = child::Mask::read(0).unwrap();
    let cpu = original.cpus()[0];
    let expected = child::Mask::new(&[cpu]);
    let directory =
        crate::PrivateDirectory::create_in(Path::new("/tmp"), "tw-resource-affinity").unwrap();
    let marker = directory.path().join("ready");
    let image = ExecutableImage::open(Path::new("/proc/self/exe"), 512 * 1024 * 1024);
    // ExecutableImage deliberately rejects symlinks; copy the actual test image to a regular file.
    assert!(image.is_err());
    let path = directory.path().join("worker");
    std::fs::copy("/proc/self/exe", &path).unwrap();
    let image = ExecutableImage::open(&path, 512 * 1024 * 1024).unwrap();
    let mut command = image
        .configured_command(|c| {
            c.args([
                "--exact",
                "process_resources::linux::tests::affinity_child",
                "--ignored",
                "--nocapture",
            ])
            .env("TICKWIRE_AFFINITY_MARKER", &marker);
        })
        .unwrap();
    // SAFETY: this isolated test uses the same allocation-free syscall helper as production.
    unsafe {
        command.pre_exec(move || expected.apply());
    }
    let mut process = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !marker.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let outcome = std::panic::catch_unwind(|| {
        assert!(
            marker.exists(),
            "child did not reach its real threaded entry"
        );
        let threads = observe::tasks(process.id()).unwrap();
        assert!(threads.len() >= 3);
        for tid in threads {
            assert!(child::Mask::read(tid).unwrap() == expected);
        }
        assert!(
            child::Mask::read(0).unwrap() == original,
            "parent affinity changed"
        );
    });
    process.kill().unwrap();
    process.wait().unwrap();
    std::fs::remove_file(path).unwrap();
    std::fs::remove_file(marker).unwrap();
    outcome.unwrap();
}
#[test]
#[ignore = "child entry invoked by the real affinity parent test"]
fn affinity_child() {
    let marker = std::env::var_os("TICKWIRE_AFFINITY_MARKER").unwrap();
    let _thread = std::thread::spawn(|| std::thread::sleep(Duration::from_secs(30)));
    std::fs::write(marker, b"ready").unwrap();
    std::thread::sleep(Duration::from_secs(30));
}

#[test]
fn resource_settings_require_exact_finite_limits_and_domain_leaf() {
    let valid: Vec<Vec<u8>> = [
        b"domain\n".as_slice(),
        b"0\n",
        b"0\n",
        b"67108864\n",
        b"0\n",
        b"nr_descendants 0\nnr_dying_descendants 0\n",
    ]
    .iter()
    .map(|v| v.to_vec())
    .collect();
    validate_settings(&valid, &limits()).unwrap();
    for (index, value) in [
        (0, b"domain threaded\n".as_slice()),
        (0, b"threaded\n"),
        (1, b"0-1\n"),
        (1, b"1\n"),
        (2, b"0-1\n"),
        (2, b"1\n"),
        (3, b"max\n"),
        (3, b"33554432\n"),
        (4, b"max\n"),
        (4, b"1\n"),
        (5, b"nr_descendants 1\nnr_dying_descendants 0\n"),
    ] {
        let mut changed = valid.clone();
        changed[index] = value.to_vec();
        assert!(
            validate_settings(&changed, &limits()).is_err(),
            "index {index}: {value:?}"
        );
    }
}

#[test]
fn changed_preexec_snapshot_prevents_target_execution() {
    let directory =
        crate::PrivateDirectory::create_in(Path::new("/tmp"), "tw-resource-stale").unwrap();
    let marker = directory.path().join("ran");
    let file = directory.path().join("sample");
    std::fs::write(&file, b"changed\n").unwrap();
    let image = ExecutableImage::open(
        &std::fs::canonicalize("/bin/sh").unwrap(),
        512 * 1024 * 1024,
    )
    .unwrap();
    let mut command = image
        .configured_command(|c| {
            c.args(["-c", "touch \"$1\"", "--"]).arg(&marker);
        })
        .unwrap();
    let full = std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/full")
        .unwrap();
    // Unit test of pre-exec revalidation only; this never constructs a ResourceDomain.
    child::Setup::new(
        full,
        vec![(File::open(&file).unwrap(), b"original\n".to_vec())],
        &[0],
    )
    .install(&mut command);
    assert!(command.spawn().is_err());
    assert!(!marker.exists());
    std::fs::remove_file(file).unwrap();
}

#[test]
fn directory_lease_ends_at_owner_drop_even_with_an_inherited_description() {
    let root = crate::PrivateDirectory::create_in(Path::new("/tmp"), "tw-lease-owner").unwrap();
    let first = files::DirectoryLease::acquire(File::open(root.path()).unwrap()).unwrap();
    let inherited = first.try_clone().unwrap();
    assert!(files::DirectoryLease::acquire(File::open(root.path()).unwrap()).is_err());
    drop(first);
    let second =
        files::DirectoryLease::acquire(File::open(root.path()).unwrap()).expect("old lease ended");
    drop(inherited);
    assert!(files::DirectoryLease::acquire(File::open(root.path()).unwrap()).is_err());
    drop(second);
    drop(files::DirectoryLease::acquire(File::open(root.path()).unwrap()).unwrap());
}
