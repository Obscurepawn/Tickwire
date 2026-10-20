#![cfg(target_os = "linux")]
use std::fs::File;
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::fs::PermissionsExt;
use std::time::{Duration, Instant};
use tickwire_control_sys::*;
use tickwire_ipc::{
    ControlEnvelope, ControlFrame, HelloBody, OfferPrefix, ResourceDescriptor, Wire, crc32c,
};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn frame(count: usize) -> Vec<u8> {
    let mut bytes = if count == 0 {
        vec![0; 320]
    } else {
        vec![0; 192 + count * 128]
    };
    if count == 0 {
        HelloBody {
            min_major: 1,
            max_major: 1,
            role: 1,
            target_arch: 2,
            page_shift: 12,
            isolation_shift: 7,
            atomic_width: 8,
            trust_profile: 1,
            required_features: 28,
            max_frame_bytes: 65_536,
            ..Default::default()
        }
        .encode(&mut bytes[128..])
        .unwrap();
    } else {
        OfferPrefix {
            descriptor_count: count as u16,
            descriptor_bytes: 128,
            ..Default::default()
        }
        .encode(&mut bytes[128..192])
        .unwrap();
        for i in 0..count {
            ResourceDescriptor {
                fd_index: i as u16,
                resource_kind: 1,
                access: 1,
                seal_profile: 1,
                ..Default::default()
            }
            .encode(&mut bytes[192 + i * 128..320 + i * 128])
            .unwrap();
        }
    }
    let env = ControlEnvelope {
        magic: *b"TWCTL001",
        abi_major: 1,
        header_bytes: 128,
        opcode: if count == 0 { 1 } else { 3 },
        frame_bytes: bytes.len() as u32,
        flags: 1,
        fd_count: count as u16,
        body_crc32c: crc32c(&bytes[128..]),
        ..Default::default()
    };
    env.encode(&mut bytes[..128]).unwrap();
    let crc = crc32c(&bytes[..128]);
    bytes[116..120].copy_from_slice(&crc.to_le_bytes());
    ControlFrame::decode(&bytes).unwrap();
    bytes
}

#[test]
fn complete_packets_and_owned_close_on_exec_fds() {
    let dir = PrivateDirectory::create_in(std::path::Path::new("/tmp"), "tw-control").unwrap();
    assert_eq!(
        std::fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777,
        0o700
    );
    let path = dir.path().join("control.sock");
    let listener = ControlListener::bind(&path).unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let mut client = Connection::connect(&path, deadline()).unwrap();
    let mut server = listener.accept(deadline()).unwrap();
    assert_eq!(server.peer_credentials().pid, std::process::id());
    for fd in [listener.as_fd(), client.as_fd(), server.as_fd()] {
        // SAFETY: all three descriptors remain owned by the live connection/listener.
        assert_ne!(
            unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        assert_ne!(
            unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) } & libc::O_NONBLOCK,
            0
        );
    }
    for count in [0, 1, 16] {
        let raw = frame(count);
        let file = File::open("/dev/null").unwrap();
        let fds = vec![file.as_fd(); count];
        client.send(&raw, &fds, deadline()).unwrap();
        let received = server.recv(deadline()).unwrap();
        assert_eq!(received.bytes(), raw);
        assert_eq!(received.fds().len(), count);
        for fd in received.fds() {
            // SAFETY: borrowed descriptor remains owned by received.
            assert_ne!(
                unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
                0
            );
        }
        assert!(file.metadata().is_ok(), "send must retain sender ownership");
    }
}

#[test]
fn timeout_eof_and_sender_validation() {
    let dir = PrivateDirectory::create_in(std::path::Path::new("/tmp"), "tw-control").unwrap();
    let path = dir.path().join("control.sock");
    let listener = ControlListener::bind(&path).unwrap();
    assert!(matches!(
        listener.accept(Instant::now() + Duration::from_millis(10)),
        Err(Error::Timeout)
    ));
    let mut client = Connection::connect(&path, deadline()).unwrap();
    let mut server = listener.accept(deadline()).unwrap();
    assert!(matches!(
        server.recv(Instant::now() + Duration::from_millis(10)),
        Err(Error::Timeout)
    ));
    let raw = frame(1);
    assert!(matches!(
        client.send(&raw, &[], deadline()),
        Err(Error::FdCount { .. })
    ));
    assert!(matches!(
        client.send(&[0; 127], &[], deadline()),
        Err(Error::Protocol(_))
    ));
    drop(client);
    assert!(matches!(server.recv(deadline()), Err(Error::Closed)));
}

#[test]
fn existing_paths_and_replacements_are_not_removed() {
    let dir = PrivateDirectory::create_in(std::path::Path::new("/tmp"), "tw-control").unwrap();
    let path = dir.path().join("control.sock");
    std::fs::write(&path, b"owned by someone else").unwrap();
    assert!(ControlListener::bind(&path).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"owned by someone else");
    std::fs::remove_file(&path).unwrap();
    let listener = ControlListener::bind(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    std::fs::write(&path, b"replacement").unwrap();
    drop(listener);
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
    std::fs::remove_file(path).unwrap();
}

#[test]
fn private_directory_rejects_unprotected_writable_parent() {
    let parent = PrivateDirectory::create_in(std::path::Path::new("/tmp"), "tw-parent").unwrap();
    std::fs::set_permissions(parent.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
    let result = PrivateDirectory::create_in(parent.path(), "nested");
    std::fs::set_permissions(parent.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(matches!(result, Err(Error::InsecureDirectory)));
    assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
}

#[test]
fn restrictive_permissions_and_cleanup_keep_unrelated_entries() {
    let dir = PrivateDirectory::create_in(std::path::Path::new("/tmp"), "tw-cleanup").unwrap();
    let dirname = dir.path().to_owned();
    let path = dir.path().join("control.sock");
    assert!(matches!(
        ControlListener::bind_with_mode(&path, 0o666),
        Err(Error::InsecureDirectory)
    ));
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o770)).unwrap();
    assert!(matches!(
        ControlListener::bind(&path),
        Err(Error::InsecureDirectory)
    ));
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let listener = ControlListener::bind_with_mode(&path, 0o660).unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o660
    );
    drop(listener);
    assert!(!path.exists());
    drop(dir);
    assert!(!dirname.exists());
    let dir = PrivateDirectory::create_in(std::path::Path::new("/tmp"), "tw-keep").unwrap();
    let path = dir.path().to_owned();
    std::fs::write(path.join("keep"), b"unrelated").unwrap();
    drop(dir);
    assert_eq!(std::fs::read(path.join("keep")).unwrap(), b"unrelated");
    std::fs::remove_file(path.join("keep")).unwrap();
    std::fs::remove_dir(path).unwrap();
}

#[test]
fn full_send_queue_expires_without_signals_or_partial_success() {
    let dir = PrivateDirectory::create_in(std::path::Path::new("/tmp"), "tw-full").unwrap();
    let path = dir.path().join("control.sock");
    let listener = ControlListener::bind(&path).unwrap();
    let mut client = Connection::connect(&path, deadline()).unwrap();
    let _server = listener.accept(deadline()).unwrap();
    let until = Instant::now() + Duration::from_millis(30);
    let bytes = frame(0);
    let mut successes = 0;
    loop {
        match client.send(&bytes, &[], until) {
            Ok(()) => successes += 1,
            Err(Error::Timeout) => break,
            Err(error) => panic!("unexpected send result: {error}"),
        }
    }
    assert!(successes > 0);
    assert!(Instant::now() < until + Duration::from_secs(1));
}

#[test]
fn shared_os_monotonic_clock() {
    let a = monotonic_ns().unwrap();
    let b = monotonic_ns().unwrap();
    assert!(a > 0 && b >= a);
}

fn checksum(bytes: &mut [u8]) {
    let body = crc32c(&bytes[128..]);
    bytes[112..116].copy_from_slice(&body.to_le_bytes());
    bytes[116..120].fill(0);
    let header = crc32c(&bytes[..128]);
    bytes[116..120].copy_from_slice(&header.to_le_bytes());
}

// Deliberately bypasses the production sender's validation, only in this test binary.
fn raw_send(fd: std::os::fd::BorrowedFd<'_>, bytes: &[u8], fds: &[i32]) {
    use std::mem::{size_of, zeroed};
    let mut iov = libc::iovec {
        iov_base: bytes.as_ptr().cast_mut().cast(),
        iov_len: bytes.len(),
    };
    // SAFETY: the bounded descriptor list fits CMSG_SPACE's integer input.
    let space = unsafe { libc::CMSG_SPACE(std::mem::size_of_val(fds) as u32) } as usize;
    let mut ancillary = vec![0usize; space.div_ceil(size_of::<usize>())];
    // SAFETY: zero is valid for unused msghdr fields.
    let mut msg: libc::msghdr = unsafe { zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    if !fds.is_empty() {
        msg.msg_control = ancillary.as_mut_ptr().cast();
        msg.msg_controllen = space;
        // SAFETY: aligned control buffer has space for this header and every descriptor.
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of_val(fds) as u32) as usize;
            for (i, raw) in fds.iter().enumerate() {
                libc::CMSG_DATA(cmsg)
                    .add(i * size_of::<i32>())
                    .cast::<i32>()
                    .write_unaligned(*raw);
            }
        }
    }
    // SAFETY: payload and ancillary storage remain live throughout sendmsg.
    let sent = unsafe {
        libc::sendmsg(
            fd.as_raw_fd(),
            &msg,
            libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
        )
    };
    assert_eq!(
        sent,
        bytes.len() as isize,
        "{}",
        std::io::Error::last_os_error()
    );
}

fn wait_child(mut child: std::process::Child) {
    let until = deadline();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "child failed: {status}");
            return;
        }
        if Instant::now() >= until {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child exceeded its deadline");
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn malformed_packets_close_all_fds_in_isolated_process() {
    wait_child(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "rejection_child", "--nocapture"])
            .env("TICKWIRE_REJECTION_CHILD", "1")
            .spawn()
            .unwrap(),
    );
}

#[test]
fn rejection_child() {
    if std::env::var_os("TICKWIRE_REJECTION_CHILD").is_none() {
        return;
    }
    let dir = PrivateDirectory::create_in(std::path::Path::new("/tmp"), "tw-reject").unwrap();
    let path = dir.path().join("control.sock");
    let listener = ControlListener::bind(&path).unwrap();
    let mut client = Connection::connect(&path, deadline()).unwrap();
    let mut server = listener.accept(deadline()).unwrap();
    let file = File::open("/dev/null").unwrap();
    let raw_fd = file.as_raw_fd();
    let count_fds = || std::fs::read_dir("/proc/self/fd").unwrap().count();
    let baseline = count_fds();
    let mut bad_crc = frame(1);
    bad_crc[128] ^= 1;
    let mut duplicate = frame(2);
    duplicate[320 + 76..320 + 78].copy_from_slice(&0u16.to_le_bytes());
    checksum(&mut duplicate);
    let mut index_out_of_range = frame(1);
    index_out_of_range[192 + 76..192 + 78].copy_from_slice(&1u16.to_le_bytes());
    checksum(&mut index_out_of_range);
    let mut oversized = frame(1);
    oversized.resize(MAX_FRAME_BYTES + 1, 0);
    let cases = [
        ("short", vec![0; 127], 1, "protocol"),
        ("crc", bad_crc, 1, "protocol"),
        ("missing FD", frame(2), 1, "count"),
        ("extra FD", frame(1), 2, "count"),
        ("duplicate index", duplicate, 2, "protocol"),
        ("out of range index", index_out_of_range, 1, "protocol"),
        ("data truncated", oversized, 1, "truncated"),
        ("ancillary truncated", frame(16), 32, "truncated"),
        ("zero payload with FD", vec![], 1, "closed"),
    ];
    for _ in 0..32 {
        for (name, bytes, count, expected) in &cases {
            raw_send(client.as_fd(), bytes, &vec![raw_fd; *count]);
            let error = match server.recv(deadline()) {
                Ok(_) => panic!("{name}: invalid frame accepted"),
                Err(error) => error,
            };
            let kind = match error {
                Error::Protocol(_) => "protocol",
                Error::FdCount { .. } => "count",
                Error::Truncated => "truncated",
                Error::Closed => "closed",
                _ => panic!("{name}: unexpected {error:?}"),
            };
            assert_eq!(kind, *expected, "{name}");
            assert_eq!(count_fds(), baseline, "{name}: received descriptors leaked");
            client.send(&frame(0), &[], deadline()).unwrap();
            assert_eq!(server.recv(deadline()).unwrap().bytes(), frame(0));
        }
    }
    client
        .send(&frame(16), &[file.as_fd(); 16], deadline())
        .unwrap();
    let received = server.recv(deadline()).unwrap();
    assert_eq!(count_fds(), baseline + 16);
    let (_, fds) = received.into_parts();
    drop(fds);
    assert_eq!(count_fds(), baseline);

    // Target the blocking thread with repeated interruptions. Signals must not renew
    // its absolute deadline; this helper is a separate process from parallel tests.
    extern "C" fn ignore_signal(_: libc::c_int) {}
    // SAFETY: zeroed sigaction is initialized below before installation.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = ignore_signal as *const () as usize;
    // SAFETY: valid signal set and action pointers; the handler has no side effects.
    assert_eq!(unsafe { libc::sigemptyset(&mut action.sa_mask) }, 0);
    assert_eq!(
        unsafe { libc::sigaction(libc::SIGUSR1, &action, std::ptr::null_mut()) },
        0
    );
    // SAFETY: pthread_self returns the live calling thread's identity.
    let thread = unsafe { libc::pthread_self() };
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let interrupt_stop = stop.clone();
    let interrupter = std::thread::spawn(move || {
        let until = Instant::now() + Duration::from_millis(500);
        while Instant::now() < until && !interrupt_stop.load(std::sync::atomic::Ordering::Relaxed) {
            // SAFETY: target thread remains alive until this worker has joined.
            assert_eq!(unsafe { libc::pthread_kill(thread, libc::SIGUSR1) }, 0);
            std::thread::sleep(Duration::from_millis(2));
        }
    });
    let start = Instant::now();
    let result = server.recv(start + Duration::from_millis(30));
    let elapsed = start.elapsed();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    interrupter.join().unwrap();
    assert!(matches!(result, Err(Error::Timeout)));
    assert!(
        elapsed < Duration::from_millis(400),
        "EINTR extended the deadline: {elapsed:?}"
    );
}

#[test]
fn independent_exec_connect_has_child_credentials_and_shared_clock() {
    let dir = PrivateDirectory::create_in(std::path::Path::new("/tmp"), "tw-exec").unwrap();
    let path = dir.path().join("control.sock");
    let listener = ControlListener::bind(&path).unwrap();
    let before = monotonic_ns().unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "pathname_child", "--nocapture"])
        .env("TICKWIRE_CHILD_SOCKET", &path)
        .env("TICKWIRE_PARENT_PID", std::process::id().to_string())
        .spawn()
        .unwrap();
    let mut connection = listener.accept(deadline()).unwrap();
    assert_eq!(connection.peer_credentials().pid, child.id());
    // SAFETY: these identity calls have no preconditions.
    assert_eq!(connection.peer_credentials().uid, unsafe {
        libc::geteuid()
    });
    assert_eq!(connection.peer_credentials().gid, unsafe {
        libc::getegid()
    });
    let received = connection.recv(deadline()).unwrap();
    let reported = ControlEnvelope::decode(&received.bytes()[..128])
        .unwrap()
        .expected_state_version;
    let after = monotonic_ns().unwrap();
    assert!(before <= reported && reported <= after);
    wait_child(child);
}

#[test]
fn pathname_child() {
    let Some(path) = std::env::var_os("TICKWIRE_CHILD_SOCKET") else {
        return;
    };
    let parent: u32 = std::env::var("TICKWIRE_PARENT_PID")
        .unwrap()
        .parse()
        .unwrap();
    let mut connection = Connection::connect(std::path::Path::new(&path), deadline()).unwrap();
    assert_eq!(connection.peer_credentials().pid, parent);
    let mut bytes = frame(0);
    let mut envelope = ControlEnvelope::decode(&bytes[..128]).unwrap();
    envelope.expected_state_version = monotonic_ns().unwrap();
    envelope.encode(&mut bytes[..128]).unwrap();
    checksum(&mut bytes);
    connection.send(&bytes, &[], deadline()).unwrap();
}

#[test]
fn received_descriptor_does_not_survive_exec() {
    use std::os::unix::fs::MetadataExt;
    let dir = PrivateDirectory::create_in(std::path::Path::new("/tmp"), "tw-cloexec").unwrap();
    let path = dir.path().join("control.sock");
    let listener = ControlListener::bind(&path).unwrap();
    let mut client = Connection::connect(&path, deadline()).unwrap();
    let mut server = listener.accept(deadline()).unwrap();
    let filename = dir.path().join("unique-file");
    let file = File::create(&filename).unwrap();
    let metadata = file.metadata().unwrap();
    client.send(&frame(1), &[file.as_fd()], deadline()).unwrap();
    let received = server.recv(deadline()).unwrap();
    wait_child(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "cloexec_child", "--nocapture"])
            .env("TICKWIRE_FORBIDDEN_INODE", metadata.ino().to_string())
            .env("TICKWIRE_FORBIDDEN_DEVICE", metadata.dev().to_string())
            .spawn()
            .unwrap(),
    );
    assert_eq!(received.fds().len(), 1); // Still owned by this parent after exec.
    std::fs::remove_file(filename).unwrap();
}

#[test]
fn cloexec_child() {
    use std::os::unix::fs::MetadataExt;
    let Ok(inode) = std::env::var("TICKWIRE_FORBIDDEN_INODE") else {
        return;
    };
    let inode: u64 = inode.parse().unwrap();
    let device: u64 = std::env::var("TICKWIRE_FORBIDDEN_DEVICE")
        .unwrap()
        .parse()
        .unwrap();
    for entry in std::fs::read_dir("/proc/self/fd").unwrap() {
        if let Ok(metadata) = std::fs::metadata(entry.unwrap().path()) {
            assert!(
                metadata.ino() != inode || metadata.dev() != device,
                "received FD survived exec"
            );
        }
    }
}

#[test]
fn inherited_socketpair_peer_pid_is_creator_after_real_fork_exec() {
    use std::ffi::CString;
    use std::os::fd::{FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    let mut pair = [-1; 2];
    // SAFETY: pair is writable storage for two newly owned descriptors.
    assert_eq!(
        unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
                0,
                pair.as_mut_ptr(),
            )
        },
        0
    );
    // SAFETY: successful socketpair returned these distinct owned descriptors.
    let parent = unsafe { OwnedFd::from_raw_fd(pair[0]) };
    let child_end = unsafe { OwnedFd::from_raw_fd(pair[1]) };
    let exe = CString::new(std::env::current_exe().unwrap().as_os_str().as_bytes()).unwrap();
    let arguments = [
        exe.clone(),
        CString::new("--exact").unwrap(),
        CString::new("inherited_peer_child").unwrap(),
        CString::new("--nocapture").unwrap(),
    ];
    let mut argv: Vec<_> = arguments.iter().map(|s| s.as_ptr()).collect();
    argv.push(std::ptr::null());
    let environment = CString::new(format!("TICKWIRE_INHERITED_FD={}", pair[1])).unwrap();
    let envp = [environment.as_ptr(), std::ptr::null()];
    // SAFETY: child executes only async-signal-safe libc calls before exec/_exit.
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0);
    if pid == 0 {
        // SAFETY: no allocation, lock, Rust unwinding, or destructor occurs in the child.
        unsafe {
            libc::close(pair[0]);
            if libc::fcntl(pair[1], libc::F_SETFD, 0) < 0 {
                libc::_exit(125);
            }
            libc::execve(exe.as_ptr(), argv.as_ptr(), envp.as_ptr());
            libc::_exit(126);
        }
    }
    drop(child_end);
    let mut pfd = libc::pollfd {
        fd: parent.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one initialized writable poll descriptor.
    assert_eq!(unsafe { libc::poll(&mut pfd, 1, 3000) }, 1);
    let mut bytes = [0u8; 8];
    // SAFETY: live writable eight-byte output buffer.
    assert_eq!(
        unsafe {
            libc::recv(
                parent.as_raw_fd(),
                bytes.as_mut_ptr().cast(),
                bytes.len(),
                libc::MSG_DONTWAIT,
            )
        },
        8
    );
    assert_eq!(
        u32::from_le_bytes(bytes[..4].try_into().unwrap()),
        pid as u32
    );
    assert_eq!(
        u32::from_le_bytes(bytes[4..].try_into().unwrap()),
        std::process::id()
    );
    // SAFETY: zeroed ucred and correctly sized writable getsockopt output.
    let mut credentials: libc::ucred = unsafe { std::mem::zeroed() };
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    assert_eq!(
        unsafe {
            libc::getsockopt(
                parent.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut credentials as *mut libc::ucred).cast(),
                &mut length,
            )
        },
        0
    );
    assert_eq!(credentials.pid as u32, std::process::id());
    assert_ne!(
        credentials.pid, pid,
        "inherited socketpair cannot authenticate the exec child"
    );
    let until = deadline();
    loop {
        let mut status = 0;
        // SAFETY: this pid is our unreaped child, status is writable.
        let result = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
        assert!(result >= 0);
        if result == pid {
            assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);
            break;
        }
        if Instant::now() >= until {
            // SAFETY: kill/reap only the unreaped child created above.
            unsafe {
                libc::kill(pid, libc::SIGKILL);
                libc::waitpid(pid, &mut status, 0);
            }
            panic!("fork child timed out");
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn inherited_peer_child() {
    use std::os::fd::{FromRawFd, OwnedFd};
    let Some(fd) = std::env::var_os("TICKWIRE_INHERITED_FD") else {
        return;
    };
    let raw: i32 = fd.to_str().unwrap().parse().unwrap();
    // SAFETY: parent deliberately passed this one owned descriptor across exec.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    let mut bytes = [0u8; 8];
    bytes[..4].copy_from_slice(&std::process::id().to_le_bytes());
    // SAFETY: correctly sized writable getsockopt output.
    let mut credentials: libc::ucred = unsafe { std::mem::zeroed() };
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    assert_eq!(
        unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut credentials as *mut libc::ucred).cast(),
                &mut length,
            )
        },
        0
    );
    bytes[4..].copy_from_slice(&(credentials.pid as u32).to_le_bytes());
    raw_send(fd.as_fd(), &bytes, &[]);
}
