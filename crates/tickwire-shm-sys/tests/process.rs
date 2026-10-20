#![cfg(target_os = "linux")]

use std::io::{BufRead, Write};
use std::os::fd::AsRawFd;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use tickwire_shm_sys::{ReaderMapping, WriterMapping};

const LENGTH: usize = 65536;
const PAYLOAD_START: usize = 4096;

// Re-exec the test binary rather than fork a multithreaded Rust test harness.
// /proc/<parent>/fd is only this test's FD delivery mechanism; production
// control transport is responsible for SCM_RIGHTS and identity checks.
#[test]
fn child_read_only_acquire_observes_parent_payload() {
    let mut writer = WriterMapping::create("independent-process", LENGTH).unwrap();
    writer.seal_for_readers().unwrap();
    let fd = writer.duplicate_fd().unwrap();
    let path = format!("/proc/{}/fd/{}", std::process::id(), fd.as_raw_fd());
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_entry", "--nocapture"])
        .env("TICKWIRE_SHM_TEST_FD", path)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let stdout = std::thread::spawn(move || {
        for line in std::io::BufReader::new(output).lines() {
            let line = line.unwrap();
            if line == "TICKWIRE_MAP_READY" {
                let _ = ready_tx.send(());
            }
        }
    });
    if ready_rx.recv_timeout(Duration::from_secs(5)).is_err() {
        let _ = child.kill();
        let _ = child.wait();
        stdout.join().unwrap();
        panic!("child never established its read-only mapping");
    }
    let payload: Vec<u8> = (PAYLOAD_START..LENGTH).map(|i| (i % 251) as u8).collect();
    // SAFETY: only this process writes, payload is not yet published, and the
    // child accesses it only after acquiring publication 1 at aligned offset 128.
    unsafe {
        writer.write_bytes(PAYLOAD_START, &payload).unwrap();
        writer.store_u64_release(128, 1).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("child reader timed out");
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    stdout.join().unwrap();
}

#[test]
fn child_entry() {
    let Ok(path) = std::env::var("TICKWIRE_SHM_TEST_FD") else {
        return;
    };
    let file = std::fs::File::open(path).unwrap();
    let reader = ReaderMapping::map(file.into(), LENGTH).unwrap();
    println!("TICKWIRE_MAP_READY");
    std::io::stdout().flush().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        // SAFETY: the parent accesses this location only with aligned u64 stores.
        let published = unsafe { reader.load_u64_acquire(128) }.unwrap();
        if published == 1 {
            break;
        }
        assert!(Instant::now() < deadline, "publication was never observed");
        std::thread::yield_now();
    }
    // SAFETY: release/acquire publication observed, parent never reuses payload.
    let bytes = unsafe { reader.read_bytes(PAYLOAD_START, LENGTH - PAYLOAD_START) }.unwrap();
    for (index, &byte) in bytes.iter().enumerate() {
        assert_eq!(byte, ((index + PAYLOAD_START) % 251) as u8);
    }
}
