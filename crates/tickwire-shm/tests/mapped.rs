#![cfg(target_os = "linux")]
#![allow(unsafe_code)]

mod common;
use common::record;
use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Write},
    os::fd::AsRawFd,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tickwire_ipc::ResourceHeader;
use tickwire_ipc::Wire;
use tickwire_shm::*;

fn header() -> ResourceHeader {
    ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        header_bytes: 4096,
        resource_kind: 1,
        target_arch: if cfg!(target_arch = "aarch64") { 2 } else { 1 },
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        record_bytes: 512,
        capacity: 64,
        resource_bytes: 8192 + 64 * 512,
        resource_id: 1u128.to_le_bytes(),
        channel_id: 2u128.to_le_bytes(),
        producer_instance: 3u128.to_le_bytes(),
        producer_generation: 1,
        channel_epoch: 1,
        ring_epoch: 1,
        feed_generation: 1,
        first_seq: 1,
        initial_membership_version: 1,
        producer_page_offset: 4096,
        slot_offset: 8192,
        max_consumers: 4,
        schema_major: 1,
        clock_domain: 1,
        required_features: 0x1c,
        topic_id: 1,
        ..Default::default()
    }
}
fn sub(p: &Publisher<4>, instance: u128) -> SubscriptionKey {
    SubscriptionKey {
        bank: p.key(),
        instance,
        generation: 1,
        cursor: instance + 100,
    }
}
fn connect(p: &mut Publisher<4>, id: u128) -> Subscriber<64> {
    let key = sub(p, id);
    let baseline = p.join(key).unwrap() - 1;
    // Same test owns both endpoints; the exact pending member remains registered.
    let reader = unsafe {
        Subscriber::attach(p.reader_fd().unwrap(), p.header().clone(), key, baseline).unwrap()
    };
    unsafe {
        p.register_cursor(key, reader.cursor_fd().unwrap()).unwrap();
    }
    reader
}

#[test]
fn mapped_broadcast_borrows_the_same_record_and_protects_gaps() {
    let mut p = Publisher::<4>::create(header()).unwrap();
    let a = connect(&mut p, 1);
    let b = connect(&mut p, 2);
    for seq in 1..=64 {
        p.publish(&[&record(seq)]).unwrap();
    }
    let first = a.borrow().unwrap().unwrap();
    let second = a.borrow().unwrap().unwrap();
    assert_eq!(first.bytes(), &record(1));
    assert_eq!(second.bytes(), &record(2));
    drop(second);
    assert_eq!(a.watermarks().released, 0);
    let other = b.borrow().unwrap().unwrap();
    assert_eq!(other.bytes(), first.bytes());
    drop(other);
    assert!(matches!(
        p.publish(&[&record(65)]),
        Err(MappedError::Protocol(Error::Full))
    ));
    drop(first);
    p.publish(&[&record(65)]).unwrap();
    assert_eq!(a.watermarks().released, 2);
    assert_eq!(a.watermarks().applied, 0);
}

#[test]
fn wrong_batch_is_rejected_before_any_bytes_are_published() {
    let mut p = Publisher::<4>::create(header()).unwrap();
    let r = connect(&mut p, 1);
    assert!(p.publish(&[&record(1), &record(3)]).is_err());
    assert_eq!(p.published(), 0);
    assert!(r.borrow().unwrap().is_none());
    p.publish(&[&record(1), &record(2)]).unwrap();
    assert_eq!(r.borrow().unwrap().unwrap().bytes(), &record(1));
    assert_eq!(r.borrow().unwrap().unwrap().bytes(), &record(2));
}

#[test]
fn freeze_preserves_bytes_for_an_outstanding_reader() {
    let mut p = Publisher::<4>::create(header()).unwrap();
    let r = connect(&mut p, 1);
    p.prepare_freeze(2, 1).unwrap();
    p.publish(&[&record(1)]).unwrap();
    let held = r.borrow().unwrap().unwrap();
    p.freeze().unwrap();
    assert!(p.publish(&[&record(2)]).is_err());
    assert_eq!(held.bytes(), &record(1));
    r.start_quiesce();
    assert!(r.quiesced().is_err());
    drop(held);
    p.detach(r.quiesced().unwrap()).unwrap();
    assert!(r.borrow().is_err());
}

#[test]
fn descriptor_mismatch_is_rejected() {
    let mut p = Publisher::<4>::create(header()).unwrap();
    let key = sub(&p, 1);
    let baseline = p.join(key).unwrap() - 1;
    let mut wrong = p.header().clone();
    wrong.ring_epoch += 1;
    assert!(
        unsafe { Subscriber::<64>::attach(p.reader_fd().unwrap(), wrong, key, baseline) }.is_err()
    );
}

#[test]
fn pure_state_quiescence_cannot_detach_a_real_live_borrow() {
    let mut p = Publisher::<4>::create(header()).unwrap();
    let r = connect(&mut p, 1);
    p.publish(&[&record(1)]).unwrap();
    let held = r.borrow().unwrap().unwrap();
    let mut fabricated = Reader::<1>::new(sub(&p, 1), 0).unwrap();
    fabricated.start_quiesce();
    assert!(p.detach(fabricated.quiesced().unwrap()).is_err());
    assert_eq!(held.bytes(), &record(1));
}

#[test]
fn required_abi_features_and_generation_limits_are_enforced() {
    let mut valid = header();
    valid.required_features = 0x1c;
    assert!(Publisher::<4>::create(valid.clone()).is_ok());
    for features in [0, 0x0c, 0x1e, 1 << 63] {
        let mut bad = valid.clone();
        bad.required_features = features;
        assert!(Publisher::<4>::create(bad).is_err());
    }
    let mut bad = valid;
    bad.ring_epoch = 1 << 63;
    assert!(Publisher::<4>::create(bad).is_err());
}

#[test]
fn forgotten_guard_prevents_reuse_and_quiescence() {
    let mut p = Publisher::<4>::create(header()).unwrap();
    let r = connect(&mut p, 1);
    for sequence in 1..=64 {
        p.publish(&[&record(sequence)]).unwrap();
    }
    std::mem::forget(r.borrow().unwrap().unwrap());
    r.start_quiesce();
    assert!(r.quiesced().is_err());
    assert!(matches!(
        p.publish(&[&record(65)]),
        Err(MappedError::Protocol(Error::Full))
    ));
}

#[test]
fn bank_admission_respects_the_advertised_consumer_limit() {
    let mut h = header();
    h.max_consumers = 1;
    let mut p = Publisher::<4>::create(h).unwrap();
    let key = sub(&p, 1);
    p.join(key).unwrap();
    assert_eq!(p.join(key).unwrap(), 1);
    assert!(matches!(
        p.join(sub(&p, 2)),
        Err(MappedError::Protocol(Error::Capacity))
    ));
}

#[test]
fn repeated_quiesce_does_not_reopen_a_completed_cursor() {
    let mut p = Publisher::<4>::create(header()).unwrap();
    let r = connect(&mut p, 1);
    r.start_quiesce();
    let evidence = r.quiesced().unwrap();
    r.start_quiesce();
    p.detach(evidence).unwrap();
}

// Linux fixture transport only: /proc/<pid>/fd duplicates a sealed capability.
// Production control-plane SCM_RIGHTS/authentication is a separate module.
#[test]
fn shared_reader_child() {
    let Ok(path) = std::env::var("TICKWIRE_RING_CHILD_BANK") else {
        return;
    };
    let instance: u128 = std::env::var("TICKWIRE_RING_CHILD_ID")
        .unwrap()
        .parse()
        .unwrap();
    let count: u64 = std::env::var("TICKWIRE_RING_CHILD_COUNT")
        .unwrap()
        .parse()
        .unwrap();
    let mut file = File::open(path).unwrap();
    let mut bytes = [0; 4096];
    file.read_exact(&mut bytes).unwrap();
    let actual = ResourceHeader::decode(&bytes).unwrap();
    let mut expected = header();
    expected.max_consumers = 32;
    expected.descriptor_hash = actual.descriptor_hash;
    assert_eq!(actual, expected);
    let key = SubscriptionKey {
        bank: BankKey {
            resource: 1,
            channel: 2,
            channel_epoch: 1,
            ring_epoch: 1,
            feed_generation: 1,
        },
        instance,
        generation: 1,
        cursor: instance + 100,
    };
    // Parent registered this exact pending reader before spawning; both peers use SDK.
    let r = unsafe { Subscriber::<64>::attach(file.into(), actual, key, 0).unwrap() };
    let cursor = r.cursor_fd().unwrap();
    println!("TW_CURSOR {} {}", std::process::id(), cursor.as_raw_fd());
    std::io::stdout().flush().unwrap();
    let mut go = String::new();
    std::io::stdin().read_line(&mut go).unwrap();
    assert_eq!(go.trim(), "GO");
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut next = 1;
    while next <= count {
        if let Some(guard) = r.borrow().unwrap() {
            assert_eq!(guard.sequence(), next);
            assert_eq!(guard.bytes(), &record(next));
            drop(guard);
            r.apply(next).unwrap();
            next += 1;
        } else {
            assert!(Instant::now() < deadline, "publication timeout at {next}");
            std::thread::yield_now();
        }
    }
    assert_eq!(
        r.watermarks(),
        Watermarks {
            read: count,
            released: count,
            applied: count
        }
    );
    r.start_quiesce();
    r.quiesced().unwrap();
    println!("TW_DONE {count}");
}

struct Children(Vec<Child>);
impl Drop for Children {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
#[test]
fn real_process_broadcast_to_1_2_4_8_16_32_consumers() {
    const COUNT: u64 = 1024;
    for consumers in [1, 2, 4, 8, 16, 32] {
        let mut h = header();
        h.max_consumers = 32;
        let mut p = Publisher::<32>::create(h).unwrap();
        let bank = p.reader_fd().unwrap();
        let bank_path = format!("/proc/{}/fd/{}", std::process::id(), bank.as_raw_fd());
        let mut children = Children(Vec::new());
        let mut output_threads = Vec::new();
        for instance in 1..=consumers {
            let key = SubscriptionKey {
                bank: p.key(),
                instance,
                generation: 1,
                cursor: instance + 100,
            };
            assert_eq!(p.join(key).unwrap(), 1);
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "shared_reader_child", "--nocapture"])
                .env("TICKWIRE_RING_CHILD_BANK", &bank_path)
                .env("TICKWIRE_RING_CHILD_ID", instance.to_string())
                .env("TICKWIRE_RING_CHILD_COUNT", COUNT.to_string())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            children.0.push(child);
            let (tx, rx) = std::sync::mpsc::channel();
            output_threads.push(std::thread::spawn(move || {
                let mut reader = BufReader::new(stdout);
                let mut all = String::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap() == 0 {
                        break;
                    }
                    all.push_str(&line);
                    if let Some(start) = line.find("TW_CURSOR ") {
                        tx.send(line[start..].trim().to_owned()).unwrap();
                        break;
                    }
                }
                reader.read_to_string(&mut all).unwrap();
                all
            }));
            let ready = rx
                .recv_timeout(Duration::from_secs(30))
                .expect("reader mapped handshake");
            let parts: Vec<_> = ready.split_whitespace().collect();
            assert_eq!(parts.len(), 3);
            let cursor = File::open(format!("/proc/{}/fd/{}", parts[1], parts[2])).unwrap();
            // Child SDK owns this independently sealed cursor; parent verifies the header.
            unsafe {
                p.register_cursor(key, cursor.into()).unwrap();
            }
        }
        for child in &mut children.0 {
            child.stdin.as_mut().unwrap().write_all(b"GO\n").unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(45);
        for sequence in 1..=COUNT {
            let bytes = record(sequence);
            loop {
                match p.publish(&[&bytes]) {
                    Ok(_) => break,
                    Err(MappedError::Protocol(Error::Full)) => {
                        assert!(Instant::now() < deadline, "consumer stalled at {sequence}");
                        std::thread::yield_now();
                    }
                    Err(e) => panic!("unexpected publish failure: {e}"),
                }
            }
        }
        for child in &mut children.0 {
            assert!(child.wait().unwrap().success());
        }
        for thread in output_threads {
            assert!(thread.join().unwrap().contains("TW_DONE 1024"));
        }
        println!("shared broadcast: {consumers} independent processes, {COUNT} records each");
    }
}
