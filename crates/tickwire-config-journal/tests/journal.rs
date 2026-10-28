use std::{
    fs,
    path::PathBuf,
    sync::{
        RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};
use tickwire_config_journal::*;
use tickwire_journal::store::FileStore;
// A child retains other threads' lock descriptions between fork and exec.
// Shared guards keep ordinary WAL tests concurrent; process creation is exclusive
// for the complete test lifetime, including child wait and journal recovery.
static FORK_BARRIER: RwLock<()> = RwLock::new(());
static NEXT: AtomicUsize = AtomicUsize::new(1);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "tickwire-local-config-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
    fn store(&self) -> FileStore {
        FileStore::open(&self.0).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn config() -> Config {
    Config {
        scope: Scope {
            cluster: "cluster-a".into(),
            namespace: [2; 16],
            host: "host-a".into(),
            process: "exec-a".into(),
            slot: [3; 16],
            locator_generation: 1,
        },
        stream_epoch: [4; 16],
        writer_epoch: 1,
        max_operations: 8,
        max_recorded_bytes: 65536,
    }
}
fn prepare(n: u8, base: Option<Version>) -> Event {
    Event::Prepared(Box::new(Prepared {
        operation: [n; 16],
        base,
        next: version(n),
        control_commit_ref: [5; 32],
        target_set_hash: [6; 32],
        expected_instance: [7; 16],
        expected_boot: [8; 16],
        expected_host_boot: [9; 16],
        exec_generation: 2,
        projection_object_bytes: 2048,
    }))
}
fn version(n: u8) -> Version {
    Version {
        revision: u64::from(n),
        global_hash: [n; 32],
        projection_hash: [2 * n; 32],
        policy_revision: 1,
        object_hash: [3 * n; 32],
    }
}
fn file(n: u8) -> FileFact {
    FileFact {
        render_hash: [n; 32],
        bytes: 1024,
        device: 5,
        inode: u64::from(n),
    }
}
fn runtime() -> RuntimeFact {
    RuntimeFact {
        instance: [7; 16],
        boot: [8; 16],
        host_boot: [9; 16],
        exec_generation: 2,
        applied_event_seq: 100,
        witness_ref: [10; 32],
    }
}
fn complete<S: tickwire_journal::store::Store>(
    j: &mut Journal<S>,
    n: u8,
    base: Option<Version>,
) -> Receipt {
    let p = j.record(&prepare(n, base)).unwrap();
    let d = j
        .record(&Event::Desired {
            operation: [n; 16],
            prepared_ref: p.record_hash(),
            file: file(11),
        })
        .unwrap();
    let r = j
        .record(&Event::RuntimeObserved {
            operation: [n; 16],
            desired_ref: d.record_hash(),
            fact: runtime(),
        })
        .unwrap();
    j.record(&Event::Effective {
        operation: [n; 16],
        runtime_ref: r.record_hash(),
        file: file(12),
    })
    .unwrap()
}
#[test]
fn actual_wal_records_all_steps_and_reopens_exact_progress() {
    let _fork_barrier = FORK_BARRIER.read().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::new();
    let mut j = Journal::create(f.store(), config()).unwrap();
    let end = complete(&mut j, 1, None);
    assert_eq!(end.sequence(), 5);
    assert_eq!(j.phase([1; 16]), Some(Phase::Effective));
    assert_eq!(j.effective_record(), Some(&version(1)));
    drop(j);
    let mut j = Journal::recover(f.store(), config(), Some(end.frontier())).unwrap();
    assert_eq!(j.effective_record(), Some(&version(1)));
    assert_eq!(complete(&mut j, 2, Some(version(1))).sequence(), 9);
}
#[test]
fn recovery_views_expose_only_recorded_steps_and_original_receipts() {
    let _fork_barrier = FORK_BARRIER.read().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::new();
    let mut j = Journal::create(f.store(), config()).unwrap();
    assert!(j.operation([1; 16]).is_none());
    assert_eq!(j.active_operation(), None);
    assert_eq!(j.scope(), &config().scope);
    let p = prepare(1, None);
    let receipt = j.record(&p).unwrap();
    assert_eq!(j.active_operation(), Some([1; 16]));
    let view = j.operation([1; 16]).unwrap();
    assert_eq!(view.event(Phase::Prepared), Some(&p));
    assert_eq!(view.receipt(Phase::Prepared), Some(&receipt));
    assert!(view.event(Phase::Desired).is_none());
    assert_eq!(j.operations().count(), 1);
    let end = complete(&mut j, 1, None);
    assert_eq!(j.active_operation(), None);
    drop(j);
    let j = Journal::recover(f.store(), config(), Some(end.frontier())).unwrap();
    let view = j.operation([1; 16]).unwrap();
    assert_eq!(view.prepared().next, version(1));
    assert_eq!(view.event(Phase::Prepared), Some(&p));
    assert_eq!(view.receipt(Phase::Prepared), Some(&receipt));
    assert_eq!(view.receipt(Phase::Effective), Some(&end));
    assert!(matches!(
        view.event(Phase::Effective),
        Some(Event::Effective { .. })
    ));
}
#[test]
fn exact_retry_after_lost_response_returns_original_sequence() {
    let _fork_barrier = FORK_BARRIER.read().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::new();
    let mut j = Journal::create(f.store(), config()).unwrap();
    let event = prepare(1, None);
    let original = j.record(&event).unwrap();
    assert_eq!(j.record(&event).unwrap(), original);
    drop(j);
    let mut j = Journal::recover(f.store(), config(), Some(original.frontier())).unwrap();
    assert_eq!(j.record(&event).unwrap(), original);
    assert_eq!(j.durable_frontier().unwrap().last_seq, 2);
    let Event::Prepared(mut changed) = event else {
        unreachable!()
    };
    changed.control_commit_ref = [19; 32];
    assert!(j.record(&Event::Prepared(changed)).is_err());
}
#[test]
fn skipped_steps_wrong_links_and_cross_boot_runtime_are_rejected() {
    let _fork_barrier = FORK_BARRIER.read().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::new();
    let mut j = Journal::create(f.store(), config()).unwrap();
    let p = j.record(&prepare(1, None)).unwrap();
    assert!(
        j.record(&Event::RuntimeObserved {
            operation: [1; 16],
            desired_ref: p.record_hash(),
            fact: runtime()
        })
        .is_err()
    );
    assert!(
        j.record(&Event::Desired {
            operation: [1; 16],
            prepared_ref: [99; 32],
            file: file(11)
        })
        .is_err()
    );
    let d = j
        .record(&Event::Desired {
            operation: [1; 16],
            prepared_ref: p.record_hash(),
            file: file(11),
        })
        .unwrap();
    for n in 0..6 {
        let mut r = runtime();
        match n {
            0 => r.instance = [88; 16],
            1 => r.boot = [88; 16],
            2 => r.host_boot = [88; 16],
            3 => r.exec_generation = 3,
            4 => r.applied_event_seq = 0,
            _ => r.witness_ref = [0; 32],
        };
        assert!(
            j.record(&Event::RuntimeObserved {
                operation: [1; 16],
                desired_ref: d.record_hash(),
                fact: r
            })
            .is_err(),
            "case {n}"
        );
    }
    assert_eq!(j.durable_frontier().unwrap().last_seq, 3);
}
#[test]
fn one_pending_operation_and_exact_effective_base_are_enforced() {
    let _fork_barrier = FORK_BARRIER.read().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::new();
    let mut j = Journal::create(f.store(), config()).unwrap();
    j.record(&prepare(1, None)).unwrap();
    assert!(j.record(&prepare(2, None)).is_err());
    complete(&mut j, 1, None);
    assert!(j.record(&prepare(2, None)).is_err());
    let mut wrong = version(1);
    wrong.object_hash = [99; 32];
    assert!(j.record(&prepare(2, Some(wrong))).is_err());
    assert!(j.record(&prepare(2, Some(version(1)))).is_ok());
}
#[test]
fn scope_writer_and_capacity_failures_preserve_existing_history() {
    let _fork_barrier = FORK_BARRIER.read().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::new();
    let mut cfg = config();
    cfg.max_operations = 1;
    let mut j = Journal::create(f.store(), cfg.clone()).unwrap();
    let end = complete(&mut j, 1, None);
    assert!(j.record(&prepare(2, Some(version(1)))).is_err());
    drop(j);
    let mut wrong = cfg.clone();
    wrong.scope.locator_generation = 2;
    assert!(Journal::recover(f.store(), wrong, None).is_err());
    let mut wrong = cfg.clone();
    wrong.stream_epoch = [19; 16];
    assert!(Journal::recover(f.store(), wrong, None).is_err());
    let mut reopened = Journal::recover(f.store(), cfg, Some(end.frontier())).unwrap();
    assert_eq!(reopened.effective_record(), Some(&version(1)));
    assert!(reopened.record(&prepare(2, Some(version(1)))).is_err());
}

#[test]
fn recovery_rejects_old_writer_and_wrong_scope_before_truncating_torn_tail() {
    let _fork_barrier = FORK_BARRIER.read().unwrap_or_else(|e| e.into_inner());
    use std::io::Write;
    let f = Fixture::new();
    let mut cfg = config();
    cfg.writer_epoch = 3;
    let mut j = Journal::create(f.store(), cfg.clone()).unwrap();
    let receipt = j.record(&prepare(1, None)).unwrap();
    drop(j);
    let path = f.store().segment_path(1);
    let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&tickwire_journal::frame::MAGIC[..3])
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    let original = fs::read(&path).unwrap();
    let mut wrong = cfg.clone();
    wrong.writer_epoch = 2;
    assert!(Journal::recover(f.store(), wrong, None).is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
    let mut wrong = cfg.clone();
    wrong.scope.slot = [44; 16];
    assert!(Journal::recover(f.store(), wrong, None).is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
    cfg.writer_epoch = 4;
    let mut recovered = Journal::recover(f.store(), cfg, Some(receipt.frontier())).unwrap();
    assert_eq!(recovered.record(&prepare(1, None)).unwrap(), receipt);
    let end = complete(&mut recovered, 1, None);
    assert_eq!(end.frontier().exec_epoch, 4);
}

#[test]
fn higher_writer_in_validated_unfinished_batch_cannot_be_erased_by_old_writer() {
    let _fork_barrier = FORK_BARRIER.read().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::new();
    let mut cfg = config();
    cfg.writer_epoch = 3;
    let mut j = Journal::create(f.store(), cfg.clone()).unwrap();
    let p = j.record(&prepare(1, None)).unwrap();
    drop(j);
    let (mut physical, _) =
        tickwire_journal::Journal::recover(f.store(), Some(p.frontier())).unwrap();
    physical.append_batch(4, &[&[42; 64]]).unwrap();
    drop(physical);
    let path = f.store().segment_path(1);
    // Retain the valid next BatchBegin and DataRecord header, but not its body.
    let torn_end = p.frontier().end_offset
        + 2 * tickwire_journal::frame::HEADER_BYTES as u64
        + tickwire_journal::frame::TRAILER_BYTES as u64
        + 2;
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(torn_end)
        .unwrap();
    let original = fs::read(&path).unwrap();
    assert!(Journal::recover(f.store(), cfg.clone(), Some(p.frontier())).is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
    cfg.writer_epoch = 4;
    let mut recovered = Journal::recover(f.store(), cfg, Some(p.frontier())).unwrap();
    assert_eq!(recovered.record(&prepare(1, None)).unwrap(), p);
    assert_eq!(complete(&mut recovered, 1, None).frontier().exec_epoch, 4);
}

#[test]
fn invalid_application_records_preserve_all_bytes_even_with_valid_physical_seals() {
    let _fork_barrier = FORK_BARRIER.read().unwrap_or_else(|e| e.into_inner());
    use std::io::Write;
    for case in 0..6 {
        let f = Fixture::new();
        let mut j = Journal::create(f.store(), config()).unwrap();
        j.record(&prepare(1, None)).unwrap();
        drop(j);
        let (mut physical, recovered) =
            tickwire_journal::Journal::recover(f.store(), None).unwrap();
        let mut payload = recovered.records[1].payload.clone();
        match case {
            0 => {} // A duplicate exact step must not appear twice on disk.
            1 => payload.insert(0, b' '),
            2..=4 => {
                let mut value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
                match case {
                    2 => value["schema"] = 2.into(),
                    3 => value["extra"] = true.into(),
                    _ => value["scope_hash"][0] = 88.into(),
                }
                payload = serde_json::to_vec(&value).unwrap();
            }
            _ => payload = recovered.records[0].payload.clone(),
        }
        physical.append_batch(1, &[&payload]).unwrap();
        drop(physical);
        let path = f.store().segment_path(1);
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&tickwire_journal::frame::MAGIC[..2])
            .unwrap();
        file.sync_all().unwrap();
        drop(file);
        let original = fs::read(&path).unwrap();
        assert!(
            Journal::recover(f.store(), config(), None).is_err(),
            "case {case}"
        );
        assert_eq!(fs::read(path).unwrap(), original, "case {case}");
    }
}

#[test]
fn sync_error_returns_no_step_and_poisoned_writer_recovers_actual_complete_record() {
    let _fork_barrier = FORK_BARRIER.read().unwrap_or_else(|e| e.into_inner());
    use std::{
        io,
        sync::{Arc, atomic::AtomicBool},
    };
    use tickwire_journal::store::Store;
    struct FailSync {
        store: FileStore,
        fail: Arc<AtomicBool>,
    }
    impl Store for FailSync {
        fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
            self.store.read_manifest()
        }
        fn publish_manifest(&mut self, b: &[u8]) -> io::Result<()> {
            self.store.publish_manifest(b)
        }
        fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
            self.store.segment_ids()
        }
        fn read_segment(&mut self, id: u64, max: u64) -> io::Result<Vec<u8>> {
            self.store.read_segment(id, max)
        }
        fn create_segment(&mut self, id: u64) -> io::Result<()> {
            self.store.create_segment(id)
        }
        fn append_segment(&mut self, id: u64, off: u64, b: &[u8]) -> io::Result<()> {
            self.store.append_segment(id, off, b)
        }
        fn truncate_segment(&mut self, id: u64, n: u64) -> io::Result<()> {
            self.store.truncate_segment(id, n)
        }
        fn sync_segment(&mut self, id: u64) -> io::Result<()> {
            self.store.sync_segment(id)?;
            if self.fail.load(Ordering::SeqCst) {
                Err(io::Error::other("lost sync completion"))
            } else {
                Ok(())
            }
        }
    }
    let f = Fixture::new();
    let fail = Arc::new(AtomicBool::new(false));
    let mut j = Journal::create(
        FailSync {
            store: f.store(),
            fail: fail.clone(),
        },
        config(),
    )
    .unwrap();
    let before = j.durable_frontier().unwrap().clone();
    fail.store(true, Ordering::SeqCst);
    assert!(j.record(&prepare(1, None)).is_err());
    assert_eq!(j.phase([1; 16]), None);
    assert!(j.operation([1; 16]).is_none());
    assert_eq!(j.operations().count(), 0);
    assert_eq!(j.active_operation(), None);
    assert_eq!(j.durable_frontier(), Some(&before));
    assert!(matches!(j.record(&prepare(1, None)), Err(Error::Poisoned)));
    drop(j);
    let mut recovered = Journal::recover(f.store(), config(), Some(&before)).unwrap();
    assert_eq!(recovered.phase([1; 16]), Some(Phase::Prepared));
    assert_eq!(recovered.record(&prepare(1, None)).unwrap().sequence(), 2);
}

#[test]
fn byte_capacity_rejects_before_write_but_retains_exact_retry() {
    let _fork_barrier = FORK_BARRIER.read().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::new();
    let mut j = Journal::create(f.store(), config()).unwrap();
    let p = j.record(&prepare(1, None)).unwrap();
    drop(j);
    let (physical, recovery) = tickwire_journal::Journal::recover(f.store(), None).unwrap();
    let bytes = recovery.records.iter().map(|r| r.payload.len()).sum();
    drop(physical);
    let mut cfg = config();
    cfg.max_recorded_bytes = bytes;
    let mut j = Journal::recover(f.store(), cfg, Some(p.frontier())).unwrap();
    assert_eq!(j.record(&prepare(1, None)).unwrap(), p);
    assert!(matches!(
        j.record(&Event::Desired {
            operation: [1; 16],
            prepared_ref: p.record_hash(),
            file: file(11)
        }),
        Err(Error::Capacity)
    ));
    assert_eq!(j.durable_frontier(), Some(p.frontier()));
}

#[test]
fn invalid_scope_and_prepared_values_cannot_create_or_advance_a_journal() {
    let _fork_barrier = FORK_BARRIER.read().unwrap_or_else(|e| e.into_inner());
    for cluster in ["", "1cluster", "cluster_name", "Cluster-a", &"a".repeat(64)] {
        let f = Fixture::new();
        let mut cfg = config();
        cfg.scope.cluster = cluster.into();
        assert!(Journal::create(f.store(), cfg).is_err());
        assert!(!f.0.join("MANIFEST.json").exists());
    }
    let f = Fixture::new();
    let mut j = Journal::create(f.store(), config()).unwrap();
    let before = j.durable_frontier().unwrap().clone();
    for case in 0..8 {
        let Event::Prepared(mut p) = prepare(1, None) else {
            unreachable!()
        };
        match case {
            0 => p.operation = [0; 16],
            1 => p.next.global_hash = [0; 32],
            2 => p.next.revision = 1 << 63,
            3 => p.next.policy_revision = 0,
            4 => p.control_commit_ref = [0; 32],
            5 => p.expected_instance = [0; 16],
            6 => p.exec_generation = 0,
            _ => p.projection_object_bytes = 0,
        }
        assert!(j.record(&Event::Prepared(p)).is_err(), "case {case}");
        assert_eq!(j.durable_frontier(), Some(&before));
        assert_eq!(j.phase([1; 16]), None);
    }
}

#[cfg(unix)]
#[test]
fn sigkill_after_each_synchronized_step_recovers_and_resumes_exact_operation() {
    let _fork_barrier = FORK_BARRIER.write().unwrap_or_else(|e| e.into_inner());
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    let phases = [
        Phase::Prepared,
        Phase::Desired,
        Phase::RuntimeObserved,
        Phase::Effective,
    ];
    for (i, phase) in phases.into_iter().enumerate() {
        let f = Fixture::new();
        let ready = f.0.with_extension("ready");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_child_entry", "--ignored", "--nocapture"])
            .env("TW_LOCAL_JOURNAL_CRASH_ROOT", &f.0)
            .env("TW_LOCAL_JOURNAL_CRASH_READY", &ready)
            .env("TW_LOCAL_JOURNAL_CRASH_STEP", (i + 1).to_string())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        while !ready.exists() {
            if Instant::now() >= deadline || child.try_wait().unwrap().is_some() {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child did not reach synchronized step {phase:?}");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success());
        let mut j = Journal::recover(f.store(), config(), None).unwrap();
        assert_eq!(j.phase([1; 16]), Some(phase));
        assert_eq!(j.durable_frontier().unwrap().last_seq, i as u64 + 2);
        let end = complete(&mut j, 1, None);
        assert_eq!(end.sequence(), 5);
        drop(j);
        assert_eq!(
            Journal::recover(f.store(), config(), Some(end.frontier()))
                .unwrap()
                .effective_record(),
            Some(&version(1))
        );
        fs::remove_file(ready).unwrap();
    }
}

#[cfg(unix)]
#[test]
#[ignore = "run by SIGKILL parent with private environment"]
fn crash_child_entry() {
    let _fork_barrier = FORK_BARRIER.read().unwrap_or_else(|e| e.into_inner());
    use std::io::Write;
    let root = std::env::var_os("TW_LOCAL_JOURNAL_CRASH_ROOT").unwrap();
    let ready = std::env::var_os("TW_LOCAL_JOURNAL_CRASH_READY").unwrap();
    let step: usize = std::env::var("TW_LOCAL_JOURNAL_CRASH_STEP")
        .unwrap()
        .parse()
        .unwrap();
    let barrier = |n| {
        if n == step {
            let mut file = fs::File::create(&ready).unwrap();
            file.write_all(b"synchronized").unwrap();
            file.sync_all().unwrap();
            loop {
                std::thread::park();
            }
        }
    };
    let mut j = Journal::create(FileStore::open(root).unwrap(), config()).unwrap();
    let p = j.record(&prepare(1, None)).unwrap();
    barrier(1);
    let d = j
        .record(&Event::Desired {
            operation: [1; 16],
            prepared_ref: p.record_hash(),
            file: file(11),
        })
        .unwrap();
    barrier(2);
    let r = j
        .record(&Event::RuntimeObserved {
            operation: [1; 16],
            desired_ref: d.record_hash(),
            fact: runtime(),
        })
        .unwrap();
    barrier(3);
    j.record(&Event::Effective {
        operation: [1; 16],
        runtime_ref: r.record_hash(),
        file: file(12),
    })
    .unwrap();
    barrier(4);
}

#[cfg(target_os = "linux")]
#[test]
fn directory_fd_backend_records_actual_file_receipts_and_reopens() {
    let _fork_barrier = FORK_BARRIER.read().unwrap_or_else(|e| e.into_inner());
    use std::{
        fs::File,
        os::unix::fs::{MetadataExt, PermissionsExt},
    };
    use tickwire_config_fs::{DurableFile, JournalStore, Store};
    fn fact(file: &DurableFile) -> FileFact {
        FileFact {
            render_hash: file.hash(),
            bytes: file.bytes() as u64,
            device: file.device(),
            inode: file.inode(),
        }
    }
    let f = Fixture::new();
    fs::create_dir(&f.0).unwrap();
    fs::set_permissions(&f.0, fs::Permissions::from_mode(0o700)).unwrap();
    for name in ["journal", "files"] {
        fs::create_dir(f.0.join(name)).unwrap();
        fs::set_permissions(f.0.join(name), fs::Permissions::from_mode(0o700)).unwrap();
    }
    let owner = fs::metadata(&f.0).unwrap().uid();
    let store = || {
        JournalStore::new(
            Store::open_at(File::open(&f.0).unwrap(), "journal", owner).unwrap(),
            Default::default(),
        )
        .unwrap()
    };
    let mut files = Store::open_at(File::open(&f.0).unwrap(), "files", owner).unwrap();
    let mut j = Journal::create(store(), config()).unwrap();
    let p = j.record(&prepare(1, None)).unwrap();
    let desired = files
        .install("desired.json", b"{\"revision\":1}", None)
        .unwrap();
    let d_event = Event::Desired {
        operation: [1; 16],
        prepared_ref: p.record_hash(),
        file: fact(&desired),
    };
    let d = j.record(&d_event).unwrap();
    let r = j
        .record(&Event::RuntimeObserved {
            operation: [1; 16],
            desired_ref: d.record_hash(),
            fact: runtime(),
        })
        .unwrap();
    let effective = files
        .install("effective.json", b"{\"revision\":1,\"applied\":100}", None)
        .unwrap();
    let e_event = Event::Effective {
        operation: [1; 16],
        runtime_ref: r.record_hash(),
        file: fact(&effective),
    };
    let e = j.record(&e_event).unwrap();
    drop(j);
    let mut j = Journal::recover(store(), config(), Some(e.frontier())).unwrap();
    assert_eq!(j.record(&d_event).unwrap(), d);
    assert_eq!(j.record(&e_event).unwrap(), e);
    assert_eq!(j.effective_record(), Some(&version(1)));
    assert_eq!(
        files.read("desired.json", 1024).unwrap(),
        b"{\"revision\":1}"
    );
    assert_eq!(
        files.read("effective.json", 1024).unwrap(),
        b"{\"revision\":1,\"applied\":100}"
    );
    assert_eq!(complete(&mut j, 2, Some(version(1))).sequence(), 9);
}
