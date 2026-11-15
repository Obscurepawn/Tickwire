//! Verify stateless in-memory JOIN idempotency, bounded FIFO eviction, startup CPU isolation
//! validation, and `md-cold` escaping a single-core parent thread affinity mask (`taskset`).
#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

use std::{fs, net::TcpListener, os::unix::fs::PermissionsExt, path::Path};
use tickwire_control::{
    Completion, Error as ControlError, RequestKey, RequestStore, StoredRequest,
};
use tickwire_control_sys::{CpuSet, PrivateDirectory, current_thread_affinity, pin_current_thread};
use tickwire_market_source::protocol::wire_bytes;
use tickwire_md::{BoundedRequestStore, config, engine::Runtime};

fn make_runtime_config(dir: &Path) -> config::Config {
    let mut cfg: config::Config =
        serde_json::from_str(include_str!("../examples/md.json")).unwrap();
    cfg.identity.arch = if cfg!(target_arch = "aarch64") { 2 } else { 1 };
    cfg.socket = dir.join("market.sock");
    cfg.admission_directory = dir.join("admissions");
    fs::create_dir(&cfg.admission_directory).unwrap();
    fs::set_permissions(&cfg.admission_directory, fs::Permissions::from_mode(0o700)).unwrap();

    let cert = rcgen::generate_simple_self_signed(vec![
        "stream.binance.com".into(),
        "api.binance.com".into(),
    ])
    .unwrap();
    let root_der = dir.join("root.der");
    fs::write(&root_der, cert.cert.der()).unwrap();

    // Bind inert loopback listeners so Runtime::new initializes feeds cleanly without external I/O.
    let ws = TcpListener::bind("127.0.0.1:0").unwrap();
    let rest = TcpListener::bind("127.0.0.1:0").unwrap();
    let ws_addr = ws.local_addr().unwrap().to_string();
    let rest_addr = rest.local_addr().unwrap().to_string();
    std::mem::forget((ws, rest));

    cfg.feeds.truncate(1);
    cfg.feeds[0].stream_address = ws_addr;
    cfg.feeds[0].snapshot_address = rest_addr;
    cfg.feeds[0].trust_anchors = vec![root_der];
    cfg.feeds[0].snapshot_limit = 5;
    cfg.feeds[0].topics = vec![101];

    cfg.topics.truncate(1);
    cfg.topics[0].resource = dir.join("topic-101.bin");
    cfg.topics[0].maximum_levels = 8;
    cfg.topics[0].maximum_cache_bytes = 1 << 20;

    let mut spec: config::ResourceSpec =
        serde_json::from_str(include_str!("../examples/topic-101.json")).unwrap();
    spec.identity = cfg.identity.clone();
    spec.capacity = 64;
    spec.arena_classes = vec![(65536, 32)];
    config::write_bytes(
        &cfg.topics[0].resource,
        &wire_bytes(&spec.header_with_exact_events().unwrap()).unwrap(),
    )
    .unwrap();
    cfg
}

fn parse_proc_cpus_allowed_list(tid: u32) -> CpuSet {
    let status_path = format!("/proc/self/task/{tid}/status");
    let status = fs::read_to_string(&status_path)
        .unwrap_or_else(|e| panic!("failed reading {status_path}: {e}"));
    let line = status
        .lines()
        .find(|line| line.starts_with("Cpus_allowed_list:"))
        .unwrap_or_else(|| panic!("missing Cpus_allowed_list in {status_path}:\n{status}"));
    let list = line.strip_prefix("Cpus_allowed_list:").unwrap().trim();
    list.parse::<CpuSet>()
        .unwrap_or_else(|e| panic!("invalid Cpus_allowed_list `{list}`: {e}"))
}

#[test]
fn in_memory_join_idempotency_and_bounded_fifo_eviction() {
    assert!(BoundedRequestStore::new(0).is_err());

    let mut store = BoundedRequestStore::new(2).unwrap();
    assert_eq!(store.capacity(), 2);
    assert!(store.is_empty());

    let key = |id: u8| RequestKey {
        subject: [1; 16],
        epoch: 1,
        request_id: [id; 16],
    };

    // 1. Register key(1) as Pending and verify duplicate returns false/Pending while conflicting digest rejects.
    assert!(store.insert_received(key(1), [0xAA; 32]).unwrap());
    assert!(!store.insert_received(key(1), [0xAA; 32]).unwrap());
    assert_eq!(
        store.lookup(key(1)).unwrap(),
        Some(StoredRequest {
            digest: [0xAA; 32],
            completion: None,
        })
    );
    assert_eq!(
        store.insert_received(key(1), [0xBB; 32]),
        Err(ControlError::Conflict)
    );
    assert_eq!(store.pending_count(), 1);

    // 2. Complete key(2) and key(3) while key(1) is still Pending: Pending entries must NEVER be evicted,
    //    so key(2) (the oldest completed entry) is deterministically evicted to stay within capacity=2.
    assert!(store.insert_received(key(2), [0x22; 32]).unwrap());
    store
        .complete(
            key(2),
            [0x22; 32],
            Completion {
                response: vec![2, 0, 10],
                state_version: 10,
            },
        )
        .unwrap();
    assert_eq!(store.len(), 2);
    assert_eq!(store.completed_count(), 1);

    assert!(store.insert_received(key(3), [0x33; 32]).unwrap());
    store
        .complete(
            key(3),
            [0x33; 32],
            Completion {
                response: vec![3, 0, 11],
                state_version: 11,
            },
        )
        .unwrap();
    assert_eq!(store.len(), 2);
    assert_eq!(store.pending_count(), 1);
    assert_eq!(store.completed_count(), 1);

    // key(1) is still Pending; key(3) is Completed; key(2) was evicted in FIFO order.
    assert_eq!(
        store.lookup(key(1)).unwrap(),
        Some(StoredRequest {
            digest: [0xAA; 32],
            completion: None,
        })
    );
    assert_eq!(store.lookup(key(2)).unwrap(), None);
    assert_eq!(
        store.lookup(key(3)).unwrap(),
        Some(StoredRequest {
            digest: [0x33; 32],
            completion: Some(Completion {
                response: vec![3, 0, 11],
                state_version: 11,
            }),
        })
    );
    assert_eq!(
        store.insert_received(key(3), [0xFF; 32]),
        Err(ControlError::Conflict)
    );

    // Now complete key(1) and insert+complete key(4): key(3) is older in completion FIFO than key(1),
    // so key(3) is evicted first while key(1) and key(4) remain cached.
    store
        .complete(
            key(1),
            [0xAA; 32],
            Completion {
                response: vec![1, 0, 12],
                state_version: 12,
            },
        )
        .unwrap();
    assert!(store.insert_received(key(4), [0x44; 32]).unwrap());
    store
        .complete(
            key(4),
            [0x44; 32],
            Completion {
                response: vec![4, 0, 13],
                state_version: 13,
            },
        )
        .unwrap();
    assert_eq!(store.len(), 2);
    assert!(store.lookup(key(1)).unwrap().unwrap().completion.is_some());
    assert!(store.lookup(key(4)).unwrap().unwrap().completion.is_some());
    // key(3) was deterministically evicted and is accepted as New again.
    assert_eq!(store.lookup(key(3)).unwrap(), None);
    assert!(store.insert_received(key(3), [0x33; 32]).unwrap());
}

#[test]
fn overlapping_or_invalid_reactor_and_housekeeping_cpus_fail_fast_at_startup() {
    let available = current_thread_affinity().unwrap();
    let cpu = available.as_slice()[0];

    let temp = PrivateDirectory::create_in(Path::new("/tmp"), "tw-md-cpu-overlap").unwrap();
    let mut cfg = make_runtime_config(temp.path());
    cfg.reactor_cpu = Some(cpu);
    cfg.housekeeping_cpus = Some(CpuSet::single(cpu).unwrap());

    let err = match Runtime::new(cfg) {
        Ok(_) => panic!("overlapping reactor_cpu and housekeeping_cpus must fail fast"),
        Err(err) => err,
    };
    assert!(
        matches!(
            err.downcast_ref::<tickwire_control_sys::Error>(),
            Some(tickwire_control_sys::Error::ResourceConflict)
        ),
        "unexpected startup error: {err:?}"
    );
    assert!(!temp.path().join("market.sock").exists());
    assert!(!temp.path().join("topic-101.bin.active").exists());

    let temp_oob = PrivateDirectory::create_in(Path::new("/tmp"), "tw-md-cpu-oob").unwrap();
    let mut cfg_oob = make_runtime_config(temp_oob.path());
    cfg_oob.reactor_cpu = Some(4096);
    let err_oob = match Runtime::new(cfg_oob) {
        Ok(_) => panic!("out-of-range reactor_cpu must fail fast"),
        Err(err) => err,
    };
    assert!(
        matches!(
            err_oob.downcast_ref::<tickwire_control_sys::Error>(),
            Some(tickwire_control_sys::Error::InvalidResources)
        ),
        "unexpected OOB startup error: {err_oob:?}"
    );
    assert!(!temp_oob.path().join("market.sock").exists());
}

#[test]
fn md_cold_escapes_single_core_parent_taskset_mask_onto_housekeeping_cpus() {
    std::thread::spawn(|| {
        let available = current_thread_affinity().unwrap();
        assert!(!available.is_empty());
        let parent_cpu = available.as_slice()[0];
        let housekeeping_cpu = if available.len() >= 2 {
            available.as_slice()[1]
        } else {
            parent_cpu
        };
        let parent_mask = CpuSet::single(parent_cpu).unwrap();
        let housekeeping_mask = CpuSet::single(housekeeping_cpu).unwrap();

        // Simulate launching under an external single-core `taskset -c <parent_cpu>` mask.
        pin_current_thread(&parent_mask).unwrap();
        assert_eq!(current_thread_affinity().unwrap(), parent_mask);

        let temp = PrivateDirectory::create_in(Path::new("/tmp"), "tw-md-taskset").unwrap();
        let mut cfg = make_runtime_config(temp.path());
        cfg.reactor_cpu = None;
        cfg.housekeeping_cpus = Some(housekeeping_mask.clone());

        let runtime = Runtime::new(cfg).unwrap();
        let cold_tid = runtime.cold_tid();
        assert!(cold_tid > 0);

        let cold_allowed = parse_proc_cpus_allowed_list(cold_tid);
        assert_eq!(cold_allowed, housekeeping_mask);
        if available.len() >= 2 {
            assert_ne!(
                cold_allowed, parent_mask,
                "md-cold must escape the parent thread's single-core taskset mask"
            );
        }

        // Verify zero WAL / journal / quota directories were created anywhere in the runtime dir.
        for forbidden in ["control", "journal", "quota", "recovery"] {
            assert!(!temp.path().join(forbidden).exists());
        }
    })
    .join()
    .unwrap();
}
