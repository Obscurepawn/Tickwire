use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tickwire_journal::store::{
    FileStore, InMemoryStore, JournalStore, MmapAsyncStore, StorageMode,
};
use tickwire_journal::{Config, Journal};

fn temp_dir(tag: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "tickwire-journal-modes-{tag}-{}-{n}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.parent().unwrap()).unwrap();
    dir
}

#[test]
fn storage_mode_defaults_to_in_memory_and_supports_segment_rotation() {
    assert_eq!(StorageMode::default(), StorageMode::InMemory);

    let cfg = Config {
        stream_epoch: [0x11; 16],
        domain: [0xaa; 16],
        segment_max_bytes: 1024,
    };
    let mut journal = Journal::create(InMemoryStore::new(), cfg).unwrap();
    let r1 = journal.append_batch(1, &[b"alpha".as_slice()]).unwrap();
    let r2 = journal
        .append_batch(1, &[b"beta-payload-triggering-roll".as_slice(); 3])
        .unwrap();
    assert_eq!(r1.frontier().last_seq, 1);
    assert_eq!(r2.frontier().last_seq, 4);
    assert!(r2.frontier().segment_id >= 2);
}

#[test]
fn mmap_async_store_rotates_recovers_and_leaves_file_store_compatible_segments() {
    let dir = temp_dir("mmap-rotate");
    let cfg = Config {
        stream_epoch: [0x22; 16],
        domain: [0xbb; 16],
        segment_max_bytes: 1024,
    };
    let last_frontier = {
        let store = MmapAsyncStore::open(&dir).unwrap();
        let mut journal = Journal::create(store, cfg).unwrap();
        for i in 0..12u64 {
            let payload = format!("order-record-{i:04}-payload-bytes");
            journal.append_batch(3, &[payload.as_bytes()]).unwrap();
        }
        journal.store().flush_durable().unwrap();
        journal.durable_frontier().cloned().unwrap()
    };

    // After clean Drop, standard FileStore can open and verify every segment without MMAP-CURSOR.bin!
    let (_reopened, recovery) =
        Journal::recover(FileStore::open(&dir).unwrap(), Some(&last_frontier)).unwrap();
    assert_eq!(recovery.records.len(), 12);
    assert_eq!(recovery.discarded_tail_bytes, 0);
    let _ = fs::remove_dir_all(&dir);
}

#[cfg(target_os = "linux")]
#[test]
fn mmap_async_store_recovers_100_percent_of_committed_batches_after_sigkill() {
    const ENV_SIGKILL_DIR: &str = "TICKWIRE_MMAP_JOURNAL_SIGKILL_DIR";
    if let Ok(raw_dir) = std::env::var(ENV_SIGKILL_DIR) {
        let dir = PathBuf::from(raw_dir);
        let cfg = Config {
            stream_epoch: [0x33; 16],
            domain: [0xcc; 16],
            segment_max_bytes: 64 * 1024,
        };
        let store = MmapAsyncStore::open(&dir).unwrap();
        let mut journal = Journal::create(store, cfg).unwrap();
        for seq in 1..=25u64 {
            let payload = format!("sigkill-batch-{seq:03}");
            journal.append_batch(5, &[payload.as_bytes()]).unwrap();
        }
        // Abrupt process death via SIGKILL without flush_durable() or Drop!
        std::process::abort();
    }

    let dir = temp_dir("sigkill");
    let exe = std::env::current_exe().unwrap();
    let status = Command::new(exe)
        .arg("--exact")
        .arg("mmap_async_store_recovers_100_percent_of_committed_batches_after_sigkill")
        .arg("--nocapture")
        .env(ENV_SIGKILL_DIR, &dir)
        .status()
        .unwrap();
    assert!(!status.success(), "child process must abort abruptly");

    let store = MmapAsyncStore::open(&dir).unwrap();
    let (mut recovered, recovery) = Journal::recover(store, None).unwrap();
    assert_eq!(recovery.records.len(), 25);
    assert_eq!(recovery.discarded_tail_bytes, 0);
    assert_eq!(
        recovery.records.last().unwrap().payload,
        b"sigkill-batch-025"
    );

    // Can continue appending under a newer writer epoch after crash recovery.
    let next = recovered
        .append_batch(6, &[b"post-recovery-026".as_slice()])
        .unwrap();
    assert_eq!(next.frontier().last_seq, 26);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn benchmark_commit_profiles_in_memory_vs_mmap_async_vs_local_durable() {
    let iterations = 200usize;
    let payload = [0x5au8; 256];
    let cfg = Config {
        stream_epoch: [0x44; 16],
        domain: [0xdd; 16],
        segment_max_bytes: 16 * 1024 * 1024,
    };

    // 1. InMemory
    let mut mem_journal = Journal::create(
        JournalStore::open(StorageMode::InMemory, None).unwrap(),
        cfg.clone(),
    )
    .unwrap();
    let mut mem_samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t0 = Instant::now();
        mem_journal.append_batch(1, &[&payload]).unwrap();
        mem_samples.push(t0.elapsed().as_nanos() as u64);
    }

    // 2. MmapAsync (warm 16 batches first so pre-faulted steady-state is measured)
    let mmap_dir = temp_dir("bench-mmap");
    let mut mmap_journal = Journal::create(
        JournalStore::open(StorageMode::MmapAsync, Some(&mmap_dir)).unwrap(),
        cfg.clone(),
    )
    .unwrap();
    for _ in 0..16 {
        mmap_journal.append_batch(1, &[&payload]).unwrap();
    }
    let mut mmap_samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t0 = Instant::now();
        mmap_journal.append_batch(1, &[&payload]).unwrap();
        mmap_samples.push(t0.elapsed().as_nanos() as u64);
    }
    drop(mmap_journal);

    // 2b. Pure MmapAsyncStore commit step (append_segment + sync_segment without outer SHA-256 framing)
    let raw_mmap_dir = temp_dir("bench-raw-mmap");
    let mut raw_mmap = MmapAsyncStore::open(&raw_mmap_dir).unwrap();
    tickwire_journal::store::Store::create_segment(&mut raw_mmap, 1).unwrap();
    let mut offset = 0u64;
    for _ in 0..16 {
        tickwire_journal::store::Store::append_segment(&mut raw_mmap, 1, offset, &payload).unwrap();
        tickwire_journal::store::Store::sync_segment(&mut raw_mmap, 1).unwrap();
        offset += payload.len() as u64;
    }
    let mut raw_mmap_samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t0 = Instant::now();
        tickwire_journal::store::Store::append_segment(&mut raw_mmap, 1, offset, &payload).unwrap();
        tickwire_journal::store::Store::sync_segment(&mut raw_mmap, 1).unwrap();
        raw_mmap_samples.push(t0.elapsed().as_nanos() as u64);
        offset += payload.len() as u64;
    }
    drop(raw_mmap);
    let _ = fs::remove_dir_all(&raw_mmap_dir);

    // 3. LocalDurable (synchronous fsync)
    let sync_iters = 30usize;
    let sync_dir = temp_dir("bench-sync");
    let mut sync_journal = Journal::create(
        JournalStore::open(StorageMode::LocalDurable, Some(&sync_dir)).unwrap(),
        cfg,
    )
    .unwrap();
    let mut sync_samples = Vec::with_capacity(sync_iters);
    for _ in 0..sync_iters {
        let t0 = Instant::now();
        sync_journal.append_batch(1, &[&payload]).unwrap();
        sync_samples.push(t0.elapsed().as_nanos() as u64);
    }
    drop(sync_journal);

    mem_samples.sort_unstable();
    mmap_samples.sort_unstable();
    raw_mmap_samples.sort_unstable();
    sync_samples.sort_unstable();
    let p50 = |v: &[u64]| v[v.len() / 2];
    let p99 = |v: &[u64]| v[(v.len() * 99) / 100];
    eprintln!(
        "commit_bench_ns: raw_mmap_store(p50={}, p99={}) framed_in_memory(p50={}, p99={}) framed_mmap_async(p50={}, p99={}) framed_local_durable(p50={}, p99={})",
        p50(&raw_mmap_samples),
        p99(&raw_mmap_samples),
        p50(&mem_samples),
        p99(&mem_samples),
        p50(&mmap_samples),
        p99(&mmap_samples),
        p50(&sync_samples),
        p99(&sync_samples),
    );
    let _ = fs::remove_dir_all(&mmap_dir);
    let _ = fs::remove_dir_all(&sync_dir);
}
