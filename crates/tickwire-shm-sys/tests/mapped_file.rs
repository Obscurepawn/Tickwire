#![cfg(all(
    target_os = "linux",
    target_endian = "little",
    target_has_atomic = "64",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use tickwire_shm_sys::{CursorState, MappedCursorPage, MappedSegment, PAGE_BYTES};

fn temp_dir(tag: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "tickwire-shm-mapped-{tag}-{}-{n}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn mapped_segment_writes_reads_syncs_and_truncates_on_clean_close() {
    let dir = temp_dir("basic");
    let seg_path = dir.join("segment-00000000000000000001.wal");
    let mut seg = MappedSegment::open_or_create(&seg_path, 4 * PAGE_BYTES).unwrap();
    assert_eq!(seg.capacity(), 4 * PAGE_BYTES);

    let payload = b"TWAL0001-hello-mapped-segment";
    seg.write_at(0, payload).unwrap();
    seg.write_at(payload.len(), b"-tail").unwrap();
    let logical_len = payload.len() + 5;
    assert_eq!(
        seg.read_to_vec(logical_len).unwrap(),
        b"TWAL0001-hello-mapped-segment-tail"
    );

    // Out-of-bounds write or read is rejected cleanly without UB.
    assert!(seg.write_at(4 * PAGE_BYTES - 2, b"abcd").is_err());
    assert!(seg.read_to_vec(4 * PAGE_BYTES + 1).is_err());

    // Duplicated fd allows background fdatasync while mapped.
    let sync_fd = seg.duplicate_sync_fd().unwrap();
    let sync_file = fs::File::from(sync_fd);
    sync_file.sync_data().unwrap();

    // Exclusive flock blocks a second concurrent writer on the same segment.
    assert!(MappedSegment::open_or_create(&seg_path, 4 * PAGE_BYTES).is_err());

    seg.truncate_unmap(logical_len as u64).unwrap();
    let disk_bytes = fs::read(&seg_path).unwrap();
    assert_eq!(disk_bytes, b"TWAL0001-hello-mapped-segment-tail");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn cursor_page_double_buffers_generations_and_detects_slot_corruption() {
    let dir = temp_dir("cursor");
    let path = dir.join("MMAP-CURSOR.bin");
    {
        let mut cursor = MappedCursorPage::open_or_create(&path).unwrap();
        assert_eq!(cursor.latest().unwrap(), None);

        cursor.record(1, 352).unwrap();
        assert_eq!(
            cursor.latest().unwrap(),
            Some(CursorState {
                generation: 1,
                segment_id: 1,
                logical_len: 352,
            })
        );

        cursor.record(1, 704).unwrap();
        assert_eq!(
            cursor.latest().unwrap(),
            Some(CursorState {
                generation: 2,
                segment_id: 1,
                logical_len: 704,
            })
        );
    }

    // Corrupt slot 0 (generation 2 resides in slot 2 % 2 == 0) to simulate torn write on crash;
    // reopening must fall back to intact slot 1 (generation 1, logical_len 352).
    let mut raw = fs::read(&path).unwrap();
    assert_eq!(raw.len(), PAGE_BYTES);
    raw[24] ^= 0xff;
    fs::write(&path, &raw).unwrap();

    let reopened = MappedCursorPage::open_or_create(&path).unwrap();
    assert_eq!(
        reopened.latest().unwrap(),
        Some(CursorState {
            generation: 1,
            segment_id: 1,
            logical_len: 352,
        })
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn survives_abrupt_sigkill_via_kernel_page_cache_without_fsync() {
    const ENV_CHILD_DIR: &str = "TICKWIRE_SHM_MAPPED_SIGKILL_DIR";
    if let Ok(child_dir) = std::env::var(ENV_CHILD_DIR) {
        let dir = PathBuf::from(child_dir);
        let mut seg = MappedSegment::open_or_create(&dir.join("seg.wal"), 8 * PAGE_BYTES).unwrap();
        let mut cursor = MappedCursorPage::open_or_create(&dir.join("MMAP-CURSOR.bin")).unwrap();
        let msg = b"survives-sigkill-in-linux-page-cache-without-blocking-fsync!";
        seg.write_at(0, msg).unwrap();
        cursor.record(1, msg.len() as u64).unwrap();
        // Terminate immediately via SIGKILL without running Drop, munmap, or fsync!
        unsafe {
            libc::kill(libc::getpid(), libc::SIGKILL);
        }
        unreachable!();
    }

    let dir = temp_dir("sigkill");
    let exe = std::env::current_exe().unwrap();
    let status = Command::new(exe)
        .arg("--exact")
        .arg("survives_abrupt_sigkill_via_kernel_page_cache_without_fsync")
        .arg("--nocapture")
        .env(ENV_CHILD_DIR, &dir)
        .status()
        .unwrap();
    assert!(!status.success(), "child must die by SIGKILL");

    let cursor = MappedCursorPage::open_or_create(&dir.join("MMAP-CURSOR.bin")).unwrap();
    let state = cursor
        .latest()
        .unwrap()
        .expect("cursor state survived SIGKILL");
    assert_eq!(state.segment_id, 1);
    let expected = b"survives-sigkill-in-linux-page-cache-without-blocking-fsync!";
    assert_eq!(state.logical_len, expected.len() as u64);

    let seg = MappedSegment::open_or_create(&dir.join("seg.wal"), 8 * PAGE_BYTES).unwrap();
    assert_eq!(
        seg.read_to_vec(state.logical_len as usize).unwrap(),
        expected
    );
    seg.truncate_unmap(state.logical_len).unwrap();
    assert_eq!(fs::read(dir.join("seg.wal")).unwrap(), expected);
    let _ = fs::remove_dir_all(&dir);
}
