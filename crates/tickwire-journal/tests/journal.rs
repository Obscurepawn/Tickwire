use std::{
    fs, io,
    sync::atomic::{AtomicU64, Ordering},
};
use tickwire_journal::{
    store::{FileStore, Store},
    *,
};
static NEXT: AtomicU64 = AtomicU64::new(1);
struct Dir(std::path::PathBuf);
impl Dir {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "tickwire-wal-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
    fn store(&self) -> FileStore {
        FileStore::open(&self.0).unwrap()
    }
    fn segment(&self, id: u64) -> std::path::PathBuf {
        self.0.join(format!("segment-{id:020}.wal"))
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn config() -> Config {
    Config::new([1; 16], [2; 16])
}
#[test]
fn synchronized_batches_reopen_and_continue_sequences() {
    let d = Dir::new();
    let mut j = Journal::create(d.store(), config()).unwrap();
    let a = j.append_batch(3, &[b"intent", b"reservation"]).unwrap();
    let b = j.append_batch(4, &[b"fact"]).unwrap();
    assert_eq!(a.frontier().last_seq, 2);
    assert_eq!(b.frontier().last_seq, 3);
    drop(j);
    let (mut j, recovered) = Journal::recover(d.store(), Some(b.frontier())).unwrap();
    assert_eq!(
        recovered
            .records
            .iter()
            .map(|r| r.payload.clone())
            .collect::<Vec<_>>(),
        vec![
            b"intent".to_vec(),
            b"reservation".to_vec(),
            b"fact".to_vec()
        ]
    );
    assert_eq!(recovered.discarded_tail_bytes, 0);
    assert_eq!(
        j.append_batch(4, &[b"next"]).unwrap().frontier().last_seq,
        4
    );
}
#[test]
fn every_unfinished_final_batch_prefix_recovers_only_previous_batch() {
    let d = Dir::new();
    let mut j = Journal::create(d.store(), config()).unwrap();
    let first = j.append_batch(1, &[b"prior"]).unwrap();
    let start = first.frontier().end_offset as usize;
    j.append_batch(1, &[b"tail-one", b"tail-two"]).unwrap();
    drop(j);
    let all = fs::read(d.segment(1)).unwrap();
    for n in start..all.len() {
        fs::write(d.segment(1), &all[..n]).unwrap();
        let (j, r) = Journal::recover(d.store(), Some(first.frontier())).unwrap();
        assert_eq!(r.records.len(), 1, "cut {n}");
        assert_eq!(r.discarded_tail_bytes, (n - start) as u64);
        drop(j);
        assert_eq!(fs::metadata(d.segment(1)).unwrap().len(), start as u64);
    }
}
#[test]
fn committed_corruption_and_required_missing_tail_quarantine_without_mutation() {
    let d = Dir::new();
    let mut j = Journal::create(d.store(), config()).unwrap();
    let receipt = j.append_batch(1, &[b"committed"]).unwrap();
    drop(j);
    let all = fs::read(d.segment(1)).unwrap();
    for offset in [0, 128, 176 + 136, all.len() - 20, all.len() - 1] {
        let mut bad = all.clone();
        bad[offset] ^= 1;
        fs::write(d.segment(1), &bad).unwrap();
        assert!(Journal::recover(d.store(), Some(receipt.frontier())).is_err());
        assert_eq!(fs::read(d.segment(1)).unwrap(), bad);
    }
    fs::write(d.segment(1), &all[..176]).unwrap();
    assert!(Journal::recover(d.store(), Some(receipt.frontier())).is_err());
    assert_eq!(fs::read(d.segment(1)).unwrap().len(), 176);
}
#[test]
fn rotation_preserves_chain_and_epoch_floor_and_missing_segment_fails() {
    let d = Dir::new();
    let mut c = config();
    c.segment_max_bytes = 1024;
    let mut j = Journal::create(d.store(), c).unwrap();
    let first = j.append_batch(9, &[b"one"]).unwrap();
    let second = j.append_batch(10, &[b"two"]).unwrap();
    assert_eq!(first.frontier().segment_id, 1);
    assert_eq!(second.frontier().segment_id, 2);
    assert!(j.append_batch(9, &[b"old epoch"]).is_err());
    drop(j);
    let (mut j, r) = Journal::recover(d.store(), Some(second.frontier())).unwrap();
    assert_eq!(r.records.len(), 2);
    assert!(j.append_batch(9, &[b"old epoch"]).is_err());
    drop(j);
    fs::remove_file(d.segment(1)).unwrap();
    assert!(Journal::recover(d.store(), None).is_err());
}
#[test]
fn oversized_batch_does_not_split_or_modify_files() {
    let d = Dir::new();
    let mut c = config();
    c.segment_max_bytes = 600;
    let mut j = Journal::create(d.store(), c).unwrap();
    let before = fs::read(d.segment(1)).unwrap();
    assert!(j.append_batch(1, &[&[0; 100]]).is_err());
    assert_eq!(fs::read(d.segment(1)).unwrap(), before);
    assert!(j.append_batch(1, &[]).is_err());
    assert!(j.append_batch(0, &[b"x"]).is_err());
    j.append_batch(1, &[b"valid"]).unwrap();
}
#[test]
fn manifest_damage_and_concurrent_writer_fail_closed() {
    let d = Dir::new();
    let j = Journal::create(d.store(), config()).unwrap();
    assert!(FileStore::open(&d.0).is_err());
    drop(j);
    let path = d.0.join("MANIFEST.json");
    let mut bytes = fs::read(&path).unwrap();
    bytes[10] ^= 1;
    fs::write(&path, &bytes).unwrap();
    assert!(Journal::recover(d.store(), None).is_err());
    assert_eq!(fs::read(&path).unwrap(), bytes);
}
struct FailStore {
    inner: FileStore,
    write_fail: bool,
    sync_fail: bool,
}
impl Store for FailStore {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.inner.read_manifest()
    }
    fn publish_manifest(&mut self, b: &[u8]) -> io::Result<()> {
        self.inner.publish_manifest(b)
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        self.inner.segment_ids()
    }
    fn read_segment(&mut self, id: u64, max: u64) -> io::Result<Vec<u8>> {
        self.inner.read_segment(id, max)
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        self.inner.create_segment(id)
    }
    fn append_segment(&mut self, id: u64, offset: u64, b: &[u8]) -> io::Result<()> {
        if self.write_fail {
            self.inner.append_segment(id, offset, &b[..b.len() / 2])?;
            return Err(io::Error::other("injected partial write"));
        }
        self.inner.append_segment(id, offset, b)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        if self.sync_fail {
            return Err(io::Error::other("injected sync failure"));
        }
        self.inner.sync_segment(id)
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.inner.truncate_segment(id, len)
    }
}
#[test]
fn short_write_and_failed_sync_never_return_receipt_and_poison_writer() {
    for sync_fail in [false, true] {
        let d = Dir::new();
        let store = FailStore {
            inner: d.store(),
            write_fail: !sync_fail,
            sync_fail,
        };
        let mut j = Journal::create(store, config()).unwrap();
        assert!(j.append_batch(1, &[b"pending"]).is_err());
        assert!(matches!(
            j.append_batch(1, &[b"must not follow"]),
            Err(Error::Poisoned)
        ));
        drop(j);
        let (_, r) = Journal::recover(d.store(), None).unwrap();
        // Complete seals after a failed sync may survive; recovery retains them.
        assert_eq!(r.records.len(), usize::from(sync_fail));
    }
}

fn rewrite_second_batch(
    bytes: &[u8],
    start: usize,
    change: impl Fn(&mut frame::Header),
) -> Vec<u8> {
    let mut result = bytes[..start].to_vec();
    let mut offset = start;
    let mut previous: Option<[u8; 32]> = None;
    while offset < bytes.len() {
        let decoded = frame::decode(&bytes[offset..]).unwrap();
        let mut header = decoded.header.clone();
        change(&mut header);
        if let Some(hash) = previous {
            header.previous_hash = hash;
        }
        let encoded = frame::encode(&header, decoded.payload).unwrap();
        previous = Some(frame::decode(&encoded).unwrap().hash);
        result.extend(encoded);
        offset += decoded.total_len;
    }
    result
}
#[test]
fn correctly_checksummed_epoch_rollback_sequence_gap_and_identity_change_quarantine() {
    let d = Dir::new();
    let mut j = Journal::create(d.store(), config()).unwrap();
    let first = j.append_batch(10, &[b"one"]).unwrap();
    j.append_batch(11, &[b"two"]).unwrap();
    drop(j);
    let all = fs::read(d.segment(1)).unwrap();
    for which in 0..4 {
        let bad = rewrite_second_batch(
            &all,
            first.frontier().end_offset as usize,
            |h| match which {
                0 => h.exec_epoch = 9,
                1 => h.record_seq += 1,
                2 => h.stream_epoch = [9; 16],
                _ => h.batch_number += 1,
            },
        );
        fs::write(d.segment(1), &bad).unwrap();
        assert!(matches!(
            Journal::recover(d.store(), None),
            Err(Error::Quarantined(_))
        ));
        assert_eq!(fs::read(d.segment(1)).unwrap(), bad);
    }
}
#[test]
fn unsealed_middle_and_sealed_tail_corruption_are_not_discarded() {
    let d = Dir::new();
    let mut j = Journal::create(d.store(), config()).unwrap();
    let first = j.append_batch(1, &[b"one"]).unwrap();
    j.append_batch(1, &[b"two"]).unwrap();
    drop(j);
    let all = fs::read(d.segment(1)).unwrap();
    let seal = first.frontier().end_offset as usize - 176;
    let mut bad = all[..seal].to_vec();
    bad.extend_from_slice(&all[seal + 176..]);
    fs::write(d.segment(1), &bad).unwrap();
    assert!(Journal::recover(d.store(), None).is_err());
    assert_eq!(fs::read(d.segment(1)).unwrap(), bad);
    let mut bad = all.clone();
    let n = bad.len();
    bad[n - 40] ^= 1;
    fs::write(d.segment(1), &bad).unwrap();
    assert!(Journal::recover(d.store(), None).is_err());
    assert_eq!(fs::read(d.segment(1)).unwrap(), bad);
}
#[test]
fn orphan_empty_rotation_segment_is_adopted_but_missing_declared_segment_quarantines() {
    let d = Dir::new();
    let mut c = config();
    c.segment_max_bytes = 1024;
    let mut j = Journal::create(d.store(), c).unwrap();
    let receipt = j.append_batch(1, &[b"one"]).unwrap();
    let mut store = j.into_store();
    store.create_segment(2).unwrap();
    drop(store);
    let (mut j, r) = Journal::recover(d.store(), Some(receipt.frontier())).unwrap();
    assert_eq!(r.records.len(), 1);
    let next = j.append_batch(2, &[b"two"]).unwrap();
    assert_eq!(next.frontier().segment_id, 2);
    drop(j);
    fs::remove_file(d.segment(2)).unwrap();
    assert!(Journal::recover(d.store(), Some(receipt.frontier())).is_err());
}
#[test]
fn frontier_ahead_or_different_hash_never_repairs_files() {
    let d = Dir::new();
    let mut j = Journal::create(d.store(), config()).unwrap();
    let r = j.append_batch(1, &[b"one"]).unwrap();
    drop(j);
    let before = fs::read(d.segment(1)).unwrap();
    let mut required = r.frontier().clone();
    required.last_seq += 1;
    assert!(Journal::recover(d.store(), Some(&required)).is_err());
    required = r.frontier().clone();
    required.seal_hash[0] ^= 1;
    assert!(Journal::recover(d.store(), Some(&required)).is_err());
    assert_eq!(fs::read(d.segment(1)).unwrap(), before);
}

#[test]
#[cfg(unix)]
fn newly_created_directory_is_owner_only() {
    use std::os::unix::fs::PermissionsExt;
    let d = Dir::new();
    let _store = d.store();
    assert_eq!(fs::metadata(&d.0).unwrap().permissions().mode() & 0o077, 0);
}
