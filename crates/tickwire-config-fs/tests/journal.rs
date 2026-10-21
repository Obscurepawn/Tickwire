#![cfg(target_os = "linux")]
use std::{
    fs::{self, File, OpenOptions, Permissions},
    io::Write,
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
use tickwire_config_fs::{JournalLimits, JournalStore, Store};
use tickwire_journal::{Config, Journal, store::Store as _};
static NEXT: AtomicUsize = AtomicUsize::new(1);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "tickwire-config-journal-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&p).unwrap();
        fs::set_permissions(&p, Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(p.join("journal")).unwrap();
        fs::set_permissions(p.join("journal"), Permissions::from_mode(0o700)).unwrap();
        Self(p)
    }
    fn store(&self, limits: JournalLimits) -> JournalStore {
        JournalStore::new(
            Store::open_at(
                File::open(&self.0).unwrap(),
                "journal",
                fs::metadata(&self.0).unwrap().uid(),
            )
            .unwrap(),
            limits,
        )
        .unwrap()
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join("journal").join(name)
    }
    fn segment(&self, id: u64) -> PathBuf {
        self.path(&format!("segment-{id:020}.wal"))
    }
    fn file(&self, name: &str, bytes: &[u8]) {
        let p = self.path(name);
        fs::write(&p, bytes).unwrap();
        fs::set_permissions(p, Permissions::from_mode(0o600)).unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn config() -> Config {
    Config {
        segment_max_bytes: 1024,
        ..Config::new([1; 16], [2; 16])
    }
}
#[test]
fn real_physical_journal_rotates_and_recovers_required_durable_prefix() {
    let f = Fixture::new();
    let mut journal = Journal::create(f.store(Default::default()), config()).unwrap();
    for i in 1..=4 {
        let r = journal.append_batch(1, &[&[i; 64]]).unwrap();
        assert_eq!(r.frontier().last_seq, u64::from(i));
    }
    let frontier = journal.durable_frontier().unwrap().clone();
    assert!(frontier.segment_id > 1);
    drop(journal);
    let (mut journal, recovered) =
        Journal::recover(f.store(Default::default()), Some(&frontier)).unwrap();
    assert_eq!(recovered.records.len(), 4);
    for (i, record) in recovered.records.iter().enumerate() {
        assert_eq!(record.payload, vec![i as u8 + 1; 64]);
        assert_eq!(record.seq, i as u64 + 1);
    }
    assert_eq!(
        journal
            .append_batch(2, &[b"next"])
            .unwrap()
            .frontier()
            .last_seq,
        5
    );
    for id in 1..=frontier.segment_id {
        let m = fs::metadata(f.segment(id)).unwrap();
        assert_eq!(m.mode() & 0o7777, 0o600);
        assert_eq!(m.nlink(), 1);
    }
}
#[test]
fn physical_recovery_truncates_only_unfinished_tail_and_keeps_required_seal() {
    let f = Fixture::new();
    let mut j = Journal::create(f.store(Default::default()), config()).unwrap();
    let frontier = j
        .append_batch(1, &[b"committed"])
        .unwrap()
        .frontier()
        .clone();
    drop(j);
    let mut file = OpenOptions::new().append(true).open(f.segment(1)).unwrap();
    file.write_all(&tickwire_journal::frame::MAGIC[..4])
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    let (mut j, recovery) = Journal::recover(f.store(Default::default()), Some(&frontier)).unwrap();
    assert_eq!(recovery.discarded_tail_bytes, 4);
    assert_eq!(recovery.records[0].payload, b"committed");
    assert_eq!(
        fs::metadata(f.segment(1)).unwrap().len(),
        frontier.end_offset
    );
    assert_eq!(
        j.append_batch(1, &[b"after"]).unwrap().frontier().last_seq,
        2
    );
}
#[test]
fn backend_read_limit_bounds_a_larger_physical_segment_configuration() {
    let f = Fixture::new();
    let mut j =
        Journal::create(f.store(Default::default()), Config::new([1; 16], [2; 16])).unwrap();
    let frontier = j.append_batch(1, &[b"small"]).unwrap().frontier().clone();
    drop(j);
    let (_, recovery) = Journal::recover(f.store(Default::default()), Some(&frontier)).unwrap();
    assert_eq!(recovery.records.len(), 1);
    assert_eq!(recovery.records[0].payload, b"small");
}
#[test]
fn multisegment_recovery_syncs_prior_segments_after_truncating_the_last() {
    let f = Fixture::new();
    let mut j = Journal::create(f.store(Default::default()), config()).unwrap();
    for i in 0..4 {
        j.append_batch(1, &[&[i; 64]]).unwrap();
    }
    let frontier = j.durable_frontier().unwrap().clone();
    assert!(frontier.segment_id > 1);
    drop(j);
    let mut file = OpenOptions::new()
        .append(true)
        .open(f.segment(frontier.segment_id))
        .unwrap();
    file.write_all(&tickwire_journal::frame::MAGIC[..4])
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    let (mut j, recovered) =
        Journal::recover(f.store(Default::default()), Some(&frontier)).unwrap();
    assert_eq!(recovered.discarded_tail_bytes, 4);
    assert_eq!(recovered.records.len(), 4);
    assert_eq!(
        j.append_batch(2, &[b"after"]).unwrap().frontier().last_seq,
        5
    );
}
#[test]
fn changed_segment_inode_cannot_be_acknowledged_by_sync() {
    let f = Fixture::new();
    let mut s = f.store(Default::default());
    s.create_segment(1).unwrap();
    s.append_segment(1, 0, b"pending").unwrap();
    fs::rename(f.segment(1), f.path("saved")).unwrap();
    f.file("segment-00000000000000000001.wal", b"pending");
    assert!(s.sync_segment(1).is_err());
    assert!(s.append_segment(1, 7, b"again").is_err());
    assert_eq!(fs::read(f.path("saved")).unwrap(), b"pending");
}
#[test]
fn recovered_read_bytes_remain_bound_to_the_inode_synchronized_later() {
    let f = Fixture::new();
    let mut s = f.store(Default::default());
    s.create_segment(1).unwrap();
    s.append_segment(1, 0, b"one").unwrap();
    s.sync_segment(1).unwrap();
    drop(s);
    let mut s = f.store(Default::default());
    assert_eq!(s.read_segment(1, 1024).unwrap(), b"one");
    fs::rename(f.segment(1), f.0.join("previous")).unwrap();
    f.file("segment-00000000000000000001.wal", b"two");
    assert!(s.sync_segment(1).is_err());
    assert!(s.append_segment(1, 3, b"more").is_err());
}
#[test]
fn malformed_names_links_permissions_and_segment_gaps_are_rejected() {
    for n in 0..6 {
        let f = Fixture::new();
        let mut j = Journal::create(f.store(Default::default()), config()).unwrap();
        let required = j.append_batch(1, &[b"record"]).unwrap().frontier().clone();
        drop(j);
        match n {
            0 => f.file("segment-bad.wal", b"bad"),
            1 => {
                fs::rename(f.segment(1), f.path("original")).unwrap();
                symlink(f.path("original"), f.segment(1)).unwrap();
            }
            2 => fs::hard_link(f.segment(1), f.0.join("alias")).unwrap(),
            3 => fs::set_permissions(f.segment(1), Permissions::from_mode(0o644)).unwrap(),
            4 => fs::rename(f.segment(1), f.segment(3)).unwrap(),
            _ => f.file("unexpected", b"bad"),
        }
        let s = f.store(Default::default());
        assert!(Journal::recover(s, Some(&required)).is_err(), "case {n}");
    }
}
#[test]
fn manifest_update_compares_the_previously_read_bytes() {
    let f = Fixture::new();
    let mut s = f.store(Default::default());
    assert_eq!(s.read_manifest().unwrap(), None);
    s.publish_manifest(b"one").unwrap();
    assert_eq!(s.read_manifest().unwrap().unwrap(), b"one");
    f.file("MANIFEST.json", b"alien");
    assert!(s.publish_manifest(b"two").is_err());
    assert_eq!(fs::read(f.path("MANIFEST.json")).unwrap(), b"alien");
}
#[test]
fn manifest_external_replacement_with_requested_bytes_is_still_drift() {
    let f = Fixture::new();
    let mut s = f.store(Default::default());
    assert_eq!(s.read_manifest().unwrap(), None);
    s.publish_manifest(b"one").unwrap();
    assert_eq!(s.read_manifest().unwrap().unwrap(), b"one");
    f.file("MANIFEST.json", b"two");
    assert!(s.publish_manifest(b"two").is_err());
    assert_eq!(fs::read(f.path("MANIFEST.json")).unwrap(), b"two");
}
#[test]
fn strict_segment_count_and_byte_budgets_preserve_existing_bytes() {
    let f = Fixture::new();
    let mut s = f.store(JournalLimits {
        max_segment_bytes: 1024,
        max_segments: 1,
        max_total_bytes: 1024,
    });
    s.create_segment(1).unwrap();
    s.append_segment(1, 0, &[9; 1024]).unwrap();
    s.sync_segment(1).unwrap();
    assert!(s.create_segment(2).is_err());
    assert!(!f.segment(2).exists());
    assert!(s.append_segment(1, 1024, &[1]).is_err());
    assert_eq!(fs::read(f.segment(1)).unwrap(), vec![9; 1024]);
}
#[test]
fn root_replacement_blocks_segment_publication() {
    let f = Fixture::new();
    let mut s = f.store(Default::default());
    fs::rename(f.path(""), f.0.join("old")).unwrap();
    fs::create_dir(f.path("")).unwrap();
    fs::set_permissions(f.path(""), Permissions::from_mode(0o700)).unwrap();
    assert!(s.create_segment(1).is_err());
    assert!(!f.0.join("old/segment-00000000000000000001.wal").exists());
    assert!(!f.segment(1).exists());
}
