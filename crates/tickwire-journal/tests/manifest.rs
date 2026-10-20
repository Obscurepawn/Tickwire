use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tickwire_journal::{
    store::{FileStore, Store},
    *,
};
struct FaultStore {
    inner: FileStore,
    path: PathBuf,
    manifest_calls: usize,
    fail: bool,
    after_rename: bool,
    syncs: Arc<AtomicUsize>,
}
impl Store for FaultStore {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.inner.read_manifest()
    }
    fn publish_manifest(&mut self, b: &[u8]) -> io::Result<()> {
        self.manifest_calls += 1;
        if self.fail && self.manifest_calls == 2 {
            if self.after_rename {
                let tmp = self.path.join("MANIFEST.tmp");
                let mut f = fs::File::create(&tmp)?;
                f.write_all(b)?;
                f.sync_all()?;
                fs::rename(tmp, self.path.join("MANIFEST.json"))?;
                // Inject the directory fsync error after the real rename.
            }
            return Err(io::Error::other("injected manifest publication failure"));
        }
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
        self.inner.append_segment(id, offset, b)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        self.syncs.fetch_add(1, Ordering::Relaxed);
        self.inner.sync_segment(id)
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.inner.truncate_segment(id, len)
    }
}
#[test]
fn manifest_failure_before_publish_or_after_rename_poisons_then_recovers() {
    for after_rename in [false, true] {
        let path = std::env::temp_dir().join(format!(
            "tickwire-manifest-{}-{after_rename}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        let store = FaultStore {
            inner: FileStore::open(&path).unwrap(),
            path: path.clone(),
            manifest_calls: 0,
            fail: true,
            after_rename,
            syncs: Arc::new(AtomicUsize::new(0)),
        };
        let mut c = Config::new([1; 16], [2; 16]);
        c.segment_max_bytes = 1024;
        let mut j = Journal::create(store, c).unwrap();
        let prior = j.append_batch(1, &[b"first"]).unwrap();
        assert!(j.append_batch(2, &[b"second"]).is_err());
        assert!(matches!(
            j.append_batch(2, &[b"cannot retry"]),
            Err(Error::Poisoned)
        ));
        drop(j);
        let (mut j, r) =
            Journal::recover(FileStore::open(&path).unwrap(), Some(prior.frontier())).unwrap();
        assert_eq!(r.records.len(), 1);
        let second = j.append_batch(2, &[b"second"]).unwrap();
        assert_eq!(second.frontier().last_seq, 2);
        assert_eq!(second.frontier().segment_id, 2);
        drop(j);
        fs::remove_dir_all(path).unwrap();
    }
}
#[test]
fn one_data_sync_per_closed_batch_and_no_manifest_per_batch() {
    let path = std::env::temp_dir().join(format!("tickwire-sync-count-{}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    let syncs = Arc::new(AtomicUsize::new(0));
    let store = FaultStore {
        inner: FileStore::open(&path).unwrap(),
        path: path.clone(),
        manifest_calls: 0,
        fail: false,
        after_rename: false,
        syncs: syncs.clone(),
    };
    let mut j = Journal::create(store, Config::new([1; 16], [2; 16])).unwrap();
    for n in 1..=3 {
        j.append_batch(1, &[b"one", b"two"]).unwrap();
        assert_eq!(syncs.load(Ordering::Relaxed), n);
    }
    let store = j.into_store();
    assert_eq!(store.manifest_calls, 1);
    drop(store);
    fs::remove_dir_all(path).unwrap();
}
