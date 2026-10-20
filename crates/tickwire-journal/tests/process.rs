#![cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use std::{
    fs, io,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tickwire_journal::{
    store::{FileStore, Store},
    *,
};

struct PausingStore {
    inner: FileStore,
    armed: Arc<AtomicBool>,
    mode: String,
    ready: PathBuf,
}
impl PausingStore {
    fn pause(&self) {
        fs::write(&self.ready, b"reached").unwrap();
        loop {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}
impl Store for PausingStore {
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
        if self.armed.load(Ordering::Relaxed) && self.mode == "partial_write" {
            self.inner.append_segment(id, offset, &b[..b.len() / 2])?;
            self.pause();
        }
        self.inner.append_segment(id, offset, b)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        if self.armed.load(Ordering::Relaxed) && self.mode == "before_sync" {
            self.pause();
        }
        self.inner.sync_segment(id)?;
        if self.armed.load(Ordering::Relaxed) && self.mode == "after_sync" {
            self.pause();
        }
        Ok(())
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.inner.truncate_segment(id, len)
    }
}
#[test]
#[ignore = "child entry point; parent runs it in a separate process"]
fn crash_child() {
    let dir = PathBuf::from(std::env::var_os("TICKWIRE_JOURNAL_CHILD_DIR").unwrap());
    let mode = std::env::var("TICKWIRE_JOURNAL_CHILD_MODE").unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let store = PausingStore {
        inner: FileStore::open(&dir).unwrap(),
        armed: armed.clone(),
        mode: mode.clone(),
        ready: dir.join("ready"),
    };
    let (mut journal, _) = Journal::recover(store, None).unwrap();
    armed.store(true, Ordering::Relaxed);
    let receipt = journal
        .append_batch(7, &[b"second intent", b"second reservation"])
        .unwrap();
    assert_eq!(receipt.frontier().last_seq, 3);
    journal.into_store().pause();
}
#[test]
fn sigkill_at_write_sync_and_receipt_boundaries_preserves_complete_prefix() {
    for mode in [
        "partial_write",
        "before_sync",
        "after_sync",
        "after_receipt",
    ] {
        let dir =
            std::env::temp_dir().join(format!("tickwire-wal-kill-{}-{mode}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut journal = Journal::create(
            FileStore::open(&dir).unwrap(),
            Config::new([1; 16], [2; 16]),
        )
        .unwrap();
        let prior = journal.append_batch(7, &[b"first committed"]).unwrap();
        drop(journal);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "crash_child", "--nocapture"])
            .env("TICKWIRE_JOURNAL_CHILD_DIR", &dir)
            .env("TICKWIRE_JOURNAL_CHILD_MODE", mode)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !dir.join("ready").exists() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("child exited before {mode}: {status}");
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child did not reach {mode}");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        assert_eq!(child.wait().unwrap().signal(), Some(9));
        let (journal, recovered) =
            Journal::recover(FileStore::open(&dir).unwrap(), Some(prior.frontier())).unwrap();
        let expected = if mode == "partial_write" { 1 } else { 3 };
        assert_eq!(recovered.records.len(), expected, "kill boundary {mode}");
        assert_eq!(recovered.records[0].payload, b"first committed");
        println!(
            "SIGKILL boundary={mode} recovered_records={expected} discarded_bytes={}",
            recovered.discarded_tail_bytes
        );
        drop(journal);
        fs::remove_dir_all(&dir).unwrap();
    }
}
