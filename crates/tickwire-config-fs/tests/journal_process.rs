#![cfg(target_os = "linux")]
use std::{
    fs::{self, File, Permissions},
    io,
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        process::ExitStatusExt,
    },
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tickwire_config_fs::{JournalStore, Store};
use tickwire_journal::{Config, Journal, store::Store as Backend};

fn store(root: &std::path::Path) -> JournalStore {
    JournalStore::new(
        Store::open_at(
            File::open(root).unwrap(),
            "journal",
            fs::metadata(root).unwrap().uid(),
        )
        .unwrap(),
        Default::default(),
    )
    .unwrap()
}
struct Pausing {
    inner: JournalStore,
    root: PathBuf,
    phase: String,
    armed: Arc<AtomicBool>,
}
impl Pausing {
    fn pause(&self) {
        fs::write(self.root.join("ready"), self.phase.as_bytes()).unwrap();
        loop {
            std::thread::park();
        }
    }
}
impl Backend for Pausing {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.inner.read_manifest()
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.inner.publish_manifest(bytes)
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
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        if self.armed.load(Ordering::Relaxed) && self.phase == "partial" {
            self.inner
                .append_segment(id, offset, &bytes[..bytes.len() / 2])?;
            self.pause();
        }
        self.inner.append_segment(id, offset, bytes)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        if self.armed.load(Ordering::Relaxed) && self.phase == "before_sync" {
            self.pause();
        }
        self.inner.sync_segment(id)?;
        if self.armed.load(Ordering::Relaxed) && self.phase == "after_sync" {
            self.pause();
        }
        Ok(())
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.inner.truncate_segment(id, len)
    }
}
#[test]
#[ignore = "subprocess entry; the parent explicitly runs all four phases"]
fn config_journal_crash_child() {
    let root = PathBuf::from(std::env::var_os("TICKWIRE_CONFIG_JOURNAL_CHILD").unwrap());
    let phase = std::env::var("TICKWIRE_CONFIG_JOURNAL_PHASE").unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let backend = Pausing {
        inner: store(&root),
        root,
        phase,
        armed: armed.clone(),
    };
    let (mut journal, _) = Journal::recover(backend, None).unwrap();
    armed.store(true, Ordering::Relaxed);
    assert_eq!(
        journal
            .append_batch(1, &[b"second", b"third"])
            .unwrap()
            .frontier()
            .last_seq,
        3
    );
    journal.into_store().pause();
}
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
#[test]
fn sigkill_before_and_after_actual_config_journal_sync_preserves_complete_batches() {
    for phase in ["partial", "before_sync", "after_sync", "after_receipt"] {
        let root = Directory(std::env::temp_dir().join(format!(
            "tickwire-config-journal-kill-{}-{phase}",
            std::process::id()
        )));
        fs::create_dir(&root.0).unwrap();
        fs::set_permissions(&root.0, Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(root.0.join("journal")).unwrap();
        fs::set_permissions(root.0.join("journal"), Permissions::from_mode(0o700)).unwrap();
        let mut journal = Journal::create(
            store(&root.0),
            Config {
                segment_max_bytes: 1024,
                ..Config::new([1; 16], [2; 16])
            },
        )
        .unwrap();
        let prior = journal
            .append_batch(1, &[b"first"])
            .unwrap()
            .frontier()
            .clone();
        drop(journal);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "config_journal_crash_child",
                "--nocapture",
            ])
            .env("TICKWIRE_CONFIG_JOURNAL_CHILD", &root.0)
            .env("TICKWIRE_CONFIG_JOURNAL_PHASE", phase)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !root.0.join("ready").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before {phase}"
            );
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child did not reach {phase}");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        assert_eq!(child.wait().unwrap().signal(), Some(9));
        let (mut journal, recovery) = Journal::recover(store(&root.0), Some(&prior)).unwrap();
        assert_eq!(recovery.records[0].payload, b"first");
        match phase {
            "partial" => assert_eq!(recovery.records.len(), 1),
            "after_sync" | "after_receipt" => assert_eq!(recovery.records.len(), 3),
            _ => assert!(matches!(recovery.records.len(), 1 | 3)),
        }
        if recovery.records.len() == 3 {
            assert_eq!(recovery.records[1].payload, b"second");
            assert_eq!(recovery.records[2].payload, b"third");
        }
        let next = recovery.records.len() as u64 + 1;
        assert_eq!(
            journal
                .append_batch(2, &[b"resumed"])
                .unwrap()
                .frontier()
                .last_seq,
            next
        );
    }
}
