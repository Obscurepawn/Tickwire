#![cfg(target_os = "linux")]
mod common;
use common::*;
use std::{
    fs::{self, File, Permissions},
    io::Write,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::PathBuf,
    process::{Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};
use tickwire_config_fs::{JournalLimits, JournalStore, Store};
use tickwire_config_journal::{Event, FileFact, Journal, Phase};
use tickwire_config_materializer::{Error, Files, Local, Result};
static NEXT: AtomicUsize = AtomicUsize::new(1);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "tickwire-materializer-linux-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, Permissions::from_mode(0o700)).unwrap();
        for name in ["files", "journal"] {
            fs::create_dir(path.join(name)).unwrap();
            fs::set_permissions(path.join(name), Permissions::from_mode(0o700)).unwrap();
        }
        Self(path)
    }
    fn files(&self) -> Store {
        Store::open_at(
            File::open(&self.0).unwrap(),
            "files",
            fs::metadata(&self.0).unwrap().uid(),
        )
        .unwrap()
    }
    fn journal(&self, recover: bool) -> Journal<JournalStore> {
        let store = Store::open_at(
            File::open(&self.0).unwrap(),
            "journal",
            fs::metadata(&self.0).unwrap().uid(),
        )
        .unwrap();
        let store = JournalStore::new(store, JournalLimits::default()).unwrap();
        if recover {
            Journal::recover(store, config(), None).unwrap()
        } else {
            Journal::create(store, config()).unwrap()
        }
    }
    fn open(&self, recover: bool) -> Local<Store, JournalStore> {
        Local::open(self.files(), self.journal(recover)).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn finish<F: Files>(local: &mut Local<F, JournalStore>, n: u8) {
    local.write_desired([n; 16]).unwrap();
    local.observe_runtime([n; 16], runtime()).unwrap();
    local.write_effective([n; 16]).unwrap();
}
#[test]
fn actual_descriptor_files_and_wal_preserve_two_generations_and_receipt_inode() {
    let f = Fixture::new();
    let mut local = f.open(false);
    let a = object(1);
    let p = prepared(&a, None);
    local.prepare(p.clone(), &a).unwrap();
    finish(&mut local, 1);
    let b = object(2);
    local.prepare(prepared(&b, Some(p.next)), &b).unwrap();
    finish(&mut local, 2);
    let op = local.journal().operation([2; 16]).unwrap();
    let Some(Event::Effective { file, .. }) = op.event(Phase::Effective) else {
        panic!("missing effective");
    };
    let path = f.0.join("files/effective.toml");
    let metadata = fs::metadata(&path).unwrap();
    assert_eq!(file.inode, metadata.ino());
    assert_eq!(file.device, metadata.dev());
    assert_eq!(file.bytes, metadata.len());
    assert_eq!(file.render_hash, hash(&fs::read(&path).unwrap()));
    assert_eq!(metadata.mode() & 0o7777, 0o600);
    assert_eq!(
        fs::read_dir(f.0.join("files/last-good")).unwrap().count(),
        2
    );
    assert_eq!(
        fs::metadata(f.0.join("files/last-good")).unwrap().mode() & 0o7777,
        0o700
    );
    drop(local);
    let mut local = f.open(true);
    let before = fs::read(&path).unwrap();
    local.write_effective([1; 16]).unwrap();
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(fs::metadata(&path).unwrap().ino(), metadata.ino());
}
struct LoseReturn {
    store: Store,
    path: &'static str,
}
impl Files for LoseReturn {
    fn create_directory(&mut self, path: &str) -> Result<()> {
        Files::create_directory(&mut self.store, path)
    }
    fn read(&mut self, path: &str, limit: usize) -> Result<Option<Vec<u8>>> {
        Files::read(&mut self.store, path, limit)
    }
    fn install(
        &mut self,
        path: &str,
        bytes: &[u8],
        previous: Option<[u8; 32]>,
    ) -> Result<FileFact> {
        let receipt = Files::install(&mut self.store, path, bytes, previous)?;
        if path == self.path {
            return Err(Error::File(tickwire_config_fs::Error::Io(
                std::io::Error::other("lost actual durable file return"),
            )));
        }
        Ok(receipt)
    }
}
#[test]
fn actual_synced_files_with_lost_returns_are_adopted_without_replacing_the_inode() {
    for path in ["desired.toml", "effective.toml"] {
        let f = Fixture::new();
        let mut local = Local::open(
            LoseReturn {
                store: f.files(),
                path,
            },
            f.journal(false),
        )
        .unwrap();
        let a = object(1);
        local.prepare(prepared(&a, None), &a).unwrap();
        if path == "effective.toml" {
            local.write_desired([1; 16]).unwrap();
            local.observe_runtime([1; 16], runtime()).unwrap();
            assert!(local.write_effective([1; 16]).is_err());
        } else {
            assert!(local.write_desired([1; 16]).is_err());
        }
        assert!(local.is_poisoned());
        let filename = f.0.join("files").join(path);
        let before = fs::metadata(&filename).unwrap().ino();
        let bytes = fs::read(&filename).unwrap();
        drop(local);
        let mut local = f.open(true);
        finish(&mut local, 1);
        assert_eq!(fs::metadata(&filename).unwrap().ino(), before);
        assert_eq!(fs::read(&filename).unwrap(), bytes);
    }
}
#[test]
fn actual_descriptor_journal_sync_return_loss_recovers_the_completed_effective() {
    let f = Fixture::new();
    let fail = std::rc::Rc::new(std::cell::Cell::new(false));
    let store = Store::open_at(
        File::open(&f.0).unwrap(),
        "journal",
        fs::metadata(&f.0).unwrap().uid(),
    )
    .unwrap();
    let journal = Journal::create(
        LoseSync {
            store: JournalStore::new(store, JournalLimits::default()).unwrap(),
            fail: fail.clone(),
        },
        config(),
    )
    .unwrap();
    let mut local = Local::open(f.files(), journal).unwrap();
    let a = object(1);
    local.prepare(prepared(&a, None), &a).unwrap();
    local.write_desired([1; 16]).unwrap();
    local.observe_runtime([1; 16], runtime()).unwrap();
    fail.set(true);
    assert!(local.write_effective([1; 16]).is_err());
    assert_eq!(local.journal().phase([1; 16]), Some(Phase::RuntimeObserved));
    assert!(local.is_poisoned());
    let inode = fs::metadata(f.0.join("files/effective.toml"))
        .unwrap()
        .ino();
    drop(local);
    let mut local = f.open(true);
    assert_eq!(local.journal().phase([1; 16]), Some(Phase::Effective));
    assert_eq!(local.write_effective([1; 16]).unwrap().sequence(), 5);
    assert_eq!(
        fs::metadata(f.0.join("files/effective.toml"))
            .unwrap()
            .ino(),
        inode
    );
}
#[test]
fn sigkill_each_completed_stage_recovers_from_the_actual_files_and_wal() {
    for stage in 1..=4 {
        let f = Fixture::new();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "crash_child_entry", "--nocapture"])
            .env("TW_MATERIALIZER_CRASH_ROOT", &f.0)
            .env("TW_MATERIALIZER_CRASH_STAGE", stage.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        while !f.0.join("ready").exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child stopped before stage {stage}"
            );
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("stage {stage} was not reached");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success());
        let mut local = f.open(true);
        let a = object(1);
        local.prepare(prepared(&a, None), &a).unwrap();
        finish(&mut local, 1);
        assert_eq!(local.journal().durable_frontier().unwrap().last_seq, 5);
        assert_eq!(local.journal().phase([1; 16]), Some(Phase::Effective));
    }
}
#[test]
#[ignore = "started by the SIGKILL parent"]
fn crash_child_entry() {
    let Some(root) = std::env::var_os("TW_MATERIALIZER_CRASH_ROOT") else {
        return;
    };
    let stage: u8 = std::env::var("TW_MATERIALIZER_CRASH_STAGE")
        .unwrap()
        .parse()
        .unwrap();
    let f = Fixture(root.into());
    let mut local = f.open(false);
    let a = object(1);
    local.prepare(prepared(&a, None), &a).unwrap();
    if stage >= 2 {
        local.write_desired([1; 16]).unwrap();
    }
    if stage >= 3 {
        local.observe_runtime([1; 16], runtime()).unwrap();
    }
    if stage >= 4 {
        local.write_effective([1; 16]).unwrap();
    }
    let mut file = File::create(f.0.join("ready")).unwrap();
    file.write_all(b"synced").unwrap();
    file.sync_all().unwrap();
    loop {
        std::thread::park();
    }
}
