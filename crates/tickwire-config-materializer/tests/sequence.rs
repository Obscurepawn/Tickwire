mod common;
use common::*;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    fs,
    path::PathBuf,
    rc::Rc,
    sync::atomic::{AtomicUsize, Ordering},
};
use tickwire_config_journal::{FileFact, Journal, Phase};
use tickwire_config_materializer::{Error, Files, Local, Result};
use tickwire_journal::store::FileStore;
static NEXT: AtomicUsize = AtomicUsize::new(1);
#[derive(Default)]
struct Memory {
    entries: BTreeMap<String, (Vec<u8>, FileFact)>,
    next_inode: u64,
    lose_return: Option<String>,
}
#[derive(Clone, Default)]
struct MemoryFiles(Rc<RefCell<Memory>>);
impl Files for MemoryFiles {
    fn create_directory(&mut self, _: &str) -> Result<()> {
        Ok(())
    }
    fn read(&mut self, path: &str, limit: usize) -> Result<Option<Vec<u8>>> {
        let m = self.0.borrow();
        let bytes = m.entries.get(path).map(|e| e.0.clone());
        if bytes.as_ref().is_some_and(|b| b.len() > limit) {
            return Err(Error::Drift);
        }
        Ok(bytes)
    }
    fn install(
        &mut self,
        path: &str,
        bytes: &[u8],
        previous: Option<[u8; 32]>,
    ) -> Result<FileFact> {
        let mut m = self.0.borrow_mut();
        if let Some((old, fact)) = m.entries.get(path)
            && old == bytes
        {
            return Ok(fact.clone());
        }
        if m.entries.get(path).map(|e| hash(&e.0)) != previous {
            return Err(Error::Conflict);
        }
        m.next_inode += 1;
        let fact = FileFact {
            render_hash: hash(bytes),
            bytes: bytes.len() as u64,
            device: 1,
            inode: m.next_inode,
        };
        m.entries
            .insert(path.into(), (bytes.to_vec(), fact.clone()));
        if m.lose_return.as_deref() == Some(path) {
            m.lose_return = None;
            return Err(Error::File(tickwire_config_fs::Error::Io(
                std::io::Error::other("lost file sync response"),
            )));
        }
        Ok(fact)
    }
}
struct Fixture {
    path: PathBuf,
    files: MemoryFiles,
}
impl Fixture {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!(
                "tickwire-materializer-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )),
            files: MemoryFiles::default(),
        }
    }
    fn open(&self, recover: bool) -> Result<Local<MemoryFiles, FileStore>> {
        let s = FileStore::open(&self.path).unwrap();
        let j = if recover {
            Journal::recover(s, config(), None).unwrap()
        } else {
            Journal::create(s, config()).unwrap()
        };
        Local::open(self.files.clone(), j)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
fn finish(local: &mut Local<MemoryFiles, FileStore>, n: u8) -> tickwire_config_journal::Receipt {
    local.write_desired([n; 16]).unwrap();
    local.observe_runtime([n; 16], runtime()).unwrap();
    local.write_effective([n; 16]).unwrap()
}
#[test]
fn two_generations_keep_exact_history_and_retries_do_not_rewrite_current_files() {
    let f = Fixture::new();
    let mut local = f.open(false).unwrap();
    let a = object(1);
    let p = prepared(&a, None);
    local.prepare(p.clone(), &a).unwrap();
    let first = finish(&mut local, 1);
    let b = object(2);
    let q = prepared(&b, Some(p.next));
    local.prepare(q, &b).unwrap();
    let second = finish(&mut local, 2);
    assert!(second.sequence() > first.sequence());
    let current = f.files.0.borrow().entries["effective.toml"].clone();
    assert_eq!(local.write_effective([1; 16]).unwrap(), first);
    assert_eq!(f.files.0.borrow().entries["effective.toml"], current);
    assert_eq!(
        f.files
            .0
            .borrow()
            .entries
            .keys()
            .filter(|p| p.starts_with("last-good/"))
            .count(),
        2
    );
    drop(local);
    let local = f.open(true).unwrap();
    assert_eq!(local.journal().effective_record().unwrap().revision, 2);
    assert_eq!(local.journal().phase([1; 16]), Some(Phase::Effective));
}
#[test]
fn wrong_projection_binding_is_rejected_before_any_object_is_written() {
    let f = Fixture::new();
    let mut local = f.open(false).unwrap();
    let a = object(1);
    let mut p = prepared(&a, None);
    p.next.projection_hash = [99; 32];
    assert!(local.prepare(p, &a).is_err());
    assert!(f.files.0.borrow().entries.is_empty());
    assert_eq!(local.journal().durable_frontier().unwrap().last_seq, 1);
    assert!(!local.is_poisoned());
}
#[test]
fn desired_written_but_completion_lost_is_recovered_as_original_operation() {
    let f = Fixture::new();
    let mut local = f.open(false).unwrap();
    let a = object(1);
    local.prepare(prepared(&a, None), &a).unwrap();
    f.files.0.borrow_mut().lose_return = Some("desired.toml".into());
    assert!(local.write_desired([1; 16]).is_err());
    assert!(local.is_poisoned());
    assert_eq!(local.journal().phase([1; 16]), Some(Phase::Prepared));
    let before = f.files.0.borrow().entries["desired.toml"].clone();
    drop(local);
    let mut local = f.open(true).unwrap();
    local.write_desired([1; 16]).unwrap();
    assert_eq!(f.files.0.borrow().entries["desired.toml"], before);
    assert_eq!(local.journal().phase([1; 16]), Some(Phase::Desired));
}
#[test]
fn effective_written_but_completion_lost_preserves_runtime_and_file_identity() {
    let f = Fixture::new();
    let mut local = f.open(false).unwrap();
    let a = object(1);
    local.prepare(prepared(&a, None), &a).unwrap();
    local.write_desired([1; 16]).unwrap();
    let r = local.observe_runtime([1; 16], runtime()).unwrap();
    f.files.0.borrow_mut().lose_return = Some("effective.toml".into());
    assert!(local.write_effective([1; 16]).is_err());
    assert!(local.is_poisoned());
    assert_eq!(local.journal().phase([1; 16]), Some(Phase::RuntimeObserved));
    let before = f.files.0.borrow().entries["effective.toml"].clone();
    drop(local);
    let mut local = f.open(true).unwrap();
    local.write_effective([1; 16]).unwrap();
    assert_eq!(
        local
            .journal()
            .operation([1; 16])
            .unwrap()
            .receipt(Phase::RuntimeObserved),
        Some(&r)
    );
    assert_eq!(f.files.0.borrow().entries["effective.toml"], before);
}
#[test]
fn altered_file_identity_and_missing_projection_quarantine_recovery() {
    let f = Fixture::new();
    let mut local = f.open(false).unwrap();
    let a = object(1);
    local.prepare(prepared(&a, None), &a).unwrap();
    finish(&mut local, 1);
    drop(local);
    f.files
        .0
        .borrow_mut()
        .entries
        .get_mut("effective.toml")
        .unwrap()
        .1
        .inode += 1;
    assert!(f.open(true).is_err());
    f.files
        .0
        .borrow_mut()
        .entries
        .get_mut("effective.toml")
        .unwrap()
        .1
        .inode -= 1;
    let path = format!("objects/{}.json", hex(a.binding().object_hash));
    f.files.0.borrow_mut().entries.remove(&path);
    assert!(f.open(true).is_err());
}

#[test]
fn missing_last_good_and_changed_desired_never_advance_runtime_observation() {
    let f = Fixture::new();
    let mut local = f.open(false).unwrap();
    let a = object(1);
    local.prepare(prepared(&a, None), &a).unwrap();
    local.write_desired([1; 16]).unwrap();
    let before = local.journal().durable_frontier().unwrap().clone();
    f.files
        .0
        .borrow_mut()
        .entries
        .get_mut("desired.toml")
        .unwrap()
        .0
        .push(b'\n');
    assert!(local.observe_runtime([1; 16], runtime()).is_err());
    assert_eq!(local.journal().durable_frontier(), Some(&before));
    assert!(local.is_poisoned());
    drop(local);
    assert!(f.open(true).is_err());
    f.files
        .0
        .borrow_mut()
        .entries
        .get_mut("desired.toml")
        .unwrap()
        .0
        .pop();
    let mut local = f.open(true).unwrap();
    finish(&mut local, 1);
    drop(local);
    let path = f
        .files
        .0
        .borrow()
        .entries
        .keys()
        .find(|p| p.starts_with("last-good/"))
        .unwrap()
        .clone();
    f.files.0.borrow_mut().entries.remove(&path);
    assert!(f.open(true).is_err());
}

#[test]
fn conflicting_runtime_retry_and_unrelated_effective_content_are_rejected() {
    let f = Fixture::new();
    let mut local = f.open(false).unwrap();
    let a = object(1);
    local.prepare(prepared(&a, None), &a).unwrap();
    local.write_desired([1; 16]).unwrap();
    let original = local.observe_runtime([1; 16], runtime()).unwrap();
    let mut changed = runtime();
    changed.applied_event_seq += 1;
    assert!(local.observe_runtime([1; 16], changed).is_err());
    assert!(!local.is_poisoned());
    assert_eq!(local.observe_runtime([1; 16], runtime()).unwrap(), original);
    drop(local);
    let desired = f.files.0.borrow().entries["desired.toml"].clone();
    f.files
        .0
        .borrow_mut()
        .entries
        .insert("effective.toml".into(), desired);
    assert!(f.open(true).is_err());
}

#[test]
fn actual_journal_sync_return_loss_recovers_each_record_without_rewriting_files() {
    use std::cell::Cell;
    for phase in [Phase::Desired, Phase::RuntimeObserved, Phase::Effective] {
        let f = Fixture::new();
        let fail = Rc::new(Cell::new(false));
        let journal = Journal::create(
            LoseSync {
                store: FileStore::open(&f.path).unwrap(),
                fail: fail.clone(),
            },
            config(),
        )
        .unwrap();
        let mut local = Local::open(f.files.clone(), journal).unwrap();
        let a = object(1);
        local.prepare(prepared(&a, None), &a).unwrap();
        if phase > Phase::Desired {
            local.write_desired([1; 16]).unwrap();
        }
        if phase > Phase::RuntimeObserved {
            local.observe_runtime([1; 16], runtime()).unwrap();
        }
        fail.set(true);
        let result = match phase {
            Phase::Desired => local.write_desired([1; 16]),
            Phase::RuntimeObserved => local.observe_runtime([1; 16], runtime()),
            Phase::Effective => local.write_effective([1; 16]),
            _ => unreachable!(),
        };
        assert!(result.is_err());
        assert!(local.is_poisoned());
        let files = f.files.0.borrow().entries.clone();
        drop(local);
        let mut local = f.open(true).unwrap();
        assert_eq!(local.journal().phase([1; 16]), Some(phase));
        assert_eq!(f.files.0.borrow().entries, files);
        finish(&mut local, 1);
        assert_eq!(local.journal().durable_frontier().unwrap().last_seq, 5);
    }
}
