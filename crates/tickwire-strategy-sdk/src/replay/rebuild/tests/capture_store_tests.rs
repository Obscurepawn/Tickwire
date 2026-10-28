use super::capture_tests::{at, fixture, identity, start};
use super::*;
use capture::store::{Attempt, read_back};
use std::{
    os::unix::fs::DirBuilderExt,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "tickwire-capture-store-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, AtomicOrdering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        Self(path)
    }
    fn output(&self) -> PathBuf {
        self.0.join("capture")
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn captured(f: &Fixture) -> capture::Captured {
    let mut owner = start(f);
    for row in &f.inputs.warm {
        owner.push(row.input.clone(), at(row.now_ns)).unwrap();
    }
    owner.finish().unwrap()
}
fn seed(f: &Fixture) -> Inputs {
    let mut seed = f.inputs.clone();
    seed.warm.clear();
    seed
}

#[test]
fn capture_attempt_persists_and_reads_back_every_bound_object() {
    let f = fixture();
    let dir = Directory::new();
    let attempt = Attempt::begin(&dir.output(), &identity(), &f.contract, &seed(&f))
        .expect("durably claim a new capture output directory");
    let completion = attempt.finish(captured(&f)).unwrap();
    let read = read_back(&dir.output(), &completion).unwrap();
    assert_eq!(read.inputs(), &f.inputs);
    assert_eq!(read.seal().end, f.plans.members[0].end);
    assert!(read.objects().any(|b| b == completion.bytes().unwrap()));
    assert!(Attempt::begin(&dir.output(), &identity(), &f.contract, &seed(&f)).is_err());
}

fn object_path(dir: &Directory, object: Object) -> PathBuf {
    dir.output().join(format!(
        "{}.bin",
        object
            .digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    ))
}

#[test]
fn capture_readback_rejects_missing_changed_and_symlinked_objects() {
    for case in 0..8 {
        let f = fixture();
        let dir = Directory::new();
        let done = Attempt::begin(&dir.output(), &identity(), &f.contract, &seed(&f))
            .unwrap()
            .finish(captured(&f))
            .unwrap();
        match case {
            0 => std::fs::remove_file(object_path(&dir, done.contract)).unwrap(),
            1 => std::fs::write(object_path(&dir, done.seed), b"corrupt").unwrap(),
            2 => std::fs::remove_file(object_path(&dir, done.inputs)).unwrap(),
            3 => std::fs::remove_file(object_path(&dir, done.seal)).unwrap(),
            4 => std::fs::write(dir.output().join("ATTEMPT.json"), b"{}").unwrap(),
            5 => std::fs::remove_file(object_path(&dir, done.attempt)).unwrap(),
            6 => {
                std::fs::remove_file(object_path(&dir, Object::of(&done.bytes().unwrap()))).unwrap()
            }
            _ => {
                let path = object_path(&dir, done.inputs);
                let moved = dir.0.join("actual-inputs");
                std::fs::rename(&path, &moved).unwrap();
                std::os::unix::fs::symlink(moved, path).unwrap();
            }
        }
        assert!(read_back(&dir.output(), &done).is_err(), "case {case}");
    }
}

#[test]
fn content_addressed_but_fabricated_seal_is_not_a_verified_window() {
    let f = fixture();
    let dir = Directory::new();
    let done = captured(&f);
    let mut fake_seal = done.seal().clone();
    let mut completion = Attempt::begin(&dir.output(), &identity(), &f.contract, &seed(&f))
        .unwrap()
        .finish(done)
        .unwrap();
    fake_seal.account_input_digest = [77; 32];
    let fake_bytes = fake_seal.bytes().unwrap();
    completion.seal = Object::of(&fake_bytes);
    std::fs::write(object_path(&dir, completion.seal), fake_bytes).unwrap();
    let bytes = completion.bytes().unwrap();
    std::fs::write(object_path(&dir, Object::of(&bytes)), &bytes).unwrap();
    std::fs::write(dir.output().join("CAPTURE_WINDOW.json"), &bytes).unwrap();
    assert!(read_back(&dir.output(), &completion).is_err());
}

#[test]
fn failed_capture_persistence_never_completes_or_reopens_the_attempt() {
    for case in 0..5 {
        let f = fixture();
        let dir = Directory::new();
        let attempt = Attempt::begin(&dir.output(), &identity(), &f.contract, &seed(&f)).unwrap();
        let done = captured(&f);
        let blocked = match case {
            0 => object_path(&dir, done.seal().contract),
            1 => object_path(&dir, done.seal().seed),
            2 => object_path(&dir, done.seal().inputs),
            3 => object_path(&dir, Object::of(&done.seal().bytes().unwrap())),
            _ => dir.output().join("CAPTURE_WINDOW.json"),
        };
        std::fs::create_dir(&blocked).unwrap();
        assert!(attempt.finish(done).is_err(), "case {case}");
        assert!(!dir.output().join("CAPTURE_WINDOW.json").is_file());
        assert!(Attempt::begin(&dir.output(), &identity(), &f.contract, &seed(&f)).is_err());
    }
}

#[test]
fn attempt_rejects_other_identity_and_typed_roots_over_budget() {
    let f = fixture();
    let dir = Directory::new();
    let mut id = identity();
    id.capture = [99; 16];
    let attempt = Attempt::begin(&dir.output(), &id, &f.contract, &seed(&f)).unwrap();
    assert!(attempt.finish(captured(&f)).is_err());
    assert!(!dir.output().join("CAPTURE_WINDOW.json").exists());
    let dir = Directory::new();
    let mut done = Attempt::begin(&dir.output(), &identity(), &f.contract, &seed(&f))
        .unwrap()
        .finish(captured(&f))
        .unwrap();
    done.inputs.bytes = MAX_BYTES as u64 + 1;
    assert!(done.bytes().is_err());
    assert!(read_back(&dir.output(), &done).is_err());
}

#[test]
fn output_directory_replacement_cannot_finish_the_original_attempt() {
    let f = fixture();
    let dir = Directory::new();
    let attempt = Attempt::begin(&dir.output(), &identity(), &f.contract, &seed(&f)).unwrap();
    let original = dir.0.join("original");
    std::fs::rename(dir.output(), &original).unwrap();
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(dir.output())
        .unwrap();
    std::fs::copy(
        original.join("ATTEMPT.json"),
        dir.output().join("ATTEMPT.json"),
    )
    .unwrap();
    assert!(attempt.finish(captured(&f)).is_err());
    assert!(!dir.output().join("CAPTURE_WINDOW.json").exists());
}
