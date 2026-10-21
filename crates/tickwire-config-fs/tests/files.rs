#![cfg(target_os = "linux")]
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, Permissions},
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
use tickwire_config_fs::{MAX_FILE_BYTES, Store};
static NEXT: AtomicUsize = AtomicUsize::new(1);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "tickwire-config-fs-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(root.join("config")).unwrap();
        fs::set_permissions(root.join("config"), Permissions::from_mode(0o700)).unwrap();
        Self(root)
    }
    fn open(&self) -> tickwire_config_fs::Result<Store> {
        Store::open_at(
            File::open(&self.0).unwrap(),
            "config",
            fs::metadata(&self.0).unwrap().uid(),
        )
    }
    fn file(&self, path: &str) -> PathBuf {
        let root = self.0.join("config");
        if path.is_empty() {
            root
        } else {
            root.join(path)
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[test]
fn actual_install_retry_replace_and_reopen_preserve_exact_bytes() {
    let f = Fixture::new();
    let mut store = f.open().unwrap();
    store.create_directory("strategy-01/last-good").unwrap();
    let one = store
        .install("strategy-01/desired.toml", b"revision = \"1\"\n", None)
        .unwrap();
    assert_eq!(one.hash(), hash(b"revision = \"1\"\n"));
    assert_eq!(one.bytes(), 15);
    assert_eq!(
        one.inode(),
        fs::metadata(f.file("strategy-01/desired.toml"))
            .unwrap()
            .ino()
    );
    let same = store
        .install("strategy-01/desired.toml", b"revision = \"1\"\n", None)
        .unwrap();
    assert_eq!(same.inode(), one.inode());
    assert!(
        store
            .install("strategy-01/desired.toml", b"revision = \"2\"\n", None)
            .is_err()
    );
    let two = store
        .install(
            "strategy-01/desired.toml",
            b"revision = \"2\"\n",
            Some(one.hash()),
        )
        .unwrap();
    assert_ne!(two.inode(), one.inode());
    assert_eq!(two.device(), one.device());
    drop(store);
    let store = f.open().unwrap();
    assert_eq!(
        store
            .read("strategy-01/desired.toml", MAX_FILE_BYTES)
            .unwrap(),
        b"revision = \"2\"\n"
    );
    let m = fs::metadata(f.file("strategy-01/desired.toml")).unwrap();
    assert_eq!(m.mode() & 0o7777, 0o600);
    assert_eq!(m.nlink(), 1);
}
#[test]
fn one_kernel_lock_covers_all_subdirectories_until_drop() {
    let f = Fixture::new();
    let mut first = f.open().unwrap();
    first.create_directory("a/b").unwrap();
    assert!(f.open().is_err());
    drop(first);
    assert!(f.open().is_ok());
}
#[test]
fn paths_cannot_escape_use_symlinks_or_replace_the_writer_lock() {
    let f = Fixture::new();
    let mut store = f.open().unwrap();
    for path in [
        "",
        "/absolute",
        "../outside",
        "a/../b",
        "a//b",
        "./a",
        "a/",
        "LOCK",
        "a/LOCK",
        ".tw-tmp-forged",
        "a\0b",
    ] {
        assert!(store.install(path, b"x", None).is_err(), "{path:?}");
        assert!(store.create_directory(path).is_err(), "{path:?}");
    }
    fs::create_dir(f.0.join("outside")).unwrap();
    fs::write(f.0.join("outside/desired.toml"), b"outside").unwrap();
    symlink(f.0.join("outside"), f.file("linked")).unwrap();
    assert!(
        store
            .install("linked/desired.toml", b"changed", None)
            .is_err()
    );
    symlink(f.0.join("outside/desired.toml"), f.file("desired.toml")).unwrap();
    assert!(store.read("desired.toml", 64).is_err());
    assert!(store.install("desired.toml", b"changed", None).is_err());
    assert_eq!(
        fs::read(f.0.join("outside/desired.toml")).unwrap(),
        b"outside"
    );
}
#[test]
fn file_hard_links_permissions_and_lock_aliases_are_rejected() {
    let f = Fixture::new();
    let mut store = f.open().unwrap();
    store.install("desired.toml", b"one", None).unwrap();
    fs::hard_link(f.file("desired.toml"), f.file("alias")).unwrap();
    assert!(store.read("desired.toml", 64).is_err());
    assert!(
        store
            .install("desired.toml", b"two", Some(hash(b"one")))
            .is_err()
    );
    fs::remove_file(f.file("alias")).unwrap();
    fs::set_permissions(f.file("desired.toml"), Permissions::from_mode(0o640)).unwrap();
    assert!(store.read("desired.toml", 64).is_err());
    drop(store);
    fs::hard_link(f.file("LOCK"), f.file("lock-alias")).unwrap();
    assert!(f.open().is_err());
}
#[test]
fn renamed_root_and_directory_permission_drift_cannot_receive_writes() {
    let f = Fixture::new();
    let mut store = f.open().unwrap();
    store.create_directory("target").unwrap();
    fs::set_permissions(f.file("target"), Permissions::from_mode(0o770)).unwrap();
    assert!(store.install("target/desired.toml", b"x", None).is_err());
    fs::rename(f.file(""), f.0.join("retired")).unwrap();
    fs::create_dir(f.file("")).unwrap();
    fs::set_permissions(f.file(""), Permissions::from_mode(0o700)).unwrap();
    assert!(store.create_directory("new-target").is_err());
    assert!(store.install("desired.toml", b"x", None).is_err());
    assert!(!f.0.join("retired/desired.toml").exists());
    assert!(!f.file("desired.toml").exists());
}
#[test]
fn bounded_reads_and_failed_compare_leave_old_content_in_place() {
    let f = Fixture::new();
    let mut store = f.open().unwrap();
    store.install("desired.toml", b"original", None).unwrap();
    assert!(store.read("desired.toml", 7).is_err());
    assert!(store.read("desired.toml", 0).is_err());
    assert!(store.read("desired.toml", MAX_FILE_BYTES + 1).is_err());
    assert!(
        store
            .install("desired.toml", b"replacement", Some([1; 32]))
            .is_err()
    );
    assert!(
        store
            .install("missing.toml", b"replacement", Some(hash(b"original")))
            .is_err()
    );
    assert!(
        store
            .install(
                "desired.toml",
                &vec![0; MAX_FILE_BYTES + 1],
                Some(hash(b"original"))
            )
            .is_err()
    );
    assert_eq!(store.read("desired.toml", 8).unwrap(), b"original");
}
#[test]
fn nonprivate_or_symlink_roots_and_wrong_owner_fail_before_lock_creation() {
    let f = Fixture::new();
    let owner = fs::metadata(&f.0).unwrap().uid();
    assert!(Store::open_at(File::open(&f.0).unwrap(), "config", owner.wrapping_add(1)).is_err());
    fs::set_permissions(f.file(""), Permissions::from_mode(0o755)).unwrap();
    assert!(f.open().is_err());
    assert!(!f.file("LOCK").exists());
    fs::rename(f.file(""), f.0.join("original")).unwrap();
    symlink(f.0.join("original"), f.file("")).unwrap();
    assert!(f.open().is_err());
}
