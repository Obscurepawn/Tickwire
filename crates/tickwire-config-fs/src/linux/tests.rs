use super::*;
use std::{
    fs::{self, Permissions},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
static NEXT: AtomicUsize = AtomicUsize::new(1);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "tickwire-config-fs-unit-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(root.join("config")).unwrap();
        fs::set_permissions(root.join("config"), Permissions::from_mode(0o700)).unwrap();
        Self(root)
    }
    fn open(&self) -> Store {
        Store::open_at(
            File::open(&self.0).unwrap(),
            "config",
            fs::metadata(&self.0).unwrap().uid(),
        )
        .unwrap()
    }
    fn target(&self) -> PathBuf {
        self.0.join("config/desired.toml")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn failures_at_real_install_boundaries_keep_old_or_explicitly_uncertain_new_files() {
    for cut in [
        Stage::Created,
        Stage::Written,
        Stage::Verified,
        Stage::FileSynced,
        Stage::Renamed,
        Stage::DirectorySynced,
    ] {
        let f = Fixture::new();
        let mut store = f.open();
        store.install("desired.toml", b"old", None).unwrap();
        let error = store.install_with("desired.toml", b"new", Some(hash(b"old")), |stage| {
            if stage == cut {
                Err(io::Error::other("injected filesystem boundary failure"))
            } else {
                Ok(())
            }
        });
        assert!(error.is_err(), "{cut:?}");
        let renamed = matches!(cut, Stage::Renamed | Stage::DirectorySynced);
        assert_eq!(
            fs::read(f.target()).unwrap(),
            if renamed { b"new" } else { b"old" },
            "{cut:?}"
        );
        assert_eq!(store.is_poisoned(), renamed, "{cut:?}");
        if renamed {
            assert!(matches!(
                store.read("desired.toml", 64),
                Err(Error::Poisoned)
            ));
            assert!(matches!(
                store.install("desired.toml", b"later", Some(hash(b"new"))),
                Err(Error::Poisoned)
            ));
        }
        assert!(fs::read_dir(f.0.join("config")).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".tw-tmp-")
        }));
        drop(store);
        let mut reopened = f.open();
        let actual = if renamed { b"new" } else { b"old" };
        assert_eq!(reopened.read("desired.toml", 64).unwrap(), actual);
        reopened
            .install("desired.toml", b"final", Some(hash(actual)))
            .unwrap();
    }
}
#[test]
fn replacing_directory_or_lock_during_install_is_rejected_before_publication() {
    for replace_lock in [false, true] {
        let f = Fixture::new();
        let mut store = f.open();
        store.create_directory("target").unwrap();
        let root = f.0.join("config");
        let result = store.install_with("target/desired.toml", b"new", None, |stage| {
            if stage == Stage::FileSynced {
                if replace_lock {
                    fs::rename(root.join("LOCK"), root.join("old-lock"))?;
                    File::create(root.join("LOCK"))?;
                    fs::set_permissions(root.join("LOCK"), Permissions::from_mode(0o600))?;
                } else {
                    fs::rename(root.join("target"), root.join("retired"))?;
                    fs::create_dir(root.join("target"))?;
                    fs::set_permissions(root.join("target"), Permissions::from_mode(0o700))?;
                }
            }
            Ok(())
        });
        assert!(result.is_err());
        assert!(!root.join("target/desired.toml").exists());
        assert!(!root.join("retired/desired.toml").exists());
    }
}
#[test]
fn a_tampered_temporary_file_never_replaces_the_prior_image() {
    let f = Fixture::new();
    let mut store = f.open();
    store.install("desired.toml", b"old", None).unwrap();
    let result = store.install_with("desired.toml", b"new", Some(hash(b"old")), |stage| {
        if stage == Stage::Written {
            let temp = fs::read_dir(f.0.join("config"))?
                .find_map(|entry| {
                    let entry = entry.ok()?;
                    entry
                        .file_name()
                        .to_str()?
                        .starts_with(".tw-tmp-")
                        .then(|| entry.path())
                })
                .unwrap();
            fs::write(temp, b"bad")?;
        }
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(store.read("desired.toml", 64).unwrap(), b"old");
}
