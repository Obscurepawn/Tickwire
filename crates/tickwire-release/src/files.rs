//! Actual bounded artifact reads from a trusted private deployment directory.
//! The deployment owner keeps these files immutable through exec; a hash alone
//! does not protect a pathname from a later same-UID replacement.
use crate::{Artifact, Error, MAX_BINARY, MAX_FILES, Object, Result, Role};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions,
    io::Read,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectFile {
    pub path: PathBuf,
    pub object: Object,
}
impl ObjectFile {
    pub fn read(&self) -> Result<Vec<u8>> {
        read_object(&self.path, self.object)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Location {
    pub role: Role,
    pub path: PathBuf,
}
struct Loaded {
    role: Role,
    path: PathBuf,
    object: Object,
    bytes: Option<Vec<u8>>,
    device: u64,
    inode: u64,
}
/// Constructed only after all required files have been read and hashed.
pub struct LoadedFiles {
    files: Vec<Loaded>,
}
impl LoadedFiles {
    /// An input may share a role with another use, but both its path and content
    /// must be covered by an artifact that was actually read.
    pub fn contains_input(&self, path: &Path, digest: crate::Hash) -> bool {
        self.files
            .iter()
            .any(|f| f.path == path && f.object.digest == digest)
    }
    pub fn path(&self, role: &Role) -> Option<&Path> {
        self.files
            .iter()
            .find(|f| &f.role == role)
            .map(|f| f.path.as_path())
    }
    pub fn bytes(&self, role: &Role) -> Option<&[u8]> {
        self.files
            .iter()
            .find(|f| &f.role == role)
            .and_then(|f| f.bytes.as_deref())
    }
    pub fn object(&self, role: &Role) -> Option<Object> {
        self.files
            .iter()
            .find(|f| &f.role == role)
            .map(|f| f.object)
    }
    /// Recheck both content and identity immediately before using executable paths.
    pub fn recheck(&self) -> Result<()> {
        for f in &self.files {
            let loaded = read(&f.path, &f.role, f.object)?;
            if loaded.device != f.device || loaded.inode != f.inode {
                return Err(Error::Identity);
            }
        }
        Ok(())
    }
}
pub fn load(required: &[Artifact], locations: &[Location]) -> Result<LoadedFiles> {
    crate::validate_files(required)?;
    if locations.len() != required.len() || locations.len() > MAX_FILES {
        return Err(Error::Missing);
    }
    let mut files = Vec::new();
    let mut total = 0u64;
    for (expected, location) in required.iter().zip(locations) {
        if location.role != expected.role {
            return Err(Error::Identity);
        }
        if location.role != Role::Binary {
            total = total
                .checked_add(expected.object.bytes)
                .filter(|n| *n <= tickwire_recovery::MAX_CLOSURE_BYTES)
                .ok_or(Error::Bounds)?;
        }
        files.push(read(&location.path, &location.role, expected.object)?);
    }
    Ok(LoadedFiles { files })
}
/// Read one exact content-addressed object under the same deployment permissions.
pub fn read_object(path: &Path, object: Object) -> Result<Vec<u8>> {
    read(path, &Role::StrategyConfig, object)?
        .bytes
        .ok_or(Error::Missing)
}

fn read(path: &Path, role: &Role, object: Object) -> Result<Loaded> {
    let max = if *role == Role::Binary {
        MAX_BINARY
    } else {
        tickwire_recovery::MAX_OBJECT_BYTES as u64
    };
    object.validate(max)?;
    if !path.is_absolute() {
        return Err(Error::Identity);
    }
    let parent = path.parent().ok_or(Error::Identity)?;
    let m = std::fs::symlink_metadata(parent).map_err(|_| Error::Missing)?;
    if !m.is_dir() || m.mode() & 0o022 != 0 {
        return Err(Error::Identity);
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| Error::Missing)?;
    let before = file.metadata().map_err(|_| Error::Missing)?;
    if !before.is_file()
        || before.mode() & 0o022 != 0
        || before.uid() != m.uid()
        || before.len() != object.bytes
    {
        return Err(Error::Identity);
    }
    let mut hash = Sha256::new();
    let mut buf = [0u8; 65536];
    let mut count = 0u64;
    let mut bytes = if *role == Role::Binary {
        None
    } else {
        Some(Vec::with_capacity(object.bytes as usize))
    };
    loop {
        let n = file.read(&mut buf).map_err(|_| Error::Missing)?;
        if n == 0 {
            break;
        }
        count = count
            .checked_add(n as u64)
            .filter(|n| *n <= object.bytes)
            .ok_or(Error::Bounds)?;
        hash.update(&buf[..n]);
        if let Some(bytes) = &mut bytes {
            bytes.extend_from_slice(&buf[..n]);
        }
    }
    let after = file.metadata().map_err(|_| Error::Missing)?;
    let digest: [u8; 32] = hash.finalize().into();
    if count != object.bytes
        || digest != object.digest
        || after.len() != before.len()
        || after.mtime() != before.mtime()
        || after.mtime_nsec() != before.mtime_nsec()
        || after.ctime() != before.ctime()
        || after.ctime_nsec() != before.ctime_nsec()
    {
        return Err(Error::Identity);
    }
    Ok(Loaded {
        role: role.clone(),
        path: path.to_path_buf(),
        object,
        bytes,
        device: before.dev(),
        inode: before.ino(),
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Write,
        os::unix::fs::PermissionsExt,
        sync::atomic::{AtomicU64, Ordering},
    };
    static NEXT: AtomicU64 = AtomicU64::new(1);
    struct Dir(PathBuf);
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn fixture() -> (Dir, Vec<Artifact>, Vec<Location>) {
        let root = std::env::temp_dir().join(format!(
            "tickwire-release-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut refs = Vec::new();
        let mut locations = Vec::new();
        for (i, role) in [
            Role::Binary,
            Role::HostConfig,
            Role::Bootstrap,
            Role::StrategyConfig,
        ]
        .into_iter()
        .enumerate()
        {
            let bytes = vec![i as u8 + 1; 100];
            let path = root.join(i.to_string());
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
                .unwrap();
            f.write_all(&bytes).unwrap();
            refs.push(Artifact {
                role: role.clone(),
                object: Object::of(&bytes),
            });
            locations.push(Location { role, path });
        }
        (Dir(root), refs, locations)
    }
    #[test]
    fn all_actual_artifacts_are_read_and_rechecked_before_launch() {
        let (_dir, refs, locations) = fixture();
        let loaded = load(&refs, &locations).unwrap();
        assert_eq!(loaded.bytes(&Role::Bootstrap), Some(&[3; 100][..]));
        assert!(loaded.bytes(&Role::Binary).is_none());
        assert!(loaded.recheck().is_ok());
        std::fs::write(&locations[1].path, [9; 100]).unwrap();
        assert!(load(&refs, &locations).is_err());
        assert!(loaded.recheck().is_err());
    }
    #[test]
    fn wrong_role_symlink_and_writable_parent_fail_closed() {
        let (dir, refs, mut locations) = fixture();
        locations.swap(0, 1);
        assert!(load(&refs, &locations).is_err());
        locations.swap(0, 1);
        let target = locations[2].path.with_extension("original");
        std::fs::rename(&locations[2].path, &target).unwrap();
        std::os::unix::fs::symlink(&target, &locations[2].path).unwrap();
        assert!(load(&refs, &locations).is_err());
        std::fs::remove_file(&locations[2].path).unwrap();
        std::fs::rename(target, &locations[2].path).unwrap();
        std::fs::set_permissions(&dir.0, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(load(&refs, &locations).is_err());
    }
}
