use crate::*;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

/// Exclusive, trusted private directory; no deletion API is exposed.
pub struct FileBlobStore {
    root: PathBuf,
    _lock: File,
    counter: u64,
}
impl FileBlobStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(&root) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                if !fs::symlink_metadata(&root)?.file_type().is_dir() {
                    return Err(Error::Invalid("store directory"));
                }
            }
            Err(e) => return Err(e.into()),
        }
        let root = fs::canonicalize(root)?;
        let lock_path = root.join("LOCK");
        if let Ok(meta) = fs::symlink_metadata(&lock_path)
            && !meta.file_type().is_file()
        {
            return Err(Error::Invalid("store lock type"));
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        lock.try_lock()
            .map_err(|e| io::Error::other(e.to_string()))?;
        lock.sync_all()?;
        File::open(&root)?.sync_all()?;
        // Also covers retry after a prior mkdir succeeded but parent sync failed.
        File::open(
            root.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?
        .sync_all()?;
        Ok(Self {
            root,
            _lock: lock,
            counter: 0,
        })
    }
    fn path(&self, key: StoreKey) -> PathBuf {
        let (tag, hash) = match key {
            StoreKey::Object(h) => ("object", h),
            StoreKey::Manifest(h) => ("manifest", h),
            StoreKey::Binding(h) => ("binding", h),
        };
        let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
        self.root.join(format!("{tag}-{hex}"))
    }
    fn file(&self, key: StoreKey) -> Result<File> {
        let path = self.path(key);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(Error::Missing),
            Err(e) => return Err(e.into()),
        };
        if !metadata.file_type().is_file() {
            return Err(Error::Invalid("blob file type"));
        }
        Ok(File::open(path)?)
    }
}
impl BlobStore for FileBlobStore {
    fn get(&self, key: StoreKey, max_bytes: usize) -> Result<Vec<u8>> {
        let file = self.file(key)?;
        if file.metadata()?.len() > max_bytes as u64 {
            return Err(Error::Capacity);
        }
        let mut out = Vec::new();
        file.take((max_bytes as u64).saturating_add(1))
            .read_to_end(&mut out)?;
        if out.len() > max_bytes {
            return Err(Error::Capacity);
        }
        Ok(out)
    }
    fn put_sync(&mut self, key: StoreKey, bytes: &[u8]) -> Result<()> {
        let limit = match key {
            StoreKey::Object(_) => MAX_OBJECT_BYTES,
            StoreKey::Manifest(_) => MAX_MANIFEST_BYTES,
            StoreKey::Binding(_) => 32,
        };
        if bytes.is_empty() || bytes.len() > limit {
            return Err(Error::Capacity);
        }
        match self.get(key, limit) {
            Ok(prior) => {
                if prior != bytes {
                    return Err(Error::Conflict);
                }
                self.file(key)?.sync_all()?;
                File::open(&self.root)?.sync_all()?;
                return Ok(());
            }
            Err(Error::Missing) => {}
            Err(e) => return Err(e),
        }
        // The lock makes rename non-overwriting with respect to all service writers.
        let (path, mut file) = loop {
            self.counter = self.counter.checked_add(1).ok_or(Error::Capacity)?;
            let path = self
                .root
                .join(format!("TEMP-{}-{}", std::process::id(), self.counter));
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&path) {
                Ok(f) => break (path, f),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e.into()),
            }
        };
        let result = (|| {
            file.write_all(bytes)?;
            file.sync_all()?;
            fs::rename(&path, self.path(key))?;
            File::open(&self.root)?.sync_all()?;
            Ok(())
        })();
        // A failed rename can leave an unpublished temporary file, never a binding.
        let _ = fs::remove_file(&path);
        result
    }
}
