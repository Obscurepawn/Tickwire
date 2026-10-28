use crate::{Result, invalid};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
};

pub(super) struct Files {
    path: PathBuf,
    directory: File,
    lock: Lock,
    uid: u32,
}
struct Lock(File);
impl Drop for Lock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}
fn options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
    options
}
fn same(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    a.dev() == b.dev() && a.ino() == b.ino()
}
fn regular(meta: &fs::Metadata, uid: u32, maximum: usize) -> Result<()> {
    if !meta.is_file()
        || meta.uid() != uid
        || meta.mode() & 0o7777 != 0o600
        || meta.nlink() != 1
        || meta.len() > maximum as u64
    {
        return Err(invalid("MD recovery file identity/permission/size"));
    }
    Ok(())
}
impl Files {
    pub(super) fn open(path: &Path) -> Result<Self> {
        if !cfg!(target_os = "linux") || !path.is_absolute() || path.parent().is_none() {
            return Err(invalid("MD recovery requires an absolute Linux directory"));
        }
        let uid = fs::metadata("/proc/self")?.uid();
        match fs::DirBuilder::new().mode(0o700).create(path) {
            Ok(()) => File::open(path.parent().ok_or_else(|| invalid("MD recovery parent"))?)?
                .sync_all()?,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        let meta = fs::symlink_metadata(path)?;
        if !meta.is_dir()
            || meta.uid() != uid
            || meta.mode() & 0o7777 != 0o700
            || fs::canonicalize(path)? != path
        {
            return Err(invalid("MD recovery private directory"));
        }
        let directory = options()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        if !same(&meta, &directory.metadata()?) {
            return Err(invalid("MD recovery directory replaced"));
        }
        let pinned = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
        let file = options()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .open(pinned.join("LOCK"))?;
        regular(&file.metadata()?, uid, 0)?;
        file.try_lock()
            .map_err(|e| invalid(&format!("MD recovery exclusive lock: {e}")))?;
        let lock = Lock(file);
        let result = Self {
            path: path.to_owned(),
            directory,
            lock,
            uid,
        };
        result.check()?;
        result.directory.sync_all()?;
        Ok(result)
    }
    pub(super) fn path(&self, name: &str) -> PathBuf {
        PathBuf::from(format!("/proc/self/fd/{}", self.directory.as_raw_fd())).join(name)
    }
    pub(super) fn check(&self) -> Result<()> {
        let actual = fs::symlink_metadata(&self.path)?;
        let fixed = self.directory.metadata()?;
        if !actual.is_dir()
            || actual.uid() != self.uid
            || actual.mode() & 0o7777 != 0o700
            || !same(&actual, &fixed)
            || fs::canonicalize(&self.path)? != self.path
        {
            return Err(invalid("MD recovery directory replaced"));
        }
        let lock = options().read(true).open(self.path("LOCK"))?;
        let actual = lock.metadata()?;
        let fixed = self.lock.0.metadata()?;
        regular(&actual, self.uid, 0)?;
        regular(&fixed, self.uid, 0)?;
        if !same(&actual, &fixed) {
            return Err(invalid("MD recovery lock replaced"));
        }
        Ok(())
    }
    pub(super) fn inventory(&self, maximum: usize) -> Result<Vec<(String, usize)>> {
        let mut entries = Vec::new();
        for entry in fs::read_dir(self.path(""))? {
            if entries.len() >= maximum {
                return Err(invalid("MD recovery object count"));
            }
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| invalid("MD recovery filename"))?;
            let meta = fs::symlink_metadata(entry.path())?;
            regular(&meta, self.uid, super::MAX_SNAPSHOT_BYTES)?;
            entries.push((name, meta.len() as usize));
        }
        Ok(entries)
    }
    pub(super) fn read(&self, name: &str, maximum: usize) -> Result<Vec<u8>> {
        let file = options().read(true).open(self.path(name))?;
        let before = file.metadata()?;
        regular(&before, self.uid, maximum)?;
        let mut bytes = Vec::new();
        (&file).take(maximum as u64 + 1).read_to_end(&mut bytes)?;
        let after = file.metadata()?;
        regular(&after, self.uid, maximum)?;
        if bytes.len() > maximum
            || before.len() != bytes.len() as u64
            || before.len() != after.len()
            || before.mtime() != after.mtime()
            || before.mtime_nsec() != after.mtime_nsec()
            || before.ctime() != after.ctime()
            || before.ctime_nsec() != after.ctime_nsec()
        {
            return Err(invalid("MD recovery file changed during read"));
        }
        Ok(bytes)
    }
    pub(super) fn create(&self, name: &str, bytes: &[u8]) -> Result<()> {
        let mut file = options()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.path(name))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        regular(&file.metadata()?, self.uid, bytes.len())?;
        self.directory.sync_all()?;
        Ok(())
    }
    pub(super) fn publish_head(&self, bytes: &[u8]) -> Result<()> {
        self.create("HEAD.pending", bytes)?;
        fs::rename(self.path("HEAD.pending"), self.path("HEAD"))?;
        self.directory.sync_all()?;
        if self.read("HEAD", 512)? != bytes {
            return Err(invalid("MD recovery HEAD replaced"));
        }
        self.check()
    }
    #[cfg(all(test, target_os = "linux"))]
    pub(super) fn inherited_lock(&self) -> File {
        self.lock.0.try_clone().unwrap()
    }
}
