use crate::{DurableFile, Error, Hash, MAX_FILE_BYTES, Result, os};
use sha2::{Digest, Sha256};
use std::{
    ffi::{CStr, CString},
    fs::{File, Metadata},
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::fs::MetadataExt,
};
pub(crate) mod journal;
#[cfg(test)]
mod tests;

pub struct Store {
    parent: File,
    root: File,
    root_name: CString,
    lock: File,
    owner: u32,
    device: u64,
    poisoned: bool,
}
struct Existing {
    file: File,
    bytes: Vec<u8>,
    metadata: Metadata,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Created,
    Written,
    Verified,
    FileSynced,
    Renamed,
    DirectorySynced,
}

impl Store {
    pub fn open_at(parent: File, name: &str, owner: u32) -> Result<Self> {
        let root_name = component(name)?;
        anchor(&parent, owner)?;
        let root = os::open(&parent, &root_name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        let device = parent.metadata()?.dev();
        directory(&root, owner, device)?;
        let lock = os::open(&root, c"LOCK", libc::O_RDWR | libc::O_CREAT, 0o600)?;
        let metadata = regular(&lock, owner, device, 0)?;
        if metadata.len() != 0 {
            return Err(Error::Invalid("writer lock length"));
        }
        lock.try_lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        lock.sync_all()?;
        root.sync_all()?;
        parent.sync_all()?;
        let store = Self {
            parent,
            root,
            root_name,
            lock,
            owner,
            device,
            poisoned: false,
        };
        store.validate()?;
        Ok(store)
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    pub fn create_directory(&mut self, path: &str) -> Result<()> {
        let parts = parts(path)?;
        self.validate()?;
        let mut parent = self.root.try_clone()?;
        for (index, name) in parts.iter().enumerate() {
            match os::mkdir(&parent, name) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
            let child = os::open(&parent, name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
            directory(&child, self.owner, self.device)?;
            child.sync_all()?;
            parent.sync_all()?;
            self.verify_directory(&parts[..=index], &child)?;
            parent = child;
        }
        self.validate()
    }
    pub fn read(&self, path: &str, max_bytes: usize) -> Result<Vec<u8>> {
        budget(max_bytes)?;
        let parts = parts(path)?;
        self.validate()?;
        let parent = self.open_directory(&parts[..parts.len() - 1])?;
        let name = parts.last().ok_or(Error::Invalid("empty file path"))?;
        let existing = self
            .existing(&parent, name, max_bytes)?
            .ok_or_else(|| Error::Io(io::Error::from(io::ErrorKind::NotFound)))?;
        self.verify_existing(&parent, name, Some(&existing))?;
        self.verify_directory(&parts[..parts.len() - 1], &parent)?;
        self.validate()?;
        Ok(existing.bytes)
    }
    pub fn install(
        &mut self,
        path: &str,
        bytes: &[u8],
        expected_previous: Option<Hash>,
    ) -> Result<DurableFile> {
        self.install_with(path, bytes, expected_previous, |_| Ok(()))
    }
    fn install_with(
        &mut self,
        path: &str,
        bytes: &[u8],
        expected_previous: Option<Hash>,
        checkpoint: impl FnMut(Stage) -> io::Result<()>,
    ) -> Result<DurableFile> {
        self.install_checked(path, bytes, expected_previous, false, checkpoint)
    }
    fn install_expected(
        &mut self,
        path: &str,
        bytes: &[u8],
        expected_previous: Option<Hash>,
    ) -> Result<DurableFile> {
        self.install_checked(path, bytes, expected_previous, true, |_| Ok(()))
    }
    fn install_checked(
        &mut self,
        path: &str,
        bytes: &[u8],
        expected_previous: Option<Hash>,
        strict_previous: bool,
        mut checkpoint: impl FnMut(Stage) -> io::Result<()>,
    ) -> Result<DurableFile> {
        if bytes.is_empty() || bytes.len() > MAX_FILE_BYTES {
            return Err(Error::Invalid("file size"));
        }
        let parts = parts(path)?;
        self.validate()?;
        let parent_parts = &parts[..parts.len() - 1];
        let parent = self.open_directory(parent_parts)?;
        let name = parts.last().ok_or(Error::Invalid("empty file path"))?;
        let prior = self.existing(&parent, name, MAX_FILE_BYTES)?;
        let digest = hash(bytes);
        let actual_previous = prior.as_ref().map(|old| hash(&old.bytes));
        if strict_previous && actual_previous != expected_previous {
            return Err(Error::Conflict);
        }
        if let Some(prior) = &prior
            && prior.bytes == bytes
        {
            prior.file.sync_all()?;
            parent.sync_all()?;
            self.verify_existing(&parent, name, Some(prior))?;
            self.verify_directory(parent_parts, &parent)?;
            self.validate()?;
            return Ok(receipt(&prior.metadata, digest, bytes.len()));
        }
        if actual_previous != expected_previous {
            return Err(Error::Conflict);
        }
        let (temp, mut file) = self.temporary(&parent)?;
        let mut renamed = false;
        let result = (|| {
            checkpoint(Stage::Created)?;
            file.write_all(bytes)?;
            checkpoint(Stage::Written)?;
            regular(&file, self.owner, self.device, bytes.len())?;
            file.seek(SeekFrom::Start(0))?;
            let mut actual = Vec::new();
            (&file)
                .take(bytes.len() as u64 + 1)
                .read_to_end(&mut actual)?;
            if actual != bytes {
                return Err(Error::Invalid("temporary file content"));
            }
            checkpoint(Stage::Verified)?;
            file.sync_all()?;
            checkpoint(Stage::FileSynced)?;
            self.validate()?;
            self.verify_directory(parent_parts, &parent)?;
            self.verify_existing(&parent, name, prior.as_ref())?;
            os::rename(&parent, &temp, name)?;
            renamed = true;
            checkpoint(Stage::Renamed)?;
            self.validate()?;
            self.verify_directory(parent_parts, &parent)?;
            parent.sync_all()?;
            checkpoint(Stage::DirectorySynced)?;
            let installed = self
                .existing(&parent, name, MAX_FILE_BYTES)?
                .ok_or(Error::Conflict)?;
            let actual_meta = regular(&file, self.owner, self.device, bytes.len())?;
            if !same_inode(&installed.metadata, &actual_meta) || installed.bytes != bytes {
                return Err(Error::Conflict);
            }
            self.validate()?;
            self.verify_directory(parent_parts, &parent)?;
            Ok(receipt(&installed.metadata, digest, bytes.len()))
        })();
        if result.is_err() && (renamed || os::unlink(&parent, &temp).is_err()) {
            self.poisoned = true;
        }
        result
    }
    fn validate(&self) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        anchor(&self.parent, self.owner)?;
        directory(&self.root, self.owner, self.device)?;
        let current = os::open(
            &self.parent,
            &self.root_name,
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        directory(&current, self.owner, self.device)?;
        if !same_inode(&current.metadata()?, &self.root.metadata()?) {
            return Err(Error::Invalid("root replaced"));
        }
        let current_lock = os::open(&self.root, c"LOCK", libc::O_RDONLY, 0)?;
        let current_meta = regular(&current_lock, self.owner, self.device, 0)?;
        let lock_meta = regular(&self.lock, self.owner, self.device, 0)?;
        if !same_inode(&current_meta, &lock_meta) {
            return Err(Error::Invalid("writer lock replaced"));
        }
        Ok(())
    }
    fn open_directory(&self, parts: &[CString]) -> Result<File> {
        let mut file = self.root.try_clone()?;
        for name in parts {
            file = os::open(&file, name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
            directory(&file, self.owner, self.device)?;
        }
        Ok(file)
    }
    fn verify_directory(&self, parts: &[CString], expected: &File) -> Result<()> {
        directory(expected, self.owner, self.device)?;
        let current = self.open_directory(parts)?;
        if same_inode(&current.metadata()?, &expected.metadata()?) {
            Ok(())
        } else {
            Err(Error::Invalid("directory replaced"))
        }
    }
    fn existing(&self, parent: &File, name: &CStr, limit: usize) -> Result<Option<Existing>> {
        let file = match os::open(parent, name, libc::O_RDONLY, 0) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let before = regular(&file, self.owner, self.device, limit)?;
        let mut bytes = Vec::new();
        (&file).take(limit as u64 + 1).read_to_end(&mut bytes)?;
        let after = regular(&file, self.owner, self.device, limit)?;
        if bytes.len() as u64 != before.len() || !same_version(&before, &after) {
            return Err(Error::Invalid("file changed while reading"));
        }
        Ok(Some(Existing {
            file,
            bytes,
            metadata: after,
        }))
    }
    fn verify_existing(
        &self,
        parent: &File,
        name: &CStr,
        expected: Option<&Existing>,
    ) -> Result<()> {
        let actual = self.existing(parent, name, MAX_FILE_BYTES)?;
        match (&actual, expected) {
            (None, None) => Ok(()),
            (Some(actual), Some(expected))
                if same_version(&actual.metadata, &expected.metadata)
                    && actual.bytes == expected.bytes =>
            {
                Ok(())
            }
            _ => Err(Error::Conflict),
        }
    }
    fn temporary(&mut self, parent: &File) -> Result<(CString, File)> {
        for _ in 0..8 {
            let mut random = [0_u8; 16];
            getrandom::getrandom(&mut random)
                .map_err(|_| Error::Invalid("temporary randomness"))?;
            let text: String = random.iter().map(|b| format!("{b:02x}")).collect();
            let name = CString::new(format!(".tw-tmp-{text}"))
                .map_err(|_| Error::Invalid("temporary name"))?;
            match os::open(
                parent,
                &name,
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                0o600,
            ) {
                Ok(file) => {
                    if let Err(error) = regular(&file, self.owner, self.device, 0) {
                        if os::unlink(parent, &name).is_err() {
                            self.poisoned = true;
                        }
                        return Err(error);
                    }
                    return Ok((name, file));
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(Error::Invalid("temporary collision budget"))
    }
}
fn component(value: &str) -> Result<CString> {
    if value.is_empty()
        || value.len() > 128
        || matches!(value, "." | ".." | "LOCK")
        || value.starts_with(".tw-")
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err(Error::Invalid("path component"));
    }
    CString::new(value).map_err(|_| Error::Invalid("path component"))
}
fn parts(path: &str) -> Result<Vec<CString>> {
    if path.len() > 1024 {
        return Err(Error::Invalid("path length"));
    }
    let parts: Vec<_> = path.split('/').collect();
    if parts.len() > 8 {
        return Err(Error::Invalid("path depth"));
    }
    parts.into_iter().map(component).collect()
}
fn anchor(file: &File, owner: u32) -> Result<()> {
    let meta = file.metadata()?;
    if !meta.is_dir() || (meta.uid() != 0 && meta.uid() != owner) || meta.mode() & 0o022 != 0 {
        return Err(Error::Invalid("trusted parent"));
    }
    Ok(())
}
fn directory(file: &File, owner: u32, device: u64) -> Result<()> {
    let meta = file.metadata()?;
    if !meta.is_dir()
        || meta.uid() != owner
        || meta.mode() & 0o7777 != 0o700
        || meta.dev() != device
    {
        return Err(Error::Invalid("directory identity"));
    }
    Ok(())
}
fn regular(file: &File, owner: u32, device: u64, limit: usize) -> Result<Metadata> {
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.uid() != owner
        || meta.mode() & 0o7777 != 0o600
        || meta.nlink() != 1
        || meta.dev() != device
        || meta.len() > limit as u64
    {
        return Err(Error::Invalid("regular file identity or size"));
    }
    Ok(meta)
}
fn same_inode(a: &Metadata, b: &Metadata) -> bool {
    a.dev() == b.dev() && a.ino() == b.ino()
}
fn same_version(a: &Metadata, b: &Metadata) -> bool {
    same_inode(a, b)
        && a.len() == b.len()
        && a.mtime() == b.mtime()
        && a.mtime_nsec() == b.mtime_nsec()
        && a.ctime() == b.ctime()
        && a.ctime_nsec() == b.ctime_nsec()
}
fn budget(max: usize) -> Result<()> {
    if (1..=MAX_FILE_BYTES).contains(&max) {
        Ok(())
    } else {
        Err(Error::Invalid("read budget"))
    }
}
fn hash(bytes: &[u8]) -> Hash {
    Sha256::digest(bytes).into()
}
fn receipt(meta: &Metadata, hash: Hash, bytes: usize) -> DurableFile {
    DurableFile {
        hash,
        bytes,
        device: meta.dev(),
        inode: meta.ino(),
    }
}
