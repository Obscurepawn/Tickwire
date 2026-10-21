use super::{Store, hash, regular, same_inode, same_version};
use crate::{Error, Hash, JournalLimits, Result, os};
use std::{
    collections::BTreeMap,
    ffi::CString,
    fs::{File, Metadata},
    io::{self, Read, Seek, SeekFrom, Write},
};
const MAX_MANIFEST: usize = 65_536;
struct Pending {
    id: u64,
    file: File,
    metadata: Metadata,
    dirty: bool,
}
pub struct JournalStore {
    root: Store,
    limits: JournalLimits,
    manifest: Option<Hash>,
    manifest_loaded: bool,
    pending: Option<Pending>,
    observed: BTreeMap<u64, Metadata>,
}
impl JournalStore {
    pub fn new(root: Store, limits: JournalLimits) -> Result<Self> {
        if !(528..=tickwire_journal::SEGMENT_MAX_BYTES).contains(&limits.max_segment_bytes)
            || !(1..=4096).contains(&limits.max_segments)
            || limits.max_total_bytes < limits.max_segment_bytes
            || limits.max_total_bytes > 4 * 1024 * 1024 * 1024
        {
            return Err(Error::Invalid("journal capacity"));
        }
        root.validate()?;
        Ok(Self {
            root,
            limits,
            manifest: None,
            manifest_loaded: false,
            pending: None,
            observed: BTreeMap::new(),
        })
    }
    fn metadata(&self, file: &File) -> Result<Metadata> {
        regular(
            file,
            self.root.owner,
            self.root.device,
            self.limits.max_segment_bytes as usize,
        )
    }
    fn open_segment(&self, id: u64, writable: bool) -> Result<File> {
        let file = os::open(
            &self.root.root,
            &name(id)?,
            if writable {
                libc::O_RDWR
            } else {
                libc::O_RDONLY
            },
            0,
        )?;
        self.metadata(&file)?;
        Ok(file)
    }
    fn pinned(&self, id: u64, file: &File, expected: &Metadata) -> Result<()> {
        self.root.validate()?;
        let current = self.open_segment(id, false)?;
        let actual = self.metadata(file)?;
        let visible = self.metadata(&current)?;
        if !same_version(&actual, expected) || !same_version(&visible, expected) {
            return Err(Error::Conflict);
        }
        self.root.validate()
    }
    fn scan(&self) -> Result<(Vec<u64>, u64)> {
        self.root.validate()?;
        let entries = os::entries(&self.root.root, self.limits.max_segments + 34)?;
        let mut ids = Vec::new();
        let mut total = 0u64;
        for bytes in entries {
            let text = std::str::from_utf8(&bytes)
                .map_err(|_| Error::Invalid("journal directory name"))?;
            if text == "LOCK" {
                continue;
            }
            let component = CString::new(bytes.clone())
                .map_err(|_| Error::Invalid("journal directory name"))?;
            let file = os::open(&self.root.root, &component, libc::O_RDONLY, 0)?;
            if text == "MANIFEST.json"
                || text
                    .strip_prefix(".tw-tmp-")
                    .is_some_and(|s| s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit()))
            {
                regular(&file, self.root.owner, self.root.device, MAX_MANIFEST)?;
            } else {
                let id = text
                    .strip_prefix("segment-")
                    .and_then(|s| s.strip_suffix(".wal"))
                    .filter(|s| s.len() == 20 && s.bytes().all(|b| b.is_ascii_digit()))
                    .and_then(|s| s.parse::<u64>().ok())
                    .filter(|id| *id > 0)
                    .ok_or(Error::Invalid("journal segment name"))?;
                let metadata = self.metadata(&file)?;
                self.check_observed(id, &metadata)?;
                total = total
                    .checked_add(metadata.len())
                    .ok_or(Error::Invalid("journal total bytes"))?;
                ids.push(id);
                if ids.len() > self.limits.max_segments || total > self.limits.max_total_bytes {
                    return Err(Error::Invalid("journal retained capacity"));
                }
            }
        }
        self.root.validate()?;
        ids.sort_unstable();
        if ids.windows(2).any(|p| p[0] == p[1]) {
            return Err(Error::Invalid("duplicate journal segment"));
        }
        if self
            .observed
            .keys()
            .any(|id| ids.binary_search(id).is_err())
        {
            return Err(Error::Conflict);
        }
        Ok((ids, total))
    }
    fn take_segment(&mut self, id: u64) -> Result<Pending> {
        if let Some(pending) = self.pending.take() {
            if pending.id == id {
                self.pinned(id, &pending.file, &pending.metadata)?;
                return Ok(pending);
            }
            if pending.dirty {
                self.pending = Some(pending);
                return Err(Error::Invalid("unsynchronized prior segment"));
            }
        }
        self.load_segment(id)
    }
    fn check_observed(&self, id: u64, metadata: &Metadata) -> Result<()> {
        if self.observed.len() >= self.limits.max_segments && !self.observed.contains_key(&id) {
            return Err(Error::Invalid("observed segment capacity"));
        }
        if self
            .observed
            .get(&id)
            .is_some_and(|old| !same_version(old, metadata))
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    fn load_segment(&self, id: u64) -> Result<Pending> {
        let file = self.open_segment(id, true)?;
        let metadata = self.metadata(&file)?;
        self.check_observed(id, &metadata)?;
        self.pinned(id, &file, &metadata)?;
        Ok(Pending {
            id,
            file,
            metadata,
            dirty: false,
        })
    }
    fn failed<T>(&mut self, result: Result<T>) -> io::Result<T> {
        if result.is_err() {
            self.root.poisoned = true;
        }
        result.map_err(error)
    }
}
impl tickwire_journal::store::Store for JournalStore {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        let result = match self.root.read("MANIFEST.json", MAX_MANIFEST) {
            Ok(bytes) => Some(bytes),
            Err(Error::Io(e)) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(error(e)),
        };
        let digest = result.as_deref().map(hash);
        if self.manifest_loaded && self.manifest != digest {
            return Err(error(Error::Conflict));
        }
        self.manifest = digest;
        self.manifest_loaded = true;
        Ok(result)
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() || bytes.len() > MAX_MANIFEST {
            return Err(error(Error::Invalid("manifest size")));
        }
        let result = self
            .root
            .install_expected("MANIFEST.json", bytes, self.manifest);
        let receipt = self.failed(result)?;
        self.manifest = Some(receipt.hash());
        self.manifest_loaded = true;
        Ok(())
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        self.scan().map(|(ids, _)| ids).map_err(error)
    }
    fn read_segment(&mut self, id: u64, max_bytes: u64) -> io::Result<Vec<u8>> {
        let result = (|| {
            if max_bytes == 0 || max_bytes > tickwire_journal::SEGMENT_MAX_BYTES {
                return Err(Error::Invalid("segment read budget"));
            }
            let max_bytes = max_bytes.min(self.limits.max_segment_bytes);
            self.root.validate()?;
            let file = self.open_segment(id, false)?;
            let before = self.metadata(&file)?;
            self.check_observed(id, &before)?;
            if before.len() > max_bytes {
                return Err(Error::Invalid("segment size"));
            }
            let mut bytes = Vec::new();
            (&file).take(max_bytes + 1).read_to_end(&mut bytes)?;
            if bytes.len() as u64 != before.len() {
                return Err(Error::Conflict);
            }
            self.pinned(id, &file, &before)?;
            if self.observed.len() >= self.limits.max_segments && !self.observed.contains_key(&id) {
                return Err(Error::Invalid("observed segment capacity"));
            }
            self.observed.insert(id, before);
            Ok(bytes)
        })();
        result.map_err(error)
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        let (ids, _) = self.scan().map_err(error)?;
        if ids.len() >= self.limits.max_segments || ids.binary_search(&id).is_ok() {
            return Err(error(Error::Invalid("segment count or duplicate")));
        }
        if self.observed.len() >= self.limits.max_segments {
            return Err(error(Error::Invalid("observed segment capacity")));
        }
        let component = name(id).map_err(error)?;
        if self.pending.as_ref().is_some_and(|p| p.dirty) {
            return Err(error(Error::Invalid("unsynchronized prior segment")));
        }
        let result = (|| {
            let file = os::open(
                &self.root.root,
                &component,
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                0o600,
            )?;
            let metadata = self.metadata(&file)?;
            if metadata.len() != 0 {
                return Err(Error::Conflict);
            }
            file.sync_all()?;
            self.root.root.sync_all()?;
            self.pinned(id, &file, &metadata)?;
            self.observed.insert(id, metadata.clone());
            self.pending = Some(Pending {
                id,
                file,
                metadata,
                dirty: false,
            });
            Ok(())
        })();
        self.failed(result)
    }
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let (_, total) = self.scan().map_err(error)?;
        let length = offset
            .checked_add(bytes.len() as u64)
            .filter(|n| *n <= self.limits.max_segment_bytes)
            .ok_or_else(|| error(Error::Invalid("segment append budget")))?;
        if bytes.is_empty()
            || total
                .checked_add(bytes.len() as u64)
                .is_none_or(|n| n > self.limits.max_total_bytes)
        {
            return Err(error(Error::Invalid("journal append budget")));
        }
        let result = (|| {
            let mut pending = self.take_segment(id)?;
            if pending.metadata.len() != offset {
                return Err(Error::Conflict);
            }
            pending.file.seek(SeekFrom::Start(offset))?;
            pending.file.write_all(bytes)?;
            let actual = self.metadata(&pending.file)?;
            if actual.len() != length || !same_inode(&actual, &pending.metadata) {
                return Err(Error::Conflict);
            }
            self.pinned(id, &pending.file, &actual)?;
            self.observed.insert(id, actual.clone());
            pending.metadata = actual;
            pending.dirty = true;
            self.pending = Some(pending);
            Ok(())
        })();
        self.failed(result)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        let result = (|| {
            // Recovery truncates the last segment before synchronizing the
            // complete prefix in ascending order. Keep its dirty descriptor
            // while independently synchronizing earlier, already-read files.
            if self.pending.as_ref().is_some_and(|p| p.id != id && p.dirty) {
                let pending = self.load_segment(id)?;
                pending.file.sync_all()?;
                self.pinned(id, &pending.file, &pending.metadata)?;
                self.observed.insert(id, pending.metadata);
                return Ok(());
            }
            let mut pending = self.take_segment(id)?;
            pending.file.sync_all()?;
            self.pinned(id, &pending.file, &pending.metadata)?;
            self.observed.insert(id, pending.metadata.clone());
            pending.dirty = false;
            self.pending = Some(pending);
            Ok(())
        })();
        self.failed(result)
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        let result = (|| {
            let mut pending = self.take_segment(id)?;
            if len > pending.metadata.len() {
                return Err(Error::Invalid("journal truncate extension"));
            }
            pending.file.set_len(len)?;
            let actual = self.metadata(&pending.file)?;
            if actual.len() != len || !same_inode(&actual, &pending.metadata) {
                return Err(Error::Conflict);
            }
            self.pinned(id, &pending.file, &actual)?;
            self.observed.insert(id, actual.clone());
            pending.metadata = actual;
            pending.dirty = true;
            self.pending = Some(pending);
            Ok(())
        })();
        self.failed(result)
    }
}
fn name(id: u64) -> Result<CString> {
    if id == 0 {
        return Err(Error::Invalid("zero segment"));
    }
    CString::new(format!("segment-{id:020}.wal")).map_err(|_| Error::Invalid("segment name"))
}
fn error(value: Error) -> io::Error {
    match value {
        Error::Io(error) => error,
        other => io::Error::other(format!("{other:?}")),
    }
}
