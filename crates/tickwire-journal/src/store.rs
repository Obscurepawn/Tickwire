//! File, memory-mapped async, and pure in-memory stores behind the WAL v1 batch protocol.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use tickwire_shm_sys::{MappedCursorPage, MappedSegment, PAGE_BYTES};

pub const DEFAULT_MMAP_PREALLOC_BYTES: usize = 4 * 1024 * 1024;
const CURSOR_FILENAME: &str = "MMAP-CURSOR.bin";

/// Selectable commit persistence profile for journals and recovery stores.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageMode {
    /// Pure RAM store with zero filesystem calls; cold startup bootstraps from exchange REST.
    #[default]
    InMemory,
    /// Preallocated `MAP_SHARED` segments + cursor page + background `fdatasync` thread.
    MmapAsync,
    /// Synchronous `write_all` + `fdatasync` per batch before returning receipts.
    LocalDurable,
}

pub trait Store {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>>;
    /// Synchronize temporary bytes, atomically rename, then synchronize the directory.
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()>;
    fn segment_ids(&mut self) -> io::Result<Vec<u64>>;
    fn read_segment(&mut self, id: u64, max_bytes: u64) -> io::Result<Vec<u8>>;
    /// Create exclusively, synchronize the empty file and its directory entry.
    fn create_segment(&mut self, id: u64) -> io::Result<()>;
    /// Must append exactly at offset; errors may leave a partial write.
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> io::Result<()>;
    /// Complete commit-profile barrier before returning success.
    fn sync_segment(&mut self, id: u64) -> io::Result<()>;
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()>;
}

/// Pure volatile RAM store for stateless cloud Pods (`StorageMode::InMemory`).
#[derive(Debug, Default, Clone)]
pub struct InMemoryStore {
    manifest: Option<Vec<u8>>,
    segments: BTreeMap<u64, Vec<u8>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Store for InMemoryStore {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        Ok(self.manifest.clone())
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.len() > 65_536 {
            return Err(io::Error::other("oversize manifest"));
        }
        self.manifest = Some(bytes.to_vec());
        Ok(())
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        Ok(self.segments.keys().copied().collect())
    }
    fn read_segment(&mut self, id: u64, max_bytes: u64) -> io::Result<Vec<u8>> {
        let seg = self
            .segments
            .get(&id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "segment not found"))?;
        if seg.len() as u64 > max_bytes {
            return Err(io::Error::other("segment capacity exceeded"));
        }
        Ok(seg.clone())
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        if id == 0 {
            return Err(io::Error::other("invalid segment id"));
        }
        if self.segments.contains_key(&id) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "segment already exists",
            ));
        }
        self.segments.insert(id, Vec::new());
        Ok(())
    }
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let seg = self
            .segments
            .get_mut(&id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "segment not found"))?;
        if seg.len() as u64 != offset {
            return Err(io::Error::other("append position changed"));
        }
        seg.extend_from_slice(bytes);
        Ok(())
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        if !self.segments.contains_key(&id) {
            return Err(io::Error::new(io::ErrorKind::NotFound, "segment not found"));
        }
        Ok(())
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        let seg = self
            .segments
            .get_mut(&id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "segment not found"))?;
        let target = usize::try_from(len).map_err(|_| io::Error::other("truncate overflow"))?;
        seg.resize(target, 0);
        Ok(())
    }
}

/// A private directory on a trusted local filesystem; lock is held until drop.
/// Other programs must not bypass this advisory single-writer lock.
pub struct FileStore {
    root: PathBuf,
    _lock: WriterLock,
}
impl FileStore {
    /// The parent directory must already exist.
    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(&root) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists && root.is_dir() => {}
            Err(e) => return Err(e),
        }
        // Keep the lock and all subsequent I/O on the same directory even if
        // the process later changes its working directory.
        let root = fs::canonicalize(&root)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("LOCK"))?;
        lock.try_lock()?;
        let lock = WriterLock(lock);
        lock.0.sync_all()?;
        File::open(&root)?.sync_all()?;
        // An earlier create may have made root visible but failed its parent
        // synchronization. Reopening must finish that publication as well.
        let parent = root
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(parent)?.sync_all()?;
        Ok(Self { root, _lock: lock })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    pub fn segment_path(&self, id: u64) -> PathBuf {
        self.root.join(format!("segment-{id:020}.wal"))
    }
    fn sync_directory(&self) -> io::Result<()> {
        File::open(&self.root)?.sync_all()
    }
}
struct WriterLock(File);
impl Drop for WriterLock {
    fn drop(&mut self) {
        // A concurrently forked child can retain the same open file description
        // until exec. Ownership ends here, not when that unrelated copy closes.
        let _ = self.0.unlock();
    }
}

impl Store for FileStore {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        let file = match File::open(self.root.join("MANIFEST.json")) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if file.metadata()?.len() > 65_536 {
            return Err(io::Error::other("oversize manifest"));
        }
        let mut bytes = Vec::new();
        file.take(65_537).read_to_end(&mut bytes)?;
        if bytes.len() > 65_536 {
            return Err(io::Error::other("oversize manifest"));
        }
        Ok(Some(bytes))
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        let tmp = self.root.join("MANIFEST.tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&tmp, self.root.join("MANIFEST.json"))?;
        self.sync_directory()
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        let mut ids = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("segment-") || name.ends_with(".wal") {
                let id = name
                    .strip_prefix("segment-")
                    .and_then(|v| v.strip_suffix(".wal"))
                    .filter(|v| v.len() == 20 && v.bytes().all(|b| b.is_ascii_digit()))
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .ok_or_else(|| io::Error::other("invalid segment name"))?;
                if !entry.file_type()?.is_file() {
                    return Err(io::Error::other("segment is not a regular file"));
                }
                ids.push(id);
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }
    fn read_segment(&mut self, id: u64, max_bytes: u64) -> io::Result<Vec<u8>> {
        let file = File::open(self.segment_path(id))?;
        if file.metadata()?.len() > max_bytes {
            return Err(io::Error::other("segment capacity exceeded"));
        }
        let mut bytes = Vec::new();
        file.take(max_bytes + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > max_bytes {
            return Err(io::Error::other("segment changed during read"));
        }
        Ok(bytes)
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.segment_path(id))?;
        file.sync_all()?;
        self.sync_directory()
    }
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let mut file = OpenOptions::new().write(true).open(self.segment_path(id))?;
        if file.metadata()?.len() != offset {
            return Err(io::Error::other("append position changed"));
        }
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(bytes)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        OpenOptions::new()
            .write(true)
            .open(self.segment_path(id))?
            .sync_data()
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        OpenOptions::new()
            .write(true)
            .open(self.segment_path(id))?
            .set_len(len)
    }
}

struct FlushShared {
    requested_seq: AtomicU64,
    completed_seq: AtomicU64,
    fd_epoch: AtomicU64,
    parked: AtomicBool,
    shutdown: AtomicBool,
    failed: AtomicBool,
    fds: Mutex<Option<(File, File)>>,
}

/// Preallocated `MAP_SHARED` WAL store with `MMAP-CURSOR.bin` and background `fdatasync`.
pub struct MmapAsyncStore {
    file_store: FileStore,
    prealloc_bytes: usize,
    cursor: Option<MappedCursorPage>,
    active: Option<(u64, MappedSegment, u64)>,
    shared: Arc<FlushShared>,
    flusher: Option<JoinHandle<()>>,
}

impl MmapAsyncStore {
    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_prealloc(root, DEFAULT_MMAP_PREALLOC_BYTES)
    }

    pub fn open_with_prealloc(root: impl AsRef<Path>, prealloc_bytes: usize) -> io::Result<Self> {
        let prealloc_bytes = tickwire_shm_sys::align_page_capacity(prealloc_bytes.max(PAGE_BYTES))
            .map_err(shm_err)?;
        let file_store = FileStore::open(root)?;
        let cursor_path = file_store.root().join(CURSOR_FILENAME);

        if cursor_path.exists() {
            let prior = MappedCursorPage::open_or_create(&cursor_path).map_err(shm_err)?;
            let latest = prior.latest().map_err(shm_err)?;
            drop(prior);
            if let Some(state) = latest {
                let seg_path = file_store.segment_path(state.segment_id);
                if seg_path.exists() {
                    OpenOptions::new()
                        .write(true)
                        .open(&seg_path)?
                        .set_len(state.logical_len)?;
                }
                let next_path = file_store.segment_path(state.segment_id.saturating_add(1));
                if next_path.exists() {
                    let mut head = [0u8; 8];
                    let mut f = File::open(&next_path)?;
                    let n = f.read(&mut head)?;
                    drop(f);
                    if n == 0 || head == [0u8; 8] {
                        OpenOptions::new()
                            .write(true)
                            .open(&next_path)?
                            .set_len(0)?;
                    }
                }
            }
        }

        let cursor = MappedCursorPage::open_or_create(&cursor_path).map_err(shm_err)?;
        let shared = Arc::new(FlushShared {
            requested_seq: AtomicU64::new(0),
            completed_seq: AtomicU64::new(0),
            fd_epoch: AtomicU64::new(0),
            parked: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            fds: Mutex::new(None),
        });
        let worker_shared = Arc::clone(&shared);
        let flusher = thread::Builder::new()
            .name("tw-wal-flush".into())
            .spawn(move || flush_loop(worker_shared))?;

        Ok(Self {
            file_store,
            prealloc_bytes,
            cursor: Some(cursor),
            active: None,
            shared,
            flusher: Some(flusher),
        })
    }

    pub fn root(&self) -> &Path {
        self.file_store.root()
    }

    /// Wait until the background flusher has synchronized all queued batches via `fdatasync`.
    pub fn flush_durable(&self) -> io::Result<u64> {
        let target = self.shared.requested_seq.load(Ordering::Acquire);
        if self.shared.parked.swap(false, Ordering::AcqRel)
            && let Some(h) = &self.flusher
        {
            h.thread().unpark();
        }
        while self.shared.completed_seq.load(Ordering::Acquire) < target {
            if self.shared.failed.load(Ordering::Acquire) {
                return Err(io::Error::other("background fdatasync failed"));
            }
            thread::yield_now();
        }
        if self.shared.failed.load(Ordering::Acquire) {
            return Err(io::Error::other("background fdatasync failed"));
        }
        Ok(target)
    }

    fn close_active(&mut self) -> io::Result<()> {
        if let Some((id, seg, logical_len)) = self.active.take() {
            let _ = self.flush_durable();
            seg.truncate_unmap(logical_len).map_err(shm_err)?;
            OpenOptions::new()
                .write(true)
                .open(self.file_store.segment_path(id))?
                .sync_data()?;
        }
        Ok(())
    }

    fn bind_flusher_fds(&self, seg: &MappedSegment) -> io::Result<()> {
        let cursor = self
            .cursor
            .as_ref()
            .ok_or_else(|| io::Error::other("cursor closed"))?;
        let seg_file = File::from(seg.duplicate_sync_fd().map_err(shm_err)?);
        let cur_file = File::from(cursor.duplicate_sync_fd().map_err(shm_err)?);
        let mut guard = self
            .shared
            .fds
            .lock()
            .map_err(|_| io::Error::other("flush mutex poisoned"))?;
        *guard = Some((seg_file, cur_file));
        self.shared.fd_epoch.fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn ensure_mapped(&mut self, id: u64, min_extra: usize) -> io::Result<()> {
        if self
            .active
            .as_ref()
            .is_some_and(|(active_id, _, _)| *active_id == id)
        {
            return Ok(());
        }
        self.close_active()?;
        let seg_path = self.file_store.segment_path(id);
        let disk_len = fs::metadata(&seg_path)?.len();
        let needed = (disk_len as usize)
            .saturating_add(min_extra)
            .max(self.prealloc_bytes);
        let seg = MappedSegment::open_or_create(&seg_path, needed).map_err(shm_err)?;
        self.bind_flusher_fds(&seg)?;
        self.cursor
            .as_mut()
            .ok_or_else(|| io::Error::other("cursor closed"))?
            .record(id, disk_len)
            .map_err(shm_err)?;
        self.active = Some((id, seg, disk_len));
        Ok(())
    }
}

impl Store for MmapAsyncStore {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.file_store.read_manifest()
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file_store.publish_manifest(bytes)
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        self.file_store.segment_ids()
    }
    fn read_segment(&mut self, id: u64, max_bytes: u64) -> io::Result<Vec<u8>> {
        if let Some((active_id, seg, logical_len)) = &self.active
            && *active_id == id
        {
            if *logical_len > max_bytes {
                return Err(io::Error::other("segment capacity exceeded"));
            }
            return seg.read_to_vec(*logical_len as usize).map_err(shm_err);
        }
        self.file_store.read_segment(id, max_bytes)
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        self.close_active()?;
        self.file_store.create_segment(id)?;
        let seg_path = self.file_store.segment_path(id);
        let seg = MappedSegment::open_or_create(&seg_path, self.prealloc_bytes).map_err(shm_err)?;
        self.bind_flusher_fds(&seg)?;
        self.cursor
            .as_mut()
            .ok_or_else(|| io::Error::other("cursor closed"))?
            .record(id, 0)
            .map_err(shm_err)?;
        self.active = Some((id, seg, 0));
        Ok(())
    }
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.ensure_mapped(id, bytes.len())?;
        let (_, seg, logical_len) = self.active.as_mut().unwrap();
        if *logical_len != offset {
            return Err(io::Error::other("append position changed"));
        }
        let end = usize::try_from(offset)
            .ok()
            .and_then(|o| o.checked_add(bytes.len()))
            .ok_or_else(|| io::Error::other("segment offset overflow"))?;
        if end > seg.capacity() {
            let next_cap = end.max(seg.capacity().saturating_add(self.prealloc_bytes));
            seg.grow(next_cap).map_err(shm_err)?;
        }
        seg.write_at(offset as usize, bytes).map_err(shm_err)?;
        let new_len = end as u64;
        self.cursor
            .as_mut()
            .ok_or_else(|| io::Error::other("cursor closed"))?
            .record(id, new_len)
            .map_err(shm_err)?;
        *logical_len = new_len;
        Ok(())
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        if self.shared.failed.load(Ordering::Acquire) {
            return Err(io::Error::other("background fdatasync failed"));
        }
        if self
            .active
            .as_ref()
            .is_none_or(|(active_id, _, _)| *active_id != id)
        {
            return self.file_store.sync_segment(id);
        }
        self.shared.requested_seq.fetch_add(1, Ordering::Release);
        if self.shared.parked.swap(false, Ordering::AcqRel)
            && let Some(h) = &self.flusher
        {
            h.thread().unpark();
        }
        Ok(())
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        if let Some((active_id, seg, logical_len)) = &mut self.active
            && *active_id == id
        {
            let target = usize::try_from(len).map_err(|_| io::Error::other("truncate overflow"))?;
            if target > seg.capacity() {
                seg.grow(target).map_err(shm_err)?;
            }
            if len > *logical_len {
                let zeros = vec![0u8; target - (*logical_len as usize)];
                seg.write_at(*logical_len as usize, &zeros)
                    .map_err(shm_err)?;
            }
            *logical_len = len;
            self.cursor
                .as_mut()
                .ok_or_else(|| io::Error::other("cursor closed"))?
                .record(id, len)
                .map_err(shm_err)?;
            return Ok(());
        }
        self.file_store.truncate_segment(id, len)
    }
}

impl Drop for MmapAsyncStore {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::Release);
        self.shared.parked.store(false, Ordering::Release);
        if let Some(h) = self.flusher.take() {
            h.thread().unpark();
            let _ = h.join();
        }
        if let Some((id, seg, logical_len)) = self.active.take() {
            let _ = seg.truncate_unmap(logical_len);
            if let Ok(f) = OpenOptions::new()
                .write(true)
                .open(self.file_store.segment_path(id))
            {
                let _ = f.sync_data();
            }
        }
        drop(self.cursor.take());
        let _ = fs::remove_file(self.file_store.root().join(CURSOR_FILENAME));
        let _ = self.file_store.sync_directory();
    }
}

fn flush_loop(shared: Arc<FlushShared>) {
    let mut completed = 0u64;
    let mut local_epoch = 0u64;
    let mut local_fds: Option<(File, File)> = None;
    loop {
        let target = shared.requested_seq.load(Ordering::Acquire);
        if target > completed {
            let epoch = shared.fd_epoch.load(Ordering::Acquire);
            if (epoch != local_epoch || local_fds.is_none())
                && let Ok(guard) = shared.fds.lock()
                && let Some((seg, cur)) = guard.as_ref()
                && let (Ok(s), Ok(c)) = (seg.try_clone(), cur.try_clone())
            {
                local_fds = Some((s, c));
                local_epoch = epoch;
            }
            if let Some((seg, cur)) = &local_fds
                && (seg.sync_data().is_err() || cur.sync_data().is_err())
            {
                shared.failed.store(true, Ordering::Release);
                return;
            }
            completed = target;
            shared.completed_seq.store(completed, Ordering::Release);
            continue;
        }
        if shared.shutdown.load(Ordering::Acquire) {
            break;
        }
        shared.parked.store(true, Ordering::Release);
        if shared.requested_seq.load(Ordering::Acquire) == completed
            && !shared.shutdown.load(Ordering::Acquire)
        {
            thread::park();
        }
        shared.parked.store(false, Ordering::Release);
    }
}

fn shm_err(err: tickwire_shm_sys::Error) -> io::Error {
    io::Error::other(err.to_string())
}

/// Polymorphic runtime `Store` covering `InMemory`, `MmapAsync`, and `LocalDurable`.
pub enum JournalStore {
    InMemory(InMemoryStore),
    MmapAsync(MmapAsyncStore),
    LocalDurable(FileStore),
}

impl JournalStore {
    pub fn open(mode: StorageMode, root: Option<&Path>) -> io::Result<Self> {
        match mode {
            StorageMode::InMemory => Ok(Self::InMemory(InMemoryStore::new())),
            StorageMode::MmapAsync => {
                let path = root.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "MmapAsync requires path")
                })?;
                Ok(Self::MmapAsync(MmapAsyncStore::open(path)?))
            }
            StorageMode::LocalDurable => {
                let path = root.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "LocalDurable requires path")
                })?;
                Ok(Self::LocalDurable(FileStore::open(path)?))
            }
        }
    }

    pub fn flush_durable(&self) -> io::Result<()> {
        match self {
            Self::InMemory(_) | Self::LocalDurable(_) => Ok(()),
            Self::MmapAsync(s) => s.flush_durable().map(|_| ()),
        }
    }
}

impl Store for JournalStore {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::InMemory(s) => s.read_manifest(),
            Self::MmapAsync(s) => s.read_manifest(),
            Self::LocalDurable(s) => s.read_manifest(),
        }
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            Self::InMemory(s) => s.publish_manifest(bytes),
            Self::MmapAsync(s) => s.publish_manifest(bytes),
            Self::LocalDurable(s) => s.publish_manifest(bytes),
        }
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        match self {
            Self::InMemory(s) => s.segment_ids(),
            Self::MmapAsync(s) => s.segment_ids(),
            Self::LocalDurable(s) => s.segment_ids(),
        }
    }
    fn read_segment(&mut self, id: u64, max_bytes: u64) -> io::Result<Vec<u8>> {
        match self {
            Self::InMemory(s) => s.read_segment(id, max_bytes),
            Self::MmapAsync(s) => s.read_segment(id, max_bytes),
            Self::LocalDurable(s) => s.read_segment(id, max_bytes),
        }
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        match self {
            Self::InMemory(s) => s.create_segment(id),
            Self::MmapAsync(s) => s.create_segment(id),
            Self::LocalDurable(s) => s.create_segment(id),
        }
    }
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        match self {
            Self::InMemory(s) => s.append_segment(id, offset, bytes),
            Self::MmapAsync(s) => s.append_segment(id, offset, bytes),
            Self::LocalDurable(s) => s.append_segment(id, offset, bytes),
        }
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        match self {
            Self::InMemory(s) => s.sync_segment(id),
            Self::MmapAsync(s) => s.sync_segment(id),
            Self::LocalDurable(s) => s.sync_segment(id),
        }
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        match self {
            Self::InMemory(s) => s.truncate_segment(id, len),
            Self::MmapAsync(s) => s.truncate_segment(id, len),
            Self::LocalDurable(s) => s.truncate_segment(id, len),
        }
    }
}

#[cfg(test)]
mod lock_owner_tests {
    use super::*;

    #[test]
    fn owner_drop_releases_lock_even_while_an_inherited_description_still_exists() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "tickwire-lock-owner-{}-{unique}",
            std::process::id()
        ));
        let owner = FileStore::open(&root).unwrap();
        // fork duplicates the same open file description. A safe duplicate
        // isolates the ownership rule without adding a process timing race.
        let inherited = owner._lock.0.try_clone().unwrap();
        assert!(FileStore::open(&root).is_err(), "live owner is exclusive");
        drop(owner);
        let replacement = FileStore::open(&root).expect("the original owner has ended");
        assert!(FileStore::open(&root).is_err(), "replacement owns the lock");
        drop(inherited);
        assert!(
            FileStore::open(&root).is_err(),
            "old copy cannot unlock replacement"
        );
        drop(replacement);
        drop(FileStore::open(&root).unwrap());
        fs::remove_dir_all(root).unwrap();
    }
}
