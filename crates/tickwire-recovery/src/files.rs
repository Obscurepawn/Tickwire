use crate::*;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
};

const MAX_ASYNC_CACHE_BYTES: usize = 128 * 1024 * 1024;

fn key_limit(key: StoreKey) -> usize {
    match key {
        StoreKey::Object(_) => MAX_OBJECT_BYTES,
        StoreKey::Manifest(_) => MAX_MANIFEST_BYTES,
        StoreKey::Binding(_) => 32,
    }
}

/// Pure volatile RAM blob store (`StorageMode::InMemory`, zero filesystem I/O).
#[derive(Debug, Default, Clone)]
pub struct InMemoryBlobStore {
    entries: BTreeMap<StoreKey, Arc<[u8]>>,
}

impl InMemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl BlobStore for InMemoryBlobStore {
    fn get(&self, key: StoreKey, max_bytes: usize) -> Result<Vec<u8>> {
        let bytes = self.entries.get(&key).ok_or(Error::Missing)?;
        if bytes.len() > max_bytes {
            return Err(Error::Capacity);
        }
        Ok(bytes.as_ref().to_vec())
    }

    fn put_sync(&mut self, key: StoreKey, bytes: &[u8]) -> Result<()> {
        let limit = key_limit(key);
        if bytes.is_empty() || bytes.len() > limit {
            return Err(Error::Capacity);
        }
        if let Some(existing) = self.entries.get(&key) {
            if existing.as_ref() != bytes {
                return Err(Error::Conflict);
            }
            return Ok(());
        }
        self.entries.insert(key, Arc::from(bytes));
        Ok(())
    }
}

/// Read-only access to a trusted writer's private object directory. This reader
/// neither locks the writer nor synchronizes files nor creates recovery authority.
pub struct FileBlobReader {
    root: PathBuf,
    owner: u32,
}
impl FileBlobReader {
    pub fn open(root: impl AsRef<Path>, owner: u32) -> Result<Self> {
        let root = root.as_ref();
        Self::directory(root, owner)?;
        Ok(Self {
            root: fs::canonicalize(root)?,
            owner,
        })
    }
    fn directory(root: &Path, owner: u32) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let m = fs::symlink_metadata(root)?;
            if !m.file_type().is_dir() || m.uid() != owner || m.mode() & 0o027 != 0 {
                return Err(Error::Invalid(
                    "read-only object directory owner/permissions",
                ));
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            let _ = (root, owner);
            Err(Error::Invalid("unsupported object directory platform"))
        }
    }
    pub fn get_object(&self, digest: Hash, limit: usize) -> Result<Vec<u8>> {
        if digest == [0; 32] || limit == 0 || limit > MAX_OBJECT_BYTES {
            return Err(Error::Capacity);
        }
        Self::directory(&self.root, self.owner)?;
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
        }
        let file = options.open(self.root.join(format!("object-{hex}")))?;
        let meta = file.metadata()?;
        if !meta.file_type().is_file() || meta.len() > limit as u64 {
            return Err(Error::Capacity);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if meta.uid() != self.owner || meta.mode() & 0o027 != 0 {
                return Err(Error::Invalid("read-only object file owner/permissions"));
            }
        }
        let mut bytes = Vec::new();
        file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err(Error::Capacity);
        }
        if <[u8; 32]>::from(Sha256::digest(&bytes)) != digest {
            return Err(Error::Conflict);
        }
        Ok(bytes)
    }
}

fn open_locked_directory(root: impl AsRef<Path>) -> Result<(PathBuf, WriterLock)> {
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
    let lock = WriterLock(lock);
    lock.0.sync_all()?;
    File::open(&root)?.sync_all()?;
    // Also covers retry after a prior mkdir succeeded but parent sync failed.
    File::open(
        root.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    Ok((root, lock))
}

fn store_key_path(root: &Path, key: StoreKey) -> PathBuf {
    let (tag, hash) = match key {
        StoreKey::Object(h) => ("object", h),
        StoreKey::Manifest(h) => ("manifest", h),
        StoreKey::Binding(h) => ("binding", h),
    };
    let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
    root.join(format!("{tag}-{hex}"))
}

fn parse_store_key_filename(name: &str) -> Option<StoreKey> {
    let (tag, hex) = name.split_once('-')?;
    if hex.len() != 64 {
        return None;
    }
    let mut hash = [0u8; 32];
    for (i, slot) in hash.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok()?;
    }
    match tag {
        "object" => Some(StoreKey::Object(hash)),
        "manifest" => Some(StoreKey::Manifest(hash)),
        "binding" => Some(StoreKey::Binding(hash)),
        _ => None,
    }
}

/// Exclusive, trusted private directory; no deletion API is exposed.
pub struct FileBlobStore {
    root: PathBuf,
    _lock: WriterLock,
    counter: u64,
}
impl FileBlobStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let (root, lock) = open_locked_directory(root)?;
        Ok(Self {
            root,
            _lock: lock,
            counter: 0,
        })
    }
    fn path(&self, key: StoreKey) -> PathBuf {
        store_key_path(&self.root, key)
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
        let limit = key_limit(key);
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

struct AsyncBlobFlushQueue {
    pending: VecDeque<PathBuf>,
    in_flight: usize,
    shutdown: bool,
    failed: bool,
}

/// Page-cache atomic rename blob store with RAM deduplication index and background `fdatasync`.
pub struct AsyncFileBlobStore {
    root: PathBuf,
    _lock: WriterLock,
    counter: u64,
    on_disk: BTreeSet<StoreKey>,
    cache: BTreeMap<StoreKey, Arc<[u8]>>,
    cached_bytes: usize,
    flusher_state: Arc<(Mutex<AsyncBlobFlushQueue>, Condvar)>,
    flusher: Option<JoinHandle<()>>,
}

impl AsyncFileBlobStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let (root, lock) = open_locked_directory(root)?;
        let mut on_disk = BTreeSet::new();
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && let Some(key) = parse_store_key_filename(&entry.file_name().to_string_lossy())
            {
                on_disk.insert(key);
            }
        }
        let flusher_state = Arc::new((
            Mutex::new(AsyncBlobFlushQueue {
                pending: VecDeque::new(),
                in_flight: 0,
                shutdown: false,
                failed: false,
            }),
            Condvar::new(),
        ));
        let worker_state = Arc::clone(&flusher_state);
        let dir_for_worker = root.clone();
        let flusher = thread::Builder::new()
            .name("tw-blob-flush".into())
            .spawn(move || blob_flush_loop(dir_for_worker, worker_state))?;

        Ok(Self {
            root,
            _lock: lock,
            counter: 0,
            on_disk,
            cache: BTreeMap::new(),
            cached_bytes: 0,
            flusher_state,
            flusher: Some(flusher),
        })
    }

    pub fn flush_durable(&self) -> Result<()> {
        let (lock, cvar) = &*self.flusher_state;
        let mut guard = lock
            .lock()
            .map_err(|_| io::Error::other("blob flush poisoned"))?;
        while !guard.pending.is_empty() || guard.in_flight > 0 {
            if guard.failed {
                return Err(io::Error::other("background blob sync failed").into());
            }
            guard = cvar
                .wait(guard)
                .map_err(|_| io::Error::other("blob flush poisoned"))?;
        }
        if guard.failed {
            return Err(io::Error::other("background blob sync failed").into());
        }
        Ok(())
    }

    fn cache_insert(&mut self, key: StoreKey, bytes: &[u8]) {
        if self.cache.contains_key(&key) {
            return;
        }
        if self.cached_bytes.saturating_add(bytes.len()) <= MAX_ASYNC_CACHE_BYTES {
            self.cached_bytes += bytes.len();
            self.cache.insert(key, Arc::from(bytes));
        }
    }

    fn read_disk_key(&self, key: StoreKey, max_bytes: usize) -> Result<Vec<u8>> {
        let path = store_key_path(&self.root, key);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(Error::Missing),
            Err(e) => return Err(e.into()),
        };
        if !metadata.file_type().is_file() {
            return Err(Error::Invalid("blob file type"));
        }
        if metadata.len() > max_bytes as u64 {
            return Err(Error::Capacity);
        }
        let mut out = Vec::new();
        File::open(path)?
            .take((max_bytes as u64).saturating_add(1))
            .read_to_end(&mut out)?;
        if out.len() > max_bytes {
            return Err(Error::Capacity);
        }
        Ok(out)
    }
}

impl BlobStore for AsyncFileBlobStore {
    fn get(&self, key: StoreKey, max_bytes: usize) -> Result<Vec<u8>> {
        if let Some(cached) = self.cache.get(&key) {
            if cached.len() > max_bytes {
                return Err(Error::Capacity);
            }
            return Ok(cached.as_ref().to_vec());
        }
        self.read_disk_key(key, max_bytes)
    }

    fn put_sync(&mut self, key: StoreKey, bytes: &[u8]) -> Result<()> {
        let limit = key_limit(key);
        if bytes.is_empty() || bytes.len() > limit {
            return Err(Error::Capacity);
        }
        if let Some(cached) = self.cache.get(&key) {
            if cached.as_ref() != bytes {
                return Err(Error::Conflict);
            }
            return Ok(());
        }
        if self.on_disk.contains(&key) {
            let prior = self.read_disk_key(key, limit)?;
            if prior != bytes {
                return Err(Error::Conflict);
            }
            self.cache_insert(key, bytes);
            return Ok(());
        }

        let (temp_path, mut file) = loop {
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

        let target_path = store_key_path(&self.root, key);
        let write_res = (|| -> Result<()> {
            file.write_all(bytes)?;
            drop(file);
            fs::rename(&temp_path, &target_path)?;
            Ok(())
        })();
        if let Err(err) = write_res {
            let _ = fs::remove_file(&temp_path);
            return Err(err);
        }

        self.on_disk.insert(key);
        self.cache_insert(key, bytes);

        let (lock, cvar) = &*self.flusher_state;
        let mut guard = lock
            .lock()
            .map_err(|_| io::Error::other("blob flush poisoned"))?;
        if guard.failed {
            return Err(io::Error::other("background blob sync failed").into());
        }
        guard.pending.push_back(target_path);
        cvar.notify_one();
        Ok(())
    }
}

impl Drop for AsyncFileBlobStore {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.flusher_state;
        if let Ok(mut guard) = lock.lock() {
            guard.shutdown = true;
            cvar.notify_all();
        }
        if let Some(h) = self.flusher.take() {
            let _ = h.join();
        }
    }
}

fn blob_flush_loop(root: PathBuf, state: Arc<(Mutex<AsyncBlobFlushQueue>, Condvar)>) {
    let (lock, cvar) = &*state;
    loop {
        let batch = {
            let mut guard = match lock.lock() {
                Ok(g) => g,
                Err(_) => return,
            };
            while guard.pending.is_empty() && !guard.shutdown {
                guard = match cvar.wait(guard) {
                    Ok(g) => g,
                    Err(_) => return,
                };
            }
            if guard.pending.is_empty() && guard.shutdown {
                cvar.notify_all();
                return;
            }
            let drained: Vec<PathBuf> = guard.pending.drain(..).collect();
            guard.in_flight = drained.len();
            drained
        };

        let mut ok = true;
        for path in batch {
            if OpenOptions::new()
                .read(true)
                .open(&path)
                .and_then(|f| f.sync_data())
                .is_err()
            {
                ok = false;
                break;
            }
        }
        if ok && File::open(&root).and_then(|d| d.sync_all()).is_err() {
            ok = false;
        }

        if let Ok(mut guard) = lock.lock() {
            guard.in_flight = 0;
            if !ok {
                guard.failed = true;
                cvar.notify_all();
                return;
            }
            cvar.notify_all();
        }
    }
}

/// Polymorphic blob store matching `InMemory`, `MmapAsync`, and `LocalDurable` profiles.
pub enum RuntimeBlobStore {
    InMemory(InMemoryBlobStore),
    MmapAsync(AsyncFileBlobStore),
    LocalDurable(FileBlobStore),
}

impl RuntimeBlobStore {
    pub fn in_memory() -> Self {
        Self::InMemory(InMemoryBlobStore::new())
    }
    pub fn mmap_async(root: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::MmapAsync(AsyncFileBlobStore::open(root)?))
    }
    pub fn local_durable(root: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::LocalDurable(FileBlobStore::open(root)?))
    }
    pub fn flush_durable(&self) -> Result<()> {
        match self {
            Self::InMemory(_) | Self::LocalDurable(_) => Ok(()),
            Self::MmapAsync(s) => s.flush_durable(),
        }
    }
}

impl BlobStore for RuntimeBlobStore {
    fn get(&self, key: StoreKey, max_bytes: usize) -> Result<Vec<u8>> {
        match self {
            Self::InMemory(s) => s.get(key, max_bytes),
            Self::MmapAsync(s) => s.get(key, max_bytes),
            Self::LocalDurable(s) => s.get(key, max_bytes),
        }
    }
    fn put_sync(&mut self, key: StoreKey, bytes: &[u8]) -> Result<()> {
        match self {
            Self::InMemory(s) => s.put_sync(key, bytes),
            Self::MmapAsync(s) => s.put_sync(key, bytes),
            Self::LocalDurable(s) => s.put_sync(key, bytes),
        }
    }
}

struct WriterLock(File);
impl Drop for WriterLock {
    fn drop(&mut self) {
        // Child launch may temporarily inherit this open file description.
        // Only this store owns the writer lock, not the inherited copy.
        let _ = self.0.unlock();
    }
}

#[cfg(test)]
mod lock_owner_tests {
    use super::*;

    #[test]
    fn owner_drop_releases_lock_while_inherited_description_remains() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "tickwire-blob-lock-{}-{unique}",
            std::process::id()
        ));
        let owner = FileBlobStore::open(&root).unwrap();
        let inherited = owner._lock.0.try_clone().unwrap();
        assert!(
            FileBlobStore::open(&root).is_err(),
            "live owner is exclusive"
        );
        drop(owner);
        let replacement = FileBlobStore::open(&root).expect("original owner ended");
        drop(inherited);
        assert!(
            FileBlobStore::open(&root).is_err(),
            "old copy cannot unlock replacement"
        );
        drop(replacement);
        drop(FileBlobStore::open(&root).unwrap());
        fs::remove_dir_all(root).unwrap();
    }
}
