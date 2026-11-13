use crate::Backend;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
use tickwire_account_codec::MAX_BASELINE_BYTES;
use tickwire_journal::{
    StorageMode,
    store::{FileStore, InMemoryStore, MmapAsyncStore, Store},
};
const BOOTSTRAP_LIMIT: usize = 65_536;

/// Pure volatile RAM backend (`StorageMode::InMemory`, default for stateless cloud Pods).
#[derive(Debug, Default, Clone)]
pub struct InMemoryBackend {
    physical: InMemoryStore,
    bootstrap: Option<Vec<u8>>,
    baseline: Option<Vec<u8>>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Backend for InMemoryBackend {
    fn read_bootstrap(&mut self) -> io::Result<Option<Vec<u8>>> {
        Ok(self.bootstrap.clone())
    }
    fn publish_bootstrap(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() || bytes.len() > BOOTSTRAP_LIMIT {
            return Err(io::Error::other("account file capacity"));
        }
        if self.bootstrap.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "account bootstrap already exists",
            ));
        }
        self.bootstrap = Some(bytes.to_vec());
        Ok(())
    }
    fn read_baseline(&mut self) -> io::Result<Option<Vec<u8>>> {
        Ok(self.baseline.clone())
    }
    fn publish_baseline(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() || bytes.len() > MAX_BASELINE_BYTES {
            return Err(io::Error::other("account file capacity"));
        }
        if self.baseline.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "account baseline already exists",
            ));
        }
        self.baseline = Some(bytes.to_vec());
        Ok(())
    }
}

impl Store for InMemoryBackend {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.physical.read_manifest()
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.physical.publish_manifest(bytes)
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        self.physical.segment_ids()
    }
    fn read_segment(&mut self, id: u64, max: u64) -> io::Result<Vec<u8>> {
        self.physical.read_segment(id, max)
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        self.physical.create_segment(id)
    }
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.physical.append_segment(id, offset, bytes)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        self.physical.sync_segment(id)
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.physical.truncate_segment(id, len)
    }
}

fn read_bounded_file(root: &Path, name: &str, max: usize) -> io::Result<Option<Vec<u8>>> {
    let file = match File::open(root.join(name)) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() > max as u64 {
        return Err(io::Error::other("oversize account file"));
    }
    let mut bytes = Vec::new();
    file.take(max as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err(io::Error::other("account file grew beyond limit"));
    }
    Ok(Some(bytes))
}

fn publish_immutable_file(root: &Path, name: &str, bytes: &[u8], max: usize) -> io::Result<()> {
    if bytes.is_empty() || bytes.len() > max {
        return Err(io::Error::other("account file capacity"));
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(root.join(name))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    File::open(root)?.sync_all()
}

/// Private trusted directory; the physical store's single-writer lock covers all files.
pub struct FileBackend {
    physical: FileStore,
    root: PathBuf,
}
impl FileBackend {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let physical = FileStore::open(path.as_ref())?;
        let root = fs::canonicalize(path)?;
        Ok(Self { physical, root })
    }
}
impl Backend for FileBackend {
    fn read_bootstrap(&mut self) -> io::Result<Option<Vec<u8>>> {
        read_bounded_file(&self.root, "ACCOUNT-BOOTSTRAP.json", BOOTSTRAP_LIMIT)
    }
    fn publish_bootstrap(&mut self, bytes: &[u8]) -> io::Result<()> {
        publish_immutable_file(&self.root, "ACCOUNT-BOOTSTRAP.json", bytes, BOOTSTRAP_LIMIT)
    }
    fn read_baseline(&mut self) -> io::Result<Option<Vec<u8>>> {
        read_bounded_file(&self.root, "ACCOUNT-BASELINE.cbor", MAX_BASELINE_BYTES)
    }
    fn publish_baseline(&mut self, bytes: &[u8]) -> io::Result<()> {
        publish_immutable_file(
            &self.root,
            "ACCOUNT-BASELINE.cbor",
            bytes,
            MAX_BASELINE_BYTES,
        )
    }
}
impl Store for FileBackend {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.physical.read_manifest()
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.physical.publish_manifest(bytes)
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        self.physical.segment_ids()
    }
    fn read_segment(&mut self, id: u64, max: u64) -> io::Result<Vec<u8>> {
        self.physical.read_segment(id, max)
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        self.physical.create_segment(id)
    }
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.physical.append_segment(id, offset, bytes)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        self.physical.sync_segment(id)
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.physical.truncate_segment(id, len)
    }
}

/// Preallocated `MAP_SHARED` account backend with background `fdatasync` (`StorageMode::MmapAsync`).
pub struct MmapAsyncBackend {
    physical: MmapAsyncStore,
    root: PathBuf,
}
impl MmapAsyncBackend {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let physical = MmapAsyncStore::open(path.as_ref())?;
        let root = physical.root().to_path_buf();
        Ok(Self { physical, root })
    }
    pub fn flush_durable(&self) -> io::Result<()> {
        self.physical.flush_durable().map(|_| ())
    }
}
impl Backend for MmapAsyncBackend {
    fn read_bootstrap(&mut self) -> io::Result<Option<Vec<u8>>> {
        read_bounded_file(&self.root, "ACCOUNT-BOOTSTRAP.json", BOOTSTRAP_LIMIT)
    }
    fn publish_bootstrap(&mut self, bytes: &[u8]) -> io::Result<()> {
        publish_immutable_file(&self.root, "ACCOUNT-BOOTSTRAP.json", bytes, BOOTSTRAP_LIMIT)
    }
    fn read_baseline(&mut self) -> io::Result<Option<Vec<u8>>> {
        read_bounded_file(&self.root, "ACCOUNT-BASELINE.cbor", MAX_BASELINE_BYTES)
    }
    fn publish_baseline(&mut self, bytes: &[u8]) -> io::Result<()> {
        publish_immutable_file(
            &self.root,
            "ACCOUNT-BASELINE.cbor",
            bytes,
            MAX_BASELINE_BYTES,
        )
    }
}
impl Store for MmapAsyncBackend {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.physical.read_manifest()
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.physical.publish_manifest(bytes)
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        self.physical.segment_ids()
    }
    fn read_segment(&mut self, id: u64, max: u64) -> io::Result<Vec<u8>> {
        self.physical.read_segment(id, max)
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        self.physical.create_segment(id)
    }
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.physical.append_segment(id, offset, bytes)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        self.physical.sync_segment(id)
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.physical.truncate_segment(id, len)
    }
}

/// Polymorphic account backend covering `InMemory` (default), `MmapAsync`, and `LocalDurable`.
pub enum AccountBackend {
    InMemory(InMemoryBackend),
    MmapAsync(MmapAsyncBackend),
    LocalDurable(FileBackend),
}

impl AccountBackend {
    pub fn open(mode: StorageMode, root: Option<&Path>) -> io::Result<Self> {
        match mode {
            StorageMode::InMemory => Ok(Self::InMemory(InMemoryBackend::new())),
            StorageMode::MmapAsync => {
                let p = root.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "MmapAsync requires path")
                })?;
                Ok(Self::MmapAsync(MmapAsyncBackend::open(p)?))
            }
            StorageMode::LocalDurable => {
                let p = root.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "LocalDurable requires path")
                })?;
                Ok(Self::LocalDurable(FileBackend::open(p)?))
            }
        }
    }

    pub fn flush_durable(&self) -> io::Result<()> {
        match self {
            Self::InMemory(_) | Self::LocalDurable(_) => Ok(()),
            Self::MmapAsync(b) => b.flush_durable(),
        }
    }
}

impl Backend for AccountBackend {
    fn read_bootstrap(&mut self) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::InMemory(b) => b.read_bootstrap(),
            Self::MmapAsync(b) => b.read_bootstrap(),
            Self::LocalDurable(b) => b.read_bootstrap(),
        }
    }
    fn publish_bootstrap(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            Self::InMemory(b) => b.publish_bootstrap(bytes),
            Self::MmapAsync(b) => b.publish_bootstrap(bytes),
            Self::LocalDurable(b) => b.publish_bootstrap(bytes),
        }
    }
    fn read_baseline(&mut self) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::InMemory(b) => b.read_baseline(),
            Self::MmapAsync(b) => b.read_baseline(),
            Self::LocalDurable(b) => b.read_baseline(),
        }
    }
    fn publish_baseline(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            Self::InMemory(b) => b.publish_baseline(bytes),
            Self::MmapAsync(b) => b.publish_baseline(bytes),
            Self::LocalDurable(b) => b.publish_baseline(bytes),
        }
    }
}

impl Store for AccountBackend {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::InMemory(b) => b.read_manifest(),
            Self::MmapAsync(b) => b.read_manifest(),
            Self::LocalDurable(b) => b.read_manifest(),
        }
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            Self::InMemory(b) => b.publish_manifest(bytes),
            Self::MmapAsync(b) => b.publish_manifest(bytes),
            Self::LocalDurable(b) => b.publish_manifest(bytes),
        }
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        match self {
            Self::InMemory(b) => b.segment_ids(),
            Self::MmapAsync(b) => b.segment_ids(),
            Self::LocalDurable(b) => b.segment_ids(),
        }
    }
    fn read_segment(&mut self, id: u64, max: u64) -> io::Result<Vec<u8>> {
        match self {
            Self::InMemory(b) => b.read_segment(id, max),
            Self::MmapAsync(b) => b.read_segment(id, max),
            Self::LocalDurable(b) => b.read_segment(id, max),
        }
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        match self {
            Self::InMemory(b) => b.create_segment(id),
            Self::MmapAsync(b) => b.create_segment(id),
            Self::LocalDurable(b) => b.create_segment(id),
        }
    }
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        match self {
            Self::InMemory(b) => b.append_segment(id, offset, bytes),
            Self::MmapAsync(b) => b.append_segment(id, offset, bytes),
            Self::LocalDurable(b) => b.append_segment(id, offset, bytes),
        }
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        match self {
            Self::InMemory(b) => b.sync_segment(id),
            Self::MmapAsync(b) => b.sync_segment(id),
            Self::LocalDurable(b) => b.sync_segment(id),
        }
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        match self {
            Self::InMemory(b) => b.truncate_segment(id, len),
            Self::MmapAsync(b) => b.truncate_segment(id, len),
            Self::LocalDurable(b) => b.truncate_segment(id, len),
        }
    }
}
