//! File operations behind the batch protocol. Implementations must honor sync semantics.
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

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
    /// Complete fdatasync-equivalent before returning success.
    fn sync_segment(&mut self, id: u64) -> io::Result<()>;
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()>;
}

/// A private directory on a trusted local filesystem; lock is held until drop.
/// Other programs must not bypass this advisory single-writer lock.
pub struct FileStore {
    root: PathBuf,
    _lock: File,
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
            Ok(()) => {
                File::open(root.parent().unwrap_or(Path::new(".")))?.sync_all()?;
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists && root.is_dir() => {}
            Err(e) => return Err(e),
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("LOCK"))?;
        lock.try_lock()
            .map_err(|e| io::Error::other(e.to_string()))?;
        lock.sync_all()?;
        File::open(&root)?.sync_all()?;
        Ok(Self { root, _lock: lock })
    }
    pub fn segment_path(&self, id: u64) -> PathBuf {
        self.root.join(format!("segment-{id:020}.wal"))
    }
    fn sync_directory(&self) -> io::Result<()> {
        File::open(&self.root)?.sync_all()
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
