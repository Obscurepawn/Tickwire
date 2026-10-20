use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::{
    io,
    path::{Path, PathBuf},
};
use tickwire_journal::store::{FileStore, Store};

/// Trusted backend. Bootstrap publication is immutable and synchronizes both
/// file contents and the directory; all Store sync guarantees also apply.
pub trait Backend: Store {
    fn read_bootstrap(&mut self) -> io::Result<Option<Vec<u8>>>;
    fn publish_bootstrap(&mut self, bytes: &[u8]) -> io::Result<()>;
}
pub struct FileBackend {
    physical: FileStore,
    root: PathBuf,
}
impl FileBackend {
    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        let physical = FileStore::open(&root)?;
        let root = fs::canonicalize(root)?;
        Ok(Self { physical, root })
    }
}
impl Store for FileBackend {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.physical.read_manifest()
    }
    fn publish_manifest(&mut self, b: &[u8]) -> io::Result<()> {
        self.physical.publish_manifest(b)
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
    fn append_segment(&mut self, id: u64, offset: u64, b: &[u8]) -> io::Result<()> {
        self.physical.append_segment(id, offset, b)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        self.physical.sync_segment(id)
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.physical.truncate_segment(id, len)
    }
}
impl Backend for FileBackend {
    fn read_bootstrap(&mut self) -> io::Result<Option<Vec<u8>>> {
        let file = match File::open(self.root.join("BUSINESS.json")) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let mut bytes = Vec::new();
        file.take(65_537).read_to_end(&mut bytes)?;
        if bytes.len() > 65_536 {
            return Err(io::Error::other("business bootstrap too large"));
        }
        Ok(Some(bytes))
    }
    fn publish_bootstrap(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.len() > 65_536 {
            return Err(io::Error::other("business bootstrap too large"));
        }
        let target = self.root.join("BUSINESS.json");
        if target.try_exists()? {
            return Err(io::Error::other("business bootstrap already exists"));
        }
        let temp = self.root.join("BUSINESS.tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(temp, target)?;
        File::open(&self.root)?.sync_all()
    }
}
