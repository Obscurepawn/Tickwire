use crate::Backend;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
use tickwire_account_codec::MAX_BASELINE_BYTES;
use tickwire_journal::store::{FileStore, Store};
const BOOTSTRAP_LIMIT: usize = 65_536;

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
    fn read(&self, name: &str, max: usize) -> io::Result<Option<Vec<u8>>> {
        let file = match File::open(self.root.join(name)) {
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
    fn publish(&self, name: &str, bytes: &[u8], max: usize) -> io::Result<()> {
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
        let mut file = options.open(self.root.join(name))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        File::open(&self.root)?.sync_all()
    }
}
impl Backend for FileBackend {
    fn read_bootstrap(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.read("ACCOUNT-BOOTSTRAP.json", BOOTSTRAP_LIMIT)
    }
    fn publish_bootstrap(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.publish("ACCOUNT-BOOTSTRAP.json", bytes, BOOTSTRAP_LIMIT)
    }
    fn read_baseline(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.read("ACCOUNT-BASELINE.cbor", MAX_BASELINE_BYTES)
    }
    fn publish_baseline(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.publish("ACCOUNT-BASELINE.cbor", bytes, MAX_BASELINE_BYTES)
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
