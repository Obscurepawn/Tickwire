//! Linux descriptor-relative file persistence; no business authority.
#[cfg(not(target_os = "linux"))]
use std::fs::File;
use std::io;
mod journal;
pub use journal::{JournalLimits, JournalStore};
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
mod os;
#[cfg(target_os = "linux")]
pub use linux::Store;
pub type Hash = [u8; 32];
pub const MAX_FILE_BYTES: usize = 8 * 1024 * 1024;
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Invalid(&'static str),
    Conflict,
    Poisoned,
    Unsupported,
}
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
pub type Result<T> = core::result::Result<T, Error>;
pub struct DurableFile {
    hash: Hash,
    bytes: usize,
    device: u64,
    inode: u64,
}
impl DurableFile {
    pub fn hash(&self) -> Hash {
        self.hash
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn device(&self) -> u64 {
        self.device
    }
    pub fn inode(&self) -> u64 {
        self.inode
    }
}
#[cfg(not(target_os = "linux"))]
pub struct Store;
#[cfg(not(target_os = "linux"))]
impl Store {
    pub fn open_at(_parent: File, _name: &str, _owner: u32) -> Result<Self> {
        Err(Error::Unsupported)
    }
    pub fn create_directory(&mut self, _path: &str) -> Result<()> {
        Err(Error::Unsupported)
    }
    pub fn read(&self, _path: &str, _max_bytes: usize) -> Result<Vec<u8>> {
        Err(Error::Unsupported)
    }
    pub fn install(
        &mut self,
        _path: &str,
        _bytes: &[u8],
        _expected_previous: Option<Hash>,
    ) -> Result<DurableFile> {
        Err(Error::Unsupported)
    }
    pub fn is_poisoned(&self) -> bool {
        false
    }
}
