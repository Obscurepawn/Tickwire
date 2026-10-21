//! Descriptor-relative backend for the existing physical journal protocol.
#[cfg(not(target_os = "linux"))]
use crate::{Result, Store};
#[cfg(not(target_os = "linux"))]
use std::io;
#[derive(Debug, Clone, Copy)]
pub struct JournalLimits {
    pub max_segment_bytes: u64,
    pub max_segments: usize,
    pub max_total_bytes: u64,
}
impl Default for JournalLimits {
    fn default() -> Self {
        Self {
            max_segment_bytes: 16 * 1024 * 1024,
            max_segments: 128,
            max_total_bytes: 256 * 1024 * 1024,
        }
    }
}
#[cfg(target_os = "linux")]
pub use crate::linux::journal::JournalStore;
#[cfg(not(target_os = "linux"))]
pub struct JournalStore;
#[cfg(not(target_os = "linux"))]
impl JournalStore {
    pub fn new(_store: Store, _limits: JournalLimits) -> Result<Self> {
        Err(crate::Error::Unsupported)
    }
}
#[cfg(not(target_os = "linux"))]
impl tickwire_journal::store::Store for JournalStore {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        Err(io::Error::other("not implemented"))
    }
    fn publish_manifest(&mut self, _bytes: &[u8]) -> io::Result<()> {
        Err(io::Error::other("not implemented"))
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        Err(io::Error::other("not implemented"))
    }
    fn read_segment(&mut self, _id: u64, _max_bytes: u64) -> io::Result<Vec<u8>> {
        Err(io::Error::other("not implemented"))
    }
    fn create_segment(&mut self, _id: u64) -> io::Result<()> {
        Err(io::Error::other("not implemented"))
    }
    fn append_segment(&mut self, _id: u64, _offset: u64, _bytes: &[u8]) -> io::Result<()> {
        Err(io::Error::other("not implemented"))
    }
    fn sync_segment(&mut self, _id: u64) -> io::Result<()> {
        Err(io::Error::other("not implemented"))
    }
    fn truncate_segment(&mut self, _id: u64, _len: u64) -> io::Result<()> {
        Err(io::Error::other("not implemented"))
    }
}
