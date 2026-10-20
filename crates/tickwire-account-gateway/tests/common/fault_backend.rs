use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};
use tickwire_account_journal::{Backend, FileBackend};
use tickwire_journal::store::Store;

pub const TORN_APPEND: u8 = 1;
pub const LOST_SYNC_REPLY: u8 = 2;

/// Real files remain the source of recovery truth. Only the requested operation fails.
pub struct FaultBackend {
    pub inner: FileBackend,
    pub next: Arc<AtomicU8>,
}
impl Store for FaultBackend {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.inner.read_manifest()
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.inner.publish_manifest(bytes)
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        self.inner.segment_ids()
    }
    fn read_segment(&mut self, id: u64, max: u64) -> io::Result<Vec<u8>> {
        self.inner.read_segment(id, max)
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        self.inner.create_segment(id)
    }
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        if self
            .next
            .compare_exchange(TORN_APPEND, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.inner
                .append_segment(id, offset, &bytes[..bytes.len() / 2])?;
            return Err(io::Error::other("injected torn append"));
        }
        self.inner.append_segment(id, offset, bytes)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        self.inner.sync_segment(id)?;
        if self.next.swap(0, Ordering::SeqCst) == LOST_SYNC_REPLY {
            return Err(io::Error::other("real sync completed; reply lost"));
        }
        Ok(())
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.inner.truncate_segment(id, len)
    }
}
impl Backend for FaultBackend {
    fn read_bootstrap(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.inner.read_bootstrap()
    }
    fn publish_bootstrap(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.inner.publish_bootstrap(bytes)
    }
    fn read_baseline(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.inner.read_baseline()
    }
    fn publish_baseline(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.inner.publish_baseline(bytes)
    }
}
