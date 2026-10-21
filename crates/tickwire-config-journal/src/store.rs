use crate::Config;
use std::io;
use tickwire_journal::store::Store;

/// Bounds all recovery reads before the physical decoder allocates records.
pub(crate) struct Bounded<S> {
    inner: S,
    remaining: u64,
    segments: usize,
}
impl<S> Bounded<S> {
    pub(crate) fn new(inner: S, config: &Config) -> Self {
        let records = config.max_operations * 4 + 1;
        Self {
            inner,
            // Each record has begin/data/seal overhead and at most seven pad bytes.
            remaining: config.max_recorded_bytes as u64 + records as u64 * 535 + 65_536,
            segments: records + 1,
        }
    }
}
impl<S: Store> Store for Bounded<S> {
    fn read_manifest(&mut self) -> io::Result<Option<Vec<u8>>> {
        self.inner.read_manifest()
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.inner.publish_manifest(bytes)
    }
    fn segment_ids(&mut self) -> io::Result<Vec<u64>> {
        let ids = self.inner.segment_ids()?;
        if ids.len() > self.segments {
            return Err(io::Error::other("configuration journal segment budget"));
        }
        Ok(ids)
    }
    fn read_segment(&mut self, id: u64, max_bytes: u64) -> io::Result<Vec<u8>> {
        let bytes = self.inner.read_segment(id, max_bytes.min(self.remaining))?;
        self.remaining = self
            .remaining
            .checked_sub(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("configuration journal read budget"))?;
        Ok(bytes)
    }
    fn create_segment(&mut self, id: u64) -> io::Result<()> {
        self.inner.create_segment(id)
    }
    fn append_segment(&mut self, id: u64, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.inner.append_segment(id, offset, bytes)
    }
    fn sync_segment(&mut self, id: u64) -> io::Result<()> {
        self.inner.sync_segment(id)
    }
    fn truncate_segment(&mut self, id: u64, len: u64) -> io::Result<()> {
        self.inner.truncate_segment(id, len)
    }
}
