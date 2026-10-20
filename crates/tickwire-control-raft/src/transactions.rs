//! Large logical updates on the existing bounded, synchronized physical journal.
use crate::{Config, Error, Result};
use sha2::{Digest, Sha256};
use tickwire_journal::{Journal, store::Store};

const MAGIC: &[u8; 8] = b"TWRT0001";
const CHUNK: usize = 60_000;
pub(crate) const MAX_UPDATE: usize = 64 * 1024 * 1024;
pub(crate) struct Transactions<S: Store> {
    journal: Journal<S>,
    writer: u64,
    next: u64,
    retained: usize,
    capacity: usize,
    poisoned: bool,
}
struct Partial {
    id: u64,
    len: usize,
    hash: [u8; 32],
    next_chunk: u32,
    bytes: Vec<u8>,
}
impl<S: Store> Transactions<S> {
    pub(crate) fn create(store: S, config: &Config) -> Result<Self> {
        Ok(Self {
            journal: Journal::create(store, config.physical.clone()).map_err(|_| Error::Store)?,
            writer: config.writer_epoch,
            next: 1,
            retained: 0,
            capacity: config.max_retained_bytes,
            poisoned: false,
        })
    }
    pub(crate) fn recover(store: S, config: &Config) -> Result<(Self, Vec<Vec<u8>>)> {
        let (journal, recovered) = Journal::recover(store, None).map_err(|_| Error::Store)?;
        if journal.config() != &config.physical {
            return Err(Error::Identity);
        }
        let mut out = Self {
            journal,
            writer: config.writer_epoch,
            next: 1,
            retained: 0,
            capacity: config.max_retained_bytes,
            poisoned: false,
        };
        let mut partial: Option<Partial> = None;
        let mut complete = Vec::new();
        let mut previous_epoch = 0;
        for record in recovered.records {
            if record.exec_epoch < previous_epoch
                || record.exec_epoch == 0
                || record.exec_epoch > config.writer_epoch
            {
                return Err(Error::Identity);
            }
            previous_epoch = record.exec_epoch;
            out.retained = out
                .retained
                .checked_add(record.payload.len())
                .ok_or(Error::Bounds)?;
            if out.retained > out.capacity {
                return Err(Error::Bounds);
            }
            let bytes = record.payload;
            if bytes.len() < 17 || &bytes[..8] != MAGIC {
                return Err(Error::Corrupt);
            }
            let id = u64::from_le_bytes(bytes[9..17].try_into().unwrap());
            match bytes[8] {
                1 if bytes.len() == 57 && id == out.next => {
                    let len = u64::from_le_bytes(bytes[17..25].try_into().unwrap());
                    if len == 0 || len > MAX_UPDATE as u64 {
                        return Err(Error::Bounds);
                    }
                    // A new Begin abandons an incomplete logical tail from an
                    // earlier process. Only a valid End ever publishes state.
                    partial = Some(Partial {
                        id,
                        len: len as usize,
                        hash: bytes[25..57].try_into().unwrap(),
                        next_chunk: 0,
                        bytes: Vec::new(),
                    });
                    out.next = out.next.checked_add(1).ok_or(Error::Bounds)?;
                }
                2 if (22..=21 + CHUNK).contains(&bytes.len()) => {
                    let p = partial.as_mut().ok_or(Error::Corrupt)?;
                    let chunk = u32::from_le_bytes(bytes[17..21].try_into().unwrap());
                    if p.id != id
                        || chunk != p.next_chunk
                        || p.bytes.len() + bytes.len() - 21 > p.len
                    {
                        return Err(Error::Corrupt);
                    }
                    p.next_chunk += 1;
                    p.bytes.extend_from_slice(&bytes[21..]);
                }
                3 if bytes.len() == 17 => {
                    let p = partial.take().ok_or(Error::Corrupt)?;
                    let hash: [u8; 32] = Sha256::digest(&p.bytes).into();
                    if p.id != id || p.bytes.len() != p.len || p.hash != hash {
                        return Err(Error::Corrupt);
                    }
                    complete.push(p.bytes);
                }
                _ => return Err(Error::Corrupt),
            }
        }
        Ok((out, complete))
    }
    pub(crate) fn append(&mut self, bytes: &[u8]) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if bytes.is_empty() || bytes.len() > MAX_UPDATE {
            return Err(Error::Bounds);
        }
        let cost = bytes
            .len()
            .checked_add(74 + bytes.len().div_ceil(CHUNK) * 21)
            .ok_or(Error::Bounds)?;
        let retained = self.retained.checked_add(cost).ok_or(Error::Bounds)?;
        if retained > self.capacity || self.next == u64::MAX {
            return Err(Error::Bounds);
        }
        let id = self.next;
        let mut begin = prefix(1, id);
        begin.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        begin.extend_from_slice(&Sha256::digest(bytes));
        self.write(&begin)?;
        for (n, bytes) in bytes.chunks(CHUNK).enumerate() {
            let mut chunk = prefix(2, id);
            chunk.extend_from_slice(&(n as u32).to_le_bytes());
            chunk.extend_from_slice(bytes);
            self.write(&chunk)?;
        }
        self.write(&prefix(3, id))?;
        self.next += 1;
        self.retained = retained;
        Ok(())
    }
    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        if self.journal.append_batch(self.writer, &[bytes]).is_err() {
            self.poisoned = true;
            return Err(Error::Store);
        }
        Ok(())
    }
}
fn prefix(tag: u8, id: u64) -> Vec<u8> {
    let mut bytes = MAGIC.to_vec();
    bytes.push(tag);
    bytes.extend_from_slice(&id.to_le_bytes());
    bytes
}
