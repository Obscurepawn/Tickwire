use crate::*;
use serde::de::DeserializeOwned;
use tickwire_journal::{Journal, PhysicalFrontier, store::Store};
const MAX_BYTES: usize = 128 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Frame {
    Chunk {
        id: u64,
        index: u32,
        bytes: Vec<u8>,
    },
    Commit {
        id: u64,
        digest: Hash,
        length: usize,
    },
}
pub struct Wal<S: Store> {
    journal: Journal<S>,
    writer: u64,
    position: u64,
    retained: usize,
    events: usize,
    poisoned: bool,
}
impl<S: Store> Wal<S> {
    pub fn create(store: S, config: tickwire_journal::Config, writer: u64) -> Result<Self> {
        if writer == 0 {
            return Err(Error::Invalid);
        }
        Ok(Self {
            journal: Journal::create(store, config).map_err(|_| Error::Store)?,
            writer,
            position: 0,
            retained: 0,
            events: 0,
            poisoned: false,
        })
    }
    pub fn recover<T: DeserializeOwned>(
        store: S,
        config: &tickwire_journal::Config,
        writer: u64,
        required: Option<&PhysicalFrontier>,
    ) -> Result<(Self, Vec<T>)> {
        if writer == 0 {
            return Err(Error::Invalid);
        }
        let (journal, recovered) = Journal::recover(store, required).map_err(|_| Error::Store)?;
        if journal.config() != config {
            return Err(Error::Identity);
        }
        let mut position = 0;
        let mut prior_writer = 0;
        let mut retained = 0usize;
        let mut events = vec![];
        let mut current = None;
        let mut next = 0;
        let mut buffer = vec![];
        for raw in recovered.records {
            if raw.seq != position + 1
                || raw.exec_epoch < prior_writer
                || raw.exec_epoch == 0
                || raw.exec_epoch > writer
            {
                return Err(Error::Identity);
            }
            position = raw.seq;
            prior_writer = raw.exec_epoch;
            retained = retained
                .checked_add(raw.payload.len())
                .ok_or(Error::Capacity)?;
            if retained > MAX_BYTES {
                return Err(Error::Capacity);
            }
            match serde_json::from_slice(&raw.payload).map_err(|_| Error::Store)? {
                Frame::Chunk { id, index, bytes } => {
                    if index == 0 {
                        if id != raw.seq {
                            return Err(Error::Store);
                        }
                        current = Some(id);
                        next = 0;
                        buffer.clear();
                    }
                    if current != Some(id)
                        || next != index
                        || bytes.is_empty()
                        || bytes.len() > 8192
                        || buffer.len() + bytes.len() > 4 * 1024 * 1024
                    {
                        return Err(Error::Store);
                    }
                    next += 1;
                    buffer.extend(bytes);
                }
                Frame::Commit { id, digest, length } => {
                    let actual: Hash = Sha256::digest(&buffer).into();
                    if current != Some(id) || length != buffer.len() || actual != digest {
                        return Err(Error::Store);
                    }
                    events.push(serde_json::from_slice(&buffer).map_err(|_| Error::Store)?);
                    if events.len() > 4096 {
                        return Err(Error::Capacity);
                    }
                    current = None;
                    buffer.clear();
                    next = 0;
                }
            }
        }
        Ok((
            Self {
                journal,
                writer,
                position,
                retained,
                events: events.len(),
                poisoned: false,
            },
            events,
        ))
    }
    pub fn append<T: Serialize>(&mut self, event: &T) -> Result<()> {
        if self.poisoned {
            return Err(Error::Store);
        }
        let bytes = serde_json::to_vec(event).map_err(|_| Error::Invalid)?;
        if bytes.is_empty() || bytes.len() > 4 * 1024 * 1024 || self.events >= 4096 {
            return Err(Error::Capacity);
        }
        let id = self.position.checked_add(1).ok_or(Error::Capacity)?;
        let mut frames = Vec::new();
        for (index, chunk) in bytes.chunks(8192).enumerate() {
            frames.push(
                serde_json::to_vec(&Frame::Chunk {
                    id,
                    index: index as u32,
                    bytes: chunk.to_vec(),
                })
                .map_err(|_| Error::Invalid)?,
            );
        }
        frames.push(
            serde_json::to_vec(&Frame::Commit {
                id,
                digest: Sha256::digest(&bytes).into(),
                length: bytes.len(),
            })
            .map_err(|_| Error::Invalid)?,
        );
        let retained = frames
            .iter()
            .try_fold(self.retained, |sum, f| sum.checked_add(f.len()))
            .ok_or(Error::Capacity)?;
        if retained > MAX_BYTES {
            return Err(Error::Capacity);
        }
        let outcome = (|| {
            for frame in frames {
                self.position = self
                    .journal
                    .append_batch(self.writer, &[&frame])
                    .map_err(|_| Error::Store)?
                    .frontier()
                    .last_seq;
            }
            Ok(())
        })();
        if outcome.is_err() {
            self.poisoned = true;
            return outcome;
        }
        self.retained = retained;
        self.events += 1;
        Ok(())
    }
    pub fn position(&self) -> u64 {
        self.position
    }
    pub fn frontier(&self) -> Option<&PhysicalFrontier> {
        if self.poisoned {
            None
        } else {
            self.journal.durable_frontier()
        }
    }
}
