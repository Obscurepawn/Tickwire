use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tickwire_journal::{Journal, PhysicalFrontier, store::Store};

const CHUNK: usize = 8192;
const MAX_EVENT: usize = 8 * 1024 * 1024;
const MAX_RETAINED: usize = 64 * 1024 * 1024;
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
        length: usize,
        digest: Hash,
    },
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub(crate) enum Event {
    Config {
        schema: u32,
        binding: Vec<u8>,
    },
    QuotaRun {
        producer: [u8; 16],
        consumer: [u8; 16],
        generation: u64,
    },
    Start {
        command: Vec<u8>,
        authorization: Authorization,
        wire_digest: Hash,
        connection_epoch: u64,
        started_ns: u64,
        clock_boot_id: [u8; 16],
        clock_generation: u64,
    },
    Complete {
        command_digest: Hash,
        outcome: Outcome,
    },
}
pub(crate) struct Audit<S: Store> {
    journal: Journal<S>,
    writer: u64,
    events: Vec<Event>,
    retained: usize,
    poisoned: bool,
    position: u64,
}
impl<S: Store> Audit<S> {
    pub fn create(store: S, config: &Config) -> Result<Self> {
        let journal = Journal::create(store, config.audit.clone()).map_err(|_| Error::Audit)?;
        let mut out = Self {
            journal,
            writer: config.writer_epoch,
            events: vec![],
            retained: 0,
            poisoned: false,
            position: 0,
        };
        out.append(Event::Config {
            schema: 1,
            binding: config.binding()?,
        })?;
        Ok(out)
    }
    pub fn recover(store: S, config: &Config, required: Option<&PhysicalFrontier>) -> Result<Self> {
        let (journal, recovery) = Journal::recover(store, required).map_err(|_| Error::Audit)?;
        if journal.config() != &config.audit {
            return Err(Error::Identity);
        }
        let mut events = vec![];
        let mut buffered = Vec::new();
        let mut current = None;
        let mut next = 0u32;
        let mut total = 0usize;
        let mut prior_epoch = 0;
        let mut record_sequence = 0u64;
        for raw in recovery.records {
            if raw.seq != record_sequence + 1
                || raw.exec_epoch < prior_epoch
                || raw.exec_epoch == 0
                || raw.exec_epoch > config.writer_epoch
            {
                return Err(Error::Identity);
            }
            record_sequence = raw.seq;
            prior_epoch = raw.exec_epoch;
            total = total
                .checked_add(raw.payload.len())
                .ok_or(Error::Capacity)?;
            if total > MAX_RETAINED * 5 {
                return Err(Error::Capacity);
            }
            let frame: Frame = serde_json::from_slice(&raw.payload).map_err(|_| Error::Audit)?;
            match frame {
                Frame::Chunk { id, index, bytes } => {
                    if index == 0 {
                        // A new event abandons only an uncommitted previous tail.
                        if id != raw.seq {
                            return Err(Error::Audit);
                        }
                        current = Some(id);
                        buffered.clear();
                        next = 0;
                    }
                    if current != Some(id)
                        || index != next
                        || bytes.is_empty()
                        || bytes.len() > CHUNK
                    {
                        return Err(Error::Audit);
                    }
                    next = next.checked_add(1).ok_or(Error::Capacity)?;
                    if buffered
                        .len()
                        .checked_add(bytes.len())
                        .is_none_or(|n| n > MAX_EVENT)
                    {
                        return Err(Error::Capacity);
                    }
                    buffered.extend(bytes);
                }
                Frame::Commit { id, length, digest } => {
                    if current != Some(id) || length != buffered.len() || hash(&buffered) != digest
                    {
                        return Err(Error::Audit);
                    }
                    let event: Event =
                        serde_json::from_slice(&buffered).map_err(|_| Error::Audit)?;
                    events.push(event);
                    if events.len() > 8193 {
                        return Err(Error::Capacity);
                    }
                    current = None;
                    buffered.clear();
                    next = 0;
                }
            }
        }
        if !matches!(events.first(),Some(Event::Config{schema:1,binding})if *binding==config.binding()?)
        {
            return Err(Error::Identity);
        }
        let retained = events.iter().try_fold(0usize, |n, e| {
            n.checked_add(serde_json::to_vec(e).map_err(|_| Error::Audit)?.len())
                .ok_or(Error::Capacity)
        })?;
        if retained > MAX_RETAINED {
            return Err(Error::Capacity);
        }
        Ok(Self {
            journal,
            writer: config.writer_epoch,
            events,
            retained,
            poisoned: false,
            position: record_sequence,
        })
    }
    pub fn append(&mut self, event: Event) -> Result<()> {
        if self.poisoned {
            return Err(Error::Audit);
        }
        let bytes = serde_json::to_vec(&event).map_err(|_| Error::Audit)?;
        let retained = self
            .retained
            .checked_add(bytes.len())
            .ok_or(Error::Capacity)?;
        if bytes.is_empty()
            || bytes.len() > MAX_EVENT
            || retained > MAX_RETAINED
            || self.events.len() >= 8193
        {
            return Err(Error::Capacity);
        }
        let id = self.position().checked_add(1).ok_or(Error::Capacity)?;
        let result = (|| {
            for (index, part) in bytes.chunks(CHUNK).enumerate() {
                let frame = serde_json::to_vec(&Frame::Chunk {
                    id,
                    index: index as u32,
                    bytes: part.to_vec(),
                })
                .map_err(|_| Error::Audit)?;
                self.position = self
                    .journal
                    .append_batch(self.writer, &[&frame])
                    .map_err(|_| Error::Audit)?
                    .frontier()
                    .last_seq;
            }
            let frame = serde_json::to_vec(&Frame::Commit {
                id,
                length: bytes.len(),
                digest: hash(&bytes),
            })
            .map_err(|_| Error::Audit)?;
            self.position = self
                .journal
                .append_batch(self.writer, &[&frame])
                .map_err(|_| Error::Audit)?
                .frontier()
                .last_seq;
            Ok(())
        })();
        if result.is_err() {
            self.poisoned = true;
            return result;
        }
        self.events.push(event);
        self.retained = retained;
        Ok(())
    }
    pub fn position(&self) -> u64 {
        self.position
    }
    pub fn events(&self) -> &[Event] {
        &self.events
    }
    pub fn start_quota_run(
        &mut self,
        producer: [u8; 16],
        consumer: [u8; 16],
        generation: u64,
    ) -> Result<()> {
        if producer == [0; 16] || consumer == [0; 16] || generation == 0 {
            return Err(Error::Identity);
        }
        let mut used = BTreeSet::new();
        for e in &self.events {
            if let Event::QuotaRun {
                producer: p,
                consumer: c,
                generation: g,
            } = e
            {
                used.insert((*p, *c, *g));
            }
        }
        if used.contains(&(producer, consumer, generation)) {
            return Err(Error::AlreadyUsed);
        }
        self.append(Event::QuotaRun {
            producer,
            consumer,
            generation,
        })
    }
}
