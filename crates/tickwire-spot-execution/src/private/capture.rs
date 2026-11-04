//! One WAL descriptor commits the whole native message. Its original bytes are immutable.
use super::{Batch, stream::*};
use crate::network::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs,
    sync::{Arc, Mutex, atomic::AtomicBool},
};
use tickwire_journal::{
    Config, Journal,
    store::{FileStore, Store},
};
use tickwire_recovery::{BlobStore, FileBlobReader, FileBlobStore, StoreKey, digest};

#[derive(Clone, Serialize, Deserialize)]
pub(super) enum Kind {
    Started {
        boot_id: [u8; 16],
    },
    Ready,
    Gap {
        reason: String,
    },
    Rows {
        channel: Channel,
        count: u32,
        object: [u8; 32],
    },
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Entry {
    first: u64,
    epoch: u64,
    ns: u64,
    kind: Kind,
}
impl Entry {
    fn last(&self) -> u64 {
        self.first
            + match &self.kind {
                Kind::Rows { count, .. } => u64::from(*count) - 1,
                _ => 0,
            }
    }
    fn validate(
        &self,
        previous: u64,
        epoch: u64,
        open: bool,
        ready: bool,
        time: u64,
    ) -> Result<()> {
        if self.first != previous.checked_add(1).ok_or("private sequence overflow")?
            || self.ns == 0
            || self.epoch >= (1u64 << 63)
        {
            return Err("invalid private sequence or time".into());
        }
        match &self.kind {
            Kind::Started { boot_id } if !open && self.epoch > epoch && *boot_id != [0; 16] => {}
            Kind::Ready if open && !ready && self.epoch == epoch && self.ns >= time => {}
            Kind::Rows { count, .. }
                if open
                    && self.epoch == epoch
                    && self.ns >= time
                    && *count > 0
                    && self.first.checked_add(u64::from(*count) - 1).is_some() => {}
            Kind::Gap { reason }
                if open
                    && self.epoch == epoch
                    && self.ns >= time
                    && matches!(
                        reason.as_str(),
                        "stopped"
                            | "connection_lost"
                            | "connection_or_identity_failed"
                            | "process_restart"
                    ) => {}
            _ => return Err("invalid private capture transition".into()),
        }
        Ok(())
    }
}
// Keep metadata bounded; bodies remain in the immutable store. Rotation is a cold operation.
const MAX_DESCRIPTORS: usize = 1_000_000;
pub(super) struct Capture {
    shared: Arc<Shared>,
    wal: Journal<FileStore>,
    objects: FileBlobStore,
    durable: u64,
    epoch: u64,
    epoch_floor: u64,
    open: bool,
    ready: bool,
    received: u64,
}
impl Capture {
    pub(super) fn open(config: StreamConfig) -> Result<Self> {
        use std::os::unix::fs::MetadataExt;
        if config.scope.account == 0
            || config.scope.risk_domain_id == [0; 16]
            || config.stream_id == [0; 16]
            || config.boot_id == [0; 16]
            || config.uid.is_empty()
            || config.uid.len() > 128
            || !config.uid.bytes().all(|b| b.is_ascii_digit())
        {
            return Err("invalid private capture binding".into());
        }
        let directory_is_private =
            |m: &fs::Metadata| m.file_type().is_dir() && m.mode() & 0o077 == 0;
        match fs::symlink_metadata(&config.directory) {
            Ok(m) if !directory_is_private(&m) => {
                return Err("private capture directory must be owner-only".into());
            }
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
        let mut store = FileStore::open(&config.directory)?;
        if !directory_is_private(&fs::symlink_metadata(&config.directory)?) {
            return Err("private capture directory must be owner-only".into());
        }
        let identity = json!([
            "spot-private/v1",
            config.scope.risk_domain_id,
            config.scope.venue,
            match config.scope.environment {
                tickwire_venue::Environment::Production => "production",
                tickwire_venue::Environment::Test => "test",
            },
            config.scope.account,
            config.uid
        ]);
        let domain = digest(&serde_json::to_vec(&identity)?)[..16].try_into()?;
        let cfg = Config::new(config.stream_id, domain);
        let root = config.directory.join("frames");
        let objects = FileBlobStore::open(&root)?;
        let reader = FileBlobReader::open(&root, fs::metadata(&root)?.uid())?;
        let mut index = Vec::new();
        let (mut durable, mut epoch, mut open, mut ready, mut last) = (0, 0, false, false, 0);
        let (wal, epoch_floor) = if store.read_manifest()?.is_some() {
            let (wal, recovery) = Journal::recover_checked(store, None, |stored, rec| {
                if stored != &cfg {
                    return Err(tickwire_journal::Error::Quarantined(
                        "private account scope changed",
                    ));
                }
                if rec.records.len() > MAX_DESCRIPTORS {
                    return Err(tickwire_journal::Error::Quarantined(
                        "private capture capacity",
                    ));
                }
                for r in &rec.records {
                    let e: Entry = serde_json::from_slice(&r.payload)
                        .map_err(|_| tickwire_journal::Error::Quarantined("private descriptor"))?;
                    if r.exec_epoch != e.epoch
                        || e.validate(durable, epoch, open, ready, last).is_err()
                    {
                        return Err(tickwire_journal::Error::Quarantined(
                            "private descriptor order",
                        ));
                    }
                    match &e.kind {
                        Kind::Started { .. } => {
                            open = true;
                            ready = false;
                        }
                        Kind::Ready => ready = true,
                        Kind::Gap { .. } => {
                            open = false;
                            ready = false;
                        }
                        Kind::Rows { object, count, .. } => {
                            let bytes =
                                reader
                                    .get_object(*object, super::wire::LIMIT)
                                    .map_err(|_| {
                                        tickwire_journal::Error::Quarantined(
                                            "missing private frame",
                                        )
                                    })?;
                            rows(&bytes, *count).map_err(|_| {
                                tickwire_journal::Error::Quarantined("invalid private frame")
                            })?;
                        }
                    }
                    epoch = e.epoch;
                    last = e.ns;
                    durable = e.last();
                    index.push(e);
                }
                Ok(())
            })?;
            (wal, recovery.observed_exec_epoch())
        } else {
            (Journal::create(store, cfg)?, 0)
        };
        let epoch_floor = if epoch_floor == 0 {
            use ring::rand::{SecureRandom, SystemRandom};
            let mut nonce = [0; 8];
            SystemRandom::new()
                .fill(&mut nonce)
                .map_err(|_| "private epoch randomness unavailable")?;
            (u64::from_le_bytes(nonce) >> 2).max(1)
        } else {
            epoch_floor
        };
        let shared = Arc::new(Shared {
            config,
            index: Mutex::new(index),
            status: Mutex::new(StreamStatus {
                epoch,
                durable_sequence: durable,
                last_received_ns: 0,
                ready: false,
            }),
            objects: reader,
            stop: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            initial_sequence: durable,
        });
        Ok(Self {
            shared,
            wal,
            objects,
            durable,
            epoch,
            epoch_floor,
            open,
            ready,
            received: last,
        })
    }
    pub(super) fn reader(&self) -> StreamReader {
        StreamReader(self.shared.clone())
    }
    pub(super) fn open_epoch(&self) -> bool {
        self.open
    }
    pub(super) fn pending(&self) {
        self.shared.status.lock().unwrap().ready = false;
    }
    pub(super) fn settled(&self, drained: bool) -> Result<()> {
        self.shared
            .status
            .lock()
            .map_err(|_| "private status poisoned")?
            .ready = self.ready && drained;
        Ok(())
    }
    pub(super) fn touch(&mut self, now: u64) -> Result<()> {
        if now == 0 || now < self.received {
            return Err("private monotonic clock regressed".into());
        }
        self.received = now;
        self.shared
            .status
            .lock()
            .map_err(|_| "private status poisoned")?
            .last_received_ns = now;
        Ok(())
    }
    pub(super) fn begin(&mut self, now: u64) -> Result<()> {
        self.append(
            Kind::Started {
                boot_id: self.shared.config.boot_id,
            },
            now,
        )
    }
    pub(super) fn gap(&mut self, reason: &str, now: u64) -> Result<()> {
        self.pending();
        // A restart can occur on a different host boot. End the old epoch at
        // its last recorded instant; the next Started binds the new clock.
        let ns = if reason == "process_restart" {
            self.received
        } else {
            now.max(self.received)
        };
        self.append(
            Kind::Gap {
                reason: reason.into(),
            },
            ns,
        )
    }
    pub(super) fn data(&mut self, b: Batch, frame: &[u8], now: u64) -> Result<()> {
        self.pending();
        let channel = match b.channel {
            "orders" => Channel::Orders,
            "fills" => Channel::Fills,
            "balance" => Channel::Balance,
            _ => return Err("unknown private channel".into()),
        };
        let object = digest(frame);
        self.objects.put_sync(StoreKey::Object(object), frame)?;
        self.append(
            Kind::Rows {
                channel,
                object,
                count: b.records().len().try_into()?,
            },
            now,
        )
    }
    pub(super) fn append(&mut self, kind: Kind, now: u64) -> Result<()> {
        self.pending();
        if self
            .wal
            .durable_frontier()
            .is_some_and(|f| f.last_seq >= MAX_DESCRIPTORS as u64)
        {
            return Err("private capture capacity exhausted".into());
        }
        let epoch = if matches!(kind, Kind::Started { .. }) {
            self.epoch_floor
                .max(self.epoch)
                .checked_add(1)
                .ok_or("private epoch overflow")?
        } else {
            self.epoch
        };
        let e = Entry {
            first: self
                .durable
                .checked_add(1)
                .ok_or("private sequence overflow")?,
            epoch,
            ns: now,
            kind,
        };
        e.validate(
            self.durable,
            self.epoch,
            self.open,
            self.ready,
            self.received,
        )?;
        self.wal.append_batch(epoch, &[&serde_json::to_vec(&e)?])?;
        let observed = match &e.kind {
            Kind::Started { .. } => Some(0),
            Kind::Gap { .. } => None,
            _ => Some(now),
        };
        match &e.kind {
            Kind::Started { .. } => {
                self.open = true;
                self.ready = false;
            }
            Kind::Ready => self.ready = true,
            Kind::Gap { .. } => {
                self.open = false;
                self.ready = false;
            }
            Kind::Rows { .. } => {}
        }
        self.epoch = epoch;
        self.durable = e.last();
        self.received = now;
        self.shared
            .index
            .lock()
            .map_err(|_| "private index poisoned")?
            .push(e);
        let mut status = self
            .shared
            .status
            .lock()
            .map_err(|_| "private status poisoned")?;
        *status = StreamStatus {
            epoch,
            durable_sequence: self.durable,
            last_received_ns: observed.unwrap_or(status.last_received_ns),
            ready: false,
        };
        Ok(())
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        if !self.shared.stop.load(Ordering::Acquire) {
            self.shared.failed.store(true, Ordering::Release);
        }
        self.shared.stop.store(true, Ordering::Release);
    }
}
fn rows(bytes: &[u8], count: u32) -> Result<Vec<Value>> {
    let mut msg: Value = serde_json::from_slice(bytes)?;
    match msg["data"].take() {
        Value::Array(rows) if rows.len() == count as usize => Ok(rows),
        _ => Err("invalid captured private row count".into()),
    }
}
impl Shared {
    pub(super) fn has_initial_cursor(&self, epoch: u64, sequence: u64) -> bool {
        if sequence == 0 || sequence > self.initial_sequence {
            return false;
        }
        self.index.lock().is_ok_and(|entries| {
            entries
                .get(entries.partition_point(|e| e.last() < sequence))
                .is_some_and(|entry| entry.epoch == epoch)
        })
    }
    pub(super) fn drain(&self, after: u64, limit: usize) -> Result<Vec<StreamEvent>> {
        if !(1..=256).contains(&limit) {
            return Err("private drain limit must be 1..256".into());
        }
        let entries = {
            let idx = self.index.lock().map_err(|_| "private index poisoned")?;
            if after > idx.last().map_or(0, Entry::last) {
                return Err("private cursor beyond durable capture".into());
            }
            let pos = idx.partition_point(|e| e.last() <= after);
            idx[pos..].iter().take(limit).cloned().collect::<Vec<_>>()
        };
        let mut out = Vec::new();
        let mut read_bytes = 0;
        for e in entries {
            let push = |out: &mut Vec<StreamEvent>, seq, kind| {
                out.push(StreamEvent {
                    sequence: seq,
                    epoch: e.epoch,
                    received_ns: e.ns,
                    kind,
                })
            };
            match e.kind {
                Kind::Started { boot_id } => {
                    push(&mut out, e.first, EventKind::Started { boot_id })
                }
                Kind::Gap { reason } => push(&mut out, e.first, EventKind::Gap { reason }),
                Kind::Ready => push(&mut out, e.first, EventKind::Ready),
                Kind::Rows {
                    object,
                    channel,
                    count,
                } => {
                    if read_bytes >= super::wire::LIMIT {
                        break;
                    }
                    let bytes = self.objects.get_object(object, super::wire::LIMIT)?;
                    read_bytes += bytes.len();
                    let skip = after.saturating_sub(e.first - 1) as usize;
                    for (i, row) in rows(&bytes, count)?
                        .into_iter()
                        .enumerate()
                        .skip(skip)
                        .take(limit - out.len())
                    {
                        push(
                            &mut out,
                            e.first + i as u64,
                            EventKind::Data { channel, row },
                        );
                    }
                }
            }
            if out.len() == limit {
                break;
            }
        }
        Ok(out)
    }
}
