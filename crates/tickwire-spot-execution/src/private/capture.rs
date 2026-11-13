//! One WAL descriptor commits the whole native message. Its original bytes are immutable.
use super::{Batch, stream::*};
use crate::network::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, VecDeque},
    fs,
    path::PathBuf,
    sync::{Arc, Mutex, atomic::AtomicBool, mpsc},
    thread,
};
use tickwire_journal::{
    Config, Journal, StorageMode,
    store::{FileStore, InMemoryStore, MmapAsyncStore, Store},
};
use tickwire_recovery::{
    AsyncFileBlobStore, BlobStore, FileBlobReader, FileBlobStore, InMemoryBlobStore, StoreKey,
    digest,
};

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

pub(super) struct IndexState {
    pub entries: VecDeque<Entry>,
    pub trimmed_through: u64,
    pub highest_drained: u64,
}

pub(super) enum SharedObjects {
    InMemory(Arc<Mutex<InMemoryBlobStore>>),
    Disk(FileBlobReader),
}
impl SharedObjects {
    fn get_object(&self, object: [u8; 32], limit: usize) -> Result<Vec<u8>> {
        match self {
            Self::InMemory(store) => {
                let guard = store
                    .lock()
                    .map_err(|_| "private in-memory object store poisoned")?;
                let bytes = guard.get(StoreKey::Object(object), limit)?;
                if digest(&bytes) != object {
                    return Err("corrupt private in-memory frame".into());
                }
                Ok(bytes)
            }
            Self::Disk(reader) => Ok(reader.get_object(object, limit)?),
        }
    }
}

enum MmapWalCmd {
    DurableLastSeq(mpsc::SyncSender<Option<u64>>),
    Append {
        epoch: u64,
        bytes: Vec<u8>,
        reply: mpsc::SyncSender<std::result::Result<(), String>>,
    },
}

struct MmapWalHandle {
    tx: Option<mpsc::SyncSender<MmapWalCmd>>,
    worker: Option<thread::JoinHandle<()>>,
}

struct OpenOutcome {
    epoch_floor: u64,
    index: VecDeque<Entry>,
    durable: u64,
    epoch: u64,
    open: bool,
    ready: bool,
    last: u64,
}

impl MmapWalHandle {
    fn open(
        directory: PathBuf,
        cfg: Config,
        frames_root: PathBuf,
        owner_uid: u32,
        max_descriptors: usize,
    ) -> Result<(Self, OpenOutcome)> {
        let (init_tx, init_rx) = mpsc::sync_channel::<std::result::Result<OpenOutcome, String>>(1);
        let (cmd_tx, cmd_rx) = mpsc::sync_channel::<MmapWalCmd>(8);
        let worker = thread::Builder::new()
            .name("spot-priv-mmap".into())
            .spawn(move || {
                let opened = (|| -> Result<(Journal<MmapAsyncStore>, OpenOutcome)> {
                    let store = MmapAsyncStore::open(&directory)?;
                    check_private_directory_after_open(&directory)?;
                    let reader = FileBlobReader::open(&frames_root, owner_uid)?;
                    recover_or_create_disk(store, cfg, &reader, max_descriptors)
                })();
                let mut wal = match opened {
                    Ok((wal, outcome)) => {
                        let _ = init_tx.send(Ok(outcome));
                        wal
                    }
                    Err(e) => {
                        let _ = init_tx.send(Err(e.to_string()));
                        return;
                    }
                };
                while let Ok(cmd) = cmd_rx.recv() {
                    match cmd {
                        MmapWalCmd::DurableLastSeq(reply) => {
                            let _ = reply.send(wal.durable_frontier().map(|f| f.last_seq));
                        }
                        MmapWalCmd::Append {
                            epoch,
                            bytes,
                            reply,
                        } => {
                            let res = wal
                                .append_batch(epoch, &[bytes.as_slice()])
                                .map(|_| ())
                                .map_err(|e| e.to_string());
                            let _ = reply.send(res);
                        }
                    }
                }
            })?;
        match init_rx
            .recv()
            .map_err(|_| "mmap private WAL worker failed during open")?
        {
            Ok(outcome) => Ok((
                Self {
                    tx: Some(cmd_tx),
                    worker: Some(worker),
                },
                outcome,
            )),
            Err(msg) => {
                let _ = worker.join();
                Err(msg.into())
            }
        }
    }

    fn durable_last_seq(&self) -> Option<u64> {
        let tx = self.tx.as_ref()?;
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        tx.send(MmapWalCmd::DurableLastSeq(reply_tx)).ok()?;
        reply_rx.recv().ok().flatten()
    }

    fn append_batch(&self, epoch: u64, bytes: &[u8]) -> Result<()> {
        let tx = self.tx.as_ref().ok_or("mmap private WAL closed")?;
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        tx.send(MmapWalCmd::Append {
            epoch,
            bytes: bytes.to_vec(),
            reply: reply_tx,
        })
        .map_err(|_| "mmap private WAL worker stopped")?;
        reply_rx
            .recv()
            .map_err(|_| "mmap private WAL worker disconnected")?
            .map_err(Into::into)
    }
}

impl Drop for MmapWalHandle {
    fn drop(&mut self) {
        self.tx.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

enum CaptureWal {
    InMemory {
        journal: Journal<InMemoryStore>,
        cfg: Config,
        records_in_journal: usize,
    },
    MmapAsync(MmapWalHandle),
    LocalDurable(Journal<FileStore>),
}
impl CaptureWal {
    fn durable_last_seq(&self) -> Option<u64> {
        match self {
            Self::InMemory { journal, .. } => journal.durable_frontier().map(|f| f.last_seq),
            Self::MmapAsync(handle) => handle.durable_last_seq(),
            Self::LocalDurable(journal) => journal.durable_frontier().map(|f| f.last_seq),
        }
    }
    fn append_entry(&mut self, epoch: u64, bytes: &[u8], max_descriptors: usize) -> Result<()> {
        match self {
            Self::InMemory {
                journal,
                cfg,
                records_in_journal,
            } => {
                if *records_in_journal >= max_descriptors {
                    *journal = Journal::create(InMemoryStore::new(), cfg.clone())?;
                    *records_in_journal = 0;
                }
                journal.append_batch(epoch, &[bytes])?;
                *records_in_journal += 1;
                Ok(())
            }
            Self::MmapAsync(handle) => handle.append_batch(epoch, bytes),
            Self::LocalDurable(journal) => {
                journal.append_batch(epoch, &[bytes])?;
                Ok(())
            }
        }
    }
}

enum CaptureObjects {
    InMemory(Arc<Mutex<InMemoryBlobStore>>),
    MmapAsync(AsyncFileBlobStore),
    LocalDurable(FileBlobStore),
}
impl CaptureObjects {
    fn put_frame(&mut self, object: [u8; 32], frame: &[u8]) -> Result<()> {
        let key = StoreKey::Object(object);
        match self {
            Self::InMemory(store) => {
                store
                    .lock()
                    .map_err(|_| "private in-memory object store poisoned")?
                    .put_sync(key, frame)?;
                Ok(())
            }
            Self::MmapAsync(store) => {
                store.put_sync(key, frame)?;
                Ok(())
            }
            Self::LocalDurable(store) => {
                store.put_sync(key, frame)?;
                Ok(())
            }
        }
    }
}

pub(super) struct Capture {
    shared: Arc<Shared>,
    wal: CaptureWal,
    objects: CaptureObjects,
    max_descriptors: usize,
    durable: u64,
    epoch: u64,
    epoch_floor: u64,
    open: bool,
    ready: bool,
    received: u64,
}

fn recover_or_create_disk<S: Store>(
    mut store: S,
    cfg: Config,
    reader: &FileBlobReader,
    max_descriptors: usize,
) -> Result<(Journal<S>, OpenOutcome)> {
    let mut index = VecDeque::new();
    let (mut durable, mut epoch, mut open, mut ready, mut last) = (0, 0, false, false, 0);
    let (wal, epoch_floor) = if store.read_manifest()?.is_some() {
        let (wal, recovery) = Journal::recover_checked(store, None, |stored, rec| {
            if stored != &cfg {
                return Err(tickwire_journal::Error::Quarantined(
                    "private account scope changed",
                ));
            }
            if rec.records.len() > max_descriptors {
                return Err(tickwire_journal::Error::Quarantined(
                    "private capture capacity",
                ));
            }
            for r in &rec.records {
                let e: Entry = serde_json::from_slice(&r.payload)
                    .map_err(|_| tickwire_journal::Error::Quarantined("private descriptor"))?;
                if r.exec_epoch != e.epoch || e.validate(durable, epoch, open, ready, last).is_err()
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
                                    tickwire_journal::Error::Quarantined("missing private frame")
                                })?;
                        rows(&bytes, *count).map_err(|_| {
                            tickwire_journal::Error::Quarantined("invalid private frame")
                        })?;
                    }
                }
                epoch = e.epoch;
                last = e.ns;
                durable = e.last();
                index.push_back(e);
            }
            Ok(())
        })?;
        (wal, recovery.observed_exec_epoch())
    } else {
        (Journal::create(store, cfg)?, 0)
    };
    Ok((
        wal,
        OpenOutcome {
            epoch_floor,
            index,
            durable,
            epoch,
            open,
            ready,
            last,
        },
    ))
}

fn check_private_directory_before_open(directory: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let directory_is_private = |m: &fs::Metadata| m.file_type().is_dir() && m.mode() & 0o077 == 0;
    match fs::symlink_metadata(directory) {
        Ok(m) if !directory_is_private(&m) => {
            return Err("private capture directory must be owner-only".into());
        }
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
        _ => {}
    }
    Ok(())
}

fn check_private_directory_after_open(directory: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let m = fs::symlink_metadata(directory)?;
    if !(m.file_type().is_dir() && m.mode() & 0o077 == 0) {
        return Err("private capture directory must be owner-only".into());
    }
    Ok(())
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
            || config.max_descriptors == 0
        {
            return Err("invalid private capture binding".into());
        }
        let max_descriptors = config.max_descriptors;
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

        let (wal, objects, shared_objects, index, durable, epoch, epoch_floor, open, ready, last) =
            match config.storage_mode {
                StorageMode::InMemory => {
                    let mem_blobs = Arc::new(Mutex::new(InMemoryBlobStore::new()));
                    let journal = Journal::create(InMemoryStore::new(), cfg.clone())?;
                    (
                        CaptureWal::InMemory {
                            journal,
                            cfg,
                            records_in_journal: 0,
                        },
                        CaptureObjects::InMemory(Arc::clone(&mem_blobs)),
                        SharedObjects::InMemory(mem_blobs),
                        VecDeque::new(),
                        0u64,
                        0u64,
                        0u64,
                        false,
                        false,
                        0u64,
                    )
                }
                StorageMode::MmapAsync => {
                    check_private_directory_before_open(&config.directory)?;
                    let mut builder = fs::DirBuilder::new();
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::DirBuilderExt;
                        builder.mode(0o700);
                    }
                    match builder.create(&config.directory) {
                        Ok(()) => {}
                        Err(e)
                            if e.kind() == std::io::ErrorKind::AlreadyExists
                                && config.directory.is_dir() => {}
                        Err(e) => return Err(e.into()),
                    }
                    check_private_directory_after_open(&config.directory)?;
                    let root = config.directory.join("frames");
                    let objects = AsyncFileBlobStore::open(&root)?;
                    let owner_uid = fs::metadata(&root)?.uid();
                    let reader = FileBlobReader::open(&root, owner_uid)?;
                    let (
                        wal,
                        OpenOutcome {
                            epoch_floor,
                            index,
                            durable,
                            epoch,
                            open,
                            ready,
                            last,
                        },
                    ) = MmapWalHandle::open(
                        config.directory.clone(),
                        cfg,
                        root,
                        owner_uid,
                        max_descriptors,
                    )?;
                    (
                        CaptureWal::MmapAsync(wal),
                        CaptureObjects::MmapAsync(objects),
                        SharedObjects::Disk(reader),
                        index,
                        durable,
                        epoch,
                        epoch_floor,
                        open,
                        ready,
                        last,
                    )
                }
                StorageMode::LocalDurable => {
                    check_private_directory_before_open(&config.directory)?;
                    let store = FileStore::open(&config.directory)?;
                    check_private_directory_after_open(&config.directory)?;
                    let root = config.directory.join("frames");
                    let objects = FileBlobStore::open(&root)?;
                    let reader = FileBlobReader::open(&root, fs::metadata(&root)?.uid())?;
                    let (
                        wal,
                        OpenOutcome {
                            epoch_floor,
                            index,
                            durable,
                            epoch,
                            open,
                            ready,
                            last,
                        },
                    ) = recover_or_create_disk(store, cfg, &reader, max_descriptors)?;
                    (
                        CaptureWal::LocalDurable(wal),
                        CaptureObjects::LocalDurable(objects),
                        SharedObjects::Disk(reader),
                        index,
                        durable,
                        epoch,
                        epoch_floor,
                        open,
                        ready,
                        last,
                    )
                }
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
            index: Mutex::new(IndexState {
                entries: index,
                trimmed_through: 0,
                highest_drained: 0,
            }),
            status: Mutex::new(StreamStatus {
                epoch,
                durable_sequence: durable,
                last_received_ns: 0,
                ready: false,
            }),
            objects: shared_objects,
            stop: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            initial_sequence: durable,
        });
        Ok(Self {
            shared,
            wal,
            objects,
            max_descriptors,
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
        self.objects.put_frame(object, frame)?;
        self.append(
            Kind::Rows {
                channel,
                object,
                count: b.records().len().try_into()?,
            },
            now,
        )
    }

    fn trim_in_memory_window(&mut self, current_object: Option<[u8; 32]>) -> Result<()> {
        let CaptureObjects::InMemory(mem_store) = &self.objects else {
            return Ok(());
        };
        let retained_objects = {
            let mut idx = self
                .shared
                .index
                .lock()
                .map_err(|_| "private index poisoned")?;
            if idx.entries.len() < self.max_descriptors {
                return Ok(());
            }
            let drained = idx.highest_drained;
            let target_len = (self.max_descriptors / 2).max(1) - 1;
            while idx.entries.len() > target_len {
                let Some(front_last) = idx.entries.front().map(Entry::last) else {
                    break;
                };
                if front_last > drained {
                    break;
                }
                idx.entries.pop_front();
                idx.trimmed_through = front_last;
            }
            if idx.entries.len() >= self.max_descriptors {
                return Err("private capture capacity exhausted".into());
            }
            let mut keep = BTreeSet::new();
            if let Some(obj) = current_object {
                keep.insert(obj);
            }
            for entry in &idx.entries {
                if let Kind::Rows { object, .. } = &entry.kind {
                    keep.insert(*object);
                }
            }
            keep
        };
        let mut store_guard = mem_store
            .lock()
            .map_err(|_| "private in-memory object store poisoned")?;
        let mut compacted = InMemoryBlobStore::new();
        for object in retained_objects {
            let key = StoreKey::Object(object);
            if let Ok(bytes) = store_guard.get(key, super::wire::LIMIT) {
                compacted.put_sync(key, &bytes)?;
            }
        }
        *store_guard = compacted;
        Ok(())
    }

    pub(super) fn append(&mut self, kind: Kind, now: u64) -> Result<()> {
        self.pending();
        if matches!(self.objects, CaptureObjects::InMemory(_)) {
            let current_obj = match &kind {
                Kind::Rows { object, .. } => Some(*object),
                _ => None,
            };
            self.trim_in_memory_window(current_obj)?;
        } else if self
            .wal
            .durable_last_seq()
            .is_some_and(|last_seq| last_seq >= self.max_descriptors as u64)
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
        self.wal
            .append_entry(epoch, &serde_json::to_vec(&e)?, self.max_descriptors)?;
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
            .entries
            .push_back(e);
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
fn partition_entries(entries: &VecDeque<Entry>, pred: impl Fn(&Entry) -> bool) -> usize {
    let mut lo = 0usize;
    let mut hi = entries.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if pred(&entries[mid]) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}
impl Shared {
    pub(super) fn has_initial_cursor(&self, epoch: u64, sequence: u64) -> bool {
        if sequence == 0 || sequence > self.initial_sequence {
            return false;
        }
        self.index.lock().is_ok_and(|idx| {
            if sequence <= idx.trimmed_through {
                return false;
            }
            let pos = partition_entries(&idx.entries, |e| e.last() < sequence);
            idx.entries
                .get(pos)
                .is_some_and(|entry| entry.epoch == epoch)
        })
    }
    pub(super) fn drain(&self, after: u64, limit: usize) -> Result<Vec<StreamEvent>> {
        if !(1..=256).contains(&limit) {
            return Err("private drain limit must be 1..256".into());
        }
        let entries = {
            let idx = self.index.lock().map_err(|_| "private index poisoned")?;
            let durable_tip = idx.entries.back().map_or(idx.trimmed_through, Entry::last);
            if after > durable_tip {
                return Err("private cursor beyond durable capture".into());
            }
            if after < idx.trimmed_through {
                return Err("private cursor precedes retained capture window".into());
            }
            let pos = partition_entries(&idx.entries, |e| e.last() <= after);
            idx.entries
                .iter()
                .skip(pos)
                .take(limit)
                .cloned()
                .collect::<Vec<_>>()
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
        if let Some(last_ev) = out.last()
            && let Ok(mut idx) = self.index.lock()
        {
            idx.highest_drained = idx.highest_drained.max(last_ev.sequence);
        }
        Ok(out)
    }
}
