use crate::frame::{self, FrameType, Header};
use crate::{Error, store::Store};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
pub const SEGMENT_MAX_BYTES: u64 = 268_435_456;
pub const BATCH_MAX_BYTES: usize = 65_536;
pub const BATCH_MAX_RECORDS: usize = 32;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub stream_epoch: [u8; 16],
    pub domain: [u8; 16],
    pub segment_max_bytes: u64,
}
impl Config {
    pub fn new(stream_epoch: [u8; 16], domain: [u8; 16]) -> Self {
        Self {
            stream_epoch,
            domain,
            segment_max_bytes: SEGMENT_MAX_BYTES,
        }
    }
}
/// An externally trusted physical prefix, never a trading authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalFrontier {
    pub stream_epoch: [u8; 16],
    pub domain: [u8; 16],
    pub segment_id: u64,
    pub end_offset: u64,
    pub end_lsn: u64,
    pub last_seq: u64,
    pub exec_epoch: u64,
    pub batch_number: u64,
    pub seal_hash: [u8; 32],
}
/// Created only after the selected Store reports successful synchronization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalSyncReceipt {
    frontier: PhysicalFrontier,
}
impl PhysicalSyncReceipt {
    pub fn frontier(&self) -> &PhysicalFrontier {
        &self.frontier
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub seq: u64,
    pub exec_epoch: u64,
    pub payload: Vec<u8>,
}
#[derive(Debug)]
pub struct Recovery {
    pub records: Vec<Record>,
    pub frontier: Option<PhysicalFrontier>,
    pub discarded_tail_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestBody {
    version: u32,
    config: Config,
    generation: u64,
    active_segment: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    body: ManifestBody,
    sha256: [u8; 32],
}
fn manifest_bytes(body: &ManifestBody) -> Result<Vec<u8>, Error> {
    let bytes = serde_json::to_vec(body).map_err(|_| Error::Invalid("manifest encoding"))?;
    let hash = Sha256::digest(bytes).into();
    serde_json::to_vec(&Manifest {
        body: body.clone(),
        sha256: hash,
    })
    .map_err(|_| Error::Invalid("manifest encoding"))
}
fn parse_manifest(bytes: &[u8]) -> Result<ManifestBody, Error> {
    let m: Manifest =
        serde_json::from_slice(bytes).map_err(|_| Error::Quarantined("manifest syntax"))?;
    let body = serde_json::to_vec(&m.body).map_err(|_| Error::Quarantined("manifest encoding"))?;
    let hash: [u8; 32] = Sha256::digest(body).into();
    if m.sha256 != hash
        || m.body.version != 1
        || m.body.generation == 0
        || m.body.active_segment == 0
    {
        return Err(Error::Quarantined("manifest checksum or version"));
    }
    check_config(&m.body.config).map_err(|_| Error::Quarantined("manifest config"))?;
    Ok(m.body)
}
fn check_config(c: &Config) -> Result<(), Error> {
    if c.stream_epoch == [0; 16]
        || c.domain == [0; 16]
        || !(528..=SEGMENT_MAX_BYTES).contains(&c.segment_max_bytes)
        || !c.segment_max_bytes.is_multiple_of(8)
    {
        return Err(Error::Invalid("identity or segment capacity"));
    }
    Ok(())
}
#[derive(Clone)]
struct Position {
    segment: u64,
    offset: u64,
    lsn: u64,
    seq: u64,
    epoch: u64,
    batch: u64,
    hash: [u8; 32],
}
impl Position {
    fn empty() -> Self {
        Self {
            segment: 1,
            offset: 0,
            lsn: 0,
            seq: 0,
            epoch: 0,
            batch: 0,
            hash: [0; 32],
        }
    }
    fn frontier(&self, c: &Config) -> PhysicalFrontier {
        PhysicalFrontier {
            stream_epoch: c.stream_epoch,
            domain: c.domain,
            segment_id: self.segment,
            end_offset: self.offset,
            end_lsn: self.lsn,
            last_seq: self.seq,
            exec_epoch: self.epoch,
            batch_number: self.batch,
            seal_hash: self.hash,
        }
    }
}
struct OpenBatch {
    before: Position,
    epoch: u64,
    number: u64,
    records: Vec<Record>,
}
pub struct Journal<S: Store> {
    store: S,
    manifest: ManifestBody,
    position: Position,
    poisoned: bool,
    durable: Option<PhysicalFrontier>,
}
impl<S: Store> Journal<S> {
    pub fn create(mut store: S, config: Config) -> Result<Self, Error> {
        check_config(&config)?;
        if store.read_manifest()?.is_some() || !store.segment_ids()?.is_empty() {
            return Err(Error::Invalid("journal already exists"));
        }
        let manifest = ManifestBody {
            version: 1,
            config,
            generation: 1,
            active_segment: 1,
        };
        store.create_segment(1)?;
        store.publish_manifest(&manifest_bytes(&manifest)?)?;
        Ok(Self {
            store,
            manifest,
            position: Position::empty(),
            poisoned: false,
            durable: None,
        })
    }
    /// Validate all bytes before truncating only the last unfinished batch.
    /// A supplied frontier must come from independent trusted evidence.
    pub fn recover(
        mut store: S,
        required: Option<&PhysicalFrontier>,
    ) -> Result<(Self, Recovery), Error> {
        let bytes = store
            .read_manifest()?
            .ok_or(Error::Quarantined("manifest missing"))?;
        let mut manifest = parse_manifest(&bytes)?;
        let ids = store.segment_ids()?;
        let last = *ids.last().ok_or(Error::Quarantined("segments missing"))?;
        if last < manifest.active_segment
            || last > manifest.active_segment.saturating_add(1)
            || ids.iter().enumerate().any(|(i, &id)| id != (i as u64) + 1)
        {
            return Err(Error::Quarantined("segment gap or manifest disagreement"));
        }
        let mut position = Position::empty();
        let mut records = Vec::new();
        let mut frontier = None;
        let mut required_seen = required.is_none();
        let mut discarded = 0;
        for &id in &ids {
            let bytes = store.read_segment(id, manifest.config.segment_max_bytes)?;
            if bytes.is_empty() && id != last {
                return Err(Error::Quarantined("empty middle segment"));
            }
            position.segment = id;
            position.offset = 0;
            let mut open: Option<OpenBatch> = None;
            let mut torn = false;
            while (position.offset as usize) < bytes.len() {
                let remaining = &bytes[position.offset as usize..];
                if remaining.len() < frame::HEADER_BYTES {
                    let prefix = remaining.len().min(frame::MAGIC.len());
                    if remaining[..prefix] != frame::MAGIC[..prefix] {
                        return Err(Error::Quarantined("invalid partial frame magic"));
                    }
                    torn = true;
                    break;
                }
                let (h, total, payload_len) = frame::decode_header(remaining)?;
                if h.stream_epoch != manifest.config.stream_epoch
                    || h.domain != manifest.config.domain
                    || h.segment_id != id
                    || h.lsn != position.lsn
                    || h.previous_hash != position.hash
                {
                    return Err(Error::Quarantined("identity, LSN or physical hash chain"));
                }
                match (&open, h.frame_type) {
                    (None, FrameType::BatchBegin) => {
                        if h.record_seq
                            != position
                                .seq
                                .checked_add(1)
                                .ok_or(Error::Quarantined("seq overflow"))?
                            || h.batch_number
                                != position
                                    .batch
                                    .checked_add(1)
                                    .ok_or(Error::Quarantined("batch overflow"))?
                            || h.exec_epoch == 0
                            || h.exec_epoch < position.epoch
                            || payload_len != 0
                        {
                            return Err(Error::Quarantined("batch begin or epoch regression"));
                        }
                    }
                    (Some(batch), FrameType::DataRecord) => {
                        let next = position
                            .seq
                            .checked_add(batch.records.len() as u64)
                            .and_then(|n| n.checked_add(1))
                            .ok_or(Error::Quarantined("seq overflow"))?;
                        if h.record_seq != next
                            || h.batch_number != batch.number
                            || h.exec_epoch != batch.epoch
                            || batch.records.len() >= BATCH_MAX_RECORDS
                        {
                            return Err(Error::Quarantined("record sequence or batch identity"));
                        }
                    }
                    (Some(batch), FrameType::BatchSeal) => {
                        let end = position
                            .seq
                            .checked_add(batch.records.len() as u64)
                            .ok_or(Error::Quarantined("seq overflow"))?;
                        if batch.records.is_empty()
                            || h.record_seq != end
                            || h.batch_number != batch.number
                            || h.exec_epoch != batch.epoch
                            || payload_len != 0
                        {
                            return Err(Error::Quarantined("batch seal"));
                        }
                    }
                    _ => return Err(Error::Quarantined("batch structure")),
                }
                if remaining.len() < total {
                    torn = true;
                    break;
                }
                let decoded = frame::decode(remaining)?;
                match h.frame_type {
                    FrameType::BatchBegin => {
                        open = Some(OpenBatch {
                            before: position.clone(),
                            epoch: h.exec_epoch,
                            number: h.batch_number,
                            records: Vec::new(),
                        })
                    }
                    FrameType::DataRecord => open.as_mut().unwrap().records.push(Record {
                        seq: h.record_seq,
                        exec_epoch: h.exec_epoch,
                        payload: decoded.payload.to_vec(),
                    }),
                    FrameType::BatchSeal => {}
                    _ => unreachable!("frame kind was validated above"),
                }
                position.offset = position
                    .offset
                    .checked_add(total as u64)
                    .ok_or(Error::Quarantined("offset overflow"))?;
                position.lsn = position
                    .lsn
                    .checked_add(total as u64)
                    .ok_or(Error::Quarantined("LSN overflow"))?;
                position.hash = decoded.hash;
                if let Some(batch) = &open
                    && position.offset - batch.before.offset > BATCH_MAX_BYTES as u64
                {
                    return Err(Error::Quarantined("batch capacity"));
                }
                if h.frame_type == FrameType::BatchSeal {
                    let batch = open.take().unwrap();
                    position.seq = h.record_seq;
                    position.epoch = h.exec_epoch;
                    position.batch = h.batch_number;
                    records.extend(batch.records);
                    let closed = position.frontier(&manifest.config);
                    if required == Some(&closed) {
                        required_seen = true;
                    }
                    frontier = Some(closed);
                }
            }
            if torn || open.is_some() {
                if id != last {
                    return Err(Error::Quarantined("unsealed middle batch"));
                }
                if let Some(batch) = open {
                    position = batch.before;
                }
                discarded = bytes.len() as u64 - position.offset;
            }
        }
        if !required_seen {
            return Err(Error::Quarantined(
                "trusted physical frontier missing or changed",
            ));
        }
        // All validation, including the trusted prefix, precedes any mutation.
        if discarded > 0 {
            store.truncate_segment(last, position.offset)?;
        }
        for &id in &ids {
            store.sync_segment(id)?;
        }
        if last != manifest.active_segment {
            manifest.active_segment = last;
            manifest.generation = manifest
                .generation
                .checked_add(1)
                .ok_or(Error::Quarantined("manifest generation overflow"))?;
            store.publish_manifest(&manifest_bytes(&manifest)?)?;
        }
        let recovery = Recovery {
            records,
            frontier,
            discarded_tail_bytes: discarded,
        };
        Ok((
            Self {
                store,
                manifest,
                position,
                poisoned: false,
                durable: recovery.frontier.clone(),
            },
            recovery,
        ))
    }
    pub fn append_batch(
        &mut self,
        exec_epoch: u64,
        payloads: &[&[u8]],
    ) -> Result<PhysicalSyncReceipt, Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if exec_epoch == 0
            || exec_epoch < self.position.epoch
            || payloads.is_empty()
            || payloads.len() > BATCH_MAX_RECORDS
        {
            return Err(Error::Invalid("epoch or batch count"));
        }
        let mut size = 2 * (frame::HEADER_BYTES + frame::TRAILER_BYTES);
        for payload in payloads {
            if payload.len() > frame::FRAME_MAX_BYTES - frame::HEADER_BYTES - frame::TRAILER_BYTES {
                return Err(Error::Invalid("frame capacity"));
            }
            size = size
                .checked_add((frame::HEADER_BYTES + payload.len() + frame::TRAILER_BYTES + 7) & !7)
                .ok_or(Error::Invalid("batch size overflow"))?;
        }
        if size > BATCH_MAX_BYTES || size as u64 > self.manifest.config.segment_max_bytes {
            return Err(Error::Invalid("batch exceeds whole-segment capacity"));
        }
        let mut next = self.position.clone();
        let rotate = next.offset + size as u64 > self.manifest.config.segment_max_bytes;
        if rotate {
            next.segment = next
                .segment
                .checked_add(1)
                .ok_or(Error::Invalid("segment overflow"))?;
            next.offset = 0;
        }
        let first = next
            .seq
            .checked_add(1)
            .ok_or(Error::Invalid("sequence overflow"))?;
        let last = next
            .seq
            .checked_add(payloads.len() as u64)
            .ok_or(Error::Invalid("sequence overflow"))?;
        let batch = next
            .batch
            .checked_add(1)
            .ok_or(Error::Invalid("batch overflow"))?;
        let mut bytes = Vec::with_capacity(size);
        for index in 0..payloads.len() + 2 {
            let (kind, seq, payload) = if index == 0 {
                (FrameType::BatchBegin, first, &b""[..])
            } else if index == payloads.len() + 1 {
                (FrameType::BatchSeal, last, &b""[..])
            } else {
                (
                    FrameType::DataRecord,
                    first + (index as u64) - 1,
                    payloads[index - 1],
                )
            };
            let h = Header {
                frame_type: kind,
                flags: 0,
                stream_epoch: self.manifest.config.stream_epoch,
                domain: self.manifest.config.domain,
                segment_id: next.segment,
                lsn: next.lsn,
                record_seq: seq,
                exec_epoch,
                batch_number: batch,
                previous_hash: next.hash,
            };
            let encoded = frame::encode(&h, payload)?;
            next.hash = encoded[encoded.len() - 40..encoded.len() - 8]
                .try_into()
                .unwrap();
            next.lsn = next
                .lsn
                .checked_add(encoded.len() as u64)
                .ok_or(Error::Invalid("LSN overflow"))?;
            next.offset += encoded.len() as u64;
            bytes.extend(encoded);
        }
        next.seq = last;
        next.batch = batch;
        next.epoch = exec_epoch;
        let mut manifest = self.manifest.clone();
        if rotate {
            manifest.active_segment = next.segment;
            manifest.generation = manifest
                .generation
                .checked_add(1)
                .ok_or(Error::Invalid("manifest generation overflow"))?;
        }
        let io_result = (|| -> Result<(), Error> {
            if rotate {
                self.store.create_segment(next.segment)?;
                self.store.publish_manifest(&manifest_bytes(&manifest)?)?;
            }
            self.store
                .append_segment(next.segment, next.offset - size as u64, &bytes)?;
            self.store.sync_segment(next.segment)?;
            Ok(())
        })();
        if let Err(error) = io_result {
            self.poisoned = true;
            return Err(error);
        }
        self.position = next;
        self.manifest = manifest;
        let frontier = self.position.frontier(&self.manifest.config);
        self.durable = Some(frontier.clone());
        Ok(PhysicalSyncReceipt { frontier })
    }
    pub fn durable_frontier(&self) -> Option<&PhysicalFrontier> {
        self.durable.as_ref()
    }
    /// Export only bytes covered by a completed synchronization. The capability
    /// cannot be constructed from caller-supplied records or a naked watermark.
    pub fn export_durable(
        &mut self,
        first_seq: u64,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<DurableExport, Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if first_seq == 0
            || max_records == 0
            || max_records > BATCH_MAX_RECORDS
            || max_bytes == 0
            || max_bytes > BATCH_MAX_BYTES
        {
            return Err(Error::Invalid("export bounds"));
        }
        let frontier = self.durable.clone();
        let terminal = frontier.as_ref().map_or(0, |f| f.last_seq);
        if first_seq > terminal.saturating_add(1) {
            return Err(Error::Invalid("export sequence gap"));
        }
        let mut records = Vec::new();
        let mut retained = 0usize;
        let mut full = false;
        let mut hash = [0; 32];
        let mut lsn = 0u64;
        let mut seq = 0u64;
        let mut batch = 0u64;
        let mut epoch = 0u64;
        let mut open: Option<(u64, u64, u64)> = None;
        if let Some(end) = &frontier {
            for id in 1..=end.segment_id {
                let bytes = self
                    .store
                    .read_segment(id, self.manifest.config.segment_max_bytes)?;
                let limit = if id == end.segment_id {
                    usize::try_from(end.end_offset)
                        .map_err(|_| Error::Quarantined("export offset"))?
                } else {
                    bytes.len()
                };
                if limit > bytes.len() || limit == 0 {
                    return Err(Error::Quarantined("export missing durable bytes"));
                }
                let mut offset = 0usize;
                while offset < limit {
                    let f = frame::decode(&bytes[offset..limit])?;
                    let h = &f.header;
                    if h.stream_epoch != self.manifest.config.stream_epoch
                        || h.domain != self.manifest.config.domain
                        || h.segment_id != id
                        || h.lsn != lsn
                        || h.previous_hash != hash
                    {
                        return Err(Error::Quarantined("export physical chain"));
                    }
                    match (open, h.frame_type) {
                        (None, FrameType::BatchBegin)
                            if h.record_seq
                                == seq
                                    .checked_add(1)
                                    .ok_or(Error::Quarantined("export sequence"))?
                                && h.batch_number
                                    == batch
                                        .checked_add(1)
                                        .ok_or(Error::Quarantined("export batch"))?
                                && h.exec_epoch > 0
                                && h.exec_epoch >= epoch
                                && f.payload.is_empty() =>
                        {
                            open = Some((h.batch_number, h.exec_epoch, 0));
                        }
                        (Some((number, owner, count)), FrameType::DataRecord)
                            if h.batch_number == number
                                && h.exec_epoch == owner
                                && h.record_seq
                                    == seq
                                        .checked_add(1)
                                        .ok_or(Error::Quarantined("export sequence"))?
                                && count < BATCH_MAX_RECORDS as u64 =>
                        {
                            seq = h.record_seq;
                            open = Some((number, owner, count + 1));
                            if seq >= first_seq && !full {
                                if records.len() >= max_records
                                    || retained
                                        .checked_add(f.payload.len())
                                        .is_none_or(|n| n > max_bytes)
                                {
                                    full = true;
                                } else {
                                    retained += f.payload.len();
                                    records.push(Record {
                                        seq,
                                        exec_epoch: owner,
                                        payload: f.payload.to_vec(),
                                    });
                                }
                            }
                        }
                        (Some((number, owner, count)), FrameType::BatchSeal)
                            if count > 0
                                && h.batch_number == number
                                && h.exec_epoch == owner
                                && h.record_seq == seq
                                && f.payload.is_empty() =>
                        {
                            batch = number;
                            epoch = owner;
                            open = None;
                        }
                        _ => return Err(Error::Quarantined("export batch structure")),
                    }
                    offset = offset
                        .checked_add(f.total_len)
                        .ok_or(Error::Quarantined("export offset"))?;
                    lsn = lsn
                        .checked_add(f.total_len as u64)
                        .ok_or(Error::Quarantined("export LSN"))?;
                    hash = f.hash;
                }
                if open.is_some() {
                    return Err(Error::Quarantined("export unsealed segment"));
                }
            }
            if seq != end.last_seq
                || hash != end.seal_hash
                || lsn != end.end_lsn
                || batch != end.batch_number
                || epoch != end.exec_epoch
            {
                return Err(Error::Quarantined("export frontier changed"));
            }
        }
        if first_seq <= terminal && records.is_empty() {
            return Err(Error::Invalid("export record exceeds requested byte bound"));
        }
        Ok(DurableExport {
            config: self.manifest.config.clone(),
            frontier,
            records,
        })
    }
    /// Consumes the writer and keeps its storage lock. The returned owner has no
    /// append/into_store method; fencing of other execution outputs is separate.
    pub fn close_for_recovery(self) -> Result<ClosedJournal<S>, Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        Ok(ClosedJournal { journal: self })
    }
    pub fn into_store(self) -> S {
        self.store
    }
    /// The persisted stream identity, including when the journal is empty.
    pub fn config(&self) -> &Config {
        &self.manifest.config
    }
}

/// Content read back from a journal's synchronized prefix.
pub struct DurableExport {
    config: Config,
    frontier: Option<PhysicalFrontier>,
    records: Vec<Record>,
}
impl DurableExport {
    pub fn config(&self) -> &Config {
        &self.config
    }
    pub fn frontier(&self) -> Option<&PhysicalFrontier> {
        self.frontier.as_ref()
    }
    pub fn records(&self) -> &[Record] {
        &self.records
    }
}
pub struct ClosedJournal<S: Store> {
    journal: Journal<S>,
}
impl<S: Store> ClosedJournal<S> {
    pub fn config(&self) -> &Config {
        self.journal.config()
    }
    pub fn durable_frontier(&self) -> Option<&PhysicalFrontier> {
        self.journal.durable_frontier()
    }
    pub fn export_durable(
        &mut self,
        first: u64,
        records: usize,
        bytes: usize,
    ) -> Result<DurableExport, Error> {
        self.journal.export_durable(first, records, bytes)
    }
}
