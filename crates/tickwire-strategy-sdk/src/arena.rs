//! Pure in-memory shared-memory arena (`memfd`) for optional oversize `IntentRecord` and
//! `ReportRecord` payloads (`payload_kind == 2`, zero disk I/O).
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]

use std::os::fd::OwnedFd;

use sha2::{Digest, Sha256};
use tickwire_ipc::{
    ArenaBlockHeader, ArenaClass, IntentRecord, RecordHeader, ReportRecord, ResourceHeader, Wire,
    validate_intent_arena_payload, validate_report_arena_payload,
};
use tickwire_shm_sys::{PAGE_BYTES, ReaderMapping, WriterMapping};

use crate::{Error, Result};

const HEADER_BYTES: usize = 4096;
const BLOCK_HEADER_BYTES: u32 = 128;
const MAX_ARENA_PAYLOAD_BYTES: u32 = 65_536;

/// Allocation metadata returned by [`ShmArenaWriter::write_payload`] prior to ring publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArenaSliceMeta {
    pub class_id: u16,
    pub payload_offset: u64,
    pub payload_length: u32,
    pub allocation_seq: u64,
    pub payload_crc32c: u32,
}

#[derive(Debug, Clone)]
struct ClassTracker {
    descriptor: ArenaClass,
    next_cursor: u32,
    block_sequences: Vec<u64>,
}

/// Configure ordered arena classes (`(payload_limit, block_count)`) onto an in-memory arena
/// [`ResourceHeader`], rounding `arena_bytes` and `resource_bytes` up to page boundaries (`4096B`)
/// with zero disk backing.
#[forbid(unsafe_code)]
pub fn configure_shm_arena_header(
    resource_id: [u8; 16],
    channel_id: [u8; 16],
    producer_instance: [u8; 16],
    consumer_instance: [u8; 16],
    ring_epoch: u64,
    producer_generation: u64,
    classes: &[(u32, u32)],
) -> Result<ResourceHeader> {
    if classes.is_empty()
        || classes.len() > 4
        || resource_id == [0; 16]
        || channel_id == [0; 16]
        || producer_instance == [0; 16]
        || ring_epoch == 0
        || ring_epoch >= (1 << 63)
        || producer_generation == 0
        || producer_generation >= (1 << 63)
    {
        return Err(Error::Identity);
    }

    let arch = if cfg!(target_arch = "x86_64") {
        1
    } else if cfg!(target_arch = "aarch64") {
        2
    } else {
        return Err(Error::Identity);
    };

    let mut descriptors: [ArenaClass; 4] = core::array::from_fn(|_| ArenaClass::default());
    let arena_offset = HEADER_BYTES as u64;
    let mut cursor = arena_offset;
    let mut prev_limit = 0u32;

    for (idx, &(payload_limit, block_count)) in classes.iter().enumerate() {
        if payload_limit == 0
            || payload_limit > MAX_ARENA_PAYLOAD_BYTES
            || payload_limit <= prev_limit
            || block_count == 0
        {
            return Err(Error::Bounds);
        }
        prev_limit = payload_limit;
        let block_bytes = BLOCK_HEADER_BYTES
            .checked_add(payload_limit)
            .ok_or(Error::Bounds)?
            .div_ceil(128)
            .checked_mul(128)
            .ok_or(Error::Bounds)?;
        let length = u64::from(block_bytes)
            .checked_mul(u64::from(block_count))
            .ok_or(Error::Bounds)?;
        descriptors[idx] = ArenaClass {
            class_id: (idx + 1) as u16,
            block_bytes,
            block_count,
            payload_limit,
            offset: cursor,
            length,
        };
        cursor = cursor.checked_add(length).ok_or(Error::Bounds)?;
    }

    let raw_arena_bytes = cursor.checked_sub(arena_offset).ok_or(Error::Bounds)?;
    let page = PAGE_BYTES as u64;
    let arena_bytes = raw_arena_bytes
        .div_ceil(page)
        .checked_mul(page)
        .ok_or(Error::Bounds)?;
    let resource_bytes = arena_offset.checked_add(arena_bytes).ok_or(Error::Bounds)?;

    let mut header = ResourceHeader {
        magic: *b"TWIPC001",
        abi_major: 1,
        abi_minor: 0,
        header_bytes: HEADER_BYTES as u32,
        resource_kind: 5,
        target_arch: arch,
        endian: 1,
        atomic_width: 8,
        page_shift: 12,
        isolation_shift: 7,
        resource_bytes,
        resource_id,
        channel_id,
        producer_instance,
        consumer_instance,
        channel_epoch: 1,
        ring_epoch,
        producer_generation,
        arena_offset,
        arena_bytes,
        required_features: 0x1c | 2,
        max_payload: prev_limit,
        max_consumers: 1,
        schema_major: 1,
        schema_minor: 0,
        arena_class_count: classes.len() as u16,
        clock_domain: 1,
        class_descriptors: descriptors,
        ..Default::default()
    };
    seal_header_hash(&mut header)?;
    Ok(header)
}

#[forbid(unsafe_code)]
fn seal_header_hash(header: &mut ResourceHeader) -> Result<[u8; HEADER_BYTES]> {
    let mut bytes = [0u8; HEADER_BYTES];
    header.descriptor_hash = [0; 32];
    header.encode(&mut bytes)?;
    header.descriptor_hash = Sha256::digest(bytes).into();
    header.encode(&mut bytes)?;
    Ok(bytes)
}

#[forbid(unsafe_code)]
fn verify_arena_header(header: &ResourceHeader) -> Result<()> {
    if header.magic != *b"TWIPC001"
        || header.abi_major != 1
        || header.abi_minor != 0
        || header.header_bytes != HEADER_BYTES as u32
        || header.arena_class_count == 0
        || header.arena_class_count > 4
        || header.arena_offset < HEADER_BYTES as u64
        || header.arena_bytes == 0
        || !header.resource_bytes.is_multiple_of(PAGE_BYTES as u64)
    {
        return Err(Error::Identity);
    }
    let mut checked = header.clone();
    seal_header_hash(&mut checked)?;
    if checked.descriptor_hash != header.descriptor_hash {
        return Err(Error::Identity);
    }
    let arena_end = header
        .arena_offset
        .checked_add(header.arena_bytes)
        .ok_or(Error::Bounds)?;
    if arena_end > header.resource_bytes {
        return Err(Error::Bounds);
    }
    let mut prev_limit = 0u32;
    for (idx, class) in header.class_descriptors[..header.arena_class_count as usize]
        .iter()
        .enumerate()
    {
        if class.class_id != (idx + 1) as u16
            || class.payload_limit == 0
            || class.payload_limit > MAX_ARENA_PAYLOAD_BYTES
            || class.payload_limit <= prev_limit
            || class.block_count == 0
            || class.block_bytes < BLOCK_HEADER_BYTES
            || !class.block_bytes.is_multiple_of(128)
            || class.length != u64::from(class.block_bytes) * u64::from(class.block_count)
            || class.offset < header.arena_offset
            || class
                .offset
                .checked_add(class.length)
                .ok_or(Error::Bounds)?
                > arena_end
        {
            return Err(Error::Bounds);
        }
        prev_limit = class.payload_limit;
    }
    Ok(())
}

/// Validate and borrow an oversize [`IntentRecord`] payload against an immutable arena byte image.
#[forbid(unsafe_code)]
pub fn resolve_intent_arena_slice<'a>(
    record: &IntentRecord,
    arena_image: &'a [u8],
    arena_header: &ResourceHeader,
) -> Result<&'a [u8]> {
    Ok(validate_intent_arena_payload(
        record,
        arena_image,
        arena_header,
    )?)
}

/// Validate and borrow an oversize [`ReportRecord`] payload against an immutable arena byte image.
#[forbid(unsafe_code)]
pub fn resolve_report_arena_slice<'a>(
    record: &ReportRecord,
    arena_image: &'a [u8],
    arena_header: &ResourceHeader,
) -> Result<&'a [u8]> {
    Ok(validate_report_arena_payload(
        record,
        arena_image,
        arena_header,
    )?)
}

/// Single-writer RAM-backed (`memfd`) shared-memory arena for optional oversize payloads.
pub struct ShmArenaWriter {
    header: ResourceHeader,
    mapping: WriterMapping,
    classes: Vec<ClassTracker>,
    scratch: Vec<u8>,
}

impl ShmArenaWriter {
    #[allow(unsafe_code)]
    pub fn create(mut header: ResourceHeader) -> Result<Self> {
        let header_bytes = seal_header_hash(&mut header)?;
        verify_arena_header(&header)?;
        let len = usize::try_from(header.resource_bytes).map_err(|_| Error::Bounds)?;
        let mut mapping = WriterMapping::create("tickwire-arena", len)?;
        // SAFETY: `mapping` is freshly created, not yet exported to any reader, and `0..4096`
        // holds the immutable `ResourceHeader` for the lifetime of the sealed mapping.
        unsafe {
            mapping.write_bytes(0, &header_bytes)?;
        }
        mapping.seal_for_readers()?;
        let mut max_block = BLOCK_HEADER_BYTES as usize;
        let mut classes = Vec::with_capacity(header.arena_class_count as usize);
        for desc in &header.class_descriptors[..header.arena_class_count as usize] {
            max_block = max_block.max(desc.block_bytes as usize);
            classes.push(ClassTracker {
                descriptor: desc.clone(),
                next_cursor: 0,
                block_sequences: vec![0; desc.block_count as usize],
            });
        }
        Ok(Self {
            header,
            mapping,
            classes,
            scratch: vec![0u8; max_block],
        })
    }

    #[forbid(unsafe_code)]
    pub fn header(&self) -> &ResourceHeader {
        &self.header
    }

    #[forbid(unsafe_code)]
    pub fn reader_fd(&self) -> Result<OwnedFd> {
        Ok(self.mapping.duplicate_fd()?)
    }

    /// Reclaim blocks whose owning ring `sequence` is `<= released_seq`.
    #[forbid(unsafe_code)]
    pub fn reclaim(&mut self, released_seq: u64) {
        for class in &mut self.classes {
            for slot_seq in &mut class.block_sequences {
                if *slot_seq != 0 && *slot_seq <= released_seq {
                    *slot_seq = 0;
                }
            }
        }
    }

    /// Write an oversize payload into an unclaimed arena block bound to `sequence` prior to
    /// publishing the referencing `IntentRecord` or `ReportRecord`.
    #[allow(unsafe_code)]
    pub fn write_payload(
        &mut self,
        sequence: u64,
        released_seq: u64,
        payload: &[u8],
    ) -> Result<ArenaSliceMeta> {
        if sequence == 0
            || sequence >= (1 << 63)
            || payload.is_empty()
            || payload.len() > MAX_ARENA_PAYLOAD_BYTES as usize
        {
            return Err(Error::Bounds);
        }
        self.reclaim(released_seq);
        let payload_len = payload.len() as u32;
        let class = self
            .classes
            .iter_mut()
            .find(|c| payload_len <= c.descriptor.payload_limit)
            .ok_or(Error::Bounds)?;

        let count = class.descriptor.block_count as usize;
        let mut chosen = None;
        for step in 0..count {
            let idx = (class.next_cursor as usize + step) % count;
            if class.block_sequences[idx] == 0 {
                chosen = Some(idx);
                class.next_cursor = ((idx + 1) % count) as u32;
                break;
            }
        }
        let block_idx = chosen.ok_or(Error::Ring(tickwire_shm::MappedError::Protocol(
            tickwire_shm::Error::ArenaExhausted,
        )))?;

        let payload_crc32c = {
            #[cfg(feature = "crc32")]
            {
                tickwire_ipc::crc32c(payload)
            }
            #[cfg(not(feature = "crc32"))]
            {
                0
            }
        };

        let block_bytes = class.descriptor.block_bytes as usize;
        let buf = &mut self.scratch[..block_bytes];
        buf.fill(0);
        ArenaBlockHeader {
            allocation_seq: sequence,
            payload_length: payload_len,
            class_id: class.descriptor.class_id,
            ring_epoch: self.header.ring_epoch,
            resource_id: self.header.resource_id,
            payload_crc32c,
        }
        .encode(&mut buf[..BLOCK_HEADER_BYTES as usize])?;
        buf[BLOCK_HEADER_BYTES as usize..BLOCK_HEADER_BYTES as usize + payload.len()]
            .copy_from_slice(payload);

        let block_offset = class
            .descriptor
            .offset
            .checked_add(u64::from(block_idx as u32) * u64::from(class.descriptor.block_bytes))
            .ok_or(Error::Bounds)?;
        let offset_usize = usize::try_from(block_offset).map_err(|_| Error::Bounds)?;
        // SAFETY: `block_sequences[block_idx] == 0` guarantees all prior readers have released
        // any earlier borrow of this block (`<= released_seq`), and no published ring slot
        // references `sequence` until the caller commits the ring write after this method returns.
        unsafe {
            self.mapping.write_bytes(offset_usize, buf)?;
        }
        class.block_sequences[block_idx] = sequence;

        Ok(ArenaSliceMeta {
            class_id: class.descriptor.class_id,
            payload_offset: block_offset + u64::from(BLOCK_HEADER_BYTES),
            payload_length: payload_len,
            allocation_seq: sequence,
            payload_crc32c,
        })
    }

    /// Copy an immutable byte snapshot of the entire arena mapping when no concurrent write is in flight.
    #[allow(unsafe_code)]
    pub fn snapshot_image(&self) -> Result<Vec<u8>> {
        let len = self.mapping.len();
        // SAFETY: `&self` serializes against `write_payload(&mut self)`, so the sole writer
        // cannot mutate any byte in `self.mapping` during this copy.
        let bytes = unsafe { self.mapping.read_bytes(0, len)? };
        Ok(bytes.to_vec())
    }
}

/// Read-only `PROT_READ` mapping of a sealed [`ShmArenaWriter`] `memfd`.
pub struct ShmArenaReader {
    header: ResourceHeader,
    mapping: ReaderMapping,
}

impl ShmArenaReader {
    /// Attach to an authenticated, `F_SEAL_FUTURE_WRITE`-sealed arena `memfd`.
    ///
    /// # Safety
    /// The peer writer must obey the [`ShmArenaWriter`] release-gated block reclamation protocol
    /// so any block referenced by a live `RecordBorrow` remains immutable until the guard drops.
    #[allow(unsafe_code)]
    pub unsafe fn attach(fd: OwnedFd, expected_header: ResourceHeader) -> Result<Self> {
        verify_arena_header(&expected_header)?;
        let len = usize::try_from(expected_header.resource_bytes).map_err(|_| Error::Bounds)?;
        let mapping = ReaderMapping::map(fd, len)?;
        // SAFETY: the first page (`0..4096`) is written before sealing and never mutated.
        let actual_raw = unsafe { mapping.read_bytes(0, HEADER_BYTES)? };
        let actual = ResourceHeader::decode(actual_raw)?;
        if actual != expected_header {
            return Err(Error::Identity);
        }
        Ok(Self {
            header: expected_header,
            mapping,
        })
    }

    #[forbid(unsafe_code)]
    pub fn header(&self) -> &ResourceHeader {
        &self.header
    }

    /// Validate and borrow the single retained arena block referenced by `record_header`.
    ///
    /// # Safety
    /// Caller must hold a live `RecordBorrow` for `record_header.sequence` on the paired ring so
    /// the writer cannot reclaim this block for the lifetime `'a`.
    #[allow(unsafe_code)]
    pub(crate) unsafe fn read_retained_payload<'a>(
        &'a self,
        record_header: &RecordHeader,
    ) -> Result<&'a [u8]> {
        if record_header.payload_kind != 2
            || record_header.payload_length == 0
            || record_header.payload_length > MAX_ARENA_PAYLOAD_BYTES
            || record_header.allocation_seq != record_header.sequence
        {
            return Err(Error::Wire(tickwire_ipc::Error::Malformed));
        }
        let class = self.header.class_descriptors[..self.header.arena_class_count as usize]
            .iter()
            .find(|c| record_header.payload_length <= c.payload_limit)
            .ok_or(Error::Wire(tickwire_ipc::Error::Bounds))?;
        if class.class_id != record_header.arena_class {
            return Err(Error::Wire(tickwire_ipc::Error::Malformed));
        }
        let block_offset = record_header
            .payload_offset
            .checked_sub(u64::from(BLOCK_HEADER_BYTES))
            .ok_or(Error::Wire(tickwire_ipc::Error::Bounds))?;
        let relative = block_offset
            .checked_sub(class.offset)
            .ok_or(Error::Wire(tickwire_ipc::Error::Bounds))?;
        if relative % u64::from(class.block_bytes) != 0
            || relative / u64::from(class.block_bytes) >= u64::from(class.block_count)
        {
            return Err(Error::Wire(tickwire_ipc::Error::Bounds));
        }
        let offset_usize =
            usize::try_from(block_offset).map_err(|_| Error::Wire(tickwire_ipc::Error::Bounds))?;
        // SAFETY: only the single block bound to `record_header.sequence` is borrowed; caller's
        // live ring `RecordBorrow` prevents `ShmArenaWriter` from reclaiming or writing it.
        let block = unsafe {
            self.mapping
                .read_bytes(offset_usize, class.block_bytes as usize)?
        };
        let block_hdr = ArenaBlockHeader::decode(&block[..BLOCK_HEADER_BYTES as usize])?;
        if block_hdr.resource_id != self.header.resource_id
            || block_hdr.ring_epoch != self.header.ring_epoch
            || block_hdr.allocation_seq != record_header.sequence
            || block_hdr.payload_length != record_header.payload_length
            || block_hdr.class_id != class.class_id
        {
            return Err(Error::Wire(tickwire_ipc::Error::Identity));
        }
        let payload_end = BLOCK_HEADER_BYTES as usize + record_header.payload_length as usize;
        if block[payload_end..].iter().any(|&b| b != 0) {
            return Err(Error::Wire(tickwire_ipc::Error::ReservedNonzero));
        }
        if block_hdr.payload_crc32c != record_header.payload_crc32c {
            return Err(Error::Wire(tickwire_ipc::Error::Checksum));
        }
        let payload = &block[BLOCK_HEADER_BYTES as usize..payload_end];
        #[cfg(feature = "crc32")]
        if record_header.flags & 8 != 0
            && tickwire_ipc::crc32c(payload) != record_header.payload_crc32c
        {
            return Err(Error::Wire(tickwire_ipc::Error::Checksum));
        }
        Ok(payload)
    }
}

/// Create an in-process `(ShmArenaWriter, ShmArenaReader)` pair over a sealed `memfd`.
#[allow(unsafe_code)]
pub fn in_process_arena_pair(header: ResourceHeader) -> Result<(ShmArenaWriter, ShmArenaReader)> {
    let writer = ShmArenaWriter::create(header)?;
    let fd = writer.reader_fd()?;
    let expected = writer.header().clone();
    // SAFETY: `writer` is freshly created and obeys the release-gated reclamation protocol.
    let reader = unsafe { ShmArenaReader::attach(fd, expected)? };
    Ok((writer, reader))
}
