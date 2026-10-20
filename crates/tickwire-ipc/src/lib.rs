//! Fixed-layout Tickwire wire ABI. Live shared-memory access is outside this crate.
#![forbid(unsafe_code)]

mod wire;
pub use wire::*;
mod config;
pub use config::*;
mod snapshot;
pub use snapshot::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Malformed,
    AbiMajor,
    AbiMinor,
    UnsupportedFeature,
    UnsupportedArchitecture,
    Bounds,
    Checksum,
    ReservedNonzero,
    UnknownMessage,
    InvalidEnum,
    Identity,
    VersionConflict,
    NotReady,
    Deadline,
    MissingResource,
    ResyncRequired,
    RecoveryEvidence,
    Permission,
    UnsupportedOperation,
    SequenceExhausted,
}
pub type Result<T> = core::result::Result<T, Error>;
impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

/// Layout codec for stable, owned byte images. It is not a shared-memory access API.
pub trait Wire: Sized {
    const SIZE: usize;
    fn decode(raw: &[u8]) -> Result<Self>;
    fn encode(&self, out: &mut [u8]) -> Result<()>;
}

fn zero(bytes: &[u8]) -> Result<()> {
    if bytes.iter().any(|&b| b != 0) {
        Err(Error::ReservedNonzero)
    } else {
        Ok(())
    }
}
fn uint(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .enumerate()
        .fold(0, |v, (i, b)| v | (*b as u64) << (i * 8))
}
fn bounded(bytes: &[u8], offset: u64, length: u64) -> Result<&[u8]> {
    let end = offset.checked_add(length).ok_or(Error::Bounds)?;
    let start = usize::try_from(offset).map_err(|_| Error::Bounds)?;
    let end = usize::try_from(end).map_err(|_| Error::Bounds)?;
    bytes.get(start..end).ok_or(Error::Bounds)
}
fn is_zero(bytes: &[u8]) -> bool {
    bytes.iter().all(|&b| b == 0)
}
fn seq(value: u64) -> Result<()> {
    if value >= 1 << 63 {
        Err(Error::SequenceExhausted)
    } else {
        Ok(())
    }
}
fn schema(major: u16, minor: u16) -> Result<()> {
    if major != 1 {
        return Err(Error::AbiMajor);
    }
    if minor != 0 {
        return Err(Error::AbiMinor);
    }
    Ok(())
}

/// Castagnoli CRC, reflected polynomial 0x82f63b78, init/xorout 0xffffffff.
pub fn crc32c(bytes: &[u8]) -> u32 {
    crc_masked(bytes, 0..0)
}
fn crc_masked(bytes: &[u8], zeros: core::ops::Range<usize>) -> u32 {
    let mut crc = !0u32;
    for (i, &byte) in bytes.iter().enumerate() {
        crc ^= if zeros.contains(&i) { 0 } else { byte as u32 };
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f63b78 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // Fixed stack storage avoids heap allocation on record decode.
pub enum Record {
    Market(MarketRecord),
    Intent(IntentRecord),
    Report(ReportRecord),
    Ownership(OwnershipRecord),
}

/// Decode an entire fixed record. Arena-backed depth additionally needs `validate_arena_payload`.
pub fn decode_record(raw: &[u8]) -> Result<Record> {
    let h = RecordHeader::decode(bounded(raw, 0, 128)?)?;
    schema(h.schema_major, h.schema_minor)?;
    if h.header_bytes != 128
        || h.sequence == 0
        || h.topic_or_scope_id == 0
        || h.producer_generation == 0
    {
        return Err(Error::Malformed);
    }
    for value in [
        h.sequence,
        h.feed_generation,
        h.producer_generation,
        h.instrument_version,
        h.allocation_seq,
    ] {
        seq(value)?;
    }
    if h.flags & !15 != 0 || h.payload_kind > 2 {
        return Err(Error::InvalidEnum);
    }
    for (bit, value) in [
        (0, h.source_timestamp_ns),
        (1, h.ingress_mono_ns),
        (2, h.publish_mono_ns),
    ] {
        if h.flags & (1 << bit) == 0 && value != 0 {
            return Err(Error::Malformed);
        }
    }
    let size = match h.message_type {
        257..=263 | 769..=779 => 512,
        513..=520 => 1024,
        _ => return Err(Error::UnknownMessage),
    };
    if raw.len() != size || h.body_bytes as usize != size - 128 {
        return Err(Error::Bounds);
    }
    if h.flags & 8 == 0 {
        if h.record_crc32c != 0 || h.payload_crc32c != 0 {
            return Err(Error::Checksum);
        }
    } else if crc_masked(raw, 112..120) != h.record_crc32c {
        return Err(Error::Checksum);
    }
    validate_fields(h.message_type, &raw[128..])?;
    if h.message_type != 259
        && (h.payload_kind != 0
            || h.payload_length != 0
            || h.payload_offset != 0
            || h.arena_class != 0
            || h.allocation_seq != 0
            || h.payload_crc32c != 0)
    {
        return Err(Error::Malformed);
    }
    if h.message_type >= 513 && h.feed_generation != 0 {
        return Err(Error::Malformed);
    }
    match h.message_type {
        257..=263 => {
            let r = MarketRecord::decode(raw)?;
            let b = &r.body;
            if h.message_type == 259 {
                let count = b.bid_count.checked_add(b.ask_count).ok_or(Error::Bounds)?;
                if count != b.level_count
                    || count > 4096
                    || h.payload_length != count.checked_mul(16).ok_or(Error::Bounds)?
                {
                    return Err(Error::Malformed);
                }
                if count <= 12 {
                    if h.payload_kind != 1
                        || h.payload_offset != 0
                        || h.arena_class != 0
                        || h.allocation_seq != 0
                    {
                        return Err(Error::Malformed);
                    }
                    zero(&raw[320 + count as usize * 16..])?;
                    validate_levels(
                        &raw[320..320 + count as usize * 16],
                        b.bid_count,
                        b.ask_count,
                    )?;
                    if h.flags & 8 != 0
                        && crc32c(&raw[320..320 + count as usize * 16]) != h.payload_crc32c
                    {
                        return Err(Error::Checksum);
                    }
                } else {
                    if h.payload_kind != 2
                        || h.payload_offset < 128
                        || !(1..=4).contains(&h.arena_class)
                        || h.allocation_seq != h.sequence
                    {
                        return Err(Error::Malformed);
                    }
                    zero(&raw[320..])?;
                }
            }
            Ok(Record::Market(r))
        }
        513..=520 => {
            let r = IntentRecord::decode(raw)?;
            if r.body.trigger_count > 8 || r.body.reduce_only > 1 {
                return Err(Error::Bounds);
            }
            zero(&raw[512 + r.body.trigger_count as usize * 64..])?;
            for t in &r.body.triggers[..r.body.trigger_count as usize] {
                if t.topic_id == 0 || t.sequence == 0 || t.feed_generation == 0 {
                    return Err(Error::Identity);
                }
                seq(t.sequence)?;
            }
            Ok(Record::Intent(r))
        }
        779 => Ok(Record::Ownership(OwnershipRecord::decode(raw)?)),
        _ => {
            let r = ReportRecord::decode(raw)?;
            padded(
                &r.body.venue_order_id,
                r.body.venue_order_id_length as usize,
            )?;
            padded(
                &r.body.venue_trade_id,
                r.body.venue_trade_id_length as usize,
            )?;
            Ok(Record::Report(r))
        }
    }
}

fn padded(bytes: &[u8], length: usize) -> Result<&[u8]> {
    let value = bytes.get(..length).ok_or(Error::Bounds)?;
    zero(bytes.get(length..).ok_or(Error::Bounds)?)?;
    Ok(value)
}

fn validate_levels(raw: &[u8], bids: u32, asks: u32) -> Result<()> {
    let count = bids.checked_add(asks).ok_or(Error::Bounds)?;
    if raw.len() as u64 != u64::from(count) * 16 {
        return Err(Error::Bounds);
    }
    let mut previous = None;
    for (i, chunk) in raw.as_chunks::<16>().0.iter().enumerate() {
        let level = DepthLevel::decode(chunk)?;
        if level.qty_lots < 0 {
            return Err(Error::Malformed);
        }
        if i == bids as usize {
            previous = None;
        }
        if let Some(price) = previous
            && ((i < bids as usize && price <= level.price_ticks)
                || (i >= bids as usize && price >= level.price_ticks))
        {
            return Err(Error::Malformed);
        }
        previous = Some(level.price_ticks);
    }
    Ok(())
}

/// Immutable arena bytes after the caller has observed publication and retained its borrow.
pub fn validate_arena_payload(
    record: &MarketRecord,
    bank: &[u8],
    resource: &ResourceHeader,
) -> Result<()> {
    let mut image = [0u8; 512];
    record.encode(&mut image)?;
    decode_record(&image)?;
    let h = &record.header;
    if h.message_type != 259
        || h.payload_kind != 2
        || resource.arena_class_count > 4
        || h.arena_class == 0
    {
        return Err(Error::Malformed);
    }
    if resource.resource_bytes != bank.len() as u64 {
        return Err(Error::Bounds);
    }
    let mut selected = None;
    for class in &resource.class_descriptors[..resource.arena_class_count as usize] {
        if h.payload_length <= class.payload_limit {
            selected = Some(class);
            break;
        }
    }
    let class = selected.ok_or(Error::Bounds)?;
    if class.class_id != h.arena_class {
        return Err(Error::Malformed);
    }
    let offset = h.payload_offset.checked_sub(128).ok_or(Error::Bounds)?;
    let relative = offset.checked_sub(class.offset).ok_or(Error::Bounds)?;
    if class.block_bytes < 128
        || relative % u64::from(class.block_bytes) != 0
        || relative / u64::from(class.block_bytes) >= u64::from(class.block_count)
    {
        return Err(Error::Bounds);
    }
    if class.length != u64::from(class.block_bytes) * u64::from(class.block_count) {
        return Err(Error::Bounds);
    }
    let arena_end = resource
        .arena_offset
        .checked_add(resource.arena_bytes)
        .ok_or(Error::Bounds)?;
    if class.offset < resource.arena_offset
        || class
            .offset
            .checked_add(class.length)
            .ok_or(Error::Bounds)?
            > arena_end
    {
        return Err(Error::Bounds);
    }
    let bytes = bounded(bank, offset, u64::from(class.block_bytes))?;
    let block = ArenaBlockHeader::decode(&bytes[..128])?;
    if block.resource_id != resource.resource_id
        || block.ring_epoch != resource.ring_epoch
        || block.allocation_seq != h.sequence
        || h.allocation_seq != h.sequence
        || block.payload_length != h.payload_length
        || block.class_id != h.arena_class
    {
        return Err(Error::Identity);
    }
    let payload = bounded(bytes, 128, u64::from(h.payload_length))?;
    validate_levels(payload, record.body.bid_count, record.body.ask_count)?;
    zero(bounded(
        bytes,
        128 + u64::from(h.payload_length),
        u64::from(class.block_bytes)
            .checked_sub(128 + u64::from(h.payload_length))
            .ok_or(Error::Bounds)?,
    )?)?;
    if block.payload_crc32c != h.payload_crc32c
        || (h.flags & 8 != 0 && crc32c(payload) != h.payload_crc32c)
    {
        return Err(Error::Checksum);
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ControlFrame<'a> {
    pub envelope: ControlEnvelope,
    pub body: &'a [u8],
}
impl<'a> ControlFrame<'a> {
    /// Check negotiated feature requirements before decoding a configuration body.
    pub fn decode_negotiated(raw: &'a [u8], negotiated_features: u64) -> Result<Self> {
        let envelope = ControlEnvelope::decode(bounded(raw, 0, 128)?)?;
        if (80..=84).contains(&envelope.opcode) && negotiated_features & 32 == 0 {
            return Err(Error::UnsupportedFeature);
        }
        Self::decode(raw)
    }

    /// Structural frame decoding; callers must separately validate negotiated capabilities.
    pub fn decode(raw: &'a [u8]) -> Result<Self> {
        let e = ControlEnvelope::decode(bounded(raw, 0, 128)?)?;
        schema(e.abi_major, e.abi_minor)?;
        if e.magic != *b"TWCTL001" || e.header_bytes != 128 {
            return Err(Error::Malformed);
        }
        if raw.len() > 65536 || e.frame_bytes as usize != raw.len() || e.fd_count > 16 {
            return Err(Error::Bounds);
        }
        if crc_masked(&raw[..128], 116..120) != e.header_crc32c
            || crc32c(&raw[128..]) != e.body_crc32c
        {
            return Err(Error::Checksum);
        }
        if e.status > 44 {
            return Err(Error::InvalidEnum);
        }
        if (e.flags == 18) != (e.status != 0) {
            return Err(Error::Malformed);
        }
        if e.opcode != 3 && e.fd_count != 0 {
            return Err(Error::Malformed);
        }
        validate_control_body(e.opcode, e.flags, &raw[128..])?;
        if e.opcode == 3 {
            let offer = OfferPrefix::decode(&raw[128..192])?;
            if offer.descriptor_count != e.fd_count {
                return Err(Error::Malformed);
            }
        }
        if e.opcode == 20 && e.flags == 10 {
            let b = QuiesceBody::decode(&raw[128..])?;
            if b.evidence_kind != 1 || !is_zero(&b.exit_evidence_id) {
                return Err(Error::Malformed);
            }
        }
        if e.opcode == 19 && e.flags != 1 {
            let b = SwitchBody::decode(&raw[128..])?;
            if !is_zero(&b.snapshot_id)
                || b.snapshot_seq != 0
                || (e.flags == 10 && b.switch_kind != 2)
                || (e.flags == 18 && (e.status != 32 || b.switch_kind != 3))
            {
                return Err(Error::Malformed);
            }
        }
        Ok(Self {
            envelope: e,
            body: &raw[128..],
        })
    }
}
fn validate_offer(raw: &[u8]) -> Result<()> {
    let p = OfferPrefix::decode(bounded(raw, 0, 64)?)?;
    if !(1..=16).contains(&p.descriptor_count)
        || p.descriptor_bytes != 128
        || raw.len() != 64 + usize::from(p.descriptor_count) * 128
    {
        return Err(Error::Bounds);
    }
    let mut used = 0u32;
    for chunk in raw[64..].as_chunks::<128>().0 {
        let d = ResourceDescriptor::decode(chunk)?;
        if d.fd_index >= p.descriptor_count || used & (1 << d.fd_index) != 0 {
            return Err(Error::Bounds);
        }
        used |= 1 << d.fd_index;
        if !(1..=9).contains(&d.resource_kind)
            || !(1..=2).contains(&d.access)
            || !(1..=2).contains(&d.seal_profile)
        {
            return Err(Error::InvalidEnum);
        }
    }
    Ok(())
}
