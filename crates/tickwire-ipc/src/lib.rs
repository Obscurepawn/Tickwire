//! Fixed-layout Tickwire wire ABI for inline SHM market, intent, and ORS report records,
//! optional SHM Arena oversize payloads, DOP logon control frames, and zero-copy views.
//! Live shared-memory mapping and atomic cursor synchronization live outside this crate.
#![deny(unsafe_code)]

#[forbid(unsafe_code)]
mod wire;
pub use wire::*;
#[forbid(unsafe_code)]
mod config;
pub use config::*;
#[forbid(unsafe_code)]
mod market_exact;
pub use market_exact::*;
#[forbid(unsafe_code)]
mod snapshot;
pub use snapshot::*;
#[allow(unsafe_code)]
mod crc32c;
pub use crc32c::{
    TABLES as CRC32C_SLICING8_TABLES, crc32c, crc32c_masked, crc32c_slicing8, crc32c_update,
};
#[allow(unsafe_code)]
mod zerocopy;
pub use zerocopy::{MarketStateRef, ZeroCopyIntentRecord, ZeroCopyRecord};

/// Wildcard strategy identifier (`255`) permitted only on `CancelAll` / `CancelAllRfqQuotes`
/// intents and venue-wide broadcast ORS reports (`ExchRateLimit`, `OrsShutDown`, `OrsRestricted`).
pub const ALL_STRATS: u8 = 255;

/// Negotiated control capability bit (`10`) gating DOP opcodes (`96..=100`) in
/// [`ControlFrame::decode_negotiated`].
pub const DOP_PROTOCOL_FEATURE_BIT: u32 = 10;
/// Feature bitmask (`1 << 10`) for DOP control opcodes (`96..=100`).
pub const DOP_PROTOCOL_FEATURE_MASK: u64 = 1 << DOP_PROTOCOL_FEATURE_BIT;

/// Inline strategy intent `command_kind` and `RecordHeader.message_type` constants.
#[forbid(unsafe_code)]
pub mod intent_kind {
    pub const NEW_ORDER: u16 = 1;
    pub const CANCEL: u16 = 2;
    pub const CANCEL_REPLACE: u16 = 3;
    pub const AMEND_KEEP_PRIORITY: u16 = 4;
    pub const MODIFY: u16 = 5;
    pub const QUERY_ORDER: u16 = 6;
    pub const CANCEL_LIST: u16 = 7;
    pub const CANCEL_ALGO: u16 = 8;
    pub const CANCEL_ALL: u16 = 9;
    pub const NEW_RFQ_QUOTE: u16 = 10;
    pub const MODIFY_RFQ_QUOTE: u16 = 11;
    pub const CANCEL_RFQ_QUOTE: u16 = 12;
    pub const CANCEL_ALL_RFQ_QUOTES: u16 = 13;

    pub const MSG_NEW_ORDER: u16 = 513;
    pub const MSG_CANCEL: u16 = 514;
    pub const MSG_CANCEL_REPLACE: u16 = 515;
    pub const MSG_AMEND_KEEP_PRIORITY: u16 = 516;
    pub const MSG_MODIFY: u16 = 517;
    pub const MSG_QUERY_ORDER: u16 = 518;
    pub const MSG_CANCEL_LIST: u16 = 519;
    pub const MSG_CANCEL_ALGO: u16 = 520;
    pub const MSG_CANCEL_ALL: u16 = 521;
    pub const MSG_NEW_RFQ_QUOTE: u16 = 522;
    pub const MSG_MODIFY_RFQ_QUOTE: u16 = 523;
    pub const MSG_CANCEL_RFQ_QUOTE: u16 = 524;
    pub const MSG_CANCEL_ALL_RFQ_QUOTES: u16 = 525;
}

/// Bitmask flags for [`IntentBody::intent_flags`].
#[forbid(unsafe_code)]
pub mod intent_flags {
    pub const BYPASS_RMS: u8 = 1 << 0;
    pub const DRY_RUN: u8 = 1 << 1;
    pub const CHECK_STP: u8 = 1 << 2;
    pub const REDUCE_ONLY: u8 = 1 << 3;
    pub const POST_ONLY: u8 = 1 << 4;
    pub const QUOTE_AMOUNT: u8 = 1 << 5;
    pub const PRICE_PROTECT: u8 = 1 << 6;
    pub const CLOSE_POSITION: u8 = 1 << 7;
}

/// Wire codes for [`IntentBody::time_in_force`].
#[forbid(unsafe_code)]
pub mod tif_code {
    pub const NONE: u8 = 0;
    pub const GTC: u8 = 1;
    pub const IOC: u8 = 2;
    pub const FOK: u8 = 3;
    pub const POST_ONLY: u8 = 4;
    pub const GTD: u8 = 5;
    pub const RPI: u8 = 6;
}

/// Inline ORS report `report_kind` and `RecordHeader.message_type` constants.
#[forbid(unsafe_code)]
pub mod report_kind {
    pub const ACK: u16 = 1;
    pub const FRONT_ACK: u16 = 2;
    pub const NACK: u16 = 3;
    pub const CANCEL_NACK: u16 = 4;
    pub const FILL: u16 = 5;
    pub const CANCELED: u16 = 6;
    pub const MODIFIED: u16 = 7;
    pub const MODIFY_REJECTED: u16 = 8;
    pub const EXCH_RATE_LIMIT: u16 = 9;
    pub const ORS_SHUT_DOWN: u16 = 10;
    pub const ORS_RESTRICTED: u16 = 11;
    pub const RFQ_QUOTE_ACK: u16 = 12;
    pub const RFQ_QUOTE_NACK: u16 = 13;

    pub const MSG_ACK: u16 = 769;
    pub const MSG_FRONT_ACK: u16 = 770;
    pub const MSG_NACK: u16 = 771;
    pub const MSG_CANCEL_NACK: u16 = 772;
    pub const MSG_FILL: u16 = 773;
    pub const MSG_CANCELED: u16 = 774;
    pub const MSG_MODIFIED: u16 = 775;
    pub const MSG_MODIFY_REJECTED: u16 = 776;
    pub const MSG_EXCH_RATE_LIMIT: u16 = 777;
    pub const MSG_ORS_SHUT_DOWN: u16 = 778;
    pub const MSG_ORS_RESTRICTED: u16 = 779;
    pub const MSG_RFQ_QUOTE_ACK: u16 = 780;
    pub const MSG_RFQ_QUOTE_NACK: u16 = 781;
    pub const MSG_ORDER_OWNERSHIP: u16 = 782;
}

/// Direct Order Entry Protocol (DOP) logon and session opcodes carried inside [`ControlFrame`].
#[forbid(unsafe_code)]
pub mod dop_opcode {
    pub const DOP_LOGIN: u16 = 96;
    pub const DOP_LOGOUT: u16 = 97;
    pub const DOP_ECHO: u16 = 98;
    pub const ORS_SESSIONS: u16 = 99;
    pub const DOP_HEART_BEAT: u16 = 100;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Malformed,
    AbiMajor,
    AbiMinor,
    UnsupportedFeature,
    UnsupportedArchitecture,
    Bounds,
    Unaligned,
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

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // Fixed stack storage avoids heap allocation on record decode.
pub enum Record {
    Market(MarketRecord),
    MarketExact(MarketExactRecord),
    Intent(IntentRecord),
    Report(ReportRecord),
    Ownership(OwnershipRecord),
}

pub(crate) fn validate_inline_or_arena_header(
    h: &RecordHeader,
    arena_offset: u64,
    arena_len: u32,
) -> Result<()> {
    if arena_len == 0 {
        if arena_offset != 0
            || h.payload_kind != 0
            || h.payload_length != 0
            || h.payload_offset != 0
            || h.arena_class != 0
            || h.allocation_seq != 0
            || h.payload_crc32c != 0
        {
            return Err(Error::Malformed);
        }
    } else if h.payload_kind != 2
        || arena_len > 65536
        || arena_offset < 128
        || h.payload_length != arena_len
        || h.payload_offset != arena_offset
        || !(1..=4).contains(&h.arena_class)
        || h.allocation_seq != h.sequence
    {
        return Err(Error::Malformed);
    }
    Ok(())
}

pub(crate) fn validate_intent_semantics(r: &IntentRecord, body_raw: &[u8]) -> Result<()> {
    let h = &r.header;
    let b = &r.body;
    if h.feed_generation != 0 || b.msg_seqnum == 0 {
        return Err(Error::Malformed);
    }
    seq(b.msg_seqnum)?;
    seq(b.order_seqnum)?;
    seq(b.orig_order_seqnum)?;
    if b.strat_id == ALL_STRATS && !matches!(h.message_type, 521 | 525) {
        return Err(Error::InvalidEnum);
    }
    if b.trigger_count > 8 {
        return Err(Error::Bounds);
    }
    zero(&body_raw[384 + usize::from(b.trigger_count) * 64..])?;
    for t in &b.triggers[..usize::from(b.trigger_count)] {
        zero(&t._pad_60)?;
        if t.topic_id == 0 || t.sequence == 0 || t.feed_generation == 0 {
            return Err(Error::Identity);
        }
        seq(t.sequence)?;
        seq(t.feed_generation)?;
    }
    padded(&b.client_id, usize::from(b.client_id_len))?;
    padded(&b.orig_client_id, usize::from(b.orig_client_id_len))?;
    padded(&b.venue_order_id, usize::from(b.venue_order_id_len))?;
    validate_inline_or_arena_header(h, b.arena_offset, b.arena_len)
}

pub(crate) fn validate_report_semantics(r: &ReportRecord) -> Result<()> {
    let h = &r.header;
    let b = &r.body;
    if h.feed_generation != 0 || b.report_seq == 0 {
        return Err(Error::Malformed);
    }
    seq(b.report_seq)?;
    seq(b.orig_seqnum)?;
    seq(b.order_seqnum)?;
    if b.reason > 30 {
        return Err(Error::InvalidEnum);
    }
    if b.strat_id == ALL_STRATS && !matches!(h.message_type, 777..=779) {
        return Err(Error::InvalidEnum);
    }
    if b.amendment > 1 || b.projected > 1 || b.has_fee > 1 || b.hard_limit_hit > 1 {
        return Err(Error::InvalidEnum);
    }
    if b.amendment == 1 && b.projected == 1 {
        return Err(Error::Malformed);
    }
    if b.has_fee == 0 && (b.fee_units != 0 || b.fee_asset_id != 0 || b.asset_exponent != 0) {
        return Err(Error::Malformed);
    }
    padded(&b.client_id, usize::from(b.client_id_len))?;
    padded(&b.venue_order_id, usize::from(b.venue_order_id_len))?;
    padded(&b.venue_trade_id, usize::from(b.venue_trade_id_len))?;
    validate_inline_or_arena_header(h, b.arena_offset, b.arena_len)
}

/// Decode an entire fixed record. Arena-backed payloads additionally validate against
/// `validate_arena_payload`, `validate_intent_arena_payload`, or `validate_report_arena_payload`.
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
        257..=266 | 769..=782 => 512,
        513..=525 => 1024,
        _ => return Err(Error::UnknownMessage),
    };
    if raw.len() != size || h.body_bytes as usize != size - 128 {
        return Err(Error::Bounds);
    }
    if h.flags & 8 == 0 {
        if h.record_crc32c != 0 || h.payload_crc32c != 0 {
            return Err(Error::Checksum);
        }
    } else {
        #[cfg(feature = "crc32")]
        if crc32c_masked(raw, 112..120) != h.record_crc32c {
            return Err(Error::Checksum);
        }
    }
    validate_fields(h.message_type, &raw[128..])?;
    if matches!(h.message_type, 257 | 258 | 260..=266 | 782)
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
                    #[cfg(feature = "crc32")]
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
        264..=266 => {
            let r = MarketExactRecord::decode(raw)?;
            validate_exact(&r)?;
            Ok(Record::MarketExact(r))
        }
        513..=525 => {
            let r = IntentRecord::decode(raw)?;
            validate_intent_semantics(&r, &raw[128..])?;
            Ok(Record::Intent(r))
        }
        782 => {
            let r = OwnershipRecord::decode(raw)?;
            padded(
                &r.body.venue_algo_id,
                usize::from(r.body.venue_algo_id_length),
            )?;
            seq(r.body.report_seq)?;
            seq(r.body.origin_intent_seq)?;
            Ok(Record::Ownership(r))
        }
        _ => {
            let r = ReportRecord::decode(raw)?;
            validate_report_semantics(&r)?;
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

/// Validate already encoded absolute depth updates without accessing a mapped bank.
pub fn validate_depth_payload(raw: &[u8], bids: u32, asks: u32) -> Result<()> {
    if bids.checked_add(asks).ok_or(Error::Bounds)? > 4096 {
        return Err(Error::Bounds);
    }
    validate_levels(raw, bids, asks)
}

fn validate_arena_block_bytes<'a>(
    h: &RecordHeader,
    bytes: &'a [u8],
    resource: &ResourceHeader,
    class: &ArenaClass,
) -> Result<&'a [u8]> {
    if bytes.len() != class.block_bytes as usize || bytes.len() < 128 {
        return Err(Error::Bounds);
    }
    let block = ArenaBlockHeader::decode(&bytes[..128])?;
    if block.resource_id != resource.resource_id
        || block.ring_epoch != resource.ring_epoch
        || block.allocation_seq != h.sequence
        || h.allocation_seq != h.sequence
        || block.payload_length != h.payload_length
        || block.class_id != h.arena_class
        || block.class_id != class.class_id
    {
        return Err(Error::Identity);
    }
    let payload = bounded(bytes, 128, u64::from(h.payload_length))?;
    zero(bounded(
        bytes,
        128 + u64::from(h.payload_length),
        u64::from(class.block_bytes)
            .checked_sub(128 + u64::from(h.payload_length))
            .ok_or(Error::Bounds)?,
    )?)?;
    if block.payload_crc32c != h.payload_crc32c {
        return Err(Error::Checksum);
    }
    #[cfg(feature = "crc32")]
    if h.flags & 8 != 0 && crc32c(payload) != h.payload_crc32c {
        return Err(Error::Checksum);
    }
    Ok(payload)
}

fn locate_and_validate_arena_slice<'a>(
    h: &RecordHeader,
    bank: &'a [u8],
    resource: &ResourceHeader,
) -> Result<&'a [u8]> {
    if h.payload_kind != 2 || resource.arena_class_count > 4 || h.arena_class == 0 {
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
    validate_arena_block_bytes(h, bytes, resource, class)
}

/// Validate exactly one immutable, retained block, never a slice over mutable neighbours.
pub fn validate_arena_block(
    record: &MarketRecord,
    bytes: &[u8],
    resource: &ResourceHeader,
    class: &ArenaClass,
) -> Result<()> {
    let payload = validate_arena_block_bytes(&record.header, bytes, resource, class)?;
    validate_depth_payload(payload, record.body.bid_count, record.body.ask_count)
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
    validate_arena_payload_prechecked(record, bank, resource)
}

/// Validate arena block geometry and payload for a `MarketRecord` already checked by
/// [`MarketRecord::from_aligned_bytes`] or [`decode_record`], avoiding a 512-byte stack copy.
pub fn validate_arena_payload_prechecked(
    record: &MarketRecord,
    bank: &[u8],
    resource: &ResourceHeader,
) -> Result<()> {
    if record.header.message_type != 259 {
        return Err(Error::Malformed);
    }
    let payload = locate_and_validate_arena_slice(&record.header, bank, resource)?;
    validate_depth_payload(payload, record.body.bid_count, record.body.ask_count)
}

/// Validate and borrow an oversize in-memory SHM Arena payload bound to an [`IntentRecord`]
/// (`payload_kind == 2`, zero disk I/O).
pub fn validate_intent_arena_payload<'a>(
    record: &IntentRecord,
    bank: &'a [u8],
    resource: &ResourceHeader,
) -> Result<&'a [u8]> {
    if !(513..=525).contains(&record.header.message_type) || record.body.arena_len == 0 {
        return Err(Error::Malformed);
    }
    validate_inline_or_arena_header(
        &record.header,
        record.body.arena_offset,
        record.body.arena_len,
    )?;
    locate_and_validate_arena_slice(&record.header, bank, resource)
}

/// Validate and borrow an oversize in-memory SHM Arena payload bound to a [`ReportRecord`]
/// (`payload_kind == 2`, zero disk I/O).
pub fn validate_report_arena_payload<'a>(
    record: &ReportRecord,
    bank: &'a [u8],
    resource: &ResourceHeader,
) -> Result<&'a [u8]> {
    if !(769..=781).contains(&record.header.message_type) || record.body.arena_len == 0 {
        return Err(Error::Malformed);
    }
    validate_inline_or_arena_header(
        &record.header,
        record.body.arena_offset,
        record.body.arena_len,
    )?;
    locate_and_validate_arena_slice(&record.header, bank, resource)
}

#[derive(Debug, Clone)]
pub struct ControlFrame<'a> {
    pub envelope: ControlEnvelope,
    pub body: &'a [u8],
}
impl<'a> ControlFrame<'a> {
    /// Check negotiated feature requirements before decoding a gated extension body.
    pub fn decode_negotiated(raw: &'a [u8], negotiated_features: u64) -> Result<Self> {
        let envelope = ControlEnvelope::decode(bounded(raw, 0, 128)?)?;
        if (80..=84).contains(&envelope.opcode) && negotiated_features & 32 == 0 {
            return Err(Error::UnsupportedFeature);
        }
        if (96..=100).contains(&envelope.opcode)
            && negotiated_features & DOP_PROTOCOL_FEATURE_MASK == 0
        {
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
        #[cfg(feature = "crc32")]
        if crc32c_masked(&raw[..128], 116..120) != e.header_crc32c
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
        match e.opcode {
            dop_opcode::DOP_LOGIN => {
                let b = DopLoginBody::decode(&raw[128..])?;
                seq(b.next_source_seqnum)?;
                if b.strat_id == ALL_STRATS {
                    return Err(Error::InvalidEnum);
                }
            }
            dop_opcode::DOP_LOGOUT => {
                let b = DopLogoutBody::decode(&raw[128..])?;
                if b.reason > 30 || b.strat_id == ALL_STRATS || b.cancel_on_logout > 1 {
                    return Err(Error::InvalidEnum);
                }
            }
            dop_opcode::DOP_ECHO => {
                let b = DopEchoBody::decode(&raw[128..])?;
                seq(b.next_source_order_seqnum)?;
                if b.refusal_reason > 30
                    || b.strat_id == ALL_STRATS
                    || b.connected > 1
                    || b.enabled > 1
                    || b.market_connected > 1
                    || b.ready_to_trade > 1
                {
                    return Err(Error::InvalidEnum);
                }
                if (b.ready_to_trade == 1
                    && (b.connected == 0 || b.enabled == 0 || b.market_connected == 0))
                    || (e.flags == 18 && b.ready_to_trade != 0)
                {
                    return Err(Error::Malformed);
                }
            }
            dop_opcode::ORS_SESSIONS => {
                let b = OrsSessionsBody::decode(&raw[128..])?;
                if b.session_count > 32 {
                    return Err(Error::Bounds);
                }
                zero(&raw[128 + 16 + usize::from(b.session_count) * 16..])?;
                for row in &b.sessions[..usize::from(b.session_count)] {
                    if row.product_mask & !0x00ff != 0
                        || row.strat_id == ALL_STRATS
                        || row.authorized > 1
                        || row.colo > 1
                        || row.ready > 1
                    {
                        return Err(Error::InvalidEnum);
                    }
                }
            }
            dop_opcode::DOP_HEART_BEAT => {
                let b = DopHeartBeatBody::decode(&raw[128..])?;
                seq(b.next_order_seqnum)?;
                if b.ready_to_trade > 1 {
                    return Err(Error::InvalidEnum);
                }
            }
            _ => {}
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
