//! Audited zero-copy `#[repr(C)]` views over validated 512-byte market/report and
//! 1,024-byte intent SHM ring slots.
//!
//! # Safety Boundary
//!
//! `unsafe` in this module is restricted to implementing the sealed [`ZeroCopyRecord`]
//! trait for [`MarketRecord`], [`MarketExactRecord`], and [`ReportRecord`] (512 bytes) plus
//! [`ZeroCopyIntentRecord`] for [`IntentRecord`] (1,024 bytes), forming a shared reference
//! `&'a Self` from an 8-byte-aligned `&'a [u8; 512]` or `&'a [u8; 1024]` byte array after
//! ABI, reserved-zero, field-whitelist, domain, and (when `feature = "crc32"` is enabled)
//! CRC-32C checks pass.
//!
//! Soundness relies on five invariants enforced at compile time and runtime:
//! 1. **Little-endian target**: `src/wire.rs` emits `compile_error!` on big-endian targets.
//! 2. **Exact `#[repr(C)]` ABI without implicit padding**: `src/wire.rs` asserts at compile
//!    time that `RecordHeader` (128B, align 8), `DepthLevel` (16B, align 8), `MarketBody`
//!    (384B, align 8), `MarketRecord` (512B, align 8), `DecimalWire` (24B, align 1),
//!    `MarketExactBody` (384B, align 8), `MarketExactRecord` (512B, align 8), `TriggerRef`
//!    (64B, align 8), `IntentBody` (896B, align 8), `IntentRecord` (1,024B, align 8),
//!    `ReportBody` (384B, align 8), and `ReportRecord` (512B, align 8) match every byte
//!    offset in `specs/contracts/ipc-wire-v1.json` with explicit `_pad_*` byte arrays.
//! 3. **All-bit-patterns-valid types**: Every field is an integer primitive (`u8`, `i8`,
//!    `u16`, `u32`, `i32`, `u64`, `i64`) or fixed-length array thereof (`[T; N]`), with zero
//!    `bool`, `enum`, `char`, or pointer/reference types. Every initialized 512-byte or
//!    1,024-byte bit pattern is inhabited without invalid-value UB.
//! 4. **Runtime alignment verification**: `bytes.as_ptr().align_offset(align_of::<Self>()) == 0`
//!    is checked before dereferencing `ptr.cast::<Self>()`, returning [`Error::Unaligned`]
//!    on any misaligned input.
//! 5. **Lifetime and aliasing**: `bytes: &'a [u8; N]` witnesses `N` initialized readable
//!    bytes with no mutable alias for the entirety of `'a`.
#![deny(unsafe_op_in_unsafe_fn)]

use crate::{
    Error, IntentRecord, MarketExactRecord, MarketRecord, MarketState, RecordHeader, ReportRecord,
    Result, schema, seq, validate_intent_semantics, validate_levels, validate_report_semantics,
    wire::validate_fields, zero,
};

mod private {
    pub trait Sealed {}
    impl Sealed for crate::MarketRecord {}
    impl Sealed for crate::MarketExactRecord {}
    impl Sealed for crate::ReportRecord {}

    pub trait SealedIntent {}
    impl SealedIntent for crate::IntentRecord {}
}

/// Sealed marker trait for 512-byte, 8-byte-aligned `#[repr(C)]` SHM wire records
/// (`MarketRecord`, `MarketExactRecord`, `ReportRecord`) whose fields are exclusively
/// integer scalars and arrays with explicit zeroed padding.
///
/// # Safety
///
/// Implementors must be `#[repr(C)]` with `size_of::<Self>() == 512`,
/// `align_of::<Self>() == 8`, zero implicit padding, and no types with invalid bit patterns.
pub unsafe trait ZeroCopyRecord: private::Sealed + Sized {
    /// Validate an 8-byte-aligned 512-byte wire image and borrow `Self` in place.
    fn from_aligned_bytes(bytes: &[u8; 512]) -> Result<&Self>;

    /// Validate an 8-byte-aligned 512-byte slice and borrow `Self` in place.
    #[inline]
    fn from_aligned_slice(bytes: &[u8]) -> Result<&Self> {
        let array: &[u8; 512] = bytes.try_into().map_err(|_| Error::Bounds)?;
        Self::from_aligned_bytes(array)
    }
}

/// Sealed marker trait for 1,024-byte, 8-byte-aligned `#[repr(C)]` inline intent wire records
/// (`IntentRecord`) whose fields are exclusively integer scalars and arrays with explicit
/// zeroed padding.
///
/// # Safety
///
/// Implementors must be `#[repr(C)]` with `size_of::<Self>() == 1024`,
/// `align_of::<Self>() == 8`, zero implicit padding, and no types with invalid bit patterns.
pub unsafe trait ZeroCopyIntentRecord: private::SealedIntent + Sized {
    /// Validate an 8-byte-aligned 1,024-byte wire image and borrow `Self` in place.
    fn from_aligned_bytes(bytes: &[u8; 1024]) -> Result<&Self>;

    /// Validate an 8-byte-aligned 1,024-byte slice and borrow `Self` in place.
    #[inline]
    fn from_aligned_slice(bytes: &[u8]) -> Result<&Self> {
        let array: &[u8; 1024] = bytes.try_into().map_err(|_| Error::Bounds)?;
        Self::from_aligned_bytes(array)
    }
}

fn validate_common_header(h: &RecordHeader) -> Result<()> {
    zero(&h._pad_38)?;
    zero(&h._pad_120)?;
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
    Ok(())
}

fn validate_header_512(
    h: &RecordHeader,
    bytes: &[u8; 512],
    expected_kind: core::ops::RangeInclusive<u16>,
    is_ors_report: bool,
) -> Result<()> {
    validate_common_header(h)?;
    if !expected_kind.contains(&h.message_type) {
        return if (513..=525).contains(&h.message_type) {
            Err(Error::Bounds)
        } else {
            Err(Error::UnknownMessage)
        };
    }
    if h.body_bytes != 384 {
        return Err(Error::Bounds);
    }
    if h.flags & 8 == 0 {
        if h.record_crc32c != 0 || h.payload_crc32c != 0 {
            return Err(Error::Checksum);
        }
    } else {
        #[cfg(feature = "crc32")]
        if crate::crc32c_masked(bytes, 112..120) != h.record_crc32c {
            return Err(Error::Checksum);
        }
        #[cfg(not(feature = "crc32"))]
        let _ = bytes;
    }
    validate_fields(h.message_type, &bytes[128..])?;
    if !is_ors_report
        && h.message_type != 259
        && (h.payload_kind != 0
            || h.payload_length != 0
            || h.payload_offset != 0
            || h.arena_class != 0
            || h.allocation_seq != 0
            || h.payload_crc32c != 0)
    {
        return Err(Error::Malformed);
    }
    Ok(())
}

fn validate_header_1024(
    h: &RecordHeader,
    bytes: &[u8; 1024],
    expected_kind: core::ops::RangeInclusive<u16>,
) -> Result<()> {
    validate_common_header(h)?;
    if !expected_kind.contains(&h.message_type) {
        return if matches!(h.message_type, 257..=266 | 769..=782) {
            Err(Error::Bounds)
        } else {
            Err(Error::UnknownMessage)
        };
    }
    if h.body_bytes != 896 {
        return Err(Error::Bounds);
    }
    if h.flags & 8 == 0 {
        if h.record_crc32c != 0 || h.payload_crc32c != 0 {
            return Err(Error::Checksum);
        }
    } else {
        #[cfg(feature = "crc32")]
        if crate::crc32c_masked(bytes, 112..120) != h.record_crc32c {
            return Err(Error::Checksum);
        }
        #[cfg(not(feature = "crc32"))]
        let _ = bytes;
    }
    validate_fields(h.message_type, &bytes[128..])
}

// SAFETY: `MarketRecord` is `#[repr(C)]`, `size_of == 512`, `align_of == 8`, has zero
// implicit compiler padding (verified by compile-time `offset_of!` assertions in `wire.rs`),
// and consists solely of integer primitives and fixed-size integer/struct arrays.
unsafe impl ZeroCopyRecord for MarketRecord {
    #[inline]
    fn from_aligned_bytes(bytes: &[u8; 512]) -> Result<&Self> {
        let ptr = bytes.as_ptr();
        if ptr.align_offset(core::mem::align_of::<Self>()) != 0 {
            return Err(Error::Unaligned);
        }
        // SAFETY: `bytes` is a live shared reference to 512 initialized bytes for the return
        // lifetime, `ptr` is 8-byte aligned, `size_of::<MarketRecord>() == 512`, and every
        // 512-byte bit pattern is a valid `MarketRecord` on little-endian targets.
        let record = unsafe { &*ptr.cast::<Self>() };
        let h = &record.header;
        validate_header_512(h, bytes, 257..=263, false)?;
        zero(&record.body._pad_180)?;
        let b = &record.body;
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
                zero(&bytes[320 + count as usize * 16..])?;
                validate_levels(
                    &bytes[320..320 + count as usize * 16],
                    b.bid_count,
                    b.ask_count,
                )?;
                #[cfg(feature = "crc32")]
                if h.flags & 8 != 0
                    && crate::crc32c(&bytes[320..320 + count as usize * 16]) != h.payload_crc32c
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
                zero(&bytes[320..])?;
            }
        }
        Ok(record)
    }
}

impl MarketRecord {
    /// Validate an 8-byte-aligned 512-byte wire image and borrow `MarketRecord` in place.
    #[inline]
    pub fn from_aligned_bytes(bytes: &[u8; 512]) -> Result<&Self> {
        <Self as ZeroCopyRecord>::from_aligned_bytes(bytes)
    }

    /// Validate an 8-byte-aligned 512-byte slice and borrow `MarketRecord` in place.
    #[inline]
    pub fn from_aligned_slice(bytes: &[u8]) -> Result<&Self> {
        <Self as ZeroCopyRecord>::from_aligned_slice(bytes)
    }
}

// SAFETY: `MarketExactRecord` is `#[repr(C)]`, `size_of == 512`, `align_of == 8`, has zero
// implicit compiler padding (verified by compile-time `offset_of!` assertions in `wire.rs`),
// and consists solely of integer primitives and fixed-size integer/struct arrays.
unsafe impl ZeroCopyRecord for MarketExactRecord {
    #[inline]
    fn from_aligned_bytes(bytes: &[u8; 512]) -> Result<&Self> {
        let ptr = bytes.as_ptr();
        if ptr.align_offset(core::mem::align_of::<Self>()) != 0 {
            return Err(Error::Unaligned);
        }
        // SAFETY: `bytes` is a live shared reference to 512 initialized bytes for the return
        // lifetime, `ptr` is 8-byte aligned, `size_of::<MarketExactRecord>() == 512`, and every
        // 512-byte bit pattern is a valid `MarketExactRecord` on little-endian targets.
        let record = unsafe { &*ptr.cast::<Self>() };
        validate_header_512(&record.header, bytes, 264..=266, false)?;
        let b = &record.body;
        for d in [
            &b.mark_price,
            &b.index_price,
            &b.estimated_settlement_price,
            &b.moving_average,
            &b.funding_rate,
        ] {
            zero(&d._pad_17)?;
        }
        zero(&b._pad_262)?;
        crate::market_exact::validate_exact(record)?;
        Ok(record)
    }
}

impl MarketExactRecord {
    /// Validate an 8-byte-aligned 512-byte wire image and borrow `MarketExactRecord` in place.
    #[inline]
    pub fn from_aligned_bytes(bytes: &[u8; 512]) -> Result<&Self> {
        <Self as ZeroCopyRecord>::from_aligned_bytes(bytes)
    }

    /// Validate an 8-byte-aligned 512-byte slice and borrow `MarketExactRecord` in place.
    #[inline]
    pub fn from_aligned_slice(bytes: &[u8]) -> Result<&Self> {
        <Self as ZeroCopyRecord>::from_aligned_slice(bytes)
    }
}

// SAFETY: `ReportRecord` is `#[repr(C)]`, `size_of == 512`, `align_of == 8`, has zero
// implicit compiler padding (verified by compile-time `offset_of!` assertions in `wire.rs`),
// and consists solely of integer primitives and fixed-size byte arrays.
unsafe impl ZeroCopyRecord for ReportRecord {
    #[inline]
    fn from_aligned_bytes(bytes: &[u8; 512]) -> Result<&Self> {
        let ptr = bytes.as_ptr();
        if ptr.align_offset(core::mem::align_of::<Self>()) != 0 {
            return Err(Error::Unaligned);
        }
        // SAFETY: `bytes` is a live shared reference to 512 initialized bytes for the return
        // lifetime, `ptr` is 8-byte aligned, `size_of::<ReportRecord>() == 512`, and every
        // 512-byte bit pattern is a valid `ReportRecord` on little-endian targets.
        let record = unsafe { &*ptr.cast::<Self>() };
        validate_header_512(&record.header, bytes, 769..=781, true)?;
        zero(&record.body._pad_360)?;
        validate_report_semantics(record)?;
        Ok(record)
    }
}

impl ReportRecord {
    /// Validate an 8-byte-aligned 512-byte wire image and borrow `ReportRecord` in place.
    #[inline]
    pub fn from_aligned_bytes(bytes: &[u8; 512]) -> Result<&Self> {
        <Self as ZeroCopyRecord>::from_aligned_bytes(bytes)
    }

    /// Validate an 8-byte-aligned 512-byte slice and borrow `ReportRecord` in place.
    #[inline]
    pub fn from_aligned_slice(bytes: &[u8]) -> Result<&Self> {
        <Self as ZeroCopyRecord>::from_aligned_slice(bytes)
    }
}

// SAFETY: `IntentRecord` is `#[repr(C)]`, `size_of == 1024`, `align_of == 8`, has zero
// implicit compiler padding (verified by compile-time `offset_of!` assertions in `wire.rs`),
// and consists solely of integer primitives and fixed-size integer/struct arrays.
unsafe impl ZeroCopyIntentRecord for IntentRecord {
    #[inline]
    fn from_aligned_bytes(bytes: &[u8; 1024]) -> Result<&Self> {
        let ptr = bytes.as_ptr();
        if ptr.align_offset(core::mem::align_of::<Self>()) != 0 {
            return Err(Error::Unaligned);
        }
        // SAFETY: `bytes` is a live shared reference to 1024 initialized bytes for the return
        // lifetime, `ptr` is 8-byte aligned, `size_of::<IntentRecord>() == 1024`, and every
        // 1024-byte bit pattern is a valid `IntentRecord` on little-endian targets.
        let record = unsafe { &*ptr.cast::<Self>() };
        validate_header_1024(&record.header, bytes, 513..=525)?;
        zero(&record.body._pad_360)?;
        validate_intent_semantics(record, &bytes[128..])?;
        Ok(record)
    }
}

impl IntentRecord {
    /// Validate an 8-byte-aligned 1,024-byte wire image and borrow `IntentRecord` in place.
    #[inline]
    pub fn from_aligned_bytes(bytes: &[u8; 1024]) -> Result<&Self> {
        <Self as ZeroCopyIntentRecord>::from_aligned_bytes(bytes)
    }

    /// Validate an 8-byte-aligned 1,024-byte slice and borrow `IntentRecord` in place.
    #[inline]
    pub fn from_aligned_slice(bytes: &[u8]) -> Result<&Self> {
        <Self as ZeroCopyIntentRecord>::from_aligned_slice(bytes)
    }
}

/// Zero-copy borrowed view of either a legacy [`MarketRecord`] (`257..=263`) or
/// an exact [`MarketExactRecord`] (`264..=266`) inside an aligned 512-byte slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketStateRef<'a> {
    Legacy(&'a MarketRecord),
    Exact(&'a MarketExactRecord),
}

impl<'a> MarketStateRef<'a> {
    /// Validate an 8-byte-aligned 512-byte market ring slot and borrow its record in place.
    #[inline]
    pub fn from_aligned_bytes(bytes: &'a [u8; 512]) -> Result<Self> {
        if bytes
            .as_ptr()
            .align_offset(core::mem::align_of::<MarketRecord>())
            != 0
        {
            return Err(Error::Unaligned);
        }
        let message_type = u16::from_le_bytes([bytes[8], bytes[9]]);
        match message_type {
            257..=263 => MarketRecord::from_aligned_bytes(bytes).map(Self::Legacy),
            264..=266 => MarketExactRecord::from_aligned_bytes(bytes).map(Self::Exact),
            513..=525 => Err(Error::Bounds),
            _ => {
                // Run header checks first so malformed headers surface identical errors to decode_record.
                MarketRecord::from_aligned_bytes(bytes).map(Self::Legacy)
            }
        }
    }

    /// Validate an 8-byte-aligned 512-byte slice and borrow its market record in place.
    #[inline]
    pub fn from_aligned_slice(bytes: &'a [u8]) -> Result<Self> {
        let array: &'a [u8; 512] = bytes.try_into().map_err(|_| Error::Bounds)?;
        Self::from_aligned_bytes(array)
    }

    #[inline]
    pub fn header(self) -> &'a RecordHeader {
        match self {
            Self::Legacy(r) => &r.header,
            Self::Exact(r) => &r.header,
        }
    }

    #[inline]
    pub fn feed_state(self) -> u16 {
        match self {
            Self::Legacy(r) => r.body.feed_state,
            Self::Exact(r) => r.body.feed_state,
        }
    }

    #[inline]
    pub fn to_owned_state(self) -> MarketState {
        match self {
            Self::Legacy(r) => MarketState::Legacy(r.clone()),
            Self::Exact(r) => MarketState::Exact(r.clone()),
        }
    }
}
