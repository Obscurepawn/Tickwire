//! Audited SIMD structural JSON scanner and exact fixed-point integer parser
//! for Bybit V5 (`orderbook.1.<SYM>`) and OKX V5 (`bbo-tbt`) spot top-of-book frames.
//!
//! # Safety Boundary and Obligations
//!
//! This module is the sole SIMD `unsafe` boundary inside `spot_quotes` and enforces
//! `#![deny(unsafe_op_in_unsafe_fn)]`:
//!
//! * **Pointer bounds**: Unaligned 32-byte loads (`_mm256_loadu_si256`) execute only after
//!   verifying `offset + 32 <= chunk.len()`; unaligned 16-byte loads (`_mm_loadu_si128` on
//!   `x86_64`, `vld1q_u8` on `aarch64`) execute only after verifying `offset + 16 <= chunk.len()`.
//!   All remaining `< 16` tail bytes are processed via safe 8-byte SWAR (`u64::from_le_bytes`
//!   over subslices) and safe scalar indexing, never reading past `chunk.len()`.
//! * **Alignment**: `_mm256_loadu_si256`, `_mm_loadu_si128`, and `vld1q_u8` are architectural
//!   unaligned load instructions requiring 1-byte alignment (`align_of::<u8>() == 1`). Raw byte
//!   pointers are passed directly to the load intrinsics and are never dereferenced as aligned
//!   Rust references.
//! * **Aliasing and initialization**: Input frames arrive as shared `&[u8]` slices whose `0..len`
//!   bytes are initialized and immutable for the duration of the call; only shared read-only loads
//!   are performed.
//! * **CPU feature soundness**: [`Backend`] is an opaque capability token. `BackendKind::Avx2` is
//!   constructed exclusively after `std::arch::is_x86_feature_detected!("avx2")` succeeds during
//!   cold session setup (or explicit capability probing), while `Sse2` on `x86_64` and `Neon` on
//!   `aarch64` are baseline ABI features requiring zero runtime checks.
//! * **Slice lifetimes and UTF-8**: Returned `Quote<'a>` values borrow `symbol` from
//!   `&'a QuoteConfig` rather than the transient frame slice. Any frame containing a backslash
//!   escape (`\`) or non-ASCII byte (`>= 0x80`) aborts via `FastOutcome::Fallback` so the safe
//!   `serde_json` oracle handles escapes and multi-byte UTF-8 without `from_utf8_unchecked`.
#![deny(unsafe_op_in_unsafe_fn)]

use super::{Quote, QuoteConfig, QuoteLevel};
use crate::{Result, invalid};
use tickwire_binance::decimal::Decimal;

const POW10_U64: [u64; 20] = {
    let mut table = [0u64; 20];
    let mut i = 0;
    let mut v = 1u64;
    while i < 20 {
        table[i] = v;
        if i + 1 < 20 {
            v *= 10;
        }
        i += 1;
    }
    table
};

/// Opaque SIMD execution backend token validated at construction time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Backend(BackendKind);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackendKind {
    #[cfg(target_arch = "x86_64")]
    /// Invariant: constructed only after `is_x86_feature_detected!("avx2")` returns `true`.
    Avx2,
    #[cfg(target_arch = "x86_64")]
    Sse2,
    #[cfg(target_arch = "aarch64")]
    Neon,
    Swar,
}

impl Backend {
    /// Selects the widest hardware-supported scanner once during cold session setup.
    #[inline]
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            if std::arch::is_x86_feature_detected!("avx2") {
                Self(BackendKind::Avx2)
            } else {
                Self(BackendKind::Sse2)
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self(BackendKind::Neon)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self(BackendKind::Swar)
        }
    }

    /// Universal 64-bit SWAR + scalar tail backend (always available).
    #[inline]
    pub const fn swar() -> Self {
        Self(BackendKind::Swar)
    }

    /// Baseline 16-byte `x86_64` SSE2 backend (zero runtime feature check).
    #[cfg(target_arch = "x86_64")]
    #[inline]
    pub const fn sse2() -> Self {
        Self(BackendKind::Sse2)
    }

    /// 32-byte `x86_64` AVX2 backend if supported by the host CPU.
    #[cfg(target_arch = "x86_64")]
    #[inline]
    pub fn avx2_if_supported() -> Option<Self> {
        std::arch::is_x86_feature_detected!("avx2").then_some(Self(BackendKind::Avx2))
    }

    /// Baseline 16-byte `aarch64` NEON backend.
    #[cfg(target_arch = "aarch64")]
    #[inline]
    pub const fn neon() -> Self {
        Self(BackendKind::Neon)
    }
}

/// Pre-normalized positive increment (`price_tick` or `quantity_step`) computed at startup.
#[derive(Clone, Copy, Debug)]
pub struct StepSpec {
    raw: Decimal,
    /// Positive reduced mantissa with trailing factors of 10 removed while `scale > 0`,
    /// or `0` when the reduced mantissa exceeds `u64::MAX` (routing to `i128` fallback).
    mantissa_u64: u64,
    scale: u8,
}

impl StepSpec {
    #[inline]
    pub fn new(step: Decimal) -> Self {
        let mut m = step.mantissa().unsigned_abs();
        let mut s = step.scale();
        while s > 0 && m > 0 && m.is_multiple_of(10) {
            m /= 10;
            s -= 1;
        }
        let mantissa_u64 = u64::try_from(m).unwrap_or(0);
        Self {
            raw: step,
            mantissa_u64,
            scale: s,
        }
    }
}

/// Per-64-byte structural classification bitmasks over pure unescaped ASCII (`0x00..=0x7F`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BlockMasks {
    /// Bit `i` is `1` iff `chunk[i] == b'"'`.
    pub quotes: u64,
    /// Bit `i` is `1` iff `chunk[i] <= 0x1F` (RFC 8259 control characters).
    pub ctrls: u64,
    /// Bit `i` is `1` iff `chunk[i]` is `{`, `}`, `[`, or `]`.
    pub braces: u64,
    /// Bit `i` is `1` iff `chunk[i]` is in `b'0'..=b'9'`.
    pub digits: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanAbort {
    /// Backslash escape (`\`) or non-ASCII byte (`>= 0x80`) encountered; delegate to oracle.
    Fallback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ParseAbort {
    Fallback,
    InvalidJson,
}

/// Outcome of the zero-allocation SIMD fast path.
pub(crate) enum FastOutcome<T> {
    Decoded(Result<T>),
    Fallback,
}

/// Computes prefix-XOR across 64 bits: bit `i` flips at every set bit in `m`.
/// When `m` marks unescaped `"` positions, `prefix_xor(m) ^ prev_in_string` marks string interiors
/// (including opening `"`, excluding closing `"`; neither quote byte is `<= 0x1F`).
#[inline(always)]
pub(crate) const fn prefix_xor(mut m: u64) -> u64 {
    m ^= m << 1;
    m ^= m << 2;
    m ^= m << 4;
    m ^= m << 8;
    m ^= m << 16;
    m ^= m << 32;
    m
}

#[inline(always)]
pub(crate) fn scan_block_64(
    chunk: &[u8],
    backend: Backend,
) -> std::result::Result<BlockMasks, ScanAbort> {
    debug_assert!(chunk.len() <= 64);
    match backend.0 {
        #[cfg(target_arch = "x86_64")]
        BackendKind::Avx2 => {
            // SAFETY: `BackendKind::Avx2` is only constructed after runtime verification via
            // `std::arch::is_x86_feature_detected!("avx2")`.
            unsafe { scan_block_avx2(chunk) }
        }
        #[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
        BackendKind::Sse2 => {
            // SAFETY: `cfg(target_feature = "sse2")` guarantees SSE2 support at compile time.
            unsafe { scan_block_sse2(chunk) }
        }
        #[cfg(all(target_arch = "x86_64", not(target_feature = "sse2")))]
        BackendKind::Sse2 => scan_block_swar(chunk),
        #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
        BackendKind::Neon => {
            // SAFETY: `cfg(target_feature = "neon")` guarantees NEON support at compile time.
            unsafe { scan_block_neon(chunk) }
        }
        #[cfg(all(target_arch = "aarch64", not(target_feature = "neon")))]
        BackendKind::Neon => scan_block_swar(chunk),
        BackendKind::Swar => scan_block_swar(chunk),
    }
}

#[inline(always)]
const fn swar_pack_msbs(msbs: u64) -> u64 {
    (((msbs >> 7).wrapping_mul(0x0102_0408_1020_4080)) >> 56) & 0xFF
}

#[inline(always)]
const fn swar_zero_byte_msbs_ascii(x: u64) -> u64 {
    !((x | 0x8080_8080_8080_8080).wrapping_sub(0x0101_0101_0101_0101)) & 0x8080_8080_8080_8080
}

#[inline(always)]
fn scan_u64_swar(w: u64) -> std::result::Result<(u64, u64, u64, u64), ScanAbort> {
    if (w & 0x8080_8080_8080_8080) != 0 {
        return Err(ScanAbort::Fallback);
    }
    // Every byte of `w` is now guaranteed to be in `0x00..=0x7F` (MSB == 0), so
    // `(x | 0x80) - 0x01` and `(d + 0x76)` never borrow or carry across byte lanes.
    let bslash_msbs = swar_zero_byte_msbs_ascii(w ^ 0x5C5C_5C5C_5C5C_5C5C);
    if bslash_msbs != 0 {
        return Err(ScanAbort::Fallback);
    }
    let quote_msbs = swar_zero_byte_msbs_ascii(w ^ 0x2222_2222_2222_2222);
    let ctrl_msbs = swar_zero_byte_msbs_ascii(w & 0x6060_6060_6060_6060);
    let w_or_20 = w | 0x2020_2020_2020_2020;
    let brace_msbs = swar_zero_byte_msbs_ascii(w_or_20 ^ 0x7B7B_7B7B_7B7B_7B7B)
        | swar_zero_byte_msbs_ascii(w_or_20 ^ 0x7D7D_7D7D_7D7D_7D7D);
    let d = w ^ 0x3030_3030_3030_3030;
    let digit_msbs = !d.wrapping_add(0x7676_7676_7676_7676) & 0x8080_8080_8080_8080;
    Ok((
        swar_pack_msbs(quote_msbs),
        swar_pack_msbs(ctrl_msbs),
        swar_pack_msbs(brace_msbs),
        swar_pack_msbs(digit_msbs),
    ))
}

#[inline(always)]
fn scan_tail_swar_scalar(
    chunk: &[u8],
    mut offset: usize,
    mut quotes: u64,
    mut ctrls: u64,
    mut braces: u64,
    mut digits: u64,
) -> std::result::Result<BlockMasks, ScanAbort> {
    let len = chunk.len();
    while offset + 8 <= len {
        let mut raw = [0u8; 8];
        raw.copy_from_slice(&chunk[offset..offset + 8]);
        let (q, c, b, d) = scan_u64_swar(u64::from_le_bytes(raw))?;
        quotes |= q << offset;
        ctrls |= c << offset;
        braces |= b << offset;
        digits |= d << offset;
        offset += 8;
    }
    while offset < len {
        let byte = chunk[offset];
        if byte >= 0x80 || byte == b'\\' {
            return Err(ScanAbort::Fallback);
        }
        let bit = 1u64 << offset;
        if byte == b'"' {
            quotes |= bit;
        }
        if byte <= 0x1F {
            ctrls |= bit;
        }
        if (byte | 0x20) == b'{' || (byte | 0x20) == b'}' {
            braces |= bit;
        }
        if byte.is_ascii_digit() {
            digits |= bit;
        }
        offset += 1;
    }
    Ok(BlockMasks {
        quotes,
        ctrls,
        braces,
        digits,
    })
}

pub(crate) fn scan_block_swar(chunk: &[u8]) -> std::result::Result<BlockMasks, ScanAbort> {
    scan_tail_swar_scalar(chunk, 0, 0, 0, 0, 0)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
#[inline]
fn scan_16_sse2(
    chunk: &[u8],
    offset: usize,
) -> std::result::Result<(u64, u64, u64, u64), ScanAbort> {
    use core::arch::x86_64::*;
    debug_assert!(offset + 16 <= chunk.len());
    // SAFETY: Caller verified `offset + 16 <= chunk.len()`, so `chunk.as_ptr().add(offset)`
    // points to 16 initialized, immutable bytes within `chunk`. `_mm_loadu_si128` performs an
    // unaligned 16-byte load with 1-byte alignment requirement (`align_of::<u8>() == 1`).
    let v = unsafe { _mm_loadu_si128(chunk.as_ptr().add(offset).cast::<__m128i>()) };
    let non_ascii = _mm_movemask_epi8(v) as u32;
    let bslash = _mm_movemask_epi8(_mm_cmpeq_epi8(v, _mm_set1_epi8(b'\\' as i8))) as u32;
    if (non_ascii | bslash) != 0 {
        return Err(ScanAbort::Fallback);
    }
    let q = _mm_movemask_epi8(_mm_cmpeq_epi8(v, _mm_set1_epi8(b'"' as i8))) as u32 as u64;
    let c =
        _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(v, _mm_set1_epi8(0x1F)), v)) as u32 as u64;
    let v_or_20 = _mm_or_si128(v, _mm_set1_epi8(0x20));
    let b = _mm_movemask_epi8(_mm_or_si128(
        _mm_cmpeq_epi8(v_or_20, _mm_set1_epi8(b'{' as i8)),
        _mm_cmpeq_epi8(v_or_20, _mm_set1_epi8(b'}' as i8)),
    )) as u32 as u64;
    let d_sub = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
    let d = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(d_sub, _mm_set1_epi8(9)), d_sub)) as u32
        as u64;
    Ok((q, c, b, d))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
#[inline]
pub(crate) unsafe fn scan_block_sse2(chunk: &[u8]) -> std::result::Result<BlockMasks, ScanAbort> {
    let len = chunk.len();
    let mut offset = 0usize;
    let mut quotes = 0u64;
    let mut ctrls = 0u64;
    let mut braces = 0u64;
    let mut digits = 0u64;
    while offset + 16 <= len {
        let (q, c, b, d) = scan_16_sse2(chunk, offset)?;
        quotes |= q << offset;
        ctrls |= c << offset;
        braces |= b << offset;
        digits |= d << offset;
        offset += 16;
    }
    scan_tail_swar_scalar(chunk, offset, quotes, ctrls, braces, digits)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
pub(crate) unsafe fn scan_block_avx2(chunk: &[u8]) -> std::result::Result<BlockMasks, ScanAbort> {
    use core::arch::x86_64::*;
    let len = chunk.len();
    let mut offset = 0usize;
    let mut quotes = 0u64;
    let mut ctrls = 0u64;
    let mut braces = 0u64;
    let mut digits = 0u64;
    while offset + 32 <= len {
        // SAFETY: `offset + 32 <= chunk.len()` holds by the loop guard, so
        // `chunk.as_ptr().add(offset)` points to 32 initialized, immutable bytes inside `chunk`.
        // `_mm256_loadu_si256` performs an unaligned 32-byte load (`vmovdqu`) with 1-byte
        // alignment requirement, and `avx2` is enabled on this function.
        let v = unsafe { _mm256_loadu_si256(chunk.as_ptr().add(offset).cast::<__m256i>()) };
        let non_ascii = _mm256_movemask_epi8(v) as u32;
        let bslash =
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'\\' as i8))) as u32;
        if (non_ascii | bslash) != 0 {
            return Err(ScanAbort::Fallback);
        }
        // Cast `i32 -> u32 -> u64` so bit 31 never sign-extends into bits 32..64.
        let q =
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'"' as i8))) as u32 as u64;
        let c = _mm256_movemask_epi8(_mm256_cmpeq_epi8(
            _mm256_min_epu8(v, _mm256_set1_epi8(0x1F)),
            v,
        )) as u32 as u64;
        let v_or_20 = _mm256_or_si256(v, _mm256_set1_epi8(0x20));
        let b = _mm256_movemask_epi8(_mm256_or_si256(
            _mm256_cmpeq_epi8(v_or_20, _mm256_set1_epi8(b'{' as i8)),
            _mm256_cmpeq_epi8(v_or_20, _mm256_set1_epi8(b'}' as i8)),
        )) as u32 as u64;
        let d_sub = _mm256_sub_epi8(v, _mm256_set1_epi8(b'0' as i8));
        let d = _mm256_movemask_epi8(_mm256_cmpeq_epi8(
            _mm256_min_epu8(d_sub, _mm256_set1_epi8(9)),
            d_sub,
        )) as u32 as u64;
        quotes |= q << offset;
        ctrls |= c << offset;
        braces |= b << offset;
        digits |= d << offset;
        offset += 32;
    }
    if offset + 16 <= len {
        let (q, c, b, d) = scan_16_sse2(chunk, offset)?;
        quotes |= q << offset;
        ctrls |= c << offset;
        braces |= b << offset;
        digits |= d << offset;
        offset += 16;
    }
    scan_tail_swar_scalar(chunk, offset, quotes, ctrls, braces, digits)
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
#[inline]
fn neon_movemask(cmp: core::arch::aarch64::uint8x16_t) -> u64 {
    use core::arch::aarch64::*;
    const WEIGHTS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
    // SAFETY: `WEIGHTS` is a 16-byte initialized array in static read-only memory; `vld1q_u8`
    // performs an unaligned 16-byte load with 1-byte alignment requirement.
    let weights = unsafe { vld1q_u8(WEIGHTS.as_ptr()) };
    let masked = vandq_u8(cmp, weights);
    let lo = vaddv_u8(vget_low_u8(masked)) as u64;
    let hi = (vaddv_u8(vget_high_u8(masked)) as u64) << 8;
    lo | hi
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
#[inline]
pub(crate) unsafe fn scan_block_neon(chunk: &[u8]) -> std::result::Result<BlockMasks, ScanAbort> {
    use core::arch::aarch64::*;
    let len = chunk.len();
    let mut offset = 0usize;
    let mut quotes = 0u64;
    let mut ctrls = 0u64;
    let mut braces = 0u64;
    let mut digits = 0u64;
    while offset + 16 <= len {
        // SAFETY: `offset + 16 <= chunk.len()` holds by the loop guard, so
        // `chunk.as_ptr().add(offset)` points to 16 initialized, immutable bytes inside `chunk`.
        // `vld1q_u8` performs an unaligned 16-byte load (`ld1`) with 1-byte alignment requirement.
        let v = unsafe { vld1q_u8(chunk.as_ptr().add(offset)) };
        let non_ascii = vcgeq_u8(v, vdupq_n_u8(0x80));
        let bslash = vceqq_u8(v, vdupq_n_u8(b'\\'));
        if vmaxvq_u8(vorrq_u8(non_ascii, bslash)) != 0 {
            return Err(ScanAbort::Fallback);
        }
        let q = neon_movemask(vceqq_u8(v, vdupq_n_u8(b'"')));
        let c = neon_movemask(vcleq_u8(v, vdupq_n_u8(0x1F)));
        let v_or_20 = vorrq_u8(v, vdupq_n_u8(0x20));
        let b = neon_movemask(vorrq_u8(
            vceqq_u8(v_or_20, vdupq_n_u8(b'{')),
            vceqq_u8(v_or_20, vdupq_n_u8(b'}')),
        ));
        let d_sub = vsubq_u8(v, vdupq_n_u8(b'0'));
        let d = neon_movemask(vcleq_u8(d_sub, vdupq_n_u8(9)));
        quotes |= q << offset;
        ctrls |= c << offset;
        braces |= b << offset;
        digits |= d << offset;
        offset += 16;
    }
    scan_tail_swar_scalar(chunk, offset, quotes, ctrls, braces, digits)
}

#[inline(always)]
const fn is_json_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

/// Converts 8 proven ASCII digit bytes (`'0'..='9'`) into `0..=99_999_999` without overflow.
#[inline(always)]
const fn parse_8_digits_proven(raw: [u8; 8]) -> u64 {
    let d = u64::from_le_bytes(raw) ^ 0x3030_3030_3030_3030;
    let v = ((d & 0x00FF_00FF_00FF_00FF) * 10) + ((d >> 8) & 0x00FF_00FF_00FF_00FF);
    let v = ((v & 0x0000_FFFF_0000_FFFF) * 100) + ((v >> 16) & 0x0000_FFFF_0000_FFFF);
    ((v & 0xFFFF_FFFF) * 10_000) + (v >> 32)
}

/// Converts a slice of `1..=20` proven ASCII digits (`'0'..='9'`) into `u64`.
#[inline(always)]
fn convert_proven_digits_u64(digits: &[u8]) -> Option<u64> {
    let len = digits.len();
    if len == 0 || len > 20 {
        return None;
    }
    if len <= 19 {
        let mut i = 0usize;
        let mut acc = 0u64;
        while i + 8 <= len {
            let mut raw = [0u8; 8];
            raw.copy_from_slice(&digits[i..i + 8]);
            acc = acc * 100_000_000 + parse_8_digits_proven(raw);
            i += 8;
        }
        while i < len {
            acc = acc * 10 + (digits[i] - b'0') as u64;
            i += 1;
        }
        return Some(acc);
    }
    // Exactly 20 digits: check overflow against `u64::MAX`.
    let mut raw0 = [0u8; 8];
    let mut raw1 = [0u8; 8];
    raw0.copy_from_slice(&digits[0..8]);
    raw1.copy_from_slice(&digits[8..16]);
    let hi16 = parse_8_digits_proven(raw0) * 100_000_000 + parse_8_digits_proven(raw1);
    let lo4 = ((digits[16] - b'0') as u64) * 1000
        + ((digits[17] - b'0') as u64) * 100
        + ((digits[18] - b'0') as u64) * 10
        + ((digits[19] - b'0') as u64);
    hi16.checked_mul(10_000)?.checked_add(lo4)
}

/// Exact integer tick/lot parser matching `Decimal::parse(s)` + `tickwire_binance::feed::units`
/// + positive level check (`n > 0`), using zero floating-point operations.
#[inline(always)]
pub fn parse_positive_units(bytes: &[u8], step: &StepSpec) -> Result<i64> {
    let len = bytes.len();
    if len == 0 || len > 19 || step.mantissa_u64 == 0 || bytes[0] == b'-' {
        return parse_positive_units_cold(bytes, step.raw);
    }
    let mut mantissa = 0u64;
    let mut dot_pos = usize::MAX;
    let mut i = 0usize;
    while i < len {
        let b = bytes[i];
        let d = b.wrapping_sub(b'0');
        if d <= 9 {
            mantissa = mantissa * 10 + d as u64;
        } else if b == b'.' && i > 0 && dot_pos == usize::MAX {
            dot_pos = i;
        } else {
            return Err(invalid("off-grid or overflowing spot price/size"));
        }
        i += 1;
    }
    let scale = if dot_pos == usize::MAX {
        0u8
    } else {
        let frac = len - 1 - dot_pos;
        if frac == 0 {
            return Err(invalid("off-grid or overflowing spot price/size"));
        }
        frac as u8
    };
    if mantissa == 0 {
        return Err(invalid("nonpositive spot quote level"));
    }
    if scale <= step.scale {
        let diff = (step.scale - scale) as usize;
        if diff <= 19
            && let Some(scaled) = mantissa.checked_mul(POW10_U64[diff])
        {
            if step.mantissa_u64 == 1 {
                if scaled <= i64::MAX as u64 {
                    return Ok(scaled as i64);
                }
                return Err(invalid("off-grid or overflowing spot price/size"));
            }
            if !scaled.is_multiple_of(step.mantissa_u64) {
                return Err(invalid("off-grid or overflowing spot price/size"));
            }
            let q = scaled / step.mantissa_u64;
            if q <= i64::MAX as u64 {
                return Ok(q as i64);
            }
            return Err(invalid("off-grid or overflowing spot price/size"));
        }
        return parse_positive_units_from_parts(mantissa as i128, scale, step.raw);
    }
    let extra = (scale - step.scale) as usize;
    if extra > 19 {
        // `0 < mantissa < 10^19 < 10^extra`, so `mantissa` cannot be divisible by `10^extra`.
        return Err(invalid("off-grid or overflowing spot price/size"));
    }
    let p10 = POW10_U64[extra];
    if !mantissa.is_multiple_of(p10) {
        return Err(invalid("off-grid or overflowing spot price/size"));
    }
    let reduced = mantissa / p10;
    if !reduced.is_multiple_of(step.mantissa_u64) {
        return Err(invalid("off-grid or overflowing spot price/size"));
    }
    let q = reduced / step.mantissa_u64;
    if q <= i64::MAX as u64 {
        Ok(q as i64)
    } else {
        Err(invalid("off-grid or overflowing spot price/size"))
    }
}

#[cold]
fn parse_positive_units_from_parts(mantissa: i128, scale: u8, step: Decimal) -> Result<i64> {
    let d = Decimal::new(mantissa, scale)
        .map_err(|_| invalid("off-grid or overflowing spot price/size"))?;
    let n = tickwire_binance::feed::units(d, step)
        .map_err(|_| invalid("off-grid or overflowing spot price/size"))?;
    if n <= 0 {
        return Err(invalid("nonpositive spot quote level"));
    }
    Ok(n)
}

#[cold]
fn parse_positive_units_cold(bytes: &[u8], step: Decimal) -> Result<i64> {
    let s = core::str::from_utf8(bytes)
        .map_err(|_| invalid("off-grid or overflowing spot price/size"))?;
    let n = Decimal::parse(s)
        .and_then(|d| tickwire_binance::feed::units(d, step))
        .map_err(|_| invalid("off-grid or overflowing spot price/size"))?;
    if n <= 0 {
        return Err(invalid("nonpositive spot quote level"));
    }
    Ok(n)
}

#[inline(always)]
fn parse_okx_millis(bytes: &[u8]) -> Result<u64> {
    let len = bytes.len();
    if (1..=19).contains(&len) && bytes[0].is_ascii_digit() && bytes[0] != b'0' {
        let mut i = 0usize;
        while i < len && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == len
            && let Some(v) = convert_proven_digits_u64(bytes)
        {
            return Ok(v);
        }
    }
    let s = core::str::from_utf8(bytes)?;
    Ok(s.parse::<u64>()?)
}

/// Register-resident 64-byte block SIMD JSON scanner up to `MAX_MESSAGE` (16 KiB).
struct Scanner<'a> {
    bytes: &'a [u8],
    backend: Backend,
    pos: usize,
    block_base: usize,
    block_end: usize,
    scanned_to: usize,
    prev_in_string: u64,
    quotes: u64,
    digits: u64,
}

impl<'a> Scanner<'a> {
    #[inline(always)]
    fn new(bytes: &'a [u8], backend: Backend) -> std::result::Result<Self, ParseAbort> {
        let mut s = Self {
            bytes,
            backend,
            pos: 0,
            block_base: 0,
            block_end: 0,
            scanned_to: 0,
            prev_in_string: 0,
            quotes: 0,
            digits: 0,
        };
        if !bytes.is_empty() {
            s.load_next_block()?;
        }
        Ok(s)
    }

    #[inline(always)]
    fn load_next_block(&mut self) -> std::result::Result<(), ParseAbort> {
        let base = self.scanned_to;
        let end = (base + 64).min(self.bytes.len());
        let masks = scan_block_64(&self.bytes[base..end], self.backend)
            .map_err(|ScanAbort::Fallback| ParseAbort::Fallback)?;
        let in_string = prefix_xor(masks.quotes) ^ self.prev_in_string;
        if (masks.ctrls & in_string) != 0 {
            return Err(ParseAbort::InvalidJson);
        }
        self.prev_in_string = ((in_string as i64) >> 63) as u64;
        if end == self.bytes.len() && self.prev_in_string != 0 {
            return Err(ParseAbort::InvalidJson);
        }
        self.block_base = base;
        self.block_end = end;
        self.scanned_to = end;
        self.quotes = masks.quotes;
        self.digits = masks.digits;
        Ok(())
    }

    #[inline(always)]
    fn ensure_pos(&mut self) -> std::result::Result<bool, ParseAbort> {
        if self.pos >= self.bytes.len() {
            return Ok(false);
        }
        while self.pos >= self.block_end {
            self.load_next_block()?;
        }
        Ok(true)
    }

    #[inline(always)]
    fn skip_ws(&mut self) -> std::result::Result<(), ParseAbort> {
        if self.pos < self.block_end && !is_json_ws(self.bytes[self.pos]) {
            return Ok(());
        }
        self.skip_ws_slow()
    }

    #[cold]
    fn skip_ws_slow(&mut self) -> std::result::Result<(), ParseAbort> {
        while self.ensure_pos()? && is_json_ws(self.bytes[self.pos]) {
            self.pos += 1;
        }
        Ok(())
    }

    #[inline(always)]
    fn peek(&mut self) -> std::result::Result<u8, ParseAbort> {
        if self.pos < self.block_end {
            return Ok(self.bytes[self.pos]);
        }
        if !self.ensure_pos()? {
            return Err(ParseAbort::InvalidJson);
        }
        Ok(self.bytes[self.pos])
    }

    #[inline(always)]
    fn expect_byte(&mut self, expected: u8) -> std::result::Result<(), ParseAbort> {
        if self.pos < self.block_end && self.bytes[self.pos] == expected {
            self.pos += 1;
            return Ok(());
        }
        self.expect_byte_slow(expected)
    }

    #[cold]
    fn expect_byte_slow(&mut self, expected: u8) -> std::result::Result<(), ParseAbort> {
        self.skip_ws()?;
        if !self.ensure_pos()? || self.bytes[self.pos] != expected {
            return Err(ParseAbort::InvalidJson);
        }
        self.pos += 1;
        Ok(())
    }

    /// Reads a `"..."` JSON string slice using the SIMD `quotes` bitmask.
    #[inline(always)]
    fn read_str(&mut self) -> std::result::Result<&'a [u8], ParseAbort> {
        if self.pos + 1 < self.block_end && self.bytes[self.pos] == b'"' {
            let start = self.pos + 1;
            let m = self.quotes >> (start - self.block_base);
            if m != 0 {
                let end = start + m.trailing_zeros() as usize;
                self.pos = end + 1;
                return Ok(&self.bytes[start..end]);
            }
        }
        self.read_str_slow()
    }

    #[cold]
    fn read_str_slow(&mut self) -> std::result::Result<&'a [u8], ParseAbort> {
        self.skip_ws()?;
        if !self.ensure_pos()? || self.bytes[self.pos] != b'"' {
            return Err(ParseAbort::InvalidJson);
        }
        let start = self.pos + 1;
        self.pos = start;
        loop {
            if !self.ensure_pos()? {
                return Err(ParseAbort::InvalidJson);
            }
            let rel = self.pos - self.block_base;
            let m = self.quotes >> rel;
            if m != 0 {
                let end = self.pos + m.trailing_zeros() as usize;
                self.pos = end + 1;
                return Ok(&self.bytes[start..end]);
            }
            self.pos = self.block_end;
        }
    }

    #[inline(always)]
    fn read_literal(&mut self, lit: &[u8]) -> std::result::Result<(), ParseAbort> {
        let n = lit.len();
        if self.pos + n <= self.bytes.len() && &self.bytes[self.pos..self.pos + n] == lit {
            self.pos += n;
            while self.pos > self.scanned_to {
                self.load_next_block()?;
            }
            return Ok(());
        }
        Err(ParseAbort::InvalidJson)
    }

    #[inline(always)]
    fn try_read_null(&mut self) -> std::result::Result<bool, ParseAbort> {
        self.skip_ws()?;
        if self.peek()? == b'n' {
            self.read_literal(b"null")?;
            return Ok(true);
        }
        Ok(false)
    }

    #[inline(always)]
    fn read_opt_str(&mut self) -> std::result::Result<Option<&'a [u8]>, ParseAbort> {
        if self.pos + 1 < self.block_end && self.bytes[self.pos] == b'"' {
            return self.read_str().map(Some);
        }
        if self.try_read_null()? {
            return Ok(None);
        }
        self.read_str().map(Some)
    }

    #[inline(always)]
    fn read_opt_bool(&mut self) -> std::result::Result<Option<bool>, ParseAbort> {
        self.skip_ws()?;
        match self.peek()? {
            b't' => {
                self.read_literal(b"true")?;
                Ok(Some(true))
            }
            b'f' => {
                self.read_literal(b"false")?;
                Ok(Some(false))
            }
            b'n' => {
                self.read_literal(b"null")?;
                Ok(None)
            }
            _ => Err(ParseAbort::InvalidJson),
        }
    }

    /// Reads an unquoted RFC 8259 non-negative integer into `u64` using SIMD `digits` bitmasks.
    #[inline(always)]
    fn read_u64(&mut self) -> std::result::Result<u64, ParseAbort> {
        if self.pos < self.block_end {
            let start = self.pos;
            let run = (!(self.digits >> (start - self.block_base))).trailing_zeros() as usize;
            let end = start + run;
            if run > 0 && end < self.block_end {
                if run > 1 && self.bytes[start] == b'0' {
                    return Err(ParseAbort::InvalidJson);
                }
                if matches!(self.bytes[end], b'.' | b'e' | b'E') {
                    return Err(ParseAbort::InvalidJson);
                }
                let val = convert_proven_digits_u64(&self.bytes[start..end])
                    .ok_or(ParseAbort::InvalidJson)?;
                self.pos = end;
                return Ok(val);
            }
        }
        self.read_u64_slow()
    }

    #[cold]
    fn read_u64_slow(&mut self) -> std::result::Result<u64, ParseAbort> {
        self.skip_ws()?;
        let first = self.peek()?;
        if !first.is_ascii_digit() {
            return Err(ParseAbort::InvalidJson);
        }
        let start = self.pos;
        self.pos += 1;
        if first == b'0' {
            if self.ensure_pos()?
                && (self.bytes[self.pos].is_ascii_digit()
                    || matches!(self.bytes[self.pos], b'.' | b'e' | b'E'))
            {
                return Err(ParseAbort::InvalidJson);
            }
            return Ok(0);
        }
        while self.ensure_pos()? && self.bytes[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        if self.pos < self.bytes.len() && matches!(self.bytes[self.pos], b'.' | b'e' | b'E') {
            return Err(ParseAbort::InvalidJson);
        }
        convert_proven_digits_u64(&self.bytes[start..self.pos]).ok_or(ParseAbort::InvalidJson)
    }

    #[inline(always)]
    fn read_opt_u64(&mut self) -> std::result::Result<Option<u64>, ParseAbort> {
        if self.pos < self.block_end && self.bytes[self.pos].is_ascii_digit() {
            return self.read_u64().map(Some);
        }
        if self.try_read_null()? {
            return Ok(None);
        }
        self.read_u64().map(Some)
    }

    /// Reads `Side<[&'a str; N]>` (`N == 2` for Bybit, `N == 4` for OKX): zero or one price level,
    /// returning the `[price, qty]` pair while strictly validating any trailing row strings.
    #[inline(always)]
    fn read_side<const N: usize>(
        &mut self,
    ) -> std::result::Result<Option<[&'a [u8]; 2]>, ParseAbort> {
        self.expect_byte(b'[')?;
        self.skip_ws()?;
        if self.peek()? == b']' {
            self.pos += 1;
            return Ok(None);
        }
        self.expect_byte(b'[')?;
        let p = self.read_str()?;
        self.expect_byte(b',')?;
        let q = self.read_str()?;
        let mut idx = 2usize;
        while idx < N {
            self.expect_byte(b',')?;
            let _ = self.read_str()?;
            idx += 1;
        }
        self.expect_byte(b']')?;
        self.expect_byte(b']')?;
        Ok(Some([p, q]))
    }

    fn skip_value(&mut self, depth: u8) -> std::result::Result<(), ParseAbort> {
        if depth >= 32 {
            return Err(ParseAbort::Fallback);
        }
        self.skip_ws()?;
        match self.peek()? {
            b'"' => {
                let _ = self.read_str()?;
                Ok(())
            }
            b't' => self.read_literal(b"true"),
            b'f' => self.read_literal(b"false"),
            b'n' => self.read_literal(b"null"),
            b'-' | b'0'..=b'9' => self.skip_number(),
            b'[' => {
                self.pos += 1;
                self.skip_ws()?;
                if self.peek()? == b']' {
                    self.pos += 1;
                    return Ok(());
                }
                loop {
                    self.skip_value(depth + 1)?;
                    self.skip_ws()?;
                    match self.peek()? {
                        b',' => self.pos += 1,
                        b']' => {
                            self.pos += 1;
                            return Ok(());
                        }
                        _ => return Err(ParseAbort::InvalidJson),
                    }
                }
            }
            b'{' => {
                self.pos += 1;
                self.skip_ws()?;
                if self.peek()? == b'}' {
                    self.pos += 1;
                    return Ok(());
                }
                loop {
                    self.skip_ws()?;
                    if self.peek()? != b'"' {
                        return Err(ParseAbort::InvalidJson);
                    }
                    let _ = self.read_str()?;
                    self.expect_byte(b':')?;
                    self.skip_value(depth + 1)?;
                    self.skip_ws()?;
                    match self.peek()? {
                        b',' => self.pos += 1,
                        b'}' => {
                            self.pos += 1;
                            return Ok(());
                        }
                        _ => return Err(ParseAbort::InvalidJson),
                    }
                }
            }
            _ => Err(ParseAbort::InvalidJson),
        }
    }

    fn skip_number(&mut self) -> std::result::Result<(), ParseAbort> {
        if self.peek()? == b'-' {
            self.pos += 1;
        }
        let first = self.peek()?;
        if !first.is_ascii_digit() {
            return Err(ParseAbort::InvalidJson);
        }
        self.pos += 1;
        if first == b'0' {
            if self.ensure_pos()? && self.bytes[self.pos].is_ascii_digit() {
                return Err(ParseAbort::InvalidJson);
            }
        } else {
            let mut count = 1usize;
            while self.ensure_pos()? && self.bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
                count += 1;
                if count > 19 {
                    return Err(ParseAbort::Fallback);
                }
            }
        }
        if self.ensure_pos()? && matches!(self.bytes[self.pos], b'.' | b'e' | b'E') {
            return Err(ParseAbort::Fallback);
        }
        Ok(())
    }

    #[inline(always)]
    fn finish(&mut self) -> std::result::Result<(), ParseAbort> {
        while self.pos < self.bytes.len() {
            if !is_json_ws(self.bytes[self.pos]) {
                return Err(ParseAbort::InvalidJson);
            }
            self.pos += 1;
        }
        Ok(())
    }
}

struct RawBybitBook<'a> {
    s: &'a [u8],
    u: u64,
    seq: u64,
    b: Option<[&'a [u8]; 2]>,
    a: Option<[&'a [u8]; 2]>,
}

struct RawOkxArg<'a> {
    channel: &'a [u8],
    inst_id: &'a [u8],
}

struct RawOkxBook<'a> {
    ts: &'a [u8],
    seq_id: u64,
    bids: Option<[&'a [u8]; 2]>,
    asks: Option<[&'a [u8]; 2]>,
}

#[inline(always)]
fn mark_seen(seen: &mut u16, bit: u16) -> std::result::Result<(), ParseAbort> {
    if (*seen & bit) != 0 {
        return Err(ParseAbort::InvalidJson);
    }
    *seen |= bit;
    Ok(())
}

impl<'a> Scanner<'a> {
    fn parse_bybit_book(&mut self) -> std::result::Result<RawBybitBook<'a>, ParseAbort> {
        self.expect_byte(b'{')?;
        self.skip_ws()?;
        if self.peek()? == b'}' {
            return Err(ParseAbort::InvalidJson);
        }
        let mut seen = 0u16;
        let mut s = None;
        let mut u = 0u64;
        let mut seq = 0u64;
        let mut b = None;
        let mut a = None;
        loop {
            let key = self.read_str()?;
            self.expect_byte(b':')?;
            match key {
                b"s" => {
                    mark_seen(&mut seen, 1 << 0)?;
                    s = Some(self.read_str()?);
                }
                b"u" => {
                    mark_seen(&mut seen, 1 << 1)?;
                    u = self.read_u64()?;
                }
                b"seq" => {
                    mark_seen(&mut seen, 1 << 2)?;
                    seq = self.read_u64()?;
                }
                b"b" => {
                    mark_seen(&mut seen, 1 << 3)?;
                    b = self.read_side::<2>()?;
                }
                b"a" => {
                    mark_seen(&mut seen, 1 << 4)?;
                    a = self.read_side::<2>()?;
                }
                _ => self.skip_value(1)?,
            }
            if self.pos < self.block_end && self.bytes[self.pos] == b',' {
                self.pos += 1;
                continue;
            }
            self.skip_ws()?;
            match self.peek()? {
                b',' => self.pos += 1,
                b'}' => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(ParseAbort::InvalidJson),
            }
        }
        if seen != 0b1_1111 {
            return Err(ParseAbort::InvalidJson);
        }
        Ok(RawBybitBook {
            s: s.ok_or(ParseAbort::InvalidJson)?,
            u,
            seq,
            b,
            a,
        })
    }

    fn parse_okx_arg(&mut self) -> std::result::Result<RawOkxArg<'a>, ParseAbort> {
        self.expect_byte(b'{')?;
        self.skip_ws()?;
        if self.peek()? == b'}' {
            return Err(ParseAbort::InvalidJson);
        }
        let mut seen = 0u16;
        let mut channel = None;
        let mut inst_id = None;
        loop {
            let key = self.read_str()?;
            self.expect_byte(b':')?;
            match key {
                b"channel" => {
                    mark_seen(&mut seen, 1 << 0)?;
                    channel = Some(self.read_str()?);
                }
                b"instId" => {
                    mark_seen(&mut seen, 1 << 1)?;
                    inst_id = Some(self.read_str()?);
                }
                _ => self.skip_value(1)?,
            }
            if self.pos < self.block_end && self.bytes[self.pos] == b',' {
                self.pos += 1;
                continue;
            }
            self.skip_ws()?;
            match self.peek()? {
                b',' => self.pos += 1,
                b'}' => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(ParseAbort::InvalidJson),
            }
        }
        if seen != 0b11 {
            return Err(ParseAbort::InvalidJson);
        }
        Ok(RawOkxArg {
            channel: channel.ok_or(ParseAbort::InvalidJson)?,
            inst_id: inst_id.ok_or(ParseAbort::InvalidJson)?,
        })
    }

    fn parse_okx_book(&mut self) -> std::result::Result<RawOkxBook<'a>, ParseAbort> {
        self.expect_byte(b'{')?;
        self.skip_ws()?;
        if self.peek()? == b'}' {
            return Err(ParseAbort::InvalidJson);
        }
        let mut seen = 0u16;
        let mut ts = None;
        let mut seq_id = 0u64;
        let mut bids = None;
        let mut asks = None;
        loop {
            let key = self.read_str()?;
            self.expect_byte(b':')?;
            match key {
                b"ts" => {
                    mark_seen(&mut seen, 1 << 0)?;
                    ts = Some(self.read_str()?);
                }
                b"seqId" => {
                    mark_seen(&mut seen, 1 << 1)?;
                    seq_id = self.read_u64()?;
                }
                b"bids" => {
                    mark_seen(&mut seen, 1 << 2)?;
                    bids = self.read_side::<4>()?;
                }
                b"asks" => {
                    mark_seen(&mut seen, 1 << 3)?;
                    asks = self.read_side::<4>()?;
                }
                _ => self.skip_value(2)?,
            }
            if self.pos < self.block_end && self.bytes[self.pos] == b',' {
                self.pos += 1;
                continue;
            }
            self.skip_ws()?;
            match self.peek()? {
                b',' => self.pos += 1,
                b'}' => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(ParseAbort::InvalidJson),
            }
        }
        if seen != 0b1111 {
            return Err(ParseAbort::InvalidJson);
        }
        Ok(RawOkxBook {
            ts: ts.ok_or(ParseAbort::InvalidJson)?,
            seq_id,
            bids,
            asks,
        })
    }
}

#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn build_quote<'a>(
    config: &'a QuoteConfig,
    epoch: u64,
    price_step: &StepSpec,
    qty_step: &StepSpec,
    millis: u64,
    received: u64,
    sequence: u64,
    update: Option<u64>,
    bid: Option<[&[u8]; 2]>,
    ask: Option<[&[u8]; 2]>,
) -> Result<Quote<'a>> {
    let exchange_event_ns = millis
        .checked_mul(1_000_000)
        .ok_or_else(|| invalid("exchange timestamp overflow"))?;
    let bid = match bid {
        Some(row) => Some(QuoteLevel {
            price_ticks: parse_positive_units(row[0], price_step)?,
            quantity_lots: parse_positive_units(row[1], qty_step)?,
        }),
        None => None,
    };
    let ask = match ask {
        Some(row) => Some(QuoteLevel {
            price_ticks: parse_positive_units(row[0], price_step)?,
            quantity_lots: parse_positive_units(row[1], qty_step)?,
        }),
        None => None,
    };
    Ok(Quote {
        venue_id: config.venue.id(),
        symbol: &config.symbol,
        instrument_id: config.instrument_id,
        instrument_version: config.instrument_version,
        connection_epoch: epoch,
        venue_sequence: sequence,
        update_id: update,
        exchange_event_ns,
        received_mono_ns: received,
        bid,
        ask,
    })
}

#[inline]
#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_bybit<'a>(
    bytes: &[u8],
    received: u64,
    backend: Backend,
    config: &'a QuoteConfig,
    epoch: u64,
    channel: &str,
    acknowledged: &mut bool,
    price_step: &StepSpec,
    qty_step: &StepSpec,
) -> FastOutcome<Option<Quote<'a>>> {
    let parsed = (|| -> std::result::Result<_, ParseAbort> {
        let mut s = Scanner::new(bytes, backend)?;
        s.expect_byte(b'{')?;
        s.skip_ws()?;
        let mut seen = 0u16;
        let mut topic = None;
        let mut kind = None;
        let mut ts = None;
        let mut op = None;
        let mut req_id = None;
        let mut success = None;
        let mut ret_msg = None;
        let mut data = None;
        if s.peek()? == b'}' {
            s.pos += 1;
        } else {
            loop {
                let key = s.read_str()?;
                s.expect_byte(b':')?;
                match key {
                    b"topic" => {
                        mark_seen(&mut seen, 1 << 0)?;
                        topic = s.read_opt_str()?;
                    }
                    b"type" => {
                        mark_seen(&mut seen, 1 << 1)?;
                        kind = s.read_opt_str()?;
                    }
                    b"ts" => {
                        mark_seen(&mut seen, 1 << 2)?;
                        ts = s.read_opt_u64()?;
                    }
                    b"op" => {
                        mark_seen(&mut seen, 1 << 3)?;
                        op = s.read_opt_str()?;
                    }
                    b"req_id" => {
                        mark_seen(&mut seen, 1 << 4)?;
                        req_id = s.read_opt_str()?;
                    }
                    b"success" => {
                        mark_seen(&mut seen, 1 << 5)?;
                        success = s.read_opt_bool()?;
                    }
                    b"ret_msg" => {
                        mark_seen(&mut seen, 1 << 6)?;
                        ret_msg = s.read_opt_str()?;
                    }
                    b"data" => {
                        mark_seen(&mut seen, 1 << 7)?;
                        data = if s.try_read_null()? {
                            None
                        } else {
                            Some(s.parse_bybit_book()?)
                        };
                    }
                    _ => s.skip_value(0)?,
                }
                if s.pos < s.block_end && s.bytes[s.pos] == b',' {
                    s.pos += 1;
                    continue;
                }
                s.skip_ws()?;
                match s.peek()? {
                    b',' => s.pos += 1,
                    b'}' => {
                        s.pos += 1;
                        break;
                    }
                    _ => return Err(ParseAbort::InvalidJson),
                }
            }
        }
        s.finish()?;
        Ok((topic, kind, ts, op, req_id, success, ret_msg, data))
    })();

    let (topic, kind, ts, op, req_id, success, ret_msg, data) = match parsed {
        Ok(v) => v,
        Err(ParseAbort::Fallback) => return FastOutcome::Fallback,
        Err(ParseAbort::InvalidJson) => {
            return FastOutcome::Decoded(Err(invalid("malformed Bybit spot JSON frame")));
        }
    };

    if let Some(op) = op {
        if op == b"subscribe" && req_id == Some(b"twbbo".as_slice()) && success == Some(true) {
            *acknowledged = true;
        } else if !(op == b"pong"
            || op == b"ping" && success == Some(true) && ret_msg == Some(b"pong".as_slice()))
        {
            return FastOutcome::Decoded(Err(invalid(
                "Bybit public subscription rejected or unexpected control message",
            )));
        }
        return FastOutcome::Decoded(Ok(None));
    }

    let Some(d) = data else {
        return FastOutcome::Decoded(Err(invalid("Bybit snapshot data")));
    };
    if !*acknowledged
        || topic != Some(channel.as_bytes())
        || kind != Some(b"snapshot".as_slice())
        || d.s != config.symbol.as_bytes()
    {
        return FastOutcome::Decoded(Err(invalid(
            "Bybit unacknowledged or mismatched spot snapshot",
        )));
    }
    let Some(millis) = ts else {
        return FastOutcome::Decoded(Err(invalid("Bybit timestamp")));
    };
    FastOutcome::Decoded(
        build_quote(
            config,
            epoch,
            price_step,
            qty_step,
            millis,
            received,
            d.seq,
            Some(d.u),
            d.b,
            d.a,
        )
        .map(Some),
    )
}

#[inline]
#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_okx<'a>(
    bytes: &[u8],
    received: u64,
    backend: Backend,
    config: &'a QuoteConfig,
    epoch: u64,
    channel: &str,
    acknowledged: &mut bool,
    price_step: &StepSpec,
    qty_step: &StepSpec,
) -> FastOutcome<Option<Quote<'a>>> {
    if bytes == b"pong" {
        return FastOutcome::Decoded(Ok(None));
    }
    let parsed = (|| -> std::result::Result<_, ParseAbort> {
        let mut s = Scanner::new(bytes, backend)?;
        s.expect_byte(b'{')?;
        s.skip_ws()?;
        let mut seen = 0u16;
        let mut event = None;
        let mut code = None;
        let mut msg = None;
        let mut id = None;
        let mut action = None;
        let mut arg = None;
        let mut data = None;
        if s.peek()? == b'}' {
            s.pos += 1;
        } else {
            loop {
                let key = s.read_str()?;
                s.expect_byte(b':')?;
                match key {
                    b"event" => {
                        mark_seen(&mut seen, 1 << 0)?;
                        event = s.read_opt_str()?;
                    }
                    b"code" => {
                        mark_seen(&mut seen, 1 << 1)?;
                        code = s.read_opt_str()?;
                    }
                    b"msg" => {
                        mark_seen(&mut seen, 1 << 2)?;
                        msg = s.read_opt_str()?;
                    }
                    b"id" => {
                        mark_seen(&mut seen, 1 << 3)?;
                        id = s.read_opt_str()?;
                    }
                    b"action" => {
                        mark_seen(&mut seen, 1 << 4)?;
                        action = s.read_opt_str()?;
                    }
                    b"arg" => {
                        mark_seen(&mut seen, 1 << 5)?;
                        arg = if s.try_read_null()? {
                            None
                        } else {
                            Some(s.parse_okx_arg()?)
                        };
                    }
                    b"data" => {
                        mark_seen(&mut seen, 1 << 6)?;
                        data = if s.try_read_null()? {
                            None
                        } else {
                            s.expect_byte(b'[')?;
                            let book = s.parse_okx_book()?;
                            s.expect_byte(b']')?;
                            Some(book)
                        };
                    }
                    _ => s.skip_value(0)?,
                }
                if s.pos < s.block_end && s.bytes[s.pos] == b',' {
                    s.pos += 1;
                    continue;
                }
                s.skip_ws()?;
                match s.peek()? {
                    b',' => s.pos += 1,
                    b'}' => {
                        s.pos += 1;
                        break;
                    }
                    _ => return Err(ParseAbort::InvalidJson),
                }
            }
        }
        s.finish()?;
        Ok((event, code, msg, id, action, arg, data))
    })();

    let (event, code, msg, id, action, arg, data) = match parsed {
        Ok(v) => v,
        Err(ParseAbort::Fallback) => return FastOutcome::Fallback,
        Err(ParseAbort::InvalidJson) => {
            return FastOutcome::Decoded(Err(invalid("malformed OKX spot JSON frame")));
        }
    };

    if event == Some(b"notice".as_slice()) {
        return FastOutcome::Decoded(Ok(None));
    }
    if event == Some(b"error".as_slice()) {
        let code_str = code
            .and_then(|c| core::str::from_utf8(c).ok())
            .unwrap_or("unknown");
        let msg_str = msg
            .and_then(|m| core::str::from_utf8(m).ok())
            .unwrap_or("unknown");
        return FastOutcome::Decoded(Err(invalid(&format!(
            "OKX rejected subscription {code_str}: {msg_str}"
        ))));
    }
    let Some(arg) = arg else {
        return FastOutcome::Decoded(Err(invalid("OKX missing subscription identity")));
    };
    if arg.channel != channel.as_bytes() || arg.inst_id != config.symbol.as_bytes() {
        return FastOutcome::Decoded(Err(invalid("OKX quote identity")));
    }
    if let Some(event) = event {
        if event != b"subscribe" || id != Some(b"twbbo".as_slice()) {
            return FastOutcome::Decoded(Err(invalid(
                "OKX subscription rejected or unexpected control message",
            )));
        }
        *acknowledged = true;
        return FastOutcome::Decoded(Ok(None));
    }
    if !*acknowledged || action.is_some_and(|s| s != b"snapshot") {
        return FastOutcome::Decoded(Err(invalid("OKX unacknowledged or incremental snapshot")));
    }
    let Some(d) = data else {
        return FastOutcome::Decoded(Err(invalid("OKX snapshot data")));
    };
    let millis = match parse_okx_millis(d.ts) {
        Ok(v) => v,
        Err(e) => return FastOutcome::Decoded(Err(e)),
    };
    FastOutcome::Decoded(
        build_quote(
            config, epoch, price_step, qty_step, millis, received, d.seq_id, None, d.bids, d.asks,
        )
        .map(Some),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn available_backends() -> Vec<Backend> {
        let mut out = vec![Backend::swar()];
        #[cfg(target_arch = "x86_64")]
        {
            out.push(Backend::sse2());
            if let Some(avx2) = Backend::avx2_if_supported() {
                out.push(avx2);
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            out.push(Backend::neon());
        }
        out
    }

    #[test]
    fn simd_backends_match_swar_across_lengths_alignments_and_bit31_boundaries() {
        let backends = available_backends();
        let sample = br#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1700000000123,"data":{"s":"BTCUSDT","b":[["31020.1","0.035"]],"a":[["31020.2","0.120"]],"u":999,"seq":1000},"ctrl":"	"}"#;
        let mut padded = vec![b' '; 256];
        for align in 0..32 {
            padded[align..align + sample.len()].copy_from_slice(sample);
            let view = &padded[align..align + sample.len()];
            for len in 0..=64.min(view.len()) {
                let sub = &view[..len];
                let expected = scan_block_swar(sub);
                for &b in &backends {
                    assert_eq!(
                        scan_block_64(sub, b),
                        expected,
                        "mismatch at align={align}, len={len}, backend={b:?}"
                    );
                }
            }
            for chunk in view.chunks(64) {
                let expected = scan_block_swar(chunk);
                for &b in &backends {
                    assert_eq!(scan_block_64(chunk, b), expected);
                }
            }
        }

        // Explicitly place `"`, `{`, `}`, `[`, `]`, `'9'`, `0x09`, `\`, and `0x80` at bit 31 and bit 63.
        for pos in [0usize, 7, 8, 15, 16, 31, 32, 47, 48, 63] {
            for &ch in &[
                b'"', b'{', b'}', b'[', b']', b'0', b'9', b'\t', b'\\', 0x80u8,
            ] {
                let mut block = [b'x'; 64];
                block[pos] = ch;
                let expected = scan_block_swar(&block);
                for &b in &backends {
                    assert_eq!(
                        scan_block_64(&block, b),
                        expected,
                        "pos={pos} ch={ch:#x} backend={b:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn step_spec_units_match_binance_decimal_oracle() {
        for step_s in [
            "1",
            "10",
            "0.1",
            "0.05",
            "0.25",
            "2.5",
            "0.000001",
            "0.00000001",
        ] {
            let raw = Decimal::parse(step_s).unwrap();
            let spec = StepSpec::new(raw);
            for input in [
                "",
                ".",
                ".5",
                "5.",
                "1..2",
                "+5",
                "0",
                "0.0",
                "-0",
                "-0.0",
                "-5",
                "005",
                "010.05",
                "10.05",
                "10.0500",
                "10.051",
                "31020.1",
                "0.035",
                "5",
                "20",
                "9223372036854775807",
                "9223372036854775808",
                "999999999999999999999999999999999999999",
            ] {
                let fast = parse_positive_units(input.as_bytes(), &spec);
                let oracle = Decimal::parse(input)
                    .and_then(|d| tickwire_binance::feed::units(d, raw))
                    .map_err(|_| ())
                    .and_then(|n| if n <= 0 { Err(()) } else { Ok(n) });
                assert_eq!(
                    fast.map_err(|_| ()),
                    oracle,
                    "step={step_s}, input={input:?}"
                );
            }
        }
    }
}
