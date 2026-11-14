//! Hardware-accelerated Castagnoli CRC-32C (`0x1EDC6F41`, reflected `0x82F63B78`)
//! with a compile-time Slicing-by-8 software fallback.
//!
//! This module is the sole `#[allow(unsafe_code)]` boundary in `tickwire-journal`.
//! `unsafe` is restricted to invoking value-only CPU CRC-32C instructions after
//! runtime feature detection (`sse4.2` on `x86_64`, `crc` on `aarch64`); all byte
//! loads use safe slice indexing and `u64::from_le_bytes` with zero pointer casts.
#![deny(unsafe_op_in_unsafe_fn)]

use core::ops::Range;

const POLY_REFLECTED: u32 = 0x82F6_3B78;

const fn gf2_step1(x: u32) -> u32 {
    (x >> 1) ^ (POLY_REFLECTED & (x & 1).wrapping_neg())
}

// 8-step reflected reductions of single-bit basis bytes (1 << 7 down to 1 << 0),
// derived algebraically without bit-by-bit loops: step^8(1 << k) == step^(8 - k)(1 << 7).
const BASIS_7: u32 = POLY_REFLECTED;
const BASIS_6: u32 = gf2_step1(BASIS_7);
const BASIS_5: u32 = gf2_step1(BASIS_6);
const BASIS_4: u32 = gf2_step1(BASIS_5);
const BASIS_3: u32 = gf2_step1(BASIS_4);
const BASIS_2: u32 = gf2_step1(BASIS_3);
const BASIS_1: u32 = gf2_step1(BASIS_2);
const BASIS_0: u32 = gf2_step1(BASIS_1);

const fn byte_crc32c(byte: u8) -> u32 {
    let b = byte as u32;
    (BASIS_0 & (b & 1).wrapping_neg())
        ^ (BASIS_1 & ((b >> 1) & 1).wrapping_neg())
        ^ (BASIS_2 & ((b >> 2) & 1).wrapping_neg())
        ^ (BASIS_3 & ((b >> 3) & 1).wrapping_neg())
        ^ (BASIS_4 & ((b >> 4) & 1).wrapping_neg())
        ^ (BASIS_5 & ((b >> 5) & 1).wrapping_neg())
        ^ (BASIS_6 & ((b >> 6) & 1).wrapping_neg())
        ^ (BASIS_7 & ((b >> 7) & 1).wrapping_neg())
}

const fn build_slicing8_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0u32; 256]; 8];
    let mut i = 0usize;
    while i < 256 {
        tables[0][i] = byte_crc32c(i as u8);
        i += 1;
    }
    let mut slice = 1usize;
    while slice < 8 {
        let mut j = 0usize;
        while j < 256 {
            let prev = tables[slice - 1][j];
            tables[slice][j] = (prev >> 8) ^ tables[0][(prev & 0xFF) as usize];
            j += 1;
        }
        slice += 1;
    }
    tables
}

/// Compile-time 8 KiB (`8 x 256 x u32`) Slicing-by-8 lookup table for Castagnoli CRC-32C.
pub const TABLES: [[u32; 256]; 8] = build_slicing8_tables();

#[inline]
const fn update_slicing8(mut state: u32, input: &[u8]) -> u32 {
    let mut offset = 0usize;
    let len = input.len();
    while len - offset >= 8 {
        let lo = u32::from_le_bytes([
            input[offset],
            input[offset + 1],
            input[offset + 2],
            input[offset + 3],
        ]) ^ state;
        let hi = u32::from_le_bytes([
            input[offset + 4],
            input[offset + 5],
            input[offset + 6],
            input[offset + 7],
        ]);
        state = TABLES[7][(lo & 0xFF) as usize]
            ^ TABLES[6][((lo >> 8) & 0xFF) as usize]
            ^ TABLES[5][((lo >> 16) & 0xFF) as usize]
            ^ TABLES[4][(lo >> 24) as usize]
            ^ TABLES[3][(hi & 0xFF) as usize]
            ^ TABLES[2][((hi >> 8) & 0xFF) as usize]
            ^ TABLES[1][((hi >> 16) & 0xFF) as usize]
            ^ TABLES[0][(hi >> 24) as usize];
        offset += 8;
    }
    while offset < len {
        let idx = ((state as u8) ^ input[offset]) as usize;
        state = (state >> 8) ^ TABLES[0][idx];
        offset += 1;
    }
    state
}

/// Universal pure-software Castagnoli CRC-32C using Slicing-by-8 (`const`-callable).
#[inline]
pub const fn crc32c_slicing8(input: &[u8]) -> u32 {
    !update_slicing8(!0u32, input)
}

/// Update an existing finalized Castagnoli CRC-32C with `input` using Slicing-by-8.
#[inline]
pub const fn crc32c_slicing8_update(crc: u32, input: &[u8]) -> u32 {
    !update_slicing8(!crc, input)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn update_sse42(state: u32, input: &[u8]) -> u32 {
    use core::arch::x86_64::{_mm_crc32_u8, _mm_crc32_u64};

    let mut crc64 = u64::from(state);
    let (chunks, remainder) = input.as_chunks::<8>();
    for &chunk in chunks {
        crc64 = _mm_crc32_u64(crc64, u64::from_le_bytes(chunk));
    }
    let mut crc32 = crc64 as u32;
    for &byte in remainder {
        crc32 = _mm_crc32_u8(crc32, byte);
    }
    crc32
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
unsafe fn update_armv8(mut state: u32, input: &[u8]) -> u32 {
    use core::arch::aarch64::{__crc32cb, __crc32cd};

    let (chunks, remainder) = input.as_chunks::<8>();
    for &chunk in chunks {
        state = __crc32cd(state, u64::from_le_bytes(chunk));
    }
    for &byte in remainder {
        state = __crc32cb(state, byte);
    }
    state
}

#[inline]
fn update_dispatch(state: u32, input: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("sse4.2") {
            // SAFETY: `is_x86_feature_detected!("sse4.2")` confirmed at runtime
            // that the executing x86_64 CPU supports SSE4.2 `crc32` instructions.
            // `update_sse42` reads `input` exclusively through safe slice indexing
            // and `u64::from_le_bytes`, imposing no alignment or pointer obligations.
            return unsafe { update_sse42(state, input) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("crc") {
            // SAFETY: `is_aarch64_feature_detected!("crc")` confirmed at runtime
            // that the executing ARMv8 CPU implements the CRC32C instruction set.
            // `update_armv8` reads `input` exclusively through safe slice indexing
            // and `u64::from_le_bytes`, imposing no alignment or pointer obligations.
            return unsafe { update_armv8(state, input) };
        }
    }
    update_slicing8(state, input)
}

/// Hardware-accelerated Castagnoli CRC-32C with Slicing-by-8 fallback.
#[inline]
pub fn crc32c(input: &[u8]) -> u32 {
    !update_dispatch(!0u32, input)
}

/// Incrementally extend a finalized Castagnoli CRC-32C (`0` for an empty prefix).
#[inline]
pub fn crc32c_update(crc: u32, input: &[u8]) -> u32 {
    !update_dispatch(!crc, input)
}

/// Compute Castagnoli CRC-32C over `input` while treating `input[zero_range]` as zeros,
/// without allocating or copying `input`.
pub fn crc32c_masked(input: &[u8], zero_range: Range<usize>) -> u32 {
    assert!(
        zero_range.start <= zero_range.end && zero_range.end <= input.len(),
        "masked CRC range out of bounds"
    );
    const ZEROS: [u8; 64] = [0u8; 64];
    let mut state = update_dispatch(!0u32, &input[..zero_range.start]);
    let mut remaining = zero_range.end - zero_range.start;
    while remaining > 0 {
        let step = remaining.min(ZEROS.len());
        state = update_dispatch(state, &ZEROS[..step]);
        remaining -= step;
    }
    state = update_dispatch(state, &input[zero_range.end..]);
    !state
}

/// Zero-allocation WAL v1 header CRC-32C (`header_with_header_crc32c_zero_and_actual_payload_crc32c`):
/// hashes `header[0..128] || [0u8; 4] || header[132..136]` as 17 8-byte words.
#[inline]
pub fn crc32c_header_masked(header: &[u8; 136]) -> u32 {
    let tail = [
        0,
        0,
        0,
        0,
        header[132],
        header[133],
        header[134],
        header[135],
    ];
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("sse4.2") {
            // SAFETY: `is_x86_feature_detected!("sse4.2")` confirmed SSE4.2 support
            // on the current x86_64 CPU; both slices are valid stack/borrowed bytes.
            return unsafe {
                let s = update_sse42(!0u32, &header[..128]);
                !update_sse42(s, &tail)
            };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("crc") {
            // SAFETY: `is_aarch64_feature_detected!("crc")` confirmed ARMv8 CRC32C
            // support on the current CPU; both slices are valid borrowed bytes.
            return unsafe {
                let s = update_armv8(!0u32, &header[..128]);
                !update_armv8(s, &tail)
            };
        }
    }
    let s = update_slicing8(!0u32, &header[..128]);
    !update_slicing8(s, &tail)
}

const _: () = {
    assert!(crc32c_slicing8(b"") == 0);
    assert!(crc32c_slicing8(b"123456789") == 0xE306_9283);
    assert!(crc32c_slicing8(&[0u8; 32]) == 0x8A91_36AA);
};
