//! Hardware-accelerated Castagnoli CRC-32C (`0x82f63b78`) with Slicing-by-8 fallback.
//!
//! # Safety Boundary
//!
//! `unsafe` in this module is restricted exclusively to invoking `core::arch`
//! hardware CRC-32C intrinsics (`_mm_crc32_u64`/`_mm_crc32_u8` on `x86_64` with
//! verified SSE4.2 support, and `__crc32cd`/`__crc32cb` on `aarch64` with
//! verified `crc` feature support). All memory loads use safe slice splitting
//! (`as_chunks::<8>()`) and `u64::from_le_bytes`, so no raw pointer dereference
//! or alignment precondition exists.
#![deny(unsafe_op_in_unsafe_fn)]

use core::ops::Range;

const POLY: u32 = 0x82f6_3b78;

const fn step1(crc: u32) -> u32 {
    if crc & 1 != 0 {
        (crc >> 1) ^ POLY
    } else {
        crc >> 1
    }
}

const fn step8(crc: u32) -> u32 {
    step1(step1(step1(step1(step1(step1(step1(step1(crc))))))))
}

const fn build_slicing8_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0u32; 256]; 8];
    let mut i = 0usize;
    while i < 256 {
        tables[0][i] = step8(i as u32);
        i += 1;
    }
    let mut t = 1usize;
    while t < 8 {
        let mut b = 0usize;
        while b < 256 {
            let prev = tables[t - 1][b];
            tables[t][b] = (prev >> 8) ^ tables[0][(prev & 0xff) as usize];
            b += 1;
        }
        t += 1;
    }
    tables
}

/// Compile-time 8 KiB Slicing-by-8 lookup table for Castagnoli CRC-32C.
pub const TABLES: [[u32; 256]; 8] = build_slicing8_tables();

#[inline]
fn slicing8_update_raw(mut crc: u32, bytes: &[u8]) -> u32 {
    let (chunks, rem) = bytes.as_chunks::<8>();
    for chunk in chunks {
        let lo = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) ^ crc;
        let hi = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        crc = TABLES[7][(lo & 0xff) as usize]
            ^ TABLES[6][((lo >> 8) & 0xff) as usize]
            ^ TABLES[5][((lo >> 16) & 0xff) as usize]
            ^ TABLES[4][(lo >> 24) as usize]
            ^ TABLES[3][(hi & 0xff) as usize]
            ^ TABLES[2][((hi >> 8) & 0xff) as usize]
            ^ TABLES[1][((hi >> 16) & 0xff) as usize]
            ^ TABLES[0][(hi >> 24) as usize];
    }
    for &byte in rem {
        crc = (crc >> 8) ^ TABLES[0][((crc as u8) ^ byte) as usize];
    }
    crc
}

/// Portable table-driven Castagnoli CRC-32C using Slicing-by-8 (zero bit loops).
#[inline]
pub fn crc32c_slicing8(bytes: &[u8]) -> u32 {
    !slicing8_update_raw(!0u32, bytes)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn sse42_update_raw(crc: u32, bytes: &[u8]) -> u32 {
    use core::arch::x86_64::{_mm_crc32_u8, _mm_crc32_u64};
    let mut state = u64::from(crc);
    let (chunks, rem) = bytes.as_chunks::<8>();
    for chunk in chunks {
        state = _mm_crc32_u64(state, u64::from_le_bytes(*chunk));
    }
    let mut state32 = state as u32;
    for &byte in rem {
        state32 = _mm_crc32_u8(state32, byte);
    }
    state32
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
unsafe fn arm_crc_update_raw(mut crc: u32, bytes: &[u8]) -> u32 {
    use core::arch::aarch64::{__crc32cb, __crc32cd};
    let (chunks, rem) = bytes.as_chunks::<8>();
    for chunk in chunks {
        crc = __crc32cd(crc, u64::from_le_bytes(*chunk));
    }
    for &byte in rem {
        crc = __crc32cb(crc, byte);
    }
    crc
}

#[inline]
fn update_raw(crc: u32, bytes: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        #[cfg(target_feature = "sse4.2")]
        {
            // SAFETY: SSE4.2 target feature is enabled at compile time.
            return unsafe { sse42_update_raw(crc, bytes) };
        }
        #[cfg(not(target_feature = "sse4.2"))]
        if std::arch::is_x86_feature_detected!("sse4.2") {
            // SAFETY: runtime CPUID detection confirmed SSE4.2 support on this CPU.
            return unsafe { sse42_update_raw(crc, bytes) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        #[cfg(target_feature = "crc")]
        {
            // SAFETY: Arm CRC target feature is enabled at compile time.
            return unsafe { arm_crc_update_raw(crc, bytes) };
        }
        #[cfg(not(target_feature = "crc"))]
        if std::arch::is_aarch64_feature_detected!("crc") {
            // SAFETY: runtime feature detection confirmed Arm CRC32 support on this CPU.
            return unsafe { arm_crc_update_raw(crc, bytes) };
        }
    }
    slicing8_update_raw(crc, bytes)
}

/// Continue a finalized Castagnoli CRC-32C (`seed = 0` for the first chunk).
#[inline]
pub fn crc32c_update(crc: u32, chunk: &[u8]) -> u32 {
    !update_raw(!crc, chunk)
}

/// Hardware-accelerated Castagnoli CRC-32C (`0x82f63b78`, init/xorout `0xffffffff`).
#[inline]
pub fn crc32c(bytes: &[u8]) -> u32 {
    !update_raw(!0u32, bytes)
}

/// Castagnoli CRC-32C treating `mask_range` (clamped to `bytes.len()`) as zero bytes.
#[inline]
pub fn crc32c_masked(bytes: &[u8], mask_range: Range<usize>) -> u32 {
    const ZEROS: [u8; 64] = [0u8; 64];
    let start = mask_range.start.min(bytes.len());
    let end = mask_range.end.min(bytes.len()).max(start);
    let mut state = !0u32;
    state = update_raw(state, &bytes[..start]);
    let mut rem = end - start;
    while rem > 0 {
        let step = rem.min(ZEROS.len());
        state = update_raw(state, &ZEROS[..step]);
        rem -= step;
    }
    state = update_raw(state, &bytes[end..]);
    !state
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slicing8_masked(bytes: &[u8], mask_range: Range<usize>) -> u32 {
        let mut copy = bytes.to_vec();
        let start = mask_range.start.min(copy.len());
        let end = mask_range.end.min(copy.len()).max(start);
        copy[start..end].fill(0);
        crc32c_slicing8(&copy)
    }

    #[test]
    fn rfc3720_standard_check_vector() {
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c_slicing8(b""), 0);
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(crc32c_slicing8(b"123456789"), 0xe306_9283);
        assert_eq!(crc32c_update(crc32c(b"1234"), b"56789"), 0xe306_9283);
    }

    #[test]
    fn differential_hw_matches_slicing8_across_alignments_lengths_and_masks() {
        let mut buf = vec![0u8; 4096 + 16];
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for byte in &mut buf {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = (state & 0xff) as u8;
        }

        for align in 0..8 {
            for len in 0..=4096 {
                let slice = &buf[align..align + len];
                assert_eq!(
                    crc32c(slice),
                    crc32c_slicing8(slice),
                    "mismatch at align={align}, len={len}"
                );
            }
        }

        for len in [0usize, 1, 7, 8, 63, 64, 120, 128, 256, 512, 1024, 4096] {
            let slice = &buf[3..3 + len];
            for mask in [
                0..0,
                0..4,
                112..120,
                116..120,
                37..193,
                0..len,
                len..len + 8,
            ] {
                assert_eq!(
                    crc32c_masked(slice, mask.clone()),
                    slicing8_masked(slice, mask.clone()),
                    "masked mismatch at len={len}, mask={mask:?}"
                );
            }
        }
    }
}
