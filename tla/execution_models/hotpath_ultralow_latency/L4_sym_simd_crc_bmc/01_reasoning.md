# Modeling reasoning — `L4_SymSimdCrcBmc`

## 1. Finite-bound blind spots in explicit-state `L0` and `L2`

1. **`L2_SimdDecodeAndOverflow` (`FrameLengths = {15, 16, 31, 32, 63, 64, 97}`, `TickStep = 2..3`, `ts_ms * 2 <= 20`)**:
   - Explicit TLC could only sample 7 buffer lengths out of `0 .. 65536`, tiny toy mantissas (`0 .. 12`), toy step sizes (`2 .. 3`), and a miniature timestamp bound `U64Max = 20` with scale factor `2`.
   - Real boundary hazards live at 19-digit `u64` mantissas (`0 .. 10^19 - 1`), 20-byte decimal strings (`19` digits + `1` decimal point in `simd_json.rs:524`), `u64` intermediate multiplication overflow `mantissa * 10^(step.scale - scale) > u64::MAX` delegating to `u128` `gcd`-reduced `parse_positive_units_from_parts` (`simd_json.rs:571-576`), `i64::MAX` (`9,223,372,036,854,775,807`) tick/lot saturation, `millis * 1_000_000 > u64::MAX` (`millis >= 18,446,744,073,710`), and `< 64` shift bounds on `1u64 << offset` (`simd_json.rs:456`) under `overflow-checks = true`.
2. **`L0_ZeroCopyAndCrcGate` (boolean `crc_valid` abstraction)**:
   - Explicit `L0` treated `crc32c_masked` as an abstract boolean flag rather than verifying slice clamping (`start.min(len)`, `end.min(len).max(start)`), 8-byte `as_chunks::<8>()` word boundaries, 64-byte `ZERO_64` chunking (`crates/tickwire-ipc/src/crc32c.rs:154-168`), and 136-byte `crc32c_header_masked` framing (`crates/tickwire-journal/src/crc32c.rs:182-237`) across inverted (`mask_start > mask_end`), out-of-bounds (`mask_end > buf_len`), and unaligned mask ranges.

## 2. Symbolic techniques applied (`Apalache` + Z3 `QF_LIA`)

1. **Symbolic 64-Byte Block & Vector/SWAR Loop State Machine (`0 <= frame_len <= 65536`)**:
   - Models `Session::decode_fast` (`MAX_MESSAGE = 16384` gate) and `Scanner::load_next_block` (`end = min(base + 64, frame_len)`) stepping through exact `AVX2` (`+32`), `SSE2`/`NEON` (`+16`), `SWAR` (`+8`), and scalar (`+1`) loop guards across all symbolic buffer lengths `0 .. 65536`.
   - Verifies `ZeroVectorOobAndValidShift` (`0 <= last_read_start <= last_read_end <= block_end <= frame_len` and `0 <= last_shift_bits < 64`, plus zero-extended `movemask` upper bits).
2. **`QF_LIA` Linearized 64-Bit/128-Bit Fixed-Point Oracle Equivalence (`FastVsOracleExactEquivalence`)**:
   - Keeps `StepSpec` scale factors linear (`step = 0.01`, `0.25`, and `5.0000` with `mantissa = 50000, scale = 4`) while letting `mantissa` range symbolically over the full 19-digit domain `0 .. 10^19 - 1` (`9,999,999,999,999,999,999`) alongside symbolic byte-slice length `str_len`, sign `has_minus`, invalid-char flag, `dot_count`, `digit_before_dot`, `digit_after_dot`, `total_digits`, and `frac_scale`.
   - Proves that `simd_json::parse_positive_units` (`slen <= 20`, `u64` fast path + `u128` `gcd`-reduced fallback) is mathematically identical (`(fast_ok <=> oracle_ok) /\ (fast_units = oracle_units)`) to `Decimal::parse` + `tickwire_binance::feed::units` (`n > 0`) — including inputs like `"9000000000000000000"` at `step = 5.0000` where `mantissa * 10^4 = 9 * 10^22 > u64::MAX` overflows `u64::checked_mul` yet reduces cleanly in `u128` to `1.8 * 10^18 <= i64::MAX`!
3. **Full 64-Bit Timestamp & Checked-Arithmetic Verification (`NoSilentArithmeticWrap`)**:
   - Symbolically verifies `millis.checked_mul(1_000_000)` over `0 <= ts_ms <= 2^64 - 1` (`ts_ok <=> ts_ms <= 18,446,744,073,709`) across all 4 Cargo profiles (`dev`, `test`, `bench`, `release`).
4. **Symbolic Masked CRC-32C Partition Algebra (`CrcMaskedSlicePartitionExact` + `Diag_JournalCrcMaskedTotalOnOobRange`)**:
   - Proves over `0 <= buf_len <= 1,048,576` and arbitrary `0 <= mask_start, mask_end <= 2,097,152` that `tickwire_ipc::crc32c_masked` never panics and partitions `[0..clamped_start]`, `clamped_end - clamped_start` zero bytes (`64`B chunks + remainder), and `[clamped_end..buf_len]` (`8`B `as_chunks::<8>()` words + remainder) to exact byte length `buf_len`, while `Diag_JournalCrcMaskedTotalOnOobRange` witnesses the assertion contract difference in `tickwire_journal::crc32c::crc32c_masked`.
