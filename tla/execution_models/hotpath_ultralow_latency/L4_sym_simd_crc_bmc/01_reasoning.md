# Modeling reasoning — `L4_SymSimdCrcBmc`

## Scope and SMT encoding
1. **Symbolic 64-byte SIMD/SWAR/scalar scanner (`crates/tickwire-md/src/spot_quotes/simd_json.rs:189-469`)**:
   - Models `StepSimd32`, `StepSimd16`, `StepSwar8`, `StepScalar1`, and `AdvanceNextBlock64` over symbolic `0 <= frame_len <= 65536` (`crates/tickwire-md/src/spot_quotes/network.rs:18`), verifying `load_end <= frame_len`, `0 <= shift < 64`, and non-overlapping partition coverage of every 64-byte block (`ZeroVectorOobAndValidShift`).
2. **Exact 19-digit `StepSpec` (`u64` + `u128` `gcd`-reduced fallback) vs. `Decimal` + `feed::units` (`crates/tickwire-md/src/spot_quotes/simd_json.rs:523-694`, `crates/tickwire-binance/src/decimal.rs:4-94`, `crates/tickwire-binance/src/feed.rs:352-375`)**:
   - Encodes full 64-bit/128-bit `QF_LIA` arithmetic up to `i64::MAX = 2^63 - 1` (`I64Max` constructed algebraically without >31-bit literals), proving `FastVsOracleExactEquivalence` across arbitrary mantissas, fractional scales `0 .. 9`, and `gcd`-reduced tick/lot denominators, plus `millis.checked_mul(1_000_000)` trapping (`NoSilentArithmeticWrap`, `Cargo.toml:58-71`).
3. **4096B SHM arena header & 64B retained block-span framing (`crates/tickwire-strategy-sdk/src/arena.rs:17-58,72-210,439-502`)**:
   - Models `configure_shm_arena_header`, `verify_arena_header`, and `ShmArenaReader::read_retained_payload` across arbitrary arena byte capacities, block indices, block counts, and payload lengths, proving `ArenaHeaderAndBlockBoundsExact` (`4096 + (block_idx + block_cnt) * 64 <= arena_total_bytes` and `0 < payload_len <= block_cnt * 64` whenever accepted).
