# Modeling reasoning — `L2_SimdDecodeAndOverflowGuard`

## Scope and state owners
1. **SIMD structural scanner & dual-path BBO decoder vs. `serde_json` oracle (`crates/tickwire-md/src/spot_quotes/simd_json.rs:178-1574`, `crates/tickwire-md/src/spot_quotes/protocol.rs:84-241`)**:
   - Models `SimdScan` over 16-byte (SSE2/NEON) and 32-byte (AVX2) vector widths with explicit slice-length guards (`pos + chunk_width <= frame_len`) and SWAR/scalar tail handling (`simd_oob_reads = 0`, `hotpath_heap_allocs = 0`).
   - Unescaped canonical BBO frames take `SimdFast` (`simd_json::decode_bybit` / `simd_json::decode_okx`); frames containing JSON escapes (`\"`, `\uXXXX` detected via structural backslash mask or `>= 0x80` non-ASCII bytes) route cleanly via `ScanAbort::Fallback` to the borrowed `serde_json` fallback without mutating `session.acknowledged` prior to full frame validation.
   - Independently evaluates `SerdeOracleDecode` (`bybit_serde_oracle` / `okx_serde_oracle`) on every reachable `(session, frame)` pair and verifies `last_fast_result = last_oracle_result /\ fast_session = oracle_session` (`ExactOracleEquivalence`).
2. **Two-stage BBO publication & grid/spread validation (`crates/tickwire-md/src/spot_quotes.rs:80-177`, `crates/tickwire-md/src/spot_quotes.rs:209-241`, `crates/tickwire-md/src/spot_quotes/simd_json.rs:523-621`, `crates/tickwire-binance/src/decimal.rs:15-55`, `crates/tickwire-binance/src/feed.rs:352-375`)**:
   - Uses non-unit `TickStep = 2` and `LotStep = 5` so off-grid prices (`3 % 2 != 0`, `5 % 2 != 0`) and off-grid quantities (`4 % 5 != 0`) are tested concretely alongside nonpositive (`<= 0`) and overflowing (`> MaxIntBound`) decimals and timestamps (`millis * 2 > MaxIntBound`).
   - Preserves the exact two-stage contract between `Session::on_text` and `Quote::publication`: one-sided (`[]`) and crossed/zero-spread (`ask_ticks <= bid_ticks`) snapshots decode as `Ok(Some(Quote))` (matching `serde_json` without failing the session) and then map deterministically to `Publication::Unavailable`, whereas `Publication::Quote` requires `bid_ticks > 0 /\ ask_ticks > bid_ticks`.
3. **Workspace-wide `overflow-checks = true` (`Cargo.toml:14-26`, `crates/tickwire-journal/tests/overflow_checks.rs:1-57`)**:
   - Models `AllProfiles == {"dev", "test", "release", "bench"}` and verifies both BBO timestamp/tick/lot decoding and downstream notional/spread arithmetic trap deterministically (`"TrappedOverflow"`) on add/sub/mul overflow rather than wrapping modulo `2^64`.

## Configurations and invariants
- **`L2_SimdDecodeAndOverflowGuard.cfg`**: `Venue = "Bybit"`, `Profile = "release"`, `TickStep = 2`, `LotStep = 5`, `MaxIntBound = 40`, `MaxSteps = 6`.
- **`L2_SimdDecodeAndOverflowGuard_OkxBench.cfg`**: `Venue = "Okx"`, `Profile = "bench"`, `TickStep = 2`, `LotStep = 5`, `MaxIntBound = 40`, `MaxSteps = 6`.
- **Invariants**: `TypeOK`, `ExactOracleEquivalence`, `TickLotAndPositiveSpreadSoundness`, `NoSilentArithmeticWrap`, `ZeroHotPathHeapAllocation`.
