# Modeling reasoning — `L2_SimdDecodeAndOverflowGuard`

## Scope and state owners
1. **Two-tier `FastOrFallbackDecode` vs. `SerdeOracleDecode` (`crates/tickwire-md/src/spot_quotes/simd_json.rs:1285-1574`, `crates/tickwire-md/src/spot_quotes/protocol.rs:84-241`)**:
   - Models the `tickwire-md` spot BBO decoder across Bybit and OKX frame classes: subscribe acks / pongs, canonical unescaped BBO snapshots across 16B/32B/64B SIMD chunk boundaries, crossed (`bid >= ask`) and one-sided books (`Publication::Unavailable` vs. `Publication::Quote`), escaped (`\"`, `\uXXXX`) or reordered JSON falling back cleanly to `serde_json` without dirtying `session.acknowledged`, truncated chunk-boundary frames, off-grid (`price % tick != 0`, `qty % lot != 0`) or non-positive values, overflowing mantissas/timestamps, and extra book levels or mismatched venue identities.
2. **Exact fixed-point equivalence & all-profile overflow checks (`crates/tickwire-binance/src/decimal.rs:15-55`, `crates/tickwire-binance/src/feed.rs:352-375`, `Cargo.toml:58-71`)**:
   - Verifies that every published `Quote` satisfies `bid_ticks > 0`, `ask_ticks > bid_ticks`, and exact tick/lot divisibility (`TickLotAndPositiveSpreadSoundness`), that steady-state fast-path decoding performs zero heap allocations (`ZeroHotPathHeapAllocation`), and that `overflow-checks = true` across `[profile.dev]`, `[profile.test]`, `[profile.release]`, and `[profile.bench]` traps overflowing arithmetic deterministically (`NoSilentArithmeticWrap`).

## Configurations and invariants
- **`L2_SimdDecodeAndOverflowGuard.cfg`**: `Venue = "Bybit"`, `Profile = "release"`.
- **`L2_SimdDecodeAndOverflowGuard_OkxBench.cfg`**: `Venue = "Okx"`, `Profile = "bench"`.
- **Safety invariants**: `TypeOK`, `ExactOracleEquivalence`, `TickLotAndPositiveSpreadSoundness`, `NoSilentArithmeticWrap`, `ZeroHotPathHeapAllocation`.
