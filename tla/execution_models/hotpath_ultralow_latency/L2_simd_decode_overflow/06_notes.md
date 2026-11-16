# Review notes — `L2_SimdDecodeAndOverflowGuard`

- **Clean fallback before state mutation**: `simd_json` returns `FastDecodeOutcome::Fallback` without mutating `session.acknowledged`, ensuring escaped (`\"`, `\uXXXX`) or key-reordered control/data frames fall through cleanly to `protocol.rs` (`crates/tickwire-md/src/spot_quotes/simd_json.rs:1285-1574`, `crates/tickwire-md/src/spot_quotes/protocol.rs:84-241`).
- **Workspace-wide overflow checks**: Root `Cargo.toml:57-69` enables `overflow-checks = true` across `dev`, `test`, `release`, and `bench`, eliminating profile divergence between test and production binaries.
- **Full 64-bit/128-bit SMT lifting**: Arbitrary `0 .. 65536` frame lengths, 19-digit `i64::MAX` `StepSpec` vs. `Decimal` equivalence, and 64-bit `checked_mul(1_000_000)` bounds are proved symbolically via Z3 `QF_LIA` in [../L4_sym_simd_crc_bmc/05_output.md](../L4_sym_simd_crc_bmc/05_output.md).
