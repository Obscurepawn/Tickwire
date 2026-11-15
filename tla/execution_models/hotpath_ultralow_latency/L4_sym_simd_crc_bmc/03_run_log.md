# Symbolic Apalache/Z3 & Explicit TLC Runs — `L4_SymSimdCrcBmc`

Dual-engine verification combining Apalache `0.63.0` (`Z3` `QF_LIA` 64-bit/128-bit symbolic bounded model checking + Mode S2 unbounded block-scan induction) and repository-pinned TLC `2.19` (`tla/tools/tla2tools.jar`, `-workers 1 -coverage 1`).

## 1. Symbolic Apalache + Z3 Runs (`BoundedMode = FALSE`, `--cinit=ConstInit`)

| Mode & Obligation | Command / Target | Result | VCs / Depth | Runtime | Evidence |
| --- | --- | --- | ---: | ---: | --- |
| Snowcat static typecheck | `run_apalache.sh typecheck L4_SymSimdCrcBmc.tla` | `PASS` (`rc=0`) | 42 typed vars | `1.616s` | [apalache_typecheck.log](04_evidence/apalache_typecheck.log) |
| Mode S1 64-bit/128-bit symbolic BMC (`0 <= frame_len <= 65536`, `0 <= mantissa <= 10^19 - 1`, `0 <= ts_ms <= 2^64 - 1`, `0 <= buf_len <= 1048576`) | `run_apalache.sh bmc --length=6 --inv=IndInv --cinit=ConstInit` | **`PASS` (`rc=0`)** | $k = 6$ (81..60 VCs/step) | `33.369s` | [apalache_bmc_k6.log](04_evidence/apalache_bmc_k6.log) |
| Mode S2 unbounded block-scan & sample symbolic induction ($\forall k \ge 0$) | `run_apalache.sh inductive --indinv=IndInv --cinit=ConstInit --inv=SafetyConjunction` | **`PASS` (`rc=0`)** | Base: 122 VCs (`3.318s`)<br>Step: 6 actions (`7.163s`)<br>Entail: 92 VCs (`2.825s`) | `13.306s` | [apalache_inductive_IndInv.log](04_evidence/apalache_inductive_IndInv.log) |
| Diagnostic Query: `tickwire_journal::crc32c::crc32c_masked` vs `tickwire_ipc::crc32c_masked` OOB/inverted `Range<usize>` contract | `bmc --length=0 --inv=Diag_JournalCrcMaskedTotalOnOobRange --cinit=ConstInit` | `WITNESS` (`rc=12`) | $k = 0$ | `2.971s` | [apalache_diag_journal_crc_range.log](04_evidence/apalache_diag_journal_crc_range.log) · [diag_journal_crc_range_witness.tla](04_evidence/diag_journal_crc_range_witness.tla) |

## 2. Explicit TLC Smoke & Vacuous-Pass Gate (`BoundedMode = TRUE`)

From the workspace root run `./scripts/check-tla.sh hotpath_ultralow_latency`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Multi-backend SIMD/SWAR/scalar + fixed-point/CRC smoke (`L4_SymSimdCrcBmc.cfg`)](04_evidence/L4_SymSimdCrcBmc.log) | 816 | 816 | 12 | 0 | <1s |

The exhaustive TLC smoke search verified all six configured invariants (`TypeOK`, `ZeroVectorOobAndValidShift`, `FastVsOracleExactEquivalence`, `NoSilentArithmeticWrap`, `CrcMaskedSlicePartitionExact`, and `IndInv`) with `0` states left on queue and **100% action coverage (`7/7` entries: `Init` + all 6 `Next` actions firing with `count > 0`)**. [Aggregate action coverage](04_evidence/coverage.json)

## 3. Post-Fix Validation (`L5_SymWarmerCrcFixValidation`, 2026-11-15)

The `tickwire_journal::crc32c::crc32c_masked` range-clamping parity fix (`crates/tickwire-journal/src/crc32c.rs:180-195`) was mechanically re-verified in [L5_sym_warmer_crc_fix_validation](../L5_sym_warmer_crc_fix_validation/03_run_log.md) (`TotalCrc32cMaskedParity` across `0 <= buf_len <= 2^64 - 1` and arbitrary/inverted/OOB `0 <= mask_start, mask_end <= 2^64 - 1`): Apalache `0.63.0` + Z3 `IndInv` (`12.371s`, `rc=0`), 64-bit BMC (`k=8`, `52.939s`, `rc=0`), and TLC (`3,525` / `1,719` distinct states, `16/16` actions covered, `rc=0`).
