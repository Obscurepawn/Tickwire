# Symbolic Apalache/Z3 & Explicit TLC Runs — `L1_SymCalibration`

Dual-engine Layer-3 symbolic verification of `tickwire_clock::Calibrator` combining Apalache `0.63.0` (`Z3` `QF_LIA` unbounded symbolic induction + 64-bit/`u128` bounded model checking) and repository-pinned TLC `2.19` (`tla/tools/tla2tools.jar`, `-workers 1 -coverage 60`).

## 1. Symbolic Apalache + Z3 Runs (`BoundedMode = FALSE`, `--cinit=ConstInit`)

| Mode & Obligation | Command / Target | Result | VCs / Depth | Runtime | Evidence |
| --- | --- | --- | ---: | ---: | --- |
| Snowcat static typecheck | `run_apalache.sh typecheck L1_SymCalibration.tla` | `PASS` (`rc=0`) | 59 typed vars | `2.816s` | [apalache_typecheck.log](04_evidence/apalache_typecheck.log) |
| Mode S2 full symbolic induction (`IndInv`, $\forall k \ge 0$, $0 \le t \le 2^{64}-1$, exact `u128` `PPB` drift cones) | `run_apalache.sh inductive --indinv=IndInv --inv=SafetyConjunction --cinit=ConstInit` | **`PASS` (`rc=0`)** | Base: 151 VCs (`4.873s`)<br>Step: 15 actions / 151 VCs (`38.501s`)<br>Entail: 143 VCs (`4.715s`) | `38.944s` (parallel wall) | [apalache_inductive.log](04_evidence/apalache_inductive.log) |
| Mode S1 64-bit `u64` & `u128` `PPB` boundary BMC | `run_apalache.sh bmc --inv=SafetyConjunction --length=2 --cinit=ConstInit` | **`PASS` (`rc=0`)** | $k = 2$ (143 VCs/step) | `13.542s` | [apalache_bmc.log](04_evidence/apalache_bmc.log) |

## 2. Explicit TLC Smoke & Vacuous-Pass Gate (`BoundedMode = TRUE`)

From the workspace root run `./scripts/check-tla.sh clock`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Spot millisecond + microsecond clock (`L1_SymCalibration_spot.cfg`, `SourceKind = 1`)](04_evidence/L1_SymCalibration_spot.log) | 189,516 | 31,296 | 6 | 0 | 71s (`-workers 1 -coverage`) |
| [USD-M millisecond-only clock (`L1_SymCalibration_usdm.cfg`, `SourceKind = 2`)](04_evidence/L1_SymCalibration_usdm.log) | 179,988 | 28,967 | 6 | 0 | 48s (`-workers 1 -coverage`) |

Both exhaustive TLC runs verified all 16 configured invariants (`TypeOK`, all 14 `L0_Calibration` safety invariants in `SafetyConjunction`, and `IndInv`) with `0` states left on queue and **100% action coverage (`16/16` entries: `Init` + all 15 `Next` actions firing with `min_count = 4`)**. [Aggregate action coverage](04_evidence/coverage.json)
