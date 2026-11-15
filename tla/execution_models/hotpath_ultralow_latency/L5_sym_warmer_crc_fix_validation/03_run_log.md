# Symbolic Apalache/Z3 & Explicit TLC Runs — `L5_SymWarmerCrcFixValidation`

Post-fix `/vibe-tla fix` validation combining Apalache `0.63.0` (`Z3` `QF_LIA` symbolic inductive invariants + 64-bit `u64` bounded model checking) and repository-pinned TLC `2.19` (`tla/tools/tla2tools.jar`, `-workers 1 -coverage 1`).

## 1. Symbolic Apalache + Z3 Runs (`BoundedMode = FALSE`, `--cinit=ConstInit`)

| Mode & Obligation | Command / Target | Result | VCs / Depth | Runtime | Evidence |
| --- | --- | --- | ---: | ---: | --- |
| Snowcat static typecheck | `run_apalache.sh typecheck L5_SymWarmerCrcFixValidation.tla` | `PASS` (`rc=0`) | 23 typed vars | `2.113s` | [apalache_typecheck.log](04_evidence/apalache_typecheck.log) |
| Mode S2 CTI baseline (`SafetyConjunction` without CTI lemmas) | `apalache-mc check --init=SafetyConjunction --next=Next --inv=SafetyConjunction --cinit=ConstInit --length=1 --no-deadlock` | `CTI` (`rc=12` at Step 1) | Step 1 `CTI` (`~warmup_configured` with `last_activity_ns = 2^64 - 2`) | `4.281s` | [apalache_cti_safety_only.log](04_evidence/apalache_cti_safety_only.log) · [cti_safety_only_witness.tla](04_evidence/cti_safety_only_witness.tla) |
| Mode S2 full symbolic induction (`IndInv`, $\forall k \ge 0$, $0 \le \text{WarmupInterval}, t, \text{buf\_len}, \text{mask} \le 2^{64}-1$) | `run_apalache.sh inductive --indinv=IndInv --inv=SafetyConjunction --cinit=ConstInit` | **`PASS` (`rc=0`)** | Base: 94 VCs (`3.790s`)<br>Step: 15 actions / 94 VCs (`4.918s`)<br>Entail: 76 VCs (`3.663s`) | `12.371s` | [apalache_inductive_IndInv.log](04_evidence/apalache_inductive_IndInv.log) |
| Mode S1 64-bit `u64` & full `Range<usize>` BMC | `run_apalache.sh bmc --inv=SafetyConjunction --length=8 --cinit=ConstInit` | **`PASS` (`rc=0`)** | $k = 8$ (76 VCs/step) | `52.939s` | [apalache_bmc_k8.log](04_evidence/apalache_bmc_k8.log) |

## 2. Explicit TLC Smoke & Vacuous-Pass Gate (`BoundedMode = TRUE`)

From the workspace root run `./scripts/check-tla.sh hotpath_ultralow_latency`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Enabled warmer + `CoreNode` + `crc32c_masked` (`L5_SymWarmerCrcFixValidation.cfg`, `WarmupInterval = 2`)](04_evidence/L5_SymWarmerCrcFixValidation.log) | 170,769 | 3,525 | 14 | 0 | 1s |
| [Disabled warmer + `CoreNode` + `crc32c_masked` (`L5_SymWarmerCrcFixValidation_Disabled.cfg`, `WarmupInterval = 0`)](04_evidence/L5_SymWarmerCrcFixValidation_Disabled.log) | 163,201 | 1,719 | 13 | 0 | 1s |

Both exhaustive TLC runs verified all six configured invariants (`TypeOK`, `NoZeroWaitSpinWhenNotReady`, `ExactNextDueCanWarmBiconditional`, `ColdStartupWaitsFullInterval`, `TotalCrc32cMaskedParity`, and `IndInv`) with `0` states left on queue and **100% action coverage (`16/16` entries: `Init` + all 15 `Next` actions firing with `count > 0`)**. [Aggregate action coverage](04_evidence/coverage.json)
