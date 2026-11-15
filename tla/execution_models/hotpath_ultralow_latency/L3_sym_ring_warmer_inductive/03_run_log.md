# Symbolic Apalache/Z3 & Explicit TLC Runs — `L3_SymRingWarmerInductive`

Dual-engine verification combining Apalache `0.63.0` (`Z3` `QF_LIA` symbolic inductive invariants + 64-bit `u64` bounded model checking) and repository-pinned TLC `2.19` (`tla/tools/tla2tools.jar`, `-workers 1 -coverage 1`).

## 1. Symbolic Apalache + Z3 Runs (`BoundedMode = FALSE`, `--cinit=ConstInit`)

| Mode & Obligation | Command / Target | Result | VCs / Depth | Runtime | Evidence |
| --- | --- | --- | ---: | ---: | --- |
| Snowcat static typecheck | `run_apalache.sh typecheck L3_SymRingWarmerInductive.tla` | `PASS` (`rc=0`) | 33 typed vars | `1.234s` | [apalache_typecheck.log](04_evidence/apalache_typecheck.log) |
| Mode S2 CTI baseline (`SafetyConjunction` without CTI lemmas) | `run_apalache.sh inductive --indinv=SafetyConjunction --cinit=ConstInit` | `CTI` (`rc=12` at Step 1) | 94 base VCs (`PASS`), Step 1 `CTI` | `2.604s` | [apalache_cti_safety_only.log](04_evidence/apalache_cti_safety_only.log) · [cti_safety_only_witness.tla](04_evidence/cti_safety_only_witness.tla) |
| Mode S2 full symbolic induction (`IndInv`, $\forall k \ge 0$, $\forall \text{RingCapacity} \ge 2$, $1 \le \text{seq} \le 2^{63}$, $0 \le t \le 2^{64}-1$) | `run_apalache.sh inductive --indinv=IndInv --cinit=ConstInit --inv=SafetyConjunction` | **`PASS` (`rc=0`)** | Base: 111 VCs (`2.304s`)<br>Step: 17 actions (`2.495s`)<br>Entail: 94 VCs (`2.303s`) | `7.102s` | [apalache_inductive_IndInv.log](04_evidence/apalache_inductive_IndInv.log) |
| Mode S1 64-bit `u64` & `2^63` sequence BMC | `run_apalache.sh bmc --length=8 --inv=SafetyConjunction --cinit=ConstInit` | **`PASS` (`rc=0`)** | $k = 8$ | `7.516s` | [apalache_bmc_k8.log](04_evidence/apalache_bmc_k8.log) |
| Diagnostic Query 1: cold-startup unseeded `anchor == 0` | `bmc --length=2 --inv=Diag_StartupSeededBeforeFirstWarm --cinit=ConstInit` | `WITNESS` (`rc=12`) | $k = 1$ | `2.242s` | [apalache_diag_startup_anchor0.log](04_evidence/apalache_diag_startup_anchor0.log) · [diag_startup_anchor0_witness.tla](04_evidence/diag_startup_anchor0_witness.tla) |
| Diagnostic Query 2: `u64::MAX` `next_due_ns` vs `can_warm` saturation | `bmc --length=2 --inv=Diag_NextDueImpliesCanWarmAtU64Max --cinit=ConstInit` | `WITNESS` (`rc=12`) | $k = 1$ | `2.230s` | [apalache_diag_u64max_next_due.log](04_evidence/apalache_diag_u64max_next_due.log) · [diag_u64max_next_due_witness.tla](04_evidence/diag_u64max_next_due_witness.tla) |
| Diagnostic Query 3: `CoreNode` frozen/not-ready `0ns` poll spin | `bmc --length=3 --inv=Diag_CoreNodeNoZeroWaitSpinWhenFrozen --cinit=ConstInit` | `WITNESS` (`rc=12`) | $k = 2$ | `2.408s` | [apalache_diag_corenode_spin.log](04_evidence/apalache_diag_corenode_spin.log) · [diag_corenode_spin_witness.tla](04_evidence/diag_corenode_spin_witness.tla) |

## 2. Explicit TLC Smoke & Vacuous-Pass Gate (`BoundedMode = TRUE`)

From the workspace root run `./scripts/check-tla.sh hotpath_ultralow_latency`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Enabled warmer + SPMC batch ring (`L3_SymRingWarmerInductive.cfg`, `WarmupInterval = 2`)](04_evidence/L3_SymRingWarmerInductive.log) | 212,709 | 87,552 | 25 | 0 | 3s |
| [Disabled warmer + SPMC batch ring (`L3_SymRingWarmerInductive_Disabled.cfg`, `WarmupInterval = 0`)](04_evidence/L3_SymRingWarmerInductive_Disabled.log) | 46,980 | 19,304 | 21 | 0 | 1s |

Both exhaustive TLC runs verified all nine configured invariants (`TypeOK`, `ZeroNoAliasWhileBorrowed`, `WatermarkMonotonicOrder`, `LiveBusinessStateInvariance`, `LiveTlsNonceSynchronization`, `DummyLoopbackTlsPairSynchronization`, `WarmupStrictlyRespectsTrackerPolicy`, `DisabledWarmupNeverFires`, and `IndInv`) with `0` states left on queue and **100% action coverage (`18/18` entries: `Init` + all 17 `Next` actions firing with `count > 0`)**. [Aggregate action coverage](04_evidence/coverage.json)

## 3. Post-Fix Validation (`L5_SymWarmerCrcFixValidation`, 2026-11-15)

All three diagnostic boundary/scheduling findings above (`Diag_StartupSeededBeforeFirstWarm`, `Diag_NextDueImpliesCanWarmAtU64Max`, `Diag_CoreNodeNoZeroWaitSpinWhenFrozen`) were fixed in production (`crates/tickwire-runtime/src/node.rs:431-451,853-874,894-905`, `crates/tickwire-transport/src/warmup.rs:152-162`, `crates/tickwire-binance-execution/src/lib.rs:695-717`, `crates/tickwire-spot-runner/src/live.rs:364-374`) and mechanically re-verified in [L5_sym_warmer_crc_fix_validation](../L5_sym_warmer_crc_fix_validation/03_run_log.md): Apalache `0.63.0` + Z3 `IndInv` (`94` base VCs `3.790s` + `15` step actions `4.918s` + `76` entailment VCs `3.663s`, `rc=0`), 64-bit BMC (`k=8`, `52.939s`, `rc=0`), and TLC (`170,769` gen / `3,525` distinct & `163,201` gen / `1,719` distinct, `16/16` actions covered, `rc=0`).
