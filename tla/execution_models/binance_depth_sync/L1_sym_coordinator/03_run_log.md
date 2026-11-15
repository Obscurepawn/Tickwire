# Symbolic Apalache/Z3 & Explicit TLC Runs — `L1_SymDepthSync`

Dual-engine symbolic verification of `tickwire_binance::depth_sync::DepthSync` + `tickwire_book::Book` combining Apalache `0.63.0` (`Z3` `QF_LIA` unbounded symbolic induction + 64-bit bounded model checking) and repository-pinned TLC `2.19` (`tla/tools/tla2tools.jar`).

## 1. Symbolic Apalache + Z3 Runs (`BoundedMode = FALSE`, `--cinit=ConstInit`)

| Mode & Obligation | Command / Target | Result | VCs / Depth | Runtime | Evidence |
| --- | --- | --- | ---: | ---: | --- |
| Snowcat static typecheck | `run_apalache.sh typecheck L1_SymDepthSync.tla` | `PASS` (`rc=0`) | 58 typed vars | `4.500s` | [apalache_typecheck.log](04_evidence/apalache_typecheck.log) |
| Mode S2 full symbolic induction (`IndInv`, $\forall k \ge 0$, $0 \le \text{id}, t \le 2^{64}-1$, arbitrary `cfg_capacity \in 1..2^64-1`) | `run_apalache.sh inductive --indinv=IndInv --inv=SafetyConjunction --cinit=ConstInit` | **`PASS` (`rc=0`)** | Base: 167 VCs (`7.163s`)<br>Step: 38 transitions / 167 VCs (`8.710s`)<br>Entail: 155 VCs (`7.680s`) | `9.23s` (parallel wall) | [apalache_inductive.log](04_evidence/apalache_inductive.log) |
| Mode S1 64-bit `u64` boundary BMC | `run_apalache.sh bmc --inv=SafetyConjunction --length=5 --cinit=ConstInit --no-deadlock` | **`PASS` (`rc=0`)** | $k = 5$ (155 VCs/step) | `10.217s` | [apalache_bmc.log](04_evidence/apalache_bmc.log) |

## 2. Explicit TLC Smoke & Vacuous-Pass Gate (`BoundedMode = TRUE`)

From the workspace root run `./scripts/check-tla.sh binance_depth_sync`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | --- |
| [Binance Spot policy (`L1_SymDepthSync_spot.cfg`, `PolicyKind = 1`)](04_evidence/L1_SymDepthSync_spot.log) | 1,472,132 | 36,470 | 7 | 0 | `39s` (`-coverage 60`) |
| [Binance USD-M policy (`L1_SymDepthSync_usdm.cfg`, `PolicyKind = 2`)](04_evidence/L1_SymDepthSync_usdm.log) | 1,466,425 | 36,270 | 7 | 0 | `39s` (`-coverage 60`) |

Both exhaustive TLC runs verified `SafetyConjunction` and `IndInv` (`02_models/L1_SymDepthSync.tla` SHA-256 `6be638d0543cb8e53b68cf2d77351723f04cefe9d826ef67bbc8067edf0c5d78`) with `0` states left on queue and **100% action coverage (`23/23` entries: `Init` + all 22 `Next` actions firing, `min_count = 1,008` on `SnapshotDelayed`)**. [Aggregate action coverage](04_evidence/coverage.json)
