# Symbolic Apalache/Z3 & Explicit TLC Runs — `L2_SymRateLimiter`

Dual-engine symbolic verification of `tickwire_quota::rate_limiter::{RateLimiter, VenueRateLimiter}` combining Apalache `0.63.0` (`Z3` `QF_LIA` unbounded symbolic induction + 64-bit/`u128` bounded model checking) and repository-pinned TLC `2.19` (`tla/tools/tla2tools.jar`).

## 1. Symbolic Apalache + Z3 Runs (`BoundedMode = FALSE`, `--cinit=ConstInit`)

| Mode & Obligation | Command / Target | Result | VCs / Depth | Runtime | Evidence |
| --- | --- | --- | ---: | ---: | --- |
| Snowcat static typecheck | `run_apalache.sh typecheck L2_SymRateLimiter.tla` | `PASS` (`rc=0`) | 41 typed vars | `2.969s` | [apalache_typecheck.log](04_evidence/apalache_typecheck.log) |
| Mode S2 full symbolic induction (`IndInv`, $\forall k \ge 0$, $0 \le t \le 2^{64}-1$, arbitrary `1..2^64-1` window caps, `u128` `80%` warn reconciliation) | `run_apalache.sh inductive --indinv=IndInv --inv=SafetyConjunction --cinit=ConstInit` | **`PASS` (`rc=0`)** | Base: 113 VCs (`6.346s`)<br>Step: 32 transitions / 113 VCs (`39.623s`)<br>Entail: 109 VCs (`6.574s`) | `~40s` (parallel wall) | [apalache_inductive.log](04_evidence/apalache_inductive.log) |
| Mode S1 64-bit `u64` & `u128` boundary BMC | `run_apalache.sh bmc --inv=SafetyConjunction --length=2 --cinit=ConstInit --no-deadlock` | **`PASS` (`rc=0`)** | $k = 2$ (109 VCs/step) | `71.104s` | [apalache_bmc.log](04_evidence/apalache_bmc.log) |

## 2. Explicit TLC Smoke & Vacuous-Pass Gate (`BoundedMode = TRUE`)

From the workspace root run `./scripts/check-tla.sh connection_router_quota`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | --- |
| [No-ban venue profile (`L2_SymRateLimiter.cfg`, `AllowBan = FALSE`)](04_evidence/L2_SymRateLimiter.log) | 532,263 | 104,416 | 4 | 0 | `01m 14s` (`-coverage 60`) |
| [Binance `418` ban-enabled profile (`L2_SymRateLimiter_ban.cfg`, `AllowBan = TRUE`)](04_evidence/L2_SymRateLimiter_ban.log) | 554,825 | 110,407 | 4 | 0 | `01m 14s` (`-coverage 60`) |

Both exhaustive TLC runs verified all 8 safety invariants in `SafetyConjunction` plus `IndInv` (`02_models/L2_SymRateLimiter.tla` SHA-256 `28cb6ca02eb0c7af3aee3666ac61b785b44b7ff51019d51c66d9e63e131b4f95`) with `0` states left on queue and **100% action coverage (`13/13` entries: `Init` + all 12 `Next` actions firing, `min_count = 5,783` on `NoteHttp418AccountBan`)**. [Aggregate action coverage](04_evidence/coverage.json)
