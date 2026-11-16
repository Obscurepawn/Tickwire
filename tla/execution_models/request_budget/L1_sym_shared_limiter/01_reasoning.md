# Symbolic abstraction and inductive invariant reasoning — `L1_SymSharedLimiter`

## Why symbolic verification (Mode S2 + Mode S1) is required
Finite-state TLC in `L0_SharedOrsLimiter` bounds `MaxClock = 1`, `SharedIpCap = 1..2`, and `ReportedLimit = 5`. Production `SharedRateLimiter` (`crates/tickwire-quota/src/shared.rs`) operates over:
1. Full 64-bit monotonic timestamps `now_ns \in 0 .. 2^64 - 1`, `AtomicFixedWindow` packed `(epoch: u32, used: u32)` with half-range modular clock-regression rejection (`epoch < cur_epoch && cur_epoch.wrapping_sub(epoch) < u32::MAX / 2`, `shared.rs:358-359`), and `AtomicRollingWindow` 20-sub-bucket ticks (`sub_window_ns = max(interval_ns / 20, 1)`, 28-bit `MAX_ROLLING_COUNT = 268_435_455`, `shared.rs:424-642`).
2. Cross-account shared IP (`RateRule::ip`, `account = 0`) sub-bucket coalescing across concurrent `AccountSession`s (`account_ref = 1..255`), where Account 2's partial-failure `put_back` must subtract only Account 2's booked units from the shared sub-bucket (`slot_tick == booked_tick && slot_count >= req`) without disturbing Account 1's units.
3. Exact `u128` integer rising-edge venue reconciliation (`u128::from(reported_count) * 100 >= u128::from(reported_limit) * 80` and `reported_count >= reported_limit`, `shared.rs:1385-1440`).

## Dual-engine verification strategy
- **Unbounded Z3 `QF_LIA` induction (`BoundedMode = FALSE`, `run_apalache.sh inductive --cinit=ConstInit --indinv=IndInv --inv=SafetyConjunction`)**: Proves `IndInv` across arbitrary capacities (`1 .. 2^32 - 1` / `1 .. 2^28 - 1`), arbitrary `u64` timestamps (`0 .. 2^64 - 1`), and all reachable depths $\forall k \ge 0$, using literal `1_000_000_000` (`1s`) fixed-epoch and `3_000_000_000` (`60s / 20`) rolling sub-bucket divisors to keep Z3 inside decidable linear integer arithmetic.
- **64-bit/`u128` BMC (`run_apalache.sh bmc --length=2`)** + **TLC 100% action-coverage smoke (`BoundedMode = TRUE`)**: Confirms non-vacuity and 100% nonzero coverage across all 13 transitions (`L1_SymSharedLimiter.cfg` and `L1_SymSharedLimiter_ban.cfg`).
