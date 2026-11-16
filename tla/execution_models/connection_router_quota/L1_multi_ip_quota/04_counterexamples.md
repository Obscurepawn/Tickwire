# Counterexamples and Rust fixes

Adversarial source audit and code-faithful modeling of `L1_MultiIpQuota` across `tickwire-quota`, `tickwire-binance-quota`, `tickwire-account-gateway`, and `tickwire-spot-execution` uncovered two cross-layer quota bugs: pre-admission quota burning on local order rejections, and read-only `can_afford` vs. mutating `allow` divergences under duplicate `RateLimitId` spends, `> u32::MAX` burst coalescing, `u64::MAX` window limits, and venue preset edge cases. Both were reproduced with failing Red tests, fixed in Rust, and verified to exhaustion under TLC.

## Bug 4: Pre-admission `allow_*` permanently burned exchange `ORDER` and `IP` windows on local admission rejection (`TransactionalRollbackExact`)

- **Scenario**:
  1. Strategy submits a `New` order with `AcctOrderCap = 1`.
  2. Local order admission rejects before queueing any wire bytes (e.g., `InsufficientAsset` or `OrderInDoubt`), so `0` bytes ever touch any socket.
  3. Strategy immediately submits a valid `1`-lot order within the same `ORDER` window.
- **Fix**: Commit mutating `allow_*` debits only after local order admission and wire encoding succeed (`crates/tickwire-account-gateway/src/pool.rs:420-480`, `crates/tickwire-spot-execution/src/session.rs:1103-1196`).

## Bug 5: `RateLimiter::can_afford` diverged from `allow` on duplicate `RateLimitId` spends, `u64` burst coalescing, and `u64::MAX` limits (`CanAffordMatchesAllow`)

- **Scenario**:
  1. Caller probes `can_afford(&[Spend::new(w, 6), Spend::new(w, 6)], now)` when `limit = 10, used = 3`: `allow` reports `Exhausted { used: 9, requested: 6 }`, whereas `can_afford` must track cumulative `prior_units` per window rather than summing upfront.
  2. Two bursts at the same `now_ns` coalesce `u32::MAX` + `10` units in `RollingWindow` with `limit = 10_000_000_000`, or `limit = u64::MAX` with `used = u64::MAX - 2`.
- **Fix**: Updated `RateLimiter::can_afford`, `FixedWindow::{can_take_readonly, try_take}`, and `RollingWindow::{retry_after_for_units, can_take_readonly, try_take, put_back}` (`crates/tickwire-quota/src/rate_limiter.rs:551-842`, `1338-1367`) to track cumulative `prior_units` per window, widen `RollingEntry.units` and `WindowUndo` to `u64`, use `checked_add` and `u128` deficit arithmetic, and align `VenueRateLimiter::can_afford_*` (`crates/tickwire-quota/src/rate_limiter.rs:1921-2268`).
- **Regression test**: `can_afford_matches_allow_across_duplicate_spends_large_coalescing_and_presets` in `crates/tickwire-quota/tests/multi_ip_limiter.rs`.
