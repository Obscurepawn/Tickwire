# Counterexamples and Rust fixes

Adversarial source audit and code-faithful modeling of `L1_MultiIpQuota` across `tickwire-quota`, `tickwire-binance-quota`, `tickwire-binance-account`, and `tickwire-spot-execution` uncovered two real cross-layer quota bugs: pre-admission quota burning on local `Account::admit` rejections, and read-only `can_afford` vs. mutating `allow` divergences under duplicate `RateLimitId` spends, `> u32::MAX` burst coalescing, `u64::MAX` window limits, and venue preset edge cases. Both were reproduced with failing Red tests, fixed in Rust, and verified to exhaustion under TLC.

## Bug 4: Pre-admission `allow_quota_*` permanently burned exchange `ORDER` and `IP` windows on local `Account::admit` rejection (`TransactionalRollbackExact`)

- **Scenario**:
  1. Strategy submits a `New` order with `AcctOrderCap = 1`.
  2. `Account::admit` rejects locally before queueing any wire bytes (e.g., `InsufficientAsset`, `OrderInDoubt`, or `ManagementDenied`), so `0` bytes ever touch any socket.
  3. Strategy immediately submits a valid `1`-lot order within the same 10-second `ORDER` window.
- **Pre-fix behavior**: `RestOwner::submit`, `RestOwner::submit_pooled_rest`, and `WsOwner::submit_websocket` called mutating `allow_quota_on_ip` *before* `ingress.submit` / `ingress.submit_on_slot`. When `Account::admit` rejected locally, the debited `ORDER` and `IP` units were never rolled back, causing the subsequent valid order to fail with `RateReject::Exhausted` even though zero requests were sent to the exchange.
- **Fix**: Updated `RestOwner::submit`, `RestOwner::submit_pooled_rest` (`crates/tickwire-binance-account/src/rest.rs:531-685`), and `WsOwner::submit_websocket` (`crates/tickwire-binance-account/src/ws/owner.rs:289-355`) to run read-only `can_afford_quota_on_ip` before admission and commit `allow_quota_*` only after gateway submission succeeds (`status != Status::Aborted`), matching `RestSession::submit` (`crates/tickwire-spot-execution/src/session.rs:711-844`).
- **Regression test**: `pooled_rest_eof_ready_baton_and_abort_inflight_and_local_reject_quota` in `crates/tickwire-binance-account/tests/multi_connection_router.rs`.

## Bug 5: `RateLimiter::can_afford` diverged from `allow` on duplicate `RateLimitId` spends, `u64` burst coalescing / `u64::MAX` limits, and unseen `OkxSpot` / `BinanceUsdm` probes (`CanAffordMatchesAllow`)

- **Scenario**:
  1. Caller probes `can_afford(&[Spend::new(w, 6), Spend::new(w, 6)], now)` when `limit = 10, used = 3`: `allow` reports `Exhausted { used: 9, requested: 6 }`, whereas `can_afford` summed `6 + 6 = 12` up front and reported `Exhausted { used: 3 }` with a shorter `RollingWindow` `retry_after_ns` computed for `6` instead of `12` units.
  2. Two bursts at the same `now_ns` coalesce `u32::MAX` + `10` units in `RollingWindow` with `limit = 10_000_000_000`: `can_afford` returned `Ok(())`, whereas `RollingEntry.units: u32` overflowed in `try_take` and returned `Err(RateReject::Invalid)`.
  3. At `limit = u64::MAX` and `used = u64::MAX - 2`, `used.saturating_add(5) > u64::MAX` evaluated `false`, overflowing `self.used += 5`.
  4. On `VenuePreset::OkxSpot` with an unseen symbol (`order_units = 0` or `order_units = 61`) or `BinanceUsdm` with `weight = 0` on clock regression, `can_afford_*` skipped checks enforced by `allow_*`.
- **Fix**: Updated `RateLimiter::can_afford`, `FixedWindow::{can_take_readonly, try_take}`, and `RollingWindow::{retry_after_for_units, can_take_readonly, try_take, put_back}` (`crates/tickwire-quota/src/rate_limiter.rs:551-842`, `1338-1367`) to track cumulative `prior_units` per window, widen `RollingEntry.units` and `WindowUndo` to `u64`, use `checked_add` and `u128` deficit arithmetic, and align `VenueRateLimiter::can_afford_*` (`crates/tickwire-quota/src/rate_limiter.rs:1891-2178`).
- **Regression test**: `can_afford_matches_allow_across_duplicate_spends_large_coalescing_and_presets` in `crates/tickwire-quota/tests/multi_ip_limiter.rs`.
