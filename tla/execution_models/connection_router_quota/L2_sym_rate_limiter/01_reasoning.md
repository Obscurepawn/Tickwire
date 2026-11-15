# Reasoning — `L2_sym_rate_limiter` (Symbolic Unbounded & Full 64-Bit `RateLimiter` / `VenueRateLimiter` Proof)

## 1. Blind Spots Closed Relative to Explicit `L0_pool_router` & `L1_multi_ip_quota`

Explicit-state TLC verification in `L1_multi_ip_quota` (`tla/execution_models/connection_router_quota/L1_multi_ip_quota/02_models/L1_MultiIpQuota.tla`) compressed numeric domains to keep explicit state spaces tractable (`FixedInterval = 2`, `RollingInterval = 3`, `AcctOrderCap = 2`, `IpWeightCap = 3..4`, `MaxNow = 5..6`, `CooldownLen = 2`). That left four production 64-bit / 128-bit arithmetic and horizon blind spots open:

1. **Full 64-bit nanosecond epoch division and `retry_after_ns` saturation (`0 .. 2^64 - 1`)**:
   - `FixedWindow::advance_to` (`crates/tickwire-quota/src/rate_limiter.rs:520-538`) computes `epoch_idx = now_ns / interval_ns` (`1_000_000_000 ns` = `1s`) and `FixedWindow::retry_after_ns` (`rate_limiter.rs:550-556`) computes `epoch_idx.saturating_add(1).saturating_mul(interval_ns).saturating_sub(now_ns)`. Near `u64::MAX = 18_446_744_073_709_551_615`, `(epoch_idx + 1) * 1_000_000_000 = 18_446_744_074_000_000_000 > u64::MAX`, exercising the `saturating_mul` clamp.
2. **Full 64-bit rolling-window eviction, coalescing, and ring-slot exhaustion**:
   - `RollingWindow::evict_expired`, `state_at`, `can_take_readonly`, `try_take`, and `put_back` (`rate_limiter.rs:649-846`) operate over arbitrary `u64` nanosecond timestamps (`60_000_000_000 ns` = `60s`), `checked_add` unit sums against arbitrary `limit \in 1 .. 2^64 - 1`, same-nanosecond tail coalescing (`WindowUndo::RollingCoalesced`), new-entry ring push (`WindowUndo::RollingPushed`), and ring-capacity rejection (`eff_len == cap && !coalesces`).
3. **Unbounded multi-spend `can_afford` vs. `allow` equivalence and reverse `put_back` rollback**:
   - `RateLimiter::can_afford` (`rate_limiter.rs:1338-1372`) accumulates per-window `prior_units` read-only across duplicate `RateLimitId` spends, whereas `RateLimiter::allow` (`rate_limiter.rs:1385-1415`) mutates windows sequentially and unwinds `undo_stack` in reverse order via `put_back` (`Fixed`, `RollingCoalesced`, `RollingPushed`) on partial failure.
4. **128-bit exchange reconciliation warning arithmetic (`u128`) and multi-IP `429` vs. `418` isolation**:
   - `RateLimiter::reconcile_exchange_usage` (`rate_limiter.rs:1422-1468`) widens `u128::from(reported_used) * 100 >= u128::from(limit) * u128::from(exch_warn_pct)` (`80%`) and clamps `reported_used.min(limit)` over arbitrary `0 .. 2^64 - 1` headers, while `note_ip_cooldown` (`429`) and `freeze_account` (`418`) (`rate_limiter.rs:1117-1305`) enforce per-IP failover vs. account-wide freeze across unbounded trace depth $\forall k \ge 0$.

## 2. Symbolic Reduction & Linear `QF_LIA` Encoding (`symbolic.md`)

- **Literal-interval `QF_LIA` linearity**: `FixedEpochIdx(t)` divides by literal `1_000_000_000` (`1s`) in Apalache (`BoundedMode = FALSE`) and literal `2` in explicit TLC (`BoundedMode = TRUE`), while `ReconcileWarn` multiplies by literals `100` and `80`. No two symbolic `VARIABLE`s are ever multiplied or divided, keeping Z3 strictly in decidable linear integer arithmetic (`QF_LIA`).
- **Canonical 2-slot symbolic rolling FIFO**: Each per-IP `RollingWindow` (`ip \in {1, 2}`) is represented by `(len, s0_ts, s0_u, s1_ts, s1_u, used, peak, warn)` with `s0_ts < s1_ts <= last_now_ns` when `len = 2`, capturing empty (`0`), single-slot (`1`), coalesced-tail, and ring-full (`2`) transitions over arbitrary `0 .. 2^64 - 1` timestamps and capacities without modulo-index quantifier blowup.
- **Dual-order transactional rollback coverage**:
  - `PlaceOrderSingle` and `PlaceOrderDuplicateFixed` (`[AcctOrder(u1), AcctOrder(u2), IpWeight(w)]`) verify duplicate-window `prior_units` accumulation and reverse `Fixed` rollback when the trailing `RollingWindow` rejects.
  - `RollingFirstMultiSpend` (`[IpWeight(w), AcctOrder(u)]`) directly exercises `WindowUndo::RollingCoalesced` and `WindowUndo::RollingPushed` inside `RollingWindow::put_back` when the trailing `FixedWindow` rejects.

## 3. Target Invariants (`SafetyConjunction` & `IndInv`)

1. `WindowCapsRespected`: `ord_used <= cfg_acct_ord_cap`, `cxl_used <= cfg_sym_cxl_cap`, `ip1_used <= cfg_ip_wt_cap`, `ip2_used <= cfg_ip_wt_cap`, `ip1_peak <= cfg_ip_wt_cap`, and `ip2_peak <= cfg_ip_wt_cap` at all reachable states.
2. `CanAffordMatchesAllow`: on every route admission (`Place`, `DuplicateFixed`, `RollingFirst`, `Cancel`, `Query`), read-only `can_afford` returns `Ok(())` if and only if transactional `allow` returns `Ok(RatePermit)`, with identical `RateReject` classification.
3. `TransactionalRollbackExact`: whenever any multi-window `allow` rejects at step $j > 0$, reverse-order `put_back` restores every earlier window (`Fixed`, `RollingCoalesced`, and `RollingPushed`) to its exact post-eviction pre-debit state with zero leaked units.
4. `CancelAndQueryIsolatedFromOrderCap`: `CancelOrder` never mutates `AcctOrder`, and `QueryRequest` never mutates `AcctOrder` or `SymCancel`, even when `ord_used == cfg_acct_ord_cap`.
5. `PerIpCooldownIsolation`: `NoteHttp429IpCooldown(ip)` never freezes the account and never raises the sibling IP's cooldown deadline, whereas `NoteHttp418AccountBan` (`AllowBan = TRUE`) permanently blocks all routes across both IPs.
6. `LocalRejectAndClockRegressionNoBurn`: pre-admission local rejects and `now_ns < last_now_ns` clock regressions leave all window counters, ring entries, and `last_now_ns` untouched.
7. `U64AndU128OverflowSafe`: `checked_add` window checks match exact $\mathbb{Z}$ bounds without wraparound, `retry_after_ns \in 0 .. U64_MAX`, and `reported * 100 <= U128_MAX` holds across `0 .. 2^64 - 1`.
