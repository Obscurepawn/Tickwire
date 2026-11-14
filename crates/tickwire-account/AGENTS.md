# tickwire-account

`tickwire-account` (`#![forbid(unsafe_code)]`) owns the single-writer in-memory state machine for one risk domain:
multi-asset balances, holds, derivative positions, orders, conditional structures, amendments, external cashflows, and
strategy handovers. Gateways and journal replay drive `Account` with validated `CanonicalCommand`, `CanonicalEvent`,
and `RecoveryRecord` inputs, performing pure state reduction over `tickwire-risk` and `tickwire-venue` without disk
I/O, sockets, or exchange wire formatting.

## Invariants and Change Guidelines

* **Candidate-first admission, native `Modify`, and `StpBook` discipline**: Keep `Account::admit`
  ([src/admission.rs](src/admission.rs)) transactional by mutating a cloned candidate before commit; freeze
  risk-increasing admissions on `Error::Conflict` or `RiskError::ArithmeticOverflow`, while keeping cancels, queries,
  and keep-priority reductions admissible ([src/amendments.rs](src/amendments.rs),
  [src/structures.rs](src/structures.rs)). Maintain cache-line-aligned integer-tick BBO state in
  [src/stp_book.rs](src/stp_book.rs) across all limit order transitions (including dual-price pending `Modify`
  aggressiveness), gating `Error::CrossesBook` admission refusals on `stp_enforcement`, and hold worst-case Spot asset
  and derivative margin/limit exposure in [src/modifications.rs](src/modifications.rs) and
  [src/calculation.rs](src/calculation.rs) while a single `PendingModify` is in flight.
* **Distinct lifecycle vs. economic evidence, `ProjectedFill`, and per-order doubt isolation**: In
  [src/events.rs](src/events.rs), [src/projected_fill.rs](src/projected_fill.rs),
  [src/calculation.rs](src/calculation.rs), and [src/query_desk.rs](src/query_desk.rs), hold reserves whenever
  `observed_cumulative` exceeds applied fills, fee closure remains open, or `in_doubt` (`DoubtReason`) is set; emit
  volatile `ProjectedFill` deltas from `on_cum_qty` using pure-integer Half-Even rounding without ever mutating settled
  `Order.filled`, `Asset`, `Position`, `facts`, or `CanonicalRecord`; exclude volatile `in_doubt`, projection
  watermarks, and diagnostic `TerminalReason` fields from `PartialEq for Order` for deterministic replay; close
  unconfirmed zero-fill single-leg `VenueEventBody::Rejected` orders (`uncreated = true`, `fees_closed = true`) without
  freezing while reopening via `Order::reopen_aborted` (`observation_unreconciled = true`, `frozen = true`) on any
  contradictory execution fact; and schedule coalesced per-order status/trade queries in `QueryDesk` with zero
  post-init heap growth.
* **Shared liquidity with isolated strategy authority**: Strategies share asset balances while preserving distinct
  `origin` and `manager` (`instance`, `generation`) identities across [src/handover.rs](src/handover.rs) so only the
  active manager can cancel, amend, or query an order.
* **Account-level inputs, cold exchange bootstrap, and recovery**: Route non-order cashflows, income associations, and
  reconciliation closures through [src/account_inputs.rs](src/account_inputs.rs), [src/external.rs](src/external.rs),
  and [src/recovery.rs](src/recovery.rs), and reconstruct stateless cold-start open orders, shared-pool (`strategy 0`)
  settled balances/positions, external lock isolation, and per-strategy `(epoch, intent_id)` sequence floors through
  `Account::from_live_exchange` in [src/live_bootstrap.rs](src/live_bootstrap.rs).
* **Active-protected windowed eviction, `O(log N)` indices, and management headroom**: Keep [src/pool.rs](src/pool.rs)
  (`WindowedTable`) free of steady-state heap allocations, synchronize `orders_by_client_id`, `orders_by_native_id`,
  `orders_by_internal_id`, and `orders_by_intent` across admission, venue ID binding, recovery, bootstrap, and eviction,
  never evict an active or unreconciled order (`!Order::is_retirable`), and preserve dedicated
  `MAX_MANAGEMENT_ADMISSIONS` headroom so cancels and queries never starve behind `MAX_ORDERS` live orders.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/pool.rs](src/pool.rs) · [src/query_desk.rs](src/query_desk.rs) ·
  [src/stp_book.rs](src/stp_book.rs) · [src/projected_fill.rs](src/projected_fill.rs) ·
  [src/modifications.rs](src/modifications.rs) · [src/admission.rs](src/admission.rs) ·
  [src/events.rs](src/events.rs) · [src/live_bootstrap.rs](src/live_bootstrap.rs) ·
  [tests/stp_modify_projected_fill.rs](tests/stp_modify_projected_fill.rs) ·
  [tests/query_desk.rs](tests/query_desk.rs) · [tests/admission.rs](tests/admission.rs) ·
  [tests/windowed_eviction.rs](tests/windowed_eviction.rs) · [tests/live_bootstrap.rs](tests/live_bootstrap.rs) ·
  [tests/amend.rs](tests/amend.rs) ·
  [tests/conditional.rs](tests/conditional.rs) · [tests/account_inputs.rs](tests/account_inputs.rs) ·
  [tests/facts.rs](tests/facts.rs) · [tests/futures.rs](tests/futures.rs) · [tests/handover.rs](tests/handover.rs) ·
  [tests/nonincreasing.rs](tests/nonincreasing.rs) · [tests/recovery.rs](tests/recovery.rs) ·
  [tests/edge_cases.rs](tests/edge_cases.rs)
* **Specifications and models**: [account.md](../../specs/implementation/account.md) ·
  [conditional-orders.md](../../specs/implementation/conditional-orders.md) ·
  [spot-amend.md](../../specs/implementation/spot-amend.md) ·
  [usdm-account-facts.md](../../specs/implementation/usdm-account-facts.md) ·
  [spot-strategy-routing.md](../../specs/implementation/spot-strategy-routing.md) ·
  [account](../../tla/execution_models/account/ROADMAP.md) ·
  [cash_evidence](../../tla/execution_models/cash_evidence/ROADMAP.md) ·
  [strategy_intake](../../tla/execution_models/strategy_intake/ROADMAP.md)

```sh
cargo test --locked -p tickwire-account --all-targets
./scripts/check-tla.sh account
```
