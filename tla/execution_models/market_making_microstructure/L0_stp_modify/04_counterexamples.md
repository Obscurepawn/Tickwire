# Counterexamples and Rust fixes

Adversarial cross-layer audit and code-faithful modeling of `L0_StpModify` across `tickwire-account`, `tickwire-account-gateway`, `tickwire-binance-account`, and `tickwire-spot-execution` uncovered four real production bugs in terminal `PendingModify` superseding, out-of-order fill/observation authorization bounds during modifications, Spot `cancelReplace` `AmendAmbiguous` two-phase query & WS replacement-leg binding, and pre-wire rate-limiter / order-slot preservation on local STP/modify refusals. All four were reproduced with failing Red tests, fixed in Rust, and verified to exhaustion under TLC.

## Bug 2: Terminal fill or cancel left in-flight `PendingModify` dangling, leaked `shadow_holds`, and blocked management eviction (`TerminalOrderClearsStpAndPendingModify` & `ModifyRollbackRestoresExactRestingState`)

- **Scenario**:
  1. Open order `o` admits `Modify(intent = M)` increasing price/quantity (`o.pending_modify = Some(M)`, locking Futures `shadow_holds[M]` and tentative `new_client_id`).
  2. Before `Modified` or `ModifyRejected` arrives, a full `Fill` or `Order(Canceled)` closes `o` (`o.terminal = true`), and later either (a) `evict_expired` runs, (b) a late `ModifyRejected` arrives, or (c) on Spot `cancelReplace`, prior-leg `Order(Canceled)` arrives over WS right before replacement-leg `Modified` with a brand-new `venue_order_id`.
- **Pre-fix behavior**: `events.rs:observe` left `o.pending_modify = Some(M)` and `shadow_holds[M]` attached to the closed order, kept `ModificationRecord` permanently unretired (`!confirmed && !aborted && rejected.is_none()`), prevented `evict_retired_management_admission` from ever reclaiming `M` under `MAX_MANAGEMENT_ADMISSIONS`, and froze `Account` if Spot WS delivered leg-1 `Canceled` before leg-2 `Modified`.
- **Fix**: Added `supersede_pending_modify_on_terminal` (`crates/tickwire-account/src/modifications.rs:903-928`, called from `crates/tickwire-account/src/events.rs:891-893`), `ModificationRecord.superseded` eviction once the parent order expires (`crates/tickwire-account/src/lib.rs:1026-1240`), idempotent late `ModifyRejected` transitions, and Spot `cancelReplace` replacement-leg reopening when a brand-new `venue_order_id` arrives after `UserCanceled` (`crates/tickwire-account/src/modifications.rs:598-610`).
- **Regression test**: `terminal_order_supersedes_pending_modify_releases_delta_hold_evicts_and_tolerates_late_modify_events` in `crates/tickwire-account/tests/stp_modify_projected_fill.rs`.

## Bug 3: Out-of-order `FillEvent` or `OrderObservation` racing with `PendingModify` / `ModificationRecord` triggered false `Conflict` freeze

- **Scenario**:
  1. Order `o` (`BUY 2 @ 100`) submits `Modify` to `BUY 5 @ 105`; venue executes `Fill` at `105` with `original_quantity = 5` (or an out-of-order prior-leg `Fill` at `100` after `Modified` lowered price to `98`) before `Modified` is observed locally.
- **Pre-fix behavior**: `known_order_quantity`, `authorized_quantity_bound`, and `authorized_price_bound` only inspected `amendments` (`AmendKeepPriority`), ignoring `o.pending_modify` and `self.modifications` (`prior_price` / `requested_price` / `requested_quantity`), rejecting valid venue fills with `Error::Conflict` and freezing `Account`.
- **Fix**: Extended `known_order_quantity` (`crates/tickwire-account/src/amendments.rs:285-315`) and `authorized_quantity_bound` / `authorized_price_bound` (`crates/tickwire-account/src/events.rs:897-940`) to include in-flight `PendingModify` and non-aborted/non-rejected `ModificationRecord`s (recording `prior_price` on `ModificationRecord` for deterministic authorization checks).
- **Regression test**: `terminal_order_supersedes_pending_modify_releases_delta_hold_evicts_and_tolerates_late_modify_events` in `crates/tickwire-account/tests/stp_modify_projected_fill.rs`.

## Bug 4: `QueryDesk` / `AccountSession` queried stale `orderId` instead of `new_client_id` on Spot `cancelReplace` `AmendAmbiguous`, and private WS rejected rotated replacement `orderId`

- **Scenario**:
  1. A Spot `Modify` (`cancelReplace` rotating `orig_cid -> new_cid` and creating a brand-new `orderId = 9702` while canceling `9701`) suffers a post-byte transport drop (`DoubtReason::AmendAmbiguous`).
  2. Separately, Private WS delivers replacement leg `executionReport(NEW, c = new_cid, i = 9704)` while `identity.venue_order_id` still holds `Some("9702")`.
- **Pre-fix behavior**: Querying `orderId = 9701` first observed `CANCELED` even after a succeeded `cancelReplace`, misclassifying the live replacement order `9702` as `ModifyRejected(OrderNotOpen)` and stranding an unhedged live order on the exchange; meanwhile private-stream binding rejected `old != native` (`"9702" != "9704"`), quarantining the WS frame.
- **Fix**: Updated `QueryDesk` and Binance `AccountSession` (`crates/tickwire-account/src/query_desk.rs:647-868`, `crates/tickwire-binance-account/src/session.rs:2885-3205`) to probe `origClientOrderId = new_client_id` first on `ProductKind::Spot` and fall back to `orderId` / `orig_client_id` on `-2013`, while binding authorized Spot replacement legs and historical native-ID aliases cleanly across live WS and REST snapshots.
- **Regression tests**: `crates/tickwire-account/tests/query_desk.rs` and `crates/tickwire-binance-account/tests/unified_binance_session.rs`.

## Bug 5: Local `CrossesBook` / `Modify` refusals burned `RateLimiter` tokens and retained aborted order slots

- **Scenario**:
  1. Strategy submits `NewOrder` or `Modify` that local STP / pending-action admission refuses before wire encoding (`CrossesBook`, `TooManyPendingActions`, `NoChange`, etc.).
- **Pre-fix behavior**: Spending rate-limit tokens before local STP / modify admission or retaining locally refused intents in active order slots burned exchange `ORDER` and `IP` quota and exhausted active order capacity under repeated local STP rejections.
- **Fix**: Validated local STP and single-in-flight `Modify` admission before `quota.pick_route_and_place_or_nack` / `quota.pick_route_and_spend_command_or_nack` (`crates/tickwire-spot-execution/src/session.rs:1103-1185`, `crates/tickwire-spot-execution/src/session.rs:1300-1375`, `crates/tickwire-binance-account/src/session.rs:1986-2124`) and rolled back zero-byte aborts cleanly via `AccountGateway::abort` (`crates/tickwire-account-gateway/src/lib.rs:1195-1212`).
- **Regression tests**: `crates/tickwire-spot-execution/tests/uta_sessions.rs` and `crates/tickwire-binance-account/tests/unified_binance_session.rs`.

