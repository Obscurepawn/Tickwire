# Counterexamples and Rust fixes

Adversarial cross-layer audit and code-faithful modeling of `L0_StpModify` across `tickwire-account`, `tickwire-binance-account`, `tickwire-spot-execution`, and `tickwire-spot-runner` uncovered four real production bugs in terminal `PendingModify` superseding, out-of-order fill/observation authorization bounds during modifications, Spot `cancelReplace` `AmendAmbiguous` two-phase query & WS replacement-leg binding, and pre-admission quota/`Origins` preservation on local STP/modify refusals. All four were reproduced with failing Red tests, fixed in Rust, and verified to exhaustion under TLC.

## Bug 2: Terminal fill or cancel left in-flight `PendingModify` dangling, leaked `shadow_holds`, and blocked management eviction (`TerminalOrderClearsStpAndPendingModify` & `ModifyRollbackRestoresExactRestingState`)

- **Scenario**:
  1. Open order `o` admits `Modify(intent = M)` increasing price/quantity (`o.pending_modify = Some(M)`, locking Futures `shadow_holds[M]` and tentative `new_client_id`).
  2. Before `Modified` or `ModifyRejected` arrives, a full `Fill` or `Order(Canceled)` closes `o` (`o.terminal = true`), and later either (a) `evict_expired` runs, (b) a late `ModifyRejected` arrives, or (c) on Spot `cancelReplace`, prior-leg `Order(Canceled)` arrives over WS right before replacement-leg `Modified` with a brand-new `venue_order_id`.
- **Pre-fix behavior**: `events.rs:observe` left `o.pending_modify = Some(M)` and `shadow_holds[M]` attached to the closed order, kept `ModificationRecord` permanently unretired (`!confirmed && !aborted && rejected.is_none()`), prevented `evict_retired_management_admission` from ever reclaiming `M` under `MAX_MANAGEMENT_ADMISSIONS`, and froze `Account` if Spot WS delivered leg-1 `Canceled` before leg-2 `Modified`.
- **Fix**: Added `supersede_pending_modify_on_terminal` (`crates/tickwire-account/src/modifications.rs:825-851`, called from `crates/tickwire-account/src/events.rs:620-622`), `ModificationRecord.superseded` eviction once the parent order expires (`crates/tickwire-account/src/lib.rs:945-985, 1132-1137`), idempotent late `ModifyRejected` transitions, and Spot `cancelReplace` replacement-leg reopening when a brand-new `venue_order_id` arrives after `UserCanceled` (`crates/tickwire-account/src/modifications.rs:521-536`).
- **Regression test**: `terminal_order_supersedes_pending_modify_releases_delta_hold_evicts_and_tolerates_late_modify_events` in `crates/tickwire-account/tests/stp_modify_projected_fill.rs`.

## Bug 3: Out-of-order `FillEvent` or `OrderObservation` racing with `PendingModify` / `ModificationRecord` triggered false `Conflict` freeze

- **Scenario**:
  1. Order `o` (`BUY 2 @ 100`) submits `Modify` to `BUY 5 @ 105`; venue executes `Fill` at `105` with `original_quantity = 5` (or an out-of-order prior-leg `Fill` at `100` after `Modified` lowered price to `98`) before `Modified` is observed locally.
- **Pre-fix behavior**: `known_order_quantity`, `authorized_quantity_bound`, and `authorized_price_bound` only inspected `amendments` (`AmendKeepPriority`), ignoring `o.pending_modify` and `self.modifications` (`prior_price` / `requested_price` / `requested_quantity`), rejecting valid venue fills with `Error::Conflict` and freezing `Account`.
- **Fix**: Extended `known_order_quantity` (`crates/tickwire-account/src/amendments.rs:285-315`) and `authorized_quantity_bound` / `authorized_price_bound` (`crates/tickwire-account/src/events.rs:627-685`) to include in-flight `PendingModify` and non-aborted/non-rejected `ModificationRecord`s (recording `prior_price` on `ModificationRecord` for deterministic WAL replay).
- **Regression test**: `terminal_order_supersedes_pending_modify_releases_delta_hold_evicts_and_tolerates_late_modify_events` in `crates/tickwire-account/tests/stp_modify_projected_fill.rs`.

## Bug 4: `PerOrderQueryDriver` queried stale `orderId` instead of `new_client_id` on Spot `cancelReplace` `AmendAmbiguous`, and private WS rejected rotated replacement `orderId`

- **Scenario**:
  1. A Spot `Modify` (`cancelReplace` rotating `orig_cid -> new_cid` and creating a brand-new `orderId = 9702` while canceling `9701`) suffers a post-byte transport drop (`DoubtReason::AmendAmbiguous`).
  2. Separately, Private WS delivers replacement leg `executionReport(NEW, c = new_cid, i = 9704)` while `identity.venue_order_id` still holds `Some("9702")`.
- **Pre-fix behavior**: `PerOrderQueryDriver` queried `orderId = 9701` (always `CANCELED` after a succeeded `cancelReplace`!), misclassifying the live replacement order `9702` as `ModifyRejected(OrderNotOpen)` and stranding an unhedged live order on the exchange; meanwhile `PrivateEvent::bind_order` rejected `old != native` (`"9702" != "9704"`), quarantining the WS frame and freezing `Account`.
- **Fix**: Updated `PerOrderQueryDriver` (`crates/tickwire-binance-account/src/query_desk_driver.rs:420-690`) to probe `origClientOrderId = new_client_id` first on `Market::Spot` and dispatch the fallback `orderId` query immediately in the same `poll()` call via `spot_fallback_tickets` on `-2013`, and added `prepare_private_bind_identity` (`crates/tickwire-binance-account/src/modify.rs:998-1031`, `crates/tickwire-binance-account/src/runtime.rs:615-645`) to bind authorized Spot replacement legs and historical native-ID aliases cleanly across live WS and WAL replay.
- **Regression test**: `spot_cancel_replace_amend_ambiguous_query_driver_new_cid_probe_fallback_and_out_of_order_ws_replay` in `crates/tickwire-binance-account/tests/modify_projected_fill_stp.rs`.

## Bug 5: Local `CrossesBook` / `Modify` refusals burned `VenueRateLimiter` tokens in `RestSession` and exhausted `AccountRunner` 24-hour `Origins` slots

- **Scenario**:
  1. Strategy submits `NewOrder` or `Modify` that `Account::admit` refuses locally (`Error::CrossesBook`, `TooManyPendingActions`, `NoChange`, etc.).
- **Pre-fix behavior**: `RestSession::submit_pooled_inner` called `VenueRateLimiter::allow_command` unconditionally even when `gateway.submit_on_slot` returned `Status::Aborted` (zero wire bytes), burning exchange `ORDER` and `IP` quota on local STP rejections; `AccountRunner::submit` called `origins.register` *before* admission and only retired the intent into the 24-hour cooldown ring on failure, permanently freezing the runner with `RunnerError::OriginCapacity` after `1,024` local STP refusals.
- **Fix**: Gated `allow_command` on `status != Status::Aborted` in `RestSession::submit_pooled_inner` (`crates/tickwire-spot-execution/src/session.rs:760-775`) and added `Origins::precheck_register` + pre-registration `Account::admit` precheck in `AccountRunner::submit` (`crates/tickwire-spot-runner/src/origins.rs:115-170`, `crates/tickwire-spot-runner/src/lib.rs:285-330`).
- **Regression test**: `stp_crosses_book_and_modify_refusals_do_not_freeze_runner_or_burn_rate_limiter_tokens` in `crates/tickwire-spot-runner/tests/modify_projected_fill_stp.rs`.
