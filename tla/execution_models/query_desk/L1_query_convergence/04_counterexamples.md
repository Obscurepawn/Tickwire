# Counterexamples and Rust fixes

Code-faithful review and iterative TLC model checking of `L1_QueryConvergence` uncovered **eight real cross-layer bugs** across `tickwire-account`, `tickwire-account-gateway`, `tickwire-binance-account`, and `tickwire-spot-runner`. Every bug was reproduced with a failing Rust regression test, fixed at the single-writer boundary, and verified to exhaustion (`5,929,082` + `63,772` generated states, `0` errors).

## Bug B: Spot `PerOrderQueryDriver` routed zero-byte `Expired` / `NotBuilt` replies to `on_transport_failure` (`IssuedOnlyCountsSent`)

- **Violation**: `AsyncRestQuerier` emits `RestStatus::NotBuilt` (e.g. clock calibration fault before building HTTP bytes) and `RestStatus::Expired` (waiting deadline elapsed before send) with `query_count == 0` (zero wire bytes transmitted). `crates/tickwire-spot-runner/src/query_desk_driver.rs:335-344` matched only `Ok | HttpError` and fell through `_ => self.record_transport_failure(...)`, incrementing `issued_query_count` / `cancel_calls` and falsely triggering `FreezeAccount` when `max_query_count = 1`.
- **Fix**: Explicitly routed `RestStatus::Expired | RestStatus::NotBuilt` to `QueryDesk::on_not_sent(&inflight.ticket, now_mono_ns)` (`crates/tickwire-spot-runner/src/query_desk_driver.rs:335-344`), matching Binance `query_desk_driver.rs:545-554`.
- **Regression test**: `spot_query_driver_not_built_preserves_issued_budget_and_bybit_mid_chain_ws_resolution_skips_history` in `crates/tickwire-spot-runner/tests/per_order_query_desk.rs`.

## Bug C: `QueryDesk` wiped `cancel_after` / `pending_cancels` across non-terminal `Trades` transitions and skipped `max_query_count` on confirmed `NotFound` (`CancelAfterNeverLost` & `BoundedEscalationToFreeze`)

- **Violation**:
  1. When `on_status_snapshot` returned `FetchTradesBeforeClose` on a **non-terminal** partially filled order (`!snap.terminal && snap.cumulative_atoms > snap.local_filled_atoms`), `QueryDesk::on_status_snapshot` unconditionally wiped `ask.cancel_after = false` and `ask.pending_cancels = 0`.
  2. When an ambiguous `Cancel` arrived while `AskKind::Trades` was waiting (`in_flight == None`), `enqueue_ask` overwrote `ask.kind = AskKind::Cancel` (delaying trade catchup) instead of latching `ask.cancel_after = true` behind `Trades`.
  3. Neither `on_trades_applied(..., true)` nor `poll_due` (`!should_query` skip) re-armed `AskKind::Cancel` when `cancel_after` / `pending_cancels > 0` remained on a non-terminal order, permanently losing the cancel ask.
  4. `on_not_found(..., order_confirmed: true)` transitioned to `FetchTradesBeforeClose` without incrementing `issued_query_count` or checking `max_query_count`.
- **Fix**: Preserved `cancel_after` and `pending_cancels` across non-terminal `Trades` transitions via `known_terminal` (`crates/tickwire-account/src/query_desk.rs:124-136`, `282-330`, `382-403`, `794-828`, `883-907`), re-armed `AskKind::Cancel` in `on_trades_applied` and `poll_due`, and enforced `issued_query_count` / `max_query_count` in `on_not_found(..., order_confirmed: true)` (`crates/tickwire-account/src/query_desk.rs:730-759`).
- **Regression test**: `non_terminal_trades_catchup_preserves_and_rearms_pending_cancel_and_confirmed_not_found_counts_budget` in `crates/tickwire-account/tests/query_desk.rs`.

## Bug D: In-flight `Trades` loss (`trades_after`), missing Binance WS `FillCatchup` `QueryDesk` enqueue, and false freeze on out-of-order stale open REST snapshots (`TradesBeforeFeeClosure`)

- **Violation**:
  1. When `enqueue_ask(..., AskKind::Trades, ...)` ran while an `OrderStatus` request was already in flight (`ask.in_flight.is_some()`), `QueryDesk` left `ask.kind` unchanged with no `trades_after` flag; if the in-flight `OrderStatus` returned a stale snapshot (`cum = 0`) or failed, the `Trades` request was lost.
  2. `RestOwner::ingest_private` (`crates/tickwire-binance-account/src/rest.rs:518-575`) isolated `DoubtReason::FillCatchup` in `Account` on lagging WS `executionReport` events (`cum > filled`) without enqueuing `AskKind::Trades` in `QueryDesk` or removing terminal caught-up orders from `QueryDesk`.
  3. Both Binance and Spot `PerOrderQueryDriver` froze the account (`outcome.applied_events != 1` / `compare::order_status` regression) when an in-flight `OrderStatus` returned an older `NEW (cum = 0)` snapshot after a faster WS push had already advanced the order to `PARTIALLY_FILLED` or `CANCELED`.
- **Fix**: Added `trades_after: bool` to `StatusAsk` (`crates/tickwire-account/src/query_desk.rs:124`, `305-330`, `477-492`, `549-565`, `680-694`, `793-813`), wired `RestOwner::ingest_private` to enqueue `AskKind::Trades` on `FillCatchup` and call `remove_order` once terminal & fee-closed (`crates/tickwire-binance-account/src/rest.rs:518-575`), accepted `Applied::Duplicate` stale snapshots in Binance (`crates/tickwire-binance-account/src/query_desk_driver.rs:727-775`), and guarded Spot `handle_order_snapshot` against regressing terminal or ahead-cumulative WS state (`crates/tickwire-spot-runner/src/query_desk_driver.rs:682-732`).
- **Regression tests**: `in_flight_trades_enqueue_promotes_after_stale_snapshot_or_transport_failure` (`crates/tickwire-account/tests/query_desk.rs`) and `ws_ahead_fill_catchup_enqueues_trades_and_survives_stale_open_order_status_snapshot` (`crates/tickwire-binance-account/tests/per_order_query_desk.rs`).

## Bug E: Wire-slot aliasing and stale Bybit `/v5/order/history` chaining after concurrent WS `remove_order` (`AtMostOneAskAndOneSlotPerOrder` & `BybitTwoStepSoundness`)

- **Violation**: When a concurrent WS terminal push called `QueryDesk::remove_order(&key)`, `QueryDesk` immediately marked `slots[s]` free while `"tw-rest-query"` still owned `self.in_flight[s]`. Spot `PerOrderQueryDriver::dispatch_due` checked `QueryDesk::poll_due` without checking `self.in_flight[slot_idx].is_some()`, overwriting `self.in_flight[s]` for a second order and stranding/aliasing the active slot; additionally, `dispatch_bybit_history` chained `/v5/order/history` on an empty `/v5/order/realtime` reply even after WS had already removed the ticket from `QueryDesk`.
- **Fix**: Added `QueryDesk::is_ticket_in_flight` (`crates/tickwire-account/src/query_desk.rs:220-233`), guarded `dispatch_due` when `self.in_flight[slot_idx].is_some()` via `on_not_sent` (`crates/tickwire-spot-runner/src/query_desk_driver.rs:990-999`), and short-circuited `dispatch_bybit_history` when `!is_ticket_in_flight(ticket)` or when an open WS partial fill arrived mid-chain (`crates/tickwire-spot-runner/src/query_desk_driver.rs:517-552`).
- **Regression test**: `spot_query_driver_not_built_preserves_issued_budget_and_bybit_mid_chain_ws_resolution_skips_history` in `crates/tickwire-spot-runner/tests/per_order_query_desk.rs`.

## Bug F: `DoubtReason::CancelAmbiguous` never cleared on `CancelRejectedStillOpen { remaining_pending_cancels: 0 }`

- **Violation**: When an ambiguous `Cancel` resolved via `OrderStatus` proving the order remained open on the book with zero remaining pending cancels (`QueryResolution::CancelRejectedStillOpen { remaining_pending_cancels: 0 }`), `QueryDesk` removed the ask, but neither Binance nor Spot driver cleared `Order.in_doubt = Some(DoubtReason::CancelAmbiguous)` in `Account`.
- **Fix**: Added `Account::clear_order_in_doubt` (`crates/tickwire-account/src/lib.rs:978-987`) and `AccountGateway::clear_order_in_doubt` (`crates/tickwire-account-gateway/src/lib.rs:1719-1723`), invoked by both drivers upon `CancelRejectedStillOpen { remaining_pending_cancels: 0 }` and non-terminal `Trades` `Resolved` (`crates/tickwire-binance-account/src/query_desk_driver.rs:808-819`, `1002-1010`; `crates/tickwire-spot-runner/src/query_desk_driver.rs:766-778`, `948-959`).
- **Regression test**: `ambiguous_cancel_still_open_clears_cancel_ambiguous_doubt_cleanly` in `crates/tickwire-binance-account/tests/per_order_query_desk.rs`.

## Bug G: Lagging WS partial-fill `orders` push clobbered `CancelAmbiguous` and triggered redundant `OrderTrades` queries

- **Violation**: In Spot `apply_private_cash` (`crates/tickwire-spot-runner/src/reconcile/ingest.rs:236-305`), a non-terminal `PartiallyFilled` WS order push unconditionally overwrote `DoubtReason::CancelAmbiguous` with `FillCatchup` (losing `CancelAmbiguous` once the WS fill arrived) and called `QueryDesk::remove_order` on open WS order pushes even when `has_pending_cancel` was true. Simultaneously, `poll_due_query_ticket` (`crates/tickwire-spot-runner/src/lib.rs:553-559`) and Binance `should_query_order` (`crates/tickwire-binance-account/src/query_desk_driver.rs:1047-1050`) checked `order_in_doubt(order).is_some()` on `AskKind::Trades`, dispatching redundant REST trade queries for open `CancelAmbiguous` orders whose fills had already caught up via WS.
- **Fix**: Preserved `DoubtReason::CancelAmbiguous` across non-terminal WS/REST `FillCatchup` transitions, guarded open WS `remove_order` with `!has_pending_cancel`, and tightened the `AskKind::Trades` `should_query` predicate to `order_in_doubt(order) == Some(DoubtReason::FillCatchup)` (`crates/tickwire-spot-runner/src/reconcile/ingest.rs:247-278`, `crates/tickwire-spot-runner/src/lib.rs:553-559`, `crates/tickwire-binance-account/src/query_desk_driver.rs:1047-1050`).
- **Regression test**: `ws_partial_fill_catchup_preserves_cancel_ambiguous_and_skips_redundant_trades_query` in `crates/tickwire-spot-runner/tests/per_order_query_desk.rs`.

## Bug H (TLC 6-state counterexample): `on_not_sent` and pre-grace `on_not_found` left `ask.kind = Place` when `cancel_after = true` (`CancelAfterNeverLost`)

- **TLC counterexample trace (`L1_QueryConvergence.cfg`, depth 6)**:
  1. `State 1`: `Init`.
  2. `State 2`: `IsolateAmbiguousPlace(1, "OpenFlat")` (`ask_kind[1] = "Place"`, `due_at[1] = 1`).
  3. `State 3`: `AdvanceClock` (`clock = 1`).
  4. `State 4`: `PollAndDispatchDue(1, 1)` (`ask_slot[1] = 1`, `ask_kind[1] = "Place"` in flight).
  5. `State 5`: `EnqueueAmbiguousCancel(1)` while `Place` is in flight (`cancel_requested[1] = TRUE`, `cancel_after[1] = TRUE`, `pending_cancels[1] = 1`).
  6. `State 6`: `QuerierReplyNotBuiltOrExpired(1)` -> `QueryDesk::on_not_sent`: cleared `ask_slot[1] = 0`, left `ask_kind[1] = "Place"` while `cancel_after[1] = TRUE` (violating `CancelAfterNeverLost`: waiting ask after in-flight return failed to promote `cancel_after` to `AskKind::Cancel`, and a subsequent `on_transport_failure` on `Place` or `NotFound` could mis-handle cancel backoff). The identical missing promotion existed in `QueryDesk::on_not_found` on the pre-grace `!order_confirmed` retry branch.
- **Fix**: Promoted `trades_after` -> `AskKind::Trades` and `cancel_after` -> `AskKind::Cancel` in both `QueryDesk::on_not_sent` (`crates/tickwire-account/src/query_desk.rs:477-512`) and pre-grace `QueryDesk::on_not_found` (`crates/tickwire-account/src/query_desk.rs:680-715`).
- **Regression test**: `cancel_joining_in_flight_place_promotes_to_cancel_on_not_sent_and_pre_grace_not_found` in `crates/tickwire-account/tests/query_desk.rs`.

## Bug I (TLC 7-state trace inspection): Spot `handle_order_not_found` omitted `DoubtReason::FillCatchup` on confirmed `FetchTradesBeforeClose` (`TradesBeforeFeeClosure`)

- **TLC trace scenario (`L1_QueryConvergence.cfg`, depth 7)**:
  1. Order `1` is `confirmed = true`, `!terminal` locally (`AdmitConfirmedOrder(1, "CanceledWithFill")`), enters `CancelAmbiguous` (`EnqueueAmbiguousCancel(1)`), and dispatches Bybit `OrderStatusPrimary` (`PollAndDispatchDue`) -> `BybitOrderStatusHistory` (`BybitRealtimeEmptyChainHistory`).
  2. Both Bybit `/v5/order/realtime` and `/v5/order/history` return empty `list: []` (`StatusReplyNotFound(1)` with `confirmed = true`), emitting `QueryResolution::FetchTradesBeforeClose` and transitioning `QueryDesk` to `AskKind::Trades`.
- **Pre-fix Spot behavior**: Unlike Binance (`crates/tickwire-binance-account/src/query_desk_driver.rs:685-691`), Spot `PerOrderQueryDriver::handle_order_not_found` (`crates/tickwire-spot-runner/src/query_desk_driver.rs:634-646`) had `_ => {}` with no `FetchTradesBeforeClose` arm, leaving `in_doubt == CancelAmbiguous` (`expected_fill_atoms == 0`, `terminal == false`). On the very next `poll_due_query_ticket`, `should_query(order, AskKind::Trades)` evaluated `false` and **dropped `AskKind::Trades` without ever querying `/v5/execution/list` or `/api/v5/trade/fills`**! Furthermore, in `handle_order_trades`, if `/v5/execution/list` returned fills that had already been deduplicated via WS, `DoubtReason::FillCatchup` was not cleared.
- **Fix**: Added the `QueryResolution::FetchTradesBeforeClose` arm in `crates/tickwire-spot-runner/src/query_desk_driver.rs:645-654` to isolate `DoubtReason::FillCatchup`, and cleared `DoubtReason::FillCatchup` in `handle_order_trades` (`crates/tickwire-spot-runner/src/query_desk_driver.rs:895-907`) once `filled_quantity >= min_expected` and `(min_expected > 0 || !fills.is_empty())`.
- **Regression test**: `confirmed_order_not_found_fetches_trades_before_close_and_clears_fill_catchup` in `crates/tickwire-spot-runner/tests/per_order_query_desk.rs`.
