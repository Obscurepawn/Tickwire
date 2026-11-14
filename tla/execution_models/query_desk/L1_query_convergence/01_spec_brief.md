# Specification brief: L1 `QueryDesk`, `PerOrderQueryDriver`, and concurrent WS convergence

## State ownership and scope

`L1_QueryConvergence` (`02_models/L1_QueryConvergence.tla`) models the exact state-machine composition of:

1. **`QueryDesk` (`crates/tickwire-account/src/query_desk.rs:111-921`)**:
   - Preallocated per-order `StatusAsk` map (`kind`, `place_unacked`, `pending_cancels`, `cancel_calls`, `cancel_after`, `trades_after`, `known_terminal`, `issued_query_count`, `first_doubt_mono_ns`, `due_mono_ns`, `in_flight`) and `SlotState` pool (`(slot, generation)` tickets).
   - Coalescing across waiting and in-flight `Place`, `Cancel`, and `Trades` asks (`enqueue_ask`), `poll_due` predicate skipping and `Cancel` re-arming, `on_not_sent` vs. `on_transport_failure` accounting, `on_not_found` grace & confirmed-order `FetchTradesBeforeClose`, `on_status_snapshot`, `on_trades_applied`, and `remove_order` / `is_ticket_in_flight`.
2. **`PerOrderQueryDriver` + `AsyncRestQuerier` slots (`crates/tickwire-binance-account/src/query_desk_driver.rs:335-1053`, `crates/tickwire-spot-runner/src/query_desk_driver.rs:295-1074`)**:
   - Driver `in_flight` slot occupancy check (`~wire_active[s]`) preventing slot aliasing when concurrent WS terminal events call `remove_order` while an `AsyncRestQuerier` task is still executing on the wire.
   - Bybit two-step `/v5/order/realtime` (`OrderStatusPrimary`) -> `/v5/order/history` (`BybitOrderStatusHistory`) status chain, including mid-chain WS resolution (`DiscardStaleWireReply`) and mid-chain open partial-fill short-circuit to `Trades`.
   - Zero-byte local/worker refusals (`DispatchNotSentLocal`, `QuerierReplyNotBuiltOrExpired`), counted-write transport/`5xx` retries (`QuerierReplyTransportFail`), Category B account hazards (`429`/`418`/`401`, `QuerierReplyCategoryBHazard`), out-of-order stale open REST snapshots (`StatusReplySnapshot(..., stale_zero = TRUE)`), and per-order trade catchup (`TradesReplyApply`).
3. **Concurrent private WebSocket `orders` & `fills` (`crates/tickwire-binance-account/src/rest.rs:518-575`, `crates/tickwire-spot-runner/src/reconcile/ingest.rs:202-310`)**:
   - Lagging `orders` pushes (`cum > local_filled`, `WsLaggingOrderPush`), concurrent flat terminal pushes (`WsFlatTerminalPush`), and `trade_id`-deduplicated `fills` pushes (`WsTradeFillPush`).

## Finite configurations

| Configuration | Constants | Purpose |
| --- | --- | --- |
| [`L1_QueryConvergence.cfg`](02_models/L1_QueryConvergence.cfg) | `Orders = {1, 2}, VenueModes = {"Absent", "OpenFlat", "CanceledWithFill"}, MaxSlots = 1, VenueIsBybit = TRUE, StatusDelay = 1, GraceTicks = 2, MaxCancelAsks = 2, MaxQueryCount = 2, MaxClock = 2, MaxDispatches = 3` | Two concurrent orders competing for 1 REST slot under Bybit two-step `realtime -> history` chaining, mid-flight WS slot reclamation, stale snapshots, and `MaxQueryCount`/hazard escalation. |
| [`L1_QueryConvergence_coalesce.cfg`](02_models/L1_QueryConvergence_coalesce.cfg) | `Orders = {1}, VenueModes = {"Absent", "OpenFlat", "OpenPartFill", "CanceledWithFill"}, MaxSlots = 1, VenueIsBybit = FALSE, StatusDelay = 1, GraceTicks = 2, MaxCancelAsks = 2, MaxQueryCount = 3, MaxClock = 4, MaxDispatches = 4` | Deep single-order coalescing across `OpenPartFill` non-terminal `Trades <-> Cancel` roundtrips, `cancel_after` + `trades_after` promotion on `on_not_sent`/`on_transport_failure`/pre-grace `on_not_found`, and `MaxCancelAsks` give-up (`VenueIsBybit = FALSE` Binance/OKX mode). |

## Invariants checked

- **`TypeOK`**: Every desk, driver, wire-slot, and order variable stays within its finite bounds.
- **`AtMostOneAskAndOneSlotPerOrder`**: At most 1 `StatusAsk` and at most 1 in-flight desk/wire slot per `OrderKey`; waiting asks hold `0` slots; `dispatch_due` never aliases an active wire slot after mid-flight `remove_order`.
- **`IssuedOnlyCountsSent`**: `on_not_sent` (ring full / quota refusal / `NotBuilt` / `Expired`) never increments `issued_query_count` or `cancel_calls` and never triggers `FreezeAccount`.
- **`TradesBeforeFeeClosure`**: `FeesReconciled` never fires while `local_filled < venue_cumulative`, confirmed orders returning `NotFound` never close as `uncreated` (transitioning to `FillCatchup` + `AskKind::Trades` instead), and duplicate WS + REST trade fills apply at most once (`trade_id` deduplication).
- **`CancelAfterNeverLost`**: `known_terminal` is never latched on an open venue order, and any ambiguous `Cancel` joining a waiting or in-flight ask (`Place`, `Cancel`, or non-terminal `Trades`) preserves `ask_kind = "Cancel" \/ cancel_after = TRUE` and re-arms `AskKind::Cancel` whenever an in-flight reply leaves the order open.
- **`BybitTwoStepSoundness`**: When `VenueIsBybit = TRUE`, `uncreated = TRUE` never fires without both an empty `/v5/order/realtime` response and a subsequent empty `/v5/order/history` response at or after `first_doubt + GraceTicks` on an unconfirmed order.
- **`BoundedEscalationToFreeze`**: Reaching `MaxQueryCount > 0` on counted non-definitive queries or observing a Category B hazard (`429`/`418`/`401`) deterministically sets `frozen = TRUE`.
