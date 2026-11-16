# L0 Per-Account ORS QueryDesk Input Scope

## Source Mapping (`crates/tickwire-account`, `crates/tickwire-binance-account`, `crates/tickwire-binance-execution`, `crates/tickwire-spot-execution`)

- Preallocated per-account `OrsQueryDesk` (`schedule_status_ask`, `mark_cancel_after`, `schedule_cancel_backoff`, `poll_due`, `complete_order`, `cleanup_non_live`, `is_ambiguous_venue_outcome`, `cancel_backoff_delay_ns`):
  - `crates/tickwire-account/src/ors_query_desk.rs:13-425`
- `OrderManager` per-order ambiguity guards, `request_cancel` `cancel_after` coalescing, `on_socket_drop`, `on_query_inconclusive` (`max_query_count`), and `reconcile_query_snapshot` (catch-up fills strictly before remainder cancel + `NotFound` disambiguation):
  - `crates/tickwire-account/src/order_manager.rs:1508-1536`
  - `crates/tickwire-account/src/order_manager.rs:2150-2249`
  - `crates/tickwire-account/src/order_manager.rs:2449-2656`
  - `crates/tickwire-account/src/dop_state.rs:236-267`
  - `crates/tickwire-account/src/uta_ledger.rs:206-275`
- Venue session scheduling and `StatusAsk` snapshot decoding (`-1007` / `-1000` / `MKT_DELAYED`, `-2013` / Bybit `110001|170145|170213` / OKX `51603|51400`, `Cancel` answered `Open` `pending_cxl` decrement, `DispatchCancel` coalesced cancel):
  - `crates/tickwire-binance-account/src/session.rs:2219-2280`
  - `crates/tickwire-binance-account/src/session.rs:2495-2520`
  - `crates/tickwire-binance-account/src/session.rs:2948-3279`
  - `crates/tickwire-binance-execution/src/lib.rs:36-65`
  - `crates/tickwire-spot-execution/src/rest_bootstrap.rs:896-1215`
  - `crates/tickwire-spot-execution/src/session.rs:2141-2287`
  - `crates/tickwire-spot-execution/src/session.rs:3012-3074`
  - `crates/tickwire-spot-execution/src/session.rs:3526-3543`

## Target Safety Invariants

1. `TypeOK`: All order states, `OrsQueryDesk` entries, retry counters, and wallet balances stay within finite bounds.
2. `ActiveLiveProtectedOnEviction`: Full-table cleanup in `schedule_status_ask` evicts **only** non-`Live` (`!is_slot_live`) entries and refuses with `TooManyPendingActions` when all occupied entries are `Live`, never dropping an active in-doubt order.
3. `CancelBackoffBounded`: Exponential `Cancel` retries stay bounded and set `cancel_exhausted` at the cap.
4. `SnapshotFillsBeforeRemainderCancel`: Granular `trades` and catch-up `cum_filled_lots` are applied via `on_fill` strictly before `on_canceled` settles the remaining unfilled quantity (`filled + canceled == OrderQty`).
5. `UncreatedNotFoundVsOpenNotFoundSound`: `NotFound` (`-2013` / `110001` / `51603`) emits `Rejected(NACK)` only when `PendingNew` with zero fills, and closes as `Canceled` preserving fills when previously `Open` or partially filled.
6. `MaxQueryCountContract`: `MaxQueryCount = 0` (`None` / `Some(0)`) never emits `INTERNAL_CANCEL`; opt-in `MaxQueryCount > 0` emits `INTERNAL_CANCEL` only once `query_fail_count >= MaxQueryCount`.
7. `SolvencyPreserved`: Multi-order wallet `free + locked + filled` conservation holds while sibling orders submit, ack, fill, and retire independently of a wedged order in `OrsQueryDesk`.
