# L1 Symbolic 64-Bit ORS QueryDesk Input Scope

## Source Mapping (`crates/tickwire-account`, `crates/tickwire-binance-account`, `crates/tickwire-spot-execution`)

- `OrsQueryDesk` 64-bit nanosecond deadlines (`next_due_mono_ns`), monotonic earlier-only `schedule_status_ask` merge, `mark_cancel_after`, and 8-attempt exponential `cancel_backoff_delay_ns` (`1s..=64s` saturating `u64` arithmetic):
  - `crates/tickwire-account/src/ors_query_desk.rs:13-47`
  - `crates/tickwire-account/src/ors_query_desk.rs:119-134`
  - `crates/tickwire-account/src/ors_query_desk.rs:165-386`
- `OrderManager` `request_cancel` coalescing, `on_socket_drop`, `on_query_inconclusive` (`max_query_count`), and `reconcile_query_snapshot` catch-up fill & `NotFound` settlement:
  - `crates/tickwire-account/src/order_manager.rs:1508-1536`
  - `crates/tickwire-account/src/order_manager.rs:2449-2656`
  - `crates/tickwire-account/src/uta_ledger.rs:206-275`
- Venue `StatusAsk` scheduling (`6000ms` delay) and REST/WS reconciliation:
  - `crates/tickwire-binance-account/src/session.rs:2219-2280`
  - `crates/tickwire-binance-account/src/session.rs:2495-2520`
  - `crates/tickwire-binance-account/src/session.rs:3010-3205`
  - `crates/tickwire-spot-execution/src/rest_bootstrap.rs:896-1215`
  - `crates/tickwire-spot-execution/src/session.rs:2141-2287`
  - `crates/tickwire-spot-execution/src/session.rs:3526-3543`

## Target Symbolic Invariants

1. `TypeOK`: Snowcat-typed 64-bit nanosecond timestamps (`0..2^64-1`) and lot quantities constructed without raw integer literals above `2^31 - 1`.
2. `MonotonicMergeNoPostponement`: Merging a `Cancel` or `SocketDrop` `StatusAsk` onto an existing entry never postpones `next_due_mono_ns` (`desk_next_due_ns <= prev_due_on_merge`), latches `cancel_after`, and never resets `query_attempts` or `cancel_attempts`.
3. `CancelBackoffBounded`: `0 <= desk_cancel_attempts <= 8`, `desk_cancel_exhausted => desk_cancel_attempts = 8`, backoff delays belong strictly to `{0, 1, 2, 4, 8, 16, 32, 64} * BaseBackoffNs`, and saturating `u64` deadlines never overflow.
4. `SnapshotFillsBeforeRemainderCancel`: Terminal snapshot reconciliation credits `snap_cum_applied` via `on_fill` strictly before releasing `last_canceled_lots` so `filled_lots + last_canceled_lots = orig_lots`.
5. `UncreatedNotFoundVsOpenNotFoundSound`: `TERM_REJECTED_NACK` occurs iff `!was_open_before_term /\ filled_lots = 0`, while previously `Open` or partially filled orders close as `TERM_CANCELED` preserving fills.
6. `ActiveLiveProtectedOnEviction`: Full-table `schedule_status_ask` never evicts an occupied `Live` peer entry (`~evicted_live_order`) and preserves desk mutual exclusion.
7. `MaxQueryCountContract`: `~OptInMaxCap` never emits `TERM_INTERNAL_CANCEL` for arbitrarily large `query_fail_count`; `OptInMaxCap` emits `TERM_INTERNAL_CANCEL` iff `query_fail_count >= cfg_max_query_limit`.
8. `SolvencyPreserved`: Exact symbolic 64-bit `wallet_free_lots + wallet_locked_lots + filled_lots = cfg_init_budget_lots` conservation.
