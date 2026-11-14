# Counterexamples and Rust fixes

Adversarial cross-layer inspection and code-faithful modeling of `L0_PoolRouter` across `tickwire-transport`, `tickwire-account-gateway`, `tickwire-binance-account`, `tickwire-spot-execution`, and `tickwire-spot-runner` uncovered four real production bugs in pooled reconnect fencing, `Status::Aborted` inflight/read ordering, multiplexed WebSocket EOF draining, and runner `Origins`/`inflight` cleanup. All four were reproduced with failing Red tests, fixed in Rust, and verified to exhaustion under TLC.

## Bug 1: Post-flush EOF + ready `DialBaton` reconnect clobbered `retained_receive` and aliased stale `SlotPending` (`NoCrossEpochResponseAliasing`)

- **Scenario**:
  1. `submit_on_slot` queues an HTTP order on `conn_id = 0` at `slot_epoch = E1`; `poll_slot` flushes all request bytes (`SendState::Flushed`).
  2. The venue sends a `200 OK` JSON response and immediately closes the TCP/TLS connection (`Connection: close` / EOF). `drop_broken_slot_isolated` captures the unread HTTP response into `slot_io[0].retained_receive` and disconnects slot `0`.
  3. An off-hot-path `DialWorker` immediately publishes `DialState::Ready` onto `slot 0`'s `DialBaton`, and `poll_pool_dials` runs at the top of `RestOwner::poll` / `RestSession::drive_pool` (or via `PerOrderQueryDriver::step`) BEFORE `poll_pooled_rest` / `drive_pool` drains `retained_receive` or finalizes `SlotPending` (`E1`).
- **Pre-fix behavior**: `AccountGateway::poll_pool_dials` harvested the ready `DialBaton` onto `conn_id = 0` unconditionally, incrementing `route.epoch` to `E2` and **clearing `slot_io[0].retained_receive.clear()`**, destroying the unread `E1` `200 OK` reply and leaving `SlotPending` (`E1`) attached while `conn_id = 0` was live at `E2` (`NoCrossEpochResponseAliasing` violated).
- **Fix**: Added `OrderConnectionPool::poll_dials_filtered` (`crates/tickwire-transport/src/order_router.rs:634-678`) and `AccountGateway::poll_pool_dials_filtered` (`crates/tickwire-account-gateway/src/pool.rs:292-329`) gating installation on `pending_send.is_none() && retained_receive.is_empty()`, paired with owner-level `slot.pending.is_none()` guards, cross-epoch `finalize_stale_pool_pending` fences, and post-drain `poll_pool_dials` calls in `RestOwner` (`crates/tickwire-binance-account/src/rest.rs:235-270`, `842-855`, `1043-1092`) and `RestSession` (`crates/tickwire-spot-execution/src/session.rs:263-289`, `957-991`, `1262`).
- **Regression tests**: `poll_pool_dials_preserves_unread_eof_retained_receive_until_drained` in `crates/tickwire-account-gateway/tests/multi_slot_pool.rs`, `pooled_rest_eof_ready_baton_and_abort_inflight_and_local_reject_quota` in `crates/tickwire-binance-account/tests/multi_connection_router.rs`, and `post_flush_eof_baton_guard_and_runner_inflight_origin_consistency` in `crates/tickwire-spot-runner/tests/multi_connection_router_quota.rs`.

## Bug 2: Duplicate `release_slot_inflight` and pre-reap socket read on `Status::Aborted` (`InflightAccountingExact` & `AbortedNeverMarkedInDoubt`)

- **Scenario**:
  1. On a `Shared` slot (`MaxShared >= 2`), request `tx_1` flushes (`slot_inflight = 1`), and request `tx_2` is queued (`slot_inflight = 2`).
  2. `tx_2` expires before first byte (`SendState::NoBytes` in `AccountGateway::apply_slot_report`), which releases `tx_2`'s router inflight (`slot_inflight = 1`) and marks `tx_2` `Status::Aborted`.
  3. On the same poll, `tx_1`'s HTTP response arrives on the socket while `SlotPending` / `SpotPending` still points to `tx_2`.
- **Pre-fix behavior**: `RestOwner::poll_pooled_rest` and `RestSession::drive_pool` read socket bytes *before* checking `gateway.status(pending) == Some(Status::Aborted)`, consuming `tx_1`'s response under `tx_2`'s aborted pending slot, AND `RestOwner` called `gateway.release_slot_inflight(conn_id, slot_epoch)` a **second** time on `Status::Aborted`, underflowing `tx_1`'s live `slot_inflight` to `0` (`InflightAccountingExact` violated).
- **Fix**: Reaped `Status::Aborted` pending entries immediately after `poll_slot` (before `read_slot`) and removed the duplicate `release_slot_inflight` call on `Status::Aborted` in `RestOwner::poll_pooled_rest` (`crates/tickwire-binance-account/src/rest.rs:859-873`, `1020-1034`) and `RestSession::drive_pool` (`crates/tickwire-spot-execution/src/session.rs:290-306`, `1006-1023`).
- **Regression test**: `pooled_rest_eof_ready_baton_and_abort_inflight_and_local_reject_quota` in `crates/tickwire-binance-account/tests/multi_connection_router.rs`.

## Bug 3: `WsOwner` dropped unread pre-EOF frames under poll caps and `check_control_report` halted on isolated WS `Close` flush (`AmbiguousSentIsolatedInDoubt`)

- **Scenario**:
  1. `WsOwner` has multiple `Flushed` WS orders in `r.inflight` (`<= 16`). The venue sends 3 JSON reply frames and closes TCP (`EOF`), while `max_messages_per_poll = 1` (or `IoBudget.max_bytes`) caps decoding at 1 frame on the first poll.
  2. Separately, when a peer sends WS `Opcode::Close` under `per_order_doubt_isolation = true`, `AccountGateway` queues a reply `WebSocketControl::Close` frame and flushes it via `check_control_report`.
- **Pre-fix behavior**: Because `has_transport_connection()` flipped `false` upon reading EOF, `WsOwner::poll_ws` immediately isolated the remaining 2 orders as `Unknown` on the first poll even though their complete JSON replies were already buffered in `r.decoder` / `retained_receive`. Separately, `AccountGateway::check_control_report` unconditionally called `self.halt()` when a WS `Close` frame finished flushing even under `per_order_doubt_isolation = true`.
- **Fix**: Added `Runtime.draining_eof` in `WsOwner::poll_ws` (`crates/tickwire-binance-account/src/ws/owner.rs:653-685`) to finish draining buffered pre-EOF frames before isolating remaining unanswered `Flushed` entries, and updated `AccountGateway::check_control_report` (`crates/tickwire-account-gateway/src/websocket.rs:214-222`) to call `drop_broken_transport_isolated()` when `per_order_doubt_isolation && !account.frozen()`.
- **Regression test**: `ws_owner_drains_all_pre_eof_frames_under_message_cap_and_isolates_peer_close` in `crates/tickwire-binance-account/tests/multi_connection_router.rs`.

## Bug 6: `AccountRunner` & `RestSession` leaked `inflight` / `Origins` on direct gateway abort, post-register submit error, or byte-identical retry

- **Scenario**:
  1. `AccountRunner::submit` registers `Origins` (`origins.register`), then either (a) `session.submit` fails after registration (e.g., `SessionError::Busy`), (b) a caller aborts a queued command directly on `session.gateway_mut()`, or (c) `AccountRunner::submit` receives a byte-identical retry of an already-registered `IntentKey`.
- **Pre-fix behavior**: Post-registration `session.submit` errors left unretired `Origins` entries blocking future strategy reuse; direct gateway aborts left stale entries in `RestSession.pool_slots[c].pending` and `AccountRunner.inflight`; and byte-identical retries failed on duplicate `origins.register`.
- **Fix**: Added `RestSession::reap_aborted_pending` (`crates/tickwire-spot-execution/src/session.rs:290-306`, `486`, `711`) and `AccountRunner::{reap_aborted_inflight, active_inflight_len}`, post-error `retire_intent`, and same-digest `origins` check (`crates/tickwire-spot-runner/src/lib.rs:145-159`, `264-465`).
- **Regression test**: `post_flush_eof_baton_guard_and_runner_inflight_origin_consistency` in `crates/tickwire-spot-runner/tests/multi_connection_router_quota.rs`.
