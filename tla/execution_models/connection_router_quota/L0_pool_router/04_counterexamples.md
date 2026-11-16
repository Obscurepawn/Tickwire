# Counterexamples and Rust fixes

Adversarial cross-layer inspection and code-faithful modeling of `L0_PoolRouter` across `tickwire-transport`, `tickwire-account-gateway`, `tickwire-binance-account`, and `tickwire-spot-execution` uncovered real production edge cases in pooled reconnect fencing, `Status::Aborted` inflight/read ordering, and multiplexed WebSocket EOF draining. All were reproduced with failing Red tests, fixed in Rust, and verified to exhaustion under TLC.

## Bug 1: Post-flush EOF + ready `DialBaton` reconnect clobbered `retained_receive` and aliased stale pending slot (`NoCrossEpochResponseAliasing`)

- **Scenario**:
  1. `submit_on_slot` queues an HTTP order on `conn_id = 0` at `slot_epoch = E1`; `poll_slot` flushes all request bytes (`SendState::Flushed`).
  2. The venue sends a `200 OK` JSON response and immediately closes the TCP/TLS connection (`Connection: close` / EOF). `drop_broken_slot_isolated` captures the unread HTTP response into `slot_io[0].retained_receive` and disconnects slot `0`.
  3. An off-hot-path `DialWorker` immediately publishes `DialState::Ready` onto `slot 0`'s `DialBaton` BEFORE `retained_receive` or pending slot state at `E1` finishes draining.
- **Fix**: `OrderConnectionPool::poll_dials_filtered` (`crates/tickwire-transport/src/order_router.rs:945-990`) and `AccountGateway::poll_pool_dials_filtered` (`crates/tickwire-account-gateway/src/pool.rs:294-330`) gate installation on `pending_send.is_none() && retained_receive.is_empty()`, paired with session-level drain/close fences (`crates/tickwire-binance-account/src/session.rs:857-955`, `crates/tickwire-spot-execution/src/session.rs:417-470`).
- **Regression test**: `poll_pool_dials_preserves_unread_eof_retained_receive_until_drained` in `crates/tickwire-account-gateway/tests/multi_slot_pool.rs`.

## Bug 2: Duplicate `release_slot_inflight` and pre-reap socket read on `Status::Aborted` (`InflightAccountingExact` & `AbortedNeverMarkedInDoubt`)

- **Scenario**:
  1. On a `Shared` slot (`MaxShared >= 2`), request `tx_1` flushes (`slot_inflight = 1`), and request `tx_2` is queued (`slot_inflight = 2`).
  2. `tx_2` expires before first byte (`SendState::NoBytes` in `AccountGateway::apply_slot_report`), which releases `tx_2`'s router inflight (`slot_inflight = 1`) and marks `tx_2` `Status::Aborted`.
  3. On the same poll, `tx_1`'s response arrives on the socket while a slot still references `tx_2`.
- **Fix**: `AccountGateway::apply_slot_report` (`crates/tickwire-account-gateway/src/pool.rs:715-735`) is the sole owner of `NoBytes` inflight release, and session slots reap aborted entries before socket reads without double-releasing inflight (`crates/tickwire-spot-execution/src/session.rs:961-980`).

## Bug 3: Pre-EOF WebSocket frames dropped under poll caps and isolated WS `Close` flush (`AmbiguousSentIsolatedInDoubt`)

- **Scenario**:
  1. Multiple `Flushed` WS orders are in flight (`<= 16`) when the venue sends buffered JSON reply frames followed immediately by TCP `EOF` or WS `Opcode::Close`.
- **Fix**: Drain buffered pre-EOF frames before isolating remaining unanswered `Flushed` entries into `OrsQueryDesk`, and isolate broken WS control flushes via `AccountGateway::check_control_report` (`crates/tickwire-account-gateway/src/websocket.rs:209-225`).
