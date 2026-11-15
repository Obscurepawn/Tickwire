# Binance Execution Adapter and First-Byte Gate

`tickwire-binance-execution` bridges an account gateway's durable admission pipeline to Binance REST and WebSocket order-entry transports for a single `(owner_id, risk_domain_id, account, product, environment, ClockDomain)` `CommandBinding`. Built in safe Rust (`#![forbid(unsafe_code)]`), `SharedSessionState<C, B>` wraps a single-threaded `Rc<RefCell<State<C, B>>>`—preventing cross-thread sharing and rejecting reentrant borrows with `Error::Busy` rather than panicking—so `RestEncoder` and `GateAuthority` share exact original-signature state without duplicating clock or quota owners.

`RestEncoder` owns wire serialization and signature retention across retries, while `GateAuthority` wraps an inner recovery-closure `Authority` (`verify_handover`, `prepare`, `verify`, `commit_state`, `current_gate`) to enforce calibrated signature validity, lease freshness, WebSocket binding readiness, and request-quota consumption immediately before the first application byte enters the transport.

## State Model and Bounds

Construction (`SharedSessionState::new`) verifies nonzero `owner_id`, `risk_domain_id`, and `account`, confirms matching `ClockDomain` and `Source` across the `SessionClock` and `tickwire-binance-quota::Endpoint`, and enforces explicit configuration bounds:

| Parameter | Permitted Range | Enforcement Behavior |
| --- | --- | --- |
| `recv_window_micros` | `1..=60,000,000` us (`<= 60 s`) | Multiplied by `1,000` into `window_ns` with overflow checking. |
| `max_http_bytes` | `1..=16 MiB` (`16 * 1024 * 1024`) | Bounds serialized HTTP request or masked WebSocket text frame size. |
| `max_signed_commands` | `1..=4,096` entries | Bounds active within-window `signed` (`BTreeMap<Hash, SignedRequest>`) and WebSocket `plans`. |
| `retention_window_ns` | `Option<u64>` (default `Some(86,400,000,000,000)` ns / `24 h`) | Configurable via `SharedSessionState::set_retention_window_ns`; bounds lazy eviction of consumed, expired, or superseded entries before rejecting with `Error::Capacity`. |
| `warmup_interval_ns` | `u64` (default `0` = disabled) | Configurable via `SharedSessionState::with_warmup_interval_ns` / `set_warmup_interval_ns` (seeding `record_activity` from `sample_mono_ns()` when enabled from `last_activity_ns == 0 && last_warm_ns == 0`); gates side-effect-free `maybe_warm_hot_path(now_ns)` dry-run execution strictly to idle windows `>= warmup_interval_ns`. |
| `credential_reference` | Nonzero `generation`, `account`, `1..=256` byte `api_key` | Domain-prefixed SHA-256 (`tickwire.ws.credential-reference.v1`) omitting signing secrets. |

When `encode`, `prepare_websocket`, or `prepare` runs for a `CanonicalCommand` whose SHA-256 `digest` is not yet cached and either `signed` or `plans` has reached `max_signed_commands`, `State` first samples the monotonic clock and lazily evicts entries that are no longer in-flight and have aged past `retention_window_ns` (consumed via `finish_gate` at `consumed_mono_ns` with `now >= consumed_mono_ns + window_ns`, expired past `deadline_mono_ns + window_ns`, or bound to a revoked `!is_current()` WebSocket plan past `created_mono_ns + window_ns`). Only if `max_signed_commands` active/within-window entries remain does it reject with `Error::Capacity`. Once admitted, `encode` samples `SessionClock` for an active `SignedTime` (`Error::MissingSignature` if uncalibrated), verifies `now < command.deadline`, prepares the signed REST or WebSocket payload via [binance-protocol.md](binance-protocol.md), frames the HTTP request or masked WebSocket text frame, and caches `(SignedTime, window_ns, created_mono_ns, deadline_mono_ns, consumed_mono_ns, authority, bytes, Option<SignedWebSocket>)` keyed by `command.digest()`. Any subsequent `encode` retry for the identical digest returns those exact immutable wire bytes and original `SignedTime` (provided any associated `WsRequestPlan` remains current), guaranteeing a retried send never re-signs at a new timestamp.

For WebSocket order entry ([binance-ws-trading.md](binance-ws-trading.md)), `SharedSessionState` supports preparing, encoding, and installing up to `MAX_SHARED_INFLIGHT` (`16`) concurrent `WsRequestPlan` and `DurableWsBinding` entries on the same active `SessionIdentity` (`max_signed_commands <= 4,096`), including `CommandBody::Modify` (`"order.modify"` on USD-M and `"order.cancelReplace"` on Spot). If local `Account::admit` refuses a command before `install_websocket_binding` commits (`CrossesBook`, `NoChange`, `TooManyPendingActions`, `OrderNotOpen`, `InvalidPrice`, `InvalidQuantity`, `InsufficientAsset`, `InsufficientCapital`, `Limit`, or `ManagementDenied`), `SharedSessionState::discard_uncommitted_websocket` removes the uncommitted `WsRequestPlan` and cached signature without touching `Endpoint` command quota or halting the session. For each admitted command digest, `GateAuthority::commit_state` returns `CommitState::Pending` until `install_websocket_binding` attaches that command's live `DurableWsBinding` whose plan, `SignedWire`, frame length, SHA-256 frame hash, `account_sequence`, `record_digest`, and `PhysicalFrontier` match the durable account `AppendReceipt`. Callers running multi-IP pools may also attach an optional `tickwire-binance-quota::MultiIpCommandQuota` (`with_multi_ip_quota`, `set_multi_ip_quota`, `multi_ip_quota`) validated against the exact `CommandBinding`; `SessionState::feedback` propagates `418` account bans to `MultiIpCommandQuota::note_account_banned` alongside `Endpoint::freeze()`.

## Core Invariants

* **Immutable signature across retries**: Once a command is signed, its `SignedTime` and framed wire bytes are frozen in `signed[digest]`. Neither transport retries nor `DeliveryBudget` callbacks can substitute a newer timestamp into an already-prepared command.
* **Callbacks precede the final clock sample**: Inside `GateAuthority::current_gate`, both `DeliveryBudget::remaining_ns` (`prepare_gate`) and `inner.current_gate` (policy, owner isolation, lease, `writer_epoch`, `strategy_generation`, and `config_revision`) execute *before* `SessionClock::check_send_sample` takes the authoritative final clock reading (`finish_gate`). Even if the budget callback or policy read stalls and advances wall time past the signature window, lease, or command deadline, the final sample catches the elapsed delay and denies the write.
* **Quota consumption inside the verified gate**: `Endpoint::consume_command` runs only after signature validity, monotonic non-regression (`final >= before` and `final >= gate.now`), active lease (`now < gate.lease_until`), and command deadline (`now < r.deadline`) all succeed, updating `gate.now` to that final monotonic timestamp before any plaintext byte reaches TLS.
* **Side-effect-free idle-window hot-path warming**: `SharedSessionState::maybe_warm_hot_path` (and `maybe_warm_hot_path_with_account`) resets its `WarmupTracker` on every real `encode`, `prepare_websocket`, `current_gate`, or explicit `record_activity(now_ns)` call and executes only when `warmup_interval_ns > 0` and idle time reaches `>= warmup_interval_ns`. Each dry-run iteration decodes a sample Binance BBO quote into stack integers, checks read-only `&Account` / `StpBook::self_cross` and exact `Fixed` notional/fee arithmetic, signs and frames a synthetic HMAC-SHA256 HTTP/1.1 `POST /api/v3/order` or masked RFC 6455 WebSocket `"order.place"` request into `[u8; 2048]` scratch memory, and completes an offline `TlsLoopbackWarmer` roundtrip plus `200 OK` acknowledgement decode without mutating `signed`, `plans`, `SessionClock`, `Endpoint` permits, `MultiIpCommandQuota`, or any live TLS session.

## Transitions and Failure Semantics

At the first application write (`GateAuthority::current_gate`), any failure in `prepare_gate`, `inner.current_gate`, or `finish_gate` maps to `AuthorityError::Denied` and aborts transmission with zero application bytes accepted:

1. **Missing or stale encoding**: Command absent from `signed`, mutated `SignedTime`/`window_ns`, or a WebSocket command lacking a current, matching `DurableWsBinding`.
2. **Policy or epoch mismatch**: `!gate.owner_isolated`, `!gate.action_permitted`, or mismatch on `writer_epoch`, `strategy_generation`, or `config_revision`.
3. **Clock or deadline violation**: Expired calibration or signature window in `check_send_sample`, changed `ClockDomain`, backward `MonoTime` or `Instant`, `now >= gate.lease_until`, or `now >= command.deadline`.
4. **Quota denial or venue rate limit**: Insufficient quota or frozen consumer in `Endpoint::consume_command`. On WebSocket `418` or `429` feedback (`SessionState::feedback`), the shared `Endpoint` freezes immediately; WebSocket session control frames (`Connect`, `Upgrade`, `Ping`, `Pong`, `Close`) likewise require a current `DurableWsSession`, `now < deadline`, and `consume_command_session`.

## Verification and References

```sh
cargo test --locked -p tickwire-binance-execution
```

Unit tests verify byte-identical encoding retries, table capacity exhaustion at 4,096 commands, missing-signature rejection, lease/epoch/policy revocations between `prepare_gate` and `finish_gate`, clock-domain faults, and `DeliveryBudget` callbacks that advance local time past the signing window or command deadline so zero bytes are accepted.

* [Execution adapter, `RestEncoder`, and `GateAuthority`](../../crates/tickwire-binance-execution/src/lib.rs)
* [Side-effect-free Binance REST/WebSocket hot-path warmer](../../crates/tickwire-binance-execution/src/warmup.rs)
* [Encoding retry, lease, clock-advancement, and warmup isolation unit tests](../../crates/tickwire-binance-execution/src/tests.rs)
* [Binance signing and order protocol specification](binance-protocol.md)
* [Binance request quota specification](binance-quota.md)
* [Binance WebSocket trading owner specification](binance-ws-trading.md)
* [Exchange clock calibration specification](clock.md)
