# Binance Private User-Data Stream Session

`PrivateClient` manages one authenticated, nonblocking private user-data session for a single `AccountScope` (`risk_domain_id`, `venue == BINANCE`, `environment`, `account`, `market`, and `credential_generation`). Built in safe Rust (`#![forbid(unsafe_code)]`), it drives either Spot WebSocket API user-data subscriptions (`SpotLogon` + `SpotSubscribe`, or single-step `SpotSubscribeSignature`) or USD-M listen-key lifecycle (`POST` create, `/private/ws/<key>` WebSocket `Upgrade`, `PUT` keepalive, and `DELETE` teardown) while enforcing calibrated signing gates and per-poll resource budgets.

`PrivateClient` owns stream authentication, renewal scheduling, frame decoding, and session-epoch isolation, delivering borrowed `PrivateEvent` observations whose `Debug` formatting redacts raw payloads, API keys, signatures, and listen keys (`[REDACTED]`). Durable account state, order correlation, gap reconciliation, and risk admission remain owned by the account coordinator.

## State Model, Tokens, and Bounds

Every control or management action mints a unique `RequestToken` binding nonzero `(connection_epoch, serial, Operation)` (`"{connection_epoch}-{serial}"` on the wire) alongside a request digest covering the entire `AccountScope`. Even a failed connection attempt consumes its `connection_epoch` rather than reusing it. Overflowing any epoch, serial, or deadline counter transitions the client permanently to `Exhausted`, dropping open sockets, clearing pending work, and revoking receiving eligibility.

| Resource | Default Bound | Enforcement Rule |
| --- | --- | --- |
| Outbound control request | `16 KiB` | Shared encoder/signer parameter representation with canonical `recvWindow` decimals. |
| Inbound private message | `1 MiB` | Bounds single WebSocket frame or HTTP management reply. |
| Early pre-ACK events | `32` messages / `1 MiB` | Buffers events arriving before subscription confirmation; overflow triggers reconnect. |
| Control frame queue | `8` frames | Pong echoes peer Ping payload and may bypass unsent app frames once any started frame finishes. |
| Per-poll work | Explicit call, byte, decode, and control budgets | An I/O error that obscures transferred byte count spends the remainder of that poll's I/O budget; decoder yields reschedule buffered fragments. |

## Core Invariants

* **Confirmed subscription before `Receiving`**:
  * **Spot**: Supports Ed25519 for `SpotLogon` (verifying the echoed API key in the logon reply before sending `SpotSubscribe`) and HMAC, RSA, or Ed25519 for `SpotSubscribeSignature`. Enters `Receiving` only upon matching the exact subscribe response (`subscription_id == 0` is valid); subsequent events must carry that established `subscription_id`, and a `SpotStatus` query can never manufacture a missing subscription.
  * **USD-M**: Uses its fixed HTTP method/path and `X-MBX-APIKEY` header to `POST` a listen key, then completes the `/private/ws/<key>` `Upgrade` before entering `Receiving`. Both successful `POST` and `PUT` anchor local key expiry to **request start time + 60 minutes** (not reply receipt time). `PUT` renewal succeeds only while the current key remains unexpired, the request itself has not timed out, and any returned key matches; issuing a new `POST` always starts a new session epoch even if Binance returns the identical listen-key string.
* **Triple-sampled first-byte signing gate**: Before the first plaintext byte of an outbound request enters TLS, `PrivateClient`:
  1. Samples `Clock`, checking `ClockDomain`, monotonic non-regression, original source/stamp, and local request deadline.
  2. For signed requests, invokes `Clock::check_signed` ([binance-private-clock-adapter.md](binance-private-clock-adapter.md)), acquires the required `ControlPermit` (`Connect`, `Upgrade`, API weight, `Pong`, `Close`), resamples `Clock`, and re-verifies monotonic order and signature validity.
  3. Samples `Clock` one final time after the last external callback and checks the local deadline **before** passing plaintext to TLS.
  Zero-byte `WouldBlock` retries repeat these checks under the same idempotent `ControlPermit` charge ID, while permanent `ClockDomain` faults stop the owner immediately.
* **Strict `NoBytes` vs `MayHaveApplied` boundary**: `NoBytes` holds only when TLS accepted zero application bytes. Partial plaintext acceptance followed by socket failure, local flush followed by disconnect, ambiguous responses, or an early matching reply before the local write completes all retain `MayHaveApplied`, and authentication or renewal state advances only after confirming both completed write and valid protocol reply.
* **Eligibility withdrawal before gap/EOF delivery**: Upon any sequence gap, stream termination, or socket EOF, `PrivateClient` revokes `Receiving` eligibility *before* emitting the termination notice or draining already-buffered complete observations (each tagged with its original `SessionContext`), preventing downstream consumers from treating trailing frames as live continuous coverage.

## Remote Teardown and Drop Semantics

Explicit remote close first revokes `Receiving` eligibility, then executes product-specific teardown: Spot unsubscribes the exact `subscription_id` and issues `SpotLogout` when using session logon; USD-M sends HTTP `DELETE`. If the USD-M `DELETE` enters `MayHaveApplied` with an unknown server outcome, `PrivateClient` retains management ownership and records `remote_close_unknown` rather than pretending server state is clean. By contrast, Rust `Drop` closes only local file descriptors without blocking on network teardown. While `Debug` impls redact sensitive material, explicit `.raw()` and payload accessors remain available for durable input journaling.

## Verification and References

```sh
cargo test --locked -p tickwire-binance-private
```

Local TLS test peers independently verify HMAC, RSA, and Ed25519 signatures and exercise Spot logon/signature modes, USD-M 60-minute renewal anchors, stale `RequestToken` rejection, early replies arriving before write completion, and ambiguous `DELETE` closure, complemented by a finite TLA+ session model.

* [Private stream session crate and types](../../crates/tickwire-binance-private/src/lib.rs)
* [Calibrated clock adapter specification](binance-private-clock-adapter.md)
* [Spot private session lifecycle tests](../../crates/tickwire-binance-private/tests/spot_lifecycle.rs)
* [USD-M listen-key lifecycle tests](../../crates/tickwire-binance-private/tests/usdm_lifecycle.rs)
* [Private session TLA+ verification plan](../../tla/execution_models/binance_private_session/PLAN.md)
