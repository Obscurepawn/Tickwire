# Binance Private User-Data Stream Session

`PrivateClient` (`tickwire-binance-private`) manages one authenticated, nonblocking private user-data session for a single `AccountScope` (`risk_domain_id`, `venue == BINANCE`, `environment`, `account`, `market`, and `credential_generation`), complementing the multi-market ORS `GuardedValue` `listenKey` and `userDataStream.subscribe` hooks in [`tickwire-binance-account`](binance-ws-trading.md). Built in safe Rust (`#![forbid(unsafe_code)]`) with zero local WAL dependencies, `PrivateClient` drives either Spot WebSocket API user-data subscriptions (`SpotLogon` + `SpotSubscribe`, or single-step `SpotSubscribeSignature`) or USD-M listen-key lifecycle (`POST` create, `/private/ws/<key>` WebSocket `Upgrade`, `PUT` keepalive, and `DELETE` teardown) while enforcing calibrated signing gates and per-poll resource budgets.

`PrivateClient` owns stream authentication, renewal scheduling, frame decoding, and session-epoch isolation, delivering borrowed `PrivateEvent` observations whose `Debug` formatting redacts raw payloads, API keys, signatures, and listen keys (`[REDACTED]`). Stateless order-slot correlation, `FillRecordPool` trade deduplication, and `OrsQueryDesk` recovery belong to [`tickwire-account`](account.md) and [`tickwire-binance-account`](binance-ws-trading.md).

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
  * **USD-M, Coin-M, and Options `listenKey` streams**: In `PrivateClient` (USD-M) and in `BinanceSession`'s `GuardedValue` stream-token lifecycle (`POST`/`PUT /{fapi,dapi,eapi}/v1/listenKey` with `stream_token_refresh_s = 1800`), the stream URL carries the listen key (`/ws/<key>` on colocation endpoints, `?listenKey=<key>` on public `/private/ws` endpoints). `PUT` keepalive stores nothing; `listenKeyExpired` clears the guarded token and reconnects only the stream pool while leaving order-entry sockets active.
* **Triple-sampled first-byte signing gate**: Before the first plaintext byte of an outbound request enters TLS, `PrivateClient`:
  1. Samples `Clock`, checking `ClockDomain`, monotonic non-regression, original source/stamp, and local request deadline.
  2. For signed requests, invokes `Clock::check_signed` ([binance-private-clock-adapter.md](binance-private-clock-adapter.md)), acquires the required `ControlPermit` (`Connect`, `Upgrade`, API weight, `Pong`, `Close`), resamples `Clock`, and re-verifies monotonic order and signature validity.
  3. Samples `Clock` one final time after the last external callback and checks the local deadline **before** passing plaintext to TLS.
* **Strict `NoBytes` vs `MayHaveApplied` boundary**: `NoBytes` holds only when TLS accepted zero application bytes. Partial plaintext acceptance followed by socket failure, local flush followed by disconnect, ambiguous responses, or an early matching reply before the local write completes all retain `MayHaveApplied`, and authentication or renewal state advances only after confirming both completed write and valid protocol reply.
* **Eligibility withdrawal before gap/EOF delivery**: Upon any sequence gap, stream termination, or socket EOF, `PrivateClient` revokes `Receiving` eligibility *before* emitting the termination notice or draining already-buffered complete observations (each tagged with its original `SessionContext`).

## Remote Teardown and Drop Semantics

Explicit remote close first revokes `Receiving` eligibility, then executes product-specific teardown: Spot unsubscribes the exact `subscription_id` and issues `SpotLogout` when using session logon; USD-M sends HTTP `DELETE`. If the USD-M `DELETE` enters `MayHaveApplied` with an unknown server outcome, `PrivateClient` retains management ownership and records `remote_close_unknown` rather than pretending server state is clean. By contrast, Rust `Drop` closes only local file descriptors without blocking on network teardown.

## Verification and References

```sh
cargo test --locked -p tickwire-binance-private --all-targets
```

* [Private stream session crate and types](../../crates/tickwire-binance-private/src/lib.rs)
* [Calibrated clock adapter specification](binance-private-clock-adapter.md)
* [Spot private session lifecycle tests](../../crates/tickwire-binance-private/tests/spot_lifecycle.rs)
* [USD-M listen-key lifecycle tests](../../crates/tickwire-binance-private/tests/usdm_lifecycle.rs)
* [Private session TLA+ verification plan](../../tla/execution_models/binance_private_session/PLAN.md)
