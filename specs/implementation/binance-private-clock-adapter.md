# Calibrated Clock Adapter for Binance Private Sessions

`tickwire-binance-session::ClockAdapter<'a, C, B>` adapts a mutably borrowed `BinanceTimeClient<C>` and a caller-supplied `DeliveryBudget` provider (`B`) to satisfy the `tickwire_binance_private::Clock` trait ([binance-private-session.md](binance-private-session.md)). By sharing the underlying `BinanceTimeClient`, HTTPS time calibration and authenticated private WebSocket/REST session control operate over the exact same calibrated exchange-time interval without maintaining a second clock estimate or an internal send queue.

Constructed with an explicit `AccountScope` (`risk_domain_id`, `venue == BINANCE`, `environment`, `account`, `market`, and `credential_generation`), `ClockAdapter::new` validates the scope and verifies that the borrowed `BinanceTimeClient::source()` matches the exact `(Market, Environment)` pair (`SpotProduction`, `SpotTest`, `UsdmProduction`, or `UsdmTest`), locking in the client's `ClockDomain` for the adapter's lifetime (`#![forbid(unsafe_code)]`).

## Sampling and Four-Step Send Verification

Calling `ClockAdapter::sample()` delegates to `BinanceTimeClient::sample()`, verifies that both `local.monotonic` and any present `SignedTime` match the adapter's `ClockDomain` and `Source`, and returns a `ClockSample { instant, monotonic, signing_time }` (where `signing_time` is `None` until initial HTTPS calibration succeeds).

When `PrivateClient` prepares to write the first byte of a signed request, `check_signed(scope, stamp, now, window_us)` executes a strict four-stage verification sequence across the adapter and private session:

1. **Identity, domain, and window validation**: Verifies `*scope == self.scope`, `stamp.source() == self.source` (`Error::Identity`), and `stamp.domain() == self.domain == now.monotonic.domain()` (`Error::ClockDomain`). Converts `window_us` (`recvWindow` in microseconds) to `window_ns` via checked multiplication by `1,000`, rejecting zero or overflowing windows with `Error::Bounds`.
2. **Pre-callback fresh sample and original-stamp preservation**: Calls `self.sample()` to obtain `fresh`, rejects any regression (`fresh.monotonic < now.monotonic` or `fresh.instant < now.instant`) with `Error::ClockDomain`, and invokes `DeliveryBudget::remaining_ns(scope, stamp, &fresh)`—passing the **original `stamp`** so a newer sample can never silently replace the timestamp embedded in the request's cryptographic signature.
3. **Post-callback calibrated send check**: Calls `BinanceTimeClient::check_send(stamp, window_ns, budget_ns)`, which takes *another* monotonic clock sample after `remaining_ns` returns and validates the original `stamp` against the current calibrated `[L, U]` exchange interval, `window_ns`, `budget_ns`, and anchor expiry ([clock.md](clock.md)).
4. **Final session deadline check**: After `check_signed` returns `Ok(())` (and after any subsequent quota callback), `PrivateClient` samples its local clock once more and verifies monotonic order and the local request deadline immediately before passing plaintext to TLS.

## Core Invariants

* **Shared calibration interval without duplicate state**: The adapter holds neither an independent clock anchor nor an outbound send queue; HTTPS time calibration and private session requests read from the exact same `BinanceTimeClient` state machine.
* **Immutable signed timestamp across budget callbacks**: `DeliveryBudget::remaining_ns` receives the freshly sampled `ClockSample` alongside the request's **original `SignedTime`**, ensuring a callback can never substitute a newer timestamp into an already-signed payload.
* **Zero tolerance for callback-induced staleness**: Because `BinanceTimeClient::check_send` resamples monotonic time *after* `DeliveryBudget::remaining_ns` returns—and `PrivateClient` resamples again after `ControlPermit` callbacks—any delay introduced by user callbacks is fully accounted for before the first byte leaves user space.

## Transitions and Failure Semantics

`clock_error` partitions underlying `BinanceTimeClient` and `tickwire_clock` errors into two distinct failure classes for `PrivateClient`:

* **Permanent owner stop (`Error::ClockDomain`)**: Triggered by `Core::Domain`, `Core::TimeBackwards`, `Core::Faulted`, `Core::ClockJump`, `Core::Overflow`, `BinanceTimeClient::Exhausted`, or monotonic/`Instant` regression across adapter samples, permanently halting the private session owner.
* **Single-request denial (`Error::Clock` / `Identity` / `Bounds`)**: Triggered by uncalibrated anchors, expired calibration or `recvWindow` bounds, transient calibration network/protocol errors, scope mismatches, or invalid `window_us`, denying only the current request with zero bytes sent so calibration can recover.

## Verification and References

```sh
cargo test --locked -p tickwire-binance-session --test integration
```

In the local HTTPS-to-WSS integration test, a signed private request is configured with a `1 s` local request deadline, a `5 s` `recvWindow`, and a `10 s` calibration anchor lifetime; when the `DeliveryBudget` callback artificially advances local time to `2 s` (still inside `recvWindow` and calibration lifetime, but past the `1 s` request deadline), the final post-callback gate aborts transmission with zero bytes accepted by the transport.

* [Borrowed clock adapter implementation](../../crates/tickwire-binance-session/src/lib.rs)
* [HTTPS-to-WSS callback-delay integration tests](../../crates/tickwire-binance-session/tests/integration.rs)
* [Binance private stream session specification](binance-private-session.md)
* [Exchange clock calibration specification](clock.md)
