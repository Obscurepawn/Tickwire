# tickwire-binance-session

`tickwire-binance-session` (`#![forbid(unsafe_code)]`) provides `ClockAdapter<'a, C, B>`, a short-lived borrowed
adapter implementing `tickwire-binance-private::Clock` over a runtime-owned `BinanceTimeClient<C>` and
`DeliveryBudget`. Runtime hosts construct `ClockAdapter` when polling `PrivateClient` so private session logons and
subscriptions share the exact calibrated clock domain and delivery-budget policy used by outbound order execution,
without duplicating clock ownership or socket state.

## Invariants and Change Guidelines

* **Strict `(Market, Environment)` to `Source` binding**: `ClockAdapter::new` in [src/lib.rs](src/lib.rs) validates
  `AccountScope` and requires `BinanceTimeClient::source()` to match the derived `Source` (`SpotProduction`,
  `SpotTest`, `UsdmProduction`, `UsdmTest`), rejecting cross-environment or cross-product pairings with
  `Error::Identity`.
* **Monotonic domain continuity across samples**: Both `Clock::sample` and `Clock::check_signed` verify that
  `MonoTime`, `SignedTime`, and caller-supplied `ClockSample` belong to the adapter's pinned `ClockDomain` and never
  move backwards (`fresh.monotonic >= now.monotonic` and `fresh.instant >= now.instant`), surfacing
  `Error::ClockDomain` on any domain fault, rollback, jump, or calibration exhaustion.
* **Fresh sample plus post-callback `check_send`**: In `check_signed`, convert `recv_window_micros` to nanoseconds
  with checked multiplication, take a fresh `BinanceTimeClient` sample before querying `DeliveryBudget::remaining_ns`,
  and delegate to `BinanceTimeClient::check_send` so time spent inside `remaining_ns` is accounted for before sending
  bytes.
* **Non-exhaustive `Debug` formatting**: Keep internal mutable references (`&mut BinanceTimeClient`, `&mut B`) omitted
  from `ClockAdapter` 's `Debug` output via `finish_non_exhaustive()`.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs)
* **Integration tests**: [tests/integration.rs](tests/integration.rs)
* **Specifications**: [binance-private-clock-adapter.md](../../specs/implementation/binance-private-clock-adapter.md)
  · [binance-private-session.md](../../specs/implementation/binance-private-session.md)

```sh
cargo test --locked -p tickwire-binance-session --all-targets
```
