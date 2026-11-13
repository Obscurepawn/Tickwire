# tickwire-binance-clock

`tickwire-binance-clock` (`#![forbid(unsafe_code)]`) calibrates a local `MonotonicClock` (`SystemClock`) against
Binance Spot or USD-M server time by driving one dedicated nonblocking HTTPS connection per sample
(`BinanceTimeClient`) over `tickwire-transport` and feeding bounded round-trip intervals into `tickwire-clock`.
Private sessions and execution adapters query `BinanceTimeClient` for `SignedTime` stamps and first-write `check_send`
validation; quota budgeting (`TimePermit`) and trading state remain outside this crate.

## Invariants and Change Guidelines

* **One authenticated connection per sample**: `BinanceTimeClient` in [src/client.rs](src/client.rs) binds each
  calibration sample to its configured `Source` (`SpotProduction`, `SpotTest`, `UsdmProduction`, `UsdmTest`),
  `ClockDomain`, `sample_serial`, and monotonically increasing `connection_epoch`, never reusing an ambiguous or
  timed-out socket across samples.
* **Bracketed monotonic RTT measurement**: Record the pre-send monotonic timestamp immediately before the first HTTP
  request byte is accepted and the post-receive timestamp after the complete `serverTime` response arrives; reject
  backwards monotonic readings, clock jumps, non-200 HTTP statuses, or round trips exceeding `CalibrationConfig` /
  `request_timeout_ns`.
* **Idempotent `TimePermit` charging**: Re-evaluate `TimePermit::allow` on every zero-byte connection or query attempt
  in [src/lib.rs](src/lib.rs) and [src/client.rs](src/client.rs), ensuring `(source, domain, connection_epoch, kind)`
  is charged at most once.
* **Post-budget re-sampling on `check_send`**: When validating an existing `SignedTime` against `recv_window_ns` and
  `budget_ns`, sample the monotonic clock after caller callbacks so elapsed callback latency cannot slip an expired
  timestamp onto the wire.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/client.rs](src/client.rs)
* **Integration tests**: [tests/https.rs](tests/https.rs) · [tests/system_clock.rs](tests/system_clock.rs)
* **Specifications and models**: [clock.md](../../specs/implementation/clock.md) ·
  [system-clock.md](../../specs/implementation/system-clock.md) · [clock](../../tla/execution_models/clock/ROADMAP.md)
  · [system_clock](../../tla/execution_models/system_clock/ROADMAP.md)

```sh
cargo test --locked -p tickwire-binance-clock --all-targets
./scripts/check-tla.sh system_clock
```
