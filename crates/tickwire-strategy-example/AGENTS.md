# tickwire-strategy-example

`tickwire-strategy-example` owns the stateless reference `Counting` strategy implementation (`Registration::v1` and `Registration::v2` with distinct schema identifiers `V1` and `V2`) and the two worker binaries (`tickwire-counting-worker` and `tickwire-counting-v1-worker`). Both the library and binary entrypoints enforce `#![forbid(unsafe_code)]`, delegating shared-memory attachment, `DOP` logon, and CPU affinity isolation exclusively to `tickwire-strategy-sdk` and `tickwire-control-sys` with zero local disk persistence.

## Invariants and Change Guidelines

* **Canonical startup parameter validation (`0` disk state)**: `Parameters::decode` in [src/lib.rs](src/lib.rs) enforces `#[serde(deny_unknown_fields)]`, exact canonical JSON round-trip equality (`bytes()? == bytes`), positive `minimum_observations`, non-negative `minimum_bid_ticks` (`0` floating-point fields), and `Spot` `Limit` `CanonicalCommand` validation whenever `command` is configured.
* **Single-shot inline `IntentRecord` emission & `ReportRecord` accounting**: `Counting` maintains private in-memory counters (`observations`, `reports`, `fills`, `spent`) across live `MarketView` callbacks and inbound 512-byte `ExecutionReport` callbacks (`FrontAck`, `Ack`, `Fill` [`Execution`, `FeeAmendment`, `Projected`], `Canceled`), emitting at most one inline 1,024-byte `OutboundIntent` (`tick_mono_ns` propagated from `MarketView::tick_mono_ns()`) once `observations >= minimum_observations` and `best_bid >= minimum_bid_ticks`.
* **Safe `#![forbid(unsafe_code)]` binary entrypoints and `crc32` forwarding**: Keep [src/bin/tickwire-counting-worker.rs](src/bin/tickwire-counting-worker.rs) and [src/bin/tickwire-counting-v1-worker.rs](src/bin/tickwire-counting-v1-worker.rs) minimal safe wrappers around `tickwire_strategy_sdk::registered::run`, and forward optional `--features crc32` (`default = []`) to `tickwire-strategy-sdk/crc32`.

## Verification and Specifications

* **Source, binaries, and guide**: [src/lib.rs](src/lib.rs) · [src/bin/tickwire-counting-worker.rs](src/bin/tickwire-counting-worker.rs) · [src/bin/tickwire-counting-v1-worker.rs](src/bin/tickwire-counting-v1-worker.rs) · [README.md](README.md) · [Deploy two strategies](../../README.md#deploy-two-strategies)
* **Integration tests**: [tests/example_worker.rs](tests/example_worker.rs)
* **Specifications**: [strategy-input-consumption.md](../../specs/implementation/strategy-input-consumption.md) · [strategy-parameter-consumption.md](../../specs/implementation/strategy-parameter-consumption.md)

```sh
cargo test --locked -p tickwire-strategy-example --all-targets
cargo clippy --locked -p tickwire-strategy-example --all-targets -- -D warnings
```
