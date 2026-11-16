# `tickwire-app` Engineering Guide (`AGENTS.md`)

## Purpose and Architectural Boundary

`tickwire-app` is the unified stateless single-venue Order Routing System (`ORS`) execution process (`1 ORS process = 1 Venue`). It wires [`BinanceVenueSession`](src/binance_session.rs) (Spot, USD-M, Coin-M, and European Options) and [`UtaVenueSession`](../tickwire-spot-runner/src/session.rs) (`Bybit V5` and `OKX V5` 6-product Unified Trading Accounts) through [`UnifiedVenueSession`](src/venue.rs) directly into [`tickwire-runtime`](../tickwire-runtime/AGENTS.md)'s generic `OrderRoutingSystem<S>` and `OrsProcess<S>`.

* **Pure Stateless 3-Process Topology**: Holds zero public market-data sockets, zero SHM quote subscriptions, zero local write-ahead logs, zero journals, zero disk blob files, and zero dynamic configuration patches. Strategy workers subscribe independently to [`tickwire-md`](../tickwire-md/AGENTS.md) and exchange inline `1,024B` `IntentRecord` and `512B` `ReportRecord` slots over sealed `memfd` SPSC rings with `tickwire-app`.
* **Cold Exchange REST Recovery**: Startup and restart recover balances, derivative/option positions, open-order holds, `OrsStpBook` entries, strategy attribution, and per-strategy `DopState::next_order_seqnum` watermarks exclusively from authenticated Exchange REST snapshots (`Account + OpenOrders`) paired with 32-character `[0-9a-z]` `SelfDescribingClientId` (`tw1...`) tokens.
* **Distinct Execution Facts**: Keeps gateway receipts (`FrontAck`), book acceptance (`Ack`), cumulative order-status projections (`ProjectedFill`), and individual trade executions (`Fill`, setting `amendment = true` when full `ORDER_TRADE_UPDATE` / `executionReport` / `execution` fee/rebate updates follow `TRADE_LITE` / `execution.fast`) strictly separate.

## Thread Isolation, Lock Hierarchy, and Safety

* **Hot vs. Cold Core Disjointness**: Dedicated `REQ` (`req_cpu`, enforcing the default `23,000 ns` microsecond stale-order gate with `RefusalReason::RejectStaleOrder`) and `RESP` (`resp_cpu`, `epoll_wait_ms = 0`) threads are strictly disjoint from `housekeeping_cpus` (`ControlServer`, `Dialer`, `AsyncRestQuerier`, `LatencyTracer`, and `main` `SignalWaiter`), verified via [`tickwire-control-sys`](../tickwire-control-sys/AGENTS.md).
* **Spinlock Discipline**: `Order` and `Connection` spinlocks never overlap; `StpBook` is entered only under a single `Order` lock; zero logging, heap allocation, or protocol parsing occurs while holding any spinlock.
* **Audited `unsafe` Boundary**: Crate-wide `#![deny(unsafe_code)]` and `#![deny(unsafe_op_in_unsafe_fn)]`, with `#![forbid(unsafe_code)]` on every module except [`src/external_lanes.rs`](src/external_lanes.rs)'s two `SCM_RIGHTS`-verified `IntentReader::attach` / `Publisher::register_cursor` calls. Zero raw OS-ABI crate calls and zero floating-point arithmetic.

## Module Layout and Verification

* [`src/lib.rs`](src/lib.rs) & [`src/main.rs`](src/main.rs): Library exports and CLI entrypoint (`tickwire-app <inspect --config PATH | run --config PATH>`).
* [`src/binance_session.rs`](src/binance_session.rs): Full-market `BinanceVenueSession` (`Spot`, `Usdm`, `Coinm`, `Options`).
* [`src/venue.rs`](src/venue.rs): `UnifiedVenueSession` enum (`Binance`, `BybitUta`, `OkxUta`).
* [`src/external_lanes.rs`](src/external_lanes.rs): Out-of-process Unix `SOCK_SEQPACKET` SPSC lane factories for `tickwire-strategy-worker`.
* [`src/config.rs`](src/config.rs) & [`src/runtime.rs`](src/runtime.rs): Static `OrsAppConfig`, `CatalogVerificationConfig` verification, and `OrsProcess` signal lifecycle (`SIGUSR1`, `SIGUSR2`, `SIGINT`/`SIGTERM`).
* **Examples & integration tests**: [examples/shared-market-strategies.json](examples/shared-market-strategies.json) · [tests/ors_binance_full_market.rs](tests/ors_binance_full_market.rs) · [tests/catalog_process.rs](tests/catalog_process.rs) · [tests/parameters_process.rs](tests/parameters_process.rs) · [tests/projection_launch.rs](tests/projection_launch.rs)
* **Specifications**: [account.md](../../specs/implementation/account.md) · [binance-execution.md](../../specs/implementation/binance-execution.md) · [spot-strategy-routing.md](../../specs/implementation/spot-strategy-routing.md) · [instrument-catalog.md](../../specs/implementation/instrument-catalog.md)

```sh
cargo test --locked -p tickwire-app --all-targets
cargo clippy --locked -p tickwire-app --all-targets -- -D warnings
```
