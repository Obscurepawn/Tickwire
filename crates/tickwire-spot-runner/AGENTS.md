# `tickwire-spot-runner` Engineering Guide (`AGENTS.md`)

## Purpose and Architectural Boundary

`tickwire-spot-runner` is the stateless Unified Trading Account (`Bybit V5` and `OKX V5`) ORS runtime adapter and process runner. It wires [`tickwire-spot-execution`](../tickwire-spot-execution/AGENTS.md) directly into [`tickwire-runtime`](../tickwire-runtime/AGENTS.md)'s generic `VenueSession`, `OrderRoutingSystem<S>`, and `OrsProcess<S>` (`1 ORS process = 1 Venue`).

* **Pure Stateless Execution**: Holds zero local write-ahead logs, journals, disk blob stores, lease files, or dynamic configuration patches. Cold boot and restart recover balances, derivative/option positions, open-order holds, `OrsStpBook` entries, strategy attribution, and per-strategy `DopState::next_order_seqnum` watermarks exclusively from authenticated Exchange REST snapshots (`Account + Positions + OpenOrders`) paired with 32-character `[0-9a-z]` `SelfDescribingClientId` (`tw1...`) tokens.
* **All 6 UTA Products & Dual Transport**: `UtaVenueSession` (`BybitVenueSession` / `OkxVenueSession`) routes `Spot`, `LinearPerpetual`, `InversePerpetual`, `LinearFuture`, `InverseFuture`, and `Option` orders—including `TimeInForce::Rpi` and optional Options/Block `RFQ` quoting (`RfqQuoteHook`)—across shared `[local_ip, count]` URL pools (`product_mask = 0`) over either multiplexed WebSocket trade (`WsLink`, `16` in-flight) or pooled HTTP/1.1 keep-alive (`HttpsLink`, `1` in-flight).
* **Distinct Execution Facts & Maintenance Switchover**: Preserves gateway acknowledgements (`FrontAck`), book acceptance (`Ack`), cumulative order-status projections (`ProjectedFill`), and granular trade executions (`Fill`, emitting `amendment = true` when full `execution` fee/rebate details follow `execution.fast`), and triggers zero-downtime `Dialer` standby switchover on OKX `64008` and Bybit maintenance `notice` frames.

## Concurrency, CPU Affinity, and Safety Invariants

* `#![forbid(unsafe_code)]`, zero raw OS-ABI crate calls (signal and CPU affinity primitives come exclusively from [`tickwire-control-sys`](../tickwire-control-sys/AGENTS.md)), and zero floating-point arithmetic.
* `UtaVenueSession` is `Send + Sync` and shared via `Arc<UtaVenueSession>` across dedicated `REQ` (`req_cpu`), `RESP` (`resp_cpu`), `Dialer`, and `AsyncRestQuerier` (`housekeeping_cpus`) threads without coarse session mutexes or hot-path `sleep` calls.
* Internal correlation caches (`inflight_tags`, `venue_ids`) use fixed-size stack/scalar arrays behind leaf `TimedSpinLock`s acquired strictly outside `Order` and `Connection` spinlocks, with zero heap allocation, logging, or protocol parsing inside any critical section.

## Module Layout and Verification

* [`src/lib.rs`](src/lib.rs) & [`src/session.rs`](src/session.rs): Crate root and `UtaVenueSession` (`BybitVenueSession` / `OkxVenueSession`) implementing `tickwire_runtime::VenueSession`.
* [`src/config.rs`](src/config.rs): Static JSON configuration (`SpotRunnerConfig`) and PEM CA loader.
* [`src/runner.rs`](src/runner.rs): `inspect_config`, `spawn_uta_ors_process`, and `run_until_signal` (`SIGUSR1` RMS reload + `QueryOpen` + `Dump`, `SIGUSR2` `CancelAll`, `SIGINT`/`SIGTERM` `2s` drain + `OrsShutDown`).
* [`src/paper_peer.rs`](src/paper_peer.rs), [`src/main.rs`](src/main.rs), [`tests/uta_ors_runtime.rs`](tests/uta_ors_runtime.rs), & [`tests/reconcile_cli.rs`](tests/reconcile_cli.rs): Ephemeral loopback TLS exchange peer, CLI (`inspect --config PATH`, `run --config PATH`, `paper <bybit|okx> [SUMMARY_PATH]`), and end-to-end integration tests.
* **Specifications**: [spot-strategy-routing.md](../../specs/implementation/spot-strategy-routing.md) · [spot-execution.md](../../specs/implementation/spot-execution.md) · [spot-rest-recovery.md](../../specs/implementation/spot-rest-recovery.md)

```sh
cargo test --locked -p tickwire-spot-runner --all-targets
cargo clippy --locked -p tickwire-spot-runner --all-targets -- -D warnings
```
