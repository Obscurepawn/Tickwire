# Tickwire

**Tickwire** is a stateless, ultra-low-latency single-host trading engine written in Rust for Linux. It isolates public market-data ingestion, strategy workers, and multi-UID venue order execution into three separate OS process roles communicating exclusively over `memfd`-sealed shared-memory rings and authenticated Unix control sockets—with zero local WAL, journal, disk blob files, HA-lease files, or runtime configuration patches.

## Architectural Guarantees

* **Pure stateless 3-process SHM topology:** Public market data ([`tickwire-md`](crates/tickwire-md/README.md)), strategy workers ([`tickwire-strategy-sdk`](crates/tickwire-strategy-sdk/AGENTS.md)), and the unified per-venue Order Routing System ([`tickwire-runtime`](crates/tickwire-runtime/AGENTS.md)) own strictly disjoint state domains. Workers consume SPMC `memfd` market rings at independent cursors and exchange inline `1024B` `IntentRecord` and `512B` `ReportRecord` SPSC slots with the ORS process over zero-disk shared memory.
* **Disjoint hot vs. cold CPU affinity and fine-grained spinlocks:** Hot data-plane threads—`tickwire-md` `reactor_cpu`, worker `worker_cpu`, ORS `REQ` busy-spin (`req_cpu`), and ORS `RESP` epoll (`resp_cpu`, `epoll_wait_ms = 0`)—are strictly isolated from cold `housekeeping_cpus` (`md-cold`, `Dialer`, `AsyncRestQuerier`/`OrsQueryDesk`, `LatencyTracer` sink, DOP `ControlServer`, and `SignalWaiter`), enforced statically in [`tickwire-config`](crates/tickwire-config/AGENTS.md) and verified via `sched_setaffinity` + `sched_getaffinity` read-back in [`tickwire-control-sys`](crates/tickwire-control-sys/AGENTS.md) (`hot_cpus ∩ housekeeping_cpus == ∅`). Synchronization uses non-overlapping fine-grained spinlocks per `Order`, per `Connection`, per symbol (`StpBook`), and on `GuardedValue` stream tokens with zero logging, heap allocation, or protocol parsing under lock.
* **Zero-allocation market hot path:** Public venue feeds normalize once into integer price ticks and quantity lots (`#![no_std]` order-book core in [`tickwire-book`](crates/tickwire-book/src/lib.rs)). Steady-state Bybit and OKX top-of-book frames publish to shared memory with zero heap allocations after startup.
* **Sub-microsecond `REQ` admission, atomic STP, and multi-IP EWMA routing:** On `req_cpu`, outbound intents pass a `23 µs` stale-order gate (`max_order_age_ns`), flat `RmsEngine` rolling windows (`1s`/`5s`/`10s`), checked fixed-point reservations across multi-wallet `AssetKey { wallet_id, asset_id }`, and a lock-free atomic BBO `StpBook::self_cross` check before `OrderRouter::pick` (`9/8` integer EWMA) selects a product-matched connection, spends atomic `local_ip` and account quotas in `SharedRateLimiter`, and transmits over `LinkOps` (WebSocket, HTTPS, or FIX with `SO_BINDTODEVICE`).
* **Single-pass `RESP` decode, immediate slot retirement, and cold REST `tw1...` bootstrap:** On `resp_cpu`, single-pass `JsonScan` separates `One ack` (`FrontAck` / `Ack`), optimistic `ProjectedFill` watermarks, and trade-ID-deduplicated `FillRecordPool` (`Fill` plus post-trade `amendment = true` fee/rebate updates) as distinct facts, unlinking `StpBook` and retiring completed order slots immediately (`RETIRED`) without stalling on asynchronous fee closure (`fees_closed`). Single-order timeouts isolate to per-account `OrsQueryDesk` on cold `housekeeping_cpus`, while cold boot and process restarts reconstruct open holds, `tw1...` `SelfDescribingClientId` strategy attribution, and sequence floors directly from authenticated Exchange REST `Account + OpenOrders` snapshots.
* **Mechanically verified boundaries:** Workspace crates default to `#![forbid(unsafe_code)]` outside four audited boundaries (`tickwire-shm-sys`, `tickwire-clock-sys`, `tickwire-control-sys`, and `tickwire-ipc` `crc32c`/`zerocopy`), wire/config/risk contracts are validated against golden vectors, and protocol interleavings are checked in [TLA+](tla/README.md) (TLC and Apalache).

---

## System Architecture

![Stateless 3-process Linux shared-memory architecture: tickwire-md decodes public venue feeds on hot reactor_cpu with cold md-cold housekeeping_cpus into a sealed SPMC memfd market ring; 1..M tickwire-strategy-worker processes on worker_cpu consume independent market cursors and exchange inline 1024B IntentRecord and 512B ReportRecord SPSC shared-memory rings plus DOP Unix socket bootstrap with a unified per-venue ORS process hosting 1..N multi-UID AccountSessions, disjoint REQ spin (req_cpu) and RESP epoll (resp_cpu) hot threads, SharedRateLimiter, LinkOps WS/HTTPS/FIX with SO_BINDTODEVICE, and cold housekeeping_cpus Dialer, AsyncRestQuerier/OrsQueryDesk, LatencyTracer, SignalWaiter, and cold Exchange REST tw1... bootstrap.](assets/architecture.svg)

A single-host deployment separates market fan-out, strategy algorithms, and multi-UID venue execution across three stateless Linux process roles (specified in [Single-Host Trading](specs/implementation/single-host-trading.md)):

1. **Market-Data Publisher ([`tickwire-md`](crates/tickwire-md/README.md)):** Terminates public venue WebSocket, HTTPS depth-snapshot, and Spot SBE streams on `reactor_cpu` (with cold helpers pinned to `md-cold` `housekeeping_cpus`), enforces sequence continuity, and publishes normalized 512-byte market records into a sealed `memfd` SPMC ring. Workers attach via the nonblocking [JOIN protocol](specs/implementation/join.md) backed by a bounded in-memory idempotency table.
2. **Supervised Strategy Workers ([`tickwire-strategy-sdk`](crates/tickwire-strategy-sdk/src/lib.rs), [`tickwire-supervisor`](crates/tickwire-supervisor/AGENTS.md)):** Isolated child processes launched from `memfd`-sealed ELF images verified via `/proc/<pid>/exe` and confined by Linux cgroup v2, NUMA, and `worker_cpu` / `housekeeping_cpus` affinities ([Supervisor Process](specs/implementation/supervisor-process.md)). Each worker advances its own market ring cursor, maintains private strategy state, targets `account_ref = 0` (default) or `1..=255`, writes inline `1024B` `IntentRecord` commands (`New`, `Modify`, `Cancel`), and consumes ordered `512B` `ReportRecord` replies.
3. **Unified Venue ORS Process ([`tickwire-runtime`](crates/tickwire-runtime/AGENTS.md), [`tickwire-app`](crates/tickwire-app/AGENTS.md), [`tickwire-spot-runner`](crates/tickwire-spot-runner/AGENTS.md)):** `1 ORS process = 1 Venue` with zero public market-data sockets or SHM quote subscriptions, managing `1..N` multi-UID `AccountSession`s (`1 AccountSession = 1 Exchange UID`) across multi-product connection pools (`OrderRoute.product_mask`), multi-wallet `AssetKey` balances, process-wide `SharedRateLimiter`, and cold-core `Dialer` + `OrsQueryDesk`.

### Order Lifecycle and Recovery Boundaries

![Stateless ultra-low-latency order lifecycle: a 1024B Strategy IntentRecord enters REQ on req_cpu through the 23-microsecond stale gate, flat RmsEngine, lock-free atomic BBO StpBook::self_cross, OrderRouter::pick 9/8 EWMA selection with SharedRateLimiter local_ip and account quotas, and LinkOps WS/HTTPS/FIX wire send; venue replies enter RESP on resp_cpu via single-pass JsonScan into distinct One ack (FrontAck/Ack), ProjectedFill, and FillRecordPool (Fill trade deduplication plus amendment = true fee patch) facts, immediate RETIRED slot reclamation and StpBook unlink without waiting on fees_closed, per-order OrsQueryDesk doubt recovery on cold housekeeping_cpus, and cold Exchange REST Account + OpenOrders tw1... bootstrap.](assets/order-lifecycle.svg)

Tickwire keeps **acknowledgement** (`FrontAck` / `Ack`), **order-status projection** (`ProjectedFill`), and **individual trade execution** (`Fill` + post-trade `amendment = true` fee/rebate patch in `FillRecordPool`) strictly distinct. Pre-wire refusals (`23 µs` stale gate, `RmsEngine`, `StpBook::self_cross`, or single-pick `SharedRateLimiter` NACK) release holds immediately without retrying peer connections; single-order timeouts or disconnects isolate to `OrsQueryDesk` on `housekeeping_cpus` while sibling orders continue trading; terminal orders immediately unlink `StpBook` and reclaim `RETIRED` slots without waiting on `fees_closed`; and cold boots recover open holds and `tw1...` strategy attribution directly from authenticated Exchange REST `Account + OpenOrders` snapshots.

---

<a id="exchange-support"></a>
## Exchange Support

| Venue | Public Market Data (`tickwire-md`) | Unified Stateless ORS Execution & Cold REST Recovery |
| :--- | :--- | :--- |
| **Binance** | Spot and USDⓈ-M Futures depth (`WebSocket` deltas bridged against `HTTPS` snapshots), trades, best bid/ask, and fixed-schema Spot SBE ([`tickwire-binance-sbe`](crates/tickwire-binance-sbe/src/lib.rs)). | Multi-UID stateless ORS ([`tickwire-app`](crates/tickwire-app/AGENTS.md) + [`tickwire-binance-execution`](crates/tickwire-binance-execution/AGENTS.md)) across Spot, USDⓈ-M, COIN-M, and Options over pooled REST and WebSocket-API links with `StpBook`, `ProjectedFill`, `FillRecordPool` (`amendment`), `OrsQueryDesk`, and Exchange REST `Account + OpenOrders` + `tw1...` bootstrap. |
| **Bybit** | V5 Spot top-of-book (`orderbook.1`) normalized into integer ticks and lots with zero steady-state heap allocations. | V5 6-product Unified Trading Account (Spot, LinearPerp, InversePerp, LinearFutures, InverseFutures, Options) execution via [`tickwire-spot-execution`](crates/tickwire-spot-execution/src/lib.rs) and [`tickwire-spot-runner`](crates/tickwire-spot-runner/AGENTS.md) on [`tickwire-runtime`](crates/tickwire-runtime/AGENTS.md), with native in-place `Modify` (`/v5/order/amend`), `StpBook`, `ProjectedFill`, `OrsQueryDesk`, and cursor-paginated REST bootstrap. |
| **OKX** | V5 Spot tick-by-tick top-of-book (`bbo-tbt`) normalized into integer ticks and lots with zero steady-state heap allocations. | V5 6-product Unified Trading Account execution via [`tickwire-spot-execution`](crates/tickwire-spot-execution/src/lib.rs) and [`tickwire-spot-runner`](crates/tickwire-spot-runner/AGENTS.md) on [`tickwire-runtime`](crates/tickwire-runtime/AGENTS.md), with native in-place `Modify` (`/api/v5/trade/amend-order`), `StpBook`, `ProjectedFill`, `OrsQueryDesk`, and stateless REST bootstrap. |

For detailed session and bootstrap rules, see [Single-Host Trading](specs/implementation/single-host-trading.md), [Bybit & OKX Execution](specs/implementation/spot-execution.md), [Spot REST Recovery](specs/implementation/spot-rest-recovery.md), and the [Binance Reconciliation Controller](specs/implementation/reconciliation-controller.md).

---

## Quickstart

### Prerequisites

* **Operating system:** Linux (`memfd_create` file sealing, `SCM_RIGHTS` descriptor passing, `SO_PEERCRED` peer authentication, `CLOCK_BOOTTIME`, `sched_setaffinity`, `SO_BINDTODEVICE`, and optional cgroup v2 confinement).
* **Toolchain:** Rust toolchain pinned in `rust-toolchain.toml`, Python 3.11+, OpenSSL, and GNU `binutils` (`strip`). Docker (`TICKWIRE_LINUX_DOCKER=1`) and Java (JRE 11+) are optional for containerized lab suites and TLA+ model checking.

<a id="deploy-two-strategies"></a>
### 1. Deploy Two Strategies on One Shared Feed

From the repository root, launch one `tickwire-md` publisher and two supervised strategy worker processes consuming the same `memfd`-sealed broadcast ring at independent cursors:

```sh
python3 scripts/run-two-strategies.py
```

The harness starts a local TLS Bybit feed and two compiled counting workers, verifying two distinct worker PIDs and independent shared-memory ring consumption. Pass `--venue okx` to switch to OKX wire frames, `--live` to subscribe to the public exchange endpoint, or `--keep` to inspect the generated configuration files (see the [spot market launcher guide](crates/tickwire-md/examples/spot-market.md)).

### 2. Validate Static Configuration, CPU Isolation, and Machine Contracts

Verify [`config/reference.toml`](config/reference.toml), [`config/production.template.toml`](config/production.template.toml), hot vs. `housekeeping_cpus` CPU-isolation rules, multi-UID `enabled_accounts`, ABI 1.0 SHM wire vectors, catalog digests, and fixed-point risk vectors:

```sh
python3 -m venv /tmp/tickwire-contracts
/tmp/tickwire-contracts/bin/pip install -r scripts/requirements-design.txt
/tmp/tickwire-contracts/bin/python scripts/verify-design-contracts.py
cargo test --locked -p tickwire-config -p tickwire-runtime --all-targets
```

### 3. Run the Stateless 3-Process Loopback Trading Lab

Exercise the complete stateless 3-process Linux pipeline (`tickwire-md`, `tickwire-strategy-worker`, and the unified ORS process) over local loopback TLS with inline `1024B`/`512B` SHM rings, DOP session logon, and Exchange REST bootstrap:

```sh
./scripts/run-core-chain-linux.sh spot-fill release
```

See the [Linux Trading Lab guide](crates/tickwire-lab/README.md) for native host execution without Docker and detailed output verification.

---

## Develop and Measure

* **Author a strategy:** Implement `Strategy::on_market` in [`tickwire-strategy-sdk`](crates/tickwire-strategy-sdk/src/lib.rs) to inspect a borrowed `MarketView` and emit inline `IntentRecord` commands. See [`tickwire-strategy-example`](crates/tickwire-strategy-example/README.md) for a complete registered worker binary.
* **Benchmark hot paths:** Measure JSON-to-integer BBO normalization and `#![no_std]` order-book updates in isolation:
  ```sh
  cargo bench --locked -p tickwire-md --bench spot_bbo_decode
  cargo bench --locked -p tickwire-book --bench depth_apply
  ```
  When profiling end-to-end execution, record CPU model, kernel environment, core pinning, and exact stage boundaries separately across quote decode, SHM borrow, `REQ` dispatch, wire send, and `RESP` decode (see [Single-Host Trading](specs/implementation/single-host-trading.md)).

---

## Workspace Layout

All **42 Rust crates** live under `crates/`, organized into six strict ownership layers:

| Layer | Workspace Crates (42) | Responsibility |
| :--- | :--- | :--- |
| **Runtimes & Entrypoints (5)** | [`tickwire-runtime`](crates/tickwire-runtime/AGENTS.md), [`tickwire-app`](crates/tickwire-app/AGENTS.md), [`tickwire-spot-runner`](crates/tickwire-spot-runner/AGENTS.md), [`tickwire-runtime-policy`](crates/tickwire-runtime-policy/AGENTS.md), [`tickwire-lab`](crates/tickwire-lab/README.md) | Generic venue-agnostic multi-UID ORS (`REQ`/`RESP` + cold housekeeping), Binance/Bybit/OKX process entrypoints, session readiness policy, and stateless 3-process Linux lab |
| **Market Data & Books (7)** | [`tickwire-md`](crates/tickwire-md/README.md), [`tickwire-market-publisher`](crates/tickwire-market-publisher/AGENTS.md), [`tickwire-market-source`](crates/tickwire-market-source/AGENTS.md), [`tickwire-book`](crates/tickwire-book/AGENTS.md), [`tickwire-snapshot`](crates/tickwire-snapshot/AGENTS.md), [`tickwire-instrument-catalog`](crates/tickwire-instrument-catalog/AGENTS.md), [`tickwire-instrument-index`](crates/tickwire-instrument-index/AGENTS.md) | Public feed ingestion (`reactor_cpu` + `md-cold`), `#![no_std]` sequence-checked order books, sealed baseline snapshots, economic catalogs, and dense slot indexing |
| **IPC, SHM & Workers (8)** | [`tickwire-shm-sys`](crates/tickwire-shm-sys/AGENTS.md), [`tickwire-shm`](crates/tickwire-shm/AGENTS.md), [`tickwire-ipc`](crates/tickwire-ipc/AGENTS.md), [`tickwire-join`](crates/tickwire-join/AGENTS.md), [`tickwire-strategy-sdk`](crates/tickwire-strategy-sdk/AGENTS.md), [`tickwire-strategy-host`](crates/tickwire-strategy-host/AGENTS.md), [`tickwire-strategy-example`](crates/tickwire-strategy-example/README.md), [`tickwire-supervisor`](crates/tickwire-supervisor/AGENTS.md) | `memfd` mappings and seals, ABI 1.0 inline `1024B`/`512B` wire codecs, SPMC/SPSC rings, bounded RAM JOIN, worker SDK/lanes, example worker, and sealed ELF / cgroup v2 supervision |
| **Account, Risk & Gateway (5)** | [`tickwire-account`](crates/tickwire-account/AGENTS.md), [`tickwire-account-codec`](crates/tickwire-account-codec/AGENTS.md), [`tickwire-account-gateway`](crates/tickwire-account-gateway/AGENTS.md), [`tickwire-risk`](crates/tickwire-risk/AGENTS.md), [`tickwire-venue`](crates/tickwire-venue/AGENTS.md) | Multi-wallet (`AssetKey`) holds, `StpBook`, `ProjectedFill`, `QueryDesk`, snapshot codecs, `OrderRouter`, checked `i128` `Fixed` risk math, and `tw1...` `SelfDescribingClientId` domain types |
| **Protocols & Transport (11)** | [`tickwire-transport`](crates/tickwire-transport/AGENTS.md), [`tickwire-spot-execution`](crates/tickwire-spot-execution/AGENTS.md), [`tickwire-binance`](crates/tickwire-binance/AGENTS.md), [`tickwire-binance-public`](crates/tickwire-binance-public/AGENTS.md), [`tickwire-binance-private`](crates/tickwire-binance-private/AGENTS.md), [`tickwire-binance-execution`](crates/tickwire-binance-execution/AGENTS.md), [`tickwire-binance-account`](crates/tickwire-binance-account/AGENTS.md), [`tickwire-binance-clock`](crates/tickwire-binance-clock/AGENTS.md), [`tickwire-binance-quota`](crates/tickwire-binance-quota/AGENTS.md), [`tickwire-binance-sbe`](crates/tickwire-binance-sbe/AGENTS.md), [`tickwire-binance-session`](crates/tickwire-binance-session/AGENTS.md) | Nonblocking `mio` + `rustls` transport (`SO_BINDTODEVICE`, `Dialer`, HTTP/WS/FIX), Bybit/OKX V5 UTA adapters, and Binance Spot/USDⓈ-M/COIN-M/Options encoders, SBE decoders, and REST snapshot clients |
| **Clock, Affinity, Quota & Config (6)** | [`tickwire-clock-sys`](crates/tickwire-clock-sys/AGENTS.md), [`tickwire-clock`](crates/tickwire-clock/AGENTS.md), [`tickwire-quota`](crates/tickwire-quota/AGENTS.md), [`tickwire-control-sys`](crates/tickwire-control-sys/AGENTS.md), [`tickwire-control`](crates/tickwire-control/AGENTS.md), [`tickwire-config`](crates/tickwire-config/AGENTS.md) | `CLOCK_BOOTTIME` sampling, drift-bounded time intervals, shared atomic multi-IP rate limiters, `SCM_RIGHTS` + verified `sched_setaffinity` primitives, DOP control sessions, and static startup configuration validation |

---

## Documentation and Verification

* **Architecture & Module Specifications:** Start at the [Specification Index](specs/INDEX.md) and the 53-contract [Module Implementation Catalog](specs/implementation/README.md).
* **Machine Contracts & Golden Vectors:** ABI 1.0 wire layouts, CRC32C vectors, fixed-point risk cases, catalog digests, and static configuration schemas live in [`specs/contracts/`](specs/contracts/README.md).
* **Formal Verification:** Finite-state TLC and symbolic Apalache TLA+ models live in [`tla/`](tla/README.md) (see [`tla/ROADMAP.md`](tla/ROADMAP.md)).
* **Verification Suite & Contributing:** See [`scripts/README.md`](scripts/README.md) for the complete verification matrix, and [`CONTRIBUTING.md`](CONTRIBUTING.md) / [`AGENTS.md`](AGENTS.md) for engineering invariants and change workflows.
