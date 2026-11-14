# Tickwire

**Tickwire** is a deterministic, single-host trading engine written in Rust for Linux. It isolates public market-data ingestion, strategy execution, and multi-asset risk accounting into separate OS processes connected over `memfd`-sealed shared-memory rings—allowing multiple independent strategies to trade against one shared pool of capital without lock contention, floating-point drift, or ambiguous crash states.

## Architectural Guarantees

* **Single-writer state ownership:** Every mutable domain—market-data publication ring, strategy replica, account ledger, write-ahead log, and request-quota budget—has a single owning writer. Cross-process consumers read `memfd`-sealed shared memory with explicit acquire/release watermarks and unforgeable cursor detachment.
* **Zero-allocation market hot path:** Public venue feeds are decoded once into integer price ticks and quantity lots (`#![no_std]` book core in [`tickwire-book`](crates/tickwire-book/src/lib.rs)). Steady-state Bybit and OKX top-of-book frames publish to shared memory with zero heap allocations after startup (~600–870 ns median decode-to-input latency).
* **Sub-microsecond admission, lock-free STP, and multi-connection EWMA routing:** Before any order or native `Modify` reaches the network, the account gateway checks fixed-point reservations and a lock-free integer-tick `StpBook`, commits the command's deterministic recovery closure and hash-chained `AccountJournal` record under the configured `StorageMode` (`InMemory` zero-disk default for cloud Pods, `MmapAsync` preallocated `MAP_SHARED` memcpy + background `fdatasync` at ~110 ns raw store commit, or `LocalDurable` synchronous `fsync`), routes across `1..=16` pooled multi-IP REST or multiplexed WebSocket connections via an integer EWMA `OrderRouter` with action-isolated per-IP rate limiters (`VenueRateLimiter`) and off-thread `Dialer` reconnects, and synchronously re-verifies its lease, epoch, deadline, and feed freshness immediately before writing the first plaintext byte to TLS.
* **Per-order doubt isolation, stateless cold bootstrap, and frozen crash recovery:** Outbound orders embed a 32-character `[0-9a-z]` `SelfDescribingClientId` (`tw1...`) with a 30-bit domain-keyed MAC, allowing an ephemeral cloud Pod to cold-start with zero local disk files and reconstruct open-order reservations, shared-pool (`strategy 0`) balances/positions, external lock isolation, and per-strategy `(epoch, intent_id)` sequence floors directly from authenticated exchange `Account + OpenOrders` snapshots. Single-order timeouts or mid-send disconnects isolate strictly to an off-thread per-order `QueryDesk` while sibling orders keep trading; full persistent restarts (`MmapAsync` / `LocalDurable`) start `frozen` with pending reservations intact until venue reconciliation proves remote state alignment.
* **Mechanically verified boundaries:** Workspace crates enforce `#![forbid(unsafe_code)]` outside small, audited OS/SHM primitives (`tickwire-shm-sys`, `tickwire-clock-sys`, `tickwire-control-sys`), wire layouts and risk math are cross-checked against language-independent golden vectors, and 22+ protocol boundaries are model-checked in [TLA+](tla/README.md).

---

## System Architecture

![A market-data process decodes public venue feeds once into a memfd-sealed shared-memory broadcast ring, fanning normalized records out to supervised strategy workers and a direct freshness gate to the account process. Strategy workers exchange SPSC New, Modify, and Cancel intents plus ordered reports and ProjectedFill updates with one single-writer account process, where fixed-point holds and a lock-free StpBook feed a selectable SelfDescribingClientId commit profile (InMemory default, MmapAsync, or fsync WAL) and an execution gate with first-byte grant rechecks, an EWMA OrderRouter pool, and per-IP Order, Cancel, and Query rate limiters, backed off-thread by non-blocking DNS and dialing plus a per-order QueryDesk across pooled multi-IP REST and 16-way WebSocket trading connections.](assets/architecture.svg)

A single-host deployment separates market fan-out, strategy logic, and capital authority across three process roles (mapped in detail in the [single-host trading design](specs/implementation/single-host-trading.md)):

1. **Market-Data Process ([`tickwire-md`](crates/tickwire-md/README.md)):** Terminates public venue WebSocket and HTTPS snapshot connections, enforces venue sequence continuity, and writes normalized 512-byte records (plus arena depth payloads where configured) into a Linux `memfd` broadcast ring. Late-starting workers attach via the nonblocking [JOIN protocol](specs/implementation/join.md), receiving an immutable sealed baseline snapshot and catching up before activation.
2. **Supervised Strategy Workers ([`tickwire-strategy-sdk`](crates/tickwire-strategy-sdk/src/lib.rs)):** Up to 32 isolated child processes launched from `memfd`-sealed ELF images verified via `/proc/<pid>/exe` and optionally constrained by Linux cgroup v2 memory limits, CPU affinity, and NUMA placement ([supervisor contract](specs/implementation/supervisor-process.md)). Each worker advances its own ring cursor, updates a private `MarketReplica`, emits `New`, `Modify`, and `Cancel` `CanonicalCommand` intents over an SPSC ring, and consumes ordered account reports plus zero-double-count `ProjectedFill` / `FillWatermarkTracker` updates over a return ring.
3. **Account Process ([`tickwire-app`](crates/tickwire-app/src/main.rs), [`tickwire-runtime`](crates/tickwire-runtime/src/node.rs), [`tickwire-spot-runner`](crates/tickwire-spot-runner/src/lib.rs)):** Hosts the strategy supervisor alongside the single-writer [`Account`](specs/implementation/account.md) (with preallocated windowed pools, lock-free `StpBook`, and `QueryDesk`) and [`AccountGateway`](specs/implementation/account-gateway.md) (`OrderRouter` pool + multi-IP `VenueRateLimiter`) for one risk domain. Within each nonblocking event-loop turn, the runtime polls clock calibration, market freshness, private venue fills, and in-flight REST/WebSocket responses *before* polling strategies—ensuring fills freed in the current turn update available funds before the next order is evaluated.

### Order Lifecycle and Crash Boundary

![Four-stage institutional order pipeline showing SelfDescribingClientId intake with lock-free StpBook and Modify holds, selectable InMemory, MmapAsync, or fsync WAL commit before send, EWMA routing with per-IP rate limiting and first-byte grant re-verification, and pooled REST or WebSocket wire flush with ProjectedFill and trade deduplication, splitting into three outcomes: proven zero-byte or STP/quota aborts that immediately release holds, single-order timeouts or disconnects isolated to an off-thread per-order QueryDesk while peer orders keep trading, and full process restarts that freeze new risk or cold-bootstrap tw1 attribution from exchange OpenOrders until verified.](assets/order-lifecycle.svg)

Tickwire distinguishes **acknowledgement**, **order status** (including optimistic side-effect-free `ProjectedFill` watermarks), and **individual trade execution** as separate facts. Proven `NoBytes` transport failures and local `StpBook` / quota refusals immediately release holds without burning venue rate limits; single-order post-byte disconnects or `AmendAmbiguous` outcomes lock only that order's reservation in `QueryDesk` while peer orders keep trading; and full process restarts either cold-bootstrap `tw1...` attribution from exchange `OpenOrders` (`InMemory`) or recover `frozen` from `AccountJournal` until venue reconciliation completes.

---

<a id="exchange-support"></a>
## Exchange Support

| Venue | Public Market Data | Order Execution & Account Reconciliation |
| :--- | :--- | :--- |
| **Binance** | Spot and USDⓈ-M Futures depth (`WebSocket` deltas bridged against `HTTPS` snapshots), trades, best bid/ask, and fixed-schema Spot SBE ([`tickwire-binance-sbe`](crates/tickwire-binance-sbe/src/lib.rs)). | File-configured runtime ([`tickwire-app`](crates/tickwire-app/src/main.rs)) routes pooled REST and multiplexed WebSocket `New`, `Modify` (`PUT /fapi/v1/order`, `order.modify`, Spot `cancelReplace`), and `Cancel` intents with multi-IP `VenueRateLimiter`, `StpBook`, `ProjectedFill`, per-order `QueryDesk`, terminal fee/funding closure, and live or WAL reconciliation. |
| **Bybit** | V5 Spot top-of-book (`orderbook.1`) normalized into integer ticks and lots with zero steady-state heap allocations. | V5 Spot limit/market orders, native in-place `Modify` (`/v5/order/amend`), cancellations, and queries via [`tickwire-spot-execution`](crates/tickwire-spot-execution/src/lib.rs); dedicated cash-account runner ([`tickwire-spot-runner`](crates/tickwire-spot-runner/src/lib.rs)) drives pooled multi-IP REST, `StpBook`, `ProjectedFill`, per-order `QueryDesk`, and dual cursor-paginated REST reconciliation. |
| **OKX** | V5 Spot tick-by-tick top-of-book (`bbo-tbt`) normalized into integer ticks and lots with zero steady-state heap allocations. | V5 cash-mode Spot limit/market orders, native in-place `Modify` (`/api/v5/trade/amend-order`), cancellations, and queries via [`tickwire-spot-execution`](crates/tickwire-spot-execution/src/lib.rs); dedicated cash-account runner drives pooled multi-IP REST, `StpBook`, `ProjectedFill`, per-order `QueryDesk`, and dual REST reconciliation. |

For live account setup and recovery semantics, see [Bybit and OKX spot execution](specs/implementation/spot-execution.md#run-an-account), [supported spot recovery profiles](specs/implementation/spot-rest-recovery.md), and the [Binance reconciliation controller](specs/implementation/reconciliation-controller.md).

---

## Quickstart

### Prerequisites

* **Operating system:** Linux (uses `memfd_create` file sealing, `SCM_RIGHTS` descriptor passing, `SO_PEERCRED` peer authentication, `CLOCK_BOOTTIME`, and optional cgroup v2 resource domains).
* **Toolchain:** Rust toolchain pinned in `rust-toolchain.toml`, Python 3.11+, OpenSSL, and GNU `binutils` (`strip`). Docker and Java (JRE) are used only for isolated Linux container suites and TLA+ model checking.

<a id="deploy-two-strategies"></a>
### 1. Deploy Two Strategies on One Shared Feed

From the repository root, launch one market-data publisher and two supervised strategy worker processes consuming the same shared-memory ring at independent cursors:

```sh
python3 scripts/run-two-strategies.py
```

The harness starts a local TLS Bybit feed and two compiled counting workers. Inspect the JSON output for `"two_strategies_observed": true`, two distinct worker PIDs, and `"quote_sequence": 1000000` recorded in both worker checkpoints. Pass `--venue okx` to switch to OKX wire frames, `--live` to subscribe to the public exchange endpoint, or `--keep` to inspect the generated configuration files (documented in the [spot market launcher guide](crates/tickwire-md/examples/spot-market.md)).

### 2. Run a Deterministic Order Roundtrip and Crash Recovery

Exercise order admission, fixed-point reservation, partial fill accounting, cancellation, and frozen-on-restart journal replay against the in-memory simulated venue:

```sh
cargo run --locked -p tickwire-mock --example spot-roundtrip
```

The output reports `submit_calls=1 spent=101 held=1 recovered_frozen=true`, confirming that reopening the write-ahead log restores uncommitted exposure and holds the account frozen until reconciliation.

### 3. Route Competing Orders Through a Native Spot Adapter

Run two independent strategy producers drawing against a single shared cash balance over local loopback TLS using the native Bybit or OKX V5 protocol encoder:

```sh
cargo run --locked -p tickwire-spot-runner -- paper okx "$(mktemp -d)/account"
```

The summary prints `"native_acknowledgements": 2` and `"open_orders": 2`, with both orders reserving a combined `120` USDT from the shared account.

---

## Develop and Measure

* **Author a strategy:** Implement `Strategy::on_market` in [`tickwire-strategy-sdk`](crates/tickwire-strategy-sdk/src/lib.rs) to inspect a borrowed `MarketView` and optionally emit a `CanonicalCommand`. See the [registered counting worker](crates/tickwire-strategy-example/README.md) for a complete executable example with parameter and checkpoint verification.
* **Benchmark hot paths:** Time native JSON-to-integer BBO decoding in isolation:
  ```sh
  cargo bench --locked -p tickwire-md --bench spot_bbo_decode
  ```
  Measure order-book delta application with `cargo bench -p tickwire-book --bench depth_apply`. When profiling end-to-end execution, measure strategy callback, gateway candidate admission, WAL `fsync`, and TLS first-byte latency as separate intervals (see [latency boundaries](specs/implementation/single-host-trading.md)).
* **Run full Linux process scenarios:** Trace a complete Binance spot order through local TLS, multi-process SHM JOIN, WAL persistence, and fee reconciliation in the [Linux trading lab](crates/tickwire-lab/README.md).

---

## Workspace Layout

All 55 Rust crates live under `crates/`, organized into seven strict ownership layers:

| Layer | Key Crates | Responsibility |
| :--- | :--- | :--- |
| **Runtimes & Entrypoints** | [`tickwire-app`](crates/tickwire-app/AGENTS.md), [`tickwire-runtime`](crates/tickwire-runtime/AGENTS.md), [`tickwire-spot-runner`](crates/tickwire-spot-runner/AGENTS.md), [`tickwire-runtime-policy`](crates/tickwire-runtime-policy/AGENTS.md), [`tickwire-lab`](crates/tickwire-lab/README.md), [`tickwire-mock`](crates/tickwire-mock/AGENTS.md) | Deployment inspection/launch/replay, budgeted single-thread event loop, Bybit/OKX account runner, live trading grants, loopback lab, and deterministic simulation |
| **Market Data & Books** | [`tickwire-md`](crates/tickwire-md/README.md), [`tickwire-market-publisher`](crates/tickwire-market-publisher/AGENTS.md), [`tickwire-market-source`](crates/tickwire-market-source/AGENTS.md), [`tickwire-book`](crates/tickwire-book/AGENTS.md), [`tickwire-snapshot`](crates/tickwire-snapshot/AGENTS.md), [`tickwire-instrument-catalog`](crates/tickwire-instrument-catalog/AGENTS.md), [`tickwire-instrument-index`](crates/tickwire-instrument-index/AGENTS.md) | Public feed ingestion, `no_std` sequence-checked order books, sealed baseline snapshots, versioned economic catalogs, and dense slot indexing |
| **IPC, SHM & Workers** | [`tickwire-shm-sys`](crates/tickwire-shm-sys/AGENTS.md), [`tickwire-shm`](crates/tickwire-shm/AGENTS.md), [`tickwire-ipc`](crates/tickwire-ipc/AGENTS.md), [`tickwire-join`](crates/tickwire-join/AGENTS.md), [`tickwire-strategy-sdk`](crates/tickwire-strategy-sdk/AGENTS.md), [`tickwire-strategy-host`](crates/tickwire-strategy-host/AGENTS.md), [`tickwire-supervisor`](crates/tickwire-supervisor/AGENTS.md), [`tickwire-release`](crates/tickwire-release/AGENTS.md) | `memfd` mappings and seals, fixed-layout wire ABI, SPMC/SPSC rings, JOIN state machine, worker SDK, sealed ELF supervision, and cgroup/CPU enforcement |
| **Account, Risk & Gateway** | [`tickwire-account`](crates/tickwire-account/AGENTS.md), [`tickwire-risk`](crates/tickwire-risk/AGENTS.md), [`tickwire-venue`](crates/tickwire-venue/AGENTS.md), [`tickwire-execution`](crates/tickwire-execution/AGENTS.md), [`tickwire-gateway`](crates/tickwire-gateway/AGENTS.md), [`tickwire-account-gateway`](crates/tickwire-account-gateway/AGENTS.md) | Multi-asset reservations and attribution, checked `i128` fixed-point math, canonical CBOR commands/events, durable admission, and first-byte gate |
| **Protocols & Transport** | [`tickwire-transport`](crates/tickwire-transport/AGENTS.md), [`tickwire-spot-execution`](crates/tickwire-spot-execution/AGENTS.md), [`tickwire-binance`](crates/tickwire-binance/AGENTS.md) (`-public`, `-private`, `-execution`, `-account`, `-clock`, `-quota`, `-sbe`, `-session`) | Budgeted nonblocking `mio` + `rustls` transport (HTTP, WebSocket, FIX) and venue-native protocol encoders, decoders, and reconciliation clients |
| **Journals & Recovery** | [`tickwire-journal`](crates/tickwire-journal/AGENTS.md), [`tickwire-journal-codec`](crates/tickwire-journal-codec/AGENTS.md), [`tickwire-business-journal`](crates/tickwire-business-journal/AGENTS.md), [`tickwire-account-codec`](crates/tickwire-account-codec/AGENTS.md), [`tickwire-account-journal`](crates/tickwire-account-journal/AGENTS.md), [`tickwire-recovery`](crates/tickwire-recovery/AGENTS.md) | WAL v1 physical framing and batch sync, `HistoryFrontier` hash-chained account logs, and content-addressed recovery-closure DAGs |
| **Clock, Quota & Config** | [`tickwire-clock-sys`](crates/tickwire-clock-sys/AGENTS.md), [`tickwire-clock`](crates/tickwire-clock/AGENTS.md), [`tickwire-quota`](crates/tickwire-quota/AGENTS.md), [`tickwire-control-sys`](crates/tickwire-control-sys/AGENTS.md), [`tickwire-control`](crates/tickwire-control/AGENTS.md), [`tickwire-control-store`](crates/tickwire-control-store/AGENTS.md), [`tickwire-config`](crates/tickwire-config/AGENTS.md) (`-fs`, `-journal`, `-materializer`) | Suspend-aware monotonic clocks, drift-bounded server time intervals, rate-limit budgets, `SCM_RIGHTS` control channels, and atomic local config materialization |

---

## Documentation and Verification

* **Design & Module Specifications:** Start at the [Specification Index](specs/INDEX.md) and the full [Module Contract Catalog](specs/implementation/README.md).
* **Machine Contracts & Golden Vectors:** Wire offsets, CRC32C vectors, fixed-point risk cases, and configuration schemas live in [`specs/contracts/`](specs/contracts/README.md).
* **Formal Verification:** Finite-state TLA+ models and TLC coverage audits live in [`tla/`](tla/README.md) (model index in [`tla/ROADMAP.md`](tla/ROADMAP.md)).
* **Verification Suite & Contributing:** See [`scripts/README.md`](scripts/README.md) for the full test, Linux IPC, contract, and TLC commands, and [`CONTRIBUTING.md`](CONTRIBUTING.md) / [`AGENTS.md`](AGENTS.md) for engineering invariants and change workflows.
