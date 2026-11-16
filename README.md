# Tickwire

**Tickwire** is a stateless, ultra-low-latency single-host crypto trading engine written in Rust for Linux, engineered for **deterministic `P99` and `P99.9` tail latency** from public quote ingress to venue wire transmission and fill settlement. Rather than layering trading logic onto garbage-collected runtimes, `tokio` work-stealing async schedulers, `Arc<Mutex<...>>` kernel futexes, heap-allocated `serde_json::Value` DOM trees, and local WAL/`fsync` persistence, Tickwire isolates public market-data ingestion, strategy workers, and multi-UID venue order execution into three pinned OS process roles communicating in place over `memfd`-sealed shared-memory rings—with zero local WAL, journal, disk blob files, HA-lease files, or runtime configuration patches.

---

## Why Tickwire: Ultra-Low-Latency Performance by Design

| Dimension | Typical Crypto Trading Frameworks | Tickwire Hot-Path Design |
| :--- | :--- | :--- |
| **Runtime & Scheduling** | GC pauses or `tokio` work-stealing timers, async task wakeups, and `sleep()` polling backoff | Dedicated pinned OS threads: `tickwire-md` `reactor_cpu`, worker `worker_cpu`, ORS `REQ` busy-spin (`req_cpu`), and ORS `RESP` `mio` epoll (`resp_cpu`, `epoll_wait_ms = 0`) |
| **IPC & Persistence** | Local WAL / journal `fsync`, socket copies, or heap message channels | Zero disk I/O; in-place `RecordBorrow` / `ZeroCopyRecord` (`512B` `MarketRecord` & `ReportRecord`) and `ZeroCopyIntentRecord` (`1024B` `IntentRecord`) over `128B` cache-line-isolated (`isolation_shift = 7`) sealed `memfd` rings |
| **Memory & Cache Warmth** | Steady-state heap allocations, minor page faults, and cold-LLC spikes after quiet intervals | Startup `mlock`-pinned (`MlockedSlice`) `OrderManager` and `FillRecordPool` tables, zero steady-state hot-path heap allocations, plus `BinanceHotPathWarmer`, `UtaHotPathWarmer`, and `dry_run` scratch slots |
| **Locking & Pre-Trade Risk** | Coarse `Mutex`/`RwLock` kernel futexes, `f64` risk math, and mutex-serialized STP books | Non-overlapping `LockedOrder` / `LockedConnection` TTAS spinlocks (zero logging/alloc/parsing under lock), `23 µs` stale gate, `O(1)` flat `RmsEngine` + checked `i128` `Fixed` math, and lock-free atomic BBO `OrsStpBook::self_cross` |
| **Wire Codecs & Books** | `serde_json::Value` DOM trees, `HashMap` symbol lookups, and `dyn` trait vtables | Fixed-schema Spot SBE (`tickwire-binance-sbe`), SIMD/zero-alloc Bybit & OKX decoders into `#![no_std]` integer ticks/lots (`tickwire-book`) with `O(1)` `FixedIndex` / `AppendIndex` (`DenseId`), single-pass `JsonScan`, and monomorphized `LinkOps` |
| **Routing & Recovery** | Blind round-robin sockets, blocking rate limiters, and local journal replay | `SO_BINDTODEVICE` NIC binding, `PublishedRoutes::pick` (`9/8` integer EWMA RTT + cold `Tcping`), single-pick atomic `SharedRateLimiter`, immediate `SlotState::Retired` recycling, and cold Exchange REST + `tw1...` bootstrap |

1. **Zero Disk I/O, Zero Async-Runtime Jitter & Pure Inline SHM IPC:** Zero local WAL/journal/`fsync`, zero GC, zero async task-scheduler wakeups on hot threads, and zero `sleep()` polling backoff (`REQ` busy-spins; `RESP` polls `mio` epoll with `epoll_wait_ms = 0`). Processes exchange `128B` cache-line-isolated (`isolation_shift = 7`, preventing cross-core false sharing) `512B` `MarketRecord`, `1024B` `IntentRecord`, and `512B` `ReportRecord` slots directly in place via [`RecordBorrow`](crates/tickwire-shm/AGENTS.md) (`ZeroCopyRecord`) and [`ZeroCopyIntentRecord`](crates/tickwire-ipc/AGENTS.md) over file-sealed `memfd` mappings, backed by pre-allocated SHM `Arena` depth blocks and hardware Castagnoli CRC-32C (`--features crc32`) or default zero-CRC intra-host mode.
2. **Strict Physical Core Pinning, Split `REQ`/`RESP` Data Plane & `taskset`-Escape Isolation:** Hot data-plane cores (`reactor_cpu`, `worker_cpu`, `req_cpu`, `resp_cpu`) are pairwise distinct and strictly disjoint from `housekeeping_cpus` (`req_cpu != resp_cpu`, `hot_cpus ∩ housekeeping_cpus == ∅`), enforced statically in [`tickwire-config`](crates/tickwire-config/AGENTS.md) and verified at startup via `sched_setaffinity` + mandatory `sched_getaffinity` read-back in [`tickwire-control-sys`](crates/tickwire-control-sys/AGENTS.md). Splitting `REQ` and `RESP` onto separate physical cores ensures inbound volatility fill/cancel floods on `RESP` never head-of-line block outbound order placement on `REQ`. Every cold helper (`md-cold`, `Dialer` DNS/TCP/TLS/WS-logon, `AsyncRestQuerier` / `OrsQueryDesk`, DOP `ControlServer`, `LatencyTracer` sink, and `SignalWaiter`) explicitly re-pins onto `housekeeping_cpus`—escaping inherited single-core parent `taskset` masks—with OS signals masked off hot threads.
3. **`mlock`-Pinned Pre-Allocated Pools, Zero Heap Allocation & Idle Cache Warmers:** Power-of-two [`OrderManager`](crates/tickwire-account/AGENTS.md) per-strategy slot rings (`slots_per_strat`) and the 8-way set-associative `FillRecordPool` (`2^fill_map_exponent` buckets) are pre-allocated at startup and locked into physical RAM via `mlock` (`MlockedSlice`) to eliminate minor page faults. Zero heap allocations occur on steady-state market BBO publication, `REQ` dispatch, or `RESP` frame application. [`BinanceHotPathWarmer`](crates/tickwire-binance-execution/AGENTS.md), [`UtaHotPathWarmer`](crates/tickwire-spot-execution/AGENTS.md), and per-strategy `dry_run` scratch slots keep I-cache, D-cache, branch predictors, and wire encoders warm during quiet market intervals without mutating live slots, spending quotas, or touching live venue sockets.
4. **Non-Overlapping Fine-Grained Spinlocks, Lock-Free Atomic STP & Immediate Slot Recycling:** Zero `Mutex`/`RwLock` kernel futexes sit on `REQ` or `RESP`—per-order (`LockedOrder`), per-connection (`LockedConnection`), per-symbol (`OrsStpBook` / `StpBook`), and stream-token (`GuardedValue`) TTAS spinlocks never overlap (`REQ` releases `LockedOrder` before `LockedConnection::send_frame`; `RESP` releases `LockedConnection` before entering `OrderManager`) and execute **zero logging, zero heap allocation, and zero protocol parsing** under lock. Outbound `REQ` enforces a `23 µs` (`max_order_age_ns`) stale-quote gate, direct-indexed `O(1)` flat-array [`RmsEngine`](crates/tickwire-risk/AGENTS.md) (`1s`/`5s`/`10s` rolling windows + integer parts-per-billion `FatFingerRatio` and checked `i128` `Fixed` reservations across multi-wallet `AssetKey { wallet_id, asset_id }`, zero `f32`/`f64`), and **lock-free atomic BBO `OrsStpBook::self_cross`** (`StpBook::self_cross`, reading published `AtomicI64` opposite-side best prices without locking the symbol book). Terminal orders unlink `OrsStpBook` and recycle `SlotState::Retired` immediately without waiting on asynchronous `amendment = true` fee/rebate patches (`fee_reconciled`).
5. **Fixed-Schema SBE / SIMD Market Ingress & Single-Pass `JsonScan` Wire Codec:** [`tickwire-md`](crates/tickwire-md/README.md) decodes fixed-schema Binance Spot SBE ([`tickwire-binance-sbe`](crates/tickwire-binance-sbe/src/lib.rs)) and SIMD/zero-alloc Bybit (`orderbook.1`) and OKX (`bbo-tbt`) feeds directly into integer price ticks and quantity lots inside `#![no_std]` [`tickwire-book`](crates/tickwire-book/src/lib.rs), resolving instruments in `O(1)` via [`FixedIndex` / `AppendIndex`](crates/tickwire-instrument-index/AGENTS.md) (`DenseId`). On `RESP`, single-pass zero-allocation [`JsonScan`](crates/tickwire-transport/AGENTS.md) (`MsgParser`, `BinanceMsgParser`, `BybitUtaParser`, `OkxUtaParser`) scans venue byte slices directly into typed execution facts rather than building heap `serde_json::Value` DOM trees.
6. **Monomorphized `LinkOps`, `SO_BINDTODEVICE`, `9/8` EWMA Multi-IP Routing & `rdtsc` Cycle Attribution:** Vtable-free static [`LinkOps`](crates/tickwire-transport/AGENTS.md) (`WsLink` 16-slot multiplexed `reply_id` correlation, `HttpsLink` 1-exclusive in-flight request, `FixLink` 64-slot `RefSeqNum` hints) bind directly to host NICs via `SO_BINDTODEVICE`. `PublishedRoutes::pick` (`OrderRouter` integer `9/8` EWMA RTT hysteresis with optional cold `Tcping` probing) selects a product-matched link and spends `local_ip` + account rolling/fixed buckets in a single lock-free atomic [`SharedRateLimiter`](crates/tickwire-quota/AGENTS.md) pass (emitting rising-edge `80%` `ExchRateLimit` warnings and NACKing immediately on budget exhaustion without retrying sibling links), while OKX server-maintenance notices (`64008`) trigger seamless connection drain/switchover and hardware `rdtsc` `LatencyTracer` records `MarkSite` stage deltas and spinlock `LockWait` holder/cycle attribution drained off-core.
7. **Safety & Formal Verification Without Runtime Tax:** Workspace crates default to `[workspace.lints.rust] unsafe_code = "forbid"`, confining `#![deny(unsafe_op_in_unsafe_fn)]` strictly to audited OS/clock/control primitives (`tickwire-shm-sys`, `tickwire-clock-sys`, `tickwire-control-sys`), SIMD/zero-copy wire codecs (`tickwire-ipc` `crc32c`/`zerocopy`, `tickwire-md` `simd_json`), and seal-verified `memfd` ring/JOIN attachments (`tickwire-shm`, `tickwire-join`, `tickwire-market-source`, `tickwire-strategy-sdk`, `tickwire-strategy-host`, `tickwire-app`). Compile-time `const CHECK_STP: bool` specialization (`OrderManager<CHECK_STP>`), golden wire/config/risk vectors, and 21 [TLA+](tla/README.md) model families (`40` TLC models and `11` Apalache symbolic `IndInv` models) verify lock freedom, slot reclamation, and 64-bit rate-limiter conservation at zero runtime cost.

---

## System Architecture

![Stateless 3-process Linux shared-memory architecture: tickwire-md decodes public venue feeds on hot reactor_cpu with cold md-cold housekeeping_cpus into a sealed SPMC memfd market ring; 1..M tickwire-strategy-worker processes on worker_cpu consume independent market cursors and exchange inline 1024B IntentRecord and 512B ReportRecord SPSC shared-memory rings plus DOP and Unix control sockets (DopLogin and SCM_RIGHTS memfd attach) with a unified per-venue ORS process hosting 1..N multi-UID AccountSessions, disjoint REQ spin (req_cpu) and RESP epoll (resp_cpu) hot threads, SharedRateLimiter, LinkOps WS/HTTPS/FIX with SO_BINDTODEVICE, and cold housekeeping_cpus Dialer, ControlServer, AsyncRestQuerier/OrsQueryDesk, LatencyTracer, SignalWaiter, and cold Exchange REST tw1... bootstrap.](assets/architecture.svg)

A single-host deployment separates market fan-out, strategy algorithms, and multi-UID venue execution across three stateless Linux process roles (specified in [Single-Host Trading](specs/implementation/single-host-trading.md)):

1. **Market-Data Publisher ([`tickwire-md`](crates/tickwire-md/README.md)):** Terminates public venue WebSocket, HTTPS depth-snapshot, and Spot SBE streams on `reactor_cpu` (with cold helpers pinned to `md-cold` `housekeeping_cpus`), enforces sequence continuity, and publishes normalized 512-byte `MarketRecord` slots into a sealed `memfd` SPMC ring. Workers attach via the nonblocking [JOIN protocol](specs/implementation/join.md) backed by a bounded in-memory idempotency table.
2. **Supervised Strategy Workers ([`tickwire-strategy-sdk`](crates/tickwire-strategy-sdk/src/lib.rs), [`tickwire-supervisor`](crates/tickwire-supervisor/AGENTS.md)):** Isolated child processes (`tickwire-strategy-worker`, `tickwire-counting-worker`) launched from `memfd`-sealed ELF images verified via `/proc/<pid>/exe` and confined by Linux cgroup v2, NUMA, and `worker_cpu` / `housekeeping_cpus` affinities ([Supervisor Process](specs/implementation/supervisor-process.md)). Each worker advances its own market ring cursor, maintains private strategy state, targets `account_ref = 0` (default) or `1..=255`, writes inline `1024B` `IntentRecord` commands (`New`, `Modify`, `Cancel`), and consumes ordered `512B` `ReportRecord` replies.
3. **Unified Venue ORS Process ([`tickwire-runtime`](crates/tickwire-runtime/AGENTS.md), [`tickwire-app`](crates/tickwire-app/AGENTS.md), [`tickwire-spot-runner`](crates/tickwire-spot-runner/AGENTS.md)):** `1 ORS process = 1 Venue` (`tickwire-app <inspect|run> --config PATH` via `UnifiedVenueSession`, or `tickwire-spot-runner <inspect|run> --config PATH | paper <bybit|okx> [SUMMARY_PATH]`) with zero public market-data sockets or SHM quote subscriptions, managing `1..N` multi-UID `AccountSession`s (`1 AccountSession = 1 Exchange UID`) across multi-product connection pools (`OrderRoute.product_mask`), multi-wallet `AssetKey` balances, process-wide `SharedRateLimiter`, and cold-core `ControlServer` + `Dialer` + `OrsQueryDesk`.

### Order Lifecycle and Recovery Boundaries

![Stateless ultra-low-latency order lifecycle: a 1024B Strategy IntentRecord enters REQ on req_cpu through the 23-microsecond stale gate, flat RmsEngine, lock-free atomic BBO OrsStpBook::self_cross, PublishedRoutes::pick (OrderRouter 9/8 EWMA) selection with SharedRateLimiter local_ip and account quotas, and LinkOps WS/HTTPS/FIX wire send; venue replies enter RESP on resp_cpu via single-pass JsonScan into distinct One ack (FrontAck/Ack), ProjectedFill, and FillRecordPool (Fill trade deduplication plus amendment = true fee patch) facts, immediate SlotState::Retired reclamation and OrsStpBook unlink without waiting on fee_reconciled, per-order OrsQueryDesk doubt recovery on cold housekeeping_cpus, and cold Exchange REST Account + OpenOrders tw1... bootstrap.](assets/order-lifecycle.svg)

Tickwire keeps **acknowledgement** (`FrontAck` / `Ack`), **order-status projection** (`ProjectedFill`), and **individual trade execution** (`Fill` + post-trade `amendment = true` fee/rebate patch in `FillRecordPool`) strictly distinct. Pre-wire refusals (`23 µs` stale gate, `RmsEngine`, `OrsStpBook::self_cross`, or single-pick `SharedRateLimiter` NACK) release holds immediately without retrying peer connections; single-order timeouts or disconnects isolate to `OrsQueryDesk` on `housekeeping_cpus` while sibling orders continue trading; terminal orders immediately unlink `OrsStpBook` and reclaim `SlotState::Retired` slots without waiting on `fee_reconciled`; and cold boots recover balances, positions, open holds, and `tw1...` `SelfDescribingClientId` strategy attribution directly from authenticated Exchange REST `Account + OpenOrders` (and `Positions`) snapshots.

---

<a id="exchange-support"></a>
## Exchange Support

| Venue | Public Market Data (`tickwire-md`) | Unified Stateless ORS Execution & Cold REST Recovery |
| :--- | :--- | :--- |
| **Binance** | Spot and USDⓈ-M Futures depth (`WebSocket` deltas bridged against `HTTPS` snapshots), trades, best bid/ask, mark/funding streams, and fixed-schema Spot SBE ([`tickwire-binance-sbe`](crates/tickwire-binance-sbe/src/lib.rs)). | Multi-UID stateless ORS ([`tickwire-app`](crates/tickwire-app/AGENTS.md) + [`tickwire-binance-execution`](crates/tickwire-binance-execution/AGENTS.md)) across Spot, USDⓈ-M, COIN-M, and Options over pooled REST and WebSocket-API links with `OrsStpBook`, `ProjectedFill`, `FillRecordPool` (`amendment`), `OrsQueryDesk`, and Exchange REST `Account + OpenOrders` + `tw1...` bootstrap. |
| **Bybit** | V5 Spot top-of-book (`orderbook.1`) normalized into integer ticks and lots with zero steady-state heap allocations. | V5 6-product Unified Trading Account (`Spot`, `LinearPerpetual`, `InversePerpetual`, `LinearFuture`, `InverseFuture`, `Option`) execution via [`tickwire-spot-execution`](crates/tickwire-spot-execution/src/lib.rs) and [`tickwire-spot-runner`](crates/tickwire-spot-runner/AGENTS.md) on [`tickwire-runtime`](crates/tickwire-runtime/AGENTS.md), with native in-place `Modify` (`order.amend` / `/v5/order/amend`), `TimeInForce::Rpi`, `OrsStpBook`, `ProjectedFill`, `OrsQueryDesk`, and cursor-paginated REST `Account + Positions + OpenOrders` bootstrap. |
| **OKX** | V5 Spot tick-by-tick top-of-book (`bbo-tbt`) normalized into integer ticks and lots with zero steady-state heap allocations. | V5 6-product Unified Trading Account execution via [`tickwire-spot-execution`](crates/tickwire-spot-execution/src/lib.rs) and [`tickwire-spot-runner`](crates/tickwire-spot-runner/AGENTS.md) on [`tickwire-runtime`](crates/tickwire-runtime/AGENTS.md), with native in-place `Modify` (`amend-order` / `/api/v5/trade/amend-order`), `TimeInForce::Rpi`, `OrsStpBook`, `ProjectedFill`, `OrsQueryDesk`, and stateless REST `Account + Positions + OpenOrders` bootstrap. |

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

The harness starts a local TLS Bybit feed and two compiled counting workers (`tickwire-counting-worker`), verifying two distinct worker PIDs and independent shared-memory ring consumption. Pass `--venue okx` to switch to OKX wire frames, `--live` (with optional `--proxy IP:PORT`) to subscribe to the public exchange endpoint, or `--keep` to inspect the generated configuration files (see the [spot market launcher guide](crates/tickwire-md/examples/spot-market.md)).

### 2. Validate Static Configuration, CPU Isolation, and Machine Contracts

Verify [`config/reference.toml`](config/reference.toml), [`config/production.template.toml`](config/production.template.toml), hot vs. `housekeeping_cpus` CPU-isolation rules, multi-UID `enabled_accounts`, ABI 1.0 SHM wire vectors, catalog digests, and fixed-point risk vectors:

```sh
python3 -m venv /tmp/tickwire-contracts
/tmp/tickwire-contracts/bin/pip install -r scripts/requirements-design.txt
/tmp/tickwire-contracts/bin/python scripts/verify-design-contracts.py
cargo test --locked -p tickwire-config -p tickwire-runtime --all-targets
```

### 3. Run the Stateless 3-Process Loopback Trading Lab

Exercise the complete stateless 3-process Linux pipeline (`tickwire-md`, `tickwire-strategy-worker`, and `tickwire-app`) over local loopback TLS with inline `1024B`/`512B` SHM rings, DOP session logon, Exchange REST bootstrap, `SIGUSR2` emergency cancel, and `SIGTERM` graceful shutdown (runs natively on Linux by default, or inside Docker when `TICKWIRE_LINUX_DOCKER=1`):

```sh
./scripts/run-core-chain-linux.sh spot-fill release
```

See the [Linux Trading Lab guide](crates/tickwire-lab/README.md) for manual `tickwire-lab generate` / `run` invocation and detailed output verification.

---

## Develop and Measure

* **Author a strategy:** Implement `Strategy::on_market` in [`tickwire-strategy-sdk`](crates/tickwire-strategy-sdk/src/lib.rs) to inspect a borrowed `MarketView` and emit inline `IntentRecord` commands. See [`tickwire-strategy-example`](crates/tickwire-strategy-example/README.md) for a complete registered worker binary.
* **Benchmark every hot-path stage (`P50` / `P99` / `P99.9`):** Measure each isolated data-plane boundary directly on your target host—from picosecond/nanosecond zero-copy SHM borrows and CRC-32C intrinsics to SIMD BBO decode, `#![no_std]` depth application, `O(1)` instrument lookup, and `UtaHotPathWarmer` cache residency:

  | Hot-Path Stage | Crate & Benchmark Target | Measured Boundary & Percentile Output |
  | :--- | :--- | :--- |
  | **SHM Zero-Copy Borrow** | [`tickwire-shm`](crates/tickwire-shm/benches/market_record_borrow.rs) (`market_record_borrow`) | Zero-copy `RecordBorrow::as_market_record` vs. 512-byte stack `decode_record` over a sealed Linux `memfd` bank (`P50`/`P99`/`P99.9`, picosecond resolution; supports `--features crc32`) |
  | **Wire CRC-32C & Views** | [`tickwire-ipc`](crates/tickwire-ipc/benches/crc32c_and_zerocopy.rs) (`crc32c_and_zerocopy`) | Hardware Castagnoli `crc32c` / `crc32c_masked` vs. Slicing-by-8 / bit-by-bit baselines and `MarketRecord::from_aligned_bytes` vs. `decode_record` (`P50`/`P99`/`P99.9`; supports `--features crc32`) |
  | **Spot BBO Feed Decode** | [`tickwire-md`](crates/tickwire-md/benches/spot_bbo_decode.rs) (`spot_bbo_decode`) | Bybit (`orderbook.1`) and OKX (`bbo-tbt`) raw JSON frame scan through integer tick/lot normalization and publication construction (`P50`/`P90`/`P99`/`P99.9`, `TICKWIRE_BENCH_SAMPLES`) |
  | **`#![no_std]` Book Depth** | [`tickwire-book`](crates/tickwire-book/benches/depth_apply.rs) (`depth_apply`, `batch_apply`, `sparse_apply`) | Top-of-book churn, contiguous batch updates (`forward`/`reverse`/`shuffle`), and scattered sparse-rank updates plus `best_bid()`/`best_ask()` view extraction (`P50`/`P99`/`P99.9`/`max`) |
  | **Instrument Indexing** | [`tickwire-instrument-index`](crates/tickwire-instrument-index/benches/lookup.rs) (`lookup`) | `FixedIndex` (`Gather` / `Flat`) and `AppendIndex` `O(1)` `DenseId` hit/miss latency and memory footprint vs. `HashMap`, `BTreeMap`, and binary search (`P50`/`P99`/`max`) |
  | **Idle Cache Warmer** | [`tickwire-spot-execution`](crates/tickwire-spot-execution/benches/hotpath_warmup.rs) (`hotpath_warmup`) | Cold-LLC-evicted vs. `UtaHotPathWarmer`-warmed Bybit V5 and OKX V5 UTA `REQ` + loopback TLS + `RESP` round-trip (`cold_p50`/`cold_p99` vs. `warm_p50`/`warm_p99`, zero heap alloc) |

  ```sh
  cargo bench --locked -p tickwire-shm --bench market_record_borrow
  cargo bench --locked -p tickwire-ipc --features crc32 --bench crc32c_and_zerocopy
  cargo bench --locked -p tickwire-md --bench spot_bbo_decode
  cargo bench --locked -p tickwire-book --bench depth_apply
  cargo bench --locked -p tickwire-book --bench batch_apply
  cargo bench --locked -p tickwire-book --bench sparse_apply
  cargo bench --locked -p tickwire-instrument-index --bench lookup
  cargo bench --locked -p tickwire-spot-execution --bench hotpath_warmup
  ```

  Always record CPU model, OS/kernel release, system load, feature flags (such as `--features crc32`), core pinning (`taskset -c <core>`), and exact stage boundaries alongside reported numbers, and separate one-time cold startup costs from steady-state per-quote, per-order, and per-fill costs (see [Single-Host Trading](specs/implementation/single-host-trading.md)).

---

## Workspace Layout

All **42 Rust crates** live under `crates/`, organized into six strict ownership layers:

| Layer | Workspace Crates (42) | Responsibility |
| :--- | :--- | :--- |
| **Runtimes & Entrypoints (5)** | [`tickwire-runtime`](crates/tickwire-runtime/AGENTS.md), [`tickwire-app`](crates/tickwire-app/AGENTS.md), [`tickwire-spot-runner`](crates/tickwire-spot-runner/AGENTS.md), [`tickwire-runtime-policy`](crates/tickwire-runtime-policy/AGENTS.md), [`tickwire-lab`](crates/tickwire-lab/README.md) | Generic venue-agnostic multi-UID ORS (`REQ`/`RESP` + cold housekeeping), Binance/Bybit/OKX process entrypoints, session readiness policy, and stateless 3-process Linux lab |
| **Market Data & Books (7)** | [`tickwire-md`](crates/tickwire-md/README.md), [`tickwire-market-publisher`](crates/tickwire-market-publisher/AGENTS.md), [`tickwire-market-source`](crates/tickwire-market-source/AGENTS.md), [`tickwire-book`](crates/tickwire-book/AGENTS.md), [`tickwire-snapshot`](crates/tickwire-snapshot/AGENTS.md), [`tickwire-instrument-catalog`](crates/tickwire-instrument-catalog/AGENTS.md), [`tickwire-instrument-index`](crates/tickwire-instrument-index/AGENTS.md) | Public feed ingestion (`reactor_cpu` + `md-cold`), `#![no_std]` sequence-checked order books, sealed baseline snapshots, economic catalogs, and dense slot indexing |
| **IPC, SHM & Workers (8)** | [`tickwire-shm-sys`](crates/tickwire-shm-sys/AGENTS.md), [`tickwire-shm`](crates/tickwire-shm/AGENTS.md), [`tickwire-ipc`](crates/tickwire-ipc/AGENTS.md), [`tickwire-join`](crates/tickwire-join/AGENTS.md), [`tickwire-strategy-sdk`](crates/tickwire-strategy-sdk/AGENTS.md), [`tickwire-strategy-host`](crates/tickwire-strategy-host/AGENTS.md), [`tickwire-strategy-example`](crates/tickwire-strategy-example/README.md), [`tickwire-supervisor`](crates/tickwire-supervisor/AGENTS.md) | `memfd` mappings and seals, ABI 1.0 inline `1024B`/`512B` wire codecs, SPMC/SPSC rings, bounded RAM JOIN, worker SDK/lanes, example worker, and sealed ELF / cgroup v2 supervision |
| **Account, Risk & Gateway (5)** | [`tickwire-account`](crates/tickwire-account/AGENTS.md), [`tickwire-account-codec`](crates/tickwire-account-codec/AGENTS.md), [`tickwire-account-gateway`](crates/tickwire-account-gateway/AGENTS.md), [`tickwire-risk`](crates/tickwire-risk/AGENTS.md), [`tickwire-venue`](crates/tickwire-venue/AGENTS.md) | Multi-wallet (`AssetKey`) holds, `OrsStpBook` / `StpBook`, `ProjectedFill`, `OrsQueryDesk` / `QueryDesk`, snapshot codecs, checked `i128` `Fixed` risk math & `RmsEngine`, and `tw1...` `SelfDescribingClientId` domain types |
| **Protocols & Transport (11)** | [`tickwire-transport`](crates/tickwire-transport/AGENTS.md), [`tickwire-spot-execution`](crates/tickwire-spot-execution/AGENTS.md), [`tickwire-binance`](crates/tickwire-binance/AGENTS.md), [`tickwire-binance-public`](crates/tickwire-binance-public/AGENTS.md), [`tickwire-binance-private`](crates/tickwire-binance-private/AGENTS.md), [`tickwire-binance-execution`](crates/tickwire-binance-execution/AGENTS.md), [`tickwire-binance-account`](crates/tickwire-binance-account/AGENTS.md), [`tickwire-binance-clock`](crates/tickwire-binance-clock/AGENTS.md), [`tickwire-binance-quota`](crates/tickwire-binance-quota/AGENTS.md), [`tickwire-binance-sbe`](crates/tickwire-binance-sbe/AGENTS.md), [`tickwire-binance-session`](crates/tickwire-binance-session/AGENTS.md) | Nonblocking `mio` + `rustls` transport (`SO_BINDTODEVICE`, `Dialer`, `OrderRouter` / `PublishedRoutes`, HTTP/WS/FIX), Bybit/OKX V5 UTA adapters, and Binance Spot/USDⓈ-M/COIN-M/Options encoders, SBE decoders, and REST snapshot clients |
| **Clock, Affinity, Quota & Config (6)** | [`tickwire-clock-sys`](crates/tickwire-clock-sys/AGENTS.md), [`tickwire-clock`](crates/tickwire-clock/AGENTS.md), [`tickwire-quota`](crates/tickwire-quota/AGENTS.md), [`tickwire-control-sys`](crates/tickwire-control-sys/AGENTS.md), [`tickwire-control`](crates/tickwire-control/AGENTS.md), [`tickwire-config`](crates/tickwire-config/AGENTS.md) | `CLOCK_BOOTTIME` sampling, drift-bounded time intervals, shared atomic multi-IP rate limiters (`SharedRateLimiter`), `SCM_RIGHTS` + verified `sched_setaffinity` primitives, DOP control sessions, and static startup configuration validation |

---

## Documentation and Verification

* **Architecture & Module Specifications:** Start at the [Specification Index](specs/INDEX.md) and the 53-contract [Module Implementation Catalog](specs/implementation/README.md).
* **Machine Contracts & Golden Vectors:** ABI 1.0 wire layouts (`48` layouts, `37` data message IDs, `38` control opcodes, `86` golden vectors), CRC32C vectors, fixed-point risk cases, catalog digests, and static configuration schemas live in [`specs/contracts/`](specs/contracts/README.md).
* **Formal Verification:** 21 model families (`40` finite-state TLC 2.19 models and `11` Snowcat-typed symbolic Apalache 0.63.0 `IndInv` models) live in [`tla/`](tla/README.md) (see [`tla/ROADMAP.md`](tla/ROADMAP.md)).
* **Verification Suite & Contributing:** See [`scripts/README.md`](scripts/README.md) for the complete verification matrix, and [`CONTRIBUTING.md`](CONTRIBUTING.md) / [`AGENTS.md`](AGENTS.md) for engineering invariants and change workflows.
