# Module Implementation Contracts

Each specification in this directory defines the state owner, thread/spinlock discipline, input/output invariants, resource bounds, and failure semantics for one subsystem boundary, maintained in strict Code-Doc Lockstep with its crate's `AGENTS.md` and `//!` rustdoc. For a guided overview of how the pure stateless 3-process SHM architecture fits together on a single Linux host (`tickwire-md`, `tickwire-strategy-worker`, and the unified `1 ORS = 1 Venue` execution process), start at the [Specification Index](../INDEX.md) and [Single-Host Trading](single-host-trading.md).

## Architectural Baseline Invariants

1. **Pure Stateless 3-Process SHM Topology:**
   * **`tickwire-md`:** Sole owner of public venue feeds and `memfd` SHM ring broadcast, with optional `reactor_cpu` and cold `housekeeping_cpus` (`md-cold`) thread pinning plus a bounded in-memory JOIN idempotency table (zero WAL).
   * **`tickwire-strategy-worker` / `tickwire-strategy-sdk`:** Reads `tickwire-md` SHM rings at independent cursors and exchanges inline `IntentRecord` (`1024B`) and `ReportRecord` (`512B`) slots over SHM rings with ORS (zero `FileQueue`, zero `FileBlobStore`, optional `worker_cpu` + `housekeeping_cpus`).
   * **Unified ORS Execution Process:** `1 ORS process = 1 Venue`, with zero public market-data connections or SHM quote subscriptions, managing `1..N` `AccountSession`s (`account_ref = 1..255`, `0` = first authorized account; `1 AccountSession = 1 Exchange UID`) across multi-product connection pools (`OrderRoute.product_mask`), multi-wallet/UTA `AssetKey { wallet_id, asset_id }`, a shared process-wide atomic IP/account `RateLimiter`, and per-account `QueryDesk`.
2. **Stateless Cold Startup & Exchange-Snapshot Recovery:** Static startup configuration only (zero WAL, zero `AccountJournal`, zero `RecoveryClosure`, zero HA-lease rotation, zero runtime config-patch). Cold boot and process restart recover exclusively from authenticated Exchange REST snapshots (`Account + OpenOrders`) and 32-character `[0-9a-z]` `SelfDescribingClientId` (`tw1...` with domain-keyed MAC).
3. **Thread Affinity & Fine-Grained Spinlock Discipline:** Dedicated `REQ` spin thread (`req_cpu`) and `RESP` epoll thread (`resp_cpu`, `epoll_wait_ms = 0` configurable) are strictly disjoint from cold `housekeeping_cpus` (`Dialer`, `AsyncRestQuerier`/`QueryDesk`, `LatencyTracer` sink, `ControlServer`, `main` `sigtimedwait`), enforced via audited `tickwire-control-sys` (`sched_setaffinity` + mandatory `sched_getaffinity` read-back verification, `hot_cpus ∩ housekeeping_cpus == ∅`). Fine-grained spinlocks (connection, order, `StpBook`, `GuardedValue` `listenKey`) follow a strict non-overlapping hierarchy (ascending `seqnum` order if multiple orders are ever touched) with zero logging, zero heap allocation, and zero protocol parsing under any spinlock.
4. **Arithmetic, Memory Safety & Formal Verification:** Checked integer ticks, lots, and `Fixed` decimals across all financial and wire paths (zero floats); `#![forbid(unsafe_code)]` workspace-wide outside audited `#![deny(unsafe_op_in_unsafe_fn)]` boundaries (`tickwire-shm-sys`, `tickwire-clock-sys`, `tickwire-control-sys`, and `tickwire-ipc` `crc32c`/`zerocopy`); and TLC + Apalache formal verification under [`tla/`](../../tla/README.md).

---

## 1. Unified ORS Execution, Account Sessions, and Risk

The unified ORS process routes strategy intents (`strat_id = 0..=254`, `account_ref = 0..=255`) across `1..N` venue `AccountSession`s, enforcing checked fixed-point reservations, self-trade prevention, and stateless exchange-REST bootstrap without local WAL files.

| Specification | State Owner & Scope |
| :--- | :--- |
| [Single-Host Trading](single-host-trading.md) | Stateless 3-process SHM topology, hot `REQ`/`RESP` vs. `housekeeping_cpus` isolation, intake gates, and end-to-end latency boundaries |
| [Account State Machine](account.md) | Multi-wallet (`AssetKey`) balances, Spot/derivatives reservations, `StpBook`, native `Modify`, `ProjectedFill`, per-account `QueryDesk`, stateless REST bootstrap, and `tw1...` attribution |
| [Account Gateway](account-gateway.md) | `AccountSession` dispatch, pooled `OrderRouter` (`OrderRoute.product_mask`), doubt isolation via `QueryDesk`, and synchronous pre-send checks |
| [Account Codec](account-codec.md) | Canonical binary encoding and strict validation for in-memory account baselines and snapshot records |
| [Risk Core](risk-core.md) | Checked `i128` fixed-point arithmetic, conservative outward rounding, margin tiers, and delivery settlement |
| [Venue Commands & Events](venue.md) | Multi-product `OrderRoute`/`AssetKey`, `SelfDescribingClientId` (`tw1...`), `Rpi`, `FrontAck`, `Fill` (`amendment`), `TerminalReason`s, and canonical digests |
| [Command Binding](command-binding.md) | Structural validation binding account scope (`account_ref`) and strategy context (`strat_id`) to trading commands |
| [Conditional Orders](conditional-orders.md) | Trigger, stop, trailing, and multi-leg list (`OCO` / `OTO` / `OTOCO`) reservation and lifecycle rules |
| [Spot Amend](spot-amend.md) | In-place Spot order quantity/price amendment exposure accounting and rejection handling |
| [USDⓈ-M Account Facts](usdm-account-facts.md) | Derivative wallet funding, realized PnL, isolated margin, and external position reconciliation |
| [Runtime Policy](runtime-policy.md) | Strategy session state, action permissions, market connectivity checks, and trading readiness |
| [Bybit & OKX Spot Execution](spot-execution.md) | V5 cash Spot request signing, account inspection, private stream handling, and stateless account execution |
| [Shared Spot Strategy Routing](spot-strategy-routing.md) | Multi-strategy DOP intake, `account_ref` routing, order-origin attribution, and per-strategy refusal/reply delivery |
| [Spot REST Recovery](spot-rest-recovery.md) | Stateless cursor-paginated REST `Account + OpenOrders` bootstrap and private WebSocket alignment on Bybit and OKX |
| [Reconciliation Controller](reconciliation-controller.md) | Binance stateless REST `Account + OpenOrders` bootstrap, terminal fee closure, and periodic funding-income scans |

---

## 2. Market Data, Exchange Protocols, Time, and Transport

`tickwire-md` normalizes public venue frames into integer ticks and lots at startup-bound scales, while venue execution sessions encode/decode frames and spend shared atomic IP/account rate-limit budgets.

| Specification | State Owner & Scope |
| :--- | :--- |
| [Public Spot Quote Input](spot-quote-input.md) | Zero-allocation Bybit (`orderbook.1`) and OKX (`bbo-tbt`) BBO decoding and paired feed-state publication |
| [Order Book Core](book-core.md) | Bounded `#![no_std]` price-level storage, Spot/USDⓈ-M sequence continuity, and snapshot coverage bounds |
| [Book State Model](book-state-model.md) | Lifecycle transitions across `AwaitingSnapshot`, `Bridging`, `Live`, and `Invalid` generations |
| [Instrument Catalog](instrument-catalog.md) | Versioned asset and contract economics, tick/step increments, canonical bytes, and semantic digests |
| [Instrument Index](instrument-index.md) | Bounded dense slot lookup scoping compact runtime IDs to an approved catalog instance |
| [Market Expansion](market-expansion.md) | Online topic membership expansion, coordinated reader quiescence, and successor bank cutover |
| [Binance Protocol](binance-protocol.md) | Request signing, response classification, error mapping, and deterministic decimal formatting for Binance |
| [Binance Streams](binance-streams.md) | Normalization of Binance Spot and USDⓈ-M public WebSocket depth, trade, and ticker frames |
| [Binance Depth Sync](binance-depth-sync.md) | Coordinator bridging buffered WebSocket depth deltas against HTTPS snapshots across connection epochs |
| [Binance Public Session](binance-public-session.md) | Budgeted nonblocking public market-data session driver and reconnect lifecycle |
| [Binance Market Events](binance-market-events.md) | Publication of normalized trades, exact BBOs, and mark/funding updates onto shared-memory topics |
| [Binance Spot SBE](binance-sbe.md) | Fixed-schema Simple Binary Encoding (SBE) market-data decoding without dynamic reflection or allocation |
| [Binance Execution](binance-execution.md) | Binance REST/WS request encoding, signing, and session state across Spot and derivatives endpoints |
| [Binance Private Session](binance-private-session.md) | Authenticated user-data stream lifecycle, `GuardedValue` `listenKey` refresh, heartbeat monitoring, and `GapNotice` emission |
| [Binance Private Clock Adapter](binance-private-clock-adapter.md) | Borrowed calibrated-clock adapter validating delivery windows on incoming private venue events |
| [Binance Quota](binance-quota.md) | Typed request-weight and order-count classification layered over the shared atomic quota coordinator |
| [Binance Request Extensions](binance-request-extensions.md) | Encoding and validation for advanced Binance Spot/USDⓈ-M order parameters, lists, and amendments |
| [Binance WebSocket Trading](binance-ws-trading.md) | Session-authenticated WebSocket order placement, cancellation, and correlation on Binance |
| [Request Quota](quota.md) | Shared atomic multi-IP/account rate-limit window tracking, ceiling enforcement, and conservative budget consumption |
| [Clock Calibration](clock.md) | Drift-bounded server timestamp intervals (`[L, U]`) mapped onto a local monotonic clock domain |
| [System Clock](system-clock.md) | Suspend-aware native monotonic clock sampling (`CLOCK_BOOTTIME`) with permanent backward-jump latching |
| [Nonblocking Transport](transport.md) | Budgeted `mio` + `rustls` TCP/TLS driver, off-thread `Dialer` handoff, and HTTP/WS/FIX framing codecs |

---

## 3. Shared Memory, IPC, Process Supervision, Thread Affinity, and Strategy Runtime

Cross-process communication uses fixed-layout inline binary records (`1024B` `IntentRecord`, `512B` `ReportRecord`, and `512B` `MarketRecord`) over `memfd`-sealed shared memory plus DOP/control sessions, with audited Linux thread CPU affinity verification.

| Specification | State Owner & Scope |
| :--- | :--- |
| [Supervisor Process](supervisor-process.md) | Sealed `memfd` ELF verification (`/proc/<pid>/exe`), child lifecycle tracking, cgroup v2/NUMA limits, and read-back verified thread CPU affinity (`CpuSet`, `pin_current_thread`, `spawn_pinned_thread`, `validate_cpu_isolation`) |
| [IPC Wire Codec](ipc-codec.md) | Fixed-layout ABI 1.0 inline `IntentRecord` (`1024B`) & `ReportRecord` (`512B`) slots, market records, DOP logon frames (`DopLogin` / `DopEcho` / `OrsSessions`), zero-copy views, and Castagnoli CRC32C |
| [Market JOIN Protocol](join.md) | Nonblocking Unix `SCM_RIGHTS` handshake with bounded in-memory idempotency table delivering a sealed baseline snapshot and live ring descriptor |
| [Shared-Memory Mapping](shm-mapping.md) | Linux `memfd` creation, file-seal verification (`0x17` / `0x0f`), page alignment, and atomic access bounds |
| [Shared-Memory Ring](shm-ring.md) | Single-writer SPMC/SPSC inline slot publication, optional SHM arena descriptors, `RecordBorrow` guards, watermark tracking, and unforgeable detach |
| [Immutable Resource](immutable-resource.md) | Read-only sealed `memfd` artifacts validated by cryptographic header and content digests |
| [Topic Snapshot](topic-snapshot.md) | Format and context validation for immutable normalized market-state baselines |
| [Topic Snapshot Builder](topic-snapshot-builder.md) | Deterministic construction and sealing of complete multi-instrument topic snapshots at cut `S` |
| [Strategy Input Consumption](strategy-input-consumption.md) | Pre-launch projection checks and live checkpoint attestation of consumed market and report streams |
| [Strategy Parameter Consumption](strategy-parameter-consumption.md) | Cryptographic binding and challenge-response proof of active static worker parameter projections |
| [Runtime Observation](runtime-observation.md) | Bounded control protocol for querying live child identity, resource confinement, and merged checkpoints |
| [Control Sessions](control-session.md) | Mutual handshake, DOP logon/session state, role authentication, and bounded in-memory request tracking on control channels |
| [Control Transport](control-transport.md) | Nonblocking Unix seqpacket/stream framing with bounded `CLOEXEC` file-descriptor receipt |

---

## 4. Static Configuration Validation and Deployment Planning

Deployment configurations are immutable static startup inputs validated against schema, host CPU/NUMA topology, and least-privilege process projections without runtime config-patch mutation or local configuration journals.

| Specification | State Owner & Scope |
| :--- | :--- |
| [Configuration Candidate](config-candidate.md) | Static JSON/TOML parsing, `system-config.schema.json` validation, permission-scoped `ProjectionDelivery`, `CapacityAssessment`, and `ComputedPlan` synthesis |
| [Plan Archive](config-plan-archive.md) | Deterministic archive encoding, strict re-decoding, and 17-input `rules_digest()` provenance verification for deployment plans |
| [Configuration Locator](config-locator.md) | Canonical bootstrap pointer validation and cryptographic root-migration chaining |
