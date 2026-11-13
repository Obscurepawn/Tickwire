# Tickwire Architecture and Specifications

Tickwire's specifications define the exact ownership boundaries, state machines, wire layouts, resource bounds, and failure semantics enforced by the Rust workspace. Every observable transition has a single state owner, a machine-verifiable encoding where data crosses processes or restarts, and deterministic tests or finite-state [TLA+ models](../tla/README.md).

## Reading Roadmap

Begin with the **[Single-Host Trading Architecture](implementation/single-host-trading.md)**, which traces a public venue frame into shared memory, through an isolated strategy callback, across the durable account gateway and first-byte TLS check, and back as an ordered execution report. Compare venue capabilities in the [Exchange Support Matrix](../README.md#exchange-support).

| Subsystem | Core Specifications | Key Guarantees Verified |
| :--- | :--- | :--- |
| **Market Data & Order Books** | [Public Spot Quotes](implementation/spot-quote-input.md) · [Order Book Core](implementation/book-core.md) · [Binance Depth Sync](implementation/binance-depth-sync.md) · [Instrument Catalog](implementation/instrument-catalog.md) | Integer tick/lot normalization, zero-allocation BBO publication, venue sequence continuity, and bounded snapshot coverage |
| **Shared Memory & Multi-Strategy IPC** | [Shared-Memory Ring](implementation/shm-ring.md) · [Market JOIN](implementation/join.md) · [Supervisor Process](implementation/supervisor-process.md) · [Strategy Subscriptions](implementation/strategy-input-consumption.md) · [Parameters](implementation/strategy-parameter-consumption.md) | Sealed `memfd` rings, independent per-worker cursors, unforgeable reader detachment, ELF/cgroup isolation, and attested input consumption |
| **Orders, Risk & Account Gateway** | [Account State Machine](implementation/account.md) · [Account Gateway](implementation/account-gateway.md) · [Risk Core](implementation/risk-core.md) · [Spot Execution](implementation/spot-execution.md) · [Shared Spot Routing](implementation/spot-strategy-routing.md) | Checked fixed-point holds, self-describing `ClientId` attribution, configurable `InMemory` / `MmapAsync` / `LocalDurable` commit before send, synchronous first-byte grant recheck, and trade-ID idempotency |
| **Durability, Replay & Reconciliation** | [Physical Journal](implementation/journal.md) · [Account Journal](implementation/account-journal.md) · [Recovery Closure](implementation/recovery-closure.md) · [Binance Reconciliation](implementation/reconciliation-controller.md) · [Spot REST Recovery](implementation/spot-rest-recovery.md) | Stateless exchange-REST cold bootstrap (`InMemory`), preallocated `MAP_SHARED` + async `fdatasync` crash recovery (`MmapAsync`), `HistoryFrontier` SHA-256 chains, and remote snapshot verification |
| **Time, Quota & Local Configuration** | [Clock Calibration](implementation/clock.md) · [System Clock](implementation/system-clock.md) · [Quota](implementation/quota.md) · [Config Candidate](implementation/config-candidate.md) · [Materializer](implementation/config-materializer.md) | Suspend-aware monotonic intervals bounded by drift and RTT, single-owner rate budgets, and atomic local configuration receipts |

---

## Specification Layers

* **[Module Contracts (`specs/implementation/`)](implementation/README.md)** — Complete catalog of all 66 implementation contracts organized by architectural layer.
* **[Machine Schemas & Golden Vectors (`specs/contracts/`)](contracts/README.md)** — Language-independent JSON definitions and test vectors for IPC wire framing, fixed-point risk math, WAL framing, instrument catalogs, and deployment configurations.
* **[Formal Verification (`tla/`)](../tla/README.md)** — TLC-checked finite-state models anchored to specific crate revisions (see the [Model Roadmap](../tla/ROADMAP.md)).
* **[Verification Commands (`scripts/`)](../scripts/README.md)** — Commands to run the Rust test suite, Linux multi-process IPC checks, Python contract validators, and TLC model runs.
* **Historical Acceptance Archives** — Point-in-time release verification bundles with byte-exact SHA-256 manifests from [October 28, 2026](evidence/20261028/README.md) and [October 29, 2026](evidence/20261029/README.md).
