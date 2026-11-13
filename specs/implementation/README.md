# Module Implementation Contracts

Each specification in this directory defines the state owner, input/output invariants, resource bounds, and failure semantics for one subsystem boundary. For a guided tour of how these components fit together on a single Linux host, start at the [Specification Index](../INDEX.md) and [Single-Host Trading](single-host-trading.md).

---

## 1. End-to-End Trading, Account State, and Risk

The account reducer and its gateway serialize competing strategy intents against one shared pool of capital, persist recovery closures before touching the network, and enforce frozen-by-default recovery.

| Specification | State Owner & Scope |
| :--- | :--- |
| [Single-Host Trading](single-host-trading.md) | Process topology, event-loop turn ordering, command intake gates, and end-to-end latency boundaries |
| [Account State Machine](account.md) | Single-writer multi-asset balances, Spot/USDⓈ-M reservations, cold exchange bootstrap, strategy attribution, and freeze semantics |
| [Account Gateway](account-gateway.md) | Dual-mode (`InMemory` / `MmapAsync` / `LocalDurable`) `Admit` commit, synchronous first-TLS-byte grant check, zero-byte `Aborted` proof, and reactivation |
| [Account Recovery Binding](account-recovery-binding.md) | Cryptographic binding between account admissions, historical prefixes, and verified recovery closures |
| [Risk Core](risk-core.md) | Checked `i128` fixed-point arithmetic, conservative outward rounding, margin tiers, and delivery settlement |
| [Venue Commands & Events](venue.md) | Canonical deterministic CBOR encoding, self-describing `ClientId` layout, and SHA-256 digests for trading commands and venue observations |
| [Execution Core](execution-core.md) | Venue-independent scalar order lifecycle primitives and deterministic state transitions |
| [Gateway Core](gateway.md) | Generic durable command admission paired with a guarded single-owner network sender |
| [Command Binding](command-binding.md) | Structural validation binding risk domain, account scope, and strategy context to canonical commands |
| [Conditional Orders](conditional-orders.md) | Trigger, stop, trailing, and multi-leg list (`OCO` / `OTO` / `OTOCO`) reservation and lifecycle rules |
| [Spot Amend](spot-amend.md) | In-place Spot order quantity/price amendment exposure accounting and rejection handling |
| [USDⓈ-M Account Facts](usdm-account-facts.md) | Derivative wallet funding, realized PnL, isolated margin, and external position reconciliation |
| [Runtime Policy](runtime-policy.md) | Supervisor leases, per-strategy action grants, live market freshness marks, and private-cut qualification |
| [Mock Abort](mock-abort.md) | Deterministic simulation of unsent zero-byte aborts and ordered event-stream replay |
| [Bybit & OKX Spot Execution](spot-execution.md) | V5 cash Spot request signing, CLI account inspection, private stream capture, and live account runner |
| [Shared Spot Strategy Routing](spot-strategy-routing.md) | Multi-strategy intake, immutable order-origin aliases, and per-member refusal/reply routing |
| [Spot REST Recovery](spot-rest-recovery.md) | Dual cursor-paginated REST scan verification and private WebSocket cut alignment on Bybit and OKX |
| [Reconciliation Controller](reconciliation-controller.md) | Binance terminal fee closure, periodic funding-income scans, three-snapshot verification, and grant renewal |

---

## 2. Market Data, Exchange Protocols, Time, and Transport

Public market adapters normalize venue frames into integer ticks and lots at startup-bound scales, while private and execution sessions operate under explicit I/O and rate-limit budgets.

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
| [Binance Execution](binance-execution.md) | Shared original-signature state connecting Binance REST/WS request encoding to the first-output gate |
| [Binance Private Session](binance-private-session.md) | Authenticated user-data stream lifecycle, heartbeat monitoring, and explicit `GapNotice` emission |
| [Binance Private Clock Adapter](binance-private-clock-adapter.md) | Borrowed calibrated-clock adapter validating delivery windows on incoming private venue events |
| [Binance Quota](binance-quota.md) | Typed request-weight and order-count classification layered over the local quota coordinator |
| [Binance Request Extensions](binance-request-extensions.md) | Encoding and validation for advanced Binance Spot/USDⓈ-M order parameters, lists, and amendments |
| [Binance WebSocket Trading](binance-ws-trading.md) | Session-authenticated WebSocket order placement, cancellation, and correlation on Binance |
| [Request Quota](quota.md) | Single-owner local rate-limit window allocation, lease expiry, and conservative budget consumption |
| [Clock Calibration](clock.md) | Drift-bounded server timestamp intervals (`[L, U]`) mapped onto a local monotonic clock domain |
| [System Clock](system-clock.md) | Suspend-aware native monotonic clock sampling (`CLOCK_BOOTTIME`) with permanent backward-jump latching |
| [Nonblocking Transport](transport.md) | Budgeted `mio` + `rustls` TCP/TLS driver, HTTP/WS/FIX framing codecs, and exact accepted-byte tracking |

---

## 3. Shared Memory, IPC, Process Supervision, and Strategy Runtime

Cross-process communication uses fixed-layout binary records over `memfd`-sealed shared memory and authenticated Unix domain control sockets.

| Specification | State Owner & Scope |
| :--- | :--- |
| [Supervisor Process](supervisor-process.md) | Sealed `memfd` ELF verification (`/proc/<pid>/exe`), child lifecycle tracking, and cgroup v2/CPU/NUMA limits |
| [IPC Wire Codec](ipc-codec.md) | Fixed-layout ABI 1.0 binary framing, zero-padding checks, Castagnoli CRC32C, and control/replay opcodes |
| [Market JOIN Protocol](join.md) | Nonblocking Unix `SCM_RIGHTS` handshake delivering a sealed baseline snapshot and live ring descriptor |
| [Shared-Memory Mapping](shm-mapping.md) | Linux `memfd` creation, file-seal verification (`0x17` / `0x0f`), page alignment, and atomic access bounds |
| [Shared-Memory Ring](shm-ring.md) | Single-writer SPMC/SPSC batch publication, `RecordBorrow` guards, watermark tracking, and unforgeable detach |
| [Immutable Resource](immutable-resource.md) | Read-only sealed `memfd` artifacts validated by cryptographic header and content digests |
| [Topic Snapshot](topic-snapshot.md) | Format and context validation for immutable normalized market-state baselines |
| [Topic Snapshot Builder](topic-snapshot-builder.md) | Deterministic construction and sealing of complete multi-instrument topic snapshots at cut `S` |
| [Strategy Input Consumption](strategy-input-consumption.md) | Pre-launch projection checks and live checkpoint attestation of consumed market and account streams |
| [Strategy Parameter Consumption](strategy-parameter-consumption.md) | Cryptographic binding and challenge-response proof of active worker parameter revisions |
| [Runtime Observation](runtime-observation.md) | Bounded control protocol for querying live child identity, resource confinement, and merged checkpoints |
| [Control Sessions](control-session.md) | Mutual handshake, role/epoch authentication, and idempotent request registration on local control channels |
| [Control Transport](control-transport.md) | Nonblocking Unix seqpacket/stream framing with bounded `CLOEXEC` file-descriptor receipt |

---

## 4. Persistence, Journals, and Crash Recovery

Durable subsystems separate physical byte framing from canonical business hashing so torn writes are repaired safely while mid-prefix corruption halts recovery immediately.

| Specification | State Owner & Scope |
| :--- | :--- |
| [Physical Journal](journal.md) | WAL v1 segmented framing, `InMemory` / `MmapAsync` (`MAP_SHARED` + async `fdatasync`) / `LocalDurable` stores, advisory locks, and tail repair |
| [Journal Codec](journal-codec.md) | Deterministic CBOR encoding and rolling logical hash chains for durable business events |
| [Business Journal](business-journal.md) | Synchronized business WAL pairing physical durability receipts with frozen-by-default replay |
| [Account Codec](account-codec.md) | Canonical binary encoding and strict validation for account baselines, records, and input seals |
| [Account Journal](account-journal.md) | Single-writer ordered account log, 128-byte `HistoryFrontier` SHA-256 chain, stateless live bootstrap, and ingress key deduplication |
| [Recovery Closure](recovery-closure.md) | Content-addressed DAG manifests, sequenced strategy input blocks, and `InMemory` / `AsyncFile` / `File` blob persistence |

---

## 5. Configuration Validation and Local Materialization

Configuration changes are validated against process identity and permission scopes before being atomically materialized to disk with verifiable receipts.

| Specification | State Owner & Scope |
| :--- | :--- |
| [Configuration Candidate](config-candidate.md) | Structural schema validation, permission-scoped projections, and cross-field deployment bounds |
| [Plan Archive](config-plan-archive.md) | Deterministic archive encoding and digest verification for multi-process deployment plans |
| [Configuration Locator](config-locator.md) | Canonical resolution of scoped configuration revisions, object digests, and target process paths |
| [Configuration File Store](config-file-store.md) | Descriptor-relative Linux directory/file operations (`O_NOFOLLOW`), atomic renames, and inode checks |
| [Configuration Journal Store](config-journal-store.md) | Durable storage adapter backing local configuration state transitions |
| [Configuration Local Journal](config-local-journal.md) | Four-stage state machine tracking `Prepared` and `RuntimeObserved` configuration revisions |
| [Configuration Materializer](config-materializer.md) | Atomic rendering and recovery of `desired`, `effective`, and immutable `last-good` configuration files |
