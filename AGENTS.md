# Engineering Guide and Invariants (`AGENTS.md`)

Start with the [shared-feed walkthrough](README.md#deploy-two-strategies), the [specification index](specs/INDEX.md), and [`CONTRIBUTING.md`](CONTRIBUTING.md). Before modifying any crate under `crates/`, read its local `AGENTS.md` and governing specification in [`specs/implementation/`](specs/implementation/README.md) to confirm which single writer or lock domain owns each state transition. All workspace crates, examples, and integration tests build against the pinned Rust toolchain (`rust-toolchain.toml`) and `Cargo.lock`.

## 1. Pure Stateless 3-Process SHM Topology & State Ownership

Tickwire runs a pure stateless 3-process shared-memory (`memfd`-sealed) production topology on Linux with zero local WAL, journal, blob-store, HA-lease, or dynamic config-patch state:

* **Market-data publisher (`tickwire-md`):** Sole owner of exchange public market-data feeds, instrument bindings, sequence continuity rules, and shared-memory ring broadcast. Supports optional hot `reactor_cpu` pinning alongside cold background thread `housekeeping_cpus` pinning (`md-cold`) and a bounded in-memory JOIN idempotency table (zero WAL).
* **Strategy workers (`tickwire-strategy-worker` / `tickwire-strategy-sdk`):** Each worker reads `tickwire-md` SHM broadcast rings at an independent cursor, maintains private algorithm state, and exchanges inline `IntentRecord` (`1024B`) and `ReportRecord` (`512B`) slots over SHM rings with the ORS execution process (zero `FileQueue`, zero `FileBlobStore`, optional `worker_cpu` + `housekeeping_cpus` pinning).
* **Unified ORS execution process (`1 ORS process = 1 Venue`):** Holds zero public market-data connections and zero SHM quote subscriptions. Manages `1..N` `AccountSession`s (`account_ref = 1..255`, with `0` routing to the strategy's first authorized account; `1 AccountSession = 1 Exchange UID`) across multi-product connection pools (`OrderRoute.product_mask`), multi-wallet / unified-trading-account balances (`AssetKey { wallet_id, asset_id }`), a shared process-wide atomic IP/account `RateLimiter`, and a per-account `QueryDesk`.
* **Stateless cold startup & exchange-snapshot recovery:** Processes load immutable static startup configuration only. Cold boot and restart recover exclusively from authenticated Exchange REST snapshots (`Account + OpenOrders`) paired with the 32-character `[0-9a-z]` `SelfDescribingClientId` (`tw1...` with domain-keyed MAC), reconstructing open-order holds, strategy attribution, and per-strategy sequence floors without prior local disk files. Keep front/order acknowledgements (`FrontAck` / `Ack`), order-status projections (`ProjectedFill`), and individual trade executions (`Fill`, including post-trade `amendment` fee/rebate updates) as distinct facts deduplicated by trade ID.

## 2. Thread & CPU Affinity Isolation and Lock Discipline

* **Disjoint hot vs. housekeeping cores:** In the ORS execution process, a dedicated `REQ` spin thread (`req_cpu`) polls outbound strategy intents while a dedicated `RESP` epoll thread (`resp_cpu`, `epoll_wait_ms = 0` configurable) drives venue sockets and writes `ReportRecord` replies. Both hot cores are strictly disjoint from cold `housekeeping_cpus` (`Dialer`, `AsyncRestQuerier` / `QueryDesk`, `LatencyTracer` sink, `ControlServer`, and `main` `sigtimedwait`), enforced at startup via [`tickwire-control-sys`](crates/tickwire-control-sys/AGENTS.md) (`sched_setaffinity` followed by mandatory `sched_getaffinity` read-back verification, requiring pairwise-distinct hot CPUs and `hot_cpus ∩ housekeeping_cpus == ∅`).
* **Fine-grained spinlocks & strict hierarchy:** Critical-path synchronization uses fine-grained spinlocks per connection, per order, per symbol (`StpBook`), and on `GuardedValue` stream tokens (`listenKey`)—never coarse process-wide mutexes:
  * Never overlap `Order` and `Connection` spinlocks (`REQ` releases the order lock before acquiring the connection lock in `sendFrame`; `RESP` releases one before taking the other).
  * Acquire `StpBook` only while already holding the caller's single `Order` lock (the send-path self-trade check reads the book's published atomic best price without locking `StpBook`); if an admin/sweep path ever touches multiple orders, acquire strictly in ascending `seqnum` order and prefer one-order-at-a-time lock-and-release scans.
  * Perform **zero logging, zero heap allocation, and zero protocol parsing** while holding any spinlock.
* **Cold startup vs. hot path:** Parse static configuration, seal worker executables, bind cgroups and thread CPU affinities, resolve catalogs, dial/TLS-handshake connections (`Dialer`), and authenticate DOP control peers off the hot path—never inside quote decoding, `REQ` order dispatch, or `RESP` frame application.

## 3. Code-Doc Lockstep, Arithmetic, Memory Safety, and Formal Verification

1. **Code-Doc Lockstep (non-negotiable):** Before or alongside any behavioral or API change, update the governing specification in [`specs/implementation/`](specs/implementation/README.md), the crate's local `AGENTS.md`, and its crate-root `//!` rustdoc in lockstep. Register structural additions in [`specs/INDEX.md`](specs/INDEX.md) and [`specs/implementation/README.md`](specs/implementation/README.md). All prose, comments, specifications, and commit messages must be written in clear, idiomatic English (zero CJK characters).
2. **Synchronize machine contracts and formal models:**
   * When SHM IPC wire framing, configuration schemas, catalog digests, or fixed-point risk rules change, update the corresponding [machine contract and golden vectors](specs/contracts/README.md) alongside its `scripts/verify-*.py` validator.
   * When event ordering, spinlock discipline, admission gating, shared-memory reclamation, or stateless exchange-REST recovery changes, inspect [`tla/`](tla/README.md) and run TLC finite-state model checking and Apalache symbolic/inductive verification.
3. **Exact fixed-point arithmetic & cost accounting:** Use integer price ticks, quantity lots, and checked fixed-point decimals (`Fixed`) across all risk, routing, and execution paths; floating-point arithmetic is forbidden in financial and wire logic. Separate startup costs from per-quote, per-order, and per-fill costs; count copies, allocations, queue hops, clock reads, and syscalls explicitly; and always record CPU model, OS/kernel environment, load, and measurement boundaries alongside benchmark numbers.
4. **Audited `unsafe` isolation:** Workspace crates default to `#![forbid(unsafe_code)]`. Encapsulate OS and zero-copy shared-memory primitives behind sound safe APIs with `#![deny(unsafe_op_in_unsafe_fn)]` and documented safety invariants strictly inside the audited boundaries: [`tickwire-shm-sys`](crates/tickwire-shm-sys/AGENTS.md), [`tickwire-clock-sys`](crates/tickwire-clock-sys/AGENTS.md), [`tickwire-control-sys`](crates/tickwire-control-sys/AGENTS.md), and [`tickwire-ipc`](crates/tickwire-ipc/AGENTS.md) (`crc32c` and `zerocopy`).

## 4. Verification, Documentation, and Commits

Run the core verification suite before committing any change:

```sh
cargo fmt --all -- --check
cargo test --locked -p <affected-crate> --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
python3 scripts/verify-documentation.py
```

* Run the full workspace test suite before committing (`./scripts/check-rust.sh`).
* For shared-memory, `memfd`, cgroup, thread-affinity, or `SCM_RIGHTS` changes, run `./scripts/check-linux.sh`.
* For ordering, lock-hierarchy, admission, or recovery changes, run `./scripts/check-tla.sh`.
* For IPC wire, configuration, instrument catalog, or risk changes, run `python3 scripts/verify-design-contracts.py` (see [`scripts/README.md`](scripts/README.md)).
* Keep captured wire traffic, SHA-256 manifests, golden vectors, and archived runs under `specs/evidence/` byte-exact.
* Diagrams under `assets/` (including [`assets/architecture.svg`](assets/architecture.svg) and [`assets/order-lifecycle.svg`](assets/order-lifecycle.svg)) are hand-authored SVGs. Whenever process boundaries or message flows change, update the SVG and its Markdown alt text together and verify legibility at 320 pixels wide.
* Commit related implementation, specification, and verification updates together in lockstep.
