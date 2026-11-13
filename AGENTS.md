# Engineering Guide and Invariants (`AGENTS.md`)

Start with the [shared-feed walkthrough](README.md#deploy-two-strategies), the [specification index](specs/INDEX.md), and [`CONTRIBUTING.md`](CONTRIBUTING.md). Before modifying any crate under `crates/`, read its local `AGENTS.md` and governing specification in [`specs/implementation/`](specs/implementation/README.md) to confirm which single writer owns each state transition. All workspace crates, examples, and integration tests build against the pinned Rust toolchain (`rust-toolchain.toml`) and `Cargo.lock`.

## 1. Preserve State Ownership

* **Market data and strategy isolation:** The market-data publisher exclusively owns venue subscriptions, instrument bindings, sequence continuity rules, and shared-memory ring publication. Each strategy worker reads its own local market replica at an independent cursor and exclusively owns its algorithm state and outbound intent sequence.
* **Single-writer risk domain:** One account process owns cash balances, exposure reservations, open orders, and strategy attribution for a single risk domain. Depending on `StorageMode` (`InMemory` zero-disk default for ephemeral cloud Pods, `MmapAsync` preallocated `MAP_SHARED` segments + background `fdatasync`, or `LocalDurable` synchronous `fsync`), the account gateway commits the command's recovery closure and `AccountJournal` admission record *before* queuing network output, and synchronously re-verifies current ownership, lease validity, feed health, and action permissions immediately before TLS accepts the first plaintext byte.
* **Distinct venue evidence and self-describing attribution:** Venue connectors own wire authentication, native request signing, and identifier translation, but never fabricate economic state. Outbound orders carry a 32-character `[0-9a-z]` `SelfDescribingClientId` (`tw1...` with domain-keyed MAC) so cold stateless restarts reconstruct strategy attribution and sequence floors directly from authenticated `Account + OpenOrders` snapshots without prior local WAL files. Keep request acknowledgements, order-status snapshots, and individual trade executions as distinct facts; trade-ID deduplication and input logging make retries and restarts deterministic.
* **Cold startup vs. hot path:** Parse configuration, seal worker executables, bind cgroups/CPUs, resolve catalogs, and authenticate control peers during startup or explicit reconnect—never inside quote decoding or order admission.

## 2. Spec-First Contract Changes

1. **Specify before coding:** Before altering observable behavior, update the input, output, state owner, and failure semantics in the relevant [module specification](specs/implementation/README.md). Register any new module in both [`specs/INDEX.md`](specs/INDEX.md) and [`specs/implementation/README.md`](specs/implementation/README.md).
2. **Test-driven regression:** Write the smallest failing regression test that demonstrates the boundary condition, implement the fix, and re-verify the specification.
3. **Synchronize machine contracts and formal models:**
   * If wire framing, CBOR tags, configuration schemas, catalog digests, or fixed-point risk rules change, update the corresponding [machine contract and golden vectors](specs/contracts/README.md) alongside its `scripts/verify-*.py` validator.
   * If event ordering, admission gating, shared-memory reclamation, or crash recovery changes, inspect the source anchors in [`tla/`](tla/README.md) and re-run the affected TLC model family.
4. **Respect compatibility and replay boundaries:** Treat normalized market records, canonical command digests, and account journal encodings as strict compatibility boundaries. Replay must evaluate admissions using the original recorded inputs and decision timestamps, keeping all pending reservations locked until authenticated venue reconciliation proves the remote outcome.

## 3. Performance, Arithmetic, and Memory Safety

* **Account for startup vs. per-event cost:** For every design change, separate startup work from per-quote, per-order, and per-fill work. Explicitly count copies, heap allocations, queue hops, clock reads, and syscalls, and define saturation/tail behavior when a reader lags or a bounded queue fills. Distinguish costs inferred from code structure from empirical measurements, and always record CPU model, OS/kernel environment, load, and timing boundaries alongside benchmark figures.
* **Exact fixed-point arithmetic:** Use integer price ticks, quantity lots, and checked fixed-point decimals (`Fixed`) across all risk and execution paths; floating-point arithmetic is forbidden in financial calculations.
* **Audited `unsafe` isolation:** Workspace crates default to `#![forbid(unsafe_code)]`. Introduce `unsafe` only for a measured OS or shared-memory bottleneck, encapsulate it behind a sound safe API with `#![deny(unsafe_op_in_unsafe_fn)]`, document bounds, alignment, aliasing, initialization, and cross-process lifetime obligations, and benchmark it against a safe baseline. Keep lint exceptions local to the audited boundary (see [`tickwire-shm-sys`](crates/tickwire-shm-sys/AGENTS.md) and [`tickwire-clock-sys`](crates/tickwire-clock-sys/AGENTS.md)).

## 4. Verification, Documentation, and Commits

Run the core verification suite before committing any change:

```sh
cargo fmt --all -- --check
cargo test --locked -p <affected-crate> --all-targets
cargo clippy --workspace --all-targets -- -D warnings
python3 scripts/verify-documentation.py
```

* Run the full workspace test suite before committing (`./scripts/check-rust.sh`).
* For shared-memory, `memfd`, cgroup, or `SCM_RIGHTS` changes, run `./scripts/check-linux.sh`.
* For ordering, admission, or durability changes, run `./scripts/check-tla.sh`.
* For wire, configuration, instrument catalog, or risk changes, run `python3 scripts/verify-design-contracts.py` (see [`scripts/README.md`](scripts/README.md)).
* Write all READMEs, specifications, and contributor guides in clear, idiomatic English. Include runnable commands with observable outputs in READMEs, concrete state transitions and acceptance cases in specifications, and exact module/test pointers in crate guides.
* Keep captured wire traffic, SHA-256 manifests, golden vectors, and archived runs under `specs/evidence/` byte-exact.
* Diagrams under `assets/` (including [`assets/architecture.svg`](assets/architecture.svg) and [`assets/order-lifecycle.svg`](assets/order-lifecycle.svg)) are hand-authored SVGs. Whenever process boundaries or message flows change, update the SVG and its Markdown alt text together and verify that all labels and arrowheads remain legible at 320 pixels wide.
* Commit related implementation, specification, and verification updates together, push each verified stage to its tracked branch, and verify the remote revision.
