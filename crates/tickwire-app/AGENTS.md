# tickwire-app

`tickwire-app` (`#![deny(unsafe_code)]`, `unsafe_op_in_unsafe_fn = "deny"`, with audited `#[allow(unsafe_code)]`
limited to [src/hosted.rs](src/hosted.rs) and [src/market.rs](src/market.rs)) is the top-level deployment assembly and
CLI (`inspect`, `run`, `replay`, `export`). It validates deployment JSON, launches `1..=32` isolated strategy workers
over shared-memory market rings and Unix control sockets, runs startup reconciliation, records runtime observations, and
drives `tickwire-runtime`; domain math, codecs, and venue protocols remain in their workspace crates.

## Invariants and Change Guidelines

* **Shared preflight and storage-profile assembly across `inspect` and `run`**: Validate deployment bounds, instrument
  catalogs, `StorageMode` (`InMemory` zero-directory default, `MmapAsync`, `LocalDurable`), `1..=32` logical slots,
  unique instance IDs, `risk_domain_id`, and disjoint member directories in [src/config.rs](src/config.rs),
  [src/launch.rs](src/launch.rs), and [src/launch/group.rs](src/launch/group.rs) before spawning children or sockets.
* **Audited `unsafe` confinement in `hosted` and `market`**: Keep shared-memory ring/snapshot mapping and OS
  credential setup isolated inside [src/hosted.rs](src/hosted.rs) and [src/market.rs](src/market.rs), giving each
  worker its own cursor and distinct JOIN session within a supervisor epoch over one decoded market feed.
* **Non-blocking multi-strategy fairness**: In [src/strategy.rs](src/strategy.rs), ensure a slow or faulted worker
  never starves peer workers and that every `SubmissionResult` and account report routes to its originating member.
* **Deterministic `replay`, `export`, and `observation`**: Preserve recorded inputs, reconciliation artifacts
  ([src/reconciliation.rs](src/reconciliation.rs)), and observation schemas ([src/observation.rs](src/observation.rs),
  [src/control.rs](src/control.rs)) during offline `replay` and `export` ([src/replay.rs](src/replay.rs)).

## Verification and Specifications

* **Source, examples, and tests**: [src/lib.rs](src/lib.rs) · [src/main.rs](src/main.rs) ·
  [src/launch/group.rs](src/launch/group.rs) · [src/hosted.rs](src/hosted.rs) · [src/market.rs](src/market.rs) ·
  [src/strategy.rs](src/strategy.rs) · [shared-market-strategies.json](examples/shared-market-strategies.json) ·
  [tests/catalog_process.rs](tests/catalog_process.rs) · [tests/inputs_process.rs](tests/inputs_process.rs) ·
  [tests/parameters_process.rs](tests/parameters_process.rs) · [tests/projection_launch.rs](tests/projection_launch.rs)
* **Specifications and models**: [single-host-trading.md](../../specs/implementation/single-host-trading.md) ·
  [strategy-input-consumption.md](../../specs/implementation/strategy-input-consumption.md) ·
  [strategy-parameter-consumption.md](../../specs/implementation/strategy-parameter-consumption.md) ·
  [runtime-observation.md](../../specs/implementation/runtime-observation.md) ·
  [Deploy two strategies](../../README.md#deploy-two-strategies) ·
  [worker_handover](../../tla/execution_models/worker_handover/ROADMAP.md) ·
  [strategy_intake](../../tla/execution_models/strategy_intake/ROADMAP.md) ·
  [quote_delivery](../../tla/execution_models/quote_delivery/ROADMAP.md)

```sh
cargo test --locked -p tickwire-app --all-targets
python3 scripts/run-two-strategies.py
```
