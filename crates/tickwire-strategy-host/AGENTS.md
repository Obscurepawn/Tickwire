# tickwire-strategy-host

`tickwire-strategy-host` owns parent-side strategy child lifecycle, authenticated intent/report lane setup, MD
admission registration (`RemoteHostedStrategy` in [src/remote.rs](src/remote.rs)), runtime readiness proofs
([src/readiness.rs](src/readiness.rs)), market bank transition coordination
([src/market_transition.rs](src/market_transition.rs)), and fresh state/resource observation
([src/observation.rs](src/observation.rs), [src/resources.rs](src/resources.rs)). Crate-wide
`#![deny(unsafe_code)]` confines `unsafe` launch entrypoints to audited supervisor/SHM peer boundaries; worker
callback logic lives in `tickwire-strategy-sdk` and cash/order risk lives in `tickwire-account`.

## Invariants and Change Guidelines

* **Authenticated child and MD admission ordering**: In [src/lib.rs](src/lib.rs) and
  [src/remote.rs](src/remote.rs), spawn the sealed worker binary via `tickwire-supervisor::Registry` inside a
  `0o700` `PrivateDirectory`, register the child's actual kernel PID/UID/GID with `tickwire-md` before
  signalling `market_admission_ready`, and verify `Identity`, `logical_strategy`, and `generation` on both
  `intent.sock` and `report.sock` before accepting commands or publishing reports.
* **Isolated per-worker cursors and fault containment**: An exited, timed-out, or unauthenticated worker must
  close its own readiness gate (`ControlPlane`) without corrupting sibling workers sharing the same market
  bank or account report writer.
* **Cold kernel observation vs hot intake**: Keep `/proc` and cgroup sampling (`observe_resources` in
  [src/resources.rs](src/resources.rs)) and control-frame observation queries
  ([src/observation.rs](src/observation.rs)) off the hot SPSC `IntentReceiver` and `ReportWriter` paths, and keep
  optional strategy-attributed `on_projected_fill` notifications volatile rather than writing non-authoritative
  projections onto durable `780` / `781` report rings.

## Verification and Specifications

* **Source and unit tests**: [src/lib.rs](src/lib.rs) · [src/remote.rs](src/remote.rs) ·
  [src/readiness.rs](src/readiness.rs) · [src/market_transition.rs](src/market_transition.rs) ·
  [src/observation.rs](src/observation.rs) · [src/resources.rs](src/resources.rs) ·
  [src/market_transition/tests.rs](src/market_transition/tests.rs) ·
  [src/resources/tests.rs](src/resources/tests.rs)
* **Specifications and models**: [single-host-trading.md](../../specs/implementation/single-host-trading.md) ·
  [strategy-input-consumption.md](../../specs/implementation/strategy-input-consumption.md) ·
  [supervisor-process.md](../../specs/implementation/supervisor-process.md) ·
  [runtime-observation.md](../../specs/implementation/runtime-observation.md) ·
  [Deploy two strategies](../../README.md#deploy-two-strategies) ·
  [worker_handover](../../tla/execution_models/worker_handover/ROADMAP.md) ·
  [binance_roundtrip](../../tla/execution_models/binance_roundtrip/ROADMAP.md)

```sh
cargo test --locked -p tickwire-strategy-host --all-targets
python3 scripts/run-two-strategies.py
./scripts/check-tla.sh worker_handover
```
