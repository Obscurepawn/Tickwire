# tickwire-market-source

`tickwire-market-source` owns the consumer-side `MarketSource` trait, shared-memory JOIN client
(`PendingJoin`, `Client`, `ShmSource`), and local dense `MarketReplica` / `MarketView` state consumed by
strategy workers and runtimes. Pure replica, checkpoint, retirement, and bank-transition modules run under
`#![deny(unsafe_code)]`; only [src/shm.rs](src/shm.rs) may discharge `JoinConsumer` attachment contracts after
authenticating peer control frames and `ResourceHeader` digests, while strategy decision logic stays in
`tickwire-strategy-sdk` and `tickwire-strategy-host`.

## Invariants and Change Guidelines

* **Verbatim baseline and catch-up retention**: During `PendingJoin` -> `Client` onboarding in
  [src/shm.rs](src/shm.rs) and [src/shm/pending.rs](src/shm/pending.rs), retain the exact sealed snapshot
  bytes (`baseline_bytes`) and every post-snapshot `CatchupRecord` (bounded by `MAX_CATCHUP_BYTES = 64 MiB`)
  with its local `applied_mono_ns` so downstream strategy recovery can journal deterministic inputs before the
  first callback.
* **Zero-allocation replica application**: `MarketReplica` ([src/replica.rs](src/replica.rs)) indexes
  instruments via `FixedIndex` (`DenseId`), fixed `StateSlots`, and preallocated `DepthWorkspace` vectors.
  Applying ring records or checking `MarketView` freshness must not allocate on the hot path or accept
  mismatched `topic_id`, `feed_generation`, `ring_epoch`, or `instrument_version`.
* **Fail-closed invalidation and bank transitions**: `MarketSource::stop`, ring sequence gaps, stale
  deadlines, or epoch freezes in [src/transition.rs](src/transition.rs),
  [src/shm/transition.rs](src/shm/transition.rs), and [src/retirement.rs](src/retirement.rs) must emit
  `MarketSink::invalidated` before discarding or switching sources so execution gates close immediately.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/shm.rs](src/shm.rs) · [src/replica.rs](src/replica.rs)
  · [src/replica/checkpoint.rs](src/replica/checkpoint.rs) · [src/transition.rs](src/transition.rs) ·
  [src/retirement.rs](src/retirement.rs) · [tests/replica_dense.rs](tests/replica_dense.rs)
* **Specifications and models**:
  [strategy-input-consumption.md](../../specs/implementation/strategy-input-consumption.md) ·
  [binance-market-events.md](../../specs/implementation/binance-market-events.md) ·
  [join.md](../../specs/implementation/join.md) ·
  [market-expansion.md](../../specs/implementation/market-expansion.md) ·
  [quote_delivery](../../tla/execution_models/quote_delivery/ROADMAP.md) ·
  [worker_handover](../../tla/execution_models/worker_handover/ROADMAP.md)

```sh
cargo test --locked -p tickwire-market-source --all-targets
./scripts/check-linux.sh
```
