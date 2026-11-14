# tickwire-market-publisher

`tickwire-market-publisher` (`#![forbid(unsafe_code)]`) owns `TopicState` and `TopicPublisher<M>`, pairing a
complete in-memory topic cut with a `JoinOwner<M>` shared-memory bank to publish normalized Binance depth,
trades, BBO, mark/funding, and Bybit/OKX spot quotes alongside exact JOIN snapshot baselines. Network sockets,
TLS sessions, and OS admission servers belong in `tickwire-md`.

## Invariants and Change Guidelines

* **Atomic multi-record batching and gate ordering**: Depth deltas, BBO, and feed-health `MarketRecord` /
  `MarketExactRecord` updates in [src/lib.rs](src/lib.rs), [src/events.rs](src/events.rs), and
  [src/spot.rs](src/spot.rs) must publish as a single `JoinOwner::publish_market` batch. When marking a quote
  or feed unusable (`spot_quote_unavailable_at` or `FaultSink::invalidate`), sequence the degraded feed-health
  record ahead of or atomically with stale state so readers close execution gates before observing unusable
  quotes, and notify `FaultSink::invalidate` synchronously even if the ring is full (`Error::NeedsBank`).
* **Preallocated dense slots and workspaces**: Resolve `InstrumentSlot` (`DenseId` in `FixedIndex`) and
  allocate `DepthWorkspace` / delta buffers (`MAX_DELTA_LEVELS = 4096`) during publisher construction so
  hot-path publication never resizes maps or replaces wire `instrument_id` identities.
* **Exact JOIN cuts, topic expansion, and `crc32` feature gate**: `TopicState` ([src/lib.rs](src/lib.rs)) tracks per-instrument
  `SideCoverage`, `revision`, and `sequence` so `build_topic_with_coverage` produces snapshots matching the
  exact ring frontier. Keep live instrument-set expansion isolated in [src/expansion.rs](src/expansion.rs),
  preserving existing `DenseId` assignments and bank epoch handoffs. Optional `--features crc32` (default OFF)
  controls whether outbound `MarketRecord` / `MarketExactRecord` headers set checksum flag `0x08` and compute
  Castagnoli CRC32C.

## Verification and Specifications

* **Source and unit tests**: [src/lib.rs](src/lib.rs) · [src/events.rs](src/events.rs) ·
  [src/spot.rs](src/spot.rs) · [src/expansion.rs](src/expansion.rs) ·
  [src/expansion/tests.rs](src/expansion/tests.rs)
* **Specifications and models**:
  [binance-market-events.md](../../specs/implementation/binance-market-events.md) ·
  [spot-quote-input.md](../../specs/implementation/spot-quote-input.md) ·
  [market-expansion.md](../../specs/implementation/market-expansion.md) ·
  [topic-snapshot-builder.md](../../specs/implementation/topic-snapshot-builder.md) ·
  [quote_delivery](../../tla/execution_models/quote_delivery/ROADMAP.md) ·
  [spot_roundtrip](../../tla/execution_models/spot_roundtrip/ROADMAP.md)

```sh
cargo test --locked -p tickwire-market-publisher --all-targets
./scripts/check-tla.sh quote_delivery
```
