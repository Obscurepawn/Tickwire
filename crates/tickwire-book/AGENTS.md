# tickwire-book

`tickwire-book` owns the `#![no_std]`, `#![forbid(unsafe_code)]`, fixed-capacity single-owner order book (`Book<P, LEVELS, MAX_UPDATES>`) and
borrowed read view (`BookView`). Upstream market-data sessions decode frames, supply monotonic receive timestamps, and feed normalized integer
`Snapshot` and `DepthEvent` slices paired with a `SequencePolicy` (`BinanceSpot` or `BinanceUsdM`); this crate performs zero heap allocations,
system calls, clock reads, or network I/O.

## Invariants and Change Guidelines

* **All-or-nothing batch preparation**: Keep price validation, duplicate detection, capacity checks, and crossed-book spread checks inside
  `prepare_side` and `validate_spread` in [src/lib.rs](src/lib.rs) before mutating `Levels<N>`. Any sequence, capacity, input, or
  clock-regression error must transition the book to `BookState::Invalid(InvalidReason)` via `Book::fail` without partially applying levels.
* **Zero-allocation fixed buffers**: Maintain inline `[Level; N]`, `[PreparedLevel; M]`, and merge scratch storage. Snapshot installation in
  `Levels::apply_validated` must delete zero-quantity updates before inserting positive quantities so intermediate occupancy never exceeds `N`.
* **Truncated snapshot coverage**: Preserve `SideCoverage` (`Complete`, `Through(PriceTicks)`, `Unknown`) across updates so `BookView::quantity`
  distinguishes `QuantityKnowledge::Known(QtyLots(0))` inside the covered price span from `QuantityKnowledge::Unknown` beyond a truncated
  snapshot boundary.
* **Generation fencing and sequence policies**: `begin_rebuild` requires a strictly increasing `generation`, and `invalidate` rejects callbacks
  from superseded generations (`BookError::WrongGeneration`). Keep venue-specific bridge and continuity rules isolated in
  `SequencePolicy::classify` while enforcing non-decreasing `last_update_id` watermarks in storage.
* **Algorithmic work bounds**: Changes to small-batch direction detection (`input_directions`, `cyclic_order`, `prepare_replacements`) or span
  merging (`apply_prepared`) must pass comparison/move assertions in [src/work_tests.rs](src/work_tests.rs), differential oracle checks in
  [tests/differential.rs](tests/differential.rs), and release depth benchmarks.

## Verification and Specifications

* **Source and benchmarks**: [src/lib.rs](src/lib.rs) · [src/work_tests.rs](src/work_tests.rs) · [benches/depth_apply.rs](benches/depth_apply.rs)
  · [benches/batch_apply.rs](benches/batch_apply.rs) · [benches/sparse_apply.rs](benches/sparse_apply.rs)
* **Integration tests**: [tests/book.rs](tests/book.rs) · [tests/batch.rs](tests/batch.rs) · [tests/small_batch.rs](tests/small_batch.rs) ·
  [tests/differential.rs](tests/differential.rs) · [tests/no_alloc.rs](tests/no_alloc.rs)
* **Specifications and models**: [book-core.md](../../specs/implementation/book-core.md) ·
  [book-state-model.md](../../specs/implementation/book-state-model.md) · [book_state](../../tla/execution_models/book_state/ROADMAP.md) ·
  [binance_depth_sync](../../tla/execution_models/binance_depth_sync/ROADMAP.md)

```sh
cargo test --locked -p tickwire-book --all-targets
cargo bench -p tickwire-book --bench depth_apply
./scripts/check-tla.sh book_state
```
