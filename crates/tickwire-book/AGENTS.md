# tickwire-book

The fixed-capacity order book applies normalized snapshots and deltas under a venue-specific sequence policy. BookView borrows the resulting levels.

Exercise stale updates, a missing sequence, deletion at the best price and queries outside known coverage. Compare sparse batches against the ordered-map oracle and run the depth benchmarks before changing memory layout.

[Crate API](src/lib.rs) · [batch](tests/batch.rs) · [book](tests/book.rs) · [differential](tests/differential.rs) · [no alloc](tests/no_alloc.rs) · [small batch](tests/small_batch.rs)

Specifications include [book core](../../specs/implementation/book-core.md), [book state model](../../specs/implementation/book-state-model.md).

```sh
cargo test --locked -p tickwire-book --all-targets
```

Run the benchmarks [`batch_apply`](benches/batch_apply.rs), [`depth_apply`](benches/depth_apply.rs), [`sparse_apply`](benches/sparse_apply.rs).
