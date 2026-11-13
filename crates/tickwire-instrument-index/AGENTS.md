# tickwire-instrument-index

`tickwire-instrument-index` (`#![forbid(unsafe_code)]`) maps discrete external `u64` instrument IDs
to dense, zero-based `DenseId` array indices via immutable `FixedIndex` and growable `AppendIndex`
tables. Cold-path builders construct or resize these directories from catalog or topic instrument
sets; hot-path quote publishers and strategy replicas call `lookup` and `key` with zero heap
allocations, while economic metadata remains in `tickwire-instrument-catalog`.

## Invariants and Change Guidelines

* **Dense order stability**: `FixedIndex::new` in [src/lib.rs](src/lib.rs) must assign
  `DenseId(0..keys.len())` in exact input slice order, and `AppendIndex::insert` /
  `AppendIndex::reserve` must never renumber or invalidate previously issued `DenseId` slots when
  growing up to `MAX_KEYS` (`1 << 20`).
* **Full-key verification on lookup**: Neither `Gather` bit-compression nor `Flat` open-addressed
  probing guarantees collision-free hashes for unseen inputs. `FixedIndex::lookup` and `Flat::probe`
  in [src/lib.rs](src/lib.rs) must always verify `keys[dense] == key` before returning
  `Some(DenseId)`.
* **Cold-path bit-compression and fallible allocation**: Keep `greedy_mask` and `Gather::new`
  precomputation on the construction path, selecting `Layout::Gather` only when compressed table
  size fits `<= MAX_BIT_SLOTS` (`1 << 22`) and `<= 4 * keys.len()`, falling back to power-of-two
  `Flat` tables otherwise. Allocate backing vectors exclusively through `try_reserve_exact`
  (`reserved`) so oversized requests return `Error::Allocation` or `Error::Capacity` without
  panicking.
* **Lookup latency**: Measure changes to `Gather::apply` or `Flat::probe` (splitmix64 `hash` and
  linear probing) with the `lookup` benchmark before altering table load factors or slot widths.

## Verification and Specifications

* **Source, unit tests, and benchmarks**: [src/lib.rs](src/lib.rs) · [src/tests.rs](src/tests.rs) ·
  [tests/index.rs](tests/index.rs) · [benches/lookup.rs](benches/lookup.rs)
* **Specifications**: [instrument-index.md](../../specs/implementation/instrument-index.md)

```sh
cargo test --locked -p tickwire-instrument-index --all-targets
cargo bench -p tickwire-instrument-index --bench lookup
```
