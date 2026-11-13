# Process-Local Dense Instrument Indexes

`tickwire-instrument-index` maps arbitrary full-width `u64` instrument identifiers (including `0` and `u64::MAX`) to contiguous, zero-based process-local `DenseId` slots so hot-path publishers and book routers can index flat arrays instead of hashing sparse 64-bit keys on every market event.

A `DenseId` has meaning only within the specific `FixedIndex` or `AppendIndex` instance (or verified expansion successor) that produced it. External wire protocols, IPC records, checkpoints, catalog approvals, and instrument versions remain keyed by full `u64` external identifiers and belong to the caller.

## State Model and Bounds

Both index types accept empty sets and enforce a hard ceiling of `1,048,576` keys (`MAX_KEYS = 1 << 20`), using `u32::MAX` internally as the private empty-slot sentinel and checked arithmetic for all capacity calculations. `memory_stats()` reports exact reserved heap bytes (`key_bytes` and `slot_bytes`) for the backing key and slot vectors, excluding struct metadata and allocator bookkeeping.

| Index Type | Construction & Layout | Mutability & Fallback Rules |
| --- | --- | --- |
| `FixedIndex` | `FixedIndex::new(keys)` assigns `DenseId(0..len)` in exact input slice order and rejects duplicates (`Error::DuplicateKey`). | Immutable. Greedily strips redundant bits from MSB (`63`) down to `0` while keeping key projections collision-free, then precomputes up to 6 parallel right-compression bit-gather stages (`Gather`). Uses direct table lookup if `2^popcount(mask) <= 4 * len` and `<= 4,194,304` (`MAX_BIT_SLOTS`); otherwise falls back to open-addressed hashing (`Flat`) at `<= 50%` load. |
| `AppendIndex` | `AppendIndex::with_capacity(max)` pre-allocates the key vector and open-addressed hash table (`<= 50%` load) up front. | Append-only; no deletion or slot reuse. `insert(key)` returns the existing `DenseId` if already present (even when full) or assigns the next sequential slot. `reserve(additional)` builds larger storage off the hot path while preserving every existing `DenseId`. |

## Core Invariants

* **Full-key verification on lookup**: Whether `FixedIndex` projects a key through its bit-gather network or `Flat` probes its open-addressed table, `lookup(key)` always compares the candidate slot's stored 64-bit key against the query `key`. Unknown keys that collide under bit projection are rejected cleanly with `None`.
* **Bounded probe and failure atomicity**: Open-addressed probes inspect at most the table length. A failed `FixedIndex::new`, `AppendIndex::insert` (`Error::Capacity`), or `AppendIndex::reserve` (`Error::Capacity` / `Error::Allocation`) leaves any existing index mappings completely intact.
* **No silent ID truncation in `BookKey`**: In Binance market data, `BookKey.instrument` is a `u32` owner-local dense slot (where slot `0` is valid, as used by both the first indexed Binance feed and the single-instrument Bybit/OKX BBO owner), whereas independent event sources and IPC messages retain full `u64` external instrument IDs. Callers must assign `BookKey.instrument` via an explicit index rather than truncating high bits of the `u64` external ID.

## Market-Data Lifecycle and Expansion

At startup, `tickwire-md` constructs its local `FeedIndex` before instantiating depth owners, ensuring every depth stream fits `BookKey` and prebinding topic publication slots with verified stable IDs. During a live topic expansion (`FeedIndex::expanded`), the candidate index clones existing mappings at their exact original `DenseId` positions and appends newly added instruments in first-seen order. Because the candidate is built privately, uncommitted or aborted expansion plans have zero effect on active readers, and committed successors preserve slot stability for every pre-existing instrument.

## Performance and Cost

Index construction, greedy bit-mask minimization, and `AppendIndex::reserve` perform heap allocation (`try_reserve_exact`) and sorting on cold setup or control paths. Steady-state `lookup` and pre-reserved `AppendIndex::insert` execute without heap allocation, locking, or system calls: `Layout::Gather` performs one bitwise mask, up to six register shift-and-merge stages, one slot load, and one 64-bit key comparison.

### Benchmark Context

The `lookup` benchmark prebuilds 4,096-query batches across key-set sizes of 32, 256, and 4,096 and records batch-mean nanoseconds per query (excluding wire decode, shared-memory publication, and strategy callbacks). Across five release runs on an AMD Ryzen 7 8845H under WSL2 pinned to CPU 2 with 4,096 keys:

* **Compressible hit keys**: Median p50 batch mean was `1.903 ns/query` (`FixedIndex`), `1.993 ns/query` (`AppendIndex`), and `6.861 ns/query` (`std::collections::HashMap`).
* **Dispersed hit keys**: Median p50 batch mean was `2.089 ns/query` (`FixedIndex`), `1.993 ns/query` (`AppendIndex`), and `6.859 ns/query` (`HashMap`).

## Verification and References

```sh
cargo test --locked -p tickwire-instrument-index
cargo bench --locked -p tickwire-instrument-index --bench lookup
```

* [Fixed and append-only dense index implementation](../../crates/tickwire-instrument-index/src/lib.rs)
* [Collision, capacity, and bit-gather correctness tests](../../crates/tickwire-instrument-index/tests/index.rs)
* [Live market topic expansion specification](market-expansion.md)
* [Public spot top-of-book specification](spot-quote-input.md)
* [Five-run benchmark summary (`20261028`)](../evidence/20261028/index-bench-summary.json)
* [Raw benchmark CSV (`20261028`)](../evidence/20261028/index-bench-1.csv)
* [Benchmark host and CPU pinning settings](../evidence/20261028/index-benchmark-settings.json)
