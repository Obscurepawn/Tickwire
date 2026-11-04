# Local instrument indexes

`tickwire-instrument-index` maps full-width `u64` keys to contiguous process-local `DenseId` values. Keep each ID with the index that created it. Instrument versions, catalog approval and wire identity belong to the caller. [Source](../../crates/tickwire-instrument-index/src/lib.rs)

## Choose an index

* `FixedIndex::new(keys)` assigns slots in input order. Duplicate keys fail. It greedily removes high-to-low mask bits while retaining distinct projections, then precomputes at most six software bit-gather stages. A reader projects the key, looks up the slot and compares the entire original key. The full comparison rejects an unknown ID with the same projection. A bit table must fit both four slots per key and 4,194,304 slots in total. Otherwise construction falls back to bounded, open-addressed hashing with at most 50% load.
* `AppendIndex::with_capacity(max)` reserves the key array and open-addressed table. New keys get the next slot. An existing key returns its original slot even when full. A failed insertion preserves every mapping. There is no deletion or slot reuse. `reserve` builds larger storage off the hot path and preserves existing IDs. Lookup and insertion within the reserved capacity do not allocate, lock or perform I/O.

Both accept zero, `u64::MAX` and empty sets. The cap is 1,048,576 keys. Slot counts use checked arithmetic. A probe checks at most the table length. `u32::MAX` is the private empty-slot sentinel. `memory_stats()` reports actual reserved bytes for key and slot arrays, excluding allocator overhead.

## Use in market data

A Binance MD process assigns its local book slot before constructing a depth owner. The depth stream ID must fit the `u32` source field in `BookKey`. An independent event source retains its full `u64` ID. Each publisher prebinds topic slots and verifies stable IDs. `FeedIndex::expanded` builds a candidate mapping, preserves all old slots and appends new IDs in first-seen order. An uncommitted candidate has no effect on old readers. The new single-instrument Bybit/OKX BBO owner uses local slot zero. Each venue, source and external instrument binding is still checked. [Expansion contract](market-expansion.md) · [Public spot BBO](spot-quote-input.md)

## Measure

```sh
cargo test --locked -p tickwire-instrument-index
cargo bench --locked -p tickwire-instrument-index --bench lookup
```

The lookup benchmark prebuilds queries for sets of 32, 256 and 4096 keys and times 4096 lookups per batch. Its percentiles describe batch means. On a Ryzen 7 8845H under WSL2, pinned to CPU 2, five release runs on 4096 compressible hit keys reported median p50 batch means of 1.903 ns/query for the fixed index, 1.993 for the append index and 6.861 for HashMap. For dispersed hits the figures were 2.089, 1.993 and 6.859. These measurements exclude decode, publication and callbacks. [Five-run summary](../evidence/20261028/index-bench-summary.json) · [Raw measurements](../evidence/20261028/index-bench-1.csv) · [Environment](../evidence/20261028/index-benchmark-settings.json) · [Correctness cases](../../crates/tickwire-instrument-index/tests/index.rs)
