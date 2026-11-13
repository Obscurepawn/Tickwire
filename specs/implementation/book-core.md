# Bounded Single-Owner Order Book

`Book<P, N, M>` maintains a deterministic, normalized price-level order book for a single `(venue, instrument, source)` `BookKey` and monotonic generation. Built with `#![no_std]` and `#![forbid(unsafe_code)]`, the reducer stores at most `N` active price levels per side and stages at most `M` level changes per event inside fixed inline arrays, isolating book state transitions completely from network transport, wire decoding, system clocks, and heap allocation.

A single mutable owner drives snapshots, incremental updates, and generation rebuilds, while readers inspect immutable `BookView` borrows gated by liveness and observation age. Venue-specific continuity rules are injected statically through the `SequencePolicy` trait (`BinanceSpot` and `BinanceUsdM`), keeping storage layout, atomic rollback, and partial-snapshot coverage semantics uniform across products.

## State Model and Bounds

A book progresses through four explicit lifecycle states (`BookState`): `AwaitingSnapshot`, `Bridging`, `Live`, and `Invalid(InvalidReason)`, where `InvalidReason` distinguishes `Sequence`, `Capacity`, `Input`, and `Clock` faults. Prices (`PriceTicks(i64)`) are strictly positive tick counts; quantities (`QtyLots(i64)`) are non-negative lot counts where positive values set absolute resting size and zero deletes the level (deleting an absent level is a valid no-op). Bids sort descending (highest to lowest) and asks sort ascending (lowest to highest).

Let `L` denote the book's `local_id` (`last_update_id`) and `U..=u` (`first_update_id..=last_update_id`) denote an incoming `DepthEvent`. Both policies immediately reject inverted ranges (`U > u`) with `SequenceError::InvalidRange`.

| Policy (`P`) | Obsolete Event (`IgnoreStale`) | Bridge and Live Continuity (`Apply` vs `Gap`) |
| --- | --- | --- |
| `BinanceSpot` | Ignore `u <= L`. At `L == u64::MAX`, every valid range (`U <= u <= u64::MAX`) is obsolete. | Both `Bridging` and `Live` require `U <= L + 1`; `U > L + 1` fails with `Gap`. |
| `BinanceUsdM` | While `Bridging`, ignore `u < L`. Once `Live`, ignore `u <= L`. | Requires `previous_update_id` (`pu`), else `MissingPrevious`. First event in `Bridging` requires `U <= L <= u`; subsequent `Live` events require `pu == L`. |

When installing a `Snapshot`, unsorted entries and zero-quantity entries are permitted, though zero-quantity entries still count toward the per-side input bound `N`. Depending on `bids_complete` and `asks_complete`, each side tracks `SideCoverage`:

* `Complete`: Every price on that side is covered; any absent tick has a known quantity of `0`, and a side with zero stored levels reports `BestLevel::Empty`.
* `Through(boundary)`: Established by a non-empty truncated snapshot from the worst included bid upward (or worst included ask downward). Absent levels at or better than `boundary` report `Known(0)`; outside `boundary`, explicitly observed positive levels remain queryable, while absent or deleted levels report `QuantityKnowledge::Unknown`.
* `Unknown`: Established when a truncated snapshot arrives empty on that side, or retained outside coverage. If every level within a `Through(boundary)` region is deleted, `best_bid()` / `best_ask()` degrades to `BestLevel::Unknown` even if deeper out-of-coverage levels remain stored, preventing an uncovered level from masquerading as top-of-book.

## Core Invariants

* **Foreign-identity isolation**: Snapshots or depth events carrying a mismatched `BookKey` (`WrongBook`) or non-current `generation` (`WrongGeneration`) fail immediately without mutating stored levels, sequence watermarks, receive timestamps, or `BookState`.
* **All-or-nothing batch commit**: Before mutating live storage, `apply` validates total update count (`<= M`), per-side price uniqueness, post-deletion level capacity (`<= N`, allowing deletions in a batch to free slots for insertions in the same batch), non-regressing `received_at_ns`, and a strictly uncrossed final book (`best_bid < best_ask`, allowing bid and ask to step together through prior positions). Any violation rolls back the batch, preserves the last committed levels, sequence, and timestamp for diagnostics, and transitions the generation to `Invalid`.
* **Stale-event transparency**: Events classified as `IgnoreStale` return `ApplyOutcome::IgnoredStale` without advancing `local_id`, refreshing `received_at_ns`, or transitioning `Bridging` to `Live`.
* **Point-in-time view freshness**: `view(now, maximum_age)` succeeds only when the book is `Live` and `received <= now <= received + maximum_age` (`NotLive`, `ClockRegressed`, or `Stale` otherwise). The returned `BookView` freezes `age_ns = now - received` at construction; callers must acquire a fresh view per decision and pass the observation generation and timestamp to downstream execution gates.

## Transitions and Failure Semantics

A newly constructed `Book` starts in `AwaitingSnapshot` at its initial generation. Calling `install_snapshot` with valid levels transitions the book to `Bridging` (a `Bridging` book also accepts a replacement snapshot, such as when a Spot adapter re-requests depth after trailing its earliest buffered delta, whereas a `Live` book rejects snapshots with `SnapshotWhileLive`). Applying the first non-stale, continuous `DepthEvent` transitions `Bridging` to `Live` (`became_live: true`), and subsequent continuous events keep the book `Live`.

Any sequence gap, missing `pu`, malformed level, duplicate price within a side, capacity overflow, crossed book, regressed timestamp, or explicit `invalidate(reason)` moves the current generation to `Invalid(reason)`. Once `Invalid`, all snapshots and deltas for that generation fail with `NeedsRebuild`. Recovery requires calling `begin_rebuild(next_generation)` with `next_generation > current_generation` (`GenerationNotIncreasing` otherwise), which resets levels and coverage and returns the book to `AwaitingSnapshot`.

## Performance and Cost

Storage is statically bounded at `O(N + M)`, with the 64-bit level and staging arrays occupying `48N + 32M` bytes before scalar book metadata, and zero heap allocations on any transition. For a single-side batch of `K` updates against `L` stored levels:

* **Quantity-only updates** write directly into existing array slots without shifting tails.
* **Small batches (`K <= 32`)** recognize sorted, reverse-sorted, and cyclic input orderings without general sorting, use a 6-bit price fingerprint filter (always confirmed by exact price equality) to detect candidate duplicates, and apply unordered positive updates directly when all prices already exist.
* **General structural updates** run in `O(K log K + K log L)` lookup/sort work plus `O(L + K)` contiguous slice moves (`copy_within`) over only the affected price span and tail. Snapshot installation has an independent worst-case cost of `O(N^2)` off the steady-state delta path.

### Historical Microbenchmark Context

An archived microbenchmark comparing the planner against commit `df8bd2d` executed 200,000 updates per workload across five rounds on an unpinned Apple M1 Pro, timing `apply + view + best_bid + best_ask` in isolation (excluding network I/O, wire decoding, shared-memory IPC, and Linux scheduling):

* **4,096 active levels, 16 scattered quantity updates**: Median p50 decreased from `958 ns` to `542 ns`, and p99.9 decreased from `3,000 ns` to `1,625 ns`.
* **512 active levels, 512 scattered updates**: Median p50 moved from `8,750 ns` to `8,792 ns` with overlapping run-to-run ranges.

Re-run the criterion suites on the pinned target deployment host before adopting these figures as latency budgets.

## Verification and References

```sh
cargo test --locked -p tickwire-book
cargo clippy --locked -p tickwire-book --all-targets -- -D warnings
```

The test suite cross-checks the fixed-array reducer against an independent `BTreeMap` oracle across 40,000 randomized batches, asserts zero heap allocations, verifies rollback and partial-coverage boundaries, and counts logical comparisons and slice moves via `#[cfg(test)]` work counters.

* [Order book reducer implementation](../../crates/tickwire-book/src/lib.rs)
* [Logical comparison and move-count tests](../../crates/tickwire-book/src/work_tests.rs)
* [Differential oracle tests](../../crates/tickwire-book/tests/differential.rs)
* [Zero-allocation tests](../../crates/tickwire-book/tests/no_alloc.rs)
* [Microbenchmark workloads](../../crates/tickwire-book/benches)
* [Finite TLA+ model of book state and coverage](book-state-model.md)
* [Binance Spot local order book sequence rules](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly)
* [Binance USD-M local order book sequence rules](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)
