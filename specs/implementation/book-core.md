# Bounded order book

`Book<P, N, M>` owns one book key and generation, up to `N` price levels on each side, and space for `M` updates per event. The sequence policy `P` selects the venue's continuity rules. The reducer uses fixed storage, builds with `no_std` and forbids unsafe code. [Implementation](../../crates/tickwire-book/src/lib.rs)

## Snapshot and sequence

A new book starts in `AwaitingSnapshot`. A valid snapshot moves it to `Bridging`. The first applicable delta moves it to `Live`. A gap in the current generation, malformed update, capacity error, or explicit invalidation moves it to `Invalid`. Begin a rebuild with a greater generation. A bridging book accepts a replacement snapshot. A live book rejects it. An event for another book or generation leaves the current book untouched.

Let `L` be the local sequence and `U..u` the event range. Reject `U > u` for both products.

| Policy | Obsolete event | Required bridge or next update |
| --- | --- | --- |
| Binance Spot | Ignore `u <= L`. | Require `U <= L + 1`. At `u64::MAX` all valid ranges are obsolete. |
| Binance USD-M | While bridging, ignore `u < L`. Once live, ignore `u <= L`. | First event spans `U <= L <= u` and has `pu`. Later events require `pu == L`. |

An ignored event leaves the receive time and book unchanged. The Spot adapter requests the snapshot again if it trails the earliest buffered start. [Spot sequence rules](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly) · [USD-M sequence rules](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)

## Apply one update

Prices are positive tick counts. Quantities are lot counts. A positive quantity sets the absolute level, zero removes it, and deleting an absent level succeeds. Bids sort from highest to lowest and asks from lowest to highest. Snapshots may arrive unsorted. Even zero-quantity snapshot entries count against the input bound `N`.

Before committing, validate the total update count, unique prices on each side, final capacity, a strictly uncrossed final book and a receive time at least as recent as the previous update. A deletion can free space for another price in the same batch. A bid and ask may move together through their previous positions. Both sides commit together. Failure retains the last committed levels, sequence and observation time for diagnostics and invalidates the current generation.

Quantity-only updates write directly to known positions. Structural updates merge the affected range with sorted changes and move the remaining tail when required. For small batches (up to 32 updates on a side), the planner recognizes sorted, reverse-sorted and cyclic order. Unordered positive batches can update known positions directly. The six-bit collision filter always confirms candidate duplicates by price.

With `K` updates and `L` live prices on one side, the worst-case update cost is `O(K log K + K log L)`, plus `O(L + K)` for structural changes. Space is `O(N + M)`. The current 64-bit field payload occupies `48N + 32M` bytes before metadata. Snapshot installation has a separate worst case of `O(N²)`.

## Read a price

A limited snapshot proves coverage from the worst included bid upwards and the worst included ask downwards. Within coverage, an absent price means zero. Outside it, an explicitly observed positive quantity remains queryable, but an absent or deleted level is unknown. If every price within coverage disappears, the best price becomes unknown even when a more distant level remains in storage. A complete empty side returns `Empty`.

`view(now, maximum_age)` requires a live book and `received <= now <= received + maximum_age`. Its reported age is fixed at view creation. Obtain a fresh view for a decision. Carry the observation and generation to the execution gate for its own checks.

## Verify

```sh
cargo test --locked -p tickwire-book
cargo clippy --locked -p tickwire-book --all-targets -- -D warnings
```

The tests compare fixed-array results with an independent map over 40,000 batches. Other cases cover rollback, sequence boundaries, incomplete coverage and allocator counts. Work counters distinguish repeated lookups from tail copies. Their counts describe logical operations. Historical unpinned M1 Pro measurements cover `apply + view + best_bid + best_ask`. They do not measure transport, decoding, IPC or Linux end-to-end latency. [Differential tests](../../crates/tickwire-book/tests/differential.rs) · [Allocation tests](../../crates/tickwire-book/tests/no_alloc.rs) · [Work counters](../../crates/tickwire-book/src/work_tests.rs)


A historical comparison against `df8bd2d` ran 200,000 updates per case across five rounds on an unpinned Apple M1 Pro. With 4096 levels and 16 scattered quantity changes, median p50 fell from 958 to 542 ns and p99.9 from 3000 to 1625 ns. With 512 levels and changes to all 512 scattered levels, p50 moved from 8750 to 8792 ns and the run ranges overlapped. Re-run the workloads on the deployment host before using those figures as budgets. [Benchmark workloads](../../crates/tickwire-book/benches)
