# Finite Model of Book State and Coverage

The `book_state` TLA+ verification suite model-checks the public state machine and partial-coverage epistemic semantics of a single exclusively owned `tickwire-book` instance against source SHA-256 digest `37beb8dcd515b76a8c3bec0a2cd46a1c3c2431375276810b45818b8b35fd04a3`. Because safe Rust enforces exclusive `&mut Book` borrows across `install_snapshot`, `apply`, `begin_rebuild`, and `invalidate` while preventing concurrent mutation during an active `BookView` borrow, the models treat each public method call and its immediate return value as one atomic transition rather than interleaving internal array writes.

Verification splits into two complementary levels so sequence-lifecycle invariants and price-coverage soundness can each be explored exhaustively within finite state domains:

* **`L0_lifecycle`** transcribes `BookState` transitions (`AwaitingSnapshot`, `Bridging`, `Live`, `Invalid`), `BinanceSpot` and `BinanceUsdM` sequence classification, stale-event suppression, `pu` bridge continuity, generation rebuilds, atomic batch rejection, and point-in-time `view` freshness checks.
* **`L1_coverage`** refines stored level knowledge against hidden complete exchange books that remain consistent with the accepted snapshot and delta prefix, proving that partial snapshots never cause the reducer to report a false quantity or a false top-of-book price.

## State Model and Bounds

Archived TLC runs exercised eight finite configurations—four for `L0` covering 19 `Next` actions and four for `L1` covering 15 `Next` actions—anchored to 425 audited source locations:

| Archived Configuration | States Generated | Distinct States | Diameter (Depth) | Focus |
| --- | ---: | ---: | ---: | --- |
| `L0_BookState` | 111,457 | 1,392 | 6 | Baseline Spot lifecycle, generation, and view freshness |
| `L0_SpotScaling` | 3,042,646 | 8,499 | 6 | Expanded Spot sequence, stale-event, and gap domains |
| `L0_UsdM` | 115,809 | 1,880 | 6 | Baseline USD-M `Bridging`/`Live` and `pu` rules |
| `L0_UsdMScaling` | 2,220,496 | 10,524 | 6 | Expanded USD-M sequence and failure combinations |
| `L1_BookCoverage` | 45,523 | 1,471 | 6 | Two-sided truncated snapshot and hidden outer levels |
| `L1_Ask` | 45,523 | 1,471 | 6 | Ask-side symmetry, boundary deletion, and `Empty` vs `Unknown` |
| `L1_AskScaling` | 585,796 | 8,244 | 6 | Expanded ask price domain and out-of-coverage updates |
| `L1_BidScaling` | 519,202 | 7,050 | 6 | Expanded bid price domain and out-of-coverage updates |

All eight configurations completed with zero invariant violations and full action coverage before closing each level.

## Core Invariants

* **Witness soundness for known queries (`L1`)**: Whenever `quantity(side, price)` returns `Known(q)` or `best_bid()` / `best_ask()` returns `Known(level)` or `Empty`, every hidden complete order book compatible with the accepted observation history contains that exact quantity or best level.
* **Necessary uncertainty outside coverage (`L1`)**: Whenever a query returns `Unknown`, there exist compatible complete books that disagree on that level or top-of-book price (for example, when all covered levels inside a truncated snapshot are deleted while deeper uncovered levels remain). Consequently, a `Live` book with a valid `BookView` may legitimately return `BestLevel::Unknown`.
* **Failure atomicity and generation isolation (`L0`)**: Rejected snapshots, sequence gaps, invalid batches, and mismatched keys/generations preserve the last committed observation witness (or invalidate only the current generation when required) without leaking partial updates.
* **Creation-time view freshness (`L0`)**: `view(now, maximum_age)` checks clock monotonicity and `now - received <= maximum_age` strictly at view creation; `BookView.age_ns` remains frozen afterward rather than mutating retroactively as external time advances.

## Transitions and Scope Boundaries

These finite models verify state-level and epistemic contracts, not low-level data structures or end-to-end trading pipelines. Specifically, `L0` and `L1` do not model:

1. Fixed-array binary search, 6-bit duplicate filters, cyclic small-batch planning, or slice-move mechanics (verified instead by `tickwire-book` differential and work-counter tests).
2. Unbounded `u64` arithmetic edges, custom third-party `SequencePolicy` implementations, or full deductive refinement proofs.
3. Network snapshot retry loops, cross-connection `DepthSync` buffering, shared-memory IPC publication, or downstream risk-admission gates.
4. Independent snapshot-only BBO feeds (Bybit and OKX), which follow their own contract in [spot-quote-input.md](spot-quote-input.md).

## Verification and References

```sh
./scripts/check-tla.sh
cargo test --locked -p tickwire-book
```

* [Bounded order book specification](book-core.md)
* [Public spot top-of-book specification](spot-quote-input.md)
* [Book state model verification plan](../../tla/execution_models/book_state/PLAN.md)
* [L0 lifecycle model output and coverage](../../tla/execution_models/book_state/L0_lifecycle/05_output.md)
* [L1 coverage model output and coverage](../../tla/execution_models/book_state/L1_coverage/05_output.md)
* [Repository TLA+ check script](../../scripts/check-tla.sh)
