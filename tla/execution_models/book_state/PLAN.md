# Book state and coverage verification plan

Target scope is the public lifecycle and query semantics of `tickwire-book` (source SHA-256 `37beb8dcd515b76a8c3bec0a2cd46a1c3c2431375276810b45818b8b35fd04a3`). Level-array sorting, binary search, and merge benchmarks are verified separately and not re-proved here.

## Call and concurrency boundaries

`install_snapshot`, `apply`, `begin_rebuild`, and `invalidate` each take `&mut Book`, while `view` returns a `BookView` borrowing `&Book`; safe Rust borrow checking forbids concurrent mutation while a `BookView` is live. Every public call and its return value form one atomic action rather than racing array writes.

`view` checks staleness at creation time (`BookView.age_ns` captures age at construction). A `Live` book may still report `BestLevel::Unknown`: having a fresh `BookView` and having a `Known` best price are distinct conditions, and the model neither invents implicit background expiration nor embeds downstream risk-gate policy.

## Verification ladder

| Level | Focus | State and scope |
| --- | --- | --- |
| `L0_lifecycle` | Lifecycle, identity, sequence policy, and failure atomicity | `generation`, four book states, snapshot/delta watermarks, ingress timestamps, and per-side content witnesses across Spot and USD-M |
| `L1_coverage` | `Known` vs. `Unknown` price-level knowledge | Bounded price grid, snapshot coverage window, explicit delta updates, and hidden out-of-window levels verifying `best` and `quantity` |
| Follow-up | Feed synchronization and execution gating | Covered in dedicated upstream/downstream models (`binance_depth_sync`, `quote_delivery`) |

L0 transcribes the lifecycle and sequence policies directly across stale identities, sequence gaps, duplicates, and invalid payload batches. L1 refines side content into bounded price grids with hidden out-of-window levels compatible with the snapshot coverage contract. Every configuration runs single-worker exhaustive BFS with action coverage enabled.
