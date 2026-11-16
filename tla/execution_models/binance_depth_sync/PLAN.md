# Binance `depth_sync` verification plan

The goal is to verify how the production `DepthSync` coordinator protects order-book synchronization epochs across asynchronous network callbacks: stale connections and stale snapshot tokens must never mutate the active book, stream subscription and the first buffered delta must precede snapshot requests, buffer drops or gaps must force a rebuild, and any failure mid-replay must never expose a partially live book.

Source scope is `crates/tickwire-binance/src/depth_sync.rs`, paired with book policies and storage states in `crates/tickwire-book/src/lib.rs` (see the [Binance depth synchronization specification](../../../specs/implementation/binance-depth-sync.md) and [book state model](../../../specs/implementation/book-state-model.md)). The model is archived independently without modifying production source or test evidence.

The concurrency model follows single-owner exclusive `&mut self` method boundaries: each public call runs to completion before the next begins. Network callbacks may arrive late, duplicate, or reference a closed connection, but no concurrent public call can interleave mid-way through a `FIFO` snapshot replay loop. The model folds the entire `FIFO` atomically via a recursive pure function so it does not fabricate intermediate states unreachable through safe Rust borrowing.

| Level | Focus | Deliverable |
| --- | --- | --- |
| `L0_coordinator` | Bounded delta buffer, connection/sync generations, request tokens, deadlines, and original delta ingress timestamps | Spot and USD-M primary and scaled-buffer configurations |
| Follow-up (planned) | Session scheduling and multi-source publication gating | Separate higher-level model once composed |

A single quantity variable represents a concrete price-level quantity assignment that passed sequence validation, acting as a content witness rather than full level arrays or out-of-window unknown depth. Level sorting, exact decimal parsing, TLS/framing, catalog eligibility, and account risk gates lie outside this level; finite-depth `Unknown` semantics are covered by `book_state/L1_coverage` and crate regression tests.
