# Broadcast ring verification ladder

Verifies the single-publisher, independent-consumer cursor, and zero-copy borrow reclamation protocol implemented in `tickwire-shm`. `L0_publish_borrow_reclaim` checks publication, slot borrowing, out-of-order release prefix tracking, cached minimum watermarks, and `Freeze` at the real minimum ring capacity (`64`).

Publisher mutations are serialized via `&mut self`, while each subscriber owns an independent `RefCell<Reader>` and its dedicated writable cursor mapping concurrently with the publisher. Publication proceeds through `claim`, per-slot payload writes, and `Release` publication; readers read ordinary slot bytes only after an `Acquire` load observes the published frontier. Linux `memfd` sealing, read-only page protections, and CPU atomic memory ordering are verified separately by the Rust/Linux test suite.

| Level | Focus | Input scope |
| --- | --- | --- |
| `L0_publish_borrow_reclaim` | Active borrows never overwritten, contiguous publish/reclaim watermarks, pending JOIN gate, and live cursor `detach` | `Capacity = 64`; 1 or 2 readers; borrow window 1 or 2; cold start (`0`) and reachable full-ring wrap (`64`) prefixes |
| `L1_identity_rotation` | Subscription generation rotation, stale cursors, `Freeze`, and bank cutover | Planned |
| `L2_exit_reclaim` | Subscriber fail-stop and process-exit mapping reclamation | Planned |

L0 models arbitrary subscriber stalls and leaked borrow guards directly by omitting fairness assumptions: a stalled or leaked reader blocks slot reclamation rather than permitting premature overwrite.
