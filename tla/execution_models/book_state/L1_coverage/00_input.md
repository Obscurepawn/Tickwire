# L1 input: bounded price grid and coverage knowledge

Continues from [L0 lifecycle](../L0_lifecycle/05_output.md), refining per-side level storage and `Known` / `Unknown` query results while retaining `generation`, lifecycle states, and level-capacity failure transitions against `crates/tickwire-book/src/lib.rs` (SHA-256 `37beb8dcd515b76a8c3bec0a2cd46a1c3c2431375276810b45818b8b35fd04a3`).

| Source | Modeled semantics |
| --- | --- |
| `crates/tickwire-book/src/lib.rs:239-260` | Price-interval containment for `Complete`, `Through`, and `Unknown` |
| `crates/tickwire-book/src/lib.rs:574-586` | `Book` fields and per-side storage |
| `crates/tickwire-book/src/lib.rs:620-642` | Snapshot installation and coverage boundary establishment |
| `crates/tickwire-book/src/lib.rs:644-710` | Accepted delta updates, net level capacity, and failure atomicity |
| `crates/tickwire-book/src/lib.rs:712-735` | Rebuild clearing and explicit invalidation |
| `crates/tickwire-book/src/lib.rs:737-761` | `view` `Live` gate (expanded in L0) |
| `crates/tickwire-book/src/lib.rs:850-858` | `Through` anchored at the worst nonzero snapshot level; empty partial snapshot maps to `Unknown` |
| `crates/tickwire-book/src/lib.rs:902-929` | Distinct knowledge rules for `quantity` vs. `best` |

`world` represents the full book side at the latest accepted watermark (including levels outside a partial snapshot's `Through` cutoff), under the caller contract that snapshots accurately set `complete` and accepted deltas carry all level changes for that version.
