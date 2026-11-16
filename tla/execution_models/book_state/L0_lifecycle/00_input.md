# L0 input: lifecycle and sequence policies

Entry level of the `book_state` ladder against `crates/tickwire-book/src/lib.rs` (SHA-256 `37beb8dcd515b76a8c3bec0a2cd46a1c3c2431375276810b45818b8b35fd04a3`).

| Source | Modeled semantics |
| --- | --- |
| `crates/tickwire-book/src/lib.rs:182-194` | Spot policy: check `U <= u` first, then stale (`u <= L`) and contiguous/bridging (`U <= L + 1`) |
| `crates/tickwire-book/src/lib.rs:198-224` | USD-M policy: bridge on `U <= L <= u` (requiring `pu` present), live continuation on `pu == L` |
| `crates/tickwire-book/src/lib.rs:590-605` | Initial `AwaitingSnapshot` state with empty sides and watermarks |
| `crates/tickwire-book/src/lib.rs:620-642` | Snapshot identity/state check, validation, two-sided install, and transition to `Bridging` |
| `crates/tickwire-book/src/lib.rs:644-710` | Delta identity/state/policy/clock check, validation plan, and atomic two-sided commit |
| `crates/tickwire-book/src/lib.rs:712-725` | Strictly increasing `generation` rebuild (`begin_rebuild`) |
| `crates/tickwire-book/src/lib.rs:729-735` | Same-generation explicit invalidation (`invalidate`) |
| `crates/tickwire-book/src/lib.rs:737-761` | Creation-time `Live` and age checks in `view` |
| `crates/tickwire-book/src/lib.rs:771-779` | Side-effect-free identity guard |
| `crates/tickwire-book/src/lib.rs:781-790` | Validation/sequence failure transitions only `state` to `Invalid` |

Exclusive `&mut Book` borrowing serializes all mutations, while `BookView` borrows `&Book` for the duration of a read.
