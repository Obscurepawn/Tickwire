# Scalar Order and Reservation Reducer

`tickwire-execution` implements a deterministic, single-owner state reducer (`Engine<N, T>`) for the lifecycle of an order intent and its claim on a bounded, single-settlement-asset capital pool. Companion crate `tickwire-mock` supplies deterministic in-memory doubles (`MockRuntime<N, T>`, `MockSql`, `MockKv`, `MockJournal`, and `MockExchange`) so tests can exercise crash recovery, lost commit receipts, pre-send aborts, and out-of-order fills without wall-clock sleeps or hidden background retries. Production multi-asset accounting (`tickwire-account`), physical segment storage (`tickwire-journal`), and remote venue reconciliation have their own dedicated owners.

Keeping `Engine<N, T>` free of heap allocation, `unsafe` code, and direct I/O isolates the fundamental safety rules of order execution: reserving worst-case budget prior to persistence, marking an order `MayHaveSent` *before* handing it to transport, deduplicating fills and fees independently, and latching `frozen` whenever evidence conflicts or actual liability exceeds capital.

## State Model and Bounds

`Engine<N, T>` is parameterized statically by maximum retained orders `N > 0` and maximum deduplicated economic events `T > 0` per order, scoped to a single `Domain { venue, account, product }`.

| Type / Port | Fields and Bounds | Responsibility |
| --- | --- | --- |
| `OrderRequest` | `OrderKey`, nonzero 32-byte `command_digest`, `quantity > 0`, `unit_budget > 0`, `fee_budget >= 0`, `epoch`, `strategy_generation`, `config_revision`, `deadline`, nonzero `recovery_object` | Immutable scalar reservation request; `reserve` checks `quantity * unit_budget + fee_budget` in `i128` before mutating state. |
| `Order<T>` | `sequence`, `durable`, `SendState`, `Option<Terminal>`, `CancelState`, `filled`, `observed_filled`, `spent`, `fees_paid`, `fee_released`, `events: [Option<EconomicEvent>; T]` | Tracks per-order transport state, cumulative fills, realized spend, and inline fill/fee deduplication slots. |
| `Engine<N, T>` | `capital >= 0`, `Gate`, `orders: [Option<Order<T>>; N]`, `next_sequence`, `spent`, `held`, `frozen` | Computes `available() = capital - spent - held` and blocks new `reserve` / `begin_send` calls whenever `frozen` or `!gate.permits(r)`. |
| `ConfigStore` (`MockSql`) | `load`, `compare_exchange(expected, payload)` | Revisions advance strictly by compare-and-swap (`ServiceError::Conflict` on stale revision). |
| `ObjectStore` (`MockKv`) | `put_immutable(key, payload)`, `get(key)` | Write-once content store; accepts identical bytes at an existing key and rejects differing bytes with `ServiceError::Conflict`. |
| `Journal` (`MockJournal`) | `commit(DurableRecord) -> CommitReceipt`, `append_fact(ExecutionFact)` | Binds `(domain, sequence)` and `recovery_object` on commit, and appends unified `MockJournalEvent` entries in exact arrival order. |

## Core Invariants

* **Creating an order only reserves funds**: `Engine::reserve` validates domain, uniqueness, `Gate::permits`, and `budget <= available()`, leaving the order in `SendState::NotSubmitted` (`durable = false`). Network output remains forbidden until `accept_commit` verifies a `CommitReceipt` with `local_durable == true` and an exact `DurableRecord` match.
* **`MayHaveSent` precedes adapter invocation**: `begin_send` transitions `Order.send` from `NotSubmitted` to `MayHaveSent` *before* returning the `OrderRequest` to call `Exchange::submit`. Consequently, losing the adapter callback or crashing mid-call leaves the order uncertain rather than falsely unsent.
* **No automatic retry on timeout**: After `begin_send`, transport outcomes map to `NoBytesAccepted` (release after durable abort), `PendingResponse` (retain reservation while allowing other orders within remaining `available()`), `Acknowledged` (`SendState::Confirmed`), or `MayHaveSent` (`uncertain`, which calls `freeze()`). Response timeouts never authorize automatic resubmission.
* **Independent fill and fee deduplication**: Fills (`trade_id`) and fees (`id`) occupy separate `EconomicEvent` variants in `Order<T>`, so a later fill carrying a numerically smaller `trade_id` still books normally. `spent` sums fill `cost` and `fees_paid`. `held()` covers remaining executable quantity (`request.quantity - filled`, or `observed_filled.saturating_sub(filled)` once `terminal.is_some()`) times `unit_budget`, plus `max(0, fee_budget - fees_paid)` unless `fee_released` is set. Releasing an unsent order's fee budget sets `fee_released = true` instead of adding to `fees_paid`, and any recorded fee—even `0`—confirms the order and invalidates any later no-bytes claim.
* **Real deficits book; anomalies freeze without partial updates**: Authentic fills and fees are applied even when `spent + held > capital`, allowing `available()` to go negative while latching `frozen = true`. Conflicting duplicate IDs (`ConflictingEvent`), full `T` slots (`EventCapacity`), invalid quantities, or `i128` overflow freeze `Engine` without partially mutating order or pool totals.

## State Transitions and Failure Semantics

In `MockRuntime<N, T>`, every incoming `ExecutionFact` (`AbortedBeforeSend`, `Fill`, `Fee`, `Terminal`) is persisted through `MockJournal::append_fact` *before* `fact.apply(&mut engine)` executes:

1. **Persistence failure**: If `append_fact` fails, `MockRuntime` immediately freezes `engine` and returns `RuntimeError::Service`, preserving the unapplied fact with the caller for retry.
2. **Unaccountable persisted fact**: If `append_fact` succeeds but `fact.apply` rejects the transition (for example, an invalid cumulative quantity or conflicting trade ID), `engine` freezes and the rejected fact remains in `MockJournal::events()` so subsequent recovery refuses to complete cleanly rather than silently dropping financial evidence.
3. **Crash reconstruction (`restart()`)**: `MockRuntime::restart` freezes the current `engine` first, allocates a replacement `Engine` that begins `frozen = true`, and replays `journal.events()` in exact append order (`recover_intent` for intents, `apply` for facts) before installing the replacement. Prior `Gate` readiness never survives a restart. Service tests explicitly distinguish losing a journal `CommitReceipt` from losing an exchange `ACK`, and exercise generic adapter routing across venue IDs `7` and `42`.

## Verification and References

```sh
cargo test --locked -p tickwire-execution -p tickwire-mock
cargo run --locked -p tickwire-mock --example spot-roundtrip
```

Running the `spot-roundtrip` example submits one simulated order, records a partial fill and cancellation, and reconstructs state from `MockJournal`, printing a terminal summary containing `submit_calls=1 spent=101 held=1 recovered_frozen=true`.

* Core implementation and ports: [scalar execution engine](../../crates/tickwire-execution/src/lib.rs), [service port traits](../../crates/tickwire-execution/src/ports.rs), [mock services and runtime](../../crates/tickwire-mock/src/lib.rs)
* Test suites and examples: [engine lifecycle tests](../../crates/tickwire-execution/tests/lifecycle.rs), [mock service fault-injection tests](../../crates/tickwire-mock/tests/services.rs), [spot-roundtrip example](../../crates/tickwire-mock/examples/spot-roundtrip.rs)
* Related specifications: [mock execution abort and ordered recovery](mock-abort.md), [binding a persisted intent to its command](command-binding.md), [multi-asset account state machine](account.md), [physical journal](journal.md)
