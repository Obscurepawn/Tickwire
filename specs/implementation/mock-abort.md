# Mock Execution Abort and Ordered Recovery

`tickwire-mock` pairs the production scalar execution reducer (`tickwire_execution::Engine<N, T>`) with deterministic in-memory service doubles (`MockJournal`, `MockExchange`, `MockKv`, and `MockSql`) to verify pre-send transport aborts and crash recovery ordering. When `MockExchange::submit` returns `SubmitOutcome::NoBytesAccepted`, `MockRuntime` persists `ExecutionFact::AbortedBeforeSend { record }` and releases the order's reserved capital—modeling the exact persistence-before-release contract required of live gateways.

This specification defines the ordering invariants between durable intents, zero-byte abort facts, and subsequent orders that reuse freed capital during crash replay. Note the verification boundary: a synthetic `NoBytesAccepted` return exercises reducer and journal ordering transitions, whereas byte-counting proof at an actual non-blocking TLS socket is owned and tested by `tickwire-transport`, `tickwire-gateway`, and `tickwire-account-gateway`.

## State Model and Journal Views

| Type / Method | Representation | Role in Abort and Replay |
| --- | --- | --- |
| `SubmitOutcome::NoBytesAccepted` | Local transport outcome for the active `submit` call | Sole `Exchange::submit` outcome permitted to emit `ExecutionFact::AbortedBeforeSend { record }`; response timeouts (`PendingResponse` / `MayHaveSent`) never establish zero bytes. |
| `ExecutionFact::AbortedBeforeSend` | `{ record: DurableRecord }` | Binds the complete `OrderRequest` (including `OrderKey`, `epoch`, `command_digest`, budgets, and `recovery_object`) and `sequence`. |
| `MockJournal::events()` | `&[MockJournalEvent]` (`Intent(DurableRecord)` \| `Fact(ExecutionFact)`) | Authoritative unified append log preserving exact causal interleaving between intents and facts. |
| `committed()` / `facts()` | `&[DurableRecord]` / `&[ExecutionFact]` | Grouped inspection slices only; cannot reconstruct interleaved capital releases during replay. |

## Core Invariants

* **Fact durability precedes capital release**: Upon receiving `SubmitOutcome::NoBytesAccepted`, `MockRuntime` appends `ExecutionFact::AbortedBeforeSend { record }` via `MockJournal::append_fact` *before* calling `ExecutionFact::apply` (`Engine::transport_rejected_without_bytes`). If `append_fact` fails, `MockRuntime` immediately calls `engine.freeze()` and leaves the reservation locked.
* **Complete `DurableRecord` verification on abort**: `ExecutionFact::apply` compares `engine.durable_record(intent)` against the entire `record` embedded in `AbortedBeforeSend`. A missing intent (`Error::NotFound`) or any field/sequence mismatch (`Error::ReceiptMismatch`) freezes `Engine` without releasing capital.
* **Contradictory economic evidence blocks abort**: `transport_rejected_without_bytes` succeeds only when the order is in `SendState::MayHaveSent` with `filled == 0`, `observed_filled == 0`, and `terminal == None` (or idempotently on replay when the order is already `NotSubmitted`, `Terminal::Rejected`, `fee_released == true`, and zeroed across `filled`, `observed_filled`, `spent`, and `fees_paid`). Any prior fill, fee (including zero), or contradictory terminal status rejects the abort and freezes `Engine`.
* **Unified append-order replay prevents capital-reuse reordering**: If order `A` aborts before send and order `B` subsequently reserves the capital freed by `A`, `MockJournal::events()` replays `Intent(A) -> Fact(AbortedBeforeSend(A)) -> Intent(B)` in exact causal sequence. Replaying all intents ahead of all facts would double-count `A` and `B`'s reservations simultaneously, risking false capacity or `i128` overflow failures.

## State Transitions and Failure Semantics

### Idempotent Commits, Lost Receipts, and Atomic `restart()`

In `MockJournal::commit`, re-submitting an identical `DurableRecord` returns a `CommitReceipt` without appending a duplicate `MockJournalEvent::Intent`, whereas setting `lose_next_receipt` injects `ServiceError::Unavailable` *after* the intent has entered `events()`—distinguishing a lost local storage acknowledgement from an uncommitted request.

When `MockRuntime::restart()` simulates an execution-process crash and recovery:

1. It immediately calls `self.engine.freeze()` so a failed recovery attempt can never leave the pre-crash engine authorized to admit new orders.
2. It constructs a replacement `Engine` initialized in the `frozen` state (`Gate` permissions from before the crash do not restore live readiness).
3. It replays every `MockJournalEvent` from `journal.events()` (`recover_intent` for `Intent`, `fact.apply` for `Fact`) and installs `self.engine = engine` only after the complete log replays without error.

Regression tests and the bounded TLA+ `mock_abort` model verify four critical fault scenarios: replay re-reserving an aborted order when `AbortedBeforeSend` was lost, failed persistence of `AbortedBeforeSend` leaving capital locked and `Engine` frozen, arithmetic overflow from reordering a terminal/abort release behind a dependent reservation, and retained `frozen` state after a failed recovery.

## Verification and References

```sh
cargo test --locked -p tickwire-mock --test services
cargo test --locked -p tickwire-execution --test lifecycle
```

* Source modules: [MockRuntime and MockJournal](../../crates/tickwire-mock/src/lib.rs), [ExecutionFact::apply](../../crates/tickwire-execution/src/ports.rs), [transport_rejected_without_bytes](../../crates/tickwire-execution/src/lib.rs)
* Tests and formal verification: [mock service and recovery ordering tests](../../crates/tickwire-mock/tests/services.rs), [TLA+ mock_abort model plan](../../tla/execution_models/mock_abort/PLAN.md)
* Related specifications: [scalar order and reservation reducer](execution-core.md), [durable command sending](gateway.md), [account gateway](account-gateway.md)
