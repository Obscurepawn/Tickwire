# Scalar Execution Journal, Admission Protocol, and Recovery Runtime

`tickwire-business-journal` couples the scalar execution reducer (`Engine`) to `tickwire-journal` and `tickwire-journal-codec` through `BusinessJournal<S: Backend>` and `Runtime<S, A: CommitAuthorizer, E: Exchange, N, T>`. It persists schema-2 scalar execution records—`PreparedIntent`, `Fill`, `Fee`, `Terminal`, and `AbortedBeforeSend`—for a single `(venue, account, product)` risk domain, distinct from the multi-asset `tickwire-account-journal`.

`BusinessJournal` owns bootstrap verification, 1-to-1 alignment between logical business sequence and physical WAL sequence, intent deduplication, and `LogicalChain` continuity. `Runtime` orchestrates two-stage gate checks, recovery-closure authorization, pre-send durability, deterministic `NoBytesAccepted` abort recording, and crash replay. A private `WalAppendReceipt` proves only that a record belongs to a locally synchronized WAL prefix; `CommitAuthorizer` and the execution gateway remain responsible for live lease validity and wire transmission authority.

## State Model and Bounds

At creation, `BusinessJournal` persists an immutable `BootstrapEnvelope` JSON document (protected by an embedded `SHA-256` digest) before writing physical WAL segments. Every subsequent business event is stored as a single physical `DataRecord` batch so logical and physical positions never diverge.

| Artifact or Invariant | Enforced Structure / Mapping |
| --- | --- |
| `BootstrapEnvelope` (`version = 1`) | Binds `codec_schema = 2`, physical `Config` (`risk_domain_id`, `journal_epoch`, `segment_max_bytes`), `venue`, `account`, `product`, non-negative decimal `initial_capital`, and `sha256` |
| Physical-to-logical alignment | 1 schema-2 `Record` (`<= 236` bytes) per physical `DataRecord`; `record.journal_seq == raw.seq`, `record.writer_exec_epoch == raw.exec_epoch`, matching `risk_domain_id` and `journal_epoch` |
| `CommitProfile` enforcement | `LocalDurable` (`1`) is active; historical records tagged `LegacySyncReplica` (`2`) fail replay with `Error::UnsupportedProfile` |
| `WalAppendReceipt` | Binds `journal_seq`, `business_digest`, `logical_payload_hash`, `logical_chain_hash`, and covering `PhysicalFrontier`; constructible only after physical `append_batch` sync succeeds |

## Core Invariants

* **Epoch separation across order admission and late economic facts**: A newly committed `PreparedIntent` must name the active writer as its creator (`intent_exec_epoch == writer_exec_epoch`). An exact `DurableRecord` retry—even after writer failover—returns the original `journal_seq` and logical hashes (paired with a current covering `PhysicalFrontier`) without appending a duplicate record, whereas a different payload sharing an existing `intent_id` or `intent_seq` immediately poisons the journal (`Error::ConflictingIntent` / `Error::ConflictingSequence`). Subsequent `Fill`, `Fee`, `Terminal`, and `AbortedBeforeSend` records preserve the originating order's `(intent_id, intent_seq, intent_exec_epoch)` while stamping the current `writer_exec_epoch` in the outer envelope.
* **Write-before-reduce and double-gated order submission**: `Runtime::submit` refuses work while `engine.frozen()`, refreshes the live execution gate via `authorizer.current_gate()`, reserves budget in `Engine`, obtains a `VerifiedClosure` bound to the full `command_digest` via `authorizer.authorize(...)`, and synchronizes `PreparedIntent` to the WAL. Immediately after WAL sync, `Runtime::submit` refreshes `current_gate()` *a second time*—re-verifying strategy generation, writer epoch, configuration revision, deadline, and lease validity—before calling `accept_commit` and `begin_send` and invoking `exchange.submit(request)`. Inbound venue `Fill`, `Fee`, and `Terminal` facts in `record_fact` are likewise synchronized to the WAL before `Engine` mutates in-memory balances or reservations.
* **Trusted-only `AbortedBeforeSend` release**: Once `begin_send` marks an order as potentially wire-visible, ambiguous transport failures keep its capital reservation locked and freeze the runtime. Only a direct `SendOutcome::NoBytesAccepted` return from the trusted `exchange.submit` call allows `Runtime::submit` to persist `AbortedBeforeSend` and release the reservation in `Engine`; external callers attempting to pass `AbortedBeforeSend` into `record_fact` are rejected and freeze the runtime.

## Operations and Failure Semantics

* **Fail-closed freeze and poison behavior**: Any physical sync error, conflicting intent binding, mismatched `(venue, account, product)` identity, invalid reducer transition, arithmetic overflow, or failed post-sync gate refresh freezes `Engine` (and poisons `BusinessJournal` when physical or identity state may be ambiguous), preventing any further order reservations until a clean recovery and reactivation cycle completes.
* **Frozen, side-effect-free crash replay**: `Runtime::recover` initializes a frozen `Engine`, validates `BootstrapEnvelope` against independently supplied caller expectations, recovers the physical WAL (verifying any independent `PhysicalFrontier` before torn-tail truncation), recomputes `LogicalChain` from genesis, invokes `authorizer.verify_recovery(...)` on every `PreparedIntent` without synthesizing missing closure objects or contacting the exchange, and replays every event in strict sequence order. Even an empty journal recovers in the frozen state; any missing recovery closure, unsupported `CommitProfile`, identity mismatch, or reducer failure aborts `recover` before publishing a `Runtime` instance.

## Verification and References

```sh
cargo test --locked -p tickwire-business-journal
cargo clippy --locked -p tickwire-business-journal --all-targets -- -D warnings
```

Integration and subprocess tests exercise exact retry idempotency across writer takeover, conflicting intent poisoning, post-sync gate expiration, `NoBytesAccepted` vs. ambiguous send outcomes, partial `AbortedBeforeSend` writes, sync failures, subprocess `SIGKILL` recovery, and directory/frontier tamper detection.

* Business journal and runtime state machine: [lib.rs](../../crates/tickwire-business-journal/src/lib.rs), [runtime.rs](../../crates/tickwire-business-journal/src/runtime.rs)
* Runtime and crash-recovery tests: [runtime.rs](../../crates/tickwire-business-journal/tests/runtime.rs)
* Formal verification: [Business journal TLA+ model](../../tla/execution_models/business_journal/PLAN.md)
* Related specifications: [Scalar business journal codec](journal-codec.md), [Physical write-ahead log](journal.md), [Recovery closures](recovery-closure.md), [Ordered account journal](account-journal.md)
