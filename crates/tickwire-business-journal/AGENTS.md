# tickwire-business-journal

`tickwire-business-journal` (`#![forbid(unsafe_code)]`) couples `tickwire-journal` physical durability,
`tickwire-journal-codec` logical chaining, and `tickwire-recovery` closure verification into `BusinessJournal<S>` and
a frozen-by-default `Runtime` wrapper over `tickwire-execution::Engine`. Outbound gateways and execution hosts append
`PreparedIntent` admissions and `ExecutionFact` observations here and reconstruct uncommitted vs. in-flight orders
after a crash. Strategy permission and recovery verification are injected via `CommitAuthorizer` with no permissive
default.

## Invariants and Change Guidelines

* **Bootstrap binding and quarantine on mismatch**: `FileBackend` in [src/backend.rs](src/backend.rs) stores
  `BUSINESS-BOOTSTRAP.json` under `FileStore` 's single-writer directory lock; `BusinessJournal::recover` in
  [src/lib.rs](src/lib.rs) quarantines startup if the bootstrap is missing, differs from the expected
  `BusinessConfig`, rolls back `writer_exec_epoch`, or disagrees with physical WAL `seq`/`exec_epoch`.
* **Synchronized closure before WAL commit**: In [src/runtime.rs](src/runtime.rs) and [src/lib.rs](src/lib.rs),
  `CommitAuthorizer::authorize` must return a `VerifiedClosure` bound to the exact `business_digest` and
  `intent_payload` before `BusinessJournal` appends and fsyncs the `PreparedIntent` batch.
* **Frozen-by-default restart and `MayHaveSent` posture**: Reopened runtimes in [src/runtime.rs](src/runtime.rs) start
  frozen for new risk and treat every durable `PreparedIntent` lacking a `AbortedBeforeSend` fact as potentially sent
  to the venue (`MayHaveSent`), preserving its capital hold until explicit venue reconciliation completes.
* **Poisoning on partial I/O**: Poison `BusinessJournal` on any physical append or sync error so callers cannot
  observe divergent in-memory `LogicalChain` state.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/backend.rs](src/backend.rs) ·
  [src/runtime.rs](src/runtime.rs)
* **Integration tests**: [tests/runtime.rs](tests/runtime.rs)
* **Specifications and models**: [business-journal.md](../../specs/implementation/business-journal.md) ·
  [command-binding.md](../../specs/implementation/command-binding.md) ·
  [mock-abort.md](../../specs/implementation/mock-abort.md) ·
  [business_journal](../../tla/execution_models/business_journal/ROADMAP.md) ·
  [gateway](../../tla/execution_models/gateway/ROADMAP.md)

```sh
cargo test --locked -p tickwire-business-journal --all-targets
./scripts/check-tla.sh business_journal
```
