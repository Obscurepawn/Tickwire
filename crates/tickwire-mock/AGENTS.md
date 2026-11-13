# tickwire-mock

`tickwire-mock` (`unsafe_code = "forbid"` via workspace lints) provides deterministic, in-memory test doubles
(`MockSql`, `MockKv`, `MockJournal`, `MockExchange`) and a synchronous reference harness (`MockRuntime<N, T>`)
implementing the `tickwire-execution::ports` traits without wall-clock sleeps, threads, or network sockets.
Integration tests, examples, and TLA+ trace checks use this crate to inject persistence and transport faults at exact
port boundaries while keeping `tickwire-execution::Engine` completely allocation-free.

## Invariants and Change Guidelines

* **Fault injection at exact port boundaries**: Keep `fail_next_commit`, `lose_next_receipt`, `fail_next_fact`,
  `fail_writes`, `unavailable`, and `SubmitOutcome` (`NoBytesAccepted`, `MayHaveSent`, `PendingResponse`,
  `Acknowledged`) in [src/lib.rs](src/lib.rs) aligned one-to-one with real WAL and `tickwire-transport` failure modes;
  `lose_next_receipt` must persist the `DurableRecord` before returning `ServiceError::Unavailable`.
* **Ordered journal event log**: Preserve true append interleaving in `MockJournal::events()`
  (`MockJournalEvent::Intent` and `MockJournalEvent::Fact`) and persist `ExecutionFact::AbortedBeforeSend` in
  `MockJournal` before applying it to `Engine` during `SubmitOutcome::NoBytesAccepted`.
* **Immutable dependency check on commit**: `MockJournal::commit` must verify that `MockKv` already holds matching
  dependency bytes for `request.recovery_object` and reject conflicting `(OrderKey)` or `(domain, sequence)` records
  with `ServiceError::Conflict`.
* **Frozen recovery posture**: Reconstructing `MockRuntime::recover` from `MockJournal` must replay intents and facts
  in order and leave `Engine` frozen when unconfirmed intents remain, matching
  [spot-roundtrip.rs](examples/spot-roundtrip.rs).

## Verification and Specifications

* **Source and examples**: [src/lib.rs](src/lib.rs) · [examples/spot-roundtrip.rs](examples/spot-roundtrip.rs)
* **Integration tests**: [tests/services.rs](tests/services.rs)
* **Specifications and models**: [execution-core.md](../../specs/implementation/execution-core.md) ·
  [mock-abort.md](../../specs/implementation/mock-abort.md) ·
  [mock_abort](../../tla/execution_models/mock_abort/ROADMAP.md) ·
  [submission](../../tla/execution_models/submission/ROADMAP.md)

```sh
cargo test --locked -p tickwire-mock --all-targets
cargo run --locked -p tickwire-mock --example spot-roundtrip
./scripts/check-tla.sh mock_abort
```
