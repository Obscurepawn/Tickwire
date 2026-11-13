# tickwire-execution

`tickwire-execution` (`#![forbid(unsafe_code)]`) provides a zero-I/O, allocation-free, fixed-capacity (`Engine<N, T>`)
order execution reducer and the service port traits (`ports::Journal`, `ports::Exchange`, `ports::ConfigStore`,
`ports::ObjectStore`) shared by deterministic mocks and formal submission models. Callers drive `Engine` through
`reserve`, local `CommitReceipt` confirmation, outbound send transitions, cancellation states, and `ExecutionFact`
replay, while network transport, timers, and physical storage remain outside this crate.

## Invariants and Change Guidelines

* **Zero-allocation fixed-capacity transitions**: Keep `Order<T>` and `Engine<N, T>` in [src/lib.rs](src/lib.rs)
  backed by inline arrays with checked `i128` budget math (`quantity * unit_budget + fee_budget`) so state transitions
  never allocate on the heap or wrap on overflow.
* **Gate and durability preconditions before send**: `Gate::permits` requires live lease (`now < lease_until`),
  deadline (`now < r.deadline`), matching `epoch`, `strategy_generation`, `config_revision`, recovery completion, and
  owner isolation; outbound submission requires a matching `CommitReceipt` (`local_durable == true`) and transitions
  through `MayHaveSent` before `Confirmed`.
* **Held-budget retention under uncertainty and late fills**: Only `AbortedBeforeSend` on a `NotSubmitted` order with
  an exact `DurableRecord` match releases reserved capital without venue evidence (any mismatch freezes `Engine`);
  after a `Terminal` report, keep `(observed_filled - filled) * unit_budget` and unreleased fee budget held until
  matching trade `Fill` and `Fee` facts arrive.
* **Idempotent economic deduplication**: `Order::record_event` in [src/lib.rs](src/lib.rs) and `ExecutionFact::apply`
  in [src/ports.rs](src/ports.rs) ignore exact duplicate `trade_id` or fee `id` replays and reject conflicting
  payloads for the same identifier with `Error::ConflictingEvent`.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/ports.rs](src/ports.rs)
* **Integration tests**: [tests/lifecycle.rs](tests/lifecycle.rs)
* **Specifications and models**: [execution-core.md](../../specs/implementation/execution-core.md) ·
  [command-binding.md](../../specs/implementation/command-binding.md) ·
  [mock-abort.md](../../specs/implementation/mock-abort.md) ·
  [submission](../../tla/execution_models/submission/ROADMAP.md) ·
  [mock_abort](../../tla/execution_models/mock_abort/ROADMAP.md) ·
  [business_journal](../../tla/execution_models/business_journal/ROADMAP.md)

```sh
cargo test --locked -p tickwire-execution --all-targets
./scripts/check-tla.sh submission
```
