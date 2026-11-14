# Formal Verification with TLA+

Tickwire uses finite-state TLA+ specifications and the TLC model checker to exhaustively verify concurrency, crash recovery, shared-memory reclamation, and network failure boundaries where interleavings are hard to cover with conventional tests alone.

The [November 2026 source audit](audits/2026-11-05/README.md) documents the latest single-host trading-route verification results, counterexamples discovered and fixed during modeling, and current crate-by-crate source anchors.

## Running TLC Models

From the workspace root, verify one or more model families in parallel (requires a Java runtime):

```sh
./scripts/check-tla.sh submission
./scripts/check-tla.sh account account_gateway market_making_microstructure
```

Or run the entire formal verification suite across all model families in parallel:

```sh
./scripts/check-tla.sh
```

For each family, the runner verifies the SHA-256 digest of the pinned TLC 1.7.4 archive (`tla/tools/tla2tools.jar`), stages every `.tla` module and its `.cfg` bounds into an isolated temporary directory, schedules `(model, .cfg)` explorations Longest-Processing-Time-First across a bounded worker pool (`TLA_JOBS`, default auto-sized from CPU cores and `MemAvailable`; per-JVM heap capped via `TLA_MAX_HEAP`, default `2560m`), and emits logs and coverage summaries in deterministic model order. A passing run requires zero states left on the queue, zero invariant violations, and nonzero successor coverage across modeled actions after all configurations for each model complete.

## Maintaining Source Alignment

See the [Model Family Roadmap](ROADMAP.md) to locate the TLA+ models governing a specific subsystem boundary. Whenever Rust code changes an ordering rule, admission gate, shared-memory watermark, or WAL recovery condition, verify the model's abstractions and source-line anchors before relying on prior TLC results.
