# Formal Verification with TLA+

Tickwire uses finite-state TLA+ specifications and the TLC model checker to exhaustively verify concurrency, crash recovery, shared-memory reclamation, and network failure boundaries where interleavings are hard to cover with conventional tests alone.

The [November 2026 source audit](audits/2026-11-05/README.md) documents the latest single-host trading-route verification results, counterexamples discovered and fixed during modeling, and current crate-by-crate source anchors.

## Running TLC Models

From the workspace root, verify a single model family (requires a Java runtime):

```sh
./scripts/check-tla.sh submission
```

Or run the entire formal verification suite across all model families:

```sh
./scripts/check-tla.sh
```

For each family, the runner verifies the SHA-256 digest of the pinned TLC 1.7.4 archive (`tla/tools/tla2tools.jar`), copies the `.tla` modules and `.cfg` bounds into a clean temporary directory, and runs exhaustive state-space exploration. A passing run requires zero states left on the queue, zero invariant violations, and nonzero successor coverage across modeled actions after the final configuration completes.

## Maintaining Source Alignment

See the [Model Family Roadmap](ROADMAP.md) to locate the TLA+ models governing a specific subsystem boundary. Whenever Rust code changes an ordering rule, admission gate, shared-memory watermark, or WAL recovery condition, verify the model's abstractions and source-line anchors before relying on prior TLC results.
