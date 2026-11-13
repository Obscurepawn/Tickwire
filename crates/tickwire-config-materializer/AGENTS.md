# tickwire-config-materializer

`tickwire-config-materializer` (`#![forbid(unsafe_code)]`) owns `Local<F, S>`, joining `Files`
(`tickwire-config-fs::Store`) with `tickwire-config-journal::Journal<S>` to stage immutable
`ProjectionObject` blobs, render `desired.toml` and `effective.toml`, archive `last-good` snapshots,
and emit durable phase `Receipt`s. Control-plane authorization and live worker handshakes remain
outside this crate.

## Invariants and Change Guidelines

* **Drift verification before and around every mutation**: `Local::open` and every mutating step
  (`prepare`, `write_desired`, `observe_runtime`, `write_effective`) in [src/local.rs](src/local.rs)
  run `verify_all` to cross-check on-disk `objects/`, `desired.toml`, `effective.toml`, and
  `last-good/` hashes against journaled `FileFact` records, returning `Error::Drift` and latching
  `poisoned` on any unrecorded modification.
* **Crash-resumable two-store ordering**: Install immutable `objects/<hash>` before recording
  `Event::Prepared`, and install rendered `desired.toml` or `effective.toml` (guarded by
  `expected_previous` CAS hash) before appending `Event::Desired` or `Event::Effective` so an
  interrupted second write reconciles deterministically on reopen.
* **Bounded retention**: Enforce `OBJECT_BYTES` (`4 MiB`), `MAX_FILE_BYTES`, and total
  `RETAINED_BYTES` (`512 MiB`) budgets in `Local::capacity` ([src/local.rs](src/local.rs)),
  reserving headroom for active mutable targets and unrecorded `last-good` backups before accepting
  a new `Prepared` operation.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/local.rs](src/local.rs) ·
  [tests/sequence.rs](tests/sequence.rs) · [tests/linux.rs](tests/linux.rs)
* **Specifications and models**:
  [config-materializer.md](../../specs/implementation/config-materializer.md) ·
  [config-local-journal.md](../../specs/implementation/config-local-journal.md) ·
  [config-file-store.md](../../specs/implementation/config-file-store.md) ·
  [config_activation](../../tla/execution_models/config_activation/ROADMAP.md)

```sh
cargo test --locked -p tickwire-config-materializer --all-targets
./scripts/check-linux.sh
./scripts/check-tla.sh config_activation
```
