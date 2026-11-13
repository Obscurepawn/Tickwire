# Local Process Configuration Materializer and Last-Good Retention

`tickwire_config_materializer::Local<F: Files, S: JournalStore>` coordinates permission-scoped `ProjectionObject` storage, deterministic `ProjectionFile` TOML rendering, descriptor-relative file installation (`tickwire-config-fs::Store`), and the four-phase `tickwire-config-journal` for a single target process slot.

`Local` owns file-before-journal ordering, immutable `last-good` snapshot retention, and full cross-verification of historical objects, snapshots, and active TOML inodes on open and before every mutation. Upstream control sessions authenticate rollout commits, and the live target process supplies its own `RuntimeFact` observation after applying a configuration revision.

## State Model and Bounds

Within the process's private configuration directory, `Local::open` verifies existing state and ensures `objects/` and `last-good/` subdirectories exist alongside `desired.toml`, `effective.toml`, and the local WAL. Total retained artifact bytes are bounded by a `512 MiB` ceiling that pre-reserves worst-case headroom for active mutable files.

| Path or Budget | Enforced Content / Bound |
| --- | --- |
| `objects/<object_hash_hex>.json` | Permission-filtered `ProjectionObject` (`<= 4 MiB`), verified against `Scope` (`cluster`, `namespace`, `host`, `process`) and `Prepared.next` |
| `desired.toml` | Rendered `ProjectionFile` (`<= 8 MiB`, `Kind::Desired`) with `applied_event_seq = 0` and `journal_receipt_ref` pointing to `Event::Prepared` |
| `effective.toml` | Rendered `ProjectionFile` (`<= 8 MiB`, `Kind::Effective`) binding `RuntimeFact.boot`, `applied_event_seq > 0`, and `Event::RuntimeObserved` receipt |
| `last-good/<rev>-<global_hash_hex>.toml` | Immutable copy of every `Effective` `ProjectionFile` (`<= 8 MiB`), retained across subsequent rollouts |
| `RETAINED_BYTES` (`512 MiB`) | Sum of all journaled `projection_object_bytes` + all completed `Effective` `last-good` file bytes + `24 MiB` (`3 * 8 MiB` reserved for `desired.toml`, `effective.toml`, and an in-flight `last-good` copy) |

## Core Invariants

* **Write-file-before-journal staging across all four phases**: Every mutating method first runs `verify_all()` to confirm zero drift on disk, synchronizes any required file artifact through `Files::install` (which syncs temporary file + atomic rename + parent directory), and records the corresponding `tickwire-config-journal` event *only* after receiving the synced `FileFact`:
  1. `prepare(prepared, object)`: Validates `Event::Prepared`, checks `ProjectionObject` scope/binding and the `512 MiB` retained capacity budget, installs `objects/<object_hash>.json`, and records `Event::Prepared`.
  2. `write_desired(operation)`: Re-loads and validates `objects/<object_hash>.json`, renders `Kind::Desired` TOML, installs `desired.toml` conditioned on the previous `Desired` `render_hash`, checks `FileFact` hash/length equality, and records `Event::Desired`.
  3. `observe_runtime(operation, fact)`: Validates and records `Event::RuntimeObserved` linking `desired_ref` to the authenticated `RuntimeFact`.
  4. `write_effective(operation)`: Renders `Kind::Effective` TOML, installs immutable `last-good/<revision>-<global_hash>.toml` (`expected_previous = None`), installs `effective.toml` conditioned on the previous `Effective` `render_hash`, and records `Event::Effective`.
* **Exhaustive open-time and pre-step audit (`verify_all`)**: Before `Local::open` returns—and before every `prepare`, `write_desired`, `observe_runtime`, or `write_effective` transition—`verify_all` re-reads and decodes *every* historical `objects/<hash>.json`, re-renders and re-syncs *every* historical `last-good/<rev>-<hash>.toml` (`install(..., Some(fact.render_hash))`, which flushes file and directory metadata without ever recreating a missing snapshot), and verifies `desired.toml` and `effective.toml` against the latest journaled `FileFact`—demanding identical bytes, byte length, `render_hash`, filesystem `device`, and `inode`.
* **Non-regressing historical retries**: Re-invoking `prepare`, `write_desired`, or `write_effective` for an operation phase already recorded in the journal returns the original cached `Receipt` immediately after `verify_all()`, never overwriting a newer `desired.toml` or `effective.toml` with an older revision's bytes.

## Operations and Failure Semantics

* **Single-step crash healing versus drift quarantine**: Because `Local` installs files *before* appending the matching journal step, a crash between `Files::install` and `journal.record` leaves an updated `desired.toml` or `effective.toml` (and `last-good` copy) on disk without its `Event::Desired` or `Event::Effective` record. `verify_current` accepts that single unjournaled forward file *if and only if* an `active_operation` exists at the immediately preceding phase (`Prepared` for `desired.toml`; `RuntimeObserved` for `effective.toml`) and the on-disk bytes match a fresh deterministic render of that active step byte for byte. Retrying the interrupted method then re-synchronizes the file and records the missing journal event.
* **Fail-closed poisoning on drift or I/O uncertainty**: Any missing `objects/*.json` or `last-good/*.toml` file, modified file bytes, replaced inode on `desired.toml` or `effective.toml`, unexpected unjournaled file, or mid-step I/O error returns `Error::Drift` (or storage/journal error) and sets `poisoned = true`, refusing further operations until both the file store and local journal are reopened and verified. Out-of-band cleanup of unreferenced temporary or abandoned files and filesystem quota enforcement remain managed by host deployment tooling.

## Verification and References

```sh
cargo test --locked -p tickwire-config-materializer
cargo clippy --locked -p tickwire-config-materializer --all-targets -- -D warnings
```

Platform-agnostic sequence tests and Linux descriptor-backed integration tests cover multi-revision `Prepared -> Desired -> RuntimeObserved -> Effective` rollouts, `512 MiB` retained budget enforcement, crash recovery between file install and journal append, non-regressing historical step retries, and `Error::Drift` quarantine on tampered bytes, replaced inodes, or deleted `last-good` snapshots.

* Materializer trait interface and `Local` state machine: [lib.rs](../../crates/tickwire-config-materializer/src/lib.rs), [local.rs](../../crates/tickwire-config-materializer/src/local.rs)
* Sequence and Linux integration tests: [sequence.rs](../../crates/tickwire-config-materializer/tests/sequence.rs), [linux.rs](../../crates/tickwire-config-materializer/tests/linux.rs)
* Formal verification: [Config activation TLA+ model](../../tla/execution_models/config_activation/PLAN.md)
* Related specifications: [Configuration candidates](config-candidate.md), [Linux configuration file store](config-file-store.md), [Local configuration journal](config-local-journal.md), [Runtime observation](runtime-observation.md)
