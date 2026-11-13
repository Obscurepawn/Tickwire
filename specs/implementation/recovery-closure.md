# Recovery Dependency Manifests, Input Blocks, and Verified Closures

`tickwire-recovery` ensures that before an execution runtime commits an order admission—and whenever a crashed node replays that admission—every causal dependency required to explain and reproduce the decision is durably stored and hash-verified on local media. Callers submit a `RecoveryManifest` and raw dependency blobs to `Persistence::publish_with_objects` (or `Persistence::publish` when blobs were pre-staged); only after every dependency object, the manifest, and its immutable `ClosureBinding` have been validated and synchronized does `tickwire-recovery` issue a `VerifiedClosure` capability.

`VerifiedClosure` proves that a complete, cycle-free dependency closure was durable at publication or restart verification (`Persistence::verify`). Evaluating whether the active strategy lease and risk grant currently permit wire transmission remains the responsibility of the execution gateway.

## State Model and Bounds

A `ClosureBinding` uniquely maps one `(risk_domain_id, recovery_object_id)` pair to a canonical `RecoveryManifest` root (`manifest_hash`) along with `(intent_id, intent_exec_epoch, business_digest, strategy_generation, config_revision)`. Manifests use definite-length, shortest-form CBOR arrays hashed with `SHA-256`; input history blocks (`kind 12`) use a compact `"TWINP001"` binary framing with rolling `SHA-256` row hashes.

| Resource or Collection | Enforced Structure / Bound |
| --- | --- |
| Object & closure capacities | `<= 64 MiB` per object (`MAX_OBJECT_BYTES`); `<= 256 MiB` total object bytes per closure (`MAX_CLOSURE_BYTES`); `<= 1 MiB` manifest (`MAX_MANIFEST_BYTES`); `<= 4,096` objects; `<= 16,384` DAG edges |
| Required dependency kinds (`1..=12`) | Bitmask `0xfff` must all be present; singleton `kind 1` (`Intent`) == `business_digest`; `kind 4` (`ActionGrant`) == `action_grant_ref`; `kind 7` (`ReleaseOperation`) == `operation_ref` |
| Command & account kinds (`13..=15`) | Optional singleton `kind 13` (`Command`); paired `kind 14` (account baseline) and `kind 15` (account `HistoryFrontier`), which must appear together and require `kind 13` |
| `TWINP001` input blocks (`kind 12`) | `68`-byte header (`"TWINP001"` + non-zero 16-byte `stream` + `first_sequence` in `1..2^63` + `count <= 1,000,000` + `previous_hash`) + rows (`seq`, `kind` in `1..=5`, `u32` len, payload) |

## Core Invariants

* **Immutable single-assignment `ClosureBinding` and full-graph verification**: Within a `risk_domain_id`, a `recovery_object_id` can bind at most one `RecoveryManifest` and order identity: an exact retry succeeds idempotently, whereas differing manifest bytes or binding fields are rejected. Object descriptors must be strictly sorted and unique by `(kind, content_hash)`, DAG edges `(from, to)` must be strictly sorted, unique, in-bounds, and acyclic (verified via Kahn's topological sort), and *every* declared descriptor—including isolated objects with zero incoming edges—must match the exact byte length and `SHA-256` of its stored blob.
* **Gap-free sequenced strategy input history (`kind 12`)**: Input blocks record the exact sequence of strategy inputs (`kind 1..=5`: market events, execution reports, timers, configuration updates, and model updates) chained via `SHA-256("tickwire.input-row.v1" || previous_hash || row_bytes)`. Across all `kind 12` objects sorted by `first_sequence`, `tickwire-recovery` verifies a single non-zero `stream`, contiguous sequence numbers without gaps or overlaps, unbroken `previous_hash -> last_hash` links, and exact equality with `manifest.input_boundary`. An empty input history is represented by a single zero-row block (`last_sequence = first_sequence - 1`, `last_hash = previous_hash`). Both `DeterministicReplay` and `DeclaredRebuild` modes reject any sequence gap.
* **Pluggable storage backends (`InMemoryBlobStore`, `AsyncFileBlobStore`, `FileBlobStore`)**: `RuntimeBlobStore` matches the deployment's `StorageMode`:
  * **`InMemoryBlobStore` (`StorageMode::InMemory`, default)**: Deduplicates immutable content-addressed blobs and `ClosureBinding` roots in process RAM with zero filesystem calls on ephemeral cloud Pods.
  * **`AsyncFileBlobStore` (`StorageMode::MmapAsync`)**: Maintains an in-memory content-hash index over the `0700` locked store directory so already-published dependency blobs (such as catalog, config, executable, and baseline pins reused across orders) resolve in ~10 nanoseconds with zero syscalls, while newly created blobs write and atomically rename into the Linux VFS page cache and enqueue asynchronous `fdatasync` + directory sync on a dedicated background flusher thread—surviving `SIGKILL` without blocking order admission on 30+ synchronous `fsync` calls.
  * **`FileBlobStore` (`StorageMode::LocalDurable`)**: Canonicalizes its private `0700` root directory, acquires an exclusive non-blocking advisory lock on `LOCK` wrapped immediately in `WriterLock` (whose `Drop` explicitly calls `unlock()` before closing the FD so inherited child descriptors cannot hold or release locks), and synchronizes both `root` and its parent directory on open. Each new content-addressed blob or binding is written to `TEMP-<pid>-<counter>` (`0600`, `create_new`), flushed via `File::sync_all()`, atomically renamed to its target key, and sealed by calling `sync_all()` on `root`.
* **Re-synchronization on same-key retry and read hardening**: If a crash occurs after `rename` makes a blob path visible in the directory cache but before the directory `sync_all()` reaches stable storage, merely reading the visible file on retry would not guarantee crash durability. Consequently, a same-key `put` retry verifies byte-for-byte equality against the existing file and still executes `sync_all()` on both the file and `root` before returning. Read-only inspection via `FileBlobReader` opens files with `O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK` and verifies both the expected owner UID and `mode & 0o027 == 0`.

## Operations and Failure Semantics

* **Non-regenerating restart verification and orphan retention**: `Persistence::verify` reopens the stored `ClosureBinding` and `RecoveryManifest`, re-validates every referenced dependency's bytes and hashes, and confirms synchronization without ever synthesizing missing blobs; any missing, truncated, or corrupt file fails recovery closed. If a crash or validation error aborts `publish_with_objects` partway through staging, already-written content-addressed blobs remain harmlessly on disk (consuming capacity until offline garbage collection) without a `ClosureBinding` root.

## Verification and References

```sh
cargo test --locked -p tickwire-recovery
cargo clippy --locked -p tickwire-recovery --all-targets -- -D warnings
```

Test suites cover CBOR manifest canonicality, DAG cycle rejection, orphan-vertex hash checks, `TWINP001` multi-block continuity and gap rejection, same-key retry re-sync, inherited-FD lock release, `FileBlobReader` symlink/permission rejection, and crash-cut durability states.

* Persistence engine, manifest codec, input blocks, and file store: [lib.rs](../../crates/tickwire-recovery/src/lib.rs), [codec.rs](../../crates/tickwire-recovery/src/codec.rs), [inputs.rs](../../crates/tickwire-recovery/src/inputs.rs), [files.rs](../../crates/tickwire-recovery/src/files.rs)
* Closure and filesystem tests: [closure.rs](../../crates/tickwire-recovery/tests/closure.rs)
* Formal verification: [Recovery closure TLA+ model](../../tla/execution_models/recovery_closure/PLAN.md)
* Related specifications: [Scalar execution journal](business-journal.md), [Ordered account journal](account-journal.md), [Account gateway](account-gateway.md)
