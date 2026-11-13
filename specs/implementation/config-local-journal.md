# Four-Phase Local Configuration Journal and Operation Ledger

`tickwire-config-journal` records the durable local lifecycle of configuration rollouts for a single `(cluster, namespace, host, process, slot, locator_generation)` scope on top of `tickwire-journal`. Record `1` writes an immutable `Scope` header batch, and every subsequent transition writes its own 1-record synchronized WAL batch advancing an operation strictly through `Prepared -> Desired -> RuntimeObserved -> Effective`.

A `Receipt` from `tickwire-config-journal` proves only that the supplied transition record is durable within a verified local WAL prefix. Control-plane commit authenticity, actual in-process configuration application, and current on-disk TOML file integrity remain validated by the control session, runtime owner, and `tickwire-config-materializer`.

## State Model and Bounds

The journal derives a domain-separated `scope_hash = SHA-256("tickwire.config.local.scope.v1\0" || JSON(scope))`, sets the physical WAL `domain` to `scope_hash[..16]` with `segment_max_bytes = 16 MiB`, and wraps every record in a canonical JSON `Envelope { schema: 1, scope_hash, body }` whose decoded value must re-encode byte-for-byte identically. During recovery, a `Bounded<S>` store wrapper caps physical segment reads before `tickwire-journal` allocates decode buffers.

| Parameter or Artifact | Enforced Structure / Bound |
| --- | --- |
| `Scope` identifiers | Lowercase ASCII `[a-z][a-z0-9-]{0,62}` for `cluster`, `host`, and `process`; non-zero 16-byte UUIDs for `namespace`, `slot`, and `stream_epoch`; non-zero `locator_generation` and `writer_epoch` |
| Record & ledger limits | `<= 16 KiB` (`MAX_RECORD`) per JSON envelope; `1..=4,096` `max_operations`; `512 B..=64 MiB` `max_recorded_bytes` (including the `seq == 1` scope header) |
| `Bounded<S>` recovery read cap | At most `4 * max_operations + 2` segments and `max_recorded_bytes + (4 * max_operations + 1) * 535 + 65,536` bytes read across recovery |
| Embedded artifact bounds | `Prepared.projection_object_bytes` in `1..=4 MiB`; `Desired` and `Effective` `FileFact.bytes` in `1..=8 MiB` with `inode > 0` and non-zero `render_hash` |
| Batch-per-record invariant | Every record at `seq >= 1` occupies its own sealed WAL batch (`frontier.last_seq == record.seq == frontier.batch_number`) |

## Core Invariants

* **Single active operation and hash-linked four-phase progression**: At most one operation UUID may be unfinished (`active_operation()`) at any time. A new `Event::Prepared` is accepted only when `active == None`, the operation UUID is fresh, `base` exactly equals the ledger's latest `Effective` `Version` (`None` before the first completed rollout), and `next.revision` is strictly greater than `base.revision` (with all revisions in `1..=2^63 - 1`). Subsequent phases for that active operation must arrive strictly in order and embed the exact `SHA-256` `record_hash` of the preceding step:
  * `Desired` binds `prepared_ref == receipt(Prepared).record_hash` and a valid `desired.toml` `FileFact`.
  * `RuntimeObserved` binds `desired_ref == receipt(Desired).record_hash` and a `RuntimeFact` matching `expected_instance`, `exec_generation`, non-zero `boot` and `host_boot` (matching `expected_boot` / `expected_host_boot` whenever non-zero), positive `applied_event_seq`, and non-zero `witness_ref`.
  * `Effective` binds `runtime_ref == receipt(RuntimeObserved).record_hash` and a valid `effective.toml` `FileFact`, promoting `next` to the ledger's `effective` baseline and clearing `active`.
* **Live exact-byte idempotency versus duplicate-free physical WAL**: On a live `Journal`, re-submitting byte-identical JSON for an already-completed `(operation, phase)` returns its cached `Receipt` without writing to disk, whereas differing bytes under the same `(operation, phase)` fail with `Error::Conflict`. During `Journal::recover`, any duplicate `(operation, phase)` step present in the physical WAL fails validation immediately.
* **Business validation and writer-epoch fencing before tail truncation**: `Journal::recover` passes its `replay` validator into `tickwire_journal::Journal::recover_checked`, verifying `Scope` header equality, 1-record-per-batch sealing, operation phase links, and `recovery.observed_exec_epoch() <= config.writer_epoch` *before* `tickwire-journal` truncates an incomplete tail batch or syncs segments. Because `observed_exec_epoch()` inspects even a valid unsealed final batch header, a stale writer cannot truncate a newer writer's partial tail (though that unsealed header confers no `Receipt`).

## Operations and Failure Semantics

* **Fail-closed append poisoning and receipt recovery on reopen**: `Journal::record` validates the event and checks capacity before calling `physical.append_batch`; in-memory `Ledger` state mutates only after physical synchronization returns `PhysicalSyncReceipt`. If `append_batch` fails, `poisoned` is set to `true` and memory retains only previously confirmed steps. Reopening via `Journal::recover` reconstructs the full valid sealed prefix—so if a batch reached disk before its sync acknowledgement was lost, retrying that step after reopen cleanly returns its original `Receipt`.

## Verification and References

```sh
cargo test --locked -p tickwire-config-journal
cargo clippy --locked -p tickwire-config-journal --all-targets -- -D warnings
```

Test suites exercise subprocess termination at each of the four lifecycle steps, lost sync acknowledgements, invalid business records embedded inside valid physical frames, stale-writer fencing against a newer writer's partial tail, and end-to-end integration with `tickwire-config-fs` file receipts.

* Journal engine, ledger state machine, envelope codec, and bounded reader: [lib.rs](../../crates/tickwire-config-journal/src/lib.rs), [journal.rs](../../crates/tickwire-config-journal/src/journal.rs), [ledger.rs](../../crates/tickwire-config-journal/src/ledger.rs), [codec.rs](../../crates/tickwire-config-journal/src/codec.rs), [store.rs](../../crates/tickwire-config-journal/src/store.rs)
* Lifecycle and recovery tests: [journal.rs](../../crates/tickwire-config-journal/tests/journal.rs)
* Related specifications: [Configuration journal Linux backend](config-journal-store.md), [Physical write-ahead log](journal.md), [Local configuration materializer](config-materializer.md)
