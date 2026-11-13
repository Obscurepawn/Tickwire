# Physical Write-Ahead Log, Segment Rotation, and Tail Recovery

`tickwire-journal` owns the single-writer physical write-ahead log (`Journal<S: Store>` and `FileStore`) for WAL v1 framed data batches within a single `(risk_domain_id, journal_epoch)` stream. A returned `PhysicalSyncReceipt` states only what contiguous framed bytes reached a local filesystem synchronization barrier (`sync_data`). Higher-level journals and execution gateways remain responsible for canonical payload schemas, command identity, recovery dependency closures, and live trading authority.

Separating physical framing and synchronization from business reduction keeps crash recovery deterministic: the physical layer guarantees contiguous frame chaining, torn-tail truncation, and corruption quarantine without interpreting order or balance state, while upper layers bind their logical hashes and recovery frontiers to `PhysicalFrontier`.

## State Model and Bounds

Every WAL record is wrapped in an 8-byte-aligned v1 frame (`MAGIC = "TWAL0001"`, `VERSION = 1`) protected by dual integrity checks: Castagnoli `crc32c` checksums over the header and payload, plus a trailing `SHA-256` digest that chains `prev_frame_hash` from frame to frame across segment boundaries. Data appends are grouped into atomic `BatchBegin .. DataRecord+ .. BatchSeal` units that never straddle two segment files.

| Structure or Parameter | Layout / Enforced Bound |
| --- | --- |
| WAL v1 frame geometry | `136`-byte header + payload + zero-filled 8-byte alignment padding + `40`-byte trailer; total frame length `176..=1,048,576` bytes (`1 MiB`), `flags == 0` |
| Frame integrity & chaining | Header `crc32c` (computed with bytes `128..132` zeroed), payload `crc32c`, `prev_frame_hash` (`[0; 32]` at `lsn == 1`), trailer repeating `total_len`, `"TEND"`, and `SHA-256` over `frame[..tail] \|\| frame[tail + 32..]` |
| Recognized `FrameType` tags (`1..=12`) | `1` `BatchBegin`, `2` `DataRecord`, `3..=11` replication/snapshot/control/dependency/freeze frames, `12` `BatchSeal` |
| Atomic data batch | Empty `BatchBegin` + `1..=32` `DataRecord` frames (`BATCH_MAX_RECORDS = 32`) + empty `BatchSeal`; total batch size `<= 65,536` bytes (`64 KiB`), single-segment |
| Segments & `MANIFEST.json` | Contiguous `segment-{id:020}.wal` (`id >= 1`), `segment_max_bytes` in `528..=268,435,456` (`256 MiB` default, multiple of 8); `MANIFEST.json` (`<= 64 KiB`) binding `version = 1`, `config`, `generation`, `active_segment`, and SHA-256 `sha256` |
| `PhysicalFrontier` | Binds `risk_domain_id`, `journal_epoch`, `segment_id`, `end_offset`, `last_lsn`, `last_seq`, `exec_epoch`, `batch_id`, and `last_seal_hash` |

## Core Invariants

* **Pre-validated all-or-nothing batch staging**: Before writing a single byte to `Store`, `append_batch` validates that the writer is not poisoned, `exec_epoch` is non-zero and non-decreasing (`>= current_exec_epoch`), record count is in `1..=32`, individual record sizes fit frame limits, sequence and LSN counters do not wrap `u64`, and the assembled `BatchBegin + DataRecord* + BatchSeal` buffer is `<= 65,536` bytes.
* **Exclusive directory lock with explicit unlock-on-drop**: `FileStore::open` creates new directories with Unix mode `0700`, canonicalizes the directory path, opens `LOCK`, and acquires an exclusive non-blocking advisory lock wrapped in `WriterLock` before calling `sync_all()` on `LOCK`, the store directory, and its parent directory. `WriterLock::Drop` explicitly invokes `unlock()` prior to closing the descriptor so a short-lived child process that inherited a duplicate file descriptor across `fork` cannot silently extend the lock lifetime or interfere with a subsequent owner's lock. Callers must supply a trusted parent path and enforce permissions on pre-existing directories.
* **Crash-consistent segment rotation**: When a staged batch will not fit inside the remaining capacity of `active_segment` (and `active_offset > 0`), `roll_segment` creates `segment-{next:020}.wal` with `create_new(true)` (`0600` on Unix), synchronizes both the new segment file and the store directory, and then publishes `MANIFEST.json` by writing `MANIFEST.tmp`, calling `sync_all()`, atomically renaming `MANIFEST.tmp` over `MANIFEST.json`, and calling `sync_all()` on the directory again. If a crash interrupts rotation after `segment-{active + 1}.wal` is created but before `MANIFEST.json` is renamed, recovery accepts at most that single `active_segment + 1` file provided the physical hash chain remains continuous, while rejecting missing named segments, larger gaps, or empty interior segments.
* **Independent frontier verification before file mutation**: `recover_checked` scans the entire segment chain, verifies any caller-supplied `PhysicalFrontier`, and runs the upper layer's record validation callback *before* truncating an unsealed tail or advancing `MANIFEST.json`. Because a frontier derived solely from the surviving disk scan cannot detect deletion of an entire trailing segment or sealed batch, callers that know a previously acknowledged `PhysicalFrontier` pass it as `required` to catch silent suffix loss prior to modifying any file.

## Operations and Failure Semantics

* **Single-sync commit, lost receipts, and writer poisoning**: `append_batch` appends the contiguous batch buffer at the tracked segment offset and calls `sync_segment` (`File::sync_data`) once. Only after `sync_segment` succeeds does the journal advance its in-memory counters and return `PhysicalSyncReceipt`. Any I/O error during segment rotation, write, or sync immediately sets `poisoned = true` and blocks subsequent appends; because a failed write or interrupted `sync_data` may still leave a complete sealed batch on storage, the absence of `PhysicalSyncReceipt` never proves that the batch was discarded.
* **Torn-tail truncation versus corruption quarantine**: Every complete sealed batch in the surviving prefix is retained during recovery regardless of whether the prior writer received its receipt. If the final batch in the final segment terminates mid-frame or lacks its `BatchSeal`, recovery records the highest valid `exec_epoch` observed in that partial batch (so epoch monotonicity cannot regress), truncates only the unsealed tail bytes (`discarded_tail_bytes`), re-synchronizes every segment, and heals `MANIFEST.json` if `active_segment + 1` was already active. Conversely, any *complete* frame with invalid magic, non-zero flags, non-zero alignment padding, bad header or payload `crc32c`, mismatched trailer length or `"TEND"`, broken `prev_frame_hash`, or bad `SHA-256` returns `Error::Quarantined` immediately, preserving all on-disk bytes untouched for forensic inspection.

## Verification and References

```sh
cargo test --locked -p tickwire-journal
cargo clippy --locked -p tickwire-journal --all-targets -- -D warnings
```

Subprocess crash tests terminate a live writer at four cut points: halfway through writing a batch, after writing all frames but before `sync_data`, after `sync_data` completes but before `PhysicalSyncReceipt` reaches the caller, and after receipt delivery. In historical Linux runs, the mid-write cut cleanly truncated a 372-byte unsealed tail while the remaining three cuts recovered complete batches (noting that bytes surviving `SIGKILL` in the OS page cache do not by themselves prove survival across physical power loss).

* Journal engine, frame codec, and filesystem store: [journal.rs](../../crates/tickwire-journal/src/journal.rs), [frame.rs](../../crates/tickwire-journal/src/frame.rs), [store.rs](../../crates/tickwire-journal/src/store.rs)
* Machine wire contract: [journal-wire-v1.json](../contracts/journal-wire-v1.json)
* Crash and manifest test suites: [process.rs](../../crates/tickwire-journal/tests/process.rs), [manifest.rs](../../crates/tickwire-journal/tests/manifest.rs)
* Formal verification: [Physical journal L0 batch-sync-recovery model](../../tla/execution_models/physical_journal/L0_batch_sync_recovery/05_output.md)
* Related specifications: [Scalar business journal codec](journal-codec.md), [Scalar execution journal](business-journal.md), [Ordered account journal](account-journal.md)
