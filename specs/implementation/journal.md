# Physical Write-Ahead Log, Storage Modes, and Tail Recovery

`tickwire-journal` owns the single-writer physical write-ahead log (`Journal<S: Store>`) for WAL v1 framed data batches within a single `(risk_domain_id, journal_epoch)` stream across three selectable `StorageMode` profiles (`JournalStore`):

1. **`StorageMode::InMemory` (`InMemoryStore`, default)**: Pure volatile RAM storage with zero filesystem directories, descriptors, or syscalls on the trading path—tailored for ephemeral public-cloud Pods where container rescheduling discards node-local storage and cold startup bootstraps directly from authenticated exchange snapshots and self-describing `ClientId` open orders.
2. **`StorageMode::MmapAsync` (`MmapAsyncStore`)**: Preallocated, pre-faulted `MAP_SHARED` segment mappings (`tickwire-shm-sys::MappedSegment`) paired with a 4-KiB `MMAP-CURSOR.bin` logical-tail cursor page (`MappedCursorPage`) and a dedicated background flusher thread running asynchronous `fdatasync`. Hot-path `append_batch` copies framed bytes into the kernel page cache, advances the alternate cursor slot after a `Release` fence, publishes the flush target atomically, and wakes the parked flusher thread without blocking—compressing hot-path commit latency from hundreds of microseconds to ~100 nanoseconds while preserving 100% deterministic same-host process-crash (`SIGKILL`/panic) recovery.
3. **`StorageMode::LocalDurable` (`FileStore`)**: Synchronous `write_all` + `File::sync_data` (`fdatasync`) before returning `PhysicalSyncReceipt`, retained for deterministic `fsync` fault-injection tests, bare-metal power-loss durability budgets, and offline archival tools.

A returned `PhysicalSyncReceipt` states that contiguous framed bytes reached the configured `Store`'s commit boundary (RAM buffer in `InMemory`, kernel `MAP_SHARED` page cache + cursor slot queued for async `fdatasync` in `MmapAsync`, or `sync_data` barrier in `LocalDurable`). Higher-level journals and execution gateways remain responsible for canonical payload schemas, command identity, recovery dependency closures, and live trading authority.

Separating physical framing and synchronization from business reduction keeps crash recovery deterministic: the physical layer guarantees contiguous frame chaining, torn-tail truncation, and corruption quarantine without interpreting order or balance state, while upper layers bind their logical hashes and recovery frontiers to `PhysicalFrontier`.

## State Model and Bounds

Every WAL record is wrapped in an 8-byte-aligned v1 frame (`MAGIC = "TWAL0001"`, `VERSION = 1`) protected by a mandatory trailing `SHA-256` digest that chains `prev_frame_hash` from frame to frame across segment boundaries, plus compile-time optional Castagnoli `crc32c` header and payload checksums (`--features crc32`, off by default). In the default build (`not(feature = "crc32")`), `frame::encode` writes deterministic `0` into `header_crc32c` (`128..132`) and `payload_crc32c` (`132..136`) without computing CRC-32C on the hot path, and `frame::decode_header` / `frame::decode` skip CRC-32C recomputation while still enforcing magic, version, length, alignment, zero padding, trailer magic/length, and full-frame `SHA-256` chaining. Enabling `crc32` (`tickwire-journal/crc32`, forwarded by `tickwire-account-journal/crc32`, `tickwire-business-journal/crc32`, and `tickwire-config-journal/crc32`) activates hardware-accelerated Castagnoli `crc32c` (`x86_64` SSE4.2 `_mm_crc32_u64`/`_mm_crc32_u8`, `aarch64` `__crc32cd`/`__crc32cb`, and a compile-time `const TABLES` Slicing-by-8 fallback `crc32c_slicing8` in `src/crc32c.rs` with zero bit-by-bit loops and zero-allocation masked header evaluation). Pure `crc32c`, `crc32c_masked` (clamping arbitrary `Range<usize>` bounds to `0..=input.len()` without panicking, matching `tickwire_ipc::crc32c_masked`), and `crc32c_slicing8` functions remain unconditionally callable in all builds. Data appends are grouped into atomic `BatchBegin .. DataRecord+ .. BatchSeal` units that never straddle two segment files.

| Structure or Parameter | Layout / Enforced Bound |
| --- | --- |
| WAL v1 frame geometry | `136`-byte header + payload + zero-filled 8-byte alignment padding + `40`-byte trailer; total frame length `176..=1,048,576` bytes (`1 MiB`), `flags == 0` |
| Frame integrity & chaining | Trailing `SHA-256` over `frame[..tail] \|\| frame[tail + 32..]`, `prev_frame_hash` (`[0; 32]` at `lsn == 1`), trailer repeating `total_len` and `"TEND"`; under `feature = "crc32"`, header `crc32c` (bytes `128..132` masked to zero) and payload `crc32c` (deterministic `0` when `not(feature = "crc32")`) |
| Recognized `FrameType` tags (`1..=12`) | `1` `BatchBegin`, `2` `DataRecord`, `3..=11` replication/snapshot/control/dependency/freeze frames, `12` `BatchSeal` |
| Atomic data batch | Empty `BatchBegin` + `1..=32` `DataRecord` frames (`BATCH_MAX_RECORDS = 32`) + empty `BatchSeal`; total batch size `<= 65,536` bytes (`64 KiB`), single-segment |
| Segments & `MANIFEST.json` | Contiguous `segment-{id:020}.wal` (`id >= 1`), `segment_max_bytes` in `528..=268,435,456` (`256 MiB` default, multiple of 8); `MANIFEST.json` (`<= 64 KiB`) binding `version = 1`, `config`, `generation`, `active_segment`, and SHA-256 `sha256` |
| `PhysicalFrontier` | Binds `risk_domain_id`, `journal_epoch`, `segment_id`, `end_offset`, `last_lsn`, `last_seq`, `exec_epoch`, `batch_id`, and `last_seal_hash` |
| All-profile integer overflow checks | Root `Cargo.toml` sets `overflow-checks = true` on `[profile.dev]`, `[profile.test]`, `[profile.release]`, and `[profile.bench]`, verified in debug and `--release` by `tests/overflow_checks.rs` |

## Core Invariants

* **Pre-validated all-or-nothing batch staging**: Before writing a single byte to `Store`, `append_batch` validates that the writer is not poisoned, `exec_epoch` is non-zero and non-decreasing (`>= current_exec_epoch`), record count is in `1..=32`, individual record sizes fit frame limits, sequence and LSN counters do not wrap `u64`, and the assembled `BatchBegin + DataRecord* + BatchSeal` buffer is `<= 65,536` bytes.
* **Exclusive directory lock with explicit unlock-on-drop**: `FileStore::open` creates new directories with Unix mode `0700`, canonicalizes the directory path, opens `LOCK`, and acquires an exclusive non-blocking advisory lock wrapped in `WriterLock` before calling `sync_all()` on `LOCK`, the store directory, and its parent directory. `WriterLock::Drop` explicitly invokes `unlock()` prior to closing the descriptor so a short-lived child process that inherited a duplicate file descriptor across `fork` cannot silently extend the lock lifetime or interfere with a subsequent owner's lock. Callers must supply a trusted parent path and enforce permissions on pre-existing directories.
* **Crash-consistent segment rotation**: When a staged batch will not fit inside the remaining capacity of `active_segment` (and `active_offset > 0`), `roll_segment` creates `segment-{next:020}.wal` with `create_new(true)` (`0600` on Unix), synchronizes both the new segment file and the store directory, and then publishes `MANIFEST.json` by writing `MANIFEST.tmp`, calling `sync_all()`, atomically renaming `MANIFEST.tmp` over `MANIFEST.json`, and calling `sync_all()` on the directory again. If a crash interrupts rotation after `segment-{active + 1}.wal` is created but before `MANIFEST.json` is renamed, recovery accepts at most that single `active_segment + 1` file provided the physical hash chain remains continuous, while rejecting missing named segments, larger gaps, or empty interior segments.
* **Independent frontier verification before file mutation**: `recover_checked` scans the entire segment chain, verifies any caller-supplied `PhysicalFrontier`, and runs the upper layer's record validation callback *before* truncating an unsealed tail or advancing `MANIFEST.json`. Because a frontier derived solely from the surviving disk scan cannot detect deletion of an entire trailing segment or sealed batch, callers that know a previously acknowledged `PhysicalFrontier` pass it as `required` to catch silent suffix loss prior to modifying any file.

## Operations and Failure Semantics

* **Single-sync commit, lost receipts, and writer poisoning**: `append_batch` appends the contiguous batch buffer at the tracked segment offset and calls `sync_segment` (`File::sync_data`) once. Only after `sync_segment` succeeds does the journal advance its in-memory counters and return `PhysicalSyncReceipt`. Any I/O error during segment rotation, write, or sync immediately sets `poisoned = true` and blocks subsequent appends; because a failed write or interrupted `sync_data` may still leave a complete sealed batch on storage, the absence of `PhysicalSyncReceipt` never proves that the batch was discarded.
* **Torn-tail truncation versus corruption quarantine**: Every complete sealed batch in the surviving prefix is retained during recovery regardless of whether the prior writer received its receipt. If the final batch in the final segment terminates mid-frame or lacks its `BatchSeal`, recovery records the highest valid `exec_epoch` observed in that partial batch (so epoch monotonicity cannot regress), truncates only the unsealed tail bytes (`discarded_tail_bytes`), re-synchronizes every segment, and heals `MANIFEST.json` if `active_segment + 1` was already active. Conversely, any *complete* frame with invalid magic, non-zero flags, non-zero alignment padding, bad header or payload `crc32c` (when `feature = "crc32"` is enabled), mismatched trailer length or `"TEND"`, broken `prev_frame_hash`, or bad `SHA-256` returns `Error::Quarantined` immediately, preserving all on-disk bytes untouched for forensic inspection.

## Verification and References

```sh
cargo test --locked -p tickwire-journal --all-targets
cargo test --locked -p tickwire-journal --all-targets --features crc32
cargo test --locked --release -p tickwire-journal --test overflow_checks
cargo clippy --locked -p tickwire-journal --all-targets -- -D warnings
cargo clippy --locked -p tickwire-journal --all-targets --features crc32 -- -D warnings
cargo bench --locked -p tickwire-journal --bench frame_crc32c
```

Subprocess crash tests terminate a live writer at four cut points: halfway through writing a batch, after writing all frames but before `sync_data`, after `sync_data` completes but before `PhysicalSyncReceipt` reaches the caller, and after receipt delivery. In historical Linux runs, the mid-write cut cleanly truncated a 372-byte unsealed tail while the remaining three cuts recovered complete batches (noting that bytes surviving `SIGKILL` in the OS page cache do not by themselves prove survival across physical power loss). An additional `MmapAsyncStore` subprocess crash test (`storage_modes.rs`) writes 25 batches and terminates abruptly via `abort()` without calling `flush_durable()` or `Drop`, verifying that reopen truncates the preallocated tail via `MMAP-CURSOR.bin`, recovers 100% of committed batches (`discarded_tail_bytes == 0`), and resumes appending under a newer writer epoch.

### WAL Frame & CRC-32C Benchmark (`benches/frame_crc32c.rs`)

Measured on an `AMD Ryzen 7 8845H w/ Radeon 780M Graphics` (`Linux 6.18.58.1-microsoft-standard-WSL2 x86_64`, load average `2.79`, `bench` profile with `overflow-checks = true`, `lto = "thin"`, `codegen-units = 1`, 4,000 samples after 200 warm-up iterations, `x86_64-sse4.2` hardware CRC32C):

| Operation / Mode | Payload / Input | Hot-Path Work & Allocations | p50 | p99 | p99.9 |
| --- | ---: | --- | ---: | ---: | ---: |
| `crc32c` bitwise baseline (bench ref) | `256 B` | 8-iteration inner shift/XOR loop per byte; `0` allocs | `864.50 ns` | `1,385.50 ns` | `2,575.91 ns` |
| `crc32c_slicing8` (`const TABLES`) | `256 B` | 8-byte `u32::from_le_bytes` + `8 x 256` table lookup; `0` allocs | `117.09 ns` | `147.78 ns` | `646.09 ns` |
| `crc32c` hardware (`_mm_crc32_u64`) | `256 B` | 32 `crc32q` instructions via `u64::from_le_bytes`; `0` allocs | `11.89 ns` | `16.28 ns` | `23.80 ns` |
| `crc32c_header_masked` (`136 B`) | `136 B` | 17 `crc32q` instructions (`128..132` zeroed in-register); `0` allocs | `7.05 ns` | `10.48 ns` | `14.09 ns` |
| `frame::encode` (legacy bitwise CRC) | `256 B` | `1` `Vec` alloc (`432 B`) + bitwise header/payload CRC + `SHA-256` | `1,561.75 ns` | `2,974.50 ns` | `6,458.75 ns` |
| `frame::encode` (`--features crc32`) | `256 B` | `1` `Vec` alloc (`432 B`) + SSE4.2 header/payload CRC + `SHA-256` | `276.75 ns` | `351.88 ns` | `1,831.00 ns` |
| `frame::encode` (default `crc32` off) | `256 B` | `1` `Vec` alloc (`432 B`) + deterministic `0` CRCs + `SHA-256` | `260.50 ns` | `339.50 ns` | `2,368.38 ns` |
| `frame::decode` (legacy bitwise CRC) | `256 B` | `0` heap allocs (`136 B` stack copy) + bitwise CRCs + `SHA-256` | `1,540.50 ns` | `2,936.88 ns` | `5,749.88 ns` |
| `frame::decode` (`--features crc32`) | `256 B` | `0` allocs + zero-copy SSE4.2 masked header/payload CRC + `SHA-256` | `248.00 ns` | `286.75 ns` | `1,547.88 ns` |
| `frame::decode` (default `crc32` off) | `256 B` | `0` allocs + structural bounds + `SHA-256` (CRC skipped) | `233.00 ns` | `290.50 ns` | `4,184.38 ns` |
| `Journal<InMemoryStore>::append_batch` | `256 B` | `3` frames (`BatchBegin`, `DataRecord`, `BatchSeal`), default `crc32` off | `954.50 ns` | `2,439.75 ns` | `26,043.00 ns` |

### Commit Profile Benchmark (`storage_modes.rs`)

Measured on an `AMD Ryzen 7 8845H` (`Linux 6.18.58.1-microsoft-standard-WSL2 x86_64`, load average `0.15`, `--release`, 256-byte record payload, 200 warm iterations for `InMemory` and `MmapAsync` after 16 warm-up batches, 30 iterations for `LocalDurable`):

| Commit Profile / Boundary | Hot-Path Syscalls & Copies | p50 | p99 |
| --- | --- | ---: | ---: |
| Raw `MmapAsyncStore` (`append_segment` + `sync_segment`) | `0` syscalls when flusher active (`1` `futex` wake if parked); `1` `MAP_SHARED` copy + `1` 64-byte cursor slot SHA-256 | `110 ns` | `9,408 ns` |
| Framed `Journal<InMemoryStore>` (`append_batch`, default `crc32` off) | `0` syscalls; `3` WAL v1 frames (`BatchBegin`, `DataRecord`, `BatchSeal`) with `SHA-256` | `955 ns` | `2,440 ns` |
| Framed `Journal<InMemoryStore>` (`append_batch`, `--features crc32`) | `0` syscalls; `3` WAL v1 frames with SSE4.2 `crc32c` + `SHA-256` | `959 ns` | `1,884 ns` |
| Framed `Journal<MmapAsyncStore>` (`append_batch`, prior bitwise CRC baseline) | `0` blocking syscalls; `3` WAL v1 frames + `MAP_SHARED` page-cache copy + async `fdatasync` | `2,826 ns` | `8,006 ns` |
| Framed `Journal<FileStore>` (`append_batch`) | `open` + `fstat` + `lseek` + `write` + blocking `fdatasync` per batch | `1,043,291 ns` | `1,840,309 ns` |

* Journal engine, CRC-32C module, frame codec, and filesystem store: [journal.rs](../../crates/tickwire-journal/src/journal.rs), [crc32c.rs](../../crates/tickwire-journal/src/crc32c.rs), [frame.rs](../../crates/tickwire-journal/src/frame.rs), [store.rs](../../crates/tickwire-journal/src/store.rs)
* Machine wire contract: [journal-wire-v1.json](../contracts/journal-wire-v1.json)
* Crash, overflow, frame, and manifest test suites: [process.rs](../../crates/tickwire-journal/tests/process.rs), [overflow_checks.rs](../../crates/tickwire-journal/tests/overflow_checks.rs), [frame.rs](../../crates/tickwire-journal/tests/frame.rs), [manifest.rs](../../crates/tickwire-journal/tests/manifest.rs), [frame_crc32c.rs](../../crates/tickwire-journal/benches/frame_crc32c.rs)
* Formal verification: [Physical journal L0 batch-sync-recovery model](../../tla/execution_models/physical_journal/L0_batch_sync_recovery/05_output.md)
* Related specifications: [Scalar business journal codec](journal-codec.md), [Scalar execution journal](business-journal.md), [Ordered account journal](account-journal.md)
