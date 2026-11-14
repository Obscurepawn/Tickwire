# tickwire-journal

`tickwire-journal` (`#![deny(unsafe_code)]`, with `#[forbid(unsafe_code)]` on all non-CRC modules and `#![deny(unsafe_op_in_unsafe_fn)]` inside the audited [src/crc32c.rs](src/crc32c.rs) hardware intrinsic boundary) owns WAL v1 physical framing (`TWAL0001`), synchronized `BatchBegin`/`DataRecord`/`BatchSeal` durability, single-writer directory locking (`FileStore`), and crash recovery of committed prefixes. Higher-level journals (`tickwire-account-journal`, `tickwire-business-journal`, and `tickwire-config-journal`) drive `Journal<S>` to append opaque payload batches and recover verified `PhysicalFrontier` positions. Payload encoding, economic validation, and trading authority belong strictly above this physical layer.

## Invariants and Change Guidelines

* **Fixed frame geometry, SHA-256 chaining, and optional hardware CRC-32C**: Keep `HEADER_BYTES` (`136`), `TRAILER_BYTES` (`40`), 8-byte alignment, `TWAL0001`/`TEND` magic markers, and SHA-256 `previous_hash` chaining in [src/frame.rs](src/frame.rs) locked to [journal-wire-v1.json](../../specs/contracts/journal-wire-v1.json). Castagnoli CRC-32C lives in [src/crc32c.rs](src/crc32c.rs) (`x86_64` SSE4.2 `_mm_crc32_u64`/`_mm_crc32_u8`, `aarch64` `__crc32cd`/`__crc32cb`, and compile-time `const TABLES` Slicing-by-8 fallback `crc32c_slicing8` with zero bit-by-bit loops). By default (`default = []`), `frame::encode` writes deterministic `0` into `header_crc32c` and `payload_crc32c` without computing CRC-32C on the hot path, and `frame::decode` skips CRC-32C recomputation while still verifying the full-frame `SHA-256` digest; enabling `--features crc32` activates zero-allocation header and payload CRC-32C generation and strict corruption rejection.
* **All-profile integer overflow checks**: Root `Cargo.toml` enforces `overflow-checks = true` across `[profile.dev]`, `[profile.test]`, `[profile.release]`, and `[profile.bench]`; [tests/overflow_checks.rs](tests/overflow_checks.rs) verifies deterministic panics on `u64`/`i64`/`i128` addition, subtraction, and multiplication overflows in both debug and `--release` builds.
* **Torn-tail truncation vs. interior quarantine**: During `Journal::recover` in [src/journal.rs](src/journal.rs), truncate only an unsealed or torn tail at the end of the final segment; any checksum, hash-chain, LSN, or sequence corruption inside an already-sealed prefix or before a `required` `PhysicalFrontier` must return `Error::Quarantined` and refuse startup.
* **Selectable commit profiles (`StorageMode`)**: [src/store.rs](src/store.rs) provides `InMemoryStore` (default zero-disk volatile RAM for ephemeral cloud Pods), `MmapAsyncStore` (preallocated `MappedSegment` + `MappedCursorPage` memcpy commit with a background `fdatasync` flusher thread for sub-microsecond hot-path appends and same-host crash recovery), and `FileStore` (`LocalDurable` synchronous `fsync`), unified behind `JournalStore`.
* **Single-writer lock and fsync discipline**: `FileStore` and `MmapAsyncStore` in [src/store.rs](src/store.rs) acquire a nonblocking `LOCK` file on a canonicalized `0o700` directory, write `MANIFEST.tmp` before atomic rename to `MANIFEST.json`, and sync directory entries on segment creation and manifest publication.
* **Poison on uncertain append**: If any segment append, sync, or manifest step fails in [src/journal.rs](src/journal.rs), mark the `Journal` poisoned (`Error::Poisoned`) so callers cannot issue subsequent appends without reopening and recovering from disk.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/crc32c.rs](src/crc32c.rs) · [src/frame.rs](src/frame.rs) · [src/journal.rs](src/journal.rs) · [src/store.rs](src/store.rs)
* **Integration tests and benchmarks**: [tests/frame.rs](tests/frame.rs) · [tests/overflow_checks.rs](tests/overflow_checks.rs) · [tests/journal.rs](tests/journal.rs) · [tests/manifest.rs](tests/manifest.rs) · [tests/process.rs](tests/process.rs) · [tests/recovery_validator.rs](tests/recovery_validator.rs) · [tests/storage_modes.rs](tests/storage_modes.rs) · [benches/frame_crc32c.rs](benches/frame_crc32c.rs)
* **Specifications, contracts, and models**: [journal.md](../../specs/implementation/journal.md) · [journal-wire-v1.json](../../specs/contracts/journal-wire-v1.json) · [physical_journal](../../tla/execution_models/physical_journal/ROADMAP.md) · [account_journal](../../tla/execution_models/account_journal/ROADMAP.md)

```sh
cargo test --locked -p tickwire-journal --all-targets
cargo test --locked -p tickwire-journal --all-targets --features crc32
cargo test --locked --release -p tickwire-journal --test overflow_checks
cargo bench --locked -p tickwire-journal --bench frame_crc32c
python3 scripts/verify-design-contracts.py
./scripts/check-tla.sh physical_journal
```
