# tickwire-journal

`tickwire-journal` (`#![forbid(unsafe_code)]`) owns WAL v1 physical framing (`TWAL0001`), synchronized
`BatchBegin`/`DataRecord`/`BatchSeal` durability, single-writer directory locking (`FileStore`), and crash recovery of
committed prefixes. Higher-level journals (`tickwire-account-journal` and `tickwire-business-journal`) drive
`Journal<S>` to append opaque payload batches and recover verified `PhysicalFrontier` positions. Payload encoding,
economic validation, and trading authority belong strictly above this physical layer.

## Invariants and Change Guidelines

* **Fixed frame geometry and checksums**: Keep `HEADER_BYTES` (`136`), `TRAILER_BYTES` (`40`), 8-byte alignment,
  `TWAL0001`/`TEND` magic markers, Castagnoli CRC-32C, and SHA-256 `previous_hash` chaining in
  [src/frame.rs](src/frame.rs) locked to [journal-wire-v1.json](../../specs/contracts/journal-wire-v1.json).
* **Torn-tail truncation vs. interior quarantine**: During `Journal::recover` in [src/journal.rs](src/journal.rs),
  truncate only an unsealed or torn tail at the end of the final segment; any checksum, hash-chain, LSN, or sequence
  corruption inside an already-sealed prefix or before a `required` `PhysicalFrontier` must return
  `Error::Quarantined` and refuse startup.
* **Single-writer lock and fsync discipline**: `FileStore` in [src/store.rs](src/store.rs) acquires a nonblocking
  `LOCK` file on a canonicalized `0o700` directory, syncs segment data before sealing, writes `MANIFEST.tmp` before
  atomic rename to `MANIFEST.json`, and syncs the directory entry on segment creation and manifest publication.
* **Poison on uncertain append**: If any segment append, sync, or manifest step fails in
  [src/journal.rs](src/journal.rs), mark the `Journal` poisoned (`Error::Poisoned`) so callers cannot issue subsequent
  appends without reopening and recovering from disk.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/frame.rs](src/frame.rs) · [src/journal.rs](src/journal.rs) ·
  [src/store.rs](src/store.rs)
* **Integration tests**: [tests/frame.rs](tests/frame.rs) · [tests/journal.rs](tests/journal.rs) ·
  [tests/manifest.rs](tests/manifest.rs) · [tests/process.rs](tests/process.rs) ·
  [tests/recovery_validator.rs](tests/recovery_validator.rs)
* **Specifications, contracts, and models**: [journal.md](../../specs/implementation/journal.md) ·
  [journal-wire-v1.json](../../specs/contracts/journal-wire-v1.json) ·
  [physical_journal](../../tla/execution_models/physical_journal/ROADMAP.md) ·
  [account_journal](../../tla/execution_models/account_journal/ROADMAP.md)

```sh
cargo test --locked -p tickwire-journal --all-targets
python3 scripts/verify-design-contracts.py
./scripts/check-tla.sh physical_journal
```
