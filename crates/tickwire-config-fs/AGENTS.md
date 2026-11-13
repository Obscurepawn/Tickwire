# tickwire-config-fs

`tickwire-config-fs` owns Linux descriptor-relative filesystem persistence (`Store` and
`JournalStore`) for configuration files and physical journal segments, returning verified
`DurableFile` metadata (`hash`, `bytes`, `device`, `inode`) without holding configuration-validation
or runtime policy authority. Crate-wide `#![deny(unsafe_code)]` and
`#![deny(unsafe_op_in_unsafe_fn)]` confine raw `openat` / `mkdirat` / `renameat` / `unlinkat` FFI
calls exclusively to [src/os.rs](src/os.rs).

## Invariants and Change Guidelines

* **Descriptor-anchored traversal without symlink following**: Every directory and file operation in
  [src/linux.rs](src/linux.rs), [src/linux/journal.rs](src/linux/journal.rs), and
  [src/os.rs](src/os.rs) resolves single path components relative to pinned directory `File`
  descriptors with `O_NOFOLLOW | O_CLOEXEC`, verifying UID ownership (`owner`), single-filesystem
  `st_dev` (`device`), and `0o700` / `0o600` permission masks before and after each step.
* **Single-writer lock and crash-atomic `install`**: `Store::open_at` acquires an exclusive
  non-blocking lock on zero-byte `LOCK` and syncs the lock, root, and parent descriptors.
  `Store::install` checks `expected_previous` SHA-256 (`Error::Conflict`), writes a randomized
  temporary sibling up to `MAX_FILE_BYTES` (8 MiB), re-reads and hashes the written inode, calls
  `sync_all` on the temporary file, atomically replaces the target via `renameat`, and calls
  `sync_all` on the parent directory, latching `poisoned` if post-rename verification fails.
* **Physical journal backing**: Keep segment creation, append synchronization, and cross-process
  writer fencing in [src/journal.rs](src/journal.rs) and
  [src/linux/journal.rs](src/linux/journal.rs) aligned with `tickwire-journal::store::Store`.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/linux.rs](src/linux.rs) ·
  [src/os.rs](src/os.rs) · [src/journal.rs](src/journal.rs) · [tests/files.rs](tests/files.rs) ·
  [tests/journal.rs](tests/journal.rs) · [tests/journal_process.rs](tests/journal_process.rs)
* **Specifications**: [config-file-store.md](../../specs/implementation/config-file-store.md) ·
  [config-journal-store.md](../../specs/implementation/config-journal-store.md)

```sh
cargo test --locked -p tickwire-config-fs --all-targets
./scripts/check-linux.sh
```
