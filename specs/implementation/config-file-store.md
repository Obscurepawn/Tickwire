# Descriptor-Relative Linux Configuration File Store

`tickwire-config-fs::Store` provides symlink-proof, single-writer file persistence on Linux using directory-descriptor-relative system calls (`openat`, `mkdirat`, `renameat`, `unlinkat`, and `SYS_getdents64`) anchored at a caller-supplied parent directory descriptor. Non-Linux targets compile a safe stub that returns `Error::Unsupported`.

`Store` owns directory ownership/mode verification, exclusive `LOCK` acquisition, atomic temporary-file staging with read-back verification, and `DurableFile` `(hash, bytes, device, inode)` receipts. It holds no configuration-commit or runtime authority; `tickwire-config-materializer` and `tickwire-config-journal` determine which projections and TOML files are valid to install.

## State Model and Bounds

All path operations walk relative to pinned `File` descriptors with `O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK`, eliminating `/proc` path lookups and symlink traversal races. Every unsafe `libc` call is isolated inside `crates/tickwire-config-fs/src/os.rs` behind owned Rust `File` handles and checked lengths.

| Resource or Path Rule | Enforced Requirement / Bound |
| --- | --- |
| Trusted parent anchor | Directory owned by `uid 0` (`root`) or designated `owner` UID, with group/other write bits clear (`mode & 0o022 == 0`) |
| Private root & subdirectories | Same filesystem `device`, owned by `owner` UID, exact permissions `0700` (`mode & 0o7777 == 0o700`) |
| Exclusive writer lock (`LOCK`) | Regular `0600` file in `root`, `nlink == 1`, length `0`, locked via non-blocking `File::try_lock()`; synced alongside `root` and `parent` at `open_at` |
| Relative paths & components | Total path `<= 1,024` bytes, `1..=8` slash-separated components; each `1..=128` ASCII `[A-Za-z0-9._-]` bytes, forbidding `.`, `..`, `LOCK`, and `.tw-*` |
| Managed regular files | Exact permissions `0600`, `nlink == 1`, same `device` and `owner` UID, size `1..=8,388,608` bytes (`MAX_FILE_BYTES = 8 MiB`) |
| `DurableFile` receipt | Binds `SHA-256` `hash`, byte length `bytes`, filesystem `device`, and `inode` |

## Core Invariants

* **Continuous root, directory, lock, and file-version pinning**: Before and after every `read`, `create_directory`, and `install` operation, `Store::validate` re-opens the named root under `parent` and `LOCK` under `root` to confirm their `(dev, ino)`, `owner`, and `0700`/`0600` modes have not been swapped or `chmod`-modified out-of-band. Reading an existing file snapshots `Metadata` before and after `read_to_end` and checks `same_version`—requiring identical `(dev, ino, len, mtime, mtime_nsec, ctime, ctime_nsec)`—so concurrent mutation while reading fails immediately.
* **Compare-and-swap installation with read-back and post-rename verification**: `Store::install(path, bytes, expected_previous)` reads any existing target file and compares its `SHA-256` against `expected_previous`. If the existing file already matches `bytes` (and satisfies `expected_previous` when strict checking is requested), `install` still executes `sync_all()` on both the file and its parent directory—healing any prior interrupted sync—before returning `DurableFile`. Otherwise, `install` executes six ordered stages:
  1. **Create**: Open `.tw-tmp-<32-random-hex>` in the target directory with `O_RDWR | O_CREAT | O_EXCL` and mode `0600` (up to 8 CSPRNG collision attempts) and verify `regular(..., limit = 0)`.
  2. **Write & Verify**: Write `bytes`, seek back to offset `0`, and read the full temporary file back to verify exact byte equality before flushing.
  3. **File Sync**: Call `file.sync_all()`, then re-validate `Store`, the target directory inode, and the unchanged prior target file (`verify_existing`).
  4. **Atomic Rename**: Call `renameat` within the pinned parent directory descriptor.
  5. **Directory Sync**: Re-validate `Store` and the parent directory, then call `parent.sync_all()`.
  6. **Final Identity Check**: Re-open the installed target name and verify its `(dev, ino)` and full contents match the synced temporary file descriptor before issuing `DurableFile`.

## Operations and Failure Semantics

* **Post-rename uncertainty and store poisoning**: If an error occurs before `renameat`, `install` unlinks the `.tw-tmp-*` file; if `renameat` has already executed—or if unlinking the temporary file fails—`Store` sets `poisoned = true` and rejects all subsequent operations with `Error::Poisoned`. Callers recover by reopening `Store` and reconciling named files (ignoring leftover `.tw-tmp-*` staging files) against `tickwire-config-journal`.

## Verification and References

```sh
cargo test --locked -p tickwire-config-fs
cargo clippy --locked -p tickwire-config-fs --all-targets -- -D warnings
```

Linux unit and integration suites test symlink/hardlink/permission/UID rejection, root and `LOCK` replacement detection, exact-repeat re-synchronization, and fault-injected failures across write, read-back, file-sync, rename, and directory-sync stages.

* Store API, Linux implementation, and audited syscall boundary: [lib.rs](../../crates/tickwire-config-fs/src/lib.rs), [linux.rs](../../crates/tickwire-config-fs/src/linux.rs), [os.rs](../../crates/tickwire-config-fs/src/os.rs)
* File durability integration tests: [files.rs](../../crates/tickwire-config-fs/tests/files.rs)
* Formal verification: [Config activation file-receipt TLA+ model](../../tla/execution_models/config_activation/PLAN.md)
* Related specifications: [Configuration journal Linux backend](config-journal-store.md), [Local configuration materializer](config-materializer.md)
