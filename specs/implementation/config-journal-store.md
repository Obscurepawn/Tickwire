# Descriptor-Relative Linux Backend for the Configuration WAL

`tickwire_config_fs::JournalStore` adapts an exclusively locked `tickwire-config-fs::Store` to the physical `tickwire_journal::store::Store` trait on Linux (returning `Error::Unsupported` on other operating systems). It executes every directory scan, segment creation, append, truncation, synchronization, and `MANIFEST.json` swap through pinned directory file descriptors.

`JournalStore` owns directory inventory hygiene, segment inode/timestamp pinning across read-append-sync boundaries, and strict manifest compare-and-swap semantics. `tickwire-journal` owns WAL v1 batch framing, `crc32c`/`SHA-256` hash chains, contiguous sequence numbering, and `PhysicalFrontier` validation, while `tickwire-config-journal` owns the four-phase configuration operation state machine.

## State Model and Bounds

`JournalStore::new(root, limits)` validates `JournalLimits` and the underlying `Store` before exposing the backend. Directory scans invoke `SYS_getdents64` directly on a fresh descriptor cloned from `root` without consulting `/proc` or resolving symbolic links.

| Limit or File Rule | Default / Enforced Bound |
| --- | --- |
| `max_segment_bytes` | `16 MiB` default; valid range `528..=268,435,456` bytes (`256 MiB`, matching `tickwire-journal`) |
| `max_segments` & scan budget | `128` segments default (valid range `1..=4,096`); directory entry scan capped at `max_segments + 34` entries |
| `max_total_bytes` | `256 MiB` default; valid range `max_segment_bytes..=4 GiB` across all `segment-*.wal` files |
| Permitted directory entries | Only `LOCK`, `MANIFEST.json` (`<= 64 KiB`), `.tw-tmp-<32-lowercase-hex>` (`<= 64 KiB`), and 20-digit decimal `segment-{id:020}.wal` (`id > 0`) |
| Segment file invariants | Regular file, mode `0600`, `nlink == 1`, matching `owner` UID and filesystem `device`, length `<= max_segment_bytes` |

## Core Invariants

* **Strict directory whitelist and `observed` metadata pinning**: Any unexpected filename, subdirectory, symlink, hardlink, cross-device file, or UID/mode mismatch fails `scan()` immediately. Whenever `JournalStore` reads, creates, appends, truncates, or syncs a segment, it records `Metadata` in `observed: BTreeMap<u64, Metadata>` and runs `pinned(id, file, expected)`, which reopens `segment-{id:020}.wal` from `root` and verifies `same_version`—matching `(dev, ino, len, mtime, mtime_nsec, ctime, ctime_nsec)`—on *both* the held file descriptor and the currently visible directory entry. Consequently, the exact inode and byte image validated during recovery is guaranteed to be the file flushed by `sync_segment`, and any disappearance or out-of-band edit of a previously observed segment triggers `Error::Conflict`.
* **Retained `Pending` descriptor and multi-segment recovery ordering**: `create_segment` (`O_RDWR | O_CREAT | O_EXCL`, `0600`) syncs both the new empty file and `root` before caching `Pending { id, file, metadata, dirty: false }`. `append_segment` verifies `pending.metadata.len() == offset` and total byte budgets, writes at `offset`, updates `observed`, and marks `pending.dirty = true` so the exact written descriptor is retained for `sync_segment` (forbidding switching to another segment while `dirty` during normal appends). During crash recovery, `tickwire-journal` truncates an incomplete tail on the *last* segment first (`dirty = true`) and then calls `sync_segment` across all segments in ascending order (`1..=last`); `sync_segment` explicitly preserves the dirty last-segment `Pending` handle while independently opening, verifying, and syncing earlier verified prefix segments until `id == last` clears `dirty`.
* **Strict manifest compare-and-swap (`install_expected`)**: `read_manifest` caches the observed `SHA-256` of `MANIFEST.json` (or `None` when absent) and rejects subsequent reads if the on-disk digest changes unexpectedly. `publish_manifest` invokes `root.install_expected("MANIFEST.json", bytes, self.manifest)`, which checks `actual_previous == self.manifest` *before* allowing exact-byte retry short-circuiting—ensuring an external actor replacing `MANIFEST.json` out-of-band causes `Error::Conflict` even if the injected file happens to match the newly proposed manifest bytes.

## Operations and Failure Semantics

* **Immediate store poisoning on uncertain mutation**: Any failure during `publish_manifest`, `create_segment`, `append_segment`, `truncate_segment`, or `sync_segment` sets `root.poisoned = true`, blocking all further I/O on the handle until a fresh `Store` and `JournalStore` reopen and recover the directory.

## Verification and References

```sh
cargo test --locked -p tickwire-config-fs -p tickwire-journal
cargo clippy --locked -p tickwire-config-fs --all-targets -- -D warnings
```

Linux test suites cover manifest drift detection, mismatched read/sync inode detection, ascending multi-segment recovery sync while holding a truncated final segment dirty, and real child-process `SIGKILL` cuts mid-write, pre-sync, post-sync, and post-receipt (noting that `SIGKILL` tests leave the host kernel running and do not by themselves simulate physical storage power loss).

* Linux journal store wrapper and implementation: [journal.rs](../../crates/tickwire-config-fs/src/journal.rs), [linux/journal.rs](../../crates/tickwire-config-fs/src/linux/journal.rs)
* Integration and crash-cut process tests: [journal.rs](../../crates/tickwire-config-fs/tests/journal.rs), [journal_process.rs](../../crates/tickwire-config-fs/tests/journal_process.rs)
* Related specifications: [Linux configuration file store](config-file-store.md), [Physical write-ahead log](journal.md), [Local configuration journal](config-local-journal.md)
