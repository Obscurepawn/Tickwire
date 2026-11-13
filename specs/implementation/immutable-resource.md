# Immutable Shared-Memory Resources and Envelope Validation

`ImmutableMapping` and `ImmutableResource` in `tickwire-shm-sys` package static, cross-process artifacts—market topic snapshots, replay/recovery bundles, and permission-scoped configuration projections (resource kinds `5..=9`)—into kernel-sealed, read-only Linux `memfd` objects. Unlike single-writer rings (`0x17`), an `ImmutableMapping` is populated entirely via `pwrite` before any `mmap` exists, locked with the strict `0x0f` seal mask (`F_SEAL_SEAL | F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE`), and converted to an `O_RDONLY | O_CLOEXEC` file description before mapping. Because the kernel permanently prohibits all future writes and size changes through any descriptor or mapping, `ImmutableMapping::bytes(&self)` can safely lend an ordinary `&[u8]` slice for the lifetime of the mapping without atomic barriers per read.

Physical immutability proves that bytes will not change while mapped, whereas `ImmutableResource` proves that the outer 4,096-byte `ResourceHeader`, zeroed page tail, and whole-file SHA-256 match *externally authenticated* metadata from a control session or deployment manifest. Neither type grants `JOIN` admission or validates internal business records: `tickwire-snapshot` separately validates internal member digests, instrument-set coverage, and freshness deadlines.

## State Model and Bounds

Every immutable resource file is page-aligned (`4,096` bytes) up to `64 MiB`, beginning with a 4,096-byte `ResourceHeader` (`TWIPC001`, ABI `1.0`, schema `1.0`) at offset `0`, `content_bytes > 0` starting at `slot_offset == 4096`, and zero-filled padding out to the page-rounded file end `padded_end == len`.

| Resource Kind / Region | Constraints and Envelope Rules |
| --- | --- |
| Kernel seal profile | Exact `IMMUTABLE_SEALS = 0x0f` (`F_SEAL_SEAL \| F_SEAL_SHRINK \| F_SEAL_GROW \| F_SEAL_WRITE`); `WrongSeals` on any missing or extra seal bit (such as `0x17`) |
| Envelope size bounds | Total mapped file `<= 64 MiB` (`67,108,864` bytes); `content_bytes > 0`; `align_up_4096(4096 + content_bytes) == mapping.len()`; bytes `(4096 + content_bytes)..len` must all be `0` |
| `ResourceHeader` (`0..4096`) | Exact equality with caller's `expected: &ResourceHeader`; `descriptor_hash` (`bytes 272..304`) equals SHA-256 of the 4,096-byte header with `272..304` zeroed; nonzero `resource_id`, `producer_instance`, `host_boot_id`, `namespace_id`; `producer_generation` in `1..2^63`; all epoch/sequence counters `< 2^63`; `producer_page_offset`, `cursor_page_offset`, `arena_offset`, `arena_bytes`, `arena_class_count`, and `class_descriptors` all zero |
| Feature mask | `required_features & 0x1c == 0x1c`; `required_features` and `optional_features` within `0x2ff`; exact-market feature bit `9` (`512`) permitted only on kinds `5` and `8` |
| Snapshot kinds (`5..=8`) | `content_bytes >= 256`; `SnapshotHeader` at `4096..4352` (`TWSNP001`, schema `1.0`, `object_bytes == len`, `entry_offset == 4352`, `sequence < 2^63`, `coverage_flags & !3 == 0`); `4352 + entry_count * entry_bytes <= 4096 + content_bytes`; passes `tickwire_ipc::validate_resource_shape` |
| Config projection (`kind 9`) | Requires config feature bit `5` (`0x20`), nonzero `consumer_instance`, `record_bytes == 1`, and `header.capacity == content_bytes <= 4 MiB` (`4,194,304` bytes) |

## Core Invariants

* **Zero writable mappings during creation**: `F_SEAL_WRITE` fails with `EBUSY` on Linux if any shared writable `mmap` exists. `ImmutableMapping::create(name, bytes)` therefore creates a private `memfd`, sets its page-aligned size via `ftruncate`, writes the complete buffer via `pwrite` (retrying `EINTR` and partial writes, and failing on `WriteZero`), applies and verifies `0x0f` seals while no mapping exists, and then delegates to `ImmutableMapping::map` so no unsealed or writable handle ever escapes.
* **Mandatory `O_RDONLY` downgrade on receipt**: Because `SCM_RIGHTS` preserves the sender's file-description access mode (`O_RDWR` vs. `O_RDONLY`), `ImmutableMapping::map` inspects `fcntl(F_GETFL) & O_ACCMODE` after checking `S_IFREG`, `st_size`, and `0x0f` seals. An `O_RDONLY` input is duplicated with `FD_CLOEXEC`; an `O_RDWR` input is kept alive only long enough to reopen `/proc/self/fd/<fd>` with `O_RDONLY | O_CLOEXEC` and then closed, ensuring `ImmutableMapping` and every descriptor exported by `duplicate_fd()` carry strictly read-only kernel file descriptions.
* **External trust anchor for `validate_resource`**: Computing expected digests or identities from the mapped file itself would prove only self-consistency against an attacker-crafted `memfd`. `ImmutableMapping::validate_resource(expected, content_bytes, object_hash)` compares the decoded `ResourceHeader` against the caller's independently authenticated `expected` struct, recomputes `descriptor_hash`, validates kind-specific header geometry and zeroed tail bytes, and hashes the *entire* mapped byte slice `[0..len]` against the external `object_hash` before returning `ImmutableResource`.

## Operations and Failure Semantics

* **Envelope vs. payload access**: `ImmutableResource::bytes()` returns the full validated `[0..len]` file image (required by `tickwire-snapshot::validate`, which checks absolute file offsets starting from `0`), while `ImmutableResource::content()` strips the 4,096-byte outer `ResourceHeader` and trailing page padding to return `[4096..4096 + content_bytes]`.
* **Descriptor consumption and teardown**: `ImmutableMapping::map` takes `OwnedFd` by value and closes it on any size, file-type, seal, or `mmap`/`MADV_DONTFORK` failure. Dropping `ImmutableResource` or `ImmutableMapping` unmaps the `PROT_READ` region before closing the read-only descriptor.

## Verification and References

```sh
cargo test --locked -p tickwire-shm-sys --test immutable
cargo test --locked -p tickwire-snapshot --test immutable_linux
```

Linux integration tests verify `O_RDONLY | FD_CLOEXEC` export even when constructed from `O_RDWR` descriptors, exact `0x0f` seal enforcement, kernel denial of `write`/`ftruncate`/`PROT_WRITE` mappings and `mprotect` upgrades, `F_SEAL_WRITE` failure while a writable mapping is live followed by success after `munmap`, and header-corruption cases where `expected` is deliberately set to match the tampered header so only the independent `descriptor_hash` and `object_hash` checks catch the mutation.

* Implementation: [immutable.rs](../../crates/tickwire-shm-sys/src/immutable.rs), [resource.rs](../../crates/tickwire-shm-sys/src/resource.rs)
* Test suites: [immutable.rs](../../crates/tickwire-shm-sys/tests/immutable.rs), [immutable_linux.rs](../../crates/tickwire-snapshot/tests/immutable_linux.rs)
* Related specifications: [Single-writer Linux mappings](shm-mapping.md), [Validate a market-topic baseline](topic-snapshot.md), [Build a market-topic baseline](topic-snapshot-builder.md)
