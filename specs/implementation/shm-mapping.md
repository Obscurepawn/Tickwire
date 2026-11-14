# Single-Writer Shared-Memory Mappings on Linux

`tickwire-shm-sys` implements the kernel-enforced single-writer shared-memory primitive (`WriterMapping` and `ReaderMapping`) backing Tickwire's broadcast rings and per-subscriber cursor pages. By mapping a zero-filled Linux `memfd` `MAP_SHARED | PROT_READ | PROT_WRITE` inside its sole creator *before* applying the `F_SEAL_FUTURE_WRITE` seal profile (`0x17`), the creator retains its established writable mapping while the Linux kernel permanently denies `write(2)`/`pwrite(2)` syscalls, file growth or truncation, new shared writable mappings, and `mprotect(..., PROT_WRITE)` upgrades on any subsequently exported file descriptor.

This layer owns Linux `memfd` creation, page-size and seal verification, `MADV_DONTFORK` virtual-memory isolation, and aligned 64-bit acquire/release atomic accessors. It intentionally keeps byte-slice and atomic accessors `unsafe` and performs neither Tickwire header validation nor slot reclamation: `tickwire-shm` owns the ring publication/borrow state machine that prevents concurrent overwrites of borrowed slots, while `tickwire-control-sys` and `tickwire-control` own authenticated `SCM_RIGHTS` descriptor delivery. Linux `memfd` visibility resides solely in volatile RAM and provides zero disk durability.

## State Model and Bounds

Supported targets are restricted at compile and runtime to little-endian 64-bit Linux (`x86_64` or `aarch64`) with 64-bit native atomics and a 4,096-byte OS page size (`sysconf(_SC_PAGESIZE) == 4096`). Both `WriterMapping` and `ReaderMapping` wrap an internal `Mapping { ptr: NonNull<u8>, len: usize, fd: OwnedFd }` that implements neither `Clone`, `Send`, nor `Sync`, pinning ownership to a single thread.

| Parameter or Seal | Value / Enforcement |
| --- | --- |
| `PAGE_BYTES` / `ISOLATION_BYTES` | `4,096` bytes per OS page / `128` bytes cache-line isolation stride |
| Valid mapping length | Nonzero exact multiple of `4,096` up to `isize::MAX` bytes (`Error::InvalidLength` otherwise) |
| `memfd` name | `1..=249` non-NUL bytes (`MFD_CLOEXEC \| MFD_ALLOW_SEALING`) |
| `SINGLE_WRITER_SEALS` (`0x17`) | `F_SEAL_SEAL (0x01) \| F_SEAL_SHRINK (0x02) \| F_SEAL_GROW (0x04) \| F_SEAL_FUTURE_WRITE (0x10)` |
| `IMMUTABLE_SEALS` (`0x0f`) | `F_SEAL_SEAL (0x01) \| F_SEAL_SHRINK (0x02) \| F_SEAL_GROW (0x04) \| F_SEAL_WRITE (0x08)` (used by `ImmutableMapping`, rejected by `ReaderMapping`) |
| Atomic access alignment | 8-byte aligned offset (`offset % align_of::<AtomicU64>() == 0`) with `offset + 8 <= len` |

## Core Invariants

* **Establish writable mapping before sealing, export descriptors only after sealing**: `WriterMapping::create` allocates the `memfd`, sets its exact size with `ftruncate`, maps it `PROT_READ | PROT_WRITE` with `MAP_SHARED`, and applies `madvise(..., MADV_DONTFORK)` while `sealed` remains `false`. Calling `duplicate_fd()` before `seal_for_readers()` returns `Error::NotSealed`, ensuring no unsealed descriptor can leave the constructor. `seal_for_readers()` applies `0x17` via `fcntl(F_ADD_SEALS)` and reads back `F_GET_SEALS == 0x17` both on seal and on every subsequent `duplicate_fd()`.
* **Strict reader verification before `mmap`**: `ReaderMapping::map(fd, expected_len)` consumes the `OwnedFd` on both success and error. Before creating a `PROT_READ | MAP_SHARED` mapping, it verifies `S_IFREG` via `fstat`, exact `st_size == expected_len`, exact `F_GET_SEALS == 0x17` (preventing confusion with `0x0f` immutable snapshots or unsealed files), sets `FD_CLOEXEC`, and applies `MADV_DONTFORK` so forked child processes never inherit shared-memory virtual address ranges.
* **Read-only page-safe acquire loads and deterministic teardown**: On `x86_64` and `aarch64`, `load_u64_acquire` executes a `Relaxed` 64-bit atomic load through `&AtomicU64` followed by `fence(Ordering::Acquire)` (avoiding any read-modify-write instruction that would fault on `PROT_READ` pages), pairing with the writer's `AtomicU64::store(..., Ordering::Release)` without ever exporting `&AtomicU64` or `*const AtomicU64` across the API boundary. On `Drop`, `Mapping` calls `munmap(ptr, len)` *before* `OwnedFd` closes the underlying file descriptor.

## Operations and Failure Semantics

* **Checked arithmetic and `unsafe` caller obligations**: Every `read_bytes`, `write_bytes`, `load_u64_acquire`, and `store_u64_release` call checks `offset.checked_add(len) <= self.len` (`Error::OutOfBounds`) and 8-byte alignment on atomics (`Error::Misaligned`) before dereferencing `self.ptr`. Because bounds checks alone cannot prevent cross-process data races, callers discharging the `unsafe` contract (`tickwire-shm::mapped`) must guarantee:
  1. Atomic offsets are accessed exclusively as aligned 64-bit atomics across all processes after initial pre-seal initialization, and byte slices (`read_bytes` / `write_bytes`) never overlap live atomic fields.
  2. `write_bytes` runs only under exclusive slot/block ownership when no reader holds a live reference to that range.
  3. `read_bytes` runs only after an `Acquire` load has observed the writer's `Release` publication and while a live reader reservation (`RecordBorrow` in `tickwire-shm::mapped`) prevents the writer from reclaiming the slot—establishing the 8-byte-aligned (`SLOTS = 8192`, `record_bytes = 512`), immutable byte-slice precondition required for zero-copy `&MarketRecord` / `&MarketExactRecord` references (`RecordBorrow::as_market_record`, `as_market_exact_record`, and `as_market_state`).
* **Platform and resource failures**: Non-Linux or non-4-KiB-page hosts fail deterministically with `UnsupportedPlatform` or `PlatformMismatch`. Any failure in `ftruncate`, `mmap`, `madvise(MADV_DONTFORK)`, or seal verification unmaps any partial mapping and closes the owned descriptor immediately. Deployment-specific latency properties—such as `mlock`, hugepage policy, CPU core isolation, page-fault pre-warming, and NUMA placement—require independent host configuration and measurement.

## Memory-Mapped File Segments and Cursor Pages (`MappedSegment`, `MappedCursorPage`)

To eliminate hot-path `write(2)` and synchronous `fdatasync(2)` syscalls in `MmapAsync` journal persistence while keeping downstream journal crates `#![forbid(unsafe_code)]`, `tickwire-shm-sys::mapped_file` encapsulates regular-file `MAP_SHARED` mappings behind a safe, single-writer boundary:

* **`MappedSegment`**: Opens or creates a regular file (`0600`, `O_RDWR | O_CLOEXEC | O_NOFOLLOW`), acquires a non-blocking exclusive `flock(LOCK_EX | LOCK_NB)`, extends or verifies the file to a 4,096-byte page-aligned capacity, maps it `PROT_READ | PROT_WRITE` with `MAP_SHARED`, applies `MADV_DONTFORK`, and pre-faults writable pages during startup/segment creation so hot-path `write_at(offset, bytes)` executes a bounds-checked memory copy directly into the Linux page cache (`~100 ns`, zero syscalls, zero minor page faults on pre-warmed pages). `duplicate_sync_fd()` clones a `CLOEXEC` descriptor for an asynchronous background flusher thread to invoke `fdatasync` without contending on the mapped pointer, and `truncate_unmap(logical_len)` unmaps the segment before `ftruncate` trims unused preallocated trailing zeros on segment rotation or graceful shutdown.
* **`MappedCursorPage` (`MMAP-CURSOR.bin`)**: Maps a single 4,096-byte file containing two cache-line-isolated 128-byte slots (`slot 0` at byte `0`, `slot 1` at byte `128`). Each commit writes `(magic = "TWMC0001", generation, segment_id, logical_len, sha256)` into the alternate slot after a `Release` fence over the segment payload. Because both mappings are `MAP_SHARED` in the kernel page cache, an abrupt process crash (`SIGKILL` or panic) preserves the highest valid `generation` slot, allowing reopen recovery to `ftruncate` the active preallocated segment to `logical_len` before scanning `TWAL0001` frames.

## Verification and References

```sh
cargo test --locked -p tickwire-shm-sys
cargo clippy --locked -p tickwire-shm-sys --all-targets -- -D warnings
cargo rustc --locked -p tickwire-shm-sys --release -- --emit=asm
```

The Linux test suite spawns a real child reader process before the parent writes 60 KiB and verifies full payload visibility across `store_u64_release` / `load_u64_acquire`, alongside regression tests for `0x17` seal bits, zero-filled pages, `FD_CLOEXEC`, `MADV_DONTFORK` `/proc/self/smaps` flags, `EACCES` on `mprotect` write upgrades, misaligned atomics, and non-page-multiple lengths. On the archived Linux `6.8.0-117-generic` AArch64 target (4,096-byte pages), `store_u64_release` lowers to `stlr` and `load_u64_acquire` lowers to `ldr` followed by `dmb ishld`.

* Implementation: [lib.rs](../../crates/tickwire-shm-sys/src/lib.rs)
* Test suites: [mapping.rs](../../crates/tickwire-shm-sys/tests/mapping.rs), [process.rs](../../crates/tickwire-shm-sys/tests/process.rs)
* Related specifications: [Shared-memory broadcast ring](shm-ring.md), [Immutable Linux resources](immutable-resource.md), [Control transport](control-transport.md)
