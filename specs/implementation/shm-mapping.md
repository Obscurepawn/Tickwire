# Single-Writer Shared-Memory Mappings and Page Locking on Linux

`tickwire-shm-sys` implements the kernel-enforced single-writer shared-memory primitive (`WriterMapping` and `ReaderMapping`) backing Tickwire's broadcast rings and per-subscriber cursor pages, immutable resource mappings (`ImmutableMapping` and `ImmutableResource`), and the audited `mlock(2)` / `munlock(2)` boundary used to pin preallocated hot-path tables in physical RAM at startup.

By mapping a zero-filled Linux `memfd` `MAP_SHARED | PROT_READ | PROT_WRITE` inside its sole creator *before* applying the `F_SEAL_FUTURE_WRITE` seal profile (`0x17`), the creator retains its established writable mapping while the Linux kernel permanently denies `write(2)`/`pwrite(2)` syscalls, file growth or truncation, new shared writable mappings, and `mprotect(..., PROT_WRITE)` upgrades on any subsequently exported file descriptor.

This layer owns Linux `memfd` creation, page-size and seal verification, `MADV_DONTFORK` virtual-memory isolation, aligned 64-bit acquire/release atomic accessors, and startup heap/slice `mlock` helpers. It intentionally keeps shared-mapping byte-slice and atomic accessors `unsafe` and performs neither ring slot reclamation nor `SCM_RIGHTS` delivery: `tickwire-shm` owns the ring publication/borrow state machine that prevents concurrent overwrites of borrowed slots, while `tickwire-control-sys` and `tickwire-control` own authenticated `SCM_RIGHTS` descriptor delivery. Linux `memfd` visibility resides solely in volatile RAM with zero local disk state.

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
* **Platform and resource failures**: Non-Linux or non-4-KiB-page hosts fail deterministically with `UnsupportedPlatform` or `PlatformMismatch`. Any failure in `ftruncate`, `mmap`, `madvise(MADV_DONTFORK)`, or seal verification unmaps any partial mapping and closes the owned descriptor immediately.

## Startup Page Locking (`mlock_slice`, `try_mlock_slice`, `MlockedSlice`, `MlockedBox`)

To keep downstream crates such as `tickwire-account` `#![forbid(unsafe_code)]` while locking preallocated hot-path tables (`OrderManager` slot pools and `FillRecord` hash buckets) into physical RAM at cold startup, `tickwire-shm-sys::mlock` encapsulates `libc::mlock` and `libc::munlock` behind safe APIs:

* **`MlockPolicy` and `MlockOutcome`**:
  * `MlockPolicy::Disabled` skips `mlock(2)` and returns `MlockOutcome::NotRequested`.
  * `MlockPolicy::BestEffort` (default) attempts `mlock(2)` and degrades cleanly to `MlockOutcome::SkippedLimitOrPermission` when `RLIMIT_MEMLOCK` or `CAP_IPC_LOCK` denies locking (`ENOMEM`, `EPERM`, or `EAGAIN`), allowing identical binaries to run in unprivileged CI containers and production hosts with `LimitMEMLOCK=infinity`.
  * `MlockPolicy::Strict` requires `mlock(2)` to succeed on non-empty allocations and returns `Error::System { operation: "mlock", .. }` on kernel failure.
* **Byte-slice and typed-container wrappers**:
  * `mlock_slice(bytes: &[u8]) -> Result<()>`, `munlock_slice(bytes: &[u8]) -> Result<()>`, and `try_mlock_slice(bytes: &[u8], policy: MlockPolicy) -> Result<MlockOutcome>` operate on initialized byte slices; empty slices succeed as no-ops (`MlockOutcome::NotRequested`).
  * `MlockedSlice<T>` and `MlockedBox<T>` convert startup `Vec<T>` / `Box<T>` allocations into fixed-address heap storage (`Box<[T]>` / `Box<T>`) and pass `(ptr, size_of_val)` directly to `libc::mlock` without forming `&[u8]` references over struct padding or interior atomics. When `outcome == MlockOutcome::Locked`, `Drop` invokes `libc::munlock` over the exact owned range before freeing the allocation so repeated test teardowns do not exhaust process `RLIMIT_MEMLOCK`.

## Verification and References

```sh
cargo test --locked -p tickwire-shm-sys --all-targets
cargo clippy --locked -p tickwire-shm-sys --all-targets -- -D warnings
cargo rustc --locked -p tickwire-shm-sys --release -- --emit=asm
```

The Linux test suite spawns a real child reader process before the parent writes 60 KiB and verifies full payload visibility across `store_u64_release` / `load_u64_acquire`, alongside regression tests for `0x17` seal bits, zero-filled pages, `FD_CLOEXEC`, `MADV_DONTFORK` `/proc/self/smaps` flags, `EACCES` on `mprotect` write upgrades, misaligned atomics, non-page-multiple lengths, and `mlock` / `munlock` policy behavior under both normal and constrained `RLIMIT_MEMLOCK` budgets.

* Implementation: [lib.rs](../../crates/tickwire-shm-sys/src/lib.rs), [mlock.rs](../../crates/tickwire-shm-sys/src/mlock.rs)
* Test suites: [mapping.rs](../../crates/tickwire-shm-sys/tests/mapping.rs), [immutable.rs](../../crates/tickwire-shm-sys/tests/immutable.rs), [mlock.rs](../../crates/tickwire-shm-sys/tests/mlock.rs), [process.rs](../../crates/tickwire-shm-sys/tests/process.rs)
* Related specifications: [Shared-memory broadcast ring](shm-ring.md), [Immutable Linux resources](immutable-resource.md), [Control transport](control-transport.md)
