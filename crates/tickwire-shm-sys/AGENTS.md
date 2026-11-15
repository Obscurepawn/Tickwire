# tickwire-shm-sys

`tickwire-shm-sys` is the audited Linux shared-memory (`memfd`) and startup page-locking (`mlock`)
boundary (`#![deny(unsafe_op_in_unsafe_fn)]`) providing `WriterMapping`, `ReaderMapping`,
`ImmutableMapping`, `ImmutableResource`, and the safe `mlock` helpers (`MlockPolicy`,
`MlockOutcome`, `mlock_slice`, `munlock_slice`, `try_mlock_slice`, `MlockedSlice`, `MlockedBox`).
Higher-level ring reclamation and JOIN coordination live in `tickwire-shm` and `tickwire-join`;
this crate isolates `memfd_create`, `fcntl` file seals, `mmap` / `munmap`, `mlock` / `munlock`,
`MADV_DONTFORK`, and aligned 64-bit atomic loads and stores on 4096-byte-paged little-endian
`x86_64` and `aarch64` Linux.

## Invariants and Change Guidelines

* **Seal profiles and `O_RDONLY` downgrade**: Enforce `SINGLE_WRITER_SEALS` (`0x17`:
  `F_SEAL_SEAL | F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_FUTURE_WRITE`) on ring/cursor mappings in
  [src/lib.rs](src/lib.rs) and `IMMUTABLE_SEALS` (`0x0f`, including `F_SEAL_WRITE`) in
  [src/immutable.rs](src/immutable.rs). `ImmutableMapping::create` writes via `pwrite` before
  sealing without ever creating a writable mapping, and `ImmutableMapping::map` reopens `O_RDWR`
  descriptors as `O_RDONLY | O_CLOEXEC` via `/proc/self/fd/<fd>` before `mmap` so `SCM_RIGHTS`
  recipients retain no write capability.
* **Descriptor and VMA isolation**: Every mapped or duplicated descriptor must carry `FD_CLOEXEC`,
  validate `S_IFREG` and exact page-aligned length (`PAGE_BYTES = 4096`), and apply `MADV_DONTFORK`
  so child processes never inherit live shared mappings across `fork`. Keep `Mapping` `!Send` and
  `!Sync` (`NonNull<u8>`).
* **Aligned atomic ordering without exported references**: Never expose `&AtomicU64` across process
  boundaries. Check 8-byte alignment and bounds (`check_atomic`) before every load/store, pairing
  `Ordering::Relaxed` loads on read-only pages with `fence(Ordering::Acquire)` (`load_u64_acquire`)
  and using `Ordering::Release` stores on writer pages.
* **Resource envelope verification**: [src/resource.rs](src/resource.rs) (`ImmutableResource`)
  checks the 4096-byte `ResourceHeader` and SHA-256 content hash against externally authenticated
  metadata, leaving interior section and JOIN business checks to `tickwire-snapshot`.
* **Audited startup `mlock` boundary**: [src/mlock.rs](src/mlock.rs) encapsulates `libc::mlock` and
  `libc::munlock` so downstream `#![forbid(unsafe_code)]` crates (`tickwire-account`) can pin
  preallocated hot slot pools at cold startup. `MlockedSlice<T>` and `MlockedBox<T>` pass raw
  allocation addresses and `size_of_val` directly to the kernel without constructing `&[u8]`
  references over struct padding or interior atomics, support `MlockPolicy::{Disabled, BestEffort,
  Strict}` for unprivileged CI environments where `RLIMIT_MEMLOCK` is constrained, and unlock on
  `Drop` when `outcome == MlockOutcome::Locked`.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/immutable.rs](src/immutable.rs) ·
  [src/resource.rs](src/resource.rs) · [src/mlock.rs](src/mlock.rs) ·
  [tests/mapping.rs](tests/mapping.rs) · [tests/immutable.rs](tests/immutable.rs) ·
  [tests/mlock.rs](tests/mlock.rs) · [tests/process.rs](tests/process.rs)
* **Specifications**: [shm-mapping.md](../../specs/implementation/shm-mapping.md) ·
  [immutable-resource.md](../../specs/implementation/immutable-resource.md)

```sh
cargo test --locked -p tickwire-shm-sys --all-targets
./scripts/check-linux.sh
```
