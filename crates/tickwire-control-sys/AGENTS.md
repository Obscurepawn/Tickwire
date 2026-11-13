# tickwire-control-sys

`tickwire-control-sys` is the audited Linux OS boundary (`#![deny(unsafe_op_in_unsafe_fn)]`) for `AF_UNIX`
`SOCK_SEQPACKET` control sockets (`ControlListener`, `PendingConnection`, `Connection`), `0o700` pinned
directories (`PrivateDirectory`), sealed ELF execution (`ExecutableImage`), and `/proc` + cgroup v2 process
resource inspection (`ResourceObservation`). Higher-level session handshakes and supervisor state machines
belong in `tickwire-control` and `tickwire-supervisor` while retaining `#![forbid(unsafe_code)]`.

## Invariants and Change Guidelines

* **Bounded `SCM_RIGHTS` receipt and `CLOEXEC` cleanup**: In [src/linux.rs](src/linux.rs), receive packets
  with `MSG_CMSG_CLOEXEC`, immediately wrap every ancillary raw descriptor in `OwnedFd` before inspecting
  truncation (`MSG_TRUNC` / `MSG_CTRUNC`) or frame headers so rejected packets never leak descriptors, and
  enforce `MAX_FRAME_BYTES` (`65,536`), `MAX_FDS` (`16`), exact `ControlEnvelope::fd_count` parity, and
  `SO_PEERCRED` (`PeerCredentials`).
* **Pinned inode directory hygiene**: `PrivateDirectory` and `OwnedEntry` ([src/linux.rs](src/linux.rs)) open
  directories with `O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC`, pin created entries via `O_PATH`, set
  modes through `/proc/self/fd/<fd>` without mutating process `umask`, and verify matching `(st_dev, st_ino)`
  via `fstatat` before `unlinkat` on drop.
* **Sealed `memfd` ELF execution**: `ExecutableImage` ([src/executable.rs](src/executable.rs)) copies verified
  ELF binaries (`<= 512 MiB`) into a `memfd`, applies mandatory
  `F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL` seals (plus `F_SEAL_EXEC` when supported), drops
  the writable descriptor before `exec` (`ETXTBSY`) in favor of an `O_RDONLY | O_CLOEXEC` reader, prevents
  `Command` program replacement in `spawn`, and checks `/proc/<pid>/exe` inode/device and seals in
  `matches_process`.
* **Process resource parsing**: Keep Linux `/proc/<pid>` and cgroup v2 readers in
  [src/process_resources/linux.rs](src/process_resources/linux.rs) separated from pure parsers in
  [src/process_resources/parse.rs](src/process_resources/parse.rs).

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/linux.rs](src/linux.rs) ·
  [src/executable.rs](src/executable.rs) · [src/process_resources.rs](src/process_resources.rs) ·
  [tests/transport.rs](tests/transport.rs) · [tests/executable.rs](tests/executable.rs) ·
  [tests/process_resources.rs](tests/process_resources.rs) · [tests/platform.rs](tests/platform.rs)
* **Specifications**: [control-transport.md](../../specs/implementation/control-transport.md) ·
  [supervisor-process.md](../../specs/implementation/supervisor-process.md) ·
  [runtime-observation.md](../../specs/implementation/runtime-observation.md)

```sh
cargo test --locked -p tickwire-control-sys --all-targets
./scripts/check-linux.sh
```
