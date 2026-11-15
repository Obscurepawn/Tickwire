# tickwire-control-sys

`tickwire-control-sys` is the audited Linux OS boundary (`#![deny(unsafe_op_in_unsafe_fn)]`) for `AF_UNIX`
`SOCK_SEQPACKET` control sockets (`ControlListener`, `PendingConnection`, `Connection`), `0o700` pinned
directories (`PrivateDirectory`), sealed ELF execution (`ExecutableImage`), thread-level CPU affinity and
core isolation (`CpuSet`, `pin_current_thread`, `current_thread_affinity`, `spawn_pinned_thread`,
`validate_cpu_isolation`, `current_tid`), network-device socket binding (`bind_socket_device`,
`open_bound_nonblocking_tcp_stream`, `connect_bound_tcp_blocking`), synchronous ORS signal masking and
waiting (`OrsSignal`, `block_ors_signals_current_thread`, `SignalWaiter`), and `/proc` + cgroup v2 process
resource inspection (`ResourceObservation`). Higher-level session handshakes, transport drivers, execution
runtimes, and supervisor state machines belong in `tickwire-control`, `tickwire-transport`,
`tickwire-supervisor`, and runtime crates while retaining `#![forbid(unsafe_code)]` with zero raw affinity,
`gettid`, `SO_BINDTODEVICE`, or `pthread_sigmask` / `sigtimedwait` calls outside this crate.

## Invariants and Change Guidelines

* **Bounded `SCM_RIGHTS` receipt, `CLOEXEC` cleanup, and `crc32` forwarding**: In [src/linux.rs](src/linux.rs), receive packets
  with `MSG_CMSG_CLOEXEC`, immediately wrap every ancillary raw descriptor in `OwnedFd` before inspecting
  truncation (`MSG_TRUNC` / `MSG_CTRUNC`) or frame headers so rejected packets never leak descriptors, and
  enforce `MAX_FRAME_BYTES` (`65,536`), `MAX_FDS` (`16`), exact `ControlEnvelope::fd_count` parity, and
  `SO_PEERCRED` (`PeerCredentials`). Optional `--features crc32` (`default = []`) forwards `tickwire-ipc/crc32`
  for `ControlFrame` checksum verification.
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
* **Audited thread CPU affinity, read-back verification, and core isolation**: In [src/affinity.rs](src/affinity.rs),
  represent CPU sets (`CpuSet`) over a 4,096-bit (512-byte) stack `Mask` (`0..4096`) shared with `pre_exec`
  and `/proc` observation. `validate_cpu_isolation` enforces `< 4096`, pairwise distinct hot-thread cores,
  and `hot_cpus ∩ housekeeping_cpus == ∅` at cold startup. `pin_current_thread` calls `sched_setaffinity(0, ...)`
  and immediately calls `sched_getaffinity(0, ...)` to verify exact equality with `cpus`, failing immediately
  if the kernel masked off offline cores or narrowed via cgroup `cpuset.cpus.effective`. `spawn_pinned_thread`
  completes a synchronous startup handshake before running `f()`, dropping `f` without executing it and
  returning `Err` synchronously if pinning fails.
* **Audited socket device binding and synchronous ORS signal delivery**: In [src/ors_os.rs](src/ors_os.rs),
  `bind_socket_device` validates `1..=15` printable ASCII interface bytes (`< libc::IFNAMSIZ`, no interior NUL
  or whitespace) before calling `setsockopt(SOL_SOCKET, SO_BINDTODEVICE)`. `open_bound_nonblocking_tcp_stream`
  and `connect_bound_tcp_blocking` apply `interface` (`SO_BINDTODEVICE`) with strict precedence over `local_ip`
  when both are provided. `block_ors_signals_current_thread` masks `SIGINT`, `SIGTERM`, `SIGUSR1`, and
  `SIGUSR2` via `pthread_sigmask(SIG_BLOCK, ...)` before ORS hot/housekeeping threads spawn, and
  `SignalWaiter::wait_timeout` polls them via `sigtimedwait` (mapping `EAGAIN`/`EINTR` to `Ok(None)`).
* **Process resource parsing**: Keep Linux `/proc/<pid>` and cgroup v2 readers in
  [src/process_resources/linux.rs](src/process_resources/linux.rs) separated from pure parsers in
  [src/process_resources/parse.rs](src/process_resources/parse.rs).

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/affinity.rs](src/affinity.rs) · [src/ors_os.rs](src/ors_os.rs) ·
  [src/linux.rs](src/linux.rs) · [src/executable.rs](src/executable.rs) · [src/process_resources.rs](src/process_resources.rs) ·
  [tests/affinity.rs](tests/affinity.rs) · [tests/ors_os.rs](tests/ors_os.rs) ·
  [tests/ors_signal_main.rs](tests/ors_signal_main.rs) · [tests/transport.rs](tests/transport.rs) ·
  [tests/executable.rs](tests/executable.rs) · [tests/process_resources.rs](tests/process_resources.rs) ·
  [tests/platform.rs](tests/platform.rs)
* **Specifications**: [control-transport.md](../../specs/implementation/control-transport.md) ·
  [supervisor-process.md](../../specs/implementation/supervisor-process.md) ·
  [runtime-observation.md](../../specs/implementation/runtime-observation.md)

```sh
cargo test --locked -p tickwire-control-sys --all-targets
./scripts/check-linux.sh
```
