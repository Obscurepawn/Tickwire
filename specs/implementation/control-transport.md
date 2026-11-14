# Linux Control Sockets, Ancillary Descriptor Transfer, and Private Directories

`tickwire-control-sys` provides the low-level Linux transport for Tickwire's control plane over `AF_UNIX` `SOCK_SEQPACKET` sockets, coupling atomic message boundaries with bounded `SCM_RIGHTS` file-descriptor passing, kernel `SO_PEERCRED` extraction, and symlink-safe private socket directories. Every socket and received ancillary descriptor is created or accepted with `CLOEXEC` and `NONBLOCK` semantics atomically in the kernel so concurrent `fork`/`exec` operations in a supervisor cannot leak control channels or shared-memory handles into unrelated child processes.

This crate isolates the `libc` `unsafe` boundary required for `sendmsg`/`recvmsg` control-message (`cmsghdr`) buffers, `SO_PEERCRED` / `SO_ERROR` socket options, `openat`/`unlinkat` inode pinning, and `CLOCK_MONOTONIC` nanosecond reads. Higher layers (`tickwire-control`, `tickwire-supervisor`, `tickwire-join`) own session handshakes, `memfd` seal verification, and protocol state machines. Non-Linux builds compile safe stubs that return `Error::UnsupportedPlatform`.

## State Model and Bounds

`SOCK_SEQPACKET` preserves exact datagram boundaries while providing connection-oriented peer credentials and EOF delivery. All socket pathnames must be absolute, non-empty, free of interior NUL bytes, and strictly shorter than `108` bytes (`sockaddr_un.sun_path`).

| Component or Constant | Bound / Kernel Configuration |
| --- | --- |
| `MAX_FRAME_BYTES` | `65,536` bytes per `SEQPACKET` frame (`ControlFrame` wire envelope + body) |
| `MAX_FDS` | `16` file descriptors per frame (`AncillaryBuffer` sized via `CMSG_SPACE(16 * sizeof(RawFd))` over `usize`-aligned words) |
| `ControlListener` | Bound via `/proc/self/fd/<parent_fd>/<name>`, `chmod` `0600` (default) or `0660`, listen backlog `64`, `accept4(..., SOCK_CLOEXEC \| SOCK_NONBLOCK)` |
| `PrivateDirectory` | `prefix` of `1..=32` ASCII `[A-Za-z0-9_-]` bytes + 128-bit hex suffix from `getrandom` (up to 16 collision retries); `mkdirat(..., 0700)` inside a canonicalized parent owned by `geteuid()` or `root` (`0`) that either forbids group/other write (`mode & 0o022 == 0`) or sets sticky bit `S_ISVTX` |
| `PeerCredentials` | Sampled via `getsockopt(SOL_SOCKET, SO_PEERCRED)` (`pid > 0`, `uid`, `gid`) immediately upon connection acceptance or completion |
| `monotonic_ns()` | `clock_gettime(CLOCK_MONOTONIC)` converted with checked `u64` nanosecond arithmetic; cross-process comparisons require identical host boot and Linux time namespace |

## Core Invariants

* **Zero-leak ancillary descriptor ownership before validation**: When `recvmsg(..., MSG_DONTWAIT | MSG_CMSG_CLOEXEC)` returns `n >= 0`, the kernel has already installed any delivered `SCM_RIGHTS` descriptors into the calling process's FD table (and automatically closed any excess descriptors dropped under `MSG_CTRUNC`). `Connection::recv_once` therefore calls `AncillaryBuffer::take_fds` *before* checking `MSG_TRUNC | MSG_CTRUNC`, `n == 0` (EOF), unknown ancillary headers, `ControlFrame::decode` (including optional `--features crc32` Castagnoli `header_crc32c` / `body_crc32c` verification; default OFF leaves local `AF_UNIX` `SEQPACKET` frame checksum fields zeroed in `tickwire-control` and skips CRC recomputation), or `envelope.fd_count == fds.len()`. Even if an unrecognized `cmsghdr` or malformed payload invalidates the packet, `take_fds` continues scanning subsequent headers to wrap every delivered `RawFd >= 0` in an `OwnedFd`, guaranteeing deterministic close on error drop.
* **Atomic datagram send without partial-suffix retry**: `Connection::try_send` and `send` decode the outgoing `ControlFrame` and verify `fds.len() == envelope.fd_count <= 16` before invoking `sendmsg(..., MSG_DONTWAIT | MSG_NOSIGNAL)`. The sender passes `BorrowedFd` slices and retains ownership of its own descriptors. A return value `n >= 0` equal to `bytes.len()` means the complete frame and duplicated descriptor references reached the kernel; any `0 <= n < bytes.len()` returns `Error::Truncated` and is never retried as a byte suffix.
* **TOCTOU-resistant socket binding and cleanup (`OwnedEntry`)**: `ControlListener::bind_with_mode` opens the parent directory with `O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC`, verifies `st_uid == geteuid()` and `st_mode & 0o022 == 0`, binds via `/proc/self/fd/<parent_fd>/<name>`, pins the newly created socket entry with `openat(..., O_PATH | O_NOFOLLOW | O_CLOEXEC)`, and applies permissions via `/proc/self/fd/<pin_fd>` without mutating the process-wide `umask`. On `Drop`, `OwnedEntry` compares `fstat(pin)` (`st_dev`, `st_ino`) against `fstatat(parent, name, AT_SYMLINK_NOFOLLOW)` before calling `unlinkat` (using `AT_REMOVEDIR` for directories, never recursive deletion). Because Linux has no atomic compare-and-unlink syscall, the trusted launcher serializes same-UID namespace edits in that directory during creation and cleanup.
* **Live child credential binding vs. inherited socketpairs**: An `exec`'d worker connects by pathname (`PendingConnection` or `Connection::connect`) so the listener's `SO_PEERCRED` reflects the child's post-`exec` PID, UID, and GID rather than a parent-created `socketpair` (which records the creator's PID at creation time). `PendingConnection`withholds the `Connection` object until handshake readiness succeeds and `SO_PEERCRED` has been read.

## Operations and Failure Semantics

* **Deadline-driven vs. event-loop (`try_*`) semantics**: Blocking-style entry points (`ControlListener::accept`, `Connection::connect`, `Connection::recv`, `Connection::send`) share an absolute `std::time::Instant` deadline: `EINTR` loops against the unchanged deadline, and `WouldBlock` waits in `poll` for the remaining milliseconds. Immediate event-loop entry points (`try_accept`, `try_recv`, `try_send`) issue at most one syscall without reading the clock or sleeping: `try_accept` and `try_recv` return `Ok(None)` on `WouldBlock`, `try_send` returns `Ok(false)` when the kernel socket buffer is full, and `EINTR` surfaces directly as `Error::System` for the caller's loop to schedule. Note that calling `recv` with a near-zero deadline differs from `try_recv` because buffer allocation and `remaining_ms(deadline)` run before `recvmsg`. Queued `SEQPACKET` frames always drain completely before `recvmsg` reports `0` (`Error::Closed`).
* **Nonblocking `AF_UNIX` backlog vs. async connect progress**: On Linux `AF_UNIX` sockets, a full listener backlog causes `connect(2)` to return `EAGAIN` *without* enqueueing a connection request, whereas `EINPROGRESS` / `EALREADY` indicates an in-flight attempt. `PendingConnection::poll` handles `EAGAIN` by keeping `connecting = false` so the next `poll()` re-issues `connect(2)` (while blocking `Connection::connect` sleeps up to 5 ms via zero-FD `poll` before retrying), and handles `EINPROGRESS` / `EALREADY` by setting `connecting = true` so subsequent polls probe zero-timeout `POLLOUT` readiness and `getsockopt(SOL_SOCKET, SO_ERROR)`. Any non-`EINTR` fatal error drops `PendingConnection`'s owned socket descriptor immediately so subsequent polls fail closed with `Error::Closed`.
* **Audited `unsafe` obligations**: Every `unsafe` block in `linux.rs` upholds `#![deny(unsafe_op_in_unsafe_fn)]`: `AncillaryBuffer` stores `Vec<usize>` to satisfy `cmsghdr` alignment while reading headers and `RawFd` payloads via `read_unaligned` / `write_unaligned` inside checked byte bounds; `OwnedFd::from_raw_fd` wraps each fresh descriptor exactly once; and stack `stat`, `ucred`, `sockaddr_un`, `msghdr`, `iovec`, and `pollfd` structs remain live for the duration of synchronous `libc` calls.

## Verification and References

```sh
cargo test --locked -p tickwire-control-sys
cargo clippy --locked -p tickwire-control-sys --all-targets -- -D warnings
```

The Linux transport suite exercises 0-, 1-, and 16-descriptor `SCM_RIGHTS` transfers, full listener backlogs (`EAGAIN` retry), `MSG_TRUNC`/`MSG_CTRUNC` and malformed ancillary frames (verifying zero leaked descriptors via `/proc/self/fd`), `SO_PEERCRED` peer identity, private directory permission checks, and FIFO drain before EOF.

* Transport implementation: [lib.rs](../../crates/tickwire-control-sys/src/lib.rs), [linux.rs](../../crates/tickwire-control-sys/src/linux.rs), [unsupported.rs](../../crates/tickwire-control-sys/src/unsupported.rs)
* Transport tests: [transport.rs](../../crates/tickwire-control-sys/tests/transport.rs), [platform.rs](../../crates/tickwire-control-sys/tests/platform.rs)
* Related specifications: [Control sessions](control-session.md), [Supervisor process](supervisor-process.md), [IPC wire codec](ipc-codec.md), [Single-writer Linux mappings](shm-mapping.md)
* External kernel references: [unix(7)](https://man7.org/linux/man-pages/man7/unix.7.html), [connect(2)](https://man7.org/linux/man-pages/man2/connect.2.html)
