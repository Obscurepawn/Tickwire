# tickwire-control

`tickwire-control` (`#![forbid(unsafe_code)]`) owns the pure protocol state machine for
authenticated local control sessions (`ClientHandshake`, `Session`, `Policy`, `Identity`,
`ProcessIdentity`) and idempotent request registration (`RequestStore`, `Admission`,
`ExecutionTicket` in [src/requests.rs](src/requests.rs)). Unix `SEQPACKET` sockets and `SCM_RIGHTS`
transfers live in `tickwire-control-sys`, and durable `tickwire-journal` request persistence lives
in `tickwire-control-store`.

## Invariants and Change Guidelines

* **Mutual handshake and peer binding**: `Policy` ([src/lib.rs](src/lib.rs)) requires distinct
  `local` and `peer` instances sharing the same `host_boot` and `namespace`, non-zero 32-byte
  `nonce`, kernel-verified `ProcessIdentity` (`pid`, `uid`, `gid`), mandatory feature bits
  (`REQUIRED_FEATURES = 0x1c`), and strict `HELLO` (`control_epoch == 0`) -> `WELCOME`
  challenge-response transition before issuing a live `Session`.
* **Monotonic session leases, sequence ordering, and `crc32` feature gate**: `Session::validate` and `Session::encode`
  enforce monotonic `now` timestamps (`Error::ClockRegression`), session lease expiry
  (`Error::Expired`), `max_frame_bytes` caps, bounded `< (1 << 63)` sequence numbers, and optional Castagnoli
  `crc32c` frame integrity (`--features crc32`; default OFF writes zero `header_crc32c` / `body_crc32c` fields on
  authenticated local Unix `SEQPACKET` sockets).
* **Single-mint `ExecutionTicket` idempotency**: Request admission in
  [src/requests.rs](src/requests.rs) hashes normalized request frames by `RequestKey` (`subject`,
  `epoch`, `request_id`). Identical retries return `Admission::Pending` or
  `Admission::Completed(Completion)`, payload mismatches return `Error::Conflict`, and only the
  first `insert_received` mints a non-cloneable `ExecutionTicket` whose `complete` checks response
  opcode, flags, and `state_version` parity before persisting.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/requests.rs](src/requests.rs) ·
  [tests/session.rs](tests/session.rs) · [tests/transport_linux.rs](tests/transport_linux.rs)
* **Specifications and models**: [control-session.md](../../specs/implementation/control-session.md)
  · [control-transport.md](../../specs/implementation/control-transport.md) ·
  [control_requests](../../tla/execution_models/control_requests/ROADMAP.md)

```sh
cargo test --locked -p tickwire-control --all-targets
./scripts/check-linux.sh
./scripts/check-tla.sh control_requests
```
