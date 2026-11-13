# Authenticated Control Sessions and Idempotent Request Admission

`tickwire-control` implements the pure protocol state machine for mutually authenticated local control sessions (`HELLO` / `WELCOME`), monotonic lease enforcement, and exactly-once request dispatch correlation (`Session::admit` and `ExecutionTicket`). Rather than trusting claims inside wire payloads alone, a `Session` binds launcher-supplied `Policy` expectations—registered instance UUIDs, generations, roles, logical strategy IDs, host boot ID, namespace UUID, architecture, trust profile, and executable/qualification/manifest SHA-256 digests—to actual kernel `ProcessIdentity` (`pid`, `uid`, `gid`) obtained from `SO_PEERCRED` and verified against the supervisor's unreaped child handle.

This crate owns control-envelope framing, capability intersection, monotonic session lease lifetimes, and request deduplication state transitions (`Execute`, `Pending`, `Completed`). Transport socket delivery and ancillary file-descriptor lifecycle belong to `tickwire-control-sys`, child process ownership belongs to `tickwire-supervisor`, and durable cross-restart persistence of request outcomes and economic operations belongs to the concrete owner's journal.

## State Model and Bounds

All sequence, generation, epoch, revision, state-version, and monotonic nanosecond values are bounded strictly below `LIMIT = 2^63`. Both peers must share the same nonzero 16-byte `host_boot` and `namespace` UUIDs and run in the same Linux `CLOCK_MONOTONIC` time namespace.

| Structure or Parameter | Constraints and Wire Binding |
| --- | --- |
| `Identity` | Nonzero 16-byte `instance`, `host_boot`, `namespace`; `generation` in `1..2^63`; `role` in `1..=9` (`role == 3` strategy requires nonzero `logical_strategy < 2^63`); nonzero 32-byte `build_hash`, `qualification_hash`, `manifest_hash`; `arch` in `1..=2`; `trust_profile` in `1..=2`. |
| `Policy` | Distinct `local` and `peer` instances; `peer_process.pid != 0`; `control_epoch` in `1..2^63`; nonzero 32-byte `nonce` (never reused across sessions); `lease_duration_ns` in `1..2^63`; `max_frame_bytes` in `320..=65,536`. |
| Feature negotiation | `KNOWN_FEATURES = 0x3ff`; baseline `REQUIRED_FEATURES = 0x1c` (bits 2, 3, and 4 mandatory for all profiles); bit 5 (`0x20`, value `32`) mandatory whenever either peer has `role == 9` (config-control). |
| Hardware / ABI geometry | ABI `1.0` (`TWCTL001`, 128-byte `ControlEnvelope`); `page_shift == 12` (4 KiB), `isolation_shift == 7` (128-byte cache-line isolation), `atomic_width == 8` (64-bit atomics); `WELCOME` fixes `max_fds == 16`, `max_consumers == 32`, and initial `lease_revision == 1`. |
| `RequestKey` & digest | Keyed by `(subject_instance, control_epoch, request_id)`. Request SHA-256 covers `opcode` (LE `u16`), `operation_id` (16B), `operation_revision` (LE `u64`), `expected_state_version` (LE `u64`), and raw `body`. |
| `MockRequestStore` | Reference in-memory test store with configurable total history (`>= 1`) and a hard cap of `256` simultaneous `Pending` (incomplete) entries. |

## Core Invariants

* **Two-sided identity and capability pinning**: `ClientHandshake::new` emits `HELLO` (opcode `1`, `flags = 1`, `control_epoch = 0`, nonzero `request_id`, zero `operation_id` / `operation_revision` / `expected_state_version`). `Session::accept` validates `Policy`, matches `actual_peer` against `policy.peer_process`, verifies every `HelloBody` identity digest and hardware shift, intersects `(required | optional) & server.supported_features`, verifies both sides' required masks are satisfied, selects `min(server.max_frame_bytes, client.max_frame_bytes)`, computes `deadline = now + lease_duration_ns < 2^63`, and emits `WELCOME` (opcode `2`, `flags = 10`) stamped with `assigned_control_epoch` and `session_nonce`. `ClientHandshake::finish` independently verifies `actual_peer`, `request_id`, server identity, epoch, nonce, features, and `0 < deadline - now <= lease_duration_ns`.
* **Irreversible closure on expiry or clock regression**: `Session` tracks `last_now` and `deadline`. On every `validate` or `renew` call, if `now < last_now` (`Error::ClockRegression`) or `now >= deadline` (`Error::Expired`), the session sets `closed = true` permanently. Because `Session::encode` serializes outbound frames without advancing local time, the receiving peer is always responsible for validating lease freshness at the moment it processes incoming bytes.
* **Safe lease renewal**: `Session::renew(nonce, revision, now)` requires a live, non-regressed session, an exact 32-byte `session_nonce` match, and a strictly increasing `revision` in `(self.revision)..2^63`. If `now + lease_duration_ns` would overflow or reach `2^63`, `renew` returns `Error::Bounds` while leaving the existing valid `deadline` and `revision` intact.
* **Single-dispatch `ExecutionTicket` discipline**: `Session::admit` validates the incoming request frame (`flags == 1`, `opcode` not `1` or `2`, nonzero `request_id`), computes the request SHA-256 digest, and queries `RequestStore::lookup` *before* checking `expected_state_version == current_version`. Consequently, an exact retry of an already admitted request returns `Admission::Pending` or `Admission::Completed(Completion)` even after the owner's `current_version` has advanced, whereas a different payload under the same `RequestKey` fails immediately with `Error::Conflict`. Only the caller for whom `insert_received` transitions the key from absent to present receives the non-cloneable, unconstructible `ExecutionTicket`.

## Operations and Failure Semantics

* **Completion correlation and idempotent persistence**: `ExecutionTicket::complete` decodes the proposed outbound response frame before writing to `RequestStore`, requiring `state_version < 2^63`, `flags` in `{10, 18}`, `fd_count == 0`, exact correlation of `request_id`, `control_epoch`, `sender_instance` (owner), `target_instance` (subject), `operation_id`, `operation_revision`, and `expected_state_version`, and a legal response opcode for the request's opcode and success/error branch (`49` / `51` `StatusBody` responses must also carry `body.state_version == state_version`). If `store.complete` encounters a transient storage failure, the caller still holds `&ExecutionTicket` and may retry with the identical `Completion`; `RequestStore::complete` accepts a byte-identical repeat and rejects any divergent completion with `Error::Conflict`.
* **Pending retries and crash recovery**: Receiving `Admission::Pending` on a duplicate request never issues a second `ExecutionTicket`; the owner must reconcile or await the in-flight execution rather than re-running side effects. If a process restarts or loses an in-memory ticket, production owners must recover terminal receipts and economic operation state from durable storage and query authoritative owner state before accepting new mutations.

## Verification and References

```sh
cargo test --locked -p tickwire-control
cargo clippy --locked -p tickwire-control --all-targets -- -D warnings
```

Unit and Linux transport integration tests exercise `HELLO`/`WELCOME` feature negotiation, kernel credential mismatches, clock regression and lease expiry closure, `RequestStore` fault injection, duplicate/conflicting request bodies across version advances, and `ExecutionTicket` response correlation.

* Session and request admission implementation: [lib.rs](../../crates/tickwire-control/src/lib.rs), [requests.rs](../../crates/tickwire-control/src/requests.rs)
* Test suites: [session.rs](../../crates/tickwire-control/tests/session.rs), [transport_linux.rs](../../crates/tickwire-control/tests/transport_linux.rs)
* Formal verification: [Control request TLA+ models](../../tla/execution_models/control_requests/PLAN.md)
* Related specifications: [Control transport](control-transport.md), [Supervisor process](supervisor-process.md), [IPC wire codec](ipc-codec.md)
