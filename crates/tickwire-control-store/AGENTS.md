# tickwire-control-store

`tickwire-control-store` (`#![forbid(unsafe_code)]`) implements `tickwire_control::RequestStore`
over `tickwire-journal` (`DurableRequests<S>`), durably persisting local worker and market-data
control admissions and terminal completions so restarts and retries return exact stored responses
without re-executing side effects. Session authentication and socket framing remain in
`tickwire-control` and `tickwire-control-sys`.

## Invariants and Change Guidelines

* **Durable-before-visible `RequestStore` contract**: `insert_received` and `complete` in
  [src/lib.rs](src/lib.rs) must append and `sync` `TWCR0001`-framed records to `Journal<S>` before
  updating in-memory `entries` or returning success, preserving terminal `Completion` records across
  reopen.
* **Digest conflict detection and response validation**: Replaying or calling `insert_received` /
  `complete` for an existing `RequestKey` (`subject`, `epoch`, `request_id`) with a different
  32-byte payload digest must return `Error::Conflict`. `validate_completion` verifies
  `ControlFrame` decoding, terminal response flags (`10` or `18`), zero `fd_count` (file-descriptor
  offers are never cached as durable responses), and `StatusBody::state_version` agreement.
* **Bounded memory and journal poisoning**: Enforce `max_requests` (`<= 1,048,576`), `max_pending`,
  `max_response_bytes` (`320..=65,536`), and `max_retained_bytes` (`<= 512 MiB`) both on live
  admission and during journal replay, and latch `poisoned` on any physical store failure so callers
  reopen cleanly.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [tests/durable.rs](tests/durable.rs)
* **Specifications and models**: [control-session.md](../../specs/implementation/control-session.md)
  · [journal.md](../../specs/implementation/journal.md) ·
  [control_requests](../../tla/execution_models/control_requests/ROADMAP.md)

```sh
cargo test --locked -p tickwire-control-store --all-targets
./scripts/check-tla.sh control_requests
```
