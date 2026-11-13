# tickwire-config-journal

`tickwire-config-journal` (`#![forbid(unsafe_code)]`) owns the durable per-slot configuration
observation log (`Journal<S>`, `OperationView`, `Event`, and `Receipt`) backed by
`tickwire-journal`. Callers record `Prepared`, `Desired`, `RuntimeObserved`, and `Effective`
transitions for one `Scope` and `writer_epoch`; file installation belongs in
`tickwire-config-materializer` and `tickwire-config-fs`, and journal receipts confer no live control
or trading authority on their own.

## Invariants and Change Guidelines

* **Causal phase chaining**: Enforce strict per-operation phase ordering in
  [src/ledger.rs](src/ledger.rs) and [src/journal.rs](src/journal.rs): `Desired` must reference the
  exact `prepared_ref` digest of `Prepared`, `RuntimeObserved` must match `desired_ref` plus
  `Prepared`'s expected instance/boot/generation tuple, and `Effective` must bind to `runtime_ref`.
  Conflicting re-submissions for an existing `operation` ID return `Error::Conflict`.
* **Canonical frame encoding and scope binding**: [src/codec.rs](src/codec.rs) and
  [src/store.rs](src/store.rs) bind every persisted record to `Config::scope` (`cluster`,
  `namespace`, `host`, `process`, `slot`, `locator_generation`) and `stream_epoch`, rejecting
  unknown fields (`#[serde(deny_unknown_fields)]`), sequence gaps, or payloads exceeding
  `max_operations` and `max_recorded_bytes` on replay.
* **Fail-closed physical errors**: Any underlying `tickwire_journal` write or sync failure poisons
  `Journal<S>` (`Error::Poisoned`) so callers reopen from durable storage rather than appending onto
  an uncertain frontier.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/journal.rs](src/journal.rs) ·
  [src/ledger.rs](src/ledger.rs) · [src/codec.rs](src/codec.rs) · [src/store.rs](src/store.rs) ·
  [tests/journal.rs](tests/journal.rs)
* **Specifications and models**:
  [config-local-journal.md](../../specs/implementation/config-local-journal.md) ·
  [config-journal-store.md](../../specs/implementation/config-journal-store.md) ·
  [config_activation](../../tla/execution_models/config_activation/ROADMAP.md)

```sh
cargo test --locked -p tickwire-config-journal --all-targets
./scripts/check-tla.sh config_activation
```
