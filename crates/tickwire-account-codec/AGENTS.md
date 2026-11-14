# tickwire-account-codec

`tickwire-account-codec` (`#![forbid(unsafe_code)]`) owns deterministic CBOR serialization, validation, and SHA-256
content addressing for account recovery artifacts: `CanonicalBaseline`, `CanonicalRecord`, and `InputSeal`. Journal
writers, replay verifiers, and strategy hosts call this crate to turn `tickwire-account` snapshots and
`RecoveryRecord` transitions into byte-exact immutable objects and to verify decoded bytes on restore. File locking,
WAL framing, and authority policy belong to `tickwire-account-journal` and `tickwire-account-gateway`.

## Invariants and Change Guidelines

* **Canonical CBOR re-encode equality**: Every `decode` path in [src/lib.rs](src/lib.rs) and
  [src/cbor.rs](src/cbor.rs) must reconstruct the value via `Self::new` and reject any input whose re-encoded bytes
  differ (`Error::NonCanonical`) or leave trailing bytes (`Error::TrailingData`).
* **Bounded WAL fit and structural limits**: Preserve `MAX_BASELINE_BYTES` (2 MiB) and `MAX_RECORD_BYTES` (60 KiB,
  sized so a `Begin`/`Data`/`Seal` batch fits inside a 64 KiB physical WAL frame), along with sorted, duplicate-free
  map/list invariants enforced in [src/validate.rs](src/validate.rs) and [src/codec.rs](src/codec.rs).
* **Baseline binding and contiguous seals**: Reject zero digests or sequence zero; ensure income and reconciliation
  records carry the exact `baseline_digest` of their `CanonicalRecord`, and keep `InputSeal::apply` strictly
  contiguous (`position + 1 == record.sequence()`) so strategy report digests never depend on volatile receive
  cursors.
* **Golden byte compatibility**: Treat `tickwire.account-baseline` and `tickwire.account-record` domain tags, field
  order, and schema version `1` as durable compatibility boundaries (embedding `tickwire-venue` commands and events
  including native `Modify` and schema-5 `Modified`/`ModifyRejected` records without changing outer `CanonicalRecord`
  framing); accompany any new `RecoveryRecord` variant with hex/SHA-256 fixtures and single-byte mutation tests.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/cbor.rs](src/cbor.rs) · [src/codec.rs](src/codec.rs) ·
  [src/validate.rs](src/validate.rs)
* **Integration tests**: [tests/baseline.rs](tests/baseline.rs) · [tests/records.rs](tests/records.rs) ·
  [tests/vectors.rs](tests/vectors.rs) · [tests/file_replay.rs](tests/file_replay.rs) ·
  [tests/ingress.rs](tests/ingress.rs) · [tests/amendment.rs](tests/amendment.rs) ·
  [tests/account_inputs.rs](tests/account_inputs.rs) · [tests/input_seal.rs](tests/input_seal.rs) ·
  [tests/nonincreasing.rs](tests/nonincreasing.rs) · [tests/legacy_encoding.rs](tests/legacy_encoding.rs)
* **Specifications and contracts**: [account-codec.md](../../specs/implementation/account-codec.md) ·
  [journal-wire-v1.json](../../specs/contracts/journal-wire-v1.json)

```sh
cargo test --locked -p tickwire-account-codec --all-targets
python3 scripts/verify-design-contracts.py
```
