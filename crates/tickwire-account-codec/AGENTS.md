# tickwire-account-codec

`tickwire-account-codec` (`#![forbid(unsafe_code)]`) owns deterministic CBOR serialization, structural validation, and
SHA-256 content addressing for stateless in-memory account baselines and transition records: `CanonicalBaseline`,
`CanonicalRecord`, and `InputSeal`. In-memory snapshot validators, audit harnesses, and replay tests call this crate to
turn `tickwire-account` `Snapshot` / `Catalog` baselines and `RecoveryRecord` transitions into byte-exact immutable
objects and verify decoded byte slices strictly in memory with zero local disk or journal dependencies.

## Invariants and Change Guidelines

* **Canonical CBOR re-encode equality**: Every `decode` path in [src/lib.rs](src/lib.rs) and
  [src/cbor.rs](src/cbor.rs) must reconstruct the value via `Self::new` and reject any input whose re-encoded bytes
  differ (`Error::NonCanonical`) or leave trailing bytes (`Error::TrailingData`).
* **Bounded payload sizes and structural limits**: Preserve `MAX_BASELINE_BYTES` (2 MiB) and `MAX_RECORD_BYTES`
  (60 KiB), along with sorted, duplicate-free map/list invariants enforced in [src/validate.rs](src/validate.rs) and
  [src/codec.rs](src/codec.rs).
* **Baseline binding and contiguous seals**: Reject zero digests or sequence zero; ensure income and reconciliation
  records carry the exact `baseline_digest` of their `CanonicalRecord`, and keep `InputSeal::apply` strictly
  contiguous (`position + 1 == record.sequence()`) so sealed report digests never depend on volatile receive cursors.
* **Golden byte compatibility**: Treat `tickwire.account-baseline` and `tickwire.account-record` domain tags, field
  order, and schema version `1` as compatibility boundaries (embedding `tickwire-venue` commands and events including
  native `Modify` and schema-5 `Modified`/`ModifyRejected` records without changing outer `CanonicalRecord` framing);
  accompany any new `RecoveryRecord` variant with hex/SHA-256 fixtures and single-byte mutation tests.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/cbor.rs](src/cbor.rs) · [src/codec.rs](src/codec.rs) ·
  [src/validate.rs](src/validate.rs)
* **Integration tests**: [tests/baseline.rs](tests/baseline.rs) · [tests/records.rs](tests/records.rs) ·
  [tests/vectors.rs](tests/vectors.rs) · [tests/file_replay.rs](tests/file_replay.rs) ·
  [tests/ingress.rs](tests/ingress.rs) · [tests/amendment.rs](tests/amendment.rs) ·
  [tests/account_inputs.rs](tests/account_inputs.rs) · [tests/input_seal.rs](tests/input_seal.rs) ·
  [tests/nonincreasing.rs](tests/nonincreasing.rs) · [tests/legacy_encoding.rs](tests/legacy_encoding.rs)
* **Specifications**: [account-codec.md](../../specs/implementation/account-codec.md)

```sh
cargo test --locked -p tickwire-account-codec --all-targets
```
