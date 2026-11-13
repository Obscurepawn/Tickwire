# tickwire-journal-codec

`tickwire-journal-codec` (`#![forbid(unsafe_code)]`) owns deterministic schema-2 CBOR encoding, decoding,
business-payload hashing (`business_digest`), and hash-chained `LogicalChain` verification for `Record` and `Event`
values (`PreparedIntent`, `Fill`, `Fee`, `Terminal`, `AbortedBeforeSend`). `tickwire-business-journal` and
`tickwire-gateway` call this crate to serialize WAL payloads and derive content-addressed dependency preimages. It
performs zero I/O, holds no durability receipts, and evaluates no risk or authorization policy.

## Invariants and Change Guidelines

* **Bounded canonical CBOR**: Keep `SCHEMA_VERSION` (`2`) and `MAX_ENCODED_EVENT_BYTES` (`236`) enforced in
  [src/lib.rs](src/lib.rs) and [src/cbor.rs](src/cbor.rs); `Reader` must require minimal integer widths, exact map key
  order (`0..=6`), and zero trailing bytes (`DecodeError::NonCanonical`, `DecodeError::TrailingData`).
* **Placement-independent `business_digest`**: `business_payload` and `business_digest` in [src/lib.rs](src/lib.rs)
  hash the `"tickwire.business-event"` domain tag, `risk_domain_id`, and the semantic `Event` (including original
  `intent_exec_epoch`), deliberately excluding `writer_exec_epoch`, `journal_seq`, and physical WAL placement so
  retries across writer epochs preserve command identity.
* **Monotonic logical chain**: `LogicalChain::append` in [src/chain.rs](src/chain.rs) enforces matching
  `risk_domain_id` and `journal_epoch`, contiguous 1-based `journal_seq` without overflow, and non-decreasing
  `writer_exec_epoch` (`ChainError::ExecutionEpochRollback`).
* **Golden vector lockstep**: Any change to tags, integer ranges, or `Event` fields must update or version alongside
  [golden-v2.json](tests/golden-v2.json) and [golden.json](tests/golden.json) and verify boundary sizes in
  [tests/codec.rs](tests/codec.rs).

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/cbor.rs](src/cbor.rs) · [src/chain.rs](src/chain.rs)
* **Integration tests and fixtures**: [tests/codec.rs](tests/codec.rs) · [golden-v2.json](tests/golden-v2.json) ·
  [golden.json](tests/golden.json)
* **Specifications, contracts, and models**: [journal-codec.md](../../specs/implementation/journal-codec.md) ·
  [command-binding.md](../../specs/implementation/command-binding.md) ·
  [journal-wire-v1.json](../../specs/contracts/journal-wire-v1.json) ·
  [business_journal](../../tla/execution_models/business_journal/ROADMAP.md)

```sh
cargo test --locked -p tickwire-journal-codec --all-targets
python3 scripts/verify-design-contracts.py
```
