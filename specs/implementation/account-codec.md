# Account Baseline and Canonical Record Codec

`tickwire-account-codec` (`#![forbid(unsafe_code)]`) defines the deterministic, stateless CBOR serialization and structural validation rules for multi-asset account baselines (`CanonicalBaseline`), ordered state-transition records (`CanonicalRecord`), and strategy input seals (`InputSeal`). Every top-level baseline, record, and seal commits to a `SHA-256` digest over its complete canonical byte encoding for deterministic snapshot/event validation and regression testing.

The crate depends solely on `sha2`, `tickwire-risk`, `tickwire-venue`, and `tickwire-account`—with zero file-journal, blob-store, or local-disk dependencies.

## State Model and Bounds

Struct encodings use fixed-arity CBOR arrays rather than string-keyed maps; `Option<T>` encodes as `[]` or `[value]`, booleans encode as integers `0` or `1`, `Fixed` preserves its exact `[atoms, scale]` unit, and venue decimals preserve canonical `[coefficient, exponent]`. Signed integers use big-endian shortest-form CBOR down to `i128::MIN` (`-1 - magnitude`), using bignum tags `2` and `3` for 9-to-16-byte magnitudes with no leading zero bytes. Floating-point items, indefinite lengths, unknown tags, and trailing bytes are unconditionally rejected.

| Object or Collection | Canonical Envelope / Enforced Limit |
| --- | --- |
| `CanonicalBaseline` (`<= 2 MiB`) | `["tickwire.account-baseline", 1, Snapshot, Catalog]`; `<= 64` assets, `<= 64` instruments, `<= 128` positions, `<= 64` `venue_available` wallets |
| Baseline inner collections | `<= 1,024` strategy ownership/limit entries per asset, position, or instrument; `<= 128` futures risk brackets per instrument |
| `CanonicalRecord` (`<= 60 KiB`) | `["tickwire.account-record", 1, baseline_sha256, sequence, RecoveryRecord]`; non-zero `baseline_sha256` and `sequence` |
| `Admit` expected image (`Tag 1`) | `<= 3` child order keys (`MAX_ADMISSION_ORDERS`), `<= 64` unique positive resource holds, `<= 64` unique positive capital holds |
| `RecoveryRecord` tags (`1..=16`) | `1` `Admit`, `2` `VenueEvent`, `3` `Aborted`, `4` `FeesReconciled`, `5` `UnattributedCashFlow`, `6` `ExternalPosition`, `7` `TransferAsset`, `8` `TransferPosition`, `9` `IngressEvent`, `10` `Reactivated`, `11` `StrategyHandover`, `12` `IngressAccountInput`, `13` `IncomeBaseline`, `14` `AccountInputsReconciled`, `15` `IncomeObservationsAssociated`, `16` `UncreatedAlgoClosed` |

## Core Invariants

* **Deterministic baseline ordering and reducer pre-flight**: `CanonicalBaseline::new` sorts `assets` by `(wallet, asset)`, `positions` by `(instrument_id, side)`, `venue_available` by `wallet`, `Catalog` entries by `instrument_id`, and per-asset/position `ownership` plus per-instrument `strategy_limits` by logical strategy ID, rejecting any duplicate key while preserving the caller's futures bracket order for risk-tier validation. Every constructed or decoded baseline must satisfy `Account::from_snapshot` and reproduce its exact input bytes on re-encoding; vector capacities are checked prior to allocation.
* **Non-downgradable admission images (`Tag 1`)**: An `Admit` record pairs a canonical command, `evaluated_at`, and expected `Admission` image (`command_digest`, `intent`, `account_epoch`, `ledger_sequence`, `revisions`, `deadline`, `resources`, `capital_holds`, `orders`, and `increases`). The codec independently computes whether the command increases risk: only `Cancel`, `Query`, `CancelList`, `CancelAlgo`, and `AmendKeepPriority` take the non-increasing branch (`increases == false`), preventing a new order command from bypassing `risk_policy_version` checks by setting `increases = false` in its recorded image.
* **Explicit evidence bindings across lifecycle and reconciliation tags**: Ingress tags (`9` `IngressEvent` and `12` `IngressAccountInput`) bind a non-zero 16-byte `input_stream`, non-zero `input_sequence`, and a canonically validated venue payload. `Reactivated` (`Tag 10`), `StrategyHandover` (`Tag 11`), `IncomeBaseline` (`Tag 13`, `<= 4,096` unique income rows), `AccountInputsReconciled` (`Tag 14`), `IncomeObservationsAssociated` (`Tag 15`, strictly sorted `FUNDING_FEE` integer transaction IDs), and `UncreatedAlgoClosed` (`Tag 16`) require non-zero scope, baseline, history, and coverage digests.

## Operations and Failure Semantics

* **Byte-exact round-trip and tag compatibility**: Every decoder path re-encodes the parsed value (including nested canonical commands, venue events, and account inputs) and rejects any input whose re-encoded bytes differ by even one bit—eliminating non-shortest integers, unsorted baseline maps, or unnormalized decimals. Adding new tags (`9..=16`) leaves the wire encoding of earlier tags (`1..=8`) bit-for-bit unchanged while causing older decoders to fail closed on unfamiliar tag numbers.

## Verification and References

```sh
cargo test --locked -p tickwire-account-codec --all-targets
cargo clippy --locked -p tickwire-account-codec --all-targets -- -D warnings
```

Test suites cover independent byte and SHA-256 golden vectors, `i128` bignum boundaries, truncation and malformed length prefixes, baseline sort/duplicate invariants, non-increasing command classification, legacy tag byte-stability, and stateless in-memory Spot/USDⓈ-M baseline + record round-trip verification.

* Codec entry points, CBOR primitives, and validators: [lib.rs](../../crates/tickwire-account-codec/src/lib.rs), [cbor.rs](../../crates/tickwire-account-codec/src/cbor.rs), [codec.rs](../../crates/tickwire-account-codec/src/codec.rs), [validate.rs](../../crates/tickwire-account-codec/src/validate.rs)
* Test suites: [baseline.rs](../../crates/tickwire-account-codec/tests/baseline.rs), [nonincreasing.rs](../../crates/tickwire-account-codec/tests/nonincreasing.rs), [legacy_encoding.rs](../../crates/tickwire-account-codec/tests/legacy_encoding.rs), [vectors.rs](../../crates/tickwire-account-codec/tests/vectors.rs), [file_replay.rs](../../crates/tickwire-account-codec/tests/file_replay.rs)
* Related specifications: [Stateless account and OrderManager](account.md), [Conditional orders](conditional-orders.md), [Spot keep-priority amend](spot-amend.md), [USDⓈ-M account facts](usdm-account-facts.md)
