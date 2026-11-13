# tickwire-venue

`tickwire-venue` (`#![forbid(unsafe_code)]`) defines the exchange-neutral domain model and deterministic CBOR wire
identity for trading commands (`CanonicalCommand`), execution and lifecycle observations (`CanonicalEvent`),
synchronous `RequestReply` outcomes, and account-level cashflow inputs. Venue adapters normalize native protocol
payloads into these bounded types, and `tickwire-account` consumes them. Stable venue catalog IDs (`1` Binance, `2`
Bybit, `3` OKX) anchor routing across market data, journals, and accounts; signing, HTTP/WS framing, and trading
authority live outside this crate.

## Invariants and Change Guidelines

* **Structural validation without authority policy**: Validate prices, quantities, time-in-force combinations,
  derivative reduce-only/position-side rules, and decimal bounds in [src/validate.rs](src/validate.rs) and
  [src/types.rs](src/types.rs) during `CanonicalCommand::new` and `CanonicalEvent::new`, enforcing
  `MAX_CANONICAL_BYTES` (16 KiB).
* **Byte-exact CBOR round-trip**: In [src/cbor.rs](src/cbor.rs) and [src/codec.rs](src/codec.rs), `decode` must re-run
  `Self::new` and reject non-canonical encodings (`Error::NonCanonical`) or trailing bytes so SHA-256 digests (`Hash`)
  uniquely identify semantic content.
* **Body-specific event schema versions**: `event_version` in [src/lib.rs](src/lib.rs) pins each new `VenueEventBody`
  capability (`1` base, `2` `Structure`, `3` `Amendment`/`AmendmentRejected`, `4` `Fill` with
  `OrderStatus::Unreported`); any new semantic variant requires a bumped version, older-version rejection test,
  hex/SHA-256 vector in `tests/data/`, and matching update in
  [journal-wire-v1.json](../../specs/contracts/journal-wire-v1.json).
* **Distinct lifecycle, trade, and account-input facts**: Keep order acknowledgements/statuses, per-trade fills,
  synchronous `RequestReply` snapshots ([src/request_reply.rs](src/request_reply.rs)), and non-order cashflow
  observations ([src/account.rs](src/account.rs)) separate rather than synthesizing implied fills.
* **Self-describing client order IDs**: [src/client_id.rs](src/client_id.rs) (`SelfDescribingClientId`) encodes
  `tw1<29 base36 chars>` (32 `[0-9a-z]` ASCII bytes, valid across Binance/Bybit/OKX) carrying
  `(logical_strategy_id, leg, epoch, intent_id)` plus a 30-bit domain-keyed SHA-256 MAC over `ClientIdDomain` so cold
  exchange bootstraps deterministically classify `DecodedClientId::Managed` vs. `DecodedClientId::External`.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/types.rs](src/types.rs) · [src/client_id.rs](src/client_id.rs) ·
  [src/account.rs](src/account.rs) · [src/request_reply.rs](src/request_reply.rs) · [src/validate.rs](src/validate.rs)
* **Integration tests**: [tests/commands.rs](tests/commands.rs) · [tests/events.rs](tests/events.rs) ·
  [tests/client_id.rs](tests/client_id.rs) · [tests/account_input.rs](tests/account_input.rs) ·
  [tests/amendment.rs](tests/amendment.rs) ·
  [tests/spot_extensions.rs](tests/spot_extensions.rs) · [tests/structures.rs](tests/structures.rs) ·
  [tests/strict.rs](tests/strict.rs) · [tests/contract.rs](tests/contract.rs) · [tests/vectors.rs](tests/vectors.rs)
* **Specifications and contracts**: [venue.md](../../specs/implementation/venue.md) ·
  [usdm-account-facts.md](../../specs/implementation/usdm-account-facts.md) ·
  [journal-wire-v1.json](../../specs/contracts/journal-wire-v1.json)

```sh
cargo test --locked -p tickwire-venue --all-targets
python3 scripts/verify-design-contracts.py
```
