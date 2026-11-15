# tickwire-venue

`tickwire-venue` (`#![forbid(unsafe_code)]`) defines the exchange-neutral stateless ORS domain model and deterministic
CBOR wire identity for trading commands (`CanonicalCommand`), execution and lifecycle observations (`CanonicalEvent`),
stateless ORS execution reports (`FrontAck`, `Fill` with post-trade `amendment` support), multi-product routing
(`Product`, `OrderRoute`, `pick_order_route`, `AssetKey`), synchronous `RequestReply` outcomes, and account-level
cashflow inputs (`CanonicalAccountInput`). Venue adapters normalize native protocol payloads into these bounded types
using exact integer/fixed-point arithmetic (`Decimal`, zero floating point), without local WALs or recovery-closure
files; signing, HTTP/WS framing, and live balance/margin authority live outside this crate.

## Invariants and Change Guidelines

* **Structural validation without authority policy**: Validate prices, quantities, time-in-force combinations (including
  `TimeInForce::Rpi`), multi-product rules (`Spot`, `UsdM`, `CoinM`, `Options`, `LinearPerp`, `InversePerp`,
  `LinearFutures`, `InverseFutures`), derivative reduce-only/position-side rules, and decimal bounds in
  [src/validate.rs](src/validate.rs) and [src/types.rs](src/types.rs) during `CanonicalCommand::new`,
  `CanonicalEvent::new`, `FrontAck::validate`, and `Fill::validate`, enforcing `MAX_CANONICAL_BYTES` (16 KiB).
* **Byte-exact CBOR round-trip**: In [src/cbor.rs](src/cbor.rs) and [src/codec.rs](src/codec.rs), `decode` must re-run
  `Self::new` and reject non-canonical encodings (`Error::NonCanonical`) or trailing bytes so SHA-256 digests (`Hash`)
  uniquely identify semantic content.
* **Body-specific event schema versions and unified ORS `TerminalReason`s**: `event_version` in [src/lib.rs](src/lib.rs)
  pins each `VenueEventBody` capability (`1` base, `2` `Structure`, `3` `Amendment`/`AmendmentRejected`, `4` `Fill`
  with `OrderStatus::Unreported`, `5` `Modified`/`ModifyRejected` tags `8`/`9`). [src/reasons.rs](src/reasons.rs)
  defines compact `TerminalReason` / `RefusalReason` codes (`0..=30`, including `NO_REASON` vs. `MKT_CANCELED`,
  `MKT_IOC_CANCELED`, `MKT_REJECT_POST_ONLY`, `MKT_REDUCE_ONLY_CANCELED`, `MKT_CANCELED_SELFTRADE`, `MKT_DELAYED`,
  `REJECT_STALE_ORDER`, `ALLOCATION_ERROR`, `UNEXPECTED_MSG_SEQNUM`, `INVALID_STRAT_ID`, `INVALID_ACCOUNT_ID`,
  `REJECT_ROUTING_ID_MISMATCH`, `ADMIN_ACTION`, `RESTRICTED_CANCEL_ONLY`, `MKT_REJECT_MARKET_DOWN`,
  `RFQ_QUOTE_NOT_SUPPORTED`, and `INTERNAL_CANCEL`) plus zero-allocation classifiers (`classify_binance_reason`,
  `classify_ors_cancel_reason`, and `classify_spot_venue_reason`).
* **Distinct lifecycle, trade, amendment-fill, and account-input facts**: Keep gateway `FrontAck` reports, order-status
  acknowledgements, per-trade `Fill` / `FillEvent` executions (including `Fill { amendment: true }` post-trade fee/rebate
  updates), synchronous `RequestReply` snapshots ([src/request_reply.rs](src/request_reply.rs)), and non-order cashflow
  observations ([src/account.rs](src/account.rs)) separate rather than synthesizing implied fills.
* **Self-describing client order IDs (`tw1...`)**: [src/client_id.rs](src/client_id.rs) (`SelfDescribingClientId`)
  encodes `tw1<29 base36 chars>` (32 `[0-9a-z]` ASCII bytes, valid across Binance/Bybit/OKX) carrying
  `(logical_strategy_id, leg, epoch, intent_id)` (plus `from_ors_order` / `ors_strat_id` for ORS `strat_id` `0..=254`)
  and a 30-bit domain-keyed SHA-256 MAC over `ClientIdDomain` so cold stateless exchange bootstraps deterministically
  classify `DecodedClientId::Managed` vs. `DecodedClientId::External`.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/types.rs](src/types.rs) · [src/reasons.rs](src/reasons.rs) ·
  [src/client_id.rs](src/client_id.rs) · [src/account.rs](src/account.rs) ·
  [src/request_reply.rs](src/request_reply.rs) · [src/validate.rs](src/validate.rs)
* **Integration tests**: [tests/commands.rs](tests/commands.rs) · [tests/events.rs](tests/events.rs) ·
  [tests/modify_and_reasons.rs](tests/modify_and_reasons.rs) · [tests/client_id.rs](tests/client_id.rs) ·
  [tests/account_input.rs](tests/account_input.rs) · [tests/amendment.rs](tests/amendment.rs) ·
  [tests/spot_extensions.rs](tests/spot_extensions.rs) · [tests/structures.rs](tests/structures.rs) ·
  [tests/strict.rs](tests/strict.rs) · [tests/contract.rs](tests/contract.rs) · [tests/vectors.rs](tests/vectors.rs)
* **Specifications**: [venue.md](../../specs/implementation/venue.md) ·
  [usdm-account-facts.md](../../specs/implementation/usdm-account-facts.md)

```sh
cargo test --locked -p tickwire-venue --all-targets
```
