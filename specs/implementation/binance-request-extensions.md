# Binance Advanced Spot Order Extensions

Canonical command schema `1` preserves command tags `1..=10` byte-for-byte (including `LIMIT + PostOnly` with `iceberg_quantity` on ordinary spot orders) and introduces tag `11` (`SpotAdvanced`) to represent pegged spot orders, pegged cancel-replace requests, multi-leg spot lists (`OCO`, `OTO`, `OTOCO`) containing pegged legs, and Smart Order Routing (`SOR`) submissions.

The entire canonical CBOR encoding of tag `11` feeds `CanonicalCommand::digest()` (SHA-256). Because older decoders reject tag `11` as unknown, producers must emit `SpotAdvanced` commands only to downstream gateways and adapters that explicitly advertise the capability. `tickwire-venue` owns canonical CBOR validation, while `tickwire-binance` owns REST and WebSocket wire parameter encoding and signing ([binance-protocol.md](binance-protocol.md)); multi-leg account reservations, symbol filter eligibility, and order-list state machines remain owned by the account and risk layers (which reject unsupported advanced commands fail-closed).

## Canonical Schema (`Tag 11`) and Bounds

Tag `11` encodes a 2-element canonical CBOR array `[11, [subtype, ...]]` over four subtypes, rejecting non-minimal integers, wrong array lengths, unknown enum discriminants, truncated payloads, or trailing bytes:

| Subtype | Discriminant & Canonical Layout | Structural Requirements |
| --- | --- | --- |
| `New` | `[1, SpotOrder]` | `SpotOrder` **must** carry `Some(Peg)`. |
| `CancelReplace` | `[2, OrderTarget, SpotOrder, CancelReplaceMode]` | `SpotOrder` **must** carry `Some(Peg)`. |
| `List` | `[3, SpotAdvancedList]` | `[list_id, client_id, ListKind, BoundedList<SpotOrder, 3>]`; nonzero `list_id`, valid leg count for `ListKind`, distinct list and leg `client_id`s, and **at least one** leg with `Some(Peg)`. |
| `SorNew` | `[4, OrderSpec]` | Accepts standard spot `LIMIT` or `MARKET` orders specified in base-asset quantity (no quote-order-qty, no peg). |

Here `SpotOrder = [OrderSpec, Option<Peg>]` and `Peg = [PegPriceType, Option<u16>]`, where `PegPriceType` is `1` (`Primary`, tracking same-side best price) or `2` (`Market`, tracking opposite-side best price), and `Option<u16>` encodes an optional level offset in `0..=100` (when present, wire output emits both `pegOffsetValue` and `pegOffsetType = PRICE_LEVEL`; offset `0` is distinct from `None`).

## Core Invariants

* **Orthogonal price fields and peg compatibility**: A pegged `SpotOrder` may omit its explicit `OrderSpec` limit price, while explicit limit price, stop/trigger price, and trailing delta retain separate semantics and never alias one another. Validation rejects pegged `MARKET` orders, `Primary` pegs on ordinary non-`GTC` `LIMIT` orders, and `Market` pegs on maker-only (`PostOnly` / `LIMIT_MAKER`) orders.
* **Whole-list validation before output**: For `OCO`, `OTO`, and `OTOCO` lists (`OPO` and `OPOCO` have no wire encoding in this adapter and are rejected), the Binance encoder validates every leg before emitting any parameter:
  * Both legs of an `OCO` pair—and the two pending legs of an `OTOCO` list—must share identical `Side` and `quantity`.
  * Every leg in the list must share the identical `SelfTradePrevention` mode.
  * Working and pending leg order types must satisfy Binance's permitted `OCO`/`OTO`/`OTOCO` combinations, every prefixed leg parameter (`above*`, `below*`, `working*`, `pending*`, `pendingAbove*`, `pendingBelow*`) is checked against an explicit per-role parameter whitelist, and an `OCO` maker leg carrying `icebergQty` automatically includes `timeInForce = GTC` where required by the venue.
* **Consistent REST/WebSocket signing within `16,384` bytes**: Each encoded request is bounded to `<= 64` parameters and `<= 16,384` bytes (`MAX_REQUEST_BYTES`, succeeding on an exact `16,384`-byte fit). REST signs the exact percent-encoded query string; WebSocket signs the sorted raw `key=value` preimage and serializes those exact parameter values into JSON.

## Transitions and Failure Semantics

Decoding a tag-`11` command in a consumer that has not enabled `SpotAdvanced` fails closed at CBOR decode time before reaching risk or protocol logic. Even when wire preparation is enabled, any invalid peg/order-type combination, duplicate client ID across list and legs, unsupported `OPO`/`OPOCO` list kind, whitelist violation, parameter count `> 64`, or serialized payload `> 16,384` bytes aborts `request::prepare` atomically before signing, while the account admission path independently rejects any advanced command variant whose reservation or list-lifecycle rules are not enabled.

## Verification and References

```sh
cargo test --locked -p tickwire-venue --test spot_extensions
cargo test --locked -p tickwire-binance --test request_extensions
```

Canonical CBOR tests verify deterministic encoding, SHA-256 digest stability, minimal-integer rules, peg/TIF/iceberg invariants, and rejection of tag `11` by non-advertising decoders. Protocol extension tests verify REST query strings, WebSocket JSON envelopes and signature preimages, `OCO`/`OTO`/`OTOCO` prefixed-parameter whitelists, and exact `64`-parameter / `16,384`-byte boundary behavior.

* [Canonical command CBOR codec](../../crates/tickwire-venue/src/codec.rs)
* [Canonical `SpotAdvanced` validation tests](../../crates/tickwire-venue/tests/spot_extensions.rs)
* [Binance wire request encoder](../../crates/tickwire-binance/src/request.rs)
* [Binance advanced spot wire encoding tests](../../crates/tickwire-binance/tests/request_extensions.rs)
* [Binance signing and order protocol specification](binance-protocol.md)
