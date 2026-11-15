# Binance Advanced Order and Time-in-Force Extensions

Canonical command schema `1` preserves command tags `1..=10` byte-for-byte (including `LIMIT + PostOnly` with `iceberg_quantity` on ordinary spot orders), introduces tag `11` (`SpotAdvanced`) to represent pegged spot orders, pegged cancel-replace requests, multi-leg spot lists (`OCO`, `OTO`, `OTOCO`) containing pegged legs, and Smart Order Routing (`SOR`) submissions, encodes schema-`5` tag `12` (`CommandBody::Modify`) across USD-M (`PUT /fapi/v1/order` / `"order.modify"`), Coin-M (`PUT /dapi/v1/order` / `"order.modify"`), and Spot (`POST /api/v3/order/cancelReplace` / `"order.cancelReplace"` with `cancelReplaceMode = STOP_ON_FAILURE`), and supports `TimeInForce::Rpi` (`"RPI"`) alongside `GTC`, `IOC`, `FOK`, `PostOnly` (`GTX` / `LIMIT_MAKER`), and `GTD`.

The entire canonical CBOR encoding feeds `CanonicalCommand::digest()` (SHA-256). `tickwire-venue` owns canonical CBOR validation, while `tickwire-binance` owns REST, form-urlencoded HTTPS, and WebSocket wire parameter encoding and signing ([binance-protocol.md](binance-protocol.md)); stateless order-slot admission, STP checks, and multi-wallet collateral holds remain owned by [`tickwire-account`](account.md) and [`tickwire-binance-account`](binance-ws-trading.md).

## Canonical Schema (`Tag 11`), Modify, and TIF Rules

Tag `11` encodes a 2-element canonical CBOR array `[11, [subtype, ...]]` over four subtypes, rejecting non-minimal integers, wrong array lengths, unknown enum discriminants, truncated payloads, or trailing bytes:

| Subtype / Extension | Discriminant & Wire Mapping | Structural Requirements |
| --- | --- | --- |
| `SpotAdvanced::New` | `[1, SpotOrder]` -> `POST /api/v3/order` / `order.place` | `SpotOrder` **must** carry `Some(Peg)`. |
| `SpotAdvanced::CancelReplace` | `[2, OrderTarget, SpotOrder, CancelReplaceMode]` -> `order.cancelReplace` | `SpotOrder` **must** carry `Some(Peg)`. |
| `SpotAdvanced::List` | `[3, SpotAdvancedList]` -> `orderList.place.{oco,oto,otoco}` | Nonzero `list_id`, valid leg count for `ListKind`, distinct list and leg `client_id`s, and **at least one** leg with `Some(Peg)`. |
| `SpotAdvanced::SorNew` | `[4, OrderSpec]` -> `POST /api/v3/sor/order` / `sor.order.place` | Standard spot `LIMIT` or `MARKET` order specified in base-asset quantity (no quote-order-qty, no peg). |
| `CommandBody::Modify` | Tag `12` -> `order.modify` (USD-M / Coin-M), `order.cancelReplace` (`STOP_ON_FAILURE`, Spot) | Preserves queue priority where supported by the venue and adjusts open-order holds and STP book prices atomically on ack. |
| `TimeInForce::Rpi` | `"timeInForce": "RPI"` on `LIMIT` orders | Retail Price Improvement maker-only time in force across supported Binance order books. |

Here `SpotOrder = [OrderSpec, Option<Peg>]` and `Peg = [PegPriceType, Option<u16>]`, where `PegPriceType` is `1` (`Primary`, tracking same-side best price) or `2` (`Market`, tracking opposite-side best price), and `Option<u16>` encodes an optional level offset in `0..=100` (`pegOffsetValue` + `pegOffsetType = PRICE_LEVEL`).

## Core Invariants

* **Orthogonal price fields and peg compatibility**: A pegged `SpotOrder` may omit its explicit `OrderSpec` limit price, while explicit limit price, stop/trigger price, and trailing delta retain separate semantics and never alias one another. Validation rejects pegged `MARKET` orders, `Primary` pegs on ordinary non-`GTC` `LIMIT` orders, and `Market` pegs on maker-only (`PostOnly` / `LIMIT_MAKER`) orders.
* **Whole-list validation before output**: For `OCO`, `OTO`, and `OTOCO` lists (`OPO` and `OPOCO` are rejected), the Binance encoder validates every leg before emitting any parameter:
  * Both legs of an `OCO` pair—and the two pending legs of an `OTOCO` list—must share identical `Side` and `quantity`.
  * Every leg in the list must share the identical `SelfTradePrevention` mode.
  * Working and pending leg order types must satisfy Binance's permitted `OCO`/`OTO`/`OTOCO` combinations, and every prefixed leg parameter (`above*`, `below*`, `working*`, `pending*`, `pendingAbove*`, `pendingBelow*`) is checked against an explicit per-role parameter whitelist.
* **Consistent REST/WebSocket signing within `16,384` bytes**: Each encoded request is bounded to `<= 64` parameters and `<= 16,384` bytes (`MAX_REQUEST_BYTES`). REST signs the exact percent-encoded query string; WebSocket signs the sorted raw `key=value` preimage and serializes those exact parameter values into JSON.

## Transitions and Failure Semantics

Decoding a tag-`11` command in a consumer that has not enabled `SpotAdvanced` fails closed at CBOR decode time. Even when wire preparation is enabled, any invalid peg/order-type/TIF combination, duplicate client ID across list and legs, unsupported `OPO`/`OPOCO` list kind, whitelist violation, parameter count `> 64`, or serialized payload `> 16,384` bytes aborts request encoding atomically before signing or touching any socket.

## Verification and References

```sh
cargo test --locked -p tickwire-venue --test spot_extensions
cargo test --locked -p tickwire-binance --test request_extensions
```

* [Canonical command CBOR codec](../../crates/tickwire-venue/src/codec.rs)
* [Canonical `SpotAdvanced` validation tests](../../crates/tickwire-venue/tests/spot_extensions.rs)
* [Binance wire request encoder](../../crates/tickwire-binance/src/request.rs)
* [Full-market ORS wire encoders](../../crates/tickwire-binance/src/full_market.rs)
* [Binance advanced spot wire encoding tests](../../crates/tickwire-binance/tests/request_extensions.rs)
* [Binance signing and order protocol specification](binance-protocol.md)
