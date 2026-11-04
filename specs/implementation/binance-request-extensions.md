# Binance advanced spot requests

Schema 1 keeps its existing canonical command tags 1–10 byte-for-byte. Tag 11 (`SpotAdvanced`) adds four signed spot request forms. The entire canonical command still contributes to its SHA-256 digest. Older decoders reject tag 11. Send it only to consumers that advertise the capability. [Canonical command tests](../../crates/tickwire-venue/tests/spot_extensions.rs)

| Subtype | Canonical array |
| --- | --- |
| New | `[1, SpotOrder]` |
| CancelReplace | `[2, OrderTarget, SpotOrder, CancelReplaceMode]` |
| List | `[3, SpotAdvancedList]` |
| SorNew | `[4, OrderSpec]` |

`SpotOrder = [OrderSpec, Option<Peg>]`. A peg holds `[PegPriceType, Option<u16>]`, where Primary is 1 and Market is 2. An advanced list holds `[list_id, client_id, ListKind, BoundedList<SpotOrder,3>]`. Reject malformed array lengths, unsupported enum values, non-minimal integers, truncation or trailing bytes.

New and CancelReplace require a peg. At least one advanced list leg must use a peg. Keep list and leg client IDs distinct. Check leg counts and a nonzero list ID. A peg can omit the explicit limit price. An explicit limit, stop price and trailing distance retain their separate meanings. A present offset is 0–100 price levels and requires both offset value and type. Reject pegged MARKET orders, a Primary peg on an ordinary non-GTC LIMIT, and a Market peg on a maker order. SOR accepts normal spot LIMIT/MARKET orders with base quantity. Ordinary spot LIMIT+PostOnly may carry iceberg quantity under the original encoding.

The Binance encoder validates the entire OCO/OTO/OTOCO list before output. OCO pairs and the OTOCO pending pair share side and quantity. All legs share self-trade-prevention mode. Check working/pending type combinations and each prefixed parameter against its whitelist. For an OCO maker iceberg include GTC as required. OPO/OPOCO have no encoding here.

REST signs the actual percent-encoded query. WebSocket signs sorted raw parameter values and sends those same parameters in JSON. A request is bounded to 64 parameters and 16,384 bytes, with exact-fit success. The adapter encodes the request. The associated order-list lifecycle, price filtering, symbol eligibility and multi-leg reservation require other components. The account path rejects unsupported advanced commands. [Wire request coverage](../../crates/tickwire-binance/tests/request_extensions.rs)

```sh
cargo test --locked -p tickwire-venue --test spot_extensions
cargo test --locked -p tickwire-binance --test request_extensions
```
