# Binance Order Signing and Full-Market Wire Protocol

`tickwire-binance` translates canonical Tickwire trading commands and zero-allocation ORS order parameters into signed REST HTTP targets, form-urlencoded Options requests, or WebSocket JSON request envelopes across all four Binance markets: **Spot (`/api/v3`, `/ws-api/v3`), USD-M Futures (`/fapi/v1`, `/ws-fapi/v1`), Coin-M Futures (`/dapi/v1`, `/ws-dapi/v1`), and European Options (`/eapi/v1`)**. It authenticates outgoing parameters with HMAC-SHA256, RSA-SHA256, or Ed25519 PKCS#8 keys (`der` and `pem`), and classifies exchange HTTP/WebSocket replies and terminal order states into deterministic outcome categories.

Built in safe Rust (`#![forbid(unsafe_code)]`) with zero floating-point arithmetic, zero I/O, and zero local WAL dependencies, this layer owns parameter formatting, integer fixed-point ASCII scaling, cryptographic signing, stream URL decoration, and wire response/reason classification, while delegating order-slot ownership, STP books, multi-wallet holds, and `OrsQueryDesk` reconciliation to [`tickwire-account`](account.md) and [`tickwire-binance-account`](binance-ws-trading.md).

## Protocol Model, Full-Market Endpoints, and Bounds

`request::prepare(command, credentials, options, max_bytes)` binds canonical Spot and USD-M commands to their `CanonicalCommand::digest()`, while `full_market` provides zero-allocation slice encoders (`&mut [u8] -> Result<usize>`) across all four `BinanceVenueMarket` domains (`Spot`, `Usdm`, `Coinm`, `Options`) for the stateless ORS `REQ`, `Dialer`, and `AsyncRestQuerier` paths:

| Market | Order Entry Wire Protocol | User-Data Stream Auth | REST Query Path Prefix |
| --- | --- | --- | --- |
| **Spot** | WebSocket `/ws-api/v3` (`order.place`, `order.cancel`, `order.cancelReplace`, `order.amend.keepPriority`), Ed25519 `session.logon` | `userDataStream.subscribe` on the authenticated order/stream WebSocket pool (no listen key) | `/api/v3` (`order`, `openOrders`, `account`) |
| **USD-M** | WebSocket `/ws-fapi/v1` (`order.place`, `order.cancel`, `order.modify`), Ed25519 `session.logon` | `POST`/`PUT /fapi/v1/listenKey`; `/ws/<key>` on colo (`fstream-mm`), `?listenKey=<key>` on public (`fstream`) | `/fapi/v1` (`order`, `openOrders`), `/fapi/v2` & `/fapi/v3` (`account`) |
| **Coin-M** | WebSocket `/ws-dapi/v1` (`order.place`, `order.cancel`, `order.modify`), Ed25519 `session.logon` | `POST`/`PUT /dapi/v1/listenKey`; `/ws/<key>` on colo (`dstream-mm`), `?listenKey=<key>` on public (`dstream`) | `/dapi/v1` (`order`, `openOrders`, `account`) |
| **Options** | HTTPS `/eapi/v1/order` (`POST` place, `DELETE` cancel), `application/x-www-form-urlencoded` body signed with HMAC-SHA256 + `X-MBX-APIKEY` | `POST`/`PUT /eapi/v1/listenKey`; `/ws/<key>` on colo, `?listenKey=<key>` on public (`fstream:443/private/ws`) | `/eapi/v1` (`order`, `openOrders`, `account`) |

| Boundary | Rule or Limit | Enforcement |
| --- | --- | --- |
| Request size & arity | `1..=16,384` bytes (`MAX_REQUEST_BYTES`), `<= 64` parameters | Checked in bounded stack scratch buffers without partial writes. |
| Key isolation & thread safety | `SigningKey: Send + Sync` (`HmacSha256`, `RsaSha256`, `Ed25519` from DER or PEM); omits `Debug`, `Clone`, and key export | Independent `SigningKey` instances constructed at startup for `REQ`, `Dialer`, and `AsyncRestQuerier` avoid cross-thread mutex contention. |
| Time-in-force & order modifiers | `GTC`, `IOC`, `FOK`, `PostOnly` (`GTX` on USD-M/Coin-M/Options, `LIMIT_MAKER` on Spot), `GTD`, and `RPI` (`TimeInForce::Rpi`); `reduceOnly`, `positionSide`, and `selfTradePreventionMode` | Unsupported market/modifier combinations fail closed before signing. |
| Fixed-point wire formatting | Exact integer `ticks` / `lots` scaled by `10^-scale` (`format_scaled_i64` and `Decimal`) | Zero floating-point conversions on financial or rate-limit paths. |
| Response & event bounds | `1..=1,048,576` bytes (`MAX_RESPONSE_BYTES`), error `msg <= 4,096` bytes, WS `id <= 256` bytes | Oversized, negative-quantity/price, or malformed error payloads return `Error::Capacity` or `Error::InvalidResponse`. |

## Core Invariants

* **Single-preimage parameter signing**: REST query targets, Options form-urlencoded bodies, and WebSocket JSON parameter objects serialize and sign from one canonical parameter representation so the signed preimage always matches the transmitted fields byte-for-byte.
* **No silent error-to-success promotion**: `response::decode_rest` inspects JSON responses for negative `code` discriminators independently of struct deserialization, ensuring malformed error bodies fail closed with `Error::InvalidResponse`.
* **Exact ORS `TerminalReason` & `explainError` classification**:
  * Ambiguous send/execution codes (`-1007` status unknown, `-1000` unknown error / try again later, `-1001`, `-1006`, HTTP `408`) classify as `TerminalReason::MKT_DELAYED` so the session retains open-order holds and schedules a `StatusAsk` rather than releasing funds prematurely.
  * `X == "EXPIRED_IN_MATCH"` or `x == "TRADE_PREVENTION"` classifies as `TerminalReason::MKT_CANCELED_SELFTRADE`.
  * `X == "EXPIRED"` maps `IOC`/`FOK` to `MKT_IOC_CANCELED`, `GTX` on Spot/Coin-M/Options to `MKT_REJECT_POST_ONLY`, and USD-M non-IOC/FOK expiry to `MKT_REDUCE_ONLY_CANCELED` (`MKT_CANCELED` on non-GTX Spot).
  * Other cancellations resolve to `TerminalReason::NO_REASON` (`ADMIN_ACTION` if operator-initiated) while `pending_cxl > 0`, and `TerminalReason::MKT_CANCELED` otherwise.

## Response Classification Semantics

`response::classify(market, status, code, message)` and `full_market::explain_binance_error` partition exchange errors without hidden retries:

* **`Banned`**: HTTP `418` IP auto-ban.
* **`RateLimited`**: HTTP `429` or error codes `-1003` / `-1015`.
* **`Partial`**: HTTP `409` cancel-replace or batch partial completion.
* **`NotFound`**: Error codes `-2011` or `-2013` (`MKT_REJECT_NO_SUCH_ORDER`).
* **`Authentication`**: HTTP `401` or error codes `-1002`, `-1022`, `-2014`, `-2015`.
* **`ClockSkew`**: Error code `-1021` (timestamp outside `recvWindow`).
* **`Rejected`**: Deterministic parameter, filter, post-only (`-5022`), reduce-only (`-2022`), no-change (`-5027`), self-trade, or insufficient-balance (`-2010`, `-2019`) codes.
* **`Unknown` (`MKT_DELAYED`)**: HTTP `408`, `-1000`, `-1001`, `-1006`, `-1007`, generic `5xx`, and unrecognized negative server codes.

## Verification and References

```sh
cargo test --locked -p tickwire-binance --all-targets
```

* [Protocol crate root and error taxonomy](../../crates/tickwire-binance/src/lib.rs)
* [Full-market Spot/USD-M/Coin-M/Options wire encoders and classifiers](../../crates/tickwire-binance/src/full_market.rs)
* [Canonical-to-wire request preparation](../../crates/tickwire-binance/src/request.rs)
* [Shared REST/WebSocket parameter encoding](../../crates/tickwire-binance/src/params.rs)
* [HMAC, RSA, and Ed25519 DER/PEM authentication](../../crates/tickwire-binance/src/auth.rs)
* [REST and WebSocket response classifier](../../crates/tickwire-binance/src/response.rs)
* [Stateless ORS account and `OrderManager` specification](account.md)
* [Binance multi-market ORS session specification](binance-ws-trading.md)
