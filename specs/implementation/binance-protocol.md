# Binance Order Signing and Wire Protocol

`tickwire-binance` translates canonical Tickwire trading commands into signed REST HTTP targets or WebSocket JSON request envelopes, authenticates outgoing parameters with HMAC-SHA256, RSA, or Ed25519 keys, and classifies exchange HTTP/WebSocket replies into deterministic outcome categories. Every `PreparedRequest` binds its immutable wire payload directly to the originating `CanonicalCommand::digest()`.

This layer is strictly stateless: it owns parameter formatting, cryptographic signing, and wire response classification in safe Rust (`#![forbid(unsafe_code)]`), while delegating margin-mode and account-configuration checks to the account coordinator and delegating durable admission, lease verification, retry policy, and reservation release to the [account gateway](account-gateway.md).

## Protocol Model and Bounds

`request::prepare(command, credentials, options, max_bytes)` verifies that `venue == 1` (Binance) and that the command's `account`, `environment`, and `Market` (`Spot` vs `Usdm`) match the selected `Credentials` before constructing any wire bytes (`Error::IdentityMismatch` otherwise).

| Boundary | Rule or Limit | Enforcement |
| --- | --- | --- |
| Request size & arity | `1..=16,384` bytes (`MAX_REQUEST_BYTES`), `<= 64` parameters | Checked via bounded stack scratch before heap materialization. |
| API key & request ID | API key `1..=256` (`A-Za-z0-9-_`); `request_id` `1..=64` printable ASCII (`33..=126`) | Rejects header/JSON injection and malformed correlation IDs. |
| `recvWindow` | `1..=60,000,000` us; USD-M requires exact millisecond multiples (`% 1000 == 0`) | Spot fractional milliseconds serialize as 3-decimal numbers; integer milliseconds serialize as integers. |
| Timestamp units | Spot accepts nonzero `Milliseconds` or `Microseconds`; USD-M accepts `Milliseconds` only | Endpoint-specific units also govern USD-M `goodTillDate` (`GTD`) timestamps. |
| Conditional, modify & FIX fields | Preserves distinct USD-M `activatePrice` and `priceProtect`; encodes native USD-M `CommandBody::Modify` (`PUT /fapi/v1/order` / `"order.modify"`) and Spot `CommandBody::Modify` (`POST /api/v3/order/cancelReplace` / `"order.cancelReplace"` with `STOP_ON_FAILURE`); FIX `SenderCompID` uses permitted ASCII | Unsupported commands, parameter combinations, or endpoint/key algorithms fail at `prepare`. |
| Response & event fields | `1..=1,048,576` bytes (`MAX_RESPONSE_BYTES`), error `msg <= 4,096` bytes, WS `id <= 256` bytes; decodes optional exact `avgPrice` on `OrderReply`, `ap` (`average_price`) and `V` (`self_trade_prevention_mode`) on `OrderUpdate` | Oversized, negative-quantity/price, or empty bodies return `Error::Capacity` or `Error::InvalidResponse`. |

Both REST and WebSocket encoders populate a single shared `Params` collection prior to signing. REST signs the exact percent-encoded query string and uses that signed string directly as the HTTP request target; WebSocket signs the sorted parameter preimage and serializes the identical parameter set into JSON, eliminating any possibility of signing one parameter value while transmitting another.

## Core Invariants

* **Cryptographic command-digest binding**: `PreparedRequest` privately owns both the serialized wire bytes and the 32-byte `CanonicalCommand` SHA-256 digest (`verify_digest`), allowing the transport gate to prove before the first application byte leaves user space that the outgoing packet matches the durably admitted intent.
* **No silent error-to-success promotion**: `response::decode_rest` probes JSON object responses for a negative `code` discriminator independently of `ApiError` struct deserialization. Consequently, an HTTP `200` carrying a malformed or truncated error object fails with `Error::InvalidResponse` rather than falling through to the success branch.
* **Conservative unknown-execution classification**: Because releasing an account reservation on an ambiguous error risks unrecorded fills, the classifier maps HTTP `408`, documented execution-unknown codes (`-1000`, `-1001`, `-1006`, `-1007`), unrecognized negative server codes, and generic `5xx` responses to `FailureKind::Unknown`. Only explicitly documented deterministic failures map to `Rejected`.

## Response Classification Semantics

`response::classify(market, status, code, message)` partitions exchange errors into eight `FailureKind` variants without ever retrying an order or mutating reservations:

* **`Banned`**: HTTP `418` IP auto-ban.
* **`RateLimited`**: HTTP `429` or error codes `-1003` / `-1015`.
* **`Partial`**: HTTP `409` cancel-replace or batch partial completion.
* **`NotFound`**: Error codes `-2011` or `-2013` (missing cancel/query target).
* **`Authentication`**: HTTP `401` or error codes `-1002`, `-1022`, `-2014`, `-2015`.
* **`ClockSkew`**: Error code `-1021` (timestamp outside `recvWindow` or ahead of server).
* **`Rejected`**: Deterministic parameter, filter, post-only, reduce-only, unmodified-order, or insufficient-balance codes (`-1013`, `-1014`, `-1020`, `-1100..=-1106`, `-1108`, `-1111`, `-1114..=-1121`, `-1128`, `-1130`, `-2010`, `-2019`, `-2021` when HTTP status `!= 409`, `-2022`, `-2038`, `-4003`, `-4014`, `-4016`, `-4024`, `-4061`, `-4131`, `-4164`, `-5021`, `-5022`, `-5027`), plus the documented USD-M HTTP `503` overload rejections (`code == -1008`, `"Service Unavailable."`, or `"Internal error; unable to process your request. Please try again."`). Private order updates also expose zero-allocation terminal reason classification via `events::classify_order_update_reason`, mapping `FILLED`, `EXPIRED_IN_MATCH`, `EXPIRED`, `CANCELED`, `REJECTED`, `timeInForce` (`GTX`/`IOC`/`FOK`), `LIMIT_MAKER`, and STP modes (`EXPIRE_MAKER`, `EXPIRE_TAKER`, `EXPIRE_BOTH`) into `tickwire_venue::TerminalReason`.
* **`Unknown`**: HTTP `408`, codes `-1000`, `-1001`, `-1006`, `-1007`, all other `5xx` statuses, and any newly introduced or unrecognized venue error code.

## Verification and References

```sh
cargo test --locked -p tickwire-binance --test responses --test protocol_primitives --test protocol_server
```

Cryptographic vector tests validate published Binance HMAC-SHA256 and FIX Ed25519 vectors alongside an independent OpenSSL RSA vector. Local TLS loopback tests drive `PreparedRequest` over real `tickwire-transport` connections—capping per-call write acceptance to 17 bytes to exercise partial-write boundaries, verifying `command_digest()` before the first accepted application byte, validating REST and WebSocket signatures server-side, and decoding chunked HTTP and fragmented WebSocket responses.

* [Protocol crate root and error taxonomy](../../crates/tickwire-binance/src/lib.rs)
* [Canonical-to-wire request preparation](../../crates/tickwire-binance/src/request.rs)
* [Shared REST/WebSocket parameter encoding](../../crates/tickwire-binance/src/params.rs)
* [HMAC, RSA, and Ed25519 authentication](../../crates/tickwire-binance/src/auth.rs)
* [REST and WebSocket response classifier](../../crates/tickwire-binance/src/response.rs)
* [Response classification test suite](../../crates/tickwire-binance/tests/responses.rs)
* [Cryptographic primitive vector tests](../../crates/tickwire-binance/tests/protocol_primitives.rs)
* [TLS loopback and 17-byte partial-send tests](../../crates/tickwire-binance/tests/protocol_server.rs)
* [Account gateway specification](account-gateway.md)
