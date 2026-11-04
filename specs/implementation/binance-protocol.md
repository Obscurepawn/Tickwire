# Binance signing and order protocol

The Binance protocol crate converts a canonical trading command into a signed REST request or a WebSocket request message. The return value binds the prepared request to the complete canonical command digest. The protocol layer performs serialization, signing and response classification. The account gateway decides whether bytes may leave the process. [Protocol entry point](../../crates/tickwire-binance/src/lib.rs) · [Request preparation](../../crates/tickwire-binance/src/request.rs)

## Request identity and signing

`prepare` verifies the venue, account, market and environment against the selected credentials before producing a request. It takes the symbol, side, quantity, modifiers and order identifiers from the frozen command. An unsupported command, parameter combination or signing algorithm yields an error. The coordinator checks current account settings, including margin mode, before sending. [Request preparation](../../crates/tickwire-binance/src/request.rs)

`PreparedRequest` privately owns the wire request and the original `CanonicalCommand.digest()`. REST uses the signed query as the HTTP request target. WebSocket signing and JSON serialization share one parameter collection, so an order cannot be signed with one value and transmitted with another. Timestamp units, receive-window precision and field spellings follow the endpoint. USD-M GTD time uses its required units, and conditional orders preserve the distinct `activatePrice` and `priceProtect` parameters. FIX SenderCompID uses the permitted protocol character set. [Parameter handling](../../crates/tickwire-binance/src/params.rs) · [Authentication](../../crates/tickwire-binance/src/auth.rs)

## Response semantics

An HTTP 200 carrying an invalid error object cannot be promoted to a successful response. The classifier distinguishes rate limits, bans, authentication failures, clock errors, missing orders, partial outcomes and definite rejections. HTTP 408, recognized execution-unknown codes and unrecognized server codes retain an unknown outcome. Most 5xx responses are also unknown. The USD-M 503 branch recognizes the specific documented overload and service-unavailable rejection cases. The classifier never retries an order or releases a reservation. [Response classifier](../../crates/tickwire-binance/src/response.rs) · [Response tests](../../crates/tickwire-binance/tests/responses.rs)

## Verification boundary

Signature tests cover the published HMAC and FIX Ed25519 vectors and an independent OpenSSL RSA vector. Loopback tests feed the same prepared request to a real transport connection, verify REST and WebSocket signatures and parameters server-side, and exercise chunked HTTP and fragmented WebSocket replies. The client bounds acceptance per call to 17 bytes and compares the command digest before the first accepted application byte. These tests exercise byte-level preparation and transport behavior. Session lifecycle, durable admission and recovery have separate owners and tests. [Primitive tests](../../crates/tickwire-binance/tests/protocol_primitives.rs) · [TLS protocol tests](../../crates/tickwire-binance/tests/protocol_server.rs) · [Gateway](account-gateway.md)
