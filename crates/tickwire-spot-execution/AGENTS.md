# tickwire-spot-execution

`tickwire-spot-execution` (`#![forbid(unsafe_code)]`) implements stateless, full-product Unified Trading Account (UTA) ORS execution for Bybit V5 (`BybitUtaSession`) and OKX V5 (`OkxUtaSession`), covering Spot, Linear/Inverse Perpetuals, Linear/Inverse Dated Futures, and Options (`Product::{Spot, LinearPerp, InversePerp, LinearFutures, InverseFutures, Options}`), Retail Price Improvement (`TimeInForce::Rpi`), optional Options/Block `RFQ` quote hooks (`RfqQuoteHook`), server maintenance `notice` graceful connection switchover, zero-allocation single-pass `JsonScan` frame parsing, `FillRecordPool` `amendment = true` fee patching, `OrsQueryDesk` single-order reconciliation, and stateless cold Exchange REST bootstrap.

## Module Architecture & Single-Writer Ownership

* **[`src/lib.rs`](src/lib.rs)**: Public venue identity (`SpotVenue`), credential types (`Credentials`, `OwnedCredentials`), UTA product/margin/position descriptors (`OkxMarginMode`, `BybitPositionIdx`, `bybit_category_for_product`, `okx_inst_type_for_product`), endpoint resolvers, and crate re-exports.
* **[`src/ws_envelope.rs`](src/ws_envelope.rs)**: Zero-allocation stack WebSocket login, subscription step state machine, and heartbeat (`ping`/`pong`) builders/verifiers for OKX V5 (`op: "login"` Base64 HMAC-SHA256 `"{ts}GET/users/self/verify"`, step `0` `orders` `instType: "ANY"`, step `1` `balance_and_position`, `20s` text `"ping"`/`"pong"`) and Bybit V5 (`op: "auth"` hex HMAC-SHA256 `"GET/realtime{expires}"`, private `["order", "execution", "execution.fast", "wallet"]`, `20s` `{"op":"ping"}`).
* **[`src/wire_encode.rs`](src/wire_encode.rs)**: Zero-allocation stack wire encoders for Bybit V5 and OKX V5 WebSocket trade frames (`WsLink` `Shared` 16-in-flight) and pooled HTTP/1.1 keep-alive `POST` requests (`HttpsLink` `Exclusive`), supporting all six UTA products, `reduceOnly`, `positionIdx` / `posSide`, `tdMode`, `smpType`, and `TimeInForce::Rpi` (`"RPI"` on Bybit, `ordType: "rpi"` on OKX).
* **[`src/json_parser.rs`](src/json_parser.rs)**: Single-pass zero-allocation `JsonScan` parsers (`BybitUtaParser`, `OkxUtaParser`, implementing `tickwire_transport::json_scan::MsgParser`) decoding synchronous WS/HTTPS order/amend/cancel replies, private `order`/`orders` projections (`ProjectedFill` + deterministic `TerminalReason` classification), `execution.fast` (no fee) -> full `execution` fee amendments (`amendment = true`), multi-wallet balances, rate-limit codes/headers (`429`, Bybit `10006`, OKX `50011`), and server maintenance notices (`{"event":"notice","code":"64008",...}` -> `graceful_drain_connection = true`).
* **[`src/rfq.rs`](src/rfq.rs)**: Optional `RfqQuoteHook` extension (`has_quote`, `open_quotes`, `submit_quote`, `cancel_quotes`) with default `NoopRfqQuoteHook` (`TerminalReason::RFQ_QUOTE_NOT_SUPPORTED`) and preallocated `UtaRfqQuoteBook` encoding Bybit `/v5/rfq/*` and OKX `/api/v5/rfq/*` payloads.
* **[`src/session.rs`](src/session.rs)**: `UtaAccountSession`, `BybitUtaSession`, and `OkxUtaSession` binding `OrderManager`, shared `[local_ip, count]` connection pools (`OrderRoute.product_mask = 0`), `PublishedRoutes::pick` (`9/8` EWMA hysteresis) -> `OrsAccountQuota` (`SharedRateLimiter`, immediate NACK on limit without retrying another connection), `dry_run` execution, graceful maintenance `notice` connection drain + `Dialer` replacement switchover, and `OrsQueryDesk` scheduling.
* **[`src/rest_bootstrap.rs`](src/rest_bootstrap.rs)**: Signed off-hot-path REST `GET` builders (`UtaRestRequestBuilder` for `AsyncRestQuerier`), single-order `StatusAsk` snapshot decoders (`OrsOrderQuerySnapshot`), and cold-startup Exchange REST snapshot normalizers (`UtaBootstrapDecoder` -> `OrsExchangeBootstrapSnapshot` reconstructed via `tw1...` `SelfDescribingClientId`).
* **[`src/private.rs`](src/private.rs), [`src/network.rs`](src/network.rs), [`src/observe.rs`](src/observe.rs), [`src/warmup.rs`](src/warmup.rs), [`src/bin/tickwire-spot-observe.rs`](src/bin/tickwire-spot-observe.rs)**: Stateless TLS/WebSocket transport helpers, side-effect-free hot-path cache & loopback TLS warmer (`UtaHotPathWarmer` / `SpotHotPathWarmer`), and read-only CLI observer.

## Non-Negotiable Invariants

1. **Pure Stateless Execution**: Zero local write-ahead log, journal, blob store, or disk recovery state. Cold boot and restart recover solely from authenticated Exchange REST snapshots (`Account + Positions + OpenOrders`) and `tw1...` `SelfDescribingClientId`, keeping gateway receipts (`FrontAck`), matching-engine book acceptance (`Ack`), cumulative order-status projections (`ProjectedFill`), and granular trade fills (`Fill` + `amendment = true` fee updates) distinct.
2. **Fine-Grained Spinlock Discipline**: Never overlap `Order` (`OrderManager`) and `Connection` (`LockedConnection`) spinlocks; acquire `OrsStpBook` only while holding a single `Order` spinlock; perform zero logging, zero heap allocation, and zero JSON/HTTP/WebSocket parsing under any spinlock.
3. **Exact Fixed-Point Math & `#![forbid(unsafe_code)]`**: Integer price ticks, integer quantity lots, and `Fixed` / `i128` scaled atoms only; zero floating-point arithmetic in financial, routing, or rate-limit paths.

## Governing Specifications & Verification

* **Specifications**: [spot-execution.md](../../specs/implementation/spot-execution.md) · [spot-rest-recovery.md](../../specs/implementation/spot-rest-recovery.md)

```sh
cargo fmt --all -- --check
cargo clippy --locked -p tickwire-spot-execution --all-targets -- -D warnings
cargo test --locked -p tickwire-spot-execution --all-targets
python3 scripts/verify-documentation.py
```
