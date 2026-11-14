# tickwire-binance

`tickwire-binance` (`#![forbid(unsafe_code)]`) owns pure, socket-free protocol codecs, cryptographic request signing
(`auth::SigningKey`), REST/WS request formatting, response classification, incremental depth synchronization
(`depth_sync`), and market/private event parsing for Binance Spot and USD-M Futures. Session drivers
(`tickwire-binance-public`, `tickwire-binance-private`, `tickwire-binance-execution`) feed wire slices into this crate
and receive typed Rust values or signed byte buffers without I/O, clocks, or hidden retries.

## Invariants and Change Guidelines

* **Secret isolation and endpoint-algorithm matrix**: `SigningKey` in [src/auth.rs](src/auth.rs) omits `Debug`, `Clone`,
  and key export, and `Endpoint::permits` restricts each endpoint (`SpotRest`, `UsdmRest`, `UsdmWebSocket`, session/FIX
  logons) to supported algorithms (`HmacSha256`, `RsaSha256`, `Ed25519`); sign canonical preimages in
  [src/request.rs](src/request.rs) and [src/params.rs](src/params.rs) before percent-encoding REST signatures once.
* **Exact decimal scaling and time units**: Convert ASCII decimals in [src/decimal.rs](src/decimal.rs) without floating
  point, and preserve explicit millisecond vs. microsecond `TimeUnit` metadata across [src/feed.rs](src/feed.rs),
  [src/market.rs](src/market.rs), and [src/events.rs](src/events.rs).
* **Distinct transport vs. venue rejection evidence**: Classify HTTP/WS responses in [src/response.rs](src/response.rs)
  so definite venue rejections (including Spot/USD-M order-creation rejections `-2010`, `-2019`, `-2021` when HTTP
  status `!= 409`, `-2038`, `-4061`, `-4164`, `-5022` while keeping `409` as `FailureKind::Partial`), `418`/`429`
  limits, and malformed replies never collapse into silent retries; bind
  private execution reports to durable local commands in [src/events.rs](src/events.rs).
* **Depth snapshot bridge**: Keep Spot and USD-M `lastUpdateId`/`U`/`u`/`pu` bridging rules in
  [src/depth_sync.rs](src/depth_sync.rs) allocation-bounded and reset to a fresh snapshot cycle on any sequence gap.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/auth.rs](src/auth.rs) · [src/request.rs](src/request.rs) ·
  [src/response.rs](src/response.rs) · [src/events.rs](src/events.rs) · [src/depth_sync.rs](src/depth_sync.rs) ·
  [tests/requests.rs](tests/requests.rs) · [tests/request_extensions.rs](tests/request_extensions.rs) ·
  [tests/responses.rs](tests/responses.rs) · [tests/private_events.rs](tests/private_events.rs) ·
  [tests/private_structures.rs](tests/private_structures.rs) · [tests/market_events.rs](tests/market_events.rs) ·
  [tests/feed.rs](tests/feed.rs) · [tests/depth_sync.rs](tests/depth_sync.rs) ·
  [tests/depth_workspace.rs](tests/depth_workspace.rs) · [tests/sbe_depth.rs](tests/sbe_depth.rs)
* **Specifications and models**: [binance-protocol.md](../../specs/implementation/binance-protocol.md) ·
  [binance-streams.md](../../specs/implementation/binance-streams.md) ·
  [binance-depth-sync.md](../../specs/implementation/binance-depth-sync.md) ·
  [binance-market-events.md](../../specs/implementation/binance-market-events.md) ·
  [binance-request-extensions.md](../../specs/implementation/binance-request-extensions.md) ·
  [binance_depth_sync](../../tla/execution_models/binance_depth_sync/ROADMAP.md)

```sh
cargo test --locked -p tickwire-binance --all-targets
./scripts/check-tla.sh binance_depth_sync
```
