# tickwire-binance

The Binance codec turns native request fields, private events and depth messages into typed Rust values.

Use exchange examples to check field names, units, timestamp precision and which bytes enter a signature. A transport timeout and a venue order rejection carry different evidence.

[Crate API](src/lib.rs) · [depth sync](tests/depth_sync.rs) · [depth sync server](tests/depth_sync_server.rs) · [depth workspace](tests/depth_workspace.rs) · [feed](tests/feed.rs) · [market events](tests/market_events.rs)

Specifications include [binance depth sync](../../specs/implementation/binance-depth-sync.md), [binance market events](../../specs/implementation/binance-market-events.md), [binance protocol](../../specs/implementation/binance-protocol.md), [binance streams](../../specs/implementation/binance-streams.md).

```sh
cargo test --locked -p tickwire-binance --all-targets
```
