# tickwire-gateway

The journaled sender binds application bytes and a socket operation to a persisted trading command.

Trace the final authorization callback and accepted-byte count. A zero-byte abort and a partially accepted send lead to different recovery states.

[Crate API](src/lib.rs) · [binance rest](tests/binance_rest.rs) · [lifecycle](tests/lifecycle.rs)

Specifications include [command binding](../../specs/implementation/command-binding.md), [gateway](../../specs/implementation/gateway.md).

```sh
cargo test --locked -p tickwire-gateway --all-targets
```
