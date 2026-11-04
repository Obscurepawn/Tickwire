# tickwire-binance-execution

This adapter binds signed Binance REST or WebSocket requests to the account gateway and its first-write authorization.

Use the original request's clock window, quota, route and digest when checking a send. Recheck those at the first accepted application byte, including a wait in the local queue.

[Crate API](src/lib.rs)

Specifications include [binance execution](../../specs/implementation/binance-execution.md).

```sh
cargo test --locked -p tickwire-binance-execution --all-targets
```
