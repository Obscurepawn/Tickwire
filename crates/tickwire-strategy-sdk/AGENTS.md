# tickwire-strategy-sdk

`Strategy::on_market` receives a borrowed view of the worker's local market state. For a top-of-book subscription, check that a complete fresh bid/ask triggers the callback and that a stale quote is rejected. Ordered account updates enter through `on_report`. [Strategy API](src/lib.rs) · [Market view](src/market.rs)

Bind an outgoing command to the input that triggered it, the decision time and the strategy's output sequence. The worker writes the command before it publishes its reference. Check both the command identity and checkpoint when changing how the worker handles a callback. [Intent writer](src/outbound.rs) · [Worker](src/worker.rs)

```sh
cargo test --locked -p tickwire-strategy-sdk --all-targets
cargo test --locked -p tickwire-strategy-sdk --doc
```
