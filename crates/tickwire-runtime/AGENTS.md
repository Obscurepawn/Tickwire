# tickwire-runtime

The [event loop](src/node.rs) polls the clock, market, private input and account before asking strategies for commands. A fill that arrives in that turn updates available funds before another strategy spends them. Sample fresh monotonic time after account polling and before submission.

Use the [strategy interface](src/node/strategy.rs) to deliver ordered account inputs and route submission results. A change in admission should exercise both file and hosted strategy intake. [Trading path](../../specs/implementation/single-host-trading.md) · [Account gateway](../../specs/implementation/account-gateway.md)

```sh
cargo test --locked -p tickwire-runtime --all-targets
```
