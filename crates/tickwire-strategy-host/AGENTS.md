# tickwire-strategy-host

The host starts a strategy child, authenticates its process identity and owns its intent receiver and report writer. With an external market source the worker joins the publisher directly. Check the instance, logical slot and generation before accepting an intent or routing a result. [Host API](src/lib.rs) · [Single-host trading](../../specs/implementation/single-host-trading.md)

When changing child startup or shutdown, exercise failed authentication and an exited worker alongside a healthy peer. Check the surviving worker's cursor and reports. [Two-worker example](../../README.md#deploy-two-strategies)

```sh
cargo test --locked -p tickwire-strategy-host --all-targets
```
