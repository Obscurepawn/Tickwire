# tickwire-binance-public

This client owns Binance public subscriptions and pairs incremental depth with the REST snapshot.

Exercise the snapshot bridge at the sequence boundary. A gap starts a fresh synchronization cycle. Best bid/ask reads must reflect the live generation.

[Crate API](src/lib.rs) · [constructor stack](tests/constructor_stack.rs) · [control](tests/control.rs) · [events](tests/events.rs) · [network](tests/network.rs) · [rebuild](tests/rebuild.rs)

Specifications include [binance public session](../../specs/implementation/binance-public-session.md).

```sh
cargo test --locked -p tickwire-binance-public --all-targets
```
