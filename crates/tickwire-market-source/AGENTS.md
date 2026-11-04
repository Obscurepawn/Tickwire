# tickwire-market-source

This crate exposes authenticated market records and local replicas to runtime consumers.

Bind each view to the current topic, generation, source and catalog. Exercise a gap, a replacement snapshot and a consumer moving to a new bank.

[Crate API](src/lib.rs) · [replica dense](tests/replica_dense.rs)

Specifications include [binance market events](../../specs/implementation/binance-market-events.md), [instrument index](../../specs/implementation/instrument-index.md), [join](../../specs/implementation/join.md), [market expansion](../../specs/implementation/market-expansion.md).

```sh
cargo test --locked -p tickwire-market-source --all-targets
```
