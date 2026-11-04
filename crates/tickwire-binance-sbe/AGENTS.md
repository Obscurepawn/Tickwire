# tickwire-binance-sbe

The Spot SBE decoder reads versioned binary market messages into normalized fields.

Check schema and template IDs, group dimensions and exact decimal exponents before reading the body. Benchmark the supported template and batch size when changing decode loops.

[Crate API](src/lib.rs) · [codec](tests/codec.rs)

Specifications include [binance sbe](../../specs/implementation/binance-sbe.md).

```sh
cargo test --locked -p tickwire-binance-sbe --all-targets
```
