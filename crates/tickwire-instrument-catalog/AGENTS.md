# tickwire-instrument-catalog

The catalog stores versioned instrument economics shared by market data, strategy and execution.

Validate venue, product, native symbol, tick and lot scales against the same catalog object before activation. Preserve canonical byte identity across readers.

[Crate API](src/lib.rs) · [catalog](tests/catalog.rs) · [vectors](tests/vectors.rs)

Specifications include [instrument catalog](../../specs/implementation/instrument-catalog.md).

```sh
cargo test --locked -p tickwire-instrument-catalog --all-targets
```
