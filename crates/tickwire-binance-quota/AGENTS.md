# tickwire-binance-quota

This crate maps Binance requests to quota weights consumed by the shared local limiter.

When adding an endpoint, specify its product, request weight and order-count effect. Test the interaction with clock requests and private session maintenance under the same budget.

[Crate API](src/lib.rs) · [permits](tests/permits.rs)

Specifications include [binance quota](../../specs/implementation/binance-quota.md).

```sh
cargo test --locked -p tickwire-binance-quota --all-targets
```
