# tickwire-binance-session

The private Binance session borrows the calibrated clock and delivery budget from the runtime owner.

Carry the original time reading through signing and validation. Test an expired window and a connection change while work remains queued.

[Crate API](src/lib.rs) · [integration](tests/integration.rs)

Specifications include [binance private clock adapter](../../specs/implementation/binance-private-clock-adapter.md).

```sh
cargo test --locked -p tickwire-binance-session --all-targets
```
