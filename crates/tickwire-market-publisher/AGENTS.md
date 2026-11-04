# tickwire-market-publisher

The publisher writes complete market updates and the matching snapshot frontier into a shared bank.

Publish related depth or quote records as one batch. Keep scratch space out of the allocation path and verify reader recovery after a missed window.

[Crate API](src/lib.rs)

Specifications include [binance market events](../../specs/implementation/binance-market-events.md).

```sh
cargo test --locked -p tickwire-market-publisher --all-targets
```
