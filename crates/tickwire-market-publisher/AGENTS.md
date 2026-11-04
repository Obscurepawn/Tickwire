# tickwire-market-publisher

The publisher writes complete market updates and the matching snapshot frontier into a shared bank.

Publish related depth or quote records as one batch. Allocate scratch space when constructing the publisher and verify reader recovery after a missed window. For a Bybit or OKX quote, publish the two-sided price and feed health under the same topic and source epoch. [Spot publisher](src/spot.rs) · [Spot input contract](../../specs/implementation/spot-quote-input.md)

[Crate API](src/lib.rs)

Specifications include [binance market events](../../specs/implementation/binance-market-events.md).

```sh
cargo test --locked -p tickwire-market-publisher --all-targets
```
