# tickwire-md

The market-data process owns public venue connections and publishes normalized data to its consumers.

Start at the CLI mode and follow its session, sequence policy and publication target. Distinguish a replacement top-of-book quote from incremental depth when choosing a channel.

[Crate API](src/lib.rs) · [instrument slots](tests/instrument_slots.rs) · [market events](tests/market_events.rs) · [publisher hot](tests/publisher_hot.rs) · [sbe publication](tests/sbe_publication.rs) · [spot quotes](tests/spot_quotes.rs)

## Where to change a feed

For Binance, configure a feed in `config` and its sequence and socket owner in `engine`. A topic binds an instrument ID to its approved catalog entry and dense slot. During expansion the existing slots remain stable, the new bank warms up, and each consumer joins at a verified cut. The old mapping stays valid while a reader holds it. Socket registrations and the producer workspace are allocated ahead of normal polling. [Catalog validation](src/catalogs.rs) · [Runtime owners](src/engine/owners.rs) · [Expansion](src/engine/expansion.rs)

For Bybit or OKX, start with the native frame and the exact price and size conversion in the spot session. The shared publisher accepts the normalized quote only for the configured venue, symbol, scales and current epoch. JOIN authenticates the child, request and topic before handing over read descriptors. Exercise a reconnect and a late subscriber with a genuine socket and shared memory. [Spot session](src/spot_quotes.rs) · [Spot process](src/spot_market.rs) · [Admission service](src/server.rs)

Specifications include [binance market events](../../specs/implementation/binance-market-events.md), [instrument catalog](../../specs/implementation/instrument-catalog.md), [market expansion](../../specs/implementation/market-expansion.md).

```sh
cargo test --locked -p tickwire-md --all-targets
```
