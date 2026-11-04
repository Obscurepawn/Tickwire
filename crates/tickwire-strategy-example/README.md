# Registered counting strategy

The counting worker implements the SDK's `Strategy` trait. Each market callback increments its observation count. With a configured limit order, the worker emits one intent after its observation and price thresholds are met. It checkpoints the counters and the identity of the consumed action. [Implementation](src/lib.rs)

From the repository root, start two counting workers on one local quote feed.

```sh
python3 scripts/run-two-strategies.py
```

Each worker has its own PID, market cursor and checkpoint. The launcher prints both PIDs and each worker's callback count. The example uses observation-only parameters. A strategy that needs just the spread can read the best bid and ask in `Strategy::on_market` and return `Ok(None)`. [Compiled spread example](../tickwire-strategy-sdk/src/lib.rs) · [Shared-feed configuration](../tickwire-md/examples/spot-market.md)

## Configure an order

Set the bootstrap factory ID to `counting_example`. Supply an absolute configuration file, its digest and byte count, and a positive revision. Encode the configuration with `Parameters::bytes()`. Its fields include `enabled`, `minimum_observations`, `minimum_bid_ticks` and an optional `command`. An enabled worker takes a canonical spot limit order belonging to its strategy instance. On qualified depth the price condition reads the best bid. A quote-only subscription updates the callback count.

```sh
cargo test --locked -p tickwire-strategy-example --all-targets
cargo test --locked -p tickwire-strategy-sdk --doc
```
