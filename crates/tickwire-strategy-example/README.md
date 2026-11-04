# Registered counting strategy

For a minimal strategy, see the compiled `Spread` example on the [SDK strategy trait](../tickwire-strategy-sdk/src/lib.rs). Its `on_market` callback reads the best bid and ask in ticks, saves their spread and returns `Ok(None)`. Run `cargo test --locked -p tickwire-strategy-sdk --doc` to compile the example. The registered strategy below also demonstrates how to submit an intent and resume after restart.

The example builds two independent worker executables. `tickwire-counting-v1-worker` uses state schema `0x5457434e0001`. `tickwire-counting-worker` uses schema `0x5457434e0002` and registers migration method `1` from V1. Both enter the SDK through `registered::run` and implement normal bootstrap, source replay, restore probe and replay rollback. During normal startup the host authenticates the worker and transfers its shared-memory channels. The execution owner controls permission to trade.

To run two observation-only instances locally against the same quote feed, use `python3 scripts/run-two-strategies.py` from the repository root. It generates both worker profiles and reports the market sequence, callback count and PID for each. [Two-worker startup](../tickwire-md/examples/spot-market.md)

## Configure a worker

Set `factory.id` to `counting_example` in its Bootstrap. Supply an absolute configuration file, its exact `Object { digest, bytes }` and a positive `revision`. Bind the same object to `StrategyConfig` in the release manifest. Set the separate built-in demo fields to `demo_enabled = false`, `demo_command_file = None` and `demo_minimum_bid_ticks = 0`.

Generate the configuration with `Parameters::bytes()`. It contains schema `1`, `enabled`, `minimum_observations`, `minimum_bid_ticks` and an optional `command`. An enabled strategy takes the canonical bytes of a Spot Limit New command. The template must identify the intended logical strategy, process instance, strategy generation and configuration revision. The decoder rejects unknown fields, noncanonical JSON and unsupported order types.

## Observe, decide and recover

Each live market callback increments `observations`. Ordered account callbacks increment `reports`. After both configured thresholds are met, the strategy stamps the command with the callback time and the actual market trigger, emits it once and records `spent`. The price condition reads the best bid from depth. A quote-only topic can update the worker's market view without triggering this example's order.

The checkpoint carries both counters, the economic action identity and `spent`. Migration at the verified cut C changes the schema and preserves all four values. A later recorded `ConfigApply` changes the target parameters. A different economic action identity fails validation.

```sh
cargo test -p tickwire-strategy-example --all-targets
```

The tests cover canonical parameters, factory construction from verified files, migration of consumed actions and replay of old callbacks between snapshot S and cut C. The process test executes the V1 binary against durable inputs and checks its PID, exit status and complete C checkpoint. [Replay protocol](../../specs/implementation/strategy-replay.md) · [Handover protocol](../../specs/implementation/strategy-handover.md)
