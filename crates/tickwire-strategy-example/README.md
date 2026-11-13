# Registered Counting Strategy (`tickwire-strategy-example`)

`tickwire-strategy-example` provides a complete, statically registered implementation of [`tickwire_strategy_sdk::Strategy`](../tickwire-strategy-sdk/src/lib.rs) ([`src/lib.rs`](src/lib.rs)). It demonstrates how a supervised worker consumes normalized market updates from its local shared-memory replica, validates runtime-injected parameters, persists deterministic replay checkpoints, and optionally emits a single canonical limit order once observation and price thresholds are satisfied.

---

## Observe a Shared Feed with Two Workers

From the workspace root on Linux, launch two independent instances of the counting worker against one local public quote publisher:

```sh
python3 scripts/run-two-strategies.py
```

Each worker runs in its own OS process with a dedicated shared-memory cursor and checkpoint directory, incrementing its callback counter on every fresh two-sided quote while running in observation-only mode (`enabled: false`). For a minimal in-code example that only computes the bid-ask spread in ticks without emitting orders, see the compiled doc-test in [`tickwire-strategy-sdk`](../tickwire-strategy-sdk/src/lib.rs) and the [shared-feed launcher guide](../tickwire-md/examples/spot-market.md).

---

## Configure Conditional Order Emission

To arm the worker to emit an order in an integration harness or deployment:

1. Set the bootstrap strategy factory identifier to `"counting_example"`.
2. Encode a `Parameters` value via `Parameters::bytes()` and supply its absolute file path, byte length, SHA-256 digest, and positive configuration revision in the worker's bootstrap descriptor:
   * `enabled`: Enables order emission once thresholds are met.
   * `minimum_observations`: Minimum count of accepted market callbacks required before acting.
   * `minimum_bid_ticks`: Minimum best-bid threshold in integer price ticks (evaluated on qualified depth topics; quote-only BBO subscriptions advance the observation counter without depth-bid triggering).
   * `command`: Optional pre-validated `CanonicalCommand` (a Spot limit order bound to this exact strategy instance and risk domain).
3. Once triggered, the worker records the consumed action identity in its checkpoint so deterministic replay or restart never emits a duplicate order.

## Verify

Run the crate's unit/integration suite and the SDK doctests:

```sh
cargo test --locked -p tickwire-strategy-example --all-targets
cargo test --locked -p tickwire-strategy-sdk --doc
```
