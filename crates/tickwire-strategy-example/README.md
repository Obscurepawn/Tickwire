# Registered Counting Strategy (`tickwire-strategy-example`)

`tickwire-strategy-example` provides a complete, statically registered stateless implementation of [`tickwire_strategy_sdk::Strategy`](../tickwire-strategy-sdk/src/lib.rs) ([`src/lib.rs`](src/lib.rs)) under `#![forbid(unsafe_code)]`. It demonstrates how a strategy worker validates its immutable SHA-256-bound startup parameter file (`FactoryBinding`), subscribes to `tickwire-md` shared-memory rings at an independent reader cursor, authenticates with the ORS execution process over `DOP`, and optionally emits a single inline 1,024-byte `IntentRecord` once observation and bid-price thresholds are satisfied—with zero local persistence.

---

## Observe a Shared Feed with Two Workers

From the workspace root on Linux, launch two independent instances of the counting worker against one local public quote publisher:

```sh
python3 scripts/run-two-strategies.py
```

Each worker runs in its own OS process with a dedicated shared-memory reader cursor and private in-memory algorithm state, incrementing its observation counter on every fresh quote while running in observation-only mode (`enabled: false`). For a minimal in-code example that computes the bid-ask spread in integer ticks without emitting orders, see the compiled doc-test in [`tickwire-strategy-sdk`](../tickwire-strategy-sdk/src/lib.rs) and the [shared-feed walkthrough](../tickwire-md/examples/spot-market.md).

---

## Configure Conditional Order Emission

To arm the worker to emit an order in an integration harness or deployment:

1. Set the bootstrap `FactoryBinding` registration identifier to `"counting_example"` and `state_schema` to `V2` (`tickwire-counting-worker`) or `V1` (`tickwire-counting-v1-worker`).
2. Encode a `Parameters` value via `Parameters::bytes()` and supply its absolute file path, byte length, SHA-256 digest (`ArtifactDigest::of`), and positive configuration revision in `Bootstrap.factory`:
   * `enabled`: Enables single-shot order emission once thresholds are met.
   * `minimum_observations`: Minimum count of accepted market callbacks required before acting.
   * `minimum_bid_ticks`: Minimum best-bid threshold in integer price ticks (`>= 0`).
   * `command`: Optional pre-validated `CanonicalCommand` (`Spot` `Limit` order matching `(logical_strategy_id, instance_id, generation, revision)`).
3. Once triggered, `Counting` sets `spent = true` in private memory and publishes a single inline 1,024-byte `IntentRecord` propagating `tick_mono_ns` from the triggering quote; on restart, `DopSession` adopts `DopEcho.next_source_order_seqnum` from ORS so `order_seqnum` never collides.

## Verify

Run the crate's unit/integration suite and the SDK doctests:

```sh
cargo test --locked -p tickwire-strategy-example --all-targets
cargo test --locked -p tickwire-strategy-sdk --doc
```
