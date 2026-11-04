# tickwire-strategy-sdk

The SDK gives a worker its local market replica, ordered account input and canonical command publisher.

Use recorded context for decision time. Keep strategy state and output identity reproducible across replay. Check intent sequence and deadline at publication and again at account intake.

[Crate API](src/lib.rs) · [source replay](tests/source_replay.rs)

## Change a callback

Authors implement `Strategy::on_market` and may handle `on_report` for ordered account updates. A market view borrows the current worker replica. Snapshot, input and command objects carry their canonical bytes into replay. Use the recorded clock and input for a decision that can emit an intent. A changed callback belongs in a replay test that checks the command identity and final checkpoint. [Author API](src/lib.rs) · [Market replica](src/market.rs) · [Intent writer](src/outbound.rs)

The live worker authenticates its lanes and applies an ordered account baseline before processing reports. A report or market transition still in progress must survive restart at its recorded cut. Capture-only startup collects input without running the strategy factory. A signed install binds the old cut, its input tail and the target state before the worker can become active. [Worker](src/worker.rs) · [Registered strategies](src/registered.rs) · [Capture](src/capture.rs)

Specifications include [instrument catalog](../../specs/implementation/instrument-catalog.md), [release preflight](../../specs/implementation/release-preflight.md), [runtime observation](../../specs/implementation/runtime-observation.md), [strategy graceful retirement](../../specs/implementation/strategy-graceful-retirement.md), [strategy handover](../../specs/implementation/strategy-handover.md), [strategy input consumption](../../specs/implementation/strategy-input-consumption.md), [strategy rebuild](../../specs/implementation/strategy-rebuild.md), [strategy replay](../../specs/implementation/strategy-replay.md).

```sh
cargo test --locked -p tickwire-strategy-sdk --all-targets
```
