# tickwire-strategy-example

`tickwire-strategy-example` owns the reference `Counting` strategy implementation
(`Registration::v1` and `Registration::v2` with distinct state schemas `V1` and `V2`) and the two
worker binaries (`tickwire-counting-worker` and `tickwire-counting-v1-worker`) exercised by the
multi-strategy launcher and handover replay tests. Library code enforces `#![deny(unsafe_code)]`;
only the binary entrypoints discharge `tickwire_strategy_sdk::registered::run` under qualified host
supervision, while market transport, checkpoint storage, and risk admission remain in the SDK, host,
and account crates.

## Invariants and Change Guidelines

* **Canonical parameter and state round-tripping**: `Parameters::decode` and `State::decode` in
  [src/lib.rs](src/lib.rs) require `#[serde(deny_unknown_fields)]`, exact byte round-trip equality
  (`bytes()? == bytes`), non-zero `minimum_observations`, and `Spot` `Limit` `CanonicalCommand`
  validation whenever `command` is configured.
* **Single-shot intent emission and schema migration**: `Counting` increments `observations` on live
  `MarketView` callbacks and `reports` on ordered account callbacks, emitting its configured
  `CanonicalCommand` at most once (`spent == true` bound to `action` digest). Preserve `V1` -> `V2`
  state-schema upgrade and deterministic source replay behavior tested in
  [tests/source_replay.rs](tests/source_replay.rs).
* **Minimal binary entrypoints and `crc32` forwarding**: Keep
  [src/bin/tickwire-counting-worker.rs](src/bin/tickwire-counting-worker.rs) and
  [src/bin/tickwire-counting-v1-worker.rs](src/bin/tickwire-counting-v1-worker.rs) thin wrappers
  around `registered::run` without ambient I/O or trading authority, and forward optional
  `--features crc32` (`default = []`) to `tickwire-strategy-sdk/crc32`.

## Verification and Specifications

* **Source, binaries, and guide**: [src/lib.rs](src/lib.rs) ·
  [src/bin/tickwire-counting-worker.rs](src/bin/tickwire-counting-worker.rs) ·
  [src/bin/tickwire-counting-v1-worker.rs](src/bin/tickwire-counting-v1-worker.rs) ·
  [README.md](README.md) · [Deploy two strategies](../../README.md#deploy-two-strategies)
* **Integration tests**: [tests/source_replay.rs](tests/source_replay.rs)
* **Specifications**:
  [strategy-input-consumption.md](../../specs/implementation/strategy-input-consumption.md) ·
  [strategy-parameter-consumption.md](../../specs/implementation/strategy-parameter-consumption.md)

```sh
cargo test --locked -p tickwire-strategy-example --all-targets
python3 scripts/run-two-strategies.py
```
