# tickwire-strategy-sdk

`tickwire-strategy-sdk` owns the worker-side `Strategy` trait, local `MarketReplica` / `MarketView` wrapper ([src/market.rs](src/market.rs)),
durable command store and SPSC `IntentWriter` / `OutboundWriter` ([src/outbound.rs](src/outbound.rs)), ordered `ReportReceiver` / `ReportWriter`
([src/reports.rs](src/reports.rs)), microstructure report views and pure-`Fixed` `FillWatermarkTracker` ([src/microstructure.rs](src/microstructure.rs)),
checkpoints ([src/checkpoint.rs](src/checkpoint.rs), [src/account_checkpoint.rs](src/account_checkpoint.rs)),
deterministic handover replay ([src/replay.rs](src/replay.rs), [src/restore.rs](src/restore.rs)), and worker event loop
([src/worker.rs](src/worker.rs), [src/registered.rs](src/registered.rs)). Crate-wide `#![deny(unsafe_code)]` isolates SHM attachment inside
`reports`, `runtime`, and `worker`; parent process supervision and risk admission live in `tickwire-strategy-host` and `tickwire-account`.

## Invariants and Change Guidelines

* **Command-before-ring publication, `ModifyRequest`, and self-describing IDs**: `IntentWriter` (`OutboundWriter` in [src/outbound.rs](src/outbound.rs))
  commits the `CanonicalCommand` blob in `CommandStore` (`RuntimeBlobStore`: `InMemory`, `MmapAsync`, or
  `LocalDurable`)—bound to its triggering input cursor, `now_ns` decision timestamp, and monotonic strategy output
  sequence—before publishing the `IntentRecord` reference onto the shared-memory ring (`command_kind = 5`, `message_type = 517`, and
  `recovery::order_count == 0` for `CommandBody::Modify`), validates positive price/quantity in `ModifyRequest`, and provides
  `mint_self_describing_client_id` so outbound orders carry `SelfDescribingClientId` attribution on the venue wire.
* **Microstructure report decoding and zero-double-counting `FillWatermarkTracker`**: `AccountReport::decode` ([src/reports.rs](src/reports.rs),
  [src/microstructure.rs](src/microstructure.rs)) surfaces `ModifyObservation`, `ModifyRejection`, and classified `TerminalReason` values directly
  from `CanonicalRecord` without string/CBOR re-parsing. `FillWatermarkTracker` uses checked `tickwire_risk::Fixed` arithmetic exclusively
  (zero `f32`/`f64`) to reconcile early `ProjectedFill` / `OrderObservation` cumulative quantities with deduplicated `FillEvent` executions so
  inventory skew never double-counts across arbitrary status/trade interleavings, while `Strategy::on_projected_fill` defaults to a no-op.
* **Freshness, zero-copy market consumption, and `crc32` feature gate**: `MarketReplica::drain_recorded`
  ([src/market.rs](src/market.rs)), legacy worker rings ([src/worker.rs](src/worker.rs)), and expansion rings
  ([src/worker/expansion.rs](src/worker/expansion.rs)) consume `MarketStateRef` (`&MarketRecord` / `&MarketExactRecord`)
  in place from `RecordBorrow::as_market_state` without 512-byte stack decodes. `MarketView` exposes borrowed local
  replica state to `Strategy::on_market` only after JOIN baseline and catch-up records are journaled, rejecting stale
  or degraded (`feed_state != 2`) feeds; ordered account execution updates arrive exclusively through `on_report`
  ([src/reports.rs](src/reports.rs)). Optional `--features crc32` (`default = ["runtime"]` leaves `crc32` OFF) gates
  checksum flag `0x08` and CRC32C computation on `IntentWriter` and `ReportWriter`.
* **Deterministic replay and handover seals**: During migration or restart in [src/replay.rs](src/replay.rs), [src/restore.rs](src/restore.rs),
  and [src/recovery.rs](src/recovery.rs), feed the original recorded inputs and decision timestamps rather than wall-clock reads, and verify
  `HandoverSeal` checkpoint and account-report parity before live cutover.
* **Non-world-writable config reads**: `worker::read_config` ([src/worker.rs](src/worker.rs)) opens bootstrap files with
  `O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK` and rejects group/world-writable modes (`0o022`).

## Verification and Specifications

* **Source and binaries**: [src/lib.rs](src/lib.rs) · [src/market.rs](src/market.rs) · [src/outbound.rs](src/outbound.rs) ·
  [src/reports.rs](src/reports.rs) · [src/microstructure.rs](src/microstructure.rs) · [src/worker.rs](src/worker.rs) ·
  [src/registered.rs](src/registered.rs) · [src/replay.rs](src/replay.rs) ·
  [src/restore.rs](src/restore.rs) · [src/bin/tickwire-strategy-worker.rs](src/bin/tickwire-strategy-worker.rs)
* **Integration tests**: [tests/source_replay.rs](tests/source_replay.rs) · [tests/microstructure_sdk.rs](tests/microstructure_sdk.rs)
* **Specifications and models**: [strategy-input-consumption.md](../../specs/implementation/strategy-input-consumption.md) ·
  [strategy-parameter-consumption.md](../../specs/implementation/strategy-parameter-consumption.md) ·
  [single-host-trading.md](../../specs/implementation/single-host-trading.md) ·
  [recovery-closure.md](../../specs/implementation/recovery-closure.md) ·
  [worker_handover](../../tla/execution_models/worker_handover/ROADMAP.md) ·
  [quote_delivery](../../tla/execution_models/quote_delivery/ROADMAP.md)

```sh
cargo test --locked -p tickwire-strategy-sdk --all-targets
cargo test --locked -p tickwire-strategy-sdk --doc
./scripts/check-tla.sh worker_handover
```
