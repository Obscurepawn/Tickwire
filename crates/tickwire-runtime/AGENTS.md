# tickwire-runtime

`tickwire-runtime` (`#![forbid(unsafe_code)]`) owns the single-threaded trading event loop (`node`), local
`RecoveryManifest` staging and verification (`authority::LocalAuthority`, `authority::RecoveryQueue`), and startup
file/catalog loaders (`files`) that wire clock calibration, public market data, private account ingress, and
`AccountGateway` together. Executable assemblies (`tickwire-app` and `tickwire-spot-runner`) instantiate and poll this
node; child-process spawning, shared-memory mappings, and policy grant rules live in `tickwire-app` and
`tickwire-runtime-policy`.

## Invariants and Change Guidelines

* **Poll-order invariant and bounded multi-completion drain**: Each reactor turn in [src/node.rs](src/node.rs) must
  poll clock calibration, public market streams, private account inputs, and up to `16` queued `RestOwner` completions
  (`has_queued_outcomes`, marking `report.runnable = true` if any remain) before polling `Strategy` for new
  submissions, then sample fresh `LocalTime` immediately prior to admission so fills and cancellations across pooled
  HTTP/1.1 slots (`mio::Token(100 + conn_id)` via `RestOwner::reregister_all`) or multiplexed WebSocket requests settle
  available funds in the same turn.
* **Staged `RecoveryBundle` completeness**: `RecoveryQueue::stage` and `LocalAuthority` in
  [src/authority.rs](src/authority.rs) require all 10 control/configuration/checkpoint dependency objects
  (`kinds 2..=11`), actual sequenced `EncodedInputs`, command object (`kind 13`), and paired account baseline/history
  (`14`/`15`) synchronized in `BlobStore` before returning a `VerifiedClosure`, and verify worker handover chains in
  [src/authority/historical.rs](src/authority/historical.rs).
* **Administrative bypass during degraded `Health`**: `Health::intake` in [src/node.rs](src/node.rs) holds new
  risk-increasing orders in `Intake::Wait` whenever `!market_live`, `!private_receiving`, or `account_frozen`, while
  still forwarding `Cancel`, `CancelAlgo`, `CancelList`, and `Query` commands as long as the clock is calibrated and
  the sender is not halted.
* **Startup-only storage-mode and catalog wiring**: Keep `StorageMode` selection (`InMemory`, `MmapAsync`,
  `LocalDurable`), `OpenMode` (`Create`, `CreateFromLiveExchange`, `RecoverFrozen`), and JSON/CBOR baseline, credential,
  and `tickwire-instrument-catalog` loading in [src/files.rs](src/files.rs) and [src/files/catalog.rs](src/files/catalog.rs)
  strictly on the startup path, never inside per-quote or per-order turns.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/node.rs](src/node.rs) ·
  [src/node/strategy.rs](src/node/strategy.rs) · [src/authority.rs](src/authority.rs) ·
  [src/authority/historical.rs](src/authority/historical.rs) · [src/files.rs](src/files.rs) ·
  [src/files/catalog.rs](src/files/catalog.rs)
* **Integration tests**: [tests/handover_closure.rs](tests/handover_closure.rs)
* **Specifications and models**: [single-host-trading.md](../../specs/implementation/single-host-trading.md) ·
  [account-gateway.md](../../specs/implementation/account-gateway.md) ·
  [binance_roundtrip](../../tla/execution_models/binance_roundtrip/ROADMAP.md)

```sh
cargo test --locked -p tickwire-runtime --all-targets
```
