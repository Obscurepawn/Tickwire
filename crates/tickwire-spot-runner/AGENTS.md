# tickwire-spot-runner

`tickwire-spot-runner` (`#![forbid(unsafe_code)]`) multiplexes hosted or file-backed strategies onto one qualified
Bybit or OKX cash-spot `AccountGateway` (`AccountRunner`), persists immutable local-to-venue order origin aliases
(`origins`), reconciles private WebSocket cursors against signed REST snapshots (`reconcile`), and ships the `paper`,
`live`, and `reconcile` CLI entrypoints. `AccountGateway` and `tickwire-runtime-policy` retain sole authority over
balance holds, WAL commits, and first-byte socket gates.

## Invariants and Change Guidelines

* **Single-owner intake and peer isolation**: `AccountRunner` ([src/lib.rs](src/lib.rs)) pairs one `RestSession` with
  its `RecoveryQueue` and `BlobStore`; non-freezing per-strategy refusals (`ManagementDenied`, `InsufficientAsset`,
  `InsufficientCapital`, `Limit`) return to the originating member while peers continue, whereas scope/digest
  mismatches or backwards monotonic clocks invoke `stop_sending()`.
* **Immutable order-origin registry, zero-alloc locators, and windowed eviction**: Compute SHA-256
  origin locators on the stack without heap allocation and bound cached `(IntentKey, order_ids,
  client_id)` mappings in [src/origins.rs](src/origins.rs) inside preallocated `WindowedTable` pools
  (`InMemory`, `MmapAsync`, or `LocalDurable` via `RuntimeBlobStore`), retiring settled/aborted
  intents and lazily evicting entries past `retention_window_ns` while supporting stateless cold
  starts from Bybit/OKX `RestEvidence` via `bootstrap_spot_from_rest_evidence` in
  [src/live_bootstrap.rs](src/live_bootstrap.rs) (`SelfDescribingClientId` decoding) so native
  acknowledgements and fills route in O(1) to the originating strategy.
* **Zero-disk `InMemory` private stream capture and background reconnects**: In [src/live.rs](src/live.rs),
  pair `StorageMode::InMemory` with `StreamConfig::in_memory` (zero ephemeral `/tmp` directories or
  disk `fsync`s), configure `AccountJournal` / `AccountGateway` / `AccountRunner` retention windows
  for `InMemory` and `MmapAsync`, and attach a background `Dialer` + `LiveNetwork` to
  `PrivateStream::start_with_network` so WebSocket TCP+TLS reconnects pre-warm off the hot path.
* **Private-cut and REST proof before reactivation**: In [src/reconcile.rs](src/reconcile.rs) and
  [src/live/account.rs](src/live/account.rs), verify the durable private cursor, account WAL frontier, orders, fees, and
  balances before installing a recovered grant; drop qualification immediately if the private stream gaps or expires.
* **Self-contained `paper` simulation**: Keep the local TLS peer ([src/paper_peer.rs](src/paper_peer.rs)), test
  credentials, and CBOR templates ([examples/order-template.cbor](examples/order-template.cbor),
  [examples/baseline-template.cbor](examples/baseline-template.cbor)) confined to `paper` in [src/main.rs](src/main.rs).

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/main.rs](src/main.rs) · [src/live.rs](src/live.rs) ·
  [src/origins.rs](src/origins.rs) · [src/live_bootstrap.rs](src/live_bootstrap.rs) ·
  [src/reconcile.rs](src/reconcile.rs) · [src/live/account.rs](src/live/account.rs) ·
  [src/producer.rs](src/producer.rs) · [tests/paper.rs](tests/paper.rs) ·
  [tests/first_byte.rs](tests/first_byte.rs) · [tests/in_memory_live.rs](tests/in_memory_live.rs) ·
  [tests/live_bootstrap.rs](tests/live_bootstrap.rs) · [tests/qualification.rs](tests/qualification.rs) ·
  [tests/live_entry.rs](tests/live_entry.rs) · [tests/reconcile_process.rs](tests/reconcile_process.rs)
* **Specifications and models**: [spot-strategy-routing.md](../../specs/implementation/spot-strategy-routing.md) ·
  [spot-execution.md](../../specs/implementation/spot-execution.md) ·
  [spot-rest-recovery.md](../../specs/implementation/spot-rest-recovery.md) ·
  [spot_account_recovery](../../tla/execution_models/spot_account_recovery/README.md) ·
  [spot_roundtrip](../../tla/execution_models/spot_roundtrip/ROADMAP.md) ·
  [strategy_intake](../../tla/execution_models/strategy_intake/ROADMAP.md)

```sh
cargo test --locked -p tickwire-spot-runner --all-targets
cargo run --locked -p tickwire-spot-runner -- paper okx "$(mktemp -d)/account"
```
