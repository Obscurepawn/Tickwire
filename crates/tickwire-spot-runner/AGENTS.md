# tickwire-spot-runner

`tickwire-spot-runner` (`#![forbid(unsafe_code)]`) multiplexes hosted or file-backed strategies onto one qualified
Bybit or OKX cash-spot `AccountGateway` (`AccountRunner`), persists immutable local-to-venue order origin aliases
(`origins`), reconciles private WebSocket cursors against signed REST snapshots (`reconcile`), and ships the `paper`,
`live`, and `reconcile` CLI entrypoints. `AccountGateway` and `tickwire-runtime-policy` retain sole authority over
balance holds, WAL commits, and first-byte socket gates.

## Invariants and Change Guidelines

* **Single-owner intake, multi-in-flight pooled routing, STP/Modify refusal isolation, and `ProjectedFill`**:
  `AccountRunner` ([src/lib.rs](src/lib.rs)) pairs one `RestSession` with its `RecoveryQueue` and `BlobStore`, tracking
  up to `MAX_ORDER_CONNECTIONS` (`16`) concurrent in-flight intents (`inflight: BTreeMap<IntentKey, u64>`) via
  `poll_tagged` / `poll_all` when pooled while preserving single-connection behavior when unpooled; non-freezing
  per-strategy refusals (`ManagementDenied`, `InsufficientAsset`, `InsufficientCapital`, `Limit`, `CrossesBook`,
  `OrderNotOpen`, `UnknownOrder`, `TooManyPendingActions`, `InvalidPrice`, `InvalidQuantity`, `NoChange`, and
  `SessionError::RateLimited`) record a typed `StrategyRejection` (`TerminalReason`) and return to the originating member
  without calling `stop_sending()` or burning `VenueRateLimiter` tokens, whereas scope/digest mismatches or backwards
  monotonic clocks invoke `stop_sending()`. When `set_projected_fill(true)` is enabled, Bybit `order.spot`
  (`cumExecQty`/`cumExecValue`/`avgPrice`) and OKX `orders` (`accFillSz`/`avgPx`) updates invoke `gateway.on_cum_qty`
  using exact fixed-point arithmetic ahead of trade executions without mutating settled cash or double-counting fills,
  and terminal venue rows (`CancelBySmp`, `EC_PostOnlyWillTakeLiquidity`, OKX `cancelSource`) record classified
  `TerminalReason`s via `order_terminal_reason`.
* **Immutable order-origin registry, rotated `Modify` client-ID aliases, zero-alloc locators, and windowed eviction**:
  Compute SHA-256 origin locators on the stack without heap allocation and bound cached `(IntentKey, order_ids,
  client_id)` mappings (plus rotated `ModifyOrder.new_client_id` aliases via `bind_client_alias`) in
  [src/origins.rs](src/origins.rs) inside preallocated `WindowedTable` pools (`InMemory`, `MmapAsync`, or `LocalDurable`
  via `RuntimeBlobStore`), retiring settled/aborted intents and lazily evicting entries past `retention_window_ns` while
  supporting stateless cold starts from Bybit/OKX `RestEvidence` via `bootstrap_spot_from_rest_evidence` in
  [src/live_bootstrap.rs](src/live_bootstrap.rs) (`SelfDescribingClientId` decoding) so native acknowledgements,
  amendments, and fills route in O(1) to the originating strategy.
* **Zero-disk `InMemory` private stream capture, pooled multi-IP dialers, and `VenueRateLimiter`**: In
  [src/live.rs](src/live.rs) and [src/live/config.rs](src/live/config.rs), pair `StorageMode::InMemory` with
  `StreamConfig::in_memory` (zero ephemeral `/tmp` directories or disk `fsync`s), configure retention windows for
  `InMemory` and `MmapAsync`, attach a background `Dialer` + `LiveNetwork` to `PrivateStream::start_with_network`, and
  when `execution_pool_size > 1` or `!local_ips.is_empty()`, enable pooled `RestSession` slots bound across `local_ips`
  with `VenueRateLimiter` (`BybitSpot` / `OkxSpot`) keeping `Place`/`Modify`, `Cancel`, and `Query` quotas isolated.
* **Per-order `QueryDesk`, `AmendAmbiguous` reconciliation, and off-hot-path `PerOrderQueryDriver`**: Keep
  `query_desk().is_none()` as the default in `AccountRunner` constructors for legacy dual-scan compatibility; when
  enabled via `with_per_order_query_desk` / `enable_per_order_query_desk` (`per_order_query_desk: true` in
  [src/live/config.rs](src/live/config.rs)), isolate ambiguous single-order `Place`, `Cancel`, and `Modify`
  (`DoubtReason::AmendAmbiguous` / `AskKind::Amend`) outcomes and lagging WS `orders` fills
  (`DoubtReason::FillCatchup`) into `QueryDesk` without freezing sibling strategies, resolve `AmendAmbiguous` in
  [src/query_desk_driver.rs](src/query_desk_driver.rs) by comparing queried `(qty/sz, price/px)` against resting order
  terms to emit `VenueEventBody::Modified` (including venue clamping) or `VenueEventBody::ModifyRejected` rollback, gate
  `AsyncRestQuerier` asks against isolated `REQUEST` quota (`try_debit_query_budget` / `with_allow_query`) while leaving
  deferred asks queued via `on_not_sent`, harvest `poll_pool_dials` reconnects, and escalate Category B `AccountHazard`
  or `max_query_count` exhaustion to `freeze_cash()`.
* **Private-cut and REST proof before reactivation**: In [src/reconcile.rs](src/reconcile.rs) and
  [src/live/account.rs](src/live/account.rs), verify the durable private cursor, account WAL frontier, orders, fees, and
  balances before installing a recovered grant; drop qualification immediately if the private stream gaps or expires.
* **Self-contained `paper` simulation**: Keep the local TLS peer ([src/paper_peer.rs](src/paper_peer.rs)), test
  credentials, and CBOR templates ([examples/order-template.cbor](examples/order-template.cbor),
  [examples/baseline-template.cbor](examples/baseline-template.cbor)) confined to `paper` in [src/main.rs](src/main.rs).
* **Idle-window hot-path cache warming (`warmup_interval_ns`)**: `AccountRunner` ([src/lib.rs](src/lib.rs)) and
  `Deployment` ([src/live/config.rs](src/live/config.rs), [src/live.rs](src/live.rs)) keep `warmup_interval_ns == 0`
  disabled by default, record real market/order/fill activity via `record_activity`, refresh the scratch `Account` clone
  only off the hot path after reconciliation, and invoke `maybe_warm_hot_path` strictly on idle reactor turns when
  `active_inflight_len() == 0` so `Account`, `AccountJournal`, `Origins`, strategy `intent_seq`, `VenueRateLimiter`, and
  live TLS sessions remain untouched.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/main.rs](src/main.rs) · [src/live.rs](src/live.rs) ·
  [src/origins.rs](src/origins.rs) · [src/live_bootstrap.rs](src/live_bootstrap.rs) ·
  [src/query_desk_driver.rs](src/query_desk_driver.rs) · [src/reconcile.rs](src/reconcile.rs) ·
  [src/live/account.rs](src/live/account.rs) · [src/producer.rs](src/producer.rs) · [tests/paper.rs](tests/paper.rs) ·
  [tests/first_byte.rs](tests/first_byte.rs) · [tests/hotpath_warmup.rs](tests/hotpath_warmup.rs) ·
  [tests/hotpath_warmup_alloc.rs](tests/hotpath_warmup_alloc.rs) ·
  [tests/in_memory_live.rs](tests/in_memory_live.rs) ·
  [tests/live_bootstrap.rs](tests/live_bootstrap.rs) · [tests/per_order_query_desk.rs](tests/per_order_query_desk.rs) ·
  [tests/multi_connection_router_quota.rs](tests/multi_connection_router_quota.rs) ·
  [tests/modify_projected_fill_stp.rs](tests/modify_projected_fill_stp.rs) ·
  [tests/qualification.rs](tests/qualification.rs) · [tests/live_entry.rs](tests/live_entry.rs) ·
  [tests/reconcile_process.rs](tests/reconcile_process.rs)
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
