# tickwire-account-gateway

`tickwire-account-gateway` (`#![forbid(unsafe_code)]`) coordinates one `Account` owner, its `AccountJournal`,
recovery-closure verification, and outbound REST or WebSocket transport (`AccountGateway`, optional multi-slot
`OrderConnectionPool` via [src/pool.rs](src/pool.rs), and `websocket::WsAccountGateway`). Runtime callers submit
canonical commands, ingest authenticated venue events, and drive `poll` / `poll_slot` to advance connections; wire
signing (`Encoder`), control policy (`Authority`, `ReactivationVerifier`), and socket framing (`tickwire-transport`)
remain injected dependencies with no permissive defaults.

## Invariants and Change Guidelines

* **Commit-profile closure and journal append before output**: In [src/lib.rs](src/lib.rs) and
  [src/binding.rs](src/binding.rs), `submit` reserves exposure in `Account`, verifies dependencies via
  `Authority::prepare`, and commits `Admit` in `AccountJournal` (`InMemory`, `MmapAsync`, or `LocalDurable`) before
  staging output; persistence failures never mutate live account state or emit network bytes.
* **Cold stateless exchange bootstrap**: `VerifiedLiveBootstrap` and `AccountGateway::create_from_live_exchange` in
  [src/lib.rs](src/lib.rs) initialize an active gateway directly from authenticated exchange `Account + OpenOrders`
  snapshots, registering imported synthetic `CanonicalCommand` entries as `Status::Flushed` and enforcing
  `writer_epoch >= max_imported_epoch`.
* **Fresh gate recheck at the first application byte**: Immediately before passing the first application byte to HTTP
  ([src/lib.rs](src/lib.rs)) or WebSocket ([src/websocket.rs](src/websocket.rs)) transport, re-evaluate
  `Authority::commit_state` and `Authority::current_gate` (`now < lease_until`, matching `writer_epoch`,
  `strategy_generation`, `config_revision`, `owner_isolated`, `action_permitted`, and `market_valid`).
* **Pre-first-byte abort vs. `MayHaveSent` retention and opt-in per-order doubt isolation**: Only a pre-first-byte gate
  refusal or transport proof that zero application bytes left the process may append `AbortBeforeSend` and release
  holds; once a single byte is accepted or a send stalls mid-stream, retain the reservation for venue reconciliation.
  When `with_per_order_doubt_isolation(true)` is enabled, `drop_broken_transport_isolated` and `isolate_intent_in_doubt`
  confine single-order `New`/`Cancel`/`AmendKeepPriority`/`Query` transport failures to `Status::Unknown` +
  `Account::mark_order_in_doubt` without freezing `Account`, halting the gateway, or disturbing sibling `Flushed`
  entries, allowing `install_transport_connection` to attach a fresh `Connection` once `retained_receive` is drained.
* **Multi-slot execution pool with single-writer admission**: Enabling `with_connection_pool` / `enable_connection_pool`
  ([src/pool.rs](src/pool.rs)) attaches up to `MAX_ORDER_CONNECTIONS` (`16`) `Exclusive` or `Shared` slots backed by
  `OrderConnectionPool` and automatically enables `per_order_doubt_isolation`. `pick_pool_slot` and
  `pick_pool_slot_filtered` apply allocation-free warmup round-robin and integer EWMA + `9/8` hysteresis selection
  (including per-IP quota predicates); `submit_on_slot` and `poll_slot` preserve sequential `AccountJournal` admission
  and synchronous first-byte `Authority::current_gate` checks on every slot while isolating single-slot drops via
  `drop_broken_slot_isolated` without disturbing sibling slots.
* **Exact-prefix reactivation**: Keep recovery and live reactivation gates in [src/recovery.rs](src/recovery.rs) bound
  to the exact `HistoryFrontier` and `CanonicalBaseline` digest before opening replacement connections.
* **Windowed entry reclamation and management headroom**: In `AccountGateway::evict_retired_entries`
  ([src/lib.rs](src/lib.rs)), reclaim only `Aborted` or `Flushed` entries that are retired in `Account` and past
  `retention_window_ns` (never `AwaitingCommit`, `Queued`, `PartiallyAccepted`, or `Unknown`), remove matching
  `completed_mono_ns` stamps without leaking, and keep `Cancel`/`Query`/`AmendKeepPriority` unblocked when all
  `max_intents` order slots hold live resting orders.

## Verification and Specifications

* **Source and tests**: [src/lib.rs](src/lib.rs) · [src/pool.rs](src/pool.rs) · [src/binding.rs](src/binding.rs) ·
  [src/recovery.rs](src/recovery.rs) · [src/websocket.rs](src/websocket.rs) ·
  [tests/submission.rs](tests/submission.rs) · [tests/per_order_isolation.rs](tests/per_order_isolation.rs) ·
  [tests/multi_slot_pool.rs](tests/multi_slot_pool.rs) ·
  [tests/windowed_eviction.rs](tests/windowed_eviction.rs) ·
  [tests/lifecycle.rs](tests/lifecycle.rs) ·
  [tests/live_bootstrap.rs](tests/live_bootstrap.rs) ·
  [tests/binance_rest.rs](tests/binance_rest.rs) · [tests/websocket.rs](tests/websocket.rs) ·
  [tests/ingress.rs](tests/ingress.rs) · [tests/income_closure.rs](tests/income_closure.rs) ·
  [tests/recovery_metadata.rs](tests/recovery_metadata.rs) ·
  [tests/persistence_failures.rs](tests/persistence_failures.rs)
* **Specifications and models**: [account-gateway.md](../../specs/implementation/account-gateway.md) ·
  [account-recovery-binding.md](../../specs/implementation/account-recovery-binding.md) ·
  [account_gateway](../../tla/execution_models/account_gateway/ROADMAP.md) ·
  [spot_roundtrip](../../tla/execution_models/spot_roundtrip/ROADMAP.md) ·
  [binance_roundtrip](../../tla/execution_models/binance_roundtrip/ROADMAP.md)

```sh
cargo test --locked -p tickwire-account-gateway --all-targets
./scripts/check-tla.sh account_gateway
```
