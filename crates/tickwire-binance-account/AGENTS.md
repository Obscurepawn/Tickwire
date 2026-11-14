# tickwire-binance-account

`tickwire-binance-account` (`#![forbid(unsafe_code)]`) owns durable Binance private-stream ingestion
(`AccountIngress`), local-to-native order identity registration, REST/WebSocket response correlation (`rest`, `ws`),
USD-M account-level cashflow normalization (`account_facts`), and multi-step startup/disconnect reconciliation
(`reconciliation`). Runtime hosts feed authenticated private events and signed query responses into `AccountIngress`,
which journals raw wire evidence before applying bound `CanonicalEvent` or `CanonicalAccountInput` transitions to
`AccountGateway`.

## Invariants and Change Guidelines

* **Journal raw private evidence before state mutation**: `AccountIngress` in [src/lib.rs](src/lib.rs) and
  [src/codec.rs](src/codec.rs) persists every authenticated private message (`raw`, `connection_epoch`, `InputKey`,
  `TimeUnit`, and `StoredDecision`) into `tickwire-journal` within `MAX_RECORD_BYTES` (`60 KiB`) before mutating
  `AccountGateway` so recovery replays the exact arrival sequence.
* **Strict order registration, $O(\log N)$ indexed correlation, native `Modify`, and `ProjectedFill`**: Correlate venue
  reports in [src/registry.rs](src/registry.rs), [src/modify.rs](src/modify.rs), [src/amendment.rs](src/amendment.rs),
  and [src/structure.rs](src/structure.rs) against admitted `CanonicalCommand` registrations via `OrderRegistry`'s
  secondary indices (`by_client_id`, `by_venue_id`, `by_list_client_id`, `by_algo_client_id`, `by_symbol`) and
  alias-preserving `Registration` records without cloning the registry on private WS frames; quarantine unknown,
  unadmitted, or conflicting reports (`QuarantineReason`) rather than fabricating local order identities. Bind native
  `CommandBody::Modify` REST/WS replies, structured modify rejections (`VenueEventBody::ModifyRejected` with classified
  `TerminalReason`), private-stream `AMENDMENT`/modified `(q, p)` updates (`VenueEventBody::Modified` preserving
  venue-clamped `original_quantity` and `price`), and `AmendAmbiguous` REST status requeries without freezing peer
  orders. When `RestOwner::set_projected_fill(true)` is enabled, extract Spot `Z` (`CumulativeQuote`) and USD-M
  `ap`/`avgPrice` (`AveragePrice`) via `gateway.on_cum_qty` before applying non-trade order observations so early
  `ProjectedFill` deltas never double-count later `TRADE` settlements. Retire expired registrations, WS binding slots,
  and `InMemory`/`MmapAsync` prefix records past `retention_window_ns` via `evict_expired` while keeping active orders
  pinned.
* **WebSocket multiplexing, pooled REST routing, multi-IP quota, and per-order `QueryDesk` isolation**: Keep
  single-connection and pooled (`with_connection_pool` / `OrderConnectionPool`) REST replies in [src/rest.rs](src/rest.rs)
  and up to `MAX_SHARED_INFLIGHT` (`16`) concurrent `WsRequestId`-correlated WebSocket trading requests in
  [src/ws.rs](src/ws.rs) and [src/ws/owner.rs](src/ws/owner.rs) tied to their durable admission digests and session
  epochs, including out-of-order replies and late trade fills arriving after terminal order statuses. When
  `VenueRateLimiter` is attached (`with_venue_rate_limiter`), enforce class-isolated budgets (`Cancel` and `Query`
  never check or debit `ORDER` caps), reconcile `X-MBX-*` / `Retry-After` / WS `"rateLimits"` counters, and fail over
  across healthy `local_ip` pool slots when a single source IP enters HTTP `429` cooldown. When
  `RestOwner::with_per_order_query_desk` / `enable_per_order_query_desk` activates per-order doubt isolation, emit
  deterministic `VenueEventBody::Rejected` events on single-order `CommandBody::New` creation rejections, isolate
  Category A single-order transport/status ambiguities and per-IP `429` cooldowns (`PlaceAmbiguous`,
  `CancelAmbiguous`, `AmendAmbiguous`, `FillCatchup`) without freezing sibling orders or links, and resolve due
  `QueryDesk` asks off the hot path via `PerOrderQueryDriver` in [src/query_desk_driver.rs](src/query_desk_driver.rs)
  over `AsyncRestQuerier` while still freezing immediately on Category B account-wide hazards (`418`, `401`/`403`,
  `-1021`, single-IP `429`, stream gaps/quarantines, `OrderStatus::Unknown`, multi-leg ambiguities, or
  `max_query_count` exhaustion).
* **Evidence-complete reconciliation, keep-alive reuse, and cold bootstrap**: Require snapshot, open/conditional order,
  trade, amendment, structure, and USD-M income closures in [src/reconciliation.rs](src/reconciliation.rs) and
  [src/account_facts.rs](src/account_facts.rs) to converge against the exact account prefix before reactivating
  risk-increasing admissions, reuse keep-alive TLS sockets and `DialBaton` pre-warmed connections via `WireReadPool` in
  [src/reconciliation/wire.rs](src/reconciliation/wire.rs), and support stateless cold starts via
  `bootstrap_from_remote_snapshots` in [src/reconciliation/live_bootstrap.rs](src/reconciliation/live_bootstrap.rs) and
  `AccountIngress::create_from_live_exchange`.

## Verification and Specifications

* **Source entrypoints**: [src/lib.rs](src/lib.rs) · [src/modify.rs](src/modify.rs) ·
  [src/query_desk_driver.rs](src/query_desk_driver.rs) ·
  [src/reconciliation.rs](src/reconciliation.rs) ·
  [src/reconciliation/wire.rs](src/reconciliation/wire.rs) ·
  [src/reconciliation/live_bootstrap.rs](src/reconciliation/live_bootstrap.rs) ·
  [src/rest.rs](src/rest.rs) · [src/ws.rs](src/ws.rs) · [src/account_facts.rs](src/account_facts.rs) ·
  [src/registry.rs](src/registry.rs)
* **Integration tests**: [tests/account_observation.rs](tests/account_observation.rs) ·
  [tests/hot_path_and_eviction.rs](tests/hot_path_and_eviction.rs) ·
  [tests/live_bootstrap.rs](tests/live_bootstrap.rs) ·
  [tests/modify_projected_fill_stp.rs](tests/modify_projected_fill_stp.rs) ·
  [tests/multi_connection_router.rs](tests/multi_connection_router.rs) ·
  [tests/network_ingress.rs](tests/network_ingress.rs) ·
  [tests/per_order_query_desk.rs](tests/per_order_query_desk.rs) · [tests/persistence.rs](tests/persistence.rs)
* **Specifications and models**: [binance-ws-trading.md](../../specs/implementation/binance-ws-trading.md) ·
  [reconciliation-controller.md](../../specs/implementation/reconciliation-controller.md) ·
  [spot-amend.md](../../specs/implementation/spot-amend.md) ·
  [usdm-account-facts.md](../../specs/implementation/usdm-account-facts.md) ·
  [binance_roundtrip](../../tla/execution_models/binance_roundtrip/ROADMAP.md)

```sh
cargo test --locked -p tickwire-binance-account --all-targets
./scripts/check-tla.sh binance_roundtrip
```
