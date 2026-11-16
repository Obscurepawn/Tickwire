# Stateless UTA Exchange REST Bootstrap and `OrsQueryDesk` Reconciliation

[`tickwire-spot-execution`](../../crates/tickwire-spot-execution/src/rest_bootstrap.rs) recovers Bybit V5 and OKX V5 Unified Trading Account (UTA) sessions on cold startup and restart exclusively from authenticated Exchange REST snapshots (`Account + Positions + OpenOrders`) paired with the 32-character `[0-9a-z]` [`SelfDescribingClientId`](../../crates/tickwire-venue/src/client_id.rs) (`tw1...`), and resolves in-flight order ambiguity at runtime through per-account [`OrsQueryDesk`](../../crates/tickwire-account/src/ors_query_desk.rs) `StatusAsk` queries executed on cold housekeeping threads via [`AsyncRestQuerier`](../../crates/tickwire-transport/src/rest_querier.rs). Zero local write-ahead logs, journals, manifests, or disk recovery files exist anywhere in the lifecycle.

## Cold Startup Exchange Snapshot Recovery

Before enabling live strategy `REQ` admission on a `BybitUtaSession` or `OkxUtaSession`, cold startup collects an authoritative point-in-time Exchange REST snapshot across the account's configured UTA products:

1. **Authenticated REST snapshot endpoints**:
   * **Bybit V5 UTA**:
     * Wallet collateral & locks: `GET /v5/account/wallet-balance?accountType=UNIFIED`
     * Derivative & option positions: `GET /v5/position/list?category={linear|inverse|option}`
     * Active open orders: `GET /v5/order/realtime?category={spot|linear|inverse|option}` (paginated via `nextPageCursor` when `spot` `openOnly=0` or `settleCoin` / symbol filters are supplied)
   * **OKX V5 UTA**:
     * Wallet collateral & locks: `GET /api/v5/account/balance`
     * Derivative & option positions: `GET /api/v5/account/positions` (optional `instType={SWAP|FUTURES|OPTION}`)
     * Active open orders: `GET /api/v5/trade/orders-pending` (optional `instType={SPOT|SWAP|FUTURES|OPTION}`, paginated via `after`)
2. **Exact fixed-point normalization (`UtaBootstrapDecoder`)**:
   * Wallet coins (`coin` / `ccy`) map through the configured `AssetKey { wallet_id, asset_id }` and asset decimal scale into [`OrsRestBalanceRow`](../../crates/tickwire-account/src/order_manager.rs) (`free_atoms`, `locked_atoms`) using exact integer scaling with zero floating-point math.
   * Net and hedge-mode derivative/option positions (`size` + `side` on Bybit; `pos` + `posSide` on OKX) normalize into signed integer lot [`OrsRestPositionRow`](../../crates/tickwire-account/src/order_manager.rs) entries keyed by `symbol_id`.
   * Active open orders normalize into [`OrsRestOpenOrderRow`](../../crates/tickwire-account/src/order_manager.rs) (`symbol_id`, `client_id`, `venue_order_id`, `side`, `tif` including `TimeInForce::Rpi`, `post_only`, `reduce_only`, `price_ticks`, `orig_qty_lots`, `cum_filled_lots`).
3. **Attribution and sequence-floor reconstruction (`OrderManager::bootstrap_from_exchange`)**:
   * Every open order carrying a valid domain-keyed `tw1...` `SelfDescribingClientId` for the configured `(venue, account, environment, product)` domain is attributed back to its originating `(strat_id, order_seqnum)`, inserted into its deterministic power-of-two `OrderSlot` and `OrsStpBook`, and advances that strategy's monotonic sequence floor so post-restart orders never collide with pre-restart client IDs.
   * Unattributed external open orders remain accounted in venue-locked balances (`external_open_orders`) without corrupting strategy slots.
   * If any required REST snapshot HTTP status is non-`2xx`, any venue status code is non-zero, or any asset/symbol scale fails fixed-point bounds validation, cold bootstrap fails closed before opening order routing.

## Runtime Per-Order `OrsQueryDesk` Reconciliation

When a cancel NACK, ungraceful socket drop with unacknowledged in-flight commands, or response timeout leaves a specific order's venue state uncertain, only that order enters per-order inquiry in [`OrsQueryDesk`](../../crates/tickwire-account/src/ors_query_desk.rs) while unrelated orders on the account continue trading without interruption:

1. **Off-hot-path signed REST builders (`UtaRestRequestBuilder` & `lookup_status_ask_target`)**:
   * Implements [`RestRequestBuilder`](../../crates/tickwire-transport/src/rest_querier.rs) for [`AsyncRestQuerier`](../../crates/tickwire-transport/src/rest_querier.rs) on cold housekeeping CPUs, spending only the `REQUEST` + `PER_IP` query quota (`OrsAccountQuota::allow_query`, strictly isolated from `ORDER` and `CANCEL` caps).
   * Reads `(account_ref, symbol_id, client_id, venue_order_id)` directly from `OrderManager::lock_order(ors_order_id)` via `lookup_status_ask_target` (copying out inline ASCII identifiers and immediately releasing the fine-grained per-order spinlock before resolving `UtaSymbolBinding` and signing HTTP headers), requiring zero auxiliary `Mutex` registry on `REQ` or `RESP`.
   * **Bybit V5 single-order `StatusAsk`**: `GET /v5/order/realtime?category={spot|linear|inverse|option}&symbol=...&orderLinkId=...` (with `&orderId=...` when known, `GET /v5/order/history` fallback when an order has aged out of the realtime cache, plus `GET /v5/execution/list` when individual trade attribution is requested).
   * **OKX V5 single-order `StatusAsk`**: `GET /api/v5/trade/order?instId=...&clOrdId=...` (with `&ordId=...` when known, plus optional `GET /api/v5/trade/fills`).
2. **Reconciliation (`decode_status_ask_snapshot` -> `OrderManager::reconcile_query_snapshot`)**:
   * Decodes the REST reply into [`OrsOrderQuerySnapshot`](../../crates/tickwire-account/src/order_manager.rs) (`Open`, `PartiallyFilled`, `Filled`, `Canceled`, `Expired`, or `NotFound`) alongside any individual `OrsFillObservation` rows.
   * Individual trade fills apply through `FillRecordPool` deduplication strictly before terminal remainder release so fill fees, positions, and wallet holds settle once across WebSocket notifications and REST queries.
   * If `reconcile_query_snapshot` returns `OrsQueryReconcileAction::DispatchCancel` (e.g., an order with a queued cancel confirmed `Open`), the session immediately dispatches the cancel over the UTA connection pool; inconclusive HTTP/transport errors call `OrderManager::on_query_inconclusive` for bounded exponential backoff retry.

## Stateless Read-Only Observation (`tickwire-spot-observe`)

[`tickwire-spot-observe`](../../crates/tickwire-spot-execution/src/bin/tickwire-spot-observe.rs) provides a stateless diagnostic CLI over [`src/observe.rs`](../../crates/tickwire-spot-execution/src/observe.rs) (`order`, `open-orders`, `fills`, `balance`, `positions`, `scan`, and bounded `watch`) across Bybit V5 and OKX V5 UTA categories without touching local files or mutating account state.

## Verification

```sh
cargo test --locked -p tickwire-spot-execution --test uta_recovery_and_rfq --test observations
```

## References

* Implementation files: [REST bootstrap and `OrsQueryDesk` builders](../../crates/tickwire-spot-execution/src/rest_bootstrap.rs), [UTA sessions](../../crates/tickwire-spot-execution/src/session.rs), [read-only observer](../../crates/tickwire-spot-execution/src/observe.rs), [observer CLI](../../crates/tickwire-spot-execution/src/bin/tickwire-spot-observe.rs), [recovery and RFQ integration tests](../../crates/tickwire-spot-execution/tests/uta_recovery_and_rfq.rs)
* Related specifications: [Bybit V5 and OKX V5 Unified Trading Account execution](spot-execution.md)
