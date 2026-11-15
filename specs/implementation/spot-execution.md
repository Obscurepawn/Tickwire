# Bybit V5 and OKX V5 Unified Trading Account Execution

[`tickwire-spot-execution`](../../crates/tickwire-spot-execution/src/lib.rs) implements stateless, full-product Unified Trading Account (UTA) execution for Bybit V5 (`BybitUtaSession`) and OKX V5 (`OkxUtaSession`) inside the unified ORS execution process (`1 ORS process = 1 Venue`, `1 AccountSession = 1 Exchange UID`). Both sessions cover cash and margin Spot (`Product::Spot`), linear and inverse perpetuals (`Product::LinearPerp`, `Product::InversePerp`), linear and inverse dated futures (`Product::LinearFutures`, `Product::InverseFutures`), and European options (`Product::Options`) over a shared multi-IP connection pool with zero local write-ahead logs, zero blob stores, and zero coarse session mutexes.

## Scope and State Ownership

* **Single-venue multi-account ORS model**: Each `BybitUtaSession` or `OkxUtaSession` binds one authenticated exchange UID (`account_ref = 1..=255`) onto a shared or session-owned [`OrderManager`](../../crates/tickwire-account/src/order_manager.rs), [`UtaWalletLedger`](../../crates/tickwire-account/src/uta_ledger.rs) (`AssetKey { wallet_id, asset_id }`), [`FillRecordPool`](../../crates/tickwire-account/src/fill_record.rs), [`OrsQueryDesk`](../../crates/tickwire-account/src/ors_query_desk.rs), and process-wide [`SharedRateLimiter`](../../crates/tickwire-quota/src/shared.rs) (`OrsVenueProfile::BybitUta` / `OrsVenueProfile::OkxUta`).
* **Concurrent `Send + Sync` sharing & strict spinlock hierarchy**: `UtaAccountSession`, `BybitUtaSession`, `OkxUtaSession`, and `RfqQuoteHook` are `Send + Sync` and take `&self` on all `REQ` and `RESP` entry points so dedicated `REQ` and `RESP` threads share one `Arc<UtaAccountSession>` concurrently with zero `std::sync::Mutex` on hot paths. Outbound dispatch on `REQ` acquires and releases the target slot's fine-grained `Order` spinlock inside `OrderManager` (acquiring `OrsStpBook` only while holding that single `Order` lock, after lock-free atomic BBO `self_cross` screening), encodes the wire payload into a caller stack buffer outside all locks, claims an execution route atomically before spending `SharedRateLimiter`, records in-flight tracking in a leaf `TimedSpinLock`, and only then calls [`LockedConnection::send_frame`](../../crates/tickwire-transport/src/links.rs) under the per-connection spinlock. Inbound `RESP` socket polling (`LockedConnection::poll_raw_rx`, `match_ws_reply`, `complete_https_reply`) swaps preallocated RX buffers out of leaf `TimedSpinLock`s via zero-alloc empty slice placeholders (`Box::new([])`) so frame decoding and `JsonScan` never overlap any `Order` or `Connection` spinlock and never allocate on the heap (using stack-bounded `PendingCancelList` arrays for `OrsQueryDesk` retry cancels). Zero logging, zero heap allocation, and zero JSON or HTTP/WebSocket parsing occur while holding any spinlock.
* **Exact fixed-point arithmetic**: Prices use integer ticks, quantities use integer lots, and multi-wallet collateral/fees use scaled integer atoms (`i128` / `Fixed`). Floating-point arithmetic is forbidden across wire encoding, JSON scanning, EWMA route scoring, and rate-limit admission.

## Product Coverage, `RPI`, and Wire Protocols

### Bybit V5 UTA (`BybitUtaSession`)

* **Product mapping (`category`)**:
  * `Product::Spot` -> `"spot"` (`marketUnit: "baseCoin"` on market orders)
  * `Product::LinearPerp` | `Product::LinearFutures` | `Product::UsdM` -> `"linear"`
  * `Product::InversePerp` | `Product::InverseFutures` | `Product::CoinM` -> `"inverse"`
  * `Product::Options` -> `"option"` (`orderLinkId` mandatory and populated with `tw1...` `SelfDescribingClientId`)
* **Execution fields**: `symbol`, `side` (`"Buy"` | `"Sell"`), `orderType` (`"Limit"` | `"Market"`), `qty`, `price`, `timeInForce` (`"GTC"`, `"IOC"`, `"FOK"`, `"PostOnly"`, and **`"RPI"`** Retail Price Improvement via `TimeInForce::Rpi`), `reduceOnly`, `positionIdx` (`0` one-way, `1` buy-hedge, `2` sell-hedge), `smpType` (`"CancelMaker"` | `"CancelTaker"` | `"CancelBoth"`), and `orderLinkId`.
* **Transports**:
  * **WebSocket trade pool**: `wss://stream.bybit.com/v5/trade` (`wss://stream-testnet.bybit.com/v5/trade` in `Environment::Test`), `WsLink` (`SendSlot::Shared`, up to `16` concurrent in-flight requests correlated by numeric `reqId`), authenticated with `{"op":"auth","args":[key,expires,hex_hmac_sha256("GET/realtime{expires}")]}` and `20s` `{"op":"ping"}` heartbeats. Supports `"op":"order.create"`, `"op":"order.amend"`, and `"op":"order.cancel"`.
  * **Pooled HTTPS**: `POST /v5/order/create`, `POST /v5/order/amend`, and `POST /v5/order/cancel` over `HttpsLink` (`SendSlot::Exclusive`, `1` in-flight per socket) signed with `X-BAPI-API-KEY`, `X-BAPI-TIMESTAMP`, `X-BAPI-RECV-WINDOW` (`5000`), and lowercase hex HMAC-SHA256 `X-BAPI-SIGN`.
  * **Private WebSocket stream**: `wss://stream.bybit.com/v5/private` subscribing in one step to `["order", "execution", "execution.fast", "wallet"]`.

### OKX V5 UTA (`OkxUtaSession`)

* **Product mapping (`instType`)**:
  * `Product::Spot` -> `"SPOT"`
  * `Product::LinearPerp` | `Product::InversePerp` | `Product::UsdM` | `Product::CoinM` -> `"SWAP"`
  * `Product::LinearFutures` | `Product::InverseFutures` -> `"FUTURES"`
  * `Product::Options` -> `"OPTION"`
* **Execution fields**: `instId`, `tdMode` (`"cash"` | `"cross"` | `"isolated"`), `posSide` (`"net"` | `"long"` | `"short"` on derivatives), `reduceOnly`, `side` (`"buy"` | `"sell"`), `ordType` (`"limit"`, `"market"`, `"post_only"`, `"fok"`, `"ioc"`, `"optimal_limit_ioc"`, and **`"rpi"`** via `TimeInForce::Rpi`), `sz`, `px`, and 32-character `[0-9a-z]` `clOrdId` (`tw1...`).
* **Transports**:
  * **Unified private & trade WebSocket pool**: `wss://ws.okx.com:8443/ws/v5/private` (`wss://wspap.okx.com:8443/ws/v5/private` in `Environment::Test`), `WsLink` (`SendSlot::Shared`, up to `16` concurrent in-flight requests correlated by decimal `id`). Login encodes `{"op":"login","args":[{"apiKey":...,"passphrase":...,"timestamp":...,"sign":...}]}` with Base64 HMAC-SHA256 over `"{unix_s}GET/users/self/verify"`; subscription step `0` subscribes `orders` (`instType: "ANY"`), step `1` subscribes `balance_and_position`, and step `>= 2` emits `0` bytes; idle heartbeat sends raw text `"ping"` every `20s` and expects `"pong"`. Trade operations dispatch `op: "order"`, `op: "amend-order"`, and `op: "cancel-order"`.
  * **Pooled HTTPS**: `POST /api/v5/trade/order`, `POST /api/v5/trade/amend-order`, and `POST /api/v5/trade/cancel-order` over `HttpsLink` (`SendSlot::Exclusive`) signed with `OK-ACCESS-KEY`, `OK-ACCESS-PASSPHRASE`, ISO-8601 `OK-ACCESS-TIMESTAMP`, and Base64 HMAC-SHA256 `OK-ACCESS-SIGN` (`x-simulated-trading: 1` in `Environment::Test`).

## Shared UTA Connection Pool, Admission, and `dry_run`

* **Shared `[local_ip, count]` pool (`product_mask = 0`)**: Every execution slot in a UTA session publishes `OrderRoute { product_mask: 0, .. }` into lock-free double-buffered [`PublishedRoutes`](../../crates/tickwire-transport/src/order_router.rs) so Spot, Swap/Perp, Futures, and Options orders share all ready connections bound to the UID's `[local_ip, count]` table.
* **`pick` then `SharedRateLimiter` spend (immediate NACK, zero retry)**: Route selection uses `PublishedRoutes::pick` (`9/8` integer EWMA hysteresis after `8` RTT samples, round-robin warmup prior to `8` samples). Immediately after picking `route`, [`OrsAccountQuota`](../../crates/tickwire-quota/src/shared.rs) debits the account/product/symbol window and the cross-account `route.local_ip` bucket in one atomic batch (`pick_route_and_place_or_nack` / `pick_route_and_cancel_or_nack`). If any window or IP cooldown rejects the spend, the session immediately refuses/rolls back the action (`RefusalReason::RateLimited` / `TerminalReason::RateLimited`) and **never** retries a sibling connection on the same request.
* **`dry_run` execution**: When `OrsNewOrderRequest.dry_run == true`, `OrderManager::place_order` exercises the strategy's dedicated scratch slot, STP check, and wallet hold math, and the session runs stack HMAC/wire encoding into its scratch buffer while skipping `SharedRateLimiter` spend and `LockedConnection::send_frame`.

## Server Maintenance `notice` Graceful Connection Switchover

Exchanges periodically announce imminent WebSocket node restarts before severing TCP connections (such as OKX `{"event":"notice","code":"64008",...}` and Bybit connection-retire maintenance notices).

1. Single-pass `JsonScan` in [`src/json_parser.rs`](../../crates/tickwire-spot-execution/src/json_parser.rs) detects the maintenance notice without heap allocation and sets `graceful_drain_connection = true`.
2. On `RESP`, the session immediately marks that `conn_id` as `draining = true` and republishes `PublishedRoutes` with `ready = false` so `REQ` stops routing new orders, amends, or cancels to the retiring socket.
3. Simultaneously, the session signals the slot's replacement [`DialBaton`](../../crates/tickwire-transport/src/dialer.rs) (`want(now_mono_ns)`) so the cold `Dialer` thread establishes a fresh TCP+TLS connection in parallel.
4. All resting open orders in `OrderManager` remain live (`0` dropped or canceled open orders), and the retiring connection stays readable on `RESP` until both its atomic `claimed_inflight` counter and `inflight_relaxed()` reach `0`. Once in-flight claims and replies finish and the replacement link is ready, `RESP` swaps the replacement link into `conn_id` (closing the old socket outside the spinlock) and republishes `ready = true`.

## Zero-Allocation Single-Pass `JsonScan` and `FillRecord` `amendment`

All hot-path inbound frames are decoded in a single forward pass over [`JsonScan`](../../crates/tickwire-transport/src/json_scan.rs) ([`src/json_parser.rs`](../../crates/tickwire-spot-execution/src/json_parser.rs)) with zero `serde_json::Value` DOM trees and zero heap allocations:

* **Order status & `ProjectedFill`**: Private `order` / `orders` frames update `OrderManager::on_ack`, `on_modify_ack`, and `on_cum_qty` (`cumExecQty` / `accFillSz` emitting `ProjectedFill` when cumulative filled quantity outruns individual trade notifications).
* **Deterministic `TerminalReason` mapping**:
  * Bybit `CancelBySmp` / `smpType` or OKX SMP `cancelSource` (`"32"`) -> `TerminalReason::MKT_CANCELED_SELFTRADE`
  * Bybit `EC_PostOnlyWillTakeLiquidity` (`110079` / `170139` / `140001`) or OKX post-only/RPI liquidity take (`51019`, `cancelSource == "20"`) -> `TerminalReason::MKT_REJECT_POST_ONLY`
  * Unfilled `IOC` / `FOK` / `optimal_limit_ioc` expiry -> `TerminalReason::MKT_IOC_CANCELED`
  * Bybit `CancelByReduceOnly` / `EC_ReduceOnlyWillTake` (`110017`) or OKX reduce-only cancel (`51116`, `51059`, `cancelSource == "14" | "15"`) -> `TerminalReason::MKT_REDUCE_ONLY_CANCELED`
  * Standard cancel completion: `TerminalReason::NO_REASON` when `pending_cxl > 0` (or `ADMIN_ACTION` when `admin_cancel_requested`), otherwise `TerminalReason::MKT_CANCELED`.
* **Fast execution -> fee patch (`amendment = true`)**: Bybit `execution.fast` frames carry `execId`, `execPrice`, and `execQty` without fee fields (`fee: None`), immediately booking position and hold updates via `OrderManager::on_fill` (`amendment = false`). When the matching full `execution` frame arrives with `execFee` and `feeCurrency`, `FillRecordPool` deduplicates by `trade_id`, patches only the fee delta in `UtaWalletLedger::apply_fee_amendment` with **zero double position or fill-quantity update**, and emits `OrsFillReport { amendment: true, .. }`.
* **Rate-limit headers and error codes**: HTTP `429`, Bybit `retCode == 10006` (and `X-Bapi-Limit-Status` / `X-Bapi-Limit`), and OKX `code` / `sCode == "50011"` reconcile usage via `SharedRateLimiter::reconcile_venue_usage` (`ExchRateLimitEvent`) and isolate the affected source IP via `SharedRateLimiter::note_ip_cooldown`.

## Optional `RFQ` Quote Extension Hook

Options and institutional block-trading strategies may attach a `Send + Sync` [`RfqQuoteHook`](../../crates/tickwire-spot-execution/src/rfq.rs) (`&self` across all quote operations) to `BybitUtaSession` or `OkxUtaSession`:

* **Default disabled posture (`NoopRfqQuoteHook`)**: `has_quote(_)` returns `false`, `open_quotes()` returns `0`, and `submit_quote` / `cancel_quotes` refuse deterministically with `TerminalReason::RFQ_QUOTE_NOT_SUPPORTED` (`RfqQuoteNotSupported`).
* **Enabled `UtaRfqQuoteBook`**: Tracks live quote lifecycles (`has_quote`, `open_quotes`, `on_quote_ack`, `on_quote_terminal`) in preallocated fixed-capacity storage guarded by a leaf `TimedSpinLock` (`LockDomain::GuardedValue`) while encoding zero-allocation RFQ wire payloads outside the lock for Bybit V5 (`/v5/rfq/create-quote`, `/v5/rfq/execute-quote`, `/v5/rfq/cancel-quote`, `/v5/rfq/cancel-all-quotes`) and OKX V5 (`/api/v5/rfq/create-quote`, `/api/v5/rfq/cancel-quote`, `/api/v5/rfq/cancel-all-quotes`).

## Run an Account

1. **Cold startup bootstrap**: Construct `OrderManager` and `SharedRateLimiter`, query authenticated Exchange REST snapshots (`Account + Positions + OpenOrders`) using [`UtaRestRequestBuilder`](../../crates/tickwire-spot-execution/src/rest_bootstrap.rs) ([spot-rest-recovery.md](spot-rest-recovery.md)), and feed the decoded `OrsExchangeBootstrapSnapshot` into `OrderManager::bootstrap_from_exchange` to reconstruct open-order holds, derivative positions, and per-strategy `SelfDescribingClientId` (`tw1...`) sequence floors without local disk state.
2. **Pool & private stream bring-up**: Configure `[local_ip, count]` [`UtaConnPoolEntry`](../../crates/tickwire-spot-execution/src/session.rs) bindings on `BybitUtaSession` or `OkxUtaSession` (`product_mask = 0`), attach cold `Dialer` and `AsyncRestQuerier` handles, and wait until private stream authentication/subscription and at least one execution route publish `ready = true`.
3. **Concurrent hot-path execution & `OrsQueryDesk` reconciliation**: Share `Arc<BybitUtaSession>` or `Arc<OkxUtaSession>` between the dedicated `REQ` thread (`submit_new_order`, `submit_modify_order`, `submit_cancel_order`, `submit_rfq_quote`, `cancel_rfq_quotes`) and `RESP` thread (`poll_resp`, `apply_order_ws_payload`, `apply_order_https_bytes`, `apply_private_ws_payload`, `poll_due_queries`, `apply_status_ask_reply`), draining due `OrsQueryDesk` `StatusAsk` and `RetryCancel` actions over `AsyncRestQuerier` off the hot path.
4. **Read-only CLI observation (`tickwire-spot-observe`)**: Operators may inspect live UTA balances, positions, open orders, fills, and bounded private WebSocket streams statelessly via `tickwire-spot-observe` ([`src/bin/tickwire-spot-observe.rs`](../../crates/tickwire-spot-execution/src/bin/tickwire-spot-observe.rs)) using read-only credentials (`TICKWIRE_SPOT_KEY`, `TICKWIRE_SPOT_SECRET`, and `TICKWIRE_SPOT_PASSPHRASE` on OKX).

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --locked -p tickwire-spot-execution --all-targets -- -D warnings
cargo test --locked -p tickwire-spot-execution --all-targets
taskset -c 2 cargo bench --locked -p tickwire-spot-execution --bench hotpath_warmup
```

## References

* Implementation files: [crate root](../../crates/tickwire-spot-execution/src/lib.rs), [UTA sessions](../../crates/tickwire-spot-execution/src/session.rs), [WS login/subscribe envelope](../../crates/tickwire-spot-execution/src/ws_envelope.rs), [stack wire encoders](../../crates/tickwire-spot-execution/src/wire_encode.rs), [single-pass `JsonScan` parser](../../crates/tickwire-spot-execution/src/json_parser.rs), [RFQ quote hook](../../crates/tickwire-spot-execution/src/rfq.rs), [REST bootstrap & `OrsQueryDesk` builder](../../crates/tickwire-spot-execution/src/rest_bootstrap.rs), [hot-path warmer](../../crates/tickwire-spot-execution/src/warmup.rs), [warmup benchmark](../../crates/tickwire-spot-execution/benches/hotpath_warmup.rs)
* Related specifications: [stateless UTA REST bootstrap and `OrsQueryDesk` reconciliation](spot-rest-recovery.md), [strategy routing](spot-strategy-routing.md), [single-host trading](single-host-trading.md)
