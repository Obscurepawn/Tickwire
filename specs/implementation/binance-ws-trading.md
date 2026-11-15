# Unified Stateless Binance Multi-Market `BinanceSession`

`tickwire-binance-account` (`#![forbid(unsafe_code)]`) implements the pure stateless, thread-isolated (`Send + Sync`) ORS `BinanceSession<CHECK_STP>` across all four Binance markets—**Spot, USD-M Futures, Coin-M Futures, and European Options**—with zero local WAL, zero single-threaded cell wrappers, and zero coarse mutexes. One `BinanceSession` represents one Binance UID (`account_ref = 1..=255`) and operates either as a single-market venue configuration or as a unified multi-market UID session routing sub-pools via `OrderRoute.product_mask` and collateral pools via `AssetKey { wallet_id, asset_id }`.

## Multi-Market Sub-Pools, Colocation vs. Public URLs, and Link Protocols

`BinanceSession::configure` resolves one URL per `(market, role)` from `BinanceUrlId`, defaulting to the colocation host where defined (`ws-fapi-mm`, `fstream-mm`, `ws-dapi-mm`, `dstream-mm`) unless the account's `urls` explicitly selects the public `UrlId` of that role. Naming both colocation and public `UrlId`s for the same `(market, role)`, leaving order sockets unbound (`local_ip == 0`), or supplying invalid key material fails `configure` immediately:

| Market | Order Pool (`UrlRole::ORDER`) | Stream Pool (`UrlRole::STREAM`) | REST Query Host (`AsyncRestQuerier`) |
| --- | --- | --- | --- |
| **Spot** | `SpotWssColo` or `SpotWssPublic` (`ws-api.binance.com/ws-api/v3`), `UrlRole::ORDER \| UrlRole::STREAM`, `WsLink` (`Shared`, 16 in-flight), Ed25519 `session.logon` | Same pool; `encode_subscribe` step `0` writes `userDataStream.subscribe` (`needs_stream_token = false`) | `SpotHttpsPublic` (`https://api.binance.com`) |
| **USD-M** | `UmOrderWssColo` (`ws-fapi-mm`) or `UmOrderWssPublic` (`ws-fapi`), `WsLink` (`Shared`, 16 in-flight), Ed25519 `session.logon` | `UmStreamWssColo` (`fstream-mm/ws/<key>`) or `UmStreamWssPublic` (`fstream/private/ws?listenKey=<key>`), `refresh = 1800 s` | `UmHttpsPublic` (`https://fapi.binance.com`) |
| **Coin-M** | `CmOrderWssColo` (`ws-dapi-mm`) or `CmOrderWssPublic` (`ws-dapi`), `WsLink` (`Shared`, 16 in-flight), Ed25519 `session.logon` | `CmStreamWssColo` (`dstream-mm/ws/<key>`) or `CmStreamWssPublic` (`dstream/private/ws?listenKey=<key>`), `refresh = 1800 s` | `CmHttpsPublic` (`https://dapi.binance.com`) |
| **Options** | `OptionHttpsOrderColo` or `OptionHttpsOrderPublic` (`https://eapi.binance.com`), `HttpsLink` (`Exclusive`, 1 in-flight per link across `[local_ip, count]`), form-urlencoded `POST` / `DELETE /eapi/v1/order` + HMAC-SHA256 + `X-MBX-APIKEY` | `OptionStreamWssColo` or `OptionStreamWssPublic` (`wss://fstream.binance.com:443/private/ws?listenKey=<key>`), `refresh = 1800 s` | `OptionHttpsPublic` (`https://eapi.binance.com:443`) |

WebSocket pings are protocol-level RFC 6455 pings handled by `WsLink` (`encode_ping` returns `0`). WebSocket order requests encode `<ors_order_id>` for `Place`, `c<ors_order_id>` for `Cancel`, and `m<ors_order_id>` for `Modify`, packing the 2-bit `BinanceRequestKind` tag alongside `ors_order_id` in `reply_id` so concurrent `Place`, `Cancel`, and `Modify` requests in `WsLink`'s 16-slot array never collide. Options HTTP responses carry no request ID on the wire and correlate via the link's positional `hint: BinanceRequestRef { id, kind: Place | Cancel }`; when an Options link drops or times out with an in-flight `BinanceRequestRef`, `on_disconnected` synthesizes Binance `-1007` (`MKT_DELAYED`) for that request.

## Zero-Allocation Single-Pass `BinanceMsgParser`

Every `RESP` frame passes through `BinanceMsgParser::parse` (`src/msg_parser.rs`), built on `tickwire_transport::json_scan` byte scanning with zero `serde_json` DOM materialization, zero `Vec`/`String` heap allocations, and zero floating-point operations:

* **Single-pass synonymous field extraction**: One linear scan extracts envelope metadata (`id`, numeric HTTP `status`, `code`, `msg`, `authorizedSince`, `listenKey`, `subscriptionId`), event type `e`, synonymous order fields (`orderId`/`i`, `clientOrderId`/`c`, `origClientOrderId`/`C`, `symbol`/`s`, `side`/`S`, `type`/`origType`/`ot`/nested `o`, `timeInForce`/`f`, `price`/`p`, `origQty`/`q`, `executedQty`/`z`, `cummulativeQuoteQty`/`Z`, `avgPrice`/`ap`, `lastExecutedQty`/`l`, `lastExecutedPrice`/`L`, string `status`/`X`, `executionType`/`x`, `tradeId`/`t`, `commission`/`n`, `commissionAsset`/`N`, `m`, `R`, `V`, `er`, `T`), and up to `4` `rateLimits[]` rows while skipping Spot `fills[]` arrays in place so per-fill sub-objects never overwrite parent order fields.
* **`TRADE_LITE` and post-trade fee/rebate amendments**: `TRADE_LITE` (or a `TRADE` frame lacking commission) emits `One ack` if still `PendingNew` and records the execution in `OrderManager`'s `FillRecordPool` with `fee: None`. When the full `ORDER_TRADE_UPDATE` / `executionReport` or post-trade rebate update arrives with the same `tradeId` (`t`) and `commission`/`commissionAsset` (`n`/`N`), `OrderManager::on_fill` emits `OrsFillReport { amendment: true, .. }`, updating only the fee delta in `UtaWalletLedger` with zero double position/hold mutation and zero account freeze—even if the order slot has already retired.
* **`One ack`, `AMENDMENT`, `ProjectedFill`, and `listenKeyExpired`**:
  * `NEW` or synchronous `200` reply transitions `PendingNew -> Open` (`One ack`) and dispatches any queued `cancel-before-ack`.
  * `AMENDMENT` updates resting `(price, qty)` in `OrderManager` (adjusting holds and `OrsStpBook` atomically).
  * Synchronous `200` replies or non-trade status frames with `executedQty > projected_fill_qty_lots` emit `ProjectedFill` (`OrderManager::on_cum_qty`) using exact `Fixed` integer division (`avgPrice`/`ap`, else `cummulativeQuoteQty / executedQty` `Z / z`, else limit price) strictly before computing terminal `canceled_qty = effective_qty - projected_fill_qty`.
  * `listenKeyExpired` clears that market's `GuardedValue` stream token and marks `stream_token_expired = true`.
* **Exact `terminalReason` & `rateLimits[]` rising-edge notices**:
  * Applies the shared `terminalReason` table (`EXPIRED_IN_MATCH` / `TRADE_PREVENTION` -> `MKT_CANCELED_SELFTRADE`; `IOC`/`FOK` -> `MKT_IOC_CANCELED`; `GTX` on Spot/Coin-M/Options -> `MKT_REJECT_POST_ONLY`; USD-M `EXPIRED` -> `MKT_REDUCE_ONLY_CANCELED`; `pending_cxl > 0` -> `NO_REASON` / `ADMIN_ACTION`, else `MKT_CANCELED`).
  * Reconciles each `rateLimits[]` row against `exch_warn_pct` (default `80`, `0` disables) via `SharedBinanceOrsQuota` and lock-free rising-edge state, emitting `ExchRateLimitNotice` (`ALL_ORS_STRATS`, `hard_limit_hit` at `count >= limit`) strictly when crossing upward into warning or hard-limit bands.

## Verification and References

```sh
cargo test --locked -p tickwire-binance-account --all-targets
```

* [Crate root and public session exports](../../crates/tickwire-binance-account/src/lib.rs)
* [Colocation vs. public URL table and `configure` validator](../../crates/tickwire-binance-account/src/urls.rs)
* [Zero-allocation single-pass `BinanceMsgParser`](../../crates/tickwire-binance-account/src/msg_parser.rs)
* [Unified `Send + Sync` full-market `BinanceSession`](../../crates/tickwire-binance-account/src/session.rs)
* [Stateless cold REST `Account + OpenOrders` bootstrap](../../crates/tickwire-binance-account/src/bootstrap.rs)
* [Per-account `OrsQueryDesk` and cold recovery specification](reconciliation-controller.md)
* [Binance execution engine and thread-isolation specification](binance-execution.md)
