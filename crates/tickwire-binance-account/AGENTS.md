# `tickwire-binance-account` Engineering Guide (`AGENTS.md`)

Governing specifications:
- [`specs/implementation/binance-ws-trading.md`](../../specs/implementation/binance-ws-trading.md)
- [`specs/implementation/reconciliation-controller.md`](../../specs/implementation/reconciliation-controller.md)

## Ownership and Invariants

1. **Pure Stateless `Send + Sync` `BinanceSession` (Zero Local WAL, Zero Single-Thread Cell Wrappers)**:
   - `tickwire-binance-account` builds directly on `tickwire-account`, `tickwire-transport`, `tickwire-binance`, `tickwire-binance-quota`, `tickwire-quota`, `tickwire-clock`, `tickwire-risk`, and `tickwire-venue` with zero local disk persistence or legacy gateway dependencies.
   - `BinanceSession<CHECK_STP>` is `Send + Sync` so `REQ` (`submit_new`, `submit_cancel`, `submit_modify`), `RESP` (`on_message`, `on_disconnected`, `apply_status`), `Dialer` (`connection_url`, `encode_login` with a dedicated `Ed25519` signing copy), and `AsyncRestQuerier` (`order_status_query`, `stream_token_query`, cold REST bootstrap) execute concurrently across physically isolated threads without coarse mutexes.
2. **Full-Market Coverage & Colo vs Public URL Validation (`src/urls.rs`, `src/session.rs`)**:
   - Supports Spot (`/api/v3`, `ws-api`), USD-M (`/fapi/v1`, `ws-fapi` / `ws-fapi-mm`), Coin-M (`/dapi/v1`, `ws-dapi`), and European Options (`/eapi/v1` `HttpsLink` `Exclusive` order pool + `fstream/private/ws` `listenKey` stream pool) both as single-market configs and as multi-product sub-pools inside one UID `BinanceSession` (`OrderRoute.product_mask`, `AssetKey { wallet_id, asset_id }`).
   - `resolve_market_urls` rejects simultaneous `colo` + `public` URL entries for the same role and forbids installing `UrlRole::Query` into persistent order/stream pools.
3. **Zero-Allocation Single-Pass `BinanceMsgParser` & Lock Hierarchy (`src/msg_parser.rs`)**:
   - `#![forbid(unsafe_code)]`; zero floating-point arithmetic (`f32`/`f64`) anywhere in wire, financial, or rate-limit logic.
   - `BinanceMsgParser` scans each `RESP` frame in a single zero-allocation pass over `tickwire_transport::json_scan::JsonScan`, extracting synonymous order fields, up to 4 `rateLimits[]` rows, and error `code`/`msg` while skipping nested Spot `fills[]` objects.
   - Never overlap `Order` and `Connection` spinlocks (`REQ` releases `LockedOrder` inside `OrderManager` before calling `LockedConnection::send_frame`). Perform zero logging, heap allocation, or JSON parsing while holding any spinlock.
4. **Rate-Limit Discipline, `TRADE_LITE` Amendments, & `OrsQueryDesk` (`src/session.rs`, `src/bootstrap.rs`)**:
   - Spend `SharedBinanceOrsQuota` strictly AFTER `PublishedRoutes::pick` on the chosen connection's `local_ip`; NACK immediately on rate-limit hit without retrying another connection.
   - Reconcile `TRADE_LITE` (`fee = None`) followed by full `ORDER_TRADE_UPDATE` or post-trade rebate updates via `FillRecordPool` (`amendment = true`, fee delta only, zero double position update, zero `Conflict` freeze).
   - Schedule `OrsQueryDesk` (`status_query_delay_ms = 6000`, `recvWindow = 60000`) on `-1007`, `-1000`, `MKT_DELAYED`, and Options `HttpsLink` disconnects; close `-2013` on `PendingNew` as `NACK`; and cold-bootstrap exclusively from Exchange REST `Account + OpenOrders` snapshots paired with `SelfDescribingClientId` (`tw1...`).

## Verification

* **Source**: [src/lib.rs](src/lib.rs) · [src/bootstrap.rs](src/bootstrap.rs) · [src/msg_parser.rs](src/msg_parser.rs) · [src/session.rs](src/session.rs) · [src/urls.rs](src/urls.rs)
* **Integration tests**: [tests/unified_binance_session.rs](tests/unified_binance_session.rs)

```sh
cargo test --locked -p tickwire-binance-account --all-targets
```
