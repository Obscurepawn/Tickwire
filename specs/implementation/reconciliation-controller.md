# Per-Account `OrsQueryDesk` Reconciliation and Stateless Cold REST Bootstrap

`tickwire-binance-account` recovers order certainty and cold-startup state across **Spot, USD-M Futures, Coin-M Futures, and European Options** without any local WAL, journal, or disk checkpoint. Runtime ambiguity resolution runs per order on the cold `AsyncRestQuerier` / `OrsQueryDesk` path while sibling orders and accounts continue trading at full hot-path speed, and process startup reconstructs open-order holds, STP books, balances, positions, and per-strategy sequence floors directly from authenticated Exchange REST snapshots paired with `SelfDescribingClientId` (`tw1...`).

## Per-Order `OrsQueryDesk` Status Queries (`status_query_delay_ms = 6000`)

Whenever an outbound `Place`, `Cancel`, or `Modify` receives an ambiguous venue outcome (`-1007` send/execution status unknown, `-1000` unknown error / try again later, `-1001`, `-1006`, HTTP `408`, or an Options `HttpsLink` disconnect/timeout with an in-flight `BinanceRequestRef`), `BinanceSession` isolates only that order into `OrderManager::query_desk()`:

| Event / Condition | Delay & Wire Request | Resolution Semantics (`apply_status`) |
| --- | --- | --- |
| Ambiguous `Place` (`MKT_DELAYED`) or Options `Place` link drop | `status_query_delay_ms = 6000` (`6 s`, exceeding the `5 s` order `recvWindow`), signed `GET /{api/v3,fapi/v1,dapi/v1,eapi/v1}/order` with `recvWindow=60000` and `origClientOrderId` (`clientOrderId` on Options) | Order remains `PendingNew` (`WaitMkt`) holding its collateral reservation. Once settled by the private stream, `order_status_query` returns `None` and sends nothing. |
| Ambiguous `Cancel` (`MKT_DELAYED`) or Options `Cancel` link drop | Emits `CancelRejected` (`MKT_DELAYED`), keeps `pending_cxl > 0` counted, and schedules a `Cancel` status ask (`6000 ms` delay), coalescing onto any existing ask for the same `ors_order_id` | First takes back one counted cancel: if the order is terminal (`CANCELED`/`EXPIRED`), settles as `NO_REASON`; if still `NEW`/`PARTIALLY_FILLED`, emits `CancelRejected` (`MKT_REJECT`) and re-queries while any additional cancel remains counted. |
| `-2013` (`Order does not exist`) on `StatusAsk` | Answered deterministic `400` / `-2013` | Closes a `PendingNew` (`WaitMkt`) order immediately as `Rejected` (`MKT_REJECT_NO_SUCH_ORDER`), releasing its hold and STP entry. |
| Transport failure, timeout, or transient HTTP `5xx`/`429` | Unanswered or inconclusive reply | For `PendingNew` (`Place` ask): calls `OrderManager::on_query_inconclusive`, retrying every `1 s` until `max_query_count` releases the wedged slot with `TerminalReason::INTERNAL_CANCEL`. For an acked order's `Cancel`: schedules exponential backoff (`1, 2, 4, 8, 16, 32, 64 s`) up to `8` attempts (`MAX_CANCEL_BACKOFF_ATTEMPTS`). |

## Listen-Key Lifecycle (`stream_token_query`)

For USD-M (`/fapi/v1/listenKey`), Coin-M (`/dapi/v1/listenKey`), and Options (`/eapi/v1/listenKey`)—Spot uses `userDataStream.subscribe` on its WebSocket order/stream pool and returns `None`—`BinanceSession` manages the stream token off the hot path via `AsyncRestQuerier`:

1. **Create (`refresh == false`)**: Issues `POST /{fapi,dapi,eapi}/v1/listenKey` with `X-MBX-APIKEY`. `on_stream_token_reply` extracts `"listenKey"` via `tickwire_transport::json_scan` and stores it inside that market's `GuardedValue<InlineAscii<128>>` so `Dialer::connection_url` can dial `/ws/<key>` (colo) or `?listenKey=<key>` (public).
2. **Keepalive (`refresh == true`)**: Issues `PUT /{fapi,dapi,eapi}/v1/listenKey` every `stream_token_refresh_s` (`1800 s` default); `on_stream_token_reply` validates absence of an error code without overwriting the active key.
3. **Expiry (`listenKeyExpired`)**: Receiving `listenKeyExpired` on the private stream clears that market's `GuardedValue` token and sets `stream_token_expired = true`, reconnecting only the stream pool and triggering a fresh `POST` while order sockets stay uninterrupted.

## Stateless Cold Startup from Exchange REST Snapshots

On cold boot or restart, `bootstrap::decode_market_bootstrap_snapshot` and `BinanceSession::bootstrap_from_rest_snapshots` (`src/bootstrap.rs`) parse authenticated `Account + OpenOrders` REST responses using zero-float `tickwire_transport::json_scan` decoders across all configured markets:

* **Spot**: `GET /api/v3/account` (`balances: [{asset, free, locked}]`) + `GET /api/v3/openOrders`.
* **USD-M**: `GET /fapi/v2/account` or `/fapi/v3/account` (`assets`, `positions`) + `GET /fapi/v1/openOrders`.
* **Coin-M**: `GET /dapi/v1/account` (`assets`, `positions`) + `GET /dapi/v1/openOrders`.
* **Options**: `GET /eapi/v1/account` (`asset: [{asset, marginBalance, available, locked}]`) + `GET /eapi/v1/openOrders` (`clientOrderId`).

Each market's collateral balances map into `AssetKey { wallet_id, asset_id }` inside `UtaWalletLedger`, net derivative positions populate `OrsRestPositionRow`, and every open order carrying a valid domain-authenticated 32-character `[0-9a-z]` `SelfDescribingClientId` (`tw1...`) reconstructs its exact `(strat_id, epoch, order_seqnum)` slot in `OrderManager`, restoring `OrsStpBook` entries, open-order holds, and per-strategy monotonic sequence floors without touching local disk files.

## Verification and References

```sh
cargo test --locked -p tickwire-binance-account --all-targets
```

* [Stateless cold REST snapshot parser and bootstrap builder](../../crates/tickwire-binance-account/src/bootstrap.rs)
* [Unified `BinanceSession` `OrsQueryDesk` and `listenKey` hooks](../../crates/tickwire-binance-account/src/session.rs)
* [Preallocated per-account `OrsQueryDesk` state machine](../../crates/tickwire-account/src/ors_query_desk.rs)
* [Stateless `OrderManager` and `tw1` client-ID recovery](account.md)
* [Binance multi-market session specification](binance-ws-trading.md)
