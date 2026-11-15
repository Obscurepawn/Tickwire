# Binance Thread-Isolated Execution Engine and Hot-Path Warmer

`tickwire-binance-execution` (`#![forbid(unsafe_code)]`) wires the stateless, `Send + Sync` multi-market `BinanceSession<CHECK_STP>` (`tickwire-binance-account`) to `PublishedRoutes`, `UrlConnections`, `AsyncRestQuerier`, `SharedBinanceOrsQuota`, and the side-effect-free `BinanceHotPathWarmer`. `REQ`, `RESP`, `Dialer`, and `QueryDesk` threads operate concurrently across physically isolated CPU cores without local disk journals, single-threaded interior mutability, or coarse process-wide mutexes.

## Physical Thread Isolation and Lock-Free / Fine-Grained State

`BinanceSession` and `BinanceExecutionEngine` satisfy `Send + Sync` (`const` static assertion) with strict per-thread ownership:

| ORS Thread | Methods Invoked on `&BinanceSession` | Cryptographic & Shared State Touched |
| --- | --- | --- |
| **`REQ` (`req_cpu`)** | `submit_new`, `submit_cancel`, `submit_modify` | Dedicated `req_ws_signer` / `req_options_hmac` key copies; lock-free `OrsStpBook::self_cross`; per-slot `LockedOrder` spinlock (released **before** acquiring `LockedConnection`); lock-free `PublishedRoutes` read; lock-free `SharedBinanceOrsQuota` CAS windows. |
| **`RESP` (`resp_cpu`)** | `reply_id`, `on_message`, `on_disconnected`, `on_stream_token_reply`, `apply_status` | Stack-only single-pass `BinanceMsgParser`; per-slot `LockedOrder` (`Order -> StpBook` hierarchy, never overlapping `LockedConnection`); `FillRecordPool`; `GuardedValue` stream-token write/clear; rising-edge `AtomicU8` `rateLimits[]` trackers. |
| **`Dialer` (`housekeeping_cpus`)** | `connection_url`, `encode_login`, `awaits_login_reply`, `login_reply`, `encode_subscribe` | Dedicated `dialer_logon_signer` Ed25519 key copy for `session.logon`; micro-spinlocked `GuardedValue` copy of `listenKey` for stream URLs. |
| **`QueryDesk` / `AsyncRestQuerier` (`housekeeping_cpus`)** | `stream_token_query`, `order_status_query`, `open_orders_queries`, `poll_query_desk` | Dedicated `rest_query_signer` HMAC/RSA key copy; per-account `OrsQueryDesk` spinlock. |

## Core Invariants

* **Zero spinlock overlap and zero allocation under locks**: `REQ` allocates and initializes the order slot under `LockedOrder`, releases `LockedOrder`, selects a ready `OrderRoute` matching `product` (`OrderRoute.product_mask`), spends `SharedBinanceOrsQuota` on the chosen connection's `local_ip`, encodes the wire frame into a caller-supplied stack slice, and only then locks the chosen `LockedConnection` to transmit bytes and record `RequestRef`. Zero heap allocation, zero protocol parsing, and zero logging occur inside any spinlock.
* **Single-shot post-`pick` rate-limit spend**: `SharedBinanceOrsQuota::pick_route_and_spend_command_or_nack` calls `OrderRouter` / `pick_order_route` **once** for the order's `BinanceMarket`, spending `ORDERS` (`300 / 10s`, `1200 / min` or per-account override) plus `REQUEST_WEIGHT` (`2400 / min` or per-IP override) on that socket's `local_ip` for `Place`/`Modify`, and `REQUEST_WEIGHT` only for `Cancel`. If any local window refuses, `submit_*` immediately rolls back the slot/hold and returns `RefusalReason::RateLimited` without retrying another connection.
* **`dry_run` wire encoding without side effects**: When `OrsNewOrderRequest.dry_run == true`, `OrderManager::place_order` validates admission, STP, and hold reservation on the strategy's dedicated scratch slot and immediately retires it (`should_send_to_venue == false`); `BinanceSession::submit_new` still encodes the complete wire frame into `frame_buf` (`dry_run == true`) while skipping rate-limit spend, in-flight slot opening, and socket transmission.
* **Side-effect-free idle-window hot-path warming (`warmup`)**: `BinanceHotPathWarmer` (`src/warmup.rs`) exercises stack `bookTicker` integer parsing, read-only `OrsStpBook::self_cross` / `Account` checks, stack RFC 2104 HMAC-SHA256 signing, HTTP/1.1 + RFC 6455 WebSocket framing, and offline `TlsLoopbackWarmer` roundtrips across Spot, USD-M, Coin-M, and Options during idle intervals (`>= warmup_interval_ns`) without touching live order slots, `SharedBinanceOrsQuota` budgets, or live venue sockets.

## Verification and References

```sh
cargo test --locked -p tickwire-binance-execution --all-targets
```

* [Thread-isolated `BinanceExecutionEngine` and re-exports](../../crates/tickwire-binance-execution/src/lib.rs)
* [Side-effect-free full-market `BinanceHotPathWarmer`](../../crates/tickwire-binance-execution/src/warmup.rs)
* [Unified `Send + Sync` `BinanceSession`](../../crates/tickwire-binance-account/src/session.rs)
* [Binance multi-market session specification](binance-ws-trading.md)
* [Binance shared ORS rate-limit specification](binance-quota.md)
* [Per-account `OrsQueryDesk` and cold bootstrap specification](reconciliation-controller.md)
