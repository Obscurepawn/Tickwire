# Shared UTA Strategy Routing for Bybit V5 and OKX V5

`UtaVenueSession` (`BybitVenueSession` and `OkxVenueSession` in `tickwire-spot-runner`, and integrated alongside `BinanceVenueSession` inside `tickwire-app`'s `UnifiedVenueSession`) connects [`tickwire-spot-execution`](../../crates/tickwire-spot-execution/src/lib.rs) directly to [`tickwire-runtime`](../../crates/tickwire-runtime/src/lib.rs)'s generic `OrderRoutingSystem<S>` and `OrsProcess<S>`, multiplexing inline `1,024B` `IntentRecord` commands from `1..N` strategy workers across `1..N` Unified Trading Account (`AccountSession`) instances (`1 ORS process = 1 Venue`, `1 AccountSession = 1 Exchange UID`, `account_ref = 1..=255`).

## State Ownership and Routing Architecture

| Component | Key / Capacity | Contract |
| --- | --- | --- |
| `DopState` & `AccountManager` | `strat_id = 0..=254`, `account_ref = 1..=255` (`0` routes to the strategy's first authorized account) | Authenticates strategy workers over the Unix `SOCK_SEQPACKET` DOP `ControlServer` (`DopLogin` / `DopEcho` / `OrsSessions`), returning the recovered `next_order_seqnum` floor and authorized `account_ref` table with zero local disk files. |
| `OrderManager<true>` & `SelfDescribingClientId` | Preallocated per-order spinlock slots, 32-char `[0-9a-z]` `tw1...` client IDs with domain-keyed MAC | Enforces shared multi-wallet fixed-point holds (`AssetKey { wallet_id, asset_id }`), atomic `OrsStpBook` self-trade prevention, and per-strategy attribution so a balance, RMS, STP, or rate-limit refusal on one strategy returns an inline `512B` `ReportRecord` NACK without stalling sibling strategies. |
| `UtaVenueSession` (`Bybit` / `OKX`) | Shared `[local_ip, count]` URL pools (`product_mask = 0`), `WebSocketTrade` (`16` in-flight) or `PooledHttps` (`1` in-flight) | Routes all 6 UTA product classes (`Spot`, `LinearPerpetual`, `InversePerpetual`, `LinearFuture`, `InverseFuture`, `Option`, including `TimeInForce::Rpi` and optional `RfqQuoteHook`) through `PublishedRoutes::pick` (`9/8` EWMA hysteresis) followed by single-pick `SharedRateLimiter` admission (NACKing immediately without retrying sibling sockets). |

## Order Lifecycle, Cold Recovery, and Maintenance Switchover

1. **Cold Exchange REST Bootstrap (Zero Local WAL)**: At startup, `OrderRoutingSystem::build` invokes `UtaVenueSession::bootstrap_snapshots`, fetching authenticated Bybit V5 (`/v5/account/wallet-balance`, `/v5/position/list`, `/v5/order/realtime`) or OKX V5 (`/api/v5/account/balance`, `/api/v5/account/positions`, `/api/v5/trade/orders-pending`) snapshots and decoding 32-character `tw1...` `SelfDescribingClientId` tokens via `UtaBootstrapDecoder` to reconstruct open-order holds, derivative/option positions, `OrsStpBook` entries, and per-strategy `next_order_seqnum` watermarks.
2. **Hot `REQ` Admission (`req_cpu`)**: `ReqWorker` polls strategy `1,024B` `IntentRecord` rings, enforces the default `23,000 ns` (`23 µs`) microsecond stale-order gate (`RefusalReason::RejectStaleOrder`), checks `OrsRuntimePolicy` and `RmsEngine`, reserves wallet collateral and checks atomic BBO self-trade in `OrderManager::place_order`, short-circuits `dry_run` after stack wire encoding without spending quota or writing to sockets, and dispatches live frames via `LockedConnection::send_frame` with zero overlap between `Order` and `Connection` spinlocks.
3. **Hot `RESP` Decoding (`resp_cpu`) & `OrsQueryDesk`**: `RespWorker` scans inbound WebSocket and HTTP payloads in one zero-allocation `JsonScan` pass (`scan_bybit_frame` / `scan_okx_frame`), keeping gateway receipts (`FrontAck`), book acceptance (`Ack`), cumulative order-status projections (`ProjectedFill`), and trade executions (`Fill`, setting `amendment = true` when full `execution` fee/rebate details follow `execution.fast`) distinct. Ambiguous replies (`50004`, `10000`, `170007`) and socket drops schedule per-order `OrsQueryDesk` `StatusAsk` queries on `AsyncRestQuerier` (`housekeeping_cpus`). Server maintenance notices (OKX `64008` / Bybit link retirement) set `graceful_drain_connection = true`, immediately unpublishing the draining link, activating the `Dialer` standby slot, and closing the old socket once in-flight count hits `0`.
4. **Signal Lifecycle**: `OrsProcess` handles `SIGUSR1` (off-thread static RMS reload + `QueryOpen` + `Dump`), `SIGUSR2` (`Mode::ManageOnly` + emergency `CancelAll` + `OrsRestricted`), and `SIGINT`/`SIGTERM` (`2s` open-order/RFQ-quote drain + `OrsShutDown`).

## Local Loopback Paper Exchange

Run the self-contained stateless TLS loopback workflow for either UTA venue:

```sh
cargo run --locked -p tickwire-spot-runner -- paper bybit
cargo run --locked -p tickwire-spot-runner -- paper okx
```

Each invocation starts an ephemeral TLS peer (`paper_peer.rs`) serving cold REST `Account + OpenOrders` snapshots and pooled HTTPS order entry, spawns `OrsProcess<UtaVenueSession>`, completes a `DopSession` login, submits two Spot limit buy orders over inline SHM rings, verifies HMAC-SHA256 headers and `tw1...` `SelfDescribingClientId` MAC attribution on the wire, observes `ReportRecord` `Ack` replies, and executes graceful `SIGTERM` shutdown with zero disk files.

## Verification and References

```sh
cargo test --locked -p tickwire-spot-runner --all-targets
```

* Implementation modules: [UtaVenueSession](../../crates/tickwire-spot-runner/src/session.rs), [SpotRunnerConfig](../../crates/tickwire-spot-runner/src/config.rs), [OrsProcess runner](../../crates/tickwire-spot-runner/src/runner.rs), [CLI entrypoint](../../crates/tickwire-spot-runner/src/main.rs), [loopback TLS paper peer](../../crates/tickwire-spot-runner/src/paper_peer.rs)
* Test suites and related specs: [UTA ORS runtime integration tests](../../crates/tickwire-spot-runner/tests/uta_ors_runtime.rs), [Bybit and OKX UTA execution](spot-execution.md), [stateless UTA REST bootstrap and `OrsQueryDesk` reconciliation](spot-rest-recovery.md)
