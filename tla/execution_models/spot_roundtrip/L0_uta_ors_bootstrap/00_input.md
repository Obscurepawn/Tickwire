# L0 Bybit V5 & OKX V5 UTA ORS Bootstrap Input Scope

## Source Mapping (`crates/tickwire-spot-execution`, `crates/tickwire-account`, `crates/tickwire-venue`)

- Stateless Bybit V5 / OKX V5 Unified Trading Account (`UtaAccountSession`) cold REST bootstrap (`GET /v5/account/wallet-balance` + `/v5/order/realtime` and `GET /api/v5/account/balance` + `/api/v5/trade/orders-pending`), `available_balance + locked_balance` multi-wallet `AssetKey { wallet_id, asset_id }` seeding, and `SelfDescribingClientId` (`tw1...`) open-order attribution:
  - `crates/tickwire-spot-execution/src/rest_bootstrap.rs:28-310`
  - `crates/tickwire-spot-execution/src/rest_bootstrap.rs:420-780`
  - `crates/tickwire-spot-execution/src/session.rs:1180-1560`
  - `crates/tickwire-account/src/order_manager.rs:2658-2804`
  - `crates/tickwire-account/src/uta_ledger.rs:21-145`
  - `crates/tickwire-venue/src/client_id.rs:45-190`
- Multi-product connection-pool selection (`OrderRoute.product_mask`: `Spot`, `Linear`, `Inverse`, `Options`), `Ready` vs `Draining` graceful maintenance failover, `WebSocketTrade` vs `PooledHttps` (`inflight == 0`) request multiplexing, command-channel `FrontAck`, private WS `Ack` / `Canceled`, and Bybit `execution.fast` (`fee: None`) -> `execution` (`amendment = true`) fee settlement:
  - `crates/tickwire-spot-execution/src/session.rs:480-760`
  - `crates/tickwire-spot-execution/src/session.rs:1720-2135`
  - `crates/tickwire-spot-execution/src/session.rs:2540-2980`
  - `crates/tickwire-account/src/fill_record.rs:171-277`

## Target Safety Invariants

1. `TypeOK`: All multi-wallet `AssetKey` balances, connection states, order lifecycles, and fast-exec amendment flags remain within finite bounds.
2. `ProductMaskZeroPoolSound`: Every dispatched order routes strictly to a `Ready` connection whose `product_mask` covers the symbol's product, never exceeding `MaxInflightPerConn` (`1` for `PooledHttps`, `> 1` for `WebSocketTrade`) and holding zero public market-data subscriptions.
3. `UtaMultiWalletSolvency`: Multi-wallet `UtaWalletLedger` `free + locked` balances across collateral wallets match open-order holds, executed fills, and net settled fees.
4. `FastExecAmendmentExact`: Bybit `execution.fast` (`fee: None`) applies fill quantity once and subsequent full `execution` (`amendment = true`) patches only the fee/rebate delta without double-counting executed lots.
5. `ZeroDiskRestartExact`: Cold restart wipes in-memory ORS state and reconstructs `DopState` watermarks (`next_seq > max_open_seq`), managed open-order slots, and multi-wallet holds strictly from the authenticated REST snapshot.
