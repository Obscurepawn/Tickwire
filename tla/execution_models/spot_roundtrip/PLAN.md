# Bybit V5 & OKX V5 UTA ORS Bootstrap Verification Plan

Source scope: [`crates/tickwire-spot-execution`](../../../crates/tickwire-spot-execution/src/session.rs), [`crates/tickwire-spot-execution/src/rest_bootstrap.rs`](../../../crates/tickwire-spot-execution/src/rest_bootstrap.rs), [`crates/tickwire-account`](../../../crates/tickwire-account/src/order_manager.rs), and [`crates/tickwire-venue`](../../../crates/tickwire-venue/src/client_id.rs).

| Level | Focus |
| --- | --- |
| [`L0_uta_ors_bootstrap`](L0_uta_ors_bootstrap/05_output.md) | Stateless Bybit V5 / OKX V5 Unified Trading Account (`UtaAccountSession`) cold REST bootstrap across multi-wallet `AssetKey { wallet_id, asset_id }` balances (`available_balance + locked_balance`), `SelfDescribingClientId` (`tw1...`) managed vs foreign open-order attribution, `OrderRoute.product_mask` (`Spot` / `Linear` / `Inverse` / `Options`) connection-pool selection (`Ready` vs `Draining` graceful failover and `PooledHttps` `inflight == 0` multiplexing vs `WebSocketTrade`), Bybit `execution.fast` (`fee: None`) followed by full `execution` `amendment = true` fee settlement, and zero-disk cold restart recovery |
