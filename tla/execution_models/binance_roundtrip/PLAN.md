# Binance ORS Bootstrap & Multi-Market Session Verification Plan

Source scope: [`crates/tickwire-binance-account`](../../../crates/tickwire-binance-account/src/session.rs), [`crates/tickwire-binance-execution`](../../../crates/tickwire-binance-execution/src/lib.rs), [`crates/tickwire-account`](../../../crates/tickwire-account/src/order_manager.rs), and [`crates/tickwire-venue`](../../../crates/tickwire-venue/src/client_id.rs).

| Level | Focus |
| --- | --- |
| [`L0_binance_ors_bootstrap`](L0_binance_ors_bootstrap/05_output.md) | Stateless cold REST bootstrap (`Account + OpenOrders`) with `BalancesIncludeOpenOrderLocks` (`Spot` `free + locked` vs derivatives wallet balance), `SelfDescribingClientId` (`tw1...`) domain-keyed MAC attribution vs unmanaged foreign orders (`managed_only`), `DopState` watermark recovery, `GuardedValue` `listenKey` / user-stream lifecycle, non-overlapping `Order` -> `Connection` spinlock order dispatch, distinct `FrontAck` / `Ack` / `ProjectedFill` (`cumQty`) / `Fill` (`tradeId`) progression, and zero-disk cold restart recovery |
