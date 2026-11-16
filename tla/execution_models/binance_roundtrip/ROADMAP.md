# Binance ORS Model Levels

| Boundary | Current Level | Result |
| --- | --- | --- |
| Stateless Binance multi-market `AccountSession`, `SelfDescribingClientId` REST bootstrap, `listenKey` stream rotation, and `Order` / `Connection` non-overlapping dispatch | [`L0_binance_ors_bootstrap`](L0_binance_ors_bootstrap/05_output.md) | Safety PASS across Spot (`BalancesIncludeOpenOrderLocks = TRUE`) and Derivatives (`BalancesIncludeOpenOrderLocks = FALSE`) configurations with 100% action coverage |
