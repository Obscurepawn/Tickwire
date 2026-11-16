# UTA ORS Roundtrip Levels

| Level | Contract | Result |
| --- | --- | --- |
| [`L0_uta_ors_bootstrap`](L0_uta_ors_bootstrap/05_output.md) | Stateless Bybit V5 / OKX V5 UTA REST bootstrap, `AssetKey` multi-wallet holds, `product_mask` connection failover (`WebSocketTrade` + `PooledHttps`), fast-exec fee amendment, and zero-disk restart | Safety PASS across both `WebSocketTrade` (`MaxInflightPerConn = 2`) and `PooledHttps` (`MaxInflightPerConn = 1`) configurations with 100% action coverage |
