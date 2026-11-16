# L0 Bybit V5 & OKX V5 UTA ORS Bootstrap Reasoning

## Modeling Choices

- **Unified Multi-Product `UtaAccountSession` Pool (`WebSocketTrade` & `PooledHttps`)**: Models two venue trade connections (`conn 0` and `conn 1`) with `UtaMaintenanceDrainConn0` transitioning `conn 0` from `Ready` to `Draining` while inflight replies drain, forcing new multi-product (`Spot` / `Linear`) orders onto `conn 1` without dropping inflight acknowledgements.
- **Transport Multiplexing Configurations**:
  - [`L0_UtaOrsBootstrap.cfg`](02_models/L0_UtaOrsBootstrap.cfg): Models multiplexed `WebSocketTrade` (`MaxInflightPerConn = 2`).
  - [`L0_UtaOrsBootstrap_https.cfg`](02_models/L0_UtaOrsBootstrap_https.cfg): Models HTTP/1.1 keep-alive `PooledHttps` (`MaxInflightPerConn = 1`, `inflight == 0` selection rule).
- **Two-Stage `execution.fast` -> `execution` Settlement & Stateless Restart**: Explicitly separates first-arrival fill execution (`UtaPrivateWsExecFirstArrival`, `fee = 0 / None`) from subsequent full `execution` fee/rebate amendment or duplicate delivery (`UtaPrivateWsExecAmendOrDup`), and verifies `UtaStatelessColdRestart` from live exchange state with zero local disk files.
