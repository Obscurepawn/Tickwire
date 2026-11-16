# L0 Bybit V5 & OKX V5 UTA ORS Bootstrap Verdict

- **Status**: **PASS** (TLC exhaustive finite-state verification with 100% `Next` action coverage)
- **Model**: [`02_models/L0_UtaOrsBootstrap.tla`](02_models/L0_UtaOrsBootstrap.tla)
- **Reasoning**: [`01_reasoning.md`](01_reasoning.md)
- **Run Log**: [`03_run_log.md`](03_run_log.md)

## Verified Properties

- Bybit V5 and OKX V5 `UtaAccountSession` cold REST bootstrap accurately seeds multi-wallet `AssetKey` balances (`available + locked`), isolates unmanaged foreign orders from `tw1...` strategy slots, and recovers `DopState` sequence floors across cold restarts with zero local disk state.
- `OrderRoute.product_mask` routing respects `Draining` connection failover and `PooledHttps` vs `WebSocketTrade` inflight caps, while `execution.fast` -> `execution` `amendment = true` fee settlement preserves `FillRecordPool` lot idempotency and multi-wallet solvency.
