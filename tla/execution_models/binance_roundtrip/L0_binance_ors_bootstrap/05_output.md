# L0 Binance ORS Stateless Bootstrap & Multi-Market Session Verdict

- **Status**: **PASS** (TLC exhaustive finite-state verification with 100% `Next` action coverage)
- **Model**: [`02_models/L0_BinanceOrsBootstrap.tla`](02_models/L0_BinanceOrsBootstrap.tla)
- **Reasoning**: [`01_reasoning.md`](01_reasoning.md)
- **Run Log**: [`03_run_log.md`](03_run_log.md)

## Verified Properties

- Stateless REST `Account + OpenOrders` bootstrap paired with `SelfDescribingClientId` (`tw1...`) reconstructs open-order holds, strategy attribution, and `DopState` sequence floors without local disk state across both Spot (`BalancesIncludeOpenOrderLocks = TRUE`) and Derivatives (`FALSE`).
- `Order` and `Connection` spinlocks never overlap, `GuardedValue` `listenKey` rotation stays coherent, and `FrontAck` / `Ack` / `ProjectedFill` / `Fill` remain distinct deduplicated facts.
