# L0 Binance ORS Stateless Bootstrap & Multi-Market Session Reasoning

## Modeling Choices

- **Unified Spot vs Derivatives Balance-Lock Accounting**: `BinanceAccountSession` supports both Spot (`GET /api/v3/account` returning separated `free` and `locked` balances where open-order holds are already reflected in `locked`, `BalancesIncludeOpenOrderLocks = TRUE`) and Derivatives (`UsdMFutures` / `CoinMFutures` / `Options` returning total wallet balance where reconstructed open-order holds are carved out of free margin, `BalancesIncludeOpenOrderLocks = FALSE`). Checking both `.cfg` variants proves neither mode double-locks or under-locks capital.
- **`SelfDescribingClientId` (`tw1...`) Domain-Keyed Attribution**: Models exchange open orders carrying both a valid `tw1...` client ID (which seeds the strategy's `OrderSlot` and advances `DopState::next_order_seqnum`) and an external/manual order (which is isolated from strategy attribution while preserving venue balance accounting).
- **Non-Overlapping Spinlock Dispatch & Zero-Disk Restart**: Splits outbound order placement from wire frame transmission to verify `~(order_lock_held /\ conn_lock_held)` and includes `StatelessColdRestart` wiping all in-memory ORS state and recovering solely from the live exchange snapshot.
