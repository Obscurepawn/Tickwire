# L0 Binance ORS Stateless Bootstrap & Multi-Market Session Input Scope

## Source Mapping (`crates/tickwire-binance-account`, `crates/tickwire-binance-execution`, `crates/tickwire-account`, `crates/tickwire-venue`)

- Stateless REST `Account + OpenOrders` bootstrap, `balances_include_open_order_locks` (`Spot` `free + locked` vs derivatives wallet balance), `SelfDescribingClientId` (`tw1...`) attribution vs foreign unmanaged orders (`managed_only`), and `DopState` watermark recovery:
  - `crates/tickwire-binance-account/src/bootstrap.rs:28-235`
  - `crates/tickwire-binance-account/src/bootstrap.rs:345-580`
  - `crates/tickwire-account/src/order_manager.rs:2658-2804`
  - `crates/tickwire-account/src/dop_state.rs:193-267`
  - `crates/tickwire-venue/src/client_id.rs:45-190`
- Multi-market `BinanceAccountSession` (`Spot`, `UsdMFutures`, `CoinMFutures`, `Options`), `GuardedValue` `listenKey` user-stream lifecycle, non-overlapping `Order` -> `Connection` spinlocks on outbound dispatch, and distinct `FrontAck` / `Ack` / `ProjectedFill` / `Fill` (`tradeId`) reconciliation:
  - `crates/tickwire-binance-account/src/session.rs:410-585`
  - `crates/tickwire-binance-account/src/session.rs:1450-1790`
  - `crates/tickwire-binance-account/src/session.rs:2530-2890`
  - `crates/tickwire-binance-account/src/msg_parser.rs:1-220`
  - `crates/tickwire-binance-execution/src/lib.rs:36-65`

## Target Safety Invariants

1. `TypeOK`: All bootstrap flags, `listenKey` epochs, spinlock owners, order states, and wallet balances stay within finite bounds.
2. `LockHierarchyDisjoint`: `Order` and `Connection` spinlocks never overlap (`~(order_lock_held /\ conn_lock_held)`).
3. `ManagedAttributionSound`: Foreign orders lacking a valid domain-keyed `tw1...` MAC never increment strategy `DopState` watermarks or enter managed strategy slots (`managed_only`), while their venue holds remain accounted for without double-deducting `Spot` `locked` balances.
4. `DopWatermarkMonotonic`: Cold REST bootstrap and subsequent cold restarts advance `DopState::next_order_seqnum` strictly beyond every open `tw1...` order (`next_seq > max_open_seq`).
5. `DistinctExecFactsSound`: `FrontAck`, `Ack`, `ProjectedFill` (`cumQty`), and `Fill` (`tradeId`) remain distinct, deduplicated facts (`filled_lots <= projected_lots <= orig_lots`).
6. `ListenKeyGuardedCoherent`: Active user-stream subscriptions always reference a valid `GuardedValue` `listenKey` epoch.
7. `StatelessBootstrapSolvency`: Both `BalancesIncludeOpenOrderLocks = TRUE` (`Spot`) and `FALSE` (`UsdM`/`CoinM`/`Options`) preserve exact wallet `free + locked` conservation across cold restarts with zero local disk state.
