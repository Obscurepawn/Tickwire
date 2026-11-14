# TLC runs

TLC 2.19 from the repository-pinned jar (`tla/tools/tla2tools.jar`), single worker (`-workers 1`), breadth-first search. From the workspace root run `./scripts/check-tla.sh market_making_microstructure`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Three-trade out-of-order status/fill interleavings, `LimitFallback` + `CumulativeQuote` denominators, duplicate `trade_id`, & `Modify` qty increase (`L1_ProjectedFill.cfg`)](04_evidence/L1_ProjectedFill.log) | 3,079 | 676 | 14 | 0 | <1s |
| [`CHECK_STP` opposite-side `self_cross` projection suppression & cancel unblocking (`L1_ProjectedFill_SelfCross.cfg`)](04_evidence/L1_ProjectedFill_SelfCross.log) | 1,319 | 356 | 12 | 0 | <1s |

Both searches verified all seven invariants (`TypeOK`, `MonotonicWatermarks`, `ZeroDoubleCountInventorySkew`, `SettledLedgerUnpollutedByProjection`, `StaleStatusIsNoOp`, `SelfCrossSuppressesProjection`, `ExactIncrementalGapPriceDenominator`) to exhaustion (`0 states left on queue`). Every one of the 10 `Next` actions (`11` coverage entries including `Init`) fired with both `> 0` distinct states and `> 0` generated successor states across the completed configurations. [Aggregate action coverage](04_evidence/coverage.json)
