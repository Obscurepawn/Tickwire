# TLC runs

TLC 2.19 from the repository-pinned jar (`tla/tools/tla2tools.jar`), single worker (`-workers 1`), breadth-first search. From the workspace root run `./scripts/check-tla.sh market_making_microstructure`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Spot worst-case holds, multi-order `swap_remove`, `client_id` rotation, & 2-phase `new_client_id -> orderId` query (`L0_StpModify.cfg`)](04_evidence/L0_StpModify.log) | 91,568 | 23,923 | 12 | 0 | 4s |
| [Futures `shadow_holds` worst-case margin, simultaneous Buy/Sell dual-price `StpBook`, clamping trim, & rollback (`L0_StpModify_Futures.cfg`)](04_evidence/L0_StpModify_Futures.log) | 916,932 | 232,019 | 12 | 0 | 31s |

Both searches verified all seven invariants (`TypeOK`, `NoSelfTradeCrossWhenEnforced`, `AtomicBboExactness`, `AtMostOnePendingActionPerOrder`, `WorstCaseModifyHoldSolvency`, `ModifyRollbackRestoresExactRestingState`, `TerminalOrderClearsStpAndPendingModify`) to exhaustion (`0 states left on queue`). Every one of the 13 `Next` actions (`14` coverage entries including `Init`) fired with both `> 0` distinct states and `> 0` generated successor states across the completed configurations. [Aggregate action coverage](04_evidence/coverage.json)
