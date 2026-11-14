# TLC runs

TLC 2.19 from the repository-pinned jar (`tla/tools/tla2tools.jar`), single worker (`-workers 1`), breadth-first search. From the workspace root run `./scripts/check-tla.sh query_desk`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Two orders, single slot, Bybit two-step `realtime -> history` & concurrent WS (`VenueIsBybit = TRUE`)](04_evidence/L1_QueryConvergence.log) | 5,929,082 | 2,339,676 | 20 | 0 | 02m 02s |
| [Single order, `OpenPartFill` coalescing & `MaxCancelAsks` / `MaxQueryCount` bounds (`VenueIsBybit = FALSE`)](04_evidence/L1_QueryConvergence_coalesce.log) | 63,772 | 29,863 | 18 | 0 | 02s |

Both searches verified all seven invariants (`TypeOK`, `AtMostOneAskAndOneSlotPerOrder`, `IssuedOnlyCountsSent`, `TradesBeforeFeeClosure`, `CancelAfterNeverLost`, `BybitTwoStepSoundness`, `BoundedEscalationToFreeze`) to exhaustion (`0 states left on queue`). Every one of the 18 `Next` actions (`19` coverage entries including `Init`) fired with both `> 0` distinct states and `> 0` generated successor states across the completed configurations. [Aggregate action coverage](04_evidence/coverage.json)
