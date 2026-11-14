# TLC runs

TLC 2.19 from the repository-pinned jar (`tla/tools/tla2tools.jar`), single worker (`-workers 1`), breadth-first search. From the workspace root run `./scripts/check-tla.sh connection_router_quota`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Two `Exclusive` REST slots, warmup round-robin, `9/8` EWMA hysteresis, & reconnect fencing (`L0_PoolRouter.cfg`)](04_evidence/L0_PoolRouter.log) | 74,860 | 27,599 | 13 | 0 | 2s |
| [Hybrid `Exclusive` REST + multiplexed `Shared` WS (`MaxShared = 2`, out-of-order `WsRequestId`, `draining_eof`) (`L0_PoolRouter_ws_shared.cfg`)](04_evidence/L0_PoolRouter_ws_shared.log) | 49,081 | 18,129 | 13 | 0 | 2s |

Both searches verified all eight invariants (`TypeOK`, `ExclusiveSlotBound`, `SharedSlotBound`, `InflightAccountingExact`, `NoCrossEpochResponseAliasing`, `AbortedNeverMarkedInDoubt`, `AmbiguousSentIsolatedInDoubt`, `HysteresisAndWarmupSoundness`) to exhaustion (`0 states left on queue`). Every one of the 12 `Next` actions (`13` coverage entries including `Init`) fired with both `> 0` distinct states and `> 0` generated successor states across the completed configurations. [Aggregate action coverage](04_evidence/coverage.json)
