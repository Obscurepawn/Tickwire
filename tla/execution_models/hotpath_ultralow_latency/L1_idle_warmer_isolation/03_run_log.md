# TLC runs

TLC 2.19 from the repository-pinned jar (`tla/tools/tla2tools.jar`), single worker (`-workers 1`), breadth-first search with action coverage (`-coverage 1`). From the workspace root run `./scripts/check-tla.sh hotpath_ultralow_latency`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Enabled idle-window warmer (`WarmupInterval = 2`) interleaved with live ticks, orders, acks, & pings (`L1_IdleWarmerIsolation.cfg`)](04_evidence/L1_IdleWarmerIsolation.log) | 151,624 | 45,550 | 18 | 0 | 3s |
| [Disabled warmer policy (`WarmupInterval = 0`) verifying zero warmup runs (`L1_IdleWarmerIsolation_Disabled.cfg`)](04_evidence/L1_IdleWarmerIsolation_Disabled.log) | 14,110 | 3,690 | 16 | 0 | 2s |

Both searches verified all five invariants (`TypeOK`, `LiveBusinessStateInvariance`, `LiveTlsNonceSynchronization`, `DummyLoopbackTlsPairSynchronization`, `WarmupStrictlyRespectsIdlePolicy`) to exhaustion (`0 states left on queue`). Every one of the 8 `Next` actions (`9` coverage entries including `Init`) fired with `> 0` generated successor states across the completed configurations. [Aggregate action coverage](04_evidence/coverage.json)
