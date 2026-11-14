# TLC runs

TLC 2.19 from the repository-pinned jar (`tla/tools/tla2tools.jar`), single worker (`-workers 1`), breadth-first search. From the workspace root run `./scripts/check-tla.sh connection_router_quota`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Two local source IPs, `Order`/`Cancel`/`Request` separation, HTTP `429` IP failover, HTTP `418` ban, & rollback (`L1_MultiIpQuota.cfg`)](04_evidence/L1_MultiIpQuota.log) | 115,487 | 20,428 | 12 | 0 | 17s |
| [Single-IP bounded rolling ring (`RingCap = 2`), burst coalescing, duplicate-`RateLimitId` batch spend, & sliding eviction (`L1_MultiIpQuota_rolling_coalesce.cfg`)](04_evidence/L1_MultiIpQuota_rolling_coalesce.log) | 10,872 | 2,349 | 10 | 0 | 3s |

Both searches verified all six invariants (`TypeOK`, `WindowCapsRespected`, `TransactionalRollbackExact`, `CanAffordMatchesAllow`, `CancelAndQueryIsolatedFromOrderCap`, `PerIpCooldownIsolation`) to exhaustion (`0 states left on queue`). Every one of the 10 `Next` actions (`11` coverage entries including `Init`) fired with both `> 0` distinct states and `> 0` generated successor states across the completed configurations. [Aggregate action coverage](04_evidence/coverage.json)
