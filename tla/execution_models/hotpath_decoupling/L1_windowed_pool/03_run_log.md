# TLC runs

TLC 2.19 from the repository-pinned jar (`tla/tools/tla2tools.jar`), single worker (`-workers 1`), breadth-first search, and fingerprint polynomial 0. From the workspace root run `./scripts/check-tla.sh hotpath_decoupling`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Two-tick window, single order slot, multi-slot management reclaim](04_evidence/L1_WindowedPool.log) | 517,004 | 79,500 | 25 | 0 | 16s |
| [Single-tick window, three competing orders across two slots](04_evidence/L1_WindowedPool_burst.log) | 4,301,720 | 522,867 | 28 | 0 | 01min 37s |

Both searches verified all five invariants (`TypeOK`, `ActiveNeverEvicted`, `WindowDedupPreserved`, `ManagementNeverStarved`, `FrontierContinuity`) to exhaustion (`0 states left on queue`). Every one of the 15 `Next` actions (`16` coverage entries including `Init`) fired with `> 0` generated successor states in both configurations. [Aggregate action coverage](04_evidence/coverage.json)
