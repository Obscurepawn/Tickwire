# TLC Runs

TLC 2.19 from the repository-pinned jar (`tla/tools/tla2tools.jar`), single worker (`-workers 1`), breadth-first search. From the workspace root run `./scripts/check-tla.sh hotpath_decoupling`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Two-tick window, single order slot, two associative fill ways](02_models/L1_WindowedPool.cfg) | 320,897 | 51,469 | 22 | 0 | 14s |
| [Single-tick window, three competing orders, single associative fill way](02_models/L1_WindowedPool_burst.cfg) | 2,369,774 | 340,142 | 23 | 0 | 01min 13s |

Both searches verified all five invariants (`TypeOK`, `ActiveNeverEvicted`, `WindowDedupPreserved`, `FillAmendmentExactness`, `ManagementNeverStarved`) to exhaustion (`0 states left on queue`). Every one of the 16 `Next` actions (`17` coverage entries including `Init`) fired with `> 0` generated successor states across the completed searches.
