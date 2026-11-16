# TLC Runs

TLC 2.19 from the repository-pinned jar (`tla/tools/tla2tools.jar`), single worker (`-workers 1`), breadth-first search. From the workspace root run `./scripts/check-tla.sh hotpath_decoupling`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Single-slot ring, two tasks, two reconnect generations](02_models/L0_DialQuerier.cfg) | 160,284 | 42,272 | 27 | 0 | 02s |
| [Two-slot ring, three tasks, deadline-bounded retries](02_models/L0_DialQuerier_scale.cfg) | 1,200,129 | 283,734 | 31 | 0 | 15s |

Both searches verified all five invariants (`TypeOK`, `ExclusiveBatonOwnership`, `FreshLinkHandoff`, `ReplySlotReservation`, `CountedWritesOnly`) to exhaustion (`0 states left on queue`). Every one of the 20 `Next` actions (`21` coverage entries including `Init`) fired with `> 0` generated successor states across the completed searches.
