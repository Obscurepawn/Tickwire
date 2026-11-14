# TLC runs

TLC 2.19 from the repository-pinned jar (`tla/tools/tla2tools.jar`), single worker (`-workers 1`), breadth-first search. From the workspace root run `./scripts/check-tla.sh query_desk`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Two orders, lawful venue grace & retention eviction (`AllowContradictedUncreated = FALSE`)](04_evidence/L0_PerOrderIsolation.log) | 641,883 | 265,077 | 20 | 0 | 15s |
| [Two orders, adversarial post-grace contradicted `uncreated` (`AllowContradictedUncreated = TRUE`)](04_evidence/L0_PerOrderIsolation_adversarial.log) | 907,543 | 367,585 | 20 | 0 | 19s |

Both searches verified all seven invariants (`TypeOK`, `ReservationLockedWhileInDoubt`, `SolvencyNeverViolated`, `HealthySiblingUnblocked`, `NoPrematureUncreatedBeforeGrace`, `ContradictedUncreatedFreezes`, `ReplayEquivalence`) to exhaustion (`0 states left on queue`). Every one of the 18 `Next` actions (`19` coverage entries including `Init`) fired with both `> 0` distinct states and `> 0` generated successor states across the completed configurations. [Aggregate action coverage](04_evidence/coverage.json)
