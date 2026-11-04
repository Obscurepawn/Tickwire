# TLC runs

TLC 2.19 from the repository-pinned jar, one worker, breadth-first search and fingerprint polynomial 0. From the workspace root run `./scripts/check-tla.sh request_budget`.

| Profile | Generated | Distinct | Depth | Remaining |
| --- | ---: | ---: | ---: | ---: |
| [One window, competing two-unit requests](04_evidence/L0_SharedWindow.log) | 30,344 | 10,420 | 18 | 0 |
| [Two windows, one-unit rival](04_evidence/L0_SharedWindow_roll.log) | 965,993 | 303,062 | 21 | 0 |

Both searches checked all three safety properties to exhaustion. `AddWindow` has no successor in the single-window configuration. It fires in the two-window configuration. Every other action fires in both. [Aggregate action coverage](04_evidence/coverage.json)
