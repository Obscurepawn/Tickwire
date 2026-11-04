# TLC runs

Run `./scripts/check-tla.sh spot_roundtrip` from the repository root. The pinned jar reports TLC 2.19. The audit used one worker, breadth-first exploration and action coverage. All four runs finished with an empty queue. The healthy and single configurations also ran the temporal checker over the full state graph. [Raw output and final coverage](04_evidence/safety-and-progress.log)

| Configuration | Members / reservations | Generated | Distinct | Depth | Result |
| --- | --- | ---: | ---: | ---: | --- |
| contended | 2 / 1, gap and deadline expiry | 3,509 | 903 | 25 | Safety pass |
| healthy | 2 / 2, fair successful venue | 555 | 195 | 25 | Safety and eventual settlement pass |
| restart | 1 / 1, gap, higher-writer restart and deadline expiry | 2,073 | 559 | 22 | Safety pass |
| single | 1 / 1, fair successful venue | 70 | 28 | 16 | Safety and eventual filled roundtrip pass |

TLC reported under one second for each configuration. All 21 Next entries generated successors across the four configurations. Some configurations turn a fault off by construction.

To reproduce the stronger two-fill expectation, copy [the model](02_models/L0_SpotRoundtrip.tla) and [probe configuration](04_evidence/strong_progress.cfg) to a scratch directory. Use the repository jar with `java -cp /absolute/path/to/tla2tools.jar tlc2.TLC -workers 1 -coverage 1 -config strong_progress.cfg L0_SpotRoundtrip.tla`. TLC returned 13 and reported a temporal counterexample. The trace reaches a terminal state at step 19 and stutters at step 20. [Counterexample](04_counterexamples.md)

Run `python3 tla/execution_models/spot_roundtrip/L0_live_account/04_evidence/check_probes.py` to run both expected counterexamples in disposable directories. The local sampling-boundary probe returned 13 at step 10. [Raw action-property trace](04_evidence/SampleBoundary.log)
