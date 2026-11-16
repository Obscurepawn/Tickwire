# L0 TLC run log

Checked with TLC 2.19 (official v1.7.4 release, `tla/tools/tla2tools.jar`), single worker, exhaustive BFS with isolated temporary state directories. Re-run via `./scripts/check-tla.sh control_requests`.

| Config | Generated | Distinct | Depth | Result |
| --- | ---: | ---: | ---: | --- |
| `L0_ControlRequests.cfg` | 5,682 | 370 | 6 | PASS |
| `L0_ControlRequests_pending.cfg` | 352,574 | 13,442 | 10 | PASS |
| `L0_ControlRequests_two.cfg` | 28,026 | 1,570 | 8 | PASS |

Full TLC logs live under `04_evidence/*.log` with every `Next` action generating successors (`generated > 0`); structured counts and SHA-256 digests are recorded in `04_evidence/results.json`.
