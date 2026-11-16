# L1 TLC run log

Checked with TLC 2.19 (official v1.7.4 release, `tla/tools/tla2tools.jar`), single worker, exhaustive BFS with isolated temporary state directories. Re-run via `./scripts/check-tla.sh control_requests`.

| Config | Generated | Distinct | Depth | Result |
| --- | ---: | ---: | ---: | --- |
| `L1_ControlSession.cfg` | 28,744 | 1,566 | 8 | PASS |
| `L1_ControlSession_renew.cfg` | 76,195 | 3,804 | 9 | PASS |
| `L1_ControlSession_two.cfg` | 103,144 | 3,926 | 8 | PASS |

Full TLC logs live under `04_evidence/*.log` with every `Next` action generating successors (`generated > 0`); structured counts and SHA-256 digests are recorded in `04_evidence/results.json`.
