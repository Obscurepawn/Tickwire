# `v2` TLC run log

Reproduce with `./scripts/check-tla.sh binance_private_session` (or the evidence runner `python3 tla/execution_models/binance_private_session/L0_session_lifecycle/04_evidence/run.py`, which runs the pinned `tla2tools.jar` single-worker with a `2 GiB` heap and isolated scratch directories).

| Configuration | Generated | Distinct | Depth | Wall time (s) | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| `L0_PrivateSession_v2.cfg` | 24,478 | 12,018 | 27 | 2.778 | PASS |
| `L0_PrivateSession_v2_buffer.cfg` | 2,244 | 1,043 | 20 | 1.304 | PASS |
| `L0_PrivateSession_v2_logon.cfg` | 3,476 | 1,881 | 28 | 1.371 | PASS |
| `L0_PrivateSession_v2_usdm.cfg` | 536,304 | 267,643 | 34 | 17.798 | PASS |

All 16 safety invariants held with `0` states remaining on queue. Action coverage is taken from the final complete coverage block of each log and unioned across product configurations; per-action counts and artifact SHA-256 digests are recorded in `04_evidence/results.json`, while historical `v1` runs remain under `04_evidence/runs/pre-final-sample-v2/`.

Each log emits 32 coverage entries, and the union across the four configurations covers `Init` + all 33 `Next` actions with `generated > 0` (`GateBudgetExpires` and `FinalSampleExpired` are `0` on USD-M because USD-M `listenKey` management does not run signed `DeliveryBudget` callbacks, and are covered in the Spot configurations).
