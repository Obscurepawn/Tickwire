# TLC evidence

Run `python3 tla/execution_models/binance_roundtrip/L0_spot_rest/evidence/reproduce.py`. The script invokes the pinned TLC jar with one worker, fingerprint 0, seed 1 and action coverage, and checks the expected result for every run. The two safety configurations also run with `./scripts/check-tla.sh binance_roundtrip`.

| Configuration | Generated | Distinct | Depth | Queued at stop | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| [One credit, duplicate allowed, gaps/expiry enabled](evidence/L0_BinanceSpot.log) | 2,037 | 838 | 40 | 0 | PASS, 1 s |
| [Zero credits, one fill permitted by the environment](evidence/NoQuota.log) | 1,071 | 432 | 20 | 0 | PASS, 1 s |
| [End-to-end completion probe](evidence/NoComplete.log) | 170 | 113 | 27 | 7 | Expected witness, 1 s |
| [Worker sees fill before REST ACK](evidence/NoFillBeforeAck.log) | 128 | 84 | 22 | 3 | Expected witness, 1 s |

Witness runs deliberately negate a reachable condition and stop on the first matching trace. Both safety runs exhaust the queue. The [combined action coverage](evidence/action-coverage.json) binds the counts to the checked model hash.
