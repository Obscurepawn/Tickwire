# TLC runs

Run `python3 tla/execution_models/strategy_intake/L0_shared_cash/evidence/reproduce.py` from the repository root. It uses the pinned TLC jar with one worker, fingerprint 0, seed 1, a 320 MiB Java heap and coverage enabled. The model's safety configurations also run via `./scripts/check-tla.sh strategy_intake`. The intentional failure lives in the evidence directory.

| Config | Generated | Distinct | Depth | Remaining | TLC wall time | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| [One cash unit](evidence/L0_SharedCash.log) | 624 | 480 | 10 | 0 | <1 s | PASS |
| [Two units and one restart](evidence/L0_SharedCash_restart.log) | 4,294 | 2,968 | 17 | 0 | 1 s | PASS |
| [Peer isolation probe](evidence/PeerCanContinue.log) | 646 | 523 | 8 | 84 | <1 s | Violation |

The final coverage blocks across the two safety runs contain positive successor counts for Init and all eleven Next actions. The one-unit run exercises 80 insufficient-cash refusals. The second run covers 900 restart successors. [Action totals and model digest](evidence/action-coverage.json)

Three consecutive [native process runs](evidence/native/run.log) reproduced the unauthorized management failure on both Bybit and OKX. All six account processes reported a live market view for the selected trigger before buy submission. Each process exited with status 1 after an acknowledged buy. [Inputs and replay instructions](04_counterexamples.md#native-reproduction-on-bybit-and-okx)
