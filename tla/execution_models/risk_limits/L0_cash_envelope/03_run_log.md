# TLC results

TLC exhaustively enumerated two strategy order slots and all integer partial fills at price one. Each row uses one worker, polynomial 0 and the repository-pinned release 1.7.4. The initial state and all four transitions generated states in each run.

| Configuration | Base / quote units | Nontrade locks | Initial base ownership (A, B, residual) | Maximum order | Absolute / gross / member caps | Generated | Distinct | Depth | Queued |
| --- | --- | --- | --- | ---: | --- | ---: | ---: | ---: | ---: |
| Primary | 2 / 3 | 0 / 1 | 2, 0, 0 | 3 | 3 / 5 / 3 | 626 | 278 | 7 | 0 |
| Scaling | 3 / 4 | 1 / 1 | 3, −1, 1 | 4 | 5 / 7 / 4 | 861 | 327 | 7 | 0 |

Each run finished in one second. Run `./scripts/check-tla.sh risk_limits` from the repository root. The recorded runs used `-workers 1 -fp 0 -coverage 1 -fpmem 0.05` and a 400 MB JVM heap. [Primary excerpt](evidence/L0_CashEnvelope.log) · [Scaling excerpt](evidence/L0_CashEnvelope_scale.log) · [Validated action coverage](evidence/coverage.json)
