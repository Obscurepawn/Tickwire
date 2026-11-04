# Economic facts model checks

The October 20 v2 runs exhausted four configurations at the [fixed source revision](https://github.com/Obscurepawn/Tickwire/tree/04351dbb59522c43d6b7a1b27366c4a2ee89ed9c/tla/execution_models/submission/L2_economic_facts/). Every configuration permits one restart and uses an arithmetic upper bound of 1,000.

| Configuration | Orders / event slots / journal facts | Capital | Distinct states | Depth |
| --- | --- | ---: | ---: | ---: |
| [Primary log](04_evidence/v2/primary.log) | 1 / 2 / 3 | 3 | 24,036 | 8 |
| [Boundary log](04_evidence/v2/boundary.log) | 2 / 2 / 3 | 1,000 | 1,495,032 | 11 |
| [Long history log](04_evidence/v2/long.log) | 1 / 3 / 4 | 3 | 280,458 | 9 |
| [Scaling log](04_evidence/v2/scaling.log) | 2 / 2 / 3 | 6 | 1,485,288 | 11 |

The [manifest](04_evidence/v2/source-and-model.sha256) records the source, test, model and configuration digests. Run `./scripts/check-tla.sh submission` from the workspace root to check the same economic model alongside the local submission gates. [Original action counts and review](https://github.com/Obscurepawn/Tickwire/tree/04351dbb59522c43d6b7a1b27366c4a2ee89ed9c/tla/execution_models/submission/L2_economic_facts/03_run_log.md)

The November 4, 2026 rerun exhausted all four configurations at the same distinct-state counts and depths. All 18 transitions and `Init` generated successors. Model SHA-256 is `1167e344a32381fee08ff9635dc2f1c3c9f4ca5b48adc23f156496045161aa29`.
