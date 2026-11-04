# L0_CommitGate model checks

Run `./scripts/check-tla.sh submission` from the repository root. The November 4, 2026 run exhausted these configurations with TLC action coverage.

| Configuration | Orders / slots / capital | Crash limit | States generated | Distinct states | Depth |
| --- | --- | ---: | ---: | ---: | ---: |
| [primary](02_models/L0_CommitGate.cfg) | 2 / 2 / 3 | – | 9255 | 1044 | 13 |
| [scaling](02_models/L0_CommitGate_scaling.cfg) | 3 / 2 / 4 | – | 44575 | 4108 | 13 |

All 11 transitions and `Init` generated successors across the 2 runs. Model SHA-256 is `5361e2378eac0e791afcd16fbc3b885b68a110d92fe51a3385031fc7468d93f7`. The [prior run](https://github.com/Obscurepawn/Tickwire/blob/dc50649ebdcada26f7740181c16139cd3b32064a/tla/execution_models/submission/L0_commit_gate/03_run_log.md) used the former commit profiles.
