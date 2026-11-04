# L1_CrashReceipt model checks

Run `./scripts/check-tla.sh submission` from the repository root. The November 4, 2026 run exhausted these configurations with TLC action coverage.

| Configuration | Orders / slots / capital | Crash limit | States generated | Distinct states | Depth |
| --- | --- | ---: | ---: | ---: | ---: |
| [primary](02_models/L1_CrashReceipt.cfg) | 1 / 1 / 2 | 1 | 2535 | 784 | 13 |
| [pending](02_models/L1_CrashReceipt_pending.cfg) | 2 / 2 / 4 | 1 | 199503 | 53544 | 22 |
| [scaling](02_models/L1_CrashReceipt_scaling.cfg) | 2 / 2 / 3 | 2 | 100891 | 28096 | 22 |

All 19 transitions and `Init` generated successors across the 3 runs. Model SHA-256 is `592aadd7ba87526fe0c314d7e78a2bab1d73e1cfdff95980c80d855b4a992dc4`. The [prior run](https://github.com/Obscurepawn/Tickwire/blob/dc50649ebdcada26f7740181c16139cd3b32064a/tla/execution_models/submission/L1_crash_receipt/03_run_log.md) used the former commit profiles.
