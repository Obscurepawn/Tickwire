# L0 TLC run log

Re-run all four configurations from the workspace root via `./scripts/check-tla.sh system_clock` (or `python3 tla/execution_models/system_clock/L0_native_owner/04_evidence/run.py`, single worker, `2 GiB` heap, pinned `tla/tools/tla2tools.jar` SHA-256 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`). All four configurations check 14 safety invariants:

| Config | Scenario | Generated | Distinct | Depth | Wall time (s) | Result |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `L0_NativeOwner.cfg` | Native provider, readings `0..2`, max 5 calls | 6,267 | 3,021 | 7 | 2.360 | PASS |
| `L0_NativeOwner_generic.cfg` | Generic provider, readings `0..2`, max 5 calls | 17,278 | 2,431 | 7 | 1.895 | PASS |
| `L0_NativeOwner_long.cfg` | Native provider, readings `0..3`, max 7 calls | 49,767 | 18,714 | 9 | 3.304 | PASS |
| `L0_NativeOwner_unsupported.cfg` | Unsupported platform (`new` fails) | 2 | 2 | 2 | 1.576 | PASS |

Artifact digests are recorded in [results.json](04_evidence/results.json), with initial pre-split generic wrapper logs preserved in [runs/initial](04_evidence/runs/initial/).

## Action coverage

```sh
python3 scripts/tlc_coverage.py --model L0_NativeOwner \
  --model-file tla/execution_models/system_clock/L0_native_owner/02_models/L0_NativeOwner.tla \
  tla/execution_models/system_clock/L0_native_owner/04_evidence/L0_NativeOwner.log \
  tla/execution_models/system_clock/L0_native_owner/04_evidence/L0_NativeOwner_generic.log \
  tla/execution_models/system_clock/L0_native_owner/04_evidence/L0_NativeOwner_long.log \
  tla/execution_models/system_clock/L0_native_owner/04_evidence/L0_NativeOwner_unsupported.log
```

The Native configurations cover `Init` + 14 `Next` actions, Generic covers `Init` + 6, and Unsupported covers `Init` + 1; the union across all four configurations covers `Init` + all 20 `Next` actions with `generated > 0`, archived in [coverage.json](04_evidence/coverage.json).
