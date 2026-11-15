# Explicit TLC Runs — `L0_DepthSync`

Frozen module SHA-256: `5cf86194dd707b058fdebdd0b4d5255b7f491445b73c78c09bf53c59218f5c97`. All four configurations (`Spot` and `USD-M` primary + buffered `Capacity = 2` scaling) pass completely with zero states remaining on queue and 100% action coverage (`23/23` entries: `Init` + all 22 `Next` actions).

Reproduce from the workspace root with `./scripts/check-tla.sh binance_depth_sync`:

| Configuration | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | --- |
| [Spot primary (`02_models/L0_DepthSync.cfg`)](02_models/L0_DepthSync.cfg) | 1,851,021 | 73,269 | 14 | 0 | `08s` (`-workers 4 -coverage 60`) |
| [USD-M primary (`02_models/L0_UsdM.cfg`)](02_models/L0_UsdM.cfg) | 1,876,613 | 73,897 | 13 | 0 | `08s` (`-workers 4 -coverage 60`) |
| [Spot buffered scaling (`02_models/L0_SpotBuffered.cfg`, `Capacity = 2`)](02_models/L0_SpotBuffered.cfg) | 75,516,208 | 3,491,866 | 14 | 0 | `01m 12s` (`-workers 8`, `OffHeapDiskFPSet` + congruent `DepthSyncView` + `@TLAPlusOperator`) |
| [USD-M buffered scaling (`02_models/L0_UsdMBuffered.cfg`, `Capacity = 2`)](02_models/L0_UsdMBuffered.cfg) | 75,549,956 | 3,492,316 | 14 | 0 | `01m 14s` (`-workers 8`, `OffHeapDiskFPSet` + congruent `DepthSyncView` + `@TLAPlusOperator`) |

- **Acceleration & exact bisimulation soundness**: Without modifying `02_models/L0_DepthSync.tla`, [`scripts/check-tla.sh`](../../../../scripts/check-tla.sh) and [`scripts/tlc_native_overrides.py`](../../../../scripts/tlc_native_overrides.py) compile `@TLAPlusOperator` bytecode overrides (`L0_DepthSyncOverrides`) and apply the congruent quotient `DepthSyncView == <<s, issued, DepthSyncObs>>` on scaling runs (`8.53x` state reduction from `>32M` unquotiented states, cutting wall time from `>3.5h` paused runs to `~72s`).
- **Evidence**: Completed 4-configuration output in [final-runner.log](04_evidence/final-runner.log) and aggregate action counts in [coverage.json](04_evidence/coverage.json) (`min_count = 7,680` across all 22 `Next` actions). Historical single-worker logs and pause records remain archived under `runs/`.
