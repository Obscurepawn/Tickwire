# TLC runs

TLC 2.19 from the repository-pinned jar (`tla/tools/tla2tools.jar`), single worker (`-workers 1`), breadth-first search with action coverage (`-coverage 1`). From the workspace root run `./scripts/check-tla.sh hotpath_ultralow_latency`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Bybit `orderbook.1` SIMD BBO decode + `release` profile overflow guard (`L2_SimdDecodeAndOverflowGuard.cfg`)](04_evidence/L2_SimdDecodeAndOverflowGuard.log) | 27,369 | 998 | 7 | 0 | 4s |
| [OKX `bbo-tbt` SIMD BBO decode + `bench` profile overflow guard (`L2_SimdDecodeAndOverflowGuard_OkxBench.cfg`)](04_evidence/L2_SimdDecodeAndOverflowGuard_OkxBench.log) | 27,369 | 998 | 7 | 0 | 4s |

Both searches verified all five invariants (`TypeOK`, `ExactOracleEquivalence`, `TickLotAndPositiveSpreadSoundness`, `NoSilentArithmeticWrap`, `ZeroHotPathHeapAllocation`) to exhaustion (`0 states left on queue`). Every one of the 11 `Next` actions (`12` coverage entries including `Init`) fired with `> 0` generated successor states across the completed configurations. [Aggregate action coverage](04_evidence/coverage.json)
