# TLC runs

TLC 2.19 from the repository-pinned jar (`tla/tools/tla2tools.jar`), single worker (`-workers 1`), breadth-first search with action coverage (`-coverage 1`). From the workspace root run `./scripts/check-tla.sh hotpath_ultralow_latency`.

| Profile | Generated | Distinct | Depth | Remaining | Runtime |
| --- | ---: | ---: | ---: | ---: | ---: |
| [Default `crc32`-disabled SPMC zero-copy `&MarketRecord` + WAL SHA-256 recovery (`L0_ZeroCopyAndCrcGate.cfg`)](04_evidence/L0_ZeroCopyAndCrcGate.log) | 1,154,681 | 288,000 | 27 | 0 | 19s |
| [Opt-in `--features crc32` strict checksum SPMC zero-copy + bit-flip fault rejection (`L0_ZeroCopyAndCrcGate_CrcOn.cfg`)](04_evidence/L0_ZeroCopyAndCrcGate_CrcOn.log) | 3,740,035 | 887,900 | 28 | 0 | 51s |

Both searches verified all five invariants (`TypeOK`, `ZeroNoAliasWhileBorrowed`, `ZeroCopyLayoutAndHeaderSoundness`, `CrcGateModeConsistency`, `WalRecoverySoundWithoutCrc`) to exhaustion (`0 states left on queue`). Every one of the 13 `Next` actions (`14` coverage entries including `Init`) fired with `> 0` generated successor states across the completed configurations. [Aggregate action coverage](04_evidence/coverage.json)
