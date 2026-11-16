# L0 TLC run log

Checked with TLC 2.19 (official v1.7.4 release, `tla/tools/tla2tools.jar`), single worker (`-workers 1`), zero fairness assumptions. Artifact SHA-256 digests are recorded in [results.json](04_evidence/results.json). Re-run from the repository root via `./scripts/check-tla.sh broadcast_ring`.

| Config | Distinct | Generated | Depth | Wall time | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| `L0_BroadcastRing.cfg` | 29,889 | 196,588 | 25 | 3 s | PASS |
| `L0_BroadcastRing_two_readers.cfg` | 930,364 | 9,587,623 | 33 | 92 s | PASS |
| `L0_BroadcastRing_wrap.cfg` | 11,324 | 71,291 | 21 | 3 s | PASS |
| `L0_BroadcastRing_pending_wrap.cfg` | 12,834 | 84,451 | 21 | 3 s | PASS |
| `L0_BroadcastRing_wrap_two.cfg` | 40,796 | 341,833 | 24 | 6 s | PASS |

All five exhaustive BFS runs verified all nine safety invariants (`0` states remaining on queue).

## Action coverage

Generated successor counts from the final coverage block of each run (`Init` listed separately; `SuffixNext` entries in wrap configurations exclude transitions already completed in the reachable prefix):

| Action | `primary` | `two_readers` | `wrap` | `pending_wrap` | `wrap_two` |
| --- | ---: | ---: | ---: | ---: | ---: |
| `Init` | 1 | 1 | 1 | 1 | 1 |
| `Join` | 40 | 5368 | prefix | prefix | prefix |
| `Ready` | 2604 | 309960 | prefix | 424 | prefix |
| `Observe` | 14796 | 970340 | 3744 | 4724 | 41552 |
| `Claim` | 7643 | 85848 | 4914 | 6424 | 12360 |
| `Write` | 5362 | 47543 | 2886 | 2886 | 3136 |
| `Publish` | 2471 | 23434 | 1314 | 1314 | 1568 |
| `Abort` | 7833 | 70977 | 4200 | 4200 | 4704 |
| `Prepare` | 19048 | 596834 | 7376 | 9188 | 30264 |
| `Freeze` | 12532 | 560970 | 3436 | 4040 | 20960 |
| `Borrow` | 2160 | 96984 | 162 | 282 | 1176 |
| `BorrowIdle` | 8760 | 636168 | 820 | 1120 | 7784 |
| `StartQuiesce` | 8668 | 530432 | 1722 | 2282 | 14392 |
| `Quiesced` | 12692 | 848744 | 3606 | 4236 | 29512 |
| `RejectForged` | 18030 | 1400756 | 3734 | 5234 | 40376 |
| `Release` | 8472 | 348720 | 872 | 1672 | 4704 |
| `Apply` | 62530 | 2828108 | 31670 | 35420 | 118648 |
| `Detach` | 2946 | 226436 | 834 | 1004 | 10696 |

Coverage parser output is archived in [coverage-parser.log](04_evidence/coverage-parser.log), and anchor checks in [anchors-update.log](04_evidence/anchors-update.log) and [anchors-check.log](04_evidence/anchors-check.log).
