# `L1_coverage` TLC run log

Checked with TLC 2.19 (official v1.7.4 release, `tla/tools/tla2tools.jar` SHA-256 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`), single worker (`-workers 1 -coverage 1`), exhaustive BFS to `0` queued states. Re-run via `./scripts/check-tla.sh book_state`.

| Configuration | Generated | Distinct | Depth | Seconds | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| [L1_BookCoverage](runs/L1_BookCoverage.log) | 45,523 | 1,471 | 6 | 3 | PASS |
| [L1_Ask](runs/L1_Ask.log) | 45,523 | 1,471 | 6 | 1 | PASS |
| [L1_AskScaling](runs/L1_AskScaling.log) | 585,796 | 8,244 | 6 | 4 | PASS |
| [L1_BidScaling](runs/L1_BidScaling.log) | 519,202 | 7,050 | 6 | 6 | PASS |

All 15 `Next` actions achieved `generated > 0` in every configuration (`Init` recorded separately as `1`):

| Action | L1_BookCoverage | L1_Ask | L1_AskScaling | L1_BidScaling |
| --- | ---: | ---: | ---: | ---: |
| `SnapshotAccepted` | 13865 | 13865 | 223344 | 129822 |
| `SnapshotRejected` | 235 | 235 | 1269 | 843 |
| `ApplyAccepted` | 22194 | 22194 | 250344 | 333630 |
| `CapacityRejected` | 2106 | 2106 | 67800 | 14589 |
| `Invalidate` | 1471 | 1471 | 8244 | 7050 |
| `IgnoreStale` | 900 | 900 | 4971 | 4299 |
| `RejectIdentity` | 1471 | 1471 | 8244 | 7050 |
| `Rebuild` | 725 | 725 | 8166 | 7005 |
| `ObserveKnownBest` | 288 | 288 | 1662 | 1629 |
| `ObserveHiddenBest` | 190 | 190 | 1176 | 975 |
| `ObserveUnknownZero` | 446 | 446 | 1797 | 3180 |
| `ObserveKnownZero` | 462 | 462 | 1698 | 2649 |
| `ObserveKnownBeyondCoverage` | 380 | 380 | 2688 | 2970 |
| `ObserveCompleteEmpty` | 10 | 10 | 15 | 15 |
| `ObserveNotLive` | 779 | 779 | 4377 | 3495 |

Structured counts and SHA-256 digests are recorded in [results.json](runs/results.json).
