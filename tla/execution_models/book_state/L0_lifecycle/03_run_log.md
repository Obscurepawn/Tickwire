# `L0_lifecycle` TLC run log

Checked with TLC 2.19 (official v1.7.4 release, `tla/tools/tla2tools.jar` SHA-256 `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`), single worker (`-workers 1 -coverage 1`), exhaustive BFS to `0` queued states. Re-run via `./scripts/check-tla.sh book_state`.

| Configuration | Generated | Distinct | Depth | Seconds | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| [L0_BookState](runs/L0_BookState.log) | 111,457 | 1,392 | 6 | 2 | PASS |
| [L0_SpotScaling](runs/L0_SpotScaling.log) | 3,042,646 | 8,499 | 6 | 12 | PASS |
| [L0_UsdM](runs/L0_UsdM.log) | 115,809 | 1,880 | 6 | 2 | PASS |
| [L0_UsdMScaling](runs/L0_UsdMScaling.log) | 2,220,496 | 10,524 | 6 | 14 | PASS |

All 19 `Next` actions achieved `generated > 0` in every configuration (`Init` recorded separately as `1`):

| Action | L0_BookState | L0_SpotScaling | L0_UsdM | L0_UsdMScaling |
| --- | ---: | ---: | ---: | ---: |
| `SnapshotAccepted` | 2184 | 27972 | 2088 | 27000 |
| `SnapshotRejected` | 182 | 777 | 174 | 750 |
| `SnapshotWrongIdentity` | 1392 | 8499 | 1880 | 10524 |
| `SnapshotBadState` | 1210 | 7722 | 1706 | 9774 |
| `ApplyAccepted` | 60288 | 2327940 | 53696 | 1519236 |
| `ApplyStale` | 10592 | 121500 | 10176 | 112050 |
| `ApplyBadSequence` | 7808 | 129060 | 17176 | 239382 |
| `ApplyBadClock` | 2688 | 78300 | 1792 | 30780 |
| `ApplyBadPayload` | 15072 | 258660 | 13424 | 168804 |
| `ApplyWrongIdentity` | 1392 | 8499 | 1880 | 10524 |
| `ApplyBadState` | 760 | 4503 | 1024 | 5691 |
| `RebuildAccepted` | 696 | 8499 | 940 | 10524 |
| `RebuildRejected` | 2088 | 16998 | 2820 | 21048 |
| `InvalidateAccepted` | 1392 | 8499 | 1880 | 10524 |
| `InvalidateWrongGeneration` | 1392 | 16998 | 1880 | 21048 |
| `ViewAccepted` | 784 | 5751 | 1176 | 4104 |
| `ViewNotLive` | 928 | 5259 | 1184 | 6420 |
| `ViewClockRegressed` | 448 | 4698 | 672 | 6156 |
| `ViewStale` | 160 | 2511 | 240 | 6156 |

Structured counts and SHA-256 digests are recorded in [results.json](runs/results.json).
