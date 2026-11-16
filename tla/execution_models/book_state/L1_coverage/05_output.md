# L1 output: bounded coverage-knowledge verification

Verdict: **PASS** (`Finding: none`). All four Bid/Ask primary and scaled configurations passed exhaustive BFS; see [run log](03_run_log.md).

Under accurate snapshot `complete` flags and complete delta updates, every `Known` quantity equals the full `world` quantity at the accepted watermark, every `Known` best level equals the true best level of `world`, and every unstored out-of-window `Unknown` quantity admits compatible `world` states with both `0` and `1` lots. `ObserveHiddenBest` fired in every configuration, confirming states where a hidden out-of-window level beats all stored levels while `best` safely reports `Unknown`. Coverage boundaries never silently expand during delta application, and deleting an out-of-window positive level reverts `quantity` to `Unknown` rather than fabricating `Known(0)`.

All 15 `Next` actions (`SideCoverage::contains`, `coverage`, `BookView::quantity`, and `BookView::best` branches plus the five projected lifecycle transitions) fired with `generated > 0` across all four configurations.
