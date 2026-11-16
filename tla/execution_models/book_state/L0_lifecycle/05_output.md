# L0 output: bounded lifecycle verification

Verdict: **PASS** (`Finding: none`). Spot and USD-M primary and scaled configurations passed exhaustive BFS; see [run log](03_run_log.md).

Verified contracts: installing a snapshot enters `Bridging` rather than `Live` directly; accepted deltas atomically commit both sides, watermarks, and ingress timestamp; validation or sequence failures preserve prior content while transitioning to `Invalid`; wrong identities or stale deltas leave the book untouched; `generation` advances strictly monotonically via `begin_rebuild`; `Invalid` exits only via `begin_rebuild`; accepted delta watermarks never regress; and `view` succeeds only when `Live` and within `max_age_ns` at creation time. Reinstalling a snapshot with a lower sequence ID while still `Bridging` is permitted by design.

Fidelity covers all five stateful public methods (`install_snapshot`, `apply`, `begin_rebuild`, `invalidate`, `view`), initialization, identity/failure helpers, and both built-in `SequencePolicy` implementations across 19 source-cited `Next` actions (`generated > 0` in all four runs, zero unresolved branches).

Exported assumptions for `L1_coverage`: failed calls do not commit content, `Live` requires an accepted bridging delta, `begin_rebuild` clears storage, wrong identity / stale updates preserve content, and `view` enforces creation-time `Live` freshness.
