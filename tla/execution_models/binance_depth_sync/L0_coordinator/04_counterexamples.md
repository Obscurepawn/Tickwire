# Counterexamples and review log

All four final safety configurations passed without counterexamples; see the [run log](03_run_log.md). Zero Rust implementation defects were found by TLC at this level.

Independent source-to-model review identified two modeling fidelity gaps that were resolved prior to closing L0:

| Issue | Impact | Fix and verification |
| --- | --- | --- |
| Initial time domain `0..1` | Valid request deadline had to be `1` and valid replay occurred at `0`, preventing distinct arrival vs. snapshot completion timestamps | Expanded all four configs to `0..2`, split `SnapshotDelayed`, and verified `SnapshotRetainsArrivalStamp` across both branches |
| Replay-failure `Reset` used pre-install `Book` on counter exhaustion | Did not affect `view` closure, but diverged from diagnostic `last_update_id()` on generation exhaustion | Applied installed/partially replayed `Replay` book state before `Reset` so exhaustion invalidates while normal rotation clears |

Initially `SnapshotDelayed` was a pure alias merged by TLC into `SnapshotResponse` coverage; adding an explicit `outcome` guard separated action coverage cleanly. Earlier runs are archived under [pre-distinct-coverage](runs/pre-distinct-coverage/).

Five early TLA+ authoring logs (`authoring-first.log` through `authoring-none-access.log`) record parser/type fixes during initial spec writing (`=<<` tokenization, `IF` indentation, dependent quantifier domains, and `None` sentinel typing) rather than production bugs.

An initial scaled run before moving the unused existential quantifier out of the full-buffer overflow branch was stopped via `SIGTERM` (exit code `143`) and archived in [pre-overflow-enumeration](runs/pre-overflow-enumeration/); all reported PASS verdicts come from the completed runs after that equivalence-preserving quantifier pushdown.

A dedicated [reachability probe](runs/late-replay-witness/L0_LateReplayProbe.tla) (`WitnessNotReached`, expected exit code `12`, `12,801` generated / `146` distinct states, depth `7`, [trace log](runs/late-replay-witness/reachability.log)) confirms that partial FIFO application followed by mid-replay failure and atomic `Reset` is reachable:

| State | Action | Key transition |
| --- | --- | --- |
| 1 | `Init` | `Disconnected`, `generation = 0` |
| 2 | `Connect` | `connection = 1`, `generation = 1`, `Buffering` |
| 3 | `Subscribe` | Active connection marked subscribed |
| 4 | `DeltaBuffer` | Buffers valid delta `U = 0, u = 1, qty = 1, stamp = 0` |
| 5 | `Request` | Issues `token = (1, 1, 1)` with `deadline = 1` |
| 6 | `DeltaBuffer` | Buffers invalid-payload delta `U = u = 2, qty = 1, stamp = 0` |
| 7 | `SnapshotValid` | First buffered delta applies, second fails; yields `LateReplayFailure`, `generation = 2`, `Buffering`, cleared buffer and book |

Step 7 executes atomically inside a single `on_snapshot` call, confirming that callers never observe an intermediate `Live` book.
