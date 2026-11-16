# Independent review and follow-up boundaries

Line-by-line review of the 485-line `DepthSync` implementation confirmed fixed product policy binding, early stale-token rejection before clock or JSON work, subscription preservation across same-connection rebuilds, view closure on generation/request counter saturation, atomic snapshot replay without intermediate `Live` exposure, original delta ingress timestamp retention, and finite-depth `Unknown` preservation.

Cross-review against `depth_sync`, `Book`, and feed callers identified the [two modeling fidelity adjustments](04_counterexamples.md) and verified the revised transitions: stale-token guards precede clock/payload checks, `FIFO` replay is atomic, HTTP/deadline failures preserve buffered deltas, same-connection rebuilds retain subscription state, counter saturation closes `view`, and Spot vs. USD-M stale/`pu` ordering matches venue rules.

Downstream transport owners must obey three caller contracts: request a snapshot only after the active connection confirms subscription and buffers its first delta, attach the original `SnapshotToken` to the HTTP response callback, and read `view` only after `on_snapshot` succeeds. Out-of-window `Unknown` depth and level-array operations remain governed by `tickwire-book` and its regression suite.

The `DeltaBuffer` quantifier pushdown optimization was also verified equivalence-preserving because full-queue `Reset` is independent of `e` and `Events(t, s.gen)` is non-empty.
