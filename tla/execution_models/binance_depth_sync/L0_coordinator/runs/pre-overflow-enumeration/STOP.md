# Runs prior to quantifier pushdown

This directory archives the model and configurations prior to pushing the event quantifier into the non-full branch of `DeltaBuffer`. Both primary Spot and USD-M runs completed with `PASS`, while the scaled Spot run and initial unconstrained reachability probe were manually stopped via `SIGTERM` (exit code `143`) because the full-buffer overflow branch redundantly enumerated every event in `Events` despite producing identical `Reset` successors. Aborted runs were never counted as `PASS`, and no parameter bounds were shrunk.

Completed runs for all four configurations using the equivalence-preserving quantifier placement live in the parent `runs/` directory; the scaled USD-M configuration file here preserves the intended input bounds from that snapshot.
