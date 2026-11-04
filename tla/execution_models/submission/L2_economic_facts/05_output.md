# Economic reducer checks

The v2 model checks the ordering of fills, fees, cancellation and terminal status under bounded journal histories, shared event slots and checked arithmetic. It preserves reservations for outstanding fills and freezes a failed recovery before the old engine can admit another order.

The exact replay ordering is tied to the [historical source](06_notes.md). The [four finite runs](03_run_log.md) and [L2-001 trace](04_counterexamples.md) reproduce the failure that motivated the change.
