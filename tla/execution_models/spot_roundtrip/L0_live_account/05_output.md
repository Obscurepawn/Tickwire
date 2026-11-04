# Spot route verdict

**PASS** for the four submitted configurations. Every accepted first byte followed a reserved, durable admission and a successful local authorization sample. The recorded sample referred to the applied private epoch and captured frontier and to a complete account-side market observation. Each command generated at most one first byte. The normalized cash budget equalled available funds plus outstanding reservations plus booked debits, including after abort or successful restart.

With one producer and the [fault-free progress assumptions](01_reasoning.md), TLC checked that the order eventually fills, is acknowledged and booked, and the account regains permission. With two producers and enough cash, every command eventually reaches an acknowledged, booked fill or a proven zero-byte abort, and the account reconciles. A stronger expectation that both concurrent orders always fill is [safe-by-design](04_counterexamples.md).

## Fidelity and coverage

The declared boundary has 20 operational actions and an idle action. All have successor coverage across four configurations; the checker reported 22 entries including initialization. The denominator is the 20 route actions in the model, not executable Rust lines. The implementation anchors and the projections for exchange matching, ownership and recovery are listed in [the model](02_models/L0_SpotRoundtrip.tla) and [reasoning](01_reasoning.md). [Run evidence](03_run_log.md)

The ACK ends at the account-side callback. The file strategy writes it to stderr. The route model constructs the recipient from its one trusted request slot; the [intake model](../../strategy_intake/L0_shared_cash/05_output.md) separately checks member selection. Restart assumes a replayable journal and successful market re-JOIN. Only the command deadline expires in this projection. The [fee-closure counterexample](../../cash_evidence/L0_terminal_fees/06_review.md) identifies an actual input for which the replay precondition fails.
