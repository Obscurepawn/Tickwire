# L0 Per-Account ORS QueryDesk Verdict

- **Status**: **PASS** (TLC exhaustive finite-state verification with 100% `Next` action coverage)
- **Model**: [`02_models/L0_OrsQueryDesk.tla`](02_models/L0_OrsQueryDesk.tla)
- **Reasoning**: [`01_reasoning.md`](01_reasoning.md)
- **Run Log**: [`03_run_log.md`](03_run_log.md)

## Verified Properties

- Sibling orders submit, ack, fill, and retire without head-of-line blocking while an ambiguous order is reconciled in `OrsQueryDesk`, and full-table cleanup never evicts an active `SlotState::Live` order.
- `Place` + `Cancel` `cancel_after` coalescing, `Cancel` answered `Open` `pending_cxl` decrements, catch-up fills applied strictly before remainder cancel, `-2013` / `NotFound` disambiguation, and `max_query_count` (`0` unbounded vs opt-in `INTERNAL_CANCEL`) preserve multi-order wallet solvency.
