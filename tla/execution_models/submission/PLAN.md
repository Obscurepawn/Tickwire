# Order submission and local recovery

One account owner serializes reservations, journal receipts and socket admission. The submission models follow an order across these boundaries.

| Model | Input and visible behavior |
| --- | --- |
| [Commit gate](L0_commit_gate/00_input.md) | Reserve one order, synchronize its record, present a receipt and begin sending while the gate is open. |
| [Crash and receipt loss](L1_crash_receipt/00_input.md) | Lose a dependency, receipt or process at a submission step. Observe whether the intent stays in the journal, whether it can be sent and how much remains reserved. |
| [Economic facts](L2_economic_facts/00_input.md) | Reproduce the historical fill, fee and terminal reducer with its original restart ordering. |

The [ordered abort model](../mock_abort/ROADMAP.md) follows the current mock's durable abort and replay. For the physical record, see the [journal contract](../../../specs/implementation/account-journal.md).
