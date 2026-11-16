# L0 Per-Account ORS QueryDesk Reasoning

## Modeling Choices

- **Two-Order Contention Over a Capacity-Bounded Desk (`Orders = {1, 2}`, `DeskCap = 1`)**: Pairing two concurrent orders with a 1-slot `OrsQueryDesk` forces every critical multi-order interleaving into the reachable state graph:
  1. **Per-Order Isolation**: Order `2` submits, receives hot WS `Ack`, and fills to completion while Order `1` remains wedged in `OrsQueryDesk` with its hold locked.
  2. **Hot-Path vs Cold-Desk Decoupling**: When hot WS `ExecutionReport` fills and retires Order `1` (`close_and_retire_locked` marks `SlotState::Retired` without locking `OrsQueryDesk`), Order `1`'s stale desk entry is either lazily pruned by `PollDuePruneNonLive` or evicted by `ScheduleOnFullEvictNonLive` when Order `2` encounters an ambiguous error.
  3. **Live-Entry Protection**: When Order `1` is still `SlotState::Live` in the full desk and Order `2` encounters an ambiguous error, `ScheduleRefuseFullAllLive` returns `TooManyPendingActions` without evicting Order `1`.
- **Two Complementary Configurations**:
  - [`L0_OrsQueryDesk.cfg`](02_models/L0_OrsQueryDesk.cfg): Verifies default unbounded retry (`MaxQueryCount = 0`), proving `InternalCancel` is unreachable (`672,496` distinct states).
  - [`L0_OrsQueryDesk_bounded.cfg`](02_models/L0_OrsQueryDesk_bounded.cfg): Enables opt-in `MaxQueryCount = 2`, exercising `ReconcileInconclusiveCapRetire` (`479,654` distinct states) and achieving 100% combined coverage across all 21 transitions.
