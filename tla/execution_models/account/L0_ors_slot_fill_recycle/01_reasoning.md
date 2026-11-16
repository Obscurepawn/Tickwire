# L0 ORS OrderSlot, FillRecordPool & Recycling Reasoning

## Modeling Choices & State-Space Discipline

- **Single Contested Modular Slot Across Multiple Sequence Epochs**: Setting `SlotsCap = 1` with `MaxSeq = 2` (`seq = 1` and `seq = 2` colliding on `(seq & slot_mask) == 0`) concentrates the finite state graph directly on the critical race window:
  1. `REQ` attempting to claim `(seq = 2)` while `(seq = 1)` is still `CLAIMED` or `LIVE` (`ReqRefuseModularAlias` returning `TOO_MANY_LIVE_ORDERS`).
  2. `RESP` passing the pre-lock `Acquire` check on `seq = 1` just before `seq = 1` retires and `REQ` recycles the physical slot for `seq = 2`, forcing the post-lock identity reload inside `OrderManager::lock_order` (`RespLockPostcheckAbort`) to drop the spinlock without mutating `seq = 2`.
- **Decoupled `FillRecordPool` Fee Amendments**: Each `trade_id` tracks `pool_recorded`, `pool_fee_known`, `pool_fee`, and a bounded single post-trade `amendment = true` transition (`pool_amend_count \in 0..1`). This verifies that late fee/rebate amendments (`execution.fast` -> `execution`) settle accurately into `UtaWalletLedger` even after the originating `OrderSlot` has already retired and been recycled for `seq = 2`.
- **Two Complementary Configurations**:
  - [`L0_OrsSlotFillRecycle.cfg`](02_models/L0_OrsSlotFillRecycle.cfg): Exercises multi-epoch live order lifecycles (`AllowDryRun = FALSE`, `RestOppositeAsk = FALSE`), partial/full fills, `ProjectedFill` catch-up, duplicate trade rejection, post-retirement fee amendment, and modular slot reuse (`97,889` distinct states).
  - [`L0_OrsSlotFillRecycle_dryrun_stp.cfg`](02_models/L0_OrsSlotFillRecycle_dryrun_stp.cfg): Enables `AllowDryRun = TRUE` and `RestOppositeAsk = TRUE` to verify `OrsStpBook` send-path crossing refusal (`ReqRefuseStpCross`) and `dry_run` zero-hold immediate `Canceled(DRY_RUN)` retirement (`ReqCompleteDryRunRetire`) (`220` distinct states), achieving 100% combined `Next` action coverage.
