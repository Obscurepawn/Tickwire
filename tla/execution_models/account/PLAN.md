# ORS Account & Order-Slot Lifecycle Verification Plan

Source scope: [`crates/tickwire-account`](../../../crates/tickwire-account/src/lib.rs). In the unified ORS execution process (`1 ORS process = 1 Venue`), [`OrderManager`](../../../crates/tickwire-account/src/order_manager.rs) coordinates fine-grained per-slot `TtasSpinLock` transitions between the outbound `REQ` thread and inbound `RESP` thread without coarse process-wide locks or local disk journals.

| Level | Focus |
| --- | --- |
| [`L0_ors_slot_fill_recycle`](L0_ors_slot_fill_recycle/05_output.md) | Exhaustive finite-state TLC verification of power-of-two `OrderSlot` 4-state transitions (`EMPTY -> CLAIMED -> LIVE -> RETIRED`), `lock_order` pre-lock/post-lock identity rechecks across modular `(seq & slot_mask)` recycling, `OrsStpBook` send-path atomic best-price admission and immediate terminal unlinking without waiting on post-trade fee settlement, `dry_run` immediate `Canceled(DRY_RUN)` retirement, `FrontAck` / `Ack` / `ProjectedFill` / `Fill` ordering, `FillRecordPool` trade deduplication + `amendment = true` fee patching, and `UtaWalletLedger` multi-wallet solvency |
| [`L1_sym_ors_order_lifecycle`](L1_sym_ors_order_lifecycle/05_output.md) | Dual-engine symbolic 64-bit / 128-bit verification (Apalache Z3 `QF_LIA` inductive invariant + BMC alongside TLC `BoundedMode` action coverage) proving modular `seqnum` slot collision safety (`seq' = seq + cfg_slots_cap`), `FillRecordPool::record_or_amend` idempotency, and `UtaWalletLedger` solvency across arbitrary `u64` quantities/ticks and `i128` asset balances |
