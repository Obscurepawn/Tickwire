# ORS Account Verification Roadmap

| Level | Status | Verified Scope |
| --- | --- | --- |
| [`L0_ors_slot_fill_recycle`](L0_ors_slot_fill_recycle/05_output.md) | PASS | 2 TLC configurations (`normal` + `dry_run` / `StpMode`), 100% `Next` action coverage, 9 safety invariants covering 4-state `OrderSlot` recycling, `OrsStpBook` unlinking, `FillRecordPool` deduplication/amendment, and `UtaWalletLedger` solvency |
| [`L1_sym_ors_order_lifecycle`](L1_sym_ors_order_lifecycle/05_output.md) | PASS | Dual-engine Apalache Z3 `QF_LIA` (`inductive` + `bmc`) + 2 TLC `BoundedMode` configurations with 100% `Next` action coverage over symbolic 64-bit `seqnum` modular aliasing and 128-bit wallet accounting |
