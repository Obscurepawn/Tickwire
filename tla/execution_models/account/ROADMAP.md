# ORS Account Verification Progress

| Level | Engine | Result |
| --- | --- | --- |
| [`L0_ors_slot_fill_recycle`](L0_ors_slot_fill_recycle/05_output.md) | TLC (`--coverage`) | Safety PASS across both `L0_OrsSlotFillRecycle.cfg` and `L0_OrsSlotFillRecycle_dryrun_stp.cfg` with 100% action coverage |
| [`L1_sym_ors_order_lifecycle`](L1_sym_ors_order_lifecycle/05_output.md) | Apalache (`inductive` + `bmc`) + TLC (`--coverage`) | Inductive `NoError` (`base`, `step`, `entailment`), BMC `NoError` (`length=5`), and TLC `BoundedMode` PASS with 100% action coverage |

All state variables and action guards are anchored directly to [`crates/tickwire-account`](../../../crates/tickwire-account/src/order_manager.rs) and [`crates/tickwire-venue`](../../../crates/tickwire-venue/src/client_id.rs) via `anchors.lock`.
