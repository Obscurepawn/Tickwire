# L0 ORS OrderSlot, FillRecordPool & Recycling Input Scope

## Source Mapping (`crates/tickwire-account` & `crates/tickwire-venue`)

- Power-of-two `OrderSlot` 4-state atomic identity (`EMPTY -> CLAIMED -> LIVE -> RETIRED`) and `(seqnum & slot_mask)` modular indexing:
  - `crates/tickwire-account/src/order_manager.rs:146-194`
  - `crates/tickwire-account/src/order_manager.rs:348-368`
  - `crates/tickwire-account/src/order_manager.rs:2805-2833`
- Fine-grained per-slot `TtasSpinLock` and `OrderManager::lock_order` pre-lock `Acquire` check + post-lock identity reload preventing wrong-occupant locks across retirement and modular reuse:
  - `crates/tickwire-account/src/spinlock.rs:90-128`
  - `crates/tickwire-account/src/order_manager.rs:1125-1153`
- Outbound `REQ` admission (`place_order`), `OrsStpBook` send-path atomic best-price check, `dry_run` immediate `Canceled(DRY_RUN)` retirement, and inbound `RESP` transitions (`on_front_ack`, `on_ack`, `on_cum_qty`, `on_fill`, `on_canceled`, `on_rejected`) with immediate terminal `RETIRED` transition and `OrsStpBook` unlinking without waiting on post-trade fee settlement:
  - `crates/tickwire-account/src/order_manager.rs:1171-1461`
  - `crates/tickwire-account/src/order_manager.rs:1635-2086`
  - `crates/tickwire-account/src/order_manager.rs:2140-2192`
  - `crates/tickwire-account/src/order_manager.rs:2905-2919`
  - `crates/tickwire-account/src/ors_stp.rs:250-333`
  - `crates/tickwire-account/src/stp_book.rs:1-120`
  - `crates/tickwire-account/src/projected_fill.rs:1-120`
- `FillRecordPool::record_or_amend` (`Applied`, `Amended`, `Duplicate`), `DopState` sequence floor, `SelfDescribingClientId`, and `UtaWalletLedger` hold/settlement accounting:
  - `crates/tickwire-account/src/fill_record.rs:44-59`
  - `crates/tickwire-account/src/fill_record.rs:171-277`
  - `crates/tickwire-account/src/dop_state.rs:193-267`
  - `crates/tickwire-account/src/uta_ledger.rs:206-275`
  - `crates/tickwire-account/src/uta_ledger.rs:322-352`
  - `crates/tickwire-account/src/uta_ledger.rs:391-540`
  - `crates/tickwire-venue/src/client_id.rs:45-190`

## Target Safety Invariants

1. `TypeOK`: All slot states, spinlock owners, order quantities, `FillRecordPool` rows, and `UtaWalletLedger` balances stay within structural domains.
2. `SlotLifecycleSound`: `CLAIMED`, `LIVE`, and `RETIRED` transitions preserve packed `(SlotState, seqnum)` coherence and modular slot aliasing safety.
3. `VerifiedLockOwnerMatch`: Any `RESP` guard returned by `lock_order(seq)` holds the spinlock on an active `LIVE` occupant whose `body_seq == seq`.
4. `AckProjectionMonotonic`: `FrontAck` and `Ack` fire at most once per order epoch and `projected_fill_qty_lots >= filled_qty_lots` remains monotonic.
5. `ImmediateRetireAndStpUnlink`: Terminal `Filled`, `Canceled`, `Rejected`, and `dry_run` transitions immediately mark the slot `RETIRED` and unlink `OrsStpBook` without waiting on post-trade fee settlement.
6. `TradeDedupAndAmendExact`: `FillRecordPool` applies each `trade_id` quantity at most once while allowing post-trade `amendment = true` fee/rebate deltas even after slot retirement.
7. `DryRunZeroHoldAndImmediateRetire`: `dry_run` orders reserve zero wallet hold and never rest in `OrsStpBook`.
8. `SolvencyPreserved`: `UtaWalletLedger` free and locked balances match open-order holds, executed fills, and net fees/rebates across slot recycling.
