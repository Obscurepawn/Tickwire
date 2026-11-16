# L1 Symbolic 64-Bit / 128-Bit ORS Order Lifecycle Input Scope

## Source Mapping (`crates/tickwire-account`)

- Power-of-two `OrderSlot` identity packing (`(SlotState, seqnum)`) and modular slot aliasing `(seq & slot_mask)`:
  - `crates/tickwire-account/src/order_manager.rs:152-194`
  - `crates/tickwire-account/src/order_manager.rs:348-368`
  - `crates/tickwire-account/src/order_manager.rs:2805-2833`
- Per-slot `TtasSpinLock` and `OrderManager::lock_order` two-phase identity check:
  - `crates/tickwire-account/src/spinlock.rs:109-121`
  - `crates/tickwire-account/src/order_manager.rs:1125-1141`
- Symbolic 64-bit order quantities, ticks, `ProjectedFill`, terminal retirement, and `OrsStpBook` unlinking:
  - `crates/tickwire-account/src/dop_state.rs:193-227`
  - `crates/tickwire-account/src/order_manager.rs:1171-1461`
  - `crates/tickwire-account/src/order_manager.rs:1753-2086`
  - `crates/tickwire-account/src/order_manager.rs:2140-2192`
  - `crates/tickwire-account/src/order_manager.rs:2905-2919`
  - `crates/tickwire-account/src/ors_stp.rs:268-333`
- Symbolic `FillRecordPool::record_or_amend` (`Applied`, `Amended`, `Duplicate`) and 128-bit `UtaWalletLedger` conservation:
  - `crates/tickwire-account/src/fill_record.rs:44-59`
  - `crates/tickwire-account/src/fill_record.rs:171-277`
  - `crates/tickwire-account/src/uta_ledger.rs:322-352`
  - `crates/tickwire-account/src/uta_ledger.rs:391-540`

## Target Symbolic Invariants

1. `TypeOK`: Snowcat-typed 64-bit (`0..2^64-1`) and 128-bit (`0..2^128-1`) domain invariants constructed without raw integer literals above `2^31 - 1`.
2. `SlotIdentityCoherent`: `CLAIMED`, `LIVE`, and `RETIRED` states preserve `slot_id_seq` and `body_seq` invariants across modular aliasing `seq' = seq + cfg_slots_cap`.
3. `LockOrderVerifiedOwner`: `resp_locked_seq > 0` implies `slot_state = SLOT_LIVE /\ slot_id_seq = resp_locked_seq /\ body_seq = resp_locked_seq`.
4. `ImmediateRetireUnlinksStp`: Non-`LIVE` slots never remain linked in `OrsStpBook`.
5. `ProjectionAndAckSound`: `front_ack_cnt <= 1`, `ack_cnt <= 1`, and `filled_qty <= proj_qty <= ord_qty` over arbitrary `u64` lot sizes.
6. `TradeDedupAndAmendIdempotent`: Symbolic trade `t1` contributes its lot quantity once and settles arbitrary signed fee/rebate amendments into `net_settled_fees`.
7. `SolvencyPreserved`: Exact 128-bit `wallet_quote_free + wallet_quote_locked = cfg_init_quote_free - cumulative_spent_notional - net_settled_fees` conservation with non-negative `free` and `locked` balances.
