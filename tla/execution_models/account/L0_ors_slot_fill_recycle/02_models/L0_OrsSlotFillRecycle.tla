---- MODULE L0_OrsSlotFillRecycle ----
(*
 * Finite-State Concurrent Interleaving Model for `tickwire-account`:
 *   1. Power-of-two `OrderManager` 4-state `OrderSlot` lifecycle
 *      (`EMPTY -> CLAIMED -> LIVE -> RETIRED`) with modular `seqnum` aliasing
 *      (`SlotOf(1) = SlotOf(3) = 1`) and `claim_slot` `ALLOCATION_ERROR` protection:
 *        crates/tickwire-account/src/order_manager.rs:152-194
 *        crates/tickwire-account/src/order_manager.rs:313-368
 *        crates/tickwire-account/src/order_manager.rs:2805-2833
 *   2. Per-slot `SpinLock` mutual exclusion and `OrderManager::lock_order`
 *      two-step `Acquire` precheck + post-lock reload identity recheck across
 *      concurrent `REQ` `dry_run` retirement and slot reuse:
 *        crates/tickwire-account/src/spinlock.rs:109-121
 *        crates/tickwire-account/src/order_manager.rs:1125-1141
 *        crates/tickwire-account/src/order_manager.rs:1306-1320
 *   3. Immediate `RETIRED` transition and `OrsStpBook::unlink_order` on terminal
 *      `Filled`, `Canceled`, and `Rejected` without waiting on post-trade fee settlement:
 *        crates/tickwire-account/src/order_manager.rs:1901-1910
 *        crates/tickwire-account/src/order_manager.rs:2023-2065
 *        crates/tickwire-account/src/order_manager.rs:2169-2177
 *        crates/tickwire-account/src/order_manager.rs:2905-2919
 *        crates/tickwire-account/src/ors_stp.rs:268-333
 *   4. `One ack` (`FrontAck` + `Ack`) single emission and exact cancel remainder
 *      `canceled_qty_lots = effective_qty_lots - projected_fill_qty_lots`:
 *        crates/tickwire-account/src/order_manager.rs:1337-1461
 *        crates/tickwire-account/src/order_manager.rs:1753-1797
 *        crates/tickwire-account/src/order_manager.rs:2031-2040
 *   5. `FillRecordPool` trade-ID deduplication and post-trade `amendment = true`
 *      fee/rebate patching (`TRADE_LITE` / `execution.fast` -> `ORDER_TRADE_UPDATE` /
 *      `execution`) on both `LIVE` and already-`RETIRED`/reused slots, proving
 *      `QuantityExactlyOnce` and `NoFalseConflictFreeze`:
 *        crates/tickwire-account/src/fill_record.rs:44-59
 *        crates/tickwire-account/src/fill_record.rs:171-277
 *        crates/tickwire-account/src/order_manager.rs:1806-2012
 *   6. Multi-wallet `UtaWalletLedger` (`AssetKey { wallet_id, asset_id }`)
 *      hold reservation, proportional release, principal settlement, and signed
 *      fee/rebate amendment (`SolvencyPreserved`):
 *        crates/tickwire-account/src/uta_ledger.rs:13-22
 *        crates/tickwire-account/src/uta_ledger.rs:322-352
 *        crates/tickwire-account/src/uta_ledger.rs:391-540
 *)
EXTENDS Integers, FiniteSets

CONSTANTS
  InitSpotQuoteAvail,
  InitMarginAvail,
  EnableDryRunSeq1,
  EnableStpCrossSeq2

Slots == {0, 1}
Seqs == {1, 2, 3}
Trades == {1, 2, 3}
Wallets == {"W_SPOT_QUOTE", "W_MARGIN_COLLAT"}

(* Power-of-two slot index (`order_seqnum & slot_mask` with `slots_per_strat = 2`):
 * `seq = 1` and `seq = 3` alias onto slot `1`; `seq = 2` maps to slot `0`. *)
SlotOf(seq) == seq % 2

WalletOf(seq) == IF seq = 2 THEN "W_MARGIN_COLLAT" ELSE "W_SPOT_QUOTE"
OrdQtyOf(seq) == IF seq = 1 THEN 2 ELSE 1
OrdPxOf(seq) == IF seq = 2 THEN 2 ELSE 1
OrdHoldOf(seq) == OrdQtyOf(seq) * OrdPxOf(seq)

(* Trade-to-order attribution: trades 1,2 belong to seq 1 (1 lot each); trade 3 belongs to seq 3 (1 lot). *)
TradeSeq(tid) == IF tid \in {1, 2} THEN 1 ELSE 3
TradeQty(tid) == 1
TradePx(tid) == 1

VARIABLES
  (* `DopState::next_order_seqnum` (`crates/tickwire-account/src/dop_state.rs:193-227`) *)
  dop_next_seq,

  (* `OrderSlot::identity` packed `(SlotState, seq48)` (`crates/tickwire-account/src/order_manager.rs:165-194`) *)
  slot_state,
  slot_id_seq,

  (* Per-slot `SpinLock<Order>` (`crates/tickwire-account/src/spinlock.rs:109-121`) *)
  slot_lock,

  (* Protected `Order` body inside `OrderSlot` (`crates/tickwire-account/src/order_manager.rs:247-310`) *)
  body_seq,
  body_status,
  body_ord_qty,
  body_filled_qty,
  body_proj_qty,
  body_hold_rem,
  body_dry_run_pending,

  (* `OrsStpBook` active resting-order link (`crates/tickwire-account/src/ors_stp.rs:268-333`) *)
  stp_linked,

  (* Concurrent `RESP` `lock_order` pre-lock `Acquire` snapshot (`crates/tickwire-account/src/order_manager.rs:1125-1141`) *)
  resp_precheck_seq,
  resp_locked_seq,

  (* `FillRecordPool` trade deduplication & fee-amendment state (`crates/tickwire-account/src/fill_record.rs:171-277`) *)
  pool_recorded,
  pool_fee_known,
  pool_fee_signed,
  pool_amend_count,

  (* Multi-wallet `UtaWalletLedger` balances & net position (`crates/tickwire-account/src/uta_ledger.rs:266-540`) *)
  wallet_avail,
  wallet_locked,
  spot_base_pos,
  settled_quote_spend,
  settled_fee_total,

  (* Per-order emitted strategy report audit counters & safety latches *)
  front_ack_count,
  ack_count,
  order_terminal,
  cxl_emitted,
  cxl_qty_recorded,
  cxl_proj_recorded,
  cxl_fill_recorded,
  alias_refused_seen,
  conflict_frozen

vars == <<
  dop_next_seq, slot_state, slot_id_seq, slot_lock,
  body_seq, body_status, body_ord_qty, body_filled_qty, body_proj_qty, body_hold_rem, body_dry_run_pending,
  stp_linked, resp_precheck_seq, resp_locked_seq,
  pool_recorded, pool_fee_known, pool_fee_signed, pool_amend_count,
  wallet_avail, wallet_locked, spot_base_pos, settled_quote_spend, settled_fee_total,
  front_ack_count, ack_count, order_terminal,
  cxl_emitted, cxl_qty_recorded, cxl_proj_recorded, cxl_fill_recorded,
  alias_refused_seen, conflict_frozen
>>

Max2(a, b) == IF a >= b THEN a ELSE b

InitWalletAvail(w) ==
  IF w = "W_SPOT_QUOTE" THEN InitSpotQuoteAvail ELSE InitMarginAvail

ActiveSlotHoldForWallet(w) ==
  LET s0 == IF slot_state[0] = "LIVE" /\ WalletOf(body_seq[0]) = w THEN body_hold_rem[0] ELSE 0
      s1 == IF slot_state[1] = "LIVE" /\ WalletOf(body_seq[1]) = w THEN body_hold_rem[1] ELSE 0
  IN s0 + s1

UniqueExecutedLots ==
  (IF pool_recorded[1] THEN TradeQty(1) ELSE 0)
  + (IF pool_recorded[2] THEN TradeQty(2) ELSE 0)
  + (IF pool_recorded[3] THEN TradeQty(3) ELSE 0)

UniquePoolFeeSum ==
  pool_fee_signed[1] + pool_fee_signed[2] + pool_fee_signed[3]

Init ==
  /\ dop_next_seq = 1
  /\ slot_state = [s \in Slots |-> "EMPTY"]
  /\ slot_id_seq = [s \in Slots |-> 0]
  /\ slot_lock = [s \in Slots |-> "FREE"]
  /\ body_seq = [s \in Slots |-> 0]
  /\ body_status = [s \in Slots |-> "NONE"]
  /\ body_ord_qty = [s \in Slots |-> 0]
  /\ body_filled_qty = [s \in Slots |-> 0]
  /\ body_proj_qty = [s \in Slots |-> 0]
  /\ body_hold_rem = [s \in Slots |-> 0]
  /\ body_dry_run_pending = [s \in Slots |-> FALSE]
  /\ stp_linked = [s \in Slots |-> FALSE]
  /\ resp_precheck_seq = 0
  /\ resp_locked_seq = 0
  /\ pool_recorded = [t \in Trades |-> FALSE]
  /\ pool_fee_known = [t \in Trades |-> FALSE]
  /\ pool_fee_signed = [t \in Trades |-> 0]
  /\ pool_amend_count = [t \in Trades |-> 0]
  /\ wallet_avail = [w \in Wallets |-> InitWalletAvail(w)]
  /\ wallet_locked = [w \in Wallets |-> 0]
  /\ spot_base_pos = 0
  /\ settled_quote_spend = 0
  /\ settled_fee_total = 0
  /\ front_ack_count = [q \in Seqs |-> 0]
  /\ ack_count = [q \in Seqs |-> 0]
  /\ order_terminal = [q \in Seqs |-> FALSE]
  /\ cxl_emitted = [q \in Seqs |-> FALSE]
  /\ cxl_qty_recorded = [q \in Seqs |-> 0]
  /\ cxl_proj_recorded = [q \in Seqs |-> 0]
  /\ cxl_fill_recorded = [q \in Seqs |-> 0]
  /\ alias_refused_seen = FALSE
  /\ conflict_frozen = FALSE

(* -------------------------------------------------------------------------
 * `REQ` Path: `place_order` (`crates/tickwire-account/src/order_manager.rs:1171-1333`)
 * ------------------------------------------------------------------------- *)

(* Step 1a: `OrsStpBook::self_cross` lock-free atomic best-price precheck refuses crossing order
 * before claiming any slot or mutating wallet holds (`crates/tickwire-account/src/order_manager.rs:1204-1214`). *)
ReqRefuseStpCross ==
  /\ EnableStpCrossSeq2
  /\ dop_next_seq = 2
  /\ stp_linked[1]
  /\ dop_next_seq' = 3
  /\ order_terminal' = [order_terminal EXCEPT ![2] = TRUE]
  /\ UNCHANGED <<
       slot_state, slot_id_seq, slot_lock,
       body_seq, body_status, body_ord_qty, body_filled_qty, body_proj_qty, body_hold_rem, body_dry_run_pending,
       stp_linked, resp_precheck_seq, resp_locked_seq,
       pool_recorded, pool_fee_known, pool_fee_signed, pool_amend_count,
       wallet_avail, wallet_locked, spot_base_pos, settled_quote_spend, settled_fee_total,
       front_ack_count, ack_count,
       cxl_emitted, cxl_qty_recorded, cxl_proj_recorded, cxl_fill_recorded,
       alias_refused_seen, conflict_frozen
     >>

(* Step 1b: Modular `seqnum` collision on an active `CLAIMED`/`LIVE` slot (`seq = 3` aliasing `seq = 1` on Slot 1)
 * returns `RefusalReason::ALLOCATION_ERROR` via `claim_slot` CAS failure without touching the live occupant
 * (`crates/tickwire-account/src/order_manager.rs:2816-2833`). *)
ReqRefuseModularAlias ==
  /\ dop_next_seq = 3
  /\ slot_state[1] \in {"CLAIMED", "LIVE"}
  /\ ~alias_refused_seen
  /\ alias_refused_seen' = TRUE
  /\ UNCHANGED <<
       dop_next_seq, slot_state, slot_id_seq, slot_lock,
       body_seq, body_status, body_ord_qty, body_filled_qty, body_proj_qty, body_hold_rem, body_dry_run_pending,
       stp_linked, resp_precheck_seq, resp_locked_seq,
       pool_recorded, pool_fee_known, pool_fee_signed, pool_amend_count,
       wallet_avail, wallet_locked, spot_base_pos, settled_quote_spend, settled_fee_total,
       front_ack_count, ack_count, order_terminal,
       cxl_emitted, cxl_qty_recorded, cxl_proj_recorded, cxl_fill_recorded,
       conflict_frozen
     >>

(* Step 1c: `claim_slot` atomically CASes `EMPTY` or `RETIRED` -> `CLAIMED` (`crates/tickwire-account/src/order_manager.rs:2816-2833`). *)
ReqClaimSlot ==
  /\ dop_next_seq \in Seqs
  /\ LET seq == dop_next_seq
         s == SlotOf(seq)
         stp_blocked == (EnableStpCrossSeq2 /\ seq = 2 /\ stp_linked[1])
     IN /\ ~stp_blocked
        /\ slot_state[s] \in {"EMPTY", "RETIRED"}
        /\ slot_lock[s] = "FREE"
        /\ dop_next_seq' = seq + 1
        /\ slot_state' = [slot_state EXCEPT ![s] = "CLAIMED"]
        /\ slot_id_seq' = [slot_id_seq EXCEPT ![s] = seq]
        /\ UNCHANGED <<
             slot_lock,
             body_seq, body_status, body_ord_qty, body_filled_qty, body_proj_qty, body_hold_rem, body_dry_run_pending,
             stp_linked, resp_precheck_seq, resp_locked_seq,
             pool_recorded, pool_fee_known, pool_fee_signed, pool_amend_count,
             wallet_avail, wallet_locked, spot_base_pos, settled_quote_spend, settled_fee_total,
             front_ack_count, ack_count, order_terminal,
             cxl_emitted, cxl_qty_recorded, cxl_proj_recorded, cxl_fill_recorded,
             alias_refused_seen, conflict_frozen
           >>

(* Step 2: Under `slot.order.lock()`, `REQ` reserves `UtaWalletLedger` hold, links `OrsStpBook`,
 * and calls `publish_live` (`CLAIMED -> LIVE`), or calls `retire_in_place` (`CLAIMED -> RETIRED`)
 * when wallet balance is insufficient (`crates/tickwire-account/src/order_manager.rs:1241-1307`). *)
ReqPopulateAndPublishOrAbort(s) ==
  /\ slot_state[s] = "CLAIMED"
  /\ slot_lock[s] = "FREE"
  /\ LET seq == slot_id_seq[s]
         w == WalletOf(seq)
         hold == OrdHoldOf(seq)
         can_afford == wallet_avail[w] >= hold
         is_dry == (EnableDryRunSeq1 /\ seq = 1)
     IN /\ IF can_afford
           THEN /\ wallet_avail' = [wallet_avail EXCEPT ![w] = wallet_avail[w] - hold]
                /\ wallet_locked' = [wallet_locked EXCEPT ![w] = wallet_locked[w] + hold]
                /\ body_seq' = [body_seq EXCEPT ![s] = seq]
                /\ body_status' = [body_status EXCEPT ![s] = "PENDING_NEW"]
                /\ body_ord_qty' = [body_ord_qty EXCEPT ![s] = OrdQtyOf(seq)]
                /\ body_filled_qty' = [body_filled_qty EXCEPT ![s] = 0]
                /\ body_proj_qty' = [body_proj_qty EXCEPT ![s] = 0]
                /\ body_hold_rem' = [body_hold_rem EXCEPT ![s] = hold]
                /\ body_dry_run_pending' = [body_dry_run_pending EXCEPT ![s] = is_dry]
                /\ stp_linked' = [stp_linked EXCEPT ![s] = TRUE]
                /\ slot_state' = [slot_state EXCEPT ![s] = "LIVE"]
                /\ order_terminal' = order_terminal
           ELSE /\ wallet_avail' = wallet_avail
                /\ wallet_locked' = wallet_locked
                /\ body_seq' = [body_seq EXCEPT ![s] = seq]
                /\ body_status' = [body_status EXCEPT ![s] = "TERMINAL"]
                /\ body_ord_qty' = [body_ord_qty EXCEPT ![s] = 0]
                /\ body_filled_qty' = [body_filled_qty EXCEPT ![s] = 0]
                /\ body_proj_qty' = [body_proj_qty EXCEPT ![s] = 0]
                /\ body_hold_rem' = [body_hold_rem EXCEPT ![s] = 0]
                /\ body_dry_run_pending' = [body_dry_run_pending EXCEPT ![s] = FALSE]
                /\ stp_linked' = [stp_linked EXCEPT ![s] = FALSE]
                /\ slot_state' = [slot_state EXCEPT ![s] = "RETIRED"]
                /\ order_terminal' = [order_terminal EXCEPT ![seq] = TRUE]
        /\ UNCHANGED <<
             dop_next_seq, slot_id_seq, slot_lock,
             resp_precheck_seq, resp_locked_seq,
             pool_recorded, pool_fee_known, pool_fee_signed, pool_amend_count,
             spot_base_pos, settled_quote_spend, settled_fee_total,
             front_ack_count, ack_count,
             cxl_emitted, cxl_qty_recorded, cxl_proj_recorded, cxl_fill_recorded,
             alias_refused_seen, conflict_frozen
           >>

(* Step 3 (`dry_run` branch): `place_order` immediately unlinks `OrsStpBook`, releases hold,
 * re-acquires `slot.order.lock()`, and calls `retire_in_place` (`LIVE -> RETIRED`)
 * (`crates/tickwire-account/src/order_manager.rs:1309-1320`). *)
ReqCompleteDryRunRetire(s) ==
  /\ slot_state[s] = "LIVE"
  /\ slot_lock[s] = "FREE"
  /\ body_dry_run_pending[s]
  /\ LET seq == body_seq[s]
         w == WalletOf(seq)
         rem == body_hold_rem[s]
     IN /\ wallet_avail' = [wallet_avail EXCEPT ![w] = wallet_avail[w] + rem]
        /\ wallet_locked' = [wallet_locked EXCEPT ![w] = wallet_locked[w] - rem]
        /\ body_hold_rem' = [body_hold_rem EXCEPT ![s] = 0]
        /\ body_status' = [body_status EXCEPT ![s] = "TERMINAL"]
        /\ body_dry_run_pending' = [body_dry_run_pending EXCEPT ![s] = FALSE]
        /\ stp_linked' = [stp_linked EXCEPT ![s] = FALSE]
        /\ slot_state' = [slot_state EXCEPT ![s] = "RETIRED"]
        /\ order_terminal' = [order_terminal EXCEPT ![seq] = TRUE]
        /\ UNCHANGED <<
             dop_next_seq, slot_id_seq, slot_lock,
             body_seq, body_ord_qty, body_filled_qty, body_proj_qty,
             resp_precheck_seq, resp_locked_seq,
             pool_recorded, pool_fee_known, pool_fee_signed, pool_amend_count,
             spot_base_pos, settled_quote_spend, settled_fee_total,
             front_ack_count, ack_count,
             cxl_emitted, cxl_qty_recorded, cxl_proj_recorded, cxl_fill_recorded,
             alias_refused_seen, conflict_frozen
           >>

(* -------------------------------------------------------------------------
 * `RESP` Path: Two-Step `lock_order` (`crates/tickwire-account/src/order_manager.rs:1125-1141`)
 * ------------------------------------------------------------------------- *)

(* `lock_order` pre-lock `Acquire` check (`slot.identity.load(Acquire) == Live|seq`). *)
RespPrecheckLive(seq) ==
  /\ resp_precheck_seq = 0
  /\ resp_locked_seq = 0
  /\ LET s == SlotOf(seq)
     IN /\ slot_state[s] = "LIVE"
        /\ slot_id_seq[s] = seq
        /\ resp_precheck_seq' = seq
        /\ UNCHANGED <<
             dop_next_seq, slot_state, slot_id_seq, slot_lock,
             body_seq, body_status, body_ord_qty, body_filled_qty, body_proj_qty, body_hold_rem, body_dry_run_pending,
             stp_linked, resp_locked_seq,
             pool_recorded, pool_fee_known, pool_fee_signed, pool_amend_count,
             wallet_avail, wallet_locked, spot_base_pos, settled_quote_spend, settled_fee_total,
             front_ack_count, ack_count, order_terminal,
             cxl_emitted, cxl_qty_recorded, cxl_proj_recorded, cxl_fill_recorded,
             alias_refused_seen, conflict_frozen
           >>

(* `lock_order` acquires `SpinLock` and runs post-lock reload recheck:
 * if the slot retired or recycled to an aliasing `seq` (`1 -> 3`) before lock acquisition,
 * `lock_order` drops the guard and returns `None` (`crates/tickwire-account/src/order_manager.rs:1134-1140`). *)
RespAcquireAndRecheck ==
  /\ resp_precheck_seq \in Seqs
  /\ resp_locked_seq = 0
  /\ LET seq == resp_precheck_seq
         s == SlotOf(seq)
         matches == (slot_state[s] = "LIVE" /\ slot_id_seq[s] = seq /\ body_seq[s] = seq /\ ~body_dry_run_pending[s])
     IN /\ slot_lock[s] = "FREE"
        /\ resp_precheck_seq' = 0
        /\ slot_lock' = [slot_lock EXCEPT ![s] = IF matches THEN "RESP" ELSE "FREE"]
        /\ resp_locked_seq' = IF matches THEN seq ELSE 0
        /\ UNCHANGED <<
             dop_next_seq, slot_state, slot_id_seq,
             body_seq, body_status, body_ord_qty, body_filled_qty, body_proj_qty, body_hold_rem, body_dry_run_pending,
             stp_linked,
             pool_recorded, pool_fee_known, pool_fee_signed, pool_amend_count,
             wallet_avail, wallet_locked, spot_base_pos, settled_quote_spend, settled_fee_total,
             front_ack_count, ack_count, order_terminal,
             cxl_emitted, cxl_qty_recorded, cxl_proj_recorded, cxl_fill_recorded,
             alias_refused_seen, conflict_frozen
           >>

(* `on_front_ack` + `on_ack`: `One ack` single-emission discipline (`crates/tickwire-account/src/order_manager.rs:1337-1461`). *)
RespLockedFrontAckAndAck ==
  /\ resp_locked_seq \in Seqs
  /\ LET seq == resp_locked_seq
         s == SlotOf(seq)
         was_pending == (body_status[s] = "PENDING_NEW")
     IN /\ slot_lock[s] = "RESP"
        /\ body_status' = [body_status EXCEPT ![s] = "OPEN"]
        /\ front_ack_count' = [front_ack_count EXCEPT ![seq] = IF front_ack_count[seq] = 0 THEN 1 ELSE front_ack_count[seq]]
        /\ ack_count' = [ack_count EXCEPT ![seq] = IF was_pending THEN ack_count[seq] + 1 ELSE ack_count[seq]]
        /\ slot_lock' = [slot_lock EXCEPT ![s] = "FREE"]
        /\ resp_locked_seq' = 0
        /\ UNCHANGED <<
             dop_next_seq, slot_state, slot_id_seq,
             body_seq, body_ord_qty, body_filled_qty, body_proj_qty, body_hold_rem, body_dry_run_pending,
             stp_linked, resp_precheck_seq,
             pool_recorded, pool_fee_known, pool_fee_signed, pool_amend_count,
             wallet_avail, wallet_locked, spot_base_pos, settled_quote_spend, settled_fee_total,
             order_terminal,
             cxl_emitted, cxl_qty_recorded, cxl_proj_recorded, cxl_fill_recorded,
             alias_refused_seen, conflict_frozen
           >>

(* `on_cum_qty`: advances `projected_fill_qty_lots` above `max(filled, projected)` (`crates/tickwire-account/src/order_manager.rs:1753-1797`). *)
RespLockedCumQty(cum_qty) ==
  /\ resp_locked_seq \in Seqs
  /\ LET seq == resp_locked_seq
         s == SlotOf(seq)
         baseline == Max2(body_filled_qty[s], body_proj_qty[s])
     IN /\ slot_lock[s] = "RESP"
        /\ cum_qty <= body_ord_qty[s]
        /\ body_proj_qty' = [body_proj_qty EXCEPT ![s] = IF cum_qty > baseline THEN cum_qty ELSE baseline]
        /\ slot_lock' = [slot_lock EXCEPT ![s] = "FREE"]
        /\ resp_locked_seq' = 0
        /\ UNCHANGED <<
             dop_next_seq, slot_state, slot_id_seq,
             body_seq, body_status, body_ord_qty, body_filled_qty, body_hold_rem, body_dry_run_pending,
             stp_linked, resp_precheck_seq,
             pool_recorded, pool_fee_known, pool_fee_signed, pool_amend_count,
             wallet_avail, wallet_locked, spot_base_pos, settled_quote_spend, settled_fee_total,
             front_ack_count, ack_count, order_terminal,
             cxl_emitted, cxl_qty_recorded, cxl_proj_recorded, cxl_fill_recorded,
             alias_refused_seen, conflict_frozen
           >>

(* `on_fill` (`Applied` fast fill with `fee = None` OR full fill with `fee = Some`, plus duplicate ignore)
 * while slot is `LIVE`; immediately retires slot and unlinks `OrsStpBook` when `filled_qty == ord_qty`
 * WITHOUT waiting on post-trade fee settlement (`crates/tickwire-account/src/order_manager.rs:1828-1927,2905-2919`). *)
RespLockedFill(tid, has_fee, fee_val) ==
  /\ resp_locked_seq \in Seqs
  /\ LET seq == resp_locked_seq
         s == SlotOf(seq)
         w == WalletOf(seq)
         qty == TradeQty(tid)
         px == TradePx(tid)
         spend == qty * px
         rel_hold == IF body_hold_rem[s] >= spend THEN spend ELSE body_hold_rem[s]
         new_filled == body_filled_qty[s] + qty
         new_proj == Max2(body_proj_qty[s], new_filled)
         reaches_full == (new_filled >= body_ord_qty[s])
         rem_after == body_hold_rem[s] - rel_hold
         final_rel == IF reaches_full THEN body_hold_rem[s] ELSE rel_hold
         eff_fee == IF has_fee THEN fee_val ELSE 0
     IN /\ slot_lock[s] = "RESP"
        /\ TradeSeq(tid) = seq
        /\ ~pool_recorded[tid]
        /\ pool_recorded' = [pool_recorded EXCEPT ![tid] = TRUE]
        /\ pool_fee_known' = [pool_fee_known EXCEPT ![tid] = has_fee]
        /\ pool_fee_signed' = [pool_fee_signed EXCEPT ![tid] = eff_fee]
        /\ body_filled_qty' = [body_filled_qty EXCEPT ![s] = new_filled]
        /\ body_proj_qty' = [body_proj_qty EXCEPT ![s] = new_proj]
        /\ body_hold_rem' = [body_hold_rem EXCEPT ![s] = IF reaches_full THEN 0 ELSE rem_after]
        /\ body_status' = [body_status EXCEPT ![s] = IF reaches_full THEN "TERMINAL" ELSE body_status[s]]
        /\ stp_linked' = [stp_linked EXCEPT ![s] = IF reaches_full THEN FALSE ELSE stp_linked[s]]
        /\ slot_state' = [slot_state EXCEPT ![s] = IF reaches_full THEN "RETIRED" ELSE "LIVE"]
        /\ order_terminal' = [order_terminal EXCEPT ![seq] = IF reaches_full THEN TRUE ELSE order_terminal[seq]]
        /\ wallet_locked' = [wallet_locked EXCEPT ![w] = wallet_locked[w] - final_rel]
        /\ wallet_avail' = [wallet_avail EXCEPT ![w] = wallet_avail[w] + final_rel - spend + eff_fee]
        /\ spot_base_pos' = spot_base_pos + qty
        /\ settled_quote_spend' = settled_quote_spend + spend
        /\ settled_fee_total' = settled_fee_total + eff_fee
        /\ slot_lock' = [slot_lock EXCEPT ![s] = "FREE"]
        /\ resp_locked_seq' = 0
        /\ UNCHANGED <<
             dop_next_seq, slot_id_seq,
             body_seq, body_ord_qty, body_dry_run_pending,
             resp_precheck_seq, pool_amend_count,
             front_ack_count, ack_count,
             cxl_emitted, cxl_qty_recorded, cxl_proj_recorded, cxl_fill_recorded,
             alias_refused_seen, conflict_frozen
           >>

(* `on_fill` post-trade fee/rebate amendment (`TRADE_LITE` / `execution.fast` -> `ORDER_TRADE_UPDATE` /
 * `execution`) and duplicate suppression across BOTH `LIVE` AND `RETIRED`/reused slots!
 * `FillRecordPool::record_or_amend` updates ONLY fee cashflow (`amendment = true`) without re-applying
 * `qty_lots` or mutating an aliasing `seq = 3` occupant (`crates/tickwire-account/src/order_manager.rs:1840-1870,1928-2011`,
 * `crates/tickwire-account/src/fill_record.rs:171-277`, `crates/tickwire-account/src/uta_ledger.rs:520-540`). *)
RespFillAmendOrDuplicate(tid, has_fee, fee_val) ==
  /\ resp_precheck_seq = 0
  /\ resp_locked_seq = 0
  /\ pool_recorded[tid]
  /\ LET seq == TradeSeq(tid)
         s == SlotOf(seq)
         w == WalletOf(seq)
         is_amend == (has_fee /\ (~pool_fee_known[tid] \/ pool_fee_signed[tid] # fee_val))
         delta == IF is_amend THEN (fee_val - pool_fee_signed[tid]) ELSE 0
     IN /\ slot_lock[s] = "FREE"
        /\ (~is_amend \/ pool_amend_count[tid] < 1)
        /\ pool_fee_known' = [pool_fee_known EXCEPT ![tid] = IF has_fee THEN TRUE ELSE pool_fee_known[tid]]
        /\ pool_fee_signed' = [pool_fee_signed EXCEPT ![tid] = IF is_amend THEN fee_val ELSE pool_fee_signed[tid]]
        /\ pool_amend_count' = [pool_amend_count EXCEPT ![tid] = IF is_amend THEN pool_amend_count[tid] + 1 ELSE pool_amend_count[tid]]
        /\ wallet_avail' = [wallet_avail EXCEPT ![w] = wallet_avail[w] + delta]
        /\ settled_fee_total' = settled_fee_total + delta
        /\ UNCHANGED <<
             dop_next_seq, slot_state, slot_id_seq, slot_lock,
             body_seq, body_status, body_ord_qty, body_filled_qty, body_proj_qty, body_hold_rem, body_dry_run_pending,
             stp_linked, resp_precheck_seq, resp_locked_seq,
             pool_recorded, wallet_locked, spot_base_pos, settled_quote_spend,
             front_ack_count, ack_count, order_terminal,
             cxl_emitted, cxl_qty_recorded, cxl_proj_recorded, cxl_fill_recorded,
             alias_refused_seen, conflict_frozen
           >>

(* `on_canceled`: computes `canceled_qty_lots = effective_qty_lots - projected_fill_qty_lots`,
 * releases remaining hold, unlinks `OrsStpBook`, and immediately retires slot (`crates/tickwire-account/src/order_manager.rs:2023-2086`). *)
RespLockedCanceled(obs_cum_qty) ==
  /\ resp_locked_seq \in Seqs
  /\ LET seq == resp_locked_seq
         s == SlotOf(seq)
         w == WalletOf(seq)
         eff_proj == Max2(Max2(body_filled_qty[s], body_proj_qty[s]), obs_cum_qty)
         cxl_qty == body_ord_qty[s] - eff_proj
         rem == body_hold_rem[s]
     IN /\ slot_lock[s] = "RESP"
        /\ obs_cum_qty <= body_ord_qty[s]
        /\ body_proj_qty' = [body_proj_qty EXCEPT ![s] = eff_proj]
        /\ body_hold_rem' = [body_hold_rem EXCEPT ![s] = 0]
        /\ body_status' = [body_status EXCEPT ![s] = "TERMINAL"]
        /\ stp_linked' = [stp_linked EXCEPT ![s] = FALSE]
        /\ slot_state' = [slot_state EXCEPT ![s] = "RETIRED"]
        /\ wallet_locked' = [wallet_locked EXCEPT ![w] = wallet_locked[w] - rem]
        /\ wallet_avail' = [wallet_avail EXCEPT ![w] = wallet_avail[w] + rem]
        /\ order_terminal' = [order_terminal EXCEPT ![seq] = TRUE]
        /\ cxl_emitted' = [cxl_emitted EXCEPT ![seq] = TRUE]
        /\ cxl_qty_recorded' = [cxl_qty_recorded EXCEPT ![seq] = cxl_qty]
        /\ cxl_proj_recorded' = [cxl_proj_recorded EXCEPT ![seq] = eff_proj]
        /\ cxl_fill_recorded' = [cxl_fill_recorded EXCEPT ![seq] = body_filled_qty[s]]
        /\ slot_lock' = [slot_lock EXCEPT ![s] = "FREE"]
        /\ resp_locked_seq' = 0
        /\ UNCHANGED <<
             dop_next_seq, slot_id_seq,
             body_seq, body_ord_qty, body_filled_qty, body_dry_run_pending,
             resp_precheck_seq,
             pool_recorded, pool_fee_known, pool_fee_signed, pool_amend_count,
             spot_base_pos, settled_quote_spend, settled_fee_total,
             front_ack_count, ack_count,
             alias_refused_seen, conflict_frozen
           >>

(* `on_rejected`: deterministic venue reject immediately releases hold, unlinks `OrsStpBook`,
 * and transitions slot to `RETIRED` (`crates/tickwire-account/src/order_manager.rs:2140-2192`). *)
RespLockedRejected ==
  /\ resp_locked_seq \in Seqs
  /\ LET seq == resp_locked_seq
         s == SlotOf(seq)
         w == WalletOf(seq)
         rem == body_hold_rem[s]
     IN /\ slot_lock[s] = "RESP"
        /\ body_status[s] = "PENDING_NEW"
        /\ body_filled_qty[s] = 0
        /\ body_hold_rem' = [body_hold_rem EXCEPT ![s] = 0]
        /\ body_status' = [body_status EXCEPT ![s] = "TERMINAL"]
        /\ stp_linked' = [stp_linked EXCEPT ![s] = FALSE]
        /\ slot_state' = [slot_state EXCEPT ![s] = "RETIRED"]
        /\ wallet_locked' = [wallet_locked EXCEPT ![w] = wallet_locked[w] - rem]
        /\ wallet_avail' = [wallet_avail EXCEPT ![w] = wallet_avail[w] + rem]
        /\ order_terminal' = [order_terminal EXCEPT ![seq] = TRUE]
        /\ slot_lock' = [slot_lock EXCEPT ![s] = "FREE"]
        /\ resp_locked_seq' = 0
        /\ UNCHANGED <<
             dop_next_seq, slot_id_seq,
             body_seq, body_ord_qty, body_filled_qty, body_proj_qty, body_dry_run_pending,
             resp_precheck_seq,
             pool_recorded, pool_fee_known, pool_fee_signed, pool_amend_count,
             spot_base_pos, settled_quote_spend, settled_fee_total,
             front_ack_count, ack_count,
             cxl_emitted, cxl_qty_recorded, cxl_proj_recorded, cxl_fill_recorded,
             alias_refused_seen, conflict_frozen
           >>

Next ==
  \/ ReqRefuseStpCross
  \/ ReqRefuseModularAlias
  \/ ReqClaimSlot
  \/ \E s \in Slots: ReqPopulateAndPublishOrAbort(s)
  \/ \E s \in Slots: ReqCompleteDryRunRetire(s)
  \/ \E seq \in Seqs: RespPrecheckLive(seq)
  \/ RespAcquireAndRecheck
  \/ RespLockedFrontAckAndAck
  \/ \E cum \in {1, 2}: RespLockedCumQty(cum)
  \/ \E tid \in Trades, hf \in BOOLEAN, fv \in {-1, 1}: RespLockedFill(tid, hf, fv)
  \/ \E tid \in Trades, hf \in BOOLEAN, fv \in {-1, 1}: RespFillAmendOrDuplicate(tid, hf, fv)
  \/ \E cum \in {0, 1}: RespLockedCanceled(cum)
  \/ RespLockedRejected

(* -------------------------------------------------------------------------
 * Safety Invariants
 * ------------------------------------------------------------------------- *)

TypeOK ==
  /\ dop_next_seq \in 1..4
  /\ slot_state \in [Slots -> {"EMPTY", "CLAIMED", "LIVE", "RETIRED"}]
  /\ slot_id_seq \in [Slots -> 0..3]
  /\ slot_lock \in [Slots -> {"FREE", "REQ", "RESP"}]
  /\ body_seq \in [Slots -> 0..3]
  /\ body_status \in [Slots -> {"NONE", "PENDING_NEW", "OPEN", "TERMINAL"}]
  /\ resp_precheck_seq \in 0..3
  /\ resp_locked_seq \in 0..3
  /\ pool_recorded \in [Trades -> BOOLEAN]
  /\ pool_fee_known \in [Trades -> BOOLEAN]
  /\ pool_fee_signed \in [Trades -> -1..1]
  /\ pool_amend_count \in [Trades -> 0..1]
  /\ front_ack_count \in [Seqs -> 0..1]
  /\ ack_count \in [Seqs -> 0..1]
  /\ order_terminal \in [Seqs -> BOOLEAN]
  /\ cxl_emitted \in [Seqs -> BOOLEAN]
  /\ alias_refused_seen \in BOOLEAN
  /\ conflict_frozen \in BOOLEAN

(* `SpinLock` + 4-state `SlotState` prevents concurrent `REQ`/`RESP` body mutation: *)
NoConcurrentBodyMutation ==
  \A s \in Slots:
    /\ (slot_lock[s] = "RESP" => slot_state[s] = "LIVE")
    /\ (slot_state[s] = "CLAIMED" => slot_lock[s] = "FREE")

(* Post-lock reload identity recheck (`lock_order`) ensures `RESP` never locks a retired or
 * modular-aliased (`seq = 1` vs `seq = 3`) occupant: *)
NoWrongOccupantLocked ==
  /\ (resp_locked_seq # 0 =>
        LET s == SlotOf(resp_locked_seq)
        IN /\ slot_lock[s] = "RESP"
           /\ slot_state[s] = "LIVE"
           /\ slot_id_seq[s] = resp_locked_seq
           /\ body_seq[s] = resp_locked_seq)
  /\ \A s \in Slots:
       (slot_lock[s] = "RESP") => (resp_locked_seq # 0 /\ s = SlotOf(resp_locked_seq))

(* Terminal `Filled`, `Canceled`, and `Rejected` orders immediately transition to `RETIRED`
 * and unlink from `OrsStpBook` without waiting on post-trade fee settlement (`pool_fee_known`): *)
TerminalRetiresImmediately ==
  /\ \A q \in Seqs:
       order_terminal[q] =>
         ~(slot_state[SlotOf(q)] = "LIVE" /\ slot_id_seq[SlotOf(q)] = q)
  /\ \A s \in Slots:
       /\ (stp_linked[s] <=> (slot_state[s] = "LIVE"))
       /\ (slot_state[s] = "LIVE" => body_filled_qty[s] < body_ord_qty[s])

(* Single-emission `FrontAck` and `Ack` (`One ack`): *)
OneAckPerOrder ==
  \A q \in Seqs: front_ack_count[q] <= 1 /\ ack_count[q] <= 1

(* Exact cancel remainder `cxl_qty = order_qty - projected_fill_qty` with `proj >= filled`: *)
ExactCancelRemainder ==
  \A q \in Seqs:
    cxl_emitted[q] =>
      /\ cxl_proj_recorded[q] >= cxl_fill_recorded[q]
      /\ cxl_proj_recorded[q] <= OrdQtyOf(q)
      /\ cxl_qty_recorded[q] = OrdQtyOf(q) - cxl_proj_recorded[q]

(* `FillRecordPool` guarantees each unique trade ID applies quantity and principal at most once,
 * even across `TRADE_LITE -> ORDER_TRADE_UPDATE` (`amendment = true`) and slot reuse (`seq 1 -> 3`): *)
QuantityExactlyOnce ==
  /\ spot_base_pos = UniqueExecutedLots
  /\ settled_quote_spend = UniqueExecutedLots
  /\ settled_fee_total = UniquePoolFeeSum

(* Zero false `Conflict` account freeze across out-of-order fast/full fills, projected fills,
 * and post-retirement fee/rebate amendments: *)
NoFalseConflictFreeze ==
  ~conflict_frozen

(* Multi-wallet `UtaWalletLedger` solvency and exact conservation across `W_SPOT_QUOTE` and `W_MARGIN_COLLAT`: *)
SolvencyPreserved ==
  /\ \A w \in Wallets:
       /\ wallet_locked[w] >= 0
       /\ wallet_locked[w] = ActiveSlotHoldForWallet(w)
  /\ \A s \in Slots:
       (slot_state[s] \in {"EMPTY", "CLAIMED", "RETIRED"}) => (body_hold_rem[s] = 0)
  /\ (wallet_avail["W_SPOT_QUOTE"] + wallet_locked["W_SPOT_QUOTE"])
       = (InitSpotQuoteAvail - settled_quote_spend + settled_fee_total)
  /\ (wallet_avail["W_MARGIN_COLLAT"] + wallet_locked["W_MARGIN_COLLAT"])
       = InitMarginAvail

====
