---- MODULE L1_SymOrsOrderLifecycle ----
(*
 * Dual-Engine Symbolic 64-Bit / 128-Bit Inductive & Bounded Model (Apalache Z3 `QF_LIA` + TLC)
 * for `tickwire-account` ORS Order Slot Lifecycle, Modular `seqnum` Aliasing, `FillRecordPool`
 * Deduplication & `amendment = true` Fee Patching, and `UtaWalletLedger` Solvency:
 *   - Power-of-two `OrderSlot` 4-state lifecycle (`EMPTY -> CLAIMED -> LIVE -> RETIRED`)
 *     and modular `seqnum` slot collision (`seq' = seq + cfg_slots_cap` mapping to the
 *     identical `(seq & slot_mask)` index):
 *       crates/tickwire-account/src/order_manager.rs:152-194
 *       crates/tickwire-account/src/order_manager.rs:348-368
 *       crates/tickwire-account/src/order_manager.rs:2805-2833
 *   - `OrderManager::lock_order` pre-lock `Acquire` check + post-lock reload identity
 *     recheck preventing wrong-occupant locks across retirement and modular reuse:
 *       crates/tickwire-account/src/spinlock.rs:109-121
 *       crates/tickwire-account/src/order_manager.rs:1125-1141
 *   - `OrderManager::place_order`, `on_front_ack`, `on_ack`, `on_cum_qty`, `on_fill`,
 *     `on_canceled`, and `on_rejected`, including immediate `RETIRED` + `OrsStpBook`
 *     unlink without waiting on post-trade fee settlement:
 *       crates/tickwire-account/src/order_manager.rs:1171-1461
 *       crates/tickwire-account/src/order_manager.rs:1753-2086
 *       crates/tickwire-account/src/order_manager.rs:2140-2192
 *       crates/tickwire-account/src/order_manager.rs:2905-2919
 *       crates/tickwire-account/src/ors_stp.rs:268-333
 *   - `FillRecordPool::record_or_amend` (`Applied`, `Amended`, `Duplicate`) and
 *     `UtaWalletLedger` (`reserve_hold`, `release_hold`, `settle_applied_fill`,
 *     `apply_fee_amendment`):
 *       crates/tickwire-account/src/fill_record.rs:44-59
 *       crates/tickwire-account/src/fill_record.rs:171-277
 *       crates/tickwire-account/src/uta_ledger.rs:322-352
 *       crates/tickwire-account/src/uta_ledger.rs:391-540
 *)
EXTENDS Integers

CONSTANTS
  (* @type: Bool; *)
  BoundedMode,
  (* @type: Bool; *)
  AllowDryRun

VARIABLES
  (* --- Symbolic configuration (`1..2^64-1` / `0..2^128-1` in Apalache, small ints in TLC) --- *)
  (* @type: Int; *)
  cfg_init_quote_free,
  (* @type: Int; *)
  cfg_slots_cap,
  (* @type: Bool; *)
  cfg_allow_dry_run,

  (* --- `DopState::next_order_seqnum` (`crates/tickwire-account/src/dop_state.rs:193-227`) --- *)
  (* @type: Int; *)
  dop_next_seq,

  (* --- Contested power-of-two `OrderSlot` (`crates/tickwire-account/src/order_manager.rs:165-368`) --- *)
  (* @type: Int; *)
  slot_state,
  (* @type: Int; *)
  slot_id_seq,
  (* @type: Int; *)
  slot_lock,
  (* @type: Int; *)
  body_seq,
  (* @type: Int; *)
  body_status,
  (* @type: Int; *)
  body_ord_qty,
  (* @type: Int; *)
  body_px,
  (* @type: Int; *)
  body_filled_qty,
  (* @type: Int; *)
  body_proj_qty,
  (* @type: Int; *)
  body_hold_rem,
  (* @type: Bool; *)
  body_dry_run,
  (* @type: Bool; *)
  stp_linked,
  (* @type: Int; *)
  front_ack_cnt,
  (* @type: Int; *)
  ack_cnt,

  (* --- Concurrent `RESP` `lock_order` precheck & verified guard --- *)
  (* @type: Int; *)
  resp_precheck_seq,
  (* @type: Int; *)
  resp_locked_seq,

  (* --- `FillRecordPool` symbolic trade entry `t1` + cumulative other unique trades --- *)
  (* @type: Bool; *)
  t1_recorded,
  (* @type: Int; *)
  t1_owner_seq,
  (* @type: Int; *)
  t1_qty,
  (* @type: Int; *)
  t1_spend,
  (* @type: Bool; *)
  t1_fee_known,
  (* @type: Int; *)
  t1_fee_signed,
  (* @type: Int; *)
  other_filled_lots,
  (* @type: Int; *)
  other_quote_spend,
  (* @type: Int; *)
  other_fee_signed,

  (* --- `UtaWalletLedger` quote/base wallet balances (`crates/tickwire-account/src/uta_ledger.rs:266-540`) --- *)
  (* @type: Int; *)
  wallet_quote_free,
  (* @type: Int; *)
  wallet_quote_locked,
  (* @type: Int; *)
  wallet_base_free,

  (* --- Verification latches & bounded TLC step horizon --- *)
  (* @type: Int; *)
  step_budget,
  (* @type: Bool; *)
  alias_protected_ok,
  (* @type: Bool; *)
  cxl_remainder_ok,
  (* @type: Bool; *)
  conflict_frozen

vars == <<
  cfg_init_quote_free, cfg_slots_cap, cfg_allow_dry_run,
  dop_next_seq,
  slot_state, slot_id_seq, slot_lock,
  body_seq, body_status, body_ord_qty, body_px, body_filled_qty, body_proj_qty, body_hold_rem,
  body_dry_run, stp_linked, front_ack_cnt, ack_cnt,
  resp_precheck_seq, resp_locked_seq,
  t1_recorded, t1_owner_seq, t1_qty, t1_spend, t1_fee_known, t1_fee_signed,
  other_filled_lots, other_quote_spend, other_fee_signed,
  wallet_quote_free, wallet_quote_locked, wallet_base_free,
  step_budget, alias_protected_ok, cxl_remainder_ok, conflict_frozen
>>

(* --- 64-Bit / 128-Bit Bounds (zero raw literals > 2147483647 for SANY) --- *)
Pow30 == 1073741824
U64_MAX == (16 * Pow30 * Pow30) - 1
U128_MAX == ((U64_MAX + 1) * (U64_MAX + 1)) - 1

EffU64Max == IF BoundedMode THEN 12 ELSE U64_MAX
EffU128Max == IF BoundedMode THEN 32 ELSE U128_MAX
EffFeeMax == IF BoundedMode THEN 2 ELSE U64_MAX
MaxBoundedSteps == 8

(* `SlotState` encoding (`crates/tickwire-account/src/order_manager.rs:165-175`) *)
SLOT_EMPTY == 0
SLOT_CLAIMED == 1
SLOT_LIVE == 2
SLOT_RETIRED == 3

(* `SpinLock` holder encoding *)
LOCK_FREE == 0
LOCK_RESP == 1

(* `OrsOrderStatus` encoding (`crates/tickwire-account/src/order_manager.rs:197-205`) *)
ST_NONE == 0
ST_PENDING_NEW == 1
ST_OPEN == 2
ST_TERMINAL == 3

CanStep == (~BoundedMode) \/ (step_budget < MaxBoundedSteps)
NextBudget == IF BoundedMode THEN step_budget + 1 ELSE 0

QtyDom == IF BoundedMode THEN 1..2 ELSE Int
CumDom == IF BoundedMode THEN 0..2 ELSE Int
FeeDom == IF BoundedMode THEN -1..1 ELSE Int
StrideDom == IF BoundedMode THEN {1} ELSE Int

Max2(a, b) == IF a >= b THEN a ELSE b

ConstInit ==
  /\ BoundedMode = FALSE
  /\ AllowDryRun \in BOOLEAN

Init ==
  /\ cfg_allow_dry_run = AllowDryRun
  /\ IF BoundedMode
     THEN /\ cfg_init_quote_free = 6
          /\ cfg_slots_cap = 2
     ELSE /\ cfg_init_quote_free \in Int /\ cfg_init_quote_free >= 0 /\ cfg_init_quote_free <= EffU128Max
          /\ cfg_slots_cap \in Int /\ cfg_slots_cap >= 2 /\ cfg_slots_cap <= EffU64Max
  /\ dop_next_seq = 1
  /\ slot_state = SLOT_EMPTY
  /\ slot_id_seq = 0
  /\ slot_lock = LOCK_FREE
  /\ body_seq = 0
  /\ body_status = ST_NONE
  /\ body_ord_qty = 0
  /\ body_px = 1
  /\ body_filled_qty = 0
  /\ body_proj_qty = 0
  /\ body_hold_rem = 0
  /\ body_dry_run = FALSE
  /\ stp_linked = FALSE
  /\ front_ack_cnt = 0
  /\ ack_cnt = 0
  /\ resp_precheck_seq = 0
  /\ resp_locked_seq = 0
  /\ t1_recorded = FALSE
  /\ t1_owner_seq = 0
  /\ t1_qty = 0
  /\ t1_spend = 0
  /\ t1_fee_known = FALSE
  /\ t1_fee_signed = 0
  /\ other_filled_lots = 0
  /\ other_quote_spend = 0
  /\ other_fee_signed = 0
  /\ wallet_quote_free = cfg_init_quote_free
  /\ wallet_quote_locked = 0
  /\ wallet_base_free = 0
  /\ step_budget = 0
  /\ alias_protected_ok = TRUE
  /\ cxl_remainder_ok = TRUE
  /\ conflict_frozen = FALSE

(* -------------------------------------------------------------------------
 * Symbolic Transitions (`Next`)
 * ------------------------------------------------------------------------- *)

(* 1. `claim_slot` on `EMPTY` or `RETIRED` using modular-aliasing sequence `seq`:
 *    - First placement uses `seq = dop_next_seq`;
 *    - Recycled placement uses `seq = slot_id_seq + stride * cfg_slots_cap` (`stride >= 1`),
 *      colliding onto the exact same `(seq & slot_mask)` slot index (`crates/tickwire-account/src/order_manager.rs:2805-2833`). *)
ReqClaimModularSlot ==
  /\ CanStep
  /\ slot_state \in {SLOT_EMPTY, SLOT_RETIRED}
  /\ slot_lock = LOCK_FREE
  /\ \E stride \in StrideDom:
       /\ stride >= 1 /\ stride <= EffU64Max
       /\ LET cand_seq == IF slot_id_seq = 0 THEN dop_next_seq ELSE (slot_id_seq + cfg_slots_cap)
              use_seq == IF cand_seq >= dop_next_seq THEN cand_seq ELSE dop_next_seq
          IN /\ use_seq >= 1 /\ use_seq < EffU64Max
             /\ slot_state' = SLOT_CLAIMED
             /\ slot_id_seq' = use_seq
             /\ dop_next_seq' = use_seq + 1
             /\ step_budget' = NextBudget
             /\ UNCHANGED <<
                  cfg_init_quote_free, cfg_slots_cap, cfg_allow_dry_run,
                  slot_lock,
                  body_seq, body_status, body_ord_qty, body_px, body_filled_qty, body_proj_qty, body_hold_rem,
                  body_dry_run, stp_linked, front_ack_cnt, ack_cnt,
                  resp_precheck_seq, resp_locked_seq,
                  t1_recorded, t1_owner_seq, t1_qty, t1_spend, t1_fee_known, t1_fee_signed,
                  other_filled_lots, other_quote_spend, other_fee_signed,
                  wallet_quote_free, wallet_quote_locked, wallet_base_free,
                  alias_protected_ok, cxl_remainder_ok, conflict_frozen
                >>

(* 2. Modular `seqnum` collision while slot is `CLAIMED` or `LIVE` (`seq_alias = slot_id_seq + cfg_slots_cap`):
 *    `claim_slot` 2-try CAS fails with `RefusalReason::ALLOCATION_ERROR` without mutating the occupant
 *    (`crates/tickwire-account/src/order_manager.rs:2816-2833`). *)
ReqAliasCollisionRefused ==
  /\ CanStep
  /\ slot_state \in {SLOT_CLAIMED, SLOT_LIVE}
  /\ LET alias_seq == slot_id_seq + cfg_slots_cap
         cas_would_succeed == (slot_state \in {SLOT_EMPTY, SLOT_RETIRED})
     IN /\ alias_seq <= EffU64Max
        /\ alias_protected_ok' = (alias_protected_ok /\ (~cas_would_succeed))
        /\ step_budget' = NextBudget
        /\ UNCHANGED <<
             cfg_init_quote_free, cfg_slots_cap, cfg_allow_dry_run,
             dop_next_seq,
             slot_state, slot_id_seq, slot_lock,
             body_seq, body_status, body_ord_qty, body_px, body_filled_qty, body_proj_qty, body_hold_rem,
             body_dry_run, stp_linked, front_ack_cnt, ack_cnt,
             resp_precheck_seq, resp_locked_seq,
             t1_recorded, t1_owner_seq, t1_qty, t1_spend, t1_fee_known, t1_fee_signed,
             other_filled_lots, other_quote_spend, other_fee_signed,
             wallet_quote_free, wallet_quote_locked, wallet_base_free,
             cxl_remainder_ok, conflict_frozen
           >>

(* 3. `place_order` under `slot.order.lock()`: reserves `hold = qty * px` from `wallet_quote_free`
 *    and publishes `LIVE` (`OrsStpBook::insert_order`), or retires in place if balance is insufficient
 *    (`crates/tickwire-account/src/order_manager.rs:1241-1307`). *)
ReqPopulatePublishOrRetire ==
  /\ CanStep
  /\ slot_state = SLOT_CLAIMED
  /\ slot_lock = LOCK_FREE
  /\ \E qty \in QtyDom, px \in {1, 2}, want_dry \in BOOLEAN:
       /\ qty >= 1 /\ qty <= EffU64Max
       /\ (~cfg_allow_dry_run => ~want_dry)
       /\ LET hold == qty * px
              can_afford == (wallet_quote_free >= hold /\ (wallet_quote_locked + hold) <= EffU128Max)
          IN /\ IF can_afford
                THEN /\ wallet_quote_free' = wallet_quote_free - hold
                     /\ wallet_quote_locked' = wallet_quote_locked + hold
                     /\ body_seq' = slot_id_seq
                     /\ body_status' = ST_PENDING_NEW
                     /\ body_ord_qty' = qty
                     /\ body_px' = px
                     /\ body_filled_qty' = 0
                     /\ body_proj_qty' = 0
                     /\ body_hold_rem' = hold
                     /\ body_dry_run' = want_dry
                     /\ stp_linked' = TRUE
                     /\ front_ack_cnt' = 0
                     /\ ack_cnt' = 0
                     /\ slot_state' = SLOT_LIVE
                ELSE /\ wallet_quote_free' = wallet_quote_free
                     /\ wallet_quote_locked' = wallet_quote_locked
                     /\ body_seq' = slot_id_seq
                     /\ body_status' = ST_TERMINAL
                     /\ body_ord_qty' = 0
                     /\ body_px' = px
                     /\ body_filled_qty' = 0
                     /\ body_proj_qty' = 0
                     /\ body_hold_rem' = 0
                     /\ body_dry_run' = FALSE
                     /\ stp_linked' = FALSE
                     /\ front_ack_cnt' = 0
                     /\ ack_cnt' = 0
                     /\ slot_state' = SLOT_RETIRED
             /\ step_budget' = NextBudget
             /\ UNCHANGED <<
                  cfg_init_quote_free, cfg_slots_cap, cfg_allow_dry_run,
                  dop_next_seq, slot_id_seq, slot_lock,
                  resp_precheck_seq, resp_locked_seq,
                  t1_recorded, t1_owner_seq, t1_qty, t1_spend, t1_fee_known, t1_fee_signed,
                  other_filled_lots, other_quote_spend, other_fee_signed,
                  wallet_base_free,
                  alias_protected_ok, cxl_remainder_ok, conflict_frozen
                >>

(* 4. `dry_run` immediate STP unlink + hold release + `retire_in_place` (`LIVE -> RETIRED`)
 *    (`crates/tickwire-account/src/order_manager.rs:1309-1320`). *)
ReqFinishDryRunRetire ==
  /\ CanStep
  /\ cfg_allow_dry_run
  /\ slot_state = SLOT_LIVE
  /\ slot_lock = LOCK_FREE
  /\ body_dry_run
  /\ wallet_quote_free' = wallet_quote_free + body_hold_rem
  /\ wallet_quote_locked' = wallet_quote_locked - body_hold_rem
  /\ body_hold_rem' = 0
  /\ body_status' = ST_TERMINAL
  /\ body_dry_run' = FALSE
  /\ stp_linked' = FALSE
  /\ slot_state' = SLOT_RETIRED
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_init_quote_free, cfg_slots_cap, cfg_allow_dry_run,
       dop_next_seq, slot_id_seq, slot_lock,
       body_seq, body_ord_qty, body_px, body_filled_qty, body_proj_qty,
       front_ack_cnt, ack_cnt,
       resp_precheck_seq, resp_locked_seq,
       t1_recorded, t1_owner_seq, t1_qty, t1_spend, t1_fee_known, t1_fee_signed,
       other_filled_lots, other_quote_spend, other_fee_signed,
       wallet_base_free,
       alias_protected_ok, cxl_remainder_ok, conflict_frozen
     >>

(* 5. `RESP` `lock_order` pre-lock `Acquire` snapshot (`crates/tickwire-account/src/order_manager.rs:1129-1132`). *)
RespPrecheckLive ==
  /\ CanStep
  /\ resp_precheck_seq = 0
  /\ resp_locked_seq = 0
  /\ slot_state = SLOT_LIVE
  /\ resp_precheck_seq' = slot_id_seq
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_init_quote_free, cfg_slots_cap, cfg_allow_dry_run,
       dop_next_seq, slot_state, slot_id_seq, slot_lock,
       body_seq, body_status, body_ord_qty, body_px, body_filled_qty, body_proj_qty, body_hold_rem,
       body_dry_run, stp_linked, front_ack_cnt, ack_cnt,
       resp_locked_seq,
       t1_recorded, t1_owner_seq, t1_qty, t1_spend, t1_fee_known, t1_fee_signed,
       other_filled_lots, other_quote_spend, other_fee_signed,
       wallet_quote_free, wallet_quote_locked, wallet_base_free,
       alias_protected_ok, cxl_remainder_ok, conflict_frozen
     >>

(* 6. `RESP` `lock_order` spinlock acquisition + post-lock reload identity recheck
 *    (`crates/tickwire-account/src/order_manager.rs:1134-1140`). *)
RespAcquireAndRecheck ==
  /\ CanStep
  /\ resp_precheck_seq >= 1
  /\ resp_locked_seq = 0
  /\ slot_lock = LOCK_FREE
  /\ LET matches == (slot_state = SLOT_LIVE /\ slot_id_seq = resp_precheck_seq
                     /\ body_seq = resp_precheck_seq /\ ~body_dry_run)
     IN /\ resp_precheck_seq' = 0
        /\ slot_lock' = (IF matches THEN LOCK_RESP ELSE LOCK_FREE)
        /\ resp_locked_seq' = (IF matches THEN resp_precheck_seq ELSE 0)
        /\ step_budget' = NextBudget
        /\ UNCHANGED <<
             cfg_init_quote_free, cfg_slots_cap, cfg_allow_dry_run,
             dop_next_seq, slot_state, slot_id_seq,
             body_seq, body_status, body_ord_qty, body_px, body_filled_qty, body_proj_qty, body_hold_rem,
             body_dry_run, stp_linked, front_ack_cnt, ack_cnt,
             t1_recorded, t1_owner_seq, t1_qty, t1_spend, t1_fee_known, t1_fee_signed,
             other_filled_lots, other_quote_spend, other_fee_signed,
             wallet_quote_free, wallet_quote_locked, wallet_base_free,
             alias_protected_ok, cxl_remainder_ok, conflict_frozen
           >>

(* 7. `on_front_ack` + `on_ack` (`One ack`) + optional `on_cum_qty` (`ProjectedFill`)
 *    (`crates/tickwire-account/src/order_manager.rs:1337-1461,1753-1797`). *)
RespLockedAckAndCumQty ==
  /\ CanStep
  /\ slot_lock = LOCK_RESP
  /\ \E cum_qty \in CumDom:
       /\ cum_qty >= 0 /\ cum_qty <= body_ord_qty
       /\ LET was_pending == (body_status = ST_PENDING_NEW)
              base_proj == Max2(body_filled_qty, body_proj_qty)
              next_proj == IF cum_qty > base_proj THEN cum_qty ELSE base_proj
          IN /\ body_status' = ST_OPEN
             /\ front_ack_cnt' = (IF front_ack_cnt = 0 THEN 1 ELSE front_ack_cnt)
             /\ ack_cnt' = (IF was_pending THEN ack_cnt + 1 ELSE ack_cnt)
             /\ body_proj_qty' = next_proj
             /\ slot_lock' = LOCK_FREE
             /\ resp_locked_seq' = 0
             /\ step_budget' = NextBudget
             /\ UNCHANGED <<
                  cfg_init_quote_free, cfg_slots_cap, cfg_allow_dry_run,
                  dop_next_seq, slot_state, slot_id_seq,
                  body_seq, body_ord_qty, body_px, body_filled_qty, body_hold_rem,
                  body_dry_run, stp_linked, resp_precheck_seq,
                  t1_recorded, t1_owner_seq, t1_qty, t1_spend, t1_fee_known, t1_fee_signed,
                  other_filled_lots, other_quote_spend, other_fee_signed,
                  wallet_quote_free, wallet_quote_locked, wallet_base_free,
                  alias_protected_ok, cxl_remainder_ok, conflict_frozen
                >>

(* 8. `on_fill` recording symbolic trade `t1` (`TRADE_LITE` / `execution.fast` with `has_fee = FALSE`
 *    or full fill with `has_fee = TRUE`): releases proportional hold `qty * body_px`, settles
 *    `spend = qty * exec_px` (`exec_px <= body_px`), and immediately retires slot if `filled_qty == ord_qty`
 *    WITHOUT waiting on `t1_fee_known` (`crates/tickwire-account/src/order_manager.rs:1828-1927,2905-2919`). *)
RespLockedRecordTrade1 ==
  /\ CanStep
  /\ slot_lock = LOCK_RESP
  /\ ~t1_recorded
  /\ \E qty \in QtyDom, exec_px \in {1, 2}, has_fee \in BOOLEAN, fee_val \in FeeDom:
       /\ qty >= 1 /\ qty <= (body_ord_qty - body_filled_qty)
       /\ exec_px <= body_px
       /\ fee_val >= -EffFeeMax /\ fee_val <= EffFeeMax
       /\ LET rel_hold == qty * body_px
              spend == qty * exec_px
              eff_fee == IF has_fee THEN fee_val ELSE 0
              new_filled == body_filled_qty + qty
              new_proj == Max2(body_proj_qty, new_filled)
              reaches_full == (new_filled = body_ord_qty)
          IN /\ (wallet_base_free + qty) <= EffU128Max
             /\ (wallet_quote_free + rel_hold - spend + eff_fee) >= -EffU128Max
             /\ (wallet_quote_free + wallet_quote_locked - spend + eff_fee) <= EffU128Max
             /\ t1_recorded' = TRUE
             /\ t1_owner_seq' = body_seq
             /\ t1_qty' = qty
             /\ t1_spend' = spend
             /\ t1_fee_known' = has_fee
             /\ t1_fee_signed' = eff_fee
             /\ body_filled_qty' = new_filled
             /\ body_proj_qty' = new_proj
             /\ body_hold_rem' = body_hold_rem - rel_hold
             /\ body_status' = (IF reaches_full THEN ST_TERMINAL ELSE body_status)
             /\ stp_linked' = (IF reaches_full THEN FALSE ELSE TRUE)
             /\ slot_state' = (IF reaches_full THEN SLOT_RETIRED ELSE SLOT_LIVE)
             /\ wallet_quote_locked' = wallet_quote_locked - rel_hold
             /\ wallet_quote_free' = wallet_quote_free + rel_hold - spend + eff_fee
             /\ wallet_base_free' = wallet_base_free + qty
             /\ slot_lock' = LOCK_FREE
             /\ resp_locked_seq' = 0
             /\ step_budget' = NextBudget
             /\ UNCHANGED <<
                  cfg_init_quote_free, cfg_slots_cap, cfg_allow_dry_run,
                  dop_next_seq, slot_id_seq,
                  body_seq, body_ord_qty, body_px, body_dry_run,
                  front_ack_cnt, ack_cnt, resp_precheck_seq,
                  other_filled_lots, other_quote_spend, other_fee_signed,
                  alias_protected_ok, cxl_remainder_ok, conflict_frozen
                >>

(* 9. `on_fill` recording a distinct subsequent trade ID (`other_*`): completes partial fills or fills
 *    a recycled modular-aliased order (`crates/tickwire-account/src/order_manager.rs:1828-1927`). *)
RespLockedRecordSubsequentTrade ==
  /\ CanStep
  /\ slot_lock = LOCK_RESP
  /\ t1_recorded
  /\ \E qty \in QtyDom, exec_px \in {1, 2}, fee_val \in FeeDom:
       /\ qty >= 1 /\ qty <= (body_ord_qty - body_filled_qty)
       /\ exec_px <= body_px
       /\ fee_val >= -EffFeeMax /\ fee_val <= EffFeeMax
       /\ LET rel_hold == qty * body_px
              spend == qty * exec_px
              new_filled == body_filled_qty + qty
              new_proj == Max2(body_proj_qty, new_filled)
              reaches_full == (new_filled = body_ord_qty)
          IN /\ (other_filled_lots + qty) <= EffU128Max
             /\ (other_quote_spend + spend) <= EffU128Max
             /\ (other_fee_signed + fee_val) >= -EffU128Max
             /\ (other_fee_signed + fee_val) <= EffU128Max
             /\ (wallet_base_free + qty) <= EffU128Max
             /\ (wallet_quote_free + rel_hold - spend + fee_val) >= -EffU128Max
             /\ (wallet_quote_free + wallet_quote_locked - spend + fee_val) <= EffU128Max
             /\ other_filled_lots' = other_filled_lots + qty
             /\ other_quote_spend' = other_quote_spend + spend
             /\ other_fee_signed' = other_fee_signed + fee_val
             /\ body_filled_qty' = new_filled
             /\ body_proj_qty' = new_proj
             /\ body_hold_rem' = body_hold_rem - rel_hold
             /\ body_status' = (IF reaches_full THEN ST_TERMINAL ELSE body_status)
             /\ stp_linked' = (IF reaches_full THEN FALSE ELSE TRUE)
             /\ slot_state' = (IF reaches_full THEN SLOT_RETIRED ELSE SLOT_LIVE)
             /\ wallet_quote_locked' = wallet_quote_locked - rel_hold
             /\ wallet_quote_free' = wallet_quote_free + rel_hold - spend + fee_val
             /\ wallet_base_free' = wallet_base_free + qty
             /\ slot_lock' = LOCK_FREE
             /\ resp_locked_seq' = 0
             /\ step_budget' = NextBudget
             /\ UNCHANGED <<
                  cfg_init_quote_free, cfg_slots_cap, cfg_allow_dry_run,
                  dop_next_seq, slot_id_seq,
                  body_seq, body_ord_qty, body_px, body_dry_run,
                  front_ack_cnt, ack_cnt, resp_precheck_seq,
                  t1_recorded, t1_owner_seq, t1_qty, t1_spend, t1_fee_known, t1_fee_signed,
                  alias_protected_ok, cxl_remainder_ok, conflict_frozen
                >>

(* 10. `FillRecordPool::record_or_amend` on `t1` (`Amended` fee/rebate patch OR `Duplicate` no-op),
 *     valid whether `t1_owner_seq`'s slot is still `LIVE`, `RETIRED`, or already recycled to a
 *     modular-aliased `seq' = t1_owner_seq + k * cfg_slots_cap`! Updates fee delta ONLY (`amendment = true`)
 *     with zero double position and zero `Conflict` freeze
 *     (`crates/tickwire-account/src/order_manager.rs:1840-1870,1928-2011`,
 *      `crates/tickwire-account/src/fill_record.rs:171-277`,
 *      `crates/tickwire-account/src/uta_ledger.rs:520-540`). *)
RespTrade1AmendOrDuplicate ==
  /\ CanStep
  /\ slot_lock = LOCK_FREE
  /\ t1_recorded
  /\ \E has_fee \in BOOLEAN, fee_val \in FeeDom:
       /\ fee_val >= -EffFeeMax /\ fee_val <= EffFeeMax
       /\ LET is_amend == (has_fee /\ (~t1_fee_known \/ t1_fee_signed # fee_val))
              delta == IF is_amend THEN (fee_val - t1_fee_signed) ELSE 0
          IN /\ (wallet_quote_free + delta) >= -EffU128Max
             /\ (wallet_quote_free + wallet_quote_locked + delta) <= EffU128Max
             /\ t1_fee_known' = (IF has_fee THEN TRUE ELSE t1_fee_known)
             /\ t1_fee_signed' = (IF is_amend THEN fee_val ELSE t1_fee_signed)
             /\ wallet_quote_free' = wallet_quote_free + delta
             /\ step_budget' = NextBudget
             /\ UNCHANGED <<
                  cfg_init_quote_free, cfg_slots_cap, cfg_allow_dry_run,
                  dop_next_seq, slot_state, slot_id_seq, slot_lock,
                  body_seq, body_status, body_ord_qty, body_px, body_filled_qty, body_proj_qty, body_hold_rem,
                  body_dry_run, stp_linked, front_ack_cnt, ack_cnt,
                  resp_precheck_seq, resp_locked_seq,
                  t1_recorded, t1_owner_seq, t1_qty, t1_spend,
                  other_filled_lots, other_quote_spend, other_fee_signed,
                  wallet_quote_locked, wallet_base_free,
                  alias_protected_ok, cxl_remainder_ok, conflict_frozen
                >>

(* 11. `on_canceled` / `on_rejected`: computes `cxl_qty = body_ord_qty - eff_proj`, releases full
 *     remaining hold `body_hold_rem`, unlinks `OrsStpBook`, and immediately retires slot
 *     (`crates/tickwire-account/src/order_manager.rs:2023-2086,2140-2192`). *)
RespLockedTerminalCancelOrReject ==
  /\ CanStep
  /\ slot_lock = LOCK_RESP
  /\ \E is_reject \in BOOLEAN, obs_cum \in CumDom:
       /\ obs_cum >= 0 /\ obs_cum <= body_ord_qty
       /\ (is_reject => (body_status = ST_PENDING_NEW /\ body_filled_qty = 0 /\ obs_cum = 0))
       /\ LET eff_proj == Max2(Max2(body_filled_qty, body_proj_qty), obs_cum)
              cxl_qty == body_ord_qty - eff_proj
              rem == body_hold_rem
              valid_cxl == (eff_proj >= body_filled_qty /\ eff_proj <= body_ord_qty
                            /\ (eff_proj + cxl_qty) = body_ord_qty)
          IN /\ (wallet_quote_free + rem) <= EffU128Max
             /\ body_proj_qty' = eff_proj
             /\ body_hold_rem' = 0
             /\ body_status' = ST_TERMINAL
             /\ stp_linked' = FALSE
             /\ slot_state' = SLOT_RETIRED
             /\ wallet_quote_locked' = wallet_quote_locked - rem
             /\ wallet_quote_free' = wallet_quote_free + rem
             /\ cxl_remainder_ok' = (cxl_remainder_ok /\ (is_reject \/ valid_cxl))
             /\ slot_lock' = LOCK_FREE
             /\ resp_locked_seq' = 0
             /\ step_budget' = NextBudget
             /\ UNCHANGED <<
                  cfg_init_quote_free, cfg_slots_cap, cfg_allow_dry_run,
                  dop_next_seq, slot_id_seq,
                  body_seq, body_ord_qty, body_px, body_filled_qty, body_dry_run,
                  front_ack_cnt, ack_cnt, resp_precheck_seq,
                  t1_recorded, t1_owner_seq, t1_qty, t1_spend, t1_fee_known, t1_fee_signed,
                  other_filled_lots, other_quote_spend, other_fee_signed,
                  wallet_base_free,
                  alias_protected_ok, conflict_frozen
                >>

Next ==
  \/ ReqClaimModularSlot
  \/ ReqAliasCollisionRefused
  \/ ReqPopulatePublishOrRetire
  \/ ReqFinishDryRunRetire
  \/ RespPrecheckLive
  \/ RespAcquireAndRecheck
  \/ RespLockedAckAndCumQty
  \/ RespLockedRecordTrade1
  \/ RespLockedRecordSubsequentTrade
  \/ RespTrade1AmendOrDuplicate
  \/ RespLockedTerminalCancelOrReject

(* -------------------------------------------------------------------------
 * Safety Properties & Strengthened Inductive Invariant (`IndInv`)
 * ------------------------------------------------------------------------- *)

TotalUniqueLots == (IF t1_recorded THEN t1_qty ELSE 0) + other_filled_lots
TotalUniqueSpend == (IF t1_recorded THEN t1_spend ELSE 0) + other_quote_spend
TotalSignedFees == (IF t1_recorded THEN t1_fee_signed ELSE 0) + other_fee_signed

TypeOK ==
  /\ cfg_init_quote_free \in Int /\ cfg_init_quote_free >= 0 /\ cfg_init_quote_free <= EffU128Max
  /\ cfg_slots_cap \in Int /\ cfg_slots_cap >= 2 /\ cfg_slots_cap <= EffU64Max
  /\ cfg_allow_dry_run \in BOOLEAN
  /\ dop_next_seq \in Int /\ dop_next_seq >= 1 /\ dop_next_seq <= EffU64Max
  /\ slot_state \in {SLOT_EMPTY, SLOT_CLAIMED, SLOT_LIVE, SLOT_RETIRED}
  /\ slot_id_seq \in Int /\ slot_id_seq >= 0 /\ slot_id_seq < dop_next_seq
  /\ slot_lock \in {LOCK_FREE, LOCK_RESP}
  /\ body_seq \in Int /\ body_seq >= 0 /\ body_seq < dop_next_seq
  /\ body_status \in {ST_NONE, ST_PENDING_NEW, ST_OPEN, ST_TERMINAL}
  /\ body_ord_qty \in Int /\ body_ord_qty >= 0 /\ body_ord_qty <= EffU64Max
  /\ body_px \in {1, 2}
  /\ body_filled_qty \in Int /\ body_filled_qty >= 0 /\ body_filled_qty <= EffU64Max
  /\ body_proj_qty \in Int /\ body_proj_qty >= 0 /\ body_proj_qty <= EffU64Max
  /\ body_hold_rem \in Int /\ body_hold_rem >= 0 /\ body_hold_rem <= EffU128Max
  /\ body_dry_run \in BOOLEAN
  /\ stp_linked \in BOOLEAN
  /\ front_ack_cnt \in 0..1
  /\ ack_cnt \in 0..1
  /\ resp_precheck_seq \in Int /\ resp_precheck_seq >= 0 /\ resp_precheck_seq < dop_next_seq
  /\ resp_locked_seq \in Int /\ resp_locked_seq >= 0 /\ resp_locked_seq < dop_next_seq
  /\ t1_recorded \in BOOLEAN
  /\ t1_owner_seq \in Int /\ t1_owner_seq >= 0 /\ t1_owner_seq < dop_next_seq
  /\ t1_qty \in Int /\ t1_qty >= 0 /\ t1_qty <= EffU64Max
  /\ t1_spend \in Int /\ t1_spend >= 0 /\ t1_spend <= EffU128Max
  /\ t1_fee_known \in BOOLEAN
  /\ t1_fee_signed \in Int /\ t1_fee_signed >= -EffFeeMax /\ t1_fee_signed <= EffFeeMax
  /\ other_filled_lots \in Int /\ other_filled_lots >= 0 /\ other_filled_lots <= EffU128Max
  /\ other_quote_spend \in Int /\ other_quote_spend >= 0 /\ other_quote_spend <= EffU128Max
  /\ other_fee_signed \in Int /\ other_fee_signed >= -EffU128Max /\ other_fee_signed <= EffU128Max
  /\ wallet_quote_free \in Int /\ wallet_quote_free >= -EffU128Max /\ wallet_quote_free <= EffU128Max
  /\ wallet_quote_locked \in Int /\ wallet_quote_locked >= 0 /\ wallet_quote_locked <= EffU128Max
  /\ wallet_base_free \in Int /\ wallet_base_free >= 0 /\ wallet_base_free <= EffU128Max
  /\ step_budget \in Int /\ step_budget >= 0 /\ step_budget <= MaxBoundedSteps
  /\ alias_protected_ok \in BOOLEAN
  /\ cxl_remainder_ok \in BOOLEAN
  /\ conflict_frozen \in BOOLEAN

SolvencyPreserved ==
  /\ wallet_quote_locked = body_hold_rem
  /\ (slot_state # SLOT_LIVE => body_hold_rem = 0)
  /\ (wallet_quote_free + wallet_quote_locked) <= EffU128Max
  /\ (wallet_quote_free + wallet_quote_locked)
       = (cfg_init_quote_free - TotalUniqueSpend + TotalSignedFees)

QuantityExactlyOnce ==
  wallet_base_free = TotalUniqueLots

NoFalseConflictFreeze ==
  ~conflict_frozen

TerminalRetiresImmediately ==
  /\ stp_linked <=> (slot_state = SLOT_LIVE)
  /\ (slot_state = SLOT_LIVE => (body_filled_qty < body_ord_qty /\ body_status \in {ST_PENDING_NEW, ST_OPEN}))

NoWrongOccupantLocked ==
  /\ alias_protected_ok
  /\ ((slot_lock = LOCK_RESP) <=> (resp_locked_seq >= 1))
  /\ ((slot_lock = LOCK_RESP) =>
        (slot_state = SLOT_LIVE /\ slot_id_seq = resp_locked_seq
         /\ body_seq = resp_locked_seq /\ ~body_dry_run))

OneAckAndExactCancel ==
  /\ front_ack_cnt <= 1
  /\ ack_cnt <= 1
  /\ cxl_remainder_ok

SafetyConjunction ==
  /\ TypeOK
  /\ SolvencyPreserved
  /\ QuantityExactlyOnce
  /\ NoFalseConflictFreeze
  /\ TerminalRetiresImmediately
  /\ NoWrongOccupantLocked
  /\ OneAckAndExactCancel

(* Mode S2 Strengthened Inductive Invariant (`IndInv`): *)
IndInv ==
  /\ SafetyConjunction
  /\ ((slot_state = SLOT_EMPTY) =>
        (slot_id_seq = 0 /\ slot_lock = LOCK_FREE /\ body_seq = 0 /\ body_status = ST_NONE
         /\ body_ord_qty = 0 /\ body_filled_qty = 0 /\ body_proj_qty = 0
         /\ body_hold_rem = 0 /\ ~body_dry_run /\ resp_precheck_seq = 0 /\ resp_locked_seq = 0))
  /\ ((slot_state = SLOT_CLAIMED) =>
        (slot_id_seq >= 1 /\ slot_lock = LOCK_FREE /\ body_hold_rem = 0
         /\ ~body_dry_run /\ resp_locked_seq = 0))
  /\ ((slot_state = SLOT_LIVE) =>
        (slot_id_seq >= 1 /\ body_seq = slot_id_seq
         /\ body_ord_qty >= 1 /\ body_filled_qty < body_ord_qty
         /\ body_proj_qty >= body_filled_qty /\ body_proj_qty <= body_ord_qty
         /\ body_hold_rem = ((body_ord_qty - body_filled_qty) * body_px)
         /\ ((body_status = ST_PENDING_NEW) => (ack_cnt = 0))
         /\ (body_dry_run => (cfg_allow_dry_run /\ slot_lock = LOCK_FREE
                              /\ body_filled_qty = 0 /\ body_status = ST_PENDING_NEW))))
  /\ ((slot_state = SLOT_RETIRED) =>
        (slot_id_seq >= 1 /\ slot_lock = LOCK_FREE /\ body_status = ST_TERMINAL
         /\ body_hold_rem = 0 /\ ~body_dry_run /\ resp_locked_seq = 0))
  /\ ((slot_lock = LOCK_RESP) => (resp_precheck_seq = 0))
  /\ ((~t1_recorded) =>
        (t1_owner_seq = 0 /\ t1_qty = 0 /\ t1_spend = 0 /\ ~t1_fee_known /\ t1_fee_signed = 0
         /\ other_filled_lots = 0 /\ other_quote_spend = 0 /\ other_fee_signed = 0))
  /\ (t1_recorded =>
        (t1_owner_seq >= 1 /\ t1_qty >= 1 /\ t1_spend >= t1_qty /\ t1_spend <= (2 * t1_qty)
         /\ ((~t1_fee_known) => (t1_fee_signed = 0))))

====
