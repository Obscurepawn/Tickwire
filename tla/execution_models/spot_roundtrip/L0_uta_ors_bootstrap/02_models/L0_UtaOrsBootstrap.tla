---- MODULE L0_UtaOrsBootstrap ----
(*
 * Stateless Cold Exchange-REST Recovery & Unified 6-Product UTA Roundtrip Model
 * (`BybitUtaSession` / `OkxUtaSession` in `tickwire-spot-execution`, `tickwire-account`, `tickwire-venue`):
 *   1. Cold startup and stateless process restart via `UtaBootstrapDecoder`
 *      (`Account + Positions + OpenOrders` REST pages, `balances_include_open_order_locks = true`,
 *      zero local disk/WAL files) and 32-char `[0-9a-z]` `tw1...` `SelfDescribingClientId`
 *      multi-product domain-MAC attribution:
 *        crates/tickwire-spot-execution/src/rest_bootstrap.rs:191-212
 *        crates/tickwire-spot-execution/src/rest_bootstrap.rs:358-854
 *        crates/tickwire-venue/src/client_id.rs:71-185
 *        crates/tickwire-account/src/order_manager.rs:2663-2802
 *   2. Unified 6-product connection pool (`product_mask = 0` matching `Spot`,
 *      `LinearPerpetual`, `InversePerpetual`, `LinearFuture`, `InverseFuture`, `Option`),
 *      atomic `claimed_inflight < max_inflight` route claiming (`WebSocketTrade` vs
 *      `PooledHttps`), and graceful server maintenance `notice` drain (`draining = true` ->
 *      `ready = false` while draining in-flight replies):
 *        crates/tickwire-spot-execution/src/session.rs:904-958
 *        crates/tickwire-spot-execution/src/session.rs:1051-1295
 *        crates/tickwire-spot-execution/src/session.rs:3354-3384
 *   3. Multi-wallet `UtaWalletLedger` (`AssetKey { wallet_id, asset_id }`) shared across
 *      Spot base/quote and Derivatives margin collateral, plus `execution.fast` ->
 *      full `execution` (`amendment = true`) trade-ID deduplication & fee/rebate patching:
 *        crates/tickwire-spot-execution/src/session.rs:2854-2927
 *        crates/tickwire-spot-execution/src/session.rs:3076-3292
 *        crates/tickwire-account/src/uta_ledger.rs:359-540
 *        crates/tickwire-account/src/fill_record.rs:171-277
 *)
EXTENDS Integers, FiniteSets

CONSTANTS
  MaxInflightPerConn,
  InitCollatFree,
  InitCollatLocked,
  InitSpotBaseFree,
  InitPerpPos

Conns == {0, 1}
ManagedSeqs == {1, 2}
Trades == {1, 2}

(* `seq = 1`: pre-existing `tw1...` Spot Buy (`orig_qty = 2`, `cum_filled = 1`, remaining hold = 1 in `W_UTA_COLLAT`).
 * `seq = 2`: live post-bootstrap Derivatives (`LinearPerpetual` / `Option`) Buy (`orig_qty = 1`, hold = 2 in `W_UTA_COLLAT`). *)
OrigQty(seq) == IF seq = 1 THEN 2 ELSE 1
ProductKind(seq) == IF seq = 1 THEN "SPOT" ELSE "LINEAR_PERP"
ExternalUtaFrozen == 1

(* Trade 1 executes remaining 1 lot of Spot `seq = 1` (spend = 1 quote, +1 spot base);
 * Trade 2 executes 1 lot of LinearPerp `seq = 2` (spend = 0 principal on margin, +1 perp pos lot, release hold = 2). *)
TradeOwner(tid) == tid
TradeLots(tid) == 1
TradeQuoteSpend(tid) == IF tid = 1 THEN 1 ELSE 0

VARIABLES
  bootstrapped,
  restarted_once,
  dop_next_seq,
  external_ignored_cnt,

  (* Unified `product_mask = 0` connection pool (`Conns = {0, 1}`) *)
  conn_draining,
  conn_claimed,
  conn_inflight_seq,

  (* Per-order `OrderManager` state (`ManagedSeqs = {1, 2}`) *)
  slot_state,
  ord_status,
  ord_filled,
  ord_hold_rem,
  stp_linked,
  front_ack_cnt,
  ack_cnt,

  (* `FillRecordPool` state (`Trades = {1, 2}`) *)
  exec_applied,
  exec_fee_known,
  exec_fee_signed,
  exec_amend_cnt,

  (* Multi-wallet `UtaWalletLedger` (`W_UTA_COLLAT`, `W_UTA_SPOT_BASE`) & positions *)
  collat_free,
  collat_locked,
  spot_base_free,
  perp_pos_lots,
  settled_spot_spend,
  settled_collat_fees,
  conflict_frozen

vars == <<
  bootstrapped, restarted_once, dop_next_seq, external_ignored_cnt,
  conn_draining, conn_claimed, conn_inflight_seq,
  slot_state, ord_status, ord_filled, ord_hold_rem, stp_linked, front_ack_cnt, ack_cnt,
  exec_applied, exec_fee_known, exec_fee_signed, exec_amend_cnt,
  collat_free, collat_locked, spot_base_free, perp_pos_lots,
  settled_spot_spend, settled_collat_fees, conflict_frozen
>>

ConnReady(c) == (~conn_draining[c]) /\ (conn_claimed[c] < MaxInflightPerConn)

ActiveUtaHold ==
  (IF slot_state[1] = "LIVE" THEN ord_hold_rem[1] ELSE 0)
  + (IF slot_state[2] = "LIVE" THEN ord_hold_rem[2] ELSE 0)

Init ==
  /\ bootstrapped = FALSE
  /\ restarted_once = FALSE
  /\ dop_next_seq = 1
  /\ external_ignored_cnt = 0
  /\ conn_draining = [c \in Conns |-> FALSE]
  /\ conn_claimed = [c \in Conns |-> 0]
  /\ conn_inflight_seq = [c \in Conns |-> 0]
  /\ slot_state = [q \in ManagedSeqs |-> "EMPTY"]
  /\ ord_status = [q \in ManagedSeqs |-> "NONE"]
  /\ ord_filled = [q \in ManagedSeqs |-> 0]
  /\ ord_hold_rem = [q \in ManagedSeqs |-> 0]
  /\ stp_linked = [q \in ManagedSeqs |-> FALSE]
  /\ front_ack_cnt = [q \in ManagedSeqs |-> 0]
  /\ ack_cnt = [q \in ManagedSeqs |-> 0]
  /\ exec_applied = [t \in Trades |-> FALSE]
  /\ exec_fee_known = [t \in Trades |-> FALSE]
  /\ exec_fee_signed = [t \in Trades |-> 0]
  /\ exec_amend_cnt = [t \in Trades |-> 0]
  /\ collat_free = 0
  /\ collat_locked = 0
  /\ spot_base_free = 0
  /\ perp_pos_lots = 0
  /\ settled_spot_spend = 0
  /\ settled_collat_fees = 0
  /\ conflict_frozen = FALSE

(* 1. Cold REST `Account + Positions + OpenOrders` bootstrap via `UtaBootstrapDecoder::bootstrap_order_manager`
 *    (`crates/tickwire-spot-execution/src/rest_bootstrap.rs:845-854`,
 *     `crates/tickwire-account/src/order_manager.rs:2663-2802`):
 *    - Seeds `W_UTA_COLLAT` (`InitCollatFree`, `InitCollatLocked` including `ExternalUtaFrozen = 1` + `seq = 1` hold `1`)
 *      and `W_UTA_SPOT_BASE` (`InitSpotBaseFree`), plus `InitPerpPos`.
 *    - Decodes `tw1...` `seq = 1` -> `LIVE` (`OPEN`), `dop_next_seq = 2`, ignoring external manual open order. *)
ColdUtaRestBootstrap ==
  /\ ~bootstrapped
  /\ InitCollatLocked >= (1 + ExternalUtaFrozen)
  /\ bootstrapped' = TRUE
  /\ dop_next_seq' = 2
  /\ external_ignored_cnt' = 1
  /\ collat_free' = InitCollatFree
  /\ collat_locked' = InitCollatLocked
  /\ spot_base_free' = InitSpotBaseFree
  /\ perp_pos_lots' = InitPerpPos
  /\ slot_state' = [slot_state EXCEPT ![1] = "LIVE"]
  /\ ord_status' = [ord_status EXCEPT ![1] = "OPEN"]
  /\ ord_filled' = [ord_filled EXCEPT ![1] = 1]
  /\ ord_hold_rem' = [ord_hold_rem EXCEPT ![1] = 1]
  /\ stp_linked' = [stp_linked EXCEPT ![1] = TRUE]
  /\ UNCHANGED <<
       restarted_once, conn_draining, conn_claimed, conn_inflight_seq,
       front_ack_cnt, ack_cnt,
       exec_applied, exec_fee_known, exec_fee_signed, exec_amend_cnt,
       settled_spot_spend, settled_collat_fees, conflict_frozen
     >>

(* 2. Graceful server maintenance `notice` on connection `0` (`initiate_graceful_maintenance_drain`,
 *    `crates/tickwire-spot-execution/src/session.rs:3354-3384`): sets `conn_draining[0] = TRUE` so
 *    `PublishedRoutes` marks `ready = FALSE` for new `REQ` traffic while in-flight reply drains! *)
UtaMaintenanceDrainConn0 ==
  /\ bootstrapped
  /\ ~conn_draining[0]
  /\ conn_draining' = [conn_draining EXCEPT ![0] = TRUE]
  /\ UNCHANGED <<
       bootstrapped, restarted_once, dop_next_seq, external_ignored_cnt,
       conn_claimed, conn_inflight_seq,
       slot_state, ord_status, ord_filled, ord_hold_rem, stp_linked, front_ack_cnt, ack_cnt,
       exec_applied, exec_fee_known, exec_fee_signed, exec_amend_cnt,
       collat_free, collat_locked, spot_base_free, perp_pos_lots,
       settled_spot_spend, settled_collat_fees, conflict_frozen
     >>

(* 3. Unified `product_mask = 0` `submit_new_order` for Derivatives `seq = 2` on ready connection `c`
 *    (`crates/tickwire-spot-execution/src/session.rs:904-958,1103-1267`):
 *    reserves `hold = 2` in shared `W_UTA_COLLAT` and claims 1 in-flight slot on `c`. *)
UtaSubmitMultiProductOrder(c) ==
  /\ bootstrapped
  /\ dop_next_seq = 2
  /\ slot_state[2] = "EMPTY"
  /\ ConnReady(c)
  /\ conn_inflight_seq[c] = 0
  /\ collat_free >= 2
  /\ dop_next_seq' = 3
  /\ collat_free' = collat_free - 2
  /\ collat_locked' = collat_locked + 2
  /\ slot_state' = [slot_state EXCEPT ![2] = "LIVE"]
  /\ ord_status' = [ord_status EXCEPT ![2] = "PENDING_NEW"]
  /\ ord_filled' = [ord_filled EXCEPT ![2] = 0]
  /\ ord_hold_rem' = [ord_hold_rem EXCEPT ![2] = 2]
  /\ stp_linked' = [stp_linked EXCEPT ![2] = TRUE]
  /\ conn_claimed' = [conn_claimed EXCEPT ![c] = conn_claimed[c] + 1]
  /\ conn_inflight_seq' = [conn_inflight_seq EXCEPT ![c] = 2]
  /\ UNCHANGED <<
       bootstrapped, restarted_once, external_ignored_cnt, conn_draining,
       front_ack_cnt, ack_cnt,
       exec_applied, exec_fee_known, exec_fee_signed, exec_amend_cnt,
       spot_base_free, perp_pos_lots,
       settled_spot_spend, settled_collat_fees, conflict_frozen
     >>

(* 4. `apply_command_reply_with_tracked` (`FrontAck`, `crates/tickwire-spot-execution/src/session.rs:2854-2927`):
 *    releases `claimed_inflight` on `c` (even if `c` entered `draining` after send!) and emits single `FrontAck`. *)
UtaCommandReplyFrontAck(c) ==
  /\ bootstrapped
  /\ conn_inflight_seq[c] = 2
  /\ conn_claimed' = [conn_claimed EXCEPT ![c] = conn_claimed[c] - 1]
  /\ conn_inflight_seq' = [conn_inflight_seq EXCEPT ![c] = 0]
  /\ front_ack_cnt' = [front_ack_cnt EXCEPT ![2] = IF slot_state[2] = "LIVE" /\ front_ack_cnt[2] = 0 THEN 1 ELSE front_ack_cnt[2]]
  /\ UNCHANGED <<
       bootstrapped, restarted_once, dop_next_seq, external_ignored_cnt, conn_draining,
       slot_state, ord_status, ord_filled, ord_hold_rem, stp_linked, ack_cnt,
       exec_applied, exec_fee_known, exec_fee_signed, exec_amend_cnt,
       collat_free, collat_locked, spot_base_free, perp_pos_lots,
       settled_spot_spend, settled_collat_fees, conflict_frozen
     >>

(* 5. Private WS `OrderRow` (`New` / `PartiallyFilled` -> `One ack`, `crates/tickwire-spot-execution/src/session.rs:3128-3155`): *)
UtaPrivateWsOrderAck(seq) ==
  /\ bootstrapped
  /\ slot_state[seq] = "LIVE"
  /\ LET was_pending == (ord_status[seq] = "PENDING_NEW")
     IN /\ ord_status' = [ord_status EXCEPT ![seq] = "OPEN"]
        /\ front_ack_cnt' = [front_ack_cnt EXCEPT ![seq] = IF front_ack_cnt[seq] = 0 THEN 1 ELSE front_ack_cnt[seq]]
        /\ ack_cnt' = [ack_cnt EXCEPT ![seq] = IF was_pending THEN ack_cnt[seq] + 1 ELSE ack_cnt[seq]]
        /\ UNCHANGED <<
             bootstrapped, restarted_once, dop_next_seq, external_ignored_cnt,
             conn_draining, conn_claimed, conn_inflight_seq,
             slot_state, ord_filled, ord_hold_rem, stp_linked,
             exec_applied, exec_fee_known, exec_fee_signed, exec_amend_cnt,
             collat_free, collat_locked, spot_base_free, perp_pos_lots,
             settled_spot_spend, settled_collat_fees, conflict_frozen
           >>

(* 6. Private WS `ExecRow` (`execution.fast` with `is_fast = TRUE` -> `fee = None`, or full `execution`
 *    with `is_fast = FALSE` -> `fee = Some`, `crates/tickwire-spot-execution/src/session.rs:3222-3292`):
 *    - `tid = 1` (Spot Buy `seq = 1`): releases hold `1`, spends `1` from `W_UTA_COLLAT`, credits `+1` to `W_UTA_SPOT_BASE`.
 *    - `tid = 2` (LinearPerp Buy `seq = 2`): releases margin hold `2` back to `W_UTA_COLLAT`, credits `+1` to `perp_pos_lots`.
 *    - Immediately retires slot and unlinks `OrsStpBook` without waiting for full `execution` fee! *)
UtaPrivateWsExecFirstArrival(tid, is_fast, fee_val) ==
  /\ bootstrapped
  /\ ~exec_applied[tid]
  /\ LET seq == TradeOwner(tid)
         lots == TradeLots(tid)
         q_spend == TradeQuoteSpend(tid)
         eff_fee == IF is_fast THEN 0 ELSE fee_val
         rel == ord_hold_rem[seq]
         is_spot == (ProductKind(seq) = "SPOT")
     IN /\ slot_state[seq] = "LIVE"
        /\ exec_applied' = [exec_applied EXCEPT ![tid] = TRUE]
        /\ exec_fee_known' = [exec_fee_known EXCEPT ![tid] = ~is_fast]
        /\ exec_fee_signed' = [exec_fee_signed EXCEPT ![tid] = eff_fee]
        /\ ord_filled' = [ord_filled EXCEPT ![seq] = ord_filled[seq] + lots]
        /\ ord_hold_rem' = [ord_hold_rem EXCEPT ![seq] = 0]
        /\ ord_status' = [ord_status EXCEPT ![seq] = "TERMINAL"]
        /\ stp_linked' = [stp_linked EXCEPT ![seq] = FALSE]
        /\ slot_state' = [slot_state EXCEPT ![seq] = "RETIRED"]
        /\ collat_locked' = collat_locked - rel
        /\ collat_free' = collat_free + rel - q_spend + eff_fee
        /\ spot_base_free' = spot_base_free + (IF is_spot THEN lots ELSE 0)
        /\ perp_pos_lots' = perp_pos_lots + (IF is_spot THEN 0 ELSE lots)
        /\ settled_spot_spend' = settled_spot_spend + q_spend
        /\ settled_collat_fees' = settled_collat_fees + eff_fee
        /\ UNCHANGED <<
             bootstrapped, restarted_once, dop_next_seq, external_ignored_cnt,
             conn_draining, conn_claimed, conn_inflight_seq,
             front_ack_cnt, ack_cnt, exec_amend_cnt, conflict_frozen
           >>

(* 7. Full `execution` arriving after `execution.fast` (`amendment = true` fee/rebate patch via
 *    `FillRecordPool::record_or_amend`) or duplicate `exec_id` suppression (`crates/tickwire-spot-execution/src/session.rs:3266-3292`). *)
UtaPrivateWsExecAmendOrDup(tid, has_fee, fee_val) ==
  /\ bootstrapped
  /\ exec_applied[tid]
  /\ LET is_amend == (has_fee /\ (~exec_fee_known[tid] \/ exec_fee_signed[tid] # fee_val))
         delta == IF is_amend THEN (fee_val - exec_fee_signed[tid]) ELSE 0
     IN /\ (~is_amend \/ exec_amend_cnt[tid] < 1)
        /\ exec_fee_known' = [exec_fee_known EXCEPT ![tid] = IF has_fee THEN TRUE ELSE exec_fee_known[tid]]
        /\ exec_fee_signed' = [exec_fee_signed EXCEPT ![tid] = IF is_amend THEN fee_val ELSE exec_fee_signed[tid]]
        /\ exec_amend_cnt' = [exec_amend_cnt EXCEPT ![tid] = IF is_amend THEN exec_amend_cnt[tid] + 1 ELSE exec_amend_cnt[tid]]
        /\ collat_free' = collat_free + delta
        /\ settled_collat_fees' = settled_collat_fees + delta
        /\ UNCHANGED <<
             bootstrapped, restarted_once, dop_next_seq, external_ignored_cnt,
             conn_draining, conn_claimed, conn_inflight_seq,
             slot_state, ord_status, ord_filled, ord_hold_rem, stp_linked, front_ack_cnt, ack_cnt,
             exec_applied, collat_locked, spot_base_free, perp_pos_lots,
             settled_spot_spend, conflict_frozen
           >>

(* 8. Private WS `OrderRow` `Canceled` (`crates/tickwire-spot-execution/src/session.rs:3175-3194`):
 *    releases remaining `W_UTA_COLLAT` hold, unlinks `OrsStpBook`, and immediately retires slot. *)
UtaPrivateWsOrderCanceled(seq) ==
  /\ bootstrapped
  /\ slot_state[seq] = "LIVE"
  /\ LET rem == ord_hold_rem[seq]
     IN /\ slot_state' = [slot_state EXCEPT ![seq] = "RETIRED"]
        /\ ord_status' = [ord_status EXCEPT ![seq] = "TERMINAL"]
        /\ ord_hold_rem' = [ord_hold_rem EXCEPT ![seq] = 0]
        /\ stp_linked' = [stp_linked EXCEPT ![seq] = FALSE]
        /\ collat_locked' = collat_locked - rem
        /\ collat_free' = collat_free + rem
        /\ UNCHANGED <<
             bootstrapped, restarted_once, dop_next_seq, external_ignored_cnt,
             conn_draining, conn_claimed, conn_inflight_seq,
             ord_filled, front_ack_cnt, ack_cnt,
             exec_applied, exec_fee_known, exec_fee_signed, exec_amend_cnt,
             spot_base_free, perp_pos_lots,
             settled_spot_spend, settled_collat_fees, conflict_frozen
           >>

(* 9. Stateless ORS cold restart (`restarted_once = FALSE -> TRUE`): resets connection pool and
 *    reconstructs `OrderManager`, `UtaWalletLedger`, `OrsStpBook`, and `DopState` floor exclusively
 *    from current Exchange REST `Account + Positions + OpenOrders` truth with zero local disk state! *)
UtaStatelessColdRestart ==
  /\ bootstrapped
  /\ ~restarted_once
  /\ conn_claimed = [c \in Conns |-> 0]
  /\ LET live_max_seq == IF slot_state[2] = "LIVE" THEN 2 ELSE IF slot_state[1] = "LIVE" THEN 1 ELSE 0
         rec_floor == IF live_max_seq >= 1 THEN (live_max_seq + 1) ELSE 1
     IN /\ restarted_once' = TRUE
        /\ conn_draining' = [c \in Conns |-> FALSE]
        /\ dop_next_seq' = (IF dop_next_seq >= rec_floor THEN dop_next_seq ELSE rec_floor)
        /\ ord_status' = [q \in ManagedSeqs |-> IF slot_state[q] = "LIVE" THEN "OPEN" ELSE ord_status[q]]
        /\ ack_cnt' = [q \in ManagedSeqs |-> IF slot_state[q] = "LIVE" THEN 0 ELSE ack_cnt[q]]
        /\ UNCHANGED <<
             bootstrapped, external_ignored_cnt, conn_claimed, conn_inflight_seq,
             slot_state, ord_filled, ord_hold_rem, stp_linked, front_ack_cnt,
             exec_applied, exec_fee_known, exec_fee_signed, exec_amend_cnt,
             collat_free, collat_locked, spot_base_free, perp_pos_lots,
             settled_spot_spend, settled_collat_fees, conflict_frozen
           >>

Next ==
  \/ ColdUtaRestBootstrap
  \/ UtaMaintenanceDrainConn0
  \/ \E c \in Conns: UtaSubmitMultiProductOrder(c)
  \/ \E c \in Conns: UtaCommandReplyFrontAck(c)
  \/ \E seq \in ManagedSeqs: UtaPrivateWsOrderAck(seq)
  \/ \E tid \in Trades, fast \in BOOLEAN, fv \in {-1, 1}: UtaPrivateWsExecFirstArrival(tid, fast, fv)
  \/ \E tid \in Trades, hf \in BOOLEAN, fv \in {-1, 1}: UtaPrivateWsExecAmendOrDup(tid, hf, fv)
  \/ \E seq \in ManagedSeqs: UtaPrivateWsOrderCanceled(seq)
  \/ UtaStatelessColdRestart

(* -------------------------------------------------------------------------
 * Safety Invariants
 * ------------------------------------------------------------------------- *)

TypeOK ==
  /\ bootstrapped \in BOOLEAN
  /\ restarted_once \in BOOLEAN
  /\ dop_next_seq \in 1..3
  /\ external_ignored_cnt \in {0, 1}
  /\ conn_draining \in [Conns -> BOOLEAN]
  /\ conn_claimed \in [Conns -> 0..MaxInflightPerConn]
  /\ conn_inflight_seq \in [Conns -> {0, 2}]
  /\ slot_state \in [ManagedSeqs -> {"EMPTY", "LIVE", "RETIRED"}]
  /\ ord_status \in [ManagedSeqs -> {"NONE", "PENDING_NEW", "OPEN", "TERMINAL"}]
  /\ stp_linked \in [ManagedSeqs -> BOOLEAN]
  /\ front_ack_cnt \in [ManagedSeqs -> 0..1]
  /\ ack_cnt \in [ManagedSeqs -> 0..1]
  /\ exec_applied \in [Trades -> BOOLEAN]
  /\ exec_fee_known \in [Trades -> BOOLEAN]
  /\ exec_fee_signed \in [Trades -> -1..1]
  /\ exec_amend_cnt \in [Trades -> 0..1]
  /\ conflict_frozen \in BOOLEAN

(* `product_mask = 0` unified connection pool never exceeds `MaxInflightPerConn` (`1` on `PooledHttps`): *)
ProductMaskZeroPoolSound ==
  \A c \in Conns:
    /\ conn_claimed[c] >= 0
    /\ conn_claimed[c] <= MaxInflightPerConn
    /\ ((conn_claimed[c] = 1) <=> (conn_inflight_seq[c] = 2))

(* Multi-wallet `UtaWalletLedger` (`W_UTA_COLLAT` + `W_UTA_SPOT_BASE`) conservation & external-freeze protection: *)
UtaMultiWalletSolvency ==
  bootstrapped =>
    /\ collat_locked = (ActiveUtaHold + ExternalUtaFrozen)
    /\ (collat_free + collat_locked)
         = ((InitCollatFree + InitCollatLocked) - settled_spot_spend + settled_collat_fees)
    /\ spot_base_free = (InitSpotBaseFree + (IF exec_applied[1] THEN 1 ELSE 0))
    /\ \A q \in ManagedSeqs:
         /\ (stp_linked[q] <=> (slot_state[q] = "LIVE"))
         /\ (slot_state[q] # "LIVE" => ord_hold_rem[q] = 0)

(* `execution.fast -> execution` (`amendment = true`) exact position & fee accounting, zero `Conflict` freeze: *)
FastExecAmendmentExact ==
  bootstrapped =>
    /\ perp_pos_lots = (InitPerpPos + (IF exec_applied[2] THEN 1 ELSE 0))
    /\ settled_collat_fees = (exec_fee_signed[1] + exec_fee_signed[2])
    /\ ack_cnt[1] = 0
    /\ ack_cnt[2] <= 1
    /\ ~conflict_frozen

(* Stateless cold recovery / restart preserves sequence floor above all live `tw1...` open orders: *)
ZeroDiskRestartExact ==
  bootstrapped =>
    /\ external_ignored_cnt = 1
    /\ \A q \in ManagedSeqs: (slot_state[q] = "LIVE" => dop_next_seq > q)

====
