---- MODULE L0_BinanceOrsBootstrap ----
(*
 * Stateless Cold Exchange-REST Bootstrap & Live `BinanceSession` Roundtrip Model
 * (`tickwire-binance-account`, `tickwire-binance-execution`, `tickwire-account`, `tickwire-venue`):
 *   1. Cold startup with zero local disk/WAL state from authenticated Exchange REST
 *      `Account + OpenOrders` snapshots (`Spot` `balances_include_open_order_locks = true`
 *      vs `USD-M` / `Coin-M` / `Options` `balances_include_open_order_locks = false`):
 *        crates/tickwire-binance-account/src/bootstrap.rs:114-224
 *        crates/tickwire-binance-account/src/bootstrap.rs:230-670
 *        crates/tickwire-binance-account/src/session.rs:3281-3378
 *        crates/tickwire-binance-execution/src/lib.rs:36-65
 *   2. 32-character `[0-9a-z]` `tw1...` `SelfDescribingClientId` domain-keyed MAC
 *      verification (`risk_domain_id`, `venue`, `environment`, `account`, `product`),
 *      attributing managed strategy open orders while ignoring external/foreign-MAC
 *      open orders without disturbing external exchange locks:
 *        crates/tickwire-venue/src/client_id.rs:71-185
 *        crates/tickwire-venue/src/client_id.rs:201-228
 *        crates/tickwire-account/src/order_manager.rs:2663-2802
 *   3. `UtaWalletLedger::reserve_bootstrap_hold` (no double-deduction when Spot
 *      `locked` already includes open-order holds vs margin reservation on derivatives)
 *      and `DopState::observe_recovered_order` sequence-floor reconstruction:
 *        crates/tickwire-account/src/uta_ledger.rs:359-387
 *        crates/tickwire-account/src/dop_state.rs:230-244
 *   4. Live post-bootstrap `BinanceSession` order placement (`tw1...` inline encode),
 *      `TRADE_LITE` (`fee = None`, implicit `One ack` when arriving before `NEW`) ->
 *      `ORDER_TRADE_UPDATE` / `executionReport` (`amendment = true` fee/rebate patch),
 *      and remainder cancellation:
 *        crates/tickwire-binance-account/src/session.rs:1880-2046
 *        crates/tickwire-binance-account/src/session.rs:2568-2815
 *        crates/tickwire-account/src/order_manager.rs:1806-2086
 *)
EXTENDS Integers, FiniteSets

CONSTANTS
  BalancesIncludeOpenOrderLocks,
  ExchSnapshotFree,
  ExchSnapshotLocked,
  ExchInitPos

ManagedSeqs == {2, 3}
Trades == {1, 2}

(* Recovered `tw1...` open order `seq = 2`: `orig_qty = 3`, `cum_filled = 1`, remaining `2` lots at `px = 1` (hold = 2).
 * Fresh post-bootstrap live order `seq = 3`: `orig_qty = 1` lot at `px = 2` (hold = 2). *)
OrigQty(seq) == IF seq = 2 THEN 3 ELSE 1
OrdPx(seq) == IF seq = 2 THEN 1 ELSE 2

(* External non-`tw1` / wrong-MAC open order lock included in Spot `ExchSnapshotLocked`: *)
ExternalSpotLock == IF BalancesIncludeOpenOrderLocks THEN 1 ELSE 0

(* Trade 1 fills remaining 2 lots of recovered `seq = 2` at `px = 1`; Trade 2 fills 1 lot of live `seq = 3` at `px = 2`. *)
TradeOwner(tid) == IF tid = 1 THEN 2 ELSE 3
TradeLots(tid) == IF tid = 1 THEN 2 ELSE 1
TradeSpend(tid) == IF tid = 1 THEN 2 ELSE 2

VARIABLES
  bootstrapped,
  dop_next_seq,
  external_ignored_cnt,
  bad_mac_attributed,
  stale_seq_refused_seen,

  (* Per-order slot & lifecycle state for `ManagedSeqs = {2, 3}` *)
  slot_state,
  ord_status,
  ord_filled,
  ord_proj,
  ord_hold_rem,
  stp_linked,
  ack_emitted_cnt,
  cxl_emitted,
  cxl_qty_out,

  (* `FillRecordPool` state for `Trades = {1, 2}` *)
  trade_applied,
  trade_fee_known,
  trade_fee_signed,
  trade_amend_cnt,

  (* `UtaWalletLedger` balance & position state *)
  wallet_free,
  wallet_locked,
  net_pos_lots,
  settled_spend,
  settled_fees,
  conflict_frozen

vars == <<
  bootstrapped, dop_next_seq, external_ignored_cnt, bad_mac_attributed, stale_seq_refused_seen,
  slot_state, ord_status, ord_filled, ord_proj, ord_hold_rem, stp_linked, ack_emitted_cnt,
  cxl_emitted, cxl_qty_out,
  trade_applied, trade_fee_known, trade_fee_signed, trade_amend_cnt,
  wallet_free, wallet_locked, net_pos_lots, settled_spend, settled_fees, conflict_frozen
>>

InitialTrueEquity ==
  IF BalancesIncludeOpenOrderLocks
  THEN (ExchSnapshotFree + ExchSnapshotLocked)
  ELSE ExchSnapshotFree

ActiveManagedHold ==
  (IF slot_state[2] = "LIVE" THEN ord_hold_rem[2] ELSE 0)
  + (IF slot_state[3] = "LIVE" THEN ord_hold_rem[3] ELSE 0)

Init ==
  /\ bootstrapped = FALSE
  /\ dop_next_seq = 1
  /\ external_ignored_cnt = 0
  /\ bad_mac_attributed = FALSE
  /\ stale_seq_refused_seen = FALSE
  /\ slot_state = [q \in ManagedSeqs |-> "EMPTY"]
  /\ ord_status = [q \in ManagedSeqs |-> "NONE"]
  /\ ord_filled = [q \in ManagedSeqs |-> 0]
  /\ ord_proj = [q \in ManagedSeqs |-> 0]
  /\ ord_hold_rem = [q \in ManagedSeqs |-> 0]
  /\ stp_linked = [q \in ManagedSeqs |-> FALSE]
  /\ ack_emitted_cnt = [q \in ManagedSeqs |-> 0]
  /\ cxl_emitted = [q \in ManagedSeqs |-> FALSE]
  /\ cxl_qty_out = [q \in ManagedSeqs |-> 0]
  /\ trade_applied = [t \in Trades |-> FALSE]
  /\ trade_fee_known = [t \in Trades |-> FALSE]
  /\ trade_fee_signed = [t \in Trades |-> 0]
  /\ trade_amend_cnt = [t \in Trades |-> 0]
  /\ wallet_free = 0
  /\ wallet_locked = 0
  /\ net_pos_lots = 0
  /\ settled_spend = 0
  /\ settled_fees = 0
  /\ conflict_frozen = FALSE

(* 1. Cold REST `Account + OpenOrders` bootstrap (`bootstrap_market_from_rest` -> `bootstrap_from_exchange`):
 *    - Seeds `(ExchSnapshotFree, ExchSnapshotLocked)` and `ExchInitPos`.
 *    - Evaluates 3 open-order rows:
 *        (a) Valid `tw1...` domain-MAC order `seq = 2` (`orig = 3`, `cum = 1`, `rem_hold = 2`) -> reconstructed `LIVE` (`OPEN`),
 *            `ack_emitted = TRUE` (suppresses duplicate live `Ack`), `dop_next_seq = max(1, 2 + 1) = 3`.
 *        (b) External non-`tw1` manual order -> `DecodedClientId::External` (ignored; Spot lock preserved).
 *        (c) Foreign-product/account `tw1...` with mismatched 30-bit MAC -> `DecodedClientId::External` (ignored).
 *    - `reserve_bootstrap_hold`:
 *        * Spot (`BalancesIncludeOpenOrderLocks = TRUE`): verifies `ExchSnapshotLocked >= 2` without double-subtracting `wallet_free`!
 *        * USD-M/Coin-M/Options (`BalancesIncludeOpenOrderLocks = FALSE`): shifts `2` from `wallet_free` to `wallet_locked`.
 *    (`crates/tickwire-account/src/order_manager.rs:2663-2802`, `crates/tickwire-account/src/uta_ledger.rs:359-387`). *)
ColdBootstrapFromExchangeRest ==
  /\ ~bootstrapped
  /\ LET rec_hold == 2
         free_after == IF BalancesIncludeOpenOrderLocks THEN ExchSnapshotFree ELSE (ExchSnapshotFree - rec_hold)
         lock_after == IF BalancesIncludeOpenOrderLocks THEN ExchSnapshotLocked ELSE (ExchSnapshotLocked + rec_hold)
     IN /\ free_after >= 0
        /\ lock_after >= (rec_hold + ExternalSpotLock)
        /\ bootstrapped' = TRUE
        /\ dop_next_seq' = 3
        /\ external_ignored_cnt' = 2
        /\ bad_mac_attributed' = FALSE
        /\ wallet_free' = free_after
        /\ wallet_locked' = lock_after
        /\ net_pos_lots' = ExchInitPos
        /\ slot_state' = [slot_state EXCEPT ![2] = "LIVE"]
        /\ ord_status' = [ord_status EXCEPT ![2] = "OPEN"]
        /\ ord_filled' = [ord_filled EXCEPT ![2] = 1]
        /\ ord_proj' = [ord_proj EXCEPT ![2] = 1]
        /\ ord_hold_rem' = [ord_hold_rem EXCEPT ![2] = rec_hold]
        /\ stp_linked' = [stp_linked EXCEPT ![2] = TRUE]
        /\ UNCHANGED <<
             stale_seq_refused_seen, ack_emitted_cnt, cxl_emitted, cxl_qty_out,
             trade_applied, trade_fee_known, trade_fee_signed, trade_amend_cnt,
             settled_spend, settled_fees, conflict_frozen
           >>

(* 2. Post-bootstrap `DopState::admit_new_order_request` deterministically refuses stale `seq <= 2`
 *    (`DUPLICATE_ORDER_SEQNUM`, `crates/tickwire-account/src/dop_state.rs:193-244`). *)
LiveRefuseBelowRecoveredFloor ==
  /\ bootstrapped
  /\ ~stale_seq_refused_seen
  /\ dop_next_seq >= 3
  /\ stale_seq_refused_seen' = TRUE
  /\ UNCHANGED <<
       bootstrapped, dop_next_seq, external_ignored_cnt, bad_mac_attributed,
       slot_state, ord_status, ord_filled, ord_proj, ord_hold_rem, stp_linked, ack_emitted_cnt,
       cxl_emitted, cxl_qty_out,
       trade_applied, trade_fee_known, trade_fee_signed, trade_amend_cnt,
       wallet_free, wallet_locked, net_pos_lots, settled_spend, settled_fees, conflict_frozen
     >>

(* 3. Live `BinanceSession::submit_new_order` for `seq = 3` (`tw1...` inline client ID, `hold = 2`,
 *    `crates/tickwire-binance-account/src/session.rs:1880-2046`, `crates/tickwire-account/src/order_manager.rs:1171-1308`). *)
LiveSubmitNewOrderSeq3 ==
  /\ bootstrapped
  /\ dop_next_seq = 3
  /\ slot_state[3] = "EMPTY"
  /\ LET hold == OrigQty(3) * OrdPx(3)
     IN /\ wallet_free >= hold
        /\ dop_next_seq' = 4
        /\ wallet_free' = wallet_free - hold
        /\ wallet_locked' = wallet_locked + hold
        /\ slot_state' = [slot_state EXCEPT ![3] = "LIVE"]
        /\ ord_status' = [ord_status EXCEPT ![3] = "PENDING_NEW"]
        /\ ord_filled' = [ord_filled EXCEPT ![3] = 0]
        /\ ord_proj' = [ord_proj EXCEPT ![3] = 0]
        /\ ord_hold_rem' = [ord_hold_rem EXCEPT ![3] = hold]
        /\ stp_linked' = [stp_linked EXCEPT ![3] = TRUE]
        /\ UNCHANGED <<
             bootstrapped, external_ignored_cnt, bad_mac_attributed, stale_seq_refused_seen,
             ack_emitted_cnt, cxl_emitted, cxl_qty_out,
             trade_applied, trade_fee_known, trade_fee_signed, trade_amend_cnt,
             net_pos_lots, settled_spend, settled_fees, conflict_frozen
           >>

(* 4. Live `on_message` `NEW` / `PARTIALLY_FILLED` ack frame (`crates/tickwire-binance-account/src/session.rs:2695-2731`):
 *    - On recovered `seq = 2` (`ord_status = "OPEN"`), `was_pending_new` is `FALSE` -> zero duplicate `Ack`!
 *    - On fresh `seq = 3` (`ord_status = "PENDING_NEW"`), transitions to `"OPEN"` and emits exactly 1 `Ack`. *)
LiveRespOrderAck(seq) ==
  /\ bootstrapped
  /\ slot_state[seq] = "LIVE"
  /\ LET was_pending == (ord_status[seq] = "PENDING_NEW")
     IN /\ ord_status' = [ord_status EXCEPT ![seq] = "OPEN"]
        /\ ack_emitted_cnt' = [ack_emitted_cnt EXCEPT ![seq] = IF was_pending THEN ack_emitted_cnt[seq] + 1 ELSE ack_emitted_cnt[seq]]
        /\ UNCHANGED <<
             bootstrapped, dop_next_seq, external_ignored_cnt, bad_mac_attributed, stale_seq_refused_seen,
             slot_state, ord_filled, ord_proj, ord_hold_rem, stp_linked,
             cxl_emitted, cxl_qty_out,
             trade_applied, trade_fee_known, trade_fee_signed, trade_amend_cnt,
             wallet_free, wallet_locked, net_pos_lots, settled_spend, settled_fees, conflict_frozen
           >>

(* 5. Live `on_message` `TRADE_LITE` (`fee = None`) or full `TRADE` (`fee = Some`):
 *    - Applies `TradeLots(tid)` once via `FillRecordPool`;
 *    - If `TRADE_LITE` arrives on `PendingNew` `seq = 3` before `NEW`, `is_ack_eligible` would see slot
 *      already retired when `new_filled == OrigQty(seq)` or emit `One ack` if partial (`crates/tickwire-binance-account/src/session.rs:2698-2731`);
 *    - Immediately retires slot and unlinks `OrsStpBook` on full fill without waiting for `ORDER_TRADE_UPDATE` fee! *)
LiveRespTradeExecution(tid, is_trade_lite, fee_val) ==
  /\ bootstrapped
  /\ ~trade_applied[tid]
  /\ LET seq == TradeOwner(tid)
         lots == TradeLots(tid)
         spend == TradeSpend(tid)
         eff_fee == IF is_trade_lite THEN 0 ELSE fee_val
         rel == ord_hold_rem[seq]
         new_filled == ord_filled[seq] + lots
         full == (new_filled >= OrigQty(seq))
     IN /\ slot_state[seq] = "LIVE"
        /\ trade_applied' = [trade_applied EXCEPT ![tid] = TRUE]
        /\ trade_fee_known' = [trade_fee_known EXCEPT ![tid] = ~is_trade_lite]
        /\ trade_fee_signed' = [trade_fee_signed EXCEPT ![tid] = eff_fee]
        /\ ord_filled' = [ord_filled EXCEPT ![seq] = new_filled]
        /\ ord_proj' = [ord_proj EXCEPT ![seq] = new_filled]
        /\ ord_hold_rem' = [ord_hold_rem EXCEPT ![seq] = IF full THEN 0 ELSE (rel - spend)]
        /\ ord_status' = [ord_status EXCEPT ![seq] = IF full THEN "TERMINAL" ELSE "OPEN"]
        /\ stp_linked' = [stp_linked EXCEPT ![seq] = IF full THEN FALSE ELSE TRUE]
        /\ slot_state' = [slot_state EXCEPT ![seq] = IF full THEN "RETIRED" ELSE "LIVE"]
        /\ wallet_locked' = wallet_locked - (IF full THEN rel ELSE spend)
        /\ wallet_free' = wallet_free + (IF full THEN rel ELSE spend) - spend + eff_fee
        /\ net_pos_lots' = net_pos_lots + lots
        /\ settled_spend' = settled_spend + spend
        /\ settled_fees' = settled_fees + eff_fee
        /\ UNCHANGED <<
             bootstrapped, dop_next_seq, external_ignored_cnt, bad_mac_attributed, stale_seq_refused_seen,
             ack_emitted_cnt, cxl_emitted, cxl_qty_out,
             trade_amend_cnt, conflict_frozen
           >>

(* 6. Live `on_message` `ORDER_TRADE_UPDATE` / `executionReport` arriving after `TRADE_LITE`
 *    (even after slot retirement!): `FillRecordPool` emits `amendment = true` fee/rebate patch
 *    or suppresses exact duplicate (`crates/tickwire-binance-account/src/session.rs:2601-2641`,
 *    `crates/tickwire-account/src/order_manager.rs:1840-1870,1928-2011`). *)
LiveRespFeeAmendmentOrDuplicate(tid, has_fee, fee_val) ==
  /\ bootstrapped
  /\ trade_applied[tid]
  /\ LET is_amend == (has_fee /\ (~trade_fee_known[tid] \/ trade_fee_signed[tid] # fee_val))
         delta == IF is_amend THEN (fee_val - trade_fee_signed[tid]) ELSE 0
     IN /\ (~is_amend \/ trade_amend_cnt[tid] < 1)
        /\ trade_fee_known' = [trade_fee_known EXCEPT ![tid] = IF has_fee THEN TRUE ELSE trade_fee_known[tid]]
        /\ trade_fee_signed' = [trade_fee_signed EXCEPT ![tid] = IF is_amend THEN fee_val ELSE trade_fee_signed[tid]]
        /\ trade_amend_cnt' = [trade_amend_cnt EXCEPT ![tid] = IF is_amend THEN trade_amend_cnt[tid] + 1 ELSE trade_amend_cnt[tid]]
        /\ wallet_free' = wallet_free + delta
        /\ settled_fees' = settled_fees + delta
        /\ UNCHANGED <<
             bootstrapped, dop_next_seq, external_ignored_cnt, bad_mac_attributed, stale_seq_refused_seen,
             slot_state, ord_status, ord_filled, ord_proj, ord_hold_rem, stp_linked,
             ack_emitted_cnt, cxl_emitted, cxl_qty_out,
             trade_applied, wallet_locked, net_pos_lots, settled_spend, conflict_frozen
           >>

(* 7. Live `on_message` `CANCELED` / `EXPIRED_IN_MATCH` (`crates/tickwire-binance-account/src/session.rs:2766-2812`):
 *    settles exact unfilled remainder (`3 - 1 = 2` on `seq = 2`, `1 - 0 = 1` on `seq = 3`), releases hold,
 *    unlinks `OrsStpBook`, and retires slot immediately. *)
LiveRespOrderCanceled(seq) ==
  /\ bootstrapped
  /\ slot_state[seq] = "LIVE"
  /\ LET rem_hold == ord_hold_rem[seq]
         cxl_q == OrigQty(seq) - ord_proj[seq]
     IN /\ slot_state' = [slot_state EXCEPT ![seq] = "RETIRED"]
        /\ ord_status' = [ord_status EXCEPT ![seq] = "TERMINAL"]
        /\ ord_hold_rem' = [ord_hold_rem EXCEPT ![seq] = 0]
        /\ stp_linked' = [stp_linked EXCEPT ![seq] = FALSE]
        /\ wallet_locked' = wallet_locked - rem_hold
        /\ wallet_free' = wallet_free + rem_hold
        /\ cxl_emitted' = [cxl_emitted EXCEPT ![seq] = TRUE]
        /\ cxl_qty_out' = [cxl_qty_out EXCEPT ![seq] = cxl_q]
        /\ UNCHANGED <<
             bootstrapped, dop_next_seq, external_ignored_cnt, bad_mac_attributed, stale_seq_refused_seen,
             ord_filled, ord_proj, ack_emitted_cnt,
             trade_applied, trade_fee_known, trade_fee_signed, trade_amend_cnt,
             net_pos_lots, settled_spend, settled_fees, conflict_frozen
           >>

Next ==
  \/ ColdBootstrapFromExchangeRest
  \/ LiveRefuseBelowRecoveredFloor
  \/ LiveSubmitNewOrderSeq3
  \/ \E seq \in ManagedSeqs: LiveRespOrderAck(seq)
  \/ \E tid \in Trades, lite \in BOOLEAN, fv \in {-1, 1}: LiveRespTradeExecution(tid, lite, fv)
  \/ \E tid \in Trades, hf \in BOOLEAN, fv \in {-1, 1}: LiveRespFeeAmendmentOrDuplicate(tid, hf, fv)
  \/ \E seq \in ManagedSeqs: LiveRespOrderCanceled(seq)

(* -------------------------------------------------------------------------
 * Safety Invariants
 * ------------------------------------------------------------------------- *)

TypeOK ==
  /\ bootstrapped \in BOOLEAN
  /\ dop_next_seq \in 1..4
  /\ external_ignored_cnt \in {0, 2}
  /\ bad_mac_attributed \in BOOLEAN
  /\ stale_seq_refused_seen \in BOOLEAN
  /\ slot_state \in [ManagedSeqs -> {"EMPTY", "LIVE", "RETIRED"}]
  /\ ord_status \in [ManagedSeqs -> {"NONE", "PENDING_NEW", "OPEN", "TERMINAL"}]
  /\ stp_linked \in [ManagedSeqs -> BOOLEAN]
  /\ ack_emitted_cnt \in [ManagedSeqs -> 0..1]
  /\ cxl_emitted \in [ManagedSeqs -> BOOLEAN]
  /\ trade_applied \in [Trades -> BOOLEAN]
  /\ trade_fee_known \in [Trades -> BOOLEAN]
  /\ trade_fee_signed \in [Trades -> -1..1]
  /\ trade_amend_cnt \in [Trades -> 0..1]
  /\ conflict_frozen \in BOOLEAN

(* Domain-MAC attribution soundness: external & mismatched-MAC orders never enter `OrderSlot`s,
 * and recovered `seq = 2` seeds `dop_next_seq >= 3` without any local disk state: *)
DomainMacAndSeqFloorSound ==
  /\ ~bad_mac_attributed
  /\ (bootstrapped => (dop_next_seq >= 3 /\ external_ignored_cnt = 2))

(* Recovered `Open` order (`seq = 2`) has `ack_emitted = TRUE` on bootstrap so live updates emit 0 `Ack`s,
 * while fresh `seq = 3` emits at most 1 `Ack`: *)
RecoveredAndLiveAckSound ==
  /\ ack_emitted_cnt[2] = 0
  /\ ack_emitted_cnt[3] <= 1

(* Zero double-lock on Spot (`balances_include_open_order_locks = true`) & exact margin hold on
 * Derivatives (`false`), preserving external Spot lock untouched throughout execution: *)
NoDoubleLockAndSolvencyPreserved ==
  bootstrapped =>
    /\ wallet_locked = (ActiveManagedHold + ExternalSpotLock)
    /\ (wallet_free + wallet_locked) = (InitialTrueEquity - settled_spend + settled_fees)
    /\ \A q \in ManagedSeqs:
         /\ (stp_linked[q] <=> (slot_state[q] = "LIVE"))
         /\ (slot_state[q] # "LIVE" => ord_hold_rem[q] = 0)
         /\ (cxl_emitted[q] => cxl_qty_out[q] = (OrigQty(q) - ord_proj[q]))

(* `TRADE_LITE -> ORDER_TRADE_UPDATE` applies executed lots once and fee/rebate patch with zero `Conflict` freeze: *)
TradeLiteAmendmentExact ==
  bootstrapped =>
    /\ net_pos_lots = ExchInitPos
                        + (IF trade_applied[1] THEN TradeLots(1) ELSE 0)
                        + (IF trade_applied[2] THEN TradeLots(2) ELSE 0)
    /\ settled_fees = (trade_fee_signed[1] + trade_fee_signed[2])
    /\ ~conflict_frozen

====
