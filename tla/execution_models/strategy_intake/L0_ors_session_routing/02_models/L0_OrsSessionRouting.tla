---- MODULE L0_OrsSessionRouting ----
(*
 * Finite explicit-state TLA+ specification of unified ORS multi-account session
 * routing, DOP sequence floor / gap gating, microsecond stale-order rejection,
 * and granular `(account_ref, strat_id)` disable + ascending one-order-at-a-time
 * auto-cancel sweep.
 *
 * Verified production Rust anchors:
 *   - Multi-account `account_ref = 0..255` resolution (`0` -> first authorized
 *     `AccountSession`, `1..=255` -> explicit `AccountSession`, unauthorized
 *     `(account_ref, strat_id)` rejected without session state mutation):
 *       crates/tickwire-runtime-policy/src/lib.rs:426-453
 *       crates/tickwire-runtime-policy/src/lib.rs:487-513
 *       crates/tickwire-account/src/dop_state.rs:409-427
 *   - `DopState` login `max(login_seq, next_order_seqnum, 1)` floor across
 *     worker restarts & `last_recv_seqnum` baseline / `+1` gap detection:
 *       crates/tickwire-account/src/dop_state.rs:192-227
 *       crates/tickwire-account/src/dop_state.rs:230-243
 *       crates/tickwire-account/src/dop_state.rs:268-314
 *       crates/tickwire-account/src/dop_state.rs:392-406
 *       crates/tickwire-runtime/src/admin.rs:840-906
 *   - Microsecond `max_order_age_ns` (`DEFAULT_MAX_ORDER_AGE_NS = 23_000`)
 *     per-order `RejectStaleOrder` gate without freezing account or strategy:
 *       crates/tickwire-runtime/src/ors.rs:61
 *       crates/tickwire-runtime/src/ors.rs:1554-1621
 *   - Granular `set_strat_enabled` (`account_ref == 0` all-accounts wildcard,
 *     `strat_id == 255` all-strategies wildcard) + ascending one-order-at-a-time
 *     `set_strat_enabled_collect` auto-cancel sweep (`ADMIN_ACTION` / `cancel_on_ack`):
 *       crates/tickwire-account/src/dop_state.rs:336-389
 *       crates/tickwire-account/src/order_manager.rs:1171-1333
 *       crates/tickwire-account/src/order_manager.rs:2271-2383
 *       crates/tickwire-runtime/src/admin.rs:450-468
 *       crates/tickwire-runtime/src/ors.rs:2205-2218
 *       crates/tickwire-runtime/src/ors.rs:2279-2336
 *)
EXTENDS Integers, FiniteSets

CONSTANTS
  MaxSlots,
  MaxMsgSeq,
  MaxOrdSeq,
  MaxOrderAgeNs,
  AcctBudget,
  MaxRestarts,
  MaxAdminOps

VARIABLES
  logged_in,
  restarts_used,
  last_recv_seq,
  next_ord_seq,
  high_water_ord_seq,
  seq_gap_rejected,
  strat_enabled,
  admin_ops_used,
  now_ns,
  acct_hold,
  slot_state,
  slot_acct,
  slot_strat,
  slot_ord_seq,
  slot_cancel_on_ack,
  sweep_cursor,
  sweep_acct,
  sweep_strat

vars == <<
  logged_in, restarts_used, last_recv_seq, next_ord_seq, high_water_ord_seq,
  seq_gap_rejected, strat_enabled, admin_ops_used, now_ns, acct_hold,
  slot_state, slot_acct, slot_strat, slot_ord_seq, slot_cancel_on_ack,
  sweep_cursor, sweep_acct, sweep_strat
>>

(* Registered `AccountSession` refs (`1..2`); `0` routes to default, `3` is unregistered. *)
Accounts == {1, 2}
ReqAccountRefs == {0, 1, 2, 3}
AdminAccountRefs == {0, 1, 2}

(* Configured strategy workers (`1..2`); `255` (`ALL_ORS_STRATS`) is admin wildcard. *)
Strats == {1, 2}
ALL_ORS_STRATS == 255
AdminStratArgs == {1, 2, ALL_ORS_STRATS}

Slots == 1..MaxSlots
SlotStates == {"Empty", "PendingNew", "Open", "PendingCancel", "PendingAdminCancel"}
ClockDomain == {1, 1 + MaxOrderAgeNs + 1}

Max2(a, b) == IF a >= b THEN a ELSE b

(* Strategy 1 is multi-account (`{1, 2}`, default `1`); Strategy 2 is single-account (`{2}`, default `2`). *)
DefaultAccount(s) == IF s = 1 THEN 1 ELSE 2
AuthorizedAccounts(s) == IF s = 1 THEN {1, 2} ELSE {2}

(*
 * Exact `OrsRuntimePolicy::resolve_account_ref` / `DopState::resolve_account_locked`
 * (`crates/tickwire-runtime-policy/src/lib.rs:426-453`, `crates/tickwire-account/src/dop_state.rs:409-427`):
 *   > 0 : resolved `AccountSession` ID (`1..255`)
 *   -1  : `INVALID_ACCOUNT_ID` (unregistered `account_ref`)
 *   -2  : `REJECT_ROUTING_ID_MISMATCH` (`(account_ref, strat_id)` unauthorized)
 *)
ResolveAccount(s, req_acct) ==
  IF req_acct = 0 THEN DefaultAccount(s)
  ELSE IF req_acct \notin Accounts THEN -1
  ELSE IF req_acct \notin AuthorizedAccounts(s) THEN -2
  ELSE req_acct

(*
 * Exact `DopState::check_msg_seqnum` (`crates/tickwire-account/src/dop_state.rs:392-406`):
 *   - `msg_seqnum == 0` -> `UNEXPECTED_MSG_SEQNUM`
 *   - `last_recv_seq == 0` -> any `msg_seqnum >= 1` establishes baseline
 *   - `last_recv_seq > 0` -> requires `msg_seqnum == last_recv_seq + 1`
 *)
ValidMsgSeq(s, mseq) ==
  /\ mseq >= 1
  /\ (last_recv_seq[s] = 0 \/ mseq = last_recv_seq[s] + 1)

MatchesAdminFilter(a, s, acct_arg, strat_arg) ==
  /\ (acct_arg = 0 \/ a = acct_arg)
  /\ (strat_arg = ALL_ORS_STRATS \/ s = strat_arg)

HasFreeSlot == \E k \in Slots : slot_state[k] = "Empty"
FirstFreeSlot == CHOOSE k \in Slots :
  /\ slot_state[k] = "Empty"
  /\ \A j \in Slots : (j < k) => (slot_state[j] /= "Empty")

SlotsForAccount(a) == {k \in Slots : slot_state[k] /= "Empty" /\ slot_acct[k] = a}

Init ==
  /\ logged_in = [s \in Strats |-> TRUE]
  /\ restarts_used = 0
  /\ last_recv_seq = [s \in Strats |-> 0]
  /\ next_ord_seq = [s \in Strats |-> 1]
  /\ high_water_ord_seq = [s \in Strats |-> 1]
  /\ seq_gap_rejected = FALSE
  /\ strat_enabled = [a \in Accounts |-> {s \in Strats : a \in AuthorizedAccounts(s)}]
  /\ admin_ops_used = 0
  /\ now_ns = 1
  /\ acct_hold = [a \in Accounts |-> 0]
  /\ slot_state = [k \in Slots |-> "Empty"]
  /\ slot_acct = [k \in Slots |-> 0]
  /\ slot_strat = [k \in Slots |-> 0]
  /\ slot_ord_seq = [k \in Slots |-> 0]
  /\ slot_cancel_on_ack = [k \in Slots |-> FALSE]
  /\ sweep_cursor = 0
  /\ sweep_acct = 0
  /\ sweep_strat = 0

(* Monotonic clock step exposing `now_ns - create_ts > MaxOrderAgeNs`. *)
AdvanceClock ==
  /\ now_ns = 1
  /\ now_ns' = 1 + MaxOrderAgeNs + 1
  /\ UNCHANGED <<
       logged_in, restarts_used, last_recv_seq, next_ord_seq, high_water_ord_seq,
       seq_gap_rejected, strat_enabled, admin_ops_used, acct_hold,
       slot_state, slot_acct, slot_strat, slot_ord_seq, slot_cancel_on_ack,
       sweep_cursor, sweep_acct, sweep_strat
     >>

(* Worker disconnect / DOP logout (`crates/tickwire-account/src/dop_state.rs:215-227`). *)
DopLogout ==
  \E s \in Strats :
    /\ logged_in[s]
    /\ restarts_used < MaxRestarts
    /\ logged_in' = [logged_in EXCEPT ![s] = FALSE]
    /\ restarts_used' = restarts_used + 1
    /\ last_recv_seq' = [last_recv_seq EXCEPT ![s] = 0]
    /\ seq_gap_rejected' = FALSE
    /\ UNCHANGED <<
         next_ord_seq, high_water_ord_seq, strat_enabled, admin_ops_used,
         now_ns, acct_hold, slot_state, slot_acct, slot_strat, slot_ord_seq,
         slot_cancel_on_ack, sweep_cursor, sweep_acct, sweep_strat
       >>

(*
 * Worker reconnect / DOP login (`crates/tickwire-account/src/dop_state.rs:192-212`,
 * `crates/tickwire-runtime/src/admin.rs:840-906`): enforces monotonic
 * `max(next_order_seqnum, login_seq, 1)` and resets `last_recv_seqnum = 0`.
 *)
DopLogin ==
  \E s \in Strats, login_seq \in 0..MaxOrdSeq :
    /\ ~logged_in[s]
    /\ LET floor == Max2(next_ord_seq[s], Max2(login_seq, 1)) IN
       /\ logged_in' = [logged_in EXCEPT ![s] = TRUE]
       /\ last_recv_seq' = [last_recv_seq EXCEPT ![s] = 0]
       /\ next_ord_seq' = [next_ord_seq EXCEPT ![s] = floor]
       /\ high_water_ord_seq' = [high_water_ord_seq EXCEPT ![s] = Max2(@, floor)]
       /\ seq_gap_rejected' = FALSE
    /\ UNCHANGED <<
         restarts_used, strat_enabled, admin_ops_used, now_ns, acct_hold,
         slot_state, slot_acct, slot_strat, slot_ord_seq, slot_cancel_on_ack,
         sweep_cursor, sweep_acct, sweep_strat
       >>

(*
 * Admitted `NewOrder` (`crates/tickwire-runtime/src/ors.rs:1581-1725`,
 * `crates/tickwire-account/src/dop_state.rs:288-314`,
 * `crates/tickwire-account/src/order_manager.rs:1171-1333`).
 *)
SubmitNewOrderAdmit ==
  /\ HasFreeSlot
  /\ \E s \in Strats, req_acct \in {0, 1, 2}, mseq \in 1..MaxMsgSeq,
        oseq \in 1..MaxOrdSeq, create_ts \in 1..(1 + MaxOrderAgeNs + 1) :
       /\ logged_in[s]
       /\ ValidMsgSeq(s, mseq)
       /\ oseq >= next_ord_seq[s]
       /\ create_ts <= now_ns
       /\ (now_ns - create_ts) <= MaxOrderAgeNs
       /\ LET acct == ResolveAccount(s, req_acct)
              k    == FirstFreeSlot
          IN
          /\ acct > 0
          /\ s \in strat_enabled[acct]
          /\ acct_hold[acct] < AcctBudget
          /\ last_recv_seq' = [last_recv_seq EXCEPT ![s] = mseq]
          /\ next_ord_seq' = [next_ord_seq EXCEPT ![s] = oseq + 1]
          /\ high_water_ord_seq' = [high_water_ord_seq EXCEPT ![s] = Max2(@, oseq + 1)]
          /\ seq_gap_rejected' = FALSE
          /\ acct_hold' = [acct_hold EXCEPT ![acct] = @ + 1]
          /\ slot_state' = [slot_state EXCEPT ![k] = "PendingNew"]
          /\ slot_acct' = [slot_acct EXCEPT ![k] = acct]
          /\ slot_strat' = [slot_strat EXCEPT ![k] = s]
          /\ slot_ord_seq' = [slot_ord_seq EXCEPT ![k] = oseq]
          /\ slot_cancel_on_ack' = [slot_cancel_on_ack EXCEPT ![k] = FALSE]
  /\ UNCHANGED <<
       logged_in, restarts_used, strat_enabled, admin_ops_used, now_ns,
       sweep_cursor, sweep_acct, sweep_strat
     >>

(*
 * Unauthorized / unregistered `account_ref` refusal (`INVALID_ACCOUNT_ID` or
 * `REJECT_ROUTING_ID_MISMATCH`, `crates/tickwire-runtime/src/ors.rs:1554-1603`,
 * `crates/tickwire-account/src/dop_state.rs:268-281,409-427`): advances only
 * transport `last_recv_seqnum` and mutates zero `AccountSession` or `next_ord_seq` state.
 *)
RejectUnauthorizedAccount ==
  \E s \in Strats, req_acct \in ReqAccountRefs, mseq \in 1..MaxMsgSeq :
    /\ logged_in[s]
    /\ ValidMsgSeq(s, mseq)
    /\ ResolveAccount(s, req_acct) < 0
    /\ last_recv_seq' = [last_recv_seq EXCEPT ![s] = mseq]
    /\ seq_gap_rejected' = FALSE
    /\ UNCHANGED <<
         logged_in, restarts_used, next_ord_seq, high_water_ord_seq,
         strat_enabled, admin_ops_used, now_ns, acct_hold,
         slot_state, slot_acct, slot_strat, slot_ord_seq, slot_cancel_on_ack,
         sweep_cursor, sweep_acct, sweep_strat
       >>

(*
 * `UNEXPECTED_MSG_SEQNUM` gap / zero refusal (`crates/tickwire-account/src/dop_state.rs:392-406`):
 * refuses without advancing `last_recv_seqnum` or mutating `AccountSession` state.
 *)
RejectMsgSeqGap ==
  \E s \in Strats, mseq \in 0..MaxMsgSeq :
    /\ logged_in[s]
    /\ ~ValidMsgSeq(s, mseq)
    /\ ~seq_gap_rejected
    /\ seq_gap_rejected' = TRUE
    /\ UNCHANGED <<
         logged_in, restarts_used, last_recv_seq, next_ord_seq, high_water_ord_seq,
         strat_enabled, admin_ops_used, now_ns, acct_hold,
         slot_state, slot_acct, slot_strat, slot_ord_seq, slot_cancel_on_ack,
         sweep_cursor, sweep_acct, sweep_strat
       >>

(*
 * `RESTRICTED_CANCEL_ONLY` / `ADMIN_ACTION` refusal when `(resolved_account, strat_id)` is disabled
 * (`crates/tickwire-runtime-policy/src/lib.rs:501-510`, `crates/tickwire-account/src/dop_state.rs:309-311`).
 *)
RejectDisabledStratNewOrder ==
  \E s \in Strats, req_acct \in {0, 1, 2}, mseq \in 1..MaxMsgSeq, oseq \in 1..MaxOrdSeq :
    /\ logged_in[s]
    /\ ValidMsgSeq(s, mseq)
    /\ LET acct == ResolveAccount(s, req_acct)
           nxt  == Max2(next_ord_seq[s], oseq + 1)
       IN
       /\ acct > 0
       /\ s \notin strat_enabled[acct]
       /\ last_recv_seq' = [last_recv_seq EXCEPT ![s] = mseq]
       /\ next_ord_seq' = [next_ord_seq EXCEPT ![s] = nxt]
       /\ high_water_ord_seq' = [high_water_ord_seq EXCEPT ![s] = Max2(@, nxt)]
       /\ seq_gap_rejected' = FALSE
    /\ UNCHANGED <<
         logged_in, restarts_used, strat_enabled, admin_ops_used, now_ns, acct_hold,
         slot_state, slot_acct, slot_strat, slot_ord_seq, slot_cancel_on_ack,
         sweep_cursor, sweep_acct, sweep_strat
       >>

(*
 * Microsecond `max_order_age_ns` stale-order gate (`RejectStaleOrder`,
 * `crates/tickwire-runtime/src/ors.rs:1607-1621`): rejects only the stale intent
 * without freezing any account or strategy.
 *)
RejectStaleNewOrder ==
  \E s \in Strats, req_acct \in {0, 1, 2}, mseq \in 1..MaxMsgSeq,
     oseq \in 1..MaxOrdSeq, create_ts \in 1..now_ns :
    /\ logged_in[s]
    /\ ValidMsgSeq(s, mseq)
    /\ (now_ns - create_ts) > MaxOrderAgeNs
    /\ LET acct == ResolveAccount(s, req_acct)
           nxt  == Max2(next_ord_seq[s], oseq + 1)
       IN
       /\ acct > 0
       /\ s \in strat_enabled[acct]
       /\ last_recv_seq' = [last_recv_seq EXCEPT ![s] = mseq]
       /\ next_ord_seq' = [next_ord_seq EXCEPT ![s] = nxt]
       /\ high_water_ord_seq' = [high_water_ord_seq EXCEPT ![s] = Max2(@, nxt)]
       /\ seq_gap_rejected' = FALSE
    /\ UNCHANGED <<
         logged_in, restarts_used, strat_enabled, admin_ops_used, now_ns, acct_hold,
         slot_state, slot_acct, slot_strat, slot_ord_seq, slot_cancel_on_ack,
         sweep_cursor, sweep_acct, sweep_strat
       >>

(*
 * Strategy management cancel (`PolicyAction::CancelOrder`,
 * `crates/tickwire-runtime-policy/src/lib.rs:490-513`,
 * `crates/tickwire-account/src/dop_state.rs:268-281`): permitted even when
 * `(acct, s)` is disabled (`RESTRICTED_CANCEL_ONLY` blocks only new orders).
 *)
SubmitStrategyCancel ==
  \E s \in Strats, req_acct \in {0, 1, 2}, mseq \in 1..MaxMsgSeq, k \in Slots :
    /\ logged_in[s]
    /\ ValidMsgSeq(s, mseq)
    /\ LET acct == ResolveAccount(s, req_acct) IN
       /\ acct > 0
       /\ slot_acct[k] = acct
       /\ slot_strat[k] = s
       /\ slot_state[k] \in {"PendingNew", "Open"}
       /\ ~(slot_state[k] = "PendingNew" /\ slot_cancel_on_ack[k])
       /\ last_recv_seq' = [last_recv_seq EXCEPT ![s] = mseq]
       /\ seq_gap_rejected' = FALSE
       /\ IF slot_state[k] = "PendingNew"
          THEN /\ slot_cancel_on_ack' = [slot_cancel_on_ack EXCEPT ![k] = TRUE]
               /\ UNCHANGED slot_state
          ELSE /\ slot_state' = [slot_state EXCEPT ![k] = "PendingCancel"]
               /\ UNCHANGED slot_cancel_on_ack
    /\ UNCHANGED <<
         logged_in, restarts_used, next_ord_seq, high_water_ord_seq,
         strat_enabled, admin_ops_used, now_ns, acct_hold,
         slot_acct, slot_strat, slot_ord_seq,
         sweep_cursor, sweep_acct, sweep_strat
       >>

(*
 * Admin `set_strat_enabled(..., enabled = false)` (`crates/tickwire-account/src/dop_state.rs:352-389`,
 * `crates/tickwire-runtime/src/ors.rs:2279-2336`):
 *   - `acct_arg == 0` disables across all registered accounts
 *   - `strat_arg == 255` (`ALL_ORS_STRATS`) disables across all strategies
 *   - starts ascending one-order-at-a-time scan at `sweep_cursor = 1`
 *)
AdminDisableAndStartSweep ==
  /\ sweep_cursor = 0
  /\ admin_ops_used < MaxAdminOps
  /\ \E acct_arg \in AdminAccountRefs, strat_arg \in AdminStratArgs :
       /\ \E a \in Accounts, s \in Strats :
            MatchesAdminFilter(a, s, acct_arg, strat_arg) /\ s \in strat_enabled[a]
       /\ strat_enabled' = [a \in Accounts |->
            {s \in strat_enabled[a] : ~MatchesAdminFilter(a, s, acct_arg, strat_arg)}]
       /\ admin_ops_used' = admin_ops_used + 1
       /\ sweep_cursor' = 1
       /\ sweep_acct' = acct_arg
       /\ sweep_strat' = strat_arg
  /\ UNCHANGED <<
       logged_in, restarts_used, last_recv_seq, next_ord_seq, high_water_ord_seq,
       seq_gap_rejected, now_ns, acct_hold, slot_state, slot_acct, slot_strat,
       slot_ord_seq, slot_cancel_on_ack
     >>

(*
 * Single-slot step of `OrderManager::set_strat_enabled_collect`
 * (`crates/tickwire-account/src/order_manager.rs:2271-2383`):
 * acquires only slot `k = sweep_cursor` in ascending order, marks `PendingNew`
 * with `cancel_on_ack = TRUE` or transitions `Open` to `PendingAdminCancel`
 * (`ADMIN_ACTION`), then releases before moving to `sweep_cursor + 1`.
 *)
AdminSweepNextSlot ==
  /\ sweep_cursor \in Slots
  /\ LET k       == sweep_cursor
         matches == /\ slot_state[k] \in {"PendingNew", "Open"}
                    /\ MatchesAdminFilter(slot_acct[k], slot_strat[k], sweep_acct, sweep_strat)
     IN
     /\ IF matches /\ slot_state[k] = "PendingNew"
        THEN /\ slot_cancel_on_ack' = [slot_cancel_on_ack EXCEPT ![k] = TRUE]
             /\ UNCHANGED slot_state
        ELSE IF matches /\ slot_state[k] = "Open"
        THEN /\ slot_state' = [slot_state EXCEPT ![k] = "PendingAdminCancel"]
             /\ UNCHANGED slot_cancel_on_ack
        ELSE UNCHANGED <<slot_state, slot_cancel_on_ack>>
     /\ IF k = MaxSlots
        THEN /\ sweep_cursor' = 0
             /\ sweep_acct' = 0
             /\ sweep_strat' = 0
        ELSE /\ sweep_cursor' = k + 1
             /\ UNCHANGED <<sweep_acct, sweep_strat>>
  /\ UNCHANGED <<
       logged_in, restarts_used, last_recv_seq, next_ord_seq, high_water_ord_seq,
       seq_gap_rejected, strat_enabled, admin_ops_used, now_ns, acct_hold,
       slot_acct, slot_strat, slot_ord_seq
     >>

(* Admin `set_strat_enabled(..., enabled = true)` (`crates/tickwire-account/src/dop_state.rs:352-389`). *)
AdminEnableStrat ==
  /\ sweep_cursor = 0
  /\ admin_ops_used < MaxAdminOps
  /\ \E acct_arg \in AdminAccountRefs, strat_arg \in AdminStratArgs :
       /\ \E a \in Accounts, s \in Strats :
            /\ a \in AuthorizedAccounts(s)
            /\ MatchesAdminFilter(a, s, acct_arg, strat_arg)
            /\ s \notin strat_enabled[a]
       /\ strat_enabled' = [a \in Accounts |->
            strat_enabled[a] \cup {s \in Strats :
              a \in AuthorizedAccounts(s) /\ MatchesAdminFilter(a, s, acct_arg, strat_arg)}]
       /\ admin_ops_used' = admin_ops_used + 1
  /\ UNCHANGED <<
       logged_in, restarts_used, last_recv_seq, next_ord_seq, high_water_ord_seq,
       seq_gap_rejected, now_ns, acct_hold, slot_state, slot_acct, slot_strat,
       slot_ord_seq, slot_cancel_on_ack, sweep_cursor, sweep_acct, sweep_strat
     >>

(* Venue `Ack` on `PendingNew`: honors `cancel_on_ack` immediately (`ADMIN_ACTION` / strategy cancel). *)
VenueAckOrder ==
  \E k \in Slots :
    /\ slot_state[k] = "PendingNew"
    /\ slot_state' = [slot_state EXCEPT ![k] =
         IF slot_cancel_on_ack[k] THEN "PendingAdminCancel" ELSE "Open"]
    /\ slot_cancel_on_ack' = [slot_cancel_on_ack EXCEPT ![k] = FALSE]
    /\ UNCHANGED <<
         logged_in, restarts_used, last_recv_seq, next_ord_seq, high_water_ord_seq,
         seq_gap_rejected, strat_enabled, admin_ops_used, now_ns, acct_hold,
         slot_acct, slot_strat, slot_ord_seq, sweep_cursor, sweep_acct, sweep_strat
       >>

(* Venue terminal completion (`Canceled` / `Filled`) releasing `AccountSession` hold. *)
VenueTerminalRelease ==
  \E k \in Slots :
    /\ slot_state[k] \in {"Open", "PendingCancel", "PendingAdminCancel"}
    /\ LET a == slot_acct[k] IN
       /\ acct_hold' = [acct_hold EXCEPT ![a] = @ - 1]
       /\ slot_state' = [slot_state EXCEPT ![k] = "Empty"]
       /\ slot_acct' = [slot_acct EXCEPT ![k] = 0]
       /\ slot_strat' = [slot_strat EXCEPT ![k] = 0]
       /\ slot_ord_seq' = [slot_ord_seq EXCEPT ![k] = 0]
       /\ slot_cancel_on_ack' = [slot_cancel_on_ack EXCEPT ![k] = FALSE]
    /\ UNCHANGED <<
         logged_in, restarts_used, last_recv_seq, next_ord_seq, high_water_ord_seq,
         seq_gap_rejected, strat_enabled, admin_ops_used, now_ns,
         sweep_cursor, sweep_acct, sweep_strat
       >>

Next ==
  \/ AdvanceClock
  \/ DopLogout
  \/ DopLogin
  \/ SubmitNewOrderAdmit
  \/ RejectUnauthorizedAccount
  \/ RejectMsgSeqGap
  \/ RejectDisabledStratNewOrder
  \/ RejectStaleNewOrder
  \/ SubmitStrategyCancel
  \/ AdminDisableAndStartSweep
  \/ AdminSweepNextSlot
  \/ AdminEnableStrat
  \/ VenueAckOrder
  \/ VenueTerminalRelease

Spec == Init /\ [][Next]_vars

(* ========================================================================= *)
(* Safety Invariants                                                         *)
(* ========================================================================= *)

TypeOK ==
  /\ logged_in \in [Strats -> BOOLEAN]
  /\ restarts_used \in 0..MaxRestarts
  /\ last_recv_seq \in [Strats -> 0..MaxMsgSeq]
  /\ next_ord_seq \in [Strats -> 1..(MaxOrdSeq + 1)]
  /\ high_water_ord_seq \in [Strats -> 1..(MaxOrdSeq + 1)]
  /\ seq_gap_rejected \in BOOLEAN
  /\ strat_enabled \in [Accounts -> SUBSET Strats]
  /\ admin_ops_used \in 0..MaxAdminOps
  /\ now_ns \in ClockDomain
  /\ acct_hold \in [Accounts -> 0..AcctBudget]
  /\ slot_state \in [Slots -> SlotStates]
  /\ slot_acct \in [Slots -> {0} \cup Accounts]
  /\ slot_strat \in [Slots -> {0} \cup Strats]
  /\ slot_ord_seq \in [Slots -> 0..MaxOrdSeq]
  /\ slot_cancel_on_ack \in [Slots -> BOOLEAN]
  /\ sweep_cursor \in 0..MaxSlots
  /\ sweep_acct \in {0} \cup Accounts
  /\ sweep_strat \in {0, ALL_ORS_STRATS} \cup Strats

(* Unauthorized `(account_ref, strat_id)` never allocates a slot or mutates session state. *)
AuthorizedSlotAttributionInv ==
  \A k \in Slots :
    (slot_state[k] = "Empty")
      \/ (/\ slot_acct[k] \in Accounts
          /\ slot_strat[k] \in Strats
          /\ slot_acct[k] \in AuthorizedAccounts(slot_strat[k])
          /\ slot_ord_seq[k] >= 1)

(* Exact per-`AccountSession` hold conservation and isolation. *)
AccountHoldConservationInv ==
  \A a \in Accounts :
    /\ acct_hold[a] = Cardinality(SlotsForAccount(a))
    /\ acct_hold[a] <= AcctBudget

(* `next_order_seqnum` is strictly monotonic across worker restarts and covers all live orders. *)
MonotonicOrderSeqFloorInv ==
  /\ \A s \in Strats :
       /\ next_ord_seq[s] >= 1
       /\ next_ord_seq[s] >= high_water_ord_seq[s]
       /\ (~logged_in[s] => last_recv_seq[s] = 0)
  /\ \A k \in Slots :
       slot_state[k] /= "Empty" => slot_ord_seq[k] < next_ord_seq[slot_strat[k]]

(* No two active slots of the same strategy can ever share `order_seqnum`. *)
UniqueActiveOrderSeqInv ==
  \A j, k \in Slots :
    (j /= k /\ slot_state[j] /= "Empty" /\ slot_state[k] /= "Empty"
     /\ slot_strat[j] = slot_strat[k])
      => slot_ord_seq[j] /= slot_ord_seq[k]

(*
 * Ascending sweep prefix + post-sweep completeness:
 *   1. During an active sweep (`sweep_cursor \in 1..MaxSlots`), every already-scanned
 *      slot `j < sweep_cursor` matching `(sweep_acct, sweep_strat)` is either in
 *      `PendingCancel` / `PendingAdminCancel` or in `PendingNew` with `cancel_on_ack = TRUE`.
 *   2. Whenever `sweep_cursor = 0` and `(a, s)` is disabled, NO active order for
 *      `(a, s)` can rest in `Open`, nor in `PendingNew` without `cancel_on_ack`.
 *)
DisabledSweepCompletenessInv ==
  /\ (sweep_cursor \in Slots =>
        \A j \in Slots :
          (j < sweep_cursor /\ slot_state[j] /= "Empty"
           /\ MatchesAdminFilter(slot_acct[j], slot_strat[j], sweep_acct, sweep_strat))
            => (slot_state[j] \in {"PendingCancel", "PendingAdminCancel"}
                \/ (slot_state[j] = "PendingNew" /\ slot_cancel_on_ack[j])))
  /\ (sweep_cursor = 0 =>
        \A k \in Slots :
          (slot_state[k] /= "Empty" /\ slot_strat[k] \notin strat_enabled[slot_acct[k]])
            => (slot_state[k] \in {"PendingCancel", "PendingAdminCancel"}
                \/ (slot_state[k] = "PendingNew" /\ slot_cancel_on_ack[k])))
====
