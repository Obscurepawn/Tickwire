---- MODULE L1_SymOrsSessionRouting ----
(*
 * Snowcat-typed dual-engine (TLC + Apalache SMT `QF_LIA`) specification of
 * unified ORS multi-account session routing (`account_ref = 0..255`), DOP
 * monotonic `max(login_seq, next_order_seqnum, 1)` floor + `last_recv_seqnum`
 * gap rejection, 64-bit `max_order_age_ns` (`DEFAULT_MAX_ORDER_AGE_NS = 23_000`)
 * per-order `RejectStaleOrder` gate, and granular `(account_ref, strat_id)`
 * `set_strat_enabled` + ascending one-order-at-a-time auto-cancel sweep.
 *
 * Dual-engine execution:
 *   - Finite TLC (`BoundedMode = TRUE` in `L1_SymOrsSessionRouting.cfg`):
 *     exhaustive explicit-state verification with 100% nonzero action coverage.
 *   - Symbolic Apalache (`BoundedMode = FALSE` in `ConstInit`):
 *     1. `typecheck` (Snowcat static typing).
 *     2. `inductive --cinit=ConstInit --indinv=IndInv` over unbounded 64-bit
 *        `0 .. 2^64 - 1` timestamps, `max_order_age_ns`, and sequence numbers,
 *        plus symbolic `account_ref \in 0..255` and `strat_id \in 0..254`.
 *     3. `bmc --cinit=ConstInit --inv=SafetyInv --length=8` over full 64-bit `QF_LIA`.
 *
 * Verified production Rust anchors:
 *   - Multi-account `account_ref = 0..255` resolution & authorization:
 *       crates/tickwire-runtime-policy/src/lib.rs:426-453
 *       crates/tickwire-runtime-policy/src/lib.rs:487-513
 *       crates/tickwire-account/src/dop_state.rs:409-427
 *   - `DopState` login sequence floor & `last_recv_seqnum` gap detection:
 *       crates/tickwire-account/src/dop_state.rs:192-243
 *       crates/tickwire-account/src/dop_state.rs:268-314
 *       crates/tickwire-account/src/dop_state.rs:392-406
 *       crates/tickwire-runtime/src/admin.rs:840-906
 *   - 64-bit `max_order_age_ns` (`DEFAULT_MAX_ORDER_AGE_NS = 23_000`) gate:
 *       crates/tickwire-runtime/src/ors.rs:61
 *       crates/tickwire-runtime/src/ors.rs:1554-1621
 *   - Granular `set_strat_enabled` & ascending one-order-at-a-time sweep:
 *       crates/tickwire-account/src/dop_state.rs:336-389
 *       crates/tickwire-account/src/order_manager.rs:1171-1333
 *       crates/tickwire-account/src/order_manager.rs:2271-2383
 *       crates/tickwire-runtime/src/admin.rs:450-468
 *       crates/tickwire-runtime/src/ors.rs:2205-2218
 *       crates/tickwire-runtime/src/ors.rs:2279-2336
 *)
EXTENDS Integers

CONSTANTS
  (* @type: Bool; *)
  BoundedMode,
  (* @type: Int; *)
  SymStrat,
  (* @type: Int; *)
  SymAcctA,
  (* @type: Int; *)
  SymAcctB,
  (* @type: Int; *)
  MaxOrderAgeNs,
  (* @type: Int; *)
  BoundMaxSeq,
  (* @type: Int; *)
  BoundU64Max

VARIABLES
  (* @type: Bool; *)
  logged_in,
  (* @type: Int; *)
  restarts_used,
  (* @type: Int; *)
  last_recv_seq,
  (* @type: Int; *)
  next_ord_seq,
  (* @type: Int; *)
  floor_ord_seq,
  (* @type: Bool; *)
  seq_gap_rejected,
  (* @type: Bool; *)
  enabled_acct_a,
  (* @type: Bool; *)
  enabled_acct_b,
  (* @type: Int; *)
  admin_ops_used,
  (* @type: Int; *)
  now_ns,
  (* @type: Int; *)
  hold_acct_a,
  (* @type: Int; *)
  hold_acct_b,
  (* @type: Str; *)
  slot1_state,
  (* @type: Int; *)
  slot1_acct,
  (* @type: Int; *)
  slot1_ord_seq,
  (* @type: Int; *)
  slot1_age_ns,
  (* @type: Bool; *)
  slot1_cancel_on_ack,
  (* @type: Bool; *)
  slot1_admin_cancel,
  (* @type: Str; *)
  slot2_state,
  (* @type: Int; *)
  slot2_acct,
  (* @type: Int; *)
  slot2_ord_seq,
  (* @type: Int; *)
  slot2_age_ns,
  (* @type: Bool; *)
  slot2_cancel_on_ack,
  (* @type: Bool; *)
  slot2_admin_cancel,
  (* @type: Int; *)
  sweep_cursor,
  (* @type: Bool; *)
  sweep_targets_a

vars == <<
  logged_in, restarts_used, last_recv_seq, next_ord_seq, floor_ord_seq,
  seq_gap_rejected, enabled_acct_a, enabled_acct_b, admin_ops_used, now_ns,
  hold_acct_a, hold_acct_b,
  slot1_state, slot1_acct, slot1_ord_seq, slot1_age_ns, slot1_cancel_on_ack, slot1_admin_cancel,
  slot2_state, slot2_acct, slot2_ord_seq, slot2_age_ns, slot2_cancel_on_ack, slot2_admin_cancel,
  sweep_cursor, sweep_targets_a
>>

slot2Vars == <<
  slot2_state, slot2_acct, slot2_ord_seq, slot2_age_ns, slot2_cancel_on_ack, slot2_admin_cancel
>>

slot1Vars == <<
  slot1_state, slot1_acct, slot1_ord_seq, slot1_age_ns, slot1_cancel_on_ack, slot1_admin_cancel
>>

(*
 * SANY-safe 64-bit upper bounds (`2^30 = 1073741824 <= 2147483647`):
 *   - `SymSeqLimit = 8 * 2^60 = 2^63`
 *   - `SymU64Max   = 16 * 2^60 - 1 = 2^64 - 1`
 *   - `DefaultMaxOrderAgeNs = 23000` (`DEFAULT_MAX_ORDER_AGE_NS = 23_000`)
 *)
(* @type: Int; *)
Pow30 == 1073741824

(* @type: Int; *)
SymSeqLimit == 8 * Pow30 * Pow30

(* @type: Int; *)
SymU64Max == (16 * Pow30 * Pow30) - 1

(* @type: Int; *)
DefaultMaxOrderAgeNs == 23000

(* @type: Int; *)
AllOrsStrats == 255

(* @type: Int; *)
EffSeqLimit == IF BoundedMode THEN BoundMaxSeq ELSE SymSeqLimit

(* @type: Int; *)
EffU64Max == IF BoundedMode THEN BoundU64Max ELSE SymU64Max

(* Small finite domains evaluated ONLY when `BoundedMode = TRUE` in TLC. *)
(* @type: Set(Int); *)
FiniteAcctReqSet == {0, 1, 2, 3}

(* @type: Set(Int); *)
FiniteAdminAcctSet == {0, 1, 2}

(* @type: Set(Int); *)
FiniteAdminStratSet == {1, 255}

(* @type: (Int, Int) => Int; *)
Max2(a, b) == IF a >= b THEN a ELSE b

(* Exact `u64::saturating_sub` (`crates/tickwire-runtime/src/ors.rs:1609`). *)
(* @type: (Int, Int) => Int; *)
SaturatingSubU64(a, b) == IF a > b THEN a - b ELSE 0

(*
 * Exact `resolve_account_ref` for `SymStrat` (`default = SymAcctA`, authorized = `{SymAcctA}`,
 * sibling registered account `SymAcctB` unauthorized):
 *   > 0 : `SymAcctA`
 *   -1  : `INVALID_ACCOUNT_ID` (`req_acct` unregistered)
 *   -2  : `REJECT_ROUTING_ID_MISMATCH` (`req_acct = SymAcctB` unauthorized)
 *)
(* @type: Int => Int; *)
ResolveAccountSym(req_acct) ==
  IF req_acct = 0 THEN SymAcctA
  ELSE IF req_acct = SymAcctA THEN SymAcctA
  ELSE IF req_acct = SymAcctB THEN -2
  ELSE -1

(* Exact `DopState::check_msg_seqnum` (`crates/tickwire-account/src/dop_state.rs:392-406`). *)
(* @type: Int => Bool; *)
ValidMsgSeqSym(mseq) ==
  /\ mseq >= 1
  /\ mseq <= EffU64Max
  /\ (last_recv_seq = 0 \/ mseq = last_recv_seq + 1)

(* Exact stale-order predicate (`crates/tickwire-runtime/src/ors.rs:1607-1610`). *)
(* @type: (Int, Int) => Bool; *)
IsStaleOrder(now_val, create_val) ==
  /\ MaxOrderAgeNs > 0
  /\ create_val > 0
  /\ SaturatingSubU64(now_val, create_val) > MaxOrderAgeNs

(* @type: (Int, Int) => Bool; *)
MatchesAcctA(acct_arg, strat_arg) ==
  /\ (acct_arg = 0 \/ acct_arg = SymAcctA)
  /\ (strat_arg = AllOrsStrats \/ strat_arg = SymStrat)

(* @type: (Int, Int) => Bool; *)
MatchesAcctB(acct_arg, strat_arg) ==
  /\ (acct_arg = 0 \/ acct_arg = SymAcctB)
  /\ (strat_arg = AllOrsStrats \/ strat_arg = SymStrat)

(* Symbolic constant initialization for Apalache (`--cinit=ConstInit`). *)
ConstInit ==
  /\ BoundedMode = FALSE
  /\ SymStrat \in Int /\ SymStrat >= 0 /\ SymStrat <= 254
  /\ SymAcctA \in Int /\ SymAcctA >= 1 /\ SymAcctA <= 255
  /\ SymAcctB \in Int /\ SymAcctB >= 1 /\ SymAcctB <= 255 /\ SymAcctB /= SymAcctA
  /\ MaxOrderAgeNs \in Int /\ MaxOrderAgeNs >= 0 /\ MaxOrderAgeNs <= SymU64Max
  /\ BoundMaxSeq = 2
  /\ BoundU64Max = 3

Init ==
  /\ logged_in = TRUE
  /\ restarts_used = 0
  /\ last_recv_seq = 0
  /\ next_ord_seq = 1
  /\ floor_ord_seq = 1
  /\ seq_gap_rejected = FALSE
  /\ enabled_acct_a = TRUE
  /\ enabled_acct_b = TRUE
  /\ admin_ops_used = 0
  /\ IF BoundedMode
     THEN now_ns = 1
     ELSE /\ now_ns \in Int
          /\ now_ns >= 0
          /\ now_ns <= EffU64Max
  /\ hold_acct_a = 0
  /\ hold_acct_b = 0
  /\ slot1_state = "Empty"
  /\ slot1_acct = 0
  /\ slot1_ord_seq = 0
  /\ slot1_age_ns = 0
  /\ slot1_cancel_on_ack = FALSE
  /\ slot1_admin_cancel = FALSE
  /\ slot2_state = "Empty"
  /\ slot2_acct = 0
  /\ slot2_ord_seq = 0
  /\ slot2_age_ns = 0
  /\ slot2_cancel_on_ack = FALSE
  /\ slot2_admin_cancel = FALSE
  /\ sweep_cursor = 0
  /\ sweep_targets_a = FALSE

(* Monotonic 64-bit clock advance (`0 .. 2^64 - 1`). *)
AdvanceClock ==
  /\ IF BoundedMode
     THEN /\ now_ns < BoundU64Max
          /\ now_ns' = BoundU64Max
     ELSE \E nxt_ns \in Int :
            /\ nxt_ns > now_ns
            /\ nxt_ns <= EffU64Max
            /\ now_ns' = nxt_ns
  /\ UNCHANGED <<
       logged_in, restarts_used, last_recv_seq, next_ord_seq, floor_ord_seq,
       seq_gap_rejected, enabled_acct_a, enabled_acct_b, admin_ops_used,
       hold_acct_a, hold_acct_b, slot1Vars, slot2Vars, sweep_cursor, sweep_targets_a
     >>

(* Worker disconnect / logout (`crates/tickwire-account/src/dop_state.rs:215-227`). *)
DopLogout ==
  /\ logged_in
  /\ (~BoundedMode \/ restarts_used < 1)
  /\ logged_in' = FALSE
  /\ restarts_used' = restarts_used + 1
  /\ last_recv_seq' = 0
  /\ seq_gap_rejected' = FALSE
  /\ UNCHANGED <<
       next_ord_seq, floor_ord_seq, enabled_acct_a, enabled_acct_b, admin_ops_used,
       now_ns, hold_acct_a, hold_acct_b, slot1Vars, slot2Vars, sweep_cursor, sweep_targets_a
     >>

(*
 * Worker reconnect / DOP login (`crates/tickwire-account/src/dop_state.rs:192-212`,
 * `crates/tickwire-runtime/src/admin.rs:840-906`): enforces monotonic
 * `max(next_ord_seq, login_seq, 1)` across restarts and resets `last_recv_seq = 0`.
 *)
DopLogin ==
  /\ ~logged_in
  /\ IF BoundedMode
     THEN \E lseq \in 0..BoundMaxSeq :
            LET flr == Max2(next_ord_seq, Max2(lseq, 1)) IN
            /\ logged_in' = TRUE
            /\ last_recv_seq' = 0
            /\ next_ord_seq' = flr
            /\ floor_ord_seq' = Max2(floor_ord_seq, flr)
            /\ seq_gap_rejected' = FALSE
     ELSE \E lseq \in Int :
            /\ lseq >= 0
            /\ lseq <= EffSeqLimit
            /\ LET flr == Max2(next_ord_seq, Max2(lseq, 1)) IN
               /\ logged_in' = TRUE
               /\ last_recv_seq' = 0
               /\ next_ord_seq' = flr
               /\ floor_ord_seq' = Max2(floor_ord_seq, flr)
               /\ seq_gap_rejected' = FALSE
  /\ UNCHANGED <<
       restarts_used, enabled_acct_a, enabled_acct_b, admin_ops_used,
       now_ns, hold_acct_a, hold_acct_b, slot1Vars, slot2Vars, sweep_cursor, sweep_targets_a
     >>

(*
 * Admitted `NewOrder` into deterministic first free slot (`slot1`, else `slot2`).
 *)
SubmitNewOrderAdmit ==
  /\ logged_in
  /\ enabled_acct_a
  /\ (slot1_state = "Empty" \/ slot2_state = "Empty")
  /\ IF BoundedMode
     THEN \E req_acct \in {0, 1}, mseq \in 1..BoundMaxSeq,
             oseq \in 1..BoundMaxSeq, create_ts \in 1..BoundU64Max :
            /\ ResolveAccountSym(req_acct) = SymAcctA
            /\ ValidMsgSeqSym(mseq)
            /\ oseq >= next_ord_seq
            /\ create_ts <= now_ns
            /\ ~IsStaleOrder(now_ns, create_ts)
            /\ LET age == SaturatingSubU64(now_ns, create_ts)
                   nxt == oseq + 1
               IN
               /\ last_recv_seq' = mseq
               /\ next_ord_seq' = nxt
               /\ floor_ord_seq' = Max2(floor_ord_seq, nxt)
               /\ seq_gap_rejected' = FALSE
               /\ hold_acct_a' = hold_acct_a + 1
               /\ IF slot1_state = "Empty"
                  THEN /\ slot1_state' = "PendingNew"
                       /\ slot1_acct' = SymAcctA
                       /\ slot1_ord_seq' = oseq
                       /\ slot1_age_ns' = age
                       /\ slot1_cancel_on_ack' = FALSE
                       /\ slot1_admin_cancel' = FALSE
                       /\ UNCHANGED slot2Vars
                  ELSE /\ slot2_state' = "PendingNew"
                       /\ slot2_acct' = SymAcctA
                       /\ slot2_ord_seq' = oseq
                       /\ slot2_age_ns' = age
                       /\ slot2_cancel_on_ack' = FALSE
                       /\ slot2_admin_cancel' = FALSE
                       /\ UNCHANGED slot1Vars
     ELSE \E req_acct \in Int, mseq \in Int, oseq \in Int, create_ts \in Int :
            /\ req_acct >= 0 /\ req_acct <= 255
            /\ ResolveAccountSym(req_acct) = SymAcctA
            /\ ValidMsgSeqSym(mseq)
            /\ oseq >= next_ord_seq /\ oseq <= EffSeqLimit
            /\ create_ts >= 1 /\ create_ts <= EffU64Max
            /\ ~IsStaleOrder(now_ns, create_ts)
            /\ LET age == SaturatingSubU64(now_ns, create_ts)
                   nxt == oseq + 1
               IN
               /\ last_recv_seq' = mseq
               /\ next_ord_seq' = nxt
               /\ floor_ord_seq' = Max2(floor_ord_seq, nxt)
               /\ seq_gap_rejected' = FALSE
               /\ hold_acct_a' = hold_acct_a + 1
               /\ IF slot1_state = "Empty"
                  THEN /\ slot1_state' = "PendingNew"
                       /\ slot1_acct' = SymAcctA
                       /\ slot1_ord_seq' = oseq
                       /\ slot1_age_ns' = age
                       /\ slot1_cancel_on_ack' = FALSE
                       /\ slot1_admin_cancel' = FALSE
                       /\ UNCHANGED slot2Vars
                  ELSE /\ slot2_state' = "PendingNew"
                       /\ slot2_acct' = SymAcctA
                       /\ slot2_ord_seq' = oseq
                       /\ slot2_age_ns' = age
                       /\ slot2_cancel_on_ack' = FALSE
                       /\ slot2_admin_cancel' = FALSE
                       /\ UNCHANGED slot1Vars
  /\ UNCHANGED <<
       logged_in, restarts_used, enabled_acct_a, enabled_acct_b, admin_ops_used,
       now_ns, hold_acct_b, sweep_cursor, sweep_targets_a
     >>

(*
 * Unauthorized / unregistered `account_ref` refusal (`INVALID_ACCOUNT_ID` or
 * `REJECT_ROUTING_ID_MISMATCH`): zero `AccountSession` or `next_ord_seq` mutation.
 *)
RejectUnauthorizedAccount ==
  /\ logged_in
  /\ IF BoundedMode
     THEN \E req_acct \in FiniteAcctReqSet, mseq \in 1..BoundMaxSeq :
            /\ ResolveAccountSym(req_acct) < 0
            /\ ValidMsgSeqSym(mseq)
            /\ last_recv_seq' = mseq
            /\ seq_gap_rejected' = FALSE
     ELSE \E req_acct \in Int, mseq \in Int :
            /\ req_acct >= 0 /\ req_acct <= 255
            /\ ResolveAccountSym(req_acct) < 0
            /\ ValidMsgSeqSym(mseq)
            /\ last_recv_seq' = mseq
            /\ seq_gap_rejected' = FALSE
  /\ UNCHANGED <<
       logged_in, restarts_used, next_ord_seq, floor_ord_seq,
       enabled_acct_a, enabled_acct_b, admin_ops_used, now_ns,
       hold_acct_a, hold_acct_b, slot1Vars, slot2Vars, sweep_cursor, sweep_targets_a
     >>

(* `UNEXPECTED_MSG_SEQNUM` refusal without advancing `last_recv_seq`. *)
RejectMsgSeqGap ==
  /\ logged_in
  /\ ~seq_gap_rejected
  /\ IF BoundedMode
     THEN \E mseq \in 0..BoundMaxSeq :
            ~ValidMsgSeqSym(mseq)
     ELSE \E mseq \in Int :
            /\ mseq >= 0 /\ mseq <= EffU64Max
            /\ ~ValidMsgSeqSym(mseq)
  /\ seq_gap_rejected' = TRUE
  /\ UNCHANGED <<
       logged_in, restarts_used, last_recv_seq, next_ord_seq, floor_ord_seq,
       enabled_acct_a, enabled_acct_b, admin_ops_used, now_ns,
       hold_acct_a, hold_acct_b, slot1Vars, slot2Vars, sweep_cursor, sweep_targets_a
     >>

(* `RESTRICTED_CANCEL_ONLY` refusal on disabled `(SymAcctA, SymStrat)`. *)
RejectDisabledStratNewOrder ==
  /\ logged_in
  /\ ~enabled_acct_a
  /\ IF BoundedMode
     THEN \E req_acct \in {0, 1}, mseq \in 1..BoundMaxSeq, oseq \in 1..BoundMaxSeq :
            /\ ResolveAccountSym(req_acct) = SymAcctA
            /\ ValidMsgSeqSym(mseq)
            /\ LET nxt == Max2(next_ord_seq, oseq + 1) IN
               /\ last_recv_seq' = mseq
               /\ next_ord_seq' = nxt
               /\ floor_ord_seq' = Max2(floor_ord_seq, nxt)
               /\ seq_gap_rejected' = FALSE
     ELSE \E req_acct \in Int, mseq \in Int, oseq \in Int :
            /\ req_acct >= 0 /\ req_acct <= 255
            /\ ResolveAccountSym(req_acct) = SymAcctA
            /\ ValidMsgSeqSym(mseq)
            /\ oseq >= 1 /\ oseq <= EffSeqLimit
            /\ LET nxt == Max2(next_ord_seq, oseq + 1) IN
               /\ last_recv_seq' = mseq
               /\ next_ord_seq' = nxt
               /\ floor_ord_seq' = Max2(floor_ord_seq, nxt)
               /\ seq_gap_rejected' = FALSE
  /\ UNCHANGED <<
       logged_in, restarts_used, enabled_acct_a, enabled_acct_b, admin_ops_used,
       now_ns, hold_acct_a, hold_acct_b, slot1Vars, slot2Vars, sweep_cursor, sweep_targets_a
     >>

(*
 * 64-bit `max_order_age_ns` (`DEFAULT_MAX_ORDER_AGE_NS = 23_000`) `RejectStaleOrder`
 * gate (`crates/tickwire-runtime/src/ors.rs:1607-1621`): rejects per-order without
 * freezing `enabled_acct_a` or mutating `hold_acct_a`.
 *)
RejectStaleNewOrder ==
  /\ logged_in
  /\ enabled_acct_a
  /\ IF BoundedMode
     THEN \E req_acct \in {0, 1}, mseq \in 1..BoundMaxSeq,
             oseq \in 1..BoundMaxSeq, create_ts \in 1..BoundU64Max :
            /\ ResolveAccountSym(req_acct) = SymAcctA
            /\ ValidMsgSeqSym(mseq)
            /\ create_ts <= now_ns
            /\ IsStaleOrder(now_ns, create_ts)
            /\ LET nxt == Max2(next_ord_seq, oseq + 1) IN
               /\ last_recv_seq' = mseq
               /\ next_ord_seq' = nxt
               /\ floor_ord_seq' = Max2(floor_ord_seq, nxt)
               /\ seq_gap_rejected' = FALSE
     ELSE \E req_acct \in Int, mseq \in Int, oseq \in Int, create_ts \in Int :
            /\ req_acct >= 0 /\ req_acct <= 255
            /\ ResolveAccountSym(req_acct) = SymAcctA
            /\ ValidMsgSeqSym(mseq)
            /\ oseq >= 1 /\ oseq <= EffSeqLimit
            /\ create_ts >= 1 /\ create_ts <= EffU64Max
            /\ IsStaleOrder(now_ns, create_ts)
            /\ LET nxt == Max2(next_ord_seq, oseq + 1) IN
               /\ last_recv_seq' = mseq
               /\ next_ord_seq' = nxt
               /\ floor_ord_seq' = Max2(floor_ord_seq, nxt)
               /\ seq_gap_rejected' = FALSE
  /\ UNCHANGED <<
       logged_in, restarts_used, enabled_acct_a, enabled_acct_b, admin_ops_used,
       now_ns, hold_acct_a, hold_acct_b, slot1Vars, slot2Vars, sweep_cursor, sweep_targets_a
     >>

(* Strategy cancel permitted even when `~enabled_acct_a`. *)
SubmitStrategyCancel ==
  /\ logged_in
  /\ IF BoundedMode
     THEN \E req_acct \in {0, 1}, mseq \in 1..BoundMaxSeq, which \in {1, 2} :
            /\ ResolveAccountSym(req_acct) = SymAcctA
            /\ ValidMsgSeqSym(mseq)
            /\ last_recv_seq' = mseq
            /\ seq_gap_rejected' = FALSE
            /\ IF which = 1
               THEN /\ slot1_state \in {"PendingNew", "Open"}
                    /\ ~(slot1_state = "PendingNew" /\ slot1_cancel_on_ack)
                    /\ IF slot1_state = "PendingNew"
                       THEN /\ slot1_cancel_on_ack' = TRUE
                            /\ UNCHANGED slot1_state
                       ELSE /\ slot1_state' = "PendingCancel"
                            /\ UNCHANGED slot1_cancel_on_ack
                    /\ UNCHANGED <<slot1_acct, slot1_ord_seq, slot1_age_ns, slot1_admin_cancel, slot2Vars>>
               ELSE /\ slot2_state \in {"PendingNew", "Open"}
                    /\ ~(slot2_state = "PendingNew" /\ slot2_cancel_on_ack)
                    /\ IF slot2_state = "PendingNew"
                       THEN /\ slot2_cancel_on_ack' = TRUE
                            /\ UNCHANGED slot2_state
                       ELSE /\ slot2_state' = "PendingCancel"
                            /\ UNCHANGED slot2_cancel_on_ack
                    /\ UNCHANGED <<slot2_acct, slot2_ord_seq, slot2_age_ns, slot2_admin_cancel, slot1Vars>>
     ELSE \E req_acct \in Int, mseq \in Int, which \in {1, 2} :
            /\ req_acct >= 0 /\ req_acct <= 255
            /\ ResolveAccountSym(req_acct) = SymAcctA
            /\ ValidMsgSeqSym(mseq)
            /\ last_recv_seq' = mseq
            /\ seq_gap_rejected' = FALSE
            /\ IF which = 1
               THEN /\ slot1_state \in {"PendingNew", "Open"}
                    /\ ~(slot1_state = "PendingNew" /\ slot1_cancel_on_ack)
                    /\ IF slot1_state = "PendingNew"
                       THEN /\ slot1_cancel_on_ack' = TRUE
                            /\ UNCHANGED slot1_state
                       ELSE /\ slot1_state' = "PendingCancel"
                            /\ UNCHANGED slot1_cancel_on_ack
                    /\ UNCHANGED <<slot1_acct, slot1_ord_seq, slot1_age_ns, slot1_admin_cancel, slot2Vars>>
               ELSE /\ slot2_state \in {"PendingNew", "Open"}
                    /\ ~(slot2_state = "PendingNew" /\ slot2_cancel_on_ack)
                    /\ IF slot2_state = "PendingNew"
                       THEN /\ slot2_cancel_on_ack' = TRUE
                            /\ UNCHANGED slot2_state
                       ELSE /\ slot2_state' = "PendingCancel"
                            /\ UNCHANGED slot2_cancel_on_ack
                    /\ UNCHANGED <<slot2_acct, slot2_ord_seq, slot2_age_ns, slot2_admin_cancel, slot1Vars>>
  /\ UNCHANGED <<
       logged_in, restarts_used, next_ord_seq, floor_ord_seq,
       enabled_acct_a, enabled_acct_b, admin_ops_used, now_ns,
       hold_acct_a, hold_acct_b, sweep_cursor, sweep_targets_a
     >>

(*
 * Granular `set_strat_enabled(acct_arg, strat_arg, false)` + start ascending scan
 * (`acct_arg = 0` wildcard across accounts; `strat_arg = 255` wildcard across strategies).
 *)
AdminDisableAndStartSweep ==
  /\ sweep_cursor = 0
  /\ (~BoundedMode \/ admin_ops_used < 2)
  /\ IF BoundedMode
     THEN \E acct_arg \in FiniteAdminAcctSet, strat_arg \in FiniteAdminStratSet :
            LET hitA == MatchesAcctA(acct_arg, strat_arg)
                hitB == MatchesAcctB(acct_arg, strat_arg)
            IN
            /\ (hitA /\ enabled_acct_a) \/ (hitB /\ enabled_acct_b)
            /\ enabled_acct_a' = (IF hitA THEN FALSE ELSE enabled_acct_a)
            /\ enabled_acct_b' = (IF hitB THEN FALSE ELSE enabled_acct_b)
            /\ admin_ops_used' = admin_ops_used + 1
            /\ sweep_cursor' = 1
            /\ sweep_targets_a' = hitA
     ELSE \E acct_arg \in {0, SymAcctA, SymAcctB}, strat_arg \in {SymStrat, AllOrsStrats} :
            LET hitA == MatchesAcctA(acct_arg, strat_arg)
                hitB == MatchesAcctB(acct_arg, strat_arg)
            IN
            /\ (hitA /\ enabled_acct_a) \/ (hitB /\ enabled_acct_b)
            /\ enabled_acct_a' = (IF hitA THEN FALSE ELSE enabled_acct_a)
            /\ enabled_acct_b' = (IF hitB THEN FALSE ELSE enabled_acct_b)
            /\ admin_ops_used' = admin_ops_used + 1
            /\ sweep_cursor' = 1
            /\ sweep_targets_a' = hitA
  /\ UNCHANGED <<
       logged_in, restarts_used, last_recv_seq, next_ord_seq, floor_ord_seq,
       seq_gap_rejected, now_ns, hold_acct_a, hold_acct_b, slot1Vars, slot2Vars
     >>

(*
 * Ascending one-order-at-a-time auto-cancel sweep (`sweep_cursor = 1` then `2`).
 *)
AdminSweepNextSlot ==
  /\ sweep_cursor \in {1, 2}
  /\ IF sweep_cursor = 1
     THEN /\ IF sweep_targets_a /\ slot1_state = "PendingNew"
             THEN /\ slot1_cancel_on_ack' = TRUE
                  /\ slot1_admin_cancel' = TRUE
                  /\ UNCHANGED <<slot1_state, slot1_acct, slot1_ord_seq, slot1_age_ns>>
             ELSE IF sweep_targets_a /\ slot1_state = "Open"
             THEN /\ slot1_state' = "PendingCancel"
                  /\ slot1_admin_cancel' = TRUE
                  /\ UNCHANGED <<slot1_acct, slot1_ord_seq, slot1_age_ns, slot1_cancel_on_ack>>
             ELSE UNCHANGED slot1Vars
          /\ UNCHANGED slot2Vars
          /\ sweep_cursor' = 2
          /\ UNCHANGED sweep_targets_a
     ELSE /\ IF sweep_targets_a /\ slot2_state = "PendingNew"
             THEN /\ slot2_cancel_on_ack' = TRUE
                  /\ slot2_admin_cancel' = TRUE
                  /\ UNCHANGED <<slot2_state, slot2_acct, slot2_ord_seq, slot2_age_ns>>
             ELSE IF sweep_targets_a /\ slot2_state = "Open"
             THEN /\ slot2_state' = "PendingCancel"
                  /\ slot2_admin_cancel' = TRUE
                  /\ UNCHANGED <<slot2_acct, slot2_ord_seq, slot2_age_ns, slot2_cancel_on_ack>>
             ELSE UNCHANGED slot2Vars
          /\ UNCHANGED slot1Vars
          /\ sweep_cursor' = 0
          /\ sweep_targets_a' = FALSE
  /\ UNCHANGED <<
       logged_in, restarts_used, last_recv_seq, next_ord_seq, floor_ord_seq,
       seq_gap_rejected, enabled_acct_a, enabled_acct_b, admin_ops_used,
       now_ns, hold_acct_a, hold_acct_b
     >>

(* Admin `set_strat_enabled(..., true)`. *)
AdminEnableStrat ==
  /\ sweep_cursor = 0
  /\ (~BoundedMode \/ admin_ops_used < 2)
  /\ IF BoundedMode
     THEN \E acct_arg \in FiniteAdminAcctSet, strat_arg \in FiniteAdminStratSet :
            LET hitA == MatchesAcctA(acct_arg, strat_arg)
                hitB == MatchesAcctB(acct_arg, strat_arg)
            IN
            /\ (hitA /\ ~enabled_acct_a) \/ (hitB /\ ~enabled_acct_b)
            /\ enabled_acct_a' = (IF hitA THEN TRUE ELSE enabled_acct_a)
            /\ enabled_acct_b' = (IF hitB THEN TRUE ELSE enabled_acct_b)
            /\ admin_ops_used' = admin_ops_used + 1
     ELSE \E acct_arg \in {0, SymAcctA, SymAcctB}, strat_arg \in {SymStrat, AllOrsStrats} :
            LET hitA == MatchesAcctA(acct_arg, strat_arg)
                hitB == MatchesAcctB(acct_arg, strat_arg)
            IN
            /\ (hitA /\ ~enabled_acct_a) \/ (hitB /\ ~enabled_acct_b)
            /\ enabled_acct_a' = (IF hitA THEN TRUE ELSE enabled_acct_a)
            /\ enabled_acct_b' = (IF hitB THEN TRUE ELSE enabled_acct_b)
            /\ admin_ops_used' = admin_ops_used + 1
  /\ UNCHANGED <<
       logged_in, restarts_used, last_recv_seq, next_ord_seq, floor_ord_seq,
       seq_gap_rejected, now_ns, hold_acct_a, hold_acct_b,
       slot1Vars, slot2Vars, sweep_cursor, sweep_targets_a
     >>

(* Venue `Ack` honoring `cancel_on_ack` (`ADMIN_ACTION` / strategy cancel). *)
VenueAckOrder ==
  /\ \E which \in {1, 2} :
       IF which = 1
       THEN /\ slot1_state = "PendingNew"
            /\ slot1_state' = (IF slot1_cancel_on_ack THEN "PendingCancel" ELSE "Open")
            /\ slot1_cancel_on_ack' = FALSE
            /\ UNCHANGED <<slot1_acct, slot1_ord_seq, slot1_age_ns, slot1_admin_cancel, slot2Vars>>
       ELSE /\ slot2_state = "PendingNew"
            /\ slot2_state' = (IF slot2_cancel_on_ack THEN "PendingCancel" ELSE "Open")
            /\ slot2_cancel_on_ack' = FALSE
            /\ UNCHANGED <<slot2_acct, slot2_ord_seq, slot2_age_ns, slot2_admin_cancel, slot1Vars>>
  /\ UNCHANGED <<
       logged_in, restarts_used, last_recv_seq, next_ord_seq, floor_ord_seq,
       seq_gap_rejected, enabled_acct_a, enabled_acct_b, admin_ops_used,
       now_ns, hold_acct_a, hold_acct_b, sweep_cursor, sweep_targets_a
     >>

(* Venue terminal completion (`Filled` / `Canceled`). *)
VenueTerminalRelease ==
  /\ \E which \in {1, 2} :
       IF which = 1
       THEN /\ slot1_state \in {"Open", "PendingCancel"}
            /\ hold_acct_a' = hold_acct_a - 1
            /\ slot1_state' = "Empty"
            /\ slot1_acct' = 0
            /\ slot1_ord_seq' = 0
            /\ slot1_age_ns' = 0
            /\ slot1_cancel_on_ack' = FALSE
            /\ slot1_admin_cancel' = FALSE
            /\ UNCHANGED slot2Vars
       ELSE /\ slot2_state \in {"Open", "PendingCancel"}
            /\ hold_acct_a' = hold_acct_a - 1
            /\ slot2_state' = "Empty"
            /\ slot2_acct' = 0
            /\ slot2_ord_seq' = 0
            /\ slot2_age_ns' = 0
            /\ slot2_cancel_on_ack' = FALSE
            /\ slot2_admin_cancel' = FALSE
            /\ UNCHANGED slot1Vars
  /\ UNCHANGED <<
       logged_in, restarts_used, last_recv_seq, next_ord_seq, floor_ord_seq,
       seq_gap_rejected, enabled_acct_a, enabled_acct_b, admin_ops_used,
       now_ns, hold_acct_b, sweep_cursor, sweep_targets_a
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
(* Safety & Inductive Invariants                                             *)
(* ========================================================================= *)

TypeOK ==
  /\ logged_in \in BOOLEAN
  /\ restarts_used \in Int /\ restarts_used >= 0
  /\ last_recv_seq \in Int /\ last_recv_seq >= 0 /\ last_recv_seq <= EffU64Max
  /\ next_ord_seq \in Int /\ next_ord_seq >= 1 /\ next_ord_seq <= EffSeqLimit + 1
  /\ floor_ord_seq \in Int /\ floor_ord_seq >= 1 /\ floor_ord_seq <= EffSeqLimit + 1
  /\ seq_gap_rejected \in BOOLEAN
  /\ enabled_acct_a \in BOOLEAN
  /\ enabled_acct_b \in BOOLEAN
  /\ admin_ops_used \in Int /\ admin_ops_used >= 0
  /\ now_ns \in Int /\ now_ns >= 0 /\ now_ns <= EffU64Max
  /\ hold_acct_a \in Int /\ hold_acct_a >= 0 /\ hold_acct_a <= 2
  /\ hold_acct_b \in Int /\ hold_acct_b = 0
  /\ slot1_state \in {"Empty", "PendingNew", "Open", "PendingCancel"}
  /\ slot1_acct \in Int /\ slot1_acct >= 0 /\ slot1_acct <= 255
  /\ slot1_ord_seq \in Int /\ slot1_ord_seq >= 0 /\ slot1_ord_seq <= EffSeqLimit
  /\ slot1_age_ns \in Int /\ slot1_age_ns >= 0 /\ slot1_age_ns <= EffU64Max
  /\ slot1_cancel_on_ack \in BOOLEAN
  /\ slot1_admin_cancel \in BOOLEAN
  /\ slot2_state \in {"Empty", "PendingNew", "Open", "PendingCancel"}
  /\ slot2_acct \in Int /\ slot2_acct >= 0 /\ slot2_acct <= 255
  /\ slot2_ord_seq \in Int /\ slot2_ord_seq >= 0 /\ slot2_ord_seq <= EffSeqLimit
  /\ slot2_age_ns \in Int /\ slot2_age_ns >= 0 /\ slot2_age_ns <= EffU64Max
  /\ slot2_cancel_on_ack \in BOOLEAN
  /\ slot2_admin_cancel \in BOOLEAN
  /\ sweep_cursor \in {0, 1, 2}
  /\ sweep_targets_a \in BOOLEAN

(* @type: (Str, Bool) => Bool; *)
SlotIsCancelingOrEmpty(st, coa) ==
  \/ st = "Empty"
  \/ st = "PendingCancel"
  \/ (st = "PendingNew" /\ coa)

AuthorizedAndHoldInv ==
  /\ hold_acct_b = 0
  /\ hold_acct_a =
       (IF slot1_state /= "Empty" THEN 1 ELSE 0)
       + (IF slot2_state /= "Empty" THEN 1 ELSE 0)
  /\ (slot1_state /= "Empty" => slot1_acct = SymAcctA)
  /\ (slot2_state /= "Empty" => slot2_acct = SymAcctA)

MonotonicDopSeqInv ==
  /\ next_ord_seq >= 1
  /\ floor_ord_seq >= 1
  /\ next_ord_seq >= floor_ord_seq
  /\ (~logged_in => last_recv_seq = 0)
  /\ (slot1_state /= "Empty" => (slot1_ord_seq >= 1 /\ slot1_ord_seq < next_ord_seq))
  /\ (slot2_state /= "Empty" => (slot2_ord_seq >= 1 /\ slot2_ord_seq < next_ord_seq))
  /\ ((slot1_state /= "Empty" /\ slot2_state /= "Empty") => slot1_ord_seq /= slot2_ord_seq)

StaleOrderGateInv ==
  /\ (slot1_state /= "Empty" /\ MaxOrderAgeNs > 0) => slot1_age_ns <= MaxOrderAgeNs
  /\ (slot2_state /= "Empty" /\ MaxOrderAgeNs > 0) => slot2_age_ns <= MaxOrderAgeNs

DisabledSweepInv ==
  /\ (sweep_cursor = 2 /\ sweep_targets_a) =>
       SlotIsCancelingOrEmpty(slot1_state, slot1_cancel_on_ack)
  /\ (sweep_cursor = 0 /\ ~enabled_acct_a) =>
       (/\ SlotIsCancelingOrEmpty(slot1_state, slot1_cancel_on_ack)
        /\ SlotIsCancelingOrEmpty(slot2_state, slot2_cancel_on_ack))

SafetyInv ==
  /\ TypeOK
  /\ AuthorizedAndHoldInv
  /\ MonotonicDopSeqInv
  /\ StaleOrderGateInv
  /\ DisabledSweepInv

(* Strengthened inductive invariant `IndInv` proved over unbounded 64-bit `QF_LIA`. *)
IndInv ==
  /\ SafetyInv
  /\ (slot1_state = "Empty" =>
       (slot1_acct = 0 /\ slot1_ord_seq = 0 /\ slot1_age_ns = 0
        /\ ~slot1_cancel_on_ack /\ ~slot1_admin_cancel))
  /\ (slot2_state = "Empty" =>
       (slot2_acct = 0 /\ slot2_ord_seq = 0 /\ slot2_age_ns = 0
        /\ ~slot2_cancel_on_ack /\ ~slot2_admin_cancel))
  /\ (slot1_state /= "PendingNew" => ~slot1_cancel_on_ack)
  /\ (slot2_state /= "PendingNew" => ~slot2_cancel_on_ack)
  /\ (sweep_cursor = 0 => ~sweep_targets_a)
  /\ (sweep_targets_a => ~enabled_acct_a)
  /\ ((~enabled_acct_a /\ ~sweep_targets_a) =>
       (/\ SlotIsCancelingOrEmpty(slot1_state, slot1_cancel_on_ack)
        /\ SlotIsCancelingOrEmpty(slot2_state, slot2_cancel_on_ack)))
====
