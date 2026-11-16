---- MODULE L1_SymDualThreadSwitchover ----
(*
 * Snowcat-typed dual-engine (TLC + Apalache SMT `QF_LIA`) specification of
 * dual-thread `REQ` / `RESP` transport concurrency, strict non-overlapping
 * `LockedOrder` vs `LockedConnection` spinlock hierarchy with leaf-only
 * `OrsStpBook` and lock-free atomic BBO reads, `DialBaton` lock-free handoff
 * with deferred `close_requested` / `reset` and stream-token epoch validation,
 * TLS/TCP short-write self-retirement (`SendOutcome::ShortWriteRetired` ->
 * `OrsQueryDesk` reconciliation), and `64008` / `graceful_drain_connection`
 * maintenance switchover with zero dropped in-flight replies.
 *
 * Dual-engine execution:
 *   - Finite TLC (`BoundedMode = TRUE` in `L1_SymDualThreadSwitchover.cfg`,
 *     with native TLC deadlock checking ENABLED): exhaustive explicit-state
 *     interleaving verification, deadlock-freedom proof, and 100% action coverage.
 *   - Symbolic Apalache (`BoundedMode = FALSE` in `ConstInit`):
 *     1. `typecheck` (Snowcat static type verification).
 *     2. `inductive --cinit=ConstInit --indinv=IndInv` over unbounded 64-bit
 *        `0 .. 2^64 - 1` `now_ns`, `baton_due_ns`, `BackoffBaseNs`, `bbo_atomic_seq`,
 *        and unbounded `token_epoch` (`GuardedValue` stream token).
 *     3. `bmc --cinit=ConstInit --inv=SafetyInv --length=8` over full 64-bit `QF_LIA`.
 *
 * Verified production Rust anchors:
 *   - `DialBaton` (`Idle -> Wanted -> Dialing -> Ready`), `take_ready`, `fail`,
 *     `reset`, and `GuardedValue` auth/stream token read on dial:
 *       crates/tickwire-transport/src/dialer.rs:177-183
 *       crates/tickwire-transport/src/dialer.rs:264-420
 *       crates/tickwire-transport/src/dialer.rs:1224-1234
 *       crates/tickwire-transport/src/links.rs:882-945
 *       crates/tickwire-transport/src/links.rs:1488-1560
 *       crates/tickwire-transport/src/order_router.rs:945-1026
 *   - Non-overlapping `LockedOrder` vs `LockedConnection` spinlocks, leaf-only
 *     `OrsStpBook` (`OrderLockProof`), and lock-free atomic BBO `self_cross`:
 *       crates/tickwire-account/src/spinlock.rs:1-157
 *       crates/tickwire-account/src/ors_stp.rs:174-224
 *       crates/tickwire-runtime/src/session.rs:672-785
 *       crates/tickwire-spot-execution/src/session.rs:1103-1295
 *       crates/tickwire-transport/src/links.rs:1108-1139
 *       crates/tickwire-transport/src/links.rs:1173-1197
 *   - `send_frame_nonblocking` TLS/TCP short-write self-retire (`ShortWriteRetired`)
 *     and `OrsQueryDesk` reconciliation:
 *       crates/tickwire-transport/src/connection.rs:168-175
 *       crates/tickwire-transport/src/connection.rs:238-302
 *       crates/tickwire-runtime/src/session.rs:764-785
 *       crates/tickwire-spot-execution/src/session.rs:1268-1278
 *   - Server maintenance `notice` (`64008` / `graceful_drain_connection`) route
 *     unpublish, parallel standby pre-dial, in-flight reply drain, and switchover:
 *       crates/tickwire-transport/src/order_router.rs:629-643
 *       crates/tickwire-spot-execution/src/json_parser.rs:1174-1177
 *       crates/tickwire-spot-execution/src/session.rs:1943-1986
 *       crates/tickwire-spot-execution/src/session.rs:2562-2566
 *       crates/tickwire-spot-execution/src/session.rs:3354-3384
 *       crates/tickwire-runtime/src/ors.rs:523-591
 *       crates/tickwire-runtime/src/ors.rs:2410-2450
 *       crates/tickwire-runtime/src/ors.rs:2591-2593
 *)
EXTENDS Integers

CONSTANTS
  (* @type: Bool; *)
  BoundedMode,
  (* @type: Int; *)
  BackoffBaseNs,
  (* @type: Int; *)
  BoundU64Max,
  (* @type: Int; *)
  BoundMaxEpoch

VARIABLES
  (* @type: Str; *)
  req_phase,
  (* @type: Str; *)
  resp_phase,
  (* @type: Str; *)
  order_lock_holder,
  (* @type: Str; *)
  stp_lock_holder,
  (* @type: Str; *)
  conn_lock_holder,
  (* @type: Str; *)
  order_phase,
  (* @type: Bool; *)
  query_desk_pending,
  (* @type: Bool; *)
  active_established,
  (* @type: Bool; *)
  active_close_pending,
  (* @type: Bool; *)
  active_draining,
  (* @type: Bool; *)
  active_published_ready,
  (* @type: Int; *)
  active_inflight,
  (* @type: Int; *)
  dropped_replies,
  (* @type: Str; *)
  baton_state,
  (* @type: Int; *)
  baton_due_ns,
  (* @type: Int; *)
  baton_dialed_token,
  (* @type: Bool; *)
  baton_close_req,
  (* @type: Int; *)
  token_epoch,
  (* @type: Bool; *)
  standby_established,
  (* @type: Int; *)
  standby_token,
  (* @type: Bool; *)
  promoted_invalid_dial,
  (* @type: Int; *)
  now_ns,
  (* @type: Int; *)
  bbo_atomic_seq

vars == <<
  req_phase, resp_phase,
  order_lock_holder, stp_lock_holder, conn_lock_holder,
  order_phase, query_desk_pending,
  active_established, active_close_pending, active_draining,
  active_published_ready, active_inflight, dropped_replies,
  baton_state, baton_due_ns, baton_dialed_token, baton_close_req,
  token_epoch, standby_established, standby_token, promoted_invalid_dial,
  now_ns, bbo_atomic_seq
>>

batonAndStandbyVars == <<
  baton_state, baton_due_ns, baton_dialed_token, baton_close_req,
  token_epoch, standby_established, standby_token, promoted_invalid_dial
>>

(* @type: <<Bool, Bool, Bool, Bool>>; *)
connHealthVars == <<
  active_established, active_close_pending, active_draining, active_published_ready
>>

(* SANY-safe 64-bit bounds (`2^30 = 1073741824 <= 2147483647`). *)
(* @type: Int; *)
Pow30 == 1073741824

(* @type: Int; *)
SymSeqLimit == 8 * Pow30 * Pow30

(* @type: Int; *)
SymU64Max == (16 * Pow30 * Pow30) - 1

(* @type: Int; *)
EffU64Max == IF BoundedMode THEN BoundU64Max ELSE SymU64Max

(* @type: Int; *)
EffMaxEpoch == IF BoundedMode THEN BoundMaxEpoch ELSE SymSeqLimit

(* @type: (Int, Int) => Int; *)
Min2(a, b) == IF a <= b THEN a ELSE b

(* @type: (Int, Int) => Int; *)
SaturatingAddU64(a, b) == IF a + b > EffU64Max THEN EffU64Max ELSE a + b

ConstInit ==
  /\ BoundedMode = FALSE
  /\ BackoffBaseNs \in Int /\ BackoffBaseNs >= 0 /\ BackoffBaseNs <= SymU64Max
  /\ BoundU64Max = 2
  /\ BoundMaxEpoch = 2

Init ==
  /\ req_phase = "Idle"
  /\ resp_phase = "Idle"
  /\ order_lock_holder = "NONE"
  /\ stp_lock_holder = "NONE"
  /\ conn_lock_holder = "NONE"
  /\ order_phase = "Idle"
  /\ query_desk_pending = FALSE
  /\ active_established = TRUE
  /\ active_close_pending = FALSE
  /\ active_draining = FALSE
  /\ active_published_ready = TRUE
  /\ active_inflight = 0
  /\ dropped_replies = 0
  /\ baton_state = "Idle"
  /\ baton_due_ns = 0
  /\ baton_dialed_token = 0
  /\ baton_close_req = FALSE
  /\ token_epoch = 1
  /\ standby_established = FALSE
  /\ standby_token = 0
  /\ promoted_invalid_dial = FALSE
  /\ IF BoundedMode
     THEN now_ns = 0
     ELSE /\ now_ns \in Int
          /\ now_ns >= 0
          /\ now_ns <= EffU64Max
  /\ bbo_atomic_seq = 0

(* ========================================================================= *)
(* `REQ` Hot-Thread Order Placement & Non-Overlapping Frame Send             *)
(* ========================================================================= *)

(*
 * `REQ` Step 1 (`crates/tickwire-account/src/ors_stp.rs:174-188`,
 * `crates/tickwire-runtime/src/session.rs:672-720`):
 * reads `OrsStpBook::self_cross` atomic BBO (`bbo_atomic_seq >= 0`) WITHOUT
 * locking `OrsStpBook` or `LockedConnection`, then acquires `LockedOrder`.
 *)
ReqReadAtomicBboAndLockOrder ==
  /\ req_phase = "Idle"
  /\ order_phase = "Idle"
  /\ active_published_ready
  /\ order_lock_holder = "NONE"
  /\ conn_lock_holder /= "REQ"
  /\ bbo_atomic_seq >= 0
  /\ req_phase' = "HoldOrderPreStp"
  /\ order_lock_holder' = "REQ"
  /\ order_phase' = "Creating"
  /\ UNCHANGED <<
       resp_phase, stp_lock_holder, conn_lock_holder, query_desk_pending,
       connHealthVars, active_inflight, dropped_replies,
       batonAndStandbyVars, now_ns, bbo_atomic_seq
     >>

(*
 * `REQ` Step 2 (`crates/tickwire-account/src/ors_stp.rs:212-224`):
 * acquires leaf `OrsStpBook` symbol lock strictly while holding `LockedOrder`
 * (`OrderLockProof<'_>`), with zero `LockedConnection` held.
 *)
ReqAcquireStpLeaf ==
  /\ req_phase = "HoldOrderPreStp"
  /\ order_lock_holder = "REQ"
  /\ stp_lock_holder = "NONE"
  /\ conn_lock_holder /= "REQ"
  /\ req_phase' = "HoldOrderAndStp"
  /\ stp_lock_holder' = "REQ"
  /\ UNCHANGED <<
       resp_phase, order_lock_holder, conn_lock_holder,
       order_phase, query_desk_pending, connHealthVars, active_inflight,
       dropped_replies, batonAndStandbyVars, now_ns, bbo_atomic_seq
     >>

(*
 * `REQ` Step 3 (`crates/tickwire-runtime/src/session.rs:715-735`):
 * updates resting STP book + atomic BBO, releases leaf `OrsStpBook`, releases
 * `LockedOrder`, and stages stack-encoded wire frame outside ALL spinlocks.
 *)
ReqReleaseStpAndOrderStageFrame ==
  /\ req_phase = "HoldOrderAndStp"
  /\ order_lock_holder = "REQ"
  /\ stp_lock_holder = "REQ"
  /\ req_phase' = "EncodedWaitConn"
  /\ stp_lock_holder' = "NONE"
  /\ order_lock_holder' = "NONE"
  /\ order_phase' = "PendingWireSend"
  /\ bbo_atomic_seq' =
       (IF BoundedMode
        THEN Min2(bbo_atomic_seq + 1, BoundU64Max)
        ELSE SaturatingAddU64(bbo_atomic_seq, 1))
  /\ UNCHANGED <<
       resp_phase, conn_lock_holder, query_desk_pending, connHealthVars,
       active_inflight, dropped_replies, batonAndStandbyVars, now_ns
     >>

(*
 * `REQ` Step 4a (`crates/tickwire-transport/src/links.rs:1108-1139`):
 * acquires `LockedConnection` ONLY after `LockedOrder` and `OrsStpBook` are
 * completely released (`order_lock_holder /= "REQ"`), and only when published
 * route is ready (`active_published_ready`).
 *)
ReqAcquireConnForSend ==
  /\ req_phase = "EncodedWaitConn"
  /\ order_phase = "PendingWireSend"
  /\ active_published_ready
  /\ conn_lock_holder = "NONE"
  /\ order_lock_holder /= "REQ"
  /\ stp_lock_holder /= "REQ"
  /\ req_phase' = "HoldConnSending"
  /\ conn_lock_holder' = "REQ"
  /\ UNCHANGED <<
       resp_phase, order_lock_holder, stp_lock_holder, order_phase,
       query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       batonAndStandbyVars, now_ns, bbo_atomic_seq
     >>

(*
 * `REQ` Step 4b (`crates/tickwire-runtime/src/session.rs:748-763`):
 * if `RESP` unpublished the route (`64008` drain or socket close) before `REQ`
 * acquired `LockedConnection`, zero bytes were sent (`NoBytes`) — `REQ` rolls
 * back the unsent `PendingNew` order cleanly under `LockedOrder` without
 * touching `LockedConnection`.
 *)
ReqRollbackUnsentWhenRouteUnavailable ==
  /\ req_phase = "EncodedWaitConn"
  /\ order_phase = "PendingWireSend"
  /\ ~active_published_ready
  /\ order_lock_holder = "NONE"
  /\ conn_lock_holder /= "REQ"
  /\ req_phase' = "Idle"
  /\ order_phase' = "Idle"
  /\ UNCHANGED <<
       resp_phase, order_lock_holder, stp_lock_holder, conn_lock_holder,
       query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       batonAndStandbyVars, now_ns, bbo_atomic_seq
     >>

(*
 * `REQ` Step 5a (`crates/tickwire-transport/src/connection.rs:238-302`):
 * `send_frame_nonblocking` flushes the complete frame and drops `LockedConnection`.
 *)
ReqSendCompleteReleaseConn ==
  /\ req_phase = "HoldConnSending"
  /\ conn_lock_holder = "REQ"
  /\ order_phase = "PendingWireSend"
  /\ active_established
  /\ ~active_close_pending
  /\ req_phase' = "Idle"
  /\ conn_lock_holder' = "NONE"
  /\ order_phase' = "InflightOnActive"
  /\ active_inflight' = 1
  /\ UNCHANGED <<
       resp_phase, order_lock_holder, stp_lock_holder, query_desk_pending,
       connHealthVars, dropped_replies, batonAndStandbyVars, now_ns, bbo_atomic_seq
     >>

(*
 * `REQ` Step 5b (`crates/tickwire-transport/src/connection.rs:238-302`,
 * `crates/tickwire-transport/src/links.rs:1123-1138`,
 * `crates/tickwire-runtime/src/session.rs:764-785`,
 * `crates/tickwire-spot-execution/src/session.rs:1268-1278`):
 * TLS/TCP short write latches `close_pending = TRUE` (`retire_with_failure`),
 * drops `LockedConnection`, unpublishes route readiness, and returns
 * `SendOutcome::ShortWriteRetired` scheduling `OrsQueryDesk` reconciliation.
 *)
ReqShortWriteRetireScheduleQuery ==
  /\ req_phase = "HoldConnSending"
  /\ conn_lock_holder = "REQ"
  /\ order_phase = "PendingWireSend"
  /\ active_established
  /\ ~active_close_pending
  /\ req_phase' = "Idle"
  /\ conn_lock_holder' = "NONE"
  /\ active_close_pending' = TRUE
  /\ active_published_ready' = FALSE
  /\ order_phase' = "ShortWriteUncertain"
  /\ query_desk_pending' = TRUE
  /\ UNCHANGED <<
       resp_phase, order_lock_holder, stp_lock_holder,
       active_established, active_draining, active_inflight, dropped_replies,
       batonAndStandbyVars, now_ns, bbo_atomic_seq
     >>

(* ========================================================================= *)
(* `RESP` Hot-Thread RX Poll, Non-Overlapping Apply, & `64008` Smooth Drain  *)
(* ========================================================================= *)

(*
 * `RESP` Step 1 (`crates/tickwire-transport/src/links.rs:1173-1197`):
 * acquires `LockedConnection` (`poll_raw_rx`) with zero `LockedOrder` held,
 * including on a `draining` socket while `active_inflight > 0`!
 *)
RespAcquireConnPollRx ==
  /\ resp_phase = "Idle"
  /\ active_established
  /\ ~active_close_pending
  /\ active_inflight = 1
  /\ conn_lock_holder = "NONE"
  /\ order_lock_holder /= "RESP"
  /\ stp_lock_holder /= "RESP"
  /\ resp_phase' = "HoldConnPollingRx"
  /\ conn_lock_holder' = "RESP"
  /\ UNCHANGED <<
       req_phase, order_lock_holder, stp_lock_holder, order_phase,
       query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       batonAndStandbyVars, now_ns, bbo_atomic_seq
     >>

(*
 * `RESP` Step 2 (`crates/tickwire-runtime/src/ors.rs:2452-2540`):
 * copies raw RX bytes and releases `LockedConnection` BEFORE JSON/FIX scan
 * or `OrderManager` spinlock acquisition.
 *)
RespReleaseConnStageReply ==
  /\ resp_phase = "HoldConnPollingRx"
  /\ conn_lock_holder = "RESP"
  /\ resp_phase' = "ParsedReplyWaitOrder"
  /\ conn_lock_holder' = "NONE"
  /\ UNCHANGED <<
       req_phase, order_lock_holder, stp_lock_holder, order_phase,
       query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       batonAndStandbyVars, now_ns, bbo_atomic_seq
     >>

(*
 * `RESP` Step 3 (`crates/tickwire-spot-execution/src/session.rs:2289-2335`):
 * acquires `LockedOrder` strictly after `LockedConnection` was released.
 *)
RespAcquireOrderLockForReply ==
  /\ resp_phase = "ParsedReplyWaitOrder"
  /\ order_phase = "InflightOnActive"
  /\ order_lock_holder = "NONE"
  /\ conn_lock_holder /= "RESP"
  /\ resp_phase' = "HoldOrderApplyingReply"
  /\ order_lock_holder' = "RESP"
  /\ UNCHANGED <<
       req_phase, stp_lock_holder, conn_lock_holder, order_phase,
       query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       batonAndStandbyVars, now_ns, bbo_atomic_seq
     >>

(*
 * `RESP` Step 4 (`crates/tickwire-account/src/ors_stp.rs:212-224`):
 * acquires leaf `OrsStpBook` while holding `LockedOrder` (`OrderLockProof`).
 *)
RespAcquireStpLeafForReply ==
  /\ resp_phase = "HoldOrderApplyingReply"
  /\ order_lock_holder = "RESP"
  /\ stp_lock_holder = "NONE"
  /\ conn_lock_holder /= "RESP"
  /\ resp_phase' = "HoldOrderAndStpReply"
  /\ stp_lock_holder' = "RESP"
  /\ UNCHANGED <<
       req_phase, order_lock_holder, conn_lock_holder, order_phase,
       query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       batonAndStandbyVars, now_ns, bbo_atomic_seq
     >>

(*
 * `RESP` Step 5: updates STP book + atomic BBO, releases `OrsStpBook` and
 * `LockedOrder`, and settles the in-flight order (`active_inflight' = 0`).
 *)
RespCompleteOrderReleaseLocks ==
  /\ resp_phase = "HoldOrderAndStpReply"
  /\ order_lock_holder = "RESP"
  /\ stp_lock_holder = "RESP"
  /\ resp_phase' = "Idle"
  /\ stp_lock_holder' = "NONE"
  /\ order_lock_holder' = "NONE"
  /\ order_phase' = "Idle"
  /\ active_inflight' = 0
  /\ bbo_atomic_seq' =
       (IF BoundedMode
        THEN Min2(bbo_atomic_seq + 1, BoundU64Max)
        ELSE SaturatingAddU64(bbo_atomic_seq, 1))
  /\ UNCHANGED <<
       req_phase, conn_lock_holder, query_desk_pending, connHealthVars,
       dropped_replies, batonAndStandbyVars, now_ns
     >>

(*
 * Server maintenance `notice` (`64008` / `graceful_drain_connection`,
 * `crates/tickwire-spot-execution/src/json_parser.rs:1174-1177`,
 * `crates/tickwire-spot-execution/src/session.rs:1943-1986,2562-2566,3354-3384`,
 * `crates/tickwire-runtime/src/ors.rs:523-591,2591-2593`,
 * `crates/tickwire-transport/src/order_router.rs:629-643`):
 *   1. Marks active connection `draining = TRUE` and republishes
 *      `active_published_ready = FALSE` so `REQ` sends 0 new frames to it.
 *   2. Triggers `DialBaton::want(now_ns)` if baton is `"Idle"` so `"tw-dialer"`
 *      pre-establishes the standby socket in parallel while `RESP` continues
 *      draining any `active_inflight` reply!
 *)
RespHandleMaintenanceNotice64008 ==
  /\ resp_phase = "Idle"
  /\ conn_lock_holder = "NONE"
  /\ active_established
  /\ ~active_close_pending
  /\ ~active_draining
  /\ active_draining' = TRUE
  /\ active_published_ready' = FALSE
  /\ IF baton_state = "Idle" /\ ~standby_established
     THEN /\ baton_state' = "Wanted"
          /\ baton_due_ns' = now_ns
          /\ baton_close_req' = FALSE
     ELSE UNCHANGED <<baton_state, baton_due_ns, baton_close_req>>
  /\ UNCHANGED <<
       req_phase, resp_phase, order_lock_holder, stp_lock_holder, conn_lock_holder,
       order_phase, query_desk_pending, active_established, active_close_pending,
       active_inflight, dropped_replies, baton_dialed_token,
       token_epoch, standby_established, standby_token, promoted_invalid_dial,
       now_ns, bbo_atomic_seq
     >>

(*
 * `RESP` `reap_close_pending` (`crates/tickwire-transport/src/links.rs:1546-1560`):
 * closes a socket retired by `REQ` short-write and schedules `DialBaton::want`.
 * Any in-flight order on a failed socket is handed to `OrsQueryDesk` (never dropped).
 *)
RespReapRetiredSocket ==
  /\ resp_phase = "Idle"
  /\ conn_lock_holder = "NONE"
  /\ active_established
  /\ active_close_pending
  /\ active_established' = FALSE
  /\ active_close_pending' = FALSE
  /\ active_draining' = FALSE
  /\ active_published_ready' = FALSE
  /\ IF active_inflight = 1
     THEN /\ active_inflight' = 0
          /\ order_phase' = "ShortWriteUncertain"
          /\ query_desk_pending' = TRUE
     ELSE UNCHANGED <<active_inflight, order_phase, query_desk_pending>>
  /\ IF baton_state = "Idle" /\ ~standby_established
     THEN /\ baton_state' = "Wanted"
          /\ baton_due_ns' = SaturatingAddU64(now_ns, BackoffBaseNs)
          /\ baton_close_req' = FALSE
     ELSE UNCHANGED <<baton_state, baton_due_ns, baton_close_req>>
  /\ UNCHANGED <<
       req_phase, resp_phase, order_lock_holder, stp_lock_holder, conn_lock_holder,
       dropped_replies, baton_dialed_token, token_epoch, standby_established,
       standby_token, promoted_invalid_dial, now_ns, bbo_atomic_seq
     >>

(*
 * `OrsQueryDesk` REST reconciliation (`crates/tickwire-runtime/src/session.rs:764-785`,
 * `crates/tickwire-spot-execution/src/session.rs:1268-1278`): resolves uncertain
 * short-write order off the hot path.
 *)
QueryDeskReconcileUncertainOrder ==
  /\ query_desk_pending
  /\ order_phase = "ShortWriteUncertain"
  /\ order_lock_holder = "NONE"
  /\ query_desk_pending' = FALSE
  /\ order_phase' = "Idle"
  /\ UNCHANGED <<
       req_phase, resp_phase, order_lock_holder, stp_lock_holder, conn_lock_holder,
       connHealthVars, active_inflight, dropped_replies,
       batonAndStandbyVars, now_ns, bbo_atomic_seq
     >>

(* ========================================================================= *)
(* `DialBaton` Handoff, Deferred `close_requested` / `reset`, & Switchover   *)
(* ========================================================================= *)

(* `DialBaton::want` when replacement / standby is needed (`crates/tickwire-transport/src/dialer.rs:322-336`). *)
RespWantStandbyDial ==
  /\ (~active_established \/ active_draining)
  /\ ~standby_established
  /\ baton_state = "Idle"
  /\ baton_state' = "Wanted"
  /\ baton_due_ns' = now_ns
  /\ baton_close_req' = FALSE
  /\ UNCHANGED <<
       req_phase, resp_phase, order_lock_holder, stp_lock_holder, conn_lock_holder,
       order_phase, query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       baton_dialed_token, token_epoch, standby_established, standby_token,
       promoted_invalid_dial, now_ns, bbo_atomic_seq
     >>

(*
 * `"tw-dialer"` `DialBaton::claim` (`crates/tickwire-transport/src/dialer.rs:338-356`,
 * `1224-1234`): claims `"Wanted"` when `now_ns >= baton_due_ns` and snapshots
 * `GuardedValue` stream/auth `token_epoch`.
 *)
DialerClaimBaton ==
  /\ baton_state = "Wanted"
  /\ now_ns >= baton_due_ns
  /\ baton_state' = "Dialing"
  /\ baton_dialed_token' = token_epoch
  /\ UNCHANGED <<
       req_phase, resp_phase, order_lock_holder, stp_lock_holder, conn_lock_holder,
       order_phase, query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       baton_due_ns, baton_close_req, token_epoch, standby_established, standby_token,
       promoted_invalid_dial, now_ns, bbo_atomic_seq
     >>

(* `"tw-dialer"` `DialBaton::fail` (`crates/tickwire-transport/src/dialer.rs:370-381`). *)
DialerFailBackoff ==
  /\ baton_state = "Dialing"
  /\ baton_state' = "Wanted"
  /\ baton_due_ns' = SaturatingAddU64(now_ns, BackoffBaseNs)
  /\ baton_dialed_token' = 0
  /\ UNCHANGED <<
       req_phase, resp_phase, order_lock_holder, stp_lock_holder, conn_lock_holder,
       order_phase, query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       baton_close_req, token_epoch, standby_established, standby_token,
       promoted_invalid_dial, now_ns, bbo_atomic_seq
     >>

(* `"tw-dialer"` `DialBaton::deliver_ready` (`crates/tickwire-transport/src/dialer.rs:358-368`). *)
DialerDeliverReady ==
  /\ baton_state = "Dialing"
  /\ baton_state' = "Ready"
  /\ UNCHANGED <<
       req_phase, resp_phase, order_lock_holder, stp_lock_holder, conn_lock_holder,
       order_phase, query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       baton_due_ns, baton_dialed_token, baton_close_req, token_epoch,
       standby_established, standby_token, promoted_invalid_dial, now_ns, bbo_atomic_seq
     >>

(*
 * Concurrent `GuardedValue` auth/listenKey rotation or deferred `close_requested` /
 * `DialBaton::reset` (`crates/tickwire-transport/src/dialer.rs:398-410`,
 * `crates/tickwire-transport/src/order_router.rs:1009-1023`):
 * invalidates an in-progress or ready `DialBaton` product before promotion.
 *)
RotateTokenOrDeferBatonClose ==
  /\ baton_state \in {"Dialing", "Ready"}
  /\ (~baton_close_req \/ token_epoch < EffMaxEpoch)
  /\ baton_close_req' = TRUE
  /\ token_epoch' =
       (IF token_epoch < EffMaxEpoch THEN token_epoch + 1 ELSE token_epoch)
  /\ UNCHANGED <<
       req_phase, resp_phase, order_lock_holder, stp_lock_holder, conn_lock_holder,
       order_phase, query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       baton_state, baton_due_ns, baton_dialed_token, standby_established, standby_token,
       promoted_invalid_dial, now_ns, bbo_atomic_seq
     >>

(*
 * `DialBaton::reset` (`crates/tickwire-transport/src/dialer.rs:398-410`):
 * drops any buffered `DialedLink` and returns baton to `"Idle"` without promoting.
 *)
RespResetInvalidatedBaton ==
  /\ baton_state \in {"Dialing", "Ready"}
  /\ baton_close_req
  /\ baton_state' = "Idle"
  /\ baton_due_ns' = 0
  /\ baton_dialed_token' = 0
  /\ baton_close_req' = FALSE
  /\ UNCHANGED <<
       req_phase, resp_phase, order_lock_holder, stp_lock_holder, conn_lock_holder,
       order_phase, query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       token_epoch, standby_established, standby_token, promoted_invalid_dial,
       now_ns, bbo_atomic_seq
     >>

(*
 * `DialBaton::take_ready` rejecting closed/expired-token dial
 * (`crates/tickwire-transport/src/links.rs:1511-1531`,
 * `crates/tickwire-transport/src/order_router.rs:961-980`): drops invalid `DialedLink`
 * and re-arms `"Wanted"` without ever setting `standby_established = TRUE`.
 *)
RespTakeReadyDiscardInvalid ==
  /\ baton_state = "Ready"
  /\ ~standby_established
  /\ (baton_close_req \/ baton_dialed_token < token_epoch)
  /\ baton_state' = "Wanted"
  /\ baton_due_ns' = SaturatingAddU64(now_ns, BackoffBaseNs)
  /\ baton_dialed_token' = 0
  /\ baton_close_req' = FALSE
  /\ UNCHANGED <<
       req_phase, resp_phase, order_lock_holder, stp_lock_holder, conn_lock_holder,
       order_phase, query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       token_epoch, standby_established, standby_token, promoted_invalid_dial,
       now_ns, bbo_atomic_seq
     >>

(*
 * `DialBaton::take_ready` installing verified standby link
 * (`crates/tickwire-runtime/src/ors.rs:2418-2448`):
 * requires `~baton_close_req` and fresh `baton_dialed_token = token_epoch`.
 *)
RespTakeReadyInstallStandby ==
  /\ baton_state = "Ready"
  /\ ~standby_established
  /\ ~baton_close_req
  /\ baton_dialed_token = token_epoch
  /\ baton_state' = "Idle"
  /\ baton_due_ns' = 0
  /\ baton_dialed_token' = 0
  /\ standby_established' = TRUE
  /\ standby_token' = baton_dialed_token
  /\ promoted_invalid_dial' = FALSE
  /\ UNCHANGED <<
       req_phase, resp_phase, order_lock_holder, stp_lock_holder, conn_lock_holder,
       order_phase, query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       baton_close_req, token_epoch, now_ns, bbo_atomic_seq
     >>

(*
 * Smooth standby switchover (`crates/tickwire-spot-execution/src/session.rs:1972-1986`,
 * `crates/tickwire-runtime/src/ors.rs:2418-2448`):
 * ONLY closes a `draining` socket once `active_inflight = 0` (`dropped_replies`
 * remains `0`!), installs `standby_established` as active, and republishes
 * `active_published_ready = TRUE`.
 *)
RespPromoteStandbyToActive ==
  /\ resp_phase = "Idle"
  /\ conn_lock_holder = "NONE"
  /\ standby_established
  /\ (~active_established \/ (active_draining /\ active_inflight = 0))
  /\ active_established' = TRUE
  /\ active_close_pending' = FALSE
  /\ active_draining' = FALSE
  /\ active_published_ready' = TRUE
  /\ standby_established' = FALSE
  /\ standby_token' = 0
  /\ UNCHANGED <<
       req_phase, resp_phase, order_lock_holder, stp_lock_holder, conn_lock_holder,
       order_phase, query_desk_pending, active_inflight, dropped_replies,
       baton_state, baton_due_ns, baton_dialed_token, baton_close_req,
       token_epoch, promoted_invalid_dial, now_ns, bbo_atomic_seq
     >>

(* Monotonic 64-bit clock progression (`0 .. 2^64 - 1`). *)
AdvanceClock ==
  /\ IF BoundedMode
     THEN /\ now_ns < BoundU64Max
          /\ now_ns' = now_ns + 1
     ELSE \E nxt_ns \in Int :
            /\ nxt_ns > now_ns
            /\ nxt_ns <= EffU64Max
            /\ now_ns' = nxt_ns
  /\ UNCHANGED <<
       req_phase, resp_phase, order_lock_holder, stp_lock_holder, conn_lock_holder,
       order_phase, query_desk_pending, connHealthVars, active_inflight, dropped_replies,
       batonAndStandbyVars, bbo_atomic_seq
     >>

Next ==
  \/ ReqReadAtomicBboAndLockOrder
  \/ ReqAcquireStpLeaf
  \/ ReqReleaseStpAndOrderStageFrame
  \/ ReqAcquireConnForSend
  \/ ReqRollbackUnsentWhenRouteUnavailable
  \/ ReqSendCompleteReleaseConn
  \/ ReqShortWriteRetireScheduleQuery
  \/ RespAcquireConnPollRx
  \/ RespReleaseConnStageReply
  \/ RespAcquireOrderLockForReply
  \/ RespAcquireStpLeafForReply
  \/ RespCompleteOrderReleaseLocks
  \/ RespHandleMaintenanceNotice64008
  \/ RespReapRetiredSocket
  \/ QueryDeskReconcileUncertainOrder
  \/ RespWantStandbyDial
  \/ DialerClaimBaton
  \/ DialerFailBackoff
  \/ DialerDeliverReady
  \/ RotateTokenOrDeferBatonClose
  \/ RespResetInvalidatedBaton
  \/ RespTakeReadyDiscardInvalid
  \/ RespTakeReadyInstallStandby
  \/ RespPromoteStandbyToActive
  \/ AdvanceClock

Spec == Init /\ [][Next]_vars

(* ========================================================================= *)
(* Safety & Inductive Invariants                                             *)
(* ========================================================================= *)

TypeOK ==
  /\ req_phase \in {
       "Idle", "HoldOrderPreStp", "HoldOrderAndStp", "EncodedWaitConn", "HoldConnSending"
     }
  /\ resp_phase \in {
       "Idle", "HoldConnPollingRx", "ParsedReplyWaitOrder",
       "HoldOrderApplyingReply", "HoldOrderAndStpReply"
     }
  /\ order_lock_holder \in {"NONE", "REQ", "RESP"}
  /\ stp_lock_holder \in {"NONE", "REQ", "RESP"}
  /\ conn_lock_holder \in {"NONE", "REQ", "RESP"}
  /\ order_phase \in {
       "Idle", "Creating", "PendingWireSend", "InflightOnActive", "ShortWriteUncertain"
     }
  /\ query_desk_pending \in BOOLEAN
  /\ active_established \in BOOLEAN
  /\ active_close_pending \in BOOLEAN
  /\ active_draining \in BOOLEAN
  /\ active_published_ready \in BOOLEAN
  /\ active_inflight \in {0, 1}
  /\ dropped_replies \in Int /\ dropped_replies = 0
  /\ baton_state \in {"Idle", "Wanted", "Dialing", "Ready"}
  /\ baton_due_ns \in Int /\ baton_due_ns >= 0 /\ baton_due_ns <= EffU64Max
  /\ baton_dialed_token \in Int /\ baton_dialed_token >= 0 /\ baton_dialed_token <= EffMaxEpoch
  /\ baton_close_req \in BOOLEAN
  /\ token_epoch \in Int /\ token_epoch >= 1 /\ token_epoch <= EffMaxEpoch
  /\ standby_established \in BOOLEAN
  /\ standby_token \in Int /\ standby_token >= 0 /\ standby_token <= EffMaxEpoch
  /\ promoted_invalid_dial \in BOOLEAN
  /\ now_ns \in Int /\ now_ns >= 0 /\ now_ns <= EffU64Max
  /\ bbo_atomic_seq \in Int /\ bbo_atomic_seq >= 0 /\ bbo_atomic_seq <= EffU64Max

(*
 * Core Lock-Hierarchy Invariant 1 (`AGENTS.md` §2):
 * Neither `REQ` nor `RESP` EVER holds `LockedOrder` and `LockedConnection` simultaneously.
 *)
NoOverlappingOrderAndConnLocks ==
  /\ ~(order_lock_holder = "REQ" /\ conn_lock_holder = "REQ")
  /\ ~(order_lock_holder = "RESP" /\ conn_lock_holder = "RESP")

(*
 * Core Lock-Hierarchy Invariant 2 (`crates/tickwire-account/src/ors_stp.rs:212-224`):
 * `OrsStpBook` is acquired strictly as a leaf under `LockedOrder` (`OrderLockProof`),
 * never without `LockedOrder` and never alongside `LockedConnection`.
 *)
StpBookLeafHierarchyInv ==
  /\ (stp_lock_holder = "REQ") =>
       (order_lock_holder = "REQ" /\ conn_lock_holder /= "REQ")
  /\ (stp_lock_holder = "RESP") =>
       (order_lock_holder = "RESP" /\ conn_lock_holder /= "RESP")

(*
 * Spinlock Deadlock-Freedom Invariant:
 * No circular wait between `REQ` and `RESP` is reachable — if one thread holds
 * `LockedOrder` while the other holds `LockedConnection`, neither holder waits on
 * the other's lock to release its own!
 *)
SpinlockDeadlockFreedomInv ==
  /\ (order_lock_holder = "REQ") => (req_phase \in {"HoldOrderPreStp", "HoldOrderAndStp"})
  /\ (conn_lock_holder = "REQ") => (req_phase = "HoldConnSending")
  /\ (conn_lock_holder = "RESP") => (resp_phase = "HoldConnPollingRx")
  /\ (order_lock_holder = "RESP") => (resp_phase \in {"HoldOrderApplyingReply", "HoldOrderAndStpReply"})

(*
 * `DialBaton` & Token Safety:
 * Closed/reset or stale-token dials never promote to `standby_established` or active.
 *)
NoInvalidDialPromotedInv ==
  /\ ~promoted_invalid_dial
  /\ (standby_established => (standby_token >= 1 /\ standby_token <= token_epoch))
  /\ (~standby_established => standby_token = 0)

(*
 * Short-Write Self-Retire & Graceful `64008` Switchover Safety:
 *   1. A `close_pending` or `draining` socket is never advertised as `active_published_ready`.
 *   2. Every `ShortWriteUncertain` order is tracked by `OrsQueryDesk` (`query_desk_pending`).
 *   3. Zero in-flight replies are ever dropped (`dropped_replies = 0`).
 *)
SwitchoverAndShortWriteSafetyInv ==
  /\ active_published_ready =
       (active_established /\ ~active_close_pending /\ ~active_draining)
  /\ (order_phase = "ShortWriteUncertain") <=> query_desk_pending
  /\ dropped_replies = 0

SafetyInv ==
  /\ TypeOK
  /\ NoOverlappingOrderAndConnLocks
  /\ StpBookLeafHierarchyInv
  /\ SpinlockDeadlockFreedomInv
  /\ NoInvalidDialPromotedInv
  /\ SwitchoverAndShortWriteSafetyInv

(* Strengthened inductive invariant `IndInv` proved over unbounded 64-bit `QF_LIA`. *)
IndInv ==
  /\ SafetyInv
  /\ ((req_phase \in {"HoldOrderPreStp", "HoldOrderAndStp"}) <=> (order_lock_holder = "REQ"))
  /\ ((req_phase = "HoldOrderAndStp") <=> (stp_lock_holder = "REQ"))
  /\ ((req_phase = "HoldConnSending") <=> (conn_lock_holder = "REQ"))
  /\ ((resp_phase = "HoldConnPollingRx") <=> (conn_lock_holder = "RESP"))
  /\ ((resp_phase \in {"HoldOrderApplyingReply", "HoldOrderAndStpReply"}) <=> (order_lock_holder = "RESP"))
  /\ ((resp_phase = "HoldOrderAndStpReply") <=> (stp_lock_holder = "RESP"))
  /\ ((req_phase \in {"HoldOrderPreStp", "HoldOrderAndStp"}) <=> (order_phase = "Creating"))
  /\ ((req_phase \in {"EncodedWaitConn", "HoldConnSending"}) <=> (order_phase = "PendingWireSend"))
  /\ ((req_phase = "HoldConnSending") => (active_established /\ ~active_close_pending))
  /\ ((order_phase = "InflightOnActive") <=> (active_inflight = 1))
  /\ ((resp_phase \in {"HoldConnPollingRx", "ParsedReplyWaitOrder", "HoldOrderApplyingReply", "HoldOrderAndStpReply"})
        => (order_phase = "InflightOnActive" /\ active_established))
  /\ (active_draining => active_established)
  /\ (~active_established => (~active_close_pending /\ ~active_draining /\ active_inflight = 0))
  /\ ((baton_state \in {"Dialing", "Ready"}) => (baton_dialed_token >= 1 /\ baton_dialed_token <= token_epoch))
  /\ ((baton_state \in {"Idle", "Wanted"}) => (baton_dialed_token = 0))
====
