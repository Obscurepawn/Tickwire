---- MODULE L5_SymWarmerCrcFixValidation ----
(*
 * Post-Fix Symbolic Inductive & 64-Bit Boundary Validation (`/vibe-tla fix`) for:
 *   1. Fix 1 (`CoreNode` non-zero `mio::Poll` sleep when unready/frozen/halted or
 *      on warmup error):
 *        crates/tickwire-runtime/src/node.rs:166-188
 *        crates/tickwire-runtime/src/node.rs:260-277
 *        crates/tickwire-runtime/src/node.rs:420-485
 *        crates/tickwire-runtime/src/node.rs:826-845
 *        crates/tickwire-runtime/src/node.rs:894-905
 *   2. Fix 2 (`WarmupTracker::next_due_ns` `checked_add` exact algebraic
 *      biconditional with `WarmupTracker::can_warm` across `0 .. 2^64 - 1`):
 *        crates/tickwire-transport/src/warmup.rs:80-187
 *   3. Fix 3 (Cold-startup `CLOCK_MONOTONIC` seeding when enabling warmup):
 *        crates/tickwire-runtime/src/node.rs:848-874
 *        crates/tickwire-binance-execution/src/lib.rs:695-717
 *        crates/tickwire-binance-execution/src/lib.rs:801-844
 *        crates/tickwire-spot-runner/src/live.rs:364-374
 *        crates/tickwire-spot-runner/src/live.rs:549-560
 *   4. Fix 4 (`tickwire_journal::crc32c::crc32c_masked` total clamping parity
 *      with `tickwire_ipc::crc32c_masked` across all `Range<usize>` inputs):
 *        crates/tickwire-journal/src/crc32c.rs:180-195
 *        crates/tickwire-ipc/src/crc32c.rs:152-168
 *
 * Dual-engine execution:
 *   - Symbolic Apalache + Z3 (`BoundedMode = FALSE` in `ConstInit`):
 *       * Snowcat static typecheck (`run_apalache.sh typecheck`)
 *       * Mode S1 64-bit `u64` BMC (`run_apalache.sh bmc --length=8`)
 *       * Mode S2 unbounded symbolic induction (`run_apalache.sh inductive --indinv=IndInv`)
 *   - Explicit TLC (`BoundedMode = TRUE` in `.cfg` files):
 *       * Exhaustive finite verification + 100% action coverage (`./scripts/check-tla.sh`).
 *)
EXTENDS Integers

CONSTANTS
  (* @type: Bool; *)
  BoundedMode,
  (* @type: Int; *)
  WarmupInterval,
  (* @type: Int; *)
  BoundU64Max,
  (* @type: Int; *)
  MaxIdleWaitNs

VARIABLES
  (* --- Post-fix `WarmupTracker`, cold-startup seeding & `CoreNode` state --- *)
  (* @type: Bool; *)
  warmup_configured,
  (* @type: Int; *)
  startup_clock_ns,
  (* @type: Bool; *)
  startup_seeded,
  (* @type: Int; *)
  now_ns,
  (* @type: Int; *)
  sample_ns,
  (* @type: Int; *)
  last_activity_ns,
  (* @type: Int; *)
  last_warm_ns,
  (* @type: Int; *)
  warm_count,
  (* @type: Int; *)
  first_warm_ns,
  (* @type: Bool; *)
  node_ready,
  (* @type: Bool; *)
  signed_in_flight,
  (* @type: Bool; *)
  last_runnable,
  (* @type: Bool; *)
  last_warm_err,
  (* @type: Bool; *)
  not_ready_polled,
  (* @type: Int; *)
  step_wait_ns,
  (* --- Post-fix `tickwire_journal` vs `tickwire_ipc` `crc32c_masked` state --- *)
  (* @type: Int; *)
  buf_len,
  (* @type: Int; *)
  mask_start,
  (* @type: Int; *)
  mask_end,
  (* @type: Int; *)
  probe_idx,
  (* @type: Int; *)
  journal_start,
  (* @type: Int; *)
  journal_end,
  (* @type: Int; *)
  ipc_start,
  (* @type: Int; *)
  ipc_end

(* @type: <<Bool, Int, Bool, Int, Int, Int, Int, Int, Int, Bool, Bool, Bool, Bool, Bool, Int>>; *)
warmerVars == <<
  warmup_configured, startup_clock_ns, startup_seeded, now_ns, sample_ns,
  last_activity_ns, last_warm_ns, warm_count, first_warm_ns, node_ready,
  signed_in_flight, last_runnable, last_warm_err, not_ready_polled, step_wait_ns
>>

(* @type: <<Int, Int, Int, Int, Int, Int, Int, Int>>; *)
crcVars == <<
  buf_len, mask_start, mask_end, probe_idx,
  journal_start, journal_end, ipc_start, ipc_end
>>

(* @type: <<Bool, Int, Bool, Int, Int, Int, Int, Int, Int, Bool, Bool, Bool, Bool, Bool, Int, Int, Int, Int, Int, Int, Int, Int, Int>>; *)
vars == <<
  warmup_configured, startup_clock_ns, startup_seeded, now_ns, sample_ns,
  last_activity_ns, last_warm_ns, warm_count, first_warm_ns, node_ready,
  signed_in_flight, last_runnable, last_warm_err, not_ready_polled, step_wait_ns,
  buf_len, mask_start, mask_end, probe_idx,
  journal_start, journal_end, ipc_start, ipc_end
>>

(* 2^30 = 1073741824 fits SANY's 32-bit literal parser; Apalache evaluates
 * 16 * 2^60 - 1 = 2^64 - 1 (`u64::MAX` = 18446744073709551615) in exact SMT Int. *)
(* @type: Int; *)
Pow30 == 1073741824

(* @type: Int; *)
SymU64Max == (16 * Pow30 * Pow30) - 1

(* @type: Int; *)
SymMaxIdleWaitCap == 1000000000

(* @type: Int; *)
EffU64Max == IF BoundedMode THEN BoundU64Max ELSE SymU64Max

(* Symbolic mode covers all buffer lengths `0 .. 2^64 - 1` (strictly subsuming
 * `0 .. 1048576`); finite TLC mode uses `0 .. 4` with masks in `0 .. 5` so both
 * out-of-bounds (`mask > buf_len`) and inverted (`mask_start > mask_end`) ranges
 * execute exhaustively. *)
(* @type: Int; *)
EffMaxBufLen == IF BoundedMode THEN 4 ELSE SymU64Max

(* @type: Set(Int); *)
FiniteBufLenSet == 0..4

(* @type: Set(Int); *)
FiniteMaskSet == 0..5

(* @type: (Int, Int) => Int; *)
Min2(a, b) == IF a <= b THEN a ELSE b

(* @type: (Int, Int) => Int; *)
Max2(a, b) == IF a >= b THEN a ELSE b

(* Exact `u64::saturating_sub` (`crates/tickwire-transport/src/warmup.rs:176`,
 * `crates/tickwire-runtime/src/node.rs:832`). *)
(* @type: (Int, Int) => Int; *)
SaturatingSubU64(a, b) == IF a > b THEN a - b ELSE 0

(* Exact `u64::saturating_add` (`crates/tickwire-transport/src/warmup.rs:146`). *)
(* @type: (Int, Int) => Int; *)
SaturatingAddU64(a, b) == IF a + b > EffU64Max THEN EffU64Max ELSE a + b

(* Effective configured warmup interval (`0` before `set_warmup_interval_ns`). *)
(* @type: Int; *)
EffInterval == IF warmup_configured THEN WarmupInterval ELSE 0

(* Exact `anchor = max(last_activity_ns, last_warm_ns)` (`crates/tickwire-transport/src/warmup.rs:156-175`). *)
(* @type: (Int, Int) => Int; *)
TrackerAnchor(act, warm) == IF act >= warm THEN act ELSE warm

(* Post-Fix 2 `WarmupTracker::next_due_ns` (`crates/tickwire-transport/src/warmup.rs:152-162`):
 * returns `None` when `warmup_interval_ns == 0` or `anchor.checked_add(interval)`
 * overflows `u64::MAX`, else `Some(anchor + interval)`. *)
(* @type: (Int, Int, Int) => Bool; *)
NextDuePresent(interval, act, warm) ==
  /\ interval > 0
  /\ (TrackerAnchor(act, warm) + interval <= EffU64Max)

(* @type: (Int, Int, Int) => Int; *)
NextDueVal(interval, act, warm) ==
  TrackerAnchor(act, warm) + interval

(* Exact `WarmupTracker::can_warm` (`crates/tickwire-transport/src/warmup.rs:166-177`). *)
(* @type: (Int, Int, Int, Int) => Bool; *)
CanWarm(t, interval, act, warm) ==
  /\ interval > 0
  /\ SaturatingSubU64(t, TrackerAnchor(act, warm)) >= interval

(* Post-Fix 1 `CoreNode::step_inner` deadline clamp + `CoreNode::run_with` poll wait
 * (`crates/tickwire-runtime/src/node.rs:440-451, 827-845`):
 * clamps `base_dl` with `next_warmup_due_ns()` STRICTLY when `ready` (`!frozen &&
 * !halted && submission_ready`) AND `NextDuePresent`. *)
(* @type: (Bool, Bool, Int, Int, Int, Int, Int) => Int; *)
ComputeStepWaitNs(runnable, ready, now_t, base_dl, interval, act, warm) ==
  LET clamped_dl ==
        IF ready /\ NextDuePresent(interval, act, warm)
        THEN Min2(base_dl, NextDueVal(interval, act, warm))
        ELSE base_dl
  IN
  IF runnable
  THEN 0
  ELSE Min2(MaxIdleWaitNs, SaturatingSubU64(clamped_dl, now_t))

(* Relational constant constraints for Apalache (`--cinit=ConstInit`). *)
ConstInit ==
  /\ BoundedMode = FALSE
  /\ WarmupInterval \in Int /\ WarmupInterval >= 0 /\ WarmupInterval <= SymU64Max
  /\ BoundU64Max = 5
  /\ MaxIdleWaitNs \in Int /\ MaxIdleWaitNs >= 1 /\ MaxIdleWaitNs <= SymMaxIdleWaitCap

Init ==
  /\ warmup_configured = FALSE
  /\ startup_clock_ns = 0
  /\ startup_seeded = FALSE
  /\ IF BoundedMode
     THEN now_ns = 0
     ELSE /\ now_ns \in Int
          /\ now_ns >= 0
          /\ now_ns <= EffU64Max
  /\ sample_ns = now_ns
  /\ last_activity_ns = 0
  /\ last_warm_ns = 0
  /\ warm_count = 0
  /\ first_warm_ns = 0
  /\ node_ready = TRUE
  /\ signed_in_flight = FALSE
  /\ last_runnable = FALSE
  /\ last_warm_err = FALSE
  /\ not_ready_polled = FALSE
  /\ step_wait_ns = MaxIdleWaitNs
  /\ buf_len = 0
  /\ mask_start = 0
  /\ mask_end = 0
  /\ probe_idx = 0
  /\ journal_start = 0
  /\ journal_end = 0
  /\ ipc_start = 0
  /\ ipc_end = 0

(* ========================================================================= *)
(* Part A Transitions — Post-Fix `WarmupTracker` & `CoreNode` (`Fix 1,2,3`)  *)
(* ========================================================================= *)

(* Fix 3: Cold-startup warmup configuration & `CLOCK_MONOTONIC` seeding
 * (`crates/tickwire-runtime/src/node.rs:853-874`,
 *  `crates/tickwire-binance-execution/src/lib.rs:695-717`,
 *  `crates/tickwire-spot-runner/src/live.rs:364-374`).
 * When `WarmupInterval > 0` and `last_activity_ns == 0 && last_warm_ns == 0`,
 * samples `now_ns` and calls `record_activity(now_ns)`. *)
EnableWarmupColdStartup ==
  /\ ~warmup_configured
  /\ ((~BoundedMode) \/ (buf_len = 0 /\ mask_start = 0 /\ mask_end = 0 /\ probe_idx = 0))
  /\ warmup_configured' = TRUE
  /\ startup_clock_ns' = now_ns
  /\ sample_ns' = now_ns
  /\ IF WarmupInterval > 0 /\ last_activity_ns = 0 /\ last_warm_ns = 0
     THEN /\ startup_seeded' = TRUE
          /\ last_activity_ns' = Max2(last_activity_ns, now_ns)
     ELSE /\ startup_seeded' = startup_seeded
          /\ last_activity_ns' = last_activity_ns
  /\ UNCHANGED <<
       now_ns, last_warm_ns, warm_count, first_warm_ns, node_ready,
       signed_in_flight, last_runnable, last_warm_err, not_ready_polled,
       step_wait_ns, crcVars
     >>

(* Host `CLOCK_MONOTONIC` advances (`0 <= now_ns < nxt <= EffU64Max`). *)
AdvanceClockMonotonic ==
  /\ now_ns < EffU64Max
  /\ ((~BoundedMode) \/ (buf_len = 0 /\ mask_start = 0 /\ mask_end = 0 /\ probe_idx = 0))
  /\ IF BoundedMode
     THEN /\ now_ns' = now_ns + 1
          /\ sample_ns' = now_ns + 1
     ELSE \E nxt \in Int :
            /\ nxt > now_ns
            /\ nxt <= EffU64Max
            /\ now_ns' = nxt
            /\ sample_ns' = nxt
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, last_activity_ns,
       last_warm_ns, warm_count, first_warm_ns, node_ready, signed_in_flight,
       last_runnable, last_warm_err, not_ready_polled, step_wait_ns, crcVars
     >>

(* Account freeze, gateway halt, or pool unready (`crates/tickwire-runtime/src/node.rs:443-446`, `crates/tickwire-runtime/src/node.rs:895-898`). *)
SetNodeNotReady ==
  /\ warmup_configured
  /\ node_ready
  /\ node_ready' = FALSE
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, now_ns, sample_ns,
       last_activity_ns, last_warm_ns, warm_count, first_warm_ns,
       signed_in_flight, last_runnable, last_warm_err, not_ready_polled,
       step_wait_ns, crcVars
     >>

RestoreNodeReady ==
  /\ warmup_configured
  /\ ~node_ready
  /\ node_ready' = TRUE
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, now_ns, sample_ns,
       last_activity_ns, last_warm_ns, warm_count, first_warm_ns,
       signed_in_flight, last_runnable, last_warm_err, not_ready_polled,
       step_wait_ns, crcVars
     >>

(* Signed command awaiting first-output gate (`crates/tickwire-binance-execution/src/lib.rs:811-818`). *)
BeginSignedInFlight ==
  /\ warmup_configured
  /\ node_ready
  /\ ~signed_in_flight
  /\ signed_in_flight' = TRUE
  /\ last_activity_ns' = Max2(last_activity_ns, now_ns)
  /\ sample_ns' = now_ns
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, now_ns,
       last_warm_ns, warm_count, first_warm_ns, node_ready, last_runnable,
       last_warm_err, not_ready_polled, step_wait_ns, crcVars
     >>

CompleteSignedInFlight ==
  /\ warmup_configured
  /\ signed_in_flight
  /\ signed_in_flight' = FALSE
  /\ last_activity_ns' = Max2(last_activity_ns, now_ns)
  /\ sample_ns' = now_ns
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, now_ns,
       last_warm_ns, warm_count, first_warm_ns, node_ready, last_runnable,
       last_warm_err, not_ready_polled, step_wait_ns, crcVars
     >>

(* Active `CoreNode::step_inner` turn (`market_messages > 0 || private_messages > 0 ||
 * submissions > 0 || rest_completed || runnable`, `crates/tickwire-runtime/src/node.rs:431-451`).
 * `poll_clock` (`crates/tickwire-runtime/src/node.rs:461-462`) advances `next_calibration = now.checked_add(retry)?`,
 * guaranteeing `base_dl \in now_ns + 1 .. EffU64Max`. *)
CoreNodeStepActive ==
  /\ warmup_configured
  /\ now_ns < EffU64Max
  /\ LET act_next == Max2(last_activity_ns, now_ns) IN
     /\ last_activity_ns' = act_next
     /\ sample_ns' = now_ns
     /\ last_warm_err' = FALSE
     /\ \E run_flag \in BOOLEAN :
          /\ last_runnable' = run_flag
          /\ IF BoundedMode
             THEN step_wait_ns' =
                    ComputeStepWaitNs(
                      run_flag, node_ready, now_ns, now_ns + 1,
                      EffInterval, act_next, last_warm_ns
                    )
             ELSE \E base_dl \in Int :
                    /\ base_dl >= now_ns + 1
                    /\ base_dl <= EffU64Max
                    /\ step_wait_ns' =
                         ComputeStepWaitNs(
                           run_flag, node_ready, now_ns, base_dl,
                           EffInterval, act_next, last_warm_ns
                         )
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, now_ns,
       last_warm_ns, warm_count, first_warm_ns, node_ready, signed_in_flight,
       not_ready_polled, crcVars
     >>

(* Fix 1a: Quiescent `CoreNode::step_inner` while `!node_ready`
 * (`frozen() || halted() || !submission_ready()`, `crates/tickwire-runtime/src/node.rs:431-451`, `crates/tickwire-runtime/src/node.rs:894-905`):
 * `maybe_warm_hot_path` calls `record_activity(now_ns)` and returns `Ok(false)`,
 * and `step_inner` omits the `next_warmup_due_ns()` clamp so `wait > 0`. *)
CoreNodeStepIdleNotReady ==
  /\ warmup_configured
  /\ ~node_ready
  /\ now_ns < EffU64Max
  /\ LET act_next == Max2(last_activity_ns, now_ns) IN
     /\ last_activity_ns' = act_next
     /\ sample_ns' = now_ns
     /\ last_runnable' = FALSE
     /\ last_warm_err' = FALSE
     /\ not_ready_polled' = TRUE
     /\ IF BoundedMode
        THEN step_wait_ns' =
               ComputeStepWaitNs(
                 FALSE, FALSE, now_ns, now_ns + 1,
                 EffInterval, act_next, last_warm_ns
               )
        ELSE \E base_dl \in Int :
               /\ base_dl >= now_ns + 1
               /\ base_dl <= EffU64Max
               /\ step_wait_ns' =
                    ComputeStepWaitNs(
                      FALSE, FALSE, now_ns, base_dl,
                      EffInterval, act_next, last_warm_ns
                    )
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, now_ns,
       last_warm_ns, warm_count, first_warm_ns, node_ready, signed_in_flight,
       crcVars
     >>

(* Quiescent `CoreNode::step_inner` while `node_ready` and `signed_in_flight`
 * (`crates/tickwire-binance-execution/src/lib.rs:811-818`): records activity and suppresses warming. *)
CoreNodeStepIdleReadySignedBusy ==
  /\ warmup_configured
  /\ node_ready
  /\ signed_in_flight
  /\ now_ns < EffU64Max
  /\ LET act_next == Max2(last_activity_ns, now_ns) IN
     /\ last_activity_ns' = act_next
     /\ sample_ns' = now_ns
     /\ last_runnable' = FALSE
     /\ last_warm_err' = FALSE
     /\ IF BoundedMode
        THEN step_wait_ns' =
               ComputeStepWaitNs(
                 FALSE, TRUE, now_ns, now_ns + 1,
                 EffInterval, act_next, last_warm_ns
               )
        ELSE \E base_dl \in Int :
               /\ base_dl >= now_ns + 1
               /\ base_dl <= EffU64Max
               /\ step_wait_ns' =
                    ComputeStepWaitNs(
                      FALSE, TRUE, now_ns, base_dl,
                      EffInterval, act_next, last_warm_ns
                    )
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, now_ns,
       last_warm_ns, warm_count, first_warm_ns, node_ready, signed_in_flight,
       not_ready_polled, crcVars
     >>

(* Fix 2 & Fix 3: Quiescent `CoreNode::step_inner` when `node_ready`, no signed
 * order is pending, and `!can_warm(now_ns)` (`crates/tickwire-runtime/src/node.rs:436-451`).
 * Because `next_due_ns` uses `checked_add`, either `next_warmup_due_ns()` is `None`
 * (`EffInterval == 0` or `anchor + interval > u64::MAX`) or `warm_due > now_ns`,
 * guaranteeing `step_wait_ns > 0`. *)
CoreNodeStepIdleReadyNotDue ==
  /\ warmup_configured
  /\ node_ready
  /\ ~signed_in_flight
  /\ ~CanWarm(now_ns, EffInterval, last_activity_ns, last_warm_ns)
  /\ now_ns < EffU64Max
  /\ sample_ns' = now_ns
  /\ last_runnable' = FALSE
  /\ last_warm_err' = FALSE
  /\ IF BoundedMode
     THEN step_wait_ns' =
            ComputeStepWaitNs(
              FALSE, TRUE, now_ns, now_ns + 1,
              EffInterval, last_activity_ns, last_warm_ns
            )
     ELSE \E base_dl \in Int :
            /\ base_dl >= now_ns + 1
            /\ base_dl <= EffU64Max
            /\ step_wait_ns' =
                 ComputeStepWaitNs(
                   FALSE, TRUE, now_ns, base_dl,
                   EffInterval, last_activity_ns, last_warm_ns
                 )
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, now_ns,
       last_activity_ns, last_warm_ns, warm_count, first_warm_ns,
       node_ready, signed_in_flight, not_ready_polled, crcVars
     >>

(* Quiescent `CoreNode::step_inner` executing `warm_once` (`Ok(true)`):
 * `record_warm(now_ns)` advances `last_warm_ns` so the next `checked_add` due
 * timestamp is either `None` (`> u64::MAX`) or `>= now_ns + EffInterval > now_ns`. *)
CoreNodeStepIdleReadyWarmOk ==
  /\ warmup_configured
  /\ node_ready
  /\ ~signed_in_flight
  /\ CanWarm(now_ns, EffInterval, last_activity_ns, last_warm_ns)
  /\ now_ns < EffU64Max
  /\ ((~BoundedMode) \/ (warm_count < EffU64Max))
  /\ LET warm_next == Max2(last_warm_ns, now_ns) IN
     /\ last_warm_ns' = warm_next
     /\ warm_count' = SaturatingAddU64(warm_count, 1)
     /\ first_warm_ns' = (IF warm_count = 0 THEN now_ns ELSE first_warm_ns)
     /\ sample_ns' = now_ns
     /\ last_runnable' = FALSE
     /\ last_warm_err' = FALSE
     /\ IF BoundedMode
        THEN step_wait_ns' =
               ComputeStepWaitNs(
                 FALSE, TRUE, now_ns, now_ns + 1,
                 EffInterval, last_activity_ns, warm_next
               )
        ELSE \E base_dl \in Int :
               /\ base_dl >= now_ns + 1
               /\ base_dl <= EffU64Max
               /\ step_wait_ns' =
                    ComputeStepWaitNs(
                      FALSE, TRUE, now_ns, base_dl,
                      EffInterval, last_activity_ns, warm_next
                    )
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, now_ns,
       last_activity_ns, node_ready, signed_in_flight, not_ready_polled, crcVars
     >>

(* Fix 1b: Quiescent `CoreNode::step_inner` when `self.maybe_warm_hot_path(now_ns)`
 * returns `Err(_)` (`Error::Busy` or `Error::Encoding`, `crates/tickwire-runtime/src/node.rs:436-439`):
 * `|| self.maybe_warm_hot_path(now_mono_ns).is_err()` triggers
 * `self.session.record_activity(now_mono_ns)`, preventing an expired `warm_due`
 * from causing a `0ns` `mio::Poll` spin. *)
CoreNodeStepIdleReadyWarmErr ==
  /\ warmup_configured
  /\ node_ready
  /\ ~signed_in_flight
  /\ EffInterval > 0
  /\ now_ns < EffU64Max
  /\ LET act_next == Max2(last_activity_ns, now_ns) IN
     /\ last_activity_ns' = act_next
     /\ sample_ns' = now_ns
     /\ last_runnable' = FALSE
     /\ last_warm_err' = TRUE
     /\ not_ready_polled' = TRUE
     /\ IF BoundedMode
        THEN step_wait_ns' =
               ComputeStepWaitNs(
                 FALSE, TRUE, now_ns, now_ns + 1,
                 EffInterval, act_next, last_warm_ns
               )
        ELSE \E base_dl \in Int :
               /\ base_dl >= now_ns + 1
               /\ base_dl <= EffU64Max
               /\ step_wait_ns' =
                    ComputeStepWaitNs(
                      FALSE, TRUE, now_ns, base_dl,
                      EffInterval, act_next, last_warm_ns
                    )
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, now_ns,
       last_warm_ns, warm_count, first_warm_ns, node_ready, signed_in_flight,
       crcVars
     >>

(* Defensive out-of-order caller sample (`0 <= jit_t < now_ns`) exercising
 * `saturating_sub` and `record_warm` monotonicity (`crates/tickwire-transport/src/warmup.rs:142-177`). *)
CoreNodeStepIdleBackwardJitter ==
  /\ warmup_configured
  /\ node_ready
  /\ ~signed_in_flight
  /\ now_ns > 0
  /\ now_ns < EffU64Max
  /\ IF BoundedMode
     THEN LET jit_t == now_ns - 1
              do_warm == CanWarm(jit_t, EffInterval, last_activity_ns, last_warm_ns)
              warm_next == IF do_warm THEN Max2(last_warm_ns, jit_t) ELSE last_warm_ns
          IN
          /\ ((~do_warm) \/ (warm_count < EffU64Max))
          /\ sample_ns' = jit_t
          /\ last_warm_ns' = warm_next
          /\ warm_count' = (IF do_warm THEN SaturatingAddU64(warm_count, 1) ELSE warm_count)
          /\ first_warm_ns' =
               (IF do_warm /\ warm_count = 0 THEN jit_t ELSE first_warm_ns)
          /\ last_runnable' = FALSE
          /\ last_warm_err' = FALSE
          /\ step_wait_ns' =
               ComputeStepWaitNs(
                 FALSE, TRUE, jit_t, jit_t + 1,
                 EffInterval, last_activity_ns, warm_next
               )
     ELSE \E jit_t \in Int, base_dl \in Int :
            /\ jit_t >= 0
            /\ jit_t < now_ns
            /\ base_dl >= jit_t + 1
            /\ base_dl <= EffU64Max
            /\ LET do_warm == CanWarm(jit_t, EffInterval, last_activity_ns, last_warm_ns)
                   warm_next == IF do_warm THEN Max2(last_warm_ns, jit_t) ELSE last_warm_ns
               IN
               /\ sample_ns' = jit_t
               /\ last_warm_ns' = warm_next
               /\ warm_count' = (IF do_warm THEN SaturatingAddU64(warm_count, 1) ELSE warm_count)
               /\ first_warm_ns' =
                    (IF do_warm /\ warm_count = 0 THEN jit_t ELSE first_warm_ns)
               /\ last_runnable' = FALSE
               /\ last_warm_err' = FALSE
               /\ step_wait_ns' =
                    ComputeStepWaitNs(
                      FALSE, TRUE, jit_t, base_dl,
                      EffInterval, last_activity_ns, warm_next
                    )
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, now_ns,
       last_activity_ns, node_ready, signed_in_flight, not_ready_polled, crcVars
     >>

(* Direct `SharedSessionState::maybe_warm_hot_path` / `RestSession::maybe_warm_hot_path`
 * call up to and including `now_ns == u64::MAX` (`crates/tickwire-binance-execution/src/lib.rs:801-837`,
 * `crates/tickwire-spot-execution/src/session.rs:1901-1958`). *)
SessionMaybeWarmAtBoundary ==
  /\ warmup_configured
  /\ ~signed_in_flight
  /\ CanWarm(now_ns, EffInterval, last_activity_ns, last_warm_ns)
  /\ ((~BoundedMode) \/ (warm_count < EffU64Max))
  /\ last_warm_ns' = Max2(last_warm_ns, now_ns)
  /\ warm_count' = SaturatingAddU64(warm_count, 1)
  /\ first_warm_ns' = (IF warm_count = 0 THEN now_ns ELSE first_warm_ns)
  /\ sample_ns' = now_ns
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, now_ns,
       last_activity_ns, node_ready, signed_in_flight, last_runnable,
       last_warm_err, not_ready_polled, step_wait_ns, crcVars
     >>

(* ========================================================================= *)
(* Part B Transition — Post-Fix `crc32c_masked` Parity (`Fix 4`)             *)
(* ========================================================================= *)

(* In finite TLC mode only, evaluate CRC-32C range combinations from the clean
 * pre-warmup state to avoid redundant explicit-state Cartesian product with
 * warmer states (`O(|Warmer| + |Crc|)` instead of `O(|Warmer| * |Crc|)`).
 * In Apalache (`BoundedMode = FALSE`), zero gating applies. *)
BoundedCrcGate ==
  (~BoundedMode) \/ (~warmup_configured /\ now_ns = 0)

(* Evaluates BOTH `tickwire_journal::crc32c::crc32c_masked` (`crates/tickwire-journal/src/crc32c.rs:182-195`)
 * and `tickwire_ipc::crc32c_masked` (`crates/tickwire-ipc/src/crc32c.rs:154-168`) on symbolic
 * `(buf_len, mask_start, mask_end, probe_idx)`. *)
EvaluateCrc32cMaskedPair ==
  /\ BoundedCrcGate
  /\ IF BoundedMode
     THEN \E blen \in FiniteBufLenSet, mstart \in FiniteMaskSet, mend \in FiniteMaskSet :
            \E pidx \in 0..3 :
              /\ ((blen = 0 /\ pidx = 0) \/ (blen > 0 /\ pidx < blen))
              /\ LET j_end   == Min2(mend, blen)
                     j_start == Min2(mstart, j_end)
                     i_start == Min2(mstart, blen)
                     i_end   == Max2(Min2(mend, blen), i_start)
                 IN
                 /\ buf_len' = blen
                 /\ mask_start' = mstart
                 /\ mask_end' = mend
                 /\ probe_idx' = pidx
                 /\ journal_start' = j_start
                 /\ journal_end' = j_end
                 /\ ipc_start' = i_start
                 /\ ipc_end' = i_end
     ELSE \E blen \in Int, mstart \in Int, mend \in Int, pidx \in Int :
            /\ blen >= 0 /\ blen <= EffMaxBufLen
            /\ mstart >= 0 /\ mstart <= EffU64Max
            /\ mend >= 0 /\ mend <= EffU64Max
            /\ ((blen = 0 /\ pidx = 0) \/ (blen > 0 /\ 0 <= pidx /\ pidx < blen))
            /\ LET j_end   == Min2(mend, blen)
                   j_start == Min2(mstart, j_end)
                   i_start == Min2(mstart, blen)
                   i_end   == Max2(Min2(mend, blen), i_start)
               IN
               /\ buf_len' = blen
               /\ mask_start' = mstart
               /\ mask_end' = mend
               /\ probe_idx' = pidx
               /\ journal_start' = j_start
               /\ journal_end' = j_end
               /\ ipc_start' = i_start
               /\ ipc_end' = i_end
  /\ UNCHANGED warmerVars

Next ==
  \/ EnableWarmupColdStartup
  \/ AdvanceClockMonotonic
  \/ SetNodeNotReady
  \/ RestoreNodeReady
  \/ BeginSignedInFlight
  \/ CompleteSignedInFlight
  \/ CoreNodeStepActive
  \/ CoreNodeStepIdleNotReady
  \/ CoreNodeStepIdleReadySignedBusy
  \/ CoreNodeStepIdleReadyNotDue
  \/ CoreNodeStepIdleReadyWarmOk
  \/ CoreNodeStepIdleReadyWarmErr
  \/ CoreNodeStepIdleBackwardJitter
  \/ SessionMaybeWarmAtBoundary
  \/ EvaluateCrc32cMaskedPair

Spec == Init /\ [][Next]_vars

(* ========================================================================= *)
(* Target Post-Fix Safety Invariants (Verified in Apalache + Z3 and TLC)     *)
(* ========================================================================= *)

TypeOK ==
  /\ warmup_configured \in BOOLEAN
  /\ startup_clock_ns \in Int /\ startup_clock_ns >= 0 /\ startup_clock_ns <= EffU64Max
  /\ startup_seeded \in BOOLEAN
  /\ now_ns \in Int /\ now_ns >= 0 /\ now_ns <= EffU64Max
  /\ sample_ns \in Int /\ sample_ns >= 0 /\ sample_ns <= EffU64Max
  /\ last_activity_ns \in Int /\ last_activity_ns >= 0 /\ last_activity_ns <= EffU64Max
  /\ last_warm_ns \in Int /\ last_warm_ns >= 0 /\ last_warm_ns <= EffU64Max
  /\ warm_count \in Int /\ warm_count >= 0 /\ warm_count <= EffU64Max
  /\ first_warm_ns \in Int /\ first_warm_ns >= 0 /\ first_warm_ns <= EffU64Max
  /\ node_ready \in BOOLEAN
  /\ signed_in_flight \in BOOLEAN
  /\ last_runnable \in BOOLEAN
  /\ last_warm_err \in BOOLEAN
  /\ not_ready_polled \in BOOLEAN
  /\ step_wait_ns \in Int /\ step_wait_ns >= 0 /\ step_wait_ns <= MaxIdleWaitNs
  /\ buf_len \in Int /\ buf_len >= 0 /\ buf_len <= EffMaxBufLen
  /\ mask_start \in Int /\ mask_start >= 0 /\ mask_start <= EffU64Max
  /\ mask_end \in Int /\ mask_end >= 0 /\ mask_end <= EffU64Max
  /\ probe_idx \in Int /\ probe_idx >= 0 /\ probe_idx <= EffMaxBufLen
  /\ journal_start \in Int /\ journal_start >= 0 /\ journal_start <= EffMaxBufLen
  /\ journal_end \in Int /\ journal_end >= 0 /\ journal_end <= EffMaxBufLen
  /\ ipc_start \in Int /\ ipc_start >= 0 /\ ipc_start <= EffMaxBufLen
  /\ ipc_end \in Int /\ ipc_end >= 0 /\ ipc_end <= EffMaxBufLen

(* 1. Fix 1 (`crates/tickwire-runtime/src/node.rs:430-452, 827-845, 894-905`):
 *    Whenever `!last_runnable` (including `!node_ready`, `last_warm_err`, and all
 *    idle ready states across `0 .. 2^64 - 1`), `mio::Poll` wait duration is strictly
 *    positive (`step_wait_ns > 0`) — zero non-blocking `0ns` CPU busy-spins. *)
NoZeroWaitSpinWhenNotReady ==
  /\ ((~last_runnable /\ ~node_ready) => (step_wait_ns > 0))
  /\ ((~last_runnable /\ last_warm_err) => (step_wait_ns > 0))
  /\ ((~last_runnable) => (step_wait_ns > 0))

(* 2. Fix 2 (`crates/tickwire-transport/src/warmup.rs:152-177`):
 *    `can_warm(t) <=> next_due_ns().is_some_and(|due| t >= due)` holds as an exact
 *    algebraic biconditional across the entire 64-bit `0 .. 2^64 - 1` domain,
 *    eliminating `Some(u64::MAX)` saturation skew. *)
ExactNextDueCanWarmBiconditional ==
  /\ (CanWarm(now_ns, EffInterval, last_activity_ns, last_warm_ns)
        <=> (NextDuePresent(EffInterval, last_activity_ns, last_warm_ns)
             /\ now_ns >= NextDueVal(EffInterval, last_activity_ns, last_warm_ns)))
  /\ (CanWarm(sample_ns, EffInterval, last_activity_ns, last_warm_ns)
        <=> (NextDuePresent(EffInterval, last_activity_ns, last_warm_ns)
             /\ sample_ns >= NextDueVal(EffInterval, last_activity_ns, last_warm_ns)))
  /\ (CanWarm(now_ns, WarmupInterval, last_activity_ns, last_warm_ns)
        <=> (NextDuePresent(WarmupInterval, last_activity_ns, last_warm_ns)
             /\ now_ns >= NextDueVal(WarmupInterval, last_activity_ns, last_warm_ns)))

(* 3. Fix 3 (`crates/tickwire-runtime/src/node.rs:853-874`,
 *    `crates/tickwire-binance-execution/src/lib.rs:695-717`,
 *    `crates/tickwire-spot-runner/src/live.rs:364-374`):
 *    Enabling warmup on a cold session seeds `last_activity_ns = startup_clock_ns`,
 *    guaranteeing `warm_count > 0 => now_ns - startup_clock_ns >= WarmupInterval`. *)
ColdStartupWaitsFullInterval ==
  /\ ((warmup_configured /\ WarmupInterval > 0) =>
        /\ startup_seeded
        /\ last_activity_ns >= startup_clock_ns)
  /\ ((warm_count > 0) =>
        /\ warmup_configured
        /\ WarmupInterval > 0
        /\ startup_seeded
        /\ first_warm_ns - startup_clock_ns >= WarmupInterval
        /\ now_ns - startup_clock_ns >= WarmupInterval)
  /\ ((WarmupInterval = 0 \/ ~warmup_configured) =>
        /\ warm_count = 0
        /\ last_warm_ns = 0)

(* 4. Fix 4 (`crates/tickwire-journal/src/crc32c.rs:180-195` vs
 *    `crates/tickwire-ipc/src/crc32c.rs:152-168`):
 *    Zero panics (`0 <= start <= end <= buf_len`), equal zero-mask lengths, exact
 *    `buf_len` conservation, and universal per-byte effective stream equivalence
 *    across all `buf_len` and arbitrary/inverted/OOB `mask_start`, `mask_end`. *)
JournalByteZeroed ==
  /\ journal_start <= probe_idx
  /\ probe_idx < journal_end

IpcByteZeroed ==
  /\ ipc_start <= probe_idx
  /\ probe_idx < ipc_end

TotalCrc32cMaskedParity ==
  /\ 0 <= journal_start /\ journal_start <= journal_end /\ journal_end <= buf_len
  /\ 0 <= ipc_start /\ ipc_start <= ipc_end /\ ipc_end <= buf_len
  /\ (journal_end - journal_start) = (ipc_end - ipc_start)
  /\ journal_start + (journal_end - journal_start) + (buf_len - journal_end) = buf_len
  /\ ipc_start + (ipc_end - ipc_start) + (buf_len - ipc_end) = buf_len
  /\ ((mask_start <= mask_end) => (journal_start = ipc_start /\ journal_end = ipc_end))
  /\ ((mask_start > mask_end) => (journal_start = journal_end /\ ipc_start = ipc_end))
  /\ ((buf_len > 0) => (JournalByteZeroed <=> IpcByteZeroed))

SafetyConjunction ==
  /\ TypeOK
  /\ NoZeroWaitSpinWhenNotReady
  /\ ExactNextDueCanWarmBiconditional
  /\ ColdStartupWaitsFullInterval
  /\ TotalCrc32cMaskedParity

(* ========================================================================= *)
(* Mode S2 Strengthened Symbolic Inductive Invariant (`IndInv`)              *)
(* ========================================================================= *)

(* CTI Lemma 1: Before `EnableWarmupColdStartup`, tracker fields remain in their
 * unseeded zero initial state so cold enablement reliably triggers seeding. *)
CTI_StartupPreEnableClean ==
  (~warmup_configured) =>
    /\ startup_clock_ns = 0
    /\ ~startup_seeded
    /\ last_activity_ns = 0
    /\ last_warm_ns = 0
    /\ warm_count = 0
    /\ first_warm_ns = 0
    /\ ~signed_in_flight
    /\ ~last_runnable
    /\ ~last_warm_err

(* CTI Lemma 2: Monotonic host clock ordering (`startup_clock_ns <= now_ns` and
 * all recorded tracker timestamps bounded above by `now_ns`). *)
CTI_ClockMonotonicOrder ==
  /\ startup_clock_ns <= now_ns
  /\ sample_ns <= now_ns
  /\ last_activity_ns <= now_ns
  /\ last_warm_ns <= now_ns
  /\ first_warm_ns <= now_ns
  /\ ((warm_count = 0) => (first_warm_ns = 0 /\ last_warm_ns = 0))
  /\ ((warm_count > 0) => (first_warm_ns <= last_warm_ns))

(* CTI Lemma 3: Functional register binding for `crc32c_masked` clamping outputs. *)
CTI_Crc_DerivedRegisters ==
  /\ ((buf_len = 0) => (probe_idx = 0))
  /\ ((buf_len > 0) => (0 <= probe_idx /\ probe_idx < buf_len))
  /\ journal_end = Min2(mask_end, buf_len)
  /\ journal_start = Min2(mask_start, journal_end)
  /\ ipc_start = Min2(mask_start, buf_len)
  /\ ipc_end = Max2(Min2(mask_end, buf_len), ipc_start)

IndInv ==
  /\ WarmupInterval >= 0
  /\ WarmupInterval <= EffU64Max
  /\ MaxIdleWaitNs >= 1
  /\ MaxIdleWaitNs <= SymMaxIdleWaitCap
  /\ SafetyConjunction
  /\ CTI_StartupPreEnableClean
  /\ CTI_ClockMonotonicOrder
  /\ CTI_Crc_DerivedRegisters

====
