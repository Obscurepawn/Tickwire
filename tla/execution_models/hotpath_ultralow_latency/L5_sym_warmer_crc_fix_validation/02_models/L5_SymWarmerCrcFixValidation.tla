---- MODULE L5_SymWarmerCrcFixValidation ----
(*
 * Post-Fix Symbolic Inductive & 64-Bit Boundary Validation for:
 *   1. Fix 1 (Idle Poll-Wait Non-Zero Sleep When Unready or On Warmup Error):
 *        crates/tickwire-runtime/src/ors.rs:2340-2380
 *        crates/tickwire-transport/src/warmup.rs:132-187
 *   2. Fix 2 (`WarmupTracker::next_due_ns` `checked_add` Exact Algebraic
 *      Biconditional with `WarmupTracker::can_warm` across `0 .. 2^64 - 1`):
 *        crates/tickwire-transport/src/warmup.rs:80-187
 *        crates/tickwire-transport/tests/warmup.rs:332-380
 *   3. Fix 3 (Cold-Startup `CLOCK_MONOTONIC` Seeding When Enabling Warmup):
 *        crates/tickwire-transport/src/warmup.rs:113-149
 *        crates/tickwire-binance-execution/src/warmup.rs:255-315
 *        crates/tickwire-spot-execution/src/warmup.rs:270-295
 *        crates/tickwire-runtime/src/session.rs:663-690
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
  step_wait_ns

(* @type: <<Bool, Int, Bool, Int, Int, Int, Int, Int, Int, Bool, Bool, Bool, Bool, Bool, Int>>; *)
vars == <<
  warmup_configured, startup_clock_ns, startup_seeded, now_ns, sample_ns,
  last_activity_ns, last_warm_ns, warm_count, first_warm_ns, node_ready,
  signed_in_flight, last_runnable, last_warm_err, not_ready_polled, step_wait_ns
>>

(* @type: Int; *)
Pow30 == 1073741824

(* @type: Int; *)
SymU64Max == (16 * Pow30 * Pow30) - 1

(* @type: Int; *)
SymMaxIdleWaitCap == 1000000000

(* @type: Int; *)
EffU64Max == IF BoundedMode THEN BoundU64Max ELSE SymU64Max

(* @type: (Int, Int) => Int; *)
Min2(a, b) == IF a <= b THEN a ELSE b

(* @type: (Int, Int) => Int; *)
Max2(a, b) == IF a >= b THEN a ELSE b

(* Exact `u64::saturating_sub` (`crates/tickwire-transport/src/warmup.rs:176`). *)
(* @type: (Int, Int) => Int; *)
SaturatingSubU64(a, b) == IF a > b THEN a - b ELSE 0

(* Exact `u64::saturating_add` (`crates/tickwire-transport/src/warmup.rs:146`). *)
(* @type: (Int, Int) => Int; *)
SaturatingAddU64(a, b) == IF a + b > EffU64Max THEN EffU64Max ELSE a + b

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

(* Post-Fix 1 deadline clamp + poll wait: clamps `base_dl` with `next_due_ns()`
 * STRICTLY when `ready` AND `NextDuePresent`. *)
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

(* Fix 3: Cold-startup warmup configuration & `CLOCK_MONOTONIC` seeding
 * (`crates/tickwire-transport/src/warmup.rs:113-139`,
 *  `crates/tickwire-spot-execution/src/warmup.rs:273-294`). *)
EnableWarmupColdStartup ==
  /\ ~warmup_configured
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
       step_wait_ns
     >>

AdvanceClockMonotonic ==
  /\ now_ns < EffU64Max
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
       last_runnable, last_warm_err, not_ready_polled, step_wait_ns
     >>

SetNodeNotReady ==
  /\ warmup_configured
  /\ node_ready
  /\ node_ready' = FALSE
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, now_ns, sample_ns,
       last_activity_ns, last_warm_ns, warm_count, first_warm_ns,
       signed_in_flight, last_runnable, last_warm_err, not_ready_polled,
       step_wait_ns
     >>

RestoreNodeReady ==
  /\ warmup_configured
  /\ ~node_ready
  /\ node_ready' = TRUE
  /\ UNCHANGED <<
       warmup_configured, startup_clock_ns, startup_seeded, now_ns, sample_ns,
       last_activity_ns, last_warm_ns, warm_count, first_warm_ns,
       signed_in_flight, last_runnable, last_warm_err, not_ready_polled,
       step_wait_ns
     >>

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
       last_warm_err, not_ready_polled, step_wait_ns
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
       last_warm_err, not_ready_polled, step_wait_ns
     >>

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
       not_ready_polled
     >>

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
       last_warm_ns, warm_count, first_warm_ns, node_ready, signed_in_flight
     >>

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
       not_ready_polled
     >>

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
       node_ready, signed_in_flight, not_ready_polled
     >>

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
       last_activity_ns, node_ready, signed_in_flight, not_ready_polled
     >>

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
       last_warm_ns, warm_count, first_warm_ns, node_ready, signed_in_flight
     >>

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
       last_activity_ns, node_ready, signed_in_flight, not_ready_polled
     >>

(* Direct warmer boundary invocation up to `now_ns == u64::MAX`
 * (`crates/tickwire-binance-execution/src/warmup.rs:282-315`,
 *  `crates/tickwire-spot-execution/src/warmup.rs:284-295`). *)
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
       last_warm_err, not_ready_polled, step_wait_ns
     >>

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

Spec == Init /\ [][Next]_vars

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

NoZeroWaitSpinWhenNotReady ==
  /\ ((~last_runnable /\ ~node_ready) => (step_wait_ns > 0))
  /\ ((~last_runnable /\ last_warm_err) => (step_wait_ns > 0))
  /\ ((~last_runnable) => (step_wait_ns > 0))

(* Fix 2 (`crates/tickwire-transport/src/warmup.rs:152-177`,
 *        `crates/tickwire-transport/tests/warmup.rs:332-380`):
 * `can_warm(t) <=> next_due_ns().is_some_and(|due| t >= due)` holds across `0 .. 2^64 - 1`. *)
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

SafetyConjunction ==
  /\ TypeOK
  /\ NoZeroWaitSpinWhenNotReady
  /\ ExactNextDueCanWarmBiconditional
  /\ ColdStartupWaitsFullInterval

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

CTI_ClockMonotonicOrder ==
  /\ startup_clock_ns <= now_ns
  /\ sample_ns <= now_ns
  /\ last_activity_ns <= now_ns
  /\ last_warm_ns <= now_ns
  /\ first_warm_ns <= now_ns
  /\ ((warm_count = 0) => (first_warm_ns = 0 /\ last_warm_ns = 0))
  /\ ((warm_count > 0) => (first_warm_ns <= last_warm_ns))

IndInv ==
  /\ TypeOK
  /\ WarmupInterval >= 0
  /\ WarmupInterval <= EffU64Max
  /\ MaxIdleWaitNs >= 1
  /\ MaxIdleWaitNs <= SymMaxIdleWaitCap
  /\ NoZeroWaitSpinWhenNotReady
  /\ ExactNextDueCanWarmBiconditional
  /\ ColdStartupWaitsFullInterval
  /\ CTI_StartupPreEnableClean
  /\ CTI_ClockMonotonicOrder

====
