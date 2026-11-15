---------------------------- MODULE counterexample ----------------------------

EXTENDS L5_SymWarmerCrcFixValidation

(* Constant initialization state *)
ConstInit ==
  BoundU64Max = 5
    /\ BoundedMode = FALSE
    /\ MaxIdleWaitNs = 1
    /\ WarmupInterval = 2

(* Initial state [_transition(0)] *)
State0 ==
  BoundU64Max = 5
    /\ BoundedMode = FALSE
    /\ MaxIdleWaitNs = 1
    /\ WarmupInterval = 2
    /\ buf_len = 1
    /\ first_warm_ns = 18446744073709551615
    /\ ipc_end = 1
    /\ ipc_start = 0
    /\ journal_end = 1
    /\ journal_start = 0
    /\ last_activity_ns = 18446744073709551614
    /\ last_runnable = TRUE
    /\ last_warm_err = TRUE
    /\ last_warm_ns = 0
    /\ mask_end = 0
    /\ mask_start = 0
    /\ node_ready = FALSE
    /\ not_ready_polled = FALSE
    /\ now_ns = 18446744073709551615
    /\ probe_idx = 0
    /\ sample_ns = 18446744073709551615
    /\ signed_in_flight = FALSE
    /\ startup_clock_ns = 18446744073709551615
    /\ startup_seeded = TRUE
    /\ step_wait_ns = 0
    /\ warm_count = 0
    /\ warmup_configured = FALSE

(* State1 [_transition(6)] *)
State1 ==
  BoundU64Max = 5
    /\ BoundedMode = FALSE
    /\ MaxIdleWaitNs = 1
    /\ WarmupInterval = 2
    /\ buf_len = 1
    /\ first_warm_ns = 18446744073709551615
    /\ ipc_end = 1
    /\ ipc_start = 0
    /\ journal_end = 1
    /\ journal_start = 0
    /\ last_activity_ns = 18446744073709551614
    /\ last_runnable = TRUE
    /\ last_warm_err = TRUE
    /\ last_warm_ns = 0
    /\ mask_end = 0
    /\ mask_start = 0
    /\ node_ready = FALSE
    /\ not_ready_polled = FALSE
    /\ now_ns = 18446744073709551615
    /\ probe_idx = 0
    /\ sample_ns = 18446744073709551615
    /\ signed_in_flight = FALSE
    /\ startup_clock_ns = 18446744073709551615
    /\ startup_seeded = TRUE
    /\ step_wait_ns = 0
    /\ warm_count = 0
    /\ warmup_configured = TRUE

(* The following formula holds true in the last state and violates the invariant *)
InvariantViolation ==
  (warmup_configured /\ WarmupInterval > 0)
    /\ (~startup_seeded \/ last_activity_ns < startup_clock_ns)

================================================================================
(* Created by Apalache on Sun Nov 15 04:30:57 CST 2026 *)
(* https://github.com/apalache-mc/apalache *)
