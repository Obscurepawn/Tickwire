---------------------------- MODULE counterexample ----------------------------

EXTENDS L3_SymRingWarmerInductive

(* Constant initialization state *)
ConstInit ==
  BoundU64Max = 4
    /\ BoundedMode = FALSE
    /\ FirstSeq = 1
    /\ MaxBatch = 1
    /\ MaxBoundSteps = 2
    /\ RingCapacity = 2
    /\ WarmupInterval = 18446744073709551614

(* Initial state [_transition(1)] *)
State0 ==
  BoundU64Max = 4
    /\ BoundedMode = FALSE
    /\ FirstSeq = 1
    /\ MaxBatch = 1
    /\ MaxBoundSteps = 2
    /\ RingCapacity = 2
    /\ WarmupInterval = 18446744073709551614
    /\ borrow_active = FALSE
    /\ borrow_seq = 0
    /\ cached_min_rel = 0
    /\ claim_first = 0
    /\ claim_last = 0
    /\ cons_applied = 0
    /\ cons_read = 0
    /\ cons_rel = 0
    /\ dummy_c2s_nonce = 0
    /\ dummy_cli_exp_nonce = 0
    /\ dummy_s2c_nonce = 0
    /\ dummy_srv_exp_nonce = 0
    /\ env_min_rel = 0
    /\ last_activity_ns = 0
    /\ last_warm_elapsed = 18446744073709551614
    /\ last_warm_ns = 0
    /\ live_cash_reserved = 0
    /\ live_journal_seq = 0
    /\ live_tls_peer_nonce = 0
    /\ live_tls_tx_nonce = 0
    /\ node_ready = TRUE
    /\ not_ready_polled = FALSE
    /\ now_ns = 18446744073709551614
    /\ order_in_flight = FALSE
    /\ prod_writing = FALSE
    /\ published_seq = 0
    /\ scratch_reserved = 0
    /\ shadow_cash_reserved = 0
    /\ shadow_journal_seq = 0
    /\ startup_clock_ns = 18446744073709551614
    /\ startup_seeded = FALSE
    /\ step_wait_ns = 1
    /\ warm_count = 0

(* State1 [_transition(9)] *)
State1 ==
  BoundU64Max = 4
    /\ BoundedMode = FALSE
    /\ FirstSeq = 1
    /\ MaxBatch = 1
    /\ MaxBoundSteps = 2
    /\ RingCapacity = 2
    /\ WarmupInterval = 18446744073709551614
    /\ borrow_active = FALSE
    /\ borrow_seq = 0
    /\ cached_min_rel = 0
    /\ claim_first = 0
    /\ claim_last = 0
    /\ cons_applied = 0
    /\ cons_read = 0
    /\ cons_rel = 0
    /\ dummy_c2s_nonce = 0
    /\ dummy_cli_exp_nonce = 0
    /\ dummy_s2c_nonce = 0
    /\ dummy_srv_exp_nonce = 0
    /\ env_min_rel = 0
    /\ last_activity_ns = 0
    /\ last_warm_elapsed = 18446744073709551614
    /\ last_warm_ns = 0
    /\ live_cash_reserved = 0
    /\ live_journal_seq = 0
    /\ live_tls_peer_nonce = 0
    /\ live_tls_tx_nonce = 0
    /\ node_ready = FALSE
    /\ not_ready_polled = FALSE
    /\ now_ns = 18446744073709551614
    /\ order_in_flight = FALSE
    /\ prod_writing = FALSE
    /\ published_seq = 0
    /\ scratch_reserved = 0
    /\ shadow_cash_reserved = 0
    /\ shadow_journal_seq = 0
    /\ startup_clock_ns = 18446744073709551614
    /\ startup_seeded = FALSE
    /\ step_wait_ns = 1
    /\ warm_count = 0

(* State2 [_transition(10)] *)
State2 ==
  BoundU64Max = 4
    /\ BoundedMode = FALSE
    /\ FirstSeq = 1
    /\ MaxBatch = 1
    /\ MaxBoundSteps = 2
    /\ RingCapacity = 2
    /\ WarmupInterval = 18446744073709551614
    /\ borrow_active = FALSE
    /\ borrow_seq = 0
    /\ cached_min_rel = 0
    /\ claim_first = 0
    /\ claim_last = 0
    /\ cons_applied = 0
    /\ cons_read = 0
    /\ cons_rel = 0
    /\ dummy_c2s_nonce = 0
    /\ dummy_cli_exp_nonce = 0
    /\ dummy_s2c_nonce = 0
    /\ dummy_srv_exp_nonce = 0
    /\ env_min_rel = 0
    /\ last_activity_ns = 0
    /\ last_warm_elapsed = 18446744073709551614
    /\ last_warm_ns = 0
    /\ live_cash_reserved = 0
    /\ live_journal_seq = 0
    /\ live_tls_peer_nonce = 0
    /\ live_tls_tx_nonce = 0
    /\ node_ready = FALSE
    /\ not_ready_polled = TRUE
    /\ now_ns = 18446744073709551614
    /\ order_in_flight = FALSE
    /\ prod_writing = FALSE
    /\ published_seq = 0
    /\ scratch_reserved = 0
    /\ shadow_cash_reserved = 0
    /\ shadow_journal_seq = 0
    /\ startup_clock_ns = 18446744073709551614
    /\ startup_seeded = FALSE
    /\ step_wait_ns = 0
    /\ warm_count = 0

(* The following formula holds true in the last state and violates the invariant *)
InvariantViolation ==
  ((~node_ready /\ not_ready_polled) /\ WarmupInterval > 0) /\ step_wait_ns <= 0

================================================================================
(* Created by Apalache on Sun Nov 15 02:59:09 CST 2026 *)
(* https://github.com/apalache-mc/apalache *)
