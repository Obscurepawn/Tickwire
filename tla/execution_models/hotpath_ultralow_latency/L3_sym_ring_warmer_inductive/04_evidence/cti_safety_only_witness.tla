---------------------------- MODULE counterexample ----------------------------

EXTENDS L3_SymRingWarmerInductive

(* Constant initialization state *)
ConstInit ==
  BoundU64Max = 4
    /\ BoundedMode = FALSE
    /\ FirstSeq = 9223372036854775806
    /\ MaxBatch = 1
    /\ MaxBoundSteps = 2
    /\ RingCapacity = 2
    /\ WarmupInterval = 1

(* Initial state [_transition(0)] *)
State0 ==
  BoundU64Max = 4
    /\ BoundedMode = FALSE
    /\ FirstSeq = 9223372036854775806
    /\ MaxBatch = 1
    /\ MaxBoundSteps = 2
    /\ RingCapacity = 2
    /\ WarmupInterval = 1
    /\ borrow_active = FALSE
    /\ borrow_seq = 9223372036854775805
    /\ cached_min_rel = 9223372036854775805
    /\ claim_first = 9223372036854775805
    /\ claim_last = 9223372036854775807
    /\ cons_applied = 9223372036854775805
    /\ cons_read = 9223372036854775808
    /\ cons_rel = 9223372036854775808
    /\ dummy_c2s_nonce = 1
    /\ dummy_cli_exp_nonce = 1
    /\ dummy_s2c_nonce = 1
    /\ dummy_srv_exp_nonce = 1
    /\ env_min_rel = 9223372036854775808
    /\ last_activity_ns = 0
    /\ last_warm_elapsed = 1
    /\ last_warm_ns = 1
    /\ live_cash_reserved = 0
    /\ live_journal_seq = 0
    /\ live_tls_peer_nonce = 0
    /\ live_tls_tx_nonce = 0
    /\ node_ready = FALSE
    /\ not_ready_polled = FALSE
    /\ now_ns = 0
    /\ order_in_flight = FALSE
    /\ prod_writing = TRUE
    /\ published_seq = 9223372036854775808
    /\ scratch_reserved = 0
    /\ shadow_cash_reserved = 0
    /\ shadow_journal_seq = 0
    /\ startup_clock_ns = 0
    /\ startup_seeded = FALSE
    /\ step_wait_ns = 0
    /\ warm_count = 1

(* State1 [_transition(1)] *)
State1 ==
  BoundU64Max = 4
    /\ BoundedMode = FALSE
    /\ FirstSeq = 9223372036854775806
    /\ MaxBatch = 1
    /\ MaxBoundSteps = 2
    /\ RingCapacity = 2
    /\ WarmupInterval = 1
    /\ borrow_active = FALSE
    /\ borrow_seq = 9223372036854775805
    /\ cached_min_rel = 9223372036854775805
    /\ claim_first = 9223372036854775805
    /\ claim_last = 9223372036854775805
    /\ cons_applied = 9223372036854775805
    /\ cons_read = 9223372036854775808
    /\ cons_rel = 9223372036854775808
    /\ dummy_c2s_nonce = 1
    /\ dummy_cli_exp_nonce = 1
    /\ dummy_s2c_nonce = 1
    /\ dummy_srv_exp_nonce = 1
    /\ env_min_rel = 9223372036854775808
    /\ last_activity_ns = 0
    /\ last_warm_elapsed = 1
    /\ last_warm_ns = 1
    /\ live_cash_reserved = 0
    /\ live_journal_seq = 0
    /\ live_tls_peer_nonce = 0
    /\ live_tls_tx_nonce = 0
    /\ node_ready = FALSE
    /\ not_ready_polled = FALSE
    /\ now_ns = 0
    /\ order_in_flight = FALSE
    /\ prod_writing = FALSE
    /\ published_seq = 9223372036854775807
    /\ scratch_reserved = 0
    /\ shadow_cash_reserved = 0
    /\ shadow_journal_seq = 0
    /\ startup_clock_ns = 0
    /\ startup_seeded = FALSE
    /\ step_wait_ns = 0
    /\ warm_count = 1

(* The following formula holds true in the last state and violates the invariant *)
InvariantViolation == cons_read > published_seq

================================================================================
(* Created by Apalache on Sun Nov 15 02:58:41 CST 2026 *)
(* https://github.com/apalache-mc/apalache *)
