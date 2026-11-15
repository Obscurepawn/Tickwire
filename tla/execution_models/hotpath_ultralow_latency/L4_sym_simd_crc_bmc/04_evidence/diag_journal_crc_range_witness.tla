---------------------------- MODULE counterexample ----------------------------

EXTENDS L4_SymSimdCrcBmc

(* Constant initialization state *)
ConstInit == BoundedMode = FALSE

(* Initial state [_transition(7)] *)
State0 ==
  BoundedMode = FALSE
    /\ backend_id = 0
    /\ block_base = 0
    /\ block_end = 1
    /\ buf_len = 0
    /\ checked_overflow_trapped = FALSE
    /\ clamped_end = 0
    /\ clamped_start = 0
    /\ digit_after_dot = TRUE
    /\ digit_before_dot = TRUE
    /\ dot_count = 2
    /\ fast_ok = FALSE
    /\ fast_units = 0
    /\ frac_scale = 2
    /\ frame_len = 1
    /\ frame_oversized = FALSE
    /\ has_invalid_char = TRUE
    /\ has_minus = TRUE
    /\ journal_masked_assert_ok = FALSE
    /\ last_read_end = 0
    /\ last_read_start = 0
    /\ last_shift_bits = 0
    /\ mantissa = 1
    /\ mask_end = 1
    /\ mask_start = 2
    /\ movemask_upper32_zero = TRUE
    /\ offset = 0
    /\ oracle_ok = FALSE
    /\ oracle_units = 0
    /\ prefix_rem8 = 0
    /\ prefix_words8 = 0
    /\ profile_id = 0
    /\ step_case = 0
    /\ str_len = 1
    /\ suffix_rem8 = 0
    /\ suffix_words8 = 0
    /\ total_digits = 20
    /\ ts_ms = 1
    /\ ts_ns = 1000000
    /\ ts_ok = TRUE
    /\ zero_chunks64 = 0
    /\ zero_rem64 = 0

(* The following formula holds true in the last state and violates the invariant *)
InvariantViolation == ~journal_masked_assert_ok

================================================================================
(* Created by Apalache on Sun Nov 15 02:59:12 CST 2026 *)
(* https://github.com/apalache-mc/apalache *)
