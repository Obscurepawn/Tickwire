# Strategy Intake Follow-up

**Verified.** Multi-account `account_ref = 0..255` resolution, unauthorized routing rejection (`INVALID_ACCOUNT_ID` / `REJECT_ROUTING_ID_MISMATCH`), `DopState` monotonic `max(login_seq, next_order_seqnum, 1)` floor across worker restarts, `last_recv_seqnum` gap rejection without cursor advancement, `max_order_age_ns` (`RejectStaleOrder`) per-order gating, and ascending one-order-at-a-time `set_strat_enabled` auto-cancel sweeps (`ADMIN_ACTION` + `cancel_on_ack`) pass both finite TLC ([L0](L0_ors_session_routing/05_output.md)) and 64-bit symbolic Apalache `QF_LIA` induction + BMC ([L1](L1_sym_ors_session_routing/05_output.md)).

**Watch.** Re-run `./scripts/check-tla.sh strategy_intake`, `anchors.sh check tla execution_models/strategy_intake/`, and Apalache `inductive` + `bmc` whenever `OrsRuntimePolicy`, `DopState`, `handle_new_order`, or `set_strat_enabled_collect` changes.
