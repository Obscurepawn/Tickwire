# Strategy Intake Verification Levels

| Level | Scope and Result |
| --- | --- |
| [L0 ORS session routing](L0_ors_session_routing/05_output.md) | Explicit-state multi-account `account_ref = 0..255` resolution, unauthorized `(account_ref, strat_id)` rejection without session mutation, `DopState` restart floor + `msg_seqnum` gap rejection, `RejectStaleOrder` gate, and ascending `set_strat_enabled` auto-cancel sweep. PASS across 2 TLC configurations (`21,488` and `73,856` distinct states, 100% action coverage). |
| [L1 symbolic ORS session routing](L1_sym_ors_session_routing/05_output.md) | Snowcat-typed dual-engine (`BoundedMode`) verification over unbounded 64-bit `0 .. 2^64 - 1` `now_ns` / `create_ts_ns` / `max_order_age_ns` (`23_000` default) and `0 .. 2^63` sequence numbers. PASS under TLC (`15,936` distinct states, 100% coverage) and Apalache `typecheck` + `inductive` (`IndInv`, 4s) + `bmc` (`k = 8`, 28s). |
