# Input: Finite ORS Session Routing and DOP Intake (`L0_OrsSessionRouting`)

Target scope: unified ORS strategy-intent admission, multi-account `account_ref = 0..255` session routing, `DopState` sequence continuity across worker restarts, microsecond `max_order_age_ns` (`DEFAULT_MAX_ORDER_AGE_NS = 23_000`) stale-order gating, and granular `(account_ref, strat_id)` disablement with ascending one-order-at-a-time auto-cancel sweeps.

## Production Rust Anchors

- Multi-account `account_ref` resolution (`0` -> strategy's first authorized `AccountSession`, `1..=255` -> explicit `AccountSession`, unauthorized `(account_ref, strat_id)` refused without session mutation):
  - `crates/tickwire-runtime-policy/src/lib.rs:426-453`
  - `crates/tickwire-runtime-policy/src/lib.rs:487-513`
  - `crates/tickwire-account/src/dop_state.rs:409-427`
- `DopState` login monotonic floor `max(login_seq, next_order_seqnum, 1)`, logout cursor reset, and `last_recv_seqnum` baseline / `+1` gap check:
  - `crates/tickwire-account/src/dop_state.rs:192-227`
  - `crates/tickwire-account/src/dop_state.rs:230-243`
  - `crates/tickwire-account/src/dop_state.rs:268-314`
  - `crates/tickwire-account/src/dop_state.rs:392-406`
  - `crates/tickwire-runtime/src/admin.rs:840-906`
- Microsecond `max_order_age_ns` (`DEFAULT_MAX_ORDER_AGE_NS = 23_000`) per-order `RejectStaleOrder` gate:
  - `crates/tickwire-runtime/src/ors.rs:61`
  - `crates/tickwire-runtime/src/ors.rs:1554-1621`
- Granular `set_strat_enabled` (`account_ref == 0` wildcard, `strat_id == 255` wildcard) and ascending one-order-at-a-time `set_strat_enabled_collect` sweep (`ADMIN_ACTION` / `cancel_on_ack`):
  - `crates/tickwire-account/src/dop_state.rs:336-389`
  - `crates/tickwire-account/src/order_manager.rs:1171-1333`
  - `crates/tickwire-account/src/order_manager.rs:2271-2383`
  - `crates/tickwire-runtime/src/admin.rs:450-468`
  - `crates/tickwire-runtime/src/ors.rs:2205-2218`
  - `crates/tickwire-runtime/src/ors.rs:2279-2336`
