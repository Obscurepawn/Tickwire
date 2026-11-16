# Input: Symbolic 64-Bit ORS Session Routing & Stale-Order Induction (`L1_SymOrsSessionRouting`)

Target scope: Snowcat-typed dual-engine (`BoundedMode`) symbolic verification of unified ORS `account_ref = 0..255` resolution, `strat_id = 0..254` (`ALL_ORS_STRATS = 255`), unbounded 64-bit `DopState` sequence continuity (`0 .. 2^63`), full 64-bit monotonic timestamp arithmetic (`0 .. 2^64 - 1`) for `max_order_age_ns` (`DEFAULT_MAX_ORDER_AGE_NS = 23_000`) `RejectStaleOrder` admission, and ascending two-slot `set_strat_enabled_collect` auto-cancel sweeps.

## Production Rust Anchors

- Multi-account `account_ref = 0..255` resolution & authorization:
  - `crates/tickwire-runtime-policy/src/lib.rs:426-453`
  - `crates/tickwire-runtime-policy/src/lib.rs:487-513`
  - `crates/tickwire-account/src/dop_state.rs:409-427`
- `DopState` login monotonic floor & `last_recv_seqnum` gap rejection:
  - `crates/tickwire-account/src/dop_state.rs:192-243`
  - `crates/tickwire-account/src/dop_state.rs:268-314`
  - `crates/tickwire-account/src/dop_state.rs:392-406`
  - `crates/tickwire-runtime/src/admin.rs:840-906`
- 64-bit `max_order_age_ns` (`DEFAULT_MAX_ORDER_AGE_NS = 23_000`) per-order `RejectStaleOrder` gate:
  - `crates/tickwire-runtime/src/ors.rs:61`
  - `crates/tickwire-runtime/src/ors.rs:1554-1621`
- Granular `set_strat_enabled` & ascending one-order-at-a-time auto-cancel sweep:
  - `crates/tickwire-account/src/dop_state.rs:336-389`
  - `crates/tickwire-account/src/order_manager.rs:1171-1333`
  - `crates/tickwire-account/src/order_manager.rs:2271-2383`
  - `crates/tickwire-runtime/src/admin.rs:450-468`
  - `crates/tickwire-runtime/src/ors.rs:2205-2218`
  - `crates/tickwire-runtime/src/ors.rs:2279-2336`
