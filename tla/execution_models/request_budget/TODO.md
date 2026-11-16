# Process-wide ORS `SharedRateLimiter` follow-up

All process-wide ORS rate-limiter invariants across `crates/tickwire-quota/src/shared.rs`, `crates/tickwire-transport/src/order_router.rs`, `crates/tickwire-runtime/src/session.rs`, `crates/tickwire-runtime/src/ors.rs`, and `crates/tickwire-spot-execution/src/session.rs` are verified under both finite-state TLC (`L0_shared_ors_limiter`) and unbounded 64-bit/`u128` Apalache/Z3 symbolic induction (`L1_sym_shared_limiter`) with zero open counterexamples.
