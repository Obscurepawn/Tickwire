# Engineering Notes — `L1_SymSharedLimiter`

## 1. Scope & Fidelity Boundaries

- Models exact `u32`, `MAX_ROLLING_COUNT = 2^28 - 1`, `u64`, and `u128` bounds across `AtomicFixedWindow`, `AtomicRollingWindow` (with explicit 2-slot active sub-bucket coalescing, expiration, and `put_back` undo), per-account `418` freeze + cooldowns, per-IP `429` cooldowns, single-pick `PublishedRoutes::pick -> spend(local_ip) -> NACK`, and `reconcile_venue_usage` (`80%` / `100%` `ExchRateLimit` rising-edge latches).
- Complementary to `L0_SharedOrsLimiter` (finite-state multi-account, multi-IP interleaving) and `connection_router_quota/L2_sym_rate_limiter` (single-account `RateLimiter` CAS loop).

## 2. Re-Verification Triggers

Re-run `anchors.sh check tla execution_models/request_budget/`, `run_apalache.sh inductive`, and `./scripts/check-tla.sh request_budget` whenever any of the following change:
- `crates/tickwire-quota/src/shared.rs` (`AtomicFixedWindow`, `AtomicRollingWindow`, `SharedRateLimiter`, `OrsAccountQuota`, `reconcile_venue_usage`)
- `crates/tickwire-transport/src/order_router.rs` (`PublishedRoutes::pick`)
- `crates/tickwire-runtime/src/session.rs` (`AccountSession::pick_and_place_or_nack`)
