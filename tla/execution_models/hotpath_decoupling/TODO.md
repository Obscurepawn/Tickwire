# Hot-path decoupling follow-up

All three Iteration 1 cross-layer boundary findings (`04_counterexamples.md`) have been resolved and covered by regression tests in `crates/tickwire-account/tests/windowed_eviction.rs` and `crates/tickwire-account-gateway/tests/windowed_eviction.rs`:

1. **Resolved**: Synchronized management-entry victim selection between `AccountGateway::reclaim_one_retired_management_entry` and `Account::evict_retired_management_admission`.
2. **Resolved**: Isolated confirmed `AmendKeepPriority` lifetime from `MAX_MANAGEMENT_ADMISSIONS` so active order amendments never starve `Cancel`/`Query` headroom.
3. **Resolved**: Ordered `AccountGateway::maybe_compact_journal` strictly before `self.account.clone()` across `submit`, `record_ingress_event`, `record_account_input`, and `abort`.
