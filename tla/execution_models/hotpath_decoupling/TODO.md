# Hot-Path Decoupling Follow-Up

All stateless windowed-pool invariants (`04_counterexamples.md`) are verified by TLC and covered by unit and integration tests in `crates/tickwire-account/tests/windowed_eviction.rs` and `crates/tickwire-account-gateway/tests/windowed_eviction.rs`:

1. **Verified**: Synchronized management-entry victim selection between `AccountGateway::reclaim_one_retired_management_entry` and `Account::evict_retired_management_admission`.
2. **Verified**: Isolated confirmed `AmendKeepPriority` lifetime from `MAX_MANAGEMENT_ADMISSIONS` so active order amendments never starve `Cancel`/`Query` headroom.
3. **Verified**: Scoped `AccountGateway::isolate_order_in_doubt` per order (`in_doubt_orders`) and separated `FillRecordOutcome::Recorded` (`TRADE_LITE` vs `TRADE`) from `FillRecordOutcome::Amended` (`amendment = true` fee delta settlement without double-booking filled lots).
