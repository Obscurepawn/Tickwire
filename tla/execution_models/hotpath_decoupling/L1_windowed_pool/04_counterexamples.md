# Counterexamples and retired boundary findings

Neither bounded configuration (`L1_WindowedPool.cfg` and `L1_WindowedPool_burst.cfg`) violated `TypeOK`, `ActiveNeverEvicted`, `WindowDedupPreserved`, `FillAmendmentExactness`, or `ManagementNeverStarved`.

Line-by-line transcription of the coupled `Account`, `FillRecordPool`, and `AccountGateway` implementations locked two stateless cross-layer boundary invariants (`retired: fixed` and regression-tested):

1. **Retired (`fixed`) — Coupled management victim-selection alignment across `AccountGateway` and `Account`**:
   - `AccountGateway::reclaim_one_retired_management_entry` (`crates/tickwire-account-gateway/src/lib.rs:576-610`) selects the oldest retired management `IntentKey` minimizing `(completed_mono_ns, evaluated_at, key)` and calls `Account::evict_retired_management_admission(key)` (`crates/tickwire-account/src/lib.rs:1026-1051`) to evict that exact `IntentKey` atomically from both `gateway.entries`, `gateway.pool.slot_bindings`, and `account.admissions`, while `evict_retired_entries` (`crates/tickwire-account-gateway/src/lib.rs:633-637`) guards `!self.account.has_admission(*key)` so gateway and reducer retention never drift.
2. **Retired (`fixed`) — Confirmed `AmendKeepPriority` / `Modify` admissions excluded from `Cancel`/`Query` management pool**:
   - `Account::admit` (`crates/tickwire-account/src/admission.rs:112-125`) and `AccountGateway::admit_in_memory` (`crates/tickwire-account-gateway/src/lib.rs:709-722`) exclude confirmed amendments (`is_confirmed_amendment`) and confirmed modifications (`is_confirmed_modification`) from the `MAX_MANAGEMENT_ADMISSIONS` (`4,096`) `Cancel`/`Query` pool counter while tying their active lifetime to their target order (`is_intent_active` / `intent_retired_mono_ns`).
