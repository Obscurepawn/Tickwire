# Counterexamples and retired boundary findings

Neither bounded configuration (`L1_WindowedPool.cfg` and `L1_WindowedPool_burst.cfg`) violated `TypeOK`, `ActiveNeverEvicted`, `WindowDedupPreserved`, `ManagementNeverStarved`, or `FrontierContinuity`.

Line-by-line transcription of the coupled `Account`, `AccountGateway`, and `AccountJournal` implementations surfaced three cross-layer boundary findings, all three of which were immediately fixed and locked with regression tests (`retired: fixed`):

1. **Retired (`fixed`) — Management victim-selection mismatch under heterogeneous command deadlines**:
   - Previously, `AccountGateway::reclaim_one_retired_management_entry` (`crates/tickwire-account-gateway/src/lib.rs`) minimized `(completed_mono_ns, evaluated_at)` while `Account::reclaim_one_retired_management_admission` (`crates/tickwire-account/src/lib.rs`) minimized `(u8::from(adm.deadline > now_mono_ns), adm.retired_mono_ns, adm.sequence)`.
   - **Fix**: Removed the `adm.deadline > now_mono_ns` divergence from `Account::reclaim_one_retired_management_admission`, added `Account::evict_retired_management_admission(intent)`, and updated `AccountGateway::reclaim_one_retired_management_entry` to evict the exact chosen `IntentKey` from both `gateway.entries` and `account.admissions` atomically, while `evict_retired_entries` guards `!self.account.has_admission(*key)`. Locked by `gateway_and_account_align_management_victim_selection_under_heterogeneous_deadlines` in `crates/tickwire-account-gateway/tests/windowed_eviction.rs`.
2. **Retired (`fixed`) — Confirmed `AmendKeepPriority` admissions pinning `MAX_MANAGEMENT_ADMISSIONS`**:
   - Previously, `Account::admit` (`crates/tickwire-account/src/admission.rs`) and `AccountGateway::submit` counted confirmed `AmendKeepPriority` admissions toward `MAX_MANAGEMENT_ADMISSIONS` (`4,096`) while excluding `a.confirmed` from management reclamation until the target order retired.
   - **Fix**: Excluded confirmed amendments (`Account::is_confirmed_amendment`) from the `MAX_MANAGEMENT_ADMISSIONS` `Cancel`/`Query` pool counter while bounding total active `amendments` at `MAX_FACTS` (`16,384`) and tying confirmed amendment active lifetime to its target order (`is_intent_active` / `intent_retired_mono_ns`). Locked by `confirmed_amendments_do_not_starve_cancel_or_query_management_headroom` in `crates/tickwire-account/tests/windowed_eviction.rs`.
3. **Retired (`fixed`) — Pre-compaction `Account` clone ordering across `poll` clock advances**:
   - Previously, `submit`, `record_ingress_event`, `record_account_input`, and `abort` in `crates/tickwire-account-gateway/src/lib.rs` cloned `let mut candidate = self.account.clone()` *before* invoking `self.maybe_compact_journal(...)`.
   - **Fix**: Moved `self.maybe_compact_journal(...)` strictly before `self.account.clone()` across all four journal-appending transitions in `crates/tickwire-account-gateway/src/lib.rs`.
