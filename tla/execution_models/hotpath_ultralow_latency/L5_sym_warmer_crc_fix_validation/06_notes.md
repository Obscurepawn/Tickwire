# Post-fix CTI strengthening & validation notes — `L5_SymWarmerCrcFixValidation`

## 1. Mode S2 CTI Strengthening (`SafetyConjunction` -> `IndInv`)
- Checking `SafetyConjunction /\ Next => SafetyConjunction'` directly without coupling lemmas allows an unreachable predecessor state with `warmup_configured = FALSE` and `last_activity_ns > 0`, causing `EnableWarmupColdStartup` to skip cold seeding.
- **Synthesized CTI lemmas (`02_models/L5_SymWarmerCrcFixValidation.tla:571-598`)**:
  1. `CTI_StartupPreEnableClean` (`lines 571-580`): before `EnableWarmupColdStartup`, `last_activity_ns = 0 /\ last_warm_ns = 0 /\ ~startup_seeded`.
  2. `CTI_ClockMonotonicOrder` (`lines 583-590`): `startup_clock_ns <= now_ns`, `sample_ns <= now_ns`, `last_activity_ns <= now_ns`, `last_warm_ns <= now_ns`, `first_warm_ns <= now_ns`, and `warm_count = 0 => (first_warm_ns = 0 /\ last_warm_ns = 0)`.

## 2. Cross-Fix Interaction at `now_ns == u64::MAX`
- Pre-fix `next_due_ns` (`saturating_add`) returned `Some(u64::MAX)` whenever `anchor + warmup_interval_ns > u64::MAX`, which at `now_ns == u64::MAX` produced `can_warm(u64::MAX) == false` alongside `warm_due.saturating_sub(u64::MAX) == 0`. Replacing `saturating_add` with `checked_add` (`crates/tickwire-transport/src/warmup.rs:152-177`) makes `next_due_ns()` return `None` upon `u64` overflow, preserving `~last_runnable => step_wait_ns > 0` across all `0 .. 2^64 - 1` states.
