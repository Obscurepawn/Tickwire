# Modeling reasoning — `L5_SymWarmerCrcFixValidation`

## Scope and post-fix invariants
1. **Non-zero idle poll sleep across unready/error turns (`crates/tickwire-runtime/src/ors.rs:2340-2380`, `crates/tickwire-transport/src/warmup.rs:113-177`)**:
   - Models ORS `RespWorker` / `WarmupTracker` deadline clamping across ready, unready (`frozen`, `halted`, `!submission_ready`), in-flight, and transient warmup-error states, proving `~last_runnable => step_wait_ns > 0` (`NoZeroWaitSpinWhenNotReady`).
2. **`WarmupTracker::next_due_ns` `checked_add` biconditional (`crates/tickwire-transport/src/warmup.rs:152-177`, `crates/tickwire-transport/tests/warmup.rs:332-380`)**:
   - Models `anchor.checked_add(warmup_interval_ns)` across `0 .. 2^64 - 1`, proving `can_warm(t) <=> next_due_ns().is_some_and(|due| t >= due)` (`ExactNextDueCanWarmBiconditional`) with zero `Some(u64::MAX)` saturation skew.
3. **Cold-startup `CLOCK_MONOTONIC` seeding (`crates/tickwire-binance-execution/src/warmup.rs:255-315`, `crates/tickwire-spot-execution/src/warmup.rs:270-295`, `crates/tickwire-transport/src/warmup.rs:113-149`)**:
   - Proves `warm_count > 0 => now_ns - startup_clock_ns >= WarmupInterval` (`ColdStartupWaitsFullInterval`) via inductive invariant `IndInv` (`02_models/L5_SymWarmerCrcFixValidation.tla:571-598`, strengthened with `CTI_StartupPreEnableClean` and `CTI_ClockMonotonicOrder`).
