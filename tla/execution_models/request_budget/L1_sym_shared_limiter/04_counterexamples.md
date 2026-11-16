# Counterexamples & Inductive Witness Analysis — `L1_SymSharedLimiter`

## 1. Summary

- **Invariant Violations Found Against Production `SharedRateLimiter` / `OrsAccountQuota`:** `0`
- **Apalache Snowcat Typecheck:** `OK` (`All expressions are typed`)
- **Apalache Inductive Verification (`Init => IndInv`, `IndInv /\ Next => IndInv'`, `IndInv => SafetyConjunction`):** `NoError` across all 112 verification conditions and all 24 extracted symbolic transitions (`1,037` anchored source lines, `0` drift).
- **Apalache Symbolic BMC (`--length=2`):** `NoError`.

## 2. Inductive Refinement Notes During Proof Construction

1. **`u32` Fixed-Window Epoch Modulo Range (`MaxFixedEpoch`):**
   - `AtomicFixedWindow::try_take` (`crates/tickwire-quota/src/shared.rs:349-360`) packs `(epoch: u32) << 32 | (used: u32)` where `epoch = ((now_ns / interval_ns) as u32)`.
   - Over `0 .. 2^64 - 1` (`18,446,744,073,709,551,615 ns`), `now_ns / 1_000_000_000` spans `0 .. 18,446,744,073` (`4.294967295` full `2^32` cycles), so reachable `epoch` values span `0 .. U32_MAX` (`4,294,967,295`), whereas `FixedEpochIdx(U64_MAX) = 1,266,874,889`.
   - Z3 immediately caught an initial overly narrow `TypeOK` upper bound (`FixedEpochIdx(EffU64Max)`) at `last_now_ns = 4,294,967,295,000,000,000 ns` (`epoch = 4,294,967,295`) and confirmed that setting `MaxFixedEpoch == U32_MAX` preserves all safety properties across `2^32` epoch wraparounds without exceeding `limit`.
2. **Exact Piecewise-Linear `WrapU32` Encoding for `QF_LIA`:**
   - Because `0 <= (t \div 1_000_000_000) < 5 * 2^32` for every `t \in 0 .. 2^64 - 1`, replacing nested Euclidean modulo `((t \div 1000000000) % 2^32)` with the 5-branch piecewise-linear `WrapU32(t \div 1000000000)` preserves bit-for-bit identity across the entire unsigned 64-bit nanosecond range while keeping Z3 strictly inside fast linear integer arithmetic (`11s` for all 24 inductive step transitions).
