# Modeling reasoning — `L1_SymCalibration` (Symbolic 64-Bit & Unbounded Inductive `Calibrator` Verification)

## 1. Finite-bound & interpreter bottlenecks in explicit `L0_Calibration`

Explicit-state `L0_Calibration` (`tla/execution_models/clock/L0_calibration/02_models/L0_Calibration.tla`) faced three structural constraints caused by TLC's 32-bit `IntValue` representation and explicit state-space enumeration:

1. **10-limb recursive `Word` encoding (`[1..10 -> 0..999]`)**: To avoid 32-bit `IntValue` overflow on nanosecond and picosecond timestamps (`1700000000000 * 10^6 * 10^9`), `L0_Calibration` encoded every timestamp as a 10-element TLA+ function and evaluated carry/borrow/long-division (`Add`, `Sub`, `Half`, `FractionCeil`, `AdvanceTruth`) via `RECURSIVE` TLA+ operators (~20k..200k states/min in interpreted TLC).
2. **Discrete 5-point `Times` set (`{0, 4ms, 10ms, 40ms, 110ms}`)**: Explicit enumeration could only sample 5 discrete monotonic timestamps and 2 initial sub-millisecond offsets (`0us`, `999us`).
3. **Bounded call horizon (`MaxCalls <= 5..6`)**: Explicit BFS capped method invocations at 5–6 steps, exploring ~110M finite states across four `.cfg` profiles without proving unbounded multi-recalibration sequences ($\forall k \ge 0$) or full `0 .. 2^64 - 1` `u64::MAX` / `u128` overflow boundaries.

## 2. Symbolic `QF_LIA` + Skolem + Relativistic Drift-Cone Architecture

`L1_SymCalibration` (`02_models/L1_SymCalibration.tla`) verifies all 15 `tickwire_clock::Calibrator` (`crates/tickwire-clock/src/lib.rs:4-415`) safety invariants across **both** unbounded symbolic induction (`Apalache 0.63.0 + Z3`, $\forall k \ge 0$, `0 .. 2^64 - 1`) and exhaustive finite TLC smoke configurations:

1. **Exact Arbitrary-Precision `QF_LIA` Arithmetic**:
   - Represents monotonic nanoseconds (`0 .. 2^64 - 1`, `U64_MAX = 16 * 2^60 - 1 = 18446744073709551615`) and continuous server picoseconds (`0 .. U64_MAX * 10^9`, corresponding to Rust `u128` `PPB = 1_000_000_000` scaling in `scale_up` and `project`, `crates/tickwire-clock/src/lib.rs:401-414`) directly as native SMT `Int` variables.
   - Distributes `ScaleUpRaw`, `ScaleDownRaw`, `AdvanceTruthPs`, `DivByUnit`, `MulByUnit`, and `WindowPrecisionOk` across explicit integer literals (`< 2^31`) per `DriftCase` (`0 ppb`, `1 ppb` outward-rounding edge, `100 ppm`, `1,000 ppm`) and `TimeUnit` (`Microseconds`, `Milliseconds`), keeping 100% of SMT constraints inside decidable linear integer arithmetic (`QF_LIA`).
2. **Pre-Solver Skolem Witness Reduction (`sk_tok_*`, `sk_stamp_*`)**:
   - Instead of accumulating unbounded history sets `tokens \subseteq SampleToken` and `stamps \subseteq SignedTime` (which cause $2^N$ subset explosion), `Begin` and `Sign` nondeterministically latch arbitrary retained caller witnesses (`latch_tok \in BOOLEAN`, `latch_st \in BOOLEAN`). Universal stale-token and signed-timestamp properties (`SerialNotReused`, `StaleDoesNotTouchClock`, `SuccessfulSendBounded`, `ContinuousSendWithinWindow`) over all historical tokens/stamps reduce losslessly to quantifier-free assertions on `(sk_tok_*, sk_stamp_*)`.
3. **Relativistic Drift-Cone Ghost Anchors (`capture_truth_ps`, `anchor_truth_ps`)**:
   - Proving `ContinuousIntervalContainsTruth` and `ContinuousSendWithinWindow` inductively ($\forall k \ge 0$) requires relating current server truth `truth_ps` to `anchor_lo_ns` / `anchor_hi_ns` across arbitrarily many intermediate `Advance` steps.
   - `IndInv` records `capture_truth_ps` at `Capture` and `anchor_truth_ps` at `FinishNormal`, maintaining the two-sided relativistic drift-cone invariant:
     $$(t_{\text{now}} - t_{\text{anchor}}) \cdot (\text{PPB} - d) \le \text{truth\_ps} - \text{anchor\_truth\_ps} \le (t_{\text{now}} - t_{\text{anchor}}) \cdot (\text{PPB} + d)$$
     Combined with `anchor_lo_ns * PPB <= anchor_truth_ps <= anchor_hi_ns * PPB` and ceiling/floor outward rounding in `ProjLo` / `ProjHi`, Z3 closes 1-step induction over all 15 transitions without unrolling history.
4. **Dual-Engine Vacuous-Pass Gate (`BoundedMode \in BOOLEAN`)**:
   - `ConstInit` sets `BoundedMode = FALSE` for Apalache (`PPB = 10^9`, `EffU64Max = 2^64 - 1`), while `L1_SymCalibration_spot.cfg` and `L1_SymCalibration_usdm.cfg` set `BoundedMode = TRUE` for `./scripts/check-tla.sh clock`, proving 100% nonzero action coverage (`16/16` entries: `Init` + all 15 `Next` actions).
