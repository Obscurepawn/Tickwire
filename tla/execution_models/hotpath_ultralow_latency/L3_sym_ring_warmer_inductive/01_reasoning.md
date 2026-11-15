# Modeling reasoning — `L3_SymRingWarmerInductive`

## 1. Finite-bound blind spots in explicit-state `L0` and `L1`

1. **`L0_ZeroCopyAndCrcGate` (`RingCapacity = 2`, `MaxRingSeq = 3`, `Consumers = {1, 2}`)**:
   - Explicit TLC could only check a single wrap-around across 2 slots and sequence horizon `3` because tracking per-slot payloads and $N$ subscriber records causes $O(\text{MaxRingSeq}^{N + \text{RingCapacity}})$ Cartesian state explosion.
   - Multi-record batch claims (`Writer::claim(count)` where `1 <= count <= RingCapacity`, `crates/tickwire-shm/src/lib.rs:280-318`), arbitrary ring capacities (`64 .. 1,048,576`), arbitrary `FirstSeq >= 1`, and unbounded sequence horizons up to `SEQUENCE_LIMIT = 2^63` (`9,223,372,036,854,775,808`) were beyond explicit-state reach.
2. **`L1_IdleWarmerIsolation` (`MaxNow = 5`, `WarmupInterval = 2`, `MaxLiveOrders = 2`, `MaxWarmRuns = 2`)**:
   - Explicit TLC abstracted `last_warm_ns` with `-1` and started `now = 0, last_live_activity = 0`, missing:
     - Full 64-bit `u64` timestamp arithmetic (`0 .. 2^64 - 1`), `saturating_sub`, `saturating_add`, backward clock jitter (`now_ns < last_activity_ns`), and `u64::MAX` saturation boundaries in `WarmupTracker` (`crates/tickwire-transport/src/warmup.rs:88-186`);
     - Cold-startup `CLOCK_MONOTONIC` offsets (`startup_clock_ns > 0` while `WarmupTracker::new` initializes `last_activity_ns = 0, last_warm_ns = 0`);
     - `CoreNode::step_inner` + `CoreNode::run_with` (`crates/tickwire-runtime/src/node.rs:438-452, 833-846`) clamping `mio::Poll` sleep duration to `next_warmup_due_ns().saturating_sub(now_ns)` when `!node_ready` (`frozen || halted || !submission_ready`);
     - Unbounded interleavings of live order submissions, acknowledgements, market activity pulses, and `TlsLoopbackWarmer` record sequences (`0 .. 2^64 - 1`).

## 2. Symbolic techniques applied (`Apalache` + Z3 `QF_LIA`)

1. **Skolem Witness Consumer Projection (`c_star`) + Environment Watermark Summary (`env_min_rel`)**:
   - By universal symmetry over active subscribers, `ZeroNoAliasWhileBorrowed` fails for *some* subscriber iff it fails for a distinguished Skolem witness subscriber `c_star` (`cons_read`, `cons_rel`, `cons_applied`, `borrow_active`, `borrow_seq`), while all other subscribers collapse into a single symbolic minimum released watermark `env_min_rel` (`FirstSeq - 1 <= env_min_rel <= published_seq`).
2. **Pure `QF_LIA` Slot-Disjointness Formulation (`ZeroNoAliasWhileBorrowed`)**:
   - Replacing symbolic modulo `(w_seq - FirstSeq) % RingCapacity` (which would force `QF_NIA` over symbolic `RingCapacity`) with the exact linear window inequality `0 < w_seq - borrow_seq < RingCapacity` (`borrow_seq < claim_first /\ claim_last - borrow_seq < RingCapacity`) keeps Z3 strictly inside fast linear integer arithmetic (`QF_LIA`) while entailing physical slot disjointness for **all** `RingCapacity >= 2` and **all** sequences `<= 2^63` (no integer in `1 .. RingCapacity - 1` is divisible by `RingCapacity`).
3. **Relational `ConstInit` + Dual-Engine `BoundedMode` Gate**:
   - `ConstInit` binds `BoundedMode = FALSE` and constrains `RingCapacity >= 2`, `1 <= FirstSeq`, `FirstSeq + RingCapacity <= 2^63`, `1 <= MaxBatch <= RingCapacity`, and `0 <= WarmupInterval <= 2^64 - 1` symbolically.
   - `BoundedMode = TRUE` in TLC `.cfg` files runs a compact finite slice with 100% action coverage (`18/18` coverage entries) under `./scripts/check-tla.sh`.
4. **Mode S2 Symbolic Inductive Invariant (`IndInv`) + CTI Strengthening Loop**:
   - Checking `SafetyConjunction` alone under `run_apalache.sh inductive` produces Step-1 counterexamples-to-induction (CTIs) from unreachable predecessor states (e.g., `cached_min_rel > cons_rel` allowing `ProducerClaimBatch` to wrap onto `borrow_seq`, or `borrow_active = TRUE` with `cons_rel = borrow_seq`).
   - Four code-anchored strengthening lemmas (`CTI_Ring_CachedMinBound`, `CTI_Ring_ActiveBorrowPinsRelease`, `CTI_Ring_ActiveClaimWindow`, `CTI_Warmer_ControlCoupling`) close the inductive proof (`Init => IndInv`, `IndInv /\ Next => IndInv'`, `IndInv => SafetyConjunction`) for all trace depths $\forall k \ge 0$.
5. **Mode S1 64-Bit `u64` Boundary BMC (`k = 8`) + Diagnostic Edge-Case Queries**:
   - Bounded model checking over full `0 .. 2^64 - 1` timestamps verifies `SafetyConjunction` and exercises three targeted diagnostic queries (`Diag_StartupSeededBeforeFirstWarm`, `Diag_NextDueImpliesCanWarmAtU64Max`, `Diag_CoreNodeNoZeroWaitSpinWhenFrozen`) to surface concrete Z3 witnesses for startup/saturation/reactor-wait edge cases.
