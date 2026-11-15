# Modeling reasoning — `L1_SymDepthSync` (Symbolic 64-Bit & Unbounded Inductive `DepthSync` + `Book` Verification)

## 1. Finite-bound blind spots in explicit `L0_DepthSync`

Explicit-state `L0_DepthSync` (`tla/execution_models/binance_depth_sync/L0_coordinator/02_models/L0_DepthSync.tla`) had to shrink six protocol dimensions to keep explicit state-space exploration tractable (even then generating `721,365,672` states on `Capacity = 2`):

1. **Sequence ID domain (`MaxId = 2..3`)**: `BinanceSpot::classify` (`crates/tickwire-book/src/lib.rs:185-190`) evaluates `local_id + 1` in `u64` arithmetic; finite `0..3` domains never reach `u64::MAX = 18_446_744_073_709_551_615`.
2. **Timestamp & staleness domain (`MaxTime = 2`, `MaximumAge = 0`)**: Production `DepthSync::observe_clock` (`crates/tickwire-binance/src/depth_sync.rs:477-484`) and `Book::view` (`crates/tickwire-book/src/lib.rs:737-761`) operate over full 64-bit `MonoTime` nanoseconds (`0 .. 2^64 - 1`) and arbitrary `max_age_ns`.
3. **Buffer capacity & trace horizon (`Capacity = 1..2`, `MaxEpoch = 2`, `MaxGeneration = 2..3`, `MaxRequest = 2`)**: Explicit `VecDeque` sequences and `issued` token sets explode combinatorially with buffer capacity and reconnect/request counts.

## 2. Symbolic `QF_LIA` + Skolem + Head/Tail FIFO Architecture

`L1_SymDepthSync` (`02_models/L1_SymDepthSync.tla`) proves all 7 state invariants (`TypeOK`, `PendingBelongs`, `LiveHasCompleteReplay`, `FirstIsBuffered`, `ClosedNotVisible`, `FailuresClose`, `ViewFreshAtCreation`), all 6 transition properties (`OldCallbacksCannotMutate`, `RetryKeepsBufferedContent`, `StaleDoesNotRefresh`, `GenerationMonotonic`, `RequestMonotonic`, `SnapshotRetainsArrivalStamp`), and 64-bit `u64` overflow/underflow freedom (`u64_safe_ok`) across unbounded depth ($\forall k \ge 0$) and full `0 .. 2^64 - 1` `u64` parameters:

1. **Exact 64-Bit Difference-Logic `QF_LIA`**: Every timestamp, sequence ID, `cfg_capacity >= 1`, `cfg_max_epoch >= 2`, `cfg_max_gen >= 2`, `cfg_max_req >= 2`, and `cfg_max_age_ns >= 0` ranges symbolically over `0 .. 2^64 - 1` (`U64_MAX = 16 * 2^60 - 1`) with zero nonlinear terms. Specifically, `SpotPlusOneSafe` proves that `BinanceSpot::classify`'s `e.last <= b.id` guard precedes `b.id + 1`, guaranteeing `b.id < U64_MAX` and preventing `u64` overflow on `b.id + 1`.
2. **Pre-Solver Skolem Token & Buffer Witnesses (`sk_iss_*`, `sk_buf_*`)**: Replaces the unbounded `issued` `SUBSET` state variable with a nondeterministically latched Skolem witness token `(sk_iss_conn, sk_iss_gen, sk_iss_req)` in `Request`, and tracks arbitrary buffered delta generation membership via `sk_buf_gen`.
3. **Arbitrary-Capacity Symbolic FIFO (`buf_len \in 0..cfg_capacity`)**: Models `buf_len` over arbitrary `cfg_capacity \in 1..U64_MAX` alongside exact head (`buf_head_*`, `first_u`) and tail (`buf_tail_*`) deltas so both buffer-full `Overflow` (`buf_len = cfg_capacity`) and multi-delta atomic snapshot `Replay` (`SnapshotLive`, `SnapshotDelayedLive`, `SnapshotBridging`, `ReplayFailure`, `LateReplayFailure`) close in 1-step induction (`IndInv`).
4. **Dual-Engine Vacuous-Pass Gate (`BoundedMode \in BOOLEAN`)**: `ConstInit` sets `BoundedMode = FALSE` for Apalache (`0 .. 2^64 - 1`), while `L1_SymDepthSync_spot.cfg` and `L1_SymDepthSync_usdm.cfg` set `BoundedMode = TRUE` for `./scripts/check-tla.sh binance_depth_sync`, verifying 100% nonzero action coverage across all 22 `Next` transitions.
