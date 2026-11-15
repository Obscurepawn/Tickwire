# Tickwire TLA+ 工作索引

[November 2026 verification audit](audits/2026-11-05/README.md) · [Per-crate coverage and drift](audits/2026-11-05/inventory.md)

## connection_router_quota / L2_sym_rate_limiter — PASS (2026-11-15)

- Mode: Dual-engine Symbolic Apalache `0.63.0` + Z3 (`Mode S2 Inductive` + `Mode S1 64-bit/u128 QF_LIA BMC` + canonical 2-slot `RollingWindow` + `ConstInit`) & explicit TLC smoke (`AllowBan = FALSE` & `AllowBan = TRUE`).
- Property: Unbounded-depth (`\forall k >= 0`), full 64-bit (`0 .. 2^64 - 1`) nanosecond epoch/sliding-window and `u128` `80%` reconciliation verification of `tickwire_quota::{RateLimiter, VenueRateLimiter}` (`WindowCapsRespected`, `CanAffordMatchesAllow` across duplicate `RateLimitId` spends, `TransactionalRollbackExact` across `Fixed`, `RollingCoalesced`, and `RollingPushed` undos, `CancelAndQueryIsolatedFromOrderCap`, `PerIpCooldownIsolation`, `LocalRejectAndClockRegressionNoBurn`, `U64AndU128OverflowSafe`).
- Apalache/Z3 & TLC: `IndInv` inductive proof `PASS` (`113` base VCs `6.346s` + `32` step branches `39.623s` + `109` entailment VCs `6.574s`); 64-bit/`u128` BMC `k=2` `PASS` (`71.104s`); TLC `104,416` / `110,407` distinct (`532,263` / `554,825` generated), depth `4` for both, `13/13` actions covered. [Run log](execution_models/connection_router_quota/L2_sym_rate_limiter/03_run_log.md)
- Finding: verified(0 bugs in `crates/tickwire-quota/src/rate_limiter.rs`). [Result and source map](execution_models/connection_router_quota/L2_sym_rate_limiter/05_output.md) · [QF_LIA & dual-engine notes](execution_models/connection_router_quota/L2_sym_rate_limiter/06_notes.md)
- Last synced: 2026-11-15 against post-fix working tree (`crates/tickwire-quota/src/rate_limiter.rs:492-847,1117-1469,1740-2232`).

## binance_depth_sync / L0_coordinator + L1_sym_coordinator — PASS (2026-11-15)

- Mode: Explicit TLC (`L0_DepthSync` 4 configs with congruent `DepthSyncView` + `@TLAPlusOperator` bytecode overrides) + Dual-engine Symbolic Apalache `0.63.0` + Z3 (`L1_SymDepthSync` `Mode S2 Inductive` + `Mode S1 64-bit BMC` + Skolem `sk_iss_*`/`sk_buf_gen` witnesses + `ConstInit`) & explicit TLC smoke (`Spot` & `UsdM`).
- Property: Explicit `Capacity = 1..2` FIFO replay (`L0`) plus unbounded-depth (`\forall k >= 0`), arbitrary-capacity (`cfg_capacity \in 1 .. 2^64 - 1`), full `0 .. 2^64 - 1` sequence/timestamp verification (`L1`) of all 7 state invariants (`PendingBelongs`, `LiveHasCompleteReplay`, `FirstIsBuffered`, `ClosedNotVisible`, `FailuresClose`, `ViewFreshAtCreation`), all 6 transition properties (`OldCallbacksCannotMutate`, `RetryKeepsBufferedContent`, `StaleDoesNotRefresh`, `GenerationMonotonic`, `RequestMonotonic`, `SnapshotRetainsArrivalStamp`), and `BinanceSpot::classify` `local_id + 1 <= u64::MAX` overflow freedom (`SpotPlusOneSafe`).
- Apalache/Z3 & TLC: `L0` TLC `73,269` / `73,897` primary (`08s`) and `3,491,866` / `3,492,316` scaling (`72s`–`74s` vs. `>3.5h` paused baseline, `23/23` actions); `L1` `IndInv` inductive proof `PASS` (`9.23s` parallel wall: `167` base VCs `7.163s` + `38` step branches `8.710s` + `155` entailment VCs `7.680s`), 64-bit BMC `k=5` `PASS` (`10.217s`), TLC `36,470` / `36,270` distinct (`1,472,132` / `1,466,425` generated), depth `7`, `23/23` actions covered. [L0 run log](execution_models/binance_depth_sync/L0_coordinator/03_run_log.md) · [L1 run log](execution_models/binance_depth_sync/L1_sym_coordinator/03_run_log.md)
- Finding: verified(0 bugs in `crates/tickwire-binance/src/depth_sync.rs` and `crates/tickwire-book/src/lib.rs`). [L0 output](execution_models/binance_depth_sync/L0_coordinator/05_output.md) · [L1 output](execution_models/binance_depth_sync/L1_sym_coordinator/05_output.md) · [QF_LIA & Skolem notes](execution_models/binance_depth_sync/L1_sym_coordinator/06_notes.md)
- Last synced: 2026-11-15 against post-fix working tree (`crates/tickwire-binance/src/depth_sync.rs:42-484`, `crates/tickwire-book/src/lib.rs:182-224,620-761`).

## clock / L1_sym_calibration — PASS (2026-11-15)

- Mode: Dual-engine Symbolic Apalache `0.63.0` + Z3 (`Mode S2 Inductive` + `Mode S1 64-bit/u128 QF_LIA BMC` + Skolem `SampleToken`/`SignedTime` witnesses + `ConstInit`) & explicit TLC smoke (`Spot` & `UsdM`).
- Property: Unbounded-depth (`\forall k >= 0`), full 64-bit (`0 .. 2^64 - 1`) and exact `u128` `PPB` drift-cone verification of all 15 `tickwire_clock::Calibrator` safety invariants (`FaultClears`, `PendingIdentity`, `AnchorIdentity`, `SerialNotReused`, `StaleDoesNotTouchClock`, `ObservedTimeMonotone`, `SuccessfulReadBounded`, `ExactSignedTimestamp`, `SuccessfulSendBounded`, `ContinuousIntervalContainsTruth`, `ContinuousSendWithinWindow`, `RejectedSampleRetainsAnchor`, `ReadNeverExtendsExpiry`, `FaultRevokesCalls`) without 10-limb `[1..10 -> 0..999]` `Word` encoding or 5-timestamp `Times` sampling blind spots.
- Apalache/Z3 & TLC: `IndInv` inductive proof `PASS` (`38.944s` parallel wall: `151` base VCs `4.873s` + `15` step actions `38.501s` + `143` entailment VCs `4.715s`); 64-bit BMC `k=2` `PASS` (`13.542s`); TLC `31,296` / `28,967` distinct (`189,516` / `179,988` generated), depth `6` for both, `16/16` actions covered. [Run log](execution_models/clock/L1_sym_calibration/03_run_log.md)
- Finding: verified(0 bugs in `crates/tickwire-clock/src/lib.rs`). [Result and source map](execution_models/clock/L1_sym_calibration/05_output.md) · [QF_LIA linearization notes](execution_models/clock/L1_sym_calibration/06_notes.md)
- Last synced: 2026-11-15 against post-fix working tree (`crates/tickwire-clock/src/lib.rs:4-415`).

## hotpath_ultralow_latency / L5_sym_warmer_crc_fix_validation — PASS (2026-11-15)

- Mode: Dual-engine Symbolic Apalache `0.63.0` + Z3 (`Mode S2 Inductive` + `Mode S1 64-bit BMC` + `ConstInit`) & explicit TLC smoke (`WarmupInterval = 2` & `WarmupInterval = 0`).
- Property: Post-fix `/vibe-tla fix` validation across unbounded depth (`\forall k >= 0`) and full `0 .. 2^64 - 1` `u64` timestamps, intervals, buffer lengths, and `Range<usize>` bounds proving strictly positive `CoreNode::run_with` `mio::Poll` sleep on every non-runnable turn including unready/frozen/halted states and transient warmup errors (`NoZeroWaitSpinWhenNotReady`), exact `can_warm(t) <=> next_due_ns().is_some_and(|due| t >= due)` equivalence via `checked_add` (`ExactNextDueCanWarmBiconditional`), cold-startup `CLOCK_MONOTONIC` seeding enforcing `warm_count > 0 => now_ns - startup_clock_ns >= WarmupInterval` (`ColdStartupWaitsFullInterval`), and panic-free byte-stream equivalence between `tickwire_journal::crc32c::crc32c_masked` and `tickwire_ipc::crc32c_masked` (`TotalCrc32cMaskedParity`).
- Apalache/Z3 & TLC: `IndInv` inductive proof `PASS` (`94` base VCs `3.790s` + `15` step actions `4.918s` + `76` entailment VCs `3.663s`); 64-bit BMC `k=8` `PASS` (`52.939s`); TLC `3,525` / `1,719` distinct (`170,769` / `163,201` generated), depths `14` and `13`, `16/16` actions covered. [Run log](execution_models/hotpath_ultralow_latency/L5_sym_warmer_crc_fix_validation/03_run_log.md)
- Finding: verified(0 bugs; closes all 4 findings from `L3` and `L4`). [Result and source map](execution_models/hotpath_ultralow_latency/L5_sym_warmer_crc_fix_validation/05_output.md) · [CTI & validation notes](execution_models/hotpath_ultralow_latency/L5_sym_warmer_crc_fix_validation/06_notes.md)
- Last synced: 2026-11-15 against post-fix working tree (`crates/tickwire-runtime/src/node.rs:166-188,260-277,420-485,826-874,894-905`, `crates/tickwire-transport/src/warmup.rs:80-187`, `crates/tickwire-binance-execution/src/lib.rs:695-717,801-844`, `crates/tickwire-spot-runner/src/live.rs:364-374,549-560`, `crates/tickwire-journal/src/crc32c.rs:180-195`, `crates/tickwire-ipc/src/crc32c.rs:152-168`).

## hotpath_ultralow_latency / L3_sym_ring_warmer_inductive — PASS (retired/fixed 3 boundary/scheduling findings) (2026-11-15)

- Mode: Dual-engine Symbolic Apalache `0.63.0` + Z3 (`Mode S2 Inductive` + `Mode S1 64-bit BMC` + Skolem consumer `c_star` + `ConstInit`) & explicit TLC smoke (`WarmupInterval = 2` & `WarmupInterval = 0`).
- Property: Unbounded-depth (`\forall k >= 0`), arbitrary-capacity (`RingCapacity >= 2`), 64-bit sequence (`1 <= seq <= 2^63`) zero-copy `RecordBorrow` slot disjointness (`ZeroNoAliasWhileBorrowed`, `WatermarkMonotonicOrder`) via 4 CTI lemmas (`CTI_Ring_CachedMinBound`, `CTI_Ring_ActiveBorrowPinsRelease`, `CTI_Ring_ActiveClaimWindow`, `CTI_Warmer_ControlCoupling`), plus full `0 .. 2^64 - 1` `WarmupTracker` / `TlsLoopbackWarmer` live business & AEAD nonce isolation (`LiveBusinessStateInvariance`, `LiveTlsNonceSynchronization`, `DummyLoopbackTlsPairSynchronization`, `WarmupStrictlyRespectsTrackerPolicy`, `DisabledWarmupNeverFires`).
- Apalache/Z3 & TLC: `IndInv` inductive proof `PASS` (`111` base VCs `2.304s` + `17` step actions `2.495s` + `94` entailment VCs `2.303s`); 64-bit BMC `k=8` `PASS` (`7.516s`); TLC `87,552` / `19,304` distinct (`212,709` / `46,980` generated), depths `25` and `21`, `18/18` actions covered. [Run log](execution_models/hotpath_ultralow_latency/L3_sym_ring_warmer_inductive/03_run_log.md)
- Finding: retired(fixed, validated by L5_SymWarmerCrcFixValidation). [Result and source map](execution_models/hotpath_ultralow_latency/L3_sym_ring_warmer_inductive/05_output.md) · [CTI & candidate notes](execution_models/hotpath_ultralow_latency/L3_sym_ring_warmer_inductive/06_notes.md)
- Fix: Route `CoreNode::step_inner` idle turns through `maybe_warm_hot_path` (recording activity on unready or `Err(_)` and clamping `next_deadline` strictly when ready), replace `WarmupTracker::next_due_ns` `saturating_add` with `checked_add`, and seed `record_activity(now_mono_ns)` on cold-startup warmup enablement. [Validated model](execution_models/hotpath_ultralow_latency/L5_sym_warmer_crc_fix_validation/05_output.md)
- Last synced: 2026-11-15 against post-fix working tree.

## hotpath_ultralow_latency / L4_sym_simd_crc_bmc — PASS (retired/fixed 1 API contract asymmetry) (2026-11-15)

- Mode: Dual-engine Symbolic Apalache `0.63.0` + Z3 (`Mode S1 64-bit/128-bit QF_LIA BMC` + `Mode S2 Inductive` + `ConstInit`) & explicit TLC smoke.
- Property: All-length (`0 <= frame_len <= 65536`) `AVX2`/`SSE2`/`NEON`/`SWAR`/scalar memory-read and `< 64` bit-shift safety (`ZeroVectorOobAndValidShift`), exact mathematical equivalence between `simd_json::parse_positive_units` + `StepSpec` (`u64` + `u128` `gcd`-reduced fallback) and `Decimal::parse` + `feed::units` across full 19-digit mantissas (`0 .. 10^19 - 1`) up to `i64::MAX = 2^63 - 1` (`FastVsOracleExactEquivalence`), full 64-bit `millis.checked_mul(1_000_000)` all-profile overflow trapping (`NoSilentArithmeticWrap`), and exact `tickwire_ipc::crc32c_masked` clamping + 8B/64B slice partitioning across arbitrary/inverted/OOB `Range<usize>` (`CrcMaskedSlicePartitionExact`).
- Apalache/Z3 & TLC: BMC `k=6` `PASS` (`33.369s`); `IndInv` inductive proof `PASS` (`122` base VCs `3.318s` + `6` step actions `7.163s` + `92` entailment VCs `2.825s`); TLC `816` distinct (`816` generated), depth `12`, `7/7` actions covered. [Run log](execution_models/hotpath_ultralow_latency/L4_sym_simd_crc_bmc/03_run_log.md)
- Finding: retired(fixed, validated by L5_SymWarmerCrcFixValidation). [Result and source map](execution_models/hotpath_ultralow_latency/L4_sym_simd_crc_bmc/05_output.md) · [Audit notes](execution_models/hotpath_ultralow_latency/L4_sym_simd_crc_bmc/06_notes.md)
- Fix: Replace `assert!` in `tickwire_journal::crc32c::crc32c_masked` with graceful `Range<usize>` clamping matching `tickwire_ipc::crc32c_masked`. [Validated model](execution_models/hotpath_ultralow_latency/L5_sym_warmer_crc_fix_validation/05_output.md)
- Last synced: 2026-11-15 against post-fix working tree.

## hotpath_ultralow_latency / L0_zerocopy_crc_gate — PASS (2026-11-15)

- Mode: fault injection (`CrcEnabled = FALSE` default & `CrcEnabled = TRUE` opt-in).
- Property: Lifetime-pinned `#[repr(C)]` 512-byte / 8-byte-aligned pure-integer `&MarketRecord` / `MarketStateRef` zero-copy SHM borrows (`ZeroNoAliasWhileBorrowed`, `ZeroCopyLayoutAndHeaderSoundness`), `RecordHeader` flag `0x08` (`CHECKSUM_PRESENT`) and CRC field consistency across `crc32` modes (`CrcGateModeConsistency`), and physical WAL SHA-256 `frame_hash` chain + `repeated_total_len` + `END_MAGIC` corruption quarantine and torn-tail truncation without CRC-32C (`WalRecoverySoundWithoutCrc`).
- TLC: 288,000 / 887,900 distinct states (1,154,681 / 3,740,035 generated); depths 27 and 28; 14/14 actions covered. [Run log](execution_models/hotpath_ultralow_latency/L0_zerocopy_crc_gate/03_run_log.md)
- Finding: verified(0 bugs across `tickwire-ipc`, `tickwire-shm`, `tickwire-market-source`, `tickwire-strategy-sdk`, and `tickwire-journal`). [Result and source map](execution_models/hotpath_ultralow_latency/L0_zerocopy_crc_gate/05_output.md)
- Last synced: 2026-11-15 against `e5d9088` (2,075 line anchors).

## hotpath_ultralow_latency / L1_idle_warmer_isolation — PASS (2026-11-15)

- Mode: fault / interleaving & negative contrast guard (`WarmupInterval = 2` & `WarmupInterval = 0`).
- Property: Bit-for-bit invariance of live `Account`, `AccountJournal`, `Origins` / `intent_seq`, SHM `published`, and `VenueRateLimiter` tokens across `WarmHotPath` (`LiveBusinessStateInvariance`), live `rustls::ClientConnection` AEAD nonce synchronization across interleaved `0..N` warmups and live orders (`LiveTlsNonceSynchronization`), isolated loopback `TlsLoopbackWarmer` client/server nonce lockstep (`DummyLoopbackTlsPairSynchronization`), and strict idle-window + disabled-mode gating (`WarmupStrictlyRespectsIdlePolicy`).
- TLC: 45,550 / 3,690 distinct states (151,624 / 14,110 generated); depths 18 and 16; 9/9 actions covered. [Run log](execution_models/hotpath_ultralow_latency/L1_idle_warmer_isolation/03_run_log.md)
- Finding: verified(0 bugs across `tickwire-transport`, `tickwire-spot-execution`, `tickwire-spot-runner`, `tickwire-binance-execution`, and `tickwire-runtime`). [Result and source map](execution_models/hotpath_ultralow_latency/L1_idle_warmer_isolation/05_output.md)
- Last synced: 2026-11-15 against `e5d9088` (1,364 line anchors).

## hotpath_ultralow_latency / L2_simd_decode_overflow — PASS (2026-11-15)

- Mode: fault injection & oracle equivalence (`Bybit` `release` & `Okx` `bench`).
- Property: Universal `FastOrFallbackDecode == SerdeOracleDecode` output and `Session` state equivalence across unescaped, reordered, escaped, chunk-boundary truncated, off-grid, crossed, and overflowing frames (`ExactOracleEquivalence`), strict positive spread and exact `TickStep` / `LotStep` divisibility on published `SpotQuote`s with `Unavailable` mapping on crossed/one-sided books (`TickLotAndPositiveSpreadSoundness`), zero silent wrap across `dev`/`test`/`release`/`bench` (`NoSilentArithmeticWrap`), and zero hot-path heap allocations or out-of-bounds vector loads (`ZeroHotPathHeapAllocation`).
- TLC: 998 / 998 distinct states (27,369 / 27,369 generated); depth 7 for both; 12/12 actions covered. [Run log](execution_models/hotpath_ultralow_latency/L2_simd_decode_overflow/03_run_log.md)
- Finding: verified(0 bugs across `tickwire-md` and workspace `Cargo.toml`). [Result and source map](execution_models/hotpath_ultralow_latency/L2_simd_decode_overflow/05_output.md)
- Last synced: 2026-11-15 against `e5d9088` (1,899 line anchors).

## market_making_microstructure / L0_stp_modify — PASS (retired/fixed 4 bugs) (2026-11-14)

- Mode: fault injection.
- Property: Lock-free `StpBook` atomic BBO (`best_bid_ticks`, `best_ask_ticks`), dual-price `aggressive_ticks` (`max` for `Buy`, `min` for `Sell`), `Vec::swap_remove` compaction, single-in-flight `PendingModify`, worst-case Spot principal and Futures `shadow_holds` reservations across venue clamping, partial/full fills superseding `PendingModify`, pre-admission local refusal non-burn, and two-phase Spot `new_client_id -> orderId` `AmendAmbiguous` query resolution.
- TLC: 23,923 / 232,019; depth 12 for both. [Run log](execution_models/market_making_microstructure/L0_stp_modify/03_run_log.md)
- Finding: retired(fixed, 4 cross-layer bugs in `supersede_pending_modify_on_terminal` + Spot `cancelReplace` reopen, historical/pending modify fill authorization bounds, `PerOrderQueryDriver` Spot `new_client_id` two-phase probe + WS rotated-leg binding, and pre-admission `VenueRateLimiter` / `Origins` non-burn). [Counterexamples](execution_models/market_making_microstructure/L0_stp_modify/04_counterexamples.md) · [Result and source map](execution_models/market_making_microstructure/L0_stp_modify/05_output.md)
- Last synced: 2026-11-14 against `4f2d47a`.

## market_making_microstructure / L1_projected_fill — PASS (retired/fixed 1 bug) (2026-11-14)

- Mode: fault injection.
- Property: Out-of-order cumulative status frames (`CumulativeQuote` & `LimitFallback`), settled-first and duplicate `FillEvent`s (`trade_id` dedup), `CHECK_STP` opposite-side `self_cross` suppression, `Modify` quantity increases after partial fill, exact `exch_quote_qty` incremental gap-price denominators, and strategy `FillWatermarkTracker` zero-double-count `effective_delta` inventory skew.
- TLC: 676 / 356; depths 14 and 12. [Run log](execution_models/market_making_microstructure/L1_projected_fill/03_run_log.md)
- Finding: retired(fixed, 1 cross-layer denominator-mismatch bug in `Order` and `OrderWatermarkEntry` tracking dedicated `exch_quote_qty` and `exch_avg_qty` watermarks across `LimitFallback` and settled-first fills). [Counterexamples](execution_models/market_making_microstructure/L1_projected_fill/04_counterexamples.md) · [Result and source map](execution_models/market_making_microstructure/L1_projected_fill/05_output.md)
- Last synced: 2026-11-14 against `4f2d47a`.

## connection_router_quota / L0_pool_router — PASS (retired/fixed 4 bugs) (2026-11-14)

- Mode: fault injection.
- Property: Pooled `OrderRouter` (`Exclusive` & `Shared` slots, warmup round-robin, `9/8` EWMA hysteresis, epoch fencing), `DialBaton` reconnect guarding, `Status::Aborted` pre-read reaping, and multiplexed `WsOwner` pre-EOF draining.
- TLC: 27,599 / 18,129; depth 13 for both. [Run log](execution_models/connection_router_quota/L0_pool_router/03_run_log.md)
- Finding: retired(fixed, 4 cross-layer bugs in `poll_pool_dials_filtered`, `Status::Aborted` inflight/read ordering, `WsOwner` `draining_eof` + isolated WS `Close` flush, and `AccountRunner` `Origins`/`inflight` cleanup). [Counterexamples](execution_models/connection_router_quota/L0_pool_router/04_counterexamples.md) · [Result and source map](execution_models/connection_router_quota/L0_pool_router/05_output.md)
- Last synced: 2026-11-14 against `cef23ae` (2,468 line anchors).

## connection_router_quota / L1_multi_ip_quota — PASS (retired/fixed 2 bugs) (2026-11-14)

- Mode: fault injection.
- Property: Multi-window `FixedWindow` and bounded-ring `RollingWindow` caps, reverse-order `put_back` rollback, `can_afford` vs. `allow` agreement across duplicate `RateLimitId` spends, `Order`/`Cancel`/`Request` isolation, and per-IP `429` failover vs. `418` account freeze.
- TLC: 20,428 / 2,349; depths 12 and 10. [Run log](execution_models/connection_router_quota/L1_multi_ip_quota/03_run_log.md)
- Finding: retired(fixed, 2 cross-layer bugs in post-admission `allow_quota_*` gating and `can_afford` duplicate-ID / `u64` coalescing / preset parity). [Counterexamples](execution_models/connection_router_quota/L1_multi_ip_quota/04_counterexamples.md) · [Result and source map](execution_models/connection_router_quota/L1_multi_ip_quota/05_output.md)
- Last synced: 2026-11-14 against `cef23ae` (2,023 line anchors).

## quote_delivery / L0_pair — retired (fixed) (2026-11-05)

- Mode: transcription.
- Property: A legal unavailable pair permanently retires the account subscription.
- TLC: 447 / 8,288; violating trace at depth 5. [Run log](execution_models/quote_delivery/L0_pair/03_run_log.md)
- Finding: retired(fixed, validated by L0_pair_v2). [Result and source map](execution_models/quote_delivery/L0_pair/06_review.md)
- Last synced: 2026-11-05 against `9b2ed63`.

- Fix: Consume valid same-generation observations while the invalidated trading gate stays closed. [Fixed model](execution_models/quote_delivery/L0_pair_v2/05_output.md)

## strategy_intake / L0_shared_cash — retired (fixed) (2026-11-05)

- Mode: fault injection.
- Property: A pre-admission foreign-order refusal terminates the shared account process.
- TLC: 480 / 2,968; violating trace at depth 8. [Run log](execution_models/strategy_intake/L0_shared_cash/03_run_log.md)
- Finding: retired(fixed, validated by L1_management_refusal). [Result and source map](execution_models/strategy_intake/L0_shared_cash/06_review.md)
- Last synced: 2026-11-05 against `9b2ed63`.

- Fix: Deliver a verified management refusal to its member and continue polling the shared account. [Fixed model](execution_models/strategy_intake/L1_management_refusal/05_output.md)

## cash_evidence / L0_terminal_fees — retired (fixed) (2026-11-05)

- Mode: fault injection.
- Property: A rejected fee decision survives in a journal that the next writer cannot replay.
- TLC: 532 / 6,986; violating trace at depth 9. [Run log](execution_models/cash_evidence/L0_terminal_fees/03_run_log.md)
- Finding: retired(fixed, validated by L1_fee_closure). [Result and source map](execution_models/cash_evidence/L0_terminal_fees/06_review.md)
- Last synced: 2026-11-05 against `9b2ed63`.

- Fix: Check the proposed fee closure before sync and recognize the recorded legacy contradiction during frozen replay. [Fixed model](execution_models/cash_evidence/L1_fee_closure/05_output.md)

## config_activation / L0_file_receipt — PASS (2026-11-05)

- Mode: fault injection.
- Property: File installation, WAL receipt, current instance identity and idempotent retry.
- TLC: 199 / 1,563; depths 17 and 31. [Run log](execution_models/config_activation/L0_file_receipt/03_run_log.md)
- Finding: none. [Result and source map](execution_models/config_activation/L0_file_receipt/05_output.md)
- Last synced: 2026-11-05 against `9b2ed63`.

## request_budget / L0_shared_window — PASS (2026-11-05)

- Mode: fault injection.
- Property: One or two budget windows preserve recovery headroom and the actual debit after stop and reclaim.
- TLC: 10,420 / 303,062; depths 18 and 21. [Run log](execution_models/request_budget/L0_shared_window/03_run_log.md)
- Finding: none. [Result and source map](execution_models/request_budget/L0_shared_window/05_output.md)
- Last synced: 2026-11-05 against `9b2ed63`.

## risk_limits / L0_cash_envelope — PASS (2026-11-05)

- Mode: transcription.
- Property: Protected cash and account/member exposure under two spot orders and partial executions.
- TLC: 278 / 327; depth 7 for both. [Run log](execution_models/risk_limits/L0_cash_envelope/03_run_log.md)
- Finding: none. [Result and source map](execution_models/risk_limits/L0_cash_envelope/05_output.md)
- Last synced: 2026-11-05 against `9b2ed63`.

## spot_roundtrip / L0_live_account — PASS (2026-11-05)

- Mode: fault injection.
- Property: From canonical quote decision to durable admission, first byte, fill and successful restart in the spot runner.
- TLC: 903 / 195 / 559 / 28; two separate stronger expectations have counterexamples by design. [Run log](execution_models/spot_roundtrip/L0_live_account/03_run_log.md)
- Finding: none. [Result and source map](execution_models/spot_roundtrip/L0_live_account/05_output.md)
- Last synced: 2026-11-05 against `9b2ed63`.

## binance_roundtrip / L0_spot_rest — PASS (2026-11-05)

- Mode: fault injection.
- Property: Binance spot CoreNode order turn, clock and quotas, admission, fill and worker report.
- TLC: 838 / 432; witnesses at depths 27 and 22. [Run log](execution_models/binance_roundtrip/L0_spot_rest/03_run_log.md)
- Finding: none. [Result and source map](execution_models/binance_roundtrip/L0_spot_rest/05_output.md)
- Last synced: 2026-11-05 against `9b2ed63`.

## worker_handover / L0_market_consumer — retired (fixed) (2026-11-05)

- Mode: fault injection.
- Property: The host can reject a valid receipt when its worker stamps the message between the host’s clock sample and socket read.
- TLC: 4,668 / 22,968; violating trace at depth 7. [Run log](execution_models/worker_handover/L0_market_consumer/03_run_log.md)
- Finding: retired(fixed, validated by L1_receipt_time). [Independent review and native reproduction](execution_models/worker_handover/L0_market_consumer/06_review.md)
- Last synced: 2026-11-05 against `9b2ed63`.

- Fix: Sample the control clock after each packet read and pass that sample to the receipt validator. [Fixed model](execution_models/worker_handover/L1_receipt_time/05_output.md)

# Historical records

Some original entries group several levels. The [inventory](audits/2026-11-05/inventory.md) lists each source lock separately.

当前源码提示：submission L1/L2 的 Mock 分组重放语义已被统一事件流替换，下方旧条目保留其历史结论；现行中止重放见 mock_abort，真实业务日志见 business_journal。

## execution / submission / L0_commit_gate — PASS（2026-10-20）

- Mode: transcription
- Property: 固定 profile 下发送前完成证据；无重复 begin_send；Unknown 预算；有界准入。
- Code anchors: `crates/tickwire-execution/src/lib.rs:60-71` gate；`:138-181` 订单与预算；`:262-392` reserve/commit/send/abort/unknown/ack。完整路径和逐行哈希见该级 anchors.lock。
- TLC: primary 2,236 distinct / depth 15；scaling 9,076 distinct / depth 15；全部 12 个 Next 动作触发。
- Finding: none
- Reviewed: 2026-10-20，ipc_codec 独立审阅，补 NoBytesRejected 后重跑 PASS。
- Output: [L0 输出](execution_models/submission/L0_commit_gate/05_output.md)
- Last synced: 2026-10-20，完整函数范围重新锚定。

## execution / submission / L1_crash_receipt — PASS（2026-10-20）

- Mode: fault-injection（日志失败、记录已存但回执丢失、未知发送、exec 崩溃与重启）
- Property: send 前保存记录、无重复 exchange.submit、依赖完整性、Unknown 预算、空/非空 journal 重启冻结。
- Code anchors: `crates/tickwire-mock/src/lib.rs:54-73` objects；`:116-145` journal；`:163-170` exchange；`:210-235` runtime submit；`:265-278` restart；`crates/tickwire-execution/src/lib.rs:504-528` recover；`crates/tickwire-execution/src/ports.rs:21-27` 传输结果。
- TLC: primary 1,744 distinct / depth 14；scaling 96,828 distinct / depth 22；pending 186,148 distinct / depth 22；全部 19 个 Next 动作触发。
- Finding: none
- Reviewed: 2026-10-20，risk_core 独立源码/模型审阅通过，并复核 PendingResponse 最终增量。
- Output: [L1 输出](execution_models/submission/L1_crash_receipt/05_output.md)
- Last synced: 2026-10-20，完整函数范围重新锚定。

两个本轮独立源码审查发现已由 Rust Red/Green 确认并修复，记录于 [L1 notes](execution_models/submission/L1_crash_receipt/06_notes.md)；不是 TLC 发现。L2 已完成交易结果记录与恢复失败分支验证，见下方条目；既有 PASS 不覆盖实盘传输/真实持久化或完整生产系统。

## execution / submission / L2_economic_facts — PASS v2（2026-10-20）

- Mode: transcription，完整同步 record/restart；单结算资产交易结果记录与一次有序重放。
- Property: 唯一事件账本、池合计、重复幂等、失败原子性、缺失成交预留、费用证据阻止 NoBytes 释放、deficit/恢复失败冻结。
- Code anchors: `crates/tickwire-execution/src/lib.rs:138-245`、`:262-339`、`:352-528`；`crates/tickwire-mock/src/lib.rs:104-114`、`:236-278`；`crates/tickwire-execution/src/ports.rs:51-80`；444 行 anchors 检查无漂移。
- TLC: primary 24,036 / long 280,458 / boundary 1,495,032 / scaling 1,485,288 distinct；完整深度分别 8/9/11/11；每 cfg 全部 18 个 Next 动作生成。
- Finding: retired(fixed, validated by L2_EconomicFacts_v2)：L2-001 恢复重放失败后旧 Engine 未冻结；原 boundary 25,367 distinct、6 状态反例，真实 Rust Red/Green 与 v2 四配置验证完成。
- Reviewed: root 独立源码/模型审查；ipc_codec 独立确认实际 API 可达性与冻结修复；risk_core 独立复跑 Rust 回归。
- Output: [L2 输出](execution_models/submission/L2_economic_facts/05_output.md)
- Last synced: 2026-10-20，源文件/model/cfg SHA-256 已归档；历史失败源码匹配提交 cd8bde270ac257b296100979621c60c64f965e42。

## broadcast_ring / L0_publish_borrow_reclaim — PASS（2026-10-20）

- Mode: transcription，固定身份的单 bank；容量 64、有限序号/窗口和任意停顿。
- Property: 完整发布、连续释放/应用、保守最小水位、活跃借用及未读槽位不被覆盖、实际 cursor 退出证明、Freeze。
- Code anchors: `crates/tickwire-shm/src/lib.rs` 的 Producer/Reader 与 `src/mapped.rs` 的发布、借用、Quiesce/detach；638 行检查无漂移，完整源码哈希见 results.json。
- TLC: primary 29,889 / two_readers 930,364 / wrap 11,324 / pending_wrap 12,834 / wrap_two 40,796 distinct；各配置所列动作全部生成后继。
- Finding: 模型无新候选。实际 cursor 拒绝、Quiesce 幂等和 guard 的 NonNull 表示修复来自独立源码审查；分别保留 Rust 回归或语言契约依据，未归因为 TLC 发现。
- Reviewed: root 与 risk_core 独立审查模型；market_core 复核映射生命周期与 NonNull 修复。
- Output: [L0 输出](execution_models/broadcast_ring/L0_publish_borrow_reclaim/05_output.md)
- Last synced: 2026-10-20，NonNull 表示不改变协议迁移，源码哈希/anchors 更新后沿用相同模型结果。

## control_requests / L0_registry + L1_session — PASS（2026-10-20）

- L0: 370 / 1,570 / 13,442 distinct，depth 6/8/10；L1: 1,566 / 3,926 / 3,804 distinct，depth 8/8/9。
- Source: tickwire-control 的 Session/MockRequestStore；独立 ipc_codec 源码/模型审核通过。每个配置全部 Next 动作生成后继。
- 范围：已认证请求、串行调用、一次出票、重试先于版本、终态不覆盖、Mock 失败；L1 增加同时间域租约及永久关闭。字节解析、实际 OS 身份、业务副作用和持久 owner 恢复另验。
- 源码审查完善响应 opcode/Status version 关联和期限耗尽错误分类，保留 Rust Red/Green；没有将其归因为 TLC 反例。
- Output: [L0](execution_models/control_requests/L0_registry/05_output.md)、[L1](execution_models/control_requests/L1_session/05_output.md)。

## physical_journal / L0_batch_sync_recovery — PASS（2026-10-20）

- 1,816 / 2,695 / 45,883 distinct；depth 18/20/28；三配置 16 个 Next 动作均生成后继。
- 固定单段、帧粒度物理内容见证；写入/同步/收据分离、poison、崩溃、末尾修复与可选外部可信前缀。root 独立源码/模型审阅，ipc_codec 独立帧 codec 审阅。
- 段/manifest 目录事务的形式模型待 L1；实际文件、短写、同步失败与四个 SIGKILL 边界有 Rust 回归。设备掉电与跨主机复制不在当前结论内。
- Output: [L0](execution_models/physical_journal/L0_batch_sync_recovery/05_output.md)。

## book_state / L0_lifecycle + L1_coverage — PASS（2026-10-20）

- L0 Spot 主/扩展：1,392 / 8,499 distinct；USD-M 主/扩展：1,880 / 10,524 distinct；四配置 depth 6，各 19 个 Next 动作生成后继。
- L1 Bid 主/扩展：1,471 / 7,050 distinct；Ask 主/扩展：1,471 / 8,244 distinct；四配置 depth 6，各 15 个 Next 动作生成后继。
- Source: `crates/tickwire-book/src/lib.rs` SHA-256 `37beb8dcd515b76a8c3bec0a2cd46a1c3c2431375276810b45818b8b35fd04a3`，与已提交性能候选相同。
- L0 转录单所有者 API、Spot/USD-M 判定顺序、Bridge/Live/Invalid/rebuild、调用时 freshness、双侧失败原子性。L1 使用真实完整侧的隐藏价位见证，检查 Known 数量/最优价正确，Unknown 不伪装成零，增量保持 coverage。
- 限定：有限 generation/id/time/price 域；L1 假定适配器提供真实快照及完整已接受变化，没有证明 L0→L1 形式细化；不验证数组排序/合并、性能或未实现的自动风险门禁。
- Finding: 无源码缺陷候选；模型初次语法修正单独归档，未当作系统反例。root 独立源码/模型审阅通过。
- Output: [L0](execution_models/book_state/L0_lifecycle/05_output.md)、[L1](execution_models/book_state/L1_coverage/05_output.md)。

## mock_abort / L0_ordered_abort — PASS（2026-10-20）

- 当前 Mock 中止先保存再释放，intent/fact 按实际事件顺序重放；零成交、零费用、LocalDurable 范围。
- 三配置不同状态 2,051 / 457 / 102,149，深度 26/22/40；各 15 个 Next 动作均触发。
- root、risk 独立源码及模型审阅通过；运行结果及 426 行锚点见 [输出](execution_models/mock_abort/L0_ordered_abort/05_output.md)。
- 旧 submission L1/L2 保留历史源码与结果；当前完整成交、费用、终态组合仍需新版本模型。

## recovery_closure / L0_publish_verify — PASS（2026-10-20）

- 区分对象与 manifest、不可变绑定、重命名可见、目录同步和结果交付；多个未完成目录项可分别保留或丢失。
- 三配置不同状态 2,345 / 15,097 / 3,898，深度 51/56/66；每组 Init 和 22 个 Next 动作均触发。
- 独立审阅指出首版故障域遗漏，修正后保持原规模重跑；初版保存在 pre-review，没有归为 Rust 缺陷。
- 原始运行保留 203 行锚点，范围与记录见 [输出](execution_models/recovery_closure/L0_publish_verify/05_output.md)。字节解析、输入采集和业务授权不由该模型证明。
- 本次同步更新为 214 行锚点，持久化状态公式与三配置不变，沿用原始 TLC 结果；kind 13～15 的形状检查和账户摘要不在旧模型覆盖内。旧源码、锁及日志保留于 `docs/validation/account-recovery-binding/pre-sync/`。

## business_journal / L0_commit_abort_recovery — PASS（2026-10-20）

- 串行 runtime：授权和恢复依赖完成、WAL 写入与同步、最终发送检查、中止保存后释放，以及冻结重放。
- 三配置不同状态 72 / 345 / 879，深度 14/26/28；每组 23 个 Next 动作均触发。
- root、ipc 独立审阅通过；332 行锚点及运行记录见 [输出](execution_models/business_journal/L0_commit_abort_recovery/05_output.md)。
- 固定身份、无成交与费用的有限范围；后续纳入交易结果组合、writer 变化、镜像与恢复后激活。最终检查边界不表示网卡实际发包期限。

## join_owner / L0_pending_activation — PASS（2026-10-20）

- 保留 S、构造与交付失败保留门禁、双资源就绪、真实 cursor 追平、原登记版本、幂等激活与协作退出。
- 四配置不同状态 2,181 / 321,256 / 172 / 7,652，深度 23/35/14/25；主配置各 22 个 Next 动作、两组 wrap 各 16 个动作全部触发。
- risk 独立源码、模型及最终日志审阅通过，1221 行锚点；[输出](execution_models/join_owner/L0_pending_activation/05_output.md)。
- 共用截止时间、固定认证身份与有限借用窗口；持久登记、独立截止、bank 轮换和死亡回收待后续。旧 ring 模型只发生行号变化，720 行锚点重审通过，状态公式未变。

## business_journal / L1_command_binding — PASS（2026-10-20）

- 源码：execution command_digest；codec schema 2；recovery kind 13；BusinessJournal 提交及 Runtime 恢复的精确命令依赖检查。
- 三配置：376 / 2664 / 7326 个不同状态；10 个不变量、全部 29 个 Next 动作可达。
- 单所有者、完整日志前缀、同步失败与最多两次崩溃；same-intent 变更冻结，恢复保持原摘要。
- 精确同请求幂等返回由 Rust 回归覆盖；完整命令语义和网络字节仍待网关组合。下层四个当前模型只更新源码锚点，公式不变，旧锁与映射见 docs/validation/command-binding。

## transport / L0_first_output — PASS（2026-10-20）

- 对应真实非阻塞 TCP/TLS 的首次应用接受、即时门禁、等待、部分接受、冲刷与零接受终态。
- 四配置不同状态 688 / 3,928 / 5,944 / 2,832；TLS 配置 19 个、明文配置 16 个 Next 动作均生成后继。
- 源码、模型与真实网络回归经独立审查；231 行锚点无漂移，完整记录见 [L0 输出](execution_models/transport/L0_first_output/05_output.md)。
- 应用接受与本机冲刷分开记录；交易所业务确认和会话恢复由上层模块处理。

## gateway / L0_journaled_sender — PASS（2026-10-20）

- 按单所有者 API 转录闭包验证、WAL 同步、入队、drive/settle/try_send、首次门禁、中止发布、冲突冻结与恢复。
- 四配置不同状态 7,572 / 57,624 / 11,188 / 8,610，深度 42/60/42/42；11 个不变量通过，每组 41 个 Next 动作全部生成后继。
- root、risk_core、market_core 独立审查通过；零 I/O 预算错误与连接错误分别建模。源码摘要、模型摘要及实际日志见 [L0 输出](execution_models/gateway/L0_journaled_sender/05_output.md)。
- 授权提供者和持久记录按接口契约建模；首次接受与本机冲刷不代替业务 ACK，持久 Aborted 的发布不代替账户资金释放。完整账户和恢复后激活继续由后续模型覆盖。

## account / L0_spot_facts + L1_reduce_shadow — PASS（2026-10-20）

- L0 最终 v3 三配置不同状态 176,596 / 192,984 / 180,131；L1 三配置 136,551 / 24,510 / 99,367。每个配置的全部 21 或 22 个 Next 动作均生成后继。
- 按账户同步 API 转录准入、成交与费用、重复事件后的状态观察、终态累计、未发送中止和异常冻结。L1 增加 USD-M 资金预留、费用额度与减仓一致性。
- root 与 ipc_codec 独立审查模型和源码；market_core 独立复核 ACK/中止、重复成交后的新观察、终态后的迟到成交三项真实 Rust 修复。问题来自源码审查及实际回归，未归因为 TLC 发现。
- 固定版本、有限账户和订单域；无跨币种保证金、在线基线切换或与真实发送协调的组合证明。完整源码锚点、配置及逐项范围见[账户路线](execution_models/account/ROADMAP.md)。
- 撤单/查询允许使用已过期的账户基线，新增显式 freeze 接口；旧模型没有这些动作。本次只同步锚点和范围，六组历史结果仍限于原有固定有效基线实例。

## account_journal / L0_ordered_recovery — PASS（2026-10-20）

- 三配置不同状态 52,700 / 748,060 / 125,574，深度 52/52/66；16 项不变量、Init 与 42 个 Next 动作全部通过并触达。
- 创建依赖顺序、写入/同步/收据分离、poison、未回执完整尾、required 精确前缀、结构与历史闭包校验、账户重放和冻结返回。
- root 与 ipc_codec 独立源码/模型审阅通过；582 个源码行锚点零漂移。初次目录同步反例为模型遗漏，已据源码修正并保留记录，无开放源码候选。
- 有限范围只包含一代原意图的 Admit/Aborted、writer 接管及 Spot 数量 1/价格 1/零费实例；物理字节、分段目录事务和网络发送不在本级证明内。见[输出](execution_models/account_journal/L0_ordered_recovery/05_output.md)。

## account / account_journal 恢复元数据源码同步（2026-10-20）

- 账户将持久 Unknown/终态异常观察与恢复附加 Unknown、本地冻结分开；重复成交的新观察计数不再依赖临时状态。
- Account L0/L1 没有恢复入口与账本序号；AccountJournal L0 只有 Admit/Aborted。三个原模型的动作与不变量不变，仅更新源码行号，2,121 个当前锚点零漂移，未重跑 TLC。
- 原始模型、配置、锁、结果和修复前源码保存在 [同步证据](https://github.com/Obscurepawn/Tickwire/blob/cadf5101706f1e226cabc8dd07fa8ba17c6f6cde/docs/validation/account-recovery-metadata/sync.md)。新增恢复序号性质与真实 Red/Green 由 AccountGateway 阶梯单独跟踪。

## account_gateway / L0_guarded_account_output — PASS

- Scope: 单所有者 Account、AccountJournal、Connection；有序 WAL、候选账户发布、首次输出、零接受中止和冻结恢复。有限 Spot 买入输入，完整 Fill 与订单观察原子应用。
- Finding: retired(fixed, validated by L0_AccountGateway_v2)。恢复 Unknown 和本地 frozen 曾改变重复观察计数；真实普通/MAX/frozen-only Red 与独立审阅确认，修复保留有效 Unknown 但分开持久状态。
- Fix: 账户私有 observed_unknown/recovery_unknown/observation_unreconciled；重复成交不依赖全局 frozen。四项回归通过，最终模型保留全部持久状态+seq 及 SuccessfulPrefixReplays。
- TLC: primary/boundary/two/crashes distinct 为 961560 / 892832 / 2024848 / 1793656，深度均 50，20 项不变量通过；58 个入口在配置合集中均 generated>0，1576 行源码锚点零漂移。
- Evidence: `execution_models/account_gateway/L0_guarded_account_output/03_run_log.md`、`04_counterexamples.md`、`04_evidence/results.json`；旧反例和新性质前一轮保存在各自历史目录。root 与 IPC 独立复核源码和模型。
- Watch: 当前模型不包括实际回报关联、连接重建、恢复激活与新基线切换；这些行为在相应实现完成后单独扩展。原始全仓检查和补强测试的文本版本由 `docs/validation/account-recovery-metadata/workspace-results.json` 分别绑定。
