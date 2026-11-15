---- MODULE L3_SymRingWarmerInductive ----
(*
 * Symbolic Inductive & 64-Bit Boundary Verification (Mode S2 + Mode S1) for:
 *   Target 1a (Opt #5): Single-writer SPMC broadcast ring retention watermarks
 *     and lifetime-pinned zero-copy `&MarketRecord` (`RecordBorrow<'a, B>`) over
 *     arbitrary `RingCapacity >= 2` and unbounded 64-bit sequence numbers
 *     (`1 <= seq <= 2^63`), eliminating `L0`'s finite `RingCapacity = 2`,
 *     `MaxRingSeq = 3` cap via Skolem consumer projection + relational `ConstInit`.
 *   Target 2 (Opt #2): Exact 64-bit `u64` `WarmupTracker` timestamp arithmetic
 *     (`saturating_sub`, `saturating_add`, `now_ns == 0`, backward clock jitter,
 *     `warmup_interval_ns \in 0..2^64-1`), unbounded live order / TLS AEAD nonce
 *     streams vs `TlsLoopbackWarmer` dummy loopback AEAD nonces, and `CoreNode`
 *     `next_warmup_due_ns` reactor poll-wait scheduling (`L1` shrunk `MaxNow = 5`,
 *     `WarmupInterval = 2`, `MaxLiveOrders = 2`, `MaxWarmRuns = 2`).
 *
 * Dual-engine execution:
 *   - Explicit TLC (`BoundedMode = TRUE` in `.cfg`): exhaustive finite smoke +
 *     100% action coverage (`./scripts/check-tla.sh hotpath_ultralow_latency`).
 *   - Symbolic Apalache + Z3 (`BoundedMode = FALSE` in `ConstInit`):
 *       1. Mode S2 CTI baseline (`SafetyConjunction` fails induction at Step 1
 *          without watermark/claim coupling lemmas) + strengthened `IndInv`
 *          proving safety for ALL depths `k >= 0` (`run_apalache.sh inductive`).
 *       2. Mode S1 64-bit `u64` boundary BMC (`run_apalache.sh bmc`) + diagnostic
 *          edge-case queries (`Diag_StartupSeededBeforeFirstWarm`,
 *          `Diag_NextDueImpliesCanWarmAtU64Max`,
 *          `Diag_CoreNodeNoZeroWaitSpinWhenFrozen`).
 *
 * Verified production Rust anchors:
 *   - SPMC ring constants, slot index & sequence limit (`SEQUENCE_LIMIT = 1 << 63`):
 *       crates/tickwire-shm/src/lib.rs:12-20
 *       crates/tickwire-shm/src/lib.rs:136-180
 *   - Producer batch claim & `min_released` watermark gate (`last - min <= capacity`):
 *       crates/tickwire-shm/src/lib.rs:280-341
 *       crates/tickwire-shm/src/lib.rs:353-367
 *       crates/tickwire-shm/src/mapped.rs:466-556
 *   - Consumer `read` / `released` / `applied` watermarks & `RecordBorrow::drop`:
 *       crates/tickwire-shm/src/lib.rs:384-399
 *       crates/tickwire-shm/src/lib.rs:467-539
 *       crates/tickwire-shm/src/mapped.rs:743-748
 *       crates/tickwire-shm/src/mapped.rs:825-952
 *       crates/tickwire-ipc/src/zerocopy.rs:94-117
 *   - `WarmupTracker` 64-bit `u64` timestamps, `next_due_ns`, `can_warm`, `should_warm`:
 *       crates/tickwire-transport/src/warmup.rs:79-187
 *   - `TlsLoopbackWarmer` isolated dummy `rustls` client/server AEAD pair:
 *       crates/tickwire-transport/src/warmup.rs:288-357
 *   - Spot & Binance session idle warmer gates & scratch `Account` rollback:
 *       crates/tickwire-spot-execution/src/warmup.rs:235-268
 *       crates/tickwire-spot-execution/src/session.rs:1867-1957
 *       crates/tickwire-binance-execution/src/lib.rs:695-828
 *   - `CoreNode` quiescent-turn `maybe_warm_hot_path` & `next_warmup_due_ns` wait:
 *       crates/tickwire-runtime/src/node.rs:430-453
 *       crates/tickwire-runtime/src/node.rs:828-846
 *       crates/tickwire-runtime/src/node.rs:885-896
 *)
EXTENDS Integers

CONSTANTS
  (* @type: Bool; *)
  BoundedMode,
  (* @type: Int; *)
  RingCapacity,
  (* @type: Int; *)
  FirstSeq,
  (* @type: Int; *)
  MaxBatch,
  (* @type: Int; *)
  WarmupInterval,
  (* @type: Int; *)
  MaxBoundSteps,
  (* @type: Int; *)
  BoundU64Max

VARIABLES
  (* @type: Int; *)
  published_seq,
  (* @type: Int; *)
  cached_min_rel,
  (* @type: Bool; *)
  prod_writing,
  (* @type: Int; *)
  claim_first,
  (* @type: Int; *)
  claim_last,
  (* @type: Int; *)
  env_min_rel,
  (* @type: Int; *)
  cons_read,
  (* @type: Int; *)
  cons_rel,
  (* @type: Int; *)
  cons_applied,
  (* @type: Bool; *)
  borrow_active,
  (* @type: Int; *)
  borrow_seq,
  (* @type: Int; *)
  startup_clock_ns,
  (* @type: Bool; *)
  startup_seeded,
  (* @type: Int; *)
  now_ns,
  (* @type: Int; *)
  last_activity_ns,
  (* @type: Int; *)
  last_warm_ns,
  (* @type: Int; *)
  warm_count,
  (* @type: Int; *)
  last_warm_elapsed,
  (* @type: Bool; *)
  order_in_flight,
  (* @type: Bool; *)
  node_ready,
  (* @type: Bool; *)
  not_ready_polled,
  (* @type: Int; *)
  step_wait_ns,
  (* @type: Int; *)
  live_cash_reserved,
  (* @type: Int; *)
  shadow_cash_reserved,
  (* @type: Int; *)
  live_journal_seq,
  (* @type: Int; *)
  shadow_journal_seq,
  (* @type: Int; *)
  live_tls_tx_nonce,
  (* @type: Int; *)
  live_tls_peer_nonce,
  (* @type: Int; *)
  dummy_c2s_nonce,
  (* @type: Int; *)
  dummy_srv_exp_nonce,
  (* @type: Int; *)
  dummy_s2c_nonce,
  (* @type: Int; *)
  dummy_cli_exp_nonce,
  (* @type: Int; *)
  scratch_reserved

ringVars == <<
  published_seq, cached_min_rel, prod_writing, claim_first, claim_last,
  env_min_rel, cons_read, cons_rel, cons_applied, borrow_active, borrow_seq
>>

warmerVars == <<
  startup_clock_ns, startup_seeded, now_ns, last_activity_ns, last_warm_ns,
  warm_count, last_warm_elapsed, order_in_flight, node_ready, not_ready_polled,
  step_wait_ns, live_cash_reserved, shadow_cash_reserved, live_journal_seq,
  shadow_journal_seq, live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
  dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
>>

vars == <<
  published_seq, cached_min_rel, prod_writing, claim_first, claim_last,
  env_min_rel, cons_read, cons_rel, cons_applied, borrow_active, borrow_seq,
  startup_clock_ns, startup_seeded, now_ns, last_activity_ns, last_warm_ns,
  warm_count, last_warm_elapsed, order_in_flight, node_ready, not_ready_polled,
  step_wait_ns, live_cash_reserved, shadow_cash_reserved, live_journal_seq,
  shadow_journal_seq, live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
  dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
>>

(* 2^30 = 1073741824 fits in SANY 32-bit literal parser; Apalache evaluates
 * 8 * 2^60 = 2^63 (SEQUENCE_LIMIT) and 16 * 2^60 - 1 = 2^64 - 1 (u64::MAX)
 * in exact arbitrary-precision SMT Int without TLC 32-bit overflow. *)
(* @type: Int; *)
Pow30 == 1073741824

(* @type: Int; *)
SymSeqLimit == 8 * Pow30 * Pow30

(* @type: Int; *)
SymU64Max == (16 * Pow30 * Pow30) - 1

(* @type: Int; *)
EffSeqLimit == IF BoundedMode THEN (FirstSeq + MaxBoundSteps) ELSE SymSeqLimit

(* @type: Int; *)
EffU64Max == IF BoundedMode THEN BoundU64Max ELSE SymU64Max

(* @type: Int; *)
OrderCost == 10

(* Small literal finite domains evaluated ONLY when `BoundedMode = TRUE` in TLC. *)
(* @type: Set(Int); *)
FiniteBatchSet == 1..2

(* @type: Set(Int); *)
FiniteClockSet == 0..4

(* @type: (Int, Int) => Int; *)
Min2(a, b) == IF a <= b THEN a ELSE b

(* @type: (Int, Int) => Int; *)
Max2(a, b) == IF a >= b THEN a ELSE b

(* Exact `u64::saturating_sub` (`crates/tickwire-transport/src/warmup.rs:175`). *)
(* @type: (Int, Int) => Int; *)
SaturatingSubU64(a, b) == IF a > b THEN a - b ELSE 0

(* Exact `u64::saturating_add` (`crates/tickwire-transport/src/warmup.rs:146,160`). *)
(* @type: (Int, Int) => Int; *)
SaturatingAddU64(a, b) == IF a + b > EffU64Max THEN EffU64Max ELSE a + b

(* Exact `anchor = max(last_activity_ns, last_warm_ns)` (`crates/tickwire-transport/src/warmup.rs:155-175`). *)
(* @type: (Int, Int) => Int; *)
AnchorNs(act, warm) == IF act >= warm THEN act ELSE warm

(* Exact `WarmupTracker::can_warm` (`crates/tickwire-transport/src/warmup.rs:165-176`). *)
(* @type: (Int, Int, Int, Int) => Bool; *)
CanWarmU64(now, interval, act, warm) ==
  /\ interval > 0
  /\ SaturatingSubU64(now, AnchorNs(act, warm)) >= interval

(* Exact `WarmupTracker::next_due_ns` (`crates/tickwire-transport/src/warmup.rs:151-161`). *)
(* @type: (Int, Int, Int) => Int; *)
NextDueU64(interval, act, warm) ==
  SaturatingAddU64(AnchorNs(act, warm), interval)

(* Relational constant constraints for Apalache (`--cinit=ConstInit`). *)
ConstInit ==
  /\ BoundedMode = FALSE
  /\ RingCapacity \in Int /\ RingCapacity >= 2
  /\ FirstSeq \in Int /\ FirstSeq >= 1 /\ FirstSeq + RingCapacity <= SymSeqLimit
  /\ MaxBatch \in Int /\ MaxBatch >= 1 /\ MaxBatch <= RingCapacity
  /\ WarmupInterval \in Int /\ WarmupInterval >= 0 /\ WarmupInterval <= SymU64Max
  /\ MaxBoundSteps = 2
  /\ BoundU64Max = 4

Init ==
  /\ published_seq = FirstSeq - 1
  /\ cached_min_rel = FirstSeq - 1
  /\ prod_writing = FALSE
  /\ claim_first = FirstSeq - 1
  /\ claim_last = FirstSeq - 1
  /\ env_min_rel = FirstSeq - 1
  /\ cons_read = FirstSeq - 1
  /\ cons_rel = FirstSeq - 1
  /\ cons_applied = FirstSeq - 1
  /\ borrow_active = FALSE
  /\ borrow_seq = FirstSeq - 1
  /\ IF BoundedMode
     THEN /\ startup_clock_ns = 0
          /\ now_ns = 0
     ELSE /\ startup_clock_ns \in Int
          /\ startup_clock_ns >= 0
          /\ startup_clock_ns <= EffU64Max
          /\ now_ns = startup_clock_ns
  /\ startup_seeded = FALSE
  /\ last_activity_ns = 0
  /\ last_warm_ns = 0
  /\ warm_count = 0
  /\ last_warm_elapsed = WarmupInterval
  /\ order_in_flight = FALSE
  /\ node_ready = TRUE
  /\ not_ready_polled = FALSE
  /\ step_wait_ns = 1
  /\ live_cash_reserved = 0
  /\ shadow_cash_reserved = 0
  /\ live_journal_seq = 0
  /\ shadow_journal_seq = 0
  /\ live_tls_tx_nonce = 0
  /\ live_tls_peer_nonce = 0
  /\ dummy_c2s_nonce = 0
  /\ dummy_srv_exp_nonce = 0
  /\ dummy_s2c_nonce = 0
  /\ dummy_cli_exp_nonce = 0
  /\ scratch_reserved = 0

(* ========================================================================= *)
(* Target 1a Actions — SPMC Broadcast Ring & `RecordBorrow` (`c_star`)       *)
(* ========================================================================= *)

(* `Writer::min_released` refresh (`crates/tickwire-shm/src/lib.rs:320-341`). *)
ProducerRefreshMinRel ==
  /\ ~prod_writing
  /\ LET actual_min == Min2(cons_rel, env_min_rel) IN
     /\ cached_min_rel < actual_min
     /\ cached_min_rel' = actual_min
  /\ UNCHANGED <<
       published_seq, prod_writing, claim_first, claim_last,
       env_min_rel, cons_read, cons_rel, cons_applied, borrow_active, borrow_seq,
       warmerVars
     >>

(* `Writer::claim(count)` (`crates/tickwire-shm/src/lib.rs:280-318`, `crates/tickwire-shm/src/mapped.rs:466-515`). *)
ProducerClaimBatch ==
  /\ ~prod_writing
  /\ IF BoundedMode
     THEN \E cnt \in FiniteBatchSet :
            /\ cnt <= MaxBatch
            /\ LET first == published_seq + 1
                   last  == published_seq + cnt
                   refreshed == IF last - cached_min_rel > RingCapacity
                                THEN Min2(cons_rel, env_min_rel)
                                ELSE cached_min_rel
               IN
               /\ last <= EffSeqLimit
               /\ last - refreshed <= RingCapacity
               /\ cached_min_rel' = refreshed
               /\ prod_writing' = TRUE
               /\ claim_first' = first
               /\ claim_last' = last
     ELSE \E cnt \in Int :
            /\ cnt >= 1
            /\ cnt <= MaxBatch
            /\ LET first == published_seq + 1
                   last  == published_seq + cnt
                   refreshed == IF last - cached_min_rel > RingCapacity
                                THEN Min2(cons_rel, env_min_rel)
                                ELSE cached_min_rel
               IN
               /\ last <= EffSeqLimit
               /\ last - refreshed <= RingCapacity
               /\ cached_min_rel' = refreshed
               /\ prod_writing' = TRUE
               /\ claim_first' = first
               /\ claim_last' = last
  /\ UNCHANGED <<
       published_seq, env_min_rel, cons_read, cons_rel, cons_applied,
       borrow_active, borrow_seq, warmerVars
     >>

(* `Claim::publish` (`crates/tickwire-shm/src/lib.rs:353-367`, `crates/tickwire-shm/src/mapped.rs:536-556`). *)
ProducerPublishBatch ==
  /\ prod_writing
  /\ published_seq' = claim_last
  /\ prod_writing' = FALSE
  /\ claim_first' = FirstSeq - 1
  /\ claim_last' = FirstSeq - 1
  /\ UNCHANGED <<
       cached_min_rel, env_min_rel, cons_read, cons_rel, cons_applied,
       borrow_active, borrow_seq, warmerVars
     >>

(* Environment summary for all other active subscribers `c != c_star`. *)
EnvAdvanceMinRel ==
  /\ env_min_rel < published_seq
  /\ IF BoundedMode
     THEN env_min_rel' = published_seq
     ELSE \E nxt \in Int :
            /\ nxt > env_min_rel
            /\ nxt <= published_seq
            /\ env_min_rel' = nxt
  /\ UNCHANGED <<
       published_seq, cached_min_rel, prod_writing, claim_first, claim_last,
       cons_read, cons_rel, cons_applied, borrow_active, borrow_seq, warmerVars
     >>

(* `Subscriber::next_record_ref` (`crates/tickwire-shm/src/mapped.rs:825-880`):
 * acquires zero-copy `RecordBorrow` (`&MarketRecord`, `crates/tickwire-ipc/src/zerocopy.rs:94-117`),
 * advancing `watermarks.read` while keeping `watermarks.released < borrow_seq`. *)
ConsumerAcquireBorrow ==
  /\ ~borrow_active
  /\ cons_read < published_seq
  /\ cons_read' = cons_read + 1
  /\ borrow_active' = TRUE
  /\ borrow_seq' = cons_read + 1
  /\ UNCHANGED <<
       published_seq, cached_min_rel, prod_writing, claim_first, claim_last,
       env_min_rel, cons_rel, cons_applied, warmerVars
     >>

(* `RecordBorrow::drop` -> `Reader::release` (`crates/tickwire-shm/src/mapped.rs:947-951`, `crates/tickwire-shm/src/lib.rs:503-525`). *)
ConsumerDropBorrow ==
  /\ borrow_active
  /\ cons_rel' = cons_read
  /\ borrow_active' = FALSE
  /\ borrow_seq' = FirstSeq - 1
  /\ UNCHANGED <<
       published_seq, cached_min_rel, prod_writing, claim_first, claim_last,
       env_min_rel, cons_read, cons_applied, warmerVars
     >>

(* `Subscriber::apply` (`crates/tickwire-shm/src/lib.rs:530-539`, `crates/tickwire-shm/src/mapped.rs:882-884`). *)
ConsumerApplyPrefix ==
  /\ cons_applied < cons_rel
  /\ IF BoundedMode
     THEN cons_applied' = cons_rel
     ELSE \E app \in Int :
            /\ app > cons_applied
            /\ app <= cons_rel
            /\ cons_applied' = app
  /\ UNCHANGED <<
       published_seq, cached_min_rel, prod_writing, claim_first, claim_last,
       env_min_rel, cons_read, cons_rel, borrow_active, borrow_seq, warmerVars
     >>

(* ========================================================================= *)
(* Target 2 Actions — 64-Bit `WarmupTracker`, Live/Dummy TLS, `CoreNode`     *)
(* ========================================================================= *)

(* In explicit TLC smoke mode only, gate warmer transitions to idle ring states
 * to avoid useless explicit-state Cartesian interleaving while preserving 100%
 * action reachability; in Apalache (`BoundedMode = FALSE`), zero coupling applies. *)
BoundedRingIdleGate ==
  (~BoundedMode) \/ (~prod_writing /\ ~borrow_active /\ published_seq = FirstSeq - 1)

AdvanceClockMonotonic ==
  /\ BoundedRingIdleGate
  /\ now_ns < EffU64Max
  /\ IF BoundedMode
     THEN \E t \in FiniteClockSet :
            /\ t > now_ns
            /\ t <= EffU64Max
            /\ now_ns' = t
     ELSE \E t \in Int :
            /\ t > now_ns
            /\ t <= EffU64Max
            /\ now_ns' = t
  /\ UNCHANGED <<
       ringVars, startup_clock_ns, startup_seeded, last_activity_ns, last_warm_ns,
       warm_count, last_warm_elapsed, order_in_flight, node_ready, not_ready_polled,
       step_wait_ns, live_cash_reserved, shadow_cash_reserved, live_journal_seq,
       shadow_journal_seq, live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
       dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
     >>

(* Non-monotonic caller timestamp jitter (`now_ns' < now_ns`): verifies `saturating_sub`
 * and `if now_ns > self.last_activity_ns` (`crates/tickwire-transport/src/warmup.rs:135-176`). *)
ClockJitterBackward ==
  /\ BoundedRingIdleGate
  /\ now_ns > 0
  /\ IF BoundedMode
     THEN /\ now_ns = EffU64Max
          /\ now_ns' = EffU64Max - 1
     ELSE \E t \in Int :
            /\ t >= 0
            /\ t < now_ns
            /\ now_ns' = t
  /\ UNCHANGED <<
       ringVars, startup_clock_ns, startup_seeded, last_activity_ns, last_warm_ns,
       warm_count, last_warm_elapsed, order_in_flight, node_ready, not_ready_polled,
       step_wait_ns, live_cash_reserved, shadow_cash_reserved, live_journal_seq,
       shadow_journal_seq, live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
       dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
     >>

(* `WarmupTracker::record_activity` (`crates/tickwire-transport/src/warmup.rs:134-138`). *)
RecordMarketActivity ==
  /\ BoundedRingIdleGate
  /\ (~BoundedMode) \/ (~startup_seeded \/ now_ns > last_activity_ns)
  /\ startup_seeded' = TRUE
  /\ last_activity_ns' = Max2(last_activity_ns, now_ns)
  /\ UNCHANGED <<
       ringVars, startup_clock_ns, now_ns, last_warm_ns, warm_count,
       last_warm_elapsed, order_in_flight, node_ready, not_ready_polled,
       step_wait_ns, live_cash_reserved, shadow_cash_reserved, live_journal_seq,
       shadow_journal_seq, live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
       dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
     >>

(* Live order admission + Journal append + live `rustls` outbound record encryption
 * (`crates/tickwire-spot-execution/src/session.rs:913,997`). *)
SubmitLiveOrder ==
  /\ BoundedRingIdleGate
  /\ node_ready
  /\ ~order_in_flight
  /\ live_tls_tx_nonce < EffU64Max
  /\ (~BoundedMode) \/ (live_journal_seq < 1)
  /\ order_in_flight' = TRUE
  /\ startup_seeded' = TRUE
  /\ last_activity_ns' = Max2(last_activity_ns, now_ns)
  /\ live_cash_reserved' = OrderCost
  /\ shadow_cash_reserved' = OrderCost
  /\ live_journal_seq' = live_journal_seq + 1
  /\ shadow_journal_seq' = shadow_journal_seq + 1
  /\ live_tls_tx_nonce' = live_tls_tx_nonce + 1
  /\ live_tls_peer_nonce' = live_tls_peer_nonce + 1
  /\ UNCHANGED <<
       ringVars, startup_clock_ns, now_ns, last_warm_ns, warm_count,
       last_warm_elapsed, node_ready, not_ready_polled, step_wait_ns,
       dummy_c2s_nonce, dummy_srv_exp_nonce, dummy_s2c_nonce,
       dummy_cli_exp_nonce, scratch_reserved
     >>

(* In-flight guard in `maybe_warm_hot_path` (`crates/tickwire-spot-execution/src/session.rs:1938-1944`,
 * `crates/tickwire-binance-execution/src/lib.rs:810-818`): records activity and skips warming. *)
MaybeWarmBlockedByInFlight ==
  /\ BoundedRingIdleGate
  /\ WarmupInterval > 0
  /\ order_in_flight
  /\ (~BoundedMode) \/ (now_ns > last_activity_ns)
  /\ startup_seeded' = TRUE
  /\ last_activity_ns' = Max2(last_activity_ns, now_ns)
  /\ UNCHANGED <<
       ringVars, startup_clock_ns, now_ns, last_warm_ns, warm_count,
       last_warm_elapsed, order_in_flight, node_ready, not_ready_polled,
       step_wait_ns, live_cash_reserved, shadow_cash_reserved, live_journal_seq,
       shadow_journal_seq, live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
       dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
     >>

ReceiveLiveOrderAck ==
  /\ BoundedRingIdleGate
  /\ order_in_flight
  /\ order_in_flight' = FALSE
  /\ startup_seeded' = TRUE
  /\ last_activity_ns' = Max2(last_activity_ns, now_ns)
  /\ live_cash_reserved' = 0
  /\ shadow_cash_reserved' = 0
  /\ UNCHANGED <<
       ringVars, startup_clock_ns, now_ns, last_warm_ns, warm_count,
       last_warm_elapsed, node_ready, not_ready_polled, step_wait_ns,
       live_journal_seq, shadow_journal_seq, live_tls_tx_nonce,
       live_tls_peer_nonce, dummy_c2s_nonce, dummy_srv_exp_nonce,
       dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
     >>

(* `maybe_warm_hot_path` (`crates/tickwire-spot-execution/src/session.rs:1931-1957`,
 * `crates/tickwire-binance-execution/src/lib.rs:801-837`, `crates/tickwire-transport/src/warmup.rs:288-357`):
 * exercises scratch `Account` + loopback `TlsLoopbackWarmer`
 * without mutating live `Account`, `AccountJournal`, or live `rustls` AEAD nonces. *)
MaybeWarmHotPath ==
  /\ BoundedRingIdleGate
  /\ node_ready
  /\ ~order_in_flight
  /\ CanWarmU64(now_ns, WarmupInterval, last_activity_ns, last_warm_ns)
  /\ dummy_c2s_nonce < EffU64Max
  /\ dummy_s2c_nonce < EffU64Max
  /\ (~BoundedMode) \/ (warm_count < 1)
  /\ last_warm_elapsed' = SaturatingSubU64(now_ns, AnchorNs(last_activity_ns, last_warm_ns))
  /\ last_warm_ns' = Max2(last_warm_ns, now_ns)
  /\ warm_count' = SaturatingAddU64(warm_count, 1)
  /\ dummy_c2s_nonce' = dummy_c2s_nonce + 1
  /\ dummy_srv_exp_nonce' = dummy_srv_exp_nonce + 1
  /\ dummy_s2c_nonce' = dummy_s2c_nonce + 1
  /\ dummy_cli_exp_nonce' = dummy_cli_exp_nonce + 1
  /\ scratch_reserved' = 0
  /\ UNCHANGED <<
       ringVars, startup_clock_ns, startup_seeded, now_ns, last_activity_ns,
       order_in_flight, node_ready, not_ready_polled, step_wait_ns,
       live_cash_reserved, shadow_cash_reserved, live_journal_seq,
       shadow_journal_seq, live_tls_tx_nonce, live_tls_peer_nonce
     >>

(* Account freeze / gateway halt / recovery not-ready transition (`crates/tickwire-runtime/src/node.rs:435-445`). *)
SetNodeNotReady ==
  /\ BoundedRingIdleGate
  /\ node_ready
  /\ ~order_in_flight
  /\ node_ready' = FALSE
  /\ not_ready_polled' = FALSE
  /\ UNCHANGED <<
       ringVars, startup_clock_ns, startup_seeded, now_ns, last_activity_ns,
       last_warm_ns, warm_count, last_warm_elapsed, order_in_flight, step_wait_ns,
       live_cash_reserved, shadow_cash_reserved, live_journal_seq,
       shadow_journal_seq, live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
       dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
     >>

(* Code-faithful `CoreNode::step_inner` + `run_with` quiescent poll while `!node_ready`
 * (`crates/tickwire-runtime/src/node.rs:438-452` & `833-835`):
 * `step_inner` skips `maybe_warm_hot_path_with_account` WITHOUT calling
 * `record_activity(now_ns)`, then clamps `next_deadline` to `next_warmup_due_ns()`. *)
CoreNodeQuiescentStepNotReady ==
  /\ BoundedRingIdleGate
  /\ ~node_ready
  /\ (~BoundedMode) \/ (~not_ready_polled)
  /\ not_ready_polled' = TRUE
  /\ step_wait_ns' =
       (IF WarmupInterval > 0
        THEN SaturatingSubU64(NextDueU64(WarmupInterval, last_activity_ns, last_warm_ns), now_ns)
        ELSE 1)
  /\ UNCHANGED <<
       ringVars, startup_clock_ns, startup_seeded, now_ns, last_activity_ns,
       last_warm_ns, warm_count, last_warm_elapsed, order_in_flight, node_ready,
       live_cash_reserved, shadow_cash_reserved, live_journal_seq,
       shadow_journal_seq, live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
       dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
     >>

RestoreNodeReady ==
  /\ BoundedRingIdleGate
  /\ ~node_ready
  /\ node_ready' = TRUE
  /\ not_ready_polled' = FALSE
  /\ step_wait_ns' = 1
  /\ startup_seeded' = TRUE
  /\ last_activity_ns' = Max2(last_activity_ns, now_ns)
  /\ UNCHANGED <<
       ringVars, startup_clock_ns, now_ns, last_warm_ns, warm_count,
       last_warm_elapsed, order_in_flight, live_cash_reserved,
       shadow_cash_reserved, live_journal_seq, shadow_journal_seq,
       live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
       dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
     >>

Next ==
  \/ ProducerRefreshMinRel
  \/ ProducerClaimBatch
  \/ ProducerPublishBatch
  \/ EnvAdvanceMinRel
  \/ ConsumerAcquireBorrow
  \/ ConsumerDropBorrow
  \/ ConsumerApplyPrefix
  \/ AdvanceClockMonotonic
  \/ ClockJitterBackward
  \/ RecordMarketActivity
  \/ SubmitLiveOrder
  \/ MaybeWarmBlockedByInFlight
  \/ ReceiveLiveOrderAck
  \/ MaybeWarmHotPath
  \/ SetNodeNotReady
  \/ CoreNodeQuiescentStepNotReady
  \/ RestoreNodeReady

Spec == Init /\ [][Next]_vars

(* ========================================================================= *)
(* Safety Invariants (Verified by both TLC and Apalache + Z3)                *)
(* ========================================================================= *)

TypeOK ==
  /\ published_seq \in Int /\ published_seq >= FirstSeq - 1 /\ published_seq <= EffSeqLimit
  /\ cached_min_rel \in Int /\ cached_min_rel >= FirstSeq - 1 /\ cached_min_rel <= EffSeqLimit
  /\ prod_writing \in BOOLEAN
  /\ claim_first \in Int /\ claim_first >= FirstSeq - 1 /\ claim_first <= EffSeqLimit
  /\ claim_last \in Int /\ claim_last >= FirstSeq - 1 /\ claim_last <= EffSeqLimit
  /\ env_min_rel \in Int /\ env_min_rel >= FirstSeq - 1 /\ env_min_rel <= EffSeqLimit
  /\ cons_read \in Int /\ cons_read >= FirstSeq - 1 /\ cons_read <= EffSeqLimit
  /\ cons_rel \in Int /\ cons_rel >= FirstSeq - 1 /\ cons_rel <= EffSeqLimit
  /\ cons_applied \in Int /\ cons_applied >= FirstSeq - 1 /\ cons_applied <= EffSeqLimit
  /\ borrow_active \in BOOLEAN
  /\ borrow_seq \in Int /\ borrow_seq >= FirstSeq - 1 /\ borrow_seq <= EffSeqLimit
  /\ startup_clock_ns \in Int /\ startup_clock_ns >= 0 /\ startup_clock_ns <= EffU64Max
  /\ startup_seeded \in BOOLEAN
  /\ now_ns \in Int /\ now_ns >= 0 /\ now_ns <= EffU64Max
  /\ last_activity_ns \in Int /\ last_activity_ns >= 0 /\ last_activity_ns <= EffU64Max
  /\ last_warm_ns \in Int /\ last_warm_ns >= 0 /\ last_warm_ns <= EffU64Max
  /\ warm_count \in Int /\ warm_count >= 0 /\ warm_count <= EffU64Max
  /\ last_warm_elapsed \in Int /\ last_warm_elapsed >= 0 /\ last_warm_elapsed <= EffU64Max
  /\ order_in_flight \in BOOLEAN
  /\ node_ready \in BOOLEAN
  /\ not_ready_polled \in BOOLEAN
  /\ step_wait_ns \in Int /\ step_wait_ns >= 0 /\ step_wait_ns <= EffU64Max
  /\ live_cash_reserved \in Int /\ live_cash_reserved >= 0
  /\ shadow_cash_reserved \in Int /\ shadow_cash_reserved >= 0
  /\ live_journal_seq \in Int /\ live_journal_seq >= 0
  /\ shadow_journal_seq \in Int /\ shadow_journal_seq >= 0
  /\ live_tls_tx_nonce \in Int /\ live_tls_tx_nonce >= 0 /\ live_tls_tx_nonce <= EffU64Max
  /\ live_tls_peer_nonce \in Int /\ live_tls_peer_nonce >= 0 /\ live_tls_peer_nonce <= EffU64Max
  /\ dummy_c2s_nonce \in Int /\ dummy_c2s_nonce >= 0 /\ dummy_c2s_nonce <= EffU64Max
  /\ dummy_srv_exp_nonce \in Int /\ dummy_srv_exp_nonce >= 0 /\ dummy_srv_exp_nonce <= EffU64Max
  /\ dummy_s2c_nonce \in Int /\ dummy_s2c_nonce >= 0 /\ dummy_s2c_nonce <= EffU64Max
  /\ dummy_cli_exp_nonce \in Int /\ dummy_cli_exp_nonce >= 0 /\ dummy_cli_exp_nonce <= EffU64Max
  /\ scratch_reserved \in Int /\ scratch_reserved = 0

(* 1. Zero-copy `&MarketRecord` slot disjointness across ALL `RingCapacity >= 2` and
 *    ALL unbounded 64-bit sequences: while `borrow_active` holds `borrow_seq` and
 *    `prod_writing` writes `[claim_first, claim_last]`, every `w_seq` in the claimed
 *    batch satisfies `0 < w_seq - borrow_seq < RingCapacity`, which mathematically
 *    forbids `(w_seq - FirstSeq) % RingCapacity == (borrow_seq - FirstSeq) % RingCapacity`. *)
ZeroNoAliasWhileBorrowed ==
  (borrow_active /\ prod_writing) =>
    /\ borrow_seq < claim_first
    /\ claim_last - borrow_seq < RingCapacity

(* 2. Consumer & producer watermark ordering (`crates/tickwire-shm/src/lib.rs:384-399`). *)
WatermarkMonotonicOrder ==
  /\ cons_applied <= cons_rel
  /\ cons_rel <= cons_read
  /\ cons_read <= published_seq

(* 3. Live account & WAL invariance under arbitrary warmer executions (`Opt #2`). *)
LiveBusinessStateInvariance ==
  /\ live_cash_reserved = shadow_cash_reserved
  /\ live_journal_seq = shadow_journal_seq
  /\ scratch_reserved = 0

(* 4. Live `rustls` AEAD record sequence synchronization (`Opt #2`). *)
LiveTlsNonceSynchronization ==
  live_tls_tx_nonce = live_tls_peer_nonce

(* 5. Dummy `TlsLoopbackWarmer` bidirectional AEAD sequence synchronization (`Opt #2`). *)
DummyLoopbackTlsPairSynchronization ==
  /\ dummy_c2s_nonce = dummy_srv_exp_nonce
  /\ dummy_s2c_nonce = dummy_cli_exp_nonce

(* 6. Every executed warm step satisfied `WarmupTracker::can_warm` (`elapsed >= interval`). *)
WarmupStrictlyRespectsTrackerPolicy ==
  (warm_count > 0) =>
    /\ WarmupInterval > 0
    /\ last_warm_elapsed >= WarmupInterval

(* 7. Zero-overhead disabled mode (`warmup_interval_ns == 0`). *)
DisabledWarmupNeverFires ==
  (WarmupInterval = 0) =>
    /\ warm_count = 0
    /\ last_warm_ns = 0
    /\ dummy_c2s_nonce = 0
    /\ dummy_s2c_nonce = 0

(* Raw safety target before inductive CTI strengthening (fails 1-step induction
 * from unreachable predecessor states where `cached_min_rel > cons_rel` or
 * `borrow_active` does not pin `cons_rel < borrow_seq`). *)
SafetyConjunction ==
  /\ TypeOK
  /\ ZeroNoAliasWhileBorrowed
  /\ WatermarkMonotonicOrder
  /\ LiveBusinessStateInvariance
  /\ LiveTlsNonceSynchronization
  /\ DummyLoopbackTlsPairSynchronization
  /\ WarmupStrictlyRespectsTrackerPolicy
  /\ DisabledWarmupNeverFires

(* ========================================================================= *)
(* Mode S2 Strengthened Symbolic Inductive Invariant (`IndInv`)              *)
(* ========================================================================= *)

(* CTI Lemma 1 (`crates/tickwire-shm/src/lib.rs:212,294-305,320-341`):
 * `Writer.min_released` never exceeds any active subscriber's `released` watermark. *)
CTI_Ring_CachedMinBound ==
  /\ cached_min_rel <= cons_rel
  /\ cached_min_rel <= env_min_rel
  /\ env_min_rel <= published_seq

(* CTI Lemma 2 (`crates/tickwire-shm/src/mapped.rs:825-952`):
 * An active `RecordBorrow` withholds `Reader::release`, pinning `cons_rel < borrow_seq <= cons_read`. *)
CTI_Ring_ActiveBorrowPinsRelease ==
  /\ (borrow_active =>
       /\ borrow_seq >= FirstSeq
       /\ borrow_seq <= cons_read
       /\ cons_rel < borrow_seq)
  /\ ((~borrow_active) => borrow_seq = FirstSeq - 1)

(* CTI Lemma 3 (`crates/tickwire-shm/src/lib.rs:288-318`):
 * Every active `Claim` satisfies `claim_first = published_seq + 1` and
 * `claim_last - cached_min_rel <= RingCapacity` until `Claim::publish`. *)
CTI_Ring_ActiveClaimWindow ==
  /\ (prod_writing =>
       /\ claim_first = published_seq + 1
       /\ claim_last >= claim_first
       /\ claim_last - cached_min_rel <= RingCapacity)
  /\ ((~prod_writing) =>
       /\ claim_first = FirstSeq - 1
       /\ claim_last = FirstSeq - 1)

(* CTI Lemma 4 (`crates/tickwire-spot-execution/src/session.rs:913,997,1938-1957`):
 * Couples `order_in_flight` to `shadow_cash_reserved` and `warm_count = 0` to
 * `last_warm_elapsed = WarmupInterval` & `dummy_c2s_nonce = dummy_s2c_nonce`. *)
CTI_Warmer_ControlCoupling ==
  /\ (IF order_in_flight THEN shadow_cash_reserved = OrderCost ELSE shadow_cash_reserved = 0)
  /\ dummy_c2s_nonce = dummy_s2c_nonce
  /\ ((warm_count = 0) =>
       /\ last_warm_ns = 0
       /\ last_warm_elapsed = WarmupInterval
       /\ dummy_c2s_nonce = 0)

IndInv ==
  /\ RingCapacity >= 2
  /\ FirstSeq >= 1
  /\ FirstSeq + RingCapacity <= EffSeqLimit
  /\ MaxBatch >= 1
  /\ MaxBatch <= RingCapacity
  /\ WarmupInterval >= 0
  /\ WarmupInterval <= EffU64Max
  /\ SafetyConjunction
  /\ CTI_Ring_CachedMinBound
  /\ CTI_Ring_ActiveBorrowPinsRelease
  /\ CTI_Ring_ActiveClaimWindow
  /\ CTI_Warmer_ControlCoupling

(* ========================================================================= *)
(* Mode S1 Diagnostic Edge-Case Queries (Probed via `run_apalache.sh bmc`)   *)
(* ========================================================================= *)

(* Diagnostic 1 (`crates/tickwire-transport/src/warmup.rs:88-95,165-176`):
 * Tests whether a cold-started session without an initial `record_activity(startup_clock_ns)`
 * always waits at least `WarmupInterval` ns after `startup_clock_ns` before firing `warm_once`.
 * Z3 finds a 1-step counterexample when `startup_clock_ns >= WarmupInterval > 0` because
 * `last_activity_ns` initializes to `0` (`now_ns - 0 >= WarmupInterval` at `elapsed = 0`). *)
Diag_StartupSeededBeforeFirstWarm ==
  (warm_count > 0 /\ ~startup_seeded) =>
    (now_ns - startup_clock_ns >= WarmupInterval)

(* Diagnostic 2 (`crates/tickwire-transport/src/warmup.rs:151-176`):
 * Tests whether reaching `now_ns >= next_due_ns()` implies `can_warm(now_ns) == TRUE`
 * across the entire 64-bit `u64` domain. Z3 finds an exact boundary mismatch at
 * `now_ns = u64::MAX` whenever `AnchorNs + WarmupInterval > u64::MAX` (`next_due_ns`
 * saturates to `Some(u64::MAX)` while `u64::MAX - anchor < WarmupInterval` is `FALSE`). *)
Diag_NextDueImpliesCanWarmAtU64Max ==
  (WarmupInterval > 0 /\ now_ns >= NextDueU64(WarmupInterval, last_activity_ns, last_warm_ns)) =>
    CanWarmU64(now_ns, WarmupInterval, last_activity_ns, last_warm_ns)

(* Diagnostic 3 (`crates/tickwire-runtime/src/node.rs:438-452,833-846`):
 * Tests whether `CoreNode::run_with` avoids a 0ns `mio::Poll` non-blocking busy-spin
 * (`step_wait_ns == 0`) on quiescent turns while `!node_ready` (`frozen || halted || !submission_ready`).
 * Z3 finds a concrete counterexample where `step_inner` omits `record_activity(now_ns)` when
 * `!node_ready` and clamps `next_deadline` to a past `next_warmup_due_ns() <= now_ns`. *)
Diag_CoreNodeNoZeroWaitSpinWhenFrozen ==
  (~node_ready /\ not_ready_polled /\ WarmupInterval > 0) =>
    (step_wait_ns > 0)

====
