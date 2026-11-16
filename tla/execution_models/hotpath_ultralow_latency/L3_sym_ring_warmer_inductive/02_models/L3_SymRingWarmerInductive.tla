---- MODULE L3_SymRingWarmerInductive ----
(*
 * Symbolic Inductive & 64-Bit Boundary Verification (Mode S2 + Mode S1) for:
 *   Target 1 (SHM SPMC Ring & Zero-Copy Borrow): Single-writer SPMC broadcast ring
 *     retention watermarks and lifetime-pinned zero-copy `&MarketRecord`
 *     (`RecordBorrow<'a, B>`) over arbitrary `RingCapacity >= 2` and unbounded
 *     64-bit sequence numbers (`1 <= seq <= 2^63`), eliminating `L0`'s finite
 *     `RingCapacity = 2`, `MaxRingSeq = 3` cap via Skolem consumer projection +
 *     relational `ConstInit`.
 *   Target 2 (Idle Hot-Path Warmer): Exact 64-bit `u64` `WarmupTracker` timestamp
 *     arithmetic (`saturating_sub`, `saturating_add`, `now_ns == 0`, backward
 *     clock jitter, `warmup_interval_ns \in 0..2^64-1`), unbounded live order /
 *     TLS AEAD nonce streams vs `TlsLoopbackWarmer` dummy loopback AEAD nonces,
 *     and idle poll-wait scheduling with zero local WAL or disk state.
 *
 * Verified production Rust anchors:
 *   - SPMC ring constants, slot index & sequence limit (`SEQUENCE_LIMIT = 1 << 63`):
 *       crates/tickwire-shm/src/lib.rs:12-20
 *       crates/tickwire-shm/src/lib.rs:134-184
 *   - Producer batch claim & `min_released` watermark gate (`last - min <= capacity`):
 *       crates/tickwire-shm/src/lib.rs:269-336
 *       crates/tickwire-shm/src/mapped.rs:460-556
 *   - Consumer `read` / `released` / `applied` watermarks & `RecordBorrow::drop`:
 *       crates/tickwire-shm/src/lib.rs:437-547
 *       crates/tickwire-shm/src/mapped.rs:734-798
 *       crates/tickwire-shm/src/mapped.rs:825-952
 *       crates/tickwire-strategy-sdk/src/arena.rs:291-386
 *   - `WarmupTracker` 64-bit `u64` timestamps, `next_due_ns`, `can_warm`, `should_warm`:
 *       crates/tickwire-transport/src/warmup.rs:57-187
 *   - `TlsLoopbackWarmer` isolated dummy `rustls` client/server AEAD pair:
 *       crates/tickwire-transport/src/warmup.rs:219-422
 *   - Execution session `dry_run` short-circuit & warmer engines:
 *       crates/tickwire-runtime/src/session.rs:663-690
 *       crates/tickwire-spot-execution/src/warmup.rs:201-425
 *       crates/tickwire-binance-execution/src/warmup.rs:193-468
 *   - ORS `RespWorker` epoll wait loop (`epoll_wait_ms`):
 *       crates/tickwire-runtime/src/ors.rs:2340-2380
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

(* @type: <<Int, Int, Bool, Int, Int, Int, Int, Int, Int, Bool, Int>>; *)
ringVars == <<
  published_seq, cached_min_rel, prod_writing, claim_first, claim_last,
  env_min_rel, cons_read, cons_rel, cons_applied, borrow_active, borrow_seq
>>

(* @type: <<Int, Bool, Int, Int, Int, Int, Int, Bool, Bool, Bool, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int>>; *)
warmerVars == <<
  startup_clock_ns, startup_seeded, now_ns, last_activity_ns, last_warm_ns,
  warm_count, last_warm_elapsed, order_in_flight, node_ready, not_ready_polled,
  step_wait_ns, live_cash_reserved, shadow_cash_reserved,
  live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
  dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
>>

(* @type: <<Int, Int, Bool, Int, Int, Int, Int, Int, Int, Bool, Int, Int, Bool, Int, Int, Int, Int, Int, Bool, Bool, Bool, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int>>; *)
vars == <<
  published_seq, cached_min_rel, prod_writing, claim_first, claim_last,
  env_min_rel, cons_read, cons_rel, cons_applied, borrow_active, borrow_seq,
  startup_clock_ns, startup_seeded, now_ns, last_activity_ns, last_warm_ns,
  warm_count, last_warm_elapsed, order_in_flight, node_ready, not_ready_polled,
  step_wait_ns, live_cash_reserved, shadow_cash_reserved,
  live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
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

(* @type: Set(Int); *)
FiniteBatchSet == 1..2

(* @type: Set(Int); *)
FiniteClockSet == 0..4

(* @type: (Int, Int) => Int; *)
Min2(a, b) == IF a <= b THEN a ELSE b

(* @type: (Int, Int) => Int; *)
Max2(a, b) == IF a >= b THEN a ELSE b

(* @type: (Int, Int) => Int; *)
SaturatingSubU64(a, b) == IF a > b THEN a - b ELSE 0

(* @type: (Int, Int) => Int; *)
SaturatingAddU64(a, b) == IF a + b > EffU64Max THEN EffU64Max ELSE a + b

(* @type: (Int, Int) => Int; *)
AnchorNs(act, warm) == IF act >= warm THEN act ELSE warm

(* @type: (Int, Int, Int, Int) => Bool; *)
CanWarmU64(now, interval, act, warm) ==
  /\ interval > 0
  /\ SaturatingSubU64(now, AnchorNs(act, warm)) >= interval

(* @type: (Int, Int, Int) => Int; *)
NextDueU64(interval, act, warm) ==
  SaturatingAddU64(AnchorNs(act, warm), interval)

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
  /\ live_tls_tx_nonce = 0
  /\ live_tls_peer_nonce = 0
  /\ dummy_c2s_nonce = 0
  /\ dummy_srv_exp_nonce = 0
  /\ dummy_s2c_nonce = 0
  /\ dummy_cli_exp_nonce = 0
  /\ scratch_reserved = 0

(* `Producer::refresh_min` (`crates/tickwire-shm/src/lib.rs:269-279`). *)
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

(* `Producer::claim(count)` (`crates/tickwire-shm/src/lib.rs:280-318`, `crates/tickwire-shm/src/mapped.rs:460-515`). *)
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

(* `Producer::publish` (`crates/tickwire-shm/src/lib.rs:327-332`, `crates/tickwire-shm/src/mapped.rs:536-556`). *)
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

(* `Subscriber::borrow` (`crates/tickwire-shm/src/mapped.rs:737-770`, `crates/tickwire-shm/src/mapped.rs:825-880`). *)
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

(* `RecordBorrow::drop` -> `Subscriber::release` (`crates/tickwire-shm/src/mapped.rs:946-952`, `crates/tickwire-shm/src/lib.rs:503-525`). *)
ConsumerDropBorrow ==
  /\ borrow_active
  /\ cons_rel' = cons_read
  /\ borrow_active' = FALSE
  /\ borrow_seq' = FirstSeq - 1
  /\ UNCHANGED <<
       published_seq, cached_min_rel, prod_writing, claim_first, claim_last,
       env_min_rel, cons_read, cons_applied, warmerVars
     >>

(* `Subscriber::apply` (`crates/tickwire-shm/src/lib.rs:526-546`, `crates/tickwire-shm/src/mapped.rs:794-802`). *)
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
       step_wait_ns, live_cash_reserved, shadow_cash_reserved,
       live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
       dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
     >>

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
       step_wait_ns, live_cash_reserved, shadow_cash_reserved,
       live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
       dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
     >>

RecordMarketActivity ==
  /\ BoundedRingIdleGate
  /\ (~BoundedMode) \/ (~startup_seeded \/ now_ns > last_activity_ns)
  /\ startup_seeded' = TRUE
  /\ last_activity_ns' = Max2(last_activity_ns, now_ns)
  /\ UNCHANGED <<
       ringVars, startup_clock_ns, now_ns, last_warm_ns, warm_count,
       last_warm_elapsed, order_in_flight, node_ready, not_ready_polled,
       step_wait_ns, live_cash_reserved, shadow_cash_reserved,
       live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
       dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
     >>

SubmitLiveOrder ==
  /\ BoundedRingIdleGate
  /\ node_ready
  /\ ~order_in_flight
  /\ live_tls_tx_nonce < EffU64Max
  /\ (~BoundedMode) \/ (live_tls_tx_nonce < 1)
  /\ order_in_flight' = TRUE
  /\ startup_seeded' = TRUE
  /\ last_activity_ns' = Max2(last_activity_ns, now_ns)
  /\ live_cash_reserved' = OrderCost
  /\ shadow_cash_reserved' = OrderCost
  /\ live_tls_tx_nonce' = live_tls_tx_nonce + 1
  /\ live_tls_peer_nonce' = live_tls_peer_nonce + 1
  /\ UNCHANGED <<
       ringVars, startup_clock_ns, now_ns, last_warm_ns, warm_count,
       last_warm_elapsed, node_ready, not_ready_polled, step_wait_ns,
       dummy_c2s_nonce, dummy_srv_exp_nonce, dummy_s2c_nonce,
       dummy_cli_exp_nonce, scratch_reserved
     >>

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
       step_wait_ns, live_cash_reserved, shadow_cash_reserved,
       live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
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
       live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
       dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
     >>

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
       live_cash_reserved, shadow_cash_reserved,
       live_tls_tx_nonce, live_tls_peer_nonce
     >>

SetNodeNotReady ==
  /\ BoundedRingIdleGate
  /\ node_ready
  /\ ~order_in_flight
  /\ node_ready' = FALSE
  /\ not_ready_polled' = FALSE
  /\ UNCHANGED <<
       ringVars, startup_clock_ns, startup_seeded, now_ns, last_activity_ns,
       last_warm_ns, warm_count, last_warm_elapsed, order_in_flight, step_wait_ns,
       live_cash_reserved, shadow_cash_reserved,
       live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
       dummy_srv_exp_nonce, dummy_s2c_nonce, dummy_cli_exp_nonce, scratch_reserved
     >>

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
       live_cash_reserved, shadow_cash_reserved,
       live_tls_tx_nonce, live_tls_peer_nonce, dummy_c2s_nonce,
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
       shadow_cash_reserved, live_tls_tx_nonce, live_tls_peer_nonce,
       dummy_c2s_nonce, dummy_srv_exp_nonce, dummy_s2c_nonce,
       dummy_cli_exp_nonce, scratch_reserved
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
  /\ live_tls_tx_nonce \in Int /\ live_tls_tx_nonce >= 0 /\ live_tls_tx_nonce <= EffU64Max
  /\ live_tls_peer_nonce \in Int /\ live_tls_peer_nonce >= 0 /\ live_tls_peer_nonce <= EffU64Max
  /\ dummy_c2s_nonce \in Int /\ dummy_c2s_nonce >= 0 /\ dummy_c2s_nonce <= EffU64Max
  /\ dummy_srv_exp_nonce \in Int /\ dummy_srv_exp_nonce >= 0 /\ dummy_srv_exp_nonce <= EffU64Max
  /\ dummy_s2c_nonce \in Int /\ dummy_s2c_nonce >= 0 /\ dummy_s2c_nonce <= EffU64Max
  /\ dummy_cli_exp_nonce \in Int /\ dummy_cli_exp_nonce >= 0 /\ dummy_cli_exp_nonce <= EffU64Max
  /\ scratch_reserved \in Int /\ scratch_reserved = 0

ZeroNoAliasWhileBorrowed ==
  (borrow_active /\ prod_writing) =>
    /\ borrow_seq < claim_first
    /\ claim_last - borrow_seq < RingCapacity

WatermarkMonotonicOrder ==
  /\ cons_applied <= cons_rel
  /\ cons_rel <= cons_read
  /\ cons_read <= published_seq

LiveBusinessStateInvariance ==
  /\ live_cash_reserved = shadow_cash_reserved
  /\ scratch_reserved = 0

LiveTlsNonceSynchronization ==
  live_tls_tx_nonce = live_tls_peer_nonce

DummyLoopbackTlsPairSynchronization ==
  /\ dummy_c2s_nonce = dummy_srv_exp_nonce
  /\ dummy_s2c_nonce = dummy_cli_exp_nonce

WarmupStrictlyRespectsTrackerPolicy ==
  (warm_count > 0) =>
    /\ WarmupInterval > 0
    /\ last_warm_elapsed >= WarmupInterval

DisabledWarmupNeverFires ==
  (WarmupInterval = 0) =>
    /\ warm_count = 0
    /\ last_warm_ns = 0
    /\ dummy_c2s_nonce = 0
    /\ dummy_s2c_nonce = 0

SafetyConjunction ==
  /\ TypeOK
  /\ ZeroNoAliasWhileBorrowed
  /\ WatermarkMonotonicOrder
  /\ LiveBusinessStateInvariance
  /\ LiveTlsNonceSynchronization
  /\ DummyLoopbackTlsPairSynchronization
  /\ WarmupStrictlyRespectsTrackerPolicy
  /\ DisabledWarmupNeverFires

CTI_Ring_CachedMinBound ==
  /\ cached_min_rel <= cons_rel
  /\ cached_min_rel <= env_min_rel
  /\ env_min_rel <= published_seq

CTI_Ring_ActiveBorrowPinsRelease ==
  /\ (borrow_active =>
       /\ borrow_seq >= FirstSeq
       /\ borrow_seq <= cons_read
       /\ cons_rel < borrow_seq)
  /\ ((~borrow_active) => borrow_seq = FirstSeq - 1)

CTI_Ring_ActiveClaimWindow ==
  /\ (prod_writing =>
       /\ claim_first = published_seq + 1
       /\ claim_last >= claim_first
       /\ claim_last - cached_min_rel <= RingCapacity)
  /\ ((~prod_writing) =>
       /\ claim_first = FirstSeq - 1
       /\ claim_last = FirstSeq - 1)

CTI_Warmer_ControlCoupling ==
  /\ (IF order_in_flight THEN shadow_cash_reserved = OrderCost ELSE shadow_cash_reserved = 0)
  /\ dummy_c2s_nonce = dummy_s2c_nonce
  /\ ((warm_count = 0) =>
       /\ last_warm_ns = 0
       /\ last_warm_elapsed = WarmupInterval
       /\ dummy_c2s_nonce = 0)

IndInv ==
  /\ TypeOK
  /\ RingCapacity >= 2
  /\ FirstSeq >= 1
  /\ FirstSeq + RingCapacity <= EffSeqLimit
  /\ MaxBatch >= 1
  /\ MaxBatch <= RingCapacity
  /\ WarmupInterval >= 0
  /\ WarmupInterval <= EffU64Max
  /\ ZeroNoAliasWhileBorrowed
  /\ WatermarkMonotonicOrder
  /\ LiveBusinessStateInvariance
  /\ LiveTlsNonceSynchronization
  /\ DummyLoopbackTlsPairSynchronization
  /\ WarmupStrictlyRespectsTrackerPolicy
  /\ DisabledWarmupNeverFires
  /\ CTI_Ring_CachedMinBound
  /\ CTI_Ring_ActiveBorrowPinsRelease
  /\ CTI_Ring_ActiveClaimWindow
  /\ CTI_Warmer_ControlCoupling

Diag_StartupSeededBeforeFirstWarm ==
  (warm_count > 0 /\ ~startup_seeded) =>
    (now_ns - startup_clock_ns >= WarmupInterval)

Diag_NextDueImpliesCanWarmAtU64Max ==
  (WarmupInterval > 0 /\ now_ns >= NextDueU64(WarmupInterval, last_activity_ns, last_warm_ns)) =>
    CanWarmU64(now_ns, WarmupInterval, last_activity_ns, last_warm_ns)

Diag_CoreNodeNoZeroWaitSpinWhenFrozen ==
  (~node_ready /\ not_ready_polled /\ WarmupInterval > 0) =>
    (step_wait_ns > 0)

====
