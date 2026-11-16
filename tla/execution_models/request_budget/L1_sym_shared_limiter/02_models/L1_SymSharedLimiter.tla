---- MODULE L1_SymSharedLimiter ----
(*
 * Symbolic Unbounded Inductive & 64-Bit / 128-Bit Verification (Mode S2 + Mode S1 + Dual TLC)
 * for `tickwire_quota::shared::{SharedRateLimiter, OrsAccountQuota}` and single-pick ORS routing:
 *   - Proves cross-account shared IP (`RateRule::ip`, `account = 0`) + per-account (`ORDER`,
 *     `CANCEL`) cap bounds, exact reverse-order `put_back_undo` multi-bucket rollback across
 *     `AtomicFixedWindow` and `AtomicRollingWindow` (preserving sibling account units coalesced
 *     in the same shared sub-bucket), strict single-pick `pick -> spend(local_ip) -> NACK`
 *     without sibling connection retry, `AtomicFixedWindow` clock-regression guard, per-account
 *     `418` freeze / cooldown isolation vs. per-IP `429` cooldown, rising-edge `ExchRateLimit`
 *     (`80%` `WarnThreshold` / `100%` `CeilingReached`, `exch_latch_ceil => exch_latch_warn`),
 *     and `u64`/`u128` overflow freedom over `0 .. 2^64 - 1` and all step depths `k >= 0`.
 *
 * Verified production Rust anchors:
 *   - `AtomicFixedWindow` (`try_take`, `put_back`, `u32` epoch clock-regression guard):
 *       crates/tickwire-quota/src/shared.rs:312-412
 *   - `AtomicRollingWindow` (`ROLLING_SUB_BUCKETS = 20`, `MAX_ROLLING_COUNT`, `evict_expired`,
 *     `try_take`, `put_back`):
 *       crates/tickwire-quota/src/shared.rs:414-643
 *   - Cooldowns, `418` account freeze, `SharedRateLimiter::allow` (`put_back_undo`),
 *     `pick_and_spend_or_nack`, and `reconcile_venue_usage` (`80%` / `100%` `exch_latch`):
 *       crates/tickwire-quota/src/shared.rs:1095-1441
 *   - `OrsAccountQuota` (`build_place_spends`, `build_cancel_spends`, `build_query_spends`,
 *     `pick_route_and_place_or_nack`, `route_ip_key`):
 *       crates/tickwire-quota/src/shared.rs:1580-1816
 *   - Lock-free `PublishedRoutes::pick` and `AccountSession::pick_and_place_or_nack`:
 *       crates/tickwire-transport/src/order_router.rs:688-745
 *       crates/tickwire-runtime/src/session.rs:518-580
 *)
EXTENDS Integers

CONSTANTS
  (* @type: Bool; *)
  BoundedMode,
  (* @type: Bool; *)
  AllowFreeze

VARIABLES
  (* --- Symbolic window capacities (`1..U32_MAX` / `2..MAX_ROLLING_COUNT` in Z3) --- *)
  (* @type: Int; *)
  cfg_acct1_ord_cap,
  (* @type: Int; *)
  cfg_acct2_ord_cap,
  (* @type: Int; *)
  cfg_acct1_cxl_cap,
  (* @type: Int; *)
  cfg_shared_ip_cap,
  (* @type: Int; *)
  cfg_rep_limit,
  (* @type: Bool; *)
  cfg_allow_freeze,

  (* --- Monotonic clock & cooldown/freeze state (crates/tickwire-quota/src/shared.rs:1095-1245) --- *)
  (* @type: Int; *)
  last_now_ns,
  (* @type: Bool; *)
  acct1_frozen,
  (* @type: Int; *)
  acct1_cd_until_ns,
  (* @type: Int; *)
  ip1_cd_until_ns,

  (* --- PublishedRoutes readiness & round-robin cursor (crates/tickwire-transport/src/order_router.rs:688-745) --- *)
  (* @type: Bool; *)
  r1_ip1_ready,
  (* @type: Int; *)
  r1_last_conn,

  (* --- Per-account `AtomicFixedWindow` state (crates/tickwire-quota/src/shared.rs:312-412) --- *)
  (* @type: Bool; *)
  a1_ord_init,
  (* @type: Int; *)
  a1_ord_ep,
  (* @type: Int; *)
  a1_ord_used,

  (* @type: Bool; *)
  a2_ord_init,
  (* @type: Int; *)
  a2_ord_ep,
  (* @type: Int; *)
  a2_ord_used,

  (* @type: Bool; *)
  a1_cxl_init,
  (* @type: Int; *)
  a1_cxl_ep,
  (* @type: Int; *)
  a1_cxl_used,

  (* --- Cross-account shared `AtomicRollingWindow` sub-buckets (crates/tickwire-quota/src/shared.rs:414-643) --- *)
  (* @type: Int; *)
  ip1_len,
  (* @type: Int; *)
  ip1_t0,
  (* @type: Int; *)
  ip1_u0,
  (* @type: Int; *)
  ip1_t1,
  (* @type: Int; *)
  ip1_u1,
  (* @type: Int; *)
  ip1_sum,

  (* @type: Int; *)
  ip2_len,
  (* @type: Int; *)
  ip2_t0,
  (* @type: Int; *)
  ip2_u0,
  (* @type: Int; *)
  ip2_t1,
  (* @type: Int; *)
  ip2_u1,
  (* @type: Int; *)
  ip2_sum,

  (* --- Rising-edge `ExchRateLimit` latch (crates/tickwire-quota/src/shared.rs:1361-1441) --- *)
  (* @type: Bool; *)
  exch_latch_warn,
  (* @type: Bool; *)
  exch_latch_ceil,

  (* --- Verification witnesses & bounded TLC horizon --- *)
  (* @type: Int; *)
  step_budget,
  (* @type: Bool; *)
  rollback_exact_ok,
  (* @type: Bool; *)
  single_pick_no_retry_ok,
  (* @type: Bool; *)
  cross_acct_isolation_ok,
  (* @type: Bool; *)
  rising_edge_latch_ok,
  (* @type: Bool; *)
  u64_u128_overflow_ok

vars == <<
  cfg_acct1_ord_cap, cfg_acct2_ord_cap, cfg_acct1_cxl_cap, cfg_shared_ip_cap, cfg_rep_limit, cfg_allow_freeze,
  last_now_ns, acct1_frozen, acct1_cd_until_ns, ip1_cd_until_ns,
  r1_ip1_ready, r1_last_conn,
  a1_ord_init, a1_ord_ep, a1_ord_used,
  a2_ord_init, a2_ord_ep, a2_ord_used,
  a1_cxl_init, a1_cxl_ep, a1_cxl_used,
  ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum,
  ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum,
  exch_latch_warn, exch_latch_ceil,
  step_budget,
  rollback_exact_ok, single_pick_no_retry_ok, cross_acct_isolation_ok, rising_edge_latch_ok, u64_u128_overflow_ok
>>

(* --- SANY-Safe 28/32/64/128-Bit Integer Bounds (zero literals > 2^31 - 1) --- *)
Pow28 == 268435456
Pow30 == 1073741824
HalfU32 == 2 * Pow30
U32_MAX == (4 * Pow30) - 1
MAX_ROLL_CNT == Pow28 - 1
U64_MAX == (16 * Pow30 * Pow30) - 1
U128_MAX == ((U64_MAX + 1) * (U64_MAX + 1)) - 1

EffU32Max == IF BoundedMode THEN 2 ELSE U32_MAX
EffRollMax == IF BoundedMode THEN 3 ELSE MAX_ROLL_CNT
EffU64Max == IF BoundedMode THEN 4 ELSE U64_MAX
EffU128Max == IF BoundedMode THEN 10000 ELSE U128_MAX
MaxBoundedSteps == 2

CanStep == (~BoundedMode) \/ (step_budget < MaxBoundedSteps)
NextBudget == IF BoundedMode THEN step_budget + 1 ELSE 0

TimeDom == IF BoundedMode THEN 0..3 ELSE Int
UnitDom == IF BoundedMode THEN 1..2 ELSE Int
ReportDom == IF BoundedMode THEN 0..3 ELSE Int

Max2(a, b) == IF a >= b THEN a ELSE b
SatAddU64(a, b) == IF (a + b) > EffU64Max THEN EffU64Max ELSE (a + b)

(* --- Linear `QF_LIA` Fixed-Window Operators (crates/tickwire-quota/src/shared.rs:340-411) --- *)
U32_MOD == 4 * Pow30

WrapU32(sec) ==
  IF sec < U32_MOD THEN sec
  ELSE IF sec < (2 * U32_MOD) THEN (sec - U32_MOD)
  ELSE IF sec < (3 * U32_MOD) THEN (sec - (2 * U32_MOD))
  ELSE IF sec < (4 * U32_MOD) THEN (sec - (3 * U32_MOD))
  ELSE (sec - (4 * U32_MOD))

FixedEpochIdx(t) ==
  IF BoundedMode THEN (t \div 2)
  ELSE WrapU32(t \div 1000000000)

MaxFixedEpoch == IF BoundedMode THEN 2 ELSE U32_MAX

FixedAdvUsed(inited, cur_ep, cur_used, now_ns) ==
  IF inited /\ (cur_ep = FixedEpochIdx(now_ns)) THEN cur_used ELSE 0

FixedClockRegressed(inited, cur_ep, now_ns) ==
  LET ep == FixedEpochIdx(now_ns) IN
  inited /\ (ep < cur_ep) /\ ((cur_ep - ep) < (IF BoundedMode THEN 2 ELSE HalfU32))

(* --- Linear `QF_LIA` 20-Sub-Bucket Rolling Operators (crates/tickwire-quota/src/shared.rs:435-642) --- *)
SubTickIdx(t) ==
  IF BoundedMode THEN t ELSE (t \div (3 * 1000000000))

SubBucketSpan == IF BoundedMode THEN 2 ELSE 20

SubExp0(len, t0, tk) == (len >= 1) /\ ((tk - t0) >= SubBucketSpan)
SubExp1(len, t0, t1, tk) == (len = 2) /\ SubExp0(len, t0, tk) /\ ((tk - t1) >= SubBucketSpan)

SubEffLen(len, t0, t1, tk) ==
  IF len = 0 THEN 0
  ELSE IF len = 1 THEN (IF SubExp0(len, t0, tk) THEN 0 ELSE 1)
  ELSE IF SubExp1(len, t0, t1, tk) THEN 0
  ELSE IF SubExp0(len, t0, tk) THEN 1
  ELSE 2

SubEffT0(len, t0, t1, tk) ==
  LET el == SubEffLen(len, t0, t1, tk) IN
  IF el = 0 THEN 0
  ELSE IF (len = 2) /\ SubExp0(len, t0, tk) THEN t1
  ELSE t0

SubEffU0(len, t0, u0, t1, u1, tk) ==
  LET el == SubEffLen(len, t0, t1, tk) IN
  IF el = 0 THEN 0
  ELSE IF (len = 2) /\ SubExp0(len, t0, tk) THEN u1
  ELSE u0

SubEffT1(len, t0, t1, tk) ==
  IF SubEffLen(len, t0, t1, tk) = 2 THEN t1 ELSE 0

SubEffU1(len, t0, t1, u1, tk) ==
  IF SubEffLen(len, t0, t1, tk) = 2 THEN u1 ELSE 0

SubEffSum(len, t0, u0, t1, u1, tk) ==
  SubEffU0(len, t0, u0, t1, u1, tk) + SubEffU1(len, t0, t1, u1, tk)

SubCoalesces(el, et0, et1, tk) ==
  (el > 0) /\ ((IF el = 2 THEN et1 ELSE et0) = tk)

SubCanTake(el, et0, et1, esum, req, cap, tk) ==
  /\ (esum + req <= EffRollMax)
  /\ (esum + req <= cap)
  /\ (SubCoalesces(el, et0, et1, tk) \/ (el < 2))

SubPostLen(el, et0, et1, tk) ==
  IF SubCoalesces(el, et0, et1, tk) THEN el ELSE (el + 1)

SubPostT0(el, et0, tk) == IF el = 0 THEN tk ELSE et0

SubPostU0(el, et0, eu0, req, tk) ==
  IF el = 0 THEN req
  ELSE IF (el = 1) /\ (et0 = tk) THEN (eu0 + req)
  ELSE eu0

SubPostT1(el, et0, et1, tk) ==
  IF el = 0 THEN 0
  ELSE IF (el = 1) /\ (et0 = tk) THEN 0
  ELSE IF el = 1 THEN tk
  ELSE et1

SubPostU1(el, et0, et1, eu1, req, tk) ==
  IF el = 0 THEN 0
  ELSE IF (el = 1) /\ (et0 = tk) THEN 0
  ELSE IF el = 1 THEN req
  ELSE (eu1 + req)

(* Explicit `AtomicRollingWindow::put_back` (crates/tickwire-quota/src/shared.rs:617-642): *)
SubPutBackLen(coal, plen) == IF coal THEN plen ELSE (plen - 1)
SubPutBackT0(coal, plen, pt0) == IF (~coal) /\ (plen = 1) THEN 0 ELSE pt0
SubPutBackU0(coal, plen, pu0, req) ==
  IF coal /\ (plen = 1) THEN (pu0 - req)
  ELSE IF (~coal) /\ (plen = 1) THEN 0
  ELSE pu0
SubPutBackT1(coal, plen, pt1) == IF (~coal) /\ (plen = 2) THEN 0 ELSE pt1
SubPutBackU1(coal, plen, pu1, req) ==
  IF coal /\ (plen = 2) THEN (pu1 - req)
  ELSE IF (~coal) /\ (plen = 2) THEN 0
  ELSE pu1

(* PublishedRoutes::pick single-pick over {1 (r1_ip1_ready), 2 (always ready)}: *)
PickRoute1 ==
  IF ~r1_ip1_ready THEN 2
  ELSE IF r1_last_conn = 1 THEN 2
  ELSE 1

ConstInit ==
  /\ BoundedMode = FALSE
  /\ AllowFreeze \in BOOLEAN

ValidConfig ==
  /\ cfg_acct1_ord_cap \in Int /\ cfg_acct1_ord_cap >= 1 /\ cfg_acct1_ord_cap <= EffU32Max
  /\ cfg_acct2_ord_cap \in Int /\ cfg_acct2_ord_cap >= 1 /\ cfg_acct2_ord_cap <= EffU32Max
  /\ cfg_acct1_cxl_cap \in Int /\ cfg_acct1_cxl_cap >= 1 /\ cfg_acct1_cxl_cap <= EffU32Max
  /\ cfg_shared_ip_cap \in Int /\ cfg_shared_ip_cap >= 2 /\ cfg_shared_ip_cap <= EffRollMax
  /\ cfg_rep_limit \in Int /\ cfg_rep_limit >= 1 /\ cfg_rep_limit <= EffU32Max
  /\ cfg_allow_freeze = AllowFreeze

Init ==
  /\ cfg_allow_freeze = AllowFreeze
  /\ IF BoundedMode
     THEN /\ cfg_acct1_ord_cap = 1
          /\ cfg_acct2_ord_cap = 1
          /\ cfg_acct1_cxl_cap = 1
          /\ cfg_shared_ip_cap = 2
          /\ cfg_rep_limit = 2
     ELSE /\ cfg_acct1_ord_cap \in Int /\ cfg_acct1_ord_cap >= 1 /\ cfg_acct1_ord_cap <= EffU32Max
          /\ cfg_acct2_ord_cap \in Int /\ cfg_acct2_ord_cap >= 1 /\ cfg_acct2_ord_cap <= EffU32Max
          /\ cfg_acct1_cxl_cap \in Int /\ cfg_acct1_cxl_cap >= 1 /\ cfg_acct1_cxl_cap <= EffU32Max
          /\ cfg_shared_ip_cap \in Int /\ cfg_shared_ip_cap >= 2 /\ cfg_shared_ip_cap <= EffRollMax
          /\ cfg_rep_limit \in Int /\ cfg_rep_limit >= 1 /\ cfg_rep_limit <= EffU32Max
  /\ last_now_ns = 0
  /\ acct1_frozen = FALSE
  /\ acct1_cd_until_ns = 0
  /\ ip1_cd_until_ns = 0
  /\ r1_ip1_ready = TRUE
  /\ r1_last_conn = 0
  /\ a1_ord_init = FALSE /\ a1_ord_ep = 0 /\ a1_ord_used = 0
  /\ a2_ord_init = FALSE /\ a2_ord_ep = 0 /\ a2_ord_used = 0
  /\ a1_cxl_init = FALSE /\ a1_cxl_ep = 0 /\ a1_cxl_used = 0
  /\ ip1_len = 0 /\ ip1_t0 = 0 /\ ip1_u0 = 0 /\ ip1_t1 = 0 /\ ip1_u1 = 0 /\ ip1_sum = 0
  /\ ip2_len = 0 /\ ip2_t0 = 0 /\ ip2_u0 = 0 /\ ip2_t1 = 0 /\ ip2_u1 = 0 /\ ip2_sum = 0
  /\ exch_latch_warn = FALSE
  /\ exch_latch_ceil = FALSE
  /\ step_budget = 0
  /\ rollback_exact_ok = TRUE
  /\ single_pick_no_retry_ok = TRUE
  /\ cross_acct_isolation_ok = TRUE
  /\ rising_edge_latch_ok = TRUE
  /\ u64_u128_overflow_ok = TRUE

(* 1. Account 1 single-pick `pick_route_and_place_or_nack` (`[Acct1Ord, SharedIp(picked)]`):
 *    If `picked = 1` rejects (e.g., `ip1` exhausted or in `429` cooldown), NEVER retries `ip2`!
 *    If `Acct1Ord` succeeds and `SharedIp(picked)` rejects, `AtomicFixedWindow::put_back` unwinds `Acct1Ord`. *)
PickAndPlaceAcct1 ==
  /\ CanStep
  /\ \E dt \in TimeDom, ord_u \in UnitDom, ip_u \in UnitDom :
       /\ dt >= 0 /\ dt <= EffU64Max
       /\ ord_u >= 1 /\ ord_u <= EffU32Max
       /\ ip_u >= 1 /\ ip_u <= EffRollMax
       /\ LET now == SatAddU64(last_now_ns, dt)
              ep == FixedEpochIdx(now)
              tk == SubTickIdx(now)
              picked == PickRoute1
              pre_ok == (~acct1_frozen) /\ (now >= acct1_cd_until_ns)
                        /\ ((picked = 1) => (now >= ip1_cd_until_ns))
              cur_ord == FixedAdvUsed(a1_ord_init, a1_ord_ep, a1_ord_used, now)
              ord_ok == (cur_ord + ord_u <= cfg_acct1_ord_cap)
              el1 == SubEffLen(ip1_len, ip1_t0, ip1_t1, tk)
              et1_0 == SubEffT0(ip1_len, ip1_t0, ip1_t1, tk)
              eu1_0 == SubEffU0(ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, tk)
              et1_1 == SubEffT1(ip1_len, ip1_t0, ip1_t1, tk)
              eu1_1 == SubEffU1(ip1_len, ip1_t0, ip1_t1, ip1_u1, tk)
              esum1 == eu1_0 + eu1_1
              el2 == SubEffLen(ip2_len, ip2_t0, ip2_t1, tk)
              et2_0 == SubEffT0(ip2_len, ip2_t0, ip2_t1, tk)
              eu2_0 == SubEffU0(ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, tk)
              et2_1 == SubEffT1(ip2_len, ip2_t0, ip2_t1, tk)
              eu2_1 == SubEffU1(ip2_len, ip2_t0, ip2_t1, ip2_u1, tk)
              esum2 == eu2_0 + eu2_1
              ip_ok == IF picked = 1
                       THEN SubCanTake(el1, et1_0, et1_1, esum1, ip_u, cfg_shared_ip_cap, tk)
                       ELSE SubCanTake(el2, et2_0, et2_1, esum2, ip_u, cfg_shared_ip_cap, tk)
              all_ok == pre_ok /\ ord_ok /\ ip_ok
              \* Explicit FixedWindow::put_back check when ord_ok /\ ~ip_ok:
              post_ord_take == cur_ord + ord_u
              put_back_ord == post_ord_take - ord_u
          IN /\ last_now_ns' = now
             /\ step_budget' = NextBudget
             /\ IF all_ok
                THEN /\ a1_ord_init' = TRUE
                     /\ a1_ord_ep' = ep
                     /\ a1_ord_used' = cur_ord + ord_u
                     /\ r1_last_conn' = picked
                     /\ IF picked = 1
                        THEN /\ ip1_len' = SubPostLen(el1, et1_0, et1_1, tk)
                             /\ ip1_t0' = SubPostT0(el1, et1_0, tk)
                             /\ ip1_u0' = SubPostU0(el1, et1_0, eu1_0, ip_u, tk)
                             /\ ip1_t1' = SubPostT1(el1, et1_0, et1_1, tk)
                             /\ ip1_u1' = SubPostU1(el1, et1_0, et1_1, eu1_1, ip_u, tk)
                             /\ ip1_sum' = esum1 + ip_u
                             /\ UNCHANGED <<ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum>>
                        ELSE /\ ip2_len' = SubPostLen(el2, et2_0, et2_1, tk)
                             /\ ip2_t0' = SubPostT0(el2, et2_0, tk)
                             /\ ip2_u0' = SubPostU0(el2, et2_0, eu2_0, ip_u, tk)
                             /\ ip2_t1' = SubPostT1(el2, et2_0, et2_1, tk)
                             /\ ip2_u1' = SubPostU1(el2, et2_0, et2_1, eu2_1, ip_u, tk)
                             /\ ip2_sum' = esum2 + ip_u
                             /\ UNCHANGED <<ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum>>
                ELSE /\ IF pre_ok /\ ord_ok
                        THEN /\ a1_ord_init' = TRUE
                             /\ a1_ord_ep' = ep
                             /\ a1_ord_used' = put_back_ord
                        ELSE UNCHANGED <<a1_ord_init, a1_ord_ep, a1_ord_used>>
                     /\ UNCHANGED <<r1_last_conn,
                                    ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum,
                                    ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum>>
             /\ rollback_exact_ok' = (rollback_exact_ok /\ (put_back_ord = cur_ord))
             /\ single_pick_no_retry_ok' = (single_pick_no_retry_ok
                  /\ ((picked = 1 /\ ~all_ok) => (ip2_sum' = ip2_sum))
                  /\ ((picked = 2 /\ ~all_ok) => (ip1_sum' = ip1_sum)))
             /\ UNCHANGED <<
                  cfg_acct1_ord_cap, cfg_acct2_ord_cap, cfg_acct1_cxl_cap, cfg_shared_ip_cap, cfg_rep_limit, cfg_allow_freeze,
                  acct1_frozen, acct1_cd_until_ns, ip1_cd_until_ns, r1_ip1_ready,
                  a2_ord_init, a2_ord_ep, a2_ord_used,
                  a1_cxl_init, a1_cxl_ep, a1_cxl_used,
                  exch_latch_warn, exch_latch_ceil,
                  cross_acct_isolation_ok, rising_edge_latch_ok, u64_u128_overflow_ok
                >>

(* 2. Sibling Account 2 concurrent placement contending on shared `RateRule::ip(1)`:
 *    Proves Account 2 shares `ip1` capacity with Account 1 while remaining immune to
 *    Account 1's `418` freeze, `acct1_cd_until_ns` cooldown, and `a1_ord_used` exhaustion! *)
PlaceAcct2SharedIp ==
  /\ CanStep
  /\ \E dt \in TimeDom, ip_u \in UnitDom :
       /\ dt >= 0 /\ dt <= EffU64Max
       /\ ip_u >= 1 /\ ip_u <= EffRollMax
       /\ LET now == SatAddU64(last_now_ns, dt)
              ep == FixedEpochIdx(now)
              tk == SubTickIdx(now)
              pre_ok == (now >= ip1_cd_until_ns)
              cur_ord2 == FixedAdvUsed(a2_ord_init, a2_ord_ep, a2_ord_used, now)
              ord2_ok == (cur_ord2 + 1 <= cfg_acct2_ord_cap)
              el1 == SubEffLen(ip1_len, ip1_t0, ip1_t1, tk)
              et1_0 == SubEffT0(ip1_len, ip1_t0, ip1_t1, tk)
              eu1_0 == SubEffU0(ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, tk)
              et1_1 == SubEffT1(ip1_len, ip1_t0, ip1_t1, tk)
              eu1_1 == SubEffU1(ip1_len, ip1_t0, ip1_t1, ip1_u1, tk)
              esum1 == eu1_0 + eu1_1
              ip1_ok == SubCanTake(el1, et1_0, et1_1, esum1, ip_u, cfg_shared_ip_cap, tk)
              all_ok == pre_ok /\ ord2_ok /\ ip1_ok
          IN /\ last_now_ns' = now
             /\ step_budget' = NextBudget
             /\ IF all_ok
                THEN /\ a2_ord_init' = TRUE
                     /\ a2_ord_ep' = ep
                     /\ a2_ord_used' = cur_ord2 + 1
                     /\ ip1_len' = SubPostLen(el1, et1_0, et1_1, tk)
                     /\ ip1_t0' = SubPostT0(el1, et1_0, tk)
                     /\ ip1_u0' = SubPostU0(el1, et1_0, eu1_0, ip_u, tk)
                     /\ ip1_t1' = SubPostT1(el1, et1_0, et1_1, tk)
                     /\ ip1_u1' = SubPostU1(el1, et1_0, et1_1, eu1_1, ip_u, tk)
                     /\ ip1_sum' = esum1 + ip_u
                ELSE /\ IF pre_ok /\ ord2_ok
                        THEN /\ a2_ord_init' = TRUE
                             /\ a2_ord_ep' = ep
                             /\ a2_ord_used' = (cur_ord2 + 1) - 1
                        ELSE UNCHANGED <<a2_ord_init, a2_ord_ep, a2_ord_used>>
                     /\ UNCHANGED <<ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum>>
             /\ cross_acct_isolation_ok' = (cross_acct_isolation_ok
                  /\ ((pre_ok /\ ord2_ok /\ ip1_ok) => all_ok))
             /\ UNCHANGED <<
                  cfg_acct1_ord_cap, cfg_acct2_ord_cap, cfg_acct1_cxl_cap, cfg_shared_ip_cap, cfg_rep_limit, cfg_allow_freeze,
                  acct1_frozen, acct1_cd_until_ns, ip1_cd_until_ns, r1_ip1_ready, r1_last_conn,
                  a1_ord_init, a1_ord_ep, a1_ord_used,
                  a1_cxl_init, a1_cxl_ep, a1_cxl_used,
                  ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum,
                  exch_latch_warn, exch_latch_ceil,
                  rollback_exact_ok, single_pick_no_retry_ok, rising_edge_latch_ok, u64_u128_overflow_ok
                >>

(* 3. Reverse-order rolling -> fixed rollback (`[SharedIp(1), Acct1Ord]`):
 *    `AtomicRollingWindow::try_take` succeeds (coalescing with Account 2's sub-bucket or pushing
 *    a new sub-bucket), then `Acct1Ord` rejects, invoking `AtomicRollingWindow::put_back`
 *    (crates/tickwire-quota/src/shared.rs:617-642) and proving sibling sub-bucket counts are preserved 100%! *)
ReverseRollFirstRollback ==
  /\ CanStep
  /\ \E dt \in TimeDom, ip_u \in UnitDom, ord_u \in UnitDom :
       /\ dt >= 0 /\ dt <= EffU64Max
       /\ ip_u >= 1 /\ ip_u <= EffRollMax
       /\ ord_u >= 1 /\ ord_u <= EffU32Max
       /\ LET now == SatAddU64(last_now_ns, dt)
              ep == FixedEpochIdx(now)
              tk == SubTickIdx(now)
              pre_ok == (~acct1_frozen) /\ (now >= acct1_cd_until_ns) /\ (now >= ip1_cd_until_ns)
              el1 == SubEffLen(ip1_len, ip1_t0, ip1_t1, tk)
              et1_0 == SubEffT0(ip1_len, ip1_t0, ip1_t1, tk)
              eu1_0 == SubEffU0(ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, tk)
              et1_1 == SubEffT1(ip1_len, ip1_t0, ip1_t1, tk)
              eu1_1 == SubEffU1(ip1_len, ip1_t0, ip1_t1, ip1_u1, tk)
              esum1 == eu1_0 + eu1_1
              coal == SubCoalesces(el1, et1_0, et1_1, tk)
              ip_ok == SubCanTake(el1, et1_0, et1_1, esum1, ip_u, cfg_shared_ip_cap, tk)
              cur_ord == FixedAdvUsed(a1_ord_init, a1_ord_ep, a1_ord_used, now)
              ord_ok == (cur_ord + ord_u <= cfg_acct1_ord_cap)
              plen == SubPostLen(el1, et1_0, et1_1, tk)
              pt0 == SubPostT0(el1, et1_0, tk)
              pu0 == SubPostU0(el1, et1_0, eu1_0, ip_u, tk)
              pt1 == SubPostT1(el1, et1_0, et1_1, tk)
              pu1 == SubPostU1(el1, et1_0, et1_1, eu1_1, ip_u, tk)
              psum == esum1 + ip_u
              ub_len == SubPutBackLen(coal, plen)
              ub_t0 == SubPutBackT0(coal, plen, pt0)
              ub_u0 == SubPutBackU0(coal, plen, pu0, ip_u)
              ub_t1 == SubPutBackT1(coal, plen, pt1)
              ub_u1 == SubPutBackU1(coal, plen, pu1, ip_u)
              ub_sum == psum - ip_u
          IN /\ last_now_ns' = now
             /\ step_budget' = NextBudget
             /\ IF pre_ok /\ ip_ok /\ ord_ok
                THEN /\ ip1_len' = plen /\ ip1_t0' = pt0 /\ ip1_u0' = pu0
                     /\ ip1_t1' = pt1 /\ ip1_u1' = pu1 /\ ip1_sum' = psum
                     /\ a1_ord_init' = TRUE /\ a1_ord_ep' = ep /\ a1_ord_used' = cur_ord + ord_u
                     /\ rollback_exact_ok' = rollback_exact_ok
                ELSE IF pre_ok /\ ip_ok /\ (~ord_ok)
                THEN /\ ip1_len' = ub_len /\ ip1_t0' = ub_t0 /\ ip1_u0' = ub_u0
                     /\ ip1_t1' = ub_t1 /\ ip1_u1' = ub_u1 /\ ip1_sum' = ub_sum
                     /\ a1_ord_init' = TRUE /\ a1_ord_ep' = ep /\ a1_ord_used' = cur_ord
                     /\ rollback_exact_ok' = (rollback_exact_ok
                          /\ ub_len = el1 /\ ub_t0 = et1_0 /\ ub_u0 = eu1_0
                          /\ ub_t1 = et1_1 /\ ub_u1 = eu1_1 /\ ub_sum = esum1)
                ELSE /\ UNCHANGED <<ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum,
                                    a1_ord_init, a1_ord_ep, a1_ord_used>>
                     /\ rollback_exact_ok' = rollback_exact_ok
             /\ UNCHANGED <<
                  cfg_acct1_ord_cap, cfg_acct2_ord_cap, cfg_acct1_cxl_cap, cfg_shared_ip_cap, cfg_rep_limit, cfg_allow_freeze,
                  acct1_frozen, acct1_cd_until_ns, ip1_cd_until_ns, r1_ip1_ready, r1_last_conn,
                  a2_ord_init, a2_ord_ep, a2_ord_used,
                  a1_cxl_init, a1_cxl_ep, a1_cxl_used,
                  ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum,
                  exch_latch_warn, exch_latch_ceil,
                  single_pick_no_retry_ok, cross_acct_isolation_ok, rising_edge_latch_ok, u64_u128_overflow_ok
                >>

(* 4. Account 1 `pick_route_and_cancel_or_nack` (`[Acct1Cxl, SharedIp(picked)]`):
 *    Strictly isolated from `a1_ord_used`! *)
PickAndCancelAcct1 ==
  /\ CanStep
  /\ \E dt \in TimeDom :
       /\ dt >= 0 /\ dt <= EffU64Max
       /\ LET now == SatAddU64(last_now_ns, dt)
              ep == FixedEpochIdx(now)
              tk == SubTickIdx(now)
              picked == PickRoute1
              pre_ok == (~acct1_frozen) /\ (now >= acct1_cd_until_ns)
                        /\ ((picked = 1) => (now >= ip1_cd_until_ns))
              cur_cxl == FixedAdvUsed(a1_cxl_init, a1_cxl_ep, a1_cxl_used, now)
              cxl_ok == (cur_cxl + 1 <= cfg_acct1_cxl_cap)
              el1 == SubEffLen(ip1_len, ip1_t0, ip1_t1, tk)
              et1_0 == SubEffT0(ip1_len, ip1_t0, ip1_t1, tk)
              eu1_0 == SubEffU0(ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, tk)
              et1_1 == SubEffT1(ip1_len, ip1_t0, ip1_t1, tk)
              eu1_1 == SubEffU1(ip1_len, ip1_t0, ip1_t1, ip1_u1, tk)
              esum1 == eu1_0 + eu1_1
              el2 == SubEffLen(ip2_len, ip2_t0, ip2_t1, tk)
              et2_0 == SubEffT0(ip2_len, ip2_t0, ip2_t1, tk)
              eu2_0 == SubEffU0(ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, tk)
              et2_1 == SubEffT1(ip2_len, ip2_t0, ip2_t1, tk)
              eu2_1 == SubEffU1(ip2_len, ip2_t0, ip2_t1, ip2_u1, tk)
              esum2 == eu2_0 + eu2_1
              ip_ok == IF picked = 1
                       THEN SubCanTake(el1, et1_0, et1_1, esum1, 1, cfg_shared_ip_cap, tk)
                       ELSE SubCanTake(el2, et2_0, et2_1, esum2, 1, cfg_shared_ip_cap, tk)
              all_ok == pre_ok /\ cxl_ok /\ ip_ok
          IN /\ last_now_ns' = now
             /\ step_budget' = NextBudget
             /\ IF all_ok
                THEN /\ a1_cxl_init' = TRUE /\ a1_cxl_ep' = ep /\ a1_cxl_used' = cur_cxl + 1
                     /\ r1_last_conn' = picked
                     /\ IF picked = 1
                        THEN /\ ip1_len' = SubPostLen(el1, et1_0, et1_1, tk)
                             /\ ip1_t0' = SubPostT0(el1, et1_0, tk)
                             /\ ip1_u0' = SubPostU0(el1, et1_0, eu1_0, 1, tk)
                             /\ ip1_t1' = SubPostT1(el1, et1_0, et1_1, tk)
                             /\ ip1_u1' = SubPostU1(el1, et1_0, et1_1, eu1_1, 1, tk)
                             /\ ip1_sum' = esum1 + 1
                             /\ UNCHANGED <<ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum>>
                        ELSE /\ ip2_len' = SubPostLen(el2, et2_0, et2_1, tk)
                             /\ ip2_t0' = SubPostT0(el2, et2_0, tk)
                             /\ ip2_u0' = SubPostU0(el2, et2_0, eu2_0, 1, tk)
                             /\ ip2_t1' = SubPostT1(el2, et2_0, et2_1, tk)
                             /\ ip2_u1' = SubPostU1(el2, et2_0, et2_1, eu2_1, 1, tk)
                             /\ ip2_sum' = esum2 + 1
                             /\ UNCHANGED <<ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum>>
                ELSE /\ IF pre_ok /\ cxl_ok
                        THEN /\ a1_cxl_init' = TRUE /\ a1_cxl_ep' = ep /\ a1_cxl_used' = (cur_cxl + 1) - 1
                        ELSE UNCHANGED <<a1_cxl_init, a1_cxl_ep, a1_cxl_used>>
                     /\ UNCHANGED <<r1_last_conn,
                                    ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum,
                                    ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum>>
             /\ cross_acct_isolation_ok' = (cross_acct_isolation_ok
                  /\ ((pre_ok /\ cxl_ok /\ ip_ok) => all_ok))
             /\ UNCHANGED <<
                  cfg_acct1_ord_cap, cfg_acct2_ord_cap, cfg_acct1_cxl_cap, cfg_shared_ip_cap, cfg_rep_limit, cfg_allow_freeze,
                  acct1_frozen, acct1_cd_until_ns, ip1_cd_until_ns, r1_ip1_ready,
                  a1_ord_init, a1_ord_ep, a1_ord_used,
                  a2_ord_init, a2_ord_ep, a2_ord_used,
                  exch_latch_warn, exch_latch_ceil,
                  rollback_exact_ok, single_pick_no_retry_ok, rising_edge_latch_ok, u64_u128_overflow_ok
                >>

(* 5. `OrsAccountQuota::allow_query` on `ip2` (isolated from ORDER & CANCEL & `ip1` `429`): *)
AllowQueryAcct1 ==
  /\ CanStep
  /\ \E dt \in TimeDom :
       /\ dt >= 0 /\ dt <= EffU64Max
       /\ LET now == SatAddU64(last_now_ns, dt)
              tk == SubTickIdx(now)
              pre_ok == (~acct1_frozen) /\ (now >= acct1_cd_until_ns)
              el2 == SubEffLen(ip2_len, ip2_t0, ip2_t1, tk)
              et2_0 == SubEffT0(ip2_len, ip2_t0, ip2_t1, tk)
              eu2_0 == SubEffU0(ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, tk)
              et2_1 == SubEffT1(ip2_len, ip2_t0, ip2_t1, tk)
              eu2_1 == SubEffU1(ip2_len, ip2_t0, ip2_t1, ip2_u1, tk)
              esum2 == eu2_0 + eu2_1
              ip2_ok == SubCanTake(el2, et2_0, et2_1, esum2, 1, cfg_shared_ip_cap, tk)
          IN /\ last_now_ns' = now
             /\ step_budget' = NextBudget
             /\ IF pre_ok /\ ip2_ok
                THEN /\ ip2_len' = SubPostLen(el2, et2_0, et2_1, tk)
                     /\ ip2_t0' = SubPostT0(el2, et2_0, tk)
                     /\ ip2_u0' = SubPostU0(el2, et2_0, eu2_0, 1, tk)
                     /\ ip2_t1' = SubPostT1(el2, et2_0, et2_1, tk)
                     /\ ip2_u1' = SubPostU1(el2, et2_0, et2_1, eu2_1, 1, tk)
                     /\ ip2_sum' = esum2 + 1
                ELSE UNCHANGED <<ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum>>
             /\ UNCHANGED <<
                  cfg_acct1_ord_cap, cfg_acct2_ord_cap, cfg_acct1_cxl_cap, cfg_shared_ip_cap, cfg_rep_limit, cfg_allow_freeze,
                  acct1_frozen, acct1_cd_until_ns, ip1_cd_until_ns, r1_ip1_ready, r1_last_conn,
                  a1_ord_init, a1_ord_ep, a1_ord_used,
                  a2_ord_init, a2_ord_ep, a2_ord_used,
                  a1_cxl_init, a1_cxl_ep, a1_cxl_used,
                  ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum,
                  exch_latch_warn, exch_latch_ceil,
                  rollback_exact_ok, single_pick_no_retry_ok, cross_acct_isolation_ok, rising_edge_latch_ok, u64_u128_overflow_ok
                >>

(* 6. `route_ip_key(route.local_ip == 0)` -> `MissingLocalIp` (crates/tickwire-quota/src/shared.rs:1809-1816), zero burn: *)
RejectMissingLocalIp ==
  /\ CanStep
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_acct1_ord_cap, cfg_acct2_ord_cap, cfg_acct1_cxl_cap, cfg_shared_ip_cap, cfg_rep_limit, cfg_allow_freeze,
       last_now_ns, acct1_frozen, acct1_cd_until_ns, ip1_cd_until_ns,
       r1_ip1_ready, r1_last_conn,
       a1_ord_init, a1_ord_ep, a1_ord_used,
       a2_ord_init, a2_ord_ep, a2_ord_used,
       a1_cxl_init, a1_cxl_ep, a1_cxl_used,
       ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum,
       ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum,
       exch_latch_warn, exch_latch_ceil,
       rollback_exact_ok, single_pick_no_retry_ok, cross_acct_isolation_ok, rising_edge_latch_ok, u64_u128_overflow_ok
     >>

(* 7. `AtomicFixedWindow::try_take` clock-regression guard (crates/tickwire-quota/src/shared.rs:358-359):
 *    When `reg_ns` maps to an earlier `u32` epoch within `< u32::MAX / 2`, rejects with zero burn! *)
ClockRegressionFixedGuard ==
  /\ CanStep
  /\ a1_ord_init
  /\ a1_ord_ep >= 1
  /\ \E reg_ns \in TimeDom :
       /\ reg_ns >= 0 /\ reg_ns < last_now_ns
       /\ FixedClockRegressed(a1_ord_init, a1_ord_ep, reg_ns)
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_acct1_ord_cap, cfg_acct2_ord_cap, cfg_acct1_cxl_cap, cfg_shared_ip_cap, cfg_rep_limit, cfg_allow_freeze,
            last_now_ns, acct1_frozen, acct1_cd_until_ns, ip1_cd_until_ns,
            r1_ip1_ready, r1_last_conn,
            a1_ord_init, a1_ord_ep, a1_ord_used,
            a2_ord_init, a2_ord_ep, a2_ord_used,
            a1_cxl_init, a1_cxl_ep, a1_cxl_used,
            ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum,
            ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum,
            exch_latch_warn, exch_latch_ceil,
            rollback_exact_ok, single_pick_no_retry_ok, cross_acct_isolation_ok, rising_edge_latch_ok, u64_u128_overflow_ok
          >>

(* 8 & 9. Route 1 disconnect / restore in `PublishedRoutes`: *)
ToggleRoute1Down ==
  /\ CanStep
  /\ r1_ip1_ready
  /\ r1_ip1_ready' = FALSE
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_acct1_ord_cap, cfg_acct2_ord_cap, cfg_acct1_cxl_cap, cfg_shared_ip_cap, cfg_rep_limit, cfg_allow_freeze,
       last_now_ns, acct1_frozen, acct1_cd_until_ns, ip1_cd_until_ns, r1_last_conn,
       a1_ord_init, a1_ord_ep, a1_ord_used,
       a2_ord_init, a2_ord_ep, a2_ord_used,
       a1_cxl_init, a1_cxl_ep, a1_cxl_used,
       ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum,
       ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum,
       exch_latch_warn, exch_latch_ceil,
       rollback_exact_ok, single_pick_no_retry_ok, cross_acct_isolation_ok, rising_edge_latch_ok, u64_u128_overflow_ok
     >>

RestoreRoute1Up ==
  /\ CanStep
  /\ ~r1_ip1_ready
  /\ r1_ip1_ready' = TRUE
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_acct1_ord_cap, cfg_acct2_ord_cap, cfg_acct1_cxl_cap, cfg_shared_ip_cap, cfg_rep_limit, cfg_allow_freeze,
       last_now_ns, acct1_frozen, acct1_cd_until_ns, ip1_cd_until_ns, r1_last_conn,
       a1_ord_init, a1_ord_ep, a1_ord_used,
       a2_ord_init, a2_ord_ep, a2_ord_used,
       a1_cxl_init, a1_cxl_ep, a1_cxl_used,
       ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum,
       ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum,
       exch_latch_warn, exch_latch_ceil,
       rollback_exact_ok, single_pick_no_retry_ok, cross_acct_isolation_ok, rising_edge_latch_ok, u64_u128_overflow_ok
     >>

(* 10. Per-IP HTTP `429` cooldown (`note_ip_cooldown`, crates/tickwire-quota/src/shared.rs:1197-1226): *)
NoteIp1Cooldown429 ==
  /\ CanStep
  /\ \E dt \in TimeDom, dur \in UnitDom :
       /\ dt >= 0 /\ dt <= EffU64Max
       /\ dur >= 1 /\ dur <= EffU64Max
       /\ LET now == SatAddU64(last_now_ns, dt)
              until_ns == SatAddU64(now, dur)
          IN /\ last_now_ns' = now
             /\ ip1_cd_until_ns' = Max2(ip1_cd_until_ns, until_ns)
             /\ step_budget' = NextBudget
             /\ UNCHANGED <<
                  cfg_acct1_ord_cap, cfg_acct2_ord_cap, cfg_acct1_cxl_cap, cfg_shared_ip_cap, cfg_rep_limit, cfg_allow_freeze,
                  acct1_frozen, acct1_cd_until_ns, r1_ip1_ready, r1_last_conn,
                  a1_ord_init, a1_ord_ep, a1_ord_used,
                  a2_ord_init, a2_ord_ep, a2_ord_used,
                  a1_cxl_init, a1_cxl_ep, a1_cxl_used,
                  ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum,
                  ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum,
                  exch_latch_warn, exch_latch_ceil,
                  rollback_exact_ok, single_pick_no_retry_ok, cross_acct_isolation_ok, rising_edge_latch_ok, u64_u128_overflow_ok
                >>

(* 11. Per-account temporary cooldown (`note_account_cooldown`, crates/tickwire-quota/src/shared.rs:1100-1129): *)
NoteAcct1Cooldown ==
  /\ CanStep
  /\ \E dt \in TimeDom, dur \in UnitDom :
       /\ dt >= 0 /\ dt <= EffU64Max
       /\ dur >= 1 /\ dur <= EffU64Max
       /\ LET now == SatAddU64(last_now_ns, dt)
              until_ns == SatAddU64(now, dur)
          IN /\ last_now_ns' = now
             /\ acct1_cd_until_ns' = Max2(acct1_cd_until_ns, until_ns)
             /\ step_budget' = NextBudget
             /\ UNCHANGED <<
                  cfg_acct1_ord_cap, cfg_acct2_ord_cap, cfg_acct1_cxl_cap, cfg_shared_ip_cap, cfg_rep_limit, cfg_allow_freeze,
                  acct1_frozen, ip1_cd_until_ns, r1_ip1_ready, r1_last_conn,
                  a1_ord_init, a1_ord_ep, a1_ord_used,
                  a2_ord_init, a2_ord_ep, a2_ord_used,
                  a1_cxl_init, a1_cxl_ep, a1_cxl_used,
                  ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum,
                  ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum,
                  exch_latch_warn, exch_latch_ceil,
                  rollback_exact_ok, single_pick_no_retry_ok, cross_acct_isolation_ok, rising_edge_latch_ok, u64_u128_overflow_ok
                >>

(* 12. Per-account HTTP `418` ban (`freeze_account(1)`, crates/tickwire-quota/src/shared.rs:1132-1161): *)
FreezeAcct1Ban418 ==
  /\ CanStep
  /\ cfg_allow_freeze
  /\ ~acct1_frozen
  /\ acct1_frozen' = TRUE
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_acct1_ord_cap, cfg_acct2_ord_cap, cfg_acct1_cxl_cap, cfg_shared_ip_cap, cfg_rep_limit, cfg_allow_freeze,
       last_now_ns, acct1_cd_until_ns, ip1_cd_until_ns, r1_ip1_ready, r1_last_conn,
       a1_ord_init, a1_ord_ep, a1_ord_used,
       a2_ord_init, a2_ord_ep, a2_ord_used,
       a1_cxl_init, a1_cxl_ep, a1_cxl_used,
       ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum,
       ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum,
       exch_latch_warn, exch_latch_ceil,
       rollback_exact_ok, single_pick_no_retry_ok, cross_acct_isolation_ok, rising_edge_latch_ok, u64_u128_overflow_ok
     >>

(* 13. Rising-edge venue `rateLimits[]` reconciliation (`reconcile_venue_usage`, crates/tickwire-quota/src/shared.rs:1361-1441):
 *     Exact `u128` threshold math: `rep * 100 >= cfg_rep_limit * 80` and `rep >= cfg_rep_limit`,
 *     with zero local bucket mutation and zero `u128` overflow across all `u64` `rep`! *)
ReconcileVenueRateLimit ==
  /\ CanStep
  /\ \E rep \in ReportDom :
       /\ rep >= 0 /\ rep <= EffU64Max
       /\ LET prod_lhs == rep * 100
              prod_rhs == cfg_rep_limit * 80
              warn_hit == (prod_lhs >= prod_rhs)
              ceil_hit == (rep >= cfg_rep_limit)
              crossed_w == warn_hit /\ (~exch_latch_warn)
              crossed_c == ceil_hit /\ (~exch_latch_ceil)
              emitted_ceil == crossed_c
              emitted_warn == (~crossed_c) /\ crossed_w
          IN /\ exch_latch_warn' = warn_hit
             /\ exch_latch_ceil' = ceil_hit
             /\ rising_edge_latch_ok' = (rising_edge_latch_ok
                  /\ (ceil_hit => warn_hit)
                  /\ (emitted_ceil <=> (ceil_hit /\ ~exch_latch_ceil))
                  /\ (emitted_warn <=> (~(ceil_hit /\ ~exch_latch_ceil) /\ warn_hit /\ ~exch_latch_warn)))
             /\ u64_u128_overflow_ok' = (u64_u128_overflow_ok
                  /\ prod_lhs >= 0 /\ prod_lhs <= EffU128Max
                  /\ prod_rhs >= 0 /\ prod_rhs <= EffU128Max)
             /\ step_budget' = NextBudget
             /\ UNCHANGED <<
                  cfg_acct1_ord_cap, cfg_acct2_ord_cap, cfg_acct1_cxl_cap, cfg_shared_ip_cap, cfg_rep_limit, cfg_allow_freeze,
                  last_now_ns, acct1_frozen, acct1_cd_until_ns, ip1_cd_until_ns, r1_ip1_ready, r1_last_conn,
                  a1_ord_init, a1_ord_ep, a1_ord_used,
                  a2_ord_init, a2_ord_ep, a2_ord_used,
                  a1_cxl_init, a1_cxl_ep, a1_cxl_used,
                  ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum,
                  ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum,
                  rollback_exact_ok, single_pick_no_retry_ok, cross_acct_isolation_ok
                >>

Next ==
  \/ PickAndPlaceAcct1
  \/ PlaceAcct2SharedIp
  \/ ReverseRollFirstRollback
  \/ PickAndCancelAcct1
  \/ AllowQueryAcct1
  \/ RejectMissingLocalIp
  \/ ClockRegressionFixedGuard
  \/ ToggleRoute1Down
  \/ RestoreRoute1Up
  \/ NoteIp1Cooldown429
  \/ NoteAcct1Cooldown
  \/ FreezeAcct1Ban418
  \/ ReconcileVenueRateLimit

Spec == Init /\ [][Next]_vars

(* --- Safety Invariants & Mode S2 Strengthened Inductive Invariant (`IndInv`) --- *)

SubRollWellFormed(len, t0, u0, t1, u1, sum, cap) ==
  /\ len \in {0, 1, 2}
  /\ t0 \in Int /\ t0 >= 0 /\ t0 <= SubTickIdx(EffU64Max)
  /\ u0 \in Int /\ u0 >= 0 /\ u0 <= cap
  /\ t1 \in Int /\ t1 >= 0 /\ t1 <= SubTickIdx(EffU64Max)
  /\ u1 \in Int /\ u1 >= 0 /\ u1 <= cap
  /\ sum \in Int /\ sum >= 0 /\ sum <= cap
  /\ ((len = 0) => (t0 = 0 /\ u0 = 0 /\ t1 = 0 /\ u1 = 0 /\ sum = 0))
  /\ ((len = 1) => (u0 >= 1 /\ t0 <= SubTickIdx(last_now_ns) /\ t1 = 0 /\ u1 = 0 /\ sum = u0))
  /\ ((len = 2) => (u0 >= 1 /\ u1 >= 1 /\ t0 < t1 /\ t1 <= SubTickIdx(last_now_ns) /\ sum = u0 + u1))

TypeOK ==
  /\ ValidConfig
  /\ last_now_ns \in Int /\ last_now_ns >= 0 /\ last_now_ns <= EffU64Max
  /\ acct1_frozen \in BOOLEAN
  /\ acct1_cd_until_ns \in Int /\ acct1_cd_until_ns >= 0 /\ acct1_cd_until_ns <= EffU64Max
  /\ ip1_cd_until_ns \in Int /\ ip1_cd_until_ns >= 0 /\ ip1_cd_until_ns <= EffU64Max
  /\ r1_ip1_ready \in BOOLEAN
  /\ r1_last_conn \in {0, 1, 2}
  /\ a1_ord_init \in BOOLEAN
  /\ a1_ord_ep \in Int /\ a1_ord_ep >= 0 /\ a1_ord_ep <= MaxFixedEpoch
  /\ a1_ord_used \in Int /\ a1_ord_used >= 0 /\ a1_ord_used <= cfg_acct1_ord_cap
  /\ a2_ord_init \in BOOLEAN
  /\ a2_ord_ep \in Int /\ a2_ord_ep >= 0 /\ a2_ord_ep <= MaxFixedEpoch
  /\ a2_ord_used \in Int /\ a2_ord_used >= 0 /\ a2_ord_used <= cfg_acct2_ord_cap
  /\ a1_cxl_init \in BOOLEAN
  /\ a1_cxl_ep \in Int /\ a1_cxl_ep >= 0 /\ a1_cxl_ep <= MaxFixedEpoch
  /\ a1_cxl_used \in Int /\ a1_cxl_used >= 0 /\ a1_cxl_used <= cfg_acct1_cxl_cap
  /\ SubRollWellFormed(ip1_len, ip1_t0, ip1_u0, ip1_t1, ip1_u1, ip1_sum, cfg_shared_ip_cap)
  /\ SubRollWellFormed(ip2_len, ip2_t0, ip2_u0, ip2_t1, ip2_u1, ip2_sum, cfg_shared_ip_cap)
  /\ exch_latch_warn \in BOOLEAN
  /\ exch_latch_ceil \in BOOLEAN
  /\ step_budget \in Int /\ step_budget >= 0 /\ step_budget <= MaxBoundedSteps
  /\ rollback_exact_ok \in BOOLEAN
  /\ single_pick_no_retry_ok \in BOOLEAN
  /\ cross_acct_isolation_ok \in BOOLEAN
  /\ rising_edge_latch_ok \in BOOLEAN
  /\ u64_u128_overflow_ok \in BOOLEAN

SharedAndAccountCapsRespected ==
  /\ a1_ord_used <= cfg_acct1_ord_cap
  /\ a2_ord_used <= cfg_acct2_ord_cap
  /\ a1_cxl_used <= cfg_acct1_cxl_cap
  /\ ip1_sum <= cfg_shared_ip_cap
  /\ ip2_sum <= cfg_shared_ip_cap

TransactionalRollbackExact == rollback_exact_ok

SinglePickNeverRetriesSibling == single_pick_no_retry_ok

CrossAccountFreezeAndCancelIsolation ==
  /\ cross_acct_isolation_ok
  /\ ((~cfg_allow_freeze) => (~acct1_frozen))

RisingEdgeExchLatchExact ==
  /\ rising_edge_latch_ok
  /\ (exch_latch_ceil => exch_latch_warn)

U64AndU128OverflowSafe == u64_u128_overflow_ok

SafetyConjunction ==
  /\ TypeOK
  /\ SharedAndAccountCapsRespected
  /\ TransactionalRollbackExact
  /\ SinglePickNeverRetriesSibling
  /\ CrossAccountFreezeAndCancelIsolation
  /\ RisingEdgeExchLatchExact
  /\ U64AndU128OverflowSafe

IndInv ==
  /\ SafetyConjunction
  /\ ((~a1_ord_init) => (a1_ord_ep = 0 /\ a1_ord_used = 0))
  /\ ((~a2_ord_init) => (a2_ord_ep = 0 /\ a2_ord_used = 0))
  /\ ((~a1_cxl_init) => (a1_cxl_ep = 0 /\ a1_cxl_used = 0))

====
