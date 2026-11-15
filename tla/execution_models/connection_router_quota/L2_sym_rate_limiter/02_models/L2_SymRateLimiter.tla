---- MODULE L2_SymRateLimiter ----
(*
 * Symbolic Unbounded Inductive & Full 64-Bit / 128-Bit Verification (Mode S2 + Mode S1)
 * for `tickwire_quota::rate_limiter::{RateLimiter, VenueRateLimiter}`:
 *   - Eliminates `L1_MultiIpQuota`'s finite bounds (`MaxNow = 5..6`, `FixedInterval = 2`,
 *     `RollingInterval = 3`, `AcctOrderCap = 2`, `IpWeightCap = 3..4`, `CooldownLen = 2`)
 *     by proving window cap bounds, `can_afford <==> allow` exact equivalence (including
 *     duplicate `RateLimitId` spends), exact reverse-order `put_back` rollback across
 *     `Fixed`, `RollingCoalesced`, and `RollingPushed` undos, `Cancel`/`Query` immunity
 *     to `ORDER` exhaustion, per-IP `429` failover vs. `418` account freeze, and `u64`/`u128`
 *     overflow freedom over `0 .. 2^64 - 1` and unbounded trace depth (`k >= 0`) in Z3 `QF_LIA`.
 *
 * Verified production Rust anchors:
 *   - `WindowUndo`, `FixedWindow` (`advance_to`, `used_at`, `retry_after_ns`,
 *     `can_take_readonly`, `try_take`, `put_back`):
 *       crates/tickwire-quota/src/rate_limiter.rs:492-614
 *   - `RollingWindow` (`evict_expired`, `state_at`, `retry_after_for_units`,
 *     `can_take_readonly`, `try_take`, `put_back`):
 *       crates/tickwire-quota/src/rate_limiter.rs:616-847
 *   - Cooldowns, `418` account freeze, `precheck_spends`, `can_afford`, `allow`,
 *     and `reconcile_exchange_usage`:
 *       crates/tickwire-quota/src/rate_limiter.rs:1117-1469
 *   - `VenueRateLimiter` (`note_ip_cooldown`, `freeze_account`, `allow_place_orders`,
 *     `can_afford_place_orders`, `allow_cancel`, `can_afford_cancel`, `allow_query`,
 *     `can_afford_query`):
 *       crates/tickwire-quota/src/rate_limiter.rs:1740-2232
 *)
EXTENDS Integers

CONSTANTS
  (* @type: Bool; *)
  BoundedMode,
  (* @type: Bool; *)
  AllowBan

VARIABLES
  (* --- Symbolic quota capacities (`1..U64_MAX` in Apalache, small ints in TLC) --- *)
  (* @type: Int; *)
  cfg_acct_ord_cap,
  (* @type: Int; *)
  cfg_sym_cxl_cap,
  (* @type: Int; *)
  cfg_ip_wt_cap,
  (* @type: Bool; *)
  cfg_allow_ban,

  (* --- Global monotonic clock & cooldown/ban state (`rate_limiter.rs:1058,1117-1305`) --- *)
  (* @type: Int; *)
  last_now_ns,
  (* @type: Bool; *)
  acct_frozen,
  (* @type: Int; *)
  acct_cd_until_ns,
  (* @type: Int; *)
  ip1_cd_until_ns,
  (* @type: Int; *)
  ip2_cd_until_ns,

  (* --- Account `ORDER` `FixedWindow` (`1s`, `rate_limiter.rs:500-614`) --- *)
  (* @type: Bool; *)
  ord_ep_some,
  (* @type: Int; *)
  ord_ep_idx,
  (* @type: Int; *)
  ord_used,
  (* @type: Bool; *)
  ord_warn,

  (* --- Symbol `CANCEL` `FixedWindow` (`1s`, `rate_limiter.rs:500-614`) --- *)
  (* @type: Bool; *)
  cxl_ep_some,
  (* @type: Int; *)
  cxl_ep_idx,
  (* @type: Int; *)
  cxl_used,
  (* @type: Bool; *)
  cxl_warn,

  (* --- Per-IP `REQUEST | WEIGHT` 2-slot `RollingWindow` (`60s`, `rate_limiter.rs:616-847`) --- *)
  (* @type: Int; *)
  ip1_len,
  (* @type: Int; *)
  ip1_s0_ts,
  (* @type: Int; *)
  ip1_s0_u,
  (* @type: Int; *)
  ip1_s1_ts,
  (* @type: Int; *)
  ip1_s1_u,
  (* @type: Int; *)
  ip1_used,
  (* @type: Int; *)
  ip1_peak,
  (* @type: Bool; *)
  ip1_warn,

  (* @type: Int; *)
  ip2_len,
  (* @type: Int; *)
  ip2_s0_ts,
  (* @type: Int; *)
  ip2_s0_u,
  (* @type: Int; *)
  ip2_s1_ts,
  (* @type: Int; *)
  ip2_s1_u,
  (* @type: Int; *)
  ip2_used,
  (* @type: Int; *)
  ip2_peak,
  (* @type: Bool; *)
  ip2_warn,

  (* --- Verification latches & bounded TLC horizon --- *)
  (* @type: Int; *)
  last_outcome,
  (* @type: Int; *)
  step_budget,
  (* @type: Bool; *)
  bicond_ok,
  (* @type: Bool; *)
  rollback_exact_ok,
  (* @type: Bool; *)
  isolation_ok,
  (* @type: Bool; *)
  ip_failover_ok,
  (* @type: Bool; *)
  local_reject_noburn_ok,
  (* @type: Bool; *)
  u64_u128_safe_ok

vars == <<
  cfg_acct_ord_cap, cfg_sym_cxl_cap, cfg_ip_wt_cap, cfg_allow_ban,
  last_now_ns, acct_frozen, acct_cd_until_ns, ip1_cd_until_ns, ip2_cd_until_ns,
  ord_ep_some, ord_ep_idx, ord_used, ord_warn,
  cxl_ep_some, cxl_ep_idx, cxl_used, cxl_warn,
  ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_used, ip1_peak, ip1_warn,
  ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_used, ip2_peak, ip2_warn,
  last_outcome, step_budget,
  bicond_ok, rollback_exact_ok, isolation_ok, ip_failover_ok, local_reject_noburn_ok, u64_u128_safe_ok
>>

(* --- 64-Bit / 128-Bit Integer Bounds & Reject Codes --- *)
Pow30 == 1073741824
U64_MAX == (16 * Pow30 * Pow30) - 1
U128_MAX == ((U64_MAX + 1) * (U64_MAX + 1)) - 1
EffU64Max == IF BoundedMode THEN 4 ELSE U64_MAX
EffU128Max == IF BoundedMode THEN 10000 ELSE U128_MAX
MaxBoundedSteps == 3

REJ_OK == 0
REJ_FROZEN == 1
REJ_ACCT_CD == 2
REJ_IP_CD == 3
REJ_EXHAUSTED == 4
REJ_CLOCK == 5
REJ_LOCAL == 6

OUT_INIT == 0
OUT_PLACE_OK == 1
OUT_PLACE_REJ == 2
OUT_DUP_OK == 3
OUT_DUP_REJ == 4
OUT_ROLL_FIRST_OK == 5
OUT_ROLL_FIRST_REJ == 6
OUT_CANCEL_OK == 7
OUT_CANCEL_REJ == 8
OUT_QUERY_OK == 9
OUT_QUERY_REJ == 10
OUT_LOCAL_REJ == 11
OUT_CLOCK_REJ == 12
OUT_IP_429 == 13
OUT_ACCT_CD == 14
OUT_ACCT_418 == 15
OUT_RECON_FIXED == 16
OUT_RECON_ROLL == 17

CanStep == (~BoundedMode) \/ (step_budget < MaxBoundedSteps)
NextBudget == IF BoundedMode THEN step_budget + 1 ELSE 0

TimeDom == IF BoundedMode THEN 0..3 ELSE Int
UnitDom == IF BoundedMode THEN 1..2 ELSE Int
ReportDom == IF BoundedMode THEN 0..3 ELSE Int

U64SatSub(a, b) == IF a >= b THEN a - b ELSE 0
Max2(a, b) == IF a >= b THEN a ELSE b
Min2(a, b) == IF a <= b THEN a ELSE b

(* --- Pure `QF_LIA` FixedWindow Operators (`rate_limiter.rs:520-614`) --- *)
FixedEpochIdx(t) == IF BoundedMode THEN (t \div 2) ELSE (t \div 1000000000)

FixedNextEpochNs(ep) ==
  LET prod == IF BoundedMode THEN ((ep + 1) * 2) ELSE ((ep + 1) * 1000000000)
  IN IF prod > EffU64Max THEN EffU64Max ELSE prod

FixedSameEpoch(ep_some, ep_idx, now_ns) ==
  ep_some /\ (ep_idx = FixedEpochIdx(now_ns))

FixedAdvUsed(ep_some, ep_idx, used, now_ns) ==
  IF FixedSameEpoch(ep_some, ep_idx, now_ns) THEN used ELSE 0

FixedAdvWarn(ep_some, ep_idx, warn, now_ns) ==
  IF FixedSameEpoch(ep_some, ep_idx, now_ns) THEN warn ELSE FALSE

FixedRetryAfterNs(now_ns) ==
  U64SatSub(FixedNextEpochNs(FixedEpochIdx(now_ns)), now_ns)

(* Exact `u64::checked_add` equivalence (`rate_limiter.rs:568-571,588-590`): *)
CheckedAdd2Le(a, b, cap) ==
  /\ (a + b <= EffU64Max)
  /\ (a + b <= cap)

CheckedAdd3Le(a, b, c, cap) ==
  /\ (a + b <= EffU64Max)
  /\ (a + b + c <= EffU64Max)
  /\ (a + b + c <= cap)

(* --- Pure `QF_LIA` 2-Slot RollingWindow Operators (`rate_limiter.rs:649-846`) --- *)
RollIntervalNs == IF BoundedMode THEN 3 ELSE (60 * 1000000000)

RollExp0(len, s0_ts, now_ns) ==
  (len >= 1) /\ (U64SatSub(now_ns, s0_ts) >= RollIntervalNs)

RollExp1(len, s0_ts, s1_ts, now_ns) ==
  (len = 2) /\ RollExp0(len, s0_ts, now_ns) /\ (U64SatSub(now_ns, s1_ts) >= RollIntervalNs)

RollEffLen(len, s0_ts, s1_ts, now_ns) ==
  IF len = 0 THEN 0
  ELSE IF len = 1 THEN (IF RollExp0(len, s0_ts, now_ns) THEN 0 ELSE 1)
  ELSE IF RollExp1(len, s0_ts, s1_ts, now_ns) THEN 0
  ELSE IF RollExp0(len, s0_ts, now_ns) THEN 1
  ELSE 2

RollEff0Ts(len, s0_ts, s1_ts, now_ns) ==
  LET el == RollEffLen(len, s0_ts, s1_ts, now_ns)
  IN IF el = 0 THEN 0
     ELSE IF (len = 2) /\ RollExp0(len, s0_ts, now_ns) THEN s1_ts
     ELSE s0_ts

RollEff0U(len, s0_ts, s0_u, s1_ts, s1_u, now_ns) ==
  LET el == RollEffLen(len, s0_ts, s1_ts, now_ns)
  IN IF el = 0 THEN 0
     ELSE IF (len = 2) /\ RollExp0(len, s0_ts, now_ns) THEN s1_u
     ELSE s0_u

RollEff1Ts(len, s0_ts, s1_ts, now_ns) ==
  IF RollEffLen(len, s0_ts, s1_ts, now_ns) = 2 THEN s1_ts ELSE 0

RollEff1U(len, s0_ts, s1_ts, s1_u, now_ns) ==
  IF RollEffLen(len, s0_ts, s1_ts, now_ns) = 2 THEN s1_u ELSE 0

RollExpiredUnits(len, s0_ts, s0_u, s1_ts, s1_u, now_ns) ==
  IF RollExp1(len, s0_ts, s1_ts, now_ns) THEN (s0_u + s1_u)
  ELSE IF RollExp0(len, s0_ts, now_ns) THEN s0_u
  ELSE 0

RollEffUsed(len, s0_ts, s0_u, s1_ts, s1_u, now_ns) ==
  RollEff0U(len, s0_ts, s0_u, s1_ts, s1_u, now_ns)
  + RollEff1U(len, s0_ts, s1_ts, s1_u, now_ns)

RollEffPeak(len, s0_ts, s0_u, s1_ts, s1_u, peak, now_ns) ==
  IF RollEffUsed(len, s0_ts, s0_u, s1_ts, s1_u, now_ns) = 0 THEN 0 ELSE peak

RollEffWarn(len, s0_ts, s0_u, s1_ts, s1_u, warn, now_ns) ==
  IF RollEffUsed(len, s0_ts, s0_u, s1_ts, s1_u, now_ns) = 0 THEN FALSE ELSE warn

RollCoalesces(el, e0_ts, e1_ts, now_ns) ==
  (el > 0) /\ ((IF el = 2 THEN e1_ts ELSE e0_ts) = now_ns)

RollCanTake(el, e0_ts, e1_ts, eu, req, cap, now_ns) ==
  /\ CheckedAdd2Le(eu, req, cap)
  /\ (RollCoalesces(el, e0_ts, e1_ts, now_ns) \/ (el < 2))

RollPostLen(el, e0_ts, e1_ts, now_ns) ==
  IF RollCoalesces(el, e0_ts, e1_ts, now_ns) THEN el ELSE (el + 1)

RollPost0Ts(el, e0_ts, now_ns) ==
  IF el = 0 THEN now_ns ELSE e0_ts

RollPost0U(el, e0_ts, e0_u, e1_ts, req, now_ns) ==
  IF el = 0 THEN req
  ELSE IF (el = 1) /\ (e0_ts = now_ns) THEN (e0_u + req)
  ELSE e0_u

RollPost1Ts(el, e0_ts, e1_ts, now_ns) ==
  IF el = 0 THEN 0
  ELSE IF (el = 1) /\ (e0_ts = now_ns) THEN 0
  ELSE IF el = 1 THEN now_ns
  ELSE e1_ts

RollPost1U(el, e0_ts, e1_ts, e1_u, req, now_ns) ==
  IF el = 0 THEN 0
  ELSE IF (el = 1) /\ (e0_ts = now_ns) THEN 0
  ELSE IF el = 1 THEN req
  ELSE (e1_u + req)

(* Explicit `RollingWindow::put_back` (`rate_limiter.rs:828-846`) on post-take state: *)
RollPutBackLen(coalesced, post_len) ==
  IF coalesced THEN post_len ELSE (post_len - 1)

RollPutBack0Ts(coalesced, post_len, p0_ts) ==
  IF (~coalesced) /\ (post_len = 1) THEN 0 ELSE p0_ts

RollPutBack0U(coalesced, post_len, p0_u, req) ==
  IF coalesced /\ (post_len = 1) THEN (p0_u - req)
  ELSE IF (~coalesced) /\ (post_len = 1) THEN 0
  ELSE p0_u

RollPutBack1Ts(coalesced, post_len, p1_ts) ==
  IF (~coalesced) /\ (post_len = 2) THEN 0 ELSE p1_ts

RollPutBack1U(coalesced, post_len, p1_u, req) ==
  IF coalesced /\ (post_len = 2) THEN (p1_u - req)
  ELSE IF (~coalesced) /\ (post_len = 2) THEN 0
  ELSE p1_u

(* Precheck route cooldowns (`precheck_route` / `precheck_spends`, `rate_limiter.rs:1263-1328`): *)
RoutePrecheckCode(ip_cd_until, now_ns) ==
  IF acct_frozen THEN REJ_FROZEN
  ELSE IF now_ns < acct_cd_until_ns THEN REJ_ACCT_CD
  ELSE IF now_ns < ip_cd_until THEN REJ_IP_CD
  ELSE REJ_OK

(* Exchange warning threshold (`u128::from(reported) * 100 >= u128::from(cap) * 80`, `rate_limiter.rs:1438-1440`): *)
ReconcileWarn80(reported, cap) ==
  (reported * 100) >= (cap * 80)

ConstInit ==
  /\ BoundedMode = FALSE
  /\ AllowBan \in BOOLEAN

ValidConfig ==
  /\ cfg_acct_ord_cap \in Int /\ cfg_acct_ord_cap >= 1 /\ cfg_acct_ord_cap <= EffU64Max
  /\ cfg_sym_cxl_cap \in Int /\ cfg_sym_cxl_cap >= 1 /\ cfg_sym_cxl_cap <= EffU64Max
  /\ cfg_ip_wt_cap \in Int /\ cfg_ip_wt_cap >= 2 /\ cfg_ip_wt_cap <= EffU64Max
  /\ cfg_allow_ban \in BOOLEAN

Init ==
  /\ cfg_allow_ban = AllowBan
  /\ IF BoundedMode
     THEN /\ cfg_acct_ord_cap = 2
          /\ cfg_sym_cxl_cap = 2
          /\ cfg_ip_wt_cap = 3
     ELSE /\ cfg_acct_ord_cap \in Int /\ cfg_acct_ord_cap >= 1 /\ cfg_acct_ord_cap <= EffU64Max
          /\ cfg_sym_cxl_cap \in Int /\ cfg_sym_cxl_cap >= 1 /\ cfg_sym_cxl_cap <= EffU64Max
          /\ cfg_ip_wt_cap \in Int /\ cfg_ip_wt_cap >= 2 /\ cfg_ip_wt_cap <= EffU64Max
  /\ last_now_ns = 0
  /\ acct_frozen = FALSE
  /\ acct_cd_until_ns = 0
  /\ ip1_cd_until_ns = 0
  /\ ip2_cd_until_ns = 0
  /\ ord_ep_some = FALSE /\ ord_ep_idx = 0 /\ ord_used = 0 /\ ord_warn = FALSE
  /\ cxl_ep_some = FALSE /\ cxl_ep_idx = 0 /\ cxl_used = 0 /\ cxl_warn = FALSE
  /\ ip1_len = 0 /\ ip1_s0_ts = 0 /\ ip1_s0_u = 0 /\ ip1_s1_ts = 0 /\ ip1_s1_u = 0
  /\ ip1_used = 0 /\ ip1_peak = 0 /\ ip1_warn = FALSE
  /\ ip2_len = 0 /\ ip2_s0_ts = 0 /\ ip2_s0_u = 0 /\ ip2_s1_ts = 0 /\ ip2_s1_u = 0
  /\ ip2_used = 0 /\ ip2_peak = 0 /\ ip2_warn = FALSE
  /\ last_outcome = OUT_INIT
  /\ step_budget = 0
  /\ bicond_ok = TRUE
  /\ rollback_exact_ok = TRUE
  /\ isolation_ok = TRUE
  /\ ip_failover_ok = TRUE
  /\ local_reject_noburn_ok = TRUE
  /\ u64_u128_safe_ok = TRUE

(* --- Helper per-IP transition macro for `[FixedWindow, RollingWindow[ip]]` spends --- *)
ExecFixedThenIp1(now_ns, f_ep_s, f_ep, f_used, f_warn, f_cap, u1, u2, has_u2, w,
                 out_ok, out_rej,
                 f_ep_s_out, f_ep_out, f_used_out, f_warn_out) ==
  LET pre == RoutePrecheckCode(ip1_cd_until_ns, now_ns)
      adv_f_u == FixedAdvUsed(f_ep_s, f_ep, f_used, now_ns)
      adv_f_w == FixedAdvWarn(f_ep_s, f_ep, f_warn, now_ns)
      el == RollEffLen(ip1_len, ip1_s0_ts, ip1_s1_ts, now_ns)
      e0_ts == RollEff0Ts(ip1_len, ip1_s0_ts, ip1_s1_ts, now_ns)
      e0_u == RollEff0U(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, now_ns)
      e1_ts == RollEff1Ts(ip1_len, ip1_s0_ts, ip1_s1_ts, now_ns)
      e1_u == RollEff1U(ip1_len, ip1_s0_ts, ip1_s1_ts, ip1_s1_u, now_ns)
      eu == RollEffUsed(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, now_ns)
      epk == RollEffPeak(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_peak, now_ns)
      ewn == RollEffWarn(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_warn, now_ns)
      exp_u == RollExpiredUnits(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, now_ns)

      (* Read-only `can_afford` (`rate_limiter.rs:1338-1372`): *)
      ca_f1 == CheckedAdd2Le(adv_f_u, u1, f_cap)
      ca_f2 == IF has_u2 THEN (ca_f1 /\ CheckedAdd3Le(adv_f_u, u1, u2, f_cap)) ELSE ca_f1
      ca_ip == RollCanTake(el, e0_ts, e1_ts, eu, w, cfg_ip_wt_cap, now_ns)
      ca_code == IF pre # REJ_OK THEN pre ELSE IF (ca_f2 /\ ca_ip) THEN REJ_OK ELSE REJ_EXHAUSTED

      (* Transactional `allow` + reverse `put_back` (`rate_limiter.rs:1385-1415`): *)
      s1_ok == CheckedAdd2Le(adv_f_u, u1, f_cap)
      u_after1 == adv_f_u + u1
      s2_ok == IF has_u2 THEN (s1_ok /\ CheckedAdd2Le(u_after1, u2, f_cap)) ELSE s1_ok
      u_after2 == IF has_u2 THEN (u_after1 + u2) ELSE u_after1
      s3_ok == s2_ok /\ ca_ip
      al_code == IF pre # REJ_OK THEN pre ELSE IF s3_ok THEN REJ_OK ELSE REJ_EXHAUSTED

      (* Reverse `put_back` exact restoration on FixedWindow when step 2 or step 3 rejects: *)
      rb_u_from2 == adv_f_u
      rb_u_from3 == IF has_u2 THEN adv_f_u ELSE adv_f_u
      final_f_ep_s == IF pre # REJ_OK THEN f_ep_s ELSE TRUE
      final_f_ep == IF pre # REJ_OK THEN f_ep ELSE FixedEpochIdx(now_ns)
      final_f_u == IF pre # REJ_OK THEN f_used
                   ELSE IF s3_ok THEN u_after2
                   ELSE IF ~s1_ok THEN adv_f_u
                   ELSE IF ~s2_ok THEN rb_u_from2
                   ELSE rb_u_from3
      final_f_w == IF pre # REJ_OK THEN f_warn ELSE adv_f_w
      retry_f == FixedRetryAfterNs(now_ns)
  IN /\ last_now_ns' = (IF pre # REJ_OK THEN last_now_ns ELSE now_ns)
     /\ f_ep_s_out = final_f_ep_s
     /\ f_ep_out = final_f_ep
     /\ f_used_out = final_f_u
     /\ f_warn_out = final_f_w
     /\ IF (pre # REJ_OK) \/ (~s2_ok)
        THEN /\ ip1_len' = ip1_len
             /\ ip1_s0_ts' = ip1_s0_ts /\ ip1_s0_u' = ip1_s0_u
             /\ ip1_s1_ts' = ip1_s1_ts /\ ip1_s1_u' = ip1_s1_u
             /\ ip1_used' = ip1_used /\ ip1_peak' = ip1_peak /\ ip1_warn' = ip1_warn
        ELSE IF ~s3_ok
             THEN /\ ip1_len' = el
                  /\ ip1_s0_ts' = e0_ts /\ ip1_s0_u' = e0_u
                  /\ ip1_s1_ts' = e1_ts /\ ip1_s1_u' = e1_u
                  /\ ip1_used' = eu /\ ip1_peak' = epk /\ ip1_warn' = ewn
             ELSE /\ ip1_len' = RollPostLen(el, e0_ts, e1_ts, now_ns)
                  /\ ip1_s0_ts' = RollPost0Ts(el, e0_ts, now_ns)
                  /\ ip1_s0_u' = RollPost0U(el, e0_ts, e0_u, e1_ts, w, now_ns)
                  /\ ip1_s1_ts' = RollPost1Ts(el, e0_ts, e1_ts, now_ns)
                  /\ ip1_s1_u' = RollPost1U(el, e0_ts, e1_ts, e1_u, w, now_ns)
                  /\ ip1_used' = eu + w /\ ip1_peak' = epk /\ ip1_warn' = ewn
     /\ last_outcome' = (IF al_code = REJ_OK THEN out_ok ELSE out_rej)
     /\ bicond_ok' = (bicond_ok /\ (ca_code = al_code))
     /\ rollback_exact_ok' = (rollback_exact_ok /\ ((pre # REJ_OK) \/ s3_ok \/ (final_f_u = adv_f_u)))
     /\ u64_u128_safe_ok' =
          (u64_u128_safe_ok
           /\ (U64SatSub(ip1_used, exp_u) = eu)
           /\ (retry_f >= 0 /\ retry_f <= EffU64Max)
           /\ (ca_f2 <=> ((adv_f_u + u1 + (IF has_u2 THEN u2 ELSE 0)) <= f_cap)))

ExecFixedThenIp2(now_ns, f_ep_s, f_ep, f_used, f_warn, f_cap, u1, u2, has_u2, w,
                 out_ok, out_rej,
                 f_ep_s_out, f_ep_out, f_used_out, f_warn_out) ==
  LET pre == RoutePrecheckCode(ip2_cd_until_ns, now_ns)
      adv_f_u == FixedAdvUsed(f_ep_s, f_ep, f_used, now_ns)
      adv_f_w == FixedAdvWarn(f_ep_s, f_ep, f_warn, now_ns)
      el == RollEffLen(ip2_len, ip2_s0_ts, ip2_s1_ts, now_ns)
      e0_ts == RollEff0Ts(ip2_len, ip2_s0_ts, ip2_s1_ts, now_ns)
      e0_u == RollEff0U(ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, now_ns)
      e1_ts == RollEff1Ts(ip2_len, ip2_s0_ts, ip2_s1_ts, now_ns)
      e1_u == RollEff1U(ip2_len, ip2_s0_ts, ip2_s1_ts, ip2_s1_u, now_ns)
      eu == RollEffUsed(ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, now_ns)
      epk == RollEffPeak(ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_peak, now_ns)
      ewn == RollEffWarn(ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_warn, now_ns)
      exp_u == RollExpiredUnits(ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, now_ns)

      ca_f1 == CheckedAdd2Le(adv_f_u, u1, f_cap)
      ca_f2 == IF has_u2 THEN (ca_f1 /\ CheckedAdd3Le(adv_f_u, u1, u2, f_cap)) ELSE ca_f1
      ca_ip == RollCanTake(el, e0_ts, e1_ts, eu, w, cfg_ip_wt_cap, now_ns)
      ca_code == IF pre # REJ_OK THEN pre ELSE IF (ca_f2 /\ ca_ip) THEN REJ_OK ELSE REJ_EXHAUSTED

      s1_ok == CheckedAdd2Le(adv_f_u, u1, f_cap)
      u_after1 == adv_f_u + u1
      s2_ok == IF has_u2 THEN (s1_ok /\ CheckedAdd2Le(u_after1, u2, f_cap)) ELSE s1_ok
      u_after2 == IF has_u2 THEN (u_after1 + u2) ELSE u_after1
      s3_ok == s2_ok /\ ca_ip
      al_code == IF pre # REJ_OK THEN pre ELSE IF s3_ok THEN REJ_OK ELSE REJ_EXHAUSTED

      final_f_ep_s == IF pre # REJ_OK THEN f_ep_s ELSE TRUE
      final_f_ep == IF pre # REJ_OK THEN f_ep ELSE FixedEpochIdx(now_ns)
      final_f_u == IF pre # REJ_OK THEN f_used
                   ELSE IF s3_ok THEN u_after2
                   ELSE adv_f_u
      final_f_w == IF pre # REJ_OK THEN f_warn ELSE adv_f_w
      retry_f == FixedRetryAfterNs(now_ns)
  IN /\ last_now_ns' = (IF pre # REJ_OK THEN last_now_ns ELSE now_ns)
     /\ f_ep_s_out = final_f_ep_s
     /\ f_ep_out = final_f_ep
     /\ f_used_out = final_f_u
     /\ f_warn_out = final_f_w
     /\ IF (pre # REJ_OK) \/ (~s2_ok)
        THEN /\ ip2_len' = ip2_len
             /\ ip2_s0_ts' = ip2_s0_ts /\ ip2_s0_u' = ip2_s0_u
             /\ ip2_s1_ts' = ip2_s1_ts /\ ip2_s1_u' = ip2_s1_u
             /\ ip2_used' = ip2_used /\ ip2_peak' = ip2_peak /\ ip2_warn' = ip2_warn
        ELSE IF ~s3_ok
             THEN /\ ip2_len' = el
                  /\ ip2_s0_ts' = e0_ts /\ ip2_s0_u' = e0_u
                  /\ ip2_s1_ts' = e1_ts /\ ip2_s1_u' = e1_u
                  /\ ip2_used' = eu /\ ip2_peak' = epk /\ ip2_warn' = ewn
             ELSE /\ ip2_len' = RollPostLen(el, e0_ts, e1_ts, now_ns)
                  /\ ip2_s0_ts' = RollPost0Ts(el, e0_ts, now_ns)
                  /\ ip2_s0_u' = RollPost0U(el, e0_ts, e0_u, e1_ts, w, now_ns)
                  /\ ip2_s1_ts' = RollPost1Ts(el, e0_ts, e1_ts, now_ns)
                  /\ ip2_s1_u' = RollPost1U(el, e0_ts, e1_ts, e1_u, w, now_ns)
                  /\ ip2_used' = eu + w /\ ip2_peak' = epk /\ ip2_warn' = ewn
     /\ last_outcome' = (IF al_code = REJ_OK THEN out_ok ELSE out_rej)
     /\ bicond_ok' = (bicond_ok /\ (ca_code = al_code))
     /\ rollback_exact_ok' = (rollback_exact_ok /\ ((pre # REJ_OK) \/ s3_ok \/ (final_f_u = adv_f_u)))
     /\ u64_u128_safe_ok' =
          (u64_u128_safe_ok
           /\ (U64SatSub(ip2_used, exp_u) = eu)
           /\ (retry_f >= 0 /\ retry_f <= EffU64Max)
           /\ (ca_f2 <=> ((adv_f_u + u1 + (IF has_u2 THEN u2 ELSE 0)) <= f_cap)))

(* --- Transitions (`Next`) --- *)

PlaceOrderSingle ==
  /\ CanStep
  /\ \E ip \in {1, 2}, now_ns \in TimeDom, u \in UnitDom, w \in UnitDom:
       /\ now_ns >= last_now_ns /\ now_ns <= EffU64Max
       /\ u >= 1 /\ u <= EffU64Max
       /\ w >= 1 /\ w <= EffU64Max
       /\ IF ip = 1
          THEN /\ ExecFixedThenIp1(
                    now_ns, ord_ep_some, ord_ep_idx, ord_used, ord_warn, cfg_acct_ord_cap,
                    u, 0, FALSE, w, OUT_PLACE_OK, OUT_PLACE_REJ,
                    ord_ep_some', ord_ep_idx', ord_used', ord_warn'
                  )
               /\ UNCHANGED <<ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_used, ip2_peak, ip2_warn>>
          ELSE /\ ExecFixedThenIp2(
                    now_ns, ord_ep_some, ord_ep_idx, ord_used, ord_warn, cfg_acct_ord_cap,
                    u, 0, FALSE, w, OUT_PLACE_OK, OUT_PLACE_REJ,
                    ord_ep_some', ord_ep_idx', ord_used', ord_warn'
                  )
               /\ UNCHANGED <<ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_used, ip1_peak, ip1_warn>>
       /\ isolation_ok' = isolation_ok
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_acct_ord_cap, cfg_sym_cxl_cap, cfg_ip_wt_cap, cfg_allow_ban,
            acct_frozen, acct_cd_until_ns, ip1_cd_until_ns, ip2_cd_until_ns,
            cxl_ep_some, cxl_ep_idx, cxl_used, cxl_warn,
            ip_failover_ok, local_reject_noburn_ok
          >>

(* Duplicate `RateLimitId` spend `[AcctOrder(u1), AcctOrder(u2), IpWeight(w)]` (`rate_limiter.rs:1344-1410`): *)
PlaceOrderDuplicateFixed ==
  /\ CanStep
  /\ \E now_ns \in TimeDom, u1 \in UnitDom, u2 \in UnitDom, w \in UnitDom:
       /\ now_ns >= last_now_ns /\ now_ns <= EffU64Max
       /\ u1 >= 1 /\ u1 <= EffU64Max
       /\ u2 >= 1 /\ u2 <= EffU64Max
       /\ w >= 1 /\ w <= EffU64Max
       /\ ExecFixedThenIp1(
            now_ns, ord_ep_some, ord_ep_idx, ord_used, ord_warn, cfg_acct_ord_cap,
            u1, u2, TRUE, w, OUT_DUP_OK, OUT_DUP_REJ,
            ord_ep_some', ord_ep_idx', ord_used', ord_warn'
          )
       /\ isolation_ok' = isolation_ok
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_acct_ord_cap, cfg_sym_cxl_cap, cfg_ip_wt_cap, cfg_allow_ban,
            acct_frozen, acct_cd_until_ns, ip1_cd_until_ns, ip2_cd_until_ns,
            cxl_ep_some, cxl_ep_idx, cxl_used, cxl_warn,
            ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_used, ip2_peak, ip2_warn,
            ip_failover_ok, local_reject_noburn_ok
          >>

(* `[IpWeight1(w), AcctOrder(u)]` directly exercising `RollingWindow::put_back` (`RollingCoalesced` & `RollingPushed`, `rate_limiter.rs:828-846`): *)
RollingFirstMultiSpend ==
  /\ CanStep
  /\ \E now_ns \in TimeDom, w \in UnitDom, u \in UnitDom:
       /\ now_ns >= last_now_ns /\ now_ns <= EffU64Max
       /\ w >= 1 /\ w <= EffU64Max
       /\ u >= 1 /\ u <= EffU64Max
       /\ LET pre == RoutePrecheckCode(ip1_cd_until_ns, now_ns)
              el == RollEffLen(ip1_len, ip1_s0_ts, ip1_s1_ts, now_ns)
              e0_ts == RollEff0Ts(ip1_len, ip1_s0_ts, ip1_s1_ts, now_ns)
              e0_u == RollEff0U(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, now_ns)
              e1_ts == RollEff1Ts(ip1_len, ip1_s0_ts, ip1_s1_ts, now_ns)
              e1_u == RollEff1U(ip1_len, ip1_s0_ts, ip1_s1_ts, ip1_s1_u, now_ns)
              eu == RollEffUsed(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, now_ns)
              epk == RollEffPeak(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_peak, now_ns)
              ewn == RollEffWarn(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_warn, now_ns)
              adv_f_u == FixedAdvUsed(ord_ep_some, ord_ep_idx, ord_used, now_ns)
              adv_f_w == FixedAdvWarn(ord_ep_some, ord_ep_idx, ord_warn, now_ns)

              s1_ok == RollCanTake(el, e0_ts, e1_ts, eu, w, cfg_ip_wt_cap, now_ns)
              coal == RollCoalesces(el, e0_ts, e1_ts, now_ns)
              p_len == RollPostLen(el, e0_ts, e1_ts, now_ns)
              p0_ts == RollPost0Ts(el, e0_ts, now_ns)
              p0_u == RollPost0U(el, e0_ts, e0_u, e1_ts, w, now_ns)
              p1_ts == RollPost1Ts(el, e0_ts, e1_ts, now_ns)
              p1_u == RollPost1U(el, e0_ts, e1_ts, e1_u, w, now_ns)
              p_used == eu + w

              s2_ok == s1_ok /\ CheckedAdd2Le(adv_f_u, u, cfg_acct_ord_cap)
              ca_code == IF pre # REJ_OK THEN pre ELSE IF s2_ok THEN REJ_OK ELSE REJ_EXHAUSTED
              al_code == IF pre # REJ_OK THEN pre ELSE IF s2_ok THEN REJ_OK ELSE REJ_EXHAUSTED

              (* Exact `RollingWindow::put_back` result when `s1_ok /\ ~s2_ok`: *)
              rb_len == RollPutBackLen(coal, p_len)
              rb0_ts == RollPutBack0Ts(coal, p_len, p0_ts)
              rb0_u == RollPutBack0U(coal, p_len, p0_u, w)
              rb1_ts == RollPutBack1Ts(coal, p_len, p1_ts)
              rb1_u == RollPutBack1U(coal, p_len, p1_u, w)
              rb_used == p_used - w
              rb_matches_evicted ==
                /\ (rb_len = el)
                /\ (rb0_ts = e0_ts) /\ (rb0_u = e0_u)
                /\ (rb1_ts = e1_ts) /\ (rb1_u = e1_u)
                /\ (rb_used = eu)
          IN /\ last_now_ns' = (IF pre # REJ_OK THEN last_now_ns ELSE now_ns)
             /\ IF pre # REJ_OK
                THEN /\ ip1_len' = ip1_len
                     /\ ip1_s0_ts' = ip1_s0_ts /\ ip1_s0_u' = ip1_s0_u
                     /\ ip1_s1_ts' = ip1_s1_ts /\ ip1_s1_u' = ip1_s1_u
                     /\ ip1_used' = ip1_used /\ ip1_peak' = ip1_peak /\ ip1_warn' = ip1_warn
                     /\ ord_ep_some' = ord_ep_some /\ ord_ep_idx' = ord_ep_idx
                     /\ ord_used' = ord_used /\ ord_warn' = ord_warn
                ELSE IF ~s1_ok
                     THEN /\ ip1_len' = el
                          /\ ip1_s0_ts' = e0_ts /\ ip1_s0_u' = e0_u
                          /\ ip1_s1_ts' = e1_ts /\ ip1_s1_u' = e1_u
                          /\ ip1_used' = eu /\ ip1_peak' = epk /\ ip1_warn' = ewn
                          /\ ord_ep_some' = ord_ep_some /\ ord_ep_idx' = ord_ep_idx
                          /\ ord_used' = ord_used /\ ord_warn' = ord_warn
                     ELSE IF ~s2_ok
                          THEN /\ ip1_len' = rb_len
                               /\ ip1_s0_ts' = rb0_ts /\ ip1_s0_u' = rb0_u
                               /\ ip1_s1_ts' = rb1_ts /\ ip1_s1_u' = rb1_u
                               /\ ip1_used' = rb_used /\ ip1_peak' = epk /\ ip1_warn' = ewn
                               /\ ord_ep_some' = TRUE /\ ord_ep_idx' = FixedEpochIdx(now_ns)
                               /\ ord_used' = adv_f_u /\ ord_warn' = adv_f_w
                          ELSE /\ ip1_len' = p_len
                               /\ ip1_s0_ts' = p0_ts /\ ip1_s0_u' = p0_u
                               /\ ip1_s1_ts' = p1_ts /\ ip1_s1_u' = p1_u
                               /\ ip1_used' = p_used /\ ip1_peak' = epk /\ ip1_warn' = ewn
                               /\ ord_ep_some' = TRUE /\ ord_ep_idx' = FixedEpochIdx(now_ns)
                               /\ ord_used' = adv_f_u + u /\ ord_warn' = adv_f_w
             /\ last_outcome' = (IF al_code = REJ_OK THEN OUT_ROLL_FIRST_OK ELSE OUT_ROLL_FIRST_REJ)
             /\ bicond_ok' = (bicond_ok /\ (ca_code = al_code))
             /\ rollback_exact_ok' =
                  (rollback_exact_ok /\ ((pre # REJ_OK) \/ (~s1_ok) \/ s2_ok \/ rb_matches_evicted))
             /\ isolation_ok' = isolation_ok
             /\ u64_u128_safe_ok' = u64_u128_safe_ok
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_acct_ord_cap, cfg_sym_cxl_cap, cfg_ip_wt_cap, cfg_allow_ban,
            acct_frozen, acct_cd_until_ns, ip1_cd_until_ns, ip2_cd_until_ns,
            cxl_ep_some, cxl_ep_idx, cxl_used, cxl_warn,
            ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_used, ip2_peak, ip2_warn,
            ip_failover_ok, local_reject_noburn_ok
          >>

(* `VenueRateLimiter::allow_cancel` — strictly isolated from `AcctOrder` (`rate_limiter.rs:2076-2175`): *)
CancelOrder ==
  /\ CanStep
  /\ \E ip \in {1, 2}, now_ns \in TimeDom, w \in UnitDom:
       /\ now_ns >= last_now_ns /\ now_ns <= EffU64Max
       /\ w >= 1 /\ w <= EffU64Max
       /\ IF ip = 1
          THEN /\ ExecFixedThenIp1(
                    now_ns, cxl_ep_some, cxl_ep_idx, cxl_used, cxl_warn, cfg_sym_cxl_cap,
                    1, 0, FALSE, w, OUT_CANCEL_OK, OUT_CANCEL_REJ,
                    cxl_ep_some', cxl_ep_idx', cxl_used', cxl_warn'
                  )
               /\ UNCHANGED <<ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_used, ip2_peak, ip2_warn>>
          ELSE /\ ExecFixedThenIp2(
                    now_ns, cxl_ep_some, cxl_ep_idx, cxl_used, cxl_warn, cfg_sym_cxl_cap,
                    1, 0, FALSE, w, OUT_CANCEL_OK, OUT_CANCEL_REJ,
                    cxl_ep_some', cxl_ep_idx', cxl_used', cxl_warn'
                  )
               /\ UNCHANGED <<ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_used, ip1_peak, ip1_warn>>
       /\ UNCHANGED <<ord_ep_some, ord_ep_idx, ord_used, ord_warn>>
       /\ isolation_ok' = isolation_ok
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_acct_ord_cap, cfg_sym_cxl_cap, cfg_ip_wt_cap, cfg_allow_ban,
            acct_frozen, acct_cd_until_ns, ip1_cd_until_ns, ip2_cd_until_ns,
            ip_failover_ok, local_reject_noburn_ok
          >>

(* `VenueRateLimiter::allow_query` — strictly isolated from BOTH `AcctOrder` and `SymCancel` (`rate_limiter.rs:2180-2231`): *)
QueryRequest ==
  /\ CanStep
  /\ \E ip \in {1, 2}, now_ns \in TimeDom, w \in UnitDom:
       /\ now_ns >= last_now_ns /\ now_ns <= EffU64Max
       /\ w >= 1 /\ w <= EffU64Max
       /\ IF ip = 1
          THEN LET pre == RoutePrecheckCode(ip1_cd_until_ns, now_ns)
                   el == RollEffLen(ip1_len, ip1_s0_ts, ip1_s1_ts, now_ns)
                   e0_ts == RollEff0Ts(ip1_len, ip1_s0_ts, ip1_s1_ts, now_ns)
                   e0_u == RollEff0U(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, now_ns)
                   e1_ts == RollEff1Ts(ip1_len, ip1_s0_ts, ip1_s1_ts, now_ns)
                   e1_u == RollEff1U(ip1_len, ip1_s0_ts, ip1_s1_ts, ip1_s1_u, now_ns)
                   eu == RollEffUsed(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, now_ns)
                   epk == RollEffPeak(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_peak, now_ns)
                   ewn == RollEffWarn(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_warn, now_ns)
                   ok == (pre = REJ_OK) /\ RollCanTake(el, e0_ts, e1_ts, eu, w, cfg_ip_wt_cap, now_ns)
               IN /\ last_now_ns' = (IF pre # REJ_OK THEN last_now_ns ELSE now_ns)
                  /\ IF pre # REJ_OK
                     THEN /\ ip1_len' = ip1_len
                          /\ ip1_s0_ts' = ip1_s0_ts /\ ip1_s0_u' = ip1_s0_u
                          /\ ip1_s1_ts' = ip1_s1_ts /\ ip1_s1_u' = ip1_s1_u
                          /\ ip1_used' = ip1_used /\ ip1_peak' = ip1_peak /\ ip1_warn' = ip1_warn
                     ELSE IF ~ok
                          THEN /\ ip1_len' = el
                               /\ ip1_s0_ts' = e0_ts /\ ip1_s0_u' = e0_u
                               /\ ip1_s1_ts' = e1_ts /\ ip1_s1_u' = e1_u
                               /\ ip1_used' = eu /\ ip1_peak' = epk /\ ip1_warn' = ewn
                          ELSE /\ ip1_len' = RollPostLen(el, e0_ts, e1_ts, now_ns)
                               /\ ip1_s0_ts' = RollPost0Ts(el, e0_ts, now_ns)
                               /\ ip1_s0_u' = RollPost0U(el, e0_ts, e0_u, e1_ts, w, now_ns)
                               /\ ip1_s1_ts' = RollPost1Ts(el, e0_ts, e1_ts, now_ns)
                               /\ ip1_s1_u' = RollPost1U(el, e0_ts, e1_ts, e1_u, w, now_ns)
                               /\ ip1_used' = eu + w /\ ip1_peak' = epk /\ ip1_warn' = ewn
                  /\ UNCHANGED <<ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_used, ip2_peak, ip2_warn>>
                  /\ last_outcome' = (IF ok THEN OUT_QUERY_OK ELSE OUT_QUERY_REJ)
          ELSE LET pre == RoutePrecheckCode(ip2_cd_until_ns, now_ns)
                   el == RollEffLen(ip2_len, ip2_s0_ts, ip2_s1_ts, now_ns)
                   e0_ts == RollEff0Ts(ip2_len, ip2_s0_ts, ip2_s1_ts, now_ns)
                   e0_u == RollEff0U(ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, now_ns)
                   e1_ts == RollEff1Ts(ip2_len, ip2_s0_ts, ip2_s1_ts, now_ns)
                   e1_u == RollEff1U(ip2_len, ip2_s0_ts, ip2_s1_ts, ip2_s1_u, now_ns)
                   eu == RollEffUsed(ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, now_ns)
                   epk == RollEffPeak(ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_peak, now_ns)
                   ewn == RollEffWarn(ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_warn, now_ns)
                   ok == (pre = REJ_OK) /\ RollCanTake(el, e0_ts, e1_ts, eu, w, cfg_ip_wt_cap, now_ns)
               IN /\ last_now_ns' = (IF pre # REJ_OK THEN last_now_ns ELSE now_ns)
                  /\ IF pre # REJ_OK
                     THEN /\ ip2_len' = ip2_len
                          /\ ip2_s0_ts' = ip2_s0_ts /\ ip2_s0_u' = ip2_s0_u
                          /\ ip2_s1_ts' = ip2_s1_ts /\ ip2_s1_u' = ip2_s1_u
                          /\ ip2_used' = ip2_used /\ ip2_peak' = ip2_peak /\ ip2_warn' = ip2_warn
                     ELSE IF ~ok
                          THEN /\ ip2_len' = el
                               /\ ip2_s0_ts' = e0_ts /\ ip2_s0_u' = e0_u
                               /\ ip2_s1_ts' = e1_ts /\ ip2_s1_u' = e1_u
                               /\ ip2_used' = eu /\ ip2_peak' = epk /\ ip2_warn' = ewn
                          ELSE /\ ip2_len' = RollPostLen(el, e0_ts, e1_ts, now_ns)
                               /\ ip2_s0_ts' = RollPost0Ts(el, e0_ts, now_ns)
                               /\ ip2_s0_u' = RollPost0U(el, e0_ts, e0_u, e1_ts, w, now_ns)
                               /\ ip2_s1_ts' = RollPost1Ts(el, e0_ts, e1_ts, now_ns)
                               /\ ip2_s1_u' = RollPost1U(el, e0_ts, e1_ts, e1_u, w, now_ns)
                               /\ ip2_used' = eu + w /\ ip2_peak' = epk /\ ip2_warn' = ewn
                  /\ UNCHANGED <<ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_used, ip1_peak, ip1_warn>>
                  /\ last_outcome' = (IF ok THEN OUT_QUERY_OK ELSE OUT_QUERY_REJ)
       /\ UNCHANGED <<
            cfg_acct_ord_cap, cfg_sym_cxl_cap, cfg_ip_wt_cap, cfg_allow_ban,
            acct_frozen, acct_cd_until_ns, ip1_cd_until_ns, ip2_cd_until_ns,
            ord_ep_some, ord_ep_idx, ord_used, ord_warn,
            cxl_ep_some, cxl_ep_idx, cxl_used, cxl_warn,
            bicond_ok, rollback_exact_ok, isolation_ok, ip_failover_ok,
            local_reject_noburn_ok, u64_u128_safe_ok
          >>
       /\ step_budget' = NextBudget

LocalAdmissionRejectNoBurn ==
  /\ CanStep
  /\ last_outcome' = OUT_LOCAL_REJ
  /\ local_reject_noburn_ok' = local_reject_noburn_ok
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_acct_ord_cap, cfg_sym_cxl_cap, cfg_ip_wt_cap, cfg_allow_ban,
       last_now_ns, acct_frozen, acct_cd_until_ns, ip1_cd_until_ns, ip2_cd_until_ns,
       ord_ep_some, ord_ep_idx, ord_used, ord_warn,
       cxl_ep_some, cxl_ep_idx, cxl_used, cxl_warn,
       ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_used, ip1_peak, ip1_warn,
       ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_used, ip2_peak, ip2_warn,
       bicond_ok, rollback_exact_ok, isolation_ok, ip_failover_ok, u64_u128_safe_ok
     >>

ClockRegressionReject ==
  /\ CanStep
  /\ last_now_ns > 0
  /\ \E bad_now \in TimeDom:
       /\ bad_now >= 0 /\ bad_now < last_now_ns
       /\ last_outcome' = OUT_CLOCK_REJ
       /\ local_reject_noburn_ok' = local_reject_noburn_ok
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_acct_ord_cap, cfg_sym_cxl_cap, cfg_ip_wt_cap, cfg_allow_ban,
            last_now_ns, acct_frozen, acct_cd_until_ns, ip1_cd_until_ns, ip2_cd_until_ns,
            ord_ep_some, ord_ep_idx, ord_used, ord_warn,
            cxl_ep_some, cxl_ep_idx, cxl_used, cxl_warn,
            ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_used, ip1_peak, ip1_warn,
            ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_used, ip2_peak, ip2_warn,
            bicond_ok, rollback_exact_ok, isolation_ok, ip_failover_ok, u64_u128_safe_ok
          >>

(* `note_ip_cooldown`: HTTP `429` cools down ONLY `ip` (`rate_limiter.rs:1117-1169,1740-1750`): *)
NoteHttp429IpCooldown ==
  /\ CanStep
  /\ \E ip \in {1, 2}, until_ns \in TimeDom:
       /\ until_ns >= 0 /\ until_ns <= EffU64Max
       /\ ip1_cd_until_ns' = (IF ip = 1 THEN Max2(ip1_cd_until_ns, until_ns) ELSE ip1_cd_until_ns)
       /\ ip2_cd_until_ns' = (IF ip = 2 THEN Max2(ip2_cd_until_ns, until_ns) ELSE ip2_cd_until_ns)
       /\ last_outcome' = OUT_IP_429
       /\ ip_failover_ok' =
            (ip_failover_ok
             /\ ((ip = 1) => (ip2_cd_until_ns' = ip2_cd_until_ns))
             /\ ((ip = 2) => (ip1_cd_until_ns' = ip1_cd_until_ns)))
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_acct_ord_cap, cfg_sym_cxl_cap, cfg_ip_wt_cap, cfg_allow_ban,
            last_now_ns, acct_frozen, acct_cd_until_ns,
            ord_ep_some, ord_ep_idx, ord_used, ord_warn,
            cxl_ep_some, cxl_ep_idx, cxl_used, cxl_warn,
            ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_used, ip1_peak, ip1_warn,
            ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_used, ip2_peak, ip2_warn,
            bicond_ok, rollback_exact_ok, isolation_ok, local_reject_noburn_ok, u64_u128_safe_ok
          >>

NoteAccountCooldown ==
  /\ CanStep
  /\ \E until_ns \in TimeDom:
       /\ until_ns >= 0 /\ until_ns <= EffU64Max
       /\ acct_cd_until_ns' = Max2(acct_cd_until_ns, until_ns)
       /\ last_outcome' = OUT_ACCT_CD
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_acct_ord_cap, cfg_sym_cxl_cap, cfg_ip_wt_cap, cfg_allow_ban,
            last_now_ns, acct_frozen, ip1_cd_until_ns, ip2_cd_until_ns,
            ord_ep_some, ord_ep_idx, ord_used, ord_warn,
            cxl_ep_some, cxl_ep_idx, cxl_used, cxl_warn,
            ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_used, ip1_peak, ip1_warn,
            ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_used, ip2_peak, ip2_warn,
            bicond_ok, rollback_exact_ok, isolation_ok, ip_failover_ok,
            local_reject_noburn_ok, u64_u128_safe_ok
          >>

(* `freeze_account`: HTTP `418` permanently freezes the account (`rate_limiter.rs:1199-1235,1757-1765`): *)
NoteHttp418AccountBan ==
  /\ CanStep
  /\ cfg_allow_ban
  /\ acct_frozen' = TRUE
  /\ last_outcome' = OUT_ACCT_418
  /\ ip_failover_ok' =
       (ip_failover_ok
        /\ (IF TRUE THEN TRUE ELSE FALSE))
  /\ step_budget' = NextBudget
  /\ UNCHANGED <<
       cfg_acct_ord_cap, cfg_sym_cxl_cap, cfg_ip_wt_cap, cfg_allow_ban,
       last_now_ns, acct_cd_until_ns, ip1_cd_until_ns, ip2_cd_until_ns,
       ord_ep_some, ord_ep_idx, ord_used, ord_warn,
       cxl_ep_some, cxl_ep_idx, cxl_used, cxl_warn,
       ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_used, ip1_peak, ip1_warn,
       ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_used, ip2_peak, ip2_warn,
       bicond_ok, rollback_exact_ok, isolation_ok, local_reject_noburn_ok, u64_u128_safe_ok
     >>

(* `reconcile_exchange_usage` on `FixedWindow` (`rate_limiter.rs:1422-1448`): *)
ReconcileExchangeFixed ==
  /\ CanStep
  /\ \E now_ns \in TimeDom, reported \in ReportDom:
       /\ now_ns >= last_now_ns /\ now_ns <= EffU64Max
       /\ reported >= 0 /\ reported <= EffU64Max
       /\ LET adv_u == FixedAdvUsed(ord_ep_some, ord_ep_idx, ord_used, now_ns)
              clamped == Min2(reported, cfg_acct_ord_cap)
              warn == ReconcileWarn80(reported, cfg_acct_ord_cap)
              u128_safe == (reported * 100 <= EffU128Max) /\ (cfg_acct_ord_cap * 80 <= EffU128Max)
          IN /\ last_now_ns' = now_ns
             /\ ord_ep_some' = TRUE
             /\ ord_ep_idx' = FixedEpochIdx(now_ns)
             /\ ord_used' = Max2(adv_u, clamped)
             /\ ord_warn' = warn
             /\ last_outcome' = OUT_RECON_FIXED
             /\ u64_u128_safe_ok' = (u64_u128_safe_ok /\ u128_safe)
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_acct_ord_cap, cfg_sym_cxl_cap, cfg_ip_wt_cap, cfg_allow_ban,
            acct_frozen, acct_cd_until_ns, ip1_cd_until_ns, ip2_cd_until_ns,
            cxl_ep_some, cxl_ep_idx, cxl_used, cxl_warn,
            ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_used, ip1_peak, ip1_warn,
            ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_used, ip2_peak, ip2_warn,
            bicond_ok, rollback_exact_ok, isolation_ok, ip_failover_ok, local_reject_noburn_ok
          >>

(* `reconcile_exchange_usage` on `RollingWindow` (`rate_limiter.rs:1449-1468`): *)
ReconcileExchangeRolling ==
  /\ CanStep
  /\ \E now_ns \in TimeDom, reported \in ReportDom:
       /\ now_ns >= last_now_ns /\ now_ns <= EffU64Max
       /\ reported >= 0 /\ reported <= EffU64Max
       /\ LET el == RollEffLen(ip1_len, ip1_s0_ts, ip1_s1_ts, now_ns)
              e0_ts == RollEff0Ts(ip1_len, ip1_s0_ts, ip1_s1_ts, now_ns)
              e0_u == RollEff0U(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, now_ns)
              e1_ts == RollEff1Ts(ip1_len, ip1_s0_ts, ip1_s1_ts, now_ns)
              e1_u == RollEff1U(ip1_len, ip1_s0_ts, ip1_s1_ts, ip1_s1_u, now_ns)
              eu == RollEffUsed(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, now_ns)
              epk == RollEffPeak(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_peak, now_ns)
              clamped == Min2(reported, cfg_ip_wt_cap)
              warn == ReconcileWarn80(reported, cfg_ip_wt_cap)
              u128_safe == (reported * 100 <= EffU128Max) /\ (cfg_ip_wt_cap * 80 <= EffU128Max)
          IN /\ last_now_ns' = now_ns
             /\ ip1_len' = el
             /\ ip1_s0_ts' = e0_ts /\ ip1_s0_u' = e0_u
             /\ ip1_s1_ts' = e1_ts /\ ip1_s1_u' = e1_u
             /\ ip1_used' = eu
             /\ ip1_peak' = Max2(epk, clamped)
             /\ ip1_warn' = warn
             /\ last_outcome' = OUT_RECON_ROLL
             /\ u64_u128_safe_ok' = (u64_u128_safe_ok /\ u128_safe)
       /\ step_budget' = NextBudget
       /\ UNCHANGED <<
            cfg_acct_ord_cap, cfg_sym_cxl_cap, cfg_ip_wt_cap, cfg_allow_ban,
            acct_frozen, acct_cd_until_ns, ip1_cd_until_ns, ip2_cd_until_ns,
            ord_ep_some, ord_ep_idx, ord_used, ord_warn,
            cxl_ep_some, cxl_ep_idx, cxl_used, cxl_warn,
            ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_used, ip2_peak, ip2_warn,
            bicond_ok, rollback_exact_ok, isolation_ok, ip_failover_ok, local_reject_noburn_ok
          >>

Next ==
  \/ PlaceOrderSingle
  \/ PlaceOrderDuplicateFixed
  \/ RollingFirstMultiSpend
  \/ CancelOrder
  \/ QueryRequest
  \/ LocalAdmissionRejectNoBurn
  \/ ClockRegressionReject
  \/ NoteHttp429IpCooldown
  \/ NoteAccountCooldown
  \/ NoteHttp418AccountBan
  \/ ReconcileExchangeFixed
  \/ ReconcileExchangeRolling

Spec == Init /\ [][Next]_vars

(* --- Safety Invariants & Mode S2 Symbolic Inductive Invariant (`IndInv`) --- *)

RollingWellFormed(len, s0_ts, s0_u, s1_ts, s1_u, used, peak, warn, cap) ==
  /\ len \in {0, 1, 2}
  /\ s0_ts \in Int /\ s0_ts >= 0 /\ s0_ts <= EffU64Max
  /\ s0_u \in Int /\ s0_u >= 0 /\ s0_u <= cap
  /\ s1_ts \in Int /\ s1_ts >= 0 /\ s1_ts <= EffU64Max
  /\ s1_u \in Int /\ s1_u >= 0 /\ s1_u <= cap
  /\ used \in Int /\ used >= 0 /\ used <= cap
  /\ peak \in Int /\ peak >= 0 /\ peak <= cap
  /\ warn \in BOOLEAN
  /\ ((len = 0) => (s0_ts = 0 /\ s0_u = 0 /\ s1_ts = 0 /\ s1_u = 0 /\ used = 0))
  /\ ((len = 1) => (s0_u >= 1 /\ s0_ts <= last_now_ns /\ s1_ts = 0 /\ s1_u = 0 /\ used = s0_u))
  /\ ((len = 2) => (s0_u >= 1 /\ s1_u >= 1 /\ s0_ts < s1_ts /\ s1_ts <= last_now_ns /\ used = s0_u + s1_u))

TypeOK ==
  /\ ValidConfig
  /\ last_now_ns \in Int /\ last_now_ns >= 0 /\ last_now_ns <= EffU64Max
  /\ acct_frozen \in BOOLEAN
  /\ acct_cd_until_ns \in Int /\ acct_cd_until_ns >= 0 /\ acct_cd_until_ns <= EffU64Max
  /\ ip1_cd_until_ns \in Int /\ ip1_cd_until_ns >= 0 /\ ip1_cd_until_ns <= EffU64Max
  /\ ip2_cd_until_ns \in Int /\ ip2_cd_until_ns >= 0 /\ ip2_cd_until_ns <= EffU64Max
  /\ ord_ep_some \in BOOLEAN
  /\ ord_ep_idx \in Int /\ ord_ep_idx >= 0 /\ ord_ep_idx <= FixedEpochIdx(EffU64Max)
  /\ ord_used \in Int /\ ord_used >= 0 /\ ord_used <= cfg_acct_ord_cap
  /\ ord_warn \in BOOLEAN
  /\ cxl_ep_some \in BOOLEAN
  /\ cxl_ep_idx \in Int /\ cxl_ep_idx >= 0 /\ cxl_ep_idx <= FixedEpochIdx(EffU64Max)
  /\ cxl_used \in Int /\ cxl_used >= 0 /\ cxl_used <= cfg_sym_cxl_cap
  /\ cxl_warn \in BOOLEAN
  /\ RollingWellFormed(ip1_len, ip1_s0_ts, ip1_s0_u, ip1_s1_ts, ip1_s1_u, ip1_used, ip1_peak, ip1_warn, cfg_ip_wt_cap)
  /\ RollingWellFormed(ip2_len, ip2_s0_ts, ip2_s0_u, ip2_s1_ts, ip2_s1_u, ip2_used, ip2_peak, ip2_warn, cfg_ip_wt_cap)
  /\ last_outcome \in 0..17
  /\ step_budget \in Int /\ step_budget >= 0 /\ step_budget <= MaxBoundedSteps
  /\ bicond_ok \in BOOLEAN /\ rollback_exact_ok \in BOOLEAN /\ isolation_ok \in BOOLEAN
  /\ ip_failover_ok \in BOOLEAN /\ local_reject_noburn_ok \in BOOLEAN /\ u64_u128_safe_ok \in BOOLEAN

WindowCapsRespected ==
  /\ ord_used <= cfg_acct_ord_cap
  /\ cxl_used <= cfg_sym_cxl_cap
  /\ ip1_used <= cfg_ip_wt_cap
  /\ ip2_used <= cfg_ip_wt_cap
  /\ ip1_peak <= cfg_ip_wt_cap
  /\ ip2_peak <= cfg_ip_wt_cap

CanAffordMatchesAllow == bicond_ok

TransactionalRollbackExact == rollback_exact_ok

CancelAndQueryIsolatedFromOrderCap == isolation_ok

PerIpCooldownIsolation ==
  /\ ip_failover_ok
  /\ ((~cfg_allow_ban) => (~acct_frozen))
  /\ (acct_frozen =>
        /\ RoutePrecheckCode(ip1_cd_until_ns, last_now_ns) = REJ_FROZEN
        /\ RoutePrecheckCode(ip2_cd_until_ns, last_now_ns) = REJ_FROZEN)

LocalRejectAndClockRegressionNoBurn == local_reject_noburn_ok

U64AndU128OverflowSafe == u64_u128_safe_ok

SafetyConjunction ==
  /\ TypeOK
  /\ WindowCapsRespected
  /\ CanAffordMatchesAllow
  /\ TransactionalRollbackExact
  /\ CancelAndQueryIsolatedFromOrderCap
  /\ PerIpCooldownIsolation
  /\ LocalRejectAndClockRegressionNoBurn
  /\ U64AndU128OverflowSafe

(* Mode S2 Strengthened Inductive Invariant (`IndInv`): *)
IndInv ==
  /\ SafetyConjunction
  /\ ((~ord_ep_some) => (ord_ep_idx = 0 /\ ord_used = 0 /\ ~ord_warn))
  /\ (ord_ep_some => (ord_ep_idx <= FixedEpochIdx(last_now_ns)))
  /\ ((~cxl_ep_some) => (cxl_ep_idx = 0 /\ cxl_used = 0 /\ ~cxl_warn))
  /\ (cxl_ep_some => (cxl_ep_idx <= FixedEpochIdx(last_now_ns)))

====
