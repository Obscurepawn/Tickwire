------------------------ MODULE L0_SharedOrsLimiter ------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Accounts,
    Ips,
    AcctOrderCap,
    AcctCancelCap,
    AcctReqCap,
    SharedIpCap,
    CooldownTicks,
    MaxClock,
    ReportedLimit,
    ReportedCounts,
    AllowAccountFreeze

(*
Code-faithful finite-state model of the unified ORS process-wide `SharedRateLimiter` +
`OrsAccountQuota` + lock-free `PublishedRoutes::pick -> pick_and_spend_or_nack` admission and
rising-edge `ExchRateLimitEvent` (`80%` warning / `100%` ceiling) reconciliation.

Variable <-> Rust source mapping:
- clock: monotonic timestamp (`now_ns`) across fixed epochs and rolling sub-ticks,
  crates/tickwire-quota/src/shared.rs:340-388,
  crates/tickwire-quota/src/shared.rs:507-614.
- conn_ready, route_drops, last_conn1, single_pick_sound: `PublishedRoutes::pick` ready-route
  bitmap, `last_conn_id` round-robin cursor, and `SharedRateLimiter::pick_and_spend_or_nack`
  single-pick no-retry discipline (`NoReadyConnection`, `MissingLocalIp`, `RateLimitedNack`),
  crates/tickwire-transport/src/order_router.rs:220-232,
  crates/tickwire-transport/src/order_router.rs:688-745,
  crates/tickwire-quota/src/shared.rs:1329-1355,
  crates/tickwire-quota/src/shared.rs:1762-1816,
  crates/tickwire-runtime/src/session.rs:518-580,
  crates/tickwire-spot-execution/src/session.rs:1185-1233.
- acct_ord_used, acct1_cxl_used, acct1_req_used: per-account (`account_ref = 1..255`)
  `AtomicFixedWindow` `ORDER` and `CANCEL` counters and `AtomicRollingWindow` `REQUEST` sub-buckets
  isolated by `RateFlags`,
  crates/tickwire-quota/src/shared.rs:312-412,
  crates/tickwire-quota/src/shared.rs:1580-1701.
- shared_ip_cur, shared_ip_prev: process-wide cross-account `RateRule::ip` (`account = 0`)
  `AtomicRollingWindow` active sub-buckets (`in_window_sum = cur + prev`, `put_back` when
  `slot_tick == booked_tick && slot_count >= req`),
  crates/tickwire-quota/src/shared.rs:414-643,
  crates/tickwire-quota/src/shared.rs:1560-1578.
- acct1_frozen, acct1_cd_until, ip1_cd_until: `AccountCooldownSlot` (`freeze_account`,
  `note_account_cooldown`, `check_account`) and `IpCooldownSlot` (`note_ip_cooldown`, `check_ip`),
  crates/tickwire-quota/src/shared.rs:1095-1245.
- rollback_exact: step-by-step witness for `SharedRateLimiter::allow` reverse-order `put_back_undo`
  unwinding (`SharedUndo::{Fixed, Rolling}`) on multi-bucket partial failure,
  crates/tickwire-quota/src/shared.rs:1251-1324.
- exch_latch, latch_rising_edge_sound: `SharedRateLimiter::reconcile_venue_usage` exact integer
  thresholds (`reported_count * 100 >= reported_limit * 80` and `reported_count >= reported_limit`),
  `LATCH_WARN_ACTIVE = 1`, `LATCH_CEIL_ACTIVE = 2`, and `OrderRoutingSystem::broadcast_exch_rate_limit`,
  crates/tickwire-quota/src/shared.rs:1361-1455,
  crates/tickwire-runtime/src/ors.rs:2919-2926,
  crates/tickwire-runtime/src/ors.rs:3377-3409.
*)

ASSUME
    /\ Accounts = {1, 2}
    /\ Ips = {1, 2}
    /\ AcctOrderCap >= 1
    /\ AcctCancelCap >= 1
    /\ AcctReqCap >= 1
    /\ SharedIpCap >= 1
    /\ CooldownTicks >= 1
    /\ MaxClock >= 1
    /\ ReportedLimit >= 1
    /\ ReportedCounts # {}
    /\ AllowAccountFreeze \in BOOLEAN

VARIABLES
    clock,
    conn_ready,
    route_drops,
    last_conn1,
    acct_ord_used,
    acct1_cxl_used,
    acct1_req_used,
    shared_ip_cur,
    shared_ip_prev,
    acct1_frozen,
    acct1_cd_until,
    ip1_cd_until,
    exch_latch,
    single_pick_sound,
    rollback_exact,
    latch_rising_edge_sound

vars == <<
    clock, conn_ready, route_drops, last_conn1,
    acct_ord_used, acct1_cxl_used, acct1_req_used,
    shared_ip_cur, shared_ip_prev,
    acct1_frozen, acct1_cd_until, ip1_cd_until,
    exch_latch, single_pick_sound, rollback_exact, latch_rising_edge_sound
>>

LATCH_WARN == 1
LATCH_CEIL == 2
MaxCooldown == MaxClock + CooldownTicks
Max(a, b) == IF a >= b THEN a ELSE b
MinElem(S) == CHOOSE x \in S : \A y \in S : x <= y

PickOnce1 ==
    IF conn_ready = {} THEN 0
    ELSE
        LET first == MinElem(conn_ready)
            after == {c \in conn_ready : last_conn1 # 0 /\ c > last_conn1}
        IN
        IF last_conn1 = 0 THEN first
        ELSE IF after # {} THEN MinElem(after)
        ELSE first

SharedIpSum(ws, ip) == ws.ip_cur[ip] + ws.ip_prv[ip]

CurrentBuckets ==
    [ord    |-> acct_ord_used,
     cxl1   |-> acct1_cxl_used,
     req1   |-> acct1_req_used,
     ip_cur |-> shared_ip_cur,
     ip_prv |-> shared_ip_prev]

AcctFrozenAt(a) == (a = 1) /\ acct1_frozen
AcctCooledAt(a, now) == (a = 1) /\ (now < acct1_cd_until)
IpCooledAt(ip, now) == (ip = 1) /\ (now < ip1_cd_until)

PrecheckChain(spends, now) ==
    LET bad_acct == {i \in 1..Len(spends) :
            spends[i].acct # 0 /\ (AcctFrozenAt(spends[i].acct) \/ AcctCooledAt(spends[i].acct, now))}
        bad_ip == {i \in 1..Len(spends) :
            spends[i].ip # 0 /\ IpCooledAt(spends[i].ip, now)}
    IN
    IF bad_acct # {} THEN
        LET i == MinElem(bad_acct)
            a == spends[i].acct
        IN [ok |-> FALSE, reason |-> IF AcctFrozenAt(a) THEN "Frozen" ELSE "AccountCooldown"]
    ELSE IF bad_ip # {} THEN
        [ok |-> FALSE, reason |-> "IpCooldown"]
    ELSE
        [ok |-> TRUE, reason |-> "None"]

TryTakeBucket(ws, sp) ==
    IF sp.kind = "AcctOrd" THEN
        IF ws.ord[sp.acct] + sp.units > AcctOrderCap THEN
            [ok |-> FALSE, ws |-> ws]
        ELSE
            [ok |-> TRUE,
             ws |-> [ws EXCEPT !.ord[sp.acct] = @ + sp.units],
             undo |-> [kind |-> "AcctOrd", acct |-> sp.acct, ip |-> 0, units |-> sp.units]]
    ELSE IF sp.kind = "Acct1Cxl" THEN
        IF ws.cxl1 + sp.units > AcctCancelCap THEN
            [ok |-> FALSE, ws |-> ws]
        ELSE
            [ok |-> TRUE,
             ws |-> [ws EXCEPT !.cxl1 = @ + sp.units],
             undo |-> [kind |-> "Acct1Cxl", acct |-> 1, ip |-> 0, units |-> sp.units]]
    ELSE IF sp.kind = "Acct1Req" THEN
        IF ws.req1 + sp.units > AcctReqCap THEN
            [ok |-> FALSE, ws |-> ws]
        ELSE
            [ok |-> TRUE,
             ws |-> [ws EXCEPT !.req1 = @ + sp.units],
             undo |-> [kind |-> "Acct1Req", acct |-> 1, ip |-> 0, units |-> sp.units]]
    ELSE
        LET in_win == SharedIpSum(ws, sp.ip) IN
        IF in_win + sp.units > SharedIpCap THEN
            [ok |-> FALSE, ws |-> ws]
        ELSE
            [ok |-> TRUE,
             ws |-> [ws EXCEPT !.ip_cur[sp.ip] = @ + sp.units],
             undo |-> [kind |-> "SharedIp", acct |-> 0, ip |-> sp.ip, units |-> sp.units]]

ApplyUndo(ws, u) ==
    IF u.kind = "AcctOrd" THEN [ws EXCEPT !.ord[u.acct] = @ - u.units]
    ELSE IF u.kind = "Acct1Cxl" THEN [ws EXCEPT !.cxl1 = @ - u.units]
    ELSE IF u.kind = "Acct1Req" THEN [ws EXCEPT !.req1 = @ - u.units]
    ELSE [ws EXCEPT !.ip_cur[u.ip] = @ - u.units]

RECURSIVE UnwindStack(_, _)
UnwindStack(ws, undos) ==
    IF Len(undos) = 0 THEN ws
    ELSE UnwindStack(ApplyUndo(ws, undos[Len(undos)]), SubSeq(undos, 1, Len(undos) - 1))

RECURSIVE RunAllowLoop(_, _, _, _)
RunAllowLoop(ws, spends, idx, undos) ==
    IF idx > Len(spends) THEN
        [ok |-> TRUE, ws |-> ws, reason |-> "None"]
    ELSE
        LET step == TryTakeBucket(ws, spends[idx]) IN
        IF step.ok THEN
            RunAllowLoop(step.ws, spends, idx + 1, Append(undos, step.undo))
        ELSE
            [ok |-> FALSE, ws |-> UnwindStack(step.ws, undos), reason |-> "Exhausted"]

AllowChain(spends, now) ==
    LET pre == PrecheckChain(spends, now) IN
    IF ~pre.ok THEN [ok |-> FALSE, ws |-> CurrentBuckets, reason |-> pre.reason]
    ELSE RunAllowLoop(CurrentBuckets, spends, 1, <<>>)

PlaceChain(a, ip) ==
    <<[kind |-> "AcctOrd",  acct |-> a, ip |-> 0,  units |-> 1],
      [kind |-> "SharedIp", acct |-> 0, ip |-> ip, units |-> 1]>>

CancelChain1(ip) ==
    <<[kind |-> "Acct1Cxl", acct |-> 1, ip |-> 0,  units |-> 1],
      [kind |-> "SharedIp", acct |-> 0, ip |-> ip, units |-> 1]>>

QueryChain1(ip) ==
    <<[kind |-> "Acct1Req", acct |-> 1, ip |-> 0,  units |-> 1],
      [kind |-> "SharedIp", acct |-> 0, ip |-> ip, units |-> 1]>>

ReversePlaceChain(a, ip) ==
    <<[kind |-> "SharedIp", acct |-> 0, ip |-> ip, units |-> 1],
      [kind |-> "AcctOrd",  acct |-> a, ip |-> 0,  units |-> 1]>>

EvalReconcile(prev_l, count, limit) ==
    LET warn_hit == (count * 100 >= limit * 80)
        ceil_hit == (count >= limit)
        desired == (IF warn_hit THEN LATCH_WARN ELSE 0) + (IF ceil_hit THEN LATCH_CEIL ELSE 0)
        prev_warn_set == (prev_l = 1 \/ prev_l = 3)
        prev_ceil_set == (prev_l = 2 \/ prev_l = 3)
        crossed_w == (warn_hit /\ ~prev_warn_set)
        crossed_c == (ceil_hit /\ ~prev_ceil_set)
        ev == IF crossed_c THEN "Ceiling"
              ELSE IF crossed_w THEN "Warn"
              ELSE "None"
    IN [next_latch |-> desired, event |-> ev]

Init ==
    /\ clock = 0
    /\ conn_ready = Ips
    /\ route_drops = 0
    /\ last_conn1 = 0
    /\ acct_ord_used = [a \in Accounts |-> 0]
    /\ acct1_cxl_used = 0
    /\ acct1_req_used = 0
    /\ shared_ip_cur = [ip \in Ips |-> 0]
    /\ shared_ip_prev = [ip \in Ips |-> 0]
    /\ acct1_frozen = FALSE
    /\ acct1_cd_until = 0
    /\ ip1_cd_until = 0
    /\ exch_latch = 0
    /\ single_pick_sound = TRUE
    /\ rollback_exact = TRUE
    /\ latch_rising_edge_sound = TRUE

\* 1a. Account 1 single-pick route + spend (`PublishedRoutes::pick -> pick_and_spend_or_nack`):
PickAndPlaceAcct1 ==
    LET picked == PickOnce1 IN
    IF picked = 0 THEN
        /\ single_pick_sound' = (single_pick_sound /\ conn_ready = {})
        /\ UNCHANGED <<
            clock, conn_ready, route_drops, last_conn1,
            acct_ord_used, acct1_cxl_used, acct1_req_used,
            shared_ip_cur, shared_ip_prev,
            acct1_frozen, acct1_cd_until, ip1_cd_until,
            exch_latch, rollback_exact, latch_rising_edge_sound
           >>
    ELSE
        LET res == AllowChain(PlaceChain(1, picked), clock)
            sib == 3 - picked
        IN
        /\ acct_ord_used' = res.ws.ord
        /\ acct1_cxl_used' = res.ws.cxl1
        /\ acct1_req_used' = res.ws.req1
        /\ shared_ip_cur' = res.ws.ip_cur
        /\ shared_ip_prev' = res.ws.ip_prv
        /\ last_conn1' = IF res.ok THEN picked ELSE last_conn1
        /\ single_pick_sound' = (single_pick_sound
            /\ picked \in conn_ready
            /\ res.ws.ip_cur[sib] = shared_ip_cur[sib]
            /\ res.ws.ip_prv[sib] = shared_ip_prev[sib])
        /\ rollback_exact' = (rollback_exact /\ (res.ok \/ res.ws = CurrentBuckets))
        /\ UNCHANGED <<
            clock, conn_ready, route_drops, acct1_frozen, acct1_cd_until, ip1_cd_until,
            exch_latch, latch_rising_edge_sound
           >>

\* 1b. Sibling Account 2 concurrent placement contending on shared `RateRule::ip`:
PlaceOrderAcct2(ip) ==
    LET res == AllowChain(PlaceChain(2, ip), clock) IN
    /\ ip \in conn_ready
    /\ acct_ord_used' = res.ws.ord
    /\ shared_ip_cur' = res.ws.ip_cur
    /\ shared_ip_prev' = res.ws.ip_prv
    /\ rollback_exact' = (rollback_exact /\ (res.ok \/ res.ws = CurrentBuckets))
    /\ UNCHANGED <<
        clock, conn_ready, route_drops, last_conn1,
        acct1_cxl_used, acct1_req_used,
        acct1_frozen, acct1_cd_until, ip1_cd_until,
        exch_latch, single_pick_sound, latch_rising_edge_sound
       >>

\* 2. Account 1 single-pick cancel (strictly isolated from ORDER windows):
PickAndCancelAcct1 ==
    LET picked == PickOnce1 IN
    /\ picked # 0
    /\ LET res == AllowChain(CancelChain1(picked), clock)
           sib == 3 - picked
       IN
       /\ acct_ord_used' = res.ws.ord
       /\ acct1_cxl_used' = res.ws.cxl1
       /\ acct1_req_used' = res.ws.req1
       /\ shared_ip_cur' = res.ws.ip_cur
       /\ shared_ip_prev' = res.ws.ip_prv
       /\ last_conn1' = IF res.ok THEN picked ELSE last_conn1
       /\ single_pick_sound' = (single_pick_sound
           /\ res.ws.ord = acct_ord_used
           /\ res.ws.ip_cur[sib] = shared_ip_cur[sib])
       /\ rollback_exact' = (rollback_exact /\ (res.ok \/ res.ws = CurrentBuckets))
       /\ UNCHANGED <<
           clock, conn_ready, route_drops, acct1_frozen, acct1_cd_until, ip1_cd_until,
           exch_latch, latch_rising_edge_sound
          >>

\* 3. Account 1 query spend (strictly isolated from ORDER and CANCEL windows):
AllowAccountQuery(ip) ==
    LET res == AllowChain(QueryChain1(ip), clock) IN
    /\ res.ok
    /\ acct1_req_used' = res.ws.req1
    /\ shared_ip_cur' = res.ws.ip_cur
    /\ shared_ip_prev' = res.ws.ip_prv
    /\ UNCHANGED <<
        clock, conn_ready, route_drops, last_conn1, acct_ord_used, acct1_cxl_used,
        acct1_frozen, acct1_cd_until, ip1_cd_until,
        exch_latch, single_pick_sound, rollback_exact, latch_rising_edge_sound
       >>

\* 4. Reverse-order rolling -> fixed rollback probe ([SharedIp(ip), AcctOrd(a)]):
ReverseOrderRollbackProbe(a, ip) ==
    LET res == AllowChain(ReversePlaceChain(a, ip), clock) IN
    /\ ~res.ok
    /\ acct_ord_used' = res.ws.ord
    /\ shared_ip_cur' = res.ws.ip_cur
    /\ shared_ip_prev' = res.ws.ip_prv
    /\ rollback_exact' = (rollback_exact /\ res.ws = CurrentBuckets)
    /\ UNCHANGED <<
        clock, conn_ready, route_drops, last_conn1, acct1_cxl_used, acct1_req_used,
        acct1_frozen, acct1_cd_until, ip1_cd_until,
        exch_latch, single_pick_sound, latch_rising_edge_sound
       >>

\* 5. route_ip_key(route.local_ip == 0) -> RouteAdmissionReject::MissingLocalIp before allow().
RejectMissingLocalIp ==
    LET before == CurrentBuckets IN
    /\ rollback_exact' = (rollback_exact /\ CurrentBuckets = before)
    /\ UNCHANGED <<
        clock, conn_ready, route_drops, last_conn1,
        acct_ord_used, acct1_cxl_used, acct1_req_used,
        shared_ip_cur, shared_ip_prev,
        acct1_frozen, acct1_cd_until, ip1_cd_until,
        exch_latch, single_pick_sound, latch_rising_edge_sound
       >>

\* 6. Bounded route drop / restore in PublishedRoutes.
DropPublishedRoute(c) ==
    /\ route_drops < 1 \/ conn_ready = {c}
    /\ route_drops < 2
    /\ c \in conn_ready
    /\ conn_ready' = conn_ready \ {c}
    /\ route_drops' = route_drops + 1
    /\ UNCHANGED <<
        clock, last_conn1,
        acct_ord_used, acct1_cxl_used, acct1_req_used,
        shared_ip_cur, shared_ip_prev,
        acct1_frozen, acct1_cd_until, ip1_cd_until,
        exch_latch, single_pick_sound, rollback_exact, latch_rising_edge_sound
       >>

RestorePublishedRoute(c) ==
    /\ c \in Ips
    /\ c \notin conn_ready
    /\ conn_ready' = conn_ready \cup {c}
    /\ UNCHANGED <<
        clock, route_drops, last_conn1,
        acct_ord_used, acct1_cxl_used, acct1_req_used,
        shared_ip_cur, shared_ip_prev,
        acct1_frozen, acct1_cd_until, ip1_cd_until,
        exch_latch, single_pick_sound, rollback_exact, latch_rising_edge_sound
       >>

\* 7. Venue HTTP 429 sets per-IP cooldown on Ip 1 across accounts while leaving Ip 2 open.
ObserveIp429Cooldown ==
    LET target == clock + CooldownTicks IN
    /\ ip1_cd_until < target
    /\ ip1_cd_until' = Max(ip1_cd_until, target)
    /\ UNCHANGED <<
        clock, conn_ready, route_drops, last_conn1,
        acct_ord_used, acct1_cxl_used, acct1_req_used,
        shared_ip_cur, shared_ip_prev,
        acct1_frozen, acct1_cd_until,
        exch_latch, single_pick_sound, rollback_exact, latch_rising_edge_sound
       >>

\* 8. Per-account temporary cooldown on Account 1 (Account 2 unaffected on shared Ip 1!).
ObserveAccountCooldown ==
    LET target == clock + CooldownTicks IN
    /\ ~acct1_frozen
    /\ acct1_cd_until < target
    /\ acct1_cd_until' = Max(acct1_cd_until, target)
    /\ UNCHANGED <<
        clock, conn_ready, route_drops, last_conn1,
        acct_ord_used, acct1_cxl_used, acct1_req_used,
        shared_ip_cur, shared_ip_prev,
        acct1_frozen, ip1_cd_until,
        exch_latch, single_pick_sound, rollback_exact, latch_rising_edge_sound
       >>

\* 9. Per-account HTTP 418 freeze on Account 1 (Account 2 unaffected on shared Ip 1!).
FreezeAccount418 ==
    /\ AllowAccountFreeze
    /\ ~acct1_frozen
    /\ acct1_frozen' = TRUE
    /\ UNCHANGED <<
        clock, conn_ready, route_drops, last_conn1,
        acct_ord_used, acct1_cxl_used, acct1_req_used,
        shared_ip_cur, shared_ip_prev,
        acct1_cd_until, ip1_cd_until,
        exch_latch, single_pick_sound, rollback_exact, latch_rising_edge_sound
       >>

\* 10. Rising-edge venue `rateLimits[]` reconciliation (`reconcile_venue_usage`).
ReconcileVenueUsage(count) ==
    LET r == EvalReconcile(exch_latch, count, ReportedLimit)
        warn_hit == (count * 100 >= ReportedLimit * 80)
        ceil_hit == (count >= ReportedLimit)
        was_warn == (exch_latch = 1 \/ exch_latch = 3)
        was_ceil == (exch_latch = 2 \/ exch_latch = 3)
    IN
    /\ exch_latch' = r.next_latch
    /\ latch_rising_edge_sound' = (latch_rising_edge_sound
        /\ (r.event = "Ceiling" <=> (ceil_hit /\ ~was_ceil))
        /\ (r.event = "Warn" <=> (~(ceil_hit /\ ~was_ceil) /\ warn_hit /\ ~was_warn))
        /\ (r.event = "None" <=> (~(ceil_hit /\ ~was_ceil) /\ ~(warn_hit /\ ~was_warn))))
    /\ UNCHANGED <<
        clock, conn_ready, route_drops, last_conn1,
        acct_ord_used, acct1_cxl_used, acct1_req_used,
        shared_ip_cur, shared_ip_prev,
        acct1_frozen, acct1_cd_until, ip1_cd_until,
        single_pick_sound, rollback_exact
       >>

\* 11. Monotonic clock step: rolls fixed epochs and shifts 2-sub-tick rolling window.
AdvanceClock ==
    /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ acct_ord_used' = [a \in Accounts |-> 0]
    /\ acct1_cxl_used' = 0
    /\ acct1_req_used' = 0
    /\ shared_ip_prev' = shared_ip_cur
    /\ shared_ip_cur' = [ip \in Ips |-> 0]
    /\ UNCHANGED <<
        conn_ready, route_drops, last_conn1,
        acct1_frozen, acct1_cd_until, ip1_cd_until,
        exch_latch, single_pick_sound, rollback_exact, latch_rising_edge_sound
       >>

Next ==
    \/ PickAndPlaceAcct1
    \/ \E ip \in Ips : PlaceOrderAcct2(ip)
    \/ PickAndCancelAcct1
    \/ \E ip \in Ips : AllowAccountQuery(ip)
    \/ \E a \in Accounts, ip \in Ips : ReverseOrderRollbackProbe(a, ip)
    \/ RejectMissingLocalIp
    \/ \E c \in Ips : DropPublishedRoute(c)
    \/ \E c \in Ips : RestorePublishedRoute(c)
    \/ ObserveIp429Cooldown
    \/ ObserveAccountCooldown
    \/ FreezeAccount418
    \/ \E cnt \in ReportedCounts : ReconcileVenueUsage(cnt)
    \/ AdvanceClock

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

TypeOK ==
    /\ clock \in 0..MaxClock
    /\ conn_ready \in SUBSET Ips
    /\ route_drops \in 0..2
    /\ last_conn1 \in Ips \cup {0}
    /\ acct_ord_used \in [Accounts -> 0..AcctOrderCap]
    /\ acct1_cxl_used \in 0..AcctCancelCap
    /\ acct1_req_used \in 0..AcctReqCap
    /\ shared_ip_cur \in [Ips -> 0..SharedIpCap]
    /\ shared_ip_prev \in [Ips -> 0..SharedIpCap]
    /\ acct1_frozen \in BOOLEAN
    /\ acct1_cd_until \in 0..MaxCooldown
    /\ ip1_cd_until \in 0..MaxCooldown
    /\ exch_latch \in {0, 1, 3}
    /\ single_pick_sound \in BOOLEAN
    /\ rollback_exact \in BOOLEAN
    /\ latch_rising_edge_sound \in BOOLEAN

SharedAndAccountCapsRespected ==
    /\ \A a \in Accounts : acct_ord_used[a] <= AcctOrderCap
    /\ acct1_cxl_used <= AcctCancelCap
    /\ acct1_req_used <= AcctReqCap
    /\ \A ip \in Ips :
        /\ shared_ip_cur[ip] >= 0
        /\ shared_ip_prev[ip] >= 0
        /\ shared_ip_cur[ip] + shared_ip_prev[ip] <= SharedIpCap

AtomicMultiBucketRollbackExact == rollback_exact

SinglePickNeverRetriesSibling == single_pick_sound

CancelAndQueryImmuneToOrderCap ==
    \A ip \in Ips :
        (~acct1_frozen
         /\ clock >= acct1_cd_until
         /\ ~IpCooledAt(ip, clock)
         /\ acct_ord_used[1] = AcctOrderCap
         /\ shared_ip_cur[ip] + shared_ip_prev[ip] < SharedIpCap) =>
            /\ (acct1_cxl_used < AcctCancelCap => AllowChain(CancelChain1(ip), clock).ok)
            /\ (acct1_req_used < AcctReqCap => AllowChain(QueryChain1(ip), clock).ok)

CrossAccountFreezeAndIpCooldownIsolation ==
    /\ \A ip \in Ips :
        ((acct1_frozen \/ clock < acct1_cd_until)
         /\ ~IpCooledAt(ip, clock)
         /\ acct_ord_used[2] < AcctOrderCap
         /\ shared_ip_cur[ip] + shared_ip_prev[ip] < SharedIpCap) =>
            /\ ~AllowChain(PlaceChain(1, ip), clock).ok
            /\ AllowChain(PlaceChain(2, ip), clock).ok
    /\ (clock < ip1_cd_until
        /\ acct_ord_used[2] < AcctOrderCap
        /\ shared_ip_cur[2] + shared_ip_prev[2] < SharedIpCap) =>
            /\ AllowChain(PlaceChain(2, 1), clock).reason = "IpCooldown"
            /\ AllowChain(PlaceChain(2, 2), clock).ok

RisingEdgeExchLatchSound ==
    /\ latch_rising_edge_sound
    /\ exch_latch # 2
    /\ (exch_latch = 3 => EvalReconcile(exch_latch, ReportedLimit, ReportedLimit).event = "None")

=============================================================================
