-------------------------- MODULE L1_MultiIpQuota --------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Ips,
    AcctOrderCap,
    SymCancelCap,
    IpWeightCap,
    RingCap,
    FixedInterval,
    RollingInterval,
    CooldownTicks,
    MaxClock,
    ReportedSet,
    AllowAccountBan

(*
Code-faithful model of Stage 3 multi-window `RateLimiter` (`FixedWindow`, bounded-ring
`RollingWindow`, burst coalescing, reverse-order `put_back` rollback, `can_afford` vs. `allow`
equivalence across duplicate `RateLimitId` spends, and `reconcile_exchange_usage`),
`VenueRateLimiter` + `MultiIpCommandQuota` (`Order` vs. `Cancel` vs. `Request` isolation across
multiple local source IPs), pre-admission local reject non-burn, per-IP HTTP `429` failover, and
account-wide HTTP `418` freeze.

Variable <-> Rust source mapping:
- clock: monotonic timestamp (`now_ns`) in `0..MaxClock`,
  crates/tickwire-quota/src/rate_limiter.rs:1308-1425.
- acct_order_epoch, acct_order_used, acct_order_warn, sym_cancel_epoch, sym_cancel_used:
  `FixedWindow` state (`current_epoch_idx`, `used`, `warn_active`) for account `ORDER` and symbol
  `CANCEL` windows,
  crates/tickwire-quota/src/rate_limiter.rs:514-614,
  crates/tickwire-quota/src/rate_limiter.rs:1422-1460.
- ip_entries, ip_peak, ip_warn: per-IP `RollingWindow` bounded ring (`entries[head..head+len]`,
  `observed_exchange_peak`, `warn_active`) with sliding expiration and same-`now_ns` coalescing,
  crates/tickwire-quota/src/rate_limiter.rs:616-842,
  crates/tickwire-quota/src/rate_limiter.rs:1422-1460.
- ip_cooldown_until, acct_frozen: `IpCooldownEntry` (`note_ip_cooldown`, `check_ip`, HTTP `429`)
  and `AccountCooldownEntry` (`freeze_account`, `check_account`, HTTP `418`),
  crates/tickwire-quota/src/rate_limiter.rs:1117-1296,
  crates/tickwire-binance-quota/src/lib.rs:1578-1615.
- rollback_exact, local_reject_preserved: step-by-step witness flags for transactional `allow`
  reverse-order `put_back` unwinding (`WindowUndo::{Fixed, RollingCoalesced, RollingPushed}`) and
  pre-admission local `Account::admit` rejection non-burn,
  crates/tickwire-quota/src/rate_limiter.rs:1338-1409,
  crates/tickwire-quota/src/rate_limiter.rs:1847-2232,
  crates/tickwire-binance-quota/src/lib.rs:1163-1576,
  crates/tickwire-binance-account/src/rest.rs:509-685,
  crates/tickwire-binance-account/src/ws/owner.rs:259-360,
  crates/tickwire-spot-execution/src/session.rs:486-552,
  crates/tickwire-spot-execution/src/session.rs:711-844.
*)

ASSUME
    /\ Ips # {}
    /\ AcctOrderCap >= 1
    /\ SymCancelCap >= 1
    /\ IpWeightCap >= 2
    /\ RingCap >= 1
    /\ FixedInterval >= 1
    /\ RollingInterval >= 1
    /\ CooldownTicks >= 1
    /\ MaxClock >= 1
    /\ ReportedSet # {}
    /\ AllowAccountBan \in BOOLEAN

VARIABLES
    clock,
    acct_order_epoch,
    acct_order_used,
    acct_order_warn,
    sym_cancel_epoch,
    sym_cancel_used,
    ip_entries,
    ip_peak,
    ip_warn,
    ip_cooldown_until,
    acct_frozen,
    rollback_exact,
    local_reject_preserved

vars == <<
    clock, acct_order_epoch, acct_order_used, acct_order_warn,
    sym_cancel_epoch, sym_cancel_used, ip_entries, ip_peak, ip_warn,
    ip_cooldown_until, acct_frozen, rollback_exact, local_reject_preserved
>>

MaxEpoch == MaxClock \div FixedInterval
MaxCooldownUntil == MaxClock + CooldownTicks
BoundedSeq(S, k) == UNION {[1..n -> S] : n \in 0..k}
RollingEntryDomain == [ts : 0..MaxClock, units : 1..IpWeightCap]

Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b
MinIp(S) == CHOOSE x \in S : \A y \in S : x <= y

RECURSIVE EvictExpired(_, _)
EvictExpired(entries, now) ==
    IF Len(entries) = 0 THEN <<>>
    ELSE IF now - Head(entries).ts >= RollingInterval
         THEN EvictExpired(Tail(entries), now)
         ELSE entries

RECURSIVE SumEntries(_)
SumEntries(entries) ==
    IF Len(entries) = 0 THEN 0
    ELSE Head(entries).units + SumEntries(Tail(entries))

RECURSIVE ScanRetry(_, _, _, _, _)
ScanRetry(entries, idx, freed, need, now) ==
    IF idx > Len(entries) THEN RollingInterval
    ELSE
        LET new_freed == freed + entries[idx].units IN
        IF new_freed >= need
        THEN (entries[idx].ts + RollingInterval) - now
        ELSE ScanRetry(entries, idx + 1, new_freed, need, now)

\* Exact transcription of RollingWindow::retry_after_for_units
\* (crates/tickwire-quota/src/rate_limiter.rs:693-717).
RetryAfterForUnits(eff_entries, limit, eff_used, req_total, now) ==
    IF req_total > limit THEN RollingInterval
    ELSE
        LET need == IF eff_used + req_total > limit THEN (eff_used + req_total) - limit ELSE 0 IN
        ScanRetry(eff_entries, 1, 0, need, now)

FixedUsedAt(epoch, used, now) ==
    IF epoch = (now \div FixedInterval) THEN used ELSE 0

\* Exact transcription of FixedWindow::can_take_readonly
\* (crates/tickwire-quota/src/rate_limiter.rs:551-581).
FixedCanTake(limit, epoch, used, prior_u, req_u, now) ==
    LET ep == now \div FixedInterval
        cur == FixedUsedAt(epoch, used, now)
        used_before == cur + prior_u
    IN
    IF used_before + req_u > limit THEN
        [ok |-> FALSE,
         err_kind |-> "Exhausted",
         err_used |-> used_before,
         err_retry |-> ((ep + 1) * FixedInterval) - now]
    ELSE
        [ok |-> TRUE, err_kind |-> "None", err_used |-> 0, err_retry |-> 0]

\* Exact transcription of RollingWindow::can_take_readonly
\* (crates/tickwire-quota/src/rate_limiter.rs:720-769).
RollingCanTake(entries, prior_u, req_u, now) ==
    LET eff == EvictExpired(entries, now)
        eff_len == Len(eff)
        eff_used == SumEntries(eff)
        used_before == eff_used + prior_u
        total_u == prior_u + req_u
        coalesces == (prior_u > 0) \/ (eff_len > 0 /\ eff[eff_len].ts = now)
    IN
    IF used_before + req_u > IpWeightCap THEN
        [ok |-> FALSE,
         err_kind |-> "Exhausted",
         err_used |-> used_before,
         err_retry |-> IF req_u > IpWeightCap
                       THEN RollingInterval
                       ELSE RetryAfterForUnits(eff, IpWeightCap, eff_used, total_u, now)]
    ELSE IF ~coalesces /\ eff_len = RingCap THEN
        [ok |-> FALSE,
         err_kind |-> "Exhausted",
         err_used |-> used_before,
         err_retry |-> RollingInterval - (now - eff[1].ts)]
    ELSE
        [ok |-> TRUE, err_kind |-> "None", err_used |-> 0, err_retry |-> 0]

\* Spend record: [kind |-> "AcctOrder" | "SymCancel" | "IpWeight", ip |-> 0 | ip, units |-> 1..2]
PrecheckSpends(spends, now) ==
    IF acct_frozen THEN
        [ok |-> FALSE, err_kind |-> "Frozen", err_used |-> 0, err_retry |-> 0]
    ELSE
        LET bad_ips == {i \in 1..Len(spends) :
                spends[i].ip # 0 /\ now < ip_cooldown_until[spends[i].ip]}
        IN
        IF bad_ips # {} THEN
            LET first_idx == CHOOSE i \in bad_ips : \A j \in bad_ips : i <= j
                bip == spends[first_idx].ip
            IN
            [ok |-> FALSE,
             err_kind |-> "IpCooldown",
             err_used |-> 0,
             err_retry |-> ip_cooldown_until[bip] - now]
        ELSE
            [ok |-> TRUE, err_kind |-> "None", err_used |-> 0, err_retry |-> 0]

PriorUnitsBefore(spends, idx) ==
    LET matching == {j \in 1..(idx - 1) :
            spends[j].kind = spends[idx].kind /\ spends[j].ip = spends[idx].ip}
        RECURSIVE SumIdx(_)
        SumIdx(S) ==
            IF S = {} THEN 0
            ELSE LET k == CHOOSE x \in S : TRUE IN spends[k].units + SumIdx(S \ {k})
    IN SumIdx(matching)

RECURSIVE CanAffordLoop(_, _, _)
CanAffordLoop(spends, idx, now) ==
    IF idx > Len(spends) THEN
        [ok |-> TRUE, err_kind |-> "None", err_used |-> 0, err_retry |-> 0]
    ELSE
        LET sp == spends[idx]
            prior_u == PriorUnitsBefore(spends, idx)
            step ==
                IF sp.kind = "AcctOrder" THEN
                    FixedCanTake(AcctOrderCap, acct_order_epoch, acct_order_used, prior_u, sp.units, now)
                ELSE IF sp.kind = "SymCancel" THEN
                    FixedCanTake(SymCancelCap, sym_cancel_epoch, sym_cancel_used, prior_u, sp.units, now)
                ELSE
                    RollingCanTake(ip_entries[sp.ip], prior_u, sp.units, now)
        IN
        IF ~step.ok THEN step
        ELSE CanAffordLoop(spends, idx + 1, now)

\* Read-only probe RateLimiter::can_afford (crates/tickwire-quota/src/rate_limiter.rs:1338-1367).
CanAffordEval(spends, now) ==
    LET pre == PrecheckSpends(spends, now) IN
    IF ~pre.ok THEN pre
    ELSE CanAffordLoop(spends, 1, now)

\* Mutable window state bundle used by transactional AllowEval + reverse put_back unwinding.
CurrentWinState ==
    [ao_ep |-> acct_order_epoch,
     ao_u  |-> acct_order_used,
     sc_ep |-> sym_cancel_epoch,
     sc_u  |-> sym_cancel_used,
     ips   |-> ip_entries]

\* Effective quota equivalence at `now`: every fixed window's active usage and every rolling
\* window's unexpired `(ts, units)` ring sequence match 100% before and after a rejected `allow`.
EffectiveWinEq(ws1, ws2, now) ==
    /\ FixedUsedAt(ws1.ao_ep, ws1.ao_u, now) = FixedUsedAt(ws2.ao_ep, ws2.ao_u, now)
    /\ FixedUsedAt(ws1.sc_ep, ws1.sc_u, now) = FixedUsedAt(ws2.sc_ep, ws2.sc_u, now)
    /\ \A ip \in Ips : EvictExpired(ws1.ips[ip], now) = EvictExpired(ws2.ips[ip], now)

ApplyOneUndo(ws, urec) ==
    IF urec.kind = "AcctOrder" THEN
        [ws EXCEPT !.ao_u = urec.prev_used]
    ELSE IF urec.kind = "SymCancel" THEN
        [ws EXCEPT !.sc_u = urec.prev_used]
    ELSE IF urec.undo_mode = "Coalesced" THEN
        LET cur == ws.ips[urec.ip]
            last_i == Len(cur)
            updated == [cur EXCEPT ![last_i].units = @ - urec.units]
        IN [ws EXCEPT !.ips[urec.ip] = updated]
    ELSE
        LET cur == ws.ips[urec.ip]
            popped == SubSeq(cur, 1, Len(cur) - 1)
        IN [ws EXCEPT !.ips[urec.ip] = popped]

RECURSIVE UnwindUndos(_, _)
UnwindUndos(ws, undos) ==
    IF Len(undos) = 0 THEN ws
    ELSE
        LET top == undos[Len(undos)]
            rest == SubSeq(undos, 1, Len(undos) - 1)
        IN UnwindUndos(ApplyOneUndo(ws, top), rest)

TryTakeStep(ws, sp, now) ==
    IF sp.kind = "AcctOrder" THEN
        LET ep == now \div FixedInterval
            cur == FixedUsedAt(ws.ao_ep, ws.ao_u, now)
            advanced == [ws EXCEPT !.ao_ep = ep, !.ao_u = cur]
        IN
        IF cur + sp.units > AcctOrderCap THEN
            [ok |-> FALSE, ws |-> advanced,
             err_kind |-> "Exhausted", err_used |-> cur,
             err_retry |-> ((ep + 1) * FixedInterval) - now]
        ELSE
            [ok |-> TRUE,
             ws |-> [advanced EXCEPT !.ao_u = cur + sp.units],
             undo |-> [kind |-> "AcctOrder", ip |-> 0, undo_mode |-> "Fixed",
                       prev_used |-> cur, units |-> 0]]
    ELSE IF sp.kind = "SymCancel" THEN
        LET ep == now \div FixedInterval
            cur == FixedUsedAt(ws.sc_ep, ws.sc_u, now)
            advanced == [ws EXCEPT !.sc_ep = ep, !.sc_u = cur]
        IN
        IF cur + sp.units > SymCancelCap THEN
            [ok |-> FALSE, ws |-> advanced,
             err_kind |-> "Exhausted", err_used |-> cur,
             err_retry |-> ((ep + 1) * FixedInterval) - now]
        ELSE
            [ok |-> TRUE,
             ws |-> [advanced EXCEPT !.sc_u = cur + sp.units],
             undo |-> [kind |-> "SymCancel", ip |-> 0, undo_mode |-> "Fixed",
                       prev_used |-> cur, units |-> 0]]
    ELSE
        LET eff == EvictExpired(ws.ips[sp.ip], now)
            eff_len == Len(eff)
            eff_used == SumEntries(eff)
            evicted_ws == [ws EXCEPT !.ips[sp.ip] = eff]
        IN
        IF eff_used + sp.units > IpWeightCap THEN
            [ok |-> FALSE, ws |-> evicted_ws,
             err_kind |-> "Exhausted", err_used |-> eff_used,
             err_retry |-> RetryAfterForUnits(eff, IpWeightCap, eff_used, sp.units, now)]
        ELSE IF eff_len > 0 /\ eff[eff_len].ts = now THEN
            LET merged == [eff EXCEPT ![eff_len].units = @ + sp.units] IN
            [ok |-> TRUE,
             ws |-> [evicted_ws EXCEPT !.ips[sp.ip] = merged],
             undo |-> [kind |-> "IpWeight", ip |-> sp.ip, undo_mode |-> "Coalesced",
                       prev_used |-> 0, units |-> sp.units]]
        ELSE IF eff_len = RingCap THEN
            [ok |-> FALSE, ws |-> evicted_ws,
             err_kind |-> "Exhausted", err_used |-> eff_used,
             err_retry |-> RollingInterval - (now - eff[1].ts)]
        ELSE
            LET pushed == Append(eff, [ts |-> now, units |-> sp.units]) IN
            [ok |-> TRUE,
             ws |-> [evicted_ws EXCEPT !.ips[sp.ip] = pushed],
             undo |-> [kind |-> "IpWeight", ip |-> sp.ip, undo_mode |-> "Pushed",
                       prev_used |-> 0, units |-> sp.units]]

RECURSIVE AllowLoop(_, _, _, _, _)
AllowLoop(ws, spends, idx, undos, now) ==
    IF idx > Len(spends) THEN
        [ok |-> TRUE, ws |-> ws, err_kind |-> "None", err_used |-> 0, err_retry |-> 0]
    ELSE
        LET step == TryTakeStep(ws, spends[idx], now) IN
        IF step.ok THEN
            AllowLoop(step.ws, spends, idx + 1, Append(undos, step.undo), now)
        ELSE
            LET unwound == UnwindUndos(step.ws, undos) IN
            [ok |-> FALSE,
             ws |-> unwound,
             err_kind |-> step.err_kind,
             err_used |-> step.err_used,
             err_retry |-> step.err_retry]

\* Transactional RateLimiter::allow (crates/tickwire-quota/src/rate_limiter.rs:1385-1409).
AllowEval(spends, now) ==
    LET pre == PrecheckSpends(spends, now) IN
    IF ~pre.ok THEN
        [ok |-> FALSE, ws |-> CurrentWinState,
         err_kind |-> pre.err_kind, err_used |-> pre.err_used, err_retry |-> pre.err_retry]
    ELSE
        AllowLoop(CurrentWinState, spends, 1, <<>>, now)

PlaceSpends(ip) ==
    <<[kind |-> "AcctOrder", ip |-> 0, units |-> 1],
      [kind |-> "IpWeight",  ip |-> ip, units |-> 1]>>

CancelSpends(ip) ==
    <<[kind |-> "SymCancel", ip |-> 0, units |-> 1],
      [kind |-> "IpWeight",  ip |-> ip, units |-> 1]>>

QuerySpends(ip) ==
    <<[kind |-> "IpWeight", ip |-> ip, units |-> 1]>>

\* Two spends targeting the SAME rolling IP window in one slice
\* (crates/tickwire-quota/src/rate_limiter.rs:1338-1367).
DupBatchSpends(ip) ==
    <<[kind |-> "IpWeight", ip |-> ip, units |-> 1],
      [kind |-> "IpWeight", ip |-> ip, units |-> 2]>>

\* Multi-window spend where spend 1 hits IpWeight(ip) and spend 2 hits AcctOrder, exercising
\* RollingPushed / RollingCoalesced put_back when AcctOrder is exhausted!
MultiWinDupSpends(ip) ==
    <<[kind |-> "IpWeight",  ip |-> ip, units |-> 1],
      [kind |-> "AcctOrder", ip |-> 0,  units |-> 1]>>

\* Route filter mirroring pick_pool_slot_filtered + can_afford_quota_on_ip
\* (crates/tickwire-binance-account/src/rest.rs:581-649).
AffordableIpsFor(spends_fn(_)) ==
    {ip \in Ips : CanAffordEval(spends_fn(ip), clock).ok}

Init ==
    /\ clock = 0
    /\ acct_order_epoch = -1
    /\ acct_order_used = 0
    /\ acct_order_warn = FALSE
    /\ sym_cancel_epoch = -1
    /\ sym_cancel_used = 0
    /\ ip_entries = [ip \in Ips |-> <<>>]
    /\ ip_peak = [ip \in Ips |-> 0]
    /\ ip_warn = [ip \in Ips |-> FALSE]
    /\ ip_cooldown_until = [ip \in Ips |-> 0]
    /\ acct_frozen = FALSE
    /\ rollback_exact = TRUE
    /\ local_reject_preserved = TRUE

\* 1. Place order routed to an affordable IP, admitted by Account::admit, then debited via allow_place.
PlaceOrderAdmitted(ip) ==
    LET elig == AffordableIpsFor(PlaceSpends)
        res == AllowEval(PlaceSpends(ip), clock)
    IN
    /\ ip \in elig
    /\ res.ok
    /\ acct_order_epoch' = res.ws.ao_ep
    /\ acct_order_used' = res.ws.ao_u
    /\ sym_cancel_epoch' = res.ws.sc_ep
    /\ sym_cancel_used' = res.ws.sc_u
    /\ ip_entries' = res.ws.ips
    /\ ip_peak' = [p \in Ips |-> IF SumEntries(res.ws.ips[p]) = 0 THEN 0 ELSE ip_peak[p]]
    /\ ip_warn' = [p \in Ips |-> IF SumEntries(res.ws.ips[p]) = 0 THEN FALSE ELSE ip_warn[p]]
    /\ UNCHANGED <<
        clock, acct_order_warn, ip_cooldown_until, acct_frozen,
        rollback_exact, local_reject_preserved
       >>

\* 2. Pre-admission local Account::admit rejection (e.g. InsufficientAsset / OrderInDoubt):
\* passes can_afford_place precheck, rejects in Account::admit BEFORE allow_place, burning ZERO quota!
PlaceOrderLocalReject(ip) ==
    LET before == CurrentWinState IN
    /\ CanAffordEval(PlaceSpends(ip), clock).ok
    /\ local_reject_preserved' = (local_reject_preserved /\ (CurrentWinState = before))
    /\ UNCHANGED <<
        clock, acct_order_epoch, acct_order_used, acct_order_warn,
        sym_cancel_epoch, sym_cancel_used, ip_entries, ip_peak, ip_warn,
        ip_cooldown_until, acct_frozen, rollback_exact
       >>

\* 3. Transactional multi-window rollback probe: spend 1 succeeds (Fixed AcctOrder or Rolling IpWeight)
\* and spend 2 rejects (Exhausted), unwinding spend 1 via put_back!
TransactionalRollbackProbe(ip) ==
    LET s1 == PlaceSpends(ip)
        s2 == MultiWinDupSpends(ip)
        spends == IF ~AllowEval(s1, clock).ok THEN s1 ELSE s2
        res == AllowEval(spends, clock)
    IN
    /\ ~acct_frozen
    /\ clock >= ip_cooldown_until[ip]
    /\ (~AllowEval(s1, clock).ok \/ ~AllowEval(s2, clock).ok)
    /\ ~res.ok
    /\ acct_order_epoch' = res.ws.ao_ep
    /\ acct_order_used' = res.ws.ao_u
    /\ sym_cancel_epoch' = res.ws.sc_ep
    /\ sym_cancel_used' = res.ws.sc_u
    /\ ip_entries' = res.ws.ips
    /\ rollback_exact' = (rollback_exact /\ EffectiveWinEq(res.ws, CurrentWinState, clock))
    /\ UNCHANGED <<
        clock, acct_order_warn, ip_peak, ip_warn,
        ip_cooldown_until, acct_frozen, local_reject_preserved
       >>

\* 4. Cancel routed to healthy IP: uses SymCancel + IpWeight(ip), completely isolated from AcctOrder!
SubmitCancel(ip) ==
    LET elig == AffordableIpsFor(CancelSpends)
        res == AllowEval(CancelSpends(ip), clock)
    IN
    /\ ip \in elig
    /\ res.ok
    /\ acct_order_epoch' = res.ws.ao_ep
    /\ acct_order_used' = res.ws.ao_u
    /\ sym_cancel_epoch' = res.ws.sc_ep
    /\ sym_cancel_used' = res.ws.sc_u
    /\ ip_entries' = res.ws.ips
    /\ UNCHANGED <<
        clock, acct_order_warn, ip_peak, ip_warn,
        ip_cooldown_until, acct_frozen, rollback_exact, local_reject_preserved
       >>

\* 5. Per-order QueryDesk / recovery read routed to healthy IP: uses IpWeight(ip) only!
SubmitQuery(ip) ==
    LET elig == AffordableIpsFor(QuerySpends)
        res == AllowEval(QuerySpends(ip), clock)
    IN
    /\ ip \in elig
    /\ res.ok
    /\ ip_entries' = res.ws.ips
    /\ UNCHANGED <<
        clock, acct_order_epoch, acct_order_used, acct_order_warn,
        sym_cancel_epoch, sym_cancel_used, ip_peak, ip_warn,
        ip_cooldown_until, acct_frozen, rollback_exact, local_reject_preserved
       >>

\* 6. Duplicate-RateLimitId batch spend on IpWeight(ip): tests both successful coalescing of
\* [Spend(ip, 1), Spend(ip, 2)] and mid-slice RollingPushed/RollingCoalesced put_back rollback!
DuplicateIdBatchSpend(ip) ==
    LET spends == DupBatchSpends(ip)
        res == AllowEval(spends, clock)
    IN
    /\ ~acct_frozen
    /\ clock >= ip_cooldown_until[ip]
    /\ ip_entries' = res.ws.ips
    /\ rollback_exact' = (rollback_exact /\ (res.ok \/ EffectiveWinEq(res.ws, CurrentWinState, clock)))
    /\ UNCHANGED <<
        clock, acct_order_epoch, acct_order_used, acct_order_warn,
        sym_cancel_epoch, sym_cancel_used, ip_peak, ip_warn,
        ip_cooldown_until, acct_frozen, local_reject_preserved
       >>

\* 7. Exchange header reconciliation (X-MBX-ORDER-COUNT / X-MBX-USED-WEIGHT,
\* crates/tickwire-quota/src/rate_limiter.rs:1422-1460):
\* conservatively raises fixed used to max(used, min(reported, limit)) and records rolling peak + 80% warn.
ReconcileExchangeHeader(ip, reported) ==
    LET ep == clock \div FixedInterval
        cur_ao == FixedUsedAt(acct_order_epoch, acct_order_used, clock)
        clamped_ao == Min(reported, AcctOrderCap)
        ao_warn == (reported * 100 >= AcctOrderCap * 80)
        eff_ip == EvictExpired(ip_entries[ip], clock)
        used_ip == SumEntries(eff_ip)
        base_peak == IF used_ip = 0 THEN 0 ELSE ip_peak[ip]
        clamped_ip == Min(reported, IpWeightCap)
        ip_w == (reported * 100 >= IpWeightCap * 80)
    IN
    /\ acct_order_epoch' = ep
    /\ acct_order_used' = Max(cur_ao, clamped_ao)
    /\ acct_order_warn' = ao_warn
    /\ ip_entries' = [ip_entries EXCEPT ![ip] = eff_ip]
    /\ ip_peak' = [ip_peak EXCEPT ![ip] = Max(base_peak, clamped_ip)]
    /\ ip_warn' = [ip_warn EXCEPT ![ip] = ip_w]
    /\ UNCHANGED <<
        clock, sym_cancel_epoch, sym_cancel_used,
        ip_cooldown_until, acct_frozen, rollback_exact, local_reject_preserved
       >>

\* 8. HTTP 429 rate-limit response on `ip` sets IpCooldown on `ip` ONLY, leaving sibling IPs open!
ObserveHttp429(ip) ==
    LET until_t == clock + CooldownTicks IN
    /\ ~acct_frozen
    /\ ip_cooldown_until[ip] < until_t
    /\ ip_cooldown_until' = [ip_cooldown_until EXCEPT ![ip] = Max(@, until_t)]
    /\ UNCHANGED <<
        clock, acct_order_epoch, acct_order_used, acct_order_warn,
        sym_cancel_epoch, sym_cancel_used, ip_entries, ip_peak, ip_warn,
        acct_frozen, rollback_exact, local_reject_preserved
       >>

\* 9. HTTP 418 IP/account ban freezes the account across ALL local IPs
\* (crates/tickwire-quota/src/rate_limiter.rs:1188-1225).
ObserveHttp418 ==
    /\ AllowAccountBan
    /\ ~acct_frozen
    /\ acct_frozen' = TRUE
    /\ UNCHANGED <<
        clock, acct_order_epoch, acct_order_used, acct_order_warn,
        sym_cancel_epoch, sym_cancel_used, ip_entries, ip_peak, ip_warn,
        ip_cooldown_until, rollback_exact, local_reject_preserved
       >>

\* 10. Monotonic clock step: triggers fixed-epoch rollover, sliding rolling-window eviction, and 429 expiry.
AdvanceClock ==
    /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ UNCHANGED <<
        acct_order_epoch, acct_order_used, acct_order_warn,
        sym_cancel_epoch, sym_cancel_used, ip_entries, ip_peak, ip_warn,
        ip_cooldown_until, acct_frozen, rollback_exact, local_reject_preserved
       >>

Next ==
    \/ \E ip \in Ips : PlaceOrderAdmitted(ip)
    \/ \E ip \in Ips : PlaceOrderLocalReject(ip)
    \/ \E ip \in Ips : TransactionalRollbackProbe(ip)
    \/ \E ip \in Ips : SubmitCancel(ip)
    \/ \E ip \in Ips : SubmitQuery(ip)
    \/ \E ip \in Ips : DuplicateIdBatchSpend(ip)
    \/ \E ip \in Ips, r \in ReportedSet : ReconcileExchangeHeader(ip, r)
    \/ \E ip \in Ips : ObserveHttp429(ip)
    \/ ObserveHttp418
    \/ AdvanceClock

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

TypeOK ==
    /\ clock \in 0..MaxClock
    /\ acct_order_epoch \in -1..MaxEpoch
    /\ acct_order_used \in 0..AcctOrderCap
    /\ acct_order_warn \in BOOLEAN
    /\ sym_cancel_epoch \in -1..MaxEpoch
    /\ sym_cancel_used \in 0..SymCancelCap
    /\ DOMAIN ip_entries = Ips
    /\ \A ip \in Ips :
        /\ Len(ip_entries[ip]) \in 0..RingCap
        /\ \A k \in 1..Len(ip_entries[ip]) : ip_entries[ip][k] \in RollingEntryDomain
    /\ ip_peak \in [Ips -> 0..IpWeightCap]
    /\ ip_warn \in [Ips -> BOOLEAN]
    /\ ip_cooldown_until \in [Ips -> 0..MaxCooldownUntil]
    /\ acct_frozen \in BOOLEAN
    /\ rollback_exact \in BOOLEAN
    /\ local_reject_preserved \in BOOLEAN

WindowCapsRespected ==
    /\ acct_order_used <= AcctOrderCap
    /\ sym_cancel_used <= SymCancelCap
    /\ FixedUsedAt(acct_order_epoch, acct_order_used, clock) <= AcctOrderCap
    /\ FixedUsedAt(sym_cancel_epoch, sym_cancel_used, clock) <= SymCancelCap
    /\ \A ip \in Ips :
        /\ Len(ip_entries[ip]) <= RingCap
        /\ SumEntries(ip_entries[ip]) <= IpWeightCap
        /\ SumEntries(EvictExpired(ip_entries[ip], clock)) <= IpWeightCap
        /\ ip_peak[ip] <= IpWeightCap

TransactionalRollbackExact ==
    /\ rollback_exact
    /\ local_reject_preserved
    /\ \A ip \in Ips :
        /\ (~AllowEval(PlaceSpends(ip), clock).ok)
            => EffectiveWinEq(AllowEval(PlaceSpends(ip), clock).ws, CurrentWinState, clock)
        /\ (~AllowEval(CancelSpends(ip), clock).ok)
            => EffectiveWinEq(AllowEval(CancelSpends(ip), clock).ws, CurrentWinState, clock)
        /\ (~AllowEval(DupBatchSpends(ip), clock).ok)
            => EffectiveWinEq(AllowEval(DupBatchSpends(ip), clock).ws, CurrentWinState, clock)
        /\ (~AllowEval(MultiWinDupSpends(ip), clock).ok)
            => EffectiveWinEq(AllowEval(MultiWinDupSpends(ip), clock).ws, CurrentWinState, clock)

AgreeOn(spends) ==
    LET ca == CanAffordEval(spends, clock)
        al == AllowEval(spends, clock)
    IN
    /\ ca.ok = al.ok
    /\ ca.err_kind = al.err_kind
    /\ ca.err_used = al.err_used
    /\ ca.err_retry = al.err_retry

CanAffordMatchesAllow ==
    \A ip \in Ips :
        /\ AgreeOn(PlaceSpends(ip))
        /\ AgreeOn(CancelSpends(ip))
        /\ AgreeOn(QuerySpends(ip))
        /\ AgreeOn(DupBatchSpends(ip))
        /\ AgreeOn(MultiWinDupSpends(ip))

CancelAndQueryIsolatedFromOrderCap ==
    (~acct_frozen /\ FixedUsedAt(acct_order_epoch, acct_order_used, clock) = AcctOrderCap) =>
        \A ip \in Ips :
            (clock >= ip_cooldown_until[ip] /\ RollingCanTake(ip_entries[ip], 0, 1, clock).ok) =>
                /\ CanAffordEval(QuerySpends(ip), clock).ok
                /\ AllowEval(QuerySpends(ip), clock).ok
                /\ (FixedUsedAt(sym_cancel_epoch, sym_cancel_used, clock) < SymCancelCap
                    => (CanAffordEval(CancelSpends(ip), clock).ok
                        /\ AllowEval(CancelSpends(ip), clock).ok))

PerIpCooldownIsolation ==
    /\ acct_frozen =>
        \A ip \in Ips :
            /\ ~CanAffordEval(PlaceSpends(ip), clock).ok
            /\ CanAffordEval(PlaceSpends(ip), clock).err_kind = "Frozen"
            /\ ~CanAffordEval(CancelSpends(ip), clock).ok
            /\ ~CanAffordEval(QuerySpends(ip), clock).ok
    /\ (~acct_frozen) =>
        \A ip_bad, ip_good \in Ips :
            (ip_bad # ip_good
             /\ clock < ip_cooldown_until[ip_bad]
             /\ clock >= ip_cooldown_until[ip_good]) =>
                /\ CanAffordEval(PlaceSpends(ip_bad), clock).err_kind = "IpCooldown"
                /\ CanAffordEval(QuerySpends(ip_bad), clock).err_kind = "IpCooldown"
                /\ (CanAffordEval(QuerySpends(ip_good), clock).ok
                    <=> RollingCanTake(ip_entries[ip_good], 0, 1, clock).ok)
                /\ ((FixedUsedAt(acct_order_epoch, acct_order_used, clock) < AcctOrderCap
                     /\ RollingCanTake(ip_entries[ip_good], 0, 1, clock).ok)
                    => ip_good \in AffordableIpsFor(PlaceSpends))

=============================================================================
