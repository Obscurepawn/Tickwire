---------------------------- MODULE L1_WindowedPool ----------------------------
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS NumOrders, MaxOrders, NumMgmt, MaxMgmt, Window, MaxTime, MaxRecords

(*
Code-faithful model of active-protected time-windowed pools (WindowedTable),
coupled Account + AccountGateway order/management/fact retention, separate
management headroom reclamation, and AccountJournal prefix compaction.

Variable <-> Rust source mapping and state/watermark denotations:
- now: monotonic clock step in 1..MaxTime representing now_mono_ns / evaluated_at
  and Account.last_mono_ns,
  crates/tickwire-account/src/pool.rs:47-56,
  crates/tickwire-account/src/lib.rs:788-796,
  crates/tickwire-account-gateway/src/lib.rs:629-640.
- acct_orders: Account.orders table over OrderIntents tracking present, filled,
  term_cum (terminal_cumulative), terminal, aborted, unknown, fees_closed,
  amend_inflight, amend_done, and retired_at (retired_mono_ns),
  crates/tickwire-account/src/pool.rs:28-40,
  crates/tickwire-account/src/pool.rs:363-493,
  crates/tickwire-account/src/lib.rs:139-167,
  crates/tickwire-account/src/lib.rs:594-655.
- acct_adm, adm_retired_at: Account.admissions state ("absent", "active", "retired")
  and Admission.retired_mono_ns separating order-creating caps (MAX_ORDERS) from
  management headroom (MAX_MANAGEMENT_ADMISSIONS + eager reclaim),
  crates/tickwire-account/src/types.rs:7-12,
  crates/tickwire-account/src/lib.rs:520-592,
  crates/tickwire-account/src/lib.rs:755-911,
  crates/tickwire-account/src/admission.rs:78-104,
  crates/tickwire-account/src/admission.rs:169-176.
- facts: Account.facts FactEntry.recorded_mono_ns per order's trade execution
  (0 when absent), enforcing trade-ID deduplication and facts_for_order_expired,
  crates/tickwire-account/src/events.rs:88-93,
  crates/tickwire-account/src/lib.rs:564-576,
  crates/tickwire-account/src/lib.rs:740-753,
  crates/tickwire-account/src/lib.rs:841-855.
- pending_send, gw_status, gw_eval_at, gw_completed_at: AccountGateway.pending,
  AccountGateway.entries status/evaluated_at, and AccountGateway.completed_mono_ns
  across submit, apply_report(Flushed), abort, evict_retired_entries, and
  reclaim_one_retired_management_entry,
  crates/tickwire-account-gateway/src/lib.rs:569-662,
  crates/tickwire-account-gateway/src/lib.rs:678-809,
  crates/tickwire-account-gateway/src/lib.rs:1509-1559.
- evicted_prefix, journal_records, journal_slots, origin_present, frontier_pos:
  AccountJournal.evicted_prefix, AccountJournal.records suffix, AdmissionSlot
  ("absent", "in_records", "retained"), AccountJournal.admission_origins keys,
  and rolling HistoryFrontier / position() watermark across compact_before and
  maybe_compact_journal,
  crates/tickwire-account-gateway/src/lib.rs:664-677,
  crates/tickwire-account-journal/src/lib.rs:173-210,
  crates/tickwire-account-journal/src/lib.rs:508-652,
  crates/tickwire-account-journal/src/lib.rs:725-794,
  crates/tickwire-account-journal/src/lib.rs:821-823.
- ever_orders, ever_mgmt, ghost_orders, ghost_retire_at, admit_count,
  fact_first_at, trade_book_count, overflow_reclaimed, total_appends:
  ghost specification observers (never read by transition guards) used strictly
  to verify ActiveNeverEvicted, WindowDedupPreserved, and FrontierContinuity.
*)

VARIABLES
    now,
    acct_orders,
    acct_adm,
    adm_retired_at,
    facts,
    pending_send,
    gw_status,
    gw_eval_at,
    gw_completed_at,
    evicted_prefix,
    journal_records,
    journal_slots,
    origin_present,
    frontier_pos,
    ever_orders,
    ever_mgmt,
    ghost_orders,
    ghost_retire_at,
    admit_count,
    fact_first_at,
    trade_book_count,
    overflow_reclaimed,
    total_appends

vars == <<
    now, acct_orders, acct_adm, adm_retired_at, facts, pending_send,
    gw_status, gw_eval_at, gw_completed_at, evicted_prefix, journal_records,
    journal_slots, origin_present, frontier_pos, ever_orders, ever_mgmt,
    ghost_orders, ghost_retire_at, admit_count, fact_first_at,
    trade_book_count, overflow_reclaimed, total_appends
>>

OrderIntents == 1..NumOrders
MgmtIntents == (NumOrders + 1)..(NumOrders + NumMgmt)
AllIntents == 1..(NumOrders + NumMgmt)
MaxAppends == (8 * NumOrders) + (3 * NumMgmt)
HalfRecords == MaxRecords \div 2

AdmStates == {"absent", "active", "retired"}
GwStatuses == {"absent", "Queued", "Flushed", "Aborted"}
SlotStates == {"absent", "in_records", "retained"}

NoneOrder == [
    present |-> FALSE,
    filled |-> 0,
    term_cum |-> 0,
    terminal |-> FALSE,
    aborted |-> FALSE,
    unknown |-> FALSE,
    fees_closed |-> FALSE,
    amend_inflight |-> FALSE,
    amend_done |-> FALSE,
    retired_at |-> 0
]

OrderValues == [
    present: BOOLEAN,
    filled: 0..1,
    term_cum: 0..1,
    terminal: BOOLEAN,
    aborted: BOOLEAN,
    unknown: BOOLEAN,
    fees_closed: BOOLEAN,
    amend_inflight: BOOLEAN,
    amend_done: BOOLEAN,
    retired_at: 0..MaxTime
]

JournalElemValues == [
    seq: 1..MaxAppends,
    admit_intent: 0..(NumOrders + NumMgmt)
]

Max(a, b) == IF a >= b THEN a ELSE b

\* WindowedTable::is_expired(Some(retired_at), now_mono_ns, Some(window_ns)).
\* crates/tickwire-account/src/pool.rs:47-56.
IsExpired(stamp, t) ==
    /\ stamp > 0
    /\ t >= stamp + Window

\* Order::remaining() == 0 and Account::is_order_retirable().
\* crates/tickwire-account/src/lib.rs:142-167, crates/tickwire-account/src/lib.rs:594-603.
OrderRemainingZero(o) ==
    \/ o.aborted
    \/ (o.terminal /\ o.term_cum = o.filled)

OrderRetirable(o) ==
    /\ o.present
    /\ ~o.unknown
    /\ ~o.amend_inflight
    /\ (o.aborted \/ (o.terminal /\ o.fees_closed /\ OrderRemainingZero(o)))

IsOpenOrder(orders_fn, k) ==
    /\ orders_fn[k].present
    /\ ~orders_fn[k].aborted
    /\ ~orders_fn[k].terminal

\* Account::is_intent_active and Account::intent_retired_mono_ns.
\* crates/tickwire-account/src/lib.rs:520-592.
IntentActive(orders_fn, adm_fn, i) ==
    IF i \in OrderIntents THEN
        orders_fn[i].present /\ ~OrderRetirable(orders_fn[i])
    ELSE
        adm_fn[i] = "active"

IntentRetireStamp(orders_fn, facts_fn, adm_fn, adm_ret_fn, i) ==
    IF IntentActive(orders_fn, adm_fn, i) THEN
        0
    ELSE IF i \in OrderIntents THEN
        IF ~orders_fn[i].present \/ orders_fn[i].retired_at = 0
        THEN 0
        ELSE Max(orders_fn[i].retired_at, facts_fn[i])
    ELSE
        IF adm_fn[i] = "retired" THEN adm_ret_fn[i] ELSE 0

\* AccountGateway::refresh_completed_entries.
\* crates/tickwire-account-gateway/src/lib.rs:569-595.
RefreshCompleted(orders_fn, facts_fn, adm_fn, adm_ret_fn, gw_st_fn, gw_ev_fn, gw_comp_fn, pend, t) ==
    [i \in AllIntents |->
        IF gw_st_fn[i] \in {"Aborted", "Flushed"}
           /\ pend # i
           /\ ~IntentActive(orders_fn, adm_fn, i)
        THEN IF gw_comp_fn[i] > 0
             THEN gw_comp_fn[i]
             ELSE Max(t, Max(gw_ev_fn[i], IntentRetireStamp(orders_fn, facts_fn, adm_fn, adm_ret_fn, i)))
        ELSE 0]

\* Coupled Account::evict_expired + AccountGateway::evict_retired_entries predicates.
\* crates/tickwire-account/src/lib.rs:788-911, crates/tickwire-account-gateway/src/lib.rs:629-662.
OrderEvictableAt(orders_fn, facts_fn, k, t) ==
    /\ orders_fn[k].present
    /\ OrderRetirable(orders_fn[k])
    /\ IsExpired(orders_fn[k].retired_at, t)
    /\ (facts_fn[k] = 0 \/ IsExpired(facts_fn[k], t))

MgmtEvictableAt(adm_fn, adm_ret_fn, m, t) ==
    /\ adm_fn[m] = "retired"
    /\ IsExpired(adm_ret_fn[m], t)

AfterEvictOrders(orders_fn, facts_fn, t) ==
    [k \in OrderIntents |->
        IF OrderEvictableAt(orders_fn, facts_fn, k, t) THEN NoneOrder ELSE orders_fn[k]]

AfterEvictFacts(orders_fn, facts_fn, t) ==
    [k \in OrderIntents |->
        IF OrderEvictableAt(orders_fn, facts_fn, k, t) THEN 0 ELSE facts_fn[k]]

AfterEvictAdm(orders_fn, facts_fn, adm_fn, adm_ret_fn, t) ==
    [i \in AllIntents |->
        IF i \in OrderIntents THEN
            IF OrderEvictableAt(orders_fn, facts_fn, i, t) THEN "absent" ELSE adm_fn[i]
        ELSE
            IF MgmtEvictableAt(adm_fn, adm_ret_fn, i, t) THEN "absent" ELSE adm_fn[i]]

AfterEvictAdmRet(orders_fn, facts_fn, adm_fn, adm_ret_fn, t) ==
    [i \in AllIntents |->
        IF i \in OrderIntents THEN
            IF OrderEvictableAt(orders_fn, facts_fn, i, t) THEN 0 ELSE adm_ret_fn[i]
        ELSE
            IF MgmtEvictableAt(adm_fn, adm_ret_fn, i, t) THEN 0 ELSE adm_ret_fn[i]]

AfterEvictGwStatus(orders_fn, facts_fn, adm_fn, adm_ret_fn, gw_st_fn, gw_ev_fn, gw_comp_fn, pend, t) ==
    LET o1 == AfterEvictOrders(orders_fn, facts_fn, t)
        f1 == AfterEvictFacts(orders_fn, facts_fn, t)
        a1 == AfterEvictAdm(orders_fn, facts_fn, adm_fn, adm_ret_fn, t)
        ar1 == AfterEvictAdmRet(orders_fn, facts_fn, adm_fn, adm_ret_fn, t)
        comp1 == RefreshCompleted(o1, f1, a1, ar1, gw_st_fn, gw_ev_fn, gw_comp_fn, pend, t)
    IN [i \in AllIntents |->
        IF gw_st_fn[i] \in {"Aborted", "Flushed"}
           /\ pend # i
           /\ ~IntentActive(o1, a1, i)
           /\ IsExpired(comp1[i], t)
        THEN "absent"
        ELSE gw_st_fn[i]]

AfterEvictGwComp(orders_fn, facts_fn, adm_fn, adm_ret_fn, gw_st_fn, gw_ev_fn, gw_comp_fn, pend, t) ==
    LET o1 == AfterEvictOrders(orders_fn, facts_fn, t)
        f1 == AfterEvictFacts(orders_fn, facts_fn, t)
        a1 == AfterEvictAdm(orders_fn, facts_fn, adm_fn, adm_ret_fn, t)
        ar1 == AfterEvictAdmRet(orders_fn, facts_fn, adm_fn, adm_ret_fn, t)
        comp1 == RefreshCompleted(o1, f1, a1, ar1, gw_st_fn, gw_ev_fn, gw_comp_fn, pend, t)
        st2 == AfterEvictGwStatus(orders_fn, facts_fn, adm_fn, adm_ret_fn, gw_st_fn, gw_ev_fn, gw_comp_fn, pend, t)
    IN [i \in AllIntents |-> IF st2[i] = "absent" THEN 0 ELSE comp1[i]]

\* Pool occupancy and eager management reclamation helpers.
\* crates/tickwire-account/src/lib.rs:755-783, crates/tickwire-account/src/admission.rs:94-104,
\* crates/tickwire-account/src/admission.rs:169-176, crates/tickwire-account-gateway/src/lib.rs:597-625.
OrderGatewayCount(gw_st_fn) ==
    Cardinality({k \in OrderIntents : gw_st_fn[k] # "absent"})

OrderAccountCount(orders_fn, adm_fn) ==
    Max(
        Cardinality({k \in OrderIntents : orders_fn[k].present}),
        Cardinality({k \in OrderIntents : adm_fn[k] # "absent"})
    )

MgmtGatewayCount(gw_st_fn) ==
    Cardinality({m \in MgmtIntents : gw_st_fn[m] # "absent"})

MgmtAccountCount(adm_fn) ==
    Cardinality({m \in MgmtIntents : adm_fn[m] # "absent"})

ReclaimableGatewayMgmt(gw_st_fn, adm_fn, pend) ==
    {m \in MgmtIntents :
        /\ gw_st_fn[m] \in {"Aborted", "Flushed"}
        /\ pend # m
        /\ adm_fn[m] # "active"}

ReclaimableAccountMgmt(adm_fn) ==
    {m \in MgmtIntents : adm_fn[m] = "retired"}

PickOldestGwMgmt(cands, gw_comp_fn, gw_ev_fn) ==
    CHOOSE m \in cands :
        \A n \in cands :
            \/ gw_comp_fn[m] < gw_comp_fn[n]
            \/ (gw_comp_fn[m] = gw_comp_fn[n] /\ gw_ev_fn[m] < gw_ev_fn[n])
            \/ (gw_comp_fn[m] = gw_comp_fn[n] /\ gw_ev_fn[m] = gw_ev_fn[n] /\ m <= n)

PickOldestAcctMgmt(cands, adm_ret_fn) ==
    CHOOSE m \in cands :
        \A n \in cands :
            \/ adm_ret_fn[m] < adm_ret_fn[n]
            \/ (adm_ret_fn[m] = adm_ret_fn[n] /\ m <= n)

\* AccountJournal::compact_before(keep_admissions, max_retained) pure state transformer.
\* crates/tickwire-account-journal/src/lib.rs:582-652.
KeepSet(adm_fn, gw_st_fn) ==
    {i \in AllIntents : adm_fn[i] # "absent" \/ gw_st_fn[i] # "absent"}

CompactPrefix(ev_pref, recs) ==
    IF Len(recs) <= HalfRecords
    THEN ev_pref
    ELSE ev_pref + (Len(recs) - HalfRecords)

CompactRecs(recs) ==
    IF Len(recs) <= HalfRecords
    THEN recs
    ELSE SubSeq(recs, (Len(recs) - HalfRecords) + 1, Len(recs))

CompactSlots(recs, slots_fn, keep_set) ==
    LET slots1 == [i \in AllIntents |->
            IF slots_fn[i] = "retained" /\ i \notin keep_set
            THEN "absent"
            ELSE slots_fn[i]]
    IN IF Len(recs) <= HalfRecords
       THEN slots1
       ELSE LET trim == Len(recs) - HalfRecords
            IN [i \in AllIntents |->
                IF \E j \in 1..trim : recs[j].admit_intent = i
                THEN IF i \in keep_set /\ slots1[i] = "in_records"
                     THEN "retained"
                     ELSE "absent"
                ELSE slots1[i]]

CompactOrigins(recs, slots_fn, origins_set, keep_set) ==
    LET stale == {i \in origins_set : slots_fn[i] = "retained" /\ i \notin keep_set}
        orig1 == origins_set \ stale
    IN IF Len(recs) <= HalfRecords
       THEN orig1
       ELSE LET trim == Len(recs) - HalfRecords
                dropped == {i \in AllIntents :
                    /\ i \notin keep_set
                    /\ \E j \in 1..trim : recs[j].admit_intent = i}
            IN orig1 \ dropped

Init ==
    /\ now = 1
    /\ acct_orders = [k \in OrderIntents |-> NoneOrder]
    /\ acct_adm = [i \in AllIntents |-> "absent"]
    /\ adm_retired_at = [i \in AllIntents |-> 0]
    /\ facts = [k \in OrderIntents |-> 0]
    /\ pending_send = 0
    /\ gw_status = [i \in AllIntents |-> "absent"]
    /\ gw_eval_at = [i \in AllIntents |-> 0]
    /\ gw_completed_at = [i \in AllIntents |-> 0]
    /\ evicted_prefix = 0
    /\ journal_records = <<>>
    /\ journal_slots = [i \in AllIntents |-> "absent"]
    /\ origin_present = {}
    /\ frontier_pos = 0
    /\ ever_orders = {}
    /\ ever_mgmt = {}
    /\ ghost_orders = [k \in OrderIntents |-> NoneOrder]
    /\ ghost_retire_at = [i \in AllIntents |-> 0]
    /\ admit_count = [i \in AllIntents |-> 0]
    /\ fact_first_at = [k \in OrderIntents |-> 0]
    /\ trade_book_count = [k \in OrderIntents |-> 0]
    /\ overflow_reclaimed = {}
    /\ total_appends = 0

\* Monotonic clock ticks across retention windows.
Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<
        acct_orders, acct_adm, adm_retired_at, facts, pending_send, gw_status,
        gw_eval_at, gw_completed_at, evicted_prefix, journal_records,
        journal_slots, origin_present, frontier_pos, ever_orders, ever_mgmt,
        ghost_orders, ghost_retire_at, admit_count, fact_first_at,
        trade_book_count, overflow_reclaimed, total_appends
       >>

\* Explicit lazy eviction sweep across Account and AccountGateway.
\* crates/tickwire-account/src/lib.rs:788-911, crates/tickwire-account-gateway/src/lib.rs:629-662.
EvictExpired ==
    /\ LET o1 == AfterEvictOrders(acct_orders, facts, now)
           f1 == AfterEvictFacts(acct_orders, facts, now)
           a1 == AfterEvictAdm(acct_orders, facts, acct_adm, adm_retired_at, now)
           ar1 == AfterEvictAdmRet(acct_orders, facts, acct_adm, adm_retired_at, now)
           st1 == AfterEvictGwStatus(acct_orders, facts, acct_adm, adm_retired_at, gw_status, gw_eval_at, gw_completed_at, pending_send, now)
           cp1 == AfterEvictGwComp(acct_orders, facts, acct_adm, adm_retired_at, gw_status, gw_eval_at, gw_completed_at, pending_send, now)
       IN /\ (o1 # acct_orders \/ a1 # acct_adm \/ st1 # gw_status)
          /\ acct_orders' = o1
          /\ facts' = f1
          /\ acct_adm' = a1
          /\ adm_retired_at' = ar1
          /\ gw_status' = st1
          /\ gw_completed_at' = cp1
    /\ UNCHANGED <<
        now, pending_send, gw_eval_at, evicted_prefix, journal_records,
        journal_slots, origin_present, frontier_pos, ever_orders, ever_mgmt,
        ghost_orders, ghost_retire_at, admit_count, fact_first_at,
        trade_book_count, overflow_reclaimed, total_appends
       >>

\* Submit a fresh order-creating New intent in monotonic strategy intent_id order:
\* runs evict_retired_entries(now), enforces MaxOrders cap, maybe_compact_journal,
\* and appends Admit(k) to AccountJournal.
\* crates/tickwire-account/src/admission.rs:78-93, crates/tickwire-account/src/admission.rs:169-176,
\* crates/tickwire-account-gateway/src/lib.rs:678-809.
SubmitNewOrder ==
    /\ pending_send = 0
    /\ Cardinality(ever_orders) < NumOrders
    /\ LET o1 == AfterEvictOrders(acct_orders, facts, now)
           f1 == AfterEvictFacts(acct_orders, facts, now)
           a1 == AfterEvictAdm(acct_orders, facts, acct_adm, adm_retired_at, now)
           ar1 == AfterEvictAdmRet(acct_orders, facts, acct_adm, adm_retired_at, now)
           st1 == AfterEvictGwStatus(acct_orders, facts, acct_adm, adm_retired_at, gw_status, gw_eval_at, gw_completed_at, 0, now)
           cp1 == AfterEvictGwComp(acct_orders, facts, acct_adm, adm_retired_at, gw_status, gw_eval_at, gw_completed_at, 0, now)
           k == Cardinality(ever_orders) + 1
       IN /\ OrderGatewayCount(st1) < MaxOrders
          /\ OrderAccountCount(o1, a1) < MaxOrders
          /\ LET new_ord == [NoneOrder EXCEPT !.present = TRUE]
                 o2 == [o1 EXCEPT ![k] = new_ord]
                 a2 == [a1 EXCEPT ![k] = "active"]
                 ar2 == [ar1 EXCEPT ![k] = 0]
                 st2 == [st1 EXCEPT ![k] = "Queued"]
                 ev2 == [gw_eval_at EXCEPT ![k] = now]
                 cp2 == [cp1 EXCEPT ![k] = 0]
                 need_comp == (Len(journal_records) + 1 >= MaxRecords)
                 keep == KeepSet(a2, st1)
                 ep_base == IF need_comp THEN CompactPrefix(evicted_prefix, journal_records) ELSE evicted_prefix
                 recs_base == IF need_comp THEN CompactRecs(journal_records) ELSE journal_records
                 slots_base == IF need_comp THEN CompactSlots(journal_records, journal_slots, keep) ELSE journal_slots
                 orig_base == IF need_comp THEN CompactOrigins(journal_records, journal_slots, origin_present, keep) ELSE origin_present
                 next_seq == ep_base + Len(recs_base) + 1
             IN /\ acct_orders' = o2
                /\ facts' = f1
                /\ acct_adm' = a2
                /\ adm_retired_at' = ar2
                /\ pending_send' = k
                /\ gw_status' = st2
                /\ gw_eval_at' = ev2
                /\ gw_completed_at' = cp2
                /\ evicted_prefix' = ep_base
                /\ journal_records' = Append(recs_base, [seq |-> next_seq, admit_intent |-> k])
                /\ journal_slots' = [slots_base EXCEPT ![k] = "in_records"]
                /\ origin_present' = orig_base \cup {k}
                /\ frontier_pos' = frontier_pos + 1
                /\ ever_orders' = ever_orders \cup {k}
                /\ ghost_orders' = [ghost_orders EXCEPT ![k] = new_ord]
                /\ ghost_retire_at' = [ghost_retire_at EXCEPT ![k] = 0]
                /\ admit_count' = [admit_count EXCEPT ![k] = @ + 1]
                /\ total_appends' = total_appends + 1
    /\ UNCHANGED <<now, ever_mgmt, fact_first_at, trade_book_count, overflow_reclaimed>>

\* Submit a Cancel/Query management intent targeting an open order in monotonic
\* intent_id order: uses independent MaxMgmt headroom and eagerly reclaims the oldest
\* retired management entry when full, even when all MaxOrders slots hold live orders.
\* crates/tickwire-account/src/lib.rs:755-783, crates/tickwire-account/src/admission.rs:94-147,
\* crates/tickwire-account-gateway/src/lib.rs:597-625, crates/tickwire-account-gateway/src/lib.rs:705-745.
SubmitManagement ==
    /\ pending_send = 0
    /\ Cardinality(ever_mgmt) < NumMgmt
    /\ LET o1 == AfterEvictOrders(acct_orders, facts, now)
           f1 == AfterEvictFacts(acct_orders, facts, now)
           a1 == AfterEvictAdm(acct_orders, facts, acct_adm, adm_retired_at, now)
           ar1 == AfterEvictAdmRet(acct_orders, facts, acct_adm, adm_retired_at, now)
           st1 == AfterEvictGwStatus(acct_orders, facts, acct_adm, adm_retired_at, gw_status, gw_eval_at, gw_completed_at, 0, now)
           cp1 == AfterEvictGwComp(acct_orders, facts, acct_adm, adm_retired_at, gw_status, gw_eval_at, gw_completed_at, 0, now)
           gw_cands == ReclaimableGatewayMgmt(st1, a1, 0)
           ac_cands == ReclaimableAccountMgmt(a1)
           m == NumOrders + Cardinality(ever_mgmt) + 1
       IN /\ \E k \in OrderIntents : IsOpenOrder(o1, k)
          /\ (MgmtGatewayCount(st1) < MaxMgmt \/ gw_cands # {})
          /\ (MgmtAccountCount(a1) < MaxMgmt \/ ac_cands # {})
          /\ LET gw_victim == IF MgmtGatewayCount(st1) >= MaxMgmt THEN PickOldestGwMgmt(gw_cands, cp1, gw_eval_at) ELSE 0
                 ac_victim == IF MgmtAccountCount(a1) >= MaxMgmt THEN PickOldestAcctMgmt(ac_cands, ar1) ELSE 0
                 st_recl == IF gw_victim # 0 THEN [st1 EXCEPT ![gw_victim] = "absent"] ELSE st1
                 cp_recl == IF gw_victim # 0 THEN [cp1 EXCEPT ![gw_victim] = 0] ELSE cp1
                 a_recl == IF ac_victim # 0 THEN [a1 EXCEPT ![ac_victim] = "absent"] ELSE a1
                 ar_recl == IF ac_victim # 0 THEN [ar1 EXCEPT ![ac_victim] = 0] ELSE ar1
                 a2 == [a_recl EXCEPT ![m] = "active"]
                 ar2 == [ar_recl EXCEPT ![m] = 0]
                 st2 == [st_recl EXCEPT ![m] = "Queued"]
                 ev2 == [gw_eval_at EXCEPT ![m] = now]
                 cp2 == [cp_recl EXCEPT ![m] = 0]
                 need_comp == (Len(journal_records) + 1 >= MaxRecords)
                 keep == KeepSet(a2, st_recl)
                 ep_base == IF need_comp THEN CompactPrefix(evicted_prefix, journal_records) ELSE evicted_prefix
                 recs_base == IF need_comp THEN CompactRecs(journal_records) ELSE journal_records
                 slots_base == IF need_comp THEN CompactSlots(journal_records, journal_slots, keep) ELSE journal_slots
                 orig_base == IF need_comp THEN CompactOrigins(journal_records, journal_slots, origin_present, keep) ELSE origin_present
                 next_seq == ep_base + Len(recs_base) + 1
                 reclaimed_set ==
                     (IF gw_victim # 0 THEN {gw_victim} ELSE {})
                     \cup (IF ac_victim # 0 THEN {ac_victim} ELSE {})
             IN /\ acct_orders' = o1
                /\ facts' = f1
                /\ acct_adm' = a2
                /\ adm_retired_at' = ar2
                /\ pending_send' = m
                /\ gw_status' = st2
                /\ gw_eval_at' = ev2
                /\ gw_completed_at' = cp2
                /\ evicted_prefix' = ep_base
                /\ journal_records' = Append(recs_base, [seq |-> next_seq, admit_intent |-> m])
                /\ journal_slots' = [slots_base EXCEPT ![m] = "in_records"]
                /\ origin_present' = orig_base \cup {m}
                /\ frontier_pos' = frontier_pos + 1
                /\ ever_mgmt' = ever_mgmt \cup {m}
                /\ ghost_retire_at' = [ghost_retire_at EXCEPT ![m] = 0]
                /\ admit_count' = [admit_count EXCEPT ![m] = @ + 1]
                /\ overflow_reclaimed' = overflow_reclaimed \cup reclaimed_set
                /\ total_appends' = total_appends + 1
    /\ UNCHANGED <<now, ever_orders, ghost_orders, fact_first_at, trade_book_count>>

\* Byte-identical retry of an admitted intent within its active/retention window:
\* hits gateway/account dedup without double-admitting; if prematurely evicted,
\* increments admit_count to trip WindowDedupPreserved.
\* crates/tickwire-account/src/admission.rs:4-15, crates/tickwire-account-gateway/src/lib.rs:685-688.
RetryDuplicateIntent ==
    /\ \E i \in (ever_orders \cup (ever_mgmt \ overflow_reclaimed)) :
        /\ (ghost_retire_at[i] = 0 \/ now < ghost_retire_at[i] + Window)
        /\ IF gw_status[i] # "absent" /\ acct_adm[i] # "absent"
           THEN UNCHANGED admit_count
           ELSE admit_count' = [admit_count EXCEPT ![i] = @ + 1]
    /\ UNCHANGED <<
        now, acct_orders, acct_adm, adm_retired_at, facts, pending_send,
        gw_status, gw_eval_at, gw_completed_at, evicted_prefix,
        journal_records, journal_slots, origin_present, frontier_pos,
        ever_orders, ever_mgmt, ghost_orders, ghost_retire_at, fact_first_at,
        trade_book_count, overflow_reclaimed, total_appends
       >>

\* Outbound TLS frame flushed (apply_report(SendState::Flushed)): retires management
\* admission at now and refreshes gateway completion stamps.
\* crates/tickwire-account-gateway/src/lib.rs:1509-1525.
FlushPending ==
    /\ pending_send # 0
    /\ LET i == pending_send
           a1 == IF i \in MgmtIntents THEN [acct_adm EXCEPT ![i] = "retired"] ELSE acct_adm
           ar1 == IF i \in MgmtIntents THEN [adm_retired_at EXCEPT ![i] = now] ELSE adm_retired_at
           st1 == [gw_status EXCEPT ![i] = "Flushed"]
           cp1 == RefreshCompleted(acct_orders, facts, a1, ar1, st1, gw_eval_at, gw_completed_at, 0, now)
       IN /\ pending_send' = 0
          /\ acct_adm' = a1
          /\ adm_retired_at' = ar1
          /\ gw_status' = st1
          /\ gw_completed_at' = cp1
          /\ ghost_retire_at' =
                IF cp1[i] > 0
                THEN [ghost_retire_at EXCEPT ![i] = cp1[i]]
                ELSE ghost_retire_at
    /\ UNCHANGED <<
        now, acct_orders, facts, gw_eval_at, evicted_prefix, journal_records,
        journal_slots, origin_present, frontier_pos, ever_orders, ever_mgmt,
        ghost_orders, admit_count, fact_first_at, trade_book_count,
        overflow_reclaimed, total_appends
       >>

\* Zero-byte unsent abort: marks order/management admission aborted and retired at now,
\* refreshes completion stamps, and appends Aborted record to AccountJournal.
\* crates/tickwire-account/src/admission.rs:540-560, crates/tickwire-account-gateway/src/lib.rs:1532-1559.
AbortPending ==
    /\ pending_send # 0
    /\ LET i == pending_send
           o1 == IF i \in OrderIntents
                 THEN [acct_orders EXCEPT ![i].aborted = TRUE,
                                          ![i].fees_closed = TRUE,
                                          ![i].retired_at = now]
                 ELSE acct_orders
           g1 == IF i \in OrderIntents
                 THEN [ghost_orders EXCEPT ![i].aborted = TRUE,
                                           ![i].fees_closed = TRUE,
                                           ![i].retired_at = now]
                 ELSE ghost_orders
           a1 == [acct_adm EXCEPT ![i] = "retired"]
           ar1 == [adm_retired_at EXCEPT ![i] = now]
           st1 == [gw_status EXCEPT ![i] = "Aborted"]
           cp1 == RefreshCompleted(o1, facts, a1, ar1, st1, gw_eval_at, gw_completed_at, 0, now)
           need_comp == (Len(journal_records) + 1 >= MaxRecords)
           keep == KeepSet(a1, st1)
           ep_base == IF need_comp THEN CompactPrefix(evicted_prefix, journal_records) ELSE evicted_prefix
           recs_base == IF need_comp THEN CompactRecs(journal_records) ELSE journal_records
           slots_base == IF need_comp THEN CompactSlots(journal_records, journal_slots, keep) ELSE journal_slots
           orig_base == IF need_comp THEN CompactOrigins(journal_records, journal_slots, origin_present, keep) ELSE origin_present
           next_seq == ep_base + Len(recs_base) + 1
       IN /\ pending_send' = 0
          /\ acct_orders' = o1
          /\ ghost_orders' = g1
          /\ acct_adm' = a1
          /\ adm_retired_at' = ar1
          /\ gw_status' = st1
          /\ gw_completed_at' = cp1
          /\ ghost_retire_at' = [ghost_retire_at EXCEPT ![i] = cp1[i]]
          /\ evicted_prefix' = ep_base
          /\ journal_records' = Append(recs_base, [seq |-> next_seq, admit_intent |-> 0])
          /\ journal_slots' = slots_base
          /\ origin_present' = orig_base
          /\ frontier_pos' = frontier_pos + 1
          /\ total_appends' = total_appends + 1
    /\ UNCHANGED <<
        now, facts, gw_eval_at, ever_orders, ever_mgmt, admit_count,
        fact_first_at, trade_book_count, overflow_reclaimed
       >>

\* Venue reports terminal order status with cumulative = 1 BEFORE trade execution arrives:
\* terminal = TRUE, term_cum = 1, filled = 0 => non-zero remaining() blocks eviction!
\* crates/tickwire-account/src/lib.rs:142-167, crates/tickwire-account/src/events.rs:458-494.
ObserveTerminalPendingFill ==
    /\ \E k \in OrderIntents :
        /\ gw_status[k] = "Flushed"
        /\ acct_orders[k].present
        /\ ~acct_orders[k].aborted
        /\ ~acct_orders[k].terminal
        /\ acct_orders[k].filled = 0
        /\ ~acct_orders[k].amend_inflight
        /\ LET o1 == [acct_orders EXCEPT ![k].terminal = TRUE, ![k].term_cum = 1, ![k].retired_at = 0]
               g1 == [ghost_orders EXCEPT ![k].terminal = TRUE, ![k].term_cum = 1, ![k].retired_at = 0]
               need_comp == (Len(journal_records) + 1 >= MaxRecords)
               keep == KeepSet(acct_adm, gw_status)
               ep_base == IF need_comp THEN CompactPrefix(evicted_prefix, journal_records) ELSE evicted_prefix
               recs_base == IF need_comp THEN CompactRecs(journal_records) ELSE journal_records
               slots_base == IF need_comp THEN CompactSlots(journal_records, journal_slots, keep) ELSE journal_slots
               orig_base == IF need_comp THEN CompactOrigins(journal_records, journal_slots, origin_present, keep) ELSE origin_present
               next_seq == ep_base + Len(recs_base) + 1
           IN /\ acct_orders' = o1
              /\ ghost_orders' = g1
              /\ evicted_prefix' = ep_base
              /\ journal_records' = Append(recs_base, [seq |-> next_seq, admit_intent |-> 0])
              /\ journal_slots' = slots_base
              /\ origin_present' = orig_base
              /\ frontier_pos' = frontier_pos + 1
              /\ total_appends' = total_appends + 1
    /\ UNCHANGED <<
        now, acct_adm, adm_retired_at, facts, pending_send, gw_status,
        gw_eval_at, gw_completed_at, ever_orders, ever_mgmt, ghost_retire_at,
        admit_count, fact_first_at, trade_book_count, overflow_reclaimed
       >>

\* Venue trade execution books 1 lot fill and records FactKey at recorded_mono_ns = now;
\* order becomes terminal with term_cum = 1, but fees_closed = FALSE keeps it non-retirable!
\* crates/tickwire-account/src/events.rs:36-145, crates/tickwire-account/src/lib.rs:559-582.
BookTradeFill ==
    /\ \E k \in OrderIntents :
        /\ gw_status[k] = "Flushed"
        /\ acct_orders[k].present
        /\ ~acct_orders[k].aborted
        /\ acct_orders[k].filled = 0
        /\ ~acct_orders[k].amend_inflight
        /\ LET o1 == [acct_orders EXCEPT ![k].filled = 1,
                                         ![k].term_cum = 1,
                                         ![k].terminal = TRUE,
                                         ![k].retired_at = 0]
               g1 == [ghost_orders EXCEPT ![k].filled = 1,
                                          ![k].term_cum = 1,
                                          ![k].terminal = TRUE,
                                          ![k].retired_at = 0]
               f1 == [facts EXCEPT ![k] = now]
               need_comp == (Len(journal_records) + 1 >= MaxRecords)
               keep == KeepSet(acct_adm, gw_status)
               ep_base == IF need_comp THEN CompactPrefix(evicted_prefix, journal_records) ELSE evicted_prefix
               recs_base == IF need_comp THEN CompactRecs(journal_records) ELSE journal_records
               slots_base == IF need_comp THEN CompactSlots(journal_records, journal_slots, keep) ELSE journal_slots
               orig_base == IF need_comp THEN CompactOrigins(journal_records, journal_slots, origin_present, keep) ELSE origin_present
               next_seq == ep_base + Len(recs_base) + 1
           IN /\ acct_orders' = o1
              /\ ghost_orders' = g1
              /\ facts' = f1
              /\ fact_first_at' = [fact_first_at EXCEPT ![k] = now]
              /\ trade_book_count' = [trade_book_count EXCEPT ![k] = @ + 1]
              /\ evicted_prefix' = ep_base
              /\ journal_records' = Append(recs_base, [seq |-> next_seq, admit_intent |-> 0])
              /\ journal_slots' = slots_base
              /\ origin_present' = orig_base
              /\ frontier_pos' = frontier_pos + 1
              /\ total_appends' = total_appends + 1
    /\ UNCHANGED <<
        now, acct_adm, adm_retired_at, pending_send, gw_status, gw_eval_at,
        gw_completed_at, ever_orders, ever_mgmt, ghost_retire_at, admit_count,
        overflow_reclaimed
       >>

\* Duplicate trade_id execution within retention window: deduplicated via facts[k] > 0;
\* if fact were prematurely evicted while order still present, increments trade_book_count!
\* crates/tickwire-account/src/events.rs:88-93.
DuplicateTradeFill ==
    /\ \E k \in ever_orders :
        /\ fact_first_at[k] > 0
        /\ (acct_orders[k].present \/ now < fact_first_at[k] + Window)
        /\ IF facts[k] > 0
           THEN UNCHANGED trade_book_count
           ELSE trade_book_count' = [trade_book_count EXCEPT ![k] = @ + 1]
    /\ UNCHANGED <<
        now, acct_orders, acct_adm, adm_retired_at, facts, pending_send,
        gw_status, gw_eval_at, gw_completed_at, evicted_prefix,
        journal_records, journal_slots, origin_present, frontier_pos,
        ever_orders, ever_mgmt, ghost_orders, ghost_retire_at, admit_count,
        fact_first_at, overflow_reclaimed, total_appends
       >>

\* Venue ambiguity / user-stream gap latches observed_unknown = TRUE, permanently
\* blocking retirement regardless of terminal status, fee closure, or clock advance.
\* crates/tickwire-account/src/lib.rs:139-141, crates/tickwire-account/src/lib.rs:164,
\* crates/tickwire-account/src/events.rs:468-475.
MarkOrderUnknown ==
    /\ \E k \in OrderIntents :
        /\ gw_status[k] = "Flushed"
        /\ acct_orders[k].present
        /\ ~acct_orders[k].aborted
        /\ ~acct_orders[k].fees_closed
        /\ ~acct_orders[k].unknown
        /\ acct_orders' = [acct_orders EXCEPT ![k].unknown = TRUE, ![k].retired_at = 0]
        /\ ghost_orders' = [ghost_orders EXCEPT ![k].unknown = TRUE, ![k].retired_at = 0]
    /\ UNCHANGED <<
        now, acct_adm, adm_retired_at, facts, pending_send, gw_status,
        gw_eval_at, gw_completed_at, evicted_prefix, journal_records,
        journal_slots, origin_present, frontier_pos, ever_orders, ever_mgmt,
        ghost_retire_at, admit_count, fact_first_at, trade_book_count,
        overflow_reclaimed, total_appends
       >>

\* In-flight amendment on an open order blocks Account::is_order_retirable until resolved.
\* crates/tickwire-account/src/lib.rs:594-603, crates/tickwire-account/src/admission.rs:114-118.
StartInflightAmend ==
    /\ \E k \in OrderIntents :
        /\ gw_status[k] = "Flushed"
        /\ IsOpenOrder(acct_orders, k)
        /\ ~acct_orders[k].amend_inflight
        /\ ~acct_orders[k].amend_done
        /\ acct_orders' = [acct_orders EXCEPT ![k].amend_inflight = TRUE,
                                              ![k].amend_done = TRUE,
                                              ![k].retired_at = 0]
        /\ ghost_orders' = [ghost_orders EXCEPT ![k].amend_inflight = TRUE,
                                                ![k].amend_done = TRUE,
                                                ![k].retired_at = 0]
    /\ UNCHANGED <<
        now, acct_adm, adm_retired_at, facts, pending_send, gw_status,
        gw_eval_at, gw_completed_at, evicted_prefix, journal_records,
        journal_slots, origin_present, frontier_pos, ever_orders, ever_mgmt,
        ghost_retire_at, admit_count, fact_first_at, trade_book_count,
        overflow_reclaimed, total_appends
       >>

\* Amendment rejected/confirmed resolves in-flight amendment protection on the order.
\* crates/tickwire-account/src/amendments.rs:167-230, crates/tickwire-account/src/lib.rs:594-626.
FinishInflightAmend ==
    /\ \E k \in OrderIntents :
        /\ acct_orders[k].present
        /\ acct_orders[k].amend_inflight
        /\ acct_orders' = [acct_orders EXCEPT ![k].amend_inflight = FALSE]
        /\ ghost_orders' = [ghost_orders EXCEPT ![k].amend_inflight = FALSE]
    /\ UNCHANGED <<
        now, acct_adm, adm_retired_at, facts, pending_send, gw_status,
        gw_eval_at, gw_completed_at, evicted_prefix, journal_records,
        journal_slots, origin_present, frontier_pos, ever_orders, ever_mgmt,
        ghost_retire_at, admit_count, fact_first_at, trade_book_count,
        overflow_reclaimed, total_appends
       >>

\* Authenticated fee closure on a terminal zero-remaining order: sets fees_closed = TRUE,
\* retires the order iff !unknown and !amend_inflight, refreshes gateway completion,
\* and appends FeesReconciled to AccountJournal.
\* crates/tickwire-account/src/admission.rs:562-591, crates/tickwire-account/src/lib.rs:614-655.
ReconcileOrderFees ==
    /\ \E k \in OrderIntents :
        /\ acct_orders[k].present
        /\ acct_orders[k].terminal
        /\ acct_orders[k].term_cum = acct_orders[k].filled
        /\ ~acct_orders[k].fees_closed
        /\ LET base_o == [acct_orders[k] EXCEPT !.fees_closed = TRUE]
               retirable == OrderRetirable(base_o)
               final_o == [base_o EXCEPT !.retired_at = IF retirable THEN now ELSE 0]
               o1 == [acct_orders EXCEPT ![k] = final_o]
               g1 == [ghost_orders EXCEPT ![k] = final_o]
               a1 == [acct_adm EXCEPT ![k] = IF retirable THEN "retired" ELSE "active"]
               ar1 == [adm_retired_at EXCEPT ![k] = IF retirable THEN Max(now, facts[k]) ELSE 0]
               cp1 == RefreshCompleted(o1, facts, a1, ar1, gw_status, gw_eval_at, gw_completed_at, pending_send, now)
               need_comp == (Len(journal_records) + 1 >= MaxRecords)
               keep == KeepSet(a1, gw_status)
               ep_base == IF need_comp THEN CompactPrefix(evicted_prefix, journal_records) ELSE evicted_prefix
               recs_base == IF need_comp THEN CompactRecs(journal_records) ELSE journal_records
               slots_base == IF need_comp THEN CompactSlots(journal_records, journal_slots, keep) ELSE journal_slots
               orig_base == IF need_comp THEN CompactOrigins(journal_records, journal_slots, origin_present, keep) ELSE origin_present
               next_seq == ep_base + Len(recs_base) + 1
           IN /\ acct_orders' = o1
              /\ ghost_orders' = g1
              /\ acct_adm' = a1
              /\ adm_retired_at' = ar1
              /\ gw_completed_at' = cp1
              /\ ghost_retire_at' =
                    IF retirable /\ cp1[k] > 0
                    THEN [ghost_retire_at EXCEPT ![k] = cp1[k]]
                    ELSE ghost_retire_at
              /\ evicted_prefix' = ep_base
              /\ journal_records' = Append(recs_base, [seq |-> next_seq, admit_intent |-> 0])
              /\ journal_slots' = slots_base
              /\ origin_present' = orig_base
              /\ frontier_pos' = frontier_pos + 1
              /\ total_appends' = total_appends + 1
    /\ UNCHANGED <<
        now, facts, pending_send, gw_status, gw_eval_at, ever_orders,
        ever_mgmt, admit_count, fact_first_at, trade_book_count,
        overflow_reclaimed
       >>

\* Explicit AccountJournal::compact_before(keep, MaxRecords \div 2) prefix trim.
\* crates/tickwire-account-journal/src/lib.rs:582-652, crates/tickwire-account-gateway/src/lib.rs:664-677.
CompactJournal ==
    /\ Len(journal_records) > HalfRecords
    /\ LET o1 == AfterEvictOrders(acct_orders, facts, now)
           f1 == AfterEvictFacts(acct_orders, facts, now)
           a1 == AfterEvictAdm(acct_orders, facts, acct_adm, adm_retired_at, now)
           ar1 == AfterEvictAdmRet(acct_orders, facts, acct_adm, adm_retired_at, now)
           st1 == AfterEvictGwStatus(acct_orders, facts, acct_adm, adm_retired_at, gw_status, gw_eval_at, gw_completed_at, pending_send, now)
           cp1 == AfterEvictGwComp(acct_orders, facts, acct_adm, adm_retired_at, gw_status, gw_eval_at, gw_completed_at, pending_send, now)
           keep == KeepSet(a1, st1)
       IN /\ acct_orders' = o1
          /\ facts' = f1
          /\ acct_adm' = a1
          /\ adm_retired_at' = ar1
          /\ gw_status' = st1
          /\ gw_completed_at' = cp1
          /\ evicted_prefix' = CompactPrefix(evicted_prefix, journal_records)
          /\ journal_records' = CompactRecs(journal_records)
          /\ journal_slots' = CompactSlots(journal_records, journal_slots, keep)
          /\ origin_present' = CompactOrigins(journal_records, journal_slots, origin_present, keep)
    /\ UNCHANGED <<
        now, pending_send, gw_eval_at, frontier_pos, ever_orders, ever_mgmt,
        ghost_orders, ghost_retire_at, admit_count, fact_first_at,
        trade_book_count, overflow_reclaimed, total_appends
       >>

Next ==
    \/ Tick
    \/ EvictExpired
    \/ SubmitNewOrder
    \/ SubmitManagement
    \/ RetryDuplicateIntent
    \/ FlushPending
    \/ AbortPending
    \/ ObserveTerminalPendingFill
    \/ BookTradeFill
    \/ DuplicateTradeFill
    \/ MarkOrderUnknown
    \/ StartInflightAmend
    \/ FinishInflightAmend
    \/ ReconcileOrderFees
    \/ CompactJournal

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ now \in 1..MaxTime
    /\ acct_orders \in [OrderIntents -> OrderValues]
    /\ acct_adm \in [AllIntents -> AdmStates]
    /\ adm_retired_at \in [AllIntents -> 0..MaxTime]
    /\ facts \in [OrderIntents -> 0..MaxTime]
    /\ pending_send \in 0..(NumOrders + NumMgmt)
    /\ gw_status \in [AllIntents -> GwStatuses]
    /\ gw_eval_at \in [AllIntents -> 0..MaxTime]
    /\ gw_completed_at \in [AllIntents -> 0..MaxTime]
    /\ evicted_prefix \in 0..MaxAppends
    /\ Len(journal_records) <= MaxRecords
    /\ \A j \in 1..Len(journal_records) : journal_records[j] \in JournalElemValues
    /\ journal_slots \in [AllIntents -> SlotStates]
    /\ origin_present \subseteq AllIntents
    /\ frontier_pos \in 0..MaxAppends
    /\ ever_orders \subseteq OrderIntents
    /\ ever_mgmt \subseteq MgmtIntents
    /\ ghost_orders \in [OrderIntents -> OrderValues]
    /\ ghost_retire_at \in [AllIntents -> 0..MaxTime]
    /\ admit_count \in [AllIntents -> 0..2]
    /\ fact_first_at \in [OrderIntents -> 0..MaxTime]
    /\ trade_book_count \in [OrderIntents -> 0..2]
    /\ overflow_reclaimed \subseteq MgmtIntents
    /\ total_appends \in 0..MaxAppends
    /\ OrderGatewayCount(gw_status) <= MaxOrders
    /\ OrderAccountCount(acct_orders, acct_adm) <= MaxOrders
    /\ MgmtGatewayCount(gw_status) <= MaxMgmt
    /\ MgmtAccountCount(acct_adm) <= MaxMgmt

GhostNonRetirable(k) ==
    LET g == ghost_orders[k]
    IN /\ g.present
       /\ \/ g.unknown
          \/ g.amend_inflight
          \/ (~g.aborted /\ (~g.terminal \/ g.term_cum # g.filled \/ ~g.fees_closed))

\* No non-retirable order (live resting, unknown, unclosed fees, non-zero remaining,
\* or in-flight amendment) is EVER evicted from Account or AccountGateway regardless
\* of how far now advances across retention windows.
ActiveNeverEvicted ==
    \A k \in ever_orders :
        GhostNonRetirable(k) =>
            /\ acct_orders[k].present
            /\ acct_adm[k] = "active"
            /\ gw_status[k] # "absent"
            /\ acct_orders[k].retired_at = 0
            /\ gw_completed_at[k] = 0

\* Within [recorded_at, recorded_at + Window), duplicate IntentKeys and duplicate
\* trade_id fills remain resident and are deterministically deduplicated with zero
\* double-admission or double-booking.
WindowDedupPreserved ==
    /\ \A i \in AllIntents : admit_count[i] <= 1
    /\ \A k \in ever_orders :
        /\ trade_book_count[k] <= 1
        /\ (acct_orders[k].present => acct_orders[k].filled = trade_book_count[k])
        /\ ((ghost_retire_at[k] = 0 \/ now < ghost_retire_at[k] + Window)
            => /\ acct_orders[k].present
               /\ acct_adm[k] # "absent"
               /\ gw_status[k] # "absent")
        /\ ((fact_first_at[k] > 0 /\ (GhostNonRetirable(k) \/ now < fact_first_at[k] + Window))
            => facts[k] = fact_first_at[k])
    /\ \A m \in (ever_mgmt \ overflow_reclaimed) :
        (ghost_retire_at[m] = 0 \/ now < ghost_retire_at[m] + Window)
        => /\ acct_adm[m] # "absent"
           /\ gw_status[m] # "absent"

\* Even when all MaxOrders slots hold live resting orders, whenever the outbound wire
\* is idle (pending_send = 0) and an open order exists, Cancel/Query headroom is
\* available either directly or via eager retired-management reclamation.
ManagementNeverStarved ==
    (pending_send = 0 /\ \E k \in OrderIntents : IsOpenOrder(acct_orders, k))
    => /\ (MgmtGatewayCount(gw_status) < MaxMgmt
           \/ ReclaimableGatewayMgmt(gw_status, acct_adm, pending_send) # {})
       /\ (MgmtAccountCount(acct_adm) < MaxMgmt
           \/ ReclaimableAccountMgmt(acct_adm) # {})

\* Journal prefix compaction (evicted_prefix) preserves contiguous logical sequence
\* numbers, rolling HistoryFrontier equivalence, and retained AdmissionOrigins for
\* every active or still-resident intent across Account and AccountGateway.
FrontierContinuity ==
    /\ evicted_prefix + Len(journal_records) = total_appends
    /\ frontier_pos = total_appends
    /\ \A j \in 1..Len(journal_records) :
        journal_records[j].seq = evicted_prefix + j
    /\ \A i \in AllIntents :
        (IntentActive(acct_orders, acct_adm, i) \/ acct_adm[i] # "absent" \/ gw_status[i] # "absent")
        => /\ i \in origin_present
           /\ journal_slots[i] \in {"in_records", "retained"}
    /\ \A i \in AllIntents :
        journal_slots[i] = "in_records"
        => (\E j \in 1..Len(journal_records) : journal_records[j].admit_intent = i)
    /\ \A i \in AllIntents :
        (i \in origin_present
         /\ ~\E j \in 1..Len(journal_records) : journal_records[j].admit_intent = i)
        => journal_slots[i] = "retained"
=============================================================================
