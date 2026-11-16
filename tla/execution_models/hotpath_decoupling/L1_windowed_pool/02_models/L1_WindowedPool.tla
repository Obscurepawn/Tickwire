---------------------------- MODULE L1_WindowedPool ----------------------------
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS NumOrders, MaxOrders, NumMgmt, MaxMgmt, MaxWays, Window, MaxTime

(*
Code-faithful model of pure stateless active-protected time-windowed pools
(WindowedTable), associative O(1) FillRecordPool trade_id deduplication and
post-trade fee/rebate amendment (amendment = true), coupled Account +
AccountGateway order/management/fact retention, per-order transport-doubt
isolation, and eager management headroom reclamation.

Variable <-> Rust source mapping and state/watermark denotations:
- now: monotonic clock step in 1..MaxTime representing now_mono_ns / evaluated_at
  and Account.last_mono_ns,
  crates/tickwire-account/src/pool.rs:47-56,
  crates/tickwire-account/src/lib.rs:1085-1093,
  crates/tickwire-account-gateway/src/lib.rs:614-625.
- acct_orders: Account.orders table over OrderIntents tracking present, filled,
  term_cum (terminal_cumulative), terminal, aborted, unknown (observed_unknown),
  in_doubt (Order.in_doubt), amend_inflight, amend_done, and retired_at
  (retired_mono_ns),
  crates/tickwire-account/src/pool.rs:28-40,
  crates/tickwire-account/src/pool.rs:363-493,
  crates/tickwire-account/src/lib.rs:247-291,
  crates/tickwire-account/src/lib.rs:844-912.
- acct_adm, adm_retired_at: Account.admissions state ("absent", "active", "retired")
  and Admission.retired_mono_ns separating order-creating caps (MAX_ORDERS) from
  management headroom (MAX_MANAGEMENT_ADMISSIONS + eager reclaim),
  crates/tickwire-account/src/types.rs:7-8,
  crates/tickwire-account/src/lib.rs:739-842,
  crates/tickwire-account/src/lib.rs:1026-1080,
  crates/tickwire-account/src/admission.rs:90-191.
- fill_ways: associative ways in a FillRecordPool bucket (FillBucket.ways) keyed by
  FillKey (ors_order_id, exec_ref, side, symbol_id), storing occupied, ord,
  fee_state ("omitted" for TRADE_LITE, "fee1", "fee2"), and recorded_at
  (recorded_mono_ns) for O(1) duplicate suppression, post-trade fee delta
  amendment (FillRecordOutcome::Amended), and oldest-expired-way reclamation,
  crates/tickwire-account/src/fill_record.rs:16-31,
  crates/tickwire-account/src/fill_record.rs:68-121,
  crates/tickwire-account/src/fill_record.rs:171-277.
- facts, wallet_fee: Account.facts FactEntry.recorded_mono_ns per order's trade
  execution (0 when absent) and net settled fee atoms booked on the Account wallet
  across reverse_prior_fee + apply_fee,
  crates/tickwire-account/src/lib.rs:294-314,
  crates/tickwire-account/src/lib.rs:1011-1024,
  crates/tickwire-account/src/events.rs:60-103,
  crates/tickwire-account/src/events.rs:220-359.
- pending_send, gw_status, gw_eval_at, gw_completed_at: AccountGateway.pending /
  GatewayPool.slot_io pending_send, AccountGateway.entries status/evaluated_at, and
  AccountGateway.completed_mono_ns across submit, apply_report(Flushed), abort,
  drop_broken_transport_isolated / isolate_intent_in_doubt, evict_retired_entries,
  and reclaim_one_retired_management_entry,
  crates/tickwire-account-gateway/src/lib.rs:544-769,
  crates/tickwire-account-gateway/src/lib.rs:1153-1211,
  crates/tickwire-account-gateway/src/lib.rs:1343-1475,
  crates/tickwire-account-gateway/src/pool.rs:54-86,
  crates/tickwire-account-gateway/src/pool.rs:840-862.
- ever_orders, ever_mgmt, ghost_orders, ghost_retire_at, admit_count,
  fact_first_at, fact_last_at, trade_book_count, ghost_expected_fee,
  overflow_reclaimed: ghost specification observers (never read by live table
  eviction logic) used strictly to verify ActiveNeverEvicted, WindowDedupPreserved,
  and FillAmendmentExactness.
*)

VARIABLES
    now,
    acct_orders,
    acct_adm,
    adm_retired_at,
    fill_ways,
    facts,
    wallet_fee,
    pending_send,
    gw_status,
    gw_eval_at,
    gw_completed_at,
    ever_orders,
    ever_mgmt,
    ghost_orders,
    ghost_retire_at,
    admit_count,
    fact_first_at,
    fact_last_at,
    trade_book_count,
    ghost_expected_fee,
    overflow_reclaimed

vars == <<
    now, acct_orders, acct_adm, adm_retired_at, fill_ways, facts, wallet_fee,
    pending_send, gw_status, gw_eval_at, gw_completed_at, ever_orders,
    ever_mgmt, ghost_orders, ghost_retire_at, admit_count, fact_first_at,
    fact_last_at, trade_book_count, ghost_expected_fee, overflow_reclaimed
>>

OrderIntents == 1..NumOrders
MgmtIntents == (NumOrders + 1)..(NumOrders + NumMgmt)
AllIntents == 1..(NumOrders + NumMgmt)
Ways == 1..MaxWays

AdmStates == {"absent", "active", "retired"}
GwStatuses == {"absent", "Queued", "Flushed", "Aborted", "Unknown"}
FeeStates == {"omitted", "fee1", "fee2"}

FeeVal(fs) ==
    IF fs = "omitted" THEN 0
    ELSE IF fs = "fee1" THEN 1
    ELSE 2

NoneOrder == [
    present |-> FALSE,
    filled |-> 0,
    term_cum |-> 0,
    terminal |-> FALSE,
    aborted |-> FALSE,
    unknown |-> FALSE,
    in_doubt |-> FALSE,
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
    in_doubt: BOOLEAN,
    amend_inflight: BOOLEAN,
    amend_done: BOOLEAN,
    retired_at: 0..MaxTime
]

EmptyWay == [
    occupied |-> FALSE,
    ord |-> 0,
    fee_state |-> "omitted",
    recorded_at |-> 0
]

WayValues == [
    occupied: BOOLEAN,
    ord: 0..NumOrders,
    fee_state: FeeStates,
    recorded_at: 0..MaxTime
]

Max(a, b) == IF a >= b THEN a ELSE b

\* WindowedTable::is_expired(Some(retired_at), now_mono_ns, Some(window_ns)).
\* crates/tickwire-account/src/pool.rs:47-56.
IsExpired(stamp, t) ==
    /\ stamp > 0
    /\ t >= stamp + Window

\* Order::remaining() == 0, Order::is_retirable(), and Account::is_order_retirable().
\* crates/tickwire-account/src/lib.rs:250-291, crates/tickwire-account/src/lib.rs:844-860.
OrderRemainingZero(o) ==
    \/ o.aborted
    \/ (o.terminal /\ o.term_cum = o.filled)

OrderRetirable(o) ==
    /\ o.present
    /\ ~o.unknown
    /\ ~o.in_doubt
    /\ ~o.amend_inflight
    /\ (o.aborted \/ (o.terminal /\ OrderRemainingZero(o)))

IsOpenOrder(orders_fn, k) ==
    /\ orders_fn[k].present
    /\ ~orders_fn[k].aborted
    /\ ~orders_fn[k].terminal

\* Account::is_intent_active and Account::intent_retired_mono_ns.
\* crates/tickwire-account/src/lib.rs:739-842.
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
\* crates/tickwire-account-gateway/src/lib.rs:549-574.
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
\* Note that AccountGateway::evict_retired_entries also guards !self.account.has_admission(key).
\* crates/tickwire-account/src/lib.rs:1085-1255, crates/tickwire-account-gateway/src/lib.rs:614-652.
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
           /\ a1[i] = "absent"
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
\* crates/tickwire-account/src/lib.rs:1026-1080, crates/tickwire-account/src/admission.rs:112-125,
\* crates/tickwire-account/src/admission.rs:184-191, crates/tickwire-account-gateway/src/lib.rs:576-610.
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

\* FillRecordPool associative way selection: prefer first empty way, else oldest expired way.
\* crates/tickwire-account/src/fill_record.rs:238-277.
FreeWays(ways_fn) ==
    {w \in Ways : ~ways_fn[w].occupied}

ExpiredWays(ways_fn, t) ==
    {w \in Ways : ways_fn[w].occupied /\ IsExpired(ways_fn[w].recorded_at, t)}

HasUsableWay(ways_fn, t) ==
    FreeWays(ways_fn) # {} \/ ExpiredWays(ways_fn, t) # {}

PickFillWay(ways_fn, t) ==
    IF FreeWays(ways_fn) # {} THEN
        CHOOSE w \in FreeWays(ways_fn) : \A u \in FreeWays(ways_fn) : w <= u
    ELSE
        CHOOSE w \in ExpiredWays(ways_fn, t) :
            \A u \in ExpiredWays(ways_fn, t) :
                \/ ways_fn[w].recorded_at < ways_fn[u].recorded_at
                \/ (ways_fn[w].recorded_at = ways_fn[u].recorded_at /\ w <= u)

InFillPool(ways_fn, k) ==
    \E w \in Ways : ways_fn[w].occupied /\ ways_fn[w].ord = k

FindFillWay(ways_fn, k) ==
    CHOOSE w \in Ways : ways_fn[w].occupied /\ ways_fn[w].ord = k

Init ==
    /\ now = 1
    /\ acct_orders = [k \in OrderIntents |-> NoneOrder]
    /\ acct_adm = [i \in AllIntents |-> "absent"]
    /\ adm_retired_at = [i \in AllIntents |-> 0]
    /\ fill_ways = [w \in Ways |-> EmptyWay]
    /\ facts = [k \in OrderIntents |-> 0]
    /\ wallet_fee = [k \in OrderIntents |-> 0]
    /\ pending_send = 0
    /\ gw_status = [i \in AllIntents |-> "absent"]
    /\ gw_eval_at = [i \in AllIntents |-> 0]
    /\ gw_completed_at = [i \in AllIntents |-> 0]
    /\ ever_orders = {}
    /\ ever_mgmt = {}
    /\ ghost_orders = [k \in OrderIntents |-> NoneOrder]
    /\ ghost_retire_at = [i \in AllIntents |-> 0]
    /\ admit_count = [i \in AllIntents |-> 0]
    /\ fact_first_at = [k \in OrderIntents |-> 0]
    /\ fact_last_at = [k \in OrderIntents |-> 0]
    /\ trade_book_count = [k \in OrderIntents |-> 0]
    /\ ghost_expected_fee = [k \in OrderIntents |-> 0]
    /\ overflow_reclaimed = {}

\* Monotonic clock ticks across retention windows.
Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<
        acct_orders, acct_adm, adm_retired_at, fill_ways, facts, wallet_fee,
        pending_send, gw_status, gw_eval_at, gw_completed_at, ever_orders,
        ever_mgmt, ghost_orders, ghost_retire_at, admit_count, fact_first_at,
        fact_last_at, trade_book_count, ghost_expected_fee, overflow_reclaimed
       >>

\* Explicit lazy eviction sweep across Account and AccountGateway.
\* crates/tickwire-account/src/lib.rs:1085-1255, crates/tickwire-account-gateway/src/lib.rs:614-652.
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
        now, fill_ways, wallet_fee, pending_send, gw_eval_at, ever_orders,
        ever_mgmt, ghost_orders, ghost_retire_at, admit_count, fact_first_at,
        fact_last_at, trade_book_count, ghost_expected_fee, overflow_reclaimed
       >>

\* Submit a fresh order-creating New intent in monotonic strategy intent_id order:
\* runs evict_retired_entries(now) and enforces MaxOrders cap.
\* crates/tickwire-account/src/admission.rs:90-191, crates/tickwire-account-gateway/src/lib.rs:696-769.
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
             IN /\ acct_orders' = o2
                /\ facts' = f1
                /\ acct_adm' = a2
                /\ adm_retired_at' = ar2
                /\ pending_send' = k
                /\ gw_status' = st2
                /\ gw_eval_at' = ev2
                /\ gw_completed_at' = cp2
                /\ ever_orders' = ever_orders \cup {k}
                /\ ghost_orders' = [ghost_orders EXCEPT ![k] = new_ord]
                /\ ghost_retire_at' = [ghost_retire_at EXCEPT ![k] = 0]
                /\ admit_count' = [admit_count EXCEPT ![k] = @ + 1]
    /\ UNCHANGED <<
        now, fill_ways, wallet_fee, ever_mgmt, fact_first_at, fact_last_at,
        trade_book_count, ghost_expected_fee, overflow_reclaimed
       >>

\* Submit a Cancel/Query management intent targeting an open order in monotonic
\* intent_id order: uses independent MaxMgmt headroom and atomically reclaims the
\* oldest retired management entry across AccountGateway and Account when full,
\* even when all MaxOrders slots hold live resting orders.
\* crates/tickwire-account/src/lib.rs:1026-1080, crates/tickwire-account/src/admission.rs:94-159,
\* crates/tickwire-account-gateway/src/lib.rs:576-610, crates/tickwire-account-gateway/src/lib.rs:708-722.
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
           m == NumOrders + Cardinality(ever_mgmt) + 1
       IN /\ \E k \in OrderIntents : IsOpenOrder(o1, k)
          /\ (MgmtGatewayCount(st1) < MaxMgmt \/ gw_cands # {})
          /\ LET victim == IF MgmtGatewayCount(st1) >= MaxMgmt THEN PickOldestGwMgmt(gw_cands, cp1, gw_eval_at) ELSE 0
                 st_recl == IF victim # 0 THEN [st1 EXCEPT ![victim] = "absent"] ELSE st1
                 cp_recl == IF victim # 0 THEN [cp1 EXCEPT ![victim] = 0] ELSE cp1
                 a_recl == IF victim # 0 THEN [a1 EXCEPT ![victim] = "absent"] ELSE a1
                 ar_recl == IF victim # 0 THEN [ar1 EXCEPT ![victim] = 0] ELSE ar1
                 a2 == [a_recl EXCEPT ![m] = "active"]
                 ar2 == [ar_recl EXCEPT ![m] = 0]
                 st2 == [st_recl EXCEPT ![m] = "Queued"]
                 ev2 == [gw_eval_at EXCEPT ![m] = now]
                 cp2 == [cp_recl EXCEPT ![m] = 0]
                 reclaimed_set == IF victim # 0 THEN {victim} ELSE {}
             IN /\ MgmtAccountCount(a_recl) < MaxMgmt
                /\ acct_orders' = o1
                /\ facts' = f1
                /\ acct_adm' = a2
                /\ adm_retired_at' = ar2
                /\ pending_send' = m
                /\ gw_status' = st2
                /\ gw_eval_at' = ev2
                /\ gw_completed_at' = cp2
                /\ ever_mgmt' = ever_mgmt \cup {m}
                /\ ghost_retire_at' = [ghost_retire_at EXCEPT ![m] = 0]
                /\ admit_count' = [admit_count EXCEPT ![m] = @ + 1]
                /\ overflow_reclaimed' = overflow_reclaimed \cup reclaimed_set
    /\ UNCHANGED <<
        now, fill_ways, wallet_fee, ever_orders, ghost_orders, fact_first_at,
        fact_last_at, trade_book_count, ghost_expected_fee
       >>

\* Byte-identical retry of an admitted intent within its active/retention window:
\* hits gateway/account dedup without double-admitting; if prematurely evicted,
\* increments admit_count to trip WindowDedupPreserved.
\* crates/tickwire-account-gateway/src/lib.rs:662-665, crates/tickwire-account-gateway/src/pool.rs:429-435.
RetryDuplicateIntent ==
    /\ \E i \in (ever_orders \cup (ever_mgmt \ overflow_reclaimed)) :
        /\ (ghost_retire_at[i] = 0 \/ now < ghost_retire_at[i] + Window)
        /\ IF gw_status[i] # "absent" /\ acct_adm[i] # "absent"
           THEN UNCHANGED admit_count
           ELSE admit_count' = [admit_count EXCEPT ![i] = @ + 1]
    /\ UNCHANGED <<
        now, acct_orders, acct_adm, adm_retired_at, fill_ways, facts,
        wallet_fee, pending_send, gw_status, gw_eval_at, gw_completed_at,
        ever_orders, ever_mgmt, ghost_orders, ghost_retire_at, fact_first_at,
        fact_last_at, trade_book_count, ghost_expected_fee, overflow_reclaimed
       >>

\* Outbound TLS frame flushed (apply_report(SendState::Flushed)): retires management
\* admission at now and refreshes gateway completion stamps.
\* crates/tickwire-account-gateway/src/lib.rs:1173-1180.
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
        now, acct_orders, fill_ways, facts, wallet_fee, gw_eval_at,
        ever_orders, ever_mgmt, ghost_orders, admit_count, fact_first_at,
        fact_last_at, trade_book_count, ghost_expected_fee, overflow_reclaimed
       >>

\* Zero-byte unsent abort: marks order/management admission aborted and retired at now,
\* releasing holds and refreshing completion stamps.
\* crates/tickwire-account/src/admission.rs:590-632, crates/tickwire-account-gateway/src/lib.rs:1181-1211.
AbortPending ==
    /\ pending_send # 0
    /\ LET i == pending_send
           o1 == IF i \in OrderIntents
                 THEN [acct_orders EXCEPT ![i].aborted = TRUE,
                                          ![i].retired_at = now]
                 ELSE acct_orders
           g1 == IF i \in OrderIntents
                 THEN [ghost_orders EXCEPT ![i].aborted = TRUE,
                                           ![i].retired_at = now]
                 ELSE ghost_orders
           a1 == [acct_adm EXCEPT ![i] = "retired"]
           ar1 == [adm_retired_at EXCEPT ![i] = now]
           st1 == [gw_status EXCEPT ![i] = "Aborted"]
           cp1 == RefreshCompleted(o1, facts, a1, ar1, st1, gw_eval_at, gw_completed_at, 0, now)
       IN /\ pending_send' = 0
          /\ acct_orders' = o1
          /\ ghost_orders' = g1
          /\ acct_adm' = a1
          /\ adm_retired_at' = ar1
          /\ gw_status' = st1
          /\ gw_completed_at' = cp1
          /\ ghost_retire_at' = [ghost_retire_at EXCEPT ![i] = cp1[i]]
    /\ UNCHANGED <<
        now, fill_ways, facts, wallet_fee, gw_eval_at, ever_orders, ever_mgmt,
        admit_count, fact_first_at, fact_last_at, trade_book_count,
        ghost_expected_fee, overflow_reclaimed
       >>

\* Mid-send socket disconnect after accepted_bytes > 0 (per_order_doubt_isolation):
\* marks only the affected order PlaceAmbiguous (in_doubt = TRUE) and Status::Unknown
\* without halting the gateway, blocking order eviction until venue resolution.
\* crates/tickwire-account-gateway/src/lib.rs:1343-1475, crates/tickwire-account-gateway/src/pool.rs:846-862.
IsolatePendingInDoubt ==
    /\ pending_send \in OrderIntents
    /\ LET k == pending_send
           o1 == [acct_orders EXCEPT ![k].in_doubt = TRUE, ![k].retired_at = 0]
           g1 == [ghost_orders EXCEPT ![k].in_doubt = TRUE, ![k].retired_at = 0]
           st1 == [gw_status EXCEPT ![k] = "Unknown"]
       IN /\ pending_send' = 0
          /\ acct_orders' = o1
          /\ ghost_orders' = g1
          /\ gw_status' = st1
          /\ gw_completed_at' = [gw_completed_at EXCEPT ![k] = 0]
    /\ UNCHANGED <<
        now, acct_adm, adm_retired_at, fill_ways, facts, wallet_fee,
        gw_eval_at, ever_orders, ever_mgmt, ghost_retire_at, admit_count,
        fact_first_at, fact_last_at, trade_book_count, ghost_expected_fee,
        overflow_reclaimed
       >>

\* Venue reports terminal order status with cumulative = 1 BEFORE trade execution arrives:
\* terminal = TRUE, term_cum = 1, filled = 0 => non-zero remaining() blocks eviction!
\* crates/tickwire-account/src/lib.rs:250-291, crates/tickwire-account/src/events.rs:824-880.
ObserveTerminalPendingFill ==
    /\ \E k \in OrderIntents :
        /\ gw_status[k] \in {"Flushed", "Unknown"}
        /\ acct_orders[k].present
        /\ ~acct_orders[k].aborted
        /\ ~acct_orders[k].terminal
        /\ acct_orders[k].filled = 0
        /\ ~acct_orders[k].amend_inflight
        /\ LET o1 == [acct_orders EXCEPT ![k].terminal = TRUE,
                                         ![k].term_cum = 1,
                                         ![k].in_doubt = FALSE,
                                         ![k].retired_at = 0]
               g1 == [ghost_orders EXCEPT ![k].terminal = TRUE,
                                          ![k].term_cum = 1,
                                          ![k].in_doubt = FALSE,
                                          ![k].retired_at = 0]
           IN /\ acct_orders' = o1
              /\ ghost_orders' = g1
    /\ UNCHANGED <<
        now, acct_adm, adm_retired_at, fill_ways, facts, wallet_fee,
        pending_send, gw_status, gw_eval_at, gw_completed_at, ever_orders,
        ever_mgmt, ghost_retire_at, admit_count, fact_first_at, fact_last_at,
        trade_book_count, ghost_expected_fee, overflow_reclaimed
       >>

\* Authoritative venue Canceled status with zero cumulative fill resolves open or
\* in_doubt order cleanly and marks it retired at now when !unknown and !amend_inflight.
\* crates/tickwire-account/src/events.rs:824-880, crates/tickwire-account/src/lib.rs:871-912.
ObserveTerminalCanceled ==
    /\ \E k \in OrderIntents :
        /\ gw_status[k] \in {"Flushed", "Unknown"}
        /\ acct_orders[k].present
        /\ ~acct_orders[k].aborted
        /\ ~acct_orders[k].terminal
        /\ acct_orders[k].filled = 0
        /\ ~acct_orders[k].amend_inflight
        /\ LET base_o == [acct_orders[k] EXCEPT !.terminal = TRUE,
                                                !.term_cum = 0,
                                                !.in_doubt = FALSE]
               retirable == OrderRetirable(base_o)
               final_o == [base_o EXCEPT !.retired_at = IF retirable THEN now ELSE 0]
               o1 == [acct_orders EXCEPT ![k] = final_o]
               g1 == [ghost_orders EXCEPT ![k] = final_o]
               a1 == [acct_adm EXCEPT ![k] = IF retirable THEN "retired" ELSE "active"]
               ar1 == [adm_retired_at EXCEPT ![k] = IF retirable THEN now ELSE 0]
               cp1 == RefreshCompleted(o1, facts, a1, ar1, gw_status, gw_eval_at, gw_completed_at, pending_send, now)
           IN /\ acct_orders' = o1
              /\ ghost_orders' = g1
              /\ acct_adm' = a1
              /\ adm_retired_at' = ar1
              /\ gw_completed_at' = cp1
              /\ ghost_retire_at' =
                    IF retirable
                    THEN [ghost_retire_at EXCEPT ![k] = Max(now, cp1[k])]
                    ELSE ghost_retire_at
    /\ UNCHANGED <<
        now, fill_ways, facts, wallet_fee, pending_send, gw_status, gw_eval_at,
        ever_orders, ever_mgmt, admit_count, fact_first_at, fact_last_at,
        trade_book_count, ghost_expected_fee, overflow_reclaimed
       >>

\* Venue trade execution (TRADE_LITE with omitted fee or full TRADE with fee1):
\* FillRecordPool::record_or_amend inserts into an empty or oldest-expired bucket way,
\* books 1 lot fill, settles initial fee, records FactEntry at now, clears in_doubt,
\* and retires the order iff OrderRetirable holds.
\* crates/tickwire-account/src/fill_record.rs:171-277, crates/tickwire-account/src/events.rs:381-385.
BookTradeFill ==
    /\ \E k \in OrderIntents, init_fee \in {"omitted", "fee1"} :
        /\ gw_status[k] \in {"Flushed", "Unknown"}
        /\ acct_orders[k].present
        /\ ~acct_orders[k].aborted
        /\ acct_orders[k].filled = 0
        /\ ~(acct_orders[k].terminal /\ acct_orders[k].term_cum = 0)
        /\ ~acct_orders[k].amend_inflight
        /\ ~InFillPool(fill_ways, k)
        /\ HasUsableWay(fill_ways, now)
        /\ LET w == PickFillWay(fill_ways, now)
               base_o == [acct_orders[k] EXCEPT !.filled = 1,
                                                !.term_cum = 1,
                                                !.terminal = TRUE,
                                                !.in_doubt = FALSE]
               retirable == OrderRetirable(base_o)
               final_o == [base_o EXCEPT !.retired_at = IF retirable THEN now ELSE 0]
               o1 == [acct_orders EXCEPT ![k] = final_o]
               g1 == [ghost_orders EXCEPT ![k] = final_o]
               f1 == [facts EXCEPT ![k] = now]
               a1 == [acct_adm EXCEPT ![k] = IF retirable THEN "retired" ELSE "active"]
               ar1 == [adm_retired_at EXCEPT ![k] = IF retirable THEN now ELSE 0]
               cp1 == RefreshCompleted(o1, f1, a1, ar1, gw_status, gw_eval_at, gw_completed_at, pending_send, now)
               fv == FeeVal(init_fee)
           IN /\ fill_ways' = [fill_ways EXCEPT ![w] = [
                     occupied |-> TRUE,
                     ord |-> k,
                     fee_state |-> init_fee,
                     recorded_at |-> now]]
              /\ acct_orders' = o1
              /\ ghost_orders' = g1
              /\ facts' = f1
              /\ wallet_fee' = [wallet_fee EXCEPT ![k] = fv]
              /\ acct_adm' = a1
              /\ adm_retired_at' = ar1
              /\ gw_completed_at' = cp1
              /\ ghost_retire_at' =
                    IF retirable
                    THEN [ghost_retire_at EXCEPT ![k] = Max(now, cp1[k])]
                    ELSE ghost_retire_at
              /\ fact_first_at' = [fact_first_at EXCEPT ![k] = now]
              /\ fact_last_at' = [fact_last_at EXCEPT ![k] = now]
              /\ trade_book_count' = [trade_book_count EXCEPT ![k] = @ + 1]
              /\ ghost_expected_fee' = [ghost_expected_fee EXCEPT ![k] = fv]
    /\ UNCHANGED <<
        now, pending_send, gw_status, gw_eval_at, ever_orders, ever_mgmt,
        admit_count, overflow_reclaimed
       >>

\* Redundant trade_id execution (late TRADE_LITE or byte-identical TRADE across order
\* socket and user stream): deduplicated via FillRecordPool (within retention_ns) and
\* Account.facts (while resident); if missing prematurely, increments trade_book_count!
\* crates/tickwire-account/src/fill_record.rs:181-205, crates/tickwire-account/src/events.rs:246-359.
DuplicateTradeFill ==
    /\ \E k \in ever_orders :
        /\ fact_first_at[k] > 0
        /\ (acct_orders[k].present \/ now < fact_last_at[k] + Window)
        /\ LET pool_ok == (now >= fact_last_at[k] + Window) \/ InFillPool(fill_ways, k)
               acct_ok == facts[k] > 0
           IN IF pool_ok /\ acct_ok
              THEN UNCHANGED trade_book_count
              ELSE trade_book_count' = [trade_book_count EXCEPT ![k] = @ + 1]
    /\ UNCHANGED <<
        now, acct_orders, acct_adm, adm_retired_at, fill_ways, facts,
        wallet_fee, pending_send, gw_status, gw_eval_at, gw_completed_at,
        ever_orders, ever_mgmt, ghost_orders, ghost_retire_at, admit_count,
        fact_first_at, fact_last_at, ghost_expected_fee, overflow_reclaimed
       >>

\* Post-trade fee upgrade (TRADE arriving after TRADE_LITE) or fee/rebate revision
\* (FillRecordOutcome::Amended, amendment = true): reverses previous_fee and applies
\* new_fee delta ONLY to wallet_fee without re-booking filled quantity, while refreshing
\* FillSlot.recorded_mono_ns, FactEntry.recorded_mono_ns, and retirement timestamps!
\* crates/tickwire-account/src/fill_record.rs:188-233, crates/tickwire-account/src/events.rs:272-331.
AmendTradeFee ==
    /\ \E k \in OrderIntents, new_fee \in {"fee1", "fee2"} :
        /\ acct_orders[k].present
        /\ acct_orders[k].filled = 1
        /\ InFillPool(fill_ways, k)
        /\ LET w == FindFillWay(fill_ways, k)
               prev_fs == fill_ways[w].fee_state
               prev_val == FeeVal(prev_fs)
               new_val == FeeVal(new_fee)
               delta == new_val - prev_val
               f1 == [facts EXCEPT ![k] = now]
               retirable == OrderRetirable(acct_orders[k])
               ar1 == IF retirable
                      THEN [adm_retired_at EXCEPT ![k] = Max(acct_orders[k].retired_at, now)]
                      ELSE adm_retired_at
               cp1 == RefreshCompleted(acct_orders, f1, acct_adm, ar1, gw_status, gw_eval_at, gw_completed_at, pending_send, now)
           IN /\ new_val > prev_val
              /\ fill_ways' = [fill_ways EXCEPT ![w].fee_state = new_fee,
                                                ![w].recorded_at = Max(@, now)]
              /\ facts' = f1
              /\ wallet_fee' = [wallet_fee EXCEPT ![k] = @ + delta]
              /\ adm_retired_at' = ar1
              /\ gw_completed_at' = cp1
              /\ fact_last_at' = [fact_last_at EXCEPT ![k] = now]
              /\ ghost_expected_fee' = [ghost_expected_fee EXCEPT ![k] = new_val]
              /\ ghost_retire_at' =
                    IF retirable
                    THEN [ghost_retire_at EXCEPT ![k] = Max(@, Max(now, cp1[k]))]
                    ELSE ghost_retire_at
    /\ UNCHANGED <<
        now, acct_orders, acct_adm, pending_send, gw_status, gw_eval_at,
        ever_orders, ever_mgmt, ghost_orders, admit_count, fact_first_at,
        trade_book_count, overflow_reclaimed
       >>

\* Venue ambiguity / user-stream gap latches observed_unknown = TRUE, permanently
\* blocking retirement regardless of terminal status or clock advance.
\* crates/tickwire-account/src/lib.rs:247-291, crates/tickwire-account/src/events.rs:820-823.
MarkOrderUnknown ==
    /\ \E k \in OrderIntents :
        /\ gw_status[k] \in {"Flushed", "Unknown"}
        /\ acct_orders[k].present
        /\ ~acct_orders[k].aborted
        /\ ~acct_orders[k].terminal
        /\ ~acct_orders[k].unknown
        /\ acct_orders' = [acct_orders EXCEPT ![k].unknown = TRUE, ![k].retired_at = 0]
        /\ ghost_orders' = [ghost_orders EXCEPT ![k].unknown = TRUE, ![k].retired_at = 0]
    /\ UNCHANGED <<
        now, acct_adm, adm_retired_at, fill_ways, facts, wallet_fee,
        pending_send, gw_status, gw_eval_at, gw_completed_at, ever_orders,
        ever_mgmt, ghost_retire_at, admit_count, fact_first_at, fact_last_at,
        trade_book_count, ghost_expected_fee, overflow_reclaimed
       >>

\* In-flight amendment on an open order blocks Account::is_order_retirable until resolved.
\* crates/tickwire-account/src/lib.rs:844-860, crates/tickwire-account/src/admission.rs:104-138.
StartInflightAmend ==
    /\ \E k \in OrderIntents :
        /\ gw_status[k] = "Flushed"
        /\ IsOpenOrder(acct_orders, k)
        /\ ~acct_orders[k].in_doubt
        /\ ~acct_orders[k].amend_inflight
        /\ ~acct_orders[k].amend_done
        /\ acct_orders' = [acct_orders EXCEPT ![k].amend_inflight = TRUE,
                                              ![k].amend_done = TRUE,
                                              ![k].retired_at = 0]
        /\ ghost_orders' = [ghost_orders EXCEPT ![k].amend_inflight = TRUE,
                                                ![k].amend_done = TRUE,
                                                ![k].retired_at = 0]
    /\ UNCHANGED <<
        now, acct_adm, adm_retired_at, fill_ways, facts, wallet_fee,
        pending_send, gw_status, gw_eval_at, gw_completed_at, ever_orders,
        ever_mgmt, ghost_retire_at, admit_count, fact_first_at, fact_last_at,
        trade_book_count, ghost_expected_fee, overflow_reclaimed
       >>

\* Amendment rejected/confirmed resolves in-flight amendment protection on the order.
\* crates/tickwire-account/src/amendments.rs:171-205, crates/tickwire-account/src/lib.rs:844-912.
FinishInflightAmend ==
    /\ \E k \in OrderIntents :
        /\ acct_orders[k].present
        /\ acct_orders[k].amend_inflight
        /\ acct_orders' = [acct_orders EXCEPT ![k].amend_inflight = FALSE]
        /\ ghost_orders' = [ghost_orders EXCEPT ![k].amend_inflight = FALSE]
    /\ UNCHANGED <<
        now, acct_adm, adm_retired_at, fill_ways, facts, wallet_fee,
        pending_send, gw_status, gw_eval_at, gw_completed_at, ever_orders,
        ever_mgmt, ghost_retire_at, admit_count, fact_first_at, fact_last_at,
        trade_book_count, ghost_expected_fee, overflow_reclaimed
       >>

Next ==
    \/ Tick
    \/ EvictExpired
    \/ SubmitNewOrder
    \/ SubmitManagement
    \/ RetryDuplicateIntent
    \/ FlushPending
    \/ AbortPending
    \/ IsolatePendingInDoubt
    \/ ObserveTerminalPendingFill
    \/ ObserveTerminalCanceled
    \/ BookTradeFill
    \/ DuplicateTradeFill
    \/ AmendTradeFee
    \/ MarkOrderUnknown
    \/ StartInflightAmend
    \/ FinishInflightAmend

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ now \in 1..MaxTime
    /\ acct_orders \in [OrderIntents -> OrderValues]
    /\ acct_adm \in [AllIntents -> AdmStates]
    /\ adm_retired_at \in [AllIntents -> 0..MaxTime]
    /\ fill_ways \in [Ways -> WayValues]
    /\ facts \in [OrderIntents -> 0..MaxTime]
    /\ wallet_fee \in [OrderIntents -> 0..2]
    /\ pending_send \in 0..(NumOrders + NumMgmt)
    /\ gw_status \in [AllIntents -> GwStatuses]
    /\ gw_eval_at \in [AllIntents -> 0..MaxTime]
    /\ gw_completed_at \in [AllIntents -> 0..MaxTime]
    /\ ever_orders \subseteq OrderIntents
    /\ ever_mgmt \subseteq MgmtIntents
    /\ ghost_orders \in [OrderIntents -> OrderValues]
    /\ ghost_retire_at \in [AllIntents -> 0..MaxTime]
    /\ admit_count \in [AllIntents -> 0..2]
    /\ fact_first_at \in [OrderIntents -> 0..MaxTime]
    /\ fact_last_at \in [OrderIntents -> 0..MaxTime]
    /\ trade_book_count \in [OrderIntents -> 0..2]
    /\ ghost_expected_fee \in [OrderIntents -> 0..2]
    /\ overflow_reclaimed \subseteq MgmtIntents
    /\ OrderGatewayCount(gw_status) <= MaxOrders
    /\ OrderAccountCount(acct_orders, acct_adm) <= MaxOrders
    /\ MgmtGatewayCount(gw_status) <= MaxMgmt
    /\ MgmtAccountCount(acct_adm) <= MaxMgmt

GhostNonRetirable(k) ==
    LET g == ghost_orders[k]
    IN /\ g.present
       /\ \/ g.unknown
          \/ g.in_doubt
          \/ g.amend_inflight
          \/ (~g.aborted /\ (~g.terminal \/ g.term_cum # g.filled))

\* No non-retirable order (live resting, unknown, in_doubt transport ambiguity,
\* non-zero remaining, or in-flight amendment) is EVER evicted from Account or
\* AccountGateway regardless of how far now advances across retention windows.
ActiveNeverEvicted ==
    \A k \in ever_orders :
        GhostNonRetirable(k) =>
            /\ acct_orders[k].present
            /\ acct_adm[k] = "active"
            /\ gw_status[k] # "absent"
            /\ acct_orders[k].retired_at = 0
            /\ gw_completed_at[k] = 0

\* Within [retired_at, retired_at + Window) and [recorded_at, recorded_at + Window),
\* duplicate IntentKeys and duplicate trade_id fills remain resident across Account,
\* AccountGateway, and FillRecordPool with zero double-admission or double-booking.
WindowDedupPreserved ==
    /\ \A i \in AllIntents : admit_count[i] <= 1
    /\ \A k \in ever_orders :
        /\ trade_book_count[k] <= 1
        /\ (acct_orders[k].present => acct_orders[k].filled = trade_book_count[k])
        /\ ((ghost_retire_at[k] = 0 \/ now < ghost_retire_at[k] + Window)
            => /\ acct_orders[k].present
               /\ acct_adm[k] # "absent"
               /\ gw_status[k] # "absent")
        /\ ((fact_last_at[k] > 0 /\ now < fact_last_at[k] + Window)
            => /\ facts[k] = fact_last_at[k]
               /\ InFillPool(fill_ways, k))
        /\ ((fact_last_at[k] > 0 /\ GhostNonRetirable(k))
            => facts[k] = fact_last_at[k])
    /\ \A m \in (ever_mgmt \ overflow_reclaimed) :
        (ghost_retire_at[m] = 0 \/ now < ghost_retire_at[m] + Window)
        => /\ acct_adm[m] # "absent"
           /\ gw_status[m] # "absent"

\* Post-trade TRADE_LITE -> TRADE fee upgrades and fee/rebate amendments
\* (amendment = true) settle exact fee deltas without re-booking filled quantity
\* or aliasing distinct keys inside FillRecordPool ways.
FillAmendmentExactness ==
    /\ \A k \in OrderIntents :
        /\ acct_orders[k].filled <= 1
        /\ wallet_fee[k] = ghost_expected_fee[k]
    /\ \A w1, w2 \in Ways :
        (w1 # w2 /\ fill_ways[w1].occupied /\ fill_ways[w2].occupied)
        => fill_ways[w1].ord # fill_ways[w2].ord

\* Even when all MaxOrders slots hold live resting orders, whenever the outbound wire
\* is idle (pending_send = 0) and an open order exists, Cancel/Query headroom is
\* available either directly or via eager retired-management reclamation.
ManagementNeverStarved ==
    (pending_send = 0 /\ \E k \in OrderIntents : IsOpenOrder(acct_orders, k))
    => /\ (MgmtGatewayCount(gw_status) < MaxMgmt
           \/ ReclaimableGatewayMgmt(gw_status, acct_adm, pending_send) # {})
       /\ (MgmtAccountCount(acct_adm) < MaxMgmt
           \/ ReclaimableAccountMgmt(acct_adm) # {})
=============================================================================
