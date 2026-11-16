---- MODULE L0_OrsQueryDesk ----
\* Code-faithful L0 TLA+ model of per-account `OrsQueryDesk` reconciliation,
\* per-order isolation, `Place` + `Cancel` `cancel_after` coalescing,
\* exponential `Cancel` retry backoff, catch-up fills before remainder cancel,
\* `-2013` / `110001` / `51603` `NotFound` disambiguation, non-`Live` eviction,
\* and `max_query_count` (`None`/`Some(0)` unbounded vs opt-in `Some(limit)`).
\*
\* Production Rust anchors (37-crate main baseline):
\* - `crates/tickwire-account/src/ors_query_desk.rs:13-425`
\* - `crates/tickwire-account/src/order_manager.rs:1508-1536`
\* - `crates/tickwire-account/src/order_manager.rs:2150-2249`
\* - `crates/tickwire-account/src/order_manager.rs:2449-2656`
\* - `crates/tickwire-binance-account/src/session.rs:2219-2280`
\* - `crates/tickwire-binance-account/src/session.rs:2495-2520`
\* - `crates/tickwire-binance-account/src/session.rs:2948-3279`
\* - `crates/tickwire-binance-execution/src/lib.rs:36-65`
\* - `crates/tickwire-spot-execution/src/rest_bootstrap.rs:896-1215`
\* - `crates/tickwire-spot-execution/src/session.rs:2141-2287`
\* - `crates/tickwire-spot-execution/src/session.rs:3012-3074`
\* - `crates/tickwire-spot-execution/src/session.rs:3526-3543`

EXTENDS Naturals, Integers, FiniteSets

CONSTANTS
    DeskCap,       \* Preallocated per-account OrsQueryDesk capacity (e.g. 1)
    MaxQueryCount  \* 0 => None / Some(0) unbounded retry; > 0 => opt-in INTERNAL_CANCEL cap

Orders == {1, 2}
DeskSlots == 1..DeskCap
OrderQty == 2
InitialBudget == 4
MaxClock == 1
DelayTicks == 1
MaxCancelBackoff == 2

VARIABLES
    \* var <-> code: `now_mono_ns` monotonic clock passed to `OrsQueryDesk` (`crates/tickwire-account/src/ors_query_desk.rs:46-47`, `crates/tickwire-account/src/ors_query_desk.rs:314-320`)
    now_tick,
    \* var <-> code: `UtaWalletLedger` free balance (`crates/tickwire-account/src/uta_ledger.rs:21-26`, `crates/tickwire-account/src/uta_ledger.rs:206-275`)
    wallet_free,
    \* var <-> code: `UtaWalletLedger` open-order hold (`crates/tickwire-account/src/uta_ledger.rs:21-26`, `crates/tickwire-account/src/order_manager.rs:520-523`)
    wallet_locked,
    \* var <-> code: `DopState::next_order_seqnum` single-epoch admission guard (`crates/tickwire-account/src/dop_state.rs:236-267`)
    order_submitted,
    \* var <-> code: bounded per-order `OrsQueryDesk` enqueue budget for finite state-space verification (`crates/tickwire-account/src/ors_query_desk.rs:165-310`)
    desk_enqueues,
    \* var <-> code: `OrsOrder::status` (`crates/tickwire-account/src/order_manager.rs:455-464`)
    order_status,
    \* var <-> code: `OrsOrder::filled_qty_lots` (`crates/tickwire-account/src/order_manager.rs:514-518`)
    filled_qty,
    \* var <-> code: `OrsCanceledReport::canceled_qty_lots` / released unexecuted lots (`crates/tickwire-account/src/order_manager.rs:2500-2514`, `crates/tickwire-account/src/order_manager.rs:2610-2648`)
    canceled_qty,
    \* var <-> code: `OrsOrder::pending_cxl` (`crates/tickwire-account/src/order_manager.rs:509-510`, `crates/tickwire-binance-account/src/session.rs:3128-3133`)
    pending_cxl,
    \* var <-> code: `OrsOrder::query_fail_count` (`crates/tickwire-account/src/order_manager.rs:513-514`, `crates/tickwire-account/src/order_manager.rs:2493-2498`)
    query_fail_count,
    \* var <-> code: terminal classification emitted by `OrderManager` (`crates/tickwire-account/src/order_manager.rs:2488-2656`)
    terminal_reason,
    \* var <-> code: `OrsQueryReconcileAction::DispatchCancel` coalesced cancel emission (`crates/tickwire-account/src/order_manager.rs:2582-2595`, `crates/tickwire-binance-account/src/session.rs:3200-3203`, `crates/tickwire-spot-execution/src/session.rs:2269-2285`)
    coalesced_cxl_dispatched,
    \* var <-> code: `OrsQueryEntry::occupied` (`crates/tickwire-account/src/ors_query_desk.rs:30-31`)
    desk_occupied,
    \* var <-> code: `OrsQueryEntry::ors_order_id` (`crates/tickwire-account/src/ors_query_desk.rs:32-33`)
    desk_oid,
    \* var <-> code: `OrsQueryEntry::origin` (`crates/tickwire-account/src/ors_query_desk.rs:21-26`, `crates/tickwire-account/src/ors_query_desk.rs:38-39`)
    desk_origin,
    \* var <-> code: `OrsQueryEntry::cancel_after` (`crates/tickwire-account/src/ors_query_desk.rs:39-40`, `crates/tickwire-account/src/ors_query_desk.rs:186-188`, `crates/tickwire-account/src/ors_query_desk.rs:231-241`)
    desk_cancel_after,
    \* var <-> code: `OrsQueryEntry::query_attempts` (`crates/tickwire-account/src/ors_query_desk.rs:41-42`, `crates/tickwire-account/src/ors_query_desk.rs:361-368`)
    desk_query_attempts,
    \* var <-> code: `OrsQueryEntry::cancel_attempts` (`crates/tickwire-account/src/ors_query_desk.rs:43-44`, `crates/tickwire-account/src/ors_query_desk.rs:269-275`, `crates/tickwire-account/src/ors_query_desk.rs:353-359`)
    desk_cancel_attempts,
    \* var <-> code: `OrsQueryEntry::cancel_exhausted` (`crates/tickwire-account/src/ors_query_desk.rs:45-46`, `crates/tickwire-account/src/ors_query_desk.rs:269-271`)
    desk_cancel_exhausted,
    \* var <-> code: `OrsQueryEntry::next_due_mono_ns` (`crates/tickwire-account/src/ors_query_desk.rs:46-47`, `crates/tickwire-account/src/ors_query_desk.rs:189-191`)
    desk_due,
    \* var <-> code: inflight `StatusAsk` task in `AsyncRestQuerier` / WS status request (`crates/tickwire-spot-execution/src/session.rs:2162-2174`, `crates/tickwire-binance-account/src/session.rs:2948-3004`)
    inflight_ask,
    \* var <-> code: witness that full-table cleanup never evicted a `SlotState::Live` entry (`crates/tickwire-account/src/ors_query_desk.rs:197-206`, `crates/tickwire-account/src/ors_query_desk.rs:281-289`)
    evicted_live_violation,
    \* var <-> code: `RefusalReason::TooManyPendingActions` returned when all desk slots hold `Live` orders (`crates/tickwire-account/src/ors_query_desk.rs:204-206`)
    refused_full_all_live

vars == <<
    now_tick, wallet_free, wallet_locked, order_submitted, desk_enqueues,
    order_status, filled_qty, canceled_qty, pending_cxl, query_fail_count,
    terminal_reason, coalesced_cxl_dispatched, desk_occupied, desk_oid,
    desk_origin, desk_cancel_after, desk_query_attempts, desk_cancel_attempts,
    desk_cancel_exhausted, desk_due, inflight_ask, evicted_live_violation,
    refused_full_all_live
>>

orderVars == <<
    wallet_free, wallet_locked, order_submitted, desk_enqueues, order_status,
    filled_qty, canceled_qty, pending_cxl, query_fail_count, terminal_reason,
    coalesced_cxl_dispatched
>>

deskVars == <<
    desk_occupied, desk_oid, desk_origin, desk_cancel_after,
    desk_query_attempts, desk_cancel_attempts, desk_cancel_exhausted, desk_due
>>

SlotLive(oid) ==
    order_status[oid] \in {"PendingNew", "Open"}

MinClock(t) ==
    IF t > MaxClock THEN MaxClock ELSE t

HasDeskEntry(oid) ==
    \E d \in DeskSlots : desk_occupied[d] /\ desk_oid[d] = oid

ClearOrderFromDesk(oid) ==
    /\ desk_occupied' = [d \in DeskSlots |-> IF desk_occupied[d] /\ desk_oid[d] = oid THEN FALSE ELSE desk_occupied[d]]
    /\ desk_oid' = [d \in DeskSlots |-> IF desk_occupied[d] /\ desk_oid[d] = oid THEN 0 ELSE desk_oid[d]]
    /\ desk_origin' = [d \in DeskSlots |-> IF desk_occupied[d] /\ desk_oid[d] = oid THEN "Place" ELSE desk_origin[d]]
    /\ desk_cancel_after' = [d \in DeskSlots |-> IF desk_occupied[d] /\ desk_oid[d] = oid THEN FALSE ELSE desk_cancel_after[d]]
    /\ desk_query_attempts' = [d \in DeskSlots |-> IF desk_occupied[d] /\ desk_oid[d] = oid THEN 0 ELSE desk_query_attempts[d]]
    /\ desk_cancel_attempts' = [d \in DeskSlots |-> IF desk_occupied[d] /\ desk_oid[d] = oid THEN 0 ELSE desk_cancel_attempts[d]]
    /\ desk_cancel_exhausted' = [d \in DeskSlots |-> IF desk_occupied[d] /\ desk_oid[d] = oid THEN FALSE ELSE desk_cancel_exhausted[d]]
    /\ desk_due' = [d \in DeskSlots |-> IF desk_occupied[d] /\ desk_oid[d] = oid THEN 0 ELSE desk_due[d]]

SpentOnFills ==
    filled_qty[1] + filled_qty[2]

ExpectedLocked ==
    (IF SlotLive(1) THEN OrderQty - filled_qty[1] ELSE 0) +
    (IF SlotLive(2) THEN OrderQty - filled_qty[2] ELSE 0)

Init ==
    /\ now_tick = 0
    /\ wallet_free = InitialBudget
    /\ wallet_locked = 0
    /\ order_submitted = [o \in Orders |-> FALSE]
    /\ desk_enqueues = [o \in Orders |-> 0]
    /\ order_status = [o \in Orders |-> "Free"]
    /\ filled_qty = [o \in Orders |-> 0]
    /\ canceled_qty = [o \in Orders |-> 0]
    /\ pending_cxl = [o \in Orders |-> 0]
    /\ query_fail_count = [o \in Orders |-> 0]
    /\ terminal_reason = [o \in Orders |-> "None"]
    /\ coalesced_cxl_dispatched = [o \in Orders |-> FALSE]
    /\ desk_occupied = [d \in DeskSlots |-> FALSE]
    /\ desk_oid = [d \in DeskSlots |-> 0]
    /\ desk_origin = [d \in DeskSlots |-> "Place"]
    /\ desk_cancel_after = [d \in DeskSlots |-> FALSE]
    /\ desk_query_attempts = [d \in DeskSlots |-> 0]
    /\ desk_cancel_attempts = [d \in DeskSlots |-> 0]
    /\ desk_cancel_exhausted = [d \in DeskSlots |-> FALSE]
    /\ desk_due = [d \in DeskSlots |-> 0]
    /\ inflight_ask = [o \in Orders |-> "None"]
    /\ evicted_live_violation = FALSE
    /\ refused_full_all_live = FALSE

\* Sibling order submission proceeds independently even if another order is wedged in `OrsQueryDesk`
\* (`crates/tickwire-account/src/order_manager.rs:1245-1460`).
SubmitOrder(oid) ==
    /\ ~order_submitted[oid]
    /\ (oid = 2 => order_submitted[1])
    /\ wallet_free >= OrderQty
    /\ wallet_free' = wallet_free - OrderQty
    /\ wallet_locked' = wallet_locked + OrderQty
    /\ order_submitted' = [order_submitted EXCEPT ![oid] = TRUE]
    /\ order_status' = [order_status EXCEPT ![oid] = "PendingNew"]
    /\ UNCHANGED <<
        now_tick, desk_enqueues, filled_qty, canceled_qty, pending_cxl, query_fail_count,
        terminal_reason, coalesced_cxl_dispatched, deskVars, inflight_ask,
        evicted_live_violation, refused_full_all_live
       >>

\* Normal WS `Ack` (`PendingNew -> Open`, `crates/tickwire-account/src/order_manager.rs:1635-1715`).
HotWsAckOrder(oid) ==
    /\ order_status[oid] = "PendingNew"
    /\ order_status' = [order_status EXCEPT ![oid] = "Open"]
    /\ UNCHANGED <<
        now_tick, wallet_free, wallet_locked, order_submitted, desk_enqueues, filled_qty,
        canceled_qty, pending_cxl, query_fail_count, terminal_reason,
        coalesced_cxl_dispatched, deskVars, inflight_ask,
        evicted_live_violation, refused_full_all_live
       >>

\* Hot-path WS full fill retires `SlotState` (`!is_slot_live`) without locking `OrsQueryDesk`,
\* allowing lazy non-`Live` pruning or full-table non-`Live` eviction (`crates/tickwire-account/src/order_manager.rs:1980-2025`).
HotWsFillAndRetire(oid) ==
    /\ SlotLive(oid)
    /\ LET rem == OrderQty - filled_qty[oid] IN
       /\ wallet_locked' = wallet_locked - rem
       /\ filled_qty' = [filled_qty EXCEPT ![oid] = OrderQty]
       /\ order_status' = [order_status EXCEPT ![oid] = "Terminal"]
       /\ terminal_reason' = [terminal_reason EXCEPT ![oid] = "Filled"]
       /\ inflight_ask' = [inflight_ask EXCEPT ![oid] = "None"]
       /\ UNCHANGED <<
           now_tick, wallet_free, order_submitted, desk_enqueues, canceled_qty, pending_cxl,
           query_fail_count, coalesced_cxl_dispatched, deskVars,
           evicted_live_violation, refused_full_all_live
          >>

\* Ambiguous `-1007` / `-1000` / `MKT_DELAYED` or `SocketDrop` schedules `StatusAsk` on a free desk slot
\* (`crates/tickwire-account/src/ors_query_desk.rs:165-226`, `crates/tickwire-binance-account/src/session.rs:2219-2280`).
ScheduleStatusAskFreeSlot(oid, origin) ==
    /\ SlotLive(oid)
    /\ ~HasDeskEntry(oid)
    /\ desk_enqueues[oid] < 2
    /\ (origin = "Cancel" => order_status[oid] = "Open")
    /\ (origin = "Cancel" => pending_cxl[oid] < 2)
    /\ \E d \in DeskSlots :
        /\ ~desk_occupied[d]
        /\ LET due == IF origin = "SocketDrop" THEN now_tick ELSE MinClock(now_tick + DelayTicks) IN
           /\ desk_occupied' = [desk_occupied EXCEPT ![d] = TRUE]
           /\ desk_oid' = [desk_oid EXCEPT ![d] = oid]
           /\ desk_origin' = [desk_origin EXCEPT ![d] = origin]
           /\ desk_cancel_after' = [desk_cancel_after EXCEPT ![d] = (origin = "Cancel")]
           /\ desk_query_attempts' = [desk_query_attempts EXCEPT ![d] = 0]
           /\ desk_cancel_attempts' = [desk_cancel_attempts EXCEPT ![d] = 0]
           /\ desk_cancel_exhausted' = [desk_cancel_exhausted EXCEPT ![d] = FALSE]
           /\ desk_due' = [desk_due EXCEPT ![d] = due]
           /\ desk_enqueues' = [desk_enqueues EXCEPT ![oid] = @ + 1]
           /\ pending_cxl' = [pending_cxl EXCEPT ![oid] = IF origin = "Cancel" THEN @ + 1 ELSE @]
    /\ UNCHANGED <<
        now_tick, wallet_free, wallet_locked, order_submitted, order_status,
        filled_qty, canceled_qty, query_fail_count, terminal_reason,
        coalesced_cxl_dispatched, inflight_ask, evicted_live_violation,
        refused_full_all_live
       >>

\* Full-table cleanup in `OrsQueryDesk::schedule_status_ask` evicts ONLY `!is_order_live` entries
\* (`crates/tickwire-account/src/ors_query_desk.rs:197-207`).
ScheduleOnFullEvictNonLive(oid) ==
    /\ SlotLive(oid)
    /\ ~HasDeskEntry(oid)
    /\ desk_enqueues[oid] < 2
    /\ \A d \in DeskSlots : desk_occupied[d]
    /\ \E d \in DeskSlots :
        /\ ~SlotLive(desk_oid[d])
        /\ LET due == MinClock(now_tick + DelayTicks) IN
           /\ evicted_live_violation' = (evicted_live_violation \/ SlotLive(desk_oid[d]))
           /\ desk_enqueues' = [desk_enqueues EXCEPT ![oid] = @ + 1]
           /\ desk_occupied' = [k \in DeskSlots |-> IF k = d THEN TRUE ELSE IF ~SlotLive(desk_oid[k]) THEN FALSE ELSE desk_occupied[k]]
           /\ desk_oid' = [k \in DeskSlots |-> IF k = d THEN oid ELSE IF ~SlotLive(desk_oid[k]) THEN 0 ELSE desk_oid[k]]
           /\ desk_origin' = [k \in DeskSlots |-> IF k = d THEN "Place" ELSE IF ~SlotLive(desk_oid[k]) THEN "Place" ELSE desk_origin[k]]
           /\ desk_cancel_after' = [k \in DeskSlots |-> IF k = d THEN FALSE ELSE IF ~SlotLive(desk_oid[k]) THEN FALSE ELSE desk_cancel_after[k]]
           /\ desk_query_attempts' = [k \in DeskSlots |-> IF k = d THEN 0 ELSE IF ~SlotLive(desk_oid[k]) THEN 0 ELSE desk_query_attempts[k]]
           /\ desk_cancel_attempts' = [k \in DeskSlots |-> IF k = d THEN 0 ELSE IF ~SlotLive(desk_oid[k]) THEN 0 ELSE desk_cancel_attempts[k]]
           /\ desk_cancel_exhausted' = [k \in DeskSlots |-> IF k = d THEN FALSE ELSE IF ~SlotLive(desk_oid[k]) THEN FALSE ELSE desk_cancel_exhausted[k]]
           /\ desk_due' = [k \in DeskSlots |-> IF k = d THEN due ELSE IF ~SlotLive(desk_oid[k]) THEN 0 ELSE desk_due[k]]
    /\ UNCHANGED <<
        now_tick, wallet_free, wallet_locked, order_submitted, order_status,
        filled_qty, canceled_qty, pending_cxl, query_fail_count, terminal_reason,
        coalesced_cxl_dispatched, inflight_ask, refused_full_all_live
       >>

\* When every occupied desk slot belongs to an active `SlotState::Live` order, `schedule_status_ask`
\* refuses with `RefusalReason::TooManyPendingActions` rather than evicting any `Live` entry
\* (`crates/tickwire-account/src/ors_query_desk.rs:204-206`).
ScheduleRefuseFullAllLive(oid) ==
    /\ SlotLive(oid)
    /\ ~HasDeskEntry(oid)
    /\ ~refused_full_all_live
    /\ \A d \in DeskSlots : desk_occupied[d] /\ SlotLive(desk_oid[d])
    /\ refused_full_all_live' = TRUE
    /\ UNCHANGED <<
        now_tick, orderVars, deskVars, inflight_ask, evicted_live_violation
       >>

\* Strategy `Cancel` coalesces `cancel_after = TRUE` onto an existing entry with monotonic
\* earlier-only `next_due_mono_ns` update (`crates/tickwire-account/src/ors_query_desk.rs:184-193`,
\* `crates/tickwire-account/src/ors_query_desk.rs:231-241`, `crates/tickwire-account/src/order_manager.rs:1508-1536`).
CoalesceCancelOntoPendingQuery(oid, cand_due) ==
    /\ SlotLive(oid)
    /\ pending_cxl[oid] < 2
    /\ \E d \in DeskSlots :
        /\ desk_occupied[d]
        /\ desk_oid[d] = oid
        /\ (~desk_cancel_after[d] \/ cand_due < desk_due[d])
        /\ pending_cxl' = [pending_cxl EXCEPT ![oid] = @ + 1]
        /\ desk_cancel_after' = [desk_cancel_after EXCEPT ![d] = TRUE]
        /\ desk_due' = [desk_due EXCEPT ![d] = IF cand_due < desk_due[d] THEN cand_due ELSE desk_due[d]]
    /\ UNCHANGED <<
        now_tick, wallet_free, wallet_locked, order_submitted, desk_enqueues, order_status,
        filled_qty, canceled_qty, query_fail_count, terminal_reason,
        coalesced_cxl_dispatched, desk_occupied, desk_oid, desk_origin,
        desk_query_attempts, desk_cancel_attempts, desk_cancel_exhausted,
        inflight_ask, evicted_live_violation, refused_full_all_live
       >>

\* Ambiguous cancel failure schedules exponential backoff (`crates/tickwire-account/src/ors_query_desk.rs:246-310`,
\* `crates/tickwire-account/src/order_manager.rs:2204-2249`).
ScheduleCancelBackoff(oid) ==
    /\ order_status[oid] = "Open"
    /\ \E d \in DeskSlots :
        /\ (desk_occupied[d] /\ desk_oid[d] = oid /\ ~desk_cancel_exhausted[d])
           \/ (~HasDeskEntry(oid) /\ ~desk_occupied[d] /\ desk_enqueues[oid] < 2)
        /\ LET existing == desk_occupied[d] /\ desk_oid[d] = oid
               cur_att == IF existing THEN desk_cancel_attempts[d] ELSE 0
               exhaust == cur_att >= MaxCancelBackoff
               next_att == IF exhaust THEN cur_att ELSE cur_att + 1
               next_due == IF exhaust THEN desk_due[d] ELSE MinClock(now_tick + 1)
           IN
           /\ desk_occupied' = [desk_occupied EXCEPT ![d] = TRUE]
           /\ desk_oid' = [desk_oid EXCEPT ![d] = oid]
           /\ desk_origin' = [desk_origin EXCEPT ![d] = "Cancel"]
           /\ desk_cancel_after' = [desk_cancel_after EXCEPT ![d] = TRUE]
           /\ desk_query_attempts' = [desk_query_attempts EXCEPT ![d] = IF existing THEN @ ELSE 0]
           /\ desk_cancel_attempts' = [desk_cancel_attempts EXCEPT ![d] = next_att]
           /\ desk_cancel_exhausted' = [desk_cancel_exhausted EXCEPT ![d] = exhaust]
           /\ desk_due' = [desk_due EXCEPT ![d] = next_due]
           /\ desk_enqueues' = [desk_enqueues EXCEPT ![oid] = IF existing THEN @ ELSE @ + 1]
           /\ pending_cxl' = [pending_cxl EXCEPT ![oid] = IF @ = 0 THEN 1 ELSE @]
    /\ UNCHANGED <<
        now_tick, wallet_free, wallet_locked, order_submitted, order_status,
        filled_qty, canceled_qty, query_fail_count, terminal_reason,
        coalesced_cxl_dispatched, inflight_ask, evicted_live_violation,
        refused_full_all_live
       >>

\* `OrsQueryDesk::poll_due` lazily prunes retired non-`Live` entries (`crates/tickwire-account/src/ors_query_desk.rs:337-340`).
PollDuePruneNonLive(d) ==
    /\ desk_occupied[d]
    /\ ~SlotLive(desk_oid[d])
    /\ desk_occupied' = [desk_occupied EXCEPT ![d] = FALSE]
    /\ desk_oid' = [desk_oid EXCEPT ![d] = 0]
    /\ desk_origin' = [desk_origin EXCEPT ![d] = "Place"]
    /\ desk_cancel_after' = [desk_cancel_after EXCEPT ![d] = FALSE]
    /\ desk_query_attempts' = [desk_query_attempts EXCEPT ![d] = 0]
    /\ desk_cancel_attempts' = [desk_cancel_attempts EXCEPT ![d] = 0]
    /\ desk_cancel_exhausted' = [desk_cancel_exhausted EXCEPT ![d] = FALSE]
    /\ desk_due' = [desk_due EXCEPT ![d] = 0]
    /\ UNCHANGED <<
        now_tick, orderVars, inflight_ask, evicted_live_violation,
        refused_full_all_live
       >>

\* `OrsQueryDesk::poll_due` emits `DueQueryAction::StatusAsk` (`crates/tickwire-account/src/ors_query_desk.rs:360-371`).
PollDueStatusAsk(d) ==
    /\ desk_occupied[d]
    /\ LET oid == desk_oid[d] IN
       /\ SlotLive(oid)
       /\ ~desk_cancel_exhausted[d]
       /\ now_tick >= desk_due[d]
       /\ ~(desk_origin[d] = "Cancel" /\ desk_cancel_attempts[d] > 0)
       /\ desk_query_attempts[d] < 2
       /\ inflight_ask[oid] = "None"
       /\ desk_query_attempts' = [desk_query_attempts EXCEPT ![d] = @ + 1]
       /\ desk_due' = [desk_due EXCEPT ![d] = MinClock(now_tick + 1)]
       /\ inflight_ask' = [inflight_ask EXCEPT ![oid] = IF desk_origin[d] = "Cancel" THEN "CancelAsk" ELSE "PlaceAsk"]
       /\ UNCHANGED <<
           now_tick, orderVars, desk_occupied, desk_oid, desk_origin,
           desk_cancel_after, desk_cancel_attempts, desk_cancel_exhausted,
           evicted_live_violation, refused_full_all_live
          >>

\* `OrsQueryDesk::poll_due` emits `DueQueryAction::RetryCancel` (`crates/tickwire-account/src/ors_query_desk.rs:344-359`).
PollDueRetryCancel(d) ==
    /\ desk_occupied[d]
    /\ LET oid == desk_oid[d] IN
       /\ SlotLive(oid)
       /\ ~desk_cancel_exhausted[d]
       /\ now_tick >= desk_due[d]
       /\ desk_origin[d] = "Cancel"
       /\ desk_cancel_attempts[d] > 0
       /\ IF desk_cancel_attempts[d] >= MaxCancelBackoff
          THEN /\ desk_cancel_exhausted' = [desk_cancel_exhausted EXCEPT ![d] = TRUE]
               /\ UNCHANGED <<desk_cancel_attempts, desk_due>>
          ELSE /\ desk_cancel_attempts' = [desk_cancel_attempts EXCEPT ![d] = @ + 1]
               /\ desk_due' = [desk_due EXCEPT ![d] = MinClock(now_tick + 1)]
               /\ UNCHANGED desk_cancel_exhausted
       /\ UNCHANGED <<
           now_tick, orderVars, desk_occupied, desk_oid, desk_origin,
           desk_cancel_after, desk_query_attempts, inflight_ask,
           evicted_live_violation, refused_full_all_live
          >>

\* `-2013` / Bybit `110001` / OKX `51603` `NotFound` on uncreated `PendingNew` order (`filled == 0`):
\* closes as `Rejected` (`NACK`) (`crates/tickwire-account/src/order_manager.rs:2640-2648`,
\* `crates/tickwire-binance-account/src/session.rs:3010-3035`, `crates/tickwire-spot-execution/src/rest_bootstrap.rs:956-978`).
ReconcileNotFoundUncreatedNack(oid) ==
    /\ inflight_ask[oid] /= "None"
    /\ order_status[oid] = "PendingNew"
    /\ filled_qty[oid] = 0
    /\ wallet_locked' = wallet_locked - OrderQty
    /\ wallet_free' = wallet_free + OrderQty
    /\ order_status' = [order_status EXCEPT ![oid] = "Terminal"]
    /\ canceled_qty' = [canceled_qty EXCEPT ![oid] = OrderQty]
    /\ terminal_reason' = [terminal_reason EXCEPT ![oid] = "RejectedNack"]
    /\ inflight_ask' = [inflight_ask EXCEPT ![oid] = "None"]
    /\ ClearOrderFromDesk(oid)
    /\ UNCHANGED <<
        now_tick, order_submitted, desk_enqueues, filled_qty, pending_cxl, query_fail_count,
        coalesced_cxl_dispatched, evicted_live_violation, refused_full_all_live
       >>

\* `NotFound` on an already `Open` or partially filled order closes as `Canceled`, preserving fills
\* (`crates/tickwire-account/src/order_manager.rs:2624-2639`).
ReconcileNotFoundOpenOrFilledCancel(oid) ==
    /\ inflight_ask[oid] /= "None"
    /\ SlotLive(oid)
    /\ (order_status[oid] = "Open" \/ filled_qty[oid] > 0)
    /\ LET rem == OrderQty - filled_qty[oid] IN
       /\ wallet_locked' = wallet_locked - rem
       /\ wallet_free' = wallet_free + rem
       /\ order_status' = [order_status EXCEPT ![oid] = "Terminal"]
       /\ canceled_qty' = [canceled_qty EXCEPT ![oid] = rem]
       /\ terminal_reason' = [terminal_reason EXCEPT ![oid] = "Canceled"]
       /\ inflight_ask' = [inflight_ask EXCEPT ![oid] = "None"]
       /\ ClearOrderFromDesk(oid)
       /\ UNCHANGED <<
           now_tick, order_submitted, desk_enqueues, filled_qty, pending_cxl, query_fail_count,
           coalesced_cxl_dispatched, evicted_live_violation, refused_full_all_live
          >>

\* `OrderManager::reconcile_query_snapshot` applies reported `trades` / catch-up `cum_filled_lots`
\* via `on_fill` strictly BEFORE settling remainder `on_canceled` (`crates/tickwire-account/src/order_manager.rs:2531-2564`,
\* `crates/tickwire-account/src/order_manager.rs:2604-2622`, `crates/tickwire-spot-execution/src/rest_bootstrap.rs:1165-1212`).
ReconcileSnapshotCanceledWithCatchupFill(oid) ==
    /\ inflight_ask[oid] /= "None"
    /\ SlotLive(oid)
    /\ filled_qty[oid] = 0
    /\ LET catchup == 1
           rem_cxl == OrderQty - catchup
       IN
       /\ filled_qty' = [filled_qty EXCEPT ![oid] = catchup]
       /\ canceled_qty' = [canceled_qty EXCEPT ![oid] = rem_cxl]
       /\ wallet_locked' = wallet_locked - OrderQty
       /\ wallet_free' = wallet_free + rem_cxl
       /\ order_status' = [order_status EXCEPT ![oid] = "Terminal"]
       /\ terminal_reason' = [terminal_reason EXCEPT ![oid] = "Canceled"]
       /\ inflight_ask' = [inflight_ask EXCEPT ![oid] = "None"]
       /\ ClearOrderFromDesk(oid)
       /\ UNCHANGED <<
           now_tick, order_submitted, desk_enqueues, pending_cxl, query_fail_count,
           coalesced_cxl_dispatched, evicted_live_violation, refused_full_all_live
          >>

\* `PlaceAsk` answered `Open`: emits missing `Ack`, clears desk entry, and returns `DispatchCancel`
\* when `cancel_after` or `pending_cxl > 0` was coalesced (`crates/tickwire-account/src/order_manager.rs:2571-2598`,
\* `crates/tickwire-binance-account/src/session.rs:3198-3204`, `crates/tickwire-spot-execution/src/session.rs:2269-2285`).
ReconcilePlaceAskOpenAndCoalescedCancel(oid) ==
    /\ inflight_ask[oid] = "PlaceAsk"
    /\ SlotLive(oid)
    /\ LET want_cxl == pending_cxl[oid] > 0 \/ (\E d \in DeskSlots : desk_occupied[d] /\ desk_oid[d] = oid /\ desk_cancel_after[d]) IN
       /\ order_status' = [order_status EXCEPT ![oid] = "Open"]
       /\ coalesced_cxl_dispatched' = [coalesced_cxl_dispatched EXCEPT ![oid] = (@ \/ want_cxl)]
       /\ inflight_ask' = [inflight_ask EXCEPT ![oid] = "None"]
       /\ ClearOrderFromDesk(oid)
       /\ UNCHANGED <<
           now_tick, wallet_free, wallet_locked, order_submitted, desk_enqueues, filled_qty,
           canceled_qty, pending_cxl, query_fail_count, terminal_reason,
           evicted_live_violation, refused_full_all_live
          >>

\* `CancelAsk` answered `Open`: delayed cancel did NOT land on the book; decrements `pending_cxl` by 1
\* and keeps query scheduled only if additional cancels remain (`crates/tickwire-binance-account/src/session.rs:3107-3171`).
ReconcileCancelAskStillOpen(oid) ==
    /\ inflight_ask[oid] = "CancelAsk"
    /\ order_status[oid] = "Open"
    /\ pending_cxl[oid] > 0
    /\ LET rem_cxl == pending_cxl[oid] - 1 IN
       /\ pending_cxl' = [pending_cxl EXCEPT ![oid] = rem_cxl]
       /\ inflight_ask' = [inflight_ask EXCEPT ![oid] = "None"]
       /\ IF rem_cxl = 0
          THEN ClearOrderFromDesk(oid)
          ELSE /\ desk_due' = [d \in DeskSlots |-> IF desk_occupied[d] /\ desk_oid[d] = oid THEN MinClock(now_tick + DelayTicks) ELSE desk_due[d]]
               /\ UNCHANGED <<
                   desk_occupied, desk_oid, desk_origin, desk_cancel_after,
                   desk_query_attempts, desk_cancel_attempts, desk_cancel_exhausted
                  >>
       /\ UNCHANGED <<
           now_tick, wallet_free, wallet_locked, order_submitted, desk_enqueues, order_status,
           filled_qty, canceled_qty, query_fail_count, terminal_reason,
           coalesced_cxl_dispatched, evicted_live_violation, refused_full_all_live
          >>

\* Inconclusive `StatusAsk` (`HTTP 5xx`/`429`/timeout/`-1021`): keeps order `Live` when `max_query_count`
\* is unbounded (`0`) or below cap (`crates/tickwire-account/src/order_manager.rs:2488-2498`).
ReconcileInconclusiveRetry(oid) ==
    /\ inflight_ask[oid] /= "None"
    /\ SlotLive(oid)
    /\ query_fail_count[oid] < 2
    /\ (MaxQueryCount = 0 \/ query_fail_count[oid] + 1 < MaxQueryCount)
    /\ query_fail_count' = [query_fail_count EXCEPT ![oid] = @ + 1]
    /\ inflight_ask' = [inflight_ask EXCEPT ![oid] = "None"]
    /\ UNCHANGED <<
        now_tick, wallet_free, wallet_locked, order_submitted, desk_enqueues, order_status,
        filled_qty, canceled_qty, pending_cxl, terminal_reason,
        coalesced_cxl_dispatched, deskVars, evicted_live_violation,
        refused_full_all_live
       >>

\* Opt-in `max_query_count == Some(limit)` (`limit > 0`) retires wedged slot with `INTERNAL_CANCEL`
\* (`crates/tickwire-account/src/order_manager.rs:2499-2518`).
ReconcileInconclusiveCapRetire(oid) ==
    /\ inflight_ask[oid] /= "None"
    /\ SlotLive(oid)
    /\ MaxQueryCount > 0
    /\ query_fail_count[oid] < 2
    /\ query_fail_count[oid] + 1 >= MaxQueryCount
    /\ LET rem == OrderQty - filled_qty[oid] IN
       /\ query_fail_count' = [query_fail_count EXCEPT ![oid] = @ + 1]
       /\ wallet_locked' = wallet_locked - rem
       /\ wallet_free' = wallet_free + rem
       /\ order_status' = [order_status EXCEPT ![oid] = "Terminal"]
       /\ canceled_qty' = [canceled_qty EXCEPT ![oid] = rem]
       /\ terminal_reason' = [terminal_reason EXCEPT ![oid] = "InternalCancel"]
       /\ inflight_ask' = [inflight_ask EXCEPT ![oid] = "None"]
       /\ ClearOrderFromDesk(oid)
       /\ UNCHANGED <<
           now_tick, order_submitted, desk_enqueues, filled_qty, pending_cxl,
           coalesced_cxl_dispatched, evicted_live_violation, refused_full_all_live
          >>

AdvanceClock ==
    /\ now_tick < MaxClock
    /\ now_tick' = now_tick + 1
    /\ UNCHANGED <<
        orderVars, deskVars, inflight_ask, evicted_live_violation,
        refused_full_all_live
       >>

AllOrdersSettledIdle ==
    /\ \A oid \in Orders : order_status[oid] = "Terminal"
    /\ \A d \in DeskSlots : ~desk_occupied[d]
    /\ now_tick = MaxClock
    /\ UNCHANGED vars

Next ==
    \/ (\E oid \in Orders : SubmitOrder(oid))
    \/ (\E oid \in Orders : HotWsAckOrder(oid))
    \/ (\E oid \in Orders : HotWsFillAndRetire(oid))
    \/ (\E oid \in Orders, org \in {"Place", "Cancel", "SocketDrop"} : ScheduleStatusAskFreeSlot(oid, org))
    \/ (\E oid \in Orders : ScheduleOnFullEvictNonLive(oid))
    \/ (\E oid \in Orders : ScheduleRefuseFullAllLive(oid))
    \/ (\E oid \in Orders, cd \in 0..MaxClock : CoalesceCancelOntoPendingQuery(oid, cd))
    \/ (\E oid \in Orders : ScheduleCancelBackoff(oid))
    \/ (\E d \in DeskSlots : PollDuePruneNonLive(d))
    \/ (\E d \in DeskSlots : PollDueStatusAsk(d))
    \/ (\E d \in DeskSlots : PollDueRetryCancel(d))
    \/ (\E oid \in Orders : ReconcileNotFoundUncreatedNack(oid))
    \/ (\E oid \in Orders : ReconcileNotFoundOpenOrFilledCancel(oid))
    \/ (\E oid \in Orders : ReconcileSnapshotCanceledWithCatchupFill(oid))
    \/ (\E oid \in Orders : ReconcilePlaceAskOpenAndCoalescedCancel(oid))
    \/ (\E oid \in Orders : ReconcileCancelAskStillOpen(oid))
    \/ (\E oid \in Orders : ReconcileInconclusiveRetry(oid))
    \/ (\E oid \in Orders : ReconcileInconclusiveCapRetire(oid))
    \/ AdvanceClock
    \/ AllOrdersSettledIdle

\* ---- Safety Invariants ----

TypeOK ==
    /\ now_tick \in 0..MaxClock
    /\ wallet_free \in 0..InitialBudget
    /\ wallet_locked \in 0..InitialBudget
    /\ order_submitted \in [Orders -> BOOLEAN]
    /\ desk_enqueues \in [Orders -> 0..2]
    /\ order_status \in [Orders -> {"Free", "PendingNew", "Open", "Terminal"}]
    /\ filled_qty \in [Orders -> 0..OrderQty]
    /\ canceled_qty \in [Orders -> 0..OrderQty]
    /\ pending_cxl \in [Orders -> 0..2]
    /\ query_fail_count \in [Orders -> 0..2]
    /\ terminal_reason \in [Orders -> {"None", "Filled", "Canceled", "RejectedNack", "InternalCancel"}]
    /\ coalesced_cxl_dispatched \in [Orders -> BOOLEAN]
    /\ desk_occupied \in [DeskSlots -> BOOLEAN]
    /\ desk_oid \in [DeskSlots -> 0..2]
    /\ desk_origin \in [DeskSlots -> {"Place", "Cancel", "SocketDrop"}]
    /\ desk_cancel_after \in [DeskSlots -> BOOLEAN]
    /\ desk_query_attempts \in [DeskSlots -> 0..2]
    /\ desk_cancel_attempts \in [DeskSlots -> 0..MaxCancelBackoff]
    /\ desk_cancel_exhausted \in [DeskSlots -> BOOLEAN]
    /\ desk_due \in [DeskSlots -> 0..MaxClock]
    /\ inflight_ask \in [Orders -> {"None", "PlaceAsk", "CancelAsk"}]
    /\ evicted_live_violation \in BOOLEAN
    /\ refused_full_all_live \in BOOLEAN

\* Full-table cleanup never evicts a `SlotState::Live` order, and at most one desk entry exists per order.
ActiveLiveProtectedOnEviction ==
    /\ ~evicted_live_violation
    /\ \A d1, d2 \in DeskSlots :
        (desk_occupied[d1] /\ desk_occupied[d2] /\ desk_oid[d1] = desk_oid[d2]) => (d1 = d2)

\* Cancel backoff never exceeds `MaxCancelBackoff` and sets `cancel_exhausted` only at the cap.
CancelBackoffBounded ==
    \A d \in DeskSlots :
        /\ desk_cancel_attempts[d] <= MaxCancelBackoff
        /\ (desk_cancel_exhausted[d] => desk_cancel_attempts[d] = MaxCancelBackoff)
        /\ ((desk_occupied[d] /\ desk_origin[d] = "Cancel") => desk_cancel_after[d])

\* Snapshot catch-up fills are credited strictly before remainder cancel so `filled + canceled == OrderQty` on terminal orders.
SnapshotFillsBeforeRemainderCancel ==
    \A oid \in Orders :
        (order_status[oid] = "Terminal") =>
            /\ filled_qty[oid] + canceled_qty[oid] = OrderQty
            /\ (terminal_reason[oid] \in {"Canceled", "RejectedNack", "InternalCancel"} => ~HasDeskEntry(oid))

\* Uncreated `-2013` `RejectedNack` occurs only with zero executed fills.
UncreatedNotFoundVsOpenNotFoundSound ==
    \A oid \in Orders :
        (terminal_reason[oid] = "RejectedNack") =>
            (filled_qty[oid] = 0 /\ canceled_qty[oid] = OrderQty)

\* Default `MaxQueryCount = 0` (`None` / `Some(0)`) never emits `InternalCancel`; opt-in cap emits only at `>= MaxQueryCount`.
MaxQueryCountContract ==
    \A oid \in Orders :
        /\ (MaxQueryCount = 0 => terminal_reason[oid] /= "InternalCancel")
        /\ (terminal_reason[oid] = "InternalCancel" =>
                (MaxQueryCount > 0 /\ query_fail_count[oid] >= MaxQueryCount))

\* Exact multi-order hold and wallet conservation even when one sibling is wedged in `OrsQueryDesk`.
SolvencyPreserved ==
    /\ wallet_locked = ExpectedLocked
    /\ wallet_free + wallet_locked + SpentOnFills = InitialBudget
====
