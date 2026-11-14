------------------------ MODULE L1_QueryConvergence ------------------------
EXTENDS Integers, FiniteSets

CONSTANTS
    Orders,
    VenueModes,
    MaxSlots,
    VenueIsBybit,
    StatusDelay,
    GraceTicks,
    MaxCancelAsks,
    MaxQueryCount,
    MaxClock,
    MaxDispatches

(*
Code-faithful model of Stage 2 per-order `QueryDesk` coupled with `PerOrderQueryDriver`
(Binance REST/WS + Bybit/OKX Spot), `AsyncRestQuerier` SPSC slots, and concurrent private
WebSocket `orders` and `fills` pushes.

Variable <-> Rust source mapping:
- clock, dispatches, frozen, hazard_seen, max_queries_hit:
  monotonic timestamp (`now_mono_ns`), bounded wire dispatch horizon, `Account.frozen`,
  Category B hazard latch (`429`/`418`/`401`), and `max_query_count` freeze latch,
  crates/tickwire-account/src/query_desk.rs:534-547,
  crates/tickwire-binance-account/src/query_desk_driver.rs:580-635,
  crates/tickwire-spot-runner/src/query_desk_driver.rs:349-390.
- admitted, venue_mode, confirmed, terminal, uncreated, fees_closed, in_doubt,
  local_filled, term_cum, trade_applied, expected_fill, wire_logical_sent,
  bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up:
  `Account.orders` (`Order`), `Account.seen_fills` (`trade_id` deduplication), and
  `PerOrderQueryDriver` / `AccountRunner` `expected_fill_atoms`,
  crates/tickwire-account/src/events.rs:229-358,
  crates/tickwire-account/src/events.rs:475-682,
  crates/tickwire-binance-account/src/rest.rs:518-575,
  crates/tickwire-spot-runner/src/lib.rs:535-565,
  crates/tickwire-spot-runner/src/reconcile/ingest.rs:202-320.
- ask_present, ask_kind, place_unacked, pending_cancels, cancel_calls, cancel_after,
  trades_after, known_terminal, issued_count, first_doubt, due_at, ask_slot, ask_gen,
  desk_slot_order, desk_slot_gen, desk_slot_kind, next_gen:
  `QueryDesk` (`StatusAsk` and `SlotState`),
  crates/tickwire-account/src/query_desk.rs:111-921.
- wire_active, wire_order, wire_gen, wire_step, local_not_sent_seen, async_not_sent_seen:
  `PerOrderQueryDriver.in_flight` + `AsyncRestQuerier` SPSC slot ownership and Bybit
  two-step `OrderStatusPrimary` (`/v5/order/realtime`) -> `BybitOrderStatusHistory`
  (`/v5/order/history`) chain,
  crates/tickwire-binance-account/src/query_desk_driver.rs:335-1053,
  crates/tickwire-spot-runner/src/query_desk_driver.rs:295-1074.
*)

VARIABLES
    clock,
    dispatches,
    frozen,
    hazard_seen,
    max_queries_hit,
    local_not_sent_seen,
    async_not_sent_seen,
    next_gen,
    admitted,
    venue_mode,
    confirmed,
    terminal,
    uncreated,
    fees_closed,
    in_doubt,
    local_filled,
    term_cum,
    trade_applied,
    expected_fill,
    wire_logical_sent,
    bybit_rt_empty_seen,
    bybit_hist_empty_seen,
    cancel_requested,
    cancel_resolved_or_given_up,
    ask_present,
    ask_kind,
    place_unacked,
    pending_cancels,
    cancel_calls,
    cancel_after,
    trades_after,
    known_terminal,
    issued_count,
    first_doubt,
    due_at,
    ask_slot,
    ask_gen,
    desk_slot_order,
    desk_slot_gen,
    desk_slot_kind,
    wire_active,
    wire_order,
    wire_gen,
    wire_step

vars == <<
    clock, dispatches, frozen, hazard_seen, max_queries_hit,
    local_not_sent_seen, async_not_sent_seen, next_gen,
    admitted, venue_mode, confirmed, terminal, uncreated, fees_closed, in_doubt,
    local_filled, term_cum, trade_applied, expected_fill, wire_logical_sent,
    bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
    ask_present, ask_kind, place_unacked, pending_cancels, cancel_calls, cancel_after,
    trades_after, known_terminal, issued_count, first_doubt, due_at, ask_slot, ask_gen,
    desk_slot_order, desk_slot_gen, desk_slot_kind,
    wire_active, wire_order, wire_gen, wire_step
>>

Slots == 1..MaxSlots
AllVenueModes == {"None", "Absent", "OpenFlat", "OpenPartFill", "CanceledWithFill"}
DoubtKinds == {"None", "PlaceAmbiguous", "CancelAmbiguous", "FillCatchup"}
AskKinds == {"None", "Place", "Cancel", "Trades"}
WireSteps == {"None", "OrderStatusPrimary", "BybitOrderStatusHistory", "OrderTrades"}

CapDue(t) == IF t > MaxClock + 1 THEN MaxClock + 1 ELSE t
Max2(a, b) == IF a >= b THEN a ELSE b

VenueCum(vm) ==
    IF vm \in {"OpenPartFill", "CanceledWithFill"} THEN 1 ELSE 0

VenueTerminal(vm) ==
    vm = "CanceledWithFill"

\* Exact `should_query_order` / `poll_due_query_ticket` predicate from
\* crates/tickwire-binance-account/src/query_desk_driver.rs:1040-1053 and
\* crates/tickwire-spot-runner/src/lib.rs:541-565.
ShouldQuery(o, k) ==
    /\ admitted[o]
    /\ ~uncreated[o]
    /\ IF k = "Trades"
       THEN in_doubt[o] = "FillCatchup"
            \/ expected_fill[o] > local_filled[o]
            \/ (terminal[o] /\ ~fees_closed[o])
       ELSE in_doubt[o] # "None" \/ (~terminal[o] /\ k = "Cancel")

\* Exact `QueryDesk::is_ticket_in_flight` / `claim_ticket` match check from
\* crates/tickwire-account/src/query_desk.rs:220-233, 430-450.
TicketMatchesDesk(s) ==
    LET o == wire_order[s] IN
    /\ o \in Orders
    /\ desk_slot_order[s] = o
    /\ desk_slot_gen[s] = wire_gen[s]
    /\ ask_present[o]
    /\ ask_slot[o] = s
    /\ ask_gen[o] = wire_gen[s]

Init ==
    /\ clock = 0
    /\ dispatches = 0
    /\ frozen = FALSE
    /\ hazard_seen = FALSE
    /\ max_queries_hit = FALSE
    /\ local_not_sent_seen = FALSE
    /\ async_not_sent_seen = FALSE
    /\ next_gen = 1
    /\ admitted = [o \in Orders |-> FALSE]
    /\ venue_mode = [o \in Orders |-> "None"]
    /\ confirmed = [o \in Orders |-> FALSE]
    /\ terminal = [o \in Orders |-> FALSE]
    /\ uncreated = [o \in Orders |-> FALSE]
    /\ fees_closed = [o \in Orders |-> FALSE]
    /\ in_doubt = [o \in Orders |-> "None"]
    /\ local_filled = [o \in Orders |-> 0]
    /\ term_cum = [o \in Orders |-> -1]
    /\ trade_applied = [o \in Orders |-> FALSE]
    /\ expected_fill = [o \in Orders |-> 0]
    /\ wire_logical_sent = [o \in Orders |-> 0]
    /\ bybit_rt_empty_seen = [o \in Orders |-> FALSE]
    /\ bybit_hist_empty_seen = [o \in Orders |-> FALSE]
    /\ cancel_requested = [o \in Orders |-> FALSE]
    /\ cancel_resolved_or_given_up = [o \in Orders |-> FALSE]
    /\ ask_present = [o \in Orders |-> FALSE]
    /\ ask_kind = [o \in Orders |-> "None"]
    /\ place_unacked = [o \in Orders |-> FALSE]
    /\ pending_cancels = [o \in Orders |-> 0]
    /\ cancel_calls = [o \in Orders |-> 0]
    /\ cancel_after = [o \in Orders |-> FALSE]
    /\ trades_after = [o \in Orders |-> FALSE]
    /\ known_terminal = [o \in Orders |-> FALSE]
    /\ issued_count = [o \in Orders |-> 0]
    /\ first_doubt = [o \in Orders |-> -1]
    /\ due_at = [o \in Orders |-> 0]
    /\ ask_slot = [o \in Orders |-> 0]
    /\ ask_gen = [o \in Orders |-> 0]
    /\ desk_slot_order = [s \in Slots |-> 0]
    /\ desk_slot_gen = [s \in Slots |-> 0]
    /\ desk_slot_kind = [s \in Slots |-> "None"]
    /\ wire_active = [s \in Slots |-> FALSE]
    /\ wire_order = [s \in Slots |-> 0]
    /\ wire_gen = [s \in Slots |-> 0]
    /\ wire_step = [s \in Slots |-> "None"]

(*
1. `isolate_intent_in_doubt(..., PlaceAmbiguous)` + `QueryDesk::enqueue_ask(AskKind::Place, true)`
(crates/tickwire-account-gateway/src/lib.rs:1647-1717,
crates/tickwire-account/src/query_desk.rs:342-367):
*)
IsolateAmbiguousPlace(o, vm) ==
    /\ ~admitted[o]
    /\ ~frozen
    /\ vm \in VenueModes
    /\ admitted' = [admitted EXCEPT ![o] = TRUE]
    /\ venue_mode' = [venue_mode EXCEPT ![o] = vm]
    /\ in_doubt' = [in_doubt EXCEPT ![o] = "PlaceAmbiguous"]
    /\ ask_present' = [ask_present EXCEPT ![o] = TRUE]
    /\ ask_kind' = [ask_kind EXCEPT ![o] = "Place"]
    /\ place_unacked' = [place_unacked EXCEPT ![o] = TRUE]
    /\ pending_cancels' = [pending_cancels EXCEPT ![o] = 0]
    /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
    /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
    /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
    /\ known_terminal' = [known_terminal EXCEPT ![o] = FALSE]
    /\ issued_count' = [issued_count EXCEPT ![o] = 0]
    /\ first_doubt' = [first_doubt EXCEPT ![o] = clock]
    /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + StatusDelay)]
    /\ ask_slot' = [ask_slot EXCEPT ![o] = 0]
    /\ ask_gen' = [ask_gen EXCEPT ![o] = 0]
    /\ UNCHANGED <<
        clock, dispatches, frozen, hazard_seen, max_queries_hit,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        confirmed, terminal, uncreated, fees_closed,
        local_filled, term_cum, trade_applied, expected_fill, wire_logical_sent,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
        desk_slot_order, desk_slot_gen, desk_slot_kind,
        wire_active, wire_order, wire_gen, wire_step
       >>

(*
2. Confirmed order admission (`Place` acknowledged cleanly on execution channel):
Allows exercising `CancelAmbiguous` with `place_unacked = FALSE` and lagging WS `FillCatchup`
on orders that never experienced `PlaceAmbiguous`.
*)
AdmitConfirmedOrder(o, vm) ==
    /\ ~admitted[o]
    /\ ~frozen
    /\ vm \in (VenueModes \ {"Absent"})
    /\ admitted' = [admitted EXCEPT ![o] = TRUE]
    /\ venue_mode' = [venue_mode EXCEPT ![o] = vm]
    /\ confirmed' = [confirmed EXCEPT ![o] = TRUE]
    /\ UNCHANGED <<
        clock, dispatches, frozen, hazard_seen, max_queries_hit,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        terminal, uncreated, fees_closed, in_doubt,
        local_filled, term_cum, trade_applied, expected_fill, wire_logical_sent,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
        ask_present, ask_kind, place_unacked, pending_cancels, cancel_calls, cancel_after,
        trades_after, known_terminal, issued_count, first_doubt, due_at, ask_slot, ask_gen,
        desk_slot_order, desk_slot_gen, desk_slot_kind,
        wire_active, wire_order, wire_gen, wire_step
       >>

(*
3. Ambiguous outbound `Cancel` (`isolate_intent_in_doubt(..., CancelAmbiguous)` +
`QueryDesk::enqueue_ask(AskKind::Cancel)`, crates/tickwire-account/src/query_desk.rs:282-367):
Coalesces with waiting `Place`, waiting `Trades` (`cancel_after = TRUE`), or ANY in-flight
ask (`ask_slot[o] > 0` -> `cancel_after = TRUE`).
*)
EnqueueAmbiguousCancel(o) ==
    /\ admitted[o]
    /\ ~frozen
    /\ ~terminal[o]
    /\ ~uncreated[o]
    /\ ~cancel_requested[o]
    /\ cancel_requested' = [cancel_requested EXCEPT ![o] = TRUE]
    /\ in_doubt' = [in_doubt EXCEPT ![o] = "CancelAmbiguous"]
    /\ IF ~ask_present[o]
       THEN /\ ask_present' = [ask_present EXCEPT ![o] = TRUE]
            /\ ask_kind' = [ask_kind EXCEPT ![o] = "Cancel"]
            /\ place_unacked' = [place_unacked EXCEPT ![o] = ~confirmed[o]]
            /\ pending_cancels' = [pending_cancels EXCEPT ![o] = 1]
            /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
            /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
            /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
            /\ known_terminal' = [known_terminal EXCEPT ![o] = FALSE]
            /\ issued_count' = [issued_count EXCEPT ![o] = 0]
            /\ first_doubt' = [first_doubt EXCEPT ![o] = clock]
            /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + StatusDelay)]
            /\ ask_slot' = [ask_slot EXCEPT ![o] = 0]
            /\ ask_gen' = [ask_gen EXCEPT ![o] = 0]
       ELSE /\ place_unacked' = [place_unacked EXCEPT ![o] = (place_unacked[o] \/ ~confirmed[o])]
            /\ IF ask_slot[o] = 0
               THEN IF ~known_terminal[o]
                    THEN /\ pending_cancels' = [pending_cancels EXCEPT ![o] = 1]
                         /\ IF ask_kind[o] = "Trades"
                            THEN /\ cancel_after' = [cancel_after EXCEPT ![o] = TRUE]
                                 /\ UNCHANGED <<ask_kind, cancel_calls, due_at>>
                            ELSE /\ ask_kind' = [ask_kind EXCEPT ![o] = "Cancel"]
                                 /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
                                 /\ due_at' = [due_at EXCEPT ![o] =
                                        CapDue(Max2(due_at[o], clock + StatusDelay))]
                                 /\ UNCHANGED cancel_after
                    ELSE UNCHANGED <<ask_kind, pending_cancels, cancel_calls, cancel_after, due_at>>
               ELSE IF ~known_terminal[o]
                    THEN /\ pending_cancels' = [pending_cancels EXCEPT ![o] = 1]
                         /\ cancel_after' = [cancel_after EXCEPT ![o] = TRUE]
                         /\ UNCHANGED <<ask_kind, cancel_calls, due_at>>
                    ELSE UNCHANGED <<ask_kind, pending_cancels, cancel_calls, cancel_after, due_at>>
            /\ UNCHANGED <<ask_present, trades_after, known_terminal, issued_count, first_doubt, ask_slot, ask_gen>>
    /\ UNCHANGED <<
        clock, dispatches, frozen, hazard_seen, max_queries_hit,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        admitted, venue_mode, confirmed, terminal, uncreated, fees_closed,
        local_filled, term_cum, trade_applied, expected_fill, wire_logical_sent,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_resolved_or_given_up,
        desk_slot_order, desk_slot_gen, desk_slot_kind,
        wire_active, wire_order, wire_gen, wire_step
       >>

(*
4. Lagging private WS `orders` push (`cum = 1 > local_filled = 0` before trade fill arrives,
crates/tickwire-binance-account/src/rest.rs:518-575,
crates/tickwire-spot-runner/src/reconcile/ingest.rs:236-269,
crates/tickwire-account/src/query_desk.rs:305-330):
Isolates `FillCatchup` (preserving `CancelAmbiguous` on non-terminal orders) and enqueues
`AskKind::Trades` (setting `trades_after = TRUE` if a request is already in flight!).
*)
WsLaggingOrderPush(o) ==
    /\ admitted[o]
    /\ ~frozen
    /\ ~uncreated[o]
    /\ venue_mode[o] \in {"OpenPartFill", "CanceledWithFill"}
    /\ local_filled[o] = 0
    /\ expected_fill[o] = 0
    /\ LET is_term == VenueTerminal(venue_mode[o]) IN
       /\ confirmed' = [confirmed EXCEPT ![o] = TRUE]
       /\ terminal' = [terminal EXCEPT ![o] = (terminal[o] \/ is_term)]
       /\ term_cum' = [term_cum EXCEPT ![o] = IF is_term THEN 1 ELSE term_cum[o]]
       /\ expected_fill' = [expected_fill EXCEPT ![o] = 1]
       /\ in_doubt' = [in_doubt EXCEPT ![o] =
            IF ~is_term /\ in_doubt[o] = "CancelAmbiguous"
            THEN "CancelAmbiguous"
            ELSE "FillCatchup"]
       /\ IF ~ask_present[o]
          THEN /\ ask_present' = [ask_present EXCEPT ![o] = TRUE]
               /\ ask_kind' = [ask_kind EXCEPT ![o] = "Trades"]
               /\ place_unacked' = [place_unacked EXCEPT ![o] = FALSE]
               /\ pending_cancels' = [pending_cancels EXCEPT ![o] = 0]
               /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
               /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
               /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
               /\ known_terminal' = [known_terminal EXCEPT ![o] = is_term]
               /\ issued_count' = [issued_count EXCEPT ![o] = 0]
               /\ first_doubt' = [first_doubt EXCEPT ![o] = clock]
               /\ due_at' = [due_at EXCEPT ![o] = clock]
               /\ ask_slot' = [ask_slot EXCEPT ![o] = 0]
               /\ ask_gen' = [ask_gen EXCEPT ![o] = 0]
          ELSE /\ place_unacked' = [place_unacked EXCEPT ![o] = FALSE]
               /\ known_terminal' = [known_terminal EXCEPT ![o] = (known_terminal[o] \/ is_term)]
               /\ IF ask_slot[o] = 0
                  THEN /\ cancel_after' = [cancel_after EXCEPT ![o] =
                            IF is_term \/ known_terminal[o]
                            THEN FALSE
                            ELSE (cancel_after[o] \/ ask_kind[o] = "Cancel")]
                       /\ pending_cancels' = [pending_cancels EXCEPT ![o] =
                            IF is_term \/ known_terminal[o]
                            THEN 0
                            ELSE IF ask_kind[o] = "Cancel" /\ pending_cancels[o] = 0
                                 THEN 1
                                 ELSE pending_cancels[o]]
                       /\ ask_kind' = [ask_kind EXCEPT ![o] = "Trades"]
                       /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
                       /\ due_at' = [due_at EXCEPT ![o] = clock]
                  ELSE /\ trades_after' = [trades_after EXCEPT ![o] = TRUE]
                       /\ cancel_after' = [cancel_after EXCEPT ![o] =
                            IF is_term THEN FALSE ELSE cancel_after[o]]
                       /\ pending_cancels' = [pending_cancels EXCEPT ![o] =
                            IF is_term THEN 0 ELSE pending_cancels[o]]
                       /\ UNCHANGED <<ask_kind, due_at>>
               /\ UNCHANGED <<ask_present, cancel_calls, issued_count, first_doubt, ask_slot, ask_gen>>
    /\ UNCHANGED <<
        clock, dispatches, frozen, hazard_seen, max_queries_hit,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        admitted, venue_mode, uncreated, fees_closed,
        local_filled, trade_applied, wire_logical_sent,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
        desk_slot_order, desk_slot_gen, desk_slot_kind,
        wire_active, wire_order, wire_gen, wire_step
       >>

(*
5. Concurrent private WS terminal flat push (`CANCELED, cum = 0`) on `"OpenFlat"`
(crates/tickwire-spot-runner/src/reconcile/ingest.rs:269-277,
crates/tickwire-account/src/query_desk.rs:234-244 `remove_order`):
Closes the order and frees its `QueryDesk` slot even while a wire task on `wire_active[s]`
(such as mid-chain Bybit `realtime`) is still in flight!
*)
WsFlatTerminalPush(o) ==
    /\ admitted[o]
    /\ ~frozen
    /\ ~uncreated[o]
    /\ ~terminal[o]
    /\ venue_mode[o] = "OpenFlat"
    /\ ask_present[o]
    /\ LET s == ask_slot[o] IN
       /\ confirmed' = [confirmed EXCEPT ![o] = TRUE]
       /\ terminal' = [terminal EXCEPT ![o] = TRUE]
       /\ term_cum' = [term_cum EXCEPT ![o] = 0]
       /\ fees_closed' = [fees_closed EXCEPT ![o] = TRUE]
       /\ in_doubt' = [in_doubt EXCEPT ![o] = "None"]
       /\ ask_present' = [ask_present EXCEPT ![o] = FALSE]
       /\ ask_kind' = [ask_kind EXCEPT ![o] = "None"]
       /\ ask_slot' = [ask_slot EXCEPT ![o] = 0]
       /\ ask_gen' = [ask_gen EXCEPT ![o] = 0]
       /\ desk_slot_order' = [s2 \in Slots |->
            IF s2 = s /\ desk_slot_gen[s2] = ask_gen[o] THEN 0 ELSE desk_slot_order[s2]]
       /\ desk_slot_gen' = [s2 \in Slots |->
            IF s2 = s /\ desk_slot_gen[s2] = ask_gen[o] THEN 0 ELSE desk_slot_gen[s2]]
       /\ desk_slot_kind' = [s2 \in Slots |->
            IF s2 = s /\ desk_slot_gen[s2] = ask_gen[o] THEN "None" ELSE desk_slot_kind[s2]]
    /\ UNCHANGED <<
        clock, dispatches, frozen, hazard_seen, max_queries_hit,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        admitted, venue_mode, uncreated,
        local_filled, trade_applied, expected_fill, wire_logical_sent,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
        place_unacked, pending_cancels, cancel_calls, cancel_after,
        trades_after, known_terminal, issued_count, first_doubt, due_at,
        wire_active, wire_order, wire_gen, wire_step
       >>

(*
6. Concurrent private WS trade fill (`trade_id` deduplicated with REST `OrderTrades`,
crates/tickwire-account/src/events.rs:229-358,
crates/tickwire-spot-runner/src/reconcile/ingest.rs:284-305):
*)
WsTradeFillPush(o) ==
    /\ admitted[o]
    /\ ~frozen
    /\ ~uncreated[o]
    /\ venue_mode[o] \in {"OpenPartFill", "CanceledWithFill"}
    /\ ~trade_applied[o]
    /\ LET closes_now == terminal[o] /\ term_cum[o] = 1
           s == ask_slot[o]
       IN
       /\ confirmed' = [confirmed EXCEPT ![o] = TRUE]
       /\ trade_applied' = [trade_applied EXCEPT ![o] = TRUE]
       /\ local_filled' = [local_filled EXCEPT ![o] = 1]
       /\ expected_fill' = [expected_fill EXCEPT ![o] = 0]
       /\ fees_closed' = [fees_closed EXCEPT ![o] = (fees_closed[o] \/ closes_now)]
       /\ in_doubt' = [in_doubt EXCEPT ![o] =
            IF closes_now
            THEN "None"
            ELSE IF in_doubt[o] \in {"FillCatchup", "PlaceAmbiguous"}
                 THEN IF ask_present[o] /\ ~known_terminal[o]
                         /\ (ask_kind[o] = "Cancel" \/ cancel_after[o] \/ pending_cancels[o] > 0)
                      THEN "CancelAmbiguous"
                      ELSE "None"
                 ELSE in_doubt[o]]
       /\ IF closes_now /\ ask_present[o]
          THEN /\ ask_present' = [ask_present EXCEPT ![o] = FALSE]
               /\ ask_kind' = [ask_kind EXCEPT ![o] = "None"]
               /\ ask_slot' = [ask_slot EXCEPT ![o] = 0]
               /\ ask_gen' = [ask_gen EXCEPT ![o] = 0]
               /\ desk_slot_order' = [s2 \in Slots |->
                    IF s2 = s /\ desk_slot_gen[s2] = ask_gen[o] THEN 0 ELSE desk_slot_order[s2]]
               /\ desk_slot_gen' = [s2 \in Slots |->
                    IF s2 = s /\ desk_slot_gen[s2] = ask_gen[o] THEN 0 ELSE desk_slot_gen[s2]]
               /\ desk_slot_kind' = [s2 \in Slots |->
                    IF s2 = s /\ desk_slot_gen[s2] = ask_gen[o] THEN "None" ELSE desk_slot_kind[s2]]
          ELSE UNCHANGED <<ask_present, ask_kind, ask_slot, ask_gen, desk_slot_order, desk_slot_gen, desk_slot_kind>>
    /\ UNCHANGED <<
        clock, dispatches, frozen, hazard_seen, max_queries_hit,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        admitted, venue_mode, terminal, uncreated, term_cum, wire_logical_sent,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
        place_unacked, pending_cancels, cancel_calls, cancel_after,
        trades_after, known_terminal, issued_count, first_doubt, due_at,
        wire_active, wire_order, wire_gen, wire_step
       >>

(*
7. `QueryDesk::poll_due` skipping an already caught-up ask (`!should_query`,
crates/tickwire-account/src/query_desk.rs:382-403):
If `ask_kind[o] = "Trades"` and a cancel was queued behind it (`cancel_after` or
`pending_cancels > 0`) on a non-terminal order, re-arms `AskKind::Cancel`; otherwise removes
the ask without consuming any REST slot!
*)
PollSkipResolvedAsk(o) ==
    /\ ~frozen
    /\ ask_present[o]
    /\ ask_slot[o] = 0
    /\ clock >= due_at[o]
    /\ ~ShouldQuery(o, ask_kind[o])
    /\ IF ask_kind[o] = "Trades" /\ ~known_terminal[o]
          /\ (cancel_after[o] \/ pending_cancels[o] > 0)
       THEN /\ ask_kind' = [ask_kind EXCEPT ![o] = "Cancel"]
            /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
            /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
            /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
            /\ pending_cancels' = [pending_cancels EXCEPT ![o] = 1]
            /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + StatusDelay)]
            /\ UNCHANGED ask_present
       ELSE /\ ask_present' = [ask_present EXCEPT ![o] = FALSE]
            /\ ask_kind' = [ask_kind EXCEPT ![o] = "None"]
            /\ UNCHANGED <<trades_after, cancel_calls, cancel_after, pending_cancels, due_at>>
    /\ UNCHANGED <<
        clock, dispatches, frozen, hazard_seen, max_queries_hit,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        admitted, venue_mode, confirmed, terminal, uncreated, fees_closed, in_doubt,
        local_filled, term_cum, trade_applied, expected_fill, wire_logical_sent,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
        place_unacked, known_terminal, issued_count, first_doubt, ask_slot, ask_gen,
        desk_slot_order, desk_slot_gen, desk_slot_kind,
        wire_active, wire_order, wire_gen, wire_step
       >>

(*
8. `PerOrderQueryDriver::dispatch_due` + `QueryDesk::poll_due`
(crates/tickwire-account/src/query_desk.rs:404-428,
crates/tickwire-spot-runner/src/query_desk_driver.rs:975-1060,
crates/tickwire-binance-account/src/query_desk_driver.rs:335-445):
Claims free `QueryDesk` slot `s` AND verifies `~wire_active[s]` (`self.in_flight[s].is_none()`)
so a mid-flight `remove_order` never causes wire-slot aliasing!
*)
PollAndDispatchDue(o, s) ==
    /\ ~frozen
    /\ dispatches < MaxDispatches
    /\ ask_present[o]
    /\ ask_slot[o] = 0
    /\ clock >= due_at[o]
    /\ ShouldQuery(o, ask_kind[o])
    /\ desk_slot_order[s] = 0
    /\ ~wire_active[s]
    /\ LET g == next_gen
           step == IF ask_kind[o] = "Trades"
                   THEN "OrderTrades"
                   ELSE "OrderStatusPrimary"
       IN
       /\ next_gen' = next_gen + 1
       /\ dispatches' = dispatches + 1
       /\ ask_slot' = [ask_slot EXCEPT ![o] = s]
       /\ ask_gen' = [ask_gen EXCEPT ![o] = g]
       /\ desk_slot_order' = [desk_slot_order EXCEPT ![s] = o]
       /\ desk_slot_gen' = [desk_slot_gen EXCEPT ![s] = g]
       /\ desk_slot_kind' = [desk_slot_kind EXCEPT ![s] = ask_kind[o]]
       /\ wire_active' = [wire_active EXCEPT ![s] = TRUE]
       /\ wire_order' = [wire_order EXCEPT ![s] = o]
       /\ wire_gen' = [wire_gen EXCEPT ![s] = g]
       /\ wire_step' = [wire_step EXCEPT ![s] = step]
    /\ UNCHANGED <<
        clock, frozen, hazard_seen, max_queries_hit,
        local_not_sent_seen, async_not_sent_seen,
        admitted, venue_mode, confirmed, terminal, uncreated, fees_closed, in_doubt,
        local_filled, term_cum, trade_applied, expected_fill, wire_logical_sent,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
        ask_present, ask_kind, place_unacked, pending_cancels, cancel_calls, cancel_after,
        trades_after, known_terminal, issued_count, first_doubt, due_at
       >>

(*
9. Pre-wire local refusal (SPSC ring full / quota denied / stale driver slot busy) ->
`QueryDesk::on_not_sent` (crates/tickwire-account/src/query_desk.rs:452-519,
crates/tickwire-spot-runner/src/query_desk_driver.rs:992-1072):
Frees the `QueryDesk` slot immediately WITHOUT incrementing `wire_logical_sent`,
`issued_count`, or `cancel_calls`, and WITHOUT freezing!
*)
DispatchNotSentLocal(o, s) ==
    /\ ~frozen
    /\ ~local_not_sent_seen
    /\ ask_present[o]
    /\ ask_slot[o] = 0
    /\ clock >= due_at[o]
    /\ ShouldQuery(o, ask_kind[o])
    /\ desk_slot_order[s] = 0
    /\ local_not_sent_seen' = TRUE
    /\ IF trades_after[o] /\ ask_kind[o] # "Trades"
       THEN /\ cancel_after' = [cancel_after EXCEPT ![o] =
                IF ~known_terminal[o]
                   /\ (ask_kind[o] = "Cancel" \/ cancel_after[o] \/ pending_cancels[o] > 0)
                THEN TRUE ELSE cancel_after[o]]
            /\ pending_cancels' = [pending_cancels EXCEPT ![o] =
                IF ~known_terminal[o]
                   /\ (ask_kind[o] = "Cancel" \/ cancel_after[o] \/ pending_cancels[o] > 0)
                THEN 1 ELSE pending_cancels[o]]
            /\ ask_kind' = [ask_kind EXCEPT ![o] = "Trades"]
            /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
            /\ due_at' = [due_at EXCEPT ![o] = clock]
            /\ UNCHANGED cancel_calls
       ELSE IF cancel_after[o] /\ ~known_terminal[o] /\ ask_kind[o] # "Cancel"
            THEN /\ ask_kind' = [ask_kind EXCEPT ![o] = "Cancel"]
                 /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
                 /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
                 /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
                 /\ pending_cancels' = [pending_cancels EXCEPT ![o] = 1]
                 /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + StatusDelay)]
            ELSE /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
                 /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + 1)]
                 /\ UNCHANGED <<ask_kind, cancel_calls, cancel_after, pending_cancels>>
    /\ UNCHANGED <<
        clock, dispatches, frozen, hazard_seen, max_queries_hit,
        async_not_sent_seen, next_gen,
        admitted, venue_mode, confirmed, terminal, uncreated, fees_closed, in_doubt,
        local_filled, term_cum, trade_applied, expected_fill, wire_logical_sent,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
        ask_present, place_unacked, known_terminal,
        issued_count, first_doubt, ask_slot, ask_gen,
        desk_slot_order, desk_slot_gen, desk_slot_kind,
        wire_active, wire_order, wire_gen, wire_step
       >>

(*
10. `AsyncRestQuerier` zero-byte reply (`RestStatus::NotBuilt | RestStatus::Expired`) ->
`QueryDesk::on_not_sent` (crates/tickwire-binance-account/src/query_desk_driver.rs:545-554,
crates/tickwire-spot-runner/src/query_desk_driver.rs:335-344):
Zero wire bytes were sent (`wire_logical_sent` unchanged); routes to `on_not_sent`, leaving
`issued_count` and `cancel_calls` untouched and promoting `cancel_after`/`trades_after`!
*)
QuerierReplyNotBuiltOrExpired(s) ==
    /\ wire_active[s]
    /\ TicketMatchesDesk(s)
    /\ ~async_not_sent_seen
    /\ LET o == wire_order[s] IN
       /\ async_not_sent_seen' = TRUE
       /\ wire_active' = [wire_active EXCEPT ![s] = FALSE]
       /\ wire_order' = [wire_order EXCEPT ![s] = 0]
       /\ wire_gen' = [wire_gen EXCEPT ![s] = 0]
       /\ wire_step' = [wire_step EXCEPT ![s] = "None"]
       /\ desk_slot_order' = [desk_slot_order EXCEPT ![s] = 0]
       /\ desk_slot_gen' = [desk_slot_gen EXCEPT ![s] = 0]
       /\ desk_slot_kind' = [desk_slot_kind EXCEPT ![s] = "None"]
       /\ ask_slot' = [ask_slot EXCEPT ![o] = 0]
       /\ ask_gen' = [ask_gen EXCEPT ![o] = 0]
       /\ IF trades_after[o] /\ ask_kind[o] # "Trades"
          THEN /\ cancel_after' = [cancel_after EXCEPT ![o] =
                    IF ~known_terminal[o]
                       /\ (ask_kind[o] = "Cancel" \/ cancel_after[o] \/ pending_cancels[o] > 0)
                    THEN TRUE ELSE cancel_after[o]]
               /\ pending_cancels' = [pending_cancels EXCEPT ![o] =
                    IF ~known_terminal[o]
                       /\ (ask_kind[o] = "Cancel" \/ cancel_after[o] \/ pending_cancels[o] > 0)
                    THEN 1 ELSE pending_cancels[o]]
               /\ ask_kind' = [ask_kind EXCEPT ![o] = "Trades"]
               /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
               /\ due_at' = [due_at EXCEPT ![o] = clock]
               /\ UNCHANGED cancel_calls
          ELSE IF cancel_after[o] /\ ~known_terminal[o] /\ ask_kind[o] # "Cancel"
               THEN /\ ask_kind' = [ask_kind EXCEPT ![o] = "Cancel"]
                    /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
                    /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
                    /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
                    /\ pending_cancels' = [pending_cancels EXCEPT ![o] = 1]
                    /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + StatusDelay)]
               ELSE /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
                    /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + 1)]
                    /\ UNCHANGED <<ask_kind, cancel_calls, cancel_after, pending_cancels>>
    /\ UNCHANGED <<
        clock, dispatches, frozen, hazard_seen, max_queries_hit,
        local_not_sent_seen, next_gen,
        admitted, venue_mode, confirmed, terminal, uncreated, fees_closed, in_doubt,
        local_filled, term_cum, trade_applied, expected_fill, wire_logical_sent,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
        ask_present, place_unacked, known_terminal, issued_count, first_doubt
       >>

(*
11. Stale wire reply discarded after concurrent WS `remove_order`
(crates/tickwire-account/src/query_desk.rs:220-233, 430-450,
crates/tickwire-spot-runner/src/query_desk_driver.rs:517-524):
Frees `wire_active[s]` cleanly without mutating `QueryDesk` or chaining Bybit `/v5/order/history`!
*)
DiscardStaleWireReply(s) ==
    /\ wire_active[s]
    /\ ~TicketMatchesDesk(s)
    /\ wire_active' = [wire_active EXCEPT ![s] = FALSE]
    /\ wire_order' = [wire_order EXCEPT ![s] = 0]
    /\ wire_gen' = [wire_gen EXCEPT ![s] = 0]
    /\ wire_step' = [wire_step EXCEPT ![s] = "None"]
    /\ UNCHANGED <<
        clock, dispatches, frozen, hazard_seen, max_queries_hit,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        admitted, venue_mode, confirmed, terminal, uncreated, fees_closed, in_doubt,
        local_filled, term_cum, trade_applied, expected_fill, wire_logical_sent,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
        ask_present, ask_kind, place_unacked, pending_cancels, cancel_calls, cancel_after,
        trades_after, known_terminal, issued_count, first_doubt, due_at, ask_slot, ask_gen,
        desk_slot_order, desk_slot_gen, desk_slot_kind
       >>

(*
12. Bybit two-step `realtime -> history` chain (`dispatch_bybit_history`,
crates/tickwire-spot-runner/src/query_desk_driver.rs:426-430, 504-613):
When Bybit `/v5/order/realtime` (`OrderStatusPrimary`) returns empty (`list: []`), NEVER closes
the order directly! Instead verifies `is_ticket_in_flight` and dispatches `/v5/order/history`
(`BybitOrderStatusHistory`) on the same `(slot, generation)` ticket (or short-circuits to
`AskKind::Trades` if an open WS partial fill arrived mid-flight).
*)
BybitRealtimeEmptyChainHistory(s) ==
    /\ VenueIsBybit
    /\ ~frozen
    /\ wire_active[s]
    /\ TicketMatchesDesk(s)
    /\ wire_step[s] = "OrderStatusPrimary"
    /\ LET o == wire_order[s]
           tk == desk_slot_kind[s]
       IN
       /\ venue_mode[o] \in {"Absent", "CanceledWithFill", "OpenPartFill"}
       /\ IF expected_fill[o] > local_filled[o] /\ ~terminal[o]
          THEN /\ wire_logical_sent' = [wire_logical_sent EXCEPT ![o] = wire_logical_sent[o] + 1]
               /\ wire_active' = [wire_active EXCEPT ![s] = FALSE]
               /\ wire_order' = [wire_order EXCEPT ![s] = 0]
               /\ wire_gen' = [wire_gen EXCEPT ![s] = 0]
               /\ wire_step' = [wire_step EXCEPT ![s] = "None"]
               /\ desk_slot_order' = [desk_slot_order EXCEPT ![s] = 0]
               /\ desk_slot_gen' = [desk_slot_gen EXCEPT ![s] = 0]
               /\ desk_slot_kind' = [desk_slot_kind EXCEPT ![s] = "None"]
               /\ ask_slot' = [ask_slot EXCEPT ![o] = 0]
               /\ ask_gen' = [ask_gen EXCEPT ![o] = 0]
               /\ place_unacked' = [place_unacked EXCEPT ![o] = FALSE]
               /\ pending_cancels' = [pending_cancels EXCEPT ![o] =
                    IF tk = "Cancel" /\ pending_cancels[o] > 0 THEN pending_cancels[o] - 1 ELSE pending_cancels[o]]
               /\ cancel_after' = [cancel_after EXCEPT ![o] =
                    IF tk = "Cancel"
                    THEN cancel_after[o]
                    ELSE (cancel_after[o] \/ pending_cancels[o] > 0)]
               /\ ask_kind' = [ask_kind EXCEPT ![o] = "Trades"]
               /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
               /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
               /\ due_at' = [due_at EXCEPT ![o] = clock]
               /\ UNCHANGED bybit_rt_empty_seen
          ELSE /\ venue_mode[o] \in {"Absent", "CanceledWithFill"}
               /\ bybit_rt_empty_seen' = [bybit_rt_empty_seen EXCEPT ![o] = TRUE]
               /\ wire_step' = [wire_step EXCEPT ![s] = "BybitOrderStatusHistory"]
               /\ UNCHANGED <<
                    wire_logical_sent, wire_active, wire_order, wire_gen,
                    desk_slot_order, desk_slot_gen, desk_slot_kind,
                    ask_slot, ask_gen, place_unacked, pending_cancels,
                    cancel_after, ask_kind, trades_after, cancel_calls, due_at
                  >>
    /\ UNCHANGED <<
        clock, dispatches, frozen, hazard_seen, max_queries_hit,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        admitted, venue_mode, confirmed, terminal, uncreated, fees_closed, in_doubt,
        local_filled, term_cum, trade_applied, expected_fill,
        bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
        ask_present, known_terminal, issued_count, first_doubt
       >>

(*
13. Counted-write transport / `5xx` failure -> `QueryDesk::on_transport_failure`
(crates/tickwire-account/src/query_desk.rs:521-645):
Increments `issued_count` when `ticket.kind != Cancel \/ place_unacked` (freezing on
`MaxQueryCount`), promotes `trades_after`, or applies cancel backoff / `MaxCancelAsks`
`CancelGivenUp` (downgrading to `Place` if `place_unacked`, else removing ask).
*)
QuerierReplyTransportFail(s) ==
    /\ ~frozen
    /\ wire_active[s]
    /\ TicketMatchesDesk(s)
    /\ LET o == wire_order[s]
           tk == desk_slot_kind[s]
           counts_q == (tk # "Cancel") \/ place_unacked[o]
           next_q == IF counts_q THEN issued_count[o] + 1 ELSE issued_count[o]
           hit_q_cap == counts_q /\ MaxQueryCount > 0 /\ next_q >= MaxQueryCount
           next_cc == cancel_calls[o] + 1
       IN
       /\ wire_logical_sent' = [wire_logical_sent EXCEPT ![o] = wire_logical_sent[o] + 1]
       /\ wire_active' = [wire_active EXCEPT ![s] = FALSE]
       /\ wire_order' = [wire_order EXCEPT ![s] = 0]
       /\ wire_gen' = [wire_gen EXCEPT ![s] = 0]
       /\ wire_step' = [wire_step EXCEPT ![s] = "None"]
       /\ desk_slot_order' = [desk_slot_order EXCEPT ![s] = 0]
       /\ desk_slot_gen' = [desk_slot_gen EXCEPT ![s] = 0]
       /\ desk_slot_kind' = [desk_slot_kind EXCEPT ![s] = "None"]
       /\ ask_slot' = [ask_slot EXCEPT ![o] = 0]
       /\ ask_gen' = [ask_gen EXCEPT ![o] = 0]
       /\ IF hit_q_cap
          THEN /\ issued_count' = [issued_count EXCEPT ![o] = next_q]
               /\ ask_present' = [ask_present EXCEPT ![o] = FALSE]
               /\ ask_kind' = [ask_kind EXCEPT ![o] = "None"]
               /\ max_queries_hit' = TRUE
               /\ frozen' = TRUE
               /\ UNCHANGED <<
                    pending_cancels, cancel_calls, cancel_after, trades_after,
                    due_at, cancel_resolved_or_given_up
                  >>
          ELSE /\ issued_count' = [issued_count EXCEPT ![o] = next_q]
               /\ max_queries_hit' = max_queries_hit
               /\ frozen' = frozen
               /\ IF trades_after[o] /\ tk # "Trades"
                  THEN /\ cancel_after' = [cancel_after EXCEPT ![o] =
                            IF ~known_terminal[o]
                               /\ (tk = "Cancel" \/ cancel_after[o] \/ pending_cancels[o] > 0)
                            THEN TRUE ELSE cancel_after[o]]
                       /\ pending_cancels' = [pending_cancels EXCEPT ![o] =
                            IF ~known_terminal[o]
                               /\ (tk = "Cancel" \/ cancel_after[o] \/ pending_cancels[o] > 0)
                            THEN 1 ELSE pending_cancels[o]]
                       /\ ask_kind' = [ask_kind EXCEPT ![o] = "Trades"]
                       /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
                       /\ due_at' = [due_at EXCEPT ![o] = clock]
                       /\ UNCHANGED <<ask_present, cancel_calls, cancel_resolved_or_given_up>>
                  ELSE IF tk = "Cancel"
                       THEN IF cancel_after[o]
                            THEN /\ ask_kind' = [ask_kind EXCEPT ![o] = "Cancel"]
                                 /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
                                 /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
                                 /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + StatusDelay)]
                                 /\ UNCHANGED <<ask_present, pending_cancels, trades_after, cancel_resolved_or_given_up>>
                            ELSE IF next_cc >= MaxCancelAsks
                                 THEN /\ cancel_resolved_or_given_up' =
                                            [cancel_resolved_or_given_up EXCEPT ![o] = TRUE]
                                      /\ IF place_unacked[o]
                                         THEN /\ ask_present' = ask_present
                                              /\ ask_kind' = [ask_kind EXCEPT ![o] = "Place"]
                                              /\ pending_cancels' = [pending_cancels EXCEPT ![o] = 0]
                                              /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
                                              /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
                                              /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + 1)]
                                         ELSE /\ ask_present' = [ask_present EXCEPT ![o] = FALSE]
                                              /\ ask_kind' = [ask_kind EXCEPT ![o] = "None"]
                                              /\ cancel_calls' = [cancel_calls EXCEPT ![o] = next_cc]
                                              /\ UNCHANGED <<pending_cancels, cancel_after, due_at>>
                                      /\ UNCHANGED trades_after
                                 ELSE /\ cancel_calls' = [cancel_calls EXCEPT ![o] = next_cc]
                                      /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + 1)]
                                      /\ UNCHANGED <<
                                            ask_present, ask_kind, pending_cancels,
                                            cancel_after, trades_after, cancel_resolved_or_given_up
                                          >>
                       ELSE /\ IF cancel_after[o]
                               THEN /\ ask_kind' = [ask_kind EXCEPT ![o] = "Cancel"]
                                    /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
                                    /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
                                    /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + StatusDelay)]
                               ELSE /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + 1)]
                                    /\ UNCHANGED <<ask_kind, cancel_calls, cancel_after>>
                            /\ UNCHANGED <<ask_present, pending_cancels, trades_after, cancel_resolved_or_given_up>>
    /\ UNCHANGED <<
        clock, dispatches, hazard_seen,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        admitted, venue_mode, confirmed, terminal, uncreated, fees_closed, in_doubt,
        local_filled, term_cum, trade_applied, expected_fill,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested,
        place_unacked, known_terminal, first_doubt
       >>

(*
14. Category B account hazard (`429` / `418` / `401` / `403`,
crates/tickwire-binance-account/src/query_desk_driver.rs:580-635,
crates/tickwire-spot-runner/src/query_desk_driver.rs:349-355, 459-477):
Removes the ask and deterministically freezes the account!
*)
QuerierReplyCategoryBHazard(s) ==
    /\ ~frozen
    /\ ~hazard_seen
    /\ wire_active[s]
    /\ TicketMatchesDesk(s)
    /\ LET o == wire_order[s] IN
       /\ wire_logical_sent' = [wire_logical_sent EXCEPT ![o] = wire_logical_sent[o] + 1]
       /\ wire_active' = [wire_active EXCEPT ![s] = FALSE]
       /\ wire_order' = [wire_order EXCEPT ![s] = 0]
       /\ wire_gen' = [wire_gen EXCEPT ![s] = 0]
       /\ wire_step' = [wire_step EXCEPT ![s] = "None"]
       /\ desk_slot_order' = [desk_slot_order EXCEPT ![s] = 0]
       /\ desk_slot_gen' = [desk_slot_gen EXCEPT ![s] = 0]
       /\ desk_slot_kind' = [desk_slot_kind EXCEPT ![s] = "None"]
       /\ ask_present' = [ask_present EXCEPT ![o] = FALSE]
       /\ ask_kind' = [ask_kind EXCEPT ![o] = "None"]
       /\ ask_slot' = [ask_slot EXCEPT ![o] = 0]
       /\ ask_gen' = [ask_gen EXCEPT ![o] = 0]
       /\ hazard_seen' = TRUE
       /\ frozen' = TRUE
    /\ UNCHANGED <<
        clock, dispatches, max_queries_hit,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        admitted, venue_mode, confirmed, terminal, uncreated, fees_closed, in_doubt,
        local_filled, term_cum, trade_applied, expected_fill,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
        place_unacked, pending_cancels, cancel_calls, cancel_after,
        trades_after, known_terminal, issued_count, first_doubt, due_at
       >>

(*
15. Definitive or pre-grace `NotFound` reply (`QueryDesk::on_not_found`,
crates/tickwire-account/src/query_desk.rs:647-760,
crates/tickwire-binance-account/src/query_desk_driver.rs:685-691,
crates/tickwire-spot-runner/src/query_desk_driver.rs:426-438, 615-660):
On Bybit, reachable ONLY from `BybitOrderStatusHistory` (never from `OrderStatusPrimary`!).
- Unconfirmed before `first_doubt + GraceTicks`: increments `issued_count` (`RetryScheduled`,
  promoting `trades_after` or `cancel_after`, or `FreezeAccount` on `MaxQueryCount`).
- Unconfirmed at/after `first_doubt + GraceTicks`: `CloseUncreated` -> `uncreated = TRUE`.
- Confirmed (`order_confirmed = TRUE`): increments `issued_count` (`FreezeAccount` on
  `MaxQueryCount`, else `FetchTradesBeforeClose` -> `in_doubt = "FillCatchup"` + `AskKind::Trades`).
*)
StatusReplyNotFound(s) ==
    /\ ~frozen
    /\ wire_active[s]
    /\ TicketMatchesDesk(s)
    /\ IF VenueIsBybit
       THEN wire_step[s] = "BybitOrderStatusHistory"
       ELSE wire_step[s] = "OrderStatusPrimary"
    /\ LET o == wire_order[s]
           next_q == issued_count[o] + 1
           hit_q_cap == MaxQueryCount > 0 /\ next_q >= MaxQueryCount
       IN
       /\ venue_mode[o] \in {"Absent", "CanceledWithFill"}
       /\ (venue_mode[o] = "CanceledWithFill" => confirmed[o])
       /\ wire_logical_sent' = [wire_logical_sent EXCEPT ![o] = wire_logical_sent[o] + 1]
       /\ bybit_hist_empty_seen' = [bybit_hist_empty_seen EXCEPT ![o] =
            IF VenueIsBybit THEN TRUE ELSE bybit_hist_empty_seen[o]]
       /\ wire_active' = [wire_active EXCEPT ![s] = FALSE]
       /\ wire_order' = [wire_order EXCEPT ![s] = 0]
       /\ wire_gen' = [wire_gen EXCEPT ![s] = 0]
       /\ wire_step' = [wire_step EXCEPT ![s] = "None"]
       /\ desk_slot_order' = [desk_slot_order EXCEPT ![s] = 0]
       /\ desk_slot_gen' = [desk_slot_gen EXCEPT ![s] = 0]
       /\ desk_slot_kind' = [desk_slot_kind EXCEPT ![s] = "None"]
       /\ ask_slot' = [ask_slot EXCEPT ![o] = 0]
       /\ ask_gen' = [ask_gen EXCEPT ![o] = 0]
       /\ IF ~confirmed[o]
          THEN IF clock < first_doubt[o] + GraceTicks
               THEN /\ issued_count' = [issued_count EXCEPT ![o] = next_q]
                    /\ IF hit_q_cap
                       THEN /\ ask_present' = [ask_present EXCEPT ![o] = FALSE]
                            /\ ask_kind' = [ask_kind EXCEPT ![o] = "None"]
                            /\ max_queries_hit' = TRUE
                            /\ frozen' = TRUE
                            /\ UNCHANGED <<
                                pending_cancels, cancel_calls, cancel_after,
                                trades_after, due_at
                               >>
                       ELSE IF trades_after[o] /\ ask_kind[o] # "Trades"
                            THEN /\ cancel_after' = [cancel_after EXCEPT ![o] =
                                        IF ~known_terminal[o]
                                           /\ (ask_kind[o] = "Cancel" \/ cancel_after[o] \/ pending_cancels[o] > 0)
                                        THEN TRUE ELSE cancel_after[o]]
                                 /\ pending_cancels' = [pending_cancels EXCEPT ![o] =
                                        IF ~known_terminal[o]
                                           /\ (ask_kind[o] = "Cancel" \/ cancel_after[o] \/ pending_cancels[o] > 0)
                                        THEN 1 ELSE pending_cancels[o]]
                                 /\ ask_kind' = [ask_kind EXCEPT ![o] = "Trades"]
                                 /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
                                 /\ due_at' = [due_at EXCEPT ![o] = clock]
                                 /\ UNCHANGED <<ask_present, cancel_calls, max_queries_hit, frozen>>
                            ELSE IF cancel_after[o] /\ ~known_terminal[o]
                                 THEN /\ ask_kind' = [ask_kind EXCEPT ![o] = "Cancel"]
                                      /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
                                      /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
                                      /\ pending_cancels' = [pending_cancels EXCEPT ![o] = 1]
                                      /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + StatusDelay)]
                                      /\ UNCHANGED <<ask_present, trades_after, max_queries_hit, frozen>>
                                 ELSE /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + 1)]
                                      /\ UNCHANGED <<
                                            ask_present, ask_kind, pending_cancels, cancel_calls,
                                            cancel_after, trades_after, max_queries_hit, frozen
                                          >>
                    /\ UNCHANGED <<uncreated, fees_closed, in_doubt, place_unacked, known_terminal>>
               ELSE /\ uncreated' = [uncreated EXCEPT ![o] = TRUE]
                    /\ fees_closed' = [fees_closed EXCEPT ![o] = TRUE]
                    /\ in_doubt' = [in_doubt EXCEPT ![o] = "None"]
                    /\ ask_present' = [ask_present EXCEPT ![o] = FALSE]
                    /\ ask_kind' = [ask_kind EXCEPT ![o] = "None"]
                    /\ UNCHANGED <<
                        issued_count, due_at, max_queries_hit, frozen, place_unacked,
                        pending_cancels, cancel_calls, cancel_after, trades_after, known_terminal
                       >>
          ELSE /\ issued_count' = [issued_count EXCEPT ![o] = next_q]
               /\ IF hit_q_cap
                  THEN /\ ask_present' = [ask_present EXCEPT ![o] = FALSE]
                       /\ ask_kind' = [ask_kind EXCEPT ![o] = "None"]
                       /\ max_queries_hit' = TRUE
                       /\ frozen' = TRUE
                       /\ UNCHANGED <<
                            in_doubt, place_unacked, pending_cancels, cancel_calls,
                            cancel_after, trades_after, known_terminal, due_at
                          >>
                  ELSE /\ ask_present' = ask_present
                       /\ ask_kind' = [ask_kind EXCEPT ![o] = "Trades"]
                       /\ in_doubt' = [in_doubt EXCEPT ![o] = "FillCatchup"]
                       /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
                       /\ known_terminal' = [known_terminal EXCEPT ![o] = TRUE]
                       /\ place_unacked' = [place_unacked EXCEPT ![o] = FALSE]
                       /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
                       /\ pending_cancels' = [pending_cancels EXCEPT ![o] = 0]
                       /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
                       /\ due_at' = [due_at EXCEPT ![o] = clock]
                       /\ UNCHANGED <<max_queries_hit, frozen>>
               /\ UNCHANGED <<uncreated, fees_closed>>
    /\ UNCHANGED <<
        clock, dispatches, hazard_seen,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        admitted, venue_mode, confirmed, terminal,
        local_filled, term_cum, trade_applied, expected_fill,
        bybit_rt_empty_seen, cancel_requested, cancel_resolved_or_given_up, first_doubt
       >>

(*
16. `200 OK` order status snapshot (`QueryDesk::on_status_snapshot`,
crates/tickwire-account/src/query_desk.rs:762-867,
crates/tickwire-binance-account/src/query_desk_driver.rs:741-876,
crates/tickwire-spot-runner/src/query_desk_driver.rs:662-807):
Includes `stale_zero = TRUE` out-of-order stale open snapshot (`remote_cum = 0` arriving after
a lagging WS `orders` event already set `expected_fill = 1` or `terminal = TRUE`), verifying
no false freeze, preservation of `cancel_after` across non-terminal `Trades` transitions, and
clearing of `CancelAmbiguous` on `CancelRejectedStillOpen { remaining_pending_cancels: 0 }`.
*)
StatusReplySnapshot(s, stale_zero) ==
    /\ ~frozen
    /\ wire_active[s]
    /\ TicketMatchesDesk(s)
    /\ wire_step[s] \in {"OrderStatusPrimary", "BybitOrderStatusHistory"}
    /\ LET o == wire_order[s]
           tk == desk_slot_kind[s]
           vm == venue_mode[o]
       IN
       /\ vm \in {"OpenFlat", "OpenPartFill", "CanceledWithFill"}
       /\ (VenueIsBybit /\ wire_step[s] = "OrderStatusPrimary") => vm # "CanceledWithFill"
       /\ stale_zero => expected_fill[o] = 1
       /\ LET remote_term == (~stale_zero) /\ VenueTerminal(vm)
              remote_cum == IF stale_zero THEN 0 ELSE VenueCum(vm)
              eff_term == remote_term \/ terminal[o]
              base_cum == Max2(Max2(remote_cum, local_filled[o]), expected_fill[o])
              eff_cum == IF (terminal[o] /\ term_cum[o] > local_filled[o])
                            \/ in_doubt[o] = "FillCatchup"
                         THEN Max2(base_cum, local_filled[o] + 1)
                         ELSE base_cum
              need_trades == (eff_cum > local_filled[o]) \/ trades_after[o]
              next_pc_trades ==
                IF ~eff_term /\ ~known_terminal[o]
                THEN IF tk = "Cancel" /\ pending_cancels[o] > 0
                     THEN pending_cancels[o] - 1
                     ELSE pending_cancels[o]
                ELSE 0
              next_ca_trades ==
                IF ~eff_term /\ ~known_terminal[o]
                THEN IF tk = "Cancel"
                     THEN (cancel_after[o] \/ next_pc_trades > 0)
                     ELSE (cancel_after[o] \/ pending_cancels[o] > 0)
                ELSE FALSE
              next_pc_open ==
                IF pending_cancels[o] > 0 THEN pending_cancels[o] - 1 ELSE 0
              rearm_open_cancel == cancel_after[o] \/ next_pc_open > 0
          IN
          /\ wire_logical_sent' = [wire_logical_sent EXCEPT ![o] = wire_logical_sent[o] + 1]
          /\ wire_active' = [wire_active EXCEPT ![s] = FALSE]
          /\ wire_order' = [wire_order EXCEPT ![s] = 0]
          /\ wire_gen' = [wire_gen EXCEPT ![s] = 0]
          /\ wire_step' = [wire_step EXCEPT ![s] = "None"]
          /\ desk_slot_order' = [desk_slot_order EXCEPT ![s] = 0]
          /\ desk_slot_gen' = [desk_slot_gen EXCEPT ![s] = 0]
          /\ desk_slot_kind' = [desk_slot_kind EXCEPT ![s] = "None"]
          /\ ask_slot' = [ask_slot EXCEPT ![o] = 0]
          /\ ask_gen' = [ask_gen EXCEPT ![o] = 0]
          /\ confirmed' = [confirmed EXCEPT ![o] = TRUE]
          /\ place_unacked' = [place_unacked EXCEPT ![o] = FALSE]
          /\ IF need_trades
             THEN /\ expected_fill' = [expected_fill EXCEPT ![o] = eff_cum]
                  /\ terminal' = [terminal EXCEPT ![o] = eff_term]
                  /\ term_cum' = [term_cum EXCEPT ![o] =
                        IF eff_term THEN eff_cum ELSE term_cum[o]]
                  /\ fees_closed' = fees_closed
                  /\ in_doubt' = [in_doubt EXCEPT ![o] =
                        IF eff_term \/ in_doubt[o] # "CancelAmbiguous"
                        THEN "FillCatchup"
                        ELSE "CancelAmbiguous"]
                  /\ ask_present' = ask_present
                  /\ ask_kind' = [ask_kind EXCEPT ![o] = "Trades"]
                  /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
                  /\ known_terminal' = [known_terminal EXCEPT ![o] = (known_terminal[o] \/ eff_term)]
                  /\ pending_cancels' = [pending_cancels EXCEPT ![o] = next_pc_trades]
                  /\ cancel_after' = [cancel_after EXCEPT ![o] = next_ca_trades]
                  /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
                  /\ due_at' = [due_at EXCEPT ![o] = clock]
                  /\ cancel_resolved_or_given_up' =
                        [cancel_resolved_or_given_up EXCEPT ![o] =
                            (cancel_resolved_or_given_up[o]
                             \/ (~eff_term /\ tk = "Cancel" /\ ~next_ca_trades))]
             ELSE /\ expected_fill' = [expected_fill EXCEPT ![o] = 0]
                  /\ terminal' = [terminal EXCEPT ![o] = eff_term]
                  /\ term_cum' = [term_cum EXCEPT ![o] =
                        IF eff_term THEN local_filled[o] ELSE term_cum[o]]
                  /\ fees_closed' = [fees_closed EXCEPT ![o] = (fees_closed[o] \/ eff_term)]
                  /\ known_terminal' = [known_terminal EXCEPT ![o] = (known_terminal[o] \/ eff_term)]
                  /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
                  /\ IF tk = "Cancel" /\ ~eff_term
                     THEN /\ pending_cancels' = [pending_cancels EXCEPT ![o] = next_pc_open]
                          /\ IF rearm_open_cancel
                             THEN /\ ask_present' = ask_present
                                  /\ ask_kind' = [ask_kind EXCEPT ![o] = "Cancel"]
                                  /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
                                  /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
                                  /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + StatusDelay)]
                                  /\ in_doubt' = [in_doubt EXCEPT ![o] = "CancelAmbiguous"]
                                  /\ UNCHANGED cancel_resolved_or_given_up
                             ELSE /\ ask_present' = [ask_present EXCEPT ![o] = FALSE]
                                  /\ ask_kind' = [ask_kind EXCEPT ![o] = "None"]
                                  /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
                                  /\ in_doubt' = [in_doubt EXCEPT ![o] = "None"]
                                  /\ cancel_resolved_or_given_up' =
                                        [cancel_resolved_or_given_up EXCEPT ![o] = TRUE]
                                  /\ UNCHANGED <<cancel_calls, due_at>>
                     ELSE IF cancel_after[o] /\ ~eff_term
                          THEN /\ ask_present' = ask_present
                               /\ ask_kind' = [ask_kind EXCEPT ![o] = "Cancel"]
                               /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
                               /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
                               /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + StatusDelay)]
                               /\ in_doubt' = [in_doubt EXCEPT ![o] = "CancelAmbiguous"]
                               /\ UNCHANGED <<pending_cancels, cancel_resolved_or_given_up>>
                          ELSE /\ ask_present' = [ask_present EXCEPT ![o] = FALSE]
                               /\ ask_kind' = [ask_kind EXCEPT ![o] = "None"]
                               /\ pending_cancels' = [pending_cancels EXCEPT ![o] =
                                    IF eff_term THEN 0 ELSE pending_cancels[o]]
                               /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
                               /\ in_doubt' = [in_doubt EXCEPT ![o] = "None"]
                               /\ UNCHANGED <<cancel_calls, due_at, cancel_resolved_or_given_up>>
    /\ UNCHANGED <<
        clock, dispatches, frozen, hazard_seen, max_queries_hit,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        admitted, venue_mode, uncreated, local_filled, trade_applied,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested,
        issued_count, first_doubt
       >>

(*
17. `200 OK` per-order trades reply (`QueryDesk::on_trades_applied`,
crates/tickwire-account/src/query_desk.rs:869-920,
crates/tickwire-binance-account/src/query_desk_driver.rs:878-1025,
crates/tickwire-spot-runner/src/query_desk_driver.rs:809-973):
Applies missing trade fills with `trade_id` deduplication, records `FeesReconciled` once
`terminal /\ local_filled == term_cum`, and re-arms `AskKind::Cancel` if `cancel_after` or
`pending_cancels > 0` remains on a non-terminal order!
*)
TradesReplyApply(s) ==
    /\ ~frozen
    /\ wire_active[s]
    /\ TicketMatchesDesk(s)
    /\ wire_step[s] = "OrderTrades"
    /\ LET o == wire_order[s]
           vm == venue_mode[o]
           has_fill == vm \in {"OpenPartFill", "CanceledWithFill"}
           is_term == terminal[o] \/ VenueTerminal(vm)
           rearm_c == ~is_term /\ ~known_terminal[o]
                      /\ (cancel_after[o] \/ pending_cancels[o] > 0)
       IN
       /\ wire_logical_sent' = [wire_logical_sent EXCEPT ![o] = wire_logical_sent[o] + 1]
       /\ wire_active' = [wire_active EXCEPT ![s] = FALSE]
       /\ wire_order' = [wire_order EXCEPT ![s] = 0]
       /\ wire_gen' = [wire_gen EXCEPT ![s] = 0]
       /\ wire_step' = [wire_step EXCEPT ![s] = "None"]
       /\ desk_slot_order' = [desk_slot_order EXCEPT ![s] = 0]
       /\ desk_slot_gen' = [desk_slot_gen EXCEPT ![s] = 0]
       /\ desk_slot_kind' = [desk_slot_kind EXCEPT ![s] = "None"]
       /\ ask_slot' = [ask_slot EXCEPT ![o] = 0]
       /\ ask_gen' = [ask_gen EXCEPT ![o] = 0]
       /\ confirmed' = [confirmed EXCEPT ![o] = (confirmed[o] \/ has_fill)]
       /\ trade_applied' = [trade_applied EXCEPT ![o] = (trade_applied[o] \/ has_fill)]
       /\ local_filled' = [local_filled EXCEPT ![o] =
            IF has_fill THEN 1 ELSE local_filled[o]]
       /\ expected_fill' = [expected_fill EXCEPT ![o] = 0]
       /\ terminal' = [terminal EXCEPT ![o] = is_term]
       /\ term_cum' = [term_cum EXCEPT ![o] =
            IF is_term THEN VenueCum(vm) ELSE term_cum[o]]
       /\ fees_closed' = [fees_closed EXCEPT ![o] = (fees_closed[o] \/ is_term)]
       /\ in_doubt' = [in_doubt EXCEPT ![o] =
            IF is_term
            THEN "None"
            ELSE IF rearm_c \/ in_doubt[o] = "CancelAmbiguous"
                 THEN "CancelAmbiguous"
                 ELSE "None"]
       /\ IF rearm_c
          THEN /\ ask_present' = ask_present
               /\ ask_kind' = [ask_kind EXCEPT ![o] = "Cancel"]
               /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
               /\ cancel_calls' = [cancel_calls EXCEPT ![o] = 0]
               /\ cancel_after' = [cancel_after EXCEPT ![o] = FALSE]
               /\ pending_cancels' = [pending_cancels EXCEPT ![o] = 1]
               /\ due_at' = [due_at EXCEPT ![o] = CapDue(clock + StatusDelay)]
          ELSE /\ ask_present' = [ask_present EXCEPT ![o] = FALSE]
               /\ ask_kind' = [ask_kind EXCEPT ![o] = "None"]
               /\ trades_after' = [trades_after EXCEPT ![o] = FALSE]
               /\ UNCHANGED <<cancel_calls, cancel_after, pending_cancels, due_at>>
    /\ UNCHANGED <<
        clock, dispatches, frozen, hazard_seen, max_queries_hit,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        admitted, venue_mode, uncreated,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
        place_unacked, known_terminal, issued_count, first_doubt
       >>

(*
18. Monotonic clock advance (`clock -> clock + 1`):
*)
AdvanceClock ==
    /\ clock < MaxClock
    /\ clock' = clock + 1
    /\ UNCHANGED <<
        dispatches, frozen, hazard_seen, max_queries_hit,
        local_not_sent_seen, async_not_sent_seen, next_gen,
        admitted, venue_mode, confirmed, terminal, uncreated, fees_closed, in_doubt,
        local_filled, term_cum, trade_applied, expected_fill, wire_logical_sent,
        bybit_rt_empty_seen, bybit_hist_empty_seen, cancel_requested, cancel_resolved_or_given_up,
        ask_present, ask_kind, place_unacked, pending_cancels, cancel_calls, cancel_after,
        trades_after, known_terminal, issued_count, first_doubt, due_at, ask_slot, ask_gen,
        desk_slot_order, desk_slot_gen, desk_slot_kind,
        wire_active, wire_order, wire_gen, wire_step
       >>

Next ==
    \/ \E o \in Orders, vm \in VenueModes : IsolateAmbiguousPlace(o, vm)
    \/ \E o \in Orders, vm \in (VenueModes \ {"Absent"}) : AdmitConfirmedOrder(o, vm)
    \/ \E o \in Orders : EnqueueAmbiguousCancel(o)
    \/ \E o \in Orders : WsLaggingOrderPush(o)
    \/ \E o \in Orders : WsFlatTerminalPush(o)
    \/ \E o \in Orders : WsTradeFillPush(o)
    \/ \E o \in Orders : PollSkipResolvedAsk(o)
    \/ \E o \in Orders, s \in Slots : PollAndDispatchDue(o, s)
    \/ \E o \in Orders, s \in Slots : DispatchNotSentLocal(o, s)
    \/ \E s \in Slots : QuerierReplyNotBuiltOrExpired(s)
    \/ \E s \in Slots : DiscardStaleWireReply(s)
    \/ \E s \in Slots : BybitRealtimeEmptyChainHistory(s)
    \/ \E s \in Slots : QuerierReplyTransportFail(s)
    \/ \E s \in Slots : QuerierReplyCategoryBHazard(s)
    \/ \E s \in Slots : StatusReplyNotFound(s)
    \/ \E s \in Slots, sz \in BOOLEAN : StatusReplySnapshot(s, sz)
    \/ \E s \in Slots : TradesReplyApply(s)
    \/ AdvanceClock

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants *)

TypeOK ==
    /\ clock \in 0..MaxClock
    /\ dispatches \in 0..MaxDispatches
    /\ frozen \in BOOLEAN
    /\ hazard_seen \in BOOLEAN
    /\ max_queries_hit \in BOOLEAN
    /\ local_not_sent_seen \in BOOLEAN
    /\ async_not_sent_seen \in BOOLEAN
    /\ next_gen \in 1..(MaxDispatches + 1)
    /\ admitted \in [Orders -> BOOLEAN]
    /\ venue_mode \in [Orders -> AllVenueModes]
    /\ confirmed \in [Orders -> BOOLEAN]
    /\ terminal \in [Orders -> BOOLEAN]
    /\ uncreated \in [Orders -> BOOLEAN]
    /\ fees_closed \in [Orders -> BOOLEAN]
    /\ in_doubt \in [Orders -> DoubtKinds]
    /\ local_filled \in [Orders -> 0..1]
    /\ term_cum \in [Orders -> -1..1]
    /\ trade_applied \in [Orders -> BOOLEAN]
    /\ expected_fill \in [Orders -> 0..1]
    /\ wire_logical_sent \in [Orders -> 0..MaxDispatches]
    /\ bybit_rt_empty_seen \in [Orders -> BOOLEAN]
    /\ bybit_hist_empty_seen \in [Orders -> BOOLEAN]
    /\ cancel_requested \in [Orders -> BOOLEAN]
    /\ cancel_resolved_or_given_up \in [Orders -> BOOLEAN]
    /\ ask_present \in [Orders -> BOOLEAN]
    /\ ask_kind \in [Orders -> AskKinds]
    /\ place_unacked \in [Orders -> BOOLEAN]
    /\ pending_cancels \in [Orders -> 0..1]
    /\ cancel_calls \in [Orders -> 0..MaxCancelAsks]
    /\ cancel_after \in [Orders -> BOOLEAN]
    /\ trades_after \in [Orders -> BOOLEAN]
    /\ known_terminal \in [Orders -> BOOLEAN]
    /\ issued_count \in [Orders -> 0..MaxQueryCount]
    /\ first_doubt \in [Orders -> -1..MaxClock]
    /\ due_at \in [Orders -> 0..(MaxClock + 1)]
    /\ ask_slot \in [Orders -> (Slots \cup {0})]
    /\ ask_gen \in [Orders -> 0..MaxDispatches]
    /\ desk_slot_order \in [Slots -> (Orders \cup {0})]
    /\ desk_slot_gen \in [Slots -> 0..MaxDispatches]
    /\ desk_slot_kind \in [Slots -> AskKinds]
    /\ wire_active \in [Slots -> BOOLEAN]
    /\ wire_order \in [Slots -> (Orders \cup {0})]
    /\ wire_gen \in [Slots -> 0..MaxDispatches]
    /\ wire_step \in [Slots -> WireSteps]

(*
1. `AtMostOneAskAndOneSlotPerOrder`:
At most 1 `StatusAsk` and at most 1 in-flight REST slot (`QueryDesk` slot and `AsyncRestQuerier`
wire slot) per `OrderKey`; waiting asks (`ask_slot[o] = 0`) hold `0` REST slots during delay or
backoff; and `dispatch_due` never aliases an active wire slot even after mid-flight `remove_order`.
*)
AtMostOneAskAndOneSlotPerOrder ==
    /\ \A o \in Orders :
        /\ (~ask_present[o]) => (ask_slot[o] = 0 /\ ask_kind[o] = "None")
        /\ (ask_slot[o] = 0) => (\A s \in Slots : desk_slot_order[s] # o)
        /\ (ask_slot[o] > 0) =>
            /\ ask_present[o]
            /\ desk_slot_order[ask_slot[o]] = o
            /\ desk_slot_gen[ask_slot[o]] = ask_gen[o]
    /\ \A s \in Slots :
        /\ (desk_slot_order[s] # 0) =>
            /\ ask_present[desk_slot_order[s]]
            /\ ask_slot[desk_slot_order[s]] = s
            /\ ask_gen[desk_slot_order[s]] = desk_slot_gen[s]
            /\ wire_active[s]
            /\ wire_order[s] = desk_slot_order[s]
            /\ wire_gen[s] = desk_slot_gen[s]
    /\ \A s1, s2 \in Slots :
        (s1 # s2) =>
            /\ (desk_slot_order[s1] # 0 => desk_slot_order[s1] # desk_slot_order[s2])
            /\ ((wire_active[s1] /\ wire_active[s2]) => wire_order[s1] # wire_order[s2])

(*
2. `IssuedOnlyCountsSent`:
`on_not_sent` (local ring/quota refusal or `AsyncRestQuerier` `NotBuilt`/`Expired`) never
increments `issued_query_count` or `cancel_calls` and never triggers `FreezeAccount`.
*)
IssuedOnlyCountsSent ==
    /\ \A o \in Orders :
        /\ issued_count[o] <= wire_logical_sent[o]
        /\ cancel_calls[o] <= wire_logical_sent[o]
        /\ (wire_logical_sent[o] = 0) => (issued_count[o] = 0 /\ cancel_calls[o] = 0)
    /\ ((\A o \in Orders : wire_logical_sent[o] = 0) /\ ~hazard_seen) => ~frozen

(*
3. `TradesBeforeFeeClosure`:
`FeesReconciled` is NEVER recorded while `local_filled < venue_cumulative`, confirmed orders
returning `NotFound` never close as `uncreated` (they transition to `AskKind::Trades` first),
and duplicate WS + REST trade fills apply at most once (`trade_id` deduplication).
*)
TradesBeforeFeeClosure ==
    \A o \in Orders :
        /\ local_filled[o] <= 1
        /\ (trade_applied[o] <=> local_filled[o] = 1)
        /\ ~(confirmed[o] /\ uncreated[o])
        /\ fees_closed[o] =>
            IF uncreated[o]
            THEN local_filled[o] = 0 /\ VenueCum(venue_mode[o]) = 0
            ELSE /\ terminal[o]
                 /\ local_filled[o] = term_cum[o]
                 /\ local_filled[o] >= expected_fill[o]
                 /\ local_filled[o] = VenueCum(venue_mode[o])

(*
4. `CancelAfterNeverLost`:
`known_terminal` is never latched on an order that is actually open on the venue, and whenever
an ambiguous `Cancel` has joined an order (`cancel_requested == TRUE`) and has not yet resolved
(`CancelRejectedStillOpen`) or exhausted `MaxCancelAsks`, while `!frozen`, `!terminal`,
`!known_terminal`, and `!uncreated`, `QueryDesk` retains either `ask_kind = "Cancel"` or
`cancel_after = TRUE` across `OrderStatus` and non-terminal `Trades`, and re-arms `Cancel`
once any in-flight `OrderStatus`/`Trades` reply leaves the order open.
*)
CancelAfterNeverLost ==
    \A o \in Orders :
        /\ known_terminal[o] => VenueTerminal(venue_mode[o])
        /\ (cancel_requested[o]
            /\ ~cancel_resolved_or_given_up[o]
            /\ ~frozen
            /\ ~terminal[o]
            /\ ~known_terminal[o]
            /\ ~uncreated[o]) =>
                /\ ask_present[o]
                /\ (ask_kind[o] = "Cancel" \/ cancel_after[o])
                /\ (ask_slot[o] = 0 /\ ask_kind[o] # "Trades") => ask_kind[o] = "Cancel"

(*
5. `BybitTwoStepSoundness`:
On Bybit (`VenueIsBybit == TRUE`), `CloseUncreated` (`uncreated == TRUE`) NEVER fires from an
empty `realtime` response alone without a subsequent empty `history` response past
`first_doubt_mono_ns + not_found_grace_ns`.
*)
BybitTwoStepSoundness ==
    \A o \in Orders :
        (VenueIsBybit /\ uncreated[o]) =>
            /\ bybit_rt_empty_seen[o]
            /\ bybit_hist_empty_seen[o]
            /\ first_doubt[o] >= 0
            /\ clock >= first_doubt[o] + GraceTicks
            /\ ~confirmed[o]

(*
6. `BoundedEscalationToFreeze`:
Reaching `MaxQueryCount > 0` on issued non-definitive queries (`max_queries_hit`) or receiving
a Category B hazard (`429`/`418`/`401`, `hazard_seen`) deterministically sets `frozen = TRUE`.
*)
BoundedEscalationToFreeze ==
    /\ (hazard_seen \/ max_queries_hit) => frozen
    /\ \A o \in Orders :
        (MaxQueryCount > 0 /\ ask_present[o]) => issued_count[o] < MaxQueryCount

=============================================================================
