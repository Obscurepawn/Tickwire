------------------------ MODULE L0_PerOrderIsolation ------------------------
EXTENDS Integers, FiniteSets

CONSTANTS
    Orders,
    WalletCap,
    GraceTicks,
    RetWindow,
    MaxClock,
    AllowContradictedUncreated

(*
Code-faithful model of Stage 2 per-order doubt isolation (`per_order_doubt_isolation = true`),
non-freezing deterministic `VenueEventBody::Rejected` (`observe_rejected`), `Account::reopen_order`
(`Order::reopen_aborted` + `shadow_holds` restoration), `WindowedTable` time-window eviction
(`Order::is_retirable` + `Account::evict_expired`), and durable WAL replay equivalence.

Variable <-> Rust source mapping:
- clock: monotonic timestamp (`now_mono_ns`) in `0..MaxClock`,
  crates/tickwire-account/src/lib.rs:828-834,
  crates/tickwire-account/src/query_desk.rs:647-727.
- conn_up, reconnected, halted, gw_status: `AccountGateway.connection.is_some()`, replacement
  link adoption (`install_transport_connection`), `AccountGateway.halted`, and per-intent
  `Entry.status` (`"None"`, `"Queued"`, `"MayHaveSent"`, `"Flushed"`, `"Aborted"`, `"Unknown"`),
  crates/tickwire-account-gateway/src/lib.rs:1647-1803.
- admitted, order_qty, confirmed, terminal, uncreated, aborted, fees_closed,
  observation_unreconciled, in_doubt, filled, term_cum, shadow_hold, reduce_hold,
  retired_at, evicted: `Account.orders` (`struct Order`), `Account.shadow_holds`,
  and `Account::evict_expired`,
  crates/tickwire-account/src/lib.rs:48-175,
  crates/tickwire-account/src/lib.rs:828-990,
  crates/tickwire-account/src/admission.rs:350-411,
  crates/tickwire-account/src/admission.rs:526-562,
  crates/tickwire-account/src/events.rs:219-227,
  crates/tickwire-account/src/events.rs:433-537,
  crates/tickwire-account/src/events.rs:620-682.
- settled_spend, uncovered_debits, frozen: `Asset.cashflow`, `Account.uncovered_debits`,
  and `Account.frozen`,
  crates/tickwire-account/src/lib.rs:223-231,
  crates/tickwire-account/src/admission.rs:502-524,
  crates/tickwire-account/src/recovery.rs:192-220.
- first_doubt_at, pre_grace_queried: `StatusAsk.first_doubt_mono_ns` and pre-grace
  `NotFoundAction::RetryScheduled` recorded when an order enters `PlaceAmbiguous`,
  crates/tickwire-account/src/query_desk.rs:342-362,
  crates/tickwire-account/src/query_desk.rs:647-727.
- venue_state, late_indexed, dup_delivered: exchange matching-engine state (`"None"`,
  `"Absent"`, `"PendingIndex"`, `"Open"`, `"Filled"`, `"Rejected"`), late `recvWindow`
  indexing witness, and `Applied::Duplicate` replay witness,
  crates/tickwire-account/src/lib.rs:84-119,
  crates/tickwire-account/src/events.rs:644-646.
- rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_uncreated, rep_aborted,
  rep_fees_closed, rep_obs_unrec, rep_filled, rep_term_cum, rep_shadow_hold,
  rep_reduce_hold, rep_settled_spend: deterministic crash-recovery reducer (`Account::recover`)
  driven solely by durable `RecoveryRecord` entries (`Admit`, `Aborted`, `VenueEvent`,
  `FeesReconciled`) without transient `in_doubt` or transport state,
  crates/tickwire-account/src/lib.rs:84-119,
  crates/tickwire-account/src/recovery.rs:1-190.
*)

VARIABLES
    clock,
    conn_up,
    reconnected,
    halted,
    gw_status,
    admitted,
    order_qty,
    confirmed,
    terminal,
    uncreated,
    aborted,
    fees_closed,
    observation_unreconciled,
    in_doubt,
    filled,
    term_cum,
    shadow_hold,
    reduce_hold,
    retired_at,
    evicted,
    settled_spend,
    uncovered_debits,
    frozen,
    first_doubt_at,
    pre_grace_queried,
    venue_state,
    late_indexed,
    dup_delivered,
    rep_admitted,
    rep_qty,
    rep_confirmed,
    rep_terminal,
    rep_uncreated,
    rep_aborted,
    rep_fees_closed,
    rep_obs_unrec,
    rep_filled,
    rep_term_cum,
    rep_shadow_hold,
    rep_reduce_hold,
    rep_settled_spend

vars == <<
    clock, conn_up, reconnected, halted, gw_status,
    admitted, order_qty, confirmed, terminal, uncreated, aborted, fees_closed,
    observation_unreconciled, in_doubt, filled, term_cum, shadow_hold, reduce_hold,
    retired_at, evicted, settled_spend, uncovered_debits, frozen,
    first_doubt_at, pre_grace_queried, venue_state, late_indexed, dup_delivered,
    rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_uncreated, rep_aborted,
    rep_fees_closed, rep_obs_unrec, rep_filled, rep_term_cum, rep_shadow_hold,
    rep_reduce_hold, rep_settled_spend
>>

GwStatuses == {"None", "Queued", "MayHaveSent", "Flushed", "Aborted", "Unknown"}
DoubtStates == {"None", "PlaceAmbiguous"}
VenueStates == {"None", "Absent", "PendingIndex", "Open", "Filled", "Rejected"}

RECURSIVE SumOver(_, _)
SumOver(S, f) ==
    IF S = {} THEN 0
    ELSE LET x == CHOOSE v \in S : TRUE
         IN f[x] + SumOver(S \ {x}, f)

\* Exact `Order::remaining(&self)` from crates/tickwire-account/src/lib.rs:148-163.
OrderRemaining(o) ==
    IF ~admitted[o] \/ evicted[o] \/ aborted[o] \/ uncreated[o]
       \/ (terminal[o] /\ term_cum[o] = filled[o])
    THEN 0
    ELSE IF order_qty[o] > filled[o] THEN order_qty[o] - filled[o] ELSE 0

\* Exact `Order::is_retirable(&self)` from crates/tickwire-account/src/lib.rs:164-175.
IsRetirable(o) ==
    admitted[o]
    /\ ~evicted[o]
    /\ (aborted[o] \/ uncreated[o] \/ (terminal[o] /\ fees_closed[o] /\ OrderRemaining(o) = 0))
    /\ ~observation_unreconciled[o]
    /\ in_doubt[o] = "None"
    /\ reduce_hold[o] = 0

RefreshedRetiredAt(o, retirable, prev_retired, now) ==
    IF retirable
    THEN IF prev_retired >= 0 THEN prev_retired ELSE now
    ELSE -1

PrincipalReserved ==
    SumOver(Orders, [o \in Orders |-> OrderRemaining(o)])

TotalPrincipalExposure ==
    settled_spend + PrincipalReserved

TotalShadowDeduction ==
    SumOver(Orders, shadow_hold) + uncovered_debits

VenueShadowAvailable ==
    WalletCap - TotalShadowDeduction

TotalReduceExposure ==
    settled_spend + SumOver(Orders, reduce_hold)

NoPendingSend ==
    \A o \in Orders : gw_status[o] \notin {"Queued", "MayHaveSent"}

Init ==
    /\ clock = 0
    /\ conn_up = TRUE
    /\ reconnected = FALSE
    /\ halted = FALSE
    /\ gw_status = [o \in Orders |-> "None"]
    /\ admitted = [o \in Orders |-> FALSE]
    /\ order_qty = [o \in Orders |-> 0]
    /\ confirmed = [o \in Orders |-> FALSE]
    /\ terminal = [o \in Orders |-> FALSE]
    /\ uncreated = [o \in Orders |-> FALSE]
    /\ aborted = [o \in Orders |-> FALSE]
    /\ fees_closed = [o \in Orders |-> FALSE]
    /\ observation_unreconciled = [o \in Orders |-> FALSE]
    /\ in_doubt = [o \in Orders |-> "None"]
    /\ filled = [o \in Orders |-> 0]
    /\ term_cum = [o \in Orders |-> -1]
    /\ shadow_hold = [o \in Orders |-> 0]
    /\ reduce_hold = [o \in Orders |-> 0]
    /\ retired_at = [o \in Orders |-> -1]
    /\ evicted = [o \in Orders |-> FALSE]
    /\ settled_spend = 0
    /\ uncovered_debits = 0
    /\ frozen = FALSE
    /\ first_doubt_at = [o \in Orders |-> -1]
    /\ pre_grace_queried = FALSE
    /\ venue_state = [o \in Orders |-> "None"]
    /\ late_indexed = FALSE
    /\ dup_delivered = FALSE
    /\ rep_admitted = [o \in Orders |-> FALSE]
    /\ rep_qty = [o \in Orders |-> 0]
    /\ rep_confirmed = [o \in Orders |-> FALSE]
    /\ rep_terminal = [o \in Orders |-> FALSE]
    /\ rep_uncreated = [o \in Orders |-> FALSE]
    /\ rep_aborted = [o \in Orders |-> FALSE]
    /\ rep_fees_closed = [o \in Orders |-> FALSE]
    /\ rep_obs_unrec = [o \in Orders |-> FALSE]
    /\ rep_filled = [o \in Orders |-> 0]
    /\ rep_term_cum = [o \in Orders |-> -1]
    /\ rep_shadow_hold = [o \in Orders |-> 0]
    /\ rep_reduce_hold = [o \in Orders |-> 0]
    /\ rep_settled_spend = 0

(*
1. `AccountGateway::submit` + `Account::admit` (crates/tickwire-account/src/admission.rs:350-411):
Admits order `o` with size `qty \in {1, WalletCap}` if available Spot principal, Futures
`venue_shadow`, and derivative `reduce` headroom cover `qty`. Commits WAL `Admit` before send.
*)
SubmitOrder(o, qty) ==
    /\ ~admitted[o]
    /\ ~frozen
    /\ ~halted
    /\ conn_up
    /\ NoPendingSend
    /\ TotalPrincipalExposure + qty <= WalletCap
    /\ qty <= VenueShadowAvailable
    /\ TotalReduceExposure + qty <= WalletCap
    /\ admitted' = [admitted EXCEPT ![o] = TRUE]
    /\ order_qty' = [order_qty EXCEPT ![o] = qty]
    /\ shadow_hold' = [shadow_hold EXCEPT ![o] = qty]
    /\ reduce_hold' = [reduce_hold EXCEPT ![o] = qty]
    /\ gw_status' = [gw_status EXCEPT ![o] = "Queued"]
    /\ rep_admitted' = [rep_admitted EXCEPT ![o] = TRUE]
    /\ rep_qty' = [rep_qty EXCEPT ![o] = qty]
    /\ rep_shadow_hold' = [rep_shadow_hold EXCEPT ![o] = qty]
    /\ rep_reduce_hold' = [rep_reduce_hold EXCEPT ![o] = qty]
    /\ UNCHANGED <<
        clock, conn_up, reconnected, halted,
        confirmed, terminal, uncreated, aborted, fees_closed, observation_unreconciled,
        in_doubt, filled, term_cum, retired_at, evicted, settled_spend, uncovered_debits,
        frozen, first_doubt_at, pre_grace_queried, venue_state, late_indexed, dup_delivered,
        rep_confirmed, rep_terminal, rep_uncreated, rep_aborted, rep_fees_closed,
        rep_obs_unrec, rep_filled, rep_term_cum, rep_settled_spend
       >>

(*
2. `AccountGateway::abort` when `accepted_bytes == 0` (`Queued` -> `NoBytes` pre-send abort,
crates/tickwire-account/src/admission.rs:526-562):
Appends WAL `Aborted` and releases principal, `reduce`, and `shadow_holds` cleanly.
*)
AbortUnsentBeforeFirstByte(o) ==
    /\ gw_status[o] = "Queued"
    /\ ~confirmed[o]
    /\ ~terminal[o]
    /\ gw_status' = [gw_status EXCEPT ![o] = "Aborted"]
    /\ aborted' = [aborted EXCEPT ![o] = TRUE]
    /\ fees_closed' = [fees_closed EXCEPT ![o] = TRUE]
    /\ shadow_hold' = [shadow_hold EXCEPT ![o] = 0]
    /\ reduce_hold' = [reduce_hold EXCEPT ![o] = 0]
    /\ retired_at' = [retired_at EXCEPT ![o] = clock]
    /\ rep_aborted' = [rep_aborted EXCEPT ![o] = TRUE]
    /\ rep_fees_closed' = [rep_fees_closed EXCEPT ![o] = TRUE]
    /\ rep_shadow_hold' = [rep_shadow_hold EXCEPT ![o] = 0]
    /\ rep_reduce_hold' = [rep_reduce_hold EXCEPT ![o] = 0]
    /\ UNCHANGED <<
        clock, conn_up, reconnected, halted,
        admitted, order_qty, confirmed, terminal, uncreated, observation_unreconciled,
        in_doubt, filled, term_cum, evicted, settled_spend, uncovered_debits, frozen,
        first_doubt_at, pre_grace_queried, venue_state, late_indexed, dup_delivered,
        rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_uncreated,
        rep_obs_unrec, rep_filled, rep_term_cum, rep_settled_spend
       >>

(*
3. `AccountGateway::check_admission` + first TLS plaintext byte accepted (`Queued` -> `MayHaveSent`,
crates/tickwire-account/src/admission.rs:480-501):
Once > 0 bytes enter TLS, the wire packet may be dropped (`"Absent"`), queued for indexing
within `recvWindow` (`"PendingIndex"`), resting (`"Open"`), matched (`"Filled"`), or
deterministically rejected by venue rules (`"Rejected"`).
*)
AcceptFirstByte(o, vstate) ==
    /\ conn_up
    /\ ~halted
    /\ ~frozen
    /\ gw_status[o] = "Queued"
    /\ in_doubt[o] = "None"
    /\ vstate \in {"Absent", "PendingIndex", "Open", "Filled", "Rejected"}
    /\ gw_status' = [gw_status EXCEPT ![o] = "MayHaveSent"]
    /\ venue_state' = [venue_state EXCEPT ![o] = vstate]
    /\ UNCHANGED <<
        clock, conn_up, reconnected, halted,
        admitted, order_qty, confirmed, terminal, uncreated, aborted, fees_closed,
        observation_unreconciled, in_doubt, filled, term_cum, shadow_hold, reduce_hold,
        retired_at, evicted, settled_spend, uncovered_debits, frozen,
        first_doubt_at, pre_grace_queried, late_indexed, dup_delivered,
        rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_uncreated, rep_aborted,
        rep_fees_closed, rep_obs_unrec, rep_filled, rep_term_cum, rep_shadow_hold,
        rep_reduce_hold, rep_settled_spend
       >>

(*
4. `SendState::Flushed` (`MayHaveSent` -> `Flushed`): outbound frame flushed to socket,
freeing `pending` for subsequent sibling strategy intents on the connection.
*)
FlushOrder(o) ==
    /\ conn_up
    /\ ~halted
    /\ gw_status[o] = "MayHaveSent"
    /\ gw_status' = [gw_status EXCEPT ![o] = "Flushed"]
    /\ UNCHANGED <<
        clock, conn_up, reconnected, halted,
        admitted, order_qty, confirmed, terminal, uncreated, aborted, fees_closed,
        observation_unreconciled, in_doubt, filled, term_cum, shadow_hold, reduce_hold,
        retired_at, evicted, settled_spend, uncovered_debits, frozen,
        first_doubt_at, pre_grace_queried, venue_state, late_indexed, dup_delivered,
        rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_uncreated, rep_aborted,
        rep_fees_closed, rep_obs_unrec, rep_filled, rep_term_cum, rep_shadow_hold,
        rep_reduce_hold, rep_settled_spend
       >>

(*
5. `AccountGateway::drop_broken_transport_isolated`
(crates/tickwire-account-gateway/src/lib.rs:1725-1772):
Mid-send socket drop on `MayHaveSent` isolates ONLY `o` into `Status::Unknown` +
`DoubtReason::PlaceAmbiguous`, leaves `halted = FALSE` and `frozen = FALSE`, and preserves
all `Flushed` sibling entries!
*)
TransportDropIsolated(o) ==
    /\ conn_up
    /\ ~halted
    /\ gw_status[o] = "MayHaveSent"
    /\ ~aborted[o]
    /\ ~uncreated[o]
    /\ ~terminal[o]
    /\ conn_up' = FALSE
    /\ gw_status' = [gw_status EXCEPT ![o] = "Unknown"]
    /\ in_doubt' = [in_doubt EXCEPT ![o] = "PlaceAmbiguous"]
    /\ retired_at' = [retired_at EXCEPT ![o] = -1]
    /\ first_doubt_at' = [first_doubt_at EXCEPT ![o] =
        IF first_doubt_at[o] >= 0 THEN first_doubt_at[o] ELSE clock]
    /\ UNCHANGED <<
        clock, reconnected, halted,
        admitted, order_qty, confirmed, terminal, uncreated, aborted, fees_closed,
        observation_unreconciled, filled, term_cum, shadow_hold, reduce_hold,
        evicted, settled_spend, uncovered_debits, frozen,
        pre_grace_queried, venue_state, late_indexed, dup_delivered,
        rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_uncreated, rep_aborted,
        rep_fees_closed, rep_obs_unrec, rep_filled, rep_term_cum, rep_shadow_hold,
        rep_reduce_hold, rep_settled_spend
       >>

(*
6. Category A single-order response ambiguity (HTTP 503 / `-1007` / WS request timeout)
on a `Flushed` order (`crates/tickwire-account-gateway/src/lib.rs:1647-1717`):
Isolates `o` in `PlaceAmbiguous` while keeping the transport connection up (`conn_up = TRUE`),
`halted = FALSE`, and `frozen = FALSE`.
*)
AmbiguousResponseIsolated(o) ==
    /\ gw_status[o] = "Flushed"
    /\ ~confirmed[o]
    /\ ~terminal[o]
    /\ ~uncreated[o]
    /\ ~aborted[o]
    /\ gw_status' = [gw_status EXCEPT ![o] = "Unknown"]
    /\ in_doubt' = [in_doubt EXCEPT ![o] = "PlaceAmbiguous"]
    /\ retired_at' = [retired_at EXCEPT ![o] = -1]
    /\ first_doubt_at' = [first_doubt_at EXCEPT ![o] =
        IF first_doubt_at[o] >= 0 THEN first_doubt_at[o] ELSE clock]
    /\ UNCHANGED <<
        clock, conn_up, reconnected, halted,
        admitted, order_qty, confirmed, terminal, uncreated, aborted, fees_closed,
        observation_unreconciled, filled, term_cum, shadow_hold, reduce_hold,
        evicted, settled_spend, uncovered_debits, frozen,
        pre_grace_queried, venue_state, late_indexed, dup_delivered,
        rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_uncreated, rep_aborted,
        rep_fees_closed, rep_obs_unrec, rep_filled, rep_term_cum, rep_shadow_hold,
        rep_reduce_hold, rep_settled_spend
       >>

(*
7. `AccountGateway::install_transport_connection`
(crates/tickwire-account-gateway/src/lib.rs:1774-1803):
Installs a pre-warmed replacement `Connection` when `!conn_up /\ !halted /\ !frozen`
without requiring full-account `Reactivated`.
*)
InstallReplacementTransport ==
    /\ ~conn_up
    /\ ~halted
    /\ ~frozen
    /\ NoPendingSend
    /\ conn_up' = TRUE
    /\ reconnected' = TRUE
    /\ UNCHANGED <<
        clock, halted, gw_status,
        admitted, order_qty, confirmed, terminal, uncreated, aborted, fees_closed,
        observation_unreconciled, in_doubt, filled, term_cum, shadow_hold, reduce_hold,
        retired_at, evicted, settled_spend, uncovered_debits, frozen,
        first_doubt_at, pre_grace_queried, venue_state, late_indexed, dup_delivered,
        rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_uncreated, rep_aborted,
        rep_fees_closed, rep_obs_unrec, rep_filled, rep_term_cum, rep_shadow_hold,
        rep_reduce_hold, rep_settled_spend
       >>

(*
8. Delayed venue matching-engine indexing within `recvWindow` (`not_found_grace_ns`):
An order initially invisible to status queries (`"PendingIndex"`) becomes `"Open"` strictly
before `first_doubt_at[o] + GraceTicks` elapses.
*)
VenueIndexLate(o) ==
    /\ venue_state[o] = "PendingIndex"
    /\ (first_doubt_at[o] < 0 \/ clock < first_doubt_at[o] + GraceTicks)
    /\ venue_state' = [venue_state EXCEPT ![o] = "Open"]
    /\ late_indexed' = TRUE
    /\ UNCHANGED <<
        clock, conn_up, reconnected, halted, gw_status,
        admitted, order_qty, confirmed, terminal, uncreated, aborted, fees_closed,
        observation_unreconciled, in_doubt, filled, term_cum, shadow_hold, reduce_hold,
        retired_at, evicted, settled_spend, uncovered_debits, frozen,
        first_doubt_at, pre_grace_queried, dup_delivered,
        rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_uncreated, rep_aborted,
        rep_fees_closed, rep_obs_unrec, rep_filled, rep_term_cum, rep_shadow_hold,
        rep_reduce_hold, rep_settled_spend
       >>

(*
9. Deterministic single-order venue reject (`VenueEventBody::Rejected`,
crates/tickwire-account/src/events.rs:620-682):
Sets `uncreated = TRUE` and `fees_closed = TRUE`, clears `reduce`, `in_doubt`, and
`shadow_holds`, refreshes `retired_mono_ns`, and keeps `frozen = FALSE`!
*)
DeliverDeterministicReject(o) ==
    /\ admitted[o]
    /\ ~evicted[o]
    /\ venue_state[o] = "Rejected"
    /\ ~uncreated[o]
    /\ ~aborted[o]
    /\ ~confirmed[o]
    /\ ~terminal[o]
    /\ filled[o] = 0
    /\ ~observation_unreconciled[o]
    /\ uncreated' = [uncreated EXCEPT ![o] = TRUE]
    /\ fees_closed' = [fees_closed EXCEPT ![o] = TRUE]
    /\ in_doubt' = [in_doubt EXCEPT ![o] = "None"]
    /\ shadow_hold' = [shadow_hold EXCEPT ![o] = 0]
    /\ reduce_hold' = [reduce_hold EXCEPT ![o] = 0]
    /\ retired_at' = [retired_at EXCEPT ![o] = clock]
    /\ gw_status' = [gw_status EXCEPT ![o] =
        IF gw_status[o] = "MayHaveSent" THEN "Flushed" ELSE gw_status[o]]
    /\ rep_uncreated' = [rep_uncreated EXCEPT ![o] = TRUE]
    /\ rep_fees_closed' = [rep_fees_closed EXCEPT ![o] = TRUE]
    /\ rep_shadow_hold' = [rep_shadow_hold EXCEPT ![o] = 0]
    /\ rep_reduce_hold' = [rep_reduce_hold EXCEPT ![o] = 0]
    /\ UNCHANGED <<
        clock, conn_up, reconnected, halted,
        admitted, order_qty, confirmed, terminal, aborted, observation_unreconciled,
        filled, term_cum, evicted, settled_spend, uncovered_debits, frozen,
        first_doubt_at, pre_grace_queried, venue_state, late_indexed, dup_delivered,
        rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_aborted,
        rep_obs_unrec, rep_filled, rep_term_cum, rep_settled_spend
       >>

(*
10. `QueryDesk::on_not_found` BEFORE `first_doubt_mono_ns + not_found_grace_ns`
(crates/tickwire-account/src/query_desk.rs:647-720):
Treats `"Absent"` / `"PendingIndex"` as non-definitive (`RetryScheduled`); NEVER sets
`uncreated = TRUE` or releases reservations!
*)
QueryNotFoundBeforeGrace(o) ==
    /\ ~pre_grace_queried
    /\ admitted[o]
    /\ ~evicted[o]
    /\ in_doubt[o] = "PlaceAmbiguous"
    /\ ~confirmed[o]
    /\ venue_state[o] \in {"Absent", "PendingIndex"}
    /\ first_doubt_at[o] >= 0
    /\ clock < first_doubt_at[o] + GraceTicks
    /\ pre_grace_queried' = TRUE
    /\ UNCHANGED <<
        clock, conn_up, reconnected, halted, gw_status,
        admitted, order_qty, confirmed, terminal, uncreated, aborted, fees_closed,
        observation_unreconciled, in_doubt, filled, term_cum, shadow_hold, reduce_hold,
        retired_at, evicted, settled_spend, uncovered_debits, frozen,
        first_doubt_at, venue_state, late_indexed, dup_delivered,
        rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_uncreated, rep_aborted,
        rep_fees_closed, rep_obs_unrec, rep_filled, rep_term_cum, rep_shadow_hold,
        rep_reduce_hold, rep_settled_spend
       >>

(*
11. `QueryDesk::on_not_found` AT OR AFTER `first_doubt_mono_ns + not_found_grace_ns`
(crates/tickwire-account/src/query_desk.rs:722-728):
Returns `CloseUncreated` -> records `VenueEventBody::Rejected`, setting `uncreated = TRUE`
and releasing principal, `reduce`, and `shadow_holds`.
*)
QueryNotFoundAfterGrace(o) ==
    /\ admitted[o]
    /\ ~evicted[o]
    /\ in_doubt[o] = "PlaceAmbiguous"
    /\ ~confirmed[o]
    /\ ~terminal[o]
    /\ filled[o] = 0
    /\ ~uncreated[o]
    /\ ~aborted[o]
    /\ ~observation_unreconciled[o]
    /\ venue_state[o] = "Absent"
    /\ first_doubt_at[o] >= 0
    /\ clock >= first_doubt_at[o] + GraceTicks
    /\ uncreated' = [uncreated EXCEPT ![o] = TRUE]
    /\ fees_closed' = [fees_closed EXCEPT ![o] = TRUE]
    /\ in_doubt' = [in_doubt EXCEPT ![o] = "None"]
    /\ shadow_hold' = [shadow_hold EXCEPT ![o] = 0]
    /\ reduce_hold' = [reduce_hold EXCEPT ![o] = 0]
    /\ retired_at' = [retired_at EXCEPT ![o] = clock]
    /\ rep_uncreated' = [rep_uncreated EXCEPT ![o] = TRUE]
    /\ rep_fees_closed' = [rep_fees_closed EXCEPT ![o] = TRUE]
    /\ rep_shadow_hold' = [rep_shadow_hold EXCEPT ![o] = 0]
    /\ rep_reduce_hold' = [rep_reduce_hold EXCEPT ![o] = 0]
    /\ UNCHANGED <<
        clock, conn_up, reconnected, halted, gw_status,
        admitted, order_qty, confirmed, terminal, aborted, observation_unreconciled,
        filled, term_cum, evicted, settled_spend, uncovered_debits, frozen,
        first_doubt_at, pre_grace_queried, venue_state, late_indexed, dup_delivered,
        rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_aborted,
        rep_obs_unrec, rep_filled, rep_term_cum, rep_settled_spend
       >>

(*
12. `VenueEventBody::Order(Canceled, cum = 0)` on an `"Open"` order
(crates/tickwire-account/src/events.rs:458-537):
Confirms and terminates the order at `cum = 0`, releasing Spot principal and `reduce`
while preserving Futures `shadow_hold` until `Reactivated`, and clearing `PlaceAmbiguous`.
*)
DeliverVenueFlatCancel(o) ==
    /\ admitted[o]
    /\ ~evicted[o]
    /\ venue_state[o] = "Open"
    /\ ~uncreated[o]
    /\ ~aborted[o]
    /\ ~terminal[o]
    /\ filled[o] = 0
    /\ gw_status[o] \in {"Flushed", "Unknown"}
    /\ confirmed' = [confirmed EXCEPT ![o] = TRUE]
    /\ terminal' = [terminal EXCEPT ![o] = TRUE]
    /\ term_cum' = [term_cum EXCEPT ![o] = 0]
    /\ reduce_hold' = [reduce_hold EXCEPT ![o] = 0]
    /\ in_doubt' = [in_doubt EXCEPT ![o] = "None"]
    /\ rep_confirmed' = [rep_confirmed EXCEPT ![o] = TRUE]
    /\ rep_terminal' = [rep_terminal EXCEPT ![o] = TRUE]
    /\ rep_term_cum' = [rep_term_cum EXCEPT ![o] = 0]
    /\ rep_reduce_hold' = [rep_reduce_hold EXCEPT ![o] = 0]
    /\ UNCHANGED <<
        clock, conn_up, reconnected, halted, gw_status,
        admitted, order_qty, uncreated, aborted, fees_closed, observation_unreconciled,
        filled, shadow_hold, retired_at, evicted, settled_spend, uncovered_debits,
        frozen, first_doubt_at, pre_grace_queried, venue_state, late_indexed, dup_delivered,
        rep_admitted, rep_qty, rep_uncreated, rep_aborted, rep_fees_closed,
        rep_obs_unrec, rep_filled, rep_shadow_hold, rep_settled_spend
       >>

(*
13. `VenueEventBody::Fill` on an `"Open"` or `"Filled"` order
(crates/tickwire-account/src/events.rs:228-355):
Books full fill `order_qty[o]` into `settled_spend`, updates `reduce_hold := 0`, preserves
`shadow_hold[o]`, clears `PlaceAmbiguous`, and marks `terminal = TRUE`.
*)
DeliverVenueFill(o) ==
    /\ admitted[o]
    /\ ~evicted[o]
    /\ venue_state[o] \in {"Open", "Filled"}
    /\ ~uncreated[o]
    /\ ~aborted[o]
    /\ ~terminal[o]
    /\ filled[o] = 0
    /\ gw_status[o] \in {"Flushed", "Unknown"}
    /\ confirmed' = [confirmed EXCEPT ![o] = TRUE]
    /\ terminal' = [terminal EXCEPT ![o] = TRUE]
    /\ filled' = [filled EXCEPT ![o] = order_qty[o]]
    /\ term_cum' = [term_cum EXCEPT ![o] = order_qty[o]]
    /\ reduce_hold' = [reduce_hold EXCEPT ![o] = 0]
    /\ in_doubt' = [in_doubt EXCEPT ![o] = "None"]
    /\ settled_spend' = settled_spend + order_qty[o]
    /\ rep_confirmed' = [rep_confirmed EXCEPT ![o] = TRUE]
    /\ rep_terminal' = [rep_terminal EXCEPT ![o] = TRUE]
    /\ rep_filled' = [rep_filled EXCEPT ![o] = rep_qty[o]]
    /\ rep_term_cum' = [rep_term_cum EXCEPT ![o] = rep_qty[o]]
    /\ rep_reduce_hold' = [rep_reduce_hold EXCEPT ![o] = 0]
    /\ rep_settled_spend' = rep_settled_spend + rep_qty[o]
    /\ UNCHANGED <<
        clock, conn_up, reconnected, halted, gw_status,
        admitted, order_qty, uncreated, aborted, fees_closed, observation_unreconciled,
        shadow_hold, retired_at, evicted, uncovered_debits, frozen,
        first_doubt_at, pre_grace_queried, venue_state, late_indexed, dup_delivered,
        rep_admitted, rep_qty, rep_uncreated, rep_aborted, rep_fees_closed,
        rep_obs_unrec, rep_shadow_hold
       >>

(*
14. Duplicate venue event delivery (`Applied::Duplicate`,
crates/tickwire-account/src/lib.rs:84-119,
crates/tickwire-account/src/events.rs:644-646):
Re-delivering an already-recorded `Rejected` or `Fill` event is a pure no-op on both live
and replayed state (`PartialEq for Order` ignores `in_doubt` and `retired_mono_ns`).
*)
DeliverDuplicateVenueFact(o) ==
    /\ ~dup_delivered
    /\ admitted[o]
    /\ ~evicted[o]
    /\ (uncreated[o] \/ (terminal[o] /\ filled[o] = order_qty[o]))
    /\ dup_delivered' = TRUE
    /\ UNCHANGED <<
        clock, conn_up, reconnected, halted, gw_status,
        admitted, order_qty, confirmed, terminal, uncreated, aborted, fees_closed,
        observation_unreconciled, in_doubt, filled, term_cum, shadow_hold, reduce_hold,
        retired_at, evicted, settled_spend, uncovered_debits, frozen,
        first_doubt_at, pre_grace_queried, venue_state, late_indexed,
        rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_uncreated, rep_aborted,
        rep_fees_closed, rep_obs_unrec, rep_filled, rep_term_cum, rep_shadow_hold,
        rep_reduce_hold, rep_settled_spend
       >>

(*
15. `Account::reconcile_fees` (crates/tickwire-account/src/events.rs:565-619):
Closes fees once `terminal /\ term_cum == filled`, refreshing `retired_mono_ns`.
*)
ReconcileOrderFees(o) ==
    /\ admitted[o]
    /\ ~evicted[o]
    /\ terminal[o]
    /\ ~fees_closed[o]
    /\ term_cum[o] = filled[o]
    /\ LET next_ret ==
            ~observation_unreconciled[o]
            /\ in_doubt[o] = "None"
            /\ reduce_hold[o] = 0
       IN
       /\ fees_closed' = [fees_closed EXCEPT ![o] = TRUE]
       /\ retired_at' = [retired_at EXCEPT ![o] =
            RefreshedRetiredAt(o, next_ret, retired_at[o], clock)]
       /\ rep_fees_closed' = [rep_fees_closed EXCEPT ![o] = TRUE]
       /\ UNCHANGED <<
            clock, conn_up, reconnected, halted, gw_status,
            admitted, order_qty, confirmed, terminal, uncreated, aborted,
            observation_unreconciled, in_doubt, filled, term_cum, shadow_hold,
            reduce_hold, evicted, settled_spend, uncovered_debits, frozen,
            first_doubt_at, pre_grace_queried, venue_state, late_indexed, dup_delivered,
            rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_uncreated,
            rep_aborted, rep_obs_unrec, rep_filled, rep_term_cum, rep_shadow_hold,
            rep_reduce_hold, rep_settled_spend
          >>

(*
16. Adversarial late venue ack arriving AFTER `uncreated == TRUE`
(`AllowContradictedUncreated = TRUE` in `L0_PerOrderIsolation_adversarial.cfg`,
crates/tickwire-account/src/lib.rs:123-144,
crates/tickwire-account/src/lib.rs:957-967,
crates/tickwire-account/src/events.rs:468-473):
`Account::reopen_order` clears `uncreated` / `aborted` / `fees_closed` / `retired_mono_ns`,
latches `observation_unreconciled = TRUE`, restores `reduce` AND unrefreshed `shadow_holds`,
and sets `frozen = TRUE`!
*)
AdversarialLateFactAfterUncreated(o) ==
    /\ AllowContradictedUncreated
    /\ admitted[o]
    /\ ~evicted[o]
    /\ uncreated[o]
    /\ confirmed' = [confirmed EXCEPT ![o] = TRUE]
    /\ uncreated' = [uncreated EXCEPT ![o] = FALSE]
    /\ aborted' = [aborted EXCEPT ![o] = FALSE]
    /\ fees_closed' = [fees_closed EXCEPT ![o] = FALSE]
    /\ observation_unreconciled' = [observation_unreconciled EXCEPT ![o] = TRUE]
    /\ shadow_hold' = [shadow_hold EXCEPT ![o] = order_qty[o]]
    /\ reduce_hold' = [reduce_hold EXCEPT ![o] = order_qty[o] - filled[o]]
    /\ retired_at' = [retired_at EXCEPT ![o] = -1]
    /\ frozen' = TRUE
    /\ rep_confirmed' = [rep_confirmed EXCEPT ![o] = TRUE]
    /\ rep_uncreated' = [rep_uncreated EXCEPT ![o] = FALSE]
    /\ rep_aborted' = [rep_aborted EXCEPT ![o] = FALSE]
    /\ rep_fees_closed' = [rep_fees_closed EXCEPT ![o] = FALSE]
    /\ rep_obs_unrec' = [rep_obs_unrec EXCEPT ![o] = TRUE]
    /\ rep_shadow_hold' = [rep_shadow_hold EXCEPT ![o] = rep_qty[o]]
    /\ rep_reduce_hold' = [rep_reduce_hold EXCEPT ![o] = rep_qty[o] - rep_filled[o]]
    /\ UNCHANGED <<
        clock, conn_up, reconnected, halted, gw_status,
        admitted, order_qty, terminal, in_doubt, filled, term_cum, evicted,
        settled_spend, uncovered_debits, first_doubt_at, pre_grace_queried,
        venue_state, late_indexed, dup_delivered,
        rep_admitted, rep_qty, rep_terminal, rep_filled, rep_term_cum, rep_settled_spend
       >>

(*
17. `Account::evict_expired(clock)` (crates/tickwire-account/src/lib.rs:828-950):
Reclaims only `IsRetirable(o)` orders past `retired_at[o] + RetWindow`, folding any
remaining unrefreshed `shadow_hold[o]` into `uncovered_debits` so `venue_shadow` is preserved.
*)
EvictExpired(o) ==
    /\ IsRetirable(o)
    /\ retired_at[o] >= 0
    /\ clock >= retired_at[o] + RetWindow
    /\ evicted' = [evicted EXCEPT ![o] = TRUE]
    /\ uncovered_debits' = uncovered_debits + shadow_hold[o]
    /\ shadow_hold' = [shadow_hold EXCEPT ![o] = 0]
    /\ UNCHANGED <<
        clock, conn_up, reconnected, halted, gw_status,
        admitted, order_qty, confirmed, terminal, uncreated, aborted, fees_closed,
        observation_unreconciled, in_doubt, filled, term_cum, reduce_hold, retired_at,
        settled_spend, frozen, first_doubt_at, pre_grace_queried,
        venue_state, late_indexed, dup_delivered,
        rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_uncreated, rep_aborted,
        rep_fees_closed, rep_obs_unrec, rep_filled, rep_term_cum, rep_shadow_hold,
        rep_reduce_hold, rep_settled_spend
       >>

(*
18. Monotonic clock advance (`clock -> clock + 1`):
Any `"PendingIndex"` packet whose venue `recvWindow` (`GraceTicks`) has expired without
indexing transitions to `"Absent"`.
*)
AdvanceClock ==
    /\ clock < MaxClock
    /\ LET next_c == clock + 1 IN
       /\ clock' = next_c
       /\ venue_state' = [o \in Orders |->
            IF venue_state[o] = "PendingIndex"
               /\ first_doubt_at[o] >= 0
               /\ next_c >= first_doubt_at[o] + GraceTicks
            THEN "Absent"
            ELSE venue_state[o]]
    /\ UNCHANGED <<
        conn_up, reconnected, halted, gw_status,
        admitted, order_qty, confirmed, terminal, uncreated, aborted, fees_closed,
        observation_unreconciled, in_doubt, filled, term_cum, shadow_hold, reduce_hold,
        retired_at, evicted, settled_spend, uncovered_debits, frozen,
        first_doubt_at, pre_grace_queried, late_indexed, dup_delivered,
        rep_admitted, rep_qty, rep_confirmed, rep_terminal, rep_uncreated, rep_aborted,
        rep_fees_closed, rep_obs_unrec, rep_filled, rep_term_cum, rep_shadow_hold,
        rep_reduce_hold, rep_settled_spend
       >>

Next ==
    \/ \E o \in Orders, qty \in {1, WalletCap} : SubmitOrder(o, qty)
    \/ \E o \in Orders : AbortUnsentBeforeFirstByte(o)
    \/ \E o \in Orders, vs \in {"Absent", "PendingIndex", "Open", "Filled", "Rejected"} :
        AcceptFirstByte(o, vs)
    \/ \E o \in Orders : FlushOrder(o)
    \/ \E o \in Orders : TransportDropIsolated(o)
    \/ \E o \in Orders : AmbiguousResponseIsolated(o)
    \/ InstallReplacementTransport
    \/ \E o \in Orders : VenueIndexLate(o)
    \/ \E o \in Orders : DeliverDeterministicReject(o)
    \/ \E o \in Orders : QueryNotFoundBeforeGrace(o)
    \/ \E o \in Orders : QueryNotFoundAfterGrace(o)
    \/ \E o \in Orders : DeliverVenueFlatCancel(o)
    \/ \E o \in Orders : DeliverVenueFill(o)
    \/ \E o \in Orders : DeliverDuplicateVenueFact(o)
    \/ \E o \in Orders : ReconcileOrderFees(o)
    \/ \E o \in Orders : AdversarialLateFactAfterUncreated(o)
    \/ \E o \in Orders : EvictExpired(o)
    \/ AdvanceClock

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants *)

TypeOK ==
    /\ clock \in 0..MaxClock
    /\ conn_up \in BOOLEAN
    /\ reconnected \in BOOLEAN
    /\ halted \in BOOLEAN
    /\ gw_status \in [Orders -> GwStatuses]
    /\ admitted \in [Orders -> BOOLEAN]
    /\ order_qty \in [Orders -> 0..WalletCap]
    /\ confirmed \in [Orders -> BOOLEAN]
    /\ terminal \in [Orders -> BOOLEAN]
    /\ uncreated \in [Orders -> BOOLEAN]
    /\ aborted \in [Orders -> BOOLEAN]
    /\ fees_closed \in [Orders -> BOOLEAN]
    /\ observation_unreconciled \in [Orders -> BOOLEAN]
    /\ in_doubt \in [Orders -> DoubtStates]
    /\ filled \in [Orders -> 0..WalletCap]
    /\ term_cum \in [Orders -> -1..WalletCap]
    /\ shadow_hold \in [Orders -> 0..WalletCap]
    /\ reduce_hold \in [Orders -> 0..WalletCap]
    /\ retired_at \in [Orders -> -1..MaxClock]
    /\ evicted \in [Orders -> BOOLEAN]
    /\ settled_spend \in 0..(Cardinality(Orders) * WalletCap)
    /\ uncovered_debits \in 0..(Cardinality(Orders) * WalletCap)
    /\ frozen \in BOOLEAN
    /\ first_doubt_at \in [Orders -> -1..MaxClock]
    /\ pre_grace_queried \in BOOLEAN
    /\ venue_state \in [Orders -> VenueStates]
    /\ late_indexed \in BOOLEAN
    /\ dup_delivered \in BOOLEAN

(*
1. `ReservationLockedWhileInDoubt`:
While an order has `in_doubt != None` and `!uncreated && !aborted`, 100% of its remaining
Spot principal reservation, Futures `shadow_holds`, and derivative `reduce` allocation stays
locked, and `IsRetirable(o) == FALSE` (never retired or evicted!).
*)
ReservationLockedWhileInDoubt ==
    \A o \in Orders :
        (in_doubt[o] # "None" /\ ~uncreated[o] /\ ~aborted[o]) =>
            /\ OrderRemaining(o) = order_qty[o] - filled[o]
            /\ shadow_hold[o] = order_qty[o]
            /\ reduce_hold[o] = order_qty[o] - filled[o]
            /\ ~IsRetirable(o)
            /\ retired_at[o] = -1
            /\ ~evicted[o]

(*
2. `SolvencyNeverViolated`:
Absent an adversarial venue lie after `not_found_grace_ns` (`observation_unreconciled`),
committed exposure across in-doubt orders + active sibling orders + settled spend never
exceeds `WalletCap` or drives `venue_shadow` negative; and any contradicted uncreated
event immediately freezes new risk.
*)
SolvencyNeverViolated ==
    /\ (\A o \in Orders : ~observation_unreconciled[o]) =>
        /\ TotalPrincipalExposure <= WalletCap
        /\ TotalShadowDeduction <= WalletCap
        /\ VenueShadowAvailable >= 0
        /\ TotalReduceExposure <= WalletCap
    /\ (\E o \in Orders : observation_unreconciled[o]) => frozen

(*
3. `HealthySiblingUnblocked`:
Single-order Category A transport drops, `-1007` ambiguity, and deterministic
`VenueEventBody::Rejected` closures never set `halted = TRUE`, never set `frozen = TRUE`
(unless `AllowContradictedUncreated` injected a contradicted uncreated event), and never
clobber `Flushed` sibling entries.
*)
HealthySiblingUnblocked ==
    /\ ~halted
    /\ ((\A o \in Orders : ~observation_unreconciled[o]) => ~frozen)
    /\ \A o1, o2 \in Orders :
        (o1 # o2 /\ in_doubt[o1] # "None" /\ gw_status[o2] = "Flushed") =>
            in_doubt[o2] = "None"

(*
4. `NoPrematureUncreatedBeforeGrace`:
`"NotFound"` (`"Absent"` / `"PendingIndex"`) on an `in_doubt` order before
`first_doubt_mono_ns + not_found_grace_ns` NEVER sets `uncreated = TRUE` or releases
reservations (only an explicit deterministic `"Rejected"` code can set `uncreated` early).
*)
NoPrematureUncreatedBeforeGrace ==
    \A o \in Orders :
        (uncreated[o] /\ venue_state[o] # "Rejected") =>
            /\ first_doubt_at[o] >= 0
            /\ clock >= first_doubt_at[o] + GraceTicks
            /\ venue_state[o] = "Absent"

(*
5. `ContradictedUncreatedFreezes`:
If any late venue observation/fill arrives for an `uncreated == TRUE` order,
`Account::reopen_order()` clears `uncreated`/`fees_closed`/`retired_at`, latches
`observation_unreconciled = TRUE`, restores Spot principal, `reduce`, and `shadow_holds`,
and sets `frozen = TRUE`.
*)
ContradictedUncreatedFreezes ==
    \A o \in Orders :
        observation_unreconciled[o] =>
            /\ frozen
            /\ ~uncreated[o]
            /\ ~aborted[o]
            /\ ~fees_closed[o]
            /\ ~IsRetirable(o)
            /\ ~evicted[o]
            /\ OrderRemaining(o) = order_qty[o] - filled[o]
            /\ shadow_hold[o] = order_qty[o]
            /\ reduce_hold[o] = order_qty[o] - filled[o]

(*
6. `ReplayEquivalence`:
Replaying durable WAL records (`Admit`, `Aborted`, `VenueEvent`, `FeesReconciled`) produces
identical durable `Order` states (`PartialEq for Order`, crates/tickwire-account/src/lib.rs:84-119)
and identical settled spend + shadow deductions regardless of transient `in_doubt`,
transport drops, or time-window eviction.
*)
ReplayEquivalence ==
    /\ settled_spend = rep_settled_spend
    /\ TotalShadowDeduction = SumOver(Orders, rep_shadow_hold)
    /\ \A o \in Orders :
        ~evicted[o] =>
            /\ admitted[o] = rep_admitted[o]
            /\ order_qty[o] = rep_qty[o]
            /\ confirmed[o] = rep_confirmed[o]
            /\ terminal[o] = rep_terminal[o]
            /\ uncreated[o] = rep_uncreated[o]
            /\ aborted[o] = rep_aborted[o]
            /\ fees_closed[o] = rep_fees_closed[o]
            /\ observation_unreconciled[o] = rep_obs_unrec[o]
            /\ filled[o] = rep_filled[o]
            /\ term_cum[o] = rep_term_cum[o]
            /\ shadow_hold[o] = rep_shadow_hold[o]
            /\ reduce_hold[o] = rep_reduce_hold[o]

=============================================================================
