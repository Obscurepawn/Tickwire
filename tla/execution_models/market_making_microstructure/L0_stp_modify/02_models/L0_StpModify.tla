---------------------------- MODULE L0_StpModify ----------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    BuyOrders,
    SellOrders,
    ModifiableOrders,
    BuyPrices,
    SellPrices,
    Qtys,
    QuoteBalance,
    BaseBalance,
    FuturesMarginMode,
    StpEnforced

(*
Code-faithful model of Stage 4 lock-free self-trade prevention (`StpBook`),
single-in-flight order modification (`PendingModify` + `ModificationRecord`),
worst-case Spot principal and Futures `shadow_holds` reservations, `client_id`
rotation and rollback, partial/full fills racing with in-flight modifications
(`supersede_pending_modify_on_terminal`), and `AmendAmbiguous` per-order query
resolution (`QueryDesk` + `AccountSession` including Spot 2-phase `new_client_id` ->
`orderId` fallback).

Variable <-> Rust source mapping:
- buy_keys, buy_ticks, sell_keys, sell_ticks, best_bid, best_ask:
  `SymbolSlot.{buy_orders, sell_orders, best_bid_ticks, best_ask_ticks}`,
  `StpBook::{self_cross, self_cross_excluding, upsert_order, remove_order}`, and
  `aggressive_ticks` (`max` for `Buy`, `min` for `Sell`),
  crates/tickwire-account/src/stp_book.rs:45-103,
  crates/tickwire-account/src/stp_book.rs:174-239,
  crates/tickwire-account/src/stp_book.rs:293-395,
  crates/tickwire-account/src/stp_book.rs:443-450.
- status, price, eff_qty, filled, cid_gen, cid_aliases, vid_gen, in_doubt:
  `Order` lifecycle, resting terms, client-ID & native-ID aliases, and `DoubtReason`,
  crates/tickwire-account/src/lib.rs:125-172,
  crates/tickwire-account/src/admission.rs:286-374,
  crates/tickwire-account/src/events.rs:770-896.
- pm_active, pm_sent, pm_req_px, pm_req_qty, pm_rot_cid, pm_tent_cid, shadow_hold,
  mod_seq, mod_superseded, mod_rejected, mod_aborted, last_rollback_kind,
  pre_mod_px, pre_mod_qty, pre_mod_cid:
  `PendingModify`, `ModificationRecord`, `admit_modify_candidate`,
  `abort_modification`, `observe_modified`, `reject_modification`, and
  `supersede_pending_modify_on_terminal`,
  crates/tickwire-account/src/modifications.rs:12-65,
  crates/tickwire-account/src/modifications.rs:67-421,
  crates/tickwire-account/src/modifications.rs:459-928,
  crates/tickwire-account/src/calculation.rs:150-240.
- query_phase:
  `QueryDesk` + `AccountSession` `AmendAmbiguous` resolution (Spot `origClientOrderId = new_client_id`
  probe with `-2013` fallback to `orderId` / `orig_client_id`),
  crates/tickwire-account/src/query_desk.rs:647-868,
  crates/tickwire-binance-account/src/session.rs:2885-3205.
- quota_spent, origins_active, refused_local:
  pre-wire local STP/modify refusal non-burn in `AccountSession::{submit_new_order,
  submit_modify_order}`, `AccountSession::submit_modify`, and `AccountGateway::abort`,
  crates/tickwire-spot-execution/src/session.rs:1103-1185,
  crates/tickwire-spot-execution/src/session.rs:1300-1375,
  crates/tickwire-binance-account/src/session.rs:1986-2124,
  crates/tickwire-account-gateway/src/lib.rs:1195-1212.
*)

Orders == BuyOrders \cup SellOrders
AllPrices == BuyPrices \cup SellPrices
MIN_TICK == 0
MAX_TICK == 10
Statuses == {"Idle", "Open", "Terminal"}
DoubtStates == {"None", "AmendAmbiguous"}
QueryPhases == {"Idle", "ProbeNewCid", "ProbeOrig"}
RollbackKinds == {"None", "Aborted", "Rejected", "Superseded"}
MaxQuotaBound == 2 * Cardinality(Orders)

ASSUME
    /\ BuyOrders \cap SellOrders = {}
    /\ Orders # {}
    /\ ModifiableOrders \subseteq Orders
    /\ BuyPrices # {} /\ SellPrices # {}
    /\ \A p \in AllPrices : p > MIN_TICK /\ p < MAX_TICK
    /\ Qtys # {} /\ \A q \in Qtys : q >= 1
    /\ QuoteBalance >= 1 /\ BaseBalance >= 1
    /\ FuturesMarginMode \in BOOLEAN
    /\ StpEnforced \in BOOLEAN

VARIABLES
    buy_keys,
    buy_ticks,
    sell_keys,
    sell_ticks,
    best_bid,
    best_ask,
    status,
    price,
    eff_qty,
    filled,
    cid_gen,
    cid_aliases,
    vid_gen,
    in_doubt,
    pm_active,
    pm_sent,
    pm_req_px,
    pm_req_qty,
    pm_rot_cid,
    pm_tent_cid,
    shadow_hold,
    mod_seq,
    mod_superseded,
    mod_rejected,
    mod_aborted,
    last_rollback_kind,
    pre_mod_px,
    pre_mod_qty,
    pre_mod_cid,
    query_phase,
    quota_spent,
    origins_active,
    refused_local

vars == <<
    buy_keys, buy_ticks, sell_keys, sell_ticks, best_bid, best_ask,
    status, price, eff_qty, filled, cid_gen, cid_aliases, vid_gen, in_doubt,
    pm_active, pm_sent, pm_req_px, pm_req_qty, pm_rot_cid, pm_tent_cid, shadow_hold,
    mod_seq, mod_superseded, mod_rejected, mod_aborted, last_rollback_kind,
    pre_mod_px, pre_mod_qty, pre_mod_cid, query_phase,
    quota_spent, origins_active, refused_local
>>

Max(a, b) == IF a >= b THEN a ELSE b
Min(a, b) == IF a <= b THEN a ELSE b
PosPart(x) == IF x > 0 THEN x ELSE 0

IsBuy(o) == o \in BuyOrders

SeqElems(seq) == {seq[i] : i \in 1..Len(seq)}

SeqIndex(seq, elem) ==
    CHOOSE i \in 1..Len(seq) : seq[i] = elem

\* Exact transcription of StpBook::aggressive_ticks
\* (crates/tickwire-account/src/stp_book.rs:443-450).
AggressiveTick(o, rest_px, pend_active, pend_px) ==
    IF ~pend_active THEN rest_px
    ELSE IF IsBuy(o) THEN Max(rest_px, pend_px)
    ELSE Min(rest_px, pend_px)

CurAggressiveTick(o) ==
    AggressiveTick(o, price[o], pm_active[o], pm_req_px[o])

\* Exact transcription of SymbolSlot::{recompute_best_bid, recompute_best_ask}
\* (crates/tickwire-account/src/stp_book.rs:80-98).
RecomputeBestBid(keys, ticks_map) ==
    IF Len(keys) = 0 THEN MIN_TICK
    ELSE
        LET active == SeqElems(keys)
            vals == {ticks_map[k] : k \in active}
        IN CHOOSE m \in vals : \A v \in vals : v <= m

RecomputeBestAsk(keys, ticks_map) ==
    IF Len(keys) = 0 THEN MAX_TICK
    ELSE
        LET active == SeqElems(keys)
            vals == {ticks_map[k] : k \in active}
        IN CHOOSE m \in vals : \A v \in vals : m <= v

\* Exact transcription of Vec::swap_remove in StpBook::remove_order
\* (crates/tickwire-account/src/stp_book.rs:359-395).
SwapRemoveSeq(seq, idx) ==
    LET n == Len(seq) IN
    IF n = 1 \/ idx = n THEN SubSeq(seq, 1, n - 1)
    ELSE [i \in 1..(n - 1) |-> IF i = idx THEN seq[n] ELSE seq[i]]

UpsertBuy(keys, ticks_map, o, agg_tick) ==
    LET new_keys == IF o \in SeqElems(keys) THEN keys ELSE Append(keys, o)
        new_ticks == [ticks_map EXCEPT ![o] = agg_tick]
        new_best ==
            IF o \notin SeqElems(keys) THEN Max(best_bid, agg_tick)
            ELSE RecomputeBestBid(new_keys, new_ticks)
    IN <<new_keys, new_ticks, new_best>>

UpsertSell(keys, ticks_map, o, agg_tick) ==
    LET new_keys == IF o \in SeqElems(keys) THEN keys ELSE Append(keys, o)
        new_ticks == [ticks_map EXCEPT ![o] = agg_tick]
        new_best ==
            IF o \notin SeqElems(keys) THEN Min(best_ask, agg_tick)
            ELSE RecomputeBestAsk(new_keys, new_ticks)
    IN <<new_keys, new_ticks, new_best>>

RemoveFromStp(o) ==
    IF IsBuy(o) THEN
        IF o \notin SeqElems(buy_keys) THEN <<buy_keys, sell_keys, best_bid, best_ask>>
        ELSE
            LET idx == SeqIndex(buy_keys, o)
                new_keys == SwapRemoveSeq(buy_keys, idx)
            IN <<new_keys, sell_keys, RecomputeBestBid(new_keys, buy_ticks), best_ask>>
    ELSE
        IF o \notin SeqElems(sell_keys) THEN <<buy_keys, sell_keys, best_bid, best_ask>>
        ELSE
            LET idx == SeqIndex(sell_keys, o)
                new_keys == SwapRemoveSeq(sell_keys, idx)
            IN <<buy_keys, new_keys, best_bid, RecomputeBestAsk(new_keys, sell_ticks)>>

\* StpBook::self_cross & self_cross_excluding (crates/tickwire-account/src/stp_book.rs:189-239).
SelfCrossForSide(is_buy, cand_px) ==
    IF is_buy THEN best_ask # MAX_TICK /\ cand_px >= best_ask
    ELSE best_bid # MIN_TICK /\ cand_px <= best_bid

SelfCrossExcluding(o, cand_px) ==
    \* Opposite-side slots never contain `o`, so excluding `o` checks opposite atomic BBO.
    SelfCrossForSide(IsBuy(o), cand_px)

\* Reservation holds (crates/tickwire-account/src/calculation.rs:150-240,
\* crates/tickwire-account/src/calculation.rs:316-473,
\* and crates/tickwire-account/src/modifications.rs:240-385).
StressHigh == CHOOSE m \in AllPrices : \A p \in AllPrices : p <= m

MarginUnitPx(o, px) ==
    IF ~FuturesMarginMode THEN px
    ELSE IF IsBuy(o) THEN px
    ELSE Max(px, StressHigh)

RestingQuoteNotional(o) ==
    IF status[o] # "Open" THEN 0
    ELSE IF ~FuturesMarginMode /\ ~IsBuy(o) THEN 0
    ELSE PosPart(eff_qty[o] - filled[o]) * MarginUnitPx(o, price[o])

PendingQuoteNotional(o, req_px, req_qty) ==
    IF ~FuturesMarginMode /\ ~IsBuy(o) THEN 0
    ELSE PosPart(req_qty - filled[o]) * MarginUnitPx(o, req_px)

OrderQuoteHold(o) ==
    IF status[o] # "Open" THEN 0
    ELSE IF pm_active[o]
    THEN Max(RestingQuoteNotional(o), PendingQuoteNotional(o, pm_req_px[o], pm_req_qty[o]))
    ELSE RestingQuoteNotional(o)

OrderBaseHold(o) ==
    IF status[o] # "Open" \/ FuturesMarginMode \/ IsBuy(o) THEN 0
    ELSE IF pm_active[o]
    THEN Max(PosPart(eff_qty[o] - filled[o]), PosPart(pm_req_qty[o] - filled[o]))
    ELSE PosPart(eff_qty[o] - filled[o])

RECURSIVE SumOver(_, _)
SumOver(S, f) ==
    IF S = {} THEN 0
    ELSE LET x == CHOOSE y \in S : TRUE IN f[x] + SumOver(S \ {x}, f)

TotalQuoteHold ==
    SumOver(Orders, [o \in Orders |-> OrderQuoteHold(o)])

TotalBaseHold ==
    SumOver(Orders, [o \in Orders |-> OrderBaseHold(o)])

TotalShadowHold ==
    SumOver(Orders, shadow_hold)

AllowedPrices(o) == IF IsBuy(o) THEN BuyPrices ELSE SellPrices

MinPrice(S) == CHOOSE x \in S : \A y \in S : x <= y

ClampedPriceValid(o, req_px, app_px) ==
    IF IsBuy(o) THEN app_px <= req_px ELSE app_px >= req_px

Init ==
    /\ buy_keys = <<>>
    /\ buy_ticks = [o \in BuyOrders |-> MIN_TICK]
    /\ sell_keys = <<>>
    /\ sell_ticks = [o \in SellOrders |-> MAX_TICK]
    /\ best_bid = MIN_TICK
    /\ best_ask = MAX_TICK
    /\ status = [o \in Orders |-> "Idle"]
    /\ price = [o \in Orders |-> 1]
    /\ eff_qty = [o \in Orders |-> 1]
    /\ filled = [o \in Orders |-> 0]
    /\ cid_gen = [o \in Orders |-> 0]
    /\ cid_aliases = [o \in Orders |-> {}]
    /\ vid_gen = [o \in Orders |-> 0]
    /\ in_doubt = [o \in Orders |-> "None"]
    /\ pm_active = [o \in Orders |-> FALSE]
    /\ pm_sent = [o \in Orders |-> FALSE]
    /\ pm_req_px = [o \in Orders |-> 1]
    /\ pm_req_qty = [o \in Orders |-> 1]
    /\ pm_rot_cid = [o \in Orders |-> FALSE]
    /\ pm_tent_cid = [o \in Orders |-> 0]
    /\ shadow_hold = [o \in Orders |-> 0]
    /\ mod_seq = [o \in Orders |-> 0]
    /\ mod_superseded = [o \in Orders |-> FALSE]
    /\ mod_rejected = [o \in Orders |-> FALSE]
    /\ mod_aborted = [o \in Orders |-> FALSE]
    /\ last_rollback_kind = [o \in Orders |-> "None"]
    /\ pre_mod_px = [o \in Orders |-> 1]
    /\ pre_mod_qty = [o \in Orders |-> 1]
    /\ pre_mod_cid = [o \in Orders |-> 0]
    /\ query_phase = [o \in Orders |-> "Idle"]
    /\ quota_spent = 0
    /\ origins_active = {}
    /\ refused_local = 0

\* 1. Admit a new limit order via Account::admit + StpBook::upsert_order
\* (crates/tickwire-account/src/admission.rs:286-374, crates/tickwire-account/src/stp_book.rs:293-357).
AdmitNewOrder(o, px, q) ==
    LET add_quote == IF FuturesMarginMode \/ IsBuy(o) THEN q * MarginUnitPx(o, px) ELSE 0
        add_base == IF ~FuturesMarginMode /\ ~IsBuy(o) THEN q ELSE 0
    IN
    /\ status[o] = "Idle"
    /\ px \in AllowedPrices(o)
    /\ q \in Qtys
    /\ (o \notin ModifiableOrders => (px = MinPrice(AllowedPrices(o)) /\ q = 1))
    /\ ~(StpEnforced /\ SelfCrossForSide(IsBuy(o), px))
    /\ TotalQuoteHold + add_quote <= QuoteBalance
    /\ TotalBaseHold + add_base <= BaseBalance
    /\ quota_spent < MaxQuotaBound
    /\ IF IsBuy(o) THEN
           LET res == UpsertBuy(buy_keys, buy_ticks, o, px) IN
           /\ buy_keys' = res[1]
           /\ buy_ticks' = res[2]
           /\ best_bid' = res[3]
           /\ UNCHANGED <<sell_keys, sell_ticks, best_ask>>
       ELSE
           LET res == UpsertSell(sell_keys, sell_ticks, o, px) IN
           /\ sell_keys' = res[1]
           /\ sell_ticks' = res[2]
           /\ best_ask' = res[3]
           /\ UNCHANGED <<buy_keys, buy_ticks, best_bid>>
    /\ status' = [status EXCEPT ![o] = "Open"]
    /\ price' = [price EXCEPT ![o] = px]
    /\ eff_qty' = [eff_qty EXCEPT ![o] = q]
    /\ filled' = [filled EXCEPT ![o] = 0]
    /\ cid_gen' = [cid_gen EXCEPT ![o] = 1]
    /\ vid_gen' = [vid_gen EXCEPT ![o] = 1]
    /\ quota_spent' = quota_spent + 1
    /\ origins_active' = origins_active \cup {o}
    /\ UNCHANGED <<
        cid_aliases, in_doubt, pm_active, pm_sent, pm_req_px, pm_req_qty,
        pm_rot_cid, pm_tent_cid, shadow_hold, mod_seq, mod_superseded,
        mod_rejected, mod_aborted, last_rollback_kind,
        pre_mod_px, pre_mod_qty, pre_mod_cid, query_phase, refused_local
       >>

\* 2. Local CrossesBook or TooManyPendingActions refusal before wire/quota commit
\* (crates/tickwire-account/src/modifications.rs:133-192,
\*  crates/tickwire-binance-account/src/session.rs:1986-2124,
\*  crates/tickwire-spot-execution/src/session.rs:1103-1185).
RefuseLocalStpOrPending(o, cand_px) ==
    /\ refused_local = 0
    /\ cand_px \in AllowedPrices(o)
    /\ \/ (status[o] = "Idle" /\ StpEnforced /\ SelfCrossForSide(IsBuy(o), cand_px))
       \/ (status[o] = "Open" /\ o \in ModifiableOrders
           /\ ((StpEnforced /\ SelfCrossExcluding(o, cand_px)) \/ pm_active[o] \/ in_doubt[o] # "None"))
    /\ refused_local' = 1
    /\ UNCHANGED <<
        buy_keys, buy_ticks, sell_keys, sell_ticks, best_bid, best_ask,
        status, price, eff_qty, filled, cid_gen, cid_aliases, vid_gen, in_doubt,
        pm_active, pm_sent, pm_req_px, pm_req_qty, pm_rot_cid, pm_tent_cid, shadow_hold,
        mod_seq, mod_superseded, mod_rejected, mod_aborted, last_rollback_kind,
        pre_mod_px, pre_mod_qty, pre_mod_cid, query_phase,
        quota_spent, origins_active
       >>

\* 3. Admit single-in-flight Modify with dual-price StpBook & worst-case hold
\* (crates/tickwire-account/src/modifications.rs:67-421).
AdmitModify(o, new_px, new_q, rot_cid) ==
    LET old_q_hold == OrderQuoteHold(o)
        new_q_hold == Max(RestingQuoteNotional(o), PendingQuoteNotional(o, new_px, new_q))
        delta_q == PosPart(new_q_hold - old_q_hold)
        old_b_hold == OrderBaseHold(o)
        new_b_hold ==
            IF FuturesMarginMode \/ IsBuy(o) THEN 0
            ELSE Max(PosPart(eff_qty[o] - filled[o]), PosPart(new_q - filled[o]))
        delta_b == PosPart(new_b_hold - old_b_hold)
        agg == AggressiveTick(o, price[o], TRUE, new_px)
    IN
    /\ o \in ModifiableOrders
    /\ status[o] = "Open"
    /\ mod_seq[o] = 0
    /\ ~pm_active[o]
    /\ in_doubt[o] = "None"
    /\ new_px \in AllowedPrices(o)
    /\ new_q \in Qtys
    /\ new_q > filled[o]
    /\ (new_px # price[o] \/ new_q # eff_qty[o])
    /\ rot_cid \in BOOLEAN
    /\ (FuturesMarginMode => ~rot_cid)
    /\ ~(StpEnforced /\ SelfCrossExcluding(o, new_px))
    /\ TotalQuoteHold + delta_q <= QuoteBalance
    /\ TotalBaseHold + delta_b <= BaseBalance
    /\ IF IsBuy(o) THEN
           LET res == UpsertBuy(buy_keys, buy_ticks, o, agg) IN
           /\ buy_keys' = res[1]
           /\ buy_ticks' = res[2]
           /\ best_bid' = res[3]
           /\ UNCHANGED <<sell_keys, sell_ticks, best_ask>>
       ELSE
           LET res == UpsertSell(sell_keys, sell_ticks, o, agg) IN
           /\ sell_keys' = res[1]
           /\ sell_ticks' = res[2]
           /\ best_ask' = res[3]
           /\ UNCHANGED <<buy_keys, buy_ticks, best_bid>>
    /\ pm_active' = [pm_active EXCEPT ![o] = TRUE]
    /\ pm_sent' = [pm_sent EXCEPT ![o] = FALSE]
    /\ pm_req_px' = [pm_req_px EXCEPT ![o] = new_px]
    /\ pm_req_qty' = [pm_req_qty EXCEPT ![o] = new_q]
    /\ pm_rot_cid' = [pm_rot_cid EXCEPT ![o] = rot_cid]
    /\ pm_tent_cid' = [pm_tent_cid EXCEPT ![o] = IF rot_cid THEN cid_gen[o] + 1 ELSE 0]
    /\ shadow_hold' = [shadow_hold EXCEPT ![o] = IF FuturesMarginMode THEN delta_q ELSE 0]
    /\ mod_seq' = [mod_seq EXCEPT ![o] = 1]
    /\ last_rollback_kind' = [last_rollback_kind EXCEPT ![o] = "None"]
    /\ pre_mod_px' = [pre_mod_px EXCEPT ![o] = price[o]]
    /\ pre_mod_qty' = [pre_mod_qty EXCEPT ![o] = eff_qty[o]]
    /\ pre_mod_cid' = [pre_mod_cid EXCEPT ![o] = cid_gen[o]]
    /\ UNCHANGED <<
        status, price, eff_qty, filled, cid_gen, cid_aliases, vid_gen, in_doubt,
        mod_superseded, mod_rejected, mod_aborted, query_phase,
        quota_spent, origins_active, refused_local
       >>

\* 4. Zero-byte abort before wire send rolls back PendingModify without burning quota
\* (crates/tickwire-account/src/modifications.rs:459-483,
\*  crates/tickwire-account-gateway/src/lib.rs:1195-1212).
AbortModifyZeroBytes(o) ==
    /\ status[o] = "Open"
    /\ pm_active[o]
    /\ ~pm_sent[o]
    /\ in_doubt[o] = "None"
    /\ IF IsBuy(o) THEN
           LET res == UpsertBuy(buy_keys, buy_ticks, o, price[o]) IN
           /\ buy_keys' = res[1]
           /\ buy_ticks' = res[2]
           /\ best_bid' = res[3]
           /\ UNCHANGED <<sell_keys, sell_ticks, best_ask>>
       ELSE
           LET res == UpsertSell(sell_keys, sell_ticks, o, price[o]) IN
           /\ sell_keys' = res[1]
           /\ sell_ticks' = res[2]
           /\ best_ask' = res[3]
           /\ UNCHANGED <<buy_keys, buy_ticks, best_bid>>
    /\ pm_active' = [pm_active EXCEPT ![o] = FALSE]
    /\ pm_tent_cid' = [pm_tent_cid EXCEPT ![o] = 0]
    /\ shadow_hold' = [shadow_hold EXCEPT ![o] = 0]
    /\ mod_aborted' = [mod_aborted EXCEPT ![o] = TRUE]
    /\ last_rollback_kind' = [last_rollback_kind EXCEPT ![o] = "Aborted"]
    /\ UNCHANGED <<
        status, price, eff_qty, filled, cid_gen, cid_aliases, vid_gen, in_doubt,
        pm_sent, pm_req_px, pm_req_qty, pm_rot_cid, mod_seq, mod_superseded, mod_rejected,
        pre_mod_px, pre_mod_qty, pre_mod_cid, query_phase,
        quota_spent, origins_active, refused_local
       >>

\* 5. Flush Modify request onto transport slot and debit VenueRateLimiter quota.
FlushModifyToWire(o) ==
    /\ status[o] = "Open"
    /\ pm_active[o]
    /\ ~pm_sent[o]
    /\ quota_spent < MaxQuotaBound
    /\ pm_sent' = [pm_sent EXCEPT ![o] = TRUE]
    /\ quota_spent' = quota_spent + 1
    /\ UNCHANGED <<
        buy_keys, buy_ticks, sell_keys, sell_ticks, best_bid, best_ask,
        status, price, eff_qty, filled, cid_gen, cid_aliases, vid_gen, in_doubt,
        pm_active, pm_req_px, pm_req_qty, pm_rot_cid, pm_tent_cid, shadow_hold,
        mod_seq, mod_superseded, mod_rejected, mod_aborted, last_rollback_kind,
        pre_mod_px, pre_mod_qty, pre_mod_cid, query_phase,
        origins_active, refused_local
       >>

\* Helper for applying Modified (exact or venue-clamped terms).
ApplyModifiedTerms(o, app_px, app_q) ==
    LET before_cap == OrderQuoteHold(o)
        after_cap ==
            IF ~FuturesMarginMode /\ ~IsBuy(o) THEN 0
            ELSE PosPart(app_q - filled[o]) * MarginUnitPx(o, app_px)
        rel_shadow == PosPart(before_cap - after_cap)
        closes_now == (filled[o] = app_q)
    IN
    /\ IF closes_now THEN
           LET rem == RemoveFromStp(o) IN
           /\ buy_keys' = rem[1]
           /\ sell_keys' = rem[2]
           /\ best_bid' = rem[3]
           /\ best_ask' = rem[4]
           /\ UNCHANGED <<buy_ticks, sell_ticks>>
       ELSE IF IsBuy(o) THEN
           LET res == UpsertBuy(buy_keys, buy_ticks, o, app_px) IN
           /\ buy_keys' = res[1]
           /\ buy_ticks' = res[2]
           /\ best_bid' = res[3]
           /\ UNCHANGED <<sell_keys, sell_ticks, best_ask>>
       ELSE
           LET res == UpsertSell(sell_keys, sell_ticks, o, app_px) IN
           /\ sell_keys' = res[1]
           /\ sell_ticks' = res[2]
           /\ best_ask' = res[3]
           /\ UNCHANGED <<buy_keys, buy_ticks, best_bid>>
    /\ status' = [status EXCEPT ![o] = IF closes_now THEN "Terminal" ELSE "Open"]
    /\ origins_active' = IF closes_now THEN origins_active \ {o} ELSE origins_active
    /\ price' = [price EXCEPT ![o] = app_px]
    /\ eff_qty' = [eff_qty EXCEPT ![o] = app_q]
    /\ cid_aliases' = [cid_aliases EXCEPT ![o] =
        IF pm_rot_cid[o] THEN @ \cup {cid_gen[o]} ELSE @]
    /\ cid_gen' = [cid_gen EXCEPT ![o] =
        IF pm_rot_cid[o] THEN pm_tent_cid[o] ELSE @]
    /\ vid_gen' = [vid_gen EXCEPT ![o] = IF pm_rot_cid[o] THEN @ + 1 ELSE @]
    /\ pm_active' = [pm_active EXCEPT ![o] = FALSE]
    /\ pm_tent_cid' = [pm_tent_cid EXCEPT ![o] = 0]
    /\ shadow_hold' = [shadow_hold EXCEPT ![o] =
        IF closes_now \/ ~FuturesMarginMode THEN 0
        ELSE PosPart(@ - rel_shadow)]
    /\ in_doubt' = [in_doubt EXCEPT ![o] = "None"]
    /\ query_phase' = [query_phase EXCEPT ![o] = "Idle"]
    /\ last_rollback_kind' = [last_rollback_kind EXCEPT ![o] = "None"]

\* 6. Venue observes Modified (including clamped price/qty) on live sent request
\* (crates/tickwire-account/src/modifications.rs:485-819).
ObserveModified(o, app_px, app_q) ==
    /\ status[o] = "Open"
    /\ pm_active[o]
    /\ pm_sent[o]
    /\ in_doubt[o] = "None"
    /\ app_px \in AllowedPrices(o)
    /\ ClampedPriceValid(o, pm_req_px[o], app_px)
    /\ ~(StpEnforced /\ SelfCrossExcluding(o, app_px))
    /\ app_q \in Qtys
    /\ filled[o] <= app_q /\ app_q <= pm_req_qty[o]
    /\ ApplyModifiedTerms(o, app_px, app_q)
    /\ UNCHANGED <<
        filled, pm_sent, pm_req_px, pm_req_qty, pm_rot_cid,
        mod_seq, mod_superseded, mod_rejected, mod_aborted,
        pre_mod_px, pre_mod_qty, pre_mod_cid, quota_spent, refused_local
       >>

\* Helper for ModifyRejected rollback on an open order.
ApplyModifyRejectedRollback(o) ==
    /\ IF IsBuy(o) THEN
           LET res == UpsertBuy(buy_keys, buy_ticks, o, price[o]) IN
           /\ buy_keys' = res[1]
           /\ buy_ticks' = res[2]
           /\ best_bid' = res[3]
           /\ UNCHANGED <<sell_keys, sell_ticks, best_ask>>
       ELSE
           LET res == UpsertSell(sell_keys, sell_ticks, o, price[o]) IN
           /\ sell_keys' = res[1]
           /\ sell_ticks' = res[2]
           /\ best_ask' = res[3]
           /\ UNCHANGED <<buy_keys, buy_ticks, best_bid>>
    /\ pm_active' = [pm_active EXCEPT ![o] = FALSE]
    /\ pm_tent_cid' = [pm_tent_cid EXCEPT ![o] = 0]
    /\ shadow_hold' = [shadow_hold EXCEPT ![o] = 0]
    /\ in_doubt' = [in_doubt EXCEPT ![o] = "None"]
    /\ query_phase' = [query_phase EXCEPT ![o] = "Idle"]
    /\ mod_rejected' = [mod_rejected EXCEPT ![o] = TRUE]
    /\ last_rollback_kind' = [last_rollback_kind EXCEPT ![o] = "Rejected"]

\* 7. Venue rejects Modify (e.g., -5022 PostOnly / -2021) and restores resting state
\* (crates/tickwire-account/src/modifications.rs:821-886).
ObserveModifyRejected(o) ==
    /\ status[o] = "Open"
    /\ pm_active[o]
    /\ pm_sent[o]
    /\ in_doubt[o] = "None"
    /\ ApplyModifyRejectedRollback(o)
    /\ UNCHANGED <<
        status, price, eff_qty, filled, cid_gen, cid_aliases, vid_gen,
        pm_sent, pm_req_px, pm_req_qty, pm_rot_cid, mod_seq, mod_superseded, mod_aborted,
        pre_mod_px, pre_mod_qty, pre_mod_cid, quota_spent, origins_active, refused_local
       >>

\* 8. Partial fill arrives on open order (including while PendingModify is in flight)
\* (crates/tickwire-account/src/events.rs:562-695).
PartialFillOpenOrder(o) ==
    /\ status[o] = "Open"
    /\ filled[o] + 1 < eff_qty[o]
    /\ filled' = [filled EXCEPT ![o] = @ + 1]
    /\ UNCHANGED <<
        buy_keys, buy_ticks, sell_keys, sell_ticks, best_bid, best_ask,
        status, price, eff_qty, cid_gen, cid_aliases, vid_gen, in_doubt,
        pm_active, pm_sent, pm_req_px, pm_req_qty, pm_rot_cid, pm_tent_cid, shadow_hold,
        mod_seq, mod_superseded, mod_rejected, mod_aborted, last_rollback_kind,
        pre_mod_px, pre_mod_qty, pre_mod_cid, query_phase,
        quota_spent, origins_active, refused_local
       >>

\* 9. Full fill or Cancel closes order; swap_remove updates StpBook and supersedes any
\* in-flight PendingModify (crates/tickwire-account/src/events.rs:770-896,
\* crates/tickwire-account/src/modifications.rs:903-928).
TerminalCloseOrder(o) ==
    LET rem == RemoveFromStp(o)
        had_pm == pm_active[o]
    IN
    /\ status[o] = "Open"
    /\ (~had_pm \/ pm_sent[o])
    /\ buy_keys' = rem[1]
    /\ sell_keys' = rem[2]
    /\ best_bid' = rem[3]
    /\ best_ask' = rem[4]
    /\ status' = [status EXCEPT ![o] = "Terminal"]
    /\ filled' = [filled EXCEPT ![o] = eff_qty[o]]
    /\ in_doubt' = [in_doubt EXCEPT ![o] = "None"]
    /\ query_phase' = [query_phase EXCEPT ![o] = "Idle"]
    /\ pm_active' = [pm_active EXCEPT ![o] = FALSE]
    /\ pm_tent_cid' = [pm_tent_cid EXCEPT ![o] = 0]
    /\ shadow_hold' = [shadow_hold EXCEPT ![o] = 0]
    /\ mod_superseded' = [mod_superseded EXCEPT ![o] = IF had_pm THEN TRUE ELSE @]
    /\ last_rollback_kind' = [last_rollback_kind EXCEPT ![o] =
        IF had_pm THEN "Superseded" ELSE @]
    /\ origins_active' = origins_active \ {o}
    /\ UNCHANGED <<
        buy_ticks, sell_ticks, price, eff_qty, cid_gen, cid_aliases, vid_gen,
        pm_sent, pm_req_px, pm_req_qty, pm_rot_cid, mod_seq, mod_rejected, mod_aborted,
        pre_mod_px, pre_mod_qty, pre_mod_cid, quota_spent, refused_local
       >>

\* 10. Late ModifyRejected arrives after terminal close superseded PendingModify
\* (crates/tickwire-account/src/modifications.rs:821-886).
LateModifyRejectedOnSuperseded(o) ==
    /\ status[o] = "Terminal"
    /\ mod_superseded[o]
    /\ ~mod_rejected[o]
    /\ mod_superseded' = [mod_superseded EXCEPT ![o] = FALSE]
    /\ mod_rejected' = [mod_rejected EXCEPT ![o] = TRUE]
    /\ UNCHANGED <<
        buy_keys, buy_ticks, sell_keys, sell_ticks, best_bid, best_ask,
        status, price, eff_qty, filled, cid_gen, cid_aliases, vid_gen, in_doubt,
        pm_active, pm_sent, pm_req_px, pm_req_qty, pm_rot_cid, pm_tent_cid, shadow_hold,
        mod_seq, mod_aborted, last_rollback_kind,
        pre_mod_px, pre_mod_qty, pre_mod_cid, query_phase,
        quota_spent, origins_active, refused_local
       >>

\* 11. Transport disconnect after flush isolates order in AmendAmbiguous and schedules
\* QueryDesk status ask (crates/tickwire-account/src/query_desk.rs:271-368,
\* crates/tickwire-account/src/ors_query_desk.rs:165-229).
IsolateAmendAmbiguous(o) ==
    /\ status[o] = "Open"
    /\ pm_active[o]
    /\ pm_sent[o]
    /\ in_doubt[o] = "None"
    /\ in_doubt' = [in_doubt EXCEPT ![o] = "AmendAmbiguous"]
    /\ query_phase' = [query_phase EXCEPT ![o] =
        IF ~FuturesMarginMode /\ pm_rot_cid[o] THEN "ProbeNewCid" ELSE "ProbeOrig"]
    /\ UNCHANGED <<
        buy_keys, buy_ticks, sell_keys, sell_ticks, best_bid, best_ask,
        status, price, eff_qty, filled, cid_gen, cid_aliases, vid_gen,
        pm_active, pm_sent, pm_req_px, pm_req_qty, pm_rot_cid, pm_tent_cid, shadow_hold,
        mod_seq, mod_superseded, mod_rejected, mod_aborted, last_rollback_kind,
        pre_mod_px, pre_mod_qty, pre_mod_cid, quota_spent, origins_active, refused_local
       >>

\* 12. Spot QueryDesk / AccountSession -2013 on new_client_id falls back
\* to original orderId probe (crates/tickwire-account/src/query_desk.rs:647-760,
\* crates/tickwire-binance-account/src/session.rs:2885-2939).
StepSpotNewCidNotFoundFallback(o) ==
    /\ status[o] = "Open"
    /\ pm_active[o]
    /\ in_doubt[o] = "AmendAmbiguous"
    /\ query_phase[o] = "ProbeNewCid"
    /\ query_phase' = [query_phase EXCEPT ![o] = "ProbeOrig"]
    /\ UNCHANGED <<
        buy_keys, buy_ticks, sell_keys, sell_ticks, best_bid, best_ask,
        status, price, eff_qty, filled, cid_gen, cid_aliases, vid_gen, in_doubt,
        pm_active, pm_sent, pm_req_px, pm_req_qty, pm_rot_cid, pm_tent_cid, shadow_hold,
        mod_seq, mod_superseded, mod_rejected, mod_aborted, last_rollback_kind,
        pre_mod_px, pre_mod_qty, pre_mod_cid, quota_spent, origins_active, refused_local
       >>

\* 13. QueryDesk + AccountSession resolves AmendAmbiguous via authenticated order query
\* (crates/tickwire-account/src/query_desk.rs:762-868,
\*  crates/tickwire-binance-account/src/session.rs:2948-3205).
ResolveAmendAmbiguousViaQuery(o, applied) ==
    /\ status[o] = "Open"
    /\ pm_active[o]
    /\ in_doubt[o] = "AmendAmbiguous"
    /\ query_phase[o] \in {"ProbeNewCid", "ProbeOrig"}
    /\ applied \in BOOLEAN
    /\ (query_phase[o] = "ProbeNewCid" => applied)
    /\ (~FuturesMarginMode /\ pm_rot_cid[o] /\ query_phase[o] = "ProbeOrig" => ~applied)
    /\ IF applied THEN
           /\ filled[o] <= pm_req_qty[o]
           /\ ApplyModifiedTerms(o, pm_req_px[o], pm_req_qty[o])
           /\ UNCHANGED mod_rejected
       ELSE
           /\ ApplyModifyRejectedRollback(o)
           /\ UNCHANGED <<status, origins_active, price, eff_qty, cid_gen, cid_aliases, vid_gen>>
    /\ UNCHANGED <<
        filled, pm_sent, pm_req_px, pm_req_qty, pm_rot_cid,
        mod_seq, mod_superseded, mod_aborted,
        pre_mod_px, pre_mod_qty, pre_mod_cid, quota_spent, refused_local
       >>

Next ==
    \/ \E o \in Orders, px \in AllPrices, q \in Qtys : AdmitNewOrder(o, px, q)
    \/ \E o \in Orders, px \in AllPrices : RefuseLocalStpOrPending(o, px)
    \/ \E o \in ModifiableOrders, px \in AllPrices, q \in Qtys, rot \in BOOLEAN :
        AdmitModify(o, px, q, rot)
    \/ \E o \in ModifiableOrders : AbortModifyZeroBytes(o)
    \/ \E o \in ModifiableOrders : FlushModifyToWire(o)
    \/ \E o \in ModifiableOrders, px \in AllPrices, q \in Qtys : ObserveModified(o, px, q)
    \/ \E o \in ModifiableOrders : ObserveModifyRejected(o)
    \/ \E o \in Orders : PartialFillOpenOrder(o)
    \/ \E o \in Orders : TerminalCloseOrder(o)
    \/ \E o \in ModifiableOrders : LateModifyRejectedOnSuperseded(o)
    \/ \E o \in ModifiableOrders : IsolateAmendAmbiguous(o)
    \/ \E o \in ModifiableOrders : StepSpotNewCidNotFoundFallback(o)
    \/ \E o \in ModifiableOrders, applied \in BOOLEAN : ResolveAmendAmbiguousViaQuery(o, applied)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

MaxQtyVal == CHOOSE m \in Qtys : \A q \in Qtys : q <= m

TypeOK ==
    /\ Len(buy_keys) <= Cardinality(BuyOrders)
    /\ SeqElems(buy_keys) \subseteq BuyOrders
    /\ buy_ticks \in [BuyOrders -> MIN_TICK..MAX_TICK]
    /\ Len(sell_keys) <= Cardinality(SellOrders)
    /\ SeqElems(sell_keys) \subseteq SellOrders
    /\ sell_ticks \in [SellOrders -> MIN_TICK..MAX_TICK]
    /\ best_bid \in MIN_TICK..MAX_TICK
    /\ best_ask \in MIN_TICK..MAX_TICK
    /\ status \in [Orders -> Statuses]
    /\ price \in [Orders -> AllPrices \cup {1}]
    /\ eff_qty \in [Orders -> 1..MaxQtyVal]
    /\ filled \in [Orders -> 0..MaxQtyVal]
    /\ cid_gen \in [Orders -> 0..2]
    /\ cid_aliases \in [Orders -> SUBSET (1..2)]
    /\ vid_gen \in [Orders -> 0..2]
    /\ in_doubt \in [Orders -> DoubtStates]
    /\ pm_active \in [Orders -> BOOLEAN]
    /\ pm_sent \in [Orders -> BOOLEAN]
    /\ pm_req_px \in [Orders -> AllPrices \cup {1}]
    /\ pm_req_qty \in [Orders -> 1..MaxQtyVal]
    /\ pm_rot_cid \in [Orders -> BOOLEAN]
    /\ pm_tent_cid \in [Orders -> 0..2]
    /\ shadow_hold \in [Orders -> 0..QuoteBalance]
    /\ mod_seq \in [Orders -> 0..1]
    /\ mod_superseded \in [Orders -> BOOLEAN]
    /\ mod_rejected \in [Orders -> BOOLEAN]
    /\ mod_aborted \in [Orders -> BOOLEAN]
    /\ last_rollback_kind \in [Orders -> RollbackKinds]
    /\ pre_mod_px \in [Orders -> AllPrices \cup {1}]
    /\ pre_mod_qty \in [Orders -> 1..MaxQtyVal]
    /\ pre_mod_cid \in [Orders -> 0..2]
    /\ query_phase \in [Orders -> QueryPhases]
    /\ quota_spent \in 0..MaxQuotaBound
    /\ origins_active \subseteq Orders
    /\ refused_local \in 0..1

NoSelfTradeCrossWhenEnforced ==
    StpEnforced =>
        /\ (best_bid # MIN_TICK /\ best_ask # MAX_TICK) => best_bid < best_ask
        /\ \A b \in BuyOrders, s \in SellOrders :
            (status[b] = "Open" /\ status[s] = "Open") =>
                /\ price[b] < price[s]
                /\ CurAggressiveTick(b) < CurAggressiveTick(s)

AtomicBboExactness ==
    /\ SeqElems(buy_keys) = {b \in BuyOrders : status[b] = "Open"}
    /\ Cardinality(SeqElems(buy_keys)) = Len(buy_keys)
    /\ \A b \in SeqElems(buy_keys) : buy_ticks[b] = CurAggressiveTick(b)
    /\ best_bid = RecomputeBestBid(buy_keys, buy_ticks)
    /\ SeqElems(sell_keys) = {s \in SellOrders : status[s] = "Open"}
    /\ Cardinality(SeqElems(sell_keys)) = Len(sell_keys)
    /\ \A s \in SeqElems(sell_keys) : sell_ticks[s] = CurAggressiveTick(s)
    /\ best_ask = RecomputeBestAsk(sell_keys, sell_ticks)

AtMostOnePendingActionPerOrder ==
    \A o \in Orders :
        /\ (pm_tent_cid[o] # 0 => pm_active[o] /\ pm_rot_cid[o])
        /\ (in_doubt[o] = "AmendAmbiguous" => pm_active[o] /\ pm_sent[o])
        /\ (query_phase[o] # "Idle" <=> in_doubt[o] = "AmendAmbiguous")
        /\ ~(mod_aborted[o] /\ mod_rejected[o])
        /\ ~(mod_superseded[o] /\ pm_active[o])

WorstCaseModifyHoldSolvency ==
    /\ TotalQuoteHold <= QuoteBalance
    /\ TotalBaseHold <= BaseBalance
    /\ TotalShadowHold <= QuoteBalance
    /\ \A o \in Orders :
        status[o] = "Open" =>
            /\ OrderQuoteHold(o) >= RestingQuoteNotional(o)
            /\ (pm_active[o] =>
                /\ OrderQuoteHold(o) >= PendingQuoteNotional(o, pm_req_px[o], pm_req_qty[o])
                /\ \A app_px \in AllowedPrices(o), app_q \in Qtys :
                    (ClampedPriceValid(o, pm_req_px[o], app_px)
                     /\ filled[o] <= app_q /\ app_q <= pm_req_qty[o]) =>
                        OrderQuoteHold(o) >= PendingQuoteNotional(o, app_px, app_q))
            /\ (~FuturesMarginMode /\ ~IsBuy(o) /\ pm_active[o] =>
                OrderBaseHold(o) >= PosPart(pm_req_qty[o] - filled[o]))

ModifyRollbackRestoresExactRestingState ==
    \A o \in Orders :
        /\ last_rollback_kind[o] \in {"Aborted", "Rejected"} =>
            /\ ~pm_active[o]
            /\ pm_tent_cid[o] = 0
            /\ shadow_hold[o] = 0
            /\ in_doubt[o] = "None"
            /\ cid_gen[o] = pre_mod_cid[o]
            /\ (status[o] = "Open" =>
                /\ price[o] = pre_mod_px[o]
                /\ eff_qty[o] = pre_mod_qty[o])
        /\ last_rollback_kind[o] = "Superseded" =>
            /\ status[o] = "Terminal"
            /\ ~pm_active[o]
            /\ pm_tent_cid[o] = 0
            /\ shadow_hold[o] = 0
        /\ (status[o] = "Idle" => o \notin origins_active)

TerminalOrderClearsStpAndPendingModify ==
    \A o \in Orders :
        status[o] # "Open" =>
            /\ o \notin SeqElems(buy_keys)
            /\ o \notin SeqElems(sell_keys)
            /\ ~pm_active[o]
            /\ pm_tent_cid[o] = 0
            /\ shadow_hold[o] = 0
            /\ in_doubt[o] = "None"
            /\ query_phase[o] = "Idle"
            /\ OrderQuoteHold(o) = 0
            /\ OrderBaseHold(o) = 0
            /\ o \notin origins_active

=============================================================================
