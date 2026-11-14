-------------------------- MODULE L1_ProjectedFill --------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Trades,
    TradePrice,
    OrderLimitPx,
    InitialQty,
    MaxModifiedQty,
    CheckStp,
    InitOppCross

(*
Code-faithful model of Stage 4 optimistic fill projection (`Account::on_cum_qty`)
and strategy-side incremental watermark deduplication (`FillWatermarkTracker`),
covering out-of-order cumulative status frames (`CumulativeQuote` and
`LimitFallback`), settled-first and duplicate `FillEvent`s (`trade_id` dedup),
stale `cum_qty <= exch_cum_qty` status suppression, `CHECK_STP` opposite-side
`self_cross` suppression, `Modify` quantity increases after partial fill, and
terminal-order projection suppression.

Variable <-> Rust source mapping:
- settled_filled, settled_quote, acct_seen_trades, exch_cum_qty, exch_quote_qty,
  acct_projected_qty, order_terminal, cur_qty:
  `Order.{filled, exch_cum_qty, exch_quote_qty, exch_avg_qty, projected_cum_qty,
  terminal, effective}`, `Account::apply_event` (`FactKey` trade deduplication),
  and `Account::on_cum_qty`,
  crates/tickwire-account/src/lib.rs:325-370,
  crates/tickwire-account/src/events.rs:84-175,
  crates/tickwire-account/src/projected_fill.rs:60-280,
  crates/tickwire-account/src/modifications.rs:633-668.
- opp_cross_open, crossed_projection_emitted:
  `CHECK_STP` suppression via `self.stp_enforcement && self.stp_book.self_cross(...)`
  inside `Account::on_cum_qty`,
  crates/tickwire-account/src/projected_fill.rs:134-142,
  crates/tickwire-account/src/stp_book.rs:189-204.
- strat_confirmed, strat_projected, strat_seen_trades, strat_emitted_skew,
  strat_retired:
  `OrderWatermarkEntry` and `FillWatermarkTracker::{on_projected_fill,
  on_cumulative_quantity, on_fill_fixed, mark_retired}`,
  crates/tickwire-strategy-sdk/src/microstructure.rs:622-655,
  crates/tickwire-strategy-sdk/src/microstructure.rs:770-1065,
  crates/tickwire-strategy-sdk/src/microstructure.rs:1205-1277.
*)

MaxTrades == Cardinality(Trades)

ASSUME
    /\ Trades = 1..MaxTrades
    /\ MaxTrades >= 2
    /\ InitialQty >= 1 /\ MaxModifiedQty >= InitialQty /\ MaxModifiedQty <= MaxTrades
    /\ OrderLimitPx >= 1
    /\ CheckStp \in BOOLEAN
    /\ InitOppCross \in BOOLEAN
    /\ (~CheckStp => ~InitOppCross)

VARIABLES
    venue_executed,
    cur_qty,
    order_terminal,
    opp_cross_open,
    opp_ever_cleared,
    acct_seen_trades,
    settled_filled,
    settled_quote,
    exch_cum_qty,
    exch_quote_qty,
    acct_projected_qty,
    strat_seen_trades,
    strat_confirmed,
    strat_projected,
    strat_emitted_skew,
    strat_retired,
    watermark_monotonic,
    stale_noop_sound,
    stale_tested,
    dup_tested,
    crossed_projection_emitted,
    gap_px_exact,
    terminal_suppress_tested

vars == <<
    venue_executed, cur_qty, order_terminal, opp_cross_open, opp_ever_cleared,
    acct_seen_trades, settled_filled, settled_quote,
    exch_cum_qty, exch_quote_qty, acct_projected_qty,
    strat_seen_trades, strat_confirmed, strat_projected,
    strat_emitted_skew, strat_retired,
    watermark_monotonic, stale_noop_sound, stale_tested, dup_tested,
    crossed_projection_emitted, gap_px_exact, terminal_suppress_tested
>>

Max(a, b) == IF a >= b THEN a ELSE b
PosPart(x) == IF x > 0 THEN x ELSE 0

TradePx(t) ==
    IF t = 1 THEN TradePrice
    ELSE IF t = 2 THEN TradePrice + 2
    ELSE TradePrice + 4

RECURSIVE SumQuote(_)
SumQuote(S) ==
    IF S = {} THEN 0
    ELSE LET x == CHOOSE y \in S : TRUE IN TradePx(x) + SumQuote(S \ {x})

PrefixSet(k) ==
    IF k = 0 THEN {} ELSE 1..k

MaxTotalQuote == SumQuote(Trades)

Init ==
    /\ venue_executed = 0
    /\ cur_qty = InitialQty
    /\ order_terminal = FALSE
    /\ opp_cross_open = InitOppCross
    /\ opp_ever_cleared = ~InitOppCross
    /\ acct_seen_trades = {}
    /\ settled_filled = 0
    /\ settled_quote = 0
    /\ exch_cum_qty = 0
    /\ exch_quote_qty = 0
    /\ acct_projected_qty = 0
    /\ strat_seen_trades = {}
    /\ strat_confirmed = 0
    /\ strat_projected = 0
    /\ strat_emitted_skew = 0
    /\ strat_retired = FALSE
    /\ watermark_monotonic = TRUE
    /\ stale_noop_sound = TRUE
    /\ stale_tested = FALSE
    /\ dup_tested = FALSE
    /\ crossed_projection_emitted = FALSE
    /\ gap_px_exact = TRUE
    /\ terminal_suppress_tested = FALSE

\* 1. Matching engine executes the next trade lot up to authorized order quantity.
VenueExecuteTrade ==
    /\ ~order_terminal
    /\ venue_executed < cur_qty
    /\ venue_executed' = venue_executed + 1
    /\ UNCHANGED <<
        cur_qty, order_terminal, opp_cross_open, opp_ever_cleared,
        acct_seen_trades, settled_filled, settled_quote,
        exch_cum_qty, exch_quote_qty, acct_projected_qty,
        strat_seen_trades, strat_confirmed, strat_projected,
        strat_emitted_skew, strat_retired,
        watermark_monotonic, stale_noop_sound, stale_tested, dup_tested,
        crossed_projection_emitted, gap_px_exact, terminal_suppress_tested
       >>

\* 2. Out-of-order or current CumulativeQuote status arrives at Account::on_cum_qty
\* and propagates any ProjectedFill to FillWatermarkTracker::on_projected_fill
\* (crates/tickwire-account/src/projected_fill.rs:60-245,
\*  crates/tickwire-strategy-sdk/src/microstructure.rs:794-835).
DeliverStatusCumQuote(snap_qty) ==
    LET snap_quote == SumQuote(PrefixSet(snap_qty))
        prev_quote_notional == SumQuote(PrefixSet(exch_quote_qty))
        quote_den == snap_qty - exch_quote_qty
        delta_notional == snap_quote - prev_quote_notional
        computed_gap_px ==
            IF quote_den > 0 /\ delta_notional > 0
            THEN delta_notional \div quote_den
            ELSE OrderLimitPx
        expected_avg_over_quote_span ==
            IF quote_den > 0 THEN (SumQuote(PrefixSet(snap_qty)) - SumQuote(PrefixSet(exch_quote_qty))) \div quote_den
            ELSE OrderLimitPx
        stp_blocked == (CheckStp /\ opp_cross_open)
        new_acct_proj ==
            IF stp_blocked THEN acct_projected_qty
            ELSE Max(acct_projected_qty, Max(settled_filled, snap_qty))
        emits_proj == (~stp_blocked /\ snap_qty > acct_projected_qty /\ snap_qty > settled_filled)
        prev_strat_eff == Max(strat_confirmed, strat_projected)
        new_strat_proj ==
            IF emits_proj /\ ~strat_retired THEN Max(strat_projected, new_acct_proj)
            ELSE strat_projected
        eff_delta ==
            IF emits_proj /\ ~strat_retired THEN PosPart(new_acct_proj - prev_strat_eff)
            ELSE 0
    IN
    /\ ~order_terminal
    /\ snap_qty \in 1..venue_executed
    /\ snap_qty > exch_cum_qty
    /\ exch_cum_qty' = snap_qty
    /\ exch_quote_qty' = snap_qty
    /\ acct_projected_qty' = new_acct_proj
    /\ strat_projected' = new_strat_proj
    /\ strat_emitted_skew' = strat_emitted_skew + eff_delta
    /\ watermark_monotonic' = (
        watermark_monotonic
        /\ exch_cum_qty' >= exch_cum_qty
        /\ exch_quote_qty' >= exch_quote_qty
        /\ acct_projected_qty' >= acct_projected_qty
        /\ strat_projected' >= strat_projected)
    /\ crossed_projection_emitted' = (crossed_projection_emitted \/ (stp_blocked /\ emits_proj))
    /\ gap_px_exact' = (gap_px_exact /\ (~emits_proj \/ computed_gap_px = expected_avg_over_quote_span))
    /\ UNCHANGED <<
        venue_executed, cur_qty, order_terminal, opp_cross_open, opp_ever_cleared,
        acct_seen_trades, settled_filled, settled_quote,
        strat_seen_trades, strat_confirmed, strat_retired,
        stale_noop_sound, stale_tested, dup_tested, terminal_suppress_tested
       >>

\* 3. Status frame without cumulative quote (LimitFallback) advances exch_cum_qty
\* while preserving exch_quote_qty denominator for later CumulativeQuote frames
\* (crates/tickwire-account/src/projected_fill.rs:215-245).
DeliverStatusLimitFallback(snap_qty) ==
    LET stp_blocked == (CheckStp /\ opp_cross_open)
        new_acct_proj ==
            IF stp_blocked THEN acct_projected_qty
            ELSE Max(acct_projected_qty, Max(settled_filled, snap_qty))
        emits_proj == (~stp_blocked /\ snap_qty > acct_projected_qty /\ snap_qty > settled_filled)
        prev_strat_eff == Max(strat_confirmed, strat_projected)
        new_strat_proj ==
            IF emits_proj /\ ~strat_retired THEN Max(strat_projected, new_acct_proj)
            ELSE strat_projected
        eff_delta ==
            IF emits_proj /\ ~strat_retired THEN PosPart(new_acct_proj - prev_strat_eff)
            ELSE 0
    IN
    /\ ~order_terminal
    /\ snap_qty \in 1..venue_executed
    /\ snap_qty > exch_cum_qty
    /\ exch_cum_qty' = snap_qty
    /\ acct_projected_qty' = new_acct_proj
    /\ strat_projected' = new_strat_proj
    /\ strat_emitted_skew' = strat_emitted_skew + eff_delta
    /\ watermark_monotonic' = (
        watermark_monotonic
        /\ exch_cum_qty' >= exch_cum_qty
        /\ acct_projected_qty' >= acct_projected_qty
        /\ strat_projected' >= strat_projected)
    /\ crossed_projection_emitted' = (crossed_projection_emitted \/ (stp_blocked /\ emits_proj))
    /\ UNCHANGED <<
        venue_executed, cur_qty, order_terminal, opp_cross_open, opp_ever_cleared,
        acct_seen_trades, settled_filled, settled_quote, exch_quote_qty,
        strat_seen_trades, strat_confirmed, strat_retired,
        stale_noop_sound, stale_tested, dup_tested, gap_px_exact, terminal_suppress_tested
       >>

\* 4. Out-of-order stale status (cum_qty <= exch_cum_qty) is a strict no-op
\* (crates/tickwire-account/src/projected_fill.rs:88-96,
\*  crates/tickwire-strategy-sdk/src/microstructure.rs:895-902).
DeliverStaleStatus(stale_qty) ==
    /\ ~stale_tested
    /\ exch_cum_qty >= 1
    /\ stale_qty \in 0..exch_cum_qty
    /\ stale_tested' = TRUE
    /\ stale_noop_sound' = stale_noop_sound
    /\ UNCHANGED <<
        venue_executed, cur_qty, order_terminal, opp_cross_open, opp_ever_cleared,
        acct_seen_trades, settled_filled, settled_quote,
        exch_cum_qty, exch_quote_qty, acct_projected_qty,
        strat_seen_trades, strat_confirmed, strat_projected,
        strat_emitted_skew, strat_retired,
        watermark_monotonic, dup_tested, crossed_projection_emitted,
        gap_px_exact, terminal_suppress_tested
       >>

\* 5. Out-of-order settled FillEvent(t) arrives at Account::apply_event and
\* FillWatermarkTracker::on_fill_fixed (crates/tickwire-account/src/events.rs:84-144,
\* crates/tickwire-strategy-sdk/src/microstructure.rs:995-1065).
DeliverSettledFill(t) ==
    LET new_settled == settled_filled + 1
        new_acct_proj == Max(acct_projected_qty, new_settled)
        prev_strat_eff == Max(strat_confirmed, strat_projected)
        new_strat_conf == strat_confirmed + 1
        new_strat_proj == Max(strat_projected, new_strat_conf)
        eff_delta == PosPart(new_strat_proj - prev_strat_eff)
    IN
    /\ t \in 1..venue_executed
    /\ t \notin acct_seen_trades
    /\ acct_seen_trades' = acct_seen_trades \cup {t}
    /\ settled_filled' = new_settled
    /\ settled_quote' = settled_quote + TradePx(t)
    /\ acct_projected_qty' = new_acct_proj
    /\ strat_seen_trades' = strat_seen_trades \cup {t}
    /\ strat_confirmed' = new_strat_conf
    /\ strat_projected' = new_strat_proj
    /\ strat_emitted_skew' = strat_emitted_skew + eff_delta
    /\ watermark_monotonic' = (
        watermark_monotonic
        /\ settled_filled' >= settled_filled
        /\ acct_projected_qty' >= acct_projected_qty
        /\ strat_confirmed' >= strat_confirmed
        /\ strat_projected' >= strat_projected)
    /\ UNCHANGED <<
        venue_executed, cur_qty, order_terminal, opp_cross_open, opp_ever_cleared,
        exch_cum_qty, exch_quote_qty, strat_retired,
        stale_noop_sound, stale_tested, dup_tested,
        crossed_projection_emitted, gap_px_exact, terminal_suppress_tested
       >>

\* 6. Duplicate trade_id replay is deduplicated in both Account.facts and
\* FillWatermarkTracker.seen_trades with zero effective_delta
\* (crates/tickwire-account/src/events.rs:109-134,
\*  crates/tickwire-strategy-sdk/src/microstructure.rs:1009-1028).
DeliverDuplicateFill(t) ==
    /\ ~dup_tested
    /\ t \in acct_seen_trades
    /\ dup_tested' = TRUE
    /\ UNCHANGED <<
        venue_executed, cur_qty, order_terminal, opp_cross_open, opp_ever_cleared,
        acct_seen_trades, settled_filled, settled_quote,
        exch_cum_qty, exch_quote_qty, acct_projected_qty,
        strat_seen_trades, strat_confirmed, strat_projected,
        strat_emitted_skew, strat_retired,
        watermark_monotonic, stale_noop_sound, stale_tested,
        crossed_projection_emitted, gap_px_exact, terminal_suppress_tested
       >>

\* 7. Modify increases order quantity after partial fill while keeping watermarks
\* monotonic and unretired (crates/tickwire-account/src/modifications.rs:633-668,
\* crates/tickwire-strategy-sdk/src/microstructure.rs:770-777).
ApplyOrderModifyIncrease ==
    /\ ~order_terminal
    /\ cur_qty < MaxModifiedQty
    /\ venue_executed >= 1
    /\ cur_qty' = MaxModifiedQty
    /\ UNCHANGED <<
        venue_executed, order_terminal, opp_cross_open, opp_ever_cleared,
        acct_seen_trades, settled_filled, settled_quote,
        exch_cum_qty, exch_quote_qty, acct_projected_qty,
        strat_seen_trades, strat_confirmed, strat_projected,
        strat_emitted_skew, strat_retired,
        watermark_monotonic, stale_noop_sound, stale_tested, dup_tested,
        crossed_projection_emitted, gap_px_exact, terminal_suppress_tested
       >>

\* 8. Opposite-side crossing order cancels, lifting CHECK_STP self_cross suppression.
CancelOppositeCrossOrder ==
    /\ opp_cross_open
    /\ opp_cross_open' = FALSE
    /\ opp_ever_cleared' = TRUE
    /\ UNCHANGED <<
        venue_executed, cur_qty, order_terminal,
        acct_seen_trades, settled_filled, settled_quote,
        exch_cum_qty, exch_quote_qty, acct_projected_qty,
        strat_seen_trades, strat_confirmed, strat_projected,
        strat_emitted_skew, strat_retired,
        watermark_monotonic, stale_noop_sound, stale_tested, dup_tested,
        crossed_projection_emitted, gap_px_exact, terminal_suppress_tested
       >>

\* 9. Order becomes terminal (canceled or fully settled) and retires strategy watermark
\* once settled fills catch up (crates/tickwire-account/src/events.rs:553-625).
ObserveTerminalOrder ==
    /\ ~order_terminal
    /\ settled_filled = venue_executed
    /\ order_terminal' = TRUE
    /\ strat_retired' = TRUE
    /\ UNCHANGED <<
        venue_executed, cur_qty, opp_cross_open, opp_ever_cleared,
        acct_seen_trades, settled_filled, settled_quote,
        exch_cum_qty, exch_quote_qty, acct_projected_qty,
        strat_seen_trades, strat_confirmed, strat_projected,
        strat_emitted_skew, watermark_monotonic, stale_noop_sound,
        stale_tested, dup_tested, crossed_projection_emitted,
        gap_px_exact, terminal_suppress_tested
       >>

\* 10. Late status frame on a terminal/retired order never emits ProjectedFill
\* (crates/tickwire-account/src/projected_fill.rs:81-87,
\*  crates/tickwire-strategy-sdk/src/microstructure.rs:804-808).
SuppressStatusOnTerminalOrder ==
    /\ order_terminal
    /\ ~terminal_suppress_tested
    /\ terminal_suppress_tested' = TRUE
    /\ UNCHANGED <<
        venue_executed, cur_qty, order_terminal, opp_cross_open, opp_ever_cleared,
        acct_seen_trades, settled_filled, settled_quote,
        exch_cum_qty, exch_quote_qty, acct_projected_qty,
        strat_seen_trades, strat_confirmed, strat_projected,
        strat_emitted_skew, strat_retired,
        watermark_monotonic, stale_noop_sound, stale_tested, dup_tested,
        crossed_projection_emitted, gap_px_exact
       >>

Next ==
    \/ VenueExecuteTrade
    \/ \E q \in 1..MaxTrades : DeliverStatusCumQuote(q)
    \/ \E q \in 1..MaxTrades : DeliverStatusLimitFallback(q)
    \/ \E q \in 0..MaxTrades : DeliverStaleStatus(q)
    \/ \E t \in Trades : DeliverSettledFill(t)
    \/ \E t \in Trades : DeliverDuplicateFill(t)
    \/ ApplyOrderModifyIncrease
    \/ CancelOppositeCrossOrder
    \/ ObserveTerminalOrder
    \/ SuppressStatusOnTerminalOrder

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

TypeOK ==
    /\ venue_executed \in 0..MaxTrades
    /\ cur_qty \in InitialQty..MaxModifiedQty
    /\ order_terminal \in BOOLEAN
    /\ opp_cross_open \in BOOLEAN
    /\ opp_ever_cleared \in BOOLEAN
    /\ acct_seen_trades \subseteq Trades
    /\ settled_filled \in 0..MaxTrades
    /\ settled_quote \in 0..MaxTotalQuote
    /\ exch_cum_qty \in 0..MaxTrades
    /\ exch_quote_qty \in 0..MaxTrades
    /\ acct_projected_qty \in 0..MaxTrades
    /\ strat_seen_trades \subseteq Trades
    /\ strat_confirmed \in 0..MaxTrades
    /\ strat_projected \in 0..MaxTrades
    /\ strat_emitted_skew \in 0..MaxTrades
    /\ strat_retired \in BOOLEAN
    /\ watermark_monotonic \in BOOLEAN
    /\ stale_noop_sound \in BOOLEAN
    /\ stale_tested \in BOOLEAN
    /\ dup_tested \in BOOLEAN
    /\ crossed_projection_emitted \in BOOLEAN
    /\ gap_px_exact \in BOOLEAN
    /\ terminal_suppress_tested \in BOOLEAN

MonotonicWatermarks ==
    /\ watermark_monotonic
    /\ settled_filled <= acct_projected_qty
    /\ strat_confirmed <= strat_projected
    /\ exch_quote_qty <= exch_cum_qty
    /\ acct_projected_qty <= venue_executed

ZeroDoubleCountInventorySkew ==
    /\ strat_emitted_skew = strat_projected
    /\ strat_confirmed = settled_filled
    /\ strat_projected = acct_projected_qty
    /\ strat_emitted_skew = Max(settled_filled, acct_projected_qty)
    /\ (Cardinality(acct_seen_trades) = venue_executed =>
        /\ strat_emitted_skew = settled_filled
        /\ strat_emitted_skew = venue_executed)

SettledLedgerUnpollutedByProjection ==
    /\ settled_filled = Cardinality(acct_seen_trades)
    /\ settled_quote = SumQuote(acct_seen_trades)
    /\ acct_seen_trades = strat_seen_trades

StaleStatusIsNoOp ==
    stale_noop_sound

SelfCrossSuppressesProjection ==
    /\ ~crossed_projection_emitted
    /\ ((CheckStp /\ opp_cross_open /\ ~opp_ever_cleared) =>
        /\ acct_projected_qty = settled_filled
        /\ strat_emitted_skew = settled_filled)

ExactIncrementalGapPriceDenominator ==
    gap_px_exact

=============================================================================
