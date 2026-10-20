--------------------------- MODULE L0_SpotFacts_v3 ---------------------------
EXTENDS Integers, FiniteSets, TLC
CONSTANTS Orders, InitialBase, InitialQuote, FeeValues
ASSUME /\ Orders = 1..Cardinality(Orders) /\ Orders # {}
       /\ InitialBase >= 2 /\ InitialQuote >= 4 /\ FeeValues \subseteq 0..4
\* var <-> code map:
\* orders: Account.orders/admissions, crates/tickwire-account/src/lib.rs:87-105
\* filled/trades/fees/paid/economic: checked fact application and dedup.
\*   crates/tickwire-account/src/events.rs:19-112
\* totalBase/totalQuote/baseOwner/quoteOwner: cashflow + original attribution.
\*   crates/tickwire-account/src/events.rs:153-287
\* terminal/cum/unknown/closed/aborted: Order lifecycle and missing-detail hold.
\*   crates/tickwire-account/src/lib.rs:56-69
\*   crates/tickwire-account/src/events.rs:352-414
\* frozen: conflict/overflow/deficit gate, not rejection of actual economics.
\*   crates/tickwire-account/src/events.rs:3-17
\*   crates/tickwire-account/src/events.rs:90-112
\* admission's all-asset candidate: crates/tickwire-account/src/admission.rs:4-31
\*   crates/tickwire-account/src/admission.rs:271-337
\* reserve formula: crates/tickwire-account/src/calculation.rs:134-259
\* abort/reconcile: crates/tickwire-account/src/admission.rs:411-451
\* Valid finite instance: qty2, buy cap2, fee rate1/2, maximum_fills2.
\* No concurrent writers, no WAL fault model; upstream durable-fact obligation.
VARIABLE s
vars == <<s>>
Max(a,b) == IF a > b THEN a ELSE b
CeilHalf(n) == (n + 1) \div 2
RECURSIVE Sum(_)
Sum(f) == IF DOMAIN f = {} THEN 0 ELSE LET i == CHOOSE x \in DOMAIN f: TRUE IN f[i] + Sum([j \in DOMAIN f \ {i} |-> f[j]])
Remaining(t,o) == IF o \notin t.orders \/ t.aborted[o] \/ (t.terminal[o] /\ t.cum[o] = t.filled[o])
                  THEN 0 ELSE 2 - t.filled[o]
BaseReserve(t,o) == IF o \notin t.orders \/ t.aborted[o] \/ t.closed[o]
                    THEN 0 ELSE Max(0,CeilHalf(t.filled[o]) + 2 - t.paid[o])
QuoteReserve(t,o) == 2 * Remaining(t,o)
FreeBase(t) == t.totalBase - Sum([o \in Orders |-> BaseReserve(t,o)])
FreeQuote(t) == t.totalQuote - Sum([o \in Orders |-> QuoteReserve(t,o)])
FreezeDeficit(t) == [t EXCEPT !.frozen = @ \/ FreeBase(t) < 0 \/ FreeQuote(t) < 0]
Init == s = [orders |-> {}, filled |-> [o \in Orders |-> 0],
             trades |-> [o \in Orders |-> [k \in 1..2 |-> 0]],
             fees |-> [o \in Orders |-> -1], paid |-> [o \in Orders |-> 0],
             economic |-> [o \in Orders |-> FALSE], confirmed |-> [o \in Orders |-> FALSE],
             totalBase |-> InitialBase, totalQuote |-> InitialQuote,
             baseOwner |-> [o \in Orders |-> 0], quoteOwner |-> [o \in Orders |-> 0],
             terminal |-> [o \in Orders |-> FALSE], cum |-> [o \in Orders |-> -1],
             unknown |-> [o \in Orders |-> FALSE], closed |-> [o \in Orders |-> FALSE],
             aborted |-> [o \in Orders |-> FALSE], frozen |-> FALSE]

\* crates/tickwire-account/src/admission.rs:4-31; crates/tickwire-account/src/admission.rs:271-337
Admit(o) == /\ o \notin s.orders /\ ~s.frozen
            /\ LET t == [s EXCEPT !.orders = @ \cup {o}] IN
               /\ FreeBase(t) >= 0 /\ FreeQuote(t) >= 0 /\ s' = t
RejectAdmission(o) == /\ o \notin s.orders
                      /\ (s.frozen \/ FreeBase(s) < 2 \/ FreeQuote(s) < 4)
                      /\ UNCHANGED s
RetryAdmission(o) == /\ o \in s.orders /\ UNCHANGED s
ConflictAdmission(o) == /\ o \in s.orders /\ s' = [s EXCEPT !.frozen = TRUE]
\* crates/tickwire-account/src/events.rs:153-270; crates/tickwire-account/src/events.rs:137-151
Fill(o,k,cost) ==
    /\ o \in s.orders /\ k \in 1..2 /\ s.trades[o][k] = 0 /\ cost \in 1..2
    /\ LET t == [s EXCEPT !.trades[o][k] = cost, !.filled[o] = @ + 1,
                         !.totalBase = @ + 1, !.totalQuote = @ - cost,
                         !.baseOwner[o] = @ + 1, !.quoteOwner[o] = @ - cost,
                         !.economic[o] = TRUE, !.confirmed[o] = TRUE, !.aborted[o] = FALSE, !.closed[o] = FALSE,
                         !.frozen = @ \/ s.aborted[o] \/ s.closed[o]
                                  \/ (s.cum[o] # -1 /\ s.filled[o] + 1 > s.cum[o])]
       IN s' = FreezeDeficit(t)
ReplayFill(o,k) == /\ o \in s.orders /\ s.trades[o][k] # 0 /\ UNCHANGED s
ConflictFill(o,k) == /\ o \in s.orders /\ s.trades[o][k] # 0
                     /\ s' = [s EXCEPT !.frozen = TRUE]
\* crates/tickwire-account/src/events.rs:289-343; zero fees are economic evidence.
Fee(o,n) ==
    /\ o \in s.orders /\ s.fees[o] = -1 /\ n \in FeeValues
    /\ LET t == [s EXCEPT !.fees[o] = n, !.paid[o] = n,
                         !.totalBase = @ - n, !.baseOwner[o] = @ - n,
                         !.economic[o] = TRUE, !.aborted[o] = FALSE, !.closed[o] = FALSE,
                         !.frozen = @ \/ s.aborted[o] \/ s.closed[o]
                                    \/ n > CeilHalf(s.filled[o]) + 2]
       IN s' = FreezeDeficit(t)
ReplayFee(o) == /\ o \in s.orders /\ s.fees[o] # -1 /\ UNCHANGED s
ConflictFee(o) == /\ o \in s.orders /\ s.fees[o] # -1 /\ s' = [s EXCEPT !.frozen = TRUE]
\* A nonterminal unknown observation. crates/tickwire-account/src/events.rs:352-414
Unknown(o) == /\ o \in s.orders /\ ~s.unknown[o]
              /\ s' = [s EXCEPT !.unknown[o] = TRUE, !.frozen = TRUE]
\* Same terminal may be reported again; conflicting cumulative freezes, max retained.
Acknowledge(o) == /\ o \in s.orders
                  /\ s' = [s EXCEPT !.confirmed[o] = TRUE, !.aborted[o] = FALSE,
                                    !.closed[o] = IF s.aborted[o] THEN FALSE ELSE @,
                                    !.frozen = @ \/ s.aborted[o]]
Terminal(o,n) ==
    /\ o \in s.orders /\ n \in 0..2
    /\ s' = [s EXCEPT !.terminal[o] = TRUE, !.confirmed[o] = TRUE, !.cum[o] = Max(@,n),
                      !.aborted[o] = FALSE, !.closed[o] = IF s.aborted[o] THEN FALSE ELSE @,
                      !.frozen = @ \/ s.aborted[o] \/ n < s.filled[o] \/ (s.cum[o] # -1 /\ s.cum[o] # n)]
\* crates/tickwire-account/src/admission.rs:437-451
\* crates/tickwire-account/src/events.rs:70-84; duplicate economics still observe.
ReplayFillUnknown(o,k) == /\ o \in s.orders /\ s.trades[o][k] # 0
                         /\ s' = [s EXCEPT !.unknown[o] = TRUE, !.frozen = TRUE]
ReplayFillTerminal(o,k,n) == /\ o \in s.orders /\ s.trades[o][k] # 0 /\ Terminal(o,n)
Reconcile(o) == /\ o \in s.orders /\ s.terminal[o] /\ s.cum[o] = s.filled[o]
                /\ ~s.closed[o] /\ s' = [s EXCEPT !.closed[o] = TRUE]
RejectReconcile(o) == /\ o \in s.orders /\ (~s.terminal[o] \/ s.cum[o] # s.filled[o])
                      /\ s' = [s EXCEPT !.frozen = TRUE]
\* Trusted journaled AbortedBeforeSend only; not a proof of WAL synchronization.
\* crates/tickwire-account/src/admission.rs:411-434
Abort(o) == /\ o \in s.orders /\ ~s.aborted[o] /\ ~s.economic[o]
            /\ ~s.confirmed[o] /\ ~s.terminal[o] /\ ~s.unknown[o]
            /\ s' = [s EXCEPT !.aborted[o] = TRUE, !.closed[o] = TRUE]
ReplayAbort(o) == /\ o \in s.orders /\ s.aborted[o] /\ UNCHANGED s
RejectAbort(o) == /\ o \in s.orders /\ ~s.aborted[o]
                  /\ (s.economic[o] \/ s.confirmed[o] \/ s.terminal[o] \/ s.unknown[o])
                  /\ s' = [s EXCEPT !.frozen = TRUE]
\* Identity or arithmetic errors retain financial state and the observation externally.
\* crates/tickwire-account/src/events.rs:3-17
RejectFact == /\ s.orders # {} /\ s' = [s EXCEPT !.frozen = TRUE]
Next == \/ (\E o \in Orders: Admit(o) \/ RejectAdmission(o) \/ RetryAdmission(o)
            \/ ConflictAdmission(o) \/ Acknowledge(o) \/ ReplayFee(o) \/ ConflictFee(o) \/ Unknown(o)
            \/ Reconcile(o) \/ RejectReconcile(o) \/ Abort(o) \/ ReplayAbort(o) \/ RejectAbort(o))
        \/ (\E o \in Orders,k \in 1..2: ReplayFill(o,k) \/ ReplayFillUnknown(o,k) \/ ConflictFill(o,k))
        \/ (\E o \in Orders,k \in 1..2,c \in 1..2: Fill(o,k,c))
        \/ (\E o \in Orders,n \in FeeValues: Fee(o,n))
        \/ (\E o \in Orders,n \in 0..2: Terminal(o,n))
        \/ (\E o \in Orders,k \in 1..2,n \in 0..2: ReplayFillTerminal(o,k,n))
        \/ RejectFact
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.orders \subseteq Orders /\ s.frozen \in BOOLEAN
          /\ s.filled \in [Orders -> 0..2] /\ s.fees \in [Orders -> {-1} \cup FeeValues]
          /\ s.cum \in [Orders -> -1..2] /\ s.paid \in [Orders -> 0..4]
          /\ s.trades \in [Orders -> [1..2 -> 0..2]]
          /\ \A o \in Orders: s.economic[o] \in BOOLEAN /\ s.confirmed[o] \in BOOLEAN /\ s.terminal[o] \in BOOLEAN
              /\ s.closed[o] \in BOOLEAN /\ s.aborted[o] \in BOOLEAN /\ s.unknown[o] \in BOOLEAN
NoOpenDeficit == ~s.frozen => FreeBase(s) >= 0 /\ FreeQuote(s) >= 0
AttributionConserved == /\ s.totalBase = InitialBase + Sum(s.baseOwner)
                        /\ s.totalQuote = InitialQuote + Sum(s.quoteOwner)
NoDuplicateEconomics ==
    /\ s.totalBase = InitialBase + Sum(s.filled) - Sum(s.paid)
    /\ s.totalQuote = InitialQuote - Sum([o \in Orders |-> Sum(s.trades[o])])
    /\ \A o \in Orders: s.filled[o] = Cardinality({k \in 1..2:s.trades[o][k] # 0})
NoAbortAfterConfirmation == \A o \in s.orders: s.aborted[o] => ~s.confirmed[o]
NoAbortAfterEconomics == \A o \in s.orders: s.aborted[o] => ~s.economic[o]
MissingDetailsRetained == \A o \in s.orders:
    s.terminal[o] /\ s.cum[o] > s.filled[o] /\ ~s.aborted[o]
        => QuoteReserve(s,o) = 2 * (2 - s.filled[o])
ZeroFeeIsEvidence == \A o \in Orders: s.fees[o] = 0 => s.economic[o] /\ ~s.aborted[o]
UnknownFreezes == \A o \in Orders: s.unknown[o] => s.frozen
TerminalContradictionFreezes == \A o \in s.orders:
    s.cum[o] # -1 /\ s.filled[o] > s.cum[o] => s.frozen
=============================================================================
