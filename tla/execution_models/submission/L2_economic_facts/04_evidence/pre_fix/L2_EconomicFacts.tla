-------------------------- MODULE L2_EconomicFacts --------------------------
EXTENDS Integers, FiniteSets, Sequences, TLC
\* var <-> code map; every action is a complete serialized Rust method call.
\* engine: Engine.orders/spent/held/frozen and Order fields:
\*   crates/tickwire-execution/src/lib.rs:138-216
\* present: intents whose legal reserve/commit/begin_send completed (Admit summary):
\*   crates/tickwire-execution/src/lib.rs:262-339
\* journal: MockJournal.facts including duplicates and rejected facts:
\*   crates/tickwire-mock/src/lib.rs:104-114
\* restarts: finite environment budget for MockRuntime.restart calls:
\*   crates/tickwire-mock/src/lib.rs:265-276
\* checks: observer-only transition witnesses, not implementation state.
\* RecordNew/RecordDuplicate/RecordRejected: append then fact.apply:
\*   crates/tickwire-mock/src/lib.rs:236-263
\*   crates/tickwire-execution/src/lib.rs:407-501
\* AppendFailure: journal unavailable => engine.freeze:
\*   crates/tickwire-mock/src/lib.rs:236-240
\* Ack/Uncertain/CancelRequest/CancelRejected:
\*   crates/tickwire-execution/src/lib.rs:374-405
\* NoBytesAccepted/NoBytesRejected:
\*   crates/tickwire-execution/src/lib.rs:352-372
\* RestartSuccess/RestartFailed: local replay, publish only after every fact succeeds:
\*   crates/tickwire-execution/src/lib.rs:504-528
\*   crates/tickwire-mock/src/lib.rs:265-276
\* observed denotes maximum reported cumulative/recorded fill quantity, not sequence.
\* Mode: transcription. Bounds & i128 upper-bound embedding: 01_reasoning.md.
CONSTANTS Orders, Capital, EventSlots, MaxFacts, MaxRestarts, ArithMax
VARIABLES engine, present, journal, restarts, checks
vars == <<engine, present, journal, restarts, checks>>
Qty == 2
Unit == 1
FeeBudget == 1
Budget == Qty * Unit + FeeBudget
Max(a,b) == IF a > b THEN a ELSE b
\* Sum values by keys, rather than summing a set of values (which loses equals).
RECURSIVE SumBy(_, _)
SumBy(keys, f) == IF keys = {} THEN 0
                 ELSE LET k == CHOOSE a \in keys: TRUE IN f[k] + SumBy(keys \ {k}, f)
EmptyOrder == [send |-> "Unknown", term |-> "None", cancel |-> "None",
               filled |-> 0, observed |-> 0, spent |-> 0, fees |-> 0,
               released |-> FALSE, events |-> {}]
Held(o) == (IF o.term = "None" THEN Qty-o.filled ELSE Max(o.observed-o.filled,0))*Unit
           + IF o.released THEN 0 ELSE Max(FeeBudget-o.fees,0)
EmptyEngine == [orders |-> [o \in Orders |-> EmptyOrder], spent |-> 0, held |-> 0, frozen |-> FALSE]
Freeze(e) == [e EXCEPT !.frozen = TRUE]
Financial(e) == <<e.orders, e.spent, e.held>>
Outcome(e,c) == [e |-> e, code |-> c]
Failure(e,c) == Outcome(Freeze(e),c)
Replace(e,i,o) ==
    LET s == e.spent-e.orders[i].spent+o.spent
        h == e.held-Held(e.orders[i])+Held(o)
    IN IF s > ArithMax \/ h > ArithMax \/ s+h > ArithMax
       THEN Failure(e,"Overflow")
       ELSE Outcome([e EXCEPT !.orders[i] = o, !.spent = s, !.held = h,
                             !.frozen = @ \/ s+h > Capital],"New")
\* Uniform fact records retain all payload fields; kind and id form the dedupe key.
Fact(i,k,id,q,a,t) == [order |-> i, kind |-> k, id |-> id, q |-> q, amount |-> a, status |-> t]
Facts(i) == {
    Fact(i,"Fill",1,1,1,"None"), Fact(i,"Fill",1,1,2,"None"),
    Fact(i,"Fill",2,1,0,"None"), Fact(i,"Fill",2,2,ArithMax-1,"None"),
    Fact(i,"Fee",1,0,0,"None"), Fact(i,"Fee",1,0,1,"None"),
    Fact(i,"Fee",2,0,ArithMax,"None"), Fact(i,"Fee",2,0,-1,"None"),
    Fact(i,"Terminal",0,0,0,"Canceled"), Fact(i,"Terminal",0,1,0,"Canceled"),
    Fact(i,"Terminal",0,2,0,"Filled"), Fact(i,"Terminal",0,1,0,"Filled") }
Apply(e,f) ==
    LET o == e.orders[f.order]
        matches == {v \in o.events: v.kind = f.kind /\ v.id = f.id}
        q == o.filled+f.q
    IN IF o.send = "NotSubmitted" THEN Failure(e,"Invalid")
       ELSE IF f.kind = "Terminal"
       THEN IF f.q > Qty \/ (f.status = "Filled" /\ f.q # Qty)
            THEN Failure(e,"Invalid")
            ELSE LET observed == Max(Max(o.observed,f.q),o.filled)
                     term == IF observed = Qty THEN "Filled"
                             ELSE IF o.term = "None" THEN f.status ELSE o.term
                 IN Replace(e,f.order,[o EXCEPT !.observed = observed, !.term = term, !.send = "Confirmed"])
       ELSE IF f.amount < 0 \/ (f.kind = "Fill" /\ f.q = 0) THEN Failure(e,"Invalid")
       ELSE IF matches # {} THEN IF f \in matches THEN Outcome(e,"Duplicate") ELSE Failure(e,"Conflict")
       ELSE IF Cardinality(o.events) = EventSlots THEN Failure(e,"Capacity")
       ELSE IF f.kind = "Fill" /\ q > Qty THEN Failure(e,"Invalid")
       ELSE IF o.spent+f.amount > ArithMax \/ (f.kind = "Fee" /\ o.fees+f.amount > ArithMax)
            THEN Failure(e,"Overflow")
       ELSE LET n == [o EXCEPT !.events = @ \cup {f}, !.spent = @+f.amount, !.send = "Confirmed"]
                updated == IF f.kind = "Fee" THEN [n EXCEPT !.fees = @+f.amount]
                           ELSE [n EXCEPT !.filled = q, !.observed = Max(@,q),
                                          !.term = IF q = Qty THEN "Filled" ELSE @]
            IN Replace(e,f.order,updated)
Errors == {"Invalid","Conflict","Capacity","Overflow"}
RECURSIVE Replay(_, _)
Replay(e,fs) == IF Len(fs) = 0 THEN Outcome(e,"Done")
               ELSE LET a == Apply(e,Head(fs))
                    IN IF a.code \in Errors THEN a ELSE Replay(a.e,Tail(fs))
Recovered == [EmptyEngine EXCEPT !.held = Cardinality(present)*Budget, !.frozen = TRUE]
ReplayResult == Replay(Recovered,journal)
InitialChecks == [atomic |-> TRUE, duplicate |-> TRUE, monotone |-> TRUE,
                  control |-> TRUE, recovery |-> TRUE, persisted |-> TRUE]
Init == /\ engine = EmptyEngine /\ present = {} /\ journal = <<>>
        /\ restarts = 0 /\ checks = InitialChecks
Admit(i) == /\ i \in Orders \ present /\ ~engine.frozen
            /\ Budget <= Capital-engine.spent-engine.held
            /\ engine' = [engine EXCEPT !.held = @+Budget]
            /\ present' = present \cup {i}
            /\ UNCHANGED <<journal,restarts,checks>>
RecordWith(f,codes) ==
    /\ Len(journal) < MaxFacts
    /\ LET a == Apply(engine,f)
       IN /\ a.code \in codes
          /\ engine' = a.e
          /\ journal' = Append(journal,f)
          /\ checks' = [checks EXCEPT
              !.atomic = @ /\ (a.code \in Errors => Financial(a.e)=Financial(engine) /\ a.e.frozen),
              !.duplicate = @ /\ (a.code="Duplicate" => a.e=engine),
              !.monotone = @ /\ a.e.orders[f.order].filled >= engine.orders[f.order].filled
                             /\ a.e.orders[f.order].observed >= engine.orders[f.order].observed
                             /\ (engine.orders[f.order].term # "None" => a.e.orders[f.order].term # "None"),
              !.persisted = @ /\ journal'[Len(journal')] = f]
    /\ UNCHANGED <<present,restarts>>
RecordNew == \E i \in present: \E f \in Facts(i): RecordWith(f,{"New"})
RecordDuplicate == \E i \in present: \E f \in Facts(i): RecordWith(f,{"Duplicate"})
RecordRejected == \E i \in present: \E f \in Facts(i): RecordWith(f,Errors)
AppendFailure == /\ engine' = Freeze(engine)
                 /\ checks' = [checks EXCEPT !.atomic = @ /\ Financial(engine')=Financial(engine)]
                 /\ UNCHANGED <<present,journal,restarts>>
Ack(i) == /\ engine.orders[i].send # "NotSubmitted"
          /\ engine' = [engine EXCEPT !.orders[i].send = "Confirmed"]
          /\ UNCHANGED <<present,journal,restarts,checks>>
Uncertain(i) == /\ engine.orders[i].send # "NotSubmitted"
                /\ engine' = Freeze(engine)
                /\ UNCHANGED <<present,journal,restarts,checks>>
CancelRequest(i) == /\ engine.orders[i].send # "NotSubmitted" /\ engine.orders[i].term = "None"
                    /\ engine' = [engine EXCEPT !.orders[i].cancel = "Requested"]
                    /\ checks' = [checks EXCEPT !.control = @ /\ engine'.spent=engine.spent /\ engine'.held=engine.held]
                    /\ UNCHANGED <<present,journal,restarts>>
CancelRejected(i) == /\ engine' = [engine EXCEPT !.orders[i].cancel = "Rejected"]
                     /\ checks' = [checks EXCEPT !.control = @ /\ engine'.spent=engine.spent /\ engine'.held=engine.held]
                     /\ UNCHANGED <<present,journal,restarts>>
CanNoBytes(o) == o.send="Unknown" /\ o.filled=0 /\ o.observed=0 /\ o.term="None"
NoBytesAccepted(i) ==
    /\ CanNoBytes(engine.orders[i])
    /\ LET o == [engine.orders[i] EXCEPT !.send="NotSubmitted", !.term="Rejected", !.released=TRUE]
       IN engine' = Replace(engine,i,o).e
    /\ UNCHANGED <<present,journal,restarts,checks>>
NoBytesRejected(i) == /\ ~CanNoBytes(engine.orders[i])
                     /\ engine' = Freeze(engine)
                     /\ checks' = [checks EXCEPT !.control = @ /\ Financial(engine')=Financial(engine)]
                     /\ UNCHANGED <<present,journal,restarts>>
RestartSuccess == /\ restarts < MaxRestarts /\ ReplayResult.code = "Done"
                  /\ engine' = ReplayResult.e /\ restarts' = restarts+1
                  /\ checks' = [checks EXCEPT !.recovery = @ /\ engine'.frozen]
                  /\ UNCHANGED <<present,journal>>
RestartFailed == /\ restarts < MaxRestarts /\ ReplayResult.code \in Errors
                 /\ restarts' = restarts+1
                 /\ checks' = [checks EXCEPT !.recovery = @ /\ engine.frozen]
                 /\ UNCHANGED <<engine,present,journal>>
Next == \/ \E i \in Orders: Admit(i)
        \/ RecordNew \/ RecordDuplicate \/ RecordRejected \/ AppendFailure
        \/ (\E i \in present: Ack(i))
        \/ (\E i \in present: Uncertain(i))
        \/ (\E i \in present: CancelRequest(i))
        \/ (\E i \in present: CancelRejected(i))
        \/ (\E i \in present: NoBytesAccepted(i))
        \/ (\E i \in present: NoBytesRejected(i))
        \/ RestartSuccess \/ RestartFailed
Spec == Init /\ [][Next]_vars
TypeOK == /\ present \subseteq Orders /\ restarts \in 0..MaxRestarts /\ Len(journal) <= MaxFacts
          /\ engine.spent \in 0..ArithMax /\ engine.held \in 0..ArithMax /\ engine.frozen \in BOOLEAN
          /\ \A i \in present: /\ engine.orders[i].filled \in 0..Qty
                                /\ engine.orders[i].observed \in engine.orders[i].filled..Qty
                                /\ Cardinality(engine.orders[i].events) <= EventSlots
OrderAccounting == \A i \in present:
    LET o == engine.orders[i]
        fills == {f \in o.events: f.kind="Fill"}
        fees == {f \in o.events: f.kind="Fee"}
    IN /\ o.filled = SumBy(fills,[f \in fills |-> f.q])
       /\ o.fees = SumBy(fees,[f \in fees |-> f.amount])
       /\ o.spent = SumBy(o.events,[f \in o.events |-> f.amount])
       /\ \A a,b \in o.events: (a.kind=b.kind /\ a.id=b.id) => a=b
LedgerAccounting == /\ engine.spent = SumBy(present,[i \in present |-> engine.orders[i].spent])
                    /\ engine.held = SumBy(present,[i \in present |-> Held(engine.orders[i])])
RepresentableDeficit == /\ engine.spent+engine.held <= ArithMax
                        /\ (~engine.frozen => engine.spent+engine.held <= Capital)
EvidencePreventsRelease == \A i \in present:
    engine.orders[i].events # {} => engine.orders[i].send="Confirmed" /\ ~engine.orders[i].released
UnreportedFillHeld == \A i \in present:
    engine.orders[i].term # "None" => Held(engine.orders[i]) >= (engine.orders[i].observed-engine.orders[i].filled)*Unit
TransitionChecks == \A k \in DOMAIN checks: checks[k]
=============================================================================
