------------------------ MODULE L0_BusinessJournal ------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
CONSTANTS Orders, Capital, MaxCrashes
\* var <-> code: one synchronous &mut Runtime; no concurrent method calls.
\* pc/current: call stack, crates/tickwire-business-journal/src/runtime.rs:85-171.
\* disk/cache: complete physical prefixes; crates/tickwire-business-journal/src/lib.rs:304-329.
\* closure: verified persistent binding; crates/tickwire-business-journal/src/runtime.rs:133-141.
\* held/prepared/aborted: reducer subset, crates/tickwire-execution/src/lib.rs:279-348.
\* frozen/recovering/index: crates/tickwire-business-journal/src/runtime.rs:41-83; crates/tickwire-business-journal/src/runtime.rs:168-170.
\* live: current_gate sampled after I/O, crates/tickwire-business-journal/src/runtime.rs:92; crates/tickwire-business-journal/src/runtime.rs:142-153.
\* called/noBytes/released/sentFresh: historical witnesses of submit/release, crates/tickwire-business-journal/src/runtime.rs:154-163.
\* crashes: bounded process failures; file child test preserves actual files.
VARIABLE s
vars == <<s>>
Ids == 1..Orders
Event(k,i) == [kind |-> k,id |-> i]
Has(es,k,i) == \E n \in 1..Len(es): es[n]=Event(k,i)
Prefix(a,b) == a=SubSeq(b,1,Len(a))

Init == s=[pc |-> "Idle",current |-> 1,disk |-> <<>>,cache |-> <<>>,
    closure |-> {},held |-> {},prepared |-> {},aborted |-> {},called |-> {},
    noBytes |-> {},released |-> {},sentFresh |-> {},live |-> TRUE,
    frozen |-> FALSE,recovering |-> FALSE,index |-> 1,crashes |-> 0]

\* reserve uses a fresh gate and available capital, crates/tickwire-business-journal/src/runtime.rs:85-101.
Reserve(i) == /\ s.pc="Idle" /\ ~s.frozen /\ s.live /\ i \in Ids
    /\ i \notin s.prepared \cup s.held /\ Cardinality(s.held)<Capital
    /\ s'=[s EXCEPT !.current=i,!.held=@ \cup {i},!.pc="Authorize"]
Authorize == /\ s.pc="Authorize"
    /\ s'=[s EXCEPT !.closure=@ \cup {s.current},!.pc="WriteIntent"]
AuthorizeFailed == /\ s.pc="Authorize"
    /\ s'=[s EXCEPT !.pc="Idle",!.frozen=TRUE]

\* BusinessJournal::append publishes only after physical append_batch returns.
WriteIntent == /\ s.pc="WriteIntent"
    /\ s'=[s EXCEPT !.cache=Append(@,Event("Prepared",s.current)),!.pc="SyncIntent"]
SyncIntent == /\ s.pc="SyncIntent"
    /\ s'=[s EXCEPT !.disk=s.cache,!.pc="PublishIntent"]
PublishIntent == /\ s.pc="PublishIntent"
    /\ s'=[s EXCEPT !.prepared=@ \cup {s.current},!.pc="Send"]
GateExpire == /\ s.live /\ s.pc \in {"Authorize","WriteIntent","SyncIntent","PublishIntent","Send"}
    /\ s'=[s EXCEPT !.live=FALSE]
\* Fresh sample + local reducer check + Exchange invocation; no blocking I/O between.
Send == /\ s.pc="Send" /\ s.live
    /\ s'=[s EXCEPT !.called=@ \cup {s.current},
        !.sentFresh=IF s.live THEN @ \cup {s.current} ELSE @,!.pc="Response"]
GateRejected == /\ s.pc="Send" /\ ~s.live
    /\ s'=[s EXCEPT !.frozen=TRUE,!.pc="Idle"]
PendingOrAck == /\ s.pc="Response" /\ s'=[s EXCEPT !.pc="Idle"]
Unknown == /\ s.pc="Response" /\ s'=[s EXCEPT !.pc="Idle",!.frozen=TRUE]
NoBytes == /\ s.pc="Response"
    /\ s'=[s EXCEPT !.noBytes=@ \cup {s.current},!.pc="WriteAbort"]
WriteAbort == /\ s.pc="WriteAbort"
    /\ s'=[s EXCEPT !.cache=Append(@,Event("Aborted",s.current)),!.pc="SyncAbort"]
SyncAbort == /\ s.pc="SyncAbort" /\ s'=[s EXCEPT !.disk=s.cache,!.pc="Release"]
Release == /\ s.pc="Release"
    /\ s'=[s EXCEPT !.held=@ \ {s.current},!.aborted=@ \cup {s.current},
                     !.released=@ \cup {s.current},!.pc="Idle"]

\* A write may fail before/after the complete final batch became readable.
WriteFailed(complete) == /\ s.pc \in {"WriteIntent","WriteAbort"} /\ complete \in BOOLEAN
    /\ s'=[s EXCEPT !.cache=IF complete THEN Append(@,Event(IF s.pc="WriteIntent" THEN "Prepared" ELSE "Aborted",s.current)) ELSE @,
                     !.pc="Idle",!.frozen=TRUE]
SyncFailed(done) == /\ s.pc \in {"SyncIntent","SyncAbort"} /\ done \in BOOLEAN
    /\ s'=[s EXCEPT !.disk=IF done THEN s.cache ELSE @,!.pc="Idle",!.frozen=TRUE]

\* Recovered complete tail is resynchronized before business replay.
Crash(keep) == /\ s.pc#"Down" /\ s.crashes<MaxCrashes /\ keep \in BOOLEAN
    /\ LET bytes==IF keep THEN s.cache ELSE s.disk IN
       s'=[s EXCEPT !.disk=bytes,!.cache=bytes,!.pc="Down",!.held={},!.prepared={},
          !.aborted={},!.frozen=TRUE,!.recovering=TRUE,!.index=1,!.crashes=@+1]
Recover == /\ s.pc="Down" /\ s'=[s EXCEPT !.pc="Replay"]
ReplayPrepared == /\ s.pc="Replay" /\ s.index<=Len(s.disk)
    /\ LET e==s.disk[s.index] IN
       /\ e.kind="Prepared" /\ e.id \in s.closure
       /\ s'=[s EXCEPT !.held=@ \cup {e.id},!.prepared=@ \cup {e.id},!.index=@+1]
ReplayAbort == /\ s.pc="Replay" /\ s.index<=Len(s.disk)
    /\ LET e==s.disk[s.index] IN
       /\ e.kind="Aborted" /\ e.id \in s.prepared
       /\ s'=[s EXCEPT !.held=@ \ {e.id},!.aborted=@ \cup {e.id},
                         !.released=@ \cup {e.id},!.index=@+1]
ReplayFailed == /\ s.pc="Replay" /\ s.index<=Len(s.disk)
    /\ s.disk[s.index].kind="Prepared"
    /\ s'=[s EXCEPT !.pc="Stopped",!.frozen=TRUE]
RecoveryDone == /\ s.pc="Replay" /\ s.index=Len(s.disk)+1
    /\ s'=[s EXCEPT !.pc="Idle"]

Next == (\E i \in Ids: Reserve(i)) \/ Authorize \/ AuthorizeFailed
    \/ WriteIntent \/ SyncIntent \/ PublishIntent \/ GateExpire \/ Send \/ GateRejected
    \/ PendingOrAck \/ Unknown \/ NoBytes \/ WriteAbort \/ SyncAbort \/ Release
    \/ (\E b \in BOOLEAN: WriteFailed(b)) \/ (\E b \in BOOLEAN: SyncFailed(b))
    \/ (\E b \in BOOLEAN: Crash(b)) \/ Recover \/ ReplayPrepared \/ ReplayAbort
    \/ ReplayFailed \/ RecoveryDone
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.held \subseteq Ids /\ s.prepared \subseteq Ids /\ s.closure \subseteq Ids
    /\ s.called \subseteq Ids /\ s.released \subseteq Ids /\ s.noBytes \subseteq Ids
    /\ Len(s.cache)<=2*Orders /\ s.crashes \in 0..MaxCrashes
DurablePrefix == Prefix(s.disk,s.cache) /\ Len(s.cache)<=Len(s.disk)+1
SendRequiresClosureAndWal == \A i \in s.called: i \in s.closure /\ Has(s.disk,"Prepared",i)
EverySendFresh == s.called=s.sentFresh
NoEarlyRelease == \A i \in s.released: i \in s.noBytes /\ Has(s.disk,"Aborted",i)
OrderedAbort == \A n \in 1..Len(s.disk):
    s.disk[n].kind="Aborted" => Has(SubSeq(s.disk,1,n-1),"Prepared",s.disk[n].id)
RecoveredStaysFrozen == s.recovering => s.frozen
ReservationBound == Cardinality(s.held)<=Capital
RecoveredHold == s.recovering /\ s.pc="Idle" =>
    s.held={i \in Ids: Has(s.disk,"Prepared",i) /\ ~Has(s.disk,"Aborted",i)}
=============================================================================
