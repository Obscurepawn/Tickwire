------------------------- MODULE L1_CommandBinding -------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
CONSTANTS Orders, Variants, Capital, MaxCrashes
\* var <-> code: synchronous &mut Runtime, no overlapping method calls.
\* pc/current/request: runtime call stack and OrderRequest.command_digest.
\* crates/tickwire-business-journal/src/runtime.rs:85-171
\* original/sent/rejectedZero: history witnesses of accepted/submitted/rejected requests.
\* crates/tickwire-execution/src/lib.rs:272-347
\* closure/objects: synchronized VerifiedClosure binding and optional command digest.
\* crates/tickwire-recovery/src/lib.rs:92-106,166-222
\* disk/cache: complete synchronized/readable WAL prefixes; request digest survives CBOR.
\* crates/tickwire-business-journal/src/lib.rs:304-329
\* crates/tickwire-journal-codec/src/lib.rs:270-271,358-362
\* held/prepared/aborted/frozen: reducer state and recovery freeze.
\* crates/tickwire-business-journal/src/runtime.rs:49-83,168-169
\* crashes/index/recovering: bounded process failure and ordered replay.
\* crates/tickwire-business-journal/src/runtime.rs:41-83
VARIABLE s
vars == <<s>>
Ids == 1..Orders
Digests == 1..Variants
Event(k,i,d) == [kind |-> k, id |-> i, digest |-> d]
Has(es,k,i,d) == \E n \in 1..Len(es): es[n]=Event(k,i,d)
Prefix(a,b) == a=SubSeq(b,1,Len(a))

Init == s=[pc |-> "Idle",current |-> 1,disk |-> <<>>,cache |-> <<>>,
    request |-> [i \in Ids |-> 0],original |-> [i \in Ids |-> 0],
    closure |-> [i \in Ids |-> 0],objects |-> {},
    held |-> {},prepared |-> {},aborted |-> {},sent |-> {},
    noBytes |-> {},released |-> {},live |-> TRUE,frozen |-> FALSE,
    recovering |-> FALSE,index |-> 1,crashes |-> 0,rejectedZero |-> FALSE]

\* Reserve/ReserveMissing/Rebind: runtime.rs:85-101; execution rejects zero before mutation.
\* crates/tickwire-business-journal/src/runtime.rs:85-101
\* crates/tickwire-execution/src/lib.rs:272-309
Reserve(i,d) == /\ s.pc="Idle" /\ ~s.frozen /\ s.live /\ i \in Ids /\ d \in Digests
    /\ s.request[i]=0 /\ Cardinality(s.held)<Capital
    /\ s'=[s EXCEPT !.current=i,!.held=@ \cup {i},!.request[i]=d,
                     !.original[i]=d,!.pc="Authorize"]
ReserveMissing == /\ s.pc="Idle" /\ ~s.frozen /\ s.live /\ ~s.rejectedZero
    /\ \E i \in Ids: s.request[i]=0
    /\ s'=[s EXCEPT !.rejectedZero=TRUE]
Rebind(i,d) == /\ s.pc="Idle" /\ ~s.frozen /\ s.live
    /\ i \in Ids /\ s.request[i]#0 /\ d \in 0..Variants /\ d#s.request[i]
    /\ s'=[s EXCEPT !.frozen=TRUE]

\* Complete immutable publication; trusted authorizer failure or mismatching capability.
\* crates/tickwire-recovery/src/lib.rs:166-177
\* crates/tickwire-business-journal/src/lib.rs:246-252
\* crates/tickwire-business-journal/src/runtime.rs:133-141,168-169
Authorize == /\ s.pc="Authorize"
    /\ s'=[s EXCEPT !.closure[s.current]=s.request[s.current],
        !.objects=@ \cup {s.request[s.current]},!.pc="WriteIntent"]
AuthorizeFailed == /\ s.pc="Authorize" /\ s'=[s EXCEPT !.pc="Idle",!.frozen=TRUE]
MissingCommand == /\ s.pc="Authorize"
    /\ s'=[s EXCEPT !.closure[s.current]=s.request[s.current],!.pc="RejectCommand"]
WrongBinding(d) == /\ s.pc="Authorize" /\ d \in Digests /\ d#s.request[s.current]
    /\ s'=[s EXCEPT !.objects=@ \cup {d},!.pc="RejectBinding"]
RejectCommand == /\ s.pc="RejectCommand" /\ s'=[s EXCEPT !.pc="Idle",!.frozen=TRUE]
RejectBinding == /\ s.pc="RejectBinding" /\ s'=[s EXCEPT !.pc="Idle",!.frozen=TRUE]

\* Complete write/sync/receipt publication; no send until all three return.
\* crates/tickwire-business-journal/src/lib.rs:304-329
WriteIntent == /\ s.pc="WriteIntent"
    /\ s'=[s EXCEPT !.cache=Append(@,Event("Prepared",s.current,s.request[s.current])),!.pc="SyncIntent"]
SyncIntent == /\ s.pc="SyncIntent" /\ s'=[s EXCEPT !.disk=s.cache,!.pc="PublishIntent"]
PublishIntent == /\ s.pc="PublishIntent"
    /\ s'=[s EXCEPT !.prepared=@ \cup {s.current},!.pc="Send"]

\* Fresh post-I/O gate, then reducer and exchange call, with no blocking I/O between them.
\* crates/tickwire-business-journal/src/runtime.rs:142-157
GateExpire == /\ s.live /\ s.pc \in {"Authorize","WriteIntent","SyncIntent","PublishIntent","Send"}
    /\ s'=[s EXCEPT !.live=FALSE]
Send == /\ s.pc="Send" /\ s.live
    /\ s'=[s EXCEPT !.sent=@ \cup {[id |-> s.current,digest |-> s.request[s.current]]},!.pc="Response"]
GateRejected == /\ s.pc="Send" /\ ~s.live /\ s'=[s EXCEPT !.frozen=TRUE,!.pc="Idle"]
PendingOrAck == /\ s.pc="Response" /\ s'=[s EXCEPT !.pc="Idle"]
Unknown == /\ s.pc="Response" /\ s'=[s EXCEPT !.pc="Idle",!.frozen=TRUE]

\* Only trusted NoBytes response permits a durable abort followed by release.
\* crates/tickwire-business-journal/src/runtime.rs:158-163
NoBytes == /\ s.pc="Response" /\ s'=[s EXCEPT !.noBytes=@ \cup {s.current},!.pc="WriteAbort"]
WriteAbort == /\ s.pc="WriteAbort"
    /\ s'=[s EXCEPT !.cache=Append(@,Event("Aborted",s.current,s.request[s.current])),!.pc="SyncAbort"]
SyncAbort == /\ s.pc="SyncAbort" /\ s'=[s EXCEPT !.disk=s.cache,!.pc="Release"]
Release == /\ s.pc="Release"
    /\ s'=[s EXCEPT !.held=@ \ {s.current},!.aborted=@ \cup {s.current},
                     !.released=@ \cup {s.current},!.pc="Idle"]

\* A failed write/sync may leave a complete readable/synchronized final batch.
\* crates/tickwire-business-journal/src/lib.rs:311-321
WriteFailed(complete) == /\ s.pc \in {"WriteIntent","WriteAbort"} /\ complete \in BOOLEAN
    /\ s'=[s EXCEPT !.cache=IF complete
        THEN Append(@,Event(IF s.pc="WriteIntent" THEN "Prepared" ELSE "Aborted",s.current,s.request[s.current]))
        ELSE @,!.pc="Idle",!.frozen=TRUE]
SyncFailed(done) == /\ s.pc \in {"SyncIntent","SyncAbort"} /\ done \in BOOLEAN
    /\ s'=[s EXCEPT !.disk=IF done THEN s.cache ELSE @,!.pc="Idle",!.frozen=TRUE]

\* Process restart discards memory and re-synchronizes surviving complete prefix.
\* crates/tickwire-business-journal/src/runtime.rs:41-83
Crash(keep) == /\ s.pc#"Down" /\ s.crashes<MaxCrashes /\ keep \in BOOLEAN
    /\ LET bytes==IF keep THEN s.cache ELSE s.disk IN
       s'=[s EXCEPT !.disk=bytes,!.cache=bytes,!.pc="Down",!.held={},!.prepared={},
          !.aborted={},!.request=[i \in Ids |-> 0],!.frozen=TRUE,
          !.recovering=TRUE,!.index=1,!.crashes=@+1]
Recover == /\ s.pc="Down" /\ s'=[s EXCEPT !.pc="Replay"]
ReplayPrepared == /\ s.pc="Replay" /\ s.index<=Len(s.disk)
    /\ LET e==s.disk[s.index] IN
       /\ e.kind="Prepared" /\ e.digest=s.closure[e.id] /\ e.digest \in s.objects
       /\ s'=[s EXCEPT !.held=@ \cup {e.id},!.prepared=@ \cup {e.id},
                         !.request[e.id]=e.digest,!.index=@+1]
ReplayAbort == /\ s.pc="Replay" /\ s.index<=Len(s.disk)
    /\ LET e==s.disk[s.index] IN
       /\ e.kind="Aborted" /\ e.id \in s.prepared
       /\ s'=[s EXCEPT !.held=@ \ {e.id},!.aborted=@ \cup {e.id},
                         !.released=@ \cup {e.id},!.index=@+1]
ReplayFailed == /\ s.pc="Replay" /\ s.index<=Len(s.disk) /\ s.disk[s.index].kind="Prepared"
    /\ s'=[s EXCEPT !.pc="Stopped",!.frozen=TRUE]
RecoveryDone == /\ s.pc="Replay" /\ s.index=Len(s.disk)+1 /\ s'=[s EXCEPT !.pc="Idle"]

Next == (\E i \in Ids,d \in Digests: Reserve(i,d)) \/ ReserveMissing
    \/ (\E i \in Ids,d \in 0..Variants: Rebind(i,d))
    \/ Authorize \/ AuthorizeFailed \/ MissingCommand \/ RejectCommand
    \/ (\E d \in Digests: WrongBinding(d)) \/ RejectBinding
    \/ WriteIntent \/ SyncIntent \/ PublishIntent \/ GateExpire \/ Send \/ GateRejected
    \/ PendingOrAck \/ Unknown \/ NoBytes \/ WriteAbort \/ SyncAbort \/ Release
    \/ (\E b \in BOOLEAN: WriteFailed(b)) \/ (\E b \in BOOLEAN: SyncFailed(b))
    \/ (\E b \in BOOLEAN: Crash(b)) \/ Recover \/ ReplayPrepared \/ ReplayAbort
    \/ ReplayFailed \/ RecoveryDone
Spec == Init /\ [][Next]_vars
TypeOK == /\ s.request \in [Ids -> 0..Variants] /\ s.original \in [Ids -> 0..Variants]
    /\ s.closure \in [Ids -> 0..Variants] /\ s.objects \subseteq Digests
    /\ s.held \subseteq Ids /\ s.prepared \subseteq Ids /\ s.released \subseteq Ids
    /\ s.sent \subseteq [id:Ids,digest:Digests] /\ Len(s.cache)<=2*Orders
    /\ s.crashes \in 0..MaxCrashes
DurablePrefix == Prefix(s.disk,s.cache) /\ Len(s.cache)<=Len(s.disk)+1
AcceptedDigestNeverChanges == \A i \in Ids: s.request[i]#0 => s.request[i]=s.original[i]
EveryLoggedCommandHasObject == \A n \in 1..Len(s.cache):
    LET e==s.cache[n] IN e.digest=s.original[e.id] /\ e.digest=s.closure[e.id] /\ e.digest \in s.objects
EverySentDigestWasCommitted == \A e \in s.sent:
    e.digest=s.original[e.id] /\ Has(s.disk,"Prepared",e.id,e.digest) /\ e.digest \in s.objects
NoEarlyRelease == \A i \in s.released: i \in s.noBytes /\ Has(s.disk,"Aborted",i,s.original[i])
OrderedAbort == \A n \in 1..Len(s.disk):
    LET e==s.disk[n] IN e.kind="Aborted" => Has(SubSeq(s.disk,1,n-1),"Prepared",e.id,e.digest)
RecoveredStaysFrozen == s.recovering => s.frozen
ReservationBound == Cardinality(s.held)<=Capital
RecoveredHold == s.recovering /\ s.pc="Idle" =>
    s.held={i \in Ids: Has(s.disk,"Prepared",i,s.original[i]) /\ ~Has(s.disk,"Aborted",i,s.original[i])}
=============================================================================
