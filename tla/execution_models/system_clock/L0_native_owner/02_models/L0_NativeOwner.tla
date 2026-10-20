-------------------------- MODULE L0_NativeOwner --------------------------
EXTENDS Naturals, Integers, Sequences, FiniteSets, TLC

\* var <-> code: one owner, synchronous public API calls; no reset/reconstruction.
\* owner: local construction, publication, and move into TimeClient
\*   crates/tickwire-clock-sys/src/lib.rs:52-85
\*   crates/tickwire-binance-clock/src/client.rs:33-72
\* native.last/fault: Guard last successful pair and permanent native fault
\*   crates/tickwire-clock-sys/src/lib.rs:28-50
\* NativeSample: native_nanos error or Guard comparison; no native call after fault
\*   crates/tickwire-clock-sys/src/lib.rs:68-118
\* NativeError: actual MonotonicClock impl maps the error, preserving input domain
\*   crates/tickwire-binance-clock/src/lib.rs:36-55
\* client.lastI/fatal: provider classification and Instant check, before core read
\*   crates/tickwire-binance-clock/src/client.rs:74-107
\* client.core: no-anchor Calibrator last_seen/fault; never network-calibrated here
\*   crates/tickwire-clock/src/lib.rs:190-211
\*   crates/tickwire-clock/src/lib.rs:295-299
\* Public sample calls local then sign_time at the same MonoTime; no-anchor returns
\* signed=None. The second observe has identical values and does not change state.
\*   crates/tickwire-binance-clock/src/client.rs:378-388
\*   crates/tickwire-clock/src/lib.rs:310-317
\* providerCalls/nativeReads and *OK are history witnesses, not source guards.
\* nativeReads counts native_nanos attempts, including Unsupported without syscall.
\* Fault inputs: syscall/field/conversion errors and defensive rollback checks.
\* Integer values denote order only; u64 conversion arithmetic is NOT abstracted
\* by a smaller maximum. MaxCalls bounds the external execution, not source state.

CONSTANTS Mode, PlatformSupported, MaxTick, MaxCalls
ASSUME /\ Mode \in {"Native","Generic"} /\ PlatformSupported \in BOOLEAN
       /\ MaxTick >= 2 /\ MaxCalls >= 4
Ticks == 0..MaxTick
NoPair == [i |-> -1,n |-> -1,d |-> 0]
Pair(i,n,d) == [i |-> i,n |-> n,d |-> d]
Permanent == {"Domain","TimeBackwards","Overflow","ClockJump","Faulted"}
Temporary == {"Transport","Expired","Uncalibrated","Uncertain"}
NativeErrors == {"System","InvalidTime","Overflow"}
Errors == Permanent \cup Temporary \cup NativeErrors \cup {"Ok","None","Unsupported","Backwards"}
EmptyNative == [last |-> NoPair,fault |-> FALSE]
EmptyCore == [seen |-> FALSE,last |-> 0,fault |-> "None"]
EmptyClient(d) == [domain |-> d,lastI |-> -1,fatal |-> "None",core |-> EmptyCore]
NativeError(e) == CASE e="Backwards" -> "TimeBackwards"
 [] e="Overflow" -> "Overflow" [] OTHER -> "Faulted"

\* crates/tickwire-clock-sys/src/lib.rs:68-85
NativeSample(s,v,e) ==
 IF s.fault THEN [state |-> s,err |-> "Faulted",reads |-> 0]
 ELSE IF e#"Ok" THEN [state |-> [s EXCEPT !.fault=TRUE],err |-> e,reads |-> 1]
 ELSE IF s.last#NoPair /\ (v.i<s.last.i \/ v.n<s.last.n)
 THEN [state |-> [s EXCEPT !.fault=TRUE],err |-> "Backwards",reads |-> 1]
 ELSE [state |-> [s EXCEPT !.last=v],err |-> "Ok",reads |-> 1]

\* crates/tickwire-clock/src/lib.rs:200-211
\* crates/tickwire-clock/src/lib.rs:295-299
CoreRead(c,v,d) == IF c.fault#"None" THEN [state |-> c,err |-> "Faulted"]
 ELSE IF v.d#d THEN [state |-> [c EXCEPT !.fault="Domain"],err |-> "Domain"]
 ELSE IF c.seen /\ v.n<c.last
 THEN [state |-> [c EXCEPT !.fault="TimeBackwards"],err |-> "TimeBackwards"]
 ELSE [state |-> [c EXCEPT !.seen=TRUE,!.last=v.n],err |-> "Ok"]

\* crates/tickwire-binance-clock/src/client.rs:74-107
ClientSample(c,v,e) == IF c.fatal#"None"
 THEN [state |-> c,err |-> c.fatal,reads |-> 0]
 ELSE IF e#"Ok" THEN [state |-> IF e \in Permanent THEN [c EXCEPT !.fatal=e] ELSE c,
                       err |-> e,reads |-> 1]
 ELSE IF c.lastI#-1 /\ v.i<c.lastI
 THEN [state |-> [c EXCEPT !.fatal="TimeBackwards"],err |-> "TimeBackwards",reads |-> 1]
 ELSE LET before==[c EXCEPT !.lastI=v.i]
          r==CoreRead(c.core,v,c.domain)
      IN [state |-> [before EXCEPT !.core=r.state],err |-> r.err,reads |-> 1]

VARIABLE p
vars == <<p>>
Init == p=[owner |-> "Absent",native |-> EmptyNative,client |-> EmptyClient(1),
 calls |-> 0,nativeReads |-> 0,providerCalls |-> 0,last |-> NoPair,err |-> "None",
 construction |-> "None",output |-> NoPair,orderOK |-> TRUE,domainOK |-> TRUE,
 nativeStickyOK |-> TRUE,clientStickyOK |-> TRUE,coreStickyOK |-> TRUE,
 noNativeReadOK |-> TRUE,noProviderReadOK |-> TRUE,nativeLastOK |-> TRUE,
 instantOrderOK |-> TRUE,temporaryOK |-> TRUE]
Bound == p.calls<MaxCalls
HealthyPair(v) == v.i>=p.native.last.i /\ v.n>=p.native.last.n

\* Commit only stores history checks. It does not filter a production transition.
Record(q,v,e,nativeError,providerError) ==
 p'=[q EXCEPT !.calls=p.calls+1,!.err=e,!.output=IF e="Ok" THEN v ELSE NoPair,
 !.last=IF e="Ok" THEN v ELSE p.last,
 !.orderOK=p.orderOK /\ (e#"Ok" \/ p.last=NoPair \/ (v.i>=p.last.i /\ v.n>=p.last.n)),
 !.domainOK=p.domainOK /\ (e#"Ok" \/ v.d=(IF q.owner="Client" THEN q.client.domain ELSE 1)),
 !.nativeStickyOK=p.nativeStickyOK /\ (~p.native.fault \/ q.native.fault),
 !.clientStickyOK=p.clientStickyOK /\ (p.client.fatal="None" \/ q.client.fatal=p.client.fatal),
 !.coreStickyOK=p.coreStickyOK /\ (p.client.core.fault="None" \/ (q.client.core.fault=p.client.core.fault /\ e#"Ok")),
 !.noNativeReadOK=p.noNativeReadOK /\ (~p.native.fault \/ q.nativeReads=p.nativeReads),
 !.noProviderReadOK=p.noProviderReadOK /\ (p.client.fatal="None" \/ q.providerCalls=p.providerCalls),
 !.nativeLastOK=p.nativeLastOK /\ (nativeError="Ok" \/ q.native.last=p.native.last),
 !.instantOrderOK=p.instantOrderOK /\
   (p.owner#"Client" \/ p.client.fatal#"None" \/ providerError#"Ok" \/ v.i<p.client.lastI \/ q.client.lastI=v.i),
 !.temporaryOK=p.temporaryOK /\ (providerError \notin Temporary \/ q.client.fatal=p.client.fatal)]

\* crates/tickwire-clock-sys/src/lib.rs:57-64
Construct(v,e) == /\ p.owner="Absent" /\ Bound
 /\ LET r==NativeSample(EmptyNative,v,e)
        q==[p EXCEPT !.native=r.state,!.nativeReads=@+r.reads,
            !.owner=IF r.err="Ok" THEN "Direct" ELSE "Failed",!.construction=r.err]
    IN Record(q,v,r.err,r.err,"None")
NewSuccess == /\ Mode="Native" /\ PlatformSupported
 /\ \E i,n \in Ticks: Construct(Pair(i,n,1),"Ok")
NewFailure == /\ Mode="Native" /\ PlatformSupported
 /\ \E e \in NativeErrors: Construct(NoPair,e)
NewUnsupported == /\ Mode="Native" /\ ~PlatformSupported
 /\ Construct(NoPair,"Unsupported")

\* crates/tickwire-binance-clock/src/client.rs:33-72
MoveToClient == /\ Mode="Native" /\ p.owner="Direct"
 /\ \E d \in {1,2}: p'=[p EXCEPT !.owner="Client",!.client=EmptyClient(d),!.output=NoPair]
NewGeneric == /\ Mode="Generic" /\ p.owner="Absent"
 /\ p'=[p EXCEPT !.owner="Client",!.client=EmptyClient(1),!.construction="Generic"]

\* crates/tickwire-clock-sys/src/lib.rs:68-85
Direct(v,e) == /\ p.owner="Direct" /\ Bound
 /\ LET r==NativeSample(p.native,v,e)
        q==[p EXCEPT !.native=r.state,!.nativeReads=@+r.reads]
    IN Record(q,v,r.err,r.err,"None")
DirectEqual == /\ ~p.native.fault /\ Direct(p.native.last,"Ok")
DirectAdvance == /\ ~p.native.fault
 /\ \E i,n \in Ticks: LET v==Pair(i,n,1) IN
     /\ HealthyPair(v) /\ v#p.native.last /\ Direct(v,"Ok")
DirectInstantRollback == /\ ~p.native.fault
 /\ \E i,n \in Ticks: /\ i<p.native.last.i /\ n>=p.native.last.n /\ Direct(Pair(i,n,1),"Ok")
DirectNativeRollback == /\ ~p.native.fault
 /\ \E i,n \in Ticks: /\ n<p.native.last.n /\ Direct(Pair(i,n,1),"Ok")
DirectReadFailure == /\ ~p.native.fault /\ \E e \in NativeErrors: Direct(NoPair,e)
DirectSticky == /\ p.native.fault /\ Direct(NoPair,"Ok")

\* native provider is called only if client.fatal is absent; native guard may itself
\* already be faulted. Core failure does not short-circuit this provider call.
\* crates/tickwire-binance-clock/src/lib.rs:41-54
\* crates/tickwire-binance-clock/src/client.rs:74-107
NativeClient(v,e) == /\ Mode="Native" /\ p.owner="Client" /\ Bound /\ p.client.fatal="None"
 /\ LET nr==NativeSample(p.native,v,e)
        pe==IF nr.err="Ok" THEN "Ok" ELSE NativeError(nr.err)
        cr==ClientSample(p.client,v,pe)
        q==[p EXCEPT !.native=nr.state,!.nativeReads=@+nr.reads,
            !.client=cr.state,!.providerCalls=@+cr.reads]
    IN Record(q,v,cr.err,nr.err,pe)
ClientNativeSuccess == /\ ~p.native.fault
 /\ \E i,n \in Ticks: /\ HealthyPair(Pair(i,n,1)) /\ NativeClient(Pair(i,n,1),"Ok")
ClientNativeRollback == /\ ~p.native.fault
 /\ \E i,n \in Ticks: /\ ~HealthyPair(Pair(i,n,1)) /\ NativeClient(Pair(i,n,1),"Ok")
ClientNativeFailure == /\ ~p.native.fault /\ \E e \in NativeErrors: NativeClient(NoPair,e)
ClientNativeSticky == /\ p.native.fault /\ NativeClient(NoPair,"Ok")

\* crates/tickwire-binance-clock/src/client.rs:74-107
Generic(v,e) == /\ Mode="Generic" /\ p.owner="Client" /\ Bound /\ p.client.fatal="None"
 /\ LET cr==ClientSample(p.client,v,e)
        q==[p EXCEPT !.client=cr.state,!.providerCalls=@+cr.reads]
    IN Record(q,v,cr.err,"None",e)
\* Redundant result classifications keep distinct TLC action coverage entries;
\* every set below is already implied by ClientSample, not a new source guard.
GenericSuccess == /\ \E i,n \in Ticks: Generic(Pair(i,n,1),"Ok")
 /\ p'.err \in {"Ok","TimeBackwards","Domain","Faulted"}
GenericWrongDomain == /\ \E i,n \in Ticks: Generic(Pair(i,n,2),"Ok")
 /\ p'.err \in {"Domain","Faulted","TimeBackwards"}
GenericPermanent == /\ \E e \in Permanent: Generic(NoPair,e)
 /\ p'.err \in Permanent
GenericTemporary == /\ \E e \in Temporary: Generic(NoPair,e)
 /\ p'.err \in Temporary
ClientFatalSticky == /\ p.owner="Client" /\ Bound /\ p.client.fatal#"None"
 /\ LET cr==ClientSample(p.client,NoPair,"Ok") IN
    Record([p EXCEPT !.client=cr.state,!.providerCalls=@+cr.reads],NoPair,cr.err,"None","None")

NativeNext == NewSuccess \/ NewFailure \/ MoveToClient \/ DirectEqual \/ DirectAdvance
 \/ DirectInstantRollback \/ DirectNativeRollback \/ DirectReadFailure \/ DirectSticky
 \/ ClientNativeSuccess \/ ClientNativeRollback \/ ClientNativeFailure \/ ClientNativeSticky
 \/ ClientFatalSticky
GenericNext == NewGeneric \/ GenericSuccess \/ GenericWrongDomain \/ GenericPermanent
 \/ GenericTemporary \/ ClientFatalSticky
UnsupportedNext == NewUnsupported
SpecNative == Init /\ [][NativeNext]_vars
SpecGeneric == Init /\ [][GenericNext]_vars
SpecUnsupported == Init /\ [][UnsupportedNext]_vars

TypeOK == /\ p.owner \in {"Absent","Failed","Direct","Client"}
 /\ p.calls \in 0..MaxCalls /\ p.err \in Errors /\ p.client.fatal \in Permanent \cup {"None"}
 /\ p.native.fault \in BOOLEAN /\ p.client.core.fault \in {"None","Domain","TimeBackwards"}
 /\ p.nativeReads<=p.calls /\ p.providerCalls<=p.calls
ConstructBeforePublish == Mode#"Native" \/ p.owner \notin {"Direct","Client"}
 \/ (p.construction="Ok" /\ p.native.last#NoPair)
ConstructorFailureUnpublished == p.construction \notin NativeErrors \cup {"Unsupported"} \/ p.owner="Failed"
OrderedOutputs == p.orderOK
FixedDomainOutputs == p.domainOK
NativeFaultSticky == p.nativeStickyOK
ClientFaultSticky == p.clientStickyOK
CoreFaultSticky == p.coreStickyOK
NoReadAfterNativeFault == p.noNativeReadOK
NoProviderAfterClientFault == p.noProviderReadOK
NativeFailureKeepsLast == p.nativeLastOK
InstantUpdatedBeforeCore == p.instantOrderOK
TemporaryFailureNotFatal == p.temporaryOK
NoOutputAfterFatal == p.output=NoPair \/ (p.client.fatal="None" /\ p.client.core.fault="None" /\ ~p.native.fault)
=============================================================================
