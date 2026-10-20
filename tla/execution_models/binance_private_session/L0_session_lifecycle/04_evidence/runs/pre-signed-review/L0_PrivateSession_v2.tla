-------------------------- MODULE L0_PrivateSession_v2 --------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC
CONSTANTS Product, Logon, MaxEpoch, MaxRequests, Capacity, MaxEvents
\* var <-> code map: s is the sole PrivateClient owner, no concurrent reentry.
\* phase/epoch/planned/stop: crates/tickwire-binance-private/src/client.rs:52-84; crates/tickwire-binance-private/src/client.rs:294-327; crates/tickwire-binance-private/src/client.rs:527-541
\* p/hist: Pending + retained ControlOutcome/transport acceptance witness:
\* crates/tickwire-binance-private/src/client.rs:21-27; crates/tickwire-binance-private/src/client.rs:347-362; crates/tickwire-binance-private/src/client.rs:543-616; crates/tickwire-binance-private/src/client.rs:1126-1182; crates/tickwire-binance-private/src/client.rs:1309-1501
\* time/lease/renew/backoff/failures: ms projection of defaults and monotonic checked deadlines:
\* crates/tickwire-binance-private/src/session.rs:14-31
\* crates/tickwire-binance-private/src/client.rs:687-748; crates/tickwire-binance-private/src/client.rs:1183-1244
\* stream/ended/draining/events: crates/tickwire-binance-private/src/client.rs:28-46; crates/tickwire-binance-private/src/client.rs:798-896; crates/tickwire-binance-private/src/client.rs:1246-1306
\* call/gateBefore/gateSecond: synchronous first-output closure, never an awaitable permit:
\* crates/tickwire-binance-private/src/client.rs:1338-1455
\* gap/remoteUnknown: crates/tickwire-binance-private/src/client.rs:328-399
\* request freshness and original stamp: crates/tickwire-binance-private/src/request.rs:65-89; crates/tickwire-binance-private/src/request.rs:318-339
\* accepted-byte monotonicity/first-output closure: crates/tickwire-transport/src/connection.rs:178-237
\* Reset/Reject/Fail/StopOnFailure/Complete/RevokeEOF are expression helpers for cited APIs.
\* received/emitted are bounded input/observation histories, not extra production queues.
\* Actual key lifetime: crates/tickwire-binance-private/src/client.rs:14
ReqMs == 5000
LeaseMs == 3600000
RenewMs == 1800000
BackoffMs == 100
None == "None"
EmptyP == [id |-> 0, epoch |-> 0, op |-> None, start |-> 0, deadline |-> 0, accepted |-> 0]
EmptyH == [epoch |-> 0, op |-> None, accepted |-> 0, outcome |-> None,
           before |-> 0, after |-> 0, deadline |-> 0, allowed |-> FALSE, lateCallback |-> FALSE]
VARIABLE s
vars == <<s>>
Active(x) == x.phase \notin {"Off", "Backoff", "Stopped"}
Fresh(x) == (x.p.id = 0 \/ x.time < x.p.deadline) /\ (x.lease = 0 \/ x.time < x.lease)
Signed(op) == op \in {"Logon", "SubscribeSignature"}
Reset(x) == [x EXCEPT !.stream=FALSE, !.planned=None, !.sub=FALSE,
                     !.lease=0, !.renew=0, !.events= <<>>, !.draining=FALSE, !.ended=FALSE]
Reject(x) == IF x.p.id=0 THEN x ELSE
 [x EXCEPT !.hist[x.p.id].outcome=IF x.p.accepted=0 THEN "NoBytes" ELSE "May",
           !.remoteUnknown=@ \/ (x.p.op="Delete" /\ x.p.accepted>0), !.p=EmptyP]
Fail(x) == LET y==Reset(Reject(x)) IN
 [y EXCEPT !.phase=IF y.stop THEN "Stopped" ELSE "Backoff", !.gap=@+1,
           !.backoff=IF y.stop THEN y.backoff ELSE y.time+BackoffMs*(2^y.failures),
           !.failures=IF y.stop THEN y.failures ELSE y.failures+1, !.call="Idle"]
StopOnFailure(x) == Fail([x EXCEPT !.stop=TRUE])
Prepare(x,op) == LET i==x.serial+1 IN
 [x EXCEPT !.serial=i, !.planned=None,
   !.p=[id |-> i, epoch |-> x.epoch, op |-> op, start |-> x.time, deadline |-> x.time+ReqMs, accepted |-> 0],
   !.hist[i]=[EmptyH EXCEPT !.epoch=x.epoch, !.op=op, !.deadline=x.time+ReqMs]]
Complete(x) == [x EXCEPT !.hist[x.p.id].outcome="Confirmed", !.p=EmptyP]
RevokeEOF(x) == LET y==Reject(x) IN
 [y EXCEPT !.phase="Backoff", !.lease=0, !.renew=0, !.planned=None,
           !.draining=TRUE, !.ended=TRUE, !.gap=@+1]
Init == s = [phase |-> "Off", epoch |-> 0, serial |-> 0, planned |-> None,
 p |-> EmptyP, hist |-> [i \in 1..MaxRequests |-> EmptyH], time |-> 0,
 lease |-> 0, renew |-> 0, backoff |-> 0, stream |-> FALSE, sub |-> FALSE,
 ended |-> FALSE, draining |-> FALSE, stop |-> FALSE, remoteUnknown |-> FALSE,
 call |-> "Idle", gateBefore |-> 0, gateSecond |-> 0, events |-> <<>>, received |-> 0,
 emitted |-> <<>>, gap |-> 1, failures |-> 0]
\* begin/lane: crates/tickwire-binance-private/src/client.rs:447-541; input bound does not replace u64 MAX.
Connect == /\ s.call="Idle" /\ s.phase \in {"Off","Backoff"} /\ ~s.draining
 /\ ~s.stop /\ s.epoch<MaxEpoch
 /\ s'=[s EXCEPT !.epoch=@+1, !.time=IF s.time<s.backoff THEN s.backoff ELSE s.time,
   !.phase=IF Product="Spot" THEN "Connecting" ELSE "CreatingKey",
   !.stream=Product="Spot", !.planned=IF Product="Spot" THEN None ELSE "Create"]
\* Successful actual HTTP Upgrade, crates/tickwire-binance-private/src/client.rs:798-846.
Upgrade == /\ s.call="Idle" /\ s.phase="Connecting" /\ Fresh(s)
 /\ s'=[s EXCEPT !.stream=TRUE, !.failures=IF Product="Usdm" THEN 0 ELSE @,
   !.phase=IF Product="Spot" THEN IF Logon THEN "Auth" ELSE "Sub" ELSE "Live",
   !.planned=IF Product="Spot" THEN IF Logon THEN "Logon" ELSE "SubscribeSignature" ELSE None]
\* Immutable preparation, crates/tickwire-binance-private/src/client.rs:543-616. Crypto/bytes validated by Rust tests.
Plan == /\ s.call="Idle" /\ s.planned#None /\ s.p.id=0 /\ Fresh(s)
 /\ s.serial<MaxRequests /\ s'=Prepare(s,s.planned)
\* First clock check and entry into synchronous permit callback, crates/tickwire-binance-private/src/client.rs:1338-1455.
GateStart == /\ s.call="Idle" /\ s.p.id>0 /\ s.p.accepted=0 /\ Fresh(s)
 /\ s'=[s EXCEPT !.call="Gate", !.gateBefore=s.time]
\* All callbacks complete; retain the second sample separately from the final one.
\* crates/tickwire-binance-private/src/client.rs:1381-1448
GateCallbacksReady == /\ s.call="Gate" /\ s'=[s EXCEPT !.call="Final", !.gateSecond=s.time]
\* DeliveryBudget advances suspend-aware Mono to the local deadline while the
\* old second sample/Instant and the signature window still pass. No owner reentry.
GateBudgetExpires == /\ s.call="Gate" /\ s.time<s.p.deadline
 /\ s'=[s EXCEPT !.call="Final", !.gateSecond=s.time, !.time=s.p.deadline,
                  !.hist[s.p.id].lateCallback=TRUE]
\* Final sample checks the actual current Mono after every callback; pure checks
\* then return directly to transport's first write with the original signed stamp.
FinalSampleExpired == /\ s.call="Final" /\ s.time>=s.p.deadline /\ s'=Fail(s)
\* All checks approved; actual first TLS acceptance can be partial or complete.
FirstAccept(n) == /\ s.call="Final" /\ n \in 1..2 /\ Fresh(s)
 /\ s'=[s EXCEPT !.call="Idle", !.p.accepted=n, !.hist[s.p.id].accepted=n,
   !.hist[s.p.id].before=s.gateBefore, !.hist[s.p.id].after=s.time,
   !.hist[s.p.id].allowed=TRUE]
\* WouldBlock accepts zero; next poll invokes a new synchronous permit.
GateBlocked == /\ s.call="Final" /\ Fresh(s) /\ s'=[s EXCEPT !.call="Idle"]
\* Permit/interval rejection, or clock domain invalidation inside callback.
GateReject == /\ s.call="Gate" /\ s'=Fail(s)
GateDomainFault == /\ s.call \in {"Gate","Final"} /\ s'=StopOnFailure(s)
\* Callback consumes enough wall time to reach the exact original deadline.
GateExpires == /\ s.call="Gate" /\ s'=Fail([s EXCEPT !.time=s.p.deadline])
\* Continuation never converts accepted bytes to NoBytes. crates/tickwire-binance-private/src/client.rs:1338-1455.
Continue == /\ s.call="Idle" /\ s.p.accepted=1 /\ Fresh(s)
 /\ s'=[s EXCEPT !.p.accepted=2, !.hist[s.p.id].accepted=2]
\* Deadline checked before processing replies, crates/tickwire-binance-private/src/client.rs:701-715; crates/tickwire-binance-private/src/client.rs:786-797.
RequestExpires == /\ s.call="Idle" /\ s.p.id>0
 /\ s'=Fail([s EXCEPT !.time=s.p.deadline])
LeaseExpires == /\ s.call="Idle" /\ s.lease>0
 /\ s'=Fail([s EXCEPT !.time=s.lease])
\* Wrong/stale WS IDs are ignored before Pending is consumed, crates/tickwire-binance-private/src/client.rs:1005-1023.
OldReply == /\ s.call="Idle" /\ Product="Spot" /\ s.p.id>0 /\ Fresh(s)
 /\ UNCHANGED s
\* Exact ID but incomplete request fails while retaining acceptance evidence.
PrematureReply == /\ s.call="Idle" /\ s.p.id>0 /\ s.p.accepted<2 /\ Fresh(s)
 /\ s'=Fail(s)
\* Successful exact reply: crates/tickwire-binance-private/src/client.rs:1044-1108; crates/tickwire-binance-private/src/client.rs:1208-1244.
ReplyValue(x) == CASE x.p.op="Logon" -> [x EXCEPT !.phase="Sub", !.planned="Subscribe"]
 [] x.p.op \in {"Subscribe","SubscribeSignature"} -> [x EXCEPT !.phase="Live", !.sub=TRUE, !.failures=0]
 [] x.p.op="Create" -> [x EXCEPT !.phase="Connecting", !.lease=x.p.start+LeaseMs, !.renew=x.p.start+RenewMs]
 [] x.p.op="Renew" -> [x EXCEPT !.lease=x.p.start+LeaseMs, !.renew=x.p.start+RenewMs]
 [] x.p.op="Unsubscribe" -> [x EXCEPT !.sub=FALSE,
      !.phase=IF Logon THEN "Closing" ELSE "Stopped", !.stream=Logon,
      !.planned=IF Logon THEN "Logout" ELSE None]
 [] x.p.op="Logout" -> [x EXCEPT !.phase="Stopped", !.stream=FALSE]
 [] x.p.op="Delete" -> [Reset(x) EXCEPT !.phase="Stopped"]
ReplySuccess(delay) == /\ s.call="Idle" /\ s.p.id>0 /\ s.p.accepted=2
 /\ delay \in 0..1
 /\ LET x==[s EXCEPT !.time=@+delay] IN Fresh(x) /\ s'=Complete(ReplyValue(x))
\* Subscription ACK and EOF share one read; revoke before publishing visible health.
ReplyEOF == /\ s.call="Idle" /\ s.p.op \in {"Subscribe","SubscribeSignature"}
 /\ s.p.accepted=2 /\ Fresh(s) /\ s'=RevokeEOF(Complete(ReplyValue(s)))
\* Deterministic remote reject, crates/tickwire-binance-private/src/client.rs:1143-1182. Identity/parse failure uses Fail.
\* Unknown preserves possible remote effect.
ReplyReject == /\ s.call="Idle" /\ s.p.id>0 /\ s.p.accepted=2 /\ Fresh(s)
 /\ s'=Fail([s EXCEPT !.hist[s.p.id].outcome="Rejected", !.p=EmptyP])
ReplyUnknown == /\ s.call="Idle" /\ s.p.id>0 /\ s.p.accepted=2 /\ Fresh(s)
 /\ s'=Fail(s)
\* Renew planned from original request start; crates/tickwire-binance-private/src/client.rs:739-750; crates/tickwire-binance-private/src/client.rs:1211-1244.
Renew == /\ s.call="Idle" /\ Product="Usdm" /\ s.phase="Live" /\ s.p.id=0
 /\ s.serial<MaxRequests /\ s.renew<s.lease
 /\ s'=Prepare([s EXCEPT !.time=s.renew],"Renew")
\* Explicit close keeps sole key-management ownership, crates/tickwire-binance-private/src/client.rs:294-327.
CloseRemote == /\ s.call="Idle" /\ s.phase="Live" /\ s.p.id=0 /\ s.planned=None
 /\ Fresh(s)
 /\ s'=[s EXCEPT !.stop=TRUE, !.phase="Closing", !.gap=@+1,
   !.events= <<>>, !.stream=Product="Spot",
   !.planned=IF Product="Spot" THEN "Unsubscribe" ELSE "Delete"]
Disconnect == /\ s.call="Idle" /\ s.phase#"Stopped" /\ ~s.stop
 /\ s'=[Fail([s EXCEPT !.stop=TRUE]) EXCEPT !.phase="Stopped"]
\* Network failure and heartbeat/rotation withdraw live status.
NetworkFailure == /\ s.call="Idle" /\ Active(s) /\ s'=Fail(s)
DomainFault == /\ s.call="Idle" /\ s.epoch>0 /\ ~s.stop /\ s'=StopOnFailure(s)
\* Rejected decoded-event delivery is permanently stopped, not retryable.
\* crates/tickwire-binance-private/src/client.rs:631-637; crates/tickwire-binance-private/src/client.rs:1289-1301
SinkFailure == /\ s.call="Idle" /\ (s.phase="Live" \/ s.draining)
 /\ Len(s.events)>0 /\ Head(s.events).valid /\ (Product="Usdm" \/ s.sub)
 /\ s'=StopOnFailure(s)
\* Bounded complete incoming events, crates/tickwire-binance-private/src/client.rs:1109-1125. IDs checked at delivery.
QueueEvent(valid) == /\ s.call="Idle" /\ s.phase \in {"Sub","Live"}
 /\ s.received<MaxEvents /\ Len(s.events)<Capacity /\ Fresh(s) /\ valid \in BOOLEAN
 /\ s'=[s EXCEPT !.received=@+1,
   !.events=Append(@,[epoch |-> s.epoch, valid |-> valid, value |-> s.received+1, arrival |-> s.time])]
Overflow == /\ s.call="Idle" /\ s.phase \in {"Sub","Live"}
 /\ Len(s.events)=Capacity /\ s.received<MaxEvents /\ Fresh(s) /\ s'=Fail(s)
\* EOF withdraws health, retains already complete old-epoch events, crates/tickwire-binance-private/src/client.rs:876-896.
StreamEOF == /\ s.call="Idle" /\ s.phase="Live" /\ Fresh(s) /\ s'=RevokeEOF(s)
\* Delivery never rewrites the old SessionContext. crates/tickwire-binance-private/src/client.rs:1246-1306.
Deliver == /\ s.call="Idle" /\ (s.phase="Live" \/ s.draining) /\ Len(s.events)>0
 /\ Head(s.events).valid /\ (Product="Usdm" \/ s.sub)
 /\ s'=[s EXCEPT !.events=Tail(@),
   !.emitted=Append(@,[event |-> Head(s.events), context |-> s.epoch, gap |-> s.draining])]
BadEvent == /\ s.call="Idle" /\ (s.phase="Live" \/ s.draining) /\ Len(s.events)>0
 /\ ~Head(s.events).valid /\ s'=Fail(s)
DrainDone == /\ s.call="Idle" /\ s.draining /\ Len(s.events)=0
 /\ s'=[Reset(s) EXCEPT !.backoff=s.time+BackoffMs]
BaseNext == Connect \/ Upgrade \/ Plan \/ GateStart \/ (\E n \in 1..2: FirstAccept(n))
 \/ GateCallbacksReady \/ GateBudgetExpires \/ FinalSampleExpired
 \/ GateBlocked \/ GateReject \/ GateDomainFault \/ GateExpires \/ Continue
 \/ RequestExpires \/ PrematureReply \/ (\E delay \in 0..1: ReplySuccess(delay)) \/ ReplyReject
 \/ ReplyUnknown \/ CloseRemote \/ Disconnect \/ NetworkFailure \/ DomainFault \/ SinkFailure
 \/ (\E valid \in BOOLEAN: QueueEvent(valid)) \/ Overflow \/ StreamEOF \/ Deliver \/ BadEvent \/ DrainDone
SpotNext == BaseNext \/ OldReply \/ ReplyEOF
UsdmNext == BaseNext \/ Renew \/ LeaseExpires
Next == IF Product="Spot" THEN SpotNext ELSE UsdmNext
TypeOK == /\ s.epoch \in 0..MaxEpoch /\ s.serial \in 0..MaxRequests
 /\ s.phase \in {"Off","Connecting","Auth","Sub","CreatingKey","Live","Backoff","Closing","Stopped"}
 /\ s.call \in {"Idle","Gate","Final"} /\ Len(s.events)<=Capacity /\ s.received<=MaxEvents
 /\ s.p.accepted \in 0..2 /\ s.time<100000000
NoBytesNeverAccepted == \A i \in 1..s.serial: s.hist[i].outcome="NoBytes" => s.hist[i].accepted=0
ConfirmedComplete == \A i \in 1..s.serial: s.hist[i].outcome="Confirmed" => s.hist[i].accepted=2
FirstOutputGuard == \A i \in 1..s.serial: s.hist[i].accepted>0 =>
 /\ s.hist[i].allowed /\ s.hist[i].before<=s.hist[i].after /\ s.hist[i].after<s.hist[i].deadline
ExpiredCallbackNeverAccepted == \A i \in 1..s.serial:
 s.hist[i].lateCallback => s.hist[i].accepted=0 /\ s.hist[i].outcome \in {None,"NoBytes"}
PendingIdentity == s.p.id>0 => s.p.epoch=s.epoch /\ s.hist[s.p.id].epoch=s.epoch
NoKnownEOFLive == s.phase="Live" => ~s.ended /\ ~s.draining /\ s.stream
LiveIdentity == s.phase="Live" => IF Product="Spot" THEN s.sub ELSE s.lease>s.time
NoRebindDelivery == \A i \in 1..Len(s.emitted): s.emitted[i].event.epoch=s.emitted[i].context
ValidDelivery == \A i \in 1..Len(s.emitted): s.emitted[i].event.valid
UnknownDeleteKeepsOwner == s.remoteUnknown => s.stop /\ s.phase="Stopped"
DrainBlocksReconnect == s.draining => s.phase="Backoff" /\ s.ended
StoppedNoPending == s.phase="Stopped" => s.p.id=0
LeaseUsesRequestStart == s.lease>0 => \E i \in 1..s.serial:
 /\ s.hist[i].outcome="Confirmed" /\ s.hist[i].epoch=s.epoch
 /\ s.hist[i].op \in {"Create","Renew"}
 /\ s.lease+ReqMs=s.hist[i].deadline+LeaseMs
LiveConfirmed == s.phase="Live" => \E i \in 1..s.serial:
 /\ s.hist[i].outcome="Confirmed" /\ s.hist[i].epoch=s.epoch
 /\ s.hist[i].op \in IF Product="Spot" THEN {"Subscribe","SubscribeSignature"} ELSE {"Create"}
DeliveryOrder == \A i,j \in 1..Len(s.emitted):
 i<j => s.emitted[i].event.value<s.emitted[j].event.value
Spec == Init /\ [][Next]_vars
=============================================================================
