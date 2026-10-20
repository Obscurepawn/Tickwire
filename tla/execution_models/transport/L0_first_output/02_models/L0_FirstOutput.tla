--------------------------- MODULE L0_FirstOutput ---------------------------
EXTENDS Naturals, FiniteSets, TLC
\* var <-> code map; exclusive Connection calls with finite IO outcomes.
\* conn: socket/connected/is_handshaking, crates/tickwire-transport/src/connection.rs:13-21.
\* current/nextId/state/accepted/deadline: SendSlot and queue, crates/tickwire-transport/src/connection.rs:9-12.
\* wire/buffered: write_tls local-output witness, crates/tickwire-transport/src/connection.rs:318-343.
\* gate/authorized: synchronous closure result at first writer.write, crates/tickwire-transport/src/connection.rs:178-227.
\* now: monotonic now.max(Instant::now()), crates/tickwire-transport/src/connection.rs:152-197.
\* tlsBlocked: TLS output capacity from set_buffer_limit, crates/tickwire-transport/src/connection.rs:43-52.
\* Faults: zero/partial write, connection/TLS failure, expiration. No request retry/reconnect.
CONSTANTS TLS, Bytes, Capacity, MaxSends, MaxTime
ASSUME /\ Bytes >= 2 /\ Capacity >= 1 /\ MaxSends >= 1 /\ MaxTime >= 2
VARIABLES conn,current,nextId,state,accepted,wire,buffered,deadline,now,gate,authorized,tlsBlocked
vars == <<conn,current,nextId,state,accepted,wire,buffered,deadline,now,gate,authorized,tlsBlocked>>
Ids == 1..MaxSends
Terminal == {"NoBytes","Failed","Flushed"}
Active == current # 0 /\ state[current] \in {"Queued","MayHaveSent"}
Live == conn # "Closed"
Fresh == Active /\ now < deadline[current]
Init ==
 /\ conn="Connecting" /\ current=0 /\ nextId=1
 /\ state=[i \in Ids |-> "Unused"] /\ accepted=[i \in Ids |-> 0]
 /\ wire=[i \in Ids |-> 0] /\ deadline=[i \in Ids |-> 0]
 /\ authorized=[i \in Ids |-> FALSE]
 /\ buffered=0 /\ now=0 /\ gate=TRUE /\ tlsBlocked=FALSE
\* queue only owns the immutable prepared bytes. crates/tickwire-transport/src/connection.rs:90-113
Queue(d) ==
 /\ Live /\ current=0 /\ nextId<=MaxSends /\ d \in 1..MaxTime
 /\ current'=nextId /\ nextId'=nextId+1
 /\ state'=[state EXCEPT ![nextId]="Queued"]
 /\ deadline'=[deadline EXCEPT ![nextId]=d]
 /\ UNCHANGED <<conn,accepted,wire,buffered,now,gate,authorized,tlsBlocked>>
\* drive take_error + peer_addr, crates/tickwire-transport/src/connection.rs:281-310
Connect ==
 /\ conn="Connecting" /\ conn'=IF TLS THEN "Handshake" ELSE "Ready"
 /\ UNCHANGED <<current,nextId,state,accepted,wire,buffered,deadline,now,gate,authorized,tlsBlocked>>
\* rustls process_new_packets permits is_ready, crates/tickwire-transport/src/connection.rs:373-380
Handshake(blocked) ==
 /\ TLS /\ conn="Handshake" /\ conn'="Ready" /\ tlsBlocked'=blocked
 /\ UNCHANGED <<current,nextId,state,accepted,wire,buffered,deadline,now,gate,authorized>>
\* drive writes existing handshake ciphertext, never the prepared application. crates/tickwire-transport/src/connection.rs:322-343
FlushHandshake ==
 /\ TLS /\ Live /\ tlsBlocked /\ tlsBlocked'=FALSE
 /\ UNCHANGED <<conn,current,nextId,state,accepted,wire,buffered,deadline,now,gate,authorized>>
\* Zero accept and non-ready paths retain Queued/MayHaveSent. crates/tickwire-transport/src/connection.rs:183-187,213-223
TryPending ==
 /\ Fresh /\ conn # "Ready" /\ UNCHANGED vars
TryZero ==
 /\ Fresh /\ conn="Ready" /\ (accepted[current]>0 \/ gate)
 /\ UNCHANGED vars
\* Denied synchronous first-output closure. crates/tickwire-transport/src/connection.rs:188-192
Deny ==
 /\ Fresh /\ conn="Ready" /\ accepted[current]=0 /\ ~gate
 /\ state'=[state EXCEPT ![current]="NoBytes"]
 /\ UNCHANGED <<conn,current,nextId,accepted,wire,buffered,deadline,now,gate,authorized,tlsBlocked>>
\* Gate duration can cross the deadline; the second check rejects before writer. crates/tickwire-transport/src/connection.rs:193-197
ExpireInGate ==
 /\ Fresh /\ conn="Ready" /\ accepted[current]=0 /\ gate
 /\ now'=deadline[current] /\ state'=[state EXCEPT ![current]="NoBytes"]
 /\ UNCHANGED <<conn,current,nextId,accepted,wire,buffered,deadline,gate,authorized,tlsBlocked>>
\* First accepted bytes; plaintext count rises before later IO failure. crates/tickwire-transport/src/connection.rs:199-227
FirstAccept(n) ==
 /\ Fresh /\ conn="Ready" /\ state[current]="Queued" /\ gate
 /\ n \in 1..Bytes /\ (~TLS \/ (~tlsBlocked /\ n<=Capacity-buffered))
 /\ accepted'=[accepted EXCEPT ![current]=n]
 /\ authorized'=[authorized EXCEPT ![current]=TRUE]
 /\ wire'=IF TLS THEN wire ELSE [wire EXCEPT ![current]=n]
 /\ buffered'=IF TLS THEN buffered+n ELSE 0
 /\ state'=[state EXCEPT ![current]=IF ~TLS /\ n=Bytes THEN "Flushed" ELSE "MayHaveSent"]
 /\ UNCHANGED <<conn,current,nextId,deadline,now,gate,tlsBlocked>>
\* Continuation is already uncertain; no fresh gate is required. crates/tickwire-transport/src/connection.rs:188-227
Continue(n) ==
 /\ Fresh /\ conn="Ready" /\ state[current]="MayHaveSent"
 /\ n \in 1..(Bytes-accepted[current]) /\ (~TLS \/ (~tlsBlocked /\ n<=Capacity-buffered))
 /\ accepted'=[accepted EXCEPT ![current]=@+n]
 /\ wire'=IF TLS THEN wire ELSE [wire EXCEPT ![current]=@+n]
 /\ buffered'=IF TLS THEN buffered+n ELSE 0
 /\ state'=[state EXCEPT ![current]=IF ~TLS /\ accepted[current]+n=Bytes THEN "Flushed" ELSE "MayHaveSent"]
 /\ UNCHANGED <<conn,current,nextId,deadline,now,gate,authorized,tlsBlocked>>
\* drive expiry precedes flushing; completed output recorded before receive EOF. crates/tickwire-transport/src/connection.rs:269-279,322-343
Flush(n) ==
 /\ TLS /\ Fresh /\ conn="Ready" /\ n \in 1..buffered
 /\ wire'=[wire EXCEPT ![current]=@+n] /\ buffered'=buffered-n
 /\ state'=[state EXCEPT ![current]=IF wire[current]+n=Bytes THEN "Flushed" ELSE @]
 /\ UNCHANGED <<conn,current,nextId,accepted,deadline,now,gate,authorized,tlsBlocked>>
\* expire zero acceptance is permanent but leaves a usable connection. crates/tickwire-transport/src/connection.rs:152-163
ExpireQueued ==
 /\ Active /\ now>=deadline[current] /\ accepted[current]=0
 /\ state'=[state EXCEPT ![current]="NoBytes"]
 /\ UNCHANGED <<conn,current,nextId,accepted,wire,buffered,deadline,now,gate,authorized,tlsBlocked>>
ExpireAccepted ==
 /\ Active /\ now>=deadline[current] /\ accepted[current]>0
 /\ conn'="Closed" /\ state'=[state EXCEPT ![current]="Failed"] /\ buffered'=0 /\ tlsBlocked'=FALSE
 /\ UNCHANGED <<current,nextId,accepted,wire,deadline,now,gate,authorized>>
\* cancel closes an accepted send and cannot manufacture NoBytes. crates/tickwire-transport/src/connection.rs:164-177
CancelQueued ==
 /\ current # 0 /\ state[current]="Queued"
 /\ state'=[state EXCEPT ![current]="NoBytes"]
 /\ UNCHANGED <<conn,current,nextId,accepted,wire,buffered,deadline,now,gate,authorized,tlsBlocked>>
CancelAccepted ==
 /\ Live /\ current # 0 /\ accepted[current]>0
 /\ conn'="Closed" /\ buffered'=0 /\ tlsBlocked'=FALSE
 /\ state'=[state EXCEPT ![current]=IF @="MayHaveSent" THEN "Failed" ELSE @]
 /\ UNCHANGED <<current,nextId,accepted,wire,deadline,now,gate,authorized>>
\* socket/TLS failure and connect timeout share fail(). crates/tickwire-transport/src/connection.rs:134-151
FailConnection ==
 /\ Live /\ conn'="Closed" /\ buffered'=0 /\ tlsBlocked'=FALSE
 /\ state'=IF Active THEN [state EXCEPT ![current]=IF accepted[current]=0 THEN "NoBytes" ELSE "Failed"] ELSE state
 /\ UNCHANGED <<current,nextId,accepted,wire,deadline,now,gate,authorized>>
\* terminal report collection leaves evidence in this model's history. crates/tickwire-transport/src/connection.rs:119-133
Take ==
 /\ current # 0 /\ state[current] \in Terminal /\ current'=0
 /\ UNCHANGED <<conn,nextId,state,accepted,wire,buffered,deadline,now,gate,authorized,tlsBlocked>>
\* Environment: current authorization may change between exclusive calls; Instant is monotonic.
ToggleGate == gate'=~gate /\ UNCHANGED <<conn,current,nextId,state,accepted,wire,buffered,deadline,now,authorized,tlsBlocked>>
Tick == now<MaxTime /\ now'=now+1 /\ UNCHANGED <<conn,current,nextId,state,accepted,wire,buffered,deadline,gate,authorized,tlsBlocked>>
NextBase == (\E d \in 1..MaxTime:Queue(d)) \/ Connect \/ TryPending \/ TryZero \/ Deny \/ ExpireInGate
 \/ (\E n \in 1..Bytes: FirstAccept(n) \/ Continue(n))
 \/ ExpireQueued \/ ExpireAccepted \/ CancelQueued \/ CancelAccepted \/ FailConnection \/ Take \/ ToggleGate \/ Tick
NextTLS == NextBase \/ (\E blocked \in BOOLEAN:Handshake(blocked)) \/ FlushHandshake \/ (\E n \in 1..Capacity:Flush(n))
NextPlain == NextBase
SpecTLS == Init /\ [][NextTLS]_vars
SpecPlain == Init /\ [][NextPlain]_vars
TypeOK == /\ conn \in {"Connecting","Handshake","Ready","Closed"} /\ current \in 0..MaxSends
 /\ nextId \in 1..(MaxSends+1) /\ state \in [Ids->{"Unused","Queued","MayHaveSent","NoBytes","Failed","Flushed"}]
 /\ accepted \in [Ids->0..Bytes] /\ wire \in [Ids->0..Bytes] /\ buffered \in 0..Capacity
 /\ deadline \in [Ids->0..MaxTime] /\ now \in 0..MaxTime /\ gate \in BOOLEAN
 /\ authorized \in [Ids->BOOLEAN] /\ tlsBlocked \in BOOLEAN
NoBytesTruth == \A i \in Ids: state[i] \in {"NoBytes","Queued","Unused"} => accepted[i]=0 /\ wire[i]=0
AcceptedEvidence == \A i \in Ids: accepted[i]>0 => authorized[i] /\ state[i] \in {"MayHaveSent","Failed","Flushed"}
LocalOutput == \A i \in Ids: wire[i]<=accepted[i] /\ (state[i]="Flushed" => wire[i]=Bytes /\ accepted[i]=Bytes)
BufferOwnership == /\ (buffered>0 => Live /\ current # 0 /\ state[current]="MayHaveSent")
 /\ (TLS /\ Active /\ Live => buffered=accepted[current]-wire[current])
PlainImmediate == ~TLS => \A i \in Ids:wire[i]=accepted[i]
=============================================================================
