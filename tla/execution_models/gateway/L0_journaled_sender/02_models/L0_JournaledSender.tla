------------------------- MODULE L0_JournaledSender -------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Intents, Bytes, AcceptChunk, MaxCrashes
ASSUME /\ Intents = 1..Cardinality(Intents) /\ Intents # {}
       /\ Bytes > 0 /\ AcceptChunk > 0 /\ MaxCrashes > 0

\* var <-> code map (all fields are in p):
\* phase/current/resume: serial &mut API program counter, not concurrent callers.
\*   crates/tickwire-gateway/src/lib.rs:254-329
\*   crates/tickwire-gateway/src/lib.rs:330-379
\*   crates/tickwire-gateway/src/lib.rs:449-459
\* status: entries[*].status; pending: Option<(intent, send_id)>.
\*   crates/tickwire-gateway/src/lib.rs:113-129
\* frozen: freeze_unknown preserves Aborted, rewrites all other entries Unknown.
\*   crates/tickwire-gateway/src/lib.rs:461-468
\* closure: verified synchronized command closure, not risk algorithm correctness.
\*   crates/tickwire-gateway/src/lib.rs:288-303
\* durableIntent/durableAbort: complete recoverable WAL event witnesses.
\*   crates/tickwire-business-journal/src/lib.rs:304-328
\* accepted/wire: app-byte prefixes accepted by TLS / flushed to local socket.
\*   crates/tickwire-transport/src/connection.rs:199-236
\* send/ready/conn: private transport send state, handshake readiness, live socket.
\*   crates/tickwire-transport/src/connection.rs:90-118
\*   crates/tickwire-transport/src/connection.rs:134-162
\* gate: conjunction of EVERY permits operand (including trusted Gate.now).
\*   crates/tickwire-gateway/src/lib.rs:531-543
\* fresh: per-command Instant deadline check, distinct from gate's trusted clock.
\*   crates/tickwire-transport/src/connection.rs:152-162
\*   crates/tickwire-transport/src/connection.rs:183-197
\* enqueues/firstGood: historical witnesses for no requeue and fresh first gate.
\*   crates/tickwire-gateway/src/lib.rs:260-267
\*   crates/tickwire-gateway/src/lib.rs:317-320
\*   crates/tickwire-gateway/src/lib.rs:345-349
\* crashes: fault-injection bound; process can stop between sync and publication.
\*   crates/tickwire-gateway/src/lib.rs:170-252
\*   crates/tickwire-gateway/src/lib.rs:303-320
\*   crates/tickwire-gateway/src/lib.rs:451-458
\* FAULTS: blocked/partial network IO, disconnect, failed sync with complete
\* recoverable tail or lost tail, crash/recover, failed recovery validation.
\* NOT MODELED: provider risk/account algorithms, command-to-signature mapping,
\* TLS cryptography, file-system internals, ACK/fills, recovery reactivation.

VARIABLE p
vars == <<p>>
States == {"None", "Queued", "Sending", "Awaiting", "Unknown", "Aborted"}
Phases == {"Boundary", "SyncIntent", "Queue", "Drive", "Settle", "Try",
           "EndPoll", "SyncAbort", "PublishAbort", "Crashed", "Rejected"}
EmptyStatus == [i \in Intents |-> "None"]
Unknown(s) == [i \in Intents |-> IF s[i] \in {"None", "Aborted"}
                                      THEN s[i] ELSE "Unknown"]
Present == {i \in Intents : p.status[i] # "None"}
CanMutate == p.phase = "Boundary" /\ ~p.frozen
HasPending == p.pending \in Intents
LiveSend == p.send \in {"Queued", "Accepted"}
CanDrive == p.phase = "Drive" /\ p.conn
DriveFresh == IF HasPending THEN p.fresh[p.pending] ELSE TRUE
NBytes(i) == 1..(IF Bytes - p.accepted[i] < AcceptChunk
                THEN Bytes - p.accepted[i] ELSE AcceptChunk)
\* Successful constructor returns an empty sender. crates/tickwire-gateway/src/lib.rs:132-168
Init == p = [phase |-> "Boundary", current |-> 0, resume |-> "Boundary",
             status |-> EmptyStatus, pending |-> 0, frozen |-> FALSE,
             closure |-> {}, durableIntent |-> {}, durableAbort |-> {},
             accepted |-> [i \in Intents |-> 0], wire |-> [i \in Intents |-> 0],
             enqueues |-> [i \in Intents |-> 0], firstGood |-> {},
             send |-> "Idle", ready |-> FALSE, conn |-> TRUE, driveOk |-> TRUE,
             gate |-> TRUE, fresh |-> [i \in Intents |-> TRUE], crashes |-> 0]

\* Binding/encoding/authorization reject before WAL and queue. Invalid requests
\* are inputs; they do not mutate the sender. crates/tickwire-gateway/src/lib.rs:277-301
RejectPreparation == /\ CanMutate /\ ~HasPending /\ Present # Intents
                     /\ UNCHANGED p
\* Valid complete command/route/digest and verified closure; external provider
\* result is trusted, matching crates/tickwire-gateway/src/lib.rs:277-303 and crates/tickwire-business-journal/src/lib.rs:247-251.
Prepare(i) == /\ CanMutate /\ ~HasPending /\ i \notin Present
              /\ p' = [p EXCEPT !.phase = "SyncIntent", !.current = i,
                               !.closure = @ \cup {i}]
\* The append call is in progress; successful return is synchronized. A failed
\* call may have left a complete recoverable tail. crates/tickwire-business-journal/src/lib.rs:311-328
IntentSyncOk == /\ p.phase = "SyncIntent"
                /\ p' = [p EXCEPT !.durableIntent = @ \cup {p.current},
                                 !.phase = "Queue"]
IntentSyncFail(keep) ==
    /\ p.phase = "SyncIntent"
    /\ p' = [p EXCEPT !.durableIntent = IF keep THEN @ \cup {p.current} ELSE @,
                      !.status = Unknown(@), !.frozen = TRUE,
                      !.phase = "Boundary", !.current = 0]
\* No API interleaves between WAL return and private queue. crates/tickwire-gateway/src/lib.rs:307-326
QueueOk == /\ p.phase = "Queue" /\ p.conn
           /\ p' = [p EXCEPT !.status[p.current] = "Queued", !.pending = p.current,
                            !.enqueues[p.current] = @ + 1, !.send = "Queued",
                            !.phase = "Boundary", !.current = 0]
\* queue closed-socket error is zero acceptance, then freeze + durable abort.
\* crates/tickwire-gateway/src/lib.rs:322-326
\* crates/tickwire-transport/src/connection.rs:90-101
QueueFail == /\ p.phase = "Queue" /\ ~p.conn
             /\ LET s == [p.status EXCEPT ![p.current] = "Unknown"] IN
                p' = [p EXCEPT !.status = Unknown(s), !.frozen = TRUE,
                               !.phase = "SyncAbort", !.resume = "Boundary"]
\* Exact retry is lookup-before-frozen; conflicts freeze even during pending IO.
\* crates/tickwire-gateway/src/lib.rs:260-267
Retry(i) == /\ p.phase = "Boundary" /\ i \in Present /\ UNCHANGED p
Conflict(i) == /\ p.phase = "Boundary" /\ i \in Present
               /\ p' = [p EXCEPT !.status = Unknown(@), !.frozen = TRUE]
\* Busy/capacity/frozen rejection preserves all historical evidence. crates/tickwire-gateway/src/lib.rs:268-276
RejectUnavailable == /\ p.phase = "Boundary"
                     /\ (p.frozen \/ HasPending \/ Present = Intents)
                     /\ UNCHANGED p
\* External trusted gate may change between calls or while drive is pending,
\* never carried as a permit across the next first application write.
\* crates/tickwire-gateway/src/lib.rs:345-349
GateChange == /\ p.phase \in {"Boundary", "Drive", "Settle", "Try"}
              /\ p' = [p EXCEPT !.gate = ~@]
\* Monotone deadline passage, including inside a synchronous method.
\* crates/tickwire-transport/src/connection.rs:152-162
\* crates/tickwire-transport/src/connection.rs:193-197
Expire(i) == /\ i \in p.closure /\ p.fresh[i]
             /\ p' = [p EXCEPT !.fresh[i] = FALSE]

\* poll serially runs drive, settle, try_send, then returns. crates/tickwire-gateway/src/lib.rs:330-369
PollStart == /\ CanMutate
             /\ p' = [p EXCEPT !.phase = "Drive", !.driveOk = TRUE]
\* Invalid zero budget returns before expiry or socket operations; the gateway
\* freezes after settling the unchanged report. This is not a disconnect.
\* crates/tickwire-transport/src/connection.rs:269-275
\* crates/tickwire-gateway/src/lib.rs:334-367
DriveBudgetError == /\ p.phase = "Drive"
                    /\ p' = [p EXCEPT !.driveOk = FALSE, !.phase = "Settle"]
DriveIdle == /\ CanDrive /\ DriveFresh
             /\ p' = [p EXCEPT !.phase = "Settle"]
DriveReady == /\ CanDrive /\ DriveFresh /\ ~p.ready
              /\ p' = [p EXCEPT !.ready = TRUE, !.phase = "Settle"]
\* Only previously accepted TLS bytes can flush. crates/tickwire-transport/src/connection.rs:269-397
DriveFlush(n) ==
    /\ CanDrive /\ DriveFresh /\ HasPending /\ p.send = "Accepted"
    /\ n \in 1..(p.accepted[p.pending] - p.wire[p.pending])
    /\ LET w == p.wire[p.pending] + n IN
       p' = [p EXCEPT !.wire[p.pending] = w,
                      !.send = IF w = Bytes THEN "Flushed" ELSE @,
                      !.phase = "Settle"]
DriveExpire == /\ CanDrive /\ HasPending /\ ~p.fresh[p.pending] /\ LiveSend
               /\ LET zero == p.accepted[p.pending] = 0 IN
                  p' = [p EXCEPT !.send = IF zero THEN "NoBytes" ELSE "Failed",
                                 !.conn = IF zero THEN @ ELSE FALSE,
                                 !.ready = IF zero THEN @ ELSE FALSE,
                                 !.driveOk = zero, !.phase = "Settle"]
\* Error / EOF closes transport. Already Flushed can be taken before the outer
\* poll freezes all unresolved entries; no new application write is possible.
\* crates/tickwire-transport/src/connection.rs:145-151; crates/tickwire-gateway/src/lib.rs:359-367
DriveFailure ==
    /\ p.phase = "Drive"
    /\ p' = [p EXCEPT !.conn = FALSE, !.ready = FALSE, !.driveOk = FALSE,
                      !.send = IF HasPending /\ LiveSend
                               THEN IF p.accepted[p.pending] = 0
                                    THEN "NoBytes" ELSE "Failed"
                               ELSE @,
                      !.phase = "Settle"]
\* Equivalent same-call flush followed by EOF; mark_flushed precedes recv.
\* crates/tickwire-transport/src/connection.rs:229-237
\* crates/tickwire-transport/src/connection.rs:330-396; crates/tickwire-gateway/src/lib.rs:431-434
\* crates/tickwire-gateway/src/lib.rs:359-367
DriveFlushThenClose ==
    /\ CanDrive /\ DriveFresh /\ HasPending /\ p.send = "Accepted"
    /\ p.accepted[p.pending] = Bytes /\ p.wire[p.pending] < Bytes
    /\ p' = [p EXCEPT !.wire[p.pending] = Bytes, !.send = "Flushed",
                      !.conn = FALSE, !.ready = FALSE, !.driveOk = FALSE,
                      !.phase = "Settle"]

\* apply_report branches, crates/tickwire-gateway/src/lib.rs:422-447
SettleNone == /\ p.phase = "Settle" /\ ~HasPending
              /\ p' = [p EXCEPT !.phase = "EndPoll"]
SettleLive == /\ p.phase = "Settle" /\ HasPending /\ LiveSend
              /\ p' = [p EXCEPT !.status[p.pending] =
                                  IF p.send = "Accepted" THEN "Sending" ELSE @,
                               !.phase = IF p.driveOk THEN "Try" ELSE "EndPoll"]
SettleFlushed == /\ p.phase = "Settle" /\ HasPending /\ p.send = "Flushed"
                 /\ p' = [p EXCEPT !.status[p.pending] = "Awaiting",
                                  !.pending = 0, !.send = "Idle", !.phase = "EndPoll"]
SettleNoBytes == /\ p.phase = "Settle" /\ HasPending /\ p.send = "NoBytes"
                 /\ p' = [p EXCEPT !.current = p.pending, !.phase = "SyncAbort",
                                  !.resume = "EndPoll"]
SettleFailed == /\ p.phase = "Settle" /\ HasPending /\ p.send = "Failed"
                /\ p' = [p EXCEPT !.frozen = TRUE, !.status = Unknown(@),
                                 !.phase = "EndPoll"]
\* try_send always rechecks its Instant before readiness or fresh gate.
\* crates/tickwire-transport/src/connection.rs:183-197; crates/tickwire-gateway/src/lib.rs:345-351
TryExpire == /\ p.phase = "Try" /\ ~p.fresh[p.pending]
             /\ LET zero == p.accepted[p.pending] = 0 IN
                p' = [p EXCEPT !.send = IF zero THEN "NoBytes" ELSE "Failed",
                               !.conn = IF zero THEN @ ELSE FALSE,
                               !.ready = IF zero THEN @ ELSE FALSE,
                               !.frozen = @ \/ ~zero,
                               !.status = IF zero THEN @ ELSE Unknown(@),
                               !.current = IF zero THEN p.pending ELSE 0,
                               !.resume = "EndPoll",
                               !.phase = IF zero THEN "SyncAbort" ELSE "EndPoll"]
TryNotReady == /\ p.phase = "Try" /\ p.fresh[p.pending] /\ ~p.ready
               /\ p' = [p EXCEPT !.phase = "EndPoll"]
FirstGateDenied == /\ p.phase = "Try" /\ p.ready /\ p.fresh[p.pending]
                   /\ p.accepted[p.pending] = 0 /\ ~p.gate
                   /\ p' = [p EXCEPT !.send = "NoBytes", !.phase = "SyncAbort",
                                    !.current = p.pending, !.resume = "EndPoll"]
\* Callback can consume the remaining Instant deadline. crates/tickwire-transport/src/connection.rs:193-197
ExpireInsideGate == /\ p.phase = "Try" /\ p.ready /\ p.fresh[p.pending]
                    /\ p.accepted[p.pending] = 0 /\ p.gate
                    /\ p' = [p EXCEPT !.fresh[p.pending] = FALSE,
                                     !.send = "NoBytes", !.phase = "SyncAbort",
                                     !.current = p.pending, !.resume = "EndPoll"]
\* 0/WouldBlock/Interrupted retains zero evidence; next poll samples fresh gate.
\* crates/tickwire-transport/src/connection.rs:213-223
TryBlocked == /\ p.phase = "Try" /\ p.ready /\ p.fresh[p.pending]
              /\ (p.accepted[p.pending] > 0 \/ p.gate)
              /\ p' = [p EXCEPT !.phase = "EndPoll"]
FirstAccept(n) ==
    /\ p.phase = "Try" /\ p.ready /\ p.fresh[p.pending] /\ p.gate
    /\ p.accepted[p.pending] = 0 /\ n \in NBytes(p.pending)
    /\ p' = [p EXCEPT !.accepted[p.pending] = n, !.firstGood = @ \cup {p.pending},
                      !.send = "Accepted", !.status[p.pending] = "Sending",
                      !.phase = "EndPoll"]
ContinueAccept(n) ==
    /\ p.phase = "Try" /\ p.ready /\ p.fresh[p.pending]
    /\ p.accepted[p.pending] > 0 /\ n \in NBytes(p.pending)
    /\ p' = [p EXCEPT !.accepted[p.pending] = @ + n, !.send = "Accepted",
                      !.status[p.pending] = "Sending", !.phase = "EndPoll"]
\* write error has the same acceptance split; it cannot invent NoBytes.
\* crates/tickwire-transport/src/connection.rs:224-227; crates/tickwire-gateway/src/lib.rs:351-355
\* crates/tickwire-gateway/src/lib.rs:445
TryFailure ==
    /\ p.phase = "Try" /\ p.ready /\ p.fresh[p.pending]
    /\ p.accepted[p.pending] < Bytes
    /\ (p.accepted[p.pending] > 0 \/ p.gate)
    /\ LET zero == p.accepted[p.pending] = 0 IN
       p' = [p EXCEPT !.conn = FALSE, !.ready = FALSE,
                      !.send = IF zero THEN "NoBytes" ELSE "Failed",
                      !.frozen = @ \/ ~zero,
                      !.status = IF zero THEN @ ELSE Unknown(@),
                      !.current = IF zero THEN p.pending ELSE 0,
                      !.resume = "EndPoll",
                      !.phase = IF zero THEN "SyncAbort" ELSE "EndPoll"]
PollEnd == /\ p.phase = "EndPoll"
           /\ p' = [p EXCEPT !.phase = "Boundary",
                            !.frozen = @ \/ ~p.driveOk,
                            !.status = IF p.driveOk THEN @ ELSE Unknown(@)]

\* Cancel delegates to transport and durably handles zero evidence. crates/tickwire-gateway/src/lib.rs:370-378
CancelZero == /\ CanMutate /\ HasPending /\ p.accepted[p.pending] = 0
              /\ p' = [p EXCEPT !.send = "NoBytes", !.current = p.pending,
                               !.phase = "SyncAbort", !.resume = "Boundary"]
CancelAccepted == /\ CanMutate /\ HasPending /\ p.accepted[p.pending] > 0
                  /\ p' = [p EXCEPT !.send = "Failed", !.frozen = TRUE,
                                   !.status = Unknown(@), !.conn = FALSE, !.ready = FALSE]
\* Synchronized abort first, externally visible Aborted only afterwards.
\* crates/tickwire-gateway/src/lib.rs:449-459
AbortSyncOk == /\ p.phase = "SyncAbort"
               /\ p' = [p EXCEPT !.durableAbort = @ \cup {p.current},
                                !.phase = "PublishAbort"]
AbortSyncFail(keep) ==
    /\ p.phase = "SyncAbort"
    /\ p' = [p EXCEPT !.durableAbort = IF keep THEN @ \cup {p.current} ELSE @,
                      !.status = Unknown(@), !.frozen = TRUE,
                      !.phase = "Boundary", !.current = 0]
PublishAbort == /\ p.phase = "PublishAbort"
                /\ p' = [p EXCEPT !.status[p.current] = "Aborted",
                                 !.pending = 0, !.send = "Idle",
                                 !.phase = p.resume, !.current = 0]

\* Crash may preserve an unacknowledged complete last append. Recovery retains
\* only complete verified events and never reconstructs a network queue.
\* crates/tickwire-gateway/src/lib.rs:170-252; crates/tickwire-business-journal/src/lib.rs:139-216
\* crates/tickwire-business-journal/src/lib.rs:311-328
Crash(keep) ==
    /\ p.phase \notin {"Crashed", "Rejected"} /\ p.crashes < MaxCrashes
    /\ p' = [p EXCEPT
         !.durableIntent = IF keep /\ p.phase = "SyncIntent" THEN @ \cup {p.current} ELSE @,
         !.durableAbort = IF keep /\ p.phase = "SyncAbort" THEN @ \cup {p.current} ELSE @,
         !.phase = "Crashed", !.current = 0, !.resume = "Boundary",
         !.status = EmptyStatus, !.pending = 0, !.send = "Idle",
         !.conn = FALSE, !.ready = FALSE, !.frozen = TRUE, !.driveOk = FALSE,
         !.crashes = @ + 1]
RecoverOk ==
    /\ p.phase = "Crashed"
    /\ p' = [p EXCEPT !.status = [i \in Intents |->
                            IF i \in p.durableAbort THEN "Aborted"
                            ELSE IF i \in p.durableIntent THEN "Unknown" ELSE "None"],
                      !.phase = "Boundary", !.frozen = TRUE]
RecoverReject == /\ p.phase = "Crashed"
                 /\ p' = [p EXCEPT !.phase = "Rejected"]

Next == \/ RejectPreparation \/ IntentSyncOk \/ QueueOk \/ QueueFail
        \/ RejectUnavailable \/ GateChange \/ PollStart \/ DriveBudgetError \/ DriveIdle \/ DriveReady
        \/ DriveExpire \/ DriveFailure \/ DriveFlushThenClose
        \/ SettleNone \/ SettleLive \/ SettleFlushed \/ SettleNoBytes \/ SettleFailed
        \/ TryExpire \/ TryNotReady \/ FirstGateDenied \/ ExpireInsideGate \/ TryBlocked
        \/ TryFailure \/ PollEnd \/ CancelZero \/ CancelAccepted
        \/ AbortSyncOk \/ PublishAbort \/ RecoverOk \/ RecoverReject
        \/ (\E i \in Intents : Prepare(i) \/ Retry(i) \/ Conflict(i) \/ Expire(i))
        \/ (\E n \in 1..Bytes : DriveFlush(n) \/ FirstAccept(n) \/ ContinueAccept(n))
        \/ (\E keep \in BOOLEAN : IntentSyncFail(keep) \/ AbortSyncFail(keep) \/ Crash(keep))
Spec == Init /\ [][Next]_vars
TypeOK ==
    /\ p.phase \in Phases /\ p.current \in Intents \cup {0}
    /\ p.pending \in Intents \cup {0} /\ p.status \in [Intents -> States]
    /\ p.closure \subseteq Intents /\ p.durableIntent \subseteq Intents
    /\ p.durableAbort \subseteq Intents /\ p.firstGood \subseteq Intents
    /\ p.accepted \in [Intents -> 0..Bytes] /\ p.wire \in [Intents -> 0..Bytes]
    /\ p.enqueues \in [Intents -> Nat] /\ p.fresh \in [Intents -> BOOLEAN]
    /\ p.frozen \in BOOLEAN /\ p.gate \in BOOLEAN /\ p.conn \in BOOLEAN
    /\ p.ready \in BOOLEAN /\ p.driveOk \in BOOLEAN /\ p.crashes \in 0..MaxCrashes
    /\ p.send \in {"Idle", "Queued", "Accepted", "Flushed", "NoBytes", "Failed"}
DurableBeforeOutput == \A i \in Intents :
    p.accepted[i] > 0 => i \in p.durableIntent /\ i \in p.closure
DurableBeforeQueue == \A i \in Intents : p.enqueues[i] > 0 => i \in p.durableIntent
FreshFirstOutput == \A i \in Intents : p.accepted[i] > 0 => i \in p.firstGood
AbortBeforeRelease == \A i \in Intents :
    p.status[i] = "Aborted" => i \in p.durableAbort /\ p.accepted[i] = 0
AbortEvidence == p.durableAbort \subseteq p.durableIntent /\
                (\A i \in p.durableAbort : p.accepted[i] = 0)
NoReplay == \A i \in Intents : p.enqueues[i] <= 1
PrefixEvidence == \A i \in Intents : p.wire[i] <= p.accepted[i]
AwaitingIsLocalFlush == \A i \in Intents : p.status[i] = "Awaiting" => p.wire[i] = Bytes
RecoveryFrozen == p.crashes > 0 => p.frozen /\ ~p.conn /\ p.pending = 0
PendingIdentity == HasPending => p.pending \in Present /\ p.enqueues[p.pending] = 1
=============================================================================
