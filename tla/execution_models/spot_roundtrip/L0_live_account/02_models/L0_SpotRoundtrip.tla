-------------------------- MODULE L0_SpotRoundtrip --------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Members, Capacity, CanGap, CanRestart, CanExpire
M == 1..Members
(*
One account owner, separate private reader/strategy producers. Source 9b2ed63.
published/mark: one authentic fresh complete quote and the account replica's paired
notice (crates/tickwire-market-publisher/src/spot.rs:129-146;
crates/tickwire-market-source/src/shm.rs:292-309). queued: canonical file descriptors
from distinct honest members, as in crates/tickwire-spot-runner/tests/reconcile_process.rs:16-90.
admitted/aborted: admission and proven zero-byte abort in the account WAL;
free: available cash measured in whole fee-inclusive reservations, not wallet total
(crates/tickwire-account-gateway/src/lib.rs:413-475,1137-1196).
slot: the sole outstanding request and immutable owner (one key per member);
sample: values observed inside the authorization callback. The empty tuple denotes no pending callback.
The account thread cannot poll a second component inside try_send. The private reader
can run after the sample but before TLS accepts bytes (crates/tickwire-transport/src/connection.rs:178-217).
writes: counted first accepted bytes; known/traded: exchange observations under ordinary
success, not implications of TLS acceptance. replies records the recipient and request key.
captured/drained: durable native trade identities visible to the private reader/account.
booked: durable economic trade IDs; debits: normalized actual fill charges (one per ID).
ready/epoch/grant/scan: authenticated private connection and recovery cut, assuming
steady deadlines except the command deadline fault below; restarted: one loss of
process-local state. Time, authentication, catalog, wallet comparison and partial
fills are projected at the boundaries stated in 01_reasoning.md.
*)
VARIABLES published, mark, queued, admitted, aborted, free, slot, sample,
          writes, known, traded, replies, captured, drained, booked, debits,
          ready, epoch, grant, scan, restarted, expired
md == <<published,mark,queued>>
ledger == <<admitted,aborted,free>>
send == <<slot,sample,writes>>
venue == <<known,traded,replies>>
econ == <<captured,drained,booked,debits>>
priv == <<ready,epoch,grant,scan,restarted,expired>>
vars == <<md,ledger,send,venue,econ,priv>>
NoSample == << >>
Stamp == <<epoch, captured, admitted, aborted, booked, known, traded>>
Init == /\ published=FALSE /\ mark=FALSE /\ queued={}
        /\ admitted={} /\ aborted={} /\ free=Capacity
        /\ slot=0 /\ sample=NoSample /\ writes=[i \in M |-> 0]
        /\ known={} /\ traded={} /\ replies={} /\ captured={} /\ drained={} /\ booked={} /\ debits=0
        /\ ready=TRUE /\ epoch=1 /\ grant=0 /\ scan= << >> /\ restarted=FALSE /\ expired=FALSE

\* A single live pair enters SHM, and independent consumers can take different turns.
\* crates/tickwire-market-publisher/src/spot.rs:129-146
Publish == /\ ~published /\ published' = TRUE /\ UNCHANGED <<mark,queued,ledger,send,venue,econ,priv>>
\* crates/tickwire-market-source/src/shm.rs:292-309
Observe == /\ published /\ ~mark /\ sample=NoSample /\ mark'=TRUE /\ UNCHANGED <<published,queued,ledger,send,venue,econ,priv>>
\* crates/tickwire-spot-runner/tests/reconcile_process.rs:16-90
Produce(i) == /\ published /\ i \notin queued /\ queued'=queued \cup {i}
              /\ UNCHANGED <<published,mark,ledger,send,venue,econ,priv>>

\* crates/tickwire-spot-runner/src/live.rs:249-270,297-316 and live/account.rs:131-159
BeginScan == /\ sample=NoSample /\ ready /\ captured=drained /\ grant=0 /\ slot=0 /\ scan= << >>
             /\ scan'=Stamp /\ UNCHANGED <<md,ledger,send,venue,econ,ready,epoch,grant,restarted,expired>>
\* crates/tickwire-spot-runner/src/live.rs:262-293 (changed cut, WAL position or result)
DropStale == /\ scan # << >> /\ (~ready \/ scan # Stamp) /\ scan'= << >>
             /\ UNCHANGED <<md,ledger,send,venue,econ,ready,epoch,grant,restarted,expired>>
\* The passing branch covers all known open/terminal orders, matches cash and books missing trades.
\* crates/tickwire-spot-runner/src/reconcile.rs:152-192; crates/tickwire-spot-runner/src/reconcile/ingest.rs:244-298
Activate == /\ scan=Stamp /\ ready /\ captured=drained /\ slot=0 /\ admitted \ aborted \subseteq known
            /\ debits'=debits+Cardinality(traded \ booked) /\ booked'=booked \cup traded
            /\ grant'=epoch /\ scan'= << >>
            /\ UNCHANGED <<md,ledger,send,venue,captured,drained,ready,epoch,restarted,expired>>
\* One round-robin scheduling choice among currently available descriptors; different producer
\* timings realize either first member. Admission/closure/WAL are a successful macrostep.
\* crates/tickwire-app/src/strategy.rs:628-668; crates/tickwire-account-gateway/src/lib.rs:413-508
Admit(i) == /\ slot=0 /\ mark /\ ready /\ grant=epoch /\ captured=drained /\ ~expired
            /\ i \in queued \ admitted /\ free>0
            /\ admitted'=admitted \cup {i} /\ free'=free-1 /\ slot'=i
            /\ UNCHANGED <<md,aborted,sample,writes,venue,econ,priv>>
Qualified == ready /\ grant=epoch /\ captured=drained /\ mark /\ ~expired
\* crates/tickwire-spot-runner/src/live.rs:147-164; crates/tickwire-account-gateway/src/lib.rs:571-587
SampleGate == /\ slot # 0 /\ writes[slot]=0 /\ sample=NoSample /\ Qualified /\ sample'= <<epoch,grant,ready,captured,drained,mark,slot \in admitted \ aborted>>
              /\ UNCHANGED <<md,ledger,slot,writes,venue,econ,priv>>
\* crates/tickwire-transport/src/connection.rs:188-217 (private status can change, deadline sampled again)
FirstByte == /\ slot # 0 /\ sample # NoSample /\ ~expired /\ writes'=[writes EXCEPT ![slot]=@+1] /\ sample'=NoSample
             /\ UNCHANGED <<md,ledger,slot,venue,econ,priv>>
\* crates/tickwire-account-gateway/src/lib.rs:1137-1196; crates/tickwire-transport/src/connection.rs:183-197
AbortEmpty == /\ slot # 0 /\ writes[slot]=0 /\ (expired \/ (sample=NoSample /\ ~Qualified))
              /\ aborted'=aborted \cup {slot} /\ free'=free+1 /\ slot'=0 /\ sample'=NoSample
              /\ UNCHANGED <<md,admitted,writes,venue,econ,priv>>
\* crates/tickwire-spot-execution/src/session.rs:201-251; remote success is an external input.
Accept(i) == /\ writes[i]>0 /\ i \notin known /\ known'=known \cup {i}
             /\ UNCHANGED <<md,ledger,send,traded,replies,econ,priv>>
Fill(i) == /\ i \in known \ traded /\ traded'=traded \cup {i}
           /\ UNCHANGED <<md,ledger,send,known,replies,econ,priv>>
\* crates/tickwire-spot-runner/src/lib.rs:294-309; crates/tickwire-app/src/strategy.rs:685-694
Ack == /\ slot \in known /\ replies'=replies \cup {<<slot,slot>>} /\ slot'=0
       /\ UNCHANGED <<md,ledger,sample,writes,known,traded,econ,priv>>
\* crates/tickwire-spot-execution/src/private/capture.rs:289-307 (separate thread)
Capture(i) == /\ ready /\ i \in traded \ captured /\ captured'=captured \cup {i}
              /\ UNCHANGED <<md,ledger,send,venue,drained,booked,debits,priv>>
\* Terminal update requires a new scan. Identical WS/REST trade IDs book once.
\* crates/tickwire-spot-runner/src/live/account.rs:70-110; crates/tickwire-account/src/events.rs:82-112
Drain(i) == /\ sample=NoSample /\ i \in captured \ drained /\ drained'=drained \cup {i}
            /\ debits'=debits+(IF i \in booked THEN 0 ELSE 1) /\ booked'=booked \cup {i}
            /\ grant'=0 /\ UNCHANGED <<md,ledger,send,venue,captured,ready,epoch,scan,restarted,expired>>
\* crates/tickwire-spot-execution/src/private/stream.rs:193-236 (observed by reader)
Gap == /\ CanGap /\ ready /\ epoch=1 /\ ready'=FALSE
       /\ UNCHANGED <<md,ledger,send,venue,econ,epoch,grant,scan,restarted,expired>>
Reconnect == /\ ~ready /\ epoch'=epoch+1 /\ ready'=TRUE
             /\ UNCHANGED <<md,ledger,send,venue,econ,grant,scan,restarted,expired>>
\* Grant invalidation by sole owner. Reader status already rejects the preceding epoch.
\* crates/tickwire-spot-runner/src/live/account.rs:59-111
Freeze == /\ sample=NoSample /\ grant # 0 /\ (~ready \/ grant # epoch) /\ grant'=0
          /\ UNCHANGED <<md,ledger,send,venue,econ,ready,epoch,scan,restarted,expired>>
\* crates/tickwire-transport/src/connection.rs:150-162,183-197
Deadline == /\ CanExpire /\ ~expired /\ expired'=TRUE
            /\ UNCHANGED <<md,ledger,send,venue,econ,ready,epoch,grant,scan,restarted>>
\* Crash + successful reopen with strictly newer approved writer. WAL and exchange survive;
\* pre-byte admissions without terminal venue evidence may remain held indefinitely.
\* crates/tickwire-account-gateway/src/lib.rs:348-384; crates/tickwire-spot-runner/src/live.rs:193-227
Restart == /\ CanRestart /\ ~restarted /\ restarted'=TRUE /\ ready'=TRUE /\ epoch'=epoch+1
           /\ grant'=0 /\ scan'= << >> /\ sample'=NoSample /\ slot'=0
           /\ UNCHANGED <<md,ledger,writes,venue,econ,expired>>
Idle == UNCHANGED vars
Next == Publish \/ Observe \/ (\E i \in M: Produce(i)) \/ BeginScan \/ DropStale \/ Activate
        \/ (\E i \in M: Admit(i)) \/ SampleGate \/ FirstByte \/ AbortEmpty
        \/ (\E i \in M: Accept(i)) \/ (\E i \in M: Fill(i)) \/ Ack
        \/ (\E i \in M: Capture(i)) \/ (\E i \in M: Drain(i))
        \/ Gap \/ Reconnect \/ Freeze \/ Deadline \/ Restart \/ Idle
TypeOK == /\ admitted \subseteq queued /\ queued \subseteq M /\ aborted \subseteq admitted
          /\ slot \in M \cup {0} /\ free \in 0..Capacity /\ debits \in 0..Members
          /\ epoch \in 1..3 /\ grant \in 0..3
SampleSound == IF sample=NoSample THEN TRUE ELSE
                  /\ sample[1]=sample[2] /\ sample[3] /\ sample[4]=sample[5] /\ sample[6] /\ sample[7]
CashConserved == /\ Capacity=free+Cardinality(admitted \ (aborted \cup booked))+debits
                 /\ debits=Cardinality(booked) /\ booked \subseteq traded
Provenance == /\ known \subseteq {i \in M: writes[i]=1} /\ traded \subseteq known
              /\ \A i \in M: writes[i] \in 0..1 /\ (writes[i]>0 => i \in admitted \ aborted /\ mark)
              /\ \A p \in replies: p[1]=p[2] /\ p[1] \in known
              /\ drained \subseteq captured /\ captured \subseteq traded
Complete == /\ booked=M /\ replies={<<i,i>> : i \in M} /\ grant=epoch /\ drained=M
Fair == /\ WF_vars(Publish) /\ WF_vars(Observe) /\ WF_vars(BeginScan) /\ WF_vars(Activate)
        /\ WF_vars(SampleGate) /\ WF_vars(FirstByte) /\ WF_vars(Ack) /\ WF_vars(DropStale) /\ WF_vars(AbortEmpty)
        /\ \A i \in M: WF_vars(Produce(i)) /\ WF_vars(Admit(i)) /\ WF_vars(Accept(i))
                       /\ WF_vars(Fill(i)) /\ WF_vars(Capture(i)) /\ WF_vars(Drain(i))
HealthySpec == Init /\ [][Next]_vars /\ Fair
Finishes == <>Complete
Resolved == /\ booked \cup aborted=M /\ replies={<<i,i>> : i \in booked} /\ grant=epoch /\ drained=booked
Settles == <>Resolved
=============================================================================
