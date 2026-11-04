---------------------------- MODULE L0_MarketConsumer -------------------------
EXTENDS Naturals
CONSTANTS MaxOld, MaxAhead
VARIABLE s
vars == <<s>>
(*
One authenticated market-source change in one supervised, remote-market worker.
The host invokes ARM/COMMIT and receives replies on a single ordered lane.
crates/tickwire-app/src/control/market.rs:191-212, crates/tickwire-app/src/control/market.rs:275-289.
crates/tickwire-strategy-host/src/remote.rs:246-272, crates/tickwire-strategy-host/src/remote.rs:309-320.
s.arm, w: host has queued ARM; local worker control point. Old reader remains
  allocated during pause. crates/tickwire-strategy-host/src/market_transition.rs:85-98,
  crates/tickwire-strategy-sdk/src/worker.rs:1012-1030.
s.oldSeq, cut, newReads: old applied watermark, captured cut, bounded count
  of ordinary callbacks after the new consumer has been assigned.
  crates/tickwire-strategy-sdk/src/worker.rs:695-722, crates/tickwire-strategy-sdk/src/worker.rs:849-880.
s.made, sent, host: highest ordered receipt stored, sent and accepted (0..3).
  poll, pollAt: single host owner sampled liveness and time before dispatching replies.
  clock: two successive instants in one monotonic clock domain. madeAt records
  the writer timestamp in each immutable receipt. bad records rejection of
  such a legitimately generated receipt as being later than the host sample.
  crates/tickwire-strategy-host/src/readiness.rs:64-91, crates/tickwire-market-source/src/transition.rs:201-221,
  crates/tickwire-market-source/src/transition.rs:357-386.
  All refer to the same operation, actual owned pid and approved banks.
  crates/tickwire-strategy-sdk/src/worker.rs:753-758, crates/tickwire-strategy-host/src/market_transition.rs:167-179,
  crates/tickwire-strategy-host/src/market_transition.rs:193-247.
s.cutover, persisted: validated commit cutover or zero; merged checkpoint call returned.
  crates/tickwire-strategy-host/src/market_transition.rs:108-137, crates/tickwire-market-source/src/transition.rs:316-349,
  crates/tickwire-strategy-sdk/src/worker.rs:795-818.
s.expired, revoked, sampleOK: approval expired, successor revoked/stale,
  and what the final frontier check observed. A fault may arrive after sampling.
  crates/tickwire-strategy-sdk/src/worker/expansion.rs:90-125, crates/tickwire-strategy-sdk/src/worker/expansion.rs:163-181.
s.installed, alive: candidate consumer assigned; worker still running.
  crates/tickwire-strategy-sdk/src/worker.rs:819-842, crates/tickwire-strategy-sdk/src/worker.rs:555-573.
*)
Init == s = [arm |-> FALSE, w |-> "old", oldSeq |-> 1, cut |-> 0,
             made |-> 0, sent |-> 0, host |-> 0, cutover |-> 0,
             poll |-> "idle", pollAt |-> 0, clock |-> 0, madeAt |-> [r \in 1..3 |-> 0],
             bad |-> FALSE, persisted |-> FALSE, sampleOK |-> FALSE, installed |-> FALSE,
             newReads |-> 0, expired |-> FALSE, revoked |-> FALSE, alive |-> TRUE]
\* crates/tickwire-strategy-host/src/market_transition.rs:53-99.
HostArm == /\ ~s.arm /\ ~s.expired /\ s.alive /\ s.poll="idle"
           /\ s'=[s EXCEPT !.arm=TRUE]
\* crates/tickwire-strategy-sdk/src/worker.rs:993-1030.
WorkerArm == /\ s.alive /\ s.arm /\ s.w="old" /\ ~s.expired
             /\ s'=[s EXCEPT !.w="armed", !.made=1, !.madeAt[1]=s.clock]
\* Completed application of an old input between full worker callback batches.
\* crates/tickwire-strategy-sdk/src/worker.rs:695-722.
OldCallback == /\ s.alive /\ s.w \in {"old","armed","expiredArm"} /\ s.oldSeq<MaxOld
               /\ s'=[s EXCEPT !.oldSeq=@+1]
\* Authenticated QUIESCE binds this old subscription and operation.
\* crates/tickwire-strategy-sdk/src/worker.rs:849-880.
Detach == /\ s.alive /\ s.w="armed" /\ ~s.expired
          /\ s'=[s EXCEPT !.w="paused", !.cut=s.oldSeq, !.made=2, !.madeAt[2]=s.clock]
\* One FIFO lane. A packet already accepted by the socket may outlive the child.
\* crates/tickwire-strategy-sdk/src/worker.rs:753-758.
SendReceipt == /\ s.alive /\ s.w#"expiredArm" /\ s.sent<s.made
               /\ s'=[s EXCEPT !.sent=@+1]
\* One owner checks process, samples the clock, then dispatches all received frames
\* with that sample. A deadline can pass between the sample and dispatch.
\* crates/tickwire-strategy-host/src/remote.rs:307-317.
StartPoll == /\ s.alive /\ s.poll="idle"
             /\ s'=[s EXCEPT !.poll=IF s.expired THEN "expired" ELSE "fresh", !.pollAt=s.clock]
EndPoll == /\ s.poll \in {"fresh","expired"} /\ s'=[s EXCEPT !.poll="idle"]
\* Only replies for the current operation, PID, expected cut and descriptor.
\* crates/tickwire-strategy-host/src/market_transition.rs:193-247.
Eligible == s.host<s.sent /\ s.poll="fresh"
HostObserve == /\ Eligible /\ s.madeAt[s.host+1]<=s.pollAt
               /\ s'=[s EXCEPT !.host=@+1]
\* Actual clock can advance between sampling and the child's receipt write,
\* even though approval and both session leases are still valid.
\* crates/tickwire-strategy-host/src/remote.rs:307-317, crates/tickwire-strategy-host/src/readiness.rs:75-91,
\* crates/tickwire-market-source/src/transition.rs:207-221, crates/tickwire-strategy-host/src/remote.rs:290-305.
Tick == /\ s.clock=0 /\ ~s.expired /\ s'=[s EXCEPT !.clock=1]
HostRejectNewer == /\ Eligible /\ s.madeAt[s.host+1]>s.pollAt
                   /\ s'=[s EXCEPT !.bad=TRUE, !.poll="failed"]
\* crates/tickwire-strategy-host/src/market_transition.rs:121-136, crates/tickwire-market-source/src/transition.rs:316-349.
HostCommit == /\ s.host=2 /\ s.cutover=0 /\ s.alive /\ ~s.expired /\ s.poll="idle"
              /\ \E frontier \in s.cut..(s.cut+MaxAhead):
                     s'=[s EXCEPT !.cutover=frontier]
\* crates/tickwire-strategy-sdk/src/worker.rs:1068-1087.
WorkerCommit == /\ s.w="paused" /\ s.alive /\ s.cutover>0 /\ ~s.expired
                /\ s'=[s EXCEPT !.w="joining"]
\* crates/tickwire-strategy-sdk/src/worker.rs:765-795, crates/tickwire-strategy-sdk/src/worker/expansion.rs:96-125.
FirstSample == /\ s.alive /\ s.w="joining" /\ ~s.expired /\ ~s.revoked
               /\ s'=[s EXCEPT !.w="saving", !.madeAt[3]=s.clock]
\* crates/tickwire-strategy-sdk/src/worker.rs:796-812.
Checkpoint == /\ s.alive /\ s.w="saving"
              /\ s'=[s EXCEPT !.w="saved", !.persisted=TRUE]
\* The active worker validates the receipt, then polls the actual client again.
\* crates/tickwire-strategy-sdk/src/worker.rs:813-828.
FinalSample == /\ s.alive /\ s.w="saved"
               /\ ~s.expired /\ ~s.revoked
               /\ s'=[s EXCEPT !.w="sampled", !.sampleOK=(~s.expired /\ ~s.revoked)]
\* No further clock read between the successful sample and assignment.
\* crates/tickwire-strategy-sdk/src/worker.rs:829-839.
Install == /\ s.alive /\ s.w="sampled"
           /\ s'=[s EXCEPT !.w="new", !.installed=TRUE, !.made=3]
\* The worker can dispatch before the host receives its ACTIVATED frame.
\* crates/tickwire-strategy-sdk/src/worker.rs:555-573, crates/tickwire-strategy-sdk/src/worker.rs:695-722.
NewCallback == /\ s.alive /\ s.w="new" /\ s.newReads<2
               /\ s'=[s EXCEPT !.newReads=@+1]
\* A control frame, EOF or stale catchup view can invalidate the candidate while saving.
\* crates/tickwire-strategy-sdk/src/worker/expansion.rs:96-120, crates/tickwire-strategy-sdk/src/worker/expansion.rs:163-181.
Expire == /\ ~s.expired /\ s'=[s EXCEPT !.expired=TRUE]
Revoke == /\ ~s.revoked /\ s.w \in {"joining","saving","saved","sampled","new"}
          /\ s'=[s EXCEPT !.revoked=TRUE]
\* Before detaching, expiry clears the expansion. Once paused the worker exits.
\* crates/tickwire-strategy-sdk/src/worker.rs:555-573.
ExpireArm == /\ s.alive /\ s.expired /\ s.w="armed"
             /\ s'=[s EXCEPT !.w="expiredArm"]
\* Unsuccessful calls and process exits do not advance local control state.
\* crates/tickwire-strategy-sdk/src/worker.rs:560-573, crates/tickwire-strategy-sdk/src/worker.rs:813-828,
\* crates/tickwire-supervisor/src/lib.rs:327-345.
Stop == /\ s.alive /\ s'=[s EXCEPT !.alive=FALSE]

Next == HostArm \/ WorkerArm \/ OldCallback \/ Detach \/ SendReceipt \/ StartPoll \/ HostObserve \/ EndPoll
        \/ HostCommit \/ WorkerCommit \/ FirstSample \/ Checkpoint \/ FinalSample
        \/ Install \/ NewCallback \/ Expire \/ Revoke \/ ExpireArm \/ Tick \/ HostRejectNewer \/ Stop
Spec == Init /\ [][Next]_vars
SourceCut == s.cut=0 \/ s.oldSeq=s.cut
ActivationEvidence == (s.installed \/ s.newReads>0) =>
                        (s.cut>0 /\ s.cutover>=s.cut /\ s.persisted /\ s.sampleOK)
HostUsesWorkerCut == /\ s.host<=s.sent /\ s.sent<=s.made
                     /\ (s.cutover>0 => s.host>=2 /\ s.cut>0)
                     /\ (s.host=3 => s.installed)
ValidReceiptHandled == ~s.bad
===============================================================================
