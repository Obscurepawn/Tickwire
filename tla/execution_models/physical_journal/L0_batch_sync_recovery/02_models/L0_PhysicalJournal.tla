----------------------- MODULE L0_PhysicalJournal -----------------------
EXTENDS Integers, Sequences, FiniteSets, TLC
CONSTANTS MaxCalls, MaxCrashes, RecordsPerBatch

\* var <-> code map; single fixed segment, after successful create and lock.
\* s.pc/pos/epoch: Journal position and synchronous call continuation
\*   crates/tickwire-journal/src/journal.rs:150-175,371-484
\* s.cache/disk: FileStore write_all / sync_data content boundaries
\*   crates/tickwire-journal/src/store.rs:135-154
\* s.pending/written/calls: encoded batch / write_all fragments, bounded calls
\*   crates/tickwire-journal/src/journal.rs:379-457
\* s.acks/lastAck/synced: proof prefix and ghost completed-sync history
\*   crates/tickwire-journal/src/journal.rs:471-484
\* s.required/checked/repair: required frontier, read scan, final-tail truncation
\*   crates/tickwire-journal/src/journal.rs:177-369
\* s.badAt: concrete complete-frame corruption location; codec rejects it
\*   crates/tickwire-journal/src/frame.rs:122-194
\* s.crashes/faultUsed/mediaLoss: bounded environment faults, design sec4
\*   docs/design/replication-and-failover.md:58-83
\* Append rejection, poisoning, recovery validation map to the source ranges above.
\* Whole-tail media loss is diagnostic: without trusted frontier no loss detection
\* is claimed. CRC/SHA collision resistance and real power loss are not proved.

VARIABLE s
vars == <<s>>
Width == RecordsPerBatch + 2
Prefix(a,b) == Len(a)<=Len(b) /\ a=SubSeq(b,1,Len(a))
Closed(a) == SubSeq(a,1,(Len(a) \div Width)*Width)
Frames(b,e,v) == [i \in 1..Width |-> [batch |-> b, epoch |-> e, body |-> v, index |-> i]]
Valid(a) ==
    /\ \A i \in 1..Len(a):
        /\ a[i].batch = ((i-1) \div Width)+1
        /\ a[i].index = ((i-1) % Width)+1
        /\ a[i].epoch \in 1..2
        /\ a[i].body \in 1..MaxCalls
        /\ LET first == ((i-1) \div Width)*Width+1 IN
            /\ a[i].epoch = a[first].epoch
            /\ a[i].body = a[first].body
    /\ \A i \in 2..Len(a): a[i].epoch >= a[i-1].epoch
LastEpoch(a) == IF Len(a)=0 THEN 0 ELSE a[Len(a)].epoch
GoodRecovery == s.badAt=0 /\ Valid(s.cache) /\ Prefix(s.required,s.cache)
Survivors == IF Len(s.disk)<=Len(s.cache)
    THEN {SubSeq(s.cache,1,k): k \in Len(s.disk)..Len(s.cache)}
    ELSE {s.disk,s.cache}

Init == s = [pc |-> "Ready", cache |-> <<>>, disk |-> <<>>, pos |-> 0,
    epoch |-> 0, pending |-> <<>>, written |-> 0, calls |-> 0, crashes |-> 0,
    acks |-> {}, lastAck |-> <<>>, synced |-> {}, required |-> <<>>,
    checked |-> <<>>, repair |-> [before |-> <<>>, after |-> <<>>],
    badAt |-> 0, faultUsed |-> FALSE, mediaLoss |-> FALSE]

\* crates/tickwire-journal/src/journal.rs:371-457: validated inputs and complete encoded batch before I/O.
BeginAppend(e) ==
    /\ s.pc="Ready" /\ s.calls<MaxCalls /\ e \in 1..2 /\ e>=s.epoch
    /\ s'=[s EXCEPT !.pc="Writing", !.pending=Frames(s.pos+1,e,s.calls+1),
                    !.written=0, !.calls=@+1]
RejectInvalid == /\ s.pc="Ready" /\ UNCHANGED s
\* crates/tickwire-journal/src/store.rs:135-141: partial write_all progress; only one owner can append.
WriteFrame ==
    /\ s.pc="Writing" /\ s.written<Width
    /\ s'=[s EXCEPT !.cache=Append(@,s.pending[s.written+1]), !.written=@+1,
                    !.pc=IF s.written+1=Width THEN "Syncing" ELSE "Writing"]
WriteFailure == /\ s.pc="Writing" /\ s'=[s EXCEPT !.pc="Poisoned"]
\* crates/tickwire-journal/src/journal.rs:473-478: synchronization can fail without a positive receipt.
SyncFailure == /\ s.pc="Syncing" /\ s'=[s EXCEPT !.pc="Poisoned"]
SyncSuccess ==
    /\ s.pc="Syncing"
    /\ s'=[s EXCEPT !.disk=s.cache, !.synced=@ \cup {s.cache}, !.pc="Returning"]
\* crates/tickwire-journal/src/journal.rs:480-484: state and proof publish after completed sync.
ReturnReceipt ==
    /\ s.pc="Returning"
    /\ s'=[s EXCEPT !.pos=@+1, !.epoch=LastEpoch(s.cache),
        !.acks=@ \cup {s.cache}, !.lastAck=s.cache, !.pc="Ready"]
RejectPoisoned == /\ s.pc="Poisoned" /\ UNCHANGED s

\* Allowed crash retains synchronized bytes; unfinished suffix may be a prefix.
\* A crash during truncate can retain either old or shortened unsealed tail.
Crash(bytes) ==
    /\ s.pc \in {"Ready","Writing","Syncing","Returning","Poisoned","Checking","RecoverySync"}
    /\ s.crashes<MaxCrashes /\ bytes \in Survivors
    /\ s'=[s EXCEPT !.cache=bytes, !.disk=bytes, !.pc="Down", !.pos=0,
        !.epoch=0, !.crashes=@+1, !.required= <<>>, !.checked= <<>>]
\* Diagnostic storage faults occur offline; rejection must preserve these bytes.
CorruptFrame(i) ==
    /\ s.pc="Down" /\ ~s.faultUsed /\ i \in 1..Len(Closed(s.cache))
    /\ s'=[s EXCEPT !.badAt=i, !.faultUsed=TRUE]
LoseWholeTail(n) ==
    /\ s.pc="Down" /\ ~s.faultUsed /\ n \in 0..((Len(s.cache) \div Width)-1)
    /\ s'=[s EXCEPT !.cache=SubSeq(@,1,n*Width), !.disk=SubSeq(@,1,n*Width),
                    !.faultUsed=TRUE, !.mediaLoss=TRUE]
\* crates/tickwire-journal/src/journal.rs:179-340: Option<trusted frontier> is selected by the caller.
BeginRecovery(trust) ==
    /\ s.pc="Down" /\ trust \in BOOLEAN
    /\ s'=[s EXCEPT !.required=IF trust THEN s.lastAck ELSE <<>>,
                    !.checked=s.cache, !.pc="Checking"]
RecoveryReject ==
    /\ s.pc="Checking" /\ ~GoodRecovery
    /\ s'=[s EXCEPT !.pc="Quarantine"]
\* crates/tickwire-journal/src/journal.rs:341-347: no modification until all validation has succeeded.
RepairTail ==
    /\ s.pc="Checking" /\ GoodRecovery
    /\ s'=[s EXCEPT !.cache=Closed(s.cache), !.pc="RecoverySync",
                    !.repair=[before |-> s.cache, after |-> Closed(s.cache)]]
RecoverySyncFailure ==
    /\ s.pc="RecoverySync" /\ s'=[s EXCEPT !.pc="Poisoned"]
RecoverySyncSuccess ==
    /\ s.pc="RecoverySync"
    /\ s'=[s EXCEPT !.disk=s.cache, !.pos=Len(s.cache) \div Width,
        !.epoch=LastEpoch(s.cache), !.synced=@ \cup {s.cache}, !.pc="Ready"]

Next ==
    \/ (\E e \in 1..2: BeginAppend(e))
    \/ RejectInvalid \/ WriteFrame \/ WriteFailure \/ SyncFailure \/ SyncSuccess
    \/ ReturnReceipt \/ RejectPoisoned
    \/ (\E bytes \in Survivors: Crash(bytes))
    \/ (\E i \in 1..Len(Closed(s.cache)): CorruptFrame(i))
    \/ (\E n \in 0..((Len(s.cache) \div Width)-1): LoseWholeTail(n))
    \/ (\E trust \in BOOLEAN: BeginRecovery(trust))
    \/ RecoveryReject \/ RepairTail \/ RecoverySyncFailure \/ RecoverySyncSuccess
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ s.pc \in {"Ready","Writing","Syncing","Returning","Poisoned","Down","Checking","RecoverySync","Quarantine"}
    /\ s.pos \in 0..MaxCalls /\ s.calls \in 0..MaxCalls /\ s.crashes \in 0..MaxCrashes
    /\ Len(s.cache)<=MaxCalls*Width /\ Len(s.disk)<=MaxCalls*Width
    /\ s.epoch \in 0..2 /\ s.written \in 0..Width
ReceiptHasCompletedSync == \A receipt \in s.acks: receipt \in s.synced
ReceiptNamesWholeBatch == \A receipt \in s.acks:
    Len(receipt)>0 /\ Len(receipt)%Width=0 /\ Valid(receipt)
LivePositionMatchesClosedBytes == s.pc="Ready" =>
    Valid(s.cache) /\ Len(s.cache)=s.pos*Width /\ s.epoch=LastEpoch(s.cache) /\ s.badAt=0
RequiredPrefixSurvivesRecovery == s.pc="Ready" => Prefix(s.required,s.cache)
QuarantinePreservesEvidence == s.pc="Quarantine" => s.cache=s.checked
RepairOnlyDropsUnsealedTail ==
    /\ s.repair.after=Closed(s.repair.before)
    /\ Prefix(s.repair.after,s.repair.before)
    /\ Len(s.repair.before)-Len(s.repair.after)<Width
NoAcknowledgedLossUnderSupportedCrash == (~s.mediaLoss /\ s.badAt=0) =>
    \A receipt \in s.acks: Prefix(receipt,s.cache)
=============================================================================
