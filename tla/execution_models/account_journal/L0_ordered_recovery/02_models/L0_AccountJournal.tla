-------------------------- MODULE L0_AccountJournal --------------------------
EXTENDS Naturals, Integers, Sequences, FiniteSets, TLC
CONSTANTS Orders, Capital, MaxRecords, CrashLimit, BadSeeds
VARIABLE s
vars == <<s>>

(* var/code map (single owner; no second append/recover call can interleave).
 files/stable/fsynced/phase: FileBackend::publish create/write/file-sync/dir-sync
   crates/tickwire-account-journal/src/backend.rs:38-66
 cache/disk/tail: Store contract and one complete record per sealed batch
   crates/tickwire-journal/src/store.rs:6-18
   crates/tickwire-journal/src/journal.rs:450-487
 visible/history/poison/hold/pending: append candidate vs published state
   crates/tickwire-account-journal/src/lib.rs:245-290
 closures/verified/prefix: external RecoveryVerifier + callback-before-advance
   crates/tickwire-account-journal/src/lib.rs:70-77
   crates/tickwire-account-journal/src/lib.rs:215-232
 writer/target/required/requiredBefore/rejected: recovery identity/frontier
   crates/tickwire-account-journal/src/lib.rs:153-212
   crates/tickwire-journal/src/journal.rs:309-348
 seen/aborted/ledger/ri/frozen/returned/unknown: finite Spot reducer instance
   crates/tickwire-account/src/recovery.rs:17-43
   crates/tickwire-account/src/recovery.rs:91-101
   crates/tickwire-account/src/admission.rs:411-436
 ack/crashes/configOK: observer witnesses and caller-selected bounded inputs.
   crates/tickwire-account-journal/src/lib.rs:86-105
   crates/tickwire-account-journal/src/history.rs:17-59
 A record contains symbolic canonical bytes; hash equality is collision-free.
 No closure object supplies a missing WAL record. Physical frame/segment/manifest
 internals and six other RecoveryRecord variants are outside this level. *)
Ids == 1..Orders
Files == {"base", "boot", "physical"}
EmptyFiles == [f \in Files |-> 0]
FullFiles == [f \in Files |-> 2]
Pre(q,n) == IF n=0 THEN <<>> ELSE SubSeq(q,1,n)
Prefix(a,b) == Len(a)<=Len(b) /\ a=Pre(b,Len(a))
AdmitIds(q) == {q[i].id : i \in {j \in 1..Len(q):q[j].kind="A"}}
AbortIds(q) == {q[i].id : i \in {j \in 1..Len(q):q[j].kind="B"}}
Ledger(q) == Cardinality(AdmitIds(q))+Cardinality(AbortIds(q))
Row(k,id,w,n,v,img) == [kind |-> k, id |-> id, original |-> 1,
 writer |-> w, seq |-> n, binding |-> 1, variant |-> v, imageSeq |-> img]
A == Row("A",1,1,1,0,1)
B == Row("B",1,1,2,0,0)
Seeds == IF BadSeeds THEN {<<>>, <<[A EXCEPT !.seq=2]>>,
 <<[A EXCEPT !.writer=0]>>, <<[A EXCEPT !.binding=2]>>,
 <<[B EXCEPT !.seq=1, !.original=2]>>,
 <<[A EXCEPT !.writer=2, !.original=2], B>>,
 <<A,[A EXCEPT !.seq=2]>>} ELSE {<<>>}
Stage(f,x) == [base |-> [create |-> "base-create",write |-> "base-write",sync |-> "base-sync",dir |-> "base-dir"],
 boot |-> [create |-> "boot-create",write |-> "boot-write",sync |-> "boot-sync",dir |-> "boot-dir"],
 physical |-> [create |-> "physical-create",write |-> "physical-write",sync |-> "physical-sync",dir |-> "physical-dir"]][f][x]
CreatePhases == {Stage(f,x): f \in Files,x \in {"create","write","sync","dir"}}
RecoverPhases == {"config","physical","required","recover-sync","structure",
 "closure","advance","replay","return"}
AppendPhases == {"append-tail","append-seal","append-sync","append-publish"}
Initial(q) == [phase |-> "down", files |-> IF q= <<>> THEN EmptyFiles ELSE FullFiles,
 stable |-> IF q= <<>> THEN EmptyFiles ELSE FullFiles, fsynced |-> {},
 cache |-> q, disk |-> q, tail |-> FALSE, visible |-> <<>>, history |-> <<>>,
 ack |-> <<>>, poison |-> FALSE, hold |-> <<>>, pending |-> "none",
 recovered |-> FALSE, writer |-> 1, target |-> 1, configOK |-> TRUE,
 required |-> [present |-> FALSE,prefix |-> <<>>], requiredBefore |-> <<>>, rejected |-> FALSE,
 closures |-> [i \in Ids |-> [present |-> FALSE,record |-> A,preceding |-> <<>>]], prefix |-> <<>>, verified |-> {},
 ci |-> 1, ri |-> 1, seen |-> {}, aborted |-> {}, ledger |-> 0,
 frozen |-> FALSE, returned |-> <<>>, unknown |-> {}, crashes |-> 0]
Init == \E q \in Seeds: s=Initial(q)

(* Exclusive create, dependency-first: crates/tickwire-account-journal/src/lib.rs:125-151; crates/tickwire-account-journal/src/backend.rs:38-66. *)
StartCreate == /\ s.phase="down" /\ s.files=EmptyFiles
 /\ s'=[s EXCEPT !.phase= "base-create", !.rejected=FALSE]
RejectCreate == /\ s.phase="down" /\ s.files#EmptyFiles /\ UNCHANGED s
CreateFile == \E f \in Files:
 /\ s.phase= Stage(f,"create")
 /\ s'=[s EXCEPT !.files[f]=1, !.phase= Stage(f,"write")]
WriteFile == \E f \in Files:
 /\ s.phase= Stage(f,"write")
 /\ s'=[s EXCEPT !.files[f]=2,
     !.fsynced=IF f="physical" THEN @ \cup {f} ELSE @, !.phase= Stage(f,"sync")]
SyncFile == \E f \in Files:
 /\ s.phase= Stage(f,"sync")
 /\ s'=[s EXCEPT !.fsynced=@ \cup {f}, !.phase= Stage(f,"dir")]
SyncDirectory == \E f \in Files:
 /\ s.phase= Stage(f,"dir")
 /\ s'=[s EXCEPT !.stable=s.files, !.fsynced={},
     !.phase=CASE f="base" -> "boot-create" [] f="boot" -> "physical-create"
             [] OTHER -> "open"]
CreateFailure == /\ s.phase \in CreatePhases
 /\ s'=[s EXCEPT !.phase="down"]

(* Append does not authorize or recompute AdmissionImage. The external fixture
 may have an exact, missing, or wrong preceding closure (crates/tickwire-account-journal/src/lib.rs:70-77,245-279). *)
PrepareAdmit == \E id \in Ids, v \in 0..1, c \in {"exact","missing","wrong"}:
 /\ s.phase="open" /\ ~s.poison /\ ~s.recovered
 /\ Len(s.visible)<MaxRecords /\ id \notin AdmitIds(s.visible)
 /\ LET r == Row("A",id,s.writer,Len(s.visible)+1,v,Ledger(s.visible)+1)
        closure == [present |-> c#"missing",record |-> r,
         preceding |-> IF c="wrong" THEN <<[A EXCEPT !.binding=3]>> ELSE s.visible]
    IN s'=[s EXCEPT !.pending=r, !.phase="append-tail", !.closures[id]=closure]
PrepareAbort == \E id \in Ids, v \in 0..1:
 /\ s.phase="open" /\ ~s.poison /\ Len(s.visible)<MaxRecords
 /\ s'=[s EXCEPT !.pending=Row("B",id,s.writer,Len(s.visible)+1,v,0),
     !.phase="append-tail"]
RetryAdmit == /\ s.phase="open" /\ ~s.poison /\ ~s.recovered
 /\ AdmitIds(s.visible)#{} /\ UNCHANGED s
ConflictAdmit == /\ s.phase="open" /\ ~s.poison /\ ~s.recovered
 /\ AdmitIds(s.visible)#{}
 /\ s'=[s EXCEPT !.poison=TRUE, !.hold=s.visible]
RejectPoison == /\ s.phase="open" /\ s.poison /\ UNCHANGED s
RejectRecoveredAdmit == /\ s.phase="open" /\ ~s.poison /\ s.recovered /\ UNCHANGED s
RejectCapacity == /\ s.phase="open" /\ ~s.poison /\ Len(s.visible)=MaxRecords
 /\ UNCHANGED s
(* Partial write / complete seal / successful sync / in-memory publication are
 separate crash boundaries: crates/tickwire-journal/src/journal.rs:450-487; crates/tickwire-account-journal/src/lib.rs:272-289. *)
WriteTail == /\ s.phase="append-tail"
 /\ s'=[s EXCEPT !.tail=TRUE, !.phase="append-seal"]
SealBatch == /\ s.phase="append-seal"
 /\ s'=[s EXCEPT !.cache=Append(@,s.pending), !.tail=FALSE, !.phase="append-sync"]
SyncBatch == /\ s.phase="append-sync"
 /\ s'=[s EXCEPT !.disk=s.cache, !.phase="append-publish"]
PublishReceipt == /\ s.phase="append-publish"
 /\ s'=[s EXCEPT !.visible=s.cache, !.history=s.cache, !.ack=s.cache,
     !.pending="none", !.phase="open"]
AppendFailure == /\ s.phase \in {"append-tail","append-seal","append-sync"}
 /\ \E d \in IF s.phase="append-sync" THEN {s.disk,s.cache} ELSE {s.disk}:
 s'=[s EXCEPT !.disk=d, !.poison=TRUE, !.hold=s.visible,
     !.pending="none", !.phase="open"]

(* FileBackend::open re-syncs root before handing the locked backend to recover:
 crates/tickwire-account-journal/src/backend.rs:17-20
 crates/tickwire-journal/src/store.rs:44-62.
 Start with caller's expected bootstrap and required physical frontier.
 crates/tickwire-account-journal/src/lib.rs:153-184; required is an exact prefix identity, not a lower-bound number. *)
StartRecover == \E w \in 1..2, ok \in BOOLEAN,
 req \in {[present |-> FALSE,prefix |-> <<>>],
           [present |-> TRUE,prefix |-> <<[A EXCEPT !.binding=3]>>]} \cup
          (IF s.ack= <<>> THEN {} ELSE {[present |-> TRUE,prefix |-> s.ack]}):
 /\ s.phase="down"
 /\ s'=[s EXCEPT !.phase="config", !.target=w, !.configOK=ok,
     !.stable=[k \in Files |-> IF k \in s.fsynced THEN s.files[k] ELSE s.stable[k]],
     !.required=req, !.requiredBefore= <<s.cache,s.disk,s.tail>>, !.rejected=FALSE,
     !.prefix= <<>>, !.verified={}, !.ci=1, !.ri=1,
     !.seen={}, !.aborted={}, !.ledger=0, !.returned= <<>>, !.unknown={}]
ConfigValid == s.configOK /\ s.files["base"]=2 /\ s.files["boot"]=2
CheckConfig == /\ s.phase="config" /\ ConfigValid
 /\ s'=[s EXCEPT !.phase="physical"]
RejectConfig == /\ s.phase="config" /\ ~ConfigValid
 /\ s'=[s EXCEPT !.phase="down"]
(* Physical batch headers reject zero/backward writer before tail mutation.
 crates/tickwire-journal/src/journal.rs:229-248. Payload seq remains a later check. *)
PhysicalValid == s.files["physical"]=2 /\
 (\A i \in 1..Len(s.cache):s.cache[i].writer>0 /\
  (i>1 => s.cache[i].writer>=s.cache[i-1].writer))
CheckPhysical == /\ s.phase="physical" /\ PhysicalValid
 /\ s'=[s EXCEPT !.phase="required"]
RejectPhysical == /\ s.phase="physical" /\ ~PhysicalValid
 /\ s'=[s EXCEPT !.phase="down"]
RequiredValid == ~s.required.present \/ Prefix(s.required.prefix,s.cache)
CheckRequired == /\ s.phase="required" /\ RequiredValid
 /\ s'=[s EXCEPT !.phase="recover-sync", !.tail=FALSE]
RejectRequired == /\ s.phase="required" /\ ~RequiredValid
 /\ s'=[s EXCEPT !.phase="down", !.rejected=TRUE]
(* Journal::recover validates before trimming, then synchronizes complete data.
 crates/tickwire-journal/src/journal.rs:309-348. *)
SyncRecovery == /\ s.phase="recover-sync"
 /\ s'=[s EXCEPT !.disk=s.cache, !.phase="structure"]
RecoveryIoFailure == /\ s.phase \in {"config","physical","required","recover-sync"}
 /\ s'=[s EXCEPT !.phase="down"]
StructureValid(q,w) ==
 /\ \A i \in 1..Len(q):
    /\ q[i].seq=i /\ q[i].binding=1 /\ q[i].writer \in 1..w
    /\ q[i].original \in 1..q[i].writer
    /\ (q[i].kind="A" => q[i].original=q[i].writer)
    /\ (i>1 => q[i].writer>=q[i-1].writer)
 /\ \A i,j \in 1..Len(q):
    (q[i].kind="A" /\ q[j].kind="A" /\ q[i].id=q[j].id) => i=j
CheckStructure == /\ s.phase="structure" /\ StructureValid(s.cache,s.target)
 /\ s'=[s EXCEPT !.phase="closure"]
RejectStructure == /\ s.phase="structure" /\ ~StructureValid(s.cache,s.target)
 /\ s'=[s EXCEPT !.phase="down"]

(* The verifier receives the record and history BEFORE history.advance.
 crates/tickwire-account-journal/src/lib.rs:215-232; crates/tickwire-account-journal/src/history.rs:32-44. *)
ClosureValid(r) == s.closures[r.id]=[present |-> TRUE,record |-> r, preceding |-> s.prefix]
VerifyClosure == /\ s.phase="closure" /\ s.ci<=Len(s.cache)
 /\ s.cache[s.ci].kind="A" /\ ClosureValid(s.cache[s.ci])
 /\ s'=[s EXCEPT !.verified=@ \cup {s.ci}, !.phase="advance"]
RejectClosure == /\ s.phase="closure" /\ s.ci<=Len(s.cache)
 /\ s.cache[s.ci].kind="A" /\ ~ClosureValid(s.cache[s.ci])
 /\ s'=[s EXCEPT !.phase="down"]
VerifierUnavailable == /\ s.phase="closure" /\ s.ci<=Len(s.cache)
 /\ s.cache[s.ci].kind="A"
 /\ s'=[s EXCEPT !.phase="down"]
SkipClosure == /\ s.phase="closure" /\ s.ci<=Len(s.cache)
 /\ s.cache[s.ci].kind="B" /\ s'=[s EXCEPT !.phase="advance"]
AdvanceHistory == /\ s.phase="advance"
 /\ s'=[s EXCEPT !.prefix=Append(@,s.cache[s.ci]), !.ci=@+1, !.phase="closure"]
BeginReplay == /\ s.phase="closure" /\ s.ci=Len(s.cache)+1
 /\ s'=[s EXCEPT !.phase="replay"]
ReplayValid(r) == IF r.kind="A" THEN
 r.variant=0 /\ r.imageSeq=s.ledger+1 /\ r.id \notin s.seen
 /\ Cardinality(s.seen \ s.aborted)<Capital
 ELSE r.variant=0 /\ r.id \in s.seen
(* Account::recover validates complete AdmissionImage and abort command digest.
 crates/tickwire-account/src/recovery.rs:17-43; crates/tickwire-account/src/admission.rs:4-32,103-143. *)
ReplayAdmit == /\ s.phase="replay" /\ s.ri<=Len(s.cache)
 /\ LET r==s.cache[s.ri] IN
    /\ r.kind="A" /\ ReplayValid(r)
    /\ s'=[s EXCEPT !.seen=@ \cup {r.id}, !.ledger=@+1, !.ri=@+1]
ReplayAbort == /\ s.phase="replay" /\ s.ri<=Len(s.cache)
 /\ LET r==s.cache[s.ri] IN
    /\ r.kind="B" /\ ReplayValid(r)
    /\ s'=[s EXCEPT !.aborted=@ \cup {r.id},
     !.ledger=IF r.id \in s.aborted THEN @ ELSE @+1, !.ri=@+1]
RejectReplay == /\ s.phase="replay" /\ s.ri<=Len(s.cache)
 /\ ~ReplayValid(s.cache[s.ri]) /\ s'=[s EXCEPT !.phase="down"]
FreezeRecovered == /\ s.phase="replay" /\ s.ri=Len(s.cache)+1
 /\ s'=[s EXCEPT !.frozen=TRUE, !.unknown=s.seen \ s.aborted, !.phase="return"]
ReturnRecovered == /\ s.phase="return"
 /\ s'=[s EXCEPT !.visible=s.cache, !.history=s.cache, !.writer=s.target,
     !.recovered=TRUE, !.poison=FALSE, !.hold= <<>>, !.returned=s.cache,
     !.pending="none", !.phase="open"]

(* Failure injection follows Store sync completion. Unsynced file names and
 bytes may disappear; file-sync without directory-sync permits 0 or full bytes.
 Completed baseline/bootstrap precede any physical log creation. *)
FileSurvivors == {f \in [Files -> 0..2]: \A k \in Files:
 IF s.stable[k]=2 THEN f[k]=2
 ELSE IF k \in s.fsynced THEN f[k] \in {0,s.files[k]}
 ELSE f[k] \in 0..s.files[k]}
Crash == /\ s.crashes<CrashLimit /\ s.phase#"down"
 /\ \E f \in FileSurvivors, q \in {s.disk,s.cache}, torn \in BOOLEAN:
 /\ (torn => (s.tail \/ Len(q)<Len(s.cache)))
 /\ s'=[s EXCEPT !.phase="down", !.files=f, !.stable=f,
     !.fsynced={}, !.cache=q, !.disk=q, !.tail=torn,
     !.visible= <<>>, !.history= <<>>, !.poison=FALSE, !.hold= <<>>,
     !.pending="none", !.recovered=FALSE, !.frozen=FALSE,
     !.rejected=FALSE, !.returned= <<>>, !.verified={}, !.prefix= <<>>, !.crashes=@+1]

Next == StartCreate \/ RejectCreate \/ CreateFile \/ WriteFile \/ SyncFile
 \/ SyncDirectory \/ CreateFailure \/ PrepareAdmit \/ PrepareAbort
 \/ RetryAdmit \/ ConflictAdmit \/ RejectPoison \/ RejectRecoveredAdmit
 \/ RejectCapacity \/ WriteTail \/ SealBatch \/ SyncBatch \/ PublishReceipt
 \/ AppendFailure \/ StartRecover \/ CheckConfig \/ RejectConfig
 \/ CheckPhysical \/ RejectPhysical \/ CheckRequired \/ RejectRequired
 \/ SyncRecovery \/ RecoveryIoFailure \/ CheckStructure \/ RejectStructure
 \/ VerifyClosure \/ RejectClosure \/ VerifierUnavailable \/ SkipClosure
 \/ AdvanceHistory \/ BeginReplay \/ ReplayAdmit \/ ReplayAbort \/ RejectReplay
 \/ FreezeRecovered \/ ReturnRecovered \/ Crash
Spec == Init /\ [][Next]_vars

TypeOK == /\ s.files \in [Files -> 0..2] /\ s.stable \in [Files -> 0..2]
 /\ s.fsynced \subseteq Files /\ s.crashes \in 0..CrashLimit
 /\ Len(s.cache)<=MaxRecords /\ Len(s.disk)<=MaxRecords
 /\ s.poison \in BOOLEAN /\ s.recovered \in BOOLEAN /\ s.frozen \in BOOLEAN
 /\ s.seen \subseteq Ids /\ s.aborted \subseteq s.seen
DependencyFirst == (s.files["physical"]>0 =>
 s.stable["base"]=2 /\ s.stable["boot"]=2)
NoLostReceipt == Prefix(s.ack,s.disk)
ReceiptDependencies == s.ack#<<>> => s.stable=FullFiles
PublishedHistory == s.visible=s.history /\ Prefix(s.visible,s.cache)
PoisonKeepsPrefix == s.poison => s.visible=s.hold
RequiredBeforeMutation == s.rejected =>
 <<s.cache,s.disk,s.tail>>=s.requiredBefore
RecoveredFrozen == s.recovered => s.frozen
RecoveryNotExposed == s.phase \in RecoverPhases => s.visible= <<>> /\ s.history= <<>>
VerifiedPreceding == \A i \in s.verified:
 s.closures[s.cache[i].id]=[present |-> TRUE,record |-> s.cache[i], preceding |-> Pre(s.cache,i-1)]
ReturnedChecked == s.returned#<<>> =>
 StructureValid(s.returned,s.writer) /\
 (\A i \in 1..Len(s.returned):s.returned[i].kind="A" => i \in s.verified)
ReplayConservation == Cardinality(s.seen \ s.aborted)<=Capital
DiskPrefix == Prefix(s.disk,s.cache)
PublicStructure == StructureValid(s.visible,s.writer)
HistoryPrefix == s.phase \in {"closure","advance","replay","return"} =>
 s.ci \in 1..(Len(s.cache)+1) /\ s.prefix=Pre(s.cache,s.ci-1)
ReturnedReplay == s.recovered =>
 /\ s.ri=Len(s.returned)+1 /\ s.seen=AdmitIds(s.returned)
 /\ s.aborted=AbortIds(s.returned) /\ s.ledger=Ledger(s.returned)
 /\ s.unknown=s.seen \ s.aborted
=============================================================================
